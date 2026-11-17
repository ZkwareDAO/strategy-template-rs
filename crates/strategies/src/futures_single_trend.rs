//! `futures_single_trend` — 期货 1:1（单策略单标的）多周期趋势策略。
//!
//! 1:1 的含义是**一个策略实例只服务一个标的**：universe 只有一行，因此
//! 引擎只建一个 slot、一个总仓、一个子仓；要跑 N 个品种就起 N 个配置目录
//! （每个目录 universe 一行），品种之间互不共享总仓与风险预算。
//!
//! 周期分工（大周期聚合由引擎完成，策略只读聚合结果）：
//! - 触发层 base TF（配置 `timeframes[0]`，如 15m）：收盘价上穿/下穿 EMA；
//! - 趋势层 `trend_tf`（如 1h）：收盘价在 EMA 之上只做多，之下只做空；
//! - 止损：入场价 ∓ `atr_stop_mult × ATR(base TF)`，引擎不模拟挂单止损，
//!   因此策略在每根 base bar 闭合时自行检查 `position.stop_price`。
//!
//! 聚合口径：引擎只把 1m 喂给每个 slot 的 `AggregationState`，由
//! `SessionWindowPolicy` 按交易时段切桶（午休强制断桶、夜盘不跨窗口），
//! 策略经 `snapshot.closed.closed(tf)` 读到已闭合的 15m/1h 序列，不会看到
//! 未闭合桶，也不自己拼接 1m。

use anyhow::anyhow;
use serde::Deserialize;

use strategy_core::candle::Timeframe as CoreTimeframe;
use strategy_core::signal::Side;
use strategy_engine::bridge::CandleSeries;
use strategy_engine::core::bar::Timeframe as EngineTimeframe;
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};
use strategy_indicators::talib;

/// 策略私有参数（配置文件 `params:` 节；拼错字段名启动即报错）。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SingleTrendParams {
    /// 触发层 EMA 周期（base TF）
    pub trigger_ema: usize,
    /// 趋势层 EMA 周期（`trend_tf`）
    pub trend_ema: usize,
    /// 趋势层周期字符串（必须已出现在 `timeframes` 里）
    pub trend_tf: String,
    /// ATR 周期（base TF）
    pub atr_period: usize,
    /// 初始止损距离 = `atr_stop_mult × ATR`
    pub atr_stop_mult: f64,
}

impl Default for SingleTrendParams {
    fn default() -> Self {
        Self {
            trigger_ema: 60,
            trend_ema: 60,
            trend_tf: "1h".into(),
            atr_period: 14,
            atr_stop_mult: 2.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base_tf: CoreTimeframe,
    pub trend_tf: CoreTimeframe,
    pub trigger_ema: usize,
    pub trend_ema: usize,
    pub atr_period: usize,
    pub atr_stop_mult: f64,
}

pub struct FuturesSingleTrend {
    config: Config,
}

impl FuturesSingleTrend {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn from_params(
        params: Option<&serde_json::Value>,
        base_tf: CoreTimeframe,
    ) -> anyhow::Result<Self> {
        let p: SingleTrendParams = match params {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|e| anyhow!("futures_single_trend params 反序列化失败: {e}"))?,
            None => SingleTrendParams::default(),
        };
        let trend_tf = CoreTimeframe::parse(&p.trend_tf)
            .map_err(|e| anyhow!("futures_single_trend trend_tf 非法: {e}"))?;
        Ok(Self::new(Config {
            base_tf,
            trend_tf,
            trigger_ema: p.trigger_ema.max(1),
            trend_ema: p.trend_ema.max(1),
            atr_period: p.atr_period.max(1),
            atr_stop_mult: p.atr_stop_mult.max(0.0),
        }))
    }

    /// 趋势层方向：`trend_tf` 收盘价在 EMA 上/下。
    fn trend_side(&self, closed: &CandleSeries) -> Option<Side> {
        let candles = closed.closed(self.config.trend_tf)?;
        if candles.len() < self.config.trend_ema + 1 {
            return None;
        }
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        let ema = talib::ema(&closes, self.config.trend_ema).ok()?;
        let last = *closes.last()?;
        let last_ema = *ema.last()?;
        if !last_ema.is_finite() {
            return None;
        }
        if last > last_ema {
            Some(Side::Long)
        } else if last < last_ema {
            Some(Side::Short)
        } else {
            None
        }
    }

    /// 触发层事件：base TF 收盘价本根上穿/下穿 EMA。
    fn trigger_side(&self, closed: &CandleSeries) -> Option<Side> {
        let candles = closed.closed(self.config.base_tf)?;
        if candles.len() < self.config.trigger_ema + 2 {
            return None;
        }
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        let ema = talib::ema(&closes, self.config.trigger_ema).ok()?;
        let n = closes.len();
        let (prev_c, last_c) = (closes[n - 2], closes[n - 1]);
        let (prev_e, last_e) = (ema[n - 2], ema[n - 1]);
        if !prev_e.is_finite() || !last_e.is_finite() {
            return None;
        }
        if prev_c <= prev_e && last_c > last_e {
            Some(Side::Long)
        } else if prev_c >= prev_e && last_c < last_e {
            Some(Side::Short)
        } else {
            None
        }
    }

    /// base TF 的 ATR 末值（止损距离基准）。
    fn atr(&self, closed: &CandleSeries) -> Option<f64> {
        let candles = closed.closed(self.config.base_tf)?;
        if candles.len() < self.config.atr_period + 1 {
            return None;
        }
        let highs: Vec<f64> = candles.iter().map(|c| c.high).collect();
        let lows: Vec<f64> = candles.iter().map(|c| c.low).collect();
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        let atr = talib::atr(&highs, &lows, &closes, self.config.atr_period).ok()?;
        atr.last().copied().filter(|v| v.is_finite() && *v > 0.0)
    }
}

impl CrossSectionalStrategyPort for FuturesSingleTrend {
    fn strategy_type(&self) -> &str {
        "futures_single_trend"
    }

    fn timeframes(&self) -> Vec<EngineTimeframe> {
        let mut tfs = std::collections::BTreeSet::new();
        tfs.insert(EngineTimeframe::M1);
        tfs.insert(EngineTimeframe::from(self.config.base_tf));
        tfs.insert(EngineTimeframe::from(self.config.trend_tf));
        tfs.into_iter().collect()
    }

    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[EngineTimeframe],
        snapshots: &[InstrumentSnapshot],
        _portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        let mut out = Vec::new();
        for s in snapshots {
            if s.stale {
                continue;
            }
            let Some(closed) = s.closed.as_ref() else {
                continue;
            };
            let price = s.bar.close;
            if price <= 0.0 {
                continue;
            }
            let trend = self.trend_side(closed);
            let trigger = self.trigger_side(closed);

            // 有仓：先查硬止损，再查趋势/触发反向。
            if let Some(pos) = &s.position {
                let stop_hit = match pos.side {
                    Side::Long => price <= pos.stop_price,
                    Side::Short => price >= pos.stop_price,
                };
                if stop_hit {
                    out.push(InstrumentAction::Close {
                        instrument: s.instrument.clone(),
                        price,
                        reason: "futures_single_trend: ATR 硬止损".into(),
                        is_stop_loss: true,
                        strength: 1.0,
                    });
                    continue;
                }
                if trend.is_some_and(|t| t != pos.side) {
                    out.push(InstrumentAction::Close {
                        instrument: s.instrument.clone(),
                        price,
                        reason: "futures_single_trend: 趋势层反转".into(),
                        is_stop_loss: false,
                        strength: 1.0,
                    });
                    continue;
                }
                if trigger.is_some_and(|t| t != pos.side) {
                    out.push(InstrumentAction::Close {
                        instrument: s.instrument.clone(),
                        price,
                        reason: "futures_single_trend: 触发线反向".into(),
                        is_stop_loss: false,
                        strength: 1.0,
                    });
                }
                continue;
            }

            // 空仓：触发方向必须与趋势方向一致才入场。
            if !s.entry_eligible {
                continue;
            }
            let (Some(side), Some(trend_side)) = (trigger, trend) else {
                continue;
            };
            if side != trend_side {
                continue;
            }
            let Some(atr) = self.atr(closed) else {
                continue;
            };
            let dist = self.config.atr_stop_mult * atr;
            let stop_price = match side {
                Side::Long => price - dist,
                Side::Short => price + dist,
            };
            if stop_price <= 0.0 {
                continue;
            }
            out.push(InstrumentAction::Open {
                instrument: s.instrument.clone(),
                side,
                price,
                stop_price,
                atr,
                strength: 1.0,
                reason: format!(
                    "futures_single_trend: 收盘{} EMA{} + 趋势{}",
                    if side == Side::Long { "上穿" } else { "下穿" },
                    self.config.trigger_ema,
                    if trend_side == Side::Long { "向上" } else { "向下" }
                ),
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone, Utc};
    use strategy_core::candle::Candle;
    use strategy_core::strategy::PositionView;
    use strategy_engine::core::bar::{Bar, BarExtensions, BarFinality};
    use strategy_engine::core::identity::{ExchangeId, InstrumentId, MarketKind};
    use strategy_engine::core::session::TradingDay;
    use strategy_engine::strategy::features::FeatureMap;

    fn inst() -> InstrumentId {
        InstrumentId {
            exchange: ExchangeId::new("SHFE"),
            market: MarketKind::CnFutures,
            symbol: "CU".into(),
            product: "CU".into(),
            contract_month: None,
        }
    }

    fn candle(i: i64, close: f64) -> Candle {
        Candle {
            open_time: Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap()
                + chrono::Duration::minutes(i),
            open: close,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 1.0,
            quote_volume: 0.0,
            trade_num: 0,
            active_buy_volume: 0.0,
            active_buy_quote_volume: 0.0,
            is_closed: true,
        }
    }

    fn series(base_closes: &[f64], trend_closes: &[f64]) -> CandleSeries {
        let last = base_closes.last().copied().unwrap_or(0.0);
        let mut out = CandleSeries::from_closed(CoreTimeframe::M1, vec![candle(0, last)]);
        out.insert_closed(
            CoreTimeframe::M15,
            base_closes
                .iter()
                .enumerate()
                .map(|(i, &c)| candle(i as i64, c))
                .collect(),
        );
        out.insert_closed(
            CoreTimeframe::H1,
            trend_closes
                .iter()
                .enumerate()
                .map(|(i, &c)| candle(i as i64, c))
                .collect(),
        );
        out
    }

    fn snapshot(
        closed: Option<CandleSeries>,
        position: Option<PositionView>,
        price: f64,
    ) -> InstrumentSnapshot {
        InstrumentSnapshot {
            instrument: inst(),
            bar: Bar {
                instrument: inst(),
                open_time: Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 1.0,
                finality: BarFinality::Final,
                extensions: BarExtensions::default(),
            },
            position,
            stale: false,
            features: FeatureMap::default(),
            entry_eligible: true,
            entry_reason: None,
            exit_triggered: None,
            trading_day: TradingDay(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
            no_trade: false,
            entry_confirm: false,
            signal_resume: false,
            cooldown: None,
            session_open_price: None,
            closed,
        }
    }

    fn portfolio() -> PortfolioContext {
        PortfolioContext {
            strategy_id: "futures_single_trend:cu".into(),
            nav: 1_000_000.0,
            drawdown: 0.0,
            portfolio_pnl_pct: 0.0,
            sub_positions: Vec::new(),
            master: None,
            multipliers: Default::default(),
            max_total_notional: 1.0,
            rebalance_due: false,
            no_trade: false,
        }
    }

    fn config() -> Config {
        Config {
            base_tf: CoreTimeframe::M15,
            trend_tf: CoreTimeframe::H1,
            trigger_ema: 5,
            trend_ema: 5,
            atr_period: 3,
            atr_stop_mult: 2.0,
        }
    }

    fn rising(n: usize) -> Vec<f64> {
        (0..n).map(|i| 100.0 + i as f64).collect()
    }

    fn falling(n: usize) -> Vec<f64> {
        (0..n).map(|i| 200.0 - i as f64).collect()
    }

    #[test]
    fn flat_with_uptrend_and_cross_up_opens_long() {
        let mut base = vec![100.0; 20];
        *base.last_mut().unwrap() = 120.0;
        let mut s = FuturesSingleTrend::new(config());
        let snaps = vec![snapshot(
            Some(series(&base, &rising(30))),
            None,
            120.0,
        )];
        let out = s.on_cross_section_bars(&[], &snaps, &portfolio());
        assert_eq!(out.len(), 1);
        match &out[0] {
            InstrumentAction::Open {
                side, stop_price, ..
            } => {
                assert_eq!(*side, Side::Long);
                assert!(*stop_price < 120.0, "多腿止损必须在入场价下方");
            }
            other => panic!("必须开多，实际 {other:?}"),
        }
    }

    #[test]
    fn flat_with_downtrend_and_cross_down_opens_short() {
        let mut base = vec![100.0; 20];
        *base.last_mut().unwrap() = 80.0;
        let mut s = FuturesSingleTrend::new(config());
        let snaps = vec![snapshot(
            Some(series(&base, &falling(30))),
            None,
            80.0,
        )];
        let out = s.on_cross_section_bars(&[], &snaps, &portfolio());
        assert_eq!(out.len(), 1);
        match &out[0] {
            InstrumentAction::Open {
                side, stop_price, ..
            } => {
                assert_eq!(*side, Side::Short);
                assert!(*stop_price > 80.0, "空腿止损必须在入场价上方");
            }
            other => panic!("必须开空，实际 {other:?}"),
        }
    }

    #[test]
    fn trigger_and_trend_conflict_holds() {
        let mut base = vec![100.0; 20];
        *base.last_mut().unwrap() = 120.0;
        let mut s = FuturesSingleTrend::new(config());
        // 触发上穿，但 1h 趋势向下 → 不入场。
        let snaps = vec![snapshot(
            Some(series(&base, &falling(30))),
            None,
            120.0,
        )];
        let out = s.on_cross_section_bars(&[], &snaps, &portfolio());
        assert!(out.is_empty(), "趋势与触发冲突不得开仓");
    }

    #[test]
    fn stop_hit_closes_with_stop_loss_flag() {
        let mut s = FuturesSingleTrend::new(config());
        let pos = PositionView {
            side: Side::Long,
            entry_price: 100.0,
            stop_price: 95.0,
            peak_price: 105.0,
            max_pnl_pct: 5.0,
            min_pnl_pct: -5.0,
            exit_active: false,
            exit_price: 0.0,
        };
        let snaps = vec![snapshot(
            Some(series(&[100.0; 20], &rising(30))),
            Some(pos),
            90.0,
        )];
        let out = s.on_cross_section_bars(&[], &snaps, &portfolio());
        assert_eq!(out.len(), 1);
        match &out[0] {
            InstrumentAction::Close {
                is_stop_loss, ..
            } => assert!(*is_stop_loss),
            other => panic!("必须平仓，实际 {other:?}"),
        }
    }

    #[test]
    fn trend_flip_closes_but_not_as_stop_loss() {
        let mut s = FuturesSingleTrend::new(config());
        let pos = PositionView {
            side: Side::Long,
            entry_price: 100.0,
            stop_price: 50.0,
            peak_price: 105.0,
            max_pnl_pct: 5.0,
            min_pnl_pct: -5.0,
            exit_active: false,
            exit_price: 0.0,
        };
        let snaps = vec![snapshot(
            Some(series(&[100.0; 20], &falling(30))),
            Some(pos),
            120.0,
        )];
        let out = s.on_cross_section_bars(&[], &snaps, &portfolio());
        assert_eq!(out.len(), 1);
        match &out[0] {
            InstrumentAction::Close {
                is_stop_loss, ..
            } => assert!(!*is_stop_loss),
            other => panic!("必须平仓，实际 {other:?}"),
        }
    }

    #[test]
    fn missing_closed_series_is_silent() {
        let mut s = FuturesSingleTrend::new(config());
        let snaps = vec![snapshot(None, None, 100.0)];
        assert!(s
            .on_cross_section_bars(&[], &snaps, &portfolio())
            .is_empty());
    }

    #[test]
    fn from_params_rejects_unknown_keys() {
        let bad = serde_json::json!({ "triger_ema": 5 });
        assert!(FuturesSingleTrend::from_params(Some(&bad), CoreTimeframe::M15).is_err());
        let good = serde_json::json!({ "trigger_ema": 20, "trend_ema": 60 });
        assert!(FuturesSingleTrend::from_params(Some(&good), CoreTimeframe::M15).is_ok());
    }
}

crate::register_strategy! {
    name: "futures_single_trend",
    factory: |args| {
        let base_tf = CoreTimeframe::parse(args.timeframe)?;
        let _ = (args.symbol, args.cash);
        Ok(Box::new(FuturesSingleTrend::from_params(args.params, base_tf)?))
    }
}
