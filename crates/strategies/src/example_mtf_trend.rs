//! example_mtf_trend — 多周期趋势教学策略（脚手架多周期样板）
//!
//! **教学要点**：`timeframes()` 多周期、跨周期已闭合序列、ATR 初始止损、反转出场。
//! 阅读顺序：`docs/strategy/EXAMPLES.md` 本文件章节 → 对照源码。
//!
//! ## 策略逻辑（对齐 Python example_mtf_trend 的三层条件）
//! - 趋势过滤（1d）：收盘价 > EMA50 允许做多；< EMA50 允许做空
//! - 触发（4h）：MA10 上穿 MA30 → 开多；下穿 → 开空
//! - 初始止损：入场价 ± `atr_stop_mult` × ATR14(4h)
//! - 出场：止损触发，或 4h 快慢线反向交叉
//!
//! ## 多周期约定
//! - `timeframes()` 返回 `[M1, H4, D1]`：1m 是引擎时钟（必须），H4 触发，D1 趋势
//! - 读数据一律 `s.closed.closed(tf)`，每个周期独立做长度检查
//! - 指标全量重算：每次从完整已闭合序列算，不缓存增量

use serde::Deserialize;

use strategy_core::candle::Timeframe;
use strategy_core::signal::Side;
use strategy_core::strategy::PositionView;
use strategy_engine::bridge::CandleSeries;
use strategy_engine::core::bar::Timeframe as EngineTimeframe;
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};
use strategy_indicators::talib;

/// 1d 趋势层最少根数（EMA50 + 缓冲）
const MIN_ROWS_TREND: usize = 60;
/// 4h 信号层最少根数（MA30 + ATR14 + 缓冲）
const MIN_ROWS_SIGNAL: usize = 40;

/// 策略私有参数（配置 params 节透传；写错字段名启动即报错）
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Params {
    /// 4h 快线 EMA 周期
    pub fast_period: usize,
    /// 4h 慢线 EMA 周期
    pub slow_period: usize,
    /// 1d 趋势 EMA 周期
    pub trend_ema_period: usize,
    /// 4h ATR 周期
    pub atr_period: usize,
    /// 初始止损距离 = atr_stop_mult × ATR
    pub atr_stop_mult: f64,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            fast_period: 10,
            slow_period: 30,
            trend_ema_period: 50,
            atr_period: 14,
            atr_stop_mult: 2.0,
        }
    }
}

/// 运行时配置
#[derive(Debug, Clone)]
pub struct Config {
    pub fast_period: usize,
    pub slow_period: usize,
    pub trend_ema_period: usize,
    pub atr_period: usize,
    pub atr_stop_mult: f64,
    /// 触发主周期（配置 `timeframe` 透传；H4 层的交叉/ATR 均用此周期）
    pub main_timeframe: Timeframe,
}

pub struct ExampleMtfTrend {
    pub config: Config,
}

impl ExampleMtfTrend {
    /// 1d 趋势过滤：Some(true)=允许做多，Some(false)=允许做空，None=数据不足
    fn trend_allows_long(&self, closed: &CandleSeries) -> Option<bool> {
        let d1 = closed.closed(Timeframe::D1)?;
        if d1.len() < MIN_ROWS_TREND {
            return None;
        }
        let closes: Vec<f64> = d1.iter().map(|c| c.close).collect();
        let ema = talib::ema(&closes, self.config.trend_ema_period).ok()?;
        Some(d1.last()?.close > *ema.last()?)
    }

    /// 4h 快慢线交叉事件：Some(true)=本根上穿，Some(false)=本根下穿，None=无交叉或数据不足
    fn cross_signal(&self, closed: &CandleSeries) -> Option<bool> {
        let main = closed.closed(self.config.main_timeframe)?;
        if main.len() < MIN_ROWS_SIGNAL {
            return None;
        }
        let closes: Vec<f64> = main.iter().map(|c| c.close).collect();
        let fast = talib::ema(&closes, self.config.fast_period).ok()?;
        let slow = talib::ema(&closes, self.config.slow_period).ok()?;
        let n = fast.len().min(slow.len());
        if n < 2 {
            return None;
        }
        let (fast_now, slow_now) = (fast[n - 1], slow[n - 1]);
        let (fast_prev, slow_prev) = (fast[n - 2], slow[n - 2]);
        if fast_prev <= slow_prev && fast_now > slow_now {
            return Some(true); // 上穿
        }
        if fast_prev >= slow_prev && fast_now < slow_now {
            return Some(false); // 下穿
        }
        None
    }

    /// 4h ATR（初始止损距离基准）
    fn atr(&self, closed: &CandleSeries) -> Option<f64> {
        let main = closed.closed(self.config.main_timeframe)?;
        if main.len() < MIN_ROWS_SIGNAL {
            return None;
        }
        let highs: Vec<f64> = main.iter().map(|c| c.high).collect();
        let lows: Vec<f64> = main.iter().map(|c| c.low).collect();
        let closes: Vec<f64> = main.iter().map(|c| c.close).collect();
        let atr = talib::atr(&highs, &lows, &closes, self.config.atr_period).ok()?;
        atr.last().copied()
    }

    /// 入场：1d 趋势过滤 + 4h 均线交叉触发 + ATR 初始止损
    fn entry_action(&self, s: &InstrumentSnapshot, closed: &CandleSeries) -> Option<InstrumentAction> {
        if s.in_stop_loss_cooldown() {
            return None;
        }
        let (Some(trend_up), Some(cross_up), Some(atr)) = (
            self.trend_allows_long(closed),
            self.cross_signal(closed),
            self.atr(closed),
        ) else {
            return None;
        };
        let side = if cross_up && trend_up {
            Side::Long
        } else if !cross_up && !trend_up {
            Side::Short
        } else {
            return None; // 趋势与信号冲突或无交叉事件：保持空仓
        };
        let price = s.bar.close;
        let dist = self.config.atr_stop_mult * atr;
        let stop_price = match side {
            Side::Long => price - dist,
            Side::Short => price + dist,
        };
        Some(InstrumentAction::Open {
            instrument: s.instrument.clone(),
            side,
            price,
            stop_price,
            atr,
            strength: 1.0,
            reason: format!(
                "MTF: 1d趋势{} + 4h MA{}/{} {}",
                if trend_up { "上" } else { "下" },
                self.config.fast_period,
                self.config.slow_period,
                if cross_up { "金叉" } else { "死叉" }
            ),
        })
    }

    /// 出场：硬止损 或 4h 反向交叉
    fn exit_action(
        &self,
        s: &InstrumentSnapshot,
        closed: &CandleSeries,
        pos: &PositionView,
    ) -> Option<InstrumentAction> {
        let price = s.bar.close;

        // ① 硬止损
        let hit = match pos.side {
            Side::Long => price <= pos.stop_price,
            Side::Short => price >= pos.stop_price,
        };
        if hit {
            return Some(InstrumentAction::Close {
                instrument: s.instrument.clone(),
                price,
                reason: "ATR 止损触发".into(),
                is_stop_loss: true,
                strength: 1.0,
            });
        }

        // ② 4h 反向交叉事件 → 趋势离场
        let cross_up = self.cross_signal(closed)?;
        let against = match pos.side {
            Side::Long => !cross_up,
            Side::Short => cross_up,
        };
        against.then(|| InstrumentAction::Close {
            instrument: s.instrument.clone(),
            price,
            reason: "4h 均线反向交叉".into(),
            is_stop_loss: false,
            strength: 1.0,
        })
    }
}

impl CrossSectionalStrategyPort for ExampleMtfTrend {
    fn strategy_type(&self) -> &str {
        "example_mtf_trend"
    }

    fn timeframes(&self) -> Vec<EngineTimeframe> {
        vec![
            EngineTimeframe::M1,
            EngineTimeframe::from(self.config.main_timeframe),
            EngineTimeframe::from(Timeframe::D1),
        ]
    }

    /// 逐快照决策；缺失已闭合序列或数据不足的标的保持沉默。
    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[EngineTimeframe],
        snapshots: &[InstrumentSnapshot],
        _portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        let mut out = Vec::new();
        for s in snapshots {
            let Some(closed) = s.closed.as_ref() else {
                continue;
            };
            let action = match s.position {
                None => self.entry_action(s, closed),
                Some(pos) => self.exit_action(s, closed, &pos),
            };
            if let Some(a) = action {
                out.push(a);
            }
        }
        out
    }
}

// ─── 注册表接线 ─────────────────────────────────────────────────
crate::register_strategy! {
    name: "example_mtf_trend",
    factory: |args| {
        let main_timeframe = Timeframe::parse(args.timeframe)?;
        let params: Params = crate::registry::params_or_default(args.params)?;
        Ok(Box::new(ExampleMtfTrend {
            config: Config {
                fast_period: params.fast_period,
                slow_period: params.slow_period,
                trend_ema_period: params.trend_ema_period,
                atr_period: params.atr_period,
                atr_stop_mult: params.atr_stop_mult,
                main_timeframe,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone, Utc};
    use strategy_core::candle::Candle;
    use strategy_engine::core::bar::{Bar, BarExtensions, BarFinality};
    use strategy_engine::core::identity::{ExchangeId, InstrumentId};
    use strategy_engine::core::session::TradingDay;
    use strategy_engine::strategy::features::FeatureMap;

    fn inst(symbol: &str) -> InstrumentId {
        InstrumentId::perpetual(&ExchangeId::new("binance"), symbol)
    }

    /// 带振幅的 K 线：保证 ATR > 0（止损距离非零才可断言止损在上/下方）。
    fn make_candle(close: f64) -> Candle {
        Candle {
            open_time: Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
            open: close,
            high: close + 1.0,
            low: close - 1.0,
            close,
            volume: 0.0,
            quote_volume: 0.0,
            trade_num: 0,
            active_buy_volume: 0.0,
            active_buy_quote_volume: 0.0,
            is_closed: true,
        }
    }

    /// 已闭合序列：M1 用末值，主周期与 D1 各插一个桶。
    fn series(main_closes: &[f64], d1_closes: &[f64]) -> CandleSeries {
        let last = main_closes.last().copied().unwrap_or(0.0);
        let mut out = CandleSeries::from_closed(Timeframe::M1, vec![make_candle(last)]);
        out.insert_closed(
            Timeframe::H4,
            main_closes.iter().map(|&c| make_candle(c)).collect(),
        );
        out.insert_closed(
            Timeframe::D1,
            d1_closes.iter().map(|&c| make_candle(c)).collect(),
        );
        out
    }

    fn bar(symbol: &str, close: f64) -> Bar {
        Bar {
            instrument: inst(symbol),
            open_time: Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
            open: close,
            high: close,
            low: close,
            close,
            volume: 0.0,
            finality: BarFinality::Final,
            extensions: BarExtensions::default(),
        }
    }

    fn snapshot(
        symbol: &str,
        closed: Option<CandleSeries>,
        position: Option<PositionView>,
        close: f64,
    ) -> InstrumentSnapshot {
        InstrumentSnapshot {
            instrument: inst(symbol),
            bar: bar(symbol, close),
            position,
            stale: false,
            features: FeatureMap::default(),
            entry_eligible: true,
            entry_reason: None,
            exit_triggered: None,
            trading_day: TradingDay(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
            no_trade: false,
            entry_confirm: true,
            signal_resume: false,
            cooldown: None,
            session_open_price: None,
            closed,
        }
    }

    fn portfolio() -> PortfolioContext {
        PortfolioContext {
            strategy_id: "example_mtf_trend:default".into(),
            nav: 1000.0,
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

    fn long_position(stop_price: f64) -> Option<PositionView> {
        Some(PositionView {
            side: Side::Long,
            entry_price: 100.0,
            stop_price,
            peak_price: 105.0,
            max_pnl_pct: 5.0,
            min_pnl_pct: -2.0,
            exit_active: false,
            exit_price: 0.0,
        })
    }

    fn strat() -> ExampleMtfTrend {
        ExampleMtfTrend {
            config: Config {
                fast_period: 10,
                slow_period: 30,
                trend_ema_period: 50,
                atr_period: 14,
                atr_stop_mult: 2.0,
                main_timeframe: Timeframe::H4,
            },
        }
    }

    fn run(snaps: &[InstrumentSnapshot]) -> Vec<InstrumentAction> {
        strat().on_cross_section_bars(&[EngineTimeframe::M1], snaps, &portfolio())
    }

    /// 上升的 D1 序列（末值高于 EMA → 允许做多）。
    fn rising_d1() -> Vec<f64> {
        (0..80).map(|i| 100.0 + i as f64).collect()
    }

    /// 下降的 D1 序列（末值低于 EMA → 允许做空）。
    fn falling_d1() -> Vec<f64> {
        (0..80).map(|i| 180.0 - i as f64).collect()
    }

    /// 4h 走平 + 末根跳升 → 金叉。
    fn gold_cross_main() -> Vec<f64> {
        let mut closes: Vec<f64> = vec![100.0; 60];
        closes.push(120.0);
        closes
    }

    #[test]
    fn trend_and_gold_cross_opens_long_with_atr_stop_below() {
        let snaps = [snapshot(
            "BTCUSDT",
            Some(series(&gold_cross_main(), &rising_d1())),
            None,
            120.0,
        )];
        let actions = run(&snaps);
        assert_eq!(actions.len(), 1, "趋势向上 + 金叉应开多，实际: {actions:?}");
        match &actions[0] {
            InstrumentAction::Open {
                instrument,
                side,
                stop_price,
                price,
                atr,
                reason,
                ..
            } => {
                assert_eq!(
                    instrument.namespaced_key(),
                    "binance/crypto_perpetual/BTCUSDT"
                );
                assert_eq!(*side, Side::Long);
                assert!(*atr > 0.0, "ATR 必须为正");
                assert!(stop_price < price, "多头止损应在入场价下方");
                assert!(reason.contains("金叉"), "入场原因: {reason}");
            }
            a => panic!("应开多，实际: {a:?}"),
        }
    }

    /// 趋势与信号冲突（D1 向下 + 4h 金叉）→ 保持空仓
    #[test]
    fn trend_signal_conflict_is_silent() {
        let snaps = [snapshot(
            "BTCUSDT",
            Some(series(&gold_cross_main(), &falling_d1())),
            None,
            120.0,
        )];
        assert!(run(&snaps).is_empty(), "趋势与信号冲突时不应开仓");
    }

    #[test]
    fn insufficient_data_is_silent() {
        let main: Vec<f64> = (0..20).map(|i| 100.0 + i as f64).collect();
        let d1: Vec<f64> = (0..20).map(|i| 100.0 + i as f64).collect();
        let snaps = [snapshot("BTCUSDT", Some(series(&main, &d1)), None, 119.0)];
        assert!(run(&snaps).is_empty(), "任一周期数据不足应保持沉默");
    }

    #[test]
    fn stop_hit_closes_position() {
        // 多头 stop=98，1m close=95 → 止损离场（不需要 D1/4h 数据也生效）
        let snaps = [snapshot(
            "BTCUSDT",
            Some(series(&gold_cross_main(), &rising_d1())),
            long_position(98.0),
            95.0,
        )];
        let actions = run(&snaps);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            InstrumentAction::Close {
                is_stop_loss,
                reason,
                ..
            } => {
                assert!(*is_stop_loss);
                assert_eq!(reason, "ATR 止损触发");
            }
            a => panic!("应止损平仓，实际: {a:?}"),
        }
    }

    /// 缺失已闭合序列（引擎未提供封闭桶）→ 策略保持沉默而不是 panic。
    #[test]
    fn missing_closed_series_is_silent() {
        let snaps = [snapshot("BTCUSDT", None, None, 199.0)];
        assert!(run(&snaps).is_empty());
    }

    /// 4h 反向死叉出场：多头持仓遇下穿事件 → 平仓（非止损）
    #[test]
    fn reverse_cross_exits_long_position() {
        let mut main: Vec<f64> = vec![100.0; 60];
        main.push(80.0); // 死叉
        let snaps = [snapshot(
            "BTCUSDT",
            Some(series(&main, &rising_d1())),
            long_position(95.0),
            98.0,
        )];
        let actions = run(&snaps);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            InstrumentAction::Close {
                is_stop_loss,
                reason,
                ..
            } => {
                assert!(!is_stop_loss, "反转离场不是止损");
                assert!(reason.contains("反向交叉"), "离场原因: {reason}");
            }
            a => panic!("应平仓，实际: {a:?}"),
        }
    }

    /// 同一份代码接受多个快照：增加 universe 行即从单标升级为横截面。
    #[test]
    fn multiple_snapshots_emit_actions_per_instrument() {
        let snaps = [
            snapshot(
                "BTCUSDT",
                Some(series(&gold_cross_main(), &rising_d1())),
                None,
                120.0,
            ),
            snapshot(
                "ETHUSDT",
                Some(series(&gold_cross_main(), &rising_d1())),
                None,
                120.0,
            ),
        ];
        let actions = run(&snaps);
        assert_eq!(
            actions.len(),
            2,
            "两个标的金叉 → 两个开仓动作: {actions:?}"
        );
    }
}
