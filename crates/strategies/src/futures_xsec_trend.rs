//! `futures_xsec_trend` — 期货 1:N（单策略多品种）横截面动量轮换。
//!
//! 1:N 的含义是**一个策略实例同时持有 N 个品种的目标仓位**：
//! - `merge_positions: true`：N 个品种共用一个组合总仓，各自一个子仓；
//! - 引擎按 base TF（配置 `timeframes[0]`，如 15m）闭合计数触发调仓窗口
//!   （`portfolio.rebalance_due`），策略在窗口内按 H1 动量选 `top_k` 做多；
//! - 未入选的持仓调回 0；持仓跌破止损或 H1 趋势转空时不等窗口、立即调回 0。
//!
//! 大周期聚合沿用引擎的 `SessionWindowPolicy`：策略只声明需要 1h，引擎就把
//! 1m 按交易时段切桶聚合成 1h，策略读 `snapshot.closed.closed(H1)`，
//! 不需要也不应该自己从 1m 拼 K 线。
//!
//! 目标手数口径与执行层容量闸门同源：`权重 × max_total_notional × NAV`
//! 除以每手名义（`price × multiplier`），单位与 `SubPosition.qty` 一致。

use anyhow::anyhow;
use serde::Deserialize;

use strategy_core::candle::Timeframe as CoreTimeframe;
use strategy_core::signal::Side;
use strategy_engine::bridge::CandleSeries;
use strategy_engine::core::bar::Timeframe as EngineTimeframe;
use strategy_engine::core::identity::InstrumentId;
use strategy_engine::strategy::model::{MasterPositionState, SubPosition, SubPositionState};
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};
use strategy_indicators::talib;

/// 策略私有参数（配置文件 `params:` 节；拼错字段名启动即报错）。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct XsecTrendParams {
    /// 动量回看根数（H1 周期）
    pub lookback: usize,
    /// 趋势过滤 EMA 周期（H1 周期）
    pub trend_ema: usize,
    /// 趋势过滤周期字符串（必须已出现在 `timeframes` 里）
    pub trend_tf: String,
    /// ATR 周期（base TF）
    pub atr_period: usize,
    /// 初始止损距离 = `atr_stop_mult × ATR`
    pub atr_stop_mult: f64,
    /// 多头入选数量
    pub top_k: usize,
}

impl Default for XsecTrendParams {
    fn default() -> Self {
        Self {
            lookback: 12,
            trend_ema: 60,
            trend_tf: "1h".into(),
            atr_period: 14,
            atr_stop_mult: 2.0,
            top_k: 2,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base_tf: CoreTimeframe,
    pub trend_tf: CoreTimeframe,
    pub lookback: usize,
    pub trend_ema: usize,
    pub atr_period: usize,
    pub atr_stop_mult: f64,
    pub top_k: usize,
}

pub struct FuturesXsecTrend {
    config: Config,
}

impl FuturesXsecTrend {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    pub fn from_params(
        params: Option<&serde_json::Value>,
        base_tf: CoreTimeframe,
    ) -> anyhow::Result<Self> {
        let p: XsecTrendParams = match params {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|e| anyhow!("futures_xsec_trend params 反序列化失败: {e}"))?,
            None => XsecTrendParams::default(),
        };
        let trend_tf = CoreTimeframe::parse(&p.trend_tf)
            .map_err(|e| anyhow!("futures_xsec_trend trend_tf 非法: {e}"))?;
        Ok(Self::new(Config {
            base_tf,
            trend_tf,
            lookback: p.lookback.max(1),
            trend_ema: p.trend_ema.max(1),
            atr_period: p.atr_period.max(1),
            atr_stop_mult: p.atr_stop_mult.max(0.0),
            top_k: p.top_k.max(1),
        }))
    }

    fn signed(sub: &SubPosition) -> f64 {
        match sub.side {
            Side::Long => sub.qty,
            Side::Short => -sub.qty,
        }
    }

    fn active_sub<'a>(
        portfolio: &'a PortfolioContext,
        instrument: &InstrumentId,
    ) -> Option<&'a SubPosition> {
        let key = instrument.namespaced_key();
        portfolio
            .sub_positions
            .iter()
            .find(|p| p.instrument.namespaced_key() == key && p.state != SubPositionState::Closed)
    }

    /// H1 动量：`last / close[lookback 根前] - 1`。
    fn momentum(&self, closed: &CandleSeries) -> Option<f64> {
        let candles = closed.closed(self.config.trend_tf)?;
        if candles.len() < self.config.lookback + 1 {
            return None;
        }
        let n = candles.len();
        let base = candles[n - 1 - self.config.lookback].close;
        let last = candles[n - 1].close;
        if base <= 0.0 || !last.is_finite() {
            return None;
        }
        Some(last / base - 1.0)
    }

    /// H1 趋势过滤：收盘价必须高于 EMA 才允许做多。
    fn trend_up(&self, closed: &CandleSeries) -> Option<bool> {
        let candles = closed.closed(self.config.trend_tf)?;
        if candles.len() < self.config.trend_ema + 1 {
            return None;
        }
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        let ema = talib::ema(&closes, self.config.trend_ema).ok()?;
        let last = *closes.last()?;
        let last_ema = *ema.last()?;
        last_ema.is_finite().then_some(last > last_ema)
    }

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

impl CrossSectionalStrategyPort for FuturesXsecTrend {
    fn strategy_type(&self) -> &str {
        "futures_xsec_trend"
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
        portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        let mut out = Vec::new();
        // 持仓风控先于窗口：硬止损与趋势转空不等调仓窗口。
        let mut risk_closed: std::collections::BTreeSet<String> = Default::default();
        for s in snapshots {
            let Some(pos) = &s.position else { continue };
            let Some(closed) = s.closed.as_ref() else {
                continue;
            };
            let trend_down = self.trend_up(closed) == Some(false);
            let stop_hit = match pos.side {
                Side::Long => s.bar.close <= pos.stop_price,
                Side::Short => s.bar.close >= pos.stop_price,
            };
            if stop_hit || trend_down {
                let key = s.instrument.namespaced_key();
                risk_closed.insert(key);
                out.push(InstrumentAction::Adjust {
                    instrument: s.instrument.clone(),
                    target_qty: 0.0,
                    reason: if stop_hit {
                        "futures_xsec_trend: 硬止损".into()
                    } else {
                        "futures_xsec_trend: H1 趋势转空".into()
                    },
                });
            }
        }

        let flat = portfolio
            .master
            .as_ref()
            .is_none_or(|m| m.state == MasterPositionState::Closed)
            && portfolio
                .sub_positions
                .iter()
                .all(|p| p.state == SubPositionState::Closed);
        if !portfolio.rebalance_due && !flat {
            return out;
        }

        // 候选：未 stale、可入场、H1 趋势向上、动量可计算。
        let mut ranked: Vec<(&InstrumentSnapshot, f64)> = snapshots
            .iter()
            .filter(|s| !s.stale && s.entry_eligible)
            .filter_map(|s| {
                let closed = s.closed.as_ref()?;
                if self.trend_up(closed) != Some(true) {
                    return None;
                }
                self.momentum(closed).map(|m| (s, m))
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.1.total_cmp(&a.1).then_with(|| {
                a.0.instrument
                    .namespaced_key()
                    .cmp(&b.0.instrument.namespaced_key())
            })
        });
        let keep: Vec<String> = ranked
            .iter()
            .take(self.config.top_k)
            .map(|(s, _)| s.instrument.namespaced_key())
            .collect();
        let weight = 1.0 / self.config.top_k as f64;

        for s in snapshots {
            let key = s.instrument.namespaced_key();
            if risk_closed.contains(&key) {
                // 本轮已在风控腿调回 0，不再同轮开仓（避免同 tick 先平后开）。
                continue;
            }
            let price = s.bar.close;
            if price <= 0.0 {
                continue;
            }
            let existing = Self::active_sub(portfolio, &s.instrument);
            if keep.contains(&key) {
                let mult = *portfolio.multipliers.get(&key).unwrap_or(&1.0);
                let unit = (price * mult).max(1e-12);
                let target = weight * portfolio.max_total_notional * portfolio.nav / unit;
                match existing {
                    None => {
                        let Some(closed) = s.closed.as_ref() else {
                            continue;
                        };
                        let Some(atr) = self.atr(closed) else {
                            continue;
                        };
                        let stop_price = price - self.config.atr_stop_mult * atr;
                        if stop_price <= 0.0 {
                            continue;
                        }
                        let mom = self.momentum(closed).unwrap_or(0.0);
                        out.push(InstrumentAction::Open {
                            instrument: s.instrument.clone(),
                            side: Side::Long,
                            price,
                            stop_price,
                            atr,
                            strength: 1.0,
                            reason: format!(
                                "futures_xsec_trend: H1 动量 {:.2}% 入选 top_{}",
                                mom * 100.0,
                                self.config.top_k
                            ),
                        });
                    }
                    Some(sub) => {
                        let current = Self::signed(sub);
                        let on_target =
                            current > 0.0 && (target - current).abs() <= 1e-9 * current.max(1.0);
                        if !on_target {
                            out.push(InstrumentAction::Adjust {
                                instrument: s.instrument.clone(),
                                target_qty: target,
                                reason: format!("futures_xsec_trend: 目标权重 {weight:.4}"),
                            });
                        }
                    }
                }
            } else if let Some(sub) = existing {
                if sub.qty > 1e-12 {
                    out.push(InstrumentAction::Adjust {
                        instrument: s.instrument.clone(),
                        target_qty: 0.0,
                        reason: "futures_xsec_trend: 跌出 top_k".into(),
                    });
                }
            }
        }

        tracing::debug!(
            strategy_id = %portfolio.strategy_id,
            top_k = self.config.top_k,
            eligible = ranked.len(),
            keep = ?keep,
            actions = out.len(),
            "futures_xsec_trend 调仓决策完成"
        );
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use chrono::{NaiveDate, TimeZone, Utc};
    use strategy_core::candle::Candle;
    use strategy_core::strategy::PositionView;
    use strategy_engine::core::bar::{Bar, BarExtensions, BarFinality};
    use strategy_engine::core::identity::{ExchangeId, MarketKind};
    use strategy_engine::core::session::TradingDay;
    use strategy_engine::strategy::features::FeatureMap;

    fn inst(symbol: &str) -> InstrumentId {
        InstrumentId {
            exchange: ExchangeId::new("SHFE"),
            market: MarketKind::CnFutures,
            symbol: symbol.into(),
            product: symbol.into(),
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
        symbol: &str,
        closed: Option<CandleSeries>,
        position: Option<PositionView>,
        price: f64,
    ) -> InstrumentSnapshot {
        InstrumentSnapshot {
            instrument: inst(symbol),
            bar: Bar {
                instrument: inst(symbol),
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

    fn sub(symbol: &str) -> SubPosition {
        SubPosition {
            id: format!("sub_{symbol}"),
            strategy_id: "futures_xsec_trend:shfe".into(),
            master_id: "master_000001".into(),
            instrument: inst(symbol),
            side: Side::Long,
            qty: 2.0,
            entry_price: 100.0,
            stop_price: Some(95.0),
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: None,
            closed_at: None,
            realized_pnl: None,
        }
    }

    fn portfolio(subs: Vec<SubPosition>, due: bool) -> PortfolioContext {
        let mut multipliers = BTreeMap::new();
        for s in ["A", "B", "C"] {
            multipliers.insert(format!("SHFE/cn_futures/{s}"), 10.0);
        }
        PortfolioContext {
            strategy_id: "futures_xsec_trend:shfe".into(),
            nav: 2_000_000.0,
            drawdown: 0.0,
            portfolio_pnl_pct: 0.0,
            sub_positions: subs,
            master: None,
            multipliers,
            max_total_notional: 0.8,
            rebalance_due: due,
            no_trade: false,
        }
    }

    fn config() -> Config {
        Config {
            base_tf: CoreTimeframe::M15,
            trend_tf: CoreTimeframe::H1,
            lookback: 12,
            trend_ema: 5,
            atr_period: 3,
            atr_stop_mult: 2.0,
            top_k: 2,
        }
    }

    fn rising(n: usize, step: f64) -> Vec<f64> {
        (0..n).map(|i| 100.0 + i as f64 * step).collect()
    }

    fn falling(n: usize) -> Vec<f64> {
        (0..n).map(|i| 200.0 - i as f64).collect()
    }

    fn flat_base() -> Vec<f64> {
        vec![100.0; 30]
    }

    fn action_symbol(action: &InstrumentAction) -> String {
        action
            .instrument_key()
            .and_then(|k| k.rsplit('/').next().map(String::from))
            .unwrap_or_default()
    }

    #[test]
    fn rebalance_opens_top_k_by_h1_momentum() {
        let mut s = FuturesXsecTrend::new(config());
        let snaps = vec![
            snapshot("A", Some(series(&flat_base(), &rising(30, 2.0))), None, 100.0),
            snapshot("B", Some(series(&flat_base(), &rising(30, 1.0))), None, 100.0),
            snapshot("C", Some(series(&flat_base(), &rising(30, 0.1))), None, 100.0),
        ];
        let out = s.on_cross_section_bars(&[], &snaps, &portfolio(Vec::new(), true));
        assert_eq!(out.len(), 2);
        let mut symbols: Vec<String> = out.iter().map(action_symbol).collect();
        symbols.sort();
        assert_eq!(symbols, vec!["A".to_string(), "B".to_string()]);
        assert!(out
            .iter()
            .all(|a| matches!(a, InstrumentAction::Open { .. })));
    }

    #[test]
    fn non_rebalance_with_position_holds() {
        let mut s = FuturesXsecTrend::new(config());
        let snaps = vec![snapshot(
            "A",
            Some(series(&flat_base(), &rising(30, 2.0))),
            None,
            100.0,
        )];
        let out = s.on_cross_section_bars(&[], &snaps, &portfolio(vec![sub("A")], false));
        assert!(out.is_empty(), "未到调仓窗口且已有持仓应保持");
    }

    #[test]
    fn dropped_out_of_top_k_adjusts_to_zero() {
        let mut s = FuturesXsecTrend::new(config());
        let snaps = vec![
            snapshot("A", Some(series(&flat_base(), &rising(30, 2.0))), None, 100.0),
            snapshot("B", Some(series(&flat_base(), &rising(30, 1.0))), None, 100.0),
            snapshot("C", Some(series(&flat_base(), &rising(30, 0.1))), None, 100.0),
        ];
        let out = s.on_cross_section_bars(&[], &snaps, &portfolio(vec![sub("C")], true));
        let c_zero = out.iter().any(|a| {
            action_symbol(a) == "C"
                && matches!(a, InstrumentAction::Adjust { target_qty, .. } if *target_qty == 0.0)
        });
        assert!(c_zero, "跌出 top_k 的持仓必须调回 0");
    }

    #[test]
    fn trend_down_candidate_is_excluded() {
        let mut s = FuturesXsecTrend::new(config());
        let snaps = vec![
            snapshot("A", Some(series(&flat_base(), &falling(30))), None, 100.0),
            snapshot("B", Some(series(&flat_base(), &rising(30, 1.0))), None, 100.0),
        ];
        let cfg = Config { top_k: 1, ..config() };
        let mut s1 = FuturesXsecTrend::new(cfg);
        let out = s1.on_cross_section_bars(&[], &snaps, &portfolio(Vec::new(), true));
        assert_eq!(out.len(), 1);
        assert_eq!(action_symbol(&out[0]), "B");
        // 原实例未被调用，只是为了保持 mut 绑定使用一次。
        let _ = s
            .on_cross_section_bars(&[], &[], &portfolio(Vec::new(), true));
    }

    #[test]
    fn stop_hit_adjusts_to_zero_before_window() {
        let mut s = FuturesXsecTrend::new(config());
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
            "A",
            Some(series(&flat_base(), &rising(30, 2.0))),
            Some(pos),
            90.0,
        )];
        let out = s.on_cross_section_bars(&[], &snaps, &portfolio(vec![sub("A")], false));
        assert_eq!(out.len(), 1);
        match &out[0] {
            InstrumentAction::Adjust { target_qty, .. } => assert_eq!(*target_qty, 0.0),
            other => panic!("必须调回 0，实际 {other:?}"),
        }
    }

    #[test]
    fn from_params_rejects_unknown_keys() {
        let bad = serde_json::json!({ "topk": 2 });
        assert!(FuturesXsecTrend::from_params(Some(&bad), CoreTimeframe::M15).is_err());
        let good = serde_json::json!({ "top_k": 2, "lookback": 12 });
        assert!(FuturesXsecTrend::from_params(Some(&good), CoreTimeframe::M15).is_ok());
    }
}

crate::register_strategy! {
    name: "futures_xsec_trend",
    factory: |args| {
        let base_tf = CoreTimeframe::parse(args.timeframe)?;
        let _ = (args.symbol, args.cash);
        Ok(Box::new(FuturesXsecTrend::from_params(args.params, base_tf)?))
    }
}
