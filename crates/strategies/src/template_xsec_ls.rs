//! `template_xsec_ls` — 多空横截面轮动模板（前 `top_k` 做多、后 `bottom_k` 做空）。
//!
//! 排名指标由 `rank_indicator` 指定（必须是 `indicators` 已声明的特征）；
//! 多头取分数靠前的一组，空头取分数靠后的一组；配置 `trend_tf` 时按
//! 收盘价与 EMA 的同向关系过滤：多头只接受收盘 > EMA，空头只接受收盘 < EMA。
//! 止损按方向对称：多头止损在价格下方，空头止损在价格上方。
//!
//! 多空各占 `risk.max_total_notional` 的一半预算（`bottom_k > 0` 时），
//! 每侧等权，保证多空名义大致中性；`bottom_k == 0` 退化为只做多并独占预算。
//!
//! 反手限制：引擎不支持同轮多↔空翻仓（`dispatch.rs` 对异号目标 soft-reject）。
//! 本模板在调仓轮把反向持仓拆成 `Close`（先平）与 `Open`（后开）两条动作，
//! 由运行时的 Phase 1 减仓 / Phase 2 加仓串行完成。

use std::collections::BTreeSet;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    #[default]
    Percent,
    Atr,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct XsecLsParams {
    /// 排名特征名（`indicators` 已声明的键，如 `macd`）。
    pub rank_indicator: String,
    /// true = 分数越高越优先做多；空头始终取相反端。
    pub higher_is_better: bool,
    pub top_k: usize,
    pub bottom_k: usize,
    /// 空仓首建不受调仓窗口限制。
    pub flat_entry: bool,
    /// 趋势过滤周期（空 = 不过滤）。
    pub trend_tf: String,
    pub trend_ema: usize,
    pub stop_mode: StopMode,
    pub stop_loss_pct: f64,
    pub atr_period: usize,
    pub atr_stop_mult: f64,
}

impl Default for XsecLsParams {
    fn default() -> Self {
        Self {
            rank_indicator: "macd".into(),
            higher_is_better: true,
            top_k: 2,
            bottom_k: 0,
            flat_entry: true,
            trend_tf: String::new(),
            trend_ema: 60,
            stop_mode: StopMode::Percent,
            stop_loss_pct: 2.0,
            atr_period: 14,
            atr_stop_mult: 2.0,
        }
    }
}

pub struct TemplateXsecLs {
    params: XsecLsParams,
    trigger_tf: CoreTimeframe,
    trend_set: Option<CoreTimeframe>,
}
/// 排名候选：快照引用 + 分数。
type Ranked<'a> = Vec<(&'a InstrumentSnapshot, f64)>;

impl TemplateXsecLs {
    pub fn new(
        params: XsecLsParams,
        trigger_tf: CoreTimeframe,
        trend_tf: Option<CoreTimeframe>,
    ) -> Self {
        Self {
            params,
            trigger_tf,
            trend_set: trend_tf,
        }
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

    fn flat(portfolio: &PortfolioContext) -> bool {
        portfolio
            .master
            .as_ref()
            .is_none_or(|m| m.state == MasterPositionState::Closed)
            && portfolio
                .sub_positions
                .iter()
                .all(|p| p.state == SubPositionState::Closed)
    }

    /// 趋势方向：1 = 收盘 > EMA（允许做多），-1 = 收盘 < EMA（允许做空），
    /// 0 = 相等；`None` = 未配置过滤或数据不足。
    fn trend_dir(&self, closed: Option<&CandleSeries>) -> Option<i8> {
        let tf = self.trend_set?;
        let candles = closed?.closed(tf)?;
        if candles.len() < self.params.trend_ema + 1 {
            return None;
        }
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        let ema = talib::ema(&closes, self.params.trend_ema).ok()?;
        let last = closes.last().copied()?;
        let last_ema = *ema.last()?;
        Some(if last > last_ema {
            1
        } else if last < last_ema {
            -1
        } else {
            0
        })
    }

    fn trend_allows(&self, closed: Option<&CandleSeries>, side: Side) -> bool {
        if self.trend_set.is_none() {
            return true;
        }
        matches!(
            (self.trend_dir(closed), side),
            (Some(1), Side::Long) | (Some(-1), Side::Short)
        )
    }

    fn atr(&self, closed: Option<&CandleSeries>) -> Option<f64> {
        let candles = closed?.closed(self.trigger_tf)?;
        if candles.len() < self.params.atr_period + 1 {
            return None;
        }
        let highs: Vec<f64> = candles.iter().map(|c| c.high).collect();
        let lows: Vec<f64> = candles.iter().map(|c| c.low).collect();
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        talib::atr(&highs, &lows, &closes, self.params.atr_period)
            .ok()?
            .last()
            .copied()
            .filter(|v| v.is_finite() && *v > 0.0)
    }

    /// 方向化止损：多头在价格下方，空头在价格上方。
    fn stop_price(&self, s: &InstrumentSnapshot, side: Side) -> Option<f64> {
        let price = s.bar.close;
        let sign = match side {
            Side::Long => 1.0,
            Side::Short => -1.0,
        };
        let stop = match self.params.stop_mode {
            StopMode::Percent => price * (1.0 - sign * self.params.stop_loss_pct / 100.0),
            StopMode::Atr => {
                let atr = self.atr(s.closed.as_ref())?;
                price - sign * self.params.atr_stop_mult * atr
            }
        };
        (stop > 0.0).then_some(stop)
    }

    fn open(&self, s: &InstrumentSnapshot, side: Side, reason: String) -> Option<InstrumentAction> {
        let stop_price = self.stop_price(s, side)?;
        Some(InstrumentAction::Open {
            instrument: s.instrument.clone(),
            side,
            price: s.bar.close,
            stop_price,
            atr: 0.0,
            strength: 1.0,
            reason,
        })
    }

    /// 命中持仓止损时返回 StopLoss 平仓；非 Active（在途）不动作。
    /// 横截面运行时不自动检查 `stop_price`，由策略逐 bar 判低点/高点触发。
    fn stop_close(&self, s: &InstrumentSnapshot, sub: &SubPosition) -> Option<InstrumentAction> {
        if sub.state != SubPositionState::Active {
            return None;
        }
        let stop = sub.stop_price?;
        let hit = match sub.side {
            Side::Long => s.bar.low <= stop,
            Side::Short => s.bar.high >= stop,
        };
        if !hit {
            return None;
        }
        Some(InstrumentAction::Close {
            instrument: s.instrument.clone(),
            price: s.bar.close,
            reason: format!("template_xsec_ls: stop {:?}", sub.side),
            is_stop_loss: true,
            strength: 1.0,
        })
    }

    /// 每组权重（占 NAV 比例）：底部做空时多空各占预算一半。
    fn weights(&self, portfolio: &PortfolioContext) -> (f64, f64) {
        let top_k = self.params.top_k.max(1);
        let bottom_k = self.params.bottom_k;
        if bottom_k == 0 {
            (portfolio.max_total_notional / top_k as f64, 0.0)
        } else {
            let long = 0.5 * portfolio.max_total_notional / top_k as f64;
            let short = 0.5 * portfolio.max_total_notional / bottom_k as f64;
            (long, short)
        }
    }

    fn ranked<'a>(&self, snapshots: &'a [InstrumentSnapshot]) -> (Ranked<'a>, Ranked<'a>) {
        let bottom_k = self.params.bottom_k;
        let mut longs: Ranked<'a> = Vec::new();
        let mut shorts: Ranked<'a> = Vec::new();
        for s in snapshots {
            if s.stale || !s.entry_eligible {
                continue;
            }
            let Some(score) = s.features.number(&self.params.rank_indicator) else {
                continue;
            };
            if !score.is_finite() {
                continue;
            }
            if self.trend_allows(s.closed.as_ref(), Side::Long) {
                longs.push((s, score));
            }
            if bottom_k > 0 && self.trend_allows(s.closed.as_ref(), Side::Short) {
                shorts.push((s, score));
            }
        }
        let higher = self.params.higher_is_better;
        longs.sort_by(|a, b| {
            let by_value = if higher {
                b.1.total_cmp(&a.1)
            } else {
                a.1.total_cmp(&b.1)
            };
            by_value.then_with(|| {
                a.0.instrument
                    .namespaced_key()
                    .cmp(&b.0.instrument.namespaced_key())
            })
        });
        // 空头取相反端：分数越差越优先；相同分数按 key 逆序取尾部，
        // 与多头正序取头部对称，避免全同分时两侧选中同一批标的。
        shorts.sort_by(|a, b| {
            let by_value = if higher {
                a.1.total_cmp(&b.1)
            } else {
                b.1.total_cmp(&a.1)
            };
            by_value.then_with(|| {
                b.0.instrument
                    .namespaced_key()
                    .cmp(&a.0.instrument.namespaced_key())
            })
        });
        (longs, shorts)
    }
}

impl CrossSectionalStrategyPort for TemplateXsecLs {
    fn strategy_type(&self) -> &str {
        "template_xsec_ls"
    }

    fn timeframes(&self) -> Vec<EngineTimeframe> {
        let mut tfs = std::collections::BTreeSet::new();
        tfs.insert(EngineTimeframe::M1);
        tfs.insert(EngineTimeframe::from(self.trigger_tf));
        if let Some(t) = self.trend_set {
            tfs.insert(EngineTimeframe::from(t));
        }
        tfs.into_iter().collect()
    }

    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[EngineTimeframe],
        snapshots: &[InstrumentSnapshot],
        portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        if portfolio
            .master
            .as_ref()
            .is_some_and(|m| m.state == MasterPositionState::Closing)
        {
            return Vec::new();
        }
        if snapshots.len() == 1 {
            let s = &snapshots[0];
            if let Some(sub) = Self::active_sub(portfolio, &s.instrument) {
                return self.stop_close(s, sub).into_iter().collect();
            }
            if s.stale || !s.entry_eligible {
                return Vec::new();
            }
            let side = if self.trend_allows(s.closed.as_ref(), Side::Long) {
                Side::Long
            } else if self.params.bottom_k > 0 && self.trend_allows(s.closed.as_ref(), Side::Short)
            {
                Side::Short
            } else {
                return Vec::new();
            };
            return self
                .open(s, side, format!("template_xsec_ls: single entry {side:?}"))
                .into_iter()
                .collect();
        }

        // 止损检查每根 bar 都做，不能被调仓窗口挡住。
        let mut out = Vec::new();
        let mut stopped = BTreeSet::new();
        for s in snapshots.iter() {
            if let Some(sub) = Self::active_sub(portfolio, &s.instrument) {
                if let Some(a) = self.stop_close(s, sub) {
                    out.push(a);
                    stopped.insert(s.instrument.namespaced_key());
                }
            }
        }

        let flat = Self::flat(portfolio);
        if !portfolio.rebalance_due && !(self.params.flat_entry && flat) {
            return out;
        }

        let top_k = self.params.top_k.max(1);
        let bottom_k = self.params.bottom_k;
        let (longs, shorts) = self.ranked(snapshots);
        let long_keys: Vec<String> = longs
            .iter()
            .take(top_k)
            .map(|(s, _)| s.instrument.namespaced_key())
            .collect();
        let long_set: BTreeSet<String> = long_keys.iter().cloned().collect();
        let mut short_keys: Vec<String> = Vec::new();
        for (s, _) in shorts.iter() {
            if short_keys.len() >= bottom_k {
                break;
            }
            let key = s.instrument.namespaced_key();
            if !long_set.contains(&key) {
                short_keys.push(key);
            }
        }
        let short_set: BTreeSet<String> = short_keys.iter().cloned().collect();
        let (long_weight, short_weight) = self.weights(portfolio);

        for s in snapshots.iter() {
            let key = s.instrument.namespaced_key();
            if stopped.contains(&key) {
                continue;
            }
            let desired = if long_set.contains(&key) {
                Some(Side::Long)
            } else if short_set.contains(&key) {
                Some(Side::Short)
            } else {
                None
            };
            let price = s.bar.close;
            let mult = *portfolio.multipliers.get(&key).unwrap_or(&1.0);
            let existing = Self::active_sub(portfolio, &s.instrument);
            match desired {
                Some(side) => match existing {
                    None => {
                        if let Some(a) =
                            self.open(s, side, format!("template_xsec_ls: rank in {side:?} set"))
                        {
                            out.push(a);
                        }
                    }
                    Some(sub) if sub.side == side => {
                        if price > 0.0 && mult > 0.0 {
                            let weight = match side {
                                Side::Long => long_weight,
                                Side::Short => short_weight,
                            };
                            let sign = match side {
                                Side::Long => 1.0,
                                Side::Short => -1.0,
                            };
                            let target = sign * weight * portfolio.nav / (price * mult);
                            let current = Self::signed(sub);
                            let on_target =
                                (target - current).abs() <= 1e-9 * current.abs().max(1.0);
                            if !on_target {
                                out.push(InstrumentAction::Adjust {
                                    instrument: s.instrument.clone(),
                                    target_qty: target,
                                    reason: format!(
                                        "template_xsec_ls: rebalance {side:?} weight {weight:.4}"
                                    ),
                                });
                            }
                        }
                    }
                    Some(_) => {
                        // 反向持仓：只在调仓轮拆成「先平后开」，借引擎
                        // Phase 1（减仓）/ Phase 2（加仓）串行完成。
                        // 目标侧止损算不出来时保持原仓，避免只平不开。
                        if portfolio.rebalance_due {
                            if let Some(a) =
                                self.open(s, side, format!("template_xsec_ls: flip open {side:?}"))
                            {
                                out.push(InstrumentAction::Close {
                                    instrument: s.instrument.clone(),
                                    price,
                                    reason: "template_xsec_ls: flip close".into(),
                                    is_stop_loss: false,
                                    strength: 1.0,
                                });
                                out.push(a);
                            }
                        }
                    }
                },
                None => {
                    if let Some(sub) = existing {
                        // stale 或特征暂缺时无法判定相对排名，保持仓位避免误平。
                        let rankable =
                            !s.stale && s.features.number(&self.params.rank_indicator).is_some();
                        if rankable && sub.qty > 1e-12 {
                            out.push(InstrumentAction::Adjust {
                                instrument: s.instrument.clone(),
                                target_qty: 0.0,
                                reason: "template_xsec_ls: dropped out of selection".into(),
                            });
                        }
                    }
                }
            }
        }
        out
    }
}

crate::register_strategy! {
    name: "template_xsec_ls",
    factory: |args| {
        let params: XsecLsParams = crate::registry::params_or_default(args.params)?;
        let trigger = CoreTimeframe::parse(args.timeframe)?;
        let trend = if params.trend_tf.trim().is_empty() {
            None
        } else {
            Some(CoreTimeframe::parse(&params.trend_tf)?)
        };
        Ok(Box::new(TemplateXsecLs::new(params, trigger, trend)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use strategy_engine::core::bar::{Bar, BarExtensions, BarFinality};
    use strategy_engine::core::identity::{ExchangeId, InstrumentId};
    use strategy_engine::core::session::TradingDay;
    use strategy_engine::strategy::features::FeatureMap;
    use strategy_engine::strategy::model::SubPosition;

    fn inst(sym: &str) -> InstrumentId {
        InstrumentId::perpetual(&ExchangeId::new("binance"), sym)
    }

    fn candle(close: f64, minutes: i64) -> strategy_core::candle::Candle {
        strategy_core::candle::Candle {
            open_time: chrono::Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap()
                + chrono::Duration::minutes(minutes),
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
            quote_volume: 0.0,
            trade_num: 0,
            active_buy_volume: 0.0,
            active_buy_quote_volume: 0.0,
            is_closed: true,
        }
    }

    fn trend_series(closes: &[f64]) -> CandleSeries {
        CandleSeries::from_closed(
            CoreTimeframe::H1,
            closes
                .iter()
                .enumerate()
                .map(|(i, c)| candle(*c, i as i64 * 60))
                .collect(),
        )
    }

    fn flat_series() -> CandleSeries {
        CandleSeries::from_closed(CoreTimeframe::M1, vec![candle(100.0, 0)])
    }

    fn snapshot_with(sym: &str, macd: f64, closed: CandleSeries) -> InstrumentSnapshot {
        let mut features = FeatureMap::default();
        features.values.insert("macd".into(), macd);
        InstrumentSnapshot {
            instrument: inst(sym),
            bar: Bar {
                instrument: inst(sym),
                open_time: chrono::Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
                open: 100.0,
                high: 100.0,
                low: 100.0,
                close: 100.0,
                volume: 1.0,
                finality: BarFinality::Final,
                extensions: BarExtensions::default(),
            },
            position: None,
            stale: false,
            features,
            entry_eligible: true,
            entry_reason: None,
            exit_triggered: None,
            trading_day: TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
            no_trade: false,
            entry_confirm: false,
            signal_resume: false,
            cooldown: None,
            session_open_price: None,
            closed: Some(closed),
        }
    }

    fn snapshot(sym: &str, macd: f64) -> InstrumentSnapshot {
        snapshot_with(sym, macd, flat_series())
    }

    fn portfolio(subs: Vec<SubPosition>, rebalance_due: bool) -> PortfolioContext {
        PortfolioContext {
            strategy_id: "t".into(),
            nav: 1000.0,
            drawdown: 0.0,
            portfolio_pnl_pct: 0.0,
            sub_positions: subs,
            master: None,
            multipliers: Default::default(),
            max_total_notional: 1.0,
            rebalance_due,
            no_trade: false,
        }
    }

    fn params(top_k: usize, bottom_k: usize) -> XsecLsParams {
        XsecLsParams {
            top_k,
            bottom_k,
            flat_entry: false,
            ..XsecLsParams::default()
        }
    }

    fn active_long(sym: &str, qty: f64) -> SubPosition {
        SubPosition {
            id: format!("sub_{sym}"),
            strategy_id: "t".into(),
            master_id: "m".into(),
            instrument: inst(sym),
            side: Side::Long,
            qty,
            entry_price: 100.0,
            stop_price: Some(95.0),
            target_weight: 0.5,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: None,
            closed_at: None,
            realized_pnl: None,
        }
    }

    fn active_short(sym: &str, qty: f64) -> SubPosition {
        SubPosition {
            side: Side::Short,
            stop_price: Some(105.0),
            ..active_long(sym, qty)
        }
    }

    #[test]
    fn long_stop_hit_emits_stop_loss_close() {
        let mut st = TemplateXsecLs::new(params(1, 1), CoreTimeframe::M1, None);
        let mut s = snapshot("AAA", 5.0);
        s.bar.low = 90.0;
        let out = st.on_cross_section_bars(
            &[],
            &[s, snapshot("BBB", 1.0)],
            // 非调仓轮也必须检查止损。
            &portfolio(vec![active_long("AAA", 10.0)], false),
        );
        assert!(
            out.iter().any(|a| matches!(
                a,
                InstrumentAction::Close {
                    is_stop_loss: true,
                    ..
                }
            )),
            "多头低点破止损应平仓: {out:?}"
        );
        assert!(
            !out.iter().any(|a| matches!(
                a,
                InstrumentAction::Open { instrument, .. } if instrument.symbol == "AAA"
            )),
            "止损当轮不得再开同标的: {out:?}"
        );
    }

    #[test]
    fn short_stop_hit_emits_stop_loss_close() {
        let mut st = TemplateXsecLs::new(params(1, 1), CoreTimeframe::M1, None);
        let mut s = snapshot("BBB", 1.0);
        s.bar.high = 110.0;
        let out = st.on_cross_section_bars(
            &[],
            &[snapshot("AAA", 5.0), s],
            &portfolio(vec![active_short("BBB", 10.0)], true),
        );
        assert!(
            out.iter().any(|a| matches!(
                a,
                InstrumentAction::Close {
                    is_stop_loss: true,
                    ..
                }
            )),
            "空头高点破止损应平仓: {out:?}"
        );
    }

    #[test]
    fn selects_top_for_long_and_bottom_for_short() {
        let mut st = TemplateXsecLs::new(params(2, 2), CoreTimeframe::M1, None);
        let out = st.on_cross_section_bars(
            &[],
            &[
                snapshot("AAA", 1.0),
                snapshot("BBB", 2.0),
                snapshot("CCC", 3.0),
                snapshot("DDD", 4.0),
                snapshot("EEE", 5.0),
            ],
            &portfolio(Vec::new(), true),
        );
        let long: Vec<&str> = out
            .iter()
            .filter_map(|a| match a {
                InstrumentAction::Open {
                    instrument,
                    side: Side::Long,
                    ..
                } => Some(instrument.symbol.as_str()),
                _ => None,
            })
            .collect();
        let short: Vec<&str> = out
            .iter()
            .filter_map(|a| match a {
                InstrumentAction::Open {
                    instrument,
                    side: Side::Short,
                    ..
                } => Some(instrument.symbol.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(long, vec!["DDD", "EEE"], "前两名做多");
        assert_eq!(short, vec!["AAA", "BBB"], "后两名做空");
    }

    #[test]
    fn equal_scores_split_heads_and_tails() {
        let mut st = TemplateXsecLs::new(params(2, 3), CoreTimeframe::M1, None);
        let out = st.on_cross_section_bars(
            &[],
            &[
                snapshot("AAA", 1.0),
                snapshot("BBB", 1.0),
                snapshot("CCC", 1.0),
                snapshot("DDD", 1.0),
                snapshot("EEE", 1.0),
            ],
            &portfolio(Vec::new(), true),
        );
        let long: Vec<&str> = out
            .iter()
            .filter_map(|a| match a {
                InstrumentAction::Open {
                    instrument,
                    side: Side::Long,
                    ..
                } => Some(instrument.symbol.as_str()),
                _ => None,
            })
            .collect();
        let short: Vec<&str> = out
            .iter()
            .filter_map(|a| match a {
                InstrumentAction::Open {
                    instrument,
                    side: Side::Short,
                    ..
                } => Some(instrument.symbol.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(long, vec!["AAA", "BBB"], "同分时多头取 key 头部");
        // 输出顺序按 snapshot key 升序；集合来自空头侧 key 尾部。
        let mut short_sorted = short.clone();
        short_sorted.sort();
        assert_eq!(
            short_sorted,
            vec!["CCC", "DDD", "EEE"],
            "同分时空头取 key 尾部"
        );
    }

    #[test]
    fn bottom_k_zero_is_long_only() {
        let mut st = TemplateXsecLs::new(params(1, 0), CoreTimeframe::M1, None);
        let out = st.on_cross_section_bars(
            &[],
            &[snapshot("AAA", 5.0), snapshot("BBB", 1.0)],
            &portfolio(Vec::new(), true),
        );
        assert_eq!(out.len(), 1);
        assert!(matches!(
            &out[0],
            InstrumentAction::Open {
                side: Side::Long,
                ..
            }
        ));
    }

    #[test]
    fn trend_filter_routes_long_up_short_down() {
        let mut st = TemplateXsecLs::new(
            XsecLsParams {
                trend_tf: "1h".into(),
                trend_ema: 2,
                top_k: 1,
                bottom_k: 1,
                flat_entry: false,
                ..XsecLsParams::default()
            },
            CoreTimeframe::M1,
            Some(CoreTimeframe::H1),
        );
        let out = st.on_cross_section_bars(
            &[],
            &[
                snapshot_with("UP", 1.0, trend_series(&[1.0, 2.0, 3.0])),
                snapshot_with("DOWN", 5.0, trend_series(&[3.0, 2.0, 1.0])),
            ],
            &portfolio(Vec::new(), true),
        );
        assert_eq!(out.len(), 2, "上涨标的多、下跌标的空: {out:?}");
        assert!(out.iter().any(|a| matches!(
            a,
            InstrumentAction::Open { instrument, side: Side::Long, .. }
                if instrument.symbol == "UP"
        )));
        assert!(out.iter().any(|a| matches!(
            a,
            InstrumentAction::Open { instrument, side: Side::Short, .. }
                if instrument.symbol == "DOWN"
        )));
    }

    #[test]
    fn short_stop_is_above_entry() {
        let mut st = TemplateXsecLs::new(params(0, 1), CoreTimeframe::M1, None);
        let out = st.on_cross_section_bars(
            &[],
            &[snapshot("AAA", 5.0), snapshot("BBB", 1.0)],
            &portfolio(Vec::new(), true),
        );
        let stop = out.iter().find_map(|a| match a {
            InstrumentAction::Open {
                side: Side::Short,
                stop_price,
                ..
            } => Some(*stop_price),
            _ => None,
        });
        let stop = stop.expect("末位仍应做空");
        assert!(stop > 100.0, "空头止损必须在价格上方: {stop}");
    }

    #[test]
    fn dropped_out_of_selection_adjusts_to_zero() {
        let mut st = TemplateXsecLs::new(params(1, 0), CoreTimeframe::M1, None);
        let out = st.on_cross_section_bars(
            &[],
            &[snapshot("AAA", 1.0), snapshot("BBB", 5.0)],
            &portfolio(vec![active_long("AAA", 10.0)], true),
        );
        assert!(
            out.iter().any(|a| matches!(
                a,
                InstrumentAction::Adjust { target_qty, .. } if *target_qty == 0.0
            )),
            "跌出选中集合必须调回 0: {out:?}"
        );
    }

    #[test]
    fn opposite_holding_flips_with_close_then_open() {
        let mut st = TemplateXsecLs::new(params(1, 1), CoreTimeframe::M1, None);
        let out = st.on_cross_section_bars(
            &[],
            &[snapshot("AAA", 5.0), snapshot("BBB", 1.0)],
            &portfolio(vec![active_long("BBB", 10.0)], true),
        );
        let pos = out
            .iter()
            .position(|a| matches!(a, InstrumentAction::Close { .. }))
            .expect("反向持仓应先平");
        assert!(matches!(
            &out[pos + 1],
            InstrumentAction::Open {
                side: Side::Short,
                ..
            }
        ));
    }
}
