//! `template_xsec` — 横截面轮动模板（N=1 退化为单标买入持有）。
//!
//! 合并原 `xsec_template`（参数化排名 + 单标分支）、`xsec_momentum`
//! （空仓首建 + MACD 排名）与 `futures_xsec_trend`（趋势过滤 + ATR 止损）：
//! 排名指标由 `rank_indicator` 指定（必须是 `indicators` 已声明的特征），
//! 趋势过滤 / 止损方式 / 首建窗口语义均由参数选择。

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
pub struct XsecParams {
    /// 排名特征名（`indicators` 已声明的键，如 `macd`）。
    pub rank_indicator: String,
    pub higher_is_better: bool,
    pub top_k: usize,
    /// 空仓首建不受调仓窗口限制（原 `xsec_momentum` 行为）。
    pub flat_entry: bool,
    /// 趋势过滤周期（空 = 不过滤）。
    pub trend_tf: String,
    pub trend_ema: usize,
    pub stop_mode: StopMode,
    pub stop_loss_pct: f64,
    pub atr_period: usize,
    pub atr_stop_mult: f64,
}

impl Default for XsecParams {
    fn default() -> Self {
        Self {
            rank_indicator: "macd".into(),
            higher_is_better: true,
            top_k: 3,
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

pub struct TemplateXsec {
    params: XsecParams,
    trigger_tf: CoreTimeframe,
    trend_set: Option<CoreTimeframe>,
}

impl TemplateXsec {
    pub fn new(
        params: XsecParams,
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

    fn trend_allows_long(&self, closed: &CandleSeries) -> Option<bool> {
        let tf = self.trend_set?;
        let candles = closed.closed(tf)?;
        if candles.len() < self.params.trend_ema + 1 {
            return None;
        }
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        let ema = talib::ema(&closes, self.params.trend_ema).ok()?;
        Some(closes.last().copied()? > *ema.last()?)
    }

    fn atr(&self, closed: &CandleSeries) -> Option<f64> {
        let candles = closed.closed(self.trigger_tf)?;
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

    fn stop_price(&self, s: &InstrumentSnapshot) -> Option<f64> {
        let price = s.bar.close;
        match self.params.stop_mode {
            StopMode::Percent => Some(price * (1.0 - self.params.stop_loss_pct / 100.0)),
            StopMode::Atr => {
                let closed = s.closed.as_ref()?;
                let atr = self.atr(closed)?;
                Some(price - self.params.atr_stop_mult * atr)
            }
        }
    }

    fn open(&self, s: &InstrumentSnapshot, reason: String) -> Option<InstrumentAction> {
        let stop_price = self.stop_price(s)?;
        if stop_price <= 0.0 {
            return None;
        }
        Some(InstrumentAction::Open {
            instrument: s.instrument.clone(),
            side: Side::Long,
            price: s.bar.close,
            stop_price,
            atr: 0.0,
            strength: 1.0,
            reason,
        })
    }
}

impl CrossSectionalStrategyPort for TemplateXsec {
    fn strategy_type(&self) -> &str {
        "template_xsec"
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
        if snapshots.len() == 1 {
            let s = &snapshots[0];
            if s.stale || !s.entry_eligible {
                return Vec::new();
            }
            if Self::active_sub(portfolio, &s.instrument).is_some() {
                return Vec::new();
            }
            return self
                .open(s, "template_xsec: single entry".into())
                .into_iter()
                .collect();
        }

        let flat = Self::flat(portfolio);
        if !portfolio.rebalance_due && !(self.params.flat_entry && flat) {
            return Vec::new();
        }

        let mut ranked: Vec<(&InstrumentSnapshot, f64)> = snapshots
            .iter()
            .filter(|s| !s.stale && s.entry_eligible)
            .filter(|s| {
                self.trend_set.is_none()
                    || s.closed
                        .as_ref()
                        .and_then(|c| self.trend_allows_long(c))
                        .unwrap_or(false)
            })
            .filter_map(|s| {
                s.features
                    .number(&self.params.rank_indicator)
                    .map(|v| (s, v))
            })
            .collect();
        ranked.sort_by(|a, b| {
            let by_value = if self.params.higher_is_better {
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
        let top_k = self.params.top_k.max(1);
        let keep: Vec<String> = ranked
            .iter()
            .take(top_k)
            .map(|(s, _)| s.instrument.namespaced_key())
            .collect();
        let weight = 1.0 / top_k as f64;

        let mut out = Vec::new();
        for s in snapshots.iter() {
            let key = s.instrument.namespaced_key();
            let price = s.bar.close;
            let mult = *portfolio.multipliers.get(&key).unwrap_or(&1.0);
            let existing = Self::active_sub(portfolio, &s.instrument);
            if keep.contains(&key) {
                match existing {
                    None => {
                        if let Some(a) = self.open(s, format!("template_xsec: rank in top_{top_k}"))
                        {
                            out.push(a);
                        }
                    }
                    Some(sub) => {
                        let target = weight * portfolio.max_total_notional * portfolio.nav
                            / (price * mult);
                        let current = Self::signed(sub);
                        let on_target =
                            current > 0.0 && (target - current).abs() <= 1e-9 * current.max(1.0);
                        if !on_target {
                            out.push(InstrumentAction::Adjust {
                                instrument: s.instrument.clone(),
                                target_qty: target,
                                reason: format!("template_xsec: rebalance weight {weight:.4}"),
                            });
                        }
                    }
                }
            } else if let Some(sub) = existing {
                // stale 或特征暂缺时无法判定相对排名，保持仓位避免误平。
                let rankable =
                    !s.stale && s.features.number(&self.params.rank_indicator).is_some();
                if rankable && sub.qty > 1e-12 {
                    out.push(InstrumentAction::Adjust {
                        instrument: s.instrument.clone(),
                        target_qty: 0.0,
                        reason: "template_xsec: dropped out of top_k".into(),
                    });
                }
            }
        }
        out
    }
}

crate::register_strategy! {
    name: "template_xsec",
    factory: |args| {
        let params: XsecParams = crate::registry::params_or_default(args.params)?;
        let trigger = CoreTimeframe::parse(args.timeframe)?;
        let trend = if params.trend_tf.trim().is_empty() {
            None
        } else {
            Some(CoreTimeframe::parse(&params.trend_tf)?)
        };
        Ok(Box::new(TemplateXsec::new(params, trigger, trend)))
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

    fn series() -> CandleSeries {
        CandleSeries::from_closed(
            CoreTimeframe::M1,
            vec![strategy_core::candle::Candle {
                open_time: chrono::Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
                open: 100.0,
                high: 100.0,
                low: 100.0,
                close: 100.0,
                volume: 1.0,
                quote_volume: 0.0,
                trade_num: 0,
                active_buy_volume: 0.0,
                active_buy_quote_volume: 0.0,
                is_closed: true,
            }],
        )
    }

    fn snapshot(sym: &str, macd: f64) -> InstrumentSnapshot {
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
            closed: Some(series()),
        }
    }

    fn portfolio(subs: Vec<SubPosition>) -> PortfolioContext {
        PortfolioContext {
            strategy_id: "t".into(),
            nav: 1000.0,
            drawdown: 0.0,
            portfolio_pnl_pct: 0.0,
            sub_positions: subs,
            master: None,
            multipliers: Default::default(),
            max_total_notional: 1.0,
            rebalance_due: true,
            no_trade: false,
        }
    }

    fn params(top_k: usize) -> XsecParams {
        XsecParams {
            top_k,
            flat_entry: false,
            ..XsecParams::default()
        }
    }

    #[test]
    fn ranks_higher_macd_into_top_k() {
        let mut st = TemplateXsec::new(params(1), CoreTimeframe::M1, None);
        let out = st.on_cross_section_bars(
            &[],
            &[snapshot("AAA", 1.0), snapshot("BBB", 5.0)],
            &portfolio(Vec::new()),
        );
        assert_eq!(out.len(), 1, "top_k=1 只开最高分");
        assert!(matches!(
            &out[0],
            InstrumentAction::Open { instrument, .. } if instrument.symbol == "BBB"
        ));
    }

    #[test]
    fn single_snapshot_uses_single_entry() {
        let mut st = TemplateXsec::new(params(3), CoreTimeframe::M1, None);
        let out =
            st.on_cross_section_bars(&[], &[snapshot("AAA", 0.0)], &portfolio(Vec::new()));
        assert_eq!(out.len(), 1, "N=1 走单标分支");
    }

    #[test]
    fn dropped_out_of_top_k_adjusts_to_zero() {
        let mut st = TemplateXsec::new(params(1), CoreTimeframe::M1, None);
        let sub = SubPosition {
            id: "sub_aaa".into(),
            strategy_id: "t".into(),
            master_id: "m".into(),
            instrument: inst("AAA"),
            side: Side::Long,
            qty: 10.0,
            entry_price: 100.0,
            stop_price: Some(95.0),
            target_weight: 0.5,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: None,
            closed_at: None,
            realized_pnl: None,
        };
        let out = st.on_cross_section_bars(
            &[],
            &[snapshot("AAA", 1.0), snapshot("BBB", 5.0)],
            &portfolio(vec![sub]),
        );
        assert!(
            out.iter().any(|a| matches!(
                a,
                InstrumentAction::Adjust { target_qty, .. } if *target_qty == 0.0
            )),
            "跌出 top_k 必须调回 0: {out:?}"
        );
    }
}
