//! `template_trend` — 逐标趋势模板（单标与 N 标独立同一代码）。
//!
//! 合并原 `example_dual_ma`（双均线交叉 + 百分比止损）、`example_mtf_trend`
//! （交叉 + 趋势过滤 + ATR）与 `futures_single_trend`（触发线 + 趋势过滤 + ATR）：
//! 入场/出场语义由 `entry_mode` 与 `stop_mode` 参数选择，单标与每标独立执行的
//! N>1 走同一份逐快照循环——策略不感知交易所。

use serde::Deserialize;
use strategy_core::candle::Timeframe as CoreTimeframe;
use strategy_core::signal::Side;
use strategy_engine::bridge::CandleSeries;
use strategy_engine::core::bar::{Bar, Timeframe as EngineTimeframe};
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};
use strategy_indicators::talib;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryMode {
    /// 快慢 EMA 交叉事件（原 `example_dual_ma`）。
    DualMa,
    /// 收盘上穿/下穿触发 EMA，且与趋势 EMA 同向（原 `futures_single_trend`）。
    #[default]
    EmaTrend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    #[default]
    Atr,
    Percent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitPriceSource {
    #[default]
    Close,
    HighLow,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TrendParams {
    pub entry_mode: EntryMode,
    pub fast_period: usize,
    pub slow_period: usize,
    /// 触发 EMA 周期（`ema_trend` 模式）。
    pub trigger_ema: usize,
    /// 趋势过滤周期（空 = 不过滤；如 `1h` / `1d`）。
    pub trend_tf: String,
    pub trend_ema: usize,
    pub stop_mode: StopMode,
    /// `stop_mode=percent` 时的止损百分比。
    pub stop_loss_pct: f64,
    pub atr_period: usize,
    pub atr_stop_mult: f64,
    pub exit_price_source: ExitPriceSource,
    pub allow_short: bool,
}

impl Default for TrendParams {
    fn default() -> Self {
        Self {
            entry_mode: EntryMode::EmaTrend,
            fast_period: 10,
            slow_period: 30,
            trigger_ema: 60,
            trend_tf: String::new(),
            trend_ema: 60,
            stop_mode: StopMode::Atr,
            stop_loss_pct: 2.0,
            atr_period: 14,
            atr_stop_mult: 2.0,
            exit_price_source: ExitPriceSource::Close,
            allow_short: true,
        }
    }
}

pub struct TemplateTrend {
    config: TrendParams,
    trigger_tf: CoreTimeframe,
    trend_set: Option<CoreTimeframe>,
}

impl TemplateTrend {
    pub fn new(
        params: TrendParams,
        trigger_tf: CoreTimeframe,
        trend_tf: Option<CoreTimeframe>,
    ) -> Self {
        Self {
            config: params,
            trigger_tf,
            trend_set: trend_tf,
        }
    }

    fn cross_signal(&self, closed: &CandleSeries) -> Option<bool> {
        let candles = closed.closed(self.trigger_tf)?;
        if candles.len() < self.config.slow_period.max(self.config.fast_period) + 2 {
            return None;
        }
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        let fast = talib::ema(&closes, self.config.fast_period).ok()?;
        let slow = talib::ema(&closes, self.config.slow_period).ok()?;
        let n = fast.len().min(slow.len());
        if n < 2 {
            return None;
        }
        let (f_now, s_now) = (fast[n - 1], slow[n - 1]);
        let (f_prev, s_prev) = (fast[n - 2], slow[n - 2]);
        if !f_now.is_finite() || !s_now.is_finite() || !f_prev.is_finite() || !s_prev.is_finite() {
            return None;
        }
        if f_prev <= s_prev && f_now > s_now {
            Some(true)
        } else if f_prev >= s_prev && f_now < s_now {
            Some(false)
        } else {
            None
        }
    }

    fn trigger_side(&self, closed: &CandleSeries) -> Option<Side> {
        let candles = closed.closed(self.trigger_tf)?;
        if candles.len() < self.config.trigger_ema + 2 {
            return None;
        }
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        let ema = talib::ema(&closes, self.config.trigger_ema).ok()?;
        let n = closes.len();
        let (p_c, l_c) = (closes[n - 2], closes[n - 1]);
        let (p_e, l_e) = (ema[n - 2], ema[n - 1]);
        if !p_e.is_finite() || !l_e.is_finite() {
            return None;
        }
        if p_c <= p_e && l_c > l_e {
            Some(Side::Long)
        } else if p_c >= p_e && l_c < l_e {
            Some(Side::Short)
        } else {
            None
        }
    }

    fn trend_side(&self, closed: &CandleSeries) -> Option<Side> {
        let tf = self.trend_set?;
        let candles = closed.closed(tf)?;
        if candles.len() < self.config.trend_ema + 1 {
            return None;
        }
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        let ema = talib::ema(&closes, self.config.trend_ema).ok()?;
        let last = closes.last().copied()?;
        let last_ema = ema.last().copied()?;
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

    fn atr(&self, closed: &CandleSeries) -> Option<f64> {
        let candles = closed.closed(self.trigger_tf)?;
        if candles.len() < self.config.atr_period + 1 {
            return None;
        }
        let highs: Vec<f64> = candles.iter().map(|c| c.high).collect();
        let lows: Vec<f64> = candles.iter().map(|c| c.low).collect();
        let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
        talib::atr(&highs, &lows, &closes, self.config.atr_period)
            .ok()?
            .last()
            .copied()
            .filter(|v| v.is_finite() && *v > 0.0)
    }

    fn stop_hit(&self, bar: &Bar, side: Side, stop: f64) -> bool {
        match (side, self.config.exit_price_source) {
            (Side::Long, ExitPriceSource::Close) => bar.close <= stop,
            (Side::Long, ExitPriceSource::HighLow) => bar.low <= stop,
            (Side::Short, ExitPriceSource::Close) => bar.close >= stop,
            (Side::Short, ExitPriceSource::HighLow) => bar.high >= stop,
        }
    }

    fn entry_side(&self, closed: &CandleSeries) -> Option<Side> {
        match self.config.entry_mode {
            EntryMode::DualMa => match self.cross_signal(closed)? {
                true => Some(Side::Long),
                false => Some(Side::Short),
            },
            EntryMode::EmaTrend => {
                let trigger = self.trigger_side(closed)?;
                if self.trend_set.is_some() {
                    let trend = self.trend_side(closed)?;
                    (trigger == trend).then_some(trigger)
                } else {
                    Some(trigger)
                }
            }
        }
    }

    fn exit_signal(&self, closed: &CandleSeries, side: Side) -> bool {
        match self.config.entry_mode {
            EntryMode::DualMa => match self.cross_signal(closed) {
                Some(up) => match side {
                    Side::Long => !up,
                    Side::Short => up,
                },
                None => false,
            },
            EntryMode::EmaTrend => {
                let trend_against = self.trend_side(closed).is_some_and(|t| t != side);
                let trigger_against = self.trigger_side(closed).is_some_and(|t| t != side);
                trend_against || trigger_against
            }
        }
    }

    fn decide(&self, s: &InstrumentSnapshot, closed: &CandleSeries) -> Option<InstrumentAction> {
        let price = s.bar.close;
        if price <= 0.0 {
            return None;
        }
        if let Some(pos) = &s.position {
            if self.stop_hit(&s.bar, pos.side, pos.stop_price) {
                return Some(InstrumentAction::Close {
                    instrument: s.instrument.clone(),
                    price,
                    reason: "template_trend: 硬止损".into(),
                    is_stop_loss: true,
                    strength: 1.0,
                });
            }
            if self.exit_signal(closed, pos.side) {
                return Some(InstrumentAction::Close {
                    instrument: s.instrument.clone(),
                    price,
                    reason: "template_trend: 反向信号".into(),
                    is_stop_loss: false,
                    strength: 1.0,
                });
            }
            return None;
        }
        if !s.entry_eligible || s.in_stop_loss_cooldown() {
            return None;
        }
        let side = self.entry_side(closed)?;
        if side == Side::Short && !self.config.allow_short {
            return None;
        }
        let (stop_price, atr) = match self.config.stop_mode {
            StopMode::Percent => {
                let pct = self.config.stop_loss_pct;
                let stop = match side {
                    Side::Long => price * (1.0 - pct / 100.0),
                    Side::Short => price * (1.0 + pct / 100.0),
                };
                (stop, 0.0)
            }
            StopMode::Atr => {
                let atr = self.atr(closed)?;
                let dist = self.config.atr_stop_mult * atr;
                let stop = match side {
                    Side::Long => price - dist,
                    Side::Short => price + dist,
                };
                (stop, atr)
            }
        };
        if stop_price <= 0.0 {
            return None;
        }
        Some(InstrumentAction::Open {
            instrument: s.instrument.clone(),
            side,
            price,
            stop_price,
            atr,
            strength: 1.0,
            reason: format!(
                "template_trend: {side:?} entry ({:?})",
                self.config.entry_mode
            ),
        })
    }
}

impl CrossSectionalStrategyPort for TemplateTrend {
    fn strategy_type(&self) -> &str {
        "template_trend"
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
            if let Some(a) = self.decide(s, closed) {
                out.push(a);
            }
        }
        out
    }
}

crate::register_strategy! {
    name: "template_trend",
    factory: |args| {
        let params: TrendParams = crate::registry::params_or_default(args.params)?;
        let trigger = CoreTimeframe::parse(args.timeframe)?;
        let trend = if params.trend_tf.trim().is_empty() {
            None
        } else {
            Some(CoreTimeframe::parse(&params.trend_tf)?)
        };
        Ok(Box::new(TemplateTrend::new(params, trigger, trend)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use strategy_core::strategy::PositionView;
    use strategy_engine::core::bar::{BarExtensions, BarFinality};
    use strategy_engine::core::identity::{ExchangeId, InstrumentId};
    use strategy_engine::core::session::TradingDay;
    use strategy_engine::strategy::features::FeatureMap;

    fn inst() -> InstrumentId {
        InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT")
    }

    fn series(closes: &[f64]) -> CandleSeries {
        let t0 = chrono::Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
        let candles: Vec<strategy_core::candle::Candle> = closes
            .iter()
            .enumerate()
            .map(|(i, &c)| strategy_core::candle::Candle {
                open_time: t0 + chrono::Duration::minutes(i as i64),
                open: c,
                high: c + 1.0,
                low: c - 1.0,
                close: c,
                volume: 1.0,
                quote_volume: 0.0,
                trade_num: 0,
                active_buy_volume: 0.0,
                active_buy_quote_volume: 0.0,
                is_closed: true,
            })
            .collect();
        CandleSeries::from_closed(CoreTimeframe::M1, candles)
    }

    fn snapshot(closed: CandleSeries, position: Option<PositionView>) -> InstrumentSnapshot {
        InstrumentSnapshot {
            instrument: inst(),
            bar: Bar {
                instrument: inst(),
                open_time: chrono::Utc
                    .with_ymd_and_hms(2026, 1, 5, 0, 0, 0)
                    .unwrap(),
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.0,
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
            trading_day: TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
            no_trade: false,
            entry_confirm: false,
            signal_resume: false,
            cooldown: None,
            session_open_price: None,
            closed: Some(closed),
        }
    }

    fn portfolio() -> PortfolioContext {
        PortfolioContext {
            strategy_id: "t".into(),
            nav: 1.0,
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

    fn params(mode: EntryMode, stop: StopMode) -> TrendParams {
        TrendParams {
            entry_mode: mode,
            stop_mode: stop,
            trigger_ema: 10,
            trend_ema: 10,
            fast_period: 2,
            slow_period: 6,
            atr_period: 3,
            ..TrendParams::default()
        }
    }

    #[test]
    fn dual_ma_cross_opens_long() {
        let mut st = TemplateTrend::new(
            params(EntryMode::DualMa, StopMode::Percent),
            CoreTimeframe::M1,
            None,
        );
        // 前 39 根横盘、最后一根跳升：交叉事件恰好发生在末根。
        let mut closes = vec![100.0; 39];
        closes.push(200.0);
        let s = snapshot(series(&closes), None);
        let out = st.on_cross_section_bars(&[], &[s], &portfolio());
        assert_eq!(out.len(), 1, "交叉事件必须开仓");
        assert!(matches!(
            out[0],
            InstrumentAction::Open { side: Side::Long, .. }
        ));
    }

    #[test]
    fn atr_stop_mode_computes_distance() {
        let mut st = TemplateTrend::new(
            params(EntryMode::DualMa, StopMode::Atr),
            CoreTimeframe::M1,
            None,
        );
        let mut closes = vec![100.0; 39];
        closes.push(200.0);
        let s = snapshot(series(&closes), None);
        let out = st.on_cross_section_bars(&[], &[s], &portfolio());
        let Some(InstrumentAction::Open { stop_price, atr, .. }) = out.first() else {
            panic!("必须开仓: {out:?}");
        };
        assert!(*atr > 0.0);
        assert!(*stop_price < 100.0);
    }

    #[test]
    fn stop_hit_closes_long() {
        let mut st = TemplateTrend::new(
            params(EntryMode::DualMa, StopMode::Percent),
            CoreTimeframe::M1,
            None,
        );
        let closes = vec![100.0; 40];
        let pos = PositionView {
            side: Side::Long,
            entry_price: 100.0,
            stop_price: 105.0,
            peak_price: 100.0,
            max_pnl_pct: 0.0,
            min_pnl_pct: 0.0,
            exit_active: false,
            exit_price: 0.0,
        };
        let s = snapshot(series(&closes), Some(pos));
        let out = st.on_cross_section_bars(&[], &[s], &portfolio());
        assert!(
            matches!(
                out.first(),
                Some(InstrumentAction::Close {
                    is_stop_loss: true,
                    ..
                })
            ),
            "收盘 100 低于止损 105 必须止损离场"
        );
    }

    #[test]
    fn allow_short_false_blocks_short_entry() {
        let mut st = TemplateTrend::new(
            TrendParams {
                allow_short: false,
                ..params(EntryMode::DualMa, StopMode::Percent)
            },
            CoreTimeframe::M1,
            None,
        );
        // 前 39 根横盘、最后一根暴跌：下穿 → 做空信号，但 allow_short=false 必须拦截。
        let mut closes = vec![100.0; 39];
        closes.push(0.0);
        let s = snapshot(series(&closes), None);
        let out = st.on_cross_section_bars(&[], &[s], &portfolio());
        assert!(out.is_empty(), "禁止做空时下穿不得开仓: {out:?}");
    }
}
