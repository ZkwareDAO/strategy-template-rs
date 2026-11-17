//! `xsec_template` — 单标/横截面双路径策略模板。
//!
//! `snapshots.len() == 1` 走单标路径：不看 `rebalance_due`，空仓且
//! `entry_eligible` 即开多；已有子仓则持有。`snapshots.len() > 1` 走横截面
//! 路径：仅 `rebalance_due` 时按 `params.rank_indicator` 排名，等权调仓到
//! `top_k`，数据有效但掉出候选的子仓目标清零；stale 或特征暂缺时保持仓位，
//! 避免在缺数据时误平仓。退出统一交给配置 `exit` / `portfolio_exit`，模板
//! 不产生 `Close`/`Reverse`。

use strategy_core::signal::Side;
use strategy_engine::core::identity::InstrumentId;
use strategy_engine::strategy::model::{SubPosition, SubPositionState};
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};

/// 策略私有参数（配置 `params:` 节；`deny_unknown_fields` 拼错即报错）。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TemplateParams {
    /// 排名所用特征名，必须与 `indicators` 中声明的键一致。
    pub rank_indicator: String,
    /// `true` = 数值越大越优；`false` = 越小越优。
    pub higher_is_better: bool,
    /// 横截面路径保留的候选数；单标路径不使用。
    pub top_k: usize,
}

impl Default for TemplateParams {
    fn default() -> Self {
        Self {
            rank_indicator: "macd".into(),
            higher_is_better: true,
            top_k: 3,
        }
    }
}

pub struct XsecTemplate {
    params: TemplateParams,
}

impl XsecTemplate {
    pub fn new(params: TemplateParams) -> Self {
        Self { params }
    }

    /// `params` 缺省 = 全默认；反序列化失败必须报错（不静默吞配置）。
    pub fn from_params(params: Option<&serde_json::Value>) -> anyhow::Result<Self> {
        crate::registry::params_or_default(params).map(Self::new)
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

    fn open(s: &InstrumentSnapshot, reason: String) -> InstrumentAction {
        let price = s.bar.close;
        InstrumentAction::Open {
            instrument: s.instrument.clone(),
            side: Side::Long,
            price,
            stop_price: price * 0.98,
            atr: 0.0,
            strength: 1.0,
            reason,
        }
    }
}

impl CrossSectionalStrategyPort for XsecTemplate {
    fn strategy_type(&self) -> &str {
        "xsec_template"
    }

    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[strategy_engine::core::bar::Timeframe],
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
            return vec![Self::open(s, "xsec_template: single entry".into())];
        }

        if !portfolio.rebalance_due {
            return Vec::new();
        }

        let mut ranked: Vec<(&InstrumentSnapshot, f64)> = snapshots
            .iter()
            .filter(|s| !s.stale && s.entry_eligible)
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
                    None => out.push(Self::open(s, format!("xsec_template: rank in top_{top_k}"))),
                    Some(sub) => {
                        // 目标按风险预算（而非全额 NAV）等权分配，与执行层容量闸门同口径。
                        let target =
                            weight * portfolio.max_total_notional * portfolio.nav / (price * mult);
                        let current = Self::signed(sub);
                        let on_target =
                            current > 0.0 && (target - current).abs() <= 1e-9 * current.max(1.0);
                        if !on_target {
                            out.push(InstrumentAction::Adjust {
                                instrument: s.instrument.clone(),
                                target_qty: target,
                                reason: format!("xsec_template: rebalance weight {weight:.4}"),
                            });
                        }
                    }
                }
            } else if let Some(sub) = existing {
                // stale 或特征暂缺时无法判断相对排名，强制清零会在缺数据时误平仓；
                // 保持仓位到下一个可判定的调仓轮。
                let rankable = !s.stale && s.features.number(&self.params.rank_indicator).is_some();
                if rankable && sub.qty > 1e-12 {
                    out.push(InstrumentAction::Adjust {
                        instrument: s.instrument.clone(),
                        target_qty: 0.0,
                        reason: "xsec_template: dropped out of top_k".into(),
                    });
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;
    use strategy_engine::core::bar::{Bar, BarExtensions, BarFinality};
    use strategy_engine::core::identity::ExchangeId;
    use strategy_engine::core::session::TradingDay;
    use strategy_engine::strategy::features::FeatureMap;

    fn inst(symbol: &str) -> InstrumentId {
        InstrumentId::perpetual(&ExchangeId::new("binance"), symbol)
    }

    fn bar(symbol: &str) -> Bar {
        let t = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
        Bar {
            instrument: inst(symbol),
            open_time: t,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.0,
            volume: 1.0,
            finality: BarFinality::Final,
            extensions: BarExtensions::default(),
        }
    }

    fn snap(symbol: &str, macd: Option<f64>, stale: bool, eligible: bool) -> InstrumentSnapshot {
        snap_feature(symbol, "macd", macd, stale, eligible)
    }

    fn snap_feature(
        symbol: &str,
        indicator: &str,
        value: Option<f64>,
        stale: bool,
        eligible: bool,
    ) -> InstrumentSnapshot {
        let mut features = FeatureMap::default();
        if let Some(v) = value {
            features.values.insert(indicator.into(), v);
        }
        InstrumentSnapshot {
            instrument: inst(symbol),
            bar: bar(symbol),
            position: None,
            stale,
            features,
            entry_eligible: eligible,
            entry_reason: None,
            exit_triggered: None,
            trading_day: TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
            no_trade: false,
            entry_confirm: true,
            signal_resume: false,
            cooldown: None,
            session_open_price: None,
            closed: None,
        }
    }

    fn ctx(subs: Vec<SubPosition>, due: bool) -> PortfolioContext {
        PortfolioContext {
            strategy_id: "xsec_template:default".into(),
            nav: 1000.0,
            drawdown: 0.0,
            portfolio_pnl_pct: 0.0,
            sub_positions: subs,
            master: None,
            multipliers: BTreeMap::new(),
            max_total_notional: 1.0,
            rebalance_due: due,
            no_trade: false,
        }
    }

    fn sub(symbol: &str, qty: f64) -> SubPosition {
        SubPosition {
            id: format!("sub_{symbol}"),
            strategy_id: "xsec_template:default".into(),
            master_id: "master_000001".into(),
            instrument: inst(symbol),
            side: Side::Long,
            qty,
            entry_price: 100.0,
            stop_price: None,
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: None,
            closed_at: None,
            realized_pnl: None,
        }
    }

    fn template(top_k: usize) -> XsecTemplate {
        XsecTemplate::new(TemplateParams {
            rank_indicator: "macd".into(),
            higher_is_better: true,
            top_k,
        })
    }

    fn template_with(top_k: usize, higher_is_better: bool) -> XsecTemplate {
        XsecTemplate::new(TemplateParams {
            rank_indicator: "macd".into(),
            higher_is_better,
            top_k,
        })
    }

    fn key(symbol: &str) -> String {
        format!("binance/crypto_perpetual/{symbol}")
    }

    fn opens(actions: &[InstrumentAction]) -> Vec<String> {
        actions
            .iter()
            .filter_map(|a| match a {
                InstrumentAction::Open { instrument, .. } => Some(instrument.namespaced_key()),
                _ => None,
            })
            .collect()
    }

    fn adjusts(actions: &[InstrumentAction]) -> Vec<(String, f64)> {
        actions
            .iter()
            .filter_map(|a| match a {
                InstrumentAction::Adjust {
                    instrument,
                    target_qty,
                    ..
                } => Some((instrument.namespaced_key(), *target_qty)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn single_instrument_opens_without_rebalance_due() {
        let snaps = vec![snap("SOLUSDT", Some(0.5), false, true)];
        let mut s = template(1);
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), false));
        assert_eq!(opens(&acts), vec![key("SOLUSDT")]);
        assert!(adjusts(&acts).is_empty(), "单标路径不做权重调仓");
    }

    #[test]
    fn single_instrument_holds_existing_position() {
        let snaps = vec![snap("SOLUSDT", Some(0.5), false, true)];
        let mut s = template(1);
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("SOLUSDT", 2.0)], false));
        assert!(opens(&acts).is_empty() && adjusts(&acts).is_empty());
    }

    #[test]
    fn single_instrument_ineligible_does_not_open() {
        let snaps = vec![snap("SOLUSDT", Some(0.5), false, false)];
        let mut s = template(1);
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), false));
        assert!(acts.is_empty(), "entry_eligible=false 不得开仓");
    }

    #[test]
    fn cross_section_ranks_by_feature_and_selects_top_k_only_when_due() {
        let snaps = vec![
            snap("BTCUSDT", Some(0.5), false, true),
            snap("ETHUSDT", Some(0.2), false, true),
            snap("SOLUSDT", Some(-0.1), false, true),
        ];
        let mut s = template(2);
        let due = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true));
        assert_eq!(opens(&due), vec![key("BTCUSDT"), key("ETHUSDT")]);
        let not_due = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), false));
        assert!(not_due.is_empty(), "N>1 且 rebalance_due=false 不决策");
    }

    #[test]
    fn lower_is_better_inverts_ranking() {
        let snaps = vec![
            snap("BTCUSDT", Some(0.5), false, true),
            snap("ETHUSDT", Some(0.2), false, true),
            snap("SOLUSDT", Some(-0.1), false, true),
        ];
        let mut s = template_with(1, false);
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true));
        assert_eq!(opens(&acts), vec![key("SOLUSDT")]);
    }

    #[test]
    fn rank_indicator_param_selects_named_feature() {
        let snaps = vec![
            snap_feature("BTCUSDT", "rsi", Some(70.0), false, true),
            snap_feature("ETHUSDT", "rsi", Some(30.0), false, true),
        ];
        let mut s = XsecTemplate::new(TemplateParams {
            rank_indicator: "rsi".into(),
            higher_is_better: true,
            top_k: 1,
        });
        assert_eq!(
            opens(&s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true))),
            vec![key("BTCUSDT")]
        );
    }

    #[test]
    fn stale_or_ineligible_snapshots_are_excluded() {
        let snaps = vec![
            snap("BTCUSDT", Some(0.9), true, true),
            snap("ETHUSDT", Some(0.5), false, false),
            snap("SOLUSDT", Some(0.1), false, true),
        ];
        let mut s = template(1);
        assert_eq!(
            opens(&s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true))),
            vec![key("SOLUSDT")]
        );
    }

    #[test]
    fn existing_position_adjusts_to_equal_weight_target() {
        let snaps = vec![
            snap("BTCUSDT", Some(0.5), false, true),
            snap("ETHUSDT", Some(0.2), false, true),
        ];
        let mut s = template(2); // weight 0.5；target = 0.5*1000/(100*1) = 5.0
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("BTCUSDT", 0.5)], true));
        assert_eq!(adjusts(&acts), vec![(key("BTCUSDT"), 5.0)]);
        assert_eq!(
            opens(&acts),
            vec![key("ETHUSDT")],
            "ETH 无仓且在前 2 → 开仓"
        );
        let ok = s.on_cross_section_bars(
            &[],
            &snaps,
            &ctx(vec![sub("BTCUSDT", 5.0), sub("ETHUSDT", 5.0)], true),
        );
        assert!(opens(&ok).is_empty() && adjusts(&ok).is_empty());
    }

    #[test]
    fn target_uses_risk_budget_not_full_nav() {
        let snaps = vec![
            snap("BTCUSDT", Some(0.5), false, true),
            snap("ETHUSDT", Some(0.2), false, true),
        ];
        let mut s = template(2); // weight 0.5
        let mut c = ctx(vec![sub("BTCUSDT", 0.5)], true);
        c.max_total_notional = 0.25;
        let acts = s.on_cross_section_bars(&[], &snaps, &c);
        assert_eq!(
            adjusts(&acts),
            vec![(key("BTCUSDT"), 1.25)],
            "0.5 × 0.25 × 1000 / (100 × 1) = 1.25"
        );
    }

    #[test]
    fn held_non_candidate_is_adjusted_to_zero() {
        let snaps = vec![
            snap("BTCUSDT", Some(0.1), false, true),
            snap("ETHUSDT", Some(0.9), false, true),
        ];
        let mut s = template(1);
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("BTCUSDT", 2.0)], true));
        assert_eq!(adjusts(&acts), vec![(key("BTCUSDT"), 0.0)]);
        assert_eq!(opens(&acts), vec![key("ETHUSDT")]);
    }

    #[test]
    fn held_stale_position_is_not_forced_out_at_rebalance() {
        let snaps = vec![
            snap("BTCUSDT", Some(0.1), true, true),
            snap("ETHUSDT", Some(0.9), false, true),
        ];
        let mut s = template(1);
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("BTCUSDT", 2.0)], true));
        assert!(adjusts(&acts).is_empty(), "stale 持仓不得被强制清零");
        assert_eq!(opens(&acts), vec![key("ETHUSDT")]);
    }

    #[test]
    fn held_position_without_rank_feature_is_not_forced_out_at_rebalance() {
        let snaps = vec![
            snap("BTCUSDT", None, false, true),
            snap("ETHUSDT", Some(0.9), false, true),
        ];
        let mut s = template(1);
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("BTCUSDT", 2.0)], true));
        assert!(adjusts(&acts).is_empty(), "缺特征持仓不得被强制清零");
        assert_eq!(opens(&acts), vec![key("ETHUSDT")]);
    }

    #[test]
    fn missing_rank_feature_excludes_instrument() {
        let snaps = vec![
            snap("BTCUSDT", None, false, true),
            snap("ETHUSDT", Some(0.1), false, true),
        ];
        let mut s = template(1);
        assert_eq!(
            opens(&s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true))),
            vec![key("ETHUSDT")],
            "缺失特征不得按 0 参与排名"
        );
    }

    #[test]
    fn rank_feature_absent_everywhere_yields_no_actions() {
        let snaps = vec![
            snap("BTCUSDT", None, false, true),
            snap("ETHUSDT", None, false, true),
        ];
        let mut s = template(1);
        assert!(
            s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true))
                .is_empty(),
            "全市场缺特征（如 params 指标未在 indicators 声明）不得交易"
        );
    }

    #[test]
    fn from_params_rejects_unknown_keys() {
        let bad = serde_json::json!({ "topk": 2 });
        assert!(XsecTemplate::from_params(Some(&bad)).is_err());
        assert_eq!(XsecTemplate::from_params(None).unwrap().params.top_k, 3);
    }
}

crate::register_strategy! {
    name: "xsec_template",
    factory: |args| {
        let _ = (args.symbol, args.timeframe, args.cash);
        Ok(Box::new(XsecTemplate::from_params(args.params)?))
    }
}
