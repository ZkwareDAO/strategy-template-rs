//! `xsec_momentum` — 示例横截面策略（PR-4）。
//!
//! alpha 在代码（配置 DSL 冻结，§5.8）：base TF 闭合且 `rebalance_due` 时，
//! 按 1h MACD 值降序选 `top_k` 做多头等权轮换；候选门 = `!stale &&
//! entry_eligible`（entry 规则归配置：如 adx > 20）。空头侧（bottom_k）本期
//! 不实现（`validate_config` 已强制 bottom_k = 0）。
//!
//! `target_qty` 口径 = 带符号净数量（多头为正，T9 裁决）；`Adjust` 与现有
//! 子仓幅值不一致才发出（容差内视为已达标）。

use anyhow::anyhow;
use strategy_core::signal::Side;
use strategy_engine::core::identity::InstrumentId;
use strategy_engine::strategy::model::{MasterPositionState, SubPosition, SubPositionState};
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};

/// 策略私有参数（配置 `params:` 节；`deny_unknown_fields` 拼错即报错）。
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MomentumParams {
    pub top_k: usize,
    pub bottom_k: usize,
}

impl Default for MomentumParams {
    fn default() -> Self {
        Self {
            top_k: 3,
            bottom_k: 0,
        }
    }
}

pub struct XsecMomentum {
    params: MomentumParams,
}

impl XsecMomentum {
    pub fn new(params: MomentumParams) -> Self {
        Self { params }
    }

    /// `params` 缺省 = 全默认；反序列化失败必须报错（不静默吞配置）。
    pub fn from_params(params: Option<&serde_json::Value>) -> anyhow::Result<Self> {
        match params {
            Some(v) => serde_json::from_value::<MomentumParams>(v.clone())
                .map(Self::new)
                .map_err(|e| anyhow!("xsec_momentum params 反序列化失败: {e}")),
            None => Ok(Self::new(MomentumParams::default())),
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
}

impl CrossSectionalStrategyPort for XsecMomentum {
    fn strategy_type(&self) -> &str {
        "xsec_momentum"
    }

    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[strategy_engine::core::bar::Timeframe],
        snapshots: &[InstrumentSnapshot],
        portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        // 空仓 = 没有可调仓的持仓：首次建仓不受调仓窗口限制，否则每次
        // 进程重启（计数器归零/首个窗口未满）都要空等一整个 every_bars。
        let flat = portfolio
            .master
            .as_ref()
            .is_none_or(|m| m.state == MasterPositionState::Closed)
            && portfolio
                .sub_positions
                .iter()
                .all(|p| p.state == SubPositionState::Closed);
        // 指标快照每轮决策都打印（不依赖是否到调仓窗口）：实盘核对 adx/macd
        // 计算值、stale 与 entry 过滤原因的第一手依据。
        for s in snapshots.iter() {
            tracing::debug!(
                strategy_id = %portfolio.strategy_id,
                instrument = %s.instrument,
                adx = ?s.features.number("adx"),
                macd = ?s.features.number("macd"),
                features = ?s.features.values,
                stale = s.stale,
                entry_eligible = s.entry_eligible,
                entry_reason = ?s.entry_reason,
                no_trade = s.no_trade,
                rebalance_due = portfolio.rebalance_due,
                flat,
                "xsec_momentum 指标快照"
            );
        }
        if !portfolio.rebalance_due && !flat {
            tracing::debug!(
                strategy_id = %portfolio.strategy_id,
                "xsec_momentum：未到调仓窗口且已有持仓（空仓首建不受窗口限制），本轮不决策"
            );
            return Vec::new();
        }
        // ① 候选排名：macd 降序，同值按 namespaced_key 升序（确定性 tie-break）。
        let mut ranked: Vec<(&InstrumentSnapshot, f64)> = snapshots
            .iter()
            .filter(|s| !s.stale && s.entry_eligible)
            .filter_map(|s| s.features.number("macd").map(|m| (s, m)))
            .collect();
        ranked.sort_by(|a, b| {
            b.1.total_cmp(&a.1).then_with(|| {
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

        if ranked.is_empty() {
            tracing::info!(
                strategy_id = %portfolio.strategy_id,
                top_k,
                snapshots = snapshots.len(),
                "xsec_momentum：无合格候选（stale / entry 规则 / macd 缺失过滤后为空），本轮不开仓"
            );
        }

        let mut out = Vec::new();
        for s in snapshots.iter() {
            let key = s.instrument.namespaced_key();
            let price = s.bar.close;
            let mult = *portfolio.multipliers.get(&key).unwrap_or(&1.0);
            let existing = Self::active_sub(portfolio, &s.instrument);
            if keep.contains(&key) {
                match existing {
                    None => out.push(InstrumentAction::Open {
                        instrument: s.instrument.clone(),
                        side: Side::Long,
                        price,
                        stop_price: price * 0.98,
                        atr: 0.0,
                        strength: 1.0,
                        reason: format!("xsec_momentum: macd rank in top_{top_k}"),
                    }),
                    Some(sub) => {
                        // 目标按风险预算（而非全额 NAV）等权分配，与执行层容量闸门同口径。
                        let target =
                            weight * portfolio.max_total_notional * portfolio.nav / (price * mult);
                        let current = Self::signed(sub);
                        // 同向且幅值在容差内 → 已达标；其余发 signed Adjust。
                        let on_target =
                            current > 0.0 && (target - current).abs() <= 1e-9 * current.max(1.0);
                        if !on_target {
                            out.push(InstrumentAction::Adjust {
                                instrument: s.instrument.clone(),
                                target_qty: target,
                                reason: format!("xsec_momentum: rebalance weight {weight:.4}"),
                            });
                        }
                    }
                }
            } else if let Some(sub) = existing {
                if sub.qty > 1e-12 {
                    out.push(InstrumentAction::Adjust {
                        instrument: s.instrument.clone(),
                        target_qty: 0.0,
                        reason: "xsec_momentum: dropped out of top_k".into(),
                    });
                }
            }
        }
        // 排名明细：macd 降序（同值按 key 升序），入选 top_k 的键集合。
        let ranking: Vec<String> = ranked
            .iter()
            .map(|(s, m)| format!("{}={m:.6}", s.instrument.namespaced_key()))
            .collect();
        tracing::info!(
            strategy_id = %portfolio.strategy_id,
            top_k,
            eligible = ranked.len(),
            nav = portfolio.nav,
            max_total_notional = portfolio.max_total_notional,
            target_weight = weight,
            ranking = ?ranking,
            keep = ?keep,
            actions = out.len(),
            "xsec_momentum 调仓决策完成"
        );
        for a in &out {
            tracing::info!(
                strategy_id = %portfolio.strategy_id,
                action = ?a,
                "xsec_momentum 派发动作"
            );
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
        let mut features = FeatureMap::default();
        if let Some(m) = macd {
            features.values.insert("macd".into(), m);
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
            strategy_id: "xsec_momentum:default".into(),
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
            strategy_id: "xsec_momentum:default".into(),
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

    fn strategy(top_k: usize) -> XsecMomentum {
        XsecMomentum::new(MomentumParams { top_k, bottom_k: 0 })
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
    fn ranks_by_macd_and_selects_top_k_only_when_rebalance_due() {
        let snaps = vec![
            snap("BTCUSDT", Some(0.5), false, true),
            snap("ETHUSDT", Some(0.2), false, true),
            snap("SOLUSDT", Some(-0.1), false, true),
        ];
        let mut s = strategy(2);
        let due = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true));
        assert_eq!(
            opens(&due),
            vec![
                "binance/crypto_perpetual/BTCUSDT".to_string(),
                "binance/crypto_perpetual/ETHUSDT".to_string()
            ],
            "macd 降序取前 2"
        );
        assert_eq!(
            adjusts(&due),
            Vec::new(),
            "SOL 不在 top_k 且无持仓 → 无动作"
        );
        let not_due_flat = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), false));
        assert_eq!(
            opens(&not_due_flat),
            vec![
                "binance/crypto_perpetual/BTCUSDT".to_string(),
                "binance/crypto_perpetual/ETHUSDT".to_string()
            ],
            "空仓首次建仓不受调仓窗口限制（否则重启后要等满 every_bars）"
        );
        let not_due_holding =
            s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("BTCUSDT", 0.5)], false));
        assert!(
            not_due_holding.is_empty(),
            "已持仓且未到调仓窗口 → 不调仓"
        );
    }

    #[test]
    fn stale_or_ineligible_snapshots_are_excluded() {
        let snaps = vec![
            snap("BTCUSDT", Some(0.9), true, true),   // stale → 剔除
            snap("ETHUSDT", Some(0.5), false, false), // entry 不合格 → 剔除
            snap("SOLUSDT", Some(0.1), false, true),
        ];
        let mut s = strategy(1);
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true));
        assert_eq!(
            opens(&acts),
            vec!["binance/crypto_perpetual/SOLUSDT".to_string()],
            "stale/不合格剔除后取剩余最优"
        );
    }

    #[test]
    fn existing_position_gets_adjust_to_target_weight() {
        let snaps = vec![snap("BTCUSDT", Some(0.5), false, true)];
        let mut s = strategy(2); // weight = 0.5 → target = 0.5*1000/(100*1) = 5.0
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("BTCUSDT", 0.5)], true));
        assert_eq!(
            adjusts(&acts),
            vec![("binance/crypto_perpetual/BTCUSDT".to_string(), 5.0)],
            "qty 与目标不一致 → Adjust(target_qty)"
        );
        let ok = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("BTCUSDT", 5.0)], true));
        assert!(
            opens(&ok).is_empty() && adjusts(&ok).is_empty(),
            "qty 已达标 → 无动作"
        );
    }

    #[test]
    fn target_uses_risk_budget_not_full_nav() {
        let snaps = vec![snap("BTCUSDT", Some(0.5), false, true)];
        let mut s = strategy(2); // weight 0.5
        let mut c = ctx(vec![sub("BTCUSDT", 0.5)], true);
        c.max_total_notional = 0.25;
        let acts = s.on_cross_section_bars(&[], &snaps, &c);
        assert_eq!(
            adjusts(&acts),
            vec![("binance/crypto_perpetual/BTCUSDT".to_string(), 1.25)],
            "0.5 × 0.25 × 1000 / (100 × 1) = 1.25"
        );
    }

    #[test]
    fn held_non_candidate_is_adjusted_to_zero() {
        let snaps = vec![
            snap("BTCUSDT", Some(0.1), false, true),
            snap("ETHUSDT", Some(0.9), false, true),
        ];
        let mut s = strategy(1);
        let acts = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("BTCUSDT", 2.0)], true));
        assert_eq!(
            adjusts(&acts),
            vec![("binance/crypto_perpetual/BTCUSDT".to_string(), 0.0)],
            "持仓掉出 top_k → 目标清零"
        );
        assert_eq!(
            opens(&acts),
            vec!["binance/crypto_perpetual/ETHUSDT".to_string()]
        );
    }

    #[test]
    fn from_params_rejects_unknown_keys() {
        let bad = serde_json::json!({ "topk": 2 });
        assert!(XsecMomentum::from_params(Some(&bad)).is_err());
        let good = serde_json::json!({ "top_k": 2, "bottom_k": 0 });
        assert!(XsecMomentum::from_params(Some(&good)).is_ok());
        assert_eq!(
            XsecMomentum::from_params(None).unwrap().params.top_k,
            MomentumParams::default().top_k
        );
    }
}

crate::register_strategy! {
    name: "xsec_momentum",
    factory: |args| {
        let _ = (args.symbol, args.timeframe, args.cash);
        Ok(Box::new(XsecMomentum::from_params(args.params)?))
    }
}
