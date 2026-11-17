//! 策略基类：把「单标的 / 横截面」两条决策路径收敛到同一骨架。
//!
//! 引擎已统一 K 线接收、指标计算、配置入场规则、组合止损与动作派发；
//! 本模块只保留策略特有的三件事：打分、入场过滤、止损定义。新增策略 =
//! 实现 [`StrategyLogic`] 并用 [`StrategyBase`] 包装，文件尾照常
//! `register_strategy!` 自注册。

use strategy_core::signal::Side;
use strategy_engine::core::bar::Timeframe;
use strategy_engine::core::identity::InstrumentId;
use strategy_engine::strategy::model::{MasterPositionState, SubPosition, SubPositionState};
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};

/// 策略私有 alpha 钩子。
///
/// 数据有效性（`stale` / `entry_eligible`）由基类判定，这里只回答
/// 「打分多少、是否允许开仓、止损放哪」。
pub trait StrategyLogic: Send {
    /// 策略类型名（必须与 `register_strategy!` 注册名一致）。
    fn name(&self) -> &str;

    /// 横截面持有的标的数；单标的路径忽略该值。
    fn top_k(&self) -> usize;

    /// 排名分数；`None` 表示特征缺失/无法判定，不参与排名也不开仓。
    fn score(&self, snapshot: &InstrumentSnapshot) -> Option<f64>;

    /// 策略私有入场门槛（在 `stale` / `entry_eligible` 之外）。
    fn entry_allowed(&self, snapshot: &InstrumentSnapshot) -> bool;

    /// 止损定义；`None` 表示本轮不开仓——开仓动作必须携带止损价。
    fn stop(&self, snapshot: &InstrumentSnapshot) -> Option<StopSpec>;

    /// 开仓原因文案；默认带策略名与打分。
    fn entry_reason(&self, _snapshot: &InstrumentSnapshot, score: Option<f64>) -> String {
        match score {
            Some(value) => format!("{}: score {value:.2}", self.name()),
            None => format!("{}: entry", self.name()),
        }
    }
}

/// 止损定义（多头口径）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StopSpec {
    /// 固定百分比（占价格）。
    Percent(f64),
    /// ATR 倍数止损；ATR 缺失或非法时回退 `fallback_pct`。
    Atr {
        mult: f64,
        /// ATR 缺失时的回退百分比。
        fallback_pct: f64,
        /// 止损距离下限；`None` 不限。
        min_distance_pct: Option<f64>,
        /// 止损距离上限；`None` 不限。
        max_distance_pct: Option<f64>,
    },
}

impl StopSpec {
    /// 计算多头止损价；价格非法时返回 `None`。
    pub fn price(&self, snapshot: &InstrumentSnapshot) -> Option<f64> {
        let price = snapshot.bar.close;
        if !price.is_finite() || price <= 0.0 {
            return None;
        }
        match *self {
            StopSpec::Percent(pct) => Some(percent_stop(price, pct)),
            StopSpec::Atr {
                mult,
                fallback_pct,
                min_distance_pct,
                max_distance_pct,
            } => {
                let mut stop = match snapshot.features.number("atr") {
                    Some(atr) if atr.is_finite() && atr > 0.0 => price - mult * atr,
                    _ => percent_stop(price, fallback_pct),
                };
                // 止损价越低代表回撤越宽：下限取更低者，上限取更高者。
                if let Some(min) = min_distance_pct {
                    stop = stop.min(percent_stop(price, min));
                }
                if let Some(max) = max_distance_pct {
                    stop = stop.max(percent_stop(price, max));
                }
                // 防止 ATR 过大时止损价非正。
                Some(stop.max(percent_stop(price, 99.0)))
            }
        }
    }
}

/// 固定百分比止损价（多头）；百分比夹在 [0.1%, 99%]，避免零或负止损。
pub fn percent_stop(price: f64, pct: f64) -> f64 {
    price * (1.0 - (pct / 100.0).clamp(0.001, 0.99))
}

/// 通用策略骨架：单快照走单标的路径，多快照走横截面排名 + 等权调仓。
///
/// 持仓退出（止损/止盈/组合回撤）交给引擎配置 `exit` / `portfolio_exit`，
/// 本基类只在标的掉出 `top_k` 时把目标数量清零。
pub struct StrategyBase<L: StrategyLogic> {
    logic: L,
}

impl<L: StrategyLogic> StrategyBase<L> {
    pub fn new(logic: L) -> Self {
        Self { logic }
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

    fn signed(sub: &SubPosition) -> f64 {
        match sub.side {
            Side::Long => sub.qty,
            Side::Short => -sub.qty,
        }
    }

    fn open(snapshot: &InstrumentSnapshot, stop_price: f64, reason: String) -> InstrumentAction {
        InstrumentAction::Open {
            instrument: snapshot.instrument.clone(),
            side: Side::Long,
            price: snapshot.bar.close,
            stop_price,
            atr: 0.0,
            strength: 1.0,
            reason,
        }
    }

    /// 单标的路径：已有未平仓持仓则不动，否则按钩子判定是否开仓。
    fn single(
        &self,
        snapshot: &InstrumentSnapshot,
        portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        if snapshot.stale
            || !snapshot.entry_eligible
            || !self.logic.entry_allowed(snapshot)
            || Self::active_sub(portfolio, &snapshot.instrument).is_some()
        {
            return Vec::new();
        }
        let Some(stop_price) = self
            .logic
            .stop(snapshot)
            .and_then(|spec| spec.price(snapshot))
        else {
            return Vec::new();
        };
        let reason = self
            .logic
            .entry_reason(snapshot, self.logic.score(snapshot));
        vec![Self::open(snapshot, stop_price, reason)]
    }

    /// 横截面路径：只在调仓轮动作，等权持有打分前 `top_k`。
    fn cross(
        &self,
        snapshots: &[InstrumentSnapshot],
        portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        let top_k = self.logic.top_k().max(1);
        let mut ranked: Vec<(&InstrumentSnapshot, f64)> = snapshots
            .iter()
            .filter(|s| !s.stale)
            .filter_map(|s| {
                self.logic
                    .score(s)
                    .filter(|value| value.is_finite())
                    .map(|value| (s, value))
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
            .take(top_k)
            .map(|(s, _)| s.instrument.namespaced_key())
            .collect();
        let weight = 1.0 / top_k as f64;

        let mut out = Vec::new();
        for snapshot in snapshots {
            let key = snapshot.instrument.namespaced_key();
            let price = snapshot.bar.close;
            let mult = *portfolio.multipliers.get(&key).unwrap_or(&1.0);
            let existing = Self::active_sub(portfolio, &snapshot.instrument);
            if keep.contains(&key) {
                match existing {
                    None => {
                        let entry_ok = !snapshot.stale
                            && snapshot.entry_eligible
                            && self.logic.entry_allowed(snapshot);
                        if !entry_ok {
                            continue;
                        }
                        let Some(stop_price) = self
                            .logic
                            .stop(snapshot)
                            .and_then(|spec| spec.price(snapshot))
                        else {
                            continue;
                        };
                        let reason = self
                            .logic
                            .entry_reason(snapshot, self.logic.score(snapshot));
                        out.push(Self::open(snapshot, stop_price, reason));
                    }
                    Some(sub) if price > 0.0 && mult > 0.0 => {
                        // 目标按风险预算（而非全额 NAV）等权分配，与执行层容量闸门同口径。
                        let target =
                            weight * portfolio.max_total_notional * portfolio.nav / (price * mult);
                        let current = Self::signed(sub);
                        let on_target =
                            current > 0.0 && (target - current).abs() <= 1e-9 * current.max(1.0);
                        if !on_target {
                            out.push(InstrumentAction::Adjust {
                                instrument: snapshot.instrument.clone(),
                                target_qty: target,
                                reason: format!(
                                    "{}: rebalance weight {weight:.4}",
                                    self.logic.name()
                                ),
                            });
                        }
                    }
                    Some(_) => {}
                }
            } else if let Some(sub) = existing {
                // 掉出 top_k 且本轮特征可判：目标清零；stale 或缺特征时
                // 无法判定排名，保持到下一个可判定的调仓轮。
                if !snapshot.stale && self.logic.score(snapshot).is_some() && sub.qty > 1e-12 {
                    out.push(InstrumentAction::Adjust {
                        instrument: snapshot.instrument.clone(),
                        target_qty: 0.0,
                        reason: format!("{}: dropped out of top_k", self.logic.name()),
                    });
                }
            }
        }
        out
    }
}

impl<L: StrategyLogic> CrossSectionalStrategyPort for StrategyBase<L> {
    fn strategy_type(&self) -> &str {
        self.logic.name()
    }

    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[Timeframe],
        snapshots: &[InstrumentSnapshot],
        portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        if snapshots.is_empty() {
            return Vec::new();
        }
        // 总仓平仓中：开仓/加仓/调仓均无意义，等子仓全部收口后再决策。
        if portfolio
            .master
            .as_ref()
            .is_some_and(|m| m.state == MasterPositionState::Closing)
        {
            return Vec::new();
        }
        if snapshots.len() == 1 {
            return self.single(&snapshots[0], portfolio);
        }
        if !portfolio.rebalance_due {
            return Vec::new();
        }
        self.cross(snapshots, portfolio)
    }
}
