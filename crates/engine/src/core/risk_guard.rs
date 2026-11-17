//! 风控兜底模块 —— 从 `ExchangeRunner::on_bar` 提取的职责 #1。
//!
//! 持仓中且策略静默时，按收盘价跑统一风控兜底（固定止损 > 回落止盈 >
//! 固定止盈），职责自旧 `Engine::check_risk_fallback` 提取（该模块已随 R4
//! 退役删除；优先级真源见 `crates/core/src/risk.rs:103-106`）。
//! trail 状态就地回写仓位——复用 `positions::PositionState`，不复制公式。

use strategy_core::risk::{RiskControlConfig, RiskController, RiskState};
use strategy_positions::PositionState;

use crate::core::bar::Bar;

/// 风控兜底子系统。
///
/// 持有 `RiskController`（统一风控配置）与 `session_open_price`（开盘价记录）。
/// `pos`/`cooldown` 由 `PositionSync` 持有，本模块通过 `&PositionState` 只读访问。
pub struct RiskGuard {
    /// 统一风控兜底（与现有回测引擎同一个 `RiskController`）。
    pub risk: RiskController,
    /// 最近一次日盘/夜盘开盘价（桥接 `Ctx::session_open_price` 口径）。
    pub session_open_price: Option<f64>,
}

impl RiskGuard {
    pub fn new() -> Self {
        Self {
            risk: RiskController::new(RiskControlConfig::default()),
            session_open_price: None,
        }
    }

    pub fn with_config(cfg: RiskControlConfig) -> Self {
        Self {
            risk: RiskController::new(cfg),
            session_open_price: None,
        }
    }

    /// 记录开盘价（禁交易窗口内同样更新）。口径源自旧 `Engine` 的开盘价记录，
    /// 该模块已随 R4 退役删除（见 `docs/exchange-spi-retirement.md` §5.1）。
    pub fn record_session_open(&mut self, price: f64) {
        self.session_open_price = Some(price);
    }

    /// 优先级沿袭旧 `Engine::check_risk_fallback`（已随 R4 退役删除）；实际判定
    /// 委托 `RiskController::check_exit`——`crates/core/src/risk.rs:103-106`：
    /// 固定止损 > 回落止盈 > 固定止盈。检测价用收盘价；trail 状态就地回写仓位。
    pub fn check_fallback(
        &mut self,
        bar: &Bar,
        pos: Option<&mut PositionState>,
    ) -> Option<strategy_core::risk::ExitSignal> {
        let p = pos?;
        let pnl_pct = p.pnl_pct(bar.close);
        let mut rs = RiskState {
            trail_activated: p.trail_activated,
            trail_trigger_pct: p.trail_trigger_pct,
        };
        let exit = self.risk.check_exit(pnl_pct, p.max_pnl_pct, &mut rs)?;
        p.trail_activated = rs.trail_activated;
        p.trail_trigger_pct = rs.trail_trigger_pct;
        Some(exit)
    }
}

impl Default for RiskGuard {
    fn default() -> Self {
        Self::new()
    }
}
