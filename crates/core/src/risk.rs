//! 统一风控 — 逐行对齐 Python `strategy_core/base/risk_control.py` + `risk_config.py`
//!
//! 职责：策略特有出场逻辑（`on_realtime_tick`）不触发时的**兜底**出场检查，
//! 优先级与 Python 一致：固定止损 > 回落止盈 > 固定止盈，首个命中即返回。
//!
//! 语义红线（对齐 Python）：
//! - 检测价只用 current_price（close），不使用 bar high/low（那是策略特有层的事）
//! - 回落止盈：`pnl >= activation_pct` 激活；触发线 = 最大盈利 × (1 - drawdown/100)，
//!   创新高时刷新；`trail_activated`/`trail_trigger_pct` 是持仓级状态（由调用方
//!   经 [`RiskState`] 持有，崩溃恢复不丢失）
//! - 固定止盈 0 = 禁用；`enabled=false` 整体禁用

/// 回落止盈配置（百分比均为数值形式，如 20 表示 20%）
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct TrailingProfitConfig {
    pub enabled: bool,
    /// 激活阈值（%，盈亏达到即激活回落跟踪）
    pub activation_pct: f64,
    /// 回落百分比（%，从最大盈利回落的相对比例）
    pub drawdown_pct: f64,
}

impl Default for TrailingProfitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            activation_pct: 20.0,
            drawdown_pct: 20.0,
        }
    }
}

/// 统一风控配置（镜像 Python `RiskControlConfig`）
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct RiskControlConfig {
    pub enabled: bool,
    /// 固定止损百分比（%）
    pub fixed_stop_loss_pct: f64,
    pub trailing_profit: TrailingProfitConfig,
    /// 固定止盈百分比（%），0 = 禁用
    pub fixed_take_profit_pct: f64,
}

impl Default for RiskControlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            fixed_stop_loss_pct: 20.0,
            trailing_profit: TrailingProfitConfig::default(),
            fixed_take_profit_pct: 0.0,
        }
    }
}

/// 回落止盈持仓级状态（镜像 Python BaseState 的 trail 字段；
/// 由引擎挂在仓位状态上持久化，平仓时复位）
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RiskState {
    /// 回落跟踪是否已激活
    pub trail_activated: bool,
    /// 触发百分比（最大盈利 × (1 - drawdown/100)）
    pub trail_trigger_pct: f64,
}

/// 出场信号（镜像 Python `ExitSignal`）
#[derive(Debug, Clone, PartialEq)]
pub struct ExitSignal {
    /// 出场原因（进入历史仓位 CSV 的 exit_reason）
    pub reason: String,
    /// 是否止损（true → 引擎记止损冷却日）
    pub is_stop_loss: bool,
}

/// 统一风控控制器（镜像 Python `RiskController`）
#[derive(Debug, Clone)]
pub struct RiskController {
    pub config: RiskControlConfig,
}

impl RiskController {
    pub fn new(config: RiskControlConfig) -> Self {
        Self { config }
    }

    /// 检查兜底出场。
    ///
    /// - `pnl_pct`：当前盈亏百分比（调用方按方向算好，Python 亦如此）
    /// - `max_pnl_pct`：持仓期间最大盈利百分比（>=0，来自 PositionState）
    /// - `risk_state`：回落止盈持仓级状态（本方法会就地更新激活状态）
    ///
    /// 返回 None = 继续持有。
    pub fn check_exit(
        &self,
        pnl_pct: f64,
        max_pnl_pct: f64,
        risk_state: &mut RiskState,
    ) -> Option<ExitSignal> {
        if !self.config.enabled {
            return None;
        }
        // 优先级：固定止损 > 回落止盈 > 固定止盈
        self.check_fixed_stop_loss(pnl_pct)
            .or_else(|| self.check_trailing_profit(pnl_pct, max_pnl_pct, risk_state))
            .or_else(|| self.check_fixed_take_profit(pnl_pct))
    }

    /// 固定止损：pnl <= -fixed_stop_loss_pct
    fn check_fixed_stop_loss(&self, pnl_pct: f64) -> Option<ExitSignal> {
        if pnl_pct > -self.config.fixed_stop_loss_pct {
            return None;
        }
        Some(ExitSignal {
            reason: format!("固定止损: {pnl_pct:.2}%"),
            is_stop_loss: true,
        })
    }

    /// 回落止盈（百分比回落模式）：
    /// 激活：pnl >= activation_pct → 触发线 = max_pnl × (1 - drawdown/100)
    /// 触发：已激活且 pnl <= 触发线
    fn check_trailing_profit(
        &self,
        pnl_pct: f64,
        max_pnl_pct: f64,
        risk_state: &mut RiskState,
    ) -> Option<ExitSignal> {
        let tp = &self.config.trailing_profit;
        if !tp.enabled {
            return None;
        }

        // 激活检查（创新高时刷新触发线，与 Python 一致）
        if pnl_pct >= tp.activation_pct {
            risk_state.trail_activated = true;
            risk_state.trail_trigger_pct = max_pnl_pct * (1.0 - tp.drawdown_pct / 100.0);
        }

        // 触发检查
        if risk_state.trail_activated && pnl_pct <= risk_state.trail_trigger_pct {
            return Some(ExitSignal {
                reason: format!("回落止盈: max={max_pnl_pct:.2}%, 当前={pnl_pct:.2}%"),
                is_stop_loss: false,
            });
        }
        None
    }

    /// 固定止盈：fixed_take_profit_pct > 0 且 pnl >= 阈值
    fn check_fixed_take_profit(&self, pnl_pct: f64) -> Option<ExitSignal> {
        if self.config.fixed_take_profit_pct <= 0.0 || pnl_pct < self.config.fixed_take_profit_pct {
            return None;
        }
        Some(ExitSignal {
            reason: format!("固定止盈: {pnl_pct:.2}%"),
            is_stop_loss: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Python 默认配置：enabled, 止损 20%, trailing(20, 20), 固定止盈 0
    fn default_cfg() -> RiskControlConfig {
        RiskControlConfig::default()
    }

    #[test]
    fn disabled_config_never_exits() {
        let mut cfg = default_cfg();
        cfg.enabled = false;
        let rc = RiskController::new(cfg);
        let mut rs = RiskState::default();
        assert!(rc.check_exit(-50.0, 0.0, &mut rs).is_none());
        assert!(rc.check_exit(50.0, 50.0, &mut rs).is_none());
    }

    #[test]
    fn fixed_stop_loss_triggers_at_threshold() {
        let mut cfg = default_cfg();
        cfg.fixed_stop_loss_pct = 2.0;
        let rc = RiskController::new(cfg);
        let mut rs = RiskState::default();

        // -1.99% 未触发
        assert!(rc.check_exit(-1.99, 0.0, &mut rs).is_none());
        // -2.0% 触发（Python: pnl > -x 不触发，即 <= -x 触发）
        let sig = rc.check_exit(-2.0, 0.0, &mut rs).unwrap();
        assert!(sig.is_stop_loss);
        assert!(sig.reason.contains("固定止损"));
    }

    #[test]
    fn trailing_profit_activation_and_trigger() {
        let mut cfg = default_cfg();
        cfg.trailing_profit = TrailingProfitConfig {
            enabled: true,
            activation_pct: 2.0,
            drawdown_pct: 20.0,
        };
        let rc = RiskController::new(cfg);
        let mut rs = RiskState::default();

        // 未激活：pnl 1.9% 不触发
        assert!(rc.check_exit(1.9, 1.9, &mut rs).is_none());

        // 激活：pnl 2.5%，max 2.5% → trigger = 2.5 × 0.8 = 2.0；pnl 2.5 > 2.0 不触发
        assert!(rc.check_exit(2.5, 2.5, &mut rs).is_none());
        assert!(rs.trail_activated);
        assert!((rs.trail_trigger_pct - 2.0).abs() < 1e-9);

        // 创新高刷新触发线：max 3.0 → trigger = 2.4
        assert!(rc.check_exit(3.0, 3.0, &mut rs).is_none());
        assert!((rs.trail_trigger_pct - 2.4).abs() < 1e-9);

        // 回落至触发线内：pnl 2.3 <= 2.4 → 触发，非止损
        let sig = rc.check_exit(2.3, 3.0, &mut rs).unwrap();
        assert!(!sig.is_stop_loss);
        assert!(sig.reason.contains("回落止盈"));
    }

    #[test]
    fn trailing_profit_never_activated_never_triggers() {
        let mut cfg = default_cfg();
        cfg.trailing_profit = TrailingProfitConfig {
            enabled: true,
            activation_pct: 2.0,
            drawdown_pct: 20.0,
        };
        let rc = RiskController::new(cfg);
        let mut rs = RiskState::default();

        // pnl 大跌但从未达到激活阈值 → 永不触发回落止盈
        assert!(rc.check_exit(1.0, 1.0, &mut rs).is_none());
        assert!(rc.check_exit(-10.0, 1.0, &mut rs).is_none());
        assert!(!rs.trail_activated);
    }

    #[test]
    fn trailing_profit_disabled() {
        let mut cfg = default_cfg();
        cfg.trailing_profit.enabled = false;
        let rc = RiskController::new(cfg);
        let mut rs = RiskState::default();
        assert!(rc.check_exit(50.0, 50.0, &mut rs).is_none());
        assert!(!rs.trail_activated);
    }

    #[test]
    fn fixed_take_profit_disabled_at_zero() {
        let rc = RiskController::new(default_cfg()); // fixed_take_profit_pct = 0
        let mut rs = RiskState::default();
        assert!(rc.check_exit(100.0, 100.0, &mut rs).is_none());
    }

    #[test]
    fn fixed_take_profit_triggers() {
        let mut cfg = default_cfg();
        cfg.fixed_take_profit_pct = 5.0;
        cfg.trailing_profit.enabled = false;
        let rc = RiskController::new(cfg);
        let mut rs = RiskState::default();

        assert!(rc.check_exit(4.99, 4.99, &mut rs).is_none());
        let sig = rc.check_exit(5.0, 5.0, &mut rs).unwrap();
        assert!(!sig.is_stop_loss);
        assert!(sig.reason.contains("固定止盈"));
    }

    #[test]
    fn priority_stop_loss_beats_trailing() {
        // 同时满足固定止损与回落止盈 → 固定止损优先（is_stop_loss=true）
        let mut cfg = default_cfg();
        cfg.fixed_stop_loss_pct = 2.0;
        cfg.trailing_profit = TrailingProfitConfig {
            enabled: true,
            activation_pct: 1.0,
            drawdown_pct: 90.0,
        };
        let rc = RiskController::new(cfg);
        let mut rs = RiskState {
            trail_activated: true,
            trail_trigger_pct: 10.0,
        };

        let sig = rc.check_exit(-3.0, 10.0, &mut rs).unwrap();
        assert!(sig.is_stop_loss);
        assert!(sig.reason.contains("固定止损"));
    }

    #[test]
    fn priority_trailing_beats_take_profit() {
        // 同时满足回落止盈与固定止盈 → 回落止盈优先
        let mut cfg = default_cfg();
        cfg.trailing_profit = TrailingProfitConfig {
            enabled: true,
            activation_pct: 2.0,
            drawdown_pct: 50.0,
        };
        cfg.fixed_take_profit_pct = 3.0;
        let rc = RiskController::new(cfg);
        let mut rs = RiskState::default();

        // max 4% → trigger = 2.0；pnl 2.0 <= 2.0 → 回落止盈（固定止盈 3% 未到）
        let sig = rc.check_exit(2.0, 4.0, &mut rs).unwrap();
        assert!(sig.reason.contains("回落止盈"));
    }
}
