//! 撮合内核接收的交易动作。

use strategy_core::signal::Side;

/// 撮合动作（`Broker::execute` 的输入）。
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// 开仓（price=触发价；stop_price=初始止损；atr=入场 ATR）
    Open {
        side: Side,
        price: f64,
        stop_price: f64,
        atr: f64,
        strength: f64,
        reason: String,
    },
    /// 平仓（price=平仓触发价）
    Close {
        price: f64,
        reason: String,
        is_stop_loss: bool,
        strength: f64,
    },
    /// 反手 = 先平后开（平仓失败则放弃开仓）
    Reverse {
        side: Side,
        price: f64,
        stop_price: f64,
        atr: f64,
        strength: f64,
        close_reason: String,
        is_stop_loss: bool,
    },
    /// 移动止损到新价位
    UpdateStop {
        stop_price: f64,
    },
    /// 回写持仓离场状态（不产生订单）
    UpdateExit {
        exit_active: bool,
        exit_price: f64,
    },
    Hold,
}
