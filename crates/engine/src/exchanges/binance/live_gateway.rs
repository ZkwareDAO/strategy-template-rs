//! 币安实盘订单出口（§7.3 `live_gateway.rs`）。
//!
//! crypto 净仓语义（§2.3）：**无平今/平昨**——`emit_offset=false`，
//! 出帧维持 `reduce_only` 隐式判定，与旧 live 链路逐字节一致（零回归）；
//! 信号状态机复用同一个 `SignalManager`（§17：桥不复制管道）。

use std::sync::Arc;

use strategy_signals::manager::SignalManager;

use crate::core::identity::InstrumentId;
use crate::core::position::PositionRules;
use crate::core::session::SessionRules;
use crate::core::trading::TradingRules;
use crate::exchanges::live::LiveExecution;

/// 装配币安 live 订单出口（emit_offset=false，见模块头）。
///
/// `manager` 由外层构造（绑定进程级 Store + 实例名），网关的 async
/// 提交半部在外层 actor 驱动（`take_submit_requests` →
/// `gateway.submit_fut().await` → `apply_submit_outcome`）。
///
/// `initial_cash` = 本地镜像权益起账（PR-5 T2；`StrategyLiveSpec` 按标的均分）。
pub fn binance_live_execution(
    instrument: InstrumentId,
    strategy_type: impl Into<String>,
    manager: SignalManager,
    rules: Arc<dyn TradingRules>,
    position_rules: Arc<dyn PositionRules>,
    session: Arc<dyn SessionRules>,
    initial_cash: f64,
) -> LiveExecution {
    LiveExecution::new(
        instrument,
        strategy_type,
        manager,
        rules,
        position_rules,
        session,
        false,
        initial_cash,
    )
}
