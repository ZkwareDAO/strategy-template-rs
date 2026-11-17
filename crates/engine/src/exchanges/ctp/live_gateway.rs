//! CTP 实盘订单出口（§7.3 `live_gateway.rs`，§13-4 验收主战场）。
//!
//! 国内期货净仓+拆腿语义（§2.3）：runner 经 `CtpPositionRules::split_close`
//! 已把平今/平昨拆成带 `Offset` 的 `OrderRequest`，本装配
//! `emit_offset=true`——`SignalIntent.offset` 落标签，`trading_service`
//! 出帧带 `offset` 字段（「实盘订单帧带平今/平昨」）。手续费按
//! `TradingRules`（费率表单一来源）给出期望值 `expected_fee`，与
//! 回报回传 fee 对账（契约测试 `fee_three_way_same_source`）。

use std::sync::Arc;

use strategy_signals::manager::SignalManager;

use crate::core::identity::InstrumentId;
use crate::core::position::PositionRules;
use crate::core::session::SessionRules;
use crate::core::trading::TradingRules;
use crate::exchanges::live::LiveExecution;

/// 装配 CTP live 订单出口（emit_offset=true，见模块头）。
///
/// `initial_cash` = 本地镜像权益起账（PR-5 T2；`StrategyLiveSpec` 按标的均分）。
pub fn ctp_live_execution(
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
        true,
        initial_cash,
    )
}
