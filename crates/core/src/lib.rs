//! strategy-core — 核心类型、状态机与 Strategy trait
//!
//! 纯逻辑 crate（零 IO）：
//! - [`candle`]：K 线/时间框架
//! - [`signal`]：策略决策（Action/Signal/SignalType）+ 交易意图
//!   （SignalIntent）与确定性 signal_id
//! - [`lifecycle`]：信号 8 态生命周期状态机（设计文档 §5.2）
//! - [`order`]：订单 7 态状态机 + 部分成交累积（§5.3）
//! - [`fill`]：完全成交通知 FillEvent
//! - [`position_math`]：加仓均价 / 减仓已实现盈亏纯函数（§5.5）
//! - [`strategy`]：Strategy trait 与 Ctx
//! - [`risk`]：统一风控兜底规则
//!
//! 流水线：1m K线流 → 聚合大周期（仅已闭合） → 指标 →
//! [`Strategy::on_candle`] → [`Action`] → [`SignalIntent`]。

pub mod candle;
pub mod error;
pub mod fill;
pub mod lifecycle;
pub mod order;
pub mod position_math;
pub mod risk;
pub mod signal;
pub mod strategy;

pub use candle::{Candle, Timeframe};
pub use error::{Error, Result};
pub use fill::FillEvent;
pub use lifecycle::SignalStatus;
pub use order::{OrderProgress, OrderState};
pub use position_math::{realized_pnl, weighted_avg};
pub use signal::{
    generate_signal_id, intents_from_action, Action, IntentCtx, IntentKind, Side, Signal,
    SignalIntent, SignalType,
};
pub use strategy::{Ctx, Strategy};
