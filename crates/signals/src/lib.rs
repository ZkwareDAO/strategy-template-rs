//! strategy-signals — 信号管理
//!
//! 两条存储线（设计文档 D6：对外报表契约锁定，内部状态独立演进）：
//! - [`writer`] / [`record`]：对外 signal.csv 追加写（列序固定，下游按列名消费）
//! - 信号当前态：[`manager::ManagedSignal`] 是统一 Store（strategy-store）中
//!   `NAME = "signal"` 的 model——行常驻表内（含 Rejected/Failed/Cancelled
//!   终态，审计红线），定时 flush 到 `{base}/{YYYYMMDD}/signal.csv`，启动跨日
//!   去重恢复。多策略实例共享同一张表，按 strategy_name 过滤。
//!
//! 生命周期管理（设计文档 §5.2）：
//! - [`manager`]：同步内核 SignalManager（回测直调；验证/提交/重试/对账）
//! - [`gateway`]：OrderGateway 端口（同步回测 / async live 两个契约）
//!
//! 对外 signal.csv 契约（列序固定）：
//! strategy_name, signal_id, timestamp, user_id, signal_action, symbol,
//! pos_type, cash, leverage, side, exchange, trigger_price, order_type, quantity

pub mod gateway;
pub mod manager;
pub mod record;
pub mod writer;

pub use gateway::{
    AsyncOrderGateway, FailureKind, OrderGateway, OrderSnapshot, QueryFuture, ReadyFill,
    SubmitError, SubmitFuture,
};
pub use manager::{
    BasicValidator, ManagedSignal, RecoveryAction, RetryPolicy, SignalManager, SignalValidator,
    SubmitRequest,
};
pub use record::{OrderType, PosType, Side, SignalRecord};
pub use writer::SignalWriter;
