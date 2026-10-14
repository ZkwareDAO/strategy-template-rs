//! strategy-app 库层 —— 回测引擎 + live RuntimeActor 装配
//!
//! - [`engine`]：回测引擎（Feed 1m 分发 + Strategy + PositionManager +
//!   BacktestOrderClient + 独立 Store/CandleClock，结束 flush 一次）
//! - [`runtime_actor`]：live 监督式策略 Actor（回填/订阅/决策/下单/恢复，
//!   共享进程级 Arc<Store>，定时+阈值+关停 flush）
//! - [`bootstrap`]：live 进程装配（共享数据/交易客户端、进程级 Store、拓扑命名）
//! - [`report`] / [`report_charts`]：回测产物（result.json、3 张 PNG、trade/equity 读取）
//! - 对外旁路固定列 CSV：signal.csv、exchange_orders.csv、history_positions 22 列

pub mod bootstrap;
pub mod config;
pub mod engine;
pub mod order_router;
pub mod report;
pub mod report_charts;
pub mod runtime_actor;
