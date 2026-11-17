//! 框架层：trait 定义 + 通用实现 + 主循环。
//!
//! 分层铁律（§4）：**exchanges/ 可以依赖 core/，反之不行**；
//! 本层不出现任何具体交易所名。
pub mod action_dispatcher;
pub mod aggregation;
pub mod bar;
pub mod error;
pub mod exchange;
pub mod execution;
pub mod identity;
pub mod position;
pub mod position_sync;
pub mod replay;
pub mod risk_guard;
pub mod runtime;
pub mod session;
pub mod subscription;
pub mod trading;
pub mod types;
