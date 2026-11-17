//! 策略运行时层：单标的、横截面与组合策略共用的接口、模型与执行骨架。
//!
//! 模块：`config` / `signal_id` / `features` / `evaluator` / `port` /
//! `align` / `dispatch` / `runner`——策略实现统一走 `port` 的
//! [`CrossSectionalStrategyPort`](port::CrossSectionalStrategyPort)。

pub mod align;
pub mod config;
pub mod dispatch;
pub mod evaluator;
pub mod features;
pub mod model;
pub mod port;
pub mod runner;
pub mod signal_id;
