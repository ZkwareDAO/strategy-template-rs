//! strategy-app 库层 —— engine 回测引擎 + live engine Live Actor 装配
//!
//! - [`spi_backtest`]：engine 回测后端装配（ExchangeRunner 统一主循环）
//! - [`bootstrap`]：live 进程装配（共享数据/交易客户端、进程级 Store、拓扑命名）
//! - [`report`] / [`report_charts`]：回测产物（result.json、3 张 PNG、trade/equity 读取）
//! - 对外旁路固定列 CSV：signal.csv、history_positions 22 列
//!   （`exchange_orders.csv` 为**旧链专属产物**——由 `BacktestBroker` 落盘；
//!   engine 统一链不产该文件，钉于 `tests/backtest_e2e.rs:201` 的 `!exists` 断言）

pub mod bootstrap;
pub mod config;
pub mod inspect;
pub mod order_router;
pub mod report;
pub mod report_charts;
pub mod runtime_types;
pub mod spi_backtest;
pub mod strategy_config;
pub mod strategy_live_actor;
pub mod ts_kline;
