//! strategy-data — 数据层
//!
//! - [`csv_store`]: K 线 CSV 读写（回测回放直接读文件）
//! - [`aggregate`]: 1m→大周期纯聚合（桶=open_time 左闭区间，丢弃残缺首桶）
//! - [`client`]: KlineClient 端口（回填 + 订阅，设计文档 §4.1）——live 实现在
//!   `app/src/ts_kline.rs`（`TradingServiceKlineClient`，走 trading-service 连接的
//!   `subscribe_kline` 定稿推送；klines-service 已退役）
//! - [`archive_download`]: Binance 公共归档历史 K 线下载（无 API key）：
//!   `download` CLI 显式导入 + run 启动时 `ensure_recent_history` 自动确保近一年；
//!   下载失败仅 warn，不在交易关键链路（warmup 另有 `get_klines` 远端回填 + 本地 CSV 兜底）

pub mod aggregate;
pub mod archive_download;
pub mod client;
pub mod csv_store;
pub mod warmup;

pub use client::{KlineBar, KlineClient, KlineEvent, KlineStream};
