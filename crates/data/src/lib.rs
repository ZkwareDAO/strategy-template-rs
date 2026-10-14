//! strategy-data — 数据层
//!
//! - [`csv_store`]: K 线 CSV 读写（回测回放直接读文件）
//! - [`aggregate`]: 1m→大周期纯聚合（桶=open_time 左闭区间，丢弃残缺首桶）
//! - [`feed`]: 1m 事件分发器（回测/live 增量聚合管线）
//! - [`client`]: KlineClient 端口（回填 + 订阅，设计文档 §4.1）
//! - [`remote_client`]: kline-service WS/HTTP 适配器（配置驱动）
//! - [`archive_download`]: Binance 公共归档历史 K 线下载（无 API key）：
//!   `download` CLI 显式导入 + run 启动时 `ensure_recent_history` 自动确保近一年；
//!   下载失败仅 warn，不在交易关键链路（warmup 另有 zkware 回填兜底）

pub mod aggregate;
pub mod archive_download;
pub mod client;
pub mod csv_store;
pub mod feed;
pub mod remote_client;
pub mod warmup;

pub use client::{KlineBar, KlineClient, KlineEvent, KlineStream};
pub use remote_client::{KlineServiceConfig, RemoteKlineServiceClient};
