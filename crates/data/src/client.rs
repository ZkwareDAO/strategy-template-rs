//! KlineClient 端口（设计文档 §3.1/§4.1）
//!
//! 数据层只认识本端口：历史回填 + 实时订阅。回测直接读 CSV（不经端口）；
//! live 由 [`crate::remote_client::RemoteKlineServiceClient`] 对接
//! kline-service（端点全部来自配置 services.kline）。

use std::future::Future;
use std::pin::Pin;

use strategy_core::candle::{Candle, Timeframe};
use tokio::sync::broadcast;

/// boxed 异步回填结果（使 trait 可 dyn）
pub type BackfillFuture<'a> =
    Pin<Box<dyn Future<Output = anyhow::Result<Vec<Candle>>> + Send + 'a>>;

/// 实时订阅事件：闭合 K 线 / 未闭合快照 / 重连通知，同一条有序流。
///
/// 进程级一条 WS 由共享分发器按 symbol 扇出（每 symbol 一个 broadcast 通道）。
/// receiver 消费落后超容量时 recv 返回 `RecvError::Lagged(n)`，调用方自愈
/// （backfill 补缺）；慢 receiver 不影响其他 symbol（broadcast 语义）。
#[derive(Debug, Clone)]
pub enum KlineEvent {
    /// 已闭合 K 线（驱动策略；落后时经 Lagged 自愈，不阻塞其他 symbol）
    Closed(KlineBar),
    /// 未闭合快照（最新价；环形缓冲，最旧先出）
    Tick(KlineBar),
    /// 每次（重）连成功广播一次，调用方据此触发历史补缺
    Reconnected,
}

/// 实时订阅接收端：drop 即注销
pub struct KlineStream {
    pub rx: broadcast::Receiver<KlineEvent>,
}

/// 一根 K 线（symbol 随消息携带）
#[derive(Debug, Clone)]
pub struct KlineBar {
    pub symbol: String,
    pub candle: Candle,
}

/// 数据客户端端口（dyn 兼容：async 用 boxed future 表达）
pub trait KlineClient: Send + Sync {
    /// 历史回填：取最近 `bars` 根已闭合 K 线（升序）
    fn backfill<'a>(
        &'a self,
        symbol: &'a str,
        timeframe: Timeframe,
        bars: usize,
    ) -> BackfillFuture<'a>;

    /// 注册式订阅：在已启动的共享分发器上注册一个接收端。
    /// 不触发连接/订阅帧；drop 返回的 KlineStream 即注销。
    /// 未在启动集合中的 symbol → warn + 永不收帧的空流（防御编程错误）。
    fn attach(&self, symbol: &str, timeframe: Timeframe) -> KlineStream;
}
