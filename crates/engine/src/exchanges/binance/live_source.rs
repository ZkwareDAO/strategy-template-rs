//! 币安实盘行情源（§7.3 `live_source.rs`）。
//!
//! 7x24 连续市场：`ChannelBarSource` 直桥 WS 已闭合 1m，无时段边界语义；
//! 未闭合 Tick 快照丢弃（决策只跑在闭合上，首公理）。重连补缺由外层
//! actor 按水位幂等重发（`ChannelBarSource::watermark` 同源口径），
//! 主循环零 `if mode` 分叉。

use std::sync::mpsc::Receiver;

use strategy_core::candle::Candle;

use crate::core::identity::InstrumentId;
use crate::exchanges::live::ChannelBarSource;

/// 装配币安 live 行情源：外层（app 组装根）把 `KlineEvent::Closed`
/// 按 symbol 过滤后经 channel 送入。
pub fn binance_live_source(instrument: InstrumentId, rx: Receiver<Candle>) -> ChannelBarSource {
    ChannelBarSource::new(instrument, rx)
}
