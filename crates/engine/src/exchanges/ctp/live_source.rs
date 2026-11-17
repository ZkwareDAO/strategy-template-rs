//! CTP 实盘行情源（§7.3 `live_source.rs`）。
//!
//! 与币安共用 `ChannelBarSource` 原语（原则 3：数据端口的搬运逻辑
//! 同一套）；CTP 差异全在**注入的规则**里：`session_rules` 的时段表
//! 驱动 `SessionWindowPolicy` 桶闭合，日界切换由 runner 的 day-boundary
//! 兜底收线（阶段 3 v2.6 落地），不需要源侧注入 SessionGap——
//! 实盘逐根到达的 1m 天然带边界信息。
//!
//! 午休/夜盘结束后的第一根帧可能跨小时级间隔：水位去重仍按
//! `open_time` 严格递增（重连补缺语义与 `Feed` 对齐，§12-7）。

use std::sync::mpsc::Receiver;

use strategy_core::candle::Candle;

use crate::core::identity::InstrumentId;
use crate::exchanges::live::ChannelBarSource;

/// 装配 CTP live 行情源（instrument 必须是具体合约，如 `CU2601`）。
pub fn ctp_live_source(instrument: InstrumentId, rx: Receiver<Candle>) -> ChannelBarSource {
    ChannelBarSource::new(instrument, rx)
}
