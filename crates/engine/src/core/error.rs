//! engine 层统一错误。

use chrono::{DateTime, Utc};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, SpiError>;

#[derive(Debug, Error)]
pub enum SpiError {
    #[error("未注册的交易所: {0}")]
    UnknownExchange(String),

    #[error("标的解析失败 [{exchange}]: {raw}")]
    InstrumentParse { exchange: String, raw: String },

    #[error("未找到合约规则: {0}")]
    MissingSpec(String),

    #[error("事件源错误: {0}")]
    Source(String),

    #[error("订单出口错误: {0}")]
    Execution(String),

    #[error("规则校验失败: {0}")]
    RuleViolation(String),

    #[error("非完整 K 线不得进入聚合: {instrument} @ {open_time}")]
    BarNotFinal {
        instrument: String,
        open_time: DateTime<Utc>,
    },

    #[error("不支持的时间框架: {tf} ({instrument})")]
    UnsupportedTimeframe { tf: String, instrument: String },

    #[error("平仓拆分失败: {0}")]
    CloseSplit(String),

    #[error("订阅解析失败: {0}")]
    Subscription(String),

    #[error("配置错误: {0}")]
    Config(String),
}
