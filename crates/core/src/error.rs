//! 错误体系

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("配置错误: {0}")]
    Config(String),
    #[error("数据不足: {0}")]
    InsufficientData(String),
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("CSV 错误: {0}")]
    Csv(#[from] csv::Error),
    #[error("信号类型非法: {0}")]
    InvalidSignalType(String),
}
