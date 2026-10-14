//! strategy-indicators — 技术指标封装（talib-rs）
//!
//! talib-rs 是纯 Rust TA-Lib 替代（自我声明与 C TA-Lib 数值一致）。
//! 输出语义：前 lookback 个值为 NaN（与 TA-Lib 一致），调用方须先做
//! 数据充足性检查（镜像 Python 红线 #4/#5：禁止跳过数据不足检查）。

pub mod efficiency;
pub mod sar_compat;
pub mod supertrend;
pub mod talib;

pub use efficiency::efficiency_ratio;
pub use supertrend::supertrend;
pub use talib::{adx, atr, ema, rsi, sar, sma};
