//! strategy-positions — 仓位管理（S4：统一 Store 后端）
//!
//! 统一 Store model（进程共享，定时 flush `{base}/{YYYYMMDD}/{model}.csv`，
//! 启动跨日去重恢复）：
//!
//! - [`manager::ManagedPosition`]：当前持仓（`NAME="position"`，id=position_id，
//!   平仓软删）
//! - [`manager::ManagedStop`]：止损单当前态（`NAME="stop"`，终态留审计）
//! - [`manager::Cooldown`]：止损冷却日（`NAME="cooldown"`）
//! - [`history::HistoryPosition`]：平仓历史（`NAME="history_position"`，只增）
//!
//! 对外固定列报表（D6 契约，旁路导出）：平仓时写
//! `history_positions/{strategy_name}/{YYYYMMDD}.csv`（22 列，Python/report
//! 按列名消费；与 signal.csv 同性质）。
//!
//! [`manager::PositionManager`]：per-symbol 仓位生命周期（开/加/减/平、峰值跟踪、
//! 止损价/回落止盈更新、冷却日、恢复）。

pub mod history;
pub mod manager;
pub mod state;

pub use history::{HistoryPosition, HistoryPositionLogger};
pub use manager::{list_positions, Cooldown, ManagedPosition, ManagedStop, PositionManager};
pub use state::{PositionState, Side};
