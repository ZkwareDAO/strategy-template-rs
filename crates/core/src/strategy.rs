//! 策略共享状态类型：止损冷却与持仓视图。

use crate::signal::Side;

/// 止损冷却（引擎注入快照；策略只查询不构造）
#[derive(Debug, Clone, PartialEq)]
pub enum Cooldown {
    /// 拦截到当日结束：bar_end（event_time+1m）的日历日 == 此日期即冷却。
    /// Python 用 1m bar 结束时刻（=Rust 触发 1m open_time+1m）判定。
    UntilDate(String),
}

/// 持仓视图（引擎注入；策略只读）
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PositionView {
    pub side: Side,
    pub entry_price: f64,
    pub stop_price: f64,
    pub peak_price: f64,
    pub max_pnl_pct: f64,
    pub min_pnl_pct: f64,
    /// SAR 触及制离场状态（从持久化持仓行注入；重启后继续保护）
    pub exit_active: bool,
    pub exit_price: f64,
}
