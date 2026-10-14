//! Strategy trait 与策略上下文（Ctx）
//!
//! 设计约束（移植自 Python 红线 #8/#10/#12）：
//! - 指标只允许在策略内用 [`Ctx::closed_data`]（已闭合序列）计算
//! - 未闭合 K 线在 Ctx 上不可见 —— 类型层面隔离未来函数
//! - 信号时间戳一律取 K 线时间，Ctx 不提供 now()
//! - 持仓状态由引擎持有并经 [`Ctx::position`] 注入（单一事实来源，
//!   崩溃恢复后策略自动与 PositionManager 一致）

use chrono::{DateTime, Utc};

use crate::candle::{Candle, Timeframe};
use crate::signal::{Action, Side};

/// 已闭合数据源（由引擎的 Feed 实现；测试可用 stub 实现）
pub trait ClosedSource {
    /// 某时间框架的已闭合 K 线序列（升序，末尾为最新已闭合）
    fn closed(&self, tf: Timeframe) -> Option<&[Candle]>;
    /// 最近一根已闭合 1m 的收盘价
    fn last_price(&self) -> Option<f64>;
}

/// 止损冷却（引擎注入 Ctx；策略只查询不构造）
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

/// 策略上下文：策略经由 Ctx 读取已闭合数据与持仓状态
pub struct Ctx<'a> {
    source: &'a dyn ClosedSource,
    /// 当前实时价（1m 粒度 = 最新已闭合 1m 收盘价 / 实盘 tick 最新价）
    pub current_price: f64,
    /// 触发本次调用的 1m K 线（含 open/high/low/close；实盘 tick 为未闭合快照，
    /// 已闭合 1m 为完整根）。策略可按 exit_price_source 配置选择 close 或
    /// high/low 判定止损。
    pub current_candle: Candle,
    /// 触发本次调用的 1m K 线开盘时间（= 事件时间，signal_id 输入）
    pub event_time: DateTime<Utc>,
    /// 当前持仓（None = 空仓）
    pub position: Option<PositionView>,
    /// 止损冷却（None = 无冷却；默认 UntilDate 当日，UntilTime 按 cooldown_bars）
    pub cooldown: Option<Cooldown>,
}

impl<'a> Ctx<'a> {
    pub fn new(
        source: &'a dyn ClosedSource,
        current_price: f64,
        current_candle: Candle,
        event_time: DateTime<Utc>,
        position: Option<PositionView>,
        cooldown: Option<Cooldown>,
    ) -> Self {
        Self {
            source,
            current_price,
            current_candle,
            event_time,
            position,
            cooldown,
        }
    }

    /// 取某时间框架的已闭合 K 线序列（升序）。
    /// 返回 None 表示该时间框架未注册或无数据。
    pub fn closed_data(&self, tf: Timeframe) -> Option<&[Candle]> {
        self.source.closed(tf)
    }

    /// 已闭合序列 + 最少根数检查（镜像 Python get_closed_data(min_rows=...)）
    pub fn closed_data_min(&self, tf: Timeframe, min_rows: usize) -> Option<&[Candle]> {
        self.source.closed(tf).filter(|v| v.len() >= min_rows)
    }

    /// 事件日（UTC 日期，"YYYY-MM-DD"）—— 用于止损冷却判断（K 线时间，禁 now()）
    pub fn event_date(&self) -> String {
        self.event_time.format("%Y-%m-%d").to_string()
    }

    /// 当前是否处于止损冷却
    pub fn in_stop_loss_cooldown(&self) -> bool {
        match &self.cooldown {
            // bar_end（event+1m）日历日 == 冷却日
            Some(Cooldown::UntilDate(d)) => {
                let bar_end = self.event_time + chrono::Duration::minutes(1);
                &bar_end.format("%Y-%m-%d").to_string() == d
            }
            None => false,
        }
    }
}

/// 策略 trait — Rust 版对齐 Python BaseStrategyCore
///
/// 生命周期：构造 → [`Strategy::timeframes`]（注册时间框架）→
/// 反复 [`Strategy::on_candle`]（每根已闭合 1m K 线一次）→
/// [`Strategy::on_realtime_tick`]（实盘每帧/回测每根 1m，做盘中止损）。
pub trait Strategy {
    /// 策略类型名（进入确定性 signal_id，如 "sar_snt3"）
    fn strategy_type(&self) -> &str;

    /// 声明需要的 K 线时间框架（含 1m；聚合器据此注册）
    fn timeframes(&self) -> Vec<Timeframe>;

    /// 入场逻辑：每根已闭合 1m K 线触发一次（镜像 Python analyze()）。
    /// 引擎保证空仓时才调用；有持仓时调用 on_realtime_tick。
    fn on_candle(&mut self, ctx: &Ctx<'_>) -> Vec<Action>;

    /// 出场逻辑（镜像 Python check_realtime_exit）。
    /// 引擎保证有持仓时才调用。返回空 = 持有。
    fn on_realtime_tick(&mut self, ctx: &Ctx<'_>) -> Vec<Action> {
        let _ = ctx;
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    struct EmptySource;
    impl ClosedSource for EmptySource {
        fn closed(&self, _tf: Timeframe) -> Option<&[Candle]> {
            None
        }
        fn last_price(&self) -> Option<f64> {
            None
        }
    }

    fn candle() -> Candle {
        Candle {
            open_time: chrono::Utc::now(),
            open: 100.0,
            high: 100.0,
            low: 100.0,
            close: 100.0,
            volume: 0.0,
            quote_volume: 0.0,
            trade_num: 0,
            active_buy_volume: 0.0,
            active_buy_quote_volume: 0.0,
            is_closed: true,
        }
    }

    fn ctx_at(t: DateTime<Utc>, cooldown: Option<Cooldown>) -> Ctx<'static> {
        Ctx::new(&EmptySource, 100.0, candle(), t, None, cooldown)
    }

    #[test]
    fn until_date_uses_bar_end_calendar_day() {
        // 判定按 event_time + 1m（Python 的 bar 时间戳 = bar_end）的日历日
        let cd = Some(Cooldown::UntilDate("2024-01-03".into()));
        // event 15:59（Python current 16:00）→ 当日，拦截
        let t = Utc.with_ymd_and_hms(2024, 1, 3, 15, 59, 0).unwrap();
        assert!(ctx_at(t, cd.clone()).in_stop_loss_cooldown());
        // event 23:58（Python 23:59）→ 当日，拦截
        let t1 = Utc.with_ymd_and_hms(2024, 1, 3, 23, 58, 0).unwrap();
        assert!(ctx_at(t1, cd.clone()).in_stop_loss_cooldown());
        // event 23:59（Python current 次日 00:00）→ 翻日，放行
        let t2 = Utc.with_ymd_and_hms(2024, 1, 3, 23, 59, 0).unwrap();
        assert!(!ctx_at(t2, cd).in_stop_loss_cooldown());
    }

    #[test]
    fn no_cooldown_means_never_blocked() {
        assert!(
            !ctx_at(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(), None)
                .in_stop_loss_cooldown()
        );
    }
}
