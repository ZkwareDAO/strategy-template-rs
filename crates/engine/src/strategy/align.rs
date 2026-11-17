//! 多源对齐事件源 `AlignedEventSource`（PR-2，Task 8）。
//!
//! 横截面策略持有多个 slot（标的/合约），每个 slot 是一个独立行情源
//! （[`crate::core::bar::BarEventSource`]）。各源时间线未必同步——午休、
//! 停牌、网络重连都会让某一源缺 bar 或产生额外事件。本模块把多路事件
//! 归一化成一条**确定性有序**的对齐流，供 runner 按 `(at, slot)` 逐拍消费。
//!
//! 对齐规则（排序键 `(at, rank, slot)`）：
//! - `Bar`：`at = bar.open_time`，`rank = 1`（普通 bar 排在同时刻的非 bar 之后）；
//! - `SessionGap`：`at = to.unwrap_or(from)`，`rank = 0`；
//! - `DayBoundary`：`at = 该源上一根 bar 时间 + 1m`，`rank = 0`；
//! - `Roll`：`at = effective_at`，`rank = 0`；
//! - `Reconnected`：`at = 该源上一根 bar 时间`，`rank = 0`。
//!
//! 每源只缓冲一个 pending 事件，`next_aligned` 每拍先补足所有空 pending，
//! 再发射 `(at, rank, slot)` 最小的那个；`last_at` 只在 `Bar` 时更新，
//! `Reconnected` / `DayBoundary` / `SessionGap` / `Roll` 都不改动它。

use chrono::{DateTime, Duration, Utc};

use crate::core::bar::{BarEventSource, MarketEvent};
use crate::core::error::Result;

/// 已对齐事件：`at` 是排序/发射时间，`slot` 是源下标，`event` 为原始事件。
#[derive(Debug, Clone)]
pub struct AlignedEvent {
    pub at: DateTime<Utc>,
    pub slot: usize,
    pub event: MarketEvent,
}

/// 非阻塞轮询结果（PR-5 Task 1，live 主循环用）。
///
/// `Ready` 负载按 clippy `large_enum_variant` 惯例装箱（同
/// `runtime_types::OrderUpdateCmd` 先例）：`Starved`/`Done` 零大小，
/// 枚举本体回归指针宽度——live 主循环每拍都要搬运它。
#[derive(Debug)]
pub enum AlignPoll {
    /// 有对齐事件可发射（选择规则与阻塞式 `next_aligned` 完全一致）。
    Ready(Box<AlignedEvent>),
    /// 本轮无可发射事件、但仍有源未耗尽：调用方应让出线程（sleep/park）后重轮询。
    /// 语义上对应「等数据」，与 [`MarketEvent::SourceIdle`] 的「该源暂无」呼应。
    Starved,
    /// 所有源均已耗尽（channel 关闭），流终结——等价阻塞式的 `Ok(None)`。
    Done,
}

/// 多源对齐事件源。
///
/// 内部每源缓冲一个 pending `(at, rank, event)`；`next_aligned` 每拍补足
/// 空 pending 后发射最小者。非 `Bar` 事件 `rank = 0`，`Bar` `rank = 1`，
/// 因此同时刻的 gap / day 边界排在 bar **之前**；再按 `slot` 定序。
pub struct AlignedEventSource {
    sources: Vec<Box<dyn BarEventSource>>,
    /// 每源最多一个待发射事件：`Some((at, rank, event))`。
    pending: Vec<Option<(DateTime<Utc>, u8, MarketEvent)>>,
    /// 每源最近一根 **bar** 的时间（gap/day/roll/reconnected 不改它）。
    last_at: Vec<Option<DateTime<Utc>>>,
    /// 源已耗尽标记。
    done: Vec<bool>,
    /// slot 的 instrument `namespaced_key`，仅用于确定性排序兜底；
    /// 本实现排序已由 `(at, rank, slot)` 完全确定，keys 只在构造期做长度
    /// 一致性断言（`new` 的 `keys` 必须与 `sources` 一一对应）。
    _keys: Vec<String>,
}

impl AlignedEventSource {
    /// `sources` 与 `keys` 按下标一一对应：`sources[i]` 属于 slot `i`，
    /// 其 instrument 的 `namespaced_key()` 为 `keys[i]`。
    pub fn new(sources: Vec<Box<dyn BarEventSource>>, keys: Vec<String>) -> Self {
        debug_assert_eq!(
            keys.len(),
            sources.len(),
            "keys 必须与 sources 一一对应（slot 数量一致）"
        );
        let n = sources.len();
        let _keys = keys;
        Self {
            sources,
            pending: vec![None; n],
            last_at: vec![None; n],
            done: vec![false; n],
            _keys,
        }
    }

    /// 发射下一对齐事件；全部源耗尽返回 `Ok(None)`。
    pub fn next_aligned(&mut self) -> Result<Option<AlignedEvent>> {
        // 1) 补足所有空 pending。
        for slot in 0..self.sources.len() {
            if self.done[slot] || self.pending[slot].is_some() {
                continue;
            }
            match self.sources[slot].next_event()? {
                None => self.done[slot] = true,
                Some(ev) => {
                    let (at, rank) = event_at(&ev, self.last_at[slot]);
                    self.pending[slot] = Some((at, rank, ev));
                }
            }
        }
        // 2) 发射 (at, rank, slot) 最小者。
        Ok(self.emit_best())
    }

    /// 非阻塞发射：用 `try_next_event` 补料（[`MarketEvent::SourceIdle`] 丢弃、
    /// `Ok(None)` 记耗尽），选择/推进与阻塞式完全一致。
    ///
    /// live 主循环用它实现「有数据就消费、没数据就让出线程」，
    /// 回测源（默认实现回退 `next_event`）行为不变。
    pub fn next_aligned_nonblocking(&mut self) -> Result<AlignPoll> {
        // 1) 非阻塞补足所有空 pending。
        for slot in 0..self.sources.len() {
            if self.done[slot] || self.pending[slot].is_some() {
                continue;
            }
            match self.sources[slot].try_next_event()? {
                None => self.done[slot] = true,
                // 该源暂无事件：本拍不补料，等下轮；不标 done（区别于耗尽）。
                Some(MarketEvent::SourceIdle) => {}
                Some(ev) => {
                    let (at, rank) = event_at(&ev, self.last_at[slot]);
                    self.pending[slot] = Some((at, rank, ev));
                }
            }
        }

        // 2) 无任何 pending：全耗尽 → Done；否则 → Starved（等数据）。
        if self.pending.iter().all(|p| p.is_none()) {
            return Ok(if self.done.iter().all(|&d| d) {
                AlignPoll::Done
            } else {
                AlignPoll::Starved
            });
        }

        // 3) 发射 (at, rank, slot) 最小者——与阻塞式同一选择器，保证同序。
        Ok(AlignPoll::Ready(Box::new(
            self.emit_best().expect("pending 非空时必有候选"),
        )))
    }

    /// 从 pending 中发射 `(at, rank, slot)` 字典序最小者；无候选返回 `None`。
    /// 阻塞式与非阻塞式共用，保证两条路径发射顺序逐拍一致。
    fn emit_best(&mut self) -> Option<AlignedEvent> {
        let mut best: Option<(DateTime<Utc>, u8, usize)> = None;
        for slot in 0..self.pending.len() {
            if let Some((at, rank, _)) = &self.pending[slot] {
                let cand = (*at, *rank, slot);
                if best.is_none_or(|b| cand < b) {
                    best = Some(cand);
                }
            }
        }
        let (at, rank, slot) = best?;
        let (stored_at, stored_rank, event) = self
            .pending
            .get_mut(slot)
            .expect("最佳候选必须存在 pending")
            .take()
            .expect("最佳候选必须是 Some");
        debug_assert_eq!((stored_at, stored_rank), (at, rank));

        // 只有 Bar 推进 last_at。
        if matches!(event, MarketEvent::Bar(_)) {
            self.last_at[slot] = Some(stored_at);
        }

        Some(AlignedEvent { at, slot, event })
    }
}

/// 按对齐规则把事件映射为 `(at, rank)`。
///
/// - `Bar` -> `(bar.open_time, 1)`
/// - `SessionGap` -> `(to.unwrap_or(from), 0)`
/// - `DayBoundary` -> `(last_bar + 1m, 0)`；`last_bar` 为空时用 `Utc::now()`
///   兜底——该情形只会在「某源首根 bar 之前就发 DayBoundary」的回测非法
///   输入中出现，debug 构建下 `debug_assert!` 直接暴露。
/// - `Roll` -> `(effective_at, 0)`
/// - `Reconnected` -> `(last_bar, 0)`，不改 `last_at`；兜底同上。
fn event_at(ev: &MarketEvent, last_bar: Option<DateTime<Utc>>) -> (DateTime<Utc>, u8) {
    match ev {
        MarketEvent::Bar(b) => (b.open_time, 1),
        MarketEvent::SessionGap(g) => (g.to.unwrap_or(g.from), 0),
        MarketEvent::DayBoundary(_) => (
            last_bar.unwrap_or_else(fallback_now) + Duration::minutes(1),
            0,
        ),
        MarketEvent::Roll(r) => (r.effective_at, 0),
        MarketEvent::Reconnected => (last_bar.unwrap_or_else(fallback_now), 0),
        // SourceIdle 是「该源暂无事件」的进程内信号，非阻塞补料时即被丢弃，
        // 永远不会进 pending；此分支仅保证 match 穷尽（live 语义下无时间轴位置）。
        MarketEvent::SourceIdle => {
            debug_assert!(false, "SourceIdle 不应进入对齐缓冲");
            (last_bar.unwrap_or_else(fallback_now), 0)
        }
    }
}

/// 仅回测不会触发的非确定性兜底（源在首根 bar 前发 DayBoundary/Reconnected）。
fn fallback_now() -> DateTime<Utc> {
    debug_assert!(
        false,
        "DayBoundary/Reconnected 出现在该源首根 bar 之前（回测不应触发）"
    );
    Utc::now()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::bar::{
        Bar, BarEventSource, BarExtensions, BarFinality, MarketEvent, VecEventSource,
    };
    use crate::core::identity::{ContractMonth, ExchangeId, InstrumentId};
    use crate::core::session::{GapReason, SessionGap, TradingDay};
    use crate::core::subscription::{RollEvent, RollPolicy};
    use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};

    fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
    }

    fn bar(inst: &InstrumentId, t: DateTime<Utc>) -> Bar {
        Bar {
            instrument: inst.clone(),
            open_time: t,
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1.0,
            finality: BarFinality::Final,
            extensions: BarExtensions::default(),
        }
    }

    fn bar_of(m: &MarketEvent) -> &Bar {
        match m {
            MarketEvent::Bar(b) => b,
            other => panic!("期望 Bar，实际 {other:?}"),
        }
    }

    fn mk_src(events: Vec<Vec<MarketEvent>>, keys: Vec<&str>) -> AlignedEventSource {
        let sources: Vec<Box<dyn BarEventSource>> = events
            .into_iter()
            .map(|ev| Box::new(VecEventSource::new(ev)) as Box<dyn BarEventSource>)
            .collect();
        let keys: Vec<String> = keys.into_iter().map(String::from).collect();
        AlignedEventSource::new(sources, keys)
    }

    #[test]
    fn interleaves_two_sources_by_timestamp_then_slot() {
        let ix = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let t0 = utc(2026, 1, 5, 0, 0);
        let mk = |i: i64| bar(&ix, t0 + Duration::minutes(i));
        let mut src = mk_src(
            vec![
                vec![
                    MarketEvent::Bar(mk(0)),
                    MarketEvent::Bar(mk(1)),
                    MarketEvent::Bar(mk(2)),
                ],
                vec![
                    MarketEvent::Bar(mk(0)),
                    MarketEvent::Bar(mk(1)),
                    MarketEvent::Bar(mk(2)),
                ],
            ],
            vec!["s0", "s1"],
        );

        let expected = [(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)];
        for (i, (min, slot)) in expected.into_iter().enumerate() {
            let ev = src
                .next_aligned()
                .unwrap()
                .unwrap_or_else(|| panic!("第 {i} 个对齐事件缺失"));
            assert_eq!(
                (ev.at, ev.slot),
                (t0 + Duration::minutes(min), slot),
                "第 {i} 个事件 (at, slot)"
            );
        }
        assert!(
            src.next_aligned().unwrap().is_none(),
            "两源耗尽后必须返 None"
        );
    }

    #[test]
    fn gap_and_day_events_precede_same_time_bar() {
        let ix = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let t0 = utc(2026, 1, 5, 0, 0);
        let mut src = mk_src(
            vec![vec![
                MarketEvent::Bar(bar(&ix, t0)),
                MarketEvent::SessionGap(SessionGap {
                    from: t0 + Duration::minutes(1),
                    to: Some(t0 + Duration::minutes(2)),
                    reason: GapReason::LunchBreak,
                    closes_bar: true,
                }),
                MarketEvent::Bar(bar(&ix, t0 + Duration::minutes(2))),
            ]],
            vec!["s0"],
        );

        let first = src.next_aligned().unwrap().unwrap();
        assert_eq!((first.at, first.slot), (t0, 0));
        assert_eq!(bar_of(&first.event).open_time, t0);

        let second = src.next_aligned().unwrap().unwrap();
        assert_eq!(
            second.at,
            t0 + Duration::minutes(2),
            "gap.at 必须取 to（rank 0 使它在同时刻 bar 之前）"
        );
        assert!(matches!(second.event, MarketEvent::SessionGap(_)));

        let third = src.next_aligned().unwrap().unwrap();
        assert_eq!(third.at, t0 + Duration::minutes(2));
        assert!(matches!(third.event, MarketEvent::Bar(_)));

        assert!(src.next_aligned().unwrap().is_none());
    }

    #[test]
    fn missing_bar_in_one_source_does_not_block_the_other() {
        // s0 有 t1，s1 没有 -> t1 只发射 slot 0（供 runner 标记 stale）。
        let ix = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let t0 = utc(2026, 1, 5, 0, 0);
        let mk = |i: i64| bar(&ix, t0 + Duration::minutes(i));
        let mut src = mk_src(
            vec![
                vec![MarketEvent::Bar(mk(0)), MarketEvent::Bar(mk(1))],
                vec![MarketEvent::Bar(mk(0))],
            ],
            vec!["s0", "s1"],
        );

        let mut out = Vec::new();
        while let Some(ev) = src.next_aligned().unwrap() {
            out.push((ev.at, ev.slot));
        }
        assert_eq!(
            out,
            vec![(t0, 0), (t0, 1), (t0 + Duration::minutes(1), 0),],
            "t1 只有 slot 0 有 bar，slot 1 已耗尽不应阻塞"
        );
    }

    #[test]
    fn exhausted_sources_are_skipped() {
        let ix = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let t0 = utc(2026, 1, 5, 0, 0);
        let mk = |i: i64| bar(&ix, t0 + Duration::minutes(i));
        let mut src = mk_src(
            vec![
                vec![MarketEvent::Bar(mk(0))],
                vec![MarketEvent::Bar(mk(0)), MarketEvent::Bar(mk(1))],
            ],
            vec!["s0", "s1"],
        );

        let mut out = Vec::new();
        while let Some(ev) = src.next_aligned().unwrap() {
            out.push((ev.at, ev.slot));
        }
        assert_eq!(
            out,
            vec![(t0, 0), (t0, 1), (t0 + Duration::minutes(1), 1)],
            "s0 先耗尽，后续只剩 s1 的 t1"
        );
    }

    #[test]
    fn deterministic_when_timestamps_tie() {
        let ix = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let t0 = utc(2026, 1, 5, 0, 0);
        let run = || {
            let mk = |i: i64| bar(&ix, t0 + Duration::minutes(i));
            let mut src = mk_src(
                vec![
                    vec![MarketEvent::Bar(mk(0)), MarketEvent::Bar(mk(1))],
                    vec![MarketEvent::Bar(mk(0)), MarketEvent::Bar(mk(1))],
                ],
                vec!["s0", "s1"],
            );
            let mut out = Vec::new();
            while let Some(ev) = src.next_aligned().unwrap() {
                out.push((ev.at, ev.slot));
            }
            out
        };
        let a = run();
        let b = run();
        assert_eq!(a, b, "两次独立构造必须逐项一致");
        assert_eq!(
            a,
            vec![
                (t0, 0),
                (t0, 1),
                (t0 + Duration::minutes(1), 0),
                (t0 + Duration::minutes(1), 1),
            ],
            "同刻时 slot 0 必须排在 slot 1 前"
        );
    }

    #[test]
    fn day_boundary_uses_last_bar_plus_one_minute_and_precedes_same_time_bar() {
        let ix = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let t0 = utc(2026, 1, 5, 0, 0);
        let mut src = mk_src(
            vec![vec![
                MarketEvent::Bar(bar(&ix, t0)),
                MarketEvent::DayBoundary(TradingDay(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap())),
                MarketEvent::Bar(bar(&ix, t0 + Duration::minutes(1))),
            ]],
            vec!["s0"],
        );

        let first = src.next_aligned().unwrap().unwrap();
        assert_eq!((first.at, first.slot), (t0, 0));
        assert!(matches!(first.event, MarketEvent::Bar(_)));

        let second = src.next_aligned().unwrap().unwrap();
        assert_eq!(
            second.at,
            t0 + Duration::minutes(1),
            "DayBoundary 的时间 = 上一根 bar + 1m"
        );
        assert!(matches!(second.event, MarketEvent::DayBoundary(_)));

        let third = src.next_aligned().unwrap().unwrap();
        assert_eq!(third.at, t0 + Duration::minutes(1));
        assert!(matches!(third.event, MarketEvent::Bar(_)));

        assert!(src.next_aligned().unwrap().is_none());
    }

    #[test]
    fn session_gap_without_to_uses_from() {
        let ix = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let t0 = utc(2026, 1, 5, 0, 0);
        let mut src = mk_src(
            vec![vec![
                MarketEvent::Bar(bar(&ix, t0)),
                MarketEvent::SessionGap(SessionGap {
                    from: t0 + Duration::minutes(1),
                    to: None,
                    reason: GapReason::Overnight,
                    closes_bar: true,
                }),
            ]],
            vec!["s0"],
        );

        let first = src.next_aligned().unwrap().unwrap();
        assert_eq!((first.at, first.slot), (t0, 0));
        assert!(matches!(first.event, MarketEvent::Bar(_)));

        let second = src.next_aligned().unwrap().unwrap();
        assert_eq!(
            second.at,
            t0 + Duration::minutes(1),
            "SessionGap.to 为空时 at 取 from"
        );
        assert!(matches!(second.event, MarketEvent::SessionGap(_)));

        assert!(src.next_aligned().unwrap().is_none());
    }

    #[test]
    fn roll_uses_effective_at() {
        let from = InstrumentId::futures_contract(
            &ExchangeId::new("SHFE"),
            "CU",
            ContractMonth {
                year: 2025,
                month: 12,
            },
        );
        let to = InstrumentId::futures_contract(
            &ExchangeId::new("SHFE"),
            "CU",
            ContractMonth {
                year: 2026,
                month: 1,
            },
        );
        let t0 = utc(2026, 1, 5, 0, 0);
        let eff = utc(2026, 1, 5, 9, 0);
        let mut src = mk_src(
            vec![vec![
                MarketEvent::Bar(bar(&from, t0)),
                MarketEvent::Roll(RollEvent {
                    trading_day: TradingDay(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap()),
                    effective_at: eff,
                    from: from.clone(),
                    to: to.clone(),
                    policy: RollPolicy::OpenInterest,
                }),
                MarketEvent::Bar(bar(&to, t0 + Duration::minutes(1))),
            ]],
            vec!["s0"],
        );

        let first = src.next_aligned().unwrap().unwrap();
        assert_eq!((first.at, first.slot), (t0, 0));
        assert!(matches!(first.event, MarketEvent::Bar(_)));

        let second = src.next_aligned().unwrap().unwrap();
        assert_eq!(second.at, eff, "Roll 的对齐时间 = effective_at");
        assert!(matches!(second.event, MarketEvent::Roll(_)));
    }

    #[test]
    fn reconnected_uses_last_bar_time_and_keeps_last_at() {
        let ix = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let t0 = utc(2026, 1, 5, 0, 0);
        let mut src = mk_src(
            vec![vec![
                MarketEvent::Bar(bar(&ix, t0)),
                MarketEvent::Reconnected,
                MarketEvent::DayBoundary(TradingDay(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap())),
                MarketEvent::Bar(bar(&ix, t0 + Duration::minutes(1))),
            ]],
            vec!["s0"],
        );

        let first = src.next_aligned().unwrap().unwrap();
        assert_eq!((first.at, first.slot), (t0, 0));
        assert!(matches!(first.event, MarketEvent::Bar(_)));

        let second = src.next_aligned().unwrap().unwrap();
        assert_eq!(second.at, t0, "Reconnected 的对齐时间 = 上一根 bar 时间");
        assert!(matches!(second.event, MarketEvent::Reconnected));

        let third = src.next_aligned().unwrap().unwrap();
        assert_eq!(
            third.at,
            t0 + Duration::minutes(1),
            "Reconnected 不改 last_at：DayBoundary 仍按上一根 bar + 1m 计算"
        );
        assert!(matches!(third.event, MarketEvent::DayBoundary(_)));

        let fourth = src.next_aligned().unwrap().unwrap();
        assert_eq!(fourth.at, t0 + Duration::minutes(1));
        assert!(matches!(fourth.event, MarketEvent::Bar(_)));

        assert!(src.next_aligned().unwrap().is_none());
    }
}
