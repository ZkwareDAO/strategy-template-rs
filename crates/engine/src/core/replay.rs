//! 历史回放事件流构造器（阶段 3，§17 最小嵌入路径-2 的事件注入内核）。
//!
//! `Exchange::make_source` 默认实现直接回放 bars，不产生断开/日界事件；
//! 历史 CSV 回放必须按 `SessionRules` 注入 `SessionGap`（午休/隔夜断桶）
//! 与 `DayBoundary`（平今→平昨滚动），否则国内期货的聚合桶与今昨仓语义
//! 和实盘不一致。app 层 `spi_backtest::csv_events` 从 `csv_store` 读数后
//! 喂这里——engine 层不依赖 `data` crate（分层铁律，§4）。

use chrono::Duration;

use crate::core::bar::{Bar, MarketEvent};
use crate::core::session::{GapReason, SessionGap, SessionRules};

/// 1m 序列（升序、均为 Final 帧）→ 完整行情事件流。
///
/// 规则：
/// - `prev.open_time + 1m < bar.open_time`（时刻不连续）→ `SessionGap`，
///   同交易日记 `LunchBreak`、跨交易日记 `Overnight`——reason 仅日志用途，
///   行为面 `closes_bar = true` 两者一致（强制断桶）；
/// - 交易日切换 → 在新一天第一根 1m 之前发 `DayBoundary(new_day)`
///   （周五 15:00 → 夜盘 21:00 即切：夜盘归次日交易日）；
/// - 首根不发边界事件（runner 的 `last_day` 兜底同样正确，双跑口径以
///   runner 为准）；`Reconnected` 对回放无意义，不产生。
pub fn events_from_bars(bars: Vec<Bar>, session: &dyn SessionRules) -> Vec<MarketEvent> {
    let mut out = Vec::with_capacity(bars.len() + 16);
    let mut prev: Option<Bar> = None;
    for bar in bars {
        if let Some(p) = &prev {
            let prev_end = p.open_time + Duration::minutes(1);
            if bar.open_time > prev_end {
                let same_day =
                    session.trading_day_of(p.open_time) == session.trading_day_of(bar.open_time);
                out.push(MarketEvent::SessionGap(SessionGap {
                    from: prev_end,
                    to: Some(bar.open_time),
                    reason: if same_day {
                        GapReason::LunchBreak
                    } else {
                        GapReason::Overnight
                    },
                    closes_bar: true,
                }));
            }
            let cur_day = session.trading_day_of(bar.open_time);
            if cur_day != session.trading_day_of(p.open_time) {
                out.push(MarketEvent::DayBoundary(cur_day));
            }
        }
        out.push(MarketEvent::Bar(bar.clone()));
        prev = Some(bar);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::bar::{BarExtensions, BarFinality};
    use crate::core::identity::{ExchangeId, InstrumentId};
    use crate::core::session::cn_futures_default;
    use chrono::{DateTime, TimeZone, Utc};

    fn inst() -> InstrumentId {
        InstrumentId::futures_contract(
            &ExchangeId::new("SHFE"),
            "CU",
            crate::core::identity::ContractMonth {
                year: 2026,
                month: 1,
            },
        )
    }

    fn bar(t: DateTime<Utc>, close: f64) -> Bar {
        Bar {
            instrument: inst(),
            open_time: t,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
            finality: BarFinality::Final,
            extensions: BarExtensions::default(),
        }
    }

    fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
    }

    #[test]
    fn replay_injects_gaps_and_day_boundaries() {
        let session = cn_futures_default();
        // 只取时段断点样本：10:14 → 10:30（午休）、14:59 → 21:00（隔夜跨交易日）。
        // reason 仅日志用途（同交易日内一律 LunchBreak）；行为面 closes_bar 恒 true。
        let bars = vec![
            bar(utc(2026, 1, 5, 2, 14), 1.0), // 10:14
            bar(utc(2026, 1, 5, 2, 30), 1.0), // 10:30
            bar(utc(2026, 1, 5, 6, 59), 1.0), // 14:59
            bar(utc(2026, 1, 5, 13, 0), 1.0), // 21:00（归属次日交易日）
        ];
        let events = events_from_bars(bars, &session);
        let kinds: Vec<String> = events
            .iter()
            .map(|e| match e {
                MarketEvent::Bar(_) => "bar".into(),
                MarketEvent::SessionGap(g) => format!("gap:{:?}", g.reason),
                MarketEvent::DayBoundary(d) => format!("day:{}", d.0),
                MarketEvent::Roll(_) | MarketEvent::Reconnected | MarketEvent::SourceIdle => {
                    "other".into()
                }
            })
            .collect();
        assert_eq!(
            kinds,
            vec![
                "bar",
                "gap:LunchBreak",
                "bar",
                "gap:LunchBreak",
                "bar",
                "gap:Overnight",
                "day:2026-01-06",
                "bar",
            ],
            "午休断桶同交易日、隔夜跨交易日，事件顺序与旧引擎回放口径一致"
        );
    }
}
