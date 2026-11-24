//! P0 回归：真实回放事件流（`events_from_bars` 注入 `SessionGap`）下，
//! 午休断开不得把同一交易日的 D1 拆成两根。
//!
//! 现有 `spi_dual_run_parity` 是逐根直接喂 `AggregationState::on_closed_1m`、
//! 不注入 gap，覆盖不到这条生产路径。

use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use strategy_engine::core::bar::{
    Bar, BarExtensions, BarFinality, MarketEvent, Timeframe, VecEventSource,
};
use strategy_engine::core::exchange::{Exchange, SourceRequest};
use strategy_engine::core::identity::InstrumentId;
use strategy_engine::core::replay::events_from_bars;
use strategy_engine::core::runtime::ExchangeRunner;
use strategy_engine::core::session::TradingDay;
use strategy_engine::core::types::{DecisionContext, StrategyAction, StrategyPort};
use strategy_engine::exchanges::ctp::CtpExchange;

fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
}

fn bar(inst: &InstrumentId, t: DateTime<Utc>) -> Bar {
    Bar {
        instrument: inst.clone(),
        open_time: t,
        open: 100.0,
        high: 100.0,
        low: 100.0,
        close: 100.0,
        volume: 1.0,
        finality: BarFinality::Final,
        extensions: BarExtensions::default(),
    }
}

fn range(inst: &InstrumentId, start: DateTime<Utc>, n: i64) -> Vec<Bar> {
    (0..n)
        .map(|i| bar(inst, start + Duration::minutes(i)))
        .collect()
}

struct NoopStrategy;

impl StrategyPort for NoopStrategy {
    fn on_closed_bars(
        &mut self,
        _newly_closed: &[Timeframe],
        _state: &strategy_engine::core::aggregation::AggregationState,
        _ctx: &DecisionContext,
    ) -> Vec<StrategyAction> {
        Vec::new()
    }
}

#[test]
fn replay_lunch_gap_keeps_single_d1() {
    let ex = CtpExchange::new();
    let inst = ex.parse_instrument("CU2601").unwrap();
    let mut bars = Vec::new();
    // 周五夜盘 21:00 北京（=UTC 01-02 13:00）.. 00:59
    bars.extend(range(&inst, utc(2026, 1, 2, 13, 0), 240));
    // 周一日盘 09:00 北京（=UTC 01:00）.. 10:14
    bars.extend(range(&inst, utc(2026, 1, 5, 1, 0), 75));
    // 午休后 13:30 北京（=UTC 05:30）.. 14:59
    bars.extend(range(&inst, utc(2026, 1, 5, 5, 30), 90));

    let mut parts = ex
        .build(&SourceRequest::backtest(inst.clone(), Vec::new(), 1_000_000.0))
        .unwrap();
    let events = events_from_bars(bars, parts.session.as_ref());
    assert!(
        events
            .iter()
            .any(|e| matches!(e, MarketEvent::SessionGap(_))),
        "回放必须注入午休 SessionGap，否则测不到生产路径"
    );
    parts.source = Box::new(VecEventSource::new(events));

    let mut runner = ExchangeRunner::new(parts, vec![Timeframe::M1, Timeframe::D1], 200);
    runner.run(&mut NoopStrategy).unwrap();

    let d1 = runner.state.closed(Timeframe::D1);
    assert_eq!(d1.len(), 1, "一个交易日只能有一根 D1");
    assert_eq!(d1[0].open_time, utc(2026, 1, 2, 13, 0));
    assert!(
        (d1[0].volume - 405.0).abs() < 1e-9,
        "D1 量必须含夜盘+上午+下午，实际 {}",
        d1[0].volume
    );
    assert_eq!(
        d1[0].extensions.trading_day,
        Some(TradingDay(
            NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()
        )),
        "D1 必须携带交易日（周五夜盘归周一）"
    );
}
