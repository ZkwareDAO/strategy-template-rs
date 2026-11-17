//! StrategyBase 行为守卫：单标的路径、横截面排名与等权调仓。

use chrono::{NaiveDate, TimeZone};
use strategy_core::signal::Side;
use strategy_engine::core::bar::{Bar, BarExtensions, BarFinality, Timeframe};
use strategy_engine::core::identity::{ExchangeId, InstrumentId};
use strategy_engine::core::session::TradingDay;
use strategy_engine::strategy::features::FeatureMap;
use strategy_engine::strategy::model::{
    master_position_view, MasterPositionState, SubPosition, SubPositionState,
};
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, MasterContext,
    PortfolioContext,
};
use strategy_strategies::common::{percent_stop, StopSpec, StrategyBase, StrategyLogic};

const STRATEGY: &str = "test_base";

struct TestLogic {
    top_k: usize,
    stop: Option<StopSpec>,
    allow_entry: bool,
}

impl StrategyLogic for TestLogic {
    fn name(&self) -> &str {
        STRATEGY
    }

    fn top_k(&self) -> usize {
        self.top_k
    }

    fn score(&self, snapshot: &InstrumentSnapshot) -> Option<f64> {
        snapshot.features.number("signal")
    }

    fn entry_allowed(&self, _snapshot: &InstrumentSnapshot) -> bool {
        self.allow_entry
    }

    fn stop(&self, _snapshot: &InstrumentSnapshot) -> Option<StopSpec> {
        self.stop
    }
}

fn base(top_k: usize, stop: Option<StopSpec>) -> StrategyBase<TestLogic> {
    StrategyBase::new(TestLogic {
        top_k,
        stop,
        allow_entry: true,
    })
}

fn inst(symbol: &str) -> InstrumentId {
    InstrumentId::perpetual(&ExchangeId::new("binance"), symbol)
}

fn bar(symbol: &str, close: f64) -> Bar {
    Bar {
        instrument: inst(symbol),
        open_time: chrono::Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
        open: close,
        high: close + 1.0,
        low: close - 1.0,
        close,
        volume: 1.0,
        finality: BarFinality::Final,
        extensions: BarExtensions::default(),
    }
}

fn snap(
    symbol: &str,
    features: &[(&str, f64)],
    stale: bool,
    entry_eligible: bool,
) -> InstrumentSnapshot {
    let mut fm = FeatureMap::default();
    for (name, value) in features {
        fm.values.insert((*name).to_string(), *value);
    }
    InstrumentSnapshot {
        instrument: inst(symbol),
        bar: bar(symbol, 100.0),
        position: None,
        stale,
        features: fm,
        entry_eligible,
        entry_reason: None,
        exit_triggered: None,
        trading_day: TradingDay(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
        no_trade: false,
        entry_confirm: true,
        signal_resume: false,
        cooldown: None,
        session_open_price: None,
        closed: None,
    }
}

fn ctx(nav: f64, subs: Vec<SubPosition>, due: bool, mult: f64) -> PortfolioContext {
    let symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
    let multipliers = symbols
        .iter()
        .map(|symbol| (inst(symbol).namespaced_key(), mult))
        .collect();
    PortfolioContext {
        strategy_id: format!("{STRATEGY}:default"),
        nav,
        drawdown: 0.0,
        portfolio_pnl_pct: 0.0,
        sub_positions: subs,
        master: None,
        multipliers,
        max_total_notional: 1.0,
        rebalance_due: due,
        no_trade: false,
    }
}

fn sub(symbol: &str, qty: f64) -> SubPosition {
    SubPosition {
        id: format!("sub_{symbol}"),
        strategy_id: format!("{STRATEGY}:default"),
        master_id: "master_000001".into(),
        instrument: inst(symbol),
        side: Side::Long,
        qty,
        entry_price: 100.0,
        stop_price: None,
        target_weight: 0.0,
        rebalance_epoch: 0,
        state: SubPositionState::Active,
        opened_at: None,
        closed_at: None,
        realized_pnl: None,
    }
}

fn opens(actions: &[InstrumentAction]) -> Vec<(String, f64)> {
    actions
        .iter()
        .filter_map(|action| match action {
            InstrumentAction::Open {
                instrument,
                stop_price,
                ..
            } => Some((instrument.namespaced_key(), *stop_price)),
            _ => None,
        })
        .collect()
}

fn adjusts(actions: &[InstrumentAction]) -> Vec<(String, f64)> {
    actions
        .iter()
        .filter_map(|action| match action {
            InstrumentAction::Adjust {
                instrument,
                target_qty,
                ..
            } => Some((instrument.namespaced_key(), *target_qty)),
            _ => None,
        })
        .collect()
}

fn key(symbol: &str) -> String {
    inst(symbol).namespaced_key()
}

#[test]
fn strategy_type_is_logic_name() {
    assert_eq!(
        base(1, Some(StopSpec::Percent(2.0))).strategy_type(),
        STRATEGY
    );
}

#[test]
fn single_opens_long_at_close_with_percent_stop() {
    let mut strategy = base(1, Some(StopSpec::Percent(2.0)));
    let snapshot = snap("BTCUSDT", &[("signal", 1.0)], false, true);
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        std::slice::from_ref(&snapshot),
        &ctx(1000.0, Vec::new(), true, 1.0),
    );

    assert_eq!(actions.len(), 1);
    match &actions[0] {
        InstrumentAction::Open {
            instrument,
            side,
            price,
            stop_price,
            atr,
            strength,
            reason,
        } => {
            assert_eq!(instrument.namespaced_key(), key("BTCUSDT"));
            assert_eq!(*side, Side::Long);
            assert_eq!(*price, 100.0);
            assert_eq!(*stop_price, 98.0);
            assert_eq!(*atr, 0.0);
            assert_eq!(*strength, 1.0);
            assert!(reason.starts_with(STRATEGY));
        }
        other => panic!("expected Open, got {other:?}"),
    }
}

#[test]
fn single_skips_existing_position() {
    let mut strategy = base(1, Some(StopSpec::Percent(2.0)));
    let snapshot = snap("BTCUSDT", &[("signal", 1.0)], false, true);
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        std::slice::from_ref(&snapshot),
        &ctx(1000.0, vec![sub("BTCUSDT", 1.0)], true, 1.0),
    );

    assert!(actions.is_empty());
}

#[test]
fn single_skips_stale_snapshot() {
    let mut strategy = base(1, Some(StopSpec::Percent(2.0)));
    let snapshot = snap("BTCUSDT", &[("signal", 1.0)], true, true);
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        std::slice::from_ref(&snapshot),
        &ctx(1000.0, Vec::new(), true, 1.0),
    );

    assert!(actions.is_empty());
}

#[test]
fn single_skips_ineligible_snapshot() {
    let mut strategy = base(1, Some(StopSpec::Percent(2.0)));
    let snapshot = snap("BTCUSDT", &[("signal", 1.0)], false, false);
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        std::slice::from_ref(&snapshot),
        &ctx(1000.0, Vec::new(), true, 1.0),
    );

    assert!(actions.is_empty());
}

#[test]
fn single_honors_entry_allowed_hook() {
    let mut strategy = StrategyBase::new(TestLogic {
        top_k: 1,
        stop: Some(StopSpec::Percent(2.0)),
        allow_entry: false,
    });
    let snapshot = snap("BTCUSDT", &[("signal", 1.0)], false, true);
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        std::slice::from_ref(&snapshot),
        &ctx(1000.0, Vec::new(), true, 1.0),
    );

    assert!(actions.is_empty());
}

#[test]
fn single_requires_stop_spec() {
    let mut strategy = base(1, None);
    let snapshot = snap("BTCUSDT", &[("signal", 1.0)], false, true);
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        std::slice::from_ref(&snapshot),
        &ctx(1000.0, Vec::new(), true, 1.0),
    );

    assert!(actions.is_empty());
}

#[test]
fn cross_waits_for_rebalance_due() {
    let mut strategy = base(1, Some(StopSpec::Percent(2.0)));
    let snapshots = [
        snap("BTCUSDT", &[("signal", 1.0)], false, true),
        snap("ETHUSDT", &[("signal", 2.0)], false, true),
    ];
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        &snapshots,
        &ctx(1000.0, Vec::new(), false, 1.0),
    );

    assert!(actions.is_empty());
}

#[test]
fn cross_selects_highest_score_top_k() {
    let mut strategy = base(1, Some(StopSpec::Percent(2.0)));
    let snapshots = [
        snap("BTCUSDT", &[("signal", 1.0)], false, true),
        snap("ETHUSDT", &[("signal", 3.0)], false, true),
        snap("SOLUSDT", &[("signal", 2.0)], false, true),
    ];
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        &snapshots,
        &ctx(1000.0, Vec::new(), true, 1.0),
    );

    assert_eq!(opens(&actions), vec![(key("ETHUSDT"), 98.0)]);
}

#[test]
fn cross_rebalances_existing_to_equal_weight() {
    let mut strategy = base(2, Some(StopSpec::Percent(2.0)));
    let snapshots = [
        snap("BTCUSDT", &[("signal", 1.0)], false, true),
        snap("ETHUSDT", &[("signal", 2.0)], false, true),
    ];
    // 等权 1/2 × nav 2000 / (price 100 × mult 10) = 目标 1 张，当前 2 张。
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        &snapshots,
        &ctx(2000.0, vec![sub("BTCUSDT", 2.0)], true, 10.0),
    );

    assert_eq!(opens(&actions), vec![(key("ETHUSDT"), 98.0)]);
    assert_eq!(adjusts(&actions), vec![(key("BTCUSDT"), 1.0)]);
}

#[test]
fn cross_holds_target_weight_without_adjust() {
    let mut strategy = base(2, Some(StopSpec::Percent(2.0)));
    let snapshots = [
        snap("BTCUSDT", &[("signal", 1.0)], false, true),
        snap("ETHUSDT", &[("signal", 2.0)], false, true),
    ];
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        &snapshots,
        &ctx(2000.0, vec![sub("BTCUSDT", 2.0)], true, 5.0),
    );

    assert!(adjusts(&actions).is_empty(), "已在目标权重上不产生 Adjust");
}

#[test]
fn cross_target_uses_risk_budget_not_full_nav() {
    let mut strategy = base(2, Some(StopSpec::Percent(2.0)));
    let snapshots = [
        snap("BTCUSDT", &[("signal", 1.0)], false, true),
        snap("ETHUSDT", &[("signal", 2.0)], false, true),
    ];
    // 等权 1/2 × 预算 0.25 × nav 2000 / (price 100 × mult 10) = 目标 0.25 张。
    let mut portfolio = ctx(2000.0, vec![sub("BTCUSDT", 2.0)], true, 10.0);
    portfolio.max_total_notional = 0.25;
    let actions = strategy.on_cross_section_bars(&[Timeframe::M1], &snapshots, &portfolio);

    assert_eq!(adjusts(&actions), vec![(key("BTCUSDT"), 0.25)]);
}

#[test]
fn cross_does_not_open_without_score() {
    let mut strategy = base(1, Some(StopSpec::Percent(2.0)));
    let snapshots = [
        snap("BTCUSDT", &[], false, true),
        snap("ETHUSDT", &[("signal", 3.0)], false, true),
    ];
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        &snapshots,
        &ctx(1000.0, Vec::new(), true, 1.0),
    );

    assert_eq!(opens(&actions), vec![(key("ETHUSDT"), 98.0)]);
}

#[test]
fn cross_ignores_non_finite_score() {
    let mut strategy = base(1, Some(StopSpec::Percent(2.0)));
    let snapshots = [
        snap("BTCUSDT", &[("signal", f64::NAN)], false, true),
        snap("ETHUSDT", &[("signal", 3.0)], false, true),
    ];
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        &snapshots,
        &ctx(1000.0, Vec::new(), true, 1.0),
    );

    assert_eq!(opens(&actions), vec![(key("ETHUSDT"), 98.0)]);
}

#[test]
fn cross_drops_held_instrument_out_of_top_k() {
    let mut strategy = base(1, Some(StopSpec::Percent(2.0)));
    let snapshots = [
        snap("BTCUSDT", &[("signal", 1.0)], false, true),
        snap("ETHUSDT", &[("signal", 3.0)], false, true),
    ];
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        &snapshots,
        &ctx(1000.0, vec![sub("BTCUSDT", 1.0)], true, 1.0),
    );

    assert_eq!(opens(&actions), vec![(key("ETHUSDT"), 98.0)]);
    assert_eq!(adjusts(&actions), vec![(key("BTCUSDT"), 0.0)]);
}

#[test]
fn cross_keeps_stale_held_instrument() {
    let mut strategy = base(1, Some(StopSpec::Percent(2.0)));
    let snapshots = [
        snap("BTCUSDT", &[], true, true),
        snap("ETHUSDT", &[("signal", 3.0)], false, true),
    ];
    let actions = strategy.on_cross_section_bars(
        &[Timeframe::M1],
        &snapshots,
        &ctx(1000.0, vec![sub("BTCUSDT", 1.0)], true, 1.0),
    );

    assert_eq!(opens(&actions), vec![(key("ETHUSDT"), 98.0)]);
    assert!(
        adjusts(&actions).is_empty(),
        "stale 时无法判定排名，不清零仓位"
    );
}

#[test]
fn master_closing_blocks_opens_and_adjusts() {
    let mut strategy = base(2, Some(StopSpec::Percent(2.0)));
    let snapshots = [
        snap("BTCUSDT", &[("signal", 1.0)], false, true),
        snap("ETHUSDT", &[("signal", 2.0)], false, true),
    ];
    let subs = vec![sub("BTCUSDT", 2.0)];
    let view = master_position_view(&subs, &Default::default(), &Default::default());
    let mut portfolio = ctx(2000.0, subs, true, 10.0);
    portfolio.master = Some(MasterContext {
        master_id: "master_000001".into(),
        state: MasterPositionState::Closing,
        view,
    });

    let actions = strategy.on_cross_section_bars(&[Timeframe::M1], &snapshots, &portfolio);
    assert!(
        actions.is_empty(),
        "总仓 Closing 时 StrategyBase 不得开仓/加仓/调仓"
    );
}

#[test]
fn percent_stop_clamps_to_safe_range() {
    assert!((percent_stop(100.0, 0.0) - 99.9).abs() < 1e-12);
    assert!((percent_stop(100.0, 200.0) - 1.0).abs() < 1e-12);
    assert_eq!(percent_stop(100.0, 2.0), 98.0);
}

#[test]
fn atr_stop_uses_atr_multiple() {
    let snapshot = snap("BTCUSDT", &[("atr", 3.0)], false, true);
    let stop = StopSpec::Atr {
        mult: 2.0,
        fallback_pct: 5.0,
        min_distance_pct: Some(5.0),
        max_distance_pct: None,
    };

    assert_eq!(stop.price(&snapshot), Some(94.0));
}

#[test]
fn atr_stop_falls_back_to_percent_without_atr() {
    let snapshot = snap("BTCUSDT", &[], false, true);
    let stop = StopSpec::Atr {
        mult: 2.0,
        fallback_pct: 5.0,
        min_distance_pct: Some(5.0),
        max_distance_pct: None,
    };

    assert_eq!(stop.price(&snapshot), Some(95.0));
}

#[test]
fn atr_stop_never_tighter_than_min_distance() {
    let snapshot = snap("BTCUSDT", &[("atr", 0.5)], false, true);
    let stop = StopSpec::Atr {
        mult: 2.0,
        fallback_pct: 5.0,
        min_distance_pct: Some(5.0),
        max_distance_pct: None,
    };

    assert_eq!(stop.price(&snapshot), Some(95.0));
}

#[test]
fn atr_stop_never_wider_than_max_distance() {
    let snapshot = snap("BTCUSDT", &[("atr", 40.0)], false, true);
    let stop = StopSpec::Atr {
        mult: 2.0,
        fallback_pct: 5.0,
        min_distance_pct: None,
        max_distance_pct: Some(20.0),
    };

    assert_eq!(stop.price(&snapshot), Some(80.0));
}

#[test]
fn atr_stop_never_below_one_percent_of_price() {
    let snapshot = snap("BTCUSDT", &[("atr", 100.0)], false, true);
    let stop = StopSpec::Atr {
        mult: 2.0,
        fallback_pct: 5.0,
        min_distance_pct: None,
        max_distance_pct: None,
    };

    let stop_price = stop.price(&snapshot).unwrap();
    assert!((stop_price - 1.0).abs() < 1e-12);
}

#[test]
fn stop_price_rejects_non_positive_price() {
    let mut snapshot = snap("BTCUSDT", &[("atr", 3.0)], false, true);
    snapshot.bar.close = 0.0;

    assert_eq!(StopSpec::Percent(2.0).price(&snapshot), None);
    assert_eq!(
        StopSpec::Atr {
            mult: 2.0,
            fallback_pct: 5.0,
            min_distance_pct: None,
            max_distance_pct: None,
        }
        .price(&snapshot),
        None
    );
}
