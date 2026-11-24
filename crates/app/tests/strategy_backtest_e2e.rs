//! 横截面回测装配 e2e（PR-4 最小面：双 slot 装配 + 组合净值 + sub_ 下单）。
//! 完整确定性/黄金值矩阵在 Task 18 追加于本文件。

use std::path::Path;

use chrono::{TimeZone, Utc};
use strategy_app::spi_backtest::{run_strategy_from_spec, StrategyRunArgs};
use strategy_app::strategy_config::StrategySpec;
use strategy_core::candle::Candle;
use strategy_core::risk::RiskControlConfig;
use strategy_engine::core::bar::Timeframe;

fn utc(mi: u32) -> chrono::DateTime<Utc> {
    // 2026-06-10T00:00:00Z = 1781049600；mi 可超 59，按 epoch 秒推进避免非法历法。
    Utc.timestamp_opt(1_781_049_600 + mi as i64 * 60, 0)
        .unwrap()
}

fn candles(n: usize, base: f64, step: f64) -> Vec<Candle> {
    (0..n)
        .map(|i| {
            let v = base + step * i as f64;
            Candle {
                open_time: utc(i as u32),
                open: v,
                high: v,
                low: v,
                close: v,
                volume: 1.0,
                quote_volume: 0.0,
                trade_num: 0,
                active_buy_volume: 0.0,
                active_buy_quote_volume: 0.0,
                is_closed: true,
            }
        })
        .collect()
}

fn symbol_spec(symbol: &str) -> strategy_app::config::SymbolSpec {
    strategy_app::config::SymbolSpec {
        strategy: "template_xsec".into(),
        symbol: symbol.into(),
        timeframe: "1m".into(),
        cash: 200.0,
        initial_cash: None,
        params: None,
        warmup_days: 0,
        exchange: "binance".into(),
        venue_key: format!("binance/crypto_perpetual/{symbol}"),
        account: "default".into(),
        leverage: 1,
        mode: "backtest".into(),
        contract: None,
        risk: RiskControlConfig {
            enabled: false,
            ..RiskControlConfig::default()
        },
        venue_qty_step: None,
        venue_min_qty: None,
        venue_qty_precision: None,
    }
}

fn spec_two_symbols() -> StrategySpec {
    let config = strategy_engine::strategy::config::StrategyConfig {
        strategy_type: "template_xsec".into(),
        strategy_id: Some("template_xsec:default".into()),
        mode: strategy_engine::strategy::config::StrategyMode::CrossSectional,
        merge_positions: true,
        universe: vec![
            strategy_engine::strategy::config::UniverseEntry::Symbol("BTCUSDT".into()),
            strategy_engine::strategy::config::UniverseEntry::Symbol("ETHUSDT".into()),
        ],
        timeframes: vec!["1m".into()],
        indicators: Default::default(),
        entry: Default::default(),
        exit: Default::default(),
        portfolio_exit: strategy_engine::strategy::config::PortfolioExitCfg {
            trailing: None,
            stop_loss_pct: None,
        },
        rebalance: Some(strategy_engine::strategy::config::RebalanceCfg {
            every_bars: 5,
            top_k: 2,
            bottom_k: 0,
            weight_scheme: strategy_engine::strategy::config::WeightScheme::Equal,
        }),
        risk: strategy_engine::strategy::config::RiskCfg {
            max_drawdown: None,
            max_total_notional: 1.0,
        },
        sizing: strategy_engine::strategy::config::SizingCfg {
            per_position_pct: 0.1,
            max_position_pct: None,
        },
        params: None,
    };
    StrategySpec {
        strategy_type: "template_xsec".into(),
        strategy_id: "template_xsec:default".into(),
        config,
        instruments: vec![symbol_spec("BTCUSDT"), symbol_spec("ETHUSDT")],
        warmup_days: 0,
        run_mode: "paper_trading".into(),
        services: None,
        state_flush_interval_secs: 60,
        state_prune_terminal: false,
    }
}

struct HoldAll;
impl strategy_engine::strategy::port::CrossSectionalStrategyPort for HoldAll {
    fn strategy_type(&self) -> &str {
        "hold_all"
    }

    fn on_cross_section_bars(
        &mut self,
        _newly: &[Timeframe],
        _snaps: &[strategy_engine::strategy::port::InstrumentSnapshot],
        _port: &strategy_engine::strategy::port::PortfolioContext,
    ) -> Vec<strategy_engine::strategy::port::InstrumentAction> {
        Vec::new()
    }
}

fn run_with(
    strategy: Box<dyn strategy_engine::strategy::port::CrossSectionalStrategyPort>,
    out: &Path,
) -> anyhow::Result<strategy_app::spi_backtest::StrategyBacktestRun> {
    let spec = spec_two_symbols();
    let trading = vec![candles(40, 100.0, 0.1), candles(40, 50.0, 0.05)];
    run_strategy_from_spec(StrategyRunArgs {
        spec: &spec,
        strategy,
        warmup: &[vec![], vec![]],
        trading: &trading,
        initial_cash: 1000.0,
        fee_rate: Some(0.0),
        state_dir: &out.join("state"),
        data_dir: &out.join("data"),
    })
}

#[test]
fn strategy_run_assembles_two_slots_and_returns_portfolio_equity() {
    let tmp = tempfile::tempdir().unwrap();
    let run = run_with(Box::new(HoldAll), tmp.path()).expect("装配+主循环应成功");
    assert_eq!(run.summary.bars_processed, 80, "2 标的 × 40 根 1m");
    assert!(run.initial_cash > 0.0);
    assert!(!run.daily.is_empty(), "组合净值点应落 Store");
    assert!(run.trade_rows.is_empty(), "Hold 策略无成交");
    // 每 slot 资金均分 500 → NAV = Σ equity = 1000（无仓）
    assert!(
        (run.summary.final_portfolio.nav - 1000.0).abs() < 1e-6,
        "nav={}",
        run.summary.final_portfolio.nav
    );
}

/// 脚本策略：首轮对 slot 标的开多（验证 sizing + sub_ ID + fill 落账）。
struct ScriptedOpen {
    fired: bool,
}

impl strategy_engine::strategy::port::CrossSectionalStrategyPort for ScriptedOpen {
    fn strategy_type(&self) -> &str {
        "scripted_open"
    }

    fn on_cross_section_bars(
        &mut self,
        _newly: &[Timeframe],
        snaps: &[strategy_engine::strategy::port::InstrumentSnapshot],
        _port: &strategy_engine::strategy::port::PortfolioContext,
    ) -> Vec<strategy_engine::strategy::port::InstrumentAction> {
        if self.fired || snaps.is_empty() {
            return Vec::new();
        }
        self.fired = true;
        let s = &snaps[0];
        vec![strategy_engine::strategy::port::InstrumentAction::Open {
            instrument: s.instrument.clone(),
            side: strategy_core::signal::Side::Long,
            price: s.bar.close,
            stop_price: s.bar.close * 0.98,
            atr: 0.0,
            strength: 1.0,
            reason: "scripted".into(),
        }]
    }
}

#[test]
fn open_fill_uses_sizing_and_sub_signal_namespace() {
    let tmp = tempfile::tempdir().unwrap();
    let run = run_with(Box::new(ScriptedOpen { fired: false }), tmp.path()).unwrap();
    assert_eq!(run.summary.fills.len(), 1, "恰一笔开仓成交");
    let f = &run.summary.fills[0];
    assert!(f.signal_id.starts_with("sub_"), "signal_id={}", f.signal_id);
    // sizing：per_position_pct 默认 0.1 × nav 1000 / (100 × 1) = 1.0
    assert!((f.qty - 1.0).abs() < 1e-9, "qty={}", f.qty);
    assert_eq!(run.trade_rows.len(), 1);
    assert_eq!(run.trade_rows[0].symbol, "BTCUSDT");
    let subs = &run.summary.final_sub_positions;
    assert_eq!(subs.len(), 1);
    assert_eq!(
        subs[0].state,
        strategy_engine::strategy::model::SubPositionState::Active,
        "fill 落地后子仓应 Active"
    );
}

// ─── Task 19：确定性、黄金值、warmup、entry/exit 场景、示例策略 ─────────────

use strategy_engine::strategy::port::InstrumentAction as XAction;
use strategy_engine::strategy::port::PortfolioContext as PCtx;
use strategy_engine::strategy::runner::StrategyRunSummary;

fn spec_with(
    universe: &[&str],
    timeframes: &[&str],
    indicators: std::collections::BTreeMap<String, strategy_engine::strategy::config::IndicatorCfg>,
    exit_overrides: std::collections::BTreeMap<
        String,
        Vec<strategy_engine::strategy::config::Rule>,
    >,
    params: Option<serde_json::Value>,
) -> StrategySpec {
    let mut cfg = strategy_engine::strategy::config::StrategyConfig {
        strategy_type: "template_xsec".into(),
        strategy_id: Some("template_xsec:default".into()),
        mode: strategy_engine::strategy::config::StrategyMode::CrossSectional,
        merge_positions: true,
        universe: universe
            .iter()
            .map(|u| strategy_engine::strategy::config::UniverseEntry::Symbol((*u).into()))
            .collect(),
        timeframes: timeframes.iter().map(|t| t.to_string()).collect(),
        indicators,
        entry: Default::default(),
        exit: strategy_engine::strategy::config::RuleSet {
            default: Vec::new(),
            overrides: exit_overrides,
        },
        portfolio_exit: strategy_engine::strategy::config::PortfolioExitCfg {
            trailing: None,
            stop_loss_pct: None,
        },
        rebalance: Some(strategy_engine::strategy::config::RebalanceCfg {
            every_bars: 96,
            top_k: 1,
            bottom_k: 0,
            weight_scheme: strategy_engine::strategy::config::WeightScheme::Equal,
        }),
        risk: strategy_engine::strategy::config::RiskCfg {
            max_drawdown: None,
            max_total_notional: 1.0,
        },
        sizing: strategy_engine::strategy::config::SizingCfg {
            per_position_pct: 0.1,
            max_position_pct: None,
        },
        params,
    };
    if universe.len() == 1 {
        cfg.rebalance = None;
    }
    StrategySpec {
        strategy_type: "template_xsec".into(),
        strategy_id: "template_xsec:default".into(),
        instruments: universe.iter().map(|u| symbol_spec(u)).collect(),
        config: cfg,
        warmup_days: 0,
        run_mode: "paper_trading".into(),
        services: None,
        state_flush_interval_secs: 60,
        state_prune_terminal: false,
    }
}

fn sma_rule(op_gt: bool, v: f64) -> strategy_engine::strategy::config::Rule {
    strategy_engine::strategy::config::Rule {
        ind: "sma".into(),
        op: if op_gt {
            strategy_engine::strategy::config::RuleOp::Gt
        } else {
            strategy_engine::strategy::config::RuleOp::Lt
        },
        value: strategy_engine::strategy::config::RuleValue::Number(v),
    }
}

fn sma_ind() -> std::collections::BTreeMap<String, strategy_engine::strategy::config::IndicatorCfg>
{
    let mut m = std::collections::BTreeMap::new();
    m.insert(
        "sma".into(),
        strategy_engine::strategy::config::IndicatorCfg {
            tf: "1m".into(),
            params: std::collections::BTreeMap::from_iter([(
                "period".into(),
                serde_json::json!(1),
            )]),
        },
    );
    m
}

/// 一次跑完的完整指纹（成交 + 权益），用于确定性对比。
fn fingerprint(
    run: &(
        StrategyRunSummary,
        Vec<strategy_app::report::TradeRow>,
        Vec<strategy_app::report::EquityPoint>,
    ),
) -> String {
    let mut s = String::new();
    for f in &run.0.fills {
        s.push_str(&format!(
            "{}|{}|{}|{:.10}|{:.10}\n",
            f.signal_id, f.instrument.symbol, f.offset as u8, f.qty, f.price
        ));
    }
    for t in &run.1 {
        s.push_str(&format!("{}|{}|{}\n", t.trade_id, t.symbol, t.timestamp));
    }
    for e in &run.2 {
        s.push_str(&format!("{:?}|{:.12}\n", e.id, e.equity));
    }
    s
}

fn run_scenario(
    out: &Path,
    strategy: Box<dyn strategy_engine::strategy::port::CrossSectionalStrategyPort>,
    spec: &StrategySpec,
    trading: &[Vec<Candle>],
) -> (
    StrategyRunSummary,
    Vec<strategy_app::report::TradeRow>,
    Vec<strategy_app::report::EquityPoint>,
) {
    let run = run_strategy_from_spec(StrategyRunArgs {
        spec,
        strategy,
        warmup: &vec![vec![]; spec.instruments.len()],
        trading,
        initial_cash: 1000.0,
        fee_rate: Some(0.0),
        state_dir: &out.join("state"),
        data_dir: &out.join("data"),
    })
    .expect("场景回测应成功");
    (run.summary, run.trade_rows, run.daily)
}

struct SeqStrategy {
    responses: std::collections::VecDeque<Vec<XAction>>,
}

impl strategy_engine::strategy::port::CrossSectionalStrategyPort for SeqStrategy {
    fn strategy_type(&self) -> &str {
        "seq"
    }

    fn on_cross_section_bars(
        &mut self,
        _n: &[strategy_engine::core::bar::Timeframe],
        snaps: &[strategy_engine::strategy::port::InstrumentSnapshot],
        _p: &PCtx,
    ) -> Vec<XAction> {
        let Some(r) = self.responses.pop_front() else {
            return Vec::new();
        };
        // 按当轮快照改写 instrument（构造时未知精确对象）。
        r.into_iter()
            .map(|a| match a {
                XAction::Open {
                    side,
                    price,
                    stop_price,
                    atr,
                    strength,
                    reason,
                    ..
                } => {
                    let inst = snaps[0].instrument.clone();
                    XAction::Open {
                        instrument: inst,
                        side,
                        price,
                        stop_price,
                        atr,
                        strength,
                        reason,
                    }
                }
                other => other,
            })
            .collect()
    }
}

fn seq(
    responses: Vec<Vec<XAction>>,
) -> Box<dyn strategy_engine::strategy::port::CrossSectionalStrategyPort> {
    Box::new(SeqStrategy {
        responses: responses.into(),
    })
}

fn open_sym(symbol: &str, price: f64) -> XAction {
    use strategy_engine::prelude::Exchange as _;
    let ex = strategy_engine::exchanges::binance::BinanceExchange::new(0.0);
    let inst = ex.parse_instrument(symbol).unwrap();
    XAction::Open {
        instrument: inst,
        side: strategy_core::signal::Side::Long,
        price,
        stop_price: price * 0.98,
        atr: 0.0,
        strength: 1.0,
        reason: "seq".into(),
    }
}

fn rising_candles(n: usize, base: f64, step: f64) -> Vec<Candle> {
    candles(n, base, step)
}

#[test]
fn same_input_twice_is_byte_identical() {
    let spec = spec_with(
        &["BTCUSDT"],
        &["1m"],
        Default::default(),
        Default::default(),
        None,
    );
    let trading = vec![rising_candles(60, 100.0, 0.2)];
    let t1 = tempfile::tempdir().unwrap();
    let t2 = tempfile::tempdir().unwrap();
    let f1 = fingerprint(&run_scenario(
        t1.path(),
        seq(vec![vec![open_sym("BTCUSDT", 100.0)]]),
        &spec,
        &trading,
    ));
    let f2 = fingerprint(&run_scenario(
        t2.path(),
        seq(vec![vec![open_sym("BTCUSDT", 100.0)]]),
        &spec,
        &trading,
    ));
    assert!(!f1.is_empty(), "指纹不应为空");
    assert_eq!(f1, f2, "同输入两次运行的 signal_id/成交/权益必须逐字节一致");
}

#[test]
fn entry_exit_scenario_with_sub_golden_exit_rule_leg() {
    let mut overrides = std::collections::BTreeMap::new();
    overrides.insert("ETHUSDT".into(), vec![sma_rule(true, 120.0)]);
    let spec = spec_with(&["ETHUSDT"], &["1m"], sma_ind(), overrides, None);
    let prices: Vec<Candle> = {
        let mut v = candles(3, 100.0, 0.0);
        v.extend(candles_at(3, 60, 130.0));
        v
    };
    let tmp = tempfile::tempdir().unwrap();
    let (summary, rows, _) = run_scenario(
        tmp.path(),
        seq(vec![vec![open_sym("ETHUSDT", 100.0)]]),
        &spec,
        &[prices],
    );
    let open = summary
        .fills
        .iter()
        .find(|f| f.signal_id.starts_with("sub_") && !f.signal_id.contains("_leg"))
        .expect("开仓 fill");
    let close = summary
        .fills
        .iter()
        .find(|f| f.signal_id.contains("_leg"))
        .expect("exit_rule 平仓 leg");
    // 黄金值：base = generate_sub_signal_id(..., at=t3(00:03), side=Short, ExitRule, 0)
    // （strategy_type 改为 template_xsec 后的确定性哈希，同输入两次运行逐字节一致）
    let golden_base = "sub_16d90164027dec8b";
    assert_eq!(
        close.signal_id,
        format!("{golden_base}_leg0"),
        "close leg 黄金值钉死"
    );
    assert!(!open.signal_id.starts_with(golden_base));
    assert_eq!(rows.len(), 2);
    // t3 的 exit 触发后，同标的本轮策略动作被抑制（Seq 无后续动作，验证 state 推进）：
    assert_eq!(
        summary.final_sub_positions.len(),
        1,
        "全平后仍留 Closed 账本行"
    );
}

fn candles_at(start_min: usize, n: usize, base: f64) -> Vec<Candle> {
    (0..n)
        .map(|i| {
            let v = base;
            Candle {
                open_time: utc(start_min as u32 + i as u32),
                open: v,
                high: v,
                low: v,
                close: v,
                volume: 1.0,
                quote_volume: 0.0,
                trade_num: 0,
                active_buy_volume: 0.0,
                active_buy_quote_volume: 0.0,
                is_closed: true,
            }
        })
        .collect()
}

#[test]
fn warmup_insufficient_blocks_opens() {
    let adx_gate = strategy_engine::strategy::config::Rule {
        ind: "adx".into(),
        op: strategy_engine::strategy::config::RuleOp::Gt,
        value: strategy_engine::strategy::config::RuleValue::Number(20.0),
    };
    let spec = {
        let mut sp = spec_with(&["BTCUSDT"], &["1m"], sma_ind(), Default::default(), None);
        sp.config.entry = strategy_engine::strategy::config::RuleSet {
            default: vec![adx_gate],
            overrides: Default::default(),
        };
        sp
    };
    let tmp = tempfile::tempdir().unwrap();
    let (summary, _, _) = run_scenario(
        tmp.path(),
        seq(vec![vec![open_sym("BTCUSDT", 100.0)]]),
        &spec,
        &[rising_candles(30, 100.0, 0.1)],
    );
    assert!(
        summary.fills.is_empty(),
        "特征缺失 → entry_eligible=false → 不得开仓"
    );
    assert!(summary.rejected_actions >= 1);
}

#[test]
fn template_xsec_example_runs_two_symbols_and_trades() {
    let mut inds = std::collections::BTreeMap::new();
    inds.insert(
        "macd".into(),
        strategy_engine::strategy::config::IndicatorCfg {
            tf: "1m".into(),
            params: std::collections::BTreeMap::from_iter([
                ("fast".into(), serde_json::json!(3)),
                ("slow".into(), serde_json::json!(5)),
                ("signal".into(), serde_json::json!(2)),
            ]),
        },
    );
    let spec = spec_with(
        &["BTCUSDT", "ETHUSDT"],
        &["1m"],
        inds,
        Default::default(),
        Some(serde_json::json!({ "top_k": 1 })),
    );
    // BTC 强势上行、ETH 震荡走弱：每 96 根触发轮换，应选中 BTC。
    let btc = rising_candles(400, 100.0, 0.2);
    let eth = rising_candles(400, 50.0, -0.02);
    let tmp = tempfile::tempdir().unwrap();
    let inst = &spec.instruments[0];
    let args = strategy_strategies::registry::FactoryArgs {
        symbol: &inst.symbol,
        timeframe: &inst.timeframe,
        cash: inst.cash,
        params: spec.config.params.as_ref(),
    };
    let strategy = strategy_strategies::registry::create("template_xsec", &args).unwrap();
    let (summary, rows, daily) = run_scenario(tmp.path(), strategy, &spec, &[btc, eth]);
    assert!(
        summary.rejected_actions <= summary.actions,
        "示例策略动作应基本成功"
    );
    assert!(!summary.fills.is_empty(), "400 根后应出现轮换成交");
    assert_eq!(rows[0].strategy_name, "template_xsec:default");
    assert!(!daily.is_empty());
    assert!(
        summary.final_portfolio.rebalance_epoch >= 1,
        "每次 Phase 2 完成都应推进 epoch"
    );
    assert!(
        summary
            .fills
            .iter()
            .any(|f| f.instrument.symbol == "BTCUSDT"
                && matches!(f.offset, strategy_engine::core::trading::Offset::Open)),
        "400 根后应选中强势 BTC 开仓"
    );
}
