//! 跨交易所契约测试（§12）——第一公理、混跑主循环、止损/反手/盯市/装配/raw 链。
//! 说明：§12 原文位置写 `core/`，但本组测试同时依赖 `core` 与 `exchanges`
//! 两层，放进 core 会违反分层铁律，故落在 crate 级集成测试目录（等价且更干净）。

use chrono::{DateTime, TimeZone, Utc};
use std::sync::Arc;
use strategy_core::signal::Side;
use strategy_core::strategy::Cooldown;
use strategy_engine::core::execution::{ExecutionPort, SimulationExecution};
use strategy_engine::exchanges::binance::BinanceExchange;
use strategy_engine::exchanges::ctp::CtpExchange;
use strategy_engine::prelude::*;

fn bar(inst: &InstrumentId, t: DateTime<Utc>, close: f64) -> Bar {
    Bar {
        instrument: inst.clone(),
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
/// 一次性「开多 → 平仓」演示策略。
struct OpenThenClose {
    calls: usize,
}

impl StrategyPort for OpenThenClose {
    fn on_closed_bars(
        &mut self,
        _newly: &[Timeframe],
        _state: &AggregationState,
        ctx: &DecisionContext,
    ) -> Vec<StrategyAction> {
        self.calls += 1;
        match self.calls {
            1 => vec![StrategyAction::Open {
                side: Side::Long,
                price: ctx.price,
                stop_price: ctx.price * 0.9,
                atr: 0.0,
                strength: 1.0,
                reason: "test".into(),
            }],
            2 => vec![StrategyAction::Close {
                price: ctx.price,
                reason: "test".into(),
                is_stop_loss: false,
                strength: 1.0,
            }],
            _ => vec![StrategyAction::Hold],
        }
    }
}
/// 演示「接实盘只需覆写两个端口」：所有规则方法都委托给同一个
/// [`CtpExchange`] 实例，一行规则代码都不重写。
struct LiveCtpExchange(CtpExchange);

impl Exchange for LiveCtpExchange {
    fn id(&self) -> &ExchangeId {
        self.0.id()
    }
    fn markets(&self) -> Vec<MarketKind> {
        self.0.markets()
    }
    fn parse_instrument(&self, raw: &str) -> Result<InstrumentId> {
        self.0.parse_instrument(raw)
    }
    fn session_rules(&self, inst: &InstrumentId) -> Arc<dyn SessionRules> {
        self.0.session_rules(inst)
    }
    fn aggregation_policy(&self, i: &InstrumentId) -> Arc<dyn BarAggregationPolicy> {
        self.0.aggregation_policy(i)
    }
    fn subscription_plan(&self) -> Arc<dyn SubscriptionPlan> {
        self.0.subscription_plan()
    }
    fn trading_rules(&self) -> Arc<dyn TradingRules> {
        self.0.trading_rules()
    }
    fn position_rules(&self) -> Arc<dyn PositionRules> {
        self.0.position_rules()
    }
    fn simulation_policy(&self) -> Arc<dyn SimulationPolicy> {
        self.0.simulation_policy()
    }

    // 实盘差异一：WS 行情源（此处用同一个回放源占位，形状一致）。
    fn make_source(&self, req: &SourceRequest) -> Result<Box<dyn BarEventSource>> {
        Ok(Box::new(VecEventSource::from_bars(req.bars.clone())))
    }

    // 实盘差异二：远程订单网关（此处仍用模拟撮合占位，但注入同一批规则）。
    fn make_execution(&self, req: &SourceRequest) -> Result<Box<dyn ExecutionPort>> {
        Ok(Box::new(SimulationExecution::new(
            req.instrument.clone(),
            req.initial_cash,
            self.0.trading_rules(),
            self.0.position_rules(),
            self.0.session_rules(&req.instrument),
            self.0.simulation_policy(),
        )))
    }
}
// ─── v2.1：§16 P1 修复的回归测试 ─────────────────────────────────────

/// 脚本化策略：按 bar 顺序吐出预置动作，并记录每根收盘时的 ctx.position。
#[derive(Default)]
struct Scripted {
    script: Vec<Vec<StrategyAction>>,
    i: usize,
    stops: Vec<Option<f64>>,
}

impl StrategyPort for Scripted {
    fn on_closed_bars(
        &mut self,
        _n: &[Timeframe],
        _s: &AggregationState,
        ctx: &DecisionContext,
    ) -> Vec<StrategyAction> {
        self.stops.push(ctx.position.as_ref().map(|p| p.stop_price));
        let a = self.script.get(self.i).cloned().unwrap_or_default();
        self.i += 1;
        a
    }
}
/// 设计第一公理的契约测试：**规则只实现一次**。
/// live 与 backtest 只允许在 source / execution 两个端口上不同。
#[test]
fn live_and_backtest_share_the_same_rules() {
    let ex = LiveCtpExchange(CtpExchange::new());
    let inst = ex.parse_instrument("CU2601").unwrap();

    let bt = ex
        .build(&SourceRequest::backtest(
            inst.clone(),
            Vec::new(),
            1_000_000.0,
        ))
        .unwrap();
    let mut live_req = SourceRequest::backtest(inst.clone(), Vec::new(), 1_000_000.0);
    live_req.mode = RunMode::Live;
    let live = ex.build(&live_req).unwrap();

    // 1) 规则对象是同一个 Arc，不是各自 new 一份（杜绝口径漂移）。
    assert!(
        Arc::ptr_eq(&bt.trading, &live.trading),
        "TradingRules 必须是同一个 Arc"
    );
    assert!(
        Arc::ptr_eq(&bt.session, &live.session),
        "SessionRules 必须是同一个 Arc"
    );
    assert_eq!(bt.position.mode(), live.position.mode());

    // 2) 同一组订单，交易日 / 价格规整 / 手数 / 手续费 / 盈亏逐项一致。
    let t = utc(2026, 1, 5, 1, 10); // 北京 09:10
    assert_eq!(bt.session.trading_day_of(t), live.session.trading_day_of(t));
    assert_eq!(
        bt.trading.normalize_price(&inst, 50007.0).unwrap(),
        live.trading.normalize_price(&inst, 50007.0).unwrap()
    );
    let sizing = SizingRequest {
        instrument: inst.clone(),
        price: 50000.0,
        equity: 1_000_000.0,
        leverage: 1.0,
        notional_per_order: None,
    };
    assert_eq!(
        bt.trading.sizing(&sizing).unwrap(),
        live.trading.sizing(&sizing).unwrap()
    );
    let close_today = FeeRequest {
        instrument: inst.clone(),
        price: 50000.0,
        qty: 4.0,
        offset: Offset::CloseToday,
    };
    let open = FeeRequest {
        offset: Offset::Open,
        ..close_today.clone()
    };
    assert_eq!(
        bt.trading.fee(&close_today).unwrap(),
        live.trading.fee(&close_today).unwrap()
    );
    assert!(
        bt.trading.fee(&close_today).unwrap() > bt.trading.fee(&open).unwrap(),
        "平今费率必须高于开仓"
    );
    assert_eq!(
        bt.trading
            .pnl(&inst, Side::Long, 50000.0, 50100.0, 4.0)
            .unwrap(),
        live.trading
            .pnl(&inst, Side::Long, 50000.0, 50100.0, 4.0)
            .unwrap()
    );

    // 3) 除两个端口外，唯一的区别就是 mode 标签本身。
    assert_eq!(bt.mode, RunMode::Backtest);
    assert_eq!(live.mode, RunMode::Live);
}

#[test]
fn d1_bucket_differs_between_crypto_and_futures() {
    let crypto = BinanceExchange::new(0.0004);
    let fut = CtpExchange::new();
    let t = utc(2026, 1, 5, 1, 0); // 北京 09:00

    let c_inst = crypto.parse_instrument("BTCUSDT").unwrap();
    let c_session = crypto.session_rules(&c_inst);
    let c_bucket = crypto
        .aggregation_policy(&c_inst)
        .bucket_of(Timeframe::D1, t, c_session.as_ref())
        .unwrap();
    assert_eq!(
        c_bucket.start,
        utc(2026, 1, 5, 0, 0),
        "crypto D1 = UTC 午夜"
    );

    let f_inst = fut.parse_instrument("CU2601").unwrap();
    let f_session = fut.session_rules(&f_inst);
    let f_bucket = fut
        .aggregation_policy(&f_inst)
        .bucket_of(Timeframe::D1, t, f_session.as_ref())
        .unwrap();
    // 期货交易日 = 周五夜盘 + 周一白天，D1 桶起点是上个交易日夜盘开盘，
    // 而不是 UTC 午夜，也不是当日 9:00。
    assert_eq!(
        f_bucket.start,
        utc(2026, 1, 2, 13, 0),
        "期货 D1 起于周五 21:00 北京"
    );
    assert_eq!(
        f_bucket.window_end,
        Some(utc(2026, 1, 5, 7, 0)),
        "期货 D1 止于周一 15:00 北京"
    );
}
#[test]
fn two_exchanges_run_same_main_loop_in_one_registry() {
    let crypto = Arc::new(BinanceExchange::new(0.0004));
    let fut = Arc::new(CtpExchange::new());
    let c_inst = crypto.parse_instrument("BTCUSDT").unwrap();
    let f_inst = fut.parse_instrument("CU2601").unwrap();

    let mut reg = ExchangeRegistry::new();
    reg.register(crypto.clone());
    reg.register(fut.clone());
    assert_eq!(reg.len(), 2);

    let c_bars: Vec<Bar> = (0..4)
        .map(|i| bar(&c_inst, utc(2026, 1, 5, 0, i), 100.0 + i as f64))
        .collect();
    // 避开开盘后 5 分钟禁交易窗口（09:00-09:05 北京）
    let f_bars: Vec<Bar> = (0..4)
        .map(|i| {
            bar(
                &f_inst,
                utc(2026, 1, 5, 1, 10 + i),
                50000.0 + 10.0 * i as f64,
            )
        })
        .collect();

    let mut parts = reg
        .build_all(vec![
            SourceRequest::backtest(c_inst.clone(), c_bars, 100_000.0),
            SourceRequest::backtest(f_inst.clone(), f_bars, 1_000_000.0),
        ])
        .unwrap();
    assert_eq!(parts.len(), 2);

    let mut runner_c = ExchangeRunner::new(
        parts.remove(&c_inst.namespaced_key()).unwrap(),
        vec![Timeframe::M1, Timeframe::H1],
        200,
    );
    let mut runner_f = ExchangeRunner::new(
        parts.remove(&f_inst.namespaced_key()).unwrap(),
        vec![Timeframe::M1, Timeframe::H1],
        200,
    );

    let sc = runner_c.run(&mut OpenThenClose { calls: 0 }).unwrap();
    let sf = runner_f.run(&mut OpenThenClose { calls: 0 }).unwrap();

    assert_eq!(sc.fills.len(), 2, "crypto 开 + 平");
    assert_eq!(sf.fills.len(), 2, "期货 开 + 平");
    assert_eq!(sc.fills[1].offset, Offset::Auto, "crypto 无平今概念");
    assert_eq!(
        sf.fills[1].offset,
        Offset::CloseToday,
        "期货同交易日平仓 = 平今"
    );
    assert!(sf.fills[1].fee > sf.fills[0].fee, "平今费率高于开仓");
    assert!(sc.account.is_some() && sf.account.is_some());
}

#[test]
fn provisional_bar_never_drives_strategy() {
    let ex = Arc::new(BinanceExchange::new(0.0004));
    let inst = ex.parse_instrument("BTCUSDT").unwrap();
    let mut provisional = bar(&inst, utc(2026, 1, 5, 0, 0), 100.0);
    provisional.finality = BarFinality::Provisional;
    let mut reg = ExchangeRegistry::new();
    reg.register(ex);
    let mut parts = reg
        .build_all(vec![SourceRequest::backtest(
            inst.clone(),
            vec![provisional, bar(&inst, utc(2026, 1, 5, 0, 1), 101.0)],
            100_000.0,
        )])
        .unwrap();
    let mut runner = ExchangeRunner::new(
        parts.remove(&inst.namespaced_key()).unwrap(),
        vec![Timeframe::M1],
        100,
    );
    let s = runner.run(&mut OpenThenClose { calls: 0 }).unwrap();
    assert_eq!(s.provisional_bars, 1);
    assert_eq!(s.bars_processed, 1);
    assert_eq!(s.fills.len(), 1, "只有第二根完整 K 线触发了策略");
}
/// C1：UpdateStop 落盘、持仓静默时统一风控兜底（收盘价口径）、
/// 止损成交记冷却日（K 线时间，非 now()，与 manager.rs 同口径）。
#[test]
fn stop_loss_fallback_update_stop_and_cooldown() {
    let ex = Arc::new(BinanceExchange::new(0.0004));
    let inst = ex.parse_instrument("BTCUSDT").unwrap();
    let bars = vec![
        bar(&inst, utc(2026, 1, 5, 0, 0), 100.0),
        bar(&inst, utc(2026, 1, 5, 0, 1), 85.0), // -15%：不触发
        bar(&inst, utc(2026, 1, 5, 0, 2), 75.0), // -25%：兜底强平
    ];
    let mut reg = ExchangeRegistry::new();
    reg.register(ex);
    let mut parts = reg
        .build_all(vec![SourceRequest::backtest(inst.clone(), bars, 100_000.0)])
        .unwrap();
    let mut runner = ExchangeRunner::new(
        parts.remove(&inst.namespaced_key()).unwrap(),
        vec![Timeframe::M1],
        100,
    );
    let mut strat = Scripted {
        script: vec![
            vec![StrategyAction::Open {
                side: Side::Long,
                price: 100.0,
                stop_price: 85.0,
                atr: 2.0,
                strength: 1.0,
                reason: "t".into(),
            }],
            vec![StrategyAction::UpdateStop { stop_price: 80.0 }],
            vec![StrategyAction::Hold],
        ],
        ..Default::default()
    };
    let s = runner.run(&mut strat).unwrap();
    // ctx 注入验证：bar2 看到开仓落盘的 85，bar3 看到 UpdateStop 后的 80
    assert_eq!(strat.stops[0], None, "开仓前空仓");
    assert_eq!(strat.stops[1], Some(85.0), "Open.stop_price 落盘");
    assert_eq!(strat.stops[2], Some(80.0), "UpdateStop 落盘");
    // 策略静默 → 统一风控兜底出场
    assert_eq!(s.fills.len(), 2, "开仓 + 风控兜底平仓");
    assert!(s.fills[1].is_stop_loss, "固定止损成交必须带标志");
    assert_eq!(s.fills[1].offset, Offset::Auto);
    assert_eq!(
        runner.pos_sync.cooldown,
        Some(Cooldown::UntilDate("2026-01-05".into()))
    );
    assert!(runner.pos_sync.pos.is_none(), "平仓后仓位状态清除");
}

/// C1：Reverse = 先平后开，一根 bar 内三条成交，反手后持仓换向。
#[test]
fn reverse_flips_position_in_one_bar() {
    let ex = Arc::new(BinanceExchange::new(0.0004));
    let inst = ex.parse_instrument("BTCUSDT").unwrap();
    let bars = vec![
        bar(&inst, utc(2026, 1, 5, 0, 0), 100.0),
        bar(&inst, utc(2026, 1, 5, 0, 1), 101.0),
    ];
    let mut reg = ExchangeRegistry::new();
    reg.register(ex);
    let mut parts = reg
        .build_all(vec![SourceRequest::backtest(inst.clone(), bars, 100_000.0)])
        .unwrap();
    let mut runner = ExchangeRunner::new(
        parts.remove(&inst.namespaced_key()).unwrap(),
        vec![Timeframe::M1],
        100,
    );
    let mut strat = Scripted {
        script: vec![
            vec![StrategyAction::Open {
                side: Side::Long,
                price: 100.0,
                stop_price: 90.0,
                atr: 1.0,
                strength: 1.0,
                reason: "t".into(),
            }],
            vec![StrategyAction::Reverse {
                side: Side::Short,
                price: 101.0,
                stop_price: 110.0,
                atr: 1.0,
                strength: 1.0,
                close_reason: "flip".into(),
                is_stop_loss: false,
            }],
        ],
        ..Default::default()
    };
    let s = runner.run(&mut strat).unwrap();
    assert_eq!(s.fills.len(), 3, "开 → 平 → 反手开");
    assert_eq!(s.fills[1].offset, Offset::Auto);
    assert_eq!(s.fills[2].offset, Offset::Open);
    let p = runner.pos_sync.pos.as_ref().expect("反手后持仓存在");
    assert_eq!(p.side, strategy_positions::Side::Short);
    assert_eq!(p.stop_price, 110.0, "反手新仓止损来自动作");
}

/// C3：盯市钩子进主循环——持仓浮盈必须进 `Account.equity()`。
#[test]
fn mark_to_market_lands_in_equity() {
    let ex = Arc::new(BinanceExchange::new(0.0004));
    let inst = ex.parse_instrument("BTCUSDT").unwrap();
    let bars = vec![
        bar(&inst, utc(2026, 1, 5, 0, 0), 100.0),
        bar(&inst, utc(2026, 1, 5, 0, 1), 110.0),
        bar(&inst, utc(2026, 1, 5, 0, 2), 120.0),
    ];
    let mut reg = ExchangeRegistry::new();
    reg.register(ex);
    let mut parts = reg
        .build_all(vec![SourceRequest::backtest(inst.clone(), bars, 100_000.0)])
        .unwrap();
    let mut runner = ExchangeRunner::new(
        parts.remove(&inst.namespaced_key()).unwrap(),
        vec![Timeframe::M1],
        100,
    );
    let mut strat = Scripted {
        script: vec![vec![StrategyAction::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 50.0,
            atr: 1.0,
            strength: 1.0,
            reason: "t".into(),
        }]],
        ..Default::default()
    };
    let s = runner.run(&mut strat).unwrap();
    let acc = s.account.expect("模拟出口必有账户");
    assert!(
        acc.unrealized_pnl > 19_000.0,
        "C3 修复前恒为 0：20% 浮盈 × ~1000 量 ≈ 20000，实得 {}",
        acc.unrealized_pnl
    );
    assert_eq!(acc.equity(), acc.cash + acc.unrealized_pnl);
}
/// C6：`SizingParams` 注入 `ExchangeRunner`——回测全仓 / paper 名义 /
/// 杠杆三种旧口径走同一通道（§2.5「装配差异」的收口点）。
#[test]
fn sizing_params_reach_open_orders() {
    use strategy_engine::core::trading::SizingParams;
    let inst = BinanceExchange::new(0.0004)
        .parse_instrument("BTCUSDT")
        .unwrap();
    let mk = |params: SizingParams| {
        let ex = Arc::new(BinanceExchange::new(0.0004));
        let mut reg = ExchangeRegistry::new();
        reg.register(ex);
        let mut parts = reg
            .build_all(vec![SourceRequest::backtest(
                inst.clone(),
                vec![bar(&inst, utc(2026, 1, 5, 0, 0), 100.0)],
                100_000.0,
            )])
            .unwrap();
        let mut runner = ExchangeRunner::new(
            parts.remove(&inst.namespaced_key()).unwrap(),
            vec![Timeframe::M1],
            100,
        )
        .with_sizing_params(params);
        runner
            .run(&mut Scripted {
                script: vec![vec![StrategyAction::Open {
                    side: Side::Long,
                    price: 100.0,
                    stop_price: 50.0,
                    atr: 1.0,
                    strength: 1.0,
                    reason: "t".into(),
                }]],
                ..Default::default()
            })
            .unwrap()
    };
    let d = mk(SizingParams::default()).fills[0].qty;
    let lev = mk(SizingParams {
        leverage: 2.0,
        notional_per_order: None,
    })
    .fills[0]
        .qty;
    let cap = mk(SizingParams {
        leverage: 1.0,
        notional_per_order: Some(50_000.0),
    })
    .fills[0]
        .qty;
    assert_eq!(d, 1000.0, "默认全仓 1 倍：equity/price");
    assert_eq!(lev, 2000.0, "C6：leverage 注入通道生效");
    assert_eq!(cap, 500.0, "C6：notional_per_order 每单名义上限生效");
}
/// C7 端到端：开/平成交的 Fill 与腿的 raw 全链贯通（模拟按精度派生）。
#[test]
fn fill_raw_populates_position_ledger() {
    let ex = Arc::new(BinanceExchange::new(0.0004));
    let inst = ex.parse_instrument("BTCUSDT").unwrap();
    let bars = vec![
        bar(&inst, utc(2026, 1, 5, 0, 0), 100.0),
        bar(&inst, utc(2026, 1, 5, 0, 1), 101.0),
    ];
    let mut reg = ExchangeRegistry::new();
    reg.register(ex);
    let mut parts = reg
        .build_all(vec![SourceRequest::backtest(inst.clone(), bars, 100_000.0)])
        .unwrap();
    let mut runner = ExchangeRunner::new(
        parts.remove(&inst.namespaced_key()).unwrap(),
        vec![Timeframe::M1],
        100,
    );
    let mut strat = Scripted {
        script: vec![
            vec![StrategyAction::Open {
                side: Side::Long,
                price: 100.0,
                stop_price: 90.0,
                atr: 1.0,
                strength: 1.0,
                reason: "t".into(),
            }],
            vec![StrategyAction::Close {
                price: 101.0,
                reason: "t".into(),
                is_stop_loss: false,
                strength: 1.0,
            }],
        ],
        ..Default::default()
    };
    let s = runner.run(&mut strat).unwrap();
    assert_eq!(s.fills[0].qty_raw.as_deref(), Some("1000.000000"));
    // 平仓腿继承批次 remaining_raw（整消耗）→ Fill 沿用同一字面量
    assert_eq!(s.fills[1].qty_raw.as_deref(), Some("1000.000000"));
}
