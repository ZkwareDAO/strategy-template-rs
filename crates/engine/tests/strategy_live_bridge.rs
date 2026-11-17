//! PR-5 Task 1 验收：live 非阻塞事件接缝。
//!
//! 承重的三条验收（计划 `docs/superpowers/plans/2026-11-11-xsec-live-pr5.md`）：
//! 1. `ChannelBarSource::try_next_event`——「暂无」（`SourceIdle`）与
//!    「耗尽」（`Ok(None)`）语义严格区分，水位幂等/未闭合丢弃与阻塞式同规则；
//! 2. `AlignedEventSource::next_aligned_nonblocking`——发射序列与阻塞式
//!    `next_aligned` 逐项一致（G3 同构：live 只是把「等」搬到调用方），
//!    且慢源不饿死快源、终结信号 `Done` 与 `Ok(None)` 等价；
//! 3. 旧链路 `ExchangeRunner` 主循环对 `SourceIdle` 帧免疫（G5：旧行为
//!    不变，新帧只是「什么都不做」）。
//!
//! 本文件为新建验收资产，不触碰任何既有断言。

use chrono::{DateTime, Duration, TimeZone, Utc};
use std::collections::VecDeque;
use std::sync::mpsc;
use std::sync::Arc;
use strategy_core::candle::Candle;
use strategy_core::order::OrderState as CoreOrderState;
use strategy_core::signal::Side;
use strategy_engine::core::bar::{Bar, BarEventSource};
use strategy_engine::exchanges::binance::BinanceExchange;
use strategy_engine::exchanges::ctp::CtpExchange;
use strategy_engine::prelude::*;
use strategy_engine::strategy::align::{AlignPoll, AlignedEventSource};
use strategy_signals::manager::SignalManager;
use strategy_store::{EpochClock, Store};

fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
}

fn candle(t: DateTime<Utc>, close: f64) -> Candle {
    Candle {
        open_time: t,
        open: close,
        high: close,
        low: close,
        close,
        volume: 1.0,
        quote_volume: close,
        trade_num: 1,
        active_buy_volume: 0.0,
        active_buy_quote_volume: 0.0,
        is_closed: true,
    }
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

fn perpetual(name: &str) -> InstrumentId {
    InstrumentId::perpetual(&ExchangeId::new("binance"), name)
}

// ─── 验收 1：channel 源的空闲/事件/断流三态 ──────────────────────────────

#[test]
fn channel_source_reports_idle_then_event_then_none_on_disconnect() {
    let inst = perpetual("BTCUSDT");
    let (tx, rx) = mpsc::channel::<Candle>();
    let mut src = ChannelBarSource::new(inst.clone(), rx);
    let t1 = utc(2026, 1, 5, 0, 0);
    let t2 = t1 + Duration::minutes(1);

    // 空 channel：暂无 ≠ 耗尽——必须报 SourceIdle，绝不提前终结流。
    assert!(
        matches!(src.try_next_event().unwrap(), Some(MarketEvent::SourceIdle)),
        "空闲 channel 必须报 SourceIdle 而非 None"
    );

    // 闭合 1m：照常成 Bar，水位推进。
    tx.send(candle(t1, 100.0)).unwrap();
    let ev = src.try_next_event().unwrap().expect("闭合帧应立即返回");
    assert!(
        matches!(ev, MarketEvent::Bar(b) if b.open_time == t1),
        "闭合帧 → Bar"
    );
    assert_eq!(src.watermark(), Some(t1), "水位随闭合帧推进");

    // 未闭合快照 + 同/旧水位帧：在 try 内部被丢弃后报 SourceIdle，
    // 且不得扰动水位（与阻塞式同一过滤规则）。
    let mut unclosed = candle(t2, 101.0);
    unclosed.is_closed = false;
    tx.send(unclosed).unwrap();
    tx.send(candle(t1, 100.0)).unwrap(); // 同水位重复
    assert!(
        matches!(src.try_next_event().unwrap(), Some(MarketEvent::SourceIdle)),
        "被丢弃帧不应伪装成事件；丢弃耗尽后报 idle"
    );
    assert_eq!(src.watermark(), Some(t1), "idle 不推进水位");

    // 新闭合帧照常进入。
    tx.send(candle(t2, 101.0)).unwrap();
    let ev = src.try_next_event().unwrap().expect("第二根闭合");
    assert!(matches!(ev, MarketEvent::Bar(b) if b.open_time == t2));

    // 发送端 drop：断流 = 耗尽 = Ok(None)，与 idle 严格区分。
    drop(tx);
    assert!(
        src.try_next_event().unwrap().is_none(),
        "断流收尾必须 Ok(None)，供对齐层标记 done"
    );
}

// ─── 验收 2：非阻塞对齐与阻塞对齐逐项一致，且不饿死 ─────────────────────

/// 可控节奏源：每真事件前塞 `idle_every` 次 `SourceIdle`；
/// 阻塞式 `next_event` 无视 idle（直接给事件），用于对照发射序列。
struct IdleGatedSource {
    events: VecDeque<MarketEvent>,
    idle_every: usize,
    countdown: usize,
}

impl IdleGatedSource {
    fn new(bars: Vec<Bar>, idle_every: usize) -> Self {
        Self {
            events: bars
                .into_iter()
                .map(MarketEvent::Bar)
                .collect::<VecDeque<_>>(),
            idle_every,
            countdown: 0,
        }
    }
}

impl BarEventSource for IdleGatedSource {
    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        Ok(self.events.pop_front())
    }

    fn try_next_event(&mut self) -> Result<Option<MarketEvent>> {
        if self.countdown > 0 {
            self.countdown -= 1;
            return Ok(Some(MarketEvent::SourceIdle));
        }
        self.countdown = self.idle_every;
        Ok(self.events.pop_front())
    }
}

fn mk_aligned(sources: Vec<Box<dyn BarEventSource>>, keys: Vec<&str>) -> AlignedEventSource {
    AlignedEventSource::new(
        sources,
        keys.into_iter().map(String::from).collect::<Vec<_>>(),
    )
}

#[test]
fn align_nonblocking_no_starvation_across_two_sources() {
    let ix = perpetual("BTCUSDT");
    let t0 = utc(2026, 1, 5, 0, 0);
    let mk = |i: i64| bar(&ix, t0 + Duration::minutes(i));
    // slot0: t0..t2（idle 噪声 1）；slot1: t0..t1（idle 噪声 2，先耗尽）。
    let b0 = vec![mk(0), mk(1), mk(2)];
    let b1 = vec![mk(0), mk(1)];

    // 对照：阻塞式发射序列（与回测同一条码路）。
    let mut blk = mk_aligned(
        vec![
            Box::new(IdleGatedSource::new(b0.clone(), 1)) as Box<dyn BarEventSource>,
            Box::new(IdleGatedSource::new(b1.clone(), 2)) as Box<dyn BarEventSource>,
        ],
        vec!["s0", "s1"],
    );
    let mut expected = Vec::new();
    while let Some(ev) = blk.next_aligned().unwrap() {
        expected.push((ev.at, ev.slot));
    }
    assert_eq!(
        expected,
        vec![
            (t0, 0),
            (t0, 1),
            (t0 + Duration::minutes(1), 0),
            (t0 + Duration::minutes(1), 1),
            (t0 + Duration::minutes(2), 0),
        ],
        "对照序列自身先自洽：s1 先耗尽不得挡住 s0 的 t2"
    );

    // 非阻塞式：同一批数据、带 idle 噪声，发射序列必须逐项一致。
    let mut nb = mk_aligned(
        vec![
            Box::new(IdleGatedSource::new(b0, 1)) as Box<dyn BarEventSource>,
            Box::new(IdleGatedSource::new(b1, 2)) as Box<dyn BarEventSource>,
        ],
        vec!["s0", "s1"],
    );
    let mut got = Vec::new();
    let mut starved = 0usize;
    let mut guard = 0usize;
    loop {
        guard += 1;
        assert!(guard < 1_000, "轮询必须有界收敛，不得活锁");
        match nb.next_aligned_nonblocking().unwrap() {
            AlignPoll::Ready(ev) => got.push((ev.at, ev.slot)),
            AlignPoll::Starved => starved += 1,
            AlignPoll::Done => break,
        }
    }
    assert_eq!(got, expected, "非阻塞发射序列必须与阻塞式逐项一致");
    assert!(
        starved > 0,
        "idle 噪声下必须真实出现过 Starved（等数据），验证三态区分"
    );
    // Done 后再问一次：幂等终结，不会再吐出事件。
    assert!(
        matches!(nb.next_aligned_nonblocking().unwrap(), AlignPoll::Done),
        "全部耗尽后 Done 是吸收态"
    );
}

// ─── 验收 3：旧链路主循环对 SourceIdle 免疫（G5 旧行为不变） ─────────────

struct NoopPort;
impl StrategyPort for NoopPort {
    fn on_closed_bars(
        &mut self,
        _newly: &[Timeframe],
        _state: &AggregationState,
        _ctx: &DecisionContext,
    ) -> Vec<StrategyAction> {
        vec![StrategyAction::Hold]
    }
}

#[test]
fn exchange_runner_and_group_ignore_source_idle() {
    // ExchangeRunner（旧单标的主循环）：混入 SourceIdle 帧不聚合、不计数、
    // 不报错——bars_processed 只认真实闭合 bar。
    let ex = CtpExchange::new();
    let logical = ex.parse_instrument("CU").unwrap();
    let t0 = utc(2026, 1, 5, 1, 30);
    let mut b1 = bar(&logical, t0);
    b1.open = 50_000.0;
    b1.high = 50_000.0;
    b1.low = 50_000.0;
    b1.close = 50_000.0;
    let mut b2 = b1.clone();
    b2.open_time = t0 + Duration::minutes(1);
    let source = VecEventSource::new(vec![
        MarketEvent::Bar(b1),
        MarketEvent::SourceIdle,
        MarketEvent::Bar(b2),
        MarketEvent::SourceIdle,
    ]);
    let mut parts = ex
        .build(&SourceRequest::backtest(logical.clone(), vec![], 100_000.0))
        .unwrap();
    parts.source = Box::new(source);
    let mut runner = ExchangeRunner::new(parts, vec![Timeframe::M1], 200);
    let summary = runner.run(&mut NoopPort).unwrap();
    assert_eq!(
        summary.bars_processed, 2,
        "SourceIdle 帧不进聚合/计数（旧链路零行为变化）"
    );
    // CrossSectionalRunnerGroup 侧的同一语义由
    // `strategy::runner::tests::source_idle_and_reconnected_are_ignored_without_round`
    // 承重（构造完整 slot 组放单元测试内联更贴近 fixture）。
}

// ─── 验收 4：LiveExecution 本地镜像权益（PR-5 T2） ───────────────────────

fn binance_mirror_exec(cash: f64, tag: &str) -> (LiveExecution, InstrumentId) {
    let dir = tempfile::tempdir().unwrap();
    let ex = BinanceExchange::new(0.0004);
    let inst = ex.parse_instrument("BTCUSDT").unwrap();
    let manager = SignalManager::new(
        tag,
        Arc::new(Store::new(dir.path(), Arc::new(EpochClock))),
        None,
    );
    let session = ex.session_rules(&inst);
    let exec = binance_live_execution(
        inst.clone(),
        "xsec_momentum",
        manager,
        ex.trading_rules(),
        ex.position_rules(),
        session,
        cash,
    );
    (exec, inst)
}

fn order_at(t: DateTime<Utc>, td: TradingDay) -> OrderContext {
    OrderContext {
        at: t,
        trigger_price: 100.0,
        reference_bar: None,
        trading_day: td,
    }
}

fn live_update(
    order_id: &str,
    signal_id: &str,
    size: f64,
    price: f64,
    fee: f64,
    at: DateTime<Utc>,
) -> LiveOrderUpdate {
    LiveOrderUpdate {
        order_id: order_id.into(),
        exchange_order_id: None,
        state: CoreOrderState::Filled,
        filled_size: size,
        avg_price: price,
        filled_quantity_raw: Some(format!("{size}")),
        signal_id: Some(signal_id.into()),
        message: None,
        fee: Some(fee),
        at,
    }
}

/// 计划期望的终值算术勘误：开仓 fee 0.1 后 cash=999.9；平仓 realized=10、
/// fee=0.1 → cash = 999.9 + 10 - 0.1 = **1009.8**（计划文本 1019.8 系把
/// 盯市浮盈与已实现 pnl 重复计入）。本测试按守恒口径断言 1009.8。
#[test]
fn live_mirror_equity_tracks_fills_and_marks() {
    let (mut exec, inst) = binance_mirror_exec(1000.0, "mirror_nav");
    let ex = BinanceExchange::new(0.0004);
    let t = utc(2026, 1, 5, 1, 0);
    let td = ex.session_rules(&inst).trading_day_of(t);
    let ctx = order_at(t, td);

    // 开仓 1@100 fee=0.1：Open 不扣现金（保证金模型），fee 扣镜像现金。
    let open = OrderRequest {
        signal_id: "sig_open_m1".into(),
        instrument: inst.clone(),
        side: Side::Long,
        offset: Offset::Open,
        price: 100.0,
        qty: 1.0,
        close_leg: None,
        reduce_only: false,
        is_stop_loss: false,
        qty_raw: None,
        strategy_id: None,
        intent: None,
    };
    let ack = exec.submit(&open, &ctx).unwrap();
    assert!(ack.accepted, "开仓必须进信号管道");
    let reqs = exec.take_submit_requests();
    exec.apply_submit_outcome(reqs.into_iter().next().unwrap(), Ok("ord_m1".into()))
        .unwrap();
    exec.on_order_update(&live_update(
        "ord_m1",
        "sig_open_m1",
        1.0,
        100.0,
        0.1,
        t + Duration::minutes(1),
    ))
    .unwrap()
    .expect("开仓 Filled 产出 Fill");

    let acc = exec
        .account_snapshot()
        .expect("镜像权益恒 Some（不返回 None）");
    let spec_currency = ex.trading_rules().spec(&inst).unwrap().currency.clone();
    assert_eq!(acc.currency, spec_currency, "currency 与规则表同源");
    assert_eq!(acc.initial_cash, 1000.0);
    assert!((acc.cash - 999.9).abs() < 1e-9, "cash = 1000 - 0.1(fee)");
    assert!(acc.unrealized_pnl.abs() < 1e-9, "入场价盯市浮盈=0");
    assert!((acc.equity() - 999.9).abs() < 1e-9);

    // 1m 收盘 110 盯市：unrealized=(110-100)*1，equity=999.9+10。
    let mut mark = bar(&inst, t + Duration::minutes(1));
    mark.close = 110.0;
    exec.on_bar(&mark);
    let acc = exec.account_snapshot().unwrap();
    assert!((acc.unrealized_pnl - 10.0).abs() < 1e-9, "镜像 book 盯市");
    assert!((acc.equity() - 1009.9).abs() < 1e-9);

    // 平仓 1@110（腿入场价 100 → realized=10）fee=0.1：
    // cash = 999.9 + 10 - 0.1；book 清空 → unrealized=0。
    let leg = CloseLeg {
        offset: Offset::Auto,
        qty: 1.0,
        lot_ids: vec![],
        side: Side::Short,
        entry_price: 100.0,
        qty_raw: None,
    };
    let close = OrderRequest {
        signal_id: "sig_close_m1".into(),
        instrument: inst.clone(),
        side: Side::Short,
        offset: Offset::Close,
        price: 110.0,
        qty: 1.0,
        close_leg: Some(leg),
        reduce_only: true,
        is_stop_loss: false,
        qty_raw: None,
        strategy_id: None,
        intent: None,
    };
    let ack = exec.submit(&close, &ctx).unwrap();
    assert!(ack.accepted, "平仓必须进信号管道");
    let reqs = exec.take_submit_requests();
    exec.apply_submit_outcome(reqs.into_iter().next().unwrap(), Ok("ord_m2".into()))
        .unwrap();
    exec.on_order_update(&live_update(
        "ord_m2",
        "sig_close_m1",
        1.0,
        110.0,
        0.1,
        t + Duration::minutes(2),
    ))
    .unwrap()
    .expect("平仓 Filled 产出 Fill");

    let acc = exec.account_snapshot().unwrap();
    assert!(
        (acc.realized_pnl - 10.0).abs() < 1e-9,
        "平仓按腿记 realized"
    );
    assert!((acc.total_fee - 0.2).abs() < 1e-9);
    assert!((acc.cash - 1009.8).abs() < 1e-9, "999.9 + 10 - 0.1");
    assert!(acc.unrealized_pnl.abs() < 1e-9, "平仓后镜像 book 清空");
    assert!((acc.equity() - 1009.8).abs() < 1e-9);
}

#[test]
fn live_mirror_starts_none_free_when_no_fills() {
    // 无成交：account_snapshot()==Some(equity==initial_cash)；不 panic。
    let (exec, _inst) = binance_mirror_exec(1000.0, "mirror_empty");
    let acc = exec.account_snapshot().expect("镜像权益恒 Some");
    assert_eq!(acc.initial_cash, 1000.0);
    assert!((acc.cash - 1000.0).abs() < 1e-9);
    assert!(acc.unrealized_pnl.abs() < 1e-9, "无 bar 不伪造 mark");
    assert!((acc.equity() - 1000.0).abs() < 1e-9);
}
