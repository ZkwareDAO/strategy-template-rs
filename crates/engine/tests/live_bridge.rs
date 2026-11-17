//! 阶段 4（v2.7，§13-4）契约验收：`ExecutionPort → (SignalManager +
//! AsyncOrderGateway)` 桥 + live 端口工厂 + `ChannelBarSource`。
//!
//! 对齐三条验收（§13 阶段 4 行）：
//! 1. **实盘订单帧带平今/平昨**——本文件证明桥侧：CTP 装配
//!    `SignalIntent.offset = "close_today"` 标签；帧侧（wire 出帧）由
//!    `crates/broker/tests/live_offset_fee_frames.rs` 承重；
//! 2. **FillEvent.fee 不再恒 0**——回报 fee 经 `handle_order_update_fee`
//!    落到 SPI `Fill.fee`（本文件）与 `FillEvent.fee`（signals 回归）；
//! 3. **契约测试校验回测/实盘费率同源**——`LiveExecution::expected_fee`
//!    与 `SimulationExecution` 用同一个 `Arc<dyn TradingRules>` 出数
//!    （本文件），paper/旧链路第三路在 `app/tests/spi_live_fee_source.rs`。
//!
//! §14-13 契约点：`Fill.qty_raw` **必须**以回报 `filled_quantity` 原样入账。
//! 本文件为阶段 4 新建验收资产：全部断言只指向验收条款，无任何既有
//! 断言被触碰。

use chrono::{DateTime, TimeZone, Utc};
use std::sync::mpsc;
use std::sync::Arc;
use strategy_core::candle::Candle;
use strategy_core::order::OrderState as CoreOrderState;
use strategy_core::signal::Side;
use strategy_engine::core::execution::{ExecutionPort, OrderContext};
use strategy_engine::core::subscription::{RollEvent, RollPolicy};
use strategy_engine::exchanges::binance::BinanceExchange;
use strategy_engine::exchanges::ctp::CtpExchange;
use strategy_engine::prelude::*;
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

fn store(dir: &std::path::Path) -> Arc<Store> {
    Arc::new(Store::new(dir, Arc::new(EpochClock)))
}

fn event_bar(ev: &MarketEvent) -> Option<&Bar> {
    match ev {
        MarketEvent::Bar(b) => Some(b),
        _ => None,
    }
}

/// CTP 桥实例：CU2601（CtpExchange::new 真实规格：multiplier 5 /
/// open=close=0.00005 / close_today=0.0001）。
struct CtpBridge {
    inst: InstrumentId,
    /// 交易日口径副本（测试自用；LiveExecution 内部持同一交易所的 Arc）
    session: Arc<WindowedSession>,
    exec: LiveExecution,
}

fn ctp_bridge(dir: &std::path::Path, name: &str) -> CtpBridge {
    let ex = CtpExchange::new();
    let inst = ex.parse_instrument("CU2601").unwrap();
    let manager = SignalManager::new(name, store(dir), None);
    let exec = ctp_live_execution(
        inst.clone(),
        "sar_test",
        manager,
        ex.trading_rules(),
        ex.position_rules(),
        ex.session_rules(&inst),
        100_000.0,
    );
    CtpBridge {
        inst,
        session: Arc::new(cn_futures_default()),
        exec,
    }
}

fn ctx_at(session: &Arc<WindowedSession>, t: DateTime<Utc>, price: f64) -> OrderContext {
    OrderContext {
        at: t,
        trigger_price: price,
        reference_bar: None,
        trading_day: session.trading_day_of(t),
    }
}

/// 验收 1+2 的桥侧主案：开 → 平今，逐字段核对意图翻译、fee 回流、
/// qty_raw 契约（§14-13）、镜像账本。
#[test]
fn ctp_open_then_close_today_intent_fee_and_raw_chain() {
    let dir = tempfile::tempdir().unwrap();
    let mut b = ctp_bridge(dir.path(), "ctp_chain");
    // 2026-01-05（周一）09:06 北京 = 01:06 UTC，日盘可交易
    let t_open = utc(2026, 1, 5, 1, 6);
    let ctx = ctx_at(&b.session, t_open, 50_000.0);

    // ── 开仓：runner 已按 MarginLots 定出 2 手（桥不改数量，只翻译）──
    let open = OrderRequest {
        signal_id: "sig_open_1".into(),
        instrument: b.inst.clone(),
        side: Side::Long,
        offset: Offset::Open,
        price: 50_000.0,
        qty: 2.0,
        close_leg: None,
        reduce_only: false,
        is_stop_loss: false,
        qty_raw: None,
        strategy_id: None,
        intent: None,
    };
    let ack = b.exec.submit(&open, &ctx).unwrap();
    assert!(ack.accepted, "开仓必须进信号管道");
    let reqs = b.exec.take_submit_requests();
    assert_eq!(reqs.len(), 1);
    let intent = &reqs[0].intent;
    assert_eq!(intent.symbol, "CU2601", "wire 标的 = 裸 symbol");
    assert_eq!(intent.offset.as_deref(), Some("open"), "CTP 帧带显式开平");
    assert_eq!(intent.size, 0.0, "开仓 order_cash 换算路径不变（§13 过渡）");
    assert!(intent.size_raw.is_none());
    assert_eq!(
        intent.created_event_time, t_open,
        "事件时间=bar 时间禁 now()"
    );

    // ack 回执
    b.exec
        .apply_submit_outcome(reqs.into_iter().next().unwrap(), Ok("ord_1".into()))
        .unwrap();
    let snap = b.exec.query("ord_1").unwrap();
    assert_eq!(snap.state, OrderState::Pending);

    // 开仓成交回报：qty_raw 必须逐字取回报（§14-13），fee=1.0（开仓费率）
    let fill = b
        .exec
        .on_order_update(&LiveOrderUpdate {
            order_id: "ord_1".into(),
            exchange_order_id: Some("exch_1".into()),
            state: CoreOrderState::Filled,
            filled_size: 2.0,
            avg_price: 50_010.0,
            filled_quantity_raw: Some("2".into()),
            signal_id: Some("sig_open_1".into()),
            message: None,
            fee: Some(1.0),
            at: utc(2026, 1, 5, 1, 7),
        })
        .unwrap()
        .expect("Filled 必产出 Fill");
    assert_eq!(fill.fee, 1.0, "验收 2：live 手续费 = 回报真值，不再恒 0");
    assert_eq!(fill.qty_raw.as_deref(), Some("2"), "§14-13 回报字面量原样");
    assert_eq!(fill.offset, Offset::Open);
    assert_eq!(fill.trading_day.0.to_string(), "2026-01-05");
    // 镜像账本：今仓 2 手 @50_010
    let lots = b.exec.remote_positions(&b.inst).unwrap();
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].remaining, 2.0);
    assert!(lots[0].is_today);
    assert_eq!(lots[0].remaining_raw.as_deref(), Some("2"));

    // ── 平今：经 PositionRules 真实拆腿（close_today 标签的源头）──
    let rules = CtpPositionRules::default();
    let mut book = PositionBook::new(b.inst.clone(), rules.mode());
    book.lots = lots;
    let legs = rules
        .split_close(
            &book,
            &CloseRequest {
                instrument: b.inst.clone(),
                qty: 2.0,
                at: utc(2026, 1, 5, 2, 0),
            },
            b.session.as_ref(),
        )
        .unwrap();
    assert_eq!(legs[0].offset, Offset::CloseToday);
    let close = OrderRequest {
        signal_id: "sig_close_1".into(),
        instrument: b.inst.clone(),
        side: Side::Short,
        offset: Offset::CloseToday,
        price: 51_000.0,
        qty: 2.0,
        close_leg: Some(legs[0].clone()),
        reduce_only: true,
        is_stop_loss: false,
        qty_raw: legs[0].qty_raw.clone(),
        strategy_id: None,
        intent: None,
    };
    b.exec.submit(&close, &ctx).unwrap();
    let reqs = b.exec.take_submit_requests();
    let intent = &reqs[0].intent;
    assert_eq!(
        intent.offset.as_deref(),
        Some("close_today"),
        "验收 1 桥侧：平今标签随意图进网关"
    );
    assert_eq!(intent.size, 2.0);
    assert_eq!(intent.size_raw.as_deref(), Some("2"), "平仓帧数量真源=raw");
    assert!(intent.position_id.is_some(), "平仓意图必须关联批次");

    // 平今成交：费率 0.0001（平今加倍），fee 真值回流
    let t_close = utc(2026, 1, 5, 2, 0);
    let expected = b.exec.expected_fee(&close).unwrap();
    assert!(
        (expected - 51_000.0 * 2.0 * 5.0 * 0.0001).abs() < 1e-9,
        "expected_fee 走 TradingRules 平今费率，实际 {expected}"
    );
    let fill = b
        .exec
        .on_order_update(&LiveOrderUpdate {
            order_id: "ord_2".into(),
            exchange_order_id: None,
            state: CoreOrderState::Filled,
            filled_size: 2.0,
            avg_price: 51_000.0,
            filled_quantity_raw: Some("2".into()),
            signal_id: Some("sig_close_1".into()),
            message: None,
            fee: Some(expected),
            at: t_close,
        })
        .unwrap()
        .expect("平仓 Filled 产出 Fill");
    assert_eq!(fill.offset, Offset::CloseToday);
    assert_eq!(fill.fee, expected, "回报 fee 与规则同源值一致");
    // 镜像账本清仓
    let lots = b.exec.remote_positions(&b.inst).unwrap();
    assert!(lots.iter().all(|l| l.remaining <= f64::EPSILON));
}

/// 重启回退路径：桥内存态（orders 映射）丢失后，回报仍能经
/// Store 行（`SignalIntent.offset` 持久化）重建 offset/qty_raw——
/// §13 过渡口径「字符串进现有字段」在恢复链路上的兑现。
#[test]
fn restart_fallback_rebuilds_offset_from_stored_intent() {
    let dir = tempfile::tempdir().unwrap();
    let mut b = ctp_bridge(dir.path(), "ctp_restart");
    let t = utc(2026, 1, 5, 1, 6);
    let ctx = ctx_at(&b.session, t, 50_000.0);
    let close = OrderRequest {
        signal_id: "sig_close_ct".into(),
        instrument: b.inst.clone(),
        side: Side::Short,
        offset: Offset::CloseToday,
        price: 50_000.0,
        qty: 3.0,
        close_leg: None,
        reduce_only: true,
        is_stop_loss: false,
        qty_raw: Some("3".into()),
        strategy_id: None,
        intent: None,
    };
    b.exec.submit(&close, &ctx).unwrap();
    let reqs = b.exec.take_submit_requests();
    b.exec
        .apply_submit_outcome(reqs.into_iter().next().unwrap(), Ok("ord_r".into()))
        .unwrap();
    // 落盘（重启前快照）
    b.exec.manager().store().flush().unwrap();

    // 模拟进程重启：同目录新 manager + 新桥，recover 读回行
    let mut recovered = ctp_bridge(dir.path(), "ctp_restart");
    recovered.exec.manager_mut().recover().unwrap();
    let fill = recovered
        .exec
        .on_order_update(&LiveOrderUpdate {
            order_id: "ord_r".into(),
            exchange_order_id: None,
            state: CoreOrderState::Filled,
            filled_size: 3.0,
            avg_price: 50_000.0,
            filled_quantity_raw: Some("3".into()),
            signal_id: Some("sig_close_ct".into()),
            message: None,
            fee: Some(2.5),
            at: utc(2026, 1, 5, 1, 10),
        })
        .unwrap()
        .expect("恢复路径必须消费成交");
    assert_eq!(
        fill.offset,
        Offset::CloseToday,
        "重启后 offset 从持久化意图标签回重建"
    );
    assert_eq!(fill.qty_raw.as_deref(), Some("3"));
    assert_eq!(fill.fee, 2.5, "恢复路径 fee 同样真值回流");
}

/// 内核拒收不静默：重复 signal_id → ack.accepted=false（可诊断原因）。
#[test]
fn duplicate_signal_id_rejected_not_swallowed() {
    let dir = tempfile::tempdir().unwrap();
    let mut b = ctp_bridge(dir.path(), "ctp_dup");
    let t = utc(2026, 1, 5, 1, 6);
    let ctx = ctx_at(&b.session, t, 50_000.0);
    let open = OrderRequest {
        signal_id: "sig_same".into(),
        instrument: b.inst.clone(),
        side: Side::Long,
        offset: Offset::Open,
        price: 50_000.0,
        qty: 1.0,
        close_leg: None,
        reduce_only: false,
        is_stop_loss: false,
        qty_raw: None,
        strategy_id: None,
        intent: None,
    };
    assert!(b.exec.submit(&open, &ctx).unwrap().accepted);
    let ack2 = b.exec.submit(&open, &ctx).unwrap();
    assert!(!ack2.accepted, "重复 id 必须显式拒，不得静默入管道");
    assert_eq!(b.exec.take_submit_requests().len(), 1, "只放行一次");
}

/// 测试工厂：live 端口从注入工厂出，回测端口走 trait 缺省。
struct TestLiveFactory {
    dir: std::path::PathBuf,
}
impl LivePortFactory for TestLiveFactory {
    fn make_source(&self, req: &SourceRequest) -> Result<Box<dyn BarEventSource>> {
        let (_tx, rx) = mpsc::channel::<Candle>();
        Ok(Box::new(ctp_live_source(req.instrument.clone(), rx)))
    }
    fn make_execution(&self, req: &SourceRequest) -> Result<Box<dyn ExecutionPort>> {
        let ex = CtpExchange::new();
        let manager = SignalManager::new("test_live", store(&self.dir), None);
        Ok(Box::new(ctp_live_execution(
            req.instrument.clone(),
            "sar_test",
            manager,
            ex.trading_rules(),
            ex.position_rules(),
            ex.session_rules(&req.instrument),
            req.initial_cash,
        )))
    }
}

/// 首公理在 live 装配下成立：两次 build（live + backtest）规则同批 Arc；
/// 端口才分叉（§3 公理 2 的对称面）。未注入工厂的 Live 装配必须报错。
#[test]
fn live_build_shares_rule_arcs_with_backtest() {
    let dir = tempfile::tempdir().unwrap();
    let ex = CtpExchange::new().with_live_factory(Arc::new(TestLiveFactory {
        dir: dir.path().to_path_buf(),
    }));
    let inst = ex.parse_instrument("CU2601").unwrap();

    // 未注入工厂时 Live 装配必须报错（错误不吞；Parts 不可 Debug 走 match）
    let bare = CtpExchange::new();
    let mut live_req = SourceRequest::backtest(inst.clone(), vec![], 100_000.0);
    live_req.mode = RunMode::Live;
    let err = match bare.build(&live_req) {
        Ok(_) => panic!("未注入 live 工厂的 Live 装配必须报错"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("live 端口工厂"), "实际: {err}");

    let live = ex.build(&live_req).expect("工厂注入后 live 装配成功");
    let bt = ex
        .build(&SourceRequest::backtest(inst.clone(), vec![], 100_000.0))
        .unwrap();
    assert!(Arc::ptr_eq(&live.trading, &bt.trading));
    assert!(Arc::ptr_eq(&live.session, &bt.session));
    assert!(Arc::ptr_eq(&live.position, &bt.position));
    assert_eq!(live.mode, RunMode::Live);
    drop(live);
    // 回测端装配不受工厂影响（缺省路径零回归）
    let bt_only = SourceRequest::backtest(inst.clone(), vec![bar_head(&inst)], 100_000.0);
    let p = ex.build(&bt_only).unwrap();
    let mut runner = ExchangeRunner::new(p, vec![Timeframe::M1], 200);
    let summary = runner.run(&mut NoopPort).unwrap();
    assert_eq!(summary.bars_processed, 1);

    // 原则 1：同一条首公理断言，两家都必须立住
    let bex = BinanceExchange::new(0.0004).with_live_factory(Arc::new(TestLiveFactory {
        dir: dir.path().to_path_buf(),
    }));
    let binst = bex.parse_instrument("BTCUSDT").unwrap();
    let mut bl = SourceRequest::backtest(binst.clone(), vec![], 10_000.0);
    bl.mode = RunMode::Live;
    let lp = bex.build(&bl).unwrap();
    let bp = bex
        .build(&SourceRequest::backtest(binst, vec![], 10_000.0))
        .unwrap();
    assert!(Arc::ptr_eq(&lp.trading, &bp.trading));
    assert!(Arc::ptr_eq(&lp.session, &bp.session));
    assert!(Arc::ptr_eq(&lp.position, &bp.position));
}

fn bar_head(inst: &InstrumentId) -> Bar {
    Bar {
        instrument: inst.clone(),
        open_time: utc(2026, 1, 5, 1, 30),
        open: 50_000.0,
        high: 50_000.0,
        low: 50_000.0,
        close: 50_000.0,
        volume: 1.0,
        finality: BarFinality::Final,
        extensions: BarExtensions::default(),
    }
}

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

/// 原则 1 对照：crypto 装配不发明今昨标签（emit_offset=false），
/// 平 = `close`，净仓单账本；与 CTP 同一套桥代码。
#[test]
fn binance_bridge_keeps_reduce_only_implicit_no_offset_label() {
    let dir = tempfile::tempdir().unwrap();
    let ex = BinanceExchange::new(0.0004);
    let inst = ex.parse_instrument("BTCUSDT").unwrap();
    let manager = SignalManager::new("btc_live", store(dir.path()), None);
    let mut exec = binance_live_execution(
        inst.clone(),
        "sar_test",
        manager,
        ex.trading_rules(),
        ex.position_rules(),
        ex.session_rules(&inst),
        100_000.0,
    );
    assert!(!exec.emit_offset());
    let t = utc(2026, 1, 5, 1, 0);
    let ctx = OrderContext {
        at: t,
        trigger_price: 60_000.0,
        reference_bar: None,
        trading_day: ex.session_rules(&inst).trading_day_of(t),
    };
    let open = OrderRequest {
        signal_id: "sig_btc_open".into(),
        instrument: inst.clone(),
        side: Side::Long,
        offset: Offset::Open,
        price: 60_000.0,
        qty: 0.01,
        close_leg: None,
        reduce_only: false,
        is_stop_loss: false,
        qty_raw: None,
        strategy_id: None,
        intent: None,
    };
    exec.submit(&open, &ctx).unwrap();
    let reqs = exec.take_submit_requests();
    assert_eq!(reqs[0].intent.offset, None, "crypto 不发 offset 字段");
    // 止损平仓：is_stop_loss 经 stop_order_id 自标记位传递（不新增字段）
    let stop_close = OrderRequest {
        signal_id: "sig_btc_stop".into(),
        instrument: inst.clone(),
        side: Side::Short,
        offset: Offset::Close,
        price: 54_000.0,
        qty: 0.01,
        close_leg: None,
        reduce_only: true,
        is_stop_loss: true,
        qty_raw: Some("0.01".into()),
        strategy_id: None,
        intent: None,
    };
    exec.submit(&stop_close, &ctx).unwrap();
    let reqs = exec.take_submit_requests();
    let it = &reqs[0].intent;
    assert_eq!(it.offset, None);
    assert_eq!(it.stop_order_id.as_deref(), Some("sig_btc_stop"));
    assert_eq!(it.size_raw.as_deref(), Some("0.01"), "小数数量逐字进帧");
}

/// `ChannelBarSource`：闭合 1m 进主循环、未闭合丢弃、水位幂等、
/// 断流收尾——§14-2「Actor 邮箱桥接」形状。
#[test]
fn channel_source_maps_closed_drops_stale_and_stops() {
    let inst = InstrumentId::futures_contract(
        &ExchangeId::new("SHFE"),
        "CU",
        ContractMonth {
            year: 2026,
            month: 1,
        },
    );
    let (tx, rx) = mpsc::channel::<Candle>();
    let mut src = ctp_live_source(inst.clone(), rx);
    let t1 = utc(2026, 1, 5, 1, 30);
    let mut tick = candle(t1, 100.0);
    tick.is_closed = false;
    tx.send(tick).unwrap(); // 未闭合：丢弃
    tx.send(candle(t1, 100.0)).unwrap();
    tx.send(candle(t1, 100.0)).unwrap(); // 同水位：幂等丢弃
    tx.send(candle(utc(2026, 1, 5, 1, 29), 99.0)).unwrap(); // 迟到旧帧：丢弃
    tx.send(candle(utc(2026, 1, 5, 1, 31), 101.0)).unwrap();
    drop(tx); // 断流

    let ev = src.next_event().unwrap().expect("第一根闭合");
    assert_eq!(event_bar(&ev).unwrap().open_time, t1);
    assert_eq!(src.watermark(), Some(t1));
    let ev2 = src.next_event().unwrap().expect("第二根闭合");
    assert_eq!(event_bar(&ev2).unwrap().open_time, utc(2026, 1, 5, 1, 31));
    assert!(src.next_event().unwrap().is_none(), "断流收尾 Ok(None)");
}

/// C5 遗留出口（v2.7）：`ExchangeRunner::subscriptions()` 可读——
/// Roll 事件流经主循环不 panic、订阅集回写后仍从此出口可见
/// （live actor 重订阅差集的数据面；direct 计划映射为不变集）。
#[test]
fn roll_event_leaves_subscriptions_visible_via_accessor() {
    let ex = CtpExchange::new();
    let logical = ex.parse_instrument("CU").unwrap();
    let b1 = bar_head(&logical);
    let mut b2 = bar_head(&logical);
    b2.open_time = utc(2026, 1, 5, 1, 31);
    let roll = MarketEvent::Roll(RollEvent {
        trading_day: TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
        effective_at: utc(2026, 1, 5, 1, 30),
        from: logical.clone(),
        to: logical.clone(),
        policy: RollPolicy::External,
    });
    let source = VecEventSource::new(vec![MarketEvent::Bar(b1), roll, MarketEvent::Bar(b2)]);
    let mut parts = ex
        .build(&SourceRequest::backtest(logical.clone(), vec![], 100_000.0))
        .unwrap();
    parts.source = Box::new(source);
    let mut runner = ExchangeRunner::new(parts, vec![Timeframe::M1], 200);
    assert_eq!(runner.subscriptions().len(), 1, "装配期订阅集可读");
    let summary = runner.run(&mut NoopPort).unwrap();
    assert_eq!(summary.rolls, 1, "Roll 事件被主循环消费");
    assert_eq!(runner.subscriptions().len(), 1, "回写后经出口仍可见");
}
