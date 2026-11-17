//! 阶段 4（v2.7，§13-4 验收 3）契约测试：**回测/实盘费率同源**。
//!
//! 同一个 `FeeSchedule`（真源：仓库 `config/contracts.yaml` 的 CU），
//! 四条出口必须给出同一个数：
//! 1. `TradingRules::fee`（engine 规则直查——单一口径的定义点）；
//! 2. `SimulationExecution` 回测 fill 的 fee（engine 回测后端）；
//! 3. `BacktestOrderClient` paper 内核 ReadyFill 的 fee（**旧链路**，
//!    `paper_trading` 实盘演练用的同一撮合内核）；
//! 4. `LiveExecution::expected_fee`（live 桥对账预言机——服务端回传
//!    fee 与此不符即对账差异，帧通道由
//!    `broker/tests/live_offset_fee_frames.rs` 承重）。
//!
//! 本文件为阶段 4 新建验收资产（用户指令「阶段4」）：断言只指向
//! 「同源」条款本身，不触碰任何既有测试；三源中任一实现漂移，这里必红。

use chrono::{TimeZone, Utc};
use std::path::Path;
use std::sync::Arc;
use strategy_broker::{BacktestBroker, BacktestConfig, BacktestOrderClient};
use strategy_core::contract::{ContractSpec, FeeMode};
use strategy_core::{IntentKind, Side, SignalIntent, SignalStatus};
use strategy_engine::exchanges::ctp::CtpExchange;
use strategy_engine::prelude::*;
use strategy_signals::gateway::OrderGateway;

fn cu_contract() -> ContractSpec {
    let cfg = strategy_app::config::load(Some(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config")
            .as_path(),
    ))
    .expect("仓库 config 加载");
    strategy_app::config::contract_for(&cfg.contracts, "CU2601")
        .cloned()
        .expect("contracts.yaml 必须含 CU")
}

fn utc(t: i64) -> chrono::DateTime<Utc> {
    // 秒级构造器只用于固定样本（2026-01-05 单测日）
    Utc.timestamp_opt(t, 0).unwrap()
}

/// 三源费率逐字段相等（开 / 平今各算一遍，同一 FeeSchedule 出数）。
#[test]
fn fee_three_way_same_source_rules_paper_live() {
    let c = cu_contract();
    let mult = c.multiplier;
    let (rate_open, rate_close_today) = match c.commission.mode {
        FeeMode::Rate => (
            c.commission.open,
            c.commission
                .close_today
                .expect("CU 平今加倍费率存在（本测试前提）"),
        ),
        FeeMode::PerLot => panic!("CU 应为 rate 模式（本测试前提）"),
    };

    // —— 源 1：engine 规则直查（同源定义点）——
    let ex = CtpExchange::new();
    let inst = ex.parse_instrument("CU2601").unwrap();
    let spec = spec_from_contract(&c);
    let rules = Arc::new(SpecDrivenTradingRules::new([(inst.clone(), spec)]));

    let price = 50_000.0;
    let qty = 2.0;
    let notional = price * qty * mult;

    let fee_req = |offset: Offset| FeeRequest {
        instrument: inst.clone(),
        price,
        qty,
        offset,
    };
    let f_open = rules.fee(&fee_req(Offset::Open)).unwrap();
    let f_ct = rules.fee(&fee_req(Offset::CloseToday)).unwrap();
    assert!(
        (f_open - notional * rate_open).abs() < 1e-9
            && (f_ct - notional * rate_close_today).abs() < 1e-9,
        "规则侧费率 = 名义 × 费率表：{f_open} vs {f_ct}"
    );

    // —— 源 4：live 桥对账预言机（注入同一 Arc）——
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(strategy_store::Store::new(
        dir.path(),
        Arc::new(strategy_store::EpochClock),
    ));
    let manager = strategy_signals::manager::SignalManager::new("fee_bridge", store, None);
    let session = ex.session_rules(&inst);
    let live_exec = ctp_live_execution(
        inst.clone(),
        "sar_test",
        manager,
        rules.clone(),
        ex.position_rules(),
        session.clone(),
        100_000.0,
    );
    let close_order = OrderRequest {
        signal_id: "sig_oracle".into(),
        instrument: inst.clone(),
        side: Side::Short,
        offset: Offset::CloseToday,
        price,
        qty,
        close_leg: None,
        reduce_only: true,
        is_stop_loss: false,
        qty_raw: Some("2".into()),
        strategy_id: None,
        intent: None,
    };
    assert_eq!(
        live_exec.expected_fee(&close_order).unwrap(),
        f_ct,
        "桥 expected_fee 与规则直查同数"
    );

    // —— 源 2：engine 回测后端（SimulationExecution 成交即 fee）——
    let mut sim = SimulationExecution::new(
        inst.clone(),
        1_000_000.0,
        rules.clone(),
        ex.position_rules(),
        session.clone(),
        Arc::new(SimpleFillModel::default()),
    );
    // 2026-01-05 01:06:00 UTC = 北京 09:06（日盘可交易）
    let t = utc(1_767_575_160);
    let ctx = OrderContext {
        at: t,
        trigger_price: price,
        reference_bar: None,
        trading_day: session.trading_day_of(t),
    };
    let open_order = OrderRequest {
        signal_id: "sim_open".into(),
        instrument: inst.clone(),
        side: Side::Long,
        offset: Offset::Open,
        price,
        qty,
        close_leg: None,
        reduce_only: false,
        is_stop_loss: false,
        qty_raw: None,
        strategy_id: None,
        intent: None,
    };
    sim.submit(&open_order, &ctx).unwrap();
    let pos_rules = CtpPositionRules::default();
    let mut book = PositionBook::new(inst.clone(), pos_rules.mode());
    book.lots = sim.book.lots.clone();
    let legs = pos_rules
        .split_close(
            &book,
            &CloseRequest {
                instrument: inst.clone(),
                qty,
                at: t,
            },
            session.as_ref(),
        )
        .unwrap();
    assert_eq!(legs[0].offset, Offset::CloseToday, "当日开当日平=平今");
    let sim_close = OrderRequest {
        signal_id: "sim_close".into(),
        instrument: inst.clone(),
        side: Side::Short,
        offset: Offset::CloseToday,
        price,
        qty,
        close_leg: Some(legs[0].clone()),
        reduce_only: true,
        is_stop_loss: false,
        qty_raw: legs[0].qty_raw.clone(),
        strategy_id: None,
        intent: None,
    };
    sim.submit(&sim_close, &ctx).unwrap();
    let sim_fills = sim.drain_fills();
    assert_eq!(sim_fills.len(), 2);
    assert!(
        (sim_fills[0].fee - f_open).abs() < 1e-9,
        "engine 回测开仓费 {} vs 规则 {f_open}",
        sim_fills[0].fee
    );
    assert!(
        (sim_fills[1].fee - f_ct).abs() < 1e-9,
        "engine 回测平今费 {} vs 规则 {f_ct}",
        sim_fills[1].fee
    );

    // —— 源 3：旧 paper 内核（BacktestOrderClient 当日开平=平今）——
    // 内核期货开仓走 sizing_lots=min(保证金上限手, 杠杆名义手)：
    // cash=500k、leverage=1 ⇒ 500k/(50000×5)=2 手，与规则侧 qty=2 同数
    let mut paper = BacktestOrderClient::new(BacktestBroker::new(
        BacktestConfig {
            cash: 500_000.0,
            leverage: 1,
            notional_per_order: notional,
            contract: Some(cu_contract()),
            exchange: "SHFE".into(),
            ..Default::default()
        },
        "fee_paper",
        dir.path(),
    ));
    let mk = |id: &str, kind: IntentKind, direction: Side| SignalIntent {
        id: id.into(),
        strategy_id: "sar_test".into(),
        symbol: "CU2601".into(),
        kind,
        direction,
        price,
        size: if kind == IntentKind::Open { 0.0 } else { qty },
        size_raw: None,
        stop_price: None,
        position_id: (kind != IntentKind::Open).then(|| "p1".to_string()),
        stop_order_id: None,
        created_event_time: t,
        status: SignalStatus::Created,
        offset: None, // 旧链路：内核按当日仓隐式判平今
    };
    paper
        .submit(&mk("pp_open", IntentKind::Open, Side::Long))
        .unwrap();
    paper
        .submit(&mk("pp_close", IntentKind::Close, Side::Short))
        .unwrap();
    let ready = paper.drain_ready_fills();
    assert_eq!(ready.len(), 2, "开平两笔即时成交");
    assert!(
        (ready[0].fee - f_open).abs() < 1e-9,
        "paper 开仓费 {} vs 规则 {f_open}",
        ready[0].fee
    );
    assert!(
        (ready[1].fee - f_ct).abs() < 1e-9,
        "paper 平今费（内核隐式判定今仓）{} vs 规则 {f_ct}",
        ready[1].fee
    );
}
