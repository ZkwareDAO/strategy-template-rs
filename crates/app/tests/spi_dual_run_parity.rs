//! 阶段 3 验收（§13 行 3 / §12-5/6/7）：
//! 旧 `Engine` 已退役（R4/D1），双跑对照失去宿主后，旧侧输出冻结为
//! 文本 golden（`tests/golden/retirement/`），engine 侧对 golden 断言——
//! 断言面不缩。逐笔对比成交价/数量/PnL（§13 验收四字段），并按 §12-6
//! 覆盖触价止损、冷却拦截、`UpdateStop`、`Reverse`、平今/平昨样本；
//! §12-7 聚合器逐桶 diff（M1/D1/夜盘 H1 一致，日盘 H1 钉差异基线）。
//!
//! 差异表（本文件显式钉死，非静默放过）：
//! 1. **crypto 平仓手续费基数**：旧 broker 按「开仓名义」计（`backtest.rs`
//!    兼容口径），engine 按「平仓成交名义」计（TradingRules 单一口径）。
//!    开仓费与期货平仓费逐笔相等；crypto 平仓费差 =
//!    (开仓价 − 平仓价) × qty × rate，逐笔断言。
//! 2. **signal_id 方案**：旧 = `generate_signal_id(平 symbol)`；engine =
//!    `sig_{namespaced}_{ts}_{seq}`（§13 过渡：命名空间字符串，阶段 5
//!    统一）。跨引擎不可比；本文件以「engine 确定性 + 重放逐项相等」
//!    承接该字段的验收语义。
//! 3. **日盘 H1 桶边界**：旧实现锚「交易时段窗口段」(9:00–10:15/…)；
//!    engine `SessionWindowPolicy` 锚「窗口内 tf 分钟网格」。夜盘 H1/D1/M1
//!    逐桶一致；日盘边界差异钉基线（§2.1 痛点修复的设计后果）。
//! 4. **持仓中重复 Open**：旧 broker 静默丢弃（单仓模型）；engine `PositionBook`
//!    支持加仓。双跑样本不开该路径（反手用 `Reverse`），差异登记 §16。
//! 5. `bar_to_candle` 桥接后大周期 `quote_volume` 取桶首根（旧取末根）——
//!    策略不消费（仅报表字段），登记。

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone, Timelike, Utc};

use strategy_app::report::TradeRow;
use strategy_app::spi_backtest::{candles_to_bars, SpiBacktest};
use strategy_core::candle::{Candle, Timeframe as CoreTf};
use strategy_core::contract::ContractSpec;
use strategy_core::risk::RiskControlConfig;
use strategy_core::signal::Side;
use strategy_engine::bridge::{bar_to_candle, spec_from_contract};
use strategy_engine::core::bar::Timeframe as EngineTimeframe;
use strategy_engine::core::execution::{Account, Fill};
use strategy_engine::exchanges::binance::BinanceExchange;
use strategy_engine::exchanges::ctp::CtpExchange;
use strategy_engine::prelude::*;
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};

// ────────────────────────────── 公共夹具 ──────────────────────────────

fn candle(t: DateTime<Utc>, price: f64) -> Candle {
    Candle {
        open_time: t,
        open: price,
        high: price + 0.5,
        low: price - 0.5,
        close: price,
        volume: 10.0,
        quote_volume: 0.0,
        trade_num: 0,
        active_buy_volume: 0.0,
        active_buy_quote_volume: 0.0,
        is_closed: true,
    }
}

fn futc(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, mi, s).unwrap()
}

fn cst() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).unwrap()
}

/// 北京时间 → UTC
fn beijing(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
    cst()
        .with_ymd_and_hms(y, m, d, h, mi, 0)
        .unwrap()
        .with_timezone(&Utc)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Event {
    OpenLong { stop: f64 },
    UpdateStop { stop: f64 },
    ReverseShort { stop: f64 },
    ClosePlain,
}

/// 生命周期策略：事件按 **1m 时间戳** 精确触发（同一份策略源码、
/// 同一张事件表 → 决策序列确定性，分叉只能来自引擎/结算层）。
/// 触价止损镜像生产策略口径（持仓且触价 ⇒ 止损平仓）。
struct Lifecycle {
    tfs: Vec<CoreTf>,
    events: BTreeMap<DateTime<Utc>, Event>,
}

impl CrossSectionalStrategyPort for Lifecycle {
    fn strategy_type(&self) -> &str {
        "parity"
    }
    fn timeframes(&self) -> Vec<EngineTimeframe> {
        self.tfs
            .iter()
            .copied()
            .map(EngineTimeframe::from)
            .collect()
    }
    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[EngineTimeframe],
        snapshots: &[InstrumentSnapshot],
        _portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        let Some(s) = snapshots.first() else {
            return vec![];
        };
        let event_time = s.bar.open_time;
        match s.position {
            None => {
                if let Some(Event::OpenLong { stop }) = self.events.get(&event_time) {
                    // 冷却拦截通道（C1）：策略自判
                    if s.in_stop_loss_cooldown() {
                        return vec![];
                    }
                    return vec![InstrumentAction::Open {
                        instrument: s.instrument.clone(),
                        side: Side::Long,
                        price: s.bar.close,
                        stop_price: *stop,
                        atr: 1.0,
                        strength: 0.8,
                        reason: "parity".into(),
                    }];
                }
                vec![]
            }
            Some(p) => {
                let price = s.bar.close;
                let hit = match p.side {
                    Side::Long => p.stop_price > 0.0 && price <= p.stop_price,
                    Side::Short => p.stop_price > 0.0 && price >= p.stop_price,
                };
                if hit {
                    return vec![InstrumentAction::Close {
                        instrument: s.instrument.clone(),
                        price,
                        reason: "sl".into(),
                        is_stop_loss: true,
                        strength: 1.0,
                    }];
                }
                match self.events.get(&event_time) {
                    Some(Event::UpdateStop { stop }) => vec![InstrumentAction::UpdateStop {
                        instrument: s.instrument.clone(),
                        stop_price: *stop,
                    }],
                    Some(Event::ReverseShort { stop }) => vec![InstrumentAction::Reverse {
                        instrument: s.instrument.clone(),
                        side: Side::Short,
                        price,
                        stop_price: *stop,
                        atr: 1.0,
                        strength: 0.8,
                        close_reason: "rev".into(),
                        is_stop_loss: false,
                    }],
                    Some(Event::ClosePlain) => vec![InstrumentAction::Close {
                        instrument: s.instrument.clone(),
                        price,
                        reason: "exit".into(),
                        is_stop_loss: false,
                        strength: 1.0,
                    }],
                    _ => vec![],
                }
            }
        }
    }
}

fn lifecycle(
    events: BTreeMap<DateTime<Utc>, Event>,
    tfs: Vec<CoreTf>,
) -> Box<dyn strategy_engine::strategy::port::CrossSectionalStrategyPort> {
    Box::new(Lifecycle { tfs, events })
}

/// engine 成交 → side 标签（BUY/SELL/SELL_CLOSE/BUY_CLOSE）
fn new_label(f: &Fill) -> &'static str {
    match (f.offset, f.side) {
        (Offset::Open, Side::Long) => "BUY",
        (Offset::Open, Side::Short) => "SELL",
        (_, Side::Long) => "BUY_CLOSE",
        (_, Side::Short) => "SELL_CLOSE",
    }
}

// ─────────────────────────── engine 后端 ───────────────────────────

fn spi_run(
    exchange: Arc<dyn Exchange>,
    inst: InstrumentId,
    bars: &[Candle],
    cash: f64,
    sizing: SizingParams,
    events: BTreeMap<DateTime<Utc>, Event>,
    tfs: Vec<CoreTf>,
) -> (RunSummary, Option<Account>) {
    let bar_vec = candles_to_bars(&inst, bars);
    let req = SourceRequest::backtest(inst, bar_vec, cash);
    let mut bt = SpiBacktest::assemble(
        &exchange,
        req,
        lifecycle(events, tfs),
        sizing,
        RiskControlConfig::default(),
        2000,
    )
    .expect("engine 装配");
    let summary = bt.run().expect("engine 主循环");
    (summary, bt.account())
}

// ────────────────────────── 逐笔对比断言 ──────────────────────────

/// §13 验收字段硬断言（成交价/手数/盈亏/时刻/方向）+ 手续费口径分支：
/// `close_basis_diff=true` 表示 crypto 平仓行（差异表 #1），差值由调用方钉公式。
fn assert_trade_pair(old: &TradeRow, new: &Fill, close_basis_diff: bool) {
    assert_eq!(
        old.timestamp,
        new.at.format("%Y-%m-%dT%H:%M:%S").to_string(),
        "成交时刻必须一致"
    );
    assert_eq!(old.side, new_label(new), "开平方向标签必须一致");
    assert!(
        (old.quantity - new.qty).abs() < 1e-9,
        "数量必须一致: golden={} new={}",
        old.quantity,
        new.qty
    );
    assert!(
        (old.price - new.price).abs() < 1e-9,
        "成交价必须一致: golden={} new={}",
        old.price,
        new.price
    );
    assert!(
        (old.pnl - new.realized_pnl).abs() < 1e-6,
        "PnL 必须一致: golden={} new={}",
        old.pnl,
        new.realized_pnl
    );
    if !close_basis_diff {
        assert!(
            (old.commission - new.fee).abs() < 1e-9,
            "开仓/期货成交手续费必须逐笔相等: golden={} new={}",
            old.commission,
            new.fee
        );
    }
}

fn crypto_exchange(inst: &InstrumentId) -> Arc<dyn Exchange> {
    Arc::new(
        BinanceExchange::named(ExchangeId::new("binance"), 0.0004)
            .with_symbol_spec(inst.clone(), InstrumentSpec::crypto_perpetual(0.0004)),
    ) as Arc<dyn Exchange>
}

// ════════════════════════ 测试 1：crypto 全生命周期 ════════════════════════

#[test]
fn crypto_lifecycle_dual_run_parity() {
    let base = futc(2026, 6, 10, 0, 0, 0);
    // 价格全取 200 的整/半除数（50/80/100/125/110/40），qty 落 1e-6 步长网格零损失
    let prices = [100.0, 125.0, 110.0, 100.0, 80.0, 100.0];
    let bars: Vec<Candle> = prices
        .iter()
        .enumerate()
        .map(|(i, p)| candle(base + chrono::Duration::minutes(i as i64), *p))
        .collect();
    let mut events: BTreeMap<DateTime<Utc>, Event> = BTreeMap::new();
    events.insert(base, Event::OpenLong { stop: 98.5 });
    // idx1 反手先于 UpdateStop（否则 103 止损会先于 110 价执行，样本失控）
    events.insert(
        base + chrono::Duration::minutes(1),
        Event::ReverseShort { stop: 135.0 },
    );
    events.insert(
        base + chrono::Duration::minutes(2),
        Event::UpdateStop { stop: 115.0 },
    );
    events.insert(base + chrono::Duration::minutes(3), Event::ClosePlain);
    events.insert(
        base + chrono::Duration::minutes(4),
        Event::OpenLong { stop: 78.0 },
    );
    events.insert(base + chrono::Duration::minutes(5), Event::ClosePlain);

    let inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
    let sizing = SizingParams {
        leverage: 1.0,
        notional_per_order: Some(200.0),
    };
    let (summary, account) = spi_run(
        crypto_exchange(&inst),
        inst.clone(),
        &bars,
        200.0,
        sizing,
        events,
        vec![CoreTf::M1],
    );

    // 6 笔：开多 / 反手平多 / 反手开空 / 平空 / 开多 / 平多
    assert_eq!(summary.fills.len(), 6, "engine 成交数");
    let rate = 0.0004f64;
    // 平仓行索引 → (开仓价, 平仓价)：费用差公式的输入
    let close_basis: BTreeMap<usize, (f64, f64)> =
        BTreeMap::from([(1, (100.0, 125.0)), (3, (125.0, 100.0)), (5, (80.0, 100.0))]);
    // golden（旧引擎冻结输出）vs engine 逐笔对比
    let g = golden_rows("crypto_lifecycle_old.csv");
    assert_eq!(g.len(), 6, "golden 行数须为 6");
    for (i, (gr, n)) in g.iter().zip(summary.fills.iter()).enumerate() {
        let is_close = close_basis.contains_key(&i);
        assert_trade_pair(gr, n, is_close);
        if let Some((entry, exit)) = close_basis.get(&i) {
            assert!(
                (gr.commission - entry * n.qty * rate).abs() < 1e-9,
                "差异表#1 golden 平仓费=开仓名义档 trade{i}"
            );
            assert!(
                (n.fee - exit * n.qty * rate).abs() < 1e-9,
                "差异表#1 engine 平仓费=平仓名义档 trade{i}"
            );
            assert!(
                (gr.commission - n.fee - (entry - exit) * n.qty * rate).abs() < 1e-9,
                "差异表#1 费用差恰等于两口径之差 trade{i}"
            );
        }
    }
    // 数量：2 / 1.6 / 2.5 精确（floor 无步长损失，探针已验证网格）
    assert!((g[0].quantity - 2.0).abs() < 1e-9);
    assert!((g[1].quantity - 2.0).abs() < 1e-9);
    for idx in [2, 3] {
        assert!(
            (summary.fills[idx].qty - 1.6).abs() < 1e-9,
            "trade{idx} qty"
        );
    }
    for idx in [4, 5] {
        assert!(
            (summary.fills[idx].qty - 2.5).abs() < 1e-9,
            "trade{idx} qty"
        );
    }
    // PnL 黄金值（golden 已与 engine 逐笔钉，此处再锚定公式）
    let pnls: Vec<f64> = g.iter().map(|t| t.pnl).collect();
    assert!((pnls[1] - 50.0).abs() < 1e-9, "反手平多 pnl");
    assert!((pnls[3] - 40.0).abs() < 1e-9, "平空 pnl");
    assert!((pnls[5] - 50.0).abs() < 1e-9, "平多 pnl");
    for i in [0, 2, 4] {
        assert!(pnls[i].abs() < 1e-12, "开仓 pnl 必须为 0");
    }

    // 终态：engine 权益恒等式 equity = cash（空仓）= 初始 + Σpnl − Σfee_new
    let acct = account.expect("账户快照");
    assert!(
        acct.unrealized_pnl.abs() < 1e-12,
        "终态空仓无浮盈（v2.6 成交后重盯市修复）"
    );
    let sum_pnl: f64 = summary.fills.iter().map(|f| f.realized_pnl).sum();
    let sum_fee: f64 = summary.fills.iter().map(|f| f.fee).sum();
    assert!(
        (acct.cash - (200.0 + sum_pnl - sum_fee)).abs() < 1e-9,
        "engine 现金恒等式"
    );
    assert!(summary.final_positions.is_empty(), "终态必须空仓");

    // 重放确定性（§12-2）：同数据再跑一遍，成交序列逐项相等
    let (again, _) = spi_run(
        crypto_exchange(&inst),
        inst,
        &bars,
        200.0,
        sizing,
        // 同一张事件表
        old_events_replay_marker(),
        vec![CoreTf::M1],
    );
    assert_eq!(again.fills, summary.fills, "engine 重放成交逐项相等");
}

/// 重放用事件表（与测试 1 相同时间锚：base = 2026-06-10 00:00 UTC）
fn old_events_replay_marker() -> BTreeMap<DateTime<Utc>, Event> {
    let base = futc(2026, 6, 10, 0, 0, 0);
    [
        (base, Event::OpenLong { stop: 98.5 }),
        (
            base + chrono::Duration::minutes(1),
            Event::ReverseShort { stop: 135.0 },
        ),
        (
            base + chrono::Duration::minutes(2),
            Event::UpdateStop { stop: 115.0 },
        ),
        (base + chrono::Duration::minutes(3), Event::ClosePlain),
        (
            base + chrono::Duration::minutes(4),
            Event::OpenLong { stop: 78.0 },
        ),
        (base + chrono::Duration::minutes(5), Event::ClosePlain),
    ]
    .into_iter()
    .collect()
}

// ═══════════════════════ 测试 2：crypto 止损 + 冷却拦截 ═══════════════════════

#[test]
fn crypto_stop_loss_cooldown_dual_run() {
    let base = futc(2026, 6, 10, 0, 0, 0);
    let prices = [100.0, 97.0, 100.0, 100.0];
    let bars: Vec<Candle> = prices
        .iter()
        .enumerate()
        .map(|(i, p)| candle(base + chrono::Duration::minutes(i as i64), *p))
        .collect();
    let mut events: BTreeMap<DateTime<Utc>, Event> = BTreeMap::new();
    events.insert(base, Event::OpenLong { stop: 98.5 });
    events.insert(
        base + chrono::Duration::minutes(2),
        Event::OpenLong { stop: 98.5 },
    );
    events.insert(
        base + chrono::Duration::minutes(3),
        Event::OpenLong { stop: 98.5 },
    );

    let inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
    let (summary, _) = spi_run(
        crypto_exchange(&inst),
        inst,
        &bars,
        200.0,
        SizingParams {
            leverage: 1.0,
            notional_per_order: Some(200.0),
        },
        events,
        vec![CoreTf::M1],
    );

    // 恰 2 笔：开多 + 止损平多；bar2/3 的再开仓被当日冷却拦截
    assert_eq!(
        summary.fills.len(),
        2,
        "engine 侧：冷却必须拦截 bar2/3 开仓"
    );
    assert!(summary.fills[1].is_stop_loss, "止损标志透传到 Fill");
    assert!(
        (summary.fills[1].realized_pnl - -6.0).abs() < 1e-9,
        "engine 止损 pnl 一致"
    );

    // golden（旧引擎冻结输出）vs engine 逐笔对比
    let g = golden_rows("crypto_cooldown_old.csv");
    assert_eq!(g.len(), 2, "golden 行数=2（开+止损平）");
    assert_trade_pair(&g[0], &summary.fills[0], false);
    assert_trade_pair(&g[1], &summary.fills[1], true);
}

// ═══════════════════ 测试 3：CTP 全生命周期（含平今/平昨） ═══════════════════

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

/// CU 交易时段连续 1m（06-02 周二起至 06-04 周四 21:00 后，含夜盘）
fn ctp_bars(price_at: &BTreeMap<DateTime<Utc>, f64>) -> Vec<Candle> {
    let mut out = Vec::new();
    for d in 0..3i64 {
        let day = NaiveDate::from_ymd_opt(2026, 6, 2 + d as u32).unwrap();
        let windows: [(u32, u32, u32, u32, bool); 4] = [
            (9, 0, 10, 15, false),
            (10, 30, 11, 30, false),
            (13, 30, 15, 0, false),
            (21, 0, 1, 0, true),
        ];
        for (oh, om, ch, cm, cross) in windows {
            let start = day.and_hms_opt(oh, om, 0).unwrap();
            let end = if cross {
                day.succ_opt().unwrap().and_hms_opt(ch, cm, 0).unwrap()
            } else {
                day.and_hms_opt(ch, cm, 0).unwrap()
            };
            let mut t = start;
            while t < end {
                let utc = cst().from_local_datetime(&t).unwrap().with_timezone(&Utc);
                let p = *price_at.get(&utc).unwrap_or(&78000.0);
                out.push(Candle {
                    open_time: utc,
                    open: p,
                    high: p + 5.0,
                    low: p - 5.0,
                    close: p,
                    volume: 10.0,
                    quote_volume: 0.0,
                    trade_num: 0,
                    active_buy_volume: 0.0,
                    active_buy_quote_volume: 0.0,
                    is_closed: true,
                });
                t += chrono::Duration::minutes(1);
            }
        }
    }
    out.sort_by_key(|c| c.open_time);
    out.dedup_by_key(|c| c.open_time);
    out
}

#[test]
fn ctp_lifecycle_dual_run_parity() {
    let contract = cu_contract();
    let mut prices: BTreeMap<DateTime<Utc>, f64> = BTreeMap::new();
    prices.insert(beijing(2026, 6, 2, 9, 10), 78040.0);
    prices.insert(beijing(2026, 6, 2, 9, 12), 77950.0);
    prices.insert(beijing(2026, 6, 2, 9, 15), 78010.0);
    prices.insert(beijing(2026, 6, 2, 9, 20), 76990.0);
    prices.insert(beijing(2026, 6, 4, 10, 31), 78120.0);
    let bars = ctp_bars(&prices);

    let mut events: BTreeMap<DateTime<Utc>, Event> = BTreeMap::new();
    // 09:00-09:05 禁交易窗（两侧同拦）→ 09:06 是窗口后第一根（signal_resume）
    events.insert(beijing(2026, 6, 2, 9, 6), Event::OpenLong { stop: 77000.0 });
    events.insert(
        beijing(2026, 6, 2, 9, 7),
        Event::UpdateStop { stop: 77900.0 },
    );
    events.insert(
        beijing(2026, 6, 2, 9, 10),
        Event::ReverseShort { stop: 79000.0 },
    );
    events.insert(beijing(2026, 6, 2, 9, 12), Event::ClosePlain);
    events.insert(
        beijing(2026, 6, 2, 9, 15),
        Event::OpenLong { stop: 77500.0 },
    );
    // 09:20 触价止损（76990 ≤ stop 77500）——on_realtime_tick 止损分支
    events.insert(
        beijing(2026, 6, 2, 9, 25),
        Event::OpenLong { stop: 77000.0 },
    ); // 冷却拦截
    events.insert(beijing(2026, 6, 3, 9, 6), Event::OpenLong { stop: 77000.0 });
    events.insert(beijing(2026, 6, 4, 10, 31), Event::ClosePlain);

    let inst = InstrumentId::futures_contract(
        &ExchangeId::new("SHFE"),
        "CU",
        ContractMonth {
            year: 2026,
            month: 1,
        },
    );
    let ex = Arc::new(
        CtpExchange::named(ExchangeId::new("SHFE"))
            .with_spec(inst.clone(), spec_from_contract(&contract)),
    ) as Arc<dyn Exchange>;
    let (summary, account) = spi_run(
        ex,
        inst,
        &bars,
        1_000_000.0,
        SizingParams::default(),
        events,
        vec![CoreTf::M1],
    );

    // 8 笔：开 / 反手平 / 反手开空 / 平空 / 开 / 止损平 / 隔日开 / 隔日平
    assert_eq!(summary.fills.len(), 8, "engine 成交数");

    // CTP 链路：四字段 + 手续费**全部**逐笔相等（§13 验收原文；期货两侧同基数口径）
    // golden（旧引擎冻结输出）vs engine 逐笔对比
    let g = golden_rows("ctp_lifecycle_old.csv");
    assert_eq!(g.len(), 8, "golden 行数须为 8");
    for (gr, n) in g.iter().zip(summary.fills.iter()) {
        assert_trade_pair(gr, n, false);
        assert_eq!(n.qty.fract(), 0.0, "期货必须整数手");
    }
    // 平今/平昨拆腿口径（强于逐笔费率的语义断言）：
    // trade1 反手平多（同交易日）= CloseToday；trade7 隔交易日平 = CloseYesterday
    assert_eq!(summary.fills[1].offset, Offset::CloseToday, "反手平多=平今");
    assert_eq!(summary.fills[3].offset, Offset::CloseToday, "当日平空=平今");
    assert_eq!(summary.fills[5].offset, Offset::CloseToday, "当日止损=平今");
    assert_eq!(
        summary.fills[7].offset,
        Offset::CloseYesterday,
        "周三开→周四平=平昨"
    );
    // 平今费率档 0.0001 / 平昨档 0.00005（公式锚定）
    let fee3 = 77950.0 * 5.0 * 2.0 * contract.commission.close_today.unwrap();
    assert!(
        (summary.fills[3].fee - fee3).abs() < 1e-6,
        "平今费按 close_today 档"
    );
    let fee7 = 78120.0 * 5.0 * 2.0 * contract.commission.close;
    assert!(
        (summary.fills[7].fee - fee7).abs() < 1e-6,
        "平昨费按 close 档"
    );
    // 手数：杠杆 1 名义上限 → 全部 2 手（整数）
    for n in &summary.fills {
        assert_eq!(n.qty, 2.0, "每笔 2 手（名义上限口径）");
    }

    // 终态现金恒等式（engine 自洽；空仓权益=现金）
    let acct = account.expect("账户");
    let sum_pnl: f64 = summary.fills.iter().map(|f| f.realized_pnl).sum();
    let sum_fee: f64 = summary.fills.iter().map(|f| f.fee).sum();
    assert!((acct.cash - (1_000_000.0 + sum_pnl - sum_fee)).abs() < 1e-6);
    assert!(acct.unrealized_pnl.abs() < 1e-12);
    assert!(summary.final_positions.is_empty());
}

// ═══════════════════ 测试 4：聚合器逐桶 diff（§12-7） ═══════════════════

/// 新旧聚合器在 CTP 历史 1m 上的桶边界/内容 diff。
/// 一致面：M1、D1（含 14:59 即时闭合口径）、夜盘 H1（21/22/23/00 锚）、
/// 缺 bar 惰性闭合、水位幂等。差异面：日盘 H1 桶锚（钉基线）。
#[test]
fn ctp_aggregator_bucket_diff() {
    // 数据：06-01(一) 夜盘 → 06-02(二) 全日盘+夜盘 → 06-03(三) 日盘至 15:00。
    // 夜盘起点让首 D1 桶对齐（避免 warmup 中途接入 tainted 分叉，C8 另有专项测试）。
    let mut bars: Vec<Candle> = Vec::new();
    let plan: [(NaiveDate, bool, bool); 3] = [
        (NaiveDate::from_ymd_opt(2026, 6, 1).unwrap(), false, true), // 只有夜盘
        (NaiveDate::from_ymd_opt(2026, 6, 2).unwrap(), true, true),  // 日盘+夜盘
        (NaiveDate::from_ymd_opt(2026, 6, 3).unwrap(), true, false), // 只日盘
    ];
    let mut k = 0usize;
    for (day, with_day, with_night) in plan {
        let mut windows: Vec<(u32, u32, u32, u32, bool)> = Vec::new();
        if with_day {
            windows.push((9, 0, 10, 15, false));
            windows.push((10, 30, 11, 30, false));
            windows.push((13, 30, 15, 0, false));
        }
        if with_night {
            windows.push((21, 0, 1, 0, true));
        }
        for (oh, om, ch, cm, cross) in windows {
            let start = day.and_hms_opt(oh, om, 0).unwrap();
            let end = if cross {
                day.succ_opt().unwrap().and_hms_opt(ch, cm, 0).unwrap()
            } else {
                day.and_hms_opt(ch, cm, 0).unwrap()
            };
            let mut t = start;
            while t < end {
                let utc = cst().from_local_datetime(&t).unwrap().with_timezone(&Utc);
                // 不规则小数：测 OHLC 逐字段
                let p = 78_000.0 + (k % 17) as f64 * 3.5 - 8.0;
                k += 1;
                bars.push(Candle {
                    open_time: utc,
                    open: p,
                    high: p + 2.5,
                    low: p - 1.5,
                    close: p + 0.5,
                    volume: 5.0 + (k % 3) as f64,
                    quote_volume: 0.0,
                    trade_num: 0,
                    active_buy_volume: 0.0,
                    active_buy_quote_volume: 0.0,
                    is_closed: true,
                });
                t += chrono::Duration::minutes(1);
            }
        }
    }
    bars.sort_by_key(|c| c.open_time);
    // 06-01 夜 240 + 06-02 全日+夜 465 + 06-03 日 225 = 930 根
    assert!(
        bars.len() > 800,
        "样本量足够跨 2 个交易日，实际 {}",
        bars.len()
    );

    let inst = InstrumentId::futures_contract(
        &ExchangeId::new("SHFE"),
        "CU",
        ContractMonth {
            year: 2026,
            month: 1,
        },
    );
    // D1b 退役注记：旧 `data::Feed` 侧随文件删除退场。原「旧 Feed vs engine」的
    // **运行时互校**（逐 bar D1/H1 收线时刻、逐桶 OHLCV、缺 bar 惰性闭合同刻、
    // 旧 Feed 水位幂等）失去宿主，其承载方式改为**对旧链冻结 golden 断言**——
    // 旧链真值仍参与判定，对照面不缩（见各节 assert_eq! 目标）。差账登记：
    // docs/exchange-spi-retirement.md §5.1。
    let mut state = AggregationState::new(&[Timeframe::M1, Timeframe::H1, Timeframe::D1], 2000);
    let policy = SessionWindowPolicy;
    let session = cn_futures_default();

    let mut state_day_h1: Vec<String> = Vec::new();
    let mut state_d1_times: Vec<String> = Vec::new();
    let fmt_bj = |t: &DateTime<Utc>| t.with_timezone(&cst()).format("%m-%d %H:%M").to_string();

    for c in &bars {
        let is_night_bar = {
            let loc = c.open_time.with_timezone(&cst());
            loc.hour() >= 20 || loc.hour() < 2
        };
        let s_new = state_from(&mut state, c, &policy, &session, &inst);
        // M1 每根都收线
        assert!(s_new.contains(&Timeframe::M1));
        if s_new.contains(&Timeframe::D1) {
            state_d1_times.push(fmt_bj(&c.open_time));
        }
        // H1 日盘分侧记账（差异基线 #3 由 golden 对承重，见下）
        if s_new.contains(&Timeframe::H1) && !is_night_bar {
            state_day_h1.push(fmt_bj(&c.open_time));
        }
    }

    // ① 夜盘 H1 逐桶内容：engine 侧对**两份** golden 同时断言
    //   （`agg_night_h1_feed.csv` 与 `agg_night_h1_state.csv` 实测逐字节相同；
    //    原「旧 Feed vs engine 逐桶 OHLCV 相等」的运行时互校随 Feed 退役，
    //    旧链真值由 *_feed golden 继续承重）
    let day_start = beijing(2026, 6, 2, 9, 0);
    let s_h1: Vec<Candle> = state
        .closed(Timeframe::H1)
        .iter()
        .map(bar_to_candle)
        .collect();
    let s_night: Vec<&Candle> = s_h1.iter().filter(|c| c.open_time < day_start).collect();
    assert!(!s_night.is_empty(), "夜盘 H1 至少一根");
    let g_night: Vec<String> = s_night.iter().map(|c| candle_golden_line(c)).collect();
    assert_eq!(
        g_night,
        read_golden_lines("agg_night_h1_state.csv"),
        "golden: engine 夜盘 H1 桶"
    );
    assert_eq!(
        g_night,
        read_golden_lines("agg_night_h1_feed.csv"),
        "golden: 旧链夜盘 H1 桶（旧侧真值仍参与判定）"
    );

    // ② D1 逐桶内容 + 收线时刻序列：D1 只有旧链侧冻结件（`agg_d1_feed.csv` /
    //   `agg_d1_close_times.txt`），原经「旧 Feed==engine」传递等值成立；
    //   Feed 退役后直接把 engine 侧钉在这两份旧链真值上（期望值一字未改）
    let s_d1: Vec<Candle> = state
        .closed(Timeframe::D1)
        .iter()
        .map(bar_to_candle)
        .collect();
    assert_eq!(s_d1.len(), 2, "D1 两根（跨 2 个交易日）");
    let g_d1: Vec<String> = s_d1.iter().map(candle_golden_line).collect();
    assert_eq!(
        g_d1,
        read_golden_lines("agg_d1_feed.csv"),
        "golden: 旧链 D1 桶内容"
    );
    assert_eq!(
        state_d1_times,
        read_golden_lines("agg_d1_close_times.txt"),
        "golden: 旧链 D1 收线时刻序列（含 14:59 即时闭合）"
    );

    // R3 golden 差异基线（差异表 #3）：日盘 H1 两侧收线序列均已在 golden 冻结。
    // 旧侧运行时列表随 `data::Feed` 退役不再产出，分叉面改由 **golden 对** 钉死。
    {
        assert_eq!(
            state_day_h1,
            read_golden_lines("agg_day_h1_state.txt"),
            "golden: engine 日盘 H1 收线"
        );
        assert_ne!(
            read_golden_lines("agg_day_h1_feed.txt"),
            read_golden_lines("agg_day_h1_state.txt"),
            "golden 对本身必须保留日盘分叉面（#3 基线失效预警）"
        );
    }

    // ③ 日盘 H1 差异基线（钉死而非放过）：原运行时形态
    //   `assert_ne!(feed_day_h1, state_day_h1)` 改为「engine vs 旧链 golden」——
    //   两侧锚点必须不同，不许因删除旧实现而静默收敛
    assert!(!state_day_h1.is_empty(), "日盘 H1 收线序列非空");
    assert_ne!(
        state_day_h1,
        read_golden_lines("agg_day_h1_feed.txt"),
        "engine 日盘 H1 锚点必须与旧链锚点不同（#3 设计后果）"
    );
    eprintln!(
        "[聚合器日盘 H1 差异基线] 旧锚(窗口段)={:?}\n[聚合器日盘 H1 差异基线] 新锚(窗口内网格)={state_day_h1:?}",
        read_golden_lines("agg_day_h1_feed.txt")
    );

    // ④ 缺 bar 惰性闭合 + ⑤ 水位幂等（夜盘内部样本）
    //   原「两侧同刻惰性闭合」与「旧 Feed 水位幂等」两条运行时互校随 Feed 退役；
    //   engine 侧行为断言（缺 22:59 时 22 点桶在 23:00 闭合、重喂拒绝）原样保留
    let mut st2 = AggregationState::new(&[Timeframe::M1, Timeframe::H1, Timeframe::D1], 2000);
    // 21:00-22:58（缺 22:59）+ 23:00,23:01：22 点桶必须在 23:00 惰性闭合
    let seg: Vec<&Candle> = bars
        .iter()
        .filter(|c| {
            c.open_time >= beijing(2026, 6, 1, 21, 0)
                && c.open_time < beijing(2026, 6, 1, 23, 0)
                && c.open_time != beijing(2026, 6, 1, 22, 59)
        })
        .chain(bars.iter().filter(|c| {
            c.open_time >= beijing(2026, 6, 1, 23, 0) && c.open_time < beijing(2026, 6, 1, 23, 2)
        }))
        .collect();
    let mut closed_at_23 = false;
    for c in seg.clone() {
        let se = state_from(&mut st2, c, &policy, &session, &inst);
        if c.open_time == beijing(2026, 6, 1, 23, 0) {
            closed_at_23 = se.contains(&Timeframe::H1);
        }
    }
    assert!(closed_at_23, "缺 22:59 时 22 点桶必须在 23:00 惰性闭合");
    // 幂等：重喂已见过的一根，拒绝且序列不增长
    let n2 = st2.closed(Timeframe::M1).len();
    let last = seg[seg.len() - 1];
    state_from(&mut st2, last, &policy, &session, &inst);
    assert_eq!(st2.closed(Timeframe::M1).len(), n2, "engine 水位幂等");
}

/// 状态侧摄入辅助（Bar 桥接 + 收线列表）
fn state_from(
    st: &mut AggregationState,
    c: &Candle,
    policy: &SessionWindowPolicy,
    session: &WindowedSession,
    inst: &InstrumentId,
) -> Vec<Timeframe> {
    st.on_closed_1m(Bar::from_candle(inst.clone(), *c), policy, session)
        .unwrap()
}

// ─────────────────── golden（R3 退役冻结位）───────────────────
//
// 旧链在 R4/D1 删除后，双跑对照失去宿主：把旧侧输出冻结为文本 golden
// （`tests/golden/retirement/`），退役后 engine 侧对 golden 断言——断言面不缩。
//
// trade 列式：`timestamp,trade_id,side,symbol,strategy_id,quantity,price,
// commission,pnl`（%.9；排除字段 id=差异表 #2 承载 signal_id 方案、
// comment=阶段 5 已声明口径、strategy_name/slippage=夹具常量）。
// 聚合桶行式：`ISO_秒,open,high,low,close,volume`；时刻列表一行一
// `MM-DD HH:MM`（北京格式化，与 fmt_bj 同形）。

fn golden_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/retirement")
}

fn read_golden_lines(name: &str) -> Vec<String> {
    let p = golden_dir().join(name);
    let text = std::fs::read_to_string(&p).unwrap_or_else(|e| {
        panic!(
            "golden 缺失 {}（{e}）——golden 文件应已随仓库提交",
            p.display()
        )
    });
    text.lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with("timestamp,"))
        .map(|l| l.to_string())
        .collect()
}

fn golden_rows(name: &str) -> Vec<TradeRow> {
    read_golden_lines(name)
        .into_iter()
        .map(|l| {
            let c: Vec<&str> = l.split(',').collect();
            assert_eq!(c.len(), 9, "golden 列数 {name}");
            TradeRow {
                id: String::new(),
                strategy_name: String::new(),
                trade_id: c[1].into(),
                strategy_id: c[4].into(),
                symbol: c[3].into(),
                side: c[2].into(),
                quantity: c[5].parse().unwrap(),
                price: c[6].parse().unwrap(),
                commission: c[7].parse().unwrap(),
                slippage: 0.0,
                pnl: c[8].parse().unwrap(),
                timestamp: c[0].into(),
                comment: String::new(),
            }
        })
        .collect()
}

/// golden 行间逐字段断言（保留供手工校验使用）
///
/// 保留位：执行链当前走 `assert_trade_pair`（golden TradeRow vs 实时 Fill）。
/// 本 helper 面向 TradeRow↔TradeRow 全字段（含 trade_id/strategy_id/symbol）
/// 手工核对，未参与账本执行——后续增删执行案时不得静默删除本保留位。
#[allow(dead_code)]
fn assert_golden_row_eq(g: &TradeRow, o: &TradeRow, i: usize) {
    assert_eq!(g.timestamp, o.timestamp, "golden 行{i} 时刻");
    assert_eq!(g.trade_id, o.trade_id, "golden 行{i} trade_id");
    assert_eq!(g.side, o.side, "golden 行{i} 方向标签");
    assert_eq!(g.symbol, o.symbol, "golden 行{i} symbol");
    assert_eq!(g.strategy_id, o.strategy_id, "golden 行{i} strategy_id");
    for (a, b, tag) in [
        (g.quantity, o.quantity, "数量"),
        (g.price, o.price, "价格"),
        (g.commission, o.commission, "手续费"),
        (g.pnl, o.pnl, "pnl"),
    ] {
        assert!((a - b).abs() < 1e-9, "golden 行{i} {tag}: {a} vs {b}");
    }
}

fn candle_golden_line(c: &Candle) -> String {
    format!(
        "{},{:.9},{:.9},{:.9},{:.9},{:.9}",
        c.open_time.format("%Y-%m-%dT%H:%M:%S"),
        c.open,
        c.high,
        c.low,
        c.close,
        c.volume
    )
}
