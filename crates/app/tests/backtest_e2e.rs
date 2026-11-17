//! strategy-app 集成测试 — engine 回测链路端到端
//!
//! 场景：合成一段先涨后跌的 1m K 线 → engine 回测完整跑一遍 → 验证
//! 统一 Store 当日快照（equity）落盘，交易行从 Fill 派生，
//! 且回测重放（同数据再跑一遍，含中途 flush 与结束 flush）产出
//! **逐字节相同** 的产物（确定性验收）。
//!
//! engine 流水线不经过旧 BacktestBroker → 不产生 signal.csv / exchange_orders.csv
//! 对外旁路 CSV（旧链路专属）；engine 成交从 Fill 派生，净值经 EquitySink
//! 写入统一 Store equity model。

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use chrono::{TimeZone, Utc};

use strategy_app::report::EquitySink;
use strategy_app::spi_backtest::{candles_to_bars, fills_to_trade_rows, SpiBacktest};
use strategy_core::candle::{Candle, Timeframe};
use strategy_core::signal::{generate_signal_id, Side, SignalType};
use strategy_engine::core::bar::Timeframe as EngineTimeframe;
use strategy_engine::exchanges::binance::BinanceExchange;
use strategy_engine::prelude::*;
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};
use strategy_store::{Clock, Store};

/// 确定性时钟：固定返回合成数据第一根 K 线的时间戳（ms）。
/// 回测 Store 时钟必须锚定 K 线时间，禁止 Utc::now（report.rs 注释）。
struct FixedClock(i64);
impl Clock for FixedClock {
    fn now_ms(&self) -> i64 {
        self.0
    }
}

/// 合成 1m 序列：4h 主周期内分段涨跌（触发信号）
fn synthetic_1m(n: usize) -> Vec<Candle> {
    (0..n)
        .map(|i| {
            let t = Utc.timestamp_opt(1_781_049_600 + i as i64 * 60, 0).unwrap();
            let bucket = i / 240;
            let v = match bucket % 4 {
                0 => 100.0 + (i % 240) as f64 * 0.3, // 涨
                1 => 172.0 - (i % 240) as f64 * 0.3, // 跌
                2 => 100.0 + (i % 240) as f64 * 0.5, // 快涨
                _ => 220.0 - (i % 240) as f64 * 0.5, // 快跌
            };
            Candle {
                open_time: t,
                open: v,
                high: v + 0.5,
                low: v - 0.5,
                close: v + 0.1,
                volume: 10.0,
                quote_volume: 0.0,
                trade_num: 0,
                active_buy_volume: 0.0,
                active_buy_quote_volume: 0.0,
                is_closed: true,
            }
        })
        .collect()
}

/// 简单测试策略：主周期（1m）收盘 > 前一根 → 开多；否则开空（保证有信号产生）
struct TestStrategy {
    opened: bool,
}

impl CrossSectionalStrategyPort for TestStrategy {
    fn strategy_type(&self) -> &str {
        "test"
    }
    fn timeframes(&self) -> Vec<EngineTimeframe> {
        vec![EngineTimeframe::M1]
    }
    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[EngineTimeframe],
        snapshots: &[InstrumentSnapshot],
        _portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        if self.opened {
            return vec![];
        }
        let Some(s) = snapshots.first() else {
            return vec![];
        };
        let Some(c) = s.closed.as_ref().and_then(|cs| cs.closed(Timeframe::M1)) else {
            return vec![];
        };
        if c.len() < 2 {
            return vec![];
        }
        let (last, prev) = (c[c.len() - 1].close, c[c.len() - 2].close);
        let side = if last > prev { Side::Long } else { Side::Short };
        self.opened = true;
        vec![InstrumentAction::Open {
            instrument: s.instrument.clone(),
            side,
            price: last,
            stop_price: last * 0.98,
            atr: 1.0,
            strength: 0.8,
            reason: "test".into(),
        }]
    }
}

const NAME: &str = "test_1m_1_BTCUSDT_BACKTEST";
const DAY: &str = "20260610";
const STRATEGY_TYPE: &str = "test";
const SYMBOL: &str = "BTCUSDT";
const INITIAL_CASH: f64 = 200.0;
const FEE_RATE: f64 = 0.0004;
const TF_MINUTES: u32 = 1; // 1m 策略周期

/// engine 回测装配：返回 (SpiBacktest, Arc<Store>, EquitySink, InstrumentId)
fn build_spi(out_base: &Path) -> (SpiBacktest, Arc<Store>, EquitySink, InstrumentId) {
    let state_dir = out_base.join("state");

    // Exchange 装配（crypto 永续，无合约）
    let exchange = BinanceExchange::named(ExchangeId::new("binance"), FEE_RATE);
    let inst = exchange.parse_instrument(SYMBOL).expect("标的解析失败");
    let ex = exchange.with_symbol_spec(inst.clone(), InstrumentSpec::crypto_perpetual(FEE_RATE));
    let ex: Arc<dyn Exchange> = Arc::new(ex);

    // SourceRequest：空 bars（手动步进）
    let req = SourceRequest::backtest(inst.clone(), vec![], INITIAL_CASH);
    let sizing = SizingParams {
        leverage: 1.0,
        notional_per_order: None, // 全仓
    };
    let risk = strategy_core::risk::RiskControlConfig::default();
    let bt = SpiBacktest::assemble(
        &ex,
        req,
        Box::new(TestStrategy { opened: false }),
        sizing,
        risk,
        2000,
    )
    .expect("engine 装配失败");

    // Store + EquitySink（确定性时钟锚定 K 线时间，禁止 EpochClock）
    let clock_ms = Utc
        .timestamp_opt(1_781_049_600, 0)
        .unwrap()
        .timestamp_millis(); // 2026-06-10 00:00 UTC
    let store = Arc::new(Store::new(&state_dir, Arc::new(FixedClock(clock_ms))));
    let sink = EquitySink::new(NAME, &store);

    (bt, store, sink, inst)
}

#[test]
fn backtest_end_to_end_writes_all_csvs() {
    let dir = tempfile::tempdir().unwrap();
    let out_base = dir.path().join("out");

    let (mut bt, store, sink, inst) = build_spi(&out_base);

    let bars = synthetic_1m(600);
    let spi_bars = candles_to_bars(&inst, &bars);
    bt.preload(&spi_bars[..0]) // 无 warmup
        .expect("engine preload 失败");

    for c in &bars {
        let bar = Bar::from_candle(inst.clone(), *c);
        bt.step(MarketEvent::Bar(bar)).expect("engine step 失败");
        // equity mark：每根 1m 收盘后取账户权益
        if let Some(acct) = bt.account_view() {
            sink.on_1m_close(c.open_time, acct.equity(), acct.cash, TF_MINUTES);
        }
    }

    // 收口
    let summary = bt.finish().expect("engine finish 失败");
    store.flush().expect("Store flush 失败");

    // 基本统计
    assert_eq!(summary.bars_processed, 600);
    assert!(
        summary.fills.len() >= 2,
        "应至少有 2 笔成交（开+平），实际 {}",
        summary.fills.len()
    );

    // engine 流水线不经过旧 BacktestBroker → 不产生 signal.csv / exchange_orders.csv
    // （对外旁路 CSV 是旧链路专属产物；engine 成交从 Fill 派生）。
    // 如实反向钉形：这两个旧旁路文件不应出现（出现 = 报表链漂移）。
    assert!(
        !out_base.join(format!("signals/{NAME}/signal.csv")).exists(),
        "engine 分支不应生成 signals/{NAME}/signal.csv（旧链路旁路产物）"
    );
    assert!(
        !out_base
            .join(format!("{NAME}/exchange_orders.csv"))
            .exists(),
        "engine 分支不应生成 {NAME}/exchange_orders.csv（旧链路旁路产物）"
    );

    // 交易行从 Fill 派生
    let trades = fills_to_trade_rows(&summary.fills, NAME, STRATEGY_TYPE, SYMBOL);
    assert!(
        trades.len() >= 2,
        "成交数 >= 2（1 开 1 平），实际 {}",
        trades.len()
    );
    // 第一笔 = 开仓 BUY
    assert_eq!(trades[0].side, "BUY");
    assert_eq!(trades[0].symbol, SYMBOL);
    assert_eq!(trades[0].strategy_name, NAME);
    // comment：engine Fill 通道不搬运策略 reason 文本，非止损为空
    assert_eq!(trades[0].comment, "");
    // 第二笔 = 平仓 SELL_CLOSE
    assert_eq!(trades[1].side, "SELL_CLOSE");
    // pnl 应非零（平仓实现了盈亏）
    assert!(
        trades[1].pnl.abs() > 1e-9,
        "平仓 pnl 应非零，实际 {}",
        trades[1].pnl
    );
    // trade_id 确定性序号
    assert_eq!(trades[0].trade_id, "T00000001");
    assert_eq!(trades[1].trade_id, "T00000002");
    // 手续费
    let fee_sum: f64 = trades.iter().map(|t| t.commission).sum();
    assert!(fee_sum > 0.0, "总手续费应 > 0");
    // signal_id 格式为 sig_{16hex}（SHA256 哈希），不是旧 sig_test_ 前缀
    assert!(
        trades[0].id.starts_with("trade_sig_"),
        "trade id 应为 trade_sig_ 前缀，实际 {}",
        trades[0].id
    );

    // 净值点
    let daily = sink.daily();
    assert_eq!(daily.len(), 1, "应只有 1 个交易日");
    assert_eq!(daily[0].label, "2026-06-10");
    assert!(
        daily[0].equity > INITIAL_CASH,
        "期末权益应 > 初始资金（盈利策略）"
    );

    let tf = sink.tf();
    assert!(
        tf.len() >= 10,
        "1h 桶至少 10 个（00:00..09:00+），实际 {}",
        tf.len()
    );
    assert_eq!(tf[0].label, "2026-06-10 00:00");
    // 最后一个 tf 桶 = 期末
    let last_tf = tf.last().unwrap();
    assert!(
        (last_tf.equity - daily[0].equity).abs() < 1e-9,
        "最后一个 tf 桶权益应等于日末权益"
    );

    // S5：state/{DAY}/equity.csv 通用 5 列格式（EquitySink 写入）
    let equity_path = out_base.join(format!("state/{DAY}/equity.csv"));
    assert!(equity_path.exists(), "equity.csv 须随当日快照落盘");
    let equity_csv = std::fs::read_to_string(&equity_path).unwrap();
    assert_eq!(
        equity_csv.lines().next().unwrap(),
        "id,deleted,created_at_ms,updated_at_ms,payload_json"
    );
    // daily 与 tf 同表（kind 区分）
    assert!(equity_csv.contains("\"\"kind\"\":\"\"daily\"\""));
    assert!(equity_csv.contains("\"\"kind\"\":\"\"tf\"\""));
}

/// 递归收集目录下全部文件：相对路径 → 字节（逐字节对比用）。
fn snapshot_tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    fn walk(root: &Path, rel: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        let Ok(rd) = std::fs::read_dir(root) else {
            return;
        };
        for e in rd.flatten() {
            let path = e.path();
            let key = rel.join(e.file_name());
            if path.is_dir() {
                walk(&path, &key, out);
            } else {
                out.insert(
                    key.to_string_lossy().into_owned(),
                    std::fs::read(&path).unwrap(),
                );
            }
        }
    }
    walk(root, Path::new(""), &mut out);
    out
}

#[test]
fn backtest_replay_is_byte_identical() {
    // 回测确定性验收：同数据重放（含中途 flush 与结束 flush）→
    // out/ 全树（state/{day}/{model}.csv 通用快照）逐字节相同。
    let run = |dir: &Path| -> (BTreeMap<String, Vec<u8>>, BTreeMap<String, Vec<u8>>) {
        let out_base = dir.join("out");
        let (mut bt, store, sink, inst) = build_spi(&out_base);

        let bars = synthetic_1m(600);
        let spi_bars = candles_to_bars(&inst, &bars);
        bt.preload(&spi_bars[..0]).expect("engine preload 失败");

        for (i, c) in bars.into_iter().enumerate() {
            let bar = Bar::from_candle(inst.clone(), c);
            bt.step(MarketEvent::Bar(bar)).expect("engine step 失败");
            if let Some(acct) = bt.account_view() {
                sink.on_1m_close(c.open_time, acct.equity(), acct.cash, TF_MINUTES);
            }
            // 与 live 定时落盘同构的中途 flush（bar 300）
            if i == 299 {
                store.flush().expect("中途 Store flush 失败");
            }
        }
        let mid = snapshot_tree(&out_base);
        // 结束 flush：覆盖写全部已注册 model 当日快照
        store.flush().expect("结束 Store flush 失败");
        let final_tree = snapshot_tree(&out_base);
        (mid, final_tree)
    };

    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();
    let (mid1, end1) = run(dir1.path());
    let (mid2, end2) = run(dir2.path());

    // 中途 flush 时刻两轮回放已经逐字节一致
    assert_eq!(mid1, mid2, "中途 flush 后两次回放不一致");
    // 结束后全树逐字节一致（文件清单 + 每个字节）
    assert_eq!(end1, end2, "结束 flush 后两次回放不一致");
    assert_eq!(
        end1.keys().collect::<Vec<_>>(),
        end2.keys().collect::<Vec<_>>()
    );

    // state/{day}/equity.csv 须出现在快照清单
    // 跨平台兼容：Windows 路径分隔符为 `\`，统一比较时归一化为 `/`。
    let state_prefix = format!("state/{DAY}/");
    let mut state_files: Vec<String> = end1
        .keys()
        .filter(|k| {
            let normalized = k.replace('\\', "/");
            normalized.starts_with(&state_prefix)
        })
        .cloned()
        .collect();
    state_files.sort();
    let equity_rel = format!("state/{DAY}/equity.csv");
    assert!(
        state_files
            .iter()
            .any(|k| k.replace('\\', "/") == equity_rel),
        "equity.csv 未出现在快照清单: {state_files:?}"
    );

    // 旧报表文件名彻底消失
    for stale in [
        "backtest_equity.csv",
        "backtest_trades.csv",
        "1h_equity.csv",
    ] {
        assert!(
            !end1.keys().any(|k| k.ends_with(stale)),
            "旧报表 {stale} 不应再产出"
        );
    }

    // engine 不产生旧链路旁路 CSV（signals/{NAME}/signal.csv、{NAME}/exchange_orders.csv）
    let signal_old = format!("signals/{NAME}/signal.csv");
    let orders_old = format!("{NAME}/exchange_orders.csv");
    assert!(
        !end1.keys().any(|k| {
            let n = k.replace('\\', "/");
            n == signal_old || n == orders_old
        }),
        "engine 分支不应生成旧链路旁路 signal.csv / exchange_orders.csv"
    );
}

#[test]
fn deterministic_signal_ids_across_runs() {
    // signal_id 只由 (strategy_type, symbol, kline_time, signal_type) 决定
    let t = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
    let a = generate_signal_id("test", "BTCUSDT", t, SignalType::Buy);
    let b = generate_signal_id("test", "BTCUSDT", t, SignalType::Buy);
    assert_eq!(a, b);
}
