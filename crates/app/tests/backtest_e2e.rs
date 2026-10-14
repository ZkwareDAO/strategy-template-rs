//! strategy-app 集成测试 — 回测链路端到端
//!
//! 场景：合成一段先涨后跌的 1m K 线 → 引擎完整跑一遍 → 验证
//! signal.csv / exchange_orders.csv 落盘、统一 Store 当日快照
//! （signal/position/stop/cooldown/history_position/trade/equity）可跨进程
//! 恢复，且回测重放（同数据再跑一遍，含中途 flush 与结束 flush）产出
//! **逐字节相同** 的产物（确定性验收）。

use std::collections::BTreeMap;
use std::path::Path;

use chrono::{TimeZone, Utc};

use strategy_app::engine::{Engine, EngineConfig};
use strategy_core::candle::{Candle, Timeframe};
use strategy_core::signal::{generate_signal_id, Action, Side, SignalType};
use strategy_core::strategy::{Ctx, Strategy};

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

impl Strategy for TestStrategy {
    fn strategy_type(&self) -> &str {
        "test"
    }
    fn timeframes(&self) -> Vec<Timeframe> {
        vec![Timeframe::M1]
    }
    fn on_candle(&mut self, ctx: &Ctx<'_>) -> Vec<Action> {
        if self.opened {
            return vec![];
        }
        let Some(c) = ctx.closed_data(Timeframe::M1) else {
            return vec![];
        };
        if c.len() < 2 {
            return vec![];
        }
        let (last, prev) = (c[c.len() - 1].close, c[c.len() - 2].close);
        let side = if last > prev { Side::Long } else { Side::Short };
        self.opened = true;
        vec![Action::Open {
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

fn build_engine(out_base: &Path) -> Engine {
    let data_base = out_base.join("data");
    let cfg = EngineConfig {
        strategy_name: NAME.into(),
        strategy_type: "test".into(),
        symbol: "BTCUSDT".into(),
        trading_mode: "backtest".into(),
        cash: 200.0,
        leverage: 1,
        ..Default::default()
    };
    let broker = strategy_broker::backtest::BacktestBroker::new(
        strategy_broker::backtest::BacktestConfig {
            leverage: 1,
            notional_per_order: 200.0,
            exchange: "binance".into(),
            ..Default::default()
        },
        NAME,
        out_base,
    );
    Engine::new(
        cfg,
        Box::new(TestStrategy { opened: false }),
        strategy_broker::BacktestOrderClient::new(broker),
        &data_base,
        out_base,
    )
}

#[test]
fn backtest_end_to_end_writes_all_csvs() {
    let dir = tempfile::tempdir().unwrap();
    let out_base = dir.path().join("out");

    let mut engine = build_engine(&out_base);
    engine.restore().unwrap();

    let bars = synthetic_1m(600);
    for c in &bars {
        engine.on_closed_1m(*c).unwrap();
        engine.on_equity_mark(c);
    }
    engine.flush_state().unwrap();

    let stats = engine.stats();
    assert_eq!(stats.bars, 600);
    assert!(stats.signals_written >= 1, "应至少写一条信号");
    assert!(stats.orders_written >= 1, "应至少有一笔成交");
    assert!(stats.positions_opened >= 1);

    // signal.csv 存在且 header/内容正确（strategy_name 为首列）
    let sig_path = out_base.join(format!("signals/{NAME}/signal.csv"));
    assert!(sig_path.exists());
    let sig = std::fs::read_to_string(&sig_path).unwrap();
    assert!(sig.starts_with("strategy_name,signal_id,timestamp,user_id,signal_action,symbol"));
    assert!(sig.contains("\ntest_1m_1_BTCUSDT_BACKTEST,sig_"));

    // exchange_orders.csv 存在（对外回执契约保留）
    let ord_path = out_base.join(format!("{NAME}/exchange_orders.csv"));
    assert!(ord_path.exists());
    let ord = std::fs::read_to_string(&ord_path).unwrap();
    assert!(ord.starts_with("signal_id,symbol,exchange,leverage,pos_type,price,side"));
    assert!(ord.contains("FILLED"));

    // S5 数值不变性：存储形态变，撮合/pnl/净值数值必须与改前逐项一致
    //（改前基线由旧 backtest_trades.csv/equity tracker 同场景跑出）。
    let trades = engine.trade_rows();
    assert_eq!(trades.len(), 2, "成交数 = 2（1 开 1 平）");
    assert_eq!(trades[0].trade_id, "T00000001");
    assert_eq!(trades[0].side, "BUY");
    assert_eq!(trades[0].comment, "open");
    assert!((trades[0].quantity - 1.9920318725099604).abs() < 1e-12);
    assert!((trades[0].price - 100.39999999999999).abs() < 1e-12);
    assert!((trades[0].commission - 0.08).abs() < 1e-12);
    assert_eq!(trades[0].pnl, 0.0);
    assert_eq!(trades[0].timestamp, "2026-06-10T00:01:00");
    assert_eq!(trades[1].trade_id, "T00000002");
    assert_eq!(trades[1].side, "SELL_CLOSE");
    assert!((trades[1].price - 158.0).abs() < 1e-12);
    assert!((trades[1].pnl - 114.74103585657373).abs() < 1e-9);
    assert_eq!(trades[1].timestamp, "2026-06-10T04:47:00");
    let pnl_sum: f64 = trades.iter().map(|t| t.pnl).sum();
    let fee_sum: f64 = trades.iter().map(|t| t.commission).sum();
    assert!((pnl_sum - 114.74103585657373).abs() < 1e-9);
    assert!((fee_sum - 0.16).abs() < 1e-12);

    let daily = engine.daily_equity();
    assert_eq!(daily.len(), 1);
    assert_eq!(daily[0].label, "2026-06-10");
    assert!((daily[0].equity - 5114.581035856574).abs() < 1e-9);
    assert!((daily[0].cash - 5114.581035856574).abs() < 1e-9);
    let tf = engine.tf_equity();
    assert_eq!(tf.len(), 10, "00:00..09:00 共 10 个 1h 桶");
    assert_eq!(tf[0].label, "2026-06-10 00:00");
    assert!((tf[0].equity - 5034.581354581674).abs() < 1e-9);
    assert!((tf[0].cash - 4999.92).abs() < 1e-9);
    assert_eq!(tf[9].label, "2026-06-10 09:00");
    assert!((tf[9].equity - 5114.581035856574).abs() < 1e-9);

    // S5：成交/净值进入统一 Store 当日快照（通用 5 列格式）
    let trade_path = out_base.join(format!("state/{DAY}/trade.csv"));
    assert!(trade_path.exists(), "trade.csv 须随当日快照落盘");
    let trade_csv = std::fs::read_to_string(&trade_path).unwrap();
    assert_eq!(
        trade_csv.lines().next().unwrap(),
        "id,deleted,created_at_ms,updated_at_ms,payload_json"
    );
    assert!(
        trade_csv.contains("trade_sig_"),
        "成交行业务 id 确定性绑定信号"
    );
    // payload_json 整列经 CSV 转义（内层引号双写）
    assert!(trade_csv.contains("\"\"pnl\"\":114.74103585657373"));

    let equity_path = out_base.join(format!("state/{DAY}/equity.csv"));
    assert!(equity_path.exists(), "equity.csv 须随当日快照落盘");
    let equity_csv = std::fs::read_to_string(&equity_path).unwrap();
    assert_eq!(
        equity_csv.lines().next().unwrap(),
        "id,deleted,created_at_ms,updated_at_ms,payload_json"
    );
    // 日净值与信号周期净值同表（kind 区分）；payload 经 CSV 转义
    assert!(equity_csv.contains("\"\"kind\"\":\"\"daily\"\""));
    assert!(equity_csv.contains("\"\"kind\"\":\"\"tf\"\""));
    assert!(equity_csv.contains("5114.581035856574"));
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
    // 回测确定性验收：同数据重放（含中途 flush 与结束 flush，CandleClock）→
    // out/ 全树（对外追加报表 + state/{day}/{model}.csv 通用快照）逐字节相同。
    let run = |dir: &Path| -> (BTreeMap<String, Vec<u8>>, BTreeMap<String, Vec<u8>>) {
        let out_base = dir.join("out");
        let mut engine = build_engine(&out_base);
        engine.restore().unwrap();
        let bars = synthetic_1m(600);
        for (i, c) in bars.into_iter().enumerate() {
            engine.on_closed_1m(c).unwrap();
            engine.on_equity_mark(&c);
            // 与 live 定时落盘同构的中途 flush（bar 300，时钟停在该 K 线）
            if i == 299 {
                engine.flush_state().unwrap();
            }
        }
        let mid = snapshot_tree(&out_base);
        // 结束 flush：与 CLI 主循环一致，覆盖写全部已注册 model 当日快照
        engine.flush_state().unwrap();
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

    // S5 新布局：state/{day}/{model}.csv 实际出现的文件清单
    let mut state_files: Vec<String> = end1
        .keys()
        .filter(|k| k.starts_with(&format!("state/{DAY}/")))
        .cloned()
        .collect();
    state_files.sort();
    for model in [
        "signal",
        "position",
        "stop",
        "cooldown",
        "history_position",
        "trade",
        "equity",
    ] {
        let rel = format!("state/{DAY}/{model}.csv");
        assert!(
            state_files.iter().any(|k| k == &rel),
            "{model}.csv 未出现在快照清单: {state_files:?}"
        );
    }

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
}

#[test]
fn deterministic_signal_ids_across_runs() {
    // signal_id 只由 (strategy_type, symbol, kline_time, signal_type) 决定
    let t = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
    let a = generate_signal_id("test", "BTCUSDT", t, SignalType::Buy);
    let b = generate_signal_id("test", "BTCUSDT", t, SignalType::Buy);
    assert_eq!(a, b);
}
