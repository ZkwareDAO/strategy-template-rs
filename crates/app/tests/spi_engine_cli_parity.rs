//! 阶段 5（v2.8，§13-5）CLI 路径验收：`--engine spi` 的编排函数
//! `spi_backtest::run_from_spec` 与旧 `Engine` 主循环**逐笔一致**
//! （golden 文件冻结自旧链路退役前的输出）。
//!
//! 旧 `Engine` 已删除（D1 退役）。本测试文件移除全部旧引擎引用，
//! 改为将 engine 输出与 `tests/golden/retirement/` 下的 golden 文件
//! 逐笔对比——golden 文件冻结自旧引擎，是旧链路退役后的幸存断言面。
//!
//! 两条阶段 5 验收：
//! 1. **两模式共用主循环**（回测侧承重）：spi 分支的事件消费、撮合、
//!    止损/权益曲线全部走 `ExchangeRunner`——与实盘桥同一份代码
//!    （live 侧旧 `runtime_actor` 的职责合并为 5b，该模块已随 R4 退役删除，见 §13 注）；
//! 2. **单进程多交易所混跑**：binance + ctp 在同一进程各自 `SpiBacktest`
//!    跑通开平，口径互不污染（期货整数手 vs crypto 小数、CNY vs USDT）。
//!
//! 逐笔对比的豁免字段 = 差异表既有登记项：`id`（signal_id 方案，#2）
//! 与 `comment`（旧链路携带策略 reason 文本，Fill 通道不搬运——本阶段
//! 如实声明的差异）。手续费在恒定价样本下两口径同数（差异 #1 的
//! 公式承重仍在 `spi_dual_run_parity`）。

use chrono::{DateTime, TimeZone, Utc};
use std::path::Path;
use strategy_app::config::{self, SymbolSpec};
use strategy_app::report::TradeRow;
use strategy_app::spi_backtest::{self, SpiBacktest, SpiRunArgs};
use strategy_core::candle::Candle;
use strategy_core::risk::RiskControlConfig;
use strategy_core::Side;
use strategy_engine::core::bar::Timeframe as EngineTimeframe;
use strategy_engine::exchanges::build_registry;
use strategy_engine::prelude::*;
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};

fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
}

/// 每分钟一根、恒定价的 1m 样本（`from` 起 `n` 根）。
fn flat_candles(from: DateTime<Utc>, n: usize, price: f64) -> Vec<Candle> {
    (0..n)
        .map(|i| Candle {
            open_time: from + chrono::Duration::minutes(i as i64),
            open: price,
            high: price,
            low: price,
            close: price,
            volume: 1.0,
            quote_volume: price,
            trade_num: 1,
            active_buy_volume: 0.0,
            active_buy_quote_volume: 0.0,
            is_closed: true,
        })
        .collect()
}

/// 极简确定性策略：空仓首根开多（低价止损永不到位），持仓后下一根市价平。
struct OnceThenExit {
    opened: bool,
    close_next: bool,
}

impl CrossSectionalStrategyPort for OnceThenExit {
    fn strategy_type(&self) -> &str {
        "once_then_exit"
    }
    fn timeframes(&self) -> Vec<EngineTimeframe> {
        // 1m + 1h：1h 兼做聚合器与「聚合K线导出」的验收面
        vec![EngineTimeframe::M1, EngineTimeframe::H1]
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
        match s.position {
            None => {
                if self.opened {
                    return vec![];
                }
                self.opened = true;
                self.close_next = true;
                vec![InstrumentAction::Open {
                    instrument: s.instrument.clone(),
                    side: Side::Long,
                    price: s.bar.close,
                    stop_price: s.bar.close * 0.5,
                    atr: 0.0,
                    strength: 1.0,
                    reason: "cli5".into(),
                }]
            }
            Some(_) => {
                if self.close_next {
                    self.close_next = false;
                    return vec![InstrumentAction::Close {
                        instrument: s.instrument.clone(),
                        price: s.bar.close,
                        reason: "cli5".into(),
                        is_stop_loss: false,
                        strength: 1.0,
                    }];
                }
                Vec::new()
            }
        }
    }
}

fn crypto_spec(symbol: &str) -> SymbolSpec {
    SymbolSpec {
        strategy: "once_then_exit".into(),
        symbol: symbol.into(),
        timeframe: "1h".into(),
        cash: 0.0,
        initial_cash: None,
        params: None,
        warmup_days: 1,
        exchange: "binance".into(),
        venue_key: format!("binance/crypto_perpetual/{symbol}"),
        account: "acc".into(),
        leverage: 1,
        mode: "backtest".into(),
        contract: None,
        risk: RiskControlConfig::default(),
        venue_qty_step: None,
        venue_min_qty: None,
        venue_qty_precision: None,
    }
}

/// 逐笔一致硬断言（豁免=id/comment；trade_id 序列号格式同可正面对比）。
fn assert_row_equal(golden: &TradeRow, spi: &TradeRow) {
    assert_eq!(golden.timestamp, spi.timestamp, "事件时刻");
    assert_eq!(golden.trade_id, spi.trade_id, "T%08 序列号");
    assert_eq!(golden.side, spi.side, "方向标签");
    assert_eq!(golden.strategy_id, spi.strategy_id);
    assert_eq!(golden.symbol, spi.symbol);
    assert!((golden.quantity - spi.quantity).abs() < 1e-9, "数量");
    assert!((golden.price - spi.price).abs() < 1e-9, "成交价");
    assert!((golden.pnl - spi.pnl).abs() < 1e-9, "pnl");
    assert!(
        (golden.commission - spi.commission).abs() < 1e-9,
        "手续费（恒价样本两口径同数）"
    );
}

/// 验收 1 主案：crypto 恒定价样本，`run_from_spec` 的交易行与 golden
/// 文件（冻结自旧引擎输出）**逐笔全字段一致**（豁免面=id/comment），
/// 且日净值末点同数（equity 采样与 `on_equity_mark` 同刻同价）。
#[test]
fn cli_spi_branch_matches_old_engine_row_by_row() {
    let dir_spi = tempfile::tempdir().unwrap();
    let spec = crypto_spec("BTCUSDT");
    let warmup = flat_candles(utc(2026, 1, 3, 0, 0), 60, 100.0);
    let trading = flat_candles(utc(2026, 1, 5, 0, 0), 30, 100.0);

    let run = spi_backtest::run_from_spec(SpiRunArgs {
        spec: &spec,
        strategy: Box::new(OnceThenExit {
            opened: false,
            close_next: false,
        }),
        strategy_name: "once_1H_3_BTCUSDT_BACKTEST",
        warmup: &warmup,
        trading: &trading,
        initial_cash: 1000.0,
        leverage: 1.0,
        fee_rate: Some(0.0004),
        risk: RiskControlConfig::default(),
        state_dir: &dir_spi.path().join("state"),
        data_dir: dir_spi.path(),
    })
    .expect("spi 分支装配运行");

    assert_eq!(run.trade_rows.len(), 2, "crypto 开平两笔");

    // golden 对比（旧链路退役后的幸存断言面）
    {
        let g = golden_rows("cli_old_engine_rows.csv");
        assert_eq!(g.len(), 2, "golden 行数一致");
        assert_eq!(g.len(), run.trade_rows.len(), "golden 与 engine 笔数对齐");
        for (gr, n) in g.iter().zip(run.trade_rows.iter()) {
            assert_row_equal(gr, n);
        }
        let last_g = read_golden_lines("cli_old_daily_last.txt");
        assert_eq!(last_g.len(), 2, "golden: label + equity 两行");
        let last_new = run.daily.last().expect("spi 日净值");
        assert_eq!(last_g[0], last_new.label, "golden 日桶 label");
        let g_eq: f64 = last_g[1].parse().unwrap();
        assert!(
            (g_eq - last_new.equity).abs() < 1e-6,
            "golden 末点权益 vs engine: {g_eq} vs {}",
            last_new.equity
        );
    }

    // 标签抽样钉形：开多=BUY、平多=SELL_CLOSE
    assert_eq!(run.trade_rows[0].side, "BUY");
    assert_eq!(run.trade_rows[1].side, "SELL_CLOSE");

    // 恒定价开平净费 0.8：1000−0.8=999.2
    let last_new = run.daily.last().expect("spi 日净值");
    assert!(
        (last_new.equity - 999.2).abs() < 1e-6,
        "恒定价开平净费 0.8：1000−0.8=999.2，实际 {}",
        last_new.equity
    );
    // tf 净值点按 1h 桶（30 根 1m ⇒ ≥1 个点；桶键与 EquitySink 同实现）
    assert!(!run.tf_points.is_empty());
    assert!(run.klines_exported >= 1, "1h 聚合 K 线导出应落盘");
    let h1_csv = dir_spi
        .path()
        .join("1h")
        .join("binance")
        .join("crypto_perpetual")
        .join("BTCUSDT_1h.csv");
    assert!(
        h1_csv.exists(),
        "聚合导出走命名空间布局 {{base}}/{{tf}}/{{exchange}}/{{market}}/{{SYMBOL}}_{{tf}}.csv（§6.1/5c）"
    );
    assert!(
        !dir_spi.path().join("1h").join("BTCUSDT_1h.csv").exists(),
        "旧平铺布局不再产出（5c 一次性切换，不双写）"
    );
}

/// 期货样本：`contracts.yaml` 真合约 → `spec_from_contract` → MarginLots
/// 整数手；当日开平=平今（费率加倍从配置读，不硬编码数字）。
#[test]
fn cli_spi_branch_ctp_integer_lots_and_close_today_fee() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config::load(Some(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config")
            .as_path(),
    ))
    .expect("仓库 config");
    let contract = config::contract_for(&cfg.contracts, "CU2601")
        .cloned()
        .expect("contracts.yaml 含 CU");
    let mut spec = crypto_spec("CU2601");
    spec.exchange = "SHFE".into();
    spec.contract = Some(contract.clone());

    // 北京日盘样本：02:00 UTC = 10:00 起（避开开盘后 5 分钟窗口）
    let warmup = flat_candles(utc(2026, 1, 2, 2, 0), 60, 50_000.0);
    let trading = flat_candles(utc(2026, 1, 5, 2, 0), 30, 50_000.0);
    let run = spi_backtest::run_from_spec(SpiRunArgs {
        spec: &spec,
        strategy: Box::new(OnceThenExit {
            opened: false,
            close_next: false,
        }),
        strategy_name: "once_1H_3_CU2601_BACKTEST",
        warmup: &warmup,
        trading: &trading,
        initial_cash: 1_000_000.0,
        leverage: 1.0,
        fee_rate: None,
        risk: RiskControlConfig::default(),
        state_dir: &dir.path().join("state"),
        data_dir: dir.path(),
    })
    .expect("CTP spi 分支");

    assert_eq!(run.trade_rows.len(), 2);
    // 整数手 + tick 对齐价
    assert_eq!(run.trade_rows[0].quantity.fract(), 0.0, "期货必须整数手");
    let ticks = run.trade_rows[0].price / contract.tick_size;
    assert!((ticks - ticks.round()).abs() < 1e-9, "价格 tick 对齐");
    assert_eq!(run.trade_rows[0].side, "BUY");
    assert_eq!(run.trade_rows[1].side, "SELL_CLOSE");

    // 费率从配置真源推得：开 = 名义×open；当日平 = 名义×close_today
    let notional = run.trade_rows[0].price * run.trade_rows[0].quantity * contract.multiplier;
    assert!(
        (run.trade_rows[0].commission - notional * contract.commission.open).abs() < 1e-6,
        "开仓费口径 {}",
        run.trade_rows[0].commission
    );
    let ct_rate = contract
        .commission
        .close_today
        .unwrap_or(contract.commission.close);
    assert!(
        (run.trade_rows[1].commission - notional * ct_rate).abs() < 1e-6,
        "平今费口径 {}（close_today={}）",
        run.trade_rows[1].commission,
        ct_rate
    );
}

/// 验收 2：单进程 binance + ctp 混跑——同一注册表装配、同一主循环代码，
/// 成交口径互不污染（小数数量 vs 整数手、USDT vs CNY）。
#[test]
fn single_process_mixed_backtest_binance_and_ctp() {
    let entries: std::collections::BTreeMap<String, serde_json::Value> = serde_json::from_value(
        serde_json::json!({
            "binance": {
                "market": "crypto_perpetual",
                "aggregation": "utc_grid",
                "fee_rate": 0.0004,
                "symbols": ["BTCUSDT"]
            },
            "ctp": {
                "market": "cn_futures",
                "aggregation": "session_window",
                "contracts": {
                    "CU": {
                        "multiplier": 5, "tick_size": 10, "margin_rate": 0.11, "min_lots": 1,
                        "commission": { "mode": "rate", "open": 0.00005, "close": 0.00005, "close_today": 0.0001 }
                    }
                }
            }
        }),
    )
    .unwrap();
    let registry = build_registry(&entries).expect("双交易所装配");

    let bin = registry.get_by_name("binance").unwrap();
    let ctp = registry.get_by_name("ctp").unwrap();
    let bin_inst = bin.parse_instrument("BTCUSDT").unwrap();
    let ctp_inst = ctp.parse_instrument("CU2601").unwrap();

    let bin_bars = flat_candles(utc(2026, 1, 5, 0, 0), 15, 60_000.0);
    let ctp_bars = flat_candles(utc(2026, 1, 5, 2, 0), 15, 50_000.0);

    let mut bin_bt = SpiBacktest::assemble(
        &bin,
        SourceRequest::backtest(
            bin_inst.clone(),
            spi_backtest::candles_to_bars(&bin_inst, &bin_bars),
            10_000.0,
        ),
        Box::new(OnceThenExit {
            opened: false,
            close_next: false,
        }),
        SizingParams {
            leverage: 1.0,
            notional_per_order: None,
        },
        RiskControlConfig::default(),
        2000,
    )
    .unwrap();
    let mut ctp_bt = SpiBacktest::assemble(
        &ctp,
        SourceRequest::backtest(
            ctp_inst.clone(),
            spi_backtest::candles_to_bars(&ctp_inst, &ctp_bars),
            1_000_000.0,
        ),
        Box::new(OnceThenExit {
            opened: false,
            close_next: false,
        }),
        SizingParams {
            leverage: 1.0,
            notional_per_order: None,
        },
        RiskControlConfig::default(),
        2000,
    )
    .unwrap();

    let bin_sum = bin_bt.run().unwrap();
    let ctp_sum = ctp_bt.run().unwrap();

    assert_eq!(bin_sum.fills.len(), 2, "crypto 开平");
    assert_eq!(ctp_sum.fills.len(), 2, "ctp 开平");
    assert!(
        bin_sum.fills[0].qty.fract() != 0.0,
        "crypto 小数数量口径: {}",
        bin_sum.fills[0].qty
    );
    assert!(
        ctp_sum.fills.iter().all(|f| f.qty.fract() == 0.0),
        "期货整数手"
    );
    let bin_acct = bin_sum.account.clone().expect("binance 账户");
    let ctp_acct = ctp_sum.account.clone().expect("ctp 账户");
    assert_eq!(bin_acct.currency, "USDT");
    assert_eq!(ctp_acct.currency, "CNY");
    // signal_id 现为 sig_{16hex}（SHA256 哈希），不再带描述性前缀。
    // 验证格式正确且两交易所 signal_id 互不相同（跨交易所不覆盖）。
    assert!(
        bin_sum.fills[0].signal_id.starts_with("sig_"),
        "binance signal_id 应以 sig_ 开头: {}",
        bin_sum.fills[0].signal_id
    );
    assert_eq!(
        bin_sum.fills[0].signal_id.len(),
        20,
        "binance signal_id 应为 sig_ + 16 hex = 20 字符: {}",
        bin_sum.fills[0].signal_id
    );
    assert!(
        ctp_sum.fills[0].signal_id.starts_with("sig_"),
        "ctp signal_id 应以 sig_ 开头: {}",
        ctp_sum.fills[0].signal_id
    );
    assert_eq!(
        ctp_sum.fills[0].signal_id.len(),
        20,
        "ctp signal_id 应为 sig_ + 16 hex = 20 字符: {}",
        ctp_sum.fills[0].signal_id
    );
    assert_ne!(
        bin_sum.fills[0].signal_id, ctp_sum.fills[0].signal_id,
        "两交易所 signal_id 必须不同（跨交易所不覆盖）"
    );
    // 终态全平、两套规则互不污染
    assert!(bin_sum
        .final_positions
        .iter()
        .all(|l| l.remaining <= f64::EPSILON));
    assert!(ctp_sum
        .final_positions
        .iter()
        .all(|l| l.remaining <= f64::EPSILON));
}

// ─────────────────── golden helpers（R3 退役冻结位）───────────────────

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
