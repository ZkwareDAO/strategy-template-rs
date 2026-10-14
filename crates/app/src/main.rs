//! strategy-rs CLI — backtest / run / download 子命令
//!
//! - backtest：1m CSV 回放 → 共用引擎 → backtest_output/{run_id}/（过程产物
//!   进统一 Store 当日快照 state/{day}/{model}.csv，报告产物 result.json/txt/PNG）
//! - run：实盘/paper（Phase 7 接线 kline-service / trading-service 后启用；
//!   当前为 fail-fast，直连交易所代码已移除）
//! - download：币安公共归档离线导入 1m CSV（数据工具，不在交易链路）
//!
//! 策略选择（模板冻结，本文件不再因新增策略而修改）：
//! - 策略实例化走 strategy_strategies::registry（build.rs 自动发现 src/*.rs）
//! - 运行参数来自 strategy.config.yaml（--config 可覆盖路径；不存在 = 纯 CLI）
//! - 优先级：CLI 显式参数 > 配置文件 per-symbol > 配置文件全局 > 默认值

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};

use strategy_app::config::{self, SymbolSpec};
use strategy_app::engine::{Engine, EngineConfig};
use strategy_app::report::ReportGenerator;
use strategy_broker::backtest::{BacktestBroker, BacktestConfig};
use strategy_broker::{BacktestOrderClient, Broker};
use strategy_core::candle::{Candle, Timeframe};
use strategy_core::strategy::Strategy;
use strategy_data::csv_store;

#[derive(Parser)]
#[command(name = "strategy-rs", version, about = "Rust CTA 策略框架")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 回测：1m CSV 逐根回放（与实盘 WS 同一引擎管线）
    Backtest {
        /// 配置文件或配置目录路径（缺省先探测 strategy.config.yaml，再探测 config/ 目录；均不存在则纯 CLI）
        #[arg(long)]
        config: Option<PathBuf>,
        /// 策略名（覆盖配置；可用列表见注册表，未知名字报错时列出）
        #[arg(long)]
        strategy: Option<String>,
        #[arg(long)]
        symbol: Option<String>,
        /// 主周期（如 8h；进入 strategy_name 命名）
        #[arg(long)]
        timeframe: Option<String>,
        /// warmup 起始日期 YYYYMMDD（可选，默认 start 前 30 天）
        #[arg(long)]
        warmup_start: Option<String>,
        /// 回测起始日期 YYYYMMDD
        #[arg(long)]
        start: String,
        /// 回测结束日期 YYYYMMDD
        #[arg(long)]
        end: String,
        /// 1m K 线 CSV 目录
        #[arg(long, default_value = "./data/klines")]
        data_dir: PathBuf,
        /// 输出目录
        #[arg(long, default_value = "./backtest_output")]
        output_dir: PathBuf,
        /// 初始现金（每单名义金额）
        #[arg(long)]
        cash: Option<f64>,
        /// 杠杆
        #[arg(long)]
        leverage: Option<u32>,
    },
    /// 实盘/paper 运行策略
    Run {
        /// 配置文件或配置目录路径（缺省先探测 strategy.config.yaml，再探测 config/ 目录；均不存在则纯 CLI）
        #[arg(long)]
        config: Option<PathBuf>,
        /// 策略名（覆盖配置，不含 per-symbol strategy）
        #[arg(long)]
        strategy: Option<String>,
        /// 交易对（可多个：--symbol BTCUSDT --symbol ETHUSDT；给出时覆盖配置 symbols）
        #[arg(long = "symbol")]
        symbols: Vec<String>,
        /// 主周期（如 8h）
        #[arg(long)]
        timeframe: Option<String>,
        /// 运行模式：live / paper_trading / smoking
        #[arg(long)]
        mode: Option<String>,
        /// 1m K 线 CSV 目录（warmup 来源）
        #[arg(long, default_value = "./data/klines")]
        data_dir: PathBuf,
        /// 输出目录
        #[arg(long, default_value = "./data")]
        output_dir: PathBuf,
        /// 每单名义金额
        #[arg(long)]
        cash: Option<f64>,
        /// 杠杆
        #[arg(long)]
        leverage: Option<u32>,
    },
    /// 下载历史 K 线（Binance 公共数据源：归档包优先，fapi 兜底；无需 API key）
    Download {
        /// 交易对（可多个：--symbol BTCUSDT --symbol ETHUSDT）
        #[arg(long = "symbol")]
        symbols: Vec<String>,
        /// K 线周期（默认 1m；回测按 1m 驱动，大周期由框架自动聚合）
        #[arg(long, default_value = "1m")]
        interval: String,
        /// CSV 不存在时下载最近 N 天（默认 365，即近一年）
        #[arg(long, default_value_t = 365)]
        days: u32,
        /// 起始日期 YYYYMMDD（给出时按区间下载，忽略 --days）
        #[arg(long)]
        start: Option<String>,
        /// 结束日期 YYYYMMDD（不含；缺省为现在）
        #[arg(long)]
        end: Option<String>,
        /// K 线 CSV 根目录（写 {data_dir}/{interval}/{SYMBOL}_{interval}.csv）
        #[arg(long, default_value = "./data/klines")]
        data_dir: PathBuf,
    },
    /// 查看 state 快照 / 任意 CSV（payload_json 展开为可读表格）
    Inspect {
        /// state 目录（如 engine/state/20260825）或单个 CSV 文件
        path: PathBuf,
        /// 只打印前 N 行（默认 50；0 = 全部）
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// JSONL 输出（每行一条记录，域字段平铺）
        #[arg(long)]
        json: bool,
        /// 把扁平 CSV 写入该目录（每输入文件一个 {name}.flat.csv）
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

fn parse_date(s: &str) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
    let d = chrono::NaiveDate::parse_from_str(s, "%Y%m%d")
        .map_err(|e| anyhow::anyhow!("日期格式应为 YYYYMMDD: {s} ({e})"))?;
    use chrono::TimeZone;
    Ok(chrono::Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap()))
}

/// 策略工厂：注册表查询（按 strategy_type 名实例化，私有参数由策略自行解析）
fn make_strategy(spec: &SymbolSpec) -> anyhow::Result<Box<dyn Strategy + Send>> {
    let args = strategy_strategies::registry::FactoryArgs {
        symbol: &spec.symbol,
        timeframe: &spec.timeframe,
        cash: spec.cash,
        params: spec.params.as_ref(),
    };
    strategy_strategies::registry::create(&spec.strategy, &args)
}

fn runtime_name(strategy: &str, timeframe: &str, symbol: &str, mode: &str) -> String {
    // 命名约定 {prefix}_{TF}_{v}_{SYMBOL}_{MODE}
    format!(
        "{}_{}_3_{}_{}",
        strategy,
        timeframe.to_uppercase(),
        symbol.to_uppercase(),
        mode.to_uppercase()
    )
}

/// 回测起始/结束日期 → "YYYY-MM-DD"（result.json config 的 start_date/end_date）
fn format_start(t: chrono::DateTime<chrono::Utc>) -> String {
    t.format("%Y-%m-%d").to_string()
}

/// 读取回测生效的配置文本（复制进报告目录 config.yaml；找不到 → None）
fn read_config_yaml(config_path: Option<&std::path::Path>) -> anyhow::Result<Option<String>> {
    let candidates: Vec<PathBuf> = match config_path {
        Some(p) => vec![p.to_path_buf()],
        None => config::DEFAULT_CONFIG_PATHS
            .iter()
            .map(PathBuf::from)
            .chain(std::iter::once(PathBuf::from(config::DEFAULT_CONFIG_DIR)))
            .collect(),
    };
    for p in candidates {
        if p.is_file() {
            return Ok(Some(std::fs::read_to_string(&p)?));
        }
        if p.is_dir() {
            // 目录形式（config/{strategy}/{SYMBOL}.yaml）：合并所有 YAML
            let mut files: Vec<PathBuf> = Vec::new();
            collect_yaml(&p, &mut files)?;
            files.sort();
            if files.is_empty() {
                continue;
            }
            let mut merged = String::new();
            for f in &files {
                if !merged.is_empty() {
                    merged.push_str("\n---\n");
                }
                merged.push_str(&std::fs::read_to_string(f)?);
            }
            return Ok(Some(merged));
        }
    }
    Ok(None)
}

fn collect_yaml(dir: &std::path::Path, out: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let p = e.path();
        if p.is_dir() {
            collect_yaml(&p, out)?;
        } else if matches!(p.extension().and_then(|e| e.to_str()), Some("yaml" | "yml")) {
            out.push(p);
        }
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    // 自动加载工作目录 .env（缺失不报错；密钥等本地变量随后均可见）
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Backtest {
            config: config_path,
            strategy,
            symbol,
            timeframe,
            warmup_start,
            start,
            end,
            data_dir,
            output_dir,
            cash,
            leverage,
        } => {
            let file_cfg = config::load(config_path.as_deref())?;
            let spec = config::resolve_backtest(
                &file_cfg,
                symbol.as_deref(),
                strategy.as_deref(),
                timeframe.as_deref(),
                cash,
            )?;
            let leverage = leverage
                .or(file_cfg.leverage)
                .unwrap_or(config::Defaults::LEVERAGE);

            let start_ts = parse_date(&start)?;
            let end_ts = parse_date(&end)?;
            anyhow::ensure!(end_ts > start_ts, "end 必须晚于 start");

            // warmup 起点：start 前 30 天（镜像 Python sync_history_days=30）
            let warmup_ts = match warmup_start {
                Some(w) => parse_date(&w)?,
                None => start_ts - chrono::Duration::days(30),
            };

            // 读取 1m CSV（[warmup, end)）
            let all = csv_store::load_or_empty(&data_dir, &spec.symbol, Timeframe::M1)?;
            let replay: Vec<Candle> = all
                .into_iter()
                .filter(|c| c.open_time >= warmup_ts && c.open_time < end_ts)
                .collect();
            let warmup: Vec<Candle> = replay
                .iter()
                .filter(|c| c.open_time < start_ts)
                .copied()
                .collect();
            let trading: Vec<Candle> = replay
                .iter()
                .filter(|c| c.open_time >= start_ts)
                .copied()
                .collect();
            anyhow::ensure!(!trading.is_empty(), "回测区间无 1m 数据: {data_dir:?}");

            let run_id = format!("{}_{}_{start}_{end}", spec.strategy, spec.symbol);
            let name = runtime_name(&spec.strategy, &spec.timeframe, &spec.symbol, "backtest");

            // Python 格式报告目录（{strategy}/{YYYYMMDD}/{HHMMSS}/{SYMBOL}/）。
            // 时间戳取回测区间 end（对齐 Python use_today_as_output_date=false 语义，
            // 同参数重跑落同一目录，可复现 diff）
            let bt_start_time = chrono::Utc::now();
            let end_yyyymmdd = end_ts.format("%Y%m%d").to_string();
            let run_stamp = format!(
                "{}T{}",
                end_ts.format("%Y%m%d"),
                bt_start_time.format("%H%M%S")
            );
            let run_dir = output_dir
                .join(&spec.strategy)
                .join(&end_yyyymmdd)
                .join(&run_stamp)
                .join(&spec.symbol);

            // out_base = {run_dir}/engine/：过程产物（signal.csv / exchange_orders.csv /
            // state/）与结果报告（equity/report/charts）分层；纯生成物清空重建，
            // 保证重跑同参数 = 全新回测（signal.csv 追加写是实盘语义，
            // 回测残留旧行会破坏确定性 diff 验收）。
            let out_base = run_dir.join("engine");
            if out_base.exists() {
                std::fs::remove_dir_all(&out_base)?;
            }

            let cfg = EngineConfig {
                strategy_name: name.clone(),
                strategy_type: spec.strategy.clone(),
                symbol: spec.symbol.clone(),
                trading_mode: "backtest".into(),
                user_id: 1,
                leverage,
                cash: spec.cash,
                exchange: "binance".into(),
                max_closed_len: 2000,
                risk: file_cfg.risk.clone().unwrap_or_default(),
                signal_tf: spec.timeframe.clone(),
            };
            let strat = make_strategy(&spec)?;
            // 回测恒用本地撮合（确定性），经 OrderGateway 端口进信号管道。
            // 全仓语义：notional_per_order = 0 → 每次开仓用全部可用现金×杠杆。
            let broker_cfg = BacktestConfig {
                leverage,
                notional_per_order: 0.0,
                exchange: "binance".into(),
                ..Default::default()
            };
            let backtest = BacktestBroker::new(broker_cfg, &name, &out_base);
            // 报告初始资金 = 撮合内核实际起始现金（BacktestConfig.cash），
            // 不是 spec.cash（那是每单名义金额）——口径不一致会让收益率失真
            let initial_cash = backtest.account().map(|a| a.cash).unwrap_or(spec.cash);
            let client = BacktestOrderClient::new(backtest);
            let mut engine = Engine::new(cfg, strat, client, out_base.join("state"), &out_base);
            // 回测每次从零开始：不从 Store 恢复持仓/冷却日（那是实盘崩溃恢复语义），
            // 否则重跑同目录会带着上一轮的残留仓位，整段回测"持仓中"不开新仓。
            engine.preload(&warmup);

            for c in &trading {
                engine.on_closed_1m(*c)?;
                engine.on_equity_mark(c);
            }
            // 回测结束统一落信号当前态快照（K 线时钟，确定性；非 live 阻塞池纪律）
            if let Err(e) = engine.flush_state() {
                tracing::warn!("回测结束信号状态 flush 失败: {e}");
            }

            let stats = engine.stats();

            // 回测主循环已喂 on_equity_mark；generator 只负责写文件
            let reporter = ReportGenerator::new(&run_dir);
            let config_yaml = read_config_yaml(config_path.as_deref())?;
            reporter
                .generate(
                    &spec.strategy,
                    &spec.symbol,
                    &format_start(start_ts),
                    &format_start(end_ts),
                    initial_cash,
                    &data_dir,
                    &spec.timeframe,
                    &engine.daily_equity(),
                    &engine.tf_equity(),
                    &engine.trade_rows(),
                    stats.bars,
                    bt_start_time,
                    chrono::Utc::now(),
                    config_yaml.as_deref(),
                )
                .map_err(|e| anyhow::anyhow!("报告生成失败: {e}"))?;

            println!("回测完成: {run_id}");
            println!(
                "bars={} signals={} orders={} opened={} closed={}",
                stats.bars,
                stats.signals_written,
                stats.orders_written,
                stats.positions_opened,
                stats.positions_closed
            );
            println!("输出目录: {out_base:?}");
            println!("报告目录: {run_dir:?}");
            Ok(())
        }
        Commands::Download {
            symbols,
            interval,
            days,
            start,
            end,
            data_dir,
        } => {
            anyhow::ensure!(!symbols.is_empty(), "必须提供至少一个 --symbol");
            let tf = Timeframe::parse(&interval)?;
            // 区间：--start/--end 显式给出 > （CSV 不存在时）now - days > CSV 末根
            let end_ts = match end {
                Some(e) => parse_date(&e)?,
                None => chrono::Utc::now(),
            };
            let start_opt = match start {
                Some(s) => Some(parse_date(&s)?),
                None => None,
            };
            for symbol in &symbols {
                let symbol = symbol.to_uppercase();
                let start_ts = match start_opt {
                    Some(s) => s,
                    None => {
                        // 增量语义：现有 CSV 末根为起点（末根本身重拉，残缺值可被覆盖）；
                        // 无 CSV 时按 --days 全量
                        let path = csv_store::csv_path(&data_dir, &symbol, tf);
                        let existing = csv_store::load_csv(&path)?;
                        match existing.last() {
                            Some(last) => {
                                tracing::info!(
                                    symbol, %tf,
                                    existing = existing.len(),
                                    last = %last.open_time,
                                    "增量补齐至 now"
                                );
                                last.open_time
                            }
                            None => end_ts - chrono::Duration::days(days as i64),
                        }
                    }
                };
                if start_ts >= end_ts {
                    tracing::info!(symbol, %tf, "已是最新，无需下载");
                    continue;
                }
                let dl = strategy_data::archive_download::Downloader::new();
                let rt = tokio::runtime::Runtime::new()?;
                let total =
                    rt.block_on(dl.download_range(&symbol, tf, start_ts, end_ts, &data_dir))?;
                println!(
                    "{symbol} {interval}: 共 {total} 根 → {}",
                    csv_store::csv_path(&data_dir, &symbol, tf).display()
                );
            }
            Ok(())
        }
        Commands::Inspect {
            path,
            limit,
            json,
            out,
        } => {
            let paths: Vec<PathBuf> = if path.is_dir() {
                let mut v: Vec<_> = std::fs::read_dir(&path)?
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("csv"))
                    .collect();
                v.sort();
                v
            } else {
                vec![path.clone()]
            };
            anyhow::ensure!(!paths.is_empty(), "无 CSV 文件: {:?}", path);
            for p in &paths {
                let rows = strategy_app::inspect::read_csv(p)?;
                let name = p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("csv");
                if let Some(dir) = &out {
                    std::fs::create_dir_all(dir)?;
                    let stem = p
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("out");
                    let out_path = dir.join(format!("{stem}.flat.csv"));
                    strategy_app::inspect::write_flat_csv(&out_path, &rows)?;
                    println!("已写出: {}", out_path.display());
                } else if json {
                    strategy_app::inspect::print_jsonl(&rows, limit);
                } else {
                    println!("══ {name} ══");
                    strategy_app::inspect::print(&rows, limit);
                }
            }
            Ok(())
        }
        Commands::Run {
            config: config_path,
            strategy,
            symbols,
            timeframe,
            mode,
            cash,
            output_dir,
            leverage: _,
            data_dir,
        } => {
            let file_cfg = config::load(config_path.as_deref())?;
            let specs = config::resolve_run(
                &file_cfg,
                &symbols,
                strategy.as_deref(),
                timeframe.as_deref(),
                cash,
                mode.as_deref(),
            )?;
            // mode 合法性已在 resolve_run 逐 spec 校验；网关按 spec.mode 分流
            let services = file_cfg.services.clone().ok_or_else(|| {
                anyhow::anyhow!("run 需要配置文件中的 services.kline / services.trading 端点")
            })?;
            std::fs::create_dir_all(&output_dir)?;
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            rt.block_on(strategy_app::bootstrap::run_live(
                specs,
                services,
                output_dir,
                data_dir,
                Duration::from_secs(file_cfg.state_flush_interval_secs),
                file_cfg.state_prune_terminal,
            ))?;
            Ok(())
        }
    }
}
