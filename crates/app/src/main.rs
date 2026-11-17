//! strategy-rs CLI — backtest / run / download 子命令
//!
//! - backtest：1m CSV 回放 → 共用引擎 → backtest_output/{run_id}/（过程产物
//!   进统一 Store 当日快照 state/{day}/{model}.csv，报告产物 result.json/txt/PNG）
//! - run：实盘/paper（trading-service 单连接：订单回报与 K 线订阅共用；
//!   直连交易所代码已移除，klines-service 已退役）
//! - download：币安公共归档离线导入 1m CSV（数据工具，不在交易链路）
//!
//! 策略选择（模板冻结，本文件不再因新增策略而修改）：
//! - 策略实例化走 strategy_strategies::registry（build.rs 自动发现 src/*.rs）
//! - 运行参数来自 strategy.config.yaml（--config 可覆盖路径；不存在 = 纯 CLI）
//! - 优先级：CLI 显式参数 > 配置文件 per-symbol > 配置文件全局 > 默认值

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};

use strategy_app::spi_backtest;

use strategy_app::report::ReportGenerator;
use strategy_core::candle::Timeframe;
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
        /// 每单名义金额（crypto/兼容模式；国内期货按合约保证金率决定手数）
        #[arg(long)]
        cash: Option<f64>,
        /// 回测账户初始权益（缺省 5000；国内期货建议显式配置，如 1000000）
        #[arg(long)]
        initial_cash: Option<f64>,
        /// 杠杆
        #[arg(long)]
        leverage: Option<u32>,
        /// 单边手续费率（仅 crypto/兼容模式；期货手续费读 config/contracts.yaml）
        #[arg(long)]
        fee_rate: Option<f64>,
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
        /// K 线 CSV 根目录（写 {data_dir}/{interval}/binance/crypto_perpetual/{SYMBOL}_{interval}.csv）
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

/// 回测起始/结束日期 → "YYYY-MM-DD"（result.json config 的 start_date/end_date）
fn format_start(t: chrono::DateTime<chrono::Utc>) -> String {
    t.format("%Y-%m-%d").to_string()
}

/// 横截面回测 CLI 参数包（避免 9 参函数）。
struct StrategyBacktestCli {
    config_path: std::path::PathBuf,
    start: String,
    end: String,
    warmup_start: Option<String>,
    data_dir: PathBuf,
    output_dir: PathBuf,
    initial_cash: Option<f64>,
    fee_rate: Option<f64>,
}

/// 横截面回测 CLI 装配（PR-4）：strategy.yaml → 每标的 1m CSV → RunnerGroup
/// 统一主循环 → 组合净值/成交报告。`--strategy/--symbol/--cash` 已在入口拒绝。
fn run_strategy_backtest(cli: StrategyBacktestCli) -> anyhow::Result<()> {
    use strategy_app::strategy_config;

    let StrategyBacktestCli {
        config_path,
        start,
        end,
        warmup_start,
        data_dir,
        output_dir,
        initial_cash,
        fee_rate,
    } = cli;
    let spec = strategy_config::load_strategy(&config_path)?;
    let start_ts = parse_date(&start)?;
    let end_ts = parse_date(&end)?;
    anyhow::ensure!(end_ts > start_ts, "end 必须晚于 start");
    let warmup_ts = match warmup_start {
        Some(w) => parse_date(&w)?,
        None => start_ts - chrono::Duration::days(spec.warmup_days as i64),
    };
    let mut warmup: Vec<Vec<strategy_core::candle::Candle>> = Vec::new();
    let mut trading: Vec<Vec<strategy_core::candle::Candle>> = Vec::new();
    for ispec in &spec.instruments {
        let all = spi_backtest::csv_1m_candles(
            &data_dir,
            &ispec.venue_key,
            Some(warmup_ts),
            Some(end_ts),
        );
        anyhow::ensure!(
            all.iter().any(|c| c.open_time >= start_ts),
            "回测区间无 1m 数据：行键 `{}`（区间 {start}..{end}）；先 download 或修正 universe",
            ispec.venue_key
        );
        warmup.push(
            all.iter()
                .filter(|c| c.open_time < start_ts)
                .copied()
                .collect(),
        );
        trading.push(
            all.into_iter()
                .filter(|c| c.open_time >= start_ts)
                .collect(),
        );
    }
    // 与 live 装配同口径：初始总额 = Σ 各标的 initial_cash（未配置按 5000）。
    // 配置加载已把 _global.initial_cash 按标的等分，取首标的会只剩 1/N。
    let initial_cash = initial_cash.unwrap_or_else(|| {
        spec.instruments
            .iter()
            .map(|s| s.initial_cash.unwrap_or(5000.0))
            .sum()
    });
    let strategy = strategy_config::build_strategy(&spec)?;

    let run_id = format!(
        "{}_{}_{start}_{end}",
        spec.strategy_type,
        spec.strategy_id.replace(':', "_")
    );
    let now_local = chrono::Local::now();
    let run_dir = output_dir
        .join(&spec.strategy_type)
        .join(now_local.format("%Y%m%d").to_string())
        .join(now_local.format("%H%M%S").to_string())
        .join(spec.strategy_id.replace(':', "_"));
    let out_base = run_dir.join("engine");
    if out_base.exists() {
        std::fs::remove_dir_all(&out_base)?;
    }
    let config_yaml = std::fs::read_to_string(&config_path).ok();

    let bt_start = chrono::Utc::now();
    let run = spi_backtest::run_strategy_from_spec(spi_backtest::StrategyRunArgs {
        spec: &spec,
        strategy,
        warmup: &warmup,
        trading: &trading,
        initial_cash,
        fee_rate,
        state_dir: &out_base.join("state"),
        data_dir: &data_dir,
    })?;
    let base_tf = spec
        .config
        .timeframes
        .first()
        .cloned()
        .unwrap_or_else(|| "15m".into());
    let reporter = ReportGenerator::new(&run_dir);
    reporter.generate(
        &spec.strategy_id,
        &spec.strategy_id,
        &format_start(start_ts),
        &format_start(end_ts),
        run.initial_cash,
        &data_dir,
        "strategy",
        &base_tf,
        &run.daily,
        &run.tf_points,
        &run.trade_rows,
        run.summary.bars_processed as usize,
        bt_start,
        chrono::Utc::now(),
        config_yaml.as_deref(),
    )?;
    println!("横截面回测完成: {run_id}");
    println!(
        "bars={} fills={} nav_final={:.4} rebalance_epoch={}",
        run.summary.bars_processed,
        run.summary.fills.len(),
        run.summary.final_portfolio.nav,
        run.summary.final_portfolio.rebalance_epoch,
    );
    println!("输出目录: {out_base:?}");
    println!("报告目录: {run_dir:?}");
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
            timeframe: _timeframe,
            warmup_start,
            start,
            end,
            data_dir,
            output_dir,
            cash,
            initial_cash,
            leverage: _leverage,
            fee_rate,
        } => {
            // ── 横截面配置分流（显式 --config 指向 strategy.yaml/目录时；旧路径零改动）──
            if let Some(p) = config_path.as_deref() {
                if strategy_app::strategy_config::is_strategy_config_path(p) {
                    if strategy.is_some() || symbol.is_some() || cash.is_some() {
                        anyhow::bail!(
                            "横截面配置（strategy.yaml）不支持 --strategy/--symbol/--cash 覆盖项：\
                             请写进 strategy.yaml 的 universe/sizing 段（设计 §5.1）"
                        );
                    }
                    return run_strategy_backtest(StrategyBacktestCli {
                        config_path: p.to_path_buf(),
                        start,
                        end,
                        warmup_start,
                        data_dir,
                        output_dir,
                        initial_cash,
                        fee_rate,
                    });
                }
                if strategy_app::strategy_config::is_strategy_set_dir(p) {
                    anyhow::bail!(
                        "横截面配置集（多组）仅支持 run：backtest 请逐组用 --config <组目录>/strategy.yaml"
                    );
                }
            }
            anyhow::bail!(
                "backtest 仅支持 strategy.yaml / 配置集：旧 per-symbol 入口已退役；请用 --config <组>/strategy.yaml"
            );
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
                // 数据源现仅 Binance 公共归档：命名空间键按
                // binance/crypto_perpetual 派生（§6.1；多交易所下载随 live 面演进）
                let venue_key = strategy_app::spi_backtest::venue_key_for("binance", None, &symbol);
                let start_ts = match start_opt {
                    Some(s) => s,
                    None => {
                        // 增量语义：现有 CSV 末根为起点（末根本身重拉，残缺值可被覆盖）；
                        // 无 CSV 时按 --days 全量。resolve 兼容旧布局（一次迁移）
                        let path = csv_store::resolve_csv_path(&data_dir, &venue_key, tf);
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
                let total = rt.block_on(
                    dl.download_range(&symbol, &venue_key, tf, start_ts, end_ts, &data_dir),
                )?;
                println!(
                    "{symbol} {interval}: 共 {total} 根 → {}",
                    csv_store::csv_path(&data_dir, &venue_key, tf).display()
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
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("csv");
                if let Some(dir) = &out {
                    std::fs::create_dir_all(dir)?;
                    let stem = p.file_stem().and_then(|s| s.to_str()).unwrap_or("out");
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
            // 横截面 run（PR-5 T6）：新 schema 走独立 loader/actor，在旧 loader 之前分流。
            if let Some(p) = config_path.as_deref() {
                if strategy_app::strategy_config::is_strategy_config_path(p)
                    || strategy_app::strategy_config::is_strategy_set_dir(p)
                {
                    // 单标的覆盖参数不支持（universe 由 strategy.yaml 决定；文案与
                    // backtest 分支同款）。
                    if !symbols.is_empty()
                        || strategy.is_some()
                        || timeframe.is_some()
                        || cash.is_some()
                    {
                        anyhow::bail!(
                            "横截面 run 不支持 --strategy/--symbol/--timeframe/--cash 覆盖（universe 由 strategy.yaml 决定）：{}",
                            p.display()
                        );
                    }
                    let specs = strategy_app::strategy_config::load_strategy_set(p)?;
                    let modes: Vec<String> = specs
                        .iter()
                        .map(|s| {
                            strategy_app::strategy_config::resolve_run_mode(mode.as_deref(), s)
                        })
                        .collect::<anyhow::Result<_>>()?;
                    let run_mode = modes[0].clone();
                    anyhow::ensure!(
                        modes.iter().all(|m| m == &run_mode),
                        "横截面配置集各组 mode 解析结果不一致：{}（各组文件 mode/_global 基线须一致；也可用 --mode 统一覆盖）",
                        modes.join(",")
                    );
                    strategy_app::strategy_config::validate_strategy_set(&specs, &run_mode)?;
                    let services = specs[0].services.clone().ok_or_else(|| {
                        anyhow::anyhow!(
                            "横截面 run 需要 _global.yaml 的 services.trading 端点（K 线与交易共用同一连接；klines-service 已退役）"
                        )
                    })?;
                    strategy_app::config::check_kline_retired(&services)?;
                    // 占位端点守卫（与单标的 run 同口径）：live 拒启，paper 出声。
                    if services.trading.is_unconfigured() {
                        if run_mode == "live" {
                            anyhow::bail!(
                                "横截面 run 拒绝启动：mode=live，但 services.trading.ws_url 仍是占位值「{}」。\n\
                                 请配置真实 trading-service 端点；若本次只想停单观察，请配真实端点并显式置\n\
                                 services.trading.submit_enabled: false（docs/RUNBOOK.md §2.1）",
                                strategy_app::config::PLACEHOLDER_TRADING_WS_URL
                            );
                        }
                        tracing::warn!(
                            ws_url = strategy_app::config::PLACEHOLDER_TRADING_WS_URL,
                            "services.trading 未配置真实端点：横截面运行不会产生任何真实下单（paper 属预期）"
                        );
                    }
                    std::fs::create_dir_all(&output_dir)?;
                    let flush_secs = specs[0].state_flush_interval_secs;
                    let prune = specs[0].state_prune_terminal;
                    let rt = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()?;
                    rt.block_on(strategy_app::bootstrap::run_live_strategy_set(
                        specs,
                        services,
                        output_dir,
                        data_dir,
                        run_mode,
                        Duration::from_secs(flush_secs),
                        prune,
                    ))?;
                    return Ok(());
                }
            }
            anyhow::bail!(
                "run 仅支持 strategy.yaml / 配置集：旧 per-symbol 入口已退役；请用 --config <组>/strategy.yaml"
            );
        }
    }
}
