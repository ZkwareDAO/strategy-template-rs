//! 策略配置 loader（设计 §5，PR-2）。
//!
//! 新 schema = 策略级单文件 `config/<strategy>/strategy.yaml`（§5.1）；
//! 旧 `{strategy}/{SYMBOL}.yaml` 路径由 [`crate::config`] 原样继续服务
//! （双读 = 两套 loader 并存，旧行为零改动）。本模块：
//! - [`is_strategy_config_path`]：`--config` 指向新 schema 的判据（组合根分流用）；
//! - [`load_strategy`]：解析 + `validate_config` + universe → `SymbolSpec` 列表
//!   （单一 exchange、venue_key 与 engine `InstrumentId::namespaced_key()` 同键）；
//! - [`build_strategy`]：统一注册表 `strategy_strategies::registry::create`
//!   （单标与横截面同一套接口，N=1 不再经 Adapter）。

use std::path::{Path, PathBuf};

use anyhow::anyhow;
use strategy_core::risk::{RiskControlConfig, TrailingProfitConfig};
use strategy_engine::strategy::config::{validate_config, StrategyConfig};
use strategy_engine::strategy::port::CrossSectionalStrategyPort;

use crate::config::{contract_for, load as load_file_config, Defaults, FileConfig, SymbolSpec};
use crate::spi_backtest::venue_key_for;

/// 横截面策略级文件保留名（stem == `strategy`，非 per-symbol 配置）。
pub const STRATEGY_FILE_STEM: &str = "strategy";

/// 合并后的策略运行规格。
#[derive(Debug, Clone)]
pub struct StrategySpec {
    pub strategy_type: String,
    pub strategy_id: String,
    pub config: StrategyConfig,
    /// 每 universe 条目一条（同序；exchange/account/venue_key/contract 已解析）。
    pub instruments: Vec<SymbolSpec>,
    pub warmup_days: u32,
    /// 运行模式基线（`live|paper_trading|smoking`；来源 `_global.yaml` `mode:`，
    /// 装配期解析；CLI `--mode` 覆盖走 [`resolve_run_mode`]）。
    pub run_mode: String,
    /// `_global.yaml` 的 `services:` 节（`run` 分支取 trading 端点）。
    pub services: Option<crate::config::ServicesConfig>,
    /// 状态 flush 周期（`_global.yaml`；默认 60s）。
    pub state_flush_interval_secs: u64,
    /// 跨日是否裁剪终态行（`_global.yaml`）。
    pub state_prune_terminal: bool,
}

/// 横截面 run 的模式解析：CLI `--mode` > 策略/全局基线 `mode:` > 默认
/// `paper_trading`；`smoking` 仅属旧单标的演练模式，横截面拒启。
pub fn resolve_run_mode(cli: Option<&str>, spec: &StrategySpec) -> anyhow::Result<String> {
    let mode = cli
        .map(str::to_string)
        .filter(|m| !m.trim().is_empty())
        .or_else(|| (!spec.run_mode.trim().is_empty()).then(|| spec.run_mode.clone()))
        .unwrap_or_else(|| "paper_trading".to_string());
    match mode.as_str() {
        "live" | "paper_trading" => Ok(mode),
        "smoking" => Err(anyhow!(
            "横截面 run 不支持 mode=smoking（仅 live / paper_trading）：smoking 属旧单标的演练模式"
        )),
        other => Err(anyhow!(
            "横截面 run 非法 mode `{other}`（仅 live / paper_trading）"
        )),
    }
}

/// 单 account 防御位（一 actor 一网关）：当前 schema 全标的同源必一致；
/// 未来 entry 级 account 放开时此守卫生效。
pub fn ensure_single_account(pairs: &[(String, String)]) -> Result<(), String> {
    let Some((first_sym, first_acct)) = pairs.first() else {
        return Ok(());
    };
    for (sym, acct) in pairs {
        if acct != first_acct {
            return Err(format!(
                "横截面实例只允许单一 account：{first_sym}={first_acct}, {sym}={acct}                 （一 actor 一网关；多 account 需拆实例）"
            ));
        }
    }
    Ok(())
}

/// 显式 `--config` 是否指向横截面配置：文件 stem == `strategy`（yaml/yml），
/// 或目录直接包含 `strategy.yaml`/`strategy.yml`（不递归——递归会把旧
/// 目录模式的兄弟策略目录误判进来）。
pub fn is_strategy_config_path(path: &Path) -> bool {
    if path.is_dir() {
        return strategy_yaml_in(path).is_some();
    }
    matches!(
        (
            path.file_stem().and_then(|s| s.to_str()),
            path.extension().and_then(|s| s.to_str())
        ),
        (Some(STRATEGY_FILE_STEM), Some("yaml") | Some("yml"))
    )
}

fn strategy_yaml_in(dir: &Path) -> Option<PathBuf> {
    ["strategy.yaml", "strategy.yml"]
        .iter()
        .map(|n| dir.join(n))
        .find(|p| p.is_file())
}

/// 配置集的直接子目录（跳过隐藏条目；文件与隐藏目录不参与分组）。
fn group_dirs(path: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(path) else {
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        // 用 metadata 跟随符号链接：运维常以链接迁移组目录，静默漏载
        // 会让该组不交易；断链按非目录处理。
        .filter(|e| {
            std::fs::metadata(e.path())
                .map(|m| m.is_dir())
                .unwrap_or(false)
        })
        .filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
        .map(|e| e.path())
        .collect();
    dirs.sort();
    dirs
}

/// `--config` 是否指向配置集目录：目录本身无顶层 `strategy.yaml`，
/// 且至少一个直接子目录含 `strategy.yaml`。
pub fn is_strategy_set_dir(path: &Path) -> bool {
    path.is_dir()
        && strategy_yaml_in(path).is_none()
        && group_dirs(path)
            .iter()
            .any(|d| strategy_yaml_in(d).is_some())
}

/// 加载单组配置或配置集目录：直接子目录按目录名排序，每个子目录一组；
/// 子目录缺 `strategy.yaml` 即点名拒启（不静默跳过，避免漏跑一组）。
pub fn load_strategy_set(path: &Path) -> anyhow::Result<Vec<StrategySpec>> {
    if is_strategy_config_path(path) {
        return Ok(vec![load_strategy(path)?]);
    }
    let dirs = group_dirs(path);
    if dirs.is_empty() {
        anyhow::bail!(
            "配置集目录下没有含 strategy.yaml 的子目录: {}",
            path.display()
        );
    }
    let bad: Vec<String> = dirs
        .iter()
        .filter(|d| strategy_yaml_in(d).is_none())
        .map(|d| d.display().to_string())
        .collect();
    if !bad.is_empty() {
        anyhow::bail!(
            "配置集子目录缺少 strategy.yaml（每个子目录必须是一组配置）: {}",
            bad.join(", ")
        );
    }
    let mut specs = Vec::with_capacity(dirs.len());
    for dir in dirs {
        specs.push(load_strategy(&dir)?);
    }
    Ok(specs)
}

fn same_services(
    a: Option<&crate::config::ServicesConfig>,
    b: Option<&crate::config::ServicesConfig>,
) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            a.kline.is_some() == b.kline.is_some()
                && a.trading.ws_url == b.trading.ws_url
                && a.trading.request_timeout_ms == b.trading.request_timeout_ms
                && a.trading.submit_enabled == b.trading.submit_enabled
                && a.trading.reconnect_backoff_max_secs == b.trading.reconnect_backoff_max_secs
                && a.trading.strategy_secret == b.trading.strategy_secret
                && a.trading.exchange == b.trading.exchange
                && a.trading.account == b.trading.account
        }
        _ => false,
    }
}

/// 跨组校验：同进程共享一条交易连接与一个 Store，故 account /
/// services / flush 策略必须一致；`run_mode` 是调用方按各组
/// [`resolve_run_mode`] 统一后的有效模式（CLI `--mode` 覆盖已生效），
/// 仅用于 live 组间 universe 不相交检查——同一账户同一标的的远程持仓只有一份。
/// 文件级 mode 基线不在此比较：未覆盖时调用方已按解析结果拒启。
pub fn validate_strategy_set(specs: &[StrategySpec], run_mode: &str) -> anyhow::Result<()> {
    let mut errors: Vec<String> = Vec::new();
    let mut seen_ids: std::collections::BTreeMap<&str, usize> = Default::default();
    for (idx, spec) in specs.iter().enumerate() {
        if seen_ids.insert(spec.strategy_id.as_str(), idx).is_some() {
            errors.push(format!("strategy_id 重复: {}", spec.strategy_id));
        }
    }
    if let Some(first) = specs.first() {
        let first_exchange = first
            .instruments
            .first()
            .map(|i| i.exchange.as_str())
            .unwrap_or("");
        let first_account = first
            .instruments
            .first()
            .map(|i| i.account.as_str())
            .unwrap_or("");
        for spec in specs.iter().skip(1) {
            if let Some(inst) = spec
                .instruments
                .iter()
                .find(|i| i.exchange != first_exchange || i.account != first_account)
            {
                errors.push(format!(
                    "组 {} 的 {} exchange/account={}/{} 与其他组不一致（基线 {}/{}）",
                    spec.strategy_id,
                    inst.symbol,
                    inst.exchange,
                    inst.account,
                    first_exchange,
                    first_account
                ));
            }
            if !same_services(first.services.as_ref(), spec.services.as_ref()) {
                errors.push(format!(
                    "组 {} 的 services.trading 与其他组不一致（同进程共享一条交易连接）",
                    spec.strategy_id
                ));
            }
            if spec.state_flush_interval_secs != first.state_flush_interval_secs {
                errors.push(format!(
                    "state_flush_interval_secs 不一致：{}={} vs {}={}",
                    first.strategy_id,
                    first.state_flush_interval_secs,
                    spec.strategy_id,
                    spec.state_flush_interval_secs
                ));
            }
            if spec.state_prune_terminal != first.state_prune_terminal {
                errors.push(format!(
                    "state_prune_terminal 不一致：{}={} vs {}={}",
                    first.strategy_id,
                    first.state_prune_terminal,
                    spec.strategy_id,
                    spec.state_prune_terminal
                ));
            }
        }
    }
    if run_mode == "live" {
        let mut owners: std::collections::BTreeMap<&str, &str> = Default::default();
        for spec in specs {
            for inst in &spec.instruments {
                if let Some(prev) = owners.insert(inst.symbol.as_str(), spec.strategy_id.as_str()) {
                    errors.push(format!(
                        "{} 同时出现在组 {} 与 {}：live 模式下同一账户同一标的远程持仓只有一份（paper 可重叠）",
                        inst.symbol, prev, spec.strategy_id
                    ));
                }
            }
        }
    }
    if !errors.is_empty() {
        anyhow::bail!("横截面配置集校验失败:\n- {}", errors.join("\n- "));
    }
    Ok(())
}

fn normalize_exchange(raw: &str) -> String {
    let lower = raw.trim().to_ascii_lowercase();
    match lower.as_str() {
        "ctp" | "shfe" => "SHFE".to_string(),
        other => other.to_string(),
    }
}

/// 全局基线：策略文件所在目录 → 其父目录依次探测 `_global.yaml`。
fn global_for(base_dir: &Path) -> anyhow::Result<FileConfig> {
    let mut dirs: Vec<PathBuf> = vec![base_dir.to_path_buf()];
    if let Some(parent) = base_dir.parent() {
        dirs.push(parent.to_path_buf());
    }
    for dir in dirs {
        if dir.as_os_str().is_empty() {
            continue;
        }
        for name in ["_global.yaml", "_global.yml"] {
            let cand = dir.join(name);
            if cand.is_file() {
                return load_file_config(Some(&cand));
            }
        }
    }
    Ok(FileConfig::default())
}

/// L1 仅硬止损（横截面/组合式装配：回落/固定止盈职责归 L3，§7.1）。
fn l1_stop_only(global: &FileConfig) -> RiskControlConfig {
    let base = global.risk.clone().unwrap_or_default();
    RiskControlConfig {
        enabled: true,
        fixed_stop_loss_pct: base.fixed_stop_loss_pct,
        trailing_profit: TrailingProfitConfig {
            enabled: false,
            ..Default::default()
        },
        fixed_take_profit_pct: 0.0,
    }
}

/// 解析并校验横截面配置（文件直读；目录内找 `strategy.yaml`）。
pub fn load_strategy(path: &Path) -> anyhow::Result<StrategySpec> {
    let (strategy_file, base_dir) = if path.is_dir() {
        let f = strategy_yaml_in(path).ok_or_else(|| {
            anyhow!(
                "目录 {} 下没有 strategy.yaml（横截面策略级单文件）",
                path.display()
            )
        })?;
        (f.clone(), f.parent().unwrap_or(path).to_path_buf())
    } else {
        (
            path.to_path_buf(),
            path.parent().map(Path::to_path_buf).unwrap_or_default(),
        )
    };
    let text = std::fs::read_to_string(&strategy_file)
        .map_err(|e| anyhow!("读取横截面配置失败: {} ({e})", strategy_file.display()))?;
    let config: StrategyConfig = serde_yaml::from_str(&text)
        .map_err(|e| anyhow!("{}: 横截面 schema 解析失败: {e}", strategy_file.display()))?;
    if let Err(errs) = validate_config(&config) {
        return Err(anyhow!("横截面配置校验失败:\n- {}", errs.join("\n- ")));
    }
    let global = global_for(&base_dir)?;
    if config.risk.max_drawdown.is_some() {
        tracing::warn!("L4 NAV 回撤层推迟：risk.max_drawdown 解析但忽略（§7.4）");
    }
    tracing::info!(
        summary = %strategy_engine::strategy::config::resolved_rule_summary(&config),
        "横截面 merged 条件集（装配期审计）"
    );

    let strategy_type = config.strategy_type.clone();
    let strategy_id = config
        .strategy_id
        .clone()
        .unwrap_or_else(|| format!("{strategy_type}:default"));
    let base_tf = config
        .timeframes
        .first()
        .cloned()
        .unwrap_or_else(|| Defaults::TIMEFRAME.to_string());
    let warmup_days = global.warmup_days.unwrap_or(30);

    // —— universe 解析（单 exchange + 去重 + 白名单）——
    let g_services_exchange = global
        .services
        .as_ref()
        .map(|s| s.trading.exchange.clone())
        .unwrap_or_default();
    let g_account = global
        .services
        .as_ref()
        .map(|s| s.trading.account.clone())
        .unwrap_or_else(|| "default".to_string());

    let mut raw_entries: Vec<(String, Option<String>)> = Vec::new();
    for e in &config.universe {
        use strategy_engine::strategy::config::UniverseEntry;
        match e {
            UniverseEntry::Symbol(s) => raw_entries.push((s.clone(), None)),
            UniverseEntry::Detailed { symbol, exchange } => {
                raw_entries.push((symbol.clone(), exchange.clone()))
            }
        }
    }

    let mut seen: std::collections::BTreeMap<String, String> = Default::default(); // venue_key -> symbol
    let mut resolved: Vec<(String, String)> = Vec::new(); // (symbol, normalized_exchange)
    let mut errors: Vec<String> = Vec::new();
    for (symbol, ex) in raw_entries {
        let symbol = symbol.trim().to_ascii_uppercase();
        let ex_norm = normalize_exchange(
            ex.as_deref()
                .or(global.exchange.as_deref())
                .or((!g_services_exchange.is_empty()).then_some(g_services_exchange.as_str()))
                .unwrap_or("binance"),
        );
        if !matches!(ex_norm.as_str(), "binance" | "SHFE") {
            errors.push(format!(
                "{symbol}: exchange `{ex_norm}` 不支持（本期横截面仅支持 binance/SHFE）"
            ));
            // 仍纳入 resolved：让下方「单一 exchange」聚合把所有条目点名（§5.4 报错即拒启）。
            resolved.push((symbol, ex_norm));
            continue;
        }
        let contract = contract_for(&global.contracts, &symbol).cloned();
        if let Some(c) = &contract {
            if let Err(e) = c.validate() {
                errors.push(format!("{symbol}: 合约参数非法: {e}"));
                continue;
            }
        }
        match (ex_norm.as_str(), &contract) {
            ("binance", Some(_)) => errors.push(format!(
                "{symbol}: binance 标的命中了 contracts 表（crypto 不应带期货合约参数）"
            )),
            ("SHFE", None) => errors.push(format!("{symbol}: SHFE 标的缺少 contracts.yaml 参数")),
            _ => {}
        }
        if !ex_norm_errors(&errors, &symbol) {
            let venue = venue_key_for(&ex_norm, contract.as_ref(), &symbol);
            if let Some(prev) = seen.insert(venue.clone(), symbol.clone()) {
                errors.push(format!(
                    "{symbol} 与 {prev} 解析出同一行键 `{venue}`（universe 重复）"
                ));
            } else {
                resolved.push((symbol, ex_norm));
            }
        }
    }
    let distinct: std::collections::BTreeSet<&str> =
        resolved.iter().map(|(_, e)| e.as_str()).collect();
    if distinct.len() > 1 {
        let pairs: Vec<String> = resolved.iter().map(|(s, e)| format!("{s}={e}")).collect();
        errors.push(format!(
            "横截面实例只允许单一 exchange，实际: {}",
            pairs.join(", ")
        ));
    }
    if !errors.is_empty() {
        return Err(anyhow!("横截面配置校验失败:\n- {}", errors.join("\n- ")));
    }

    // 统一后单标与横截面共用 L1 仅硬止损配置（trailing 由 L3 负责）。
    // live 的初始权益 = Σ instruments.initial_cash（按标的等分起账）；
    // 配置的组合总额等分落到每个标的，未配置则保持 None（回落 5000/标的）。
    let initial_cash_per_symbol = global
        .initial_cash
        .map(|c| c / (resolved.len().max(1) as f64));
    let instruments: Vec<SymbolSpec> = resolved
        .iter()
        .map(|(symbol, ex_norm)| {
            let contract = contract_for(&global.contracts, symbol).cloned();
            let (venue_qty_step, venue_min_qty, venue_qty_precision) =
                crate::config::venue_qty_spec_for(&global, ex_norm, ex_norm, symbol);
            SymbolSpec {
                strategy: strategy_type.clone(),
                symbol: symbol.clone(),
                timeframe: base_tf.clone(),
                cash: global.cash.unwrap_or(Defaults::CASH),
                initial_cash: initial_cash_per_symbol,
                params: config.params.clone(),
                warmup_days,
                exchange: ex_norm.clone(),
                venue_key: venue_key_for(ex_norm, contract.as_ref(), symbol),
                account: global
                    .account
                    .clone()
                    .or_else(|| Some(g_account.clone()))
                    .unwrap_or_else(|| "default".into()),
                leverage: global.leverage.unwrap_or(Defaults::LEVERAGE),
                mode: "backtest".to_string(),
                contract,
                risk: l1_stop_only(&global),
                venue_qty_step,
                venue_min_qty,
                venue_qty_precision,
            }
        })
        .collect();

    // U 防御位：一 actor 一网关——全 universe 必须同 account（未来 entry 级
    // account 放开时此守卫接管）。
    let pairs: Vec<(String, String)> = instruments
        .iter()
        .map(|s| (s.symbol.clone(), s.account.clone()))
        .collect();
    if let Err(msg) = ensure_single_account(&pairs) {
        return Err(anyhow!("横截面配置校验失败:\n- {msg}"));
    }
    let run_mode = global
        .mode
        .clone()
        .unwrap_or_else(|| "paper_trading".to_string());
    if !matches!(run_mode.as_str(), "live" | "paper_trading" | "smoking") {
        return Err(anyhow!(
            "横截面配置校验失败:\n- `mode: {run_mode}` 非法（run 模式仅 live / paper_trading / smoking）"
        ));
    }
    Ok(StrategySpec {
        strategy_type,
        strategy_id,
        config,
        instruments,
        warmup_days,
        run_mode,
        services: global.services.clone(),
        state_flush_interval_secs: global.state_flush_interval_secs,
        state_prune_terminal: global.state_prune_terminal,
    })
}

fn ex_norm_errors(errors: &[String], symbol: &str) -> bool {
    errors.iter().any(|e| e.starts_with(&format!("{symbol}:")))
}

/// 按配置实例化策略：统一注册表查询。
/// 工厂参数取首个 instrument（symbol/timeframe/cash 只影响单标语义，
/// 横截面策略忽略它们），私有参数由策略自行解析。
pub fn build_strategy(spec: &StrategySpec) -> anyhow::Result<Box<dyn CrossSectionalStrategyPort>> {
    let inst = spec
        .instruments
        .first()
        .ok_or_else(|| anyhow!("横截面配置无 instrument，无法实例化策略"))?;
    let args = strategy_strategies::registry::FactoryArgs {
        symbol: &inst.symbol,
        timeframe: &inst.timeframe,
        cash: inst.cash,
        params: inst.params.as_ref(),
    };
    strategy_strategies::registry::create(&spec.strategy_type, &args)
}

#[cfg(test)]
mod venue_qty_tests {
    use super::*;

    #[test]
    fn load_strategy_carries_venue_qty_constraints_from_global() {
        // 横截面 live 装配的数量口径必须与场所一致：_global.yaml 的
        // exchanges.specs 步长要透传到每个 SymbolSpec（fix5 拒单闭环）。
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("strategy.yaml"),
            "strategy_type: template_xsec\nstrategy_id: template_xsec:test\n\
             warmup_days: 1\nuniverse: [BTCUSDT]\ntimeframes: [1m]\n\
             indicators:\n  adx: { tf: 1m, period: 14 }\n  macd: { tf: 1m, fast: 12, slow: 26, signal: 9 }\n\
             entry: { default: [] }\nexit: { default: [] }\n\
             portfolio_exit: { stop_loss_pct: 50 }\n\
             rebalance: { every_bars: 1, top_k: 1, bottom_k: 0, weight_scheme: equal }\n\
             risk: { max_total_notional: 0.041 }\n\
             sizing: { per_position_pct: 0.0205, max_position_pct: 0.0205 }\n\
             params: { top_k: 1 }\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("_global.yaml"),
            "mode: paper_trading\nwarmup_days: 1\n\
             services:\n  trading:\n    ws_url: ws://127.0.0.1:1/ws\n    submit_enabled: false\n    exchange: binance\n    account: test\n\
             exchanges:\n  binance:\n    symbols: [BTCUSDT]\n    specs:\n      BTCUSDT: {qty_step: 0.001, min_qty: 0.001, qty_precision: 3}\n",
        )
        .unwrap();
        let spec = load_strategy(dir.path()).unwrap();
        let btc = spec
            .instruments
            .iter()
            .find(|s| s.symbol == "BTCUSDT")
            .expect("BTCUSDT 应在 instruments 中");
        assert_eq!(btc.venue_qty_step, Some(0.001));
        assert_eq!(btc.venue_min_qty, Some(0.001));
        assert_eq!(btc.venue_qty_precision, Some(3));
    }

    #[test]
    fn load_strategy_splits_configured_initial_cash_across_instruments() {
        // live 的初始权益 = Σ instruments.initial_cash（按标的等分起账）；
        // 组合总额必须等分落到每个标的，否则期货 100 万配置退化成默认
        // 5000/标的，单仓预算连一手名义都覆盖不了。
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("strategy.yaml"),
            "strategy_type: template_xsec\nstrategy_id: template_xsec:test\n\
             warmup_days: 1\nuniverse: [BTCUSDT, ETHUSDT]\ntimeframes: [1m]\n\
             indicators:\n  adx: { tf: 1m, period: 14 }\n  macd: { tf: 1m, fast: 12, slow: 26, signal: 9 }\n\
             entry: { default: [] }\nexit: { default: [] }\n\
             portfolio_exit: { stop_loss_pct: 50 }\n\
             rebalance: { every_bars: 1, top_k: 2, bottom_k: 0, weight_scheme: equal }\n\
             risk: { max_total_notional: 1.0 }\n\
             sizing: { per_position_pct: 0.5, max_position_pct: 0.5 }\n\
             params: { top_k: 2 }\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("_global.yaml"),
            "mode: paper_trading\ninitial_cash: 500000\n\
             services:\n  trading:\n    ws_url: ws://127.0.0.1:1/ws\n    submit_enabled: false\n    exchange: binance\n    account: test\n",
        )
        .unwrap();
        let spec = load_strategy(dir.path()).unwrap();
        assert_eq!(spec.instruments.len(), 2);
        assert_eq!(spec.instruments[0].initial_cash, Some(250000.0));
        let total: f64 = spec.instruments.iter().filter_map(|s| s.initial_cash).sum();
        assert_eq!(total, 500000.0);
    }
}
