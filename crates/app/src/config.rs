//! 运行配置 — 模板冻结文件，新增策略不修改
//!
//! 配置文件 `strategy.config.yaml`（默认路径，可用 --config 覆盖）。
//! schema 与 Python 侧 `backtest/config/{strategy}/{SYMBOL}.yaml` 同构，
//! Python 侧 YAML 可直接使用：
//!
//! ```yaml
//! strategy: example_strategy          # 顶层扁平 key（Python 侧以策略名为 key 包裹，两边都兼容）
//! timeframe: 8h
//! cash: 200.0
//! leverage: 1
//! mode: paper_trading
//! warmup_days: 30
//! params: {}
//! risk:                        # 统一风控（镜像 Python RiskControlConfig）
//!   enabled: true
//!   fixed_stop_loss_pct: 2.0
//!   trailing_profit:
//!     enabled: true
//!     activation_pct: 2.0
//!     drawdown_pct: 20.0
//!   fixed_take_profit_pct: 0.0
//! symbols:
//!   - symbol: BTCUSDT
//!   - symbol: SOLUSDT
//!     timeframe: 4h
//!     cash: 100.0
//! ```
//!
//! 合并优先级（高 → 低）：CLI 显式参数 > per-symbol > 全局 > 默认值。
//! `params` 为策略私有参数，per-symbol 整体覆盖全局（不做深合并）。
//! 配置文件不存在（默认路径）= 纯 CLI 运行，行为与旧版完全兼容。

use std::path::Path;

use serde::Deserialize;
use strategy_core::risk::RiskControlConfig;

/// 默认配置文件路径（依次探测：yaml → yml → json 兼容旧文件）
pub const DEFAULT_CONFIG_PATHS: [&str; 3] = [
    "./strategy.config.yaml",
    "./strategy.config.yml",
    "./strategy.config.json",
];

/// 默认配置目录（Python 同构布局：{strategy}/{SYMBOL}.yaml；单文件都不存在时探测）
pub const DEFAULT_CONFIG_DIR: &str = "./config";

/// 目录模式共享全局文件名（不含扩展名；位于扫描树内任意层级均可识别）。
/// 该文件不产生 symbol/runtime，只为 per-symbol 文件提供共享基线。
pub const GLOBAL_FILE_STEM: &str = "_global";

/// 密钥环境变量名：非空时覆盖 services.trading.strategy_secret（密钥不入库）
pub const STRATEGY_SECRET_ENV: &str = "STRATEGY_SECRET";

/// 配置文件顶层结构
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct FileConfig {
    /// 默认策略名
    pub strategy: Option<String>,
    /// 默认主周期（如 "8h"）
    pub timeframe: Option<String>,
    /// 默认每单名义金额
    pub cash: Option<f64>,
    /// 杠杆
    pub leverage: Option<u32>,
    /// 目标交易所实例名（place_order 帧 exchange；缺省回落 services.trading）
    pub exchange: Option<String>,
    /// 账户名（place_order 帧 account；缺省回落 services.trading）
    pub account: Option<String>,
    /// 运行模式 live / paper_trading / smoking（仅 run）
    pub mode: Option<String>,
    /// 策略私有参数（透传给策略工厂）
    pub params: Option<serde_json::Value>,
    /// warmup 预载天数（默认 30；指标计算起点）
    pub warmup_days: Option<u32>,
    /// 统一风控配置（缺省 = RiskControlConfig::default()，镜像 Python risk 节）
    pub risk: Option<RiskControlConfig>,
    /// per-symbol 覆盖列表
    pub symbols: Vec<SymbolEntry>,
    /// 外部服务端点（run 命令必需；backtest/download 不需要）
    #[serde(default)]
    pub services: Option<ServicesConfig>,
    /// live/paper 状态定时全 Store flush 间隔（秒，缺省 60）。仅 run 生效：
    /// RuntimeActor 按此间隔时间驱动落盘；回测结束时只 flush 一次，不用此值。
    #[serde(default = "default_state_flush_interval_secs")]
    pub state_flush_interval_secs: u64,
    /// 跨日 flush 时是否物理裁剪终态行（缺省 true）。false = 旧行为：
    /// 终态行保留并继续落盘，recover 读全部日文件（spec §设计.7）。
    #[serde(default = "default_true")]
    pub state_prune_terminal: bool,
}

/// 状态定时 flush 默认间隔（秒）
fn default_state_flush_interval_secs() -> u64 {
    60
}

fn default_true() -> bool {
    true
}

/// kline-service / trading-service 端点（设计文档 §3）
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct ServicesConfig {
    pub kline: KlineServiceCfg,
    pub trading: TradingServiceCfg,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct KlineServiceCfg {
    pub backfill_url: String,
    pub ws_url: String,
    pub reconnect_backoff_max_secs: u64,
}

impl Default for KlineServiceCfg {
    fn default() -> Self {
        Self {
            // 占位端点：无 services 配置时连接失败暴露未配置状态，
            // 真实地址由用户在 config/_global.yaml（或 per-symbol）中提供
            backfill_url: "https://your-kline-service.example.com".into(),
            ws_url: "wss://your-kline-service.example.com/ws/klines".into(),
            reconnect_backoff_max_secs: 120,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TradingServiceCfg {
    pub ws_url: String,
    pub request_timeout_ms: u64,
    /// 安全开关：false 时本地拦截所有下单（默认）
    pub submit_enabled: bool,
    pub reconnect_backoff_max_secs: u64,
    /// HMAC 共享密钥
    pub strategy_secret: String,
    /// 目标交易所实例名（per-symbol 未配置 exchange 时的回落）
    pub exchange: String,
    /// 账户名（per-symbol 未配置 account 时的回落）
    pub account: String,
}

impl Default for TradingServiceCfg {
    fn default() -> Self {
        Self {
            ws_url: "ws://127.0.0.1:9100/ws".into(),
            request_timeout_ms: 5000,
            submit_enabled: false,
            reconnect_backoff_max_secs: 120,
            strategy_secret: String::new(),
            exchange: "binance".into(),
            account: "default".into(),
        }
    }
}

/// per-symbol 覆盖项
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolEntry {
    pub symbol: String,
    pub strategy: Option<String>,
    pub timeframe: Option<String>,
    pub cash: Option<f64>,
    pub params: Option<serde_json::Value>,
    pub warmup_days: Option<u32>,
    /// 目标交易所实例名（place_order 帧 exchange；缺省回落 services.trading）
    pub exchange: Option<String>,
    /// 账户名（place_order 帧 account；缺省回落 services.trading）
    pub account: Option<String>,
    /// 杠杆倍数（place_order 帧 leverage；缺省回落文件顶层 leverage）
    pub leverage: Option<u32>,
    /// 运行模式（live / paper_trading；缺省回落 _global → 默认 paper_trading）
    pub mode: Option<String>,
}

/// 合并后的单 symbol 运行规格
#[derive(Debug, Clone)]
pub struct SymbolSpec {
    pub strategy: String,
    pub symbol: String,
    pub timeframe: String,
    pub cash: f64,
    pub params: Option<serde_json::Value>,
    pub warmup_days: u32,
    /// place_order 帧字段：交易所实例名
    pub exchange: String,
    /// place_order 帧字段：账户名
    pub account: String,
    /// place_order 帧字段：杠杆倍数
    pub leverage: u32,
    /// 运行模式 live / paper_trading（决定下单网关与 submit 闸门）
    pub mode: String,
}

/// 运行默认值（模板冻结）
pub struct Defaults;

impl Defaults {
    pub const STRATEGY: &'static str = "example_dual_ma";
    pub const TIMEFRAME: &'static str = "8h";
    pub const CASH: f64 = 200.0;
    pub const LEVERAGE: u32 = 1;
    pub const MODE: &'static str = "paper_trading";
}

/// CLI / 配置 / 默认 三级回落（CLI 显式 > 配置 > 默认）
fn pick(cli: Option<String>, file: Option<String>, default: &str) -> String {
    cli.or(file).unwrap_or_else(|| default.into())
}

/// params 归属校验：只继承**与最终生效策略同属**的 params。
/// CLI `--strategy` 换策略后，旧策略条目里的 params（schema 不同）不得
/// 错配注入（策略侧 `deny_unknown_fields` 会直接报错），此时回落策略全默认。
/// 归属判定：entry params 归属 entry.strategy（缺省时跟随文件顶层 strategy）；
/// 文件顶层 params 归属文件顶层 strategy。双方都未声明 strategy 的 params
/// 视为跟随最终策略（纯 CLI / 无 strategy 字段场景）。
fn params_for(
    resolved: &str,
    entry_strategy: Option<&str>,
    file_strategy: Option<&str>,
    entry_params: Option<serde_json::Value>,
    file_params: Option<serde_json::Value>,
) -> Option<serde_json::Value> {
    let belongs = |owner: Option<&str>| owner.is_none_or(|s| s.eq_ignore_ascii_case(resolved));
    // entry params 归属：entry.strategy 缺省时跟随文件顶层 strategy
    if belongs(entry_strategy.or(file_strategy)) {
        if let Some(p) = entry_params {
            return Some(p);
        }
    }
    if belongs(file_strategy) {
        return file_params;
    }
    None
}

/// 反序列化 FileConfig：YAML 优先（Python 侧同构），JSON 兼容。
/// 先试 YAML，失败再试 JSON（JSON 是 YAML 的子集语法场景之外，
/// 两套解析器互为兜底：`deny_unknown_fields` 错误两边都会报）。
fn parse_config(text: &str) -> anyhow::Result<FileConfig> {
    match serde_yaml::from_str::<FileConfig>(text) {
        Ok(cfg) => Ok(cfg),
        Err(yaml_err) => serde_json::from_str::<FileConfig>(text).map_err(|json_err| {
            anyhow::anyhow!("配置文件解析失败（YAML: {yaml_err}）（JSON: {json_err}）")
        }),
    }
}

/// 环境变量覆盖（密钥不入库的最后一环）：
/// [`STRATEGY_SECRET_ENV`] 非空时强制覆盖 `services.trading.strategy_secret`。
fn apply_env_overrides(cfg: &mut FileConfig) {
    if let Ok(secret) = std::env::var(STRATEGY_SECRET_ENV) {
        if !secret.trim().is_empty() {
            cfg.services
                .get_or_insert_with(ServicesConfig::default)
                .trading
                .strategy_secret = secret;
        }
    }
}

/// 加载配置文件。
/// - `path` 为 Some → 文件必须存在（显式指定却缺失 = 配置错误）
/// - `path` 为 None → 依次探测 [`DEFAULT_CONFIG_PATHS`]，都不存在再探测
///   [`DEFAULT_CONFIG_DIR`]（Python 同构目录布局），都没有返回空配置（纯 CLI 兼容）
pub fn load(path: Option<&Path>) -> anyhow::Result<FileConfig> {
    let mut cfg = match path {
        Some(p) => {
            if p.is_dir() {
                return load_dir(p);
            }
            let text = std::fs::read_to_string(p)
                .map_err(|e| anyhow::anyhow!("读取配置文件失败: {} ({e})", p.display()))?;
            parse_config(&text)?
        }
        None => {
            let mut found = None;
            for p in DEFAULT_CONFIG_PATHS {
                let p = Path::new(p);
                if p.exists() {
                    let text = std::fs::read_to_string(p)?;
                    found = Some(parse_config(&text)?);
                    break;
                }
            }
            match found {
                Some(cfg) => cfg,
                None => {
                    let dir = Path::new(DEFAULT_CONFIG_DIR);
                    if dir.is_dir() {
                        return load_dir(dir);
                    }
                    FileConfig::default()
                }
            }
        }
    };
    apply_env_overrides(&mut cfg);
    // 单文件模式也补挂同目录（及 ./config/）下的 _global.yaml 共享基线，
    // 使 services: 等全局字段不因配置组织方式而丢失。
    apply_global_file_fallback(&mut cfg, path)?;
    Ok(cfg)
}

/// 单文件配置的 `_global.yaml` 回落：
/// - `path` 为 None（默认单文件命中）→ 探测配置文件同目录与 `./config/`；
/// - `path` 为 Some(文件) → 只探测该文件同目录。
/// - 目录模式（`load_dir`）已自带 `_global.yaml` 合并，此处跳过；
///   已有 services/全局字段时不覆盖（单文件显式配置优先于全局基线）。
fn apply_global_file_fallback(cfg: &mut FileConfig, path: Option<&Path>) -> anyhow::Result<()> {
    if path.map(|p| p.is_dir()).unwrap_or(false) {
        return Ok(()); // 目录模式已在 load_dir 内合并
    }
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    if let Some(p) = path {
        if let Some(parent) = p.parent() {
            if parent != Path::new("") {
                candidates.push(parent.join(format!("{GLOBAL_FILE_STEM}.yaml")));
                candidates.push(parent.join(format!("{GLOBAL_FILE_STEM}.yml")));
            }
        }
    } else {
        candidates.push(Path::new(DEFAULT_CONFIG_DIR).join(format!("{GLOBAL_FILE_STEM}.yaml")));
        candidates.push(Path::new(DEFAULT_CONFIG_DIR).join(format!("{GLOBAL_FILE_STEM}.yml")));
    }
    for candidate in candidates {
        if !candidate.exists() {
            continue;
        }
        let text = std::fs::read_to_string(&candidate)
            .map_err(|e| anyhow::anyhow!("读取配置文件失败: {} ({e})", candidate.display()))?;
        let g = parse_config(&text)
            .map_err(|err| anyhow::anyhow!("{}: {err}", candidate.display()))?;
        anyhow::ensure!(
            g.symbols.is_empty(),
            "{}: 共享全局文件不允许包含 symbols 段",
            candidate.display()
        );
        cfg.strategy = cfg.strategy.take().or(g.strategy);
        cfg.timeframe = cfg.timeframe.take().or(g.timeframe);
        cfg.cash = cfg.cash.take().or(g.cash);
        cfg.leverage = cfg.leverage.take().or(g.leverage);
        cfg.mode = cfg.mode.take().or(g.mode);
        cfg.risk = cfg.risk.take().or(g.risk);
        cfg.exchange = cfg.exchange.take().or(g.exchange);
        cfg.account = cfg.account.take().or(g.account);
        cfg.services = cfg.services.take().or(g.services);
        break; // 至多一个 _global 生效
    }
    Ok(())
}

/// 判断是否为共享全局文件（文件名 stem == [`GLOBAL_FILE_STEM`]）
fn is_global_file(path: &Path) -> bool {
    path.file_stem().and_then(|s| s.to_str()) == Some(GLOBAL_FILE_STEM)
}

/// 目录式配置加载（Python 同构布局）：
///
/// ```text
/// config/
/// ├── _global.yaml             # 共享全局基线（不产生 runtime；可选，至多一个）
/// └── example_strategy/
///     ├── BTCUSDT.yaml
///     └── ETHUSDT.yaml
/// ```
///
/// 递归收集目录（含策略子目录）下全部 `{SYMBOL}.yaml`，每个文件对应
/// 恰好一个 symbol（扁平 schema，与单文件模式字段相同；symbol 可省略，
/// 默认取文件名去扩展名大写）。文件顶层 strategy/timeframe/cash/params/
/// warmup_days/mode 归**该文件 symbol 的 per-symbol 覆盖**；leverage/risk
/// 是目录级全局字段，取首个出现者。文件按路径排序，保证确定性。
///
/// `_global.yaml`（stem == [`GLOBAL_FILE_STEM`]，扩展名 yaml/yml）不生成
/// symbol entry，其全局字段（含 `services`）作为共享基线；优先级：
/// per-symbol 文件 > `_global.yaml` > 内置默认。
pub fn load_dir(dir: &Path) -> anyhow::Result<FileConfig> {
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    collect_yaml_files(dir, &mut files)?;
    files.sort(); // 路径序确定性（跨平台 diff 验收依赖）

    anyhow::ensure!(
        !files.is_empty(),
        "配置目录无 YAML 文件: {}（期望 {{strategy}}/{{SYMBOL}}.yaml）",
        dir.display()
    );

    // 拆出共享全局文件（至多一个；不允许 symbols 段）
    let mut global_cfg: Option<FileConfig> = None;
    let mut symbol_files: Vec<std::path::PathBuf> = Vec::new();
    for path in files {
        if is_global_file(&path) {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!("读取配置文件失败: {} ({e})", path.display()))?;
            let fc =
                parse_config(&text).map_err(|err| anyhow::anyhow!("{}: {err}", path.display()))?;
            anyhow::ensure!(
                fc.symbols.is_empty(),
                "{}: 共享全局文件不允许包含 symbols 段",
                path.display()
            );
            anyhow::ensure!(
                global_cfg.is_none(),
                "存在多个共享全局文件（{}），期望至多一个 {GLOBAL_FILE_STEM}.yaml",
                path.display()
            );
            global_cfg = Some(fc);
        } else {
            symbol_files.push(path);
        }
    }

    anyhow::ensure!(
        !symbol_files.is_empty(),
        "配置目录只有 {GLOBAL_FILE_STEM}.yaml，缺少 per-symbol 文件（形如 strategy/SYMBOL.yaml）: {}",
        dir.display()
    );

    let mut merged = FileConfig::default();
    for path in symbol_files {
        let text = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("读取配置文件失败: {} ({e})", path.display()))?;
        let fc = parse_config(&text).map_err(|err| anyhow::anyhow!("{}: {err}", path.display()))?;

        anyhow::ensure!(
            fc.symbols.len() <= 1,
            "{}: 目录模式下每文件只能有一个 symbol（找到 {} 个）",
            path.display(),
            fc.symbols.len()
        );

        // symbol 条目：显式 symbols:[1 项] 优先（缺项回落文件顶层字段）；
        // 无 symbols: → 整个文件就是该 symbol 的配置（symbol 取文件名）
        let entry = if let Some(mut e) = fc.symbols.into_iter().next() {
            e.strategy = e.strategy.or(fc.strategy.clone());
            e.timeframe = e.timeframe.or(fc.timeframe.clone());
            e.cash = e.cash.or(fc.cash);
            e.params = e.params.or(fc.params.clone());
            e.warmup_days = e.warmup_days.or(fc.warmup_days);
            e.exchange = e.exchange.or(fc.exchange.clone());
            e.account = e.account.or(fc.account.clone());
            e.leverage = e.leverage.or(fc.leverage);
            e.mode = e.mode.or(fc.mode.clone());
            e
        } else {
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_uppercase();
            anyhow::ensure!(
                !stem.is_empty(),
                "{}: 无法从文件名推断 symbol",
                path.display()
            );
            SymbolEntry {
                symbol: stem,
                strategy: fc.strategy.clone(),
                timeframe: fc.timeframe.clone(),
                cash: fc.cash,
                params: fc.params.clone(),
                warmup_days: fc.warmup_days,
                exchange: fc.exchange.clone(),
                account: fc.account.clone(),
                leverage: fc.leverage,
                mode: fc.mode.clone(),
            }
        };

        // 目录级全局字段：任一首个非空值（后续文件不覆盖）
        // mode 例外：归 per-symbol entry，不进全局赢家（U1/U5）
        merged.strategy = merged.strategy.or(fc.strategy);
        merged.timeframe = merged.timeframe.or(fc.timeframe);
        merged.cash = merged.cash.or(fc.cash);
        merged.leverage = merged.leverage.or(fc.leverage);
        merged.risk = merged.risk.or(fc.risk);
        merged.exchange = merged.exchange.or(fc.exchange);
        merged.account = merged.account.or(fc.account);
        merged.services = merged.services.or(fc.services);
        merged.symbols.push(entry);
    }

    // 共享基线补缺（per-symbol 文件已给出的字段不覆盖）
    if let Some(g) = global_cfg {
        merged.strategy = merged.strategy.or(g.strategy);
        merged.timeframe = merged.timeframe.or(g.timeframe);
        merged.cash = merged.cash.or(g.cash);
        merged.leverage = merged.leverage.or(g.leverage);
        merged.mode = merged.mode.or(g.mode);
        merged.risk = merged.risk.or(g.risk);
        merged.exchange = merged.exchange.or(g.exchange);
        merged.account = merged.account.or(g.account);
        merged.services = merged.services.or(g.services);
    }

    apply_env_overrides(&mut merged);
    Ok(merged)
}

/// 递归收集目录下 yaml/yml 文件
fn collect_yaml_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) -> std::io::Result<()> {
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let p = e.path();
        if p.is_dir() {
            collect_yaml_files(&p, out)?;
        } else if matches!(
            p.extension().and_then(|e| e.to_str()),
            Some("yaml") | Some("yml")
        ) {
            out.push(p);
        }
    }
    Ok(())
}

/// 解析 backtest 的单 symbol 规格。
/// CLI 显式值 > 配置文件全局值 > 默认值；symbol 取 CLI，缺省回落配置首个 symbol。
pub fn resolve_backtest(
    file: &FileConfig,
    cli_symbol: Option<&str>,
    cli_strategy: Option<&str>,
    cli_timeframe: Option<&str>,
    cli_cash: Option<f64>,
) -> anyhow::Result<SymbolSpec> {
    let first = file.symbols.first();
    let symbol = match cli_symbol {
        Some(s) => s.to_string(),
        None => first
            .map(|e| e.symbol.clone())
            .ok_or_else(|| anyhow::anyhow!("缺少 symbol：CLI --symbol 或配置文件 symbols[0]"))?,
    };
    let symbol = symbol.to_uppercase();
    // 目录模式：选中 --symbol 对应的 per-symbol 条目（单文件模式 symbols 为空 → None）
    let entry = file
        .symbols
        .iter()
        .find(|e| e.symbol.eq_ignore_ascii_case(&symbol));
    // 合并优先级：CLI 显式 > per-symbol 条目 > 文件顶层 > 默认
    let strategy = pick(
        cli_strategy.map(str::to_string),
        entry
            .and_then(|e| e.strategy.clone())
            .or(file.strategy.clone()),
        Defaults::STRATEGY,
    );
    let timeframe = pick(
        cli_timeframe.map(str::to_string),
        entry
            .and_then(|e| e.timeframe.clone())
            .or(file.timeframe.clone()),
        Defaults::TIMEFRAME,
    );
    let cash = cli_cash
        .or_else(|| entry.and_then(|e| e.cash))
        .or(file.cash)
        .unwrap_or(Defaults::CASH);
    // 交易所/账户：per-symbol > 文件顶层 exchange/account > services.trading > 回测撮合默认
    let services_ex = file
        .services
        .as_ref()
        .map(|s| (s.trading.exchange.clone(), s.trading.account.clone()));
    let exchange = entry
        .and_then(|e| e.exchange.clone())
        .or(file.exchange.clone())
        .or_else(|| services_ex.as_ref().map(|(ex, _)| ex.clone()))
        .unwrap_or_else(|| "binance".into());
    let account = entry
        .and_then(|e| e.account.clone())
        .or(file.account.clone())
        .or_else(|| services_ex.as_ref().map(|(_, ac)| ac.clone()))
        .unwrap_or_else(|| "default".into());
    // params 归属校验须在 strategy 移入 SymbolSpec 前完成
    let params = params_for(
        &strategy,
        entry.and_then(|e| e.strategy.as_deref()),
        file.strategy.as_deref(),
        entry.and_then(|e| e.params.clone()),
        file.params.clone(),
    );
    Ok(SymbolSpec {
        strategy,
        symbol,
        timeframe,
        cash,
        params,
        warmup_days: entry
            .and_then(|e| e.warmup_days)
            .or(file.warmup_days)
            .unwrap_or(30),
        exchange,
        account,
        leverage: entry
            .and_then(|e| e.leverage)
            .or(file.leverage)
            .unwrap_or(Defaults::LEVERAGE),
        // 回测不使用 mode（trading_mode 硬编码 backtest），但保持字段完整
        mode: entry
            .and_then(|e| e.mode.clone())
            .or_else(|| file.mode.clone())
            .unwrap_or_else(|| Defaults::MODE.to_string()),
    })
}

/// 候选条目：symbol + 可选 per-symbol 覆盖
/// （strategy/timeframe/cash/params/warmup_days/exchange/account/leverage/mode）
struct Entry {
    symbol: String,
    strategy: Option<String>,
    timeframe: Option<String>,
    cash: Option<f64>,
    params: Option<serde_json::Value>,
    warmup_days: Option<u32>,
    exchange: Option<String>,
    account: Option<String>,
    leverage: Option<u32>,
    mode: Option<String>,
}

/// 解析 run 的 per-symbol 规格列表。
/// CLI --symbol 显式给出 → 以 CLI 列表为准（全局值合并）；
/// 否则用配置文件 symbols（逐项合并全局）。
/// `cli_mode` 显式给出时强制覆盖所有 symbol（与 cli_strategy 同款压制语义）。
pub fn resolve_run(
    file: &FileConfig,
    cli_symbols: &[String],
    cli_strategy: Option<&str>,
    cli_timeframe: Option<&str>,
    cli_cash: Option<f64>,
    cli_mode: Option<&str>,
) -> anyhow::Result<Vec<SymbolSpec>> {
    // 全局基准（CLI > 全局 > 默认）。
    // CLI 显式给出 strategy/timeframe/cash 时强制压过 per-symbol（优先级最高）。
    let g_strategy = pick(
        cli_strategy.map(str::to_string),
        file.strategy.clone(),
        Defaults::STRATEGY,
    );
    let g_timeframe = pick(
        cli_timeframe.map(str::to_string),
        file.timeframe.clone(),
        Defaults::TIMEFRAME,
    );
    let g_cash = cli_cash.or(file.cash).unwrap_or(Defaults::CASH);
    // 交易所/账户全局基准：文件顶层 exchange/account > services.trading > 结构默认
    let g_exchange = file
        .exchange
        .clone()
        .or_else(|| file.services.as_ref().map(|s| s.trading.exchange.clone()))
        .unwrap_or_else(|| TradingServiceCfg::default().exchange);
    let g_account = file
        .account
        .clone()
        .or_else(|| file.services.as_ref().map(|s| s.trading.account.clone()))
        .unwrap_or_else(|| TradingServiceCfg::default().account);
    let g_leverage = file.leverage.unwrap_or(Defaults::LEVERAGE);
    // mode 全局基准：_global/文件顶层 > 默认（CLI 强制覆盖在循环内逐 spec 判）
    let g_mode = file
        .mode
        .clone()
        .unwrap_or_else(|| Defaults::MODE.to_string());

    // 候选列表：CLI symbols 优先（只带名字，其余回落全局）；否则用配置文件 entries
    let entries: Vec<Entry> = if !cli_symbols.is_empty() {
        // CLI 过滤：仍按 symbol 名查 per-symbol 条目（字段缺失时回落全局）
        cli_symbols
            .iter()
            .map(|s| {
                let e = file
                    .symbols
                    .iter()
                    .find(|e| e.symbol.eq_ignore_ascii_case(s));
                Entry {
                    symbol: s.clone(),
                    strategy: e.and_then(|e| e.strategy.clone()),
                    timeframe: e.and_then(|e| e.timeframe.clone()),
                    cash: e.and_then(|e| e.cash),
                    params: e.and_then(|e| e.params.clone()),
                    warmup_days: e.and_then(|e| e.warmup_days),
                    exchange: e.and_then(|e| e.exchange.clone()),
                    account: e.and_then(|e| e.account.clone()),
                    leverage: e.and_then(|e| e.leverage),
                    mode: e.and_then(|e| e.mode.clone()),
                }
            })
            .collect()
    } else {
        file.symbols
            .iter()
            .map(|e| Entry {
                symbol: e.symbol.clone(),
                strategy: e.strategy.clone(),
                timeframe: e.timeframe.clone(),
                cash: e.cash,
                params: e.params.clone(),
                warmup_days: e.warmup_days,
                exchange: e.exchange.clone(),
                account: e.account.clone(),
                leverage: e.leverage,
                mode: e.mode.clone(),
            })
            .collect()
    };

    anyhow::ensure!(
        !entries.is_empty(),
        "缺少 symbol：CLI --symbol 或配置文件 symbols"
    );

    let mut specs: Vec<SymbolSpec> = Vec::new();
    for entry in entries {
        let symbol = entry.symbol;
        // CLI 显式 > per-symbol > 全局(含默认)；CLI 显式时 g_* 已是 CLI 值，
        // 但 per-symbol 不能压过它，故须显式判 CLI。
        let entry_strategy = entry.strategy.clone();
        let strategy = match cli_strategy {
            Some(_) => g_strategy.clone(),
            None => entry.strategy.unwrap_or_else(|| g_strategy.clone()),
        };
        let timeframe = match cli_timeframe {
            Some(_) => g_timeframe.clone(),
            None => entry.timeframe.unwrap_or_else(|| g_timeframe.clone()),
        };
        let cash = match cli_cash {
            Some(_) => g_cash,
            None => entry.cash.unwrap_or(g_cash),
        };
        // exchange/account/leverage 无 CLI 开关：per-symbol > 全局
        let exchange = entry.exchange.unwrap_or_else(|| g_exchange.clone());
        let account = entry.account.unwrap_or_else(|| g_account.clone());
        let leverage = entry.leverage.unwrap_or(g_leverage);
        // mode：CLI 显式强制全覆盖 > per-symbol > _global/文件顶层 > 默认
        let mode = match cli_mode {
            Some(m) => m.to_string(),
            None => entry.mode.unwrap_or_else(|| g_mode.clone()),
        };
        // params 归属校验须在 strategy 移入 SymbolSpec 前完成
        let params = params_for(
            &strategy,
            entry_strategy.as_deref(),
            file.strategy.as_deref(),
            entry.params,
            file.params.clone(),
        );
        anyhow::ensure!(
            matches!(mode.as_str(), "live" | "paper_trading"),
            "symbol {symbol}: mode 必须是 live / paper_trading，实际 {mode:?}"
        );
        specs.push(SymbolSpec {
            strategy,
            symbol: symbol.to_uppercase(),
            timeframe,
            cash,
            params,
            warmup_days: entry.warmup_days.or(file.warmup_days).unwrap_or(30),
            exchange,
            account,
            leverage,
            mode,
        });
    }
    Ok(specs)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `set_current_dir` 是进程全局态：默认路径测试必须串行，否则互相踩踏；
    /// RAII 在 panic 时也恢复原工作目录。
    static CWD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    struct CwdGuard {
        prev: std::path::PathBuf,
        _lock: std::sync::MutexGuard<'static, ()>,
    }
    impl CwdGuard {
        fn enter(dir: &std::path::Path) -> Self {
            let lock = CWD_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let prev = std::env::current_dir().unwrap();
            std::env::set_current_dir(dir).unwrap();
            Self { prev, _lock: lock }
        }
    }
    impl Drop for CwdGuard {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.prev);
        }
    }

    fn yaml(y: &str) -> FileConfig {
        serde_yaml::from_str(y).unwrap()
    }

    fn file(json: &str) -> FileConfig {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn empty_config_pure_cli_defaults() {
        let f = FileConfig::default();
        let spec = resolve_backtest(&f, Some("btcusdt"), None, None, None).unwrap();
        assert_eq!(spec.strategy, "example_dual_ma");
        assert_eq!(spec.timeframe, "8h");
        assert_eq!(spec.cash, 200.0);
        assert_eq!(spec.symbol, "BTCUSDT");
    }

    // ===== YAML 解析 =====

    #[test]
    fn yaml_basic_parse() {
        let f = yaml(
            r#"
strategy: example_strategy
timeframe: 8h
cash: 200.0
leverage: 1
mode: paper_trading
symbols:
  - symbol: BTCUSDT
  - symbol: ETHUSDT
"#,
        );
        assert_eq!(f.strategy.as_deref(), Some("example_strategy"));
        assert_eq!(f.timeframe.as_deref(), Some("8h"));
        assert_eq!(f.cash, Some(200.0));
        assert_eq!(f.symbols.len(), 2);
        assert_eq!(f.symbols[0].symbol, "BTCUSDT");
    }

    #[test]
    fn yaml_risk_section_parses_into_risk_config() {
        let f = yaml(
            r#"
risk:
  enabled: true
  fixed_stop_loss_pct: 2.0
  trailing_profit:
    enabled: true
    activation_pct: 2.0
    drawdown_pct: 20.0
  fixed_take_profit_pct: 0.0
symbols:
  - symbol: BTCUSDT
"#,
        );
        let risk = f.risk.unwrap();
        assert!(risk.enabled);
        assert_eq!(risk.fixed_stop_loss_pct, 2.0);
        assert!(risk.trailing_profit.enabled);
        assert_eq!(risk.trailing_profit.activation_pct, 2.0);
        assert_eq!(risk.trailing_profit.drawdown_pct, 20.0);
        assert_eq!(risk.fixed_take_profit_pct, 0.0);
    }

    #[test]
    fn yaml_risk_partial_fields_fill_defaults() {
        // serde(default)：只给部分字段，其余回落默认值（镜像 Python from_dict）
        let f = yaml(
            r#"
risk:
  fixed_stop_loss_pct: 3.0
symbols:
  - symbol: BTCUSDT
"#,
        );
        let risk = f.risk.unwrap();
        assert!(risk.enabled); // 默认 true
        assert_eq!(risk.fixed_stop_loss_pct, 3.0);
        assert_eq!(risk.trailing_profit.activation_pct, 20.0); // 默认
        assert_eq!(risk.fixed_take_profit_pct, 0.0); // 默认禁用
    }

    #[test]
    fn yaml_risk_missing_section_falls_back_to_engine_default() {
        let f = yaml(
            r#"
symbols:
  - symbol: BTCUSDT
"#,
        );
        assert!(f.risk.is_none()); // main.rs unwrap_or_default()
    }

    #[test]
    fn yaml_python_style_multi_doc_header_ignored() {
        // Python 侧 YAML 以策略名为顶层 key 包裹（example_strategy_v3: {...}）。
        // 这种结构在本扁平 schema 下是 unknown field → 必须报错而不是静默忽略。
        // 直接复用 Python 文件需人工去掉顶层包裹（一次性迁移成本，schema 保持扁平）。
        let r = serde_yaml::from_str::<FileConfig>(
            r#"
example_strategy_v3:
  enabled: true
  symbols: [BTCUSDT]
"#,
        );
        assert!(r.is_err(), "Python 顶层 key 包裹结构应报 unknown field");
    }

    #[test]
    fn yaml_unknown_fields_rejected() {
        let r = serde_yaml::from_str::<FileConfig>("nope: 1");
        assert!(r.is_err());
    }

    #[test]
    fn json_still_supported_for_compat() {
        let f = file(r#"{"cash": 77.0, "symbols": [{"symbol": "BTCUSDT"}]}"#);
        assert_eq!(f.cash, Some(77.0));
    }

    // ===== resolve 系列（与 JSON 时代行为一致）=====

    #[test]
    fn backtest_cli_overrides_config() {
        let f = file(
            r#"{"strategy": "a", "timeframe": "4h", "cash": 50.0, "symbols": [{"symbol": "BTCUSDT"}]}"#,
        );
        let spec = resolve_backtest(
            &f,
            Some("BTCUSDT"),
            Some("example_dual_ma"),
            Some("8h"),
            Some(99.0),
        )
        .unwrap();
        assert_eq!(spec.strategy, "example_dual_ma");
        assert_eq!(spec.timeframe, "8h");
        assert_eq!(spec.cash, 99.0);
    }

    #[test]
    fn backtest_falls_back_to_config_then_default() {
        let f = yaml(
            r#"
timeframe: 4h
symbols:
  - symbol: ETHUSDT
"#,
        );
        let spec = resolve_backtest(&f, None, None, None, None).unwrap();
        assert_eq!(spec.symbol, "ETHUSDT");
        assert_eq!(spec.timeframe, "4h");
        assert_eq!(spec.strategy, "example_dual_ma"); // 默认
    }

    #[test]
    fn cli_strategy_switch_drops_foreign_params() {
        // 回归：CLI --strategy 换策略后，旧策略条目 params（schema 不同）
        // 不得错配注入——否则策略侧 deny_unknown_fields 直接报错。
        // 未声明 strategy 的 params 视为跟随最终策略，仍继承。
        let f = file(
            r#"{
              "strategy": "example_strategy",
              "params": {"fixed_stop_enabled": true},
              "symbols": [{"symbol": "BTCUSDT", "params": {"stop_loss_pct": 2.0}}]
            }"#,
        );
        // 换成 schema 完全不同的策略：entry/file params 都属 example_strategy → 全部丢弃
        let spec =
            resolve_backtest(&f, Some("BTCUSDT"), Some("minimal_dual_ma"), None, None).unwrap();
        assert_eq!(spec.strategy, "minimal_dual_ma");
        assert!(
            spec.params.is_none(),
            "旧策略 params 必须丢弃，实际 {:?}",
            spec.params
        );

        // 不换策略：params 正常继承（per-symbol 优先于文件顶层）
        let spec2 = resolve_backtest(&f, Some("BTCUSDT"), None, None, None).unwrap();
        assert_eq!(spec2.strategy, "example_strategy");
        let p = spec2.params.expect("应继承 params");
        assert_eq!(p["stop_loss_pct"], 2.0);

        // run 同语义
        let specs = resolve_run(
            &f,
            &["BTCUSDT".into()],
            Some("minimal_dual_ma"),
            None,
            None,
            None,
        )
        .unwrap();
        assert!(specs[0].params.is_none(), "run 换策略同样丢弃旧 params");
        let specs2 = resolve_run(&f, &["BTCUSDT".into()], None, None, None, None).unwrap();
        assert_eq!(specs2[0].params.as_ref().unwrap()["stop_loss_pct"], 2.0);
    }

    #[test]
    fn param_without_strategy_follows_final_strategy() {
        // 未声明 strategy 的 params（纯 CLI 场景）视为跟随最终策略，照常继承
        let f = file(r#"{"symbols": [{"symbol": "BTCUSDT", "params": {"fast_period": 5}}]}"#);
        let spec =
            resolve_backtest(&f, Some("BTCUSDT"), Some("minimal_dual_ma"), None, None).unwrap();
        assert_eq!(spec.params.unwrap()["fast_period"], 5);
    }

    #[test]
    fn run_per_symbol_overrides_global() {
        let f = file(
            r#"{
              "strategy": "example_strategy", "timeframe": "8h", "cash": 200.0,
              "symbols": [
                {"symbol": "BTCUSDT"},
                {"symbol": "SOLUSDT", "timeframe": "4h", "cash": 100.0, "params": {"adx_threshold": 25.0}}
              ]
            }"#,
        );
        let specs = resolve_run(&f, &[], None, None, None, None).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].timeframe, "8h");
        assert_eq!(specs[0].cash, 200.0);
        assert_eq!(specs[1].timeframe, "4h");
        assert_eq!(specs[1].cash, 100.0);
        assert_eq!(specs[1].params.as_ref().unwrap()["adx_threshold"], 25.0);
        assert!(specs[0].params.is_none());
    }

    #[test]
    fn run_cli_symbols_override_config_symbols() {
        let f = yaml(
            r#"
cash: 111.0
symbols:
  - symbol: BTCUSDT
"#,
        );
        let specs = resolve_run(&f, &["ETHUSDT".into()], None, None, None, None).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].symbol, "ETHUSDT");
        assert_eq!(specs[0].cash, 111.0); // CLI symbol 列表 + 全局配置
    }

    #[test]
    fn run_cli_strategy_wins_over_per_symbol() {
        let f = yaml(
            r#"
strategy: a
symbols:
  - symbol: BTCUSDT
    strategy: b
"#,
        );
        let specs = resolve_run(&f, &[], Some("example_dual_ma"), None, None, None).unwrap();
        assert_eq!(specs[0].strategy, "example_dual_ma"); // CLI 显式最高
    }

    #[test]
    fn run_no_symbols_anywhere_errors() {
        let f = FileConfig::default();
        assert!(resolve_run(&f, &[], None, None, None, None).is_err());
    }

    // ===== config::load 文件 IO 路径 =====

    #[test]
    fn load_explicit_missing_file_errors() {
        // 显式指定却不存在 = 配置错误（不静默回落）
        let p = std::path::Path::new("/nonexistent/strategy.config.yaml");
        assert!(load(Some(p)).is_err());
    }

    #[test]
    fn load_explicit_yaml_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("strategy.config.yaml");
        std::fs::write(&p, "strategy: x\nsymbols:\n  - symbol: BTCUSDT\n").unwrap();
        let cfg = load(Some(&p)).unwrap();
        assert_eq!(cfg.strategy.as_deref(), Some("x"));
    }

    #[test]
    fn load_explicit_file_picks_up_global_fallback() {
        // 单文件模式：同目录 _global.yaml 作为共享基线补挂（单文件显式字段优先）
        // 与 env_secret_* 测试隔离：env var 是进程全局态，并行下会污染 services
        let _lock = CWD_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::remove_var(STRATEGY_SECRET_ENV);
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("strategy.config.yaml"),
            "strategy: x\ncash: 50.0\nsymbols:\n  - symbol: BTCUSDT\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("_global.yaml"),
            "strategy: g\ncash: 999.0\nservices:\n  kline:\n    backfill_url: https://g.example\n    ws_url: wss://g.example/ws\n  trading:\n    ws_url: wss://t.example/ws\n",
        )
        .unwrap();
        let cfg = load(Some(&dir.path().join("strategy.config.yaml"))).unwrap();
        assert_eq!(cfg.strategy.as_deref(), Some("x"), "单文件显式字段优先");
        assert_eq!(cfg.cash, Some(50.0));
        let services = cfg.services.expect("services 应从 _global.yaml 补挂");
        assert_eq!(services.kline.backfill_url, "https://g.example");
    }

    #[test]
    fn load_explicit_file_without_global_is_unchanged() {
        // 与 env_secret_* 测试隔离：并行下 STRATEGY_SECRET 会注入 services
        let _lock = CWD_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::remove_var(STRATEGY_SECRET_ENV);
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("strategy.config.yaml");
        std::fs::write(&p, "strategy: x\nsymbols:\n  - symbol: BTCUSDT\n").unwrap();
        let cfg = load(Some(&p)).unwrap();
        assert!(cfg.services.is_none());
    }

    #[test]
    fn load_explicit_file_global_with_symbols_errors() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("strategy.config.yaml");
        std::fs::write(&p, "strategy: x\nsymbols:\n  - symbol: BTCUSDT\n").unwrap();
        std::fs::write(
            dir.path().join("_global.yaml"),
            "symbols:\n  - symbol: ETHUSDT\n",
        )
        .unwrap();
        let err = load(Some(&p)).unwrap_err().to_string();
        assert!(err.contains("不允许包含 symbols"), "{err}");
    }

    #[test]
    fn load_explicit_json_file_still_works() {
        // JSON 兼容：显式指定 .json 文件也能读（旧配置平滑迁移）
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("strategy.config.json");
        std::fs::write(
            &p,
            r#"{"strategy": "x", "symbols": [{"symbol": "BTCUSDT"}]}"#,
        )
        .unwrap();
        let cfg = load(Some(&p)).unwrap();
        assert_eq!(cfg.strategy.as_deref(), Some("x"));
    }

    #[test]
    fn load_explicit_invalid_content_errors() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("strategy.config.yaml");
        std::fs::write(&p, "{ not yaml either").unwrap();
        let err = load(Some(&p)).unwrap_err().to_string();
        assert!(err.contains("配置文件解析失败"), "{err}");
    }

    #[test]
    fn load_default_path_missing_returns_empty() {
        // 默认路径不存在 = 纯 CLI 兼容（空配置）。默认路径由工作目录决定，
        // 切到 tempdir 运行以验证"不存在 → 空配置"分支。
        let dir = tempfile::tempdir().unwrap();
        let _g = CwdGuard::enter(dir.path());
        let cfg = load(None).unwrap();
        assert_eq!(cfg.strategy, None);
        assert!(cfg.symbols.is_empty());
    }

    #[test]
    fn load_default_path_prefers_yaml_over_json() {
        let dir = tempfile::tempdir().unwrap();
        let _g = CwdGuard::enter(dir.path());
        std::fs::write(
            std::path::Path::new("./strategy.config.yaml"),
            "cash: 55.0\n",
        )
        .unwrap();
        std::fs::write(
            std::path::Path::new("./strategy.config.json"),
            r#"{"cash": 77.0}"#,
        )
        .unwrap();
        let cfg = load(None).unwrap();
        assert_eq!(cfg.cash, Some(55.0)); // yaml 优先
    }

    // ===== 目录式配置（config/{strategy}/{SYMBOL}.yaml）=====

    /// 建目录式配置：config/{strategy}/ 下若干 {SYMBOL}.yaml
    fn make_dir_cfg(
        root: &std::path::Path,
        strategy: &str,
        files: &[(&str, String)],
    ) -> std::path::PathBuf {
        let dir = root.join("config").join(strategy);
        std::fs::create_dir_all(&dir).unwrap();
        for (name, content) in files {
            std::fs::write(dir.join(name), content).unwrap();
        }
        root.join("config").join(strategy)
    }

    #[test]
    fn dir_mode_loads_each_symbol_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[
                (
                    "BTCUSDT.yaml",
                    "strategy: example_strategy\ntimeframe: 8h\ncash: 200.0\n".into(),
                ),
                (
                    "ETHUSDT.yaml",
                    "strategy: example_strategy\ntimeframe: 4h\ncash: 100.0\nparams:\n  st_factor: 2.5\n".into(),
                ),
            ],
        );
        let cfg = load(Some(&dir)).unwrap();
        assert_eq!(cfg.symbols.len(), 2);
        assert_eq!(cfg.symbols[0].symbol, "BTCUSDT");
        assert_eq!(cfg.symbols[1].symbol, "ETHUSDT");

        // per-symbol 差异生效
        let specs = resolve_run(&cfg, &[], None, None, None, None).unwrap();
        assert_eq!(specs[0].timeframe, "8h");
        assert_eq!(specs[0].cash, 200.0);
        assert_eq!(specs[1].timeframe, "4h");
        assert_eq!(specs[1].cash, 100.0);
        assert_eq!(specs[1].params.as_ref().unwrap()["st_factor"], 2.5);
    }

    #[test]
    fn dir_mode_symbol_defaults_to_filename() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[("SOLUSDT.yaml", "strategy: example_strategy\n".into())],
        );
        let cfg = load(Some(&dir)).unwrap();
        assert_eq!(cfg.symbols.len(), 1);
        assert_eq!(cfg.symbols[0].symbol, "SOLUSDT"); // 文件名推断，无需写 symbols:
    }

    #[test]
    fn dir_mode_global_fields_from_first_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[
                (
                    "BTCUSDT.yaml",
                    "strategy: example_strategy\nmode: paper_trading\nrisk:\n  fixed_stop_loss_pct: 2.0\n".into(),
                ),
                (
                    "ETHUSDT.yaml",
                    "strategy: example_strategy\nmode: live\nrisk:\n  fixed_stop_loss_pct: 5.0\n".into(),
                ),
            ],
        );
        let cfg = load(Some(&dir)).unwrap();
        // risk 仍是目录级全局（取首个文件）；mode 已下沉 per-symbol，不再被首文件劫持
        assert_eq!(cfg.risk.as_ref().unwrap().fixed_stop_loss_pct, 2.0);
    }

    // ===== per-symbol mode（U1-U5）=====

    /// U1/U5：每个 per-symbol 文件的 mode 独立生效，不被"路径排序后首个非空"劫持
    #[test]
    fn dir_mode_per_symbol_mode_is_independent() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[
                (
                    "BTCUSDT.yaml",
                    "strategy: example_strategy\nmode: live\n".into(),
                ),
                (
                    "WLDUSDT.yaml",
                    "strategy: example_strategy\nmode: paper_trading\n".into(),
                ),
                (
                    "ZECUSDT.yaml",
                    "strategy: example_strategy\nmode: live\n".into(),
                ),
            ],
        );
        let cfg = load(Some(&dir)).unwrap();
        let specs = resolve_run(&cfg, &[], None, None, None, None).unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].mode, "live", "BTCUSDT 应为 live");
        assert_eq!(specs[1].mode, "paper_trading", "WLDUSDT 应为 paper_trading");
        assert_eq!(specs[2].mode, "live", "ZECUSDT 应为 live");
    }

    /// U2：CLI --mode 显式给出 = 强制全覆盖所有 symbol
    #[test]
    fn resolve_run_cli_mode_overrides_all_symbols() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[
                ("BTCUSDT.yaml", "strategy: s\nmode: live\n".into()),
                ("WLDUSDT.yaml", "strategy: s\nmode: paper_trading\n".into()),
            ],
        );
        let cfg = load(Some(&dir)).unwrap();
        let specs = resolve_run(&cfg, &[], None, None, None, Some("paper_trading")).unwrap();
        assert_eq!(specs[0].mode, "paper_trading");
        assert_eq!(specs[1].mode, "paper_trading");
    }

    /// U3a：per-symbol 未写 mode → 回落 _global.yaml 的 mode
    #[test]
    fn resolve_run_mode_falls_back_to_global_baseline() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[
                ("BTCUSDT.yaml", "strategy: s\n".into()),
                ("WLDUSDT.yaml", "strategy: s\nmode: live\n".into()),
            ],
        );
        std::fs::write(dir.join("_global.yaml"), "mode: paper_trading\n").unwrap();
        let cfg = load(Some(&dir)).unwrap();
        let specs = resolve_run(&cfg, &[], None, None, None, None).unwrap();
        assert_eq!(specs[0].mode, "paper_trading", "BTCUSDT 缺省应回落 _global");
        assert_eq!(specs[1].mode, "live", "WLDUSDT 显式值优先");
    }

    /// U3b：per-symbol 与 _global 均未写 mode → 默认 paper_trading
    #[test]
    fn resolve_run_mode_falls_back_to_default() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[("BTCUSDT.yaml", "strategy: s\n".into())],
        );
        let cfg = load(Some(&dir)).unwrap();
        let specs = resolve_run(&cfg, &[], None, None, None, None).unwrap();
        assert_eq!(specs[0].mode, "paper_trading");
    }

    /// U4：非法 mode 值报错并指明 symbol 名
    #[test]
    fn resolve_run_invalid_mode_reports_symbol_name() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[
                ("BTCUSDT.yaml", "strategy: s\nmode: live\n".into()),
                ("WLDUSDT.yaml", "strategy: s\nmode: smoking\n".into()),
            ],
        );
        let cfg = load(Some(&dir)).unwrap();
        let err = resolve_run(&cfg, &[], None, None, None, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("WLDUSDT"), "错误应指明 symbol: {err}");
        assert!(err.contains("mode"), "错误应提及 mode: {err}");
    }

    #[test]
    fn dir_mode_empty_dir_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("config").join("example_strategy");
        std::fs::create_dir_all(&dir).unwrap();
        let err = load(Some(&dir)).unwrap_err().to_string();
        assert!(err.contains("无 YAML 文件"), "{err}");
    }

    #[test]
    fn dir_mode_non_yaml_files_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[("BTCUSDT.yaml", "strategy: example_strategy\n".into())],
        );
        std::fs::write(dir.join("README.md"), "忽略我").unwrap();
        std::fs::write(dir.join("notes.txt"), "忽略我").unwrap();
        let cfg = load(Some(&dir)).unwrap();
        assert_eq!(cfg.symbols.len(), 1);
    }

    #[test]
    fn dir_mode_files_sorted_deterministically() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[
                ("SOLUSDT.yaml", "strategy: s\n".into()),
                ("BTCUSDT.yaml", "strategy: s\n".into()),
                ("ETHUSDT.yaml", "strategy: s\n".into()),
            ],
        );
        let cfg = load(Some(&dir)).unwrap();
        let symbols: Vec<&str> = cfg.symbols.iter().map(|e| e.symbol.as_str()).collect();
        assert_eq!(symbols, ["BTCUSDT", "ETHUSDT", "SOLUSDT"]); // 文件名序，与创建顺序无关
    }

    #[test]
    fn dir_mode_multi_symbol_file_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[(
                "BTCUSDT.yaml",
                "symbols:\n  - symbol: BTCUSDT\n  - symbol: ETHUSDT\n".into(),
            )],
        );
        let err = load(Some(&dir)).unwrap_err().to_string();
        assert!(err.contains("只能有一个 symbol"), "{err}");
    }

    #[test]
    fn default_path_falls_back_to_config_dir() {
        // 单文件不存在 + ./config 存在 → 目录模式
        let tmp = tempfile::tempdir().unwrap();
        make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[(
                "BTCUSDT.yaml",
                "strategy: example_strategy\ncash: 42.0\n".into(),
            )],
        );
        let _g = CwdGuard::enter(tmp.path());
        let cfg = load(None).unwrap();
        assert_eq!(cfg.symbols[0].symbol, "BTCUSDT");
        assert_eq!(cfg.cash, Some(42.0));
    }

    // ===== _global.yaml 共享全局文件 =====

    /// _global.yaml + per-symbol 文件：建目录树
    fn make_dir_with_global(
        root: &std::path::Path,
        global: &str,
        symbol_files: &[(&str, String)],
    ) -> std::path::PathBuf {
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        std::fs::write(config_dir.join("_global.yaml"), global).unwrap();
        for (rel, content) in symbol_files {
            let p = config_dir.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, content).unwrap();
        }
        config_dir
    }

    const GLOBAL_SERVICES: &str = "
services:
  kline:
    backfill_url: https://global.example
    ws_url: wss://global.example/ws
    reconnect_backoff_max_secs: 99
  trading:
    ws_url: ws://global.example:9100/ws
    request_timeout_ms: 3000
    submit_enabled: true
    reconnect_backoff_max_secs: 88
    strategy_secret: global-placeholder
    exchange: global_ex
    account: global_acct
";

    #[test]
    fn resolve_backtest_picks_per_symbol_entry_in_dir_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let btc = [
            "strategy: example_strategy",
            "timeframe: 4h",
            "cash: 200.0",
            "leverage: 1",
            "exchange: binance",
            "account: test_binance",
            "params:",
            "  kline_window: 200",
            "  sar:",
            "    timeframe: 2h",
            "    kline_window: 200",
            "  supertrend:",
            "    timeframe: 4h",
            "    factor: 3.0",
            "",
        ]
        .join("\n");
        // ETH 给一组不同的 per-symbol 值，证明选中的是正确条目
        let eth = btc
            .replace("cash: 200.0", "cash: 100.0")
            .replace("    factor: 3.0", "    factor: 2.5");
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[("BTCUSDT.yaml", btc), ("ETHUSDT.yaml", eth)],
        );
        let cfg = load(Some(&dir)).unwrap();

        // CLI 显式选中 BTC：per-symbol params/顶层字段必须生效
        let spec = resolve_backtest(&cfg, Some("BTCUSDT"), None, None, None).unwrap();
        assert_eq!(spec.symbol, "BTCUSDT");
        assert_eq!(spec.timeframe, "4h");
        assert_eq!(spec.cash, 200.0);
        assert_eq!(spec.leverage, 1);
        assert_eq!(spec.exchange, "binance");
        assert_eq!(spec.account, "test_binance");
        let params = spec.params.expect("目录模式应携带 per-symbol params");
        assert_eq!(params["sar"]["timeframe"], "2h");
        assert_eq!(params["supertrend"]["factor"], 3.0);

        // 选中第二个条目
        let spec = resolve_backtest(&cfg, Some("ETHUSDT"), None, None, None).unwrap();
        assert_eq!(spec.symbol, "ETHUSDT");
        assert_eq!(spec.cash, 100.0);
        assert_eq!(spec.params.unwrap()["supertrend"]["factor"], 2.5);

        // 无 CLI symbol：取首条目
        let spec = resolve_backtest(&cfg, None, None, None, None).unwrap();
        assert_eq!(spec.symbol, "BTCUSDT");
        assert_eq!(spec.cash, 200.0);
    }

    #[test]
    fn resolve_run_cli_filter_keeps_per_symbol_fields_in_dir_mode() {
        let tmp = tempfile::tempdir().unwrap();
        let mk = |sym: &str, cash: &str| {
            [
                "strategy: example_strategy",
                "timeframe: 4h",
                &format!("cash: {cash}"),
                "leverage: 1",
                &format!("exchange: {sym}_ex"),
                "account: test_binance",
                "params:",
                "  kline_window: 200",
                "  sar:",
                "    timeframe: 2h",
                "    kline_window: 200",
                "  supertrend:",
                "    timeframe: 4h",
                &format!("    factor: {cash}"), // 借 cash 值做 per-symbol 差异探针
                "",
            ]
            .join("\n")
        };
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[
                ("BNBUSDT.yaml", mk("BNB", "200.0")),
                ("BTCUSDT.yaml", mk("BTC", "150.0")),
                ("FILUSDT.yaml", mk("FIL", "100.0")),
            ],
        );
        let cfg = load(Some(&dir)).unwrap();

        // CLI 只挑 BTC（等价于"排除 BNB/FIL"）：必须带上 BTC 条目的 per-symbol 值
        let specs = resolve_run(&cfg, &["BTCUSDT".to_string()], None, None, None, None).unwrap();
        assert_eq!(specs.len(), 1);
        let s = &specs[0];
        assert_eq!(s.symbol, "BTCUSDT");
        assert_eq!(s.cash, 150.0);
        assert_eq!(s.exchange, "BTC_ex");
        let p = s.params.clone().expect("CLI 过滤后 per-symbol params 丢失");
        assert_eq!(p["sar"]["timeframe"], "2h");
        assert_eq!(p["supertrend"]["factor"], 150.0);
    }

    #[test]
    fn global_file_merges_services_without_symbol_entry() {
        // 与 env_secret_* 测试隔离：持同锁、清除全局变量，避免 env 覆盖占位值
        let _lock = CWD_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::remove_var(STRATEGY_SECRET_ENV);
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_with_global(
            tmp.path(),
            GLOBAL_SERVICES,
            &[(
                "example_strategy/BTCUSDT.yaml",
                "strategy: example_strategy\ntimeframe: 8h\ncash: 200.0\n".into(),
            )],
        );
        let cfg = load(Some(&dir)).unwrap();
        assert_eq!(cfg.symbols.len(), 1, "_global.yaml 不产生 symbol entry");
        assert_eq!(cfg.symbols[0].symbol, "BTCUSDT");
        let s = cfg.services.expect("services 应从 _global.yaml 合并");
        assert_eq!(s.kline.ws_url, "wss://global.example/ws");
        assert_eq!(s.kline.reconnect_backoff_max_secs, 99);
        assert_eq!(s.trading.ws_url, "ws://global.example:9100/ws");
        assert_eq!(s.trading.strategy_secret, "global-placeholder");
    }

    #[test]
    fn per_symbol_global_fields_win_over_global_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_with_global(
            tmp.path(),
            GLOBAL_SERVICES,
            &[(
                "example_strategy/BTCUSDT.yaml",
                "strategy: local_strategy\ntimeframe: 4h\ncash: 50.0\n".into(),
            )],
        );
        let cfg = load(Some(&dir)).unwrap();
        // 全局字段：per-symbol 文件优先于 _global.yaml
        assert_eq!(cfg.strategy.as_deref(), Some("local_strategy"));
        assert_eq!(cfg.timeframe.as_deref(), Some("4h"));
        assert_eq!(cfg.cash, Some(50.0));
        // per-symbol 未给出的字段回落 _global.yaml
        assert_eq!(cfg.services.unwrap().trading.exchange, "global_ex");
    }

    #[test]
    fn global_file_with_symbols_section_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_with_global(
            tmp.path(),
            "symbols:\n  - symbol: BTCUSDT\n",
            &[("example_strategy/BTCUSDT.yaml", "strategy: s\n".into())],
        );
        let err = load(Some(&dir)).unwrap_err().to_string();
        assert!(err.contains("不允许包含 symbols 段"), "{err}");
    }

    #[test]
    fn duplicate_global_files_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_with_global(
            tmp.path(),
            GLOBAL_SERVICES,
            &[
                ("a/BTCUSDT.yaml", "strategy: s\n".into()),
                ("b/_global.yaml", GLOBAL_SERVICES.into()),
            ],
        );
        let err = load(Some(&dir)).unwrap_err().to_string();
        assert!(err.contains("多个共享全局文件"), "{err}");
    }

    #[test]
    fn only_global_file_without_symbol_files_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_with_global(tmp.path(), GLOBAL_SERVICES, &[]);
        let err = load(Some(&dir)).unwrap_err().to_string();
        assert!(err.contains("缺少 per-symbol 文件"), "{err}");
    }

    /// STRATEGY_SECRET 环境变量覆盖（进程全局态，与默认路径测试共用锁串行）
    struct EnvSecretGuard<'a> {
        _lock: std::sync::MutexGuard<'a, ()>,
    }
    impl EnvSecretGuard<'_> {
        fn enter(value: Option<&str>) -> Self {
            let lock = CWD_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match value {
                Some(v) => std::env::set_var(STRATEGY_SECRET_ENV, v),
                None => std::env::remove_var(STRATEGY_SECRET_ENV),
            }
            Self { _lock: lock }
        }
    }
    impl Drop for EnvSecretGuard<'_> {
        fn drop(&mut self) {
            std::env::remove_var(STRATEGY_SECRET_ENV);
        }
    }

    #[test]
    fn env_secret_overrides_global_file() {
        let _g = EnvSecretGuard::enter(Some("env-secret-64hex"));
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_with_global(
            tmp.path(),
            GLOBAL_SERVICES,
            &[("example_strategy/BTCUSDT.yaml", "strategy: s\n".into())],
        );
        let cfg = load(Some(&dir)).unwrap();
        assert_eq!(
            cfg.services.unwrap().trading.strategy_secret,
            "env-secret-64hex"
        );
    }

    #[test]
    fn blank_env_secret_does_not_override() {
        let _g = EnvSecretGuard::enter(Some("   "));
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_with_global(
            tmp.path(),
            GLOBAL_SERVICES,
            &[("example_strategy/BTCUSDT.yaml", "strategy: s\n".into())],
        );
        let cfg = load(Some(&dir)).unwrap();
        assert_eq!(
            cfg.services.unwrap().trading.strategy_secret,
            "global-placeholder"
        );
    }

    #[test]
    fn dir_mode_bad_yaml_reports_file_path() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[("BTCUSDT.yaml", "nope_field: 1\n".into())],
        );
        let err = load(Some(&dir)).unwrap_err().to_string();
        assert!(err.contains("BTCUSDT.yaml"), "错误应包含文件路径: {err}");
    }
}

#[cfg(test)]
mod services_tests {
    use super::*;

    #[test]
    fn services_section_parses_with_defaults() {
        let text = "
strategy: example_strategy
timeframe: 8h
services:
  kline:
    backfill_url: http://kline:50051
    ws_url: ws://kline:50052/kline
    reconnect_backoff_max_secs: 30
  trading:
    ws_url: ws://trading:9100/ws
    request_timeout_ms: 2000
    submit_enabled: true
    reconnect_backoff_max_secs: 45
    strategy_secret: dev-shared-secret
    exchange: binance
    account: test_binance
symbols:
  - symbol: ETHUSDT
";
        let f: FileConfig = serde_yaml::from_str(text).unwrap();
        let s = f.services.expect("services 应解析");
        assert_eq!(s.kline.ws_url, "ws://kline:50052/kline");
        assert_eq!(s.kline.reconnect_backoff_max_secs, 30);
        assert_eq!(s.trading.ws_url, "ws://trading:9100/ws");
        assert_eq!(s.trading.request_timeout_ms, 2000);
        assert!(s.trading.submit_enabled);
        assert_eq!(s.trading.reconnect_backoff_max_secs, 45);
        assert_eq!(s.trading.strategy_secret, "dev-shared-secret");
        assert_eq!(s.trading.exchange, "binance");
        assert_eq!(s.trading.account, "test_binance");
    }

    #[test]
    fn run_per_symbol_exchange_account_leverage_win() {
        // 顶层 exchange/account/leverage 为全局基准；per-symbol 可各自覆盖
        let f: FileConfig = serde_json::from_str(
            r#"{
              "strategy": "example_strategy", "leverage": 2,
              "exchange": "binance", "account": "global_acct",
              "symbols": [
                {"symbol": "BTCUSDT", "exchange": "binance", "account": "test_binance", "leverage": 3},
                {"symbol": "ETHUSDT"}
              ]
            }"#,
        )
        .unwrap();
        let specs = resolve_run(&f, &[], None, None, None, None).unwrap();
        assert_eq!(specs[0].exchange, "binance");
        assert_eq!(specs[0].account, "test_binance");
        assert_eq!(specs[0].leverage, 3); // per-symbol 覆盖全局 2
                                          // 未覆盖者回落全局
        assert_eq!(specs[1].exchange, "binance");
        assert_eq!(specs[1].account, "global_acct");
        assert_eq!(specs[1].leverage, 2);
    }

    #[test]
    fn run_exchange_account_falls_back_to_services_section() {
        // 无顶层 exchange/account 时回落 services.trading
        let text = "
strategy: example_strategy
services:
  trading:
    ws_url: ws://x
    request_timeout_ms: 1000
    submit_enabled: false
    reconnect_backoff_max_secs: 1
    strategy_secret: secret
    exchange: binance
    account: svc_acct
symbols:
  - symbol: SOLUSDT
";
        let f: FileConfig = serde_yaml::from_str(text).unwrap();
        let specs = resolve_run(&f, &[], None, None, None, None).unwrap();
        assert_eq!(specs[0].exchange, "binance");
        assert_eq!(specs[0].account, "svc_acct");
        assert_eq!(specs[0].leverage, 1); // Defaults::LEVERAGE
    }

    #[test]
    fn unknown_services_key_rejected() {
        let text = "
services:
  kline:
    backfill_url: http://x
    ws_url: ws://x
    reconnect_backoff_max_secs: 1
    bogus: 1
";
        assert!(serde_yaml::from_str::<FileConfig>(text).is_err());
    }
}
