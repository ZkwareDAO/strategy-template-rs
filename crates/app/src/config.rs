//! 运行配置 — 模板冻结文件，新增策略不修改
//!
//! 配置文件 `strategy.config.yaml`（默认路径，可用 --config 覆盖）。
//! schema 与 Python 侧 `backtest/config/{strategy}/{SYMBOL}.yaml` 同构，
//! Python 侧 YAML 可直接使用：
//!
//! ```yaml
//! strategy: example_strategy          # 顶层扁平 key（Python 侧以策略名为 key 包裹，两边都兼容）
//! symbol: BTCUSDT                     # 本文件的标的身份（flat 文件用；缺省回落文件名 stem）
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
//!
//! **目录模式（`--config <dir>`）的身份铁律**：标的身份类字段
//! （`strategy/timeframe/cash/initial_cash/leverage/exchange/account`）只来自
//! ① 声明该标的的那个文件，② `_global.yaml` 的目录级基线，③ 内置默认——
//! **绝不来自同目录里别的 symbol 文件**。"全局"不等于"字母序第一个文件"。
//! 由此，`--symbol X` 在目录内找不到自己的条目时直接报错（列出可用标的），
//! 而不是静默套用兄弟文件的交易所，导致行键错位（`SHFE/crypto_perpetual/BTCUSDT`）。

use std::path::Path;

use serde::Deserialize;
use strategy_core::contract::ContractSpec;
use strategy_core::risk::RiskControlConfig;

/// `bar_merge` 为**历史字段**（旧 `data::Feed` 的聚合开关）。统一引擎的聚合规则
/// 由 `Exchange::aggregation_policy` 提供（`engine/src/core/aggregation.rs`），
/// 该字段不再影响任何行为；保留解析只为兼容存量 yaml（写了不硬错），并在检测到
/// 显式声明时告警——消除「写了不生效也不报错」的用户陷阱。
/// 清退登记：docs/exchange-spi-retirement.md §5.1（D1 残留）。
fn warn_bar_merge_deprecated(raw: Option<&str>) {
    if let Some(s) = raw {
        tracing::warn!(
            "配置项 bar_merge={s:?} 已废弃、不再生效（聚合规则来自交易所 SPI），可安全删除该行"
        );
    }
}

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

/// 期货合约参数文件名（不含扩展名；不产生 symbol/runtime）。
/// 支持 `contracts.yaml` / `_contracts.yaml`，由回测直接读取手续费/保证金。
pub const CONTRACTS_FILE_STEM: &str = "contracts";

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
    /// K 线聚合模式——**已废弃**：聚合规则改由交易所 SPI 的
    /// `BarAggregationPolicy` 决定。此键仅为兼容存量 yaml 保留，写了不生效，
    /// 加载时告警一次（见 [`warn_bar_merge_deprecated`]）。
    pub bar_merge: Option<String>,
    /// 默认每单名义金额
    pub cash: Option<f64>,
    /// 回测账户初始权益（缺省 = 撮合内核默认 5000）
    pub initial_cash: Option<f64>,
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
    /// 国内期货合约参数表（按 symbol；`config/contracts.yaml` 自动加载）
    #[serde(default)]
    pub contracts: std::collections::BTreeMap<String, ContractSpec>,
    /// 本文件所属标的——**文件身份声明**。
    /// 目录模式下无 `symbols:` 块的 flat 文件用它确定 symbol（显式身份优先于文件名
    /// stem；`{strategy}/{SYMBOL}.yaml` 约定仍有效，只是文件名是*隐式*身份，容易与
    /// 内容不一致，例如模板 `example.yaml` 的名字并不等于它描述的标的）。
    /// 单文件模式：CLI 省略 `--symbol` 时用它兜底。
    pub symbol: Option<String>,
    /// per-symbol 覆盖列表
    pub symbols: Vec<SymbolEntry>,
    /// 外部服务端点（run 命令必需；backtest/download 不需要）
    #[serde(default)]
    pub services: Option<ServicesConfig>,
    /// 阶段 2（§8）：按交易所分组（键 = 实例名，`instruments:` 用它引用；
    /// 值 = 类型配置子树，由 strategy-engine 解析 binance | ctp）。
    /// 目录模式为**共享基础设施**字段：首个声明者胜（标的身份类字段不参与，
    /// 见模块头"身份铁律"）。
    #[serde(default)]
    pub exchanges: std::collections::BTreeMap<String, serde_json::Value>,
    /// 阶段 2（§8）：单进程混跑的标的清单（可跨交易所）。
    /// 旧 `symbols:` 链路不受影响；阶段 3 引擎适配器开始消费本清单。
    #[serde(default)]
    pub instruments: Vec<InstrumentEntry>,
    /// live/paper 状态定时全 Store flush 间隔（秒，缺省 60）。仅 run 生效：
    /// live actor（`spi_live_actor`）按此间隔时间驱动落盘；回测结束时只 flush 一次，不用此值。
    #[serde(default = "default_state_flush_interval_secs")]
    pub state_flush_interval_secs: u64,
    /// 跨日 flush 时是否物理裁剪终态行（缺省 true）。false = 旧行为：
    /// 终态行保留并继续落盘，recover 读全部日文件（spec §设计.7）。
    #[serde(default = "default_true")]
    pub state_prune_terminal: bool,
    /// **来源标记（非 yaml 字段）**：本配置是否由目录模式（`--config <dir>`）装配。
    /// 只有目录模式才存在"兄弟文件"，也才可能把别人的 `exchange` 漏给本标的，
    /// 因此"名实相符"校验（[`ensure_declared`]）只在此为 true 时生效：
    /// 单文件配置里 `--symbol` 给出未声明的标的仍是合法的模板复用
    /// （所有回落值都来自这一个文件自己，不存在跨文件污染）。
    #[serde(skip)]
    pub from_dir: bool,
}

/// 状态定时 flush 默认间隔（秒）
fn default_state_flush_interval_secs() -> u64 {
    60
}

fn default_true() -> bool {
    true
}

/// trading-service 端点（设计文档 §3；klines-service 退役后 K 线与订单共用同一连接）
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct ServicesConfig {
    /// **已退役——tombstone**：旧 `services.kline` 节（外部 klines-service WS+REST
    /// 不复存在）。解析仍放行——存量 yaml 因此能在启动时拿到迁移指引，而不是
    /// serde 的 unknown-field 裸报错；但 `run` 路径检测到本节即拒启
    /// （[`check_kline_retired`]，名实不符即拒启，与 62ff2fa 同一家族）。
    /// K 线一律经 `services.trading` 同一连接的 `subscribe_kline` 推送
    /// （定稿 `kline_1m_batch`，历史 `get_klines`；契约 contracts/trading-service.md §8）。
    pub kline: Option<serde_yaml::Value>,
    pub trading: TradingServiceCfg,
}

/// `services.kline` 退役检查：节仍存在即失败并给出迁移指引（`run` 启动入口调用；
/// `bootstrap::run_live` 内另有双保险）。
pub fn check_kline_retired(services: &ServicesConfig) -> anyhow::Result<()> {
    if services.kline.is_some() {
        anyhow::bail!(
            "services.kline 已退役（klines-service 不复存在）：请整节删除 backfill_url / ws_url / \
             reconnect_backoff_max_secs / market / source——K 线现经交易连接（与 services.trading \
             同一条 WS）`subscribe_kline` 订阅收取，推送为定稿 `kline_1m_batch`，历史回填走 \
             `get_klines`；见 contracts/trading-service.md §8 与 CHANGELOG 迁移说明"
        );
    }
    Ok(())
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
            ws_url: PLACEHOLDER_TRADING_WS_URL.into(),
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
    /// 已废弃字段（同 `FileConfig::bar_merge`）：保留仅为兼容存量 yaml，写了不生效
    pub bar_merge: Option<String>,
    pub cash: Option<f64>,
    /// 回测账户初始权益（缺省回落文件顶层/默认）
    pub initial_cash: Option<f64>,
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

/// 未配置 `services.trading.ws_url` 时使用的占位端点。
///
/// 它同时是「交易通道从未被真实配置」的判据（见 [`TradingServiceCfg::is_unconfigured`]）：
/// 缺陷登记于 `docs/exchange-spi-retirement.md` §7 风险 8——此前 live 实例会静默用
/// 这个不存在的网关跑成「永不下单且零信号」。
pub const PLACEHOLDER_TRADING_WS_URL: &str = "ws://127.0.0.1:9100/ws";

impl TradingServiceCfg {
    /// 交易通道是否仍停留在占位端点（= 未真实配置）。
    ///
    /// **刻意只看 `ws_url`，不看 `submit_enabled`**：`submit_enabled: false` 配真实端点
    /// 是 RUNBOOK §2.1 的**紧急熔断**（运维故意让 live 不发单，随时可开回来），
    /// 不能被当成"未配置"而拒绝启动；而占位端点意味着连的是空气，熔断都够不着。
    pub fn is_unconfigured(&self) -> bool {
        self.ws_url == PLACEHOLDER_TRADING_WS_URL
    }
}

/// §8 `instruments:` 条目（阶段 2 混跑入口；阶段 3 引擎适配器消费）。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentEntry {
    /// `exchanges:` 中的实例名，或直接交易所 id（如 `SHFE`）
    pub exchange: String,
    /// 品种码 / 挂牌合约 / 交易对：`CU` / `CU2601` / `BTCUSDT`；
    /// 命名空间解析交由对应交易所的 `parse_instrument`
    pub product: String,
    /// 策略名（缺省回落目录顶层 strategy）
    #[serde(default)]
    pub strategy: Option<String>,
    /// 主周期（缺省回落目录顶层 timeframe）
    #[serde(default)]
    pub timeframe: Option<String>,
    /// `main`（走订阅计划选主）| `second_main` | `contract`（按 product 字面）
    #[serde(default)]
    pub role: Option<String>,
}

/// `instruments:` 跨文件合并：按 (实例名, 品种) 去重，先到先得。
fn push_instruments(dst: &mut Vec<InstrumentEntry>, src: Vec<InstrumentEntry>) {
    for e in src {
        let dup = dst.iter().any(|d| {
            d.exchange.eq_ignore_ascii_case(&e.exchange)
                && d.product.eq_ignore_ascii_case(&e.product)
        });
        if !dup {
            dst.push(e);
        }
    }
}

/// 合并后的单 symbol 运行规格
#[derive(Debug, Clone)]
pub struct SymbolSpec {
    pub strategy: String,
    pub symbol: String,
    pub timeframe: String,
    pub cash: f64,
    /// 回测账户初始权益（None = 撮合内核默认 5000）
    pub initial_cash: Option<f64>,
    pub params: Option<serde_json::Value>,
    pub warmup_days: u32,
    /// place_order 帧字段：交易所实例名
    pub exchange: String,
    /// 行键/数据路径命名空间键（§6.1，阶段 5c run 合并步）：
    /// `{exchange}/{market}/{symbol}`，由 `spi_backtest::venue_key_for` 派生——
    /// CSV 路径、报表目录、实例名、信号行键 venue_key 的共同单一源。
    pub venue_key: String,
    /// place_order 帧字段：账户名
    pub account: String,
    /// place_order 帧字段：杠杆倍数
    pub leverage: u32,
    /// 运行模式 live / paper_trading（决定下单网关与 submit 闸门）
    pub mode: String,
    /// 国内期货合约参数（None = crypto/兼容模式）
    pub contract: Option<ContractSpec>,
    /// 场所数量约束（来自 `exchanges:` 子树 per-symbol 规格；None = 适配器默认）。
    /// 低于 `venue_qty_step` 的平仓差额由 `normalize_qty` 取整为 0（视为已达标），
    /// 避免发单被场所以数量越界拒绝。
    pub venue_qty_step: Option<f64>,
    pub venue_min_qty: Option<f64>,
    pub venue_qty_precision: Option<u8>,
    /// 统一风控配置（顶层 risk 节；回测与 live/paper 同源）
    pub risk: RiskControlConfig,
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

// ─── 阶段 2（§8 启动流程 2/3）：按配置装配交易所注册表 ─────────────────────

/// `instruments:` 条目的解析结果：命名空间标的 + 每标的策略覆盖。
#[derive(Debug, Clone)]
pub struct ResolvedInstrument {
    pub id: strategy_engine::core::identity::InstrumentId,
    pub strategy: Option<String>,
    pub timeframe: Option<String>,
    pub role: Option<String>,
}

/// §8 启动流程第 2 步：`exchanges:` 分组 → [`ExchangeRegistry`]。
///
/// 阶段 2 仅提供装配入口；旧 `symbols:` 运行链路不消费注册表（阶段 3
/// 引擎适配器接入后切换）。未配置 `exchanges:` 时报错而非静默空注册表。
pub fn build_exchange_registry(
    cfg: &FileConfig,
) -> anyhow::Result<strategy_engine::registry::ExchangeRegistry> {
    anyhow::ensure!(
        !cfg.exchanges.is_empty(),
        "配置缺少 `exchanges:` 分组（阶段 2 装配入口；旧单交易所模式可不声明）"
    );
    strategy_engine::exchanges::build_registry(&cfg.exchanges)
        .map_err(|e| anyhow::anyhow!("装配交易所注册表失败: {e}"))
}

/// `exchanges:` 已声明 → 装配注册表（registry 路由校验用）；未声明 →
/// `None`（旧单交易所配置零回归）。
fn registry_for(
    file: &FileConfig,
) -> anyhow::Result<Option<strategy_engine::registry::ExchangeRegistry>> {
    if file.exchanges.is_empty() {
        Ok(None)
    } else {
        Ok(Some(build_exchange_registry(file)?))
    }
}

/// §8 启动流程第 3 步：`instruments:` 逐条按注册表解析为强类型标的。
///
/// 校验点：实例名必须已注册、role 取值合法、product 必须能被对应交易所
/// `parse_instrument` 规范化。装配期报错，不留到下单链路。
pub fn resolve_instruments(
    cfg: &FileConfig,
    registry: &strategy_engine::registry::ExchangeRegistry,
) -> anyhow::Result<Vec<ResolvedInstrument>> {
    let mut out = Vec::with_capacity(cfg.instruments.len());
    for e in &cfg.instruments {
        let ex = registry
            .get_by_name(&e.exchange)
            .map_err(|err| anyhow::anyhow!("instruments 条目 `{}`: {err}", e.exchange))?;
        if let Some(role) = &e.role {
            anyhow::ensure!(
                matches!(role.as_str(), "main" | "second_main" | "contract"),
                "instruments.{}: 不支持的 role `{role}`（main | second_main | contract）",
                e.product
            );
        }
        let id = ex
            .parse_instrument(&e.product)
            .map_err(|err| anyhow::anyhow!("标的解析失败: {err}"))?;
        out.push(ResolvedInstrument {
            id,
            strategy: e.strategy.clone(),
            timeframe: e.timeframe.clone(),
            role: e.role.clone(),
        });
    }
    Ok(out)
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
    apply_contracts_fallback(&mut cfg, path)?;
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
        let g =
            parse_config(&text).map_err(|err| anyhow::anyhow!("{}: {err}", candidate.display()))?;
        anyhow::ensure!(
            g.symbols.is_empty(),
            "{}: 共享全局文件不允许包含 symbols 段",
            candidate.display()
        );
        anyhow::ensure!(
            g.symbol.is_none(),
            "{}: 共享全局文件不属于单个标的，不允许声明顶层 symbol:",
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
        cfg.initial_cash = cfg.initial_cash.take().or(g.initial_cash);
        merge_contracts(&mut cfg.contracts, g.contracts, false);
        break; // 至多一个 _global 生效
    }
    Ok(())
}

/// 判断是否为共享全局文件（文件名 stem == [`GLOBAL_FILE_STEM`]）
fn is_global_file(path: &Path) -> bool {
    path.file_stem().and_then(|s| s.to_str()) == Some(GLOBAL_FILE_STEM)
}

/// 判断是否为期货合约参数文件（`contracts.yaml` / `_contracts.yaml`）
fn is_contracts_file(path: &Path) -> bool {
    matches!(
        path.file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.trim_start_matches('_')),
        Some(CONTRACTS_FILE_STEM)
    )
}

/// 合并合约表（键统一大写；`overwrite=false` 时保留已有条目）
fn merge_contracts(
    dst: &mut std::collections::BTreeMap<String, ContractSpec>,
    src: std::collections::BTreeMap<String, ContractSpec>,
    overwrite: bool,
) {
    for (k, v) in src {
        let key = k.to_uppercase();
        if overwrite {
            dst.insert(key, v);
        } else {
            dst.entry(key).or_insert(v);
        }
    }
}

/// 从 `exchanges:` 子树读取 per-symbol 数量规格。
/// 键匹配：实例名 → 实例 `id`；symbol 大小写不敏感。返回 `None` 表示
/// 该交易所/标的未配置，执行端沿用适配器默认（低于步长的平仓差额
/// 由 `normalize_qty` 取整为 0，视为已达标，不发单被拒）。
pub(crate) fn venue_qty_spec_for(
    file: &FileConfig,
    exchange: &str,
    venue_exchange: &str,
    symbol: &str,
) -> (Option<f64>, Option<f64>, Option<u8>) {
    let names = [exchange, venue_exchange];
    let cfg = file
        .exchanges
        .iter()
        .find(|(k, v)| {
            names.iter().any(|n| {
                k.eq_ignore_ascii_case(n)
                    || v.get("id")
                        .and_then(|x| x.as_str())
                        .is_some_and(|id| id.eq_ignore_ascii_case(n))
            })
        })
        .map(|(_, v)| v);
    let Some(cfg) = cfg else {
        return (None, None, None);
    };
    let upper = symbol.to_uppercase();
    let Some(spec) = cfg.get("specs").and_then(|s| s.as_object()).and_then(|s| {
        s.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(&upper))
            .map(|(_, v)| v)
    }) else {
        return (None, None, None);
    };
    (
        spec.get("qty_step").and_then(|v| v.as_f64()),
        spec.get("min_qty").and_then(|v| v.as_f64()),
        spec.get("qty_precision")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8),
    )
}

/// 合约表查找（symbol 大小写不敏感；支持 `CU` / `CU2611` 归一化到品种前缀）
pub fn contract_for<'a>(
    contracts: &'a std::collections::BTreeMap<String, ContractSpec>,
    symbol: &str,
) -> Option<&'a ContractSpec> {
    let upper = symbol.to_uppercase();
    if let Some(c) = contracts.get(&upper) {
        return Some(c);
    }
    contracts.iter().find_map(|(k, v)| {
        let key = k.to_uppercase();
        if key.len() < upper.len() && upper.starts_with(&key) {
            Some(v)
        } else {
            None
        }
    })
}

/// 合约参数文件候选路径（显式配置目录 → 其父目录 → ./config/）
fn contracts_candidates(config_path: Option<&Path>) -> Vec<std::path::PathBuf> {
    let mut out: Vec<std::path::PathBuf> = Vec::new();
    let mut push_dir = |dir: &Path| {
        if dir == Path::new("") {
            return;
        }
        out.push(dir.join("contracts.yaml"));
        out.push(dir.join("contracts.yml"));
        out.push(dir.join("_contracts.yaml"));
        out.push(dir.join("_contracts.yml"));
    };
    match config_path {
        Some(p) if p.is_dir() => {
            push_dir(p);
            if let Some(parent) = p.parent() {
                push_dir(parent);
            }
        }
        Some(p) => {
            if let Some(parent) = p.parent() {
                push_dir(parent);
                if let Some(grand) = parent.parent() {
                    push_dir(grand);
                }
            }
        }
        None => {}
    }
    push_dir(Path::new(DEFAULT_CONFIG_DIR));
    out.push(std::path::PathBuf::from("contracts.yaml"));
    out
}

/// 从候选路径加载第一个存在的合约参数文件（不覆盖主配置已有的合约）
fn apply_contracts_fallback(
    cfg: &mut FileConfig,
    config_path: Option<&Path>,
) -> anyhow::Result<()> {
    for candidate in contracts_candidates(config_path) {
        if !candidate.is_file() {
            continue;
        }
        let text = std::fs::read_to_string(&candidate)
            .map_err(|e| anyhow::anyhow!("读取合约参数失败: {} ({e})", candidate.display()))?;
        let fc =
            parse_config(&text).map_err(|err| anyhow::anyhow!("{}: {err}", candidate.display()))?;
        anyhow::ensure!(
            fc.symbols.is_empty(),
            "{}: 合约参数文件不允许包含 symbols 段",
            candidate.display()
        );
        anyhow::ensure!(
            fc.symbol.is_none(),
            "{}: 合约参数文件按 symbol 建表，不允许声明顶层 symbol:",
            candidate.display()
        );
        merge_contracts(&mut cfg.contracts, fc.contracts, false);
        break;
    }
    Ok(())
}

/// 读取实际生效的合约参数文件文本（复制进回测报告 config.yaml）
pub fn read_contracts_yaml(config_path: Option<&Path>) -> Option<String> {
    for candidate in contracts_candidates(config_path) {
        if candidate.is_file() {
            return std::fs::read_to_string(&candidate).ok();
        }
    }
    None
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
///
/// 全局基线按**目录作用域**：给定根目录下的 `_global.yaml` 是整棵树共享的
/// 基线；子目录下的 `_global.yaml` 只作用于本子树的 symbol 文件（就近优先，
/// 命中即不再回落到根基线——与 [`crate::strategy_config::load_strategy`]
/// 的 `global_for` 同一口径）。
pub fn load_dir(dir: &Path) -> anyhow::Result<FileConfig> {
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    collect_yaml_files(dir, &mut files)?;
    files.sort(); // 路径序确定性（跨平台 diff 验收依赖）

    anyhow::ensure!(
        !files.is_empty(),
        "配置目录无 YAML 文件: {}（期望 {{strategy}}/{{SYMBOL}}.yaml）",
        dir.display()
    );

    // 全局基线按目录作用域：根目录一份共享基线 + 各子目录各自的基线。
    let mut global_cfg: Option<FileConfig> = None;
    let mut scoped_globals: Vec<(std::path::PathBuf, FileConfig)> = Vec::new();
    let mut global_contracts: std::collections::BTreeMap<String, ContractSpec> =
        std::collections::BTreeMap::new();
    let mut symbol_files: Vec<std::path::PathBuf> = Vec::new();
    for path in files {
        if is_contracts_file(&path) {
            let text = std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!("读取配置文件失败: {} ({e})", path.display()))?;
            let fc =
                parse_config(&text).map_err(|err| anyhow::anyhow!("{}: {err}", path.display()))?;
            anyhow::ensure!(
                fc.symbols.is_empty(),
                "{}: 合约参数文件不允许包含 symbols 段",
                path.display()
            );
            anyhow::ensure!(
                fc.symbol.is_none(),
                "{}: 合约参数文件按 symbol 建表，不允许声明顶层 symbol:",
                path.display()
            );
            merge_contracts(&mut global_contracts, fc.contracts, true);
        } else if is_global_file(&path) {
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
                fc.symbol.is_none(),
                "{}: 共享全局文件不属于单个标的，不允许声明顶层 symbol:",
                path.display()
            );
            let parent = path
                .parent()
                .map(std::path::Path::to_path_buf)
                .unwrap_or_else(|| dir.to_path_buf());
            if parent == dir {
                anyhow::ensure!(
                    global_cfg.is_none(),
                    "根目录存在多个共享全局文件（{}），期望至多一个 {GLOBAL_FILE_STEM}.yaml",
                    path.display()
                );
                global_cfg = Some(fc);
            } else {
                anyhow::ensure!(
                    !scoped_globals.iter().any(|(d, _)| d == &parent),
                    "子目录 {} 存在多个共享全局文件（{}），期望至多一个 {GLOBAL_FILE_STEM}.yaml",
                    parent.display(),
                    path.display()
                );
                scoped_globals.push((parent, fc));
            }
        } else if path
            .file_stem()
            .and_then(|s| s.to_str())
            .is_some_and(|s| s == crate::strategy_config::STRATEGY_FILE_STEM)
        {
            // 横截面策略级单文件（config/<strategy>/strategy.yaml）不是 per-symbol
            // 配置——旧 loader 直接跳过，默认 `--config config` 目录模式零误伤；
            // 消费入口是 `crate::strategy_config::load_strategy`（组合根按路径分流）。
            tracing::debug!(file = %path.display(), "目录模式：跳过横截面策略级 strategy.yaml（由 strategy_config 消费）");
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
    // (文件路径, 该文件声明的标的)——用于重名标的告警
    let mut identities: Vec<(std::path::PathBuf, String)> = Vec::new();
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
        // 无 symbols: → 整个文件就是该 symbol 的配置（身份取 `symbol:`，再回落文件名）
        let mut entry = if let Some(mut e) = fc.symbols.into_iter().next() {
            // 同一文件既写 `symbol:` 又写 `symbols:` 且两者不一致 ⇒ 身份含糊，拒收
            if let Some(declared) = fc.symbol.as_deref().filter(|s| !s.trim().is_empty()) {
                anyhow::ensure!(
                    declared.eq_ignore_ascii_case(&e.symbol),
                    "{}: 顶层 symbol: `{declared}` 与 symbols[0].symbol: `{}` 冲突，\
                     一个文件只能有一个标的身份",
                    path.display(),
                    e.symbol
                );
            }
            e.strategy = e.strategy.or(fc.strategy.clone());
            e.timeframe = e.timeframe.or(fc.timeframe.clone());
            // bar_merge 仅向下传递给废弃告警使用（warn_bar_merge_deprecated），
            // 不参与任何行为决策——聚合规则来自交易所 SPI。
            e.bar_merge = e.bar_merge.or(fc.bar_merge.clone());
            e.cash = e.cash.or(fc.cash);
            e.initial_cash = e.initial_cash.or(fc.initial_cash);
            e.params = e.params.or(fc.params.clone());
            e.warmup_days = e.warmup_days.or(fc.warmup_days);
            e.exchange = e.exchange.or(fc.exchange.clone());
            e.account = e.account.or(fc.account.clone());
            e.leverage = e.leverage.or(fc.leverage);
            e.mode = e.mode.or(fc.mode.clone());
            e
        } else {
            // flat 文件身份：显式 `symbol:` > 文件名 stem（大写）。
            // 文件名是隐式约定，`example.yaml` 这类模板名并不等于它描述的标的，
            // 过去只能靠 stem 猜出 "EXAMPLE"，于是 `--symbol BTCUSDT` 找不到自己的
            // 条目、转而去吃别的文件的 exchange（见本函数末尾的赢家清单注释）。
            let declared = fc
                .symbol
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.to_uppercase());
            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_uppercase();
            let symbol = match declared {
                // 显式身份优先，但与文件名不一致时**必须出声**：复制模板
                // （`cp BTCUSDT.yaml ETHUSDT.yaml`）却忘了改 `symbol:` 时，静默取
                // 其一都是坑；取显式值 + warn，是把坑摆在日志里而不是埋进行键里。
                Some(d) => {
                    if !d.eq_ignore_ascii_case(&stem) {
                        tracing::warn!(
                            file = %path.display(),
                            file_stem = %stem,
                            declared = %d,
                            "flat 配置的文件名与顶层 symbol: 不一致，采用显式声明的 symbol:（复制模板后请改对）"
                        );
                    }
                    d
                }
                None => stem,
            };
            anyhow::ensure!(
                !symbol.is_empty(),
                "{}: 无法确定 symbol（文件名 stem 为空且未声明顶层 `symbol:`）",
                path.display()
            );
            SymbolEntry {
                symbol,
                strategy: fc.strategy.clone(),
                timeframe: fc.timeframe.clone(),
                bar_merge: fc.bar_merge.clone(),
                cash: fc.cash,
                initial_cash: fc.initial_cash,
                params: fc.params.clone(),
                warmup_days: fc.warmup_days,
                exchange: fc.exchange.clone(),
                account: fc.account.clone(),
                leverage: fc.leverage,
                mode: fc.mode.clone(),
            }
        };

        // 子目录基线先补 entry（就近优先；per-symbol 文件自己的顶层字段仍先胜）。
        if let Some(g) = nearest_scoped_global(&path, dir, &scoped_globals) {
            fill_entry_from_global(&mut entry, g);
        }

        // 目录级共享字段。**只有基础设施类**字段允许跨文件首个非空者胜：
        //   bar_merge —— 已废弃，仅喂 warn_bar_merge_deprecated，不参与任何决策；
        //   risk      —— 顶层风控预算，SymbolEntry 无对应槽位，目录内共享是唯一去处；
        //   services / contracts / exchanges / instruments —— 服务端点与注册表，
        //              本就是"整个目录共用一套"的东西。
        // **标的身份类字段一律不再进赢家清单**（strategy / timeframe / cash /
        // initial_cash / leverage / exchange / account）：它们只能来自
        //   ① 本文件自己的顶层字段（已在上面折进 entry），或
        //   ② `_global.yaml` 显式声明的目录级基线（见下方 global_cfg 补缺），或
        //   ③ 内置默认。
        // 旧写法让"字母序第一个文件"替所有 symbol 决定 exchange，混放期货与现货的
        // 策略目录里 `--symbol BTCUSDT` 会继承 `CU.yaml` 的 `exchange: SHFE` ⇒ 行键
        // 变成 `SHFE/crypto_perpetual/BTCUSDT` ⇒ 查无数据（2026-11-09 实测）。
        // mode 例外早已存在（U1/U5），本次把同一规则推广到其余身份字段。
        merged.bar_merge = merged.bar_merge.or(fc.bar_merge);
        merged.risk = merged.risk.or(fc.risk);
        merged.services = merged.services.or(fc.services);
        // per-symbol 文件里的合约条目优先级高于 contracts.yaml
        merge_contracts(&mut merged.contracts, fc.contracts, true);
        // 阶段 2：目录级共享字段，首个非空者胜（与 strategy/cash 同规则）
        for (k, v) in fc.exchanges {
            merged.exchanges.entry(k).or_insert(v);
        }
        push_instruments(&mut merged.instruments, fc.instruments);
        identities.push((path.clone(), entry.symbol.clone()));
        merged.symbols.push(entry);
    }

    // 共享基线补缺（per-symbol 文件已给出的字段不覆盖）
    if let Some(g) = global_cfg {
        merged.strategy = merged.strategy.or(g.strategy);
        merged.timeframe = merged.timeframe.or(g.timeframe);
        merged.cash = merged.cash.or(g.cash);
        merged.initial_cash = merged.initial_cash.or(g.initial_cash);
        merged.leverage = merged.leverage.or(g.leverage);
        merged.mode = merged.mode.or(g.mode);
        merged.risk = merged.risk.or(g.risk);
        merged.exchange = merged.exchange.or(g.exchange);
        merged.account = merged.account.or(g.account);
        merged.services = merged.services.or(g.services);
        merge_contracts(&mut merged.contracts, g.contracts, false);
        // 阶段 2：共享基线补缺（per-symbol 已声明的实例名/标的不覆盖）
        for (k, v) in g.exchanges {
            merged.exchanges.entry(k).or_insert(v);
        }
        push_instruments(&mut merged.instruments, g.instruments);
    }
    // 子目录基线的合约表/实例清单按 key 补缺并入；`services`/`risk` 这类单槽
    // 目录级字段保持"一次运行只有一份"的现状，不并入子目录基线。
    for (_, g) in &scoped_globals {
        merge_contracts(&mut merged.contracts, g.contracts.clone(), false);
        for (k, v) in g.exchanges.clone() {
            merged.exchanges.entry(k).or_insert(v);
        }
        push_instruments(&mut merged.instruments, g.instruments.clone());
    }
    // 合约参数文件：只补缺（不覆盖配置树中已声明的同名合约）
    merge_contracts(&mut merged.contracts, global_contracts, false);
    // 目录模式的合约文件可能位于父目录（如 --config config/<strategy>）
    apply_contracts_fallback(&mut merged, Some(dir))?;

    // 目录模式来源标记（见 FileConfig::from_dir）：身份校验只在目录模式生效
    merged.from_dir = true;
    // 重名标的**告警**（不硬拒）：两个文件声明同一标的时，`run` 会起同名实例、
    // state/signals 按实例名归并 ⇒ 互相覆盖，必须先让运维看见是哪两个文件。
    // 不 bail 的理由：仓库顶层 `config/` 递归收集各策略子目录的同名样例，
    // 硬拒等于把"目录递归"与"每策略一个目录"两件事绑死，那是另一个设计决策。
    {
        let mut seen: std::collections::BTreeMap<String, Vec<std::path::PathBuf>> =
            Default::default();
        for (p, sym) in identities {
            seen.entry(sym).or_default().push(p);
        }
        for (sym, files) in seen.iter().filter(|(_, v)| v.len() > 1) {
            tracing::warn!(
                symbol = %sym,
                files = ?files.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
                "目录模式：多个文件声明同一标的，run 会起同名实例并互相覆盖 state/signal 目录"
            );
        }
    }

    apply_env_overrides(&mut merged);
    Ok(merged)
}

/// 就近查找 symbol 文件所属目录作用域基线：从文件所在目录向上走到根目录，
/// 命中子目录 `_global.yaml` 即停止（不回落到根基线）。
fn nearest_scoped_global<'a>(
    file: &std::path::Path,
    root: &std::path::Path,
    scoped: &'a [(std::path::PathBuf, FileConfig)],
) -> Option<&'a FileConfig> {
    let mut cur = file.parent();
    while let Some(d) = cur {
        if let Some((_, g)) = scoped.iter().find(|(gd, _)| gd == d) {
            return Some(g);
        }
        if d == root {
            break;
        }
        cur = d.parent();
    }
    None
}

/// 子目录基线补进单标的条目：只补缺，调用前该文件自身声明已写入 entry。
fn fill_entry_from_global(entry: &mut SymbolEntry, g: &FileConfig) {
    entry.strategy = entry.strategy.take().or_else(|| g.strategy.clone());
    entry.timeframe = entry.timeframe.take().or_else(|| g.timeframe.clone());
    entry.bar_merge = entry.bar_merge.take().or_else(|| g.bar_merge.clone());
    entry.cash = entry.cash.or(g.cash);
    entry.initial_cash = entry.initial_cash.or(g.initial_cash);
    entry.params = entry.params.take().or_else(|| g.params.clone());
    entry.warmup_days = entry.warmup_days.or(g.warmup_days);
    entry.exchange = entry.exchange.take().or_else(|| g.exchange.clone());
    entry.account = entry.account.take().or_else(|| g.account.clone());
    entry.leverage = entry.leverage.or(g.leverage);
    entry.mode = entry.mode.take().or_else(|| g.mode.clone());
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

/// **名实相符校验**：**目录模式**下配置一旦声明了 per-symbol 条目，请求的标的
/// 就必须是其中之一。
///
/// 为什么必须报错而不是静默回落：请求一个目录里没有的标的时，`entry` 为 `None`，
/// 于是所有身份字段（exchange/account/strategy/cash…）都去够目录级默认值——
/// 那些值过去来自"字母序第一个文件"，BTCUSDT 会拿到兄弟文件的 `exchange: SHFE`，
/// 行键错位后表现为"区间无 1m 数据"，用户完全看不出是配置解析的错。
///
/// 为什么**只管目录模式**（`file.from_dir`）：单文件配置没有兄弟文件，它的顶层字段
/// 就是它自己的全局，`--symbol` 给出未声明的标的属合法模板复用（原
/// `run_cli_symbols_override_config_symbols` 钉的正是这条），不该误伤。
fn ensure_declared(file: &FileConfig, symbol: &str) -> anyhow::Result<()> {
    if !file.from_dir || file.symbols.is_empty() {
        return Ok(());
    }
    if file
        .symbols
        .iter()
        .any(|e| e.symbol.eq_ignore_ascii_case(symbol))
    {
        return Ok(());
    }
    let available: Vec<&str> = file.symbols.iter().map(|e| e.symbol.as_str()).collect();
    anyhow::bail!(
        "标的 `{symbol}` 在本配置中没有对应的 per-symbol 条目（已声明：{}）。目录模式下 \
         每个 --symbol 必须在目录里找到自己的文件（`{{strategy}}/{{SYMBOL}}.yaml`），\
         或在该 flat 文件顶层写 `symbol: {symbol}`；静默套用兄弟文件的交易所/账户会让\
         行键错位（如 SHFE/crypto_perpetual/BTCUSDT）。详见 docs/RUNBOOK.md §7.1。",
        available.join(", ")
    )
}

/// 解析 backtest 的单 symbol 规格。
/// CLI 显式值 > per-symbol 条目 > `_global.yaml`/文件顶层 > 默认值；
/// symbol 取 CLI，缺省回落配置首个 symbol，再回落本文件的 `symbol:`。
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
            .or_else(|| file.symbol.clone())
            .ok_or_else(|| anyhow::anyhow!("缺少 symbol：CLI --symbol 或配置文件 symbols[0]"))?,
    };
    let symbol = symbol.to_uppercase();
    ensure_declared(file, &symbol)?;
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
    let initial_cash = entry.and_then(|e| e.initial_cash).or(file.initial_cash);
    // 聚合模式：历史字段，仅告警不消费（见 warn_bar_merge_deprecated）
    warn_bar_merge_deprecated(
        entry
            .and_then(|e| e.bar_merge.as_deref())
            .or(file.bar_merge.as_deref()),
    );
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
    let contract = contract_for(&file.contracts, &symbol).cloned();
    if let Some(c) = &contract {
        c.validate()
            .map_err(|e| anyhow::anyhow!("合约参数 {symbol}: {e}"))?;
    }
    // registry 路由（§13-5c run 合并步）：声明了 `exchanges:` 时，生效 exchange
    // 必须能在注册表解析（实例名/别名三级，打错 fail-fast），且命名空间键取
    // **注册 id**（配置 key `ctp` → `SHFE`，与 engine 链 `namespaced_key()` 同值，
    // §6.1 双链单源）；未声明（旧单交易所配置）→ 零回归、原样派生。
    let registry = registry_for(file)?;
    let venue_exchange = match &registry {
        Some(reg) => reg
            .get_by_name(&exchange)
            .map_err(|e| anyhow::anyhow!("backtest exchange `{exchange}` 解析失败: {e}"))?
            .id()
            .as_str()
            .to_string(),
        None => exchange.clone(),
    };
    let venue_key = crate::spi_backtest::venue_key_for(&venue_exchange, contract.as_ref(), &symbol);
    let (venue_qty_step, venue_min_qty, venue_qty_precision) =
        venue_qty_spec_for(file, &exchange, &venue_exchange, &symbol);
    Ok(SymbolSpec {
        strategy,
        symbol,
        timeframe,
        cash,
        initial_cash,
        params,
        warmup_days: entry
            .and_then(|e| e.warmup_days)
            .or(file.warmup_days)
            .unwrap_or(30),
        exchange,
        venue_key,
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
        contract,
        venue_qty_step,
        venue_min_qty,
        venue_qty_precision,
        risk: file.risk.clone().unwrap_or_default(),
    })
}

/// 候选条目：symbol + 可选 per-symbol 覆盖
/// （strategy/timeframe/cash/params/warmup_days/exchange/account/leverage/mode）
struct Entry {
    symbol: String,
    strategy: Option<String>,
    timeframe: Option<String>,
    bar_merge: Option<String>,
    cash: Option<f64>,
    initial_cash: Option<f64>,
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
        // 名实相符：请求的标的必须在已声明条目内（同 backtest 口径，见 ensure_declared）
        for s in cli_symbols {
            ensure_declared(file, s)?;
        }
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
                    bar_merge: e.and_then(|e| e.bar_merge.clone()),
                    cash: e.and_then(|e| e.cash),
                    initial_cash: e.and_then(|e| e.initial_cash),
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
                bar_merge: e.bar_merge.clone(),
                cash: e.cash,
                initial_cash: e.initial_cash,
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
    // registry 路由（§13-5c run 合并步）：声明 `exchanges:` → 装配一次注册表，
    // 逐 spec 校验生效 exchange 可解析；旧单交易所配置（未声明）零回归。
    let registry = registry_for(file)?;

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
        warn_bar_merge_deprecated(entry.bar_merge.as_deref().or(file.bar_merge.as_deref()));
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
        let contract = contract_for(&file.contracts, &symbol).cloned();
        if let Some(c) = &contract {
            c.validate()
                .map_err(|e| anyhow::anyhow!("合约参数 {symbol}: {e}"))?;
        }
        let venue_exchange = match &registry {
            Some(reg) => reg
                .get_by_name(&exchange)
                .map_err(|e| {
                    anyhow::anyhow!("symbol {symbol}: exchange `{exchange}` 解析失败: {e}")
                })?
                .id()
                .as_str()
                .to_string(),
            None => exchange.clone(),
        };
        let venue_key =
            crate::spi_backtest::venue_key_for(&venue_exchange, contract.as_ref(), &symbol);
        let (venue_qty_step, venue_min_qty, venue_qty_precision) =
            venue_qty_spec_for(file, &exchange, &venue_exchange, &symbol);
        specs.push(SymbolSpec {
            strategy,
            symbol: symbol.to_uppercase(),
            timeframe,
            cash,
            initial_cash: entry.initial_cash.or(file.initial_cash),
            params,
            warmup_days: entry.warmup_days.or(file.warmup_days).unwrap_or(30),
            exchange,
            venue_key,
            account,
            leverage,
            mode,
            contract,
            venue_qty_step,
            venue_min_qty,
            venue_qty_precision,
            risk: file.risk.clone().unwrap_or_default(),
        });
    }
    Ok(specs)
}

#[cfg(test)]
mod trading_channel_guard_tests {
    //! 「未配置交易通道」判据的钉死案（retirement §7 风险 8 的处置 1+2）。
    //! 这里守的是三条容易各自跑偏的线：占位值与 Default 必须同源、占位 ⇒ 视为未配置、
    //! 而紧急熔断（真实端点 + submit_enabled=false）**不得**被判成未配置。
    use super::*;

    #[test]
    fn placeholder_constant_and_default_stay_in_lockstep() {
        // 若有人改了 Default 里的 ws_url 而没改常量（或反之），这条立刻红——
        // 否则 is_unconfigured 会静默失效，风险 8 原样复发。
        assert_eq!(
            TradingServiceCfg::default().ws_url,
            PLACEHOLDER_TRADING_WS_URL,
            "占位常量与 Default 必须同源"
        );
        assert!(
            TradingServiceCfg::default().is_unconfigured(),
            "Default（未配置）必须被识别为未配置"
        );
    }

    #[test]
    fn real_endpoint_is_not_unconfigured() {
        let cfg = TradingServiceCfg {
            ws_url: "ws://10.0.0.7:9100/ws".into(),
            ..TradingServiceCfg::default()
        };
        assert!(!cfg.is_unconfigured(), "配了真实端点即视为已配置");
    }

    #[test]
    fn emergency_fuse_with_real_endpoint_is_not_unconfigured() {
        // RUNBOOK §2.1 的紧急熔断 = 真实端点 + submit_enabled: false。
        // 判据只看端点，故熔断不会被启动期校验误杀（否则运维没法"连着但不发单"）。
        let fuse = TradingServiceCfg {
            ws_url: "ws://10.0.0.7:9100/ws".into(),
            submit_enabled: false,
            ..TradingServiceCfg::default()
        };
        assert!(
            !fuse.is_unconfigured(),
            "熔断（真实端点 + submit_enabled=false）必须仍可启动"
        );
        assert!(!fuse.submit_enabled, "熔断语义本身不变");
    }

    #[test]
    fn placeholder_with_submit_enabled_false_still_unconfigured() {
        // 反例：运维写了 submit_enabled: false 但端点仍是占位 ⇒ 依旧是"连的是空气"，
        // 必须判为未配置（live 会被拒绝启动，paper 会出声）。
        let bad = TradingServiceCfg {
            submit_enabled: false,
            ..TradingServiceCfg::default()
        };
        assert!(bad.is_unconfigured(), "占位端点 + false 不等于熔断");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `set_current_dir` 是进程全局态：默认路径测试必须串行，否则互相踩踏；
    /// RAII 在 panic 时也恢复原工作目录。
    pub(crate) static CWD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
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
            "strategy: g\ncash: 999.0\nservices:\n  trading:\n    ws_url: wss://t.example/ws\n",
        )
        .unwrap();
        let cfg = load(Some(&dir.path().join("strategy.config.yaml"))).unwrap();
        assert_eq!(cfg.strategy.as_deref(), Some("x"), "单文件显式字段优先");
        assert_eq!(cfg.cash, Some(50.0));
        let services = cfg.services.expect("services 应从 _global.yaml 补挂");
        assert_eq!(services.trading.ws_url, "wss://t.example/ws");
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

    // ===== 国内期货合约参数 =====

    #[test]
    fn repo_contracts_file_parses_and_validates() {
        let text = include_str!("../../../config/contracts.yaml");
        let fc: FileConfig = serde_yaml::from_str(text).unwrap();
        for sym in ["CU", "AU", "AG", "RB", "RU", "I"] {
            let c = fc
                .contracts
                .get(sym)
                .unwrap_or_else(|| panic!("contracts.yaml 缺少 {sym}"));
            c.validate()
                .unwrap_or_else(|e| panic!("{sym} 参数非法: {e}"));
        }
        // 上期所铜：开/平 0.5‱，平今 1‱
        let cu = &fc.contracts["CU"];
        assert!((cu.commission.open - 0.00005).abs() < 1e-12);
        assert!((cu.commission.close_today.unwrap() - 0.0001).abs() < 1e-12);
        // 收盘前禁交易窗口：CU 日盘 15:00 / 夜盘 01:00
        assert_eq!(cu.session_closes, vec!["15:00", "01:00"]);
        assert_eq!(cu.no_trade_before_close_minutes, 5);
        // 开盘后禁交易窗口：CU 日盘 09:00 / 夜盘 21:00
        assert_eq!(cu.session_opens, vec!["09:00", "21:00"]);
        assert_eq!(cu.no_trade_after_open_minutes, 5);
    }

    #[test]
    fn contracts_parse_and_resolve_to_symbol_spec() {
        let f = yaml(
            r#"
strategy: s
cash: 200.0
initial_cash: 1000000
contracts:
  CU:
    multiplier: 5
    tick_size: 10
    margin_rate: 0.11
    commission:
      mode: rate
      open: 0.00005
      close: 0.00005
      close_today: 0.000025
symbols:
  - symbol: CU
"#,
        );
        let spec = resolve_backtest(&f, Some("cu"), None, None, None).unwrap();
        let c = spec.contract.expect("应解析出 CU 合约");
        assert_eq!(c.multiplier, 5.0);
        assert_eq!(c.margin_rate, 0.11);
        assert_eq!(spec.initial_cash, Some(1_000_000.0));
    }

    #[test]
    fn contracts_file_in_dir_is_not_a_symbol_and_is_merged() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "futures",
            &[
                ("CU.yaml", "strategy: s\ncash: 200.0\n".into()),
                (
                    "contracts.yaml",
                    r#"contracts:
  CU:
    multiplier: 5
    tick_size: 10
    margin_rate: 0.11
    commission:
      mode: per_lot
      open: 20
      close: 20
      close_today: 10
"#
                    .into(),
                ),
            ],
        );
        let cfg = load(Some(&dir)).unwrap();
        assert_eq!(cfg.symbols.len(), 1, "contracts.yaml 不产生 symbol");
        assert_eq!(cfg.symbols[0].symbol, "CU");
        let spec = resolve_backtest(&cfg, Some("CU"), None, None, None).unwrap();
        assert!(spec.contract.is_some());
    }

    #[test]
    fn contracts_file_in_parent_dir_is_discovered() {
        // --config config/<strategy>：合约文件在父目录 config/contracts.yaml
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "futures",
            &[("CU.yaml", "strategy: s\n".into())],
        );
        std::fs::write(
            tmp.path().join("config/contracts.yaml"),
            r#"contracts:
  CU:
    multiplier: 5
    tick_size: 10
    margin_rate: 0.11
    commission:
      mode: rate
      open: 0.00005
      close: 0.00005
"#,
        )
        .unwrap();
        let cfg = load(Some(&dir)).unwrap();
        let spec = resolve_backtest(&cfg, Some("CU"), None, None, None).unwrap();
        assert!(spec.contract.is_some(), "应向上发现父目录 contracts.yaml");
    }

    #[test]
    fn invalid_contract_is_rejected() {
        let f = yaml(
            r#"
strategy: s
contracts:
  CU:
    multiplier: 0
    tick_size: 10
    margin_rate: 0.11
    commission:
      mode: rate
      open: 0.00005
      close: 0.00005
symbols:
  - symbol: CU
"#,
        );
        let err = resolve_backtest(&f, Some("CU"), None, None, None).unwrap_err();
        assert!(err.to_string().contains("multiplier"), "{err}");
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
        assert!(
            cfg.from_dir,
            "默认路径命中 ./config ⇒ 必须带目录模式来源标记"
        );
        assert_eq!(cfg.symbols[0].symbol, "BTCUSDT");
        // 该文件的 cash 留在它自己的 entry 上，不再冒充目录级默认值（根治跨文件污染）
        assert_eq!(cfg.symbols[0].cash, Some(42.0));
        assert_eq!(
            cfg.cash, None,
            "目录级 cash 只能来自 _global.yaml；此处没有全局文件 ⇒ 保持缺省"
        );
        assert_eq!(
            resolve_backtest(&cfg, Some("BTCUSDT"), None, None, None)
                .unwrap()
                .cash,
            42.0,
            "解析层仍须取到本文件的 cash"
        );
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
    fn bar_merge_field_is_accepted_but_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let base = [
            "strategy: example_strategy",
            "timeframe: 4h",
            "cash: 200.0",
            "leverage: 1",
            "",
        ]
        .join("\n");
        // BTC：显式 ctp；ETH/SOL：不写（回落顶层）
        let btc = format!("{base}bar_merge: ctp\n");
        let dir = make_dir_cfg(
            tmp.path(),
            "example_strategy",
            &[
                ("BTCUSDT.yaml", btc),
                ("ETHUSDT.yaml", base.clone()),
                ("SOLUSDT.yaml", base.clone()),
            ],
        );
        // 顶层 bar_merge（作用于未显式声明的 symbol）
        let top =
            "bar_merge: ctp\nstrategy: example_strategy\ntimeframe: 4h\ncash: 200.0\nleverage: 1\n";
        std::fs::write(dir.join("_global.yaml"), top).unwrap();

        let cfg = load(Some(&dir)).unwrap();
        // D1 清退后的命题：字段**仍被解析**（两处 struct 都带 `deny_unknown_fields`，
        // 删字段会让存量 yaml 硬错，故保留 `Option<String>` 兼容位），但不再进入
        // SymbolSpec、不影响聚合。原「值生效」三断言转为「存量兼容 + 装配成功」。
        assert_eq!(
            cfg.bar_merge.as_deref(),
            Some("ctp"),
            "顶层 bar_merge 仍被解析（兼容存量 yaml）"
        );
        let btc_spec = resolve_backtest(&cfg, Some("BTCUSDT"), None, None, None).unwrap();
        assert_eq!(btc_spec.symbol, "BTCUSDT", "写 bar_merge 的回测装配仍成功");
        let eth_spec = resolve_backtest(&cfg, Some("ETHUSDT"), None, None, None).unwrap();
        assert_eq!(eth_spec.symbol, "ETHUSDT", "未写 bar_merge 同样成功");

        // 任意值也不再 fail-fast（字段无语义，报错无意义）
        let tmp2 = tempfile::tempdir().unwrap();
        let weird = format!("{base}bar_merge: nonsense\n");
        let dir2 = make_dir_cfg(tmp2.path(), "example_strategy", &[("X.yaml", weird)]);
        let cfg2 = load(Some(&dir2)).unwrap();
        let spec2 = resolve_backtest(&cfg2, Some("X"), None, None, None).unwrap();
        assert_eq!(spec2.symbol, "X", "历史字段任意值均不影响装配");
    }

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
        assert_eq!(s.kline, None, "退役后 _global 基线不再携带 services.kline");
        assert_eq!(s.trading.ws_url, "ws://global.example:9100/ws");
        assert_eq!(s.trading.strategy_secret, "global-placeholder");
    }

    /// 目录模式的字段归属：标的身份**只认声明它的那个文件**，目录级默认值只认
    /// `_global.yaml`。
    ///
    /// 原名 `per_symbol_global_fields_win_over_global_file` 钉的正是"per-symbol 文件的
    /// 顶层字段被提升为目录级默认值"——那机制本身就是要根治的跨文件污染
    /// （BTCUSDT 会继承字母序在前那份 `CU.yaml` 的 `exchange: SHFE`）。
    /// 2026-11-10 根治后改为在**正确的层**断言同一条优先级，并补上"不外溢"这一半：
    /// ① per-symbol 值仍压过 `_global.yaml`（经 entry 生效）；② 它不再出现在 cfg.*。
    /// 覆盖只增不减，不是把断言改松。
    #[test]
    fn per_symbol_identity_beats_global_but_never_leaks_to_dir_level() {
        let tmp = tempfile::tempdir().unwrap();
        // _global.yaml 给三个目录级基线值，与 per-symbol 文件刻意不同
        let global =
            format!("strategy: global_strategy\ntimeframe: 1d\ncash: 999.0\n{GLOBAL_SERVICES}");
        let dir = make_dir_with_global(
            tmp.path(),
            &global,
            &[(
                "example_strategy/BTCUSDT.yaml",
                "strategy: local_strategy\ntimeframe: 4h\ncash: 50.0\n".into(),
            )],
        );
        let cfg = load(Some(&dir)).unwrap();

        // ① 目录级只剩 _global.yaml 的值
        assert_eq!(
            cfg.strategy.as_deref(),
            Some("global_strategy"),
            "目录级 strategy 只能来自 _global.yaml，不得来自兄弟 symbol 文件"
        );
        assert_eq!(cfg.timeframe.as_deref(), Some("1d"));
        assert_eq!(cfg.cash, Some(999.0));
        // 未声明的字段仍回落 _global（services 属共享基础设施，跨文件可用是本意）
        assert_eq!(cfg.services.as_ref().unwrap().trading.exchange, "global_ex");

        // ② 本文件的顶层字段就是它的身份，留在 entry 上
        let e = &cfg.symbols[0];
        assert_eq!(e.symbol, "BTCUSDT", "目录模式身份取文件名 stem");
        assert_eq!(e.strategy.as_deref(), Some("local_strategy"));
        assert_eq!(e.timeframe.as_deref(), Some("4h"));
        assert_eq!(e.cash, Some(50.0));

        // ③ 解析层优先级不变：CLI > per-symbol > _global > 默认
        let spec = resolve_backtest(&cfg, Some("BTCUSDT"), None, None, None).unwrap();
        assert_eq!(
            spec.strategy, "local_strategy",
            "per-symbol 仍须压过 _global.yaml"
        );
        assert_eq!(spec.timeframe, "4h");
        assert_eq!(spec.cash, 50.0);
        // 该文件没写 exchange ⇒ 走合法回落（services.trading），不是兄弟文件的值
        assert_eq!(spec.exchange, "global_ex");
    }

    /// 目录作用域基线：子目录 `_global.yaml` 只作用于本子树的 symbol 文件，
    /// 根目录基线仍是整棵树共享基线；两处共存不再被判为「多个共享全局文件」。
    #[test]
    fn nested_global_is_scoped_to_subtree() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_with_global(
            tmp.path(),
            &format!("cash: 999.0\n{GLOBAL_SERVICES}"),
            &[
                (
                    "group_a/BTCUSDT.yaml",
                    "strategy: a\ntimeframe: 4h\n".into(),
                ),
                (
                    "group_b/ETHUSDT.yaml",
                    "strategy: b\ntimeframe: 4h\n".into(),
                ),
            ],
        );
        std::fs::write(
            dir.join("group_a").join("_global.yaml"),
            "cash: 111.0\nmode: paper_trading\n",
        )
        .unwrap();
        let cfg = load(Some(&dir)).expect("子目录 _global.yaml 与根目录基线可以共存");
        let btc = cfg.symbols.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.cash, Some(111.0), "本子树基线优先于根目录基线");
        assert_eq!(btc.mode.as_deref(), Some("paper_trading"));
        let eth_spec = resolve_backtest(&cfg, Some("ETHUSDT"), None, None, None).unwrap();
        assert_eq!(eth_spec.cash, 999.0, "无子目录基线 → 解析层回落根目录基线");
        assert_eq!(cfg.cash, Some(999.0), "目录级基线不被子树基线改写");
        assert_eq!(cfg.services.as_ref().unwrap().trading.exchange, "global_ex");
    }

    /// 兄弟文件的身份字段不得溢出（原缺陷的最小复现）：期货文件在前、现货文件在后。
    #[test]
    fn sibling_futures_file_cannot_stamp_crypto_symbol() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_dual_ma",
            &[
                // 字母序在前：期货（SHFE）
                (
                    "CU.yaml",
                    "strategy: example_dual_ma\ntimeframe: 1h\nexchange: SHFE\naccount: ctp_acct\n"
                        .into(),
                ),
                // 字母序在后：现货（binance），即被污染的那一份
                (
                    "BTCUSDT.yaml",
                    "strategy: example_dual_ma\ntimeframe: 4h\nexchange: binance\naccount: spot_acct\n"
                        .into(),
                ),
            ],
        );
        let cfg = load(Some(&dir)).unwrap();
        assert_eq!(cfg.symbols.len(), 2);

        let btc = resolve_backtest(&cfg, Some("BTCUSDT"), None, None, None).unwrap();
        assert_eq!(btc.exchange, "binance", "BTCUSDT 必须用自己的交易所");
        assert_eq!(btc.account, "spot_acct");
        assert_eq!(btc.timeframe, "4h", "不得继承 CU.yaml 的主周期");
        assert_eq!(btc.venue_key, "binance/crypto_perpetual/BTCUSDT");

        let cu = resolve_backtest(&cfg, Some("CU"), None, None, None).unwrap();
        assert_eq!(cu.exchange, "SHFE", "CU 同样只认自己那份");
        assert_eq!(cu.account, "ctp_acct");
        assert_eq!(cu.timeframe, "1h");
        // CU 在 contracts.yaml 未声明 ⇒ 按 crypto 兼容口径归入 cn_futures 之外，
        // 这里只断言交易所身份（行键随合约有无而变，属 §6.1 另一条口径）
        assert!(cu.venue_key.starts_with("SHFE/"), "{}", cu.venue_key);
    }

    /// 目录模式要求名实相符：请求没有文件的标的 ⇒ 出声报错，而非静默错位。
    #[test]
    fn dir_mode_undeclared_symbol_fails_fast_listing_available() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_dual_ma",
            &[
                ("CU.yaml", "strategy: s\nexchange: SHFE\n".into()),
                ("BTCUSDT.yaml", "strategy: s\nexchange: binance\n".into()),
            ],
        );
        let cfg = load(Some(&dir)).unwrap();
        assert!(cfg.from_dir, "目录模式须带来源标记");

        let err = resolve_backtest(&cfg, Some("SOLUSDT"), None, None, None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("SOLUSDT"), "{err}");
        assert!(
            err.contains("BTCUSDT") && err.contains("CU"),
            "须列出可用标的：{err}"
        );
        assert!(err.contains("RUNBOOK"), "须指路文档：{err}");

        // run 侧同口径
        let err = resolve_run(&cfg, &["SOLUSDT".into()], None, None, None, None).unwrap_err();
        assert!(err.to_string().contains("SOLUSDT"), "{err}");
        // 已声明的标的不受影响
        assert!(resolve_run(&cfg, &["btcusdt".into()], None, None, None, None).is_ok());
    }

    /// flat 文件的显式身份：`symbol:` 决定条目名（文件名 stem 只是隐式默认）。
    #[test]
    fn declared_symbol_overrides_filename_stem() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "example_dual_ma",
            &[(
                "template.yaml",
                "symbol: BTCUSDT\nstrategy: s\nexchange: binance\n".into(),
            )],
        );
        let cfg = load(Some(&dir)).unwrap();
        assert_eq!(cfg.symbols[0].symbol, "BTCUSDT", "显式身份优先于文件名");
        let spec = resolve_backtest(&cfg, Some("BTCUSDT"), None, None, None).unwrap();
        assert_eq!(spec.venue_key, "binance/crypto_perpetual/BTCUSDT");
        // 请求别的标的仍然报错（条目已声明，名实必须相符）
        assert!(resolve_backtest(&cfg, Some("ETHUSDT"), None, None, None).is_err());
    }

    /// 同一文件既写 `symbol:` 又写 `symbols:` 且不一致 ⇒ 身份含糊，拒收。
    #[test]
    fn symbol_and_symbols_conflict_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "s",
            &[(
                "BTCUSDT.yaml",
                "symbol: ETHUSDT\nsymbols:\n  - symbol: BTCUSDT\n".into(),
            )],
        );
        let err = load(Some(&dir)).unwrap_err().to_string();
        assert!(err.contains("冲突"), "{err}");
    }

    /// 共享文件不属于任何单个标的：`_global.yaml` 写 `symbol:` 必须硬错而非静默忽略。
    #[test]
    fn global_file_with_symbol_field_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_with_global(
            tmp.path(),
            "symbol: BTCUSDT\n",
            &[("s/BTCUSDT.yaml", "strategy: s\n".into())],
        );
        let err = load(Some(&dir)).unwrap_err().to_string();
        assert!(err.contains("不允许声明顶层 symbol"), "{err}");
    }

    /// 目录内两个文件声明同一标的 ⇒ 告警（run 会起同名实例互相覆盖），不硬拒。
    #[test]
    fn duplicate_symbol_entries_warn_not_reject() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_cfg(
            tmp.path(),
            "a",
            &[("BTCUSDT.yaml", "strategy: s1\n".into())],
        );
        // 第二个策略子目录里同名文件（make_dir_cfg 把文件都放在 config/ 下递归收集）
        std::fs::create_dir_all(dir.join("b")).unwrap();
        std::fs::write(dir.join("b/BTCUSDT.yaml"), "strategy: s2\n").unwrap();
        let cfg = load(Some(&dir)).unwrap();
        assert_eq!(cfg.symbols.len(), 2, "重名标的仍被装配（不 bail）");
    }

    /// 单文件模式不受影响：没有兄弟文件，`--symbol` 复用模板是合法路径
    /// （原 `run_cli_symbols_override_config_symbols` 钉的正是这条，此处只补一句）。
    #[test]
    fn single_file_mode_still_allows_undeclared_symbol() {
        let f = yaml(
            r#"
cash: 111.0
exchange: binance
symbols:
  - symbol: BTCUSDT
"#,
        );
        assert!(!f.from_dir, "单文件解析不得带目录标记");
        let specs = resolve_run(&f, &["ETHUSDT".into()], None, None, None, None).unwrap();
        assert_eq!(specs[0].cash, 111.0);
        assert_eq!(specs[0].exchange, "binance", "回落值全部来自这一个文件自己");
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
        // 目录作用域允许"根基线 + 各子目录基线"共存；同一目录内两份才硬错。
        let tmp = tempfile::tempdir().unwrap();
        let dir = make_dir_with_global(
            tmp.path(),
            GLOBAL_SERVICES,
            &[
                ("a/BTCUSDT.yaml", "strategy: s\n".into()),
                ("a/_global.yaml", GLOBAL_SERVICES.into()),
                ("a/_global.yml", "cash: 111.0\n".into()),
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
        assert_eq!(s.kline, None, "无 kline 节即为 None");
        assert_eq!(s.trading.ws_url, "ws://trading:9100/ws");
        assert_eq!(s.trading.request_timeout_ms, 2000);
        assert!(s.trading.submit_enabled);
        assert_eq!(s.trading.reconnect_backoff_max_secs, 45);
        assert_eq!(s.trading.strategy_secret, "dev-shared-secret");
        assert_eq!(s.trading.exchange, "binance");
        assert_eq!(s.trading.account, "test_binance");
    }

    /// tombstone 语义：存量 `services.kline` 节解析放行（拒启报错由 run 入口
    /// `check_kline_retired` 给出迁移指引，而非 serde unknown-field 裸报错），
    /// 但 run 路径必须失败。
    #[test]
    fn services_kline_tombstone_parses_but_run_rejects() {
        let f: FileConfig = serde_yaml::from_str(
            "strategy: x\nservices:\n  kline:\n    market: futures\n  trading:\n    ws_url: ws://t:9100/ws\nsymbols:\n  - symbol: CU2601\n",
        )
        .expect("存量 services.kline 必须解析放行（tombstone）");
        let s = f.services.expect("services 应解析");
        assert!(s.kline.is_some());
        let err = check_kline_retired(&s).unwrap_err().to_string();
        assert!(err.contains("已退役"), "失败必须指向迁移指引：{err}");

        let clean: FileConfig = serde_yaml::from_str(
            "strategy: x\nservices:\n  trading:\n    ws_url: ws://t:9100/ws\nsymbols:\n  - symbol: BTCUSDT\n",
        )
        .unwrap();
        let s = clean.services.expect("services 应解析");
        assert!(s.kline.is_none());
        check_kline_retired(&s).expect("无 kline 节应放行");
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
        // services 子树内的**真未知端点**仍拒（deny_unknown_fields 原意不变）。
        let text = "
services:
  quote:
    url: ws://x
";
        assert!(serde_yaml::from_str::<FileConfig>(text).is_err());
        // 而 `services.kline` 是 tombstone：任意子键都必须解析放行——
        // 拒启报错要由 check_kline_retired 给迁移指引，而非 serde 裸 unknown-field。
        let tombstone = "
services:
  kline:
    backfill_url: http://x
    bogus: 1
";
        let f: FileConfig =
            serde_yaml::from_str(tombstone).expect("tombstone kline 节任意内容必须放行");
        assert!(f.services.unwrap().kline.is_some());
    }

    /// 期货示例配置：仓库内静态样例必须可被生产加载路径解析，
    /// 且 CU 合约经 contracts.yaml 自动加载（bar_merge 已废弃）；
    /// klines-service 退役后样例不再携带 services.kline 节。
    #[test]
    fn example_cu_config_parses() {
        // 与 env_secret_* 测试共用进程环境变量：并行下 STRATEGY_SECRET 会让
        // load() 注入 services，断言 services.is_none() 被污染。
        let _lock = super::tests::CWD_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::env::remove_var(STRATEGY_SECRET_ENV);
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config/example_dual_ma_cu/strategy.yaml");
        let spec = crate::strategy_config::load_strategy(&path).expect("期货示例配置应可解析");
        assert_eq!(spec.instruments.len(), 1);
        assert_eq!(spec.instruments[0].symbol, "CU");
        assert!(
            spec.instruments[0].contract.is_some(),
            "CU 合约参数应由 config/contracts.yaml 自动加载"
        );
        assert!(
            std::path::Path::new(&path).is_file(),
            "示例配置须随仓库提交"
        );
    }

    /// §6.1/§13-5c：`SymbolSpec.venue_key` 单一派生——未声明 `exchanges:` 的
    /// 旧配置按配置 key 原样命名空间化（零回归）；声明后按**注册 id**
    /// （`ctp` → `SHFE`，与 engine 链 namespaced_key() 同值）。
    #[test]
    fn venue_key_derives_from_exchange_and_contract() {
        fn yaml(y: &str) -> FileConfig {
            serde_yaml::from_str(y).unwrap()
        }
        // 旧单交易所配置（无 exchanges:）：原样派生
        let f = yaml(
            r#"
strategy: s
symbols:
  - { symbol: BTCUSDT }
"#,
        );
        let spec = resolve_backtest(&f, Some("BTCUSDT"), None, None, None).unwrap();
        assert_eq!(spec.venue_key, "binance/crypto_perpetual/BTCUSDT");

        // 声明注册表：ctp 实例 id=SHFE → 命名空间键取注册 id（双链同值面）
        let f2 = yaml(
            r#"
exchange: ctp
contracts:
  CU:
    multiplier: 5
    tick_size: 10
    margin_rate: 0.11
    commission:
      mode: rate
      open: 0.00005
      close: 0.00005
      close_today: 0.000025
symbols:
  - symbol: CU2601
exchanges:
  ctp: { id: SHFE }
"#,
        );
        let specs = resolve_run(&f2, &[], None, None, None, None).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].exchange, "ctp", "wire 帧仍用配置实例名");
        assert_eq!(
            specs[0].venue_key, "SHFE/cn_futures/CU2601",
            "行键取注册 id"
        );
    }

    /// registry 路由 fail-fast：声明了 `exchanges:` 而生效 exchange 解析不了
    /// → 报错点名；未声明 → 旧配置零回归（不装配、不报错）。
    #[test]
    fn registry_routing_fails_fast_on_unknown_exchange() {
        fn yaml(y: &str) -> FileConfig {
            serde_yaml::from_str(y).unwrap()
        }
        let f = yaml(
            r#"
exchange: dce
symbols:
  - { symbol: BTCUSDT }
exchanges:
  binance:
    market: crypto_perpetual
    aggregation: utc_grid
    fee_rate: 0.0004
    symbols: [BTCUSDT]
"#,
        );
        let err = resolve_backtest(&f, Some("BTCUSDT"), None, None, None).unwrap_err();
        assert!(err.to_string().contains("dce"), "实际: {err}");

        // 未声明 exchanges：旧配置原样通过
        let legacy = yaml(
            r#"
exchange: whatever
symbols:
  - { symbol: BTCUSDT }
"#,
        );
        let spec = resolve_backtest(&legacy, Some("BTCUSDT"), None, None, None).unwrap();
        assert_eq!(spec.venue_key, "whatever/crypto_perpetual/BTCUSDT");
    }

    /// run 多 symbol（§13-5c「run 命令多 --symbol 走 registry 路由」）：
    /// 双 symbol 各自命名空间化；同名裸 symbol 挂两个交易所实例时
    /// venue_key 互异（§14-3 路径/实例名面的根）。
    #[test]
    fn run_multi_symbol_routes_per_venue() {
        fn yaml(y: &str) -> FileConfig {
            serde_yaml::from_str(y).unwrap()
        }
        let f = yaml(
            r#"
exchange: binance
symbols:
  - { symbol: BTCUSDT }
  - { symbol: ETHUSDT, exchange: binance }
"#,
        );
        let specs = resolve_run(&f, &[], None, None, None, None).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].venue_key, "binance/crypto_perpetual/BTCUSDT");
        assert_eq!(specs[1].venue_key, "binance/crypto_perpetual/ETHUSDT");
    }
}

#[cfg(test)]
mod venue_qty_spec_tests {
    use super::*;

    #[test]
    fn run_spec_carries_venue_qty_constraints_from_exchanges_tree() {
        // 实盘数量口径必须与场所规格一致：解析 exchanges.specs 的 per-symbol
        // qty 步长，供执行端口 normalize，避免低于步长的差额发单被拒。
        let file = parse_config(
            "exchanges:\n  binance:\n    symbols: [BTCUSDT]\n    specs:\n      BTCUSDT: {qty_step: 0.001, min_qty: 0.001, qty_precision: 3}\nsymbols:\n  - symbol: BTCUSDT\nstrategy: xsec_momentum\ntimeframe: 1m\ncash: 200\nmode: live\n",
        )
        .unwrap();
        let specs = resolve_run(&file, &[], None, None, None, None).unwrap();
        let btc = specs.iter().find(|s| s.symbol == "BTCUSDT").unwrap();
        assert_eq!(btc.venue_qty_step, Some(0.001));
        assert_eq!(btc.venue_min_qty, Some(0.001));
        assert_eq!(btc.venue_qty_precision, Some(3));
    }
}
