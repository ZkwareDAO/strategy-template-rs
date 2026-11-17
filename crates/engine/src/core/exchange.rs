//! 交易所总契约（注册表见 `crate::registry`）。
//!
//! 核心思想：**规则与运行模式正交**。
//! `Exchange` 只描述「这个场所的规则是什么」，不描述「怎么连」；
//! [`Exchange::make_source`] / [`Exchange::make_execution`] 才按 `RunMode`
//! 装配端口。规则对象在 live 与 backtest 中是**同一批 Arc**，
//! 因此新增交易所只需实现一次规则，实盘/回测自动共用。

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::core::aggregation::BarAggregationPolicy;
use crate::core::bar::{Bar, BarEventSource, Money, Timeframe, VecEventSource};
use crate::core::error::{Result, SpiError};
use crate::core::execution::{ExecutionPort, SimulationExecution, SimulationPolicy};
use crate::core::identity::{ExchangeId, InstrumentId, MarketKind, RunMode};
use crate::core::position::PositionRules;
use crate::core::session::SessionRules;
use crate::core::subscription::{ContractSnapshot, Subscription, SubscriptionPlan};
use crate::core::trading::TradingRules;

/// 数据源装配请求。生产实现把 `bars` 换成文件路径 / DB 查询 / WS 订阅参数。
#[derive(Debug, Clone)]
pub struct SourceRequest {
    pub instrument: InstrumentId,
    pub mode: RunMode,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub warmup_bars: usize,
    /// 回测注入的 1m 历史（Demo / 单测用）
    pub bars: Vec<Bar>,
    /// 当日合约快照（期货选主用）
    pub universe: Vec<ContractSnapshot>,
    pub initial_cash: Money,
    /// 阶段 3（v2.6）：装配期「时钟锚点」。订阅计划（主力选主等）在此时刻
    /// 解析合约；回测传历史 `as_of`，None = 墙钟 `Utc::now()`（live 语义）。
    /// 解决阶段 2 登记的时钟口径债（§8 注）：历史回放不再在装配期报
    /// 「无可用合约快照」。
    pub as_of: Option<DateTime<Utc>>,
}

impl SourceRequest {
    pub fn backtest(instrument: InstrumentId, bars: Vec<Bar>, initial_cash: Money) -> Self {
        Self {
            instrument,
            mode: RunMode::Backtest,
            from: None,
            to: None,
            warmup_bars: 0,
            bars,
            universe: Vec::new(),
            initial_cash,
            as_of: None,
        }
    }
}

/// 一次 build 产出的全部运行时部件。引擎不再持有任何具体类型。
pub struct ExchangeParts {
    pub exchange_id: ExchangeId,
    pub instrument: InstrumentId,
    pub mode: RunMode,
    pub session: Arc<dyn SessionRules>,
    pub aggregation: Arc<dyn BarAggregationPolicy>,
    pub subscription: Arc<dyn SubscriptionPlan>,
    pub subscriptions: Vec<Subscription>,
    pub trading: Arc<dyn TradingRules>,
    pub position: Arc<dyn PositionRules>,
    pub simulation: Arc<dyn SimulationPolicy>,
    pub source: Box<dyn BarEventSource>,
    pub execution: Box<dyn ExecutionPort>,
}

/// 交易所 engine 总入口。
pub trait Exchange: Send + Sync {
    fn id(&self) -> &ExchangeId;

    /// 本交易所支持的形态（用于配置校验，防止 crypto 与期货混到同一实例）。
    fn markets(&self) -> Vec<MarketKind>;

    /// `BTCUSDT` / `CU2601` / `CU` → 规范化标的。
    fn parse_instrument(&self, raw: &str) -> Result<InstrumentId>;

    // ── 规则族（实盘 / 回测共用同一批 Arc） ─────────────────────────────
    // v2.1 修订（C2）：session_rules 为 per-instrument——同一交易所内 AU(夜盘
    // 02:30) 与 CU(01:00) 时段不同，全所一张表无法表达（§2.1 痛点）。
    // 原同族的 `decoder` 端口因生产链路零消费，已随 BarDecoder/RawFrame 一并退役。
    fn session_rules(&self, instrument: &InstrumentId) -> Arc<dyn SessionRules>;
    fn aggregation_policy(&self, instrument: &InstrumentId) -> Arc<dyn BarAggregationPolicy>;
    fn subscription_plan(&self) -> Arc<dyn SubscriptionPlan>;
    fn trading_rules(&self) -> Arc<dyn TradingRules>;
    fn position_rules(&self) -> Arc<dyn PositionRules>;
    fn simulation_policy(&self) -> Arc<dyn SimulationPolicy>;

    // ── 端口工厂（唯一允许按 RunMode 分叉的地方） ────────────────────────

    /// 行情源。默认实现让**回测开箱即用**：直接回放 `req.bars`。
    /// 实盘必须覆写为 WS 订阅适配器；生产回测也可覆写为 CSV/DB 游标。
    fn make_source(&self, req: &SourceRequest) -> Result<Box<dyn BarEventSource>> {
        if req.mode == RunMode::Live {
            return Err(SpiError::Config(format!(
                "{} 未实现 live 行情源（make_source）",
                self.id()
            )));
        }
        Ok(Box::new(VecEventSource::from_bars(req.bars.clone())))
    }

    /// 订单出口。默认实现让**回测 / paper 开箱即用**：用注入的规则
    /// 组装 `SimulationExecution`。实盘必须覆写为远程网关适配器。
    fn make_execution(&self, req: &SourceRequest) -> Result<Box<dyn ExecutionPort>> {
        if req.mode == RunMode::Live {
            return Err(SpiError::Config(format!(
                "{} 未实现 live 订单网关（make_execution）",
                self.id()
            )));
        }
        Ok(Box::new(SimulationExecution::new(
            req.instrument.clone(),
            req.initial_cash,
            self.trading_rules(),
            self.position_rules(),
            self.session_rules(&req.instrument),
            self.simulation_policy(),
        )))
    }

    /// 统一装配。绝大多数交易所不需要覆写：这里保证 live 与 backtest
    /// 拿到的是同一套规则对象，且订阅计划、解码器、聚合策略一致。
    fn build(&self, req: &SourceRequest) -> Result<ExchangeParts> {
        let instrument = &req.instrument;
        if !self.markets().contains(&instrument.market) {
            return Err(SpiError::Config(format!(
                "{} 不支持市场形态 {:?}",
                self.id(),
                instrument.market
            )));
        }
        let subscription = self.subscription_plan();
        // 阶段 3（v2.6 时钟债闭环）：as_of 注入历史时刻时，选主按该时点
        // 解析（可回测、可复现）；缺省仍为墙钟（live 语义）。
        let as_of = req.as_of.unwrap_or_else(Utc::now);
        let subscriptions = subscription.resolve(instrument, as_of, &req.universe)?;
        Ok(ExchangeParts {
            exchange_id: self.id().clone(),
            instrument: instrument.clone(),
            mode: req.mode,
            session: self.session_rules(instrument),
            aggregation: self.aggregation_policy(instrument),
            subscription,
            subscriptions,
            trading: self.trading_rules(),
            position: self.position_rules(),
            simulation: self.simulation_policy(),
            source: self.make_source(req)?,
            execution: self.make_execution(req)?,
        })
    }
}

/// 策略侧需要的时间框架由运行时传入；这里给一个便捷默认值。
pub fn default_timeframes() -> Vec<Timeframe> {
    vec![Timeframe::M1, Timeframe::M15, Timeframe::H1, Timeframe::D1]
}

// ─── `services:` 子树（§8；阶段 4 v2.7 债务闭环：接受 → 类型化消费）──────

/// 单个服务端点（trading-service 的 §8 形态；kline-service 已退役归档
/// `docs/archive/kline-service.md`，本类型为宽容配置面，旧 kline 键照收不消费）。
///
/// **宽容 + 不丢键**：已知字段类型化；未知字段进 `extra` 原样保留
/// （阶段 5 网关全接线时按需消费），拼错键不会静默消失也不会
/// 提前锁死协议面。
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct EndpointCfg {
    /// 主连接地址（kline ws_url / trading ws_url 的 §8 统一名）。
    pub url: Option<String>,
    /// 历史回填端点（kline 专用）。
    pub backfill_url: Option<String>,
    /// 市场形态标签（`crypto` / `futures`——数据客户端回填窗口口径）。
    pub market: Option<String>,
    /// 请求超时（毫秒，trading 命令帧）。
    pub request_timeout_ms: Option<u64>,
    /// 重连退避上限（秒）。
    pub reconnect_backoff_max_secs: Option<u64>,
    /// 实盘下单总闸（缺省 false：本地拦截，绝不发真单——与
    /// `TradingConfig::submit_enabled` 同语义的**配置面**表达）。
    pub submit_enabled: Option<bool>,
    /// 其余键原样保留（不丢配置）。
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// `exchanges.<name>.services` 类型化形态（§8）。
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct ServicesCfg {
    pub kline: Option<EndpointCfg>,
    pub trading: Option<EndpointCfg>,
    /// 未知端点名（未来 `quote:` / `position:` 等）原样保留。
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// 阶段 4（v2.7，§13-4）：live 端口工厂。
///
/// [`Exchange::make_source`] / [`Exchange::make_execution`] 的缺省实现
/// 在 Live 模式直接报「未实现」——接实盘时交易所不必在 `mod.rs` 手写
/// 覆写体，注入一个工厂即可（`exchanges/live.rs` 提供桥原语，各所
/// `live_gateway.rs`/`live_source.rs` 薄装配）。
///
/// 工厂**不产规则**：session/trading/position 等 Arc 仍由 `Exchange`
/// 提供，live/backtest 同一批对象（首公理不因实盘装配破防）。
pub trait LivePortFactory: Send + Sync {
    /// 实盘行情源（WS 订阅适配器）。
    fn make_source(&self, req: &SourceRequest) -> Result<Box<dyn BarEventSource>>;
    /// 实盘订单出口（远程网关适配器）。
    fn make_execution(&self, req: &SourceRequest) -> Result<Box<dyn ExecutionPort>>;
}
