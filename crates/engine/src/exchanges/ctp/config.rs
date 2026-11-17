//! CTP / 国内期货配置驱动装配（阶段 2，§8 `exchanges.ctp:` 子树）。
//!
//! 字段口径与 `config/contracts.yaml` 对齐（multiplier / tick_size /
//! margin_rate / min_lots / commission{mode,open,close,close_today}），
//! 让 contracts.yaml 成为本配置的直接数据源。
//! session per-product 表是 C2 的配置载体；交割月细则仍待 §16 C12 决策
//! （`exclude_delivery_month` 先行透传给 `MainContractPlan`）。

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::core::bar::Timeframe;
use crate::core::error::{Result, SpiError};
use crate::core::identity::{ContractMonth, ExchangeId, InstrumentId, MarketKind};
use crate::core::session::SessionCfg;
use crate::core::subscription::{MainContractPlan, RollPolicy};
use crate::core::trading::{FeeMode, FeeSchedule, InstrumentSpec, SizingMode};
use crate::exchanges::ctp::CtpExchange;

/// 单品种手续费配置（contracts.yaml `commission:` 同构）。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct CtpFeeCfg {
    /// `rate`（成交金额比例）| `per_lot`（元/手）。
    pub mode: Option<String>,
    pub open: f64,
    /// 平昨 / 普通平仓；缺省 = open。
    pub close: Option<f64>,
    /// 平今；缺省 = close。
    pub close_today: Option<f64>,
    /// 平昨独立档（部分品种平昨免收）。
    pub close_yesterday: Option<f64>,
}

impl CtpFeeCfg {
    fn to_schedule(&self) -> Result<FeeSchedule> {
        let mode = match self
            .mode
            .as_deref()
            .unwrap_or("rate")
            .to_ascii_lowercase()
            .as_str()
        {
            "rate" => FeeMode::Rate,
            "per_lot" => FeeMode::PerLot,
            other => return Err(SpiError::Config(format!("非法手续费模式: {other}"))),
        };
        let close = self.close.unwrap_or(self.open);
        Ok(FeeSchedule {
            mode,
            open: self.open,
            close,
            close_today: self.close_today,
            close_yesterday: self.close_yesterday,
        })
    }
}

/// 品种规格配置（contracts.yaml 单条目同构；未声明字段取期货缺省）。
/// 关键参数缺省为 0，由 `to_spec` 在装配期显式拒绝——不静默带病出厂。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct CtpSpecCfg {
    /// 品种中文名，仅注释用途（与 contracts.yaml 保持同构，解析后丢弃）。
    pub name: Option<String>,
    pub multiplier: f64,
    pub tick_size: f64,
    pub margin_rate: f64,
    /// 最小下单手数；缺省 1。
    pub min_lots: Option<u32>,
    /// 数量步进；期货缺省 1.0（整数手）。
    pub qty_step: Option<f64>,
    pub max_lots: Option<f64>,
    /// 价格小数位；缺省 0（tick 规整后再舍入）。
    pub price_precision: Option<u8>,
    /// 数量小数位；缺省 0（整数手）。
    pub qty_precision: Option<u8>,
    pub currency: Option<String>,
    /// `margin_lots`（缺省）| `notional_fraction`。
    pub sizing_mode: Option<String>,
    /// 缺省 = 零费（不伪造费率；生产配置必须显式声明）。
    pub commission: Option<CtpFeeCfg>,
}

impl CtpSpecCfg {
    fn to_spec(&self) -> Result<InstrumentSpec> {
        // 必填三件套：缺省 0 / NaN 都在装配期拒绝（deny 静默零费率/零乘数）。
        if !self.multiplier.is_finite() || self.multiplier <= 0.0 {
            return Err(SpiError::Config("contracts 条目缺少 multiplier".into()));
        }
        if !self.tick_size.is_finite() || self.tick_size <= 0.0 {
            return Err(SpiError::Config("contracts 条目缺少 tick_size".into()));
        }
        if !self.margin_rate.is_finite() || self.margin_rate <= 0.0 || self.margin_rate > 1.0 {
            return Err(SpiError::Config(format!(
                "非法 margin_rate: {}（须在 (0,1]）",
                self.margin_rate
            )));
        }
        let sizing_mode = match self
            .sizing_mode
            .as_deref()
            .unwrap_or("margin_lots")
            .to_ascii_lowercase()
            .as_str()
        {
            "margin_lots" => SizingMode::MarginLots,
            "notional_fraction" => SizingMode::NotionalFraction,
            other => return Err(SpiError::Config(format!("非法开仓数量模型: {other}"))),
        };
        let fee = self
            .commission
            .as_ref()
            .map(CtpFeeCfg::to_schedule)
            .transpose()?
            .unwrap_or(FeeSchedule::flat_rate(0.0));
        Ok(InstrumentSpec {
            multiplier: self.multiplier,
            tick_size: self.tick_size,
            qty_step: self.qty_step.unwrap_or(1.0),
            min_qty: f64::from(self.min_lots.unwrap_or(1)),
            max_qty: self.max_lots,
            price_precision: self.price_precision.unwrap_or(0),
            qty_precision: self.qty_precision.unwrap_or(0),
            margin_rate: self.margin_rate,
            fee,
            currency: self.currency.clone().unwrap_or_else(|| "CNY".into()),
            sizing_mode,
        })
    }
}

/// 订阅 / 选主配置（§8 `subscription:`）。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct CtpSubscriptionCfg {
    /// `open_interest`（缺省）| `volume` | `fixed_date` | `external`。
    pub policy: Option<String>,
    pub include_second_main: Option<bool>,
    /// 进入交割月即剔除；缺省 true（per-product 交割细则待 C12）。
    pub exclude_delivery_month: Option<bool>,
    /// 选主用的行情周期；缺省 `1m`。
    pub timeframe: Option<String>,
}

impl CtpSubscriptionCfg {
    fn to_plan(&self) -> Result<MainContractPlan> {
        let policy = match self
            .policy
            .as_deref()
            .unwrap_or("open_interest")
            .to_ascii_lowercase()
            .as_str()
        {
            "open_interest" => RollPolicy::OpenInterest,
            "volume" => RollPolicy::Volume,
            "fixed_date" => RollPolicy::FixedDate,
            "external" => RollPolicy::External,
            other => return Err(SpiError::Config(format!("非法换月策略: {other}"))),
        };
        let timeframe = self
            .timeframe
            .as_deref()
            .map(Timeframe::parse)
            .transpose()?
            .unwrap_or(Timeframe::M1);
        Ok(MainContractPlan {
            policy,
            include_second_main: self.include_second_main.unwrap_or(false),
            exclude_delivery_month: self.exclude_delivery_month.unwrap_or(true),
            timeframe,
        })
    }
}

/// §8 示例形态（节选）：
/// ```yaml
/// ctp:
///   session: { day_cutover: "20:00", windows: [...] }
///   sessions: { AU: {...}, CU: {...} }
///   contracts: { CU: { multiplier: 5, tick_size: 10, ... } }
///   subscription: { policy: open_interest, include_second_main: true }
/// ```
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct CtpConfig {
    pub kind: Option<String>,
    /// 交易所 id；缺省 `SHFE`（DCE/CFFEX 等多场所时显式声明）。
    pub id: Option<String>,
    /// 市场形态；阶段 2 仅允许 `cn_futures`（与 §8 示例同构，校验不吞键）。
    pub market: Option<String>,
    /// 聚合策略；阶段 2 仅允许 `session_window`。
    pub aggregation: Option<String>,
    /// 全所默认时段表；缺省 = `cn_futures_default()`。
    pub session: Option<SessionCfg>,
    /// per-product 时段表（C2 的配置载体；键 = 品种码，大小写不敏感）。
    #[serde(default)]
    pub sessions: BTreeMap<String, SessionCfg>,
    /// 品种规格表（键 = 品种 `CU` 或具体合约 `CU2601`；后者精确命中，
    /// 前者经 spec() 品种回落（C13）服务所有挂牌月份）。
    #[serde(default)]
    pub contracts: BTreeMap<String, CtpSpecCfg>,
    /// 便捷开关：等价 `subscription: {}`（按持仓量选主）。
    pub main_contract: Option<bool>,
    pub subscription: Option<CtpSubscriptionCfg>,
    /// 连接参数（阶段 4 v2.7 债务闭环：§8 注③「接受不解析」→ 类型化，
    /// live 装配经 `CtpExchange::live_services` 消费；未知端点/键不丢失）。
    pub services: Option<crate::core::exchange::ServicesCfg>,
}

impl CtpConfig {
    pub fn from_value(v: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(v.clone())
            .map_err(|e| SpiError::Config(format!("ctp 配置解析失败: {e}")))
    }

    pub fn build(self) -> Result<CtpExchange> {
        if let Some(m) = &self.market {
            if m != "cn_futures" {
                return Err(SpiError::Config(format!(
                    "ctp 阶段 2 仅支持 market=cn_futures（{m} 未实现）"
                )));
            }
        }
        if let Some(a) = &self.aggregation {
            if a != "session_window" {
                return Err(SpiError::Config(format!(
                    "ctp 聚合策略仅支持 session_window（{a} 未实现）"
                )));
            }
        }
        let id = ExchangeId::new(self.id.unwrap_or_else(|| "SHFE".into()));
        let mut ex = CtpExchange::named(id.clone());
        if let Some(session) = &self.session {
            ex = ex.with_default_session(session.to_windowed()?);
        }
        for (product, session) in &self.sessions {
            ex = ex.with_session(product, session.to_windowed()?);
        }
        for (key, scfg) in &self.contracts {
            let spec = scfg.to_spec()?;
            ex = ex.with_spec(parse_contract_key(&id, key)?, spec);
        }
        match &self.subscription {
            Some(sub) => ex = ex.with_subscription_plan(sub.to_plan()?),
            None => {
                if self.main_contract == Some(true) {
                    ex = ex.with_main_contract();
                }
            }
        }
        // 阶段 4（§8 注③）：`services:` 从「接受不解析」升级为类型化透传
        if let Some(services) = self.services {
            ex = ex.with_services(services);
        }
        Ok(ex)
    }
}

/// `contracts:` 键 → 标的：`CU2601` 解析成具体合约，`CU` 落逻辑标的
/// （品种级条目，经 C13 回落服务全部挂牌月份）。
fn parse_contract_key(id: &ExchangeId, key: &str) -> Result<InstrumentId> {
    let upper = key.to_ascii_uppercase();
    let split = upper
        .find(|c: char| c.is_ascii_digit())
        .unwrap_or(upper.len());
    let (product, digits) = upper.split_at(split);
    if product.is_empty() {
        return Err(SpiError::Config(format!("非法合约键: {key}")));
    }
    match digits.len() {
        0 => Ok(InstrumentId {
            exchange: id.clone(),
            market: MarketKind::CnFutures,
            symbol: product.to_string(),
            product: product.to_string(),
            contract_month: None,
        }),
        4 => {
            let yy: u16 = digits[..2]
                .parse()
                .map_err(|_| SpiError::Config(format!("非法合约键: {key}")))?;
            let mm: u8 = digits[2..]
                .parse()
                .map_err(|_| SpiError::Config(format!("非法合约键: {key}")))?;
            if !(1..=12).contains(&mm) {
                return Err(SpiError::Config(format!("非法合约月份: {key}")));
            }
            Ok(InstrumentId::futures_contract(
                id,
                product,
                ContractMonth {
                    year: 2000 + yy,
                    month: mm,
                },
            ))
        }
        _ => Err(SpiError::Config(format!("非法合约键: {key}"))),
    }
}
