//! 交易规则：数量/价格规整、保证金、手续费、盈亏。
//!
//! 现状：这些规则在 `broker/backtest.rs` 里以 `if let Some(contract)` 分成
//! 期货/crypto 两套；`tick_size` 只校验不使用；`stepSize` 只在 mock 里
//! 硬编码 1e-6；盈亏/手续费在 broker、engine、测试里各算一遍；
//! 更严重的是 `contracts/trading-service.md §7` 把整数手、保证金、
//! 平今平昨全部推给**外部服务**，模板实盘链路只发 `order_cash+leverage`，
//! 靠「人工保持同一费率口径」对齐回测——这是回测/实盘不可比的根因。
//!
//! 本模块让规则成为交易所能力，且 `Exchange` 在 live 与 backtest
//! 装配的是**同一个 `Arc<dyn TradingRules>`**，规则只实现一次。

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use strategy_core::signal::Side;

use crate::core::bar::Money;
use crate::core::error::{Result, SpiError};
use crate::core::identity::InstrumentId;

/// 开平标志。CTP 需要区分平今/平昨；crypto 只有 Close。
/// 引擎只发「平 X 量」，由 [`crate::core::position::PositionRules`] 拆腿时赋值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Offset {
    Open,
    Close,
    CloseToday,
    CloseYesterday,
    /// 由交易所/柜台自行选择（crypto 的 reduce_only 等价物）
    Auto,
}

impl Offset {
    /// 阶段 4（v2.7）协议标签：`wire.rs` 出帧 `offset` 字段与
    /// `SignalIntent.offset` 的**唯一字符串口径**（snake_case，与
    /// `FeeMode` serde 形态一致）。实盘帧带平今/平昨即由此产生；
    /// 旧链路不发此字段（None ⇒ 服务端隐式判定，零回归）。
    pub fn label(&self) -> &'static str {
        match self {
            Offset::Open => "open",
            Offset::Close => "close",
            Offset::CloseToday => "close_today",
            Offset::CloseYesterday => "close_yesterday",
            Offset::Auto => "auto",
        }
    }

    /// 标签回解析（对账/重启恢复读 `SignalIntent.offset` 用）；未知串 None。
    pub fn from_label(raw: &str) -> Option<Self> {
        Some(match raw.trim().to_ascii_lowercase().as_str() {
            "open" => Offset::Open,
            "close" => Offset::Close,
            "close_today" => Offset::CloseToday,
            "close_yesterday" => Offset::CloseYesterday,
            "auto" => Offset::Auto,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeMode {
    /// 按成交金额比例
    Rate,
    /// 按手数固定收费
    PerLot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeSchedule {
    pub mode: FeeMode,
    pub open: f64,
    /// 平昨 / 普通平仓费率
    pub close: f64,
    /// 平今；None 表示与 close 相同（crypto / 无平今概念的市场）
    pub close_today: Option<f64>,
    /// 平昨与平今之外的独立档（部分品种平昨免收）
    pub close_yesterday: Option<f64>,
}

impl FeeSchedule {
    pub fn flat_rate(rate: f64) -> Self {
        Self {
            mode: FeeMode::Rate,
            open: rate,
            close: rate,
            close_today: None,
            close_yesterday: None,
        }
    }

    pub fn rate_for(&self, offset: Offset) -> f64 {
        match offset {
            Offset::Open => self.open,
            Offset::CloseToday => self.close_today.unwrap_or(self.close),
            Offset::CloseYesterday => self.close_yesterday.unwrap_or(self.close),
            Offset::Close | Offset::Auto => self.close,
        }
    }
}

/// 开仓数量模型。把 `backtest.rs` 里「整数手 / 小数数量」两条分支
/// 变成规格数据，而不是代码分叉。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SizingMode {
    /// crypto：名义金额 / 价格 → 小数数量
    NotionalFraction,
    /// 期货：受保证金与名义敞口双约束的**整数手**
    MarginLots,
}

/// 合约规格。对应现有 `ContractSpec`，但补齐了缺失的精度字段。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstrumentSpec {
    /// 合约乘数：期货 5 吨/手；crypto 为 1
    pub multiplier: f64,
    /// 最小变动价位（现在真正用于价格规整，而非只做 >0 校验）
    pub tick_size: f64,
    /// 最小数量步进：期货 1.0（手），crypto 0.000001 等
    pub qty_step: f64,
    pub min_qty: f64,
    pub max_qty: Option<f64>,
    pub price_precision: u8,
    pub qty_precision: u8,
    /// 保证金率；crypto 现货/永续可用 1/leverage 表达
    pub margin_rate: f64,
    pub fee: FeeSchedule,
    pub currency: String,
    pub sizing_mode: SizingMode,
}

impl InstrumentSpec {
    /// crypto U 本位永续的常用默认值。
    pub fn crypto_perpetual(fee_rate: f64) -> Self {
        Self {
            multiplier: 1.0,
            tick_size: 0.01,
            qty_step: 0.000001,
            min_qty: 0.000001,
            max_qty: None,
            price_precision: 2,
            qty_precision: 6,
            margin_rate: 1.0,
            fee: FeeSchedule::flat_rate(fee_rate),
            currency: "USDT".into(),
            sizing_mode: SizingMode::NotionalFraction,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rounding {
    Floor,
    Ceil,
    Nearest,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SizingRequest {
    pub instrument: InstrumentId,
    pub price: f64,
    pub equity: Money,
    pub leverage: f64,
    /// 每单名义上限；None = 用满可用权益
    pub notional_per_order: Option<Money>,
}

/// 回测/paper/live 共用的开仓装配参数（v2.2 C6，§9.2）。
///
/// 现状硬编码点的替代通道：`main.rs:347 --leverage`、
/// `main.rs:344 notional_per_order=0`（回测全仓）、`bootstrap.rs:78
/// notional_per_order=spec.cash`（paper 名义）——旧架构里三种口径
/// 各自为政，engine 统一为本结构按 instrument 注入 `ExchangeRunner`。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SizingParams {
    pub leverage: f64,
    /// None = 全仓（用满 equity*leverage）；Some(x) = 每单名义上限 x
    pub notional_per_order: Option<Money>,
}

impl Default for SizingParams {
    fn default() -> Self {
        Self {
            leverage: 1.0,
            notional_per_order: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FeeRequest {
    pub instrument: InstrumentId,
    pub price: f64,
    pub qty: f64,
    pub offset: Offset,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarginRequest {
    pub instrument: InstrumentId,
    pub price: f64,
    pub qty: f64,
}

/// 待校验的下单草稿（规整之后、发出之前）。
#[derive(Debug, Clone, PartialEq)]
pub struct OrderDraft {
    pub instrument: InstrumentId,
    pub side: Side,
    pub offset: Offset,
    pub price: f64,
    pub qty: f64,
    /// 数量原始字面量（v2.2 C7）：成交/账本据此维护 `remaining_raw` 口径，
    /// 避免 f64 往返改变精确数量（对齐 `positions::state::remaining_size_raw`）。
    pub qty_raw: Option<String>,
}

/// 按规格精度把规整后的数量派生成字面量（模拟链路的申报/成交量写法）。
/// live 链路的真源是柜台回报 `filled_quantity` 原样，不经本函数。
pub fn format_qty_raw(qty: f64, precision: u8) -> String {
    format!("{:.*}", precision as usize, qty)
}

/// 交易规则端口。
///
/// 实盘用途：下单前规整数量/价格、校验最小量、生成带 offset 的订单帧；
/// 回测用途：开仓 sizing、逐笔手续费、保证金、盈亏结算。
/// 二者调用**同一实现**，从根本上消除「回测与实盘口径不一致」。
pub trait TradingRules: Send + Sync {
    fn spec(&self, instrument: &InstrumentId) -> Result<Arc<InstrumentSpec>>;

    fn normalize_price(&self, instrument: &InstrumentId, raw: f64) -> Result<f64>;

    fn normalize_qty(&self, instrument: &InstrumentId, raw: f64, rounding: Rounding)
        -> Result<f64>;

    /// 名义金额 = price × multiplier × qty（crypto 的 multiplier 为 1，
    /// 因此不再需要 `pos.entry_price * qty` 这种兼容口径特判）。
    fn notional(&self, instrument: &InstrumentId, price: f64, qty: f64) -> Result<Money>;

    /// 返回 0 表示资金不足，不应下单。
    fn sizing(&self, req: &SizingRequest) -> Result<f64>;

    fn fee(&self, req: &FeeRequest) -> Result<Money>;

    fn margin(&self, req: &MarginRequest) -> Result<Money>;

    fn pnl(
        &self,
        instrument: &InstrumentId,
        side: Side,
        entry: f64,
        exit: f64,
        qty: f64,
    ) -> Result<Money>;

    /// 发单前统一校验；实盘网关与回测撮合都调用。
    fn validate(&self, draft: &OrderDraft) -> Result<()>;
}

/// 规格驱动的通用实现。绝大多数交易所只需提供 `InstrumentSpec` 表，
/// 无需重写计算逻辑；有特殊规则（阶梯保证金、maker/taker）再覆写方法。
#[derive(Clone)]
pub struct SpecDrivenTradingRules {
    specs: BTreeMap<String, InstrumentSpec>,
}

impl SpecDrivenTradingRules {
    pub fn new(entries: impl IntoIterator<Item = (InstrumentId, InstrumentSpec)>) -> Self {
        let specs = entries
            .into_iter()
            .map(|(k, v)| (k.namespaced_key(), v))
            .collect();
        Self { specs }
    }

    pub fn insert(&mut self, instrument: &InstrumentId, spec: InstrumentSpec) {
        self.specs.insert(instrument.namespaced_key(), spec);
    }
}

impl TradingRules for SpecDrivenTradingRules {
    fn spec(&self, instrument: &InstrumentId) -> Result<Arc<InstrumentSpec>> {
        let key = instrument.namespaced_key();
        if let Some(s) = self.specs.get(&key) {
            return Ok(Arc::new(s.clone()));
        }
        // 阶段 2（v2.5 C13）：配置 `contracts:` 按**品种**登记（键 `CU` → 逻辑标的），
        // 具体合约 `CU2601` 精确 miss 后回落品种级条目——对齐现有
        // `config::contract_for` 的前缀归一语义，避免逐月登记规格。
        if instrument.contract_month.is_some() {
            let mut logical = instrument.clone();
            logical.symbol = logical.product.clone();
            logical.contract_month = None;
            if let Some(s) = self.specs.get(&logical.namespaced_key()) {
                return Ok(Arc::new(s.clone()));
            }
        }
        Err(SpiError::MissingSpec(instrument.to_string()))
    }

    fn normalize_price(&self, instrument: &InstrumentId, raw: f64) -> Result<f64> {
        let s = self.spec(instrument)?;
        if s.tick_size <= 0.0 || !raw.is_finite() {
            return Err(SpiError::RuleViolation(format!("非法价格: {raw}")));
        }
        let v = (raw / s.tick_size).round() * s.tick_size;
        let p = 10f64.powi(s.price_precision as i32);
        Ok((v * p).round() / p)
    }

    fn normalize_qty(
        &self,
        instrument: &InstrumentId,
        raw: f64,
        rounding: Rounding,
    ) -> Result<f64> {
        let s = self.spec(instrument)?;
        if s.qty_step <= 0.0 || !raw.is_finite() || raw < 0.0 {
            return Err(SpiError::RuleViolation(format!("非法数量: {raw}")));
        }
        let n = raw / s.qty_step;
        let n = match rounding {
            Rounding::Floor => n.floor(),
            Rounding::Ceil => n.ceil(),
            Rounding::Nearest => n.round(),
        };
        let q = n * s.qty_step;
        let p = 10f64.powi(s.qty_precision as i32);
        Ok((q * p).round() / p)
    }

    fn notional(&self, instrument: &InstrumentId, price: f64, qty: f64) -> Result<Money> {
        let s = self.spec(instrument)?;
        Ok(price * s.multiplier * qty)
    }

    fn sizing(&self, req: &SizingRequest) -> Result<f64> {
        let s = self.spec(&req.instrument)?;
        if !req.price.is_finite() || req.price <= 0.0 || req.equity <= 0.0 {
            return Ok(0.0);
        }
        let max_notional = req.equity * req.leverage.max(0.0);
        let qty = match s.sizing_mode {
            SizingMode::NotionalFraction => {
                let notional = req
                    .notional_per_order
                    .filter(|v| *v > 0.0)
                    .map(|v| v.min(max_notional))
                    .unwrap_or(max_notional);
                notional / (req.price * s.multiplier)
            }
            SizingMode::MarginLots => {
                let notional_per_lot = req.price * s.multiplier;
                let margin_per_lot = notional_per_lot * s.margin_rate;
                let fee_per_lot = self.fee(&FeeRequest {
                    instrument: req.instrument.clone(),
                    price: req.price,
                    qty: 1.0,
                    offset: Offset::Open,
                })?;
                let cost_per_lot = margin_per_lot + fee_per_lot;
                if cost_per_lot <= 0.0 {
                    return Ok(0.0);
                }
                let by_margin = (req.equity / cost_per_lot + 1e-9).floor();
                let by_leverage = (max_notional / notional_per_lot + 1e-9).floor();
                by_margin.min(by_leverage)
            }
        };
        let qty = self.normalize_qty(&req.instrument, qty, Rounding::Floor)?;
        Ok(if qty < s.min_qty { 0.0 } else { qty })
    }

    fn fee(&self, req: &FeeRequest) -> Result<Money> {
        let s = self.spec(&req.instrument)?;
        let notional = self.notional(&req.instrument, req.price, req.qty)?;
        let rate = s.fee.rate_for(req.offset);
        Ok(match s.fee.mode {
            FeeMode::Rate => notional * rate,
            FeeMode::PerLot => req.qty * rate,
        })
    }

    fn margin(&self, req: &MarginRequest) -> Result<Money> {
        let s = self.spec(&req.instrument)?;
        Ok(req.price * s.multiplier * req.qty * s.margin_rate)
    }

    fn pnl(
        &self,
        instrument: &InstrumentId,
        side: Side,
        entry: f64,
        exit: f64,
        qty: f64,
    ) -> Result<Money> {
        let s = self.spec(instrument)?;
        let diff = match side {
            Side::Long => exit - entry,
            Side::Short => entry - exit,
        };
        Ok(diff * qty * s.multiplier)
    }

    fn validate(&self, draft: &OrderDraft) -> Result<()> {
        let s = self.spec(&draft.instrument)?;
        if draft.qty <= 0.0 {
            return Err(SpiError::RuleViolation("数量必须为正".into()));
        }
        if draft.qty < s.min_qty - 1e-9 {
            return Err(SpiError::RuleViolation(format!(
                "数量 {} 小于最小下单量 {}",
                draft.qty, s.min_qty
            )));
        }
        if let Some(max) = s.max_qty {
            if draft.qty > max {
                return Err(SpiError::RuleViolation(format!("数量超过上限 {max}")));
            }
        }
        let steps = draft.qty / s.qty_step;
        if (steps - steps.round()).abs() > 1e-6 {
            return Err(SpiError::RuleViolation(format!(
                "数量 {} 不是步长 {} 的整数倍",
                draft.qty, s.qty_step
            )));
        }
        let ticks = draft.price / s.tick_size;
        if (ticks - ticks.round()).abs() > 1e-6 {
            return Err(SpiError::RuleViolation(format!(
                "价格 {} 不是最小变动价位 {} 的整数倍",
                draft.price, s.tick_size
            )));
        }
        Ok(())
    }
}
