//! 订单出口、回测撮合与统一结算。
//!
//! 现状：`OrderGateway` / `AsyncOrderGateway` 形状够用，但入参
//! `SignalIntent` 不带交易所上下文；`BacktestBroker` 内部同时承担
//! 撮合、sizing、手续费、盈亏、现金记账，且（迁移前）与旧 `Engine::close_pnl`、
//! parity 测试各算一遍；`slippage` 没有配置入口，报表恒 0；
//! 实盘 `FillEvent.fee` 恒 0。
//!
//! 本模块：
//! - [`ExecutionPort`]：live 网关与回测撮合实现同一端口；
//! - [`SimulationPolicy`]：只描述「回测特有的成交假设」（成交价、时点、
//!   禁交易窗口），**不重复实现**手续费/保证金/盈亏；
//! - [`SimulationExecution`]：所有结算只经 `TradingRules` + `PositionRules`，
//!   产出带 `fee`/`pnl` 的 [`Fill`]，供报表与账户共用同一份数据。

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use strategy_core::signal::Side;

use crate::core::bar::{Bar, Money};
use crate::core::error::{Result, SpiError};
use crate::core::identity::InstrumentId;
use crate::core::position::{CloseLeg, PositionBook, PositionLot, PositionRules};
use crate::core::session::{SessionRules, TradingDay};
use crate::core::trading::{
    format_qty_raw, FeeRequest, MarginRequest, Offset, OrderDraft, TradingRules,
};

/// 发给订单出口的请求。offset 与 close_leg 已由 PositionRules 填好，
/// 网关只负责翻译协议，不再自己猜开平。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub signal_id: String,
    pub instrument: InstrumentId,
    pub side: Side,
    pub offset: Offset,
    pub price: f64,
    pub qty: f64,
    /// 平仓腿（回执对账 / 手续费口径追溯）
    pub close_leg: Option<CloseLeg>,
    pub reduce_only: bool,
    /// 止损平仓标志（v2.1 C1：从策略动作透传到 Fill，驱动冷却日与报表口径）
    #[serde(default)]
    pub is_stop_loss: bool,
    /// 申报数量原始字面量（v2.2 C7，§14-13）：live 平仓帧的 quantity 真源
    /// （对齐 `positions::state::remaining_size_raw` 语义）；缺失时下游
    /// 按 `qty_precision` 格式化派生，**不得伪造外部精确来源**。
    #[serde(default)]
    pub qty_raw: Option<String>,
    /// 归属策略实例（横截面链路写 `strategy_id`；旧链 None，serde 兼容）
    #[serde(default)]
    pub strategy_id: Option<String>,
    /// 订单意图（`sub_` 链路写 `SubIntent::as_str()`；旧链 None）
    #[serde(default)]
    pub intent: Option<String>,
}

impl OrderRequest {
    pub fn draft(&self) -> OrderDraft {
        OrderDraft {
            instrument: self.instrument.clone(),
            side: self.side,
            offset: self.offset,
            price: self.price,
            qty: self.qty,
            qty_raw: self.qty_raw.clone(),
        }
    }
}

/// 下单时点的市场上下文。live 实现只用于日志/幂等，回测实现用于定价。
#[derive(Debug, Clone)]
pub struct OrderContext {
    pub at: DateTime<Utc>,
    pub trigger_price: f64,
    pub reference_bar: Option<Bar>,
    pub trading_day: TradingDay,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderAck {
    pub order_id: String,
    pub accepted: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderState {
    Pending,
    Filled,
    PartiallyFilled,
    Rejected,
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderSnapshot {
    pub order_id: String,
    pub state: OrderState,
    pub filled_qty: f64,
    pub avg_price: f64,
    /// 实盘回执里的真实手续费（现状恒 0，必须补齐）
    pub fee: Money,
}

/// 成交。一条 Fill 同时是账户流水、报表 TradeRow、持仓更新的唯一数据源。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fill {
    pub order_id: String,
    pub signal_id: String,
    pub instrument: InstrumentId,
    pub side: Side,
    pub offset: Offset,
    pub qty: f64,
    pub price: f64,
    pub fee: Money,
    pub realized_pnl: Money,
    pub margin_release: Money,
    pub at: DateTime<Utc>,
    pub trading_day: TradingDay,
    /// 止损平仓成交（v2.1 C1：Fill 是唯一结算数据源，冷却日/报表由此派生）
    #[serde(default)]
    pub is_stop_loss: bool,
    /// 成交数量原始字面量（v2.2 C7）：live = 回报 `filled_quantity` 原样
    /// （与 `core::signal::SignalIntent::size_raw` 同源口径）；模拟 = 按
    /// 规格精度从规整量派生。账本递减只信本字段，不信 f64 往返。
    #[serde(default)]
    pub qty_raw: Option<String>,
}

/// 订单出口端口。一个实例绑定一个交易所；多交易所混跑由
/// `ExchangeRegistry` 按 `InstrumentId` 路由到不同实例。
pub trait ExecutionPort: Send {
    fn submit(&mut self, order: &OrderRequest, ctx: &OrderContext) -> Result<OrderAck>;

    fn query(&mut self, order_id: &str) -> Result<OrderSnapshot>;

    fn remote_positions(&mut self, instrument: &InstrumentId) -> Result<Vec<PositionLot>>;

    /// 该信号的订单是否仍在途（已建行但未到终态：Created/Validated/
    /// Submitted/Accepted）。对账用它区分「远程账本尚未反映的开仓」与
    /// 「远程确实无仓」：实盘成交回补存在秒级~分钟级延迟，仅凭远程空仓
    /// 收口会让随后到达的成交变成无主的 legacy 远程仓。
    /// 模拟撮合即时成交、无异步在途，缺省 `false`。
    fn order_in_flight(&self, _signal_id: &str) -> bool {
        false
    }

    /// 每根已闭合 bar 的后结算钩子（v2.1 C3）：模拟出口在此盯市，
    /// 保证 `Account.equity()` 与旧引擎逐分钟权益曲线同口径；
    /// live 实现可缺省（权益由对账服务给出）。
    fn on_bar(&mut self, _bar: &Bar) {}

    /// 模拟撮合即时产出成交；live 由 order_update 流异步产出，此处恒空。
    fn drain_fills(&mut self) -> Vec<Fill> {
        Vec::new()
    }

    /// 账户快照（只有模拟撮合能本地给出；live 由对账服务提供）。
    fn account_snapshot(&self) -> Option<Account> {
        None
    }

    /// 阶段 5b（v2.11，§13 live 编排合并）：编排层的 live 特有访问缝。
    ///
    /// live 桥（`exchanges::live::LiveExecution`）装进 `Box<dyn
    /// ExecutionPort>` 后，外层 actor 仍需桥特有 API
    /// （`take_submit_requests` / `on_order_update` / `manager_mut`）驱动
    /// async 半边（B2：桥内零 async）。缺省 None = 不需要该缝的端口
    /// （`SimulationExecution` 等）零影响零回归；仅 live 桥实现覆写返回自身。
    ///
    /// **P1-6 改进**：原 `as_any_mut() -> Option<&mut dyn Any>` 的运行时
    /// downcast 替换为编译期类型安全的 `as_live_mut() -> Option<&mut dyn
    /// LiveExecutionPort>`——消除 `Any` 擦除，调用方直接走 trait 方法，
    /// 错配出口在编译期报错而非运行时 panic。
    fn as_live_mut(&mut self) -> Option<&mut dyn super::super::exchanges::live::LiveExecutionPort> {
        None
    }

    /// 阶段 5b（v2.11）：**场所侧换算量**声明。
    ///
    /// `true` = 本出口的成交量权威在场所/内核侧（live：`order_cash` 由
    /// 服务端换算；paper：撮合内核按自身权益换算），runner 的本地
    /// `sizing` 闸门必须旁路——否则 `account_snapshot` 为 None（权益
    /// 服务端权威，模板不伪造）时 equity=0 会**误杀所有开仓**。
    /// 缺省 `false` = 本地撮合口径（`SimulationExecution` 回测按注入
    /// 权益 sizing），双跑链零回归。
    fn venue_sized(&self) -> bool {
        false
    }

    /// 场所侧单笔开仓名义（USDT）；`None` = 本地撮合，数量完全由 runner 决定。
    /// live 出口返回场所实际下单口径（如 trading-service 的 `order_cash`），
    /// runner 必须按该名义预留容量并生成影子数量，否则本地账本与场所敞口脱节。
    fn venue_order_notional(&self) -> Option<f64> {
        None
    }
}

// ─── 回测特有的成交假设 ───────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FillTiming {
    /// 当前 1m 收盘价成交（现状）
    ThisBarClose,
    /// 下一根 1m 开盘价成交（避免用未来数据）
    NextBarOpen,
}

/// 只负责「成交价与可交易时点」，不碰手续费/盈亏/保证金。
pub trait SimulationPolicy: Send + Sync {
    fn fill_timing(&self) -> FillTiming {
        FillTiming::ThisBarClose
    }

    /// trigger 为策略触发价；bar 为当前 1m（NextBarOpen 时由 runtime 传入下一根）。
    fn fill_price(&self, bar: Option<&Bar>, side: Side, trigger: f64) -> f64;

    /// 禁交易窗口等市场专属限制，实盘/回测共用 `SessionRules`。
    fn can_trade(&self, at: DateTime<Utc>, session: &dyn SessionRules) -> bool {
        !session.is_no_trade_window(at)
    }

    fn mark_price(&self, bar: &Bar) -> f64 {
        bar.close
    }
}

#[derive(Debug, Clone)]
pub struct SimpleFillModel {
    pub slippage: f64,
    pub timing: FillTiming,
}

impl Default for SimpleFillModel {
    fn default() -> Self {
        Self {
            slippage: 0.0,
            timing: FillTiming::ThisBarClose,
        }
    }
}

impl SimulationPolicy for SimpleFillModel {
    fn fill_timing(&self) -> FillTiming {
        self.timing
    }

    fn fill_price(&self, bar: Option<&Bar>, side: Side, trigger: f64) -> f64 {
        let base = match self.timing {
            FillTiming::ThisBarClose => trigger,
            FillTiming::NextBarOpen => bar.map(|b| b.open).unwrap_or(trigger),
        };
        match side {
            Side::Long => base * (1.0 + self.slippage),
            Side::Short => base * (1.0 - self.slippage),
        }
    }
}

// ─── 账户 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub currency: String,
    pub initial_cash: Money,
    pub cash: Money,
    pub realized_pnl: Money,
    pub total_fee: Money,
    pub margin_used: Money,
    pub unrealized_pnl: Money,
}

impl Account {
    pub fn new(currency: impl Into<String>, initial_cash: Money) -> Self {
        Self {
            currency: currency.into(),
            initial_cash,
            cash: initial_cash,
            realized_pnl: 0.0,
            total_fee: 0.0,
            margin_used: 0.0,
            unrealized_pnl: 0.0,
        }
    }

    pub fn equity(&self) -> Money {
        self.cash + self.unrealized_pnl
    }
}

// ─── 统一的模拟撮合出口 ───────────────────────────────────────────────────

/// 回测 / paper 共用的撮合实现。规则全部来自注入的 `Arc<dyn ...>`，
/// 因此换交易所不需要改撮合代码。
pub struct SimulationExecution {
    pub rules: Arc<dyn TradingRules>,
    pub position_rules: Arc<dyn PositionRules>,
    pub session: Arc<dyn SessionRules>,
    pub simulation: Arc<dyn SimulationPolicy>,
    pub account: Account,
    pub book: PositionBook,
    pending_fills: Vec<Fill>,
    seq: u64,
}

impl SimulationExecution {
    pub fn new(
        instrument: InstrumentId,
        initial_cash: Money,
        rules: Arc<dyn TradingRules>,
        position_rules: Arc<dyn PositionRules>,
        session: Arc<dyn SessionRules>,
        simulation: Arc<dyn SimulationPolicy>,
    ) -> Self {
        let currency = rules
            .spec(&instrument)
            .map(|s| s.currency.clone())
            .unwrap_or_else(|_| "CNY".into());
        let book = PositionBook::new(instrument, position_rules.mode());
        Self {
            rules,
            position_rules,
            session,
            simulation,
            account: Account::new(currency, initial_cash),
            book,
            pending_fills: Vec::new(),
            seq: 0,
        }
    }

    /// 每根 1m 结束时盯市（唯一口径，替代 engine 里另算一遍的浮动盈亏）。
    pub fn mark_to_market(&mut self, bar: &Bar) {
        let price = self.simulation.mark_price(bar);
        self.mark_to_market_price(price);
    }

    /// 按给定价盯市。v2.1 C3 的成交时点变体：见 submit 末注释。
    fn mark_to_market_price(&mut self, price: f64) {
        let mut unrealized = 0.0;
        for lot in &self.book.lots {
            let sign = match lot.side {
                Side::Long => 1.0,
                Side::Short => -1.0,
            };
            unrealized += (price - lot.entry_price)
                * lot.remaining
                * sign
                * self
                    .rules
                    .spec(&lot.instrument)
                    .map(|s| s.multiplier)
                    .unwrap_or(1.0);
        }
        self.account.unrealized_pnl = unrealized;
    }

    pub fn account(&self) -> &Account {
        &self.account
    }
}

impl ExecutionPort for SimulationExecution {
    fn submit(&mut self, order: &OrderRequest, ctx: &OrderContext) -> Result<OrderAck> {
        // 1. 规则校验（实盘与回测同一份）
        self.rules.validate(&order.draft())?;

        // 2. 市场门控
        if !self.simulation.can_trade(ctx.at, self.session.as_ref()) {
            return Ok(OrderAck {
                order_id: String::new(),
                accepted: false,
                reason: Some("禁交易窗口".into()),
            });
        }

        // 3. 成交价（唯一的回测特有假设）
        let fill_price =
            self.simulation
                .fill_price(ctx.reference_bar.as_ref(), order.side, ctx.trigger_price);
        let fill_price = self.rules.normalize_price(&order.instrument, fill_price)?;

        // 4. 结算（唯一的 fee / pnl 计算点）
        let fee = self.rules.fee(&FeeRequest {
            instrument: order.instrument.clone(),
            price: fill_price,
            qty: order.qty,
            offset: order.offset,
        })?;
        let (pnl, margin_release) = if order.offset == Offset::Open {
            (0.0, 0.0)
        } else {
            let entry_side = match order.side {
                Side::Long => Side::Short, // 买入平空：原仓是空
                Side::Short => Side::Long,
            };
            let entry_price = order
                .close_leg
                .as_ref()
                .map(|l| l.entry_price)
                .unwrap_or(fill_price);
            let pnl = self.rules.pnl(
                &order.instrument,
                entry_side,
                entry_price,
                fill_price,
                order.qty,
            )?;
            let release = self.rules.margin(&MarginRequest {
                instrument: order.instrument.clone(),
                price: entry_price,
                qty: order.qty,
            })?;
            (pnl, release)
        };

        self.account.cash += pnl - fee;
        self.account.realized_pnl += pnl;
        self.account.total_fee += fee;
        self.account.margin_used = (self.account.margin_used - margin_release).max(0.0);
        if order.offset == Offset::Open {
            self.account.margin_used += self.rules.margin(&MarginRequest {
                instrument: order.instrument.clone(),
                price: fill_price,
                qty: order.qty,
            })?;
        }

        // 4.5 v2.2 C7：成交数量字面量。live 网关传入回报 `filled_quantity`
        // 原样；模拟链路申报无 raw 时按规格精度派生（申报口径字面量，
        // 不是外部真源——外部真源只来自回报，与 manager.rs 同语义）。
        let fill_raw = order.qty_raw.clone().or_else(|| {
            self.rules
                .spec(&order.instrument)
                .ok()
                .map(|s| format_qty_raw(order.qty, s.qty_precision))
        });

        // 5. 持仓账本（今昨仓由交易所规则决定）
        if order.offset == Offset::Open {
            let mut draft = order.draft();
            draft.qty_raw = fill_raw.clone();
            self.position_rules.on_open_fill(
                &mut self.book,
                &draft,
                order.qty,
                fill_price,
                ctx.at,
                self.session.as_ref(),
            )?;
        } else {
            let leg = order
                .close_leg
                .clone()
                .ok_or_else(|| SpiError::Execution("平仓请求缺少 CloseLeg".into()))?;
            self.position_rules.on_close_fill(
                &mut self.book,
                &leg,
                order.qty,
                fill_raw.as_deref(),
            )?;
        }

        // 5.5 v2.6（阶段 3 双跑发现）：成交后立即按成交价重盯市。
        // 修复前 `unrealized_pnl` 要等下一根 on_bar 才刷新——同根反手
        // （先平后开）时 `equity()` 仍含已平旧仓的浮盈，开仓 sizing 被
        // 虚增权益污染；全平后权益也滞留旧值。成交回报是唯一结算数据源。
        self.mark_to_market_price(fill_price);

        self.seq += 1;
        let order_id = format!("sim_{}_{}", ctx.at.timestamp_millis(), self.seq);
        self.pending_fills.push(Fill {
            order_id: order_id.clone(),
            signal_id: order.signal_id.clone(),
            instrument: order.instrument.clone(),
            side: order.side,
            offset: order.offset,
            qty: order.qty,
            price: fill_price,
            fee,
            realized_pnl: pnl,
            margin_release,
            at: ctx.at,
            trading_day: ctx.trading_day,
            is_stop_loss: order.is_stop_loss,
            qty_raw: fill_raw,
        });
        Ok(OrderAck {
            order_id,
            accepted: true,
            reason: None,
        })
    }

    fn query(&mut self, order_id: &str) -> Result<OrderSnapshot> {
        Ok(OrderSnapshot {
            order_id: order_id.to_string(),
            state: OrderState::Filled,
            filled_qty: 0.0,
            avg_price: 0.0,
            fee: 0.0,
        })
    }

    fn remote_positions(&mut self, _instrument: &InstrumentId) -> Result<Vec<PositionLot>> {
        Ok(self.book.lots.clone())
    }

    /// v2.1 C3：每根 1m 收盘盯市（主循环唯一盯市点）。
    fn on_bar(&mut self, bar: &Bar) {
        self.mark_to_market(bar);
    }

    fn drain_fills(&mut self) -> Vec<Fill> {
        std::mem::take(&mut self.pending_fills)
    }

    fn account_snapshot(&self) -> Option<Account> {
        Some(self.account.clone())
    }
}
