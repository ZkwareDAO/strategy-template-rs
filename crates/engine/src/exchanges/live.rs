//! 阶段 4（v2.7，§13-4）：engine 端口 ↔ 现有实盘管道的共享桥接原语。
//!
//! §17 signals 行点名的隐藏工作量——「`ExecutionPort → (SignalManager +
//! AsyncOrderGateway)` 桥适配器」在此落地。两个原语：
//!
//! - [`LiveExecution`]：实现 [`ExecutionPort`]，把 runner 的
//!   `OrderRequest`（offset 已由 `PositionRules` 拆腿）翻译为
//!   `SignalIntent`（带 `offset` 标签，§13 过渡口径），交 `SignalManager`
//!   状态机；回报经 [`LiveExecution::on_order_update`] 回流为 engine [`Fill`]
//!   （fee 不再恒 0，qty_raw 逐字取回报 `filled_quantity`——§14-13）。
//! - [`ChannelBarSource`]：实现 [`BarEventSource`]，把实盘 WS 已闭合
//!   1m（`Candle`）经 std channel 桥进 runner 主循环——§14-2 的
//!   「Actor 邮箱桥接」形状（同步拉取，断流即 `Ok(None)` 收尾）。
//!
//! 设计约束（铁律不破）：
//! - **规则单一来源**：`LiveExecution` 只持有注入的 `Arc<dyn ...>`，
//!   live 与 backtest 拿到同一批对象（首公理）；
//! - **信号同一套代码**（文末原则 3）：本桥复用 `SignalManager` 状态机
//!   本体（retry/recover/在途约束/落盘全部不动），不复制第二套下单管道；
//! - **不依赖 broker**：网关 await 由外层 actor 驱动（B2：runtime 内
//!   不得 block_on），桥只提供同步半部 `take_submit_requests` +
//!   `apply_submit_outcome`，沿用旧 `RuntimeActor` 的形状（该模块已随 R4 退役删除，
//!   现宿主为 `app::spi_live_actor`）。
//! - live 权益口径：`realized_pnl`/`margin_release`/`account_snapshot`
//!   在模板侧不可信（服务端账户才是权威），Fill 落 0 且如实注释，
//!   报表派生归阶段 5（`TradeRow` 从 Fill 派生时接对账服务）。

use std::collections::{BTreeMap, VecDeque};
use std::sync::mpsc::Receiver;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use strategy_core::candle::Candle;
use strategy_core::order::OrderState as CoreOrderState;
use strategy_core::signal::Side;
use strategy_core::{FillEvent, IntentKind, SignalIntent, SignalStatus};
use strategy_signals::gateway::SubmitError;
use strategy_signals::manager::{SignalManager, SubmitRequest};

use crate::core::bar::{Bar, BarEventSource, MarketEvent, Money};
use crate::core::error::{Result, SpiError};
use crate::core::execution::{
    Account, ExecutionPort, Fill, OrderAck, OrderContext, OrderRequest, OrderSnapshot, OrderState,
};
use crate::core::identity::InstrumentId;
use crate::core::position::{PositionBook, PositionLot, PositionRules};
use crate::core::session::SessionRules;
use crate::core::trading::{FeeRequest, Offset, OrderDraft, TradingRules};

// ─── 订单出口：engine ↔ 信号管道 ──────────────────────────────────────────────

/// P1-6 改进：live 特有 API 的编译期类型安全 trait。
///
/// 替代原 `as_any_mut() -> Option<&mut dyn Any>` 的运行时 downcast 方案。
/// actor 侧通过 `ExecutionPort::as_live_mut()` 拿到 `&mut dyn
/// LiveExecutionPort` 后直接调用方法——错配出口在编译期报错而非运行时
/// `.expect("必须是 LiveExecution")` panic。
///
/// 仅 `LiveExecution` 实现此 trait；`SimulationExecution` 等回测出口不实现，
/// `as_live_mut()` 缺省返回 `None`。
pub trait LiveExecutionPort: Send {
    /// 信号管理器只读视图（查询 intent / order_id 映射 / due_retries 等）。
    fn manager(&self) -> &SignalManager;
    /// 信号管理器可变视图（apply_ready_fill / request_flush / recover 等）。
    fn manager_mut(&mut self) -> &mut SignalManager;
    /// 取走待提交的 `SubmitRequest` 队列（async 半边由外壳驱动）。
    fn take_submit_requests(&mut self) -> Vec<SubmitRequest>;
    /// 是否有待提交请求。
    fn has_pending_submits(&self) -> bool;
    /// 提交回执落状态机。
    fn apply_submit_outcome(
        &mut self,
        req: SubmitRequest,
        outcome: std::result::Result<String, SubmitError>,
    ) -> std::io::Result<()>;
    /// 回报入口：状态机推进 + engine Fill 转换 + 镜像账本递减。
    fn on_order_update(&mut self, u: &LiveOrderUpdate) -> std::io::Result<Option<Fill>>;
    /// 外层对账整体覆盖镜像账本（服务端仓位视图才是权威）。
    fn reconcile_book(&mut self, lots: Vec<PositionLot>);
    /// paper 本地撮合 ReadyFill 的桥侧吸收口。
    fn absorb_ready_fill(&mut self, fe: &FillEvent) -> Option<Fill>;
    /// 预期手续费（规则单一来源直查）。
    fn expected_fee(&self, order: &OrderRequest) -> Result<Money>;
}

/// `on_order_update` 的入参形态：外层 actor 从网关事件（broker
/// `OrderUpdate` 等）搬进桥的最小结构。**不依赖 broker**（依赖方向纪律）。
#[derive(Debug, Clone)]
pub struct LiveOrderUpdate {
    pub order_id: String,
    pub exchange_order_id: Option<String>,
    pub state: CoreOrderState,
    pub filled_size: f64,
    pub avg_price: f64,
    /// 回报 `filled_quantity` 逐字面量（§14-13：Fill.qty_raw 唯一真源）
    pub filled_quantity_raw: Option<String>,
    pub signal_id: Option<String>,
    pub message: Option<String>,
    /// 真实手续费（§13-4 验收：FillEvent/Fill 的 fee 不再恒 0）
    pub fee: Option<f64>,
    /// 事件时间（K 线/交易所时间；本模块禁止 now()）
    pub at: DateTime<Utc>,
}

/// live 本地镜像权益账本（PR-5 T2）。
///
/// 权威权益在券商侧账户（bridge 契约 v0.3 无权益帧）——本镜像由成交回报
/// 逐笔累计（真实 fee + 本地折算 realized）与镜像 book 盯市合成，
/// 且构造/装配处明示 `nav_source=local_mirror`，**绝不冒充券商口径**。
struct MirrorAccount {
    /// 起账现金（= `SourceRequest.initial_cash`）
    initial_cash: f64,
    cash: f64,
    realized: f64,
    fee: f64,
    /// 最近一根 1m 收盘价（未收到任何 bar 时 None：unrealized=0，不伪造 mark）
    last_price: Option<f64>,
}

/// live 订单出口：runner 的主循环不感知差异，这里把 `OrderRequest` 交给
/// 与旧 `RuntimeActor` **同一个** `SignalManager` 状态机。
pub struct LiveExecution {
    instrument: InstrumentId,
    strategy_type: String,
    manager: SignalManager,
    rules: Arc<dyn TradingRules>,
    position_rules: Arc<dyn PositionRules>,
    session: Arc<dyn SessionRules>,
    /// 成交镜像账本（live 权威在服务端；镜像用于 runner 决策视图与
    /// `remote_positions`，对账纠偏由外层驱动 [`Self::reconcile_book`]）
    book: PositionBook,
    /// CTP true：出帧带 `offset`（平今/平昨，§13-4 验收）；
    /// crypto false：净仓模型维持 reduce_only 隐式判定（零回归）。
    emit_offset: bool,
    /// 待外壳 async 提交的请求（`take_submit_requests` 取走）
    pending_submits: VecDeque<SubmitRequest>,
    /// 回报转换出的 engine Fill（`drain_fills` 取走）
    fills: VecDeque<Fill>,
    /// signal_id → 原 OrderRequest（回报对账 offset/腿/stop 语义）
    orders: BTreeMap<String, OrderRequest>,
    /// order_id → 最新快照（`query` 服务）
    states: BTreeMap<String, OrderSnapshot>,
    /// 本地镜像权益（PR-5 T2：LiveExecution 的 NAV 来源）
    mirror: MirrorAccount,
    /// 场所单笔开仓名义（trading-service `order_cash`）；None = 未声明，
    /// runner 按本地 sizing 生成数量（非币安横截面链的旧行为）。
    venue_order_notional: Option<f64>,
}

impl LiveExecution {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instrument: InstrumentId,
        strategy_type: impl Into<String>,
        manager: SignalManager,
        rules: Arc<dyn TradingRules>,
        position_rules: Arc<dyn PositionRules>,
        session: Arc<dyn SessionRules>,
        emit_offset: bool,
        initial_cash: f64,
    ) -> Self {
        // PR-5 T2 口径声明：live 权益 = 本地镜像（券商权益契约缺帧，不伪造）。
        tracing::info!(
            instrument = %instrument,
            nav_source = "local_mirror",
            initial_cash,
            "LiveExecution 权益=本地镜像；broker 权益契约缺帧，不伪造"
        );
        let book = PositionBook::new(instrument.clone(), position_rules.mode());
        Self {
            instrument,
            strategy_type: strategy_type.into(),
            manager,
            rules,
            position_rules,
            session,
            book,
            emit_offset,
            pending_submits: VecDeque::new(),
            fills: VecDeque::new(),
            orders: BTreeMap::new(),
            states: BTreeMap::new(),
            mirror: MirrorAccount {
                initial_cash,
                cash: initial_cash,
                realized: 0.0,
                fee: 0.0,
                last_price: None,
            },
            venue_order_notional: None,
        }
    }

    /// 声明场所单笔开仓名义，使 runner 按 `order_cash` 预留容量并生成影子数量。
    pub fn with_order_cash(mut self, order_cash: f64) -> Self {
        self.venue_order_notional = (order_cash > 0.0).then_some(order_cash);
        self
    }

    /// 只读访问信号内核（外层 actor 取 `due_retries`/`in_flight_orders`
    /// 等状态机能力；内核方法集合不改——§13「保持不变」清单）。
    pub fn manager(&self) -> &SignalManager {
        &self.manager
    }

    pub fn manager_mut(&mut self) -> &mut SignalManager {
        &mut self.manager
    }

    /// CTP 侧平今平昨是否显式出帧（binance=false / ctp=true）。
    pub fn emit_offset(&self) -> bool {
        self.emit_offset
    }

    /// `OrderRequest → SignalIntent`（§13 过渡口径逐字段）：
    /// - `id` 直接沿用 runner 的确定性 signal_id（幂等键）；
    /// - `symbol` 为裸挂牌代码（wire 帧 instrument=交易所注册名；§6.1
    ///   行键与帧内代码两角色分离）。行键命名空间化已在阶段 5c（v2.9）
    ///   交付：本路径 signal_id 由 runner 依 `namespaced_key()` 派生，
    ///   旧 CLI 链 `venue_key` 进哈希同位（§14-3 跨交易所不覆盖验收）。
    /// - 开仓 `size=0`（order_cash 换算路径不变，与旧 live 帧形一致）；
    ///   平仓 `size=qty` + `size_raw=qty_raw`（回报字面量精确递减）；
    /// - `offset`：emit_offset 时写 `Offset::label()`，crypto 侧 None；
    /// - `stop_order_id`：止损平仓以 signal_id 自标记（manager.fill_event
    ///   据此把 FillEvent.is_stop_loss 置真——与旧「止盈止损单 ID」同判据
    ///   位，不新增字段）。
    fn to_intent(&self, order: &OrderRequest, ctx: &OrderContext) -> SignalIntent {
        let is_open = order.offset == Offset::Open;
        SignalIntent {
            id: order.signal_id.clone(),
            strategy_id: self.strategy_type.clone(),
            symbol: order.instrument.symbol.clone(),
            kind: if is_open {
                IntentKind::Open
            } else {
                IntentKind::Close
            },
            direction: order.side,
            price: order.price,
            size: if is_open { 0.0 } else { order.qty },
            size_raw: if is_open { None } else { order.qty_raw.clone() },
            stop_price: None,
            position_id: order.close_leg.as_ref().map(|l| {
                if l.lot_ids.is_empty() {
                    l.offset.label().to_string()
                } else {
                    l.lot_ids.join("+")
                }
            }),
            stop_order_id: order.is_stop_loss.then(|| order.signal_id.clone()),
            created_event_time: ctx.at,
            status: SignalStatus::Created,
            offset: self.emit_offset.then(|| order.offset.label().to_string()),
        }
    }

    /// 外壳（actor）取走待提交请求；对每条做
    /// `gateway.submit_fut(&req.intent).await` 后把结果交回
    /// [`Self::apply_submit_outcome`]。**桥内零 async**：B2 教训
    /// （runtime 内 block_on 必 panic）不重演。
    pub fn take_submit_requests(&mut self) -> Vec<SubmitRequest> {
        self.pending_submits.drain(..).collect()
    }

    pub fn has_pending_submits(&self) -> bool {
        !self.pending_submits.is_empty()
    }

    /// 提交回执落状态机（委托 [`SignalManager::apply_submit_outcome`]，
    /// retry/退避语义内核自理——不动）。
    pub fn apply_submit_outcome(
        &mut self,
        req: SubmitRequest,
        outcome: std::result::Result<String, SubmitError>,
    ) -> std::io::Result<()> {
        if let Ok(order_id) = &outcome {
            self.states.insert(
                order_id.clone(),
                OrderSnapshot {
                    order_id: order_id.clone(),
                    state: OrderState::Pending,
                    filled_qty: 0.0,
                    avg_price: 0.0,
                    fee: 0.0,
                },
            );
        }
        self.manager
            .apply_submit_outcome(req, outcome)
            .map_err(std::io::Error::other)
    }

    /// 回报入口：状态机推进 + engine Fill 转换 + 镜像账本递减。
    /// 返回 `Some(Fill)` 同时进入 `drain_fills` 队列；
    /// 未知订单/非成交态返回 `None`（外部平仓由对账层负责）。
    pub fn on_order_update(&mut self, u: &LiveOrderUpdate) -> std::io::Result<Option<Fill>> {
        let fill_event = self
            .manager
            .handle_order_update_fee(
                &u.order_id,
                u.exchange_order_id.as_deref(),
                u.state,
                u.filled_size,
                u.avg_price,
                u.filled_quantity_raw.clone(),
                u.signal_id.as_deref(),
                u.message.as_deref(),
                u.fee,
                u.at,
            )
            .map_err(std::io::Error::other)?;
        // 快照登记（query 服务）
        self.states.insert(
            u.order_id.clone(),
            OrderSnapshot {
                order_id: u.order_id.clone(),
                state: map_state(u.state),
                filled_qty: u.filled_size,
                avg_price: u.avg_price,
                fee: u.fee.unwrap_or(0.0),
            },
        );
        let Some(fe) = fill_event else {
            return Ok(None);
        };
        let Some(fill) = self.to_fill(&fe) else {
            return Ok(None);
        };
        // 镜像权益结算先于账本递减：平仓 realized 的入场价回退路径要读
        // 平仓前的 book（腿缺失的重启补偿路径）。
        self.mirror_accrue(&fill);
        self.mirror_book(&fill);
        let out = fill.clone();
        self.fills.push_back(fill);
        Ok(Some(out))
    }

    /// FillEvent → engine Fill。offset/腿等申报语义取回原始 `OrderRequest`；
    /// 重启恢复后桥内无该请求（内存态丢失）时从意图回退重建最小口径，
    /// 并在 [`Self::reconcile_book`] 处由对账纠偏。
    fn to_fill(&self, fe: &FillEvent) -> Option<Fill> {
        let order = self.orders.get(&fe.signal_id).cloned().or_else(|| {
            // 回退：intent 行可恢复（Store 持久化）——offset 标签读
            // `SignalIntent.offset`；无标签按 kind 粗判（重启补偿路径）。
            let intent = self.manager.intent_for_signal(&fe.signal_id)?;
            let offset = intent
                .offset
                .as_deref()
                .and_then(Offset::from_label)
                .unwrap_or(match intent.kind {
                    IntentKind::Open => Offset::Open,
                    _ => Offset::Close,
                });
            Some(OrderRequest {
                signal_id: intent.id.clone(),
                instrument: self.instrument.clone(),
                side: intent.direction,
                offset,
                price: intent.price,
                qty: intent.size,
                close_leg: None,
                reduce_only: intent.kind != IntentKind::Open,
                is_stop_loss: intent.stop_order_id.is_some(),
                qty_raw: intent.size_raw.clone(),
                strategy_id: None,
                intent: None,
            })
        })?;
        Some(Fill {
            order_id: fe.order_id.clone(),
            signal_id: fe.signal_id.clone(),
            instrument: self.instrument.clone(),
            side: fe.direction,
            offset: order.offset,
            qty: fe.filled_size,
            price: fe.filled_price,
            // §13-4 验收：live 手续费 = 回报真值（None 时 FillEvent 已兜底 0）
            fee: fe.fee as Money,
            // 权威 realized_pnl/margin 在服务端账户，模板不伪造（保持 0，
            // 阶段 5 报表派生接对账数据源）
            realized_pnl: 0.0,
            margin_release: 0.0,
            at: fe.event_time,
            trading_day: self.session.trading_day_of(fe.event_time),
            is_stop_loss: fe.is_stop_loss,
            // §14-13 契约：qty_raw 必须以回报 `filled_quantity` 原样入账
            qty_raw: fe.filled_size_raw.clone(),
        })
    }

    /// 镜像账本递减：开仓按回报实际量入册，平仓按申报腿精确扣减。
    /// 腿缺失（重启补偿路径）时不强改——留给 `reconcile_book`。
    fn mirror_book(&mut self, fill: &Fill) {
        match fill.offset {
            Offset::Open => {
                let draft = OrderDraft {
                    instrument: fill.instrument.clone(),
                    side: fill.side,
                    offset: Offset::Open,
                    price: fill.price,
                    qty: fill.qty,
                    qty_raw: fill.qty_raw.clone(),
                };
                let _ = self.position_rules.on_open_fill(
                    &mut self.book,
                    &draft,
                    fill.qty,
                    fill.price,
                    fill.at,
                    self.session.as_ref(),
                );
            }
            _ => {
                if let Some(order) = self.orders.get(&fill.signal_id) {
                    if let Some(leg) = &order.close_leg {
                        let _ = self.position_rules.on_close_fill(
                            &mut self.book,
                            leg,
                            fill.qty,
                            fill.qty_raw.as_deref(),
                        );
                    }
                }
            }
        }
    }

    /// 外层对账（每分钟 `positions_fut` 结果换算成 `PositionLot`）整体
    /// 覆盖镜像账本——服务端仓位视图才是权威（live 语义）。
    pub fn reconcile_book(&mut self, lots: Vec<PositionLot>) {
        self.book.lots = lots;
    }

    /// 镜像权益逐笔结算（PR-5 T2）。
    ///
    /// - fee 恒取回报真值（§13-4 口径），Open/Close 均扣；
    /// - Open 成交不扣现金（保证金模型与 `SimulationExecution` 对齐）；
    /// - 平仓 realized 经规则表 `TradingRules::pnl`（含 multiplier/精度口径）
    ///   折算：入场价优先取申报腿（`CloseLeg.entry_price`），腿缺失的重启补偿
    ///   路径回退镜像 book 同向批次加权均价；两者皆无则不记（不伪造）。
    fn mirror_accrue(&mut self, fill: &Fill) {
        let fee = fill.fee;
        let pnl = if fill.offset == Offset::Open {
            0.0
        } else {
            let entry_side = match fill.side {
                Side::Long => Side::Short, // 买入平空：原仓是空
                Side::Short => Side::Long,
            };
            let entry_price = self
                .orders
                .get(&fill.signal_id)
                .and_then(|o| o.close_leg.as_ref().map(|l| l.entry_price))
                .or_else(|| self.mirror_avg_entry(entry_side));
            entry_price
                .and_then(|entry| {
                    self.rules
                        .pnl(&self.instrument, entry_side, entry, fill.price, fill.qty)
                        .ok()
                })
                .unwrap_or(0.0)
        };
        self.mirror.fee += fee;
        self.mirror.realized += pnl;
        self.mirror.cash += pnl - fee;
    }

    /// 镜像 book 中指定方向批次的加权入场均价（平仓 realized 的兜底口径）。
    fn mirror_avg_entry(&self, side: Side) -> Option<f64> {
        let mut notional = 0.0;
        let mut qty = 0.0;
        for lot in &self.book.lots {
            if lot.side == side && lot.remaining > 0.0 {
                notional += lot.entry_price * lot.remaining;
                qty += lot.remaining;
            }
        }
        (qty > 0.0).then(|| notional / qty)
    }

    /// 本地镜像浮动盈亏：镜像 book × 最近 1m 收盘价。无 mark（未收到 bar）时
    /// 恒 0——不伪造盯市。
    fn mirror_unrealized(&self) -> f64 {
        let Some(price) = self.mirror.last_price else {
            return 0.0;
        };
        let multiplier = self
            .rules
            .spec(&self.instrument)
            .map(|s| s.multiplier)
            .unwrap_or(1.0);
        self.book
            .lots
            .iter()
            .map(|lot| {
                let sign = match lot.side {
                    Side::Long => 1.0,
                    Side::Short => -1.0,
                };
                (price - lot.entry_price) * lot.remaining * sign * multiplier
            })
            .sum()
    }

    /// 手续费「规则单一来源」直查（§13-4 契约测试用）：live 对账与回测
    /// 撮合用同一个 `Arc<dyn TradingRules>` 计算期望手续费。
    pub fn expected_fee(&self, order: &OrderRequest) -> Result<Money> {
        self.rules.fee(&FeeRequest {
            instrument: order.instrument.clone(),
            price: order.price,
            qty: order.qty,
            offset: order.offset,
        })
    }

    /// 阶段 5b（v2.11，§13 live 编排合并）：paper 本地撮合（ReadyFill
    /// 通道）的桥侧吸收口。
    ///
    /// 信号状态机推进由外层完成（`manager.apply_ready_fill`——与旧链
    /// `submit_apply` 同一函数，零分叉）；本方法只补桥侧口径：
    /// `FillEvent → engine Fill` 入队 + 镜像账本递减——复用 `to_fill` /
    /// `mirror_book`（与 WS 回报成交同一对原语，不写第二套公式）。
    /// 入队后由编排层统一 drain（成交落地单一门）。
    pub fn absorb_ready_fill(&mut self, fe: &FillEvent) -> Option<Fill> {
        let fill = self.to_fill(fe)?;
        self.mirror_book(&fill);
        let out = fill.clone();
        self.fills.push_back(fill);
        Some(out)
    }
}

impl LiveExecutionPort for LiveExecution {
    fn manager(&self) -> &SignalManager {
        &self.manager
    }
    fn manager_mut(&mut self) -> &mut SignalManager {
        &mut self.manager
    }
    fn take_submit_requests(&mut self) -> Vec<SubmitRequest> {
        self.pending_submits.drain(..).collect()
    }
    fn has_pending_submits(&self) -> bool {
        !self.pending_submits.is_empty()
    }
    fn apply_submit_outcome(
        &mut self,
        req: SubmitRequest,
        outcome: std::result::Result<String, SubmitError>,
    ) -> std::io::Result<()> {
        LiveExecution::apply_submit_outcome(self, req, outcome)
    }
    fn on_order_update(&mut self, u: &LiveOrderUpdate) -> std::io::Result<Option<Fill>> {
        LiveExecution::on_order_update(self, u)
    }
    fn reconcile_book(&mut self, lots: Vec<PositionLot>) {
        LiveExecution::reconcile_book(self, lots)
    }
    fn absorb_ready_fill(&mut self, fe: &FillEvent) -> Option<Fill> {
        LiveExecution::absorb_ready_fill(self, fe)
    }
    fn expected_fee(&self, order: &OrderRequest) -> Result<Money> {
        LiveExecution::expected_fee(self, order)
    }
}

fn map_state(s: CoreOrderState) -> OrderState {
    match s {
        CoreOrderState::Filled => OrderState::Filled,
        CoreOrderState::PartiallyFilled => OrderState::PartiallyFilled,
        CoreOrderState::Canceled => OrderState::Canceled,
        CoreOrderState::Rejected => OrderState::Rejected,
        CoreOrderState::Created | CoreOrderState::Sent | CoreOrderState::Ack => OrderState::Pending,
    }
}

impl ExecutionPort for LiveExecution {
    fn submit(&mut self, order: &OrderRequest, ctx: &OrderContext) -> Result<OrderAck> {
        let intent = self.to_intent(order, ctx);
        let prepared = self
            .manager
            .receive_intent_prepare(intent)
            .map_err(|e| SpiError::Execution(format!("信号内核建行失败: {e}")))?;
        match prepared {
            Some(req) => {
                self.orders.insert(order.signal_id.clone(), order.clone());
                self.pending_submits.push_back(req);
                Ok(OrderAck {
                    order_id: String::new(),
                    accepted: true,
                    reason: Some("live：已交信号管道，等待异步 ack".into()),
                })
            }
            // 重复 signal_id / B5 在途约束：内核已把原因写进信号行
            None => Ok(OrderAck {
                order_id: String::new(),
                accepted: false,
                reason: Some("live：信号内核拒收（重复 id 或在途约束）".into()),
            }),
        }
    }

    fn query(&mut self, order_id: &str) -> Result<OrderSnapshot> {
        self.states.get(order_id).cloned().ok_or_else(|| {
            SpiError::Execution(format!("live 桥无订单 {order_id}（异步查询归外壳）"))
        })
    }

    /// 镜像账本视图（runner 决策/终态汇总用；权威对账走
    /// [`Self::reconcile_book`]）。
    fn remote_positions(&mut self, _instrument: &InstrumentId) -> Result<Vec<PositionLot>> {
        Ok(self.book.lots.clone())
    }

    /// 实盘：信号行未到终态（Created/Validated/Submitted/Accepted）即视为
    /// 在途。成交回补延迟期间远程可能暂空，对账不得据此提前收口。
    fn order_in_flight(&self, signal_id: &str) -> bool {
        self.manager
            .status_of(signal_id)
            .is_some_and(|s| s.is_pending())
    }

    fn drain_fills(&mut self) -> Vec<Fill> {
        self.fills.drain(..).collect()
    }

    /// PR-5 T2：live 权益 = 本地镜像（明示 `nav_source=local_mirror`）。
    ///
    /// 券商侧无权益帧（bridge 契约 v0.3）——不返回 None 也不伪造，
    /// 由成交回报累计 + 镜像 book 盯市合成；currency/multiplier 与规则表同源。
    fn account_snapshot(&self) -> Option<Account> {
        let currency = self
            .rules
            .spec(&self.instrument)
            .map(|s| s.currency.clone())
            .unwrap_or_else(|_| "CNY".into());
        let mut account = Account::new(currency, self.mirror.initial_cash);
        account.cash = self.mirror.cash;
        account.realized_pnl = self.mirror.realized;
        account.total_fee = self.mirror.fee;
        account.unrealized_pnl = self.mirror_unrealized();
        Some(account)
    }

    /// 每根 1m 收盘写入镜像 mark 价（浮动盈亏唯一口径，见 `mirror_unrealized`）。
    fn on_bar(&mut self, bar: &Bar) {
        self.mirror.last_price = Some(bar.close);
    }

    /// 阶段 5b（v2.11）：编排层 live 特有 API 的编译期类型安全访问缝。
    /// P1-6：替代原 `as_any_mut` downcast——直接返回 `&mut dyn
    /// LiveExecutionPort`，调用方零 `Any` 擦除。
    fn as_live_mut(&mut self) -> Option<&mut dyn LiveExecutionPort> {
        Some(self)
    }

    /// 阶段 5b（v2.11）：成交量权威在场所侧——live 由服务端按 `order_cash`
    /// 换算（出帧形状与旧链逐字一致），paper 由撮合内核按自身权益换算；
    /// runner 本地 sizing 闸门旁路（见 `ExecutionPort::venue_sized` 文档）。
    fn venue_sized(&self) -> bool {
        true
    }

    fn venue_order_notional(&self) -> Option<f64> {
        self.venue_order_notional
    }
}

// ─── 行情源：WS 已闭合 1m → runner 主循环 ─────────────────────────────────

/// `BarEventSource` 的实盘桥接形态（§14-2：channel 桥，同步拉取）。
///
/// 外层 actor 把 `KlineEvent::Closed`（按 symbol 过滤后）的 `Candle`
/// 发进 channel；runner 线程 `recv()` 阻塞等待——WS 断流/任务退出时
/// channel 关闭，`Ok(None)` 收尾（与回测 `VecEventSource` 同一主循环
/// 消费形状，循环体零 `if mode` 分叉）。
///
/// 幂等水位：同/旧 `open_time` 帧丢弃（重连补缺与 preload 共用，
/// `Feed` 水位语义对齐，§12-7）。
pub struct ChannelBarSource {
    instrument: InstrumentId,
    rx: Receiver<Candle>,
    last_open_time: Option<DateTime<Utc>>,
}

impl ChannelBarSource {
    pub fn new(instrument: InstrumentId, rx: Receiver<Candle>) -> Self {
        Self {
            instrument,
            rx,
            last_open_time: None,
        }
    }
}

impl BarEventSource for ChannelBarSource {
    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        loop {
            match self.rx.recv() {
                // 发送端 drop：断流收尾
                Err(_) => return Ok(None),
                Ok(candle) => {
                    // 只消费已闭合 1m；未闭合快照归 Tick 语义，本桥丢弃
                    if !candle.is_closed {
                        continue;
                    }
                    // 幂等水位：重连补缺/重复帧丢弃（§12-7 同构）
                    if self
                        .last_open_time
                        .is_some_and(|prev| candle.open_time <= prev)
                    {
                        continue;
                    }
                    self.last_open_time = Some(candle.open_time);
                    return Ok(Some(MarketEvent::Bar(Bar::from_candle(
                        self.instrument.clone(),
                        candle,
                    ))));
                }
            }
        }
    }

    fn watermark(&self) -> Option<DateTime<Utc>> {
        self.last_open_time
    }

    /// 非阻塞轮询（PR-5 Task 1）：`recv_timeout(ZERO)`——
    /// Timeout → `SourceIdle`（暂无事件，区别于耗尽）；Disconnected → `Ok(None)`。
    /// 丢弃规则（未闭合快照、水位幂等）与 `next_event` 完全一致。
    fn try_next_event(&mut self) -> Result<Option<MarketEvent>> {
        loop {
            match self.rx.recv_timeout(std::time::Duration::ZERO) {
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    return Ok(Some(MarketEvent::SourceIdle))
                }
                // 发送端 drop：断流收尾
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Ok(None),
                Ok(candle) => {
                    if !candle.is_closed {
                        continue;
                    }
                    if self
                        .last_open_time
                        .is_some_and(|prev| candle.open_time <= prev)
                    {
                        continue;
                    }
                    self.last_open_time = Some(candle.open_time);
                    return Ok(Some(MarketEvent::Bar(Bar::from_candle(
                        self.instrument.clone(),
                        candle,
                    ))));
                }
            }
        }
    }
}
