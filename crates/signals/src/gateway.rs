//! 订单网关端口 —— SignalManager 面向交易执行的唯一出口
//!
//! 设计文档 §3.2/§5.3：三条逻辑通道（下单 / 订单状态回调 / 订单查询）。
//! 两个端口共存：
//! - [`OrderGateway`] 是**同步**契约：回测适配器本地成交，由回测引擎逐根
//!   K 线确定性直调（BacktestOrderClient）
//! - [`AsyncOrderGateway`] 是 **async 契约**（boxed future，可 dyn）：
//!   live 信号 Actor 在 runtime 内 `.await`，对接 trading-service WS；
//!   不允许在 runtime 内再用 `Handle::block_on` 桥接（B2：必 panic）
//!
//! SignalManager 同步内核不感知 WS/重连/协议，只在状态机的对应点经 Actor
//! 外壳调用本端口。

use std::future::Future;
use std::pin::Pin;

use strategy_core::order::OrderState;
use strategy_core::SignalIntent;

/// boxed 异步提交结果（使 trait 可 dyn）
pub type SubmitFuture<'a> = Pin<Box<dyn Future<Output = Result<String, SubmitError>> + Send + 'a>>;

/// boxed 异步查询结果
pub type QueryFuture<'a> =
    Pin<Box<dyn Future<Output = Result<OrderSnapshot, SubmitError>> + Send + 'a>>;

/// boxed 异步仓位查询结果
pub type PositionsFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<RemotePosition>, SubmitError>> + Send + 'a>>;

/// 提交失败的分类（决定信号是否重试，设计文档 §5.2.4）
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FailureKind {
    /// 永久性拒绝（参数错误/风控/交易所拒单）：不重试
    Rejected,
    /// 超时/临时不可用：可重试
    Retryable,
}

#[derive(Debug, Clone)]
pub struct SubmitError {
    pub kind: FailureKind,
    pub message: String,
}

impl SubmitError {
    pub fn rejected(message: impl Into<String>) -> Self {
        Self {
            kind: FailureKind::Rejected,
            message: message.into(),
        }
    }
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            kind: FailureKind::Retryable,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for SubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for SubmitError {}

/// 远程仓位快照（get_positions 应答元素，仅取对账所需字段；
/// trading-service 仓位视图，由服务端自行与交易所同步）。
#[derive(Debug, Clone, PartialEq)]
pub struct RemotePosition {
    /// 归一化标的
    pub instrument: String,
    /// 交易所方向原值（大写 BUY/SELL）
    pub side: String,
    /// 当前持仓量（绝对值）
    pub quantity: f64,
    /// 持仓开仓均价（pos_price）
    pub avg_price: f64,
    /// 服务端存活标记：false=活跃仓，true=当日已平仓行（不得匹配）
    pub deleted: bool,
}

/// 实际委托要素（回调帧审计字段）：服务端受理后回写的真实委托类型/价/量。
/// 金额单（order_cash）本地意图不携带数量，真实委托量只在回调里出现。
#[derive(Debug, Clone, PartialEq)]
pub struct OrderPlace {
    /// limit / market（小写）
    pub order_type: String,
    /// 委托价；市价单为 0
    pub order_price: f64,
    /// 委托数量（quantity 或 order_cash 换算结果）
    pub order_quantity: f64,
}

/// 订单查询快照（重启对账输入）
#[derive(Debug, Clone, PartialEq)]
pub struct OrderSnapshot {
    pub order_id: String,
    pub state: OrderState,
    /// 累计成交量/加权均价
    pub filled_size: f64,
    pub avg_price: f64,
    /// 交易所原生单号（受理后才有，可能为 None）
    pub exchange_order_id: Option<String>,
    /// 累计成交量原始字面量（平仓帧 quantity 精确来源）
    pub filled_quantity_raw: Option<String>,
    /// 实际委托要素（审计核对"真实下了什么单"）
    pub place: Option<OrderPlace>,
}

/// 已就绪的成交（同步回测：submit 即成交；live 恒空——成交走 order_update 流）
#[derive(Debug, Clone, PartialEq)]
pub struct ReadyFill {
    pub order_id: String,
    pub filled_size: f64,
    pub avg_price: f64,
    /// 本笔手续费
    pub fee: f64,
}

/// 订单网关端口（单写者：SignalManager 独占 [`Box<dyn OrderGateway>`]）。
///
/// `&mut self` 使回测内核（现金/持仓可变）可直接实现；live 适配器内部用
/// channel 与连接 task 通信，`&mut self` 不构成约束。
pub trait OrderGateway: Send {
    /// 提交意图，成功返回 trading-service/交易所订单 ID
    fn submit(&mut self, intent: &SignalIntent) -> Result<String, SubmitError>;
    /// 查询订单（重启恢复对账）
    fn query_order(&mut self, order_id: &str) -> Result<OrderSnapshot, SubmitError>;
    /// 取出提交后同步就绪的成交（回测即时成交）。live 恒返回空。
    fn drain_ready_fills(&mut self) -> Vec<ReadyFill> {
        Vec::new()
    }
}

/// 异步订单网关端口（live 信号 Actor 单写者持有 `Box<dyn AsyncOrderGateway>`）。
///
/// 与 [`OrderGateway`] 同构，仅等待方式不同：Actor 在提交/查询点 `.await`，
/// 让出 worker 而非阻塞线程。实现侧须保证 submit 以确定性 signal_id 作为
/// 幂等键（client_msg_id），重试/重连重发不产生重复订单。
pub trait AsyncOrderGateway: Send {
    /// 提交意图，成功返回 trading-service/交易所订单 ID
    fn submit_fut<'a>(&'a mut self, intent: &'a SignalIntent) -> SubmitFuture<'a>;
    /// 按 signal_id 查询订单（重连/重启对账，先查后发；get_order 请求键）
    fn query_fut<'a>(&'a mut self, signal_id: &'a str) -> QueryFuture<'a>;
    /// 查询远程仓位（每分钟对账；instrument + side 精确筛选，无仓返回空 Vec）。
    /// side 为 `"long"/"short"`（兼容 buy/sell）
    fn positions_fut<'a>(&'a mut self, instrument: &'a str, side: &'a str) -> PositionsFuture<'a>;
    /// 提交后同步就绪的成交（回测型适配器即时成交）。live 恒返回空。
    fn drain_ready_fills(&mut self) -> Vec<ReadyFill> {
        let _ = self;
        Vec::new()
    }
}
