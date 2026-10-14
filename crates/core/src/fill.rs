//! 成交通知（设计文档附录 A：FillEvent）
//!
//! 由订单状态机在订单**完全成交**时产生（部分成交只在订单内累积，不发本事件），
//! 经 mpsc 交给仓位管理。时间一律取回调承载的交易所/K 线时间，禁止 now()。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::signal::{IntentKind, Side};

/// 一次完全成交（反手时平/开各产生一条）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillEvent {
    /// 触发本订单的信号 ID（确定性 sig_ 前缀）
    pub signal_id: String,
    /// trading-service / 交易所订单 ID（回测为合成 bt_{signal_id}）
    pub order_id: String,
    pub symbol: String,
    /// 成交方向（Buy=平空/开多；Sell=平多/开空，具体语义由仓位管理按净仓判定）
    pub direction: Side,
    /// 成交量（基础币）
    pub filled_size: f64,
    /// 成交均价
    pub filled_price: f64,
    /// 本笔手续费（执行层上报；回测来自撮合配置）
    pub fee: f64,
    /// 事件时间（K 线/交易所时间，UTC）
    pub event_time: DateTime<Utc>,
    /// 是否为止损平仓（B6：跨管道透传，替代 live 侧硬编码 false）
    #[serde(default)]
    pub is_stop_loss: bool,
    /// 平仓原因（止损/止盈/反手/普通 live_close）
    #[serde(default)]
    pub exit_reason: Option<String>,
    /// 开仓初始止损价（Open 成交携带 intent.stop_price；None 由消费侧兜底）
    #[serde(default)]
    pub initial_stop: Option<f64>,
    /// 意图类型：开仓/平仓语义（无仓收到平仓 fill 时 fail-closed，不反手开仓）
    #[serde(default)]
    pub kind: Option<IntentKind>,
    /// 关联仓位（平仓成交携带 intent.position_id）
    #[serde(default)]
    pub position_id: Option<String>,
    /// 成交量原始字面量（回报 filled_quantity 原样；开仓时落 PositionState，
    /// 作为平仓帧 quantity 的精确来源）
    #[serde(default)]
    pub filled_size_raw: Option<String>,
}

impl FillEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        signal_id: impl Into<String>,
        order_id: impl Into<String>,
        symbol: impl Into<String>,
        direction: Side,
        filled_size: f64,
        filled_price: f64,
        fee: f64,
        event_time: DateTime<Utc>,
    ) -> Self {
        Self {
            signal_id: signal_id.into(),
            order_id: order_id.into(),
            symbol: symbol.into(),
            direction,
            filled_size,
            filled_price,
            fee,
            event_time,
            is_stop_loss: false,
            exit_reason: None,
            initial_stop: None,
            kind: None,
            position_id: None,
            filled_size_raw: None,
        }
    }

    /// 标记为止损/带原因平仓（链式）
    pub fn with_exit(mut self, is_stop_loss: bool, reason: impl Into<String>) -> Self {
        self.is_stop_loss = is_stop_loss;
        self.exit_reason = Some(reason.into());
        self
    }

    /// 携带意图语义/关联仓位/成交量原始字面量（链式；由信号内核构造时填）
    pub fn with_fill_meta(
        mut self,
        kind: IntentKind,
        position_id: Option<String>,
        filled_size_raw: Option<String>,
    ) -> Self {
        self.kind = Some(kind);
        self.position_id = position_id;
        self.filled_size_raw = filled_size_raw;
        self
    }

    /// 携带开仓初始止损（链式）
    pub fn with_initial_stop(mut self, stop: Option<f64>) -> Self {
        self.initial_stop = stop;
        self
    }
}
