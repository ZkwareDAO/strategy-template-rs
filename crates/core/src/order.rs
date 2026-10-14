//! 订单状态机（设计文档 §5.3.2）
//!
//! 与信号状态机分离：
//! [`OrderState`] 追踪 trading-service / 交易所侧订单；部分成交在
//! [`OrderProgress`] 内累积，仅完全成交时由调用方打包成 FillEvent。

use serde::{Deserialize, Serialize};

/// 订单状态（严格转移，拒绝非法跳转）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderState {
    /// 本地已创建，尚未发出
    Created,
    /// 已发送（等待对端 Ack）
    Sent,
    /// 对端已接收
    Ack,
    /// 部分成交
    PartiallyFilled,
    /// 完全成交（终态）
    Filled,
    /// 已取消（终态）
    Canceled,
    /// 被拒绝（终态）
    Rejected,
}

impl OrderState {
    /// 合法转移表
    pub fn can_transit(self, to: OrderState) -> bool {
        use OrderState::*;
        matches!(
            (self, to),
            (Created, Sent)
                | (Sent, Ack | Canceled | Rejected)
                | (Ack, PartiallyFilled | Filled | Canceled | Rejected)
                | (PartiallyFilled, PartiallyFilled | Filled | Canceled)
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            OrderState::Filled | OrderState::Canceled | OrderState::Rejected
        )
    }
}

/// 部分成交累积器：同一订单多次回报的成交量/成交额
#[derive(Debug, Clone, Default, PartialEq)]
pub struct OrderProgress {
    pub filled_size: f64,
    /// 累计成交额（∑ price×size），用于加权均价
    pub filled_value: f64,
    /// 回报次数
    pub updates: u32,
}

impl OrderProgress {
    pub fn new() -> Self {
        Self::default()
    }

    /// 应用一笔成交回报
    pub fn accumulate(&mut self, size: f64, price: f64) {
        self.filled_size += size;
        self.filled_value += size * price;
        self.updates += 1;
    }

    /// 加权成交均价（无成交为 0）
    pub fn weighted_price(&self) -> f64 {
        if self.filled_size <= 0.0 {
            return 0.0;
        }
        self.filled_value / self.filled_size
    }

    /// 累计成交量是否已达到订单目标量（容差 1e-9）
    pub fn is_fully_filled(&self, requested_size: f64) -> bool {
        self.filled_size + 1e-9 >= requested_size && requested_size > 0.0
    }
}

#[cfg(test)]
mod tests {
    use super::OrderState::*;
    use super::*;

    #[test]
    fn valid_order_lifecycle() {
        assert!(Created.can_transit(Sent));
        assert!(Sent.can_transit(Ack));
        assert!(Ack.can_transit(PartiallyFilled));
        assert!(PartiallyFilled.can_transit(PartiallyFilled));
        assert!(PartiallyFilled.can_transit(Filled));
    }

    #[test]
    fn cancel_reject_edges() {
        assert!(Sent.can_transit(Canceled));
        assert!(Sent.can_transit(Rejected));
        assert!(Ack.can_transit(Canceled));
        assert!(Ack.can_transit(Rejected));
        assert!(PartiallyFilled.can_transit(Canceled));
    }

    #[test]
    fn illegal_order_transitions() {
        assert!(!Created.can_transit(Ack), "不可跳过 Sent");
        assert!(!Ack.can_transit(Sent), "不可回退");
        assert!(!Filled.can_transit(Canceled), "Filled 终态");
        assert!(!Rejected.can_transit(Sent));
        assert!(
            !PartiallyFilled.can_transit(Rejected),
            "部分成交后只能撤不能拒"
        );
        for t in [Filled, Canceled, Rejected] {
            assert!(t.is_terminal());
        }
        assert!(!Ack.is_terminal());
    }

    #[test]
    fn partial_fill_accumulates_weighted_average() {
        let mut p = OrderProgress::new();
        p.accumulate(1.0, 100.0);
        p.accumulate(3.0, 110.0);
        assert_eq!(p.updates, 2);
        assert!((p.filled_size - 4.0).abs() < 1e-9);
        // (100 + 330) / 4 = 107.5
        assert!((p.weighted_price() - 107.5).abs() < 1e-9);
        assert!(p.is_fully_filled(4.0));
        assert!(!p.is_fully_filled(4.5));
    }

    #[test]
    fn empty_progress_price_zero() {
        assert_eq!(OrderProgress::new().weighted_price(), 0.0);
        assert!(!OrderProgress::new().is_fully_filled(1.0));
    }
}
