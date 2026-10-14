//! OrderRouter —— T7 order_forwarder 的路由表（按策略类型隔离）
//!
//! 信号提交成功（ack）后，RuntimeActor 把 (order_id, 自身句柄) 注册进来；
//! order_update 优先精确投递；查无路由时按帧内 strategy_name 仅广播给同策略
//! 类型的 Actor（无身份才全量兜底）。重连事件只通知同策略类型的 Actor 对账。
//! HashMap 仅 T7 单 task 读写，无锁。

use std::collections::HashMap;

use strategy_broker::OrderUpdate;
use tracing::{debug, warn};

use crate::runtime_actor::RuntimeHandle;

pub struct OrderRouter {
    /// order_id → 精确接收方（ack 后注册）
    routes: HashMap<String, RuntimeHandle>,
    /// (策略类型名, is_live, 句柄) 列表（同一策略类型可有多个符号实例）
    all: Vec<(String, bool, RuntimeHandle)>,
}

impl OrderRouter {
    pub fn new(all: Vec<(String, bool, RuntimeHandle)>) -> Self {
        Self {
            routes: HashMap::new(),
            all,
        }
    }

    pub fn bind(&mut self, order_id: String, actor: RuntimeHandle) {
        debug!(%order_id, "T7 路由绑定：order_id → owner Actor（之后精确投递）");
        self.routes.insert(order_id, actor);
    }

    /// 精确路由一单 order_update；未知订单按策略身份限定广播，无身份才全量兜底。
    pub async fn dispatch_update(&mut self, u: OrderUpdate) {
        if let Some(h) = self.routes.get(&u.order_id) {
            debug!(order_id = %u.order_id, ?u.state, "T7 精确路由：按 order_id 投递到 owner");
            Self::forward_update(h, &u, "订单更新精确投递失败").await;
            return;
        }
        match u.strategy_name.as_deref() {
            // 有身份：仅同策略类型的实例广播
            Some(name) => {
                debug!(order_id = %u.order_id, strategy = name, "T7 未知订单有身份：限定广播给同策略类型实例");
                for (_, _, h) in self.all.iter().filter(|(ty, _, _)| ty == name) {
                    Self::forward_update(h, &u, "订单更新广播投递失败").await;
                }
            }
            // 无身份：全量兜底
            None => {
                debug!(order_id = %u.order_id, "T7 未知订单无身份：全量广播兜底");
                for (_, _, h) in &self.all {
                    Self::forward_update(h, &u, "订单更新广播投递失败").await;
                }
            }
        }
    }

    /// 某策略类型的交易 WS（重）连：只通知该类型的 **live** RuntimeActor 对账。
    /// paper 实例不走交易 WS，重连与它无关，不得被波及（否则触发无意义对账）。
    pub async fn dispatch_reconnect(&self, strategy_name: &str) {
        debug!(
            strategy = strategy_name,
            "T7 交易 WS 重连：通知同策略类型 live Actor 对账"
        );
        for (_, _, h) in self
            .all
            .iter()
            .filter(|(ty, live, _)| ty == strategy_name && *live)
        {
            if let Err(e) = h.reconcile().await {
                warn!(strategy = %strategy_name, error = %e, "重连对账通知投递失败");
            }
        }
    }

    /// 向单个 Actor 投递订单更新，失败仅记日志（不影响其他接收方）。
    async fn forward_update(h: &RuntimeHandle, u: &OrderUpdate, err_msg: &'static str) {
        if let Err(e) = h
            .order_update(
                u.order_id.clone(),
                u.exchange_order_id.clone(),
                u.state,
                u.filled_size,
                u.avg_price,
                u.filled_quantity_raw.clone(),
                u.signal_id.clone(),
                u.message.clone(),
                u.place.clone(),
                chrono::Utc::now(),
            )
            .await
        {
            warn!(order_id = %u.order_id, error = %e, "{err_msg}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strategy_core::order::OrderState;
    use tokio::sync::mpsc;

    use crate::runtime_actor::{RuntimeCommand, RuntimeHandle};

    fn pair() -> (RuntimeHandle, mpsc::Receiver<RuntimeCommand>) {
        let (tx, rx) = mpsc::channel(8);
        (RuntimeHandle::for_test(tx), rx)
    }

    fn update(order_id: &str, strategy_name: Option<&str>) -> OrderUpdate {
        OrderUpdate {
            order_id: order_id.into(),
            exchange_order_id: Some("ex-1".into()),
            state: OrderState::Filled,
            filled_size: 1.0,
            avg_price: 100.0,
            filled_quantity_raw: Some("1".into()),
            strategy_name: strategy_name.map(str::to_string),
            signal_id: None,
            message: None,
            place: None,
        }
    }

    #[tokio::test]
    async fn bound_order_routes_only_to_owner() {
        let (h1, mut rx1) = pair();
        let (h2, mut rx2) = pair();
        let mut router = OrderRouter::new(vec![
            ("sar_snt3".into(), true, h1.clone()),
            ("sar_supertrend".into(), true, h2.clone()),
        ]);
        router.bind("ord_1".into(), h1);
        router
            .dispatch_update(update("ord_1", Some("sar_snt3")))
            .await;
        assert!(rx1.try_recv().is_ok(), "归属 Actor 应收到");
        assert!(rx2.try_recv().is_err(), "非归属 Actor 不应收到");
    }

    #[tokio::test]
    async fn unknown_order_fans_out_only_to_same_strategy_type() {
        let (h1, mut rx1) = pair();
        let (h2, mut rx2) = pair();
        let (h3, mut rx3) = pair();
        let mut router = OrderRouter::new(vec![
            ("sar_snt3".into(), true, h1.clone()),
            ("sar_snt3".into(), true, h2.clone()), // 同类型不同符号
            ("sar_supertrend".into(), true, h3.clone()),
        ]);
        router
            .dispatch_update(update("ord_ghost", Some("sar_snt3")))
            .await;
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
        assert!(rx3.try_recv().is_err(), "不同策略类型不应收到");
    }

    #[tokio::test]
    async fn unknown_order_without_identity_fans_out_all() {
        let (h1, mut rx1) = pair();
        let (h2, mut rx2) = pair();
        let mut router = OrderRouter::new(vec![
            ("sar_snt3".into(), true, h1.clone()),
            ("sar_supertrend".into(), true, h2.clone()),
        ]);
        router.dispatch_update(update("ord_x", None)).await;
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[tokio::test]
    async fn reconnect_only_notifies_matching_strategy_type() {
        let (h1, mut rx1) = pair();
        let (h2, mut rx2) = pair();
        let router = OrderRouter::new(vec![
            ("sar_snt3".into(), true, h1),
            ("sar_supertrend".into(), true, h2),
        ]);
        router.dispatch_reconnect("sar_snt3").await;
        assert!(matches!(rx1.try_recv(), Ok(RuntimeCommand::Reconcile)));
        assert!(rx2.try_recv().is_err(), "非重连策略类型不应对账");
    }

    /// U12：重连只通知 live 实例；paper 不得收到 Reconcile
    #[tokio::test]
    async fn reconnect_skips_paper_instances() {
        let (h1, mut rx_live) = pair();
        let (h2, mut rx_paper) = pair();
        let router = OrderRouter::new(vec![
            ("sar_supertrend".into(), true, h1),  // BTCUSDT live
            ("sar_supertrend".into(), false, h2), // WLDUSDT paper
        ]);
        router.dispatch_reconnect("sar_supertrend").await;
        assert!(
            matches!(rx_live.try_recv(), Ok(RuntimeCommand::Reconcile)),
            "live 实例应收到对账通知"
        );
        assert!(rx_paper.try_recv().is_err(), "paper 实例不应被重连波及");
    }
}
