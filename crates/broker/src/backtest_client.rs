//! BacktestOrderClient —— 回测用 OrderGateway
//!
//! 包装 [`crate::backtest::BacktestBroker`] 撮合内核（滑点/手续费/全仓语义/
//! exchange_orders.csv 回执全部不变）。可 Clone（共享 `Arc<Mutex<内核>>`）：
//! 引擎与 SignalManager 各持一份——前者读账户快照做权益曲线，后者经
//! [`OrderGateway`] 端口下单并取出同步就绪的成交。
//!
//! 回测中订单恒为即时全量成交：submit 成功后成交进入就绪队列，
//! SignalManager 用 `drain_ready_fills` 取出并驱动 Filled 状态与仓位管理。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use strategy_core::signal::{Action, SignalIntent};
use strategy_core::OrderState;
use strategy_signals::gateway::{
    AsyncOrderGateway, OrderGateway, OrderSnapshot, PositionsFuture, QueryFuture, ReadyFill,
    RemotePosition, SubmitError, SubmitFuture,
};

use crate::backtest::{BacktestBroker, Fill};
use crate::{AccountSnapshot, Broker};

#[derive(Default)]
struct Inner {
    broker: Option<BacktestBroker>,
    /// order_id → 成交（query_order）
    by_order: HashMap<String, ReadyFill>,
    /// 就绪成交 FIFO
    ready: VecDeque<ReadyFill>,
}

#[derive(Clone)]
pub struct BacktestOrderClient {
    inner: Arc<Mutex<Inner>>,
}

impl BacktestOrderClient {
    pub fn new(broker: BacktestBroker) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                broker: Some(broker),
                by_order: HashMap::new(),
                ready: VecDeque::new(),
            })),
        }
    }

    /// 账户快照（权益曲线；实盘适配器返回 None）
    pub fn account(&self) -> Option<AccountSnapshot> {
        self.inner.lock().unwrap().broker.as_ref()?.account()
    }

    fn intent_to_action(intent: &SignalIntent) -> Action {
        match intent.kind {
            strategy_core::IntentKind::Open | strategy_core::IntentKind::Reverse => {
                // Reverse 在意图层已拆成先平后开两条，这里各自映射
                Action::Open {
                    side: intent.direction,
                    price: intent.price,
                    stop_price: intent.stop_price.unwrap_or(0.0),
                    atr: 0.0,
                    strength: 1.0,
                    reason: String::new(),
                }
            }
            strategy_core::IntentKind::Close => Action::Close {
                price: intent.price,
                reason: String::new(),
                // 止损/止盈触发的平仓带 stop_order_id
                is_stop_loss: intent.stop_order_id.is_some(),
                strength: 1.0,
            },
        }
    }
}

impl OrderGateway for BacktestOrderClient {
    fn submit(&mut self, intent: &SignalIntent) -> Result<String, SubmitError> {
        let mut g = self.inner.lock().unwrap();
        let broker = g
            .broker
            .as_mut()
            .ok_or_else(|| SubmitError::rejected("撮合内核不可用"))?;
        let action = Self::intent_to_action(intent);
        let result = broker
            .execute(&intent.id, &action, intent.created_event_time)
            .map_err(|e| SubmitError::rejected(format!("回测撮合失败: {e}")))?;
        if result.fills.is_empty() {
            // 已有持仓再开 / 无仓可平：内核忽略，按永久拒绝记录（信号 Failed 可审计）
            return Err(SubmitError::rejected("撮合未产生成交（仓位状态不允许）"));
        }
        record_fills(&mut g, &result.fills);
        Ok(format!("bt_{}", intent.id))
    }

    fn query_order(&mut self, order_id: &str) -> Result<OrderSnapshot, SubmitError> {
        let g = self.inner.lock().unwrap();
        g.by_order
            .get(order_id)
            .map(|f| OrderSnapshot {
                order_id: f.order_id.clone(),
                state: OrderState::Filled,
                filled_size: f.filled_size,
                avg_price: f.avg_price,
                exchange_order_id: None,
                filled_quantity_raw: None,
                place: None,
            })
            .ok_or_else(|| SubmitError::rejected(format!("回测无此订单: {order_id}")))
    }

    fn drain_ready_fills(&mut self) -> Vec<ReadyFill> {
        self.inner.lock().unwrap().ready.drain(..).collect()
    }
}

/// live Actor 装配（集成测试/paper 本地撮合）下的 async 端口：
/// 本地同步撮合是微秒级 CPU 操作，直接在 future 内返回，无 block_on。
impl AsyncOrderGateway for BacktestOrderClient {
    fn submit_fut<'a>(&'a mut self, intent: &'a SignalIntent) -> SubmitFuture<'a> {
        let r = OrderGateway::submit(self, intent);
        Box::pin(async move { r })
    }
    fn query_fut<'a>(&'a mut self, signal_id: &'a str) -> QueryFuture<'a> {
        // 回测订单号为 bt_{signal_id}：入参语义与 live 同为 signal_id
        let r = OrderGateway::query_order(self, &format!("bt_{signal_id}"));
        Box::pin(async move { r })
    }
    fn positions_fut<'a>(
        &'a mut self,
        _instrument: &'a str,
        _side: &'a str,
    ) -> PositionsFuture<'a> {
        // 本地撮合不涉远程仓位对账：空 Vec（无远程仓）
        Box::pin(async move { Ok(Vec::<RemotePosition>::new()) })
    }
    fn drain_ready_fills(&mut self) -> Vec<ReadyFill> {
        OrderGateway::drain_ready_fills(self)
    }
}

fn record_fills(g: &mut Inner, fills: &[Fill]) {
    for f in fills {
        let order_id = format!("bt_{}", f.signal_id);
        let ready = ReadyFill {
            order_id: order_id.clone(),
            filled_size: f.quantity,
            avg_price: f.fill_price,
            fee: f.fee,
        };
        g.by_order.insert(order_id, ready.clone());
        g.ready.push_back(ready);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backtest::BacktestConfig;
    use chrono::TimeZone;
    use strategy_core::{IntentKind, Side, SignalIntent, SignalStatus};
    use strategy_signals::gateway::FailureKind;

    fn t() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
    }

    fn intent(kind: IntentKind, side: Side, price: f64) -> SignalIntent {
        let id = format!("sig_{:?}_{:?}", kind, side).to_lowercase();
        SignalIntent {
            id,
            strategy_id: "sar_snt3".into(),
            symbol: "BTCUSDT".into(),
            kind,
            direction: side,
            price,
            size: 0.0,
            size_raw: None,
            stop_price: None,
            position_id: None,
            stop_order_id: None,
            created_event_time: t(),
            status: SignalStatus::Created,
        }
    }

    fn client(dir: &std::path::Path) -> BacktestOrderClient {
        let cfg = BacktestConfig {
            cash: 1000.0,
            leverage: 1,
            notional_per_order: 0.0,
            ..Default::default()
        };
        BacktestOrderClient::new(BacktestBroker::new(cfg, "st", dir))
    }

    #[test]
    fn open_immediately_fills_and_is_queryable() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = client(dir.path());
        let oid = c
            .submit(&intent(IntentKind::Open, Side::Long, 100.0))
            .unwrap();
        assert_eq!(oid, "bt_sig_open_long");

        let fills = OrderGateway::drain_ready_fills(&mut c);
        assert_eq!(fills.len(), 1);
        assert!(fills[0].filled_size > 0.0);

        let snap = c.query_order(&oid).unwrap();
        assert_eq!(snap.state, OrderState::Filled);
        assert_eq!(snap.filled_size, fills[0].filled_size);
        assert!(c.account().is_some());
    }

    #[test]
    fn open_while_position_exists_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = client(dir.path());
        c.submit(&intent(IntentKind::Open, Side::Long, 100.0))
            .unwrap();
        OrderGateway::drain_ready_fills(&mut c);
        let err = c
            .submit(&intent(IntentKind::Open, Side::Long, 101.0))
            .unwrap_err();
        assert_eq!(err.kind, FailureKind::Rejected);
    }

    #[test]
    fn close_without_position_is_rejected_then_reopen_works() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = client(dir.path());
        assert!(c
            .submit(&intent(IntentKind::Close, Side::Short, 100.0))
            .is_err());
        assert!(c
            .submit(&intent(IntentKind::Open, Side::Long, 100.0))
            .is_ok());
    }

    #[test]
    fn shared_clone_sees_same_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = client(dir.path());
        let clone = c.clone();
        c.submit(&intent(IntentKind::Open, Side::Long, 100.0))
            .unwrap();
        // 另一个句柄能查到同一笔成交（管理器/引擎共享内核）
        let mut clone = clone;
        assert!(clone.query_order("bt_sig_open_long").is_ok());
    }
}
