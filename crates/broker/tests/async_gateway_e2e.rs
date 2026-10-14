//! 集成测试 —— runtime 内真单链路（真实 SouthCommand/ClientEvent 协议）：
//! SignalManager 同步内核 + AsyncOrderGateway → 真实 trading-service WS →
//! ack(NEW)/order_event(FILLED)。本仓库不内置 mock 服务实现——对端是外部
//! 服务进程（trading_service_rs 的 trading-service + exchange-mock）。
//!
//! 门控：未设置环境变量 `TRADING_WS_URL` 的用例直接跳过（打印说明并 return）；
//! 冷启动对账用例不发单不建连，无需外部服务，常开。
//! 注意：真实服务按 strategy_name 路由回调和订单事件——并行用例必须各自
//! 使用互不相同的策略名（旧内嵌 mock 每测试一实例，无此约束）。
//!
//! 运行（先按 docs/RUNBOOK.md §2 起 trading-service + exchange-mock）：
//!   TRADING_WS_URL=ws://127.0.0.1:9100/ws \
//!     cargo test -p strategy-broker --test async_gateway_e2e -- --nocapture
//!
//! 覆盖：
//! 1. intent → ack → order_event(filled) → FillEvent（订单号由服务端生成）
//! 2. 平仓 intent（方向已反向）同链路成交
//! 3. submit 后对账：已 ack 在途单 query 得 Retryable，随后 order_event 补推成交
//!    （要求 exchange-mock 成交间隔 ≥500ms，保证对账先于成交发生）
//! 4. 冷启动 Submitted 在途：query Retryable，状态保留，不伪造成交

use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use strategy_broker::{
    ClientEvent, OrderVenueCfg, TradingConfig, TradingGateway, TradingServiceClient,
};
use strategy_core::{FillEvent, IntentKind, Side, SignalIntent, SignalStatus};
use strategy_signals::gateway::{AsyncOrderGateway, FailureKind};
use strategy_signals::manager::RecoveryAction;
use strategy_signals::{ManagedSignal, SignalManager};
use strategy_store::{Clock, Store};
use tokio::sync::mpsc;

const FIXED_MS: i64 = 1_781_049_600_000;
const SECRET: &str = "e2e-secret";

/// 外部服务门控：未设置 TRADING_WS_URL 时返回 None（用例跳过）。
/// 真实服务尚未强制 HMAC，secret 任意值可连（见 docs/RUNBOOK.md §3）。
fn external_url() -> Option<String> {
    match std::env::var("TRADING_WS_URL") {
        Ok(url) if !url.is_empty() => Some(url),
        _ => {
            eprintln!(
                "跳过：未设置 TRADING_WS_URL（需先启动 trading-service + exchange-mock，见 docs/RUNBOOK.md §2）"
            );
            None
        }
    }
}

struct FixedClock(AtomicI64);
impl Clock for FixedClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn intent(strategy: &str, id: &str, side: Side, kind: IntentKind) -> SignalIntent {
    SignalIntent {
        id: id.into(),
        strategy_id: strategy.into(),
        // instrument 与 exchange-mock 注册名一致（小写，同 trading_service_smoke）
        symbol: "btcusdt".into(),
        kind,
        direction: side,
        price: 100.0,
        size: 1.0,
        size_raw: None,
        stop_price: None,
        // Close 帧必须携带 position_id（组帧判定依据）
        position_id: (kind == IntentKind::Close).then(|| "e2e-position".into()),
        stop_order_id: None,
        created_event_time: Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap(),
        status: SignalStatus::Created,
    }
}

struct Driver {
    manager: SignalManager,
    gateway: TradingGateway,
    events: mpsc::Receiver<ClientEvent>,
    fill_tx: mpsc::Sender<FillEvent>,
    fill_rx: mpsc::Receiver<FillEvent>,
}

impl Driver {
    async fn submit_intent(&mut self, intent: SignalIntent) -> String {
        let Some(req) = self.manager.receive_intent_prepare(intent).unwrap() else {
            panic!("prepare 不应过滤该 intent");
        };
        let now = req.event_time;
        let outcome = self.gateway.submit_fut(&req.intent).await;
        let order_id = outcome.expect("ack 应成功");
        self.manager
            .apply_submit_outcome(req, Ok(order_id.clone()))
            .unwrap();
        for rf in self.gateway.drain_ready_fills() {
            if let Some(fill) = self.manager.apply_ready_fill(rf, now).unwrap() {
                self.fill_tx.send(fill).await.ok();
            }
        }
        order_id
    }

    async fn handle_client_event(&mut self, ev: ClientEvent) {
        match ev {
            ClientEvent::OrderUpdate(u) => {
                if let Some(fill) = self
                    .manager
                    .handle_order_update(
                        &u.order_id,
                        u.exchange_order_id.as_deref(),
                        u.state,
                        u.filled_size,
                        u.avg_price,
                        u.filled_quantity_raw,
                        u.signal_id.as_deref(),
                        u.message.as_deref(),
                        Utc::now(),
                    )
                    .unwrap()
                {
                    self.fill_tx.send(fill).await.ok();
                }
            }
            ClientEvent::Reconnected { .. } => self.reconcile().await,
        }
    }

    /// 真实对账：Resubmit 幂等重提；Query 得 Retryable 保留（不伪造成交）。
    async fn reconcile(&mut self) {
        let now = Utc::now();
        let actions = self.manager.recover().unwrap();
        for action in actions {
            match action {
                RecoveryAction::Resubmit { signal_id } => {
                    let req = self.manager.submit_request(&signal_id, now);
                    let outcome = self.gateway.submit_fut(&req.intent).await;
                    self.manager.apply_submit_outcome(req, outcome).unwrap();
                }
                RecoveryAction::Query { signal_id, .. } => {
                    let err = self.gateway.query_fut(&signal_id).await.unwrap_err();
                    assert_eq!(
                        err.kind,
                        FailureKind::Retryable,
                        "无应答必须 Retryable 保留"
                    );
                }
            }
        }
    }

    async fn next_fill(&mut self, label: &str) -> FillEvent {
        tokio::time::timeout(Duration::from_secs(15), self.fill_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("等待成交超时：{label}"))
            .expect("fill 通道不应关闭")
    }
}

fn spawn_driver(url: String, dir: &Path, strategy: &str) -> Driver {
    let (client, events) = TradingServiceClient::start_arc(
        TradingConfig {
            ws_url: url,
            submit_enabled: true,
            request_timeout: Duration::from_secs(10),
            strategy_secret: SECRET.into(),
            reconnect_backoff_max_secs: 8,
        },
        tokio::runtime::Handle::current(),
    );
    let store = Arc::new(Store::new(
        dir,
        Arc::new(FixedClock(AtomicI64::new(FIXED_MS))),
    ));
    let manager = SignalManager::new("st", store, None).with_flush_every(usize::MAX);
    let venue = OrderVenueCfg {
        exchange: "binance".into(),
        account: "test_binance".into(),
        leverage: 1,
        order_cash: 1.0,
    };
    let gateway = TradingGateway::new(client, strategy.into(), venue);
    let (fill_tx, fill_rx) = mpsc::channel(8);
    Driver {
        manager,
        gateway,
        events,
        fill_tx,
        fill_rx,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_intent_ack_then_filled_event_produces_fill() {
    let Some(url) = external_url() else { return };
    let dir = tempfile::tempdir().unwrap();
    let mut drv = spawn_driver(url, dir.path(), "e2e_open");

    let order_id = drv
        .submit_intent(intent(
            "e2e_open",
            "sig_open_1",
            Side::Long,
            IntentKind::Open,
        ))
        .await;
    assert!(order_id.starts_with("0x"), "服务端订单号应为 0x 开头");

    // 先吃掉建连时的 Reconnected（submit 触发了懒建连）。
    let connected = tokio::time::timeout(Duration::from_secs(10), drv.events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(connected, ClientEvent::Reconnected { .. }));

    let ev = tokio::time::timeout(Duration::from_secs(10), drv.events.recv())
        .await
        .expect("等待 order_event 超时")
        .unwrap();
    drv.handle_client_event(ev).await;

    let fill = drv.next_fill("开仓").await;
    assert_eq!(fill.signal_id, "sig_open_1");
    assert_eq!(fill.order_id, order_id);
    assert!((fill.filled_size - 1.0).abs() < 1e-9);
    assert!(
        fill.filled_price > 0.0,
        "成交价由 exchange-mock 决定，应为正数"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_intent_uses_same_place_order_path_and_fills() {
    let Some(url) = external_url() else { return };
    let dir = tempfile::tempdir().unwrap();
    let mut drv = spawn_driver(url, dir.path(), "e2e_close");

    // 平多：direction=Short，与开仓同一 place_order 帧，仅 side 不同。
    let order_id = drv
        .submit_intent(intent(
            "e2e_close",
            "sig_close_1",
            Side::Short,
            IntentKind::Close,
        ))
        .await;

    // 先 drain Reconnected。
    let _connected = tokio::time::timeout(Duration::from_secs(10), drv.events.recv())
        .await
        .unwrap()
        .unwrap();

    let ev = tokio::time::timeout(Duration::from_secs(10), drv.events.recv())
        .await
        .unwrap()
        .unwrap();
    drv.handle_client_event(ev).await;

    let fill = drv.next_fill("平仓").await;
    assert_eq!(fill.signal_id, "sig_close_1");
    assert_eq!(fill.order_id, order_id);
    assert_eq!(fill.direction, Side::Short);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_inflight_survives_reconcile_until_fill_pushed() {
    // 依赖 exchange-mock 成交节奏（--order_fill_interval ≥500ms）：
    // submit 返回 ack 时信号已是 Submitted；此时 reconcile → Query → Retryable
    // （不伪造、不乱改）；随后 FILLED 补推。
    let Some(url) = external_url() else { return };
    let dir = tempfile::tempdir().unwrap();
    let mut drv = spawn_driver(url, dir.path(), "e2e_rec");

    let order_id = drv
        .submit_intent(intent("e2e_rec", "sig_rec_1", Side::Long, IntentKind::Open))
        .await;

    // 先 drain Reconnected（不调 handle_client_event，避免提前 reconcile）。
    let _connected = tokio::time::timeout(Duration::from_secs(10), drv.events.recv())
        .await
        .unwrap()
        .unwrap();

    drv.reconcile().await; // 不得产生 fill
    let premature = tokio::time::timeout(Duration::from_millis(150), drv.fill_rx.recv()).await;
    assert!(premature.is_err(), "对账不得伪造成交");

    let ev = tokio::time::timeout(Duration::from_secs(15), drv.events.recv())
        .await
        .unwrap()
        .unwrap();
    drv.handle_client_event(ev).await;
    let fill = drv.next_fill("延迟成交").await;
    assert_eq!(fill.order_id, order_id);
    assert_eq!(fill.signal_id, "sig_rec_1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_start_submitted_inflight_is_kept_not_fabricated() {
    // 无需外部服务：不发单、不建连（Query Retryable 为本地 fail-closed 语义）。
    let dir = tempfile::tempdir().unwrap();
    {
        let seed = Arc::new(Store::new(
            dir.path(),
            Arc::new(FixedClock(AtomicI64::new(FIXED_MS))),
        ));
        seed.table::<ManagedSignal>()
            .insert(ManagedSignal {
                strategy_name: "st".into(),
                intent: intent("e2e_boot", "sig_boot_1", Side::Long, IntentKind::Open),
                version: 3,
                status: SignalStatus::Submitted,
                order_id: Some("0xboot".into()),
                attempts: 1,
                next_retry_at: None,
                last_error: None,
                last_failure_kind: None,
                exchange_order_id: None,
                filled_price: None,
                filled_quantity: None,
                filled_quantity_raw: None,
                order_type: None,
                order_price: None,
                order_quantity: None,
            })
            .unwrap();
        seed.flush().unwrap();
    }

    let mut drv = spawn_driver("ws://127.0.0.1:1".into(), dir.path(), "e2e_boot");
    drv.reconcile().await; // Query Retryable：保留在途（无 submit → 不建连）
    let no_fill = tokio::time::timeout(Duration::from_millis(300), drv.fill_rx.recv()).await;
    assert!(no_fill.is_err(), "冷启动不允许把在途单伪造成交");
}
