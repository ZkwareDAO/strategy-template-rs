//! 阶段 4（v2.7，§13-4）帧级契约测试：进程内脚本 WS（模式复刻
//! `src/trading_service.rs` 内嵌 `spawn_reply_server`），承重三件事：
//!
//! 1. **实盘订单帧带平今/平昨**：`SignalIntent.offset` 显式时，
//!    place_order 帧出现 `offset` 字段（`close_today`/`open`），且
//!    平仓帧形不再依赖 `position_id` 隐式判定（§17 broker 行「隐式开平
//!    判定退役、改消费 leg.offset」的帧侧证据）；
//! 2. **旧链路零回归**：`offset=None` 的意图出帧**不含** offset 键、
//!    字段集与升级前逐字节一致（§14-3 外部协议升级前向兼容）；
//! 3. **FillEvent.fee 数据通道**：`order_event`/`order` 帧回传 `fee` →
//!    `OrderUpdate.fee` / `OrderSnapshot.fee`（signals 内核直通已由
//!    `manager::order_update_fee_lands_in_fill_event…` 承重）。

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use strategy_broker::{
    ClientEvent, OrderVenueCfg, TradingConfig, TradingGateway, TradingServiceClient,
};
use strategy_core::{IntentKind, Side, SignalIntent, SignalStatus};
use strategy_signals::gateway::AsyncOrderGateway;
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// 脚本服务：捕获命令帧；place_order 回 ack(NEW)，随后追发
/// `post_place` 帧（FILLED 带 fee）；get_order 回 `order_reply`。
async fn spawn_service(
    post_place: Option<Value>,
    order_reply: Option<Value>,
) -> (String, Arc<Mutex<Vec<Value>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());
    let got = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&got);
    tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(tcp).await.unwrap();
        while let Some(Ok(msg)) = ws.next().await {
            let Message::Text(t) = msg else { continue };
            let Ok(v) = serde_json::from_str::<Value>(&t) else {
                continue;
            };
            let Some(opt) = v.get("option").and_then(|o| o.as_str()).map(String::from) else {
                continue;
            };
            captured.lock().unwrap().push(v.clone());
            match opt.as_str() {
                "place_order" => {
                    let sid = v["signal_id"].as_str().unwrap_or_default().to_string();
                    let ack = json!({
                        "type": "ack", "status": "NEW",
                        "client_order_id": "0xfeed", "signal_id": sid,
                    });
                    ws.send(Message::Text(ack.to_string())).await.unwrap();
                    if let Some(f) = &post_place {
                        ws.send(Message::Text(f.to_string())).await.unwrap();
                    }
                }
                "get_order" => {
                    if let Some(f) = &order_reply {
                        ws.send(Message::Text(f.to_string())).await.unwrap();
                    }
                }
                _ => {}
            }
        }
    });
    (url, got)
}

fn cfg_for(url: String) -> TradingConfig {
    TradingConfig {
        ws_url: url,
        request_timeout: std::time::Duration::from_secs(3),
        submit_enabled: true,
        reconnect_backoff_max_secs: 1,
        strategy_secret: "test".into(),
    }
}

fn venue() -> OrderVenueCfg {
    OrderVenueCfg {
        exchange: "ctp".into(),
        account: "acc".into(),
        leverage: 1,
        order_cash: 100_000.0,
    }
}

fn intent(
    id: &str,
    side: Side,
    kind: IntentKind,
    offset: Option<&str>,
    with_position: bool,
    size: f64,
    size_raw: Option<&str>,
) -> SignalIntent {
    SignalIntent {
        id: id.into(),
        strategy_id: "sar_test".into(),
        symbol: "CU2601".into(),
        kind,
        direction: side,
        price: 51_000.0,
        size,
        size_raw: size_raw.map(String::from),
        stop_price: None,
        position_id: with_position.then(|| "lot_1".to_string()),
        stop_order_id: None,
        created_event_time: chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 1, 5, 2, 0, 0)
            .unwrap(),
        status: SignalStatus::Created,
        offset: offset.map(String::from),
    }
}

/// 验收 1：平今意图 → 帧带 `offset:"close_today"`，数量逐字取 raw；
/// FILLED 回传 fee → OrderUpdate.fee 真值（验收 2 的帧侧）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_today_frame_carries_offset_and_fee_returns() {
    let filled = json!({
        "type": "order_event",
        "status": "FILLED",
        "client_order_id": "0xfeed",
        "signal_id": "sig_ct",
        "filled_price": 51_000.0,
        "filled_quantity": "2",
        "fee": 25.5,
    });
    let (url, got) = spawn_service(Some(filled), None).await;
    let (client, mut events) = TradingServiceClient::start_arc(cfg_for(url), Handle::current());
    let mut gw = TradingGateway::new(client, "sar_test".into(), venue());

    let order_id = gw
        .submit_fut(&intent(
            "sig_ct",
            Side::Short,
            IntentKind::Close,
            Some("close_today"),
            true,
            2.0,
            Some("2"),
        ))
        .await
        .expect("ack 应成功");
    assert_eq!(order_id, "0xfeed");

    let frame = {
        let g = got.lock().unwrap();
        g.first().expect("服务端应收到 place_order").clone()
    };
    assert_eq!(frame["option"], "place_order");
    assert_eq!(frame["offset"], "close_today", "验收 1：平今落帧");
    assert_eq!(frame["quantity"], "2", "数量=回报字面量");
    assert_eq!(frame["reduce_only"], true);
    assert_eq!(frame["side"], "short");

    // FILLED 回报：fee 真值进入 OrderUpdate（下游 FillEvent/Fill 直通已测）
    let mut saw_fill = false;
    for _ in 0..200 {
        let Ok(ev) =
            tokio::time::timeout(std::time::Duration::from_millis(50), events.recv()).await
        else {
            continue;
        };
        let Some(ClientEvent::OrderUpdate(u)) = ev else {
            continue;
        };
        if u.state == strategy_core::order::OrderState::Filled {
            assert_eq!(u.fee, Some(25.5), "验收 2：fee 数据通道打通");
            assert_eq!(u.filled_quantity_raw.as_deref(), Some("2"));
            saw_fill = true;
            break;
        }
    }
    assert!(saw_fill, "应消费到 FILLED 回报");
}

/// 零回归钉：offset=None 的旧链路意图——出帧**不含** offset 键，
/// 字段集合与升级前十键完全一致（§14-3 前置升级的兼容底线）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_intent_frame_has_no_offset_key_and_old_field_set() {
    let (url, got) = spawn_service(None, None).await;
    let (client, _events) = TradingServiceClient::start_arc(cfg_for(url), Handle::current());
    let mut gw = TradingGateway::new(client, "sar_test".into(), venue());

    gw.submit_fut(&intent(
        "sig_old",
        Side::Long,
        IntentKind::Open,
        None,
        false,
        0.0,
        None,
    ))
    .await
    .unwrap();

    let frame = got.lock().unwrap().first().cloned().expect("应收到帧");
    assert!(
        frame.get("offset").is_none(),
        "旧意图帧内不得出现 offset 键"
    );
    let mut keys: Vec<&str> = frame
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "account",
            "exchange",
            "instrument",
            "instrument_type",
            "leverage",
            "option",
            "order_cash",
            "order_type",
            "side",
            "signal_id",
        ],
        "字段集合 = 升级前十键（逐字节兼容面）"
    );
}

/// 帧形由 offset 驱动（§17「隐式开平判定退役」的桥路径）：
/// `offset:"open"` 且无 position_id → 开仓帧（金额路径不变，§13 过渡）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_drives_frame_shape_not_position_guess() {
    let (url, got) = spawn_service(None, None).await;
    let (client, _events) = TradingServiceClient::start_arc(cfg_for(url), Handle::current());
    let mut gw = TradingGateway::new(client, "sar_test".into(), venue());
    // 开仓：显式 open + size 0 + 无 position → 金额帧
    gw.submit_fut(&intent(
        "sig_o2",
        Side::Long,
        IntentKind::Open,
        Some("open"),
        false,
        0.0,
        None,
    ))
    .await
    .unwrap();
    {
        let g = got.lock().unwrap();
        let f = g.first().unwrap();
        assert_eq!(f["offset"], "open");
        assert!(f.get("order_cash").is_some());
        assert!(f.get("quantity").is_none());
    }
}

/// get_order 应答带 fee → OrderSnapshot.fee（重启/看门狗对账路径的
/// fee 通道；旧 runtime_actor 交叉对账的 FillEvent 构造已接 snap.fee——该模块已随
/// R4 退役删除，现宿主为 `app::spi_live_actor`（交叉对账段）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_order_snapshot_carries_fee() {
    let reply = json!({
        "type": "order",
        "status": "FILLED",
        "client_order_id": "0xfeed",
        "signal_id": "sig_q",
        "filled_price": 50_500.0,
        "filled_quantity": 3,
        "fee": "8.05",
    });
    let (url, _got) = spawn_service(None, Some(reply)).await;
    let (client, _events) = TradingServiceClient::start_arc(cfg_for(url), Handle::current());
    let mut gw = TradingGateway::new(client, "sar_test".into(), venue());
    let snap = gw.query_fut("sig_q").await.expect("order 应答");
    assert_eq!(snap.fee, Some(8.05), "数字字符串 fee 双口径");
    assert_eq!(snap.filled_quantity_raw.as_deref(), Some("3"));
}
