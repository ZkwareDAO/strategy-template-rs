//! # broker::mock —— 支持协议 v2（order_cash）的本地 trading-service 模拟器
//!
//! 联调/测试组件，在 127.0.0.1 随机端口起一个与 trading-service 南向协议
//! 兼容的 WS 服务，**不修改外部 trading_service_rs 仓库**：
//! - 升级前 HMAC 握手（strategy_name + nonce + signature，nonce 防重放，恒定
//!   时间比较）；失败直接 401，不升级、不注册、不 spawn；
//! - place_order 两种帧形：
//!   · 开仓帧（`order_cash` + `leverage`，无 quantity）：mock 按参考价
//!   （[`MockScenario::fill_price`]）换算 `quantity=order_cash/参考价`，floor 到 1e-6；
//!   · 平仓帧（`quantity` + `reduce_only`）：数量**逐字采用**，不重算；
//! - 服务端生成 client_order_id（0x+32hex）与 exchange_order_id；先回
//!   ack(NEW)（完整字段、signal_id 回声、side=BUY/SELL），再按节奏推 PARTIAL…FILLED；
//!   同 signal_id 重发幂等回同一单；
//! - cancel_order / get_position 同真实服务语义。
//!
//! **非生产服务，禁止在 live 装配中引用。** 另提供 [`spawn_raw`]（不鉴权）
//! 供坏帧/异常时序单测。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures_util::lock::Mutex;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, accept_hdr_async, WebSocketStream};

use crate::auth;
use crate::wire;

/// 服务端侧一条 WS 连接（裸脚本的入参）。
pub type MockConn = WebSocketStream<TcpStream>;

type ScriptFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// 裸脚本：拿到 WS 流自由收/发帧，返回即连接结束。
pub trait ConnectionScript: Send + Sync + 'static {
    fn run(&self, ws: MockConn) -> ScriptFuture;
}

impl<F, Fut> ConnectionScript for F
where
    F: Fn(MockConn) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn run(&self, ws: MockConn) -> ScriptFuture {
        Box::pin(self(ws))
    }
}

impl<T: ConnectionScript + ?Sized> ConnectionScript for Arc<T> {
    fn run(&self, ws: MockConn) -> ScriptFuture {
        (**self).run(ws)
    }
}

/// 鉴权/成交场景配置。
#[derive(Debug, Clone)]
pub struct MockScenario {
    pub secret_key: String,
    /// FILLED 前回报段数（1=直接 FILLED；3=PARTIAL,PARTIAL,FILLED）
    pub fill_times: u32,
    /// 每段回报间隔
    pub fill_interval: Duration,
    /// 模拟市价（成交价 filled_price；也是开仓 order_cash 换算的参考价）
    pub fill_price: f64,
    /// 按 instrument 覆盖参考价（多币种共用一个 mock 时各自的成交/换算价）；
    /// 未命中的 instrument 回落 `fill_price`
    pub symbol_prices: std::collections::HashMap<String, f64>,
}

impl MockScenario {
    pub fn new(secret_key: impl Into<String>) -> Self {
        Self {
            secret_key: secret_key.into(),
            fill_times: 1,
            fill_interval: Duration::ZERO,
            fill_price: 100.5,
            symbol_prices: std::collections::HashMap::new(),
        }
    }
    pub fn fill_times(mut self, n: u32) -> Self {
        self.fill_times = n.max(1);
        self
    }
    pub fn fill_interval(mut self, d: Duration) -> Self {
        self.fill_interval = d;
        self
    }
    pub fn fill_price(mut self, p: f64) -> Self {
        self.fill_price = p;
        self
    }
    /// 为某个 instrument 指定参考价/成交价（可链式多次调用）。
    pub fn symbol_price(mut self, instrument: impl Into<String>, p: f64) -> Self {
        self.symbol_prices.insert(instrument.into(), p);
        self
    }
}

type SharedSink = Arc<Mutex<SplitSink<MockConn, Message>>>;

struct HubEntry {
    sink: SharedSink,
    token: u64,
}

/// strategy_name → 当前连接写端（token 防旧连接误摘新连接）。
#[derive(Default)]
struct Hub {
    clients: DashMap<String, HubEntry>,
    next_token: AtomicU64,
}

impl Hub {
    fn register(&self, name: String, sink: SharedSink) -> u64 {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        self.clients.insert(name, HubEntry { sink, token });
        token
    }

    fn unregister_if(&self, name: &str, token: u64) {
        self.clients.remove_if(name, |_, e| e.token == token);
    }

    /// 向策略连接写一帧；无连接/写失败返回 false（失败按 token 摘除）。
    async fn send(&self, name: &str, text: String) -> bool {
        // 先克隆需要的部分再释放 dashmap 守卫（守卫非 Send，不能跨 await）。
        let (sink, token) = match self.clients.get(name) {
            Some(g) => (g.sink.clone(), g.token),
            None => return false,
        };
        let result = sink.lock().await.send(Message::Text(text)).await;
        match result {
            Ok(()) => true,
            Err(_) => {
                self.clients.remove_if(name, |_, e| e.token == token);
                false
            }
        }
    }
}

/// mock 内存订单（帧内字段原样回声）。
#[derive(Debug, Clone)]
struct OrderRec {
    cid: String,
    exchange_order_id: String,
    exchange: String,
    instrument: String,
    /// long / short（回声帧转 BUY / SELL）
    side: String,
    order_type: String,
    quantity: f64,
    reduce_only: bool,
    /// 帧内 leverage（仅留痕/测试核对；mock 不真正下单）
    #[allow(dead_code)]
    leverage: u64,
    status: String,
    /// 该单成交价（按 instrument 参考价；幂等快照原样回声）
    fill_price: f64,
}

struct MockState {
    secret: String,
    fill_times: u32,
    fill_interval: Duration,
    fill_price: f64,
    symbol_prices: std::collections::HashMap<String, f64>,
    hub: Hub,
    /// nonce → 校验时刻（TTL 5min，惰性清理）
    nonces: DashMap<String, Instant>,
    /// client_order_id → 内存订单
    orders: DashMap<String, OrderRec>,
    signal_to_order: DashMap<String, String>,
    exchange_seq: AtomicU64,
}

impl MockState {
    fn new(s: MockScenario) -> Self {
        Self {
            secret: s.secret_key,
            fill_times: s.fill_times.max(1),
            fill_interval: s.fill_interval,
            fill_price: s.fill_price,
            symbol_prices: s.symbol_prices,
            hub: Hub::default(),
            nonces: DashMap::new(),
            orders: DashMap::new(),
            signal_to_order: DashMap::new(),
            exchange_seq: AtomicU64::new(1),
        }
    }

    /// 某 instrument 的成交/换算参考价：per-symbol 覆盖 > 全局 fill_price。
    fn reference_price(&self, instrument: &str) -> f64 {
        self.symbol_prices
            .get(instrument)
            .copied()
            .unwrap_or(self.fill_price)
    }
}

/// 绑定随机本地端口，返回（url, listener）。
async fn bind_ephemeral() -> (String, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock 端口");
    let port = listener.local_addr().unwrap().port();
    (format!("ws://127.0.0.1:{port}/ws"), listener)
}

/// 启动鉴权 mock trading-service（主入口）。
#[allow(clippy::result_large_err)] // tungstenite accept_hdr_async 回调签名固定
pub async fn spawn(scenario: MockScenario) -> (String, JoinHandle<()>) {
    let (url, listener) = bind_ephemeral().await;
    let state = Arc::new(MockState::new(scenario));
    let handle = tokio::spawn(run_accept_loop(listener, state));
    (url, handle)
}

/// 独立 mock 服务：绑定**指定地址**（固定端口）并常驻，供策略联调进程使用。
pub struct MockServer {
    listener: TcpListener,
    state: Arc<MockState>,
}

impl MockServer {
    pub async fn bind(addr: &str, scenario: MockScenario) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            state: Arc::new(MockState::new(scenario)),
        })
    }

    /// 开始接受连接并常驻（直到监听 socket 关闭）。
    pub async fn serve(self) {
        run_accept_loop(self.listener, self.state).await;
    }
}

/// accept 主循环：每条连接 spawn 握手 + 服务；回收已完成连接。
#[allow(clippy::result_large_err)] // tungstenite accept_hdr_async 的 Err(ErrorResponse) 类型固定
async fn run_accept_loop(listener: TcpListener, state: Arc<MockState>) {
    let mut conns = JoinSet::new();
    while let Ok((tcp, _peer)) = listener.accept().await {
        let state = Arc::clone(&state);
        conns.spawn(async move {
            // FnOnce 回调把认证身份写这里，accept 成功后取出。
            let authed: Arc<StdMutex<Option<String>>> = Arc::new(StdMutex::new(None));
            let cb_state = Arc::clone(&state);
            let cb_authed = Arc::clone(&authed);
            let ws = match accept_hdr_async(tcp, move |req: &Request, resp: Response| {
                authenticate(&cb_state, req, resp, &cb_authed)
            })
            .await
            {
                Ok(ws) => ws,
                Err(_) => return, // 握手失败（含 401）：不分配业务资源
            };
            let name = authed
                .lock()
                .unwrap()
                .take()
                .expect("握手成功必须已写入策略身份");
            serve_connection(state, name, ws).await;
        });
        while conns.try_join_next().is_some() {}
    }
    while conns.join_next().await.is_some() {}
}

/// 启动裸脚本 mock（不鉴权，供协议异常/坏帧单测）。
pub async fn spawn_raw<S>(script: S) -> (String, JoinHandle<()>)
where
    S: ConnectionScript + Clone + 'static,
{
    let (url, listener) = bind_ephemeral().await;
    let handle = tokio::spawn(async move {
        let mut conns = JoinSet::new();
        while let Ok((tcp, _peer)) = listener.accept().await {
            let script = script.clone();
            conns.spawn(async move {
                let Ok(ws) = accept_async(tcp).await else {
                    return;
                };
                script.run(ws).await;
            });
            while conns.try_join_next().is_some() {}
        }
        while conns.join_next().await.is_some() {}
    });
    (url, handle)
}

/// 升级前校验：三件套齐全 → nonce 未重放 → HMAC 恒定时间验签。
#[allow(clippy::result_large_err)]
fn authenticate(
    state: &MockState,
    req: &Request,
    resp: Response,
    out: &StdMutex<Option<String>>,
) -> Result<Response, ErrorResponse> {
    let deny = || {
        Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Some("unauthorized".to_string()))
            .expect("401 响应构造恒成立")
    };
    let params = parse_query(req.uri().query().unwrap_or(""));
    let Some((name, nonce, signature)) = (|| {
        let name = params.get("strategy_name")?;
        let nonce = params.get("nonce")?;
        let signature = params.get("signature")?;
        Some((name.clone(), nonce.clone(), signature.clone()))
    })() else {
        return Err(deny());
    };

    // 惰性清理过期 nonce（TTL 5min），再查重放。
    state
        .nonces
        .retain(|_, t| t.elapsed() < Duration::from_secs(300));
    if state.nonces.contains_key(&nonce) {
        return Err(deny());
    }
    if !auth::verify(&state.secret, &name, &nonce, &signature) {
        return Err(deny());
    }
    state.nonces.insert(nonce, Instant::now());
    *out.lock().unwrap() = Some(name);
    Ok(resp)
}

/// 极简 query 解析（值做 %XX 解码）。
fn parse_query(q: &str) -> HashMap<String, String> {
    q.split('&')
        .filter(|s| !s.is_empty())
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (percent_decode(k), percent_decode(v)))
        .collect()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
        i += 1;
    }
    String::from_utf8(out).unwrap_or_default()
}

/// 一条鉴权连接的读写循环。
async fn serve_connection(state: Arc<MockState>, name: String, ws: MockConn) {
    let (raw_sink, mut stream) = ws.split();
    let sink: SharedSink = Arc::new(Mutex::new(raw_sink));
    let token = state.hub.register(name.clone(), Arc::clone(&sink));

    while let Some(frame) = stream.next().await {
        match frame {
            Ok(Message::Text(text)) => {
                handle_text(Arc::clone(&state), name.clone(), text.as_str()).await
            }
            Ok(Message::Binary(bytes)) => {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    handle_text(Arc::clone(&state), name.clone(), &text).await;
                }
            }
            Ok(Message::Ping(p)) => {
                let mut g = sink.lock().await;
                let _ = g.send(Message::Pong(p)).await;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }
    state.hub.unregister_if(&name, token);
}

async fn handle_text(state: Arc<MockState>, name: String, text: &str) {
    let cmd: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => {
            send_error(&state, &name, None, "报文解析失败").await;
            return;
        }
    };
    match cmd["option"].as_str() {
        Some("place_order") | Some("place") => place_order(&state, &name, &cmd).await,
        Some("cancel_order") | Some("cancel") => cancel_order(&state, &name, &cmd).await,
        Some("get_position") | Some("get_positions") | Some("positions") => {
            let frame = serde_json::json!({
                "type": "positions",
                "status": "OK",
                "strategy_name": name,
                "positions": [],
                "message": "mock 不维护持仓快照"
            });
            state.hub.send(&name, frame.to_string()).await;
        }
        Some(other) => send_error(&state, &name, None, &format!("未知命令 `{other}`")).await,
        None => send_error(&state, &name, None, "缺少 option 字段").await,
    }
}

/// mock lot 对齐：floor 到 1e-6（模拟交易所 stepSize；无外部规则表）。
fn floor_lot(quantity: f64) -> f64 {
    (quantity * 1e6).floor() / 1e6
}

async fn place_order(state: &Arc<MockState>, name: &str, cmd: &serde_json::Value) {
    let signal_id = cmd["signal_id"].as_str().map(str::to_string);

    // signal_id 幂等：已见信号不建新单，回该单当前状态的完整快照。
    if reply_if_signal_seen(state, name, signal_id.as_deref()).await {
        return;
    }

    // 帧内字段（身份 strategy_name 以握手为准）；instrument 先解析，
    // 以便按 symbol 取各自的参考价。
    let exchange = cmd["exchange"].as_str().unwrap_or("binance");
    let instrument = cmd["instrument"].as_str().unwrap_or("");
    let side = cmd["side"].as_str().unwrap_or("long");
    let reference = state.reference_price(instrument);

    // 帧形分流：平仓帧 quantity 逐字采用 + reduce_only；
    // 开仓帧 order_cash / 该 symbol 参考价换算。None=已回错误帧。
    let Some((quantity, reduce_only)) = resolve_quantity(state, name, cmd, reference).await else {
        return;
    };

    let order_type = cmd["order_type"].as_str().unwrap_or("market");
    let leverage = cmd["leverage"].as_u64().unwrap_or(1);

    let mut raw = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut raw);
    let cid = format!("0x{}", hex::encode(raw));
    let seq = state.exchange_seq.fetch_add(1, Ordering::Relaxed);
    let rec = OrderRec {
        cid: cid.clone(),
        exchange_order_id: format!("MOCK-{seq:06}"),
        exchange: exchange.to_string(),
        instrument: instrument.to_string(),
        side: side.to_string(),
        order_type: order_type.to_string(),
        quantity,
        reduce_only,
        leverage,
        status: "NEW".to_string(),
        fill_price: reference,
    };
    state.orders.insert(cid.clone(), rec.clone());
    if let Some(sid) = &signal_id {
        state.signal_to_order.insert(sid.clone(), cid.clone());
    }

    // 受理：立即 ack(NEW)（成交量此时为 0）。
    send_snapshot(
        state,
        name,
        "ack",
        "NEW",
        &rec,
        0.0,
        0.0,
        signal_id.as_deref(),
    )
    .await;

    // 交易所式成交：独立任务按节奏推 PARTIAL…FILLED。
    spawn_fill_progress(
        Arc::clone(state),
        name.to_string(),
        cid,
        quantity,
        reference,
        signal_id,
    );
}

/// 命中已见 signal_id：回该单当前状态完整快照并返回 true；未见返回 false。
async fn reply_if_signal_seen(state: &Arc<MockState>, name: &str, signal_id: Option<&str>) -> bool {
    let Some(sid) = signal_id else {
        return false;
    };
    let Some(cid) = state.signal_to_order.get(sid).map(|g| g.clone()) else {
        return false;
    };
    let Some(rec) = state.orders.get(&cid).map(|g| g.clone()) else {
        return false;
    };
    let (filled_quantity, filled_price) = if rec.status == "FILLED" {
        (rec.quantity, rec.fill_price)
    } else {
        (0.0, 0.0)
    };
    send_snapshot(
        state,
        name,
        "ack",
        &rec.status,
        &rec,
        filled_quantity,
        filled_price,
        Some(sid),
    )
    .await;
    true
}

/// 解析下单数量：平仓帧 quantity 逐字采用；开仓帧按 order_cash/参考价换算。
/// 返回 None 表示已回错误帧，调用方应直接返回。
async fn resolve_quantity(
    state: &Arc<MockState>,
    name: &str,
    cmd: &serde_json::Value,
    reference: f64,
) -> Option<(f64, bool)> {
    let close_quantity = cmd
        .get("quantity")
        .map(wire::f64_or_zero)
        .filter(|q| *q > 0.0);
    if let Some(q) = close_quantity {
        return Some((q, cmd["reduce_only"].as_bool().unwrap_or(false)));
    }

    let cash = cmd.get("order_cash").map(wire::f64_or_zero).unwrap_or(0.0);
    if cash <= 0.0 {
        send_error(
            state,
            name,
            None,
            "quantity 与 order_cash 必须提供其一（且为正数）",
        )
        .await;
        return None;
    }
    if reference <= 0.0 {
        send_error(state, name, None, "mock 参考价非法，无法换算 order_cash").await;
        return None;
    }
    let converted = floor_lot(cash / reference);
    if converted <= 0.0 {
        send_error(state, name, None, "order_cash 换算数量非正（金额过小）").await;
        return None;
    }
    Some((converted, false))
}

/// spawn 独立任务，按节奏推 PARTIAL…FILLED（交易所式成交）。
fn spawn_fill_progress(
    state: Arc<MockState>,
    name: String,
    cid: String,
    quantity: f64,
    price: f64,
    signal_id: Option<String>,
) {
    let times = state.fill_times;
    let interval = state.fill_interval;
    tokio::spawn(async move {
        for step in 1..=times {
            if interval > Duration::ZERO {
                tokio::time::sleep(interval).await;
            }
            let status = if step == times { "FILLED" } else { "PARTIAL" };
            let cum = if step == times {
                quantity
            } else {
                floor_lot((quantity / times as f64) * step as f64)
            };
            let rec = state
                .orders
                .get(&cid)
                .map(|g| {
                    let mut r = g.clone();
                    r.status = status.to_string();
                    r
                })
                .expect("订单刚建");
            if let Some(mut s) = state.orders.get_mut(&cid) {
                s.status = status.to_string();
            }
            send_snapshot(
                &state,
                &name,
                "order_event",
                status,
                &rec,
                cum,
                price,
                signal_id.as_deref(),
            )
            .await;
        }
    });
}

async fn cancel_order(state: &Arc<MockState>, name: &str, cmd: &serde_json::Value) {
    let Some(cid) = cmd["client_order_id"].as_str() else {
        send_error(state, name, None, "cancel_order 缺少 client_order_id").await;
        return;
    };
    let Some(rec) = state.orders.get(cid).map(|g| g.clone()) else {
        send_error(state, name, Some(cid), "订单不存在").await;
        return;
    };
    if rec.status == "FILLED" || rec.status == "CANCELED" {
        send_error(state, name, Some(cid), &format!("订单已 {}", rec.status)).await;
        return;
    }
    if let Some(mut s) = state.orders.get_mut(cid) {
        s.status = "CANCELED".to_string();
    }
    let rec = state.orders.get(cid).map(|g| g.clone()).expect("刚校验");
    send_snapshot(state, name, "ack", "CANCELED", &rec, 0.0, 0.0, None).await;
}

/// 构造并路由一帧完整订单快照（ack/order_event 同形）。
#[allow(clippy::too_many_arguments)]
async fn send_snapshot(
    state: &Arc<MockState>,
    name: &str,
    kind: &str,
    status: &str,
    rec: &OrderRec,
    filled_quantity: f64,
    filled_price: f64,
    signal_id: Option<&str>,
) {
    // side 回声：long→BUY，short→SELL（对齐真实服务大写口径）
    let side = if rec.side == "short" { "SELL" } else { "BUY" };
    let frame = serde_json::json!({
        "signal_id": signal_id,
        "type": kind,
        "status": status,
        "client_order_id": rec.cid,
        "exchange_order_id": rec.exchange_order_id,
        "exchange": rec.exchange,
        "strategy_name": name,
        "instrument": rec.instrument,
        "side": side,
        "order_type": rec.order_type,
        "order_price": 0,
        "order_quantity": rec.quantity,
        "reduce_only": rec.reduce_only,
        "filled_price": filled_price,
        "filled_quantity": filled_quantity,
        "message": serde_json::Value::Null,
        "positions": serde_json::Value::Null,
    });
    state.hub.send(name, frame.to_string()).await;
}

async fn send_error(state: &Arc<MockState>, name: &str, cid: Option<&str>, msg: &str) {
    let frame = serde_json::json!({
        "type": "error",
        "status": "REJECTED",
        "strategy_name": name,
        "client_order_id": cid,
        "message": msg,
    });
    state.hub.send(name, frame.to_string()).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trading_service::{
        ClientEvent, TradingConfig, TradingGateway, TradingServiceClient,
    };
    use strategy_core::order::OrderState;
    use strategy_core::{IntentKind, Side as CoreSide, SignalIntent, SignalStatus};
    use strategy_signals::gateway::AsyncOrderGateway;
    use tokio::runtime::Handle;

    const SECRET: &str = "test-secret";

    fn client_cfg(url: String) -> TradingConfig {
        TradingConfig {
            ws_url: url,
            submit_enabled: true,
            request_timeout: Duration::from_secs(3),
            strategy_secret: SECRET.into(),
            reconnect_backoff_max_secs: 8,
        }
    }

    fn venue_for(order_cash: f64) -> crate::trading_service::OrderVenueCfg {
        crate::trading_service::OrderVenueCfg {
            exchange: "binance".into(),
            account: "test_binance".into(),
            leverage: 3,
            order_cash,
        }
    }

    fn intent(signal: &str, side: CoreSide, kind: IntentKind) -> SignalIntent {
        SignalIntent {
            id: signal.into(),
            strategy_id: "sar_supertrend".into(),
            symbol: "BTCUSDT".into(),
            kind,
            direction: side,
            price: 100.0,
            size: 0.0,
            size_raw: None,
            stop_price: None,
            position_id: None,
            stop_order_id: None,
            created_event_time: chrono::Utc::now(),
            status: SignalStatus::Created,
        }
    }

    /// 用正确签名直连（不经过客户端）。
    async fn raw_connect(
        url: &str,
        name: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        let nonce = auth::new_nonce();
        let sig = auth::sign(SECRET, name, &nonce);
        let full = auth::connect_url(url, name, &nonce, &sig);
        tokio_tungstenite::connect_async(full)
            .await
            .expect("握手应成功")
            .0
    }

    /// 消费该连接上下一条文本帧 → JSON。
    async fn next_json(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        label: &str,
    ) -> serde_json::Value {
        let msg = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .unwrap_or_else(|_| panic!("等待帧超时: {label}"))
            .unwrap()
            .unwrap();
        serde_json::from_str(msg.to_text().unwrap()).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handshake_rejects_missing_bad_and_replay() {
        let (url, _srv) = spawn(MockScenario::new(SECRET)).await;
        assert!(tokio_tungstenite::connect_async(&url).await.is_err());
        let bad = auth::connect_url(&url, "st", &auth::new_nonce(), "deadbeef");
        assert!(tokio_tungstenite::connect_async(bad).await.is_err());
        let mut ws = raw_connect(&url, "st").await;
        let nonce = auth::new_nonce();
        let sig = auth::sign(SECRET, "st", &nonce);
        let replay_url = auth::connect_url(&url, "st", &nonce, &sig);
        let _ = tokio_tungstenite::connect_async(replay_url)
            .await
            .expect("首次 nonce");
        assert!(
            tokio_tungstenite::connect_async(auth::connect_url(&url, "st", &nonce, &sig))
                .await
                .is_err(),
            "重复 nonce 必须拒绝"
        );
        let _ = ws.close(None).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(unused_assignments)] // 收集变量在 loop 内 break 前必赋值
    async fn open_order_cash_converts_and_full_fields() {
        // 开仓 order_cash=100，mock 参考价 100.5 → quantity=0.995024
        let (url, _srv) = spawn(MockScenario::new(SECRET)).await;
        let (client, mut events) =
            TradingServiceClient::start_arc(client_cfg(url), Handle::current());
        let mut gw = TradingGateway::new(client, "sar_supertrend".into(), venue_for(100.0));
        let cid = gw
            .submit_fut(&intent("sig_open", CoreSide::Long, IntentKind::Open))
            .await
            .expect("ack 应成功");
        assert!(cid.starts_with("0x") && cid.len() == 34);

        let mut got_eoid = None;
        let mut filled = None;
        loop {
            match tokio::time::timeout(Duration::from_secs(3), events.recv())
                .await
                .unwrap()
                .unwrap()
            {
                ClientEvent::OrderUpdate(u) => {
                    // 先按 Filled 判定（避免部分 move 后再用整个 u）
                    if u.state == OrderState::Filled {
                        got_eoid = u.exchange_order_id.clone();
                        filled = Some(u);
                        break;
                    }
                }
                ClientEvent::Reconnected { .. } => {}
            }
        }
        assert!(got_eoid.is_some(), "exchange_order_id 必须落账");
        let filled = filled.unwrap();
        assert!(
            (filled.filled_size - 0.995024).abs() < 1e-9,
            "got {}",
            filled.filled_size
        );
        assert!((filled.avg_price - 100.5).abs() < 1e-9);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_frame_passthrough_quantity() {
        // 平仓帧 quantity 逐字采用、reduce_only
        let (url, _srv) = spawn(MockScenario::new(SECRET)).await;
        let mut ws = raw_connect(&url, "st_close").await;
        ws.send(Message::Text(
            serde_json::json!({
                "option": "place_order",
                "exchange": "binance",
                "instrument": "BTCUSDT",
                "side": "short",
                "order_type": "market",
                "quantity": "0.01",
                "reduce_only": true,
                "signal_id": "sig_close"
            })
            .to_string(),
        ))
        .await
        .unwrap();

        let ack = next_json(&mut ws, "ack").await;
        assert_eq!(ack["status"], "NEW");
        assert_eq!(ack["side"], "SELL");
        assert_eq!(ack["order_quantity"], 0.01);
        assert_eq!(ack["reduce_only"], true);
        assert_eq!(ack["signal_id"], "sig_close");
        assert!(ack["exchange_order_id"].is_string());

        let ev = next_json(&mut ws, "order_event").await;
        assert_eq!(ev["status"], "FILLED");
        assert_eq!(ev["filled_quantity"], 0.01);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn per_symbol_price_overrides_default() {
        // 默认参考价 100.5；SOLUSDT 单独给 50.0：order_cash=100 → qty 2.0，fill 50
        let scenario = MockScenario::new(SECRET).symbol_price("SOLUSDT", 50.0);
        let (url, _srv) = spawn(scenario).await;
        let mut ws = raw_connect(&url, "st_multi").await;
        ws.send(Message::Text(
            serde_json::json!({
                "option": "place_order",
                "exchange": "binance",
                "instrument": "SOLUSDT",
                "side": "long",
                "order_type": "market",
                "order_cash": "100",
                "leverage": 1,
                "signal_id": "sig_sol_px"
            })
            .to_string(),
        ))
        .await
        .unwrap();

        let _ack = next_json(&mut ws, "ack").await;
        let ev = next_json(&mut ws, "order_event").await;
        assert_eq!(ev["status"], "FILLED");
        assert_eq!(ev["filled_quantity"], 2.0);
        assert_eq!(ev["filled_price"], 50.0);

        // 未覆盖的 instrument 仍回落默认参考价：100/100.5 = 0.995024
        ws.send(Message::Text(
            serde_json::json!({
                "option": "place_order",
                "exchange": "binance",
                "instrument": "BTCUSDT",
                "side": "long",
                "order_type": "market",
                "order_cash": "100",
                "leverage": 1,
                "signal_id": "sig_btc_default"
            })
            .to_string(),
        ))
        .await
        .unwrap();
        let _ack = next_json(&mut ws, "ack").await;
        let ev = next_json(&mut ws, "order_event").await;
        assert_eq!(ev["filled_quantity"], 0.995024);
        assert_eq!(ev["filled_price"], 100.5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resubmit_same_signal_is_idempotent() {
        let (url, _srv) = spawn(MockScenario::new(SECRET)).await;
        let (client, _events) = TradingServiceClient::start_arc(client_cfg(url), Handle::current());
        let mut gw = TradingGateway::new(client, "st_id".into(), venue_for(100.0));
        let it = intent("sig_idem", CoreSide::Long, IntentKind::Open);
        let id1 = gw.submit_fut(&it).await.unwrap();
        let id2 = gw.submit_fut(&it).await.unwrap();
        assert_eq!(id1, id2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rejects_frame_without_quantity_or_cash() {
        let (url, _srv) = spawn(MockScenario::new(SECRET)).await;
        let mut ws = raw_connect(&url, "st_raw").await;
        ws.send(Message::Text(
            serde_json::json!({"option": "place_order", "signal_id": "sig_bad"}).to_string(),
        ))
        .await
        .unwrap();
        let ev = next_json(&mut ws, "error").await;
        assert_eq!(ev["status"], "REJECTED");

        ws.send(Message::Text(
            serde_json::json!({"option": "get_position"}).to_string(),
        ))
        .await
        .unwrap();
        let ev = next_json(&mut ws, "positions").await;
        assert_eq!(ev["type"], "positions");
        assert!(ev["positions"].is_array());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_strategies_no_crosstalk() {
        let (url, _srv) = spawn(MockScenario::new(SECRET)).await;
        let (client_a, mut events_a) =
            TradingServiceClient::start_arc(client_cfg(url.clone()), Handle::current());
        let (client_b, mut events_b) =
            TradingServiceClient::start_arc(client_cfg(url), Handle::current());

        let mut gw_a = TradingGateway::new(client_a, "st_a".into(), venue_for(100.0));
        let cid_a = gw_a
            .submit_fut(&intent("sig_a1", CoreSide::Long, IntentKind::Open))
            .await
            .unwrap();
        let mut gw_b = TradingGateway::new(client_b, "st_b".into(), venue_for(100.0));
        let cid_b = gw_b
            .submit_fut(&intent("sig_b1", CoreSide::Long, IntentKind::Open))
            .await
            .unwrap();

        async fn filled_cid(rx: &mut tokio::sync::mpsc::Receiver<ClientEvent>) -> String {
            loop {
                match tokio::time::timeout(Duration::from_secs(3), rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                {
                    ClientEvent::OrderUpdate(u) if u.state == OrderState::Filled => {
                        return u.order_id
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(filled_cid(&mut events_a).await, cid_a);
        assert_eq!(filled_cid(&mut events_b).await, cid_b);
    }
}
