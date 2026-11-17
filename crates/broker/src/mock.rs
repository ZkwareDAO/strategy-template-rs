//! # broker::mock —— 支持协议 v2（order_cash）的本地 trading-service 模拟器
//!
//! 联调/测试组件，在 127.0.0.1 随机端口起一个与 trading-service 南向协议
//! 兼容的 WS 服务，**不修改外部 trading_service_rs 仓库**：
//! - 升级前 HMAC 握手（strategy_name + nonce + signature，nonce 防重放，恒定
//!   时间比较）；失败直接 401，不升级、不注册、不 spawn；
//! - place_order 两种帧形：
//!   · 开仓帧（`order_cash` + `leverage`，无 quantity）：mock 按参考价
//!   （[`MockScenario::fill_price`]）换算 `quantity=order_cash/参考价`，floor 到 1e-6；
//!   若配置了 [`MockScenario::contract`]（期货）则改为 [`ContractSpec::sizing_lots`]
//!   整数手（与回测同一口径；一手都开不起 → REJECTED）。mock 不记现金/PnL，手续费不在此计；
//!   · 平仓帧（`quantity` + `reduce_only`）：数量**逐字采用**，不重算；
//! - 服务端生成 client_order_id（0x+32hex）与 exchange_order_id；先回
//!   ack(NEW)（完整字段、signal_id 回声、side=BUY/SELL），再按节奏推 PARTIAL…FILLED；
//!   同 signal_id 重发幂等回同一单；
//! - cancel_order 同真实服务语义；get_positions：维护**净持仓镜像**
//!   （FILLED 开仓加权累计、reduce_only 扣减，按 instrument/side 过滤应答，
//!   要素对齐协议 §3.5——否则 live 每分钟远程对账会把 mock 空仓当真、误触
//!   external_close）；
//! - **K线模拟推送**（`--kline-dir` 装载 1m CSV 后启用）：`subscribe_kline(s)` /
//!   `unsubscribe_kline(s)` / `get_klines` 命令（对齐协议 §10）；推送统一为
//!   `kline_1m_batch` 合帧（同 exchange 同 tick 的多根 bar 聚一帧数组、组内按
//!   (symbol,start_time) 升序；单根亦发 batch——真实服务端只发 batch），
//!   按 [`MockKlineReplay::interval`] 节奏回放 CSV（缺省加速回放；断线重订
//!   从游标续推不回放历史）；回放后该 symbol 的成交价自动跟随最新收盘价
//!   （显式 `symbol_price` 覆盖仍优先）。详见 [`crate::kline`]。
//!
//! **非生产服务，禁止在 live 装配中引用。** 另提供 [`spawn_raw`]（不鉴权）
//! 供坏帧/异常时序单测。

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures_util::lock::Mutex;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use rand::RngCore;
use strategy_core::contract::ContractSpec;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{accept_async, accept_hdr_async, WebSocketStream};

use crate::auth;
use crate::kline::{Bar, KlineBook};
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

/// K线模拟回放配置（`subscribe_kline` 的数据源与节奏）。
#[derive(Debug, Clone)]
pub struct MockKlineReplay {
    /// 1m bar 库（CSV 装载见 [`crate::kline::KlineBook`]）
    pub book: KlineBook,
    /// 同一 symbol 相邻两根 bar 的推送间隔（`Duration::ZERO` = 尽快推）
    pub interval: Duration,
    /// 回放起点（UTC 毫秒，含）；`None` = CSV 第一根
    pub start_ms: Option<i64>,
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
    /// 期货合约参数：`Some` 时开仓手数走 [`ContractSpec::sizing_lots`]（整数手，与回测同口径）；
    /// `None` 保持 crypto 小数口径（order_cash / 参考价，floor 1e-6）
    pub contract: Option<ContractSpec>,
    /// K线模拟回放：`Some` 时支持 `subscribe_kline` 推送与「成交价跟随
    /// 最新收盘」；`None`（缺省）保持纯订单 mock 行为不变
    pub kline: Option<MockKlineReplay>,
}

impl MockScenario {
    pub fn new(secret_key: impl Into<String>) -> Self {
        Self {
            secret_key: secret_key.into(),
            fill_times: 1,
            fill_interval: Duration::ZERO,
            fill_price: 100.5,
            symbol_prices: std::collections::HashMap::new(),
            contract: None,
            kline: None,
        }
    }
    /// 期货模式：开仓按合约整数手换算（见 [`MockScenario::contract`] 字段说明）。
    pub fn contract(mut self, contract: Option<ContractSpec>) -> Self {
        self.contract = contract;
        self
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
    /// 启用 K线模拟回放：直接给出装载好的 [`KlineBook`] 与节奏。
    pub fn kline_replay(
        mut self,
        book: KlineBook,
        interval: Duration,
        start_ms: Option<i64>,
    ) -> Self {
        self.kline = Some(MockKlineReplay {
            book,
            interval,
            start_ms,
        });
        self
    }
    /// 便捷入口：从目录递归装载 `*_1m.csv`（装载失败清单原样返回，
    /// 坏文件跳过不阻断——联调期目录常有 gitignore 占位空文件）。
    pub fn kline_dir(
        self,
        dir: impl AsRef<Path>,
        interval: Duration,
        start_ms: Option<i64>,
    ) -> (Self, Vec<String>) {
        let (book, errors) = KlineBook::load(dir.as_ref());
        (self.kline_replay(book, interval, start_ms), errors)
    }
}

/// 净持仓镜像外部改仓句柄（`MockServer::serve` 消费 self 后仍可用）。
#[derive(Clone)]
pub struct MockPositionHandle {
    state: Arc<MockState>,
}

impl MockPositionHandle {
    /// 直接改写净持仓镜像：`quantity <= 0` 移除条目（空仓应答）。
    pub fn set_net_position(&self, instrument: &str, side: &str, quantity: f64, price: f64) {
        if quantity <= f64::EPSILON {
            self.state.pos_book.remove(instrument);
            return;
        }
        self.state.pos_book.insert(
            instrument.to_string(),
            PosMirror {
                side: norm_side(side),
                exchange: "external".into(),
                qty: quantity,
                price,
            },
        );
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

    /// 以「登记时的 token」向连接写帧：token 已换代（同名新连接顶替旧连接）
    /// 或写失败 → false。K线回放任务用它保证旧任务不会把帧灌进新连接。
    async fn send_as(&self, name: &str, token: u64, text: String) -> bool {
        let sink = match self.clients.get(name) {
            Some(g) if g.token == token => g.sink.clone(),
            _ => return false,
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
    contract: Option<ContractSpec>,
    /// K线模拟回放（`None` = 订单-only mock，行为与旧版逐字节一致）
    kline: Option<Arc<MockKlineReplay>>,
    /// K线回放推进后记录的最新收盘价（key：订阅请求名 + 文件名义名都写）；
    /// 成交价参考链：symbol_prices 显式覆盖 > 最新收盘 > 全局 fill_price
    last_close: DashMap<String, f64>,
    /// (strategy_name, 订阅请求 symbol) → 已推送 bar 的下标游标；
    /// 断线重订从游标 +1 续推（对齐真实服务「只推定稿、不补历史」语义）
    replay_cursor: DashMap<(String, String), usize>,
    /// instrument → 净持仓镜像（由 FILLED 回报推进；供 get_positions 应答）
    pos_book: DashMap<String, PosMirror>,
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
            contract: s.contract,
            kline: s.kline.map(Arc::new),
            last_close: DashMap::new(),
            replay_cursor: DashMap::new(),
            pos_book: DashMap::new(),
            hub: Hub::default(),
            nonces: DashMap::new(),
            orders: DashMap::new(),
            signal_to_order: DashMap::new(),
            exchange_seq: AtomicU64::new(1),
        }
    }

    /// 某 instrument 的成交/换算参考价：per-symbol 显式覆盖 > K线回放最新
    /// 收盘 > 全局 fill_price。
    fn reference_price(&self, instrument: &str) -> f64 {
        if let Some(p) = self.symbol_prices.get(instrument).copied() {
            return p;
        }
        if let Some(p) = self.last_close_price(instrument) {
            return p;
        }
        self.fill_price
    }

    /// 最新收盘价查询：精确（原样/大写）→ 最长前缀（CU2601↔CU 双向，
    /// 与 [`KlineBook::lookup`] 同一口径）。
    fn last_close_price(&self, req: &str) -> Option<f64> {
        let up = req.trim().to_uppercase();
        for key in [req, up.as_str()] {
            if let Some(g) = self.last_close.get(key) {
                return Some(*g);
            }
        }
        let mut best: Option<(usize, f64)> = None;
        for e in self.last_close.iter() {
            let k = e.key().to_uppercase();
            let hit = if k.len() <= up.len() {
                up.starts_with(k.as_str())
            } else {
                k.starts_with(up.as_str())
            };
            if hit && best.is_none_or(|(bl, _)| k.len() > bl) {
                best = Some((k.len(), *e.value()));
            }
        }
        best.map(|(_, v)| v)
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

    /// 实际绑定地址（`bind("127.0.0.1:0")` 时取随机端口用）。
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// 演练/测试用：直接改写某标的净持仓镜像（模拟第三方在他处开/平仓——
    /// `get_positions` 对账应据此收敛）。`quantity <= 0` 移除条目 = 空仓应答。
    pub fn set_net_position(&self, instrument: &str, side: &str, quantity: f64, price: f64) {
        self.position_handle()
            .set_net_position(instrument, side, quantity, price);
    }

    /// serve 之后仍需改仓：先取句柄（与服务器共享同一份镜像）。
    pub fn position_handle(&self) -> MockPositionHandle {
        MockPositionHandle {
            state: Arc::clone(&self.state),
        }
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
        eprintln!("[mock] 401 握手缺参（strategy_name/nonce/signature 三件套不齐）");
        return Err(deny());
    };

    // 惰性清理过期 nonce（TTL 5min），再查重放。
    state
        .nonces
        .retain(|_, t| t.elapsed() < Duration::from_secs(300));
    if state.nonces.contains_key(&nonce) {
        eprintln!("[mock] 401 nonce 重放：name={name} nonce={nonce}");
        return Err(deny());
    }
    if !auth::verify(&state.secret, &name, &nonce, &signature) {
        eprintln!(
            "[mock] 401 验签失败：name={name}（请核对 --secret 与 services.trading.strategy_secret / STRATEGY_SECRET 环境变量）"
        );
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
    // 本连接的 K线订阅状态（symbol→exchange 登记 + 聚合泵）；连接结束停泵。
    let mut kconn = KlineConn::default();

    while let Some(frame) = stream.next().await {
        match frame {
            Ok(Message::Text(text)) => {
                handle_text(
                    Arc::clone(&state),
                    name.clone(),
                    text.as_str(),
                    token,
                    &mut kconn,
                )
                .await
            }
            Ok(Message::Binary(bytes)) => {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    handle_text(Arc::clone(&state), name.clone(), &text, token, &mut kconn).await;
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
    if let Some(pump) = kconn.pump.take() {
        pump.abort();
    }
    state.hub.unregister_if(&name, token);
}

async fn handle_text(
    state: Arc<MockState>,
    name: String,
    text: &str,
    token: u64,
    kconn: &mut KlineConn,
) {
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
        Some("get_order") => {
            // §7：按客户单号或信号 id 查内存单；查无回 error（不泄露存在性）。
            // 在途看门狗（live poll_in_flight）依赖本命令，缺了会一直
            // "未知命令 `get_order`" 空转。
            let cid = cmd["client_order_id"]
                .as_str()
                .map(str::to_string)
                .or_else(|| {
                    cmd["signal_id"]
                        .as_str()
                        .and_then(|sid| state.signal_to_order.get(sid).map(|g| g.clone()))
                });
            match cid.and_then(|c| state.orders.get(&c).map(|g| g.clone())) {
                Some(rec) => {
                    let (fq, fp) = if rec.status == "FILLED" {
                        (rec.quantity, rec.fill_price)
                    } else {
                        (0.0, 0.0)
                    };
                    send_snapshot(
                        &state,
                        &name,
                        "order",
                        &rec.status,
                        &rec,
                        fq,
                        fp,
                        cmd["signal_id"].as_str(),
                    )
                    .await;
                }
                None => send_error(&state, &name, None, "订单不存在").await,
            }
        }
        Some("get_position") | Some("get_positions") | Some("positions") => {
            // 净持仓镜像应答（协议 §3.5/§8）：可选 instrument/side 过滤，
            // 命中元素给 WirePosition 消费的核心要素 + 少量同形装饰字段。
            let want_inst = cmd["instrument"]
                .as_str()
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let want_side = cmd["side"]
                .as_str()
                .map(norm_side)
                .filter(|s| s == "BUY" || s == "SELL");
            let rows: Vec<serde_json::Value> = state
                .pos_book
                .iter()
                .filter(|e| {
                    want_inst.is_none_or(|i| e.key().eq_ignore_ascii_case(i))
                        && want_side.as_ref().is_none_or(|s| &e.value().side == s)
                })
                .enumerate()
                .map(|(i, e)| {
                    let m = e.value();
                    serde_json::json!({
                        "id": 1000 + i as u64,
                        "account_id": 1,
                        "rule_id": 0,
                        "exchange": m.exchange,
                        "instrument_type": "futures",
                        "instrument": e.key(),
                        "side": m.side,
                        "quantity": m.qty,
                        "multiplier": 1,
                        "pos_price": m.price,
                        "current_price": m.price,
                        "notional": m.qty * m.price,
                        "leverage": 1,
                        "margin": m.qty * m.price,
                        "deleted": false,
                    })
                })
                .collect();
            let frame = serde_json::json!({
                "type": "positions",
                "status": "OK",
                "strategy_name": name,
                "positions": rows,
                "message": "mock 净持仓镜像（由 FILLED 回报维护）"
            });
            state.hub.send(&name, frame.to_string()).await;
        }
        Some("subscribe_kline") | Some("subscribe_klines") => {
            subscribe_kline(&state, &name, token, &cmd, kconn).await
        }
        Some("unsubscribe_kline") | Some("unsubscribe_klines") => unsubscribe_kline(&cmd, kconn),
        Some("get_klines") => get_klines(&state, &name, &cmd).await,
        Some(other) => send_error(&state, &name, None, &format!("未知命令 `{other}`")).await,
        None => send_error(&state, &name, None, "缺少 option 字段").await,
    }
}

/// 订阅帧 symbols 提取：`symbols` 数组优先；兼容单标的 `symbol`/`instrument`
/// （真实服务层回退取 instrument() 的 mock 对应物）；去重保序。
fn extract_symbols(cmd: &serde_json::Value) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    if let Some(arr) = cmd["symbols"].as_array() {
        for v in arr {
            if let Some(s) = v.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                if !out.iter().any(|o| o == s) {
                    out.push(s.to_string());
                }
            }
        }
    }
    if out.is_empty() {
        for key in ["symbol", "instrument"] {
            if let Some(s) = cmd[key].as_str().map(str::trim).filter(|s| !s.is_empty()) {
                out.push(s.to_string());
                break;
            }
        }
    }
    out
}

/// K线周期归一（对齐真实服务 normalize_kline_interval：仅 1m 系列）。
fn normalize_interval(raw: Option<&str>) -> bool {
    matches!(
        raw.unwrap_or("1m").trim().to_ascii_lowercase().as_str(),
        "" | "1m" | "1min" | "1minute"
    )
}

/// `subscribe_kline(s)`：登记 symbol→exchange 并惰性启动本连接聚合泵。
/// 校验对齐真实服务 §10.1：缺 exchange/非法周期/空标的列表均回 error；
/// 未知标的记入缺失清单回 error（真实服务对未知标的同样拒订阅）。
/// 订阅类错误 message 恒含「K线」/「subscribe_kline」——客户端 error 帧
/// 匹配链据此不误吞订单/get_klines 在途请求。重复订阅幂等（游标不回退）。
async fn subscribe_kline(
    state: &Arc<MockState>,
    name: &str,
    token: u64,
    cmd: &serde_json::Value,
    kconn: &mut KlineConn,
) {
    if state.kline.is_none() {
        send_error(
            state,
            name,
            None,
            "mock 未装载 K线数据（启动缺 --kline-dir），无法响应 subscribe_kline",
        )
        .await;
        return;
    };
    if !normalize_interval(cmd["interval"].as_str()) {
        send_error(state, name, None, "mock K线仅支持 interval=1m").await;
        return;
    }
    let Some(exchange) = cmd["exchange"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        send_error(state, name, None, "subscribe_kline 缺少必填字段：exchange").await;
        return;
    };
    let symbols = extract_symbols(cmd);
    if symbols.is_empty() {
        send_error(state, name, None, "subscribe_kline 缺少 symbols").await;
        return;
    }
    let mut missing = Vec::new();
    {
        let mut subs = kconn.subs.lock().unwrap();
        for req in symbols {
            if state
                .kline
                .as_ref()
                .and_then(|k| k.book.lookup(&req))
                .is_none()
            {
                missing.push(req);
                continue;
            }
            subs.insert(req, exchange.to_string());
        }
    }
    if !missing.is_empty() {
        send_error(
            state,
            name,
            None,
            &format!("mock 无以下标的的 1m K线数据：{}", missing.join(",")),
        )
        .await;
    }
    // 有有效登记即启动聚合泵（首 tick 立即推——与旧逐 symbol 任务节奏一致）
    if kconn.pump.is_none() && !kconn.subs.lock().unwrap().is_empty() {
        kconn.pump = Some(spawn_kline_pump(
            Arc::clone(state),
            name.to_string(),
            token,
            Arc::clone(&kconn.subs),
        ));
    }
}

/// `unsubscribe_kline(s)`：从本连接登记集摘除；缺 symbols = 清空
/// （泵随之停推——对齐真实服务「最后连接退订后向交易所退订」）。
fn unsubscribe_kline(cmd: &serde_json::Value, kconn: &mut KlineConn) {
    let targets = extract_symbols(cmd);
    let mut subs = kconn.subs.lock().unwrap();
    if targets.is_empty() {
        subs.clear();
        return;
    }
    for s in targets {
        subs.remove(&s);
    }
}

/// `get_klines`（§10.4）：只读 mock 装载的 KlineBook（镜像真实服务
/// 「本地留存，不调交易所」）。应答 `kline_history` 帧：缺 start_time =
/// 窗口内最近 limit 根（向更早翻页），给了 start_time = 窗口内最早 limit 根；
/// 无数据回空数组（不是错误）；参数非法回 error 帧——错误 message 不含
/// 「K线」/「subscribe_kline」，保证客户端匹配链能命中在途 GetKlines。
async fn get_klines(state: &Arc<MockState>, name: &str, cmd: &serde_json::Value) {
    let Some(kr) = state.kline.as_ref() else {
        send_error(
            state,
            name,
            None,
            "mock 未装载历史 bar 数据（启动缺 --kline-dir）",
        )
        .await;
        return;
    };
    let Some(exchange) = cmd["exchange"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        send_error(state, name, None, "get_klines 缺少必填字段：exchange").await;
        return;
    };
    let symbol = ["symbol", "instrument"]
        .iter()
        .find_map(|k| cmd[*k].as_str().map(str::trim).filter(|s| !s.is_empty()));
    let Some(symbol) = symbol else {
        send_error(state, name, None, "get_klines 缺少必填字段：symbol").await;
        return;
    };
    if cmd
        .get("interval")
        .and_then(|v| v.as_str())
        .is_some_and(|raw| !normalize_interval(Some(raw)))
    {
        send_error(state, name, None, "仅支持 interval=1m").await;
        return;
    }
    let start = cmd["start_time"].as_i64();
    let end = cmd["end_time"].as_i64();
    if let (Some(s), Some(e)) = (start, end) {
        if s >= e {
            send_error(state, name, None, "start_time 必须小于 end_time").await;
            return;
        }
    }
    let limit = cmd["limit"]
        .as_u64()
        .filter(|l| *l > 0)
        .unwrap_or(300)
        .min(1500) as usize;
    let empty_frame = |start_v: serde_json::Value, end_v: serde_json::Value| {
        serde_json::json!({
            "type": "kline_history",
            "status": "OK",
            "exchange": exchange,
            "symbol": symbol,
            "interval": "1m",
            "start_time": start_v,
            "end_time": end_v,
            "has_more": false,
            "klines": [],
        })
    };
    let Some((_, bars)) = kr.book.lookup(symbol) else {
        // 无该标的留存 = 空结果（§10.4：不是错误）
        state
            .hub
            .send(
                name,
                empty_frame(serde_json::Value::Null, serde_json::Value::Null).to_string(),
            )
            .await;
        return;
    };
    let window: Vec<&Bar> = bars
        .iter()
        .filter(|b| start.is_none_or(|s| b.start_ms >= s) && end.is_none_or(|e| b.start_ms < e))
        .collect();
    let has_more = window.len() > limit;
    let page: Vec<&Bar> = if start.is_some() {
        window.iter().take(limit).copied().collect()
    } else {
        window.iter().rev().take(limit).rev().copied().collect()
    };
    if page.is_empty() {
        state
            .hub
            .send(
                name,
                empty_frame(
                    start
                        .map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null),
                    end.map(serde_json::Value::from)
                        .unwrap_or(serde_json::Value::Null),
                )
                .to_string(),
            )
            .await;
        return;
    }
    let frame = serde_json::json!({
        "type": "kline_history",
        "status": "OK",
        "exchange": exchange,
        "symbol": symbol,
        "interval": "1m",
        "start_time": page.first().unwrap().start_ms,
        "end_time": page.last().unwrap().start_ms + 60_000,
        "has_more": has_more,
        "klines": page
            .iter()
            .map(|b| b.bar_value(symbol, Some(exchange)))
            .collect::<Vec<_>>(),
    });
    state.hub.send(name, frame.to_string()).await;
}

/// 每连接的 K线订阅状态：`symbol（订阅方请求名）→ exchange` 登记集 +
/// 聚合泵句柄（首次有效订阅惰性启动，连接结束 abort）。
#[derive(Default)]
struct KlineConn {
    subs: Arc<StdMutex<HashMap<String, String>>>,
    pump: Option<JoinHandle<()>>,
}

/// 启动连接级 K线聚合泵：每个回放 tick 把订阅集中各 symbol 游标推进一根，
/// 按 exchange 分组各发一帧 `kline_1m_batch`（组内按 (symbol, start_time)
/// 升序；§10.3——真实服务端同所同分钟合帧，单根亦发 batch）。写失败/顶号
/// 即退出（游标未推进的 bar 不回放——真实服务对死连同样不补发）。
/// last_close（成交价参考）与游标仅在帧写成功后推进，与旧逐 symbol 任务同语义。
fn spawn_kline_pump(
    state: Arc<MockState>,
    name: String,
    token: u64,
    subs: Arc<StdMutex<HashMap<String, String>>>,
) -> JoinHandle<()> {
    let kr = Arc::clone(state.kline.as_ref().expect("subscribe 前置已检查"));
    tokio::spawn(async move {
        loop {
            let snapshot: Vec<(String, String)> = subs
                .lock()
                .unwrap()
                .iter()
                .map(|(s, e)| (s.clone(), e.clone()))
                .collect();
            let mut due: BTreeMap<String, Vec<(String, usize, Bar)>> = BTreeMap::new();
            for (sym, ex) in snapshot {
                let Some((_, bars)) = kr.book.lookup(&sym) else {
                    continue;
                };
                let idx = match state.replay_cursor.get(&(name.clone(), sym.clone())) {
                    Some(g) => *g + 1,
                    None => KlineBook::first_index_at_or_after(bars, kr.start_ms),
                };
                let Some(bar) = bars.get(idx).cloned() else {
                    continue;
                };
                due.entry(ex).or_default().push((sym, idx, bar));
            }
            for (ex, mut group) in due {
                group.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.2.start_ms.cmp(&b.2.start_ms)));
                let frame = serde_json::json!({
                    "type": "kline_1m_batch",
                    "exchange": ex,
                    "strategy_name": name,
                    "klines": group
                        .iter()
                        .map(|(sym, _, b)| b.bar_value(sym, Some(&ex)))
                        .collect::<Vec<_>>(),
                });
                if !state.hub.send_as(&name, token, frame.to_string()).await {
                    return; // 连接已换 token 或写失败：真实服务亦不再向死连补发
                }
                for (sym, idx, bar) in group {
                    state.last_close.insert(sym.clone(), bar.close);
                    state.last_close.insert(bar.file_symbol.clone(), bar.close);
                    state.replay_cursor.insert((name.clone(), sym), idx);
                }
            }
            if kr.interval > Duration::ZERO {
                tokio::time::sleep(kr.interval).await;
            }
        }
    })
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
    // 期货模式：整数手，与回测撮合同一换算（ContractSpec::sizing_lots）；
    // 一手都开不起 → REJECTED 且不建单（与下方“金额过小”同一拒单通道）。
    if let Some(c) = state.contract.as_ref() {
        let leverage = u32::try_from(cmd["leverage"].as_u64().unwrap_or(1)).unwrap_or(u32::MAX);
        let Some(lots) = c.sizing_lots(reference, cash, leverage) else {
            send_error(
                state,
                name,
                None,
                "order_cash 不足以开最小手数（保证金/杠杆约束）",
            )
            .await;
            return None;
        };
        return Some((lots as f64, false));
    }

    let converted = floor_lot(cash / reference);
    if converted <= 0.0 {
        send_error(state, name, None, "order_cash 换算数量非正（金额过小）").await;
        return None;
    }
    Some((converted, false))
}

/// spawn 独立任务，按节奏推 PARTIAL…FILLED（交易所式成交）。
/// 净持仓镜像（一 instrument 一条；模板策略是单方向净持仓，反手按重置简化）。
#[derive(Debug, Clone)]
struct PosMirror {
    /// 规范方向：BUY/SELL（大写，对齐协议回报口径）
    side: String,
    exchange: String,
    qty: f64,
    /// 加权开仓均价
    price: f64,
}

/// 方向入参归一（协议 §3.2 宽松枚举 → 规范大写）。
fn norm_side(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().as_str() {
        "long" | "buy" | "bid" | "多" => "BUY".to_string(),
        "short" | "sell" | "ask" | "空" => "SELL".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

/// FILLED 回报推进净持仓镜像：开仓加权累计；reduce_only 按数量扣减，
/// 归零即移除条目（空仓 = 应答空数组，与真实服务「全平后 positions 不含
/// 该 instrument」一致）。
fn apply_fill_to_book(state: &Arc<MockState>, rec: &OrderRec, fill_price: f64) {
    let side = norm_side(&rec.side);
    if rec.reduce_only {
        if let Some(mut m) = state.pos_book.get_mut(&rec.instrument) {
            m.qty = (m.qty - rec.quantity).max(0.0);
            if m.qty <= f64::EPSILON {
                drop(m);
                state.pos_book.remove(&rec.instrument);
            }
        }
        return;
    }
    match state.pos_book.get_mut(&rec.instrument) {
        Some(mut m) if m.qty > f64::EPSILON => {
            if m.side == side {
                let total = m.qty + rec.quantity;
                if total > f64::EPSILON {
                    m.price = (m.price * m.qty + fill_price * rec.quantity) / total;
                }
                m.qty = total;
            } else {
                // 反手开仓（模板不会发出；mock 简化为整体重置并保留可见性）
                m.side = side;
                m.qty = rec.quantity;
                m.price = fill_price;
            }
        }
        _ => {
            state.pos_book.insert(
                rec.instrument.clone(),
                PosMirror {
                    side,
                    exchange: rec.exchange.clone(),
                    qty: rec.quantity,
                    price: fill_price,
                },
            );
        }
    }
}

/// 交易所式成交：独立任务按节奏推 PARTIAL…FILLED；末段 FILLED 送完后
/// 推进净持仓镜像（供 get_positions 应答）。
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
        // 回报段送完：以订单终态推进净持仓镜像（fill_times>=1 恒达 FILLED）
        if let Some(r) = state.orders.get(&cid).map(|g| g.clone()) {
            if r.status == "FILLED" {
                apply_fill_to_book(&state, &r, price);
            }
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
            offset: None,
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

    /// 净持仓镜像：FILLED 开仓累计 → reduce_only 部分平 → 全平移除；
    /// get_positions 按 instrument+side 过滤，元素给策略对账消费的核心要素。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn position_mirror_tracks_fills_and_closes() {
        let (url, _srv) = spawn(MockScenario::new(SECRET)).await; // fill_times=1 → 即时 FILLED
        let mut ws = raw_connect(&url, "st_pos").await;
        let open = serde_json::json!({
            "option": "place_order", "exchange": "binance", "account": "a",
            "instrument": "BTCUSDT", "instrument_type": "futures", "side": "long",
            "order_type": "market", "quantity": "10", "signal_id": "pos_open",
        });
        ws.send(Message::Text(open.to_string())).await.unwrap();
        let _ack = next_json(&mut ws, "ack").await;
        let _filled = next_json(&mut ws, "order_event").await;
        let q =
            serde_json::json!({"option": "get_positions", "instrument": "BTCUSDT", "side": "long"});
        ws.send(Message::Text(q.to_string())).await.unwrap();
        let ev = next_json(&mut ws, "positions").await;
        let rows = ev["positions"].as_array().cloned().unwrap_or_default();
        assert_eq!(rows.len(), 1, "FILLED 开仓后应有仓");
        assert_eq!(rows[0]["side"], "BUY");
        assert_eq!(rows[0]["quantity"], 10.0);
        assert!(rows[0]["pos_price"].as_f64().unwrap() > 0.0);

        // 平半：reduce_only 反向 4 → 净仓 6
        let half = serde_json::json!({
            "option": "place_order", "exchange": "binance", "account": "a",
            "instrument": "BTCUSDT", "instrument_type": "futures", "side": "short",
            "order_type": "market", "quantity": "4", "reduce_only": true, "signal_id": "pos_half",
        });
        ws.send(Message::Text(half.to_string())).await.unwrap();
        let _ack2 = next_json(&mut ws, "ack").await;
        let _filled2 = next_json(&mut ws, "order_event").await;
        ws.send(Message::Text(q.to_string())).await.unwrap();
        let ev2 = next_json(&mut ws, "positions").await;
        let rows2 = ev2["positions"].as_array().cloned().unwrap_or_default();
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0]["quantity"], 6.0, "reduce_only 应净额扣减");

        // 全平：镜像条目移除 → 空数组；反向 side 过滤同样空
        let rest = serde_json::json!({
            "option": "place_order", "exchange": "binance", "account": "a",
            "instrument": "BTCUSDT", "instrument_type": "futures", "side": "short",
            "order_type": "market", "quantity": "6", "reduce_only": true, "signal_id": "pos_rest",
        });
        ws.send(Message::Text(rest.to_string())).await.unwrap();
        let _ack3 = next_json(&mut ws, "ack").await;
        let _filled3 = next_json(&mut ws, "order_event").await;
        ws.send(Message::Text(q.to_string())).await.unwrap();
        let ev3 = next_json(&mut ws, "positions").await;
        assert!(
            ev3["positions"]
                .as_array()
                .cloned()
                .unwrap_or_default()
                .is_empty(),
            "全平后应为空仓: {ev3}"
        );
    }

    /// get_order（§7）: query the snapshot by signal_id or client_order_id; returning error if not found.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_order_returns_snapshot_or_error() {
        let (url, _srv) = spawn(MockScenario::new(SECRET)).await;
        let (client, mut events) =
            TradingServiceClient::start_arc(client_cfg(url), Handle::current());
        let mut gw = TradingGateway::new(client, "st_q".into(), venue_for(100.0));
        let cid = gw
            .submit_fut(&intent("sig_q1", CoreSide::Long, IntentKind::Open))
            .await
            .unwrap();
        loop {
            let ev = tokio::time::timeout(Duration::from_secs(3), events.recv())
                .await
                .expect("成交回报应到达")
                .expect("通道");
            if let ClientEvent::OrderUpdate(u) = ev {
                if u.order_id == cid && u.state == OrderState::Filled {
                    break;
                }
            }
        }
        let snap = gw.query_fut("sig_q1").await.expect("get_order 应返回快照");
        assert_eq!(snap.state, OrderState::Filled);
        assert!(snap.filled_size > 0.0, "应回累计成交量");
        let err = gw.query_fut("sig_nope").await.unwrap_err();
        assert_eq!(
            err.kind,
            strategy_signals::gateway::FailureKind::Rejected,
            "查无单应 Rejected"
        );
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

    /// 1 手名义 = 110000 × 乘数 5 = 550000；order_cash 60 万、杠杆 1 → 杠杆上限 1.09 手 → 整数 1 手
    fn cu_contract() -> ContractSpec {
        use strategy_core::contract::{CommissionSpec, FeeMode};
        ContractSpec {
            name: Some("铜".into()),
            multiplier: 5.0,
            tick_size: 10.0,
            margin_rate: 0.11,
            min_lots: 1,
            session_opens: vec![],
            no_trade_after_open_minutes: 0,
            session_closes: vec![],
            no_trade_before_close_minutes: 0,
            commission: CommissionSpec {
                mode: FeeMode::PerLot,
                open: 5.0,
                close: 5.0,
                close_today: None,
            },
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mock_futures_open_fills_integer_lots() {
        let scenario = MockScenario::new(SECRET)
            .fill_price(110_000.0)
            .contract(Some(cu_contract()));
        let (url, _srv) = spawn(scenario).await;
        let mut ws = raw_connect(&url, "st_futures").await;
        ws.send(open_frame("sig_cu_open", "CU", 600_000.0, 1))
            .await
            .unwrap();
        let ack = next_json(&mut ws, "ack").await;
        assert_eq!(ack["order_quantity"], 1.0, "期货开仓必须整数手");
        let ev = next_json(&mut ws, "order_event").await;
        assert_eq!(ev["status"], "FILLED");
        assert_eq!(ev["filled_quantity"], 1.0, "成交回执 quantity 为整数手");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mock_futures_open_below_one_lot_is_rejected() {
        // order_cash 5 万 < 1 手名义 55 万（杠杆 1）→ sizing_lots=None → REJECTED，不建单
        let scenario = MockScenario::new(SECRET)
            .fill_price(110_000.0)
            .contract(Some(cu_contract()));
        let (url, _srv) = spawn(scenario).await;
        let mut ws = raw_connect(&url, "st_futures_small").await;
        ws.send(open_frame("sig_cu_small", "CU", 50_000.0, 1))
            .await
            .unwrap();
        let ev = next_json(&mut ws, "error").await;
        assert_eq!(ev["status"], "REJECTED");
    }

    fn open_frame(signal: &str, instrument: &str, order_cash: f64, leverage: u64) -> Message {
        Message::Text(
            serde_json::json!({
                "option": "place_order",
                "exchange": "binance",
                "instrument": instrument,
                "side": "long",
                "order_type": "market",
                "order_cash": order_cash.to_string(),
                "leverage": leverage,
                "signal_id": signal
            })
            .to_string(),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mock_crypto_math_unchanged_when_contract_none() {
        // 无 contract：开仓 quantity = order_cash / 参考价，floor 到 1e-6（小数口径不变）
        // 600000 / 100.5 = 5970.1492537… → 5970.149253
        let (url, _srv) = spawn(MockScenario::new(SECRET).fill_price(100.5)).await;
        let mut ws = raw_connect(&url, "st_crypto_pin").await;
        ws.send(open_frame("sig_crypto_pin", "BTCUSDT", 600_000.0, 1))
            .await
            .unwrap();
        let ack = next_json(&mut ws, "ack").await;
        assert_eq!(ack["order_quantity"], 5970.149253);
        let ev = next_json(&mut ws, "order_event").await;
        assert_eq!(ev["status"], "FILLED");
        assert_eq!(ev["filled_quantity"], 5970.149253);
    }

    // ── K线模拟回放 ─────────────────────────────────────────────────────

    /// 临时 1m CSV 目录：BTCUSDT 5 根（收盘 101..105）。
    fn tmp_kline_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("BTCUSDT_1m.csv"),
            "timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume\n\
             2025-12-01 13:00:00+00:00,100,101,99,101,10,1010,1,1,101\n\
             2025-12-01 13:01:00+00:00,101,102,100,102,11,1122,2,2,204\n\
             2025-12-01 13:02:00+00:00,102,103,101,103,12,1236,3,3,309\n\
             2025-12-01 13:03:00+00:00,103,104,102,104,13,1352,4,4,416\n\
             2025-12-01 13:04:00+00:00,104,105,103,105,14,1470,5,5,525\n",
        )
        .unwrap();
        dir
    }

    fn subscribe_frame(symbols: &[&str]) -> String {
        serde_json::json!({
            "option": "subscribe_kline",
            "exchange": "binance",
            "symbols": symbols,
            "interval": "1m",
        })
        .to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kline_subscribe_replays_final_bars_and_reports_missing() {
        let dir = tmp_kline_dir();
        let (scenario, errs) =
            MockScenario::new(SECRET).kline_dir(dir.path(), Duration::from_millis(20), None);
        assert!(errs.is_empty(), "装载错误清单应为空: {errs:?}");
        let (url, _srv) = spawn(scenario).await;
        let mut ws = raw_connect(&url, "st_kline_replay").await;

        ws.send(Message::Text(subscribe_frame(&["BTCUSDT", "NOPEUSDT"])))
            .await
            .unwrap();
        // 缺失标的先回一帧 error（订阅数据在 replay 前到达与否不定，
        // 用类型过滤收取：error 必至，且 message 点名缺失 symbol）。
        let mut saw_error = None;
        let mut bars = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while bars.len() < 5 || saw_error.is_none() {
            let remain = deadline - tokio::time::Instant::now();
            let Ok(Some(Ok(msg))) = tokio::time::timeout(remain, ws.next()).await else {
                break;
            };
            let v: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
            match v["type"].as_str().unwrap_or("") {
                "kline_1m_batch" => {
                    if let Some(arr) = v["klines"].as_array() {
                        bars.extend(arr.iter().cloned());
                    }
                }
                "error" => saw_error = Some(v),
                _ => {}
            }
        }
        let err = saw_error.expect("缺失 symbol 必须有 error 帧");
        assert!(
            err["message"].as_str().unwrap().contains("NOPEUSDT"),
            "error 应点名缺失标的: {err}"
        );
        assert_eq!(bars.len(), 5, "5 根定稿 bar 全部到达（batch 帧展开）");
        assert_eq!(bars[0]["symbol"], "BTCUSDT");
        assert_eq!(bars[0]["venue"], "binance");
        assert_eq!(bars[0]["interval"], "1m");
        assert_eq!(bars[0]["close"], "101", "decimal 字符串口径逐字保留");
        assert_eq!(bars[4]["close"], "105");
        assert_eq!(
            bars[0]["end_time"].as_i64().unwrap() - bars[0]["start_time"].as_i64().unwrap(),
            60_000
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kline_unsubscribe_stops_replay() {
        let dir = tmp_kline_dir();
        // 300ms/根 × 5 根 = 1.5s 供给；unsubscribe 后必须显著提前静默。
        let (scenario, _) =
            MockScenario::new(SECRET).kline_dir(dir.path(), Duration::from_millis(300), None);
        let (url, _srv) = spawn(scenario).await;
        let mut ws = raw_connect(&url, "st_kline_unsub").await;
        ws.send(Message::Text(subscribe_frame(&["BTCUSDT"])))
            .await
            .unwrap();
        let first = next_json(&mut ws, "kline_1m_batch").await;
        assert_eq!(first["type"], "kline_1m_batch");
        ws.send(Message::Text(
            serde_json::json!({"option": "unsubscribe_kline", "symbols": ["BTCUSDT"]}).to_string(),
        ))
        .await
        .unwrap();
        // 之后 700ms（>2 个回放周期）不得再有 kline_1m_batch 帧。
        let quiet = tokio::time::timeout(Duration::from_millis(700), async {
            loop {
                let Some(Ok(msg)) = ws.next().await else {
                    return;
                };
                let v: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
                if v["type"].as_str() == Some("kline_1m_batch") {
                    panic!("unsubscribe 后仍在回放: {v}");
                }
            }
        })
        .await;
        assert!(quiet.is_err(), "静默窗口超时 = 无 K线帧，符合预期");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kline_resubscribe_resumes_cursor_without_history() {
        let dir = tmp_kline_dir();
        let (scenario, _) =
            MockScenario::new(SECRET).kline_dir(dir.path(), Duration::from_millis(300), None);
        let (url, _srv) = spawn(scenario).await;

        // 连接一：收 1 根后退订并断开。
        let mut ws1 = raw_connect(&url, "st_kline_resume").await;
        ws1.send(Message::Text(subscribe_frame(&["BTCUSDT"])))
            .await
            .unwrap();
        let f = next_json(&mut ws1, "kline_1m_batch").await;
        assert_eq!(f["klines"][0]["close"], "101");
        ws1.send(Message::Text(
            serde_json::json!({"option": "unsubscribe_kline", "symbols": ["BTCUSDT"]}).to_string(),
        ))
        .await
        .unwrap();
        drop(ws1);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 同名重连再订阅：游标续推，从第 2 根（close=102）开始，不回放历史。
        let mut ws2 = raw_connect(&url, "st_kline_resume").await;
        ws2.send(Message::Text(subscribe_frame(&["BTCUSDT"])))
            .await
            .unwrap();
        let f2 = next_json(&mut ws2, "kline_1m_batch").await;
        assert_eq!(
            f2["klines"][0]["close"], "102",
            "重订必须从游标续推（对齐真实服务不补历史）"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fill_price_follows_last_replayed_close_symbol_price_wins() {
        let dir = tmp_kline_dir();
        let (scenario, _) =
            MockScenario::new(SECRET).kline_dir(dir.path(), Duration::from_millis(10), None);
        let (url, _srv) = spawn(scenario).await;
        let (client, mut events) =
            TradingServiceClient::start_arc(client_cfg(url), Handle::current());
        let mut kr = client.kline_updates();
        client
            .subscribe_klines("sar_supertrend", "binance", &["BTCUSDT".to_string()])
            .await
            .unwrap();
        // 吃满 5 根（close 101..105，流尽后 last_close 定格 105）再下单：
        // FILLED 均价必须等于最后回放收盘，而非全局 fill_price(100.5)。
        for _ in 0..5 {
            tokio::time::timeout(Duration::from_secs(3), kr.recv())
                .await
                .expect("K线流应在 3s 内到达")
                .expect("broadcast 未关闭");
        }
        let cid = client
            .submit_order(
                "sar_supertrend",
                &venue_for(60.0),
                &intent("sig_kline_fill", CoreSide::Long, IntentKind::Open),
            )
            .await
            .unwrap();
        let mut filled = None;
        while filled.is_none() {
            let ev = tokio::time::timeout(Duration::from_secs(3), events.recv())
                .await
                .expect("订单事件应到达")
                .expect("通道未关闭");
            if let ClientEvent::OrderUpdate(u) = ev {
                if u.order_id == cid && u.state == OrderState::Filled {
                    filled = Some(u.avg_price);
                }
            }
        }
        assert_eq!(filled.unwrap(), 105.0, "成交价应跟随最后回放收盘");

        // 显式 symbol_price 覆盖优先级仍在。
        let scenario2 = MockScenario::new(SECRET)
            .kline_dir(dir.path(), Duration::from_millis(10), None)
            .0
            .symbol_price("BTCUSDT", 999.0);
        let (url2, _srv2) = spawn(scenario2).await;
        let (client2, mut events2) =
            TradingServiceClient::start_arc(client_cfg(url2), Handle::current());
        let mut kr2 = client2.kline_updates();
        client2
            .subscribe_klines("sar_supertrend", "binance", &["BTCUSDT".to_string()])
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), kr2.recv())
            .await
            .expect("K线到达")
            .expect("通道");
        let cid2 = client2
            .submit_order(
                "sar_supertrend",
                &venue_for(60.0),
                &intent("sig_kline_override", CoreSide::Long, IntentKind::Open),
            )
            .await
            .unwrap();
        loop {
            let ev = events2.recv().await.expect("通道");
            if let ClientEvent::OrderUpdate(u) = ev {
                if u.order_id == cid2 && u.state == OrderState::Filled {
                    assert_eq!(u.avg_price, 999.0, "显式参考价必须优先于回放收盘");
                    break;
                }
            }
        }
    }

    /// 聚合泵形态：多 symbol 同 tick 合为一帧 `kline_1m_batch`（数组两根、
    /// 组内按 (symbol, start_time) 排序），顶层带 exchange/strategy_name（§10.3）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kline_batch_frames_aggregate_symbols() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("BTCUSDT_1m.csv"),
            "timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume\n\
             2025-12-01 13:00:00+00:00,100,101,99,101,10,1010,1,1,101\n\
             2025-12-01 13:01:00+00:00,101,102,100,102,11,1122,2,2,204\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("ETHUSDT_1m.csv"),
            "timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume\n\
             2025-12-01 13:00:00+00:00,200,201,199,201,20,4020,2,2,402\n\
             2025-12-01 13:01:00+00:00,201,202,200,202,21,4242,3,3,603\n",
        )
        .unwrap();
        let (scenario, errs) =
            MockScenario::new(SECRET).kline_dir(dir.path(), Duration::from_millis(30), None);
        assert!(errs.is_empty());
        let (url, _srv) = spawn(scenario).await;
        let mut ws = raw_connect(&url, "st_kline_batch").await;
        ws.send(Message::Text(subscribe_frame(&["BTCUSDT", "ETHUSDT"])))
            .await
            .unwrap();
        let f = next_json(&mut ws, "kline_1m_batch").await;
        assert_eq!(f["exchange"], "binance");
        let arr = f["klines"].as_array().cloned().unwrap_or_default();
        assert_eq!(arr.len(), 2, "同 tick 两 symbol 必须合为一帧");
        assert_eq!(
            arr[0]["symbol"], "BTCUSDT",
            "组内按 (symbol, start_time) 升序"
        );
        assert_eq!(arr[1]["symbol"], "ETHUSDT");
    }

    /// 命令别名（§3.1）+ exchange 必填校验（§10.1）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kline_alias_option_and_missing_exchange_error() {
        let dir = tmp_kline_dir();
        let (scenario, _) =
            MockScenario::new(SECRET).kline_dir(dir.path(), Duration::from_millis(20), None);
        let (url, _srv) = spawn(scenario).await;
        let mut ws = raw_connect(&url, "st_kline_alias").await;
        ws.send(Message::Text(
            serde_json::json!({"option": "subscribe_klines", "exchange": "binance", "symbols": ["BTCUSDT"], "interval": "1m"})
                .to_string(),
        ))
        .await
        .unwrap();
        let f = next_json(&mut ws, "kline_1m_batch").await;
        assert_eq!(f["klines"][0]["symbol"], "BTCUSDT");
        // 缺 exchange 的订阅帧 → error 帧点名必填字段
        ws.send(Message::Text(
            serde_json::json!({"option": "subscribe_kline", "symbols": ["BTCUSDT"]}).to_string(),
        ))
        .await
        .unwrap();
        let err = next_json(&mut ws, "error").await;
        assert!(
            err["message"].as_str().unwrap().contains("exchange"),
            "应提示缺 exchange: {err}"
        );
    }

    /// get_klines（§10.4）客户端端到端：缺省=最近 N 根 + has_more；
    /// end_time 排他翻页；无数据=空 OK；start>=end → error。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_klines_client_end_to_end() {
        let dir = tmp_kline_dir(); // 5 根 close 101..105（13:00..13:04）
        let (scenario, _) =
            MockScenario::new(SECRET).kline_dir(dir.path(), Duration::from_millis(20), None);
        let (url, _srv) = spawn(scenario).await;
        let (client, _ev) = TradingServiceClient::start_arc(client_cfg(url), Handle::current());
        let h = tokio::time::timeout(
            Duration::from_secs(5),
            client.get_klines("st_get_klines", "binance", "BTCUSDT", None, None, Some(2)),
        )
        .await
        .expect("kline_history 应答应在 5s 内到达")
        .expect("get_klines 成功");
        assert_eq!(h.bars.len(), 2, "缺 start_time = 取最近 2 根");
        assert_eq!(h.bars[0].close, 104.0, "升序");
        assert_eq!(h.bars[1].close, 105.0);
        assert!(h.has_more, "更早侧还有 3 根应报 has_more");
        // 翻页：以本页最早 bar 的 start_time 为排他上界续取更早
        let h2 = client
            .get_klines(
                "st_get_klines",
                "binance",
                "BTCUSDT",
                None,
                Some(h.bars[0].start_ms),
                Some(2),
            )
            .await
            .unwrap();
        assert_eq!(h2.bars[0].close, 102.0);
        assert_eq!(h2.bars[1].close, 103.0);
        // 未知 symbol = 空结果非错误（§10.4）
        let h3 = client
            .get_klines("st_get_klines", "binance", "DOGEUSDT", None, None, Some(2))
            .await
            .unwrap();
        assert!(h3.bars.is_empty() && !h3.has_more);
        // 参数非法（start >= end）→ error 帧 → 调用方 Err
        let bad = client
            .get_klines(
                "st_get_klines",
                "binance",
                "BTCUSDT",
                Some(10),
                Some(5),
                Some(2),
            )
            .await;
        assert!(bad.is_err(), "start_time >= end_time 应报错");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_kline_without_kline_dir_errors() {
        let (url, _srv) = spawn(MockScenario::new(SECRET)).await;
        let mut ws = raw_connect(&url, "st_kline_none").await;
        ws.send(Message::Text(subscribe_frame(&["BTCUSDT"])))
            .await
            .unwrap();
        let err = next_json(&mut ws, "error").await;
        assert_eq!(err["type"], "error");
        assert!(
            err["message"].as_str().unwrap().contains("--kline-dir"),
            "应提示 mock 未装载 K线: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_kline_updates_map_single_and_batch_frames() {
        // 脚本化服务端：手工推 kline_1m 与 kline_1m_batch，验证客户端 broadcast
        // 出形（含 decimal 字符串→f64、批量拆单根）。
        let dir = tmp_kline_dir();
        let book = KlineBook::load(dir.path()).0;
        let bars = book.lookup("BTCUSDT").unwrap().1.clone();
        let single = bars[0].clone();
        let batch = vec![bars[1].clone(), bars[2].clone()];
        // spawn_raw 的脚本按连接反复调用（Fn）：捕获在闭包入口克隆进 future。
        let (url, _h) = spawn_raw(move |mut ws: MockConn| {
            let single = single.clone();
            let batch = batch.clone();
            async move {
                // 收下订阅帧（不解析细节，只确认到达）
                let _ = ws.next().await;
                let f = single.frame("BTCUSDT", Some("binance"));
                ws.send(Message::Text(f.to_string())).await.unwrap();
                let frame = serde_json::json!({
                    "type": "kline_1m_batch",
                    "venue": "binance",
                    "klines": batch.iter().map(|b| b.frame("BTCUSDT", Some("binance"))["kline"].clone()).collect::<Vec<_>>(),
                });
                ws.send(Message::Text(frame.to_string())).await.unwrap();
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        })
        .await;
        let (client, _ev) = TradingServiceClient::start_arc(client_cfg(url), Handle::current());
        let mut kr = client.kline_updates();
        client
            .subscribe_klines("sar_supertrend", "binance", &["BTCUSDT".to_string()])
            .await
            .unwrap();
        let u1 = tokio::time::timeout(Duration::from_secs(3), kr.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(u1.symbol, "BTCUSDT");
        assert_eq!(u1.close, 101.0, "decimal 字符串应解析为 f64");
        assert_eq!(u1.trade_num, 1);
        let u2 = tokio::time::timeout(Duration::from_secs(3), kr.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(u2.close, 102.0, "batch 应拆为逐根有序事件");
        let u3 = tokio::time::timeout(Duration::from_secs(3), kr.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(u3.close, 103.0);
    }
}
