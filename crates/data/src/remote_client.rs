//! zkware klines-service 客户端
//!
//! 协议以 cta-strategy-code/data_manager 为参考实现：
//! - 历史回填：`GET {root}/api/v1/klines?symbol=&interval=&limit=&startTime=&endTime=`
//!   （时间范围查询，毫秒 epoch），响应为币安风格裸二维数组；
//!   亦兼容 `{ "data": [[...]] }` 包裹。
//! - 实时订阅：进程级一条 WS，连上后发一帧 `{"action":"subscribe","symbols":[...]}`
//!   订阅启动集合全部 symbols；服务端推 `{"type":"kline","symbol":..,"data":{..}}`
//!   （1m），`is_final=true` 为已闭合，否则为未闭合快照。共享分发器按 symbol
//!   扇出：每 symbol 一条 broadcast 通道（容量 `channel_cap`），慢 receiver 由
//!   Lagged 自愈，无 receiver 时静默丢弃。
//! - 断线指数退避重连（1→2→4…≤上限秒），每次（重）连成功广播
//!   [`KlineEvent::Reconnected`]，调用方据此做历史补缺。
//!
//! 契约见 contracts/kline-service.md。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::Value;
use strategy_core::candle::{Candle, Timeframe};
use tokio::sync::broadcast;
use tokio::time::Instant;
use tracing::{debug, trace, warn};

use crate::client::{KlineBar, KlineClient, KlineEvent, KlineStream};

#[derive(Debug, Clone)]
pub struct KlineServiceConfig {
    pub backfill_url: String,
    pub ws_url: String,
    pub reconnect_backoff_max_secs: u64,
    /// 每 symbol 广播通道容量
    pub channel_cap: usize,
    /// WS 应用层 Ping 心跳间隔（秒）：市场静默期靠它确认连接仍存活
    pub ping_interval_secs: u64,
    /// 读空闲看门狗：该秒数内收不到**任何**帧（含 Pong）→ 判定半开死连
    pub read_idle_timeout_secs: u64,
}

impl Default for KlineServiceConfig {
    fn default() -> Self {
        Self {
            backfill_url: "https://zkwarefr.zkware.cn".into(),
            ws_url: "wss://zkwarefr.zkware.cn/ws/klines".into(),
            reconnect_backoff_max_secs: 120,
            channel_cap: 100,
            ping_interval_secs: 15,
            read_idle_timeout_secs: 30,
        }
    }
}

#[derive(Clone)]
pub struct RemoteKlineServiceClient {
    cfg: KlineServiceConfig,
    http: reqwest::Client,
    handle: tokio::runtime::Handle,
    /// symbol → 扇出通道（连接任务持 sender 写入；attach 只 subscribe）
    fanout: HashMap<String, broadcast::Sender<KlineEvent>>,
}

impl RemoteKlineServiceClient {
    /// 启动共享分发器：为每个 symbol 建一条 broadcast 通道并 spawn 进程唯一
    /// 连接任务（一条 WS、一帧订阅全部 symbols；退避重连；按 symbol 扇出）。
    /// 返回 Arc，可直接作 `Arc<dyn KlineClient>` 使用。
    pub fn start(
        symbols: &[String],
        cfg: KlineServiceConfig,
        handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
        debug!(?symbols, ws_url = %cfg.ws_url, backfill_url = %cfg.backfill_url, "K线共享分发器启动：一条 WS 订阅全部 symbol + 按 symbol 扇出");
        let mut fanout = HashMap::new();
        for s in symbols {
            let (tx, _) = broadcast::channel(cfg.channel_cap.max(1));
            fanout.insert(s.clone(), tx);
        }
        let client = Arc::new(Self {
            cfg: cfg.clone(),
            http: reqwest::Client::new(),
            handle: handle.clone(),
            fanout,
        });
        let conn = Arc::clone(&client);
        let symbols = symbols.to_vec();
        handle.spawn(async move {
            conn.connection_loop(symbols).await;
        });
        client
    }

    /// 连接任务：一条 WS 循环。每次（重）连成功 → 广播 Reconnected（先于数据帧，
    /// 触发各订阅者补缺）+ 重发订阅帧；帧按 symbol 扇出。
    /// `broadcast::send` 不阻塞：慢 consumer 由其自身 Lagged 自愈；
    /// 无 receiver（Actor 代间）静默丢弃。
    async fn connection_loop(&self, symbols: Vec<String>) {
        let mut attempt: u32 = 0;
        loop {
            debug!(attempt, url = %self.cfg.ws_url, "K线 WS 正在连接");
            match tokio_tungstenite::connect_async(&self.cfg.ws_url).await {
                Ok((ws, _)) => {
                    attempt = 0;
                    debug!("K线 WS 连接成功，广播 Reconnected（触发各订阅者补缺）并重发订阅帧");
                    for (symbol, tx) in &self.fanout {
                        if tx.send(KlineEvent::Reconnected).is_err() {
                            trace!(%symbol, "Reconnected 无 receiver，静默");
                        }
                    }
                    let (mut sink, mut source) = ws.split();
                    let sub = serde_json::to_string(&SubscribeMsg {
                        action: "subscribe",
                        symbols: &symbols,
                    })
                    .unwrap_or_default();
                    debug!(%sub, "订阅帧已发送（一帧含全部 symbols）");
                    if sink
                        .send(tokio_tungstenite::tungstenite::Message::Text(sub))
                        .await
                        .is_err()
                    {
                        warn!("K线 WS 订阅帧发送失败，1s 后重连");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    // 心跳：interval 首 tick 立即就绪，先消费掉再进循环
                    // （对齐 trading_service 的看门狗模式）。
                    let ping_dur = Duration::from_secs(self.cfg.ping_interval_secs.max(1));
                    let mut ping_interval = tokio::time::interval(ping_dur);
                    ping_interval.tick().await;
                    // 读空闲看门狗：每收到一帧就 reset；触发即强制断开重连。
                    let idle_dur = Duration::from_secs(self.cfg.read_idle_timeout_secs.max(1));
                    let mut idle_timeout = Box::pin(tokio::time::sleep(idle_dur));

                    loop {
                        tokio::select! {
                            item = source.next() => {
                                let Some(result) = item else {
                                    warn!("K线 WS 读循环结束（连接断开），进入退避重连");
                                    break;
                                };
                                // 任何入站帧（文本/Pong/服务端 Ping）都证明连接
                                // 双向存活，看门狗无条件复位。
                                idle_timeout
                                    .as_mut()
                                    .reset(Instant::now() + idle_dur);
                                let msg = match result {
                                    Ok(msg) => msg,
                                    Err(e) => {
                                        warn!(error = %e, "K线 WS 读取错误，进入退避重连");
                                        break;
                                    }
                                };
                                if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                                    trace!(frame = %text, "K线 WS 收到原始文本帧");
                                    match serde_json::from_str::<InMessage>(&text) {
                                        Ok(InMessage::Kline(p)) => {
                                            let Some(bar) = p.data.into_bar(p.symbol) else {
                                                warn!(payload = %text, "kline 帧缺字段，丢弃");
                                                continue;
                                            };
                                            let Some(tx) = self.fanout.get(&bar.symbol) else {
                                                debug!(symbol = %bar.symbol, "未订阅 symbol 的帧，丢弃");
                                                continue;
                                            };
                                            let closed = bar.candle.is_closed;
                                            let sym = bar.symbol.clone();
                                            let bt = bar.candle.open_time;
                                            let ev = if closed {
                                                KlineEvent::Closed(bar)
                                            } else {
                                                KlineEvent::Tick(bar)
                                            };
                                            match tx.send(ev) {
                                                Ok(n) => debug!(
                                                    symbol = sym,
                                                    bar_time = %bt,
                                                    kind = if closed { "闭合" } else { "未闭合快照" },
                                                    receivers = n + 1,
                                                    "K线帧已按 symbol 扇出投递"
                                                ),
                                                Err(tokio::sync::broadcast::error::SendError(_)) => {
                                                    debug!(
                                                        symbol = sym,
                                                        bar_time = %bt,
                                                        "K线帧扇出无 receiver，静默丢弃"
                                                    );
                                                }
                                            }
                                        }
                                        Ok(InMessage::Subscribed { symbols }) => {
                                            debug!(?symbols, "kline 订阅已确认（服务端回执）");
                                        }
                                        Err(e) => warn!(error = %e, "kline 坏消息"),
                                    }
                                }
                            }
                            _ = ping_interval.tick() => {
                                if sink
                                    .send(tokio_tungstenite::tungstenite::Message::Ping(
                                        Vec::new(),
                                    ))
                                    .await
                                    .is_err()
                                {
                                    warn!("K线 WS Ping 发送失败，进入退避重连");
                                    break;
                                }
                                debug!("K线 WS：已发送 Ping 心跳（Pong 未归时读空闲看门狗将强制重连）");
                            }
                            _ = idle_timeout.as_mut() => {
                                warn!(
                                    idle_secs = idle_dur.as_secs(),
                                    "K线 WS：读空闲超时（TCP 在但无任何帧，判定半开死连），强制断开重连"
                                );
                                break;
                            }
                        }
                    }
                }
                Err(e) => warn!(error = %e, "klines-service 连接失败"),
            }
            attempt += 1;
            let delay = 1u64
                .checked_shl(attempt.saturating_sub(1))
                .unwrap_or(self.cfg.reconnect_backoff_max_secs)
                .min(self.cfg.reconnect_backoff_max_secs);
            debug!(attempt, delay_secs = delay, "K线 WS 退避等待后重连");
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
    }
}

/// 订阅消息：`{"action":"subscribe","symbols":["BTCUSDT"]}`
#[derive(Debug, serde::Serialize)]
struct SubscribeMsg<'a> {
    action: &'static str,
    symbols: &'a [String],
}

/// 服务端推送（按外层 `type` 区分：kline 被消费，subscribed 为订阅确认）
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InMessage {
    // KlinePayload 约 376 字节，装箱避免与小变体的尺寸差（clippy::large_enum_variant）
    Kline(Box<KlinePayload>),
    /// 订阅确认：`{"type":"subscribed","symbols":["BTCUSDT"]}`
    Subscribed {
        symbols: Vec<String>,
    },
}

/// `{ "type":"kline", "symbol":可选, "data":{..} }`
#[derive(Debug, Deserialize)]
struct KlinePayload {
    /// 外层 symbol（data 内缺省时使用）
    symbol: Option<String>,
    data: KlineData,
}

/// 单根 K 线（币安风格字段；价格为数字或数字字符串）
#[derive(Debug, Deserialize)]
struct KlineData {
    /// 开盘时间（毫秒 epoch；兼容字符串）
    start_time: Value,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    open: Value,
    #[serde(default)]
    high: Value,
    #[serde(default)]
    low: Value,
    #[serde(default)]
    close: Value,
    #[serde(default)]
    volume: Value,
    #[serde(default, alias = "quoteVolume")]
    quote_volume: Value,
    #[serde(default, alias = "count", alias = "trade_num")]
    trade_num: Value,
    #[serde(default, alias = "taker_buy_base", alias = "activeBuyVolume")]
    active_buy_volume: Value,
    #[serde(default, alias = "taker_buy_quote", alias = "activeBuyQuoteVolume")]
    active_buy_quote_volume: Value,
    /// 是否闭合（缺省按闭合处理，兼容只推闭合 K 线的服务）
    #[serde(default = "default_true")]
    is_final: bool,
}

fn default_true() -> bool {
    true
}

/// 周期 → 服务端 interval 串（1m / 15m / 1h / 4h / 1d，其余 {n}m）
fn tf_str(tf: Timeframe) -> String {
    match tf.0 {
        1 => "1m".into(),
        60 => "1h".into(),
        240 => "4h".into(),
        1440 => "1d".into(),
        n => format!("{n}m"),
    }
}

/// JSON 数字或数字字符串 → f64；null/NaN/非法 → 0.0（对齐 Python _safe_float）
fn f64_or_zero(v: &Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .filter(|f| f.is_finite())
        .unwrap_or(0.0)
}

/// JSON 数字/数字字符串/浮点 → u64；null/非法 → 0（成交笔数）
fn u64_or_zero(v: &Value) -> u64 {
    v.as_u64()
        .or_else(|| v.as_f64().map(|f| f as u64))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0)
}

/// 毫秒 epoch（数字或数字字符串）→ UTC 时间；无法解析返回 None
fn ms_to_utc(v: &Value) -> Option<DateTime<Utc>> {
    let ms: i64 = v
        .as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))?;
    Utc.timestamp_millis_opt(ms).single()
}

impl KlineData {
    /// 转 KlineBar；symbol 取 data 内或外层；时间/价格非法则丢弃（None）
    fn into_bar(self, outer_symbol: Option<String>) -> Option<KlineBar> {
        let symbol = self.symbol.or(outer_symbol)?;
        let open_time = ms_to_utc(&self.start_time)?;
        Some(KlineBar {
            symbol,
            candle: Candle {
                open_time,
                open: f64_or_zero(&self.open),
                high: f64_or_zero(&self.high),
                low: f64_or_zero(&self.low),
                close: f64_or_zero(&self.close),
                volume: f64_or_zero(&self.volume),
                quote_volume: f64_or_zero(&self.quote_volume),
                trade_num: u64_or_zero(&self.trade_num),
                active_buy_volume: f64_or_zero(&self.active_buy_volume),
                active_buy_quote_volume: f64_or_zero(&self.active_buy_quote_volume),
                is_closed: self.is_final,
            },
        })
    }
}

/// 币安风格数组行 → Candle（位序见模块头）；时间非法返回 None。
/// 扩展列缺失（行仅 6 列）按 0 处理。
fn binance_row(row: &[Value]) -> Option<Candle> {
    if row.len() < 6 {
        return None;
    }
    let get = |i: usize| row.get(i);
    let open_time = ms_to_utc(&row[0])?;
    Some(Candle {
        open_time,
        open: f64_or_zero(&row[1]),
        high: f64_or_zero(&row[2]),
        low: f64_or_zero(&row[3]),
        close: f64_or_zero(&row[4]),
        volume: f64_or_zero(&row[5]),
        quote_volume: get(7).map(f64_or_zero).unwrap_or(0.0),
        trade_num: get(8).map(u64_or_zero).unwrap_or(0),
        active_buy_volume: get(9).map(f64_or_zero).unwrap_or(0.0),
        active_buy_quote_volume: get(10).map(f64_or_zero).unwrap_or(0.0),
        is_closed: true,
    })
}

/// 回填响应 → Candle 列表：裸二维数组或 `{data:[[..]]}` 包裹，升序
fn parse_backfill(json: Value) -> Vec<Candle> {
    let rows = match json {
        Value::Array(rows) => rows,
        Value::Object(mut map) => match map.remove("data") {
            Some(Value::Array(rows)) => rows,
            _ => return Vec::new(),
        },
        _ => return Vec::new(),
    };
    rows.iter()
        .filter_map(|r| r.as_array().and_then(|arr| binance_row(arr)))
        .collect()
}

impl KlineClient for RemoteKlineServiceClient {
    fn backfill<'a>(
        &'a self,
        symbol: &'a str,
        timeframe: Timeframe,
        bars: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<Candle>>> + Send + 'a>>
    {
        Box::pin(async move {
            // 端口语义是“最近 bars 根”：用 now 反推时间范围（服务端按 startTime/endTime 查询）
            let now = Utc::now().timestamp_millis();
            let span_ms = (bars.max(1) as i64) * (timeframe.0 as i64) * 60_000;
            let url = format!(
                "{}/api/v1/klines",
                self.cfg.backfill_url.trim_end_matches('/')
            );
            debug!(
                symbol, %timeframe, limit = bars,
                start_ms = now - span_ms, end_ms = now,
                "HTTP backfill 请求：最近 {} 根 {}", bars, timeframe
            );
            let json: Value = self
                .http
                .get(url)
                .query(&[
                    ("symbol", symbol.to_string()),
                    ("interval", tf_str(timeframe)),
                    ("limit", bars.to_string()),
                    ("startTime", (now - span_ms).to_string()),
                    ("endTime", now.to_string()),
                ])
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let candles = parse_backfill(json);
            debug!(
                symbol,
                n = candles.len(),
                "HTTP backfill 返回，已解析为 K 线"
            );
            Ok(candles)
        })
    }

    fn attach(&self, symbol: &str, timeframe: Timeframe) -> KlineStream {
        debug_assert_eq!(
            timeframe,
            Timeframe::M1,
            "klines-service 只推送 1m，大周期由本地 Feed 聚合"
        );
        match self.fanout.get(symbol) {
            Some(tx) => {
                debug!(symbol, "注册式 attach：订阅该 symbol 的扇出通道");
                KlineStream { rx: tx.subscribe() }
            }
            None => {
                warn!(
                    symbol,
                    "attach 未在启动集合的 symbol，返回空流（防御编程错误）"
                );
                let (tx, rx) = broadcast::channel(1);
                // 常驻空任务持有 sender：流永不收帧、永不关闭
                self.handle.spawn(async move {
                    let _keepalive = tx;
                    std::future::pending::<()>().await;
                });
                KlineStream { rx }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message;

    /// 脚本帧间节奏 + 门控窗口：先推的帧间各隔一拍；门控信号 fire 后再隔
    /// 一拍推末帧。测试在"前 n-1 帧已消费丢弃、末帧未推"的窗口内 attach，
    /// 确定复现"只收到最后一帧"（attach 时机由信号门控，不依赖握手墙钟余量）。
    const FRAME_PACE_MS: u64 = 500;

    /// 脚本化 klines-service：逐连接服务（校验并记录订阅帧 → 推脚本帧 → 关闭连接）。
    /// 订阅帧收集进 subscribe_frames 供测试断言（服务 task 内 panic 不传播给测试）。
    /// 每连接推完除末帧外的全部帧、再隔一拍后 fire `sent` 门控（末帧在其后
    /// 一拍才推）：测试等待门控再 attach，可确定只收到末帧。
    async fn spawn_scripted_ws(
        scripts: Vec<Vec<Value>>,
    ) -> (
        String,
        Arc<Mutex<Vec<Value>>>,
        tokio::task::JoinHandle<()>,
        Arc<tokio::sync::Notify>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let subscribe_frames = Arc::new(Mutex::new(Vec::new()));
        let recorded = subscribe_frames.clone();
        let sent = Arc::new(tokio::sync::Notify::new());
        let gate = sent.clone();
        let h = tokio::spawn(async move {
            for frames in scripts {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let sub = ws.next().await.unwrap().unwrap().into_text().unwrap();
                recorded
                    .lock()
                    .unwrap()
                    .push(serde_json::from_str(&sub).unwrap());
                let n = frames.len();
                // 推前 n-1 帧（帧间保持节奏）；此刻客户端已消费并静默丢弃
                // （无 receiver），且末帧尚未推送
                for (i, f) in frames.iter().take(n.saturating_sub(1)).enumerate() {
                    if i > 0 {
                        tokio::time::sleep(Duration::from_millis(FRAME_PACE_MS)).await;
                    }
                    ws.send(Message::Text(f.to_string())).await.unwrap();
                }
                if n > 1 {
                    // 末帧前保留原帧间节奏的一拍，确保前 n-1 帧已扇出后
                    // 再 fire 门控（fire 紧跟末帧前的最后一拍）
                    tokio::time::sleep(Duration::from_millis(FRAME_PACE_MS)).await;
                }
                sent.notify_waiters();
                if let Some(f) = frames.last() {
                    if n > 1 {
                        // 门控 → 末帧窗口：测试在此 attach
                        tokio::time::sleep(Duration::from_millis(FRAME_PACE_MS)).await;
                    }
                    ws.send(Message::Text(f.to_string())).await.unwrap();
                }
                // drop(ws) → 连接关闭，触发客户端退避重连
            }
        });
        (format!("ws://{addr}/ws/klines"), subscribe_frames, h, gate)
    }

    fn kline_frame(symbol: &str, close: f64, is_final: bool) -> Value {
        serde_json::json!({
            "type":"kline","symbol":symbol,
            "data":{"start_time":1781049600000_i64,"symbol":symbol,
                "open":100.0,"high":101.0,"low":99.0,"close":close,
                "volume":1.0,"is_final":is_final}
        })
    }

    fn cfg_with(url: String) -> KlineServiceConfig {
        KlineServiceConfig {
            backfill_url: "http://127.0.0.1:1".into(),
            ws_url: url,
            ..Default::default()
        }
    }

    /// 多 symbol 扇出：BTC 流只收到自己的 Reconnected/Tick/Closed，无 ETH 帧
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_symbol_fanout_receives_own_frames() {
        let (url, subs, _srv, _gate) = spawn_scripted_ws(vec![vec![
            kline_frame("BTCUSDT", 100.5, false),
            kline_frame("ETHUSDT", 200.5, true),
            kline_frame("BTCUSDT", 101.0, true),
        ]])
        .await;
        let client = RemoteKlineServiceClient::start(
            &["BTCUSDT".into(), "ETHUSDT".into()],
            cfg_with(url),
            tokio::runtime::Handle::current(),
        );
        let mut btc = client.attach("BTCUSDT", Timeframe::M1);
        let mut eth = client.attach("ETHUSDT", Timeframe::M1);

        // 订阅帧单帧含全部 symbols
        tokio::time::sleep(Duration::from_millis(100)).await;
        let frames = subs.lock().unwrap().clone();
        assert_eq!(frames.len(), 1, "只应有一条连接一帧订阅");
        let syms: Vec<String> = frames[0]["symbols"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|s| s.as_str().map(str::to_string))
            .collect();
        assert_eq!(syms, vec!["BTCUSDT", "ETHUSDT"]);

        // BTC 收到自己的 Reconnected + Tick + Closed（恰 3 个，无 ETH 帧）
        let kind = |e: &KlineEvent| match e {
            KlineEvent::Closed(b) => ("Closed", b.symbol.clone(), b.candle.close),
            KlineEvent::Tick(b) => ("Tick", b.symbol.clone(), b.candle.close),
            KlineEvent::Reconnected => ("Reconnected", String::new(), 0.0),
        };
        let mut btc_kinds = Vec::new();
        for _ in 0..3 {
            let ev = match tokio::time::timeout(Duration::from_secs(2), btc.rx.recv()).await {
                Ok(Ok(ev)) => ev,
                Ok(Err(e)) => panic!("BTC 事件流意外关闭: {e}"),
                Err(_) => panic!("BTC 事件不足（应恰为 3 个）"),
            };
            btc_kinds.push(kind(&ev));
        }
        assert_eq!(
            btc_kinds,
            vec![
                ("Reconnected", String::new(), 0.0),
                ("Tick", "BTCUSDT".into(), 100.5),
                ("Closed", "BTCUSDT".into(), 101.0),
            ],
            "BTC 流不得混入 ETH 帧"
        );
        // ETH：Reconnected + 自己的 Closed
        let e0 = tokio::time::timeout(Duration::from_secs(2), eth.rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(e0, KlineEvent::Reconnected));
        let e1 = tokio::time::timeout(Duration::from_secs(2), eth.rx.recv())
            .await
            .unwrap()
            .unwrap();
        match e1 {
            KlineEvent::Closed(b) => assert_eq!(b.symbol, "ETHUSDT"),
            other => panic!("期望 ETH 闭合帧，实际 {other:?}"),
        }
    }

    /// 同 symbol 多 consumer：两个 receiver 都收到 Reconnected + Closed
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_attaches_same_symbol_both_receive() {
        let (url, _subs, _srv, _gate) =
            spawn_scripted_ws(vec![vec![kline_frame("BTCUSDT", 101.0, true)]]).await;
        let client = RemoteKlineServiceClient::start(
            &["BTCUSDT".into()],
            cfg_with(url),
            tokio::runtime::Handle::current(),
        );
        let mut a = client.attach("BTCUSDT", Timeframe::M1);
        let mut b = client.attach("BTCUSDT", Timeframe::M1);
        // 每个 receiver 恰 2 个事件（Reconnected + Closed）：收到 Closed 即停，
        // 不读第 3 个（broadcast 无第 3 个事件）；超时/关闭都 break 由末尾断言兜底
        for rx in [&mut a.rx, &mut b.rx] {
            let mut saw_closed = false;
            for _ in 0..3 {
                match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
                    Ok(Ok(KlineEvent::Closed(c))) => {
                        assert_eq!(c.symbol, "BTCUSDT");
                        saw_closed = true;
                        break;
                    }
                    Ok(Ok(_)) => {}
                    _ => break,
                }
            }
            assert!(saw_closed, "每个 receiver 都应收到闭合帧");
        }
    }

    /// 重连重发订阅帧 + 每次（重）连成功广播 Reconnected
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_resends_subscribe_and_broadcasts_reconnected() {
        // 两个连接脚本：第一次服务后关闭 → 客户端退避重连 → 第二次服务
        let (url, subs, _srv, _gate) = spawn_scripted_ws(vec![
            vec![kline_frame("BTCUSDT", 101.0, true)],
            vec![kline_frame("BTCUSDT", 102.0, true)],
        ])
        .await;
        let client = RemoteKlineServiceClient::start(
            &["BTCUSDT".into(), "ETHUSDT".into()],
            cfg_with(url),
            tokio::runtime::Handle::current(),
        );
        let mut s = client.attach("BTCUSDT", Timeframe::M1);

        // 第二次连接后：订阅帧再次发出且 symbols 完整
        for _ in 0..50 {
            if subs.lock().unwrap().len() >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let frames = subs.lock().unwrap().clone();
        assert_eq!(frames.len(), 2, "重连后应重发订阅帧");
        for f in frames.iter() {
            let syms: Vec<String> = f["symbols"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect();
            assert_eq!(syms, vec!["BTCUSDT", "ETHUSDT"]);
        }

        // 两次连接成功 → 两个 Reconnected + 两根闭合帧
        let mut reconnects = 0;
        let mut closed = 0;
        for _ in 0..6 {
            match tokio::time::timeout(Duration::from_secs(5), s.rx.recv()).await {
                Ok(Ok(KlineEvent::Reconnected)) => reconnects += 1,
                Ok(Ok(KlineEvent::Closed(_))) => closed += 1,
                Ok(Ok(KlineEvent::Tick(_))) => {}
                Ok(Err(_)) | Err(_) => break,
            }
        }
        assert!(
            reconnects >= 2,
            "每次（重）连成功都广播 Reconnected，实际 {reconnects}"
        );
        assert_eq!(closed, 2);
    }

    /// 半开死连自愈：服务端接受连接后永久静默（TCP 在但无帧），读空闲看门狗
    /// 必须在 idle 超时后强制断开退避重连——第二连接的闭合帧应正常到达。
    /// 复现 09-25 实盘事故：行情 WS 裸 read 循环在半开 TCP 上永久挂起。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn half_open_silence_forces_reconnect() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
        let addr = listener.local_addr().unwrap();
        let sub_count = Arc::new(AtomicUsize::new(0));
        let server_subs = Arc::clone(&sub_count);
        let closed_frame = kline_frame("BTCUSDT", 101.0, true);
        // 连接 1（accept 必须在独立任务：客户端在 server 就位后才启动）：
        // 读完订阅帧后停止读取（不处理 Ping、不回 Pong），常驻仅持有连接——
        // 真半开：客户端 Ping 无应答、无任何帧。
        let conn1_subs = Arc::clone(&server_subs);
        let conn1_listener = Arc::clone(&listener);
        tokio::spawn(async move {
            let (tcp, _) = conn1_listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            assert!(ws.next().await.is_some(), "应先收到订阅帧");
            conn1_subs.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
        });

        // 连接 2（看门狗触发退避重连后到达）：正常服务，推一根闭合帧
        let conn2_listener = Arc::clone(&listener);
        tokio::spawn(async move {
            let (tcp, _) = conn2_listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            assert!(ws.next().await.is_some(), "应先收到订阅帧");
            server_subs.fetch_add(1, Ordering::SeqCst);
            ws.send(Message::Text(closed_frame.to_string()))
                .await
                .unwrap();
        });

        let client = RemoteKlineServiceClient::start(
            &["BTCUSDT".into()],
            KlineServiceConfig {
                backfill_url: "http://127.0.0.1:1".into(),
                ws_url: format!("ws://{addr}/ws/klines"),
                ping_interval_secs: 1,
                read_idle_timeout_secs: 1,
                ..Default::default()
            },
            tokio::runtime::Handle::current(),
        );
        let mut s = client.attach("BTCUSDT", Timeframe::M1);

        // 看门狗（idle=1s）+ 1s 退避 → 5s 内必须发生第二次连接
        let reconnected = tokio::time::timeout(Duration::from_secs(5), async {
            while sub_count.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(reconnected.is_ok(), "半开静默连接必须被看门狗强制重连");

        // 第二连接：Reconnected + 闭合帧到达
        let mut reconnects = 0u32;
        let mut got_closed = false;
        for _ in 0..4 {
            match tokio::time::timeout(Duration::from_secs(2), s.rx.recv()).await {
                Ok(Ok(KlineEvent::Reconnected)) => reconnects += 1,
                Ok(Ok(KlineEvent::Closed(_))) => {
                    got_closed = true;
                    break;
                }
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => break,
            }
        }
        // attach 晚于首连接的 Reconnected（broadcast 不重放历史，由
        // reconnect_resends... 测试覆盖），此处断言重连广播 + 闭合帧：
        assert!(
            reconnects >= 1,
            "重连成功后应广播 Reconnected，实际 {reconnects}"
        );
        assert!(got_closed, "重连后应收到第二连接的闭合帧");
    }

    /// 未知 symbol 空流：不收帧也不关闭（超时而非 Closed）
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn attach_unknown_symbol_returns_inert_stream() {
        let client = RemoteKlineServiceClient::start(
            &["BTCUSDT".into()],
            cfg_with("ws://127.0.0.1:1/ws/klines".into()),
            tokio::runtime::Handle::current(),
        );
        let mut s = client.attach("DOGEUSDT", Timeframe::M1);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), s.rx.recv())
                .await
                .is_err(),
            "未知 symbol 流不应收到帧，也不应关闭（超时而非 Closed）"
        );
    }

    /// 无 receiver 期间帧静默丢弃（B1 语义移植）：broadcast 不重放历史，
    /// Reconnected 亦不重放；attach 晚于前两帧扇出、早于最后一帧，
    /// 只收到最后一根闭合帧。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frames_with_no_receiver_are_dropped_silently() {
        // 先推 2 根帧（此刻无 receiver），再 attach，后续帧仍正常到达
        let (url, _subs, _srv, sent) = spawn_scripted_ws(vec![vec![
            kline_frame("BTCUSDT", 1.5, false),
            kline_frame("BTCUSDT", 2.0, true),
            kline_frame("BTCUSDT", 2.5, true),
        ]])
        .await;
        let client = RemoteKlineServiceClient::start(
            &["BTCUSDT".into()],
            cfg_with(url),
            tokio::runtime::Handle::current(),
        );
        // 信号门控（替代握手墙钟余量）：服务端推完除末帧外的全部帧、隔一拍后
        // fire 门控——此刻前两帧已无 receiver 静默丢弃（Reconnected 同样
        // 不重放），且末帧尚未推送；在此 attach，随后只收到最后一根闭合帧。
        sent.notified().await;
        let mut s = client.attach("BTCUSDT", Timeframe::M1);
        let ev = tokio::time::timeout(Duration::from_secs(2), s.rx.recv())
            .await
            .expect("attach 后应收到最后一根闭合帧")
            .expect("事件流不应关闭");
        match ev {
            KlineEvent::Closed(b) => {
                assert!(b.candle.is_closed);
                assert_eq!(b.candle.close, 2.5);
            }
            other => panic!("期望最后一根闭合帧，实际 {other:?}"),
        }
    }

    /// 极简 HTTP mock：返回币安风格裸二维数组，并断言真实请求形状
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backfill_parses_binance_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 2048];
            let n = tcp.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..n]);
            assert!(
                req.starts_with("GET /api/v1/klines?"),
                "{}",
                &req[..60.min(req.len())]
            );
            assert!(req.contains("interval=1m"));
            assert!(req.contains("symbol=BTCUSDT"));
            assert!(req.contains("startTime=") && req.contains("endTime="));
            // 两根币安行（毫秒时间、字符串价格）
            let body = serde_json::json!([
                [
                    1781049600000_i64,
                    "1.0",
                    "2.0",
                    "0.5",
                    "1.5",
                    "3.0",
                    1781049659999_i64,
                    "4.5",
                    10,
                    "1.5",
                    "3.0",
                    "0"
                ],
                [
                    1781049660000_i64,
                    "1.5",
                    "2.5",
                    "1.0",
                    "2.0",
                    "4.0",
                    1781049719999_i64,
                    "6.0",
                    12,
                    "2.0",
                    "4.0",
                    "0"
                ]
            ])
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            tcp.write_all(response.as_bytes()).await.unwrap();
        });

        let client = RemoteKlineServiceClient::start(
            &[],
            KlineServiceConfig {
                backfill_url: format!("http://{addr}"),
                ws_url: "ws://127.0.0.1:1".into(),
                ..Default::default()
            },
            tokio::runtime::Handle::current(),
        );
        let rows = client.backfill("BTCUSDT", Timeframe::M1, 2).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!((rows[1].close - 2.0).abs() < 1e-9);
        assert_eq!(
            rows[0].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn parse_backfill_accepts_data_wrapper_and_skips_bad_rows() {
        let wrapped: Value = serde_json::json!({"data":[
            [1781049600000_i64,"1","2","0.5","1.5","3",1781049659999_i64,"x",1,"y","z","0"],
            ["bad-ts","1","2","0.5","1.5","3",0,"x",1,"y","z","0"]
        ]});
        assert_eq!(parse_backfill(wrapped).len(), 1);
        assert_eq!(parse_backfill(serde_json::json!([])).len(), 0);
    }
}
