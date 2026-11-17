//! TradingServiceClient —— trading-service 的 WS 客户端（真实 SouthCommand/ClientEvent 协议）
//!
//! 拓扑：一个客户端实例持有「策略类型名 → 一条认证 WS 连接」的懒连接池；
//! 同策略类型的多个符号 live actor 实例复用一条连接（真实服务 1 连接绑定 1 策略名）。
//! - 每次（重）连接：随机 nonce + HMAC-SHA256(secret, strategy_name+nonce)，
//!   query 携带 strategy_name/nonce/signature；
//! - place_order 开仓/平仓同帧（平仓方向已由 core 反向）；ack(NEW) 返回
//!   服务端生成的 client_order_id；order_event(PARTIAL/FILLED) 走事件流；
//! - get_order 真实查询：query_fut 发 option=get_order 按 signal_id 读订单
//!   快照（服务端自动限定握手策略名），应答帧 type=order；查无/越权回
//!   error；超时 Retryable；
//! - get_positions 仓位对账：positions_fut 发 option=get_positions 按
//!   instrument 读 trading-service 仓位视图，应答帧 type=positions；
//!   空数组=无仓；venue 非法回 error；超时 Retryable；
//! - get_klines 历史回填：[`TradingServiceClient::get_klines`] 发 option=get_klines
//!   读服务端本地留存 1m bar（内存保留窗+归档 CSV，协议 §10.4），应答帧
//!   type=kline_history（升序，has_more 翻页）；无数据=空数组非错误；超时 Retryable；
//! - submit_enabled=false（默认）本地即拒，绝不发真实下单请求；
//! - **K线流**（协议 v2 `subscribe_kline` → `kline_1m(_batch)` 推送）：
//!   [`TradingServiceClient::subscribe_klines`] 按 **(策略类型, exchange)** 分组
//!   登记订阅（每次（重）连接**逐组**重发订阅帧——同策略跨交易所不再互相顶掉）；
//!   定稿 bar 走**独立 broadcast 通道**
//!   [`TradingServiceClient::kline_updates`]，不占订单事件 mpsc——
//!   高频回放不会反压订单路径，消费落后由 broadcast::Lagged 自愈。

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use strategy_core::order::OrderState;
use strategy_core::{Side, SignalIntent};
use strategy_signals::gateway::{
    AsyncOrderGateway, OrderPlace, OrderSnapshot, PositionsFuture, QueryFuture, RemotePosition,
    SubmitError, SubmitFuture,
};
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, trace, warn};

use crate::auth;
use crate::wire::{self, InEvent, PlaceOrderClose, PlaceOrderOpen, WireKline};

#[derive(Debug, Clone)]
pub struct TradingConfig {
    pub ws_url: String,
    pub request_timeout: Duration,
    /// 安全开关：false 时所有下单本地拒绝（默认，防误下真单）
    pub submit_enabled: bool,
    pub reconnect_backoff_max_secs: u64,
    /// HMAC SECRET_KEY
    pub strategy_secret: String,
}

impl Default for TradingConfig {
    fn default() -> Self {
        Self {
            ws_url: "ws://127.0.0.1:9100/ws".into(),
            request_timeout: Duration::from_secs(5),
            submit_enabled: false,
            reconnect_backoff_max_secs: 120,
            strategy_secret: String::new(),
        }
    }
}

/// 每策略实例的下单场所/资金配置（来自 config/{strategy}/{SYMBOL}.yaml）。
/// 开仓帧与平仓帧的 exchange/account 均以此为准；身份 strategy_name 不入帧。
#[derive(Debug, Clone)]
pub struct OrderVenueCfg {
    /// 目标交易所实例名（服务端 registry 键）
    pub exchange: String,
    /// 账户名
    pub account: String,
    /// 杠杆倍数
    pub leverage: u32,
    /// 每单名义金额（USDT）→ 开仓帧 order_cash
    pub order_cash: f64,
}

/// 服务端推送事件（订单更新流 / 重连通知，均带策略身份）
#[derive(Debug, Clone, PartialEq)]
pub enum ClientEvent {
    OrderUpdate(OrderUpdate),
    Reconnected { strategy_name: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderUpdate {
    /// 服务端 client_order_id（0x+32hex）
    pub order_id: String,
    /// 交易所返回的订单号（回调帧 exchange_order_id）
    pub exchange_order_id: Option<String>,
    pub state: OrderState,
    pub filled_size: f64,
    pub avg_price: f64,
    /// 成交量原始字面量（回调 filled_quantity 逐字保留，平仓帧 quantity 用）
    pub filled_quantity_raw: Option<String>,
    pub strategy_name: Option<String>,
    /// 帧内 signal_id：回报早于本地 ack/绑定时，按它找回信号（不丢弃）
    pub signal_id: Option<String>,
    /// 帧内 message：Rejected 时的交易所原文（外壳识别 ReduceOnly 拒单）
    pub message: Option<String>,
    /// 实际委托要素（受理回写，审计核对真实下单参数）
    pub place: Option<OrderPlace>,
    /// 真实手续费（阶段 4 v2.7，§13-4「FillEvent.fee 不再恒 0」）：
    /// 服务端升级后 FILLED/PARTIAL 帧回传；None = 未回传（下游 0 兜底）。
    pub fee: Option<f64>,
}

/// 连接任务在途命令类别（决定应答帧按哪条路径分派）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CmdKind {
    /// place_order：应答帧 ack/error，关联键 signal_id
    Submit,
    /// get_order：应答帧 order/error，关联键 signal_id
    GetOrder,
    /// get_positions：应答帧 positions/error，关联键 instrument
    GetPositions,
    /// get_klines：应答帧 kline_history/error，关联键 symbol（帧顶层回声）
    GetKlines,
    /// subscribe_kline / unsubscribe_kline：无请求-应答关联（推送走
    /// kline broadcast；订阅失败由 error 帧在途匹配之外的通道反馈）
    Notify,
}

/// 在途应答通道：按命令类别携带不同回包
enum CmdReply {
    Submit(oneshot::Sender<Result<String, SubmitError>>),
    Order(oneshot::Sender<Result<OrderSnapshot, SubmitError>>),
    Positions(oneshot::Sender<Result<Vec<RemotePosition>, SubmitError>>),
    Klines(oneshot::Sender<Result<KlineHistory, SubmitError>>),
    /// Notify 类命令：无人等待应答
    None,
}

/// 一条在途请求（应答帧回声关联键后精确匹配）
struct PendingEntry {
    kind: CmdKind,
    key: String,
    reply: CmdReply,
}

struct WsCommand {
    payload: serde_json::Value,
    kind: CmdKind,
    /// 关联键：Submit=signal_id；GetOrder=client_order_id
    key: String,
    reply: CmdReply,
}

/// K线定稿推送（`kline_1m`/`kline_1m_batch` 帧的统一出形；只增不改订单流）。
#[derive(Debug, Clone, PartialEq)]
pub struct KlineUpdate {
    pub venue: String,
    pub symbol: String,
    /// 桶开始时间（UTC 毫秒，含）
    pub start_ms: i64,
    pub end_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_num: u64,
    pub active_buy_volume: f64,
    pub active_buy_quote_volume: f64,
}

impl WireKline {
    /// 线帧 → 本地 KlineUpdate；缺关键字段（时间戳非数）返回 None 并告警。
    fn into_update(self) -> Option<KlineUpdate> {
        let (Some(start_ms), Some(end_ms)) = (self.start_time, self.end_time) else {
            warn!(symbol = %self.symbol, "kline 帧缺 start_time/end_time，丢弃");
            return None;
        };
        Some(KlineUpdate {
            venue: self.venue.unwrap_or_default(),
            symbol: self.symbol,
            start_ms,
            end_ms,
            open: wire::opt_f64(&self.open),
            high: wire::opt_f64(&self.high),
            low: wire::opt_f64(&self.low),
            close: wire::opt_f64(&self.close),
            volume: wire::opt_f64(&self.volume),
            quote_volume: wire::opt_f64(&self.quote_volume),
            trade_num: wire::opt_f64(&self.trade_num) as u64,
            active_buy_volume: wire::opt_f64(&self.active_buy_volume),
            active_buy_quote_volume: wire::opt_f64(&self.active_buy_quote_volume),
        })
    }
}

/// get_klines 应答（协议 §10.4 `kline_history` 帧）：升序定稿 bar + 翻页标记。
#[derive(Debug, Clone, PartialEq)]
pub struct KlineHistory {
    pub bars: Vec<KlineUpdate>,
    /// true = 服务端还有更早数据（用本批最早 bar 的 start_time 作 end_time 续翻）
    pub has_more: bool,
}

struct Inner {
    cfg: TradingConfig,
    handle: Handle,
    event_tx: mpsc::Sender<ClientEvent>,
    conns: DashMap<String, mpsc::Sender<WsCommand>>,
    /// K线 broadcast 出口（与订单 mpsc 隔离；容量见 start_arc）
    kline_tx: broadcast::Sender<KlineUpdate>,
    /// (策略类型, exchange) → K线订阅 symbol 集（重连时逐组各发一帧）。
    /// 键里必须带 exchange：曾因登记只按策略类型单值 exchange，同策略跨
    /// 交易所后订者覆盖先订者，重连后先订交易所的 K 线永久断流。
    kline_subs: DashMap<(String, String), Vec<String>>,
}

pub struct TradingServiceClient {
    inner: Arc<Inner>,
}

impl TradingServiceClient {
    /// 启动客户端（连接按需懒建立），返回共享句柄与订单/重连事件的唯一消费口。
    pub fn start_arc(
        cfg: TradingConfig,
        handle: Handle,
    ) -> (Arc<Self>, mpsc::Receiver<ClientEvent>) {
        let (event_tx, event_rx) = mpsc::channel(256);
        // K线 broadcast：加速回放一帧一根，容量放大到 4096 吸收短时突发；
        // 仍落后则 Lagged（消费方按既有 Lagged 自愈路径补数据）。
        let (kline_tx, _) = broadcast::channel(4096);
        let inner = Arc::new(Inner {
            cfg,
            handle,
            event_tx,
            conns: DashMap::new(),
            kline_tx,
            kline_subs: DashMap::new(),
        });
        (Arc::new(Self { inner }), event_rx)
    }

    /// 取（或懒建）某策略类型的连接命令通道。
    fn conn_for(&self, strategy_type: &str) -> mpsc::Sender<WsCommand> {
        self.inner
            .conns
            .entry(strategy_type.to_string())
            .or_insert_with(|| {
                let (cmd_tx, cmd_rx) = mpsc::channel(256);
                let inner = Arc::clone(&self.inner);
                self.inner
                    .handle
                    .spawn(connection_loop(inner, strategy_type.to_string(), cmd_rx));
                cmd_tx
            })
            .clone()
    }

    /// 登记并立即发送 `subscribe_kline` 订阅帧（协议 v2：symbols + interval=1m）。
    /// 同 (策略类型, exchange) 重复调用做 symbol 并集；不同 exchange 各自成组
    /// 互不覆盖；断线重连由连接任务按登记**逐组**重发。
    /// K线帧经 [`Self::kline_updates`] 收取，不占订单事件通道。
    pub async fn subscribe_klines(
        &self,
        strategy_type: &str,
        exchange: &str,
        symbols: &[String],
    ) -> Result<(), SubmitError> {
        if symbols.is_empty() {
            return Ok(());
        }
        let key = (strategy_type.to_string(), exchange.to_string());
        {
            let mut entry = self.inner.kline_subs.entry(key.clone()).or_default();
            for s in symbols {
                if !entry.iter().any(|e| e == s) {
                    entry.push(s.clone());
                }
            }
        }
        let syms = self
            .inner
            .kline_subs
            .get(&key)
            .map(|g| g.clone())
            .unwrap_or_default();
        let payload = TradingServiceClient::subscribe_frame(exchange, &syms);
        let conn = self.conn_for(strategy_type);
        conn.send(WsCommand {
            payload,
            kind: CmdKind::Notify,
            key: String::new(),
            reply: CmdReply::None,
        })
        .await
        .map_err(|_| SubmitError::retryable("连接任务已关闭"))
    }

    /// 订阅帧构形（首订与重连重发共用；协议 §10.1）。
    fn subscribe_frame(exchange: &str, symbols: &[String]) -> serde_json::Value {
        serde_json::json!({
            "option": "subscribe_kline",
            "exchange": exchange,
            "instrument_type": "futures",
            "symbols": symbols,
            "interval": "1m",
        })
    }

    /// 某策略类型全部订阅登记 → 每 exchange 各一帧（重连/周期重订共用）。
    fn subs_frames_for(
        kline_subs: &DashMap<(String, String), Vec<String>>,
        strategy_type: &str,
    ) -> Vec<serde_json::Value> {
        kline_subs
            .iter()
            .filter(|e| e.key().0 == strategy_type)
            .map(|e| TradingServiceClient::subscribe_frame(&e.key().1, e.value()))
            .collect()
    }

    /// K线定稿推送接收端（broadcast：每个 receiver 独立游标，落后 Lagged 自愈；
    /// drop 即取消，不影响其他消费者）。
    pub fn kline_updates(&self) -> broadcast::Receiver<KlineUpdate> {
        self.inner.kline_tx.subscribe()
    }

    /// 异步下单：发 place_order → 等 ack，成功返回服务端 client_order_id。
    /// 帧形判定（阶段 4 v2.7）：
    /// - `intent.offset` 显式给出（SPI live 桥路径）：**按 offset 定帧形**——
    ///   `open` ⇒ 开仓帧（order_cash+leverage），其余 ⇒ 平仓帧（quantity 精确
    ///   取成交回报量，reduce_only 恒 true）；`offset` 字段原样落帧，
    ///   「实盘订单帧带平今/平昨」验收由此承重（§13-4）。
    /// - `intent.offset == None`（旧链路）：维持 `position_id` 隐式判定，
    ///   帧内不出现 offset 字段——旧服务端行为逐字节不变（零回归）。
    pub async fn submit_order(
        &self,
        strategy_type: &str,
        venue: &OrderVenueCfg,
        intent: &SignalIntent,
    ) -> Result<String, SubmitError> {
        if !self.inner.cfg.submit_enabled {
            debug!(
                strategy_type,
                signal_id = intent.id,
                "订单：submit_enabled=false，本地拦截，未发送 place_order"
            );
            return Err(SubmitError::rejected(
                "submit_enabled=false：本地拦截，未发送",
            ));
        }
        let side = match intent.direction {
            Side::Long => "long",
            Side::Short => "short",
        };
        let offset = intent.offset.as_deref();
        let is_close = match offset {
            Some("open") => false,
            Some("close") | Some("close_today") | Some("close_yesterday") => true,
            // 过渡口径：auto/未知标签退回隐式判定（不猜，保守按 position_id）
            _ => intent.position_id.is_some(),
        };
        if offset
            .is_some_and(|o| !matches!(o, "open" | "close" | "close_today" | "close_yesterday"))
        {
            warn!(
                signal_id = intent.id,
                offset = offset.unwrap(),
                "未知 offset 标签，回落 position_id 隐式判定"
            );
        }
        let payload = if is_close {
            let quantity = intent.size;
            if quantity <= 0.0 {
                return Err(SubmitError::rejected(
                    "平仓数量非法：缺少成交回报 filled_quantity 来源",
                ));
            }
            // 优先用成交回报原始字面量，保证数量不被浮点往返修改
            let qty_raw = intent
                .size_raw
                .clone()
                .unwrap_or_else(|| quantity.to_string());
            serde_json::to_value(PlaceOrderClose {
                option: "place_order",
                exchange: &venue.exchange,
                account: &venue.account,
                instrument: &intent.symbol,
                instrument_type: "futures",
                side,
                order_type: "market",
                quantity: &qty_raw,
                reduce_only: true,
                signal_id: &intent.id,
                offset,
            })
        } else {
            let order_cash = venue.order_cash;
            if order_cash <= 0.0 {
                return Err(SubmitError::rejected("下单名义金额 order_cash 非法（<=0）"));
            }
            let cash_raw = order_cash.to_string();
            serde_json::to_value(PlaceOrderOpen {
                option: "place_order",
                exchange: &venue.exchange,
                account: &venue.account,
                instrument: &intent.symbol,
                instrument_type: "futures",
                side,
                order_type: "market",
                order_cash: &cash_raw,
                leverage: venue.leverage,
                signal_id: &intent.id,
                offset,
            })
        }
        .expect("place_order 帧序列化恒成立");
        debug!(
            strategy_type,
            signal_id = intent.id,
            frame = if is_close { "close" } else { "open" },
            exchange = %venue.exchange,
            account = %venue.account,
            instrument = %intent.symbol,
            %payload,
            "订单：准备提交 place_order（完整下单参数如上，经策略类型连接池）"
        );

        let (reply_tx, reply_rx) = oneshot::channel();
        let conn = self.conn_for(strategy_type);
        conn.send(WsCommand {
            payload,
            kind: CmdKind::Submit,
            key: intent.id.clone(),
            reply: CmdReply::Submit(reply_tx),
        })
        .await
        .map_err(|_| SubmitError::retryable("连接任务已关闭"))?;

        match tokio::time::timeout(self.inner.cfg.request_timeout, reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(SubmitError::retryable("应答通道关闭（连接断开）")),
            Err(_) => Err(SubmitError::retryable("ack 等待超时")),
        }
    }

    /// 查询订单：发 get_order（按 signal_id）→ 等 type=order 应答，
    /// 返回订单快照（活动/当日终态/跨日历史由服务端三级查询，对本端透明；
    /// signal_id 查询由服务端自动限定在握手策略名下）。
    /// 查无/越权返回 Rejected；超时/断线返回 Retryable。
    pub async fn get_order(
        &self,
        strategy_type: &str,
        venue: &OrderVenueCfg,
        signal_id: &str,
    ) -> Result<OrderSnapshot, SubmitError> {
        let payload = serde_json::json!({
            "option": "get_order",
            "exchange": venue.exchange,
            "account": venue.account,
            "signal_id": signal_id,
        });
        debug!(
            strategy_type, signal_id, %payload,
            "订单：准备提交 get_order（经策略类型连接池）"
        );

        let (reply_tx, reply_rx) = oneshot::channel();
        let conn = self.conn_for(strategy_type);
        conn.send(WsCommand {
            payload,
            kind: CmdKind::GetOrder,
            key: signal_id.to_string(),
            reply: CmdReply::Order(reply_tx),
        })
        .await
        .map_err(|_| SubmitError::retryable("连接任务已关闭"))?;

        match tokio::time::timeout(self.inner.cfg.request_timeout, reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(SubmitError::retryable("应答通道关闭（连接断开）")),
            Err(_) => Err(SubmitError::retryable("order 应答等待超时")),
        }
    }

    /// 仓位对账：发 get_positions（按 instrument 精确筛选期货仓）→ 等
    /// type=positions 应答，返回 trading-service 仓位视图（与交易所同步
    /// 由服务端负责）。无仓为 Vec::new()（不是错误）；
    /// venue/参数非法返回 Rejected；超时/断线返回 Retryable。
    pub async fn get_positions(
        &self,
        strategy_type: &str,
        venue: &OrderVenueCfg,
        instrument: &str,
        side: &str,
    ) -> Result<Vec<RemotePosition>, SubmitError> {
        let payload = serde_json::json!({
            "option": "get_positions",
            "exchange": venue.exchange,
            "account": venue.account,
            "instrument_type": "futures",
            "instrument": instrument,
            // 带方向筛选：只查本地持仓方向的仓位
            "side": side,
        });
        debug!(
            strategy_type, instrument, %payload,
            "仓位：准备提交 get_positions（经策略类型连接池）"
        );

        let (reply_tx, reply_rx) = oneshot::channel();
        let conn = self.conn_for(strategy_type);
        conn.send(WsCommand {
            payload,
            kind: CmdKind::GetPositions,
            key: instrument.to_string(),
            reply: CmdReply::Positions(reply_tx),
        })
        .await
        .map_err(|_| SubmitError::retryable("连接任务已关闭"))?;

        match tokio::time::timeout(self.inner.cfg.request_timeout, reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(SubmitError::retryable("应答通道关闭（连接断开）")),
            Err(_) => Err(SubmitError::retryable("positions 应答等待超时")),
        }
    }

    /// 历史 K 线回填：发 get_klines（§10.4，读服务端本地留存）→ 等
    /// type=kline_history 应答。start/end 缺省 = 取最近 `limit` 根（升序）；
    /// 无数据返回空 bars（不是错误）；参数非法回 Rejected；超时/断线 Retryable。
    pub async fn get_klines(
        &self,
        strategy_type: &str,
        exchange: &str,
        symbol: &str,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        limit: Option<u32>,
    ) -> Result<KlineHistory, SubmitError> {
        let mut payload = serde_json::json!({
            "option": "get_klines",
            "exchange": exchange,
            "symbol": symbol,
        });
        if let Some(s) = start_ms {
            payload["start_time"] = serde_json::json!(s);
        }
        if let Some(e) = end_ms {
            payload["end_time"] = serde_json::json!(e);
        }
        if let Some(l) = limit {
            payload["limit"] = serde_json::json!(l);
        }
        debug!(
            strategy_type, symbol, %payload,
            "K线：准备提交 get_klines（历史回填，经策略类型连接池）"
        );
        let (reply_tx, reply_rx) = oneshot::channel();
        let conn = self.conn_for(strategy_type);
        conn.send(WsCommand {
            payload,
            kind: CmdKind::GetKlines,
            key: symbol.to_string(),
            reply: CmdReply::Klines(reply_tx),
        })
        .await
        .map_err(|_| SubmitError::retryable("连接任务已关闭"))?;
        match tokio::time::timeout(self.inner.cfg.request_timeout, reply_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(SubmitError::retryable("应答通道关闭（连接断开）")),
            Err(_) => Err(SubmitError::retryable("kline_history 应答等待超时")),
        }
    }
}

/// live 信号 Actor 持有的 async 网关；构造时绑定策略类型（连接池键）与
/// 该策略实例的下单场所/资金配置。
pub struct TradingGateway {
    client: Arc<TradingServiceClient>,
    strategy_type: String,
    venue: OrderVenueCfg,
    /// 实例级下单闸门（mode == "live" 才 true）：与 `TradingConfig.submit_enabled`
    /// 组成双保险，paper 装配绝不放行真单。
    allow_submit: bool,
}

impl TradingGateway {
    pub fn new(
        client: Arc<TradingServiceClient>,
        strategy_type: String,
        venue: OrderVenueCfg,
    ) -> Self {
        Self::with_submit(client, strategy_type, venue, true)
    }

    /// 带实例闸门的构造：`allow_submit=false` 时 `submit_fut` 本地拒绝。
    pub fn with_submit(
        client: Arc<TradingServiceClient>,
        strategy_type: String,
        venue: OrderVenueCfg,
        allow_submit: bool,
    ) -> Self {
        Self {
            client,
            strategy_type,
            venue,
            allow_submit,
        }
    }
}

impl AsyncOrderGateway for TradingGateway {
    fn submit_fut<'a>(&'a mut self, intent: &'a SignalIntent) -> SubmitFuture<'a> {
        Box::pin(async move {
            if !self.allow_submit {
                return Err(SubmitError::rejected("非 live 模式：本地拦截，未发送"));
            }
            self.client
                .submit_order(&self.strategy_type, &self.venue, intent)
                .await
        })
    }

    fn query_fut<'a>(&'a mut self, signal_id: &'a str) -> QueryFuture<'a> {
        // 真实 get_order：经共享连接发 option=get_order，按 signal_id 读订单快照
        Box::pin(async move {
            self.client
                .get_order(&self.strategy_type, &self.venue, signal_id)
                .await
        })
    }

    fn positions_fut<'a>(&'a mut self, instrument: &'a str, side: &'a str) -> PositionsFuture<'a> {
        // 真实 get_positions：经共享连接读 trading-service 仓位视图
        Box::pin(async move {
            self.client
                .get_positions(&self.strategy_type, &self.venue, instrument, side)
                .await
        })
    }
}

// ── 连接任务 ────────────────────────────────────────────────────────────────

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect(cfg: &TradingConfig, strategy_type: &str) -> Result<WsStream, String> {
    // 每次连接一次性 nonce（重连也是新 nonce）。
    let nonce = auth::new_nonce();
    let signature = auth::sign(&cfg.strategy_secret, strategy_type, &nonce);
    let url = auth::connect_url(&cfg.ws_url, strategy_type, &nonce, &signature);
    debug!(strategy_type, ws_url = %cfg.ws_url, "订单：正在建立策略类型认证 WS（新 nonce + HMAC 握手；签名不入日志）");
    match tokio_tungstenite::connect_async(url).await {
        Ok((ws, _)) => {
            debug!(strategy_type, "订单：trading-service WS 已连接");
            Ok(ws)
        }
        Err(e) => Err(e.to_string()),
    }
}

async fn connection_loop(
    inner: Arc<Inner>,
    strategy: String,
    mut cmd_rx: mpsc::Receiver<WsCommand>,
) {
    let mut attempt: u32 = 0;
    loop {
        match connect(&inner.cfg, &strategy).await {
            Ok(ws) => {
                attempt = 0;
                if inner
                    .event_tx
                    .send(ClientEvent::Reconnected {
                        strategy_name: strategy.clone(),
                    })
                    .await
                    .is_err()
                {
                    return; // 事件消费端关闭 → 退出
                }
                debug!(strategy = %strategy, "订单：已通知 Reconnected（T7 将下发对账）");
                if run_connection(
                    ws,
                    &mut cmd_rx,
                    &inner.event_tx,
                    &inner.kline_tx,
                    &inner.kline_subs,
                    &strategy,
                    PING_INTERVAL,
                    READ_IDLE_TIMEOUT,
                    RESUBSCRIBE_INTERVAL,
                )
                .await
                {
                    debug!(strategy = %strategy, "订单：连接任务退出（命令通道关闭）");
                    return; // 命令通道关闭 → 退出
                }
                debug!(strategy = %strategy, "订单：连接断开，进入退避重连");
            }
            Err(e) => {
                warn!(error = %e, strategy = %strategy, "trading-service 连接失败");
            }
        }
        attempt += 1;
        let delay = 1u64
            .checked_shl(attempt.saturating_sub(1))
            .unwrap_or(inner.cfg.reconnect_backoff_max_secs)
            .min(inner.cfg.reconnect_backoff_max_secs);
        debug!(strategy = %strategy, attempt, delay_secs = delay, "订单：退避等待后重连");
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
}

/// WS 应用层 Ping 心跳间隔：服务端 south.rs 收到 Ping 立即回 Pong，市场静默
/// 期靠它确认连接（及该连接的服务端 handler）仍然存活。
const PING_INTERVAL: Duration = Duration::from_secs(15);
/// 读空闲看门狗：该时长内未收到**任何**帧（含 Pong）→ 判定半开死连
/// （TCP ESTAB 但服务端 handler 已死），强制断开走既有退避重连。
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// K线订阅周期重订：服务端订阅生命周期可能在连接存活时被静默清空（异常关
/// 连接的策略级清理等），不能只依赖断线重连；按周期重发 subscribe_kline 兜底。
const RESUBSCRIBE_INTERVAL: Duration = Duration::from_secs(60);

/// 处理一条连接上的双向消息。返回 true = 应退出任务（命令通道关闭/事件通道关闭）。
// 参数全是连接级共享通道与三个独立心跳配置，打包 struct 只增加间接层。
#[allow(clippy::too_many_arguments)]
async fn run_connection(
    ws: WsStream,
    cmd_rx: &mut mpsc::Receiver<WsCommand>,
    event_tx: &mpsc::Sender<ClientEvent>,
    kline_tx: &broadcast::Sender<KlineUpdate>,
    kline_subs: &DashMap<(String, String), Vec<String>>,
    strategy: &str,
    ping_interval_cfg: Duration,
    read_idle_timeout_cfg: Duration,
    resubscribe_interval_cfg: Duration,
) -> bool {
    // 在途命令应答（Submit/GetOrder）：应答帧回声关联键后精确匹配
    let mut pending: VecDeque<PendingEntry> = VecDeque::new();
    let (raw_sink, mut source) = ws.split();
    // select 中「命令帧」与「心跳帧」两个分支都要写 sink，各自的 send future
    // 会同时持有 &mut sink → 包一层 Arc<Mutex>，分支只共享地借用锁。
    let sink = Arc::new(tokio::sync::Mutex::new(raw_sink));

    // 重连重发的 K线订阅帧（真实服务订阅为连接级，断线即失效；
    // 按登记逐 exchange 各发一帧——见 kline_subs 键设计）。
    let subs_payload = TradingServiceClient::subs_frames_for(kline_subs, strategy);
    for frame in subs_payload {
        debug!(%frame, "K线：重连重发 subscribe_kline 订阅帧");
        let mut s = sink.lock().await;
        if s.send(Message::Text(frame.to_string())).await.is_err() {
            return false; // 触发重连
        }
        drop(s);
    }

    // 心跳：interval 首 tick 立即就绪，先消费掉再进循环。
    let mut ping_interval = tokio::time::interval(ping_interval_cfg);
    ping_interval.tick().await;
    // 周期重订：interval 首 tick 立即就绪，先消费掉，首次重发发生在一个周期后。
    // 0 周期会让 interval panic，兜底到 1ms（测试可传极短值）。
    let mut resubscribe_interval =
        tokio::time::interval(resubscribe_interval_cfg.max(Duration::from_millis(1)));
    resubscribe_interval.tick().await;
    // 读空闲看门狗：每收到一帧就 reset 到时点后；触发即重连。
    let mut idle_timeout = Box::pin(tokio::time::sleep(read_idle_timeout_cfg));

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { return true };
                let mut s = sink.lock().await;
                if s
                    .send(Message::Text(cmd.payload.to_string()))
                    .await
                    .is_err()
                {
                    drop(s);
                    let e = SubmitError::retryable("WS 发送失败");
                    // 两类 reply 的 Ok 类型不同，结果逐臂丢弃
                    match cmd.reply {
                        CmdReply::Submit(r) => {
                            let _ = r.send(Err(e));
                        }
                        CmdReply::Order(r) => {
                            let _ = r.send(Err(e));
                        }
                        CmdReply::Positions(r) => {
                            let _ = r.send(Err(e));
                        }
                        CmdReply::Klines(r) => {
                            let _ = r.send(Err(e));
                        }
                        CmdReply::None => {}
                    }
                    return false; // 触发重连
                }
                drop(s);
                debug!(kind = ?cmd.kind, params = %cmd.payload, "已通过 WS 发送请求帧（应答按命令类别关联；Notify 无应答）");
                // Notify 类命令（subscribe_kline 等）无请求-应答关联，不入在途表。
                if !matches!(cmd.reply, CmdReply::None) {
                    pending.push_back(PendingEntry {
                        kind: cmd.kind,
                        key: cmd.key,
                        reply: cmd.reply,
                    });
                }
            }
            item = source.next() => {
                let Some(result) = item else { return false };
                // 任何入站帧（文本/Pong/服务端 Ping）都证明连接双向存活，
                // 看门狗无条件复位——半开死连上收不到任何东西。
                idle_timeout
                    .as_mut()
                    .reset(tokio::time::Instant::now() + read_idle_timeout_cfg);
                match result {
                    Ok(Message::Text(text)) => {
                        debug!(frame = %text, "订单：WS 收到原始帧（服务端逐字返回）");
                        match serde_json::from_str::<InEvent>(&text) {
                            Ok(ev) => {
                                debug!(
                                    frame_type = ev.kind,
                                    status = ?ev.status,
                                    client_order_id = ?ev.client_order_id,
                                    signal_id = ?ev.signal_id,
                                    exchange_order_id = ?ev.exchange_order_id,
                                    filled_price = ?ev.filled_price,
                                    filled_quantity = ?ev.filled_quantity,
                                    order_type = ?ev.order_type,
                                    order_price = ?ev.order_price,
                                    order_quantity = ?ev.order_quantity,
                                    // positions 帧：服务端返回的仓位数组（核心核对内容）
                                    positions = ?ev.positions,
                                    "订单：入站帧已解析（回调参数全量审计）"
                                );
                                if !dispatch_in(ev, &mut pending, event_tx, kline_tx).await {
                                    return true;
                                }
                            }
                            Err(e) => warn!(error = %e, text = %text, "未知/坏消息"),
                        }
                    }
                    Ok(Message::Pong(_)) => {
                        trace!("订单：收到 Pong，读空闲看门狗已复位")
                    }
                    Ok(Message::Ping(_)) => {
                        // tungstenite 读 Ping 时已在内部队列挂好自动 Pong，
                        // 下次写（含心跳）即发出。
                        trace!("订单：收到服务端 Ping（自动回 Pong），看门狗已复位")
                    }
                    Ok(other) => trace!(?other, "订单：收到非文本帧，看门狗已复位"),
                    Err(e) => {
                        warn!(error = %e, "订单：WS 读取错误，进入退避重连");
                        return false;
                    }
                }
            }
            _ = resubscribe_interval.tick() => {
                // 订阅生命周期兜底：连接心跳正常但仍收不到 K 线时（服务端在
                // 异常关连接等路径上静默清订阅），按登记周期重发订阅帧，
                // 一个周期内自愈，不必等到断线重连。
                let frames = TradingServiceClient::subs_frames_for(kline_subs, strategy);
                if !frames.is_empty() {
                    let mut s = sink.lock().await;
                    for frame in frames {
                        debug!(%frame, "K线：周期重订 subscribe_kline 订阅帧");
                        if s.send(Message::Text(frame.to_string())).await.is_err() {
                            return false; // 触发重连
                        }
                    }
                }
            }
            _ = ping_interval.tick() => {
                let mut s = sink.lock().await;
                if s.send(Message::Ping(Vec::new())).await.is_err() {
                    return false;
                }
                debug!("订单：已发送 WS Ping 心跳（Pong 未归时读空闲看门狗将强制重连）");
            }
            _ = idle_timeout.as_mut() => {
                warn!(idle_secs = read_idle_timeout_cfg.as_secs(), "订单：读空闲超时（TCP 在但无任何帧，判定半开死连），强制断开重连");
                return false;
            }
        }
    }
}

/// 取 Submit 在途条目：帧 signal_id 精确匹配；否则 FIFO 第一个 Submit 条目。
fn take_submit(
    pending: &mut VecDeque<PendingEntry>,
    signal_id: Option<&str>,
) -> Option<PendingEntry> {
    if let Some(sid) = signal_id {
        if let Some(pos) = pending
            .iter()
            .position(|p| p.kind == CmdKind::Submit && p.key == sid)
        {
            return pending.remove(pos);
        }
    }
    let pos = pending.iter().position(|p| p.kind == CmdKind::Submit)?;
    pending.remove(pos)
}

/// 取 GetOrder 在途条目：按帧 signal_id 精确（请求键即 signal_id），
/// 无匹配/帧无关联键时 FIFO（服务端缺参 error 同样走此路）。
fn take_order(
    pending: &mut VecDeque<PendingEntry>,
    signal_id: Option<&str>,
) -> Option<PendingEntry> {
    if let Some(sid) = signal_id {
        if let Some(pos) = pending
            .iter()
            .position(|p| p.kind == CmdKind::GetOrder && p.key == sid)
        {
            return pending.remove(pos);
        }
    }
    let pos = pending.iter().position(|p| p.kind == CmdKind::GetOrder)?;
    pending.remove(pos)
}

/// 取 GetPositions 在途条目：应答非空时按首元素 instrument 精确；
/// 空数组（已平仓）帧无标的回声 → FIFO（请求按序，服务端通常按序应答）。
fn take_positions(
    pending: &mut VecDeque<PendingEntry>,
    instrument: Option<&str>,
) -> Option<PendingEntry> {
    if let Some(inst) = instrument {
        if let Some(pos) = pending
            .iter()
            .position(|p| p.kind == CmdKind::GetPositions && p.key == inst)
        {
            return pending.remove(pos);
        }
    }
    let pos = pending
        .iter()
        .position(|p| p.kind == CmdKind::GetPositions)?;
    pending.remove(pos)
}

/// 取 GetKlines 在途条目：应答顶层 symbol 精确；空结果/参数错误帧可能无
/// 回声 → FIFO（请求按序，服务端按序应答）。
fn take_klines(pending: &mut VecDeque<PendingEntry>, symbol: Option<&str>) -> Option<PendingEntry> {
    if let Some(sym) = symbol {
        if let Some(pos) = pending
            .iter()
            .position(|p| p.kind == CmdKind::GetKlines && p.key == sym)
        {
            return pending.remove(pos);
        }
    }
    let pos = pending.iter().position(|p| p.kind == CmdKind::GetKlines)?;
    pending.remove(pos)
}

/// 从回调帧提取实际委托要素；order_type 缺失（旧服务端）→ None。
fn place_from(
    order_type: &Option<String>,
    order_price: &Option<serde_json::Value>,
    order_quantity: &Option<serde_json::Value>,
) -> Option<OrderPlace> {
    order_type.as_ref().map(|t| OrderPlace {
        order_type: t.clone(),
        order_price: wire::opt_f64(order_price),
        order_quantity: wire::opt_f64(order_quantity),
    })
}

/// 分发入站事件；false = 事件通道关闭应退出。K线帧走 kline_tx broadcast，
/// 不占订单事件通道（高频回放与订单路径隔离）。
async fn dispatch_in(
    ev: InEvent,
    pending: &mut VecDeque<PendingEntry>,
    event_tx: &mpsc::Sender<ClientEvent>,
    kline_tx: &broadcast::Sender<KlineUpdate>,
) -> bool {
    // ev 为本函数独有：顶层解构，各臂按值使用，避免对帧内 String 的多余 clone。
    let InEvent {
        kind,
        status,
        client_order_id,
        exchange_order_id,
        strategy_name,
        signal_id,
        reduce_only: _reduce_only,
        filled_price,
        filled_quantity,
        order_type,
        order_price,
        order_quantity,
        fee,
        positions,
        kline,
        klines,
        symbol,
        has_more,
        message,
    } = ev;
    // 真实手续费（阶段 4 v2.7）：缺失=null → None；数字/字符串双口径。
    let fee_val = fee.as_ref().map(wire::f64_or_zero);
    match kind.as_str() {
        "ack" => {
            // 订阅 ack（首订/周期重订）：无 client_order_id，不是订单应答。
            // 走 debug，避免周期重订把「无在途请求的 ack」WARN 每分钟刷一遍。
            if client_order_id.is_none() && message.as_deref().is_some_and(|m| m.contains("已订阅"))
            {
                debug!(message = %message.unwrap_or_default(), "K线订阅 ack（无订单在途）");
                return true;
            }
            // ack 恒为 place_order 应答：只匹配 Submit 条目
            let Some(entry) = take_submit(pending, signal_id.as_deref()) else {
                tracing::warn!(?client_order_id, "无在途请求的 ack，忽略");
                return true;
            };
            let CmdReply::Submit(reply) = entry.reply else {
                unreachable!("take_submit 恒返回 Submit 条目")
            };
            let result = ack_result(status.as_deref(), client_order_id.clone(), message);
            let accepted = result.is_ok();
            let _ = reply.send(result);
            if accepted {
                // 成功 ack 携带 exchange_order_id：补一条 Ack 状态更新，让信号
                // 内核落交易所订单号。同 task 时序保证它在 apply_submit_outcome
                // （Submitted）之后才被处理，状态推进 Submitted → Accepted。
                let Some(cid) = client_order_id else {
                    return true;
                };
                let place = place_from(&order_type, &order_price, &order_quantity);
                let upd = OrderUpdate {
                    order_id: cid,
                    exchange_order_id,
                    state: OrderState::Ack,
                    filled_size: 0.0,
                    avg_price: 0.0,
                    filled_quantity_raw: None,
                    strategy_name,
                    signal_id,
                    message: None,
                    place,
                    fee: fee_val,
                };
                if let Err(e) = event_tx.try_send(ClientEvent::OrderUpdate(upd)) {
                    tracing::warn!(error = %e, "ack 附带 OrderUpdate 投递失败（exchange_order_id 未落账）");
                }
            }
        }
        "error" => {
            // K线订阅相关错误（subscribe_kline 拒订阅等，帧无 signal_id 回声）
            // 不得吞掉订单在途请求：显式识别后仅告警丢弃，正常订单 error 路径不变。
            if message
                .as_deref()
                .is_some_and(|m| m.contains("K线") || m.contains("subscribe_kline"))
            {
                warn!(
                    message = %message.unwrap_or_default(),
                    "K线订阅错误帧（不占订单在途匹配，已丢弃）"
                );
                return true;
            }
            // 按「Submit signal_id 精确 → GetOrder signal_id 精确 → GetOrder FIFO
            // → GetPositions FIFO（error 帧无 instrument 回声）→ Submit FIFO」
            // 顺序匹配，避免类别间互吞。
            let entry: Option<PendingEntry> = if let Some(sid) = signal_id.as_deref() {
                pending
                    .iter()
                    .position(|p| p.kind == CmdKind::Submit && p.key == sid)
                    .and_then(|i| pending.remove(i))
            } else {
                None
            };
            let entry = entry.or_else(|| {
                if let Some(sid) = signal_id.as_deref() {
                    pending
                        .iter()
                        .position(|p| p.kind == CmdKind::GetOrder && p.key == sid)
                        .and_then(|i| pending.remove(i))
                } else {
                    None
                }
            });
            let entry = entry
                .or_else(|| {
                    pending
                        .iter()
                        .position(|p| p.kind == CmdKind::GetOrder)
                        .and_then(|i| pending.remove(i))
                })
                .or_else(|| {
                    pending
                        .iter()
                        .position(|p| p.kind == CmdKind::GetPositions)
                        .and_then(|i| pending.remove(i))
                })
                .or_else(|| {
                    // get_klines 的参数错误帧无 symbol 回声（§10.4）→ FIFO 命中在途查询
                    pending
                        .iter()
                        .position(|p| p.kind == CmdKind::GetKlines)
                        .and_then(|i| pending.remove(i))
                })
                .or_else(|| take_submit(pending, None));
            let Some(entry) = entry else {
                tracing::warn!(?message, "无在途请求的 error 帧，忽略");
                return true;
            };
            let msg = message.unwrap_or_else(|| "trading-service 拒绝".into());
            match entry.reply {
                CmdReply::Submit(reply) => {
                    let _ = reply.send(Err(SubmitError::rejected(msg)));
                }
                CmdReply::Order(reply) => {
                    let _ = reply.send(Err(SubmitError::rejected(msg)));
                }
                CmdReply::Positions(reply) => {
                    let _ = reply.send(Err(SubmitError::rejected(msg)));
                }
                CmdReply::Klines(reply) => {
                    let _ = reply.send(Err(SubmitError::rejected(msg)));
                }
                CmdReply::None => {}
            }
        }
        // get_order 成功应答：字段与 ack/order_event 同形
        "order" => {
            let Some(entry) = take_order(pending, signal_id.as_deref()) else {
                tracing::warn!(?signal_id, "无在途 get_order 请求的 order 帧，忽略");
                return true;
            };
            let CmdReply::Order(reply) = entry.reply else {
                unreachable!("take_order 恒返回 GetOrder 条目")
            };
            let result: Result<OrderSnapshot, SubmitError> =
                match status.as_deref().map(wire::map_status) {
                    Some(Some(st)) => Ok(OrderSnapshot {
                        order_id: client_order_id.unwrap_or_default(),
                        state: st,
                        filled_size: wire::opt_f64(&filled_quantity),
                        avg_price: wire::opt_f64(&filled_price),
                        exchange_order_id,
                        filled_quantity_raw: wire::opt_number_raw(&filled_quantity),
                        place: place_from(&order_type, &order_price, &order_quantity),
                        fee: fee_val,
                    }),
                    Some(None) => Err(SubmitError::retryable(format!(
                        "order 帧状态未知：{}",
                        status.unwrap_or_default()
                    ))),
                    None => Err(SubmitError::retryable("order 帧缺 status")),
                };
            let _ = reply.send(result);
        }
        // get_positions 应答：positions 元素映射为 RemotePosition
        "positions" => {
            let rows = positions.unwrap_or_default();
            let echo_inst = rows.first().map(|p| p.instrument.as_str());
            let Some(entry) = take_positions(pending, echo_inst) else {
                tracing::warn!("无在途 get_positions 请求的 positions 帧，忽略");
                return true;
            };
            let CmdReply::Positions(reply) = entry.reply else {
                unreachable!("take_positions 恒返回 GetPositions 条目")
            };
            let mapped: Vec<RemotePosition> = rows
                .iter()
                .map(|p| RemotePosition {
                    instrument: p.instrument.clone(),
                    side: p.side.clone(),
                    quantity: wire::opt_f64(&p.quantity),
                    avg_price: wire::opt_f64(&p.pos_price),
                    deleted: p.deleted,
                })
                .collect();
            let _ = reply.send(Ok(mapped));
        }
        "order_event" => {
            let Some(status) = status.as_deref() else {
                return true;
            };
            let Some(state) = wire::map_status(status) else {
                tracing::warn!(status, "无法识别的订单状态，跳过该帧");
                return true;
            };
            let Some(order_id) = client_order_id else {
                tracing::warn!("order_event 缺 client_order_id，跳过");
                return true;
            };
            let place = place_from(&order_type, &order_price, &order_quantity);
            let upd = OrderUpdate {
                order_id,
                exchange_order_id,
                state,
                filled_size: wire::opt_f64(&filled_quantity),
                avg_price: wire::opt_f64(&filled_price),
                filled_quantity_raw: wire::opt_number_raw(&filled_quantity),
                strategy_name,
                signal_id,
                message,
                place,
                fee: fee_val,
            };
            if event_tx.send(ClientEvent::OrderUpdate(upd)).await.is_err() {
                return false;
            }
        }
        // get_klines 应答（§10.4）：klines 与批量推送帧内 bar 完全同形；
        // 顶层 symbol 回声为关联键，无回声（或越序）时 FIFO 命中在途查询。
        "kline_history" => {
            let echo = symbol.as_deref();
            let Some(entry) = take_klines(pending, echo) else {
                tracing::warn!(?echo, "无在途 get_klines 请求的 kline_history 帧，忽略");
                return true;
            };
            let CmdReply::Klines(reply) = entry.reply else {
                unreachable!("take_klines 恒返回 GetKlines 条目")
            };
            let bars = klines
                .unwrap_or_default()
                .into_iter()
                .filter_map(WireKline::into_update)
                .collect();
            let _ = reply.send(Ok(KlineHistory {
                bars,
                has_more: has_more.unwrap_or(false),
            }));
        }
        // K线定稿推送（协议 v2）：单根 kline_1m / 同所批量 kline_1m_batch。
        "kline_1m" => {
            if let Some(k) = kline.and_then(WireKline::into_update) {
                // broadcast send 永不阻塞；无订阅者 = 无人挂 K线流，静默。
                let _ = kline_tx.send(k);
            }
        }
        "kline_1m_batch" => {
            for k in klines
                .unwrap_or_default()
                .into_iter()
                .filter_map(WireKline::into_update)
            {
                let _ = kline_tx.send(k);
            }
        }
        // positions（get_position 应答）当前无消费方；welcome 等忽略。
        other => tracing::debug!(kind = other, "忽略的入站消息类型"),
    }
    true
}

/// ack 帧 → submit 结果。
///
/// 交易路径 fail-closed：只有显式白名单内的成功/进行中状态才返回 Ok；
/// 终态失败返回 Rejected；未知/缺失状态保守判 Retryable（调用方对账重发）。
fn ack_result(
    status: Option<&str>,
    client_order_id: Option<String>,
    message: Option<String>,
) -> Result<String, SubmitError> {
    match status {
        // 成功/进行中白名单：有 client_order_id → 受理成功
        Some("NEW") | Some("PLACING") | Some("PARTIAL") | Some("FILLED") => match client_order_id {
            Some(cid) => Ok(cid),
            None => Err(SubmitError::rejected("ack 缺少 client_order_id")),
        },
        // 终态失败：直接拒绝，不重试
        Some("REJECTED") | Some("EXPIRED") | Some("CANCELED") => {
            let fallback = format!("订单被 {}", status.unwrap_or("拒绝"));
            Err(SubmitError::rejected(message.unwrap_or(fallback)))
        }
        // 未知/缺失状态：保守判可重试 + 告警日志，由对账流程兜底
        status => {
            let raw = status.unwrap_or("none");
            tracing::warn!(
                status = raw,
                client_order_id = ?client_order_id,
                "ack 状态未知/缺失，保守判可重试"
            );
            Err(SubmitError::retryable(format!("ack 状态未知/缺失：{raw}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strategy_core::{IntentKind, SignalStatus};
    use strategy_signals::gateway::FailureKind;

    fn intent_with(id: &str, side: Side, kind: IntentKind) -> SignalIntent {
        SignalIntent {
            id: id.into(),
            strategy_id: "sar_snt3".into(),
            symbol: "BTCUSDT".into(),
            kind,
            direction: side,
            price: 100.0,
            size: 1.0,
            size_raw: None,
            stop_price: None,
            position_id: None,
            stop_order_id: None,
            created_event_time: chrono::Utc::now(),
            status: SignalStatus::Created,
            offset: None,
        }
    }

    /// 测试用下单场所/资金配置（order_cash 可按用例覆盖）
    fn venue_for(order_cash: f64) -> OrderVenueCfg {
        OrderVenueCfg {
            exchange: "binance".into(),
            account: "test_binance".into(),
            leverage: 3,
            order_cash,
        }
    }

    fn cfg_for(url: String) -> TradingConfig {
        TradingConfig {
            ws_url: url,
            submit_enabled: true,
            request_timeout: Duration::from_secs(3),
            strategy_secret: "ignored-by-raw".into(),
            reconnect_backoff_max_secs: 8,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_disabled_rejects_locally() {
        let (client, _events) = TradingServiceClient::start_arc(
            TradingConfig {
                ws_url: "ws://127.0.0.1:1".into(),
                submit_enabled: false,
                ..Default::default()
            },
            Handle::current(),
        );
        let err = client
            .submit_order(
                "st",
                &venue_for(0.0),
                &intent_with("s", Side::Long, IntentKind::Open),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind, FailureKind::Rejected);
    }

    /// U8：网关实例闸门 allow_submit=false 时本地拒绝，即使 client.submit_enabled=true
    /// 也不得外发（双保险之实例层）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn allow_submit_false_rejects_locally_without_wire() {
        let (client, _events) = TradingServiceClient::start_arc(
            TradingConfig {
                ws_url: "ws://127.0.0.1:1".into(),
                submit_enabled: true, // 总开关开着，但实例闸门关闭
                ..Default::default()
            },
            Handle::current(),
        );
        let mut gw = TradingGateway::with_submit(client, "st".into(), venue_for(1.0), false);
        let err = gw
            .submit_fut(&intent_with("s", Side::Long, IntentKind::Open))
            .await
            .unwrap_err();
        assert_eq!(err.kind, FailureKind::Rejected);
    }

    /// U9：双保险——allow_submit=true 但 client.submit_enabled=false（总保险丝）仍拒绝。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_submit_switch_still_blocks_when_allow_submit_true() {
        let (client, _events) = TradingServiceClient::start_arc(
            TradingConfig {
                ws_url: "ws://127.0.0.1:1".into(),
                submit_enabled: false, // 总保险丝关掉
                ..Default::default()
            },
            Handle::current(),
        );
        let mut gw = TradingGateway::with_submit(client, "st".into(), venue_for(1.0), true);
        let err = gw
            .submit_fut(&intent_with("s", Side::Long, IntentKind::Open))
            .await
            .unwrap_err();
        assert_eq!(err.kind, FailureKind::Rejected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zero_quantity_rejected_locally_when_no_default() {
        let (client, _events) =
            TradingServiceClient::start_arc(cfg_for("ws://127.0.0.1:1".into()), Handle::current());
        // Open 意图 size 恒 0：金额只能来自 venue.order_cash；<=0 必须本地拒绝
        let it = intent_with("s", Side::Long, IntentKind::Open);
        let err = client
            .submit_order("st", &venue_for(0.0), &it)
            .await
            .unwrap_err();
        assert_eq!(err.kind, FailureKind::Rejected);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn query_unreachable_times_out_retryable() {
        let (client, _events) =
            TradingServiceClient::start_arc(cfg_for("ws://127.0.0.1:1".into()), Handle::current());
        let mut gw = TradingGateway::new(client, "st".into(), venue_for(1.0));
        // 服务不可达：连接失败 → request_timeout(3s) 后保守判 Retryable
        let err = gw.query_fut("0xanything").await.unwrap_err();
        assert_eq!(err.kind, FailureKind::Retryable);
    }

    /// 同策略类型跨交易所订阅：登记互不覆盖，重连重发逐 exchange 各一帧
    /// （回归钉：曾因 kline_subs 只按策略类型存单值 exchange，后订者顶掉
    /// 先订者，重连后先订交易所的 K 线永久断流）。
    #[tokio::test]
    async fn kline_subs_per_exchange_no_overwrite() {
        let (client, _ev) =
            TradingServiceClient::start_arc(cfg_for("ws://127.0.0.1:1".into()), Handle::current());
        client
            .subscribe_klines("dual", "binance", &["BTCUSDT".to_string()])
            .await
            .expect("命令入队即成功（连接懒建，不要求 TCP 可达）");
        client
            .subscribe_klines("dual", "SHFE", &["CU".to_string()])
            .await
            .expect("命令入队即成功");
        let frames = TradingServiceClient::subs_frames_for(&client.inner.kline_subs, "dual");
        assert_eq!(frames.len(), 2, "两个 exchange 各成一组，重连应逐组重发");
        let by_ex: std::collections::BTreeMap<String, Vec<String>> = frames
            .iter()
            .map(|f| {
                (
                    f["exchange"].as_str().unwrap().to_string(),
                    f["symbols"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|s| s.as_str().unwrap().to_string())
                        .collect(),
                )
            })
            .collect();
        assert_eq!(by_ex.get("binance").unwrap(), &vec!["BTCUSDT".to_string()]);
        assert_eq!(by_ex.get("SHFE").unwrap(), &vec!["CU".to_string()]);
        // 其他策略类型不受牵连
        assert!(
            TradingServiceClient::subs_frames_for(&client.inner.kline_subs, "other").is_empty()
        );
    }

    // ── get_order 真实查询：脚本 WS 收到 get_order 帧后回 type=order ───────

    /// 脚本服务：收到带 option 的命令帧（get_order/get_positions）后回给定
    /// 应答帧，并逐字捕获请求帧；其余帧（Ping）照常读，保持连接健康。
    async fn spawn_reply_server(
        reply: String,
    ) -> (String, Arc<std::sync::Mutex<Vec<serde_json::Value>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let got = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = Arc::clone(&got);
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(msg)) = ws.next().await {
                if let TMessage::Text(t) = msg {
                    let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                    if v.get("option").is_some() {
                        captured.lock().unwrap().push(v);
                        ws.send(TMessage::Text(reply.clone())).await.unwrap();
                    }
                }
            }
        });
        (url, got)
    }

    /// 断言 get_order 请求帧按 signal_id 查询、不含 client_order_id。
    fn assert_request_keyed_by_signal_id(
        got: &std::sync::Mutex<Vec<serde_json::Value>>,
        signal_id: &str,
    ) {
        let frames = got.lock().unwrap();
        let req = frames.first().expect("服务端应收到 get_order 帧");
        assert_eq!(req["option"], "get_order");
        assert_eq!(req["signal_id"], signal_id);
        assert!(
            req.get("client_order_id").is_none(),
            "帧内不得再带 client_order_id"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_order_returns_filled_snapshot() {
        let reply = serde_json::json!({
            "type": "order",
            "status": "FILLED",
            "client_order_id": "0x1b084f71c81046ceafc777880573a55d",
            "signal_id": "sig_5c63c85a31aebc3a",
            "exchange_order_id": "3189000000012345",
            "order_type": "market",
            "order_price": 0,
            "order_quantity": 0.073,
            "filled_price": 2657.33,
            "filled_quantity": 0.073
        })
        .to_string();
        let (url, got) = spawn_reply_server(reply).await;
        let (client, _events) = TradingServiceClient::start_arc(cfg_for(url), Handle::current());
        let mut gw = TradingGateway::new(client, "st".into(), venue_for(1.0));

        let snap = gw
            .query_fut("sig_5c63c85a31aebc3a")
            .await
            .expect("get_order 应返回订单快照");
        assert_request_keyed_by_signal_id(&got, "sig_5c63c85a31aebc3a");
        assert_eq!(snap.order_id, "0x1b084f71c81046ceafc777880573a55d");
        assert_eq!(snap.state, OrderState::Filled);
        assert!((snap.filled_size - 0.073).abs() < 1e-9);
        assert!((snap.avg_price - 2657.33).abs() < 1e-9);
        // 交易所单号 / 数量原始字面量 / 实际委托要素必须随快照带回
        assert_eq!(snap.exchange_order_id.as_deref(), Some("3189000000012345"));
        assert_eq!(snap.filled_quantity_raw.as_deref(), Some("0.073"));
        let place = snap.place.expect("order 帧应带实际委托要素");
        assert_eq!(place.order_type, "market");
        assert_eq!(place.order_price, 0.0);
        assert!((place.order_quantity - 0.073).abs() < 1e-9);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_order_error_frame_is_rejected() {
        let reply = serde_json::json!({
            "type": "error",
            "status": "REJECTED",
            "message": "订单不存在"
        })
        .to_string();
        let (url, got) = spawn_reply_server(reply).await;
        let (client, _events) = TradingServiceClient::start_arc(cfg_for(url), Handle::current());
        let mut gw = TradingGateway::new(client, "st".into(), venue_for(1.0));

        let err = gw.query_fut("sig_5c63c85a31aebc3a").await.unwrap_err();
        assert_request_keyed_by_signal_id(&got, "sig_5c63c85a31aebc3a");
        assert_eq!(err.kind, FailureKind::Rejected);
        assert!(err.message.contains("订单不存在"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_positions_returns_mapped_elements() {
        let reply = serde_json::json!({
            "type": "positions",
            "status": "OK",
            "positions": [
                { "instrument": "BTCUSDT", "side": "SELL",
                  "quantity": 0.002, "pos_price": 83136.1 },
                { "instrument": "BTCUSDT", "side": "BUY",
                  "quantity": "0.01", "pos_price": "80000.0" }
            ]
        })
        .to_string();
        let (url, got) = spawn_reply_server(reply).await;
        let (client, _events) = TradingServiceClient::start_arc(cfg_for(url), Handle::current());
        let mut gw = TradingGateway::new(client, "st".into(), venue_for(1.0));

        let positions = gw
            .positions_fut("BTCUSDT", "short")
            .await
            .expect("应答正常");
        // 请求帧按 instrument + side 筛选期货仓
        let req = got.lock().unwrap().first().unwrap().clone();
        assert_eq!(req["option"], "get_positions");
        assert_eq!(req["instrument"], "BTCUSDT");
        assert_eq!(req["instrument_type"], "futures");
        assert_eq!(req["side"], "short");
        assert_eq!(
            positions,
            vec![
                RemotePosition {
                    instrument: "BTCUSDT".into(),
                    side: "SELL".into(),
                    quantity: 0.002,
                    avg_price: 83136.1,
                    deleted: false
                },
                RemotePosition {
                    instrument: "BTCUSDT".into(),
                    side: "BUY".into(),
                    quantity: 0.01,
                    avg_price: 80000.0,
                    deleted: false
                },
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_positions_empty_array_is_ok() {
        // 无仓：positions: []（不是错误）→ 空 Vec
        let reply = serde_json::json!({
            "type": "positions", "status": "OK", "positions": []
        })
        .to_string();
        let (url, _got) = spawn_reply_server(reply).await;
        let (client, _events) = TradingServiceClient::start_arc(cfg_for(url), Handle::current());
        let mut gw = TradingGateway::new(client, "st".into(), venue_for(1.0));

        let positions = gw
            .positions_fut("BTCUSDT", "short")
            .await
            .expect("空数组是正常应答");
        assert!(positions.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_positions_error_frame_is_rejected() {
        let reply = serde_json::json!({
            "type": "error",
            "status": "REJECTED",
            "message": "账户在该所未配置"
        })
        .to_string();
        let (url, _got) = spawn_reply_server(reply).await;
        let (client, _events) = TradingServiceClient::start_arc(cfg_for(url), Handle::current());
        let mut gw = TradingGateway::new(client, "st".into(), venue_for(1.0));

        let err = gw.positions_fut("BTCUSDT", "short").await.unwrap_err();
        assert_eq!(err.kind, FailureKind::Rejected);
        assert!(err.message.contains("未配置"));
    }

    // ── ack_result 纯函数单测（I2：fail-closed 白名单回归）──

    #[test]
    fn ack_result_whitelist_success_states_return_ok() {
        // NEW/PLACING/PARTIAL/FILLED + 有 cid → Ok
        for s in ["NEW", "PLACING", "PARTIAL", "FILLED"] {
            let r = ack_result(Some(s), Some("0xabc".into()), None);
            assert_eq!(r.unwrap(), "0xabc", "状态 {s} 应返回 Ok(cid)");
        }
        // 白名单状态但缺 cid → Rejected（不是静默成功）
        let r = ack_result(Some("NEW"), None, None);
        assert_eq!(r.unwrap_err().kind, FailureKind::Rejected);
    }

    #[test]
    fn ack_result_unknown_terminal_state_is_retryable_not_ok() {
        // 未来新增的未知终态（如 SUSPENDED）：fail-closed，判 Retryable，不静默成功
        let r = ack_result(Some("SUSPENDED"), Some("0xabc".into()), None);
        let err = r.unwrap_err();
        assert_eq!(
            err.kind,
            FailureKind::Retryable,
            "未知状态必须 Retryable，不能 Ok"
        );
        assert!(err.message.contains("未知"), "错误信息应包含状态说明");
    }

    #[test]
    fn ack_result_rejected_terminal_state_is_rejected() {
        let r = ack_result(
            Some("REJECTED"),
            Some("0xabc".into()),
            Some("insufficient balance".into()),
        );
        let err = r.unwrap_err();
        assert_eq!(err.kind, FailureKind::Rejected);
        assert!(err.message.contains("balance"));
    }

    #[test]
    fn ack_result_missing_status_is_retryable() {
        // status 完全缺失：保守判可重试
        let r = ack_result(None, Some("0xabc".into()), None);
        assert_eq!(r.unwrap_err().kind, FailureKind::Retryable);
    }

    // ── 心跳 / 读空闲看门狗：本地 WS 服务对，秒级计时 ─────────────────────

    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::Message as TMessage;

    /// 起一个只做握手的本地 WS 服务：
    /// - silent：握手后不再读帧 → 客户端 Ping 无 Pong（半开场景）
    /// - reply_pong：握手后轮询，Ping → Pong（健康场景）
    async fn spawn_ws_server(reply_pong: bool) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            if !reply_pong {
                return; // 不再读：Ping 停在缓冲，无 Pong
            }
            while let Some(Ok(msg)) = ws.next().await {
                if let TMessage::Ping(p) = msg {
                    if ws.send(TMessage::Pong(p)).await.is_err() {
                        break;
                    }
                }
            }
        });
        url
    }

    /// 半开死连：Ping 无 Pong、持续静默 → 读空闲超时，返回 false（外层重连）。
    #[tokio::test]
    async fn half_open_read_idle_forces_reconnect() {
        use std::time::Duration;
        let url = spawn_ws_server(false).await;
        let cfg = TradingConfig {
            ws_url: url,
            request_timeout: Duration::from_secs(5),
            submit_enabled: false,
            reconnect_backoff_max_secs: 8,
            strategy_secret: String::new(),
        };
        let ws = connect(&cfg, "st").await.expect("握手成功");
        let subs: DashMap<(String, String), Vec<String>> = DashMap::new();
        let (_cmd_tx, mut cmd_rx) = mpsc::channel::<WsCommand>(8);
        let (event_tx, _event_rx) = mpsc::channel::<ClientEvent>(8);
        let (kline_tx, _kline_rx) = broadcast::channel::<KlineUpdate>(8);
        let exit = tokio::time::timeout(
            Duration::from_secs(2),
            run_connection(
                ws,
                &mut cmd_rx,
                &event_tx,
                &kline_tx,
                &subs,
                "st",
                Duration::from_millis(100),
                Duration::from_millis(300),
                Duration::from_secs(3600),
            ),
        )
        .await
        .expect("300ms 空闲超时应在 2s 内触发");
        assert!(!exit, "半开死连必须返回 false → 外层退避重连");
    }

    /// 健康连接：服务端及时回 Pong，超过 idle 时长也不误判；关闭命令通道后
    /// 返回 true（真正退出）。
    #[tokio::test]
    async fn pong_keeps_connection_alive_until_command_channel_closes() {
        use std::time::Duration;
        let url = spawn_ws_server(true).await;
        let cfg = TradingConfig {
            ws_url: url,
            request_timeout: Duration::from_secs(5),
            submit_enabled: false,
            reconnect_backoff_max_secs: 8,
            strategy_secret: String::new(),
        };
        let ws = connect(&cfg, "st").await.expect("握手成功");
        let (cmd_tx, cmd_rx) = mpsc::channel::<WsCommand>(8);
        let (event_tx, _event_rx) = mpsc::channel::<ClientEvent>(8);
        let (kline_tx, _kline_rx) = broadcast::channel::<KlineUpdate>(8);
        let subs: DashMap<(String, String), Vec<String>> = DashMap::new();
        let task = tokio::spawn(async move {
            let mut cmd_rx = cmd_rx;
            run_connection(
                ws,
                &mut cmd_rx,
                &event_tx,
                &kline_tx,
                &subs,
                "st",
                Duration::from_millis(100),
                Duration::from_millis(300),
                Duration::from_secs(3600),
            )
            .await
        });
        // 存活 800ms（>2 个 idle 周期），Pong 持续保活
        tokio::time::sleep(Duration::from_millis(800)).await;
        drop(cmd_tx); // 命令通道关闭 → 优雅退出
        let exit = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("任务应在通道关闭后退出")
            .unwrap();
        assert!(exit, "Pong 保活期间不得误判；通道关闭才返回 true");
    }

    /// 起一个本地 WS 服务：记录客户端发来的文本帧（断言周期重订用）。
    async fn spawn_ws_frame_recorder() -> (String, mpsc::UnboundedReceiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/ws", listener.local_addr().unwrap());
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            while let Some(Ok(msg)) = ws.next().await {
                match msg {
                    TMessage::Text(t) => {
                        let _ = tx.send(t.to_string());
                    }
                    TMessage::Ping(p) => {
                        let _ = ws.send(TMessage::Pong(p)).await;
                    }
                    _ => {}
                }
            }
        });
        (url, rx)
    }

    /// 服务端静默清订阅（连接仍存活）时，客户端必须按周期重发 subscribe_kline
    /// 兜底，否则策略会静默收不到行情直到下一次重连。
    #[tokio::test]
    async fn periodic_resubscribe_resends_kline_frames() {
        use std::time::Duration;
        let (url, mut frames_rx) = spawn_ws_frame_recorder().await;
        let cfg = TradingConfig {
            ws_url: url,
            request_timeout: Duration::from_secs(5),
            submit_enabled: false,
            reconnect_backoff_max_secs: 8,
            strategy_secret: String::new(),
        };
        let ws = connect(&cfg, "futures_energy_chem")
            .await
            .expect("握手成功");
        let subs: DashMap<(String, String), Vec<String>> = DashMap::new();
        subs.insert(
            ("futures_energy_chem".into(), "ctp".into()),
            vec!["fu".into(), "bu".into()],
        );
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<WsCommand>(8);
        let (event_tx, _event_rx) = mpsc::channel::<ClientEvent>(8);
        let (kline_tx, _kline_rx) = broadcast::channel::<KlineUpdate>(8);
        let task = tokio::spawn(async move {
            run_connection(
                ws,
                &mut cmd_rx,
                &event_tx,
                &kline_tx,
                &subs,
                "futures_energy_chem",
                Duration::from_millis(200), // 心跳慢于重订周期，避免混入
                Duration::from_millis(800),
                Duration::from_millis(150),
            )
            .await
        });

        let deadline = tokio::time::Instant::now() + Duration::from_millis(650);
        let mut subscribe_frames = 0usize;
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, frames_rx.recv()).await {
                Ok(Some(text)) => {
                    if text.contains("subscribe_kline") {
                        subscribe_frames += 1;
                    }
                }
                _ => break,
            }
        }
        drop(cmd_tx);
        let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
        assert!(
            subscribe_frames >= 3,
            "650ms 内应有初次订阅 + ≥2 次周期重订（150ms 周期），实际 {subscribe_frames} 帧"
        );
    }
}
