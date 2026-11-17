//! trading-service 作唯一 K线数据源（klines-service 已退役）。
//!
//! K 线经 trading-service 交易 WS 推送（协议 §10：`subscribe_kline` → 定稿
//! `kline_1m_batch`，权威定义见 trading_service_rust
//! `docs/策略端ws接口文档.md`）；本适配器把
//! [`TradingServiceClient::kline_updates`] 的 broadcast 流映射为
//! [`KlineClient`] 端口，使策略管线（预热、1m→大周期聚合）与回测完全同形
//! ——联调用 mock 回放 `data/klines/1m` CSV 即可端到端驱动策略。
//!
//! 与旧 klines-service 路径的口径差异：
//! - 推送只有**定稿** bar（真实服务与 mock 同语义），无未闭合 Tick 快照；
//! - 多策略类型订阅同 symbol 时，各连接会各推一份同 bar——router 按
//!   (venue, symbol) **单调水位去重**，下游每根只收一次；
//! - 历史回填 = 本地 1m CSV（`kline_data_dir`）为主 + `get_klines`
//!   （§10.4，服务端本地留存）分页补足缺口；两路皆空才报错；
//! - 订阅在装配期一次性发起（bootstrap），运行期不增删 symbol；
//!   重连重发由连接任务按登记（逐 exchange 分组）完成（订阅是连接级）。

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use strategy_broker::{KlineHistory, KlineUpdate, TradingServiceClient};
use strategy_core::candle::{Candle, Timeframe};
use strategy_data::client::{KlineBar, KlineClient, KlineEvent, KlineStream};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

/// 扇出通道容量（与订单事件通道同量级；慢消费经 Lagged 自愈回补）。
const FANOUT_CAP: usize = 100;

/// `get_klines` 单页上限（服务端硬上限 1500，§10.4；翻页步长）。
const GET_KLINES_PAGE: usize = 1500;

/// 本地 CSV 尾根落后超过该窗口即判定陈旧：启动回填必须先推进到 now，
/// 否则 warmup 用「陈旧尾部 + 当前实时 bar」拼接，指标会跨过缺口。
const TAIL_STALE_TOLERANCE: Duration = Duration::from_secs(300);

/// CTP 合约代码去尾部月份数字转品种前缀（`fu2701` → `FU`）；无尾部数字
/// （crypto 标的）返回 None，不参与前缀路由。
fn ctp_product_prefix(symbol: &str) -> Option<String> {
    let digits_at = symbol.find(|c: char| c.is_ascii_digit())?;
    if digits_at == 0 || !symbol[digits_at..].chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(symbol[..digits_at].to_ascii_uppercase())
}

/// 帧 symbol → 扇出订阅键：精确匹配优先；CTP 帧内是当前主力合约
/// （`fu2701`），按品种前缀匹配到订阅键 `FU`（服务端只推主力，换月随帧变化）。
fn fanout_key<'a>(
    fan: &'a HashMap<String, broadcast::Sender<KlineEvent>>,
    frame_symbol: &str,
) -> Option<(String, &'a broadcast::Sender<KlineEvent>)> {
    if let Some(tx) = fan.get(frame_symbol) {
        return Some((frame_symbol.to_string(), tx));
    }
    ctp_product_prefix(frame_symbol)
        .and_then(|prefix| fan.get(&prefix).map(|tx| (prefix.clone(), tx)))
}

/// CTP 服务端按小写品种/合约代码匹配；`get_klines` 回填请求必须发小写。
fn wire_symbol(exchange: &str, symbol: &str) -> String {
    if exchange.eq_ignore_ascii_case("ctp") {
        symbol.to_ascii_lowercase()
    } else {
        symbol.to_string()
    }
}

/// K线源 = trading-service 的 [`KlineClient`] 实现。
pub struct TradingServiceKlineClient {
    /// symbol → 扇出通道（router 任务持 sender 克隆写入；attach 只 subscribe）
    fanout: HashMap<String, broadcast::Sender<KlineEvent>>,
    /// 本地 K线数据根（`data/klines`，其下 `1m/**/{SYMBOL}_1m.csv`）
    data_dir: PathBuf,
    /// symbol → (strategy_type, exchange)：`get_klines` 远端回填的路由表
    ///（装配期按订阅分组登记；无路由 = 该 symbol 不可远端回填）
    routes: HashMap<String, (String, String)>,
    /// 共享交易连接（get_klines 命令通道）
    trading: Arc<TradingServiceClient>,
}

impl TradingServiceKlineClient {
    /// 建扇出通道并 spawn router（broadcast → 按 symbol 投递）。
    /// 订阅帧由调用方（bootstrap）经 `TradingServiceClient::subscribe_klines`
    /// 发起——重连重发在连接任务内部完成，此处无需干预。
    pub fn start(
        symbols: &[String],
        trading: Arc<TradingServiceClient>,
        data_dir: PathBuf,
        routes: HashMap<String, (String, String)>,
        handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let mut fanout = HashMap::new();
        for s in symbols {
            let (tx, _) = broadcast::channel(FANOUT_CAP);
            fanout.insert(s.clone(), tx);
        }
        debug!(
            ?symbols,
            "K线源 trading_service：router 启动（broadcast → 按 symbol 扇出）"
        );
        let mut router = trading.kline_updates();
        let fan = fanout.clone();
        handle.spawn(async move {
            let mut delivered: u64 = 0;
            let mut dropped: u64 = 0;
            let mut deduped: u64 = 0;
            // (venue, symbol) → 已投递的最新 start_ms：多策略类型订阅同
            // symbol 时同 bar 会从多条连接各到一份，水位去重保证下游每根
            // 只投一次（Actor 侧还有 last_handed 二道闸，这里先省通道容量）。
            let mut last_seen: HashMap<(String, String), i64> = HashMap::new();
            loop {
                match router.recv().await {
                    Ok(u) => {
                        let Some((fan_key, tx)) = fanout_key(&fan, &u.symbol) else {
                            // 扇出口径错配（如 CTP 品种前缀订阅后帧内变主力合约）
                            // 必须可见——静默丢弃等于无声断流，升 warn 并计数。
                            dropped += 1;
                            if dropped == 1 || dropped % 60 == 1 {
                                warn!(
                                    symbol = %u.symbol,
                                    dropped,
                                    "K线帧 symbol 不在订阅扇出集（已丢弃）：核对订阅名与帧内 symbol 口径——CTP 品种前缀订阅时帧内为主力合约，换月会变"
                                );
                            }
                            continue;
                        };
                        let key = (u.venue.clone(), fan_key.clone());
                        if let Some(&prev) = last_seen.get(&key) {
                            if u.start_ms <= prev {
                                deduped += 1;
                                if deduped == 1 || deduped % 300 == 1 {
                                    debug!(
                                        symbol = %u.symbol,
                                        deduped,
                                        "K线重复帧（多连接各推一份），router 水位去重丢弃"
                                    );
                                }
                                continue;
                            }
                        }
                        last_seen.insert(key, u.start_ms);
                        delivered += 1;
                        info!(
                            delivered,
                            subscribe_symbol = %fan_key,
                            frame_symbol = %u.symbol,
                            venue = %u.venue,
                            start_ms = u.start_ms,
                            end_ms = u.end_ms,
                            open = u.open,
                            high = u.high,
                            low = u.low,
                            close = u.close,
                            volume = u.volume,
                            quote_volume = u.quote_volume,
                            trade_num = u.trade_num,
                            active_buy_volume = u.active_buy_volume,
                            active_buy_quote_volume = u.active_buy_quote_volume,
                            "K线源 trading_service：定稿 bar 投递（完整 OHLCV）"
                        );
                        // Closed 语义：kline_1m(_batch) 恒为定稿 bar（Tick 快照本路径不产生）
                        let _ = tx.send(KlineEvent::Closed(to_bar(u)));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // router 落后：丢弃 n 根；各 symbol 消费端经 Lagged →
                        // resync_backfill（get_klines/本地 CSV + 后续帧）自愈。
                        warn!(skipped = n, "trading_service K线 router 消费落后（Lagged），跳过的 bar 由消费端补缺");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("trading_service K线 router：上游关闭，退出");
                        return;
                    }
                }
            }
        });
        Arc::new(Self {
            fanout,
            data_dir,
            routes,
            trading,
        })
    }

    /// 经路由 (strategy_type, exchange) 向服务端取一页历史。
    /// `start_ms` = 含；`end_ms` = 排他上界。缺省 `start_ms` 取窗口内最近
    /// `limit` 根（向更早翻页）；给了 `start_ms` 取窗口内最早 `limit` 根
    /// （正向翻页），续翻步长见 §8.5。
    async fn remote_page(
        &self,
        symbol: &str,
        start_ms: Option<i64>,
        end_ms: Option<i64>,
        limit: usize,
    ) -> anyhow::Result<KlineHistory> {
        let (stype, exchange) = self
            .routes
            .get(symbol)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("装配期未登记该 symbol 的路由，无法远端回填"))?;
        let wire_sym = wire_symbol(&exchange, symbol);
        self.trading
            .get_klines(
                &stype,
                &exchange,
                &wire_sym,
                start_ms,
                end_ms,
                Some(limit as u32),
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }

    /// 正向补齐：从 `start_ms`（含）向 `end_ms` 逐页取升序 bar。服务端
    /// `start_time` 语义 = 窗口内最早 limit 根；续翻起点 = 本页最晚 bar + 1m。
    async fn fill_forward(
        &self,
        symbol: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> anyhow::Result<Vec<Candle>> {
        let mut out = Vec::new();
        let mut cursor = start_ms;
        while cursor < end_ms {
            let page = self
                .remote_page(symbol, Some(cursor), Some(end_ms), GET_KLINES_PAGE)
                .await?;
            if page.bars.is_empty() {
                break;
            }
            let bars: Vec<Candle> = page
                .bars
                .iter()
                .map(update_to_candle)
                .filter(|c| c.open_time.timestamp_millis() >= cursor)
                .collect();
            if bars.is_empty() {
                break;
            }
            let next = bars
                .last()
                .map(|c| c.open_time.timestamp_millis() + 60_000)
                .unwrap_or(cursor + 60_000);
            out.extend(bars);
            if next <= cursor {
                break; // 服务端未按 start_time 前移：防死循环
            }
            cursor = next;
            if !page.has_more {
                break;
            }
        }
        Ok(out)
    }

    /// 向更早方向补足到 `target` 根：`candles` 为空时取最近 `target` 根，
    /// 否则以已有最早一根为排他上界续翻。远端失败保留已有部分（返回值第二
    /// 项标记失败，调用方决定是否告警）。
    async fn fill_older(
        &self,
        symbol: &str,
        mut candles: Vec<Candle>,
        target: usize,
    ) -> (Vec<Candle>, bool) {
        let mut remote_failed = false;
        loop {
            let need = target.saturating_sub(candles.len());
            if need == 0 {
                break;
            }
            let end_ms = candles.first().map(|c| c.open_time.timestamp_millis());
            match self
                .remote_page(symbol, None, end_ms, need.min(GET_KLINES_PAGE))
                .await
            {
                Ok(page) => {
                    if page.bars.is_empty() {
                        break; // 服务端更早方向无数据
                    }
                    let cutoff = candles.first().map(|c| c.open_time);
                    let mut more: Vec<Candle> = page
                        .bars
                        .iter()
                        .map(update_to_candle)
                        .filter(|c| cutoff.is_none_or(|t| c.open_time < t))
                        .collect();
                    if more.is_empty() {
                        break; // 服务端未按 end_time 前移：防死循环
                    }
                    more.append(&mut candles);
                    candles = more;
                    if !page.has_more {
                        break;
                    }
                }
                Err(e) => {
                    warn!(
                        symbol,
                        error = %e,
                        "get_klines 远端回填失败（本地 CSV 已有部分结果则照常返回）"
                    );
                    remote_failed = true;
                    break;
                }
            }
        }
        (candles, remote_failed)
    }
}

/// 本地旧序列 + 正向补齐序列按 open_time 合并去重（后者覆盖同刻），
/// 升序，尾部保留最多 `bars` 根。
fn merge_forward(local: Vec<Candle>, forward: Vec<Candle>, bars: usize) -> Vec<Candle> {
    let mut by_ts: BTreeMap<i64, Candle> = BTreeMap::new();
    for c in local {
        by_ts.insert(c.open_time.timestamp_millis(), c);
    }
    for c in forward {
        by_ts.insert(c.open_time.timestamp_millis(), c);
    }
    let mut all: Vec<Candle> = by_ts.into_values().collect();
    if all.len() > bars {
        all = all.split_off(all.len() - bars);
    }
    all
}

/// KlineUpdate → Candle（本路径全部为定稿 bar，is_closed 恒 true）。
fn update_to_candle(u: &KlineUpdate) -> Candle {
    let open_time = Utc
        .timestamp_millis_opt(u.start_ms)
        .single()
        .unwrap_or_else(Utc::now);
    Candle {
        open_time,
        open: u.open,
        high: u.high,
        low: u.low,
        close: u.close,
        volume: u.volume,
        quote_volume: u.quote_volume,
        trade_num: u.trade_num,
        active_buy_volume: u.active_buy_volume,
        active_buy_quote_volume: u.active_buy_quote_volume,
        is_closed: true,
    }
}

/// 线推 bar → 端口 KlineBar（is_closed 恒 true：本路径只收定稿）。
fn to_bar(u: KlineUpdate) -> KlineBar {
    let symbol = u.symbol.clone();
    KlineBar {
        symbol,
        candle: update_to_candle(&u),
    }
}

/// 在 `{base}/1m` 下递归找 `{SYMBOL}_1m.csv`（大小写不敏感，深度 ≤4）。
fn find_symbol_csv(base: &Path, symbol: &str) -> Option<PathBuf> {
    fn walk(dir: &Path, want: &str, depth: u8, out: &mut Option<PathBuf>) {
        if depth > 4 || out.is_some() {
            return;
        }
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, want, depth + 1, out);
            } else if p
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.eq_ignore_ascii_case(&format!("{want}_1m.csv")))
            {
                *out = Some(p);
                return;
            }
        }
    }
    let mut out = None;
    walk(&base.join("1m"), symbol, 0, &mut out);
    out
}

impl KlineClient for TradingServiceKlineClient {
    /// 预热/补缺回填：本地 1m CSV 打底；尾部陈旧先正向补齐到 now，再向更早
    /// 方向补足到 `bars` 根；两路皆空才报错。本数据源只供给 1m；大周期由
    /// Feed 聚合。
    fn backfill<'a>(
        &'a self,
        symbol: &'a str,
        timeframe: Timeframe,
        bars: usize,
    ) -> strategy_data::client::BackfillFuture<'a> {
        Box::pin(async move {
            if timeframe != Timeframe::M1 {
                anyhow::bail!(
                    "trading_service K线源仅支持 1m 回填（大周期由本地 Feed 聚合），请求 {timeframe}"
                );
            }
            let local: Vec<Candle> = match find_symbol_csv(&self.data_dir, symbol) {
                Some(path) => {
                    let all = strategy_data::csv_store::load_csv(&path)?;
                    let n = all.len();
                    all.into_iter().skip(n.saturating_sub(bars)).collect()
                }
                None => Vec::new(),
            };
            let route_ok = self.routes.contains_key(symbol);
            let now_ms = Utc::now().timestamp_millis();
            let mut remote_failed = false;
            let mut candles = local;
            // ① 尾部陈旧：缺口在预热窗口内时正向补到 now；缺口大于窗口时
            // 正向补齐会被尾部截断丢弃，直接取最近 bars 更省请求。
            if route_ok {
                if let Some(last_ms) = candles.last().map(|c| c.open_time.timestamp_millis()) {
                    let stale =
                        now_ms.saturating_sub(last_ms) > TAIL_STALE_TOLERANCE.as_millis() as i64;
                    if stale {
                        let gap_bars = (now_ms - last_ms).max(0) / 60_000;
                        if gap_bars > bars as i64 {
                            let (recent, failed) = self.fill_older(symbol, Vec::new(), bars).await;
                            remote_failed |= failed;
                            if !recent.is_empty() {
                                candles = recent;
                            }
                        } else {
                            match self.fill_forward(symbol, last_ms + 60_000, now_ms).await {
                                Ok(forward) => {
                                    candles = merge_forward(candles, forward, bars);
                                }
                                Err(e) => {
                                    warn!(
                                        symbol,
                                        error = %e,
                                        "get_klines 尾部补齐失败（保留本地陈旧尾部，启动期将告警）"
                                    );
                                    remote_failed = true;
                                }
                            }
                        }
                    }
                }
            }
            // ② 仍不足预热窗口：向更早方向补足（无本地时等价取最近 bars）。
            if route_ok && candles.len() < bars {
                let (filled, failed) = self.fill_older(symbol, candles, bars).await;
                candles = filled;
                remote_failed |= failed;
            }
            if candles.is_empty() {
                anyhow::bail!(
                    "无 {symbol} 的 1m 历史：本地 {} 下未找到 {symbol}_1m.csv，{}也失败",
                    self.data_dir.join("1m").display(),
                    if remote_failed {
                        "远端 get_klines"
                    } else {
                        "该 symbol 无远端路由/服务端无更早数据"
                    }
                );
            }
            if candles.len() > bars {
                candles = candles.split_off(candles.len() - bars);
            }
            Ok(candles)
        })
    }

    fn attach(&self, symbol: &str, timeframe: Timeframe) -> KlineStream {
        debug_assert_eq!(
            timeframe,
            Timeframe::M1,
            "trading_service K线源只推送 1m，大周期由本地 Feed 聚合"
        );
        match self.fanout.get(symbol) {
            Some(tx) => KlineStream { rx: tx.subscribe() },
            None => {
                warn!(
                    symbol,
                    "attach 未在启动集合中的 symbol（trading_service 源）：返回永不收帧的空流"
                );
                let (tx, rx) = broadcast::channel(1);
                drop(tx);
                KlineStream { rx }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;
    use std::time::Duration;
    use strategy_broker::{MockScenario, MockServer};

    /// CTP 帧内是主力合约（fu2701），路由必须落回品种前缀订阅键；crypto 不作前缀匹配。
    #[test]
    fn fanout_key_routes_ctp_main_contract_to_product_prefix() {
        let mut fan: HashMap<String, broadcast::Sender<KlineEvent>> = HashMap::new();
        let (tx, _) = broadcast::channel(4);
        fan.insert("FU".to_string(), tx);
        let (key, _) = fanout_key(&fan, "fu2701").expect("主力合约应路由到品种前缀");
        assert_eq!(key, "FU");
        assert!(fanout_key(&fan, "BTCUSDT").is_none());
        assert_eq!(ctp_product_prefix("fu2701").as_deref(), Some("FU"));
        assert_eq!(ctp_product_prefix("BTCUSDT"), None);
    }

    #[test]
    fn wire_symbol_lowercases_ctp_only() {
        assert_eq!(wire_symbol("ctp", "FU"), "fu");
        assert_eq!(wire_symbol("binance", "BTCUSDT"), "BTCUSDT");
    }

    fn write_csv(dir: &Path, rel: &str, rows: &str) {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(
            &p,
            format!(
                "timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume\n{rows}"
            ),
        )
        .unwrap();
    }

    fn test_client(
        url: String,
    ) -> (
        Arc<TradingServiceClient>,
        tokio::sync::mpsc::Receiver<strategy_broker::ClientEvent>,
    ) {
        // 事件接收端必须存活：connection_loop 发 Reconnected 失败即退出。
        TradingServiceClient::start_arc(
            strategy_broker::TradingConfig {
                ws_url: url,
                submit_enabled: false,
                request_timeout: Duration::from_secs(3),
                strategy_secret: "sec".into(),
                reconnect_backoff_max_secs: 4,
            },
            tokio::runtime::Handle::current(),
        )
    }

    fn btc_routes() -> HashMap<String, (String, String)> {
        HashMap::from([(
            "BTCUSDT".to_string(),
            ("t".to_string(), "binance".to_string()),
        )])
    }

    /// 相对当前时刻的整分钟 CSV 行（测试用：保证尾部陈旧判定与 now 对齐）。
    fn minute_row(minutes_ago: i64, price: f64) -> String {
        use chrono::Timelike;
        let t = (Utc::now() - chrono::Duration::minutes(minutes_ago))
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        format!(
            "{},{price},{price},{price},{price},1,1,1,1,1",
            t.format("%Y-%m-%d %H:%M:%S+00:00")
        )
    }

    /// 本地尾部陈旧（15 分钟前）→ 远端从本地末根向 now 正向补齐缺口，
    /// 不得只返回陈旧局部数据。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backfill_fills_stale_tail_forward_to_now() {
        let local = tempfile::tempdir().unwrap();
        write_csv(
            local.path(),
            "1m/binance/crypto_perpetual/BTCUSDT_1m.csv",
            &minute_row(15, 100.0),
        );
        let kdir = tempfile::tempdir().unwrap();
        let rows: Vec<String> = (1..=14).rev().map(|m| minute_row(m, 101.0)).collect();
        write_csv(kdir.path(), "1m/BTCUSDT_1m.csv", &rows.join("\n"));
        let (trading, _events) = mock_with_klines(kdir.path()).await;
        let adapter = TradingServiceKlineClient::start(
            &["BTCUSDT".to_string()],
            trading.clone(),
            local.path().to_path_buf(),
            btc_routes(),
            tokio::runtime::Handle::current(),
        );
        let v = tokio::time::timeout(
            Duration::from_secs(10),
            adapter.backfill("BTCUSDT", Timeframe::M1, 20),
        )
        .await
        .expect("尾部补齐应在 10s 内返回")
        .expect("回填成功");
        assert_eq!(v.len(), 15, "1 根本地 + 14 根缺口全部补齐");
        let aligned_last = (Utc::now() - chrono::Duration::minutes(15))
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        assert_eq!(
            v.first().unwrap().open_time,
            aligned_last,
            "本地陈旧根仍在窗口内"
        );
        assert_eq!(
            v.last().unwrap().open_time,
            aligned_last + chrono::Duration::minutes(14),
            "尾部必须推进到当前时刻前一分钟"
        );
        assert!(v.windows(2).all(|w| w[0].open_time < w[1].open_time));
    }

    /// 缺口超过预热窗口时直接取最近 N 根，不从陈旧末根逐页扫过来（避免
    /// 无界请求量），保证返回窗口尾部紧贴 now。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backfill_gap_larger_than_window_prefers_recent_bars() {
        let local = tempfile::tempdir().unwrap();
        write_csv(
            local.path(),
            "1m/binance/crypto_perpetual/BTCUSDT_1m.csv",
            &minute_row(40, 90.0),
        );
        let kdir = tempfile::tempdir().unwrap();
        let rows: Vec<String> = (1..=10).rev().map(|m| minute_row(m, 101.0)).collect();
        write_csv(kdir.path(), "1m/BTCUSDT_1m.csv", &rows.join("\n"));
        let (trading, _events) = mock_with_klines(kdir.path()).await;
        let adapter = TradingServiceKlineClient::start(
            &["BTCUSDT".to_string()],
            trading.clone(),
            local.path().to_path_buf(),
            btc_routes(),
            tokio::runtime::Handle::current(),
        );
        let v = tokio::time::timeout(
            Duration::from_secs(10),
            adapter.backfill("BTCUSDT", Timeframe::M1, 5),
        )
        .await
        .expect("最近窗口回填应在 10s 内返回")
        .expect("回填成功");
        assert_eq!(v.len(), 5, "只取最近 5 根");
        let aligned_now = (Utc::now() - chrono::Duration::minutes(1))
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap();
        assert_eq!(v.last().unwrap().open_time, aligned_now);
        assert!(
            v.iter()
                .all(|c| c.open_time >= aligned_now - chrono::Duration::minutes(4)),
            "窗口内不得混入 40 分钟前的陈旧根"
        );
    }

    async fn mock_with_klines(
        dir: &Path,
    ) -> (
        Arc<TradingServiceClient>,
        tokio::sync::mpsc::Receiver<strategy_broker::ClientEvent>,
    ) {
        let (scenario, errs) =
            MockScenario::new("sec").kline_dir(dir.join("1m"), Duration::from_millis(30), None);
        assert!(errs.is_empty());
        let server = MockServer::bind("127.0.0.1:0", scenario).await.unwrap();
        let port = server.local_addr().unwrap().port();
        tokio::spawn(server.serve());
        test_client(format!("ws://127.0.0.1:{port}/ws"))
    }

    /// 端到端：mock 回放 CSV → kline_updates broadcast → router 扇出 →
    /// attach 流收到 Closed(KlineBar)（策略管线所见的最终形态）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adapter_streams_mock_replay_as_closed_bars() {
        let dir = tempfile::tempdir().unwrap();
        write_csv(
            dir.path(),
            "1m/BTCUSDT_1m.csv",
            "2025-12-01 13:00:00+00:00,100,101,99,101,10,1010,1,1,101\n\
             2025-12-01 13:01:00+00:00,101,102,100,102,11,1122,2,2,204\n",
        );
        let (trading, _events) = mock_with_klines(dir.path()).await;
        let symbols = vec!["BTCUSDT".to_string()];
        let adapter = TradingServiceKlineClient::start(
            &symbols,
            trading.clone(),
            dir.path().to_path_buf(),
            btc_routes(),
            tokio::runtime::Handle::current(),
        );
        let mut stream = adapter.attach("BTCUSDT", Timeframe::M1);
        trading
            .subscribe_klines("t", "binance", &symbols)
            .await
            .unwrap();
        let ev = tokio::time::timeout(Duration::from_secs(5), stream.rx.recv())
            .await
            .expect("Closed bar 应在 5s 内到达")
            .expect("通道未关闭");
        match ev {
            KlineEvent::Closed(bar) => {
                assert_eq!(bar.symbol, "BTCUSDT");
                assert!(bar.candle.is_closed, "定稿语义");
                assert_eq!(bar.candle.close, 101.0);
                let want = chrono::DateTime::parse_from_str(
                    "2025-12-01 13:00:00+00:00",
                    "%Y-%m-%d %H:%M:%S%:z",
                )
                .unwrap()
                .with_timezone(&Utc);
                assert_eq!(bar.candle.open_time, want);
            }
            other => panic!("期望 Closed，得到 {other:?}"),
        }
        let ev2 = tokio::time::timeout(Duration::from_secs(5), stream.rx.recv())
            .await
            .expect("第二根")
            .expect("通道");
        if let KlineEvent::Closed(b) = ev2 {
            assert_eq!(b.candle.close, 102.0);
        } else {
            panic!("期望 Closed");
        }
    }

    /// 多策略同 symbol：一条 client 两条连接各推一份同 bar → router
    /// (venue,symbol) 水位去重，下游每根只收一次、无重复无丢失。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn router_dedupes_duplicate_stream_bars() {
        let dir = tempfile::tempdir().unwrap();
        write_csv(
            dir.path(),
            "1m/BTCUSDT_1m.csv",
            "2025-12-01 13:00:00+00:00,100,101,99,101,10,1010,1,1,101\n\
             2025-12-01 13:01:00+00:00,101,102,100,102,11,1122,2,2,204\n",
        );
        let (trading, _events) = mock_with_klines(dir.path()).await;
        let symbols = vec!["BTCUSDT".to_string()];
        let adapter = TradingServiceKlineClient::start(
            &symbols,
            trading.clone(),
            dir.path().to_path_buf(),
            HashMap::new(),
            tokio::runtime::Handle::current(),
        );
        let mut stream = adapter.attach("BTCUSDT", Timeframe::M1);
        // 两个策略类型订阅同 symbol ⇒ 两条连接各推 2 根 = 4 帧入 broadcast
        trading
            .subscribe_klines("s1", "binance", &symbols)
            .await
            .unwrap();
        trading
            .subscribe_klines("s2", "binance", &symbols)
            .await
            .unwrap();
        for want in [101.0, 102.0] {
            let ev = tokio::time::timeout(Duration::from_secs(5), stream.rx.recv())
                .await
                .expect("去重后每根仍应在 5s 内到达")
                .expect("通道未关闭");
            match ev {
                KlineEvent::Closed(b) => assert_eq!(b.candle.close, want),
                other => panic!("期望 Closed，得到 {other:?}"),
            }
        }
        // 两连接共 4 帧，去重后只投 2 —— 其后不得再有帧泄漏
        let idle = tokio::time::timeout(Duration::from_millis(800), stream.rx.recv()).await;
        match idle {
            Err(_) => {}
            Ok(other) => panic!("重复帧漏网: {other:?}"),
        }
    }

    /// 本地 CSV 预热回填：尾部 bars 根、升序、is_closed=true（need=0 不触远端）。
    #[tokio::test]
    async fn backfill_reads_tail_of_local_csv() {
        let dir = tempfile::tempdir().unwrap();
        write_csv(
            dir.path(),
            "1m/binance/crypto_perpetual/SOLUSDT_1m.csv",
            "2025-12-01 00:00:00+00:00,1,1,1,1,1,1,1,1,1\n\
             2025-12-01 00:01:00+00:00,2,2,2,2,2,2,2,2,2\n\
             2025-12-01 00:02:00+00:00,3,3,3,3,3,3,3,3,3\n",
        );
        // 不可达连接即可：本用例 need=0，绝不触远端
        let (dead, _ev) = TradingServiceClient::start_arc(
            strategy_broker::TradingConfig {
                ws_url: "ws://127.0.0.1:1".into(),
                strategy_secret: "sec".into(),
                request_timeout: Duration::from_millis(200),
                ..Default::default()
            },
            tokio::runtime::Handle::current(),
        );
        let adapter = TradingServiceKlineClient::start(
            &[],
            dead,
            dir.path().to_path_buf(),
            HashMap::new(),
            tokio::runtime::Handle::current(),
        );
        let v = adapter.backfill("SOLUSDT", Timeframe::M1, 2).await.unwrap();
        assert_eq!(v.len(), 2, "尾部 2 根");
        assert_eq!(v[0].open, 2.0);
        assert_eq!(v[1].open, 3.0);
        assert!(v.iter().all(|c| c.is_closed));
        assert!(
            adapter.backfill("NOPE", Timeframe::M1, 2).await.is_err(),
            "本地缺文件且无远端路由应报错而非空成功"
        );
    }

    /// 本地无 CSV 时经 get_klines 从服务端补齐（mock 与真实服务同接口 §10.4）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backfill_uses_get_klines_when_local_missing() {
        let kdir = tempfile::tempdir().unwrap();
        write_csv(
            kdir.path(),
            "1m/BTCUSDT_1m.csv",
            "2025-12-01 13:00:00+00:00,100,101,99,101,10,1010,1,1,101\n\
             2025-12-01 13:01:00+00:00,101,102,100,102,11,1122,2,2,204\n",
        );
        let (trading, _events) = mock_with_klines(kdir.path()).await;
        let empty_local = tempfile::tempdir().unwrap();
        let adapter = TradingServiceKlineClient::start(
            &["BTCUSDT".to_string()],
            trading.clone(),
            empty_local.path().to_path_buf(),
            btc_routes(),
            tokio::runtime::Handle::current(),
        );
        let v = tokio::time::timeout(
            Duration::from_secs(5),
            adapter.backfill("BTCUSDT", Timeframe::M1, 2),
        )
        .await
        .expect("远端回填应在 5s 内返回")
        .expect("get_klines 回填成功");
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].open, 100.0, "升序最早根");
        assert_eq!(v[1].open, 101.0);
        assert!(v.iter().all(|c| c.is_closed));
    }

    /// 本地+远端合并去重：本地已有 13:01，远端页仅补更早的 13:00。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backfill_merges_local_and_remote_without_dupes() {
        let kdir = tempfile::tempdir().unwrap();
        write_csv(
            kdir.path(),
            "1m/BTCUSDT_1m.csv",
            "2025-12-01 13:00:00+00:00,100,101,99,101,10,1010,1,1,101\n\
             2025-12-01 13:01:00+00:00,101,102,100,102,11,1122,2,2,204\n",
        );
        let (trading, _events) = mock_with_klines(kdir.path()).await;
        let local = tempfile::tempdir().unwrap();
        write_csv(
            local.path(),
            "1m/BTCUSDT_1m.csv",
            "2025-12-01 13:01:00+00:00,101,102,100,102,11,1122,2,2,204\n",
        );
        let adapter = TradingServiceKlineClient::start(
            &["BTCUSDT".to_string()],
            trading.clone(),
            local.path().to_path_buf(),
            btc_routes(),
            tokio::runtime::Handle::current(),
        );
        let v = tokio::time::timeout(
            Duration::from_secs(5),
            adapter.backfill("BTCUSDT", Timeframe::M1, 2),
        )
        .await
        .expect("合并回填应在 5s 内返回")
        .expect("回填成功");
        assert_eq!(v.len(), 2, "去重后恰 2 根");
        assert_eq!(v[0].open, 100.0, "远端补更早根在前");
        assert_eq!(v[1].open, 101.0);
    }
}
