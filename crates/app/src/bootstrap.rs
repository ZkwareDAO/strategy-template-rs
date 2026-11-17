//! live Bootstrap（设计文档 §14）
//!
//! 装配顺序：配置 → 数据/交易客户端 →（每策略实例）live actor 监督启动
//! （`spi_live_actor::spawn_spi_live`，内部完成回填订阅、策略决策、信号/仓位、对账）
//! → T7 订单转发 → 等 Ctrl-C 优雅关闭。
//!
//! 外部服务不可达时连接 task 内部退避重连，本函数不返回错误（fail-fast 只
//! 针对"未配置 services"）。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, FixedOffset, NaiveTime, TimeZone, Utc, Weekday,
};
#[cfg(test)]
use strategy_broker::BacktestOrderClient;
use strategy_broker::{
    BacktestConfig, ClientEvent, OrderVenueCfg, TradingConfig, TradingGateway, TradingServiceClient,
};
use strategy_core::candle::Candle;
use strategy_core::contract::ContractSpec;
use strategy_data::client::{KlineEvent, KlineStream};
use strategy_data::csv_store::{self, LiveCsvSink};
use strategy_data::KlineClient;
use strategy_signals::gateway::AsyncOrderGateway;
use strategy_store::{EpochClock, Store};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{ServicesConfig, SymbolSpec};
use crate::order_router::OrderRouter;
use crate::runtime_types::{RouteBind, RuntimeHandle};

/// warmup 尾部超过该秒数仍未推进到 now → 告警（1m 源正常只落后数十秒）。
const WARMUP_STALE_WARN_SECS: i64 = 300;

/// CTP 休市（午休/夜盘收盘/周末/节假日）期间，最后一根与 now 的差可达数十
/// 小时；按「最近一次真实收盘」判定新鲜度，回看窗口覆盖周末 + 长假。
const CT_SESSION_LOOKBACK_DAYS: i64 = 12;

/// 收盘时刻与最后一根 1m 的固有差：bar 的 open_time 在收盘前一分钟。
const WARMUP_FINAL_BAR_SLACK_SECS: i64 = 120;

/// 期货合约最近一次真实收盘（UTC）；`None` = 合约未配置时段表。
fn latest_ct_session_close(contract: &ContractSpec, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    if contract.session_opens.is_empty()
        || contract.session_closes.len() != contract.session_opens.len()
    {
        return None;
    }
    let cst = FixedOffset::east_opt(8 * 3600)?;
    let local_date = now.with_timezone(&cst).date_naive();
    let mut best: Option<DateTime<Utc>> = None;
    for back in 0..=CT_SESSION_LOOKBACK_DAYS {
        let d = local_date - ChronoDuration::days(back);
        // 只认工作日开盘：否则周末的幻影时段会把「最近收盘」推到周日凌晨。
        if !matches!(
            d.weekday(),
            Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu | Weekday::Fri
        ) {
            continue;
        }
        for (open_raw, close_raw) in contract.session_opens.iter().zip(&contract.session_closes) {
            let (Ok(open), Ok(close)) = (
                NaiveTime::parse_from_str(open_raw, "%H:%M"),
                NaiveTime::parse_from_str(close_raw, "%H:%M"),
            ) else {
                continue;
            };
            // close <= open 表示夜盘跨零点（如 21:00→01:00）。
            let close_naive = if close > open {
                d.and_time(close)
            } else {
                (d + ChronoDuration::days(1)).and_time(close)
            };
            let Some(close_utc) = cst
                .from_local_datetime(&close_naive)
                .single()
                .map(|dt| dt.with_timezone(&Utc))
            else {
                continue;
            };
            if close_utc <= now && best.is_none_or(|b| close_utc > b) {
                best = Some(close_utc);
            }
        }
    }
    best
}

/// warmup 尾部是否可视为新鲜：连续交易（crypto）按固定容忍窗口；期货在
/// 休市期按「最后一根不得早于最近一次收盘」判定，消除午休/周末误报。
fn warmup_tail_fresh(
    contract: Option<&ContractSpec>,
    last_bar: DateTime<Utc>,
    now: DateTime<Utc>,
) -> bool {
    if now - last_bar <= ChronoDuration::seconds(WARMUP_STALE_WARN_SECS) {
        return true;
    }
    contract
        .and_then(|c| latest_ct_session_close(c, now))
        .is_some_and(|close| {
            last_bar >= close - ChronoDuration::seconds(WARMUP_FINAL_BAR_SLACK_SECS)
        })
}

/// 运行时策略名（Cooldown 行键 = 实例名，§15 positions 面）：
/// `{strategy}_{TF}_3_{venue_key 下划线形}_{MODE}`（阶段 5c run 合并步，§6.1：
/// symbol 段换命名空间键——同名裸 symbol 跨交易所实例互斥；venue 段与
/// engine runner id 前缀 `sig_{namespaced_key 下划线形}_` 逐字同形，单一源）。
pub fn runtime_name(strategy: &str, timeframe: &str, venue_key: &str, mode: &str) -> String {
    format!(
        "{}_{}_3_{}_{}",
        strategy,
        timeframe.to_uppercase(),
        venue_key.replace('/', "_"),
        mode.to_uppercase()
    )
}

/// 启动时的 per-symbol mode 表格（U15：肉眼核对 live/paper 分流）
#[cfg(test)]
pub(crate) fn format_mode_table(specs: &[SymbolSpec]) -> String {
    let mut out = String::from("运行实例 mode 一览：\n");
    for s in specs {
        out.push_str(&format!("  {} {:>14} → {}\n", s.symbol, s.strategy, s.mode));
    }
    out
}

/// K 线协议使用交易所精确名（CTP 为 `ctp`）；引擎内部行键把 ctp/shfe
/// 归一为 `SHFE`。订阅帧与 `get_klines` 必须发服务端名，否则回 error 帧。
fn wire_exchange(exchange: &str) -> String {
    match exchange {
        "SHFE" => "ctp".to_string(),
        other => other.to_string(),
    }
}

/// 服务端 CTP 品种前缀按小写匹配；大写订阅会被静默忽略（无帧无 error 帧）。
fn wire_symbols(exchange: &str, symbols: &[String]) -> Vec<String> {
    if exchange == "ctp" {
        symbols.iter().map(|s| s.to_ascii_lowercase()).collect()
    } else {
        symbols.to_vec()
    }
}

/// 按 per-symbol mode 装配下单网关：
/// - `live` → [`TradingGateway`]（真单通道，实例闸门放行）
/// - `paper_trading` → [`BacktestOrderClient`]（本地撮合，不经交易 WS）
#[cfg(test)]
pub(crate) fn make_gateway(
    mode: &str,
    trading: Arc<TradingServiceClient>,
    strategy_type: String,
    venue: OrderVenueCfg,
    paper: BacktestOrderClient,
) -> Box<dyn AsyncOrderGateway> {
    match mode {
        "live" => Box::new(TradingGateway::with_submit(
            trading,
            strategy_type,
            venue,
            true,
        )),
        _ => Box::new(paper),
    }
}

/// paper 本地撮合内核配置：由 per-symbol 规格映射（初始权益 / 杠杆 / 单笔名义 /
/// 合约参数 / 交易所）。`run_live` 的 paper 工厂与集成测试共用本函数，保证配置合约
/// （`SymbolSpec.contract`）必然透传进撮合内核；合约为 None 时撮合退化为 crypto 小数数量口径。
pub fn paper_backtest_config(spec: &SymbolSpec) -> BacktestConfig {
    BacktestConfig {
        cash: spec.initial_cash.unwrap_or(5000.0),
        leverage: spec.leverage,
        notional_per_order: spec.cash,
        contract: spec.contract.clone(),
        exchange: spec.exchange.clone(),
        ..Default::default()
    }
}

// ─── 横截面 run（PR-5 T6 / PR-6 多组）────────────────────────────────

/// 同进程多横截面组的运行句柄：逐组 shutdown，再统一收口 actor 任务。
pub struct StrategyLiveSet {
    handles: Vec<RuntimeHandle>,
    actor_tasks: Vec<JoinHandle<()>>,
    forwarders: Vec<JoinHandle<()>>,
    order_forwarder: JoinHandle<()>,
}

impl StrategyLiveSet {
    /// 优雅关停：逐组 shutdown（actor 线程单写者 flush）→ 8s 收口每代
    /// 监督任务 → abort K 线 forwarder 与订单转发器。
    pub async fn shutdown(self) {
        for handle in &self.handles {
            let _ = handle.shutdown().await;
        }
        futures::future::join_all(self.actor_tasks.into_iter().map(|task| async move {
            let _ = tokio::time::timeout(Duration::from_secs(8), task).await;
        }))
        .await;
        self.order_forwarder.abort();
        for forwarder in self.forwarders {
            forwarder.abort();
        }
    }
}

/// 启动多组横截面 live/paper 运行时（每组一 actor 监督代），返回可关停句柄。
///
/// 顺序（G3/live 公理）：共享交易客户端 → 按 `(strategy_type, exchange)`
/// 横截面运行期 K线落盘器集合：每 symbol 唯一追加器，多组共享同一份
/// （Mutex 串行化写盘 + 水位幂等），避免同一 CSV 出现多个写者。
type BarSinks = Mutex<HashMap<String, LiveCsvSink>>;

/// 已闭合 1m 扇出：先追加落盘再投递策略数据面。落盘失败只告警——磁盘问题
/// 不得阻断交易所需的行情投递。
async fn forward_closed_bars(
    mut stream: KlineStream,
    symbol: String,
    tx: std::sync::mpsc::Sender<Candle>,
    sinks: Arc<BarSinks>,
) {
    use tokio::sync::broadcast::error::RecvError;
    loop {
        match stream.rx.recv().await {
            Ok(KlineEvent::Closed(b)) if b.symbol == symbol => {
                if let Some(sink) = sinks.lock().unwrap().get_mut(&symbol) {
                    if let Err(e) = sink.push(&b.candle) {
                        warn!(symbol = %symbol, error = %e, "横截面 K线追加 CSV 失败");
                    }
                }
                if tx.send(b.candle).is_err() {
                    break; // actor 已收口
                }
            }
            Ok(_) => {}
            Err(RecvError::Lagged(skipped)) => warn!(
                symbol = %symbol, skipped,
                "横截面 K 线订阅落后，跳帧（重连补缺兜底）"
            ),
            Err(RecvError::Closed) => break,
        }
    }
}

/// 合并订阅（symbol 并集）→ 唯一 symbol warmup 只回填一次、按组截尾部 →
/// 每 symbol 每组 fanout → 每组 `spawn_strategy_live` → 单 OrderRouter。
#[allow(clippy::too_many_arguments)]
pub async fn start_live_strategy_set(
    specs: Vec<crate::strategy_config::StrategySpec>,
    services: ServicesConfig,
    output_dir: PathBuf,
    kline_data_dir: PathBuf,
    run_mode: String,
    state_flush_interval: Duration,
    state_prune_terminal: bool,
) -> anyhow::Result<StrategyLiveSet> {
    use std::collections::{BTreeMap, BTreeSet};

    crate::config::check_kline_retired(&services)?;
    anyhow::ensure!(!specs.is_empty(), "横截面配置集为空");
    for spec in &specs {
        anyhow::ensure!(
            !spec.instruments.is_empty(),
            "横截面组 {} 的 universe 为空",
            spec.strategy_id
        );
    }
    // 防御位：CLI 已校验；直接调 bootstrap 的路径（测试）同样拒启。
    crate::strategy_config::validate_strategy_set(&specs, &run_mode)?;
    // 策略工厂在监督任务内被调用：非法 strategy_type 会 panic 并静默杀死
    // 整组 actor（进程照常运行，该组永不交易）。装配期逐组试构造，点名拒启。
    for spec in &specs {
        crate::strategy_config::build_strategy(spec).map_err(|e| {
            anyhow::anyhow!(
                "横截面组 {} 的 strategy_type={} 无法构造: {e}",
                spec.strategy_id,
                spec.strategy_type
            )
        })?;
    }

    let handle = tokio::runtime::Handle::current();

    // 1) 交易客户端（全组共享一条 WS；与单标的 run 同拓扑）。
    let (trading, mut trading_events) = TradingServiceClient::start_arc(
        TradingConfig {
            ws_url: services.trading.ws_url,
            request_timeout: Duration::from_millis(services.trading.request_timeout_ms),
            submit_enabled: services.trading.submit_enabled,
            reconnect_backoff_max_secs: services.trading.reconnect_backoff_max_secs,
            strategy_secret: services.trading.strategy_secret.clone(),
        },
        handle.clone(),
    );

    // 2) 订阅合并：同 (strategy_type, exchange) 一帧，symbol 取并集。
    let mut subs: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
    for spec in &specs {
        for inst in &spec.instruments {
            subs.entry((spec.strategy_type.clone(), wire_exchange(&inst.exchange)))
                .or_default()
                .insert(inst.symbol.clone());
        }
    }
    for ((strategy_type, exchange), symbols) in &subs {
        let registered: Vec<String> = symbols.iter().cloned().collect();
        let list = wire_symbols(exchange, &registered);
        // 订阅内容必须可见：期货品种前缀与帧内主力合约可能不同名，
        // 出问题时先核对这里登记的 symbol 与服务端推送口径。
        info!(
            strategy_type = %strategy_type,
            exchange = %exchange,
            symbols = ?list,
            "订阅K线（subscribe_kline，interval=1m）"
        );
        if let Err(e) = trading
            .subscribe_klines(strategy_type, exchange, &list)
            .await
        {
            warn!(
                strategy_type = %strategy_type,
                exchange = %exchange,
                error = %e,
                "subscribe_kline 首帧发送失败（连接建立后仍会按登记自动重发）"
            );
        }
    }

    // 3) K 线客户端：唯一 symbol 并集；routes 首个组胜出（paper 允许重叠，
    //    每 symbol 每组各 attach 一个接收端）。
    let mut symbol_set: BTreeSet<String> = BTreeSet::new();
    let mut routes: std::collections::HashMap<String, (String, String)> = Default::default();
    for spec in &specs {
        for inst in &spec.instruments {
            symbol_set.insert(inst.symbol.clone());
            routes
                .entry(inst.symbol.clone())
                .or_insert((spec.strategy_type.clone(), wire_exchange(&inst.exchange)));
        }
    }
    let symbols: Vec<String> = symbol_set.iter().cloned().collect();
    let ts_kline = crate::ts_kline::TradingServiceKlineClient::start(
        &symbols,
        trading.clone(),
        kline_data_dir.clone(),
        routes,
        handle.clone(),
    );
    let kline: Arc<dyn KlineClient> = ts_kline;

    // 4) warmup：每 symbol 按全组最大需求回填一次（本地 CSV 打底 + 远端
    //    尾部补齐 + 新 bar 追加落盘；失败只告警）。
    let mut want_by_symbol: BTreeMap<String, usize> = BTreeMap::new();
    let mut venue_key_by_symbol: BTreeMap<String, String> = BTreeMap::new();
    let mut contract_by_symbol: BTreeMap<String, ContractSpec> = BTreeMap::new();
    for spec in &specs {
        let want = (spec.warmup_days.max(1) as usize) * 24 * 60;
        for inst in &spec.instruments {
            want_by_symbol
                .entry(inst.symbol.clone())
                .and_modify(|max| *max = (*max).max(want))
                .or_insert(want);
            venue_key_by_symbol
                .entry(inst.symbol.clone())
                .or_insert_with(|| inst.venue_key.clone());
            if let Some(c) = &inst.contract {
                contract_by_symbol
                    .entry(inst.symbol.clone())
                    .or_insert_with(|| c.clone());
            }
        }
    }
    let mut backfilled: BTreeMap<String, Vec<strategy_core::candle::Candle>> = BTreeMap::new();
    for (symbol, want) in &want_by_symbol {
        let Some(venue_key) = venue_key_by_symbol.get(symbol) else {
            warn!(
                symbol = %symbol,
                "横截面 warmup 跳过：装配期未登记 venue_key（无法定位/写回本地 CSV）"
            );
            backfilled.insert(symbol.clone(), Vec::new());
            continue;
        };
        let (bars, stats) = strategy_data::warmup::load_warmup(
            &kline_data_dir,
            kline.as_ref(),
            symbol,
            venue_key,
            *want,
        )
        .await;
        // 尾部未推进到 now = 远端补齐失败或服务端无更多数据：显式告警，
        // 不静默让指标跨缺口计算。
        match bars.last() {
            Some(last) => {
                let now = Utc::now();
                let age_secs = (now - last.open_time).num_seconds();
                if !warmup_tail_fresh(contract_by_symbol.get(symbol), last.open_time, now) {
                    warn!(
                        symbol = %symbol,
                        last_bar = %last.open_time,
                        age_secs,
                        local_bars = stats.local_bars,
                        remote_bars = stats.remote_bars,
                        persisted_new = stats.persisted_new,
                        "横截面 warmup 尾部仍陈旧（补齐失败或服务端无更多数据，指标将跨缺口）"
                    );
                } else {
                    info!(
                        symbol = %symbol,
                        local_bars = stats.local_bars,
                        remote_bars = stats.remote_bars,
                        persisted_new = stats.persisted_new,
                        total_bars = stats.total_bars,
                        "横截面 warmup 完成（本地打底 + 远端补齐 + 落盘）"
                    );
                }
            }
            None => warn!(
                symbol = %symbol,
                local_bars = stats.local_bars,
                remote_bars = stats.remote_bars,
                "横截面 warmup 为空（本地无 CSV 且远端无数据；entry 门兜底）"
            ),
        }
        backfilled.insert(symbol.clone(), bars);
    }

    // 5) 每组每标的 channel 扇出（attach → std mpsc；水位去重/重连补缺沿 ts_kline）。
    //    落盘器在 warmup 之后、扇出之前打开：水位接续盘载末根，运行期每根
    //    闭合 1m 即时追加（多组同 symbol 共用同一写者）。
    let bar_sinks: Arc<BarSinks> = Arc::new(Mutex::new(HashMap::new()));
    let mut candle_rxs_by_group: Vec<
        Vec<std::sync::mpsc::Receiver<strategy_core::candle::Candle>>,
    > = Vec::with_capacity(specs.len());
    let mut warmups_by_group: Vec<Vec<Vec<strategy_core::candle::Candle>>> =
        Vec::with_capacity(specs.len());
    let mut forwarders: Vec<JoinHandle<()>> = Vec::new();
    for spec in &specs {
        let want = (spec.warmup_days.max(1) as usize) * 24 * 60;
        let mut candle_rxs = Vec::with_capacity(spec.instruments.len());
        let mut warmups = Vec::with_capacity(spec.instruments.len());
        for inst in &spec.instruments {
            let bars = backfilled
                .get(&inst.symbol)
                .map(|all| all[all.len().saturating_sub(want)..].to_vec())
                .unwrap_or_default();
            warmups.push(bars);

            let stream = kline.attach(&inst.symbol, strategy_core::Timeframe::M1);
            {
                let mut guard = bar_sinks.lock().unwrap();
                if !guard.contains_key(&inst.symbol) {
                    let path = csv_store::resolve_csv_path(
                        &kline_data_dir,
                        &inst.venue_key,
                        strategy_core::candle::Timeframe::M1,
                    );
                    match LiveCsvSink::open(&path) {
                        Ok(sink) => {
                            guard.insert(inst.symbol.clone(), sink);
                        }
                        Err(e) => warn!(
                            symbol = %inst.symbol,
                            path = %path.display(),
                            error = %e,
                            "横截面 K线落盘器打开失败，运行期该 symbol 不落盘"
                        ),
                    }
                }
            }
            let (tx, rx) = std::sync::mpsc::channel::<strategy_core::candle::Candle>();
            forwarders.push(tokio::spawn(forward_closed_bars(
                stream,
                inst.symbol.clone(),
                tx,
                Arc::clone(&bar_sinks),
            )));
            candle_rxs.push(rx);
        }
        candle_rxs_by_group.push(candle_rxs);
        warmups_by_group.push(warmups);
    }

    // 6) 进程级 Store（全组共享；行按 strategy_id scope 隔离）。
    let state_root = output_dir.join("state");
    std::fs::create_dir_all(&state_root)?;
    let store = Arc::new(Store::new_with_policy(
        &state_root,
        Arc::new(EpochClock),
        state_prune_terminal,
    ));

    // 7) 每组 actor；共享 bind 通道（一个 OrderRouter 管多组）。
    let (bind_tx, mut bind_rx) = tokio::sync::mpsc::channel::<RouteBind>(256);
    let mut handles: Vec<RuntimeHandle> = Vec::with_capacity(specs.len());
    let mut actor_tasks: Vec<JoinHandle<()>> = Vec::with_capacity(specs.len());
    for (idx, spec) in specs.iter().enumerate() {
        let first = &spec.instruments[0];
        let spec_for_strategy = spec.clone();
        let strategy_factory = Box::new(
            move || -> Box<dyn strategy_engine::strategy::port::CrossSectionalStrategyPort> {
                crate::strategy_config::build_strategy(&spec_for_strategy)
                    .expect("横截面策略工厂在装配期已校验")
            },
        );
        // live → TradingGateway（venue 取该组首标的）；paper → None（本地撮合）。
        let gateway_factory: Option<Box<dyn Fn() -> Box<dyn AsyncOrderGateway> + Send>> =
            if run_mode == "live" {
                let venue = OrderVenueCfg {
                    exchange: wire_exchange(&first.exchange),
                    account: first.account.clone(),
                    leverage: first.leverage,
                    order_cash: first.cash,
                };
                let trading_c = trading.clone();
                let stype = spec.strategy_type.clone();
                Some(Box::new(move || -> Box<dyn AsyncOrderGateway> {
                    Box::new(TradingGateway::with_submit(
                        trading_c.clone(),
                        stype.clone(),
                        venue.clone(),
                        true,
                    ))
                }))
            } else {
                None
            };
        let live_spec = crate::strategy_live_actor::StrategyLiveSpec {
            strategy_id: spec.strategy_id.clone(),
            strategy_type: spec.strategy_type.clone(),
            config: spec.config.clone(),
            instruments: spec.instruments.clone(),
            mode: run_mode.clone(),
            state_dir: state_root.clone(),
            state_flush_interval_secs: state_flush_interval.as_secs().max(1),
            position_check_interval_secs: crate::runtime_types::POSITION_CHECK_INTERVAL_SECS,
            initial_cash_total: spec
                .instruments
                .iter()
                .map(|s| s.initial_cash.unwrap_or(5000.0))
                .sum(),
            fee_rate: None,
            channel_cap: 256,
        };
        let (strategy_handle, actor_task) = crate::strategy_live_actor::spawn_strategy_live(
            crate::strategy_live_actor::StrategyLiveAssembly {
                spec: live_spec,
                candle_rxs: std::mem::take(&mut candle_rxs_by_group[idx]),
                warmup: std::mem::take(&mut warmups_by_group[idx]),
                store: Arc::clone(&store),
                gateway_factory,
                strategy_factory,
                route_tx: Some(bind_tx.clone()),
            },
        );
        info!(
            strategy_id = %spec.strategy_id,
            group_index = idx,
            mode = %run_mode,
            instruments = %spec.instruments.iter().map(|s| s.symbol.clone()).collect::<Vec<_>>().join(","),
            "横截面 Actor 已启动"
        );
        handles.push(strategy_handle);
        actor_tasks.push(actor_task);
    }
    drop(bind_tx);

    // 8) T7 转发器：order_id 精确路由 + 同类型兜底广播（跨组隔离靠
    //    owns_signal；未绑定回报只投同类型 actor）。
    let typed: Vec<(String, bool, RuntimeHandle)> = specs
        .iter()
        .zip(handles.iter())
        .map(|(spec, handle)| {
            (
                spec.strategy_type.clone(),
                run_mode == "live",
                handle.clone(),
            )
        })
        .collect();
    let order_forwarder = tokio::spawn(async move {
        let mut router = OrderRouter::new(typed);
        loop {
            tokio::select! {
                maybe = trading_events.recv() => {
                    let Some(ev) = maybe else { break };
                    match ev {
                        ClientEvent::OrderUpdate(u) => router.dispatch_update(u).await,
                        ClientEvent::Reconnected { strategy_name } => {
                            router.dispatch_reconnect(&strategy_name).await
                        }
                    }
                }
                maybe = bind_rx.recv() => {
                    let Some(bind) = maybe else { continue };
                    router.bind(bind.order_id, bind.actor);
                }
            }
        }
    });

    Ok(StrategyLiveSet {
        handles,
        actor_tasks,
        forwarders,
        order_forwarder,
    })
}

/// 启动多组运行时并等待 Ctrl-C（CLI `run` 入口）。
#[allow(clippy::too_many_arguments)]
pub async fn run_live_strategy_set(
    specs: Vec<crate::strategy_config::StrategySpec>,
    services: ServicesConfig,
    output_dir: PathBuf,
    kline_data_dir: PathBuf,
    run_mode: String,
    state_flush_interval: Duration,
    state_prune_terminal: bool,
) -> anyhow::Result<()> {
    let set = start_live_strategy_set(
        specs,
        services,
        output_dir,
        kline_data_dir,
        run_mode,
        state_flush_interval,
        state_prune_terminal,
    )
    .await?;
    tokio::signal::ctrl_c().await.ok();
    info!("收到退出信号，优雅关闭横截面运行时");
    set.shutdown().await;
    info!("横截面运行时已关闭");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use strategy_broker::{BacktestBroker, BacktestConfig, BacktestOrderClient, TradingConfig};
    use strategy_core::signal::SignalIntent;
    use strategy_core::SignalStatus;

    /// K线协议用交易所精确名：引擎行键归一出的 `SHFE` 必须发成 `ctp`，
    /// 否则服务端回 error 帧「K线暂不支持交易所」。
    #[test]
    fn wire_exchange_maps_shfe_to_ctp() {
        assert_eq!(wire_exchange("SHFE"), "ctp");
        assert_eq!(wire_exchange("ctp"), "ctp");
        assert_eq!(wire_exchange("binance"), "binance");
    }

    fn ct_contract(opens: &[&str], closes: &[&str]) -> strategy_core::contract::ContractSpec {
        strategy_core::contract::ContractSpec {
            name: None,
            multiplier: 1.0,
            tick_size: 1.0,
            margin_rate: 0.1,
            min_lots: 1,
            session_opens: opens.iter().map(|s| s.to_string()).collect(),
            no_trade_after_open_minutes: 5,
            session_closes: closes.iter().map(|s| s.to_string()).collect(),
            no_trade_before_close_minutes: 5,
            commission: strategy_core::contract::CommissionSpec {
                mode: strategy_core::contract::FeeMode::Rate,
                open: 0.0,
                close: 0.0,
                close_today: None,
            },
        }
    }

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    /// 周末/节假日：最后交易根是周五夜盘收盘，距 now 数十小时属正常。
    #[test]
    fn warmup_tail_weekend_is_not_stale() {
        let c = ct_contract(&["09:00", "21:00"], &["15:00", "01:00"]);
        // now = 周日 12:00 CST（2026-11-15 是周日）；最后一根 = 周六 01:00 CST（周五夜盘收盘）。
        let now = utc(2026, 11, 15, 4, 0);
        let last = utc(2026, 11, 13, 17, 0);
        assert!(warmup_tail_fresh(Some(&c), last, now), "周末不得误报陈旧");
    }

    /// 午休：最后一根 11:30，now 12:00，落后 30 分钟属正常。
    #[test]
    fn warmup_tail_lunch_break_is_not_stale() {
        let c = ct_contract(&["09:00", "21:00"], &["15:00", "01:00"]);
        let now = utc(2026, 11, 13, 4, 0); // 周五 12:00 CST
        let last = utc(2026, 11, 13, 3, 30); // 11:30 CST
        assert!(warmup_tail_fresh(Some(&c), last, now), "午休不得误报陈旧");
    }

    /// 不同品种夜盘收盘不同（AU/AG 02:30）：收盘后到次日开盘同样不算陈旧。
    #[test]
    fn warmup_tail_night_close_0230_is_not_stale() {
        let c = ct_contract(&["09:00", "21:00"], &["15:00", "02:30"]);
        let now = utc(2026, 11, 13, 18, 40); // 周五 02:40 CST
        let last = utc(2026, 11, 13, 18, 29); // 02:29 CST
        assert!(warmup_tail_fresh(Some(&c), last, now), "夜盘收盘后不得误报");
    }

    /// 真陈旧：周一开盘后最后一根还停在周五日盘收盘（夜盘/周末数据全缺）。
    #[test]
    fn warmup_tail_missing_last_session_is_stale() {
        let c = ct_contract(&["09:00", "21:00"], &["15:00", "01:00"]);
        let now = utc(2026, 11, 16, 2, 0); // 周一 10:00 CST
        let last = utc(2026, 11, 13, 7, 0); // 周五 15:00 CST（缺周五夜盘）
        assert!(
            !warmup_tail_fresh(Some(&c), last, now),
            "缺整个夜盘必须判陈旧"
        );
    }

    /// 无合约（crypto 连续交易）：仍按固定 300s 容忍。
    #[test]
    fn warmup_tail_crypto_uses_fixed_tolerance() {
        let now = utc(2026, 11, 13, 0, 0);
        assert!(warmup_tail_fresh(
            None,
            now - chrono::Duration::seconds(60),
            now
        ));
        assert!(!warmup_tail_fresh(
            None,
            now - chrono::Duration::seconds(600),
            now
        ));
    }

    /// 横截面实盘扇出：每根闭合 1m 必须追加进本地 CSV（重复帧水位去重），
    /// 策略数据面照常收到全部帧（落盘不改变投递语义）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn xsec_forwarder_persists_closed_bars_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = strategy_data::csv_store::resolve_csv_path(
            dir.path(),
            "binance/crypto_perpetual/BTCUSDT",
            strategy_core::candle::Timeframe::M1,
        );
        let sinks: Arc<BarSinks> =
            Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
        sinks.lock().unwrap().insert(
            "BTCUSDT".to_string(),
            strategy_data::csv_store::LiveCsvSink::open(&path).unwrap(),
        );
        let (btx, _) = tokio::sync::broadcast::channel(8);
        let stream = strategy_data::client::KlineStream {
            rx: btx.subscribe(),
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let task = tokio::spawn(forward_closed_bars(
            stream,
            "BTCUSDT".to_string(),
            tx,
            Arc::clone(&sinks),
        ));
        let bar = |m: i64, px: f64| strategy_data::client::KlineBar {
            symbol: "BTCUSDT".to_string(),
            candle: strategy_core::candle::Candle {
                open_time: utc(2026, 1, 5, 0, 0) + ChronoDuration::minutes(m),
                open: px,
                high: px,
                low: px,
                close: px,
                volume: 1.0,
                quote_volume: 1.0,
                trade_num: 1,
                active_buy_volume: 1.0,
                active_buy_quote_volume: 1.0,
                is_closed: true,
            },
        };
        use strategy_data::client::KlineEvent;
        btx.send(KlineEvent::Closed(bar(0, 100.0))).unwrap();
        btx.send(KlineEvent::Closed(bar(0, 100.0))).unwrap(); // 重复帧
        btx.send(KlineEvent::Closed(bar(1, 101.0))).unwrap();

        for m in [0, 0, 1] {
            assert_eq!(
                rx.recv_timeout(Duration::from_secs(2))
                    .expect("闭合帧必须原样投递策略数据面")
                    .open_time,
                utc(2026, 1, 5, 0, 0) + ChronoDuration::minutes(m)
            );
        }
        drop(btx);
        task.await.unwrap();
        let rows = strategy_data::csv_store::load_csv(&path).unwrap();
        assert_eq!(rows.len(), 2, "每根恰好落盘一次（重复帧被水位去重）");
        assert_eq!(rows.last().unwrap().close, 101.0);
    }

    /// 服务端 CTP 品种前缀按小写匹配；大写订阅会被静默忽略（无帧无 error 帧）。
    #[test]
    fn wire_symbols_lowercases_ctp_products_only() {
        let syms = vec!["FU".to_string(), "BU".to_string()];
        assert_eq!(
            wire_symbols("ctp", &syms),
            vec!["fu".to_string(), "bu".to_string()]
        );
        assert_eq!(wire_symbols("binance", &syms), syms);
    }

    fn intent(id: &str) -> SignalIntent {
        SignalIntent {
            id: id.into(),
            strategy_id: "sar_supertrend".into(),
            symbol: "BTCUSDT".into(),
            kind: strategy_core::IntentKind::Open,
            direction: strategy_core::Side::Long,
            price: 100.0,
            size: 0.0,
            size_raw: None,
            stop_price: Some(95.0),
            position_id: None,
            stop_order_id: None,
            created_event_time: chrono::Utc::now(),
            status: SignalStatus::Created,
            offset: None,
        }
    }

    fn venue() -> OrderVenueCfg {
        OrderVenueCfg {
            exchange: "binance".into(),
            account: "test_binance".into(),
            leverage: 1,
            order_cash: 200.0,
        }
    }

    fn paper_client(dir: &std::path::Path) -> BacktestOrderClient {
        BacktestOrderClient::new(BacktestBroker::new(
            BacktestConfig {
                cash: 5000.0,
                leverage: 1,
                notional_per_order: 200.0,
                exchange: "binance".into(),
                ..Default::default()
            },
            "test_paper",
            dir,
        ))
    }

    fn unreachable_trading() -> Arc<TradingServiceClient> {
        let (client, _events) = TradingServiceClient::start_arc(
            TradingConfig {
                ws_url: "ws://127.0.0.1:1".into(),
                submit_enabled: true,
                ..Default::default()
            },
            tokio::runtime::Handle::current(),
        );
        client
    }

    /// U10：runtime_name 后缀按 per-symbol mode 分化；5c run 合并步：
    /// symbol 段为命名空间键下划线形（§6.1，与 runner id 前缀同形）
    #[test]
    fn runtime_name_reflects_per_symbol_mode() {
        assert_eq!(
            runtime_name(
                "sar_supertrend",
                "4h",
                "binance/crypto_perpetual/BTCUSDT",
                "live"
            ),
            "sar_supertrend_4H_3_binance_crypto_perpetual_BTCUSDT_LIVE"
        );
        assert_eq!(
            runtime_name(
                "sar_supertrend",
                "4h",
                "binance/crypto_perpetual/WLDUSDT",
                "paper_trading"
            ),
            "sar_supertrend_4H_3_binance_crypto_perpetual_WLDUSDT_PAPER_TRADING"
        );
        // Cooldown 行键=实例名：同名裸 symbol 跨交易所互斥（§14-3 run 面）
        assert_ne!(
            runtime_name("s", "8h", "binance/crypto_perpetual/BTCUSDT", "live"),
            runtime_name("s", "8h", "bybit/crypto_perpetual/BTCUSDT", "live")
        );
    }

    /// U15：启动 mode 表格——每 symbol 一行，可肉眼核对 live/paper 分流
    #[test]
    fn format_mode_table_lists_each_symbol() {
        let mk = |symbol: &str, mode: &str| SymbolSpec {
            strategy: "sar_supertrend".into(),
            symbol: symbol.into(),
            timeframe: "4h".into(),
            cash: 200.0,
            initial_cash: None,
            params: None,
            warmup_days: 30,
            exchange: "binance".into(),
            venue_key: format!("binance/crypto_perpetual/{symbol}"),
            account: "test".into(),
            leverage: 1,
            mode: mode.into(),
            contract: None,
            risk: Default::default(),
            venue_qty_step: None,
            venue_min_qty: None,
            venue_qty_precision: None,
        };
        let specs = vec![mk("BTCUSDT", "live"), mk("WLDUSDT", "paper_trading")];
        let table = format_mode_table(&specs);
        assert!(table.contains("BTCUSDT"), "表格应含 symbol: {table}");
        assert!(table.contains("live"), "表格应含 mode: {table}");
        assert!(table.contains("WLDUSDT") && table.contains("paper_trading"));
    }

    /// U7：paper_trading 网关本地撮合，提交后就绪队列有成交
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gateway_for_paper_mode_fills_locally() {
        let tmp = tempfile::tempdir().unwrap();
        let mut gw = make_gateway(
            "paper_trading",
            unreachable_trading(),
            "sar_supertrend".into(),
            venue(),
            paper_client(tmp.path()),
        );
        let order_id = gw
            .submit_fut(&intent("sig_p1"))
            .await
            .expect("本地撮合应成交");
        assert!(order_id.starts_with("bt_"), "本地撮合订单号: {order_id}");
        let fills = gw.drain_ready_fills();
        assert_eq!(fills.len(), 1, "应产出 1 笔就绪成交");
    }

    /// U6：live 网关不本地撮合——就绪队列为空（成交来自 WS 回报）
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gateway_for_live_mode_does_not_fill_locally() {
        let tmp = tempfile::tempdir().unwrap();
        let mut gw = make_gateway(
            "live",
            unreachable_trading(),
            "sar_supertrend".into(),
            venue(),
            paper_client(tmp.path()),
        );
        // WS 不可达：提交失败或重试，但绝无本地成交
        let _ = gw.submit_fut(&intent("sig_l1")).await;
        assert!(
            gw.drain_ready_fills().is_empty(),
            "live 网关不得本地撮合出成交"
        );
    }
}
