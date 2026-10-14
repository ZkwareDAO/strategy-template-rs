//! live Bootstrap（设计文档 §14）
//!
//! 装配顺序：配置 → 数据/交易客户端 →（每策略实例）RuntimeActor 监督启动
//! （内部完成回填订阅、策略决策、信号/仓位、对账）→ T7 订单转发 → 等 Ctrl-C
//! 优雅关闭。
//!
//! 外部服务不可达时连接 task 内部退避重连，本函数不返回错误（fail-fast 只
//! 针对"未配置 services"）。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use strategy_broker::{
    BacktestBroker, BacktestConfig, BacktestOrderClient, ClientEvent, OrderVenueCfg, TradingConfig,
    TradingGateway, TradingServiceClient,
};
use strategy_data::{KlineClient, KlineServiceConfig, RemoteKlineServiceClient};
use strategy_signals::gateway::AsyncOrderGateway;
use strategy_store::{EpochClock, Store};
use strategy_strategies::registry::{self, FactoryArgs};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::{ServicesConfig, SymbolSpec};
use crate::order_router::OrderRouter;
use crate::runtime_actor::{RouteBind, RuntimeHandle, RuntimeSpec};

/// 运行时策略名（进入 CSV 路径与 position_id）：
/// `{strategy}_{TF}_3_{SYMBOL}_{MODE}`
fn runtime_name(strategy: &str, timeframe: &str, symbol: &str, mode: &str) -> String {
    format!(
        "{}_{}_3_{}_{}",
        strategy,
        timeframe.to_uppercase(),
        symbol.to_uppercase(),
        mode.to_uppercase()
    )
}

/// 启动时的 per-symbol mode 表格（U15：肉眼核对 live/paper 分流）
pub(crate) fn format_mode_table(specs: &[SymbolSpec]) -> String {
    let mut out = String::from("运行实例 mode 一览：\n");
    for s in specs {
        out.push_str(&format!("  {} {:>14} → {}\n", s.symbol, s.strategy, s.mode));
    }
    out
}

/// 按 per-symbol mode 装配下单网关：
/// - `live` → [`TradingGateway`]（真单通道，实例闸门放行）
/// - `paper_trading` → [`BacktestOrderClient`]（本地撮合，不经交易 WS）
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

/// 启动 live/paper 运行时，直到收到 Ctrl-C。mode 按 per-symbol `spec.mode` 分流。
pub async fn run_live(
    specs: Vec<SymbolSpec>,
    services: ServicesConfig,
    output_dir: PathBuf,
    kline_data_dir: PathBuf,
    state_flush_interval: Duration,
    state_prune_terminal: bool,
) -> anyhow::Result<()> {
    let handle = tokio::runtime::Handle::current();

    // 数据客户端（共享分发器：一条 WS 订阅全部 symbols，按 symbol 扇出）
    let symbols: Vec<String> = specs.iter().map(|s| s.symbol.clone()).collect();
    let kline: Arc<dyn KlineClient> = RemoteKlineServiceClient::start(
        &symbols,
        KlineServiceConfig {
            backfill_url: services.kline.backfill_url,
            ws_url: services.kline.ws_url,
            reconnect_backoff_max_secs: services.kline.reconnect_backoff_max_secs,
            channel_cap: 100,
            // Ping/空闲看门狗沿用生产默认（15s/30s），暂不下放到 yaml
            ..Default::default()
        },
        handle.clone(),
    );

    // 交易客户端（共享 WS）。submit_enabled 只看配置总开关——mode 分流在
    // per-symbol 网关层（live→TradingGateway，paper→BacktestOrderClient）。
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
    info!("订单网关已启动（连接由 task 内部维护；下单按 per-symbol mode 分流）");
    info!("\n{}", format_mode_table(&specs));

    let mut runtime_handles: Vec<RuntimeHandle> = Vec::new();
    let mut runtime_handles_typed: Vec<(String, bool, RuntimeHandle)> = Vec::new();
    let mut actor_tasks: Vec<JoinHandle<()>> = Vec::new();
    // B8：ack 后 order_id → Actor 的精确路由注册通道（喂给 T7）
    let (bind_tx, mut bind_rx) = tokio::sync::mpsc::channel::<RouteBind>(256);

    // 进程级统一 Store：所有策略实例共享同一张 model 表（signal_id 全局唯一），
    // base 沿用状态根目录；live 元数据时间戳用 EpochClock（不进事件/ID/报表）。
    // flush 节奏（S6）：①内核每 1000 事件置标志 → Actor spawn_blocking 全 Store
    // flush；②每 state_flush_interval 独立 interval 时间驱动 flush（与阈值去重）；
    // ③优雅关停前无条件 flush 一次。
    let state_root = output_dir.join("state");
    std::fs::create_dir_all(&state_root)?;
    let store = Arc::new(Store::new_with_policy(
        &state_root,
        Arc::new(EpochClock),
        state_prune_terminal,
    ));
    debug!(state_root = %state_root.display(), "装配：进程级统一 Store 已建立（所有策略实例共享 model 表）");

    // 启动前自动确保近一年 1m 历史（Binance 公共归档，幂等：日常重启
    // 已最新时零请求）。失败不阻断启动——warmup 仍有本地 CSV +
    // kline-service 30 天回填兜底。在监督代之外执行，策略 panic 重建不会重复下载。
    // 各 symbol CSV 相互独立，并发下载互不影响（仅首次/长停机才真下载）。
    let ensure_futs = specs.iter().map(|spec| {
        let symbol = spec.symbol.clone();
        let data_dir = kline_data_dir.clone();
        async move {
            let result = strategy_data::archive_download::Downloader::new()
                .ensure_recent_history(
                    &symbol,
                    strategy_core::candle::Timeframe::M1,
                    strategy_data::archive_download::ENSURE_HISTORY_DAYS,
                    &data_dir,
                )
                .await;
            (symbol, result)
        }
    });
    for (symbol, result) in futures::future::join_all(ensure_futs).await {
        if let Err(e) = result {
            warn!(
                symbol = %symbol,
                error = %e,
                "近一年历史自动下载失败，按现有本地数据继续启动"
            );
        }
    }

    for spec in &specs {
        let name = runtime_name(&spec.strategy, &spec.timeframe, &spec.symbol, &spec.mode);

        // RuntimeActor（监督启动，B11）：panic 整代重建——重新回填订阅、
        // restore + 启动对账恢复
        let rt_spec = RuntimeSpec {
            name: name.clone(),
            strategy_type: spec.strategy.clone(),
            symbol: spec.symbol.clone(),
            mode: spec.mode.clone(),
            state_dir: output_dir.join("state"),
            kline_data_dir: kline_data_dir.clone(),
            warmup_bars: (spec.warmup_days.max(1) as usize) * 24 * 60,
            state_flush_interval,
            poll_grace_secs: crate::runtime_actor::POLL_GRACE_SECS,
            poll_interval_secs: crate::runtime_actor::POLL_INTERVAL_SECS,
            position_check_interval_secs: crate::runtime_actor::POSITION_CHECK_INTERVAL_SECS,
        };
        let factory = {
            let trading = trading.clone();
            let strategy_name = spec.strategy.clone();
            let symbol = spec.symbol.clone();
            let timeframe = spec.timeframe.clone();
            let cash = spec.cash;
            let params = spec.params.clone();
            let mode = spec.mode.clone();
            let name_for_bt = name.clone();
            let out_base = output_dir.clone();
            // 下单场所/资金配置：全部来自本 symbol 的策略配置（yaml）
            let venue = OrderVenueCfg {
                exchange: spec.exchange.clone(),
                account: spec.account.clone(),
                leverage: spec.leverage,
                order_cash: spec.cash,
            };
            Box::new(move || {
                // paper 本地撮合内核：跨监督代共享（BacktestOrderClient 是共享句柄）
                let paper = BacktestOrderClient::new(BacktestBroker::new(
                    BacktestConfig {
                        cash: 5000.0,
                        leverage: venue.leverage,
                        notional_per_order: cash,
                        exchange: venue.exchange.clone(),
                        ..Default::default()
                    },
                    &name_for_bt,
                    &out_base,
                ));
                let gateway = make_gateway(
                    &mode,
                    trading.clone(),
                    strategy_name.clone(),
                    venue.clone(),
                    paper,
                );
                let args = FactoryArgs {
                    symbol: &symbol,
                    timeframe: &timeframe,
                    cash,
                    params: params.as_ref(),
                };
                let strategy =
                    registry::create(&strategy_name, &args).expect("策略工厂在装配期已校验");
                (gateway, strategy)
            })
        };
        let (runtime_handle, actor_task) = crate::runtime_actor::spawn_supervised(
            rt_spec,
            kline.clone(),
            Arc::clone(&store),
            factory,
            256,
            Some(bind_tx.clone()),
        );
        actor_tasks.push(actor_task);
        runtime_handles.push(runtime_handle.clone());
        runtime_handles_typed.push((spec.strategy.clone(), spec.mode == "live", runtime_handle));

        let tf = strategy_core::Timeframe::parse(&spec.timeframe)
            .map_err(|e| anyhow::anyhow!("非法 timeframe {}: {e}", spec.timeframe))?;
        info!(strategy = %spec.strategy, timeframe = tf.0, "RuntimeActor 已启动");
    }

    let runtime_handles_for_shutdown = runtime_handles.clone();

    // T7：order_id 精确路由（B8），未知订单按策略类型广播兜底；交易 WS 重连 →
    // 同策略类型 RuntimeActor 对账。注册消息与交易事件在此 select 合并。
    let order_forwarder = tokio::spawn(async move {
        let mut router = OrderRouter::new(runtime_handles_typed);
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
    drop(bind_tx);

    // 监督 Actor 内部已隔离策略 panic（重启恢复），这里只等关闭
    tokio::signal::ctrl_c().await.ok();
    info!("收到退出信号，优雅关闭运行时");

    // 1) 通知每个 RuntimeActor：处理完箱内命令 → flush 快照 → 退出（带超时）
    for h in &runtime_handles_for_shutdown {
        let _ = h.shutdown().await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(8), async {
        for t in actor_tasks {
            let _ = t.await;
        }
    })
    .await;
    // 2) 转发器最后停（关闭期间仍可路由在途回报）
    order_forwarder.abort();
    info!("运行时已关闭");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use strategy_broker::{BacktestBroker, BacktestConfig, BacktestOrderClient, TradingConfig};
    use strategy_core::signal::SignalIntent;
    use strategy_core::SignalStatus;

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

    /// U10：runtime_name 后缀按 per-symbol mode 分化
    #[test]
    fn runtime_name_reflects_per_symbol_mode() {
        assert_eq!(
            runtime_name("sar_supertrend", "4h", "BTCUSDT", "live"),
            "sar_supertrend_4H_3_BTCUSDT_LIVE"
        );
        assert_eq!(
            runtime_name("sar_supertrend", "4h", "WLDUSDT", "paper_trading"),
            "sar_supertrend_4H_3_WLDUSDT_PAPER_TRADING"
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
            params: None,
            warmup_days: 30,
            exchange: "binance".into(),
            account: "test".into(),
            leverage: 1,
            mode: mode.into(),
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
