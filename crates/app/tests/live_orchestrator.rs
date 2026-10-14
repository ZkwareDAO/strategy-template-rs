//! Phase 3 Step2 集成测试 —— live 装配（策略并入 RuntimeActor）：
//! - K 线 → 同一 Actor 内：Feed→策略→信号→本地撮合→仓位直读入账
//! - 冷启动交叉对账（信号 Filled、仓位缺失 → query 补成交）

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{TimeZone, Utc};
use strategy_broker::backtest::{BacktestBroker, BacktestConfig};
use strategy_broker::BacktestOrderClient;
use strategy_core::candle::{Candle, Timeframe};
use strategy_core::order::OrderState;
use strategy_core::signal::Action;
use strategy_core::strategy::{Ctx, Strategy};
use strategy_core::{IntentKind, Side, SignalIntent, SignalStatus};
use strategy_data::client::{KlineBar, KlineClient, KlineEvent, KlineStream};
use strategy_positions::{Cooldown, ManagedPosition, ManagedStop, PositionManager};
use strategy_signals::gateway::{
    AsyncOrderGateway, OrderSnapshot, PositionsFuture, QueryFuture, RemotePosition, SubmitError,
    SubmitFuture,
};
use strategy_signals::ManagedSignal;
use strategy_store::{Clock, Store};

/// 可控时钟（测试不引墙钟）：固定在 2026-06-10 00:00 UTC，当日目录 20260610。
struct FixedClock;
impl Clock for FixedClock {
    fn now_ms(&self) -> i64 {
        1_781_049_600_000
    }
}

fn test_store(dir: &std::path::Path) -> Arc<Store> {
    std::fs::create_dir_all(dir).ok();
    Arc::new(Store::new(dir, Arc::new(FixedClock)))
}

/// 以新进程视角从当日快照恢复 signal 行（走 Store 公共往返契约，不耦合
/// CSV 字段转义）。测试内固定时钟 → 日目录 20260610。
/// 兼容模式 recover（多日合并）：filled 信号在裁剪模式下会被 is_terminal
/// 过滤——本文件断言的是「落盘可见性」而非裁剪语义（裁剪由 store 集成测试覆盖）。
fn recovered_signals(dir: &std::path::Path) -> Vec<ManagedSignal> {
    assert!(
        dir.join("20260610").join("signal.csv").exists(),
        "当日信号快照应落盘"
    );
    let store = Store::new_with_policy(dir, Arc::new(FixedClock), false);
    store.recover::<ManagedSignal>().unwrap();
    store.table::<ManagedSignal>().list()
}

/// 以新进程视角从当日快照恢复 position 行（走 Store 公共往返契约）。
fn recovered_positions(dir: &std::path::Path) -> Vec<ManagedPosition> {
    assert!(
        dir.join("20260610").join("position.csv").exists(),
        "当日持仓快照应落盘"
    );
    let store = Store::new_with_policy(dir, Arc::new(FixedClock), false);
    store.recover::<ManagedPosition>().unwrap();
    store.table::<ManagedPosition>().list()
}

/// 存活期轮询共享 Store（同进程状态即时可见，S4 后无逐条事件文件）。
async fn wait_position(store: &Arc<Store>, name: &str) -> ManagedPosition {
    for _ in 0..100 {
        if let Some(p) = store
            .table::<ManagedPosition>()
            .list()
            .into_iter()
            .find(|p| p.strategy_name == name)
        {
            return p;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    panic!("未在共享 Store 观察到 {name} 的持仓行");
}

/// 轮询共享 Store：先等本实例持仓行出现（actor 恢复完成），
/// 再等其消失（external_close 软删），避免启动竞态导致假通过。
async fn wait_position_closed(store: &Arc<Store>, name: &str) {
    wait_position(store, name).await;
    for _ in 0..100 {
        let still_open = store
            .table::<ManagedPosition>()
            .list()
            .into_iter()
            .any(|p| p.strategy_name == name);
        if !still_open {
            return;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    panic!("未观察到 {name} 的持仓被关闭");
}

/// 播种本地持仓（+止损单）：经 PositionManager 公共路径写共享 Store 并 flush。
fn seed_position(dir: &std::path::Path, name: &str, size: f64) {
    let store = test_store(dir);
    let mut pm = PositionManager::new(name, "BTCUSDT", "live", Arc::clone(&store), dir);
    let fill = strategy_core::FillEvent::new(
        String::from("sig_pos"),
        String::from("ord_pos"),
        String::from("BTCUSDT"),
        Side::Long,
        size,
        100.0,
        0.0,
        Utc::now(),
    );
    pm.apply_open_fill(&fill, Utc::now(), 98.0, 0.0).unwrap();
    store.flush().unwrap();
}

fn candle(i: usize) -> Candle {
    Candle {
        open_time: Utc.timestamp_opt(1_781_049_600 + i as i64 * 60, 0).unwrap(),
        open: 100.0 + i as f64,
        high: 101.0 + i as f64,
        low: 99.0 + i as f64,
        close: 100.0 + i as f64,
        volume: 1.0,
        quote_volume: 0.0,
        trade_num: 0,
        active_buy_volume: 0.0,
        active_buy_quote_volume: 0.0,
        is_closed: true,
    }
}

/// 脚本化 KlineClient：attach 建独立 broadcast 通道并推 Reconnected + 闭合 1m；
/// backfills 计数（Lagged 自愈断言用）；cap 为通道容量（测试调小制造落后）
struct MockKline {
    bars: Vec<Candle>,
    backfills: Arc<AtomicUsize>,
    cap: usize,
}
impl KlineClient for MockKline {
    fn backfill<'a>(
        &'a self,
        _symbol: &'a str,
        _tf: Timeframe,
        _bars: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<Candle>>> + Send + 'a>>
    {
        self.backfills.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(Vec::new()) })
    }
    fn attach(&self, symbol: &str, _tf: Timeframe) -> KlineStream {
        let (tx, rx) = tokio::sync::broadcast::channel(self.cap);
        let bars = self.bars.clone();
        let symbol = symbol.to_string();
        tokio::spawn(async move {
            tx.send(KlineEvent::Reconnected).ok();
            for c in bars {
                // 与生产一致：无 receiver 时静默丢弃，任务不退（跨代可用）；
                // 帧标签跟随 attach symbol（生产按 symbol 扇出，帧不带异 symbol）
                let _ = tx.send(KlineEvent::Closed(KlineBar {
                    symbol: symbol.clone(),
                    candle: c,
                }));
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            // 模拟服务端持续在线（生产 T1 内部重连保证接收端不关闭）
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        });
        KlineStream { rx }
    }
}

/// 首根闭合 K 线开多（带 98 止损）
struct OpenOnce {
    done: bool,
}
impl Strategy for OpenOnce {
    fn strategy_type(&self) -> &str {
        "openonce"
    }
    fn timeframes(&self) -> Vec<Timeframe> {
        vec![Timeframe::M1]
    }
    fn on_candle(&mut self, _ctx: &Ctx<'_>) -> Vec<Action> {
        if self.done {
            return vec![];
        }
        self.done = true;
        vec![Action::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 98.0,
            atr: 1.0,
            strength: 1.0,
            reason: "test".into(),
        }]
    }
}

/// 永不动作（交叉对账：bar 不触发开仓）
struct NoopStrategy;
impl Strategy for NoopStrategy {
    fn strategy_type(&self) -> &str {
        "noop"
    }
    fn timeframes(&self) -> Vec<Timeframe> {
        vec![Timeframe::M1]
    }
    fn on_candle(&mut self, _ctx: &Ctx<'_>) -> Vec<Action> {
        vec![]
    }
}

/// 查询应答型网关（交叉对账用）
struct QueryGateway {
    queries: std::sync::Mutex<VecDeque<OrderSnapshot>>,
}
impl AsyncOrderGateway for QueryGateway {
    fn submit_fut<'a>(&'a mut self, _intent: &'a SignalIntent) -> SubmitFuture<'a> {
        Box::pin(async { Err(SubmitError::rejected("测试网关不接单")) })
    }
    fn query_fut<'a>(&'a mut self, order_id: &'a str) -> QueryFuture<'a> {
        let snap = self.queries.lock().unwrap().pop_front();
        let id = order_id.to_string();
        Box::pin(async move {
            snap.ok_or_else(|| SubmitError::retryable(format!("无脚本应答: {id}")))
        })
    }
    fn positions_fut<'a>(
        &'a mut self,
        _instrument: &'a str,
        _side: &'a str,
    ) -> PositionsFuture<'a> {
        Box::pin(async { Err(SubmitError::retryable("测试未编排仓位应答")) })
    }
}

fn spec(
    name: &str,
    strategy_type: &str,
    dir: &std::path::Path,
) -> strategy_app::runtime_actor::RuntimeSpec {
    strategy_app::runtime_actor::RuntimeSpec {
        name: name.into(),
        strategy_type: strategy_type.into(),
        symbol: "BTCUSDT".into(),
        mode: "live".into(),
        state_dir: dir.to_path_buf(),
        kline_data_dir: dir.join("klines"),
        warmup_bars: 10,
        state_flush_interval: Duration::from_secs(60),
        poll_grace_secs: 60,
        poll_interval_secs: 60,
        position_check_interval_secs: 3600,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bar_drives_open_fill_and_position_within_one_actor() {
    let dir = tempfile::tempdir().unwrap();
    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![candle(0), candle(1), candle(2)],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let state_dir = dir.path().to_path_buf();
    let factory = Box::new(move || {
        let broker = BacktestBroker::new(
            BacktestConfig {
                cash: 1000.0,
                leverage: 1,
                notional_per_order: 0.0,
                ..Default::default()
            },
            "ok_live",
            &state_dir,
        );
        let gw: Box<dyn AsyncOrderGateway> = Box::new(BacktestOrderClient::new(broker));
        (
            gw,
            Box::new(OpenOnce { done: false }) as Box<dyn Strategy + Send>,
        )
    });
    let store = test_store(dir.path());
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        spec("ok_live", "openonce", dir.path()),
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    // Step2：成交与仓位同 task 直接入账。S4 后无逐条事件文件，存活期断言
    // 改为读共享 Store（同进程内存即时可见）：position/stop 当前态行已建。
    let pos = wait_position(&store, "ok_live").await;
    assert_eq!(pos.state.side, strategy_positions::Side::Long);
    assert!((pos.state.entry_price - 100.0).abs() < 1e-9);
    assert!((pos.state.stop_price - 98.0).abs() < 1e-9);
    let stop = store
        .table::<ManagedStop>()
        .list()
        .into_iter()
        .find(|s| s.strategy_name == "ok_live")
        .expect("止损单当前态行应已建");
    assert_eq!(stop.lifecycle, "created");
    assert!((stop.trigger_price - 98.0).abs() < 1e-9);

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();

    // 关停前无条件 flush：信号当前态以统一 Store 当日快照落盘。
    // version=5 + status=filled 证明生命周期 Created→Validated→Submitted
    // →Accepted→Filled 经 4 次合法转移走完全程（与旧事件日志同等可见）。
    let rows = recovered_signals(dir.path());
    let row = rows
        .iter()
        .find(|r| r.strategy_name == "ok_live")
        .expect("当日快照须含 ok_live 信号行");
    assert_eq!(row.status, SignalStatus::Filled);
    assert_eq!(row.version, 5, "Created 起 4 次转移");
    assert!(row.order_id.is_some());

    // 持仓未平：position 行随当日快照落盘，新进程可恢复
    let positions = recovered_positions(dir.path());
    let pos = positions
        .iter()
        .find(|p| p.strategy_name == "ok_live")
        .expect("当日快照须含 ok_live 持仓行");
    assert!((pos.state.entry_price - 100.0).abs() < 1e-9);
    assert_eq!(pos.state.side, strategy_positions::Side::Long);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_cross_reconcile_fills_missing_position() {
    let dir = tempfile::tempdir().unwrap();
    let now = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();

    // 播种信号：Created→...→Filled(ord_cross)
    let intent = SignalIntent {
        id: "sig_cross".into(),
        strategy_id: "noop".into(),
        symbol: "BTCUSDT".into(),
        kind: IntentKind::Open,
        direction: Side::Long,
        price: 100.0,
        size: 1.0,
        size_raw: None,
        stop_price: Some(98.0),
        position_id: None,
        stop_order_id: None,
        created_event_time: now,
        status: SignalStatus::Created,
    };
    // 播种信号当前态：Created→…→Filled(ord_cross)。旧事件日志逐条 append，
    // 新模型只持久化当前态——直接写 v5/Filled 的 signal 行并 flush 当日快照。
    {
        let seed = test_store(dir.path());
        seed.table::<ManagedSignal>()
            .insert(ManagedSignal {
                strategy_name: "cross_live".into(),
                intent,
                version: 5,
                status: SignalStatus::Filled,
                order_id: Some("ord_cross".into()),
                attempts: 1,
                next_retry_at: None,
                last_error: None,
                last_failure_kind: None,
                exchange_order_id: None,
                filled_price: Some(100.0),
                filled_quantity: Some(1.0),
                filled_quantity_raw: Some("1".into()),
                order_type: None,
                order_price: None,
                order_quantity: None,
            })
            .unwrap();
        seed.flush().unwrap();
    }

    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let factory = Box::new(|| {
        let gw = QueryGateway {
            queries: std::sync::Mutex::new(VecDeque::from([OrderSnapshot {
                order_id: "ord_cross".into(),
                state: OrderState::Filled,
                filled_size: 1.0,
                avg_price: 100.0,
                exchange_order_id: None,
                filled_quantity_raw: None,
                place: None,
            }])),
        };
        (
            Box::new(gw) as Box<dyn AsyncOrderGateway>,
            Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
        )
    });
    let store = test_store(dir.path());
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        spec("cross_live", "noop", dir.path()),
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    // 交叉对账（信号 Filled、仓位缺失 → query 补成交）后持仓行进共享 Store
    let pos = wait_position(&store, "cross_live").await;
    assert_eq!(pos.state.position_id, "sig_cross");
    assert!((pos.state.entry_price - 100.0).abs() < 1e-9);
    assert!((pos.state.total_size - 1.0).abs() < 1e-9);

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();

    // 关停 flush：补回的持仓随当日快照落盘，新进程可恢复
    let positions = recovered_positions(dir.path());
    assert!(
        positions
            .iter()
            .any(|p| p.strategy_name == "cross_live" && p.state.position_id == "sig_cross"),
        "交叉对账补回的仓位须落当日快照：{positions:?}"
    );
}

#[test]
fn services_config_defaults_to_none() {
    let cfg: strategy_app::config::FileConfig = Default::default();
    assert!(cfg.services.is_none());
}

/// 首根 bar 即 panic 的策略（模拟 Actor 内部 panic）
struct PanicStrategy;
impl Strategy for PanicStrategy {
    fn strategy_type(&self) -> &str {
        "panic"
    }
    fn timeframes(&self) -> Vec<Timeframe> {
        vec![Timeframe::M1]
    }
    fn on_candle(&mut self, _ctx: &Ctx<'_>) -> Vec<Action> {
        panic!("模拟策略 panic")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_restarts_after_strategy_panic_and_recovers() {
    use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
    use std::sync::Mutex;
    use strategy_signals::gateway::{ReadyFill, SubmitFuture};

    struct OkGateway {
        ready: Mutex<Vec<ReadyFill>>,
    }
    impl AsyncOrderGateway for OkGateway {
        fn submit_fut<'a>(&'a mut self, intent: &'a SignalIntent) -> SubmitFuture<'a> {
            self.ready.lock().unwrap().push(ReadyFill {
                order_id: "ord_rec".into(),
                filled_size: 1.0,
                avg_price: intent.price,
                fee: 0.0,
            });
            Box::pin(async { Ok("ord_rec".to_string()) })
        }
        fn query_fut<'a>(
            &'a mut self,
            order_id: &'a str,
        ) -> strategy_signals::gateway::QueryFuture<'a> {
            let id = order_id.to_string();
            Box::pin(async move { Err(SubmitError::retryable(id)) })
        }
        fn positions_fut<'a>(
            &'a mut self,
            _instrument: &'a str,
            _side: &'a str,
        ) -> strategy_signals::gateway::PositionsFuture<'a> {
            Box::pin(async { Err(SubmitError::retryable("测试未编排仓位应答")) })
        }
        fn drain_ready_fills(&mut self) -> Vec<ReadyFill> {
            std::mem::take(&mut *self.ready.lock().unwrap())
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let generations = Arc::new(AtomicU32::new(0));
    let gens = generations.clone();
    let factory = Box::new(move || {
        let gen = gens.fetch_add(1, AtomicOrdering::SeqCst);
        let gw: Box<dyn AsyncOrderGateway> = Box::new(OkGateway {
            ready: std::sync::Mutex::new(Vec::new()),
        });
        let strategy: Box<dyn Strategy + Send> = if gen == 0 {
            Box::new(PanicStrategy)
        } else {
            Box::new(OpenOnce { done: false })
        };
        (gw, strategy)
    });
    // 两代都重放同一脚本：第一代在 candle(0) panic（其 sink 已把 candle(0)
    // 落盘）；新代 warmup 从 CSV 读到 candle(0)，订阅后重放的 candle(0)
    // 必须被 Feed 水位去重跳过，candle(1) 作为重启后的新分钟驱动开仓。
    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![candle(0), candle(1)],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let store = test_store(dir.path());
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        spec("rec_live", "openonce", dir.path()),
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    // 第一代 panic → 整代重建 → 新代 bar 开仓成交。S4 后无逐条事件文件，
    // 存活期直接轮询共享 Store 的 position 行（同进程内存即时可见）；
    // 关停后再断言当日信号/持仓快照。
    let pos = wait_position(&store, "rec_live").await;
    assert!((pos.state.entry_price - 100.0).abs() < 1e-9);
    assert!(
        generations.load(AtomicOrdering::SeqCst) >= 2,
        "工厂应至少被调用两代"
    );

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();

    let rows = recovered_signals(dir.path());
    assert!(
        rows.iter().any(|r| r.status == SignalStatus::Filled),
        "关停 flush 的当日快照应含 filled 行：{rows:?}"
    );
    let positions = recovered_positions(dir.path());
    assert!(
        positions.iter().any(|p| p.strategy_name == "rec_live"),
        "关停 flush 的当日快照应含 rec_live 持仓行：{positions:?}"
    );
}

/// 记录每根 bar 看到的 Ctx 止损冷却日（验证 cooldown 表 → Ctx 注入读路径）。
struct CooldownSpy {
    seen: Arc<Mutex<Vec<Option<strategy_core::strategy::Cooldown>>>>,
}
impl Strategy for CooldownSpy {
    fn strategy_type(&self) -> &str {
        "cooldownspy"
    }
    fn timeframes(&self) -> Vec<Timeframe> {
        vec![Timeframe::M1]
    }
    fn on_candle(&mut self, ctx: &Ctx<'_>) -> Vec<Action> {
        self.seen.lock().unwrap().push(ctx.cooldown.clone());
        vec![]
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cooldown_row_is_loaded_from_store_and_injected_into_ctx() {
    let dir = tempfile::tempdir().unwrap();

    // 播种 cooldown 行（模拟上一运行止损后 flush 的当日快照内存态）
    let store = test_store(dir.path());
    store
        .table::<Cooldown>()
        .insert(Cooldown {
            strategy_name: "cool_live".into(),
            date: "2026-06-10".into(),
        })
        .unwrap();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let factory = {
        let seen = seen.clone();
        Box::new(move || {
            let gw: Box<dyn AsyncOrderGateway> = Box::new(QueryGateway {
                queries: std::sync::Mutex::new(VecDeque::new()),
            });
            (
                gw,
                Box::new(CooldownSpy { seen: seen.clone() }) as Box<dyn Strategy + Send>,
            )
        })
    };
    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![candle(0)],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        spec("cool_live", "cooldownspy", dir.path()),
        kline,
        store,
        factory,
        8,
        None,
    );

    tokio::time::sleep(Duration::from_millis(400)).await;
    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();

    let seen = seen.lock().unwrap();
    assert!(
        seen.iter().any(|d| {
            d == &Some(strategy_core::strategy::Cooldown::UntilDate(
                "2026-06-10".into(),
            ))
        }),
        "cooldown 表的冷却应注入策略 Ctx：{seen:?}"
    );
}

/// S6 数据面闭环：服务端反查的"外部读者"只持进程共享 `Arc<Store>`
/// （不经 Actor 通道、不发 RuntimeCommand），经 positions crate 的只读访问器
/// 并发读 Actor 正在更新的 position 表——开仓后读到一致快照；平仓（软删）后
/// 读不到，而行以墓碑留在表内。
///
/// WS「服务端查仓位」消息属 sidecar 对签范畴（见 positions::list_positions
/// 的 TODO），本测试只验数据通道，不构造任何假协议消息。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_reader_concurrently_sees_open_and_hides_closed() {
    let dir = tempfile::tempdir().unwrap();
    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![candle(0)],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let state_dir = dir.path().to_path_buf();
    let factory = Box::new(move || {
        let broker = BacktestBroker::new(
            BacktestConfig {
                cash: 1000.0,
                leverage: 1,
                notional_per_order: 0.0,
                ..Default::default()
            },
            "ext_live",
            &state_dir,
        );
        let gw: Box<dyn AsyncOrderGateway> = Box::new(BacktestOrderClient::new(broker));
        (
            gw,
            Box::new(OpenOnce { done: false }) as Box<dyn Strategy + Send>,
        )
    });
    let store = test_store(dir.path());

    // 外部读者 task：Actor 存活期间持续反查（不持 RuntimeHandle、不发消息）
    let opened = Arc::new(tokio::sync::Notify::new());
    let reader = {
        let store = Arc::clone(&store);
        let opened = opened.clone();
        tokio::spawn(async move {
            let mut hits = 0usize;
            for _ in 0..400 {
                // 只读访问器：返回克隆快照，行 Mutex 不泄露
                for p in strategy_positions::list_positions(&store, "ext_live") {
                    assert_eq!(p.side, strategy_positions::Side::Long);
                    assert!((p.entry_price - 100.0).abs() < 1e-9);
                    assert!((p.stop_price - 98.0).abs() < 1e-9);
                    hits += 1;
                    if hits == 1 {
                        opened.notify_one();
                    }
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            hits
        })
    };

    let rt_spec = spec("ext_live", "openonce", dir.path());
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        rt_spec,
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    // 关键：读者必须在 Actor 仍运行时读到开仓（证明并发数据通道，而非关停后）
    tokio::time::timeout(Duration::from_secs(3), opened.notified())
        .await
        .expect("外部读者应在 Actor 存活期间读到开仓快照");

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();
    let hits = reader.await.unwrap();
    assert!(hits >= 1, "并发反查应至少读到一次开仓快照");

    // 平仓（软删）后：只读访问器读不到；position 行仍以墓碑留在表内
    let mut closer = PositionManager::new(
        "ext_live",
        "BTCUSDT",
        "live",
        Arc::clone(&store),
        dir.path(),
    );
    let restored = closer
        .restore()
        .unwrap()
        .expect("关停 flush 后应可恢复持仓");
    let position_id = restored.position_id.clone();
    closer
        .close(
            103.0,
            Utc.with_ymd_and_hms(2026, 6, 10, 1, 0, 0).unwrap(),
            "close",
            false,
            1.0,
        )
        .unwrap();

    assert!(
        strategy_positions::list_positions(&store, "ext_live").is_empty(),
        "平仓软删后外部反查应读不到"
    );
    let meta = store
        .table::<ManagedPosition>()
        .meta(&position_id)
        .expect("软删行仍留在表内（墓碑语义）");
    assert!(meta.deleted);
}

/// S6 时间驱动 flush：无任何信号/K 线事件、不经关停，预先存在于 Store 的行
/// 也能在间隔到期后落盘到 `{YYYYMMDD}/*.csv`——与旧"仅阈值/关停 flush"的
/// 本质差别。间隔走 RuntimeSpec（生产由 config.state_flush_interval_secs 注入）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_interval_flushes_preexisting_rows_without_events() {
    let dir = tempfile::tempdir().unwrap();
    let store = test_store(dir.path());

    // 预置只存在于内存的行（显式不 flush），且 strategy 名与 Actor 不同：
    // Actor 启动恢复与事件路径都碰不到它，只能靠定时全 Store flush 落盘。
    store
        .table::<Cooldown>()
        .insert(Cooldown {
            strategy_name: "seeded_external_row".into(),
            date: "2026-06-10".into(),
        })
        .unwrap();

    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![], // 无 bar
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let factory = Box::new(|| {
        let gw: Box<dyn AsyncOrderGateway> = Box::new(QueryGateway {
            queries: std::sync::Mutex::new(VecDeque::new()),
        });
        (gw, Box::new(NoopStrategy) as Box<dyn Strategy + Send>)
    });
    let mut rt_spec = spec("flush_live", "noop", dir.path());
    rt_spec.state_flush_interval = Duration::from_millis(50); // 测试专用小间隔
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        rt_spec,
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    // 有界等待（interval 驱动 + 轮询文件，不裸 sleep 断言）：50ms 间隔，2s 上界
    let csv = dir.path().join("20260610").join("cooldown.csv");
    let mut landed = false;
    for _ in 0..40 {
        if let Ok(text) = std::fs::read_to_string(&csv) {
            if text.contains("seeded_external_row") {
                landed = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        landed,
        "无信号事件时定时 flush 也应把预置内存行落盘到 {}",
        csv.display()
    );

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();
}

/// 阻塞式网关：submit 挂在 release 上（模拟慢执行/盘 IO 拖住 Actor 消费）
struct BlockingGateway {
    release: Arc<tokio::sync::Notify>,
}
impl AsyncOrderGateway for BlockingGateway {
    fn submit_fut<'a>(&'a mut self, _intent: &'a SignalIntent) -> SubmitFuture<'a> {
        let n = Arc::clone(&self.release);
        Box::pin(async move {
            n.notified().await;
            Err(SubmitError::retryable("阻塞后放行失败（测试用）"))
        })
    }
    fn query_fut<'a>(&'a mut self, _order_id: &'a str) -> QueryFuture<'a> {
        Box::pin(async { Err(SubmitError::retryable("无应答")) })
    }
    fn positions_fut<'a>(
        &'a mut self,
        _instrument: &'a str,
        _side: &'a str,
    ) -> PositionsFuture<'a> {
        Box::pin(async { Err(SubmitError::retryable("测试未编排仓位应答")) })
    }
}

/// B12 自愈：Actor 消费落后（提交卡住）→ Lagged → backfill 补缺
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lagged_receiver_triggers_backfill_self_heal() {
    let dir = tempfile::tempdir().unwrap();
    let backfills = Arc::new(AtomicUsize::new(0));
    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: (0..200).map(candle).collect(), // 远大于 cap=16
        backfills: Arc::clone(&backfills),
        cap: 16,
    });
    let store = test_store(dir.path());
    let release = Arc::new(tokio::sync::Notify::new());
    let factory = Box::new({
        let release = Arc::clone(&release);
        move || {
            let gw: Box<dyn AsyncOrderGateway> = Box::new(BlockingGateway {
                release: Arc::clone(&release),
            });
            let strategy: Box<dyn Strategy + Send> = Box::new(OpenOnce { done: false });
            (gw, strategy)
        }
    });
    let (handle, _task) = strategy_app::runtime_actor::spawn_supervised(
        spec("lag_live", "openonce", dir.path()),
        kline.clone(),
        store,
        factory,
        256,
        None,
    );

    // 基线：warmup(1) + Reconnected(1) 两轮 backfill 落地（Actor 已卡在首次 submit）
    for _ in 0..100 {
        if backfills.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        backfills.load(Ordering::SeqCst) >= 2,
        "基线：warmup + Reconnected 各一次 backfill"
    );

    // 放行网关：Actor 恢复消费 → 落后帧触发 Lagged → 自愈 backfill。
    // 放行前等待推送窗口走完：MockKline 以 2ms/根推 200 根（≈400ms），
    // 期间 Actor 卡在 submit、无消费，cap=16 通道必然溢出（积压帧被
    // 覆盖丢弃）——恢复消费后首帧即 Lagged。若立刻放行，Actor 尚能
    // 跟上 2ms 节奏（未溢出即不落后），Lagged 场景无从成立（时序竞态）。
    tokio::time::sleep(Duration::from_millis(500)).await;
    release.notify_waiters();
    for _ in 0..200 {
        if backfills.load(Ordering::SeqCst) >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        backfills.load(Ordering::SeqCst) >= 3,
        "Lagged 后应触发自愈 backfill，实际计数 {}",
        backfills.load(Ordering::SeqCst)
    );
    handle.shutdown().await.ok();
}

/// B12 隔离：一个代币的 Actor 卡死（提交永久阻塞），另一代币正常驱动并开仓
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_actor_does_not_stall_other_symbol() {
    let dir = tempfile::tempdir().unwrap();
    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![candle(0), candle(1), candle(2)],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let store = test_store(dir.path());
    let state_dir = dir.path().to_path_buf();

    // 慢 Actor：submit 永久阻塞（release 前不放行）
    let release = Arc::new(tokio::sync::Notify::new());
    let slow_factory = Box::new({
        let release = Arc::clone(&release);
        move || {
            let gw: Box<dyn AsyncOrderGateway> = Box::new(BlockingGateway {
                release: Arc::clone(&release),
            });
            let strategy: Box<dyn Strategy + Send> = Box::new(OpenOnce { done: false });
            (gw, strategy)
        }
    });
    // 慢 Actor 用不同代币：同策略跨代币是生产常态；若两实例同 symbol + 同策略，
    // 确定性 signal_id（策略/symbol/时间戳，不含实例名）在共享 Store 被全局
    // 去重——后到实例的首根意图被丢弃，隔离场景无从成立（时序竞态）。
    let mut slow_spec = spec("slow_live", "openonce", dir.path());
    slow_spec.symbol = "ETHUSDT".into();
    let (slow_handle, _) = strategy_app::runtime_actor::spawn_supervised(
        slow_spec,
        kline.clone(),
        store.clone(),
        slow_factory,
        256,
        None,
    );

    // 正常 Actor：BacktestBroker 即时成交（参照 bar_drives_open_fill_and_position_within_one_actor）
    let ok_factory = Box::new(move || {
        let broker = BacktestBroker::new(
            BacktestConfig {
                cash: 1000.0,
                leverage: 1,
                notional_per_order: 0.0,
                ..Default::default()
            },
            "ok_live",
            &state_dir,
        );
        let gw: Box<dyn AsyncOrderGateway> = Box::new(BacktestOrderClient::new(broker));
        let strategy: Box<dyn Strategy + Send> = Box::new(OpenOnce { done: false });
        (gw, strategy)
    });
    let (ok_handle, _) = strategy_app::runtime_actor::spawn_supervised(
        spec("ok_live", "openonce", dir.path()),
        kline.clone(),
        store.clone(),
        ok_factory,
        256,
        None,
    );

    // 快 Actor 正常完成开仓（慢 Actor 此刻仍阻塞在提交上）
    let p = wait_position(&store, "ok_live").await;
    assert!(p.state.total_size.abs() > 1e-9, "正常代币应完成开仓");
    assert!(
        store
            .table::<ManagedPosition>()
            .list()
            .iter()
            .all(|p| p.strategy_name != "slow_live"),
        "慢代币阻塞期间不应有持仓（且不得拖垮其他代币）"
    );

    // 放行慢 Actor（清理），随后优雅关闭
    release.notify_waiters();
    slow_handle.shutdown().await.ok();
    ok_handle.shutdown().await.ok();
}

/// 看门狗测试网关：query 按脚本逐次应答——第 1 次（启动对账）失败，
/// 第 2 次（bar 驱动看门狗）成功；并计数全部查询。submit 恒拒。
struct PollScriptGateway {
    calls: Mutex<VecDeque<Result<OrderSnapshot, SubmitError>>>,
    count: Arc<AtomicUsize>,
}
impl AsyncOrderGateway for PollScriptGateway {
    fn submit_fut<'a>(&'a mut self, _intent: &'a SignalIntent) -> SubmitFuture<'a> {
        Box::pin(async { Err(SubmitError::rejected("测试网关不接单")) })
    }
    fn query_fut<'a>(&'a mut self, order_id: &'a str) -> QueryFuture<'a> {
        self.count.fetch_add(1, Ordering::SeqCst);
        let id = order_id.to_string();
        let r = self
            .calls
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(SubmitError::retryable(format!("无脚本应答: {id}"))));
        Box::pin(async move { r })
    }
    fn positions_fut<'a>(
        &'a mut self,
        _instrument: &'a str,
        _side: &'a str,
    ) -> PositionsFuture<'a> {
        Box::pin(async { Err(SubmitError::retryable("测试未编排仓位应答")) })
    }
}

/// 在途看门狗：信号卡在 Submitted（order_event 丢失场景），启动对账查询
/// 失败后，bar 驱动的轮询主动 get_order 补回成交；且受最小间隔约束。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_watchdog_polls_stuck_order_on_bar() {
    let dir = tempfile::tempdir().unwrap();
    let old = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();

    // 播种：信号已 Submitted（cid=ord_stuck），但本地无持仓——order_event 丢失
    let seed_intent = SignalIntent {
        id: "sig_stuck".into(),
        strategy_id: "noop".into(),
        symbol: "BTCUSDT".into(),
        kind: IntentKind::Open,
        direction: Side::Long,
        price: 100.0,
        size: 1.0,
        size_raw: None,
        stop_price: Some(98.0),
        position_id: None,
        stop_order_id: None,
        created_event_time: old,
        status: SignalStatus::Created,
    };
    {
        let seed = test_store(dir.path());
        seed.table::<ManagedSignal>()
            .insert(ManagedSignal {
                strategy_name: "poll_live".into(),
                intent: seed_intent,
                version: 3,
                status: SignalStatus::Submitted,
                order_id: Some("ord_stuck".into()),
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

    // 3 根 bar：第 1 根触发看门狗查询（间隔 1s，后续 bar 不得重复查）
    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![candle(0), candle(1), candle(2)],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let query_count = Arc::new(AtomicUsize::new(0));
    let factory = {
        let query_count = Arc::clone(&query_count);
        Box::new(move || {
            let gw = PollScriptGateway {
                count: Arc::clone(&query_count),
                calls: Mutex::new(VecDeque::from([
                    // 启动对账：查询失败（保留下次）
                    Err(SubmitError::retryable("启动查询临时失败")),
                    // 看门狗：服务端真实状态 FILLED
                    Ok(OrderSnapshot {
                        order_id: "ord_stuck".into(),
                        state: OrderState::Filled,
                        filled_size: 1.0,
                        avg_price: 100.0,
                        exchange_order_id: None,
                        filled_quantity_raw: None,
                        place: None,
                    }),
                ])),
            };
            (
                Box::new(gw) as Box<dyn AsyncOrderGateway>,
                Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
            )
        })
    };
    let mut sp = spec("poll_live", "noop", dir.path());
    sp.poll_grace_secs = 0;
    sp.poll_interval_secs = 1;
    let store = test_store(dir.path());
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        sp,
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    // 看门狗查询补回成交 → 持仓行进共享 Store
    let pos = wait_position(&store, "poll_live").await;
    assert_eq!(pos.state.position_id, "sig_stuck");
    assert!((pos.state.entry_price - 100.0).abs() < 1e-9);

    // 等 3 根 bar 全部推送完：总查询 = 启动对账 1 + 看门狗 1（间隔去重）
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        query_count.load(Ordering::SeqCst),
        2,
        "启动对账 1 次 + 看门狗 1 次，间隔内不得重复查"
    );

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();
}

/// 在途看门狗：意图刚创建（在 60s 宽限内）时 bar 驱动不得提前查询。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_flight_watchdog_waits_for_grace_period() {
    let dir = tempfile::tempdir().unwrap();

    // 播种：信号 Submitted，created=now（宽限窗口内）；启动对账查询返回失败
    let seed_intent = SignalIntent {
        id: "sig_fresh".into(),
        strategy_id: "noop".into(),
        symbol: "BTCUSDT".into(),
        kind: IntentKind::Open,
        direction: Side::Long,
        price: 100.0,
        size: 1.0,
        size_raw: None,
        stop_price: Some(98.0),
        position_id: None,
        stop_order_id: None,
        created_event_time: Utc::now(),
        status: SignalStatus::Created,
    };
    {
        let seed = test_store(dir.path());
        seed.table::<ManagedSignal>()
            .insert(ManagedSignal {
                strategy_name: "grace_live".into(),
                intent: seed_intent,
                version: 3,
                status: SignalStatus::Submitted,
                order_id: Some("ord_fresh".into()),
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

    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![candle(0), candle(1)],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let query_count = Arc::new(AtomicUsize::new(0));
    let factory = {
        let query_count = Arc::clone(&query_count);
        Box::new(move || {
            let gw = PollScriptGateway {
                count: Arc::clone(&query_count),
                calls: Mutex::new(VecDeque::from([Err(SubmitError::retryable(
                    "启动查询临时失败",
                ))])),
            };
            (
                Box::new(gw) as Box<dyn AsyncOrderGateway>,
                Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
            )
        })
    };
    let mut sp = spec("grace_live", "noop", dir.path());
    sp.poll_grace_secs = 60; // 宽限 60s：bar 驱动的查询须被压住
    sp.poll_interval_secs = 1;
    let store = test_store(dir.path());
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        sp,
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    // 等两根 bar 推送完：仅启动对账 1 次，无持仓（看门狗在宽限内不查）
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        query_count.load(Ordering::SeqCst),
        1,
        "宽限内只应有启动对账查询"
    );
    assert!(
        store
            .table::<ManagedPosition>()
            .list()
            .into_iter()
            .all(|p| p.strategy_name != "grace_live"),
        "宽限内看门狗不得查询/补仓"
    );

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();
}

// ── 每分钟远程仓位对账（get_positions）──

/// 仓位对账网关：positions_fut 按脚本逐次应答；submit/query 不接单。
struct PosCheckGateway {
    answers: Mutex<VecDeque<Result<Vec<RemotePosition>, SubmitError>>>,
}
impl AsyncOrderGateway for PosCheckGateway {
    fn submit_fut<'a>(&'a mut self, _intent: &'a SignalIntent) -> SubmitFuture<'a> {
        Box::pin(async { Err(SubmitError::rejected("测试网关不接单")) })
    }
    fn query_fut<'a>(&'a mut self, _order_id: &'a str) -> QueryFuture<'a> {
        Box::pin(async { Err(SubmitError::retryable("测试网关不查单")) })
    }
    fn positions_fut<'a>(
        &'a mut self,
        _instrument: &'a str,
        _side: &'a str,
    ) -> PositionsFuture<'a> {
        // 脚本耗尽 → Err（fail-open，不会被当作无仓）
        let r = self
            .answers
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(SubmitError::retryable("无脚本仓位应答")));
        Box::pin(async move { r })
    }
}

/// 1s 间隔的 RuntimeSpec（测试用）。
fn pos_check_spec(name: &str, dir: &std::path::Path) -> strategy_app::runtime_actor::RuntimeSpec {
    let mut s = spec(name, "noop", dir);
    s.position_check_interval_secs = 1;
    s
}

/// 远程空仓：本地幽灵仓按 external_close 关闭，止损单撤销，history 可查。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn position_check_closes_local_when_remote_empty() {
    let dir = tempfile::tempdir().unwrap();
    seed_position(dir.path(), "pos_live", 1.0);

    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let factory = Box::new(|| {
        let gw = PosCheckGateway {
            answers: Mutex::new(VecDeque::from([Ok(vec![])])),
        };
        (
            Box::new(gw) as Box<dyn AsyncOrderGateway>,
            Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
        )
    });
    let store = test_store(dir.path());
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        pos_check_spec("pos_live", dir.path()),
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    wait_position_closed(&store, "pos_live").await;

    // 止损单已撤销；平仓历史按 external_close 落账
    let stop = store
        .table::<ManagedStop>()
        .list()
        .into_iter()
        .find(|s| s.strategy_name == "pos_live")
        .expect("止损行仍在");
    assert_eq!(stop.lifecycle, "cancelled");
    let hist = store
        .table::<strategy_positions::HistoryPosition>()
        .list()
        .into_iter()
        .find(|h| h.exit_reason == "external_close")
        .expect("external_close 应落 history");
    assert!((hist.exit_size - 1.0).abs() < 1e-9);

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();
}

/// 远程数量小于本地（远程 1、本地 2）：判定已平仓。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn position_check_closes_when_remote_quantity_smaller() {
    let dir = tempfile::tempdir().unwrap();
    seed_position(dir.path(), "pos_live", 2.0);

    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let factory = Box::new(|| {
        // 同方向但只有 1.0：小仓可能不是本策略的
        let gw = PosCheckGateway {
            answers: Mutex::new(VecDeque::from([Ok(vec![RemotePosition {
                instrument: "BTCUSDT".into(),
                side: "BUY".into(),
                quantity: 1.0,
                avg_price: 100.0,
                deleted: false,
            }])])),
        };
        (
            Box::new(gw) as Box<dyn AsyncOrderGateway>,
            Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
        )
    });
    let store = test_store(dir.path());
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        pos_check_spec("pos_live", dir.path()),
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    wait_position_closed(&store, "pos_live").await;

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();
}

/// 远程三要素一致：持仓保留（跨两个对账 tick，第二次脚本耗尽 fail-open）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn position_check_keeps_position_when_remote_matches() {
    let dir = tempfile::tempdir().unwrap();
    seed_position(dir.path(), "pos_live", 1.0);

    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let factory = Box::new(|| {
        let gw = PosCheckGateway {
            answers: Mutex::new(VecDeque::from([Ok(vec![RemotePosition {
                instrument: "BTCUSDT".into(),
                side: "BUY".into(),
                quantity: 1.0,
                avg_price: 100.0,
                deleted: false,
            }])])),
        };
        (
            Box::new(gw) as Box<dyn AsyncOrderGateway>,
            Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
        )
    });
    let store = test_store(dir.path());
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        pos_check_spec("pos_live", dir.path()),
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    // 等两个对账周期（第一次匹配，第二次无脚本 fail-open），持仓仍在
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let open = store
        .table::<ManagedPosition>()
        .list()
        .into_iter()
        .any(|p| p.strategy_name == "pos_live");
    assert!(open, "远程匹配时不得关闭本地仓");
    assert!(
        store
            .table::<strategy_positions::HistoryPosition>()
            .list()
            .is_empty(),
        "匹配时不得落 external_close 历史"
    );

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();
}

/// 查询失败（Err）：fail-open 保留持仓，不落 history。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn position_check_failopen_keeps_position_on_error() {
    let dir = tempfile::tempdir().unwrap();
    seed_position(dir.path(), "pos_live", 1.0);

    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    let factory = Box::new(|| {
        let gw = PosCheckGateway {
            answers: Mutex::new(VecDeque::from([Err(SubmitError::retryable(
                "positions 应答超时",
            ))])),
        };
        (
            Box::new(gw) as Box<dyn AsyncOrderGateway>,
            Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
        )
    });
    let store = test_store(dir.path());
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        pos_check_spec("pos_live", dir.path()),
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    // 两个对账 tick 都失败（第二个脚本耗尽也是 Err），持仓保留
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let open = store
        .table::<ManagedPosition>()
        .list()
        .into_iter()
        .any(|p| p.strategy_name == "pos_live");
    assert!(open, "查询失败必须 fail-open，不得错杀本地仓");
    assert!(
        store
            .table::<strategy_positions::HistoryPosition>()
            .list()
            .is_empty(),
        "fail-open 不得落 external_close 历史"
    );

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();
}

/// U11：paper 模式不跑远程对账——即使远程应答为空（对 live 是幽灵仓信号），
/// 本地仓位也必须存活，不得 external_close。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn paper_mode_position_survives_remote_reconcile() {
    let dir = tempfile::tempdir().unwrap();
    seed_position(dir.path(), "pos_paper", 1.0);

    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });
    // 远程返回空：live 路径会判幽灵仓并 external_close；paper 必须无视
    let factory = Box::new(|| {
        let gw = PosCheckGateway {
            answers: Mutex::new(VecDeque::from([Ok(vec![])])),
        };
        (
            Box::new(gw) as Box<dyn AsyncOrderGateway>,
            Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
        )
    });
    let store = test_store(dir.path());
    let mut s = pos_check_spec("pos_paper", dir.path());
    s.mode = "paper_trading".into(); // ← U11 关键：paper 实例
    let (handle, task) = strategy_app::runtime_actor::spawn_supervised(
        s,
        kline,
        Arc::clone(&store),
        factory,
        8,
        None,
    );

    // 等超过 2 个对账 tick（1s 间隔），仓位必须仍在
    tokio::time::sleep(Duration::from_millis(2300)).await;
    let open = store
        .table::<ManagedPosition>()
        .list()
        .into_iter()
        .any(|p| p.strategy_name == "pos_paper");
    assert!(open, "paper 仓不得被远程对账误平（U11）");
    assert!(
        store
            .table::<strategy_positions::HistoryPosition>()
            .list()
            .is_empty(),
        "paper 不得落 external_close 历史"
    );

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("Shutdown 后应退出")
        .unwrap();
}

/// U14：混合模式共存——同一进程两个 Actor（live + paper）共享 Store，
/// 各自的 position 行独立共存，互不覆盖。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_mode_actors_coexist_on_shared_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = test_store(dir.path());
    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });

    // 两个实例各自的 strategy_name 空间（`_LIVE` / `_PAPER_TRADING` 后缀）
    let live_name = "mix_4H_3_BTCUSDT_LIVE";
    let paper_name = "mix_4H_3_WLDUSDT_PAPER_TRADING";

    // 共享 Store：先各写一行仓位（模拟双方开仓落账），验证独立共存
    {
        use strategy_core::{FillEvent, Side as CoreSide};
        use strategy_positions::PositionManager;
        let mut pm_live =
            PositionManager::new(live_name, "BTCUSDT", "live", Arc::clone(&store), dir.path());
        let mut pm_paper = PositionManager::new(
            paper_name,
            "WLDUSDT",
            "paper_trading",
            Arc::clone(&store),
            dir.path(),
        );
        let fill_live = FillEvent::new(
            String::from("sig_live"),
            String::from("ord_live"),
            String::from("BTCUSDT"),
            CoreSide::Long,
            1.0,
            100.0,
            0.0,
            Utc::now(),
        );
        let fill_paper = FillEvent::new(
            String::from("sig_paper"),
            String::from("ord_paper"),
            String::from("WLDUSDT"),
            CoreSide::Short,
            2.0,
            0.5,
            0.0,
            Utc::now(),
        );
        pm_live
            .apply_open_fill(&fill_live, Utc::now(), 98.0, 0.0)
            .unwrap();
        pm_paper
            .apply_open_fill(&fill_paper, Utc::now(), 0.52, 0.0)
            .unwrap();
        store.flush().unwrap();
    }

    // 共享表两行独立共存：strategy_name 各自隔离，size/方向不互踩
    let rows = store.table::<ManagedPosition>().list();
    let live_row = rows
        .iter()
        .find(|p| p.strategy_name == live_name)
        .expect("live 仓位行应在共享表中");
    let paper_row = rows
        .iter()
        .find(|p| p.strategy_name == paper_name)
        .expect("paper 仓位行应在共享表中");
    assert_eq!(live_row.state.symbol, "BTCUSDT");
    assert_eq!(paper_row.state.symbol, "WLDUSDT");
    assert!((live_row.state.total_size - 1.0).abs() < 1e-9);
    assert!((paper_row.state.total_size - 2.0).abs() < 1e-9);

    // 两个 Actor 挂载各自 mode 后并存运行，互不干扰
    let mut live_spec = spec(live_name, "noop", dir.path());
    live_spec.mode = "live".into();
    live_spec.symbol = "BTCUSDT".into();
    let (h_live, t_live) = strategy_app::runtime_actor::spawn_supervised(
        live_spec,
        Arc::clone(&kline),
        Arc::clone(&store),
        Box::new(|| {
            let broker = BacktestBroker::new(
                BacktestConfig {
                    cash: 1000.0,
                    ..Default::default()
                },
                "mix_live",
                std::env::temp_dir(),
            );
            (
                Box::new(BacktestOrderClient::new(broker)) as Box<dyn AsyncOrderGateway>,
                Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
            )
        }),
        8,
        None,
    );
    let mut paper_spec = spec(paper_name, "noop", dir.path());
    paper_spec.mode = "paper_trading".into();
    paper_spec.symbol = "WLDUSDT".into();
    let (h_paper, t_paper) = strategy_app::runtime_actor::spawn_supervised(
        paper_spec,
        Arc::clone(&kline),
        Arc::clone(&store),
        Box::new(|| {
            let broker = BacktestBroker::new(
                BacktestConfig {
                    cash: 1000.0,
                    ..Default::default()
                },
                "mix_paper",
                std::env::temp_dir(),
            );
            (
                Box::new(BacktestOrderClient::new(broker)) as Box<dyn AsyncOrderGateway>,
                Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
            )
        }),
        8,
        None,
    );

    tokio::time::sleep(Duration::from_millis(200)).await;
    // 并存运行后两行仍在（restore 不覆盖兄弟实例的未 flush 态）
    let rows_after = store.table::<ManagedPosition>().list();
    assert!(
        rows_after.iter().any(|p| p.strategy_name == live_name),
        "live 仓位行不得被 paper 实例踩掉"
    );
    assert!(
        rows_after.iter().any(|p| p.strategy_name == paper_name),
        "paper 仓位行不得被 live 实例踩掉"
    );

    h_live.shutdown().await.unwrap();
    h_paper.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let _ = t_live.await;
        let _ = t_paper.await;
    })
    .await
    .expect("两实例 Shutdown 后应退出");
}

/// 快速验证 live/paper **互不影响**：共享 Store 上两个实例，远程对账均返回空——
/// live 仓被 external_close（正常对账语义），paper 仓必须原样存活。
/// 一次跑通即证明：live 的对账不波及 paper，paper 的存在不改变 live 行为。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_and_paper_reconcile_are_isolated() {
    let dir = tempfile::tempdir().unwrap();
    // 同一 Store 实例 seed 两行（两次独立 seed 会互相覆盖日快照）
    let store = test_store(dir.path());
    {
        use strategy_core::{FillEvent, Side as CoreSide};
        use strategy_positions::PositionManager;
        let mut pm_live = PositionManager::new(
            "iso_live",
            "BTCUSDT",
            "live",
            Arc::clone(&store),
            dir.path(),
        );
        let mut pm_paper = PositionManager::new(
            "iso_paper",
            "WLDUSDT",
            "paper_trading",
            Arc::clone(&store),
            dir.path(),
        );
        let fill = |sid: &str, sym: &str, side: CoreSide| {
            FillEvent::new(
                String::from(sid),
                String::from("ord_") + sid,
                String::from(sym),
                side,
                1.0,
                100.0,
                0.0,
                Utc::now(),
            )
        };
        pm_live
            .apply_open_fill(
                &fill("sig_live", "BTCUSDT", CoreSide::Long),
                Utc::now(),
                98.0,
                0.0,
            )
            .unwrap();
        pm_paper
            .apply_open_fill(
                &fill("sig_paper", "WLDUSDT", CoreSide::Short),
                Utc::now(),
                102.0,
                0.0,
            )
            .unwrap();
        store.flush().unwrap();
    }
    // 前置：两行都已落共享表（防止空断言通过）
    {
        let rows = store.table::<ManagedPosition>().list();
        assert!(
            rows.iter().any(|p| p.strategy_name == "iso_live"),
            "前置：live 仓位行必须已存在"
        );
        assert!(
            rows.iter().any(|p| p.strategy_name == "iso_paper"),
            "前置：paper 仓位行必须已存在"
        );
    }

    let kline: Arc<dyn KlineClient> = Arc::new(MockKline {
        bars: vec![],
        backfills: Arc::new(AtomicUsize::new(0)),
        cap: 64,
    });

    // 双方远程对账都返回空（对 live 是幽灵仓信号；paper 应直接跳过对账）
    let mk_factory = || {
        Box::new(|| {
            let gw = PosCheckGateway {
                answers: Mutex::new(VecDeque::from([Ok(vec![])])),
            };
            (
                Box::new(gw) as Box<dyn AsyncOrderGateway>,
                Box::new(NoopStrategy) as Box<dyn Strategy + Send>,
            )
        })
    };

    let mut live_spec = pos_check_spec("iso_live", dir.path());
    live_spec.mode = "live".into();
    let (h_live, t_live) = strategy_app::runtime_actor::spawn_supervised(
        live_spec,
        Arc::clone(&kline),
        Arc::clone(&store),
        mk_factory(),
        8,
        None,
    );

    let mut paper_spec = pos_check_spec("iso_paper", dir.path());
    paper_spec.mode = "paper_trading".into();
    let (h_paper, t_paper) = strategy_app::runtime_actor::spawn_supervised(
        paper_spec,
        Arc::clone(&kline),
        Arc::clone(&store),
        mk_factory(),
        8,
        None,
    );

    // 等超过 2 个对账 tick（1s 间隔）
    tokio::time::sleep(Duration::from_millis(2300)).await;

    let rows = store.table::<ManagedPosition>().list();
    let live_still_open = rows.iter().any(|p| p.strategy_name == "iso_live");
    let paper_still_open = rows.iter().any(|p| p.strategy_name == "iso_paper");

    // live：远程空 → 正常 external_close（对账语义不变）
    assert!(!live_still_open, "live 仓应被对账关闭（远程无匹配）");
    // paper：对账跳过 → 仓位存活（live 的对账不波及 paper）
    assert!(paper_still_open, "paper 仓不得被 live 的对账逻辑波及");

    // history：只有 live 落 external_close，paper 不得落
    let hist = store.table::<strategy_positions::HistoryPosition>().list();
    let hist_dbg: Vec<_> = hist
        .iter()
        .map(|h| format!("{}:{}", h.strategy_name, h.exit_reason))
        .collect();
    assert!(
        hist.iter()
            .any(|h| h.strategy_name == "iso_live" && h.exit_reason == "external_close"),
        "live 应落 external_close 历史，实际: {hist_dbg:?}"
    );
    assert!(
        !hist.iter().any(|h| h.strategy_name == "iso_paper"),
        "paper 不得产生任何平仓历史，实际: {hist_dbg:?}"
    );

    h_live.shutdown().await.unwrap();
    h_paper.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let _ = t_live.await;
        let _ = t_paper.await;
    })
    .await
    .expect("两实例 Shutdown 后应退出");
}
