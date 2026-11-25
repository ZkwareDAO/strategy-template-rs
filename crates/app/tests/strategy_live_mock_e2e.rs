//! PR-5 Task 4 验收：横截面 live actor 装配冒烟（paper 通路）。
//!
//! 预注入双标的各 5 根 1m（channel 扇出口径）→ `spawn_strategy_live` → 事件泵
//! 自然收口（Done）→ flush 出 `strategy_portfolio.csv`（bars>0、无成交）。
//! 本文件为新建验收资产，不触碰任何既有断言。

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use strategy_app::bootstrap::start_live_strategy_set;
use strategy_app::config::{ServicesConfig, SymbolSpec, TradingServiceCfg};
use strategy_app::strategy_config::StrategySpec;
use strategy_app::strategy_live_actor::{
    spawn_strategy_live, StrategyLiveAssembly, StrategyLiveSpec,
};
use strategy_core::candle::Candle;
use strategy_core::risk::RiskControlConfig;
use strategy_data::csv_store;
use strategy_engine::core::bar::Timeframe;
use strategy_engine::strategy::config::{
    PortfolioExitCfg, RebalanceCfg, RiskCfg, SizingCfg, StrategyConfig, StrategyMode, TrailingCfg,
    UniverseEntry, WeightScheme,
};
use strategy_engine::strategy::model::{
    MasterPosition, MasterPositionState, PortfolioPhase, StrategyPortfolio, SubPosition,
};
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};
use strategy_store::{Clock, Store};

/// 模拟行情时钟：按日裁剪的「今天」必须与 K 线时间同源。用墙钟会把
/// 2026-01-05 的模拟平仓行判成历史日，从内存与快照里裁掉。
struct SimClock;
impl Clock for SimClock {
    fn now_ms(&self) -> i64 {
        Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0)
            .unwrap()
            .timestamp_millis()
    }
}

fn candle(t: chrono::DateTime<Utc>, close: f64) -> Candle {
    Candle {
        open_time: t,
        open: close,
        high: close,
        low: close,
        close,
        volume: 1.0,
        quote_volume: close,
        trade_num: 1,
        active_buy_volume: 0.0,
        active_buy_quote_volume: 0.0,
        is_closed: true,
    }
}

fn symbol_spec(symbol: &str) -> SymbolSpec {
    SymbolSpec {
        strategy: "template_xsec_ls".into(),
        symbol: symbol.into(),
        timeframe: "1m".into(),
        cash: 200.0,
        initial_cash: None,
        params: None,
        warmup_days: 0,
        exchange: "binance".into(),
        venue_key: format!("binance/crypto_perpetual/{symbol}"),
        account: "default".into(),
        leverage: 1,
        mode: "paper_trading".into(),
        contract: None,
        risk: RiskControlConfig {
            enabled: false,
            ..RiskControlConfig::default()
        },
        venue_qty_step: None,
        venue_min_qty: None,
        venue_qty_precision: None,
    }
}

fn strategy_cfg() -> StrategyConfig {
    StrategyConfig {
        strategy_type: "template_xsec_ls".into(),
        strategy_id: Some("template_xsec_ls:default".into()),
        mode: StrategyMode::CrossSectional,
        merge_positions: true,
        universe: vec![
            UniverseEntry::Symbol("BTCUSDT".into()),
            UniverseEntry::Symbol("ETHUSDT".into()),
        ],
        timeframes: vec!["1m".into()],
        indicators: Default::default(),
        entry: Default::default(),
        exit: Default::default(),
        portfolio_exit: PortfolioExitCfg {
            trailing: None,
            stop_loss_pct: None,
        },
        rebalance: Some(RebalanceCfg {
            every_bars: 5,
            top_k: 2,
            bottom_k: 0,
            weight_scheme: WeightScheme::Equal,
        }),
        risk: RiskCfg {
            max_drawdown: None,
            max_total_notional: 1.0,
        },
        sizing: SizingCfg {
            per_position_pct: 0.1,
            max_position_pct: None,
        },
        params: None,
    }
}

/// 计数策略：每次决策轮 +1，恒 Hold（无成交路径）。
struct CountingStrategy {
    calls: Arc<AtomicUsize>,
}

impl CrossSectionalStrategyPort for CountingStrategy {
    fn strategy_type(&self) -> &str {
        "counting"
    }

    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[Timeframe],
        _snapshots: &[InstrumentSnapshot],
        _portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Vec::new()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strategy_actor_paper_two_symbols_processes_queued_candles() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let store = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();

    // 双标各 5 根 1m（合成价）；发送端 drop = 断流 → 对齐层 Done → 收口。
    let mut candle_rxs = Vec::new();
    for seed in [100.0f64, 50.0] {
        let (tx, rx) = std::sync::mpsc::channel::<Candle>();
        for k in 0..5i64 {
            tx.send(candle(t0 + ChronoDuration::minutes(k), seed + k as f64))
                .unwrap();
        }
        drop(tx);
        candle_rxs.push(rx);
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = Arc::clone(&calls);
    let strategy_factory = Box::new(move || -> Box<dyn CrossSectionalStrategyPort> {
        Box::new(CountingStrategy {
            calls: Arc::clone(&factory_calls),
        })
    });

    let spec = StrategyLiveSpec {
        strategy_id: "template_xsec_ls:default".into(),
        strategy_type: "template_xsec_ls".into(),
        config: strategy_cfg(),
        instruments: vec![symbol_spec("BTCUSDT"), symbol_spec("ETHUSDT")],
        mode: "paper_trading".into(),
        state_dir: state_dir.clone(),
        state_flush_interval_secs: 3600,
        position_check_interval_secs: 3600,
        initial_cash_total: 10_000.0,
        fee_rate: None,
        channel_cap: 64,
    };
    let assembly = StrategyLiveAssembly {
        spec,
        candle_rxs,
        warmup: vec![Vec::new(), Vec::new()],
        store: Arc::clone(&store),
        gateway_factory: None, // paper：本地撮合
        strategy_factory,
        route_tx: None,
    };
    let (_handle, task) = spawn_strategy_live(assembly);
    tokio::time::timeout(Duration::from_secs(15), task)
        .await
        .expect("数据耗尽后 actor 必须自然收口（不得悬挂）")
        .expect("监督任务不得 panic");

    assert!(
        calls.load(Ordering::SeqCst) > 0,
        "bars>0：至少发生一次决策轮（策略被调用）"
    );
    assert!(
        store.table::<SubPosition>().list().is_empty(),
        "无成交路径不得产生子仓行"
    );
    // state 目录 flush 出组合账本 CSV（按日目录布局 {state}/{YYYYMMDD}/{NAME}.csv）。
    let mut found = false;
    if let Ok(rd) = std::fs::read_dir(&state_dir) {
        for day in rd.flatten() {
            if day.path().join("strategy_portfolio.csv").exists() {
                found = true;
            }
        }
    }
    assert!(found, "state 目录必须落盘 strategy_portfolio.csv");
}

// ─── T5：live 订单/回报接线验收（stub AsyncOrderGateway）────────────────

use strategy_app::runtime_types::RuntimeHandle;
use strategy_core::order::OrderState as CoreOrderState;
use strategy_core::signal::Side;
use strategy_core::{IntentKind, SignalIntent, SignalStatus};
use strategy_engine::core::identity::ExchangeId;
use strategy_engine::core::identity::InstrumentId;
use strategy_engine::strategy::model::SubPositionState;
use strategy_signals::gateway::{
    AsyncOrderGateway, FailureKind, OrderSnapshot, PositionsFuture, QueryFuture, SubmitError,
    SubmitFuture,
};
use strategy_signals::ManagedSignal;

/// 可观测 stub 网关：记录每次 submit 的 signal_id；Close 可按开关拒单
/// （ReduceOnly 文案）；查询恒无快照（看门狗路径 fail-open）。
#[derive(Default)]
struct StubState {
    attempts: std::sync::Mutex<Vec<String>>,
    reject_close: std::sync::Mutex<Option<String>>,
    /// 开仓 submit 直接拒单（交易所侧拒绝，语义见 P0 异常面用例）
    reject_open: std::sync::Mutex<Option<String>>,
    next_id: AtomicUsize,
    /// `positions_fut` 应答（T8 重启演练注入「券商仍持有」的仓位）
    remote_positions: std::sync::Mutex<Vec<strategy_signals::gateway::RemotePosition>>,
    /// `query_fut` 应答（重启/重连的在途单查询快照；None = 可重试失败）
    query_snapshot: std::sync::Mutex<Option<OrderSnapshot>>,
}

struct StubGateway {
    state: Arc<StubState>,
}

impl AsyncOrderGateway for StubGateway {
    fn submit_fut<'a>(&'a mut self, intent: &'a SignalIntent) -> SubmitFuture<'a> {
        let sid = intent.id.clone();
        let kind = intent.kind;
        let st = Arc::clone(&self.state);
        Box::pin(async move {
            st.attempts.lock().unwrap().push(sid);
            if kind == IntentKind::Close {
                if let Some(msg) = st.reject_close.lock().unwrap().clone() {
                    return Err(SubmitError::rejected(msg));
                }
            }
            if kind == IntentKind::Open {
                if let Some(msg) = st.reject_open.lock().unwrap().clone() {
                    return Err(SubmitError::rejected(msg));
                }
            }
            let n = st.next_id.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(format!("ord_{n}"))
        })
    }

    fn query_fut<'a>(&'a mut self, _signal_id: &'a str) -> QueryFuture<'a> {
        let snap = self.state.query_snapshot.lock().unwrap().clone();
        Box::pin(async move {
            snap.ok_or_else(|| SubmitError {
                kind: FailureKind::Retryable,
                message: "stub：无查询快照".into(),
            })
        })
    }

    fn positions_fut<'a>(
        &'a mut self,
        _instrument: &'a str,
        _side: &'a str,
    ) -> PositionsFuture<'a> {
        let rows = self.state.remote_positions.lock().unwrap().clone();
        Box::pin(async move { Ok(rows) })
    }
}

/// 按序脚本策略：第 n 次决策调用返回 script[n-1]，用尽后恒 Hold。
struct ScriptedStrategy {
    calls: Arc<AtomicUsize>,
    script: Vec<Vec<InstrumentAction>>,
}

impl CrossSectionalStrategyPort for ScriptedStrategy {
    fn strategy_type(&self) -> &str {
        "scripted"
    }

    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[Timeframe],
        _snapshots: &[InstrumentSnapshot],
        _portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.script.get(n - 1).cloned().unwrap_or_default()
    }
}

struct LiveHarness {
    handle: RuntimeHandle,
    task: tokio::task::JoinHandle<()>,
    store: Arc<Store>,
    stub: Arc<StubState>,
    tx: std::sync::mpsc::Sender<Candle>,
    t0: chrono::DateTime<Utc>,
}

fn spawn_harness(
    dir: &std::path::Path,
    script: Vec<Vec<InstrumentAction>>,
    reject_close: Option<String>,
) -> LiveHarness {
    spawn_harness_stub(
        dir,
        script,
        StubState {
            reject_close: std::sync::Mutex::new(reject_close),
            ..Default::default()
        },
    )
}

/// 可编排 stub 的装配变体：拒开仓 / 查询快照由用例预置。
fn spawn_harness_stub(
    dir: &std::path::Path,
    script: Vec<Vec<InstrumentAction>>,
    stub: StubState,
) -> LiveHarness {
    let state_dir = dir.join("state");
    let store = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
    let (tx, rx) = std::sync::mpsc::channel::<Candle>();
    let script_f = script.clone();
    let strategy_factory = Box::new(move || -> Box<dyn CrossSectionalStrategyPort> {
        Box::new(ScriptedStrategy {
            calls: Arc::new(AtomicUsize::new(0)),
            script: script_f.clone(),
        })
    });
    let stub = Arc::new(stub);
    let stub_f = Arc::clone(&stub);
    let gateway_factory: Option<Box<dyn Fn() -> Box<dyn AsyncOrderGateway> + Send>> =
        Some(Box::new(move || -> Box<dyn AsyncOrderGateway> {
            Box::new(StubGateway {
                state: Arc::clone(&stub_f),
            })
        }));
    let spec = StrategyLiveSpec {
        strategy_id: "template_xsec_ls:default".into(),
        strategy_type: "template_xsec_ls".into(),
        config: strategy_cfg(),
        instruments: vec![symbol_spec("BTCUSDT")],
        mode: "live".into(),
        state_dir,
        state_flush_interval_secs: 3600,
        position_check_interval_secs: 3600,
        initial_cash_total: 5_000.0,
        fee_rate: None,
        channel_cap: 64,
    };
    let assembly = StrategyLiveAssembly {
        spec,
        candle_rxs: vec![rx],
        warmup: vec![Vec::new()],
        store: Arc::clone(&store),
        gateway_factory,
        strategy_factory,
        route_tx: None,
    };
    let (handle, task) = spawn_strategy_live(assembly);
    LiveHarness {
        handle,
        task,
        store,
        stub,
        tx,
        t0: Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
    }
}

impl LiveHarness {
    fn send_bar(&self, minute: i64, close: f64) {
        self.tx
            .send(candle(self.t0 + ChronoDuration::minutes(minute), close))
            .unwrap();
    }

    async fn wait_sub(&self, want: SubPositionState) -> SubPosition {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(s) = self
                .store
                .table::<SubPosition>()
                .list()
                .into_iter()
                .find(|s| s.state == want)
            {
                return s;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "等待子仓状态 {want:?} 超时（当前 {:?}）",
                self.store.table::<SubPosition>().list()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn shutdown(self) {
        self.handle.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .expect("Shutdown 后 actor 必须收口")
            .expect("监督任务不得 panic");
    }
}

fn open_action(inst: &InstrumentId) -> InstrumentAction {
    InstrumentAction::Open {
        instrument: inst.clone(),
        side: Side::Long,
        price: 100.0,
        stop_price: 0.0,
        atr: 0.0,
        strength: 1.0,
        reason: "t5_open".into(),
    }
}

fn close_action(inst: &InstrumentId) -> InstrumentAction {
    InstrumentAction::Close {
        instrument: inst.clone(),
        price: 100.0,
        reason: "t5_close".into(),
        is_stop_loss: false,
        strength: 1.0,
    }
}

/// 三级确认闭环（ack → update/fill → ledger）：子仓
/// OpenRequested →（回报 FILLED 后轮首 sync）→ Active；同一 signal_id 只提交一次。
/// 备注：`PositionState.remaining_size_raw` 属进程内视图（不外露），C7 raw 原样
/// 已由 engine 级 `live_bridge` 测试钉死；此处按可观测的账本行断言。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_ack_update_fill_ledger_three_stages() {
    let dir = tempfile::tempdir().unwrap();
    let h = spawn_harness(dir.path(), vec![vec![open_action(&perpetual_btc())]], None);
    // t0/t1 → 决策轮 t0：Open 派发 → 提交 → stub ack(ord_1) → 子仓 OpenRequested。
    h.send_bar(0, 100.0);
    h.send_bar(1, 100.0);
    let sub = h.wait_sub(SubPositionState::OpenRequested).await;
    assert!(
        sub.id.starts_with("sub_"),
        "横截面确定性 signal_id（sub_ 命名空间），实际 {}",
        sub.id
    );
    let attempts = h.stub.attempts.lock().unwrap().clone();
    assert_eq!(attempts, vec![sub.id.clone()], "每轮至多一次提交");

    // 回报 FILLED（真实 fee/qty_raw）→ 下一根 bar 轮首 sync → Active。
    h.handle
        .order_update(
            "ord_1".into(),
            None,
            CoreOrderState::Filled,
            1.0,
            100.0,
            Some("1".into()),
            Some(sub.id.clone()),
            None,
            None,
            Some(0.1),
            h.t0 + ChronoDuration::minutes(2),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    h.send_bar(2, 100.5);
    let sub = h.wait_sub(SubPositionState::Active).await;
    assert_eq!(sub.qty, 1.0, "成交量按回报入账");

    // 再走一轮：不得重发同一 signal_id（确定性 ID + 1:1 守卫）。
    h.send_bar(3, 101.0);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        h.stub.attempts.lock().unwrap().len(),
        1,
        "同一信号绝不重复提交"
    );
    h.shutdown().await;
}

fn perpetual_btc() -> InstrumentId {
    InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT")
}

/// ReduceOnly 拒单 ≡ 交易所无仓：crypto 清镜像账本 → 立即对账 → 子仓
/// RemoteFlat→Closed；后续轮不得重发平仓单。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reduce_only_reject_maps_external_close() {
    let dir = tempfile::tempdir().unwrap();
    let inst = perpetual_btc();
    let h = spawn_harness(
        dir.path(),
        vec![
            vec![open_action(&inst)],
            Vec::new(), // 第 2 轮 Hold（等 FILL 落账为 Active）
            vec![close_action(&inst)],
        ],
        Some("ReduceOnly：交易所无持仓，拒单".into()),
    );
    // 开仓链：提交 → ack → FILLED → 下一根 → Active。
    h.send_bar(0, 100.0);
    h.send_bar(1, 100.0);
    let sub = h.wait_sub(SubPositionState::OpenRequested).await;
    h.handle
        .order_update(
            "ord_1".into(),
            None,
            CoreOrderState::Filled,
            1.0,
            100.0,
            Some("1".into()),
            Some(sub.id.clone()),
            None,
            None,
            Some(0.1),
            h.t0 + ChronoDuration::minutes(2),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    h.send_bar(2, 100.0);
    h.wait_sub(SubPositionState::Active).await;

    // t3 → 决策轮 t2：Close 派发 → 提交 → stub ReduceOnly 拒单 → 外部平仓对账。
    h.send_bar(3, 100.0);
    let sub = h.wait_sub(SubPositionState::Closed).await;
    assert_eq!(sub.qty, 1.0, "平仓收敛不篡改历史数量");
    let attempts = h.stub.attempts.lock().unwrap().clone();
    assert_eq!(attempts.len(), 2, "开/平各一次提交");
    assert!(
        attempts[1].starts_with("sub_") && attempts[1].ends_with("_leg0"),
        "平仓腿 signal_id 属 sub_ 命名空间且带 leg 后缀（独立确定性派生，实际 {}）",
        attempts[1]
    );
    // 后续轮：Closed 子仓不再产生平仓提交（禁止重发）。
    h.send_bar(4, 100.0);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(h.stub.attempts.lock().unwrap().len(), 2, "拒绝后禁止重发");
    h.shutdown().await;
}

/// 重启先查后发：Store 预置 Active 子仓 + 远程镜像为空 → recover 装配期即
/// 收敛（Closed），随后才消费已排队的 candle；全程零提交。
/// （泵代码顺序（assemble→recover→restart_recovery/poll→loop）即断言对象）
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_reconciles_inflight_before_consuming_candles() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let store = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
    let inst = perpetual_btc();
    // 预置：Store 行 Active qty=1（上一代遗留），镜像账本（远程）为空。
    store.table::<SubPosition>().upsert(SubPosition {
        id: "sub_restart".into(),
        strategy_id: "template_xsec_ls:default".into(),
        master_id: "master_000001".into(),
        instrument: inst.clone(),
        side: Side::Long,
        qty: 1.0,
        entry_price: 100.0,
        stop_price: None,
        target_weight: 0.0,
        rebalance_epoch: 0,
        state: SubPositionState::Active,
        opened_at: Some(Utc.with_ymd_and_hms(2026, 1, 4, 23, 0, 0).unwrap()),
        closed_at: None,
        realized_pnl: None,
    });
    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
    let (tx, rx) = std::sync::mpsc::channel::<Candle>();
    // 消费前就有排队 candle：证明 recover 先于消费收口。
    tx.send(candle(t0, 100.0)).unwrap();
    tx.send(candle(t0 + ChronoDuration::minutes(1), 100.0))
        .unwrap();
    let strategy_factory = Box::new(|| -> Box<dyn CrossSectionalStrategyPort> {
        Box::new(CountingStrategy {
            calls: Arc::new(AtomicUsize::new(0)),
        })
    });
    let stub = Arc::new(StubState::default());
    let stub_f = Arc::clone(&stub);
    let gateway_factory: Option<Box<dyn Fn() -> Box<dyn AsyncOrderGateway> + Send>> =
        Some(Box::new(move || -> Box<dyn AsyncOrderGateway> {
            Box::new(StubGateway {
                state: Arc::clone(&stub_f),
            })
        }));
    let spec = StrategyLiveSpec {
        strategy_id: "template_xsec_ls:default".into(),
        strategy_type: "template_xsec_ls".into(),
        config: strategy_cfg(),
        instruments: vec![symbol_spec("BTCUSDT")],
        mode: "live".into(),
        state_dir,
        state_flush_interval_secs: 3600,
        position_check_interval_secs: 3600,
        initial_cash_total: 5_000.0,
        fee_rate: None,
        channel_cap: 64,
    };
    let (handle, task) = spawn_strategy_live(StrategyLiveAssembly {
        spec,
        candle_rxs: vec![rx],
        warmup: vec![Vec::new()],
        store: Arc::clone(&store),
        gateway_factory,
        strategy_factory,
        route_tx: None,
    });
    // 装配期 recover（远程空 → RemoteFlat→Closed）先完成，客观断言：
    // 子仓 Closed 出现时网关提交数必为 0（消费 candle 之前，Hold 无动作）。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let subs = store.table::<SubPosition>().list();
        if let Some(s) = subs.iter().find(|s| s.id == "sub_restart") {
            if s.state == SubPositionState::Closed {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "recover 对账未在期限内收敛（当前 {:?}）",
            subs
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        stub.attempts.lock().unwrap().is_empty(),
        "重启恢复全程不得提交（先查后发）"
    );
    // 放行收口：断流 → Done → finish；再断言零提交。
    drop(tx);
    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("Shutdown 后必须收口")
        .expect("监督任务不得 panic");
    assert!(stub.attempts.lock().unwrap().is_empty());
}

// ─── T7：mock WS 端到端演练（全链 + 重启 recover）──────────────────────

use std::collections::HashMap;
use strategy_app::order_router::OrderRouter;
use strategy_app::ts_kline::TradingServiceKlineClient;
use strategy_broker::{
    ClientEvent, MockPositionHandle, MockScenario, MockServer, OrderVenueCfg, TradingConfig,
    TradingGateway, TradingServiceClient,
};
use strategy_data::KlineClient;

const DRILL_SECRET: &str = "drill_secret";
const DRILL_SYMBOLS: [&str; 2] = ["BTCUSDT", "ETHUSDT"];

/// 双标合成 1m CSV（`*_1m.csv` 命名 = KlineBook symbol 推断口径）。
fn write_drill_csv(dir: &std::path::Path, symbol: &str, base: f64) {
    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
    let mut body = String::from("timestamp,open,high,low,close,volume\n");
    for k in 0..40i64 {
        let t = t0 + ChronoDuration::minutes(k);
        let px = base + k as f64 * 0.1;
        body.push_str(&format!(
            "{},{px},{px},{px},{px},1.0\n",
            t.format("%Y-%m-%dT%H:%M:%S+00:00")
        ));
    }
    std::fs::write(dir.join(format!("{symbol}_1m.csv")), body).unwrap();
}

struct Drill {
    url: String,
    trading: Arc<TradingServiceClient>,
    events: Option<tokio::sync::mpsc::Receiver<ClientEvent>>,
    pos: MockPositionHandle,
    kdir: std::path::PathBuf,
    _server: tokio::task::JoinHandle<()>,
}

impl Drill {
    /// 取出事件流（单所有权：每代 forwarder 独占）。
    fn take_events(&mut self) -> tokio::sync::mpsc::Receiver<ClientEvent> {
        self.events.take().expect("事件流只能取一次")
    }

    /// 新交易客户端（同一 mock URL）：每监督代独立 WS 连接/事件流；
    /// mock 净持仓镜像在服务端状态内跨代保留。
    fn new_client(
        &self,
    ) -> (
        Arc<TradingServiceClient>,
        tokio::sync::mpsc::Receiver<ClientEvent>,
    ) {
        TradingServiceClient::start_arc(
            TradingConfig {
                ws_url: self.url.clone(),
                request_timeout: Duration::from_secs(2),
                submit_enabled: true,
                reconnect_backoff_max_secs: 1,
                strategy_secret: DRILL_SECRET.into(),
            },
            tokio::runtime::Handle::current(),
        )
    }
}

/// 起 mock（K线回放 + 净持仓镜像 + submit_enabled）与共享交易客户端。
async fn start_drill(dir: &std::path::Path) -> Drill {
    start_drill_with_interval(dir, Duration::from_millis(5)).await
}

/// 可调 K 线回放间隔：决策轮按 `rebalance.every_bars`（5 根）推进，间隔太短
/// 会让脚本里的「先开仓、后平仓」跨轮行为不可观测。
async fn start_drill_with_interval(dir: &std::path::Path, bar_interval: Duration) -> Drill {
    start_drill_with(dir, bar_interval, 1, Duration::from_millis(5)).await
}

/// 可注入 mock 回报段数/间隔（P2：venue 侧 PARTIAL→FILLED 分段回报）。
async fn start_drill_with(
    dir: &std::path::Path,
    bar_interval: Duration,
    fill_times: u32,
    fill_interval: Duration,
) -> Drill {
    let kdir = dir.join("klines");
    std::fs::create_dir_all(&kdir).unwrap();
    write_drill_csv(&kdir, "BTCUSDT", 100.0);
    write_drill_csv(&kdir, "ETHUSDT", 50.0);
    let scenario = MockScenario::new(DRILL_SECRET)
        .fill_times(fill_times)
        .fill_interval(fill_interval)
        .symbol_price("BTCUSDT", 100.0)
        .symbol_price("ETHUSDT", 50.0)
        .kline_dir(&kdir, bar_interval, None)
        .0;
    let server = MockServer::bind("127.0.0.1:0", scenario).await.unwrap();
    let pos = server.position_handle();
    let url = format!("ws://{}/ws", server.local_addr().unwrap());
    let server_task = tokio::spawn(server.serve());
    let (trading, events) = TradingServiceClient::start_arc(
        TradingConfig {
            ws_url: url.clone(),
            request_timeout: Duration::from_secs(2),
            submit_enabled: true,
            reconnect_backoff_max_secs: 1,
            strategy_secret: DRILL_SECRET.into(),
        },
        tokio::runtime::Handle::current(),
    );
    Drill {
        url,
        trading,
        events: Some(events),
        pos,
        kdir,
        _server: server_task,
    }
}

fn drill_symbols() -> Vec<String> {
    DRILL_SYMBOLS.iter().map(|s| s.to_string()).collect()
}

/// 起 K线客户端 + 每标的 std mpsc 扇出（与 bootstrap::start_live_strategy_set 同款）。
fn drill_kline_channels(
    kdir: &std::path::Path,
    trading: Arc<TradingServiceClient>,
) -> (Arc<dyn KlineClient>, Vec<std::sync::mpsc::Receiver<Candle>>) {
    let symbols = drill_symbols();
    let mut routes: HashMap<String, (String, String)> = HashMap::new();
    for s in &symbols {
        routes.insert(
            s.clone(),
            ("template_xsec_ls".to_string(), "binance".to_string()),
        );
    }
    let kline: Arc<dyn KlineClient> = TradingServiceKlineClient::start(
        &symbols,
        trading,
        kdir.to_path_buf(),
        routes,
        tokio::runtime::Handle::current(),
    );
    let mut rxs = Vec::new();
    for s in &DRILL_SYMBOLS {
        let mut stream = kline.attach(s, strategy_core::Timeframe::M1);
        let (tx, rx) = std::sync::mpsc::channel::<Candle>();
        let sym = s.to_string();
        tokio::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match stream.rx.recv().await {
                    Ok(strategy_data::client::KlineEvent::Closed(b)) if b.symbol == sym => {
                        if tx.send(b.candle).is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(RecvError::Lagged(_)) => {}
                    Err(RecvError::Closed) => break,
                }
            }
        });
        rxs.push(rx);
    }
    (kline, rxs)
}

fn drill_venue() -> OrderVenueCfg {
    OrderVenueCfg {
        exchange: "binance".into(),
        account: "default".into(),
        leverage: 1,
        order_cash: 200.0,
    }
}

fn drill_spec(state_dir: std::path::PathBuf) -> StrategyLiveSpec {
    drill_spec_cfg(state_dir, strategy_cfg())
}

/// 可注入策略配置的装配说明：S3（组合止损/回落跟踪）、S4（盈亏口径）
/// 需要与默认演练配置不同的 `portfolio_exit`。
fn drill_spec_cfg(state_dir: std::path::PathBuf, config: StrategyConfig) -> StrategyLiveSpec {
    StrategyLiveSpec {
        strategy_id: "template_xsec_ls:default".into(),
        strategy_type: "template_xsec_ls".into(),
        config,
        instruments: vec![symbol_spec("BTCUSDT"), symbol_spec("ETHUSDT")],
        mode: "live".into(),
        state_dir,
        state_flush_interval_secs: 3600,
        position_check_interval_secs: 3600,
        initial_cash_total: 10_000.0,
        fee_rate: None,
        channel_cap: 256,
    }
}

/// T7 桩：spawn actor + 转发器（order_id 精确路由 + 策略类型广播兜底）。
fn spawn_drill_actor(
    spec: StrategyLiveSpec,
    candle_rxs: Vec<std::sync::mpsc::Receiver<Candle>>,
    store: Arc<Store>,
    trading: Arc<TradingServiceClient>,
    script: Vec<Vec<InstrumentAction>>,
    mut events: tokio::sync::mpsc::Receiver<ClientEvent>,
) -> (
    RuntimeHandle,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let factory_script = script.clone();
    let strategy_factory = Box::new(move || -> Box<dyn CrossSectionalStrategyPort> {
        Box::new(ScriptedStrategy {
            calls: Arc::new(AtomicUsize::new(0)),
            script: factory_script.clone(),
        })
    });
    let venue = drill_venue();
    let trading_f = Arc::clone(&trading);
    let gateway_factory: Option<Box<dyn Fn() -> Box<dyn AsyncOrderGateway> + Send>> =
        Some(Box::new(move || -> Box<dyn AsyncOrderGateway> {
            Box::new(TradingGateway::with_submit(
                Arc::clone(&trading_f),
                "template_xsec_ls".into(),
                venue.clone(),
                true,
            ))
        }));
    let (bind_tx, mut bind_rx) = tokio::sync::mpsc::channel(256);
    let (handle, task) = spawn_strategy_live(StrategyLiveAssembly {
        spec,
        candle_rxs,
        warmup: vec![Vec::new(), Vec::new()],
        store,
        gateway_factory,
        strategy_factory,
        route_tx: Some(bind_tx.clone()),
    });
    drop(bind_tx);
    let router_handle = handle.clone();
    let forwarder = tokio::spawn(async move {
        let mut router = OrderRouter::new(vec![("template_xsec_ls".to_string(), true, router_handle)]);
        loop {
            tokio::select! {
                maybe = events.recv() => {
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
    (handle, task, forwarder)
}

async fn wait_sub_rows<F>(store: &Store, what: &str, pred: F) -> Vec<SubPosition>
where
    F: Fn(&Vec<SubPosition>) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let rows = store.table::<SubPosition>().list();
        if pred(&rows) {
            return rows;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待 {what} 超时，当前子仓: {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

/// 演练内 tracing 初始化（`RUST_LOG` 控制级别；Once 防多测重复）。
fn init_drill_tracing() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    });
}

/// 全链演练：mock 订阅（symbols 数组）→ 回放 1m → 决策出双单（sub_ id）→
/// ack → FILLED → 子仓 Active → 外部改净仓（他人平 ETH）→ Reconcile →
/// drifts 收敛（ETH Closed、BTC 不动）→ 全链无重复 signal_id、无权益帧依赖。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strategy_live_drill_two_symbols_full_loop() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let mut drill = start_drill(dir.path()).await;
    let symbols = drill_symbols();
    // 与 bootstrap 同款：一帧订阅全 universe（mock 支持 symbols 数组）。
    drill
        .trading
        .subscribe_klines("template_xsec_ls", "binance", &symbols)
        .await
        .expect("mock 订阅帧必须被受理");
    let (_kline, candle_rxs) = drill_kline_channels(&drill.kdir, Arc::clone(&drill.trading));
    let state_dir = dir.path().join("state");
    let store = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));

    let btc = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
    let eth = InstrumentId::perpetual(&ExchangeId::new("binance"), "ETHUSDT");
    let (handle, task, forwarder) = spawn_drill_actor(
        drill_spec(state_dir),
        candle_rxs,
        Arc::clone(&store),
        Arc::clone(&drill.trading),
        vec![vec![open_action(&btc), open_action(&eth)]],
        drill.take_events(),
    );

    // 双单：sub_ 确定性 id → ack → FILLED（mock 自动回报）→ 子仓 Active。
    let rows = wait_sub_rows(&store, "双标的 Active", |rows| {
        rows.len() == 2
            && rows
                .iter()
                .filter(|r| r.state == SubPositionState::Active)
                .count()
                == 2
    })
    .await;
    assert!(
        rows.iter().all(|r| r.id.starts_with("sub_")),
        "sub_ id: {rows:?}"
    );
    assert!(rows.iter().any(|r| r.qty > 0.0), "成交量已入账: {rows:?}");

    // 外部直改镜像（模拟他人平掉 ETH）→ Reconcile。本进程刚成交的腿首拍空仓
    // 不算事实（交易所仓位可能滞后于成交 ack），需第二拍复核才收口。
    drill.pos.set_net_position("ETHUSDT", "long", 0.0, 0.0);
    handle.reconcile().await.unwrap();
    let rows = store.table::<SubPosition>().list();
    assert!(
        rows.iter()
            .any(|r| r.instrument.symbol == "ETHUSDT" && r.state == SubPositionState::Active),
        "首拍空仓不得收口: {rows:?}"
    );
    handle.reconcile().await.unwrap();
    let rows = wait_sub_rows(&store, "ETH 外部平仓收敛", |rows| {
        rows.iter()
            .any(|r| r.instrument.symbol == "ETHUSDT" && r.state == SubPositionState::Closed)
    })
    .await;
    assert!(
        rows.iter()
            .any(|r| { r.instrument.symbol == "BTCUSDT" && r.state == SubPositionState::Active }),
        "未受影响的 BTC 子仓必须保留: {rows:?}"
    );
    assert_eq!(rows.len(), 2, "全链无重复 signal_id（每标的至多一行）");

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("Shutdown 后必须收口")
        .expect("监督任务不得 panic");
    forwarder.abort();
}

/// 重启 recover：第一代开仓 Active → Shutdown（已 flush）；第二代新 Store
/// 句柄同目录 + mock 净仓镜像保留 → 装配序（get_positions→recover）保留
/// Active 子仓、不产生新开仓；Reconcile 零 drift（子仓仍 Active）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strategy_live_drill_restart_recover() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let mut drill = start_drill(dir.path()).await;
    let state_dir = dir.path().join("state");
    let btc = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");

    // ── 第一代：开 BTC，等 Active，Shutdown（flush 落盘）。
    {
        let symbols = drill_symbols();
        drill
            .trading
            .subscribe_klines("template_xsec_ls", "binance", &symbols)
            .await
            .unwrap();
        let (_k, rxs) = drill_kline_channels(&drill.kdir, Arc::clone(&drill.trading));
        let store1 = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
        let (h1, t1, f1) = spawn_drill_actor(
            drill_spec(state_dir.clone()),
            rxs,
            Arc::clone(&store1),
            Arc::clone(&drill.trading),
            vec![vec![open_action(&btc)]],
            drill.take_events(),
        );
        wait_sub_rows(&store1, "第一代 Active", |rows| {
            rows.iter().any(|r| r.state == SubPositionState::Active)
        })
        .await;
        h1.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), t1)
            .await
            .expect("第一代收口")
            .expect("第一代不 panic");
        f1.abort();
    }
    // 证明盘载：第一代必须已把子仓行 flush 到日目录。
    let written = std::fs::read_dir(&state_dir)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .any(|d| d.path().join("sub_position.csv").exists());
    assert!(written, "第一代必须 flush 出 sub_position.csv");

    // ── 第二代：新 Store 句柄同目录 + 新 WS 客户端（mock 净仓镜像保留）。
    let (trading2, events2) = drill.new_client();
    let symbols = drill_symbols();
    trading2
        .subscribe_klines("template_xsec_ls", "binance", &symbols)
        .await
        .unwrap();
    let (_k2, rxs2) = drill_kline_channels(&drill.kdir, Arc::clone(&trading2));
    let store2 = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
    let (h2, t2, f2) = spawn_drill_actor(
        drill_spec(state_dir),
        rxs2,
        Arc::clone(&store2),
        trading2,
        Vec::new(), // 新代策略无动作：不得产生新开仓
        events2,
    );
    let rows = wait_sub_rows(&store2, "第二代保留 Active", |rows| {
        rows.iter().any(|r| r.state == SubPositionState::Active)
    })
    .await;
    assert_eq!(
        rows.len(),
        1,
        "重启后账本与 Store 一致（无重复行）: {rows:?}"
    );
    // Reconcile：remote（mock 镜像）与本地一致 → 零 drift（子仓仍 Active）。
    h2.reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let rows = store2.table::<SubPosition>().list();
    assert_eq!(
        rows.iter()
            .filter(|r| r.state != SubPositionState::Closed)
            .count(),
        1,
        "reconcile 零 drift：BTC 子仓保持 Active: {rows:?}"
    );
    h2.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("第二代收口")
        .expect("第二代不 panic");
    f2.abort();
}

// ─── T8：panic 断路器实盘语义 ─────────────────────────────────────────

/// 首轮开仓、次轮 panic 的脚本策略（之后恒 Hold）。
struct OpenThenPanicStrategy {
    calls: Arc<AtomicUsize>,
    open: InstrumentAction,
}

impl CrossSectionalStrategyPort for OpenThenPanicStrategy {
    fn strategy_type(&self) -> &str {
        "open_then_panic"
    }

    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[Timeframe],
        _snapshots: &[InstrumentSnapshot],
        _portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        match n {
            1 => vec![self.open.clone()],
            2 => panic!("T8：注入策略 panic（验证断路器暂停语义）"),
            _ => Vec::new(),
        }
    }
}

fn t8_spec(state_dir: std::path::PathBuf, instruments: Vec<SymbolSpec>) -> StrategyLiveSpec {
    StrategyLiveSpec {
        strategy_id: "template_xsec_ls:default".into(),
        strategy_type: "template_xsec_ls".into(),
        config: strategy_cfg(),
        instruments,
        mode: "live".into(),
        state_dir,
        state_flush_interval_secs: 3600,
        position_check_interval_secs: 3600,
        initial_cash_total: 5_000.0,
        fee_rate: None,
        channel_cap: 64,
    }
}

#[allow(clippy::type_complexity)]
fn spawn_t8_actor(
    stub: Arc<StubState>,
    spec: StrategyLiveSpec,
    rxs: Vec<std::sync::mpsc::Receiver<Candle>>,
    store: Arc<Store>,
    strategy_factory: Box<dyn Fn() -> Box<dyn CrossSectionalStrategyPort> + Send>,
) -> (RuntimeHandle, tokio::task::JoinHandle<()>) {
    let warmup = vec![Vec::new(); rxs.len()];
    let stub_f = Arc::clone(&stub);
    let gateway_factory: Option<Box<dyn Fn() -> Box<dyn AsyncOrderGateway> + Send>> =
        Some(Box::new(move || -> Box<dyn AsyncOrderGateway> {
            Box::new(StubGateway {
                state: Arc::clone(&stub_f),
            })
        }));
    spawn_strategy_live(StrategyLiveAssembly {
        spec,
        candle_rxs: rxs,
        warmup,
        store,
        gateway_factory,
        strategy_factory,
        route_tx: None,
    })
}

fn t8_bar(t0: chrono::DateTime<Utc>, minute: i64, close: f64) -> Candle {
    candle(t0 + ChronoDuration::minutes(minute), close)
}

/// 策略 panic → 组 Paused：本代后续轮次不再产生提交；actor 进程内仍存活
/// （事件泵继续收口，端口数据面持续记账）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strategy_panic_pauses_group_until_process_restart() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let store = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
    let inst = perpetual_btc();
    let stub = Arc::new(StubState::default());
    let (tx, rx) = std::sync::mpsc::channel::<Candle>();
    let open = open_action(&inst);
    let strategy_factory = Box::new(move || -> Box<dyn CrossSectionalStrategyPort> {
        Box::new(OpenThenPanicStrategy {
            calls: Arc::new(AtomicUsize::new(0)),
            open: open.clone(),
        })
    });
    let (handle, task) = spawn_t8_actor(
        Arc::clone(&stub),
        t8_spec(state_dir, vec![symbol_spec("BTCUSDT")]),
        vec![rx],
        Arc::clone(&store),
        strategy_factory,
    );
    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
    // t0/t1 → 决第 1 轮：开仓提交；t2 → 决第 2 轮：策略 panic → Paused。
    tx.send(t8_bar(t0, 0, 100.0)).unwrap();
    tx.send(t8_bar(t0, 1, 100.0)).unwrap();
    let sub = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(s) = store.table::<SubPosition>().list().into_iter().next() {
                break s;
            }
            assert!(tokio::time::Instant::now() < deadline, "开仓子仓未出现");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    assert_eq!(stub.attempts.lock().unwrap().len(), 1, "第 1 轮提交一次");
    assert_eq!(sub.state, SubPositionState::OpenRequested, "等待回报中");
    tx.send(t8_bar(t0, 2, 100.0)).unwrap(); // 触发第 2 轮（panic）
    tokio::time::sleep(Duration::from_millis(200)).await;
    // 后续多轮：不得再产生任何提交（Paused 门控策略调用与派发）。
    for m in 3..8 {
        tx.send(t8_bar(t0, m, 100.0)).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        stub.attempts.lock().unwrap().len(),
        1,
        "Paused 后禁止新提交"
    );
    // 进程内活跃性：actor 仍处理轮次（组合账本每轮 upsert）并可优雅关停。
    drop(tx);
    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("panic 后 actor 仍须可收口（隔离在组级，不杀进程）")
        .expect("监督任务不得 panic");
}

/// panic 后进程重启（新监督代 + after_panic=false 的正常启动路径）：
/// Store 行 + 券商镜像一致 → 账本恢复 Running，可继续开新仓。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_restart_after_panic_recovers_ledger_to_running() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let inst = perpetual_btc();
    let eth = InstrumentId::perpetual(&ExchangeId::new("binance"), "ETHUSDT");
    let stub = Arc::new(StubState::default());
    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();

    // ── 第一代：开仓成交 → Active → 次轮 panic → Paused → Shutdown（flush）。
    {
        let store1 = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
        let (tx1, rx1) = std::sync::mpsc::channel::<Candle>();
        let open = open_action(&inst);
        let strategy_factory = Box::new(move || -> Box<dyn CrossSectionalStrategyPort> {
            Box::new(OpenThenPanicStrategy {
                calls: Arc::new(AtomicUsize::new(0)),
                open: open.clone(),
            })
        });
        let (h1, t1) = spawn_t8_actor(
            Arc::clone(&stub),
            t8_spec(state_dir.clone(), vec![symbol_spec("BTCUSDT")]),
            vec![rx1],
            Arc::clone(&store1),
            strategy_factory,
        );
        tx1.send(t8_bar(t0, 0, 100.0)).unwrap();
        tx1.send(t8_bar(t0, 1, 100.0)).unwrap();
        let sub = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(s) = store1.table::<SubPosition>().list().into_iter().next() {
                    break s;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "第一代开仓子仓未出现"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        h1.order_update(
            "ord_1".into(),
            None,
            CoreOrderState::Filled,
            1.0,
            100.0,
            Some("1".into()),
            Some(sub.id.clone()),
            None,
            None,
            Some(0.1),
            t0 + ChronoDuration::minutes(2),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        tx1.send(t8_bar(t0, 2, 100.0)).unwrap(); // 填充落账轮之后（t3 触发 panic 轮）
        tx1.send(t8_bar(t0, 3, 100.0)).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let rows = store1.table::<SubPosition>().list();
        assert_eq!(
            rows.iter()
                .filter(|r| r.state == SubPositionState::Active)
                .count(),
            1,
            "第一代账本：BTC Active: {rows:?}"
        );
        drop(tx1);
        h1.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), t1)
            .await
            .expect("第一代收口")
            .expect("第一代不 panic");
    }
    // 券商镜像保留持仓（重启后 get_positions 采纳路径的依据）。
    stub.remote_positions
        .lock()
        .unwrap()
        .push(strategy_signals::gateway::RemotePosition {
            instrument: "BTCUSDT".into(),
            side: "long".into(),
            quantity: 1.0,
            avg_price: 100.0,
            deleted: false,
        });
    stub.attempts.lock().unwrap().clear();
    assert!(
        std::fs::read_dir(&state_dir)
            .ok()
            .into_iter()
            .flatten()
            .flatten()
            .any(|d| d.path().join("sub_position.csv").exists()),
        "第一代必须 flush 出 sub_position.csv"
    );

    // ── 第二代（进程重启等价：新 Store 句柄 + 新策略实例）→ Running 可交易。
    let store2 = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
    let (tx_btc, rx_btc) = std::sync::mpsc::channel::<Candle>();
    let (tx_eth, rx_eth) = std::sync::mpsc::channel::<Candle>();
    let open_eth = open_action(&eth);
    let strategy_factory = Box::new(move || -> Box<dyn CrossSectionalStrategyPort> {
        Box::new(ScriptedStrategy {
            calls: Arc::new(AtomicUsize::new(0)),
            script: vec![vec![open_eth.clone()]],
        })
    });
    let (h2, t2) = spawn_t8_actor(
        Arc::clone(&stub),
        t8_spec(
            state_dir,
            vec![symbol_spec("BTCUSDT"), symbol_spec("ETHUSDT")],
        ),
        vec![rx_btc, rx_eth],
        Arc::clone(&store2),
        strategy_factory,
    );
    // 双 slot 都喂 bar（否则 ETH slot stale 会使 quorum 不成立、决策轮不触发）。
    tx_btc.send(t8_bar(t0, 10, 100.0)).unwrap();
    tx_btc.send(t8_bar(t0, 11, 100.0)).unwrap();
    tx_eth.send(t8_bar(t0, 10, 50.0)).unwrap();
    tx_eth.send(t8_bar(t0, 11, 50.0)).unwrap();
    // 恢复账本：BTC 仍 Active（remote 一致），未因重启误杀。
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let rows = store2.table::<SubPosition>().list();
            if rows
                .iter()
                .any(|r| r.instrument.symbol == "BTCUSDT" && r.state == SubPositionState::Active)
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "BTC Active 恢复超时: {rows:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    // Running 证明：新策略动作被派发（出现提交尝试；Paused 时会被门控拒绝）。
    {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if stub
                .attempts
                .lock()
                .unwrap()
                .iter()
                .any(|sid| !sid.is_empty())
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "重启后无任何提交尝试（组未恢复 Running？）"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    drop(tx_btc);
    drop(tx_eth);
    h2.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("第二代收口")
        .expect("第二代不 panic");
}

// ── PR-6 Task 2：多组同进程装配（共享客户端/Store/路由）──

fn multi_group_paper_spec(id: &str, symbols: &[&str]) -> StrategySpec {
    let mut config = strategy_cfg();
    config.strategy_id = Some(id.to_string());
    config.universe = symbols
        .iter()
        .map(|s| UniverseEntry::Symbol((*s).to_string()))
        .collect();
    StrategySpec {
        strategy_type: "template_xsec_ls".into(),
        strategy_id: id.into(),
        config,
        instruments: symbols.iter().map(|s| symbol_spec(s)).collect(),
        warmup_days: 0,
        run_mode: "paper_trading".into(),
        services: None,
        state_flush_interval_secs: 3600,
        state_prune_terminal: false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_group_paper_shares_store_and_fans_out_overlapping_symbol() {
    let dir = tempfile::tempdir().unwrap();
    let drill = start_drill(dir.path()).await;
    let services = ServicesConfig {
        kline: None,
        trading: TradingServiceCfg {
            ws_url: drill.url.clone(),
            request_timeout_ms: 2_000,
            submit_enabled: false,
            reconnect_backoff_max_secs: 1,
            strategy_secret: DRILL_SECRET.into(),
            exchange: "binance".into(),
            account: "default".into(),
        },
    };
    // A: BTC+ETH；B: BTC+DOGE（DOGE 无 CSV/无回放——一组缺历史不得阻断另一组）。
    let spec_a = multi_group_paper_spec("grp_a", &["BTCUSDT", "ETHUSDT"]);
    let spec_b = multi_group_paper_spec("grp_b", &["BTCUSDT", "DOGEUSDT"]);
    let set = start_live_strategy_set(
        vec![spec_a, spec_b],
        services,
        dir.path().join("out"),
        drill.kdir.clone(),
        "paper_trading".into(),
        Duration::from_secs(3600),
        false,
    )
    .await
    .expect("multi-group start");

    // mock 回放 40 根 @5ms；给 actor 留 1s 处理，再关停（关停时 flush）。
    tokio::time::sleep(Duration::from_secs(1)).await;
    set.shutdown().await;

    let probe = Store::new(dir.path().join("out").join("state"), Arc::new(SimClock));
    probe.recover::<StrategyPortfolio>().unwrap();
    let rows = probe.table::<StrategyPortfolio>().list();
    let ids: Vec<&str> = rows.iter().map(|r| r.strategy_id.as_str()).collect();
    assert!(
        ids.contains(&"grp_a") && ids.contains(&"grp_b"),
        "两组都要有账本行（缺行说明该组没消费到任何 bar）: {rows:?}"
    );
    assert!(
        probe.table::<SubPosition>().list().is_empty(),
        "空指标配置不应开仓"
    );
}

// ─── PR-6 T3：多组同进程隔离验收 ─────────────────────────────────────

/// 多组 live 装配助手：每组独立 actor（共享 Store / 单个交易事件流），
/// 单 OrderRouter 管多组（order_id 精确优先，同策略类型广播兜底）。
#[allow(clippy::type_complexity)]
fn spawn_multi_group_actors(
    groups: Vec<(
        StrategyLiveSpec,
        Vec<std::sync::mpsc::Receiver<Candle>>,
        Vec<Vec<InstrumentAction>>,
    )>,
    store: Arc<Store>,
    trading: Arc<TradingServiceClient>,
    mut events: tokio::sync::mpsc::Receiver<ClientEvent>,
) -> (
    Vec<RuntimeHandle>,
    Vec<tokio::task::JoinHandle<()>>,
    tokio::task::JoinHandle<()>,
) {
    let (bind_tx, mut bind_rx) = tokio::sync::mpsc::channel(256);
    let mut handles = Vec::new();
    let mut tasks = Vec::new();
    for (spec, candle_rxs, script) in groups {
        let strategy_factory = Box::new(move || -> Box<dyn CrossSectionalStrategyPort> {
            Box::new(ScriptedStrategy {
                calls: Arc::new(AtomicUsize::new(0)),
                script: script.clone(),
            })
        });
        let venue = drill_venue();
        let trading_f = Arc::clone(&trading);
        let gateway_factory: Option<Box<dyn Fn() -> Box<dyn AsyncOrderGateway> + Send>> =
            Some(Box::new(move || -> Box<dyn AsyncOrderGateway> {
                Box::new(TradingGateway::with_submit(
                    Arc::clone(&trading_f),
                    "template_xsec_ls".into(),
                    venue.clone(),
                    true,
                ))
            }));
        let warmup = vec![Vec::new(); candle_rxs.len()];
        let (handle, task) = spawn_strategy_live(StrategyLiveAssembly {
            spec,
            candle_rxs,
            warmup,
            store: Arc::clone(&store),
            gateway_factory,
            strategy_factory,
            route_tx: Some(bind_tx.clone()),
        });
        handles.push(handle);
        tasks.push(task);
    }
    drop(bind_tx);
    let routes: Vec<(String, bool, RuntimeHandle)> = handles
        .iter()
        .map(|h| ("template_xsec_ls".to_string(), true, h.clone()))
        .collect();
    let forwarder = tokio::spawn(async move {
        let mut router = OrderRouter::new(routes);
        loop {
            tokio::select! {
                maybe = events.recv() => {
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
    (handles, tasks, forwarder)
}

fn multi_group_spec(state_dir: std::path::PathBuf, id: &str, symbol: &str) -> StrategyLiveSpec {
    let mut spec = drill_spec(state_dir.join(id));
    spec.strategy_id = id.into();
    spec.instruments = vec![symbol_spec(symbol)];
    spec
}

/// 共享 Store + 单 OrderRouter：两组的 SubPosition/StrategyPortfolio 行
/// 必须按 strategy_id 作用域隔离，跨组广播回报不得串账。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_group_live_shared_store_keeps_scopes_isolated() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let mut drill = start_drill(dir.path()).await;
    let symbols = drill_symbols();
    drill
        .trading
        .subscribe_klines("template_xsec_ls", "binance", &symbols)
        .await
        .expect("mock 订阅帧必须被受理");
    let (_kline, candle_rxs) = drill_kline_channels(&drill.kdir, Arc::clone(&drill.trading));
    let state_dir = dir.path().join("state");
    let store = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
    let btc = perpetual_btc();
    let eth = InstrumentId::perpetual(&ExchangeId::new("binance"), "ETHUSDT");

    let mut rxs = candle_rxs.into_iter();
    let groups = vec![
        (
            multi_group_spec(state_dir.clone(), "grp_a", "BTCUSDT"),
            vec![rxs.next().unwrap()],
            vec![vec![open_action(&btc)]],
        ),
        (
            multi_group_spec(state_dir.clone(), "grp_b", "ETHUSDT"),
            vec![rxs.next().unwrap()],
            vec![vec![open_action(&eth)]],
        ),
    ];
    let (handles, tasks, forwarder) = spawn_multi_group_actors(
        groups,
        Arc::clone(&store),
        Arc::clone(&drill.trading),
        drill.take_events(),
    );

    let rows = wait_sub_rows(&store, "两组各自 Active", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Active)
    })
    .await;
    let a_rows = store.table::<SubPosition>().by_scope("grp_a");
    let b_rows = store.table::<SubPosition>().by_scope("grp_b");
    assert_eq!(a_rows.len(), 1, "A scope 只应有 BTC 一条: {a_rows:?}");
    assert_eq!(a_rows[0].instrument.symbol, "BTCUSDT");
    assert_eq!(a_rows[0].strategy_id, "grp_a");
    assert_eq!(b_rows.len(), 1, "B scope 只应有 ETH 一条: {b_rows:?}");
    assert_eq!(b_rows[0].instrument.symbol, "ETHUSDT");
    assert_eq!(b_rows[0].strategy_id, "grp_b");
    assert!(
        !b_rows.iter().any(|r| r.instrument.symbol == "BTCUSDT"),
        "B scope 不得持有 A 的 BTC 行: {b_rows:?}"
    );
    assert!(
        !a_rows.iter().any(|r| r.instrument.symbol == "ETHUSDT"),
        "A scope 不得持有 B 的 ETH 行: {a_rows:?}"
    );
    // 两组各自一行组合账本，无合并/覆盖。
    let portfolios = store.table::<StrategyPortfolio>().list();
    let ids: Vec<&str> = portfolios.iter().map(|p| p.strategy_id.as_str()).collect();
    assert!(
        ids.contains(&"grp_a") && ids.contains(&"grp_b"),
        "组合账本必须每策略一行: {rows:?}"
    );
    assert_eq!(rows.len(), 2, "每标的至多一行（跨组不得重复）");

    for h in &handles {
        h.shutdown().await.unwrap();
    }
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("多组监督任务收口超时")
            .expect("监督任务不得 panic");
    }
    forwarder.abort();
}

/// A 组策略 panic → 仅 A 组暂停；B 组继续提交，且两者均可优雅收口。
/// B 组用多标的逐轮新开仓（StubGateway 无成交回报，Adjust/重复 Open 会被
/// 状态机拒）制造可观测的持续提交面。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panic_in_one_group_does_not_stop_sibling_group() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    let store = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
    let btc = perpetual_btc();
    let symbols_b = [
        "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "BNBUSDT", "LTCUSDT", "TRXUSDT",
    ];

    let stub_a = Arc::new(StubState::default());
    let stub_b = Arc::new(StubState::default());
    let (tx_a, rx_a) = std::sync::mpsc::channel::<Candle>();
    let mut txs_b = Vec::new();
    let mut rxs_b = Vec::new();
    for _ in &symbols_b {
        let (tx, rx) = std::sync::mpsc::channel::<Candle>();
        txs_b.push(tx);
        rxs_b.push(rx);
    }

    let open_a = open_action(&btc);
    let strategy_factory_a = Box::new(move || -> Box<dyn CrossSectionalStrategyPort> {
        Box::new(OpenThenPanicStrategy {
            calls: Arc::new(AtomicUsize::new(0)),
            open: open_a.clone(),
        })
    });
    let script_b: Vec<Vec<InstrumentAction>> = symbols_b
        .iter()
        .map(|s| {
            vec![open_action(&InstrumentId::perpetual(
                &ExchangeId::new("binance"),
                s,
            ))]
        })
        .collect();
    let strategy_factory_b = Box::new(move || -> Box<dyn CrossSectionalStrategyPort> {
        Box::new(ScriptedStrategy {
            calls: Arc::new(AtomicUsize::new(0)),
            script: script_b.clone(),
        })
    });

    let mut spec_a = t8_spec(state_dir.clone(), vec![symbol_spec("BTCUSDT")]);
    spec_a.strategy_id = "grp_a".into();
    let mut spec_b = t8_spec(
        state_dir.clone(),
        symbols_b.iter().map(|s| symbol_spec(s)).collect(),
    );
    spec_b.strategy_id = "grp_b".into();
    let (h_a, task_a) = spawn_t8_actor(
        Arc::clone(&stub_a),
        spec_a,
        vec![rx_a],
        Arc::clone(&store),
        strategy_factory_a,
    );
    let (h_b, task_b) = spawn_t8_actor(
        Arc::clone(&stub_b),
        spec_b,
        rxs_b,
        Arc::clone(&store),
        strategy_factory_b,
    );

    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
    // 轮次在下一根异刻 bar 到达时 flush：先喂 t0/t1 建立首轮。
    for minute in 0..=1 {
        tx_a.send(t8_bar(t0, minute, 100.0)).unwrap();
        for tx in &txs_b {
            tx.send(t8_bar(t0, minute, 100.0)).unwrap();
        }
    }
    // 等 A 开仓行出现、B 首轮提交。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while store.table::<SubPosition>().list().is_empty()
        || stub_b.attempts.lock().unwrap().is_empty()
    {
        assert!(
            tokio::time::Instant::now() < deadline,
            "首轮提交未出现：A attempts={:?}, B attempts={:?}",
            stub_a.attempts.lock().unwrap(),
            stub_b.attempts.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(stub_a.attempts.lock().unwrap().len(), 1, "A 首轮开仓一次");
    assert_eq!(stub_b.attempts.lock().unwrap().len(), 1, "B 首轮开仓一次");

    // 第 2 轮：A panic；B 开下一个标的，提交继续。
    tx_a.send(t8_bar(t0, 2, 100.0)).unwrap();
    for tx in &txs_b {
        tx.send(t8_bar(t0, 2, 100.0)).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let b_before = stub_b.attempts.lock().unwrap().len();
    assert!(
        b_before >= 2,
        "兄弟组 panic 当轮 B 仍须提交，实际 {b_before}"
    );
    assert_eq!(
        stub_a.attempts.lock().unwrap().len(),
        1,
        "A Paused 后不得再提交"
    );

    // panic 后继续推 5 轮：A 冻结，B 持续增长。
    for minute in 3..=7 {
        tx_a.send(t8_bar(t0, minute, 100.0)).unwrap();
        for tx in &txs_b {
            tx.send(t8_bar(t0, minute, 100.0)).unwrap();
        }
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let a_after = stub_a.attempts.lock().unwrap().len();
    let b_after = stub_b.attempts.lock().unwrap().len();
    assert_eq!(a_after, 1, "A 组 Paused 后提交数固定为 1");
    assert!(
        b_after > b_before,
        "B 组不得被兄弟组 panic 波及：{b_before} -> {b_after}"
    );

    drop(tx_a);
    h_a.shutdown().await.unwrap();
    h_b.shutdown().await.unwrap();
    for task in [task_a, task_b] {
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("panic 隔离下两组均须在 10s 内收口")
            .expect("监督任务不得 panic");
    }
}

/// 启动期 warmup 必须把远端补齐结果写回本地 CSV：重启后无需再次全量拉取，
/// 且盘载尾部与远端最新一致（PR-5 横截面 warmup 落盘回归）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_warmup_backfills_and_persists_recent_bars() {
    let dir = tempfile::tempdir().unwrap();
    let drill = start_drill(dir.path()).await;
    let services = ServicesConfig {
        kline: None,
        trading: TradingServiceCfg {
            ws_url: drill.url.clone(),
            request_timeout_ms: 2_000,
            submit_enabled: false,
            reconnect_backoff_max_secs: 1,
            strategy_secret: DRILL_SECRET.into(),
            exchange: "binance".into(),
            account: "default".into(),
        },
    };
    let spec = multi_group_paper_spec("warmup_persist", &["BTCUSDT"]);
    let set = start_live_strategy_set(
        vec![spec],
        services,
        dir.path().join("out"),
        drill.kdir.clone(),
        "paper_trading".into(),
        Duration::from_secs(3600),
        false,
    )
    .await
    .expect("start");

    // warmup 在 actor 启动前完成：落盘文件应已存在且覆盖 mock 的 40 根。
    let path = drill
        .kdir
        .join("1m/binance/crypto_perpetual/BTCUSDT_1m.csv");
    let rows = csv_store::load_csv(&path).expect("warmup 必须把回填结果写回 CSV");
    assert_eq!(rows.len(), 40, "mock 的 40 根应全部持久化");
    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
    assert_eq!(
        rows.last().unwrap().open_time,
        t0 + ChronoDuration::minutes(39),
        "盘载尾部应到 mock 最新一根"
    );

    set.shutdown().await;
}

// ─── S1/S2：系统级全链验收（开→持→平→落盘→同日重启→跨日裁剪）──────────

/// 可推进的模拟时钟：S2 要在同一天重启、再跨到次日验证按日裁剪。
struct AtomicSimClock(AtomicI64);
impl AtomicSimClock {
    fn set(&self, ms: i64) {
        self.0.store(ms, Ordering::SeqCst);
    }
}
impl Clock for AtomicSimClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn day_key(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap()
        .format("%Y%m%d")
        .to_string()
}

/// 持续逐根喂 bar 直到 `stop` 置位：决策轮每根 bar 一次，而成交要在
/// 下一轮轮首 `sync_slot_fills` 才落 SubPosition；「开仓提交 / 落账 /
/// 平仓」分属不同轮，一次性灌入会让中间态根本来不及可观测。
fn spawn_bar_feeder(
    btc_tx: std::sync::mpsc::Sender<Candle>,
    eth_tx: std::sync::mpsc::Sender<Candle>,
    t0: chrono::DateTime<Utc>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut m = 0i64;
        while !stop.load(Ordering::SeqCst) {
            let t = t0 + ChronoDuration::minutes(m);
            if btc_tx.send(candle(t, 100.0)).is_err() || eth_tx.send(candle(t, 50.0)).is_err() {
                break;
            }
            m += 1;
            // 决策轮每根 bar 推进一次；节拍太密会让 Active 窗口短于轮询间隔。
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

async fn wait_master_closed(store: &Store) -> MasterPosition {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(m) = store
            .table::<MasterPosition>()
            .list()
            .into_iter()
            .find(|m| m.state == MasterPositionState::Closed)
        {
            return m;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待总仓 Closed 超时，当前 {:?}",
            store.table::<MasterPosition>().list()
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

fn day_csv(state_dir: &std::path::Path, day: &str, model: &str) -> String {
    let path = state_dir.join(day).join(format!("{model}.csv"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("缺少当日快照 {}: {e}", path.display()))
}

/// S1：K线 → 决策开仓 → ack/fill → 双标的持仓 → 策略主动全平 → 总仓收口
/// → 当日快照落盘（当日终态保留）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_chain_open_hold_close_and_persist() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let mut drill = start_drill_with_interval(dir.path(), Duration::from_millis(100)).await;
    // 订阅保持与真实启动一致（mock 的连接/策略名下路由依赖它）。
    let symbols = drill_symbols();
    drill
        .trading
        .subscribe_klines("template_xsec_ls", "binance", &symbols)
        .await
        .expect("mock 订阅帧必须被受理");
    // 手动喂 bar：mock 回放节拍不可控，「开→平」两轮可能在首个等待窗口内
    // 直接走完；K 线接入本身已由其余 drill 用例覆盖，这里只固定事件顺序。
    let (btc_tx, btc_rx) = std::sync::mpsc::channel();
    let (eth_tx, eth_rx) = std::sync::mpsc::channel();
    let candle_rxs = vec![btc_rx, eth_rx];
    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();

    let state_dir = dir.path().join("state");
    let store = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
    let btc = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
    let eth = InstrumentId::perpetual(&ExchangeId::new("binance"), "ETHUSDT");
    let (handle, task, forwarder) = spawn_drill_actor(
        drill_spec(state_dir.clone()),
        candle_rxs,
        Arc::clone(&store),
        Arc::clone(&drill.trading),
        vec![
            vec![open_action(&btc), open_action(&eth)],
            // 决策轮之间要留一轮空动作：第 2 轮才有 sync_slot_fills 把
            // OpenRequested 置 Active，紧挨着平仓会观测不到持仓态。
            Vec::new(),
            vec![close_action(&btc), close_action(&eth)],
        ],
        drill.take_events(),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let feeder = spawn_bar_feeder(btc_tx, eth_tx, t0, Arc::clone(&stop));
    let rows = wait_sub_rows(&store, "双标的 Active", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Active)
    })
    .await;
    assert!(
        rows.iter().all(|r| r.qty > 0.0 && r.id.starts_with("sub_")),
        "成交数量与确定性 sub_ id 已入账: {rows:?}"
    );
    assert_ne!(rows[0].id, rows[1].id, "不同标的不得复用 signal_id");

    // 第 3 轮决策：策略主动全平 → 子仓 Closed → 总仓收口
    let rows = wait_sub_rows(&store, "双标的 Closed", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Closed)
    })
    .await;
    let master = wait_master_closed(&store).await;
    stop.store(true, Ordering::SeqCst);
    feeder.await.unwrap();
    assert!(
        rows.iter().all(|r| r.master_id == master.master_id),
        "子仓归属总仓不一致: {rows:?} vs {}",
        master.master_id
    );
    let portfolios = store.table::<StrategyPortfolio>().list();
    assert_eq!(portfolios.len(), 1, "组合行唯一");
    assert!(portfolios[0].nav > 0.0, "净值已推进: {}", portfolios[0].nav);
    assert!(
        portfolios[0].active_master_id.is_none(),
        "全平后活动总仓指针必须清空"
    );

    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("Shutdown 后必须收口")
        .expect("监督任务不得 panic");
    forwarder.abort();

    // 当日快照：当日终态保留（审计），活行同样在（此处已全平，只剩终态）。
    let day = "20260105";
    let sub_csv = day_csv(&state_dir, day, "sub_position");
    assert_eq!(sub_csv.lines().count(), 3, "表头 + 两条当日终态");
    assert!(sub_csv.contains(&rows[0].id) && sub_csv.contains(&rows[1].id));
    assert!(sub_csv.contains("closed"), "必须带 closed 状态: {sub_csv}");
    let master_csv = day_csv(&state_dir, day, "master_position");
    assert!(master_csv.contains(&master.master_id), "总仓行未落盘");
    assert!(
        master_csv.contains("closed"),
        "总仓收口未落盘: {master_csv}"
    );
    assert!(
        state_dir.join(day).join("strategy_portfolio.csv").exists(),
        "组合行必须落当日快照"
    );
}

/// S2：同日重启恢复「活仓 + 当日终态」；跨日后上一日终态从内存裁掉、
/// 次日快照不携带，旧日文件仍保留审计。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_chain_restart_same_day_and_cross_day() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    // 实盘成交时间戳来自 broker 事件（墙钟），裁剪归属日以 closed_at 判定；
    // 因此「今天」只能锚在墙钟日上，否则「下一日」反而早于成交日、裁不掉。
    let day0_ms = Utc::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let clock = Arc::new(AtomicSimClock(AtomicI64::new(day0_ms)));
    let state_dir = dir.path().join("state");
    let mut drill = start_drill_with_interval(dir.path(), Duration::from_millis(100)).await;
    let symbols = drill_symbols();
    drill
        .trading
        .subscribe_klines("template_xsec_ls", "binance", &symbols)
        .await
        .expect("mock 订阅帧必须被受理");
    // 手动喂 bar：固定「双开 → 平 ETH」两轮事件顺序（K 线接入由其余 drill 用例覆盖）。
    let (btc_tx, btc_rx) = std::sync::mpsc::channel();
    let (eth_tx, eth_rx) = std::sync::mpsc::channel();
    let candle_rxs = vec![btc_rx, eth_rx];
    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
    let store1 = Arc::new(Store::new(state_dir.clone(), clock.clone()));
    let btc = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
    let eth = InstrumentId::perpetual(&ExchangeId::new("binance"), "ETHUSDT");
    let (h1, t1, f1) = spawn_drill_actor(
        drill_spec(state_dir.clone()),
        candle_rxs,
        Arc::clone(&store1),
        Arc::clone(&drill.trading),
        vec![
            vec![open_action(&btc), open_action(&eth)],
            // 留一轮空动作让 sync_slot_fills 落 Active（见 S1 注释）。
            Vec::new(),
            vec![close_action(&eth)],
        ],
        drill.take_events(),
    );
    let stop1 = Arc::new(AtomicBool::new(false));
    let feeder1 = spawn_bar_feeder(btc_tx, eth_tx, t0, Arc::clone(&stop1));

    // 第一代：双开、只平 ETH → 当日文件同时含活仓与当日终态
    wait_sub_rows(&store1, "第一代双 Active", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Active)
    })
    .await;
    let rows = wait_sub_rows(&store1, "第一代 ETH Closed", |rows| {
        rows.iter()
            .any(|r| r.instrument.symbol == "ETHUSDT" && r.state == SubPositionState::Closed)
    })
    .await;
    stop1.store(true, Ordering::SeqCst);
    feeder1.await.unwrap();
    let eth_id = rows
        .iter()
        .find(|r| r.instrument.symbol == "ETHUSDT")
        .expect("ETH 行")
        .id
        .clone();
    h1.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), t1)
        .await
        .expect("第一代收口")
        .expect("第一代不 panic");
    f1.abort();
    let day1 = day_key(day0_ms);
    let day1_sub = day_csv(&state_dir, &day1, "sub_position");
    assert!(day1_sub.contains(&eth_id) && day1_sub.contains("closed"));

    // ── 同日重启：活仓 + 当日终态都应恢复（当天快照语义）
    let (trading2, events2) = drill.new_client();
    // 新代不喂任何 bar：只验证装配期 recover，不得产生新开仓。
    let (_btc_tx2, btc_rx2) = std::sync::mpsc::channel();
    let (_eth_tx2, eth_rx2) = std::sync::mpsc::channel();
    let rxs2 = vec![btc_rx2, eth_rx2];
    let store2 = Arc::new(Store::new(state_dir.clone(), clock.clone()));
    let (h2, t2, f2) = spawn_drill_actor(
        drill_spec(state_dir.clone()),
        rxs2,
        Arc::clone(&store2),
        trading2,
        Vec::new(), // 新代无动作：不得产生新开仓
        events2,
    );
    wait_sub_rows(&store2, "同日重启恢复活仓+当日终态", |rows| {
        rows.len() == 2
            && rows
                .iter()
                .any(|r| r.instrument.symbol == "BTCUSDT" && r.state == SubPositionState::Active)
            && rows
                .iter()
                .any(|r| r.instrument.symbol == "ETHUSDT" && r.state == SubPositionState::Closed)
    })
    .await;
    h2.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("第二代收口")
        .expect("第二代不 panic");
    f2.abort();

    // ── 跨日：上一日终态被裁，次日快照不携带；旧日文件保留
    // 推进两天而非一天，避免测试恰跨 UTC 零点时成交日追平「今天」。
    let day2_ms = day0_ms + 2 * 86_400_000;
    clock.set(day2_ms);
    store2.flush().unwrap();
    let rows = store2.table::<SubPosition>().list();
    assert_eq!(rows.len(), 1, "跨日后只留活仓: {rows:?}");
    assert_eq!(rows[0].instrument.symbol, "BTCUSDT");
    assert_eq!(rows[0].state, SubPositionState::Active);
    let day2 = day_key(day2_ms);
    let day2_sub = day_csv(&state_dir, &day2, "sub_position");
    assert!(!day2_sub.contains(&eth_id), "次日快照不得携带上一日终态");
    assert!(
        std::fs::read_to_string(state_dir.join(&day1).join("sub_position.csv"))
            .unwrap()
            .contains(&eth_id),
        "旧日文件必须保留审计"
    );

    // ── 再次重启：最新日只有活仓
    let store3 = Arc::new(Store::new(state_dir, clock));
    assert_eq!(store3.recover::<SubPosition>().unwrap(), 1);
    let rows = store3.table::<SubPosition>().list();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, SubPositionState::Active);
}

// ─── S3/S4：风控自动平仓 / 盈亏与组合行口径 ─────────────────────────────

/// 按价格表喂 bar：序列用完后按最后一档平盘继续推进，直到 `stop` 置位。
/// 价格表用于驱动组合层风控（止损/回落跟踪以 mark 价算浮盈亏），
/// 与 mock 的成交价（`symbol_price`）互不影响。
fn spawn_priced_feeder(
    btc_tx: std::sync::mpsc::Sender<Candle>,
    eth_tx: std::sync::mpsc::Sender<Candle>,
    t0: chrono::DateTime<Utc>,
    prices: Vec<(f64, f64)>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut m = 0i64;
        let mut last = prices.last().copied().unwrap_or((100.0, 50.0));
        for (btc, eth) in prices {
            if stop.load(Ordering::SeqCst) {
                return;
            }
            if btc_tx
                .send(candle(t0 + ChronoDuration::minutes(m), btc))
                .is_err()
                || eth_tx
                    .send(candle(t0 + ChronoDuration::minutes(m), eth))
                    .is_err()
            {
                return;
            }
            last = (btc, eth);
            m += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        while !stop.load(Ordering::SeqCst) {
            if btc_tx
                .send(candle(t0 + ChronoDuration::minutes(m), last.0))
                .is_err()
                || eth_tx
                    .send(candle(t0 + ChronoDuration::minutes(m), last.1))
                    .is_err()
            {
                return;
            }
            m += 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

/// 清零后的 Paused 断言在 `closed` 之后一拍才落组合行（总仓收口与阶段
/// 切换不在同一轮），这里等到组合行真正进入 Paused。
async fn wait_portfolio_paused(store: &Store) -> StrategyPortfolio {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let rows = store.table::<StrategyPortfolio>().list();
        if let Some(p) = rows.first().filter(|p| p.phase == PortfolioPhase::Paused) {
            return p.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待组合进入 Paused 超时，当前 {:?}",
            rows
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

/// S3/S4 共用装配：mock（下单回报）+ 双标的 actor + 价格表喂 bar。
struct FullChain {
    drill: Drill,
    handle: RuntimeHandle,
    task: tokio::task::JoinHandle<()>,
    forwarder: tokio::task::JoinHandle<()>,
    store: Arc<Store>,
    state_dir: std::path::PathBuf,
    stop: Arc<AtomicBool>,
    feeder: tokio::task::JoinHandle<()>,
}

impl FullChain {
    /// 停喂 → 等 actor 收口（shutdown 内含 flush）→ 中止回报转发器。
    async fn shutdown(self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.feeder.await;
        self.handle.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .expect("Shutdown 后必须收口")
            .expect("监督任务不得 panic");
        self.forwarder.abort();
        drop(self.drill);
    }
}

async fn spawn_full_chain(
    dir: &std::path::Path,
    config: StrategyConfig,
    script: Vec<Vec<InstrumentAction>>,
    prices: Vec<(f64, f64)>,
) -> FullChain {
    spawn_full_chain_with_fills(dir, config, script, prices, 1, Duration::from_millis(5)).await
}

/// 可注入 mock 分段回报的全链装配（P2 部分成交用例）。
async fn spawn_full_chain_with_fills(
    dir: &std::path::Path,
    config: StrategyConfig,
    script: Vec<Vec<InstrumentAction>>,
    prices: Vec<(f64, f64)>,
    fill_times: u32,
    fill_interval: Duration,
) -> FullChain {
    let mut drill =
        start_drill_with(dir, Duration::from_millis(100), fill_times, fill_interval).await;
    let symbols = drill_symbols();
    drill
        .trading
        .subscribe_klines("template_xsec_ls", "binance", &symbols)
        .await
        .expect("mock 订阅帧必须被受理");
    let (btc_tx, btc_rx) = std::sync::mpsc::channel();
    let (eth_tx, eth_rx) = std::sync::mpsc::channel();
    let state_dir = dir.join("state");
    let store = Arc::new(Store::new(state_dir.clone(), Arc::new(SimClock)));
    let (handle, task, forwarder) = spawn_drill_actor(
        drill_spec_cfg(state_dir.clone(), config),
        vec![btc_rx, eth_rx],
        Arc::clone(&store),
        Arc::clone(&drill.trading),
        script,
        drill.take_events(),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
    let feeder = spawn_priced_feeder(btc_tx, eth_tx, t0, prices, Arc::clone(&stop));
    FullChain {
        drill,
        handle,
        task,
        forwarder,
        store,
        state_dir,
        stop,
        feeder,
    }
}

fn btc() -> InstrumentId {
    InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT")
}

fn eth() -> InstrumentId {
    InstrumentId::perpetual(&ExchangeId::new("binance"), "ETHUSDT")
}

/// S3a：组合级硬止损（`portfolio_exit.stop_loss_pct`）。策略脚本只有一条开仓
/// 动作，价格跌破阈值后全平只能来自 L1 风控；收口后组合进入 Paused。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_chain_portfolio_stop_loss_liquidates_all() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = strategy_cfg();
    cfg.portfolio_exit.stop_loss_pct = Some(2.0);
    let mut prices = vec![(100.0, 50.0); 3];
    // mark 跌 3%（成本名义 400 → 浮亏 12），越过 2% 硬止损线。
    prices.extend([(97.0, 48.5); 3]);
    let chain = spawn_full_chain(
        dir.path(),
        cfg,
        vec![vec![open_action(&btc()), open_action(&eth())]],
        prices,
    )
    .await;
    let store = Arc::clone(&chain.store);
    wait_sub_rows(&store, "止损前双 Active", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Active)
    })
    .await;
    let rows = wait_sub_rows(&store, "硬止损全平", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Closed)
    })
    .await;
    assert!(
        rows.iter().all(|r| r.qty == 0.0 && r.closed_at.is_some()),
        "止损平仓后子仓必须清零并带平仓时间: {rows:?}"
    );
    assert!(
        rows.iter().all(|r| r.realized_pnl == Some(0.0)),
        "mock 成交价静态，硬止损的已实现盈亏为 0: {rows:?}"
    );
    wait_master_closed(&store).await;
    let portfolio = wait_portfolio_paused(&store).await;
    assert!(portfolio.active_master_id.is_none());
    // 只有一条开仓脚本：若风控未生效，子仓会一直 Active 而不是 Closed。
    chain.shutdown().await;
}

/// S3b：组合浮盈回落跟踪（`portfolio_exit.trailing`）。先涨到 +4% 激活，
/// 再回撤到 +1%（低于峰值 16 的 80% 线 12.8）触发全平；策略无平仓动作。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_chain_trailing_giveback_liquidates_all() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = strategy_cfg();
    cfg.portfolio_exit.trailing = Some(TrailingCfg {
        arm_pnl_pct: 2.0,
        giveback_pct: 20.0,
    });
    let mut prices = vec![(100.0, 50.0); 3];
    // 成本名义 400：104/52 时浮盈 16（4%）→ 激活；101/50.5 时浮盈 4 → 触发。
    prices.extend([(104.0, 52.0); 3]);
    prices.extend([(101.0, 50.5); 3]);
    let chain = spawn_full_chain(
        dir.path(),
        cfg,
        vec![vec![open_action(&btc()), open_action(&eth())]],
        prices,
    )
    .await;
    let store = Arc::clone(&chain.store);
    wait_sub_rows(&store, "回落前双 Active", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Active)
    })
    .await;
    let rows = wait_sub_rows(&store, "回落跟踪全平", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Closed)
    })
    .await;
    assert!(
        rows.iter().all(|r| r.qty == 0.0 && r.closed_at.is_some()),
        "回落跟踪平仓后子仓必须清零: {rows:?}"
    );
    wait_master_closed(&store).await;
    let portfolio = wait_portfolio_paused(&store).await;
    assert!(portfolio.active_master_id.is_none());
    chain.shutdown().await;
}

/// S4：盈亏与组合行口径（复用 S1 的开→持→平序列）。mock 成交价静态且无
/// 手续费/滑点，因此开平价相同 → 已实现盈亏恰为 0.0，nav 恒等于初始资金；
/// 组合行落当日快照。已知缺口：live 侧无 `equity.csv`/报表产物（app 的
/// `ReportGenerator` 仅由回测路径调用），`cash` 也不在 `StrategyPortfolio`
/// 行内（只存在于执行账户快照），本用例不做伪造断言。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_chain_pnl_and_portfolio_row() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let chain = spawn_full_chain(
        dir.path(),
        strategy_cfg(),
        vec![
            vec![open_action(&btc()), open_action(&eth())],
            Vec::new(),
            vec![close_action(&btc()), close_action(&eth())],
        ],
        vec![(100.0, 50.0); 6],
    )
    .await;
    let store = Arc::clone(&chain.store);
    let rows = wait_sub_rows(&store, "双标的 Active", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Active)
    })
    .await;
    // 成交价 = mock 的 symbol_price（100/50），数量按资金/价格计算后取整手。
    assert!(rows.iter().all(|r| r.entry_price > 0.0 && r.qty > 0.0));
    let portfolio = &store.table::<StrategyPortfolio>().list()[0];
    assert!(
        (portfolio.nav - 10_000.0).abs() < 1e-6,
        "开仓不改变净值（无浮盈）: {}",
        portfolio.nav
    );
    assert_eq!(portfolio.high_watermark, portfolio.nav);
    assert_eq!(portfolio.drawdown, 0.0, "无回撤");

    let rows = wait_sub_rows(&store, "双标的 Closed", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Closed)
    })
    .await;
    // 开平价都由 mock 以同一 symbol_price 成交 → 已实现盈亏精确为 0。
    assert!(
        rows.iter().all(|r| r.realized_pnl == Some(0.0)),
        "静态成交价下已实现盈亏必须恰为 0: {rows:?}"
    );
    assert!(rows.iter().all(|r| r.qty == 0.0));
    wait_master_closed(&store).await;
    let portfolio = &store.table::<StrategyPortfolio>().list()[0];
    assert!(
        (portfolio.nav - 10_000.0).abs() < 1e-6,
        "平仓后净值回到初始资金: {}",
        portfolio.nav
    );
    assert!(portfolio.active_master_id.is_none());
    assert_eq!(portfolio.drawdown, 0.0);

    let state_dir = chain.state_dir.clone();
    chain.shutdown().await;
    let day = "20260105";
    let sub_csv = day_csv(&state_dir, day, "sub_position");
    assert_eq!(sub_csv.lines().count(), 3, "表头 + 两条当日终态");
    assert!(sub_csv.contains("closed"));
    assert!(
        state_dir.join(day).join("strategy_portfolio.csv").exists(),
        "组合行必须落当日快照"
    );
}

// ─── P0 异常面：部分成交 / 拒单 / 重连对账 / 在途重启 ─────────────────

async fn wait_signal<F>(store: &Store, what: &str, pred: F) -> ManagedSignal
where
    F: Fn(&ManagedSignal) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(m) = store
            .table::<ManagedSignal>()
            .list()
            .into_iter()
            .find(|m| pred(m))
        {
            return m;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待信号「{what}」超时（当前 {:?}）",
            store
                .table::<ManagedSignal>()
                .list()
                .iter()
                .map(|m| (m.intent.id.clone(), m.status))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn live_partial_fill_stages_land_in_ledger() {
    let dir = tempfile::tempdir().unwrap();
    let h = spawn_harness(dir.path(), vec![vec![open_action(&perpetual_btc())]], None);
    h.send_bar(0, 100.0);
    h.send_bar(1, 100.0);
    let sub = h.wait_sub(SubPositionState::OpenRequested).await;
    let sid = sub.id.clone();

    // PARTIAL 只落信号审计价/量，子仓不得提前 Active（成交仍不完整）。
    h.handle
        .order_update(
            "ord_1".into(),
            None,
            CoreOrderState::PartiallyFilled,
            0.4,
            100.0,
            Some("0.4".into()),
            Some(sid.clone()),
            None,
            None,
            Some(0.04),
            h.t0 + ChronoDuration::minutes(2),
        )
        .await
        .unwrap();
    let m = wait_signal(&h.store, "首次部分成交 0.4", |m| {
        m.intent.id == sid && m.filled_quantity == Some(0.4)
    })
    .await;
    assert_eq!(
        m.status,
        SignalStatus::Accepted,
        "部分成交补 Accepted，不改终态"
    );
    assert_eq!(
        h.store.table::<SubPosition>().get(&sid).unwrap().state,
        SubPositionState::OpenRequested,
        "部分成交不得提前置 Active"
    );

    // 服务端累计值语义：第二次 PARTIAL 覆盖为 0.8，而不是相加成 1.2。
    h.handle
        .order_update(
            "ord_1".into(),
            None,
            CoreOrderState::PartiallyFilled,
            0.8,
            100.5,
            Some("0.8".into()),
            Some(sid.clone()),
            None,
            None,
            Some(0.08),
            h.t0 + ChronoDuration::minutes(3),
        )
        .await
        .unwrap();
    let m = wait_signal(&h.store, "累计部分成交 0.8", |m| {
        m.intent.id == sid && m.filled_quantity == Some(0.8)
    })
    .await;
    assert_eq!(m.status, SignalStatus::Accepted, "仍非终态");
    assert_eq!(
        h.store.table::<SubPosition>().get(&sid).unwrap().state,
        SubPositionState::OpenRequested,
        "累计未达全量前不得 Active"
    );

    // 终态 FILLED → 轮首 sync → Active（只有一行，量按回报入账）。
    h.handle
        .order_update(
            "ord_1".into(),
            None,
            CoreOrderState::Filled,
            1.0,
            100.5,
            Some("1".into()),
            Some(sid.clone()),
            None,
            None,
            Some(0.1),
            h.t0 + ChronoDuration::minutes(4),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    h.send_bar(4, 100.5);
    let sub = h.wait_sub(SubPositionState::Active).await;
    assert_eq!(sub.id, sid, "沿用同一确定性信号 ID");
    assert_eq!(sub.qty, 1.0, "终态回报决定实际持仓量");
    assert_eq!(
        h.store.table::<SubPosition>().list().len(),
        1,
        "不得因分段回报生成多行"
    );
    let m = wait_signal(&h.store, "信号终态 Filled", |m| m.intent.id == sid).await;
    assert_eq!(m.status, SignalStatus::Filled);
    h.shutdown().await;
}

#[tokio::test]
async fn live_open_reject_marks_signal_failed_and_discards_pending_row() {
    let dir = tempfile::tempdir().unwrap();
    let h = spawn_harness_stub(
        dir.path(),
        vec![vec![open_action(&perpetual_btc())]],
        StubState {
            reject_open: std::sync::Mutex::new(Some("交易所拒单：可用保证金不足".into())),
            ..Default::default()
        },
    );
    // 两根 bar 才闭合首根（横截面聚合按下一根 bar 的时标收线）。
    h.send_bar(0, 100.0);
    h.send_bar(1, 100.0);
    let m = wait_signal(&h.store, "开仓拒单落账", |m| {
        m.intent.kind == IntentKind::Open && m.status == SignalStatus::Failed
    })
    .await;
    assert_eq!(
        m.last_failure_kind,
        Some(FailureKind::Rejected),
        "拒单分类必须落账（重连时据此不复活）"
    );
    assert!(
        m.last_error.as_deref().unwrap_or("").contains("保证金"),
        "保留交易所拒单原文：{:?}",
        m.last_error
    );
    assert_eq!(m.next_retry_at, None, "明确拒单不得安排退避重试");

    // 拒单后不可能再产生成交：挂单行必须在下一轮被撤出，
    // 否则会永久停在 OpenRequested 误导对账与后续决策。
    for minute in 1..6 {
        h.send_bar(minute, 100.0);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        h.store.table::<SubPosition>().list().is_empty(),
        "拒单不得留下悬挂子仓行：{:?}",
        h.store.table::<SubPosition>().list()
    );
    assert_eq!(
        h.stub.attempts.lock().unwrap().len(),
        1,
        "交易所明确拒单不得重复提交"
    );
    h.shutdown().await;
}

#[tokio::test]
async fn live_reconcile_keeps_inflight_open_intact() {
    let dir = tempfile::tempdir().unwrap();
    let h = spawn_harness(dir.path(), vec![vec![open_action(&perpetual_btc())]], None);
    h.send_bar(0, 100.0);
    h.send_bar(1, 100.0);
    let sub = h.wait_sub(SubPositionState::OpenRequested).await;
    let sid = sub.id.clone();

    // 断线重连后的对账：远程镜像为空也不能把在途单当成丢失仓位处理。
    h.handle.reconcile().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let row = h
        .store
        .table::<SubPosition>()
        .get(&sid)
        .expect("对账不得删除在途子仓行");
    assert_eq!(
        row.state,
        SubPositionState::OpenRequested,
        "在途单保持等待回报"
    );
    assert_eq!(
        h.stub.attempts.lock().unwrap().len(),
        1,
        "对账不得重复提交在途信号"
    );

    // 回报补到后仍按正常链路 Active。
    h.handle
        .order_update(
            "ord_1".into(),
            None,
            CoreOrderState::Filled,
            1.0,
            100.0,
            Some("1".into()),
            Some(sid.clone()),
            None,
            None,
            Some(0.1),
            h.t0 + ChronoDuration::minutes(2),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    h.send_bar(2, 100.0);
    let sub = h.wait_sub(SubPositionState::Active).await;
    assert_eq!(sub.id, sid);
    assert_eq!(sub.qty, 1.0);
    h.shutdown().await;
}

#[tokio::test]
async fn live_restart_with_inflight_open_queries_to_filled() {
    let dir = tempfile::tempdir().unwrap();
    // gen1：开仓 ack 但无回报 → 持久化 Submitted 信号 + OpenRequested 子仓。
    let h1 = spawn_harness(dir.path(), vec![vec![open_action(&perpetual_btc())]], None);
    h1.send_bar(0, 100.0);
    h1.send_bar(1, 100.0);
    let sub = h1.wait_sub(SubPositionState::OpenRequested).await;
    let m = wait_signal(&h1.store, "等待开仓 ack", |m| {
        m.intent.id == sub.id && m.order_id.is_some()
    })
    .await;
    assert_eq!(m.status, SignalStatus::Submitted, "ack 后等待异步成交回报");
    let order_id = m.order_id.clone().unwrap();
    h1.shutdown().await;

    // gen2：启动恢复必须先查网关快照，已成交直接补账，不得重复下单。
    let h2 = spawn_harness_stub(
        dir.path(),
        Vec::new(),
        StubState {
            query_snapshot: std::sync::Mutex::new(Some(OrderSnapshot {
                order_id: order_id.clone(),
                state: CoreOrderState::Filled,
                filled_size: 1.0,
                avg_price: 100.0,
                exchange_order_id: None,
                filled_quantity_raw: Some("1".into()),
                place: None,
                fee: Some(0.1),
            })),
            ..Default::default()
        },
    );
    // 横截面聚合按下一根 bar 收线：两根才会触发决策轮/成交落账。
    h2.send_bar(1, 100.5);
    h2.send_bar(2, 100.5);
    let sub2 = h2.wait_sub(SubPositionState::Active).await;
    assert_eq!(sub2.id, sub.id, "恢复后沿用确定性信号 ID");
    assert_eq!(sub2.qty, 1.0, "查询快照的成交量必须入账");
    assert!(
        h2.stub.attempts.lock().unwrap().is_empty(),
        "先查后发：快照已成交不得重复下单"
    );
    h2.shutdown().await;
}

// ─── P1：调仓两阶段 / 组合平仓 / 子级止损 / 坏行恢复 / 非平价权益 ─────

fn adjust_action(inst: &InstrumentId, target_qty: f64, reason: &str) -> InstrumentAction {
    InstrumentAction::Adjust {
        instrument: inst.clone(),
        target_qty,
        reason: reason.into(),
    }
}

/// P1a：同一调仓轮「BTC 减仓 + ETH 加仓」走 Phase 1/2——
/// Phase 1 先派减仓腿，Phase 2 加仓等减仓成交确认后补派；
/// 加仓并入既有子仓行（G1 不产生第二行）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_chain_adjust_phase_split_reduces_then_adds_in_place() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = strategy_cfg();
    // 每根 bar 都是调仓窗口，让 Adjust 必然走两阶段裁决。
    cfg.rebalance = Some(RebalanceCfg {
        every_bars: 1,
        top_k: 2,
        bottom_k: 0,
        weight_scheme: WeightScheme::Equal,
    });
    let chain = spawn_full_chain(
        dir.path(),
        cfg,
        vec![
            vec![open_action(&btc()), open_action(&eth())],
            // 空动作轮：成交在下一轮轮首 sync 落合计账。
            Vec::new(),
            vec![
                adjust_action(&btc(), 1.0, "reduce_btc"),
                adjust_action(&eth(), 4.0, "add_eth"),
            ],
        ],
        vec![(100.0, 50.0); 40],
    )
    .await;
    let store = Arc::clone(&chain.store);
    let opened = wait_sub_rows(&store, "调仓前双 Active", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Active)
    })
    .await;
    let opened_btc = opened
        .iter()
        .find(|r| r.instrument.symbol == "BTCUSDT")
        .expect("BTC 行")
        .qty;
    assert!(
        opened_btc > 1.0 + 1e-9,
        "初始仓位须大于减仓目标 1.0，实际 {opened_btc}"
    );

    let rows = wait_sub_rows(&store, "减仓 + 加仓落定", |rows| {
        rows.len() == 2
            && rows.iter().all(|r| r.state == SubPositionState::Active)
            && rows
                .iter()
                .any(|r| r.instrument.symbol == "BTCUSDT" && (r.qty - 1.0).abs() < 1e-9)
            && rows
                .iter()
                .any(|r| r.instrument.symbol == "ETHUSDT" && (r.qty - 4.0).abs() < 1e-9)
    })
    .await;
    assert_eq!(rows.len(), 2, "加仓必须并入既有子仓行（G1）: {rows:?}");
    let btc_row = rows
        .iter()
        .find(|r| r.instrument.symbol == "BTCUSDT")
        .expect("BTC 行");
    assert_eq!(btc_row.qty, 1.0, "减仓腿成交量精确递减");
    assert!(
        btc_row.realized_pnl.is_some(),
        "部分平仓必须留下已实现盈亏审计值: {btc_row:?}"
    );
    let eth_row = rows
        .iter()
        .find(|r| r.instrument.symbol == "ETHUSDT")
        .expect("ETH 行");
    assert_eq!(eth_row.qty, 4.0, "Phase 2 加仓差额已成交");
    assert_eq!(eth_row.realized_pnl, None, "加仓不产生已实现盈亏");
    chain.shutdown().await;
}

/// P1b：组合级 `CloseMaster` 一次平掉全部非 Closed 总仓/子仓。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_chain_close_master_liquidates_every_sub() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let chain = spawn_full_chain(
        dir.path(),
        strategy_cfg(),
        vec![
            vec![open_action(&btc()), open_action(&eth())],
            Vec::new(),
            vec![InstrumentAction::CloseMaster {
                reason: "compound_exit".into(),
                is_stop_loss: false,
            }],
        ],
        vec![(100.0, 50.0); 30],
    )
    .await;
    let store = Arc::clone(&chain.store);
    wait_sub_rows(&store, "组合平仓前双 Active", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Active)
    })
    .await;
    let rows = wait_sub_rows(&store, "CloseMaster 全平", |rows| {
        rows.len() == 2
            && rows
                .iter()
                .all(|r| r.state == SubPositionState::Closed && r.qty == 0.0)
    })
    .await;
    assert!(rows.iter().all(|r| r.closed_at.is_some()), "{rows:?}");
    wait_master_closed(&store).await;
    let p = chain.store.table::<StrategyPortfolio>().list();
    assert_eq!(p.len(), 1);
    assert!(
        p[0].active_master_id.is_none(),
        "组合平仓后不得残留活动总仓"
    );
    chain.shutdown().await;
}

/// P1c：`UpdateStop` 把子级止损价落到子仓/持仓行（实盘交易所止损下单
/// 仍是已知缺口，此处锁定账本口径，防止静默丢失）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_chain_update_stop_persists_sub_level_stop() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let chain = spawn_full_chain(
        dir.path(),
        strategy_cfg(),
        vec![
            vec![open_action(&btc()), open_action(&eth())],
            Vec::new(),
            vec![InstrumentAction::UpdateStop {
                instrument: btc(),
                stop_price: 99.0,
            }],
        ],
        vec![(100.0, 50.0); 30],
    )
    .await;
    let store = Arc::clone(&chain.store);
    wait_sub_rows(&store, "双 Active", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Active)
    })
    .await;
    let rows = wait_sub_rows(&store, "止损价已写子仓行", |rows| {
        rows.iter()
            .any(|r| r.instrument.symbol == "BTCUSDT" && r.stop_price == Some(99.0))
    })
    .await;
    assert!(
        rows.iter().all(|r| r.state == SubPositionState::Active),
        "仅更新止损价不得触发平仓: {rows:?}"
    );
    // 价格不动 → 不应出现任何 Close 行或数量变化。
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rows = store.table::<SubPosition>().list();
    assert_eq!(rows.len(), 2, "不得凭空生成新行: {rows:?}");
    assert!(
        rows.iter()
            .any(|r| r.instrument.symbol == "BTCUSDT" && r.stop_price == Some(99.0)),
        "止损价必须持久保留: {rows:?}"
    );
    chain.shutdown().await;
}

/// P1d：坏 payload 行在重启盘载时跳过（不毒死整表），好行照常恢复。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_corrupt_state_row_skipped_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    // 第一代：开仓 FILLED → Active 后 Shutdown flush。
    let h1 = spawn_harness(dir.path(), vec![vec![open_action(&perpetual_btc())]], None);
    h1.send_bar(0, 100.0);
    h1.send_bar(1, 100.0);
    let sub = h1.wait_sub(SubPositionState::OpenRequested).await;
    h1.handle
        .order_update(
            "ord_1".into(),
            None,
            CoreOrderState::Filled,
            1.0,
            100.0,
            Some("1".into()),
            Some(sub.id.clone()),
            None,
            None,
            Some(0.1),
            h1.t0 + ChronoDuration::minutes(2),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    h1.send_bar(2, 100.0);
    h1.wait_sub(SubPositionState::Active).await;
    h1.shutdown().await;
    let sub_id = sub.id.clone();

    // 追加一行 payload 形状不匹配的坏行（合法 CSV 字段、非法 JSON 结构）。
    let csv = dir.path().join("state/20260105/sub_position.csv");
    let mut txt = std::fs::read_to_string(&csv).unwrap();
    txt.push_str("sub_corrupt,0,1,1,{}\n");
    std::fs::write(&csv, txt).unwrap();

    // 第二代：券商镜像仍持有该腿 → 启动对账不得误杀 Active。
    let h2 = spawn_harness_stub(
        dir.path(),
        Vec::new(),
        StubState {
            remote_positions: std::sync::Mutex::new(vec![
                strategy_signals::gateway::RemotePosition {
                    instrument: "BTCUSDT".into(),
                    side: "long".into(),
                    quantity: 1.0,
                    avg_price: 100.0,
                    deleted: false,
                },
            ]),
            ..Default::default()
        },
    );
    h2.send_bar(3, 100.0);
    h2.send_bar(4, 100.0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rows = h2.store.table::<SubPosition>().list();
    assert_eq!(rows.len(), 1, "坏行必须被跳过且好行保留: {rows:?}");
    assert_eq!(rows[0].id, sub_id, "恢复的是同一确定性子仓行");
    assert_eq!(
        rows[0].state,
        SubPositionState::Active,
        "镜像仍在，不得误杀"
    );
    assert!(
        !rows.iter().any(|r| r.id == "sub_corrupt"),
        "坏行不得进入活表"
    );
    h2.shutdown().await;
}

/// P1e：非平价平仓的权益口径——live `realized_pnl` 保持 0（服务端权威，
/// 模板不伪造），但镜像权益/净值必须体现价差收益与两笔手续费。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_non_flat_close_moves_nav_through_mirror() {
    let dir = tempfile::tempdir().unwrap();
    let h = spawn_harness(
        dir.path(),
        vec![
            vec![open_action(&perpetual_btc())],
            Vec::new(),
            vec![close_action(&perpetual_btc())],
        ],
        None,
    );
    h.send_bar(0, 100.0);
    h.send_bar(1, 100.0);
    let sub = h.wait_sub(SubPositionState::OpenRequested).await;
    h.handle
        .order_update(
            "ord_1".into(),
            None,
            CoreOrderState::Filled,
            1.0,
            100.0,
            Some("1".into()),
            Some(sub.id.clone()),
            None,
            None,
            Some(0.1),
            h.t0 + ChronoDuration::minutes(2),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    h.send_bar(2, 100.0);
    let sub = h.wait_sub(SubPositionState::Active).await;

    h.send_bar(3, 110.0);
    let close_signal = wait_signal(&h.store, "平仓信号已受理", |m| {
        m.intent.kind == IntentKind::Close && m.order_id.is_some()
    })
    .await;
    let order_id = close_signal.order_id.clone().unwrap();
    h.handle
        .order_update(
            order_id,
            None,
            CoreOrderState::Filled,
            1.0,
            110.0,
            Some("1".into()),
            Some(close_signal.intent.id.clone()),
            None,
            None,
            Some(0.1),
            h.t0 + ChronoDuration::minutes(4),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    h.send_bar(4, 110.0);
    let closed = h.wait_sub(SubPositionState::Closed).await;
    assert_eq!(closed.id, sub.id);
    assert_eq!(closed.qty, 0.0);
    assert_eq!(
        closed.realized_pnl,
        Some(0.0),
        "live 子仓已实现盈亏保持服务端权威口径（不本地伪造）"
    );
    let portfolio = h.store.table::<StrategyPortfolio>().list();
    assert_eq!(portfolio.len(), 1);
    let nav = portfolio[0].nav;
    // 初始 5000 → 开仓付 100.1 → 平仓收 109.9 → 5009.8（价差 +10 - 两笔 0.1 费）。
    assert!(
        (nav - 5009.8).abs() < 1e-6,
        "镜像权益必须体现价差与手续费，实际 nav={nav}"
    );
    h.shutdown().await;
}

// ─── P2：venue 分段回报 / 时钟跨日 / 网关契约 ─────────────────────────

/// P2a：venue 侧 PARTIAL→PARTIAL→FILLED 三段回报（mock 真实 WS 驱动）：
/// 中间态子仓保持 OpenRequested，终态一次性按累计量落 Active，只有一行。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_partial_frames_land_full_qty_once() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let chain = spawn_full_chain_with_fills(
        dir.path(),
        strategy_cfg(),
        vec![vec![open_action(&btc())]],
        vec![(100.0, 50.0); 40],
        3,
        Duration::from_millis(150),
    )
    .await;
    let store = Arc::clone(&chain.store);
    let sub = wait_sub_rows(&store, "首段回报前的挂单行", |rows| {
        rows.iter()
            .any(|r| r.state == SubPositionState::OpenRequested)
    })
    .await;
    let sub_id = sub[0].id.clone();
    // 两段 PARTIAL 期间：信号 Accepted（累计量未满），子仓不得提前 Active。
    let m = wait_signal(&store, "PARTIAL 累计不足", |m| {
        m.intent.id == sub_id
            && m.status == SignalStatus::Accepted
            && m.filled_quantity.is_some_and(|q| q > 0.0)
    })
    .await;
    let partial_qty = m.filled_quantity.unwrap();
    assert_eq!(
        store.table::<SubPosition>().get(&sub_id).unwrap().state,
        SubPositionState::OpenRequested,
        "分段回报期间不得提前 Active"
    );

    let rows = wait_sub_rows(&store, "终段 FILLED 落 Active", |rows| {
        rows.iter()
            .any(|r| r.id == sub_id && r.state == SubPositionState::Active)
    })
    .await;
    let active = rows.iter().find(|r| r.id == sub_id).unwrap();
    let final_signal = wait_signal(&store, "信号 Filled", |m| {
        m.intent.id == sub_id && m.status == SignalStatus::Filled
    })
    .await;
    let full = final_signal.filled_quantity.unwrap();
    assert!(full > partial_qty, "累计量必须按服务端口径累进");
    assert_eq!(active.qty, full, "子仓数量必须等于终段累计成交量");
    assert_eq!(
        store.table::<SubPosition>().list().len(),
        1,
        "分段回报不得生成多行"
    );
    chain.shutdown().await;
}

/// P2b：时钟注入——持仓跨日后 flush 落到新日目录，内存行保持 Active，
/// 不因「归属日」变化被裁剪或复活。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_clock_rollover_keeps_active_position_in_new_day_dir() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let day0_ms = Utc::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let clock = Arc::new(AtomicSimClock(AtomicI64::new(day0_ms)));
    let mut drill = start_drill_with_interval(dir.path(), Duration::from_millis(100)).await;
    let symbols = drill_symbols();
    drill
        .trading
        .subscribe_klines("template_xsec_ls", "binance", &symbols)
        .await
        .expect("mock 订阅帧必须被受理");
    let (btc_tx, btc_rx) = std::sync::mpsc::channel();
    let (eth_tx, eth_rx) = std::sync::mpsc::channel();
    let state_dir = dir.path().join("state");
    let store = Arc::new(Store::new(state_dir.clone(), clock.clone()));
    let btc = btc();
    let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
    let (handle, task, forwarder) = spawn_drill_actor(
        drill_spec_cfg(state_dir.clone(), strategy_cfg()),
        vec![btc_rx, eth_rx],
        Arc::clone(&store),
        Arc::clone(&drill.trading),
        vec![vec![open_action(&btc)]],
        drill.take_events(),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let feeder = spawn_priced_feeder(
        btc_tx,
        eth_tx,
        t0,
        vec![(100.0, 50.0); 40],
        Arc::clone(&stop),
    );
    let rows = wait_sub_rows(&store, "跨日前 Active", |rows| {
        rows.iter().any(|r| r.state == SubPositionState::Active)
    })
    .await;
    let sub_id = rows[0].id.clone();

    let day2_ms = day0_ms + 86_400_000;
    let day2 = day_key(day2_ms);
    clock.set(day2_ms);
    store.flush().expect("跨日 flush 必须成功");
    let sub_csv = day_csv(&state_dir, &day2, "sub_position");
    assert!(
        sub_csv.contains(&sub_id),
        "新日目录必须含活动子仓行（{day2}）：{sub_csv}"
    );
    assert!(
        sub_csv.contains("active"),
        "跨日 flush 后状态仍为 active：{sub_csv}"
    );
    let rows = store.table::<SubPosition>().list();
    assert_eq!(rows.len(), 1, "跨日不得复制行: {rows:?}");
    assert_eq!(rows[0].state, SubPositionState::Active, "跨日不得收口");
    stop.store(true, Ordering::SeqCst);
    feeder.await.unwrap();
    handle.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("Shutdown 后必须收口")
        .expect("监督任务不得 panic");
    forwarder.abort();
}

/// P2c：网关契约（venue conformance 的种子）：任意 `AsyncOrderGateway`
/// 后端必须满足 submit→FILLED 查询→仓位镜像的可观测闭环。新增交易所时
/// 用同一脚本替换 mock 服务端即可复用。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn venue_contract_submit_query_positions_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let drill = start_drill(dir.path()).await;
    let venue = OrderVenueCfg {
        exchange: "binance".into(),
        account: "default".into(),
        leverage: 1,
        order_cash: 200.0,
    };
    let intent = SignalIntent {
        id: "venue_contract_open_1".into(),
        strategy_id: "template_xsec_ls".into(),
        symbol: "BTCUSDT".into(),
        kind: IntentKind::Open,
        direction: Side::Long,
        price: 100.0,
        size: 0.0,
        size_raw: None,
        stop_price: None,
        position_id: None,
        stop_order_id: None,
        created_event_time: Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
        status: SignalStatus::Created,
        offset: None,
    };
    let order_id = drill
        .trading
        .submit_order("template_xsec_ls", &venue, &intent)
        .await
        .expect("mock venue 必须受理开仓");
    assert!(!order_id.is_empty(), "venue 必须返回非空订单号");

    let mut filled = None;
    for _ in 0..200 {
        match drill
            .trading
            .get_order("template_xsec_ls", &venue, &intent.id)
            .await
        {
            Ok(snap) if snap.state == CoreOrderState::Filled => {
                filled = Some(snap);
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    let snap = filled.expect("2s 内必须可查到 FILLED 快照");
    assert!(snap.filled_size > 0.0, "成交快照成交量必须为正");
    assert!(
        (snap.avg_price - 100.0).abs() < 1e-9,
        "成交价按 venue 参考价"
    );

    let mut remote_qty = 0.0;
    for side in ["long", "short"] {
        let rows = drill
            .trading
            .get_positions("template_xsec_ls", &venue, "BTCUSDT", side)
            .await
            .expect("仓位查询必须成功");
        remote_qty += rows
            .iter()
            .filter(|r| !r.deleted)
            .map(|r| r.quantity)
            .sum::<f64>();
    }
    assert!(
        (remote_qty - snap.filled_size).abs() < 1e-9,
        "仓位镜像必须等于成交累计量：remote={remote_qty} filled={}",
        snap.filled_size
    );
}

/// P2d：`merge_positions=false`（逐标的独立总仓）live 全链——
/// 每个标的独立 master（`scope` 标注本标的），L3 只平触发标的，
/// 另一标的保持持仓。该模式由 `config/sar_supertrend`、`ctp_multi_xsec` 使用。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_chain_per_symbol_scopes_l3_closes_only_triggered() {
    init_drill_tracing();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = strategy_cfg();
    cfg.merge_positions = false;
    // 逐标的总仓按标的各自判定，不依赖跨标的调仓窗口。
    cfg.rebalance = None;
    cfg.portfolio_exit.stop_loss_pct = Some(2.0);
    let mut prices = vec![(100.0, 50.0); 4];
    // BTC 跌 3% 越过 2% 硬止损；ETH 价格不动不得被连带平仓。
    prices.extend([(97.0, 50.0); 10]);
    let chain = spawn_full_chain(
        dir.path(),
        cfg,
        vec![
            // 逐标的总仓模式下策略按 slot 逐个调用：每标的一笔开仓。
            vec![open_action(&btc())],
            vec![open_action(&eth())],
        ],
        prices,
    )
    .await;
    let store = Arc::clone(&chain.store);
    let opened = wait_sub_rows(&store, "逐标的总仓双 Active", |rows| {
        rows.len() == 2 && rows.iter().all(|r| r.state == SubPositionState::Active)
    })
    .await;
    let masters = store.table::<MasterPosition>().list();
    assert_eq!(masters.len(), 2, "逐标的总仓必须各自建仓: {masters:?}");
    for row in &opened {
        let key = row.instrument.namespaced_key();
        let master = masters
            .iter()
            .find(|m| m.master_id == row.master_id)
            .unwrap_or_else(|| panic!("子仓总仓缺失: {row:?}"));
        assert_eq!(
            master.scope.as_deref(),
            Some(key.as_str()),
            "总仓必须限定在子仓所属标的: {master:?}"
        );
    }
    assert_ne!(
        opened[0].master_id, opened[1].master_id,
        "标的间不得共享总仓"
    );

    let rows = wait_sub_rows(&store, "BTC 跌破止损", |rows| {
        rows.iter()
            .any(|r| r.instrument.symbol == "BTCUSDT" && r.state == SubPositionState::Closed)
    })
    .await;
    let eth = rows
        .iter()
        .find(|r| r.instrument.symbol == "ETHUSDT")
        .expect("ETH 行");
    assert_eq!(eth.state, SubPositionState::Active, "L3 不得连带平其他标的");
    assert!(eth.qty > 0.0, "未被触发的标的仓位必须保留: {eth:?}");

    let btc_key = rows
        .iter()
        .find(|r| r.instrument.symbol == "BTCUSDT")
        .unwrap()
        .instrument
        .namespaced_key();
    let eth_key = eth.instrument.namespaced_key();
    let portfolio = store.table::<StrategyPortfolio>().list();
    assert_eq!(portfolio.len(), 1);
    let s = &portfolio[0];
    assert!(
        s.scope_risk.get(&btc_key).is_some_and(|r| r.paused),
        "触发标的必须置暂停: {:?}",
        s.scope_risk
    );
    assert!(
        !s.scope_risk.get(&eth_key).is_some_and(|r| r.paused),
        "未触发标的不得被连带暂停: {:?}",
        s.scope_risk
    );
    let masters = store.table::<MasterPosition>().list();
    let btc_master = masters
        .iter()
        .find(|m| m.scope.as_deref() == Some(btc_key.as_str()))
        .expect("BTC 总仓");
    assert_eq!(
        btc_master.state,
        MasterPositionState::Closed,
        "触发标的总仓收口"
    );
    let eth_master = masters
        .iter()
        .find(|m| m.scope.as_deref() == Some(eth_key.as_str()))
        .expect("ETH 总仓");
    assert_ne!(
        eth_master.state,
        MasterPositionState::Closed,
        "另一标的总仓保持"
    );
    chain.shutdown().await;
}
