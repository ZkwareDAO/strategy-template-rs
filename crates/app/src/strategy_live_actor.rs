//! 策略实盘 actor（PR-5 Task 4 骨架；提交/回报接线 = Task 5）。
//!
//! 与旧单标的 `spi_live_actor` 同族的**监督代**形态：tokio 外壳线程 +
//! `std::thread`（current_thread runtime + `catch_unwind`）内核；内核装配
//! 失败/panic 时整代结束，由外壳退避重建（重建装配先
//! `recover_from_store(after_panic=true)`，否则 `false`——「先账本后消费」）。
//!
//! 内核单线程事件泵（G3：与回测同一段 `CrossSectionalRunnerGroup` 主循环）：
//! - **数据面**：每标的 `ChannelBarSource`（bootstrap 扇出的已闭合 1m）→
//!   `AlignedEventSource::next_aligned_nonblocking`；`Ready` → `group.on_event`，
//!   `Starved` → 让出线程，`Done` → 收口；
//! - **控制面**：tokio mpsc `RuntimeCommand`（`OrderUpdate`/`Reconcile`/`Shutdown`，
//!   复用旧 `runtime_types` 零扩展）；`Reconcile` → `group.reconcile_remote`；
//! - **定时器**：`position_check_interval` 周期对账、`state_flush_interval`
//!   flush（actor 线程单写者纪律：不与他处并发 flush）。
//!
//! 本代限制（Task 5/6 补齐，已登记）：live 提交面未接通（`take_submit_requests`
//! 取出即丢弃并告警，绝不静默）；panic 重建代无实时源（bootstrap 在 T6 承担
//! 重订阅后重新供给 `candle_rxs`）。

use std::path::PathBuf;
use std::sync::mpsc as stdmpsc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use strategy_core::candle::Candle;
use strategy_core::signal::Side;
use strategy_core::IntentKind;
use strategy_engine::core::bar::{Timeframe, VecEventSource};
use strategy_engine::core::exchange::SourceRequest;
use strategy_engine::core::identity::RunMode;
use strategy_engine::core::position::PositionLot;
use strategy_engine::exchanges::live::{ChannelBarSource, LiveExecution, LiveOrderUpdate};
use strategy_engine::strategy::align::{AlignPoll, AlignedEventSource};
use strategy_engine::strategy::config::StrategyConfig;
use strategy_engine::strategy::features::FeaturePlan;
use strategy_engine::strategy::model::SubPositionState;
use strategy_engine::strategy::port::CrossSectionalStrategyPort;
use strategy_engine::strategy::runner::{CrossSectionalRunnerGroup, SlotInit, StrategySlot};
use strategy_signals::gateway::{AsyncOrderGateway, FailureKind, RemotePosition, SubmitError};
use strategy_signals::manager::{RecoveryAction, SignalManager, SubmitRequest};
use strategy_store::Store;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{debug, error, info, warn};

use crate::config::SymbolSpec;
use crate::runtime_types::{
    OrderUpdateCmd, PollWatch, RouteBind, RuntimeCommand, RuntimeHandle, POLL_GRACE_SECS,
    POLL_INTERVAL_SECS,
};
use crate::spi_backtest::{candles_to_bars, exchange_for_spec};

/// 策略 live 运行规格（bootstrap 由 `load_strategy` 产物填充；T6 接线）。
#[derive(Debug, Clone)]
pub struct StrategyLiveSpec {
    pub strategy_id: String,
    pub strategy_type: String,
    pub config: StrategyConfig,
    /// 每 universe 条目一条（同序；exchange/contract/venue_key 已解析）
    pub instruments: Vec<SymbolSpec>,
    /// `live` | `paper_trading`（仅日志/审计用；执行面由 `gateway_factory` 判定）
    pub mode: String,
    pub state_dir: PathBuf,
    pub state_flush_interval_secs: u64,
    /// 对账周期（默认 60；0 按 1 处理）
    pub position_check_interval_secs: u64,
    /// 组合总初始权益（按标的等分起账，与回测 `run_strategy_from_spec` 同口径）
    pub initial_cash_total: f64,
    pub fee_rate: Option<f64>,
    pub channel_cap: usize,
}

/// 监督代装配输入（每代重建一次；策略/网关工厂与旧 `RuntimeFactory` 同型）。
///
/// 注：计划中的 `me: RuntimeHandle` 由 [`spawn_strategy_live`] 内部生成后注入
/// 内核——调用方在 spawn 返回前无法持有句柄，故不放入公开装配结构。
pub struct StrategyLiveAssembly {
    pub spec: StrategyLiveSpec,
    /// 每标的一条「已闭合 1m」流（bootstrap 扇出；仅首代有效）
    pub candle_rxs: Vec<stdmpsc::Receiver<Candle>>,
    /// 每标的预热（下标一一对应；不足只告警，entry 门兜底）
    pub warmup: Vec<Vec<Candle>>,
    pub store: Arc<Store>,
    /// None = paper（`SimulationExecution` 本地撮合）；Some = live（T5 提交面）
    pub gateway_factory: Option<Box<dyn Fn() -> Box<dyn AsyncOrderGateway> + Send>>,
    /// 每代新建策略实例（recover 的 fresh_strategy 也从这里取——内存中间态
    /// 一律不续用，与 `recover_from_store` 契约一致）
    pub strategy_factory: Box<dyn Fn() -> Box<dyn CrossSectionalStrategyPort> + Send>,
    pub route_tx: Option<mpsc::Sender<RouteBind>>,
}

/// 启动横截面 live actor：返回命令句柄与外壳任务（监督循环）。
pub fn spawn_strategy_live(
    assembly: StrategyLiveAssembly,
) -> (RuntimeHandle, tokio::task::JoinHandle<()>) {
    let cap = assembly.spec.channel_cap.max(1);
    let (tx, rx) = mpsc::channel::<RuntimeCommand>(cap);
    let me = RuntimeHandle::from_command_tx(tx);
    let stable = me.clone();
    let sup = tokio::spawn(async move {
        let mut outer_rx = rx;
        let mut generation: u32 = 0;
        let mut after_panic = false;
        // 实时源只能被一代消费：首代取出；panic 重建代由 T6 bootstrap 重订阅后
        // 重新装配（本文件先以空源收口，日志显式告知）。
        let mut rxs: Option<Vec<stdmpsc::Receiver<Candle>>> = Some(assembly.candle_rxs);
        'outer: loop {
            let gateway = assembly.gateway_factory.as_ref().map(|f| f());
            // 两实例：group 持一个，`recover_from_store` 的 fresh_strategy 取另一个
            // （recover 契约禁止续用内存中间态）。
            let strategy = (assembly.strategy_factory)();
            let fresh_strategy = (assembly.strategy_factory)();
            let candle_rxs = rxs.take();
            let (fwd_tx, fwd_rx) = mpsc::channel::<RuntimeCommand>(cap);
            let (done_tx, mut done_rx) = tokio::sync::oneshot::channel::<std::thread::Result<()>>();
            let spec = assembly.spec.clone();
            let store = Arc::clone(&assembly.store);
            let warmup = assembly.warmup.clone();
            let me_t = stable.clone();
            let route_tx = assembly.route_tx.clone();
            let thread_name = format!("strategy-live-{}-g{}", spec.strategy_id, generation);
            let spawned = std::thread::Builder::new()
                .name(thread_name)
                .spawn(move || {
                    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let rt = match tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                        {
                            Ok(rt) => rt,
                            Err(e) => {
                                error!(error = %e, "strategy live actor 线程 runtime 构建失败");
                                return;
                            }
                        };
                        rt.block_on(async move {
                            match StrategyLiveKernel::assemble(AssembleInput {
                                spec,
                                candle_rxs,
                                warmup,
                                store,
                                gateway,
                                strategy,
                                fresh_strategy,
                                me: me_t,
                                route_tx,
                                after_panic,
                            })
                            .await
                            {
                                Ok(k) => {
                                    if let Err(e) = run_generation(k, fwd_rx).await {
                                        error!(error = %e, "strategy live 内核运行失败，本代结束");
                                    }
                                }
                                Err(e) => {
                                    error!(error = ?e, "strategy live 装配失败，本代结束（监督层整代重建）")
                                }
                            }
                        });
                    }));
                    let _ = done_tx.send(caught);
                });
            if let Err(e) = spawned {
                warn!(error = %e, "strategy live actor 线程 spawn 失败，退避后重建整代");
                generation += 1;
                after_panic = true;
                tokio::time::sleep(Duration::from_millis(
                    200u64.saturating_mul(generation.min(6) as u64),
                ))
                .await;
                continue;
            }
            loop {
                tokio::select! {
                    cmd = outer_rx.recv() => {
                        let Some(cmd) = cmd else {
                            // 句柄全部 drop：让线程排空后优雅退出
                            drop(fwd_tx);
                            let _ = (&mut done_rx).await;
                            break 'outer;
                        };
                        let is_shutdown = matches!(cmd, RuntimeCommand::Shutdown);
                        if fwd_tx.send(cmd).await.is_err() {
                            continue;
                        }
                        if is_shutdown {
                            let _ = (&mut done_rx).await;
                            break 'outer;
                        }
                    }
                    r = &mut done_rx => match r {
                        Ok(Ok(())) => break 'outer,
                        Ok(Err(_)) | Err(_) => {
                            generation += 1;
                            after_panic = true;
                            warn!(generation, "strategy live actor panic，整代重建（重订阅 + recover(after_panic=true)）");
                            tokio::time::sleep(Duration::from_millis(
                                200u64.saturating_mul(generation.min(6) as u64),
                            ))
                            .await;
                            break;
                        }
                    },
                }
            }
        }
    });
    (me, sup)
}

struct AssembleInput {
    spec: StrategyLiveSpec,
    /// None = panic 重建代（T6 前无实时源；以空源收口并显式告警）
    candle_rxs: Option<Vec<stdmpsc::Receiver<Candle>>>,
    warmup: Vec<Vec<Candle>>,
    store: Arc<Store>,
    gateway: Option<Box<dyn AsyncOrderGateway>>,
    strategy: Box<dyn CrossSectionalStrategyPort>,
    /// recover 用的全新实例（与 `strategy` 同厂、不同实例）
    fresh_strategy: Box<dyn CrossSectionalStrategyPort>,
    me: RuntimeHandle,
    route_tx: Option<mpsc::Sender<RouteBind>>,
    after_panic: bool,
}

struct StrategyLiveKernel {
    spec: StrategyLiveSpec,
    group: CrossSectionalRunnerGroup,
    aligned: AlignedEventSource,
    store: Arc<Store>,
    /// 提交/查询网关（None = paper：`SimulationExecution` 本地撮合）
    gateway: Option<Box<dyn AsyncOrderGateway>>,
    /// 每 slot 的在途看门狗轮询状态（每代重建）
    poll_watch: Vec<PollWatch>,
    /// 策略 panic → 组暂停的一次性告警（进程重启前不放行）
    pause_noted: bool,
    me: RuntimeHandle,
    route_tx: Option<mpsc::Sender<RouteBind>>,
}

/// 网关 `get_positions` → 各 slot 镜像账本（remote→local 单向）。
///
/// fail-open：某 slot 查询失败 → 保留其原镜像（下次再查），不臆造空仓。
/// 确认的空仓也会**采纳**（`reconcile_book(vec![])`）——外部平仓由此进入
/// 对账收敛（`RemoteFlat→Closed`）。
async fn refresh_remote_positions_impl(
    group: &mut CrossSectionalRunnerGroup,
    gateway: &mut dyn AsyncOrderGateway,
    now: DateTime<Utc>,
) {
    for i in 0..group.slots.len() {
        let inst = group.slots[i].parts.instrument.clone();
        let symbol = inst.symbol.clone();
        let session = Arc::clone(&group.slots[i].parts.session);
        let mut lots: Vec<PositionLot> = Vec::new();
        let mut ok = true;
        for want in ["long", "short"] {
            match gateway.positions_fut(&symbol, want).await {
                Ok(rows) => {
                    let side = if want == "long" {
                        Side::Long
                    } else {
                        Side::Short
                    };
                    for r in rows
                        .into_iter()
                        .filter(|r: &RemotePosition| !r.deleted && r.quantity > 1e-12)
                    {
                        lots.push(PositionLot {
                            id: format!("remote_{symbol}_{want}"),
                            instrument: inst.clone(),
                            side,
                            entry_price: r.avg_price,
                            qty: r.quantity,
                            remaining: r.quantity,
                            opened_at: now,
                            trading_day: session.trading_day_of(now),
                            is_today: true,
                            qty_raw: None,
                            remaining_raw: None,
                        });
                    }
                }
                Err(e) => {
                    warn!(
                        symbol = %symbol, side = want, error = %e,
                        "strategy live：get_positions 查询失败，fail-open 保留镜像"
                    );
                    ok = false;
                }
            }
        }
        if ok {
            if let Some(live) = group.slots[i].parts.execution.as_live_mut() {
                live.reconcile_book(lots);
            }
        }
    }
}

/// live 场所按固定名义下单：单笔名义必须落在单仓预算内，否则配置层拒绝启动。
/// 与 runner 的 `sizing` 口径对齐，避免每轮调仓产生无谓拒单。
fn validate_live_order_budget(order_cash: f64, single_position_budget: f64) -> anyhow::Result<()> {
    anyhow::ensure!(order_cash > 0.0, "live 下单名义必须为正，实际 {order_cash}");
    anyhow::ensure!(
        order_cash <= single_position_budget + 1e-9,
        "live 下单名义 {order_cash:.6} 超过单仓预算 {single_position_budget:.6}"
    );
    Ok(())
}

impl StrategyLiveKernel {
    /// 每监督代一次：装配 slots → 拉远程仓位进镜像（remote→local）→
    /// `recover_from_store`（先账本后消费）→ 对齐源。
    async fn assemble(mut a: AssembleInput) -> anyhow::Result<Self> {
        let spec = a.spec;
        let n = spec.instruments.len();
        anyhow::ensure!(n >= 1, "横截面 universe 为空");
        let live = a.gateway.is_some();
        let timeframes: Vec<Timeframe> = spec
            .config
            .timeframes
            .iter()
            .map(|s| Timeframe::parse(s).map_err(|e| anyhow!("timeframe `{s}` 非法: {e}")))
            .collect::<anyhow::Result<_>>()?;
        anyhow::ensure!(!timeframes.is_empty(), "timeframes 为空");
        // 合并总仓：每个 slot 以完整总资金虚拟起账，汇总层扣除重复现金；
        // 逐标的独立总仓：每个 slot 只拿自己那份，直接汇总 NAV。
        let slot_cash = if spec.config.merge_positions {
            spec.initial_cash_total
        } else {
            spec.initial_cash_total / n as f64
        };
        // live 币安按 venue.order_cash 下单（忽略 runner qty）：启动即校验
        // 单笔名义 ≤ 单仓预算，配置冲突不等到第一轮调仓才暴露成拒单。
        if live {
            let sizing = &spec.config.sizing;
            let pct = sizing
                .max_position_pct
                .map_or(sizing.per_position_pct, |cap| {
                    sizing.per_position_pct.min(cap)
                });
            let budget = slot_cash * pct;
            for ispec in &spec.instruments {
                if ispec.contract.is_none() {
                    validate_live_order_budget(ispec.cash, budget)
                        .map_err(|e| anyhow!("{}: {e}", ispec.symbol))?;
                }
            }
        }

        let mut rxs = a.candle_rxs;
        if let Some(v) = &rxs {
            anyhow::ensure!(
                v.len() == n,
                "candle_rxs 数量（{}）与 instruments（{n}）不一致",
                v.len()
            );
        } else {
            warn!(
                strategy_id = %spec.strategy_id,
                "strategy live：panic 重建代无实时源（T6 bootstrap 重订阅接管），本代以空源收口"
            );
        }
        anyhow::ensure!(
            a.warmup.len() == n,
            "warmup 数量（{}）与 instruments（{n}）不一致",
            a.warmup.len()
        );

        let mut slots: Vec<StrategySlot> = Vec::with_capacity(n);
        for (i, ispec) in spec.instruments.iter().enumerate() {
            let feature_plan = FeaturePlan::from_config(&spec.config)
                .map_err(|errs| anyhow!("特征计划构建失败:\n- {}", errs.join("\n- ")))?;
            let (exchange, inst) = exchange_for_spec(ispec, spec.fee_rate)?;
            // build 只接受 non-live 模式（默认实现按模式分叉）：paper 直接复用
            // SimulationExecution；live 在替换执行端口后再改回 RunMode::Live。
            let mut parts = exchange
                .build(&SourceRequest {
                    instrument: inst.clone(),
                    mode: RunMode::Paper,
                    from: None,
                    to: None,
                    warmup_bars: 0,
                    bars: Vec::new(),
                    universe: Vec::new(),
                    initial_cash: slot_cash,
                    as_of: None,
                })
                .map_err(|e| anyhow!("{}: 装配失败: {e}", ispec.symbol))?;

            // 执行端口：paper = 本地撮合（build 默认）；live = LiveExecution（镜像账本）。
            if live {
                let trading = Arc::clone(&parts.trading);
                let position = Arc::clone(&parts.position);
                let session = Arc::clone(&parts.session);
                let manager = SignalManager::new(
                    format!("{}-s{i}", spec.strategy_id),
                    Arc::clone(&a.store),
                    None,
                );
                let exec: LiveExecution = if ispec.contract.is_some() {
                    strategy_engine::exchanges::ctp::live_gateway::ctp_live_execution(
                        inst.clone(),
                        spec.strategy_type.clone(),
                        manager,
                        trading,
                        position,
                        session,
                        slot_cash,
                    )
                } else {
                    strategy_engine::exchanges::binance::live_gateway::binance_live_execution(
                        inst.clone(),
                        spec.strategy_type.clone(),
                        manager,
                        trading,
                        position,
                        session,
                        slot_cash,
                    )
                    .with_order_cash(ispec.cash)
                };
                parts.execution = Box::new(exec);
                parts.mode = RunMode::Live;
            }

            // 行情源：已闭合 1m channel 桥（bootstrap 扇出）。
            parts.source = match rxs.as_mut() {
                Some(v) => Box::new(ChannelBarSource::new(inst.clone(), v.remove(0))),
                None => Box::new(VecEventSource::new(Vec::new())),
            };

            let mut slot = StrategySlot::new(SlotInit {
                parts,
                timeframes: timeframes.clone(),
                feature_plan,
                risk: ispec.risk.clone(),
                strategy_type: spec.strategy_type.clone(),
                max_closed_len: 2000,
            });
            let warm = candles_to_bars(&inst, &a.warmup[i]);
            if let Err(e) = slot.preload(&warm) {
                warn!(
                    instrument = %inst,
                    error = %e,
                    "strategy live：warmup 预载失败（不足由 entry 门兜底，不阻断装配）"
                );
            }
            slots.push(slot);
        }

        let mut group = CrossSectionalRunnerGroup::new(
            &spec.strategy_id,
            &spec.strategy_type,
            spec.config.clone(),
            slots,
            a.strategy,
            Some(Arc::clone(&a.store)),
        )
        .map_err(|e| anyhow!("横截面组装配失败: {e}"))?;
        if spec.config.merge_positions {
            group.set_initial_cash_total(spec.initial_cash_total);
        } else {
            group.set_initial_cash_split(spec.initial_cash_total);
        }
        group.enable_history_projection(&spec.state_dir, &spec.mode);
        // 先账本后消费（含 T7 顺序）：先把网关 get_positions 采纳进镜像账本，
        // recover 的远程对账才有真实 broker 事实可比（重启后 Active 子仓不被误杀）。
        if let Some(gw) = a.gateway.as_mut() {
            refresh_remote_positions_impl(&mut group, gw.as_mut(), Utc::now()).await;
        }
        // Store 行重建 + 远程对账（panic 后代 after_panic=true）。
        let report = group
            .recover_from_store(a.store.as_ref(), a.fresh_strategy, a.after_panic)
            .map_err(|e| anyhow!("recover_from_store 失败: {e}"))?;
        if !report.drifts.is_empty() {
            warn!(
                strategy_id = %spec.strategy_id,
                synced = report.synced_instruments,
                drifts = report.drifts.len(),
                "strategy live：启动对账发现漂移（详见逐条日志）"
            );
            for (instrument, kind, detail) in &report.drifts {
                warn!(instrument = %instrument, kind = %kind, detail = %detail, "strategy live：recover drift");
            }
        }
        let (srcs, keys) = group.sources();
        let aligned = AlignedEventSource::new(srcs, keys);
        Ok(Self {
            spec,
            group,
            aligned,
            store: a.store,
            gateway: a.gateway,
            poll_watch: (0..n).map(|_| PollWatch::default()).collect(),
            pause_noted: false,
            me: a.me,
            route_tx: a.route_tx,
        })
    }

    /// 周期/命令触发的远程对账：永不发单，只收敛账本 + 告警。
    fn reconcile(&mut self) {
        match self.group.reconcile_remote() {
            Ok(report) => {
                for (instrument, kind, detail) in &report.drifts {
                    warn!(instrument = %instrument, kind = %kind, detail = %detail, "strategy live：对账漂移");
                }
            }
            Err(e) => error!(error = %e, "strategy live：对账失败（本拍跳过）"),
        }
    }

    /// 远程对账全套（T7）：get_positions 采纳进镜像 → group.reconcile_remote
    /// 收敛子仓（永不发单）。paper 无网关时退化为仅本地对账。
    async fn reconcile_all(&mut self, now: DateTime<Utc>) {
        if let Some(gw) = self.gateway.as_mut() {
            refresh_remote_positions_impl(&mut self.group, gw.as_mut(), now).await;
        }
        self.reconcile();
    }

    /// 回报入口（T5）：按 signal_id/order_id 精确路由到所属 slot 的
    /// `LiveExecution`（三级确认之一：update → 内核状态机 + engine Fill）。
    fn apply_order_update(&mut self, u: OrderUpdateCmd) {
        let lu = LiveOrderUpdate {
            order_id: u.order_id.clone(),
            exchange_order_id: u.exchange_order_id.clone(),
            state: u.state,
            filled_size: u.filled_size,
            avg_price: u.avg_price,
            filled_quantity_raw: u.filled_quantity_raw.clone(),
            signal_id: u.signal_id.clone(),
            message: u.message.clone(),
            fee: u.fee,
            at: u.event_time,
        };
        let Some(i) = self.locate_order(&u) else {
            warn!(order_id = %u.order_id, "strategy live：无 slot 识别该订单回报（丢弃并审计）");
            return;
        };
        debug!(
            slot = i,
            order_id = %u.order_id,
            signal_id = ?u.signal_id,
            state = ?u.state,
            "strategy live：回报已定位到 slot"
        );
        let Some(live) = self.group.slots[i].parts.execution.as_live_mut() else {
            return;
        };
        match live.on_order_update(&lu) {
            Ok(fill) => debug!(
                slot = i,
                order_id = %u.order_id,
                produced_fill = fill.is_some(),
                "strategy live：回报内核已处理"
            ),
            Err(e) => error!(error = %e, order_id = %u.order_id, "strategy live：回报内核处理失败"),
        }
        // 实际委托要素审计落账（与旧链同位）。
        if let Some(place) = &u.place {
            if let Err(e) =
                live.manager_mut()
                    .record_place(&u.order_id, u.signal_id.as_deref(), place)
            {
                error!(error = %e, "strategy live：委托要素落账失败");
            }
        }
        self.reduce_only_external_close(
            i,
            &u.order_id,
            u.signal_id.as_deref(),
            u.message.as_deref(),
            u.event_time,
        );
        // 成交在下一轮轮首 `sync_slot_fills` 落 SubPosition（C7：raw 原样）。
    }

    /// 订单归属定位：signal_id 优先（意图在所有 slot 内核持久化），
    /// 回退 order_id 映射。
    fn locate_order(&mut self, u: &OrderUpdateCmd) -> Option<usize> {
        // 1) order_id → 各实例 scoped order_index（唯一无歧义真源）。
        for i in 0..self.group.slots.len() {
            let hit = match self.group.slots[i].parts.execution.as_live_mut() {
                Some(live) => live.manager().intent_for_order(&u.order_id).is_some(),
                None => false,
            };
            if hit {
                return Some(i);
            }
        }
        // 2) 兜底：按实例归属判定（`intent_for_signal` 是共享表全表查找，
        //    直接用会把兄弟 slot 的信号错投——T7 演练实测踩过）。
        if let Some(sid) = u.signal_id.as_deref() {
            for i in 0..self.group.slots.len() {
                let hit = match self.group.slots[i].parts.execution.as_live_mut() {
                    Some(live) => live.manager().owns_signal(sid),
                    None => false,
                };
                if hit {
                    return Some(i);
                }
            }
        }
        None
    }

    /// 提交面（T5）：每轮后把 `LiveExecution` 的待提交队列送网关（帧字段与
    /// 旧链一致：exchange/account/order_cash 由网关侧翻译；signal_id 原样，
    /// 策略运行时为 `sub_...`）。paper（无网关）没有待提交（本地撮合即成交）。
    async fn pump_submits(&mut self) {
        for i in 0..self.group.slots.len() {
            let reqs = match self.group.slots[i].parts.execution.as_live_mut() {
                Some(live) => live.take_submit_requests(),
                None => continue,
            };
            for req in reqs {
                self.submit_one(i, req).await;
            }
        }
    }

    /// 单笔提交（旧 `pump_submit` 语义移植）：平仓守卫 → 网关等待 → ack 落内核
    /// → RouteBind 注册 → ReduceOnly 外部平仓对账。
    async fn submit_one(&mut self, slot_idx: usize, req: SubmitRequest) {
        let signal_id = req.signal_id.clone();
        let is_close = req.intent.kind == IntentKind::Close;
        let event_time = req.event_time;
        if is_close && (req.intent.size <= 0.0 || !self.slot_has_open_sub(slot_idx)) {
            warn!(
                signal_id = %signal_id,
                "strategy live：平仓条件不成立（无子仓/无精确成交量），不发送 place_order"
            );
            if let Some(live) = self.group.slots[slot_idx].parts.execution.as_live_mut() {
                let _ = live.apply_submit_outcome(
                    req,
                    Err(SubmitError::rejected("平仓时无有效子仓/成交量，未发送")),
                );
            }
            return;
        }
        let Some(gateway) = self.gateway.as_mut() else {
            return;
        };
        let outcome = gateway.submit_fut(&req.intent).await;
        let rejected_msg = match &outcome {
            Err(e) if e.kind == FailureKind::Rejected => Some(e.message.clone()),
            _ => None,
        };
        let order_id = {
            let Some(live) = self.group.slots[slot_idx].parts.execution.as_live_mut() else {
                return;
            };
            if let Err(e) = live.apply_submit_outcome(req, outcome) {
                error!(error = %e, signal_id = %signal_id, "strategy live：提交结果落账失败");
                return;
            }
            live.manager().order_id_of(&signal_id)
        };
        self.bind_order(order_id.clone()).await;
        self.reduce_only_external_close(
            slot_idx,
            order_id.as_deref().unwrap_or(""),
            Some(&signal_id),
            rejected_msg.as_deref(),
            event_time,
        );
    }

    fn slot_has_open_sub(&self, slot_idx: usize) -> bool {
        self.group.slots[slot_idx]
            .sub_positions
            .iter()
            .any(|s| s.state != SubPositionState::Closed)
    }

    /// ack 后 order_id → 本 actor 的精确路由注册（T7 转发器代码不改）。
    async fn bind_order(&self, order_id: Option<String>) {
        let (Some(tx), Some(order_id)) = (&self.route_tx, order_id) else {
            return;
        };
        if let Err(e) = tx
            .send(RouteBind {
                order_id,
                actor: self.me.clone(),
            })
            .await
        {
            warn!(error = %e, "strategy live：order_id 路由注册失败");
        }
    }

    /// ReduceOnly 拒单 ≡ 交易所已无仓位（旧 U18 语义）：crypto 服务端确认无仓
    /// → 镜像账本清零 + 立即对账收敛（子仓 RemoteFlat→Closed，此后 dispatch
    /// 只对 Active 平仓——**禁止重发平仓单**）；CTP 服务端帧无今昨粒度，
    /// 不覆盖镜像，仅对账留痕。
    fn reduce_only_external_close(
        &mut self,
        slot_idx: usize,
        order_id: &str,
        signal_id: Option<&str>,
        message: Option<&str>,
        now: DateTime<Utc>,
    ) {
        if !message.is_some_and(|m| m.contains("ReduceOnly")) {
            return;
        }
        let intent = match self.group.slots[slot_idx].parts.execution.as_live_mut() {
            Some(live) => live
                .manager()
                .intent_for_order(order_id)
                .or_else(|| signal_id.and_then(|sid| live.manager().intent_for_signal(sid))),
            None => None,
        };
        let Some(intent) = intent else {
            warn!(
                order_id,
                "strategy live：ReduceOnly 拒单找不到意图，跳过外部平仓对账"
            );
            return;
        };
        if intent.kind != IntentKind::Close {
            return;
        }
        if self.spec.instruments[slot_idx].contract.is_none() {
            if let Some(live) = self.group.slots[slot_idx].parts.execution.as_live_mut() {
                live.reconcile_book(Vec::new());
            }
        }
        match self.group.reconcile_remote() {
            Ok(report) => {
                for (instrument, kind, detail) in &report.drifts {
                    warn!(
                        instrument = %instrument, kind = %kind, detail = %detail, at = %now,
                        "strategy live：ReduceOnly 外部平仓对账"
                    );
                }
                if report.drifts.is_empty() {
                    warn!(
                        signal_id = ?signal_id,
                        "strategy live：ReduceOnly 拒单但本地无漂移（可能重复提交，需人工核对）"
                    );
                }
            }
            Err(e) => error!(error = %e, "strategy live：ReduceOnly 外部平仓对账失败"),
        }
    }

    /// 启动/重连恢复（先查后发）：内核 `recover()` 动作——`Query` 查询真值
    /// 落账、`Resubmit` 幂等重提（确定性 signal_id + 1:1 守卫双保险）。
    async fn restart_recovery(&mut self, now: DateTime<Utc>) {
        for i in 0..self.group.slots.len() {
            let actions = match self.group.slots[i].parts.execution.as_live_mut() {
                Some(live) => match live.manager_mut().recover() {
                    Ok(a) => a,
                    Err(e) => {
                        error!(error = %e, "strategy live：事件重建失败，跳过本 slot 恢复");
                        continue;
                    }
                },
                None => continue,
            };
            for action in actions {
                match action {
                    RecoveryAction::Resubmit { signal_id } => {
                        let req = match self.group.slots[i].parts.execution.as_live_mut() {
                            Some(live) if live.manager().order_id_of(&signal_id).is_none() => {
                                Some(live.manager().submit_request(&signal_id, now))
                            }
                            _ => None,
                        };
                        if let Some(req) = req {
                            info!(signal_id = %signal_id, "strategy live：恢复重提（先查后发）");
                            self.submit_one(i, req).await;
                        }
                    }
                    RecoveryAction::Query {
                        signal_id,
                        order_id,
                    } => {
                        self.query_order(i, &signal_id, &order_id, now).await;
                    }
                }
            }
        }
    }

    /// 在途看门狗（旧 T5 语义移植）：宽限/最小间隔与 `POLL_GRACE_SECS` /
    /// `POLL_INTERVAL_SECS` 同源；策略组的未成交腿由内核 `in_flight_orders`
    /// 覆盖（OpenRequested/CloseRequested 子仓即其意图行），`get_order`
    /// 快照查询驱动状态收敛。
    async fn poll_in_flight_at(&mut self, now: DateTime<Utc>) {
        let grace = chrono::Duration::seconds(POLL_GRACE_SECS as i64);
        let min_interval = chrono::Duration::seconds(POLL_INTERVAL_SECS.max(1) as i64);
        for i in 0..self.group.slots.len() {
            let due: Vec<(String, String)> = match self.group.slots[i].parts.execution.as_live_mut()
            {
                Some(live) => live
                    .manager()
                    .in_flight_orders()
                    .into_iter()
                    .filter(|(sid, _, created)| {
                        self.poll_watch[i].is_due(sid, *created, now, grace, min_interval)
                    })
                    .map(|(sid, oid, _)| (sid, oid))
                    .collect(),
                None => continue,
            };
            for (signal_id, order_id) in due {
                self.poll_watch[i].mark(&signal_id, now);
                debug!(
                    signal_id = %signal_id, order_id = %order_id,
                    "strategy live：在途看门狗主动查询（get_order）"
                );
                self.query_order(i, &signal_id, &order_id, now).await;
            }
        }
    }

    /// 查询回报落账（看门狗/恢复共用）：`on_order_update` + 委托要素审计。
    async fn query_order(
        &mut self,
        slot_idx: usize,
        signal_id: &str,
        order_id: &str,
        now: DateTime<Utc>,
    ) {
        let Some(gateway) = self.gateway.as_mut() else {
            return;
        };
        match gateway.query_fut(signal_id).await {
            Ok(snap) => {
                let u = LiveOrderUpdate {
                    order_id: order_id.to_string(),
                    exchange_order_id: snap.exchange_order_id.clone(),
                    state: snap.state,
                    filled_size: snap.filled_size,
                    avg_price: snap.avg_price,
                    filled_quantity_raw: snap.filled_quantity_raw.clone(),
                    signal_id: Some(signal_id.to_string()),
                    message: None,
                    fee: snap.fee,
                    at: now,
                };
                if let Some(live) = self.group.slots[slot_idx].parts.execution.as_live_mut() {
                    if let Err(e) = live.on_order_update(&u) {
                        error!(error = %e, "strategy live：查询回报落账失败");
                    }
                    if let Some(place) = &snap.place {
                        if let Err(e) =
                            live.manager_mut()
                                .record_place(order_id, Some(signal_id), place)
                        {
                            error!(error = %e, "strategy live：委托要素落账失败");
                        }
                    }
                }
            }
            Err(e) => warn!(
                signal_id, order_id, error = %e,
                "strategy live：在途查询失败，保留下次轮询"
            ),
        }
    }
}

/// 内核泵：控制面 → 定时器 → 数据面（单线程，禁并发 flush）。
async fn run_generation(
    mut k: StrategyLiveKernel,
    mut rx: mpsc::Receiver<RuntimeCommand>,
) -> std::io::Result<()> {
    let flush_every = Duration::from_secs(k.spec.state_flush_interval_secs.max(1));
    let check_every = Duration::from_secs(k.spec.position_check_interval_secs.max(1));
    let poll_every = Duration::from_secs(POLL_INTERVAL_SECS.max(1));
    let mut next_flush = tokio::time::Instant::now() + flush_every;
    let mut next_check = tokio::time::Instant::now() + check_every;
    let mut next_poll = tokio::time::Instant::now() + poll_every;

    info!(
        strategy_id = %k.spec.strategy_id,
        mode = %k.spec.mode,
        instruments = k.group.slots.len(),
        "strategy live 内核启动（recover 完成，进入事件泵）"
    );

    // 重启先查后发（T5 顺序断言对象）：装配期 recover_from_store 已完成，
    // 此处先在途恢复查询/幂等重提，完成后才进入 candle 消费循环。
    let startup_now = Utc::now();
    k.restart_recovery(startup_now).await;
    k.poll_in_flight_at(startup_now).await;

    // 启动首轮决策：用预热数据里的最后一根 base bar 立即评估一次，不必等
    // 下一个 base TF 闭合（空仓重启尤其重要；每实例只补一次）。
    match k.group.prime_decision_round() {
        Ok(Some(report)) if report.decision_round => {
            info!(
                at = %report.at,
                nav = report.nav,
                cash = report.cash,
                fills = report.fills.len(),
                actions = report.actions,
                rejected = report.rejected,
                "strategy live 启动首轮决策"
            );
            k.pump_submits().await;
        }
        Ok(_) => {}
        Err(e) => error!(error = %e, "strategy live 启动首轮决策失败（继续事件泵）"),
    }

    loop {
        // 1) 控制面：所有积压命令先处理完。
        let mut shutdown = false;
        loop {
            match rx.try_recv() {
                Ok(RuntimeCommand::Shutdown) => {
                    shutdown = true;
                    break;
                }
                Ok(RuntimeCommand::Reconcile) => k.reconcile_all(Utc::now()).await,
                Ok(RuntimeCommand::OrderUpdate(u)) => k.apply_order_update(*u),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    shutdown = true;
                    break;
                }
            }
        }
        if shutdown {
            break;
        }

        // 2) 定时器：周期对账 / 周期 flush（actor 线程单写者）。
        let now = tokio::time::Instant::now();
        if now >= next_flush {
            if let Err(e) = k.store.flush() {
                error!(error = %e, "strategy live：周期 flush 失败");
            }
            next_flush = now + flush_every;
        }
        if now >= next_check {
            k.reconcile_all(Utc::now()).await;
            next_check = now + check_every;
        }
        if now >= next_poll {
            k.poll_in_flight_at(Utc::now()).await;
            next_poll = now + poll_every;
        }

        // 3) 数据面：非阻塞对齐泵（与回测同一段 on_event 主循环）。
        match k.aligned.next_aligned_nonblocking() {
            Ok(AlignPoll::Ready(ev)) => {
                let out = k.group.on_event(*ev).map_err(std::io::Error::other)?;
                if let Some(report) = out {
                    if report.decision_round {
                        info!(
                            at = %report.at,
                            nav = report.nav,
                            cash = report.cash,
                            fills = report.fills.len(),
                            actions = report.actions,
                            rejected = report.rejected,
                            "strategy live 决策轮"
                        );
                    }
                }
                if k.group.is_paused() && !k.pause_noted {
                    k.pause_noted = true;
                    warn!(
                        strategy_id = %k.spec.strategy_id,
                        "strategy live：策略 panic 已暂停本组（不发新单）；恢复须重启进程走 recover 监督代（after_panic=true）"
                    );
                }
                k.pump_submits().await;
            }
            Ok(AlignPoll::Starved) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(AlignPoll::Done) => break,
            Err(e) => return Err(std::io::Error::other(e)),
        }
    }

    // 收口：flush 最后决策轮 → 同步成交 → 落盘。
    let summary = k.group.finish().map_err(std::io::Error::other)?;
    k.store.flush().map_err(std::io::Error::other)?;
    info!(
        strategy_id = %k.spec.strategy_id,
        bars = summary.bars_processed,
        fills = summary.fills.len(),
        sub_positions = summary.final_sub_positions.len(),
        "strategy live 收口"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_order_budget_rejects_venue_notional_above_single_position_budget() {
        assert!(validate_live_order_budget(200.0, 200.0).is_ok());
        let err = validate_live_order_budget(201.0, 200.0).unwrap_err();
        assert!(
            err.to_string().contains("超过单仓预算"),
            "错误需指明口径冲突: {err}"
        );
    }
}
