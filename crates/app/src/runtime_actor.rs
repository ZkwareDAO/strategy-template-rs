//! RuntimeActor —— 每策略实例的**单写者内核**（Phase 3 Step1 + Step2）
//!
//! Step2 后一个 task 独占：K 线事件流 + Feed 聚合 + 策略决策 + 信号内核 +
//! 仓位内核 + async 交易网关。成交同 task 直接入账，无 FillEvent 通道；
//! 仓位由策略**直读**（不再有每-bar oneshot 查询，B9 消解）。
//!
//! 命令通道仅保留三类外部输入：订单回报 / 重连对账 / 关闭。
//!
//! 生命周期：仓位 restore → 信号对账 → 历史回填+订阅 → 事件循环
//! （Closed bar 驱动：价格更新→直读仓位→决策→提交→入账）。
//! panic 由外层监督 task 捕获：工厂重建策略/网关与全部内核，重新订阅并走
//! 启动对账恢复；慢磁盘只影响本实例，不跨实例。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use strategy_core::candle::{Candle, Timeframe};
use strategy_core::order::OrderState;
use strategy_core::signal::{intents_from_action, Action, IntentCtx, IntentKind};
use strategy_core::strategy::{ClosedSource, Ctx, PositionView, Strategy};
use strategy_core::{FillEvent, Side, SignalIntent};
use strategy_data::csv_store::{csv_path, LiveCsvSink};
use strategy_data::feed::Feed;
use strategy_data::warmup;
use strategy_data::{KlineClient, KlineEvent};
use strategy_positions::{PositionManager, Side as PositionSide};
use strategy_signals::gateway::{
    AsyncOrderGateway, FailureKind, OrderPlace, RemotePosition, SubmitError,
};
use strategy_signals::manager::{RecoveryAction, SignalManager, SubmitRequest};
use strategy_store::Store;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// 实例静态规格（监督重启时复用）
#[derive(Clone)]
pub struct RuntimeSpec {
    /// 进入 CSV 路径与 position_id 的运行时名
    pub name: String,
    /// 策略类型名（进入 IntentCtx / signal_id）
    pub strategy_type: String,
    pub symbol: String,
    pub mode: String,
    pub state_dir: PathBuf,
    /// 1m K 线 CSV 根目录（live 预热打底 + 闭合帧实时追加）
    pub kline_data_dir: PathBuf,
    pub warmup_bars: usize,
    /// 状态定时全 Store flush 间隔（S6）；独立于 1s 到期重试 ticker。
    /// 0 视为缺省 60s（防御配置误填导致 interval panic）。
    pub state_flush_interval: Duration,
    /// 在途看门狗：意图创建后多少秒内不主动查（给 order_event 推送留宽限）
    pub poll_grace_secs: u64,
    /// 在途看门狗：两次主动查询的最小间隔（去重，秒）
    pub poll_interval_secs: u64,
    /// 远程仓位对账间隔（秒）：每 N 秒 get_positions 比对交易所侧真实仓位
    pub position_check_interval_secs: u64,
}

/// 看门狗生产默认：提交宽限 60s / 查询最小间隔 60s
pub const POLL_GRACE_SECS: u64 = 60;
/// 看门狗生产默认：查询最小间隔 60s
pub const POLL_INTERVAL_SECS: u64 = 60;

/// 远程仓位对账默认间隔（每分钟对每个代币查一次 get_positions）
pub const POSITION_CHECK_INTERVAL_SECS: u64 = 60;

/// 两数量是否在相对容差内相等（1e-9，对极小量给绝对下限）。
fn qty_close(a: f64, b: f64) -> bool {
    let tol = 1e-9_f64.max(1e-9 * b.abs());
    (a - b).abs() <= tol
}

/// 本地仓是否与远程仓一致：存在 instrument 相同、方向相同、数量容差内
/// 相等的远程元素。远程为空（已平仓）或数量不符（小仓可能不是本策略的）
/// 均判 false。
fn remote_matches_local(
    instrument: &str,
    side: PositionSide,
    remaining_size: f64,
    remote: &[RemotePosition],
) -> bool {
    let want_side = match side {
        PositionSide::Long => "BUY",
        PositionSide::Short => "SELL",
    };
    remote.iter().any(|r| {
        // deleted=true 是服务端当日已平仓行：必须排除，否则会把幽灵仓误判为存在
        !r.deleted
            && r.instrument == instrument
            && r.side.eq_ignore_ascii_case(want_side)
            && qty_close(r.quantity, remaining_size)
    })
}

/// 外部命令（仅订单回报 / 重连对账 / 关闭；其余全部在 bar 驱动内完成）
#[derive(Debug)]
pub enum RuntimeCommand {
    OrderUpdate {
        order_id: String,
        exchange_order_id: Option<String>,
        state: OrderState,
        filled_size: f64,
        avg_price: f64,
        filled_quantity_raw: Option<String>,
        /// 帧内 signal_id：回报早于 ack/绑定时按它找回信号
        signal_id: Option<String>,
        /// Rejected 帧交易所原文（识别 ReduceOnly 拒单→外部已平仓对账）
        message: Option<String>,
        /// 实际委托要素（受理回写审计）
        place: Option<OrderPlace>,
        event_time: DateTime<Utc>,
    },
    /// 交易 WS（重）连成功：在途信号先查后发对账
    Reconcile,
    /// 优雅关闭：处理完箱内命令后 flush 快照退出
    Shutdown,
}

#[derive(Clone)]
pub struct RuntimeHandle {
    tx: mpsc::Sender<RuntimeCommand>,
}

impl RuntimeHandle {
    #[allow(clippy::too_many_arguments)]
    pub async fn order_update(
        &self,
        order_id: String,
        exchange_order_id: Option<String>,
        state: OrderState,
        filled_size: f64,
        avg_price: f64,
        filled_quantity_raw: Option<String>,
        signal_id: Option<String>,
        message: Option<String>,
        place: Option<OrderPlace>,
        event_time: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::OrderUpdate {
                order_id,
                exchange_order_id,
                state,
                filled_size,
                avg_price,
                filled_quantity_raw,
                signal_id,
                message,
                place,
                event_time,
            })
            .await
            .map_err(|e| anyhow::anyhow!("RuntimeActor 已关闭: {e}"))
    }

    pub async fn reconcile(&self) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::Reconcile)
            .await
            .map_err(|e| anyhow::anyhow!("RuntimeActor 已关闭: {e}"))
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::Shutdown)
            .await
            .map_err(|e| anyhow::anyhow!("RuntimeActor 已关闭: {e}"))
    }

    /// 测试用：直接包裹一个命令发送端
    #[cfg(test)]
    pub(crate) fn for_test(tx: mpsc::Sender<RuntimeCommand>) -> Self {
        Self { tx }
    }
}

/// ack 后向 T7 注册 order_id → 本 Actor 的精确路由（B8）
pub struct RouteBind {
    pub order_id: String,
    pub actor: RuntimeHandle,
}

/// 监督重启工厂：每代产出全新网关与策略（内核由 Actor 自建）
pub type RuntimeFactory =
    Box<dyn FnMut() -> (Box<dyn AsyncOrderGateway>, Box<dyn Strategy + Send>) + Send>;

/// 监督启动：外层持稳定句柄；内层 panic（策略/网关/落盘）后整代重建——
/// 重新回填订阅、restore + 启动对账；收到 Shutdown 则 flush 后正常退出。
pub fn spawn_supervised(
    spec: RuntimeSpec,
    kline: Arc<dyn KlineClient>,
    store: Arc<Store>,
    mut make_runtime: RuntimeFactory,
    channel_cap: usize,
    route_tx: Option<mpsc::Sender<RouteBind>>,
) -> (RuntimeHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(channel_cap.max(1));
    let me = RuntimeHandle { tx };
    let stable = me.clone();
    let cap = channel_cap.max(1);
    let sup = tokio::spawn(async move {
        let mut outer_rx = rx;
        let mut generation: u32 = 0;
        'outer: loop {
            let (gateway, strategy) = make_runtime();
            let (inner_tx, inner_rx) = mpsc::channel(cap);
            // 每代新建 manager（重走启动对账）；Store 进程共享、panic 不丢内存表
            let signals = SignalManager::new(&spec.name, Arc::clone(&store), None);
            let positions = PositionManager::new(
                &spec.name,
                &spec.symbol,
                &spec.mode,
                Arc::clone(&store),
                &spec.state_dir,
            );
            let route = RouteCtx {
                tx: route_tx.clone(),
                me: stable.clone(),
            };
            let mut task = tokio::spawn(run_generation(
                spec.clone(),
                kline.clone(),
                signals,
                positions,
                gateway,
                strategy,
                inner_rx,
                route,
            ));
            let inner_tx = Some(inner_tx);
            loop {
                tokio::select! {
                    cmd = outer_rx.recv() => {
                        let Some(cmd) = cmd else { break 'outer };
                        let is_shutdown = matches!(cmd, RuntimeCommand::Shutdown);
                        if let Some(itx) = &inner_tx {
                            if itx.send(cmd).await.is_err() {
                                // 内层已退出：等 task 分支决定重启
                                continue;
                            }
                        }
                        if is_shutdown {
                            let _ = (&mut task).await;
                            break 'outer;
                        }
                    }
                    r = &mut task => match r {
                        Ok(()) => break 'outer,
                        Err(e) if e.is_panic() => {
                            generation += 1;
                            warn!(generation, error = %e,
                                "RuntimeActor panic，整代重建（重新订阅 + 启动对账恢复）");
                            tokio::time::sleep(Duration::from_millis(
                                200u64.saturating_mul(generation.min(6) as u64),
                            ))
                            .await;
                            break;
                        }
                        Err(e) => {
                            warn!(error = %e, "RuntimeActor 任务异常退出，不再重启");
                            break 'outer;
                        }
                    },
                }
            }
        }
    });
    (me, sup)
}

/// 单代 Actor 主体。
#[allow(clippy::too_many_arguments)]
async fn run_generation(
    spec: RuntimeSpec,
    kline: Arc<dyn KlineClient>,
    mut signals: SignalManager,
    mut positions: PositionManager,
    mut gateway: Box<dyn AsyncOrderGateway>,
    mut strategy: Box<dyn Strategy + Send>,
    mut rx: mpsc::Receiver<RuntimeCommand>,
    route: RouteCtx,
) {
    // 1) 仓位快照恢复
    if let Err(e) = positions.restore() {
        error!(error = %e, "仓位 restore 失败，继续走对账");
    }
    // 2) 信号对账 + 交叉对账（重提成功即注册路由）
    reconcile(&mut signals, &mut positions, &mut *gateway, &route).await;

    // 3) 历史预热（本地 CSV 打底 + 远程回填补缺）+ 实时订阅（每代重建，
    //    与历史→实时顺序衔接）。sink 把闭合 1m 持续追加回本地 CSV。
    let tfs = strategy.timeframes();
    // 主周期分钟数（tfs 最大者）：把 1m 预热根数折算成主周期根数，
    // 便于启动时一眼判断是否达到策略 min_rows 门槛
    let main_tf_mins = tfs.iter().map(|t| t.0).max().unwrap_or(1).max(1) as usize;
    let (hist, wstats) = warmup::load_warmup(
        &spec.kline_data_dir,
        kline.as_ref(),
        &spec.symbol,
        spec.warmup_bars,
    )
    .await;
    info!(
        symbol = %spec.symbol,
        local_1m = wstats.local_bars,
        remote_1m = wstats.remote_bars,
        remote_ok = wstats.remote_ok,
        persisted_new = wstats.persisted_new,
        total_1m = wstats.total_bars,
        main_tf_bars = wstats.total_bars / main_tf_mins,
        "历史预热完成（本地 CSV + 远程回填；main_tf_bars 为折合主周期根数）"
    );
    let mut feed = Feed::new(tfs, 2000);
    feed.preload(&hist);

    let mut csv_sink = match LiveCsvSink::open(csv_path(
        &spec.kline_data_dir,
        &spec.symbol,
        Timeframe::M1,
    )) {
        Ok(s) => Some(s),
        Err(e) => {
            warn!(symbol = %spec.symbol, error = %e, "打开 1m CSV 追加器失败，本次不持久化 K 线");
            None
        }
    };

    let mut stream = kline.attach(&spec.symbol, Timeframe::M1);

    // 在途看门狗轮询状态（内存态，每代重建）
    let mut poll_watch = PollWatch::default();

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    // S6：时间驱动的全 Store flush——独立 interval，**不复用 1s 重试 ticker**
    // （重试节奏与落盘节奏解耦）。每监督代重建；首个即时 tick 先消费掉，
    // 避免启动即刷；硬崩溃丢失窗口从"1000 事件"收紧到本间隔。
    let flush_period = if spec.state_flush_interval.is_zero() {
        Duration::from_secs(60)
    } else {
        spec.state_flush_interval
    };
    let mut flush_ticker = tokio::time::interval(flush_period);
    flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    flush_ticker.tick().await;

    // 远程仓位对账 ticker：每 N 秒查 get_positions，独立于 bar/flush 节奏；
    // 最小 1s（防御配置误填 0）。每监督代重建，首个即时 tick 消费掉。
    let check_period = Duration::from_secs(spec.position_check_interval_secs.max(1));
    let mut position_check_ticker = tokio::time::interval(check_period);
    position_check_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    position_check_ticker.tick().await;

    enum Selected {
        Bar(Option<Result<KlineEvent, RecvError>>),
        Cmd(Option<RuntimeCommand>),
        Tick,
        FlushTick,
        PositionCheck,
    }
    loop {
        let picked = tokio::select! {
            ev = stream.rx.recv() => Selected::Bar(Some(ev)),
            cmd = rx.recv() => Selected::Cmd(cmd),
            _ = ticker.tick() => Selected::Tick,
            _ = flush_ticker.tick() => Selected::FlushTick,
            _ = position_check_ticker.tick() => Selected::PositionCheck,
        };
        match picked {
            Selected::Bar(Some(Ok(KlineEvent::Closed(bar)))) => {
                debug!(
                    symbol = %spec.symbol, bar_time = %bar.candle.open_time,
                    o = bar.candle.open, h = bar.candle.high, l = bar.candle.low,
                    c = bar.candle.close, v = bar.candle.volume,
                    "Actor 收到闭合 1m（KlineEvent::Closed）"
                );
                // 闭合 1m 先追加本地 CSV（水位去重），再驱动策略
                if let Some(sink) = csv_sink.as_mut() {
                    match sink.push(&bar.candle) {
                        Ok(true) => debug!(symbol = %spec.symbol, "Actor：闭合 1m 已追加本地 CSV"),
                        Ok(false) => debug!(symbol = %spec.symbol, "Actor：CSV 水位去重跳过"),
                        Err(e) => warn!(symbol = %spec.symbol, error = %e, "1m K 线追加 CSV 失败"),
                    }
                }
                if let Err(e) = drive_bar(
                    &spec,
                    &mut signals,
                    &mut positions,
                    &mut *gateway,
                    &route,
                    &mut feed,
                    &mut *strategy,
                    bar.candle,
                    &mut poll_watch,
                )
                .await
                {
                    error!(symbol = %spec.symbol, error = %e, "bar 驱动异常");
                }
                flush_if_needed(&mut signals, false);
            }
            Selected::Bar(Some(Ok(KlineEvent::Reconnected))) => {
                if let Err(e) =
                    resync_backfill(&spec, kline.as_ref(), &mut csv_sink, &mut feed).await
                {
                    warn!(symbol = %spec.symbol, error = %e, "重连后历史补缺失败");
                }
            }
            Selected::Bar(Some(Err(RecvError::Lagged(n)))) => {
                // B12（共享 WS）：本代币消费落后（如盘 IO 卡住），其他 symbol 不受
                // 影响（broadcast 语义）；丢弃积压窗口，走与重连相同的补缺路径
                // 自愈（Feed 水位幂等去重）。
                warn!(symbol = %spec.symbol, skipped = n, "kline 消费落后，backfill 自愈");
                if let Err(e) =
                    resync_backfill(&spec, kline.as_ref(), &mut csv_sink, &mut feed).await
                {
                    warn!(symbol = %spec.symbol, error = %e, "自愈补缺失败");
                }
            }
            Selected::Bar(Some(Ok(KlineEvent::Tick(_)))) => {}
            // 分发器消失（共享客户端被 drop）或通道关闭：结束本代，交监督层决定
            Selected::Bar(Some(Err(RecvError::Closed))) | Selected::Bar(None) => break,
            Selected::Cmd(Some(cmd)) => match cmd {
                RuntimeCommand::OrderUpdate {
                    order_id,
                    exchange_order_id,
                    state,
                    filled_size,
                    avg_price,
                    filled_quantity_raw,
                    signal_id,
                    message,
                    place,
                    event_time,
                } => {
                    debug!(%order_id, ?exchange_order_id, ?state, filled_size, avg_price, ?place, "Actor 收到命令 OrderUpdate（T7 路由的订单回报）");
                    if let Ok(Some(fill)) = signals.handle_order_update(
                        &order_id,
                        exchange_order_id.as_deref(),
                        state,
                        filled_size,
                        avg_price,
                        filled_quantity_raw.clone(),
                        signal_id.as_deref(),
                        message.as_deref(),
                        event_time,
                    ) {
                        match apply_fill(&mut positions, &fill) {
                            Err(e) => error!(error = %e, "订单回报成交入账失败"),
                            Ok(true) => {
                                // 平仓/部分平仓进度立即落盘（等不到 60s 定时 flush）
                                signals.request_flush();
                                flush_if_needed(&mut signals, false);
                            }
                            Ok(false) => {}
                        }
                    }
                    // 实际委托要素落账（纯审计，核对真实下了什么单）
                    if let Some(place) = &place {
                        if let Err(e) = signals.record_place(&order_id, signal_id.as_deref(), place)
                        {
                            error!(error = %e, "委托要素落账失败");
                        }
                    }
                    // ReduceOnly 拒单 ≡ 交易所已无仓位：外部平仓对账
                    external_close_on_reduce_only(
                        &mut signals,
                        &mut positions,
                        &order_id,
                        signal_id.as_deref(),
                        message.as_deref(),
                        event_time,
                    );
                }
                RuntimeCommand::Reconcile => {
                    debug!("Actor 收到命令 Reconcile（交易 WS 重连：先查后发对账）");
                    reconcile(&mut signals, &mut positions, &mut *gateway, &route).await;
                }
                RuntimeCommand::Shutdown => {
                    debug!("Actor 收到命令 Shutdown，排空箱内命令后准备退出");
                    break;
                }
            },
            Selected::Cmd(None) => break,
            Selected::Tick => {
                retry_due(&mut signals, &mut positions, &mut *gateway, &route).await;
                flush_if_needed(&mut signals, false);
            }
            Selected::FlushTick => {
                // 时间驱动全 Store flush（去重：take 顺带消费挂起的阈值请求，
                // 同一轮阈值+到点只落一次盘）
                flush_if_needed(&mut signals, true);
            }
            Selected::PositionCheck => {
                check_remote_position(&spec, &mut signals, &mut positions, &mut *gateway).await;
            }
        }
    }
    // 优雅关闭：全 Store flush 交阻塞池并等其完成（行更新此前只在内存，
    // 关停必须落当日快照；flush 不持行锁跨线程，多实例共享无死锁）。
    let store = signals.store();
    let join = tokio::task::spawn_blocking(move || store.flush());
    match join.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => error!(error = %e, "关闭时 Store flush 失败"),
        Err(e) => error!(error = %e, "关闭时 flush 任务异常"),
    }
}

struct FeedSource<'a> {
    feed: &'a Feed,
}
impl<'a> ClosedSource for FeedSource<'a> {
    fn closed(&self, tf: Timeframe) -> Option<&[Candle]> {
        self.feed.closed_map().get(&tf).map(|v| v.as_slice())
    }
    fn last_price(&self) -> Option<f64> {
        self.feed.last_price()
    }
}

/// 补缺窗口 + 追加本地 CSV + Feed 重载（重连与 Lagged 自愈共用；Feed 水位幂等去重）
async fn resync_backfill(
    spec: &RuntimeSpec,
    kline: &dyn KlineClient,
    sink: &mut Option<LiveCsvSink>,
    feed: &mut Feed,
) -> std::io::Result<()> {
    let hist = kline
        .backfill(&spec.symbol, Timeframe::M1, spec.warmup_bars)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    // 补缺窗口同时追加 CSV（sink 水位自动跳过已存在的帧）
    if let Some(sink) = sink.as_mut() {
        for c in &hist {
            if let Err(e) = sink.push(c) {
                warn!(symbol = %spec.symbol, error = %e, "补缺 K 线追加 CSV 失败");
                break;
            }
        }
    }
    feed.preload(&hist);
    Ok(())
}

/// 一根闭合 bar 的完整决策链（原 T2 drive_closed，Step2 并入同 task）：
/// 价格更新 → 直读仓位 → 策略决策 → 止损/意图（提交→入账）。
#[allow(clippy::too_many_arguments)]
async fn drive_bar(
    spec: &RuntimeSpec,
    signals: &mut SignalManager,
    positions: &mut PositionManager,
    gateway: &mut dyn AsyncOrderGateway,
    route: &RouteCtx,
    feed: &mut Feed,
    strategy: &mut (dyn Strategy + Send),
    candle: Candle,
    poll: &mut PollWatch,
) -> std::io::Result<()> {
    let event = feed.on_closed_1m(candle);
    if event.newly_closed.is_empty() {
        debug!(bar_time = %candle.open_time, "drive_bar：Feed 无新闭合，直接返回");
        return Ok(());
    }
    debug!(
        bar_time = %candle.open_time,
        newly_closed = ?event.newly_closed.iter().map(|t| t.0).collect::<Vec<_>>(),
        "drive_bar：Feed 聚合出新闭合，开始驱动（on_price → 策略 → 信号）"
    );
    positions.on_price(candle.high, candle.low)?;

    // 止损冷却日从 cooldown 表读（启动恢复 + 跨日 upsert 后即时可见；
    // Ctx 内部按本根 K 线日期比对，跨日自动失效）。
    let cooldown = positions.stop_loss_cooldown();
    let pos = positions.position();
    debug!(
        symbol = spec.symbol, bar_time = %candle.open_time,
        has_position = pos.is_some(), cooldown = ?cooldown,
        "drive_bar：直读当前仓位（同 task，零消息），{}",
        if pos.is_some() { "将走 on_realtime_tick 出场逻辑" } else { "将走 on_candle 入场逻辑" }
    );
    let view: Option<PositionView> = pos.as_ref().map(|p| PositionView {
        side: match p.side {
            strategy_positions::Side::Long => Side::Long,
            strategy_positions::Side::Short => Side::Short,
        },
        entry_price: p.entry_price,
        stop_price: p.stop_price,
        peak_price: p.peak_price,
        max_pnl_pct: p.max_pnl_pct,
        min_pnl_pct: p.min_pnl_pct,
        exit_active: p.exit_active,
        exit_price: p.exit_price,
    });

    let c = event.candle_1m;
    let event_time = c.open_time;
    let (position_id, position_side, position_size, position_size_raw) = match &pos {
        Some(p) => (
            Some(p.position_id.clone()),
            Some(match p.side {
                strategy_positions::Side::Long => Side::Long,
                strategy_positions::Side::Short => Side::Short,
            }),
            // 可平量口径与 apply_close_fill 一致：remaining 优先，回落 total
            Some(if p.remaining_size > 0.0 {
                p.remaining_size
            } else {
                p.total_size
            }),
            // 平仓帧 quantity：剩余可平量 raw（部分成交后已精确递减）；
            // 旧记录无该字段时回落开仓 raw
            p.remaining_size_raw
                .as_deref()
                .or(p.entry_size_raw.as_deref()),
        ),
        None => (None, None, None, None),
    };

    enum Out {
        Stop(f64),
        Exit { active: bool, price: f64 },
        Intents(Vec<SignalIntent>),
    }
    // Ctx 非 Send：策略调用与意图构造全部限制在此同步块内
    let outputs: Vec<Out> = {
        let source = FeedSource { feed: &*feed };
        let ctx = Ctx::new(&source, c.close, c, event_time, view, cooldown.clone());
        let actions = if pos.is_some() {
            debug!(
                symbol = spec.symbol,
                "调用策略回调 on_realtime_tick（持仓中）"
            );
            strategy.on_realtime_tick(&ctx)
        } else {
            debug!(symbol = spec.symbol, "调用策略回调 on_candle（空仓）");
            strategy.on_candle(&ctx)
        };
        debug!(
            symbol = spec.symbol,
            actions = actions.len(),
            "策略本轮执行完毕，产出动作数如上"
        );
        let ictx = IntentCtx {
            strategy_type: &spec.strategy_type,
            symbol: &spec.symbol,
            event_time,
            position_id: position_id.as_deref(),
            position_side,
            position_size,
            position_size_raw,
        };
        actions
            .into_iter()
            .map(|action| match action {
                Action::UpdateStop { stop_price } => Out::Stop(stop_price),
                Action::UpdateExit {
                    exit_active,
                    exit_price,
                } => Out::Exit {
                    active: exit_active,
                    price: exit_price,
                },
                other => Out::Intents(intents_from_action(&other, &ictx)),
            })
            .collect()
    };

    for out in outputs {
        match out {
            Out::Stop(stop_price) => {
                debug!(
                    symbol = spec.symbol,
                    stop_price, "动作分流：UpdateStop → 同 task 更新仓位止损（不下单）"
                );
                positions.update_stop(stop_price)?
            }
            Out::Exit { active, price } => {
                debug!(
                    symbol = spec.symbol,
                    exit_active = active,
                    exit_price = price,
                    "动作分流：UpdateExit → 同 task 回写持仓离场状态（不下单）"
                );
                positions.update_exit(active, price)?
            }
            Out::Intents(intents) => {
                debug!(
                    symbol = spec.symbol,
                    n = intents.len(),
                    "动作分流：{}个意图 → 进信号内核 prepare",
                    intents.len()
                );
                for intent in intents {
                    let Some(req) = signals.receive_intent_prepare(intent)? else {
                        debug!(
                            symbol = spec.symbol,
                            "意图被信号内核过滤（去重/冲突/校验），跳过"
                        );
                        continue;
                    };
                    submit_apply(signals, positions, gateway, route, req).await;
                }
            }
        }
    }

    // 闭合 bar 末尾：在途看门狗主动查询卡住的订单（受宽限/间隔约束）
    poll_in_flight(
        signals,
        positions,
        gateway,
        poll,
        chrono::Duration::seconds(spec.poll_grace_secs as i64),
        chrono::Duration::seconds(spec.poll_interval_secs.max(1) as i64),
        Utc::now(),
    )
    .await;
    Ok(())
}

/// 路由注册上下文（B8）：提交成功后把 order_id 绑定到本 Actor。
#[derive(Clone)]
struct RouteCtx {
    tx: Option<mpsc::Sender<RouteBind>>,
    me: RuntimeHandle,
}
impl RouteCtx {
    /// order_id 须在 await 前同步取出（SignalManager 非 Sync，不能持引跨 await）。
    async fn bind_order(&self, order_id: Option<String>) {
        let (Some(tx), Some(order_id)) = (&self.tx, order_id) else {
            return;
        };
        if let Err(e) = tx
            .send(RouteBind {
                order_id,
                actor: self.me.clone(),
            })
            .await
        {
            warn!(error = %e, "order_id 路由注册失败");
        }
    }
}

/// 对账（启动/Reconcile 同路径）：recover 动作先查后发/幂等重提；
/// 交叉对账：信号 Filled 但仓位缺失 → query 补成交。
async fn reconcile(
    signals: &mut SignalManager,
    positions: &mut PositionManager,
    gateway: &mut dyn AsyncOrderGateway,
    route: &RouteCtx,
) {
    let now = Utc::now();
    let actions = match signals.recover() {
        Ok(a) => a,
        Err(e) => {
            error!(error = %e, "对账：事件重建失败，跳过本轮");
            return;
        }
    };
    for action in actions {
        match action {
            RecoveryAction::Resubmit { signal_id } => {
                let req = signals.submit_request(&signal_id, now);
                submit_apply(signals, positions, gateway, route, req).await;
            }
            RecoveryAction::Query {
                signal_id,
                order_id,
            } => {
                match gateway.query_fut(&signal_id).await {
                    Ok(snap) => {
                        if let Ok(Some(fill)) = signals.handle_order_update(
                            &order_id,
                            snap.exchange_order_id.as_deref(),
                            snap.state,
                            snap.filled_size,
                            snap.avg_price,
                            snap.filled_quantity_raw.clone(),
                            Some(&signal_id),
                            None,
                            now,
                        ) {
                            // 对账重建的成交：冷却已在 ManagedStop/cooldown
                            // 写入，不覆盖；失败不影响下面 place 审计落账
                            if let Err(e) = apply_fill(positions, &fill) {
                                error!(error = %e, order_id, "对账成交入账失败");
                            }
                        }
                        if let Some(place) = &snap.place {
                            if let Err(e) = signals.record_place(&order_id, Some(&signal_id), place)
                            {
                                error!(error = %e, "对账委托要素落账失败");
                            }
                        }
                    }
                    Err(e) => warn!(
                        signal_id, order_id, error = %e,
                        "对账查询失败，保留下次对账"
                    ),
                }
            }
        }
    }

    // 交叉对账：仓位缺失而信号存在 Filled Open 订单 → query 补成交
    if positions.position().is_none() {
        for (sid, oid) in signals.filled_open_orders() {
            let snap = match gateway.query_fut(&sid).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(order_id = oid, error = %e, "交叉对账查询失败");
                    continue;
                }
            };
            if snap.state != OrderState::Filled {
                continue;
            }
            let Some(intent) = signals.intent_for_order(&oid) else {
                continue;
            };
            let is_stop = intent.stop_order_id.is_some();
            let reason = if is_stop { "stop_loss" } else { "live_close" };
            // 实际委托要素审计落账（须在 FillEvent 移走 sid 之前）
            if let Some(place) = &snap.place {
                if let Err(e) = signals.record_place(&oid, Some(&sid), place) {
                    error!(error = %e, "交叉对账委托要素落账失败");
                }
            }
            let fill = FillEvent::new(
                sid,
                oid.clone(),
                intent.symbol,
                intent.direction,
                snap.filled_size,
                snap.avg_price,
                0.0,
                now,
            )
            .with_exit(is_stop, reason)
            .with_initial_stop(intent.stop_price)
            // 补的是开仓成交（无仓位 + Filled Open 单）
            .with_fill_meta(
                intent.kind,
                intent.position_id.clone(),
                snap.filled_quantity_raw.clone(),
            );
            if let Err(e) = apply_fill(positions, &fill) {
                error!(error = %e, order_id = oid, "交叉对账补成交失败");
            }
        }
    }
}

/// 在途看门狗轮询状态：signal_id → 上次主动查询时刻（内存态，每代重建）。
#[derive(Default)]
struct PollWatch {
    last_poll: HashMap<String, DateTime<Utc>>,
}

impl PollWatch {
    /// 本轮是否应查询该单：距意图创建已过宽限，且距上次查询 ≥ min_interval。
    fn is_due(
        &self,
        signal_id: &str,
        created: DateTime<Utc>,
        now: DateTime<Utc>,
        grace: chrono::Duration,
        min_interval: chrono::Duration,
    ) -> bool {
        if now - created < grace {
            return false;
        }
        if let Some(t) = self.last_poll.get(signal_id) {
            if now - *t < min_interval {
                return false;
            }
        }
        true
    }

    fn mark(&mut self, signal_id: &str, now: DateTime<Utc>) {
        self.last_poll.insert(signal_id.into(), now);
    }
}

/// 在途看门狗：每根闭合 bar 末尾，对 Submitted/Accepted 且过宽限、到查询
/// 间隔的单主动 get_order。补救"连接存活但 order_event 帧丢失"导致的永久
/// 卡态。查询失败仅 warn 保留下次；查询本身走同一 WS，受 request_timeout 约束。
#[allow(clippy::too_many_arguments)]
async fn poll_in_flight(
    signals: &mut SignalManager,
    positions: &mut PositionManager,
    gateway: &mut dyn AsyncOrderGateway,
    poll: &mut PollWatch,
    grace: chrono::Duration,
    min_interval: chrono::Duration,
    now: DateTime<Utc>,
) {
    for (signal_id, cid, created) in signals.in_flight_orders() {
        if !poll.is_due(&signal_id, created, now, grace, min_interval) {
            continue;
        }
        poll.mark(&signal_id, now);
        debug!(
            signal_id = %signal_id, order_id = %cid,
            "在途看门狗：主动查询订单状态（get_order）"
        );
        match gateway.query_fut(&signal_id).await {
            Ok(snap) => {
                if let Ok(Some(fill)) = signals.handle_order_update(
                    &cid,
                    snap.exchange_order_id.as_deref(),
                    snap.state,
                    snap.filled_size,
                    snap.avg_price,
                    snap.filled_quantity_raw.clone(),
                    Some(&signal_id),
                    None,
                    now,
                ) {
                    match apply_fill(positions, &fill) {
                        Err(e) => error!(error = %e, "看门狗成交入账失败"),
                        Ok(true) => signals.request_flush(),
                        Ok(false) => {}
                    }
                }
                if let Some(place) = &snap.place {
                    if let Err(e) = signals.record_place(&cid, Some(&signal_id), place) {
                        error!(error = %e, "看门狗委托要素落账失败");
                    }
                }
            }
            Err(e) => warn!(
                order_id = %cid, error = %e,
                "看门狗查询失败，保留下次轮询"
            ),
        }
    }
}

/// 远程仓位对账：查 trading-service 仓位视图，与本地持仓三要素
///（标的/方向/当前持有量）比对。本地有仓但远程无匹配——
/// 远程为空（已平仓）或数量/方向不符（小仓可能不是本策略的）——
/// 按 external_close 关闭幽灵仓。查询失败 **fail-open**：仅 warn 保留，
/// 绝不把超时/错误误判为无仓错杀。
///
/// **paper 模式跳过**：本地撮合不涉远程仓位，`positions_fut` 返回空
/// 会被误判为幽灵仓错杀（U11）。
#[allow(clippy::too_many_arguments)]
async fn check_remote_position(
    spec: &RuntimeSpec,
    signals: &mut SignalManager,
    positions: &mut PositionManager,
    gateway: &mut dyn AsyncOrderGateway,
) {
    if spec.mode != "live" {
        debug!(symbol = %spec.symbol, mode = %spec.mode, "跳过远程仓位对账（非 live 模式）");
        return;
    }
    let Some(local) = positions.position() else {
        // 本地无仓：不查询也不采纳远程仓（不是本策略开的，不重建）
        return;
    };
    let want_side = match local.side {
        PositionSide::Long => "long",
        PositionSide::Short => "short",
    };
    match gateway.positions_fut(&spec.symbol, want_side).await {
        Ok(remote) => {
            if remote_matches_local(&spec.symbol, local.side, local.remaining_size, &remote) {
                debug!(
                    symbol = %spec.symbol,
                    local_size = local.remaining_size,
                    remote = ?remote,
                    "远程仓位对账：三要素匹配，持仓保留"
                );
                return;
            }
            warn!(
                symbol = %spec.symbol,
                local_size = local.remaining_size,
                remote = ?remote,
                "远程仓位对账：交易所侧无匹配仓，判定已平仓，本地按 external_close 关闭"
            );
            match positions.external_close(Utc::now()) {
                Ok(_) => {
                    signals.request_flush();
                    flush_if_needed(signals, false);
                }
                Err(e) => error!(error = %e, "远程对账 external_close 失败"),
            }
        }
        Err(e) => warn!(
            symbol = %spec.symbol, error = %e,
            "远程仓位查询失败，fail-open 保留本地仓（下次再查）"
        ),
    }
}

/// ReduceOnly 拒单对账：交易所明确回 `ReduceOnly Order is rejected` 意味着
/// 仓位已不存在（多为外部手动平仓/上一轮 reduce_only 已成交）。若该单是平仓
/// 意图，把本地幽灵仓位按 external_close 关闭并立即落盘；其他拒单（精度/
/// 风控等）不在此列——仓位可能仍敞着。
#[allow(clippy::too_many_arguments)]
fn external_close_on_reduce_only(
    signals: &mut SignalManager,
    positions: &mut PositionManager,
    order_id: &str,
    signal_id: Option<&str>,
    message: Option<&str>,
    now: DateTime<Utc>,
) {
    if !message.is_some_and(|m| m.contains("ReduceOnly")) {
        return;
    }
    let intent = signals
        .intent_for_order(order_id)
        .or_else(|| signal_id.and_then(|sid| signals.intent_for_signal(sid)));
    let Some(intent) = intent else {
        warn!(
            order_id,
            "ReduceOnly 拒单：找不到对应信号意图，跳过外部平仓对账"
        );
        return;
    };
    if intent.kind != IntentKind::Close {
        return;
    }
    match positions.external_close(now) {
        Ok(Some(_)) => {
            signals.request_flush();
            flush_if_needed(signals, false);
        }
        Ok(None) => warn!(order_id, "ReduceOnly 拒单：本地已无仓位，无需外部平仓对账"),
        Err(e) => error!(order_id, error = %e, "ReduceOnly 拒单外部平仓对账失败"),
    }
}

/// 到期重试（1s ticker）：due 列表逐个 async 提交、成交直接入账。
async fn retry_due(
    signals: &mut SignalManager,
    positions: &mut PositionManager,
    gateway: &mut dyn AsyncOrderGateway,
    route: &RouteCtx,
) {
    let now = Utc::now();
    for req in signals.due_retries(now) {
        submit_apply(signals, positions, gateway, route, req).await;
    }
}

/// 提交一次：await 网关 → 回交内核 → 注册路由 → 就绪成交直接入账。
async fn submit_apply(
    signals: &mut SignalManager,
    positions: &mut PositionManager,
    gateway: &mut dyn AsyncOrderGateway,
    route: &RouteCtx,
    req: SubmitRequest,
) {
    let now = req.event_time;
    let signal_id = req.signal_id.clone();
    // 平仓守卫：平仓意图必须当前有仓位且带精确成交量（意图层已拦截无仓
    // Close，此处覆盖信号在途期间仓位被平掉等竞态），不满足则不发送
    if req.intent.position_id.is_some()
        && (positions.position().is_none() || req.intent.size <= 0.0)
    {
        warn!(
            signal_id = %req.intent.id,
            has_position = positions.position().is_some(),
            size = req.intent.size,
            "平仓条件不成立（无仓位/无精确成交量），不发送 place_order"
        );
        if let Err(e) = signals.apply_submit_outcome(
            req,
            Err(SubmitError::rejected("平仓时无有效仓位/成交量，未发送")),
        ) {
            error!(error = %e, "平仓守卫落盘失败");
        }
        return;
    }
    let outcome = gateway.submit_fut(&req.intent).await;
    // 拒单原文在 req 被消费前取出：ReduceOnly 拒单需做外部平仓对账
    let rejected_msg = match &outcome {
        Err(e) if e.kind == FailureKind::Rejected => Some(e.message.clone()),
        _ => None,
    };
    let is_close = req.intent.kind == IntentKind::Close;
    if let Err(e) = signals.apply_submit_outcome(req, outcome) {
        error!(error = %e, "提交结果落盘失败");
        return;
    }
    let order_id = signals.order_id_of(&signal_id);
    if is_close {
        external_close_on_reduce_only(
            signals,
            positions,
            order_id.as_deref().unwrap_or(""),
            Some(&signal_id),
            rejected_msg.as_deref(),
            now,
        );
    }
    route.bind_order(order_id).await;
    for rf in gateway.drain_ready_fills() {
        match signals.apply_ready_fill(rf, now) {
            Ok(Some(fill)) => match apply_fill(positions, &fill) {
                Err(e) => error!(error = %e, "就绪成交入账失败"),
                Ok(true) => signals.request_flush(),
                Ok(false) => {}
            },
            Ok(None) => {}
            Err(e) => error!(error = %e, "就绪成交内核处理失败"),
        }
    }
}

/// B10/S6：阈值标志（每 1000 事件）或定时 tick 任一到达 → 全 Store flush
/// 移入阻塞池（标志在内核同步路径上置位，微秒级）。`timed` 分支先 take
/// 阈值标志：同一轮两者都就绪时只 flush 一次。flush 失败仅 error!，不杀 Actor
/// （当日既有快照 + 关停 flush + 启动恢复兜底）。flush 落所有已注册表的当日
/// 快照；多实例共享同一 Store 时重复 flush 仅重复覆盖同一文件，无死锁
/// （flush 不持行锁跨线程等待）。
fn flush_if_needed(signals: &mut SignalManager, timed: bool) {
    let threshold = signals.take_flush_requested();
    if !threshold && !timed {
        return;
    }
    let store = signals.store();
    tokio::task::spawn_blocking(move || {
        if let Err(e) = store.flush() {
            error!(error = %e, "Store flush 写盘失败（可由当日快照/恢复兜底）");
        }
    });
}

/// 成交 → 仓位内核（无仓开、同向加、反向平；止损语义经 B6 字段）。
/// 返回 true = 发生平仓/部分平仓（调用方应立即请求 flush 落盘进度）。
fn apply_fill(m: &mut PositionManager, fill: &FillEvent) -> std::io::Result<bool> {
    let Some(p) = m.position() else {
        // fail-closed：平仓语义成交到达时无仓位 → 报错丢弃，绝不反手开新仓
        if fill.position_id.is_some() {
            return Err(std::io::Error::other(format!(
                "平仓成交（signal={}）到达时无仓位，丢弃，不反手开仓",
                fill.signal_id
            )));
        }
        let stop = fill.initial_stop.unwrap_or(fill.filled_price * 0.98);
        return m
            .apply_open_fill(fill, fill.event_time, stop, 0.0)
            .map(|_| false);
    };
    let fill_long = fill.direction == Side::Long;
    let same = matches!(
        (p.side, fill_long),
        (strategy_positions::Side::Long, true) | (strategy_positions::Side::Short, false)
    );
    if same {
        m.apply_add_fill(fill)?;
        Ok(false)
    } else {
        m.apply_close_fill(
            fill,
            fill.is_stop_loss,
            fill.exit_reason.as_deref().unwrap_or("live_close"),
        )?;
        Ok(true)
    }
}

#[cfg(test)]
mod match_tests {
    use super::*;
    use strategy_signals::gateway::RemotePosition;

    fn rp(instrument: &str, side: &str, quantity: f64, deleted: bool) -> RemotePosition {
        RemotePosition {
            instrument: instrument.into(),
            side: side.into(),
            quantity,
            avg_price: 0.0,
            deleted,
        }
    }

    #[test]
    fn matched_when_instrument_side_and_quantity_align() {
        let remote = [rp("BTCUSDT", "SELL", 0.002, false)];
        assert!(remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &remote
        ));
        // side 大小写容忍
        let remote = [rp("BTCUSDT", "sell", 0.002, false)];
        assert!(remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &remote
        ));
    }

    #[test]
    fn deleted_true_means_position_closed_even_if_other_fields_match() {
        // 实盘 bug：服务端元素 quantity/side/instrument 全一致但 deleted=true
        // （当日已平仓行），不得判匹配
        let remote = [rp("BTCUSDT", "SELL", 0.002, true)];
        assert!(!remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &remote
        ));
        // 已平行与活跃行同时存在：以活跃行为准
        let remote = [
            rp("BTCUSDT", "SELL", 0.002, true),
            rp("BTCUSDT", "SELL", 0.002, false),
        ];
        assert!(remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &remote
        ));
    }

    #[test]
    fn empty_remote_means_position_closed() {
        assert!(!remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &[]
        ));
    }

    #[test]
    fn smaller_remote_quantity_means_closed() {
        // 远程 1、本地 2：数量不符 → 已平仓（小仓可能不是本策略的）
        let remote = [rp("BTCUSDT", "SELL", 1.0, false)];
        assert!(!remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            2.0,
            &remote
        ));
    }

    #[test]
    fn opposite_side_or_other_instrument_does_not_match() {
        let remote = [
            rp("BTCUSDT", "BUY", 0.002, false),
            rp("ETHUSDT", "SELL", 0.002, false),
        ];
        assert!(!remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &remote
        ));
    }

    #[test]
    fn float_tolerance_does_not_reject_near_equal_quantity() {
        let remote = [rp("BTCUSDT", "SELL", 2143.0000000001, false)];
        assert!(remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            2143.0,
            &remote
        ));
    }
}
