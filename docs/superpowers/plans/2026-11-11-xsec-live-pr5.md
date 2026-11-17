# 横截面策略实盘接入（PR-5）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `CrossSectionalRunnerGroup` 以 live / paper_trading 模式跑通 trading-service 单连接链路：universe 全标的订阅合并、订单三级确认、周期 `remote_positions` 对账、重启 recover（含 `peak_unrealized_pnl`），并保持回测/实盘同管道公理。

**Architecture:** engine 侧只加三样非破坏性接缝——`BarEventSource::try_next_event`（非阻塞轮询 + `MarketEvent::SourceIdle`）、`AlignedEventSource` 的非阻塞取事件（starved 语义）、`LiveExecution` 本地镜像账本（`account_snapshot` 从 None 变为「明示 local 镜像」）与 `CrossSectionalRunnerGroup::reconcile_remote`。app 侧新增 `xsec_live_actor`（监督代 + 双平面：数据面 per-slot candle channel、控制面 `RuntimeCommand` mpsc），`bootstrap::run_live` 增横截面分派；订单/回报/看门狗全部复用既有 `AsyncOrderGateway`/`SignalManager`/`RouteBind` 机制。mock（`crates/broker`）已具备多标的 K 线回放与净持仓镜像，演练不新增协议帧。

**Tech Stack:** Rust tokio（current_thread runtime per 监督代，与 `spawn_spi_live` 同型）、trading_service WS 客户端、统一 Store、broker mock。

**Spec:** `deliverables/gstack/feature-dev-cross-sectional-strategy-2026-11-09.md`（§10 订阅与实盘链路、§16 部署拓扑、§12 PR-5、§13-2 历史仓位、§4.4 nav 来源优先级）
**上游计划（已交付）:** `docs/superpowers/plans/2026-11-10-cross-sectional-strategy.md`（PR-1～PR-4 回测端到端；本计划复用其全部类型与运行时）

## Global Constraints

- G3 公理：实盘/回测差异只允许在 `BarEventSource` / `ExecutionPort` 两端口；`CrossSectionalRunnerGroup::flush_round` 内零 `if mode`。
- `ExecutionPort` trait 形状不变（`account_snapshot` 已有默认签名，LiveExecution 从 `None` 改为 Some(mirror) 属实现侧行为，镜像必须日志声明 `nav_source=local_mirror`——服务端当前契约无权益帧，**不伪造 broker 权益**）。
- 不新增 wire 帧：`contracts/trading-service.md` v0.3 的 `subscribe_kline(symbols[])`、`get_positions`、`order_update` 原样使用；`SourceIdle` 是进程内事件，不上线。
- 一实例 = 单 exchange + 单 account：`load_xsec` 增 account 一致性校验，混合即拒启（与 exchange 校验同层）。
- `remote_positions` 是最终事实：drift 修正方向恒为 remote → 本地账本（含 SubPosition/`PositionBook`），**对账永不发单**。
- 无 `strategy_id` 的遗留远程仓位：只审计（warn 列明 instrument/qty），不纳入子仓位/NAV/portfolio_exit 度量（§13-2）。
- panic 恢复语义（v2.4）：策略 panic → 本组 Paused 且不自动重试；放行 = 进程/监督代重建 + `recover`（Store 落盘行 + remote 对账）后 Running。旧策略实例不续用。
- 重启幂等：`sub_` 确定性 ID 保证 recover 后重试同 bar 意图不产生新 ID；启动对账先于任何决策事件投递。
- PR-1～PR-4 全部既有行为零改动：`sig_` 黄金值、`ExchangeRunner`、旧 per-symbol live actor、旧配置 loader 全量回归绿。
- 数量口径：live 下 qty 由组合层 `nav × per_position_pct` 计算后随订单 quantity 出帧；服务端保证金校验拒单（`insufficient balance`）走 `rejected_actions` 计数——旁路 `venue_sized` 的旧 runner 语义不适用于横截面 dispatch（在代码注释写明，勿改 `ActionDispatcher`）。
- paper_trading = live 形状（真 WS K 线 + `SimulationExecution` 本地撮合），与 `spi_live_actor` 既有 paper 分流一致；本计划两种模式同一 actor 代码。

## Review Focus

- 数据面停流/单标的缺 bar：非阻塞对齐不得饿死另一标的（starved→退避轮询），也不得把「空 channel」当 source 耗尽（`Ok(None)` 语义只留给真结束）。
- 重启时序：recover 完成前不得消费任何 candle/order_update（旧在途单必须先 `poll_in_flight` 式查询再放行决策）。
- 对账方向错误风险：remote 有仓而本地无（他人/遗留仓）绝不得生成 SubPosition 纳入 NAV；本地有而 remote 无 → Closed + 告警。
- Shutdown 落盘次序：`flush → store.flush` 与 60s 定时 flush 竞态（actor 线程单写者约束，沿用 `flush_if_needed` 纪律）。
- account/exchange 混合与 `run` 覆盖项（`--strategy/--symbol`）拒启文案：报错必须点名条目，避免静默降级。

---

## File Structure

**engine（新增接缝，全部向后兼容）**
- `crates/engine/src/core/bar.rs` — `MarketEvent::SourceIdle` 变体 + `BarEventSource::try_next_event`（默认实现回退 `next_event`）
- `crates/engine/src/exchanges/live.rs` — `ChannelBarSource` 用 `recv_timeout(ZERO)` 实现 `try_next_event`；`LiveExecution` 镜像账本（cash/realized/fee/last_price）+ `account_snapshot` Some(mirror)
- `crates/engine/src/core/runtime.rs` / `xsec/runner.rs` — `ExchangeRunner::on_event` 与 group 对 `SourceIdle` 的忽略处理（计数 `source_idle` 仅日志）
- `crates/engine/src/xsec/align.rs` — `next_aligned_nonblocking(&mut self) -> Result<AlignPoll>`，`AlignPoll::{Ready(AlignedEvent), Starved, Done}`
- `crates/engine/src/xsec/runner.rs` — `reconcile_remote(&mut self) -> Result<ReconcileReport>`；`recover_from_store(&mut self, store, fresh_strategy, after_panic) -> Result<ReconcileReport>`（`resume_from_store` 的 panic 语义保持：recover = Running 启动路径，恢复语义复用同一实现）

**app**
- `crates/app/src/xsec_live_actor.rs` — 新文件：`XsecLiveSpec/XsecLiveAssembly`、`spawn_xsec_live`（监督代）、actor 主循环（数据面 channel 汇聚 + 控制面命令 + 定时 reconcile/flush）
- `crates/app/src/bootstrap.rs` — `run_live` 增横截面分支（共享 `TradingServiceKlineClient` 订阅 universe 全标的、每标的 per-slot candle channel、装配组）
- `crates/app/src/main.rs` — `Run` 分支：`is_xsec_config_path` → `run_live_xsec`（替换现 bail）
- `crates/app/src/xsec_config.rs` — account 一致性校验 + `run_mode`（`live|paper_trading`，取 CLI `--mode` > 策略文件 > `_global`）

**测试/演练**
- `crates/engine/tests/xsec_live_bridge.rs` — try_next_event/SourceIdle/对齐 nonblocking/LiveExecution 镜像/reconcile_remote 单测（channel 注入）
- `crates/app/tests/xsec_live_mock_e2e.rs` — mock WS：双标的订阅→决策→ack→order_update fill→对账（缩时）→重启 recover 不重复下单→panic 断路→遗留仓审计
- `crates/broker/src/mock.rs` — 仅按需小补（若多标的 `get_positions` 镜像或 kline replay 有缺口才动；先测后改）

**文档**
- `README.md` / `CHANGELOG.md` / `docs/RUNBOOK.md`（xsec live 用法 + panic/恢复手册）/ `contracts/trading-service.md` 注记（无权益帧 → local mirror 口径）

---

## Task 1: `try_next_event` + `SourceIdle` + 非阻塞对齐（engine）

**Files:**
- Modify: `crates/engine/src/core/bar.rs`
- Modify: `crates/engine/src/exchanges/live.rs`（ChannelBarSource）
- Modify: `crates/engine/src/xsec/align.rs`
- Modify: `crates/engine/src/core/runtime.rs`（ExchangeRunner::on_event 忽略 `SourceIdle`）
- Test: `crates/engine/tests/xsec_live_bridge.rs`（新文件，本任务起）

**Interfaces:**
- Produces:
```rust
pub enum MarketEvent { Bar(Bar), SessionGap(SessionGap), DayBoundary(TradingDay),
    Roll(RollEvent), Reconnected, /// 新增：非阻塞轮询时该源暂无事件（进程内，不上线）
    SourceIdle }

pub trait BarEventSource: Send {
    fn next_event(&mut self) -> Result<Option<MarketEvent>>;
    /// 非阻塞取一个事件：无可用 → `Ok(Some(MarketEvent::SourceIdle))`；
    /// 真耗尽（channel 关闭）→ `Ok(None)`。默认实现回退 `next_event`（回测源零改动）。
    fn try_next_event(&mut self) -> Result<Option<MarketEvent>> { self.next_event() }
    fn watermark(&self) -> Option<DateTime<Utc>> { None }
}

pub enum AlignPoll { Ready(AlignedEvent), Starved, Done }
impl AlignedEventSource {
    /// 非阻塞版：每源先 `try_next_event` 填 pending；
    /// 所有源 pending 为空且无耗尽 → Starved；全部耗尽 → Done。
    pub fn next_aligned_nonblocking(&mut self) -> Result<AlignPoll>;
}
```
- `ChannelBarSource::try_next_event`：`rx.recv_timeout(ZERO)`；`Disconnected` → `Ok(None)`；`Timeout` → `Ok(Some(MarketEvent::SourceIdle))`；保留既有同/旧 `open_time` 水位丢弃。
- `ExchangeRunner::on_event` / `CrossSectionalRunnerGroup::on_event`：`SourceIdle` → `Ok(())` 直接忽略（不进轮次、不计数）。

- [ ] **Step 1: 写失败测试**（`xsec_live_bridge.rs`）

```rust
#[test] fn channel_source_reports_idle_then_event_then_none_on_disconnect() {
    // std mpsc：空 channel → SourceIdle；send(candle) → Bar；drop(tx) → None
}
#[test]
fn align_nonblocking_no_starvation_across_two_sources() {
    // s0 有 t0/t1/t2，s1 空（未耗尽）：连续 next_aligned_nonblocking 只吐 s0 事件，
    // s1 空时返回 Starved 而非 Done；s1 补 t0 后 t0 的 slot0/slot1 顺序恢复确定性；
    // 全部 drop(tx) 后 Done。
}
#[test]
fn exchange_runner_and_group_ignore_source_idle() {
    // 喂 [SourceIdle; 3] + Bar：bars_processed==1、provisional==0、无 panic。
}
```

- [ ] **Step 2:** Run: `cargo test -p strategy-engine --test xsec_live_bridge`；Expected: FAIL（编译）。
- [ ] **Step 3:** 实现（enum 加变体后编译器穷尽 match 报错即位点清单；仅 engine 内部适配，`ts_kline`/actor 的 `match event` 若在 app 层亦按编译错误补齐忽略分支）。
- [ ] **Step 4:** 新测试 PASS + `cargo test --workspace` 旧链全绿（`spi_live_orchestrator` 等编译过即证穷尽处理无漏）。
- [ ] **Step 5:** Commit（逐文件 add：bar.rs、live.rs、align.rs、runtime.rs、tests/xsec_live_bridge.rs；必要时含被编译器点名到的 app 忽略分支文件）：
```bash
git commit -m "feat(engine): BarEventSource::try_next_event + SourceIdle + 非阻塞对齐"
```

---

## Task 2: `LiveExecution` 本地镜像账本（engine）

**Files:**
- Modify: `crates/engine/src/exchanges/live.rs`
- Test: `crates/engine/tests/xsec_live_bridge.rs`

**Interfaces:**
- Produces: `LiveExecution` 新私有字段 `mirror: MirrorAccount { cash: f64, realized: f64, fee: f64, last_price: Option<f64> }`（由 `SourceRequest.initial_cash` 起账），并：
```rust
impl ExecutionPort for LiveExecution {
    fn on_bar(&mut self, bar: &Bar) { self.mirror.last_price = Some(bar.close); } // 若已有覆写则并入
    fn account_snapshot(&self) -> Option<Account> {
        // 明示 local mirror：cash + unrealized(镜像 book × last_price)；fee/realized 逐笔累计。
        // 无 last_price 时 unrealized=0（不伪造 mark）。
    }
}
```
- 更新点：回报转 Fill 处——`cash += realized_pnl - fee`（回报真实 fee/pnl，阶段 4 v2.7 口径）；Open 成交不扣现金（crypto 保证金模型与 `SimulationExecution` 对齐）；平仓按腿记 `realized_pnl`。
- 装配日志：构造时 `tracing::info!(instrument, nav_source="local_mirror", "LiveExecution 权益=本地镜像；broker 权益契约缺帧，不伪造")`。
- `venue_sized()` 保持 `true`（旧单标 runner 语义不动）。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn live_mirror_equity_tracks_fills_and_marks() {
    // initial_cash=1000；open Fill(1@100, fee=0.1) → equity==999.9（unrealized 0 @entry）
    // on_bar(close=110) → equity==1009.9；close Fill(realized=10, fee=0.1)
    // → cash==1019.8 且 unrealized==0；currency 与规则表一致。
}
#[test]
fn live_mirror_starts_none_free_when_no_fills() {
    // 无成交：account_snapshot()==Some(equity==initial_cash)；不 panic。
}
```

- [ ] **Step 2:** FAIL（`live_mirror_equity_tracks_fills_and_marks` 等）。
- [ ] **Step 3:** 实现。
- [ ] **Step 4:** PASS；`grep -rn "account_snapshot" crates/app/src` 复核旧链对 `None` 无行为依赖（paper/live actor 仅 `venue_sized`/mirror 消费）；`cargo test --workspace` 全绿——若发现旧 actor 因 Some 改变行为，STOP 报 DONE_WITH_CONCERNS，不得改测试掩盖。
- [ ] **Step 5:** Commit `feat(engine): LiveExecution 本地镜像权益账本（明示 local_mirror）`。

---

## Task 3: `reconcile_remote` + `recover_from_store`（engine group）

**Files:**
- Modify: `crates/engine/src/xsec/runner.rs`
- Test: `crates/engine/src/xsec/runner.rs` 内联（`TestExecution` 桩加 `remote: Vec<PositionLot>` 注入字段）

**Interfaces:**
- Produces:
```rust
pub struct ReconcileReport {
    pub synced_instruments: u32,
    /// (instrument, kind, detail)：kind ∈ {closed, qty_corrected, legacy_remote_only}
    pub drifts: Vec<(String, String, String)>,
}
impl CrossSectionalRunnerGroup {
    /// remote_positions = 最终事实：pos_sync.book ← remote lots；
    /// 本地非 Closed 子仓而 remote 平 → advance(RemoteFlat)=Closed + 告警；
    /// qty 不符 → 子仓按 remote 记 drift（qty_corrected；Decimal 口径不变）；
    /// remote 有仓但本组无对应非 Closed 子仓 → drifts 记 legacy_remote_only（仅审计，
    /// 不建 SubPosition、不入 NAV/portfolio 度量）。
    pub fn reconcile_remote(&mut self) -> crate::core::error::Result<ReconcileReport>;

    /// 启动恢复（live/回测共用）：Store 行 + 每 slot remote 对账重建账本。
    /// after_panic=true 保留旧 resume_from_store 文案语义（可疑内存丢弃）；
    /// false = 正常启动路径（phase 恢复为 Running）。
    pub fn recover_from_store(
        &mut self,
        store: &strategy_store::Store,
        fresh_strategy: Box<dyn CrossSectionalStrategyPort>,
        after_panic: bool,
    ) -> crate::core::error::Result<ReconcileReport>;
}
```
- `resume_from_store(store, fresh)` 改为 `recover_from_store(store, fresh, true).map(|_| ())` 薄封装（既有调用与测试零改动）。
- 修正/recover 后：变动子仓 upsert 回 Store（复用 `subs_changed`/`persist_round` 路径）。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn reconcile_closes_sub_when_remote_flat() {
    // Active sub + remote 空 → Closed + drifts("closed")；RecordingExecution.orders 为空
    //（对账永不发单）。
}
#[test]
fn reconcile_corrects_qty_toward_remote() {
    // sub qty=1、remote lot remaining=0.4 → sub.qty=0.4、drifts("qty_corrected")、
    // pos_sync.book 同步。
}
#[test]
fn reconcile_audits_foreign_remote_without_ledger() {
    // remote 有 lot、本组无 sub → drifts("legacy_remote_only")；
    // master_position().gross_notional 不含它。
}
#[test]
fn recover_running_and_after_panic_paths_agree() {
    // 同一 Store 行：after_panic=false → Running；true → Running+panic 文案日志；
    // 两路径最终 sub_positions/PositionState 一致（复用现有 resume 测试断言）。
}
```

- [ ] **Step 2:** FAIL。 [ ] **Step 3:** 实现。 [ ] **Step 4:** 新测试 + 全部 `xsec::` 绿（现有 `resume_from_store_discards_untrusted_memory_and_rebuilds` 不得改动且必须继续 PASS）。
- [ ] **Step 5:** Commit `feat(engine): 横截面组 reconcile_remote 与 recover_from_store`。

---

## Task 4: `xsec_live_actor` 骨架（app，双平面事件泵）

**Files:**
- Create: `crates/app/src/xsec_live_actor.rs`
- Modify: `crates/app/src/lib.rs`（`pub mod xsec_live_actor;`）
- Test: `crates/app/tests/xsec_live_mock_e2e.rs`（新文件，本任务先建装配冒烟）

**Interfaces:**
- Produces:
```rust
pub struct XsecLiveSpec {
    pub strategy_id: String,
    pub strategy_type: String,
    pub config: strategy_engine::xsec::config::XsecConfig,
    pub instruments: Vec<crate::config::SymbolSpec>, // load_xsec 产物（venue_key 等已解析）
    pub mode: String,                                 // live | paper_trading
    pub state_dir: PathBuf,
    pub state_flush_interval_secs: u64,
    pub position_check_interval_secs: u64,            // 对账周期（默认 60）
    pub initial_cash_total: f64,
    pub fee_rate: Option<f64>,
    pub channel_cap: usize,
}

/// 每监督代重建一次（strategy_factory/gateway_factory 与旧 RuntimeFactory 同型）。
pub struct XsecLiveAssembly {
    pub spec: XsecLiveSpec,
    /// 每标的一条「已闭合 1m」流（bootstrap 扇出注入；actor 侧包 ChannelBarSource）
    pub candle_rxs: Vec<std::sync::mpsc::Receiver<strategy_core::candle::Candle>>,
    pub warmup: Vec<Vec<strategy_core::candle::Candle>>, // 每标的预载（不足只告警，entry 门兜底）
    pub store: Arc<Store>,
    pub gateway_factory: Option<Box<dyn Fn() -> Box<dyn AsyncOrderGateway> + Send>>, // None=paper
    pub strategy_factory: Box<dyn Fn() -> Box<dyn CrossSectionalStrategyPort> + Send>,
    pub me: RuntimeHandle,
    pub route_tx: Option<mpsc::Sender<RouteBind>>,
}

pub fn spawn_xsec_live(assembly: XsecLiveAssembly) -> (RuntimeHandle, JoinHandle<()>);
```
- 内核线程（tokio current_thread + `catch_unwind`，形态抄 `spawn_spi_live` 监督代；代失败=整代重建，重建装配先 `recover_from_store(after_panic=true)` 若上一代以 panic 终止，否则 `false`）：
  - 装配：每标的 `exchange_for_spec` → `SourceRequest`（paper=`SimulationExecution`；live=`LiveExecution` + gateway）→ `XsecSlot`（`FeaturePlan`/warmup preload）→ `CrossSectionalRunnerGroup::new(..., Some(store))` → `recover_from_store`（先账本+对账，后消费）。
  - 泵循环（单线程事件驱动）：遍历 slot `try_recv` candle 推入各自 `ChannelBarSource` 的 tx（app 侧持 rx→ 或直接把 rx 移入 source）；`group.sources()` 取回后 `next_aligned_nonblocking`：`Ready(ev)` → `group.on_event` → 消费 `RoundReport`（净值日志）；`Starved` → `sleep(100ms)` + 控制面/定时器检查；`Done` → 收口。
  - 控制面：`mpsc::try_recv`——`OrderUpdate`（live：注入对应 slot `LiveExecution`（经 `as_live_mut()` 的 `on_order_update` 既有缝）→ 轮首 `sync_slot_fills`）、`Reconcile` → `group.reconcile_remote()` + drifts 日志、`Shutdown` → `store.flush()` + 退出。
  - 定时器（`tokio::time::interval`）：`position_check_interval` 发 Reconcile、`state_flush_interval` flush（沿用「actor 线程单写者」纪律，不与他处并发 flush）。
  - live 提交面本任务先留 `take_submit_requests` 空循环钩子，Task 5 接通 gateway。
- `RuntimeCommand` 零扩展（OrderUpdate/Reconcile/Shutdown 复用）。

- [ ] **Step 1: 装配冒烟测试**（paper 通路）

```rust
#[test]
fn xsec_actor_paper_two_symbols_processes_queued_candles() {
    // 双 channel 预注入各 5 根 1m（合成价），mode=paper_trading，store=tempdir；
    // spawn → 等 bars_processed 推进（轮询 store 或暴露 handle 查询位）→ 发 Shutdown
    // → join → 断言 state 目录 flush 出 strategy_portfolio.csv（bars>0、无成交）。
}
```

- [ ] **Step 2:** FAIL（模块不存在）。
- [ ] **Step 3:** 实现（先 paper；`gateway_factory=Some` 路径编译可达但行为 Task 5 前只 log+skip submit）。
- [ ] **Step 4:** 冒烟 PASS + `cargo test --workspace`（旧 `spi_live_actor`/orchestrator 全绿——它零 diff）。
- [ ] **Step 5:** Commit `feat(app): xsec live actor 骨架（双平面泵 + 定时器 + 监督代）`。

---

## Task 5: live 订单/回报接线 + 在途看门狗（app actor）

**Files:**
- Modify: `crates/app/src/xsec_live_actor.rs`
- Test: `crates/app/tests/xsec_live_mock_e2e.rs`

**内容（对照 `SpiLiveKernel` 逐项移植语义、不复制其单标 sizing）：**
- 每轮后 `take_submit_requests()`（`LiveExecutionPort` 既有缝）→ `gateway.place_order`（帧字段与旧链一致：exchange/account/quantity/order_cash；`signal_id=sub_...` 原样）→ ack 回写 `bind_order_id` + `RouteBind` 注册。
- `on_order_update(OrderUpdateCmd)` 路由到 slot：`as_live_mut().on_order_update` → engine Fill（`qty_raw`=回报 `filled_quantity` 原样）→ 下轮 ① `sync_slot_fills` 落 SubPosition/`PositionState.remaining_size_raw`（C7 口径）。
- 在途看门狗：`poll_in_flight`（grace=60s/interval=60s 与 `POLL_GRACE_SECS` 同源）——xsec 组的未成交腿（OpenRequested/CloseRequested 子仓）按 `get_order` 快照查询驱动状态。
- reduce-only 拒单（`external_close_on_reduce_only` 决策树等价）→ 子仓按外部已平走 RemoteFlat/CloseRequested 收敛，禁止重发平仓单。
- 重启先查后发：装配序 = recover → `poll_in_flight_at(now)` → 才消费 candle（泵代码顺序即断言对象）。
- 三级确认 ↔ 5 态映射文档化在模块头注释：ack=OpenRequested、FILL=Active/递减、reconcile=Closed 最终事实。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn live_ack_update_fill_ledger_three_stages() {
    // stub AsyncOrderGateway：place_order→ack(order_id)；回报 FILLED(真实 fee/qty_raw)。
    // 断言：sub OpenRequested→Active；PositionState.remaining_size_raw==Some(回报字面量)；
    // get_positions 镜像 == pos_sync.book；drifts 空。
}
#[test]
fn restart_reconciles_inflight_before_consuming_candles() {
    // Store 预置 OpenRequested 子仓；gateway 该单未成交（get_order=PARTIALLY?）；
    // recover 后在途查询完成前注入会触发同决策的 candle → 无第二个相同 signal_id 提交
    //（确定性 ID + 1:1 守卫双保险；orders 计数断言）。
}
#[test]
fn reduce_only_reject_maps_external_close() {
    // FILLED 前远端已平 → reduce-only 拒单 → 子仓 Closed + 告警；后续无新提交。
}
```

- [ ] **Step 2:** FAIL。 [ ] **Step 3:** 实现。 [ ] **Step 4:** 三案 PASS + 旧 `spi_live_orchestrator`/`spi_live_merge` 全绿（零 diff）。 [ ] **Step 5:** Commit `feat(app): xsec actor live 订单回报与在途看门狗（三级确认闭环）`。

---

## Task 6: bootstrap/main/loader 分派 + 校验（app CLI）

**Files:**
- Modify: `crates/app/src/bootstrap.rs`
- Modify: `crates/app/src/main.rs`
- Modify: `crates/app/src/xsec_config.rs`
- Modify: `crates/app/tests/xsec_config_guards.rs`、`crates/app/tests/run_config_guards.rs`（追加）

**内容:**
- `XsecSpec.run_mode: String`（`live|paper_trading`；CLI `--mode` > 策略文件 `mode:`（注意：该字段此前被旧 loader 忽略）> `_global.mode` > 默认 `paper_trading`；`smoking` → Err）。
- `load_xsec` 追加校验：`fn ensure_single_account(pairs: &[(String /*symbol*/, String /*account*/)]) -> Result<(), String>`（防御位：当前 schema 全标的同源必一致，校验与报错文案按此实现，未来 entry 级 account 放开即生效）；run 分支 `--strategy/--symbol/--cash` 混用 → Err（文案与 backtest 分支同款）。
- `bootstrap::run_live` 入口：`is_xsec_config_path(config)` → `run_live_xsec(spec, file_cfg, services, output_dir, data_dir, mode)`：
  1. 建/复用共享 `TradingServiceKlineClient`：**一帧订阅全 universe symbols**（`subscribe_klines(exchange, symbols[])`，mock/服务端已支持数组）；
  2. 每标的 warmup：本地 1m CSV 主 + `get_klines` 分页补足（既有 `backfill` 面）；不足 → `warn`（不阻断；entry 硬门拒开仓兜「预热不足禁交易」）；
  3. router 按 symbol 扇出注入每标的 `std::mpsc::channel`（既有 attach 面，(venue,symbol) 水位去重不动）；
  4. `spawn_xsec_live`（live：gateway_factory= trading_service AsyncOrderGateway + route_tx 路由表；paper：None）；
  5. ctrl-c：`handle.shutdown()` + join + `store.flush()`（进程拓扑不变：一 xsec 组=一 actor 代，多组同进程并存由后续演进）。
- `main.rs` `Run` 分支：删除「PR-5 尚未实现」bail，改为解析 run_mode 并透传；`live + services.trading 占位端点` 拒启守卫沿用（`is_unconfigured`）。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn xsec_run_mode_resolution_and_override() {
    // 文件无 mode → run_mode=="paper_trading"；--mode live → "live"；
    // --mode smoking → Err 含 "paper_trading"；_global.mode 生效序在 CLI 之下。
}
#[test]
fn xsec_single_account_guard() {
    // universe [{BTCUSDT, acct via _global acct_a}, ETHUSDT 条目级 account 不同]
    // → Err 点名两 account 值与 symbol。
}
#[test]
fn xsec_run_rejects_single_symbol_overrides() {
    // run --config strategy.yaml --symbol X → Err 含「不支持」。
}
```

- [ ] **Step 2:** FAIL。 [ ] **Step 3:** 实现。 [ ] **Step 4:** 新案 + `run_config_guards` 旧 5 案 + `xsec_config_guards` 旧 10 案全绿。 [ ] **Step 5:** Commit `feat(app): run 分派横截面——订阅合并、run_mode、单 account 守卫`（Files 逐文件 add）。

---

## Task 7: mock 端到端演练（先测后改 broker）

**Files:**
- Test: `crates/app/tests/xsec_live_mock_e2e.rs`（扩两案）
- Modify（仅当测试暴露缺口）: `crates/broker/src/mock.rs`
- Modify（一句注记）: `contracts/trading-service.md`（无权益查询帧 → 策略侧 NAV 为 local_mirror）

**测试（in-process mock WS，真实帧序列）:**

```rust
#[test]
fn xsec_live_drill_two_symbols_full_loop() {
    // mock：kline_dir(BTC/ETH 合成 1m)+净持仓镜像+submit_enabled；
    // 断言：订阅帧 symbols 数组含双标的；决策出单（sub_ id）→ ack → FILLED →
    // 子仓 Active；get_positions 镜像 == 本地 book；外部直改镜像（模拟他人平仓）→
    // 缩时 Reconcile → drifts("closed" 或 "qty_corrected") + 子仓收敛；全程无重复
    // signal_id、无权益帧依赖。
}
#[test]
fn xsec_live_drill_restart_recover() {
    // 第一代开仓后 Shutdown（已 flush）；第二代（新 store 句柄同目录、gateway 镜像保留）
    // → 日志断言 recover 行数；注入可触发决策的 K 线 → 不产生新开仓；Reconcile 零 drift。
}
```

- [ ] **Step 1:** 写测试跑当前实现；记录红因归类（mock 缺口 / app 缺口 / 测试装置缺口）。
- [ ] **Step 2:** 只补最小缺口（预期候选：mock `kline_dir` 多标的按各自 CSV 文件、外部改仓注入面；若无缺口跳过改 mock）。
- [ ] **Step 3:** 两案 PASS。 [ ] **Step 4:** `cargo test --workspace`（含 `crates/broker` 全套）。
- [ ] **Step 5:** Commit `test(app): 横截面实盘 mock 演练（全链 + 重启 recover）`（含 broker/contracts 改动则同提交或在 message 注明）。

---

## Task 8: panic 断路器实盘语义 + 运维文档

**Files:**
- Modify: `crates/app/src/xsec_live_actor.rs`（上一代 panic 标记传递：`recover_from_store(after_panic=true)` 入口日志；本代策略 panic → 组 Paused、进程不重启则不放行）
- Test: `crates/app/tests/xsec_live_mock_e2e.rs`（追加）
- Modify: `docs/RUNBOOK.md`、`README.md`、`CHANGELOG.md`

```rust
#[test]
fn strategy_panic_pauses_group_until_process_restart() {
    // 注入 panic 策略：本代组 is_paused、后续 candle 无新单；
    // 旧单标 actor（同进程另一 RuntimeHandle）继续处理 bar（进程内隔离，§16）。
}
#[test]
fn process_restart_after_panic_recovers_ledger_to_running() {
    // 第一代 panic → Store 有 Active 行；第二代（strategy_factory 新实例, after_panic=true）
    // → Running、账本与 Store+remote 一致、恢复消费决策。
}
```

- RUNBOOK 新节「横截面实盘」：启动前置（单 exchange+account、warmup、mock 演练步骤与 `.env`/代理坑）、**panic→重启人工放行手册**（drifts 三态读法）、NAV=local_mirror 的含义与边界。
- README：PR-5 边界句从「未实现」改为已实现 + 指向 RUNBOOK；CHANGELOG Added 条目（含 contracts 注记一行）。
- [ ] 测试 PASS → `cargo test --workspace` 全绿 → commit（feat 与 docs 按 diff 拆两条）。

---

## Task 9: 最终验收（证据包）

- [ ] `cargo fmt --all -- --check`；`cargo clippy --workspace --all-targets -- -D warnings`（0 告警）；`cargo test --workspace`（全绿，记录总数；旧 `spi_live_*`/`backtest_*`/`parity`/`golden_tests` 全含）。
- [ ] **mock live 实跑证据**：隔离 cwd（不加载仓库 `.env`；`STRATEGY_SECRET` 显式对齐 mock），双标的 universe `run --mode live` 对 mock 跑 ≥30 根/标的；日志摘录必备：订阅帧（symbols 双标的）、`nav_source=local_mirror`、ack、FILLED、reconcile（零 drift）、Shutdown flush。
- [ ] PR-1～PR-4 回测对照：`cargo test -p strategy-app --test xsec_backtest_e2e --test xsec_config_guards` 绿（证明 PR-5 未破坏回测面）。
- [ ] `git status` 仅余非本计划的外部文档改动；残留改动独立 commit，禁 `git add -A`。

---

## 明确不做（PR-5 边界）

- 服务端权益/净值查询帧（契约无此面；NAV=local mirror 并显式标注；真实 broker 权益接入=契约演进，另立项）。
- 跨所实例（单 exchange 约束保留）；跨策略资金分配/`multi_instance`（仍拒启）；L4 NAV 回撤（`max_drawdown` 仍忽略+告警）。
- 多 xsec 组同进程的订阅再合并/跨组隔离测试（进程拓扑默认单组；扩展留 PR-6）。
- paper↔live 权益逐笔 parity 断言（mock 镜像一致性在 Task 7 覆盖账本面；equity parity 依赖 broker 真帧，无帧不测）。
- 并发撮合/多决策线程：主循环保持单线程事件驱动。

## 执行顺序提示

T1→T2→T3（engine 全绿后再动 app）→ T4→T5→T6→T7→T8→T9。每任务独立 commit；T7 若动 `crates/broker` 需回归其全套；跨任务接口变更（`MarketEvent::SourceIdle` 穷尽 match）以编译器为清单。
