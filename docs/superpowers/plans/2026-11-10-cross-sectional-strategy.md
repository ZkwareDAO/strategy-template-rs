# 横截面策略支持（PR-1～PR-4）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按设计文档实现「1 策略 → 1 总仓位 → N 子仓位」的统一运行时，使回测端到端可跑横截面策略；旧单标的策略行为不变。

**Architecture:** 在 `crates/engine` 新增 `xsec` 顶层模块（模型、配置类型、特征、规则求值、组合运行时）；`strategy-engine` 通过 `CrossSectionalStrategyPort` 驱动 N 个 `XsecSlot`（每 slot 复用现有 `ExchangeParts`/`AggregationState`/`PositionSync`/`RiskGuard`）。1m 事件逐根驱动聚合/成交/盯市，**base TF（`timeframes[0]`）闭合才是决策轮次**（entry/exit/L1/L2/L3/策略均在其闭合时执行）；`SingleInstrumentAdapter` 包装旧 `dyn Strategy` 保证 N=1 行为与 `ExchangeRunner` 等价。配置在 app 层新增横截面 loader（`strategy.yaml`），旧 `{strategy}/{SYMBOL}.yaml` 路径不改。子仓位 `SubPosition` / 组合 `StrategyPortfolio` 走既有 `Store` 泛型 `ModelTable`，`MasterPositionView` 为派生视图不落表。

**Tech Stack:** Rust 2021 workspace（core / positions / store / indicators / engine / strategies / app）；chrono、serde、serde_yaml、serde_json、talib-rs、sha2、tempfile。

**Spec:** `deliverables/gstack/feature-dev-cross-sectional-strategy-2026-11-09.md`

## Global Constraints

- G1：一个策略实例管理 N 个标的子仓位；本期 `SubPosition : instrument = 1:1`，同一 `strategy_id + instrument` 同时最多一个非 `Closed` 子仓位。
- G2：本期风控 = L1 子仓位硬止损 + L2 标的级 `exit` 规则 + L3 `portfolio_exit` 总仓位止盈止损；L4 NAV 回撤层（`PortfolioRiskGuard`）推迟，`risk.max_drawdown` 只解析、加载时告警、不生效。
- G3：回测/实盘同管道；本计划只实现回测，`BarEventSource` / `ExecutionPort` 两端口形状不改。实盘（PR-5）不在本计划范围；`run` 遇到横截面配置必须**明确报错**，不得半实现。
- G4：`sub_` 信号 ID 确定性；`sub_` 与既有 `sig_`（`generate_signal_id`）是两个独立命名空间，**禁止修改既有 `generate_signal_id`**。
- G5：旧策略/旧配置行为零改动；旧 `ExchangeRunner` 路径保留；`SingleInstrumentAdapter` 的 N=1 结果（动作、成交、signal_id）必须与旧路径一致。
- G6：配置渐进式；`universe` 1 个标的时组合级字段自动退化；`multi_instance` 本期 loader 遇到即明确报错。
- 本期一个策略实例只允许单一 `exchange`；`universe` ≥2 时全部标的同 base TF，否则 loader 拒启。
- 未闭合 bar 不参与任何计算或决策；主循环单线程事件驱动，不做异步/并发撮合。
- 组合聚合数值口径：f64 求和顺序固定为 instrument `namespaced_key()` 字典序；数量类沿用 `Decimal` 既有口径。
- 配置 DSL 冻结：只承载过滤与风控（entry/exit/portfolio_exit/risk/sizing/rebalance），alpha 永远在策略代码；不引入字符串表达式 DSL。
- panic 断路器：策略 panic 只暂停该 RunnerGroup；暂停实例内存态一律视为可疑，恢复必须从 Store 落盘行 + `remote_positions` 重建后显式放行，不做冷却后自动重试。
- 旧策略的运行模式、`ExchangeRunner`、`PositionSync`、`RiskGuard`、`ActionDispatcher` 行为不得改变。

## Review Focus

- `universe` 混入不同 `exchange` / 同名 `symbol` 重复：loader 必须拒启并点名条目，绝不允许静默路由或行键碰撞。
- 某标的缺 bar / stale：本轮 quorum（`ceil(0.8N)`）不达标时必须跳过策略决策；stale 标的不得以 0/NaN mark 参与组合 PnL，也不得进入排名。
- `portfolio_exit` 的 N=1 退化：激活、回落、止损三个边界必须与旧 `RiskController` trailing 数值一致（百分比口径以**成本名义**为分母）。
- 部分平仓与 `_leg{i}`：Close 腿 signal_id 必须确定性且不与 Open 冲突；L2 `ExitRule` 触发时该标的本轮策略动作必须被抑制，避免同轮双重决策。
- panic 断路器：暂停组不得再发任何订单，兄弟组必须继续处理行情；恢复只从 Store/carry 重建，不信任暂停时的内存中间态。
- 组合聚合的求和顺序：同一组子仓位以不同插入顺序构造时，`MasterPositionView` 与报表数值必须逐位一致。

---

## File Structure

**新增（engine xsec）**
- `crates/engine/src/xsec/mod.rs` — 模块出口与 prelude
- `crates/engine/src/xsec/model.rs` — `SubPosition` / 5 态 / `StrategyPortfolio` / `MasterPositionView` / Store `Model` impl
- `crates/engine/src/xsec/signal_id.rs` — `sub_` 确定性 ID
- `crates/engine/src/xsec/config.rs` — `XsecConfig` 与 entry/exit/portfolio_exit/rebalance/risk/sizing 类型 + 结构校验 + 条件集摘要
- `crates/engine/src/xsec/features.rs` — `FeatureMap` + 指标流水线（两级周期）
- `crates/engine/src/xsec/evaluator.rs` — entry/exit 规则纯函数
- `crates/engine/src/xsec/port.rs` — `InstrumentSnapshot` / `PortfolioContext` / `InstrumentAction` / `CrossSectionalStrategyPort`
- `crates/engine/src/xsec/adapter.rs` — `SingleInstrumentAdapter`
- `crates/engine/src/xsec/align.rs` — `AlignedEventSource`（多源对齐 fan-in）
- `crates/engine/src/xsec/dispatch.rs` — 子仓位下单（`sub_` 命名空间、`Adjust`、按 NAV sizing、杠杆上限）
- `crates/engine/src/xsec/runner.rs` — `XsecSlot` / `CrossSectionalRunnerGroup` / `XsecRunSummary` / `RoundReport`

**新增（示例策略）**
- `crates/xsec-strategies/Cargo.toml`
- `crates/xsec-strategies/src/lib.rs` — 横截面策略注册表 `create` / `all`
- `crates/xsec-strategies/src/momentum.rs` — `xsec_momentum` 示例策略

**修改（app / 配置）**
- `crates/app/src/xsec_config.rs` — 新 schema loader、`is_xsec_config_path`、universe 解析、`make_xsec_strategy`
- `crates/app/src/spi_backtest.rs` — `run_xsec_from_spec` / `XsecRunArgs` / `XsecBacktestRun` / `fills_to_xsec_trade_rows`
- `crates/app/src/main.rs` — `backtest` 显式 `--config` 时检测横截面配置并分派
- `crates/app/src/lib.rs` — 注册 `xsec_config` 模块
- `config/xsec_momentum/strategy.yaml` — 文档示例配置
- `Cargo.toml` / `crates/app/Cargo.toml` / `crates/engine/Cargo.toml` — 新成员与新依赖
- `CHANGELOG.md` / `README.md` — 用法与登记

**修改（既有类型）**
- `crates/positions/src/state.rs` — `PositionState.strategy_id: Option<String>`
- `crates/engine/src/core/position_sync.rs`、`crates/positions/src/manager.rs` — 构造点补 `strategy_id: None`

**测试**
- `crates/engine/src/xsec/*` 内联单测（模型/ID/配置/特征/规则/对齐/运行时）
- `crates/app/tests/xsec_backtest_e2e.rs` — 双标的回测、确定性、黄金值、warmup、CLI/装配 e2e
- `crates/app/tests/xsec_config_guards.rs` — 配置拒启矩阵

---

## Task 1: `PositionState.strategy_id`（PR-1 纯加法）

**Files:**
- Modify: `crates/positions/src/state.rs`
- Modify: `crates/engine/src/core/position_sync.rs:92`
- Modify: `crates/positions/src/manager.rs:253,291`
- Test: `crates/positions/src/state.rs` 内联测试

**Interfaces:**
- Produces: `PositionState { pub strategy_id: Option<String>, ... }`，旧 JSON 无此字段可反序列化（`#[serde(default)]`）。

- [ ] **Step 1: 写失败测试**

在 `crates/positions/src/state.rs` 的 `mod tests` 增加：

```rust
#[test]
fn strategy_id_defaults_when_missing_in_old_snapshot() {
    let json = serde_json::json!({
        "position_id": "sig_x", "symbol": "BTCUSDT", "side": "Long",
        "entry_price": 100.0,
        "entry_time": "2026-06-10T00:00:00Z", "entry_timestamp": 0,
        "peak_price": 100.0, "stop_price": 98.0,
        "max_pnl_pct": 0.0, "min_pnl_pct": 0.0
    });
    let s: PositionState = serde_json::from_value(json).unwrap();
    assert_eq!(s.strategy_id, None, "旧快照无 strategy_id，必须默认 None");
}

#[test]
fn strategy_id_roundtrips_through_json() {
    let mut s = long_state(100.0);
    s.strategy_id = Some("xsec_momentum:default".into());
    let back: PositionState = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
    assert_eq!(back.strategy_id.as_deref(), Some("xsec_momentum:default"));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-positions state::`
Expected: 编译失败，`PositionState` 无 `strategy_id` 字段。

- [ ] **Step 3: 加字段并修全部构造点**

`crates/positions/src/state.rs` 的 `PositionState` 增加：

```rust
/// 归属策略实例（`{strategy_type}:{instance_id}`）。旧链不填 = None；
/// 横截面/组合链写 `strategy_id` 以便 Store 按策略归集。
#[serde(default)]
pub strategy_id: Option<String>,
```

在以下构造点补 `strategy_id: None`：
- `crates/engine/src/core/position_sync.rs:92`
- `crates/positions/src/manager.rs:253、291`
- `crates/positions/src/state.rs` 的 `long_state` 测试夹具

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-positions && cargo test -p strategy-engine core::position_sync`
Expected: PASS；旧快照兼容与往返两条测试均绿。

- [ ] **Step 5: Commit**

```bash
git add crates/positions/src/state.rs crates/engine/src/core/position_sync.rs crates/positions/src/manager.rs
git commit -m "feat(positions): PositionState 增加可选 strategy_id（旧快照兼容）"
```

---

## Task 2: `SubPosition` 5 态 + `StrategyPortfolio`（PR-1）

**Files:**
- Create: `crates/engine/src/xsec/mod.rs`
- Create: `crates/engine/src/xsec/model.rs`
- Modify: `crates/engine/src/lib.rs`（`pub mod xsec;`）
- Modify: `crates/engine/Cargo.toml`（`strategy-store.workspace = true` 从 dev-deps 提升为常规依赖）
- Test: `crates/engine/src/xsec/model.rs` 内联测试

**Interfaces:**
- Produces:
```rust
pub enum SubPositionState { PendingOpen, OpenRequested, Active, CloseRequested, Closed }

pub enum SubPositionEvent {
    SubmitAccepted,
    FillOpen { qty: f64, price: f64, at: DateTime<Utc> },
    CloseRequested,
    FillClose { qty: f64, realized_pnl: f64, at: DateTime<Utc>, remaining_after: f64 },
    RemoteFlat,
    Rejected,
}
pub fn advance_sub_state(current: SubPositionState, event: &SubPositionEvent) -> Option<SubPositionState>;

pub struct SubPosition {
    pub id: String,              // = 开仓 signal_id
    pub strategy_id: String,
    pub instrument: InstrumentId,
    pub side: Side,              // strategy_core::signal::Side
    pub qty: f64,
    pub entry_price: f64,
    pub stop_price: Option<f64>,
    pub target_weight: f64,
    pub rebalance_epoch: u64,
    pub state: SubPositionState,
    pub opened_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    pub realized_pnl: Option<f64>,
}

pub enum PortfolioPhase { Running, Paused, Liquidating }
pub struct TargetHolding { pub instrument: InstrumentId, pub target_weight: f64 }
pub struct StrategyPortfolio {
    pub strategy_id: String,
    pub phase: PortfolioPhase,
    pub nav: f64,
    pub high_watermark: f64,
    pub drawdown: f64,
    pub peak_unrealized_pnl: f64,
    pub trail_armed: bool,       // v2.4 扩展：回落跟踪是否已激活（重启恢复必需）
    pub target_holdings: Vec<TargetHolding>,
    pub max_total_notional: f64,
    pub last_rebalance_at: Option<DateTime<Utc>>,
    pub rebalance_epoch: u64,
}
pub struct MasterPositionView {
    pub strategy_id: String,
    pub gross_notional: f64,   // 成本名义 Σ|qty*entry_price*multiplier|（L3 百分比分母）
    pub net_notional: f64,
    pub avg_entry_price: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub sub_position_count: u32,
}
pub fn master_position_view(
    subs: &[SubPosition],
    marks: &std::collections::BTreeMap<String, f64>,
    multipliers: &std::collections::BTreeMap<String, f64>,
) -> MasterPositionView;
```
- `impl Model for SubPosition`：`NAME = "sub_position"`，`id() = &id`，`scope() = Some(&strategy_id)`，`is_terminal() = state == Closed`。
- `impl Model for StrategyPortfolio`：`NAME = "strategy_portfolio"`，`id() = &strategy_id`，`scope() = Some(&strategy_id)`。

- [ ] **Step 1: 写失败测试**

`model.rs` 内联测试至少包含：

```rust
#[test]
fn state_machine_matrix() {
    use SubPositionEvent::*;
    use SubPositionState::*;
    let cases = [
        (PendingOpen, SubmitAccepted, Some(OpenRequested)),
        (PendingOpen, Rejected, None),
        (OpenRequested, Rejected, None),
        (OpenRequested, FillOpen { qty: 1.0, price: 100.0, at: now() }, Some(Active)),
        (Active, CloseRequested, Some(CloseRequested)),
        (Active, FillClose { qty: 0.4, realized_pnl: 1.0, at: now(), remaining_after: 0.6 }, Some(Active)),
        (Active, FillClose { qty: 1.0, realized_pnl: 2.0, at: now(), remaining_after: 0.0 }, Some(Closed)),
        (Active, RemoteFlat, Some(Closed)), // 远程归零 = 最终事实（drift 修正）
        (OpenRequested, RemoteFlat, Some(Closed)), // 未成交但远程已无仓（拒单/撤单后对账）
        (CloseRequested, FillClose { qty: 0.4, realized_pnl: 1.0, at: now(), remaining_after: 0.6 }, Some(CloseRequested)),
        (CloseRequested, FillClose { qty: 0.6, realized_pnl: 2.0, at: now(), remaining_after: 0.0 }, Some(Closed)),
        (CloseRequested, RemoteFlat, Some(Closed)),
        (Closed, SubmitAccepted, None),
    ];
    for (from, ev, want) in cases {
        assert_eq!(advance_sub_state(from, &ev), want, "{from:?} + {ev:?}");
    }
}

#[test]
fn closed_is_terminal_for_store_pruning() {
    assert!(sub(SubPositionState::Closed).is_terminal());
    for s in [PendingOpen, OpenRequested, Active, CloseRequested] {
        assert!(!sub(s).is_terminal(), "{s:?} 不得被跨日裁剪");
    }
}

#[test]
fn master_view_sums_in_namespaced_key_order() {
    // BTC: qty=2, entry=50, mark=60；ETH: qty=1, entry=100, mark=110；mult=1
    // 期望 gross_notional=200、unrealized_pnl=30、avg_entry_price=(100+100)/3
    // 把 subs 顺序反转后再次调用，四个字段必须逐位相等（字典序求和固定）。
}

#[test]
fn store_roundtrip_and_recover() {
    let tmp = tempfile::tempdir().unwrap();
    let clock = std::sync::Arc::new(strategy_store::EpochClock);
    let store = strategy_store::Store::new(tmp.path(), clock.clone());
    store.table::<SubPosition>().upsert(sub(Active));
    store.table::<StrategyPortfolio>().upsert(portfolio());
    store.flush().unwrap();
    let recovered = strategy_store::Store::new(tmp.path(), clock.clone());
    assert_eq!(recovered.recover::<SubPosition>().unwrap(), 1);
    assert_eq!(recovered.recover::<StrategyPortfolio>().unwrap(), 1);
    // Closed 终态：prune 模式下 recover 不得复活
    store.table::<SubPosition>().upsert(sub(Closed));
    store.flush().unwrap();
    let recovered2 = strategy_store::Store::new(tmp.path(), clock);
    assert_eq!(recovered2.recover::<SubPosition>().unwrap(), 1);
}
```
（测试里的 `sub` / `portfolio` / `now` 为文件内辅助构造函数；第二个 Store 用新 `EpochClock` 实例，避免借用已移动变量。）

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::model`
Expected: 编译失败（`xsec` 模块不存在）。

- [ ] **Step 3: 实现 model.rs 与模块接线**

- 在 `crates/engine/Cargo.toml` 的 `[dependencies]` 增加 `strategy-store.workspace = true`（`[dev-dependencies]` 中的同名行删除，避免重复）。
- 在 `crates/engine/src/lib.rs` 增加 `pub mod xsec;`。
- `xsec/mod.rs`：

```rust
pub mod model;
```
（后续任务继续在此追加 `config` / `signal_id` / `features` / `evaluator` / `port` / `adapter` / `align` / `dispatch` / `runner`。）
- `model.rs` 实现上述类型与 `advance_sub_state`。状态机规则：
  - `PendingOpen + SubmitAccepted -> OpenRequested`；`Rejected -> None`（行删除，不占状态位）。
  - `OpenRequested + FillOpen -> Active`；`Rejected -> None`。
  - `Active + CloseRequested -> CloseRequested`；`Active + FillClose{remaining_after>1e-12} -> Active`（无平仓请求的部分减仓）；`Active + FillClose{remaining_after<=1e-12} -> Closed`（外部全平/对账事实）。
  - `CloseRequested + FillClose{remaining_after>1e-12} -> CloseRequested`；`CloseRequested + FillClose{remaining_after<=1e-12} -> Closed`（全平即终态，待对账确认由 `RemoteFlat` 幂等处理）。
  - `RemoteFlat` 是远程账本最终事实：`PendingOpen/OpenRequested/Active/CloseRequested + RemoteFlat -> Closed`（drift 修正；对应 live 对账，PR-5 接入）。
  - `Closed` 为终态，任何事件返回 `None`（调用方视为无效事件）。
- `master_position_view` 只聚合 `state != Closed` 的 subs，先按 `instrument.namespaced_key()` 排序再求和；`gross_notional` 用**成本价**（`entry_price * qty * multiplier` 的绝对值），`unrealized_pnl` 用 mark 价与 entry 差（mark 缺失用 entry，不产生 NaN）；`avg_entry_price` 按 qty 加权，`sub_position_count` 为聚合条数。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::model`
Expected: PASS（状态矩阵、终态裁剪、求和顺序、Store recover 全绿）。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/Cargo.toml crates/engine/src/lib.rs crates/engine/src/xsec/mod.rs crates/engine/src/xsec/model.rs
git commit -m "feat(engine): 横截面数据模型——SubPosition 5 态、StrategyPortfolio、MasterPosition 派生视图"
```

---

## Task 3: `sub_` 确定性信号 ID（PR-1）

**Files:**
- Create: `crates/engine/src/xsec/signal_id.rs`
- Modify: `crates/engine/src/xsec/mod.rs`
- Modify: `crates/engine/Cargo.toml`（`sha2.workspace = true`；engine 现未直接依赖 sha2）
- Test: `crates/engine/src/xsec/signal_id.rs` 内联测试

**Interfaces:**
- Produces:
```rust
pub enum SubIntent { Open, RebalanceReduce, RiskCloseAll, SubStop, ExitRule }
impl SubIntent { pub fn as_str(&self) -> &'static str; }

pub fn generate_sub_signal_id(
    strategy_type: &str,
    strategy_id: &str,
    venue_key: &str,
    aligned_bar_time: DateTime<Utc>,
    side: Side,
    intent: SubIntent,
    rebalance_epoch: u64,
) -> String;
```
- 哈希输入固定为：`format!("{}|{}|{}|{}|{}|{}|{}", strategy_type.to_lowercase(), strategy_id, venue_key.to_uppercase(), ts_ms, side_str, intent.as_str(), rebalance_epoch)`，其中 `side_str` = `"long"|"short"`，`ts_ms = aligned_bar_time.timestamp_millis()`。
- 输出 `"sub_" + SHA256(key) 前 8 字节 hex`（长度 20）。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn deterministic_and_namespaced() {
    let t = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
    let id = generate_sub_signal_id(
        "xsec_momentum", "xsec_momentum:default",
        "binance/crypto_perpetual/BTCUSDT", t, Side::Long, SubIntent::Open, 0,
    );
    assert_eq!(
        id, "sub_b526b953d8bc66f0",
        "黄金值：key=xsec_momentum|xsec_momentum:default|BINANCE/CRYPTO_PERPETUAL/BTCUSDT|1781049600000|long|open|0"
    );
    assert!(id.starts_with("sub_") && id.len() == 20);
    assert_ne!(id, generate_sub_signal_id("xsec_momentum", "xsec_momentum:default",
        "okx/crypto_perpetual/BTCUSDT", t, Side::Long, SubIntent::Open, 0));
    assert_ne!(id, generate_sub_signal_id("xsec_momentum", "xsec_momentum:default",
        "binance/crypto_perpetual/BTCUSDT", t, Side::Long, SubIntent::ExitRule, 0));
}

#[test]
fn sig_namespace_untouched() {
    let t = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
    assert_eq!(
        strategy_core::signal::generate_signal_id("sar_snt3", "BTCUSDT", t, strategy_core::signal::SignalType::Buy),
        "sig_9c1b75861b10144f"
    );
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::signal_id`
Expected: 编译失败（函数未定义）。

- [ ] **Step 3: 实现**

按 Interfaces 实现 key 格式、`sha2::Sha256`、`SubIntent::as_str`（`"open" | "rebalance_reduce" | "risk_close_all" | "sub_stop" | "exit_rule"`）。`Side` 用 match 输出 `"long" | "short"`。**不要触碰** `crates/core/src/signal.rs` 的 `generate_signal_id`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::signal_id && cargo test -p strategy-core golden_tests`
Expected: 新黄金值 PASS，旧 `sig_` 黄金值仍 PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/signal_id.rs crates/engine/src/xsec/mod.rs
git commit -m "feat(engine): sub_ 确定性信号 ID（独立 sig_ 命名空间）"
```

---

## Task 4: 配置类型与结构校验（PR-1/PR-2）

**Files:**
- Create: `crates/engine/src/xsec/config.rs`
- Modify: `crates/engine/src/xsec/mod.rs`
- Test: `crates/engine/src/xsec/config.rs` 内联测试

**Interfaces:**
- Produces 全部配置类型与：
```rust
pub fn validate_config(cfg: &XsecConfig) -> Result<(), Vec<String>>;
pub fn resolved_rule_summary(cfg: &XsecConfig) -> String; // 装配期审计日志与快照测试用
```
- 类型：
```rust
pub struct XsecConfig {
    pub strategy_type: String,
    pub strategy_id: Option<String>,
    pub mode: XsecMode,                      // 默认 CrossSectional
    pub universe: Vec<UniverseEntry>,
    pub timeframes: Vec<String>,             // [0] = base TF
    pub indicators: BTreeMap<String, IndicatorCfg>,
    pub entry: RuleSet,
    pub exit: RuleSet,
    pub portfolio_exit: PortfolioExitCfg,
    pub rebalance: Option<RebalanceCfg>,
    pub risk: RiskCfg,
    pub sizing: SizingCfg,
    pub params: Option<serde_json::Value>,
}
pub enum XsecMode { CrossSectional, MultiInstance }
pub enum UniverseEntry { Symbol(String), Detailed { symbol: String, exchange: Option<String> } }
pub struct IndicatorCfg { pub tf: String, #[serde(flatten)] pub params: BTreeMap<String, serde_json::Value> }
pub struct RuleSet { pub default: Vec<Rule>, pub overrides: BTreeMap<String, Vec<Rule>> }
pub struct Rule { pub ind: String, pub op: RuleOp, pub value: RuleValue }
pub enum RuleOp { Gt, Lt, Ge, Le, Eq, Ne }
pub enum RuleValue { Number(f64), Text(String) }
pub struct PortfolioExitCfg { pub trailing: Option<TrailingCfg>, pub stop_loss_pct: Option<f64> }
pub struct TrailingCfg { pub arm_pnl_pct: f64, pub giveback_pct: f64 }
pub struct RebalanceCfg { pub every_bars: u64, pub top_k: usize, pub bottom_k: usize, pub weight_scheme: WeightScheme }
pub enum WeightScheme { Equal }
pub struct RiskCfg { pub max_drawdown: Option<f64>, pub max_total_notional: f64 }
pub struct SizingCfg { pub per_position_pct: f64 }
pub struct ResolvedIndicator { pub tf: Timeframe, pub kind: IndicatorKind }
pub enum IndicatorKind {
    Adx { period: usize }, Rsi { period: usize }, Ema { period: usize },
    Sma { period: usize }, Atr { period: usize }, Macd { fast: usize, slow: usize, signal: usize },
}
pub fn resolve_indicator(name: &str, cfg: &IndicatorCfg) -> Result<ResolvedIndicator, String>;
```
- Serde：`mode` snake_case；`RuleOp` 从 `">" "<" ">=" "<=" "==" "!="` 字符串反序列化；`RuleValue` untagged number/string；`UniverseEntry` untagged string/object。

- [ ] **Step 1: 写失败测试**

```rust
fn doc_example() -> XsecConfig { serde_yaml::from_str(DOC_YAML).unwrap() }

#[test]
fn parses_doc_example_and_resolves_indicators() {
    let cfg = doc_example();
    assert_eq!(cfg.universe.len(), 2);
    assert_eq!(cfg.timeframes[0], "15m");
    assert_eq!(resolve_indicator("adx", &cfg.indicators["adx"]).unwrap().kind,
               IndicatorKind::Adx { period: 14 });
    assert!(validate_config(&cfg).is_ok());
}

#[test]
fn multi_instance_is_rejected() {
    let err = validate_config(&XsecConfig { mode: XsecMode::MultiInstance, ..doc_example() })
        .unwrap_err().join(" ");
    assert!(err.contains("multi_instance"), "{err}");
}

#[test]
fn rule_ind_must_be_declared() {
    let mut cfg = doc_example();
    cfg.entry.default[0].ind = "not_declared".into();
    let err = validate_config(&cfg).unwrap_err().join("\n");
    assert!(err.contains("not_declared"), "错误必须点名未声明指标: {err}");
}

#[test]
fn indicator_tf_must_be_integer_multiple_of_base() {
    let mut cfg = doc_example();
    cfg.timeframes = vec!["15m".into()];
    cfg.indicators.get_mut("adx").unwrap().tf = "20m".into();
    let err = validate_config(&cfg).unwrap_err().join("\n");
    assert!(err.contains("20m") && err.contains("15m"), "{err}");
}

#[test]
fn universe_two_requires_rebalance() {
    let mut cfg = doc_example();
    cfg.rebalance = None;
    let err = validate_config(&cfg).unwrap_err().join("\n");
    assert!(err.contains("rebalance"), "{err}");
}

#[test]
fn leverage_cap_must_cover_universe() {
    let mut cfg = doc_example();
    cfg.sizing.per_position_pct = 0.6;
    cfg.risk.max_total_notional = 0.8;
    let err = validate_config(&cfg).unwrap_err().join("\n");
    assert!(err.contains("max_total_notional"), "{err}");
}

#[test]
fn macd_cross_text_rules_are_validated() {
    let mut cfg = doc_example();
    cfg.exit.overrides.insert(
        "ETHUSDT".into(),
        vec![Rule { ind: "macd_cross".into(), op: RuleOp::Eq, value: RuleValue::Text("bearish".into()) }],
    );
    assert!(validate_config(&cfg).is_ok(), "bearish 必须合法");
    cfg.exit.overrides.insert(
        "ETHUSDT".into(),
        vec![Rule { ind: "macd_cross".into(), op: RuleOp::Eq, value: RuleValue::Text("sideways".into()) }],
    );
    let err = validate_config(&cfg).unwrap_err().join("\n");
    assert!(err.contains("sideways"), "{err}");
}

#[test]
fn rule_summary_is_deterministic() {
    let a = resolved_rule_summary(&doc_example());
    let b = resolved_rule_summary(&doc_example());
    assert_eq!(a, b, "同一配置摘要必须逐字节一致");
    assert!(a.contains("BTCUSDT") && a.contains("ETHUSDT") && a.contains("adx"), "{a}");
}

#[test]
fn bottom_k_must_be_zero_this_milestone() {
    let mut cfg = doc_example();
    cfg.rebalance.as_mut().unwrap().bottom_k = 1;
    let err = validate_config(&cfg).unwrap_err().join("\n");
    assert!(err.contains("bottom_k"), "{err}");
}
```

测试里的 `DOC_YAML` 直接复制设计文档 §5.1 的示例（`strategy_type: xsec_momentum`、universe BTC/ETH、`entry`/`exit`/`portfolio_exit`/`rebalance`/`risk`/`sizing`）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::config`
Expected: 编译失败（类型未定义）。

- [ ] **Step 3: 实现 config.rs**

校验规则（全部实现，返回 `Vec<String>` 聚合全部错误，不早停）：
1. `strategy_type` 非空；`universe` 非空；`mode == MultiInstance` 明确报「multi_instance 未实现」。
2. `timeframes` 非空且均可 `Timeframe::parse`；`IndicatorCfg.tf` 必须 ⊆ timeframes 且 `tf.0 % base.0 == 0`。
3. `indicators` 名称白名单 `adx|rsi|ema|sma|atr|macd`；参数键白名单：前五种仅 `period`，`macd` 仅 `fast|slow|signal`，缺省按 14/12/26/9；未知键报错。
4. entry/exit 每条 `ind` 必须已声明，或 `macd_cross` 在声明了 `macd` 时可用；`Text` 仅允许 `macd_cross` 且值 ∈ `bullish|bearish`，`Text` 仅允许 `Eq|Ne`；数值指标只接受 `Number`。
5. `universe.len() >= 2` 时 `rebalance` 必填；`universe.len() == 1` 时 `rebalance` 可省略。`rebalance.every_bars > 0`、`top_k > 0`；本期 `bottom_k` 必须为 `0`（空头侧推迟），非 0 报错「bottom_k 本期未实现」。
6. `sizing.per_position_pct > 0` 且 `sizing.per_position_pct * universe.len() <= risk.max_total_notional`。
7. `risk.max_drawdown` 若有值：不报错（loader 告警「L4 推迟，忽略」）。
8. `portfolio_exit.trailing` 若存在：`arm_pnl_pct > 0`、`giveback_pct > 0 && <= 100`；`stop_loss_pct > 0`。

`resolved_rule_summary`：按 universe 符号字典序输出 `symbol: entry=[...] exit=[...]`，规则顺序为配置文件顺序，数值格式化固定（`{:.6}`），供装配期 `tracing::info!` 与测试快照。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::config`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/config.rs crates/engine/src/xsec/mod.rs
git commit -m "feat(engine): 横截面配置 schema 与加载期校验（entry/exit/portfolio_exit/rebalance）"
```

---

## Task 5: 特征流水线与两级周期（PR-2）

**Files:**
- Create: `crates/engine/src/xsec/features.rs`
- Modify: `crates/engine/src/xsec/mod.rs`
- Modify: `crates/engine/Cargo.toml`（`strategy-indicators.workspace = true`）
- Test: `crates/engine/src/xsec/features.rs` 内联测试

**Interfaces:**
- Produces:
```rust
pub struct FeatureMap {
    pub values: BTreeMap<String, f64>,
    pub labels: BTreeMap<String, String>,
}
impl FeatureMap {
    pub fn number(&self, ind: &str) -> Option<f64>;
    pub fn label(&self, ind: &str) -> Option<&str>;
}

pub struct FeaturePlan { /* private specs */ }
impl FeaturePlan {
    pub fn from_config(cfg: &XsecConfig) -> Result<Self, Vec<String>>;
    pub fn compute(&self, state: &AggregationState) -> FeatureMap;
}
```
- 语义：每个指标在其声明的 tf 的**已闭合**序列上计算；两次闭合之间取最后已闭合值。指标输出为 NaN 或历史不足 → 该特征不写入 `FeatureMap`（不伪造 0）。
- `macd` 额外产出 `macd_cross` 标签：`macd_line > signal -> "bullish"`，`< -> "bearish"`，`== -> "equal"`。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn features_use_last_closed_value_on_declared_tf() {
    // 合成 1m 200 根；state M1+H1；adx tf=H1 period=14
    // compute() 的 adx 值 == indicators::adx(high, low, close, 14) 的最后一根
}

#[test]
fn warmup_insufficient_omits_feature_instead_of_zero() {
    // 只有 10 根 H1：adx period 14 为 NaN -> FeatureMap 无 "adx"
}

#[test]
fn macd_cross_label_tracks_last_relation() {
    // 明确构造一段上涨后下跌的 H1 序列，断言最终 label ∈ {"bullish","bearish"}
    // 并断言同一时刻 values["macd"] 与直接 EMA 计算一致（误差 1e-9）
}

#[test]
fn feature_plan_rejects_unknown_indicator_param() {
    // adx 配置 period + bogus -> Err 含 bogus
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::features`
Expected: 编译失败。

- [ ] **Step 3: 实现**

- `FeaturePlan::from_config` 复用 Task 4 的 `resolve_indicator`，任何错误聚合返回。
- `compute(state)`：
  - 对每个 `ResolvedIndicator` 取 `state.closed(tf)`；空或不足直接跳过。
  - `Adx/Rsi/Ema/Sma/Atr`：把 `Bar` 序列转 `high/low/close` 切片调用 `strategy_indicators::{adx,rsi,ema,sma,atr}`，取最后值，NaN 跳过。
  - `Macd`：对 `close` 调 `ema(fast)`/`ema(slow)`，从两序列都非 NaN 的首个索引起计算 `line`；`line_tail.len() < signal_period` 时跳过 `macd` 与 `macd_cross`（warmup 不足不伪造），否则 `signal = ema(line_tail, signal_period)`；取最后值写入 `values["<indicator_name>"]`（键为配置中的指标名，通常 `macd`），并写 `labels["macd_cross"]`。
  - 指标名到 FeatureMap 键：使用配置里的键名（如 `adx`）；`macd_cross` 固定作为派生键。
- 全部计算只读 `state`，无 `now()`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::features`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/Cargo.toml crates/engine/src/xsec/features.rs crates/engine/src/xsec/mod.rs
git commit -m "feat(engine): 特征流水线与两级周期——已闭合 bar 最后值、warmup 缺失不伪造"
```

---

## Task 6: entry/exit 规则纯函数（PR-2/PR-3）

**Files:**
- Create: `crates/engine/src/xsec/evaluator.rs`
- Modify: `crates/engine/src/xsec/mod.rs`
- Test: `crates/engine/src/xsec/evaluator.rs` 内联测试

**Interfaces:**
- Produces:
```rust
pub fn effective_rules<'a>(set: &'a RuleSet, symbol: &str) -> &'a [Rule];
pub fn entry_eligible(rules: &[Rule], features: &FeatureMap) -> (bool, Option<String>);
pub fn exit_triggered(rules: &[Rule], features: &FeatureMap) -> Option<String>;
pub fn rule_matches(rule: &Rule, features: &FeatureMap) -> Option<bool>;
```
- 语义：规则集为 **AND**；`entry_eligible` 全部命中返回 `(true, None)`，否则 `(false, Some("missing feature: adx"))`（缺特征优先报 missing，其次报首个不命中规则 `"{ind} {op} {value}"`）。
- `effective_rules`：`overrides` 命中 `symbol`（ASCII 大小写不敏感）则整组替换 `default`，否则用 `default`。
- `exit_triggered` 全部命中时返回规则摘要（与不命中原因同格式），否则 `None`。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn gt_is_strict_and_gte_includes_boundary() {
    // features adx=20.0：gt 20 -> false；gte 20 -> true；lt 20 -> false；lte 20 -> true
}

#[test]
fn missing_feature_fails_entry_with_reason() {
    let (ok, reason) = entry_eligible(&[rule("adx", Gt, 20.0)], &FeatureMap::default());
    assert!(!ok);
    assert_eq!(reason.as_deref(), Some("missing feature: adx"));
}

#[test]
fn all_rules_must_match() {
    // adx>20 且 rsi<70：只命中一条 -> false
}

#[test]
fn overrides_replace_default_wholesale() {
    // default [adx>20]；overrides {"BTCUSDT": [adx>25]} -> BTC 用 25，ETH 用 default
}

#[test]
fn text_equality_for_macd_cross() {
    // label bearish + Eq bearish -> true；Eq bullish -> false；Ne -> 取反
}

#[test]
fn exit_triggers_only_when_all_rules_match() {
    // 空规则集（default=[]，无 override）-> None（平仓只由策略/组合风控决定）
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::evaluator`
Expected: 编译失败。

- [ ] **Step 3: 实现**

比较逻辑：
- `Number`: `Gt/Lt/Ge/Le/Eq/Ne` 用对应 `partial_cmp`；NaN 视为不命中。
- `Text`: 仅 `Eq/Ne`；大小写不敏感比较。
- `RuleValue` 与特征类型不匹配（Text 规则配 number 特征等）视为不命中。
- 空 `exit` 规则集返回 `None`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::evaluator`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/evaluator.rs crates/engine/src/xsec/mod.rs
git commit -m "feat(engine): entry/exit 规则纯函数求值（AND、override 整组替换、缺失特征不伪造）"
```

---

## Task 7: 策略端口与 `SingleInstrumentAdapter`（PR-2）

**Files:**
- Create: `crates/engine/src/xsec/port.rs`
- Create: `crates/engine/src/xsec/adapter.rs`
- Modify: `crates/engine/src/xsec/mod.rs`
- Test: `crates/engine/src/xsec/port.rs`、`crates/engine/src/xsec/adapter.rs` 内联测试

**Interfaces:**
- Produces:
```rust
pub struct InstrumentSnapshot {
    pub instrument: InstrumentId,
    pub bar: Bar,
    pub position: Option<PositionView>,
    pub stale: bool,
    pub features: FeatureMap,
    pub entry_eligible: bool,
    pub entry_reason: Option<String>,
    pub exit_triggered: Option<String>,
    // G5 扩展：旧策略经 Adapter 复现 Ctx 所必需
    pub trading_day: TradingDay,
    pub no_trade: bool,
    pub entry_confirm: bool,
    pub signal_resume: bool,
    pub cooldown: Option<Cooldown>,
    pub session_open_price: Option<f64>,
    pub closed: Option<crate::bridge::CandleSeries>,
}

pub struct PortfolioContext {
    pub strategy_id: String,
    pub nav: f64,
    pub drawdown: f64,
    pub portfolio_pnl_pct: f64,
    pub sub_positions: Vec<SubPosition>,
    pub multipliers: std::collections::BTreeMap<String, f64>, // instrument key -> multiplier
    pub rebalance_due: bool,
    pub no_trade: bool,
}

pub enum InstrumentAction {
    // price/atr 为 G5 扩展：旧 Action::Open 携带它们，Adapter 必须无损映射；
    // 新横截面策略通常 price = snapshot.bar.close、atr = 0.0。
    Open { instrument: InstrumentId, side: Side, price: f64, stop_price: f64, atr: f64, strength: f64, reason: String },
    Close { instrument: InstrumentId, price: f64, reason: String, is_stop_loss: bool, strength: f64 },
    // G5 扩展：旧 Action::Reverse 必须先平后开且平仓失败则放弃开仓，
    // Adapter 以单动作透传，Legacy 路径交回 ActionDispatcher::dispatch 原生处理。
    Reverse { instrument: InstrumentId, side: Side, price: f64, stop_price: f64, atr: f64, strength: f64, close_reason: String, is_stop_loss: bool },
    Adjust { instrument: InstrumentId, target_qty: f64, reason: String },
    UpdateStop { instrument: InstrumentId, stop_price: f64 },
    UpdateExit { instrument: InstrumentId, exit_active: bool, exit_price: f64 },
    Hold,
}

pub enum SignalIdNamespace { Legacy, Sub }

pub trait CrossSectionalStrategyPort {
    fn on_cross_section_bars(
        &mut self,
        newly_closed: &[Timeframe],
        snapshots: &[InstrumentSnapshot],
        portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction>;
    /// 旧策略经 Adapter 返回 Legacy（沿用 sig_ ID 与 ActionDispatcher sizing），
    /// 新横截面策略返回 Sub。
    fn signal_id_namespace(&self) -> SignalIdNamespace { SignalIdNamespace::Sub }
}
```
- `SingleInstrumentAdapter`：
```rust
pub struct SingleInstrumentAdapter { /* Box<dyn Strategy + Send> */ }
impl SingleInstrumentAdapter {
    pub fn new(strategy: Box<dyn strategy_core::strategy::Strategy + Send>) -> Self;
}
impl CrossSectionalStrategyPort for SingleInstrumentAdapter { ... }
```
- Adapter 行为必须逐项对齐 `LegacyStrategyBridge::on_closed_bars`：
  - `Ctx::new(closed_source, bar.close, bar_to_candle(&bar), bar.open_time, position, cooldown)`；
  - `.with_newly_closed(newly_closed)`、`.with_entry_confirm`、`.with_signal_resume`、`.with_session_open_price`；
  - 有持仓调 `on_realtime_tick`，空仓调 `on_candle`；
  - 使用 `snapshots[0].closed`（必须存在；缺失时返回 `vec![Hold]` 并 `debug_assert`）。
  - `Action -> InstrumentAction` 一一映射（含 `Reverse -> InstrumentAction::Reverse`，保留「先平后开、平仓失败则放弃开仓」语义）；`Action::Hold -> Hold`。
  - `signal_id_namespace() -> Legacy`。

- [ ] **Step 1: 写失败测试**

`adapter.rs`：
```rust
struct TestStrategy { /* 记录调用；按 fixture 返回 Action */ }

#[test]
fn adapter_maps_every_action_variant() {
    // Open/Close/Reverse/UpdateStop/UpdateExit/Hold -> InstrumentAction 逐项断言
    // Reverse 保持单动作并携带 close_reason/is_stop_loss/price/atr
}

#[test]
fn adapter_routes_flat_to_on_candle_and_positioned_to_realtime_tick() {
    // TestStrategy 内部计数 on_candle/on_realtime_tick；
    // snapshot.position=None -> calls.on_candle == 1；
    // 同一 adapter 再喂 position=Some(PositionView) -> calls.on_realtime_tick == 1。
}

#[test]
fn adapter_namespace_is_legacy() {
    let adapter = SingleInstrumentAdapter::new(Box::new(TestStrategy::default()));
    assert_eq!(adapter.signal_id_namespace(), SignalIdNamespace::Legacy);
}

#[test]
fn adapter_builds_ctx_with_newly_closed_and_resume_flags() {
    // TestStrategy 记录 Ctx：bar_closed(Timeframe::H1) == true（newly_closed 含 H1）、
    // entry_confirm == false、signal_resume == true、session_open_price == Some(101.0)；
    // 与 LegacyStrategyBridge 同样构造顺序。
}
```

`port.rs`：`InstrumentSnapshot` / `PortfolioContext` / `InstrumentAction` 的构造与 `Vec` 排序测试（按 namespaced_key 构造顺序稳定）。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::adapter`
Expected: 编译失败。

- [ ] **Step 3: 实现**

- `port.rs` 定义类型；`Side` 用 `strategy_core::signal::Side`；`PositionView` 用 `strategy_core::strategy::PositionView`；`CandleSeries` 从 `crate::bridge` 引入。
- `adapter.rs` 实现映射与 `Ctx` 构造；`bar_to_candle` 复用 `crate::bridge::bar_to_candle`。
- 在 `xsec/mod.rs` 追加 `pub mod port; pub mod adapter;`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::adapter`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/port.rs crates/engine/src/xsec/adapter.rs crates/engine/src/xsec/mod.rs
git commit -m "feat(engine): CrossSectionalStrategyPort 与 SingleInstrumentAdapter（旧策略零改码）"
```

---

## Task 8: 多源对齐事件源 `AlignedEventSource`（PR-2）

**Files:**
- Create: `crates/engine/src/xsec/align.rs`
- Modify: `crates/engine/src/xsec/mod.rs`
- Test: `crates/engine/src/xsec/align.rs` 内联测试

**Interfaces:**
- Produces:
```rust
pub struct AlignedEvent {
    pub at: DateTime<Utc>,
    pub slot: usize,
    pub event: MarketEvent,
}

pub struct AlignedEventSource { /* sources + 每源 pending + last_at */ }
impl AlignedEventSource {
    /// `keys` 为 slot 的 instrument namespaced_key，仅用于确定性排序兜底。
    pub fn new(sources: Vec<Box<dyn BarEventSource>>, keys: Vec<String>) -> Self;
    pub fn next_aligned(&mut self) -> Result<Option<AlignedEvent>>;
}
```
- 事件时间规则：`Bar -> bar.open_time`（rank 1）；`SessionGap -> to.unwrap_or(from)`（rank 0）；`DayBoundary -> 该源上一根 bar 时间 + 1m`（rank 0）；`Roll -> effective_at`（rank 0）；`Reconnected -> 该源上一根 bar 时间`（rank 0）。
- 排序键 `(at, rank, slot)`；bar 在同时刻的 gap/day 之后。每源缓冲一个 pending 事件，选最小者发射。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn interleaves_two_sources_by_timestamp_then_slot() {
    // s0: t0,t1,t2；s1: t0,t1,t2 -> 依次 (t0,0),(t0,1),(t1,0),(t1,1),(t2,0),(t2,1)
}

#[test]
fn gap_and_day_events_precede_same_time_bar() {
    // s0: bar 00:00、gap(from 00:01,to 00:02)、bar 00:02
    // 输出顺序：bar 00:00、gap、bar 00:02；gap.at = 00:02 且 rank 0
}

#[test]
fn missing_bar_in_one_source_does_not_block_the_other() {
    // s0 有 t1，s1 没有 -> t1 只发射 slot 0（供 runner 标记 stale）
}

#[test]
fn exhausted_sources_are_skipped() {
    // s0 只有 t0；s1 有 t0,t1 -> 输出 (t0,0),(t0,1),(t1,1)，随后 None
}

#[test]
fn deterministic_when_timestamps_tie() {
    // 两个 slot 同一时刻：两次独立构造 AlignedEventSource 并跑完，
    // 输出 (at, slot) 序列逐项相等，且 slot 0 排在 slot 1 前。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::align`
Expected: 编译失败。

- [ ] **Step 3: 实现**

- 内部 `pending: Vec<Option<(DateTime<Utc>, u8, MarketEvent)>>`、`last_at: Vec<Option<DateTime<Utc>>>`、`done: Vec<bool>`。
- `next_aligned` 循环填充所有空 pending（对每个未耗尽源 `next_event()`，按规则赋 `at`；`DayBoundary` 用 `last_at + 1m`，`last_at` 为空时用 `Utc::now()` 作为仅回测不会触发的兜底并 `debug_assert`）。
- 选 `(at, rank, slot)` 最小；清空该 pending，更新 `last_at`（bar 才更新），返回。
- `Reconnected` 不改变 `last_at`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::align`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/align.rs crates/engine/src/xsec/mod.rs
git commit -m "feat(engine): AlignedEventSource 多源按时间/slot 确定性对齐"
```

---

## Task 9: `XsecSlot` 与子仓位下单 `SubDispatcher`（PR-2/PR-3）

**Files:**
- Create: `crates/engine/src/xsec/dispatch.rs`
- Create: `crates/engine/src/xsec/runner.rs`（本任务先只放 `XsecSlot` 与 `SlotInit`）
- Modify: `crates/engine/src/xsec/mod.rs`
- Modify: `crates/engine/src/core/execution.rs`（`OrderRequest` 加法字段）
- Modify: `crates/engine/src/core/action_dispatcher.rs`（构造点补 `None`）
- Modify: `crates/engine/src/exchanges/live.rs:308`（构造点补 `None`）
- Modify: `crates/engine/tests/live_bridge.rs`（6 处构造点补 `None`）
- Modify: `crates/app/tests/spi_live_fee_source.rs`（3 处构造点补 `None`）
- Test: `crates/engine/src/xsec/dispatch.rs` 内联测试（用 mock `ExecutionPort`）

**Interfaces:**
- Produces:
```rust
pub struct SlotInit {
    pub parts: ExchangeParts,
    pub timeframes: Vec<Timeframe>,          // 含 base TF；已按配置解析
    pub feature_plan: FeaturePlan,
    pub risk: strategy_core::risk::RiskControlConfig, // L1：仅 fixed_stop_loss_pct，trailing/TP 关闭
    pub sizing: SizingParams,                // Legacy 路径用
    pub strategy_type: String,
    pub max_closed_len: usize,
}

pub struct XsecSlot {
    pub parts: ExchangeParts,
    pub state: AggregationState,
    pub pos_sync: PositionSync,
    pub risk_guard: RiskGuard,
    pub dispatcher: ActionDispatcher,        // Legacy namespace 复用
    pub feature_plan: FeaturePlan,
    pub candle_cache: crate::bridge::CandleSeries, // Legacy adapter 的 ClosedSource 快照
    pub sub_positions: Vec<SubPosition>,
    pub last_mark: Option<f64>,
    pub last_bar_at: Option<DateTime<Utc>>,
    pub stale: bool,
    pub pending_newly: Vec<Timeframe>,
    pub blocked_closed: Vec<Timeframe>,
    pub signal_blocked_prev: bool,
    pub last_day: Option<TradingDay>,
}
impl XsecSlot {
    pub fn new(init: SlotInit) -> Self;
    pub fn instrument_key(&self) -> String;   // namespaced_key()
    pub fn take_source(&mut self) -> Box<dyn BarEventSource>; // 替换为 VecEventSource::default()
    pub fn preload(&mut self, bars: &[Bar]) -> Result<()>;
}

pub struct StrategyIds<'a> {
    pub strategy_type: &'a str,
    pub strategy_id: &'a str,
    pub rebalance_epoch: u64,
}
pub struct DispatchOutcome { pub actions: u64, pub rejected: u64, pub signal_ids: Vec<String> } // signal_ids = accepted 腿

pub fn open_position(
    slot: &mut XsecSlot,
    ids: &StrategyIds<'_>,
    side: Side,
    price: f64,
    qty: f64,
    stop_price: f64,
    at: DateTime<Utc>,
    reason: &str,
    intent: SubIntent,
) -> Result<DispatchOutcome>;

pub fn close_position(
    slot: &mut XsecSlot,
    ids: &StrategyIds<'_>,
    price: f64,
    reason: &str,
    is_stop_loss: bool,
    intent: SubIntent,
) -> Result<DispatchOutcome>;

pub fn adjust_position(
    slot: &mut XsecSlot,
    ids: &StrategyIds<'_>,
    target_qty: f64,
    price: f64,
    at: DateTime<Utc>,
    reason: &str,
    intent: SubIntent,
) -> Result<DispatchOutcome>;
```
- **`OrderRequest` 规格加法**（`crates/engine/src/core/execution.rs`，`ExecutionPort` trait 零改动）：
```rust
pub struct OrderRequest {
    /* ...既有字段不变... */
    /// 归属策略实例（横截面链路写 `strategy_id`；旧链 None，serde 兼容）
    #[serde(default)]
    pub strategy_id: Option<String>,
    /// 订单意图（`sub_` 链路写 `SubIntent::as_str()`；旧链 None）
    #[serde(default)]
    pub intent: Option<String>,
}
```
  `action_dispatcher.rs` 的 3 处 `OrderRequest` 构造点（open / close leg）补 `strategy_id: None, intent: None`；`dispatch.rs` 的 sub 链路填 `Some(ids.strategy_id.clone())` 与 `Some(intent.as_str().into())`。
- `open_position`：若该 instrument 已有非 `Closed` 子仓位（1:1 约束）→ 直接 `rejected += 1` 并返回；否则 `signal_id = generate_sub_signal_id(... intent ...)`；先建 `SubPosition { state: PendingOpen, id: signal_id, target_weight: 0.0, ... }` 写入 `slot.sub_positions`，`open_meta.insert(signal_id, (stop_price, 0.0, 0.0))`；`execution.submit`；accepted → `advance_sub_state(PendingOpen, SubmitAccepted)` = OpenRequested；rejected → 删除该行、`rejected += 1`。
- `close_position`：若该 instrument 的 sub 已是 `CloseRequested/Closed` 则直接返回（1:1 约束，防重复平仓）；否则先 `advance_sub_state(Active, CloseRequested)`，再按现有 `PositionRules::split_close` 生成腿；每腿 `OrderRequest { signal_id: format!("{base}_leg{i}"), ... }`；`base = generate_sub_signal_id(..., side = 持仓方向的反向（平多=Short/平空=Long）, intent)`；accepted 的腿计入 `actions`；**全部腿被拒**（accepted=0）→ 子仓位回滚为 `Active` 并计 `rejected += 1`（允许下一轮/下一窗口重试）。
- `adjust_position`：当前 `SubPosition` qty > target → 内部 `close_qty_inner(slot, ids, diff, intent=RebalanceReduce)` 部分平仓；target > current → `open_position` 差额（`intent=Open`）；`qty <= 1e-12` → no-op。
- 所有下单前调用 `trading.normalize_qty` / `normalize_price`。

- [ ] **Step 1: 写失败测试**

dispatch.rs 内联测试使用 `RecordingExecution` 实现 `ExecutionPort`（记录 `OrderRequest`，accepted/rejected 可配置）；`XsecSlot` 用 `BinanceExchange` 快速装配（crypto perpetual，费率 0.0004）。测试：
```rust
#[test]
fn open_submits_expected_order_and_creates_subposition() {
    // signal_id 前缀 sub_ 且与 generate_sub_signal_id 一致；qty 规整；ack 后 state == OpenRequested
}

#[test]
fn rejected_open_removes_subposition_and_counts() {
    // execution 返回 accepted=false：
    // slot.sub_positions 为空、outcome.rejected == 1、outcome.actions == 0。
}

#[test]
fn close_emits_leg_ids_and_reduce_only() {
    // 1 lot -> 1 腿；order.signal_id == format!("{base}_leg0")、
    // order.reduce_only == true、order.side 反向、order.offset 由 split_close 给出；
    // 相同输入第二次调用 base 相同（确定性）。
}

#[test]
fn adjust_up_opens_diff_and_adjust_down_closes_diff() {
    // current 0, target 2 -> 1 笔 Open，qty == 2；
    // 之后 current 2, target 0.5 -> 1 笔 Close，qty == 1.5（部分平仓）；
    // 两侧 OrderRequest.strategy_id == Some("xsec_momentum:default")、
    // intent == Some("open"/"rebalance_reduce")。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::dispatch`
Expected: 编译失败。

- [ ] **Step 3: 实现**

- `runner.rs` 先实现 `SlotInit` / `XsecSlot`。`XsecSlot::new` 用 `with_strategy_type` 同等方式初始化 `state`、`pos_sync`、`risk_guard`、`dispatcher`；`risk_guard = RiskGuard::with_config(risk)`；`dispatcher.sizing = sizing`。
- `take_source`：`std::mem::replace(&mut self.parts.source, Box::new(VecEventSource::default()))`。
- `preload`：逐根 `state.on_closed_1m(bar.clone(), parts.aggregation.as_ref(), parts.session.as_ref())`。
- `dispatch.rs` 实现 `open_position` / `close_qty_inner` / `adjust_position`；close 腿用 `CloseRequest { instrument, qty, at }` 与 `PositionRules::split_close`；`OrderRequest` 透传 `is_stop_loss`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::dispatch`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/runner.rs crates/engine/src/xsec/dispatch.rs crates/engine/src/xsec/mod.rs
git commit -m "feat(engine): XsecSlot 与子仓位下单（sub_ ID、Adjust、reduce-only 腿）"
```

---

## Task 10: `CrossSectionalRunnerGroup` 轮次编排（PR-2）

**Files:**
- Modify: `crates/engine/src/xsec/runner.rs`（追加 group）
- Test: `crates/engine/src/xsec/runner.rs` 内联测试

**Interfaces:**
- Produces:
```rust
pub struct RoundReport {
    pub at: DateTime<Utc>,
    pub nav: f64,
    pub cash: f64,
    pub fills: Vec<Fill>,
    pub actions: u64,
    pub rejected: u64,
    /// true = base TF 闭合触发的决策轮；false = 仅 1m 盯市/成交同步
    pub decision_round: bool,
}

#[derive(Clone)]
pub struct XsecRunSummary {
    pub strategy_id: String,
    pub bars_processed: u64,
    pub provisional_bars: u64,
    pub bars_skipped_stale: u64,
    pub actions: u64,
    pub rejected_actions: u64,
    pub fills: Vec<Fill>,
    pub final_sub_positions: Vec<SubPosition>,
    pub final_portfolio: StrategyPortfolio,
}

pub struct CrossSectionalRunnerGroup {
    pub strategy_id: String,
    pub strategy_type: String,
    pub config: XsecConfig,
    pub slots: Vec<XsecSlot>,
    pub strategy: Box<dyn CrossSectionalStrategyPort>,
    pub portfolio: StrategyPortfolio,
    pub phase: PortfolioPhase,
    /* private: bars_since_rebalance, pending round, paused_after_panic, summary */
}
impl CrossSectionalRunnerGroup {
    pub fn new(
        strategy_id: &str,
        strategy_type: &str,
        config: XsecConfig,
        slots: Vec<XsecSlot>,
        strategy: Box<dyn CrossSectionalStrategyPort>,
        /// 回测/实盘状态落盘门面；None = 纯内存（单测可用）
        store: Option<std::sync::Arc<strategy_store::Store>>,
    ) -> crate::core::error::Result<Self>;
    pub fn preload(&mut self, warmup: &[Vec<Bar>]) -> crate::core::error::Result<()>;
    pub fn sources(&mut self) -> (Vec<Box<dyn BarEventSource>>, Vec<String>);
    pub fn on_event(&mut self, ev: AlignedEvent) -> crate::core::error::Result<Option<RoundReport>>;
    pub fn finish(&mut self) -> crate::core::error::Result<XsecRunSummary>;
    /// 取走最后一次 flush 的轮次报告（`finish` 后调用可补采最后一根净值）
    pub fn take_last_round(&mut self) -> Option<RoundReport>;
    pub fn master_position(&self) -> MasterPositionView;
    pub fn is_paused(&self) -> bool;
    pub fn take_fills(&mut self) -> Vec<Fill>;
}
```
- 轮次语义（**base TF 是决策节奏，1m 仍逐根驱动聚合/成交/盯市**）：
  - `on_event` 收到 `Bar`：非 `Final` 帧只计入 `summary.provisional_bars` 后返回（不进入聚合/轮次）；`Final` 帧喂 slot 的 `AggregationState`，执行 `execution.on_bar`、`pos_sync.on_price`、`handle_session`（日界/gap 与旧 runner 同构，按 slot 独立），`summary.bars_processed += 1`。
  - 该 bar 返回的 `newly_closed` 若**不含 base TF**（`config.timeframes[0]`）：只做 ① sync fills/books + 更新 marks/portfolio，返回 `Some(RoundReport { actions: 0, rejected: 0, decision_round: false, .. })`（供 app 逐 1m 采净值）；不调用 L2/L3/策略/L1。
  - 若**含 base TF**：把该 slot 加入本轮决策集合；当同一 `ev.at` 的所有 slot bar 处理完后（或下一时间戳到达时）执行一次 `flush_round`，返回 `RoundReport { decision_round: true, .. }`。若某一 slot 在该 at 没有 base TF 闭合（缺 bar），它在 snapshots 中 `stale=true`。
  - 非 `Bar` 事件：按 slot 分发 `SessionGap/DayBoundary/Roll`，行为与 `ExchangeRunner::on_event` 相同；不触发决策轮次。
  - `flush_round` 顺序严格按设计 §7.3：① sync fills/books（含子仓位落账）→ ② L3（本任务占位 false）→ ③ L2（本任务占位 None）→ ④ quorum + 策略决策 → ⑤ L1 fallback → dispatch。`newly_closed` 传本轮 base TF 闭合的并集（含 M1 与 base TF）。
  - **子仓位落账（① 的一部分）**：每 slot `drain_fills()` 后逐笔：
    - `Offset::Open` → 按 `signal_id` 找 `PendingOpen/OpenRequested` 子仓位，`advance_sub_state(_, FillOpen{qty,price,at})` 落 `Active`，`qty/entry_price/opened_at` 用 Fill 值；找不到（例如直接成交）则新建 Active 子仓位并以 Fill.signal_id 为 id。
    - 平仓 Fill → 找该 instrument 唯一非 Closed 子仓位：`qty -= fill.qty`、`realized_pnl += fill.realized_pnl`；若该 sub 处于 `CloseRequested`，`advance_sub_state(CloseRequested, FillClose { qty, realized_pnl, at: fill.at, remaining_after: new_qty })`（>1e-12 停留 `CloseRequested`，否则 `Closed`，`closed_at = fill.at`）；若处于 `Active`，部分平仓后仍 `Active`。
    - 同步 `slot.pos_sync.apply_fill(fill)`（复用旧链路），并 `sync_book`。
  - snapshots：`bar = state.closed(base_tf).back().cloned()`（本轮该 slot 无 base TF 闭合则用上一根并 `stale=true`、`entry_eligible=false`；**禁止用 1m bar 冒充 base bar**）；`features = slot.feature_plan.compute(&slot.state)`；`(entry_eligible, entry_reason) = evaluator::entry_eligible(effective_rules(&config.entry, symbol), &features)`（stale slot 直接 `false`）；`exit_triggered`（本任务 `None`）、`position`、`closed`（`CandleSeries::refresh` 克隆）、`trading_day/no_trade/cooldown/...`。
  - **entry 硬门**：`entry_eligible == false` 的 slot，其 `Open` 与「向上 `Adjust`」（target_qty > 当前 qty）在 dispatch 前丢弃并 `rejected_actions += 1`；`Close`/向下 `Adjust` 不受限。`port_context.no_trade` 或本轮 quorum 不达标时**不调用策略**。
  - **L1 兜底**：仅 `!no_trade` 时；策略对本 slot 沉默且 `pos_sync.pos` 存在时，用 slot 的 `RiskGuard::check_fallback`（与旧 runner 同源）产出 Close，经 `close_position(..., is_stop_loss=true, intent=SubStop)` 派发；Legacy 路径同样转成 `StrategyAction::Close` 交 `ActionDispatcher`。
  - dispatch 借用拆分：Legacy 路径下单时按字段解构 `XsecSlot`（`let XsecSlot { dispatcher, parts, pos_sync, .. } = slot;`），避免 `&mut slot` 与 `&mut slot.parts.execution` 同时借用的编译错误。
  - quorum：`non_stale_slots >= ((4 * n) + 4) / 5` 才调用策略；不达标 `bars_skipped_stale += 1`，只跑 L1/L3（L3 占位）与 fills 同步。
  - `PortfolioContext`：`nav = Σ slot.execution.account_snapshot().equity()`（每 slot 初始资金相等均分）、`drawdown = if high_watermark > 0 { (high_watermark - nav)/high_watermark } else { 0.0 }`、`portfolio_pnl_pct` 由 `master_position` 计算、`sub_positions` 克隆、`multipliers` 由 `trading.spec` 填、`rebalance_due = bars_since_rebalance >= every_bars.unwrap_or(u64::MAX)`、`no_trade` = 本轮任一 slot `no_trade`。
  - dispatch 按 `strategy.signal_id_namespace()`：
    - `Legacy`：把 `InstrumentAction` 转回 `StrategyAction`，调用该 slot 的 `ActionDispatcher::dispatch`（`DecisionContext` 由 slot 生成，字段与 `ExchangeRunner::build_ctx` 一致），更新 `summary.actions/rejected`。
    - `Sub`：`Open` qty = `nav * sizing.per_position_pct / (price * multiplier)`，再按 `risk.max_total_notional` 剩余容量裁剪；`Close` / `Adjust` 按 Task 9；`UpdateStop` 同时回写 `pos_sync.pos.stop_price` 与该 instrument 的 `SubPosition.stop_price`；`UpdateExit` 回写 `pos_sync.pos.exit_active/exit_price`；`Hold` no-op。
  - 每轮 `rebalance_due` 且产生了 Open/Adjust/Close → `rebalance_epoch += 1`、`last_rebalance_at = Some(at)`、`bars_since_rebalance = 0`。
  - `bars_since_rebalance` 在每轮存在 base TF 闭合时 +1；`nav`/`high_watermark`/`drawdown` 每轮刷新并写回 `portfolio`。
  - **持久化**：`store` 为 `Some` 时，任何 `SubPosition` 新建/状态/数量变化后 `store.table::<SubPosition>().upsert(sub.clone())`；每轮结束 `store.table::<StrategyPortfolio>().upsert(portfolio.clone())`。回测结束由 app `store.flush()`。
- **本任务 L2/L3 为占位**（Task 15/16 实现），但轮次顺序已固定。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn round_processes_all_slots_at_same_timestamp_once() {
    // base TF = 1m；两个 VecEventSource 同一 t 各一根，下一根 t+1m 到达时 flush：
    // 返回的 t 轮次 RoundReport.decision_round == true；
    // calls.len() == 1、snapshots.len() == 2、report.fills 为空、bars_processed 累加 2。
}

#[test]
fn base_tf_close_gates_strategy_and_1m_ticks_do_not() {
    // base TF = 15m；喂 15 根 1m：前 14 根返回 decision_round=false、策略 0 次调用；
    // finish() 后 take_last_round() 为 decision_round=true、策略 1 次调用
    // （决策在 base 闭合的 at 上、下一事件或 finish 时 flush）。
}

#[test]
fn missing_slot_is_marked_stale_and_quorum_skips_strategy() {
    // N=3，base TF 闭合时只有 2 个有 bar -> non_stale=2,
    // ((4*3)+4)/5 == 3 -> 策略 0 次调用；
    // 缺失 slot 的 snapshot.stale == true 且 entry_eligible == false；
    // summary.bars_skipped_stale == 1。
}

#[test]
fn nav_and_high_watermark_update_after_round() {
    // 每 slot 初始资金 1000/N；每个 1m RoundReport.nav == Σ account.equity()；
    // portfolio.nav == report.nav；high_watermark == max(历史 nav)。
}

#[test]
fn base_tf_close_increments_rebalance_counter_and_epoch() {
    // base TF = 15m；喂 30 根 1m（两个 15m 桶）；every_bars=2 ->
    // 第二个 base 闭合时 rebalance_due；策略返回 Adjust 并成交 ->
    // rebalance_epoch == 1、last_rebalance_at == 该 base 闭合 at、
    // bars_since_rebalance == 0。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::runner`
Expected: 编译失败。

- [ ] **Step 3: 实现**

- `runner.rs` 追加 group 与轮次状态；复用 `aggregate_and_gate` 的 blocked/pending newly 逻辑（按 slot 字段）。
- `flush_round` 返回 `RoundReport`；`on_event` 在切换轮次时返回上一个 report；`finish` flush 最后一个轮次并收口 summary。
- `sources()` 依次 take 每个 slot 的 source，key 用 `instrument_key()`。
- panic 捕获留到 Task 11（本任务直接调用）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::runner`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/runner.rs
git commit -m "feat(engine): CrossSectionalRunnerGroup 对齐轮次编排与组合 NAV/轮换计数"
```

---

## Task 11: N=1 旧策略等价 + panic 断路器 + 恢复（PR-2）

**Files:**
- Modify: `crates/engine/src/xsec/runner.rs`
- Test: `crates/engine/src/xsec/runner.rs` 内联测试

**Interfaces:**
- 追加：
```rust
impl CrossSectionalRunnerGroup {
    /// panic 后恢复：只信任 Store 落盘行；内存中间态（含策略实例）全部丢弃。
    /// `fresh_strategy` 必须是调用方新建的实例（旧实例可能处于 panic 时的半状态）。
    pub fn resume_from_store(
        &mut self,
        store: &strategy_store::Store,
        fresh_strategy: Box<dyn CrossSectionalStrategyPort>,
    ) -> crate::core::error::Result<()>;
}
```
- `flush_round` 的 ④ 段用 `std::panic::catch_unwind(AssertUnwindSafe(|| self.strategy.on_cross_section_bars(...)))`；捕获后：
  - `self.phase = PortfolioPhase::Paused`；
  - `tracing::error!(strategy_id, "策略 panic，已暂停该 RunnerGroup")`；
  - 本轮不发策略动作；L3/L1/fills 同步照常。
- `resume_from_store`：
  - 从 `store.table::<SubPosition>()` 按 `strategy_id` 收 `by_scope`，按 instrument 归位重建 `slot.sub_positions`；
  - 从 `store.table::<StrategyPortfolio>().get(strategy_id)` 恢复 `portfolio`（没有则默认）；
  - 清空各 slot `pos_sync.pos` / `open_meta` 后按恢复的 subpositions 重建 `PositionState`（entry/qty/side/stop/strategy_id）；
  - 以 `execution.remote_positions` 对账：远程为空但 sub 非 Closed → 置 `Closed` 并告警；
  - `phase = Running`，`trail_armed`/peak 来自 Store。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn n1_legacy_adapter_matches_exchange_runner() {
    // 同一 TestStrategy（timeframes = [M1]，base TF=1m，调用节奏与旧 runner 一致）
    // + 同一 1m 序列 + 同一 BinanceExchange +
    // 同一 RiskControlConfig（两侧 L1 一致；本测试 portfolio_exit 为 None）：
    // (a) ExchangeRunner + LegacyStrategyBridge
    // (b) RunnerGroup(N=1) + SingleInstrumentAdapter
    // 断言 fills 的 (signal_id, side, qty, price, realized_pnl) 逐笔一致，
    // actions/rejected 一致，最终 SubPosition 与 PositionState 的 (side, qty, entry) 一致。
}

#[test]
fn panicking_strategy_pauses_only_its_group() {
    // 两个 group（A/B），各 3 根同构 bars；A 的策略在第一轮 panic：
    // A.is_paused() == true；A 之后返回的 RoundReport 均无 actions/fills；
    // B 正常跑完 3 根，summary.bars_processed == 3。
}

#[test]
fn resume_from_store_discards_untrusted_memory_and_rebuilds() {
    // Store 预置 Active SubPosition（A=1 手，entry=100）；
    // 暂停后把内存 sub_positions 改成 qty=999 并清空 pos_sync；
    // resume_from_store(&store, Box::new(TestStrategy::default())) ->
    // 内存 qty == 1、strategy_id 正确、pos_sync.pos 重建、strategy 是新实例；
    // 远程（模拟 execution）无仓 -> 该 sub.state == Closed 并告警；phase == Running。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::runner`
Expected: 新测试失败。

- [ ] **Step 3: 实现**

- 加 `catch_unwind`、`phase` 写入、`resume_from_store`。
- `catch_unwind` 用 `AssertUnwindSafe`（panic 后该实例内存全部弃用，注释说明断言依据）。
- `resume_from_store` 先 `self.strategy = fresh_strategy`，再重建账本；`PositionState` 重建时 `strategy_id = Some(self.strategy_id.clone())`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::runner`
Expected: PASS（等价性、断路器、恢复）。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/runner.rs
git commit -m "feat(engine): N=1 旧策略等价、per-strategy panic 断路器与 Store 重建恢复"
```

---

## Task 12: 示例横截面策略 crate `xsec_momentum`（PR-4）

**Files:**
- Create: `crates/xsec-strategies/Cargo.toml`
- Create: `crates/xsec-strategies/src/lib.rs`
- Create: `crates/xsec-strategies/src/momentum.rs`
- Modify: `Cargo.toml`（workspace members + `strategy-xsec-strategies`）
- Modify: `crates/app/Cargo.toml`（依赖）
- Test: `crates/xsec-strategies/src/momentum.rs` 内联测试

**Interfaces:**
- Produces:
```rust
// lib.rs
pub mod momentum;
pub fn all() -> &'static [&'static str]; // ["xsec_momentum"]
pub fn create(
    name: &str,
    params: Option<&serde_json::Value>,
) -> anyhow::Result<Box<dyn strategy_engine::xsec::port::CrossSectionalStrategyPort>>;
```
- `momentum.rs`：
```rust
#[derive(serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MomentumParams { pub top_k: usize, pub bottom_k: usize }
impl Default for MomentumParams { fn default() -> Self { Self { top_k: 3, bottom_k: 0 } } }
pub struct XsecMomentum { params: MomentumParams }
impl CrossSectionalStrategyPort for XsecMomentum { ... }
```
- 行为：
  - 仅当 `portfolio.rebalance_due` 时决策；否则全部 `Hold`。
  - 候选 = `!stale && entry_eligible` 且 `features.number("macd")` 存在者；按 `macd` 降序、`namespaced_key()` 升序做 tie-break。
  - 前 `top_k`：空仓 → `Open { price: snapshot.bar.close, atr: 0.0, ... }`；已有仓且 qty 与目标不一致 → `Adjust { target_qty = target_weight * nav / (price * multiplier) }`；一致 → `Hold`。
  - 持有但不在 top_k → `Adjust { target_qty: 0.0 }`。
  - `bottom_k` 本期必须为 0（Task 4 已在 `validate_config` 拒绝非 0；策略忽略该字段）。
  - `stop_price`：多头 `price * 0.98`；本期只做多头。
  - `signal_id_namespace() -> Sub`。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn ranks_by_macd_and_selects_top_k_only_when_rebalance_due() {
    // 3 snapshots macd 0.5/0.2/-0.1，top_k=2，rebalance_due=true：
    // 返回 2 个 Open（BTC/ETH 顺序按 macd 降序），第三个无动作；
    // rebalance_due=false -> 全部 Hold（空 Vec 或 vec![Hold]）。
}

#[test]
fn stale_or_ineligible_snapshots_are_excluded() {
    // 最高 macd 的 snapshot stale=true，次高 entry_eligible=false；
    // top_k=1 -> 选中第三名。
}

#[test]
fn existing_position_gets_adjust_to_target_weight() {
    // snapshot 已有 sub qty=0.5，target_weight=0.5、nav=1000、price=100、mult=1
    // -> Adjust(target_qty == 5.0)；qty 已是 5.0 -> Hold。
}

#[test]
fn held_non_candidate_is_adjusted_to_zero() {
    // 持有仓位的 snapshot 不在 top_k -> Adjust(target_qty == 0.0)。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-xsec-strategies`
Expected: 编译失败（crate 不存在）。

- [ ] **Step 3: 实现**

- 新建 crate；`Cargo.toml` 依赖 `strategy-core`、`strategy-engine`、`chrono`、`serde`、`serde_json`、`anyhow`。
- 根 `Cargo.toml` members 加 `"crates/xsec-strategies"`；`[workspace.dependencies]` 加 `strategy-xsec-strategies = { path = "crates/xsec-strategies" }`；app 依赖加入。
- 实现 `lib.rs` 注册表与 `momentum.rs`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-xsec-strategies`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/xsec-strategies crates/app/Cargo.toml
git commit -m "feat(strategies): xsec_momentum 示例横截面策略与 registry"
```

---

## Task 13: app 横截面配置 loader（PR-2）

**Files:**
- Create: `crates/app/src/xsec_config.rs`
- Modify: `crates/app/src/lib.rs`
- Modify: `crates/app/src/config.rs`（`load_dir` 跳过 stem == `strategy` 的横截面文件，避免默认 `--config config` 把新配置误当 per-symbol 文件）
- Test: `crates/app/tests/xsec_config_guards.rs` + `xsec_config.rs` 内联测试

**Interfaces:**
- Produces:
```rust
pub struct XsecSpec {
    pub strategy_type: String,
    pub strategy_id: String,
    pub config: strategy_engine::xsec::config::XsecConfig,
    pub instruments: Vec<crate::config::SymbolSpec>, // 每 universe 条目一条
    pub warmup_days: u32,
}

pub fn is_xsec_config_path(path: &std::path::Path) -> bool; // 文件 stem == "strategy" 或目录含 strategy.yaml
pub fn load_xsec(path: &std::path::Path) -> anyhow::Result<XsecSpec>;
pub fn make_xsec_strategy(
    spec: &XsecSpec,
) -> anyhow::Result<Box<dyn strategy_engine::xsec::port::CrossSectionalStrategyPort>>;
```
- `load_xsec`：
  - 文件：直接读；`_global.yaml` 依次探测**文件所在目录 → 其父目录**（与既有多文件模式的回落口径一致），用 `crate::config::load(Some(&global))` 取 services/contracts/risk/exchange/account/leverage/warmup_days。
  - 目录：要求存在 `strategy.yaml`/`strategy.yml`，否则 bail 点名目录；`_global.yaml` 依次探测该目录 → 其父目录。
  - 调 `validate_config`；所有错误拼接为 one `anyhow!("横截面配置校验失败:\n- ...")`。
  - universe 解析：`symbol` 大写；`exchange = entry.exchange > global.exchange > services.trading.exchange > "binance"`；规范化 `ctp`/`shfe` → `SHFE`、其余小写（保证 `venue_key_for` 与 `exchange_for_spec` 产出的 `InstrumentId.namespaced_key()` 同键）；全部条目规范化后必须相同，否则 bail「横截面实例只允许单一 exchange，实际: ...」；`namespaced_key` 去重，重复 bail。
  - 每标的 `SymbolSpec`：`strategy = strategy_type`、`timeframe = timeframes[0]`、`cash = global.cash/Defaults::CASH`、`initial_cash = None`、`params = config.params.clone()`、`warmup_days`、`exchange`、`venue_key = crate::spi_backtest::venue_key_for(&exchange, contract.as_ref(), &symbol)`、`account`、`leverage = global.leverage/1`、`mode = "backtest"`、`contract`、`risk`：
    - `universe.len() == 1` **且** `strategy_type` 属于旧 `strategy_strategies` registry：沿用 `global.risk.unwrap_or_default()`（G5：旧单标策略经 Adapter 后 trail 行为不变）。
    - 其余情况（`universe.len() >= 2`，或 N=1 的新横截面策略）：L1 仅硬止损 `RiskControlConfig { enabled: true, fixed_stop_loss_pct: global.risk.unwrap_or_default().fixed_stop_loss_pct, trailing_profit: { enabled: false, ..default }, fixed_take_profit_pct: 0.0 }`（回落/止盈职责归 L3，避免层间打架）。
  - 交易所白名单：`binance`（crypto perpetual，`contract_for` 必须无命中）与 `SHFE`/`ctp`（`contract_for` 必须命中合约参数，否则 bail「SHFE 标的缺少 contracts.yaml 参数」）；其它 → bail「本期横截面仅支持 binance/SHFE」。
  - `risk.max_drawdown` 有值 → `tracing::warn!("L4 NAV 回撤层推迟，max_drawdown 忽略")`。
  - 装配期 `tracing::info!(summary = %resolved_rule_summary(&config))`。
- `make_xsec_strategy`：
  - `spec.instruments.len() == 1` 且策略名在旧 `strategy_strategies::registry::all()`：`registry::create` → `SingleInstrumentAdapter::new`。
  - 否则（N>=2 或旧 registry 无此名）：`strategy_xsec_strategies::create`。
  - 两套都没有 → 报错并列出两套可用名。

- [ ] **Step 1: 写失败测试**

`xsec_config_guards.rs`：
```rust
#[test] fn parses_strategy_yaml_file_and_resolves_two_instruments() {
    // 写临时 strategy.yaml（BTC/ETH、exchange 缺省 binance）：
    // spec.instruments.len() == 2；venue_key ==
    // "binance/crypto_perpetual/BTCUSDT" / ".../ETHUSDT"；strategy_id ==
    // "xsec_momentum:default"；timeframe == "15m"。
}
#[test] fn parses_strategy_directory() {
    // 目录含 strategy.yaml + _global.yaml：load_xsec(dir) 成功且 services 来自 _global。
}
#[test] fn mixed_exchange_is_rejected_and_names_entries() {
    // universe [{BTCUSDT, binance}, {ETHUSDT, okx}] -> Err 含 "BTCUSDT" 与 "ETHUSDT"。
}
#[test] fn duplicate_symbol_is_rejected() {
    // universe 两条 BTCUSDT -> Err 含 namespaced_key 与 "重复"。
}
#[test] fn multi_instance_mode_is_rejected() {
    // mode: multi_instance -> Err 含 "multi_instance"。
}
#[test] fn directory_without_strategy_yaml_is_rejected() {
    // 空目录/只有 _global.yaml -> Err 点名目录并要求 strategy.yaml。
}
#[test] fn non_whitelisted_exchange_is_rejected() {
    // universe exchange: kraken -> Err 含 "binance/SHFE"。
}
#[test] fn n1_legacy_strategy_can_be_wrapped_by_adapter() {
    // 单标的 + strategy_type "example_dual_ma" -> make_xsec_strategy 返回
    // signal_id_namespace() == Legacy 的 Box。
}
#[test] fn n2_unknown_strategy_lists_both_registries() {
    // 双标的 + strategy_type "nope" -> Err 含 "xsec_momentum" 与 "example_dual_ma"。
}
#[test] fn old_dir_loader_ignores_strategy_yaml() {
    // config/{old}/BTCUSDT.yaml + config/{old}/xsec/strategy.yaml（新 schema）：
    // crate::config::load(Some(dir)) 成功，symbols 只有 BTCUSDT，不报 unknown field；
    // is_xsec_config_path(该 strategy.yaml) == true。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-app --test xsec_config_guards`
Expected: 编译失败。

- [ ] **Step 3: 实现**

- `crates/app/src/lib.rs` 加 `pub mod xsec_config;`。
- `_global.yaml` 读取：直接 `crate::config::load(Some(&global_path))`；不存在用 `FileConfig::default()`。
- `config.rs::load_dir` 在收集 `symbol_files` 时跳过 `file_stem == "strategy"` 的文件（横截面策略级单文件，不是 per-symbol 配置）；新增回归测试「目录同时含 BTCUSDT.yaml 与 xsec/strategy.yaml → 旧 loader 只产生 1 个 symbol，不报 unknown field」。
- 交易所白名单与解析规则见 Interfaces（依赖 Task 12 的 crate）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-app --test xsec_config_guards`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/app/src/xsec_config.rs crates/app/src/lib.rs crates/app/Cargo.toml crates/app/tests/xsec_config_guards.rs
git commit -m "feat(app): 横截面配置 loader——strategy.yaml、universe 解析、单一 exchange 与 multi_instance 拒启"
```

---

## Task 14: 回测装配 `run_xsec_from_spec` 与 CLI 分派（PR-2/PR-4）

**Files:**
- Modify: `crates/app/src/spi_backtest.rs`
- Modify: `crates/app/src/main.rs`
- Test: `crates/app/tests/xsec_backtest_e2e.rs`

**Interfaces:**
- Produces:
```rust
pub struct XsecRunArgs<'a> {
    pub spec: &'a crate::xsec_config::XsecSpec,
    pub strategy: Box<dyn strategy_engine::xsec::port::CrossSectionalStrategyPort>,
    pub warmup: &'a [Vec<Candle>],
    pub trading: &'a [Vec<Candle>],
    pub initial_cash: f64,
    pub fee_rate: Option<f64>,
    pub state_dir: &'a Path,
    pub data_dir: &'a Path,
}
pub struct XsecBacktestRun {
    pub summary: strategy_engine::xsec::runner::XsecRunSummary,
    pub trade_rows: Vec<crate::report::TradeRow>,
    pub daily: Vec<crate::report::EquityPoint>,
    pub tf_points: Vec<crate::report::EquityPoint>,
    pub initial_cash: f64,
}
pub fn run_xsec_from_spec(args: XsecRunArgs<'_>) -> anyhow::Result<XsecBacktestRun>;
pub fn fills_to_xsec_trade_rows(
    fills: &[strategy_engine::core::execution::Fill],
    strategy_name: &str,
    strategy_type: &str,
) -> Vec<crate::report::TradeRow>;
```
- 装配步骤：
  1. `n = instruments.len()`；每 slot 初始资金 `initial_cash / n as f64`。
  2. 每标的 `exchange_for_spec` → `SourceRequest::backtest(inst, vec![], slot_cash)` → `exchange.build` → `parts`。
  3. 从 `parts.session` 生成该标的 CSV 事件流（`events_from_bars(candles_to_bars(&inst, &trading[i]), session.as_ref())`），`parts.source = Box::new(VecEventSource::new(events))`（warmup 走 `group.preload` 直接喂 bars）。
  4. `SlotInit`：timeframes = `spec.config.timeframes` 解析（含 base）、`FeaturePlan::from_config`、`risk = spec.instruments[i].risk`、`sizing = SizingParams { leverage: spec.instruments[i].leverage as f64, notional_per_order: None }`、`strategy_type`。
  5. 先建 `Arc<Store>`（`state_dir`，时钟用回测首根 K 线时间）；`CrossSectionalRunnerGroup::new(..., Some(store.clone()))`；`preload(&warmup bars per slot)`；`sources()` → `AlignedEventSource::new`。
  6. 循环 `next_aligned()` → `on_event`；每次 `RoundReport` → `EquitySink::on_1m_close(report.at, report.nav, report.cash, base_tf_minutes)`。
  7. `finish()` 后再 `take_last_round()`，若有则补采最后一根净值；`fills_to_xsec_trade_rows` 用 `f.instrument.symbol`。
  8. `store.flush()`。
- `main.rs` `Backtest` 分支：
  - 若 `config_path` 为 `Some` 且 `is_xsec_config_path(p)`：调用新增 `run_xsec_backtest` helper（日期解析、每标的 CSV（命名空间路径 `venue_key`）、装配、`ReportGenerator::generate`（`symbol = spec.strategy_id`、`venue_key = ""`、`tf_key = base_tf`、`config_yaml = strategy.yaml 文本`）、打印摘要），返回 `Ok(())`。
  - 否则保持现有单标的路径逐字不变。
  - 未显式 `--config` 时默认路径仍走旧 loader（文档注明）。
  - 横截面分支若同时给出 `--strategy` / `--symbol` / `--cash`：`anyhow::bail!` 说明这些单标的覆盖项不适用于 `strategy.yaml`（避免静默忽略）。
  - `Run` 分支在 `config::load` **之前**检测 `is_xsec_config_path` → `anyhow::bail!("横截面策略实盘接入（PR-5）尚未实现；当前仅支持 backtest")`（否则旧 loader 会先因 unknown field 报出误导性错误）。
- `fills_to_xsec_trade_rows`：与 `fills_to_trade_rows` 同样的 side/comment 规则，`symbol = f.instrument.symbol`，`trade_id` 按 fill 顺序递增。

- [ ] **Step 1: 写失败测试**

`xsec_backtest_e2e.rs` 最小装配测试：
```rust
#[test]
fn xsec_run_assembles_two_slots_and_returns_portfolio_equity() {
    // 合成 2 标的 1m 数据写临时 CSV；测试用实现 CrossSectionalStrategyPort 返回 Hold；
    // run_xsec_from_spec -> bars_processed > 0、daily 非空、initial_cash 原样
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-app --test xsec_backtest_e2e`
Expected: 编译失败。

- [ ] **Step 3: 实现**

按 Interfaces。CSV 读取复用 `csv_1m_candles(data_dir, &spec.instruments[i].venue_key, from, to)`（warmup 起点按 `warmup_days` 前推）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-app --test xsec_backtest_e2e`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/app/src/spi_backtest.rs crates/app/src/main.rs crates/app/tests/xsec_backtest_e2e.rs
git commit -m "feat(app): 横截面回测装配 run_xsec_from_spec 与 backtest CLI 分派"
```

---

## Task 15: L2 标的级 `exit` 规则接入（PR-3）

**Files:**
- Modify: `crates/engine/src/xsec/runner.rs`
- Test: `crates/engine/src/xsec/runner.rs` 内联测试

**Interfaces:**
- `flush_round` 第 ③ 段：对每 slot（仅当该 slot 本轮有 base TF 闭合且 quorum 达标）：
  - `effective_rules(&config.exit, symbol)`；`exit_triggered(rules, &snapshot.features)`。
  - 触发 → `close_position(slot, ids, price=bar.close, reason=rule_reason, is_stop_loss=false, intent=ExitRule)`；置 `snapshot.exit_triggered = Some(reason)`；本轮该 slot 的策略动作被抑制。
  - 未触发 → `snapshot.exit_triggered = None`。
- 策略动作过滤：`exit_triggered` 标的的 `Open/Adjust` 丢弃并 `rejected_actions += 1`；`Close` 允许，但若 sub 已 `CloseRequested/Closed` 则跳过（1:1 约束）。
- 空 `exit` 规则集不触发。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn exit_rule_closes_only_its_instrument_and_suppresses_strategy_open() {
    // N=2：ETH 的 exit 规则（rsi>80）命中，BTC 未命中；策略对两者都返回 Open；
    // 断言：ETH 只有 1 笔 Close(intent "exit_rule")、无 Open；
    // BTC 有 1 笔 Open；summary.rejected_actions == 1（ETH 的 Open 被抑制）。
}
#[test]
fn empty_exit_rules_never_trigger() {
    // default=[]、无 override：策略 Open 不被抑制，snapshot.exit_triggered == None。
}
#[test]
fn exit_and_strategy_close_do_not_double_close() {
    // ETH exit 触发后策略也返回 Close：只发 1 笔 Close（sub.state 已 CloseRequested 时跳过）。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::runner exit_`
Expected: FAIL。

- [ ] **Step 3: 实现**

在 `flush_round` 中按顺序插入 L2；`close_position` 前检查该 instrument 的 sub 状态。`snapshot.exit_triggered` 先在 snapshot 构造时置 `None`，L2 后回填。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::runner`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/runner.rs
git commit -m "feat(engine): L2 标的级 exit 规则接入与同轮策略动作抑制"
```

---

## Task 16: L3 `portfolio_exit` 与 N=1 退化等价（PR-3）

**Files:**
- Modify: `crates/engine/src/xsec/model.rs`（如需辅助）
- Modify: `crates/engine/src/xsec/runner.rs`
- Test: `crates/engine/src/xsec/runner.rs` 内联测试

**Interfaces:**
- `flush_round` 第 ② 段（在 L2/策略之前）：
  - `let view = self.master_position();`
  - `portfolio_pnl_pct = if view.gross_notional > 1e-12 { view.unrealized_pnl / view.gross_notional * 100.0 } else { 0.0 }`
  - `trailing`：`pnl_pct >= arm_pnl_pct` → `portfolio.trail_armed = true`；`peak_unrealized_pnl = max(peak, view.unrealized_pnl)`；armed 且 `view.unrealized_pnl <= peak * (1.0 - giveback_pct/100.0)` → CloseAll。
  - `stop_loss_pct`：`portfolio_pnl_pct <= -stop_loss_pct` → CloseAll。
  - 触发 → `phase = Liquidating`；对所有非 Closed 子仓位 `close_position(..., intent=RiskCloseAll)`；**抑制本轮 L2 与策略动作**。
  - `Liquidating` 期间不调用策略；全部子仓位 `Closed` → `phase = Paused`。
  - `portfolio.peak_unrealized_pnl` 每轮在检查前更新。
- `master_position` 用 `slot.last_mark`（无则 `sub.entry_price` 兜底，不产生 NaN）。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn n1_portfolio_exit_matches_legacy_trailing_exactly() {
    // base TF=1m（决策与旧 runner 同为每根 1m）；bar 的 high=low=close；entry=100；
    // 价格 100->120 (+20%) -> 112 (=+12%)：
    // 旧 RiskController trailing activation=10%, drawdown=40% 在 112 触发；
    // 新 portfolio_exit arm=10%, giveback=40% 也在同一根触发；
    // 断言触发轮次一致且只有一次 CloseAll。
}

#[test]
fn portfolio_stop_loss_triggers_close_all() {
    // entry=100、qty=1、mult=1；价格 91（-9%）且 stop_loss_pct=8：
    // 触发 1 笔 Close(intent "risk_close_all")；phase == Liquidating。
}
#[test]
fn l3_precedes_l2_and_strategy_and_suppresses_both() {
    // 同一轮 L3（价格 -9%）与 ETH 的 exit 规则都命中，策略还返回 Open：
    // 断言只有 L3 的 CloseAll（intent risk_close_all）；无 exit_rule、无 Open。
}
#[test]
fn liquidating_blocks_new_opens_until_flat() {
    // L3 触发后下一轮策略仍返回 Open -> 0 笔新 Open；
    // 全平成交后 phase == Paused，之后策略调用次数不再增加。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::runner portfolio_`
Expected: FAIL。

- [ ] **Step 3: 实现**

按 Interfaces；`gross_notional` 用成本名义（Task 2 已定义），确保 N=1 百分比与旧 `pnl_pct` 同分母 `entry_price`。触发条件用金额：`unrealized <= peak_unrealized * (1 - giveback/100)`。`RiskController` 对照测试直接调用旧 `check_exit`，喂同一 `pnl_pct`/`max_pnl_pct`，断言触发轮次相同。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::runner`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/runner.rs crates/engine/src/xsec/model.rs
git commit -m "feat(engine): L3 portfolio_exit（组合浮盈回落/止损全平）与 N=1 旧 trail 等价"
```

---

## Task 17: 组合杠杆上限与按 NAV sizing（PR-3）

**Files:**
- Modify: `crates/engine/src/xsec/runner.rs`（`Sub` dispatch 路径）
- Modify: `crates/engine/src/xsec/dispatch.rs`（可选 qty 裁剪 helper）
- Test: `crates/engine/src/xsec/runner.rs` 内联测试

**Interfaces:**
- `gross_exposure(slots) -> f64`：Σ |sub.qty * mark_price * multiplier|（mark 缺失用 entry）。
- `max_total_notional`：`gross_exposure + new_notional <= nav * risk.max_total_notional`，否则按剩余容量等比裁剪；裁剪后 `qty <= 1e-12` → 跳过并 `rejected += 1`。
- `per_position_pct`：`target_notional = nav * sizing.per_position_pct`；`qty = target_notional / (price * multiplier)`。
- `Adjust` 的 target 同样受上限约束。
- Legacy 路径不受影响（仍用 `ActionDispatcher` 的 `SizingParams`）。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn open_qty_uses_nav_per_position_pct() {
    // nav=1000、per_position_pct=0.1、price=100、mult=1 -> OrderRequest.qty ≈ 1.0
}
#[test]
fn leverage_cap_scales_or_rejects_open() {
    // max_total_notional=0.2、已有 exposure=0.15 nav（150）、新单目标 0.1 nav（100）：
    // 剩 50 -> qty 从 1.0 裁到 0.5；
    // 已有 exposure=0.2 nav -> 剩 0 -> rejected=1、无 OrderRequest。
}
#[test]
fn adjust_respects_cap() {
    // target_qty 使 exposure 超上限 -> 实际下单 qty 被裁到剩余容量；
    // 剩余为 0 -> Adjust 被拒并计 rejected_actions。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::runner leverage_`
Expected: FAIL。

- [ ] **Step 3: 实现**

在 `flush_round` 的 Sub dispatch 前计算；`Open` qty 与 `Adjust` target 都过 `apply_notional_cap`。记录 `rejected_actions`。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::runner`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/runner.rs crates/engine/src/xsec/dispatch.rs
git commit -m "feat(engine): 组合杠杆上限与按 NAV sizing（per_position_pct）"
```

---

## Task 18: 两阶段再平衡（PR-4）

**Files:**
- Modify: `crates/engine/src/xsec/runner.rs`
- Test: `crates/engine/src/xsec/runner.rs` 内联测试

**Interfaces:**
- 新增轮次状态：
```rust
struct PendingRebalance {
    /// Phase 1 已派发、等待成交确认的减仓腿数
    reductions_in_flight: usize,
    /// Phase 1 已派发的 signal_id（含 `_leg{i}`），用于匹配 Fill 递减 in-flight
    reduction_ids: std::collections::BTreeSet<String>,
    /// Phase 2 待派发的加仓动作（按 instrument namespaced_key 排序）
    pending_adds: Vec<(InstrumentAction, bool /* action 时刻 entry_eligible */)>,
}
```
- 语义（设计 §9.2）：
  - 当 `PortfolioContext.rebalance_due == true` 且策略对本轮返回动作时，按 instrument 拆成两类：
    - **减仓**：`Close`、`Adjust { target_qty < 当前 qty }`；按 `namespaced_key()` 字典序立即派发（Phase 1）。
    - **加仓**：`Open`、`Adjust { target_qty > 当前 qty }`；先过 entry 硬门与 L2 抑制（`entry_eligible == false` / `exit_triggered` 触发的直接丢弃并计 `rejected_actions`），其余放入 `pending_adds`，**本轮不派发**。
  - `reductions_in_flight = Phase 1 实际提交的**腿数**（`DispatchOutcome.actions`）`；每笔匹配到该轮减仓 `signal_id`（含 `_leg{i}`）的 Fill 使计数 -1；被拒的腿也从计数扣除。后续每轮 `sync fills/books` 后：
    - 若 `reductions_in_flight == 0` 且 `pending_adds` 非空 → 用 `StrategyIds { rebalance_epoch: portfolio.rebalance_epoch + 1, .. }` 按字典序派发 Phase 2；派发后 `rebalance_epoch += 1`、`last_rebalance_at = Some(at)`、重置 `bars_since_rebalance`、清空 `pending_adds`；
    - 减仓订单被拒（`rejected`）→ 从 `reductions_in_flight` 扣除，允许 Phase 2 在下一轮继续（不阻塞整个组合）；超时/未成交在回测中由 Fill 驱动，live 对账属 PR-5。
  - 非 rebalance 轮（`rebalance_due == false`）不进入两阶段：策略若仍返回动作，按普通单轮 dispatch（L2/L3 除外）。
  - 两阶段期间（`reductions_in_flight > 0` 或 `pending_adds` 非空）`PortfolioContext.rebalance_due` 强制为 `false`，策略不再被触发新一轮轮换；若 L3 触发 → 清空 `pending_adds` 并进入 Liquidating。
- 顺序保证：Phase 1 全部按 `namespaced_key()` 字典序，Phase 2 同序；同一轮不混发减仓与加仓，避免「先加后减」导致资金竞争。

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn rebalance_reduces_before_adds_in_separate_rounds() {
    // 初始持有 A=2、B=0；策略在 rebalance_due 轮返回
    // A Adjust(0)、B Open：
    // 第一轮只有 A 的 Close/Adjust-down 订单；B 无订单；
    // 下一轮 fills 确认后 B 才下单；rebalance_epoch 在 Phase 2 后 +1。
}

#[test]
fn phase_two_waits_until_all_reductions_filled() {
    // 两个减仓单只成交 1 个 -> pending_adds 不派发；第二个成交后才派发
}

#[test]
fn rejected_reduction_does_not_deadlock_phase_two() {
    // 减仓被拒 -> reductions_in_flight 归零 -> 下一轮 Phase 2 继续
}

#[test]
fn l3_trigger_during_pending_rebalance_drops_adds() {
    // pending_adds 非空时 L3 触发 -> pending_adds 清空，只有 CloseAll
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-engine xsec::runner rebalance_`
Expected: FAIL（当前实现单轮混发）。

- [ ] **Step 3: 实现**

在 `flush_round` 的 Sub dispatch 段引入 `PendingRebalance`：
- 判定 `rebalance_due && !pending_adds.is_empty()` 等分支；
- Phase 1 只派发减仓并按 `DispatchOutcome.actions - rejected` 计数；
- Phase 2 派发前用 `pending_adds` 里记录的 action 时刻 `entry_eligible` 复核（false 直接丢弃），并检查杠杆上限（Task 17）；
- 每次 Phase 2 完成更新 `rebalance_epoch`/`last_rebalance_at`/`bars_since_rebalance`（Task 10 的单轮行为改为两阶段行为）。
- `portfolio.target_holdings` 在 Phase 1/2 派发时按 `target_qty * price / nav` 写入/更新，按 `namespaced_key` 排序保存。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-engine xsec::runner`
Expected: PASS（含原有单轮轮换测试——无减仓时 Phase 1 为空，Phase 2 当轮直接完成）。

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/xsec/runner.rs
git commit -m "feat(engine): 两阶段再平衡——先减仓确认、后加仓，字典序派发"
```

---

## Task 19: 双标的回测 e2e、确定性与黄金值（PR-4）

**Files:**
- Modify: `crates/app/tests/xsec_backtest_e2e.rs`
- Create: `config/xsec_momentum/strategy.yaml`
- Test: 同上

**Fixtures:** 使用 `xsec_momentum`；临时 CSV 用 `strategy_data::csv_store::save_csv(csv_store::csv_path(data_dir, venue_key, Timeframe::M1), &candles)` 写入命名空间布局。合成 2 标的（BTCUSDT、ETHUSDT）1m 序列，至少覆盖 1h macd warmup + 2 次 rebalance：
- 12000 根 1m（约 8.3 天）；15m base = 800 根；1h = 200 根；`every_bars: 96`（24h）。
- 价格路径让 BTC 更强、ETH 更弱，确保排名稳定。

**测试：**
```rust
#[test] fn xsec_momentum_backtest_runs_two_instruments_and_produces_trades() {
    // summary.bars_processed == 2 * bars；fills 非空；
    // trade_rows 中同时出现 BTCUSDT 与 ETHUSDT；final_portfolio.nav > 0。
}
#[test] fn same_input_twice_is_byte_identical() {
    // 两次不同 out dir；trade_rows 逐字段一致、daily/tf equity 逐字段一致、
    // 两次运行的 signal_id 集合完全相同。
}
#[test] fn sub_signal_golden_for_exit_rule_leg() {
    // L2 exit 触发的 Close 单 signal_id == "sub_<golden>_leg0"；
    // golden 由 generate_sub_signal_id(strategy_type, strategy_id, venue_key,
    // aligned_at, 平仓单 side（平多 = Side::Short）, SubIntent::ExitRule, epoch)
    // 计算并钉死；断言 base 与 _leg0 拼接规则。
}
#[test] fn warmup_insufficient_blocks_opens() {
    // 只给 10 根 1h（< macd warmup）-> 全部 entry_eligible=false；
    // 0 笔 Open、0 笔 Adjust-up；只有 fills 同步。
}
#[test] fn entry_exit_scenario_end_to_end() {
    // adx 从 19 升到 25：前 N 轮无 Open；超过 20 后出现 Open；
    // 随后 ETH 的 exit 规则命中 -> 出现 intent "exit_rule" 的 Close，
    // 且该轮 ETH 无策略 Open（抑制生效）。
}
```

- [ ] **Step 1: 写失败测试**

按测试名与断言写入 `crates/app/tests/xsec_backtest_e2e.rs`。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p strategy-app --test xsec_backtest_e2e`
Expected: FAIL（功能未完成或断言不满足）。

- [ ] **Step 3: 修实现直到测试通过（不改测试断言）**

- golden 值用 `generate_sub_signal_id` 在测试中算出期望后与 fill 对比，再把字符串一次写死（先打印确认）。
- `config/xsec_momentum/strategy.yaml` 按设计文档 §5.1 完整写出，universe BTC/ETH；额外加 `params: { top_k: 3 }`（示例策略私有参数，与 `rebalance.top_k` 保持一致）。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p strategy-app --test xsec_backtest_e2e`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add crates/app/tests/xsec_backtest_e2e.rs config/xsec_momentum/strategy.yaml
git commit -m "test(app): 横截面双标的回测 e2e、确定性、sub_ 黄金值与 warmup/exit 场景"
```

---

## Task 20: 旧链路回归与文档登记（PR-4 收口）

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `README.md`

- [ ] **Step 1: 跑旧链路全量回归**

Run: `cargo test --workspace`
Expected: 全部 PASS，尤其 `strategy-core` 既有 `sig_` 黄金值、`backtest_e2e` / `backtest_paper_parity` / `spi_dual_run_parity` / `cross_exchange` / `config_registry` / `live_bridge`。

- [ ] **Step 2: 跑 clippy**

Run: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
Expected: 0 warning。

- [ ] **Step 3: 写 CHANGELOG / README**

CHANGELOG `[Unreleased]` 增加 `Added — 横截面策略支持（PR-1～PR-4）`：
- 新模型/新 trait/新配置/示例策略/CLI 用法；
- 明确「实盘 PR-5 未实现，`run` 遇到横截面配置拒启」；
- 明确 L4 `max_drawdown` 解析但忽略；
- 明确 `multi_instance` 拒启。

README 增加最小用法：
```bash
cargo run -- backtest --config config/xsec_momentum/strategy.yaml \
  --start 20260101 --end 20260601 --data-dir ./data/klines
```

- [ ] **Step 4: 提交**

```bash
git add CHANGELOG.md README.md
git commit -m "docs: 横截面策略 PR-1～PR-4 用法与边界登记"
```

---

## Task 21: 最终验收（证据包）

- [ ] **Step 1: 格式检查**

Run: `cargo fmt --all -- --check`
Expected: 无 diff。

- [ ] **Step 2: 全量测试**

Run: `cargo test --workspace`
Expected: 全部 PASS；记录测试总数与耗时。

- [ ] **Step 3: 横截面回测实跑**

Run:
```bash
cargo run -- backtest --config config/xsec_momentum/strategy.yaml \
  --start 20260101 --end 20260601 --data-dir ./data/klines
```
Expected: 生成 `backtest_output/xsec_momentum/<day>/<time>/xsec_momentum_default/`，`result.json` + `backtest_trades.csv` + equity 曲线存在；stdout 打出 bars/fills/nav。

- [ ] **Step 4: 旧单标回测对照**

Run: `cargo test -p strategy-app --test backtest_e2e --test spi_dual_run_parity`
Expected: PASS；证明旧链路未被改动。

- [ ] **Step 5: 记录证据并提交（若有剩余改动）**

```bash
git status --short
git add -A
git commit -m "chore: 横截面策略 PR-1～PR-4 最终验收"
```

---

## 明确不做（本计划边界）

- **PR-5 实盘接入**：订阅合并 fan-out、live `remote_positions` 对账、重启 recover（含 `peak_unrealized_pnl`）——另立计划并在 `spi_live_actor`/`bootstrap` 层扩展；本期 `run` 遇到横截面配置直接报错。
- `multi_instance` 复制式（schema 字段保留，loader 拒启）。
- L4 `PortfolioRiskGuard`（NAV 回撤）、`risk.max_drawdown` 生效。
- 同标的并发多子仓位、跨策略资金分配、未闭合 bar 参与计算、跨标的原子事务、单策略跨多交易所。
- `scripts/migrate_config.py` 一次性 codemod（本期用「旧 loader 不改 + 新 loader 并存」满足双读；存量迁移脚本不在本期验收）。

## 执行顺序提示

1. Task 1～4（PR-1 纯加法）先落地并跑绿；
2. Task 12（示例策略 crate）先于 Task 13（loader 需要 `strategy-xsec-strategies`）；
3. Task 5～11（PR-2 运行时）→ Task 13～14（配置/装配）→ Task 15～17（PR-3 风控）→ Task 18（两阶段再平衡）→ Task 19（PR-4 e2e）；
4. Task 20～21 收口。每个 Task 独立 commit；每完成 3 个 Task 跑一次 `cargo test --workspace` 防止漂移。
