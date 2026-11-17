# 统一策略接口迁移实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 所有策略统一实现 `CrossSectionalStrategyPort`：一个策略一个顶层文件、一个注册表、一套元数据；迁移完成后删除旧 `Strategy` / `Ctx` / `ClosedSource` / `Action` / `SingleInstrumentAdapter` / `LegacyStrategyBridge` 与 `SignalIdNamespace::Legacy` 路径。

**Architecture:** `CrossSectionalStrategyPort` 是唯一策略基类（Rust trait + 默认 `timeframes()`）。SPI 回测/实盘保留 `ExchangeRunner`，新增 `PortStrategyBridge` 把单标的 `DecisionContext` 包成 1 条 `InstrumentSnapshot` + `PortfolioContext` 再调 port。`build.rs` 继续扫描 `src/*.rs` 自动生成模块与工厂；`registry::create` 统一返回 `Box<dyn CrossSectionalStrategyPort>`。xsec runner 删除 Legacy 分派，只保留 Sub 语义。

**Tech Stack:** Rust workspace（`strategy-core` / `strategy-engine` / `strategy-strategies` / `strategy-app`）、serde_json、cargo test/clippy/fmt。

**Spec:** `docs/strategy/XSEC_GUIDE.md` 的现行双接口契约（§1/§2/§4/§5）与用户 2026-11-11 需求“所有策略走一个基类、统一后旧代码不需要存在”；本计划 §Architecture 冻结迁移后的唯一接口。

## Global Constraints

- 中文注释、中文回复、中文 commit；commit 格式「类型: 简述」。
- 不主动 push；禁止 `--no-verify`；提交前必须跑测试。
- 禁止 `git add -A`，按文件显式 `git add` / `git rm`。
- 不触碰、不提交 `docs/design-package/01-需求分析.md` 与 `docs/design-package-old/`。
- 测试/演练只用 mock，绝不真实下单；mock live 演练必须在隔离 cwd + 不可达代理下运行。
- 不提交 `.env`/密钥；不动 `node_modules` / `dist` / `.git`。
- 注释只写原因；禁 emoji、空 catch、占位代码。
- 每个任务结束运行该任务列出的命令；Task 6 必须跑 `cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`。
- 破坏性命令（`rm -rf`、force push、`reset --hard`）先列影响再等确认；本计划删除文件一律用 `git rm`。

## Review Focus

1. `PortStrategyBridge` 快照字段（`trading_day/no_trade/entry_confirm/signal_resume/cooldown/session_open_price/closed`）若漏注入或口径错，单标策略在 SPI 回测/实盘会改变决策——Task 1 单测 + Task 2 双跑对齐测试钉住。
2. `SingleInstrumentAdapter` 过渡期若只返回默认 `timeframes() == [M1]`，双均线/多周期策略在 SPI 路径收不到 H1/D1——Task 1/2 增加转发测试。
3. `InstrumentAction::Adjust` 在 SPI 单标桥无法表达，若被误映射成 Open/Close 会真实改仓——Task 1 明确映射为 `StrategyAction::Hold` 并测试。
4. 统一 registry 的条目名与策略 `strategy_type()` 不一致时，signal_id、报表行键会漂移——Task 3 遍历 `all()` 逐项断言。
5. xsec N=1 旧配置从 Legacy 切到 Sub 后，`sig_` 变 `sub_` 且不再走 `ActionDispatcher` 风控口径——Task 3 修改 guard 测试并在 Task 6 文档写明行为变化。

---

## File Structure

**新增**
- `crates/engine/src/bridge.rs` 内新增 `PortStrategyBridge`（不新建文件）。
- `crates/strategies/src/xsec_momentum.rs`（由 `src/xsec/momentum.rs` 迁入）。
- `crates/strategies/src/xsec_template.rs`（由 `src/xsec/template.rs` 迁入）。

**修改**
- `crates/engine/src/xsec/port.rs`：trait 元数据、快照冷却辅助。
- `crates/engine/src/xsec/adapter.rs`：过渡期元数据转发，Task 5 删除。
- `crates/engine/src/xsec/runner.rs`：Task 5 删除 Legacy 分派。
- `crates/engine/src/lib.rs`：导出调整。
- `crates/strategies/src/registry.rs`、`lib.rs`、`build.rs`。
- `crates/strategies/src/example_dual_ma.rs`、`example_mtf_trend.rs`。
- `crates/app/src/main.rs`、`spi_backtest.rs`、`spi_live_actor.rs`、`runtime_types.rs`、`bootstrap.rs`、`xsec_config.rs`。
- `crates/app/tests/{backtest_e2e,backtest_paper_parity,spi_dual_run_parity,spi_engine_cli_parity,spi_live_merge,spi_live_orchestrator,xsec_config_guards,spi_row_key_namespace}.rs`。
- `crates/strategies/tests/xsec_registry.rs`（重命名为 `tests/registry.rs`）。
- `crates/core/src/{strategy.rs,signal.rs,lib.rs}`（Task 5 删除旧类型）。
- `docs/strategy/XSEC_GUIDE.md`、根 `README.md`、`crates/strategies/README.md`（若存在）、`docs/RUNBOOK.md`（仅改仍引用旧接口的段落）。

**删除**
- `crates/strategies/src/xsec/mod.rs` 与 `src/xsec/` 目录（Task 3）。
- `crates/engine/src/xsec/adapter.rs`（Task 5）。
- `crates/core` 内 `Strategy` / `Ctx` / `ClosedSource` / `Action` / `IntentCtx` / `intents_from_action`（Task 5）。

---

## Task 1: 新增 PortStrategyBridge 与 port 元数据

**Files:**
- Modify: `crates/engine/src/xsec/port.rs`
- Modify: `crates/engine/src/bridge.rs`
- Modify: `crates/engine/src/xsec/adapter.rs`
- Modify: `crates/engine/src/lib.rs`
- Modify: `crates/engine/src/xsec/runner.rs`（仅测试 mock）
- Modify: `crates/strategies/src/xsec/momentum.rs`、`xsec/template.rs`（仅补 `strategy_type`）
- Modify: `crates/app/tests/xsec_live_mock_e2e.rs`（仅补 `strategy_type`）
- Test: 上述文件的 `#[cfg(test)]` 模块

**Interfaces:**
- Produces:
  ```rust
  // xsec/port.rs
  pub trait CrossSectionalStrategyPort: Send {
      fn strategy_type(&self) -> &str;                    // 必填：进入报表/runner 命名
      fn timeframes(&self) -> Vec<Timeframe> { vec![Timeframe::M1] } // engine 类型
      fn on_cross_section_bars(...) -> Vec<InstrumentAction>;
      fn signal_id_namespace(&self) -> SignalIdNamespace { SignalIdNamespace::Sub }
  }
  impl InstrumentSnapshot {
      pub fn in_stop_loss_cooldown(&self) -> bool; // bar.open_time + 1m 的日历日 == cooldown 日
  }
  // bridge.rs
  impl CandleSeries {
      pub fn from_closed(tf: CoreTimeframe, candles: Vec<Candle>) -> Self;
      pub fn insert_closed(&mut self, tf: CoreTimeframe, candles: Vec<Candle>);
      pub fn closed(&self, tf: CoreTimeframe) -> Option<&[Candle]>;
      pub fn last_price(&self) -> Option<f64>;
  }
  pub struct PortStrategyBridge { /* strategy + CandleSeries */ }
  impl PortStrategyBridge {
      pub fn new(strategy: Box<dyn CrossSectionalStrategyPort>) -> Self;
      pub fn timeframes(&self) -> Vec<Timeframe>;
      pub fn strategy_type(&self) -> &str;
      pub fn closed_source(&self) -> &CandleSeries;
  }
  impl StrategyPort for PortStrategyBridge { /* on_closed_bars */ }
  ```

- [ ] **Step 1: 写失败测试**

在 `crates/engine/src/bridge.rs` 的 `mod tests` 增加：

```rust
#[test]
fn port_bridge_maps_flat_context_to_one_snapshot_and_open_action() {
    // RecordingPort 记录收到的 snapshots，返回 Open{instrument=snapshots[0].instrument,...}
    // 断言：
    // - bridge.strategy_type() == "recording"
    // - bridge.timeframes() == vec![Timeframe::M1, Timeframe::H1]
    // - 返回 [StrategyAction::Open { side: Long, price: 100.0, .. }]
    // - 记录的 snapshot：position=None、stale=false、entry_eligible=true、
    //   features 为空、closed.is_some()、trading_day/no_trade/entry_confirm/
    //   signal_resume/cooldown/session_open_price 与 DecisionContext 一致
}

#[test]
fn port_bridge_maps_adjust_to_hold() {
    // 策略返回 [InstrumentAction::Adjust { target_qty: 5.0, .. }]
    // 断言输出 == vec![StrategyAction::Hold]
}
```

在 `crates/engine/src/xsec/port.rs` 的测试中增加：

```rust
#[test]
fn snapshot_cooldown_uses_bar_open_plus_one_minute() {
    // cooldown=UntilDate("2026-01-06")，bar.open_time=2026-01-05T23:59:00Z → true
    // bar.open_time=2026-01-05T23:58:00Z → false
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p strategy-engine bridge`
Expected: 编译失败，提示 `PortStrategyBridge` / `from_closed` / `in_stop_loss_cooldown` 不存在。

- [ ] **Step 3: 实现 port 元数据与快照辅助**

- `CrossSectionalStrategyPort` 增加必填 `strategy_type(&self) -> &str` 与默认 `timeframes()`。
- 为现有 9 个实现补 `strategy_type`：`SingleInstrumentAdapter` 委托内层 `Strategy`；`XsecMomentum` 返回 `"xsec_momentum"`；`XsecTemplate` 返回 `"xsec_template"`；runner 测试 mock 与 `xsec_live_mock_e2e` mock 返回各自固定名。
- `SingleInstrumentAdapter` 同时 override `timeframes()`：把内层 `CoreTimeframe` 列表转成 engine `Timeframe`。
- 在 `adapter.rs` 测试模块新增 `adapter_forwards_metadata`：内层实现 `strategy_type="inner"`、`timeframes=[M1,H1]`，断言 Adapter 两个返回值一致。
- `InstrumentSnapshot::in_stop_loss_cooldown` 逻辑照搬 `Ctx::in_stop_loss_cooldown`，时间基准用 `self.bar.open_time`。

- [ ] **Step 4: 实现 CandleSeries 公共读接口**

`from_closed` 设置 `last_price = candles.last().map(|c| c.close)`；`insert_closed` 只写 map，若该 tf 是 M1 则同步 `last_price`；`closed` / `last_price` 为只读访问。保留 `ClosedSource` 实现到 Task 5。

- [ ] **Step 5: 实现 PortStrategyBridge**

`on_closed_bars`：
1. `self.source.refresh(state)`；
2. 构造 1 条 `InstrumentSnapshot`：`instrument/bar/price` 取自 `DecisionContext`，`position=ctx.position`，`stale=false`，`features=FeatureMap::default()`，`entry_eligible=true`，`trading_day/no_trade/entry_confirm/signal_resume/cooldown/session_open_price` 从 ctx 注入，`closed=Some(self.source.clone())`；
3. `PortfolioContext { strategy_id: self.strategy_type().into(), nav: 0.0, drawdown: 0.0, portfolio_pnl_pct: 0.0, sub_positions: vec![], multipliers: BTreeMap::new(), rebalance_due: false, no_trade: ctx.no_trade }`；
4. `instrument_action` 映射：Open/Close/Reverse/UpdateStop/UpdateExit 同字段去 `instrument`；`Adjust` → `StrategyAction::Hold`；`Hold` → `StrategyAction::Hold`。
5. 每次调用都调 port 一次，是否决策交给策略（与 xsec 语义一致）。

- [ ] **Step 6: 跑测试**

Run: `cargo test -p strategy-engine`
Expected: 全部通过；新增 3 个测试通过。

- [ ] **Step 7: 提交**

```bash
git add crates/engine/src/bridge.rs crates/engine/src/xsec/port.rs crates/engine/src/xsec/adapter.rs crates/engine/src/lib.rs crates/engine/src/xsec/runner.rs crates/strategies/src/xsec/momentum.rs crates/strategies/src/xsec/template.rs crates/app/tests/xsec_live_mock_e2e.rs
git commit -m "feat: 新增 port 策略桥与统一元数据"
```

---

## Task 2: SPI 回测/实盘切到 PortStrategyBridge（过渡期用 Adapter 包旧策略）

**Files:**
- Modify: `crates/app/src/spi_backtest.rs`
- Modify: `crates/app/src/spi_live_actor.rs`
- Modify: `crates/app/src/runtime_types.rs`
- Modify: `crates/app/src/main.rs`
- Modify: `crates/app/src/bootstrap.rs`
- Modify: `crates/app/tests/common/mod.rs`
- Modify: `crates/app/tests/{backtest_e2e,backtest_paper_parity,spi_dual_run_parity,spi_engine_cli_parity,spi_live_merge,spi_live_orchestrator}.rs`

**Interfaces:**
- Consumes: Task 1 的 `PortStrategyBridge`。
- Produces:
  ```rust
  // spi_backtest.rs
  pub struct SpiBacktest { pub bridge: PortStrategyBridge, ... }
  pub fn assemble(..., strategy: Box<dyn CrossSectionalStrategyPort>, ...)
  pub struct SpiRunArgs<'a> { pub strategy: Box<dyn CrossSectionalStrategyPort>, ... }
  // runtime_types.rs
  pub type RuntimeFactory = Box<dyn FnMut() -> (Box<dyn AsyncOrderGateway>, Box<dyn CrossSectionalStrategyPort>) + Send>;
  // spi_live_actor.rs
  pub struct SpiLiveAssembly { pub strategy: Box<dyn CrossSectionalStrategyPort>, ... }
  // tests/common/mod.rs
  pub fn legacy_port(s: Box<dyn Strategy + Send>) -> Box<dyn CrossSectionalStrategyPort>;
  ```

- [ ] **Step 1: 改 `spi_backtest.rs`**

- `SpiBacktest.bridge` 改 `PortStrategyBridge`；`assemble` 形参改 port；内部 `PortStrategyBridge::new(strategy)`。
- `SpiRunArgs.strategy` 改 port。
- `run_from_spec` 不变（透传）。
- 第 151 行 `closed_source().closed(...)` 改用 Task 1 的 inherent 方法，删除 `use ClosedSource as _`。

- [ ] **Step 2: 改 live 链路类型**

- `runtime_types.rs` 的 `RuntimeFactory` 返回 port。
- `SpiLiveAssembly.strategy`、`SpiLiveKernel` 内 bridge 类型改 `PortStrategyBridge`；`assemble` 中 `PortStrategyBridge::new(a.strategy)`。
- 更新模块注释里 `intents_from_action` / `LegacyStrategyBridge` 的旧描述。

- [ ] **Step 3: 生产工厂包 Adapter**

- `main.rs::make_strategy` 返回 `Box<dyn CrossSectionalStrategyPort>`，内容为 `SingleInstrumentAdapter::new(registry::create(...)?)`。
- `bootstrap.rs` 的 runtime factory 同样把 `registry::create` 结果包 Adapter 后返回。
- `xsec_config.rs` 已是 port 返回，不改。

- [ ] **Step 4: 测试公共过渡 helper**

在 `tests/common/mod.rs` 增加：

```rust
#[allow(dead_code)]
pub fn legacy_port(
    strategy: Box<dyn strategy_core::strategy::Strategy + Send>,
) -> Box<dyn strategy_engine::xsec::port::CrossSectionalStrategyPort> {
    Box::new(strategy_engine::xsec::adapter::SingleInstrumentAdapter::new(strategy))
}
```

- [ ] **Step 5: 更新测试装配点**

对 6 个测试文件执行同一机械替换：
- `SpiBacktest::assemble(..., Box::new(X), ...)` → `..., common::legacy_port(Box::new(X) as Box<dyn Strategy + Send>), ...`。
- `SpiRunArgs { strategy: Box::new(X) }` → `strategy: common::legacy_port(...)`。
- `RuntimeFactory` 闭包返回的 `(gw, Box::new(X) as Box<dyn Strategy + Send>)` → `(gw, common::legacy_port(...))`。
- 未引入 `mod common;` 的文件补一行；`spi_live_orchestrator.rs` 有约 15 处工厂点，全部照此替换。
- 保留各测试替身的 `strategy_type` 字符串（`"test"`、`"dual_ma_test"`、`"parity"`、`"once_then_exit"`、`"test_merge"`、`"openonce"` 等），Adapter 会转发它们。

- [ ] **Step 6: 跑受影响测试**

Run:
```bash
cargo test -p strategy-app --test backtest_e2e --test backtest_paper_parity --test spi_dual_run_parity --test spi_engine_cli_parity --test spi_live_merge --test spi_live_orchestrator
```
Expected: 全部通过，成交/行键/冷却断言与迁移前一致；若 live mock 测试要求隔离环境，按 `docs/RUNBOOK.md` §5 的隔离 cwd + 不可达代理口径执行。

- [ ] **Step 7: 提交**

```bash
git add crates/app/src/spi_backtest.rs crates/app/src/spi_live_actor.rs crates/app/src/runtime_types.rs crates/app/src/main.rs crates/app/src/bootstrap.rs crates/app/tests/common/mod.rs crates/app/tests/backtest_e2e.rs crates/app/tests/backtest_paper_parity.rs crates/app/tests/spi_dual_run_parity.rs crates/app/tests/spi_engine_cli_parity.rs crates/app/tests/spi_live_merge.rs crates/app/tests/spi_live_orchestrator.rs
git commit -m "refactor: SPI 运行时改用 port 策略桥"
```

---

## Task 3: 策略 crate 与注册表统一切换

**Files:**
- Modify: `crates/strategies/src/registry.rs`
- Modify: `crates/strategies/src/lib.rs`
- Modify: `crates/strategies/src/build.rs`
- Modify: `crates/strategies/src/example_dual_ma.rs`
- Modify: `crates/strategies/src/example_mtf_trend.rs`
- Rename: `crates/strategies/src/xsec/momentum.rs` → `crates/strategies/src/xsec_momentum.rs`
- Rename: `crates/strategies/src/xsec/template.rs` → `crates/strategies/src/xsec_template.rs`
- Rename: `crates/strategies/tests/xsec_registry.rs` → `crates/strategies/tests/registry.rs`
- Delete: `crates/strategies/src/xsec/mod.rs`
- Modify: `crates/app/src/main.rs`、`bootstrap.rs`、`xsec_config.rs`
- Modify: `crates/app/tests/xsec_config_guards.rs`

**Interfaces:**
- Consumes: Task 1 trait（`strategy_type` 必填、`timeframes` 默认）、Task 2 app port 面。
- Produces:
  ```rust
  // registry.rs
  pub struct StrategyEntry {
      pub name: &'static str,
      pub factory: fn(&FactoryArgs) -> anyhow::Result<Box<dyn CrossSectionalStrategyPort>>,
  }
  pub fn create(name: &str, args: &FactoryArgs) -> anyhow::Result<Box<dyn CrossSectionalStrategyPort>>;
  pub fn all() -> Vec<StrategyEntry>;
  ```
  `register_strategy! { name: "...", factory: |args| { ... Ok(Box::new(S)) } }` 形状不变，只换返回类型。

- [ ] **Step 1: 写失败测试**

`crates/strategies/tests/registry.rs`：
```rust
#[test]
fn all_entries_factory_matches_name_and_metadata() {
    for e in strategy_strategies::registry::all() {
        let s = strategy_strategies::registry::create(e.name, &args("1h")).unwrap();
        assert_eq!(s.strategy_type(), e.name, "注册名与策略元数据必须一致");
        assert!(s.timeframes().contains(&strategy_engine::core::bar::Timeframe::M1));
    }
    // names 包含 example_dual_ma / example_mtf_trend / xsec_momentum / xsec_template
}
```
`crates/app/tests/xsec_config_guards.rs` 把 `n1_legacy_strategy_can_be_wrapped_by_adapter` 改为：
```rust
#[test]
fn n1_old_strategy_uses_unified_registry() {
    // 同一配置加载
    // assert!(!spec.instruments[0].risk.trailing_profit.enabled, "统一走横截面 L1");
    let strategy = make_xsec_strategy(&spec).unwrap();
    assert_eq!(strategy.strategy_type(), "example_dual_ma");
}
```
Run: `cargo test -p strategy-strategies --test registry`
Expected: 编译失败（`registry::create` 仍返回旧 `Strategy`，示例尚无 port 实现）。

- [ ] **Step 2: 统一 registry**

- `StrategyEntry.factory` 与 `create` 返回 port。
- 测试 `create_example_dual_ma_ok` 改为断言 `strategy_type() == "example_dual_ma"`。
- `params_or_default` 不变。

- [ ] **Step 3: 两个示例改 port（各自单文件）**

`example_dual_ma.rs`：
- 删除 `impl Strategy`，新增 `impl CrossSectionalStrategyPort`：
  - `strategy_type()` 返回 `"example_dual_ma"`。
  - `timeframes()` 返回 `[EngineTf::M1, EngineTf::from(self.config.main_timeframe)]`。
  - `on_cross_section_bars`：遍历 `snapshots`，对每条 `InstrumentSnapshot` 调内部 `decide(s)`；`position.is_none()` 走入场，`Some` 走出场；`s.closed` 缺失返回空。
  - 入场/出场逻辑保持原判定：均线交叉事件、止损 `bar.close/high/low`、`s.in_stop_loss_cooldown()`、`reason` 文案不变。
- helper 参数从 `&Ctx` 改为 `(&CandleSeries, &Bar, Option<&PositionView>)`；输出 `InstrumentAction` 时补 `instrument: s.instrument.clone()`。
- 单测 7 个改为快照 fixture：用 `CandleSeries::from_closed` 构造 M1 + 主周期，断言 `Open/Close` 的 `instrument/price/stop_price/side/reason`。

`example_mtf_trend.rs`：同样迁移；`trend_allows_long/cross_signal/atr` 改读 `s.closed` 的 D1 / 主周期；`timeframes()` 返回 `[M1, main, D1]`。

- [ ] **Step 4: 横截面策略迁顶层并自注册**

```bash
git mv crates/strategies/src/xsec/momentum.rs crates/strategies/src/xsec_momentum.rs
git mv crates/strategies/src/xsec/template.rs crates/strategies/src/xsec_template.rs
git rm crates/strategies/src/xsec/mod.rs
```
- 两个文件各补：
  ```rust
  impl CrossSectionalStrategyPort for XsecMomentum {
      fn strategy_type(&self) -> &str { "xsec_momentum" }
      ...
  }
  crate::register_strategy! {
      name: "xsec_momentum",
      factory: |args| Ok(Box::new(XsecMomentum::from_params(args.params)?))
  }
  ```
  `xsec_template` 同样，factory 调 `XsecTemplate::from_params(args.params)`。
- `lib.rs` 删除 `pub mod xsec;`；`build.rs` 仅更新注释为“任意顶层 `.rs` 文件即一个策略”。
- 确认 `build.rs` 扫描到 4 个顶层策略文件。

- [ ] **Step 5: app 统一创建**

- `main.rs::make_strategy` 删除 Adapter 包装，直接 `registry::create`。
- `bootstrap.rs` 删除 Adapter 包装。
- `xsec_config.rs`：
  - 删除 `is_legacy_registered`、`legacy_n1`、`SingleInstrumentAdapter` import。
  - `make_xsec_strategy` 取 `spec.instruments.first()` 构造 `FactoryArgs`，直接 `registry::create(&spec.strategy_type, &args)`；错误由 registry 统一列出全部策略名。
  - 所有 xsec 实例的 `risk` 统一为 `l1_stop_only(&global)`（行为变化见 Spec 与 Task 6）。

- [ ] **Step 6: 跑策略与 app 守卫测试**

Run:
```bash
cargo test -p strategy-strategies
cargo test -p strategy-app --test xsec_config_guards
cargo test -p strategy-app --test xsec_backtest_e2e
```
Expected: 通过；`xsec_momentum` 名单与 N=1 单标路径行为不变。

- [ ] **Step 7: 提交**

```bash
git add crates/strategies/src/registry.rs crates/strategies/src/lib.rs crates/strategies/src/build.rs crates/strategies/src/example_dual_ma.rs crates/strategies/src/example_mtf_trend.rs crates/strategies/src/xsec_momentum.rs crates/strategies/src/xsec_template.rs crates/strategies/tests/registry.rs crates/app/src/main.rs crates/app/src/bootstrap.rs crates/app/src/xsec_config.rs crates/app/tests/xsec_config_guards.rs
git rm crates/strategies/tests/xsec_registry.rs
git commit -m "refactor: 统一策略接口与注册表"
```

---

## Task 4: app 测试替身改为原生 port

**Files:**
- Modify: `crates/app/tests/backtest_e2e.rs`
- Modify: `crates/app/tests/backtest_paper_parity.rs`
- Modify: `crates/app/tests/spi_dual_run_parity.rs`
- Modify: `crates/app/tests/spi_engine_cli_parity.rs`
- Modify: `crates/app/tests/spi_live_merge.rs`
- Modify: `crates/app/tests/spi_live_orchestrator.rs`
- Modify: `crates/app/tests/common/mod.rs`

**Interfaces:**
- Consumes: Task 3 统一 registry/port。
- Produces: 测试替身实现 `CrossSectionalStrategyPort`；装配点直接传 `Box<dyn CrossSectionalStrategyPort>`。

- [ ] **Step 1: 逐个替换测试替身**

对每个 `impl Strategy for X`：
- `strategy_type()` 保留原字符串（`"test"` / `"dual_ma_test"` / `"recorder"` 委托内层 / `"never_exit"` / `"parity"` / `"once_then_exit"` / `"test_merge"` / `"test_merge_openonce"` / `"openonce"` / `"noop"` / `"panic"` / `"cooldownspy"`）。
- `timeframes()` 保留原列表，返回 engine `Timeframe`（core 转 engine）。
- `on_candle` / `on_realtime_tick` 合并：
  ```rust
  fn on_cross_section_bars(&mut self, _newly, snapshots, _portfolio) -> Vec<InstrumentAction> {
      let Some(s) = snapshots.first() else { return vec![] };
      // 原 on_candle 逻辑（s.position.is_none()）与 on_realtime_tick 逻辑（Some）
  }
  ```
- `ctx.current_candle` → `s.bar`；`ctx.position` → `s.position`；`ctx.closed_data_min(tf, n)` → `s.closed.as_ref()?.closed(tf)` 且长度检查；`ctx.in_stop_loss_cooldown()` → `s.in_stop_loss_cooldown()`。
- `Action::Open/Close` → `InstrumentAction::{Open,Close}` 并补 `instrument: s.instrument.clone()`。

- [ ] **Step 2: 去掉过渡包装**

- 所有 `common::legacy_port(...)` 调用改为直接 `Box::new(X)` / 原构造。
- 删除 `tests/common/mod.rs` 的 `legacy_port`。
- 删除仅为该 helper 新增的 `mod common;`（保留原本需要的）。

- [ ] **Step 3: 跑 6 个测试文件**

Run:
```bash
cargo test -p strategy-app --test backtest_e2e --test backtest_paper_parity --test spi_dual_run_parity --test spi_engine_cli_parity --test spi_live_merge --test spi_live_orchestrator
```
Expected: 全部通过，成交/行键/冷却断言与 Task 2 一致。

- [ ] **Step 4: 提交**

```bash
git add crates/app/tests/backtest_e2e.rs crates/app/tests/backtest_paper_parity.rs crates/app/tests/spi_dual_run_parity.rs crates/app/tests/spi_engine_cli_parity.rs crates/app/tests/spi_live_merge.rs crates/app/tests/spi_live_orchestrator.rs crates/app/tests/common/mod.rs
git commit -m "test: 迁移 SPI 测试替身到 port 接口"
```

---

## Task 5: 删除 Legacy 分派与旧策略接口

**Files:**
- Modify: `crates/engine/src/xsec/port.rs`
- Modify: `crates/engine/src/xsec/adapter.rs`
- Modify: `crates/engine/src/xsec/runner.rs`
- Modify: `crates/engine/src/xsec/mod.rs`
- Modify: `crates/engine/src/bridge.rs`
- Modify: `crates/engine/src/lib.rs`
- Modify: `crates/core/src/strategy.rs`
- Modify: `crates/core/src/signal.rs`
- Modify: `crates/core/src/lib.rs`
- Modify: `crates/app/src/{spi_backtest.rs,spi_live_actor.rs}`
- Modify: `crates/app/tests/spi_row_key_namespace.rs`
- Delete: `crates/engine/src/xsec/adapter.rs`

**Interfaces:**
- Consumes: Task 4 全部策略/测试替身已原生实现 port。
- Produces: `CrossSectionalStrategyPort` 只保留 `strategy_type` / `timeframes` / `on_cross_section_bars`；xsec runner 只走 `dispatch_sub`。

- [ ] **Step 1: 删除 xsec runner 的 Legacy 分派**

- `xsec/port.rs` 删除 `SignalIdNamespace` 与 trait 的 `signal_id_namespace` 默认方法；更新文档。
- `xsec/adapter.rs` 删除 `signal_id_namespace` override。
- `runner.rs`：
  - 删除 `SignalIdNamespace` import、`XsecSlot.dispatcher` 字段、`ActionDispatcher` import 与初始化、`SlotInit.sizing` 字段及所有构造点。
  - 5 处 `match ns`（约 785/901/1274/1625 及 1009 门控）去掉 Legacy 臂，直接调 `dispatch_sub`。
  - 删除 `dispatch_legacy`、`legacy_ctx`。
  - 删除测试 mock 的 `signal_id_namespace` override；删除 `n1_legacy_adapter_matches_exchange_runner`（Adapter 已无 Legacy 路径）。
- Run: `cargo test -p strategy-engine`
Expected: 通过；xsec 测试全绿。

- [ ] **Step 2: 删除旧 core 策略类型**

- `core/src/strategy.rs` 删除 `Strategy`、`Ctx`、`ClosedSource` 及其测试；保留 `Cooldown`、`PositionView`。
- `core/src/signal.rs` 删除 `Action`、`IntentCtx`、`intents_from_action` 及相关测试；保留 `SignalIntent`、`generate_signal_id`。
- `core/src/lib.rs` 删除对应 re-export。
- Run: `cargo check -p strategy-core`
Expected: 通过。

- [ ] **Step 3: 删除旧桥与 Adapter**

- `engine/src/bridge.rs` 删除 `LegacyStrategyBridge`、`legacy_action`、`strategy_core::signal::Action` / `strategy::*` import、`CandleSeries` 的 `ClosedSource` impl；保留 `CandleSeries` inherent 方法与 `PortStrategyBridge`。
- `engine/src/xsec/mod.rs` 删除 `pub mod adapter;`；`git rm crates/engine/src/xsec/adapter.rs`。
- `engine/src/lib.rs` 删除 `LegacyStrategyBridge` 导出。
- `app/src/spi_backtest.rs`、`spi_live_actor.rs` 删除残留 `Strategy` import 与旧桥注释；`spi_row_key_namespace.rs` 改用直接构造：
  ```rust
  fn open_intent(strategy_type: &str, symbol: &str, venue_key: &str, ts: DateTime<Utc>) -> SignalIntent {
      SignalIntent {
          id: generate_signal_id(strategy_type, venue_key, ts, SignalType::Buy),
          strategy_id: strategy_type.into(),
          symbol: symbol.into(),
          kind: IntentKind::Open,
          direction: Side::Long,
          price: 100.0,
          size: 0.0,
          size_raw: None,
          stop_price: Some(50.0),
          position_id: None,
          stop_order_id: None,
          created_event_time: ts,
          status: SignalStatus::Created,
          offset: None,
      }
  }
  ```
  两个用例改用 `open_intent("once", "BTCUSDT", "binance/crypto_perpetual/BTCUSDT", ts)` 等直接构造，删除 `Action` / `IntentCtx` / `intents_from_action` import。
- Run:
```bash
cargo check --workspace --all-targets
cargo test --workspace
```
Expected: 编译通过、全绿；`grep -rn "LegacyStrategyBridge\|SingleInstrumentAdapter\|intents_from_action\|SignalIdNamespace" crates` 无生产代码命中。

- [ ] **Step 4: 提交**

```bash
git add crates/engine/src/xsec/port.rs crates/engine/src/xsec/adapter.rs crates/engine/src/xsec/runner.rs crates/engine/src/xsec/mod.rs crates/engine/src/bridge.rs crates/engine/src/lib.rs crates/core/src/strategy.rs crates/core/src/signal.rs crates/core/src/lib.rs crates/app/src/spi_backtest.rs crates/app/src/spi_live_actor.rs crates/app/tests/spi_row_key_namespace.rs
git rm crates/engine/src/xsec/adapter.rs
git commit -m "refactor: 删除旧 Strategy 接口与 Legacy 分派"
```

---

## Task 6: 文档与最终验证

**Files:**
- Modify: `docs/strategy/XSEC_GUIDE.md`
- Modify: `README.md`
- Modify: `crates/strategies/README.md`（若存在）
- Modify: `docs/RUNBOOK.md`（只改仍引用旧接口的段落）
- Modify: `crates/strategies/src/registry.rs`、`lib.rs` 文档注释

- [ ] **Step 1: 重写 XSEC_GUIDE 接口章节**

- §1 改为单接口：所有策略实现 `CrossSectionalStrategyPort`；N=1/N>1 同一份代码；加 universe 行即横截面。
- §2 改为单目录：`crates/strategies/src/*.rs` 每个文件一个策略，文件尾 `register_strategy!`，`build.rs` 自动发现；删除 `src/xsec/` 与手写 `all()/create()` 描述。
- 删除旧 `Strategy` / `SingleInstrumentAdapter` / `SignalIdNamespace::Legacy` 的表格与建议。
- 补充 SPI 路径说明：`ExchangeRunner` 经 `PortStrategyBridge` 以单条快照调用同一 port；`strategy_type()/timeframes()` 元数据用于 runner 命名与周期聚合。
- 写明行为变化：xsec N=1 旧策略名现在走 `sub_` 信号 ID 与横截面 L1 风控（trailing 关闭），不再有 Legacy 兼容分支。
- §8/§9/§10 的测试命令、注册步骤、边界同步更新；删除“旧 registry 重名抢占”表述。

- [ ] **Step 2: 更新 README 与代码注释**

- 根 `README.md` 的“新增策略”说明改为单接口/单文件/自动注册。
- `crates/strategies/src/lib.rs`、`registry.rs` 文档注释删除旧 `Strategy` 描述。
- `grep -rn "SingleInstrumentAdapter\|LegacyStrategyBridge\|SignalIdNamespace\|intents_from_action\|src/xsec" README.md docs crates/*/README.md` 应无过时引用。

- [ ] **Step 3: 最终验证**

Run:
```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
Expected: 三条全部通过；测试只报通过摘要。

- [ ] **Step 4: 提交**

```bash
git add docs/strategy/XSEC_GUIDE.md README.md crates/strategies/README.md crates/strategies/src/registry.rs crates/strategies/src/lib.rs docs/RUNBOOK.md
git commit -m "docs: 更新统一策略接口与开发指南"
```

---

## 完成标准

- `crates/strategies/src/*.rs` 每个顶层文件就是一个策略；`src/xsec/` 不存在。
- 全仓库只有一种策略 trait：`CrossSectionalStrategyPort`。
- `grep -rn "impl Strategy for\|LegacyStrategyBridge\|SingleInstrumentAdapter\|SignalIdNamespace\|intents_from_action\|strategy_core::strategy::Ctx" crates` 无命中（历史注释除外）。
- SPI 回测/实盘、xsec 回测/live mock 测试全部通过。
- 文档不再教用户写旧 `Strategy`。
