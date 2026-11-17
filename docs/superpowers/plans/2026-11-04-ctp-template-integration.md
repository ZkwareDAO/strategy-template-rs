# CTP 模板能力接入 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax of tracking.

**Goal:** 把 `strategy-template-rs` 从"仅 crypto 口径"升级为"CTP 期货回测 + CTP 聚合K线 + 仿真(paper)同口径"，同时 crypto 行为逐位不变。

**Architecture:** 三阶段：①从同根 CTP 仓库按路径白名单移植已完成的设施（时段聚合、合约撮合、报告）；②补齐两仓库都缺失的期货基础设施（交易日历、行情契约期货形态、paper/mock 合约化、parity 覆盖）；③数据工具（主连拼接）与模板配套（示例配置/文档/脚本）。回测与 paper 的手数/保证金/手续费共用 `core::contract` 同一纯函数，由 `backtest_paper_parity` 锁口径。

**Tech Stack:** Rust（rust-toolchain.toml 固定）、serde/serde_yaml、chrono、csv、tokio-tungstenite。无新增外部依赖。

**Spec:** 本计划自带背景与证据（见下）；缺口清单来自 2026-11-04 会话对 `/home/qpw/workspace/strategy-template-rs-ctp` 与本地 `c82a23e` 的 diff 分析。

## 背景与已定决策

- 参照仓库：`/home/qpw/workspace/strategy-template-rs-ctp`（**只读**，不得写入/改其 index），与本地共享历史，base=`c82a23e`，其上 13 commit + 未提交工作区改动。
- 差距证据：核心 diff——`data/ctp_session.rs`(215行)、`feed.rs`(+498, `bar_merge=ctp`)、`core/contract.rs`(462行)+`config/contracts.yaml`、`broker/backtest.rs`(+205, 整数手/保证金/平今费)、`app/config.rs`(+315)、`app/engine.rs`(+157, `Ctx` 注入 `newly_closed/entry_confirm/session_open_price`)。策略部分（7 个策略文件、对应 config、`data/src/bin/spread_prep.rs`）**不移植**。
- **决策 1（行情源）**：CTP 直连不进模板；扩展 kline-service 契约为期货形态，由上游行情服务负责 CTP 接入。理由：模板定位=发信号+回测；无 CTP 账号，直连无法验收；解析层差异仅是 symbol/窗口口径。
- **决策 2（手数口径）**：下单链路开仓帧只带 `order_cash+leverage`（`broker/src/wire.rs:23-25`），整数手换算在 trading-service 服务端；模板侧的失守点是 `broker/src/mock.rs`（本地模拟器，现状 crypto 小数数学，见其头注释 L8-9）与 parity 覆盖为 0（`backtest_paper_parity.rs` 中 contract/ctp 出现次数=0）。因此**模板侧不改下单帧**，改 mock 与验收。
- **决策 3（平今/平昨）**：由 trading-service 决策，契约文档写明规则；模板不加 offset 字段（YAGNI）。
- 兼容性证据：`Ctx::new` 新字段全默认值；`contract: Option<ContractSpec>`，未在 contracts.yaml 声明的 symbol 走 crypto 兼容模式（contracts.yaml 头注 #4）；策略注册为 build.rs 自动扫描，不复制策略文件零影响。

## Global Constraints

- 本地仓库 `/mnt/d/wsl_cache/workspace/strategy-template-rs`，基线 `c82a23e` 工作区干净，`cargo check --workspace --tests` 已绿（2026-11-04 实测）。
- 全部工作在分支 `feat/ctp-integration`。
- 不新增外部 crate 依赖；不引入 ctp-rs/simnow 绑定。
- 不修改 `crates/strategies/`（registry 冻结）与 crypto 现有行为：每阶段末 `cargo test -p strategy-app --test backtest_paper_parity` 必须绿。
- 对外契约变更（`contracts/*.md`、signal.csv 列）必须同步文档与 golden 测试。
- 每个 commit 消息尾部加：`Co-Authored-By: Claude Code <noreply@anthropic.com>`。

## Review Focus

（每行在对应任务的测试里钉死）
1. 含午休/隔夜/节假日缺口的行情：`bar_merge=ctp` 桶边界与 `Ctx::newly_closed` 不得凭空造边界或漏闭合 → Task 1 移植测试 + Task 9。
2. 未在 contracts.yaml 声明的 symbol（全部 crypto）行为逐字节不变 → Task 3 基线对比 + 各阶段 parity 绿。
3. 交易日历误判（日历未覆盖的品种/日期）：必须 fallback 到现有 23:00 启发式并 `warn!`，不得丢 bar、不得 panic → Task 5。
4. mock 整数手取整与 backtest sizing 同函数，临界值（cash 恰等于 1 手名义）两处结果一致 → Task 7/8 共用纯函数 + 边界测试。
5. signal.csv/下单帧不新增必填字段，旧 reader（含测试 fixture）不破 → Task 9 列头断言。

---

## Phase A — 移植 CTP 仓库已有设施

### Task 1: core+data 层移植（时段聚合 + 合约模块）

**Files:**
- Modify: `crates/core/src/{lib.rs,strategy.rs,signal.rs}`（补丁）
- Create: `crates/core/src/contract.rs`（从 CTP 仓库复制，462 行原样）
- Modify: `crates/data/src/{lib.rs,feed.rs}`（补丁）
- Create: `crates/data/src/ctp_session.rs`（随 diff 新增）

**Interfaces:**
- Consumes: base `c82a23e`。
- Produces: `strategy_core::contract::{ContractSpec, CommissionSpec, FeeMode, FeeKind, ctp_trading_day}`；`strategy_data::feed::BarMergeMode { Fixed, Ctp }`；`strategy_data::ctp_session`；`Ctx::{newly_closed, entry_confirm, signal_resume, session_open_price}` + `bar_closed(tf)`。Task 4/5/7/8 全部消费这些。

- [ ] **Step 1: 建立分支与 crypto 基线**

```bash
git checkout -b feat/ctp-integration
cargo test -p strategy-app --test backtest_paper_parity 2>&1 | tail -3   # 记录结果作对照
cargo run -p strategy-app -- backtest config/example_dual_ma/example.yaml \
  --out /tmp/baseline-crypto   # 保留 equity/trades CSV 供 Task 3 对比（参数以 --help 为准）
```

- [ ] **Step 2: 从 CTP 仓库生成 core+data 补丁（只读，不写其 index）**

```bash
CTP=/home/qpw/workspace/strategy-template-rs-ctp
git -C $CTP diff c82a23e -- \
  crates/core/src/strategy.rs crates/core/src/signal.rs crates/core/src/lib.rs \
  crates/data/src/ctp_session.rs crates/data/src/feed.rs crates/data/src/lib.rs \
  > /tmp/ctp-A-core-data.patch
```

- [ ] **Step 3: 应用补丁并复制 contract.rs**

```bash
git apply --check /tmp/ctp-A-core-data.patch && git apply /tmp/ctp-A-core-data.patch
cp $CTP/crates/core/src/contract.rs crates/core/src/contract.rs
```

预期：无报错。`lib.rs` 补丁已含 `pub mod contract;` 行，复制文件后即闭环。

- [ ] **Step 4: 验证**

Run: `cargo test -p strategy-core -p strategy-data`
Expected: 全绿（ctp_session 时段查表测试、feed bar_merge 测试在此首次运行）。失败只允许修移植接缝（use 路径等），禁止改断言。

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(core,data): 移植 CTP 时段聚合与期货合约参数模块

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

### Task 2: broker+app+tests 层移植（期货撮合 + 接线 + 报告）

**Files:**
- Modify: `crates/broker/src/backtest.rs`、`crates/app/src/{config.rs,engine.rs,main.rs,bootstrap.rs,report.rs,report_charts.rs,runtime_actor.rs}`（补丁）
- Modify: `crates/app/tests/{backtest_e2e.rs,backtest_paper_parity.rs,live_orchestrator.rs}`（补丁）
- Create: `config/contracts.yaml`（复制）
- Modify: `README.md`、`docs/strategy/DEVELOPMENT_GUIDE.md`（补丁）

**Interfaces:**
- Consumes: Task 1 全部导出类型。
- Produces: `AppConfig.contracts: BTreeMap<String, ContractSpec>`（自动加载 `config/contracts.yaml`）；`AppConfig::contract_for(symbol) -> Option<&ContractSpec>`（补丁已含私有 helper `contract_for`，暴露为 pub 供 Task 8/11 用）；`BacktestConfig.contract: Option<ContractSpec>`；CLI `backtest --fee-rate <f64>`；`RuntimeSpec.bar_merge: BarMergeMode`。

- [ ] **Step 1: 生成并应用 broker+app+tests+docs 补丁**

```bash
git -C $CTP diff c82a23e -- \
  crates/broker/src/backtest.rs \
  crates/app/src/config.rs crates/app/src/engine.rs crates/app/src/main.rs \
  crates/app/src/bootstrap.rs crates/app/src/report.rs crates/app/src/report_charts.rs \
  crates/app/src/runtime_actor.rs \
  crates/app/tests/backtest_e2e.rs crates/app/tests/backtest_paper_parity.rs \
  crates/app/tests/live_orchestrator.rs \
  README.md docs/strategy/DEVELOPMENT_GUIDE.md \
  > /tmp/ctp-B-app.patch
git apply --check /tmp/ctp-B-app.patch && git apply /tmp/ctp-B-app.patch
cp $CTP/config/contracts.yaml config/contracts.yaml
```

- [ ] **Step 2: 全量验证**

Run: `cargo test --workspace`
Expected: 全绿。禁止以修改测试断言换取通过。

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat(broker,app): 移植期货撮合、contracts.yaml 加载、CTP 报告与K线导出

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

### Task 3: crypto 口径回归验收（Phase A 出口）

**Files:**
- 无新文件；产出物是对比结论与 tag。

- [ ] **Step 1: 重跑 crypto 基线并 diff**

```bash
cargo run -p strategy-app -- backtest config/example_dual_ma/example.yaml --out /tmp/after-port-crypto
diff /tmp/baseline-crypto/*equity*.csv /tmp/after-port-crypto/*equity*.csv
```

Expected: equity/trades CSV 逐字节一致（Review Focus #2）。有 diff 即为兼容模式被破坏，回查 `contract: None` 分支；不得放宽为"误差可接受"。

- [ ] **Step 2: CTP 侧冒烟**

Run: `cargo test -p strategy-app --test backtest_e2e`
Expected: 全绿，含移植过来的 comment 承载开仓理由断言（原 commit `15fd5b0`）。

- [ ] **Step 3: 打基线 tag**

```bash
git tag ctp-port-baseline
```

---

## Phase B — 交易日历与行情契约（两仓库皆缺）

### Task 4: `TradingCalendar` 交易日历模块

**Files:**
- Create: `crates/data/src/trading_calendar.rs`
- Modify: `crates/data/src/lib.rs`（`pub mod trading_calendar;`）
- Test: 同文件 `#[cfg(test)]`

**Interfaces:**
- Consumes: `chrono::{NaiveDate, FixedOffset, DateTime}`。
- Produces:

```rust
pub struct TradingCalendar { /* 内嵌节假日表 + 覆盖边界 */ }
impl TradingCalendar {
    pub fn builtin() -> Self;                     // SHFE/DCE/CZCE 2024-01..2027-12
    pub fn is_trading_day(&self, d: NaiveDate) -> bool;
    /// 北京时区时间戳 → 所属交易日（夜盘归次一交易日）
    pub fn trading_day_of(&self, beijing: &DateTime<FixedOffset>) -> NaiveDate;
    pub fn next_trading_day(&self, d: NaiveDate) -> NaiveDate;
    pub fn prev_trading_day(&self, d: NaiveDate) -> NaiveDate;
    /// backfill 窗口起点：end 往前数 n 个交易日（end 为交易日则含）
    pub fn window_start(&self, end: NaiveDate, n: usize) -> NaiveDate;
}
```

- [ ] **Step 1: 写失败测试**：`weekend_is_not_trading_day`；`night_session_maps_to_next_trading_day`（周五 21:00 → 下周一）；`spring_festival_2026_blocked`；`window_start_skips_oct_long_holiday`。
  Run: `cargo test -p strategy-data trading_calendar` → Expected: FAIL（模块不存在）。
- [ ] **Step 2: 实现**：节假日以 `const HOLIDAYS: &[(i32, i32, i32)]` 静态数组内嵌，来源口径在模块头注释标注；调休补班的周末仍按非交易日（期货周末不开市）。
- [ ] **Step 3: `cargo test -p strategy-data` 全绿；Commit** `feat(data): 内嵌期货交易日历`（含 attribution 行，下同，不再重复）

### Task 5: feed 交易日判定接日历（保留启发式 fallback）

**Files:**
- Modify: `crates/data/src/feed.rs`（commit `cef1224` 引入的 23:00 收市启发式处）
- Test: `feed.rs` 内新增 `#[cfg(test)]`

**Interfaces:**
- Consumes: Task 4 `TradingCalendar`；Task 1 `ctp_session`/`BarMergeMode`。
- Produces: `Feed` 新方法 `pub fn with_calendar(mut self, cal: TradingCalendar) -> Self`；仅 `bar_merge=Ctp` 时生效（Fixed/crypto 零变化）。

- [ ] **Step 1: 写失败测试** `holiday_d1_uses_calendar_not_wallclock`：合成含国庆长假缺口的 1m + builtin 日历，断言 D1 桶不跨假日、tainted 首日=假日后首交易日。
- [ ] **Step 2: 实现**：日历覆盖内用 `trading_day_of`；覆盖外 fallback 现有启发式并 `warn!` 一次（Review Focus #3）。
- [ ] **Step 3: `cargo test -p strategy-data` 绿；Commit**

### Task 6: kline-service 契约期货形态（backfill 窗口交易日化）

**Files:**
- Modify: `contracts/kline-service.md`（新增"期货形态"节）
- Modify: `crates/data/src/remote_client.rs`
- Test: `remote_client.rs` 内 `#[cfg(test)]`

**Interfaces:**
- Consumes: Task 4 `window_start`。
- Produces: `RemoteConfig` 新字段 `#[serde(default)] pub market: MarketKind`；`#[derive(Deserialize, Clone, Copy, PartialEq, Default)] #[serde(rename_all="snake_case")] pub enum MarketKind { #[default] Crypto, Futures }`。Futures 模式 backfill：`startTime = calendar.window_start(today, bars)`，替代墙钟 `now - n*60s`（remote_client.rs 现有"按最近 N 根墙钟反推"处）。

- [ ] **Step 1: 写失败测试** `futures_backfill_window_skips_holidays`（2026-10-10 往前 20 交易日 → startTime 落在 2026-09-08，非墙钟 09-10）；`crypto_backfill_window_unchanged`（默认 Crypto 计算逐值相等）。
- [ ] **Step 2: 实现**：只分支包住窗口计算；`binance_row` 解析不动（列序兼容是上游服务职责，模板仅在契约文档约定）。
- [ ] **Step 3: 文档**：kline-service.md 期货形态节——symbol 规范（品种码 `CU` 或合约码 `CU2601`；主连续由上游服务或 Task 10 工具产出）、时段缺口与交易日语义、`market: futures` 配置示例。
- [ ] **Step 4: `cargo test -p strategy-data && cargo test --workspace` 绿；Commit**

---

## Phase C — paper/mock 合约化与 parity 闭环

### Task 7: `ContractSpec::sizing_lots` 共享纯函数（backtest 与 mock 单一口径）

**Files:**
- Modify: `crates/core/src/contract.rs`
- Modify: `crates/broker/src/backtest.rs`（改调用共享函数，删重复数学）
- Test: `contract.rs` 内新增 `#[cfg(test)]`

**Interfaces:**
- Produces:

```rust
impl ContractSpec {
    /// 整数手数 = min(floor(equity*leverage/名义每手), floor(cash/名义每手))，名义每手=price*multiplier；
    /// 且受保证金率约束（可负担手数 = floor(equity/(名义每手*margin_rate)) 与杠杆约束取小）。
    /// None = 一手都开不起
    pub fn sizing_lots(&self, price: f64, cash: f64, leverage: u32, equity: f64) -> Option<u64>;
}
```

- [ ] **Step 1: 写失败测试** `sizing_lots_margin_binds_at_high_leverage`、`sizing_lots_cash_binds_at_low_cash`、`sizing_lots_exact_one_lot_boundary`（cash 恰等于 1 手名义 → `Some(1)`；Review Focus #4）。
- [ ] **Step 2: 实现**；`broker/backtest.rs` 期货开仓手数计算替换为调用 `sizing_lots`——其现有 4 个 futures 测试（`futures_open_is_integer_lots_*` 等）全绿即行为等价的证明。
- [ ] **Step 3: `cargo test -p strategy-core -p strategy-broker` 绿；Commit**

### Task 8: mock broker 期货模式（paper 与 backtest 同口径）

**Files:**
- Modify: `crates/broker/src/mock.rs`
- Modify: `crates/app/src/runtime_actor.rs`（`RuntimeSpec` 增 `contract: Option<ContractSpec>`）、`crates/app/src/bootstrap.rs`（用 `contract_for(symbol)` 填充）
- Test: `mock.rs` 内 `#[cfg(test)]`；`crates/app/tests/live_orchestrator.rs` 补 1 用例

**Interfaces:**
- Consumes: Task 7 `sizing_lots`；Task 2 `contract_for`、`RuntimeSpec.bar_merge`。
- Produces: `MockScenario::contract(Option<ContractSpec>)` builder。期货模式：开仓帧 `order_cash` → `sizing_lots` 整数手（`contract: None` 保留现有 `cash/price` floor-1e-6 小数数学）；盈亏按乘数；手续费 open/close/close_today 三档；成交回执 quantity 为整数。

- [ ] **Step 1: 写失败测试** `mock_futures_open_fills_integer_lots`（order_cash=600_000，1 手名义=550_000 → 成交 1 手非 1.0909）、`mock_crypto_math_unchanged_when_contract_none`（现有 mock 测试逐值不变）。
- [ ] **Step 2: 实现**：换算全部走 `sizing_lots`，mock 内不重写数学（DRY）。
- [ ] **Step 3: 接线** bootstrap→RuntimeSpec→mock 的 contract 透传；`live_orchestrator.rs` 新增带 CU contract 的下单用例。
- [ ] **Step 4: `cargo test -p strategy-broker -p strategy-app` 绿；Commit**

### Task 9: CTP parity 与端到端验收（Phase B/C 出口）

**Files:**
- Modify: `crates/app/tests/backtest_paper_parity.rs`（现状 contract/ctp 覆盖 = 0）
- Create: `crates/app/tests/ctp_backtest_e2e.rs`
- 共享 fixture：`synthetic_ctp_1m()`（放 `crates/app/tests/common/mod.rs`）——带午休(10:15-10:30、11:30-13:30)/隔夜(15:00→21:00)/周五夜盘→周一缺口的合成 1m，内嵌一个长假周

**Interfaces:**
- Consumes: Task 1-8 全部产物。
- Produces: 模板北极星（回测/live 同内核）在期货口径下的验收基线。

- [ ] **Step 1: 写期货场景 parity 失败测试** `futures_backtest_paper_parity_cu`：同一 `example_dual_ma` 策略、同一 `synthetic_ctp_1m()`、同一 CU 合约参数，backtest 与 paper(mock) 各跑一遍，断言：逐笔手数一致且为整数、equity 曲线逐值相等（含乘数盈亏与平今费）、`Ctx::bar_closed` 同一 bar 在两条路径同点亮（Review Focus #1）。
- [ ] **Step 2: 写 `ctp_backtest_e2e.rs`**：测试内嵌临时 config 目录（等效 `example_dual_ma/CU.yaml`，Task 11 转正）——断言 contracts.yaml 自动加载、`backtest_equity/trades/signals.csv` 三件套存在、trades 的 `comment` 列非空、聚合大周期 K 线导出文件存在、signal.csv 列头与现有 golden 一致（Review Focus #5）。
- [ ] **Step 3: 修分叉**：实现期暴露的桶边界/平今触发时点差异，修在符合"决策 2/3 + contracts.yaml 头注"的一侧；禁止改断言迁就实现。
- [ ] **Step 4: `cargo test --workspace` 全绿；Commit** `test(app): CTP 回测/paper parity 与端到端验收`

---

## Phase D — 数据工具与模板配套

### Task 10: `roll_build` 主力连续构造工具

**Files:**
- Create: `crates/data/src/bin/roll_build.rs`
- Test: 同文件 `#[cfg(test)]`

**Interfaces:**
- Consumes: `crates/data/src/csv_store.rs` 现有 10 列读写。
- Produces: CLI `roll_build --in <dir> --product CU --out <dir> [--backadjust none|ratio]`；选月纯函数 `fn pick_main<'a>(rows: impl Iterator<Item = (&'a str, f64)>, today: NaiveDate) -> Option<&'a str>`（当日末 bar 持仓量最大，剔除进入交割月合约）；输出主力 1m 标准 10 列 CSV + `roll_events.csv`（date,new_symbol,old_symbol）。

- [ ] **Step 1: 写失败测试** `pick_main_uses_last_bar_oi`、`pick_main_skips_delivery_month`。
- [ ] **Step 2: 实现**：合约目录两种布局识别规则参考 CTP 仓库 `spread_prep.rs` 头注释（全生命周期目录 + 逐交易日目录），代码重写、不含策略专用元数据通道。
- [ ] **Step 3: 冒烟** `cargo run -p strategy-data --bin roll_build -- --help` 退出 0；Commit

### Task 11: 期货示例配置与文档

**Files:**
- Create: `config/example_dual_ma/CU.yaml`（`bar_merge: ctp`、`market: futures`、symbol CU，复用 contracts.yaml 条目）
- Modify: `contracts/trading-service.md`（新增"期货语义"节：开仓帧 `order_cash+leverage` 由服务端换算整数手与保证金；平今/平昨选择规则归服务端（决策 3）；平仓帧 quantity 为整数手）
- Modify: `config/_global.example.yaml`（contracts.yaml 字段说明注释）、`docs/RUNBOOK.md`（新增 §CTP：日历覆盖范围 2024-2027 与续期责任、行情源要求（kline-service 期货形态）、roll_build 用法）

**Interfaces:**
- Consumes: Task 2 `contract_for`、Task 6 `MarketKind`、Task 4 日历。

- [ ] **Step 1: 写失败测试** `example_cu_config_parses`（`crates/app/src/config.rs` 的 `#[cfg(test)]`：加载 `config/example_dual_ma/CU.yaml`，断言 `bar_merge == Ctp` 且 `contract_for("CU").is_some()`）。
- [ ] **Step 2: 配置与文档落地。**
- [ ] **Step 3: `cargo test -p strategy-app` 绿；Commit** `feat(config,docs): 期货示例配置与 CTP RUNBOOK`

### Task 12: 批跑脚本转正

**Files:**
- Create: `scripts/run_backtest_matrix.sh`、`scripts/collect_reports.py`、`scripts/collect_benchmark.py`（自 CTP 仓库未跟踪版本移植；策略名/品种硬编码改为参数，`--dry-run` 支持）

- [ ] **Step 1: 移植并参数化**（用法注释置文件头）。
- [ ] **Step 2: 验证**：`bash -n scripts/*.sh`；`bash scripts/run_backtest_matrix.sh --dry-run example_dual_ma example.yaml` 打印将执行组合且退出 0；`python3 -I scripts/collect_reports.py --help` 正常。
- [ ] **Step 3: Commit** `chore(scripts): 回测矩阵批跑与报告收集工具`

---

## 整体验收（计划出口）

- [ ] `cargo test --workspace` 全绿（贴输出）。
- [ ] Task 3 的 crypto 基线 diff 在合并前重验一次为空。
- [ ] Task 9 的 `futures_backtest_paper_parity_cu` 与 `ctp_backtest_e2e` 绿 = 北极星在期货口径成立。
- [ ] `git log --oneline c82a23e..HEAD` 每层一 commit，可 bisect。
- [ ] 复盘入 RUNBOOK：日历 2027 年底续期、contracts.yaml 费率时效核对、上游 kline-service 期货形态上线状态三项维护提醒。
