# 横截面多组同进程（PR-6）Implementation Plan

<!-- refcheck:coords=off  历史实施计划（已完成）：坐标随后续重构漂移，不再参与 CI 坐标校验。 -->

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 一个 `strategy-rs` 进程同时运行多个横截面组：配置集目录一次加载、共享交易客户端与 Store、按组独立 actor/NAV/账本，组间订阅合并且互不串扰。

**Architecture:** 配置集目录（无顶层 `strategy.yaml`、每个直接子目录一个组）加载为 `Vec<XsecSpec>`；`bootstrap` 用一条 `start_live_xsec_set` 装配共享 `TradingServiceClient` / `Store` / `OrderRouter`，每组独立 `spawn_xsec_live`。订阅按 `(strategy_type, exchange)` 合并 symbol 并集；K 线按 symbol 扇出到各组；warmup 按 symbol 只回填一次、按组截取自己的尾部长度。live 模式要求组间 universe 不相交（同账户同标的远程持仓只有一份）。

**Tech Stack:** Rust（workspace：`strategy-app` / `strategy-engine` / `strategy-broker` / `strategy-data` / `strategy-store`），tokio，serde_yaml，tracing。

**Spec:** `deliverables/gstack/feature-dev-cross-sectional-strategy-2026-11-09.md`（§16 单进程多策略拓扑）；PR-6 范围依据 `docs/superpowers/plans/2026-11-11-xsec-live-pr5.md:451` 的预留项。

## Global Constraints

- 每个任务严格执行 TDD：先写/改测试 → 运行确认失败 → 最小实现 → 运行确认通过 → 提交。
- 提交前至少跑本任务涉及的测试；分支收口必须跑 `cargo test --workspace`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo fmt --all -- --check`。
- commit 格式「类型: 简述」（`feat:` / `fix:` / `test:` / `docs:`），结尾加 `Co-Authored-By: Claude Code <anthropic@anthropic.com>`；只提交本任务文件，**禁止 `git add -A`**。
- **不远程推送**；不触碰 `docs/design-package/01-需求分析.md` 与 `docs/design-package-old/`（工作区已有未提交改动）。
- 不提交 `.env`/密钥；测试与演练只用 mock，绝不下真实单；不在仓库根用真实 `config/_global.yaml` 跑测试（先 `current_dir` 到临时目录）。
- 注释/测试消息用简体中文；禁止 emoji、占位代码、空 catch。
- 不改 `node_modules` / `dist` / `.git`。

## Review Focus

1. 配置集目录里混入非组目录（旧单标的 yaml 目录、`state/`、隐藏目录之外的杂物）——必须拒启并点名，不能静默跳过。测试：Task 1 `set_dir_rejects_stray_subdirectory_without_strategy_yaml`。
2. live 模式下两组 universe 重叠——同一账户同一标的远程持仓只有一份，必须拒启，否则双方对账会把对方仓位当自己的。测试：Task 1 `set_validation_live_requires_disjoint_symbols_paper_allows_overlap`。
3. 同 `strategy_type` 两组的回报串组——必须 order_id 精确路由；未绑定广播由各组 `owns_signal` 过滤。测试：Task 3 两组同类型、各自 scope 行互不出现；既有 `order_router` / T7 测试继续钉住精确路由。
4. 一组 panic/收口不得拖死其他组（共享 Store flush 锁、共享 forwarder）——其他组继续处理 bar，shutdown 全部收口。测试：Task 3 `panic_in_one_group_does_not_stop_sibling_group`。
5. 一组 warmup 回填失败/不足不得阻断其他组启动，各组仍按自身 `warmup_days` 截取预热长度。测试：Task 2 e2e 中一组含无历史标的，另一组照常产出账本行。

---

### Task 1: 配置集目录加载与跨组校验

**Files:**
- Modify: `crates/app/src/xsec_config.rs`（新增公开函数与私有 `same_services` 助手）
- Test: `crates/app/tests/xsec_config_guards.rs`（追加测试与夹具助手）

**Interfaces:**
- Consumes: 既有 `load_xsec(path) -> Result<XsecSpec>`、`strategy_yaml_in(dir)`、`FileConfig`、`XsecSpec`、`TradingServiceCfg`。
- Produces:
  - `pub fn is_xsec_set_dir(path: &Path) -> bool` — 目录、无顶层 `strategy.yaml`、至少一个直接子目录含 `strategy.yaml`。
  - `pub fn load_xsec_set(path: &Path) -> anyhow::Result<Vec<XsecSpec>>` — 单组路径 → `vec![load_xsec(path)?]`；组目录集 → 按子目录名排序逐个 `load_xsec`；直接子目录缺 `strategy.yaml` 即报错点名（跳过隐藏目录）；空集报错。
  - `pub fn validate_xsec_set(specs: &[XsecSpec], run_mode: &str) -> anyhow::Result<()>` — 跨组校验，错误聚合为 `横截面配置集校验失败:\n- ...`。

- [ ] **Step 1: 写失败测试（loader + 校验）**

`crates/app/tests/xsec_config_guards.rs` 顶部 `use` 改为包含 `is_xsec_set_dir, load_xsec_set, validate_xsec_set`，并追加：

```rust
fn set_group_yaml(strategy_id: &str, universe: &str) -> String {
    format!(
        "{}{}",
        base_yaml().replace(
            "strategy_id: xsec_momentum:default",
            &format!("strategy_id: {strategy_id}")
        ),
        universe
    )
}

/// 两组目录：a_group=BTCUSDT、b_group=ETHUSDT。
fn write_set(root: &Path) {
    for (name, universe) in [("a_group", "  - BTCUSDT"), ("b_group", "  - ETHUSDT")] {
        let dir = root.join(name);
        write(
            &dir,
            "strategy.yaml",
            &set_group_yaml(&format!("xsec_{name}"), universe),
        );
    }
}
```

测试：

```rust
#[test]
fn set_dir_loads_groups_in_directory_name_order() {
    let tmp = tempfile::tempdir().unwrap();
    let set = tmp.path().join("set");
    write(&set, "_global.yaml", "account: acct_shared\nmode: paper_trading\n");
    write_set(&set);
    assert!(is_xsec_set_dir(&set));
    let specs = load_xsec_set(&set).expect("set loads");
    let ids: Vec<&str> = specs.iter().map(|s| s.strategy_id.as_str()).collect();
    assert_eq!(ids, vec!["xsec_a_group", "xsec_b_group"]);
    validate_xsec_set(&specs, "paper_trading").expect("same account/mode/services");
}

#[test]
fn set_dir_rejects_stray_subdirectory_without_strategy_yaml() {
    let tmp = tempfile::tempdir().unwrap();
    let set = tmp.path().join("set");
    write_set(&set);
    std::fs::create_dir_all(set.join("legacy")).unwrap();
    write(&set.join("legacy"), "BTCUSDT.yaml", "strategy: example\n");
    let err = load_xsec_set(&set).unwrap_err().to_string();
    assert!(err.contains("legacy"), "报错须点名字目录: {err}");
}

#[test]
fn single_config_dir_is_not_a_set_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("one");
    write(&dir, "strategy.yaml", &format!("{}{}", base_yaml(), universe("  - BTCUSDT")));
    assert!(is_xsec_config_path(&dir));
    assert!(!is_xsec_set_dir(&dir));
}

#[test]
fn set_validation_rejects_duplicate_strategy_id() {
    let tmp = tempfile::tempdir().unwrap();
    let set = tmp.path().join("set");
    write_set(&set);
    write(
        &set.join("b_group"),
        "strategy.yaml",
        &set_group_yaml("xsec_a_group", "  - ETHUSDT"),
    );
    let specs = load_xsec_set(&set).unwrap();
    let err = validate_xsec_set(&specs, "paper_trading").unwrap_err().to_string();
    assert!(err.contains("strategy_id"), "{err}");
}

#[test]
fn set_validation_rejects_cross_group_inconsistencies() {
    // b_group 自己的 _global.yaml 覆盖父目录；每 case 断言错误含对应字段名。
    let cases = [
        ("account: acct_b\n", "account"),
        ("services:\n  trading:\n    ws_url: ws://other.example/ws\n", "services"),
        ("mode: live\n", "mode"),
        ("state_flush_interval_secs: 5\n", "state_flush_interval_secs"),
    ];
    for (group_global, needle) in cases {
        let tmp = tempfile::tempdir().unwrap();
        let set = tmp.path().join("set");
        write(&set, "_global.yaml", "account: acct_a\nmode: paper_trading\n");
        write_set(&set);
        write(&set.join("b_group"), "_global.yaml", group_global);
        let specs = load_xsec_set(&set).unwrap();
        let err = validate_xsec_set(&specs, "paper_trading").unwrap_err().to_string();
        assert!(err.contains(needle), "case {needle}: {err}");
    }
}

#[test]
fn set_validation_live_requires_disjoint_symbols_paper_allows_overlap() {
    let tmp = tempfile::tempdir().unwrap();
    let set = tmp.path().join("set");
    write(&set, "_global.yaml", "account: acct_shared\nmode: paper_trading\n");
    write(&set.join("a_group"), "strategy.yaml", &set_group_yaml("xsec_a", "  - BTCUSDT"));
    write(&set.join("b_group"), "strategy.yaml", &set_group_yaml("xsec_b", "  - BTCUSDT"));
    let specs = load_xsec_set(&set).unwrap();
    validate_xsec_set(&specs, "paper_trading").expect("paper overlap allowed");
    let err = validate_xsec_set(&specs, "live").unwrap_err().to_string();
    assert!(
        err.contains("BTCUSDT") && err.contains("xsec_a") && err.contains("xsec_b"),
        "{err}"
    );
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p strategy-app --test xsec_config_guards -v`
Expected: 编译失败（`is_xsec_set_dir`/`load_xsec_set`/`validate_xsec_set` 未定义）。

- [ ] **Step 3: 实现 loader 与校验**

`xsec_config.rs` 追加：

```rust
pub fn is_xsec_set_dir(path: &Path) -> bool;
pub fn load_xsec_set(path: &Path) -> anyhow::Result<Vec<XsecSpec>>;
pub fn validate_xsec_set(specs: &[XsecSpec], run_mode: &str) -> anyhow::Result<()>;
```

实现要点：
- `is_xsec_set_dir`：`path.is_dir() && strategy_yaml_in(path).is_none() && group_dirs(path).iter().any(|d| strategy_yaml_in(d).is_some())`；`group_dirs` = 直接子目录、跳过文件名以 `.` 开头的条目。
- `load_xsec_set`：单组路径先走 `is_xsec_config_path` 短路；组目录集用 `group_dirs` 按 `file_name()` 排序；缺 `strategy.yaml` 的目录收集后一次性 `bail!("配置集子目录缺少 strategy.yaml（每个子目录必须是一组配置）: {}", bad.join(", "))`；空集 `bail!("配置集目录下没有含 strategy.yaml 的子目录")`。
- `validate_xsec_set`：逐项检查并聚合错误：
  - `strategy_id` 唯一（`BTreeMap` 记录首次出现组）。
  - 所有 `instruments[].exchange` 相同；所有 `instruments[].account` 相同。
  - `run_mode` 基线（`spec.run_mode`）相同。
  - `services` 相同：比较 `kline.is_some()` 与 `TradingServiceCfg` 七字段（`ws_url`、`request_timeout_ms`、`submit_enabled`、`reconnect_backoff_max_secs`、`strategy_secret`、`exchange`、`account`）；一组 `None`、另一组 `Some` 亦为不一致。
  - `state_flush_interval_secs` 与 `state_prune_terminal` 相同（Store 进程级单例）。
  - `run_mode == "live"` 时组间 `instruments[].symbol` 不相交：`BTreeMap<&str, &str>` 记录 symbol → strategy_id，重复即报“同一账户同一标的远程持仓只有一份”。
  - 传入的 `run_mode` 只用于 live 相交检查；非法 mode 已由 `resolve_run_mode` 拦截。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p strategy-app --test xsec_config_guards -v`
Expected: 原有用例 + 新增 6 个全绿。

- [ ] **Step 5: 提交**

```bash
git add crates/app/src/xsec_config.rs crates/app/tests/xsec_config_guards.rs
git commit -m "feat: 横截面配置集目录加载与跨组校验" -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

### Task 2: 多组装配（共享客户端/Store/路由）与 paper 验收

**Files:**
- Modify: `crates/app/src/bootstrap.rs:360-589`（`run_live_xsec` → `start_live_xsec_set` / `run_live_xsec_set` + `XsecLiveSet`）
- Test: `crates/app/tests/xsec_live_mock_e2e.rs`（追加多组 paper e2e；复用既有 `start_drill` / `write_drill_csv`）

**Interfaces:**
- Consumes: Task 1 的 `XsecSpec` 列表；既有 `TradingServiceClient::start_arc`、`subscribe_klines`、`TradingServiceKlineClient::start/attach/backfill`、`Store`、`spawn_xsec_live`、`OrderRouter`、`RouteBind`。
- Produces:
  - `pub struct XsecLiveSet`，唯一公开方法 `pub async fn shutdown(self)`。
  - `pub async fn start_live_xsec_set(specs: Vec<XsecSpec>, services: ServicesConfig, output_dir: PathBuf, kline_data_dir: PathBuf, run_mode: String, state_flush_interval: Duration, state_prune_terminal: bool) -> anyhow::Result<XsecLiveSet>`。
  - `pub async fn run_live_xsec_set(...same...) -> anyhow::Result<()>`：`start` → 等 ctrl-c → `shutdown`。

- [ ] **Step 1: 写失败测试（两组 paper，重叠 symbol + 一组含无历史标的）**

在 `crates/app/tests/xsec_live_mock_e2e.rs` 追加；顶部补 `use strategy_app::bootstrap::start_live_xsec_set;`、`use strategy_app::config::{ServicesConfig, TradingServiceCfg};`、`use strategy_app::xsec_config::XsecSpec;`、`use strategy_engine::xsec::model::StrategyPortfolio;`。

构造 `multi_group_paper_spec(id, symbols) -> XsecSpec`：`strategy_type="xsec_momentum"`、`config=xsec_cfg()` 但 `strategy_id=Some(id)` 且 `universe=symbols` 对应、`instruments=symbols.map(symbol_spec)`、`run_mode="paper_trading"`、`warmup_days=0`、`services=None`。

```rust
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
    let spec_a = multi_group_paper_spec("xsec_grp_a", &["BTCUSDT", "ETHUSDT"]);
    let spec_b = multi_group_paper_spec("xsec_grp_b", &["BTCUSDT", "DOGEUSDT"]);
    let set = start_live_xsec_set(
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

    let probe = Store::new(dir.path().join("out").join("state"), Arc::new(EpochClock));
    probe.recover::<StrategyPortfolio>().unwrap();
    let rows = probe.table::<StrategyPortfolio>().list();
    let ids: Vec<&str> = rows.iter().map(|r| r.strategy_id.as_str()).collect();
    assert!(
        ids.contains(&"xsec_grp_a") && ids.contains(&"xsec_grp_b"),
        "两组都要有账本行（缺行说明该组没消费到任何 bar）: {rows:?}"
    );
    assert!(probe.table::<SubPosition>().is_empty(), "空指标配置不应开仓");
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p strategy-app --test xsec_live_mock_e2e multi_group_paper_shares_store_and_fans_out_overlapping_symbol -v`
Expected: 编译失败（`start_live_xsec_set` 未定义；`run_live_xsec` 仍是单组签名）。

- [ ] **Step 3: 实现多组装配**

`bootstrap.rs` 重写 360-589 段：

1. 保留 `check_kline_retired`；`ensure!(!specs.is_empty())`；调用 `crate::xsec_config::validate_xsec_set(&specs, &run_mode)?` 作为防御位。
2. 交易客户端一次：`TradingServiceClient::start_arc`（同现状）。
3. 订阅合并：`BTreeMap<(String, String), BTreeSet<String>>` 收集所有 `(strategy_type, exchange)` → symbol 并集；逐 key 调 `subscribe_klines(stype, exchange, &symbols_vec)`；失败仅 warn。
4. 唯一 symbol 并集 + routes：`BTreeSet<String>` 全量；`routes` 按 spec 顺序 `entry(symbol).or_insert((strategy_type, exchange))`（首个组胜出，paper 重叠可接受）。
5. warmup：`BTreeMap<String, usize>` 取每 symbol 所有组的 `warmup_days.max(1)*1440` 最大值；每 symbol `kline.backfill(M1, max_bars)` 只调一次，失败 warn + 空；再按每组/每标的 `want = warmup_days.max(1)*1440` 截取尾部 `bars[len.saturating_sub(want)..].to_vec()` 填入该组 `warmups`。
6. 每组每标的 `attach` → std mpsc forwarder（沿用现状代码，`Vec<Receiver>` 每组一份）。
7. `Store::new_with_policy(&output_dir.join("state"), EpochClock, state_prune_terminal)` 一次，`Arc` 共享。
8. 每组：strategy factory（`make_xsec_strategy(&spec)`）、gateway factory（`live` 时 `TradingGateway::with_submit(trading.clone(), group.strategy_type.clone(), venue_from_first, true)`；paper → None）、`XsecLiveSpec`（`initial_cash_total` 按组求和）、`spawn_xsec_live`，收集 `(handle, task)`；`bind_tx` 共享，每组 `route_tx: Some(bind_tx.clone())`。
9. 一个 `OrderRouter`：`typed = specs.iter().zip(handles).map(|(s, h)| (s.strategy_type.clone(), run_mode=="live", h.clone()))`；单个转发器同时消费 `trading_events` 与 `bind_rx`（沿用现状 loop）。
10. `XsecLiveSet { handles, actor_tasks, forwarders, order_forwarder }`；`shutdown(self)`：逐 handle `shutdown().await`，再用 `futures::future::join_all` 对每个 actor task 包 `timeout(8s)`，最后 abort `order_forwarder` 与 `forwarders`。
11. `run_live_xsec_set`：`let set = start_live_xsec_set(...).await?; tokio::signal::ctrl_c().await.ok(); info!("收到退出信号..."); set.shutdown().await; info!("横截面运行时已关闭"); Ok(())`。
12. 启动日志每组一条：`strategy_id / mode / instruments / group_index`。

注意：`start_live_xsec_set` 内 `tokio::runtime::Handle::current()` 取一次并 `clone()` 给所有客户端/forwarder；`services.trading` 字段被 move 进 `TradingConfig`（与现状一致）；文件顶部 `run_live_xsec` 的 doc 注释同步改名，测试文件里“与 bootstrap::run_live_xsec 同款”注释改为 `start_live_xsec_set`。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p strategy-app --test xsec_live_mock_e2e -v`
Expected: 既有 PR-5 e2e 全绿 + 新多组 paper e2e 绿。

- [ ] **Step 5: 提交**

```bash
git add crates/app/src/bootstrap.rs crates/app/tests/xsec_live_mock_e2e.rs
git commit -m "feat: 横截面多组同进程装配（共享客户端/Store/路由）" -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

### Task 3: 跨组隔离验收（live 账本 scope + panic 断路）

**Files:**
- Test: `crates/app/tests/xsec_live_mock_e2e.rs`（追加两个验收测试 + 多组转发器测试助手）
- 生产代码预期不改；若测试暴露跨组状态泄漏，修 `crates/app/src/xsec_live_actor.rs` 或 `order_router.rs` 后同任务提交。

**Interfaces:**
- Consumes: Task 2 的 `start_live_xsec_set`；既有 `spawn_drill_actor` / `spawn_t8_actor` / `ScriptedStrategy` / `OpenThenPanicStrategy` / `StubGateway` / `StubState`。
- Produces: 仅测试助手 `spawn_multi_group_actors(...) -> (Vec<RuntimeHandle>, Vec<JoinHandle<()>>, JoinHandle<()>)`（单 `OrderRouter` 管多组）。

- [ ] **Step 1: 写失败/验收测试**

测试 A `multi_group_live_shared_store_keeps_scopes_isolated`：
- `start_drill` 起 mock；共享一个 `Arc<Store>`、一个 `TradingServiceClient` 事件流。
- 组 A：`strategy_id="xsec_grp_a"`、`instruments=[BTCUSDT]`、脚本首轮 `open_action(BTC)`；组 B：`strategy_id="xsec_grp_b"`、`instruments=[ETHUSDT]`、脚本首轮 `open_action(ETH)`；两组 `strategy_type` 同为 `xsec_momentum`。
- `spawn_multi_group_actors` 内：每组 `spawn_xsec_live`（`route_tx: Some(bind_tx.clone())`），单个转发器用 `OrderRouter::new(vec![(type, true, hA), (type, true, hB)])` 消费 `trading_events` 与 `bind_rx`。
- 从 mock 回放推进 bar，等两组各自出现 `SubPosition` 且 `state == Active`。
- 断言：`SubPosition` 行按 `scope` 分组——`xsec_grp_a` 只有 BTC 行、`xsec_grp_b` 只有 ETH 行；`StrategyPortfolio` 两行 id 分别为两组；A 的 BTC 行不被 B 的 scope 持有。

测试 B `panic_in_one_group_does_not_stop_sibling_group`：
- 复用 T8 桩：`StubGateway`/`StubState` 为每组独立实例（计数分开）。
- 组 A 策略 `OpenThenPanicStrategy`（第 1 轮开仓提交、第 2 轮 panic）；组 B 策略每轮都发 `open_action(ETH)`（或 `Adjust`）以持续产生提交。
- 共享 `Store`，两个 `spawn_xsec_live` + 两个独立 channel；不接交易客户端。
- 推 bar 至 A panic 后继续推 5 轮：断言 A 的 `stub_a.attempts` 固定为 1（Paused 门控），B 的 `stub_b.attempts` 持续增长；两边 `shutdown().await` 后两个 actor task 均在 10s 内 join。

- [ ] **Step 2: 运行**

Run: `cargo test -p strategy-app --test xsec_live_mock_e2e multi_group_live_shared_store_keeps_scopes_isolated panic_in_one_group_does_not_stop_sibling_group -v`
Expected: 预期首跑即绿（PR-5 已按组隔离；共享 Store 的 scope 过滤与 `owns_signal` 已在位）。若红，按失败点修最小代码，复跑至绿。

- [ ] **Step 3: 回归本轮相关套件**

Run: `cargo test -p strategy-app --test xsec_live_mock_e2e -v`
Expected: 全绿（含 Task 2 多组 paper）。

- [ ] **Step 4: 提交**

```bash
git add crates/app/tests/xsec_live_mock_e2e.rs
git commit -m "test: 横截面多组 live 账本隔离与 panic 断路验收" -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

### Task 4: main `run` 配置集分流 + CLI 守卫

**Files:**
- Modify: `crates/app/src/main.rs:645-709`（Run xsec 分支）与 `:369-388`（Backtest 分支）
- Test: `crates/app/tests/run_config_guards.rs`（追加 set 目录夹具与 3 个 CLI 级测试）

**Interfaces:**
- Consumes: Task 1 的 `is_xsec_set_dir` / `load_xsec_set` / `validate_xsec_set`；Task 2 的 `run_live_xsec_set`。
- Produces: `Commands::Run` 在 `--config` 指向单组或组目录集时都进入 xsec 分支；`backtest` 指向组目录集时明确拒绝。

- [ ] **Step 1: 写失败测试（CLI 级）**

`run_config_guards.rs` 追加：
- `fn write_xsec_set(tmp: &Path) -> PathBuf`：`set/a/strategy.yaml`（id `xsec_a`，universe BTCUSDT）、`set/b/strategy.yaml`（id `xsec_b`，universe ETHUSDT）、`set/_global.yaml`（`services.trading` 占位端点 + `mode: live`）；group yaml 用最小横截面 yaml（`strategy_type: xsec_momentum`、`strategy_id`、`universe`、`timeframes: [1m]`、空 indicators/entry/exit、`portfolio_exit`、`rebalance`、`risk`、`sizing`）。
- `fn run_backtest_cli(cfg: &str, tmp: &Path) -> (Option<ExitStatus>, String)`：`backtest --config cfg --start 20260101 --end 20260102 --data-dir klines --output-dir out`，5s 超时后 kill，捕获 stdout+stderr（实现与 `run_xsec_cli` 同款轮询）。

测试：
```rust
#[test]
fn xsec_set_run_enters_set_loader_and_refuses_placeholder_live() {
    let tmp = tempfile::TempDir::new().unwrap();
    let set = write_xsec_set(tmp.path());
    let (status, text) = run_xsec_cli(set.to_str().unwrap(), &["--mode", "live"], tmp.path());
    assert_eq!(status.and_then(|s| s.code()), Some(1), "{text}");
    assert!(text.contains("占位值") && text.contains("submit_enabled"), "{text}");
}

#[test]
fn xsec_set_backtest_is_refused_with_run_hint() {
    let tmp = tempfile::TempDir::new().unwrap();
    let set = write_xsec_set(tmp.path());
    let (status, text) = run_backtest_cli(set.to_str().unwrap(), tmp.path());
    assert_eq!(status.and_then(|s| s.code()), Some(1), "{text}");
    assert!(text.contains("仅支持 run"), "须提示逐组 backtest: {text}");
}

#[test]
fn xsec_set_duplicate_strategy_id_is_refused() {
    let tmp = tempfile::TempDir::new().unwrap();
    let set = write_xsec_set(tmp.path());
    let b = set.join("b").join("strategy.yaml");
    let body = std::fs::read_to_string(&b)
        .unwrap()
        .replace("strategy_id: xsec_b", "strategy_id: xsec_a");
    std::fs::write(&b, body).unwrap();
    let (status, text) = run_xsec_cli(set.to_str().unwrap(), &[], tmp.path());
    assert_eq!(status.and_then(|s| s.code()), Some(1), "{text}");
    assert!(text.contains("strategy_id"), "{text}");
}
```
注：`xsec_set_run...` 只在占位端点 + live 时拒启；`run_xsec_cli` 的 5s 超时保证不会挂住。

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p strategy-app --test run_config_guards xsec_set -v`
Expected: 三测失败（set 目录仍走旧 loader 或不被识别）。

- [ ] **Step 3: 改 main 分流**

Run 分支：把现有 `if is_xsec_config_path(p)` 改为 `if is_xsec_config_path(p) || is_xsec_set_dir(p)`；内部：
```rust
let specs = strategy_app::xsec_config::load_xsec_set(p)?;
let modes: Vec<String> = specs
    .iter()
    .map(|s| strategy_app::xsec_config::resolve_run_mode(mode.as_deref(), s))
    .collect::<anyhow::Result<_>>()?;
let run_mode = modes[0].clone();
anyhow::ensure!(modes.iter().all(|m| m == &run_mode), "横截面配置集各组 mode 解析结果不一致");
strategy_app::xsec_config::validate_xsec_set(&specs, &run_mode)?;
let services = specs[0].services.clone().ok_or_else(|| anyhow::anyhow!("..."))?;
```
后续 `check_kline_retired` / 占位守卫 / `create_dir_all` / runtime 构造保持不变；`flush_secs = specs[0].state_flush_interval_secs`、`prune = specs[0].state_prune_terminal`；改调 `bootstrap::run_live_xsec_set(specs, services, ...)`。覆盖参数拒绝文案保留（可追加“（配置集同此）”）。

Backtest 分支：在单组 `if is_xsec_config_path(p)` 之后加
```rust
if strategy_app::xsec_config::is_xsec_set_dir(p) {
    anyhow::bail!("横截面配置集（多组）仅支持 run：backtest 请逐组用 --config <组目录>/strategy.yaml");
}
```

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p strategy-app --test run_config_guards -v`
Expected: 原有 8 测 + 新 3 测全绿。

- [ ] **Step 5: 提交**

```bash
git add crates/app/src/main.rs crates/app/tests/run_config_guards.rs
git commit -m "feat: run 支持横截面配置集目录，backtest 明确拒绝" -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

### Task 5: 拒单清理 `open_meta` 侧信道

**Files:**
- Modify: `crates/engine/src/xsec/dispatch.rs:143-153`（`open_position` 拒单分支）
- Test: `crates/engine/src/xsec/dispatch.rs`（扩展现有 `rejected_open_removes_subposition_and_counts`）

**Interfaces:**
- Consumes: `PositionSync.open_meta: HashMap<String, (f64, f64, f64)>`（键 = 开仓 `signal_id`）。
- Produces: 拒单后 `open_meta` 与 `sub_positions` 状态一致（无孤儿条目）。

- [ ] **Step 1: 改测试加断言**

在 `rejected_open_removes_subposition_and_counts` 末尾追加：
```rust
assert!(
    slot.pos_sync.open_meta.is_empty(),
    "拒单须同时清理 open_meta 侧信道（否则成交落地会读到幽灵条目）"
);
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p strategy-engine --lib xsec::dispatch::tests::rejected_open_removes_subposition_and_counts -v`
Expected: FAIL，`open_meta` 仍含 1 条。

- [ ] **Step 3: 修复**

拒单分支在 `sub_positions.retain(|s| s.id != order.signal_id);` 之前/之后加：
```rust
pos_sync.open_meta.remove(&order.signal_id);
```
（`order.signal_id` 在 `retain` 后仍可用；`pos_sync` 与 `sub_positions` 同段可变借用，按现有解构位置插入。）

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p strategy-engine --lib xsec::dispatch::tests -v`
Expected: 全绿。

- [ ] **Step 5: 提交**

```bash
git add crates/engine/src/xsec/dispatch.rs
git commit -m "fix: 开仓拒单清理 open_meta 侧信道" -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

### Task 6: 平仓拆腿失败不得卡在 `CloseRequested`

**Files:**
- Modify: `crates/engine/src/xsec/dispatch.rs`（`close_position` 状态推进顺序）
- Test: `crates/engine/src/xsec/dispatch.rs`（新增 `close_error_keeps_sub_position_active` + 测试桩 `FailingSplitRules`）

**Interfaces:**
- Consumes: `close_qty_inner(...) -> Result<DispatchOutcome>`；`advance_sub_state(Active, CloseRequested)`。
- Produces: `close_position` 仅在 `close_qty_inner` 返回 `Ok` 且 `out.actions > 0` 时把子仓位推进为 `CloseRequested`；任何 `Err`/0 腿都保持 `Active`（可下一轮重试）。

- [ ] **Step 1: 写失败测试**

```rust
struct FailingSplitRules;

impl PositionRules for FailingSplitRules {
    fn mode(&self) -> PositionMode {
        PositionMode::Net
    }
    fn split_close(
        &self,
        _book: &PositionBook,
        _req: &CloseRequest,
        _session: &dyn crate::core::session::SessionRules,
    ) -> Result<Vec<CloseLeg>> {
        Err(crate::core::error::SpiError::Execution(
            "测试注入：拆腿失败".into(),
        ))
    }
    fn on_open_fill(
        &self,
        _book: &mut PositionBook,
        _draft: &OrderDraft,
        _filled_qty: f64,
        _fill_price: f64,
        _at: DateTime<Utc>,
        _session: &dyn crate::core::session::SessionRules,
    ) -> Result<()> {
        Ok(())
    }
    fn on_close_fill(
        &self,
        _book: &mut PositionBook,
        _leg: &CloseLeg,
        _filled_qty: f64,
        _filled_raw: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }
    fn rollover(&self, _book: &mut PositionBook, _new_day: crate::core::session::TradingDay) {}
}

#[test]
fn close_error_keeps_sub_position_active() {
    let (mut slot, _) = binance_slot();
    let at = t();
    open_position(
        &mut slot, &ids(), Side::Long, 30_000.0, 1.0, 29_000.0, at, "entry", SubIntent::Open,
    )
    .unwrap();
    slot.sub_positions[0].state = SubPositionState::Active;
    slot.last_bar_at = Some(at);
    let failing: Arc<dyn PositionRules> = Arc::new(FailingSplitRules);
    slot.parts.position = failing;

    let err = close_position(&mut slot, &ids(), 30_500.0, "exit", false, SubIntent::ExitRule);
    assert!(err.is_err(), "拆腿失败必须向上传播");
    assert_eq!(
        slot.sub_positions[0].state,
        SubPositionState::Active,
        "拆腿失败不得把子仓位卡在 CloseRequested（下一轮可重试）"
    );
}
```
顶部 `use` 补 `crate::core::position::{CloseLeg, CloseRequest, OrderDraft, PositionBook, PositionMode, PositionRules}`（`TradingDay`/`SessionRules` 用全路径）、`crate::core::session::SessionRules`（若全路径已够则不补）。

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p strategy-engine --lib xsec::dispatch::tests::close_error_keeps_sub_position_active -v`
Expected: FAIL，状态为 `CloseRequested`。

- [ ] **Step 3: 修复 `close_position` 顺序**

把“先推进 `CloseRequested` → `close_qty_inner(...)?` → 0 腿回滚”改为：
```rust
let qty = sub.qty;
let mut out = close_qty_inner(slot, ids, qty, price, at, is_stop_loss, intent)?;
if out.actions == 0 {
    out.rejected += 1;
    return Ok(out);
}
// 提交成功后推进：拆腿/下单 Err 时保持 Active，下一轮可重试。
if let Some(s) = slot.sub_positions.iter_mut().find(|s| {
    s.instrument == slot.parts.instrument && s.state == SubPositionState::Active
}) {
    if let Some(next) = advance_sub_state(s.state, &SubPositionEvent::CloseRequested) {
        s.state = next;
    }
}
Ok(out)
```
删除原回滚块；`sub` 的不可变借用只用于 `qty`（在 `close_qty_inner` 前结束）。

- [ ] **Step 4: 运行确认通过**

Run: `cargo test -p strategy-engine --lib xsec::dispatch::tests -v`
Expected: 全绿（既有 `close_emits_leg_ids_and_reduce_only` 仍断言成功后为 `CloseRequested`）。

- [ ] **Step 5: 提交**

```bash
git add crates/engine/src/xsec/dispatch.rs
git commit -m "fix: 平仓拆腿失败保持子仓位 Active 可重试" -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

### Task 7: 文档与示例配置口径

**Files:**
- Modify: `README.md`（run 用法补多组配置集）
- Modify: `CHANGELOG.md`（Added PR-6）
- Modify: `docs/RUNBOOK.md`（新增 §5.3 多组同进程运维）
- Modify: `config/xsec_momentum/strategy.yaml:2-3`（去掉“PR-5 实盘接入未实现”的过时注释）

**Interfaces:**
- Consumes: Task 1–4 的 CLI 行为与校验规则；Task 5–6 的缺陷修复。
- Produces: 运维可见的目录布局、拒启规则、共享账户/NAV 口径与排障读法。

- [ ] **Step 1: 写文档**

README 增一段（放在横截面 run 说明附近）：
- `strategy-rs run --config <set_dir>`：`<set_dir>` 无顶层 `strategy.yaml`，每个直接子目录一组。
- 目录示例：`set/_global.yaml`（共享 `services.trading`/`mode`/flush 策略）、`set/momentum_a/strategy.yaml`、`set/momentum_b/strategy.yaml`。
- 拒启规则：`strategy_id` 唯一、同 exchange/account/services/run_mode/flush/prune；live 组间 universe 不相交；paper 可重叠。
- 每组 NAV = 本地镜像（`nav_source=local_mirror`），同一 broker 账户保证金共享，不做跨组权益合并。

RUNBOOK §5.3：
1. 启动前置：`_global.yaml` 放配置集根目录（`global_for` 只向组目录及其父目录探测，组目录 = `set/<group>` 的父目录才是 set 根）。
2. live 启动自检输出：每组 `横截面 Actor 已启动 strategy_id=... instruments=...`；共享一条交易连接（同 `strategy_type` 订阅帧合并为 symbol 并集）。
3. 拒启对照：组间 universe 重叠（live）、`strategy_id` 重复、account/services/mode/flush 不一致。
4. 状态文件：共用 `out/state/<day>/` 三件套；`strategy_portfolio` 行 id = `strategy_id`；`sub_position` 行在 `payload_json` 内看 `strategy_id`/`instrument` 区分组。
5. panic：单组 Paused 一次性告警，其他组继续；恢复仍须整进程重启走 `recover(after_panic=true)`。
6. 关停：ctrl-c 逐组 `shutdown`，每组 8s 收口上限，最后 abort K 线 forwarder。

CHANGELOG `[Unreleased]` Added：
- 横截面多组同进程：配置集目录加载、共享交易客户端/Store/OrderRouter、订阅按 `(strategy_type, exchange)` 合并、按组 actor/NAV、live 组间 universe 不相交校验。
- Fixed：开仓拒单清理 `open_meta`；平仓拆腿失败保持 `Active` 可重试。

示例配置注释改为：
```yaml
# 用法：backtest 单组用 --config 本文件；run 多组用 --config <配置集根目录>
# （布局与拒启规则见 RUNBOOK §5.3）
```

- [ ] **Step 2: 校验文档**

Run: `cargo fmt --all -- --check`；`grep -n "PR-5 实盘接入未实现" config/xsec_momentum/strategy.yaml`。
Expected: fmt 干净、grep 无输出。

- [ ] **Step 3: 提交**

```bash
git add README.md CHANGELOG.md docs/RUNBOOK.md config/xsec_momentum/strategy.yaml
git commit -m "docs: 横截面多组同进程用法与运维口径" -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

## 收口验证（全部任务完成后）

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `git status` 只应出现本计划文件改动；`docs/design-package/01-需求分析.md` 与 `docs/design-package-old/` 保持未暂存、不提交。

Expected: fmt 干净；clippy 0 告警；workspace 全绿（套件数 ≥ 45，新增测试计入）；无远程推送。
</content>
