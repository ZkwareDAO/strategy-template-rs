# 统一策略目录（横截面迁入 strategy-strategies）实施计划

<!-- refcheck:coords=off  历史实施计划（已完成）：坐标随后续重构漂移，不再参与 CI 坐标校验。 -->

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把横截面策略源码迁入 `crates/strategies/src/xsec/`，删除独立 `strategy-xsec-strategies` crate，补一个可运行的 `xsec_template` 与开发指南，使单体、横截面、单体起步扩展多标的三种策略都在同一个 crate 内开发。

**Architecture:** 旧 `Strategy` 实现仍在 `src/*.rs` 顶层，由冻结的 `build.rs` 自动注册；横截面 `CrossSectionalStrategyPort` 放 `src/xsec/` 子目录，由 `src/xsec/mod.rs` 手写 registry（build.rs 只扫顶层文件，不改）。app 的 `make_xsec_strategy` 改为引用 `strategy_strategies::xsec`。模板以 universe=1 起步，配置加标的即横截面；旧 `Strategy` 仍只经 `SingleInstrumentAdapter` 跑 N=1。

**Tech Stack:** Rust 1.98.1（`rust-toolchain.toml` 钉死）、cargo workspace、`strategy-engine`/`strategy-core`/`strategy-indicators`、serde/serde_json/serde_yaml、anyhow。

**Spec:** 用户 2026-11-11 指令（所有策略放 `crates/strategies/src`，单体/横截面/单体变多体都行）；`deliverables/gstack/feature-dev-cross-sectional-strategy-2026-11-09.md` §5.1/§5.2/G6；`README.md` 横截面节。

## 背景：两个接口的区别（结论同步写入指南）

| 维度 | `Strategy`（旧，`src/*.rs` 顶层） | `CrossSectionalStrategyPort`（横截面，`src/xsec/`） |
|---|---|---|
| 输入 | `Ctx`：单标的已闭合序列、当前价、该标的仓位 | 一次拿到全 universe 的 `InstrumentSnapshot` + `PortfolioContext`（NAV/回撤/子仓/乘数） |
| 调用 | 空仓→`on_candle`；持仓→`on_realtime_tick` | 每个 base TF 决策轮→`on_cross_section_bars` 一次 |
| 输出 | 单标的 `Action`（Open/Close/Reverse/UpdateStop/UpdateExit/Hold） | 多标的 `InstrumentAction`（含 `Adjust{target_qty}` 调仓） |
| 能力 | 无法跨标的排名/分配，无组合视图 | 排名、轮换、目标权重、组合级决策 |
| N 能力 | 仅 N=1（经 `SingleInstrumentAdapter`） | N=1 与 N>1 同一份代码；加 universe 行即升级 |
| 注册 | `build.rs` 自动发现 + `register_strategy!` | `src/xsec/mod.rs` 的 `all()`/`create()` 各登记一行 |
| 单标的起步 | 仅 N=1，加标的不会升级 | universe=1 走模板单标路径；加第二行自动切横截面路径 |

**新策略默认入口：** 无论先做单标的还是直接横截面，新策略统一从 `src/xsec/template.rs` 起步；旧 `Strategy` 只用于维护既有策略。

**关键边界：** 旧 `Strategy` 不能靠加 universe 行升级——`make_xsec_strategy` 只在 `instruments.len()==1 && is_legacy_registered` 时走 Adapter（`crates/app/src/xsec_config.rs:517`）；N≥2 只查横截面 registry。需要“单体起步→加代币变多体”的策略必须实现 `CrossSectionalStrategyPort`。旧策略的多实例需求仍按规格 §5.7 用 N 个单实例进程，不是本计划的交付。

## Global Constraints

- 本地提交，不远程推送；禁止 `git add -A`；不触碰 `docs/design-package/`、`docs/design-package-old/`。
- commit 格式「类型: 简述」；末尾加 `Co-Authored-By: Claude Code <anthropic@anthropic.com>`。
- G5 行为零改动：旧 `Strategy` 路径、旧配置回测逐字节不变；`crates/strategies/build.rs` 与 `src/*.rs` 顶层旧策略文件不动。
- 横截面策略只放 `crates/strategies/src/xsec/`；新增策略只改 `src/xsec/mod.rs` 的 `all()`/`create()`，不碰 `src/lib.rs`（本次一次性接线除外）。
- 注释只写原因（禁 emoji/占位代码）；中文注释/commit。
- 全量验证：`cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 全绿。
- 测试只用回测 CSV/mock，不产生真实订单。

## Review Focus

1. 迁移后旧 N=1 路径仍走 Adapter、行为不变——`xsec_config_guards` 的 `n1_legacy_strategy_can_be_wrapped_by_adapter` 与 `xsec_set_*` 用例必须原样通过。
2. 旧/新 registry 同名策略的优先级不得改变——N=1 且名字在旧 registry 时仍优先 Adapter；`make_xsec_strategy` 的现有顺序与错误文案保持。
3. 单标路径：N=1 且配置无 `rebalance` 时仍每轮决策并开仓；持仓时不反复 Adjust——`single_instrument_opens_without_rebalance_due` / `single_instrument_holds_existing_position` 覆盖。
4. 横截面路径：N>1 才受 `rebalance_due` 门控；stale/entry_eligible/特征缺失不产生动作——Task 2 单测覆盖。
5. 加第二行代币后同一模板自动切横截面路径，配置补齐 `rebalance`——Task 3 双配置 smoke 覆盖。
6. 删除独立 crate 后无悬挂引用、无依赖环、错误文案仍列出横截面可用策略——Task 1 测试与 Task 5 grep/`cargo check` 覆盖。

---

### Task 1: 迁移横截面策略到 `crates/strategies/src/xsec/`

**Files:**
- Create: `crates/strategies/tests/xsec_registry.rs`
- Move: `crates/xsec-strategies/src/momentum.rs` → `crates/strategies/src/xsec/momentum.rs`
- Move: `crates/xsec-strategies/src/lib.rs` → `crates/strategies/src/xsec/mod.rs`
- Modify: `crates/strategies/src/lib.rs`（加 `pub mod xsec;`，更新「模板冻结区」注释）
- Modify: `crates/strategies/Cargo.toml`（加 `strategy-engine`）
- Modify: `crates/app/src/xsec_config.rs:514,528`
- Modify: `crates/app/tests/xsec_backtest_e2e.rs:558`
- Modify: `crates/app/Cargo.toml`（删 `strategy-xsec-strategies`）
- Modify: `Cargo.toml`（members 与 workspace.dependencies 删 `strategy-xsec-strategies`）
- Delete: `crates/xsec-strategies/Cargo.toml`（`git rm` 后 `rmdir` 空目录）

**Interfaces:**
- Consumes: `strategy_engine::xsec::port::{CrossSectionalStrategyPort, SignalIdNamespace}`
- Produces:
  - `strategy_strategies::xsec::all() -> &'static [&'static str]`
  - `strategy_strategies::xsec::create(name: &str, params: Option<&serde_json::Value>) -> anyhow::Result<Box<dyn CrossSectionalStrategyPort>>`
  - `strategy_strategies::xsec::momentum::XsecMomentum`（模块公开，保持原路径语义）

- [ ] **Step 1: 写失败的 registry 测试**

```rust
// crates/strategies/tests/xsec_registry.rs
use strategy_engine::xsec::port::SignalIdNamespace;

#[test]
fn xsec_registry_reachable_from_strategy_crate() {
    let names = strategy_strategies::xsec::all();
    assert!(names.contains(&"xsec_momentum"), "{names:?}");
    let strategy = strategy_strategies::xsec::create("xsec_momentum", None).unwrap();
    assert_eq!(strategy.signal_id_namespace(), SignalIdNamespace::Sub);
}

#[test]
fn unknown_xsec_strategy_lists_registered_names() {
    let err = strategy_strategies::xsec::create("nope", None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("xsec_momentum"), "{err}");
}
```

- [ ] **Step 2: 运行测试确认 RED**

Run: `cargo test -p strategy-strategies --test xsec_registry`
Expected: 编译失败，`unresolved import strategy_strategies::xsec`（模块尚未存在）。

- [ ] **Step 3: 移动源码并接线**

```bash
mkdir -p crates/strategies/src/xsec
git mv crates/xsec-strategies/src/momentum.rs crates/strategies/src/xsec/momentum.rs
git mv crates/xsec-strategies/src/lib.rs crates/strategies/src/xsec/mod.rs
git rm crates/xsec-strategies/Cargo.toml
rmdir crates/xsec-strategies/src crates/xsec-strategies
```

- [ ] **Step 4: 更新注册路径与依赖**

1. `crates/strategies/Cargo.toml` 增加：
   ```toml
   strategy-engine = { workspace = true }
   ```
2. `crates/strategies/src/lib.rs` 在 `pub mod registry;` 后加 `pub mod xsec;`，并把「模板冻结区」注释改为：旧策略仍由 build.rs 自动注册；横截面策略在 `src/xsec/` 的手写 registry 登记。
3. `crates/strategies/src/xsec/mod.rs` 顶部文档改为「本 crate 的横截面策略子模块」；`pub mod momentum;` 与 `create` 的 `momentum::XsecMomentum` 保持不变。
4. `crates/app/src/xsec_config.rs`：`:514` 注释与 `:528` 改为 `strategy_strategies::xsec::create(name, spec.config.params.as_ref())`。
5. `crates/app/tests/xsec_backtest_e2e.rs:558` 改为 `strategy_strategies::xsec::create(...)`。
6. `crates/app/Cargo.toml` 删除 `strategy-xsec-strategies = { workspace = true }`；根 `Cargo.toml` 删除 member 与 workspace dependency 两行。

- [ ] **Step 5: 运行定向测试确认 GREEN**

Run: `cargo test -p strategy-strategies --test xsec_registry`
Expected: 2 passed。

Run: `cargo test -p strategy-strategies`
Expected: 旧 registry 用例 + momentum 单测全部通过。

Run: `cargo test -p strategy-app --test xsec_backtest_e2e`
Expected: 全部通过（行为不变）。

Run: `cargo test -p strategy-app --test xsec_config_guards`
Expected: 全部通过（N=1 Adapter 与配置集守卫不变）。

- [ ] **Step 6: 提交**

```bash
git add crates/strategies/src/xsec crates/strategies/src/lib.rs crates/strategies/Cargo.toml \
  crates/strategies/tests/xsec_registry.rs crates/app/src/xsec_config.rs \
  crates/app/tests/xsec_backtest_e2e.rs crates/app/Cargo.toml Cargo.toml Cargo.lock
git commit -m "refactor: 横截面策略迁入 strategy-strategies 并移除独立 crate" \
  -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

### Task 2: 新增 `xsec_template` 策略骨架（单标/横截面双路径）

**Files:**
- Create: `crates/strategies/src/xsec/template.rs`
- Modify: `crates/strategies/src/xsec/mod.rs`（`pub mod template;`、`all()`、`create()`）

**Interfaces:**
- Produces:
  - `#[derive(Debug, Clone, serde::Deserialize)]` 的 `TemplateParams { rank_indicator: String, higher_is_better: bool, top_k: usize }`；实现 `Default`（`"macd"` / `true` / `3`），`#[serde(default, deny_unknown_fields)]`
  - `XsecTemplate::new(TemplateParams) -> Self`
  - `XsecTemplate::from_params(Option<&serde_json::Value>) -> anyhow::Result<Self>`（走 `crate::registry::params_or_default`）
  - `XsecTemplate` 实现 `CrossSectionalStrategyPort`

- [ ] **Step 1: 写失败测试与最小 stub**

在 `template.rs` 写 `TemplateParams`、`XsecTemplate`，`on_cross_section_bars` 先返回 `Vec::new()`。测试 fixture 直接复制 `xsec/momentum.rs` 测试模块的 `inst/bar/ctx/sub/opens/adjusts`，`strategy(top_k)` 换成 `template(top_k)`，并加：

```rust
fn template(top_k: usize) -> XsecTemplate {
    XsecTemplate::new(TemplateParams {
        rank_indicator: "macd".into(),
        higher_is_better: true,
        top_k,
    })
}

fn template_with(top_k: usize, higher_is_better: bool) -> XsecTemplate {
    XsecTemplate::new(TemplateParams {
        rank_indicator: "macd".into(),
        higher_is_better,
        top_k,
    })
}

fn key(symbol: &str) -> String {
    format!("binance/crypto_perpetual/{symbol}")
}

fn snap_feature(
    symbol: &str,
    indicator: &str,
    value: Option<f64>,
    stale: bool,
    eligible: bool,
) -> InstrumentSnapshot {
    let mut features = FeatureMap::default();
    if let Some(v) = value {
        features.values.insert(indicator.into(), v);
    }
    InstrumentSnapshot {
        instrument: inst(symbol),
        bar: bar(symbol),
        position: None,
        stale,
        features,
        entry_eligible: eligible,
        entry_reason: None,
        exit_triggered: None,
        trading_day: TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
        no_trade: false,
        entry_confirm: true,
        signal_resume: false,
        cooldown: None,
        session_open_price: None,
        closed: None,
    }
}
```

```rust
#[test]
fn single_instrument_opens_without_rebalance_due() {
    let snaps = vec![snap("SOLUSDT", Some(0.5), false, true)];
    let mut s = template(1);
    let acts = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), false));
    assert_eq!(opens(&acts), vec![key("SOLUSDT")]);
    assert!(adjusts(&acts).is_empty(), "单标路径不做权重调仓");
}

#[test]
fn single_instrument_holds_existing_position() {
    let snaps = vec![snap("SOLUSDT", Some(0.5), false, true)];
    let mut s = template(1);
    let acts = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("SOLUSDT", 2.0)], false));
    assert!(opens(&acts).is_empty() && adjusts(&acts).is_empty());
}

#[test]
fn single_instrument_ineligible_does_not_open() {
    let snaps = vec![snap("SOLUSDT", Some(0.5), false, false)];
    let mut s = template(1);
    let acts = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), false));
    assert!(acts.is_empty(), "entry_eligible=false 不得开仓");
}

#[test]
fn cross_section_ranks_by_feature_and_selects_top_k_only_when_due() {
    let snaps = vec![
        snap("BTCUSDT", Some(0.5), false, true),
        snap("ETHUSDT", Some(0.2), false, true),
        snap("SOLUSDT", Some(-0.1), false, true),
    ];
    let mut s = template(2);
    let due = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true));
    assert_eq!(opens(&due), vec![key("BTCUSDT"), key("ETHUSDT")]);
    let not_due = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), false));
    assert!(not_due.is_empty(), "N>1 且 rebalance_due=false 不决策");
}

#[test]
fn lower_is_better_inverts_ranking() {
    let snaps = vec![
        snap("BTCUSDT", Some(0.5), false, true),
        snap("ETHUSDT", Some(0.2), false, true),
        snap("SOLUSDT", Some(-0.1), false, true),
    ];
    let mut s = template_with(1, false);
    let acts = s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true));
    assert_eq!(
        opens(&acts),
        vec!["binance/crypto_perpetual/SOLUSDT".to_string()]
    );
}

#[test]
fn rank_indicator_param_selects_named_feature() {
    let snaps = vec![
        snap_feature("BTCUSDT", "rsi", Some(70.0), false, true),
        snap_feature("ETHUSDT", "rsi", Some(30.0), false, true),
    ];
    let mut s = XsecTemplate::new(TemplateParams {
        rank_indicator: "rsi".into(),
        higher_is_better: true,
        top_k: 1,
    });
    assert_eq!(
        opens(&s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true))),
        vec!["binance/crypto_perpetual/BTCUSDT".to_string()]
    );
}

#[test]
fn stale_or_ineligible_snapshots_are_excluded() {
    let snaps = vec![
        snap("BTCUSDT", Some(0.9), true, true),
        snap("ETHUSDT", Some(0.5), false, false),
        snap("SOLUSDT", Some(0.1), false, true),
    ];
    let mut s = template(1);
    assert_eq!(
        opens(&s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true))),
        vec!["binance/crypto_perpetual/SOLUSDT".to_string()]
    );
}

#[test]
fn existing_position_adjusts_to_equal_weight_target() {
    let snaps = vec![
        snap("BTCUSDT", Some(0.5), false, true),
        snap("ETHUSDT", Some(0.2), false, true),
    ];
    let mut s = template(2); // weight 0.5；target = 0.5*1000/(100*1) = 5.0
    let acts = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("BTCUSDT", 0.5)], true));
    assert_eq!(adjusts(&acts), vec![(key("BTCUSDT"), 5.0)]);
    assert_eq!(opens(&acts), vec![key("ETHUSDT")], "ETH 无仓且在前 2 → 开仓");
    let ok = s.on_cross_section_bars(
        &[],
        &snaps,
        &ctx(vec![sub("BTCUSDT", 5.0), sub("ETHUSDT", 5.0)], true),
    );
    assert!(opens(&ok).is_empty() && adjusts(&ok).is_empty());
}

#[test]
fn held_non_candidate_is_adjusted_to_zero() {
    let snaps = vec![
        snap("BTCUSDT", Some(0.1), false, true),
        snap("ETHUSDT", Some(0.9), false, true),
    ];
    let mut s = template(1);
    let acts = s.on_cross_section_bars(&[], &snaps, &ctx(vec![sub("BTCUSDT", 2.0)], true));
    assert_eq!(
        adjusts(&acts),
        vec![("binance/crypto_perpetual/BTCUSDT".to_string(), 0.0)]
    );
    assert_eq!(opens(&acts), vec!["binance/crypto_perpetual/ETHUSDT".to_string()]);
}

#[test]
fn missing_rank_feature_excludes_instrument() {
    let snaps = vec![
        snap("BTCUSDT", None, false, true),
        snap("ETHUSDT", Some(0.1), false, true),
    ];
    let mut s = template(1);
    assert_eq!(
        opens(&s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true))),
        vec!["binance/crypto_perpetual/ETHUSDT".to_string()],
        "缺失特征不得按 0 参与排名"
    );
}

#[test]
fn rank_feature_absent_everywhere_yields_no_actions() {
    let snaps = vec![
        snap("BTCUSDT", None, false, true),
        snap("ETHUSDT", None, false, true),
    ];
    let mut s = template(1);
    assert!(
        s.on_cross_section_bars(&[], &snaps, &ctx(Vec::new(), true))
            .is_empty(),
        "全市场缺特征（如 params 指标未在 indicators 声明）不得交易"
    );
}

#[test]
fn from_params_rejects_unknown_keys() {
    let bad = serde_json::json!({ "topk": 2 });
    assert!(XsecTemplate::from_params(Some(&bad)).is_err());
    assert_eq!(XsecTemplate::from_params(None).unwrap().params.top_k, 3);
}
```

- [ ] **Step 2: 运行测试确认 RED**

Run: `cargo test -p strategy-strategies xsec::template`
Expected: `single_instrument_opens_without_rebalance_due` 等断言失败（stub 返回空动作），不是编译错误。

- [ ] **Step 3: 实现最小行为**

`on_cross_section_bars` 按以下顺序实现：
1. **单标路径**：`snapshots.len() == 1` 时立即处理，不看 `rebalance_due`。若 `!stale && entry_eligible` 且 `active_sub` 为 None → `Open { instrument, side: Long, price: bar.close, stop_price: price*0.98, atr: 0.0, strength: 1.0, reason: "xsec_template: single entry".into() }`；已有子仓或不满足条件 → 空。该路径不做权重 `Adjust`，因此 N=1 配置可省略 `rebalance`。
2. **横截面路径**：`snapshots.len() > 1` 且 `!portfolio.rebalance_due` → 空。
3. 过滤 `!stale && entry_eligible` 且 `features.number(&params.rank_indicator)` 存在；按数值 `higher_is_better` 决定升降序，同值按 `instrument.namespaced_key()` 升序。
4. `top_k = params.top_k.max(1)`；`weight = 1.0 / top_k as f64`；keep = 前 top_k 的 key 集合。
5. 逐快照：用 `active_sub`（`state != SubPositionState::Closed`）取子仓。在 keep 中：无子仓 → `Open { instrument, side: Long, price: bar.close, stop_price: price*0.98, atr: 0.0, strength: 1.0, reason: format!("xsec_template: rank in top_{top_k}") }`；有子仓 → `target = weight * portfolio.nav / (price * mult)`，`mult = multipliers.get(&key).unwrap_or(&1.0)`，当前带符号数量与 target 不一致（同向且 `|target-current| <= 1e-9 * current.max(1.0)` 视为达标）→ `Adjust { instrument, target_qty: target, reason: format!("xsec_template: rebalance weight {weight:.4}") }`。
6. 不在 keep 且子仓 `qty > 1e-12` → `Adjust { target_qty: 0.0, reason: "xsec_template: dropped out of top_k".into() }`。
7. 模板不产生 `Close`/`Reverse`：单标的退出由配置 `exit`（引擎 L2 自动 Close）与 `portfolio_exit` 处理；空头与 `bottom_k` 不在本模板范围（引擎校验仍要求 `bottom_k: 0`）。

- [ ] **Step 4: 注册并跑测试**

`src/xsec/mod.rs`：加 `pub mod template;`；`all()` 改为 `&["xsec_momentum", "xsec_template"]`；`create` 加 `"xsec_template" => Ok(Box::new(template::XsecTemplate::from_params(params)?))`。

Run: `cargo test -p strategy-strategies`
Expected: 全部通过（含新模板 12 个用例）。

- [ ] **Step 5: 提交**

```bash
git add crates/strategies/src/xsec/template.rs crates/strategies/src/xsec/mod.rs
git commit -m "feat: 新增横截面策略模板 xsec_template" \
  -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

### Task 3: 示例配置与装配验收

**Files:**
- Create: `config/xsec_template/strategy.yaml`（N=2 横截面）
- Create: `config/xsec_template_single/strategy.yaml`（N=1 单标的，无 `rebalance`）
- Modify: `crates/app/tests/xsec_config_guards.rs`（新增用例，使用全限定路径，无需改 import）

**Interfaces:**
- Consumes: `strategy_app::xsec_config::{load_xsec, make_xsec_strategy}`
- Produces: 两份可直接回测的示例配置，`strategy_type: xsec_template`；strategy_id 分别为 `xsec_template:default`（横截面）与 `xsec_template:single`（单标的）。

- [ ] **Step 1: 写失败的配置验收测试**

```rust
#[test]
fn xsec_template_sample_config_loads_and_instantiates() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../config/xsec_template/strategy.yaml");
    let spec = load_xsec(&path).expect("示例配置必须可加载");
    assert_eq!(spec.strategy_type, "xsec_template");
    assert_eq!(spec.strategy_id, "xsec_template:default");
    assert_eq!(spec.instruments.len(), 2);
    let strategy = make_xsec_strategy(&spec).expect("模板必须已注册");
    assert_eq!(
        strategy.signal_id_namespace(),
        strategy_engine::xsec::port::SignalIdNamespace::Sub
    );
}

#[test]
fn xsec_template_single_config_loads_without_rebalance() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../config/xsec_template_single/strategy.yaml");
    let spec = load_xsec(&path).expect("单标配置必须可加载");
    assert_eq!(spec.strategy_type, "xsec_template");
    assert_eq!(spec.strategy_id, "xsec_template:single");
    assert_eq!(spec.instruments.len(), 1);
    assert!(spec.config.rebalance.is_none(), "N=1 可不配 rebalance");
    let strategy = make_xsec_strategy(&spec).expect("模板必须已注册");
    assert_eq!(
        strategy.signal_id_namespace(),
        strategy_engine::xsec::port::SignalIdNamespace::Sub
    );
}
```

Run: `cargo test -p strategy-app --test xsec_config_guards xsec_template`
Expected: FAIL，两份配置文件均不存在（RED）。

- [ ] **Step 2: 创建两份示例配置**

`config/xsec_template/strategy.yaml`（SOLUSDT/BNBUSDT 在 `data/klines/1m/` 有现成 CSV）：

```yaml
strategy_type: xsec_template
strategy_id: xsec_template:default
universe:
  - SOLUSDT
  - BNBUSDT
timeframes: [15m, 1h]
indicators:
  macd: { tf: 1h, fast: 12, slow: 26, signal: 9 }
entry:
  default: []
exit:
  default: []
portfolio_exit:
  stop_loss_pct: 8
rebalance: { every_bars: 96, top_k: 2, bottom_k: 0, weight_scheme: equal }
risk: { max_total_notional: 0.80 }
sizing: { per_position_pct: 0.10 }
params: { rank_indicator: macd, higher_is_better: true, top_k: 2 }
```

`config/xsec_template_single/strategy.yaml`（N=1 单标的；无 `rebalance`，模板走单标路径；加 `BNBUSDT` 与 `rebalance` 即变横截面）：

```yaml
strategy_type: xsec_template
strategy_id: xsec_template:single
universe:
  - SOLUSDT
timeframes: [15m, 1h]
indicators:
  macd: { tf: 1h, fast: 12, slow: 26, signal: 9 }
entry:
  default: []
exit:
  default: []
portfolio_exit:
  stop_loss_pct: 8
risk: { max_total_notional: 0.80 }
sizing: { per_position_pct: 0.10 }
params: { rank_indicator: macd, higher_is_better: true, top_k: 1 }
```

- [ ] **Step 3: 跑通装配与 smoke 回测**

Run: `cargo test -p strategy-app --test xsec_config_guards xsec_template`
Expected: 两个用例 PASS。

Run: `cargo run -p strategy-app -- backtest --config config/xsec_template_single/strategy.yaml --start 20260901 --end 20260915 --data-dir ./data/klines`
Expected: 输出 `横截面回测完成: ...`，随后一行 `fills=` > 0；证明 N=1 且无 `rebalance` 时单标路径可交易。

Run: `cargo run -p strategy-app -- backtest --config config/xsec_template/strategy.yaml --start 20260901 --end 20260915 --data-dir ./data/klines`
Expected: 输出 `横截面回测完成: ...`，随后一行 `fills=` > 0；证明加第二行代币后同一模板走横截面路径。若 fills=0，先检查 warmup/数据是否覆盖区间，不得改模板来“凑成交”。

- [ ] **Step 4: 提交**

```bash
git add config/xsec_template/strategy.yaml config/xsec_template_single/strategy.yaml \
  crates/app/tests/xsec_config_guards.rs
git commit -m "feat: xsec_template 单标/横截面示例配置与装配验收" \
  -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

### Task 4: 横截面开发指南与入口链接

**Files:**
- Create: `docs/strategy/XSEC_GUIDE.md`
- Modify: `docs/strategy/README.md`（阅读顺序插入一条）
- Modify: `README.md`（横截面节加指南链接，修正「加一行」表述）

**Interfaces:** 无代码接口；文档必须与 Task 1-3 的实际路径/函数名一致。

- [ ] **Step 1: 写指南**

`docs/strategy/XSEC_GUIDE.md` 固定包含以下章节与要点：
1. **两种接口的区别与选择**：复制本计划「背景」表；新策略（单标或横截面）统一从 `xsec_template` 起步，旧 `Strategy` 只用于维护既有策略；写明旧接口仅 N=1 Adapter、不能加 universe 升级。
2. **目录布局与注册**：`src/*.rs` 旧策略自动注册；`src/xsec/*.rs` 横截面策略在 `src/xsec/mod.rs` 的 `all()`/`create()` 各加一行；`src/lib.rs` 不再为新增策略改动；新增策略名不得与旧 registry 重名（N=1 时会被 `make_xsec_strategy` 优先判为旧策略）。
3. **从零写策略（单标/横截面双路径）**：复制 `src/xsec/template.rs`；`snapshots.len()==1` 分支是单标路径（不看 `rebalance_due`，`entry_eligible` 开仓、持仓持有、退出交给配置 `exit`/`portfolio_exit`），`>1` 分支是横截面路径（`rebalance_due` 时排名/等权 `Adjust`）；只做单标可删掉横截面分支，只做横截面可保留单标分支以支持 universe=1；改类型名/`TemplateParams` 默认值、登记名字、建 `config/<name>/strategy.yaml`。
4. **输入契约**：`InstrumentSnapshot` 字段（`bar/features/position/stale/entry_eligible/closed`）与 `PortfolioContext`（`nav/sub_positions/multipliers/rebalance_due`）；`snapshot.closed` 是全部已配置周期（`CandleSeries`）的已闭合序列视图，只读。
5. **动作语义**：`Open/Close/Reverse/Adjust/UpdateStop/UpdateExit/Hold`；`Adjust` 的 `target_qty` 为带符号净数量；引擎保证 `stale`/`entry_eligible`/`rebalance_due`/quorum/1:1 开仓约束。
6. **配置字段速查**：`universe`、`timeframes[0]`=base TF、`indicators` 白名单（adx/rsi/ema/sma/atr/macd）、`entry`/`exit`、`portfolio_exit`、`rebalance`（N=1 可省略并走单标路径，N≥2 必填）、`risk/sizing`、`params`；`params.top_k` 是横截面路径实际选股数（单标路径不使用），`rebalance.top_k` 参与配置校验，二者保持同值；`params.rank_indicator` 必须与 `indicators` 中声明的键一致，否则所有候选因特征缺失被过滤而完全不交易。
7. **单体起步 → 加代币变多体**：先 `universe` 写 1 个标的、可省略 `rebalance`（运行 `config/xsec_template_single/strategy.yaml`，模板走单标路径）；加第二行代币并补 `rebalance`（N≥2 必填，`top_k` ≤ universe 数、与 `params.top_k` 同值）即横截面，策略代码一行不改；示例命令对照两份配置。
8. **测试与运行**：`cargo test -p strategy-strategies`、单标/横截面两条示例回测命令、`run --mode paper_trading/live` 与 `docs/RUNBOOK.md` §5 链接。
9. **本期边界**：`bottom_k` 必须 0、`weight_scheme` 仅 `equal`、指标白名单固定、单组单 exchange/单 account、`multi_instance` 拒启、NAV=local_mirror、KlineCache 降级预热。
10. **提交前清单**：参数 `deny_unknown_fields`、保留 `closed` 只读、不做 `now()`、单测覆盖单标无 `rebalance`/横截面 due/特征缺失/调仓、跑 workspace 测试。

- [ ] **Step 2: 更新入口链接与表述**

1. `docs/strategy/README.md` 阅读顺序在 QUICKSTART 后插入：
   `2. [XSEC_GUIDE.md](XSEC_GUIDE.md) — 单标/横截面策略接口、模板与从单标的扩到多标的`
   后续条目编号顺延。
2. `README.md` 横截面节：把「单标的策略是 N=1 特例：`universe` 加一行代币即升级为横截面，无需改代码」改为：
   「横截面 port 策略从 universe=1 起步：`universe` 加一行代币即升级为横截面，无需改代码；旧单标 `Strategy` 仍只经 Adapter 跑 N=1。」
   并在 schema 条目前加：
   `- **开发指南**：[docs/strategy/XSEC_GUIDE.md](docs/strategy/XSEC_GUIDE.md)（模板 `crates/strategies/src/xsec/template.rs`、示例配置 `config/xsec_template_single/strategy.yaml` 与 `config/xsec_template/strategy.yaml`）。`

- [ ] **Step 3: 校验链接与事实**

Run: `grep -n "XSEC_GUIDE" README.md docs/strategy/README.md`
Expected: 两处命中。

Run: `grep -n "crates/strategies/src/xsec/template.rs\|config/xsec_template_single/strategy.yaml\|config/xsec_template/strategy.yaml" docs/strategy/XSEC_GUIDE.md`
Expected: 三处命中。

- [ ] **Step 4: 提交**

```bash
git add docs/strategy/XSEC_GUIDE.md docs/strategy/README.md README.md
git commit -m "docs: 单标/横截面策略开发指南与模板入口" \
  -m "Co-Authored-By: Claude Code <anthropic@anthropic.com>"
```

---

### Task 5: 全量验证与收尾

**Files:** 无新增；仅验证与必要修正。

- [ ] **Step 1: 检查无悬挂引用**

Run: `grep -rn "strategy_xsec_strategies\|strategy-xsec-strategies" crates config README.md docs/strategy Cargo.toml`
Expected: 0 命中（扫描范围不含历史计划与用户分析产物）。

- [ ] **Step 2: 格式与 lint**

Run: `cargo fmt --check`
Expected: exit 0，无 diff。

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: exit 0，0 warning。

- [ ] **Step 3: 全量测试**

Run: `cargo test --workspace`
Expected: 全部套件通过，0 failed；新用例包含 `xsec_registry`、`xsec::template`、`xsec_template_sample_config_loads_and_instantiates`、`xsec_template_single_config_loads_without_rebalance`。

- [ ] **Step 4: 工作树核对**

Run: `git status --short`
Expected: 只有本计划涉及的文件；`docs/design-package/01-需求分析.md` 仍是用户本地修改、不得暂存/提交。若有 lint/test 修正，按「fix:」或「test:」单独提交。

---

## 执行前检查

- 计划文件自身位于 `docs/superpowers/plans/2026-11-11-unify-strategy-crate.md`，执行前如需按仓库惯例提交，单独 `docs(plan): 统一策略目录实施计划`，不要混入 Task 1 的 refactor commit。
- 本计划不实现 `multi_instance` / 旧策略 N>1 自动升级；这两项仍是规格 §5.7 的推迟项。
