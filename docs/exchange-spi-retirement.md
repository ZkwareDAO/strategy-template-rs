# 旧链退役与删除收口方案（§13 收口序列 · 阶段 R/D）

<!-- refcheck:coords=off  账本文件：R/D 各条登记引用了已删实现的坐标（engine.rs、
     runtime_actor.rs、feed.rs、ctp_session.rs 等），属"当时测得"的历史事实，按
     D3 铁律只追加不改写，故不参与坐标存在性校验。执行登记 §5.1 的订正条目按
     "追加不改写"原则记录漂移本身。 -->

> 定位：§13 删除顺序后两段（**旧链退役 → 删除收口**）的执行方案，衔接
> `--engine spi` 缺省切换 PR 合入之后。推进前置 =
> `docs/exchange-spi-live-acceptance.md` 硬勾全绿 **且** 切换 PR
> （`main.rs` 两处 `default_value`）合入平稳。
> 本文只定方案与门，不授权执行；每一步开工前单独取得授权。
> 清单表引用面均为成文当日 grep 实据（行号可回溯）。

## 0. 三条铁律

1. **断言账本只增不减（口径修订 2026-11-09，详见 §6 第2项）**：删除任何旧链代码前，
   其钉住的不变量必须已在 SPI 侧有钉死案、或已冻结为 golden、或登记为退役并说明
   「无对应实现可测」。每个 PR 描述登记工作区测试案数（基线 531）；总数下降**不再
   自动判红**，但必须逐条解释到旧案名——**无解释的下降仍为红**，禁止「迁移时顺手删」。
2. **一步一 PR**：§5 各步独立可 revert。过门三步 =
   `cargo test --workspace --no-fail-fast` 全绿 +
   `cargo clippy --workspace --all-targets -- -D warnings` 0 +
   `cargo fmt --all --check`；含线程套件按 flaky SOP 追加同案 N× 重复。
3. **回退线随推进收缩**：R4 前 `--engine old` 仍是运行时秒级回退；R4 后回退
   = git revert + 重新部署（分钟级）。该代价见 §7.1，需运维确认后才开 R4。

## 1. 对象清单（引用面 = grep 实据）

动作三类：**删** / **保留** / **先迁**。行号指当前工作区。

| 对象 | 位置 | 现引用面（代码；文档注释不计） | 动作 |
|---|---|---|---|
| `RuntimeActor` internals（`spawn_supervised`/`FeedSource`/`drive_bar`/backfill 自愈） | `runtime_actor.rs`(224/349/563/575) | 仅 `bootstrap.rs:278` 旧分支、`live_orchestrator.rs` | 退役 R4 删 |
| runtime 共享类型（`RuntimeSpec/RuntimeHandle/RouteBind/RuntimeCommand/RuntimeFactory/PollWatch/OrderUpdateCmd/remote_matches_local`、`POLL_*` 常量） | `runtime_actor.rs` | `spi_live_actor.rs:71`、`bootstrap.rs:27/205-207` | **保留**，R1 先抽离 |
| `bootstrap.rs` `engine=="old"` 分支；`main.rs` backtest/run 两处 old 分支（`Engine::new` 调用点 440） | bootstrap 90–、main 346–/440 | CLI 面 | R4 删 |
| `Feed`/`FeedEvent`/`BarMergeMode` | `data/feed.rs`(26/341/358) | `engine.rs:31-32/111/196/854`（旧回测）、`runtime_actor.rs:27/349`（R4 后随 actor 死）、`config.rs:39-48/263/729/754`、`bootstrap.rs:439`、`live_orchestrator`/`ctp_backtest_e2e`/`backtest_paper_parity`/`spi_dual_run_parity` 旧侧 fixture | 收口 D1 删 |
| 旧回测链 `Engine` | `engine.rs` | `main.rs:440`（R4 后成无引用 pub 死码） | D1 整模块删 |
| `ctp_session`（`cst/is_ctp_supported/window_start/window_end`） | `data/ctp_session.rs` | `feed.rs`（随 D1 死）、`remote_client.rs:31`(cst)、`backtest_paper_parity.rs:41`(window_start) | 先迁 P3，D2 删 |
| `BacktestBroker`/`BacktestOrderClient` | broker | 两链共享 paper 内核（设计文档既定：保留） | **保留** |
| `mock` | broker | `trading_service_smoke` example 冒烟主家（既定：保留） | **保留** |
| `wire.rs` offset/fee、Store v2.9 行键、`PositionManager` | — | SPI 在用 | **保留** |

SPI 侧等价物已存在（退役不产生能力空洞）：`exchange-spi/src/core/session.rs`
配置驱动时段窗替代 `ctp_session` 硬编码表（ctp/config.rs:137 注记）；
`aggregation.rs:205` 多周期聚合等价旧 `Feed`；`bridge.rs:57/92` 仅以旧 `Feed`
为文档对齐锚。`SpiLiveSpec` 已明注「`runtime.bar_merge` 在 SPI 路径**不消费**」。

## 2. 前置动作（任何删除 PR 之前完成）

- **P1 golden 冻结**：新建 `crates/app/tests/golden/retirement/`，冻结
  `--engine old` 产物（机器生成，标注来源 commit）：回测权益/成交行样例、
  下单帧样例、mode 一览、五件套 CSV。素材可直接取在场验收 §9 证据包
  （`acc_old.log` + frames + state flat csv），不另造观测。
- **P2 `bar_merge` 过渡**：`config.rs` 三 struct 均 `serde(deny_unknown_fields)`
  （76/140/147）——**直接删字段会让存量 `CU.yaml:17 bar_merge: ctp` 解析硬错**。
  顺序：字段保留但标 deprecated（SPI 已不消费）→ 示例 yaml（CU.yaml/_global.example.yaml）
  删键 → README 注明；D1 才删字段与 `parse_bar_merge`。
- **P3 `ctp_session` 迁离**：`remote_client.rs:31` 的 `cst()` 改本模块定长
  offset 常量（仅时区换算，无窗口逻辑）；`backtest_paper_parity.rs:41` 的
  `window_start` 改走 exchange-spi session 配置驱动接口（正确性已由
  `ctp_live_frames_carry_offset_labels` 钉）。
- **P4 `.workbuddy-ai/` 处置**：提交前入 `.gitignore` 或清除，不与收口混面。

## 3. live_orchestrator 19 案迁移（R2）

形态：新建 `crates/app/tests/spi_live_orchestrator.rs`，同名加 `spi_` 前缀，
fixture `RuntimeSpec`→`SpiLiveSpec`，断言照搬**只加不减**。

| 处置 | 案（旧行号） | 依据 |
|---|---|---|
| **A. SPI 已有钉死案，ledger 对号**（6） | bar_drives(275)；startup_cross_reconcile(422)；supervised_restarts_after_panic(546)；lagged_receiver_backfill(927)；mixed_mode_coexist(1631)；futures_paper_integer_lots(356) | 分别对应 `spi_live_merge` 的 lifecycle(364)/restart_reconcile(529)/panic 钉/threaded_t7(749)/双实例装配/paper_ready_fill(708)。R2 仍以 SPI fixture 重跑一遍为实据——若跑出缺口，降级 B 类补案，不许口头「已覆盖」 |
| **B. SPI 侧重写**（12） | cooldown(662)、external_reader(728)、timed_flush(840)、slow_actor_no_stall(991)、watchdog_polls(1103)、watchdog_grace(1218)、position_check×4(1358/1413/1459/1518)、paper_survives_reconcile(1574)、live_paper_isolated(1777)；services_config_defaults_to_none(526) 移入 config 套件（`exchanges_config.rs`） | SPI 复用同一批原语（`PollWatch/remote_matches_local`，见 spi_live_actor.rs:71 import），行为应逐项等价；`slow_actor` 断言升级为**跨线程**不阻塞（SPI 每实例独立 OS 线程） |
| **C. 随链退役**（0） | — | 19 案全数要有落点，无「旧链特有不验」 |

过门：`spi_live_orchestrator` **18 案**绿 + `exchanges_config` 套件 +1 案，
合计落点 19/19；工作区案数 = 531+19（尚未删旧，账本登记）。

## 4. parity 测试收口（R3）

| 测试 | 对旧链依赖 | 处置 |
|---|---|---|
| `spi_dual_run_parity.rs`（966 行） | 新旧双跑对照 | 转 golden 回放：对 P1 冻结产物断言；对照段删除、断言段保留 |
| `spi_engine_cli_parity.rs`（456 行） | CLI 两分支各跑一遍 | 同上；`--engine old` 调用面随 R4 移除 |
| `backtest_paper_parity.rs` | 旧 `Engine` 做回测侧 | **保留命题**（回测 vs paper），实现改走 SPI 回测链；`window_start` 依赖先满足 P3 |
| `ctp_backtest_e2e.rs` | `BarMergeMode::Ctp` fixture | 改走 SPI 聚合配置；D1 前绿 |
| `live_orchestrator.rs` | 旧 actor 本体 | R4 随旧链删（其 19 案已由 §3 落点，账本不降） |

过门：**旧链还在**时完成全部 golden 化并全绿（双断言期）——才允许进 R4。

## 5. 执行序列（拓扑序，不许跳）

```
门 0：在场验收全绿 + 缺省切换 PR 合入平稳
R1  共享类型抽离 runtime_types.rs（纯移动，零行为改动；过门=531 全绿）
    —— 已执行（2026-11-06 工作区，未 commit）：531/39/0 + clippy 0 + fmt 0
    + spi_live_merge ×10 零 flake；match_tests 7 案随函数迁新家仍执行（路径
    runtime_types::match_tests::* 实跑可见）。门序偏差登记：在场验收未完成
    即开工（用户指令），R1 不删旧链、--engine old 基线位无损，风险=零。
R2  19 案迁移 spi_live_orchestrator.rs（新旧并存，531+19）
    —— 已执行（2026-11-06 工作区，未 commit）：新套件 18 案 +
    exchanges_config +1 案 = 落点 19/19；账本 550/40 套件/0 失败；
    ×10 重跑零 flake；clippy 0 + fmt 0。
    **A 类重跑抓出 5b 真缺口（本步最大价值）**：SPI 交叉对账原走
    on_order_update 状态机——已 Filled 信号无转移恒产不出 FillEvent，
    「信号有、仓位缺」补不回；旧链是直构 FillEvent 入账
    （runtime_actor.rs:785 同源账法）。已修 spi_live_actor.rs（改走
    intent+快照直构 → absorb_ready_fill 统一门 → process_fills），
    红案断言一字未弱化，改码追绿。
R3  parity golden 化（旧链仍运行；双断言期）
    —— 部分执行（2026-11-06 工作区，未 commit）：
    ① `spi_dual_run_parity` golden 化完成：9 个冻结件
      （`tests/golden/retirement/`，含 trade 行式/聚合桶/D1·日盘 H1 时刻
      对），双断言（即时==golden + SPI==golden）在场全绿；冻结入口
      `freeze_retirement_golden -- --ignored`。
    ② `spi_engine_cli_parity` golden 化完成：`cli_old_engine_rows.csv` +
      `cli_old_daily_last.txt`，同双断言形。
    ③ `ctp_backtest_e2e` 改线完成：新增 `--engine spi` e2e 案（同夹具双跑），
      SPI 报表面如实登记——backtest_signals.csv 属旧链 engine/signals 写家
      专属，SPI 分支不生成（5a Fill 派生口径），钉为反向断言；D3 需把该
      口径写进报表文档。
    ④ `backtest_paper_parity` **未动——设计分叉待拍板**：其回测侧对照源是
      `engine.positions().history()`（HistoryPosition 行），而 SPI 回测链
      按设计不持久化历史仓（SpiBacktest 仅 Store 权益 + Fill 派生交易行）。
      可选路：a) 测试内由 trade_rows 配对派生历史形（纯测试面，转换语义
      要逐案核对）；b) SpiBacktest 增加 history 导出（生产新功能，违背
      5a「不扩张」纪律，需单独立项）；c) 该套件改为「SPI paper vs SPI
      回测交易行」的新命题（保留 paper==backtest 语义，换对照坐标系）。
      倾向 a/c 组合，但这是命题重述，**不许执行 PR 现场拍板**。
    账本：551 = 531 + 19(R2) + 1(e2e SPI 案)；冻结案 2 个 #[ignore] 不计数。
R4  删旧 live 链：bootstrap old 分支 + runtime_actor.rs 整模块 +
    live_orchestrator.rs + main.rs 两处 old 分支
    （engine.rs/feed.rs 此时成为无引用 pub 死码，编译过门不报错）
D1  删旧回测链：engine.rs 整模块 + data/feed.rs（Feed/FeedEvent/BarMergeMode）
    + config.rs 字段与 parse_bar_merge + bootstrap.rs:439 默认值
    （前置=P2 已完成）
D2  删 ctp_session（前置=P3；feed.rs 内部调用已随 D1 消亡）
D3  口径收口：README/RUNBOOK.md §8 回退条目去 --engine old；设计文档 §13
    阶段全勾；aggregation/bridge 文档注释「对齐旧 Feed」改引 golden 文件；
    CHANGELOG 记退役
```

### 5.1 执行登记（2026-11-09 口径收口 · 全部数字为实测，非推算）

**状态刷新**（相对本文成文当日的 §5 表）：

| 步骤 | 登记状态 | 实测依据 |
|---|---|---|
| R1/R2/R3 | 本文原已登记（2026-11-06 工作区，**未 commit**） | — |
| **R4** 删旧 live 链 | **已执行（2026-11-09 工作区，未 commit）** | `crates/app/src/runtime_actor.rs` 不存在；`git status` 显示 `D`；`main.rs` `Commands` 仅 4 臂无 old 分支 |
| **D1** 删旧回测链 | **已执行完毕**（D1a 配置面 + D1b 实现面，2026-11-09） | `engine.rs` 已删；`data/feed.rs`（1027 行）已删、`lib.rs` `pub mod feed` 撤；`parse_bar_merge`/`SymbolSpec.bar_merge`/`RuntimeSpec.bar_merge`/`bootstrap.rs` 赋值全清，配置键保留 `Option<String>` 兼容位并告警 |
| **D2** 删 `ctp_session` | **已执行**（2026-11-09） | 文件已删、`pub mod` 撤；`cst()` 迁 `trading_calendar.rs`，`window_start` 消费点改 engine `sessions_on` 窗口起点（**口径变更**，见 §5.1 D1b/D2 段） |
| **D3** 口径收口 | **本登记即执行体** | RUNBOOK §4/§7、`app/src/lib.rs:7`、2 处悬空文档引用、本表；CHANGELOG 新建 |

**§6 终局验收判据实测**（不粉饰，逐条给数）：

- 零残留 grep：**未达成**。代码命中 `BarMergeMode` 57、`ctp_session` 19、
  `strategy_data::feed` 11、`FeedSource` 1（`engine/src/bridge.rs:7` 注释）、
  `spawn_supervised` 3（均为注释/doc-link 别名，`spi_live_actor.rs:31` 把该链接
  显式指向 `spawn_spi_live`，故非坏链）、`engine::Engine` 0。
- 账本：**531 passed / 39 套件 / 0 failed / 3 ignored**（`cargo test --workspace`，
  exit 0；ignored 含 `data/tests/zkware_live.rs:20,81` 等外部 live 依赖）。
  本文 §5 R2/R3 记的是 **551 = 531+19+1**；R4 删除旧 `live_orchestrator.rs`
  19 案后净回落至 531——满足 §6 第2项下限「删旧后仍 ≥531」，但**净增量为 0**。
  另：R3③ 声称新增的 +1 CTP SPI e2e 案在实测中**未单列**——
  `ctp_backtest_e2e.rs` 现为 1 案（`ctp_backtest_cli_end_to_end_cu:46`），
  SPI 双跑在该案体内。账本口径需二选一钉死：按 test 函数计（则 +1 不成立）
  或补独立案。本文不代拍。
- `--engine` 处置：§6 第3项要求「选项保留 + 值域只认 `spi` + `old` 显式报错」，
  **实际取"选项整体删除"**（`grep --engine crates/app/src/main.rs` → 0）。
  本文 §7 风险 2「选项删除会炸调用脚本」由此**成真**：调用脚本若带
  `--engine`，现得 clap unknown-arg 而非指路文案。
- clippy：**0 error 达成**（本收口修复 4 处：`engine/src/core/runtime.rs:22` 与
  `engine/src/core/types.rs:9` 两处 unused import；`app/src/main.rs:384`
  `needless_return`→尾表达式 `Ok(())`；`tests/spi_dual_run_parity.rs:943`
  `assert_golden_row_eq` 保留位加 `#[allow(dead_code)]` + 来源注释，
  **未删任何断言、未加 `#[ignore]`、未改阈值**）。
- ⚠️ 订正（2026-11-09 追加，不改写上条）：上条所引
  `tests/spi_dual_run_parity.rs:943` 是**写入时**行号，该文件现为 910 行
  （D1b/D2 删改后漂移）⇒ 现真值 `:883`。同型风险入册：`file:行` 坐标会因任意
  上方编辑静默作废，CI 三门（fmt/clippy/test）一个都抓不到它——已提议把引用
  校验脚本接进 CI 作第四步，待批。
  **追加（同日）**：该步已落地（commit `f2d6de0`，`scripts/check_doc_refs.py` +
  `ci.yml` 第四步），并已抓到 4 处新失效（含本文件上方所记 `:943` 同类漂移）；
  上句「待批」为提议当时的状态，保留原文。
- fmt：**已收口（追加订正）**。用户指令「先跑 `cargo fmt --all`，再拆 commit 提交」
  执行后，下述 62 处 diff 已全部格式化落库，现 `cargo fmt --all -- --check`
  **exit 0**（`714af21` 复验）；下条「仍红」为执行前的历史状态，按账本规则保留原文。
- fmt：**仍红，未在本次处理**。`cargo fmt --all -- --check` 报 **62 处** diff
  （engine 33 / app 26 / signals 2 / store 1），CI 第 1 步即失败；
  属机械改动但覆盖面大，留给独立 PR 决策（含是否只先格式化 `crates/engine`）。
- ⚠️ **文档债登记追加**：
  ① `docs/design.md`（登记时正文 716 行，加状态块后 747 行；README 头牌入口）已于
  2026-11-09 **登记为历史文档**（用户决策「design.md 登记为历史，不重写」）——
  正文（**不含**新加状态块）含已删实体 14 处实测（`RuntimeActor` 11 /
  `runtime_actor.rs` 2 / `drive_bar` 1；对全文 `grep -c` 会得 17，多出的 3 处正是
  状态块自身提及，测量范围必须随数字写明），均已随 R4/D1b/D2 退役删除。
  标题后追加状态块，**正文一字未改**；四处入口指针同步标注（README 两处、
  `docs/strategy/README.md`、`positions/src/history.rs:9` 注释）；其正文 15 处坐标
  实测仍可解析（0 失效），但已按历史文档关闭坐标校验（避免给不再维护的文档背上
  维护义务）。
  ② 同类未登记者：`docs/live-signal-position-flow.md`（211 行整篇旧链叙述，
  0 处坐标可修）、`docs/design-review.md` 的结论时效——**待断，未擅动**。
  ③ 追加（同日）：`docs/live-signal-position-flow.md` 已**同样登记为历史**（决策原文
  「live-signal-position-flow.md 同样登记为历史」）——211 行、含已删实体 8 处实测
  （`RuntimeActor` 6 / `runtime_actor.rs` 1 / `drive_bar` 1）、「终态」自称 3 处、
  0 处坐标故**不加** coords=off（没有可校验对象却关校验＝凭空造债务）、正文一字未改。
  其「不再有 FillEvent 通道」一句已逐条拆开标注：在"无跨 task fill 通道"意义上仍成立，
  但 `FillEvent` 类型存活（`app/src/spi_live_actor.rs:407`、`:767` 构造、`:472` 入账），
  字面易误读；§1「终态 task 拓扑 3N+2」**未核即按失效对待**，不替它背书。
  上条 ② 的未登记名单由此收缩为 `docs/design-review.md` 结论时效一项。

**D1a 已清（bar_merge 哑字段 · 2026-11-09）**：`SymbolSpec.bar_merge`、
`RuntimeSpec.bar_merge`、`parse_bar_merge`、`bootstrap.rs` 两处赋值全部移除；
`FileConfig:83`/`SymbolEntry:207` 保留 `Option<String>` 兼容位（两处
`deny_unknown_fields`，删键会让存量 yaml 硬错），改为 `warn_bar_merge_deprecated`
**告警一次**——原「写了不生效也不报错」的用户陷阱已消除。聚合真源为
`CtpExchange::aggregation_policy`（`ctp/mod.rs:219-221` → `SessionWindowPolicy`）
+ `contracts.yaml`；`config/example_dual_ma/CU.yaml` 与 `_global.example.yaml`
的示例声明同步去掉；RUNBOOK §7.1 已改为「已废弃字段，写了不生效」。

- 残差实测：`BarMergeMode` 57 → **32**（余量集中在待删的 `data/src/feed.rs` 本体
  与 parity 测试夹具）、`strategy_data::feed` 11 → **1**。
- 断言差账（就地登记，非静默）：`config.rs` 的
  `bar_merge_field_parsed_and_merged` 更名 `..._is_accepted_but_ignored` 并转为
  兼容断言（新增「任意值不 fail-fast」一条）；`backtest_paper_parity.rs` 原
  `spec.bar_merge == Ctp` 守卫取消（聚合真源已移 SPI，CU 的 CTP 形态仍由
  contract 透传断言与夹具保证）；`ctp_backtest_e2e.rs` 同条替换为
  `spec.timeframe == "1h"`。**案数不变**：531 passed / 39 套件 / 0 failed，
  clippy 0、fmt 0（均实测）。

**D1b/D2 已清（2026-11-09）**：`data/src/feed.rs`（1027 行，`Feed`/`FeedEvent`/
`BarMergeMode`）与 `data/src/ctp_session.rs` 已删除，`lib.rs` 两处 `pub mod` 同撤。

- 承接方式（旧链真值不丢）：`spi_dual_run_parity::ctp_aggregator_bucket_diff` 的
  「旧 Feed 运行时对照侧」退役，改为 engine 侧**同时**对照 `*_state` 与 `*_feed`
  两份冻结 golden（实测 `agg_night_h1_feed.csv` 与 `agg_night_h1_state.csv`
  **逐字节相同**；D1 只有旧链侧冻结件 `agg_d1_feed.csv` + `agg_d1_close_times.txt`，
  engine 侧直接钉在其上——期望值一字未改）；日盘 H1 分叉基线（差异表 #3）由
  `assert_ne!(golden_feed, golden_state)` + 新增 `assert_ne!(engine, 旧链 golden)`
  双重钉死，**不因删除旧实现而静默收敛**。
- 旧实现自带单测随实现同葬：`feed.rs` 18 案 + `ctp_session.rs` 6 案 = **24 案**。
  逐案归属如下（新案全部位于 `engine/src/core/aggregation.rs` 测试模块；
  「既有案」列指未新增、由现存断言承接）。
  **自我纠正**：`030e740` 的 commit message 与本文当时只登记 2 条退役案，漏了
  `d1_fixed_mode_still_uses_ctp_trading_day`——engine 侧 D1 口径由交易所规则决定
  （crypto 走 `UtcGridPolicy`、境内期货走 `SessionWindowPolicy`），「fixed 模式下
  D1 仍走 CTP 交易日」这一组合语义**不存在**，故本案属退役而非丢失。现补记为 3 条。

| # | 旧案（宿主） | 去向 | 承接位置 |
|---|---|---|---|
| 1 | `preload_populates_all_timeframes` (feed) | 新案 | `feed_populates_all_timeframes_and_reports_newly_closed` |
| 2 | `realtime_close_fires_newly_closed` (feed) | 新案 | 同上（合并为一案，M1 即时收线逐根断言） |
| 3 | `tainted_head_bucket_never_enters_closed` (feed) | 既有案+新案 | `aligned_first_bucket_kept_mid_start_dropped` + `d1_head_bucket_dropped_when_ingested_mid_trading_day` |
| 4 | `max_len_trim_keeps_complete_buckets` (feed) | 新案 | `max_len_trim_keeps_only_complete_buckets` |
| 5 | `d1_fixed_mode_still_uses_ctp_trading_day` (feed) | **退役** | 无对应实现（见上「自我纠正」） |
| 6 | `d1_tainted_partial_head_dropped` (feed) | 新案 | `d1_head_bucket_dropped_when_ingested_mid_trading_day` |
| 7 | `d1_night_session_ending_23h_merges_into_next_trading_day` (feed) | 新案 | `weekend_and_night_bars_roll_into_next_trading_day` |
| 8 | `d1_uses_ctp_trading_day_and_closes_at_day_session_close` (feed) | 新案+golden | `d1_spans_from_trading_day_night_open_to_day_close` + `spi_dual_run_parity` 的 `agg_d1_feed.csv`/`agg_d1_close_times.txt` |
| 9 | `d1_keeps_weekend_night_session_with_next_monday` (feed) | 新案 | `weekend_and_night_bars_roll_into_next_trading_day` |
| 10 | `eight_hour_buckets` (feed) | 新案 | `ctp_4h_and_8h_buckets_anchor_within_windows` |
| 11 | `repeated_preload_same_window_is_idempotent` (feed) | 新案+parity | `watermark_rejects_repeated_and_overlapping_bars` + parity ⑤ |
| 12 | `overlapping_preload_only_ingests_new_candles` (feed) | 新案 | `watermark_rejects_repeated_and_overlapping_bars` |
| 13 | `ctp_1h_night_and_day_windows` (feed) | 新案 | `h1_night_buckets_anchor_from_21`（本次补）+ `session_window_never_spans_lunch_break` |
| 14 | `ctp_4h_two_bars_per_trading_day` (feed) | 新案 | `h4_bucket_spans_night_and_truncates_at_break`（本次补）+ `ctp_4h_and_8h_…` |
| 15 | `ctp_tainted_partial_head_dropped` (feed) | 新案 | `d1_head_bucket_dropped_when_ingested_mid_trading_day` |
| 16 | `ctp_unsupported_tf_falls_back_to_fixed` (feed) | **退役** | `SessionWindowPolicy::supports` 恒真，二值回落语义不存在 |
| 17 | `holiday_d1_uses_calendar_not_wallclock` (feed) | 新案 | `holiday_table_suppresses_windows_and_rolls_trading_day` |
| 18 | `out_of_coverage_falls_back_to_heuristic` (feed) | **退役** | 改为注入 `WindowedSession.holidays`，无覆盖外启发式 |
| 19 | `is_ctp_supported_only_h1_h2_h4` (ctp_session) | **退役** | 同上（`supports` 恒真） |
| 20 | `h1_night_windows_anchored_at_21` (ctp_session) | 新案 | `h1_night_buckets_anchor_from_21`（本次补；与旧表**逐点等价**，含跨 0 点与 01:00 收盘外） |
| 21 | `h2_windows` (ctp_session) | 新案 | `h2_buckets_anchor_within_windows`（本次补；夜盘三点等价，日盘 11:29 为**有意分叉**：旧 09:00 跨午休 → engine 10:30 不跨窗） |
| 22 | `h4_windows` (ctp_session) | 新案 | `h4_bucket_spans_night_and_truncates_at_break`（本次补；夜盘「跨日一根」等价，日盘被午休截断） |
| 23 | `h1_day_windows_across_break` (ctp_session) | 既有案（本次补标注） | `session_window_never_spans_lunch_break` |
| 24 | `window_end_cases` (ctp_session) | 既有案（本次补标注） | `session_window_never_spans_lunch_break` + `ctp_4h_and_8h_…` 的 `window_end` 断言 |

  **账本算式**：`030e740` 时点 515 = 531 − 24 + 8；本片 +3 案（20/21/22 号）→
  **531 − 24 + 11 = 518**（实测 `cargo test --workspace`）。
  净差值 −13 全部来自 3 条退役案与 10 条「多对一/由既有断言承接」的合并，
  无一条为凑数而写：三条新案的期望值均为按 engine 语义独立推导、实测一次通过。
  未加 `#[ignore]`、未改任何阈值、未删任何既有断言。
  §6 判据原文（「账本终值 ≥ 531」）**未改动**——是否修订该判据由断事人决定。

- **口径变更（不是等价替换，须知情）**：`backtest_paper_parity.rs` 的
  `post_gap_h1_closures` 原用旧 `window_start(60, t)`（返回 **H1 段首**，夜盘
  22:30→22:00），现改为 engine `cn_futures_default().sessions_on(..)` 的
  **窗口起点**（→21:00）。该辅助函数的文档命题本就是「所属 CTP **窗口**的首根」，
  新实现与文档一致、与旧实现不同。`backtest_paper_parity` 5 案原样通过，
  但**不宣称逐时刻等价**——判据集合本身变了。
- `cst()`（UTC+8）承接：迁至 `data/src/trading_calendar.rs`（其唯一生产需求方），
  `remote_client.rs` 改从该模块引用；app 侧不留引用。
- 注释残留同步清理（5 处历史引用改为「旧实现/已退役」表述）：
  `engine/core/aggregation.rs`、`engine/core/session.rs`×2、
  `engine/exchanges/ctp/mod.rs`、`engine/bridge.rs`（`FeedSource` 提法）、
  `spi_dual_run_parity.rs` 差异表 #3、`spi_live_actor.rs`×3（旧监督器名与其
  doc-link 别名）。

**§6 零残留判据复跑（2026-11-09 D1b/D2 后）**：`BarMergeMode` **0**、
`ctp_session` **0**、`strategy_data::feed` **0**、`FeedSource` **0**、
`engine::Engine` **0**、`spawn_supervised` **0**——六项全部达成。
§6 其余两项仍未满足：`--engine` 处置与本文 §6 原文要求不一致（见上），
以及「在场验收全绿」前置 20 项 0 勾。§6 判据文本按历史口径保留不改写，
现实差异以本注记为准。

**门序偏差（重申并升级为现状描述）**：本文 `:158-159` 铁律「在场验收未完成前，
一切步骤不得开工」与 `:92-93` 已登记的「在场验收未完成即开工（用户指令）」
并存；`exchange-spi-live-acceptance.md` 前置 20 项**实测 0 勾**，而 R4/D1 已执行。
- **0 勾归因订正（追加）**：主因不是「外部服务不可达」，而是**该验收文档自身
  不可执行**——实测三族失效：① 10 处 `--engine` 命令行被 clap 拒
  （`error: unexpected argument '--engine' found`）；② 10 处日志锚串前缀已由
  `SPI ` 改为 `engine `（`spi_live_actor.rs`），原锚逐条 grep 命中 0；
  ③ 锚 3「`SPI live 预热完成…`」在代码中**从未存在**（只有失败变体 :1092），
  ——**此判断后经复核为误**：该锚只是 `SPI `→`engine ` 改名，现串
  `engine live 预热完成（本地 CSV + 远程回填；聚合只推进不驱动策略）` 实存于
  `app/src/spi_live_actor.rs:1083`。误因：按"同一行含 `info!`"过滤 grep，而宏起
  :1016、字面量在 :1024。原文按账本规则保留，此处追加更正；acceptance 表第 3 行已回归真锚。
  **锚存在性检查必须按字面量本体 grep**，这条已写进 acceptance 表格与脚本说明。
  且 A1 基线仍写 531（现行 518）、B2/D3/F1/§2.1 要求与**已删旧链**当场对照。
  文档已订正为 v1.1（命令/锚/方法三族；20 硬勾不删不降级）。外部服务不可达降为
  **次要**原因。本文 §7 风险 2 早已登记「调用脚本带 `--engine` 会得 unknown-arg」
  会成真——预言中了，只是没人去撞那堵墙，直到 2026-11-09 实测才暴露。
- **断言账本第二次变动登记（追加，数字为门禁实测非推算）**：`BarDecoder` 退役删除
  （`ab92793`）使 engine `#[test]` 50→49、全仓 **517 passed / 0 failed / 3 ignored**
  （`cargo test --all` exit 0）。消失的唯一一条是
  `core::bar::tests::infer_finality_boundary_semantics`——它钉的正是被删的
  `infer_finality` 三分支，代码不存在后无对象可钉；未加 `#[ignore]`、未改阈值、
  未写替代实现凑绿。相邻不变量继续承重且实存：`engine/tests/cross_exchange.rs:313`
  （非 Final 绝不驱动策略）、`engine/src/core/replay.rs:93`（断开/日界注入）；
  `cross_exchange.rs` 另删的是一个 `fn decoder()` trait 实现（端口方法移除后的
  必需同步），不是断言。**等式更新：517 = 531 − 24 + 11 − 1。**
  §6 第 2 项的可追溯判据据此重算：解释为"3 案退役 + 10 案多对一 + 1 案随端口删除"。
后果不是"违规"二字可结——**回退线已消失**：`--engine old` 不存在，`git revert`
成为唯一手段。此项按 §7 风险 1 属"运维事前确认"范畴，**至今没有运维签字记录**。

**悬空引用修复**：`runtime_types.rs:2` 与 `spi_live_orchestrator.rs:5` 原引
`docs/engine-retirement.md`（该文件不存在），已改指本文。

R4 与 D1 之间保留一段净主干观察窗（建议 ≥1 周实盘/仿真在 `--engine spi` 上跑），
与 §0 铁律 3 的运维确认配套。

## 6. 终局验收（收口 PR 合并的判据）

- [ ] 零残留：`grep -rn "spawn_supervised\|FeedSource\|BarMergeMode\|ctp_session\|strategy_data::feed\|engine::Engine" crates/` 代码命中 0（golden、CHANGELOG、本文与 archive 除外）。
- [ ] 账本**可追溯**：被删旧链代码所钉的每条不变量，必须有明确归属——SPI 侧新案 /
      既有断言 / 已冻结 golden / 登记退役（须说明「无对应实现可测」），逐条列表见
      §5.1 归属表；**归属无空洞即过门**。**案数**降为告警值并持续登记（基线 531 →
      现 518 = 531 − 24 旧案 + 11 新案；净 −13 全部来自 3 条登记退役案与 10 条
      多对一合并案），任何一步使总数下降仍须在 PR 描述里逐条解释到旧案名。
      其余不变：全绿 + clippy 0 + fmt + 线程案 N× 重跑零 flake。
      > **判据修订理由（断事人拍板，2026-11-09）**：原文「账本终值 ≥ 531」把
      > **旧实现的测试函数数量**当成了尺子。D1b/D2 删掉的 24 案钉的是
      > `data::Feed` 与旧时段表的内部形状——实现既已删除，这些案数既不可能也不
      > 应该被原样保留。继续以 531 为硬门，量的是旧实现的轮廓而不是当前行为覆盖，
      > 且会诱导「为凑数写弱断言」——那比放宽断言更隐蔽的失真。尺子改为
      > 「不变量可追溯 + 案数下降逐条可解释」，**铁律 1 的实质约束不降级**，
      > 只把它一直想保护的东西（覆盖）换成直接度量。
- [ ] 引擎入口唯一：`backtest`/`run` 的唯一主循环 = `ExchangeRunner::on_event`
      （live 同码），CLI 不再有 `--engine` 开关。现实取「**选项整体删除**」而非原拟
      「保留 + `old` 显式报错」——可复核实据：`main.rs` 的 `Commands` 为 4 臂、
      `--engine` 作为 **CLI 参数**在 `crates/app/src` 命中 0（曾有一处过期注释位于
      `spi_backtest.rs`，已随本修订改写为「参数已退役」说明）。判据随之改写为两条可同时验收的
      约束：① 入口唯一性可 grep 复核；② **回退代价已被知情**——运行时秒级回退
      （`--engine old`）不存在，回退 = `git revert` + 重新部署（分钟级，见 §7.1）。
      ② **仍需运维签字才能勾**，签字记录至今不存在，故本项保持未勾，
      不作为已完成事项宣称。
      > **判据修订理由（断事人拍板，2026-11-09）**：原条文的「保留选项 + `old` 报错」
      > 是给调用脚本留的兼容缓冲；R4/D1 执行时选择了整体删除，缓冲已不复存在。
      > 与其留一条永远勾不上的判据自欺，不如把它改写成当前真实约束——
      > 唯一入口 + 回退代价知情，并把「运维未签字」这件事继续摆在明面上。
- [ ] 三原则终检：协议统一面在 `core/`、市场个性在 `exchanges/`、信号与仓位单内核（`PositionManager`）——grep 复核无第二实现残留。

## 7. 风险登记（执行前逐项对齐）

1. **R4 移除 `--engine old` 运行时回退线**：运维需事前确认（预案 = git revert
   上一 commit + 部署）。运维若不接受，停在 R3（旧链留死码）——但 §6 零残留
   即无法达成；此决策记在此处，**不许在执行 PR 里现场拍板**。
2. `--engine` 选项删除会炸调用脚本：本方案取「选项保留 + `old` 值显式报错」，
   报错文案指向本文，比 clap unknown-arg 友好。
3. `spi_dual_run_parity` 966 行 golden 化是单步最大工作量（估 1–2 人日）；
   golden 来源 commit 必须与 P1 冻结一致，否则断言漂移无从归因。
4. `mock`/`BacktestBroker` 去留 = 设计文档既定「保留」，本方案不重开该口径。
5. 在场验收未完成前，本文一切步骤**不得开工**——R1 虽零行为改动，其过门证据
   （双链并存全绿）依赖验收窗口的 `--engine old` 基线位。
6. **端口"已实现"与"已接线"是两件事**（2026-11-09 实测登记）：`BarDecoder`、
   `SubscriptionPlan` 订阅集、`ContinuousSeriesBuilder` 三项有 trait、有实现、有单测，
   但生产路径不消费（调用点证据见 `docs/design-package/03-详细设计.md` §3.10）。
   风险不在代码，在**叙述**：一旦在验收/交付口径里被称"已具备能力"，就是在制造
   新型假实据。处置：本文与 design-package 一律记为"实现就位、主路未消费"，
   验收清单单列一项不计入已交付。
   **同日进展（追加，不改写上述登记）**：决策「先接订阅面，BarDecoder 一并退役删掉」
   落地——①订阅集**已接入主路**（`spi_live_actor.rs` 以 `runner.subscriptions()` 的
   topic 驱动 `attach` 与 `load_warmup`；非 1m 在装配期 fail-fast，因 `run_generation`
   无错误通道；多合约仅取首条 + `warn!`），**但仍不含"自动换月"**：Roll 生产端所需的
   合约快照宇宙在生产侧恒空（`SourceRequest.universe`，`spi_live_actor.rs:261`），
   `ContractSnapshot` 构造者全在测试里，此缺口如实留着，不以假数据凑可用；
   ②`BarDecoder` 按本口径**退役删除**（不保留永不执行的实现充当架构完整性）；
   ③`ContinuousSeriesBuilder` 仍未接线，保持"不计入已交付"。
7. **风险 5 的"不得开工"前置在实际执行中被越过**（既成事实登记，不追溯改写上述原文）：
   R1–R4 / D1–D3 已执行完毕，而 §6 现场验收仍 0/20、回退线已随删除消失。
   后果：现网缺陷只能靠 revert 提交处置（且旧链代码需一并恢复），
   本文 §6 第 3 项「回退代价知情」**至今没有运维签字**。
   此条的意义在于**下一批工作不得再以此为先例**：先验后删的顺序仍是纪律，
   只是这次已经破了一次，破了要留名，不能靠口径抹平。
8. **配置 fail-fast 只覆盖一层，另一层是静默降级**（2026-11-09 实测裁定，登记为缺陷）：
   `services:` **整块缺失**确实 fail-fast（`app/src/main.rs:505-506`，实测 `exit=1`
   + 明确文案）；但 `services:` 在而**子块缺**（`trading:` 或 `kline:`）时，两个 struct
   都带 `#[serde(default)]` 与占位 `Default` impl（`app/src/config.rs:188-200`、
   `:159-170`），缺失即静默取占位值。实测后果：`run --mode live` + 无 `trading:` 子块
   ⇒ 进程正常启动、打印「mode 一览 … → live」与「订单网关已启动」，**全程无任何缺配置
   告警/错误**，而 `submit_enabled` 被静默置 `false` 并原样交给网关
   （`crates/app/src/bootstrap.rs:124`）⇒ **永不下单的 live 实例，零信号**。
   这与 D1a 消灭的 `bar_merge`（写了不生效、静默哑字段）同属一类，只是方向相反：
   那次是"配了没用"，这次是"没配就当默认跑"。
   **处置口径待定**（不属引用订正，未擅改代码）：候选 ①`live` 模式下 `trading` 子块
   缺席即启动失败；②缺席时打 `error!` 明示"将以 submit_enabled=false 运行"；
   ③把 `services.trading` 从 `#[serde(default)]` 收紧为必填。三者都需要区分
   「未提供」与「显式为 false」，故涉及 `Option` 形态改动，非纯文案。
   **已处置（同日追加，用户选 1+2）**：`app/src/main.rs` 启动期新增校验——live 命中
   占位交易端点 ⇒ `bail!` 拒绝启动（文案点名 `services.trading` 并指路熔断写法）；
   非 live ⇒ `tracing::warn!` 明示「不会产生任何真实下单」。判据
   `TradingServiceCfg::is_unconfigured` 只比端点是否等于 `PLACEHOLDER_TRADING_WS_URL`
   （该常量与 `Default` 由专档测试锁死同源），**刻意不看 `submit_enabled`**，
   故 §2.1 的熔断（真实端点 + `false`）不被误杀。候选 ③（改必填）未采纳：
   会波及存量 yaml 解析面，且 `deny_unknown_fields` 结构下 Option 化的改动面
   大于收益。钉死案 8 个：`tests/run_config_guards.rs` 4 个 CLI 级（层一退出码、
   层二 live 拒绝、层二 paper 放行且出声、熔断不被误杀）+ `config.rs` 内 4 个单元级；
   CLI 级全部在 `TempDir` 内以 `current_dir` 运行，规避上条记载的 cwd 陷阱。
   **账本：525 passed / 0 failed / 3 ignored / 40 套件**（= 517 + 8，只增不减）。
8b. **运行时文案的零残留补记（同日追加，不改写上条）**：`app/src/runtime_types.rs`
   三处 `anyhow!("RuntimeActor 已关闭")` 已收敛为 helper `channel_closed()`
   （`live actor 命令通道已关闭（<OrderUpdate|Reconcile|Shutdown> 未投递）`），
   R4 删除的组件名不再出现在运行时输出；新增 4 案跑真投递失败路径钉住
   （含"不得再出现已删组件名"）。**账本：529 passed / 0 failed / 3 ignored**（= 525 + 4）。
   同批回测实测（BTCUSDT 4h，2024-01-01..04-01）：bars=131040 / fills=27 /
   cash 5000→6939.69，产物与标签口径正常；顺带暴露一个**既有**的目录模式合并陷阱
   （`--symbol` 继承同目录 `CU.yaml` 的 `exchange: SHFE` ⇒ 行键错位报"无 1m 数据"），
   非本轮引入、未擅改代码，已写进 RUNBOOK §5/§7.1 待拍。
   附带复现陷阱（已写进 01/02）：全局回落按 **cwd** 找 `config/_global.yaml`，
   在仓库根内跑任何"缺配置"实验都会被真实配置静默掩盖——我第一轮就测错了这个。
8c. **回测报表占位值与回撤口径（同日复检 8b 的实跑批次，不改写上条）**：重跑同一组
   参数得同一组数值（bars=131040 / fills=27 / cash_final=6939.6933 ⇒ 引擎记账未变），
   但**同一份产物内部互相打脸**：`equity 7,363.10 ≠ cash 6,939.69` 而
   `position_count` 硬编码 `0`、`backtest_report.txt` 的「可用现金」印的是权益副本、
   「持仓保证金」印死 `0.00`。逐日核对判定**权益序列是对的**（净量 0.103217 ×
   (末 1m 收盘 71,363.0 − 开仓 67,260.9) = 423.41 恰等 equity−cash；03-29 亦 272.77），
   错的是报表占位值 ⇒ 现由成交流水推导（`report.rs` 私有 `end_positions`）。
   同批另一处为**同名不同义**：`backtest_analysis_report.md` 沿用 `analyzer.py` 的
   全局峰谷差却标作「最大回撤」，本例谷(2024-01-04)早于峰(2024-03-04) ⇒
   报 35.17% 而滚动口径为 7.43%（风险夸大约 4.7 倍，且据此误判风险档）。
   现两值分列正名（滚动回撤 / 权益振幅），背离时出提示，风险判据取滚动回撤；
   **这是有意偏离 Python 参考实现的标签**，数值仍全量保留。
   账本可解释：与退役面无关的报表缺陷，但既在同批实跑中发现即入账。**账本：
   533 passed / 0 failed / 3 ignored**（= 529 + 4，只增不减；4 案名见 CHANGELOG）。
   测试面教训（登记为可复用约束）：`report.rs` 测试构造器 `day()` 把 cash 与 equity
   写死相等，使"现金类科目"永远无法被证伪——新增 `day_cash()` 分离两者；
   以后凡断言报表数字的测试，**构造数据必须让相邻科目取不同值**，否则断言是空的。
9. **目录模式跨文件身份污染根治（2026-11-10，用户「接着根治目录模式合并陷阱」）**：
   8b 记的那次实跑之所以报"区间无 1m 数据"，根因在 `load_dir`：各 yaml 顶层字段按
   "字母序首个非空者胜"提升为**目录级**默认，而 flat 文件的条目身份取文件名 stem
   （`example.yaml` ⇒ `EXAMPLE`），`--symbol BTCUSDT` 因此匹配不到自己的条目、整体回落
   到兄弟文件 `CU.yaml` 的 `exchange: SHFE` ⇒ 行键 `SHFE/crypto_perpetual/BTCUSDT`。
   处置四条：**R1** 身份类字段（strategy/timeframe/cash/initial_cash/leverage/exchange/
   account）退出目录级赢家清单，只认「声明它的文件 / `_global.yaml` / 内置默认」，
   跨文件共享仅保留基础设施类（services/contracts/exchanges/instruments/risk +
   废弃的 bar_merge 告警）——`mode` 早就是这个规则（U1/U5），本次是推广而非新发明；
   **R2** 目录模式要求名实相符，请求无文件的标的直接拒绝并列出可用清单（靠新增的
   `FileConfig::from_dir` 来源标记限定作用域；单文件模式无污染源，模板复用照旧，
   故原钉案 `run_cli_symbols_override_config_symbols` 一字未改）；**R3** 新增顶层
   `symbol:` 作显式身份（与文件名不一致告警；`_global.yaml`/`contracts.yaml` 写它硬错）；
   **R4** 仓库样例改按 `{SYMBOL}.yaml` 约定命名（`git mv`，两处 `example.yaml` →
   `BTCUSDT.yaml`），样例名实不符本就是这坑的一半，引用面 6 处文档同步。
   另修 `main.rs` 两处：找不到数据时报错带出行键与期望 CSV 路径（身份错位不再伪装成
   数据缺失）、杠杆回落从目录级改取已解析的 `spec.leverage`。
   实跑对钉：目录模式 `--config config/example_dual_ma --symbol BTCUSDT` ⇒
   `bars=131040 / fills=27 / cash_final=6939.6933`，与单文件模式逐位一致（**同一份数据、
   两条路径同结果**才是根治的证据，只断言"不报错"不够）。
   账本：**540 passed / 0 failed / 3 ignored**（= 533 + 7）。两条旧案
   （`per_symbol_global_fields_win_over_global_file`、`default_path_falls_back_to_config_dir`）
   原断言的正是被移除的污染机制，已**改写为在正确的层断言同一优先级 + 目录级不得被污染**，
   未删除、未放宽。遗留：目录内多文件声明同一标的仅告警不硬拒（仓库顶层 `config/`
   递归收集各策略子目录，硬拒会把"目录递归"与"每策略一目录"绑死）——待拍。

---
*维护锚*：本文行号引用为成文当日工作区状态；每个 R/D PR 合入后，对应行标
「已完成」并附 commit 号，引用面变化不回填旧行（账本性质，不改写历史）。
