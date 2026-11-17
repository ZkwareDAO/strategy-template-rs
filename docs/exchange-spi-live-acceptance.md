# SPI Live 在场验收 RUNBOOK（§14-3 · 统一引擎主循环的在场验证）

<!-- refcheck:coords=off anchors=off  本文件是在场验收账本：坐标与日志锚全部取自
     已随 R4 退役删除的 `spi_live_actor.rs`（现行宿主为 `strategy_live_actor.rs`，
     锚串已改名），代码本体不在仓库，CI 无从校验，故关坐标与锚校验。账本内容与
     未勾项一字不删，未完成项保持红。 -->

> 适用：**R4/D1 之后**的真服务在场验收——`--engine` CLI 参数已删除、旧引擎与
> `RuntimeActor` 已退役，统一 `ExchangeRunner` 主循环是**唯一路径**。
> 执行者：持交易账户与外部服务部署权限的人。
>
> ⚠️ **订正（v1.0→v1.1，2026-11-09）**：本文原版以「`--engine old` 缺省路 +
> 双链 A/B 对照」为前提，实测已不可执行，三族失效：
> ① **命令面**：10 处 `--engine` 命令行，clap 现报
> `error: unexpected argument '--engine' found`（实测），照原文连进程都起不来；
> ② **锚串面**：日志前缀已由 `SPI ` 改为 `engine `（见 `spi_live_actor.rs`），
> 原 10 处锚对不上代码；锚 3 亦属**同一批前缀改名**（现串见下表第 3 行） <!-- refcheck:ignore -->
> （只有失败变体 `engine live 预热 preload 失败`，:1032）；
> ③ **方法面**：B2/D3/F1 与 §2.1 要求「与旧链当场对照」，旧链代码已删除，
> 当场 A/B 不可能。
> **这不是"没人去跑"，是"跑不起来"**——`docs/exchange-spi-retirement.md` 与
> `CHANGELOG.md` 此前把 20 项 0 勾归因为「外部服务不可达」，该归因不完整，已一并订正。
> 本次订正**只改命令、锚串与失效方法的陈述**：20 个硬勾一项不删、不降级、不预判为绿；
> 依赖旧链对照的项标「方法失效 + 现有替代证据」，在重定义前保持红。
> 本文每个断言点都是**当场可 grep / 可核对的锚**；锚串直接取自代码
> （`crates/app/src/spi_live_actor.rs`、`crates/app/src/bootstrap.rs`），
> 对不上就是缺陷，禁止改锚迁就。

## 0. 边界：本文只验「进程内证明不了的东西」

| 已被进程内钉死（不必在场重复证明） | 只有在场才能证明（本文职责） |
|---|---|
| 编排主循环与回测同段（`spi_live_merge` 6/6 + 32 路并发压测） | trading-service 服务端**接受 `offset` 字段并回传 `fee`**（v2.7 协议升级落地） |
| T7 路由 / retry 同键幂等 / recover 对账 / 重启快照恢复 | 真柜台风控语义：ReduceOnly 拒单、今昨仓、交易日历日切换 |
| 出帧形状（size=0 order_cash / 数量字面量 raw 全链） | 真实 WS 时序：断线重连风暴、回报乱序、迟到 ack |
| 仓位内核两链同码（`PositionManager`，原则 3） | 行情真实缺口下的 warmup/backfill 自愈 |
| 镜像账本今昨双链同口径（入册按回报 `event_time`） | 服务端侧成交与本地五件套的**终局一致性** |

**运行纪律**：
- **回退线**：⚠️ `--engine old` 已随 R4 删除，**运行期无秒级回退**；唯一退路是
  `git revert` 到 `c18e6e4` 之前并重新编译（分钟级）。该代价登记于
  `docs/exchange-spi-retirement.md` §6 第 3 项「回退代价知情」，
  **运维签字至今不存在**；本文勾绿与否都不改变这一条。
- 紧急熔断：`services.trading.submit_enabled: false`（所有 live 不发单，RUNBOOK.md §2.1）。
- 记录纪律：每勾一项记「命令 + 锚串实际输出 + 时间戳 + 证据文件路径」。
  失败项**不得弱化断言关闭**——登记进差异表，查明根因后才允许勾绿。

## 1. 预检门（无外部服务依赖，可在验收前一天完成）

```bash
# 1.1 模板侧门禁基线（预期：39 套件 / 518 passed / 0 failed；
#     518 = 531 − 24 + 11，账本等式与逐案归属见 docs/exchange-spi-retirement.md §5.1。
#     原写 531 为 D1b/D2 清退前基线，已随旧实现自带单测同葬）
cargo test --workspace --no-fail-fast
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings

# 1.2 CLI 唯一路径实印（订正：原要求实印 [default: old]，该参数已不存在）
cargo run -q -p strategy-app -- run --help | grep -- "--engine" \
  && echo "异常：CLI 仍有 --engine" || echo "确认：--engine 已随 R4 删除，统一循环为唯一路径"

# 1.3 配置 lint（逐 symbol 核对）
#   crypto：exchange/account/mode/leverage/cash；
#   CTP   ：contracts.yaml 有该品种条目（bar_merge 已废弃——不要写，写了不生效
#           且加载时告警，见 retirement §5.1 D1）；
#           klines-service 已退役（2026-11-10）：不再有 services.kline 节，
#           K 线走 trading-service 订阅（契约 §8）；旧节写了 run 即拒启

# 1.4 服务端协议版本确认（关键前置，v2.7）：
#   trading-service 版本必须已支持入帧 `offset` 字段与回帧 `fee` 字段。
#   未升级服务端下 SPI 帧带 offset 会被忽略或拒——行为口径先与服务端拉通。
```

- [ ] 勾 A1：门禁基线复现（518/39/0，日志留档）
- [ ] 勾 A2：CLI 唯一路径实印——`run --help` 中 `--engine` **不存在**（1.2 的
      「确认」分支输出留档；这同时是「回退线已消失」的在场证据）
- [ ] 勾 A3：服务端 offset/fee 支持确认（服务端版本号或联调窗口承诺，留文字证据）

## 2. 冒烟位：本地模拟服务在场（xili-exchange + trading-service）

外部服务拉起步骤照 `docs/RUNBOOK.md` §2（xili 9200 → trading-service 南向 9100）。
本节是**纸面风险的第一次在场**，真账户资金为 0。

```bash
# 2.1 基线对照（订正：旧链代码已随 R4/D1 删除，无法当场跑 old 取帧形）
#     可得的替代证据，均为既存冻结件，不据此宣称逐时刻等价：
#       - 双跑套件 crates/app/tests/spi_dual_run_parity.rs（旧链 TradeRow golden 对）
#       - crates/app/tests/golden/retirement/（11 件，逐件有引用，含旧链 D1/H1 冻结）
#     在场只验 §0 表格右列「进程内证明不了的东西」。
cargo run -p strategy-app -- run --config config/<dir> \
  --output-dir data/accept_spi > /tmp/acc_spi.log 2>&1
# 观察 ≥1 个完整开平周期后 Ctrl-C
```

**日志锚（`/tmp/acc_spi.log` 按序出现，缺一不可）**：

| 顺序 | 锚串（grep -F 原文匹配） | 含义 |
|---|---|---|
| 1 | `运行实例 mode 一览` | mode 分流表打印（肉眼核对每 symbol 的 live/paper） |
| 2 | `订单网关已启动（连接由 task 内部维护；下单按 per-symbol mode 分流）` | 共享网关建立 |
| 3 | `engine live 预热完成（本地 CSV + 远程回填；聚合只推进不驱动策略）` | 每实例 warmup 完成（字段 `local_1m`/`remote_1m`/`total_1m` 非零，见 `spi_live_actor.rs:1075-1083`）。**订正 v1.2**：本行曾被改为另一锚并声明"原锚从未存在"——**该判断是错的**，原锚只是 `SPI `→`engine ` 改名，实串现存于 `app/src/spi_live_actor.rs:1083`；错因是我按"同一行含 `info!`"过滤 grep，而宏起于 :1016、字面量在 :1024。教训入册：**锚存在性只按字面量本体 grep** |
| 4 | `运行时 Actor 已启动`（字段 `engine=spi`） | 分流确落 SPI，非旧链 |
| 5 | `engine 编排收到 OrderUpdate（T7 路由）` | B8 精确路由在场生效 |

**禁止出现的锚（出现即红）**：`engine live 装配失败`、`engine live actor panic`、
`engine Store flush 写盘失败`、`engine 编排：提交结果落盘失败`、`engine 编排：回报内核处理失败`。

**xili/trading-service 侧**：`REST place_order received`、`south message received`
（option=place_order）逐单出现。

**state 五件套核对**（RUNBOOK.md §2.2 inspect）：

```bash
cargo run -p strategy-app -- inspect data/accept_spi/state/<YYYYMMDD>/signal.csv
cargo run -p strategy-app -- inspect data/accept_spi/state/<YYYYMMDD>/position.csv
```

- 行键前缀（v2.9 命名空间钉）：crypto `sig_binance_crypto_perpetual_<SYMBOL>_`、
  CTP `sig_SHFE_cn_futures_<合约>_`（实例名段=venue_key 下划线形）。
- signal 状态机完整：`Submitted → Filled` 无悬挂（在途超期由看门狗查询收敛，§5 演练）。

- [ ] 勾 B1：锚 1–5 全部按序出现；禁止锚零出现
- [ ] 勾 B2：帧形对照——**方法已失效（订正）**：旧链代码删除，无法当场取 old 帧。
      在场改证「帧形等于规格」：出帧十键逐键核 `crates/broker/src/wire.rs:8` 形状 +
      `signal_id` 外**逐键一致**（十键形状；crypto offset=None 不带字段 = 旧形态逐字节），
      并把现场帧与 `spi_dual_run_parity.rs` 冻结的旧链帧样本比对（**非当场 A/B**，
      只证「与冻结时一致」，不证「与今日旧链一致」——旧链已不存在）
- [ ] 勾 B3：五件套行键/状态机核对通过

## 3. 线协议在场核对（offset / fee / 数量字面量）

以 CTP 合约（如 CU2601，contracts.yaml 真条目）+ 模拟柜台为主路径：

| 检查点 | 期望 | 取证 |
|---|---|---|
| 开仓帧 | `offset: "open"`（或与服务端约定的开仓表达，口径先拉通） | trading-service 收到的原始帧 |
| 日盘内先开后平 | `offset: "close_today"` | 同上；**双链同口径**：入册按回报 `event_time` 判今昨，跨交易日报落的开仓，日内平应落 `close_yesterday`——柜台侧核对 |
| 数量 | 整数手：帧内数量与 `size_raw` 字面量一致（§14-13 全链） | 帧日志 + signal.csv `size_raw` |
| 回帧 fee | `fee` 数字/字符串双口径均可；本地 `FillEvent.fee` 不再恒 0 | position/成交 CSV fee 字段 + `engine 编排：就绪成交内核处理失败` 不出现 |
| 重复回报 | 同一 order_id 重复 Filled 帧不二次入账 | 服务端回放或手工补发，本地行数不变 |

- [ ] 勾 C1：offset 在场标签与服务端落库一致（平今/平昨柜台上真实区分，不是被忽略）
- [ ] 勾 C2：fee 回传落进本地账本（非 0）
- [ ] 勾 C3：数量字面量全链无精度改写

## 4. 真柜台在场（simnow 先行 → 生产柜台小仓）

顺序钉死：simnow（或柜台仿真环境）全绿后才进生产小仓；两环境各勾一遍 C 组。

```bash
# per-symbol yaml：mode: live；exchange/account 指向真实例
cargo run -p strategy-app -- run --config config/<dir> \
  --output-dir data/accept_live ...   # 首时段先挂 submit_enabled:false 空跑观察行情消费
```

CTP 检查点：
- [ ] 勾 D1：交易日历日切换（夜盘→次日）今昨仓判定与柜台结算单一致
- [ ] 勾 D2：柜台 ReduceOnly 拒单触发本地外部平仓对账路径
      （锚：`ReduceOnly 外部平仓` 相关 warn/error 只在预期时出现；仓位收敛正确）
- [ ] 勾 D3：开收盘禁翻窗（contracts.yaml no_trade_*）在场可证部分：禁窗内不出单 +
      门控日志锚出现；「与旧链行为一致」**当场不可证**（旧链已删），该等价性由
      `SessionWindowPolicy` / `aggregate_and_gate` 单测承重（账本见 retirement §5.1）
- [ ] 勾 D4：真实断线（杀 trading-service 进程再拉起）→ 重连对账：
      锚 `engine 编排对账：` / `engine 交叉对账` 出现且仓位/在途终局一致
- [ ] 勾 D5：行情断流（杀 trading-service/mock 再拉起——K 线与订单同一条连接，
      断连即断流）→ `engine kline 消费落后，backfill 自愈`
      出现（补缺 = 本地 CSV + get_klines，契约 §8.5），恢复后无双算/漏 bar

Binance 检查点（小名义 `--cash`）：
- [ ] 勾 D6：下单→成交→平仓全周期，回帧 fee 落账；无 offset 字段（crypto 口径）
- [ ] 勾 D7：`engine 跳过远程仓位对账（非 live 模式）` 在 paper 出现、live 不出现；
      live 下远程对账锚 `engine 远程仓位对账：三要素匹配，持仓保留` 与实际持仓相符

## 5. 故障与重启演练（先模拟位，后生产位）

```bash
# 5.1 优雅关停：Ctrl-C
#   锚：`收到退出信号，优雅关闭运行时` → 各实例 flush → 进程退出码 0
# 5.2 崩溃重启：kill -9；再原命令拉起
#   期望：restore 读快照成功（`engine live 仓位 restore 失败` **不**出现）；
#         启动对账先查后发补平差异；在途单不误重提
# 5.3 整代重建观察（在场只能等真 panic，进程内已由 spi_live_merge 钉死）：
#   若演练期出现 `engine live actor panic，整代重建（重新订阅 + 启动对账恢复）`：
#   核对退避时长 ~200ms×min(gen,6)、重订阅成功、仓位无损；无则如实记「未观测」，
#   不造假观测。
```

- [ ] 勾 E1：优雅关停 flush 完整（重启后 inspect 行数 ≥ 关停前）
- [ ] 勾 E2：kill -9 恢复后与柜台账务终局一致（结算单/REST positions 比对）
- [ ] 勾 E3：（若观测到）整代重建路径正确；未观测则登记「在场未触发」

## 6. 双链并行对照（**方法已消失**，保留条目以示未被偷偷勾绿）

⚠️ 订正：本节原要求同一服务端两账户分别跑 `--engine old` 与 `--engine spi` 并行对照。
旧链代码已随 R4/D1 删除，**该对照在今日不可能执行**——不是"没做"，是"做不了"。
现存最接近的承重证据：`spi_dual_run_parity.rs`（旧链 TradeRow 冻结对）+
`golden/retirement/` 11 件。它们证的是「与冻结时点一致」，不证「与今日旧链一致」。

同一服务端开两个账户、并行跑同一策略参数与同一行情源的**单链多账户**演练仍可做
（`--output-dir` 必须分离；进程级 Store 各自独立，同 symbol 同 venue 写同目录互踩）。
预期仍为：决策序列（信号方向/时点）一致，成交差异仅来自报价滑点与延迟；
差异超窗即登记差异表并归因，不许「大概一致」过关。

- [ ] 勾 F1（可选）：≥1 个完整交易日并行对照，决策序列一致

## 7. 通过判据与缺省切换

**切换判据（全部为硬勾）**：
- A 组 3 项 + B 组 3 项 + C 组 3 项 + D 组（CTP 5 项 + crypto 2 项，按在场范围）
  + E 组 3 项（E3 允许「在场未触发」如实登记）。
- 证据包齐（§8）。任何红项**不得**以改锚/豁免关闭；豁免只有一条已登记的
  差异表 #2（signal_id 方案）。

⚠️ 订正：**缺省切换已经发生**，不再是「本文之后的独立小 PR」——R4 删除了
`main.rs` 两处 `--engine` 分流与 `default_value = "old"`，统一 `ExchangeRunner`
主循环成为唯一路径（实测 `run --help` 已无 `--engine`）。因此本文的性质从
「切换前置门禁」变为「**已切换事实的在场验证 + 代价知情登记**」；切换的代价是
回退线消失（§8），该代价的运维签字尚未取得，登记于 retirement §6 第 3 项。
**在场验证未通过 ≠ 可以回退到旧链**（旧链代码不存在），只能 revert 提交或修缺陷。

## 8. 回退

- 运行路：**无 CLI 回退**（`--engine old` 已随 R4 删除）。唯一退路是
  `git revert c18e6e4` 之前的区间 + 重新编译（分钟级，且需旧链依赖的
  `data::Feed`/`ctp_session` 一并恢复——它们已在 `030e740` 删除）。
- 数据：SPI 行与历史旧行同 Store schema 共存（v2.9 行键设计，零重写）；行键为
  随行持久化的不透明 ID，现链可按落盘 id 恢复旧行（**旧链已无代码可读新行**，
  原「旧链可读新行」一条随删除失效）。
  疑污时：备份当次 `state/` 目录后重跑恢复（RUNBOOK.md §6 同法）。
- 协议：SPI 帧与旧帧仅差 `offset`（有合约时）；服务端无需回退。

## 9. 证据包清单（归档目录建议 `docs/archive/acceptance-<YYYYMMDD>/`）

```
acc_old.log / acc_spi.log           # §2 两链日志
frames_diff.txt                     # §2 勾 B2 帧逐键对照
trading_service_frames.log          # 服务端收到的原始帧
state/                              # 两目录五件套 CSV 或 --out 导出的 flat csv
settlement_compare.txt              # §4/§5 与柜台结算单比对记录
checklist.md                        # 本文全部勾项 + 实际输出摘录 + 时间戳
```

---
*维护锚*：本文日志串与 `crates/app/src/spi_live_actor.rs` /
`crates/app/src/bootstrap.rs` 的宏字面量绑定；改代码日志必须同改本文，
CI 无法替盯，评审清单加一条「RUNBOOK 锚串核对」。
