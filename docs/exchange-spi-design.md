# 交易所 SPI 设计文档

<!-- refcheck:coords=off  账本文件：正文含大量**已删实现**（data/src/feed.rs、
     ctp_session.rs、app/engine.rs、runtime_actor.rs）的诊断坐标——那是"旧实现为何
     错"的动机叙述，按 D3 铁律只追加不改写，故本文件不参与坐标存在性校验。
     现行实现的坐标由 docs/design-package/*、docs/RUNBOOK.md、
     docs/exchange-spi-live-acceptance.md 承重（这三者坐标校验全开）。 -->

> 新增一个交易所（或一种市场形态），只需在 `exchanges/{name}/` 下实现一组 Rust trait，
> 就能直接跑通 **实盘 + paper + 回测** 的全套逻辑；并让多品种、多交易所在同一进程内混跑。
>
> 本文是唯一的设计文档，涵盖：现状剖析、目录结构、接口定义、交易所模板、
> 配置分派、迁移路径、测试策略、已知风险、交付物状态。
>
> **v2（2026-11-05）审计合并**：全量代码核验后，§1/§2.2/§2.3/§2.6 勘误，
> §6.5/§6.9/§7/§9/§12/§13/§14 按审计结论修订（止损语义、per-instrument
> 签名、盯市挂接、换月因果），§15 验证状态以 Linux/WSL 实测刷新（8/8 +
> 全仓测试全绿），新增 §16「审计修改点台账」与 §17「模块爆炸半径与最小嵌入路径」。

> **端口收口注记（2026-11-09，追加；上方与下方各处 v2.x 原文一律不改写）**
>
> 本文有 13 处叙述 `BarDecoder`/`JsonBarDecoder`/`RawFrame`，按**节**定位（故意不写
> 行号：文档会长，行号必漂——今天已实测过一次，本注记插入 28 行就让同段所引行号全部
> 失效）：§3 设计原则、§3 分层图（ASCII 框）、§4 目录结构（文件树注释）、§4 分层原则、
> **§6.2 `BarDecoder` 全节**、§5 接口清单表、§6.9 `Exchange` 总契约（`fn decoder`
> 声明）、§7.1 与 §7.2 两份 `mod.rs` 标准模板、§10 crypto vs 期货差异映射、
> §11「新增一个交易所」步骤清单、§16 审计台账 C2 条目。这些是**设计史**，
> 按 D3 铁律保留原文；今日实情如下，读本文时以本注记为准：
>
> 1. **`BarDecoder` 端口已退役删除**（commit `ab92793`，说明更正见 `280ca47`）。
>    删除判据为调用点实测：`parts.decoder` 读取点 0、`resolve_finality` 调用者 0
>    （含测试）、`decode()` 唯一调用点在 `resolve_finality` 体内 ⇒ 整条不可达；
>    删后六符号在代码侧的非注释命中为 0。断言账本 −1 案
>    （`core::bar::tests::infer_finality_boundary_semantics`，钉的正是被删代码），
>    全仓 **517 = 531 − 24 + 11 − 1**。帧解码继续由 `data` 承担、闭合帧经
>    `ChannelBarSource` 入主循环——**故 §6.2 的"实现 decoder() 即可接新交易所"
>    这条接入路径今天不成立**，新增交易所的行情面请照 `docs/design-package/03` 现状节。
> 2. **`SubscriptionPlan` 已接入主路**（commit `63299f2`）：live 的订阅与历史回补
>    改以 `runner.subscriptions()` 解析出的 topic 为单一真源；非 1m 在装配期
>    fail-fast；多合约目前只消费首条。**自动换月仍未生效**——`MarketEvent::Roll`
>    无生产端，其输入（合约快照宇宙）在生产侧恒空，本文 §5/§6.3 关于换月的叙述
>    属设计意图，非今日行为。
> 3. **`ContinuousSeriesBuilder` 仍未接线**（`detect_roll`/`stitch` 生产调用 0），
>    与 `AdjustMode::ratio`（复权未实现，CLI 参数解析期即报错）同批处置；
>    逐条证据与剩余缺口见 `docs/design-package/03-详细设计.md` §3.10。
>
> 一句话口径：**本文的 trait 清单是"当初打算开哪些扩展点"的账，不是"哪些已交付"**；
> 现状真源 = `docs/design-package/`（坐标全开校验）+ `docs/RUNBOOK.md` +
> `docs/exchange-spi-live-acceptance.md`（日志锚校验开启）。

---

## 目录

- [1. 结论先行](#1-结论先行)
- [2. 现状剖析：六个痛点分别落在哪](#2-现状剖析六个痛点分别落在哪)
- [3. 设计目标与原则](#3-设计目标与原则)
- [4. 目录结构](#4-目录结构)
- [5. 接口清单](#5-接口清单)
- [6. 接口 trait 定义](#6-接口-trait-定义)
- [7. 每个交易所的 mod.rs 标准模板](#7-每个交易所的-modrs-标准模板)
- [8. 配置驱动：按交易所分派](#8-配置驱动按交易所分派)
- [9. 统一主循环](#9-统一主循环)
- [10. crypto vs 国内期货差异映射](#10-crypto-vs-国内期货差异映射)
- [11. 新增一个交易所：完整步骤](#11-新增一个交易所完整步骤)
- [12. 测试策略](#12-测试策略)
- [13. 迁移路径](#13-迁移路径)
- [14. 已知风险与待决策项](#14-已知风险与待决策项)
- [15. 交付物与验证状态](#15-交付物与验证状态)
- [16. 审计修改点台账（v2）](#16-审计修改点台账v2)
- [17. 模块爆炸半径与最小嵌入路径（v2）](#17-模块爆炸半径与最小嵌入路径v2)

---

## 1. 结论先行

当前模板「作为 crypto 策略开发脚手架是合格的」，但**扩展性问题不是散落的
if/else，而是三个结构性的缺失**：

| 结构性缺失 | 表现 |
|---|---|
| **没有「市场规则」这一层** | 规则以 `Option<ContractSpec>` 表达（`Some` = 期货 / `None` = crypto），散落在 config / main / engine / runtime_actor / backtest / mock 六个文件、12 处分支（v2 勘误：原文所列 bootstrap、contract 两文件实无此分支，遗漏 `broker/src/mock.rs:580`） |
| **实盘与回测的规则执行者不同** | 回测在 `BacktestBroker` 内自算整数手/保证金/平今；实盘把同一套规则全部推给外部 `trading-service`，靠「人工保持同一费率口径」对齐（`contracts/trading-service.md §7`） |
| **标的是裸字符串、市场是进程全局值** | `symbol: String` 贯穿全链路；`exchange` 只是透传标签；`services.kline.market: MarketKind` 是全局单值，配置注释明写「crypto 与期货不要混跑」（**已随 klines-service 退役，2026-11-10 删除**） |

因此只做「把 `BarMergeMode` 换成 trait」是不够的。正确的切法是引入一个
**交易所 SPI 层**，把差异收敛成 12 个接口，并让**规则只实现一次**：
`Exchange` 在 `RunMode::{Live, Paper, Backtest}` 下装配的是同一批
`Arc<dyn TradingRules / PositionRules / SessionRules>`，只有「数据从哪来」
和「订单到哪去」两个端口允许分叉。

> **v2 外部前置（从 §14 风险 3 提升到结论级）**：`trading-service` 协议当前
> 不接受 `offset`、不回传 `fee`（`crates/broker/src/wire.rs:32-84`，
> `contracts/trading-service.md` §7 第 3 条明写「模板侧不发送 offset 字段」）。
> **协议不升级，第一公理只在回测/paper 侧成立**，实盘侧规则仍然分叉。
> 阶段 4 必须与协议升级同步排期，这是方案成立性的前置约束，不是可选风险。

---

## 2. 现状剖析：六个痛点分别落在哪

### 2.1 K 线聚合方式不同 + 完整 K 线判定

主链路是 `Feed`（`crates/data/src/feed.rs`），实盘 WS 闭合帧与回测 CSV 逐根
1m 都进 `Feed::on_closed_1m`（`feed.rs:424`），这点是好的。问题在 `ingest`
（`:436-471`）用二值枚举硬分派：

```rust
// feed.rs:23-30 / 448-466
pub enum BarMergeMode { Fixed, Ctp }        // 二值，不是 trait
...
if tf.0 == 1 { ... }
else if *tf == Timeframe::D1 { state.merge_d1(c, cal) }        // D1 恒走 CTP 交易日
else if self.mode == Ctp && ctp_session::is_ctp_supported(tf.0) { state.merge_ctp(...) }
else { state.merge(...) }                                      // UTC epoch 网格
```

具体硬编码：

- `ctp_session.rs:27-53` 的窗口表（21:00–01:00、9:00–10:15、10:30–11:30、
  13:30–15:00）**写死在源码**，`is_ctp_supported` 只支持 60/120/240 分钟
  （v2 勘误：表内实为跨休市累积窗口 `[10:15,11:30)`，"10:30–11:30" 出自
  模块头注释的时段口径，`ctp_session.rs:4-5`）；
- `Feed::new` 只接收 `BarMergeMode`，**拿不到 `ContractSpec`**。整个
  `crates/data/` 目录对 `session_opens` / `session_closes` / `contracts.yaml`
  零引用 —— `config/contracts.yaml` 里 AU/AG 的 02:30 收盘根本无法表达；
- `aggregate.rs:14-18` 的 D1 = UTC 午夜，而 `feed.rs:454` 的 D1 **恒按 CTP
  交易日切分、与 mode 无关**（`feed.rs:596` 测试注释自认）。两套口径冲突，
  crypto 的日线语义被污染；`crates/app/src/report_charts.rs:77` 又用了前者做兜底（v2 勘误路径，原误作 data crate）。另 `feed.rs:317-338` 还藏着**第三套**交易日口径（18/8/20 时阈值启发式），审计补录。

**「K 线是否完整」当前是两个层次混在一起**：

| 层次 | 现状 | 位置 |
|---|---|---|
| 单根 1m 是否收线 | WS `is_final` → `Candle.is_closed`；CSV 恒 `true` | `remote_client.rs:350,412`、`csv_store.rs:96` |
| 大周期桶是否收满 | `merge/merge_ctp/merge_d1` 返回 bool → `FeedEvent::newly_closed` | `feed.rs:357-363, 436-471` |

CTP 类数据源可能根本没有 `is_final` 字段，需要靠「会话边界 + 时间」推断；
现有代码没有这个抽象。另外期货的「中间断开」（午休 10:15–13:30、夜盘跨日、
节假日）在上游契约里被推给行情服务保证连续（写作时为 kline-service；
2026-11-10 已退役，现由 trading-service `subscribe_kline` 承担——见
`contracts/trading-service.md` §8，旧契约归档 `docs/archive/kline-service.md`），
模板不补缺也不显式断桶。

### 2.2 交易规则不同

- `ContractSpec`（`core/src/contract.rs:75-105`）有 `multiplier / tick_size /
  margin_rate / min_lots / commission`，但**没有** `qty_step / min_qty /
  price_precision / step_size`；`tick_size` 只在 `validate()` 里校验 > 0，
  从未用于价格取整（文件头注释已承认）。
- 步进实现有两处（v2 勘误：原文称「唯一」过强）：`broker/src/mock.rs:442` 的 `floor_lot()` 硬编码 1e-6；以及 `core/src/contract.rs:255-285` 的整数手 `sizing_lots/max_lots`（被 `backtest.rs:207` 与 `mock.rs:580` 共用）。
- 开仓被分成两段：`backtest.rs:206`（期货整数手 + 保证金 + 合约手续费）与
  `:248`（crypto `qty = notional / price`，无任何规整）。
- 未配置合约的 symbol 一律走「crypto 兼容模式」：`main.rs:329`
  `multiplier = map_or(1.0, ...)`、`engine.rs:357` `no_trade = false` 等一串兜底。

### 2.3 平仓规则不同（平今 / 平昨）

- 全仓只有 `PositionState.remaining_size` 一个净仓（`positions/src/state.rs`），
  **没有今昨仓概念**；
- 「平今」只存在于 `BacktestBroker::Position.open_trading_day`（`backtest.rs:113`，v2 行号校正），
  判据是 `open_trading_day == ctp_trading_day(t)`（`:301`），用来选费率；
- 实盘 `crates/broker/src/wire.rs`（v2 勘误路径，原误作 app crate）的 `PlaceOrderClose`（:32-46）没有 `offset` 字段，开平靠
  `intent.position_id.is_some()` 隐式判定（`crates/broker/src/trading_service.rs:203`），
  平今平昨「由服务端按 CTP 交易日决定」（`contracts/trading-service.md` §7 第 3 条 :237-239；v2 勘误：契约文件无 §7.3 子锚点）；
- **两套交易日口径并存**：`contract::ctp_trading_day`（20:00 切日、仅周末近似，
  用于平今费率与 roll_build 选主 :509）vs `TradingCalendar::trading_day_of`（18:00 切日、有节假日表，
  用于日线聚合 `feed.rs:253` 与期货回填窗口 `remote_client.rs:102`）。同一笔单在两处可能落到不同交易日。（v2 勘误：原文把「选主」归给 `trading_day_of`，与本文 §2.4 自相矛盾——选主实际用的正是 `ctp_trading_day`；另有第三套口径见 §2.1 补录。）
- **两套持仓模型并存**：`broker::backtest::Position`（带 trading_day、手数语义，
  驱动资金）与 `positions::PositionState`（净仓、`remaining_size_raw`，驱动报表
  与实盘平仓数量）。

### 2.4 订阅规则不同（主力 / 次主力）

- 主链路无「主力」概念，`symbol` 是裸字符串；`KlineClient::attach(&str)`
  （`data/src/client.rs:45`）与 `csv_path(base, symbol, tf)` 都按字符串索引；
- `docs/archive/kline-service.md §5.1`（写作时 `contracts/kline-service.md`，
  现已退役归档）明确「主连不在模板内拼接，由上游产出」；
- 唯一实现是独立 CLI bin `data/src/bin/roll_build.rs`（1249 行）：按 OI 选主、
  因果口径（D 日观测 D+1 生效）、产出 `{PRODUCT}_CONT_1m.csv`，但**不做复权**，
  且用的是第三套交易日近似（`ctp_trading_day`，20:00、无节假日表）。
- 换月、主次主力价差、订阅切换都不在运行时链路里。

### 2.5 回测规则不一致

| 项 | 现状 |
|---|---|
| 成交价 | `backtest.rs:132` 当前 1m 收盘价 ± 滑点；`slippage` 默认 0 且**没有 CLI/配置入口**，报表 `TradeRow.slippage` 恒 0（`engine.rs:720`） |
| 禁交易窗口 | `engine.rs:347-395`（回测）与 `runtime_actor.rs:600-634`（live）**各实现一遍**；engine 的 `pending_actions` 因 `:376` 提前 return 而恒为空，两处已不对称 |
| 结算 | 手续费/盈亏算三遍：`backtest.rs:298-322`（现金）、`engine.rs:676-692`（报表 PnL）、`backtest_paper_parity.rs:724-767`（测试派生台账） |
| 实盘手续费 | `signals/manager.rs:1099`、`runtime_actor.rs:938` 的 `FillEvent.fee` 恒 `0.0`，因为 `wire.rs` 的 `InEvent` 没有 fee 字段 |
| 装配差异 | `notional_per_order` 回测强制 0（全仓，`main.rs:344`），paper 用 `spec.cash`（`bootstrap.rs:78`） |
| 一致性测试 | `backtest_paper_parity.rs` 只覆盖「单 symbol、单配置目录、单 market」，paper 侧仍是本地撮合，未覆盖真实 trading-service 链路 |

### 2.6 只支持单币种 / 无法混跑

单例假设贯穿全链路：

| 层 | 位置 | 假设 |
|---|---|---|
| CLI | `main.rs:46` | `Backtest { symbol: Option<String> }` 单个 |
| 数据路径 | `csv_store.rs:18` | `{base}/{tf}/{SYMBOL}_{tf}.csv`，无 exchange 命名空间 |
| 引擎 | `engine.rs:41-136, 182` | 单 `Feed` / 单 `PositionManager` / `client: BacktestOrderClient`（**具体类型**） |
| 意图 | `core/src/signal.rs:175` | `SignalIntent` 无 exchange / market 字段 |
| 网关 | `bootstrap.rs:210` | `exchange` 在**网关构造时**绑定为 `OrderVenueCfg`，非意图级分派 |
| 全局市场 | `config.rs:144` | `services.kline.market` 每次运行单值（v2 校正：「crypto 与期货不要混跑」字样实见 `config/_global.example.yaml:55`、`config/tpl_trend_crypto/CU.yaml:11`、`docs/RUNBOOK.md:184`，`config.rs:143` 注释仅为形态说明）（**已随 klines-service 退役，2026-11-10 删除**） |
| 合约表 | `config/contracts.yaml` | `BTreeMap<symbol, ContractSpec>`，无 exchange 维度 |
| 报表 / Store | `main.rs:306`、`positions/src/manager.rs:47` | 目录只按 symbol；持久化行按 `strategy_name` 过滤，无 exchange 维度（v2 勘误：过滤点在 positions，原误标 `store/src/lib.rs:405`，该处为 flush 快照路径） |

> 关键判断：`exchange` 目前是「透传字符串标签」，从不参与分派；真正决定撮合
> 规则的是 `contract: Option<ContractSpec>`；而 `market` 反而是全局单值，
> 直接决定 K 线回填窗口口径。三者互不相干，是多交易所混跑的根本障碍。

---

## 3. 设计目标与原则

### 目标

| 目标 | 具体要求 |
|---|---|
| 每交易所一套标准 | `impl Exchange` 就是标准，所有交易所遵循同一接口 |
| 新增交易所只加文件夹 | `exchanges/{name}/` 下实现 `mod.rs`，不动 core 任何文件 |
| 回测和实盘统一 | 规则方法只实现一次，`make_source` / `make_execution` 按 RunMode 分叉 |
| 启动按交易所走不同配置 | 配置按 `exchanges: { name: ... }` 分组，Registry 自动分派 |
| 多交易所可混跑 | 每个标的独立 `ExchangeParts`（含独立数据源 + 订单出口 + 规则 Arc） |

### 设计原则

1. **规则单一来源（Single Source of Truth）**
   `TradingRules` / `PositionRules` / `SessionRules` 是纯规则对象，
   `Exchange::build()` 在三种 `RunMode` 下注入**同一批 `Arc`**。
   实盘网关必须消费模板侧的规整结果与 `offset`，不再让外部服务另算一套。
2. **实盘 / 回测差异只允许出现在两个端口**
   `BarEventSource`（数据从哪来）与 `ExecutionPort`（订单到哪去）。
   `ExchangeRunner::run()` 里没有任何 `if mode == backtest`。
3. **两个层次的「完整 K 线」解耦**
   单根 1m 收线 = `BarDecoder`；大周期桶收满 = `BarAggregationPolicy`。
4. **标的是命名空间键**
   `InstrumentId { exchange, market, symbol, product, contract_month }`，
   所有路由 / CSV / 报表 / Store 都基于 `namespaced_key()`。
5. **object-safe + 运行时注册表**
   全部 trait 可 `dyn`，用 `ExchangeRegistry` 按 `ExchangeId` 分派，
   新增交易所无需把泛型传染到 `Engine` / CLI / 报表。

### 分层图

```
                    ┌─────────────────────────────────────────────┐
                    │  ExchangeRegistry: id -> Arc<dyn Exchange>  │
                    └──────────────────────┬──────────────────────┘
                                           │ build(SourceRequest)
        ┌──────────────────────────────────┴──────────────────────────────────┐
        │  规则族（live / paper / backtest 共用同一批 Arc）                     │
        │  SessionRules · BarAggregationPolicy · BarDecoder                    │
        │  TradingRules · PositionRules · SimulationPolicy · SubscriptionPlan  │
        └──────────────────────────────────┬──────────────────────────────────┘
                                           │
        ┌──────────────────────────────────┴──────────────────────────────────┐
        │  端口（唯一允许按 RunMode 分叉）                                     │
        │  BarEventSource:  WS 适配器(live) / CSV·DB 回放(backtest)            │
        │  ExecutionPort :  远程网关(live) / SimulationExecution(backtest)     │
        └──────────────────────────────────┬──────────────────────────────────┘
                                           │
                    ┌──────────────────────┴──────────────────────┐
                    │  ExchangeRunner::run  —— 单一主循环           │
                    │  收线 → 聚合 → 门控 → 策略 → 规整/拆腿        │
                    │        → 下单 → 成交 → 结算 → 持仓账本         │
                    └─────────────────────────────────────────────┘
```

---

## 4. 目录结构

> **命名收口注记（2026-11-09）**：本节拟名 `crates/exchange-spi/`（包名
> `strategy-exchange-spi`）在实装时落为 **`crates/engine/`**（包名
> `strategy-engine`），`Cargo.toml` workspace members 已含之；`design-review.md:195/326`
> 的 P2-8「重命名 exchange-spi → engine」即此事实的登记项，**已生效**。
> 本文其余 19 处 `exchange-spi` 字样按「同一层、旧拟名」读，不回填改写。

```
crates/exchange-spi/src/
│
├── lib.rs                  # 框架入口 + prelude + 自动注册
├── registry.rs             # ExchangeRegistry, build_all()
│
├── core/                   # ── 框架层：trait 定义 + 通用实现 + 主循环 ──
│   ├── mod.rs              #   pub use 所有子模块
│   ├── identity.rs         #   InstrumentId, ExchangeId, MarketKind, RunMode
│   ├── bar.rs              #   Bar, BarFinality, BarDecoder, MarketEvent, BarEventSource
│   ├── session.rs          #   SessionRules trait, ContinuousSession, WindowedSession
│   ├── aggregation.rs      #   BarAggregationPolicy, UtcGridPolicy, SessionWindowPolicy
│   ├── trading.rs          #   TradingRules trait, InstrumentSpec, SpecDrivenTradingRules
│   ├── position.rs         #   PositionRules trait, PositionBook, NetRules, CtpRules
│   ├── execution.rs        #   ExecutionPort trait, SimulationPolicy, Fill, SimulationExecution
│   ├── exchange.rs         #   Exchange trait, ExchangeParts, SourceRequest
│   └── runtime.rs          #   ExchangeRunner（单一主循环，无 if mode == backtest）
│
├── exchanges/              # ── 交易所实现：每个交易所一个文件夹 ──
│   ├── mod.rs              #   pub mod binance; pub mod ctp;（新增交易所在此加一行）
│   │
│   ├── binance/            #   Binance 实现
│   │   ├── mod.rs          #     BinanceExchange: impl Exchange
│   │   ├── decoder.rs      #     WS 帧解码（币安 12 列数组格式）
│   │   ├── live_source.rs  #     实盘 WS K 线源（覆写 make_source）
│   │   └── live_gateway.rs #     实盘订单网关（覆写 make_execution）
│   │
│   ├── ctp/                #   CTP / 国内期货实现
│   │   ├── mod.rs          #     CtpExchange: impl Exchange
│   │   ├── decoder.rs      #     CTP 帧解码（无 is_final，靠会话边界推断）
│   │   ├── session.rs      #     时段表配置（从 contracts.yaml 读，替代 ctp_session.rs）
│   │   └── live_gateway.rs #     实盘订单网关（覆写 make_execution）
│   │
│   ├── okx/                #   未来新增（同样结构）
│   └── cffex/              #   未来新增（同样结构）
```

### 分层原则

| 层 | 目录 | 职责 | 改动频率 |
|---|---|---|---|
| **框架层** | `core/` | trait 定义 + 通用实现 + 主循环 | 极低（接口稳定后基本不动） |
| **交易所层** | `exchanges/{name}/` | 该交易所的规则组装 + 实盘适配 | 新增交易所时才加文件 |
| **注册层** | `lib.rs` + `registry.rs` | 自动发现 + 装配 | 新增交易所时加一行 `pub mod` |

> 核心约束：**exchanges/ 下的代码可以依赖 core/，反之不行。**
>
> **v2.4 阶段 1 实况**：本结构已落地——`core/`（11 文件）、`exchanges/binance/mod.rs`、
> `exchanges/ctp/mod.rs`、`registry.rs`；`decoder.rs` / `live_source.rs` /
> `live_gateway.rs` 等实盘文件按 §7.3 留待阶段 4 按需新增。参考实现的
> `JsonBarDecoder` 归入 `core/bar.rs`（框架级默认解码器）；交易所名对齐
> §7 模板：`BinanceExchange` / `CtpExchange`。跨交易所测试位置说明见 §12。

---

## 5. 接口清单

一个交易所在实盘 / 回测各需要实现什么：

| # | 接口 | 关键方法 | 实盘 | 回测 | 默认实现 | 解决痛点 |
|---|---|---|---|---|---|---|
| 1 | `Exchange` | `id / markets / parse_instrument / build` | 注册 | 注册 | `build` 有默认体 | 整体扩展 |
| 2 | `SessionRules` | `trading_day_of / sessions_on / is_no_trade_window / backfill_start / gap_after` | 门控、回填 | 断桶、日切 | `ContinuousSession` / `WindowedSession` | K线聚合、回测 |
| 3 | `BarDecoder` | `decode / finality_of / infer_finality` | WS 帧 | CSV 恒 Final | `JsonBarDecoder` | 完整K线判定 |
| 4 | `BarAggregationPolicy` | `bucket_of / is_last_bar_of / closes_on_gap / closes_on_day_boundary` | 是 | 是 | `UtcGridPolicy` / `SessionWindowPolicy` | 聚合方式 |
| 5 | `AggregationState` | `on_closed_1m / on_gap / on_day_boundary` | 是 | 是 | **框架提供** | 聚合 |
| 6 | `SubscriptionPlan` | `resolve / on_roll / continuous_series` | 主力订阅 | 连续合约 | `DirectSubscriptionPlan` / `MainContractPlan` | 订阅规则 |
| 7 | `ContinuousSeriesBuilder` | `detect_roll / stitch` | 可选 | 换月拼接 | `NoAdjustContinuousSeries` | 连续合约 |
| 8 | `TradingRules` | `spec / normalize_price / normalize_qty / sizing / fee / margin / pnl / validate` | 下单前规整 | 撮合结算 | `SpecDrivenTradingRules` | 交易规则 |
| 9 | `PositionRules` | `split_close / on_open_fill / on_close_fill / rollover` | 生成 offset 腿 | 平今计费 | `NetPositionRules` / `CtpPositionRules` | 平今平昨 |
| 10 | `BarEventSource` | `next_event` | **覆写** = WS | 默认 = CSV 回放 | 有默认 | 回测差异 |
| 11 | `ExecutionPort` | `submit / query / drain_fills / account_snapshot` | **覆写** = 远程网关 | 默认 = 模拟撮合 | 有默认 | 回测差异 |
| 12 | `SimulationPolicy` | `fill_price / fill_timing / can_trade / mark_price` | — | 唯一回测专属 | `SimpleFillModel` | 回测差异 |
| 13 | `ExchangeRegistry` | `register / build_all` | 多交易所 | 多品种批量 | **框架提供** | 多交易所混跑 |

**最小实现集**：

- **跑回测 / paper**：只需在 `mod.rs` 实现 `Exchange` 的规则方法（#1~4、6、8、9、12），
  大部分有现成默认实现，通常只需给 `InstrumentSpec` 表和 `SessionRules` 配置。
  `make_source` / `make_execution` / `build` 全走默认。
- **接实盘**：再加 `live_source.rs` 和 `live_gateway.rs`，覆写 `make_source` 和 `make_execution`。
  **规则代码一行都不用改。**

---

## 6. 接口 trait 定义

### 6.1 `InstrumentId` — 命名空间强类型标的

```rust
// core/identity.rs

pub struct ExchangeId(pub String);        // "binance", "SHFE", "CFFEX"
pub enum MarketKind {
    CryptoPerpetual,   // 7x24 连续、净仓
    CryptoSpot,
    CnFutures,         // 交易日 + 时段 + 今昨仓 + 整数手
    CnOptions,
    GlobalFutures,
}

pub struct InstrumentId {
    pub exchange: ExchangeId,
    pub market: MarketKind,               // per-instrument，不再是进程全局值
    pub symbol: String,                   // 挂牌代码 BTCUSDT / CU2601
    pub product: String,                  // 品种码 CU
    pub contract_month: Option<ContractMonth>,
}

impl InstrumentId {
    /// 用作 Store / CSV / 报告 / 订单路由的命名空间键
    /// 例如 "binance/crypto_perpetual/BTCUSDT" 或 "SHFE/cn_futures/CU2601"
    pub fn namespaced_key(&self) -> String;
}
```

`exchange` 与 `market` 是**正交**的两个维度：SHFE 与 CFFEX 是不同 exchange
但都是 `CnFutures`；Binance 现货与 U 本位永续是同一 exchange 但不同 market，
费率与持仓规则不同。所有 CSV 路径、报表目录、Store 行键、OrderRouter 路由键
都改用 `namespaced_key()`。

### 6.2 `BarDecoder` — 单根 1m 是否收线 + 统一事件流

> ⚠️ 本节所述端口**已于 2026-11-09 退役删除**（`ab92793`）；下文保留设计史原文不改写。
> 现状与判据见文首「端口收口注记」及 `docs/design-package/03-详细设计.md` §3.10。

```rust
// core/bar.rs

pub enum BarFinality { Final, Provisional, Unknown }

pub trait BarDecoder: Send + Sync {
    /// 把原始帧解码为标准 Bar
    fn decode(&self, frame: &RawFrame) -> Result<Bar>;

    /// 从帧中读取收线标记（WS is_final）
    fn finality_of(&self, frame: &RawFrame) -> BarFinality;

    /// 无标记时，靠会话边界 + 时间推断（CTP 类数据源）
    fn infer_finality(
        &self,
        bar: &Bar,
        now: DateTime<Utc>,
        session: &dyn SessionRules,
    ) -> BarFinality;

    /// 显式标记优先，Unknown 时降级推断；Unknown 一律当非完整
    fn resolve_finality(
        &self,
        bar: Bar,
        frame: Option<&RawFrame>,
        now: DateTime<Utc>,
        session: &dyn SessionRules,
    ) -> Result<Bar>;
}

pub enum MarketEvent {
    Bar(Bar),
    SessionGap(SessionGap),       // 午休 / 隔夜断桶
    DayBoundary(TradingDay),      // 交易日切换
    Roll(RollEvent),              // 换月
    Reconnected,                  // WS 重连
}

pub trait BarEventSource: Send {
    fn next_event(&mut self) -> Result<Option<MarketEvent>>;
}
```

要点：

- `Bar` 把现有 `Candle` 的 `quote_volume / trade_num / active_buy_*`
  （币安专有，期货只能填 0）降级为可选 `BarExtensions`，并补上换月必需的
  `open_interest` 与 `settlement_price`；提供 `Bar::from_candle()` 桥接迁移。
- `Unknown` 一律当非完整处理（`is_final()` 返回 false），宁可丢帧不误判。
- `MarketEvent` 是**实盘与回测唯一的汇合点**：回测回放器按交易日顺序
  发出 `Bar / SessionGap / DayBoundary / Roll`，实盘 WS 适配器把
  `is_final`、重连、换月通知翻译成同样的事件。引擎不再知道数据从哪来。

### 6.3 `SessionRules` — 一个权威交易日 + 配置驱动时段表

```rust
// core/session.rs

pub struct TradingDay(pub NaiveDate);     // 强类型，杜绝「自然日」误用

pub trait SessionRules: Send + Sync {
    fn timezone(&self) -> FixedOffset;
    fn trading_day_of(&self, t: DateTime<Utc>) -> TradingDay;     // 唯一定义
    fn is_trading_day(&self, d: NaiveDate) -> bool;
    fn sessions_on(&self, day: TradingDay) -> Vec<SessionWindow>;
    fn session_at(&self, t: DateTime<Utc>) -> SessionStatus;
    fn is_session_boundary(&self, t: DateTime<Utc>) -> bool;
    fn is_no_trade_window(&self, t: DateTime<Utc>) -> bool;        // 开盘后/收盘前 N 分钟
    fn is_entry_confirm_time(&self, t: DateTime<Utc>) -> bool;
    fn backfill_start(&self, now, tf, bars) -> DateTime<Utc>;      // 按窗口倒推，跳过休市
    fn gap_after(&self, t: DateTime<Utc>) -> Option<SessionGap>;   // 午休/隔夜断桶信号
}
```

**两个参考实现**（在 `core/session.rs`，交易所可直接复用或自建）：

| 实现 | 适用 | 特征 |
|---|---|---|
| `ContinuousSession` | crypto 7x24 | `backfill_start = now - N*tf`，永不产生 gap |
| `WindowedSession` | 国内期货 / 境外期货 | `windows: Vec<(NaiveTime, NaiveTime, SessionKind)>` 由配置构造，`close <= open` 表示跨零点，`day_cutover` 显式配置切日时间，`holidays` 从外部日历注入 |

`SessionGap { reason: LunchBreak|Overnight, closes_bar }` — 午休 `closes_bar = true`，
聚合器据此切断当前桶，不会把 10:15 与 13:30 的 1m 拼进同一根小时线。

这一举解决：
- `ctp_session.rs` 硬编码窗口表、只支持 60/120/240 分钟；
- AU/AG 02:30 收盘无法表达；
- 两套交易日口径（平今 20:00 vs 日线 18:00）冲突。

### 6.4 `BarAggregationPolicy` — 大周期桶边界和闭合条件

```rust
// core/aggregation.rs

pub trait BarAggregationPolicy: Send + Sync {
    fn supports(&self, tf: Timeframe) -> bool;
    fn bucket_of(&self, tf, bar_time, session: &dyn SessionRules) -> Option<BucketId>;
    fn is_last_bar_of(&self, bucket: &BucketId, bar_time, session) -> bool;
    fn drop_partial_head(&self) -> bool;
    fn closes_on_gap(&self, gap: &SessionGap) -> bool;     // 午休断桶
    fn closes_on_day_boundary(&self) -> bool;
}
```

| 策略 | 桶起点 | 闭合条件 | D1 语义 |
|---|---|---|---|
| `UtcGridPolicy` | UTC epoch 整数倍网格 | `bar_end == start + tf` | UTC 午夜（crypto） |
| `SessionWindowPolicy` | 时段窗口内切分，**绝不跨窗口** | 窗口终点或桶满 | 该交易日**首个窗口开盘 → 最后窗口收盘**（含周五夜盘） |

`AggregationState` 是框架提供的有状态聚合器（旧 `Feed` 的去市场判断版）：
水位幂等、首桶 `tainted` 丢弃、新窗口首根到达时**惰性闭合**上一窗口
（覆盖期货缺 bar / 休市）、`on_gap` 强制断桶、`on_day_boundary` 切 D1。

> 这直接解决「D1 恒走 CTP 交易日」的隐性硬编码：crypto 用 `UtcGridPolicy`
> 自然得到 UTC 午夜，期货用 `SessionWindowPolicy` 得到交易日，不再二选一写死。

### 6.5 `SubscriptionPlan` — 主力/次主力/连续合约

```rust
// core/subscription.rs

pub trait SubscriptionPlan: Send + Sync {
    fn resolve(&self, logical: &InstrumentId, as_of: DateTime<Utc>,
               universe: &[ContractSnapshot]) -> Result<Vec<Subscription>>;
    fn on_roll(&self, ev: &RollEvent, current: &[Subscription]) -> Result<Vec<Subscription>>;
    fn continuous_series(&self) -> Option<Arc<dyn ContinuousSeriesBuilder>>;
    fn roll_policy(&self) -> RollPolicy;
}

pub trait ContinuousSeriesBuilder: Send + Sync {
    /// 返回 (新主力, 换月事件)；无换月返回 None。
    /// v2.1 C11：session 参数用于计算 D+1 生效时点（因果口径唯一定义点）。
    fn detect_roll(&self, day, universe, current,
                   session: &dyn SessionRules) -> Option<(InstrumentId, RollEvent)>;
    fn stitch(&self, bars: &[Bar], roll: &RollEvent, mode: AdjustMode) -> Vec<Bar>;
}
```

`DirectSubscriptionPlan`（crypto 一对一透传）与 `MainContractPlan`
（按 OI 选主、剔除交割月、可选次主力、换月切订阅）是参考实现。
`RollEvent` 带 `effective_at`，保持 `roll_build.rs` 已有的**因果口径**
（D 日收盘观测、D+1 交易日生效），避免未来函数。
连续合约拼接/复权从独立 CLI bin 提升为运行时能力。

> **v2 审计修订**：
> 1. 阶段 0 代码**违反了本节承诺的因果口径**——`subscription.rs:291-299` 把
>    `effective_at` 算成观察日**当日 00:00**（本地），早于观测时点，是未来函数；
>    正确语义应为「下一交易日首个窗口开盘」（§16 C11，P1）。
> 2. 「进入交割月即剔除」不是全所通用规则：CFFEX 交割在第三周五、
>    SHFE 有色交割月上中旬仍可交易、DCE 农产品又不同。框架默认实现
>    （`subscription.rs:181`「合约月 > 当前自然月」）不能作为一刀切缺省，
>    交割/剔除规则必须是 **per-product 可覆写参数，具体策略下沉到
>    `exchanges/{name}/`**（§16 C12，P2）——这也是文末「原则 2」的要求。

### 6.6 `TradingRules` — 一套公式覆盖两种市场

```rust
// core/trading.rs

pub trait TradingRules: Send + Sync {
    fn spec(&self, inst: &InstrumentId) -> Result<Arc<InstrumentSpec>>;
    fn normalize_price(&self, inst, raw) -> Result<f64>;         // 按 tick_size 取整
    fn normalize_qty(&self, inst, raw, rounding: Rounding) -> Result<f64>;  // 按 qty_step 取整
    fn notional(&self, inst, price, qty) -> Result<Money>;       // price * multiplier * qty
    fn sizing(&self, req: &SizingRequest) -> Result<f64>;        // crypto: 名义/杠杆; 期货: 整数手+保证金
    fn fee(&self, req: &FeeRequest) -> Result<Money>;            // 按开/平/平今三档
    fn margin(&self, req: &MarginRequest) -> Result<Money>;
    fn pnl(&self, inst, side, entry, exit, qty) -> Result<Money>;
    fn validate(&self, draft: &OrderDraft) -> Result<()>;
}
```

`InstrumentSpec` 补齐 `qty_step / min_qty / price_precision / qty_precision /
sizing_mode`，并把 `notional` 统一为 `price * multiplier * qty`
（crypto 的 multiplier = 1，不再需要 `pos.entry_price * qty` 兼容特判）。

`SizingMode::{NotionalFraction, MarginLots}` 把 `backtest.rs:206` 与 `:248`
两段代码分叉变成**规格数据**：前者小数数量，后者整数手 + 保证金/名义双约束。
`SpecDrivenTradingRules` 是通用实现，绝大多数交易所只需填规格表；
阶梯保证金、maker/taker 等特殊规则再覆写单个方法。

**实盘也要调用它**：`normalize_price / normalize_qty / validate` 在订单帧
生成之前执行，模板下发**已规整的手数与带 offset 的指令**，外部 trading-service
不再自行换算，`trading-service.md §7` 的「人工保持同一费率口径」被取消。

### 6.7 `PositionRules` — 平仓拆腿是交易所能力

```rust
// core/position.rs

pub trait PositionRules: Send + Sync {
    fn mode(&self) -> PositionMode;                         // Net / Dual
    fn split_close(&self, book: &PositionBook, req: &CloseRequest,
                   session: &dyn SessionRules) -> Result<Vec<CloseLeg>>;
    fn on_open_fill(&self, book, lot: &PositionLot) -> Result<()>;
    fn on_close_fill(&self, book, leg: &CloseLeg, filled_qty: f64) -> Result<()>;
    fn rollover(&self, book: &mut PositionBook, new_day: TradingDay);
}

pub struct CloseLeg {
    pub offset: Offset,     // Open / Close / CloseToday / CloseYesterday / Auto
    pub qty: f64,
    pub lot_ids: Vec<String>,   // v2.1 C4：合并腿记录全部贡献批次
    pub side: Side,
    pub entry_price: f64,       // v2.1 C4：同 (offset, side) 多批次合并时为加权均价
}
```

引擎只说「平 X 手」：
- **crypto**（`NetPositionRules`）→ 一条 `Offset::Auto`（对应 `reduce_only`）
- **CTP**（`CtpPositionRules`）→ 按 `ClosePriority::{TodayFirst, YesterdayFirst, ProRata}` 拆成 `CloseToday` + `CloseYesterday` 两条腿

同一批腿同时用于：实盘订单帧的 `offset`、回测撮合的费率选择、盈亏结算与报表。
平今规则**只实现一次**。`PositionBook` 用 `Vec<PositionLot>` 按 `trading_day` 分桶，
取代 `broker::Position` 与 `positions::PositionState` 两套模型；
`DayBoundary` 事件驱动 `rollover`（今仓 → 昨仓）。

### 6.8 `ExecutionPort` + `SimulationPolicy` — 订单出口

```rust
// core/execution.rs

pub trait ExecutionPort: Send {
    fn submit(&mut self, order: &OrderRequest, ctx: &OrderContext) -> Result<OrderAck>;
    fn query(&mut self, order_id: &str) -> Result<OrderSnapshot>;
    fn remote_positions(&mut self, inst: &InstrumentId) -> Result<Vec<PositionLot>>;
    fn drain_fills(&mut self) -> Vec<Fill>;
    fn account_snapshot(&self) -> Option<Account>;
}

pub trait SimulationPolicy: Send + Sync {    // 唯一回测专属规则
    fn fill_timing(&self) -> FillTiming;     // ThisBarClose / NextBarOpen
    fn fill_price(&self, bar: Option<&Bar>, side: Side, trigger: f64) -> f64;
    fn can_trade(&self, at: DateTime<Utc>, session: &dyn SessionRules) -> bool;
    fn mark_price(&self, bar: &Bar) -> f64;
}
```

`SimulationExecution`（框架默认实现）串起唯一一条结算链：

```
validate(rules) → can_trade(simulation+session) → fill_price(simulation)
→ fee(rules, offset) → pnl(rules) → margin(rules)
→ account 记账 → position_rules 更新账本 → Fill { fee, realized_pnl, ... }
```

`Fill` 是唯一结算数据源（账户流水、报表 TradeRow、持仓更新都从它派生），
消除当前「盈亏算三遍」。`OrderSnapshot.fee` 明确要求实盘回执带真实手续费
（现状 `InEvent` 无 fee 字段、`FillEvent.fee` 恒 0，需要协议升级）。

### 6.9 `Exchange` — 总契约

```rust
// core/exchange.rs

pub trait Exchange: Send + Sync {
    fn id(&self) -> &ExchangeId;
    fn markets(&self) -> Vec<MarketKind>;
    fn parse_instrument(&self, raw: &str) -> Result<InstrumentId>;

    // ── 规则族：live / backtest 共用同一批 Arc ──
    // v2 审计修订（C2）：session_rules / decoder 改为 per-instrument——
    // AU/AG 夜盘 02:30 收盘与 CU 01:00 不同，全所一张窗口表/一个解码器
    // 无法表达，§2.1 痛点必须靠这两个签名收口。阶段 0 代码待同步。
    fn session_rules(&self, inst: &InstrumentId) -> Arc<dyn SessionRules>;
    fn aggregation_policy(&self, inst: &InstrumentId) -> Arc<dyn BarAggregationPolicy>;
    fn decoder(&self, inst: &InstrumentId) -> Arc<dyn BarDecoder>;
    fn subscription_plan(&self) -> Arc<dyn SubscriptionPlan>;
    fn trading_rules(&self) -> Arc<dyn TradingRules>;
    fn position_rules(&self) -> Arc<dyn PositionRules>;
    fn simulation_policy(&self) -> Arc<dyn SimulationPolicy>;

    // ── 端口工厂：唯一允许按 RunMode 分叉的地方 ──
    fn make_source(&self, req: &SourceRequest) -> Result<Box<dyn BarEventSource>>;    // 有默认
    fn make_execution(&self, req: &SourceRequest) -> Result<Box<dyn ExecutionPort>>; // 有默认
    fn build(&self, req: &SourceRequest) -> Result<ExchangeParts>;                   // 有默认
}
```

`make_source` 默认实现：回测直接回放 `req.bars`，实盘返回 Err 提示「未实现 live 行情源」。
`make_execution` 默认实现：回测 / paper 用 `SimulationExecution`，实盘返回 Err。
**交易所覆写这两个方法即可接实盘，规则代码一行都不用改。**

### 6.10 `ExchangeRegistry` — 注册表

```rust
// registry.rs

pub struct ExchangeRegistry {
    map: BTreeMap<String, Arc<dyn Exchange>>,
}

impl ExchangeRegistry {
    pub fn register(&mut self, exchange: Arc<dyn Exchange>);
    pub fn get(&self, id: &ExchangeId) -> Result<Arc<dyn Exchange>>;

    /// 批量装配：一次运行可同时包含 Binance + CTP，
    /// 每个标的一份独立的 ExchangeParts（含独立数据源与订单出口）。
    pub fn build_all(
        &self,
        requests: Vec<SourceRequest>,
    ) -> Result<BTreeMap<String, ExchangeParts>>;
}
```

---

## 7. 每个交易所的 `mod.rs` 标准模板

### 7.1 Binance（crypto 7x24）

```rust
// exchanges/binance/mod.rs

use crate::core::prelude::*;
use std::sync::Arc;

pub struct BinanceExchange {
    id: ExchangeId,
    rules: Arc<SpecDrivenTradingRules>,
}

impl BinanceExchange {
    /// 从配置构造：fee_rate、InstrumentSpec 表等
    pub fn new(fee_rate: f64) -> Self {
        let id = ExchangeId::new("binance");
        let btc = InstrumentId::perpetual(&id, "BTCUSDT");
        let spec = InstrumentSpec::crypto_perpetual(fee_rate);
        let rules = SpecDrivenTradingRules::new([(btc, spec)]);
        Self { id, rules: Arc::new(rules) }
    }
}

impl Exchange for BinanceExchange {
    fn id(&self) -> &ExchangeId { &self.id }
    fn markets(&self) -> Vec<MarketKind> { vec![MarketKind::CryptoPerpetual, MarketKind::CryptoSpot] }
    fn parse_instrument(&self, raw: &str) -> Result<InstrumentId> {
        Ok(InstrumentId::perpetual(&self.id, raw))
    }

    // 规则族：回测和实盘共用同一批 Arc
    fn session_rules(&self, _: &InstrumentId) -> Arc<dyn SessionRules> {
        Arc::new(ContinuousSession::default())            // 7x24
    }
    fn aggregation_policy(&self, _: &InstrumentId) -> Arc<dyn BarAggregationPolicy> {
        Arc::new(UtcGridPolicy)                            // UTC epoch 网格
    }
    fn decoder(&self, inst: &InstrumentId) -> Arc<dyn BarDecoder> {
        Arc::new(super::decoder::BinanceDecoder::new(inst)) // 币安 12 列数组（v2: per-instrument）
    }
    fn subscription_plan(&self) -> Arc<dyn SubscriptionPlan> {
        Arc::new(DirectSubscriptionPlan)                   // 一对一透传
    }
    fn trading_rules(&self) -> Arc<dyn TradingRules> { self.rules.clone() }
    fn position_rules(&self) -> Arc<dyn PositionRules> { Arc::new(NetPositionRules) }
    fn simulation_policy(&self) -> Arc<dyn SimulationPolicy> { Arc::new(SimpleFillModel::default()) }

    // 回测/paper 不覆写 → 默认装配即可跑
    // 实盘覆写这两个：
    fn make_source(&self, req: &SourceRequest) -> Result<Box<dyn BarEventSource>> {
        super::live_source::BinanceWsSource::build(req)
    }
    fn make_execution(&self, req: &SourceRequest) -> Result<Box<dyn ExecutionPort>> {
        super::live_gateway::BinanceGateway::build(req, &self.rules)
    }
}
```

### 7.2 CTP（国内期货）

```rust
// exchanges/ctp/mod.rs

use crate::core::prelude::*;
use std::sync::Arc;

pub struct CtpExchange {
    id: ExchangeId,
    // v2（C2）：product -> 时段表（AU 02:30 / CU 01:00 各自成表），
    // 由 contracts.yaml 的 session_opens/session_closes 驱动
    sessions: BTreeMap<String, Arc<WindowedSession>>,
    rules: Arc<SpecDrivenTradingRules>,
    main_contract: bool,
}

impl CtpExchange {
    pub fn new(session: WindowedSession, specs: Vec<(InstrumentId, InstrumentSpec)>) -> Self {
        let id = ExchangeId::new("SHFE");
        Self {
            id,
            session: Arc::new(session),
            rules: Arc::new(SpecDrivenTradingRules::new(specs)),
            main_contract: true,
        }
    }
}

impl Exchange for CtpExchange {
    fn id(&self) -> &ExchangeId { &self.id }
    fn markets(&self) -> Vec<MarketKind> { vec![MarketKind::CnFutures] }
    fn parse_instrument(&self, raw: &str) -> Result<InstrumentId> {
        // "CU2601" -> { product: "CU", contract_month: 2026-01 }
        // "CU"     -> 逻辑品种码，交给 SubscriptionPlan 解析
    }

    // v2（C2）：session 按品种装配——CtpExchange 持 product -> WindowedSession
    // 映射（contracts.yaml 的 session_opens/session_closes 驱动），
    // AU 的 02:30 与 CU 的 01:00 各查各的，不再全所一张表。
    fn session_rules(&self, inst: &InstrumentId) -> Arc<dyn SessionRules> {
        self.session_for(inst)
    }
    fn aggregation_policy(&self, _: &InstrumentId) -> Arc<dyn BarAggregationPolicy> {
        Arc::new(SessionWindowPolicy)                       // 按交易时段切桶
    }
    fn decoder(&self, inst: &InstrumentId) -> Arc<dyn BarDecoder> {
        Arc::new(super::decoder::CtpDecoder::new(inst))     // 无 is_final，靠会话边界推断（v2: per-instrument）
    }
    fn subscription_plan(&self) -> Arc<dyn SubscriptionPlan> {
        if self.main_contract {
            Arc::new(MainContractPlan::default())           // 按 OI 选主
        } else {
            Arc::new(DirectSubscriptionPlan)
        }
    }
    fn trading_rules(&self) -> Arc<dyn TradingRules> { self.rules.clone() }
    fn position_rules(&self) -> Arc<dyn PositionRules> { Arc::new(CtpPositionRules::default()) }
    fn simulation_policy(&self) -> Arc<dyn SimulationPolicy> { Arc::new(SimpleFillModel::default()) }

    // 回测/paper 不覆写 → 默认装配即可跑
    // 实盘覆写：
    fn make_source(&self, req: &SourceRequest) -> Result<Box<dyn BarEventSource>> {
        super::live_source::CtpWsSource::build(req)
    }
    fn make_execution(&self, req: &SourceRequest) -> Result<Box<dyn ExecutionPort>> {
        super::live_gateway::CtpGateway::build(req, &self.rules, &self.session)
    }
}
```

### 7.3 文件夹内其他文件

| 文件 | 何时需要 | 内容 |
|---|---|---|
| `mod.rs` | **必须** | `impl Exchange`，组装规则 |
| `decoder.rs` | 实盘 / 特殊格式 | 该交易所的 WS / API 帧解码器 |
| `live_source.rs` | 接实盘时 | WS K 线订阅适配器（覆写 `make_source`） |
| `live_gateway.rs` | 接实盘时 | 远程订单网关适配器（覆写 `make_execution`） |
| `session.rs` | 时段非标准时 | 该交易所的时段表配置逻辑（CTP 从 contracts.yaml 读） |

> 回测 / paper 只需要 `mod.rs` 一个文件。接实盘时按需加 `decoder.rs`、
> `live_source.rs`、`live_gateway.rs`。

---

## 8. 配置驱动：按交易所分派

### 当前（全局单值，无法混跑）

```yaml
services:
  kline:
    market: crypto_perpetual    # 进程全局，只能一种
  trading:
    exchange: binance           # 透传标签
```

### 优化后（按交易所分组，可混跑）

```yaml
exchanges:
  binance:
    market: crypto_perpetual
    services:
      kline: { url: "ws://..." }
      trading: { url: "http://..." }
    aggregation: utc_grid
    fee_rate: 0.0004

  ctp:
    market: cn_futures
    services:
      kline: { url: "ws://..." }
      trading: { url: "http://..." }
    aggregation: session_window
    session:
      day_cutover: "20:00"
      windows:
        - { open: "09:00", close: "10:15", kind: day }
        - { open: "10:30", close: "11:30", kind: day }
        - { open: "13:30", close: "15:00", kind: day }
        - { open: "21:00", close: "01:00", kind: night }   # 跨零点
      holidays: [2026-01-01, 2026-02-17, ...]
    subscription:
      policy: open_interest
      include_second_main: true
      exclude_delivery_month: true
    contracts:
      CU: { multiplier: 5, tick_size: 10, margin_rate: 0.11, ... }
      AU: { multiplier: 1000, tick_size: 0.02, ... }

instruments:                       # 一次运行可混跑多个交易所
  - { exchange: binance, product: BTCUSDT, strategy: template_trend }
  - { exchange: ctp,     product: CU,      role: main, strategy: template_trend }
```

### 启动流程

```
1. 读取配置
2. 对配置中出现的每个 exchange：
   a. ExchangeRegistry 查找已注册的 impl（对应 exchanges/{name}/）
   b. 注入该交易所的 services / session / contracts 等配置
   c. 构造 Exchange 实例并 register
3. 对 instruments 列表中每个标的：
   a. parse_instrument -> InstrumentId
   b. build(SourceRequest) -> ExchangeParts（独立数据源 + 订单出口 + 规则 Arc）
4. ExchangeRunner::run(parts, strategy)
```

> **v2.5 阶段 2 落地注**：本节形态已进代码，字段名有 3 处落地差异需对齐口径——
> ① 时段表键为 `timezone_offset_hours`（整小时偏移），非裸 `timezone`；
> ② `session:` 的 `windows` 条目形态 `{open, close, kind}`，kind ∈
> `day|night|after_hours`；`holidays` 为 `YYYY-MM-DD` 字符串数组；
> ③ `services:` 子树阶段 2 **接受不解析**（阶段 4 实盘网关消费），
>    **v2.7 已闭环**：`exchanges/ctp|binance` 的 `services:` 类型化为
>    `core::exchange::ServicesCfg`（kline/trading 端点 + `url`/
>    `backfill_url`/`submit_enabled` 等已知字段强类型，未知键/端点
>    flatten 进 `extra` 原样保留不静默吞）；`{Xxx}Exchange::live_services()`
>    为 live 装配消费口，未声明 = None（**不造默认地址**）。
>    回归 `tests/config_registry.rs::services_subtree_is_typed_and_preserved`；
>    bootstrap 多交易所分客户端路由随阶段 5（`Engine`/`RuntimeActor` 合并）。
> `contracts:` 键 = 品种码（走 §16 C13 品种回落）或具体合约（精确命中）。
> 装配入口：SPI `exchanges::build_registry`（组合根，core 不感知具体交易所）
> + app `config::build_exchange_registry` / `resolve_instruments`。
> **时钟口径债（v2.6 阶段 3 已闭环）**：`MainContractPlan` 生效时，
> `build()` 内用 `Utc::now()` + `req.universe` 解析选主——空快照/过月合约
> 在装配期即报错（阶段 2 测试坐实）。v2.6：`SourceRequest` 增 `as_of:
> Option<DateTime<Utc>>`，`build()` 按注入时点解析、缺省仍墙钟（live 语义
> 不变）；回归测试 `as_of_injection_makes_main_contract_backtest_assemblable`
> （历史 as_of 装配成功 + 过期快照仍如实报错）。

---

## 9. 统一主循环（无 `if mode == backtest`）

```rust
// core/runtime.rs

pub fn run(&mut self, strategy: &mut dyn StrategyPort) -> Result<RunSummary> {
    loop {
        let Some(event) = self.parts.source.next_event()? else { break; };
        match event {
            Bar(bar) => self.on_bar(bar, strategy)?,
            SessionGap(gap)   => self.state.on_gap(&gap, &self.policy),
            DayBoundary(day)  => {
                self.state.on_day_boundary(&self.policy);
                self.position.rollover(&mut self.book, day);
            }
            Roll(roll)       => self.subscription.on_roll(&roll, ...),
            Reconnected      => { /* 水位幂等补缺 */ }
        }
    }
}
```

`on_bar` 内部流水线：

1. 非 `Final` -> 只 `strategy.on_tick()`，**不聚合、不交易**
2. 交易日切换兜底 -> D1 闭合 + 今仓转昨仓
3. `state.on_closed_1m()` -> `newly_closed`
4. `SessionRules::is_no_trade_window()` 门控（K 线照常推进，只是不发动作）
5. `StrategyPort::on_closed_bars()` -> `StrategyAction::{Open, Close, Reverse, UpdateStop, Hold}`（v2，§9.1）
6. `Open` -> `TradingRules::sizing + normalize` -> `OrderRequest{Offset::Open}`
   `Close` -> `PositionRules::split_close` -> 每腿一个 `OrderRequest`
7. `ExecutionPort::submit()` -> `drain_fills()` -> 同步持仓镜像 / 汇总
8. `ExecutionPort::on_bar(&bar)` -> 盯市更新 `Account.unrealized_pnl`（v2 新增，修复点 C3：缺这一步 `equity()` 恒等于现金，与旧引擎逐分钟权益曲线静默不一致；`Fill` 是唯一结算数据源）

**没有 `if mode == backtest`**。`Engine::dispatch` 与 `RuntimeActor::drive_bar`
两套重复实现合并为这一处。

### 9.1 策略动作面与止损语义（v2 审计修订，修复点 C1 — P1）

现有动作面是全集：`core/src/signal.rs:59` 的 `Action` 含
`Open{side, price, stop_price, atr, strength}`、`Close{price, reason,
is_stop_loss, strength}`、`Reverse`、`UpdateStop{stop_price}`；止损状态机现由
`positions::PositionManager` 承载——触价盘中监控（`runtime_actor.rs:614`
`on_price(high, low)`）、冷却（`:638`）、止损价落盘（`:756`）、离场标记（`:765`）、
跟踪止盈（`:817`）、重启恢复（`:297`）。

阶段 0 的 `StrategyAction`（`runtime.rs:48-59`）只有
`Open{side, price, reason}` / `Close{price, reason}` / `Hold`，**窄于现有动作面**：
止损/反手/跟踪在 SPI 主循环没有落点，§13 阶段 3「与旧 Engine 逐笔一致（信号 ID、
成交价、手数、PnL）」的验收**在当前形状下不可达**——凡触发止损的样本必然分叉。

修订决定（二选一，推荐 a；这是 §13 迁移表必须先拍板的前置项）：

- **(a) 止损机上移**：`StrategyAction` 对齐 `Action` 全集；`ExchangeRunner`
  吸收触价/冷却/跟踪/`UpdateStop` 语义（决策上下文 `DecisionContext` 增
  `position: Option<PositionView>` 与 `cooldown`，与现有 `Ctx` 同构）；
  `positions` crate 退化为持久化镜像 + 重启恢复，不再是规则持有者。
- **(b) 双写镜像**：SPI 只做腿记账，止损留在 `PositionManager`——重新引入
  「两套持仓模型并存」，违背 §1 结构性缺失一的消除目标，仅在阶段 3 过渡期允许。

> **v2.1 落地状态**：已采用 **(a)**——`ExchangeRunner` 复用
> `positions::PositionState`（峰值/盈亏极值 `on_price`、trail、exit 字段）与
> `core::risk::RiskController`（收盘价口径兜底，镜像 `engine.rs`
> `check_risk_fallback`），不复制公式；`DecisionContext` 注入
> `position: Option<PositionView>` 与 `cooldown`（与现有 `Ctx` 同构，桥接
> 适配器零翻译损耗）；止损成交按 K 线时间记冷却日（与 `manager.rs` 同口径）。
> 回归测试：`stop_loss_fallback_update_stop_and_cooldown`、
> `reverse_flips_position_in_one_bar`。

### 9.2 数量与回测装配的注入通道（v2 审计修订，修复点 C6/C7）

- `runtime.rs:228-239` 硬编码 `leverage: 1.0`、`notional_per_order: None`——
  现有配置通道（`main.rs:347 --leverage`、`bootstrap.rs:78
  notional_per_order = spec.cash`）必须成为 `ExchangeRunner` 的构造参数
  （`SizingParams`），否则 §2.5 批评的「装配差异」在新架构里原样复刻。
  ✅ **v2.2 已落地**：`SizingParams { leverage, notional_per_order }` +
  `with_sizing_params()`，默认值保持旧语义，阶段 3 桥接按 backtest/paper/live
  各自配置注入。
- `OrderRequest.qty`/`PositionLot.qty` 全程 f64，丢失现有生产链路的
  **原始字面量数量口径**（`positions/src/state.rs:71-75` `remaining_size_raw`
  注释明写「避免 f64 往返修改精确数量」，`core/src/signal.rs:191
  size_raw`）——实盘平仓帧数量来源会被重构降级。`PositionLot` 与
  `OrderRequest` 需增 `qty_raw: Option<String>`，与已知风险 #7（Decimal）同批决策。
  ✅ **v2.2 已落地**：`qty_raw/remaining_raw` 贯通
  `OrderRequest → Fill → PositionLot → CloseLeg` 与 `PositionState`
  （`entry_size_raw/remaining_size_raw`，镜像 manager.rs 递减语义）；
  递减复用从 `PositionManager` 提取为公共的
  `strategy_positions::subtract_qty_raw`（**单一实现，不复制公式**）；
  合并腿用同引擎 Decimal 加法；不可精确分解一律置 `None`（宁缺勿伪）。
  阶段 4 契约要求：live 网关接入回报时**必须**填
  `Fill.qty_raw = filled_quantity` 原样。

---

## 10. crypto vs 国内期货差异映射

| 维度 | Binance U 本位永续 | 国内期货（SHFE 等） | 由哪个接口承担 |
|---|---|---|---|
| 交易日 | UTC 自然日 | 交易日（夜盘归次日）+ 节假日表 | `SessionRules::trading_day_of` |
| 交易时段 | 7x24 连续 | 日盘 + 夜盘 + 午休断开，跨零点 | `SessionRules::sessions_on` |
| 桶边界 | UTC epoch 整数倍 | 时段窗口内切分，午休强制断桶 | `BarAggregationPolicy` |
| D1 | UTC 00:00-24:00 | 上一交易日 21:00 -> 当日 15:00 | 同上（不再硬编码） |
| 完整 K 线 | WS `is_final` | 无 `is_final`，会话边界 + 时间推断 | `BarDecoder` |
| 回填窗口 | `now - N*tf` 墙钟 | 按交易窗口倒推，跳过休市 | `SessionRules::backfill_start` |
| 订阅 | 交易对一对一 | 主力/次主力/连续合约 + 换月 | `SubscriptionPlan` |
| 数量 | 小数、`stepSize` | **整数手**、最小 1 手 | `TradingRules::normalize_qty / sizing` |
| 价格 | `tickSize` | 最小变动价位（如 CU = 10） | `TradingRules::normalize_price` |
| 保证金 | 名义/杠杆 | 交易所保证金率 + 合约乘数 | `TradingRules::margin / sizing` |
| 手续费 | 单一费率（maker/taker） | 开 / 平 / **平今** 三档，可按手或按额 | `FeeSchedule` + `Offset` |
| 持仓 | 净仓 | 双向持仓、**今仓/昨仓** | `PositionRules` + `PositionBook` |
| 平仓 | `reduce_only` 一条腿 | 平今 / 平昨**多腿**，可配置优先级 | `PositionRules::split_close` |
| 盈亏 | `(exit-entry)*qty` | `(exit-entry)*qty*multiplier` | `TradingRules::pnl` |
| 禁交易窗口 | 无 | 开盘后 / 收盘前 N 分钟 | `SessionRules::is_no_trade_window` |
| 账户币种 | USDT | CNY | `InstrumentSpec.currency` |

---

## 11. 新增一个交易所：完整步骤

以「新增某境外期货交易所（CME）」为例：

| 步骤 | 动作 | 改动文件 |
|---|---|---|
| 1 | 新建 `exchanges/cme/` 文件夹 | 无（新建目录） |
| 2 | `ExchangeId::new("CME")` + `MarketKind::GlobalFutures` | `exchanges/cme/mod.rs` |
| 3 | 实现 `parse_instrument`：`ESZ6` -> `{ product: "ES", contract_month: ... }` | 同上 |
| 4 | 给 `SessionRules`：用 `WindowedSession` 填时区、windows、day_cutover、holidays | 同上 |
| 5 | 给 `BarAggregationPolicy`：通常复用 `SessionWindowPolicy` | 同上 |
| 6 | 给 `BarDecoder`：解析该所 WS 帧；无标记返回 `Unknown` | `exchanges/cme/decoder.rs` |
| 7 | 给 `SubscriptionPlan`：复用 `MainContractPlan` 或自实现 | `mod.rs` |
| 8 | 给 `InstrumentSpec` 表并构造 `SpecDrivenTradingRules` | `mod.rs` |
| 9 | 给 `PositionRules`：净仓复用 `NetPositionRules`，今昨仓复用 `CtpPositionRules` | `mod.rs` |
| 10 | 给 `SimulationPolicy`：填滑点、成交时点 | `mod.rs` |
| 11 | 在 `exchanges/mod.rs` 加 `pub mod cme;` | `exchanges/mod.rs`（+1 行） |
| 12 | 在配置文件加 `exchanges: { cme: { ... } }` | `config/*.yaml` |
| 13 | **回测到这里就能跑了**（`make_source` / `make_execution` 走默认） | — |
| 14 | 接实盘：加 `live_source.rs` + `live_gateway.rs`，覆写两个端口 | `exchanges/cme/` |

**不需要动 `core/` 下任何文件。不需要动 `Engine` / `CLI` / `报表` / `策略`。**

---

## 12. 测试策略

### 每个交易所文件夹内的测试

```rust
// exchanges/binance/mod.rs
#[cfg(test)]
mod tests {
    // binance 专属：UTC 网格、净仓、小数数量、单一费率
    fn d1_is_utc_midnight() { ... }
    fn close_is_reduce_only() { ... }
    fn sizing_is_notional_fraction() { ... }
}
```

```rust
// exchanges/ctp/mod.rs
#[cfg(test)]
mod tests {
    // CTP 专属：交易日、午休断桶、平今平昨、整数手
    fn d1_is_trading_day() { ... }
    fn lunch_break_splits_bucket() { ... }
    fn split_close_today_yesterday() { ... }
    fn integer_lots_and_tick_rounding() { ... }
}
```

### core/ 下的跨交易所测试

> v2.4 阶段 1 实况：本组测试同时依赖 `core` 与 `exchanges` 两层，放进
> core/ 会违反分层铁律，落在 crate 级集成测试 **`tests/cross_exchange.rs`**
> （等价、且依赖方向更干净）。`exchanges/ctp/` 专属测试 6 个、`core/`
> 单元（bar/session/aggregation/subscription）5 个，合计 20。

| 测试 | 验证 |
|---|---|
| `live_and_backtest_share_the_same_rules` | **设计第一公理**：同一 `Exchange` 的 Live / Backtest 装配拿到同一个 `TradingRules` / `SessionRules` Arc（`ptr_eq`），交易日、价格规整、手数、手续费、盈亏逐项一致；只有 `source` / `execution` 两个端口与 `mode` 标签不同 |
| `d1_bucket_differs_between_crypto_and_futures` | crypto D1 = UTC 午夜；期货 D1 = 上周五夜盘 21:00 -> 周一 15:00（交易日口径） |
| `session_window_never_spans_lunch_break` | 午休两侧 1m 不会落进同一小时桶 |
| `ctp_split_close_produces_today_and_yesterday_legs` | 平仓请求拆成 `CloseToday(2 手)` + `CloseYesterday(3 手)` |
| `integer_lots_and_tick_rounding` | 100 万权益 / 1 倍杠杆 -> 4 手；50007 -> 50010；非 tick 整数倍被拒 |
| `two_exchanges_run_same_main_loop_in_one_registry` | 一个 registry 同时装配 Binance + SHFE，各自跑通开平；crypto 平仓 `Offset::Auto`、期货 `CloseToday` 且平今费 > 开仓费 |
| `provisional_bar_never_drives_strategy` | 未闭合快照不进入聚合、不触发策略 |
| `windowed_session_backfill_skips_closed_hours` | 回填起点跨过休市，而非简单墙钟倒推 |

落地到现有仓库时建议补充：

1. **规则一致性契约测试扩展到每个新交易所**：`live_and_backtest_share_the_same_rules`
   已给出范式，新增交易所时照此写一份作为接入验收，防止有人把费率/精度写进实盘网关；
2. **回放确定性**：`VecEventSource` 两次回放字节级一致（对齐 `backtest_e2e` 的现有断言）；
3. **跨交易所命名空间**：同名 symbol 在两个 exchange 下 CSV / Store / 报告不覆盖；
   **v2.9 Store 行键面已交付**（`spi_row_key_namespace`：signal/position/stop/trade
   行键同名跨所共存不覆盖、幂等去重不削弱；混跑 runner 键前缀钉）；
   **v2.10（5c-run 合并步）CSV 数据路径与报表目录面已交付**（`csv_path` 命名空间
   布局 + 旧布局存量一次 rename 迁移；报表目录末段与实例名取 `venue_key`
   下划线形——`spi_engine_cli_parity` 导出双钉、`ctp_backtest_e2e` CLI 迁移链钉）；
4. **换月因果性**：D 日观测的 OI 只在 D+1 生效，无未来函数；
5. **旧后端 parity**：迁移期间逐笔对比旧 `Engine` 结果。
6. **止损路径 parity（v2 新增）**：双跑样本必须包含触发盘中触价止损、冷却拦截、
   `UpdateStop`、跟踪止盈、`Reverse` 的行情——只对比「正常开平」的用例会漏掉
   §9.1 的全部缺口。
7. **聚合器 parity（v2 新增）**：旧 `Feed` 与新 `AggregationState` 在 CTP 历史
   1m 数据上逐桶 diff，边界样本至少覆盖：首桶丢弃（`aggregation.rs:284` 的
   tainted 判定过严问题，C8）、午休断桶、跨零点夜盘、缺 bar 惰性闭合、
   重连幂等。

---

## 13. 迁移路径

| 阶段 | 动作 | 验收 |
|---|---|---|
| **0（当前）** | 已有 `crates/exchange-spi/` 可编译骨架（trait + presets）；不改动任何现有业务代码 | `cargo check` / `cargo fmt --check` 通过；**Linux/WSL 已实测 `cargo test` 8/8 通过、全仓测试全绿（§15 v2）**；⚠️ 合入前置：清 5 处 clippy warning——CI 门禁是 `cargo clippy --all-targets -- -D warnings`（`.github/workflows/ci.yml:25`），现状合入必红（C9）；`git diff` 仅 `Cargo.toml` + 新增文件 |
| **1（目录重构 + v2 修订落地）✅ 已完成** | ~~先落 §16 P1 修复~~（v2.1–v2.3 已全部落地）；trait 定义已移 `core/`；`presets.rs` 已拆成 `exchanges/binance/` 与 `exchanges/ctp/`；`ExchangeRegistry` 独立成 `registry.rs` | 编译通过；**测试 20/20**（11 lib + 9 集成）；P1/C6–C10 各项有对应测试；`cargo clippy --all-targets -- -D warnings` 通过；`git status` 仅 `Cargo.toml/lock` + 新增/未跟踪文件 |
| **2（配置体系）✅ 已完成（v2.5）** | `config/*.yaml` 支持 `exchanges: { name: ... }` 分组；启动时按配置装配 Registry；`session` 配置 per-product 化（C2/C12） | `binance + ctp` 可在单进程内混跑。**实测**：SPI `tests/config_registry.rs` 4/4——§8 示例配置装配双交易所、per-product 时段（AU 02:30 vs CU 01:00）、品种级 contracts 回落（C13）、**同进程双 Runner 混跑各自开平两笔成交**（期货整数手 vs crypto 小数数量，主循环零分叉）；app `tests/exchanges_config.rs` 3/3——目录模式 `_global.yaml` 合并、实例名别名解析、旧配置零回归。workspace clippy `-D warnings` 0 告警、32 套件全绿 |
| **3（接入现有引擎）✅ 已完成（v2.6）** | 适配层已落地：`bridge.rs`（`CandleSeries` 实现 `ClosedSource`、`LegacyStrategyBridge` 实现 `StrategyPort`、`spec_from_contract`）；`core/replay.rs`（`events_from_bars` 按 `SessionRules` 注入 SessionGap/DayBoundary）；`bar_to_candle` 补齐 `Candle↔Bar` 双向；app `spi_backtest.rs`（SPI 回测可选后端：`SpiBacktest::assemble/from_parts/preload/set_source/run` + CSV 读取包壳）；`SourceRequest.as_of` 解时钟债 | `backtest_e2e` 与 `ctp_backtest_e2e` 的结果逐笔一致（信号 ID、成交价、手数、PnL），**含止损/反手/跟踪样本（§12-6）**。**实测**（`tests/spi_dual_run_parity.rs` 4/4）：crypto 全生命周期 6 笔逐笔一致（止损/反手/`UpdateStop`/冷却拦截/重放确定性）；CTP 8 笔**含手续费在内逐笔相等**（平今×3 + 平昨×1 拆腿口径钉死，真实 contracts.yaml 直连）；聚合器 §12-7 逐桶 diff（M1/D1/夜盘 H1 一致、缺 bar 惰性闭合同刻、水位幂等，日盘 H1 差异钉基线）；双跑抓出并修复旧链路反手吞单缺陷（§16 C15）+ SPI 侧六处对齐缺口（§16 C16）。注：原行的 `SignalIntent↔OrderRequest` 意图层桥按 §17 属「ExecutionPort→(SignalManager+AsyncOrderGateway) 桥」阶段 4 隐藏工作量，双跑路径不经信号管道，未在本阶段虚报 |
| **4（实盘适配）✅ 已完成（v2.7）** | `exchanges/live.rs`（§17 点名的隐藏工作量：`LiveExecution`＝`ExecutionPort→(SignalManager+AsyncOrderGateway)` 桥、`ChannelBarSource`＝WS→`BarEventSource` 桥、`LiveOrderUpdate`/`expected_fee`）；`exchanges/binance/live_source.rs`+`live_gateway.rs`、`exchanges/ctp/live_source.rs`+`live_gateway.rs`（§7.3 模板落位，两家同套原语薄装配——emit_offset 差异仅一个构造参数）；`Exchange::LivePortFactory` 注入钩子（`with_live_factory` 覆写两端口工厂，规则零改动）；`wire.rs` 增 `offset`（出帧，None 时逐字节旧形态）与 `fee`（入帧，数字/字符串双口径）；`SignalIntent.offset` 过渡字符串字段（serde default 旧行兼容）；`OrderUpdate.fee`/`OrderSnapshot.fee`/`handle_order_update_fee`/`RuntimeCommand::OrderUpdate(Box<OrderUpdateCmd>)` 全链 fee 通道；`services:` 子树类型化（`ServicesCfg`/`EndpointCfg`，未知键 flatten 不丢，§8 注③债务闭环）；C5 遗留出口 `ExchangeRunner::subscriptions()`（重订阅差集数据面） | **实盘订单帧带平今/平昨；`FillEvent.fee` 不再恒 0；契约测试校验回测/实盘费率同源。实测**：`broker/tests/live_offset_fee_frames.rs` 4/4（进程内脚本 WS：`close_today` 落帧+数量逐字 raw+FILLED 回传 fee→`OrderUpdate.fee`；`offset=None` 旧帧十键逐字节零回归；`get_order` fee 通道）；`exchange-spi/tests/live_bridge.rs` 7/7（CTP 开→平今意图标签/fee 真值/`qty_raw` §14-13 契约/镜像账本；重启回退读持久化 `offset` 标签；重复 id 显式拒；crypto 不发 offset 且 stop 自标记；live/backtest 装配 `Arc::ptr_eq` 首公理双所立住——逼出并修复 C17；Roll 后订阅集出口）；`app/tests/spi_live_fee_source.rs`（规则==SPI 回测 fill==paper 内核 ReadyFill==桥 expected_fee 四源同数）；signals `order_update_fee_lands_in_fill_event…`（Some→真值、None→0.0 兜底双路）。**外部前置仍成立**（§14-3）：模板侧协议面已就绪，trading-service 服务端需接受 `offset` 并回传 `fee`——未升级服务端下行为逐字节不变 |
| **5（全量迁移）🚧 5a/5c-行键/5c-run合并/5b 已完成（v2.11）** | **5a（v2.8）**：`ExchangeRunner::on_event/finish/account` 步进 API（run 变薄循环，编排层可插事件间——live actor 的结构前提）；`SpiBacktest::step/export_merged_klines`；`spi_backtest::run_from_spec`（spec→Exchange 合成 `exchange_for_spec`、warmup preload、逐根步进 + `EquitySink` 同刻采样、**`Fill→TradeRow` 派生**）；CLI `backtest --engine [old\|spi]`（缺省 old 零回归；spi 分支与旧分支共用 `ReportGenerator`）；app `tests/spi_engine_cli_parity.rs` 3/3——**旧 Engine 与 ExchangeRunner 交易行逐笔全字段一致**（豁免面=差异表 #2 signal_id + comment 口径如实登记）、CTP 整数手+平今费率从 contracts.yaml 真源推得、**单进程 binance+ctp 混跑**（USDT vs CNY、小数 vs 整数手互不污染）。**5c-行键（v2.9）**：**行键命名空间化一次迁完**——格式单一源 `namespaced_key_of`（§6.1，`InstrumentId::namespaced_key()` 委托其，双链不分叉）；旧 CLI 链 `IntentCtx` 新增 `venue_key` 进 signal_id 哈希（engine/runtime_actor/bootstrap 由 `spi_backtest::venue_key_for` 按 exchange+contract 派生；`SignalIntent.symbol` 保持裸挂牌代码——wire 帧十键旧形态零回归，§6.1 两角色分离）；position/stop/trade 派生行键随源键；SPI 链 runner id 本由 namespaced_key 派生，混跑前缀现已钉死。**迁移口径=零重写**：行键为随行持久化的不透明 ID，旧格式行与新行同表共存、按落盘 id 恢复（`tests/spi_row_key_namespace.rs` 3/3 钉：同名 symbol 跨交易所行互不覆盖且幂等 AlreadyExists 不削弱、存量行共存、§6.1 双链同值+例串逐字）。**5c-run合并（v2.10，本轮）**：**run 多 symbol 走 registry 路由 + §6.1 路径面一次迁完**——`SymbolSpec.venue_key` 由 resolver 单点派生（未声明注册表→按配置 key 原样、零回归；声明 `exchanges:`→**取注册 id 解析**：配置 key `ctp`→`SHFE` 与 SPI 链 `namespaced_key()` 同值，非法实例名 fail-fast）；CSV 布局 `{tf}/{exchange}/{market}/{SYMBOL}_{tf}.csv`（`csv_path` 单一源；`resolve_csv_path` 对旧布局存量**一次 rename 迁移**，零下载零复制——与行键「零重写」同口径；裸 symbol 输入退化旧布局=未升级调用者兼容位）；报表目录末段与**实例名**取 `venue_key` 下划线形（`bootstrap::runtime_name` 单源化、main/bin 共用——Cooldown 行键=实例名，同名裸挂牌代码跨所互斥，§15 positions 面对齐项落地）；Download/warmup/paper 本地 CSV 写面同键；`OrderRouter` 精确路由键=order_id 已随 v2.9 行键天然命名空间化（扇出键=策略类型，误由 Actor 按 id 自查兜底）；出单帧十键逐字节不变（`wire.rs:8`：身份在 WS 握手绑定、不在帧内——实例名变化=握手面跨仓对齐点，钉于迁移注记）。**5b（v2.11，本轮）**：**live 编排合并——`spi_live_actor` 落地，RuntimeActor 职责逐项搬迁（模块头对照表）**：内核 = `ExchangeRunner::on_event`（与回测同一段主循环）+ `LiveExecution` 桥（同一 `SignalManager` 内核，B2 async 外形 `take_submit_requests→pump_submit→apply_submit_outcome`）+ `ChannelBarSource`（§14-2 邮箱形；交棒水位预闸保证异步上下文不阻塞自旋的不变量）+ `LegacyStrategyBridge`（策略零翻译）；T7/`RouteBind`/`RuntimeHandle`/`RuntimeCommand` **命令面原样复用**——OrderRouter/关停代码零改动（`RuntimeHandle::from_command_tx` 缝）；retry（`due_pump`）/在途看门狗（`PollWatch` 复用）/启动与重连对账（`recover` 先查后发 + 交叉对账）/ReduceOnly 外部平仓/远程仓位对账（fail-open/`deleted` 行语义不动，`remote_matches_local` 共用）/S6 flush/预热与重连补缺（`load_warmup`/`backfill` 同一批函数）逐项搬迁；仓位内核 = **同一份** `PositionManager`（文末原则 3），runner 经 `seed_position` 消费决策视图（零公式分叉；成交跨 bar 到达经 `sync_book` 同步拆腿视图——5b 实测发现并修复的时序缺陷）；线程模型 = 每实例专用 OS 线程（runner/策略 `!Send` 的家）+ 线程内 current-thread runtime（B2 合规：非共享 runtime 内 block_on），监督语义与 `spawn_supervised` 同形（panic→整代重建+退避）。新增 SPI 缝（纯增零回归）：`ExecutionPort::as_any_mut`（downcast 缝，缺省 None）/`ExecutionPort::venue_sized`（场所侧换算量声明——**真缺陷发现**：live 无账户快照时 equity=0 会误杀全部开仓，现占位量过闸门、出帧 intent.size=0 形状不变）/`ExchangeRunner::seed_position`/`take_open_meta`/`LiveExecution::absorb_ready_fill`（paper ReadyFill 统一门）。CLI `run --engine [old|spi]`（缺省 old 零回归；与回测开关同形，bootstrap 网关/客户端/Store 装配共用）。验收 `tests/spi_live_merge.rs` 6/6：生命周期（开→成交→平→历史，含 T7 bind 与重复帧幂等钉）/retry 同键幂等重提/重启对账 recover→Query 补成交（同目录全新 Store 实例只认盘上快照）/CTP offset 帧（`open`/`close_today` 标签 + §14-13 数量字面量全链传播 + 今昨判定「入册按回报 event_time」双链同口径钉）/paper ReadyFill 统一落地/线程端到端跨实例不串台。**§14-3 边界钉**：模板侧只保证**进程内**正确性——真交易所（CTP 柜台/币安 API）在场验收需外部服务，`--engine spi` 缺省路切换必须在场验收之后，本轮不提前。**遗留**：**删除收口（缺省路切换后）**——5b 交付后旧 `RuntimeActor` 仍是**缺省路**（`--engine old` 零回归）：`Feed`/`BarMergeMode`/`ctp_session` 仍被旧链宿主（live/paper 聚合面 + `remote_client::cst()` 时区 + parity 测试窗锚）；`BacktestBroker` 为**两链共用 paper 撮合内核**（5b paper 面走同一 `make_gateway` 工厂——删除会同时伤 SPI paper 面）；`mock` 为 `examples/mock_server.rs`（smoking 冒烟工具）宿主。删除顺序钉死为：真交易所在场验收（§14-3）→ `--engine spi` 缺省切换 → 旧链退役（`live_orchestrator` 套件迁 SPI 面或重写）→ 删除收口；强拆=伤功能，不提前 | 阶段 5 验收「两模式共用主循环；单进程多交易所混跑」——**回测侧已逐笔坐实**；**live/paper 侧 v2.11 进程内坐实**（`spi_live_merge` 6/6，与回测同一段 `ExchangeRunner` 主循环）；行键跨交易所不覆盖已钉（§14-3 Store 面）；**§6.1 路径面（CSV/报表目录/实例名）v2.10 已钉**；**尾距=真交易所在场验收（§14-3 外部服务；验收程序已成文 `docs/exchange-spi-live-acceptance.md`，勾项表+日志锚+证据包清单齐备）→ 缺省路切换 → 删除收口（退役/收口方案已成文 `docs/exchange-spi-retirement.md`，R1–R4/D1–D3 拓扑序+19 案迁移映射+golden 冻结前置+断言账本铁律齐备）**——在场前不勾全 ✅，不做「工牌还亮着就发到家了」 |

**收口注记（2026-11-09，不改写上方各阶段历史行）**：阶段 5 的收尾腿已在
当前工作区执行——`crates/app/src/{engine.rs,runtime_actor.rs}` 已删、
`main.rs` 的 `--engine` 选项**整体移除**（并非本方案 §15 终局验收所要求的
「选项保留 + `old` 显式报错」），`bootstrap.rs:89` 注明「统一走 engine」。
实测状态：`cargo test --workspace` = **531 passed / 39 套件 / 0 failed / 3 ignored**
（exit 0）；`cargo clippy --all-targets -- -D warnings` = **0 error**（本收口清 4 处
lint）；`cargo fmt --all -- --check` = **仍报 62 处 diff（engine 33 / app 26 /
signals 2 / store 1），未处理**；
`exchange-spi-live-acceptance.md` 在场验收前置 **20 项 0 勾**。
命名口径见 §4 收口注记（实装为 `crates/engine`）。逐项登记与残差计数见
`docs/exchange-spi-retirement.md` §5.1。

**可以保持不变（v2 修订清单）**：`Strategy` / `ClosedSource` / `Ctx`（注意
`Ctx` 是 struct）、`SignalManager` 状态机（retry/recover/in-flight 逻辑不动，
但行键随 `InstrumentId` 命名空间化）、`OrderGateway` / `AsyncOrderGateway`
的方法集合、`Store` 持久化机制与 `Clock`、`registry`（build.rs 自动发现）、
`Candle` 的读取兼容（通过 `From`）、现有 `config/contracts.yaml` 数据
（映射到 `InstrumentSpec`）。

**不再列入「保持不变」（v2 勘误）**：`positions::PositionManager`——它不是
纯仓位账本，而是触价止损 / 冷却 / 跟踪止盈 / 重启恢复状态机的宿主
（`runtime_actor.rs:614/638/756/765/817`），必须随 §9.1 决策一并迁移；
`data::Feed` / `remote_client::MarketKind` / `ctp_session` 由 SPI 端口
替代后在阶段 5 删除（爆炸半径见 §17）。

**`SignalIntent` 迁移策略（v2，§17）**：给 `SignalIntent` 加
exchange/market/offset 字段是全仓涟漪源（wire 帧 / Store 行 schema /
`ManagedSignal`/`ManagedPosition` 等 Model impl / 4 个 e2e 测试夹具）。
建议阶段 3 用「命名空间字符串塞进现有 `symbol` 字段」作过渡
（`binance/crypto_perpetual/BTCUSDT`），schema 迁移集中压到阶段 5 一次
做完——避免两次迁移，也避免双跑期新旧路径因 schema 分叉无法逐笔对比。

> **v2.9 交付注记（5c 行键）**：这次「一次迁完」落地为「**一次格式变更 +
> 不透明 ID 承接**」：`SignalIntent` 未加 exchange/market 字段（`symbol`
> 仍是裸挂牌代码，wire 帧旧形态零回归），命名空间经 `IntentCtx.venue_key`
> 进 signal_id 哈希；存量行按落盘 id 恢复，无数据重写（`spi_row_key_namespace`
> 的共存案钉死）。**跨语言契约注记**：`generate_signal_id` 与 Python 的
> 「同输入同输出」在函数级逐字节未动（黄金值仍钉），但调用方喂入已从裸
> symbol 换为命名空间键——Python 侧对齐需按同约定喂 `{exchange}/{market}/{symbol}`，
> 登记为跨仓对齐点（sidecar/trading-service 若复算 ID 须同步此口径）。

> **v2.10 交付注记（5c-run 合并：路径面 + registry 路由）**：两个跨仓对齐点，
> 均为设计要求的显式变更、非静默破坏——① **CSV 目录布局**：
> `{tf}/{SYMBOL}_{tf}.csv`（Python 镜像）→ `{tf}/{exchange}/{market}/{SYMBOL}_{tf}.csv`，
> 模板对旧布局存量**首次访问自动一次 rename**（`resolve_csv_path`，零下载零复制）；
> 直接读该目录的外部 Python 工具链需适配新布局（或先经模板任一次读取触发迁移）。
> ② **实例名（WS 握手绑定身份，`wire.rs:8` 身份不在出单帧内）**：symbol 段换
> `venue_key` 下划线形（`sar_8H_3_binance_crypto_perpetual_BTCUSDT_LIVE`）——
> 服务端若按 strategy_name 做人工/脚本匹配须同步此口径；place_order 出单帧
> 十键逐字节旧形态不变（§6.1 两角色分离，`live_offset_fee_frames` 零回归钉未动）。
> ③ **registry 路由口径**：声明 `exchanges:` 的配置，命名空间键取**注册 id**
> （配置实例名 `ctp` 归一为 `SHFE`），与 SPI 链 `InstrumentId::namespaced_key()`
> 双链同值（`venue_key_single_source…` + `venue_key_derives_from_exchange_and_contract`
> 钉）；未声明的旧单交易所配置按配置 key 原样派生，零回归。

> **v2.11 交付注记（5b live 编排合并：`spi_live_actor`）**：三个口径对齐点，
> 均无新跨仓面（出单帧十键、`SignalManager` 状态机、Store schema 全部不动）——
> ① **缺省路不变**：`run --engine` 缺省 `old`，SPI live 编排为 opt-in；
> 缺省切换必须等真交易所在场验收（§14-3 外部服务）之后，本轮不提前交作业。
> ② **live 开仓量口径**：`venue_sized` 出口旁路本地 sizing（equity 模板侧无
> 权威不伪造）；出帧 intent.size=0 / order_cash 换算形状不变——真实量以
> 成交回报为准（镜像账本与仓位内核吃回报值）；paper 面同走网关内核量
> （ReadyFill 统一门 `apply_ready_fill→absorb_ready_fill→process_fills`）。
> ③ **CTP 今昨判定**双链同口径：入册按回报 `event_time` 判 `is_today`、
> 窗口判定按 K 线时间——旧链如此，SPI 链如实保持（回报跨交易日入册落
> 昨仓），`spi_live_merge` 以测试钉死而非文档口头。线程模型决策：runner/
> 策略 `!Send` + B2 禁 runtime 内 block_on → 每实例专用 OS 线程持自有
> current-thread runtime（网关 await 发生在线程自己的 runtime 里，合规）。

### 阶段 3 双跑差异表（v2.6，`tests/spi_dual_run_parity.rs` 逐条断言钉死）

| # | 差异 | 旧路径 | SPI 路径 | 处置 |
|---|---|---|---|---|
| 1 | crypto 平仓手续费基数 | `entry_price×qty×rate`（backtest.rs 兼容口径） | `fill_price×qty×rate`（TradingRules 单一口径） | 保留 SPI 口径（§2.5 消除的重复之一）；测试逐笔断言差值公式 `(entry−exit)×qty×rate` |
| 2 | signal_id 方案 | `generate_signal_id(平 symbol)` | `sig_{namespaced}_{ts}_{seq}` | §13 过渡设计使然；以「两侧各自确定性 + SPI 重放逐项相等」承接验收；**v2.9（5c）行键命名空间口径统一**——两侧 ID 均由 namespaced key 派生（旧链 `venue_key` 进哈希、SPI 链 runner 前缀钉 `spi_engine_cli_parity`/`spi_row_key_namespace`）；跨路径**逐字节相等豁免保留**：SPI 平仓 `_legN` 按腿拆单使每腿键天然异于旧路径单腿意图，非口径缺陷，钉于本表 |
| 3 | 日盘 H1 桶锚 | 交易时段窗口段（9:00–10:15/…，旧 ctp_session 表） | 窗口内 tf 分钟网格（9:00/10:00/…截窗） | §2.1 痛点修复的设计后果；聚合器测试两侧锚点各钉基线；夜盘 H1/D1/M1 逐桶一致 |
| 4 | 持仓中重复 `Open` | broker 静默丢弃（单仓模型） | `PositionBook` 加仓 | 双跑样本用 `Reverse` 不开该路径；阶段 5 统一单仓语义前保持登记 |
| 5 | 大周期 `quote_volume` | 取桶末根 | 桥接取桶首根 | 策略不消费（报表字段）；阶段 4 报表派生时对齐 |
| 6 | crypto D1 | 旧 Feed 恒走 CTP 交易日桶（污染 crypto 日线） | `UtcGridPolicy` UTC 午夜 | 已知缺陷修复方向（`d1_bucket_differs…` 已证）；crypto 样本不含 D1 事件 |

**旧链路缺陷修复（C15）**：`BacktestOrderClient::intent_to_action` 曾把
`IntentKind::Reverse` 的平仓/开仓两条意图一律映射为 `Action::Open`——
反手在旧回测链路**永不成交**（平仓单被「已有持仓」吞、开仓单又被旧仓吞），
存量 `backtest_e2e` 从不产生 Reverse 故零暴露。双跑反手样本逼出后修复
（size>0 ⇒ Close），`engine.rs` 保持零改动。

---

## 14. 已知风险与待决策项

| # | 事项 | 说明 / 建议 |
|---|---|---|
| 1 | `dyn` 虚调用开销 | 每根 1m 调用 `bucket_of` 等数次虚方法，相对指标计算可忽略；若 profiling 显示热点，可对 `BarAggregationPolicy` 改泛型 + enum dispatch |
| 2 | `BarEventSource` 是同步拉取 | 实盘 WS 是异步流，适配器内部用 tokio channel `blocking_recv()` 或 Actor 邮箱桥接；现有 `RuntimeActor` 本来就是逐条同步处理 WS 事件，形状吻合 |
| 3 | `trading-service` 协议升级 | 阶段 4 必须让服务端接受 `offset` 并回传 `fee`；否则「规则单一来源」只能覆盖模板侧，实盘仍会分叉。这是**外部依赖的前置条件**。**v2.7 模板侧已就绪**：出帧 `offset`（None 时字段不出现、旧帧逐字节不变）、入帧 `fee`（缺失容忍）；服务端升级前后模板行为兼容，`live_offset_fee_frames.rs` 已把两侧形态都钉死 |
| 4 | 交易日历数据源 | 现 `trading_calendar.rs` 内嵌 2024-2027 节假日表且有 `TODO(maintenance)`；`WindowedSession.holidays` 应改为可注入（配置 / 外部日历服务） |
| 5 | 保证金是否冻结 | 现状 `sizing_lots` 只做上限约束，不冻结/释放现金；SPI 的 `Account.margin_used` 已记账但是否影响 `sizing` 的 `equity` 需产品决策 |
| 6 | 结算价 / 盯市 | 期货按结算价盯市、crypto 按标记价；当前 `SimulationPolicy::mark_price` 默认 `bar.close`，期货实现应覆写 |
| 7 | f64 金额 | 生产建议 `rust_decimal::Decimal`；SPI 为降低接入成本暂沿用现有 f64 口径 |
| 8 | 换月复权 | `NoAdjustContinuousSeries` 与现状一致（价格断档）；`Ratio` / `Difference` 需与策略口径共同确认 |
| 9 | 多账户 / 多币种资金池 | `ExchangeRunner` 目前一个标的一个 `Account`；跨交易所共享资金池需在上层加 `Portfolio` 聚合 |
| 10 | `NextBarOpen` 成交时点 | `SimulationPolicy` 已声明，但 `ExchangeRunner` 目前只用 `ThisBarClose`；实现挂起队列时需与 `pending_actions` 的旧语义明确切割 |
| 11 | **止损机归属决策（v2 新增，阻塞级）** | 触价止损/冷却/跟踪现由 `positions::PositionManager` 承载，SPI 无对应语义。§9.1 二选一（推荐上移进 `ExchangeRunner`）。**不拍板则阶段 3 逐笔一致验收不可达**（§16 C1） |
| 12 | **交割月/换月 per-product 差异（v2 新增）** | CFFEX 第三周五交割、SHFE 有色交割月中旬可交易、DCE 农产品又不同；`MainContractPlan` 的一刀切剔除不可作缺省，须下沉为交易所层可覆写规则（§16 C12） |
| 13 | **数量原始字面量口径（v2 新增，关联 #7）** | 生产平仓帧数量依赖 `size_raw`/`remaining_size_raw` 字符串口径防 f64 往返（`positions/src/state.rs:71-75`）；SPI 纯 f64 会降级实盘帧精度，`PositionLot`/`OrderRequest` 需补 `qty_raw`（§16 C7）。**v2.2 模板侧已闭环**；**v2.7 阶段 4 全部闭环**：live 网关以回报 `filled_quantity` 原样填 `Fill.qty_raw`——契约测试 `ctp_open_then_close_today_intent_fee_and_raw_chain`（桥侧逐字）+ `close_today_frame_carries_offset_and_fee_returns`（帧侧逐字）+ `restart_fallback_rebuilds_offset_from_stored_intent`（重启回退不丢） |

---

## 15. 交付物与验证状态

### 当前交付物（阶段 0）

| 文件 | 内容 |
|---|---|
| `crates/exchange-spi/` | 13 个模块文件（trait 定义 + 通用实现 + 参考实现），共 ~4140 行代码 |
| `crates/exchange-spi/src/lib.rs` | 模块地图、设计公理、最小实现集说明、prelude |
| `Cargo.toml` / `Cargo.lock` | 仅新增 workspace member 与内部依赖两行及对应 lock 条目，未改动任何现有配置 |
| `docs/exchange-spi-design.md` | 本文 |

### 已通过

（v2：原「8 个测试仅类型检查、从未实际运行」的欠账已在 Linux/WSL 补跑闭环，2026-11-05。）

| 检查 | 环境 | 结果 |
|---|---|---|
| `cargo test -p strategy-exchange-spi` | Linux/WSL (rustc 1.98.1, gcc) | **v2.7：39 passed / 0 failed**（v2.6 的 31 + `config_registry` services 类型化 1 + 新套件 `tests/live_bridge.rs` 桥契约 7）；**v2.11：+4 处纯增缝（`as_any_mut`/`venue_sized` 缺省方法、`seed_position`/`take_open_meta`、`absorb_ready_fill`），39 passed / 0 failed 原样不变** |
| `cargo test -p strategy-broker` | Linux/WSL | **v2.7：lib 34 + `async_gateway_e2e` 4 + 新套件 `live_offset_fee_frames` 4，0 failed**（wire offset/fee 形态、帧级平今落帧与 fee 回流、旧帧十键零回归钉） |
| `cargo test -p strategy-signals` | Linux/WSL | **v2.7：51 passed / 0 failed**（+`order_update_fee_lands_in_fill_event_when_server_reports_it`：Some→真值 / None→0 双路） |
| `cargo test -p strategy-app` | Linux/WSL | **v2.11：新套件 `spi_live_merge` 6/6——内核步进（生命周期/T7 bind/retry 同键幂等/重启对账 Query 补成交/CTP offset 帧+§14-13 字面量全链/paper ReadyFill 统一门）+ 线程端到端跨实例不串台；`run --engine [old|spi]` 分流（缺省 old 零回归）**；v2.10 原案全绿：config venue/registry 路由钉 +3（派生单源含注册 id 归一 `ctp`→`SHFE`、非法实例 fail-fast+旧配置零回归、多 symbol 分流）；`ctp_backtest_e2e` CLI 迁移链钉（旧布局写→首读一次 rename→导出新布局真值+旧位置不残留）；`spi_engine_cli_parity` 导出双钉（命名空间真值+旧平铺不再产出）；bootstrap `runtime_name` 跨所互斥钉（U10 案强化钉）**；v2.9 全部原案仍绿：`spi_row_key_namespace` 3/3、`spi_engine_cli_parity` 混跑键前缀钉 3/3、`live_orchestrator` 19/19、`spi_dual_run_parity` 4/4、`spi_live_fee_source` 1/1、`backtest_paper_parity` 5/5、`backtest_e2e` 3/3（逐笔对比面 id 豁免未扩——口径仍钉差异表 #2） |
| `cargo test -p strategy-core --lib` | Linux/WSL | **v2.9：+2 行键命名空间钉（跨所互异/确定性/旧口径回归 pin；`intents_from_action` 双角色案）；Python 黄金值逐字节未动仍过** |
| `cargo test --workspace` | Linux/WSL | **v2.11：39 套件 result:ok / 531 passed / 0 failed**（v2.10 的 525 + 5b 验收 `spi_live_merge` 6；`strategy-data` lib 72 原钉保留） |
| `cargo clippy --workspace --all-targets -- -D warnings` | Linux/WSL | **v2.11：0 告警**（本轮曾拦下真实 lint：rustdoc 列表续行缩进——非样式洁癖，doc 面承重项） |
| `cargo check --workspace` | Linux/WSL | 通过（`ring` 有 gcc 即可构建，坐实下表「与本次改动无关」的判断） |
| `cargo check -p strategy-exchange-spi [--tests]` | Windows GNU | 0 error / 0 warning |
| `cargo fmt --all -- --check` | Linux/WSL | 无差异（v2.11 阶段 5b 合并全部新改文件纳入） |

### 待处理（v2 新增）

| 检查 | 结果 | 处置 |
|---|---|---|
| `cargo clippy -p strategy-exchange-spi --all-targets` | ✅ v2.1：**`-D warnings` 门禁 0 告警通过**（原 5 warning 清零：is_multiple_of×2、doc 缩进、while-let、derivable Default） | 已闭环（C9） |

### Windows 本机环境限制（Linux/CI 实测已可全量验证，v2 口径更新）

| 命令 | 现象 | 根因 |
|---|---|---|
| `cargo test -p strategy-exchange-spi` | 测试二进制可编译，**无法链接执行** | Windows GNU target：rustc 用绝对路径调用 `dlltool.exe` 生成 `kernel32.dll` 导入库，该工具需要 GNU `as.exe` 子进程，本机缺失。换 `lld` 无效，因为 dlltool 由 rustc 直接调用，与 linker 选择无关 |
| 同上（改用 MSVC target + `lld-link`） | 缺 `kernel32.lib` / `ntdll.lib` / `ws2_32.lib` / `dbghelp.lib` | 本机只有 Visual Studio Installer，没有 Windows SDK 导入库与 C++ Build Tools |
| `cargo check --workspace` | `cc-rs: failed to find tool "gcc.exe"` | `ring v0.17.14` 的 build script 要编译 C 代码；依赖链 `strategy-data -> reqwest -> rustls -> ring`。这是**既有工程**的本地构建前提，新 crate 不依赖 `ring` |

补充证据：`.cargo/bin/` 下所有 `*.exe` 均为 0 字节无效 shim（含 `cargo.exe` / `cargo-fmt.exe`），
因此必须显式调用真实 toolchain 目录下的二进制；`cargo fmt` 之前的 `os error 193`
就是这么来的，现已绕过。

### 在完整环境复现

装好 MSVC Build Tools（或完整 MinGW）的开发机 / CI 上：

```bash
cargo fmt -p strategy-exchange-spi -- --check
cargo check -p strategy-exchange-spi --tests
cargo test  -p strategy-exchange-spi
cargo check --workspace
```

若同样遇到 shim 问题，显式指定真实 toolchain（本机验证用）：

```bash
export RUSTUP_TOOLCHAIN=1.99.0-x86_64-pc-windows-gnu
export TC=/c/Users/qiupe/.rustup/toolchains/1.99.0-x86_64-pc-windows-gnu
export PATH="$TC/bin:$TC/lib/rustlib/x86_64-pc-windows-gnu/bin/self-contained:$PATH"
"$TC/bin/cargo-fmt.exe" fmt -p strategy-exchange-spi
cargo check -p strategy-exchange-spi --tests
```


### 原则
1.交易所对应框架来说是统一的，如要加接口，每个交易所理论上都应该新增
2.每个交易所都有自己的处理方式，如果K线， 期货自己的处理逻辑可以再自己交易所文件夹下实现，如选取主力合约和次主力合约
3.发送信号和仓位维护理论上都是同一套代码

> **v2.5 阶段 2 对三条原则的实况核验**（证据：`cargo test -p strategy-exchange-spi`
> 25/25，其中 `tests/config_registry.rs` 4 项全部以此为断言对象）：
>
> - **原则 1（接口统一）**✅ 有机器验证——§8 示例配置同时装配
>   `binance + ctp`，两者走同一 `Exchange` trait、同一
>   `ExchangeRunner::run` 主循环；若未来给 `Exchange` 加方法，
>   缺省实现兜底，但测试矩阵会立刻暴露「只改一家」的偏科。
> - **原则 2（特有逻辑进交易所文件夹）**✅ 阶段 2 再进一步——
>   配置形态按交易所归位：`exchanges/binance/config.rs`（fee_rate/
>   symbols 缺省）、`exchanges/ctp/config.rs`（session per-product/
>   contracts/subscription 选主参数）；`core/` 只留通用输入形态
>   `SessionCfg`（境外期货同样可复用）。选主/次主力的策略参数
>   （`include_second_main`、`exclude_delivery_month`）已可 per-exchange 配置。
> - **原则 3（信号与仓位同一套代码）**✅ 混跑承重验证——配置装配的
>   双交易所各自跑通「开多→平仓」两笔成交：CTP 侧全程整数手 +
>   tick 规整 + 平今费率表，binance 侧小数数量 + 比例费率，
>   **主循环代码零分叉**（差异只在注入的规则表），即
>   `binance_and_ctp_mixed_run_in_one_process` 断言的全部内容。
>   **v2.6 阶段 3 再加码**：同一份 `Strategy` 源码零改码经
>   `LegacyStrategyBridge` 跑进新旧两条引擎（`spi_dual_run_parity`
>   crypto/CTP 逐笔一致）——「信号同一套」从命题变成断言；
>   仓位侧两侧共用 `positions::PositionState`/`core::risk::RiskController`
>   （止损/冷却/回落跟踪逐笔同结果），持仓账本同源 `Fill`（C3/C4 口径）。
>   **v2.7 阶段 4 再闭环（信号出口）**：SPI live 桥 `LiveExecution`
>   复用**同一个** `SignalManager` 状态机（retry/在途约束/落盘零复制，
>   §17「信号与仓位同一套」的出口面）；CTP 平今/平昨由 `PositionRules`
>   拆腿直达 `wire` 帧 `offset`，手续费四源同数
>   （规则==SPI 回测==paper 内核==桥 expected_fee）——回测/实盘
>   从「两套费率口径人工对齐」变成同一 `Arc<dyn TradingRules>` 出数。

---

## 16. 审计修改点台账（v2）

> 2026-11-05 全量审计：§2 六痛点 30+ 论断逐条核验（绝大多数行号级属实，
> 8 处引用偏差已在原文就地勘误）；`crates/exchange-spi/` 4140 行逐文件精读。
> 下表为**阶段 0 代码**的修改点台账，按「不改会怎样」分级。
>
> **v2.1 状态（2026-11-05）**：P1 五项（C1/C2/C3/C4/C11）已落进代码，
> C9（clippy）清零、C5 顺带回写订阅集；`cargo test -p strategy-exchange-spi`
> **15/15 通过**（原 8 + 新增 7 个 P1 回归测试）、
> `cargo clippy --all-targets -- -D warnings` 0 告警。
>
> **v2.2 状态（2026-11-05）**：C6/C7 落地——`SizingParams` 注入通道 +
> `qty_raw` 字面量链（`OrderRequest → Fill → PositionLot/PositionState → CloseLeg`，
> 递减复用 `strategy_positions::subtract_qty_raw` 单一实现）；测试 **18/18**。
>
> **v2.3 状态（2026-11-05）**：C8/C10 落地——首桶 tainted 需「桶中间接入」才标污；
> `infer_finality` 三分支定版（边界即 Final / 严格越过 Final / 非边界整点 Provisional）。
> 测试 **20/20**（回归 9 个），workspace 全量 0 failed。
> 剩余待办：C12（交割月 per-product，需各所规则拍板后下沉交易所层）。

| # | 级别 | 修改点 | 证据（阶段 0 代码） | 修法 |
|---|---|---|---|---|
| C1 | **P1** | `StrategyAction` 窄于现有 `Action`：缺 `stop_price/atr/strength/Reverse/UpdateStop/is_stop_loss`，止损机在 SPI 无落点 | `runtime.rs:48-59` vs `core/src/signal.rs:59-80`；止损机实位于 `runtime_actor.rs:614/638/756/765/817` | ✅ **v2.1 已落地**：动作面全集 + 止损机按 §9.1(a) 上移（复用 `PositionState`/`RiskController`），回归测试 ×2 |
| C2 | **P1** | `session_rules()/decoder()` 全所级、无 per-instrument——AU 02:30 与 CU 01:00 共用一张窗口表；decoder 硬编码单一标的 | `exchange.rs:82-83`；`presets.rs:285-298`（注释自认「生产由 build 注入」但无通道）；旧病同构 `ctp_session.rs:65` | ✅ **v2.1 已落地**：签名 `(&self, inst: &InstrumentId)` + `CtpExchange` product→session 映射（回落默认表保持 Arc 单一来源）；回归测试 `per_product_session_windows_differ` |
| C3 | **P1** | `mark_to_market` 从未被主循环调用，`ExecutionPort` 无盯市钩子 | `execution.rs:262` 定义；`runtime.rs:191-200` 只 drain/同步持仓 | ✅ **v2.1 已落地**：`ExecutionPort::on_bar(&bar)`（决策前调用，equity 当根新鲜）；回归测试 `mark_to_market_lands_in_equity` |
| C4 | **P1** | 同 offset 多批次合并平仓腿时 `entry_price` 只取首批次 → 实现盈亏聚合错算；`ClosePriority::ProRata` 声明未实现 | `position.rs:310-316`（合并腿）、`execution.rs:321-325`（用腿首价算全量 PnL）、`position.rs:293`（ProRata 落 `_ => 0`） | ✅ **v2.1 已落地**：合并腿加权均价 + `lot_ids` 全批次精确扣减（不再跨 offset 盲扣）；ProRata 按比例分摊实现；回归测试 ×2 |
| C11 | **P1** | 换月因果倒置（未来函数）：`effective_at` = 观察日当日 00:00，早于 D 日收盘观测时点 | `subscription.rs:291-299` vs 本文 §6.5 承诺与 `roll_build.rs:21-22` 因果口径 | ✅ **v2.1 已落地**：`detect_roll` 增 `session` 参，生效时点 = 下一交易日首个窗口开盘；回归测试 `roll_effective_at_next_trading_day_open` |
| C5 | P2 | `on_roll` 返回的新订阅集被丢弃（`let _ =`），`parts.subscriptions` 永不更新——换月不切订阅 | `runtime.rs:127-131` | ✅ **v2.1 顺带落地**：回写订阅集；**v2.7 阶段 4**：`ExchangeRunner::subscriptions()` 出口落地（重订阅差集数据面，回归 `roll_event_leaves_subscriptions_visible_via_accessor`）；WS 重订阅**动作**属编排职责，随阶段 5 actor 合并接线 |
| C6 | P2 | `leverage=1.0`、`notional_per_order=None` 硬编码，现有配置无注入通道 | `runtime.rs:228-239` vs `main.rs:347`、`bootstrap.rs:78` | ✅ **v2.2 已落地**：`SizingParams` + `ExchangeRunner::with_sizing_params()`（默认 1.0/None 保持旧语义）；回归测试 `sizing_params_reach_open_orders` |
| C7 | P2 | 数量全程 f64，缺 `size_raw` 原始字面量口径，实盘帧精度降级 | `position.rs:41-51`/`execution.rs:32-42` vs `positions/src/state.rs:71-75`、`core/src/signal.rs:191` | ✅ **v2.2 已落地**：`qty_raw/remaining_raw` 贯通 `OrderRequest→Fill→PositionLot/CloseLeg/PositionState`；递减复用提取为公共函数的 `strategy_positions::subtract_qty_raw`，不可分解处置 None 不伪造；回归测试 ×2 |
| C8 | P2 | 首桶 `tainted` 判定过严：warmup 恰在桶起点也丢首桶（旧 `Feed` 仅「从桶中间开始」才丢） | `aggregation.rs:284` | ✅ **v2.3 已落地**：tainted 增加 `bar.open_time != id.start` 条件（对齐起点首桶保留）；回归测试 `aligned_first_bucket_kept_mid_start_dropped` 双向覆盖 |
| C12 | P2 | `MainContractPlan` 交割月剔除一刀切，不区分 CFFEX/SHFE/DCE | `subscription.rs:181` | ⏳ **v2.5 部分落地**：per-product 剔除参数已有配置通道（`exchanges.ctp.subscription.exclude_delivery_month` + `sessions` per-product 表），但各所交割细则（最后交易日/交割通知日口径）仍待用户提供规则后下沉 `exchanges/ctp/` |
| C9 | P3 | clippy 5 warning vs CI `-D warnings` 门禁 | `cargo clippy` 输出（§15 待处理表） | ✅ **v2.1 已清零**：`clippy --all-targets -- -D warnings` 通过 |
| C10 | P3 | `infer_finality` 缺省实现中会话边界分支两路同值（死代码），§6.2「边界推断」空转 | `bar.rs:205-208` | ✅ **v2.3 已落地**：三分支定版——边界到点即 Final、严格越过 Final、非边界整点 Provisional（宁可丢帧不误判）；回归测试 `infer_finality_boundary_semantics` |
| C13 | P2 | 配置 `contracts:` 按品种登记（`CU`），运行时标的是具体合约（`CU2601`）——精确 key miss 即 `MissingSpec`，逐月登记不可维护；且 `instruments:` 引用的实例名（`ctp`）与 `ExchangeId`（`SHFE`）不同名无法解析 | `trading.rs spec()` 精确 key 语义 vs 现有 `config::contract_for` 前缀归一现实；`registry.rs` 仅按 id 建键 | ✅ **v2.5 已落地**：`spec()` 增「具体合约 → 同品种逻辑标的」回落（命中行为逐字节不变）；`ExchangeRegistry` 增实例名别名表（`register_named`/`get_by_name`，id 精确→别名→小写三级）；回归测试 `ctp_config_per_product_and_product_level_specs` |
| C14 | P2 | 既有 lint/fmt 债：workspace 级 `clippy --all-targets -D warnings` 与 `fmt --check` 在本分支 HEAD 上本就红（app 4 处 + core 1 处 + `inspect.rs` 未格式化），阶段 0-1 从未跑过 workspace 全量门禁 | `engine.rs:362`、`report.rs:582`、`report_charts.rs:209-210`、`backtest_paper_parity.rs:515/719`、`contract.rs:493`（均非阶段 2 触及代码） | ✅ **v2.5 顺带清零**（纯机械、零语义）：`map_or→is_none_or`、删同型 `map_err`、`&RED→RED`、`then→then_some`、doc 转 code fence、`==false→!`、fmt 归一；复验 `cargo fmt --all -- --check` + workspace clippy `-D warnings` 全绿——合入前置条件补齐 |
| C15 | P1 | **旧回测链路反手吞单**：`intent_to_action` 把 `IntentKind::Reverse` 两条意图（先平 size>0 / 后开 size==0）一律映射 `Action::Open`——平仓单被 broker「已有持仓」分支拒、开仓单又被未平旧仓拒，反手在回测永不成交；存量 e2e 无 Reverse 样本，缺陷从未暴露 | `broker/backtest_client.rs:intent_to_action`（阶段 3 双跑逼出：`ctp_lifecycle_dual_run_parity` 首跑旧侧仅 6/8 笔） | ✅ **v2.6 已修复**：Reverse 意图按 size 分流 Close/Open（`size>0⇒Close`，stop_order_id 同判据）；修复后旧侧 8/8 与新侧逐笔一致（含手续费）。`engine.rs` 保持零改动；§13「含反手样本」验收自此可达 |
| C16 | P1 | **阶段 3 双跑逼出的 SPI 侧六处对齐缺口**：① `build()` 选主硬用 `Utc::now()`（时钟债）；② `on_gap/on_day_boundary` 返回的收线列表被丢弃（`bar_closed(D1)` 类策略永久丢事件）；③ 平仓成交后 `unrealized_pnl` 滞留旧值（同根反手再开仓 sizing 被虚增权益污染）；④ `WindowedSession` 无 `is_entry_confirm_time` 实现（trait 缺省恒 true vs 引擎分钟脉冲）且 `is_no_trade_window` 把午休端点误当禁交易口（与 contract 口径逐笔必分叉）；⑤ runner 缺 `signal_resume`/`blocked_closed` 补报与 `session_open_price` 注入（桥接非零翻译损耗）；⑥ `StrategyPort: Send` 上界与 `Box<dyn Strategy>`（无 Send 超类型）矛盾，任何现有策略都桥不进 | ①`exchange.rs build()`；②`runtime.rs run()/on_bar`；③`execution.rs submit`；④`session.rs WindowedSession`；⑤`runtime.rs on_bar/DecisionContext`；⑥`runtime.rs StrategyPort` | ✅ **v2.6 全部落地**：①`SourceRequest.as_of` 注入（缺省墙钟不变，回归 `as_of_injection_…`）；②`pending_newly` 队列随下一根 1m 并报；③成交后 `mark_to_market_price(fill_price)` 重盯市；④真实开收盘口 `real_boundaries` 语义（回归 `no_trade_and_confirm_only_at_real_boundaries`）+ trait `is_session_open_time`；⑤`DecisionContext.session_open_price` + 窗口后首根补报（`spi_dual_run_parity` 09:06 首根样本承重）；⑥去 `Send` 上界（数据/订单端口 Send 语义不变） |
| C17 | P1 | **规则对象瞬时 Arc 违首公理（阶段 4 装配测试逼出）**：`CtpExchange::position_rules()` 与 `BinanceExchange::session_rules()/position_rules()` 每次调用现造 `Arc::new`——两次 `build()`（live/backtest 或重复装配）拿到的不是同一批对象，「规则只实现一次、两模式同一批 Arc」在规则工厂层破防（既往测试单 build 视角从未暴露） | `exchanges/ctp/mod.rs`、`exchanges/binance/mod.rs`（v2.6 及之前） | ✅ **v2.7 已修复**：两家规则对象改存常驻字段（`position`/`session` Arc），getter 返回 `self.x.clone()`；原则 1 同修两家不留偏科；回归 `live_build_shares_rule_arcs_with_backtest`（CTP+binance 双所 `Arc::ptr_eq(trading/session/position)`） |

**对文末三原则的审计对齐**：原则 1（接口全员实现）✅ 成立——trait 全方法 +
默认实现体即此形状；原则 2（特有逻辑进交易所文件夹）⚠️ 半成立——受 C2/C12
影响，session 与交割规则目前留在框架层，签名/参数化后才能真正下沉；
原则 3（信号与仓位同一套代码）✅ 方向成立——但 C1/C4/C7/C11 不修，
「同一套」会退化为「回测阉割版一套 + 实盘另一套」，回到 §1 的痛点原点。

---

## 17. 模块爆炸半径与最小嵌入路径（v2）

> 回答「按本方案嵌入既有模块，谁要动、动多少」。结论：**§13「可保持不变」
> 清单在阶段 3（双跑）成立，在阶段 5（全量迁移）不成立**——持久化模型行键
> 命名空间化会启动 schema 迁移，消费者横跨 signals/positions/app。

| 模块 | 等级 | 改动内容 |
|---|---|---|
| strategies | 零 | `Strategy`/`Ctx`/`Action` 面由适配层全保留；registry build.rs 自动发现不碰 |
| store | 零 | `Model` 泛型机制不动；所有 Model 实现方（下行）负责加列 |
| core | 中 | `SignalIntent(:175)` 加 exchange/market/offset（过渡方案见 §13）；`ctp_trading_day(:292)`/`sizing_lots(:255)`/`CommissionSpec` 退役归 `TradingRules`；`Candle`/`Strategy`/`Action` 本体不动 |
| data | 高 | 新增两个 `BarEventSource` 适配（**包住** `csv_store`/`remote_client`，非重写）；`csv_path(:18)` 加命名空间——**v2.10 已交付**（布局 `{tf}/{exchange}/{market}/{SYMBOL}_{tf}.csv`，`resolve_csv_path` 旧布局存量一次 rename 迁移，裸 symbol 退化兼容位；warmup/download/actor 写面同键）；删 `remote_client::MarketKind(:37)` 二值枚举与 `:94-115` 回填硬分叉；`trading_calendar` 节假日表改注入 `WindowedSession.holidays`（#4 落地）；`Feed`/`BarMergeMode`/`ctp_session` 阶段 5 删除——**v2.11 状态刷新**：SPI live 面（`spi_live_actor`）已走 `AggregationState`/`warmup`/`csv_store` 命名空间键（5b 交付），但旧 `RuntimeActor` 仍是**缺省路**（`--engine old`）并宿主 `Feed`、`ctp_session` 另被 `remote_client::cst()` 消费——删除收口排在「在场验收→缺省切换→旧链退役」之后，不提前强拆；`roll_build` 因果选主迁入 `MainContractPlan`（先修 C11） |
| signals | 中 | 状态机本体（retry/recover/tick）**确实不动**；`ManagedSignal` 行键随命名空间化 ✅（v2.9：`IntentCtx.venue_key` 进 signal_id 哈希，`tests/spi_row_key_namespace.rs` 钉共存不覆盖与幂等）；新增 `ExecutionPort → (SignalManager + AsyncOrderGateway)` 桥适配器（submit/ack/drain ↔ receive_intent/ready-fills——这层此前无设计，是阶段 4 的隐藏工作量）；`FillEvent.fee` 接真实值（依赖协议升级） |
| positions | **高** | **文档原「保持不变」误判**：它是触价止损/冷却/跟踪/恢复状态机宿主（`runtime_actor.rs:614/638/756/765/817`），SPI 无对应语义。按 §9.1 (a) 上移后，positions 退化为持久化镜像 + 重启恢复；`ManagedPosition/Stop/Cooldown` 行键命名空间化（过滤点 `manager.rs:47`）——v2.9：position/stop 行键由命名空间化源键（signal_id 派生）传导 ✅；Cooldown 行键与集合过滤键=实例名（已互斥），多 symbol 同进程的名称唯一性——**v2.10 对齐落地**（`runtime_name` 单源含 `venue_key` 段，同名裸挂牌代码跨所 Cooldown 互斥，bootstrap U10 案强化钉）；**v2.11**：`PositionManager` 同为 SPI live 编排的仓位内核（`spi_live_actor` 经 `seed_position` 消费决策视图——restore/入账/冷却/历史 CSV 一套代码两链共用，文末原则 3 就此成立） |
| broker | 中 | `wire.rs` 帧加 `offset`/`fee`（**外部协议同步**，§1 前置）；`trading_service.rs:203` 隐式开平判定退役、改消费 `leg.offset`；`BacktestBroker`/`mock::floor_lot` 阶段 5 删除，双跑期不动——**v2.10 依赖登记**：`BacktestBroker` 仍为 paper 撮合内核（`bootstrap.rs:223` 生产路径）+ 7 测试套件内核，`mock` 仍为 `examples/mock_server.rs`（smoking 冒烟工具）宿主；**v2.11 补钉**：5b paper 面**复用同一网关内核**（`venue_sized` 声明 + ReadyFill 统一门）——`BacktestBroker` 现为**两链共用** kernel，删除需先把 paper 撮合迁到 SPI `SimulationExecution` 并另行验收，仍非强拆对象 |
| app | 最高 | `config.rs` exchanges 分组 + `instruments` 列表（§8）；`main.rs` 多 `--symbol`、`:329/:344` 兜底删除；`bootstrap.rs:78/:210` 网关级 venue → 意图级路由；`engine.rs`+`runtime_actor.rs`（两个 800+ 行文件）阶段 5 合并为 `ExchangeRunner`——**全方案最重的一块**；report/report_charts D1 口径统一、`TradeRow` 从 `Fill` 派生、目录命名空间化。**v2.11 落地**：live/paper 编排合并 = 新模块 `spi_live_actor.rs`（职责对照表在模块头；`runtime_actor.rs` 仅可见性 widening——`PollWatch`/`remote_matches_local`/`RuntimeHandle::from_command_tx`，行为零变化，旧链测试原样绿）；`bootstrap::run_live` 增 `engine` 分流；`main.rs` `run --engine`。**合并完成判据**=缺省路切换+旧链退役（在场验收后），当前双链并行 |

### 最小嵌入路径（阶段 3 双跑，不动 data/signals/positions/store 内部）

1. **`StrategyPort ↔ Strategy` 适配器**（app 或 SPI 新文件）：实现
   `ClosedSource for AggregationState`（`Timeframe` 互转已由
   `bar.rs:79-89` 提供），`Action` 全集翻译进 `StrategyAction`
   ——**前提 C1 已修**，否则适配器只能丢字段。
2. **`CsvEventSource`**（SPI 新增）：读现有 `csv_store` 路径布局（命名空间
   延后到阶段 5），按 `SessionRules::gap_after` 注入 `SessionGap/DayBoundary`。
3. **`Bar::from_candle()`**（`bar.rs:144` 已有）桥接 Candle 读取兼容。

旧 `Engine` 一行不改，新旧同数据双跑出差异表——把 parity 验收压力前置到
适配层，更早暴露 C1/C3/C4/C8/C11。基线证据：2026-11-05 全仓
`cargo test --workspace` 全绿，差异表的「旧侧」可复现。