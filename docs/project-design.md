# strategy-template-rs 项目设计文档

> 修订日期：2026-11-06  
> 版本：v0.2 — Store 扩展 + Model 层混合模式
>
> ## ⚠️ 口径声明（2026-11-09 收口加注，先读这段再看正文）
>
> 本文与代码**分处两个时点**，且**从未进入 git**（`git ls-files docs/project-design.md`
> → 空）。以 2026-11-09 工作区实测为准，下列章节**已失效或仅描述历史状态**：
>
> | 章节 | 状态 | 实况（file:line 证据） |
> |---|---|---|
> | §2 `exchange-spi` crate | ⚠️ 命名失效 | 实装为 `crates/engine`（包名 `strategy-engine`），`Cargo.toml` members 含之 |
> | §3.1 新增 Model `strategy_config` / `order` | ❌ 未实施 | 全仓 `grep StrategyConfig\|strategy_config` = 0；实际 7 类（signal/position/stop/cooldown/history_position/trade/equity） |
> | §3.1/§6.2/§11.3/§12.3 区块B KlineCache + `store.klines()` | ❌ 未实施 | `grep KlineCache\|klines` 全仓 = 0 命中；1m 读取走 `data/csv_store.rs`，大周期聚合在 `engine/src/core/aggregation.rs` |
> | §3.1 区块C 接口签名 | ⚠️ 实现换形（更优） | `store/src/lib.rs:462` 用 `HashMap<TypeId, Arc<dyn Any>>`、`:528 register_connection::<T>()`（类型化，无 key 参数），非本文设想的 `HashMap<String, _>` |
> | §4 Model 混合模式（含 Phase 1–4） | ❌ 一行未落地 | `grep try_transition\|apply_fill` = 0；逻辑仍在 manager（`positions/manager.rs:220 is_open`）；`exchange-spi-design.md` 全篇 0 承接——**孤儿设计，待判定做或删** |
> | §6.4 `exchange_orders.csv` | ❌ 新链不产 | `tests/backtest_e2e.rs:201` 硬断言其不存在；仅旧 `BacktestBroker` 产（见 `app/src/lib.rs:7` 更正） |
> | §7.2 / §7.3 两套引擎现状 | ❌ 已成历史 | `engine.rs`/`runtime_actor.rs` 已删；CLI 无 `--engine`（`main.rs:35 Commands` 仅 4 臂）；`bootstrap.rs:89`「统一走 engine」 |
> | §10 CLI `backtest --engine old\|spi` | ❌ 参数已废 | 同上 |
> | §12.2 Phase 5/6/7 | ✅ 均已执行 | 7 的删码早于在场验收（前置 20 项 0 勾），偏差登记见 `exchange-spi-retirement.md` §5.1 |
> | 附录 Store 行索引 | ⚠️ 漂移约 200 行 | 实测：`Clock:31` / `Model:45` / `RowMeta:66` / `ModelTable:114` / `Store:449` / `tests:668`；另 `Persistable:285` 已**私有**（本文列为对外 Layer2 接口） |
>
> | §2 领域层 `data` = 「Feed 聚合」 | ❌ 已删（2026-11-09 D1b） | `crates/data/src/feed.rs` 已删除；大周期聚合唯一真源为 `engine/src/core/aggregation.rs`（`SessionWindowPolicy`/`UtcGridPolicy`）+ `WindowedSession`。`docs/design-package/01` 亦仍引 `data/src/feed.rs`，同批待订 |
>
> **本文未被代码甩下的部分（仍然成立）**：§5 三层锁结构、§3.3 `Arc<Store>` 依赖注入、
> §6.1 state CSV 5 列、§7.5 信号 8 态（`core/lifecycle.rs:21`）、§8.1 确定性
> `signal_id`（`core/signal.rs:133-149`，键为 `策略类型_venue_key_时间戳_信号类型`）、
> §9 配置体系三文件与四步加载（`app/src/config.rs:343/474/604`）、§10 四子命令本身。
>
> **代码领先本文、0 字记载的部分**：`store/persist.rs:68 StorageBackend` 端口注入
> （design-review P3-12「sled 抽象层」已落地）、`store/lib.rs:118 scope_index`
> 二级索引、`engine/` 整个 crate 的 SPI 分层与规则对象。
>
> **真源分层**：结构现状 → `docs/design-package/`（v1.0，2026-11-09，与代码一致）；
> 迁移史与门禁 → `docs/exchange-spi-design.md`（v2.11）+ `docs/exchange-spi-retirement.md`；
> 存储与并发专题 → 本文（按上表打折读）。

---

## 目录

1. [项目定位与北极星目标](#1-项目定位与北极星目标)
2. [Workspace 结构与依赖分层](#2-workspace-结构与依赖分层)
3. [Store 统一存储架构（扩展后）](#3-store-统一存储架构扩展后)
4. [Model 层混合模式设计](#4-model-层混合模式设计)
5. [锁结构与并发模型](#5-锁结构与并发模型)
6. [持久化文件布局](#6-持久化文件布局)
7. [交易流水线与两套引擎](#7-交易流水线与两套引擎)
8. [关键设计原则](#8-关键设计原则)
9. [配置体系](#9-配置体系)
10. [CLI 入口](#10-cli-入口)
11. [扩展性分析](#11-扩展性分析)
12. [迁移计划](#12-迁移计划)

---

## 1. 项目定位与北极星目标

**定位**：Rust CTA（商品交易顾问）策略开发脚手架。

**北极星目标**：clone 本仓库后 10 分钟内跑通一次回测，照着 example 复制写出第一个 CTA 策略。

**支持标的**：加密货币永续合约 + 国内期货（CTP）。

**核心特征**：回测与实盘共用同一套策略 trait 和交易管道，回测产物可逐字节复现。

---

## 2. Workspace 结构与依赖分层

项目采用 Cargo Workspace 组织，依赖严格分层：**核心层 → 领域层 → 应用层**，上层依赖下层，下层永远不反向引用上层。

### 核心层（零 IO，纯逻辑）

| crate     | 职责                                                                                                                                    |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| **core**  | 全系统类型基石：Candle/Timeframe、Signal 8 态状态机、Order 7 态状态机、FillEvent、加仓均价/减仓盈亏纯函数、Strategy trait、风控兜底规则、确定性 signal_id 生成（SHA256 哈希）、国内期货合约参数 |
| **store** | 统一持久化门面：ModelTable 泛型内存表 + CSV 日快照 + 跨日恢复。三层独立区块：持久化 ModelTable 层、K线缓存层、WS 连接注册表                                                      |

### 领域层

| crate            | 职责                                                             |
| ---------------- | -------------------------------------------------------------- |
| **signals**      | 信号全生命周期管理：SignalManager 同步内核、OrderGateway 端口、signal.csv 追加写    |
| **positions**    | PositionManager：开/加/减/平、峰值跟踪、止损/回落止盈、止损冷却                      |
| **broker**       | 订单网关层：BacktestBroker、TradingServiceClient（WS+HMAC）、MockServer  |
| **data**         | K线数据层：KlineClient 端口（经 trading-service 订阅）、Feed 聚合、CSV 存储、归档下载 |
| **indicators**   | 技术指标库：Supertrend、SAR、效率比、talib 绑定                              |
| **exchange-spi** | 交易所 SPI 抽象层：Exchange trait + ExchangeRunner 通用主循环（**实装名 `crates/engine`**，包名 `strategy-engine`；实际依赖 core+positions+signals，位于领域层之上而非本文所述同级）              |

### 应用层

| crate          | 职责                                                               |
| -------------- | ---------------------------------------------------------------- |
| **app**        | 回测引擎 Engine（旧链路）+ SpiBacktest（SPI 新链路）、CLI、配置解析、报告生成             |
| **strategies** | 策略插件 crate：build.rs 自动扫描注册，含 example_dual_ma 和 example_mtf_trend |

### 依赖方向铁律

```
应用层 → 领域层 → 核心层
  ↑         ↑        ↑
  └─── 不允许反向引用 ───┘

exchange-spi 可依赖 core，反之不行
store 不依赖任何领域 crate
```

---

## 3. Store 统一存储架构（扩展后）

Store 从单一的持久化 ModelTable 层扩展为**三个独立区块 + 共享基础设施**，各区块持独立锁，互不阻塞。

### 3.1 整体结构

```
Store (Arc 全局共享 · 依赖注入)
│
├── 区块A: 持久化 ModelTable 层
│   ├── 已有 Model: signal / position / stop / cooldown / history_position / trade / equity
│   ├── 新增 Model: strategy_config / order
│   ├── 三层锁: 注册表 RwLock → 表级 RwLock → 行级 Mutex
│   └── 持久化: state/{YYYYMMDD}/{model}.csv (全表快照 flush + 启动 recover)
│
├── 区块B: K线缓存层（新增）
│   ├── KlineCache: HashMap<key, Vec<Candle>>
│   ├── 独立锁: RwLock<HashMap>，不参与 store.flush()
│   ├── 懒加载: get_or_load() 内存无则读 CSV
│   ├── 实时追加: append() WS闭合帧追加到内存+CSV
│   └── 持久化: data/csv_store.rs 追加写 + 水位线去重
│
├── 区块C: WS 连接注册表（新增）
│   ├── trading_ws: TradingServiceClient（行情订阅 + 交易下单共享连接）
│   ├── 独立锁: RwLock<HashMap>，连接生命周期由各模块自管
│   └── 不持久化: 重启自动重连
│
└── 共享基础设施
    ├── Clock: 回测=K线时间 / 实盘=EpochClock
    └── base: 状态根目录 path
```

### 3.2 统一接口层（Store 对外 API）

| 接口                             | 路由到                 | 使用方                                      |
| ------------------------------ | ------------------- | ---------------------------------------- |
| `store.table::<T>()`           | 区块A ModelTable CRUD | SignalManager / PositionManager / Engine |
| `store.klines().get_or_load()` | 区块B K线缓存（懒加载）       | Feed / Engine / 回测                       |
| `store.klines().append()`      | 区块B K线追加            | WS 行情回调（经 trading-service）               |
| `store.connection::<T>()`      | 区块C WS连接获取          | 需要行情订阅/交易下单的模块                           |
| `store.flush()`                | 区块A 全表快照            | RuntimeActor 定时调用                        |
| `store.recover::<T>()`         | 区块A 启动恢复            | 各 Manager 启动时调用                          |

### 3.3 全局访问方式

保持**依赖注入模式**：`Arc<Store>` 在 `bootstrap.rs` 创建后传给所有 Actor。更显式、更可测试，且当前代码已经在用。

---

## 4. Model 层混合模式设计

### 4.1 设计问题

当前 Manager 中混在一起的三类逻辑：

| 类型    | 示例                                                 | 特点                         |
| ----- | -------------------------------------------------- | -------------------------- |
| 纯数据判断 | is_open / can_retry / is_active / pending_conflict | 只读 self，无副作用               |
| 状态转移  | transition(signal, to) / open/close(position)      | 改 self + 校验合法性，不调外部        |
| 编排协调  | receive_intent / handle_order_update / recover     | 调 Store + 网关 + 日志，有 IO 副作用 |

### 4.2 混合模式（推荐方案）

纯逻辑归 Model，编排归 Manager。

- 优点：纯逻辑无 IO 单测极简，渐进迁移，Manager 瘦身后只做编排
- 边界：方法签名里出现 Store / Gateway / Clock / IO → Manager；否则 → Model

### 4.3 详细设计

#### Model 层（纯逻辑，无 IO）

```rust
impl ManagedSignal {
    // 纯判断（只读 self）
    fn is_active(&self) -> bool { !self.status.is_terminal() }
    fn can_retry(&self) -> bool { matches!(self.status, SignalStatus::Failed) }
    fn in_flight(&self) -> bool { /* ... */ }

    // 状态转移：校验合法性 + 改 self，返回 Result
    // 不调 Store、不调网关、不记日志
    fn try_transition(&mut self, to: SignalStatus) -> Result<(), String> {
        if !self.status.can_transit(to) {
            return Err(format!("非法转移 {:?} → {:?}", self.status, to));
        }
        self.version += 1;
        self.status = to;
        Ok(())
    }

    // 落账成交数据：纯字段赋值
    fn apply_fill(&mut self, price: f64, qty: f64) {
        self.filled_price = Some(price);
        self.filled_quantity = Some(qty);
    }

    // 测试夹具
    #[cfg(test)]
    fn test_fixture() -> Self { /* 构造 Created 态信号 */ }
}
```

#### Manager 层（编排协调，有 IO）

```rust
impl SignalManager {
    fn receive_intent(&mut self, intent: SignalIntent) -> Result<Vec<FillEvent>> {
        // 1. 验证（纯逻辑）
        self.validator.validate(&intent)?;

        // 2. 状态转移（Model 纯逻辑，在 Store 行子锁内执行）
        self.table.with(&id, |signal| {
            signal.try_transition(SignalStatus::Validated)
                .map_err(|e| std::io::Error::other(e))
        }).ok_or(...)??;

        // 3. 提交网关（IO）
        let order_id = self.gateway.submit(&intent)?;

        // 4. 落账订单号（Model 纯逻辑，在子锁内）
        self.table.with(&id, |signal| {
            signal.order_id = Some(order_id.clone());
            signal.try_transition(SignalStatus::Submitted);
        });

        self.note_event();
        Ok(vec![])
    }
}
```

#### 桥接机制

|                  |   |                                 |
| ---------------- | - | ------------------------------- |
| \`table.with(id, | m | m.try_transition(to))\` 是天然的桥接： |

- Model 方法在行子锁内执行，原子性由 Store 保证
- Manager 不直接操作 Model 字段，只调用 Model 方法
- Store 对 Model 的领域方法完全无感知

### 4.4 测试分层

| 测试层          | 测什么                                             | 需要 Store？   | 需要网关？     | 速度  |
| ------------ | ----------------------------------------------- | ----------- | --------- | --- |
| Model 单测     | try_transition 合法性、can_retry 判断、apply_fill 字段更新 | 不需要         | 不需要       | 纳秒级 |
| Manager 集成测试 | receive_intent 全流程、状态机端到端                       | 需要（tempdir） | mock stub | 毫秒级 |

```rust
// Model 单测——不需要 Store、不需要 mock
#[test]
fn transition_created_to_validated() {
    let mut sig = ManagedSignal::test_fixture();
    assert!(sig.try_transition(Validated).is_ok());
    assert_eq!(sig.status, Validated);
    assert_eq!(sig.version, 2);
}

// Manager 集成测试——需要 Store + stub gateway（现有测试模式不变）
#[test]
fn receive_intent_full_cycle() {
    let store = fixed_store(dir);
    let gw = StubGateway::new();
    let mut mgr = SignalManager::new("test", store, Some(Box::new(gw)));
    mgr.receive_intent(intent("sig-1", 100.0));
    assert_eq!(mgr.status_of("sig-1"), Some(Validated));
}
```

### 4.5 三层接口分层

#### Layer 1: Model trait（领域数据契约）

**基础契约**（store 定义，不可变）：

```rust
trait Model: Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
    const NAME: &'static str;
    fn id(&self) -> &str;
    fn is_terminal(&self) -> bool { false }
}
```

**领域接口**（各模块在 impl block 中自由扩展）：

各模块定义业务方法（is_active / can_retry / try_transition / apply_fill 等），Store 不感知这些方法。

#### Layer 2: ModelTable<T>（通用 CRUD + 持久化）

**CRUD 接口**（行级子锁保护）：

- `insert(data: T) -> Result` — 新建行，主键冲突报错
- `upsert(data: T)` — 插入或整行替换，保留 created_at
- `get(id) -> Option<T>` — 读一行（返回克隆）
- `list() -> Vec<T>` — 全部未删除行
- `delete(id) -> bool` — 软删除
- `meta(id) -> Option<RowMeta>` — 行元数据
- `with(id, f: FnOnce(&mut T) -> R) -> Option<R>` — 读写算闭包

**持久化接口**（类型擦除 Persistable trait）：

- `snapshot_csv() -> Vec<CsvRecord>` — 全表快照
- `purge_terminal() -> usize` — 物理清除终态行
- `load_merged(merged, prune) -> Result<usize>` — 启动恢复载入

Store 层调用：`store.flush()` / `store.recover::<T>()`

#### Layer 3: Store 门面（全局入口）

- `table::<T>()` — 泛型注册，同类型永远同一张表
- `flush()` — 遍历全表落盘
- `recover::<T>()` — 单类型恢复
- 扩展点：`klines()` / `connection::<T>()`

---

## 5. 锁结构与并发模型

### 5.1 三层锁结构

```
Store 注册表 RwLock                    ← 极短，首次注册后几乎不触碰
  └─ 各 ModelTable RwLock              ← 表级独立，signal 写不影响 position
       └─ 行级 Mutex                   ← 行级独立，signal-A 写不影响 signal-B
```

### 5.2 并发规则

| 操作                             | 锁类型                     | 持锁范围              | 影响面             |
| ------------------------------ | ----------------------- | ----------------- | --------------- |
| `table::<T>()` 首次注册            | 注册表写锁                   | 极短，HashMap insert | 后续注册被阻塞（极少发生）   |
| `table::<T>()` 已注册查找           | 注册表读锁                   | 极短，HashMap get    | 不阻塞任何操作         |
| `insert` / `upsert` / `delete` | 表级写锁 → 行级 Mutex         | 拿到句柄后释放表锁         | 同表并发读被阻塞，不影响其他表 |
| `get` / `meta`                 | 表级读锁 → 立即释放 → 行级 Mutex  | 极短                | 不阻塞任何写操作        |
| `with`（读写算）                    | 表级读锁 → 立即释放 → 行级 Mutex  | 闭包执行期间            | 同行被阻塞，不同行不阻塞    |
| `list`                         | 表级读锁 → 各行 Mutex         | 遍历全表              | 短暂持有，不影响其他表     |
| `flush`                        | 注册表读锁 → 各表读锁 → 行级 Mutex | 遍历读取              | 不阻塞写操作          |

### 5.3 并发铁律

- **表间独立**：`store<signal>` 写不影响 `store<position>`，各表持独立 RwLock
- **行间独立**：同一表内 `id-A` 写不影响 `id-B`，各行持独立 Mutex
- **读不阻塞写**：RwLock 多读并发，get 拿读锁取句柄后立即释放再锁子锁
- **flush 不阻塞并发写**：flush 遍历注册表用读锁，各表 snapshot 用读锁
- **子锁严禁跨 await**
- **区块B/C 各持独立锁**：与区块A的锁完全无关，三者可完全并发

### 5.4 唯二的全局锁

1. `tables: RwLock<HashMap>`（注册表）——仅在 `table::<T>()` 时短暂持有
2. `last_flush_day: Mutex<Option<String>>`——仅在 `flush()` 时持有，与 CRUD 无关

---

## 6. 持久化文件布局

### 6.1 区块A：ModelTable CSV

```
{base}/{YYYYMMDD}/{model}.csv
```

- 列：`id,deleted,created_at_ms,updated_at_ms,payload_json`
- 原子写：tmp → fsync → rename
- 读取：逐行容错，单行损坏只 warn 跳过

### 6.2 区块B：K线 CSV

```
{tf}/{exchange}/{market}/{SYMBOL}_{tf}.csv
```

- 列：`timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume`
- 追加写 + 水位线去重

### 6.3 区块C：WS 连接

无持久化。trading-service 一条 WS 连接同时承载行情订阅与交易下单，重启自动重建。

### 6.4 旁路报表（固定列追加写）

- `signal.csv` — 信号报表
- `exchange_orders.csv` — 订单报表（**旧链专属产物**，由 `BacktestBroker` 落盘；engine 统一链不产，钉于 `tests/backtest_e2e.rs:201`）
- `history_positions/{name}/{YYYYMMDD}.csv` — 平仓历史报表

---

## 7. 交易流水线与两套引擎

### 7.1 核心数据流

```
1m K线 → Feed 聚合(仅已闭合) → Strategy::on_candle → Action
→ intents_from_action → SignalIntent(确定性 ID)
→ SignalManager(8态状态机) → OrderGateway(端口)
→ Broker 撮合/WS下单 → FillEvent
→ PositionManager(开/加/减/平) → 统一 Store(CSV 快照)
```

### 7.2 两套引擎（**已成历史** · 2026-11-09 收口：下表描述 11-06 时点，old 链路已删除）

| 引擎      | CLI 切换         | 主循环               | 回测/实盘差异                                      |
| ------- | -------------- | ----------------- | -------------------------------------------- |
| old（缺省） | `--engine old` | Engine dispatch() | 回测专用                                         |
| spi（新）  | `--engine spi` | ExchangeRunner    | 与实盘同一段代码，差异压到 BarEventSource + ExecutionPort |

### 7.3 两套引擎的差异根因

核心数据流（§7.1）完全相同，但回测与实盘在以下维度存在物理差异：

| 维度          | old Engine（回测）                           | RuntimeActor（实盘）                    |
| ----------- | ---------------------------------------- | ----------------------------------- |
| **K线来源**    | CSV 逐根同步喂入（`on_closed_1m(candle)`）       | WS broadcast channel 异步推送           |
| **下单方式**    | `BacktestOrderClient` 同步撮合               | `AsyncOrderGateway` 异步 WS 下单        |
| **Fill 产出** | `receive_intent()` 同步返回 `Vec<FillEvent>` | 订单回报异步回流，`handle_order_update()` 产出 |
| **时钟**      | `CandleClock`（锚定 K线 open_time）           | `EpochClock`（系统墙钟）                  |
| **对账**      | 不需要（撮合内核即真相）                             | 5 种对账路径（启动/重连/远程仓位/看门狗/ReduceOnly）  |
| **并发**      | 单线程同步                                    | tokio select! 多路复用                  |
| **监督重建**    | 不需要                                      | panic 后整代重建（`spawn_supervised`）     |


old `Engine::dispatch()` 与 `RuntimeActor::drive_bar()` **各写了一遍**会话门控、策略调用分流、风控兜底、Action→意图→提交编排、仓位极值更新，期货规则在两处重复且已出现不对称（engine 的 `pending_actions` 因提前 return 恒为空）。

### 7.4 引擎统一目标（最终态）

**old `Engine` 将被废弃。** 回测和实盘统一走 `ExchangeRunner`，差异只存在于端口注入。

**统一后的架构**：

```
                     ExchangeRunner.on_bar()  ← 唯一主循环，无 if mode 分支
                    /                         \
          BarEventSource                    ExecutionPort
         /            \                   /              \
  VecEventSource   ChannelBarSource   SimulationExecution  LiveExecution
  (回测 CSV)        (实盘 WS)          (回测本地撮合)       (实盘 WS 网关)
```

**统一原则**：
- 规则对象（session/trading/position/simulation）在 live 与 backtest 中是同一批 `Arc<dyn ...>`，新增交易所只需实现一次规则
- 主循环不感知运行模式：`run()` 只消费 `MarketEvent`，不含任何 `if mode == backtest` 分支
- 回测与实盘的差异只允许出现在 `BarEventSource`（数据从哪来）和 `ExecutionPort`（订单怎么执行）两个端口
- 编排层（回测采样 equity / live actor 的网关 pump）可以在事件之间插手，主循环语义不变

|  |  |
| --- | --- |
**当前进度**：双跑验收阶段——CLI `--engine old|spi` 两个引擎跑同一份数据，逐笔对比结果，确认 spi 引擎无回归后切换默认值，最终移除 old Engine 代码。

### 7.5 信号状态机（8态）

```
Created → Validated → Submitted → Accepted → Filled
                                ↗
旁路: Rejected / Failed(→Submitted 可重试) / Cancelled
```

终态不可转移。信号一旦产生即落盘，下单结果只改变其状态。

---

## 8. 关键设计原则

### 8.1 确定性红线

- Store 时钟锚定 K线 `open_time`，全程禁止 `Utc::now()`（实盘用 EpochClock，回测用 CandleClock）
- `signal_id` = SHA256(策略类型_交易所_标的命名空间键_K线时间戳_信号类型)，同输入同输出
- 回测产物可逐字节复现

### 8.2 内存即真相

- 盘载只填空缺 id，不覆盖内存新态
- 同进程监督重建不覆盖内存新态
- flush 用读锁，不影响并发写

### 8.3 统一 Store 持久化

- 所有运行时状态统一进 Store 的泛型 ModelTable
- 日快照格式固定 5 列
- 跨日物理裁剪终态行 + 软删行
- 启动恢复只读最新存在的日文件（结构上防复活）

### 8.4 exchange-spi 分层铁律

- exchanges/ 可依赖 core/，反之不行
- 新增交易所：只实现 Exchange trait 及规则对象
- 实盘/回测差异只允许出现在 BarEventSource 和 ExecutionPort 两个端口

### 8.5 交易链路统一经 trading-service

- run 命令经 trading-service 一个外部服务，同时提供行情订阅和交易下单
- 端点走配置文件 services 节，缺失即 fail-fast
- K线订阅与订单提交共享同一条 WS 连接，Store 持有连接对象供各模块获取

### 8.6 开放-封闭原则（OCP）

- Store 的泛型 + blanket impl + 类型擦除设计
- 新增 Model：Store 零改动，自动获得 CRUD + 持久化
- 新增 ModelTable 通用接口：各 Model 零改动
- 新增 Store 区块：现有 Model / ModelTable 零改动

---

## 9. 配置体系

### 9.1 配置文件结构

| 文件 | 作用 |
|---|---|
| `config/_global.yaml` | 全局基线（services 端点 + 密钥，gitignore 不入库） |
| `config/{strategy}/{symbol}.yaml` | per-symbol 配置（策略参数/合约/模式） |
| `config/contracts.yaml` | 国内期货合约参数（CU/AU/AG/RB/RU/I） |

### 9.2 合并优先级

CLI 显式参数 > per-symbol 文件 > 全局默认

### 9.3 配置加载流程

1. 探测 `DEFAULT_CONFIG_PATHS`（yaml/yml/json）
2. `apply_env_overrides` — 环境变量覆盖
3. `apply_global_file_fallback` — 补挂 _global.yaml
4. `apply_contracts_fallback` — 自动加载 contracts.yaml

---

## 10. CLI 入口

`strategy-rs` 二进制提供四个子命令：

| 命令 | 用途 |
|---|---|
| `backtest` | 1m CSV 回放 → 共用引擎 → JSON/TXT/PNG 报告（~~`--engine old\|spi`~~ 该参数已随旧链退役删除，2026-11-09） |
| `run` | 实盘/paper/smoking 运行（需 services 配置） |
| `download` | Binance 公共归档离线导入 1m CSV |
| `inspect` | 展开 state 快照 CSV 的 payload_json 为可读表格 |

---

## 11. 扩展性分析

### 11.1 新增一个 Order Model 的改动量

| 改动位置 | 文件 | 改动量 |
|---|---|---|
| Store crate | 无 | 0 行 |
| Model 定义 | 自己 crate 新建 order_model.rs | ~30 行（struct + impl Model + 领域方法） |
| Service 编排 | 自己 crate 新建 order_service.rs | ~20 行 |
| 使用方 | Engine / RuntimeActor | 1 行（`store.table::<Order>()`） |

### 11.2 Store 零改动的原因

1. **泛型注册**：`store.table::<T>()` 泛型方法，T=Order 首次调用自动注册
2. **Blanket impl**：`impl<T: Model> Persistable for ModelTable<T>` 自动覆盖
3. **类型擦除遍历**：`store.flush()` 遍历 `HashMap<&str, Arc<dyn Persistable>>`，新表自动被遍历到

### 11.3 各种扩展场景

| 场景 | Store 改动 | 自己 crate 改动 | 测试方式 |
|---|---|---|---|
| 新增 Order Model | 0 | 新建 2 文件 ~50 行 | Model 单测 + Service 集成测试 |
| 新增 StrategyConfig Model | 0 | 新建 1 文件 ~30 行 | Model 单测 |
| Order 新增领域方法 | 0 | impl block 加 3 行 | Model 单测加 1 case |
| ModelTable 新增通用接口 | 加 1 方法 | 各 Model 零改动 | Store 自己测试 |
| Store 新增 KlineCache 区块 | 加字段+方法 | 各 Model 零改动 | Store 自己测试 |

---

## 12. 迁移计划

### 12.1 渐进式迁移策略

混合模式向后兼容，可以一个方法一个方法地迁移，不需要一次性重构。

#### Phase 1: 纯判断方法迁移

把 Manager 中的纯判断方法搬到 Model impl block：

- `SignalManager::status_of` → `ManagedSignal::status`（已有字段，加 convenience 方法）
- `SignalManager::pending_conflict::in_flight` → `ManagedSignal::in_flight`
- `PositionManager::is_open` → `ManagedPosition::is_open`

#### Phase 2: 状态转移逻辑迁移

把 Manager 中的状态转移逻辑抽成 Model 方法：

- `SignalManager::transition` → `ManagedSignal::try_transition`
- `SignalManager::apply_exchange_update` → `ManagedSignal::apply_exchange_update`
- `PositionManager::insert_open` 中的状态构造 → `ManagedPosition::new_open`

#### Phase 3: Manager 瘦身

|  |  |  |
| --- | --- | --- |
Manager 中对应位置改为 `table.with(id, |m| m.try_transition(to))`：

- `receive_intent` 内部改为调 Model 方法
- `handle_order_update` 内部改为调 Model 方法

#### Phase 4: 补充 Model 单测

为每个 Model 纯逻辑方法补充单测，不需要 Store / mock。

### 12.2 引擎统一迁移（old Engine 废弃）

#### Phase 5: SPI 回测双跑验收（当前阶段）

|  |  |
| --- | --- |
- CLI `--engine old|spi` 两个引擎跑同一份 1m 数据，逐笔对比信号/成交/净值
- 差异登记表逐项消解（signal_id 方案、comment 口径、equity 采样时点等）
- spi 回测产物与 old 逐字节对齐后标记"验收通过"

#### Phase 6: 实盘编排合并到 ExchangeRunner

- `RuntimeActor` 的主循环替换为 `ExchangeRunner.on_event()` 步进形态
- live 端口工厂（`LivePortFactory`）注入 `ChannelBarSource` + `LiveExecution`
- 仓位内核由外层 `PositionManager` 承重，runner 只消费决策视图（`seed_position` / `take_open_meta`）
- 5 种对账路径迁入编排层，不经 runner 主循环

#### Phase 7: old Engine 代码移除

- 删除 `crates/app/src/engine.rs`
- CLI `--engine` 参数废弃，回测默认走 spi 链路
- `BacktestOrderClient` 如果仍被 paper 模式引用则保留，否则一并移除
- 文档更新：§7 两套引擎描述改为"统一引擎"

### 12.3 新增区块实施

#### K线缓存层

1. Store 中新增 `kline_cache: RwLock<HashMap<KlineKey, Vec<Candle>>>` 字段
2. 新增 `klines()` 方法返回 KlineCache 句柄
3. `get_or_load(key, range)` — 内存命中返回，未命中从 CSV 读取并缓存
4. `append(key, candle)` — 追加到内存 + CSV
5. 不参与 `store.flush()` 全表快照

#### WS 连接注册表

1. Store 中新增 `connections: RwLock<HashMap<String, Arc<dyn Any + Send + Sync>>>` 字段
2. 新增 `register_connection(key, conn)` / `connection::<T>(key)` 方法
3. trading-service 一条 WS 连接同时承载行情订阅与交易下单，注册到 Store 后各模块共享获取
4. 连接生命周期管理由 broker 模块负责（建连/重连/Ping/看门狗），Store 只做引用持有
5. 不参与 flush / recover

---

## 附录：Store 完整代码结构索引

```
crates/store/
├── Cargo.toml
└── src/
    ├── lib.rs          # Model trait / ModelTable<T> / Persistable / Store / Clock
    └── persist.rs      # CsvRecord / write_day_csv / read_day_csv / day_str

crates/store/src/lib.rs 关键行：
  25-36   Clock trait + EpochClock
  38-49   Model trait（基础契约）
  51-57   RowMeta
  59-71   StoreError / Result
  73-82   Stored<T> / RowHandle<T>（行级子锁）
  94-211  ModelTable<T>（CRUD 接口）
  214-330 Persistable trait（类型擦除持久化接口）
  332-478 Store 门面（注册表 + flush + recover）
  480+    测试
```
