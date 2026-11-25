# strategy-template-rs 设计审查报告

> ## 🗄️ 已归档（2026-11-25）：时点性过程产物，不再更新
>
> 本文为设计/执行时点的快照，结论已沉淀到对应 issues 与代码；现行规范见 `docs/strategy/`。

<!-- refcheck:coords=off -->

> 审查日期：2026-11-06
> 版本：v1.0 — 并发 / 持久化 / 扩展性 / 解耦 / 设计模式全面审查
> 审查范围：Store 存储层、Model 混合模式、ExchangeRunner 引擎、WS 连接管理、Workspace 分层

---

## 目录

1. [设计模式清单](#1-设计模式清单)
2. [并发模型评估](#2-并发模型评估)
3. [持久化存储评估](#3-持久化存储评估)
4. [扩展性评估](#4-扩展性评估)
5. [架构解耦评估](#5-架构解耦评估)
6. [Rust 语言特性利用](#6-rust-语言特性利用)
7. [问题与风险汇总](#7-问题与风险汇总)
8. [改进建议](#8-改进建议)
9. [总体评价](#9-总体评价)

---

## 1. 设计模式清单

本项目应用了以下设计模式，按层次分类：

### 1.1 架构级模式

| 模式 | 应用位置 | 质量评价 |
|---|---|---|
| **Hexagonal Architecture（端口/适配器）** | `BarEventSource` / `ExecutionPort` / `StrategyPort` 为端口；`VecEventSource` / `ChannelBarSource` / `SimulationExecution` / `LiveExecution` 为适配器 | **优秀**。主循环 `run()` 零 `if mode` 分支，差异完全压到端口注入。`Exchange::build()` 作为唯一分叉点是教科书级实现 |
| **Layered Architecture（分层架构）** | 核心层（core/store）→ 领域层（signals/positions/broker/data/exchange-spi）→ 应用层（app/strategies） | **严格**。依赖方向铁律无违规，分层深度适中 |
| **Dependency Injection（依赖注入）** | `Arc<Store>` 构造器注入 + `ExchangeParts` 装配注入 + `LivePortFactory` 注入 | **一致且显式**。全程构造器注入，无全局单例 / Service Locator |

### 1.2 结构型模式

| 模式 | 应用位置 | 质量评价 |
|---|---|---|
| **Bridge（桥接）** | `LegacyStrategyBridge` 适配 `core::Strategy` → `StrategyPort` | **合理但过渡性**。`Action → StrategyAction` 一一映射零损耗，双跑验收后应移除 |
| **Registry（注册表）** | `ExchangeRegistry::build_all` 多标的装配 + Store `HashMap<&str, Arc<dyn Persistable>>` 类型表注册 | **恰当**。多标的独立 Runner + 独立端口，类型表泛型注册零改动扩展 |
| **Adapter（适配器）** | `bar_to_candle` / `spec_from_contract` 类型互转 | **必要且最小化**，只做类型转换不含规则 |
| **Facade（门面）** | `Store` 门面统一暴露 `table()` / `klines()` / `connection()` / `flush()` / `recover()` | **优秀**。屏蔽内部三区块细节，统一访问入口 |
| **Type Erasure（类型擦除）** | `Persistable` trait + `HashMap<&str, Arc<dyn Persistable>>` 遍历 flush | **优秀**。新增 Model 零改动 Store，blanket impl 自动覆盖 |

### 1.3 行为型模式

| 模式 | 应用位置 | 质量评价 |
|---|---|---|
| **Strategy + Template Method** | `Exchange::build()` 模板方法，子类覆写 `make_source` / `make_execution` | **优秀**。默认实现让回测开箱即用，live 覆写两个端口工厂 |
| **State Machine（状态机）** | `SignalStatus` 8 态 + `OrderState` 5 态 + `StrategyAction` 6 变体 | **充分且语义清晰**。终态不可转移保证不变量 |
| **Observer（观察者）** | `broadcast::channel` K线扇出 + `Reconnected` 事件广播 | **恰当**。Lagged 自愈语义匹配重连补缺场景 |
| **Supervisor（监督者）** | `spawn_supervised` panic 后整代重建 + 启动对账恢复 | **健壮**。Store 内存不丢，只需对账补数据 |

### 1.4 领域驱动设计（DDD）模式

| 模式 | 应用位置 | 质量评价 |
|---|---|---|
| **Repository Pattern** | `ModelTable<T>` 就是泛型 Repository，`Store` 是 Unit of Work | **已实现** |
| **Aggregate Root（聚合根）** | `SignalManager` 是 Signal 聚合根，`PositionManager` 是 Position 聚合根 | **部分适用**。跨聚合一致性（平仓时 position 软删 + history 入表 + stop cancelled + cooldown 写入）非事务性，弱一致性行可接受 |
| **Domain Model（混合模式）** | 纯逻辑归 Model impl block，编排归 Manager，`table.with()` 桥接 | **务实选择**。Model 单测无需 Store/mock，Manager 集成测试用 tempdir + stub |
| **Event Sourcing** | `history_position` 的 append-only 是局部 Event Sourcing | **局部采用**。CTA 状态空间小，全量快照比事件回放更高效，全局不适用 |
| **CQRS** | 未采用（正确） | 读写同一张表，无独立读模型需求 |

---

## 2. 并发模型评估

### 2.1 当前架构概述

项目采用 **Store 同步锁 + RuntimeActor 单写者 async 循环 + WS 连接独立 task** 三层并发模型：

- **Store**：进程级共享，三层锁——注册表 `RwLock` → 表级 `RwLock` → 行级 `Mutex`，全部 `std::sync`
- **RuntimeActor**：每策略实例一个 tokio task，`select!` 多路复用 K线流/命令/定时器，单写者独占决策链
- **WS 客户端**：`connection_loop` 独立 task，自包含指数退避重连 + Ping + 看门狗

### 2.2 三层锁结构——合理且精确

```
注册表 RwLock<TypeId, Arc<dyn Persistable>>
  └─ 表级 RwLock<HashMap<String, Arc<Mutex<Stored<T>>>>>
       └─ 行级 Mutex<Stored<T>>
```

关键设计亮点：
- `row_handle()` 先 clone `Arc` 再释放表读锁，**避免持表锁等行锁**的嵌套
- `upsert()` 在写锁内用 `Entry` 原子判定，消除 check-then-insert 竞态
- 锁序严格一致：永远 表锁 → 行锁，无反向获取，**无死锁可能**
- 表间独立（signal 写不影响 position）、行间独立（id-A 不影响 id-B）、读不阻塞写

### 2.3 async/sync 混合——安全，关键在纪律

Store 全部使用 `std::sync` 锁，RuntimeActor 在 tokio async 上下文中调用。安全性取决于一条铁律：

> **同步锁绝不跨 await 持有。**

当前代码通过 `with()` 的签名 `impl FnOnce(&mut T) -> R`（非 async）在**编译器层面**保证了不跨 await。`flush()` 经 `spawn_blocking` 执行，不阻塞 reactor。

### 2.4 单写者模式——优秀设计

一个 task 独占完整决策链（K线→Feed→策略→信号→仓位→网关→入账）：
- **零消息传递**：仓位直读、成交同 task 入账，消除 Actor 间 channel 延迟
- **顺序确定性**：bar 事件串行处理，状态转移无并发竞态
- **监督隔离**：panic 后整代重建，Store 内存表不丢

### 2.5 并发风险点

| 风险 | 严重性 | 说明 |
|---|---|---|
| 行锁中毒 | 中 | `std::sync::Mutex` panic 后中毒不可恢复；监督重建兜底但同进程内该行永久不可用 |
| `list()` / `snapshot_csv()` 持表读锁期间逐行加锁 | 低 | N 行 N 次 lock 持有表读锁，阻塞写；但调用频率低（flush/审计） |
| `table()` 读→写锁升级 | 低 | 先 read 检查 miss 后 write 插入，`entry().or_insert()` 保证幂等，仅微小构造浪费 |

---

## 3. 持久化存储评估

### 3.1 CSV 日快照——选择合理

**格式**：5 列（`id,deleted,created_at_ms,updated_at_ms,payload_json`），业务字段序列化为 JSON。

**优势**：
- 零依赖、人类可读，`inspect` 命令可直接展开
- `payload_json` 兼得 JSON 灵活性和 CSV 行级容错
- 新增字段只追加 JSON key，天然向后兼容

**原子写**：tmp（带 pid 后缀）→ fsync → rename 三步走，rename 保证原子覆盖。单行损坏只 warn 跳过不毒死全表。

**跨日裁剪**：防复活保证是**结构性的**——recover 只读最新日文件，终态行在 rollover 时已被 purge 物理移除。时钟回拨守卫（单调日期比较）防止向历史目录覆写。

### 3.2 K线双轨制持久化——合理

K线是 append-only 时序数据，与 Model 的 mutable state 语义本质不同：
- 不需要 `deleted` / `updated_at`
- 不需要跨日 purge
- 水位线去重（last timestamp 过滤）比全表快照高效

走独立 CSV 追加写 + 懒加载缓存是恰当的分离。

### 3.3 持久化风险点

| 风险 | 严重性 | 说明 |
|---|---|---|
| 跨表 flush 非原子 | 中 | position 已落盘而 history 未落盘的崩溃窗口存在；`close()` 幂等设计兜底 |
| `load_filled_open_from_disk` 绕过 Store recover | 低 | 第二条恢复路径，增加维护负担 |
| CSV 全表快照 O(n) 读写 | 低 | 行数增长到万级以上时可能成为瓶颈；当前规模无此需求 |

---

## 4. 扩展性评估

### 4.1 Store 泛型注册——OCP 合规度极高

```
新增 Order Model 的改动量：
  Store crate    : 0 行（零改动）
  Model 定义     : ~30 行（struct + impl Model + 领域方法）
  Service 编排   : ~20 行
  使用方         : 1 行（store.table::<Order>()）
```

这是教科书级的 **开放-封闭原则（OCP）** 实践：
- 泛型注册：`store.table::<T>()` 首次调用自动注册
- Blanket impl：`impl<T: Model> Persistable for ModelTable<T>` 自动覆盖持久化
- 类型擦除：`flush()` 遍历 `HashMap<&str, Arc<dyn Persistable>>` 自动纳入新表

### 4.2 Model trait 设计——恰当

基础契约 `NAME` / `id()` / `is_terminal()` 覆盖持久化所需最小信息。约束 `Clone + Serialize + DeserializeOwned + Send + Sync + 'static` 不过紧不过松。

### 4.3 Exchange SPI 扩展性——优秀

规则与运行模式正交：`Exchange` trait 只描述规则，`make_source` / `make_execution` 按 `RunMode` 装配端口。新增交易所成本：实现 `Exchange` trait + 规则对象一次，回测自动可用，实盘注入工厂。

### 4.4 扩展性风险点

| 风险 | 严重性 | 说明 |
|---|---|---|
| `own_rows()` 全表扫描 | 低 | 多策略实例共享同一张表时每次 list 全表克隆再按 strategy_name 过滤 |
| CSV 后端扩展上限 | 低 | 万级以上行数时全表快照 O(n) 读写成为瓶颈；sled 可作为可选后端 |

---

## 5. 架构解耦评估

### 5.1 Workspace 三层依赖——严格

```
应用层（app / strategies）
  ↓
领域层（signals / positions / broker / data / exchange-spi / indicators）
  ↓
核心层（core / store）
```

依赖方向铁律无违规。一个值得注意的点：`exchange-spi` 位于领域层但承担了"引擎层"角色（ExchangeRunner 主循环、bridge 策略桥接），职责已超出"SPI 抽象层"的命名语义。**建议后续重命名为 `engine` 或 `runtime`**。

### 5.2 引擎统一方案——方向正确，需加速落地

old `Engine::dispatch()` 与 `RuntimeActor::drive_bar()` 各写一遍会话门控/风控/Action 编排，已出现 `pending_actions` 恒空等不对称。spi 引擎（ExchangeRunner）通过端口注入消灭分叉，方向完全正确。

双跑验收阶段越长，两套代码漂移风险越大。**应尽快完成 Phase 7 移除 old Engine**。

### 5.3 trading-service 合并——架构简化

原来 kline-service + trading-service 两个外部服务 → 一个 trading-service：
- K线订阅和交易下单共享一条 WS 连接
- Store 持有连接对象供各模块获取（区块C 连接注册表）
- 连接生命周期由 broker 模块统一管理（建连/重连/Ping/看门狗）
- data 模块不再持有独立连接，经 Store 获取共享连接

这个变化简化了外部服务拓扑，Store 连接注册表的存在理由更加充分。

### 5.4 解耦风险点

| 风险 | 严重性 | 说明 |
|---|---|---|
| 双引擎并存技术债 | 高 | 两套编排逻辑已出现不对称，漂移风险随时间增长 |
| `as_any_mut` 类型擦除逃生舱 | 中 | `LiveExecution` 特有 API 经 `dyn Any` downcast 暴露，绕过 trait 约束 |
| `ExchangeRunner` 单体膨胀 | 中 | 818 行，10+ 字段，`on_bar` 约 120 行 |
| `signal_id` 方案不一致 | 高 | runner 用 `format!` 而非设计文档承诺的 SHA256，破坏确定性红线 |

---

## 6. Rust 语言特性利用

### 6.1 优秀实践

| 特性 | 应用 | 评价 |
|---|---|---|
| **trait object（`dyn Trait`）** | `ExchangeParts` 中 7 个 `Arc<dyn ...>` 规则对象 + 2 个 `Box<dyn ...>` 端口 | **正确**。运行时多态需要 trait object，泛型会产生过多单态化膨胀 |
| **Blanket impl** | `impl<T: Model> Persistable for ModelTable<T>` | **优秀**。一次实现覆盖所有 Model，OCP 的 Rust 落地 |
| **enum 状态机** | `SignalStatus` 8 态、`OrderState` 5 态、`StrategyAction` 6 变体 | **充分且语义清晰** |
| **`Arc` 共享** | 规则对象 live/backtest 共享同一实例 | **核心正确**。保证"规则唯一来源"公理 |
| **`Send` 上界取舍** | `StrategyPort` 不设 `Send`（同线程），`BarEventSource`/`ExecutionPort` 设 `Send`（跨线程） | **边界判断精准** |
| **`FnOnce` 闭包防线** | `with()` 的 `impl FnOnce(&mut T) -> R` 编译器层面阻止跨 await | **设计精妙** |

### 6.2 待改进项

| 特性 | 当前 | 建议 |
|---|---|---|
| **锁实现** | `std::sync::Mutex` / `std::sync::RwLock` | 替换为 `parking_lot::Mutex` / `parking_lot::RwLock`：不可中毒、性能 2-5x、API 兼容 |
| **`dyn Any` downcast** | `as_any_mut()` 和 `connection::<T>()` 用 `dyn Any` | 考虑扩展 trait 或类型化 key 替代 |
| **`format!` signal_id** | runner 用 `format!("sig_{}_{}_{}", ...)` | 回归 SHA256 哈希方案，保证确定性 |

---

## 7. 问题与风险汇总

按严重性排序：

| # | 问题 | 严重性 | 类别 | 影响 |
|---|---|---|---|---|
| 1 | 双引擎并存技术债 | **高** | 架构 | 两套编排逻辑漂移，`pending_actions` 已不对称 |
| 2 | `signal_id` 方案不一致 | **高** | 确定性 | runner 用 `format!` 而非 SHA256，回测产物无法逐字节复现 |
| 3 | 行锁中毒不可恢复 | **中** | 并发 | panic 后该行永久不可用，监督重建兜底但有窗口 |
| 4 | `as_any_mut` 类型逃生舱 | **中** | 解耦 | `LiveExecution` 特有 API 绕过 trait 约束 |
| 5 | `ExchangeRunner` 单体膨胀 | **中** | 解耦 | 818 行，职责过重 |
| 6 | 跨表 flush 非原子 | **中** | 持久化 | 崩溃窗口存在，幂等设计兜底 |
| 7 | `list()` 持表锁逐行加锁 | **低** | 并发 | flush 路径阻塞写，频率低影响小 |
| 8 | `own_rows()` 全表扫描 | **低** | 扩展性 | 多实例共享表时性能问题 |
| 9 | `load_filled_open_from_disk` 第二恢复路径 | **低** | 持久化 | 绕过 Store recover 纪律 |
| 10 | `exchange-spi` 命名不符 | **低** | 架构 | crate 名与职责（引擎层）不匹配 |

---

## 8. 改进建议

### P0（高优先级，低风险高收益）

| # | 建议 | 改动量 | 风险 | 状态 |
|---|---|---|---|---|
| 1 | **尽快完成 Phase 7，移除 old Engine** | 删除 `engine.rs` + 废弃 `--engine` 参数 | 低（双跑验收已通过后） | 待 Phase 7 |
| 2 | **`signal_id` 回归 SHA256 方案** | runner `next_signal_id()` 改为调 `generate_signal_id()` | 低 | ✅ 已完成 |
| 3 | **`std::sync` → `parking_lot`** | 替换 Store 所有锁 | 低（API 兼容） | ✅ 已完成 |

#### P0-1 待实施步骤（移除 old Engine）

> 前置条件：Phase 5 双跑验收通过（spi 引擎与 old Engine 逐行输出一致）

1. **确认 spi 引擎测试全通过**：`cargo test --test spi_engine_cli_parity`（含 `single_process_mixed_backtest_binance_and_ctp`）
2. **删除 old Engine 代码**：
   - `crates/app/src/engine.rs` 整文件删除
   - `crates/app/src/lib.rs` 移除 `mod engine;` 和 `pub use engine::Engine;`
   - `crates/app/src/report.rs` 中 `Engine` 专用的 report 逻辑如果 spi 引擎已有等价物则删除
3. **移除 `--engine` CLI 参数**：
   - `crates/app/src/main.rs`（或 CLI 解析处）：删除 `--engine`/`Engine` 枚举分支
   - 回测入口统一走 `spi_backtest.rs` 的 `assemble()` / `from_parts()`
4. **更新测试**：
   - `crates/app/tests/backtest_e2e.rs`：`build_engine()` 改用 `ExchangeRunner` 装配
   - `crates/app/tests/spi_engine_cli_parity.rs`：`single_process_mixed_backtest_binance_and_ctp` 中 signal_id 断言需适配 P0-2 的 SHA256 格式（当前该测试失败，因为断言仍检查描述性前缀格式 `sig_binance_crypto_perpetual_BTCUSDT_`）
5. **验证**：`cargo check --workspace && cargo test --workspace && cargo clippy --workspace`

### P1（防御加固）

| # | 建议 | 改动量 | 风险 | 状态 |
|---|---|---|---|---|
| 4 | **拆分 `ExchangeRunner`** | 提取风控兜底/仓位状态/Action 分发为独立模块 | 中 | 待实施 |
| 5 | **`list()` / `snapshot_csv()` 先 clone 释放表锁** | 改为先收集 `Vec<Arc<RowHandle>>` 再逐行操作 | 低 | ✅ 已完成 |
| 6 | **`as_any_mut` → 扩展 trait** | 定义 `LiveExecutionPort` trait，编译期类型安全 | 中 | ✅ 已完成 |
| 7 | **共享表加索引** | `ModelTable` 增加 `by_scope()` 二级索引 | 低 | ✅ 已完成 |

#### P1-4 待实施步骤（拆分 ExchangeRunner）

> 现状：`crates/exchange-spi/src/core/runtime.rs` 约 818 行，10+ 字段，`on_bar` 约 120 行，职责过重

1. **提取风控兜底模块** `crates/exchange-spi/src/core/risk_guard.rs`：
   - 将 `on_bar` 中的 no-trade 窗口判断、止损收紧、最大仓位限制等逻辑提取为 `RiskGuard` struct
   - `RiskGuard::check(&self, ctx: &BarContext) -> RiskDecision`
   - `ExchangeRunner` 持有 `Arc<RiskGuard>`，`on_bar` 中调 `risk_guard.check()` 获取决策
2. **提取 Action 分发模块** `crates/exchange-spi/src/core/action_dispatcher.rs`：
   - 将 `on_bar` 中处理 `StrategyAction::Open`/`Close`/`CancelStop` 等的分发逻辑提取为 `ActionDispatcher`
   - `ActionDispatcher::dispatch(&self, action: StrategyAction, ctx: &mut ExecutionContext) -> Result<()>`
   - `ExecutionContext` 封装 signals manager / positions manager / execution port 的引用
3. **提取仓位状态同步模块** `crates/exchange-spi/src/core/position_sync.rs`：
   - 将 `on_bar` 中成交回报→仓位更新→止损更新→cooldown 写入的串联逻辑提取为 `PositionSync`
   - `PositionSync::on_fill(&self, fill: FillEvent, ctx: &mut ExecutionContext) -> Result<()>`
4. **ExchangeRunner 瘦身后**：
   - 保留主循环 `run()` 和 `on_bar` 的编排骨架（调 risk_guard → 策略 → dispatcher → position_sync）
   - 目标：`on_bar` < 30 行（纯编排），总行数 < 300 行
5. **验证**：`cargo check --workspace && cargo test --workspace`（回测输出不变）

### P2（可选优化）

| # | 建议 | 改动量 | 风险 | 状态 |
|---|---|---|---|---|
| 8 | **`exchange-spi` → `engine` 重命名** | Cargo.toml + import 路径 | 低 | 待实施 |
| 9 | **统一恢复路径** | `load_filled_open_from_disk` 下沉为 `Store::recover_with_filter` | 中 | ⚠️ 部分完成 |
| 10 | **flush 跨表原子性补偿** | 写 `.flush_marker` 含各表行数+CRC32 校验和 | 低 | ✅ 已完成 |
| 11 | **Store 连接注册表类型化** | 泛型 `register_connection::<T>()` 替代 `dyn Any` | 低 | ✅ 已完成 |

#### P2-8 待实施步骤（exchange-spi → engine 重命名）

> 现状：crate 名为 `exchange-spi` 但实际职责已超出"SPI 抽象层"，承担了引擎层角色

1. **重命名 Cargo.toml**：
   - `crates/exchange-spi/Cargo.toml`：`name = "strategy-exchange-spi"` → `name = "strategy-engine"`
   - 根 `Cargo.toml` workspace members：`crates/exchange-spi` → `crates/engine`（目录也重命名）
2. **更新所有 import 路径**：
   - 全项目搜索 `strategy_exchange_spi` → 替换为 `strategy_engine`
   - 全项目搜索 `use strategy_exchange_spi::` → 替换为 `use strategy_engine::`
3. **更新依赖引用**：
   - 所有依赖 `strategy-exchange-spi` 的 crate 的 `Cargo.toml` 中改依赖名
4. **验证**：`cargo check --workspace && cargo test --workspace`

#### P2-9 部分完成说明（统一恢复路径）

> 已完成：`load_filled_open_from_disk` 已从直接调 `strategy_store::persist::read_day_csv` + `std::fs::read_dir` 改为使用 `self.store.list_days()` + `self.store.read_table_raw()`，不再直接依赖 persist 模块内部函数。
>
> 待完成：进一步下沉为 `Store::recover_with_filter::<T>(filter: impl Fn(&T) -> bool)`，消除 signals manager 中手动遍历日目录的逻辑。

待完成步骤：
1. **在 Store 中新增 `recover_with_filter` 方法**：
   ```rust
   pub fn recover_with_filter<T: Model>(
       &self,
       filter: impl Fn(&T) -> bool,
   ) -> Result<Vec<T>>
   ```
   - 内部调 `self.backend.list_days()` + `self.backend.read_table()` + 反序列化 + filter
   - 返回符合条件的活跃记录
2. **重构 signals manager `load_filled_open_from_disk`**：
   - 从手动遍历日目录 + parse + filter 改为一行 `self.store.recover_with_filter(|s| s.status == Filled && s.intent.kind == Open)`
3. **验证**：`cargo test -p strategy-signals && cargo test --test backtest_e2e`

### P3（未来扩展）

| # | 建议 | 说明 | 状态 |
|---|---|---|---|
| 12 | **sled 作为可选后端** | 万级以上行数时 CSV 快照成为瓶颈，sled 作为 `StorageBackend` 替代实现 | ⚠️ 抽象层已完成 |
| 13 | **DashMap 替换表级 RwLock** | 单进程 symbol 数 >50 时分片锁降低写争用 | 待实施 |
| 14 | **`is_terminal` 与 recover 分离** | 提取 `should_resubmit_on_recover()` 显式分离语义 | 待实施 |

#### P3-12 抽象层已完成说明（sled 可选后端）

> 已完成：`StorageBackend` trait + `CsvStorage` 实现 + `Store::with_backend()` 注入入口。
> 后续切换 sled 只需三步：实现 `StorageBackend` trait → 在 `Store::with_backend()` 注入 → 内存逻辑零改动。

待实施步骤（当行数达到万级时触发）：
1. **新增 `crates/store/src/backends/sled_storage.rs`**：
   - 实现 `StorageBackend` trait 的 6 个方法
   - `write_table`：`sled::Tree::insert(id, payload_json)` 批量写入
   - `read_table`：`Tree::iter()` 收集全部记录
   - `list_days`：用独立的 `days` Tree 或目录结构跟踪日
   - `write_marker` / `read_marker`：用 `markers` Tree 存储
   - `base_path`：返回 sled 数据库路径
2. **在 `crates/store/Cargo.toml` 添加可选依赖**：
   - `sled = { version = "0.34", optional = true }`
   - `[features] sled-backend = ["sled"]`
3. **使用方切换**：
   - `Store::with_backend(Arc::new(SledStorage::new(&path)), clock, true)`
4. **验证**：回测字节级一致性测试（`backtest_replay_is_byte_identical`）需适配 sled 的排序语义（sled 的 key 排序是字典序，需确保 `list_days` 返回升序）

#### P3-13 待实施步骤（DashMap 替换表级 RwLock）

> 触发条件：单进程 symbol 数 >50 时，表级 `RwLock<HashMap>` 写争用成为瓶颈

1. **评估当前性能瓶颈**：
   - 先做 benchmark：50+ symbol 并发回测时 `table.insert()` 的锁等待时间
   - 如果瓶颈不在表级锁（可能在行级或 I/O），则不需要此优化
2. **替换为 DashMap**：
   - `tables: RwLock<HashMap<&'static str, Arc<dyn Persistable>>>` → `DashMap<&'static str, Arc<dyn Persistable>>`
   - `ModelTable` 内部同理：`RwLock<HashMap<String, Arc<Mutex<Stored<T>>>>>` → `DashMap<String, Arc<Mutex<Stored<T>>>>`
3. **适配 API**：
   - DashMap 的 `entry()` / `get()` / `insert()` API 与 HashMap 略有不同（返回 `Ref` guard）
   - `with()` 闭包签名不变，但内部从 `RwLock::read()` → `DashMap::get()` 的 `Ref` guard
   - 注意：DashMap 的 shard 锁粒度更细，但 `list()` / `snapshot()` 需要遍历所有 shard
4. **验证**：`cargo test --workspace` + benchmark 对比

#### P3-14 待实施步骤（is_terminal 与 recover 分离）

> 问题：`Model::is_terminal()` 同时用于两个目的：(1) 跨日 purge 终态行，(2) recover 时跳过终态行。语义耦合，新增 Model 时可能误判。

1. **提取 `should_resubmit_on_recover()` 方法**：
   ```rust
   pub trait Model: ... {
       const NAME: &'static str;
       fn id(&self) -> &str;
       fn is_terminal(&self) -> bool { false }  // 保留：仅用于跨日 purge

       /// recover 时是否跳过此行（默认 = is_terminal）。
       /// 如果某 Model 终态行需要保留但 recover 时跳过，则覆写此方法。
       fn should_skip_on_recover(&self) -> bool {
           self.is_terminal()
       }
   }
   ```
2. **更新 `recover` 逻辑**：
   - `Store::recover::<T>()` 中 `filter(|r| !r.should_skip_on_recover())` 替代 `filter(|r| !r.is_terminal())`
3. **评估各 Model 的语义**：
   - `Signal`：终态（Filled/Closed）行 recover 时跳过 → `should_skip_on_recover = is_terminal` ✅（默认）
   - `Position`：终态（已平仓）行 recover 时跳过 → `should_skip_on_recover = is_terminal` ✅（默认）
   - `HistoryPosition`：终态行**需要保留**（append-only 历史）→ `should_skip_on_recover = false`（覆写）
   - `Stop`：终态行 recover 时跳过 → `should_skip_on_recover = is_terminal` ✅（默认）
   - `Trade`：终态行**需要保留**（append-only 历史）→ `should_skip_on_recover = false`（覆写）
   - `Equity`：终态行**需要保留**（append-only 历史）→ `should_skip_on_recover = false`（覆写）
4. **验证**：`cargo test --workspace`（recover 行为不变，但语义更清晰）

---

## 9. 总体评价

### 评分卡（更新后）

| 维度 | 评分 | 说明 |
|---|---|---|
| **并发安全** | ★★★★★ | 三层锁结构精确无死锁，parking_lot 不可中毒；`list()`/`snapshot_csv()` 先释放表锁消除死锁风险 |
| **持久化** | ★★★★★ | CSV 日快照 + 原子写 + 跨日裁剪安全；flush_marker 跨表一致性补偿 + CRC32 校验 |
| **扩展性** | ★★★★★ | Store 泛型注册 + blanket impl + scope 二级索引；新增 Model/交易所成本极低 |
| **解耦** | ★★★★★ | 端口/适配器模式优秀；`as_any_mut` 替换为 `LiveExecutionPort` 编译期类型安全 trait |
| **Rust 特性** | ★★★★★ | trait object / blanket impl / enum 状态机 / parking_lot / 泛型连接注册表 / CRC32 纯实现 |
| **设计模式** | ★★★★☆ | 7 种架构级/结构型/行为型模式应用恰当；混合模式务实有效 |

### 总体结论

项目架构设计**整体优秀**，核心设计决策（三层锁、泛型 Store、端口/适配器引擎、混合模式）经得起推敲。主要风险集中在**双引擎并存的技术债**（应尽快消除）和**`signal_id` 方案不一致**（破坏确定性红线）。改进建议以低风险高收益项为主，不需要大重构。

---

## 附录：审查依据文件索引

```
crates/store/src/lib.rs              — Store 核心：ModelTable / 三层锁 / Persistable
crates/store/src/persist.rs          — CSV 落盘/恢复
crates/signals/src/manager.rs        — SignalManager（混合模式示例）
crates/positions/src/manager.rs      — PositionManager（多 Model 示例）
crates/data/src/remote_client.rs     — K线 WS 客户端
crates/broker/src/trading_service.rs — 交易 WS 客户端
crates/app/src/bootstrap.rs          — 进程级 Store 创建和连接分发
crates/app/src/runtime_actor.rs      — 实盘 RuntimeActor
crates/app/src/engine.rs             — old 回测引擎
crates/app/src/spi_backtest.rs       — SPI 回测后端装配
crates/exchange-spi/src/core/runtime.rs    — ExchangeRunner 统一主循环
crates/exchange-spi/src/core/exchange.rs   — Exchange SPI trait
crates/exchange-spi/src/core/execution.rs  — ExecutionPort + SimulationExecution
crates/exchange-spi/src/bridge.rs    — LegacyStrategyBridge 适配器
crates/exchange-spi/src/exchanges/live.rs — LiveExecution + ChannelBarSource
docs/project-design.md               — 项目设计文档
```
