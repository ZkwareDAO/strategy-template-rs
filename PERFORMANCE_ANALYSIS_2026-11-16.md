# 性能深度分析（2026-11-16）

对象：`strategy-template-rs`（engine 决策主循环 / app actor / store 落盘 / data 数据面）
方法：热路径源码逐行核对 + 微基准实测（`rustc -O`，本机 WSL2 / Windows，单线程 memcpy）

---

## 0. 实测基线

| 项 | 实测值 |
|---|---|
| `size_of::<Candle>()` | **96 B** |
| `max_closed_len`（硬编码） | **2000**（`spi_backtest.rs:513`、`strategy_live_actor.rs:439`） |
| 单 slot 快照数据量 | 4 TF × 2000 × 96 B = **768 KB** |
| 20 slot 一轮 `CandleSeries` 全量拷贝 | **15.36 MB / 0.63 ms**（24.5 GB/s，热缓存最优值） |
| 特征三元组构造（20 slot × 5 指标 × 3×Vec<f64> 2000） | **4.8 MB / 0.18 ms 每轮** |

> 注：0.63 ms 是无分配器竞争、纯 L2/L3 命中下的下限。真实路径每轮还叠加 new/alloc/free 与 `BTreeMap` 节点分配，实测通常 1.5–3×；下文外推统一按 **2× 保守系数**。

**回测外推（20 标的、1 年 1m = 525,600 轮，快照填满后恒为满额）**

- `CandleSeries` 深拷贝：**8.07 TB**，纯 memcpy ≥ **5.5 min**，真实约 **11–17 min**
- 特征三元组冗余分配：**2.5 TB 分配/释放**，约 **2 min**
- 合计：**仅这两项就吞掉 13–19 分钟**——且它们 100% 是可消除的纯浪费。

---

## 1. 每轮（每根 1m bar）固定开销分解

`flush_round` 是唯一决策入口（`runner.rs:1152`），每根 base-TF bar 调一次：

| # | 位置 | 每轮动作 | 成本量级（20 slot） | 是否必要 |
|---|---|---|---|---|
| 1 | `runner.rs:1618` `closed: Some(slot.candle_cache.clone())` | 每 slot 深拷贝 768 KB | **15.4 MB / 0.6–1.5 ms** | ❌ 零拷贝可解 |
| 2 | `runner.rs:1596` `feature_plan.compute()` | 每 slot 每指标 3 个新 Vec + 全长 O(n) 计算只为取末值 | 4.8 MB / 0.18 ms | ❌ 只需 `period*3` 根 |
| 3 | `runner.rs:1191/1625/1832/1976` `master_position()` ×4 | 每次 `all_sub_positions()` 全量 clone + `multiplier_map()` → 每 slot 一次 `spec()` | 80 次 spec clone + 4 次全子仓拷贝 | ❌ 每轮算 1 次即可 |
| 4 | `runner.rs:2231/2246` `current_nav()`/`current_cash()` | 2 × N 次 `account_snapshot()`，每次 1 次 `spec()` + `Account::new(String)` | 40 次 spec clone | ⚠️ 可降到 2 次 |
| 5 | `runner.rs:1293/1318/1431/1716/2306` | 每个 action 对每个 slot `instrument_key()`（`format!`）线性比对 | O(actions × slots) 次 String 分配 | ❌ 索引化 |
| 6 | `runner.rs:1523` `persist_round()` | 每轮 upsert 全部 master + 全部 sub_position | **O(历史仓位)**，每轮增长 | ❌ 脏标记 + 节流 |
| 7 | `runner.rs:1520` `settle_masters()` | masters × slots × subs 三重扫描 | O(M×N×S)，每轮增长 | ❌ 计数器可 O(1) |

**关键观察**：第 1、2 项是常量级大额浪费，第 6、7 项是**随运行时长线性增长**的浪费。前者决定回测跑多久，后者决定实盘能跑多久。

---

## 2. 三类根因与修复

### 根因 A：快照按值深拷贝（收益最大，改动最小）

```rust
// runner.rs:1618 —— 每轮每 slot 复制整段历史
closed: Some(slot.candle_cache.clone()),
```

`CandleSeries` 内部是 `BTreeMap<CoreTimeframe, Vec<Candle>>`（`bridge.rs:63`），`Clone` 是深拷贝：BTreeMap 重建 + 每 TF 一个 Vec 重新分配。

**同时**：`CandleSeries::refresh`（`bridge.rs:66-88`）虽然写了"增量"判断，但条件是 `v.len() == q.len() && 末根 open_time 相同`——M1 每分钟长度都变，判定恒为 `!fresh`，于是每根 bar 都把 2000 根 `Bar` 重新 `map(bar_to_candle).collect()` 一遍。**即每轮做两次全量重建**（refresh 一次 + snapshot clone 一次）。

修复（两处，可分开上）：

```rust
// 1) 共享快照：把 CandleSeries 装进 Arc，快照只持 handle
pub struct InstrumentSnapshot {
    pub closed: Option<Arc<CandleSeries>>,   // 由 Option<CandleSeries> 改
    ...
}
// 决策期只读；下一轮 refresh 时用 Arc::make_mut 触发 COW（仅当有外部持有）

// 2) 真增量：series 改 VecDeque，append + pop_front
fn refresh(&mut self, state: &AggregationState) {
    for &tf in state.timeframes() {
        let q = state.closed(tf);
        let key: CoreTimeframe = tf.into();
        let dst = self.series.entry(key).or_default();
        // 尾部对齐：仅追加新增的、超出 max_len 的 pop_front
        let skip = dst.len().saturating_sub(1);      // 用末根 open_time 找水位
        if dst.back().map(|c| c.open_time) == q.get(skip).map(|b| b.open_time) {
            for b in q.iter().skip(skip + 1) { dst.push_back(bar_to_candle(b)); }
            while dst.len() > q.len() { dst.pop_front(); }
        } else {
            *dst = q.iter().map(bar_to_candle).collect();  // 跳变才重建
        }
    }
}
```

**预期收益**：回测 −11~17 min；实盘每轮少 15 MB 分配，GC 压力（分配器竞争）显著下降。

### 根因 B：全部历史重算，只为取最后一个值

`features.rs:141-143`：

```rust
let high: Vec<f64> = bars.iter().map(|b| b.high).collect();
let low: Vec<f64>  = bars.iter().map(|b| b.low).collect();
let close: Vec<f64> = bars.iter().map(|b| b.close).collect();
match kind {
    IndicatorKind::Rsi { period } => single_value(rsi(&close, *period)),  // 返回全长 Vec，只取 last
    ...
}
```

- 每个指标都重建 3 个 2000 长 Vec（48 KB），`adx/rsi/ema/atr` 返回**全长** `Vec<f64>`，`single_value` 只取 `non_nan_last`——99.9% 的算力和分配被丢弃。
- 5 个指标就是 15 次分配，其中 `high/low` 对 RSI/EMA/SMA/MACD 根本用不到。
- 且 `build_snapshots` 对 **stale slot 也照样算**（`runner.rs:1596` 无条件调用），而 stale 分支只把 `entry_eligible` 置 false（`:1601-1605`）。

修复分两档：

- **低成本档（1 小时）**：按指标按需构造切片 + 只喂 `period * 3` 尾部，`single_value` 改为只读尾值；stale slot 跳过 feature 计算（或延后到策略真正读取时）。
- **正解档（半天）**：`FeaturePlan` 持 per-indicator 增量状态（EMA/RSI/ATR/ADX 都是 O(1) 递推），每根 bar 只 O(指标数)。

**预期收益**：回测 −2 min 分配 + 指标 CPU 从 O(2000×指标) 降到 O(指标)。

### 根因 C：`spec()` 每次 clone 一份合约规格

```rust
// trading.rs:287-292
fn spec(&self, instrument: &InstrumentId) -> Result<Arc<InstrumentSpec>> {
    let key = instrument.namespaced_key();          // format! 分配
    if let Some(s) = self.specs.get(&key) {
        return Ok(Arc::new(s.clone()));             // 深拷贝 + 新建 Arc
    }
```

`InstrumentSpec` 含 `String currency` + `FeeSchedule`；`namespaced_key()` 每次 `format!("{}/{}/{}")`。调用遍布每轮：`master_position` 的 `multiplier_map()`（N 次）、`current_nav/cash`（2N 次）、`account_snapshot`（N 次）、每笔成交/每条腿的 `validate/fee/margin/pnl`。

修复：

```rust
specs: BTreeMap<String, Arc<InstrumentSpec>>,      // 存 Arc
fn spec(&self, i: &InstrumentId) -> Result<Arc<InstrumentSpec>> {
    // 优先用 slot 预计算的 key（StrategySlot 加 field key: String）
    self.specs.get(i.key_str()).map(Arc::clone).ok_or(...)
}
```

**预期收益**：每轮 ~120 次 `format!` + 深拷贝归零（约 30–60 µs/轮）；回测再省 15–30 min 的合计 CPU（分散在各处）。

---

## 3. 增长型开销（决定实盘能不能连跑几周）

| 容器 | 位置 | 现状 | 后果 |
|---|---|---|---|
| `sub_positions: Vec<SubPosition>` | `runner.rs:66` | Closed 永不删，8 处 `iter().find()` | 每天每次事件 O(历史仓位数) |
| `masters: BTreeMap<String, MasterPosition>` | `runner.rs:188` | 只增不减，6 处全表遍历 | `settle_masters`/`persist_round` O(M) |
| `summary.fills: Vec<Fill>` | `runner.rs:259` | `take_fills()` 实盘侧从未调用 | 每单 5–6 String 永久驻留 |
| `orders`/`states` | `live.rs:140/142` | 只 insert，无 remove | 每单完整 `OrderRequest` 永久驻留 |
| `pending` 在途表 | `broker/trading_service.rs:735` | 超时条目永不淘汰 | 表无界增长 + FIFO 错配 |

**增长曲线**（趋势策略、日 3 单、20 标的）：

| 运行时长 | masters | sub_positions | 每轮 `persist_round` upsert 行数 |
|---|---|---|---|
| 1 天 | ~60 | ~60 | ~120 |
| 1 月 | ~1,800 | ~1,800 | ~3,600 |
| 1 年 | ~21,900 | ~21,900 | ~43,800 |

而 `persist_round` 是**每根 bar 无条件调用**（`runner.rs:1523`），每行一次行锁 + clone：一年后每根 bar 要 upsert 4.4 万行——**单轮从 µs 级劣化到 100 ms 级**，且 `settle_masters` 的三重扫描同步劣化。这正是"回测跑得越到后面越慢"和"实盘跑了几天后开始卡"的同一个根因。

修复（三件事）：

1. **脏标记节流**：`subs_changed` 已存在但未被用于门控 persist。改为 `if self.subs_changed || now - last_persist > 5s { persist_round(); }`，非决策轮直接跳过。
2. **归档**：Closed 的 master/sub_position 移入按日归档（或直接从内存删、只留 store 行），活动集合保持 O(活跃仓位)。
3. **计数器**：`master_id → live_sub_count: u32`，`settle_masters` 从 O(M×N×S) 降到 O(1) 判定。

**预期收益**：长跑不再随时间劣化；回测后期提速数倍（回测里 `persist_round` 也是每 bar 跑）。

---

## 4. 阻塞与调度

### 4.1 `store.flush()` 在 async 事件循环里同步执行

`strategy_live_actor.rs:932`（周期 flush）与 `:982`（收口）：

```rust
if now >= next_flush {
    if let Err(e) = k.store.flush() { ... }   // 全表序列化 + 全量重写 + fsync
    next_flush = now + flush_every;
}
```

actor 跑在 `new_current_thread()` runtime（`:129`）。flush 期间：**行情消费、订单回报、定时对账、提交全部停摆**。flush 耗时随行数线性增长（每表 `snapshot()` → `serde_json` → 写盘 → `fsync`），且 `Store::flush` 全程持 `last_flush_day` Mutex（`store/lib.rs:549`），并发调用完全串行。

同仓 `spi_live_actor.rs:1021-1026` 已是正确写法——两条链路口径不一致，直接对齐：

```rust
let store = Arc::clone(&k.store);
tokio::task::spawn_blocking(move || store.flush()).await??;
```

**注意**：`Store` 是单写者语义（actor 线程独占），`spawn_blocking` 期间 actor 若继续 `upsert` 会与 flush 的 `snapshot()` 竞争——正确做法是**先取快照（快，锁内）→ 后台写盘（锁外）**，需要把 `flush` 拆成 `snapshot_all()` + `write_all()` 两段。

### 4.2 50 ms 忙轮询

`strategy_live_actor.rs:947-949`：`AlignPoll::Starved → sleep(50ms)`。

- 每根 bar 的处理延迟最坏 **+50 ms**（对 1m 策略是 0.08% 的时点抖动，对入场价敏感策略有实感）
- 20 Hz 空转 × slots 次 `recv_timeout(ZERO)`（`live.rs:733`）
- 新 bar 到达后最多等 50 ms 才被取走

修复：`ChannelBarSource` 改用 `tokio::sync::mpsc` + `Notify`，事件循环 `select!` 在 `notified()` 上等待，超时退化为 `SourceIdle` 处理控制面。延迟从 ≤50 ms 降到 µs 级，空转归零。

### 4.3 阻塞 IO 散布在 async 上下文

| 位置 | 问题 |
|---|---|
| `bootstrap.rs:510-514` | 每根 bar 持 `sinks.lock()` 同步写 CSV；Lagged 补缺时突发批量写 |
| `ts_kline.rs:407-414` | `backfill` future 内 `read_dir` 递归 + `load_csv` 全文件解析 |
| `archive_download.rs:408-423/450/491` | async 内全量 `load_csv`/`save_csv`；`bootstrap.rs:292` 用 `join_all` 并发调 → 多 symbol 同时占满 worker |

统一 `tokio::task::spawn_blocking` 包裹，或专用写盘线程 + 有界通道。

---

## 5. 数据面（离线/预热，影响启动时间）

1. **同一 CSV 读两遍** — `warmup.rs:76-77` 调 `load_csv` 取全量，`LiveCsvSink::open` 内部（`csv_store.rs:245`）再 `load_csv` 一次只为拿末根时间戳。一年 1m ≈ 52.5 万行 → 双倍解析。改 `load_csv` 返回 `(Vec<Candle>, last_ts)` 透传。
2. **每根 K 线一次 `flush` 系统调用** — `csv_store.rs:266-278`：`BufWriter::flush` 每行触发一次 write syscall，补一年数据 = 50 万次。改为批次结束/`Drop` 时 flush。
3. **整表三份驻留** — `archive_download.rs:450-491`：`existing` + `fetched` + `merge_rows` 的 HashMap 同时存在，再加 `save_csv` 全量重写。一年 1m ≈ 3 × 年数据内存。按块流式合并。
4. **回测全量数据复制 3 份** — `main.rs:429-443`：`all` → `replay` → `warmup` + `trading` 各 `.collect()`。改切片借用 `&all[a..b]`。

---

## 6. 优化路线图

### 第一批（半天，立竿见影）

| 改动 | 文件 | 预期收益 |
|---|---|---|
| `closed: Option<Arc<CandleSeries>>` | `runner.rs:1618`、`port.rs` | 回测 −11~17 min |
| `specs: BTreeMap<String, Arc<InstrumentSpec>>` + slot 缓存 key | `trading.rs:269/287`、`runner.rs:102` | 每轮 −120 次 format!+clone |
| `master_position()` 每轮算 1 次并传参 | `runner.rs:1191/1625/1832/1976` | −3 次全子仓拷贝/轮 |
| stale slot 跳过 feature 计算 | `runner.rs:1596` | 视 stale 比例 |
| `persist_round` 脏标记门控 | `runner.rs:1523` | 回测后期数倍提速 |

### 第二批（1–2 天，结构性）

| 改动 | 预期收益 |
|---|---|
| `CandleSeries` 改 `VecDeque` + 真增量 | 每轮 −15 MB 分配 |
| 指标增量状态机（O(1)/bar） | 指标 CPU 降 2 个数量级 |
| `key → idx` 索引替代线性 `position` + `format!` | 每轮 −数百次 String 分配 |
| `live_sub_count` 计数器替代三重扫描 | `settle_masters` O(M×N×S) → O(1) |
| 五个无界容器加归档/裁剪 | 长跑不再劣化 |

### 第三批（关联正确性）

| 改动 | 说明 |
|---|---|
| `flush` 拆 `snapshot_all` + 后台写盘 | 消除事件循环停摆 |
| 通知驱动替代 50 ms 轮询 | 延迟 ≤50 ms → µs |
| 在途表 `HashMap` + 死条目淘汰 + 写超时 | 同时修资金级错单风险 |

---

## 7. 怎么验证（不要凭感觉）

```bash
# 1) 回测基线：记录 wall time + 峰值 RSS
cargo run --release -- backtest --config config/<name> --out /tmp/bt_before

# 2) 每轮耗时热点：临时在 flush_round 首尾打点（debug 级）
#    t0 = Instant::now(); ... debug!(elapsed_us = t0.elapsed().as_micros());

# 3) 分配热点（Linux/WSL）：heaptrack 或 jemalloc profiling
heaptrack ./target/release/strategy-rs backtest ...

# 4) 回归门禁：固定数据 + 固定区间，比较 rounds/s 与峰值 RSS
```

验收标准建议：同一份 1 年 20 标的回测，**rounds/s 提升 ≥ 3×、峰值 RSS 下降 ≥ 40%**，且 `backtest_result.json` 的 trades/NAV 曲线逐行一致（性能优化不得改变输出）。
