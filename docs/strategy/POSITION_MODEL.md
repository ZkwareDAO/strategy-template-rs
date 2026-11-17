# 策略总仓与子仓模型（已实现）

> 范围：`crates/engine/src/strategy/{model,runner,dispatch,port}.rs`。
> 状态：已实现并持久化；本文记录**当前代码契约与不变量**，不是演进提案。
> 维护规则：仓位行为改动必须同 PR 更新本文与 [XSEC_GUIDE.md](XSEC_GUIDE.md)。

## 0. 一句话结论

```text
StrategyPortfolio（策略实例账本，1 个）
  └─ MasterPosition（策略总仓，至多 1 个非 Closed）
       ├─ SubPosition（标的子仓，0..N）
       └─ ...
```

- 总仓只保存**生命周期**；`gross_notional`、`net_notional`、`avg_entry_price`、
  `realized_pnl`、`unrealized_pnl`、`sub_position_count` 全部由非 `Closed` 子仓实时派生。
- `MasterPositionView` 是只读聚合视图，不落表。
- 策略只读 `PortfolioContext.master` / `sub_positions`，不得自己维护仓位状态。

## 1. 数据结构

| 结构 | 位置 | 关键字段与职责 |
|---|---|---|
| `StrategyPortfolio` | `model.rs` | 每策略一行：`phase`、`active_master_id`、`master_seq`、`nav`、`high_watermark`、`drawdown`、`peak_unrealized_pnl`、`trail_armed`、`max_total_notional`、`rebalance_epoch` |
| `MasterPosition` | `model.rs` | `master_id`（`master_{seq:06}`）、`strategy_id`、`state`、`opened_at`、`closed_at`；只存生命周期 |
| `SubPosition` | `model.rs` | 唯一持仓实体：`id` = 开仓 `sub_` signal_id、`master_id`、`instrument`、`side`、`qty`（正幅值）、`entry_price`、`stop_price`、`state`、`realized_pnl` |
| `MasterPositionView` | `model.rs` | 只读派生聚合，按 `instrument.namespaced_key()` 字典序排序后计算，保证 f64 求和逐位确定 |
| `MasterContext` | `port.rs` | 策略可见快照：`master_id`、`state`、`view` |
| `StrategySlot` | `runner.rs` | 单标的运行时切片：子仓列表、`last_mark`、`last_bar_at`、执行/行情部件 |

聚合口径：

- 只聚合 `state != Closed` 的子仓。
- `gross_notional` = 成本名义 `Σ|qty * entry_price * multiplier|`，是 L3 百分比分母。
- `net_notional` = 带方向数量 `Long 正 / Short 负` 的成本名义和。
- `avg_entry_price` 按 `qty` 幅值加权。
- `unrealized_pnl` = `Σ signed_qty * (mark - entry) * multiplier`；mark 缺失用 entry 兜底，不产生 NaN。

命名：

- 总仓：`master_{seq:06}`，序号来自 `StrategyPortfolio.master_seq`，只在本地账本使用。
- 子仓：`sub_<hash>`，开仓确定性信号 ID；平仓腿为 `sub_<hash>_leg{i}`。
- 旧数据迁移总仓：`master_legacy`。

## 2. 状态机与不变量

### 2.1 总仓

```text
无总仓 --第一个子仓开仓--> Open
Open --CloseMaster / L3--> Closing
Open --所有子仓 Closed 且无在途--> Closed
Closing --所有子仓 Closed 且无在途--> Closed
```

事件只有两个：`StartClosing`、`AllSubsClosed`。`Closed` 是终态；
**只有 `AllSubsClosed` 能进入 `Closed`**，平总仓动作本身不能直接关总仓。

### 2.2 子仓

```text
PendingOpen -> OpenRequested -> Active -> CloseRequested -> Closed
任意非终态 --RemoteFlat--> Closed
```

- `PendingOpen` / `OpenRequested`：订单在途，未形成持仓。
- `Active`：实际持仓，只有它能被平仓或调仓。
- `CloseRequested`：平仓在途，重复平仓请求 no-op。
- `Closed`：终态，Store 跨日 flush 时按 `Model::is_terminal` 裁剪。

### 2.3 不变量

- 1 个策略同时至多 1 个非 `Closed` 总仓；1 个总仓下同一 `instrument` 至多 1 个非 `Closed` 子仓（G1）。
- 总仓 `Closed` 当且仅当关联子仓全部 `Closed`，且没有 `PendingOpen/OpenRequested` 在途。
- 总仓 `Closing` 时拒绝新的 `Open` 与**增大持仓幅值的 `Adjust`**；允许平仓/减仓。
- 子仓平掉后总仓自然收口；下一轮开仓创建新总仓，不复用已 `Closed` 总仓。
- `StrategyPortfolio.phase` 管交易阶段（`Running/Paused/Liquidating`），`MasterPosition.state` 管仓位周期，两者不可混用。

## 3. 开仓链路

1. 策略返回 `Open` 或空仓 `Adjust`；`dispatch_actions` 按 `instrument_key()` 升序派发。
2. `Open` 与“增大持仓幅值的 `Adjust`”受 entry 硬门约束：`entry_eligible == false` 直接计拒单。
3. 需要总仓的动作先 `ensure_active_master`：
   - 只复用 `Open` 总仓；`Closing` 总仓不得接收新子仓。
   - 无可用总仓时创建 `master_{seq:06}` 并递增 `master_seq`。
   - 纯平仓轮次不得创建空总仓。
4. 计算数量：
   - 本地撮合：`qty = nav * min(per_position_pct, max_position_pct) / (price * multiplier)`。
   - live 场所：`ExecutionPort::venue_order_notional()` 返回固定单笔名义（trading-service
     `order_cash`），影子数量 `qty = order_cash / (price * multiplier)`，使本地
     `gross_notional` 立即按场所真实敞口预留容量。
5. 容量裁剪：`remaining = max_total_notional * nav - master.gross_notional`。
   - 本地撮合：`qty` 压到剩余容量，容量不足拒单。
   - live：`remaining < order_cash` 直接拒单（场所无法按更小名义下单）。
6. `dispatch::open_position` 做 G1 检查，生成确定性 `sub_` ID，写入
   `PendingOpen` 子仓并提交 `Offset::Open` 订单。
7. 受理后 `PendingOpen -> OpenRequested`；被拒则删除该行并清理 `open_meta`，计 `rejected`。
8. 成交归属：
   - 按 `signal_id` 命中在途子仓：`FillOpen -> Active`，写入成交价/数量/时间。
   - 未命中但同 `instrument` 已有非 `Closed` 子仓：加权并入既有子仓（加仓），不新建第二行。
   - 都没有：新建 `Active` 子仓。
   - `master_for_fill` 允许成交落在 `Closing` 总仓：成交是既成事实，随后立即补平。

空仓 `Adjust` 在实现上等价于开仓（`open_position`），也会创建总仓与 `sub_` ID。

## 4. 平仓链路

### 4.1 平单个子仓

1. `dispatch_actions` 对 `Close` 不做 entry 门控。
2. 只允许 `Active` 子仓：`PendingOpen/OpenRequested` 计拒单；`CloseRequested` no-op，防重复平仓。
3. `dispatch::close_position` 取子仓 `qty`，按 `PositionRules::split_close` 拆平仓腿，
   腿 ID 为 `{base}_leg{i}` 的 `reduce_only` 订单。
4. 提交成功后 `Active -> CloseRequested`；全部腿被拒则保持 `Active`，下一轮可重试。
5. 成交后扣减 `qty`、累计 `realized_pnl`；剩余量归零进入 `Closed`。

### 4.2 平总仓

`InstrumentAction::CloseMaster { reason, is_stop_loss }`：

1. `dispatch_actions` 先处理 `CloseMaster`，不按 instrument 循环。
2. 总仓 `Open -> Closing`；已是 `Closing` 则继续；已 `Closed` 为 no-op。
3. 对每个关联的 `Active` 子仓发平仓单。`PendingOpen/OpenRequested` 不发单，
   等成交落地后由 `close_inflight_under_closing` 立即补平。
4. 所有子仓 `Closed`、无在途开仓/减仓后，`settle_masters` 把总仓置 `Closed`，
   并清空 `active_master_id`。
5. L3 `portfolio_exit` 复用 `close_master`；全部清零后 `phase` 从 `Liquidating` 转 `Paused`。

顺序必须是“总仓先 `Closing`，再变更子仓状态”，不能先关总仓再平子仓。

### 4.3 收口时机

`settle_masters` 在决策轮、对账、成交同步后运行：

```text
若总仓非 Closed，且其关联子仓全部 Closed：
    AllSubsClosed -> Closed，清空 active_master_id
```

这条规则同时收口“开仓被拒后遗留的空总仓”和“平掉最后一个子仓后的空总仓”。

### 4.4 横截面调仓

- 两阶段：先平旧子仓（生成 `reduction_ids`），成交确认后按 epoch+1 发新开仓
  （`pending_adds`），避免同一轮先开后平导致容量与归属混乱。
- 调仓只在 `rebalance_due == true` 且无在途减仓/加仓时发生。
- 旧子仓平掉、新子仓开仓都挂在同一总仓下；总仓跨 rebalance 保留。
- 本轮无新标的且所有子仓平掉，总仓自然 `Closed`；下一轮开仓创建新总仓。

## 5. 资金、容量与拒单口径

| 口径 | 公式 / 规则 |
|---|---|
| 策略总预算 | `max_total_notional * nav`（`risk.max_total_notional`，经 `PortfolioContext.max_total_notional` 暴露） |
| 单仓目标比例 | `sizing.per_position_pct`，可被 `sizing.max_position_pct` 收紧 |
| 等权目标数量 | `weight * max_total_notional * nav / (price * multiplier)`；**不得用全额 NAV** |
| 总仓剩余容量 | `max_total_notional * nav - master.gross_notional` |
| 子仓硬上限 | `max_position_pct * nav`；已超限仓位只禁止继续加仓，不强制减仓 |
| live 单笔名义 | `order_cash`；启动校验 `order_cash ≤ min(per_position_pct, max_position_pct) * initial_cash_total` |

拒单与静默跳过必须分清：

- `rejected`：策略动作被门控/容量/状态拒绝，计入 `rejected_actions`。
- live 加仓：目标差额小于单笔名义、或剩余容量/子仓预算不足，视为已达标**静默跳过**，
  不计 `rejected`——场所无法按更小名义下单，反复计拒单只会污染指标。
- 在途子仓（`PendingOpen/OpenRequested/CloseRequested`）上的 `Adjust` 视为等待，
  静默延后到下一轮，不拒单。

NAV 口径：

- 策略 NAV 是策略总资金。paper/live 每个 slot 以完整总资金虚拟起账时，
  引擎汇总 `current_nav` / `current_cash` 会扣除重复起账现金，`PortfolioContext.nav` 不虚高。
- `initial_cash_total` 是组合总初始权益；回测与 live 使用同一口径。

## 6. 信号与下单映射

`sub_` ID 哈希输入：

```text
strategy_type | strategy_id | venue_key | ts_ms | side | intent | rebalance_epoch
```

- 开仓使用决策轮的 `round.at`；平仓使用 `slot.last_bar_at`。
- 同一输入永远得到相同 ID，实盘把它当幂等键。
- 平仓腿在 base ID 后追加 `_leg0`、`_leg1`。
- `master_id`、`namespaced_key` 只用于本地账本与信号哈希，不进入交易所帧。

发送链路：

```text
InstrumentAction
  -> OrderRequest（完整 InstrumentId + sub_ signal_id）
  -> 回测：SimulationExecution 本地撮合，产生 Fill
  -> 实盘：SignalIntent -> place_order JSON -> 交易所 WebSocket
```

| 场景 | 帧内关键字段 |
|---|---|
| 开仓 | `order_cash`、`leverage`、`signal_id`；`price/stop_price` 不进帧 |
| 平仓 | `quantity`、`reduce_only: true`、`signal_id`（腿 ID） |
| CTP 平今/平昨 | 额外带 `offset: close_today / close_yesterday`；crypto 净仓省略 |

crypto 与期货在数量单位、乘数、保证金、时段、平今上的完整差异见
[CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)。

实盘 `submit` 返回 `accepted=true` 仅表示进入信号管道；最终以异步 ack / order update 落回，
再经 `sync_slot_fills -> apply_sub_fill` 写回子仓状态。回测的“发送”是本地函数调用。

## 7. 持久化、重启与迁移

- Store 行：`strategy_portfolio`、`sub_position`、`master_position`，均按 `strategy_id` 分 scope。
- `recover_from_store` 顺序：盘载三类行 -> 重建 slots 与执行层 `PositionState` ->
  迁移旧数据 -> `Running` -> 远程对账 -> 持久化。
- 旧数据迁移：`SubPosition.master_id` 为空串时归入 `master_legacy`；已迁移数据幂等。
- `max_total_notional` 是**当前配置**而非历史状态：重启恢复后用配置值覆盖盘载值，
  避免改配置后容量缓冲被旧值静默覆盖。
- 对账期间开仓订单在途时不得提前收口总仓；以远程账本为最终事实。

## 8. 不变量回归清单

- 1:1：首个子仓开仓自动建总仓；平最后一个子仓后总仓自动 `Closed`。
- 1:N：多个子仓同一总仓；只平一个不关总仓；`CloseMaster` 等全部子仓 `Closed` 后才关总仓。
- 在途开仓遇到 `CloseMaster`：总仓保持 `Closing`，成交后立即补平，不提前 `Closed`。
- 重启恢复：账本、总仓指针、旧数据迁移、对账顺序正确。
- 子仓/总仓双层上限、live `order_cash` 启动校验、live 加仓静默跳过均有测试。
- 信号 ID 黄金值不变；`strategy_type` / 注册名 / 配置名一致。

```bash
cargo test -p strategy-engine
cargo test -p strategy-strategies
cargo test --workspace
```
