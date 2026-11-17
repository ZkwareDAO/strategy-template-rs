# AI 编码红线（生成/修改策略时必须遵守）

> 本文件是硬约束。生成的每一行策略代码都要过一遍下面的清单。

## 禁止

| # | 禁令 | 理由 |
|---|---|---|
| 1 | 禁调用 `now()` / 取系统时间 | 一律 `snapshot.bar.open_time`；回测要逐字节可复现 |
| 2 | 禁读未闭合 K 线 / 绕过 `snapshot.closed` 自建数据源 | 只用 `snapshot.closed.closed(tf)`；未来函数在类型层隔离 |
| 3 | 禁在策略内做 IO（文件/网络/日志落盘） | 策略是纯计算 |
| 4 | 禁绕过 `register_strategy!` 或改 `build.rs`/`registry.rs` | 注册机制模板冻结 |
| 5 | 禁 `#[serde(default)]` 不加 `deny_unknown_fields` | 参数拼错必须启动即报错 |
| 6 | 禁丢弃指标 NaN 段而不做 warmup 长度检查 | 显式检查 `closed(tf)` 返回序列长度 |
| 7 | 禁从 `timeframes()` 移除 `Timeframe::M1` | 聚合器据此注册 |
| 8 | 禁在 `on_cross_section_bars` 主路径 `unwrap()`/`expect()` | 用 `Option`/`?` 静默降级 |
| 9 | 禁在策略里改 `snapshot`/`portfolio` 之外的状态存储 | 仓位由引擎 `MasterPosition`/`SubPosition` 账本持有 |
| 10 | 禁用未来函数（如拿当前未闭合 high/low 做已闭合判定） | 同 #2 |
| 11 | 禁 `strategy_type()` / 注册名 / 配置名三者不一致 | 配置键：`strategy.yaml` 用 `strategy_type:`，旧 per-symbol 用 `strategy:`；不一致会让 signal_id 与报表路径分裂 |
| 12 | 禁参数默认值与 `config/*/example.yaml` 不一致 | 文档即契约 |
| 13 | 禁无测试新增策略 | 每个策略至少覆盖入场 + 出场 |
| 14 | 禁在 `on_cross_section_bars` 里累积增量指标状态 | 指标全量重算 |
| 15 | 禁在 `portfolio.master.state == Closing` 时开仓/加仓/调仓 | 新仓会逃过本次总仓平仓；`StrategyBase` 已内置门控 |
| 16 | 禁用全额 `portfolio.nav` 计算等权目标 | 必须乘 `portfolio.max_total_notional`；否则目标恒高于执行层容量，每轮产生无谓拒单 |
| 17 | 禁用逐标的 `Close` 模拟组合平仓 | 组合级退出用 `CloseMaster`，由引擎保证“总仓先 Closing、子仓全平后 Closed” |
| 18 | 禁在策略内维护 `master_id`/子仓状态/总仓状态 | 账本由引擎持有，策略只读 `PortfolioContext` 与 `InstrumentSnapshot` |
| 19 | 禁在策略/`params` 里写死合约乘数、手数、保证金率、手续费 | 唯一来源 `config/contracts.yaml`；写死导致换品种即错 |
| 20 | 禁期货 symbol 未在 `config/contracts.yaml` 声明就回测 | 未声明会静默退化为 crypto 兼容模式（小数数量 + `fee_rate`），结果不可信 |
| 21 | 禁自行拼接期货日盘/夜盘/午休或按 UTC 日期断交易日 | 聚合由引擎 `SessionWindowPolicy` 完成，策略只读 `closed(tf)` |
| 22 | 禁假设 7x24，或自行模拟平今/平昨 | 平仓 `offset` 由执行层生成；策略只发 `Close`/`CloseMaster`/`Adjust` |
| 23 | 禁在用户未提供期货 K 线位置时开始回测，或用 mock/合成/其他品种数据顶替 | 结果不可信；必须先拿到逐合约目录或 1m CSV 路径 |

## 必须

| # | 要求 |
|---|---|
| 1 | 用 `StrategyBase` 时实现 `StrategyLogic` 的 `name/top_k/score/entry_allowed/stop`；直接实现 port 时实现 `strategy_type` / `timeframes` / `on_cross_section_bars` |
| 2 | 文件尾调用一次 `register_strategy!` |
| 3 | 为 `Params` 提供 `Default` |
| 4 | 至少覆盖入场 + 出场两类测试（建议加数据不足、方向冲突、stale/缺特征、风险预算） |
| 5 | 在 `config/<name>/` 放可跑的 `strategy.yaml` 或 `example.yaml` 样例 |
| 6 | 保证 `strategy_type()` == 注册名 == 配置 `strategy_type:`（旧 per-symbol 为 `strategy:`） |
| 7 | 用 `closed(tf)` 返回序列的长度检查做 warmup 门控 |
| 8 | `timeframes()` 包含 `Timeframe::M1` |
| 9 | 给出止损（`StrategyLogic::stop`、`Open.stop_price` 或 `UpdateStop`）；用户未指定止损时先询问或显式写明假设，不得静默发明 |
| 10 | 等权目标数量乘 `portfolio.max_total_notional`，不用全额 NAV |
| 11 | 期货策略补 `multiplier != 1`、整数手取整、时段聚合用例；配置显式 `initial_cash` |
| 12 | 提交前自检 [REVIEW_CHECKLIST.md](REVIEW_CHECKLIST.md) |
