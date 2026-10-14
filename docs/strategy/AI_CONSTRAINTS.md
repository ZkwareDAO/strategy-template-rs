# AI 编码红线（生成/修改策略时必须遵守）

> 本文件是硬约束。生成的每一行策略代码都要过一遍下面的清单。

## 禁止

| # | 禁令 | 理由 |
|---|---|---|
| 1 | 禁调用 `now()` / 取系统时间 | 一律 `ctx.event_time`；回测要逐字节可复现 |
| 2 | 禁读未闭合 K 线 / 绕过 `Ctx` 自建数据源 | 只用 `ctx.closed_data*()`；未来函数在类型层隔离 |
| 3 | 禁在策略内做 IO（文件/网络/日志落盘） | 策略是纯计算 |
| 4 | 禁绕过 `register_strategy!` 或改 `build.rs`/`registry.rs` | 注册机制模板冻结 |
| 5 | 禁 `#[serde(default)]` 不加 `deny_unknown_fields` | 参数拼错必须启动即报错 |
| 6 | 禁丢弃指标 NaN 段而不做 warmup 长度检查 | 用 `closed_data_min(tf, n)` |
| 7 | 禁从 `timeframes()` 移除 `Timeframe::M1` | 聚合器据此注册 |
| 8 | 禁在 `on_candle`/`on_realtime_tick` 主路径 `unwrap()`/`expect()` | 用 `Option`/`?` 静默降级 |
| 9 | 禁在策略里改 `Ctx` 之外的状态存储 | 仓位由引擎（PositionManager）持有 |
| 10 | 禁用未来函数（如拿当前未闭合 high/low 做已闭合判定） | 同 #2 |
| 11 | 禁 `strategy_type()` / 注册名 / 配置 `strategy:` 三者不一致 | signal_id 与 CSV 路径会分裂 |
| 12 | 禁参数默认值与 `config/*/example.yaml` 不一致 | 文档即契约 |
| 13 | 禁无测试新增策略 | 每个策略至少覆盖入场 + 出场 |
| 14 | 禁在 `on_candle` 里累积增量指标状态 | 指标全量重算 |

## 必须

| # | 要求 |
|---|---|
| 1 | 实现 `strategy_type` / `timeframes` / `on_candle`（`on_realtime_tick` 可选） |
| 2 | 文件尾调用一次 `register_strategy!` |
| 3 | 为 `Params` 提供 `Default` |
| 4 | 至少覆盖入场 + 出场两类测试（建议加数据不足、方向冲突） |
| 5 | 在 `config/<name>/` 放 `example.yaml` 样例 |
| 6 | 保证 `strategy_type()` == 注册名 == 配置 `strategy:` |
| 7 | 用 `ctx.closed_data_min(tf, n)` 做 warmup 长度检查 |
| 8 | `timeframes()` 包含 `Timeframe::M1` |
| 9 | 给出止损（`stop_price` 或 `UpdateStop`） |
| 10 | 提交前自检 [REVIEW_CHECKLIST.md](REVIEW_CHECKLIST.md) |
