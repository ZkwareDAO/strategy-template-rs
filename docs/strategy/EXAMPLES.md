# Example 逐段讲解

## example_dual_ma（入门）

**看什么**：一个完整策略的最小构成。

| 段落 | 位置 | 要点 |
|---|---|---|
| 文件头 `//!` | 顶部 | 本文件展示的 5 件事 + 红线 |
| `Params` | 中部 | `deny_unknown_fields, default`；`Default` 实现 |
| `trend_direction` | impl 块 | `closed_data_min` + `talib::ema` + `.last()` |
| `on_candle` | Strategy impl | 冷却检查 → 趋势判断 → `Action::Open` |
| `on_realtime_tick` | Strategy impl | 止损 → 反向交叉 → `Action::Close` |
| `register_strategy!` | 文件尾 | 唯一"胶水"，改名字即可 |
| `mod tests` | 末尾 | `FakeSource: ClosedSource` + 4 个测试 |

**对应 Python 模板**：`strategies/example_ma_cross/`（单周期双均线）。
差异：Rust 用 `Strategy` trait 替代三层基类；仓位由引擎注入 `ctx.position`，
策略不自己持有状态。

## example_mtf_trend（多周期）

**看什么**：跨周期读数据与多周期指标。

| 段落 | 要点 |
|---|---|
| `timeframes()` | 返回 `[M1, H4, D1]`——1m 引擎时钟 + 4h 触发 + 1d 趋势 |
| `trend_allows_long` | 读 `Timeframe::D1`，独立 `MIN_ROWS_TREND` 长度检查 |
| `signal_bullish` | 读 `Timeframe::H4` |
| `atr` | `talib::atr(highs, lows, closes, period)` |
| 入场 | 趋势与信号**同向才开仓**，冲突保持空仓 |
| 出场 | 硬止损（ATR 距离）+ 4h 反向交叉 |

**对应 Python 模板**：`strategies/example_mtf_trend/`（多周期趋势）。
差异：Python 用 `_get_indicator_timeframes()` 声明额外周期；Rust 一律
`timeframes()` 一次声明，`closed_data(tf)` 按周期取。

## 与 Python 的语义差异速查

| 概念 | Python | Rust |
|---|---|---|
| 策略入口 | `analyze()` / `check_realtime_exit()` | `on_candle()` / `on_realtime_tick()` |
| 持仓状态 | `BaseState` 自持 + JSON 持久化 | `ctx.position: Option<PositionView>`（引擎注入） |
| 时间 | `datetime.now()` 禁用，取 K 线时间 | `ctx.event_time`（无 `now()`） |
| 注册 | `importlib` 加载 `strategy.py` | `register_strategy!` + `build.rs` 自动发现 |
| 参数 | overrides YAML | `Params` + 配置 `params:` 节 |
