# 策略清单与源码导读

本文件只做“看什么、去哪里看”的导读，不逐行贴代码。源码是唯一事实源。
模板仓库只保留 **2 个策略模板**：`template_trend`（逐标）与 `template_xsec`（横截面）。

## 1. 两个模板

| 文件 | 定位 | 看点 |
|---|---|---|
| `template_trend.rs` | 逐标趋势（单标 / N 标独立） | `Params` + `Default`、`timeframes()`、EMA 交叉与趋势过滤、百分比/ATR 止损、`register_strategy!`、`mod tests`；同一份代码配 `config/tpl_trend_crypto/` 与 `config/tpl_trend_futures/` |
| `template_xsec.rs` | 横截面轮动（N=1 退化为单标） | 参数化排名 `rank_indicator`、`top_k` 等权 `Adjust`、`flat_entry` 首建、趋势过滤、ATR/百分比止损、掉出 `Adjust{0}` |

两个模板都直接实现 `CrossSectionalStrategyPort`；`common/mod.rs` 的
`StrategyBase` 是可选写法（只写 `name/top_k/score/entry_allowed/stop` 五个钩子），
基类契约测试在 `crates/strategies/tests/strategy_base.rs`。

## 2. 市场与形态覆盖

| 配置 | 策略 | universe | 关键点 |
|---|---|---|---|
| `config/tpl_trend_crypto/strategy.yaml` | `template_trend` | BTCUSDT | `dual_ma` 交叉 + 百分比止损 |
| `config/tpl_trend_futures/strategy.yaml` | `template_trend` | CU（SHFE） | `ema_trend` + 趋势过滤 + ATR；`contracts.yaml` 乘数/整数手 |
| `config/tpl_xsec_crypto/strategy.yaml` | `template_xsec` | BTCUSDT / ETHUSDT | MACD 排名 + 等权调仓 |
| `config/tpl_xsec_futures/strategy.yaml` | `template_xsec` | RB / I / CU | `merge_positions: true` + 趋势过滤 + ATR |

crypto 与期货的差异（合约乘数、整数手、日盘/夜盘、平今、数据落位）见
[CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)，不要从策略代码里猜。

## 3. `params` 速查

| 策略 | `params` 字段（默认） |
|---|---|
| `template_trend` | `entry_mode: ema_trend`、`fast_period: 10`、`slow_period: 30`、`trigger_ema: 60`、`trend_tf: ""`、`trend_ema: 60`、`stop_mode: atr`、`stop_loss_pct: 2.0`、`atr_period: 14`、`atr_stop_mult: 2.0`、`exit_price_source: close`、`allow_short: true` |
| `template_xsec` | `rank_indicator: macd`、`higher_is_better: true`、`top_k: 3`、`flat_entry: true`、`trend_tf: ""`、`trend_ema: 60`、`stop_mode: percent`、`stop_loss_pct: 2.0`、`atr_period: 14`、`atr_stop_mult: 2.0` |

规则：配置 `params` 必须能反序列化进对应结构（`deny_unknown_fields`）；
多标的 `params.top_k` 与 `rebalance.top_k` 保持同值。

## 4. 建议阅读路径

1. `template_trend.rs`：逐标决策的最小完整实现（入场、止损、反向离场）。
2. `template_xsec.rs`：横截面排名、等权调仓、掉出清零与 N=1 单标分支。
3. `common/mod.rs`：可选的 `StrategyBase` 骨架（排名/调仓/总仓门控）。
4. [POSITION_MODEL.md](POSITION_MODEL.md)：总仓/子仓、`CloseMaster`、容量与拒单口径。
5. [CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)：市场差异与落配置方式。

## 5. 策略文件通用结构

```text
文件头 //!：策略逻辑与红线摘要
Params：deny_unknown_fields + default + Default
CrossSectionalStrategyPort（或 StrategyLogic + StrategyBase）：打分/入场/出场
register_strategy!：文件尾，name == strategy_type()
#[cfg(test)]：入场、出场、数据不足、stale/缺特征；期货另补 multiplier != 1 的手数用例
```

新增文件不需要改 `lib.rs`、`registry.rs` 或任何手写清单；`build.rs` 自动发现。
