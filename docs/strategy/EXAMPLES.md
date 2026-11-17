# 策略清单与源码导读

本文件只做“看什么、去哪里看”的导读，不逐行贴代码。源码是唯一事实源。
模板仓库只保留 6 个策略，按“入门 → 通用基类 → 期货”三组阅读。

## 1. 入门示例（从零起步）

| 文件 | 定位 | 看点 |
|---|---|---|
| `example_dual_ma.rs` | 入门最小完整策略 | `Params` + `Default`、`timeframes()`、趋势判定、`Open/Close`、`register_strategy!`、`mod tests`；同一份代码配 `config/example_dual_ma/`（crypto）与 `config/example_dual_ma_cu/`（期货 CU） |
| `example_mtf_trend.rs` | crypto 多周期样板 | `timeframes()` 返回 `[M1, 主周期, D1]`；`closed(tf)` 跨周期取数；ATR 止损；反向交叉出场 |

两个示例都直接实现 `CrossSectionalStrategyPort`，适合理解 port 的原始形状。

## 2. 通用基类（推荐写法）

| 文件 | 定位 | 看点 |
|---|---|---|
| `common/mod.rs` | `StrategyBase` + `StrategyLogic` | 基类本身不是策略文件：单标/横截面骨架、`top_k` 排名、等权 `Adjust`、`master == Closing` 门控、风险预算目标；契约测试在 `crates/strategies/tests/strategy_base.rs` |
| `xsec_template.rs` | 直接实现 port 的双路径模板 | `snapshots.len() == 1` 单标分支；`rebalance_due` 横截面分支；掉出候选 `Adjust{0}` |
| `xsec_momentum.rs` | 直接实现 port 的横截面示例 | 指标排名、`top_k`、`Adjust` 目标数量、`params` 校验；新策略最省事的起点 |

新增策略可以用 `StrategyBase` 只写 `name/top_k/score/entry_allowed/stop` 五个钩子；
当前 6 个模板都是直接实现 port（没有 StrategyBase 版模板文件）。需要完全自定义
决策流时，复制 `xsec_template.rs`。

## 3. 期货模板

| 文件 | 定位 | 看点 |
|---|---|---|
| `futures_single_trend.rs` | 期货 1:1（单策略单品种） | 整数手、`contracts.yaml` 乘数、`SessionWindowPolicy` 时段聚合、ATR 止损 |
| `futures_xsec_trend.rs` | 期货 1:N（单策略多品种） | `merge_positions: true` 一个总仓 + N 子仓；`rebalance_due` 动量轮换；乘数换算目标手数 |

期货模板的配置样例：

| 策略 | 配置 | universe |
|---|---|---|
| `futures_single_trend` | `config/ctp_single_cu/strategy.yaml` | `CU`（SHFE） |
| `futures_xsec_trend` | `config/ctp_multi_xsec/strategy.yaml` | `RB / I / CU`（SHFE） |

期货与 crypto 的差异（合约乘数、整数手、日盘/夜盘、平今、数据落位）见
[CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)，不要从策略代码里猜。

## 4. 六个模板的 `params` 速查

| 策略 | `params` 字段（默认） |
|---|---|
| `example_dual_ma` | `fast_period: 10`、`slow_period: 30`、`stop_loss_pct: 2.0`、`exit_price_source: close` |
| `example_mtf_trend` | `fast_period: 10`、`slow_period: 30`、`trend_ema_period: 50`、`atr_period: 14`、`atr_stop_mult: 2.0` |
| `xsec_template` | `rank_indicator: macd`、`higher_is_better: true`、`top_k: 3` |
| `xsec_momentum` | `top_k: 3`、`bottom_k: 0` |
| `futures_single_trend` | `trigger_ema: 60`、`trend_ema: 60`、`trend_tf: "1h"`、`atr_period: 14`、`atr_stop_mult: 2.0` |
| `futures_xsec_trend` | `lookback: 12`、`trend_ema: 60`、`trend_tf: "1h"`、`atr_period: 14`、`atr_stop_mult: 2.0`、`top_k: 2` |

规则：配置 `params` 必须能反序列化进对应 `Params`（`deny_unknown_fields`）；
多标的 `params.top_k` 与 `rebalance.top_k` 保持同值。

## 5. 建议阅读路径

1. `common/mod.rs` 的 `StrategyBase`：先理解单标/横截面如何分流、目标数量怎么算。
2. `xsec_momentum.rs`：看一个策略最少需要写什么。
3. `example_dual_ma.rs`：需要完全自定义决策流时，看 port 三方法的原始写法。
4. `example_mtf_trend.rs`：跨周期取数与多周期声明。
5. `xsec_template.rs`：横截面排名与调仓分支的完整骨架。
6. `futures_single_trend.rs` → `futures_xsec_trend.rs`：期货从 1:1 扩到 1:N。
7. [POSITION_MODEL.md](POSITION_MODEL.md)：总仓/子仓、`CloseMaster`、容量与拒单口径。
8. [CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)：市场差异与落配置方式。

## 6. 策略文件通用结构

```text
文件头 //!：策略逻辑与红线摘要
Params：deny_unknown_fields + default + Default
StrategyLogic 或 CrossSectionalStrategyPort：决定打分/入场/出场
register_strategy!：文件尾，name == strategy_type()
#[cfg(test)]：入场、出场、数据不足、stale/缺特征；期货另补 multiplier != 1 的手数用例
```

新增文件不需要改 `lib.rs`、`registry.rs` 或任何手写清单；`build.rs` 自动发现。
