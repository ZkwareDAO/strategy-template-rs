# 策略开发指南

## 1. 策略模型

全仓库只有一种策略接口：`strategy_engine::strategy::port::CrossSectionalStrategyPort`。
单体策略、横截面策略、单体起步扩多标的，全部实现它。

```rust
pub trait CrossSectionalStrategyPort: Send {
    fn strategy_type(&self) -> &str;                  // 与注册名、配置 strategy_type 一致
    fn timeframes(&self) -> Vec<Timeframe> { vec![Timeframe::M1] } // 所需周期（含 1m）
    fn on_cross_section_bars(
        &mut self,
        newly_closed: &[Timeframe],
        snapshots: &[InstrumentSnapshot],
        portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction>;
}
```

生命周期：构造 → `timeframes()` 声明周期 → 每个 base TF 决策轮调用一次
`on_cross_section_bars`。一次拿到全 universe 的已闭合快照与组合视图，
因此 N=1 与 N>1 是同一份代码：`universe` 加一行即升级为横截面。

实现方式二选一：

- 推荐用 `crates/strategies/src/common/mod.rs` 的 `StrategyBase` + `StrategyLogic`，
  只写 `name/top_k/score/entry_allowed/stop` 五个钩子。
- 或直接实现 port，参考 `template_xsec.rs` / `template_xsec.rs`。

- `newly_closed`：本轮新闭合的周期，只会是 `timeframes()` 声明过的值。
- `snapshots`：每个标的的 bar、仓位、特征、配置规则结果、已闭合序列。
- `portfolio`：NAV、回撤、子仓、总仓快照、乘数、风险预算、`rebalance_due`、`no_trade`。

**红线**：只读已闭合数据。`snapshot.closed` 不提供 `now()`、不暴露未闭合 K 线——
未来函数无法编译。

## 2. 输入与动作

`InstrumentSnapshot` 关键字段：

| 字段 | 含义 |
|---|---|
| `instrument` / `bar` / `position` | 标的、当前已闭合 bar、该标的仓位视图 |
| `stale` | 数据过期；`true` 时不得开仓或参与排名 |
| `features` | `FeatureMap`；`features.number("macd")` 取数值，缺失为 `None` |
| `entry_eligible` / `entry_reason` | 配置 `entry` 规则的纯函数结果；空 `entry` 视为合格 |
| `exit_triggered` | 配置 `exit` 规则已触发；引擎 L2 自动平仓并抑制该标的策略动作 |
| `closed` | 已配置周期的已闭合序列（`CandleSeries`），只读 |
| `trading_day` / `no_trade` / `entry_confirm` / `signal_resume` / `cooldown` / `session_open_price` | 交易日与门控扩展字段 |

`PortfolioContext` 关键字段：`strategy_id`、`nav`、`drawdown`、`portfolio_pnl_pct`、
`sub_positions`（用 `state != SubPositionState::Closed` 判断活动子仓）、
`master`（`MasterContext`：`master_id` / `state` / 派生 `view`；`state == Closing`
时停止开仓/加仓/调仓）、`multipliers`（`instrument.namespaced_key()` → 合约乘数，
缺省 1.0）、`max_total_notional`（风险预算比例，等权目标必须乘它）、
`rebalance_due`（横截面调仓门控）、`no_trade`（组合级禁交易）。

`InstrumentAction` 语义：

| 动作 | 下游效果 |
|---|---|
| `Open{side,price,stop_price,atr,strength,reason}` | 开仓并登记初始止损 |
| `Close{price,reason,is_stop_loss,strength}` | 平仓 |
| `Reverse{...}` | 先平后开；平仓失败则放弃开仓 |
| `Adjust{target_qty}` | 调整到带符号净数量（多头为正）；空仓 Adjust 等价开仓 |
| `CloseMaster{reason,is_stop_loss}` | 平掉本策略全部非 Closed 总仓及子仓；子仓全平后才收口 |
| `UpdateStop{stop_price}` | 更新持仓止损价，不产单 |
| `UpdateExit{exit_active,exit_price}` | 更新离场触发状态，不产单 |
| `Hold` | 组合级 no-op |

`Adjust` 的目标数量口径：`target = weight * max_total_notional * nav / (price * multiplier)`。
等权目标合计等于总仓预算，与执行层容量闸门同口径。

`Reverse` 在新横截面路径是软拒（计拒单、不下单）；调仓一律用 `Adjust`。
组合级退出用 `CloseMaster`，不要用逐标的 `Close` 模拟平总仓。

## 3. Params 约定

```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]   // 拼错字段名启动即报错
pub struct Params { /* ... */ }
impl Default for Params { /* ... */ }
```

工厂里用 `crate::registry::params_or_default(args.params)?` —— 配置给了就反序列化，
没给就全默认。**不要自己手写 match None**。用 `StrategyBase` 时，私有参数仍按此约定
解析，`StrategyLogic` 只消费参数，不管配置装配。

## 4. 注册机制

新增策略 = 在 `crates/strategies/src/` 放一个 `.rs` 文件，文件尾一次：

```rust
crate::register_strategy! {
    name: "my_strategy",
    factory: |args| { Ok(Box::new(MyStrategy { /* ... */ })) }
}
```

`build.rs` 扫描 `src/*.rs` 自动生成模块与工厂清单，**新增文件即可，不需要改任何其他文件**。
策略名小写蛇形且全局唯一（`registry.rs` 测试校验）。

## 5. 配置

两种布局：

- **策略级单文件**（推荐）：`config/<group>/strategy.yaml`，字段
  `strategy_type / strategy_id / mode / universe / timeframes / indicators / entry / exit /
  portfolio_exit / rebalance / risk / sizing / params`。用于统一端口策略，
  `backtest` 与 `run` 都接受；字段全表与校验规则见
  [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md)。
- **旧 per-symbol**（已标记退役，仅兼容历史配置）：`config/<strategy>/<SYMBOL>.yaml`，字段
  `strategy / timeframe / cash / initial_cash / leverage / exchange / account / mode /
  warmup_days / params / risk / contracts`。目录模式按文件名认领标的。

`cash`（live 单笔开仓名义）、`services`、`mode`、`leverage`、`contracts` 等运行参数
放在共享的 `config/_global.yaml`（或 `strategy.config.yaml`），**不在 `strategy.yaml` 内**。
注意 `strategy.yaml` 的 schema **不会拒绝未知字段**：写错字段名会被静默忽略、不报错，
必须对照 [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md) 逐项确认；
`_global.yaml` 与旧 per-symbol 文件才带 `deny_unknown_fields`。

国内期货的手续费、保证金率、合约乘数统一放在仓库根目录
`config/contracts.yaml`（回测直接读取，按 symbol 匹配）；`initial_cash` 为
回测账户初始权益（期货按保证金决定手数，建议显式配置）。
期货 symbol 未在 `contracts.yaml` 声明会静默退化为 crypto 兼容模式（小数数量 +
`fee_rate`），回测能跑但口径不是期货。完整差异见
[CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)。

合并优先级（旧 per-symbol / 目录模式）：CLI 显式参数 > per-symbol 文件 >
`config/_global.yaml` > 内置默认。`strategy.yaml` 的共享基线同样来自 `_global.yaml`。

## 6. 风控分层

两处 `risk` 不要混淆：

- `_global.yaml` 的 `risk` 是 **L1 持仓级固定止损**（`fixed_stop_loss_pct`，默认 20）。
  统一端口/横截面路径只使用该值；`trailing_profit` 强制关闭、
  `fixed_take_profit_pct` 不生效。
- `strategy.yaml` 的 `portfolio_exit` 是 **L3 组合级**浮盈回落/止损
  （`trailing.arm_pnl_pct/giveback_pct`、`stop_loss_pct`）。
- `strategy.yaml` 的 `risk.max_total_notional` 是**容量预算**，与止损无关；
  `max_drawdown` 本期解析但忽略。

层级顺序：策略自设 `Open.stop_price` → L1 固定止损 → L3 组合止损。
详细字段见 [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md) §4.1。

## 7. 测试写法

直接构造 `InstrumentSnapshot` / `PortfolioContext` 喂合成已闭合 K 线
（见 `template_xsec.rs`、`template_trend.rs` 的 `mod tests`）。`PortfolioContext`
至少显式给出 `master`、`multipliers`、`max_total_notional`、`rebalance_due`。
至少覆盖：入场、出场、数据不足、方向冲突、`stale`、缺失特征、风险预算目标数量。
`StrategyBase` 的通用行为由 `crates/strategies/tests/strategy_base.rs` 统一覆盖，
新策略只补自己的 alpha 钩子分支。

## 8. 回测 ↔ live 一致性

同一策略 + 同一 K 线序列，回测引擎与 paper 模式必须产出**同向同入场价**的交易。
该性质由 `strategy_backtest_e2e.rs`、`strategy_live_mock_e2e.rs` 端到端验收——
你的策略改动跑 `cargo test --workspace` 即受此门禁保护。
