# 策略开发指南

## 1. 策略模型

```rust
pub trait Strategy {
    fn strategy_type(&self) -> &str;                    // 与注册名、配置 strategy: 一致
    fn timeframes(&self) -> Vec<Timeframe>;             // 必须含 Timeframe::M1
    fn on_candle(&mut self, ctx: &Ctx<'_>) -> Vec<Action>;   // 空仓入场
    fn on_realtime_tick(&mut self, ctx: &Ctx<'_>) -> Vec<Action> { Vec::new() } // 持仓出场
}
```

生命周期：构造 → `timeframes()`（注册周期）→ 反复 `on_candle`（每根已闭合 1m，
**引擎保证空仓时才调用**）/ `on_realtime_tick`（**引擎保证持仓时才调用**）。

## 2. Ctx 与数据访问

| 字段/方法 | 含义 |
|---|---|
| `ctx.closed_data(tf)` | 某周期已闭合 K 线序列（升序） |
| `ctx.closed_data_min(tf, n)` | 同上 + 最少根数检查（数据不足返回 None） |
| `ctx.current_price` | 最新价 |
| `ctx.current_candle` | 触发本次调用的 1m K 线 |
| `ctx.event_time` | 事件时间（= 1m open_time，signal_id 输入） |
| `ctx.position` | `Option<PositionView>`（引擎注入，只读） |
| `ctx.in_stop_loss_cooldown()` | 止损日冷却判定 |

**红线**：只读已闭合数据。`Ctx` 不提供 `now()`、不暴露未闭合 K 线——未来函数无法编译。

## 3. Action 语义

| 变体 | 下游效果 |
|---|---|
| `Open{side,price,stop_price,atr,strength,reason}` | 产生 1 条开仓 SignalIntent |
| `Close{price,reason,is_stop_loss,strength}` | 产生 1 条平仓 SignalIntent |
| `Reverse{...}` | 产生 2 条（先平后开） |
| `UpdateStop{stop_price}` | 直接改持仓止损价，不产单 |
| `UpdateExit{exit_active,exit_price}` | 更新离场触发状态，不产单 |
| `Hold` / 空 Vec | 无操作 |

## 4. Params 约定

```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]   // 拼错字段名启动即报错
pub struct Params { /* ... */ }
impl Default for Params { /* ... */ }
```

工厂里用 `crate::registry::params_or_default(args.params)?` —— 配置给了就反序列化，
没给就全默认。**不要自己手写 match None**。

## 5. 注册机制

文件尾一次：

```rust
crate::register_strategy! {
    name: "my_strategy",
    factory: |args| { Ok(Box::new(MyStrategy { /* ... */ })) }
}
```

`build.rs` 扫描 `src/*.rs` 自动生成注册表，**新增文件即可，不需要改任何其他文件**。

## 6. 配置

`config/<strategy>/<SYMBOL>.yaml` 顶层字段：
`strategy / timeframe / cash / leverage / exchange / account / mode / warmup_days / params / risk`

合并优先级：CLI 显式参数 > per-symbol 文件 > `config/_global.yaml` > 内置默认。

## 7. 风控（`risk:` 节）

优先级：**固定止损 > 回落止盈 > 固定止盈**。
与策略自设 `stop_price` 并行：策略止损是第一道，risk 兜底是第二道。

## 8. 测试写法

`FakeSource: ClosedSource` 喂合成已闭合 K 线（见 `example_dual_ma.rs` 与
`example_mtf_trend.rs` 的 `mod tests`）。至少覆盖：入场、出场、数据不足、方向冲突。

## 9. 回测 ↔ live 一致性

同一策略 + 同一 K 线序列，回测引擎与 paper 模式必须产出**同向同入场价**的交易。
该性质由 `crates/app/tests/backtest_paper_parity.rs` 自动验收——你的策略改动
跑 `cargo test --workspace` 即受此门禁保护。
