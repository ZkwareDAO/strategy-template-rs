# 策略规格文件（.strategy-spec.yaml）

`.strategy-spec.yaml` 的字段 schema。它是**策略逻辑确认阶段**的产物，
由实现阶段消费。运行时**不读取**这个文件——它服务于人机确认和 AI 生成代码。

## 字段说明

| 字段 | 必需 | 说明 |
|---|---|---|
| `strategy_name` | 是 | 等于模块名/文件名（不含 `.rs`） |
| `rust_module` | 是 | `crates/strategies/src/<rust_module>.rs` |
| `prefix` | 是 | 命名前缀（去 `cta_`、去 `_vN`、去下划线大写） |
| `direction` | 是 | `neutral` / `long` / `short` |
| `timeframes` | 是 | 业务周期列表，`[0]` 为触发主周期（1m 由框架强制，不写） |
| `extra_timeframes` | 否 | 其他订阅周期 |
| `symbols` | 是 | 设计覆盖标的 |
| `entry.description` | 是 | 一句话概述 |
| `entry.conditions` | 是 | 可判定条件数组，每条对应代码里一个布尔量 |
| `exit.stop_loss` | 是 | 硬止损规则；无止损须显式写依赖 `risk` 段 |
| `exit.trailing_stop` | 否 | 移动止损，无则 `""` |
| `exit.signal_reversal` | 否 | 信号反转出场，无则 `""` |
| `exit.note` | 否 | 其他出场说明 |
| `state_fields` | 否 | 策略特有字段 |
| `default_params` | 是 | 参数默认值（对应 `Params::default()`） |
| `config_files` | 否 | 预期的 `config/<name>/<SYM>.yaml` 清单 |

## 写法要求

- 每条 `entry.conditions` 要能直接对应 `on_candle()` 里的一个布尔变量
- 注明指标、周期、参数（"RSI14(1h) > 55"，不要只写"RSI 超买"）
- 多周期条件必须注明周期
- `default_params` 必须与 `Params::default()` 一致

## 位置

`docs/strategy/specs/<name>.strategy-spec.yaml`
（`build.rs` 只扫 `src/*.rs`，yaml 不会进注册表）
