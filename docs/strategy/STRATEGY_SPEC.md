# 策略规格文件（.strategy-spec.yaml）

`.strategy-spec.yaml` 的字段 schema。它是**策略逻辑确认阶段**的产物，
由实现阶段消费。运行时**不读取**这个文件——它服务于人机确认和 AI 生成代码。

## 字段说明

| 字段 | 必需 | 说明 |
|---|---|---|
| `strategy_name` | 是 | 等于注册名（`strategy_type()`）与模块名/文件名（不含 `.rs`） |
| `rust_module` | 是 | `crates/strategies/src/<rust_module>.rs` |
| `prefix` | 是 | 命名前缀（去 `cta_`、去 `_vN`、去下划线大写） |
| `market` | 否 | `crypto` / `futures`；缺省按配置 `exchange` 推断。期货的乘数/保证金/费率/时段写 `config/contracts.yaml`，不写进 `default_params` |
| `direction` | 是 | `neutral` / `long` / `short` |
| `timeframes` | 是 | 业务周期列表，`[0]` 为触发主周期（1m 由框架强制，不写） |
| `extra_timeframes` | 否 | 其他订阅周期 |
| `symbols` | 是 | 设计覆盖标的 |
| `data_source` | 否 | K 线位置（**期货必需**）：`{ dir: <数据根> }` 或 `{ files: [<1m CSV 路径>] }`。crypto 可写下载命令/默认路径 |
| `backtest` | 否 | `{ start: YYYYMMDD, end: YYYYMMDD, initial_cash: <number> }`；期货必须给初始权益 |
| `entry.description` | 是 | 一句话概述 |
| `entry.conditions` | 是 | 可判定条件数组，每条对应代码里一个布尔量 |
| `exit.stop_loss` | 是 | 硬止损规则；无止损须显式写依赖 `risk` 段 |
| `exit.trailing_stop` | 否 | 移动止损，无则 `""` |
| `exit.signal_reversal` | 否 | 信号反转出场，无则 `""` |
| `exit.note` | 否 | 其他出场说明 |
| `state_fields` | 否 | 策略特有字段 |
| `default_params` | 是 | 参数默认值（对应 `Params::default()` / `StrategyLogic` 消费的参数） |
| `config_files` | 否 | 预期配置清单：`config/<group>/strategy.yaml` 或 `config/<name>/<SYM>.yaml` |

## 写法要求

- 每条 `entry.conditions` 要能直接对应 `on_cross_section_bars()` 或
  `StrategyLogic` 钩子里的一个布尔变量
- 注明指标、周期、参数（"RSI14(1h) > 55"，不要只写"RSI 超买"）
- 多周期条件必须注明周期
- `default_params` 必须与 `Params::default()` 一致
- 期货策略的 `default_params` 不得包含费率、保证金率、合约乘数、交易时段；
  这些是账户/合约属性，由 `config/contracts.yaml` 提供
- 组合级出场（`CloseMaster` / `portfolio_exit`）写在 `exit.note`，不要伪装成单标 `Close`
- 期货 spec 必须由用户提供 `data_source`；缺失时不得进入代码实现或回测

## 位置

`docs/strategy/specs/<name>.strategy-spec.yaml`
（`build.rs` 只扫 `src/*.rs`，yaml 不会进注册表）
