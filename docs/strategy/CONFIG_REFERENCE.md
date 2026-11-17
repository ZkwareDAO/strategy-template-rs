# 配置参考（strategy.yaml / _global.yaml / contracts.yaml）

> 本文件是配置字段与校验规则的唯一速查。实现策略前先读完 §5 的单标/多标差异，
> 再照 §7 选一份库内样例复制。源码真源：
> `crates/engine/src/strategy/config.rs`（schema/校验）、`crates/app/src/config.rs`
> （`_global` 与 per-symbol）、`crates/app/src/strategy_config.rs`（装载）。

## 0. 文件布局

| 布局 | 路径 | 适用 |
|---|---|---|
| 策略级单文件 | `config/<group>/strategy.yaml` | crypto / 期货统一端口；`backtest`、`run` 都接受 |
| 配置集 | `config/<set>/<group>/strategy.yaml` + `config/<set>/_global.yaml` | `run` 多组并行 |
| 旧 per-symbol | `config/<strategy>/<SYMBOL>.yaml` | 旧单标链路；仍兼容 |
| 共享基线 | `config/<group>/_global.yaml` 或 `config/<set>/_global.yaml` | `cash/mode/services/initial_cash/...` |
| 合约参数 | 仓库根 `config/contracts.yaml`（或组内 `contracts.yaml`） | 期货乘数/保证金/费率/时段 |

`--config` 指目录时：目录本身有 `strategy.yaml` = 单组；没有但直接子目录有 =
配置集。`backtest` 只接受单组，`run` 两者都接受。

## 1. strategy.yaml 完整字段

| 字段 | 必需 | 类型 | 默认 / 约束 |
|---|---|---|---|
| `strategy_type` | 是 | string | 必须等于注册名与 `strategy_type()`；非空 |
| `strategy_id` | 否 | string | 缺省 `{strategy_type}:default`；配置集内唯一；进入 `sub_` 信号 ID 与报表路径，改动会使历史信号/报表分裂 |
| `mode` | 否 | `cross_sectional` | 仅此值；`multi_instance` 拒启 |
| `merge_positions` | 否 | bool | 缺省 `true`；单标无意义；N≥2 时决定一个总仓还是每标的总仓 |
| `universe` | 是 | list | 非空；字符串 `SYMBOL` 或 `{symbol, exchange}`；同文件只能一个 exchange |
| `timeframes` | 是 | list | 非空；`[0]` = base TF（决策节奏），其余为上级周期 |
| `indicators` | 是 | map | 指标名 → `{tf, ...参数}`；白名单见 §2.3 |
| `entry` | 是 | RuleSet | `{ default: [], overrides: {SYM: []} }`；空集 = 全部合格 |
| `exit` | 是 | RuleSet | 同上；触发后引擎自动平仓并抑制该标的策略动作 |
| `portfolio_exit` | 是 | object | `{ trailing: null|{arm_pnl_pct,giveback_pct}, stop_loss_pct: null|number }` |
| `rebalance` | 视形态 | object | N≥2 且 `merge_positions=true` 时必需；`every_bars/top_k/bottom_k/weight_scheme` |
| `risk` | 是 | object | `max_total_notional`（组合名义预算）；`max_drawdown` 解析但本期忽略 |
| `sizing` | 是 | object | `per_position_pct` > 0；`max_position_pct` 可选硬上限 |
| `params` | 否 | map | 原样透传给策略工厂；省略 = `Params::default()` |

`strategy.yaml` 的 `mode` 是**组合形态**（仅 `cross_sectional`）；运行模式
`live/paper_trading` 在 `_global.yaml` 的 `mode`，两者不要混用。

**未知字段行为（重要）**：

- `strategy.yaml` 的 `StrategyConfig` **没有** `deny_unknown_fields`，字段名拼错会被
  serde 静默忽略，不报错。必须逐项对照本表；写完用启动/回测验证字段确实生效。
- `_global.yaml`、旧 per-symbol `*.yaml` 带 `deny_unknown_fields`，拼错即拒启。
- 策略 `Params` 带 `deny_unknown_fields`，`params` 内拼错在工厂实例化时拒启。
- `indicators` 的参数是 `flatten` 进 `params` 的，未知参数会被白名单校验拒绝。

## 2. 字段语义

### 2.1 universe

```yaml
universe:
  - BTCUSDT                      # 简写：exchange 取 _global/services.trading
  - { symbol: CU, exchange: SHFE }
```

- 一个 `strategy.yaml` 只能属于一个 exchange（横截面路径当前白名单
  `binance` / `SHFE`）；混交易所、重复标的都会拒启。
- `exchange` 决定 venue key（如 `binance/crypto_perpetual/BTCUSDT`、
  `SHFE/cn_futures/CU`）与数据路径；写错会报「区间无 1m 数据」。

### 2.2 timeframes

- `timeframes[0]` 是 base TF，决策轮按它闭合触发。
- 指标 `tf` 必须在 `timeframes` 内，且是 base 的整数倍。
- 策略代码的 `timeframes()` 必须包含 `M1`（引擎聚合入口），配置里无需列 `1m`。
- 不要自己从 1m 拼 15m/1h/D1；引擎按交易所聚合（crypto UTC 网格 / 期货时段切桶）。

### 2.3 indicators

白名单与参数：

| 指标 | 允许参数 | 默认 |
|---|---|---|
| `adx` / `rsi` / `ema` / `sma` / `atr` | `period` | 14 |
| `macd` | `fast` / `slow` / `signal` | 12 / 26 / 9 |

```yaml
indicators:
  macd: { tf: 1h, fast: 12, slow: 26, signal: 9 }
  adx:  { tf: 1h, period: 14 }
```

- 未知指标名、未知参数、非正整数参数都会拒启。
- 策略里用 `features.number("macd")` 取值；缺失返回 `None`，不得按 0 参与排名。
- 用户逻辑用到白名单外指标（BOLL/KDJ/SAR 等）：在策略内从 `snapshot.closed` 全量
  计算，参数放 `params`；**不要改引擎白名单，也不要伪造 `indicators` 条目**。

### 2.4 entry / exit 规则 DSL

```yaml
entry:
  default:
    - { ind: adx, op: ">", value: 20 }
  overrides:
    ETHUSDT:
      - { ind: rsi, op: "<", value: 30 }
exit:
  default:
    - { ind: macd_cross, op: "==", value: bearish }
```

- `default` 内多条为 **AND**；某标的有 `overrides` 时**整组替换** `default`，不合并。
- `op` 仅支持 `> < >= <= == !=`。
- 数值指标只接受数值 `value`；`macd_cross` 只接受文本 `bullish|bearish` 且 op 仅 `==/!=`，
  并要求已声明 `macd`。
- 空 `entry` = 全部合格；`exit` 触发后引擎 L2 自动平仓，策略不要再发 `Close`。

### 2.5 portfolio_exit

| 字段 | 语义 |
|---|---|
| `stop_loss_pct` | 组合浮亏达到成本名义百分比时 `CloseMaster` 全平；必须 > 0 |
| `trailing.arm_pnl_pct` | 浮盈达到该百分比后武装回落止盈；必须 > 0 |
| `trailing.giveback_pct` | 从峰值回吐该百分比触发全平；`(0,100]` |

策略自设的 `Open.stop_price` 是第一道标的级止损，`portfolio_exit` 是组合级兜底，两者层级不同。

### 2.6 rebalance（仅多标合并总仓）

```yaml
rebalance: { every_bars: 12, top_k: 2, bottom_k: 0, weight_scheme: equal }
```

- `every_bars` 是 base TF 的根数；`> 0`。
- `top_k` 必须 `> 0` 且 ≤ universe 数；`params.top_k` 应与它同值。
- `bottom_k` 本期必须为 `0`（空头侧未实现）。
- `weight_scheme` 仅 `equal`。
- 只有 `merge_positions=true` 的 N≥2 才要求 `rebalance`；逐标的独立总仓不需要。

### 2.7 risk / sizing 与容量口径

| 字段 | 语义 |
|---|---|
| `risk.max_total_notional` | 策略总名义预算比例；等权目标 = `weight * max_total_notional * NAV` |
| `risk.max_drawdown` | L4 本期未启用：解析但忽略，仅告警 |
| `sizing.per_position_pct` | 单仓目标比例（占策略 NAV） |
| `sizing.max_position_pct` | 可选子仓硬上限；已超限只禁止继续加仓，不强制减仓 |

校验：`min(per_position_pct, max_position_pct) * exposure_slots <= max_total_notional`，
其中 `exposure_slots = universe.len()`（`merge_positions=true`）或 `1`（`false`）。
违反即拒启。目标数量公式：

```text
target_qty = weight * max_total_notional * nav / (price * multiplier)
```

不得用全额 NAV；否则目标恒高于执行层容量，产生无谓拒单。

### 2.8 params

- 策略私有 schema，引擎加载期不校验；工厂用 `params_or_default` 反序列化。
- `Params` 必须 `deny_unknown_fields + default`，拼错字段启动即报错。
- `Params::default()` 必须与配置 `params` 一致（文档即契约）。

## 3. 校验失败速查

| 报错关键词 | 原因 |
|---|---|
| `universe 不能为空` | 没写标的 |
| `multi_instance 未实现` | `mode` 写了 `multi_instance` |
| `timeframes[i] 解析失败` | 周期写法非法（如 `240` 缺单位、`4x` 非法单位、`0m`） |
| `不在 timeframes 内` / `必须是 base 时间框架的整数倍` | 指标 tf 与 `timeframes` 不匹配 |
| `未知指标` / `未知参数` / `必须为正整数` | 指标白名单或参数错误 |
| `引用了未声明指标` | 规则 `ind` 未在 `indicators` 声明 |
| `macd_cross 文本值只允许 bullish|bearish` | 文本值写错 |
| `必须配置 rebalance` | N≥2 且 `merge_positions=true` 但没写调仓 |
| `bottom_k 本期未实现` | `bottom_k != 0` |
| `sizing 有效单仓比例...超过 risk.max_total_notional` | 仓位预算超总预算 |
| `strategy_id 重复` | 配置集内 `strategy_id` 冲突 |
| `混合 exchange` / `duplicate symbol` | universe 跨交易所或标的重复 |

## 4. _global.yaml 字段

`_global.yaml` 是共享运行基线，`deny_unknown_fields`：

| 字段 | 默认 | 语义 |
|---|---|---|
| `mode` | `paper_trading` | `live` / `paper_trading`；`smoking` 仅旧单标演练 |
| `warmup_days` | 30 | 指标预热天数 |
| `cash` | 200 | live 每单名义（crypto USDT / 期货元）；回测仓位由 `sizing` 决定 |
| `initial_cash` | 5000 | 组合初始权益；装配时按 universe **等分**到每个 slot |
| `leverage` | 1 | 策略自选名义敞口上限（1 = 不放大） |
| `risk` | 见 §4.1 | L1 固定止损配置（`enabled/fixed_stop_loss_pct/trailing_profit/fixed_take_profit_pct`）；横截面路径只取 `fixed_stop_loss_pct` |
| `exchange` / `account` | 回落 `services.trading` | 标的身份默认值 |
| `services.trading` | 见源码 | `ws_url/request_timeout_ms/submit_enabled/reconnect_backoff_max_secs/strategy_secret/exchange/account` |
| `state_flush_interval_secs` | 60 | live/paper 状态落盘间隔 |
| `state_prune_terminal` | true | 跨日是否裁剪终态行 |
| `contracts` | `contracts.yaml` 自动加载 | 期货合约参数表 |
| `exchanges` / `instruments` | 空 | 多交易所注册与清单（进阶） |

合并优先级（旧 per-symbol / 目录模式）：CLI 显式参数 > per-symbol 文件 >
`_global.yaml` > 内置默认。`strategy.yaml` 的策略级字段来自文件本身，不会被
`_global.yaml` 覆盖；`mode/cash/initial_cash/services` 等运行字段才从 `_global` 取。

### 4.1 全局风控与初始权益分配

- `_global.yaml` 的 `risk` 是 L1（持仓级固定止损），默认 `fixed_stop_loss_pct: 20`。
  统一端口/横截面路径只使用该值：`trailing_profit` 强制关闭，
  `fixed_take_profit_pct` 不生效（回落止盈交给 `strategy.yaml` 的 `portfolio_exit`）。
- `strategy.yaml` 的 `portfolio_exit` 是 L3（组合级浮盈回落/止损）。
  层级顺序：策略自设 `Open.stop_price` → L1 固定止损 → L3 组合止损。
- `initial_cash` 是**组合总额**，装配时按 universe 等分给每个 slot（N=1 即全额）。
  多品种期货要保证“总额 / N”仍够开 1 手，否则部分品种永远开不出仓。

## 5. 单标 vs 多标（核心差异）

| 维度 | 单标 N=1 | 多标 N>1 |
|---|---|---|
| `universe` | 1 条 | ≥2 条 |
| 策略可见 `snapshots.len()` | 1，走单标分支 | N，走横截面分支 |
| `rebalance` | 可省略；单标路径不看 `rebalance_due` | `merge_positions=true` 时**必需** |
| `top_k` | 忽略（基类单标路径） | 排名保留数，≤ universe 数 |
| 仓位结构 | 1 总仓 + 1 子仓 | `merge=true`：1 总仓 + N 子仓；`false`：每标的独立总仓 |
| 决策 | 空仓 + 门控通过即开仓 | 仅 `rebalance_due` 时排名/等权调仓 |
| 出场 | 策略/配置逐标的触发 | 掉出 top_k 调回 0；组合级用 `CloseMaster`；`portfolio_exit` 兜底 |
| `merge=false` | 无意义 | 每标的总仓独立，不需要 `rebalance`；容量校验按 1 个 slot |
| 期货形态 | `futures_single_trend`（1:1） | `futures_xsec_trend`（1:N，`merge=true`） |

升级路径：同一份策略代码，`universe` 加一行 + 补 `rebalance` + `params.top_k` 同值，
即从单标切横截面；不需要改注册或其他文件。

### 5.1 策略代码里的单标/多标分支

- 直接实现 port 的模板看 `snapshots.len()`：`1` 走单标分支，`> 1` 走横截面分支。
  `xsec_template.rs` 同时演示两条路径；`example_dual_ma.rs` 只走单标。
- `StrategyBase` 自动分流：单标忽略 `rebalance_due`，横截面只在 `rebalance_due`
  时排名；策略只实现 `score/entry_allowed/stop`，不要在钩子里判断标的数量。
- 用户的 alpha 逻辑应写成市场无关形式：单标是多标的 N=1 特例，
  同一份打分/入场函数不应出现“单标用 A、多标用 B”的分叉。
- 多标的掉出 `top_k` 由模板调 `Adjust{0}`；`stale`/缺特征时保持仓位，
  不要在策略里自行猜价格平仓。

### 5.2 merge_positions 的真实含义

- `true`（默认）：所有标的共享一个组合总仓，每个标的是一个子仓；组合浮盈、
  `CloseMaster`、`max_total_notional` 都按组合算。
- `false`：每个标的各自一个总仓，互不共享容量与组合止损；适合“每品种独立跑”。
- 单标时该字段无意义，不要用它表达策略形态。

## 6. 期货配置链

期货的差异**不在** `strategy.yaml` 里，而在三处：

1. `universe.exchange`：`SHFE` / `DCE` / `CZCE` / `INE`。
2. `config/contracts.yaml`：`multiplier/tick_size/margin_rate/min_lots/session_opens/
   session_closes/no_trade_*_minutes/commission{open,close,close_today}`。
3. `_global.yaml`：`initial_cash`（按元，必须够开 ≥1 手）。

不要在 `strategy.yaml` 或策略 `params` 里写乘数/保证金/费率/时段；
`contracts.yaml` 未声明的 symbol 会静默退化为 crypto 兼容模式（小数数量 + `fee_rate`）。

字段与默认（源码 `crates/core/src/contract.rs`）：

| 字段 | 默认 | 约束 / 语义 |
|---|---|---|
| `multiplier` | 无（必填） | `> 0`；1 手对应的标的数量 |
| `tick_size` | 无（必填） | `> 0`；最小变动价位 |
| `margin_rate` | 无（必填） | `(0, 1]`；0.11 = 11% |
| `min_lots` | 1 | 最小开仓手数 |
| `session_opens` / `session_closes` | `[]` | 北京时间 `HH:MM`；空 = 无时段限制（crypto 兼容） |
| `no_trade_after_open_minutes` | 5 | 开盘后禁开/平/反手；0 = 关闭 |
| `no_trade_before_close_minutes` | 5 | 收盘前禁开/平/反手；0 = 关闭 |
| `commission.mode` | 无（必填） | `rate`（成交金额比例）或 `per_lot`（元/手） |
| `commission.open` / `close` | 无（必填） | 开仓 / 平昨费率 |
| `commission.close_today` | `close` | 平今费率；缺省按平昨 |

可开手数（回测撮合）：

```text
每手名义   = price × multiplier
每手保证金 = 每手名义 × margin_rate
手数 = min(
  floor(权益 / (每手保证金 + 开仓手续费)),
  floor(权益 × leverage / 每手名义)
)
再按 min_lots 取整；只开整数手，保证金只限制数量、不从权益重复扣除。
```

内置交易所标准品种：`CU/AU/AG/RB/RU/FU/BU/SP/BR/I/ISP`；期货公司加收的保证金/手续费请按自己账户覆盖。
`close_yesterday` 当前没有配置来源，平昨按 `close` 费率计，不会伪造平昨档。

回测前必须有真实 1m 数据，位置与造数命令见
[AI_WORKFLOW.md](AI_WORKFLOW.md) §4；没有数据不得回测。

## 7. 四种形态与库内样例

| 形态 | 样例配置 | 关键点 |
|---|---|---|
| crypto 单标 | `config/example_dual_ma/strategy.yaml` | N=1、无 `rebalance`、`sizing.per_position_pct=1.0` |
| crypto 单标（模板） | `config/xsec_template_single/strategy.yaml` | 同一份 `xsec_template` 代码，N=1、不配 `rebalance` |
| crypto 多标 | `config/xsec_momentum/strategy.yaml` | N=2、`rebalance.top_k=3`、`sizing.per_position_pct=0.10`、`risk.max_total_notional=0.80` |
| 期货单标 | `config/ctp_single_cu/strategy.yaml` | `CU/SHFE`、`timeframes [15m,1h]`、组内 `initial_cash` |
| 期货多标 | `config/ctp_multi_xsec/strategy.yaml` | `merge_positions:true`、N=3、`rebalance.top_k=2` |

最小可抄骨架（crypto 多标，`params` 对齐 `xsec_template`；删注释即能跑）：

```yaml
strategy_type: my_strategy
strategy_id: my_strategy:default
universe:
  - BTCUSDT
  - ETHUSDT
timeframes: [15m, 1h]
indicators:
  macd: { tf: 1h, fast: 12, slow: 26, signal: 9 }
entry:  { default: [] }
exit:   { default: [] }
portfolio_exit: { stop_loss_pct: 8 }
rebalance: { every_bars: 96, top_k: 1, bottom_k: 0, weight_scheme: equal }
risk:   { max_total_notional: 0.80 }
sizing: { per_position_pct: 0.40 }
params: { rank_indicator: macd, higher_is_better: true, top_k: 1 }
```

对应的 `_global.yaml`（同目录）：

```yaml
mode: paper_trading
warmup_days: 30
cash: 200.0
services:
  trading: { exchange: binance, account: demo }
```

## 8. 常见错误

| 现象 | 原因 |
|---|---|
| 配置写了新字段但行为没变 | `strategy.yaml` 未知字段被静默忽略；核对字段名 |
| 启动即报未知字段 | 该字段写进了 `_global.yaml` 或旧 per-symbol（这两处严格） |
| 策略完全不交易 | `params.rank_indicator` 与 `indicators` 键不一致，特征全缺失 |
| 每个调仓轮都有拒单 | 目标用了全额 NAV，没乘 `max_total_notional` |
| 单标策略被 `rebalance` 拖慢 | 给 N=1 配了 `rebalance`；单标路径本来不该依赖它 |
| 期货回测报数据缺失 | 未把 1m CSV 放到 `data/klines/1m/<EXCHANGE>/cn_futures/<SYMBOL>_1m.csv` |
| 期货手数出现小数 | symbol 未在 `contracts.yaml` 声明，退化成 crypto 兼容模式 |
