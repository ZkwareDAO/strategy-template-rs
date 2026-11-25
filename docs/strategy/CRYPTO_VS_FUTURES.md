# 加密货币永续 vs 国内期货：策略开发差异

> 面向 AI 与人类策略作者。全仓库只有一种策略接口
> `CrossSectionalStrategyPort`；市场差异不在 trait 里，而在**标的身份、合约参数、
> 交易时段、订单语义、账务口径**五层。
>
> 写 alpha 时尽量保持市场无关；必须市场相关的地方只有三处：
> `universe.exchange`、`multiplier`、交易时段输入。

## 0. 一句话结论

- 数量换算统一用
  `target = weight * max_total_notional * nav / (price * multiplier)`；
  `multiplier` 从 `PortfolioContext.multipliers` 取，crypto 缺省 `1.0`。
- 不要自己拼大周期 K 线，也不要自己判断开盘/收盘/夜盘/午休：引擎按交易所聚合策略
  切桶，crypto 走 `UtcGridPolicy`，国内期货走 `SessionWindowPolicy`。
- 不要自己算手续费、保证金或手数取整：这些来自 `config/contracts.yaml`，执行层按
  `qty_step / min_qty / margin_rate / commission` 落地。
- 策略私有 `params` 里禁止出现费率、保证金率、合约乘数、交易时段。它们是账户或合约
  属性，不是 alpha。

## 1. 差异总表

| 维度 | 加密货币永续（crypto） | 国内期货（CTP 口径） |
|---|---|---|
| 行键/身份 | `binance/crypto_perpetual/BTCUSDT` | `SHFE/cn_futures/CU` |
| 交易时段 | 7x24，无休市 | 日盘 + 夜盘；午休强制断桶，夜盘 bar 归下一交易日 |
| 大周期聚合 | `UtcGridPolicy`：UTC epoch 网格，日线 = UTC 午夜 | `SessionWindowPolicy`：按交易日/时段切桶，日线起点 = 当日首个时段开盘 |
| 数量单位 | 币的小数数量（`qty_step=1e-6`） | **整数手**（`qty_step=1`、`min_lots>=1`） |
| 名义价值 | `price * qty` | `price * qty * multiplier`（乘数来自 `contracts.yaml`） |
| 保证金 | `margin_rate=1.0` 缺省；`leverage` 是策略自选名义上限 | `margin_rate` 按品种（如 CU 0.11）；实际可开手数受保证金与名义双约束 |
| 手续费 | 单边 `fee_rate`（crypto/兼容模式） | `open / close / close_today` 分档，支持 `rate`（比例）或 `per_lot`（元/手） |
| 平仓语义 | 净仓 `reduce_only`，不区分今昨仓 | 订单带 `offset`：`open / close_today / close_yesterday`；同一 CTP 交易日平仓走平今 |
| 禁交易窗口 | 无 | 开盘后 / 收盘前 N 分钟挂起新动作（`no_trade_after_open_minutes` / `no_trade_before_close_minutes`），止损更新仍即时 |
| 账户初始权益 | `initial_cash` 组合总额（缺省 5000，按 universe 等分） | 同左；期货建议显式配置（按元），且等分后仍够开 1 手 |
| 价格连续性 | 永续合约连续价格 | 主力连续**不复权**，换月有价格断档；跨换月回测需谨慎 |
| 资金费率 | **未建模**（回测不含 funding） | 不适用 |
| 追保/强平 | **未建模**；保证金只限制可开数量 | **未建模**；保证金只限制可开手数 |
| 数据路径 | `data/klines/1m/binance/crypto_perpetual/<SYM>_1m.csv` | `data/klines/1m/SHFE/cn_futures/<SYM>_1m.csv` |
| `contracts.yaml` 未声明 | 正常（crypto 兼容模式） | **危险**：会按小数数量 + `fee_rate` 撮合，看起来能跑但不是期货规则 |

## 2. 引擎已经替你统一的部分

同一份策略代码在两种市场都能跑，靠的是下面这些引擎保证：

1. **数量与取整**：策略只给目标数量；执行层按 `InstrumentSpec` 的
   `qty_step / min_qty` 向下取整。crypto 目标可以是小数，期货会被裁成整数手；
   `min_qty` 不足时目标归零，不产生碎手单。
2. **名义与容量**：`PortfolioContext.multipliers` 提供逐标的乘数；
   `gross_notional`、`max_total_notional * nav` 与执行层容量闸门口径一致。
   策略不需要知道标的是币还是手。
3. **周期聚合**：策略只声明 `timeframes()`（必须含 `M1`），引擎把 1m 按交易所
   策略聚合成 15m/1h/D1。期货午休、夜盘、节假日伪交易日都由 `SessionWindowPolicy`
   处理；策略只读 `snapshot.closed.closed(tf)`。
4. **禁交易门控**：`InstrumentSnapshot.no_trade`、`trading_day`、
   `session_open_price` 已由引擎填好。策略可以读，但不要自己用 UTC 日期推断交易时段。
5. **订单 offset**：crypto 与期货的平仓差异由 `PositionRules` 转成平仓腿；
   策略只发 `Close` / `CloseMaster` / `Adjust`，不需要写 `close_today`。
6. **费用与权益**：crypto 读 `fee_rate`，期货读 `contracts.yaml` 分档费率；
   回测撮合与账务统一走 `FeeSchedule`。

## 3. 写策略时的市场相关检查点

| # | 检查 | crypto | futures |
|---|---|---|---|
| 1 | `universe` 的 `exchange` 与数据目录一致 | `binance` / `bybit` | `SHFE` / `DCE` / `CZCE` / `INE` |
| 2 | 目标数量是否用了 `multiplier` | 可缺省 1.0 | **必须**从 `portfolio.multipliers` 取，禁止写死 |
| 3 | 是否假设 7x24 | 可以 | 不可以；只信 `closed(tf)` 与 `no_trade` |
| 4 | 止损价是否按 `tick_size` 对齐 | 可选 | 建议对齐，避免不可成交价 |
| 5 | 是否在 `params` 里写费率/保证金/乘数 | 禁止 | 禁止 |
| 6 | 配置里是否有 `initial_cash` | 可选 | 必须显式，且按保证金能开得起至少 1 手 |
| 7 | 回测区间是否跨换月 | 无此问题 | 有断档；分段回测或接受偏差 |
| 8 | 是否把 `qty` 当名义金额 | 否 | 否；名义 = `qty * price * multiplier` |

## 4. 同一策略跑两个市场

`template_trend` 同时提供 crypto 与 CU 两份配置，策略代码完全不改：

| 文件 | `universe` | base TF | `_global.yaml` 关键项 |
|---|---|---|---|
| `config/tpl_trend_crypto/strategy.yaml` | `{BTCUSDT, binance}` | 1h | `cash: 200`（USDT 单笔名义） |
| `config/tpl_trend_futures/strategy.yaml` | `{CU, SHFE}` | 15m | `initial_cash: 1000000`（元） |

差异全部落在配置与 `config/contracts.yaml`：

```yaml
contracts:
  CU:
    multiplier: 5          # 5 吨/手
    tick_size: 10
    margin_rate: 0.11
    min_lots: 1
    session_opens: ["09:00", "21:00"]
    session_closes: ["15:00", "01:00"]
    commission:
      mode: rate
      open: 0.00005
      close: 0.00005
      close_today: 0.0001
```

排查口诀：**crypto 看 fee_rate，期货看 contracts.yaml**。期货 symbol 没在
`contracts.yaml` 声明时不会报错，会静默退化成 crypto 兼容模式（小数数量 + 按费率收费），
这是最常见的“回测能跑、结果不对”。

期货手续费还有两个容易忽略的口径：

- `commission.close_today` 缺省 = `close`；同一 CTP 交易日平仓才走平今。
- `close_yesterday` 当前没有配置来源；平昨按 `close` 费率计，不会伪造平昨档。

## 5. 配置与数据落位

- 共享运行参数（`mode`、`cash`、`services`、`leverage`、`initial_cash`）放
  `config/<group>/_global.yaml`；`strategy.yaml` 只放策略级字段（`deny_unknown_fields`）。
- 合约参数只放仓库根 `config/contracts.yaml`，按 symbol 匹配；策略 YAML 里重复写会被拒启。
- 1m 数据落位：
  - crypto：`data/klines/1m/binance/crypto_perpetual/BTCUSDT_1m.csv`
  - 期货：`data/klines/1m/SHFE/cn_futures/CU_1m.csv`（主力连续需先 `roll_build` 生成）
- crypto 与期货的配置应分目录（如 `template_trend/` 与 `template_trend_cu/`）；
  同一目录内的每个文件只认自己的 `exchange`，不再从兄弟文件继承。

## 5.1 期货回测前置：K 线位置必须由用户提供

模板仓库**不附带期货行情**：`data/klines/1m/SHFE/cn_futures/CU_1m.csv` 是 gitignore
占位（可能只有表头）。期货没有 crypto 那样的自动下载命令，AI 必须先向用户拿到：

1. 逐合约原始 CSV 目录（列 `symbol,bob|open_time,open,high,low,close,volume,position`），
   再跑 `roll_build` 生成主力连续；或
2. 已生成的 1m CSV 路径（文件放到
   `data/klines/1m/<EXCHANGE>/cn_futures/<SYMBOL>_1m.csv`，或用 `--data-dir` 指向数据根）。

命令与完整步骤见 [AI_WORKFLOW.md](AI_WORKFLOW.md) §4。缺数据时**不得回测**，
不得用 mock/合成/其他品种数据顶替，也不得把“没跑”写成“跑通”。

## 6. 本期边界（别在策略里补）

- 模板**不直连 CTP**：期货只做回测 + paper 同口径信号，行情/下单由 trading-service 承担。
- 不模拟资金费率、追保、强平、每日结算；保证金只限制可开数量。
- 期货主力连续不复权，跨换月价格断档是数据事实，不在策略里做复权。
- 回测年化按 `365 / 交易日数`；期货一年约 250 个交易日，跨市场比较年化时需自行换算。
- 横截面支持多空：前 `top_k` 做多、后 `bottom_k` 做空，两侧预算各占一半；
  同一标的跨轮反向走 `Close`+`Open` 两段式，`weight_scheme` 仅 `equal`。
