# AI 策略开发工作流（从用户逻辑到可回测策略）

> 目标：用户只说策略逻辑，AI 只把它翻译成模板允许的最小实现。
> 模板提供全部基础设施（数据、聚合、仓位账本、风控、下单），AI **不替用户发明 alpha**。
>
> 配套：[CONFIG_REFERENCE.md](CONFIG_REFERENCE.md)（配置字段）、
> [CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)（市场差异）、
> [AI_CONSTRAINTS.md](AI_CONSTRAINTS.md)（红线）。

## 0. 三条铁律

1. **只实现用户说的逻辑**。用户没说的指标、过滤、止损、冷却、加仓、regime，
   一律不加；需要补默认值时用模板默认并在交付说明里写明。
2. **先收信息，再写代码**。缺市场、标的、周期、K 线位置、回测区间时先问；
   不要猜，不要用别的品种数据代替。
3. **策略代码只做 alpha**。入场/出场规则、风控、调仓节奏、手数取整、交易时段
   都已经有承载处（配置或引擎），不要搬进策略逻辑。

## 1. 开工前必须向用户收集的信息

| # | 信息 | 必需性 | 说明 / 缺失时怎么办 |
|---|---|---|---|
| 1 | 市场 | 必需 | `crypto` 或 `futures`；决定 exchange、合约口径、时段 |
| 2 | 标的与交易所 | 必需 | 如 `BTCUSDT/binance`、`CU/SHFE`；期货必须明确交易所 |
| 3 | **K 线位置** | **必需** | 见 §4。crypto 可下载；**期货没有自动下载，必须用户提供路径** |
| 4 | 回测区间 | 必需 | `--start/--end`，格式 `YYYYMMDD`；区间内必须有 1m 数据 |
| 5 | 初始权益 | 期货必需 | `initial_cash`（元）；必须够开至少 1 手 |
| 6 | 策略方向 | 必需 | 只多 / 只空 / 多空；横截面多空用 `bottom_k` 指定空头端（0=只多），单标用 `allow_short` |
| 7 | 指标与周期 | 必需 | 名称、参数、周期（如 `EMA20(4h)`）；必须落在指标白名单 |
| 8 | 入场条件 | 必需 | 可判定布尔条件，逐条列；多条件默认 AND |
| 9 | 出场/止损 | 必需 | 止损方式与阈值；止盈/反向出场；无止损必须显式说明 |
| 10 | 仓位/调仓 | 视形态 | 单标给单仓比例；多标给 `top_k`、调仓频率、等权 |
| 11 | 运行形态 | 必需 | 单标 / 多标；多标是否合并总仓（`merge_positions`） |
| 12 | 参数默认值 | 可选 | 用户没给就用模板默认，并在交付里列出 |

用户只给一句模糊逻辑时，AI 要把它拆成 6–9 行的可判定条件再确认，不能直接开写。
用户没说止损时先问；用户明确说“不止损”时，也要说明模板要求开仓带标的级止损，
至少用 `_global.yaml` 的 `risk.fixed_stop_loss_pct` 或 `portfolio_exit` 兜底，
并在交付说明中写清假设。

## 2. 用户逻辑 → 落点映射

| 用户说法 | 落到哪里 | 不要怎么做 |
|---|---|---|
| “金叉做多、死叉平仓” | `StrategyLogic::entry_allowed` + `score`，或 port 内布尔判定；出场可放配置 `exit` | 不要在策略里再叠 ADX/成交量过滤 |
| “ADX > 20 才进场” | 配置 `entry` 规则（若指标已声明）或 `entry_allowed` | 不要两边都写、口径还不一致 |
| “亏损 2% 止损” | `StrategyLogic::stop` / `Open.stop_price`，或配置 `exit.stop_loss` | 不要自己维护持仓状态算浮亏 |
| “组合回撤 8% 全平” | 配置 `portfolio_exit.stop_loss_pct` | 不要逐标的发 `Close` 模拟组合平仓 |
| “每 3 小时换一次 top2” | 配置 `rebalance.every_bars/top_k` | 不要在策略里数 bar 自己调仓 |
| “每次买 20% 资金” | 配置 `sizing.per_position_pct` / `risk.max_total_notional` | 不要用全额 NAV 算目标 |
| “期货 5 吨一手” | `config/contracts.yaml` 的 `multiplier` | 不要写进策略 `params`，也不要在代码里乘 5 |
| “开盘后 5 分钟不交易” | `config/contracts.yaml` 的禁交易窗口 | 不要用 UTC 时间自己判断 |
| 指标不在白名单（BOLL/KDJ/SAR） | 策略内从 `snapshot.closed` 全量计算，参数进 `params` | 不要改引擎白名单，也不要伪造 `indicators` |

## 3. 选实现方式（决策树）

```text
用户要单标还是多标？
├─ 单标 N=1
│   ├─ crypto → 复制 template_trend.rs（直接 port，最小）
│   └─ futures → 复制 template_trend.rs（整数手 + 时段）
└─ 多标 N>1
    ├─ crypto → 复制 template_xsec_ls.rs（直接 port 横截面）
    └─ futures → 复制 template_xsec_ls.rs（1:N + 乘数）
```

也可以用 `common::StrategyBase` 只写五个钩子；当前 2 个模板都直接实现 port，
基类契约在 `common/mod.rs`。

单标/多标的完整差异见 [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md) §5。

## 4. 期货回测前置：K 线位置必须由用户提供

**这是硬前置。** 模板仓库不附带期货行情：`data/klines/1m/SHFE/cn_futures/CU_1m.csv`
是 gitignore 占位（可能只有表头）。没有真实 1m 数据，回测会报
「区间无 1m 数据」并拒绝运行。

AI 必须先让用户二选一：

1. **给逐合约原始 CSV 目录**（列：`symbol,bob|open_time,open,high,low,close,volume,position`；
   文件名如 `CU2612.csv`）。AI 执行：

   ```bash
   cargo run -p strategy-data --bin roll_build -- \
     --in <用户给的目录> --product CU --out <临时输出目录>
   cp <临时输出目录>/1m/CU_CONT_1m.csv data/klines/1m/SHFE/cn_futures/CU_1m.csv
   ```

2. **给已生成的 1m CSV 绝对路径**。AI 确认路径后，把文件放到
   `data/klines/1m/<EXCHANGE>/cn_futures/<SYMBOL>_1m.csv`，或直接用
   `--data-dir` 指向用户的数据根目录。CSV 需为标准 10 列：
   `timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume`
   （旧 6 列兼容，扩展列缺省 0）。

任一缺失时：**停止，不要回测**。禁止用 mock、合成、其他品种或其他区间数据顶替，
也不要在报告里把“没跑”写成“跑通”。

crypto 的数据位置：

- 已有：`data/klines/1m/binance/crypto_perpetual/<SYMBOL>_1m.csv`
- 缺失：`cargo run -p strategy-app -- download --symbol <SYM> --interval 1m --days 40`
  写入同一路径。

## 5. 实施步骤

1. **写 spec**：`docs/strategy/specs/<name>.strategy-spec.yaml`，按
   [STRATEGY_SPEC.md](STRATEGY_SPEC.md) 填 `market/data_source/entry/exit/default_params`。
   每条 entry 条件必须能对应代码里一个布尔量。
2. **写策略**：从决策树选模板复制为 `crates/strategies/src/<name>.rs`，只改：
   类型名、`Params`、打分/入场/止损钩子、`register_strategy!` 的 name。
   保留文件头 `//!`、`Params::default()`、`#[cfg(test)]`。
3. **写配置**：`config/<group>/strategy.yaml`（字段对照
   [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md)），共享运行参数放同组
   `_global.yaml`；期货合约参数写 `config/contracts.yaml`。
4. **写测试**：入场（多/空）、出场、数据不足、`stale`/缺特征；期货补
   `multiplier != 1` 与整数手用例。
5. **验证**：`cargo fmt --check`、`cargo clippy --all-targets -- -D warnings`、
   `cargo test --workspace`。
6. **回测**：确认 §4 数据就位后运行，读报告并与 spec 对照。
7. **交付说明**：列出文件、参数、运行命令、数据前提、未实现项（不要留隐性假设）。

## 6. 完成定义（DoD）

- [ ] spec、策略源码、`strategy.yaml`、`_global.yaml`（如需要）、测试齐全
- [ ] `strategy_type` == 注册名 == 配置名；`Params::default()` == 配置 `params`
- [ ] 策略只实现用户逻辑；没有新增用户未提的过滤器/止损/止盈
- [ ] 单标/多标路径与用户要求的形态一致；多标 `rebalance` 与 `top_k` 匹配
- [ ] 期货：`contracts.yaml` 已声明、`initial_cash` 可开 1 手、用户 K 线位置已验证
- [ ] `cargo test --workspace` 全绿；回测命令实际跑过并给出输出摘要

## 7. 反模式（AI 最容易犯）

| 反模式 | 后果 |
|---|---|
| 用户说双均线，AI 自己加 RSI 过滤 | 回测结果不是用户要的策略 |
| 用户没说止损，AI 默认加 2% 止损 | 悄悄改变策略语义；应显式询问或说明假设 |
| 期货策略写死 `multiplier = 5` | 换品种/换合约即错 |
| 单标逻辑直接加 `rebalance` | N=1 不需要调仓门控，行为可能被推迟 |
| 多标不配 `rebalance`（`merge_positions=true`） | 启动即拒（校验要求） |
| 用 `Close` 逐标的模拟组合平仓 | 总仓状态机不成立，应 `CloseMaster` |
| 期货没数据就“先用 mock 跑一下” | 产出不可信；必须等用户给数据 |
