# 横截面策略开发指南

本指南面向在 `strategy-template-rs` 中开发单标的、横截面，或“单标的起步后加代币升级”的策略。
全仓库只有一种策略接口：`strategy_engine::strategy::port::CrossSectionalStrategyPort`。

两个起步点：

- **推荐**：用 `crates/strategies/src/common/mod.rs` 的 `StrategyBase` + `StrategyLogic`
  写 alpha 钩子（打分/入场过滤/止损），单标与横截面骨架由基类提供。
- **直接实现 port**：复制 `crates/strategies/src/template_xsec.rs`，自己写双路径分支。

## 1. 唯一策略接口

全仓库只有一种策略接口：`CrossSectionalStrategyPort`（`strategy_engine::strategy::port`）。

| 维度 | 统一 `CrossSectionalStrategyPort` |
|---|---|
| 输入 | 一次拿到全 universe 的 `InstrumentSnapshot` + `PortfolioContext`（NAV/回撤/子仓/乘数） |
| 调用 | 每个 base TF 决策轮 → `on_cross_section_bars` 一次 |
| 输出 | 多标的 `InstrumentAction`（含 `Adjust{target_qty}` 调仓） |
| 元数据 | `strategy_type()` 注册名；`timeframes()` 声明所需周期（默认 `[M1]`） |
| 能力 | 排名、轮换、目标权重、组合级决策 |
| N 能力 | N=1 与 N>1 同一份代码；universe 加一行即升级 |
| 注册 | 文件尾 `register_strategy!`；`build.rs` 自动发现 |

SPI 单标路径（`ExchangeRunner`）走同一 port：`PortStrategyBridge` 以单条
`InstrumentSnapshot` 调用 `on_cross_section_bars`；`strategy_type()` 提供 runner
命名，`timeframes()` 提供周期聚合清单。没有旧 `Strategy` 兼容分支。

**行为变化**：N=1 现在与横截面走同一路径——信号 ID 使用 `sub_` 命名空间，
风控走横截面 L1（组合级 trailing 关闭）。

## 2. 目录布局与注册

- **所有策略**：`crates/strategies/src/*.rs` 顶层文件，一个文件一个策略。
  `build.rs` 自动扫描并生成模块与工厂清单；文件尾 `register_strategy!` 自注册。
  新增策略不需要改 `src/lib.rs`、`registry.rs` 或任何手写清单。
- **命名**：策略名小写蛇形且全局唯一（`registry.rs` 测试钉住）。
  `xsec_` 前缀是历史命名，不是约定；模块路径统一是 `strategy_engine::strategy`。
- **基类**：`common::StrategyBase` 已沉淀单标/横截面两条决策路径；策略文件只实现
  `StrategyLogic`（`name` / `top_k` / `score` / `entry_allowed` / `stop`）。
- **模板**：`template_xsec` 是直接实现 port 的参考；复制为 `src/<name>.rs` 即可起步。

## 3. 从零写策略

### 3.1 推荐：StrategyBase + StrategyLogic

新增策略 = 实现 `StrategyLogic` 钩子并用 `StrategyBase::new(logic)` 包装：

| 钩子 | 回答的问题 |
|---|---|
| `name()` | 注册名，必须与 `strategy_type()` / 配置一致 |
| `top_k()` | 横截面保留数；单标路径忽略 |
| `score(snapshot)` | 排名分数；`None` = 特征缺失，不参与排名/不开仓 |
| `entry_allowed(snapshot)` | 策略私有入场门槛（在 `stale` / `entry_eligible` 之外） |
| `stop(snapshot)` | 止损定义（`StopSpec::Percent` / `Atr`）；`None` = 本轮不开仓 |

基类固定行为（不要重复实现）：

- `snapshots.len() == 1` 走单标路径：不看 `rebalance_due`，空仓 + 门控通过即开多。
- `snapshots.len() > 1` 走横截面路径：仅 `rebalance_due` 时按分数排名，等权持有前 `top_k`。
- 已有持仓：目标按 `weight * max_total_notional * nav / (price * multiplier)` 计算，
  容差内不发动作；掉出 `top_k` 且特征可判时 `Adjust{0}`；`stale` / 缺特征时保持。
- `portfolio.master.state == Closing` 时直接返回空动作，不开仓/不加仓/不调仓。
- 退出（止损/止盈/组合回撤）交给配置 `exit` / `portfolio_exit`，基类不产生 `Close`。

参考实现：`template_xsec`（crypto 横截面）、`template_xsec`（期货 1:N）。
两者都直接实现 port；`StrategyBase` 的通用行为由 `common/mod.rs` 与
`crates/strategies/tests/strategy_base.rs` 定义，当前模板未附 `StrategyBase` 版策略文件。

### 3.2 直接实现 port：复制 template_xsec

复制 `crates/strategies/src/template_xsec.rs` 为 `src/<name>.rs`，按以下顺序改：

1. 改类型名与 `TemplateParams` 默认值（`rank_indicator` 默认 `"macd"`、`higher_is_better`
   默认 `true`、`top_k` 默认 `3`）。
2. `snapshots.len() == 1` 分支 = 单标路径：不看 `rebalance_due`；空仓且
   `!stale && entry_eligible` 时开多；已有子仓持有；退出交给配置。
3. `snapshots.len() > 1` 分支 = 横截面路径：仅 `rebalance_due` 时排名，保留前 `top_k`，
   等权 `Adjust`；掉出候选且数据可判的子仓目标清零，`stale`/缺特征持仓保持到下一轮。
4. 文件尾 `register_strategy!` 的 `name` 必须与 `strategy_type()` 一致。
5. 建 `config/<name>/strategy.yaml`（目录名任意，但必须包含名为 `strategy.yaml` 的文件）。

骨架（只保留结构，不抄实现）：

```text
单标：空仓 + !stale + entry_eligible -> Open；已有子仓 -> 持有
横截面：!rebalance_due -> 空动作；否则排名 -> 前 top_k 等权 Adjust，掉出 -> Adjust{0}
```

## 4. 输入契约

`InstrumentSnapshot` 字段：

- `instrument` / `bar` / `position`：标的、当前已闭合 bar、该标的仓位视图。
- `stale`：数据过期；`stale=true` 不得参与开仓或排名。
- `features`：`FeatureMap`；`features.number("macd")` 取数值，缺失返回 `None`。
  **缺失特征不得按 0 参与排名**。
- `entry_eligible` / `entry_reason`：配置 `entry` 规则的纯函数结果；空 `entry` 视为合格。
- `exit_triggered`：配置 `exit` 规则已触发；引擎 L2 会自动 `Close` 并抑制该标的策略动作。
- `trading_day` / `no_trade` / `entry_confirm` / `signal_resume` / `cooldown` /
  `session_open_price`：交易日与门控扩展字段（冷却、禁交易、开盘确认等）。
  这些由引擎按配置/状态填充；策略只需使用，不要自己用 UTC 日期或计数器复算。
- `closed`：全部已配置周期的已闭合序列视图（`CandleSeries`），只读；不要在其中
  `now()` 或写入。

`PortfolioContext` 字段：

- `strategy_id` / `nav` / `drawdown` / `portfolio_pnl_pct`。
- `sub_positions`：`SubPosition` 列表；用 `state != SubPositionState::Closed` 判断活动子仓。
- `master`：`Option<MasterContext>`（`master_id` / `state` / `view`）；`view` 只聚合非
  `Closed` 子仓，提供 `gross_notional` / `net_notional` / `avg_entry_price` /
  `realized_pnl` / `unrealized_pnl` / `sub_position_count`。
- `multipliers`：`instrument.namespaced_key()` → 合约乘数；缺省按 `1.0`。
- `max_total_notional`：组合总名义预算（`risk.max_total_notional`）。等权目标必须用它，
  不能用全额 NAV，否则目标恒高于执行层容量闸门，每个调仓轮都产生无谓拒单。
- `rebalance_due`：横截面路径的调仓门控；单标路径不使用；有在途减仓/加仓时为 `false`。
- `no_trade`：组合级禁交易标记。

### 4.1 总仓与子仓语义

- 1 个策略任意时刻最多 1 个非 `Closed` 总仓；每个 instrument 最多 1 个非 `Closed`
  子仓。首次 `Open` 自动创建总仓，后续 `Open`/`Adjust` 复用同一总仓。
- 总仓状态 `Open → Closing → Closed`；`Closing` 不得回退到 `Open`。策略看到
  `master.state == Closing` 时必须停止开仓/加仓/调仓，`StrategyBase` 已内置该门控。
- 子仓是实际下单与成交载体；总仓只承载生命周期和组合指标（由子仓聚合派生）。
- `CloseMaster` 平掉本策略全部非 `Closed` 总仓及其子仓；子仓全部 `Closed` 后，
  settle 收口将总仓置 `Closed`（平掉最后一个子仓同样会触发自动收口）。
- L3 组合止损走 `CloseMaster`；N=1 时与旧单仓链路数值等价。

## 5. 动作语义

| 动作 | 语义 |
|---|---|
| `Open` | 开仓；新策略通常 `price = bar.close`、`stop_price = price * 0.98`、`atr = 0.0`、`strength = 1.0` |
| `Close` | 平仓；模板不产生，配置 `exit` / `portfolio_exit` 由引擎自动处理 |
| `Reverse` | 先平后开；模板不产生 |
| `Adjust` | 调整子仓到 `target_qty`（带符号净数量，多头为正），不产生信号 ID；总仓生命周期由引擎维护 |
| `CloseMaster` | 组合级平仓：平掉本策略全部非 `Closed` 总仓及其子仓；无总仓时 no-op；子仓全部 `Closed` 后才收口总仓 |
| `UpdateStop` | 更新止损价 |
| `UpdateExit` | 更新退出规则状态 |
| `Hold` | 组合级 no-op |

- 引擎保证 `stale`、`entry_eligible`、`rebalance_due`、quorum、1:1 开仓等约束。
- `Adjust` 的目标数量口径：`target = weight * max_total_notional * nav / (price * multiplier)`。
  等权目标合计等于总仓预算（`max_total_notional * nav`），与执行层容量闸门
  `remaining = max_total_notional * nav - master.gross_notional` 同口径；用全额
  NAV 计算会让目标恒高于容量，每个调仓轮都产生无谓拒单。
- 本地撮合（paper）首次 `Open` 的数量由引擎按 `sizing.per_position_pct` 计算并按
  剩余容量裁剪，不是直接按等权 `1/top_k`；等权目标要等下一次 `rebalance_due` 的
  `Adjust` 才校正，因此新仓到下一调仓轮之间仓位可能低于目标权重。
- live 场所按固定名义下单（trading-service `order_cash`，忽略 runner qty）：runner
  通过 `ExecutionPort::venue_order_notional()` 生成影子数量
  `qty = order_cash / (price * multiplier)`，使本地 `gross_notional` 立即按场所真实
  敞口预留容量。启动校验要求
  `order_cash ≤ min(per_position_pct, max_position_pct) * initial_cash_total`。
- live 加仓只能按固定名义执行：目标差额小于单笔名义、或剩余容量/子仓预算不足时
  视为已达标静默跳过，不计入 `rejected`。
- 模板在调仓轮对持仓的处理：数据有效但掉出 top_k → `Adjust{0}`；`stale` 或缺少
  `rank_indicator` 特征 → 保持仓位，等下一个可判定的调仓轮，避免缺数据误平仓。
- `Open.stop_price` 是标的级初始止损（模板示例 `price * 0.98` 即 -2%，由持仓层
  处理）；`portfolio_exit.stop_loss_pct` 是组合级 L3 全平（示例 8%）。两者层级
  不同，示例中 2% 标的止损先于 8% 组合止损触发。
- 信号 ID 统一使用 `sub_` 命名空间（`sub_<strategy_id>`），不存在旧 `sig_`/`Legacy` 分支。

## 6. 配置字段速查

完整字段、默认值、校验规则与单标/多标差异见 [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md)；
下表只列横截面视角的常用项。

| 字段 | 说明 |
|---|---|
| `strategy_type` | 注册名，例如 `template_xsec` / `template_xsec` |
| `strategy_id` | 组合 ID，例如 `template_xsec:default`；多组配置集内必须唯一 |
| `universe` | 标的列表；1 条字符串 = 单所默认，`{symbol, exchange}` 为完整形 |
| `timeframes` | `timeframes[0]` = base TF（决策节奏） |
| `indicators` | 指标级 tf 绑定；白名单 `adx/rsi/ema/sma/atr/macd` |
| `entry` / `exit` | `default` + overrides 的条件规则；空 `entry` 视为合格 |
| `portfolio_exit` | 组合浮盈回落/止损全平；N=1 与旧单仓 trail 数值等价 |
| `rebalance` | 两阶段调仓；N=1 可省略并走单标路径，N≥2 必填 |
| `risk` / `sizing` | 总仓名义上限 `max_total_notional`；`per_position_pct` 单仓目标比例；可选 `max_position_pct` 子仓硬上限（缺省不启用） |
| `params` | 策略私有 schema；模板为 `rank_indicator/higher_is_better/top_k` |

`strategy.yaml` 只承载策略级字段。注意该 schema **不会拒绝未知字段**（拼错会被静默
忽略），必须对照 [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md) 逐项确认；
`_global.yaml` 与旧 per-symbol 文件才带 `deny_unknown_fields`。
`cash`、`services`、`mode`、`leverage`、`contracts` 等运行参数在共享的
`config/_global.yaml`（配置集根目录），不在 `strategy.yaml` 内；live 下
`cash` 是场所单笔开仓名义（trading-service `order_cash`），启动校验
`cash ≤ min(per_position_pct, max_position_pct) * initial_cash_total`。

### 6.1 配置布局

| 布局 | 路径 | 适用 |
|---|---|---|
| 策略级单文件 | `config/<group>/strategy.yaml` | 横截面 / 统一端口策略；backtest 与 run 均可用 |
| 配置集 | `config/<set>/<group>/strategy.yaml` + `config/<set>/_global.yaml` | `run` 多组同时运行；组间 `strategy_id` 必须唯一 |
| 旧 per-symbol | `config/<strategy>/<SYMBOL>.yaml`（`strategy:` 字段） | 单标 SPI 路径；已标记退役，仅兼容历史配置 |

`--config` 指向目录时：目录本身有 `strategy.yaml` = 单组策略配置；目录本身没有、
但直接子目录有 = 配置集。`backtest` 只接受单组，`run` 两者都接受。

- `rebalance.top_k` ≤ universe 数；`bottom_k` 必须为 `0`；`weight_scheme` 仅 `equal`。
- `params.top_k` 是横截面路径实际选股数（单标路径不使用），与 `rebalance.top_k`
  保持同值，避免配置校验与实际选股不一致。
- `params.rank_indicator` 必须与 `indicators` 中声明的键一致；否则所有候选因特征缺失
  被过滤，策略完全不交易。
- N=1 配置可省略 `rebalance`；模板单标路径不依赖 `rebalance_due`。
- 期货配置另需：`universe` 写 `{ symbol, exchange }`，合约参数放
  `config/contracts.yaml`，不要写进 `strategy.yaml`；完整差异见
  [CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)。
- `sizing.max_position_pct` 可选；配置后 Open 数量取 `per_position_pct`、该硬上限与
  总仓剩余容量三者的更小值。`Adjust` 仅在加仓方向套用硬上限：已超限仓位只禁止继续
  加仓，不强制减仓。
- 策略 NAV 是策略总资金：paper/live 每个 slot 以完整总资金虚拟起账时，引擎汇总
  `current_nav` / `current_cash` 会扣除重复起账现金，`PortfolioContext.nav` 不虚高。

## 7. 单体起步 → 加代币变多体

1. **单标**：`universe` 写 1 个标的，省略 `rebalance`，运行
   `config/tpl_trend_crypto/strategy.yaml`。模板走单标路径。
2. **加代币**：`universe` 加第二行（例如 `BNBUSDT`），补 `rebalance`
   （`top_k` ≤ universe 数、与 `params.top_k` 同值），运行
   `config/tpl_xsec_crypto/strategy.yaml`。同一份策略代码自动切横截面路径。
3. 不需要改任何注册文件；同一份策略代码自动切横截面路径。

```bash
# 单标
cargo run -p strategy-app -- backtest \
  --config config/tpl_trend_crypto/strategy.yaml \
  --start 20260901 --end 20260915 --data-dir ./data/klines

# 横截面（同一模板，加了一行代币与 rebalance）
cargo run -p strategy-app -- backtest \
  --config config/tpl_xsec_crypto/strategy.yaml \
  --start 20260901 --end 20260915 --data-dir ./data/klines
```

## 8. 测试与运行

- 基类测试：`crates/strategies/tests/strategy_base.rs`，覆盖单标开仓/跳过、
  横截面 `rebalance_due`、排名、等权调仓、掉出 `top_k`、`max_total_notional` 预算。
- 模板单测：`crates/strategies/src/template_xsec.rs` 的 `#[cfg(test)]` 模块，
  覆盖单标无 `rebalance`、横截面 `rebalance_due`、特征缺失、调仓、`params` 校验。
- 注册集成测试：`crates/strategies/tests/registry.rs`（注册名 = `strategy_type()`，
  且每个策略都声明 1m）。
- 配置装配测试：`crates/app/tests/strategy_config_guards.rs` 的
  `template_xsec_sample_config_loads_and_instantiates` 与
  `template_xsec_single_config_loads_without_rebalance`。
- 端到端：`crates/app/tests/strategy_backtest_e2e.rs`、
  `strategy_live_mock_e2e.rs`；回测/paper 一致性由这两个用例端到端验收。

```bash
cargo test -p strategy-strategies
cargo test -p strategy-app --test strategy_config_guards template_xsec
cargo test -p strategy-app --test strategy_backtest_e2e
```

- 回测：见第 7 节两条命令；成功输出 `横截面回测完成: ...`，随后一行 `bars=... fills=...`。
- paper/live：`run --config ... --mode paper_trading/live`；启动前置、mock 演练、
  `nav_source=local_mirror` 口径见 `docs/RUNBOOK.md` §5。

## 9. 本期边界

- `bottom_k` 必须为 `0`；空头侧未实现。
- `weight_scheme` 仅支持 `equal`。
- 指标白名单固定：`adx/rsi/ema/sma/atr/macd`。
- 单组单 exchange、单 account；`multi_instance` 模式拒启。
- NAV 仍为 `local_mirror`，不冒充券商权益帧。
- KlineCache 降级预热；warmup 不足时不伪造特征。
- N>1 无需“升级”：任何已注册策略直接支持横截面。
- 运行时模块名已从 `xsec` 改为 `strategy`；策略文件名/配置里的 `xsec_` 前缀
  只是命名历史，不是模块路径，新增策略不必带该前缀。

## 10. 提交前清单

- `Params` 使用 `#[serde(default, deny_unknown_fields)]`；拼错参数必须报错。
- 保留 `snapshot.closed` 只读，不在策略内引入 `now()`。
- 等权目标必须乘 `portfolio.max_total_notional`，不得用全额 NAV。
- `portfolio.master.state == Closing` 时不开仓/不加仓/不调仓。
- 不自己维护仓位/总仓状态；仓位账本由引擎持有。
- 单测至少覆盖：单标无 `rebalance` 可开仓、横截面 `rebalance_due` 门控、
  `stale`/`entry_eligible` 过滤、持仓遇 stale/缺特征不清仓、特征缺失、持仓调仓、
  掉出 top_k 清零、风险预算目标数量。
- 跑 `cargo fmt --check`、`cargo clippy --workspace --all-targets -- -D warnings`、
  `cargo test --workspace`。
- 策略名小写蛇形且全局唯一（`registry.rs` 测试校验）；配置 `strategy_id` 在多组配置集内唯一。
