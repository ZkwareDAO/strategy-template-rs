# strategy-template-rs — Rust CTA 策略开发脚手架

> **北极星**：clone 本仓库，10 分钟内跑通一次回测；照着 example 复制，
> 写出你的第一个 CTA 策略。

Rust 版 CTA 策略开发脚手架（三层架构：外部服务 → 策略 → 信号/仓位统一 Store 管理。
现状架构见 [`docs/design-package/`](docs/design-package/)；
[`docs/design.md`](docs/design.md) 已登记为**历史文档**，不再更新）。
流水线：**外部服务（trading-service 单连接：订单/回报 + K 线订阅）→ 策略 → 信号管理
（统一 Store 持久化/重试/对账）→ 订单网关 → 仓位管理（统一 Store）**。
回测与 live 共用同一套策略 trait、信号状态机与信号/仓位同步内核
（由 `strategy_backtest_e2e.rs` / `strategy_live_mock_e2e.rs` 端到端验收）。

> 交易链路不直连交易所：`run` 经 trading-service（下单/回报/K线订阅同一条 WS；
> klines-service 已退役，旧 `services.kline` 节写了 `run` 即拒启；端点
> 全部走配置文件 `services:` 节，缺失即 fail-fast）。`download` 命令的
> 公共归档下载不在交易链路，予以保留。

## 文档地图

| 你是谁 | 看哪里 |
|---|---|
| **要写新策略** | [`docs/strategy/`](docs/strategy/QUICKSTART.md) — 从 QUICKSTART 开始 |
| 要理解框架 | [`docs/design-package/`](docs/design-package/) — 需求/架构/详设/接口（现状）；[`docs/design.md`](docs/design.md) — 历史文档（迁移前架构，不再更新） |
| 要部署运维 | [`docs/RUNBOOK.md`](docs/RUNBOOK.md) |
| 要贡献代码 | [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md) |

## 10 分钟上手

```bash
# 1. 跑通基线
cargo test --workspace

# 2. 跑一次回测（数据已在 data/klines/；缺则先 download）
cargo run -p strategy-app -- backtest \
  --config config/tpl_trend_crypto/strategy.yaml \
  --start 20260801 --end 20260826

# 3. 复制 example 写你的策略
cp crates/strategies/src/template_trend.rs crates/strategies/src/my_strategy.rs
```

详见 [`docs/strategy/QUICKSTART.md`](docs/strategy/QUICKSTART.md)。

## 新增策略（三步走）

1. 复制 `crates/strategies/src/template_trend.rs` 为你的策略文件；
2. 改策略名与逻辑，文件尾 `register_strategy!` 自注册——**无需修改任何其他文件**，
   `build.rs` 自动发现；
3. 建配置 `config/<group>/strategy.yaml`（样例见
   `config/tpl_trend_crypto/strategy.yaml`），`strategy_type` 填注册名。
   旧 per-symbol `<SYMBOL>.yaml` 仍被兼容读取，但已标记退役；新配置统一用
   `strategy.yaml` 或配置集（字段与单标/多标差异见 `docs/strategy/CONFIG_REFERENCE.md`）。

模板策略（2 个，导读见 [`docs/strategy/EXAMPLES.md`](docs/strategy/EXAMPLES.md)）：
- `template_trend`：逐标趋势（crypto 单标 / 期货单标，同一份代码）
- `template_xsec`：横截面轮动（crypto N=2 / 期货 N=3，N=1 退化为单标）

## 横截面策略（1 策略 → 1 总仓位 → N 子仓位；PR-1～PR-5，回测 + live/paper）

所有策略（单标与横截面）实现同一个 `CrossSectionalStrategyPort`：从 universe=1 起步，
`universe` 加一行代币即升级为横截面，无需改代码。

```bash
# 配置 = 策略级单文件 config/<strategy>/strategy.yaml（样例 config/tpl_xsec_crypto/strategy.yaml）
cargo run -p strategy-app -- backtest \
  --config config/tpl_xsec_crypto/strategy.yaml \
  --start 20260101 --end 20260601 --data-dir ./data/klines
```

- **开发指南**：[docs/strategy/XSEC_GUIDE.md](docs/strategy/XSEC_GUIDE.md)（模板
  `crates/strategies/src/template_xsec.rs`、示例配置
  `config/tpl_trend_crypto/strategy.yaml` 与 `config/tpl_xsec_crypto/strategy.yaml`）。
- **schema**：`universe`（1 条字符串 = 单所默认；`{symbol, exchange}` 完整形）、
  `timeframes[0]` = base TF（决策节奏）、`indicators`（指标级 tf 绑定）、
  `entry`/`exit`（default + overrides，AND 纯函数）、`portfolio_exit`
  （组合浮盈回落/止损全平；N=1 与旧单仓 trail 数值等价）、`rebalance`
  （两阶段：先减仓确认、后加仓）、`risk/sizing`（杠杆上限、per_position_pct）。
- **新策略**：在 `crates/strategies/src/<name>.rs` 实现
  `strategy_engine::strategy::port::CrossSectionalStrategyPort`，文件尾
  `register_strategy!` 自注册（模板 `template_trend` / `template_xsec`）；
  `build.rs` 自动发现，无需改其他文件。
- **数据模型**：`SubPosition`/`StrategyPortfolio` 走统一 Store；`MasterPosition`
  为派生视图不落表；子仓位与信号 ID 统一用 `sub_` 命名空间。
- **实盘（PR-5）**：`run --config strategy.yaml` 已支持 paper/live（`--mode`）；
  启动前置、mock 演练、`nav_source=local_mirror` 口径与 panic→重启人工放行
  手册见 `docs/RUNBOOK.md` §5。
- **多组同进程（PR-6）**：`run --config <set_dir>`；`<set_dir>` 无顶层
  `strategy.yaml`，每个直接子目录一组（`set/_global.yaml` 共享
  `services.trading`/`mode`/flush 策略，`set/<group>/strategy.yaml` 各组独立）。
  拒启规则：`strategy_id` 唯一，exchange/account/services/run_mode/flush 一致；
  live 组间 universe 不相交（paper 允许重叠）。每组 NAV 仍是本地镜像
  （`nav_source=local_mirror`），同一 broker 账户保证金共享、不做跨组权益合并；
  目录布局与排障见 `docs/RUNBOOK.md` §5.5。
- **边界（本期）**：无券商权益帧（NAV=local mirror，不冒充券商口径）；
  `multi_instance`、L4 NAV 回撤（`risk.max_drawdown` 解析但忽略）、同标的并发
  多子仓、跨交易所实例均为非目标。设计全文：
  `deliverables/gstack/feature-dev-cross-sectional-strategy-2026-11-09.md`。

## 国内期货回测（整数手 / 手续费 / 保证金）

期货品种的回测撮合读取仓库根目录 `config/contracts.yaml`（无需在策略 YAML
中重复配置）：

```yaml
contracts:
  CU:
    multiplier: 5          # 合约乘数（5 吨/手）
    tick_size: 10          # 最小变动价位
    margin_rate: 0.11      # 交易所保证金率
    commission:            # rate=成交金额比例；per_lot=元/手
      mode: rate
      open: 0.00005
      close: 0.00005
      close_today: 0.0001   # 平今（AU/RU 为 0 = 免收）
```

回测行为：

- 开仓手数同时受两个上限约束，**只开整数手**：
  - 保证金：`手数 × (每手保证金 + 开仓手续费) ≤ 权益`
  - 名义敞口：`手数 × 每手名义金额 ≤ 权益 × leverage`
- 手续费按开仓/平仓/平今分别计收；同一 CTP 交易日平仓自动走 `close_today`。
- 收盘前 5 分钟、开盘后 5 分钟不交易：`contracts.yaml` 的 `session_opens` /
  `session_closes` + `no_trade_after_open_minutes` / `no_trade_before_close_minutes`
  定义窗口；窗口内的开/平/反手动作挂起到下一可交易 1m 开盘执行
  （止损价更新仍即时生效）。
- 盈亏 = 价差 × 合约乘数 × 手数；保证金只限制最大持仓，不从权益中重复扣除。
- `leverage` 是策略自选的名义敞口上限（1 = 不放大）；实际可开手数不会超过
  保证金率允许的交易所上限。
- 账户初始权益在品种 YAML 的 `initial_cash` 配置（缺省 5000）：

```yaml
initial_cash: 1000000    # 回测账户初始权益（元）
```

`config/contracts.yaml` 已内置 CU/AU/AG/RB/RU/I 的最新交易所标准
（数据来源与更新日期见文件头注释）；未声明的 symbol 继续走 crypto/兼容模式
（小数数量 + `--fee-rate`）。期货公司加收的保证金/手续费请按自己账户覆盖。

## 模块

| crate | 职责 |
|---|---|
| `core` | Candle/Timeframe、Signal 意图/状态机(8态)/订单状态机(7态)、FillEvent、加减仓纯函数、Strategy trait、风控、确定性 signal_id |
| `store` | 统一 model 门面：Store/ModelTable、毫秒时钟注入、日快照 CSV、墓碑与跨日 recover |
| `signals` | 信号全生命周期：SignalManager 同步内核、OrderGateway 端口、Store `signal` model、对外 signal.csv |
| `positions` | PositionManager（开/加/减/平、峰值跟踪、止损价/回落止盈、止损冷却）、四个 model、对外历史仓位固定列 CSV |
| `broker` | BacktestOrderClient、TradingServiceClient（WS 下单/回报/K线/历史多命令通道+重连）、脚本化 mock（K线回放/get_klines）、exchange_orders.csv |
| `data` | KlineClient 端口（live 实现在 `app/src/ts_kline.rs`，走 trading-service）、Feed（1m 增量聚合）、CSV 存储、live 预热、归档下载 |
| `strategies` | 策略插件（2 个模板，见 `docs/strategy/EXAMPLES.md`），build.rs 自动发现注册，**新增策略只需加文件** |
| `app` | 回测引擎、RuntimeActor、OrderRouter、bootstrap、配置、CLI、result.json/PNG 报告 |

## 状态机

- 信号：`created → validated → submitted → accepted → filled`；
  `rejected`（验证失败）、`failed`（可 `failed→submitted` 重试）、`cancelled`
- 订单：`created/sent/ack/partially_filled/filled/canceled/rejected`

## 统一 Store（model 内存表 + 日快照 CSV）

```
state/{YYYYMMDD}/
├── signal.csv  position.csv  stop.csv  cooldown.csv
├── history_position.csv
└── (回测) trade.csv  equity.csv
```

对外旁路报表（固定列、列序锁定）：
- `signals/{name}/signal.csv`
- `{name}/exchange_orders.csv`
- `state/history_positions/{name}/{YYYYMMDD}.csv`（24 列）
- 回测报告：`backtest_result.json` / `backtest_report.txt` /
  `backtest_analysis_report.md` / `charts/*.png`

> `state/{YYYYMMDD}/{model}.csv` 是**机器恢复格式**
> （`id,deleted,created_at_ms,updated_at_ms,payload_json`），域数据
> 嵌在转义 JSON 里，终端直读困难——用 `inspect` 子命令展开查看：
>
> ```bash
> # 目录（该日全部 model）或单个 CSV；列≤12 横向表格，>12 纵向键值块
> cargo run -p strategy-app -- inspect backtest_output/<run>/<SYMBOL>/engine/state/<YYYYMMDD>
> # 其他形态：--json（JSONL）/ --out <dir>（导出扁平 CSV）/ --limit N（默认 50，0=全部）
> ```
>
> 只读工具，不改动快照格式（恢复语义不受影响）。详见
> [`docs/RUNBOOK.md`](docs/RUNBOOK.md) §2.2。

## 外部服务配置（run 必需）

见 `config/_global.yaml` 的 `services:` 节与
[`docs/RUNBOOK.md`](docs/RUNBOOK.md)。消息契约见 `contracts/`。

## 测试

```bash
cargo test --workspace
cargo clippy --all-targets -- -D warnings
```

## 免责声明

本项目是**量化策略研究与开发脚手架**，不构成任何投资建议。

- 回测结果基于历史数据与简化的成交模型，**不代表未来表现**
- 加密货币衍生品交易具有高杠杆风险，可能导致本金全部损失
- 使用者需自行承担全部交易决策与盈亏结果
