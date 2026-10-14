# strategy-template-rs — Rust CTA 策略开发脚手架

> **北极星**：clone 本仓库，10 分钟内跑通一次回测；照着 example 复制，
> 写出你的第一个 CTA 策略。

Rust 版 CTA 策略开发脚手架（三层架构：外部服务 → 策略 → 信号/仓位统一 Store 管理，详见 [`docs/design.md`](docs/design.md)）。
流水线：**外部服务（kline-service / trading-service）→ 策略 → 信号管理
（统一 Store 持久化/重试/对账）→ 订单网关 → 仓位管理（统一 Store）**。
回测与 live 共用同一套策略 trait、信号状态机与信号/仓位同步内核
（由 `crates/app/tests/backtest_paper_parity.rs` 自动验收）。

> 交易链路不直连交易所：`run` 经 kline-service / trading-service（端点
> 全部走配置文件 `services:` 节，缺失即 fail-fast）。`download` 命令的
> 公共归档下载不在交易链路，予以保留。

## 文档地图

| 你是谁 | 看哪里 |
|---|---|
| **要写新策略** | [`docs/strategy/`](docs/strategy/QUICKSTART.md) — 从 QUICKSTART 开始 |
| 要理解框架 | [`docs/design.md`](docs/design.md) — 架构、状态机、Store 语义 |
| 要部署运维 | [`docs/RUNBOOK.md`](docs/RUNBOOK.md) |
| 要贡献代码 | [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md) |

## 10 分钟上手

```bash
# 1. 跑通基线
cargo test --workspace

# 2. 跑一次回测（数据已在 data/klines/；缺则先 download）
cargo run -p strategy-app -- backtest \
  --config config/example_dual_ma --symbol BTCUSDT \
  --start 20260801 --end 20260826

# 3. 复制 example 写你的策略
cp crates/strategies/src/example_dual_ma.rs crates/strategies/src/my_strategy.rs
```

详见 [`docs/strategy/QUICKSTART.md`](docs/strategy/QUICKSTART.md)。

## 新增策略（三步走）

1. 复制 `crates/strategies/src/example_dual_ma.rs` 为你的策略文件；
2. 改策略名与逻辑，文件尾 `register_strategy!` 自注册——**无需修改任何其他文件**，
   `build.rs` 自动发现；
3. 建配置 `config/<strategy>/<symbol>.yaml`（样例见
   `config/example_dual_ma/example.yaml`），`strategy:` 字段填注册名。

教学 example：
- [`example_dual_ma.rs`](crates/strategies/src/example_dual_ma.rs) — 单周期双均线入门
- [`example_mtf_trend.rs`](crates/strategies/src/example_mtf_trend.rs) — 多周期趋势

## 模块

| crate | 职责 |
|---|---|
| `core` | Candle/Timeframe、Signal 意图/状态机(8态)/订单状态机(7态)、FillEvent、加减仓纯函数、Strategy trait、风控、确定性 signal_id |
| `store` | 统一 model 门面：Store/ModelTable、毫秒时钟注入、日快照 CSV、墓碑与跨日 recover |
| `signals` | 信号全生命周期：SignalManager 同步内核、OrderGateway 端口、Store `signal` model、对外 signal.csv |
| `positions` | PositionManager（开/加/减/平、峰值跟踪、止损价/回落止盈、止损冷却）、四个 model、对外历史仓位固定列 CSV |
| `broker` | BacktestOrderClient、TradingServiceClient（WS 三通道+重连）、脚本化 mock、exchange_orders.csv |
| `data` | KlineClient 端口、RemoteKlineServiceClient、Feed（1m 增量聚合）、CSV 存储、live 预热、归档下载 |
| `strategies` | 策略插件（example_dual_ma / example_mtf_trend），build.rs 自动发现注册，**新增策略只需加文件** |
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
