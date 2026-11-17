# Contributing — strategy-rs

## 开发环境

- Rust（edition 2021），工具链以仓库根 `Cargo.toml` workspace 为准；无系统级依赖。
- 克隆后 `cargo build --workspace` 即可。
- 运行目录下的 `.env` 会被自动加载（`dotenvy`，缺失不报错）。`STRATEGY_SECRET`
  环境变量可覆盖 `services.trading.strategy_secret`（live 模式 HMAC 密钥注入）。
  `.env` 与 `strategy.config.yaml` 含真实密钥，均已 gitignore，**禁止提交**。
- workspace 成员：core / signals / positions / data / indicators / broker /
  store / strategies / app（二进制 `strategy-rs`）。

## 常用命令

| Command | Description |
|---------|-------------|
| `cargo run -p strategy-app -- backtest --config <dir> --symbol BTCUSDT --start 20260801 --end 20260826` | 回测（1m CSV 逐根回放，与实盘同一引擎管线） |
| `cargo run -p strategy-app -- run --config strategy.config.yaml --mode live` | 实盘运行（模式：`live` / `paper_trading`，Ctrl-C 优雅退出） |
| `cargo run -p strategy-app -- download --symbol BTCUSDT` | 公共数据源离线 K 线导入（非交易链路） |
| `cargo run -p strategy-broker --example trading_service_smoke -- --url ws://127.0.0.1:9100/ws --strategy-name smoke --secret <key> --exchange xili --account test_binance --instrument btcusdt --side long --quantity 1` | 真实 trading-service 联调冒烟（加 `--side short --close` 验平仓） |
| `cargo test --workspace` | 全量测试 |
| `cargo test -p strategy-broker` | 单 crate 测试（含 broker 内联单测） |
| `cargo test -p strategy-broker --test async_gateway_e2e` | 交易链路端到端测试（鉴权 mock） |
| `cargo clippy --workspace --all-targets -- -D warnings` | 静态检查（零告警门禁） |
| `cargo fmt --check` | 格式检查（提交前 `cargo fmt`） |
| `python3 -I scripts/check_doc_refs.py` | **引用坐标与日志锚校验**（CI 第四步）：文档与代码注释里的 `file:行` 必须可解析、不歧义、不越界；声明 `refcheck:anchors=on` 的 runbook，其反引号日志锚必须存在于 `crates/**/*.rs` |
| `cargo audit` | 依赖漏洞扫描（RustSec 数据库） |

## 配置

- YAML 配置结构以 `crates/app/src/config.rs` 为唯一事实源（`FileConfig`/
  `TradingServiceCfg` 等结构与其内嵌解析测试即示例）。
- 外部服务（kline/trading）字段说明与示例见 `README.md`「外部服务配置」。
- 线协议契约（含已知限制）：`contracts/trading-service.md`（下单/回报 + K 线
  订阅 `subscribe_kline`/`kline_1m_batch`/历史 `get_klines`，§2/§8）。
  已退役契约归档于 `docs/archive/`（klines-service）。

## 测试约定

- 先写失败测试再实现；测试断言**真实行为**，不断言 mock 的内部动作。
- 异步测试用 `#[tokio::test(flavor = "multi_thread", worker_threads = N)]`；
  同步点用事件接收 + 有界 timeout，**禁止用固定 sleep 等待**（成交节奏可用
  mock 的 `fill_interval` 控制）。
- 网络类测试一律打 `127.0.0.1:0` 随机端口的内置 mock（`broker::mock::spawn`
  鉴权版 / `spawn_raw` 裸脚本版），禁止依赖真实外网。
- 回测确定性：时间只取 K 线 open_time，禁 `Utc::now()`；live 元数据时间走
  `EpochClock`。改完回测黄金断言（pnl/equity 锚点）必须重新确认。

## 代码风格

- `cargo fmt` + clippy `-D warnings` + `check_doc_refs.py` 是硬门禁；提交前本地必过。
- **改了代码就复查引用坐标**：`file:行` 是易碎坐标，任意上方编辑都会静默作废其下
  所有同文件引用（2026-11-09 实测：给注释加 2 行，令 `SymbolEntry` 203→205，当场
  顶歪两处刚写下的引用）。三门只看代码，抓不到这个——所以有第四步。
- **豁免必须显式且可审**：行内 `refcheck:ignore`（历史叙述、外部仓库路径）或文件头
  `refcheck:coords=off`（账本类文档引用已删实现坐标）。脚本退出时打印关闭清单，
  债务可见；**唯一正确修法是改坐标/改锚，不是加豁免**。
- 模块/公共项写中文 `///` 文档注释，解释“为什么”而不是复述代码。
- 并发：控制消息走有界通道，model 数据走共享 Store 表；禁止在 tokio runtime 内
  `Handle::block_on`；DashMap 守卫不得跨 `.await`。
- 重构与功能分开提交；人机协作提交的 commit message 末尾带
  `Co-Authored-By: Claude Code <noreply@anthropic.com>`。

## PR 检查单

- [ ] `cargo test --workspace` 全绿，新行为有真实测试覆盖
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` 零告警
- [ ] `cargo fmt --check` 通过，`cargo audit` 无未处理漏洞
- [ ] `python3 -I scripts/check_doc_refs.py` 通过（引用坐标与日志锚；新增豁免需在
      PR 描述里逐条说明理由）
- [ ] 涉及线协议/配置变更时同步 `contracts/`、`README.md`、`docs/`
- [ ] 交易路径保持双保险：`submit_enabled=false` 默认 + `default_quantity=0` 默认
