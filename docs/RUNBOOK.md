# Runbook — strategy-rs 本地运行与联调

> 本项目当前无集中部署、无服务端健康端点：所有运行均为本地/单机进程。
> 本文覆盖真实可执行的启动、联调、排障与回退步骤。

## 1. 组件

| 组件 | 形态 | 默认端点 |
|---|---|---|
| strategy-rs（本仓库） | 本地二进制 `strategy-rs`（backtest/run/download/inspect） | — |
| kline-service | 外部进程（REST 回填 + WS 实时） | 见 strategy.config.yaml services.kline |
| trading-service | 外部进程（南向 WS 收策略；对交易所主动外连） | 南向 9100 |
| xili-exchange | trading_service_rs 的交易所模拟二进制（REST 下单 + 账户事件 WS） | 9200 |

## 2. live / paper 运行

```bash
# 1) 起交易所模拟（xili-exchange；REST + 账户事件 WS 统一监听 9200）
cd ~/workspace/trading_service_rs
cargo run --bin xili-exchange -- \
  --instance xili --order-fill-interval 500ms --addr 127.0.0.1:9200

# 2) 起 trading-service（南向 9100，主动外连 xili；本地联调跳过南向 HMAC）
cargo run --bin trading-service -- \
  --south-addr 127.0.0.1:9100 --exchange xili=http://127.0.0.1:9200 \
  --south-dev-insecure

# 3) 本仓库（mode 按 per-symbol 配置；见下文混合模式）
cargo run -p strategy-app -- run --config config
```

- trading-service 需 `{data-dir}/account.csv` 含与 `--exchange` 名称精确匹配的
  有效账户（e2e/冒烟用 `test_binance` + `exchange=xili`；首联调可复制
  `account.example.csv` 并把 exchange 改为 `xili`）。
- 停止：终端 `Ctrl-C`，优雅关停（完成在途 flush 后退出）。
- 状态产物：`{output_dir}/state/`（Store 日快照 CSV）；旁路报表见 README。
- `state/{YYYYMMDD}/{model}.csv` 为**全量日快照**（UTC 日）：目录名 = flush 当日，
  非「仓位形成日」；跨日 flush 时**物理裁剪全部终态行**（history_position/软删
  position/cancelled stop/终态 signal），在途行永久保留。`recover` 只读最新
  存在的日文件，旧日目录不再参与恢复。按日成交归档读
  `history_positions/{strategy}/{YYYYMMDD}.csv`（append-only，永不裁剪）。
  回滚开关：yaml `state_prune_terminal: false`（旧行为：不裁 + 多日 recover）。

### 2.1 per-symbol mode（live / paper_trading 可混合）

每个 `config/{strategy}/{SYMBOL}.yaml` 的 `mode` **各自独立生效**。
脚手架自带 `config/example_dual_ma/example.yaml` 作样例；per-symbol 配置需自行创建：

```bash
cp config/example_dual_ma/example.yaml config/example_dual_ma/BTCUSDT.yaml
cp config/example_dual_ma/example.yaml config/example_dual_ma/WLDUSDT.yaml
```

```yaml
# config/example_dual_ma/BTCUSDT.yaml
mode: live            # 真单通道（TradingGateway）
# config/example_dual_ma/WLDUSDT.yaml
mode: paper_trading   # 本地撮合（BacktestOrderClient，产出仓位五件套）
```

**解析优先级**：CLI `--mode`（强制全覆盖）> per-symbol 文件 > `_global.yaml` > 默认 `paper_trading`。

**运行时行为**：

| | `live` | `paper_trading` |
|---|---|---|
| 下单网关 | `TradingGateway`（真 WS） | `BacktestOrderClient`（本地撮合） |
| 双保险 | `services.trading.submit_enabled` ∧ 实例闸门 | 不经交易 WS |
| 成交价 | 真实市价 | 意图价（`notional_per_order = cash`） |
| 远程仓位对账 | 每分钟 `get_positions` | **跳过**（不被 external_close 误平） |
| 交易 WS 重连对账 | 触发 | 不波及 |
| strategy_name 后缀 | `..._LIVE` | `..._PAPER_TRADING` |
| 五件套 state 文件 | ✅ | ✅ |

**启动时打印 mode 一览表**（肉眼核对分流）：

```
运行实例 mode 一览：
  BTCUSDT   example_dual_ma → live
  WLDUSDT   example_dual_ma → paper_trading
```

**紧急熔断**：`services.trading.submit_enabled: false` 时**所有** live 也不发单。

### 2.2 查看 state 快照（strategy-rs inspect）

`state/{YYYYMMDD}/{model}.csv` 是机器恢复格式
（`id,deleted,created_at_ms,updated_at_ms,payload_json`），域数据嵌在
转义 JSON 里，终端直读困难。`inspect` 子命令**只读**展开 `payload_json`
为扁平列（嵌套对象以点号拼键，至多 2 层），**不改动快照格式**——
恢复语义（软删墓碑、跨日去重、schema-less 演进）不受影响。

```bash
# 目录 = 该日全部 model；单个 CSV = 只看一个 model
cargo run -p strategy-app -- inspect {output_dir}/state/{YYYYMMDD}
cargo run -p strategy-app -- inspect {output_dir}/state/{YYYYMMDD}/signal.csv

# 输出形态
--json         # JSONL（每行一条记录，null 为 JSON null）
--out <dir>    # 导出扁平 CSV（{name}.flat.csv，Excel/pandas 可直接读）
--limit <N>    # 行数限制（默认 50；0 = 全部）
```

展示规则：展开列数 ≤12 用横向对齐表格，>12 用纵向键值块（每行一个
记录，含 `created_at`/`updated_at`，软删行标 `[deleted]`）；
时间戳列格式化为 UTC `YYYY-MM-DD HH:MM:SS`。任意**扁平** CSV
（如对外报表 `signals/{name}/signal.csv`）可原样直读。

排障示例（核对某信号的意图与成交）：

```bash
cargo run -p strategy-app -- inspect {output_dir}/state/{YYYYMMDD}/signal.csv --limit 1
```

## 3. 冒烟联调（不开完整策略，直验下单链路）

```bash
cargo run -p strategy-broker --example trading_service_smoke -- \
  --url ws://127.0.0.1:9100/ws --strategy-name smoke --secret <key> \
  --exchange xili --account test_binance --instrument btcusdt \
  --side long --quantity 1
# 平仓链路：追加 --side short --close
```

预期：打印 `connected as smoke` → `ack client_order_id=0x…` →
`order_event Filled … filled=1@80000`。trading-service 日志出现
`south message received`（option=place_order），exchange-mock 日志出现
`REST place_order received` 与 `order event pushed`。

> 当前真实服务尚未强制 HMAC，`--secret` 任意值可连（额外 query 被忽略）；
> 服务端开启鉴权后密钥须与服务端配置一致，mock 侧始终强制校验。

## 4. 健康检查（无 HTTP 健康端点时的替代手段）

```bash
cargo test --workspace                       # 进程内行为自检

# 外部服务 e2e（先按 §2 起 trading-service + exchange-mock；未设变量自动跳过）
TRADING_WS_URL=ws://127.0.0.1:9100/ws \
  cargo test -p strategy-broker --test async_gateway_e2e -- --nocapture
cargo audit                                  # 依赖漏洞
ss -ltnp | grep -E '9100|9101|50051|50052'  # 外部服务端口监听确认
```

## 5. 常见问题

| 现象 | 排查 / 处理 |
|---|---|
| `连接失败` 反复退避 | trading-service 未起或 `ws_url`/端口不符；检查 9100 监听 |
| ack 超时（Retryable） | 目标交易所未注册为 `exchange` 配置同名实例；检查北向 9101 连接与注册日志 |
| 本地拒单 `submit_enabled=false` | paper 模式强制 false 属预期；live 还需配置显式置 true |
| 本地拒单 `数量非法` | 信号 size=0 且 `default_quantity<=0`；配置正数量或由策略带数量 |
| mock 测试握手 401 | nonce 5 分钟内重放，或签名/密钥不一致；每连接须新 nonce |
| 状态疑似被旧快照覆盖 | 查看 Store recover 日志；Store 盘载只填空缺 id（活行不被磁盘覆盖） |

## 6. 回退

- 代码/配置：`git checkout <旧提交> -- strategy.config.yaml` 或直接切回旧提交后
  重新 `cargo run`；无数据库迁移，Store CSV 为追加/快照结构，跨版本读取异常时
  将当次 `state/` 目录备份后重跑。
- 协议：trading-service 线协议变更以 `contracts/trading-service.md` 版本号为准；
  客户端不兼容时回退双方到同一版本（已知限制见
  `contracts/trading-service.md` §5.1）。
