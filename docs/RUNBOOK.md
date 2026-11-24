# Runbook — strategy-rs 本地运行与联调

> 本项目当前无集中部署、无服务端健康端点：所有运行均为本地/单机进程。
> 本文覆盖真实可执行的启动、联调、排障与回退步骤。

## 1. 组件

| 组件 | 形态 | 默认端点 |
|---|---|---|
| strategy-rs（本仓库） | 本地二进制 `strategy-rs`（backtest/run/download/inspect） | — |
| trading-service | 外部进程（南向 WS 收策略；对交易所主动外连；**K线订阅/历史回填同此一条连接**，契约 §8） | 南向 9100 |
| xili-exchange | trading_service_rs 的交易所模拟二进制（REST 下单 + 账户事件 WS） | 9200 |

## 2. live / paper 运行

```bash
# 1) 起交易所模拟（xili-exchange；REST + 账户事件 WS 统一监听 9200）
cd ~/workspace/trading_service_rust
cargo run --bin xili-exchange -- \
  --instance xili --order-fill-interval 500ms --addr 127.0.0.1:9200

# 2) 起 trading-service（南向 9100，主动外连模拟所；本地联调跳过南向 HMAC）。
#    模拟所挂名要与**策略帧里的 exchange**一致（账户 exchange 字段同样精确匹配）：
#    - §3 smoke（帧名由 smoke CLI 给）：挂 xili
cargo run --bin trading-service -- \
  --south-addr 127.0.0.1:9100 --exchange xili=http://127.0.0.1:9200 \
  --south-dev-insecure
#    - §4 async_gateway_e2e（帧硬编码 binance）：改挂 binance（注册表里 xili
#      provider 同名优先，模拟所接管该名；账户保持样例 exchange=binance）
cargo run --bin trading-service -- \
  --south-addr 127.0.0.1:9100 --exchange binance=http://127.0.0.1:9200 \
  --south-dev-insecure

# 3) 本仓库（mode 按 per-symbol 配置；见下文混合模式）
cargo run -p strategy-app -- run --config config
```

- trading-service 需 `{data-dir}/account.csv` 含与**策略帧 exchange 精确匹配**的有效账户：
  §4 `async_gateway_e2e` 直接复制 `account.example.csv` 原样用（`test_binance` +
  `exchange=binance`，配 §2 步 2 的 binance 挂名变体）；§3 smoke 传 `--exchange xili`
  时账户 exchange 才改 `xili`。旧文「e2e 用 exchange=xili」经实测会把 e2e 开/平仓帧
  全部拒在账户解析（帧硬编码 `binance`），已订正。
- 停止：终端 `Ctrl-C`，优雅关停（完成在途 flush 后退出）。
- 状态产物：`{output_dir}/state/`（Store 日快照 CSV）；旁路报表见 README。
- `state/{YYYYMMDD}/{model}.csv` 为**全量日快照**（UTC 日）：目录名 = flush 当日，
  非「仓位形成日」；跨日 flush 时**物理裁剪全部终态行**（history_position/软删
  position/cancelled stop/终态 signal），在途行永久保留。`recover` 只读最新
  存在的日文件，旧日目录不再参与恢复。按日成交归档读
  `history_positions/{strategy}/{YYYYMMDD}.csv`（append-only，永不裁剪）。
  回滚开关：yaml `state_prune_terminal: false`（旧行为：不裁 + 多日 recover）。

### 2.1 per-symbol mode（已退役，2026-11-16）

> 旧 per-symbol 目录模式已退役（2026-11-16）：`run`/`backtest` 只接受 `strategy.yaml`
> 或配置集，旧读取路径已删除；本节保留为历史参考。

每个 `config/{strategy}/{SYMBOL}.yaml` 的 `mode` **各自独立生效**；**文件名即标的身份**
（目录模式按 `{SYMBOL}.yaml` 认领，见 §7.1 末注）。历史样例已迁移为
`config/tpl_trend_crypto/strategy.yaml`，per-symbol 样例不再随仓库提供。

历史格式示例（现已不可直接运行）：

```yaml
# config/tpl_trend_crypto/BTCUSDT.yaml
mode: live            # 真单通道（TradingGateway）
# config/tpl_trend_crypto/WLDUSDT.yaml
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
  BTCUSDT   template_trend → live
  WLDUSDT   template_trend → paper_trading
```

**紧急熔断**：`services.trading.submit_enabled: false` 时**所有** live 也不发单。
⚠️ 2026-11-09 起熔断有前提：**必须配真实 `services.trading.ws_url`**。若 `trading`
子块缺席（端点停留在占位值 `ws://127.0.0.1:9100/ws`），`run` 对 live 实例**直接拒绝启动**
——占位端点连的是空气，"熔断"会变成"静默永不下单还看不出区别"（retirement §7 风险 8；
判据 `TradingServiceCfg::is_unconfigured` 只看端点，不看 `submit_enabled`）。
paper/smoking 命中占位端点不致命，但启动日志会 `warn` 明示「不会产生任何真实下单」。

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

### 2.3 K线源 = trading-service（mock 回放实盘回测）

K 线唯一通道即 trading-service 连接订阅（klines-service 已退役；yaml 里写
`services.kline` 节会**直接拒启**，报错含迁移指引）。联调/回测时
由本地 mock 自动读取 `data/klines/1m` 的 CSV 回放，端到端走通
「K线 → 策略 → 下单 → 成交」同一链路（契约见 `contracts/trading-service.md` §8）。

```yaml
# 策略 yaml 的差异项（其余照常）——只需 trading 节，K 线走同一条连接
services:
  trading:
    ws_url: ws://127.0.0.1:9112/ws
    strategy_secret: "dev-secret"    # 与 mock --secret 一致
    submit_enabled: true             # live 实例才真正发单
```

```bash
# 终端 1：mock（K线回放 + 订单；--kline-start 支持 YYYY-MM-DD 或 epoch 毫秒）
cargo run -p strategy-broker --example mock_server -- \
  --addr 127.0.0.1:9112 --secret dev-secret \
  --kline-dir data/klines/1m --kline-interval-ms 10 --kline-start 2026-11-08

# 终端 2：策略（预热数据目录必须**截断到回放起点之前**，否则预热已消化
# 回放段 bar，引擎视旧 bar 为过期 → 无信号。做法：把目标 symbol 的 CSV
# 截到 <回放起点，放进独立 --data-dir；并封掉启动期自动历史下载，
# 免得它把数据补回"现在"）
STRATEGY_SECRET=dev-secret HTTP_PROXY=http://127.0.0.1:1 HTTPS_PROXY=http://127.0.0.1:1 \
  cargo run -p strategy-app -- run --config /tmp/klive_cfg/BTCUSDT.yaml \
  --symbol BTCUSDT --data-dir /tmp/replay_data --output-dir /tmp/klive_out
```

要点与排障：

- **`STRATEGY_SECRET` 环境变量（含仓库根 `.env` 经 dotenvy 自动加载）会覆盖
  yaml 的 `strategy_secret`**（config.rs §密钥不入库）。本地 mock 联调时在
  启动命令前显式给 `STRATEGY_SECRET=dev-secret`，dotenvy 不覆盖已存在变量。
- 验收日志：mock 打印 `K线回放=…（间隔=…ms/根 起点=…）已装载: …`；app 打印
  `K线源 = trading_service…` 与周期性
  `K线源 trading_service：定稿 bar 投递中（每 60 根记一条）`；
  状态落盘 `state/{YYYYMMDD}/signal.csv` 出现 `filled` 行、
  `history_position.csv` 出现完整开平记录。
- mock 侧 401：核对 `--secret` 与 app 实际生效的密钥（见上条环境变量覆盖）。
- 回放命中 `--kline-start` 之前的 bar 被跳过；同 symbol 断线重订从游标续推，
  不会重播历史（对齐真实服务「只推定稿、不补历史」）。
- mock 装载 14 个 symbol 的 1m CSV 约需数秒（BTCUSDT 单文件约 120 万行），
  启动后先等 banner 出现再起策略，可少几轮连接拒绝退避。

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

# 外部服务 e2e（先按 §2 起 trading-service + exchange-mock，**服务用 binance 挂名变体**；
# 未设变量自动跳过。xili 模拟所不推行情 ⇒ 开仓金额单按契约 §5.1.1 被拒，
# 对应用例运行时打印跳过原因；平仓 quantity 链路全断言必须过）
TRADING_WS_URL=ws://127.0.0.1:9100/ws \
  cargo test -p strategy-broker --test async_gateway_e2e -- --nocapture
cargo audit                                  # 依赖漏洞
ss -ltnp | grep -E '9100|9101|50051|50052'  # 外部服务端口监听确认
```

> SPI live 在场验收照 `docs/exchange-spi-live-acceptance.md` 执行；本节命令仅为
> 进程内自检，不构成验收结论。**口径更新（2026-11-09 收口）**：CLI `--engine`
> 已随旧链退役删除（R4/D1 已在当前工作区执行，尚未 commit），因此该文档原写的
> 「缺省切换前置」已不成立——统一主循环 `ExchangeRunner` 现为**唯一缺省路**，
> 在场验收的性质由「切换前置」变为「对已生效缺省路的事后确认」，且
> `--engine old` 运行时回退线已随 R4 消失（回退手段只剩 git revert）。

## 5. 常见问题

| 现象 | 排查 / 处理 |
|---|---|
| `连接失败` 反复退避 | trading-service 未起或 `ws_url`/端口不符；检查 9100 监听 |
| ack 超时（Retryable） | 目标交易所未注册为 `exchange` 配置同名实例；检查北向 9101 连接与注册日志 |
| 本地拒单 `submit_enabled=false` | paper 模式强制 false 属预期；live 需显式置 true，且**端点必须真实**——占位端点 + false 在 live 下会被拒绝启动（见 §2.1） |
| 本地拒单 `数量非法` | 信号 size=0 且 `default_quantity<=0`；配置正数量或由策略带数量 |
| mock 测试握手 401 | nonce 5 分钟内重放，或签名/密钥不一致；每连接须新 nonce |
| 状态疑似被旧快照覆盖 | 查看 Store recover 日志；Store 盘载只填空缺 id（活行不被磁盘覆盖） |
| `回测区间无 1m 数据` 但 CSV 明明在 | 报错现在会**打印行键与期望 CSV 路径**——先看行键是不是落到了别的市场（如 crypto 标的出现 `SHFE/`）。2026-11-10 前这类错位多由**目录模式跨文件继承**造成（已根治，见 §7.1 末注）；如今剩余原因通常是：该标的在目录里没有自己的文件（会被拒绝启动并列出可用标的）、或数据确实没下载到这个区间 |

## 6. 回退

- 代码/配置：`git checkout <旧提交> -- strategy.config.yaml` 或直接切回旧提交后
  重新 `cargo run`；无数据库迁移，Store CSV 为追加/快照结构，跨版本读取异常时
  将当次 `state/` 目录备份后重跑。
- 协议：trading-service 线协议变更以 `contracts/trading-service.md` 版本号为准；
  客户端不兼容时回退双方到同一版本（已知限制见
  `contracts/trading-service.md` §5.1）。

## 7. 国内期货（CTP）接入

模板对期货只做**回测 + 信号（paper 同口径）**，不直连 CTP；行情与下单均由
trading-service 承担（K 线契约 `contracts/trading-service.md` §8；旧
klines-service 已退役，契约归档 `docs/archive/kline-service.md`）。

### 7.1 配置

> **前置：CU 的 1m 数据要先用 `roll_build` 造**。仓库里的
> `data/klines/1m/SHFE/cn_futures/CU_1m.csv` 是 **gitignore 占位**（可能只有表头），
> 空数据时回测会带行键与期望路径报「区间无 1m 数据」拒跑——这是 fail-fast，不是缺陷。
> 造数（输入为逐合约日 CSV，必需列 `symbol,bob|open_time,open,high,low,close,volume,position`；
> `CU8888` 一类合成代码按"月 ∉ 1..12"规则**自动忽略**，主力连续只由真实月份合约选）：
>
> ```bash
> cargo run -p strategy-data --bin roll_build -- \
>   --in <含 CUYYMM.csv 的目录> --product CU --out /tmp/cu_cont
> cp /tmp/cu_cont/1m/CU_CONT_1m.csv data/klines/1m/SHFE/cn_futures/CU_1m.csv
> ```
>
> 输出为模板 10 列标准格式（`quote_volume/trade_num/active_*` 恒 0，原始输入无此列）；
> **不复权**，跨换月有价格断档（见 §7.1 末与下条警告）。

```bash
# 期货示例（CU，统一端口 strategy.yaml，CTP 时段聚合）
cargo run -p strategy-app -- backtest --config config/tpl_trend_futures/strategy.yaml \
  --start 20260101 --end 20260630 --data-dir data/klines --output-dir reports

cargo run -p strategy-app -- run --config config/tpl_trend_futures/strategy.yaml
```

> ⚠️ klines-service 退役（2026-11-10）后，旧 `services.kline` 节（含 `market`
> 全局字段）整体退役——写了 `run` 即拒启；K 线一律经 trading-service 订阅（§2.3），
> 「行情形态全局单值、crypto 与期货实跑分目录」的旧约束随之消失。

- `config/contracts.yaml`：乘数/最小变动价位/保证金率/开平今手续费/日盘夜盘时段。
  未声明的 symbol 走 crypto 兼容口径（小数数量 + `--fee-rate`）。

> ✅ **目录模式的身份归属已根治（2026-11-10）**：`--config <目录>` 里每个 yaml 的
> **标的身份类字段**（`strategy / timeframe / cash / initial_cash / leverage / exchange / account`）
> 只认三处来源——① 声明该标的的那个文件、② `_global.yaml`、③ 内置默认；
> **不再从同目录的兄弟 symbol 文件继承**（旧实现按"字母序首个非空者胜"提升成目录级默认值，
> 于是混放期货与现货的目录里 `--symbol BTCUSDT` 会拿到 `CU.yaml` 的 `exchange: SHFE`
> ⇒ 行键 `SHFE/crypto_perpetual/BTCUSDT` ⇒ 报「区间无 1m 数据」，实测复现过）。
> 跨文件仍共享的只有**基础设施类**字段：`services / contracts / exchanges / instruments / risk`
> （本就是整目录共用一套），以及已废弃、仅喂告警的 `bar_merge`。
>
> **现在的行为**：
> - 目录里**没有**该标的的文件 ⇒ **拒绝启动**并列出可用标的，不再静默套用别人的交易所。
>   （单文件模式不受此限：没有兄弟文件就没有污染，`--symbol` 复用模板照旧合法。）
> - 标的身份 = **文件名**（`{strategy}/{SYMBOL}.yaml`）。历史 per-symbol 样例曾是
>   `config/tpl_trend_crypto/BTCUSDT.yaml`（crypto）与 `.../CU.yaml`（期货）；当前样例已
>   迁移为 `config/tpl_trend_crypto/strategy.yaml` 与 `config/tpl_trend_futures/strategy.yaml`
>   （分目录，互不干扰）。
> - 文件名不便表达标的时，可在该文件顶层写 `symbol: BTCUSDT` 显式声明身份；
>   与文件名不一致会打 `WARN`（复制模板忘了改时就是这条）。
>   `_global.yaml` 与 `contracts.yaml` 写 `symbol:` 直接硬错——共享文件不属于单个标的。
> - 「区间无 1m 数据」现在会**带出行键与期望 CSV 路径**，身份错位不再伪装成数据缺失。

- `bar_merge: ctp`（per-symbol）：**已废弃字段，写了不生效**。engine 统一链的聚合
  规则由 `CtpExchange::aggregation_policy` 提供（`crates/engine/src/exchanges/ctp/mod.rs:219-221`
  → `SessionWindowPolicy`），时段真源是 `config/contracts.yaml`；
  `crates/app/src/config.rs:48-56` 注释明确「`bar_merge` 是历史字段，不再影响任何
  行为」。字段仍被解析（`app/src/config.rs:41-52`
  `warn_bar_merge_deprecated`——原引 `config.rs:43` 已随 D1a 删除）只为兼容存量 <!-- refcheck:ignore -->
  `CU.yaml` 不报硬错——**写了不生效，不报错**；清退登记于
  `docs/exchange-spi-retirement.md` §5.1 D1 残留。午休/隔夜缺口不切错桶现由
  `SessionWindowPolicy` 保证，与此配置项无关。
- ~~`services.kline.market: futures`~~ **已退役**：K 线统一走 trading-service（§2.3）；
  历史回填 = 本地 1m CSV + `get_klines` 分页（契约 §8.5），服务端只返回实际存在的
  bar，不再有墙钟/交易日历回填窗口的全局形态开关。
- 回测/paper 均执行“开盘后/收盘前 N 分钟禁止开平反手”；paper 与回测共用同一
  `RiskControlConfig` 兜底（`risk:` 顶层字段），且逐分钟权益口径已在
  `strategy_live_mock_e2e` / `strategy_backtest_e2e` 中验收。

### 7.2 主力连续构造（roll_build）

```bash
# 输入为外部原始逐合约 1m CSV（列：symbol,bob,open,high,low,close,volume,position）
cargo run -p strategy-data --bin roll_build -- \
  --in data/klines/output --product CU --out data/klines_roll
# 输出：data/klines_roll/1m/CU_CONT_1m.csv + roll_events.csv
```

口径与限制：
- **因果选主**：D 日收盘持仓量(OI)选出的主力，**次一交易日**生效；首个观测日
  无生效主力、不输出 bar。换月事件 `roll_events.csv` 的 `date` 为生效日。
- 剔除进入交割月的合约（通用规则；各交易所个别规则未覆盖）。
- 不提供复权（`--backadjust ratio` 明确报错）；跨换月存在价格断档，回测跨月
  请自行按需处理或只用单段。
- 交易日分组复用 `ctp_trading_day`：仅按周末回落的近似（无节假日表）。若某
  交易日只有夜盘 bar（节假日伪交易日），工具会打印 warning；请人工核对。

### 7.3 维护提醒（到期复核）

1. ~~`crates/data/src/trading_calendar.rs` 内嵌节假日表~~ **已随 klines-service
   退役删除（2026-11-10）**——模板不再自维护交易日历数据；期货交易日/夜盘口径由
   `config/contracts.yaml` session（回测/paper）与 trading-service 服务端（实盘）承担。
2. `config/contracts.yaml` 费率/保证金为交易所标准快照；期货公司加收部分请
   按账户实际覆盖，并定期核对交易所结算参数。
3. trading-service 的 K 线需支持期货交易日口径（CTP 夜盘 bar 归次日，契约 §8.2
   `trading_day`）；mock 按 UTC 自然日简化，实盘以真服务验收为准
   （docs/exchange-spi-live-acceptance.md）。

## 5. 横截面实盘（PR-5：run --config strategy.yaml）

### 5.1 启动前置

- **单 exchange + 单 account**：一个横截面组 = 一个 actor = 一套 gateway
  （loader 里 `ensure_single_account` 守卫生效；`multi_instance` 仍拒启）。
- **universe 预热**：启动期按 `warmup_days` 逐标的回填 1m（本地 CSV 主 +
  `get_klines` 分页补足）；不足只告警——entry 硬门会拒绝「预热不足」的开仓。
- **mode 解析**：CLI `--mode` > `_global.yaml` 的 `mode:` > 默认
  `paper_trading`；`smoking` 仅属旧单标的演练，横截面拒启。
- **services**：`services.trading` 取自 `_global.yaml`；live + 占位端点
  （`ws_url` 未配真值）**拒启**，paper 启动但高声告警（与单标 `run` 同口径）。
- `run` 分支不接受 `--strategy/--symbol/--timeframe/--cash` 覆盖（universe
  来自 `strategy.yaml`）。

```bash
# paper（本地撮合，无网关）
cargo run -p strategy-app -- run --config config/tpl_xsec_crypto/strategy.yaml \
  --mode paper_trading --data-dir ./data/klines --output-dir ./out
# live（需 _global.yaml 配真实 services.trading；紧急停单用 submit_enabled: false）
cargo run -p strategy-app -- run --config config/tpl_xsec_crypto/strategy.yaml --mode live
```

### 5.2 mock 演练（隔离 cwd；先 dry 再上 live）

在**独立临时目录**跑（避开仓库根 `.env` 与 `config/_global.yaml`；`.env` 里的
`STRATEGY_SECRET` 会覆盖演练 secret，且启动期自动历史下载会用真实归档地址——
给隔离目录配不可达代理让它快速失败即可）：

1. `config/_global.yaml` 写 `services.trading`（mock 端口 + 与 mock 启动参数一致的
   `strategy_secret`；`submit_enabled: true`）。
2. `strategy.yaml` 的 `universe` 至少 2 个标的；`_global.yaml` 的 `mode:` 设 `live`。
3. 起 mock：`cargo run -p strategy-broker --example mock_server -- --addr 127.0.0.1:9100
   --secret <同上> --kline-dir <1m CSV 目录> --kline-interval-ms 40
   --symbol-price BTCUSDT=100 --symbol-price ETHUSDT=50`（1m CSV 文件名
   `<SYMBOL>_1m.csv`）。
4. 观察日志必备行：订阅帧（`subscribe_kline`，`symbols` 数组含全 universe）、
   `nav_source="local_mirror"`、`ack`、`order_event FILLED`、周期 `Reconcile`、
   `Shutdown` 后 flush。进程级验收清单见 PR-5 计划 Task 9。
5. **两个实测坑**（2026-11-11 演练记录）：
   - warmup CSV 必须**截断在回放起点之前**（单独目录），否则预热段与回放段
     同一批 bar 重复喂入聚合层被判旧 → `bars_processed>0` 但零决策轮；
     推荐两段式：warmup=起点前 1440 根、回放段=起点后新 CSV。
   - mock 的回放游标按 `(strategy_name, symbol)` 跨连接记忆：**每次演练重跑
     必须重启 mock**，否则新连接从游标 +1 续推（旧游标已到尾 → 零 bar）。

### 5.3 NAV 口径（local_mirror）

trading-service 契约 v0.3 **无账户权益/净值帧**（`contracts/trading-service.md`
「契约缺帧登记」）。模板侧 live NAV = 本地镜像：成交回报逐笔累计 fee/折算
realized + 镜像 book × 最新 1m 收盘盯市，装配日志明示
`nav_source="local_mirror"`。**不要**把它当券商口径核对出入金/利息；真实权益
属契约演进（另立项）。

### 5.4 panic 断路器（人工放行手册）

- 策略 panic → 该 RunnerGroup 置 **Paused**（日志 `策略 panic，已暂停该
  RunnerGroup` + actor 侧「恢复须重启进程走 recover 监督代」告警）；
  **进程内不会自动放行**，事件泵继续收口但策略调用被 `can_trade()` 门控。
- 恢复 = **重启进程**：装配期 `recover_from_store` 先盘载 Store 行、再把
  `get_positions` 采纳进镜像（remote→local 单向），然后才消费 candle
  （「先账本后消费」）。重启后 `Paused` 清除、验证恢复为 `Running`。
- `Reconcile` 的 drifts 三态读法（`(instrument, kind, detail)`）：
  - `closed`：本地非 Closed 子仓而远程同向已平 → 已收敛 `Closed`（他人平仓
    或 ReduceOnly 拒单的外部平仓路径）；核对成交历史后无需动作。
  - `qty_corrected`：数量按远程修正（raw 字面量不伪造）；若频繁出现，核对
    partial fill / 手工干预记录。
  - `legacy_remote_only`：远程有仓但本组无对应子仓（非本策略开的仓）——
    **仅审计**，不入 NAV/组合度量；确认归属后再人工处理。
- 在途看门狗：`POLL_GRACE_SECS`（60s）后每 `POLL_INTERVAL_SECS`（60s）主动
  `get_order` 查询；重启/重连先查后发（Query/Resubmit 由内核状态机决定）。

### 5.5 多组同进程（配置集 `run --config <set_dir>`）

1. **目录布局**：`<set_dir>` 无顶层 `strategy.yaml`，每个直接子目录一组；
   `_global.yaml` 必须放**配置集根目录**——`global_for` 只向组目录及其父目录
   探测，而 `set/<group>/strategy.yaml` 的父目录才是 set 根。示例：
   ```
   set/_global.yaml            # 共享 services.trading / mode / flush 策略
   set/momentum_a/strategy.yaml
   set/momentum_b/strategy.yaml
   ```
2. **live 启动自检**：每组一条 `横截面 Actor 已启动 strategy_id=... instruments=...`；
   全组**共享一条交易连接**（同 `strategy_type` 的订阅帧合并为 symbol 并集），
   `OrderRouter` 精确 order_id 优先、同类型广播按各组 `owns_signal` 过滤。
3. **拒启对照**（任一命中即启动失败，进程不进入 bootstrap）：
   - 组间 universe 重叠（**live**；paper 允许重叠，账本按 `strategy_id` scope 隔离）
   - `strategy_id` 重复
   - 各组 account / services / 解析后 `run_mode` / flush/prune 不一致
   - `backtest` 指向配置集：明确拒绝，提示逐组 `--config <组目录>/strategy.yaml`
4. **状态文件**：共用 `out/state/<day>/` 三件套；`strategy_portfolio` 行 id 即
   `strategy_id`；`sub_position` 行在 `payload_json` 内按 `strategy_id`/`instrument`
   区分归属组。NAV 仍是**每组独立**的 `nav_source=local_mirror`，同一 broker
   账户保证金共享、不做跨组权益合并（见 §5.3）。
5. **panic**：单组 Paused 并一次性告警，其他组继续运行；恢复仍须**整进程重启**
   走 `recover(after_panic=true)` 监督代（见 §5.4）。
6. **关停**：ctrl-c 后逐组 `shutdown`，每组 8s 收口上限，全部结束后最后 abort
   K 线 forwarder；任一组超时被强制 abort 时，其余组与共享客户端仍按序收口。
