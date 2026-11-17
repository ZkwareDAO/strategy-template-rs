# trading-service 接口契约（策略项目侧）

> 版本：v0.3（2026-11-10，对齐 trading_service_rust `docs/策略端ws接口文档.md`：
> K 线订阅 §10 转正——`kline_1m_batch` 统一形态、CTP 品种前缀/主力、
> `get_klines` 历史查询；klines-service 已退役，见 `docs/archive/kline-service.md`）
> 旧版：v0.2（2026-09-17，对齐真实 trading-service 实现）
> 事实源：`~/workspace/trading_service_rust` 的
> `trading-core/src/protocol.rs`（SouthCommand / ClientEvent）、
> `docs/策略端ws接口文档.md`（南向协议权威文档）与
> `trading-service/src/{south.rs,service.rs,hub.rs}`。
> Rust 侧可执行规格：`crates/broker/src/trading_service.rs`（客户端）、
> `crates/broker/src/mock.rs`（鉴权 + 成交 + K 线回放/历史 mock）、
> `crates/broker/tests/async_gateway_e2e.rs`（端到端用例）。

## 1. 连接

- WebSocket，基础 URL 由配置 `services.trading.ws_url` 给出（真实服务默认
  `ws://127.0.0.1:9100/ws`，路径 `/` 与 `/ws` 等价）；文本帧、JSON、UTF-8。
- **一条连接绑定一个策略身份**：query 携带 `strategy_name`，服务端按此注册
  回调路由；strategy-rs 按策略类型各建一条连接（同类型多符号复用），
  各自独立指数退避重连（1→2→4…≤`reconnect_backoff_max_secs`）。
- 握手鉴权（《服务端与客户端连接.md》方案；真实服务当前只读 `strategy_name`，
  额外参数被忽略，客户端已前向实现，服务端开启强校验后无需改动）：

  ```
  ws://<host>/ws?strategy_name=<urlenc>&nonce=<64hex>&signature=<64hex>
  ```

  - `nonce`：每次（重）连接生成的 32 字节随机数 hex；
  - `signature = hex(HMAC_SHA256(SECRET_KEY, strategy_name + nonce))`；
  - 密钥来自配置 `services.trading.strategy_secret`，不上线传输。
- 连接建立后服务端可能先推 `welcome` 等非业务帧，客户端忽略。

## 2. 逻辑通道

以帧内 `option`（请求）/ `type`（回调）字段区分：

| 方向 | 帧 | 说明 |
|---|---|---|
| 出站 | `option: "place_order"` | 开仓帧（order_cash+leverage）/ 平仓帧（quantity+reduce_only） |
| 出站 | `option: "cancel_order"` | 撤单（按 client_order_id） |
| 出站 | `option: "get_order"` | 订单快照查询（按 signal_id；活动/当日终态/跨日历史服务端三级查询） |
| 出站 | `option: "get_positions"` | 持仓查询（instrument+side 精确筛选；每分钟远程对账） |
| 出站 | `option: "subscribe_kline"`（别名 `subscribe_klines`） | K 线订阅（exchange+symbols+interval=1m，见 §8.1） |
| 出站 | `option: "unsubscribe_kline"`（别名 `unsubscribe_klines`） | K 线退订（symbols 缺省=全停；§8.1） |
| 出站 | `option: "get_klines"` | 历史 1m K 线查询（服务端本地留存；应答 `kline_history`，§8.5） |
| 入站 | `type: "ack"` | place_order 的受理应答（status=NEW） |
| 入站 | `type: "order"` | get_order 应答（订单快照） |
| 入站 | `type: "order_event"` | 服务端主动订单状态推送（PARTIAL/FILLED/…） |
| 入站 | `type: "error"` | 拒绝/错误（status=REJECTED） |
| 入站 | `type: "positions"` | get_positions 应答 |
| 入站 | `type: "kline_1m_batch"` | 定稿 K 线合帧推送（同所同分钟多标的数组；§8.2） |
| 入站 | `type: "kline_1m"` | 定稿 K 线单根（兼容形态；服务端与 mock 现只发 batch） |
| 入站 | `type: "kline_history"` | get_klines 应答（§8.5） |

断线重连后客户端先查后发：从未收到 ack 的信号重发（幂等），已 ack 在途单经
`get_order` 对账修正状态，不靠 order_event 重放。

> **契约缺帧登记（v0.3）**：本协议**无账户权益/净值查询帧**（无 `get_account`、
> `equity` 类应答）。模板侧 live NAV 因此取**本地镜像**（`nav_source=local_mirror`：
> 成交回报逐笔累计 fee/折算 realized + 镜像 book × 最新 1m 收盘盯市），
> **明示不冒充券商口径**；真实 broker 权益接入属契约演进，另立项
> （见 PR-5 计划「明确不做」清单与 `docs/RUNBOOK.md` 横截面实节）。

## 3. 下单（place_order）

策略身份 `strategy_name` **不入帧**：由 WS 握手绑定。开仓与平仓为两种帧形。

### 3.1 开仓帧

```json
{
  "option": "place_order",
  "exchange": "binance",
  "account": "test_binance",
  "instrument": "BTCUSDT",
  "instrument_type": "futures",
  "side": "long",
  "order_type": "market",
  "order_cash": "100",
  "leverage": 3,
  "signal_id": "sig_xxx"
}
```

| 字段 | 取值 | 说明 |
|---|---|---|
| `exchange` | string | 目标交易所名，服务端 registry 键 |
| `account` | string | 账户名 |
| `instrument` | string | 交易所原生标的 |
| `instrument_type` | `futures` | 标的类型（金额单仅支持合约） |
| `side` | `long` / `short` | 开仓方向（服务端兼容 buy/sell） |
| `order_type` | `market` | 固定市价单 |
| `order_cash` | string | 每单名义金额（quote 币）；服务端按 `order_cash / 市价` 换算数量 |
| `leverage` | number | 杠杆倍数（>1 时服务端先幂等 set_leverage） |
| `signal_id` | string | 策略信号 ID；服务端在 ack/order_event 中原样带回 |

- `exchange/account/order_cash/leverage` 均取自该 symbol 的策略配置
  （`config/{strategy}/{SYMBOL}.yaml` 顶层字段；缺省回落 `services.trading`）。
- 服务端换算：参考价优先共享 QuoteTable（binance mark，mark>mid>last），
  行情缺失走交易所 REST ticker 兜底，均无（xili 不推行情）→ REJECTED；
  数量按市价步长向下对齐。

### 3.2 平仓帧

```json
{
  "option": "place_order",
  "exchange": "binance",
  "account": "test_binance",
  "instrument": "BTCUSDT",
  "instrument_type": "futures",
  "side": "short",
  "order_type": "market",
  "quantity": "0.01",
  "reduce_only": true,
  "signal_id": "sig_xxx"
}
```

| 字段 | 取值 | 说明 |
|---|---|---|
| `side` | `long` / `short` | 与持仓方向相反（持 long 发 short，反之亦然） |
| `quantity` | string | **必须逐字等于开仓 FILLED 回报的 filled_quantity**，不得修改 |
| `reduce_only` | `true` | 恒 true（只减仓） |

- 平仓前必须存在仓位；无仓位/无精确成交量时不发帧（fail-closed）。
- `exchange/account` 同样取自 `{SYMBOL}.yaml`。

## 4. 应答与订单状态流

受理应答（交易所 REST 受理成功后回调）：

```json
{
  "signal_id": "sig_xxx",
  "type": "ack",
  "status": "NEW",
  "client_order_id": "0x9f2c…",
  "exchange_order_id": "XILI-1",
  "exchange": "binance",
  "strategy_name": "sar_supertrend",
  "instrument": "BTCUSDT",
  "side": "BUY",
  "order_type": "market",
  "order_price": 0,
  "order_quantity": 0.01,
  "reduce_only": false,
  "filled_price": 0,
  "filled_quantity": 0,
  "message": null,
  "positions": null
}
```

成交/状态推送（交易所回报后回调，可多次 PARTIAL，最终 FILLED）：

```json
{ "signal_id": "sig_xxx", "type": "order_event", "status": "FILLED",
  "client_order_id": "0x9f2c…", "exchange_order_id": "XILI-1",
  "reduce_only": false,
  "filled_price": 80000.5, "filled_quantity": 0.01 }
```

- 客户端按 `signal_id` 关联本地信号，落账
  `status/exchange_order_id/filled_price/filled_quantity`（成交量同时保存原始
  字面量，平仓帧 quantity 逐字取用）；仅 **FILLED** 生成仓位，其余状态不生成。

拒绝：

```json
{ "type": "error", "status": "REJECTED", "client_order_id": null,
  "strategy_name": "sar_snt3", "message": "quantity 必须为正数" }
```

服务端状态（大写）→ 客户端 `OrderState` 映射：

| 服务端 status | OrderState | submit/处理语义 |
|---|---|---|
| `NEW` | Ack | submit 成功，返回 client_order_id |
| `PLACING` | Sent | 跳过（不产生状态转移） |
| `PARTIAL` | PartiallyFilled | 推进累计成交量，不入仓位 |
| `FILLED` | Filled | 成交，产出 FillEvent 入仓位管理 |
| `CANCELED` | Canceled | 终态 |
| `REJECTED` / `EXPIRED` | Rejected | **永久失败**（Rejected，不重试） |
| `UNKNOWN` / 缺失 / 未识别 | — | submit 判**可重试**（ack fail-closed）；order_event 跳过该帧 |

- `filled_quantity` / `filled_price` 为 JSON 数字（客户端同时兼容数字字符串）。
- 数值字段缺失按 0；未知帧类型（welcome/positions 等）忽略不报错。
- 请求-应答配对：submit 在途按 `signal_id` 精确匹配应答，服务端未回显
  signal_id 时退化为 FIFO（同连接 submit 逐条 await，同时仅一个在途）。
- 超时（`request_timeout_ms`）/连接断开 = 可重试失败，oneshot 随连接销毁失败，
  由信号状态机在下次对账时重发。

## 5. 撤单与查询

```json
{ "option": "cancel_order", "strategy_name": "sar_snt3",
  "client_order_id": "0x9f2c…" }
```

- 成功回 `type:"ack", status:"CANCELED"`；订单不存在/已终结回 `error`。

```json
{ "option": "get_order", "exchange": "binance", "account": "test_account",
  "signal_id": "sig_xxx" }
```

- 按 `signal_id` 查订单快照，应答 `type:"order"`；服务端自动限定在握手策略名下
  （跨策略查询回 `error`）。活动/当日终态/跨日历史由服务端三级查询，对客户端透明。
- 用途：重连/重启对账（先查后发）、在途单卡单轮询。

```json
{ "option": "get_positions", "exchange": "binance", "account": "test_account",
  "instrument_type": "futures", "instrument": "BTCUSDT", "side": "long" }
```

- 应答 `type:"positions", status:"OK", positions:[…]`；`instrument`+`side`
  精确筛选，无仓返回空数组（不是错误）。
- 用途：每分钟远程仓位对账（远程无匹配仓位 → 本地 external_close，fail-open）。

## 5.1 已知限制

1. **本地模拟器（xili-exchange）不支持金额单**：模拟器不推送行情，开仓
   `order_cash` 换算无可用价 → REJECTED；`quantity` 路径（平仓帧）不受影响。
2. **无 order_event 重放**：冷启动/重连期间错过的推送无法追回，靠 `get_order`
   主动对账兜底。
3. 市价单（平仓帧）价格由交易所撮合。
4. 真实服务当前不强制 TLS（生产应使用 `wss://`）；客户端经 MaybeTlsStream
   天然支持 wss。

## 6. 安全约定

- `submit_enabled=false`（默认）时客户端本地拒绝一切下单；
  `paper_trading` 模式在装配层强制 false（双保险）。
- `default_quantity<=0` 且信号未带数量时本地拒绝，防止误下零数量真单。
- HMAC 密钥与签名不得出现在日志中；握手失败（缺参数/nonce 重放/签名不符）
  在 mock 中直接 HTTP 401，不升级 WebSocket、不分配连接资源。
- nonce 防重放窗口 5 分钟（mock 实现；真实服务鉴权上线后以其实现为准）。

## 7. 期货语义（CTP 品种）

模板侧对国内期货只负责**发信号**；整数手换算、保证金与平今/平昨选择归服务端。
本节为期货接入的对签口径（与 `config/contracts.yaml` 头注一致）：

1. **开仓帧**：仍只发送 `order_cash + leverage`（§3.1），不发送手数。
   服务端按 `instrument` 对应合约的乘数/保证金率/手续费换算整数手；
   模板侧 `config/contracts.yaml` 的同名字段仅用于**回测/paper 撮合**，两者
   必须保持同一费率口径（否则回测与实盘结果不可比）。
2. **平仓帧**：`quantity` 为**整数手**（字符串），取模板持仓行的
   `remaining_size_raw`；`reduce_only=true`。
3. **平今/平昨**：由服务端按 CTP 交易日决定（同一交易日开平 = 平今优先，
   具体规则以服务端实现为准）；模板侧不发送 offset 字段。
4. **合约乘数与盈亏**：模板回测按 `price_diff × multiplier × lots` 结算；
   服务端回执的成交价/数量直接进入模板仓位与历史仓，不做二次换算。
5. **时段约束**：模板在 `contracts.yaml` 声明的日盘/夜盘开盘后与收盘前 N 分钟
   内不产生开/平/反手动作（回测与 paper 同口径）；live 服务端可另行拒单，
   模板侧不得假设服务端会替它执行该约束。
6. **未声明合约**：`contracts.yaml` 未收录的 symbol 一律按 crypto 兼容口径
   （小数数量 + `--fee-rate`），不得混用期货字段。

## 8. K线推送（subscribe_kline / kline_1m_batch / get_klines）

K 线**唯一**通道：经 trading-service 交易连接订阅（协议 §10，权威定义见
trading_service_rust `docs/策略端ws接口文档.md`；klines-service 已退役，
旧 `services.kline` 配置节为 tombstone——写了 run 拒启）。
预热/补缺 = 本地 1m CSV 为主 + `get_klines`（§8.5）分页补足；实时只吃本节推送。

### 8.1 订阅命令（客户端 → 服务端，无请求-应答关联）

```json
{"option":"subscribe_kline","exchange":"binance","instrument_type":"futures",
 "symbols":["BTCUSDT"],"interval":"1m"}
```

- `exchange` 必填（精确名）；`symbols`：具体合约/标的（CTP 为 `cu2512`
  形态），亦支持 **CTP 品种前缀**（`cu`）——服务端展开为该品种在交易合约并
  **只推当前主力**，换月随帧内 `symbol` 变化（选举需连续 N 个有数据交易日胜出，
  默认 3；月份只进不退）。别名 `subscribe_klines`；单标的可写 `symbol`/`instrument`。
- `interval` 仅支持 `1m`（缺省/空 = 1m；其余周期拒绝）。
- 同连接重复订阅幂等；多连接同标的服务端只向交易所订一份（引用计数）。
- 订阅是**连接级**的：断线即失效（服务端自动清退该连接全部订阅），重连必须
  重发（模板侧 `TradingServiceClient` 按 **(策略类型, exchange)** 分组登记，
  重连逐组重发——同策略跨交易所订阅互不覆盖）。
- `unsubscribe_kline` 同形（`symbols` 缺省 = 全停）。

### 8.2 推送帧（服务端 → 客户端）

**统一只下发 `kline_1m_batch`**：同交易所、同分钟定稿的多根 bar 聚为一帧数组，
组内按 `(symbol, start_time)` 升序；该分钟仅一根时数组长度也为 1。单根
`{"type":"kline_1m","kline":{...}}` 为协议兼容形态（客户端双解析保留）。
bar 载荷（12 个公共字段 + 各所 extras 平铺；**客户端勿开 deny_unknown_fields**）：

| 字段 | 类型 | 说明 |
|---|---|---|
| venue | string | binance/ctp/… |
| symbol | string | 标的；CTP 如实为具体合约 |
| interval | string | 恒 `1m` |
| trading_day | u32 | YYYYMMDD（CTP 夜盘归次日；mock 简化按 UTC 日） |
| start_time / end_time | i64 | 桶起止（UTC 毫秒，左闭右开） |
| open/high/low/close | decimal（JSON 字符串） | OHLC |
| volume | i64 | 周期成交量（增量） |
| quote_volume | decimal | 周期成交额（增量） |
| trade_num / active_buy_volume / active_buy_quote_volume | binance extras | 其余所可缺省 |
| exchange_id / open_interest | ctp extras | 币圈所无 |

**只推定稿 bar，不补历史**（断开期间定稿的 bar 不补发；历史走 §8.5
get_klines 或本地 CSV）；K线帧不携带策略身份（广播给订阅连接）；
CTP 由墙钟在分钟边界定稿，空窗不补。

### 8.3 模板侧消费（`TradingServiceClient`）

- 定稿 bar 走**独立 broadcast 通道**（`kline_updates()`），不占订单事件
  mpsc——高频回放不反压订单路径；消费落后 `Lagged` 由策略侧 backfill 自愈。
  多策略类型订阅同 symbol 时各连接各推一份，`ts_kline` router 按
  (venue, symbol) 单调水位去重，下游每根只投一次（Actor `last_handed` 为二道闸）。
- `TradingServiceKlineClient`（`crates/app/src/ts_kline.rs`）适配为
  `KlineClient` 端口：`attach` 扇出（扇出口径错配 **warn+计数**——CTP
  品种前缀订阅时帧内 symbol 为主力合约，与订阅名不同即丢弃可见化）；
  `backfill` = 本地 1m CSV 打底 → 尾部陈旧先 `get_klines` 正向补齐到 now
  → 不足再向更早翻页；warmup 侧 `warmup::load_warmup` 合并去重并把远端
  新 bar 追加回 CSV（重启后本地尾部保持与 now 对齐）。
- 订单成交价与 K线**无耦合**：成交价由服务端撮合结果决定（mock 见 8.4）。

### 8.4 mock 的 K线回放（本地联调用，非生产）

`mock_server --kline-dir data/klines/1m [--kline-interval-ms 100]
[--kline-start 2025-12-01|<epoch_ms>]`：

- 启动时递归装载 `*_1m.csv`（`timestamp,open,high,low,close,volume,quote_volume,
  trade_num,active_buy_volume,active_buy_quote_volume`，UTC 时间戳）；坏文件跳过。
- 收到 `subscribe_kline(s)` 后按 `--kline-interval-ms` 节奏推
  `kline_1m_batch` 合帧（同 tick 多 symbol 聚一帧数组、组内按
  (symbol,start_time) 升序；单根亦发 batch——与生产出形一致）；轮次耗尽即
  静默。命令面与真实服务对齐：别名 `subscribe_klines`/`unsubscribe_klines`、
  `exchange` 必填（缺失回 error 点名）、数据缺失标的回 error 点名。
- **游标续推**：同 `(strategy_name, symbol)` 重订从上次位置 +1 继续，不回放历史。
- **成交价自动跟随最新回放收盘**（显式 `--symbol-price` 覆盖仍优先）——
  回放驱动的信号按回放市价成交，PnL 才有意义。
- `get_klines`（§8.5）已实现：只读 `--kline-dir` 装载的本地 CSV（镜像真实
  服务「本地留存，不调交易所」语义），支持缺省最近 N 根 / `end_time` 排他
  翻页 / 无数据空 OK / 参数非法 error。
- 已知口径差异：`trading_day` 按 UTC 自然日简化（真实 CTP 夜盘归属由服务端
  按交易日历决定）；品种前缀订阅不做主力选举模拟——帧内 symbol 恒为订阅
  请求名（真实服务前缀订阅只推当前主力、换月随帧内 symbol 变化，该口径差
  以真服务验收覆盖；模板侧扇出已 warn+计数兜底，见 §8.3）。

### 8.5 历史查询 get_klines（请求-应答，协议 §10.4）

```json
{"option":"get_klines","exchange":"binance","symbol":"BTCUSDT",
 "start_time":1761696000000,"end_time":1761699600000,"limit":300}
```

- 只读服务端本地留存（内存保留窗 + 归档 CSV），**不调交易所**；无数据回
  空数组（不是错误）；`limit` 缺省 300、超 1500 服务端截断。
- `start_time`（含）/`end_time`（不含）UTC 毫秒，均可省：缺 `start_time` =
  取窗口内**最近** `limit` 根（升序返回）；给了 `start_time` = 窗口内最早
  `limit` 根（正向翻页）。
- 应答 `kline_history` 帧：`klines[]` 元素与 §8.2 bar 完全同形；顶层带
  `exchange`/`symbol`（客户端关联键）/`has_more`/实际生效的 `start_time`/
  `end_time`（空结果且未给区间为 null）。模板侧续翻步长 = 本页最早 bar 的
  `start_time` 作下页 `end_time`。
- 参数非法（缺 `exchange`/`symbol`、`start_time >= end_time`、非 1m 的
  `interval`）回 error 帧。
- 模板侧实现：`TradingServiceClient::get_klines`（oneshot 关联，symbol
  回声精确 → FIFO 兜底；超时/断线 Retryable）；
  `TradingServiceKlineClient::backfill` 已接入（本地 1m CSV 打底；尾部陈旧
  先以 `start_time = 本地末根 + 1m` 正向补齐到 now，仍不足再向更早翻页；
  两路皆空才报错）。
