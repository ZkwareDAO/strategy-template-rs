# trading-service 接口契约（策略项目侧）

> 版本：v0.2（2026-09-17，对齐真实 trading-service 实现）
> 事实源：`~/workspace/trading_service_rs` 的
> `trading-core/src/protocol.rs`（SouthCommand / ClientEvent）与
> `trading-service/src/{south.rs,service.rs,hub.rs}`。
> Rust 侧可执行规格：`crates/broker/src/trading_service.rs`（客户端）、
> `crates/broker/src/mock.rs`（鉴权 + 成交模拟）、
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
| 入站 | `type: "ack"` | place_order 的受理应答（status=NEW） |
| 入站 | `type: "order"` | get_order 应答（订单快照） |
| 入站 | `type: "order_event"` | 服务端主动订单状态推送（PARTIAL/FILLED/…） |
| 入站 | `type: "error"` | 拒绝/错误（status=REJECTED） |
| 入站 | `type: "positions"` | get_positions 应答 |

断线重连后客户端先查后发：从未收到 ack 的信号重发（幂等），已 ack 在途单经
`get_order` 对账修正状态，不靠 order_event 重放。

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
{ "option": "get_order", "exchange": "binance", "account": "test_qpw",
  "signal_id": "sig_xxx" }
```

- 按 `signal_id` 查订单快照，应答 `type:"order"`；服务端自动限定在握手策略名下
  （跨策略查询回 `error`）。活动/当日终态/跨日历史由服务端三级查询，对客户端透明。
- 用途：重连/重启对账（先查后发）、在途单卡单轮询。

```json
{ "option": "get_positions", "exchange": "binance", "account": "test_qpw",
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
