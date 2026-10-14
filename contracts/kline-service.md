# klines-service 接口契约（策略项目侧）

> 版本：v0.2（2026-09-17，对齐 zkwarefr 远程服务与
> cta-strategy-code/data_manager 参考实现）
> Rust 侧可执行规格：`crates/data/src/remote_client.rs`（客户端）、
> 其内联单测与 `crates/data/tests/zkware_live.rs`（手动真实服务探针，
> `--ignored` 运行）。

- 默认端点：`https://zkwarefr.zkware.cn`（HTTP）、
  `wss://zkwarefr.zkware.cn/ws/klines`（WS），配置在 `services.kline`。
- 服务只提供 **1m** K 线；大周期由策略侧 `Feed` 本地聚合。

## 1. 历史回填（HTTP）

时间范围查询：

```
GET {root}/api/v1/klines?symbol=BTCUSDT&interval=1m&limit=1500&startTime=<ms>&endTime=<ms>
```

| 参数 | 说明 |
|---|---|
| `symbol` | 交易对（大写） |
| `interval` | `1m`（参考实现另用 `15m/1h/4h/1d`，本项目只取 1m） |
| `limit` | 单次上限（参考实现 1500） |
| `startTime` / `endTime` | 毫秒 epoch；Rust 客户端按“最近 N 根”用墙钟反推窗口 |

响应：币安风格**裸二维数组**（亦兼容 `{"data":[...]}` 包裹），升序：

```json
[
  [1789636860000,"76700.00","76705.50","76677.00","76685.80","74.266",
   1789636919999,"5695878.37",1449,"39.003","2991311.07","0"]
]
```

位序：`[0]openTime(ms) [1]open [2]high [3]low [4]close [5]volume
[6]closeTime [7]quoteVolume [8]count [9]takerBuyBase [10]takerBuyQuote [11]-`。
价格为数字字符串（客户端容忍数字/字符串，非法值归零，时间戳无法解析的行丢弃）。

另有单日下载 `POST {root}/api/v1/klines/daily`，body
`{"symbol":"BTCUSDT","day":"YYYY-MM-DD"}`（参考实现用于离线导入；本项目
`download` 命令目前走币安公共归档，不依赖此端点）。

## 2. 实时订阅（WebSocket）

URL：`{ws_url}`（如 `wss://zkwarefr.zkware.cn/ws/klines`），文本帧 JSON。

连接建立后**必须显式发送订阅帧**：

```json
{ "action": "subscribe", "symbols": ["BTCUSDT"] }
```

- 字段为 `action`（非 `type`），`symbols` 是数组（批量多 symbol）；
  取消订阅 `{"action":"unsubscribe","symbols":[...]}`。
- 服务端先回确认：`{"type":"subscribed","symbols":[...]}`（客户端忽略非 kline 帧）。
- 推送：

```json
{
  "type": "kline",
  "symbol": "BTCUSDT",
  "data": {
    "symbol": "BTCUSDT",
    "start_time": 1789636860000,
    "open": "76700.00", "high": "76705.50", "low": "76677.00",
    "close": "76685.80", "volume": "74.266",
    "quote_volume": "5695878.37", "trade_num": 1449,
    "active_buy_volume": "39.003", "active_buy_quote_volume": "2991311.07",
    "is_final": true, "interval": "1m"
  }
}
```

- 时间字段 `start_time` 为**毫秒 epoch**（兼容字符串）；
- 完结标记 `is_final`：`true`=已闭合 K 线（驱动策略，不可丢，通道满背压），
  `false`=未闭合快照（可丢弃最新一根，不进指标/聚合）；缺省按闭合处理；
- symbol 取 `data.symbol`，缺省回落外层 `symbol`。

## 3. 断线重连

- 客户端指数退避：1→2→4…≤`reconnect_backoff_max_secs`（默认 120s），无限重连。
- 每次（重）连成功先在事件流产生 `Reconnected`，并重新发送订阅帧；
  上层据此重新 HTTP 回填窗口补缺（只进聚合/缓存，不重放历史信号）。
- 闭合 K 线背压等待、未闭合快照满则丢弃，两类事件共用一条有序通道。

## 4. 已知现状（2026-09-17 实测）

- HTTP `/api/v1/klines` 正常返回数据（墙钟反推窗口，返回最近 N 根闭合 1m）。
- WS 全链路已实测：连接 → 回 `type:subscribed` 确认 → 分钟闭合时推送
  `type:kline`（`is_final:true`）；Rust 探针正确解析为 Closed 事件。
  行情可能间歇性中断（某段时间只有确认无推送，分钟边界后恢复），客户端
  重连后经 HTTP 回填补缺。
- 无鉴权头/签名；无服务端心跳数据要求（WebSocket 协议层 ping/pong）。
