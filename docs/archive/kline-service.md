# klines-service 接口契约（策略项目侧）

> ⚠️ **已退役（2026-11-10）**：kline-service 已并入 trading-service，K 线成为其一个接口
> （同端点、同 HMAC 鉴权、同帧族）。语义基线（1m-only、`is_final`、币安风格 12 列、
> 期货交易日窗口）由 `docs/design-package/04-trading-service接口设计.md` **§6** 继承；
> 回填主路径改为 WS `get_klines` 帧，HTTP `GET /api/v1/klines` 仅作迁移期兼容。
> 本契约仅作历史语义参考，不再演进。

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

## 5. 期货形态

适用于 CTP 期货品种（上期所 / 大商所 / 郑商所）。加密（24/7）形态的
第 1–4 节不变；期货形态仅在以下四点与之不同，通过配置开启。

### 5.1 symbol 规范

- 品种码：`CU`（沪铜）、`RB`（螺纹）等，与 `config/contracts.yaml` 中的键一致。
- 合约码：`CU2601`（品种码 + 年月）。K 线按合约码分发与回填，`symbol` 取合约码。
- 主连（连续合约）：不在模板内拼接。由上游服务直接产出主连序列，或由
  `roll_build` 工具按换月规则生成；模板侧只消费其产出的合约码 / 主连 K 线，
  不做换月拼接。

### 5.2 交易时段缺口与缺 bar 语义

- 交易时段内（如日盘 09:00–10:15、夜盘 21:00–23:00）应连续出 1m bar；
  时段内的缺 bar 由上游负责保证连续（补齐或标注），模板不做额外补缺推断。
- 午休（11:30–13:30）、日盘与夜盘之间、隔夜与节假日的空档属于**正常休市**，
  缺省不算断点：不触发补缺、不计入缺 bar、不打断指标与聚合的连续性。

### 5.3 配置示例

```yaml
services:
  kline:
    backfill_url: https://your-kline-service.example.com
    ws_url: wss://your-kline-service.example.com/ws/klines
    market: futures   # 缺省 crypto（墙钟回填窗口）；futures 启用交易日历回填窗口
```

`market` 取值 `crypto`（缺省）或 `futures`，未知取值视为配置错误。

### 5.4 回填窗口：交易日口径

期货形态下，回填窗口按**交易日**而非墙钟反推（节假日与周末不计入）：

- 周期分钟数 × 请求根数 按 240 分钟 / 交易日向上折算为交易日数 `n`（至少 1，
  仅日盘折算，夜盘未计入，故窗口偏宽，由请求 `limit` 截断）。
- `startTime` = 北京时间「今日往前数第 n 个交易日」当日 00:00 的毫秒 epoch；
  `endTime` = 当前时刻，与加密形态一致。
- 日历覆盖 2024-01..2027-12；窗口越出覆盖范围时告警一次，并按周末回落继续
  （不 panic，不静默信任）。

### 5.5 参考实现职责（列兼容）

- 上游回填返回**币安风格 12 列**裸二维数组（位序见第 1 节）。
- 模板客户端只读取 `[0]`–`[5]`（必需）与 `[7]`–`[10]`（可选，缺省为 0）；
  行长不足 6 列丢弃。因此**追加第 13 列及之后的列会被忽略、可正常解析**，
  12 列与 13 列均兼容。
- 列序与字段语义由上游服务保证；模板不做列序推断，列序变更须同步修订本契约。
