# 代码审查报告：性能与安全（2026-11-16）

范围：crates/{broker, store, data, engine, app}，约 6.9 万行 Rust。
审查方式：四路并行深审（broker 签名与传输 / store+data 持久化与下载 / engine 事件循环 / app 配置与报告）。

---

## 一、安全（按严重度）

### High

1. **报告目录落盘含密钥的完整配置** — `crates/app/src/report.rs:455-458`
   `fs::write(run_dir/"config.yaml", yaml)` 把 `./config` 目录合并后的原始 YAML 原样写进报告（`main.rs:172-204` 的 `read_config_yaml` 会整目录拼接，含 `_global.yaml` 中的真实 `strategy_secret`）。报告是对外分享产物，密钥随之分发。
   修复：落盘前脱敏 `services.trading.strategy_secret` → `"***"`，或只写"生效参数摘要"。

2. **空 HMAC 密钥可启动实盘** — `crates/broker/src/trading_service.rs:64`、`crates/app/src/config.rs:213`
   `strategy_secret` 默认空串，全仓无非空/占位符校验；`submit_enabled: true` 不与密钥联动。空 key 的 HMAC 依然合法 → 鉴权形同虚设。
   修复：live 模式且 `submit_enabled` 时 fail-fast：拒绝空串与 `REPLACE_WITH_*` 占位值。

3. **跨主机明文 ws:// + 签名放 URL query** — `trading_service.rs:60/648-654`、`broker/src/auth.rs:49-55`
   实配 `ws://192.168.1.12:9100/ws` 明文链路，签名（唯一凭证）出现在 URL query：可被嗅探重放（nonce TTL 5min），MITM 可注入伪造 `FILLED`；tungstenite trace 日志/反向代理 access log 都会记录完整请求行。
   修复：非 loopback 的 `ws://` 拒启动；签名迁到握手 Header（`Authorization: HMAC name:nonce:sig`）。

4. **平仓数量直接透传交易所原始字符串** — `trading_service.rs:379-391` + `broker/src/wire.rs:184-195`
   `size_raw` 来自交易所 `filled_quantity` 原始字符串，不经数字校验整串塞进 `PlaceOrderClose.quantity` 发出。`"1,000"`、`"1e999"`、`"-5"` 全部照发——唯一能把脏数据变成真实委托的路径。
   修复：`size_raw` 必须能 `parse::<Decimal>()` 且有限、>0，否则 rejected。

5. **下载无体积上限 + zip 炸弹** — `crates/data/src/archive_download.rs:540`（`r.bytes()` 全量进内存）、`:212-216`（zip 条目 `read_to_string` 无 `take()` 上限）
   被劫持服务器/恶意归档可撑爆内存。修复：流式读取 + `MAX_DOWNLOAD_BYTES`；zip 条目加 `take(512MB)`。

6. **外部时间戳直接 `.unwrap()` → 远端数据可打崩进程** — `archive_download.rs:229`（归档 CSV open_time）、`:294`（fapi JSON open_time）
   越界毫秒值 → `timestamp_millis_opt` 返回 None → panic。同类问题：`aggregate.rs:17`、`kline.rs:95-99`、`ts_kline.rs:334-338`（非法 start_ms 被静默替换成"当前时间"，脏数据伪装合法）。
   修复：全部 `.ok()` 过滤并计数告警。

### Medium

7. **路径注入** — `data/src/csv_store.rs:37-48`：`symbol`/`venue_key` 直接拼进文件路径，`symbol: ../../../tmp/evil` 可覆盖 data_dir 外文件。同型问题：`archive_download.rs:166-180` 把 symbol 拼进 URL。修复：白名单 `^[A-Z0-9._-]{1,32}$` + 断言最终路径在 base 内。
8. **签名消息无域分隔** — `auth.rs:19-25`：`name || nonce` 直接拼接，`("ab","c"+n)` 与 `("abc", n)` 同签名，共用密钥下可跨策略冒名。加域标签 + 长度前缀。
9. **nonce 防重放 check-then-insert 非原子** — `mock.rs:499-513`；nonce 表进程内、重启即清空。
10. **含 secret 的类型可 Debug** — `trading_service.rs:46-55`、`mock.rs:94-96`、`config.rs:163-165`：手写 Debug 打码或 `secrecy::SecretString`。
11. **下载数据无完整性校验** — `archive_download.rs:535-556`：从不拉取比对 Binance `.CHECKSUM`（SHA256），经代理篡改的 K 线直接进回测/实盘输入。修复：校验 `<url>.CHECKSUM`。
12. **WS 无握手超时/消息上限** — `trading_service.rs:648-661`：`connect_async` 无 timeout，`max_message_size` 默认 64MiB。入站 `positions`/`klines` 数组也不限长（`:1123-1142`）。
13. **错误路由匹配自由文本** — `trading_service.rs:1017-1029`：`message` 含"K线"就整帧丢弃 → 交易所拒单原文恰好带该词时订单挂到超时。改用结构化字段。
14. **`f64_or_zero` 静默兜 0** — `wire.rs:171-180`：脏数据变"成交 0 手/均价 0"写进账本。改 `Option` + 告警。

### 已确认无问题（正面清单）

- HMAC 用 `subtle` 恒定时间比较，实现正确
- TLS 未关闭校验（rustls + webpki-roots），全仓无 http:// 下载
- zip-slip 不存在（只读第 0 条目进内存，条目名不参与路径）
- 无 SQL（store 是 CSV 快照持久化，CSV 写入转义正确）
- 真实密钥未入库：`.env`/`config/_global.yaml` 均已正确 gitignore
- 回测侧 `cash`/`leverage` 无校验（config.rs:1088、main.rs:505）接受负数/0 杠杆 → 回测静默失真，建议 `ensure!(cash > 0 && leverage >= 1)`（med）

---

## 二、性能（按实盘收益排序）

1. **`spec()` 每 tick 深拷贝 + format!** — `engine/src/core/trading.rs:287-291`
   每次返回 `Arc::new(spec.clone())` + `namespaced_key()` format!。调用点遍布热路径（每动作/每单/每腿/每次 NAV 刷新）。修复：预建 `HashMap<key, Arc<InstrumentSpec>>`，`spec()` 返回 `Arc::clone`。**一处改动收益最大。**

2. **长跑实盘内存无界增长（唯一会让系统几周后整体变慢的根因）**
   - `runner.rs:66` `sub_positions`：Closed 永不删，且全部查找是线性 `find`
   - `runner.rs:188` `masters`：BTreeMap 只增不减
   - `runner.rs:259` `summary.fills`：`take_fills()` 实盘侧从未调用，每单持续 extend
   - `live.rs:140/142` `orders`/`states`：只 insert 无 remove
   修复：终态归档 + 保留活动指针 + 上限裁剪。

3. **决策轮三大分配源** — `strategy/features.rs:141-143`（每指标重建整段历史三个 Vec<f64> 并全长计算只为取末值，历史 10 万根时每轮 O(指标×历史)）；`strategy/runner.rs:1618`（每快照深拷贝整个 CandleSeries）；`bridge.rs:77`（每分钟全量重建 M1 序列）。
   修复：指标改 O(1) 增量递推；CandleSeries 用 `Arc` + 增量 push。

4. **在途请求表错配 + 无淘汰** — `trading_service.rs:735/864-931`
   `VecDeque` O(n) 扫描；超时后死条目永不清理、无界增长；应答缺 `signal_id` 时无条件 FIFO 取第一条 → **A 信号拿到 B 单的 client_order_id，整条订单生命周期跟错单（资金级错误）**。另外 `sink.send` 写在 select! 分支体内无超时（`:762-790`），对端窗口打满时心跳/看门狗全部失效，连接永久挂死。

5. **async 事件循环内同步落盘** — `app/src/strategy_live_actor.rs:932/982`
   `store.flush()`（全表序列化+全量重写+fsync）直接跑在 `current_thread` runtime 上，期间行情/订单回报全停。同仓 `spi_live_actor.rs:1021` 已用 `spawn_blocking`，两条链路不一致——对齐即可。

6. **50ms 忙轮询事件循环** — `strategy_live_actor.rs:947-974`
   `Starved → sleep(50ms)`：每 bar 最坏 +50ms 处理延迟（下单时点抖动）+ 20Hz 空转。改 `tokio::sync::Notify`/mpsc 通知驱动。

7. **O(n²) 与循环内 format!** — `store/src/lib.rs:309-345`（snapshot 每 id 线性 find）；`runner.rs:1295/1318/1431/1716/2306`（每轮多次 `slots.iter().position()` 且每次比较都 format! 一次 key）。修复：一次建 `key→idx` 索引。

8. **每根 K 线逐行 flush 系统调用** — `data/src/csv_store.rs:266-278`；warmup 补一年数据 ≈ 50 万次 syscall。改为批次结束/Drop 时 flush。

9. **阻塞 IO 散布在 async 上下文** — `bootstrap.rs:510-514`（每 bar 同步写 CSV）、`ts_kline.rs:407-414`（backfill 同步读全量 CSV）、`archive_download.rs:408-423`（async 内全量 CSV 读写）。统一 `spawn_blocking`。

10. **session 窗口表每 bar 重算** — `engine/src/core/session.rs:333-347`：每 bar × slot × tf 调 3-5 次，每次扫 4 天 + Vec 分配。按交易日缓存。

11. **回测全量 1m 数据复制 3 份** — `app/src/main.rs:429-443`：一年 1m ≈ 52 万根 × 3 份。改 partition/切片借用。

---

## 三、修复优先级建议

| # | 事项 | 类型 | 成本/收益 |
|---|------|------|-----------|
| 1 | 报告脱敏密钥（report.rs:455） | 安全 | 极低/高 |
| 2 | 空密钥 fail-fast + 非 loopback ws:// 拒启 | 安全 | 低/高 |
| 3 | 平仓数量 Decimal 校验（trading_service.rs:379） | 安全-资金级 | 低/高 |
| 4 | FIFO 错配 + 在途表淘汰 + send 超时 | 安全-资金级 | 中/高 |
| 5 | 外部时间戳 unwrap 清理（data/） | 稳定性 | 低/高 |
| 6 | `spec()` Arc 缓存（trading.rs:287） | 性能 | 低/高 |
| 7 | 五个无界容器加裁剪 | 性能-长跑 | 中/高 |
| 8 | flush 移出 async + 轮询改通知 | 性能 | 中/中 |
| 9 | 签名迁 Header + 域分隔 | 安全 | 中/中 |
| 10 | 下载限流 + CHECKSUM 校验 | 安全 | 中/中 |
