//! 真实 zkware klines-service 端到端探针（默认不跑：`cargo test -p strategy-data
//! --test zkware_live -- --ignored --nocapture`）。
//!
//! 验证：① WS 握手 + action/subscribe 订阅帧被接受；
//! ② 收到 type:kline 推送并解析为 Closed/Tick；
//! ③ HTTP /api/v1/klines 时间范围回填返回币安行；
//! ④ 单帧多 symbol 订阅：subscribed 回执是否回显全部 symbols、
//!    kline 帧如何携带 symbol（data.symbol / 外层 symbol）区分代币。
//! 仅本机手动运行，不进常规门禁。

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use strategy_core::candle::Timeframe;
use strategy_data::{KlineClient, KlineEvent, KlineServiceConfig, RemoteKlineServiceClient};
use tokio::runtime::Handle;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn probe_zkware_ws_and_http() {
    let cfg = KlineServiceConfig {
        backfill_url: "https://zkwarefr.zkware.cn".into(),
        ws_url: "wss://zkwarefr.zkware.cn/ws/klines".into(),
        ..Default::default()
    };
    let client = RemoteKlineServiceClient::start(&["BTCUSDT".to_string()], cfg, Handle::current());

    // 1) HTTP 回填：最近 5 根 1m
    let hist = client
        .backfill("BTCUSDT", Timeframe::M1, 5)
        .await
        .expect("回填请求失败");
    println!("HTTP backfill 根数: {}", hist.len());
    assert!(!hist.is_empty(), "回填不应为空");
    for c in &hist {
        println!(
            "  {} O={} H={} L={} C={} V={}",
            c.open_time, c.open, c.high, c.low, c.close, c.volume
        );
    }

    // 2) WS 订阅：等 Reconnected + 首根 kline。服务端每分钟收盘只推 1 帧，
    //    订阅时刻最坏需等近 60s，窗口放到 75s（final 或 tick 均可）
    let mut stream = client.attach("BTCUSDT", Timeframe::M1);
    let mut got_reconnect = false;
    let mut got_bar = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(75);
    while tokio::time::Instant::now() < deadline && !got_bar {
        match tokio::time::timeout_at(deadline, stream.rx.recv()).await {
            Ok(Ok(KlineEvent::Reconnected)) => {
                got_reconnect = true;
                println!("WS Reconnected");
            }
            Ok(Ok(KlineEvent::Closed(b))) => {
                got_bar = true;
                println!(
                    "WS CLOSED {} @{} C={}",
                    b.symbol, b.candle.open_time, b.candle.close
                );
            }
            Ok(Ok(KlineEvent::Tick(b))) => {
                got_bar = true;
                println!(
                    "WS TICK   {} @{} C={}",
                    b.symbol, b.candle.open_time, b.candle.close
                );
            }
            Ok(Err(_)) => panic!("事件流意外关闭"),
            Err(_) => break,
        }
    }
    assert!(got_reconnect, "应收到 Reconnected");
    assert!(got_bar, "75s 内未收到任何 kline 推送");
}

/// 单帧多 symbol 订阅探针：一条 WS 发 `symbols:[BTCUSDT,ETHUSDT,SOLUSDT]`，
/// 观察 ① subscribed 回执是否回显全部 symbols；② kline 帧的 symbol 携带位置
/// （data.symbol / 外层 symbol）；③ 每 symbol 各自收到几根帧。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn probe_zkware_multi_symbol_subscribe() {
    const SYMBOLS: [&str; 3] = ["BTCUSDT", "ETHUSDT", "SOLUSDT"];
    const URL: &str = "wss://zkwarefr.zkware.cn/ws/klines";

    let (ws, _) = tokio_tungstenite::connect_async(URL)
        .await
        .expect("WS 连接失败");
    let (mut sink, mut source) = ws.split();
    sink.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::json!({ "action": "subscribe", "symbols": SYMBOLS }).to_string(),
    ))
    .await
    .expect("订阅帧发送失败");
    println!("已发送订阅帧: symbols={SYMBOLS:?}");

    let mut confirmed: Option<Vec<String>> = None;
    let mut counts: std::collections::HashMap<String, (u32, u32)> = Default::default(); // (closed, tick)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, source.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(t)))) => {
                let v: Value = match serde_json::from_str(&t) {
                    Ok(v) => v,
                    Err(e) => {
                        println!("[非 JSON 帧] {t:.200} ({e})");
                        continue;
                    }
                };
                match v.get("type").and_then(|s| s.as_str()) {
                    Some("subscribed") => {
                        let syms: Vec<String> = v
                            .get("symbols")
                            .and_then(|s| s.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default();
                        println!("[subscribed 回执] symbols={syms:?}");
                        confirmed = Some(syms);
                    }
                    Some("kline") => {
                        let outer = v.get("symbol").and_then(|s| s.as_str());
                        let inner = v
                            .get("data")
                            .and_then(|d| d.get("symbol"))
                            .and_then(|s| s.as_str());
                        let is_final = v
                            .get("data")
                            .and_then(|d| d.get("is_final"))
                            .and_then(|b| b.as_bool())
                            .unwrap_or(true);
                        let symbol = inner.or(outer).unwrap_or("?");
                        let e = counts.entry(symbol.to_string()).or_insert((0, 0));
                        if is_final {
                            e.0 += 1;
                        } else {
                            e.1 += 1;
                        }
                        println!(
                            "[kline] symbol={symbol} (外层={outer:?}, data内={inner:?}) is_final={is_final}"
                        );
                    }
                    other => println!("[其他帧] type={other:?} {t:.200}"),
                }
            }
            Ok(Some(Ok(other))) => println!("[非文本帧] {other:?}"),
            Ok(Some(Err(e))) => {
                println!("[WS 错误] {e}");
                break;
            }
            Ok(None) => {
                println!("[WS 关闭]");
                break;
            }
            Err(_) => break,
        }
    }

    println!("=== 汇总 ===");
    println!(
        "subscribed 回执: {:?}",
        confirmed.as_deref().unwrap_or(&["（未收到）".into()][..])
    );
    for (sym, (closed, tick)) in counts.iter() {
        println!("  {sym}: closed={closed} tick={tick}");
    }
    assert!(
        confirmed.is_some(),
        "未收到 subscribed 确认帧——多 symbol 单帧订阅未被接受"
    );
}
