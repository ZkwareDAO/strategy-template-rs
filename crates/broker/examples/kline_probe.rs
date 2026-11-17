//! 临时探针：连接真实 trading-service 订阅 K 线并打印收到的帧。
//! 仅用于 CTP K 线接收诊断，运行结束即删除。
use std::time::Duration;

use strategy_broker::{TradingConfig, TradingServiceClient};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://192.168.1.12:9100/ws".into());
    let name = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "kline_probe".into());
    let exchange = std::env::args().nth(3).unwrap_or_else(|| "ctp".into());
    let syms: Vec<String> = std::env::args()
        .nth(4)
        .unwrap_or_else(|| "FU,BU,RU,SP,BR".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let secret = std::env::var("STRATEGY_SECRET").expect("STRATEGY_SECRET");
    let (client, _events) = TradingServiceClient::start_arc(
        TradingConfig {
            ws_url: url,
            request_timeout: Duration::from_secs(5),
            submit_enabled: false,
            reconnect_backoff_max_secs: 5,
            strategy_secret: secret,
        },
        tokio::runtime::Handle::current(),
    );
    match client.subscribe_klines(&name, &exchange, &syms).await {
        Ok(()) => println!("SUBSCRIBED exchange={exchange} symbols={syms:?}"),
        Err(e) => {
            println!("SUBSCRIBE_FAIL {e}");
            return;
        }
    }
    let mut rx = client.kline_updates();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(75);
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            println!("NO_FRAME_75S");
            break;
        }
        match tokio::time::timeout(left, rx.recv()).await {
            Ok(Ok(u)) => println!(
                "FRAME symbol={} venue={} start_ms={} close={}",
                u.symbol, u.venue, u.start_ms, u.close
            ),
            Ok(Err(e)) => {
                println!("RECV_ERR {e}");
                break;
            }
            Err(_) => {
                println!("NO_FRAME_75S");
                break;
            }
        }
    }
}
