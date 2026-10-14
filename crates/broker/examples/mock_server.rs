//! mock_server —— 独立的本地 trading-service 模拟服务（协议 v2）
//!
//! 启动一个常驻进程，供策略联调：支持开仓帧 order_cash 按参考价换算、平仓帧
//! quantity+reduce_only，HMAC 握手，ack/order_event 全字段回声。
//!
//! 运行：
//! ```bash
//! cargo run -p strategy-broker --example mock_server -- \
//!   --addr 127.0.0.1:9100 --secret dev-secret --price 80000
//! ```
//! 然后策略把 services.trading.ws_url 指向 ws://127.0.0.1:9100/ws 即可。

use std::time::Duration;

use strategy_broker::{MockScenario, MockServer};

#[derive(Default)]
struct Args {
    addr: String,
    secret: String,
    price: f64,
    fill_interval_ms: u64,
    /// (instrument, reference_price)：per-symbol 参考价，可重复给出
    symbol_prices: Vec<(String, f64)>,
}

fn parse_args() -> Args {
    let mut a = Args {
        addr: "127.0.0.1:9100".into(),
        secret: "dev-secret".into(),
        price: 80000.0,
        fill_interval_ms: 0,
        symbol_prices: Vec::new(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        match k.as_str() {
            "--addr" => a.addr = it.next().unwrap_or(a.addr),
            "--secret" => a.secret = it.next().unwrap_or(a.secret),
            "--price" => a.price = it.next().and_then(|v| v.parse().ok()).unwrap_or(a.price),
            "--fill-interval-ms" => {
                a.fill_interval_ms = it.next().and_then(|v| v.parse().ok()).unwrap_or(0)
            }
            // per-symbol 参考价：--symbol-price SOLUSDT=111.5（可重复）
            "--symbol-price" => {
                if let Some(raw) = it.next() {
                    if let Some((sym, px)) = raw.split_once('=') {
                        if let Ok(px) = px.parse::<f64>() {
                            a.symbol_prices.push((sym.trim().to_string(), px));
                            continue;
                        }
                    }
                    eprintln!("忽略非法 --symbol-price（应为 SYMBOL=PRICE）：{raw}");
                }
            }
            _ => eprintln!("忽略参数 {k}"),
        }
    }
    a
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let mut scenario = MockScenario::new(args.secret)
        .fill_price(args.price)
        .fill_interval(Duration::from_millis(args.fill_interval_ms));
    for (sym, px) in &args.symbol_prices {
        scenario = scenario.symbol_price(sym.clone(), *px);
    }
    println!(
        "mock-server 启动：addr={} 默认参考价={} per-symbol={:?} 成交间隔={}ms（Ctrl-C 退出）",
        args.addr, args.price, args.symbol_prices, args.fill_interval_ms
    );
    // 绑定指定地址（非随机端口）并常驻。
    MockServer::bind(&args.addr, scenario)
        .await
        .expect("绑定 mock 端口失败")
        .serve()
        .await;
}
