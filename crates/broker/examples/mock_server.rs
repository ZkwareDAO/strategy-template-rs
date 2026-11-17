//! mock_server —— 独立的本地 trading-service 模拟服务（协议 v2）
//!
//! 启动一个常驻进程，供策略联调：支持开仓帧 order_cash 按参考价换算、平仓帧
//! quantity+reduce_only，HMAC 握手，ack/order_event 全字段回声；
//! **K线模拟回放**：`--kline-dir` 指向 `data/klines/1m` 后，响应
//! `subscribe_kline` 并按节奏推 `kline_1m_batch` 合帧（§10.3，单根亦发
//! batch），成交价自动跟随最新收盘；`get_klines` 应答本地留存历史（§10.4）。
//!
//! 运行：
//! ```bash
//! cargo run -p strategy-broker --example mock_server -- \
//!   --addr 127.0.0.1:9100 --secret dev-secret --price 80000 \
//!   --kline-dir data/klines/1m --kline-interval-ms 100 --kline-start 2025-12-01
//! ```
//! 然后策略把 services.trading.ws_url 指向 ws://127.0.0.1:9100/ws——只需这一个
//! 端点：K线即从本进程的订阅帧收取（旧 services.kline 节已退役，写了会被拒启）。

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
    /// K线回放目录（如 data/klines/1m）；给出即启用 subscribe_kline 推送
    kline_dir: Option<String>,
    /// 相邻两根 bar 推送间隔（毫秒）；0 = 尽快推（纯回放吞吐测试用）
    kline_interval_ms: u64,
    /// 回放起点：epoch 毫秒 或 YYYY-MM-DD（UTC 零点）；缺省 = CSV 第一根
    kline_start: Option<String>,
}

/// --kline-start 解析：纯数字按 epoch 毫秒；YYYY-MM-DD 按 UTC 零点。
fn parse_start(s: &str) -> Option<i64> {
    if let Ok(ms) = s.trim().parse::<i64>() {
        return Some(ms);
    }
    chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d")
        .ok()
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis())
}

fn parse_args() -> Args {
    let mut a = Args {
        addr: "127.0.0.1:9100".into(),
        secret: "dev-secret".into(),
        price: 80000.0,
        fill_interval_ms: 0,
        symbol_prices: Vec::new(),
        kline_dir: None,
        kline_interval_ms: 100,
        kline_start: None,
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
            "--kline-dir" => a.kline_dir = it.next(),
            "--kline-interval-ms" => {
                a.kline_interval_ms = it.next().and_then(|v| v.parse().ok()).unwrap_or(100)
            }
            "--kline-start" => a.kline_start = it.next(),
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
    let start_ms = args.kline_start.as_deref().and_then(|s| {
        parse_start(s).or_else(|| {
            eprintln!("忽略非法 --kline-start（应为 epoch 毫秒或 YYYY-MM-DD）：{s}");
            None
        })
    });
    let kline_desc = match &args.kline_dir {
        Some(dir) => {
            let (sc, errs) =
                scenario.kline_dir(dir, Duration::from_millis(args.kline_interval_ms), start_ms);
            scenario = sc;
            for e in &errs {
                eprintln!("K线装载告警：{e}");
            }
            let syms = scenario
                .kline
                .as_ref()
                .map(|k| k.book.symbols().join(","))
                .unwrap_or_default();
            format!(
                "K线回放={}（间隔={}ms/根 起点={:?}）已装载: {syms}",
                dir, args.kline_interval_ms, args.kline_start
            )
        }
        None => "K线回放=关（订阅将被拒并提示 --kline-dir）".to_string(),
    };
    println!(
        "mock-server 启动：addr={} 默认参考价={} per-symbol={:?} 成交间隔={}ms\n{kline_desc}（Ctrl-C 退出）",
        args.addr, args.price, args.symbol_prices, args.fill_interval_ms
    );
    // 绑定指定地址（非随机端口）并常驻。
    MockServer::bind(&args.addr, scenario)
        .await
        .expect("绑定 mock 端口失败")
        .serve()
        .await;
}
