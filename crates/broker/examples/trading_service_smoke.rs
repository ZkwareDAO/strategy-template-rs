//! 真实 trading-service 联调冒烟脚本。
//!
//! 运行（另两个终端分别起 trading-service 与 exchange-mock）：
//!   cargo run -p strategy-broker --example trading_service_smoke -- \
//!     --url ws://127.0.0.1:9100/ws --strategy-name smoke \
//!     --secret dev-secret --exchange xili --account test_binance \
//!     --instrument btcusdt --side long --quantity 1
//!
//! 期望输出：ack 拿到 0x 订单号，随后收到 FILLED order_event。

use std::time::Duration;

use strategy_broker::{
    ClientEvent, OrderVenueCfg, TradingConfig, TradingGateway, TradingServiceClient,
};
use strategy_core::{IntentKind, Side, SignalIntent, SignalStatus};
use strategy_signals::gateway::AsyncOrderGateway;

#[derive(Debug, PartialEq)]
struct Args {
    url: String,
    name: String,
    secret: String,
    exchange: String,
    account: String,
    instrument: String,
    side: Side,
    quantity: f64,
    kind: IntentKind,
}

fn parse_args() -> Args {
    parse_from(std::env::args().skip(1))
}

fn parse_from<I>(mut iter: I) -> Args
where
    I: Iterator<Item = String>,
{
    let mut a = Args {
        url: "ws://127.0.0.1:9100/ws".into(),
        name: "smoke".into(),
        secret: String::new(),
        exchange: "binance".into(),
        account: "test_binance".into(),
        instrument: "btcusdt".into(),
        side: Side::Long,
        quantity: 1.0,
        kind: IntentKind::Open,
    };
    while let Some(k) = iter.next() {
        match k.as_str() {
            // 取值参数：共用一条 match 臂，取下一个 token 作为值
            "--url" | "--strategy-name" | "--secret" | "--exchange" | "--account"
            | "--instrument" => {
                let v = iter.next().unwrap_or_default();
                match k.as_str() {
                    "--url" => a.url = v,
                    "--strategy-name" => a.name = v,
                    "--secret" => a.secret = v,
                    "--exchange" => a.exchange = v,
                    "--account" => a.account = v,
                    "--instrument" => a.instrument = v,
                    _ => unreachable!(),
                }
            }
            "--side" => {
                let v = iter.next().unwrap_or_default();
                a.side = match v.as_str() {
                    "short" | "sell" => Side::Short,
                    _ => Side::Long,
                };
            }
            "--quantity" => {
                let v = iter.next().unwrap_or_default();
                a.quantity = v.parse().expect("quantity 必须是数字");
            }
            // 无值开关：不取后续 token
            "--close" => a.kind = IntentKind::Close,
            _ => eprintln!("忽略未知参数 {k}"),
        }
    }
    a
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let args = parse_args();
    let cfg = TradingConfig {
        ws_url: args.url.clone(),
        request_timeout: Duration::from_secs(10),
        submit_enabled: true,
        reconnect_backoff_max_secs: 8,
        strategy_secret: args.secret.clone(),
    };
    let (client, mut events) =
        TradingServiceClient::start_arc(cfg, tokio::runtime::Handle::current());
    // smoke 下单场所配置：杠杆固定 1，金额按 --quantity 名义值
    let venue = OrderVenueCfg {
        exchange: args.exchange.clone(),
        account: args.account.clone(),
        leverage: 1,
        order_cash: args.quantity,
    };
    let mut gw = TradingGateway::new(client, args.name.clone(), venue);

    let is_close = args.kind == IntentKind::Close;
    let intent = SignalIntent {
        id: format!("smoke-{}", chrono::Utc::now().timestamp_millis()),
        strategy_id: args.name.clone(),
        symbol: args.instrument.clone(),
        kind: args.kind,
        direction: args.side,
        price: 0.0,
        size: args.quantity,
        size_raw: None,
        stop_price: None,
        // 平仓帧必须携带 position_id（组帧判定依据）
        position_id: is_close.then(|| "smoke-position".into()),
        stop_order_id: None,
        created_event_time: chrono::Utc::now(),
        status: SignalStatus::Created,
    };

    println!(
        "[smoke:{}] 提交 place_order（{:?} {:?} {}）",
        args.name, args.kind, args.side, args.quantity
    );
    match gw.submit_fut(&intent).await {
        Ok(cid) => println!("[smoke:{}] ack client_order_id={cid}", args.name),
        Err(e) => {
            eprintln!("[smoke:{}] 下单失败: {e}", args.name);
            std::process::exit(1);
        }
    }

    // 收 10s 事件（Reconnected + 订单回报）。
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            ev = events.recv() => match ev {
                Some(ClientEvent::OrderUpdate(u)) => {
                    println!("[smoke:{}] << order_event {:?} {} filled={}@{}",
                        args.name, u.state, u.order_id, u.filled_size, u.avg_price);
                }
                Some(ClientEvent::Reconnected { strategy_name }) => {
                    println!("[smoke:{}] << connected as {strategy_name}", args.name);
                }
                None => break,
            },
            _ = &mut deadline => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(x: &[&str]) -> Vec<String> {
        x.iter().map(|&s| s.to_string()).collect()
    }

    #[test]
    fn close_flag_before_side_does_not_consume_side() {
        // 核心回归：--close 不得吞掉 --side
        let a = parse_from(s(&["--close", "--side", "short", "--quantity", "2"]).into_iter());
        assert_eq!(a.kind, IntentKind::Close);
        assert_eq!(a.side, Side::Short);
        assert!((a.quantity - 2.0).abs() < 1e-9);
    }

    #[test]
    fn close_flag_at_end_works() {
        let a = parse_from(s(&["--side", "short", "--quantity", "1", "--close"]).into_iter());
        assert_eq!(a.kind, IntentKind::Close);
        assert_eq!(a.side, Side::Short);
        assert!((a.quantity - 1.0).abs() < 1e-9);
    }

    #[test]
    fn default_values_on_empty_iterator() {
        let a = parse_from(std::iter::empty());
        assert_eq!(a.kind, IntentKind::Open);
        assert_eq!(a.side, Side::Long);
        assert!((a.quantity - 1.0).abs() < 1e-9);
        assert_eq!(a.url, "ws://127.0.0.1:9100/ws");
        assert_eq!(a.exchange, "binance");
        assert_eq!(a.account, "test_binance");
        assert_eq!(a.name, "smoke");
    }
}
