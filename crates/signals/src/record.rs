//! signal.csv 行记录

use chrono::{DateTime, Utc};

/// 持仓类型：1=现货 2=合约
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PosType(pub u8);

impl PosType {
    pub const SPOT: PosType = PosType(1);
    pub const FUTURES: PosType = PosType(2);

    pub fn as_u8(self) -> u8 {
        self.0
    }
}

/// 方向：1=buy 2=sell（沿用 Python SIDE_MAP）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_u8(self) -> u8 {
        match self {
            Side::Buy => 1,
            Side::Sell => 2,
        }
    }

    pub fn from_direction_long(is_long: bool) -> Self {
        if is_long {
            Side::Buy
        } else {
            Side::Sell
        }
    }
}

/// 订单类型：1=限价 2=市价
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderType(pub u8);

impl OrderType {
    pub const LIMIT: OrderType = OrderType(1);
    pub const MARKET: OrderType = OrderType(2);

    pub fn as_u8(self) -> u8 {
        self.0
    }
}

/// signal.csv 单行
#[derive(Debug, Clone)]
pub struct SignalRecord {
    pub strategy_name: String,
    pub signal_id: String,
    /// K 线时间（RFC3339，UTC）
    pub timestamp: DateTime<Utc>,
    pub user_id: u64,
    /// 信号动作字符串（buy/sell/buy_close/sell_close/reverse_long/reverse_short/flat）
    pub signal_action: String,
    pub symbol: String,
    pub pos_type: PosType,
    /// 金额（与 quantity 二选一；>0 时有效）
    pub cash: f64,
    pub leverage: u32,
    pub side: Side,
    pub exchange: String,
    pub trigger_price: f64,
    pub order_type: OrderType,
    /// 数量（与 cash 二选一；>0 时有效）
    pub quantity: f64,
}

impl SignalRecord {
    pub fn to_row(&self) -> Vec<String> {
        vec![
            self.strategy_name.clone(),
            self.signal_id.clone(),
            self.timestamp.to_rfc3339(),
            self.user_id.to_string(),
            self.signal_action.clone(),
            self.symbol.clone(),
            self.pos_type.as_u8().to_string(),
            fmt_f64(self.cash),
            self.leverage.to_string(),
            self.side.as_u8().to_string(),
            self.exchange.clone(),
            fmt_f64(self.trigger_price),
            self.order_type.as_u8().to_string(),
            fmt_f64(self.quantity),
        ]
    }
}

/// 数值输出：整数不带小数点，否则原样输出
fn fmt_f64(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn fmt_f64_variants() {
        assert_eq!(fmt_f64(100.0), "100");
        assert_eq!(fmt_f64(0.5), "0.5");
        assert_eq!(fmt_f64(-12.25), "-12.25");
    }

    #[test]
    fn to_row_order() {
        let r = SignalRecord {
            strategy_name: "sar_snt3_8H_3_BTCUSDT_LIVE".into(),
            signal_id: "sig_abc".into(),
            timestamp: Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap(),
            user_id: 1,
            signal_action: "buy".into(),
            symbol: "BTCUSDT".into(),
            pos_type: PosType::FUTURES,
            cash: 200.0,
            leverage: 5,
            side: Side::Buy,
            exchange: "binance".into(),
            trigger_price: 60000.5,
            order_type: OrderType::MARKET,
            quantity: 0.0,
        };
        assert_eq!(r.to_row()[0], "sar_snt3_8H_3_BTCUSDT_LIVE");
        assert_eq!(r.to_row()[2], "2026-06-10T00:00:00+00:00");
        assert_eq!(r.to_row()[6], "2"); // pos_type
        assert_eq!(r.to_row()[9], "1"); // side buy
        assert_eq!(r.to_row()[12], "2"); // order_type market
    }
}
