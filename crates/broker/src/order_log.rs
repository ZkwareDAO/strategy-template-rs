//! exchange_orders.csv 追加写 — 订单回执（每次订单状态更新追加一行）
//!
//! 契约（列序固定，用户指定）：
//! signal_id, symbol, exchange, leverage, pos_type, price, side,
//! exchange_order_id, exchange_order_status, exchange_order_price,
//! exchange_order_quantity, exchange_update_time
//!
//! - price = 信号触发价；exchange_order_price = 实际成交价
//! - side 沿用 1=buy 2=sell；pos_type 1=现货 2=合约
//! - status 用币安枚举（NEW / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED）
//! - 组织：{base}/{strategy_name}/exchange_orders.csv
//! - update_time 用 K 线时间（回测可复现；实盘由 broker 传交易所时间）

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

/// 币安订单状态枚举（本框架产生的子集）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

impl OrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            OrderStatus::New => "NEW",
            OrderStatus::PartiallyFilled => "PARTIALLY_FILLED",
            OrderStatus::Filled => "FILLED",
            OrderStatus::Canceled => "CANCELED",
            OrderStatus::Rejected => "REJECTED",
        }
    }
}

/// 方向码：1=buy 2=sell
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SideCode {
    Buy,
    Sell,
}

impl SideCode {
    pub fn as_u8(self) -> u8 {
        match self {
            SideCode::Buy => 1,
            SideCode::Sell => 2,
        }
    }
}

/// 持仓类型：1=现货 2=合约
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PosType {
    Spot,
    Futures,
}

impl PosType {
    pub fn as_u8(self) -> u8 {
        match self {
            PosType::Spot => 1,
            PosType::Futures => 2,
        }
    }
}

pub const FIELDNAMES: [&str; 12] = [
    "signal_id",
    "symbol",
    "exchange",
    "leverage",
    "pos_type",
    "price",
    "side",
    "exchange_order_id",
    "exchange_order_status",
    "exchange_order_price",
    "exchange_order_quantity",
    "exchange_update_time",
];

/// 一次订单回执行
#[derive(Debug, Clone)]
pub struct ExchangeOrderRecord {
    pub strategy_name: String,
    pub signal_id: String,
    pub symbol: String,
    pub exchange: String,
    pub leverage: u32,
    pub pos_type: PosType,
    /// 信号触发价
    pub price: f64,
    pub side: SideCode,
    /// 交易所订单号（回测 = 合成 ID `bt_{signal_id}`）
    pub exchange_order_id: String,
    pub status: OrderStatus,
    /// 实际成交价（回测 = 触发价±滑点）
    pub exchange_order_price: f64,
    pub exchange_order_quantity: f64,
    /// 状态更新时间（K 线时间）
    pub exchange_update_time: DateTime<Utc>,
}

impl ExchangeOrderRecord {
    fn to_row(&self) -> Vec<String> {
        vec![
            self.signal_id.clone(),
            self.symbol.clone(),
            self.exchange.clone(),
            self.leverage.to_string(),
            self.pos_type.as_u8().to_string(),
            fmt(self.price),
            self.side.as_u8().to_string(),
            self.exchange_order_id.clone(),
            self.status.as_str().to_string(),
            fmt(self.exchange_order_price),
            fmt(self.exchange_order_quantity),
            self.exchange_update_time.to_rfc3339(),
        ]
    }
}

fn fmt(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// 追加写器：{base}/{strategy_name}/exchange_orders.csv
pub struct ExchangeOrderWriter {
    base: PathBuf,
}

impl ExchangeOrderWriter {
    pub fn new(base: impl AsRef<Path>) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
        }
    }

    /// 追加一条订单回执（header-on-create；失败返回 Err 由调用方决策）
    pub fn write(&self, rec: &ExchangeOrderRecord) -> io::Result<()> {
        let path = self
            .base
            .join(&rec.strategy_name)
            .join("exchange_orders.csv");
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let write_header = !path.exists();
        let mut wtr = csv::WriterBuilder::new().flexible(true).from_writer(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?,
        );
        if write_header {
            wtr.write_record(FIELDNAMES)?;
        }
        wtr.write_record(rec.to_row())?;
        wtr.flush()?;
        wtr.into_inner().map_err(io::Error::other)?;

        tracing::info!(
            signal_id = %rec.signal_id,
            status = rec.status.as_str(),
            fill_price = rec.exchange_order_price,
            "exchange_orders.csv 已写入"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn rec(status: OrderStatus) -> ExchangeOrderRecord {
        ExchangeOrderRecord {
            strategy_name: "sar_snt3_8H_3_BTCUSDT_LIVE".into(),
            signal_id: "sig_abc".into(),
            symbol: "BTCUSDT".into(),
            exchange: "binance".into(),
            leverage: 5,
            pos_type: PosType::Futures,
            price: 60000.0,
            side: SideCode::Buy,
            exchange_order_id: "bt_sig_abc".into(),
            status,
            exchange_order_price: 60005.5,
            exchange_order_quantity: 0.0033,
            exchange_update_time: Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap(),
        }
    }

    #[test]
    fn writes_header_then_appends() {
        let dir = tempfile::tempdir().unwrap();
        let w = ExchangeOrderWriter::new(dir.path());

        w.write(&rec(OrderStatus::Filled)).unwrap();
        w.write(&rec(OrderStatus::Canceled)).unwrap();

        let path = dir
            .path()
            .join("sar_snt3_8H_3_BTCUSDT_LIVE/exchange_orders.csv");
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3); // header + 2
        assert!(lines[0].starts_with("signal_id,symbol,exchange,leverage,pos_type,price,side"));
        assert!(lines[1].contains("sig_abc"));
        assert!(lines[1].contains("FILLED"));
        assert!(lines[1].contains("60005.5"));
        assert!(lines[2].contains("CANCELED"));
    }

    #[test]
    fn status_strings() {
        assert_eq!(OrderStatus::New.as_str(), "NEW");
        assert_eq!(OrderStatus::PartiallyFilled.as_str(), "PARTIALLY_FILLED");
        assert_eq!(OrderStatus::Rejected.as_str(), "REJECTED");
    }
}
