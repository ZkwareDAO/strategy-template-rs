//! K 线与时间框架

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// 单根 K 线（serde 字段名即结构字段名，open_time 为 RFC3339）
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Candle {
    /// 开盘时间（UTC，K 线 id）
    pub open_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    /// 成交额（计价币）
    pub quote_volume: f64,
    /// 成交笔数
    pub trade_num: u64,
    /// 主动买入成交量（基础币）
    pub active_buy_volume: f64,
    /// 主动买入成交额（计价币）
    pub active_buy_quote_volume: f64,
    /// 是否已闭合（实盘 WS 会推未闭合快照；聚合/指标只消费 true）
    pub is_closed: bool,
}

impl Candle {
    /// 1m 收盘时间 = 开盘时间 + 60s
    pub fn close_time_1m(&self) -> DateTime<Utc> {
        self.open_time + chrono::Duration::seconds(60)
    }
}

/// 时间框架（分钟数表达；1m=1, 15m=15, 1h=60, 4h=240, 1d=1440）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Timeframe(pub u32);

impl Timeframe {
    pub const M1: Timeframe = Timeframe(1);
    pub const M15: Timeframe = Timeframe(15);
    pub const H1: Timeframe = Timeframe(60);
    pub const H4: Timeframe = Timeframe(240);
    pub const D1: Timeframe = Timeframe(1440);

    /// "8h" / "15m" / "1d" → Timeframe
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim().to_ascii_lowercase();
        let (num, unit) = s.split_at(
            s.len()
                .checked_sub(1)
                .ok_or_else(|| Error::Config(format!("非法时间框架: {s}")))?,
        );
        let n: u32 = num
            .parse()
            .map_err(|_| Error::Config(format!("非法时间框架: {s}")))?;
        let minutes = match unit {
            "m" => n,
            "h" => n
                .checked_mul(60)
                .ok_or_else(|| Error::Config(format!("时间框架溢出: {s}")))?,
            "d" => n
                .checked_mul(1440)
                .ok_or_else(|| Error::Config(format!("时间框架溢出: {s}")))?,
            "w" => n
                .checked_mul(10080)
                .ok_or_else(|| Error::Config(format!("时间框架溢出: {s}")))?,
            _ => return Err(Error::Config(format!("非法时间框架单位: {s}"))),
        };
        if minutes == 0 {
            return Err(Error::Config(format!("时间框架不能为 0: {s}")));
        }
        Ok(Timeframe(minutes))
    }
}

impl std::fmt::Display for Timeframe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let m = self.0;
        if m.is_multiple_of(10080) {
            // 周线按 7d 表达（非自然周对齐，纯 10080 分钟窗口）
            write!(f, "{}d", m / 1440)
        } else if m.is_multiple_of(1440) {
            write!(f, "{}d", m / 1440)
        } else if m.is_multiple_of(60) {
            write!(f, "{}h", m / 60)
        } else {
            write!(f, "{}m", m)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn parse_roundtrip() {
        for s in ["1m", "15m", "1h", "4h", "8h", "1d"] {
            let tf = Timeframe::parse(s).unwrap();
            assert_eq!(tf.to_string(), s, "{s} 往返失败");
        }
    }

    #[test]
    fn parse_rejects_garbage() {
        for s in ["", "x", "hm", "0m", "4x"] {
            assert!(Timeframe::parse(s).is_err(), "{s} 应被拒绝");
        }
    }

    #[test]
    fn close_time_1m() {
        let t = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
        let c = Candle {
            open_time: t,
            open: 1.0,
            high: 2.0,
            low: 0.5,
            close: 1.5,
            volume: 10.0,
            quote_volume: 0.0,
            trade_num: 0,
            active_buy_volume: 0.0,
            active_buy_quote_volume: 0.0,
            is_closed: true,
        };
        assert_eq!(c.close_time_1m(), t + chrono::Duration::seconds(60));
    }
}
