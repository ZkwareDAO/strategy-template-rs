//! signal.csv 追加写器

use std::fs;
use std::path::{Path, PathBuf};

use tracing::{info, warn};

use crate::record::SignalRecord;

pub const FIELDNAMES: [&str; 14] = [
    "strategy_name",
    "signal_id",
    "timestamp",
    "user_id",
    "signal_action",
    "symbol",
    "pos_type",
    "cash",
    "leverage",
    "side",
    "exchange",
    "trigger_price",
    "order_type",
    "quantity",
];

/// 信号写器：{base}/{strategy_name}/signal.csv
pub struct SignalWriter {
    base: PathBuf,
}

impl SignalWriter {
    pub fn new(base: impl AsRef<Path>) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
        }
    }

    fn file_path(&self, strategy_name: &str) -> PathBuf {
        self.base.join(strategy_name).join("signal.csv")
    }

    /// 追加一条信号。文件不存在时先写 header（header-on-create）。
    /// 写入失败返回 Err（信号丢失必须暴露，不静默吞掉）。
    pub fn write(&self, rec: &SignalRecord) -> std::io::Result<()> {
        let path = self.file_path(&rec.strategy_name);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }

        let write_header = !path.exists();
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        let mut wtr = csv::WriterBuilder::new().flexible(true).from_writer(file);

        if write_header {
            wtr.write_record(FIELDNAMES)?;
        }
        wtr.write_record(rec.to_row())?;
        wtr.flush()?; // 信号必须立即落盘
        wtr.into_inner().map_err(|e| {
            warn!(error = %e, "signal.csv into_inner 失败");
            std::io::Error::other(e)
        })?; // 缓冲异常在此暴露

        info!(
            signal_id = %rec.signal_id,
            strategy = %rec.strategy_name,
            action = %rec.signal_action,
            "signal.csv 已写入"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{OrderType, PosType, Side};
    use chrono::{TimeZone, Utc};

    fn sample(signal_id: &str) -> SignalRecord {
        SignalRecord {
            strategy_name: "sar_snt3_8H_3_BTCUSDT_LIVE".into(),
            signal_id: signal_id.into(),
            timestamp: Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap(),
            user_id: 1,
            signal_action: "buy".into(),
            symbol: "BTCUSDT".into(),
            pos_type: PosType::FUTURES,
            cash: 200.0,
            leverage: 5,
            side: Side::Buy,
            exchange: "binance".into(),
            trigger_price: 60000.0,
            order_type: OrderType::MARKET,
            quantity: 0.0,
        }
    }

    #[test]
    fn write_creates_header_then_appends() {
        let dir = tempfile::tempdir().unwrap();
        let w = SignalWriter::new(dir.path());

        w.write(&sample("sig_1")).unwrap();
        w.write(&sample("sig_2")).unwrap();

        let content =
            std::fs::read_to_string(dir.path().join("sar_snt3_8H_3_BTCUSDT_LIVE/signal.csv"))
                .unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3); // header + 2 行
        assert!(lines[0].starts_with("strategy_name,signal_id"));
        assert!(lines[1].contains("sig_1"));
        assert!(lines[2].contains("sig_2"));
    }
}
