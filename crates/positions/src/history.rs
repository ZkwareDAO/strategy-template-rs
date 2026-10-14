//! 平仓历史 —— 统一 Store `history_position` model + 对外固定列 CSV 旁路导出
//!
//! 双轨（D6，与 signal.csv 同性质）：
//! - 内存/恢复真相：[`HistoryPosition`] 是统一 Store 中 `NAME="history_position"`
//!   的 model（id=position_id，只增不改），随进程 Store 定时 flush 到
//!   `{base}/{YYYYMMDD}/history_position.csv`，启动跨日去重恢复；
//! - 对外报表契约：[`HistoryPositionLogger`] 仍在平仓时把同一行追加写到
//!   `history_positions/{strategy_name}/{YYYYMMDD}.csv`（22 列固定列序，下游
//!   Python/report 按列名消费；见 docs/design.md 对外报表清单）。
//!
//! 与 Python 的差异（有意）：分文件日期取**平仓 K 线时间的 UTC 日期**，
//! 不取 now() —— 回测重放时文件归属可复现（K线时间红线）。

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use strategy_store::Model;

use crate::state::Side;

pub const FIELDNAMES: [&str; 24] = [
    "position_id",
    "strategy_name",
    "symbol",
    "position_type",
    "entry_price",
    "exit_price",
    "entry_time",
    "exit_time",
    "entry_timestamp",
    "exit_timestamp",
    "peak_price",
    "stop_price",
    "max_pnl_pct",
    "min_pnl_pct",
    "exit_reason",
    "is_stop_loss",
    "price_diff",
    "pnl_pct",
    "duration_seconds",
    "atr_at_entry",
    "trail_activated",
    "trading_mode",
    "entry_size",
    "exit_size",
];

/// 一次平仓的历史记录（Store 行；同时是对外 22 列 CSV 的导出来源）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistoryPosition {
    pub position_id: String,
    pub strategy_name: String,
    pub symbol: String,
    pub position_type: Side,
    pub entry_price: f64,
    pub exit_price: f64,
    pub entry_time: Option<DateTime<Utc>>,
    pub exit_time: DateTime<Utc>,
    pub entry_timestamp: i64,
    pub exit_timestamp: i64,
    pub peak_price: f64,
    pub stop_price: f64,
    pub max_pnl_pct: f64,
    pub min_pnl_pct: f64,
    pub exit_reason: String,
    pub is_stop_loss: bool,
    pub atr_at_entry: f64,
    pub trail_activated: bool,
    pub trading_mode: String,
    /// 开仓成交量（FILLED 回报 filled_quantity）
    #[serde(default)]
    pub entry_size: f64,
    /// 平仓成交量（本次平仓 fill 的 filled_quantity）
    #[serde(default)]
    pub exit_size: f64,
}

impl Model for HistoryPosition {
    const NAME: &'static str = "history_position";

    fn id(&self) -> &str {
        &self.position_id
    }

    fn is_terminal(&self) -> bool {
        // 每行都是一笔已完成成交，无任何后续状态转移
        true
    }
}

impl HistoryPosition {
    /// 方向价差（long: exit-entry；short: entry-exit）
    pub fn price_diff(&self) -> f64 {
        match self.position_type {
            Side::Long => self.exit_price - self.entry_price,
            Side::Short => self.entry_price - self.exit_price,
        }
    }

    pub fn pnl_pct(&self) -> f64 {
        if self.entry_price <= 0.0 {
            return 0.0;
        }
        self.price_diff() / self.entry_price * 100.0
    }

    pub fn duration_seconds(&self) -> i64 {
        self.exit_timestamp - self.entry_timestamp
    }

    fn to_row(&self) -> Vec<String> {
        vec![
            self.position_id.clone(),
            self.strategy_name.clone(),
            self.symbol.clone(),
            self.position_type.as_str().into(),
            fmt(self.entry_price),
            fmt(self.exit_price),
            self.entry_time.map(|t| t.to_rfc3339()).unwrap_or_default(),
            self.exit_time.to_rfc3339(),
            self.entry_timestamp.to_string(),
            self.exit_timestamp.to_string(),
            fmt(self.peak_price),
            fmt(self.stop_price),
            fmt(self.max_pnl_pct),
            fmt(self.min_pnl_pct),
            self.exit_reason.clone(),
            self.is_stop_loss.to_string(),
            fmt4(self.price_diff()),
            fmt4(self.pnl_pct()),
            self.duration_seconds().to_string(),
            fmt(self.atr_at_entry),
            self.trail_activated.to_string(),
            self.trading_mode.clone(),
            fmt(self.entry_size),
            fmt(self.exit_size),
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

fn fmt4(v: f64) -> String {
    format!("{v:.4}")
}

/// 对外固定列历史仓位写器：{base}/history_positions/{strategy_name}/{YYYYMMDD}.csv
pub struct HistoryPositionLogger {
    base: PathBuf,
}

impl HistoryPositionLogger {
    pub fn new(base: impl AsRef<Path>) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
        }
    }

    fn file_path(&self, strategy_name: &str, exit_time: DateTime<Utc>) -> PathBuf {
        self.base
            .join("history_positions")
            .join(strategy_name)
            .join(format!("{}.csv", exit_time.format("%Y%m%d")))
    }

    /// 追加一条平仓记录；文件不存在时先写 header。
    /// 失败返回 Err（由 manager 决定 warn 不阻断清除，对齐 Python 时序）。
    pub fn log_exit(&self, rec: &HistoryPosition) -> io::Result<()> {
        let path = self.file_path(&rec.strategy_name, rec.exit_time);
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
            position_id = %rec.position_id,
            pnl_pct = format!("{:.2}", rec.pnl_pct()),
            exit_reason = %rec.exit_reason,
            "历史仓位记录"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn rec(exit_time: DateTime<Utc>) -> HistoryPosition {
        HistoryPosition {
            position_id: "sig_1".into(),
            strategy_name: "sar_snt3_8H_3_BTCUSDT_LIVE".into(),
            symbol: "BTCUSDT".into(),
            position_type: Side::Long,
            entry_price: 100.0,
            exit_price: 103.0,
            entry_time: Some(Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()),
            exit_time,
            entry_timestamp: 1000,
            exit_timestamp: 4600,
            peak_price: 104.0,
            stop_price: 98.0,
            max_pnl_pct: 4.0,
            min_pnl_pct: -0.5,
            exit_reason: "trailing_stop".into(),
            is_stop_loss: false,
            atr_at_entry: 1.5,
            trail_activated: true,
            trading_mode: "backtest".into(),
            entry_size: 0.01,
            exit_size: 0.01,
        }
    }

    #[test]
    fn writes_header_once_and_computes_fields() {
        let dir = tempfile::tempdir().unwrap();
        let logger = HistoryPositionLogger::new(dir.path());
        let exit_time = Utc.with_ymd_and_hms(2026, 6, 11, 12, 0, 0).unwrap();

        logger.log_exit(&rec(exit_time)).unwrap();
        logger.log_exit(&rec(exit_time)).unwrap();

        let path = dir
            .path()
            .join("history_positions/sar_snt3_8H_3_BTCUSDT_LIVE/20260611.csv");
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3); // header + 2
        assert!(lines[0].starts_with("position_id,strategy_name"));
        assert!(lines[1].contains("3.0000")); // price_diff / pnl_pct
        assert!(lines[1].contains("3600")); // duration
        assert!(lines[1].contains("false")); // is_stop_loss
    }

    #[test]
    fn file_date_uses_exit_time_not_now() {
        let dir = tempfile::tempdir().unwrap();
        let logger = HistoryPositionLogger::new(dir.path());
        // 退出时间在"过去"，文件必须按退出时间命名（回测可复现）
        let exit_time = Utc.with_ymd_and_hms(2020, 1, 2, 0, 0, 0).unwrap();
        logger.log_exit(&rec(exit_time)).unwrap();
        assert!(dir
            .path()
            .join("history_positions/sar_snt3_8H_3_BTCUSDT_LIVE/20200102.csv")
            .exists());
    }

    #[test]
    fn short_price_diff() {
        let mut r = rec(Utc::now());
        r.position_type = Side::Short;
        r.entry_price = 100.0;
        r.exit_price = 97.0;
        assert!((r.price_diff() - 3.0).abs() < 1e-9);
        assert!((r.pnl_pct() - 3.0).abs() < 1e-9);
    }

    #[test]
    fn history_position_is_always_terminal() {
        assert!(rec(Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()).is_terminal());
    }
}
