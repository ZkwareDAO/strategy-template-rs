//! K 线 CSV 存取 — 布局镜像 Python `data/klines/{tf}/{SYMBOL}_{tf}.csv`
//!
//! 列（10）：timestamp,open,high,low,close,volume,quote_volume,trade_num,
//! active_buy_volume,active_buy_quote_volume
//! timestamp：UTC，格式 `2025-12-24 00:00:00+00:00`（pandas 兼容）；
//! 桶时间语义 = open_time（左闭区间起点）。读取兼容旧 6 列文件（扩展列缺省 0）。

use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use tracing::{debug, trace};

use strategy_core::candle::{Candle, Timeframe};

/// {base}/{tf}/{SYMBOL}_{tf}.csv（镜像 Python save_to_csv 的目录布局）
pub fn csv_path(base: impl AsRef<Path>, symbol: &str, tf: Timeframe) -> PathBuf {
    let tf_str = tf.to_string();
    base.as_ref()
        .join(&tf_str)
        .join(format!("{}_{}.csv", symbol.to_uppercase(), tf_str))
}

/// pandas 风格 UTC 时间戳：`2025-12-24 00:00:00+00:00`
pub fn fmt_ts(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S%:z").to_string()
}

/// 解析 pandas 风格时间戳（容忍 ISO8601 与 `%Y-%m-%d %H:%M:%S%:z`）
pub fn parse_ts(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(t) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%:z") {
        return Some(t.with_timezone(&Utc));
    }
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// 读取 K 线 CSV；返回升序 Candle 列表（is_closed 一律 true —— CSV 里的都是历史已闭合）
pub fn load_csv(path: impl AsRef<Path>) -> io::Result<Vec<Candle>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)?;
    let header: Vec<String> = rdr.headers()?.iter().map(String::from).collect();
    let idx = |name: &str| header.iter().position(|h| h == name);

    let (i_ts, i_o, i_h, i_l, i_c, i_v) = match (
        idx("timestamp").or_else(|| idx("open_time")),
        idx("open"),
        idx("high"),
        idx("low"),
        idx("close"),
        idx("volume"),
    ) {
        (Some(a), Some(b), Some(c), Some(d), Some(e), Some(f)) => (a, b, c, d, e, f),
        _ => {
            return Err(io::Error::other(format!(
                "CSV 列缺失: {path:?}（需要 timestamp/open/high/low/close/volume）"
            )))
        }
    };
    // 扩展列可选（旧 6 列文件缺失时按 0 读）
    let i_qv = idx("quote_volume");
    let i_tn = idx("trade_num");
    let i_abv = idx("active_buy_volume");
    let i_abqv = idx("active_buy_quote_volume");
    let num = |rec: &csv::StringRecord, i: Option<usize>| {
        i.and_then(|i| rec.get(i))
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0)
    };

    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        let ts = parse_ts(rec.get(i_ts).unwrap_or_default()).ok_or_else(|| {
            io::Error::other(format!("非法时间戳: {}", rec.get(i_ts).unwrap_or("")))
        })?;
        out.push(Candle {
            open_time: ts,
            open: rec.get(i_o).and_then(|v| v.parse().ok()).unwrap_or(0.0),
            high: rec.get(i_h).and_then(|v| v.parse().ok()).unwrap_or(0.0),
            low: rec.get(i_l).and_then(|v| v.parse().ok()).unwrap_or(0.0),
            close: rec.get(i_c).and_then(|v| v.parse().ok()).unwrap_or(0.0),
            volume: rec.get(i_v).and_then(|v| v.parse().ok()).unwrap_or(0.0),
            quote_volume: num(&rec, i_qv),
            trade_num: num(&rec, i_tn) as u64,
            active_buy_volume: num(&rec, i_abv),
            active_buy_quote_volume: num(&rec, i_abqv),
            is_closed: true,
        });
    }
    out.sort_by_key(|c| c.open_time);
    Ok(out)
}

/// 整体重写 K 线 CSV（按 open_time 升序；临时文件 + rename 防半写）
pub fn save_csv(path: impl AsRef<Path>, candles: &[Candle]) -> io::Result<()> {
    let path = path.as_ref();
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("csv.tmp");
    {
        let mut wtr = csv::WriterBuilder::new().from_path(&tmp)?;
        wtr.write_record(HEADER)?;
        for c in candles {
            wtr.write_record(record_of(c))?;
        }
        wtr.flush()?;
        wtr.into_inner().map_err(io::Error::other)?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

fn fmt_f64(v: f64) -> String {
    if v == v.trunc() && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// 读取（或初始化）某 symbol 某周期的全部已闭合 K 线
pub fn load_or_empty(
    base: impl AsRef<Path>,
    symbol: &str,
    tf: Timeframe,
) -> io::Result<Vec<Candle>> {
    load_csv(csv_path(base, symbol, tf))
}

const HEADER: [&str; 10] = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_num",
    "active_buy_volume",
    "active_buy_quote_volume",
];

/// 单根 K 线的 10 列字符串（与 save_csv/LiveCsvSink 共用）
fn record_of(c: &Candle) -> [String; 10] {
    [
        fmt_ts(c.open_time),
        fmt_f64(c.open),
        fmt_f64(c.high),
        fmt_f64(c.low),
        fmt_f64(c.close),
        fmt_f64(c.volume),
        fmt_f64(c.quote_volume),
        c.trade_num.to_string(),
        fmt_f64(c.active_buy_volume),
        fmt_f64(c.active_buy_quote_volume),
    ]
}

/// live 增量追加器：WS 闭合帧/重连补缺的 K 线按 open_time 单调追加，
/// 内存记住最后时间戳，旧帧/重复帧跳过（跨代重启从盘载末根初始化水位）。
pub struct LiveCsvSink {
    wtr: csv::Writer<BufWriter<fs::File>>,
    last_ts: Option<DateTime<Utc>>,
}

impl LiveCsvSink {
    /// 打开（或新建）追加器。新文件写表头；已存在文件以盘载末根为水位。
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let existed = path.exists();
        let last_ts = if existed {
            load_csv(path)?.last().map(|c| c.open_time)
        } else {
            None
        };
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let mut wtr = csv::WriterBuilder::new().from_writer(BufWriter::new(file));
        if !existed {
            wtr.write_record(HEADER)?;
            wtr.flush()?;
            debug!(path = %path.display(), "1m CSV 追加器：新文件已建并写表头");
        } else {
            debug!(path = %path.display(), watermark = ?last_ts, "1m CSV 追加器：已打开，以盘载末根为水位");
        }
        Ok(Self { wtr, last_ts })
    }

    /// 追加一根更新的 K 线；open_time 不晚于水位时跳过。
    /// 返回是否实际写入。
    pub fn push(&mut self, candle: &Candle) -> io::Result<bool> {
        if let Some(last) = self.last_ts {
            if candle.open_time <= last {
                trace!(bar_time = %candle.open_time, watermark = %last, "1m CSV 追加：不晚于水位，跳过（幂等去重）");
                return Ok(false);
            }
        }
        self.wtr.write_record(record_of(candle))?;
        self.wtr.flush()?;
        debug!(bar_time = %candle.open_time, close = candle.close, "1m CSV 已追加闭合 K 线");
        self.last_ts = Some(candle.open_time);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn candle(min: u32, close: f64) -> Candle {
        Candle {
            open_time: Utc.with_ymd_and_hms(2026, 6, 10, 0, min, 0).unwrap(),
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
            quote_volume: 0.0,
            trade_num: 0,
            active_buy_volume: 0.0,
            active_buy_quote_volume: 0.0,
            is_closed: true,
        }
    }

    #[test]
    fn roundtrip_matches_python_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("BTCUSDT_1m.csv");

        save_csv(&path, &[candle(0, 87463.1), candle(1, 87518.7)]).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(
            lines[0],
            "timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume"
        );
        assert_eq!(
            lines[1],
            "2026-06-10 00:00:00+00:00,87463.1,87463.1,87463.1,87463.1,1,0,0,0,0"
        );

        let loaded = load_csv(&path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[1].close, 87518.7);
        assert!(loaded[1].is_closed);
    }

    #[test]
    fn load_missing_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_csv(dir.path().join("nope.csv")).unwrap().is_empty());
    }

    #[test]
    fn csv_path_layout() {
        let p = csv_path("/data/klines", "btcusdt", Timeframe::parse("8h").unwrap());
        assert_eq!(p, PathBuf::from("/data/klines/8h/BTCUSDT_8h.csv"));
    }

    #[test]
    fn sink_new_file_writes_header_and_appends_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("BTCUSDT_1m.csv");
        let mut sink = LiveCsvSink::open(&path).unwrap();
        assert!(sink.push(&candle(0, 10.0)).unwrap());
        assert!(sink.push(&candle(1, 11.0)).unwrap());
        // 重复/更旧的时间戳跳过
        assert!(!sink.push(&candle(1, 11.0)).unwrap());
        assert!(!sink.push(&candle(0, 10.0)).unwrap());
        drop(sink);

        let loaded = load_csv(&path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[1].close, 11.0);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().lines().next().unwrap(),
            "timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume"
        );
    }

    #[test]
    fn sink_reopen_appends_without_dup_header_and_skips_existing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("BTCUSDT_1m.csv");
        save_csv(&path, &[candle(0, 10.0), candle(1, 11.0)]).unwrap();

        let mut sink = LiveCsvSink::open(&path).unwrap();
        assert!(!sink.push(&candle(1, 99.0)).unwrap(), "已存在时间戳不覆写");
        assert!(sink.push(&candle(2, 12.0)).unwrap());
        drop(sink);

        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            content.matches("timestamp,open").count(),
            1,
            "表头只出现一次"
        );
        let loaded = load_csv(&path).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[1].close, 11.0, "旧行不被覆写");
        assert_eq!(loaded[2].close, 12.0);
    }
}
