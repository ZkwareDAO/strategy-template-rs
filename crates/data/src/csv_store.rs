//! K 线 CSV 存取 — 命名空间布局（§6.1，阶段 5c run 合并步）
//!
//! 布局：`{base}/{tf}/{exchange}/{market}/{SYMBOL}_{tf}.csv`——路径键 =
//! `namespaced_key_of` 产出的 venue_key（`{exchange}/{market}/{symbol}`），
//! 同名裸挂牌代码跨交易所不再共享文件（§14-3 CSV 面）。
//! 传裸 symbol（无 `/`）回落旧布局 `{base}/{tf}/{SYMBOL}_{tf}.csv`，
//! `resolve_csv_path` 对旧布局存量文件做**一次 rename 迁移**。
//!
//! 列（10）：timestamp,open,high,low,close,volume,quote_volume,trade_num,
//! active_buy_volume,active_buy_quote_volume
//! timestamp：UTC，格式 `2025-12-24 00:00:00+00:00`（pandas 兼容）；
//! 桶时间语义 = open_time（左闭区间起点）。读取兼容旧 6 列文件（扩展列缺省 0）。
//!
//! 可选第 11 列 `trading_day`（`YYYY-MM-DD`）：交易所交易日（期货夜盘归次日、
//! crypto 为 UTC 自然日），在大周期 K 线导出时写入；旧文件/旧调用方
//! 无此列时读取为 None，10 列格式继续保持兼容。

use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};
use tracing::{debug, trace};

use strategy_core::candle::{Candle, Timeframe};

/// venue_key 拆段：3 段 = exchange/market/symbol；2 段 = exchange/symbol；
/// 无 `/` = 裸 symbol（回落旧布局，迁移期未升级的调用者零破坏）。
fn split_venue_key(venue_key: &str) -> (Option<&str>, Option<&str>, &str) {
    let mut segs = venue_key.rsplitn(3, '/');
    match (segs.next(), segs.next(), segs.next()) {
        (Some(sym), Some(mk), Some(ex)) => (Some(ex), Some(mk), sym),
        (Some(sym), Some(ex), None) => (Some(ex), None, sym),
        _ => (None, None, venue_key),
    }
}

/// `{base}/{tf}/{exchange}/{market}/{SYMBOL}_{tf}.csv`（§6.1 命名空间布局）。
/// `venue_key` = `namespaced_key_of(exchange, market, symbol)`；裸 symbol
/// 输入退化为旧布局 `{base}/{tf}/{SYMBOL}_{tf}.csv`。
pub fn csv_path(base: impl AsRef<Path>, venue_key: &str, tf: Timeframe) -> PathBuf {
    let tf_str = tf.to_string();
    let (exchange, market, symbol) = split_venue_key(venue_key);
    let mut p = base.as_ref().join(&tf_str);
    if let Some(ex) = exchange {
        p = p.join(ex);
    }
    if let Some(mk) = market {
        p = p.join(mk);
    }
    p.join(format!("{}_{}.csv", symbol.to_uppercase(), tf_str))
}

/// 旧（迁移前）布局：`{base}/{tf}/{SYMBOL}_{tf}.csv`——仅作迁移源与兼容钉。
pub fn legacy_csv_path(base: impl AsRef<Path>, symbol: &str, tf: Timeframe) -> PathBuf {
    let tf_str = tf.to_string();
    base.as_ref()
        .join(&tf_str)
        .join(format!("{}_{}.csv", symbol.to_uppercase(), tf_str))
}

/// 取命名空间路径；若新路径不存在而旧布局文件存在，则**一次 rename 迁移**
/// （零下载、零复制——「schema 一次迁完」在数据文件面的对应物）。
pub fn resolve_csv_path(base: impl AsRef<Path>, venue_key: &str, tf: Timeframe) -> PathBuf {
    let path = csv_path(&base, venue_key, tf);
    if path.exists() {
        return path;
    }
    let (_, _, symbol) = split_venue_key(venue_key);
    let legacy = legacy_csv_path(&base, symbol, tf);
    if legacy.exists() {
        if let Some(dir) = path.parent() {
            if let Err(e) = fs::create_dir_all(dir) {
                debug!(path = %dir.display(), error = %e, "CSV 迁移：创建命名空间目录失败，沿用旧路径");
                return legacy;
            }
        }
        match fs::rename(&legacy, &path) {
            Ok(()) => {
                trace!(old = %legacy.display(), new = %path.display(), "CSV 迁移：存量文件已改名至命名空间布局")
            }
            Err(e) => {
                debug!(old = %legacy.display(), error = %e, "CSV 迁移：rename 失败，本次沿用旧路径")
            }
        }
    }
    path
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

/// 读取 K 线 CSV（含可选 `trading_day` 列）；返回升序 `(Candle, 交易日)`。
/// 无该列或该行为空时为 None（旧 10 列文件 / crypto）。
pub fn load_csv_annotated(
    path: impl AsRef<Path>,
) -> io::Result<Vec<(Candle, Option<NaiveDate>)>> {
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
    let i_td = idx("trading_day");
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
        let trading_day = i_td
            .and_then(|i| rec.get(i))
            .filter(|s| !s.is_empty())
            .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
        out.push((
            Candle {
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
            },
            trading_day,
        ));
    }
    out.sort_by_key(|(c, _)| c.open_time);
    Ok(out)
}

/// 读取 K 线 CSV；返回升序 Candle 列表（is_closed 一律 true —— CSV 里的都是历史已闭合）
pub fn load_csv(path: impl AsRef<Path>) -> io::Result<Vec<Candle>> {
    Ok(load_csv_annotated(path)?
        .into_iter()
        .map(|(c, _)| c)
        .collect())
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

/// 写出带交易日标注的大周期 K 线 CSV。
///
/// `trading_days` 与 `candles` 等长；全为 None 时退化为 10 列旧格式
/// （crypto / 无交易日信息），任一有值时写 11 列表头（末列 `trading_day`）。
pub fn save_csv_with_trading_days(
    path: impl AsRef<Path>,
    candles: &[Candle],
    trading_days: &[Option<NaiveDate>],
) -> io::Result<()> {
    if candles.len() != trading_days.len() {
        return Err(io::Error::other(format!(
            "trading_day 长度 {} 与 K 线根数 {} 不一致",
            trading_days.len(),
            candles.len()
        )));
    }
    if trading_days.iter().all(Option::is_none) {
        return save_csv(path, candles);
    }
    let path = path.as_ref();
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("csv.tmp");
    {
        let mut wtr = csv::WriterBuilder::new().from_path(&tmp)?;
        wtr.write_record(HEADER_ANNOTATED)?;
        for (c, td) in candles.iter().zip(trading_days) {
            let mut row = record_of(c).to_vec();
            row.push(
                td.map(|d| d.format("%Y-%m-%d").to_string())
                    .unwrap_or_default(),
            );
            wtr.write_record(&row)?;
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

/// 读取（或初始化）某命名空间标的某周期的全部已闭合 K 线。
/// `venue_key` 为 §6.1 命名空间键（裸 symbol 兼容旧布局；旧布局存量经
/// `resolve_csv_path` 一次 rename 迁移）。
pub fn load_or_empty(
    base: impl AsRef<Path>,
    venue_key: &str,
    tf: Timeframe,
) -> io::Result<Vec<Candle>> {
    load_csv(resolve_csv_path(base, venue_key, tf))
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

/// 11 列表头：前 10 列同 [`HEADER`]，末列为交易所交易日。
const HEADER_ANNOTATED: [&str; 11] = [
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
    "trading_day",
];

/// 单根 K 线的 10 列字符串（与 save_csv/LiveCsvSink 共用）
fn record_of(c: &Candle) -> [String; 10] {    [
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
    fn annotated_roundtrip_writes_trading_day_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("CU_1d.csv");
        let tds = [
            Some(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
            Some(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap()),
        ];
        save_csv_with_trading_days(&path, &[candle(0, 100.0), candle(1, 101.0)], &tds)
            .unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(
            content.lines().next().unwrap().ends_with(",trading_day"),
            "有交易日时必须写第 11 列表头"
        );
        assert!(content.contains("2026-01-05"));
        let rows = load_csv_annotated(&path).unwrap();
        assert_eq!(rows[1].1, tds[1]);
        // 旧 API 不受新增列影响
        assert_eq!(load_csv(&path).unwrap().len(), 2);
    }

    #[test]
    fn annotated_all_none_degrades_to_legacy_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("BTC_1d.csv");
        save_csv_with_trading_days(&path, &[candle(0, 100.0)], &[None]).unwrap();
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(
            !content.lines().next().unwrap().contains("trading_day"),
            "全 None 保持 10 列旧格式"
        );
        assert!(load_csv_annotated(&path).unwrap()[0].1.is_none());
    }

    #[test]
    fn csv_path_layout() {
        // §6.1 命名空间布局：exchange/market 落目录段，文件名保留裸挂牌代码
        let p = csv_path(
            "/data/klines",
            "binance/crypto_perpetual/BTCUSDT",
            Timeframe::parse("8h").unwrap(),
        );
        assert_eq!(
            p,
            PathBuf::from("/data/klines/8h/binance/crypto_perpetual/BTCUSDT_8h.csv")
        );
        // 裸 symbol 退化 = 旧布局（迁移期未升级调用者零破坏）
        let bare = csv_path("/data/klines", "btcusdt", Timeframe::parse("8h").unwrap());
        assert_eq!(
            bare,
            legacy_csv_path("/data/klines", "btcusdt", Timeframe::parse("8h").unwrap())
        );
        assert_eq!(bare, PathBuf::from("/data/klines/8h/BTCUSDT_8h.csv"));
        // §14-3 CSV 面：同名裸 symbol 跨交易所不共享文件
        assert_ne!(
            csv_path("/d", "binance/crypto_perpetual/BTCUSDT", Timeframe::M1),
            csv_path("/d", "bybit/crypto_perpetual/BTCUSDT", Timeframe::M1)
        );
    }

    /// 迁移钉：旧布局存量文件经 resolve 一次 rename 进命名空间布局，
    /// 内容原样、旧位置不留副本（零下载、零复制）。
    #[test]
    fn resolve_csv_path_migrates_legacy_file() {
        let dir = tempfile::tempdir().unwrap();
        let old = legacy_csv_path(dir.path(), "BTCUSDT", Timeframe::M1);
        fs::create_dir_all(old.parent().unwrap()).unwrap();
        save_csv(&old, &[candle(0, 10.0), candle(1, 11.0)]).unwrap();

        let p = resolve_csv_path(
            dir.path(),
            "binance/crypto_perpetual/BTCUSDT",
            Timeframe::M1,
        );
        assert_eq!(
            p,
            csv_path(
                dir.path(),
                "binance/crypto_perpetual/BTCUSDT",
                Timeframe::M1
            )
        );
        assert!(p.exists(), "迁移后新路径存在");
        assert!(!old.exists(), "旧路径不残留（rename 非复制）");
        let loaded = load_csv(&p).unwrap();
        assert_eq!(loaded.len(), 2, "内容原样搬迁");
        assert_eq!(loaded[1].close, 11.0);

        // 幂等：再次 resolve 不再动文件
        let p2 = resolve_csv_path(
            dir.path(),
            "binance/crypto_perpetual/BTCUSDT",
            Timeframe::M1,
        );
        assert_eq!(p, p2);
        assert_eq!(load_csv(&p2).unwrap().len(), 2);

        // load_or_empty 走同一迁移路径
        let all = load_or_empty(
            dir.path(),
            "binance/crypto_perpetual/BTCUSDT",
            Timeframe::M1,
        )
        .unwrap();
        assert_eq!(all.len(), 2);
    }

    /// 跨所同名共存钉：两个交易所的同名裸 symbol 各写各文件互不影响。
    #[test]
    fn same_symbol_two_venues_do_not_share_files() {
        let dir = tempfile::tempdir().unwrap();
        let a = resolve_csv_path(
            dir.path(),
            "binance/crypto_perpetual/BTCUSDT",
            Timeframe::M1,
        );
        let b = resolve_csv_path(dir.path(), "bybit/crypto_perpetual/BTCUSDT", Timeframe::M1);
        fs::create_dir_all(a.parent().unwrap()).unwrap();
        fs::create_dir_all(b.parent().unwrap()).unwrap();
        save_csv(&a, &[candle(0, 10.0)]).unwrap();
        save_csv(&b, &[candle(0, 99.0)]).unwrap();
        assert_eq!(
            load_or_empty(
                dir.path(),
                "binance/crypto_perpetual/BTCUSDT",
                Timeframe::M1
            )
            .unwrap()[0]
                .close,
            10.0
        );
        assert_eq!(
            load_or_empty(dir.path(), "bybit/crypto_perpetual/BTCUSDT", Timeframe::M1).unwrap()[0]
                .close,
            99.0
        );
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
