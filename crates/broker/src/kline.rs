//! # broker::kline —— trading-service mock 的 1m K线数据源
//!
//! 从 `data/klines/1m` 目录（递归扫描 `*_1m.csv`）加载已定稿 bar，按
//! trading_service_rust 南向协议 v2 的 `kline_1m` 帧形（见
//! `trading-core/src/protocol.rs` 的 `KlineEvent`）序列化推送。
//! **只读镜像，不修改外部服务**；真实服务的 CTP 夜盘归属日等口径此处
//! 简化为 UTC 自然日（mock 用途足够，差异在 contracts/trading-service.md 标注）。
//!
//! CSV 列口径（与 `crates/data/csv_store` 一致）：
//! `timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume`

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeZone, Utc};
use serde_json::{json, Value};

/// 一根已定稿 1m bar（数值解析值 + 原 CSV 文本并存：出帧用文本保精度）。
#[derive(Debug, Clone, PartialEq)]
pub struct Bar {
    /// 文件名义 symbol（大写，如 `BTCUSDT` / `CU`）
    pub file_symbol: String,
    /// 交易所标识（由目录推得：`binance`→binance，`SHFE`→ctp；兜底 binance）
    pub venue: String,
    /// 桶开始时间（UTC 毫秒，含）
    pub start_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// 成交量（CSV 可能带小数，线协议按 i64 取整发出）
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_num: u64,
    pub active_buy_volume: f64,
    pub active_buy_quote_volume: f64,
    /// 原 CSV 行文本（出帧逐字用，避免 f64 往返丢精度）
    pub raw: BarRaw,
}

/// 出帧用的 CSV 原文（decimal_wire 字符串口径）。
#[derive(Debug, Clone, PartialEq)]
pub struct BarRaw {
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub quote_volume: String,
}

impl Bar {
    /// 归属交易日 YYYYMMDD（mock 口径：UTC 自然日；真实服务 CTP 夜盘归次日）。
    pub fn trading_day(&self) -> u32 {
        let dt: DateTime<Utc> = utc_ms(self.start_ms);
        dt.format("%Y%m%d").to_string().parse().unwrap_or(0)
    }

    /// 定稿 bar 载荷对象（`kline_1m.kline` / `kline_1m_batch.klines[]` /
    /// `kline_history.klines[]` 三处同形，协议 §10.3/§10.4）：12 公共字段 +
    /// binance extras；decimal 用 CSV 原文（对齐 decimal_wire），volume 整数。
    /// symbol 用**订阅方请求名**，对齐真实服务「CTP 如实为具体合约」的口径；
    /// venue 缺省用文件推得值。
    pub fn bar_value(&self, symbol: &str, venue: Option<&str>) -> Value {
        json!({
            "venue": venue.unwrap_or(&self.venue),
            "symbol": symbol,
            "interval": "1m",
            "trading_day": self.trading_day(),
            "start_time": self.start_ms,
            "end_time": self.start_ms + 60_000,
            "open": self.raw.open,
            "high": self.raw.high,
            "low": self.raw.low,
            "close": self.raw.close,
            "volume": self.volume.round().max(0.0) as i64,
            "quote_volume": self.raw.quote_volume,
            "trade_num": self.trade_num,
            "active_buy_volume": self.active_buy_volume,
            "active_buy_quote_volume": self.active_buy_quote_volume,
        })
    }

    /// 定稿 bar → `kline_1m` 单根推送帧（§10.3 兼容形态；真实服务端与 mock
    /// 泵均只发 `kline_1m_batch`，本形态留作脚本化测试与旧对端兼容）。
    pub fn frame(&self, symbol: &str, venue: Option<&str>) -> Value {
        json!({
            "type": "kline_1m",
            "kline": self.bar_value(symbol, venue)
        })
    }
}

fn utc_ms(ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .expect("毫秒 UTC 恒有效")
}

/// 解析 CSV 时间戳列（容忍 pandas `%Y-%m-%d %H:%M:%S%:z` 与 RFC3339）。
fn parse_ts_ms(s: &str) -> Option<i64> {
    let t = DateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S%:z")
        .or_else(|_| DateTime::parse_from_rfc3339(s.trim()))
        .ok()?;
    Some(t.timestamp_millis())
}

/// 递归找 `*_1m.csv`（深度 ≤4，防误配大目录）。
fn walk_1m(dir: &Path, depth: u8, out: &mut Vec<PathBuf>) {
    if depth > 4 {
        return;
    }
    let Ok(rd) = fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            walk_1m(&p, depth + 1, out);
        } else if p
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with("_1m.csv"))
        {
            out.push(p);
        }
    }
}

/// 目录路径推断 venue：`SHFE`→ctp，`binance`→binance，其余兜底 binance。
fn venue_of(path: &Path) -> String {
    let s = path.to_string_lossy().to_ascii_lowercase();
    if s.contains("shfe") {
        "ctp".to_string()
    } else {
        "binance".to_string()
    }
}

/// 解析单个 CSV 为升序 bar 列表；坏行跳过（宁缺勿断流）。
fn parse_csv(path: &Path) -> io::Result<Vec<Bar>> {
    let mut bars = Vec::new();
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)?;
    let headers: Vec<String> = rdr
        .headers()
        .map_err(io::Error::other)?
        .iter()
        .map(str::to_string)
        .collect();
    let col = |name: &str| headers.iter().position(|h| h == name);
    let (Some(i_ts), Some(i_o), Some(i_h), Some(i_l), Some(i_c), Some(i_v)) = (
        col("timestamp"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("volume"),
    ) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} 缺少必需列 timestamp/open/high/low/close/volume",
                path.display()
            ),
        ));
    };
    let i_qv = col("quote_volume");
    let i_tn = col("trade_num");
    let i_abv = col("active_buy_volume");
    let i_abq = col("active_buy_quote_volume");

    let file_symbol = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .trim_end_matches("_1m")
        .to_uppercase();
    let venue = venue_of(path);

    for rec in rdr.records() {
        let Ok(rec) = rec else { continue };
        let get = |i: Option<usize>| -> Option<&str> { i.and_then(|j| rec.get(j)).map(str::trim) };
        let Some(ts) = parse_ts_ms(rec.get(i_ts).unwrap_or("").trim()) else {
            continue;
        };
        let num = |i: Option<usize>, d: f64| get(i).and_then(|s| s.parse().ok()).unwrap_or(d);
        let raw = BarRaw {
            open: get(Some(i_o)).unwrap_or("0").to_string(),
            high: get(Some(i_h)).unwrap_or("0").to_string(),
            low: get(Some(i_l)).unwrap_or("0").to_string(),
            close: get(Some(i_c)).unwrap_or("0").to_string(),
            quote_volume: get(i_qv).unwrap_or("0").to_string(),
        };
        bars.push(Bar {
            file_symbol: file_symbol.clone(),
            venue: venue.clone(),
            start_ms: ts,
            open: num(Some(i_o), 0.0),
            high: num(Some(i_h), 0.0),
            low: num(Some(i_l), 0.0),
            close: num(Some(i_c), 0.0),
            volume: num(Some(i_v), 0.0),
            quote_volume: num(i_qv, 0.0),
            trade_num: num(i_tn, 0.0) as u64,
            active_buy_volume: num(i_abv, 0.0),
            active_buy_quote_volume: num(i_abq, 0.0),
            raw,
        });
    }
    bars.sort_by_key(|b| b.start_ms);
    Ok(bars)
}

/// K线库：symbol（大写）→ 升序全量 bar。
#[derive(Debug, Clone, Default)]
pub struct KlineBook {
    entries: HashMap<String, Vec<Bar>>,
}

impl KlineBook {
    /// 扫描目录加载全部 `*_1m.csv`；单文件解析失败收集为错误清单（调用方决定告警）。
    /// 返回（库, 错误清单）。
    pub fn load(dir: &Path) -> (Self, Vec<String>) {
        let mut files = Vec::new();
        walk_1m(dir, 0, &mut files);
        files.sort();
        let mut entries: HashMap<String, Vec<Bar>> = HashMap::new();
        let mut errors = Vec::new();
        for f in files {
            match parse_csv(&f) {
                Ok(bars) if !bars.is_empty() => {
                    let sym = bars[0].file_symbol.clone();
                    // 同 symbol 多文件：合并后按时间去重（保留先到）。
                    let e = entries.entry(sym).or_default();
                    for b in bars {
                        if e.last().is_none_or(|p| p.start_ms != b.start_ms) {
                            e.push(b);
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => errors.push(format!("{}: {e}", f.display())),
            }
        }
        for v in entries.values_mut() {
            v.sort_by_key(|b| b.start_ms);
        }
        (Self { entries }, errors)
    }

    /// 已加载 symbol 集合（大写文件名口径）。
    pub fn symbols(&self) -> Vec<&str> {
        self.entries.keys().map(String::as_str).collect()
    }

    /// 请求 symbol → 文件条目：精确（大小写不敏感）优先；否则
    /// 「文件符号是请求前缀」（CU2601→CU）或「请求是文件符号前缀」（cu→CU），
    /// 取最长匹配（主力连续/具体合约共用一份 CSV 的 mock 口径）。
    pub fn lookup(&self, req: &str) -> Option<(&String, &Vec<Bar>)> {
        let up = req.trim().to_uppercase();
        if let Some((k, v)) = self.entries.get_key_value(&up) {
            return Some((k, v));
        }
        let mut best: Option<(usize, &String, &Vec<Bar>)> = None;
        for (k, v) in &self.entries {
            let hit = if k.len() <= up.len() {
                up.starts_with(k.as_str())
            } else {
                k.starts_with(up.as_str())
            };
            if hit && best.is_none_or(|(bl, _, _)| k.len() > bl) {
                best = Some((k.len(), k, v));
            }
        }
        best.map(|(_, k, v)| (k, v))
    }

    /// 某请求 symbol 的 bar 序列起点索引（按 start_ms 过滤）。
    pub fn first_index_at_or_after(bars: &[Bar], start_ms: Option<i64>) -> usize {
        let Some(s) = start_ms else {
            return 0;
        };
        bars.partition_point(|b| b.start_ms < s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, rel: &str, rows: &str) {
        let p = dir.join(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(
            &p,
            format!(
                "timestamp,open,high,low,close,volume,quote_volume,trade_num,active_buy_volume,active_buy_quote_volume\n{rows}"
            ),
        )
        .unwrap();
    }

    #[test]
    fn loads_flat_and_nested_layouts_with_venue_inference() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "BNBUSDT_1m.csv",
            "2025-09-24 10:16:00+00:00,1027.61,1027.89,1026.14,1026.96,800.18,821925.3652,1221,373.83,383958.2679\n2025-09-24 10:15:00+00:00,1026,1027,1025,1027.6,10,10250,5,1,1025\n",
        );
        write(
            dir.path(),
            "SHFE/cn_futures/CU_1m.csv",
            "2025-12-01 01:00:00+00:00,89410,89440,89290,89440,2452,0,0,0,0\n",
        );
        let (book, errors) = KlineBook::load(dir.path());
        assert!(errors.is_empty(), "解析错误不应出现: {errors:?}");
        assert_eq!(book.symbols().len(), 2);
        // 升序
        let bn = &book.entries["BNBUSDT"];
        assert_eq!(bn.len(), 2);
        assert!(bn[0].start_ms < bn[1].start_ms, "必须按时间升序");
        assert_eq!(bn[0].venue, "binance");
        let cu = &book.entries["CU"];
        assert_eq!(cu[0].venue, "ctp", "SHFE 目录必须推为 ctp");
        assert_eq!(cu[0].close, 89440.0);
    }

    #[test]
    fn lookup_matches_exact_prefix_and_case() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "SHFE/cn_futures/CU_1m.csv",
            "2025-12-01 01:00:00+00:00,1,2,0.5,1.5,1,1,1,1,1\n",
        );
        write(
            dir.path(),
            "BTCUSDT_1m.csv",
            "2025-12-01 01:00:00+00:00,1,2,0.5,1.5,1,1,1,1,1\n",
        );
        let (book, _) = KlineBook::load(dir.path());
        // 精确（大小写不敏感）
        assert_eq!(book.lookup("btcusdt").unwrap().0, "BTCUSDT");
        // 具体合约 → 品种连续文件（前缀）
        assert_eq!(book.lookup("CU2601").unwrap().0, "CU");
        // 品种前缀请求也命中（cu → CU）
        assert_eq!(book.lookup("cu").unwrap().0, "CU");
        assert!(book.lookup("SOLUSDT").is_none());
    }

    #[test]
    fn frame_carries_production_wire_shape() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "BTCUSDT_1m.csv",
            "2025-12-01 13:00:00+00:00,89410,89440,89290,89440,2452,219348480,1234,1000,89000000\n",
        );
        let (book, _) = KlineBook::load(dir.path());
        let bars = &book.entries["BTCUSDT"];
        let f = bars[0].frame("BTCUSDT", Some("binance"));
        assert_eq!(f["type"], "kline_1m");
        let k = &f["kline"];
        assert_eq!(k["venue"], "binance");
        assert_eq!(k["symbol"], "BTCUSDT");
        assert_eq!(k["interval"], "1m");
        assert_eq!(k["trading_day"], 20251201);
        assert_eq!(
            k["start_time"].as_i64().unwrap(),
            parse_ts_ms("2025-12-01 13:00:00+00:00").unwrap()
        );
        assert_eq!(
            k["end_time"].as_i64().unwrap(),
            k["start_time"].as_i64().unwrap() + 60_000
        );
        // decimal 字符串口径（对齐 decimal_wire），volume 整数
        assert_eq!(k["open"], "89410");
        assert_eq!(k["close"], "89440");
        assert_eq!(k["volume"], 2452);
        assert_eq!(k["quote_volume"], "219348480");
        assert_eq!(k["trade_num"], 1234);
    }

    #[test]
    fn first_index_honors_start_filter() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "X_1m.csv",
            "2025-12-01 01:00:00+00:00,1,1,1,1,1,1,1,1,1\n2025-12-01 01:01:00+00:00,1,1,1,1,1,1,1,1,1\n2025-12-01 01:02:00+00:00,1,1,1,1,1,1,1,1,1\n",
        );
        let (book, _) = KlineBook::load(dir.path());
        let bars = &book.entries["X"];
        assert_eq!(KlineBook::first_index_at_or_after(bars, None), 0);
        assert_eq!(
            KlineBook::first_index_at_or_after(bars, Some(bars[1].start_ms)),
            1
        );
        assert_eq!(
            KlineBook::first_index_at_or_after(bars, Some(bars[2].start_ms + 1)),
            3
        );
    }
}
