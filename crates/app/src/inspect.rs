//! state 快照查看器 —— 把 payload_json 展开成可读形式
//!
//! 统一 Store 的 `state/{YYYYMMDD}/{model}.csv` 是机器导向的恢复格式
//! （`id,deleted,created_at_ms,updated_at_ms,payload_json`），域数据全部
//! 嵌在转义 JSON 里，终端直读困难。本模块只读不写：把 payload_json 展开
//! 为扁平列，支持对齐表格 / 纵向键值 / JSONL / 扁平 CSV 四种输出。
//!
//! 用法见 `strategy-rs inspect --help`。

use std::path::Path;

use chrono::{DateTime, Utc};

/// 列数阈值：展开列数 ≤ 该值用横向表格，否则用纵向键值块（终端可读）
const TABLE_COL_THRESHOLD: usize = 12;
/// 单元格显示封顶（字符数）
const CELL_MAX: usize = 32;

/// 一条已展开的快照行
pub struct FlatRow {
    pub id: String,
    pub deleted: bool,
    pub created_at: String,
    pub updated_at: String,
    /// 展开后的域字段（key=value，按首次出现保序）
    pub fields: Vec<(String, String)>,
}

/// 展开 JSON payload：嵌套对象以点号拼键（至多 2 层），标量转字符串。
fn flatten(v: &serde_json::Value, prefix: &str, depth: usize, out: &mut Vec<(String, String)>) {
    match v {
        serde_json::Value::Object(map) if depth < 2 => {
            for (k, val) in map {
                let key = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten(val, &key, depth + 1, out);
            }
        }
        serde_json::Value::Null => out.push((prefix.to_string(), "null".into())),
        serde_json::Value::String(s) => out.push((prefix.to_string(), s.clone())),
        other => out.push((prefix.to_string(), other.to_string())),
    }
}

/// epoch 毫秒 → "YYYY-MM-DD HH:MM:SS" UTC；非法值原样返回。
fn fmt_ms(raw: &str) -> String {
    raw.parse::<i64>()
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| raw.to_string())
}

/// 读取一个 state 快照 CSV（或任意扁平 CSV），返回展开后的行。
/// 自动识别：含 `payload_json` 列 = 快照格式，展开；否则原样保留列。
pub fn read_csv(path: &Path) -> anyhow::Result<Vec<FlatRow>> {
    let file = std::fs::File::open(path)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(file);
    let headers = rdr.headers()?.clone();
    let is_snapshot = headers.iter().any(|h| h == "payload_json");
    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        if is_snapshot {
            let Some(payload) = rec.get(4) else {
                continue;
            };
            let Ok(v) = serde_json::from_str::<serde_json::Value>(payload) else {
                continue;
            };
            let mut fields = Vec::new();
            flatten(&v, "", 0, &mut fields);
            out.push(FlatRow {
                id: rec.get(0).unwrap_or("").to_string(),
                deleted: rec.get(1).unwrap_or("0") == "1",
                created_at: fmt_ms(rec.get(2).unwrap_or("")),
                updated_at: fmt_ms(rec.get(3).unwrap_or("")),
                fields,
            });
        } else {
            let fields = headers
                .iter()
                .zip(rec.iter())
                .map(|(h, v)| (h.to_string(), v.to_string()))
                .collect();
            out.push(FlatRow {
                id: rec.get(0).unwrap_or("").to_string(),
                deleted: false,
                created_at: String::new(),
                updated_at: String::new(),
                fields,
            });
        }
    }
    Ok(out)
}

/// 列名并集（按首次出现顺序）
fn column_union(rows: &[FlatRow]) -> Vec<String> {
    let mut cols: Vec<String> = Vec::new();
    for r in rows {
        for (k, _) in &r.fields {
            if !cols.iter().any(|c| c == k) {
                cols.push(k.clone());
            }
        }
    }
    cols
}

fn take_rows<'a>(rows: &'a [FlatRow], limit: usize) -> Vec<&'a FlatRow> {
    if limit == 0 {
        rows.iter().collect()
    } else {
        rows.iter().take(limit).collect()
    }
}

fn truncated(s: &str) -> String {
    s.chars().take(CELL_MAX).collect()
}

/// 横向对齐表格（列数 ≤ 阈值时）
fn print_table(rows: &[FlatRow], limit: usize) {
    let cols = column_union(rows);
    let show = take_rows(rows, limit);
    if show.is_empty() {
        println!("（无数据）");
        return;
    }
    // 列宽 = max(表头, 各行值)，封顶 CELL_MAX；首列固定为 id
    let mut widths: Vec<usize> = vec!["id".chars().count()];
    widths.extend(cols.iter().map(|c| c.chars().count()));
    for r in &show {
        let id = if r.deleted {
            format!("{} [deleted]", r.id)
        } else {
            r.id.clone()
        };
        let w = id.chars().count().min(CELL_MAX);
        if w > widths[0] {
            widths[0] = w;
        }
        for (i, c) in cols.iter().enumerate() {
            let v = r
                .fields
                .iter()
                .find(|(k, _)| k == c)
                .map(|(_, v)| v.as_str())
                .unwrap_or("");
            let w = v.chars().count().min(CELL_MAX);
            if w > widths[i + 1] {
                widths[i + 1] = w;
            }
        }
    }
    let sep = widths
        .iter()
        .map(|w| "-".repeat(*w))
        .collect::<Vec<_>>()
        .join("  ");
    let mut header = vec![format!("{:<w$}", "id", w = widths[0])];
    header.extend(
        cols.iter()
            .enumerate()
            .map(|(i, c)| format!("{c:<w$}", w = widths[i + 1])),
    );
    println!("{}", header.join("  "));
    println!("{sep}");
    for r in &show {
        let id = if r.deleted {
            format!("{} [deleted]", r.id)
        } else {
            r.id.clone()
        };
        let mut cells = vec![format!("{:<w$}", truncated(&id), w = widths[0])];
        for (i, c) in cols.iter().enumerate() {
            let v = r
                .fields
                .iter()
                .find(|(k, _)| k == c)
                .map(|(_, v)| v.as_str())
                .unwrap_or("");
            cells.push(format!("{:<w$}", truncated(v), w = widths[i + 1]));
        }
        println!("{}", cells.join("  "));
    }
    if limit > 0 && rows.len() > limit {
        println!("… 共 {} 行，显示前 {limit} 行（--limit 0 看全部）", rows.len());
    }
}

/// 纵向键值块（列数 > 阈值时，每行一个记录块）
fn print_vertical(rows: &[FlatRow], limit: usize) {
    let show = take_rows(rows, limit);
    if show.is_empty() {
        println!("（无数据）");
        return;
    }
    for r in &show {
        let tag = if r.deleted { " [deleted]" } else { "" };
        println!("── {}{} ──", r.id, tag);
        let key_w = r
            .fields
            .iter()
            .map(|(k, _)| k.chars().count())
            .max()
            .unwrap_or(0)
            .max("created_at".chars().count());
        println!("  {:<kw$}  {}", "created_at", r.created_at, kw = key_w);
        println!("  {:<kw$}  {}", "updated_at", r.updated_at, kw = key_w);
        for (k, v) in &r.fields {
            println!("  {:<kw$}  {}", k, v, kw = key_w);
        }
        println!();
    }
    if limit > 0 && rows.len() > limit {
        println!("… 共 {} 行，显示前 {limit} 行（--limit 0 看全部）", rows.len());
    }
}

/// 按展开列数自动选择表格 / 纵向块打印
pub fn print(rows: &[FlatRow], limit: usize) {
    let cols = column_union(rows);
    if cols.len() <= TABLE_COL_THRESHOLD {
        print_table(rows, limit);
    } else {
        print_vertical(rows, limit);
    }
}

/// JSONL 输出（每行一条记录，域字段平铺）
pub fn print_jsonl(rows: &[FlatRow], limit: usize) {
    for r in take_rows(rows, limit) {
        let mut obj = serde_json::Map::new();
        obj.insert("id".into(), serde_json::Value::String(r.id.clone()));
        obj.insert("deleted".into(), serde_json::Value::Bool(r.deleted));
        obj.insert(
            "created_at".into(),
            serde_json::Value::String(r.created_at.clone()),
        );
        obj.insert(
            "updated_at".into(),
            serde_json::Value::String(r.updated_at.clone()),
        );
        for (k, v) in &r.fields {
            let val = if v == "null" {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(v.clone())
            };
            obj.insert(k.clone(), val);
        }
        println!("{}", serde_json::Value::Object(obj));
    }
}

/// 写出扁平 CSV（id,deleted,created_at,updated_at,<域列并集>）
pub fn write_flat_csv(path: &Path, rows: &[FlatRow]) -> anyhow::Result<()> {
    let cols = column_union(rows);
    let file = std::fs::File::create(path)?;
    let mut wtr = csv::Writer::from_writer(file);
    let mut header = vec![
        "id".to_string(),
        "deleted".to_string(),
        "created_at".to_string(),
        "updated_at".to_string(),
    ];
    header.extend(cols.iter().cloned());
    wtr.write_record(&header)?;
    for r in rows {
        let mut rec: Vec<String> = vec![
            r.id.clone(),
            if r.deleted { "1".into() } else { "0".into() },
            r.created_at.clone(),
            r.updated_at.clone(),
        ];
        for c in &cols {
            let v = r
                .fields
                .iter()
                .find(|(k, _)| k == c)
                .map(|(_, v)| v.clone())
                .unwrap_or_default();
            rec.push(v);
        }
        wtr.write_record(&rec)?;
    }
    wtr.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn snapshot_csv() -> Vec<u8> {
        let mut buf = Vec::new();
        writeln!(
            buf,
            "id,deleted,created_at_ms,updated_at_ms,payload_json"
        )
        .unwrap();
        writeln!(
            buf,
            "sig_1,0,1786953540000,1786953540000,\"{{\"\"kind\"\":\"\"open\"\",\"\"price\"\":100.0,\"\"intent\"\":{{\"\"direction\"\":\"\"long\"\"}}}}\""
        )
        .unwrap();
        buf
    }

    #[test]
    fn read_and_flatten_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("signal.csv");
        std::fs::write(&p, snapshot_csv()).unwrap();
        let rows = read_csv(&p).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "sig_1");
        assert!(!rows[0].deleted);
        assert_eq!(rows[0].created_at, "2026-08-17 07:59:00");
        let fields: Vec<&str> = rows[0].fields.iter().map(|(k, _)| k.as_str()).collect();
        assert!(fields.contains(&"kind"));
        assert!(fields.contains(&"price"));
        assert!(fields.contains(&"intent.direction"));
    }

    #[test]
    fn flat_csv_passthrough() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("flat.csv");
        std::fs::write(&p, "a,b\n1,2\n").unwrap();
        let rows = read_csv(&p).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fields, vec![("a".into(), "1".into()), ("b".into(), "2".into())]);
    }

    #[test]
    fn write_flat_csv_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("signal.csv");
        std::fs::write(&src, snapshot_csv()).unwrap();
        let rows = read_csv(&src).unwrap();
        let out = dir.path().join("flat.csv");
        write_flat_csv(&out, &rows).unwrap();
        let content = std::fs::read_to_string(&out).unwrap();
        assert!(content.starts_with("id,deleted,created_at,updated_at,"));
        assert!(content.contains("sig_1"));
        assert!(content.contains("intent.direction"));
    }
}
