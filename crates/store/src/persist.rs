//! CSV 落盘/恢复（S2）。
//!
//! 布局：`{base}/{YYYYMMDD}/{model}.csv`（UTC 日）。
//! 列：`id,deleted,created_at_ms,updated_at_ms,payload_json`。
//! payload_json 承载 model 全字段，新增字段只追加 JSON（向后兼容）。
//!
//! 写入：tmp + fsync + rename（原子覆盖，同日重写不追加）。
//! 读取：逐行容错，单行损坏（截断/坏数字/坏 JSON）只 warn 跳过，不毒死全表。

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{Result, StoreError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CsvRecord {
    pub id: String,
    pub deleted: u8,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub payload_json: String,
}

pub(crate) const HEADER: &str = "id,deleted,created_at_ms,updated_at_ms,payload_json";

/// epoch 毫秒 → `YYYYMMDD`（UTC）。
pub(crate) fn day_str(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .expect("毫秒时间戳越界")
        .format("%Y%m%d")
        .to_string()
}

/// 原子写当日快照（tmp → fsync → rename 覆盖）。
pub(crate) fn write_day_csv(path: &Path, records: &[CsvRecord]) -> Result<()> {
    let dir = path
        .parent()
        .ok_or_else(|| StoreError::Io(std::io::Error::other("落盘路径无父目录")))?;
    fs::create_dir_all(dir)?;
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| StoreError::Io(std::io::Error::other("落盘文件名非法")))?;
    // tmp 名带进程唯一后缀：多进程共享同一 state 目录（run 多策略各一进程）
    // 时并发 flush 同一张表，固定 tmp 名会互踩（A rename 走 tmp → B rename ENOENT）。
    // rename 本身原子，唯一 tmp 名后并发写同一目标仍是"最后完成者胜"，安全。
    let tmp = dir.join(format!(".{file_name}.{}.tmp", std::process::id()));

    {
        let file = File::create(&tmp)?;
        let mut buf = BufWriter::new(file);
        writeln!(buf, "{HEADER}")?;
        for r in records {
            buf.write_all(&encode_record(r)?)?;
        }
        buf.flush()?;
        let file = buf
            .into_inner()
            .map_err(std::io::IntoInnerError::into_error)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

fn encode_record(r: &CsvRecord) -> Result<Vec<u8>> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    wtr.serialize(r)?;
    wtr.flush()?;
    Ok(wtr.into_inner().map_err(csv::IntoInnerError::into_error)?)
}

/// 读一个日文件；坏行跳过（warn），返回所有成功解析的记录。
pub fn read_day_csv(path: &Path) -> Result<Vec<CsvRecord>> {
    let file = File::open(path)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(file);
    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = match rec {
            Ok(r) => r,
            Err(e) => {
                warn!(path = %path.display(), error = %e, "日文件尾部损坏，停止解析剩余字节");
                break;
            }
        };
        match parse_record(&rec) {
            Some(r) => out.push(r),
            None => warn!(path = %path.display(), "跳过无法解析的 model 行"),
        }
    }
    Ok(out)
}

fn parse_record(rec: &csv::StringRecord) -> Option<CsvRecord> {
    if rec.len() != 5 {
        return None;
    }
    let deleted = rec.get(1)?.parse::<u8>().ok()?;
    if deleted > 1 {
        return None;
    }
    let payload_json = rec.get(4)?;
    // payload 必须是合法 JSON：引号不闭合的撕裂尾行会被 csv 解析器宽松返回，
    // 在此拦截（坏行只 warn 跳过，不毒死整表恢复）。形状不匹配留给泛型反序列化报错。
    if serde_json::from_str::<serde_json::Value>(payload_json).is_err() {
        return None;
    }
    Some(CsvRecord {
        id: rec.get(0)?.to_string(),
        deleted,
        created_at_ms: rec.get(2)?.parse::<i64>().ok()?,
        updated_at_ms: rec.get(3)?.parse::<i64>().ok()?,
        payload_json: payload_json.to_string(),
    })
}
