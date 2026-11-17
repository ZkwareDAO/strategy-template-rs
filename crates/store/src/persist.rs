//! 持久化后端抽象 + CSV 实现。
//!
//! 设计类似 `ExecutionPort` 的端口/适配器模式：
//! - [`StorageBackend`] trait 定义存储后端接口（flush / recover / marker）；
//! - [`CsvStorage`] 是第一个实现（CSV 日快照），现状不变；
//! - 未来 sled 等后端只需实现同一 trait，`Store` 构造时注入即可切换。
//!
//! CSV 布局：`{base}/{YYYYMMDD}/{model}.csv`（UTC 日）。
//! 列：`id,deleted,created_at_ms,updated_at_ms,payload_json`。
//! payload_json 承载 model 全字段，新增字段只追加 JSON（向后兼容）。
//!
//! 写入：tmp + fsync + rename（原子覆盖，同日重写不追加）。
//! 读取：逐行容错，单行损坏（截断/坏数字/坏 JSON）只 warn 跳过，不毒死全表。

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::{Result, StoreError};

// ─── 通用数据载体（与存储格式无关） ─────────────────────────────────────────

/// 一行的持久化载体：业务主键 + 元数据 + JSON 序列化的 payload。
///
/// 与 `CsvRecord` 字段完全对齐，但语义上是**格式无关**的——CSV 后端
/// 将其编码为 CSV 行，sled 后端可以直接存 `id -> payload_bytes`。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistRecord {
    pub id: String,
    pub deleted: u8,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub payload_json: String,
}

/// flush 跨表一致性摘要（P2-10）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushTableDigest {
    pub name: String,
    pub row_count: usize,
    /// 所有 record 的 `id` 字段排序后拼接的 CRC32。
    pub id_crc32: u32,
}

/// flush marker（P2-10）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushMarker {
    pub flushed_at_ms: i64,
    pub tables: Vec<FlushTableDigest>,
}

// ─── 存储后端端口（类似 ExecutionPort） ────────────────────────────────────

/// 存储后端抽象接口。
///
/// `Store` 持有 `Arc<dyn StorageBackend>`，构造时注入具体实现。
/// 切换后端（CSV → sled 等）只需实现此 trait 并在 `Store::new` 时注入，
/// `ModelTable` 和 `Persistable` 的内存逻辑零改动。
///
/// 方法按职责分两组：
/// - **flush 组**：`write_table`（写单表快照）+ `write_marker`（写一致性标记）
/// - **recover 组**：`read_table`（读单表记录）+ `list_table_names`（列出已有表名）
///   + `read_marker`（读一致性标记）
pub trait StorageBackend: Send + Sync {
    /// 写单表快照（原子覆盖）。`day` 是 YYYYMMDD 格式的 UTC 日。
    fn write_table(&self, day: &str, table_name: &str, records: &[PersistRecord]) -> Result<()>;

    /// 读单表指定日期的全部记录。文件不存在返回空 Vec（非错误）。
    fn read_table(&self, day: &str, table_name: &str) -> Result<Vec<PersistRecord>>;

    /// 列出 base 目录下所有 YYYYMMDD 日目录名（升序）。
    fn list_days(&self) -> Result<Vec<String>>;

    /// 写 flush marker（P2-10 跨表一致性补偿）。
    fn write_marker(&self, day: &str, marker: &FlushMarker) -> Result<()>;

    /// 读 flush marker。不存在或损坏返回 None。
    fn read_marker(&self, day: &str) -> Option<FlushMarker>;

    /// base 目录路径（上层 `load_filled_open_from_disk` 等需要直接扫日目录时用）。
    fn base_path(&self) -> &Path;
}

// ─── CSV 后端实现 ──────────────────────────────────────────────────────────

/// CSV 日快照存储后端。
///
/// 布局：`{base}/{YYYYMMDD}/{model}.csv`（UTC 日）。
/// 每次写入是全量快照（tmp + fsync + rename 原子覆盖）。
pub struct CsvStorage {
    base: PathBuf,
}

impl CsvStorage {
    pub fn new(base: impl AsRef<Path>) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
        }
    }

    fn day_dir(&self, day: &str) -> PathBuf {
        self.base.join(day)
    }

    fn table_path(&self, day: &str, table_name: &str) -> PathBuf {
        self.day_dir(day).join(format!("{table_name}.csv"))
    }
}

impl StorageBackend for CsvStorage {
    fn write_table(&self, day: &str, table_name: &str, records: &[PersistRecord]) -> Result<()> {
        let path = self.table_path(day, table_name);
        write_day_csv(&path, records)
    }

    fn read_table(&self, day: &str, table_name: &str) -> Result<Vec<PersistRecord>> {
        let path = self.table_path(day, table_name);
        if !path.exists() {
            return Ok(Vec::new());
        }
        read_day_csv(&path)
    }

    fn list_days(&self) -> Result<Vec<String>> {
        let rd = match fs::read_dir(&self.base) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut days: Vec<String> = rd
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let path = e.path();
                let name = path.file_name()?.to_str()?.to_string();
                (path.is_dir() && name.len() == 8 && name.bytes().all(|b| b.is_ascii_digit()))
                    .then_some(name)
            })
            .collect();
        days.sort();
        Ok(days)
    }

    fn write_marker(&self, day: &str, marker: &FlushMarker) -> Result<()> {
        write_flush_marker(&self.day_dir(day), marker)
    }

    fn read_marker(&self, day: &str) -> Option<FlushMarker> {
        read_flush_marker(&self.day_dir(day))
    }

    fn base_path(&self) -> &Path {
        &self.base
    }
}

// ─── CSV 编码/解码（内部实现） ─────────────────────────────────────────────

pub(crate) const HEADER: &str = "id,deleted,created_at_ms,updated_at_ms,payload_json";

/// epoch 毫秒 → `YYYYMMDD`（UTC）。
pub(crate) fn day_str(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .expect("毫秒时间戳越界")
        .format("%Y%m%d")
        .to_string()
}

/// 原子写当日快照（tmp → fsync → rename 覆盖）。
pub(crate) fn write_day_csv(path: &Path, records: &[PersistRecord]) -> Result<()> {
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

fn encode_record(r: &PersistRecord) -> Result<Vec<u8>> {
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    wtr.serialize(r)?;
    wtr.flush()?;
    Ok(wtr.into_inner().map_err(csv::IntoInnerError::into_error)?)
}

/// 读一个日文件；坏行跳过（warn），返回所有成功解析的记录。
pub(crate) fn read_day_csv(path: &Path) -> Result<Vec<PersistRecord>> {
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

fn parse_record(rec: &csv::StringRecord) -> Option<PersistRecord> {
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
    Some(PersistRecord {
        id: rec.get(0)?.to_string(),
        deleted,
        created_at_ms: rec.get(2)?.parse::<i64>().ok()?,
        updated_at_ms: rec.get(3)?.parse::<i64>().ok()?,
        payload_json: payload_json.to_string(),
    })
}

// ─── P2-10：flush 跨表原子性补偿 ───────────────────────────────────────────
//
// flush 遍历各表逐个 `write_day_csv`——单表原子（tmp+fsync+rename），但跨表
// 非原子（进程在表 A 写完、表 B 未写时崩溃 → 恢复时 A 是最新、B 是昨日）。
// marker 文件记录各表行数 + CRC32 校验和，恢复时可检测不一致并告警。
// marker 本身也走 tmp+fsync+rename 原子写，放在日目录下 `{day}/.flush_marker`。
//
// `FlushTableDigest` / `FlushMarker` 类型定义已移至文件顶部通用区域。

const MARKER_FILE: &str = ".flush_marker";

/// 计算 record 集的 CRC32（id 排序后拼接）。
pub(crate) fn compute_id_crc32(records: &[PersistRecord]) -> u32 {
    let mut ids: Vec<&str> = records.iter().map(|r| r.id.as_str()).collect();
    ids.sort();
    let mut buf = String::new();
    for id in &ids {
        buf.push_str(id);
        buf.push('\n');
    }
    crc32(buf.as_bytes())
}

/// CRC32 (IEEE 802.3 polynomial)——纯实现无外部依赖。
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

/// 写 flush marker（原子：tmp + fsync + rename）。
pub(crate) fn write_flush_marker(day_dir: &Path, marker: &FlushMarker) -> Result<()> {
    let json = serde_json::to_string_pretty(marker)?;
    let tmp = day_dir.join(format!(".{MARKER_FILE}.{}.tmp", std::process::id()));
    {
        let file = File::create(&tmp)?;
        let mut buf = BufWriter::new(file);
        buf.write_all(json.as_bytes())?;
        buf.flush()?;
        let f = buf
            .into_inner()
            .map_err(std::io::IntoInnerError::into_error)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, day_dir.join(MARKER_FILE))?;
    Ok(())
}

/// 读 flush marker（不存在返回 None；损坏返回 None + warn）。
pub(crate) fn read_flush_marker(day_dir: &Path) -> Option<FlushMarker> {
    let path = day_dir.join(MARKER_FILE);
    let data = match fs::read_to_string(&path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "flush_marker 读取失败");
            return None;
        }
    };
    match serde_json::from_str::<FlushMarker>(&data) {
        Ok(m) => Some(m),
        Err(e) => {
            warn!(path = %path.display(), error = %e, "flush_marker 损坏，跳过一致性校验");
            None
        }
    }
}

/// 校验单张表的 flush 一致性：行数与 CRC32 是否与 marker 记录一致。
/// 返回 `Ok(())` 一致 / `Err(description)` 不一致（调用方告警不致命）。
pub(crate) fn verify_table_digest(
    records: &[PersistRecord],
    expected: &FlushTableDigest,
) -> std::result::Result<(), String> {
    if records.len() != expected.row_count {
        return Err(format!(
            "行数不一致：marker={} 实际={}",
            expected.row_count,
            records.len()
        ));
    }
    let actual_crc = compute_id_crc32(records);
    if actual_crc != expected.id_crc32 {
        return Err(format!(
            "CRC32 不一致：marker={:#010x} 实际={:#010x}",
            expected.id_crc32, actual_crc
        ));
    }
    Ok(())
}
