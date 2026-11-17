//! strategy-store —— 统一 model 内存与持久化门面（S1：内存内核）
//!
//! 设计来源 docs/archive/strategy-rs.md「内存和持久化管理」：
//! - 对外统一接口管理所有 model 的内存 CRUD 与 CSV 持久化；
//! - 每个 model 一张总表（读写算），表内每行一个子锁；
//! - 每行带 deleted（0/1）、created_at、updated_at（毫秒时间戳）。
//!
//! S2：`{base}/{YYYYMMDD}/{model}.csv` 定时快照 + 启动跨日去重恢复。
//!
//! ## 行生命周期（store 表内存 → 日文件）
//!
//! - 出生：`insert` / `upsert` 首次见到 id → `created_at = updated_at = now`；
//!   业务已终态时记 `terminal_at`（业务时间优先，缺省取 now）。
//! - 更新：`upsert` 命中已有行 → 保留 `created_at` / `terminal_at`，刷新
//!   `updated_at`；离开终态时清 `terminal_at`；复活软删行时清 `deleted_at`。
//! - 软删：`delete` → `deleted=1`、`deleted_at=now`，当日仍进快照。
//! - 裁剪：`flush` 每次按 UTC 日调 `purge_before_day`，归属日（终态行的
//!   `terminal_at`、软删行的 `deleted_at`）早于当日才物理删除；当日终态
//!   保留到次日，供当日快照审计。裁剪无墓碑，历史日文件不回改。
//! - 恢复：裁剪模式只读最新存在的日文件；`deleted` 行不入内存；终态行只
//!   保留最新日（当天）的，更早日期的终态不复活（不读旧日文件即结构保证）。
//! - 注意：裁剪后同一 id 若再被 `upsert`（表里已无该行）会按新行重建、
//!   `created_at` 重置为当时时钟；持有副本账本的上层（runner）必须在
//!   写表前自行过滤旧日终态。

pub mod persist;

// ─── 公共 API re-export ────────────────────────────────────────────────────
// PersistRecord / StorageBackend / CsvStorage 是 store 的公共 API：
// 上层（signals manager 等）通过 `strategy_store::PersistRecord` 直接引用，
// 无需深入 `persist` 模块路径。
pub use persist::{CsvStorage, PersistRecord, StorageBackend};
pub use persist::{FlushMarker, FlushTableDigest};

use std::any::Any;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

/// 毫秒时钟：回测注入 K 线时间（确定性），live 注入 [`EpochClock`]。
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

/// 墙钟实现（epoch 毫秒，UTC）。
pub struct EpochClock;

impl Clock for EpochClock {
    fn now_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis()
    }
}

/// 所有可入表 model 的约束。`NAME` 即落盘文件名 `{NAME}.csv`。
pub trait Model: Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
    const NAME: &'static str;
    /// 业务主键（同 model 内唯一）。
    fn id(&self) -> &str;
    /// 业务终态：无后续状态转移、且无挂起工作。归属日早于今日的终态行
    /// 在 flush 时物理裁剪（spec 2026-10-09-store-terminal-pruning §设计.1）。
    /// 默认 false = 永不裁剪，向后兼容。
    fn is_terminal(&self) -> bool {
        false
    }
    /// 终态发生时间（毫秒），裁剪归属日的判据；当日终态保留到次日。
    /// 默认 None = 由 `Stored` 在首次进入终态时按当时时钟记录。
    /// 有业务时间字段的 model（如平仓 `closed_at`）应显式实现：`updated_at_ms`
    /// 会被每轮 `upsert` 刷新，不能用它判归属日。
    fn terminal_at_ms(&self) -> Option<i64> {
        None
    }
    /// P1-7：共享表的二级索引键（如 `strategy_name`、`symbol`）。
    /// 返回 `Some` 的 model 在 `ModelTable` 中自动维护 `scope -> [id]` 索引，
    /// `by_scope()` 查询无需全表扫描。默认 `None` = 不索引。
    /// scope 不要求唯一——多行可共享同一 scope（如同一策略的多次交易）。
    fn scope(&self) -> Option<&str> {
        None
    }
}

/// 行级元数据（deleted/created_at/updated_at，毫秒）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowMeta {
    pub deleted: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("model {model} 行 {id} 已存在")]
    AlreadyExists { model: &'static str, id: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// 表内一行：业务数据 + 行级子锁保护的元数据。
pub(crate) struct Stored<T> {
    pub data: T,
    pub deleted: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// 首次进入终态的时间：裁剪归属日。业务字段优先，缺省取当时时钟。
    pub terminal_at_ms: Option<i64>,
    /// 软删时间：deleted=1 行的裁剪归属日。
    pub deleted_at_ms: Option<i64>,
}

/// 行句柄：总表持有的就是它，每行一把独立子锁。
pub(crate) type RowHandle<T> = Arc<Mutex<Stored<T>>>;

/// 新建活行元数据（未删除，created_at = updated_at = now）。
fn active_stored<T: Model>(data: T, now: i64) -> Stored<T> {
    let terminal_at_ms = data
        .is_terminal()
        .then(|| data.terminal_at_ms().unwrap_or(now));
    Stored {
        data,
        deleted: false,
        created_at_ms: now,
        updated_at_ms: now,
        terminal_at_ms,
        deleted_at_ms: None,
    }
}

/// 一个 model 的总表：总表 RwLock 索引行句柄，每行一把独立子锁。
///
/// 锁纪律：子锁只在同步闭包期持有，**严禁跨 await**；外部读到的永远是克隆值。
///
/// P1-7：`scope_index` 是二级索引（`scope -> {id}`），仅在 `Model::scope()`
/// 返回 `Some` 时维护。`by_scope()` 利用此索引避免全表扫描——共享表
/// （多策略同表）行数增长时查询性能从 O(n) → O(k)，k = 该 scope 下的行数。
pub struct ModelTable<T: Model> {
    model: &'static str,
    rows: RwLock<HashMap<String, RowHandle<T>>>,
    /// P1-7：scope -> {id} 二级索引。scope 为 None 的行不进索引。
    scope_index: RwLock<HashMap<String, Vec<String>>>,
    clock: Arc<dyn Clock>,
}

impl<T: Model> ModelTable<T> {
    fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            model: T::NAME,
            rows: RwLock::new(HashMap::new()),
            scope_index: RwLock::new(HashMap::new()),
            clock,
        }
    }

    /// 取行句柄后立即释放总表读锁，避免持总表锁等子锁。
    fn row_handle(&self, id: &str) -> Option<RowHandle<T>> {
        self.rows.read().get(id).cloned()
    }

    /// P1-7：将 (id, scope) 加入二级索引。调用方需持有 rows 写锁或保证
    /// 无并发写入（insert/upsert/load_merged 内调用）。
    fn index_scope(&self, id: &str, scope: Option<&str>) {
        if let Some(s) = scope {
            self.scope_index
                .write()
                .entry(s.to_string())
                .or_default()
                .push(id.to_string());
        }
    }

    /// 新建一行（created_at = updated_at = now）；主键冲突报错且不覆盖。
    pub fn insert(&self, data: T) -> Result<()> {
        let id = data.id().to_string();
        let scope = data.scope().map(|s| s.to_string());
        let mut rows = self.rows.write();
        if rows.contains_key(&id) {
            return Err(StoreError::AlreadyExists {
                model: self.model,
                id,
            });
        }
        let now = self.clock.now_ms();
        rows.insert(id.clone(), Arc::new(Mutex::new(active_stored(data, now))));
        // P1-7：维护二级索引（scope 已在上游提取，此处传引用避免移动问题）。
        self.index_scope(&id, scope.as_deref());
        Ok(())
    }

    /// 插入或整行替换；保留 created_at，刷新 updated_at。
    /// 命中同日软删行时**复活**（deleted 复位为 false）。全程在写锁内判定，
    /// 杜绝 check-then-insert 竞态。
    pub fn upsert(&self, data: T) {
        use std::collections::hash_map::Entry;
        let now = self.clock.now_ms();
        let id = data.id().to_string();
        let scope = data.scope().map(|s| s.to_string());
        let is_new = {
            let mut rows = self.rows.write();
            match rows.entry(id.clone()) {
                Entry::Vacant(e) => {
                    e.insert(Arc::new(Mutex::new(active_stored(data, now))));
                    true
                }
                Entry::Occupied(e) => {
                    let handle = Arc::clone(e.get());
                    let mut row = handle.lock();
                    // 归属日取业务时间；没有业务字段时按首次进入终态的时钟记。
                    // 不随每轮 upsert 刷新，否则「当天平仓」永远判在今天。
                    let was_terminal = row.data.is_terminal();
                    let terminal_at_ms = if data.is_terminal() {
                        data.terminal_at_ms().or(if was_terminal {
                            row.terminal_at_ms
                        } else {
                            Some(now)
                        })
                    } else {
                        None
                    };
                    row.data = data;
                    row.terminal_at_ms = terminal_at_ms;
                    row.deleted = false; // 复活同日软删行
                    row.deleted_at_ms = None;
                    row.updated_at_ms = now;
                    false
                }
            }
        };
        // P1-7：仅新行加入索引（已存在行的 scope 在 insert 时已索引，
        // upsert 不改 id 也不改 scope——如果 scope 变了则索引会有微量陈旧，
        // 但 scope 作为业务分区键通常不变，可接受）。
        if is_new {
            self.index_scope(&id, scope.as_deref());
        }
    }

    /// 读一个未删除行（返回克隆，外部修改不影响表内）。
    pub fn get(&self, id: &str) -> Option<T> {
        let handle = self.row_handle(id)?;
        let row = handle.lock();
        (!row.deleted).then(|| row.data.clone())
    }

    /// 行元数据（含已删除行，恢复/审计用）。
    pub fn meta(&self, id: &str) -> Option<RowMeta> {
        let handle = self.row_handle(id)?;
        let row = handle.lock();
        Some(RowMeta {
            deleted: row.deleted,
            created_at_ms: row.created_at_ms,
            updated_at_ms: row.updated_at_ms,
        })
    }

    /// 全部未删除行（克隆）。
    /// 先 clone 行句柄释放表读锁，再逐行加锁克隆数据——避免持表锁期间逐行
    /// 加锁阻塞写者。
    pub fn list(&self) -> Vec<T> {
        let handles: Vec<RowHandle<T>> = self.rows.read().values().cloned().collect();
        handles
            .into_iter()
            .filter_map(|h| {
                let row = h.lock();
                (!row.deleted).then(|| row.data.clone())
            })
            .collect()
    }

    /// P1-7：按 scope 查询未删除行（克隆）。
    /// 利用二级索引 `scope -> {id}` 直接定位，无需全表扫描。
    /// scope 不存在或 model 未实现 `scope()` 返回空 Vec。
    /// 先 clone id 列表释放索引读锁，再逐行取句柄+加锁——锁纪律与 `list()` 一致。
    pub fn by_scope(&self, scope: &str) -> Vec<T> {
        let ids: Vec<String> = {
            let idx = self.scope_index.read();
            idx.get(scope).cloned().unwrap_or_default()
        };
        let rows = self.rows.read();
        ids.into_iter()
            .filter_map(|id| {
                let handle = rows.get(&id)?;
                let row = handle.lock();
                (!row.deleted).then(|| row.data.clone())
            })
            .collect()
    }

    /// 软删除（deleted=1，刷新 updated_at）。返回本次是否实际删除。
    pub fn delete(&self, id: &str) -> bool {
        let handle = match self.row_handle(id) {
            Some(h) => h,
            None => return false,
        };
        let mut row = handle.lock();
        if row.deleted {
            return false;
        }
        row.deleted = true;
        let now = self.clock.now_ms();
        row.deleted_at_ms = Some(now);
        row.updated_at_ms = now;
        true
    }

    /// 读写算：在该行子锁下同步执行闭包，返回闭包结果；行不存在/已删除返回 None。
    /// 闭包内禁止 await、禁止调用网关。
    pub fn with<R>(&self, id: &str, f: impl FnOnce(&mut T) -> R) -> Option<R> {
        let handle = self.row_handle(id)?;
        let mut row = handle.lock();
        if row.deleted {
            return None;
        }
        let result = f(&mut row.data);
        row.updated_at_ms = self.clock.now_ms();
        Some(result)
    }
}

/// 类型擦除的表接口：Store 统一调度落盘/跨日清 deleted/恢复，不关心行类型。
///
/// 方法与存储格式无关——`snapshot()` 产出通用 `PersistRecord`，
/// `load_merged()` 消费通用 `PersistRecord`。具体的 CSV/sled 编码
/// 由 `StorageBackend` 实现承担。
trait Persistable: Any + Send + Sync {
    fn name(&self) -> &'static str;
    /// 全表快照（格式无关的 `PersistRecord`）。
    fn snapshot(&self) -> Result<Vec<PersistRecord>>;
    /// 物理清除归属日早于 `day` 的终态行（软删行按删除时间、业务终态行按
    /// 终态时间）。当日终态必须保留——它是当日快照的审计内容；更早的终态
    /// 已在它们自己的日文件里，留着只会让 `persist_round` 把它们写回当日。
    /// 判不出归期的行保守保留。
    fn purge_before_day(&self, day: &str) -> usize;
    /// 启动恢复：载入去重后的记录。`deleted` 行不入活表；终态行只在
    /// `latest_day` 当天时保留（内存语义是「当天终态 + 活行」，重启不能把
    /// 当天已平/已成交的行丢掉），更早日期的终态不复活。
    fn load_merged(
        &self,
        merged: HashMap<String, PersistRecord>,
        prune_enabled: bool,
        latest_day: Option<&str>,
    ) -> Result<usize>;
    fn into_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

impl<T: Model> Persistable for ModelTable<T> {
    fn name(&self) -> &'static str {
        self.model
    }

    fn snapshot(&self) -> Result<Vec<PersistRecord>> {
        // 先 clone 行句柄 + id 释放表读锁，再逐行加锁序列化——避免持表锁
        // 期间逐行加锁阻塞写者（flush 在 spawn_blocking 中调用，但仍可能
        // 与同表的其他写者竞争）。
        let entries: Vec<(String, RowHandle<T>)> = {
            let rows = self.rows.read();
            rows.iter()
                .map(|(id, h)| (id.clone(), Arc::clone(h)))
                .collect()
        };
        let mut ids: Vec<&str> = entries.iter().map(|(id, _)| id.as_str()).collect();
        ids.sort();
        // 活行与墓碑共用同一组落盘列，仅 deleted 取值与数据源不同。
        let encode = |g: &Stored<T>| -> Result<(u8, i64, i64, String)> {
            Ok((
                u8::from(g.deleted),
                g.created_at_ms,
                g.updated_at_ms,
                serde_json::to_string(&g.data)?,
            ))
        };
        ids.into_iter()
            .map(|id| -> Result<PersistRecord> {
                let handle = entries
                    .iter()
                    .find(|(eid, _)| eid == id)
                    .expect("id 必在")
                    .1
                    .clone();
                let g = handle.lock();
                let (deleted, created_at_ms, updated_at_ms, payload_json) = encode(&g)?;
                Ok(PersistRecord {
                    id: id.to_string(),
                    deleted,
                    created_at_ms,
                    updated_at_ms,
                    payload_json,
                })
            })
            .collect()
    }

    fn purge_before_day(&self, day: &str) -> usize {
        let mut rows = self.rows.write();
        let before = rows.len();
        // P1-7：收集被物理删除的 id，用于清理 scope_index。
        let mut purged_ids: Vec<String> = Vec::new();
        // 物理丢弃，不再进墓碑（墓碑随本设计整体删除）。活行判据：
        // 未软删 且 业务未终态 或 归属日不早于 day。
        rows.retain(|id, h| {
            let g = h.lock();
            let before_day = |t: Option<i64>| t.is_some_and(|t| persist::day_str(t).as_str() < day);
            let stale = if g.deleted {
                before_day(g.deleted_at_ms)
            } else if g.data.is_terminal() {
                before_day(g.terminal_at_ms)
            } else {
                false
            };
            if stale {
                purged_ids.push(id.clone());
            }
            !stale
        });
        // P1-7：清理被 purge 行的 scope 索引条目。
        if !purged_ids.is_empty() {
            let mut idx = self.scope_index.write();
            for ids in idx.values_mut() {
                ids.retain(|id| !purged_ids.contains(id));
            }
            idx.retain(|_, ids| !ids.is_empty());
        }
        before - rows.len()
    }

    fn load_merged(
        &self,
        merged: HashMap<String, PersistRecord>,
        prune_enabled: bool,
        latest_day: Option<&str>,
    ) -> Result<usize> {
        let mut rows = self.rows.write();
        let mut loaded = 0;
        for rec in merged.into_values() {
            // 活表现有 id（含未 flush 的软删行）永远优先于磁盘：同进程内存是真相，
            // 监督重建/兄弟实例不得被旧快照回滚或复活。冷启动空表时这里恒为缺失。
            if rows.contains_key(&rec.id) {
                continue;
            }
            let data: T = match serde_json::from_str(&rec.payload_json) {
                Ok(data) => data,
                Err(e) => {
                    // 形状不匹配与其他坏行一致：warn 跳过，不毒死整表恢复
                    // （fail-closed 收紧见后续 P1-3）。
                    tracing::warn!(
                        model = self.model,
                        id = %rec.id,
                        error = %e,
                        "跳过 payload 形状不匹配的盘载行"
                    );
                    continue;
                }
            };
            let deleted = rec.deleted == 1;
            // 终态盘载行：最新日（当天）的保留——「当天终态 + 活行」是内存
            // 快照的完整内容，重启后当天已平/已成交的行仍在；更早日期的
            // 终态只在各自日文件里，恢复最新日即可保证不复活。
            if prune_enabled && data.is_terminal() {
                let terminal_day =
                    persist::day_str(data.terminal_at_ms().unwrap_or(rec.updated_at_ms));
                if latest_day.is_some_and(|latest| terminal_day.as_str() < latest) {
                    continue;
                }
            }
            if deleted {
                // 软删盘载行不入活表（原先进墓碑防跨日复活；现在
                // recover 单日化后防复活由「不读旧日文件」结构保证，
                // 兼容模式多日合并取最新后软删行本就不入活表，语义等价）
                continue;
            }
            // P1-7：重建 scope 二级索引（恢复时整表重建，无并发写）。
            let scope = data.scope().map(|s| s.to_string());
            // 终态时间取业务字段；旧快照缺字段时回退到落盘时的 updated_at，
            // 保证重启后裁剪判据仍有归期（否则这些行会永久滞留内存）。
            let terminal_at_ms = data
                .is_terminal()
                .then(|| data.terminal_at_ms().unwrap_or(rec.updated_at_ms));
            rows.insert(
                rec.id.clone(),
                Arc::new(Mutex::new(Stored {
                    data,
                    deleted: false,
                    created_at_ms: rec.created_at_ms,
                    updated_at_ms: rec.updated_at_ms,
                    terminal_at_ms,
                    deleted_at_ms: None,
                })),
            );
            // 索引在 rows 写锁内重建——scope_index 用独立锁，此时无并发
            // 写入（recover 是启动期单线程），安全。
            if let Some(ref s) = scope {
                self.scope_index
                    .write()
                    .entry(s.clone())
                    .or_default()
                    .push(rec.id.clone());
            }
            loaded += 1;
        }
        Ok(loaded)
    }

    fn into_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }
}

/// 统一门面：按 model 类型持有总表；定时 flush 落盘，启动 recover 恢复。
///
/// 存储后端通过 `StorageBackend` trait 注入（类似 `ExecutionPort`）——
/// 默认 `CsvStorage`，未来 sled 等只需实现同一 trait 并在构造时注入。
///
/// P2-11：连接注册表采用类型化泛型 `register_connection::<T>()`，
/// 替代 `dyn Any` 擦除——编译期保证类型安全，取错类型编译报错而非运行时 None。
pub struct Store {
    /// 存储后端（端口注入）。CSV 是默认实现；切换 sled 只需换一个 impl。
    backend: Arc<dyn StorageBackend>,
    clock: Arc<dyn Clock>,
    tables: RwLock<HashMap<&'static str, Arc<dyn Persistable>>>,
    /// 上次落盘的 UTC 日（YYYYMMDD）；跨日触发终态裁剪。
    last_flush_day: Mutex<Option<String>>,
    /// 跨日裁剪策略（spec §设计.7）：true=裁终态行+单日 recover；
    /// false=旧行为（多日 recover），线上回滚开关。
    prune_enabled: bool,
    /// P2-11：类型安全的连接注册表。
    /// key = `TypeId::of::<T>()`，value = `Arc<T>` 擦除为 `Arc<dyn Any + Send + Sync>`。
    /// 取出时 `downcast_ref::<T>()` 保证编译期类型安全（泛型 T 在调用点已知）。
    connections: RwLock<HashMap<std::any::TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl Store {
    /// 默认 CSV 后端构造（向后兼容）。
    pub fn new(base: impl AsRef<Path>, clock: Arc<dyn Clock>) -> Self {
        Self::with_backend(Arc::new(CsvStorage::new(base)), clock, true)
    }

    /// 默认 CSV 后端 + 裁剪策略。
    pub fn new_with_policy(
        base: impl AsRef<Path>,
        clock: Arc<dyn Clock>,
        prune_enabled: bool,
    ) -> Self {
        Self::with_backend(Arc::new(CsvStorage::new(base)), clock, prune_enabled)
    }

    /// 注入自定义存储后端构造（切换 sled 等的入口）。
    pub fn with_backend(
        backend: Arc<dyn StorageBackend>,
        clock: Arc<dyn Clock>,
        prune_enabled: bool,
    ) -> Self {
        Self {
            backend,
            clock,
            tables: RwLock::new(HashMap::new()),
            last_flush_day: Mutex::new(None),
            prune_enabled,
            connections: RwLock::new(HashMap::new()),
        }
    }

    /// 状态根目录（`StorageBackend::base_path()` 的便捷出口）。
    /// 仅供需要对日文件做定向读取的上层（如 signals 启动交叉对账）使用。
    pub fn base(&self) -> &std::path::Path {
        self.backend.base_path()
    }

    /// 同一 model 类型在同一 Store 内永远拿到同一张表。
    pub fn table<T: Model>(&self) -> Arc<ModelTable<T>> {
        if let Some(t) = self.tables.read().get(T::NAME) {
            return downcast::<T>(Arc::clone(t));
        }
        let mut tables = self.tables.write();
        let table: Arc<dyn Persistable> = Arc::new(ModelTable::<T>::new(Arc::clone(&self.clock)));
        let entry = Arc::clone(tables.entry(T::NAME).or_insert(table));
        downcast::<T>(entry)
    }

    /// 通过存储后端读取指定日期、指定表的全部原始记录。
    /// 供上层（如 `SignalManager::load_filled_open_from_disk`）直接扫描历史日
    /// 文件用——不经 `load_merged` 的 is_terminal 过滤，原始数据直达。
    pub fn read_table_raw(&self, day: &str, table_name: &str) -> Result<Vec<PersistRecord>> {
        self.backend.read_table(day, table_name)
    }

    /// 通过存储后端列出所有日目录名（升序）。
    pub fn list_days(&self) -> Result<Vec<String>> {
        self.backend.list_days()
    }

    /// P2-11：注册一个共享连接对象（如 WS 连接、gRPC client）。
    /// 同一类型 `T` 在同一 Store 内只保留最后一次注册。
    /// 取出时用 [`Self::connection::<T>()`]——泛型保证编译期类型安全。
    pub fn register_connection<T: Send + Sync + 'static>(&self, conn: Arc<T>) {
        let type_id = std::any::TypeId::of::<T>();
        self.connections
            .write()
            .insert(type_id, conn as Arc<dyn Any + Send + Sync>);
    }

    /// P2-11：按类型取出已注册的连接对象。
    /// 未注册返回 `None`——调用方按需 `.expect()` 或 fallback。
    pub fn connection<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let type_id = std::any::TypeId::of::<T>();
        let conns = self.connections.read();
        conns
            .get(&type_id)
            .and_then(|arc| arc.downcast_ref::<Arc<T>>())
            .cloned()
    }

    /// 当前时钟的 UTC 日（`YYYYMMDD`），与 `flush` 落盘同口径。
    /// 上层（runner 账本）按日过滤持久化时必须用它：两边日界判定错开
    /// 会让旧终态在跨日瞬间漏网或提前被写回。
    pub fn current_day(&self) -> String {
        persist::day_str(self.clock.now_ms())
    }

    /// 把所有已注册表的当前内存快照通过 `StorageBackend` 写入持久化层；
    /// 每次 flush 先按日裁掉归属日早于今日的终态行。
    pub fn flush(&self) -> Result<()> {
        let mut last = self.last_flush_day.lock();
        let today = persist::day_str(self.clock.now_ms());
        // 单调守卫：仅当日严格前进才写新日目录；时钟回拨时继续写入最后日期，
        // 避免向历史目录覆写全量快照或提前清扫终态。
        let (day, _rollover) = match last.as_deref() {
            None => (today, false),
            Some(prev) if today.as_str() > prev => (today, true),
            Some(prev) => (prev.to_string(), false),
        };

        let tables: Vec<Arc<dyn Persistable>> = {
            let reg = self.tables.read();
            let mut pairs: Vec<(&&'static str, &Arc<dyn Persistable>)> = reg.iter().collect();
            pairs.sort_by_key(|(name, _)| *name);
            pairs.into_iter().map(|(_, v)| v.clone()).collect()
        };
        // P2-10：收集各表摘要，flush 末尾写 marker（跨表原子性补偿）。
        // 表按 name 排序遍历——marker 中 tables 数组顺序确定性（回测字节级复现）。
        let mut digests: Vec<persist::FlushTableDigest> = Vec::with_capacity(tables.len());
        for t in &tables {
            // 每次落盘都按日裁剪（幂等）：重启后 `last_flush_day` 为空，
            // 只在 rollover 裁会漏掉停机期间积压的旧日终态。
            if self.prune_enabled {
                t.purge_before_day(&day);
            }
            let records = t.snapshot()?;
            let name = t.name().to_string();
            let row_count = records.len();
            let id_crc32 = persist::compute_id_crc32(&records);
            digests.push(persist::FlushTableDigest {
                name,
                row_count,
                id_crc32,
            });
            self.backend.write_table(&day, t.name(), &records)?;
        }
        // P2-10：写 flush marker——恢复时可检测跨表不一致。
        let marker = persist::FlushMarker {
            flushed_at_ms: self.clock.now_ms(),
            tables: digests,
        };
        if let Err(e) = self.backend.write_marker(&day, &marker) {
            // marker 写失败不致命——核心数据已落盘，仅告警。
            tracing::warn!(error = %e, day = %day, "flush_marker 写入失败，跨表一致性校验不可用");
        }
        *last = Some(day);
        Ok(())
    }

    /// 启动恢复。
    /// 裁剪模式（prune_enabled=true）：只读最新**存在**的日文件（跨日重启、
    /// 新日尚未 flush 时即昨天的文件）。最新日文件里可能含当天已平/已成交的
    /// 终态行——它们是「当天终态 + 活行」内存快照的一部分，恢复时保留；
    /// 更早日期的终态不复活（防复活由「不读旧日文件」结构性保证）。
    /// 文件缺失/损坏 fail-open 空表，绝不回退读更旧文件（spec §设计.5）。
    /// 兼容模式（false）：旧行为，升序读全部日文件，同 id 取 updated_at_ms
    /// 最新，靠 deleted 标志防复活。
    pub fn recover<T: Model>(&self) -> Result<usize> {
        let table = self.table::<T>();
        let mut merged: HashMap<String, PersistRecord> = HashMap::new();

        let day_dirs = self.backend.list_days()?;
        let latest_day = day_dirs.last().map(String::as_str);

        if self.prune_enabled {
            // 只读最新存在的日文件
            let Some(latest) = latest_day else {
                return Ok(0);
            };
            let records = self.backend.read_table(latest, T::NAME)?;
            if records.is_empty() {
                return Ok(0);
            }
            for rec in records {
                merged.insert(rec.id.clone(), rec);
            }
        } else {
            // 兼容模式：多日合并，同 id 取 updated_at_ms 最新
            for day in &day_dirs {
                let records = self.backend.read_table(day, T::NAME)?;
                for rec in records {
                    merged
                        .entry(rec.id.clone())
                        .and_modify(|cur: &mut PersistRecord| {
                            if rec.updated_at_ms > cur.updated_at_ms {
                                *cur = rec.clone();
                            }
                        })
                        .or_insert(rec);
                }
            }
        }
        // P2-10：flush marker 一致性校验——检测跨表非原子写导致的
        // 数据不一致（进程崩溃在部分表写完时）。仅告警不致命。
        if self.prune_enabled {
            if let Some(latest) = day_dirs.last() {
                if let Some(marker) = self.backend.read_marker(latest) {
                    let merged_vec: Vec<PersistRecord> = merged.values().cloned().collect();
                    if let Some(digest) = marker.tables.iter().find(|d| d.name == T::NAME) {
                        if let Err(reason) = persist::verify_table_digest(&merged_vec, digest) {
                            tracing::warn!(
                                model = T::NAME,
                                day = %latest,
                                reason = %reason,
                                "flush 一致性校验未通过——可能因进程崩溃导致跨表非原子写"
                            );
                        }
                    }
                }
            }
        }
        table.load_merged(
            merged,
            self.prune_enabled,
            if self.prune_enabled { latest_day } else { None },
        )
    }
}

fn downcast<T: Model>(arc: Arc<dyn Persistable>) -> Arc<ModelTable<T>> {
    arc.into_any_arc()
        .downcast::<ModelTable<T>>()
        .expect("model NAME 与类型冲突")
}

#[cfg(test)]
mod tests {
    pub(super) use super::*;
    pub(super) use serde::{Deserialize, Serialize};
    pub(super) use std::sync::atomic::{AtomicI64, Ordering};

    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct Widget {
        id: String,
        n: i64,
    }

    impl Model for Widget {
        const NAME: &'static str = "widget";
        fn id(&self) -> &str {
            &self.id
        }
    }

    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    pub(super) struct DoneWidget {
        id: String,
        n: i64,
    }

    impl Model for DoneWidget {
        const NAME: &'static str = "done_widget";
        fn id(&self) -> &str {
            &self.id
        }
        // 表征「业务终态」model：跨日裁剪的判定来源之一
        fn is_terminal(&self) -> bool {
            self.n >= 100
        }
    }

    pub(super) fn done_widget(id: &str, n: i64) -> DoneWidget {
        DoneWidget {
            id: id.to_string(),
            n,
        }
    }

    #[test]
    fn model_is_terminal_defaults_to_false() {
        // 默认实现 false：未显式实现的 model 永不因 is_terminal 被裁
        assert!(!widget("w", 1).is_terminal());
        assert!(done_widget("d", 100).is_terminal());
        assert!(!done_widget("d", 99).is_terminal());
    }

    /// 可控时钟：测试推进时间，不引入墙钟（确定性红线）。
    pub(super) struct AtomicClock(pub(super) AtomicI64);

    impl AtomicClock {
        pub(super) fn set(&self, ms: i64) {
            self.0.store(ms, Ordering::SeqCst);
        }
    }

    impl Clock for AtomicClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn store_at(now: i64) -> (tempfile::TempDir, Arc<Store>) {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(AtomicClock(AtomicI64::new(now)));
        let store = Arc::new(Store::new(dir.path(), clock));
        (dir, store)
    }

    fn widget(id: &str, n: i64) -> Widget {
        Widget {
            id: id.to_string(),
            n,
        }
    }

    #[test]
    fn insert_then_get_returns_clone_with_ms_timestamps() {
        let (_dir, store) = store_at(1_700_000_000_000);
        let table = store.table::<Widget>();

        table.insert(widget("w1", 1)).unwrap();

        assert_eq!(table.get("w1"), Some(widget("w1", 1)));
        let meta = table.meta("w1").unwrap();
        assert!(!meta.deleted);
        assert_eq!(meta.created_at_ms, 1_700_000_000_000);
        assert_eq!(meta.updated_at_ms, 1_700_000_000_000);
    }

    #[test]
    fn duplicate_insert_is_rejected() {
        let (_dir, store) = store_at(1);
        let table = store.table::<Widget>();

        table.insert(widget("w1", 1)).unwrap();
        let err = table.insert(widget("w1", 2)).unwrap_err();
        assert!(matches!(err, StoreError::AlreadyExists { .. }));
        // 原行不被覆盖
        assert_eq!(table.get("w1"), Some(widget("w1", 1)));
    }

    #[test]
    fn upsert_preserves_created_at_and_bumps_updated_at() {
        let dir = tempfile::tempdir().unwrap();
        let clock: Arc<AtomicClock> = Arc::new(AtomicClock(AtomicI64::new(100)));
        let store = Store::new(dir.path(), clock.clone());
        let table = store.table::<Widget>();
        table.insert(widget("w1", 1)).unwrap();

        clock.set(200);
        table.upsert(widget("w1", 2));

        let meta = table.meta("w1").unwrap();
        assert_eq!(meta.created_at_ms, 100);
        assert_eq!(meta.updated_at_ms, 200);
        assert_eq!(table.get("w1"), Some(widget("w1", 2)));
    }

    #[test]
    fn soft_delete_hides_row_and_marks_meta() {
        let dir = tempfile::tempdir().unwrap();
        let clock: Arc<AtomicClock> = Arc::new(AtomicClock(AtomicI64::new(10)));
        let store = Store::new(dir.path(), clock.clone());
        let table = store.table::<Widget>();
        table.insert(widget("w1", 1)).unwrap();
        clock.set(20);

        assert!(table.delete("w1"));
        assert!(!table.delete("w1")); // 已删除/不存在
        assert!(!table.delete("missing"));

        assert_eq!(table.get("w1"), None);
        assert!(table.list().is_empty());
        let meta = table.meta("w1").unwrap();
        assert!(meta.deleted);
        assert_eq!(meta.updated_at_ms, 20);
    }

    #[test]
    fn with_runs_compute_under_row_lock_and_bumps_updated_at() {
        let dir = tempfile::tempdir().unwrap();
        let clock: Arc<AtomicClock> = Arc::new(AtomicClock(AtomicI64::new(10)));
        let store = Store::new(dir.path(), clock.clone());
        let table = store.table::<Widget>();
        table.insert(widget("w1", 0)).unwrap();
        clock.set(30);

        let returned = table.with("w1", |w| {
            w.n += 5;
            w.n
        });
        assert_eq!(returned, Some(5));
        assert_eq!(table.get("w1").unwrap().n, 5);
        assert_eq!(table.meta("w1").unwrap().updated_at_ms, 30);
        assert_eq!(table.with("missing", |w| w.n), None);
    }

    #[test]
    fn list_returns_only_active_rows() {
        let (_dir, store) = store_at(1);
        let table = store.table::<Widget>();
        table.insert(widget("a", 1)).unwrap();
        table.insert(widget("b", 2)).unwrap();
        table.insert(widget("c", 3)).unwrap();
        assert!(table.delete("b"));

        let mut active: Vec<String> = table.list().into_iter().map(|w| w.id).collect();
        active.sort();
        assert_eq!(active, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn get_returns_independent_clone() {
        let (_dir, store) = store_at(1);
        let table = store.table::<Widget>();
        table.insert(widget("w1", 1)).unwrap();

        let mut borrowed = table.get("w1").unwrap();
        borrowed.n = 999;
        assert_eq!(table.get("w1").unwrap().n, 1);
    }

    #[test]
    fn table_is_shared_per_model_type_within_store() {
        let (_dir, store) = store_at(1);
        let t1 = store.table::<Widget>();
        let t2 = store.table::<Widget>();
        t1.insert(widget("w1", 1)).unwrap();
        assert!(Arc::ptr_eq(&t1, &t2));
        assert_eq!(t2.get("w1").unwrap().n, 1);
    }

    #[test]
    fn concurrent_with_on_same_row_loses_no_updates() {
        let (_dir, store) = store_at(1);
        let table = store.table::<Widget>();
        table.insert(widget("hot", 0)).unwrap();

        let mut handles = Vec::new();
        for _ in 0..16 {
            let t = Arc::clone(&table);
            handles.push(std::thread::spawn(move || {
                for _ in 0..200 {
                    t.with("hot", |w| w.n += 1);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(table.get("hot").unwrap().n, 16 * 200);
    }

    #[test]
    fn epoch_clock_emills_positive() {
        let ms = EpochClock.now_ms();
        assert!(ms > 1_700_000_000_000);
    }

    // ---------- S2：持久化与恢复 ----------

    use chrono::TimeZone;

    pub(super) fn day_ms(y: i32, m: u32, d: u32, h: u32) -> i64 {
        chrono::Utc
            .from_utc_datetime(
                &chrono::NaiveDate::from_ymd_opt(y, m, d)
                    .unwrap()
                    .and_hms_opt(h, 0, 0)
                    .unwrap(),
            )
            .timestamp_millis()
    }

    fn controllable_store(now: i64) -> (tempfile::TempDir, Arc<AtomicClock>, Arc<Store>) {
        let dir = tempfile::tempdir().unwrap();
        let clock = Arc::new(AtomicClock(AtomicI64::new(now)));
        let store = Arc::new(Store::new(dir.path(), clock.clone()));
        (dir, clock, store)
    }

    #[test]
    fn flush_writes_day_csv_and_recover_restores_active_rows_only() {
        let (dir, _clock, store) = controllable_store(day_ms(2023, 11, 15, 10));
        let table = store.table::<Widget>();
        table.insert(widget("a", 1)).unwrap();
        table.insert(widget("b", 2)).unwrap();
        assert!(table.delete("b"));

        store.flush().unwrap();

        let path = dir.path().join("20231115").join("widget.csv");
        let content = std::fs::read_to_string(&path).unwrap();
        let mut lines = content.lines();
        assert_eq!(
            lines.next(),
            Some("id,deleted,created_at_ms,updated_at_ms,payload_json")
        );
        let body: Vec<&str> = lines.collect();
        assert_eq!(body.len(), 2, "当日快照含已删除行: {body:?}");
        assert!(body.iter().any(|l| l.starts_with("a,0,")));
        assert!(body.iter().any(|l| l.starts_with("b,1,")));

        // 新进程视角：恢复后 deleted 终态行进不了内存
        let store2 = Store::new(dir.path(), Arc::new(EpochClock));
        let loaded = store2.recover::<Widget>().unwrap();
        assert_eq!(loaded, 1);
        let t2 = store2.table::<Widget>();
        assert_eq!(t2.get("a"), Some(widget("a", 1)));
        assert_eq!(t2.get("b"), None);
        assert_eq!(t2.meta("b"), None);
    }

    #[test]
    fn same_day_flush_overwrites_not_appends() {
        let (dir, _clock, store) = controllable_store(day_ms(2023, 11, 15, 10));
        store.table::<Widget>().insert(widget("a", 1)).unwrap();
        store.flush().unwrap();
        store.table::<Widget>().insert(widget("c", 3)).unwrap();
        store.flush().unwrap();

        let content = std::fs::read_to_string(dir.path().join("20231115/widget.csv")).unwrap();
        assert_eq!(content.lines().count(), 1 + 2, "表头 + 覆盖后的两行");
    }

    #[test]
    fn same_day_terminal_rows_stay_until_next_day() {
        let (dir, clock, store) = controllable_store(day_ms(2023, 11, 15, 10));
        let table = store.table::<DoneWidget>();
        table.insert(done_widget("done_today", 100)).unwrap();
        store.flush().unwrap();
        // 当日终态是当日快照的审计内容：内存与文件都保留到次日。
        assert!(table.get("done_today").is_some());
        let day1 = std::fs::read_to_string(dir.path().join("20231115/done_widget.csv")).unwrap();
        assert!(day1.contains("done_today"));

        clock.set(day_ms(2023, 11, 16, 10));
        store.flush().unwrap();
        // 归属日早于今天：内存裁掉，新日快照不再携带旧交易日终态。
        assert_eq!(table.get("done_today"), None);
        let day2 = std::fs::read_to_string(dir.path().join("20231116/done_widget.csv")).unwrap();
        assert!(!day2.contains("done_today"));
    }

    #[test]
    fn recover_same_day_restores_terminal_rows() {
        let (dir, clock, store) = controllable_store(day_ms(2023, 11, 15, 10));
        store
            .table::<DoneWidget>()
            .insert(done_widget("done_today", 100))
            .unwrap();
        store.flush().unwrap();

        // 同日重启：最新日文件含当天终态，恢复后仍在内存（当天快照语义）。
        let store2 = Store::new(
            dir.path(),
            Arc::new(AtomicClock(AtomicI64::new(day_ms(2023, 11, 15, 12)))),
        );
        assert_eq!(store2.recover::<DoneWidget>().unwrap(), 1);
        assert!(store2.table::<DoneWidget>().get("done_today").is_some());

        // 次日：该行被裁，最新日文件不再含它，恢复为空。
        clock.set(day_ms(2023, 11, 16, 10));
        store.flush().unwrap();
        let store3 = Store::new(
            dir.path(),
            Arc::new(AtomicClock(AtomicI64::new(day_ms(2023, 11, 16, 12)))),
        );
        assert_eq!(store3.recover::<DoneWidget>().unwrap(), 0);
    }

    #[test]
    fn day_rollover_purges_prior_day_deleted_rows_from_memory() {
        let (dir, clock, store) = controllable_store(day_ms(2023, 11, 15, 23));
        let table = store.table::<Widget>();
        table.insert(widget("a", 1)).unwrap();
        table.insert(widget("b", 2)).unwrap();
        assert!(table.delete("b"));
        store.flush().unwrap();

        clock.set(day_ms(2023, 11, 16, 0));
        store.flush().unwrap();

        // b 从内存物理清除（新语义：不进墓碑；防复活由 recover 单日化结构保证）
        assert_eq!(table.meta("b"), None);
        assert_eq!(table.get("a"), Some(widget("a", 1)));
        // 新日快照只含活行，不再携带墓碑
        let day2 = std::fs::read_to_string(dir.path().join("20231116/widget.csv")).unwrap();
        assert_eq!(day2.lines().count(), 2, "表头 + a 活行: {day2}");
        assert!(day2.lines().any(|l| l.starts_with("a,0,")));
        assert!(!day2.lines().any(|l| l.starts_with("b,1,")));
        // 同日多次落盘稳定
        store.flush().unwrap();
        let day2 = std::fs::read_to_string(dir.path().join("20231116/widget.csv")).unwrap();
        assert!(!day2.lines().any(|l| l.starts_with("b,1,")));
        // 旧日文件保留（历史可查）
        assert!(dir.path().join("20231115/widget.csv").exists());
    }

    #[test]
    fn recover_reads_all_days_dedupes_newest_then_honors_latest_delete() {
        let (dir, clock, store) = controllable_store(day_ms(2023, 11, 15, 9));
        let table = store.table::<Widget>();
        table.insert(widget("w1", 1)).unwrap();
        store.flush().unwrap();
        let t1 = table.meta("w1").unwrap().created_at_ms;

        clock.set(day_ms(2023, 11, 16, 9));
        table.upsert(widget("w1", 2));
        store.flush().unwrap();

        let store2 = Store::new(dir.path(), Arc::new(EpochClock));
        assert_eq!(store2.recover::<Widget>().unwrap(), 1);
        let t2 = store2.table::<Widget>();
        assert_eq!(t2.get("w1").unwrap().n, 2);
        let meta = t2.meta("w1").unwrap();
        assert_eq!(meta.created_at_ms, t1);
        assert_eq!(meta.updated_at_ms, day_ms(2023, 11, 16, 9));

        // 第三日删除：跨日去重取最新状态 → 恢复后消失
        clock.set(day_ms(2023, 11, 17, 9));
        assert!(table.delete("w1"));
        store.flush().unwrap();
        let store3 = Store::new(dir.path(), Arc::new(EpochClock));
        assert_eq!(store3.recover::<Widget>().unwrap(), 0);
        assert_eq!(store3.table::<Widget>().get("w1"), None);
    }

    #[test]
    fn recover_skips_corrupt_record_instead_of_failing() {
        let (dir, _clock, store) = controllable_store(day_ms(2023, 11, 15, 9));
        store.table::<Widget>().insert(widget("good", 1)).unwrap();
        store.flush().unwrap();

        let path = dir.path().join("20231115/widget.csv");
        let mut s = std::fs::read_to_string(&path).unwrap();
        s.push_str("broken,not-a-number,oops,xx,not-json\n");
        std::fs::write(&path, s).unwrap();

        let store2 = Store::new(dir.path(), Arc::new(EpochClock));
        let loaded = store2.recover::<Widget>().unwrap();
        assert_eq!(loaded, 1);
        assert_eq!(
            store2.table::<Widget>().get("good"),
            Some(widget("good", 1))
        );
    }

    // ---------- 覆盖率补盲：既有分支的表征测试 ----------

    /// 手工写一个日文件（五列齐全）。
    fn write_day(base: &Path, day: &str, rec: PersistRecord) {
        persist::write_day_csv(&base.join(day).join("widget.csv"), &[rec]).unwrap();
    }

    /// csv crate 编码一行（payload 含逗号/引号时正确转义）。
    fn encode(r: &PersistRecord) -> String {
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        wtr.serialize(r).unwrap();
        String::from_utf8(wtr.into_inner().unwrap()).unwrap()
    }

    fn rec(id: &str, deleted: u8, created: i64, updated: i64, n: i64) -> PersistRecord {
        PersistRecord {
            id: id.to_string(),
            deleted,
            created_at_ms: created,
            updated_at_ms: updated,
            payload_json: format!(r#"{{"id":"{id}","n":{n}}}"#),
        }
    }

    #[test]
    fn recover_no_day_dirs_fails_open() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path(), Arc::new(EpochClock));
        assert_eq!(store.recover::<Widget>().unwrap(), 0);
        assert_eq!(store.table::<Widget>().list().len(), 0);
    }

    #[test]
    fn rollover_purges_terminal_rows_and_keeps_active() {
        let (dir, clock, store) = controllable_store(day_ms(2023, 11, 15, 10));
        let table = store.table::<DoneWidget>();
        // 活行 / 业务终态行 / 软删行 三种形态
        table.insert(done_widget("act", 1)).unwrap();
        table.insert(done_widget("term", 100)).unwrap();
        table.insert(done_widget("del", 2)).unwrap();
        assert!(table.delete("del"));

        // 同日 flush：终态行仍在快照（裁剪只在 rollover，spec A3）
        store.flush().unwrap();
        let same_day =
            std::fs::read_to_string(dir.path().join("20231115/done_widget.csv")).unwrap();
        assert_eq!(same_day.lines().count(), 4, "同日快照含全部 3 行");
        assert!(table.get("term").is_some(), "同日终态行不裁");

        // 跨日 flush：终态/软删行物理消失，活行保留（spec A1/A2）
        clock.set(day_ms(2023, 11, 16, 10));
        store.flush().unwrap();
        assert_eq!(table.get("act"), Some(done_widget("act", 1)));
        assert_eq!(table.get("term"), None);
        assert_eq!(table.get("del"), None);
        assert_eq!(table.meta("term"), None, "物理移除，非墓碑");
        let next_day =
            std::fs::read_to_string(dir.path().join("20231116/done_widget.csv")).unwrap();
        assert_eq!(next_day.lines().count(), 2, "表头 + 仅活行 1 行");
        assert!(next_day.contains("act,0,"), "新快照只含活行: {next_day:?}");
    }

    #[test]
    fn insert_same_id_after_terminal_purge_succeeds() {
        let (dir, clock, store) = controllable_store(day_ms(2023, 11, 15, 10));
        let table = store.table::<DoneWidget>();
        table.insert(done_widget("reuse", 100)).unwrap();
        // 先同日 flush（last=20231115），再跨日——rollover 需要 prev 日存在
        store.flush().unwrap();
        clock.set(day_ms(2023, 11, 16, 10));
        store.flush().unwrap(); // rollover 裁掉 reuse
                                // 裁掉后同 id 重建：不再有墓碑拦截（spec A11）
        assert!(table.insert(done_widget("reuse", 1)).is_ok());
        assert_eq!(table.get("reuse"), Some(done_widget("reuse", 1)));
        let _ = dir;
    }

    #[test]
    fn upsert_resurrects_same_day_soft_delete() {
        // 同日软删复活走 rows.deleted 标志，与墓碑无关——删墓碑不得破坏（Review Focus #3）
        let (_dir, store) = store_at(1_700_000_000_000);
        let table = store.table::<Widget>();
        table.insert(widget("w", 1)).unwrap();
        assert!(table.delete("w"));
        assert_eq!(table.get("w"), None);
        table.upsert(widget("w", 2)); // 同日 upsert 复活
        assert_eq!(table.get("w"), Some(widget("w", 2)));
        assert!(!table.meta("w").unwrap().deleted);
    }

    #[test]
    fn clock_rollback_still_uses_last_day_and_does_not_purge() {
        let (dir, clock, store) = controllable_store(day_ms(2023, 11, 16, 10));
        let table = store.table::<DoneWidget>();
        table.insert(done_widget("t", 100)).unwrap();
        store.flush().unwrap(); // last = 20231116
                                // 时钟回拨到前一天：单调守卫不触发 rollover，不裁剪
        clock.set(day_ms(2023, 11, 15, 10));
        store.flush().unwrap();
        assert_eq!(table.get("t"), Some(done_widget("t", 100)), "回拨不裁剪");
        // 快照继续写回拨前的最后日期，不覆写 20231115
        let rolled = std::fs::read_to_string(dir.path().join("20231116/done_widget.csv")).unwrap();
        assert!(rolled.contains("t,0,"));
        assert!(!dir.path().join("20231115/done_widget.csv").exists());
    }

    #[test]
    fn prune_disabled_keeps_legacy_behavior() {
        let dir = tempfile::tempdir().unwrap();
        // day1: 软删行 X + 活行 a（一文件两行）；day2: 活行 a 新版
        // （DoneWidget NAME=done_widget，write_day helper 固定写 widget.csv 不适用）
        persist::write_day_csv(
            &dir.path().join("20231114/done_widget.csv"),
            &[rec("X", 1, 1, 1, 0), rec("a", 0, 1, 1, 1)],
        )
        .unwrap();
        persist::write_day_csv(
            &dir.path().join("20231115/done_widget.csv"),
            &[rec("a", 0, 1, 2, 2)],
        )
        .unwrap();

        let clock = Arc::new(AtomicClock(AtomicI64::new(day_ms(2023, 11, 16, 10))));
        let store = Store::new_with_policy(dir.path(), clock.clone(), false);
        let table = store.table::<DoneWidget>();

        // recover：多日合并（旧行为），软删行不入活表
        assert_eq!(store.recover::<DoneWidget>().unwrap(), 1, "只载活行");
        // 多日合并同 id 取 updated_at_ms 最新：day2 (n=2) 胜出
        assert_eq!(table.get("a"), Some(done_widget("a", 2)));

        // 跨日 flush：不裁剪
        table.insert(done_widget("t", 100)).unwrap();
        clock.set(day_ms(2023, 11, 17, 10));
        store.flush().unwrap();
        assert_eq!(
            table.get("t"),
            Some(done_widget("t", 100)),
            "兼容模式不裁终态"
        );
        assert_eq!(table.get("a"), Some(done_widget("a", 2)));
        // 快照含全量表内行（软删行已不在内存；is_terminal 行保留）
        let snap = std::fs::read_to_string(dir.path().join("20231117/done_widget.csv")).unwrap();
        assert!(snap.contains("t,0,"), "终态行保留在快照: {snap:?}");
        assert!(snap.contains("a,0,"));
    }

    #[test]
    fn pruned_terminal_row_not_resurrected_by_recover() {
        // 核心不变量（spec A5）：跨日裁剪 → 写盘 → 新 Store recover → 不复活
        let (dir, clock, store) = controllable_store(day_ms(2023, 11, 15, 10));
        let table = store.table::<DoneWidget>();
        table.insert(done_widget("act", 1)).unwrap();
        table.insert(done_widget("term", 100)).unwrap();
        clock.set(day_ms(2023, 11, 16, 10));
        store.flush().unwrap();

        let store2 = Store::new(dir.path(), Arc::new(EpochClock));
        let loaded = store2.recover::<DoneWidget>().unwrap();
        assert_eq!(loaded, 1);
        let t2 = store2.table::<DoneWidget>();
        assert_eq!(t2.get("act"), Some(done_widget("act", 1)));
        assert_eq!(t2.get("term"), None, "被裁行结构上无法复活");
    }

    #[test]
    fn recover_reads_only_latest_existing_day() {
        let dir = tempfile::tempdir().unwrap();
        // day1 一文件两行：软删行 X + 活行 a；day2: 活行 a 的最新版
        persist::write_day_csv(
            &dir.path().join("20231114/widget.csv"),
            &[rec("X", 1, 1, 1, 0), rec("a", 0, 1, 1, 1)],
        )
        .unwrap();
        write_day(dir.path(), "20231115", rec("a", 0, 1, 2, 2));

        let store2 = Store::new(dir.path(), Arc::new(EpochClock));
        let loaded = store2.recover::<Widget>().unwrap();
        // 单日：X 被结构上隔离在旧日文件，永不复活；a 取最新版本 n=2
        assert_eq!(loaded, 1);
        let t2 = store2.table::<Widget>();
        assert_eq!(t2.get("a"), Some(widget("a", 2)));
        assert_eq!(t2.get("X"), None);
        assert_eq!(t2.meta("X"), None);
    }

    #[test]
    fn recover_reads_latest_existing_day_not_todays_date() {
        // 跨日重启、新日尚未 flush：目录里只有 day1 → 必须从 day1 恢复活动行，
        // 不得因「day1 != 今天」而空表（Review Focus #1）
        let dir = tempfile::tempdir().unwrap();
        write_day(dir.path(), "20231114", rec("live_pos", 0, 1, 1, 7));

        let store2 = Store::new(dir.path(), Arc::new(EpochClock));
        let loaded = store2.recover::<Widget>().unwrap();
        assert_eq!(loaded, 1);
        assert_eq!(
            store2.table::<Widget>().get("live_pos"),
            Some(widget("live_pos", 7))
        );
    }

    #[test]
    fn recover_corrupt_latest_day_skips_bad_rows() {
        let dir = tempfile::tempdir().unwrap();
        write_day(dir.path(), "20231115", rec("good", 0, 1, 1, 1));
        // 尾部撕裂行（JSON 未闭合）追加在文件末尾
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("20231115/widget.csv"))
            .unwrap();
        writeln!(f, "bad,0,1,1,\"{{\"\"id\"\":\"\"bad\"\"").unwrap();
        drop(f);

        let store2 = Store::new(dir.path(), Arc::new(EpochClock));
        assert_eq!(store2.recover::<Widget>().unwrap(), 1);
        assert_eq!(
            store2.table::<Widget>().get("good"),
            Some(widget("good", 1))
        );
    }

    #[test]
    fn upsert_creates_missing_row() {
        let (_dir, store) = store_at(10);
        let table = store.table::<Widget>();

        table.upsert(widget("new", 7));

        assert_eq!(table.get("new"), Some(widget("new", 7)));
        assert_eq!(table.meta("new").unwrap().created_at_ms, 10);
    }

    #[test]
    fn upsert_after_cross_day_delete_recreates_row() {
        let (dir, clock, store) = controllable_store(day_ms(2023, 11, 15, 10));
        let table = store.table::<Widget>();
        table.insert(widget("w1", 1)).unwrap();
        store.flush().unwrap();
        clock.set(day_ms(2023, 11, 16, 9));
        assert!(table.delete("w1"));
        store.flush().unwrap(); // 跨日：软删行被 purge 物理清除
        clock.set(day_ms(2023, 11, 16, 10));
        table.upsert(widget("w1", 9));
        store.flush().unwrap(); // 同日覆写：新行写入文件

        let store2 = Store::new(dir.path(), Arc::new(EpochClock));
        assert_eq!(store2.recover::<Widget>().unwrap(), 1);
        let t2 = store2.table::<Widget>();
        assert_eq!(t2.get("w1"), Some(widget("w1", 9)));
        assert!(!t2.meta("w1").unwrap().deleted);
    }

    #[test]
    fn with_on_deleted_row_returns_none() {
        let (_dir, store) = store_at(1);
        let table = store.table::<Widget>();
        table.insert(widget("w1", 1)).unwrap();
        assert!(table.delete("w1"));

        assert_eq!(table.with("w1", |w| w.n += 1), None);
        assert_eq!(table.meta("w1").unwrap().updated_at_ms, 1);
    }

    #[test]
    fn recover_missing_base_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(dir.path().join("does-not-exist"), Arc::new(EpochClock));
        assert_eq!(store.recover::<Widget>().unwrap(), 0);
    }

    #[test]
    fn recover_ignores_non_day_entries_and_missing_model_file() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        std::fs::write(base.join("notes.txt"), b"x").unwrap(); // 散文件
        std::fs::create_dir_all(base.join("notadate")).unwrap();
        std::fs::write(base.join("notadate/widget.csv"), b"garbage").unwrap();
        std::fs::create_dir_all(base.join("20231115")).unwrap();
        std::fs::write(base.join("20231115/other.csv"), b"x").unwrap(); // 无 widget.csv

        let store = Store::new(base, Arc::new(EpochClock));
        assert_eq!(store.recover::<Widget>().unwrap(), 0);
    }

    #[test]
    fn recover_equal_updated_at_keeps_earlier_day_record() {
        // 跨日同 id 场景只在兼容模式（多日合并）下存在——归入兼容分支保留
        let dir = tempfile::tempdir().unwrap();
        write_day(dir.path(), "20231115", rec("w1", 0, 1, 100, 1));
        write_day(dir.path(), "20231116", rec("w1", 0, 1, 100, 2));

        let store = Store::new_with_policy(dir.path(), Arc::new(EpochClock), false);
        assert_eq!(store.recover::<Widget>().unwrap(), 1);
        // 时间戳相等时保留升序先读到的较早日记录。
        assert_eq!(store.table::<Widget>().get("w1"), Some(widget("w1", 1)));
    }

    #[test]
    fn recover_wrong_shape_payload_is_skipped_like_other_bad_rows() {
        let dir = tempfile::tempdir().unwrap();
        write_day(
            dir.path(),
            "20231115",
            PersistRecord {
                id: "bad".into(),
                deleted: 0,
                created_at_ms: 1,
                updated_at_ms: 2,
                payload_json: "[1,2]".into(), // 合法 JSON 但形状不匹配 → 行级跳过
            },
        );

        let store = Store::new(dir.path(), Arc::new(EpochClock));
        assert_eq!(store.recover::<Widget>().unwrap(), 0);
        assert_eq!(store.table::<Widget>().get("bad"), None);
    }

    #[test]
    fn recover_never_overwrites_live_rows_nor_resurrects_unflushed_deletes() {
        // 监督重建场景（同进程、共享 Store）：磁盘是旧快照，内存永远是真相。
        let dir = tempfile::tempdir().unwrap();
        let clock: Arc<AtomicClock> =
            Arc::new(AtomicClock(AtomicI64::new(day_ms(2023, 11, 16, 10))));
        let store = Store::new(dir.path(), clock.clone());
        let table = store.table::<Widget>();
        table.insert(widget("w1", 5)).unwrap(); // 未 flush 的新态
        table.insert(widget("w2", 1)).unwrap();
        assert!(table.delete("w2")); // 未 flush 的平仓（软删行仍在活表）
                                     // 磁盘旧快照：w1=1（旧值），w2 仍 active（删除前落盘）。
        write_day(dir.path(), "20231115", rec("w1", 0, 1, 100, 1));
        write_day(dir.path(), "20231115", rec("w2", 0, 1, 100, 1));

        // 模拟新一代 manager 再次盘载：两 id 均已存在于活表 → 装载 0 行。
        assert_eq!(store.recover::<Widget>().unwrap(), 0);
        assert_eq!(table.get("w1"), Some(widget("w1", 5))); // 不被旧快照回滚
        assert_eq!(table.get("w2"), None); // 不复活
        assert!(table.meta("w2").unwrap().deleted);
    }

    #[test]
    fn upsert_same_day_soft_deleted_row_revives_it() {
        let (_dir, store) = store_at(10);
        let table = store.table::<Widget>();
        table.insert(widget("w1", 1)).unwrap();
        assert!(table.delete("w1")); // 同日软删（行柄仍在，未跨日 purge）

        table.upsert(widget("w1", 9));

        assert_eq!(table.get("w1"), Some(widget("w1", 9)));
        assert!(!table.meta("w1").unwrap().deleted);
    }

    #[test]
    fn concurrent_upserts_of_new_id_never_lose_data() {
        let (_dir, store) = store_at(1);
        let mut handles = Vec::new();
        for i in 0..32 {
            let table = Arc::clone(&store.table::<Widget>());
            handles.push(std::thread::spawn(move || {
                table.upsert(widget(&format!("id{i}"), i));
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 32 个不同新 id 并发创建：一个都不许丢（旧 TOCTOU 实现会静默漏行）。
        let table = store.table::<Widget>();
        for i in 0..32 {
            assert_eq!(
                table.get(&format!("id{i}")),
                Some(widget(&format!("id{i}"), i))
            );
        }
    }

    #[test]
    fn flush_clock_rollback_keeps_last_day_and_does_not_purge() {
        let (dir, clock, store) = controllable_store(day_ms(2023, 11, 15, 23));
        let table = store.table::<Widget>();
        table.insert(widget("a", 1)).unwrap();
        table.insert(widget("b", 2)).unwrap();
        assert!(table.delete("b"));
        store.flush().unwrap(); // 15 日：b 仍在活表（软删未 purge）

        // 时钟回拨到 14 日：不得 purge，仍写入 15 日目录（单调守卫）。
        clock.set(day_ms(2023, 11, 14, 12));
        store.flush().unwrap();
        assert!(table.meta("b").is_some(), "回拨不得触发跨日清扫");
        assert!(!dir.path().join("20231114/widget.csv").exists());
        assert!(dir.path().join("20231115/widget.csv").exists());

        // 时钟恢复并前进到 16 日：正常 rollover，purge 生效。
        clock.set(day_ms(2023, 11, 16, 0));
        store.flush().unwrap();
        assert_eq!(table.meta("b"), None);
        assert!(dir.path().join("20231116/widget.csv").exists());
    }

    #[test]
    fn recover_syntactically_bad_payload_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("20231115/widget.csv");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let good = encode(&rec("a", 0, 1, 1, 1));
        // 五列齐全、数字合法，但 payload 不是合法 JSON → 跳过不毒死恢复。
        let bad = "b,0,1,2,not-json\n";
        std::fs::write(&path, format!("{}\n{good}{bad}", persist::HEADER)).unwrap();

        let store = Store::new(dir.path(), Arc::new(EpochClock));
        assert_eq!(store.recover::<Widget>().unwrap(), 1);
        assert_eq!(store.table::<Widget>().get("a"), Some(widget("a", 1)));
    }

    #[test]
    fn flush_propagates_io_error_when_day_path_is_a_file() {
        let dir = tempfile::tempdir().unwrap();
        // 当日目录名位置被普通文件占据 → create_dir_all 失败。
        std::fs::write(dir.path().join(persist::day_str(1_700_000_000_000)), b"x").unwrap();
        let store = Store::new(dir.path(), Arc::new(FixedClock1700));
        store.table::<Widget>().insert(widget("w1", 1)).unwrap();

        assert!(matches!(store.flush(), Err(StoreError::Io(_))));
    }

    struct FixedClock1700;
    impl Clock for FixedClock1700 {
        fn now_ms(&self) -> i64 {
            1_700_000_000_000
        }
    }

    #[test]
    fn write_day_csv_rejects_path_without_parent() {
        // 空路径无 parent（文件名亦非法），首先撞 parent 守卫。
        let err = persist::write_day_csv(std::path::Path::new(""), &[]).unwrap_err();
        assert!(matches!(err, StoreError::Io(_)));
    }

    #[test]
    fn write_day_csv_rejects_path_without_file_name() {
        // "/.." 有父目录但 file_name() 为 None。
        let err = persist::write_day_csv(std::path::Path::new("/.."), &[]).unwrap_err();
        assert!(matches!(err, StoreError::Io(_)));
    }

    #[test]
    fn write_day_csv_errors_when_tmp_is_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("widget.csv");
        // tmp 名含 pid（多进程并发安全），预置本进程的 tmp 为目录
        let tmp_name = format!(".widget.csv.{}.tmp", std::process::id());
        std::fs::create_dir_all(dir.path().join(tmp_name)).unwrap(); // tmp 是目录

        let err = persist::write_day_csv(&path, &[rec("a", 0, 1, 1, 1)]).unwrap_err();
        assert!(matches!(err, StoreError::Io(_)));
    }

    #[test]
    fn write_day_csv_errors_when_target_is_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("widget.csv");
        std::fs::create_dir_all(target.join("inner")).unwrap(); // 目标是非空目录

        let err = persist::write_day_csv(&target, &[rec("a", 0, 1, 1, 1)]).unwrap_err();
        assert!(matches!(err, StoreError::Io(_)));
    }

    #[test]
    fn read_day_csv_errors_on_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            persist::read_day_csv(&dir.path().join("nope.csv")),
            Err(StoreError::Io(_))
        ));
    }

    #[test]
    fn read_day_csv_torn_quote_stops_parse_but_keeps_prior_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("widget.csv");
        let mut content = persist::HEADER.to_string();
        content.push('\n');
        // 第一行完好（csv 正确转义）；第二行引号不闭合 → csv 宽松返回撕裂记录，
        // payload 不是合法 JSON → parse 跳过。
        content.push_str(&encode(&PersistRecord {
            id: "a".into(),
            deleted: 0,
            created_at_ms: 1,
            updated_at_ms: 1,
            payload_json: serde_json::to_string(&widget("a", 1)).unwrap(),
        }));
        content.push_str("x,0,1,1,\"{unterminated\n");
        content.push_str("b,0,2,2,ignored\n");
        std::fs::write(&path, content).unwrap();

        let rows = persist::read_day_csv(&path).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "a");
    }

    #[test]
    fn recover_skips_rows_with_bad_count_bad_deleted_or_bad_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("20231115/widget.csv");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let good = encode(&rec("a", 0, 1, 1, 1));
        // 下面四行均为手工坏行（payload 占位 `{}` 是合法 JSON，坏在前面列）。
        let body = [
            good.trim_end_matches('\n').to_string(),
            "b,0,1".to_string(),       // 列数不足
            "c,2,1,1,{}".to_string(),  // deleted=2
            "d,0,xx,1,{}".to_string(), // created_at 坏
            "e,0,1,yy,{}".to_string(), // updated_at 坏
        ]
        .join("\n");
        std::fs::write(&path, format!("{}\n{body}\n", persist::HEADER)).unwrap();

        let store = Store::new(dir.path(), Arc::new(EpochClock));
        assert_eq!(store.recover::<Widget>().unwrap(), 1);
        assert_eq!(store.table::<Widget>().get("a"), Some(widget("a", 1)));
    }
    #[test]
    fn probe_flush_purge_insert() {
        // `_dir` 持有临时目录生命周期（store 内部已持有路径，此处仅保活）
        let (_dir, clock, store) = controllable_store(day_ms(2023, 11, 15, 10));
        let table = store.table::<DoneWidget>();
        table.insert(done_widget("reuse", 100)).unwrap();
        clock.set(day_ms(2023, 11, 16, 10));
        store.flush().unwrap();
        eprintln!("after flush get={:?}", table.get("reuse"));
        eprintln!("insert={:?}", table.insert(done_widget("reuse", 1)));
    }
}
