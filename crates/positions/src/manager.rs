//! per-symbol 仓位生命周期管理（S4：统一 Store 后端）
//!
//! 存储（全部经进程共享统一 Store；多策略实例共享总表，集合查询一律按
//! `strategy_name` 过滤）：
//! - [`ManagedPosition`] `NAME="position"`：当前持仓，id=position_id（全局唯一，
//!   每实例至多一行未删除行）；平仓软删（deleted 行随当日快照落盘，跨日成墓碑防复活）；
//! - [`ManagedStop`] `NAME="stop"`：止损单当前态，id=`stop_{position_id}_sl`，
//!   生命周期 created→updated→cancelled；终态行不删除（审计）；
//! - [`Cooldown`] `NAME="cooldown"`：止损冷却日，id=strategy_name；
//! - [`HistoryPosition`] `NAME="history_position"`：平仓历史，id=position_id，
//!   只增不改；平仓时同时旁路导出对外 22 列固定列 CSV（与 signal.csv 同性质）。
//!
//! 恢复：[`PositionManager::restore`] 走 Store 跨日去重 recover；Store 盘载
//! 只填空缺 id，活行/墓碑永不被磁盘旧快照覆盖，故同进程监督重建（panic 后
//! 新一代复用进程级 Store）不回滚任何实例未 flush 的状态，跨实例互不影响。
//!
//! 锁纪律：行变更全部经 `ModelTable::with`/upsert/delete 的同步调用，
//! 闭包内不 await、不调网关。

use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use strategy_core::position_math::{realized_pnl, weighted_avg};
use strategy_core::FillEvent;
use strategy_store::{Model, ModelTable, Store};
use tracing::{debug, info, warn};

use crate::history::{HistoryPosition, HistoryPositionLogger};
use crate::state::{PositionState, Side};

/// 止损单业务 id（与旧 stop_events 口径逐字一致）。
pub fn stop_order_id(position_id: &str) -> String {
    format!("stop_{position_id}_sl")
}

/// 共享总表中按运行时实例名过滤的行（多实例共享一张 model 表的集合查询口径）。
fn scoped_rows<T, K>(table: &ModelTable<T>, strategy_name: &str, scope: K) -> Vec<T>
where
    T: Model,
    K: Fn(&T) -> &str,
{
    table
        .list()
        .into_iter()
        .filter(|row| scope(row) == strategy_name)
        .collect()
}

/// 统计表中属于本实例的行数与全表行数（统一 Store 后多实例行共表，
/// 启动恢复日志需区分「本实例 / 全表」，避免全表数被误读为单实例持仓数）。
fn scope_split<T, K>(rows: &[T], strategy_name: &str, scope: K) -> (usize, usize)
where
    K: Fn(&T) -> &str,
{
    let mine = rows
        .iter()
        .filter(|row| scope(row) == strategy_name)
        .count();
    (mine, rows.len())
}

/// 外部只读反查：服务端/旁路持进程共享 [`Store`] 直接读某策略实例的**当前持仓
/// 快照**，不经 RuntimeActor 通道、不发任何消息（docs/archive/strategy-rs.md
/// 「服务端也可通过连接查询策略仓位」的数据面）。
///
/// 返回克隆值（每实例至多 1 行未删除持仓），行 Mutex 不外泄；平仓软删行由
/// Store 自动过滤，读不到即"无仓"。与单写者 Actor 并发安全：读走行级子锁，
/// 闭包期外不持锁。
///
/// TODO(S6, sidecar 对签)：交易 WS 入站目前只有 OrderUpdate/Reconnected，
/// 「服务端查仓位」消息类型（请求 id、strategy_name、应答结构）待与 sidecar
/// 对签后在 broker crate 增补；本函数只提供数据通道，**不含任何 WS 消息**，
/// 不得在此之前构造假协议。
pub fn list_positions(store: &Arc<Store>, strategy_name: &str) -> Vec<PositionState> {
    scoped_rows(&store.table::<ManagedPosition>(), strategy_name, |row| {
        row.strategy_name.as_str()
    })
    .into_iter()
    .map(|row| row.state)
    .collect()
}

/// 当前持仓行（Store `position` model；id=position_id 全局唯一）。
///
/// 多策略实例共享同一张表：每实例至多 1 行未删除持仓，集合查询按
/// `strategy_name` 过滤；同策略平仓后重新开仓 = 新 position_id 的新行
/// （旧行已软删，不与新行冲突）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedPosition {
    /// 运行时策略实例名（共享表过滤键）
    pub strategy_name: String,
    /// 仓位状态机单调时钟：每次内存变更 +1（监督重建恢复合并用）
    pub version: u64,
    pub state: PositionState,
}

impl Model for ManagedPosition {
    const NAME: &'static str = "position";

    fn id(&self) -> &str {
        &self.state.position_id
    }

    fn is_terminal(&self) -> bool {
        // 平仓走 deleted 软删标志（close()），is_terminal 恒 false；
        // 裁剪判定是 deleted OR is_terminal 的并集
        false
    }
}

/// 止损单当前态行（Store `stop` model；id=stop_order_id）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedStop {
    pub stop_order_id: String,
    pub strategy_name: String,
    pub position_id: String,
    /// 该止损单自身的版本：created=1，每次 updated/cancelled +1
    pub version: u64,
    /// 最近生命周期事件：created / updated / cancelled
    pub lifecycle: String,
    pub trigger_price: f64,
    pub size: f64,
}

impl Model for ManagedStop {
    const NAME: &'static str = "stop";

    fn id(&self) -> &str {
        &self.stop_order_id
    }

    fn is_terminal(&self) -> bool {
        // created/updated = 在途止损单；cancelled = 已归档终态
        self.lifecycle == "cancelled"
    }
}

/// 止损冷却日行（Store `cooldown` model；id=strategy_name）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cooldown {
    pub strategy_name: String,
    /// 冷却日（K 线时间，%Y-%m-%d；Ctx 按 bar_end 日历日比对）
    pub date: String,
}

impl Model for Cooldown {
    const NAME: &'static str = "cooldown";

    fn id(&self) -> &str {
        &self.strategy_name
    }
}

pub struct PositionManager {
    strategy_name: String,
    symbol: String,
    trading_mode: String,
    store: Arc<Store>,
    positions: Arc<ModelTable<ManagedPosition>>,
    stops: Arc<ModelTable<ManagedStop>>,
    cooldowns: Arc<ModelTable<Cooldown>>,
    history: Arc<ModelTable<HistoryPosition>>,
    /// 对外固定列历史仓位 CSV（旁路报表；内存真相在 history 表）
    history_writer: HistoryPositionLogger,
    /// 最近一次 on_price 所见价格（运行时内存，不落盘）：外部平仓对账时
    /// 作为 exit_price；无 tick 记录则回退入场价
    last_price: Option<f64>,
    /// store.recover 每代只盘载一次（同信号侧 loaded 纪律）
    loaded: bool,
}

impl PositionManager {
    pub fn new(
        strategy_name: impl Into<String>,
        symbol: impl Into<String>,
        trading_mode: impl Into<String>,
        store: Arc<Store>,
        report_base: impl AsRef<Path>,
    ) -> Self {
        let positions = store.table::<ManagedPosition>();
        let stops = store.table::<ManagedStop>();
        let cooldowns = store.table::<Cooldown>();
        let history = store.table::<HistoryPosition>();
        Self {
            strategy_name: strategy_name.into(),
            symbol: symbol.into(),
            trading_mode: trading_mode.into(),
            store,
            positions,
            stops,
            cooldowns,
            history,
            history_writer: HistoryPositionLogger::new(report_base.as_ref()),
            last_price: None,
            loaded: false,
        }
    }

    /// 进程共享 Store（外壳 spawn_blocking(flush) / 测试断言用）。
    pub fn store(&self) -> Arc<Store> {
        Arc::clone(&self.store)
    }

    /// 本实例当前持仓行（id=position_id；未删除行至多 1 行）。
    fn current_row(&self) -> Option<ManagedPosition> {
        scoped_rows(&self.positions, &self.strategy_name, |r| {
            r.strategy_name.as_str()
        })
        .into_iter()
        .next()
    }

    /// 当前持仓（克隆；无仓返回 None）。
    pub fn position(&self) -> Option<PositionState> {
        self.current_row().map(|r| r.state)
    }

    pub fn is_open(&self) -> bool {
        self.current_row().is_some()
    }

    /// 当前止损冷却（None=无冷却；读 cooldown 表）。
    pub fn stop_loss_cooldown(&self) -> Option<strategy_core::strategy::Cooldown> {
        self.cooldowns
            .get(&self.strategy_name)
            .map(|c| strategy_core::strategy::Cooldown::UntilDate(c.date))
    }

    /// 某持仓的止损单当前态（测试/对账查询用）。
    pub fn stop_row(&self, position_id: &str) -> Option<ManagedStop> {
        self.stops.get(&stop_order_id(position_id))
    }

    /// 本实例全部历史仓（克隆；按 strategy_name 过滤）。
    pub fn history(&self) -> Vec<HistoryPosition> {
        scoped_rows(&self.history, &self.strategy_name, |h| {
            h.strategy_name.as_str()
        })
    }

    /// 开仓：position_id 由调用方传（用确定性 signal_id，保证回测/实盘一致）
    pub fn open(
        &mut self,
        position_id: String,
        side: Side,
        entry_price: f64,
        entry_time: DateTime<Utc>,
        stop_price: f64,
        atr_at_entry: f64,
    ) -> std::io::Result<PositionState> {
        let state = PositionState {
            position_id: position_id.clone(),
            symbol: self.symbol.clone(),
            side,
            entry_price,
            entry_time,
            entry_timestamp: entry_time.timestamp(),
            peak_price: entry_price,
            stop_price,
            max_pnl_pct: 0.0,
            min_pnl_pct: 0.0,
            atr_at_entry,
            total_size: 0.0,
            remaining_size: 0.0,
            realized_pnl: 0.0,
            trail_activated: false,
            trail_trigger_pct: 0.0,
            entry_size_raw: None,
            remaining_size_raw: None,
            exit_active: false,
            exit_price: 0.0,
        };
        self.insert_open(state, stop_price, 0.0)
    }

    /// FillEvent 驱动开仓（P5 意图管道使用；携带成交量）。
    pub fn apply_open_fill(
        &mut self,
        fill: &FillEvent,
        entry_time: DateTime<Utc>,
        stop_price: f64,
        atr_at_entry: f64,
    ) -> std::io::Result<PositionState> {
        debug_assert!(!self.is_open(), "重复开仓：平仓后再开");
        let side = match fill.direction {
            strategy_core::Side::Long => Side::Long,
            strategy_core::Side::Short => Side::Short,
        };
        let state = PositionState {
            position_id: fill.signal_id.clone(),
            symbol: fill.symbol.clone(),
            side,
            entry_price: fill.filled_price,
            entry_time,
            entry_timestamp: entry_time.timestamp(),
            peak_price: fill.filled_price,
            stop_price,
            max_pnl_pct: 0.0,
            min_pnl_pct: 0.0,
            atr_at_entry,
            total_size: fill.filled_size,
            remaining_size: fill.filled_size,
            realized_pnl: 0.0,
            trail_activated: false,
            trail_trigger_pct: 0.0,
            // 成交回报原始字面量：开仓量与初始可平量都以回报为准
            entry_size_raw: fill.filled_size_raw.clone(),
            // 剩余可平量原始字面量：平仓帧 quantity 的真正精确来源
            remaining_size_raw: fill.filled_size_raw.clone(),
            exit_active: false,
            exit_price: 0.0,
        };
        self.insert_open(state, stop_price, fill.filled_size)
    }

    fn insert_open(
        &mut self,
        state: PositionState,
        stop_price: f64,
        stop_size: f64,
    ) -> std::io::Result<PositionState> {
        let position_id = state.position_id.clone();
        let row = ManagedPosition {
            strategy_name: self.strategy_name.clone(),
            version: 1,
            state: state.clone(),
        };
        self.positions
            .insert(row)
            .map_err(|e| std::io::Error::other(format!("position 行写入失败: {e}")))?;
        let stop = ManagedStop {
            stop_order_id: stop_order_id(&position_id),
            strategy_name: self.strategy_name.clone(),
            position_id,
            version: 1,
            lifecycle: "created".into(),
            trigger_price: stop_price,
            size: stop_size,
        };
        self.stops
            .insert(stop)
            .map_err(|e| std::io::Error::other(format!("stop 行写入失败: {e}")))?;
        tracing::info!(
            position_id = %state.position_id,
            entry_price = state.entry_price,
            side = %state.side,
            "开仓"
        );
        Ok(state)
    }

    /// 同向加仓（加权均价；当前策略单仓进出，路径保留并测试）。
    pub fn apply_add_fill(&mut self, fill: &FillEvent) -> std::io::Result<()> {
        let Some(prev) = self.current_row() else {
            return Ok(());
        };
        let avg = weighted_avg(
            prev.state.total_size,
            prev.state.entry_price,
            fill.filled_size,
            fill.filled_price,
        );
        self.positions
            .with(&prev.state.position_id, |r| {
                r.state.entry_price = avg;
                r.state.total_size += fill.filled_size;
                r.state.remaining_size += fill.filled_size;
                r.version += 1;
            })
            .expect("持仓行刚读取存在");
        Ok(())
    }

    /// 精确数量递减：剩余 raw − 本笔成交 raw（Decimal 精确运算，无 f64 往返）。
    /// raw 缺失或解析失败时返回 None（调用方保留原 raw，不写入错误值）。
    fn subtract_raw(remaining_raw: Option<&str>, closed_raw: &str) -> Option<String> {
        use std::str::FromStr;
        let rem = rust_decimal::Decimal::from_str(remaining_raw?).ok()?;
        let closed = rust_decimal::Decimal::from_str(closed_raw).ok()?;
        Some(format!("{}", rem - closed))
    }

    /// 部分平仓：在 history 表写/更新累计进度行（id=position_id）。
    /// 崩溃恢复后可知已平部分（exit_size 为累计已平量）；对外固定列 CSV
    /// 仅最终平仓时写，24 列契约不变。
    fn record_partial_close(
        &mut self,
        prev: &PositionState,
        fill: &FillEvent,
        cumulative_exit: f64,
    ) {
        let rec = HistoryPosition {
            position_id: prev.position_id.clone(),
            strategy_name: self.strategy_name.clone(),
            symbol: prev.symbol.clone(),
            position_type: prev.side,
            entry_price: prev.entry_price,
            exit_price: fill.filled_price,
            entry_time: Some(prev.entry_time),
            exit_time: fill.event_time,
            entry_timestamp: prev.entry_timestamp,
            exit_timestamp: fill.event_time.timestamp(),
            peak_price: prev.peak_price,
            stop_price: prev.stop_price,
            max_pnl_pct: prev.max_pnl_pct,
            min_pnl_pct: prev.min_pnl_pct,
            exit_reason: "partial_close".into(),
            is_stop_loss: fill.is_stop_loss,
            atr_at_entry: prev.atr_at_entry,
            trail_activated: prev.trail_activated,
            trading_mode: self.trading_mode.clone(),
            entry_size: prev.total_size,
            exit_size: cumulative_exit,
        };
        match self.history.insert(rec.clone()) {
            Ok(()) => info!(
                position_id = %prev.position_id,
                exit_size = cumulative_exit,
                "部分平仓：累计历史进度已写入"
            ),
            Err(strategy_store::StoreError::AlreadyExists { .. }) => {
                let id = prev.position_id.clone();
                self.history
                    .with(&id, |r| {
                        r.exit_price = rec.exit_price;
                        r.exit_time = rec.exit_time;
                        r.exit_timestamp = rec.exit_timestamp;
                        r.exit_size = rec.exit_size;
                        r.exit_reason = rec.exit_reason.clone();
                        r.is_stop_loss = rec.is_stop_loss;
                    })
                    .expect("累计进度行刚报 AlreadyExists 必存在");
                info!(
                    position_id = %prev.position_id,
                    exit_size = cumulative_exit,
                    "部分平仓：累计历史进度已更新"
                );
            }
            Err(e) => warn!(
                error = %e, position_id = %prev.position_id,
                "部分平仓历史进度写入失败（不阻断持仓状态）"
            ),
        }
    }

    /// 反向减仓/平仓 FillEvent。部分减仓保留仓位并累计 realized_pnl；
    /// 全部平掉走历史记录 + 冷却（与 [`Self::close`] 同语义）。
    pub fn apply_close_fill(
        &mut self,
        fill: &FillEvent,
        is_stop_loss: bool,
        exit_reason: &str,
    ) -> std::io::Result<Option<HistoryPosition>> {
        let Some(prev) = self.current_row() else {
            tracing::error!(?fill.signal_id, "平仓成交到达但无仓位，忽略（不反手开仓）");
            return Ok(None);
        };
        // position_id 一致性：本 fill 必须针对当前仓位（防止旧仓回报误平新仓）
        if fill
            .position_id
            .as_deref()
            .is_some_and(|pid| pid != prev.state.position_id)
        {
            tracing::error!(
                fill_pid = ?fill.position_id,
                current = %prev.state.position_id,
                "平仓成交的 position_id 与当前仓位不一致，忽略"
            );
            return Ok(None);
        }
        // 已成交量以 total 计，可平量以 remaining 计（加仓后二者可能不同）
        let held = if prev.state.remaining_size > 0.0 {
            prev.state.remaining_size
        } else {
            prev.state.total_size
        };
        let pnl = realized_pnl(
            match prev.state.side {
                Side::Long => strategy_core::Side::Long,
                Side::Short => strategy_core::Side::Short,
            },
            prev.state.entry_price,
            fill.filled_size
                .min(if held > 0.0 { held } else { fill.filled_size }),
            fill.filled_price,
        );
        let full_close = held <= 0.0 || fill.filled_size + 1e-9 >= held;

        if !full_close {
            // 本笔成交 raw（优先回报字面量）：剩余可平量精确递减
            let closed_raw = fill
                .filled_size_raw
                .clone()
                .unwrap_or_else(|| fill.filled_size.to_string());
            let new_remaining_raw =
                Self::subtract_raw(prev.state.remaining_size_raw.as_deref(), &closed_raw);
            let new_remaining = (prev.state.remaining_size - fill.filled_size).max(0.0);
            self.positions
                .with(&prev.state.position_id, |r| {
                    r.state.remaining_size = new_remaining;
                    r.state.remaining_size_raw = new_remaining_raw.clone();
                    r.state.realized_pnl += pnl;
                    r.version += 1;
                })
                .expect("持仓行刚读取存在");
            // 部分平仓立即写累计历史进度（崩溃恢复可知已平部分；对外固定列
            // CSV 契约不变，仅最终平仓时写）
            let cumulative_exit = prev.state.total_size - new_remaining;
            self.record_partial_close(&prev.state, fill, cumulative_exit);
            return Ok(None);
        }
        self.close(
            fill.filled_price,
            fill.event_time,
            exit_reason,
            is_stop_loss,
            fill.filled_size,
        )
    }

    /// 行情更新：跟踪峰值/极值（行 updated_at 随 Store 时钟刷新；定时 flush 落盘，
    /// 崩溃可从最近一次当日快照恢复）。传入 1m K 线 high/low。
    pub fn on_price(&mut self, high: f64, low: f64) -> std::io::Result<()> {
        let Some(current) = self.current_row() else {
            return Ok(());
        };
        self.last_price = Some((high + low) / 2.0);
        self.positions
            .with(&current.state.position_id, |r| {
                r.state.on_price(high, low);
                r.version += 1;
            })
            .expect("持仓行刚读取存在");
        let fresh = self.position().expect("持仓行刚更新存在");
        debug!(
            strategy = %self.strategy_name, symbol = fresh.symbol,
            high, low, peak_price = fresh.peak_price,
            max_pnl_pct = fresh.max_pnl_pct, min_pnl_pct = fresh.min_pnl_pct,
            "仓位状态：on_price 已更新峰值/浮盈极值"
        );
        Ok(())
    }

    /// 更新止损价（移动止损等；只往有利方向由策略保证）。
    /// 无仓位时为 no-op。
    pub fn update_stop(&mut self, stop_price: f64) -> std::io::Result<()> {
        let Some(pos) = self.current_row() else {
            return Ok(());
        };
        let stop_id = stop_order_id(&pos.state.position_id);
        let existed = self
            .stops
            .with(&stop_id, |s| {
                s.lifecycle = "updated".into();
                s.trigger_price = stop_price;
                s.version += 1;
            })
            .is_some();
        if !existed {
            // 防御：止损行理论上随开仓建立；缺失则补建（不阻断止损更新）
            self.stops.upsert(ManagedStop {
                stop_order_id: stop_id.clone(),
                strategy_name: self.strategy_name.clone(),
                position_id: pos.state.position_id.clone(),
                version: 1,
                lifecycle: "created".into(),
                trigger_price: stop_price,
                size: pos.state.total_size,
            });
        }
        self.positions
            .with(&pos.state.position_id, |r| {
                r.state.stop_price = stop_price;
                r.version += 1;
            })
            .expect("持仓行刚读取存在");
        debug!(
            strategy = %self.strategy_name, position_id = %pos.state.position_id,
            old_stop = pos.state.stop_price, new_stop = stop_price,
            "仓位状态：止损价已更新（stop 行同步）"
        );
        Ok(())
    }

    /// 更新 SAR 触及制离场状态（策略 refresh_exit_state 变化时回写）。
    /// exit 是持仓子状态，只更新 position 行，无独立 model。无仓位时 no-op。
    pub fn update_exit(&mut self, exit_active: bool, exit_price: f64) -> std::io::Result<()> {
        let Some(pos) = self.current_row() else {
            return Ok(());
        };
        self.positions
            .with(&pos.state.position_id, |r| {
                r.state.exit_active = exit_active;
                r.state.exit_price = exit_price;
                r.version += 1;
            })
            .expect("持仓行刚读取存在");
        debug!(
            strategy = %self.strategy_name, position_id = %pos.state.position_id,
            exit_active, exit_price,
            "仓位状态：SAR 离场状态已更新（position 行同步）"
        );
        Ok(())
    }

    /// 更新回落止盈持仓级状态（统一风控激活/刷新触发线时调用）。
    /// 无仓位时为 no-op。
    pub fn update_trail_state(
        &mut self,
        trail_activated: bool,
        trail_trigger_pct: f64,
    ) -> std::io::Result<()> {
        let Some(current) = self.current_row() else {
            return Ok(());
        };
        self.positions
            .with(&current.state.position_id, |r| {
                r.state.trail_activated = trail_activated;
                r.state.trail_trigger_pct = trail_trigger_pct;
                r.version += 1;
            })
            .expect("持仓行刚读取存在");
        Ok(())
    }

    /// 平仓：history_position 建行 + 对外 CSV 导出 + 止损单归档（cancelled）+
    /// 当前持仓软删 +（止损时）写冷却日（K 线时间）。
    ///
    /// 时序对齐 Python `_notify_exit_and_clear`：历史记录失败不阻断清除
    /// （失败只记日志），清除保证幂等（软删行二次 close 返回 None）。
    pub fn close(
        &mut self,
        exit_price: f64,
        exit_time: DateTime<Utc>,
        exit_reason: &str,
        is_stop_loss: bool,
        // 本次平仓成交量（平仓 fill 的 filled_quantity）
        exit_size: f64,
    ) -> std::io::Result<Option<HistoryPosition>> {
        let Some(row) = self.current_row() else {
            return Ok(None);
        };
        let state = row.state;

        let rec = HistoryPosition {
            position_id: state.position_id.clone(),
            strategy_name: self.strategy_name.clone(),
            symbol: self.symbol.clone(),
            position_type: state.side,
            entry_price: state.entry_price,
            exit_price,
            entry_time: Some(state.entry_time),
            exit_time,
            entry_timestamp: state.entry_timestamp,
            exit_timestamp: exit_time.timestamp(),
            peak_price: state.peak_price,
            stop_price: state.stop_price,
            max_pnl_pct: state.max_pnl_pct,
            min_pnl_pct: state.min_pnl_pct,
            exit_reason: exit_reason.to_string(),
            is_stop_loss,
            atr_at_entry: state.atr_at_entry,
            trail_activated: state.trail_activated,
            trading_mode: self.trading_mode.clone(),
            entry_size: state.total_size,
            exit_size,
        };

        // 1. 历史行入 Store（position_id 全局唯一；只增不改）。
        //    崩溃恢复重放同一平仓时行已存在：该 model 不可变，视为成功继续清除
        //    仓位（基线 append-only CSV + 无条件清除也是此语义），不得卡住持仓。
        if let Err(e) = self.history.insert(rec.clone()) {
            if !matches!(e, strategy_store::StoreError::AlreadyExists { .. }) {
                return Err(std::io::Error::other(format!(
                    "history_position 行写入失败: {e}"
                )));
            }
            // 部分平仓已写过累计进度行：用最终记录整行替换（含 exit_reason、
            // exit_size 全量），随后照常写对外固定列 CSV
            self.history
                .with(&rec.position_id, |r| *r = rec.clone())
                .expect("历史行刚报 AlreadyExists 必存在");
        }
        // 2. 对外固定列 CSV（失败仅告警，不阻断清除）
        if let Err(e) = self.history_writer.log_exit(&rec) {
            warn!(error = %e, position_id = %state.position_id, "历史仓位记录失败（不阻断清除）");
        }
        // 3. 止损单归档（终态行留审计，不物理删除）
        self.stops.with(&stop_order_id(&state.position_id), |s| {
            s.lifecycle = "cancelled".into();
            s.version += 1;
        });
        // 4. 当前持仓软删（跨日 flush 后成墓碑，恢复不复活）
        self.positions.delete(&state.position_id);
        // 5. 止损 → 记冷却日（K 线时间，非 now()）
        if is_stop_loss {
            self.cooldowns.upsert(Cooldown {
                strategy_name: self.strategy_name.clone(),
                date: exit_time.format("%Y-%m-%d").to_string(),
            });
            info!(
                strategy = %self.strategy_name, position_id = %state.position_id,
                cooldown_date = %exit_time.format("%Y-%m-%d"),
                "仓位状态：止损冷却日已设置（当日拦截新开仓）"
            );
        }

        info!(
            strategy = %self.strategy_name, position_id = %state.position_id,
            symbol = state.symbol, side = %state.side,
            entry_price = state.entry_price, exit_price,
            exit_reason, is_stop_loss,
            realized_pnl_after = rec.pnl_pct(),
            "平仓：当前持仓软删 + 止损单 cancelled + 历史仓已记录"
        );
        Ok(Some(rec))
    }

    /// 外部已平仓对账：交易所已无仓位（reduce_only 平仓单被拒，典型
    /// `ReduceOnly Order is rejected`），本地仍是幽灵仓。以最近 tick 价格
    /// （无则入场价）按 external_close 落 history + 软删当前持仓；非止损，
    /// 不设冷却。正常 reduce_only 成交路径仍走 [`Self::apply_close_fill`]。
    pub fn external_close(
        &mut self,
        exit_time: DateTime<Utc>,
    ) -> std::io::Result<Option<HistoryPosition>> {
        let Some(row) = self.current_row() else {
            return Ok(None);
        };
        let exit_size = row.state.total_size;
        let exit_price = self.last_price.unwrap_or(row.state.entry_price);
        let rec = self.close(exit_price, exit_time, "external_close", false, exit_size)?;
        if rec.is_some() {
            info!(
                strategy = %self.strategy_name,
                position_id = %row.state.position_id,
                exit_price,
                "平仓对账：交易所已无仓位，本地幽灵仓按 external_close 关闭"
            );
        }
        Ok(rec)
    }

    /// 启动恢复（崩溃/重启/监督重建调用）：跨日去重载入四张表，返回恢复到的持仓。
    ///
    /// 同一实例只盘载一次：同进程监督重建复用进程级 Store，其中可能有比最近
    /// flush 更新、尚未落盘的行——recover 前先快照内存当前态，回读后按 version
    /// 保留较新行；冷启动（空表）等同纯盘载。
    pub fn restore(&mut self) -> std::io::Result<Option<PositionState>> {
        if !self.loaded {
            // Store 的盘载保证"只填空缺 id"：活行与墓碑永不被磁盘旧快照覆盖
            // （见 ModelTable::load_merged）。因此冷启动等同纯盘载；同进程监督
            // 重建时本实例与兄弟实例的未 flush 状态都在共享活表里，逐行合并已无必要。
            // P1-3：单表失败仍 log-and-continue（fail-closed 收紧待后续）。
            for (model, res) in [
                ("position", self.store.recover::<ManagedPosition>()),
                ("stop", self.store.recover::<ManagedStop>()),
                ("cooldown", self.store.recover::<Cooldown>()),
                ("history_position", self.store.recover::<HistoryPosition>()),
            ] {
                if let Err(e) = res {
                    warn!(error = %e, model, "{model} 表盘载失败，继续以内存态运行");
                }
            }
            self.loaded = true;
            // 统一 Store 后 position/stop 多实例共表：分别报告本实例数与全表数。
            let (pos_mine, pos_total) =
                scope_split(&self.positions.list(), &self.strategy_name, |r| {
                    r.strategy_name.as_str()
                });
            let (stop_mine, stop_total) =
                scope_split(&self.stops.list(), &self.strategy_name, |r| {
                    r.strategy_name.as_str()
                });
            debug!(
                strategy = %self.strategy_name,
                positions_mine = pos_mine,
                positions_total = pos_total,
                stops_mine = stop_mine,
                stops_total = stop_total,
                cooldowns_loaded = self.cooldowns.list().len(),
                history_loaded = self.history.list().len(),
                "仓位恢复：Store 跨日去重 recover 完成（mine=本实例/total=全表）"
            );
        }
        Ok(self.position())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::atomic::{AtomicI64, Ordering};
    use strategy_core::IntentKind;
    use strategy_store::Clock;

    const NAME: &str = "sar_snt3_8H_3_BTCUSDT_LIVE";

    /// 固定时钟：2026-06-10 00:00 UTC（当日目录 20260610；测试不引墙钟）。
    struct FixedClock(AtomicI64);
    impl Clock for FixedClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn store_at(dir: &Path) -> Arc<Store> {
        Arc::new(Store::new(
            dir.join("state"),
            Arc::new(FixedClock(AtomicI64::new(1_781_049_600_000))),
        ))
    }

    fn manager_at(dir: &Path) -> PositionManager {
        PositionManager::new(NAME, "BTCUSDT", "backtest", store_at(dir), dir)
    }

    fn manager_with(store: Arc<Store>, dir: &Path) -> PositionManager {
        PositionManager::new(NAME, "BTCUSDT", "backtest", store, dir)
    }

    #[test]
    fn scope_split_counts_mine_and_total() {
        struct Row {
            owner: &'static str,
        }
        let rows = [
            Row {
                owner: "a_BTC_LIVE",
            },
            Row {
                owner: "a_ETH_LIVE",
            },
            Row {
                owner: "a_BTC_LIVE",
            },
        ];
        let (mine, total) = scope_split(&rows, "a_BTC_LIVE", |r| r.owner);
        assert_eq!((mine, total), (2, 3));
        let (mine, total) = scope_split(&rows, "a_XLM_LIVE", |r| r.owner);
        assert_eq!((mine, total), (0, 3));
    }

    #[test]
    fn open_close_full_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        assert!(!m.is_open());

        let t0 = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
        m.open("sig_1".into(), Side::Long, 100.0, t0, 98.0, 1.5)
            .unwrap();

        // 当前持仓与止损单行已建（position/stop model）
        assert!(m.is_open());
        assert_eq!(m.position().unwrap().position_id, "sig_1");
        let stop = m.stop_row("sig_1").expect("止损单行存在");
        assert_eq!(stop.stop_order_id, "stop_sig_1_sl");
        assert_eq!(stop.lifecycle, "created");
        assert_eq!(stop.trigger_price, 98.0);
        assert_eq!(stop.size, 0.0);
        assert_eq!(stop.version, 1);

        m.on_price(105.0, 104.0).unwrap();
        m.on_price(104.0, 101.0).unwrap();

        let t1 = Utc.with_ymd_and_hms(2026, 6, 10, 1, 0, 0).unwrap();
        let rec = m
            .close(103.0, t1, "trailing_stop", false, 1.0)
            .unwrap()
            .unwrap();
        assert_eq!(rec.position_id, "sig_1");
        assert!((rec.pnl_pct() - 3.0).abs() < 1e-9);
        assert_eq!(rec.peak_price, 105.0);

        // 平仓后：持仓行软删（墓碑）、止损单归档、历史入表 + 对外 CSV 导出
        assert!(!m.is_open());
        assert_eq!(m.position(), None);
        assert!(m.positions.meta("sig_1").unwrap().deleted);
        assert_eq!(m.stop_row("sig_1").unwrap().lifecycle, "cancelled");
        assert_eq!(m.history().len(), 1);
        assert_eq!(m.history()[0].position_id, "sig_1");
        assert!(dir
            .path()
            .join("history_positions")
            .join(NAME)
            .join("20260610.csv")
            .exists());

        // 再开仓正常（新持仓行 + 新止损单行）
        let t2 = Utc.with_ymd_and_hms(2026, 6, 11, 0, 0, 0).unwrap();
        m.open("sig_2".into(), Side::Short, 103.0, t2, 105.0, 1.2)
            .unwrap();
        assert_eq!(m.position().unwrap().position_id, "sig_2");
        assert_eq!(m.stop_row("sig_2").unwrap().lifecycle, "created");
        // sig_1 的审计行不受影响
        assert_eq!(m.stop_row("sig_1").unwrap().lifecycle, "cancelled");
    }

    #[test]
    fn external_close_uses_last_tick_price_and_marks_external_close_reason() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        let t0 = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
        m.open("sig_1".into(), Side::Long, 100.0, t0, 98.0, 1.5)
            .unwrap();

        // 最近 tick 高低点 103/101 → mid 102 作为 exit_price
        m.on_price(103.0, 101.0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 6, 10, 1, 0, 0).unwrap();
        let rec = m.external_close(t1).unwrap().unwrap();
        assert_eq!(rec.position_id, "sig_1");
        assert_eq!(rec.exit_reason, "external_close");
        assert_eq!(rec.exit_price, 102.0);
        assert!(!rec.is_stop_loss, "外部平仓非止损");

        // 当前持仓已软删、历史入表；非止损 → 无冷却日
        assert!(!m.is_open());
        assert_eq!(m.position(), None);
        assert_eq!(m.history().len(), 1);
        assert_eq!(m.stop_loss_cooldown(), None);

        // 无仓位再调为 no-op
        assert!(m.external_close(t1).unwrap().is_none());
    }

    #[test]
    fn stop_loss_sets_cooldown_date_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = manager_at(dir.path());
            let t0 = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
            m.open("sig_sl".into(), Side::Long, 100.0, t0, 98.0, 0.0)
                .unwrap();
            let t1 = Utc.with_ymd_and_hms(2026, 6, 10, 5, 0, 0).unwrap();
            m.close(98.0, t1, "stop_loss", true, 1.0).unwrap();
            assert_eq!(
                m.stop_loss_cooldown(),
                Some(strategy_core::strategy::Cooldown::UntilDate(
                    "2026-06-10".into()
                ))
            );
            m.store().flush().unwrap();
            // 冷却日行已随当日快照落盘
            assert!(dir.path().join("state/20260610/cooldown.csv").exists());
        }

        // 新进程视角：持仓为墓碑（不复活），冷却日仍可读
        let mut m2 = manager_at(dir.path());
        assert!(m2.restore().unwrap().is_none());
        assert_eq!(
            m2.stop_loss_cooldown(),
            Some(strategy_core::strategy::Cooldown::UntilDate(
                "2026-06-10".into()
            ))
        );
    }

    #[test]
    fn update_exit_persists_and_restores() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut m = manager_at(dir.path());
            let t0 = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
            m.open("sig_x".into(), Side::Long, 100.0, t0, 98.0, 0.0)
                .unwrap();
            // SAR 反向激活的离场状态回写持仓行
            m.update_exit(true, 103.5).unwrap();
            let p = m.position().unwrap();
            assert!(p.exit_active);
            assert_eq!(p.exit_price, 103.5);
            m.store().flush().unwrap();
            assert!(dir.path().join("state/20260610/position.csv").exists());
        } // 进程退出

        // 重启 restore：exit 状态随 position 行恢复，继续保护
        let mut m2 = manager_at(dir.path());
        let restored = m2.restore().unwrap().unwrap();
        assert!(restored.exit_active, "exit_active 应持久化恢复");
        assert_eq!(restored.exit_price, 103.5, "exit_price 应持久化恢复");

        // 取消回写（平仓流程之外的防御场景）同样落盘
        m2.update_exit(false, 0.0).unwrap();
        assert!(!m2.position().unwrap().exit_active);
    }

    #[test]
    fn update_exit_without_position_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        m.update_exit(true, 103.5).unwrap(); // 无仓不 panic
        assert!(m.position().is_none());
    }

    #[test]
    fn non_stop_loss_no_cooldown() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        let t0 = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
        m.open("sig_ok".into(), Side::Long, 100.0, t0, 98.0, 0.0)
            .unwrap();
        m.close(103.0, t0, "trailing_stop", false, 1.0).unwrap();
        assert!(m.stop_loss_cooldown().is_none());
    }

    #[test]
    fn partial_close_decrements_raw_and_records_progress() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());

        // 开仓成交（精确量 raw）
        let open = fill(
            "sig_1",
            "ord_open",
            strategy_core::Side::Long,
            0.86361122,
            115.89,
        )
        .with_fill_meta(IntentKind::Open, None, Some("0.86361122".into()));
        m.apply_open_fill(&open, t(), 114.67, 0.0).unwrap();

        // 部分平仓成交 0.86（交易所按 stepSize 截断）
        let cf = strategy_core::FillEvent::new(
            "sig_cf",
            "ord_cf",
            "BTCUSDT",
            strategy_core::Side::Short,
            0.86,
            116.95,
            0.0,
            t(),
        )
        .with_exit(false, "live_close")
        .with_fill_meta(
            IntentKind::Close,
            Some("sig_1".to_string()),
            Some("0.86".into()),
        );
        m.apply_close_fill(&cf, false, "live_close").unwrap();

        // 仓位仍在：f64 与 raw 都精确递减（不再是开仓全量）
        let p = m.position().unwrap();
        assert!((p.remaining_size - 0.00361122).abs() < 1e-12);
        assert_eq!(p.remaining_size_raw.as_deref(), Some("0.00361122"));

        // 部分平仓也有历史记录（累计进度行）
        let h = m.history();
        assert_eq!(h.len(), 1);
        assert_eq!(h[0].position_id, "sig_1");
        assert!((h[0].exit_size - 0.86).abs() < 1e-12);
        assert_eq!(h[0].exit_reason, "partial_close");

        // 对外固定列 CSV 契约不变：仅最终平仓时写
        assert!(!dir.path().join("history_positions").exists());
    }

    #[test]
    fn restore_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
        {
            let mut m = manager_at(dir.path());
            m.open("sig_keep".into(), Side::Long, 100.0, t0, 98.0, 1.0)
                .unwrap();
            m.on_price(104.0, 100.0).unwrap();
            m.store().flush().unwrap();
        } // 进程"崩溃"

        // 重启恢复：峰值/极值/止损单当前态保留
        let mut m2 = manager_at(dir.path());
        let restored = m2.restore().unwrap().unwrap();
        assert_eq!(restored.position_id, "sig_keep");
        assert_eq!(restored.peak_price, 104.0);
        assert_eq!(restored.max_pnl_pct, 4.0);
        let stop = m2.stop_row("sig_keep").unwrap();
        assert_eq!(stop.lifecycle, "created");
        assert_eq!(stop.trigger_price, 98.0);
    }

    fn t() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn fill(
        signal_id: &str,
        order_id: &str,
        side: strategy_core::Side,
        size: f64,
        price: f64,
    ) -> strategy_core::FillEvent {
        strategy_core::FillEvent::new(signal_id, order_id, "BTCUSDT", side, size, price, 0.0, t())
    }

    #[test]
    fn apply_open_then_add_recomputes_average() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        m.apply_open_fill(
            &fill("sig_1", "o1", strategy_core::Side::Long, 100.0, 100.0),
            t(),
            98.0,
            1.5,
        )
        .unwrap();
        m.apply_add_fill(&fill(
            "sig_2",
            "o2",
            strategy_core::Side::Long,
            100.0,
            110.0,
        ))
        .unwrap();
        let p = m.position().unwrap();
        assert!((p.entry_price - 105.0).abs() < 1e-9);
        assert!((p.total_size - 200.0).abs() < 1e-9);
        assert!((p.remaining_size - 200.0).abs() < 1e-9);
    }

    #[test]
    fn close_fill_without_position_is_noop_not_reverse_open() {
        // 平仓 fill 到达时无仓位：必须 no-op，不能反手当成开仓
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        let close = fill("sig_c", "o_c", strategy_core::Side::Short, 1.0, 100.0).with_fill_meta(
            IntentKind::Close,
            Some("ghost-pos".into()),
            Some("1".into()),
        );
        let out = m.apply_close_fill(&close, false, "live_close").unwrap();
        assert!(out.is_none());
        assert!(!m.is_open(), "无仓平仓 fill 不得反手开新仓");
    }

    #[test]
    fn close_fill_position_id_mismatch_is_noop() {
        // fill 针对的 position_id 与当前仓位不一致（旧仓迟到回报）：忽略
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        m.apply_open_fill(
            &fill("sig_1", "o1", strategy_core::Side::Long, 1.0, 100.0),
            t(),
            98.0,
            1.5,
        )
        .unwrap();
        let stale = fill("sig_old", "o_old", strategy_core::Side::Short, 1.0, 105.0)
            .with_fill_meta(
                IntentKind::Close,
                Some("already-gone-pos".into()),
                Some("1".into()),
            );
        let out = m.apply_close_fill(&stale, false, "live_close").unwrap();
        assert!(out.is_none());
        // 当前仓位不受影响
        assert_eq!(m.position().unwrap().position_id, "sig_1");
    }

    #[test]
    fn partial_reduce_keeps_position_and_accumulates_pnl() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        m.apply_open_fill(
            &fill("sig_1", "o1", strategy_core::Side::Long, 100.0, 100.0),
            t(),
            98.0,
            1.5,
        )
        .unwrap();
        // 减仓 40@105 → realized 200，仓位仍在
        let rec = m
            .apply_close_fill(
                &fill("sig_2", "o2", strategy_core::Side::Short, 40.0, 105.0),
                false,
                "reduce",
            )
            .unwrap();
        assert!(rec.is_none(), "部分减仓不写历史记录");
        let p = m.position().unwrap();
        assert!((p.remaining_size - 60.0).abs() < 1e-9);
        assert!((p.realized_pnl - 200.0).abs() < 1e-9);
        assert!(m.is_open());
        // 止损单在部分减仓时不归档
        assert_eq!(m.stop_row("sig_1").unwrap().lifecycle, "created");
        // 剩余全平 → 关闭入历史
        let rec = m
            .apply_close_fill(
                &fill("sig_3", "o3", strategy_core::Side::Short, 60.0, 105.0),
                false,
                "close",
            )
            .unwrap();
        assert!(rec.is_some());
        assert!(!m.is_open());
        assert_eq!(m.history().len(), 1);
        assert_eq!(m.stop_row("sig_1").unwrap().lifecycle, "cancelled");
    }

    /// 旧 position_events/stop_events 追加日志断言的等价迁移：
    /// 开仓建 position(v1)+stop(v1,created)；update_stop → stop(v2,updated)
    /// 且持仓行 stop_price 同步；全平 → stop(v3,cancelled)。
    #[test]
    fn stop_lifecycle_created_updated_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        m.apply_open_fill(
            &fill("sig_1", "o1", strategy_core::Side::Long, 2.0, 100.0),
            t(),
            98.0,
            1.5,
        )
        .unwrap();
        assert_eq!(m.current_row().unwrap().version, 1);
        let stop = m.stop_row("sig_1").unwrap();
        assert_eq!(stop.version, 1, "stop 是独立聚合，version 从 1 起");
        assert_eq!(stop.lifecycle, "created");
        assert!((stop.size - 2.0).abs() < 1e-9);

        m.update_stop(99.0).unwrap();
        let stop = m.stop_row("sig_1").unwrap();
        assert_eq!(stop.lifecycle, "updated");
        assert_eq!(stop.trigger_price, 99.0);
        assert_eq!(stop.version, 2);
        assert_eq!(m.position().unwrap().stop_price, 99.0);

        m.apply_close_fill(
            &fill("sig_2", "o2", strategy_core::Side::Short, 2.0, 105.0),
            false,
            "close",
        )
        .unwrap();
        let stop = m.stop_row("sig_1").unwrap();
        assert_eq!(stop.lifecycle, "cancelled");
        assert_eq!(stop.version, 3);
    }

    #[test]
    fn close_without_position_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        assert!(m.close(100.0, t(), "manual", false, 1.0).unwrap().is_none());
    }

    #[test]
    fn seeded_cooldown_row_is_visible() {
        // live 读管道：cooldown 表已有行（启动/跨日恢复后）→ manager 立即可读，
        // 由外壳注入策略 Ctx。
        let dir = tempfile::tempdir().unwrap();
        let store = store_at(dir.path());
        store
            .table::<Cooldown>()
            .insert(Cooldown {
                strategy_name: NAME.into(),
                date: "2026-01-01".into(),
            })
            .unwrap();
        let m = manager_with(store, dir.path());
        assert_eq!(
            m.stop_loss_cooldown(),
            Some(strategy_core::strategy::Cooldown::UntilDate(
                "2026-01-01".into()
            ))
        );
    }

    /// B11 同进程监督重建：gen1 flush 旧态后又在内存推进（未再 flush），
    /// gen2（新 manager、同一进程级 Store）restore 不得用旧快照回滚；
    /// gen3 未 flush 的平仓（软删行）同样不得复活。
    #[test]
    fn restore_within_process_keeps_unflushed_newer_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_at(dir.path());
        {
            let mut g1 = manager_with(Arc::clone(&store), dir.path());
            g1.apply_open_fill(
                &fill("sig_1", "o1", strategy_core::Side::Long, 100.0, 100.0),
                t(),
                98.0,
                1.5,
            )
            .unwrap();
            g1.store().flush().unwrap(); // 日文件停在 v1/peak100
            g1.on_price(110.0, 100.0).unwrap(); // 内存推进，未 flush
            assert_eq!(g1.position().unwrap().peak_price, 110.0);
        }

        // panic 后整代重建：新 manager 复用存活的进程级 Store
        let mut g2 = manager_with(Arc::clone(&store), dir.path());
        let restored = g2.restore().unwrap().unwrap();
        assert_eq!(restored.peak_price, 110.0, "未 flush 的峰值不得回滚");
        assert!((restored.max_pnl_pct - 10.0).abs() < 1e-9);

        // 平仓后不再 flush，新一代恢复：旧快照有开仓行，但内存软删更新
        {
            let mut g3 = manager_with(Arc::clone(&store), dir.path());
            g3.restore().unwrap();
            g3.apply_close_fill(
                &fill("sig_2", "o2", strategy_core::Side::Short, 100.0, 110.0),
                false,
                "close",
            )
            .unwrap();
            assert!(!g3.is_open());
            let mut g4 = manager_with(Arc::clone(&store), dir.path());
            assert!(g4.restore().unwrap().is_none(), "未 flush 的平仓不得复活");
        }
    }

    /// 多策略实例共享同一张表：position/history 集合查询严格按 strategy_name 隔离。
    #[test]
    fn shared_table_isolates_instances_by_strategy_name() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_at(dir.path());
        let mut a = PositionManager::new(
            "strat_A",
            "BTCUSDT",
            "backtest",
            Arc::clone(&store),
            dir.path(),
        );
        let mut b = PositionManager::new(
            "strat_B",
            "BTCUSDT",
            "backtest",
            Arc::clone(&store),
            dir.path(),
        );
        a.open("posA".into(), Side::Long, 100.0, t(), 98.0, 1.0)
            .unwrap();
        b.open("posB".into(), Side::Long, 200.0, t(), 196.0, 1.0)
            .unwrap();

        assert_eq!(a.position().unwrap().position_id, "posA");
        assert_eq!(b.position().unwrap().position_id, "posB");
        // 物理总表两行，各自只读到自己的当前持仓
        assert_eq!(a.positions.list().len(), 2);
        assert!(a.is_open());
        assert!(b.is_open());

        a.close(101.0, t(), "close", false, 1.0).unwrap();
        assert!(!a.is_open());
        assert!(b.is_open());
        assert_eq!(a.history().len(), 1);
        assert_eq!(a.history()[0].strategy_name, "strat_A");
        assert!(b.history().is_empty());
    }

    /// 评审 C1 回归：实例 A 监督重建（新 manager 重跑 restore，盘载覆盖全表）
    /// 不得回滚/复活兄弟实例 B 未 flush 的状态。
    #[test]
    fn restore_of_one_instance_never_rolls_back_or_resurrects_another() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_at(dir.path());
        let mk = |name: &str| {
            PositionManager::new(name, "BTCUSDT", "live", Arc::clone(&store), dir.path())
        };

        // B 开仓并 flush（日文件停在 B 持仓 active）
        let mut b = mk("strat_B");
        b.open("posB".into(), Side::Long, 100.0, t(), 98.0, 1.0)
            .unwrap();
        store.flush().unwrap();
        // B 未 flush 平仓（软删行仍在共享活表）；A 未 flush 开仓
        b.close(105.0, t(), "close", false, 1.0).unwrap();
        let mut a = mk("strat_A");
        a.open("posA".into(), Side::Long, 200.0, t(), 196.0, 1.0)
            .unwrap();

        // A panic 后整代重建：新 manager restore，盘载是全表旧快照
        let mut a2 = mk("strat_A");
        a2.restore().unwrap();
        // 同理新建 B 代 manager
        let mut b2 = mk("strat_B");
        b2.restore().unwrap();

        // B 的未 flush 平仓不得复活，历史行仍在；A 的未 flush 开仓不得回滚
        assert!(b2.position().is_none(), "兄弟实例平仓被旧快照复活");
        assert_eq!(b2.history().len(), 1);
        assert_eq!(a2.position().unwrap().position_id, "posA");

        // 再 flush：落盘态必须与内存一致（重新冷启动仍为 B 无仓/A 有仓）
        store.flush().unwrap();
        let cold_store = Arc::new(Store::new(
            dir.path().join("state"),
            Arc::new(strategy_store::EpochClock),
        ));
        let mut b3 = PositionManager::new("strat_B", "BTCUSDT", "live", cold_store, dir.path());
        b3.restore().unwrap();
        assert!(b3.position().is_none(), "冷启动复活已平仓持仓");
    }

    /// 评审 R2 回归：跨表 flush 非原子，崩溃窗口可造成"history 已落盘、
    /// position 仍是活行"；对账重放同一平仓时 close 必须幂等软删，不被
    /// history AlreadyExists 卡住。
    #[test]
    fn close_is_idempotent_when_history_row_already_exists() {
        use crate::history::HistoryPosition;

        let dir = tempfile::tempdir().unwrap();
        let mut m = manager_at(dir.path());
        let t0 = t();
        m.open("pos1".into(), Side::Long, 100.0, t0, 98.0, 1.0)
            .unwrap();
        // 模拟崩溃窗口：历史行已随当日快照落盘，持仓行还停在旧快照（活行）。
        m.history
            .insert(HistoryPosition {
                position_id: "pos1".into(),
                strategy_name: NAME.into(),
                symbol: "BTCUSDT".into(),
                position_type: Side::Long,
                entry_price: 100.0,
                exit_price: 105.0,
                entry_time: Some(t0),
                exit_time: t(),
                entry_timestamp: t0.timestamp(),
                exit_timestamp: t().timestamp(),
                peak_price: 105.0,
                stop_price: 98.0,
                max_pnl_pct: 5.0,
                min_pnl_pct: 0.0,
                exit_reason: "close".into(),
                is_stop_loss: false,
                atr_at_entry: 1.0,
                trail_activated: false,
                trading_mode: "backtest".into(),
                entry_size: 1.0,
                exit_size: 1.0,
            })
            .unwrap();

        let rec = m.close(105.0, t(), "close", false, 1.0).unwrap().unwrap();
        assert_eq!(rec.position_id, "pos1");
        assert!(!m.is_open(), "history 撞键不得阻断持仓软删");
    }
}
