//! SignalManager —— 信号全生命周期（设计文档 §5.2；S3 统一 Store 后端）
//!
//! 同步内核：回测逐根 K 线直接调用（确定性，不 spawn）；live 由 RuntimeActor
//! 在同一 task 内调用，网络侧经 [`crate::gateway::AsyncOrderGateway`] await。
//!
//! 存储：信号当前态 [`ManagedSignal`] 是进程共享统一 Store 中 `NAME="signal"`
//! 的 model。行常驻表内（Created..Filled 全状态保留；Rejected/Failed/Cancelled
//! 不删除——审计红线），定时 flush 到 `{base}/{YYYYMMDD}/signal.csv`。
//! 多个策略实例共享同一张表（signal_id 全局唯一），本 manager 的集合查询
//! 一律按 `strategy_name` 过滤。
//!
//! 职责链：
//! ```text
//! receive_intent  → 建行 Created（Store insert）
//!                 → validate（失败转 Rejected，审计可见）
//!                 → gateway.submit（成功转 Submitted+order_id；
//!                    失败按 Retryable/Rejected 转 Failed 并安排重试）
//! handle_order_update → Accepted / Filled（产出 FillEvent）/ Cancelled
//! tick(now)          → 到期重试（Failed → Submitted，指数退避）
//! recover            → store.recover 载入当前态 → 重建 order_index，
//!                      产出 Resubmit/Query 对账动作
//! ```
//!
//! 锁纪律：行更新全部经 `ModelTable::with` 的同步闭包完成，闭包内不 await、
//! 不调网关；锁不跨 await。

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use strategy_core::order::OrderState;
use strategy_core::{FillEvent, IntentKind, SignalIntent, SignalStatus};
use strategy_store::{Model, ModelTable, Store};
use tracing::{debug, info, warn};

use crate::gateway::{FailureKind, OrderGateway, OrderPlace, ReadyFill, SubmitError};

/// 内核对外壳的提交请求：状态机已推进到 Validated/重试点，
/// 由外壳决定用同步还是异步网关执行，结果回交 [`SignalManager::apply_submit_outcome`]。
pub struct SubmitRequest {
    pub signal_id: String,
    pub intent: SignalIntent,
    /// 提交后 attempts 计数（首次=1，重试递增）
    pub attempts: u32,
    /// 落盘事件时间（首次=意图 K 线时间；重试/恢复=外壳调度钟 Utc::now()）
    pub event_time: DateTime<Utc>,
}

/// 重试策略（live 有效；回测不重试）
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// 最多提交次数（首次为 1；超限后 Failed 不再调度）
    pub max_attempts: u32,
    /// 初始退避秒数，每次翻倍
    pub base_delay_secs: i64,
    /// 退避上限秒数
    pub max_delay_secs: i64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_secs: 1,
            max_delay_secs: 60,
        }
    }
}

impl RetryPolicy {
    fn delay_secs(&self, attempt: u32) -> i64 {
        let exp = self
            .base_delay_secs
            .saturating_mul(2i64.saturating_pow(attempt.saturating_sub(1)));
        exp.min(self.max_delay_secs)
    }
}

/// 信号验证器（策略/风控可替换实现）
pub trait SignalValidator: Send + Sync {
    fn validate(&self, intent: &SignalIntent) -> Result<(), String>;
}

/// 基础参数校验（price>0、symbol 非空）
pub struct BasicValidator;

impl SignalValidator for BasicValidator {
    fn validate(&self, intent: &SignalIntent) -> Result<(), String> {
        if intent.symbol.trim().is_empty() {
            return Err("symbol 为空".into());
        }
        if intent.price <= 0.0 || !intent.price.is_finite() {
            return Err(format!("非法信号价格: {}", intent.price));
        }
        if intent.size < 0.0 || !intent.size.is_finite() {
            return Err(format!("非法信号数量: {}", intent.size));
        }
        Ok(())
    }
}

/// 信号当前态（统一 Store 的 signal 行）。
///
/// 多策略实例共享同一张表：`strategy_name` 是运行时实例名（进 CSV 路径与
/// position_id 的那个），作为 manager 全部集合查询的过滤键；它与
/// `intent.strategy_id`（策略类型名，进 signal_id 哈希）是两个口径。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedSignal {
    /// 运行时策略实例名（多实例共享 signal 表时的过滤键）
    pub strategy_name: String,
    pub intent: SignalIntent,
    pub version: u64,
    pub status: SignalStatus,
    pub order_id: Option<String>,
    /// 已提交次数
    pub attempts: u32,
    pub next_retry_at: Option<DateTime<Utc>>,
    /// 最近一次 Rejected/Failed 原因（审计；成功转移清空）
    pub last_error: Option<String>,
    /// 最近一次失败分类（持久化；决定重连/对账时终态 Failed 的 close 是否
    /// 复活：Retryable=状态未知可幂等重提，Rejected=交易所明确拒单不重提）
    #[serde(default)]
    pub last_failure_kind: Option<FailureKind>,
    /// 交易所订单号（回调帧 exchange_order_id；ack 时落账）
    #[serde(default)]
    pub exchange_order_id: Option<String>,
    /// 成交价（回调 filled_price；PARTIAL/FILLED 更新）
    #[serde(default)]
    pub filled_price: Option<f64>,
    /// 成交量（回调 filled_quantity 数值；PARTIAL/FILLED 更新）
    #[serde(default)]
    pub filled_quantity: Option<f64>,
    /// 成交量原始字面量（回调逐字保留；平仓帧 quantity 精确来源）
    #[serde(default)]
    pub filled_quantity_raw: Option<String>,
    /// 实际委托类型（回调 order_type；受理时落账）
    #[serde(default)]
    pub order_type: Option<String>,
    /// 实际委托价（市价单为 0；受理时落账）
    #[serde(default)]
    pub order_price: Option<f64>,
    /// 实际委托数量（金额单换算的真实下单量；受理时落账）
    #[serde(default)]
    pub order_quantity: Option<f64>,
}

impl Model for ManagedSignal {
    const NAME: &'static str = "signal";

    fn id(&self) -> &str {
        &self.intent.id
    }

    fn is_terminal(&self) -> bool {
        match self.status {
            // 真终态粘滞（SignalStatus::is_terminal），无出边，recover 不产生动作
            s if s.is_terminal() => true,
            // Failed：在途重试（next_retry_at 挂起）不裁；其余只有「recover
            // 不会重提」的才可裁。重提条件见 SignalManager::recover 的
            // Close+Failed 分支——两处必须互斥（B5 不变量测试锁死）
            SignalStatus::Failed => {
                self.next_retry_at.is_none()
                    && !(self.intent.kind == IntentKind::Close
                        && self.last_failure_kind != Some(FailureKind::Rejected)
                        && !self
                            .last_error
                            .as_deref()
                            .is_some_and(|e| e.contains("ReduceOnly")))
            }
            // 在途：Created / Validated / Submitted / Accepted
            _ => false,
        }
    }
}

/// 重启恢复动作（设计文档 §5.2.5）
#[derive(Debug, Clone)]
pub enum RecoveryAction {
    /// Created/Validated：重新提交
    Resubmit { signal_id: String },
    /// Submitted/Accepted：查询交易所真实状态
    Query { signal_id: String, order_id: String },
}

pub struct SignalManager {
    strategy_name: String,
    store: Arc<Store>,
    table: Arc<ModelTable<ManagedSignal>>,
    gateway: Option<Box<dyn OrderGateway>>,
    validator: Arc<dyn SignalValidator>,
    retry: RetryPolicy,
    /// order_id → signal_id（仅本策略实例；启动/对账时重建）
    order_index: HashMap<String, String>,
    events_since_flush: usize,
    flush_every: usize,
    /// 阈值到达：外壳取出标志交 spawn_blocking(store.flush)
    flush_requested: bool,
    /// store.recover 只在首代执行一次：重连对账不得用旧快照覆盖内存新态
    loaded: bool,
}

impl SignalManager {
    pub fn new(
        strategy_name: impl Into<String>,
        store: Arc<Store>,
        gateway: Option<Box<dyn OrderGateway>>,
    ) -> Self {
        let table = store.table::<ManagedSignal>();
        Self {
            strategy_name: strategy_name.into(),
            store,
            table,
            gateway,
            validator: Arc::new(BasicValidator),
            retry: RetryPolicy::default(),
            order_index: HashMap::new(),
            events_since_flush: 0,
            flush_every: 1000,
            flush_requested: false,
            loaded: false,
        }
    }

    pub fn with_validator(mut self, validator: Arc<dyn SignalValidator>) -> Self {
        self.validator = validator;
        self
    }

    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// 周期 flush 阈值（事件数；回测传 usize::MAX 关闭周期落盘）。
    pub fn with_flush_every(mut self, n: usize) -> Self {
        self.flush_every = n;
        self
    }

    /// 进程共享 Store（外壳 spawn_blocking(flush) 用）。
    pub fn store(&self) -> Arc<Store> {
        Arc::clone(&self.store)
    }

    /// B10：取出阈值 flush 标志（取出即清除）；外壳在阻塞池执行全 Store flush。
    pub fn take_flush_requested(&mut self) -> bool {
        std::mem::take(&mut self.flush_requested)
    }

    /// 主动请求一次全 Store flush（部分平仓等需要即时落盘的场景）。
    pub fn request_flush(&mut self) {
        self.flush_requested = true;
    }

    pub fn len(&self) -> usize {
        self.own_rows().len()
    }

    pub fn is_empty(&self) -> bool {
        self.own_rows().is_empty()
    }

    pub fn status_of(&self, signal_id: &str) -> Option<SignalStatus> {
        self.table
            .get(signal_id)
            .filter(|m| m.strategy_name == self.strategy_name)
            .map(|m| m.status)
    }

    /// 已 Filled 的 Open 订单（signal_id, order_id）：交叉对账补仓位用。
    pub fn filled_open_orders(&self) -> Vec<(String, String)> {
        self.own_rows()
            .iter()
            .filter(|m| m.status == SignalStatus::Filled && m.intent.kind == IntentKind::Open)
            .filter_map(|m| Some((m.intent.id.clone(), m.order_id.clone()?)))
            .collect()
    }

    /// 在途订单（Submitted/Accepted 且持有 cid）：看门狗轮询用。
    /// 返回 (signal_id, client_order_id, intent.created_event_time)。
    pub fn in_flight_orders(&self) -> Vec<(String, String, DateTime<Utc>)> {
        self.own_rows()
            .iter()
            .filter(|m| matches!(m.status, SignalStatus::Submitted | SignalStatus::Accepted))
            .filter_map(|m| {
                Some((
                    m.intent.id.clone(),
                    m.order_id.clone()?,
                    m.intent.created_event_time,
                ))
            })
            .collect()
    }

    /// 按 order_id 反查意图（交叉对账合成 FillEvent 用）。
    pub fn intent_for_order(&self, order_id: &str) -> Option<SignalIntent> {
        let sid = self.order_index.get(order_id)?;
        self.table.get(sid).map(|m| m.intent)
    }

    /// 信号当前绑定的 order_id（B8：ack 后向路由表注册）。
    pub fn order_id_of(&self, signal_id: &str) -> Option<String> {
        self.table.get(signal_id).and_then(|m| m.order_id)
    }

    /// 信号进入管道（同步端口，回测直调）：Created 建行 → 验证 → 同步提交。
    /// 同一 signal_id 重复进入为幂等 no-op。
    ///
    /// 返回同步就绪的成交（回测 submit 即成交）；live 外壳走
    /// [`Self::receive_intent_prepare`] + async 网关，成交经
    /// [`Self::handle_order_update`] 回调。
    pub fn receive_intent(&mut self, intent: SignalIntent) -> std::io::Result<Vec<FillEvent>> {
        let Some(req) = self.receive_intent_prepare(intent)? else {
            return Ok(Vec::new());
        };
        let now = req.event_time;
        let outcome = match self.gateway.as_mut() {
            Some(g) => g.submit(&req.intent),
            None => return Err(std::io::Error::other("SignalManager 未配置 OrderGateway")),
        };
        self.apply_submit_outcome(req, outcome)?;

        // 同步就绪成交（回测）：驱动 Accepted→Filled 并产出 FillEvent
        let ready = self
            .gateway
            .as_mut()
            .map(|g| g.drain_ready_fills())
            .unwrap_or_default();
        let mut fills = Vec::new();
        for rf in ready {
            if let Some(fill) = self.apply_ready_fill(rf, now)? {
                fills.push(fill);
            }
        }
        Ok(fills)
    }

    /// 信号进入管道的纯内核半步（不调网关）：去重 → Created 建行 → 验证，
    /// 返回待提交请求；重复 signal_id 或验证拒绝返回 `None`（拒绝已留痕）。
    pub fn receive_intent_prepare(
        &mut self,
        intent: SignalIntent,
    ) -> std::io::Result<Option<SubmitRequest>> {
        if self.table.get(&intent.id).is_some() {
            // 同 signal_id 全局去重（id 含策略类型/symbol/周期/时间戳，不含实例名）。
            // 正常路径不可能重复；命中通常意味着多实例参数不同却产出同 id——
            // 显式告警，避免无日志静默丢单。
            warn!(
                signal_id = %intent.id,
                strategy = %self.strategy_name,
                "重复 signal_id 被去重丢弃（若为多实例同 id，请检查实例命名）"
            );
            return Ok(None);
        }
        let now = intent.created_event_time;
        let id = intent.id.clone();

        // 1) Created 建行（当前态行常驻表，含完整意图，供恢复重提）
        let row = ManagedSignal {
            strategy_name: self.strategy_name.clone(),
            intent: intent.clone(),
            version: 1,
            status: SignalStatus::Created,
            order_id: None,
            attempts: 0,
            next_retry_at: None,
            last_error: None,
            last_failure_kind: None,
            exchange_order_id: None,
            filled_price: None,
            filled_quantity: None,
            filled_quantity_raw: None,
            order_type: None,
            order_price: None,
            order_quantity: None,
        };
        self.table
            .insert(row)
            .map_err(|e| std::io::Error::other(format!("信号行写入失败: {e}")))?;
        self.note_event();
        debug!(signal_id = %id, kind = ?intent.kind, direction = ?intent.direction, "信号：Created 行已建立（当前态行常驻表，供恢复重提）");

        // 2) 在途约束（B5）：另一信号的 Open/Close 未终结时拒绝重复意图
        //    （仅查本策略实例；共享表中其他实例不可见）。
        if let Some(reason) = self.pending_conflict(&intent) {
            debug!(signal_id = %id, %reason, "信号：被 B5 在途约束拦截");
            self.transition(&id, SignalStatus::Rejected, None, Some(reason), None)?;
            return Ok(None);
        }

        // 3) 验证
        if let Err(reason) = self.validator.validate(&intent) {
            debug!(signal_id = %id, %reason, "信号：校验未通过");
            self.transition(&id, SignalStatus::Rejected, None, Some(reason), None)?;
            return Ok(None);
        }
        self.transition(&id, SignalStatus::Validated, None, None, None)?;
        debug!(signal_id = %id, "信号：Created → Validated，交外壳提交");

        // 4) 交外壳提交
        Ok(Some(self.submit_request(&id, now)))
    }

    /// 订单状态回调 → 信号状态；完全成交时返回 FillEvent 交仓位管理。
    #[allow(clippy::too_many_arguments)]
    pub fn handle_order_update(
        &mut self,
        order_id: &str,
        exchange_order_id: Option<&str>,
        state: OrderState,
        filled_size: f64,
        avg_price: f64,
        filled_quantity_raw: Option<String>,
        frame_signal_id: Option<&str>,
        // Rejected 帧携带的交易所原文（审计；外壳据此识别 ReduceOnly 拒单）
        reject_message: Option<&str>,
        now: DateTime<Utc>,
    ) -> std::io::Result<Option<FillEvent>> {
        let signal_id = if let Some(sid) = self.order_index.get(order_id).cloned() {
            sid
        } else {
            // 回报早于 ack/路由注册（服务端先推 order_event）：按帧内 signal_id
            // 找回本实例信号，不再直接丢弃
            let Some(fsid) = frame_signal_id else {
                warn!(order_id, "未知订单的回调，忽略");
                return Ok(None);
            };
            let Some(m) = self.table.get(fsid) else {
                warn!(order_id, signal_id = fsid, "未知订单的回调，忽略");
                return Ok(None);
            };
            if m.strategy_name != self.strategy_name {
                // 其他实例的信号（广播帧）：与本 manager 无关
                return Ok(None);
            }
            if m.status != SignalStatus::Validated {
                // 非 Validated（如 Created）无法补齐 Submitted：保守忽略等正式 ack
                warn!(order_id, signal_id = fsid, status = ?m.status, "早到回报状态无法补齐，忽略");
                return Ok(None);
            }
            self.transition(fsid, SignalStatus::Submitted, Some(order_id), None, None)?;
            self.order_index
                .insert(order_id.to_string(), fsid.to_string());
            fsid.to_string()
        };
        // 终态粘滞：Filled/Cancelled/Rejected 无任何状态出边。迟到的非终态帧
        // （旧 NEW/PARTIAL、重复回报）不推进状态，也不回写价量审计值。
        if self
            .table
            .get(&signal_id)
            .expect("索引一致")
            .status
            .is_terminal()
        {
            debug!(%signal_id, order_id, "信号已终态，忽略迟到回报（终态粘滞）");
            return Ok(None);
        }
        // 帧带成交量时，价/量（含原始字面量）一并落账；服务端为权威累计值
        let has_fill = filled_size > 0.0;
        let price_in = has_fill.then_some(avg_price);
        let qty_in = has_fill.then_some(filled_size);
        let raw_in = filled_quantity_raw.as_deref();
        match state {
            OrderState::Ack => {
                if self.table.get(&signal_id).expect("索引一致").status == SignalStatus::Submitted
                {
                    self.transition(
                        &signal_id,
                        SignalStatus::Accepted,
                        Some(order_id),
                        None,
                        None,
                    )?;
                }
                // ack 帧的 exchange_order_id 落账（成交量此时通常为 0）
                self.apply_exchange_update(&signal_id, exchange_order_id, None, None, None)?;
                Ok(None)
            }
            OrderState::PartiallyFilled => {
                // 部分成交不改信号终态：Submitted 时补 Accepted；已 Accepted 则
                // 当前态无变化；价/量逐帧落账
                if self.table.get(&signal_id).expect("索引一致").status == SignalStatus::Submitted
                {
                    self.transition(
                        &signal_id,
                        SignalStatus::Accepted,
                        Some(order_id),
                        None,
                        None,
                    )?;
                }
                self.apply_exchange_update(
                    &signal_id,
                    exchange_order_id,
                    price_in,
                    qty_in,
                    raw_in,
                )?;
                Ok(None)
            }
            OrderState::Filled => {
                // 回调可能跳过 Ack：Submitted →（补 Accepted）→ Filled
                if self.table.get(&signal_id).expect("索引一致").status == SignalStatus::Submitted
                {
                    self.transition(
                        &signal_id,
                        SignalStatus::Accepted,
                        Some(order_id),
                        None,
                        None,
                    )?;
                }
                self.transition(&signal_id, SignalStatus::Filled, Some(order_id), None, None)?;
                self.apply_exchange_update(
                    &signal_id,
                    exchange_order_id,
                    price_in,
                    qty_in,
                    raw_in,
                )?;
                Ok(Some(self.fill_event(
                    &signal_id,
                    order_id,
                    filled_size,
                    avg_price,
                    filled_quantity_raw,
                    now,
                )))
            }
            OrderState::Canceled => {
                self.transition(
                    &signal_id,
                    SignalStatus::Cancelled,
                    Some(order_id),
                    None,
                    None,
                )?;
                // B7：撤单前有累计成交 → 残仓 FillEvent（信号仍终结于 Cancelled）
                if filled_size > 0.0 {
                    self.apply_exchange_update(
                        &signal_id,
                        exchange_order_id,
                        price_in,
                        qty_in,
                        raw_in,
                    )?;
                    Ok(Some(self.fill_event(
                        &signal_id,
                        order_id,
                        filled_size,
                        avg_price,
                        filled_quantity_raw,
                        now,
                    )))
                } else {
                    Ok(None)
                }
            }
            OrderState::Rejected => {
                let msg = reject_message.unwrap_or("交易所拒单").to_string();
                self.transition(
                    &signal_id,
                    SignalStatus::Failed,
                    Some(order_id),
                    Some(msg),
                    Some(FailureKind::Rejected),
                )?;
                Ok(None)
            }
            OrderState::Created | OrderState::Sent => Ok(None),
        }
    }

    /// 到期重试（同步端口，回测/测试直调；时间由调用方给，内部不取时）。
    /// live 外壳改用 [`Self::due_retries`] + async 网关 + apply。
    pub fn tick(&mut self, now: DateTime<Utc>) -> std::io::Result<Vec<String>> {
        let due = self.due_retries(now);
        let ids: Vec<String> = due.iter().map(|r| r.signal_id.clone()).collect();
        for req in due {
            let outcome = match self.gateway.as_mut() {
                Some(g) => g.submit(&req.intent),
                None => return Err(std::io::Error::other("SignalManager 未配置 OrderGateway")),
            };
            self.apply_submit_outcome(req, outcome)?;
        }
        Ok(ids)
    }

    /// 当前到期可重试信号的提交请求（纯查询，不改状态；外壳逐个异步执行）。
    pub fn due_retries(&self, now: DateTime<Utc>) -> Vec<SubmitRequest> {
        self.own_rows()
            .iter()
            .filter(|m| m.status == SignalStatus::Failed)
            .filter(|m| m.next_retry_at.is_some_and(|t| t <= now))
            .map(|m| SubmitRequest {
                signal_id: m.intent.id.clone(),
                intent: m.intent.clone(),
                attempts: m.attempts + 1,
                event_time: now,
            })
            .collect()
    }

    /// 启动恢复：首代先 `store.recover` 升序跨日去重载入信号当前态，再重建本
    /// 实例的 order_index，产出对账动作（不自动执行）。
    ///
    /// 同一 manager 实例只回读磁盘一次：重连再调时内存即真相（未 flush 的
    /// 新态不得被旧快照覆盖），仅重建索引并按当前态产出动作。
    /// 从盘上直接载入 Filled Open 信号行（绕过 is_terminal 过滤）。
    /// 用途：启动对账的「信号 Filled Open、本地仓位缺失」交叉补成交
    /// 依赖这些行；裁剪模式的 load_merged 会把终态行拦在表外。
    /// 只补空缺 id，不覆盖内存已有行。
    fn load_filled_open_from_disk(&mut self) -> std::io::Result<()> {
        let mut merged = std::collections::HashMap::new();
        let rd = match std::fs::read_dir(self.store.base()) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let mut days: Vec<std::path::PathBuf> = rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|d| {
                d.is_dir()
                    && d.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.len() == 8 && n.bytes().all(|b| b.is_ascii_digit()))
            })
            .collect();
        days.sort();
        // 升序扫**全部**日文件：Filled Open / 挂起重试行可能在任意历史日落盘
        // （跨日 rollover 裁剪后最新日文件不再含它们，旧日文件是唯一备份——
        // 旧日文件永不删除，这里就是它们的恢复通道）。同 id 取 updated_at 最新。
        for d in &days {
            let file = d.join("signal.csv");
            if !file.exists() {
                continue;
            }
            let recs = match strategy_store::persist::read_day_csv(&file) {
                Ok(r) => r,
                // 单日损坏不阻断恢复（与 Store 容错纪律一致）
                Err(e) => {
                    tracing::warn!(file = %file.display(), error = %e, "信号日文件读取失败，跳过该日");
                    continue;
                }
            };
            for rec in recs {
                merged
                    .entry(rec.id.clone())
                    .and_modify(|cur: &mut strategy_store::persist::CsvRecord| {
                        if rec.updated_at_ms > cur.updated_at_ms {
                            *cur = rec.clone();
                        }
                    })
                    .or_insert(rec);
            }
        }
        // 主 recover 盘载按 is_terminal 过滤；这里把「重启后仍有用途」的行
        // 按无过滤路径装回：①Filled Open（交叉对账补成交来源）②Failed + 挂起重试
        // （重试队列延续，next_retry_at 未清 = 预算未耗尽）。
        let resurrect: std::collections::HashMap<String, strategy_store::persist::CsvRecord> =
            merged
                .into_iter()
                .filter(|(_, rec)| rec.deleted == 0)
                .filter(|(_, rec)| {
                    serde_json::from_str::<ManagedSignal>(&rec.payload_json).is_ok_and(|row| {
                        (row.status == SignalStatus::Filled && row.intent.kind == IntentKind::Open)
                            || (row.status == SignalStatus::Failed && row.next_retry_at.is_some())
                    })
                })
                .collect();
        if resurrect.is_empty() {
            return Ok(());
        }
        // ModelTable 未暴露单行装回——用 upsert 语义（幂等，空缺 id 才生效）
        for (id, rec) in &resurrect {
            let Ok(row) = serde_json::from_str::<ManagedSignal>(&rec.payload_json) else {
                continue;
            };
            if self.table.get(id).is_some() {
                continue; // 活表现有 id 优先（与 load_merged 纪律一致）
            }
            self.table.upsert(row);
        }
        Ok(())
    }

    pub fn recover(&mut self) -> std::io::Result<Vec<RecoveryAction>> {
        if !self.loaded {
            // Store 的盘载"只填空缺 id"：冷启动等同纯盘载；同进程监督重建时
            // 共享活表里（含兄弟实例）的新态不会被旧快照回滚，无需逐行合并。
            self.store
                .recover::<ManagedSignal>()
                .map_err(std::io::Error::other)?;
            self.loaded = true;
            // 裁剪模式下 recover 按 is_terminal 过滤盘载行，但启动对账需要
            // Filled Open 信号行做「信号有、仓位缺」的交叉对账补成交
            // （filled_open_orders 来源）。重启后仓位若真的在（随 position 表
            // 恢复）该行只供审计；仓位缺失则由交叉对账 query 补回——因此
            // Filled Open 行必须在盘载时保留。
            self.load_filled_open_from_disk()?;
        }
        self.rebuild_index();

        let mut actions = Vec::new();
        for m in self.own_rows() {
            match m.status {
                SignalStatus::Created | SignalStatus::Validated if m.intent.price > 0.0 => {
                    actions.push(RecoveryAction::Resubmit {
                        signal_id: m.intent.id.clone(),
                    });
                }
                SignalStatus::Submitted | SignalStatus::Accepted => {
                    if let Some(order_id) = m.order_id {
                        actions.push(RecoveryAction::Query {
                            signal_id: m.intent.id.clone(),
                            order_id,
                        });
                    }
                }
                // 终态 Failed 的**平仓**信号分两种：
                // - Retryable（ack 超时/死连）：交易所状态未知、仓位可能仍敞着，
                //   重连对账幂等重提（Failed→Submitted 转移已支持）；
                // - Rejected（交易所明确拒单，典型 ReduceOnly Order is rejected
                //   ≡ 仓位已不存在）：**不再重提**，否则每次对账刷一轮报错；
                //   外壳会据此把本地幽灵仓位按 external_close 平掉。
                // 旧行兼容：last_failure_kind 缺失时，按 last_error 文本兜底识别。
                // 开仓信号不在此列——是否重开由策略信号面决定。
                SignalStatus::Failed
                    if m.intent.kind == IntentKind::Close
                        && m.last_failure_kind != Some(FailureKind::Rejected)
                        && !m
                            .last_error
                            .as_deref()
                            .is_some_and(|e| e.contains("ReduceOnly")) =>
                {
                    actions.push(RecoveryAction::Resubmit {
                        signal_id: m.intent.id.clone(),
                    });
                }
                _ => {}
            }
        }
        debug!(
            strategy = %self.strategy_name,
            actions = actions.len(),
            resubmit = actions.iter().filter(|a| matches!(a, RecoveryAction::Resubmit{..})).count(),
            queries = actions.iter().filter(|a| matches!(a, RecoveryAction::Query{..})).count(),
            "信号恢复：recover 完成（Created/Validated → 幂等重提；Submitted/Accepted → 先 query）"
        );
        Ok(actions)
    }

    /// 执行 recover 的对账动作（gateway 必须存在），返回确认成交的 FillEvent。
    pub fn execute_recovery(&mut self, now: DateTime<Utc>) -> std::io::Result<Vec<FillEvent>> {
        let actions = self.recover()?;
        if self.gateway.is_none() {
            return Err(std::io::Error::other("SignalManager 未配置 OrderGateway"));
        }
        let mut fills = Vec::new();
        for action in actions {
            match action {
                RecoveryAction::Resubmit { signal_id } => {
                    let req = self.submit_request(&signal_id, now);
                    let outcome = match self.gateway.as_mut() {
                        Some(g) => g.submit(&req.intent),
                        None => {
                            return Err(std::io::Error::other("SignalManager 未配置 OrderGateway"))
                        }
                    };
                    self.apply_submit_outcome(req, outcome)?;
                }
                RecoveryAction::Query { order_id, .. } => {
                    let result = self
                        .gateway
                        .as_mut()
                        .expect("已检查")
                        .query_order(&order_id);
                    match result {
                        Ok(snap) => {
                            if let Some(fill) = self.handle_order_update(
                                &order_id,
                                None,
                                snap.state,
                                snap.filled_size,
                                snap.avg_price,
                                None,
                                None,
                                None,
                                now,
                            )? {
                                fills.push(fill);
                            }
                        }
                        Err(e) => {
                            warn!(order_id, error = %e, "恢复查询失败，保留下次对账");
                        }
                    }
                }
            }
        }
        Ok(fills)
    }

    /// B5：返回与待入意图冲突的在途原因（无冲突 None）。
    /// 在途 = Created/Validated/Submitted/Accepted；Filled/Cancelled/Failed 释放。
    fn pending_conflict(&self, intent: &SignalIntent) -> Option<String> {
        fn in_flight(status: SignalStatus) -> bool {
            matches!(
                status,
                SignalStatus::Created
                    | SignalStatus::Validated
                    | SignalStatus::Submitted
                    | SignalStatus::Accepted
            )
        }
        match intent.kind {
            IntentKind::Open => {
                let blocked = self.own_rows().iter().any(|m| {
                    m.intent.id != intent.id
                        && m.intent.kind == IntentKind::Open
                        && in_flight(m.status)
                });
                blocked.then(|| "在途开仓未终结，丢弃重复 Open".to_string())
            }
            IntentKind::Close => {
                let pid = intent.position_id.as_deref()?;
                let blocked = self.own_rows().iter().any(|m| {
                    m.intent.id != intent.id
                        && m.intent.kind == IntentKind::Close
                        && m.intent.position_id.as_deref() == Some(pid)
                        && in_flight(m.status)
                });
                blocked.then(|| format!("在途平仓未终结，按 position_id={pid} 去重"))
            }
            // Reverse 在意图层已拆成 Close+Open，约束由各自类型承担
            IntentKind::Reverse => None,
        }
    }

    // ── 外壳协作（同步回测壳与 async live 壳共用）─────────────────────────

    /// 构造一次提交请求（attempts+1，不改内存计数；计数在 apply 时落）。
    /// 按 signal_id 直读意图（Rejected 帧未绑路由时按帧内 signal_id 找回）。
    pub fn intent_for_signal(&self, id: &str) -> Option<SignalIntent> {
        self.table.get(id).map(|m| m.intent)
    }

    pub fn submit_request(&self, id: &str, event_time: DateTime<Utc>) -> SubmitRequest {
        let m = self.table.get(id).expect("信号行存在");
        SubmitRequest {
            signal_id: id.to_string(),
            intent: m.intent.clone(),
            attempts: m.attempts + 1,
            event_time,
        }
    }

    /// 应用一次网关提交结果：Submitted（建 order_index）或 Failed（重试调度）。
    /// 纯状态机 + 行更新，不调网关。
    pub fn apply_submit_outcome(
        &mut self,
        req: SubmitRequest,
        outcome: Result<String, SubmitError>,
    ) -> std::io::Result<()> {
        let SubmitRequest {
            signal_id: id,
            attempts,
            event_time: now,
            ..
        } = req;
        self.table
            .with(&id, |row| row.attempts = attempts)
            .expect("信号行存在");
        match outcome {
            Ok(order_id) => {
                self.order_index.insert(order_id.clone(), id.clone());
                // 两种入口：首次 Validated→Submitted、重试 Failed→Submitted；
                // 回报早到已推进过则跳过
                let status = self.table.get(&id).expect("信号行存在").status;
                if matches!(status, SignalStatus::Validated | SignalStatus::Failed) {
                    self.transition(&id, SignalStatus::Submitted, Some(&order_id), None, None)?;
                }
                self.table.with(&id, |row| row.next_retry_at = None);
                info!(signal_id = %id, %order_id, "信号提交成功（等待异步成交回报）");
            }
            Err(e)
                if e.kind == crate::gateway::FailureKind::Retryable
                    && attempts < self.retry.max_attempts =>
            {
                let delay = self.retry.delay_secs(attempts);
                let message = e.message.clone();
                debug!(signal_id = %id, attempts, retry_in = delay, error = %message, "信号提交可重试：置 Failed 并安排退避重发（同 signal_id 幂等）");
                self.transition(
                    &id,
                    SignalStatus::Failed,
                    None,
                    Some(message),
                    Some(FailureKind::Retryable),
                )?;
                self.table.with(&id, |row| {
                    row.next_retry_at = Some(now + chrono::Duration::seconds(delay));
                });
            }
            Err(e) => {
                // 重试预算耗尽或永久拒绝：终态 Failed，no longer 调度
                warn!(signal_id = %id, kind = ?e.kind, error = %e.message, "信号提交终态失败：终态 Failed，不再重试");
                self.transition(
                    &id,
                    SignalStatus::Failed,
                    None,
                    Some(e.message.clone()),
                    Some(e.kind),
                )?;
                self.table.with(&id, |row| row.next_retry_at = None);
            }
        }
        Ok(())
    }

    /// 应用一笔同步就绪成交（回测/回测型适配器）：驱动状态机并补手续费。
    pub fn apply_ready_fill(
        &mut self,
        rf: ReadyFill,
        now: DateTime<Utc>,
    ) -> std::io::Result<Option<FillEvent>> {
        let Some(mut fill) = self.handle_order_update(
            &rf.order_id,
            None,
            OrderState::Filled,
            rf.filled_size,
            rf.avg_price,
            None,
            None,
            None,
            now,
        )?
        else {
            return Ok(None);
        };
        fill.fee = rf.fee;
        Ok(Some(fill))
    }

    // ── 内部 ──────────────────────────────────────────────────────────────

    /// 本表中属于本策略实例的全部行（含终态，克隆值）。
    fn own_rows(&self) -> Vec<ManagedSignal> {
        self.table
            .list()
            .into_iter()
            .filter(|r| r.strategy_name == self.strategy_name)
            .collect()
    }

    fn rebuild_index(&mut self) {
        self.order_index.clear();
        for m in self.own_rows() {
            if let Some(oid) = m.order_id {
                self.order_index.insert(oid, m.intent.id.clone());
            }
        }
    }

    fn note_event(&mut self) {
        self.events_since_flush += 1;
        if self.events_since_flush >= self.flush_every {
            // 同步内核不在事件路径上落盘；置标志由外壳延迟 flush 全 Store
            self.events_since_flush = 0;
            self.flush_requested = true;
        }
    }

    /// 状态机转移：先克隆当前行做合法性判定（非法不动行、不刷 updated_at），
    /// 再在行子锁的同步闭包内落 version/status/order_id/last_error。
    /// 闭包内不 await、不调网关。
    fn transition(
        &mut self,
        id: &str,
        to: SignalStatus,
        order_id: Option<&str>,
        error: Option<String>,
        failure_kind: Option<FailureKind>,
    ) -> std::io::Result<()> {
        let from = self
            .table
            .get(id)
            .ok_or_else(|| std::io::Error::other(format!("信号行不存在: {id}")))?
            .status;
        if !from.can_transit(to) {
            return Err(std::io::Error::other(format!(
                "非法信号状态转移 {from:?} → {to:?}（signal={id}）"
            )));
        }
        self.table
            .with(id, |row| {
                row.version += 1;
                row.status = to;
                row.last_error = error;
                row.last_failure_kind = failure_kind;
                if let Some(oid) = order_id {
                    row.order_id = Some(oid.to_string());
                }
            })
            .ok_or_else(|| std::io::Error::other(format!("信号行在转移时消失: {id}")))?;
        self.note_event();
        debug!(signal_id = %id, "{from:?} → {to:?}（order_id={order_id:?}）");
        Ok(())
    }

    /// 行锁内落交易所回调字段：exchange_order_id 与价/量。
    /// 服务端值为权威累计值；只更新帧实际携带的字段，不改变信号状态机。
    fn apply_exchange_update(
        &self,
        id: &str,
        exchange_order_id: Option<&str>,
        filled_price: Option<f64>,
        filled_quantity: Option<f64>,
        filled_quantity_raw: Option<&str>,
    ) -> std::io::Result<()> {
        self.table
            .with(id, |row| {
                if let Some(eoid) = exchange_order_id {
                    row.exchange_order_id = Some(eoid.to_string());
                }
                if let Some(price) = filled_price {
                    row.filled_price = Some(price);
                }
                if let Some(qty) = filled_quantity {
                    row.filled_quantity = Some(qty);
                }
                if let Some(raw) = filled_quantity_raw {
                    row.filled_quantity_raw = Some(raw.to_string());
                }
            })
            .ok_or_else(|| std::io::Error::other(format!("信号行在回调落账时消失: {id}")))?;
        Ok(())
    }

    /// 受理回调：落账实际委托要素（order_type/order_price/order_quantity）。
    /// cid 优先按 order_index 找回信号；未登记（早到回报）时用帧内 signal_id。
    /// 纯审计字段，终态行也允许补写，不参与状态机。
    pub fn record_place(
        &mut self,
        order_id: &str,
        frame_signal_id: Option<&str>,
        place: &OrderPlace,
    ) -> std::io::Result<()> {
        let signal_id = if let Some(sid) = self.order_index.get(order_id) {
            sid.clone()
        } else {
            let Some(fsid) = frame_signal_id else {
                warn!(order_id, "record_place：未知订单且帧无 signal_id，忽略");
                return Ok(());
            };
            let Some(m) = self.table.get(fsid) else {
                warn!(order_id, signal_id = fsid, "record_place：信号不存在，忽略");
                return Ok(());
            };
            if m.strategy_name != self.strategy_name {
                // 其他实例的信号（广播帧）
                return Ok(());
            }
            fsid.to_string()
        };
        self.table
            .with(&signal_id, |row| {
                row.order_type = Some(place.order_type.clone());
                row.order_price = Some(place.order_price);
                row.order_quantity = Some(place.order_quantity);
            })
            .ok_or_else(|| {
                std::io::Error::other(format!("委托要素落账时信号行消失: {signal_id}"))
            })?;
        Ok(())
    }

    /// 按当前行构造 FillEvent（B6：止损/原因/初始止损/意图元数据随意图透传）。
    fn fill_event(
        &self,
        signal_id: &str,
        order_id: &str,
        filled_size: f64,
        avg_price: f64,
        filled_size_raw: Option<String>,
        now: DateTime<Utc>,
    ) -> FillEvent {
        let m = self.table.get(signal_id).expect("索引一致");
        let is_stop = m.intent.stop_order_id.is_some();
        let reason = if is_stop { "stop_loss" } else { "live_close" };
        FillEvent::new(
            signal_id,
            order_id,
            m.intent.symbol.clone(),
            m.intent.direction,
            filled_size,
            avg_price,
            0.0,
            now,
        )
        .with_exit(is_stop, reason)
        .with_initial_stop(m.intent.stop_price)
        .with_fill_meta(m.intent.kind, m.intent.position_id.clone(), filled_size_raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::{FailureKind, OrderGateway, OrderSnapshot, SubmitError};
    use chrono::TimeZone;
    use std::fs;
    use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
    use std::sync::Mutex;
    use strategy_store::Clock;

    /// 可控时钟：测试不引入墙钟（时钟纪律）。
    struct FixedClock(AtomicI64);
    impl Clock for FixedClock {
        fn now_ms(&self) -> i64 {
            self.0.load(AtomicOrdering::SeqCst)
        }
    }

    fn fixed_store(dir: &Path) -> Arc<Store> {
        Arc::new(Store::new(
            dir,
            Arc::new(FixedClock(AtomicI64::new(
                Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0)
                    .unwrap()
                    .timestamp_millis(),
            ))),
        ))
    }

    use std::path::Path;

    fn time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
    }

    fn intent(id: &str, price: f64) -> SignalIntent {
        SignalIntent {
            id: id.into(),
            strategy_id: "sar_snt3".into(),
            symbol: "BTCUSDT".into(),
            kind: IntentKind::Open,
            direction: strategy_core::Side::Long,
            price,
            size: 0.0,
            size_raw: None,
            stop_price: Some(98.0),
            position_id: None,
            stop_order_id: None,
            created_event_time: time(),
            status: SignalStatus::Created,
        }
    }

    /// 测试辅助：只落 Created 行（不验证、不提交），模拟提交前崩溃
    fn raw_created(m: &SignalManager, intent: SignalIntent) {
        m.table
            .insert(ManagedSignal {
                strategy_name: m.strategy_name.clone(),
                intent,
                version: 1,
                status: SignalStatus::Created,
                order_id: None,
                attempts: 0,
                next_retry_at: None,
                last_error: None,
                last_failure_kind: None,
                exchange_order_id: None,
                filled_price: None,
                filled_quantity: None,
                filled_quantity_raw: None,
                order_type: None,
                order_price: None,
                order_quantity: None,
            })
            .unwrap();
    }

    /// 脚本化网关：按队列返回成功/失败，并记录全部提交
    #[derive(Default)]
    struct StubGateway {
        submits: Mutex<Vec<String>>,
        results: Mutex<Vec<Result<String, SubmitError>>>,
        queries: Mutex<Vec<OrderSnapshot>>,
    }

    impl StubGateway {
        fn push_ok(&self, oid: &str) {
            self.results.lock().unwrap().push(Ok(oid.into()));
        }
        fn push_err(&self, kind: FailureKind, msg: &str) {
            let e = match kind {
                FailureKind::Rejected => SubmitError::rejected(msg),
                FailureKind::Retryable => SubmitError::retryable(msg),
            };
            self.results.lock().unwrap().push(Err(e));
        }
    }

    impl StubGateway {
        fn do_submit(&self, intent: &SignalIntent) -> Result<String, SubmitError> {
            self.submits.lock().unwrap().push(intent.id.clone());
            self.results.lock().unwrap().remove(0)
        }
        fn do_query(&self, order_id: &str) -> Result<OrderSnapshot, SubmitError> {
            Ok(self
                .queries
                .lock()
                .unwrap()
                .iter()
                .find(|s| s.order_id == order_id)
                .cloned()
                .expect("测试需预置 query 结果"))
        }
    }

    impl OrderGateway for StubGateway {
        fn submit(&mut self, intent: &SignalIntent) -> Result<String, SubmitError> {
            self.do_submit(intent)
        }
        fn query_order(&mut self, order_id: &str) -> Result<OrderSnapshot, SubmitError> {
            self.do_query(order_id)
        }
    }

    /// 测试需要在管理器持有网关后继续断言，故用 Arc 共享脚本队列
    impl OrderGateway for Arc<StubGateway> {
        fn submit(&mut self, intent: &SignalIntent) -> Result<String, SubmitError> {
            self.do_submit(intent)
        }
        fn query_order(&mut self, order_id: &str) -> Result<OrderSnapshot, SubmitError> {
            self.do_query(order_id)
        }
    }

    fn mgr(dir: &Path, gw: Arc<StubGateway>) -> SignalManager {
        SignalManager::new("st", fixed_store(dir), Some(Box::new(gw))).with_flush_every(100_000)
    }

    fn in_flight_keys(m: &SignalManager) -> Vec<(String, String, DateTime<Utc>)> {
        m.in_flight_orders()
    }

    #[test]
    fn happy_path_created_to_submitted_and_row_updated() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_1");
        let mut m = mgr(dir.path(), gw);
        m.receive_intent(intent("sig_1", 100.0)).unwrap();

        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Submitted));
        // 当前态行：Created(v1)→Validated(v2)→Submitted(v3)，版本即转移计数
        let row = m.table.get("sig_1").unwrap();
        assert_eq!(row.status, SignalStatus::Submitted);
        assert_eq!(row.version, 3);
        assert_eq!(row.order_id.as_deref(), Some("ord_1"));
        assert_eq!(row.attempts, 1);
        assert_eq!(row.strategy_name, "st");
        // 完整意图随当前态保留（恢复重提所需）
        assert_eq!(row.intent.price, 100.0);
        assert_eq!(row.intent.stop_price, Some(98.0));
        assert_eq!(row.last_error, None);
    }

    #[test]
    fn duplicate_intent_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_1");
        let mut m = mgr(dir.path(), gw.clone());
        m.receive_intent(intent("sig_1", 100.0)).unwrap();
        m.receive_intent(intent("sig_1", 100.0)).unwrap();
        assert_eq!(gw.submits.lock().unwrap().len(), 1);
        assert_eq!(m.table.list().len(), 1);
        assert_eq!(m.table.get("sig_1").unwrap().version, 3);
    }

    #[test]
    fn invalid_intent_persists_rejected_and_never_submits() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        let mut m = mgr(dir.path(), gw.clone());
        m.receive_intent(intent("sig_bad", 0.0)).unwrap();
        assert_eq!(m.status_of("sig_bad"), Some(SignalStatus::Rejected));
        assert!(gw.submits.lock().unwrap().is_empty());
        let row = m.table.get("sig_bad").unwrap();
        assert_eq!(row.status, SignalStatus::Rejected);
        assert_eq!(row.version, 2, "Created→Rejected 一次转移");
        assert!(
            row.last_error.as_deref().unwrap().contains("价格"),
            "拒绝原因须审计可见: {:?}",
            row.last_error
        );
        // 审计红线：Rejected 行不删除
        assert_eq!(m.table.list().len(), 1);
    }

    #[test]
    fn retryable_failure_schedules_retry_then_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_err(FailureKind::Retryable, "timeout");
        gw.push_ok("ord_2");
        let mut m = mgr(dir.path(), gw.clone());
        m.receive_intent(intent("sig_1", 100.0)).unwrap();
        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Failed));
        assert_eq!(gw.submits.lock().unwrap().len(), 1);

        // 未到退避时间（1s）→ 不重试
        assert!(m.tick(time()).unwrap().is_empty());
        // 到期 → 重试成功
        let due = time() + chrono::Duration::seconds(1);
        let retried = m.tick(due).unwrap();
        assert_eq!(retried, vec!["sig_1".to_string()]);
        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Submitted));
        // Created→Validated→Failed→(重试)Submitted：4 个版本
        let row = m.table.get("sig_1").unwrap();
        assert_eq!(row.version, 4);
        assert_eq!(row.status, SignalStatus::Submitted);
        assert_eq!(row.attempts, 2);
        assert_eq!(row.next_retry_at, None);
        assert_eq!(row.last_error, None);
    }

    #[test]
    fn permanent_rejection_is_failed_without_retry() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_err(FailureKind::Rejected, "balance");
        let mut m = mgr(dir.path(), gw);
        m.receive_intent(intent("sig_1", 100.0)).unwrap();
        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Failed));
        assert!(m
            .tick(time() + chrono::Duration::hours(1))
            .unwrap()
            .is_empty());
        let row = m.table.get("sig_1").unwrap();
        assert_eq!(row.attempts, 1);
        assert_eq!(row.next_retry_at, None);
        assert_eq!(row.last_error.as_deref(), Some("balance"));
    }

    #[test]
    fn retries_exhaust_after_max_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        for _ in 0..3 {
            gw.push_err(FailureKind::Retryable, "down");
        }
        let mut m = mgr(dir.path(), gw).with_retry_policy(RetryPolicy {
            max_attempts: 3,
            base_delay_secs: 1,
            max_delay_secs: 60,
        });
        m.receive_intent(intent("sig_1", 100.0)).unwrap();
        m.tick(time() + chrono::Duration::seconds(1)).unwrap();
        m.tick(time() + chrono::Duration::seconds(3)).unwrap();
        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Failed));
        assert!(m
            .tick(time() + chrono::Duration::hours(10))
            .unwrap()
            .is_empty());
        // 重试预算耗尽（原 retries_exhausted 事件类型语义）：3 次尝试、终态 Failed、
        // 不再调度、原因留痕
        let row = m.table.get("sig_1").unwrap();
        assert_eq!(row.attempts, 3);
        assert_eq!(row.status, SignalStatus::Failed);
        assert_eq!(row.next_retry_at, None);
        assert_eq!(row.last_error.as_deref(), Some("down"));
    }

    #[test]
    fn in_flight_orders_lists_submitted_and_accepted_with_cid() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_1");
        let mut m = mgr(dir.path(), gw);

        // Submitted：在途，列出
        m.receive_intent(intent("sig_1", 100.0)).unwrap();
        let got = in_flight_keys(&m);
        assert_eq!(
            got.iter()
                .map(|(s, c, _)| (s.as_str(), c.as_str()))
                .collect::<Vec<_>>(),
            vec![("sig_1", "ord_1")]
        );
        assert!(got[0].2 <= chrono::Utc::now());

        // Accepted：仍在途，继续列出
        m.handle_order_update(
            "ord_1",
            None,
            OrderState::Ack,
            0.0,
            0.0,
            None,
            None,
            None,
            time(),
        )
        .unwrap();
        let got = in_flight_keys(&m);
        assert_eq!(
            got.iter().map(|(_, c, _)| c.as_str()).collect::<Vec<_>>(),
            vec!["ord_1"]
        );

        // Filled：终态，排除
        m.handle_order_update(
            "ord_1",
            None,
            OrderState::Filled,
            1.0,
            100.0,
            None,
            None,
            None,
            time(),
        )
        .unwrap();
        assert!(in_flight_keys(&m).is_empty(), "终态单不得列为在途");
    }

    #[test]
    fn order_updates_drive_accepted_and_filled_event() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_1");
        let mut m = mgr(dir.path(), gw);
        m.receive_intent(intent("sig_1", 100.0)).unwrap();

        assert!(m
            .handle_order_update(
                "ord_1",
                None,
                OrderState::Ack,
                0.0,
                0.0,
                None,
                None,
                None,
                time()
            )
            .unwrap()
            .is_none());
        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Accepted));

        assert!(m
            .handle_order_update(
                "ord_1",
                None,
                OrderState::PartiallyFilled,
                0.5,
                100.0,
                None,
                None,
                None,
                time()
            )
            .unwrap()
            .is_none());
        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Accepted));

        let fill = m
            .handle_order_update(
                "ord_1",
                None,
                OrderState::Filled,
                1.0,
                100.5,
                None,
                None,
                None,
                time(),
            )
            .unwrap()
            .expect("完全成交产出 FillEvent");
        assert_eq!(fill.signal_id, "sig_1");
        assert_eq!(fill.order_id, "ord_1");
        assert!((fill.filled_price - 100.5).abs() < 1e-9);
        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Filled));
        // Submitted→Accepted→Filled：5 个版本
        assert_eq!(m.table.get("sig_1").unwrap().version, 5);
    }

    #[test]
    fn late_nonterminal_frame_after_filled_does_not_roll_back() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_1");
        let mut m = mgr(dir.path(), gw);
        m.receive_intent(intent("sig_1", 100.0)).unwrap();

        // 推进到 Filled：全量 1.0 @ 100.5
        m.handle_order_update(
            "ord_1",
            None,
            OrderState::Ack,
            0.0,
            0.0,
            None,
            None,
            None,
            time(),
        )
        .unwrap();
        m.handle_order_update(
            "ord_1",
            None,
            OrderState::Filled,
            1.0,
            100.5,
            None,
            None,
            None,
            time(),
        )
        .unwrap()
        .expect("完全成交产出 FillEvent");

        // 迟到 PartiallyFilled（旧值 0.5 @ 100.0）：终态粘滞，整体不更新
        assert!(m
            .handle_order_update(
                "ord_1",
                None,
                OrderState::PartiallyFilled,
                0.5,
                100.0,
                None,
                None,
                None,
                time()
            )
            .unwrap()
            .is_none());
        let row = m.table.get("sig_1").unwrap().clone();
        assert_eq!(row.status, SignalStatus::Filled);
        assert_eq!(row.filled_quantity, Some(1.0), "成交量审计值不得被旧帧回写");
        assert_eq!(row.filled_price, Some(100.5), "成交价审计值不得被旧帧回写");
    }

    #[test]
    fn record_place_persists_order_audit_fields() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_1");
        let mut m = mgr(dir.path(), gw);
        m.receive_intent(intent("sig_1", 100.0)).unwrap();

        // 受理回调（NEW）：信号 Accepted
        m.handle_order_update(
            "ord_1",
            None,
            OrderState::Ack,
            0.0,
            0.0,
            None,
            None,
            None,
            time(),
        )
        .unwrap();

        // 实际委托要素随帧带回（金额单本地意图 size=0，真实委托量只在回调里）
        let place = OrderPlace {
            order_type: "market".into(),
            order_price: 0.0,
            order_quantity: 0.002,
        };
        m.record_place("ord_1", None, &place).unwrap();
        let row = m.table.get("sig_1").unwrap().clone();
        assert_eq!(row.order_type.as_deref(), Some("market"));
        assert_eq!(row.order_price, Some(0.0));
        assert_eq!(row.order_quantity, Some(0.002));

        // cid 未登记时：按帧内 signal_id 同样可落账（早到回报场景）
        let place2 = OrderPlace {
            order_type: "market".into(),
            order_price: 0.0,
            order_quantity: 0.003,
        };
        m.record_place("ord_unregistered", Some("sig_1"), &place2)
            .unwrap();
        let row = m.table.get("sig_1").unwrap().clone();
        assert_eq!(row.order_quantity, Some(0.003));
    }

    #[test]
    fn early_fill_frame_resolved_by_signal_id_before_ack_registered() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_1");
        let mut m = mgr(dir.path(), gw);

        // 只走内核半步：信号停在 Validated，order_id 尚未注册（模拟服务端先推回报）
        m.receive_intent_prepare(intent("sig_1", 100.0))
            .unwrap()
            .unwrap();
        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Validated));

        // order_id 未知但帧带 signal_id：不再"未知订单忽略"，成功找回并产出 FillEvent
        let fill = m
            .handle_order_update(
                "ord_1",
                None,
                OrderState::Filled,
                1.0,
                100.5,
                None,
                Some("sig_1"),
                None,
                time(),
            )
            .unwrap()
            .expect("早到成交不再被丢弃");
        assert_eq!(fill.signal_id, "sig_1");
        assert_eq!(fill.order_id, "ord_1");
        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Filled));

        // 正式 ack 结果晚到：状态已推进，不回滚（apply_submit_outcome 容错）
        let row = m.table.get("sig_1").unwrap().clone();
        let req = SubmitRequest {
            signal_id: "sig_1".into(),
            intent: row.intent.clone(),
            attempts: 1,
            event_time: time(),
        };
        m.apply_submit_outcome(req, Ok("ord_1".into())).unwrap();
        assert_eq!(m.status_of("sig_1"), Some(SignalStatus::Filled));
    }

    #[test]
    fn exchange_fields_persisted_through_callback_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_1");
        let mut m = mgr(dir.path(), gw);
        m.receive_intent(intent("sig_1", 100.0)).unwrap();

        // Ack 帧携带交易所订单号 → exchange_order_id 落账；价/量此时未到
        m.handle_order_update(
            "ord_1",
            Some("ex-order-1"),
            OrderState::Ack,
            0.0,
            0.0,
            None,
            None,
            None,
            time(),
        )
        .unwrap();
        let row = m.table.get("sig_1").unwrap();
        assert_eq!(row.exchange_order_id.as_deref(), Some("ex-order-1"));
        assert!(row.filled_price.is_none());

        // FILLED 帧携带价/量与原始字面量 → 逐字落账
        m.handle_order_update(
            "ord_1",
            Some("ex-order-1"),
            OrderState::Filled,
            0.0001,
            100.5,
            Some("0.0001".into()),
            None,
            None,
            time(),
        )
        .unwrap();
        let row = m.table.get("sig_1").unwrap();
        assert_eq!(row.filled_price, Some(100.5));
        assert_eq!(row.filled_quantity, Some(0.0001));
        // 原始字面量逐字保留（平仓帧 quantity 的精确来源）
        assert_eq!(row.filled_quantity_raw.as_deref(), Some("0.0001"));
    }

    #[test]
    fn b5_duplicate_open_while_in_flight_is_rejected_then_released() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_b5_1");
        gw.push_ok("ord_b5_2");
        let mut m = mgr(dir.path(), gw.clone());

        m.receive_intent(intent("sig_b5_1", 100.0)).unwrap();
        assert_eq!(m.status_of("sig_b5_1"), Some(SignalStatus::Submitted));

        m.receive_intent(intent("sig_b5_2", 101.0)).unwrap();
        assert_eq!(
            m.status_of("sig_b5_2"),
            Some(SignalStatus::Rejected),
            "在途 Open 未终结，重复 Open 应拒绝留痕"
        );
        assert_eq!(gw.submits.lock().unwrap().len(), 1, "被拒信号不得提交网关");

        m.handle_order_update(
            "ord_b5_1",
            None,
            OrderState::Filled,
            1.0,
            100.0,
            None,
            None,
            None,
            time(),
        )
        .unwrap();
        m.receive_intent(intent("sig_b5_3", 102.0)).unwrap();
        assert_eq!(m.status_of("sig_b5_3"), Some(SignalStatus::Submitted));
        assert_eq!(gw.submits.lock().unwrap().len(), 2);
    }

    #[test]
    fn b5_close_dedupes_by_position_id_while_in_flight() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_c1");
        gw.push_ok("ord_c2");
        let mut m = mgr(dir.path(), gw);
        let mut first = intent("sig_close_1", 100.0);
        first.kind = IntentKind::Close;
        first.direction = strategy_core::Side::Short;
        first.position_id = Some("pos_b5".into());
        first.stop_price = None;
        m.receive_intent(first).unwrap();
        assert_eq!(m.status_of("sig_close_1"), Some(SignalStatus::Submitted));

        let mut dup = intent("sig_close_2", 100.0);
        dup.kind = IntentKind::Close;
        dup.direction = strategy_core::Side::Short;
        dup.position_id = Some("pos_b5".into());
        dup.stop_price = None;
        m.receive_intent(dup).unwrap();
        assert_eq!(m.status_of("sig_close_2"), Some(SignalStatus::Rejected));
    }

    #[test]
    fn b6_open_fill_carries_initial_stop_from_intent() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_b6o");
        let mut m = mgr(dir.path(), gw);
        m.receive_intent(intent("sig_b6o", 100.0)).unwrap();
        let fill = m
            .handle_order_update(
                "ord_b6o",
                None,
                OrderState::Filled,
                1.0,
                100.0,
                None,
                None,
                None,
                time(),
            )
            .unwrap()
            .expect("成交产出 FillEvent");
        assert_eq!(fill.initial_stop, Some(98.0), "开仓成交须携带意图初始止损");
        assert!(!fill.is_stop_loss);
    }

    #[test]
    fn b6_stop_loss_close_fill_is_flagged_with_reason() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_b6c");
        let mut close = intent("sig_b6c", 100.0);
        close.kind = IntentKind::Close;
        close.direction = strategy_core::Side::Short;
        close.stop_price = None;
        close.position_id = Some("pos_1".into());
        close.stop_order_id = Some("stop_1".into());
        let mut m = mgr(dir.path(), gw);
        m.receive_intent(close).unwrap();
        let fill = m
            .handle_order_update(
                "ord_b6c",
                None,
                OrderState::Filled,
                1.0,
                100.0,
                None,
                None,
                None,
                time(),
            )
            .unwrap()
            .expect("成交产出 FillEvent");
        assert!(fill.is_stop_loss, "止损平仓标记必须跨管道透传");
        assert_eq!(fill.exit_reason.as_deref(), Some("stop_loss"));
        assert_eq!(fill.initial_stop, None);
    }

    #[test]
    fn b7_canceled_with_partial_fill_emits_residual_fill() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_pf");
        let mut m = mgr(dir.path(), gw);
        m.receive_intent(intent("sig_pf", 100.0)).unwrap();
        let fill = m
            .handle_order_update(
                "ord_pf",
                None,
                OrderState::Canceled,
                0.5,
                100.2,
                None,
                None,
                None,
                time(),
            )
            .unwrap()
            .expect("部分成交后撤单应产出残仓 FillEvent");
        assert!((fill.filled_size - 0.5).abs() < 1e-9);
        assert!((fill.filled_price - 100.2).abs() < 1e-9);
        assert_eq!(m.status_of("sig_pf"), Some(SignalStatus::Cancelled));

        let gw2 = Arc::new(StubGateway::default());
        gw2.push_ok("ord_pf0");
        let mut m2 = mgr(tempfile::tempdir().unwrap().path(), gw2);
        m2.receive_intent(intent("sig_pf0", 100.0)).unwrap();
        assert!(m2
            .handle_order_update(
                "ord_pf0",
                None,
                OrderState::Canceled,
                0.0,
                0.0,
                None,
                None,
                None,
                time()
            )
            .unwrap()
            .is_none());
    }

    #[test]
    fn canceled_callback() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_c");
        let mut m = mgr(dir.path(), gw);
        m.receive_intent(intent("sig_c", 100.0)).unwrap();
        m.handle_order_update(
            "ord_c",
            None,
            OrderState::Canceled,
            0.0,
            0.0,
            None,
            None,
            None,
            time(),
        )
        .unwrap();
        assert_eq!(m.status_of("sig_c"), Some(SignalStatus::Cancelled));
    }

    #[test]
    fn unknown_order_update_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        let mut m = mgr(dir.path(), gw);
        assert!(m
            .handle_order_update(
                "ghost",
                None,
                OrderState::Filled,
                1.0,
                1.0,
                None,
                None,
                None,
                time()
            )
            .unwrap()
            .is_none());
    }

    #[test]
    fn recover_resubmits_unsubmitted_and_queries_in_flight() {
        let dir = tempfile::tempdir().unwrap();

        // 第一次"运行"：sig_a 已提交在途；sig_b 验证通过后"崩溃"（未提交）
        {
            let gw1 = Arc::new(StubGateway::default());
            gw1.push_ok("ord_a");
            let mut m = mgr(dir.path(), gw1);
            m.receive_intent(intent("sig_a", 100.0)).unwrap();
            raw_created(&m, intent("sig_b", 100.0));
            m.transition("sig_b", SignalStatus::Validated, None, None, None)
                .unwrap();
            // 崩溃前定时落盘
            m.store().flush().unwrap();
        }

        // "重启"：sig_a 查询确认成交；sig_b 重新提交
        let gw2 = Arc::new(StubGateway::default());
        gw2.push_ok("ord_b");
        gw2.queries.lock().unwrap().push(OrderSnapshot {
            order_id: "ord_a".into(),
            state: OrderState::Filled,
            filled_size: 2.0,
            avg_price: 101.0,
            exchange_order_id: None,
            filled_quantity_raw: None,
            place: None,
        });
        let mut m2 = mgr(dir.path(), gw2.clone());
        let fills = m2.execute_recovery(time()).unwrap();

        assert_eq!(m2.status_of("sig_a"), Some(SignalStatus::Filled));
        assert_eq!(m2.status_of("sig_b"), Some(SignalStatus::Submitted));
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, "ord_a");
        assert_eq!(gw2.submits.lock().unwrap().as_slice(), ["sig_b"]);
        // 重提的意图字段完整（价格/止损来自当前态行）
        let b = m2.table.get("sig_b").unwrap();
        assert_eq!(b.intent.price, 100.0);
        assert_eq!(b.intent.stop_price, Some(98.0));
    }

    /// 终态 Failed 的平仓信号（死连 ack 超时、重试预算耗尽）在重连对账时
    /// 必须被幂等重提；同为终态 Failed 的开仓信号不动。
    #[test]
    fn terminal_failed_close_retryable_revives_but_rejected_close_and_open_do_not() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        // 三个信号首次提交均落终态 Failed（next_retry_at=None）：
        // - close（Retryable，预算 1 耗尽）：ack 超时，交易所状态未知 → 可复活
        // - close（Rejected）：ReduceOnly 拒单，仓位已不存在 → 不复活
        // - open（Rejected）：永久拒绝 → 不复活
        gw.push_err(FailureKind::Retryable, "ack 等待超时");
        gw.push_err(FailureKind::Rejected, "ReduceOnly Order is rejected");
        gw.push_err(FailureKind::Rejected, "ack 永久拒绝");

        let mut close_retryable = intent("sig_close", 100.0);
        close_retryable.kind = IntentKind::Close;
        close_retryable.direction = strategy_core::Side::Short;
        close_retryable.stop_price = None;
        close_retryable.position_id = Some("pos_1".into());

        let mut close_rejected = intent("sig_close_rej", 100.0);
        close_rejected.kind = IntentKind::Close;
        close_rejected.direction = strategy_core::Side::Short;
        close_rejected.stop_price = None;
        close_rejected.position_id = Some("pos_2".into());

        let mut m = SignalManager::new("st", fixed_store(dir.path()), Some(Box::new(gw.clone())))
            .with_retry_policy(RetryPolicy {
                max_attempts: 1,
                ..RetryPolicy::default()
            })
            .with_flush_every(100_000);
        m.receive_intent(close_retryable).unwrap();
        m.receive_intent(close_rejected).unwrap();
        m.receive_intent(intent("sig_open", 100.0)).unwrap();
        assert_eq!(m.status_of("sig_close"), Some(SignalStatus::Failed));
        assert_eq!(m.status_of("sig_close_rej"), Some(SignalStatus::Failed));
        assert_eq!(m.status_of("sig_open"), Some(SignalStatus::Failed));
        assert_eq!(
            m.table.get("sig_close").unwrap().next_retry_at,
            None,
            "终态 Failed：无下次重试时间"
        );
        assert_eq!(
            m.table.get("sig_close").unwrap().last_failure_kind,
            Some(FailureKind::Retryable)
        );
        assert_eq!(
            m.table.get("sig_close_rej").unwrap().last_failure_kind,
            Some(FailureKind::Rejected)
        );

        // 模拟重连对账：仅为可重试失败的 close 预置一次成功 ack
        gw.push_ok("ord_close");
        let fills = m.execute_recovery(time()).unwrap();
        assert_eq!(
            m.status_of("sig_close"),
            Some(SignalStatus::Submitted),
            "Retryable 平仓信号终态复活：Failed → Submitted"
        );
        assert_eq!(
            m.status_of("sig_close_rej"),
            Some(SignalStatus::Failed),
            "Rejected 平仓信号（仓位已不存在）不得重提"
        );
        assert_eq!(
            m.status_of("sig_open"),
            Some(SignalStatus::Failed),
            "开仓信号不得自动重提"
        );
        assert!(fills.is_empty());
        assert_eq!(
            gw.submits.lock().unwrap().as_slice(),
            // 三次首次提交；对账时只有 sig_close 重提
            ["sig_close", "sig_close_rej", "sig_open", "sig_close"]
        );
    }

    #[test]
    fn missing_gateway_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = SignalManager::new("st", fixed_store(dir.path()), None);
        assert!(m.receive_intent(intent("sig_1", 100.0)).is_err());
    }

    /// 崩溃点组合恢复：当日快照已落 Accepted(v4)，快照文件尾部混入坏行，
    /// "重启"后 execute_recovery 经 query 把状态修正为 Filled（坏行跳过不毒表）。
    #[test]
    fn recovery_skips_corrupt_snapshot_line_and_queries_to_filled() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();

        let gw1 = Arc::new(StubGateway::default());
        gw1.push_ok("ord_x");
        {
            let mut m = mgr(dir.path(), gw1);
            m.receive_intent(intent("sig_x", 100.0)).unwrap();
            m.handle_order_update(
                "ord_x",
                None,
                OrderState::Ack,
                0.0,
                0.0,
                None,
                None,
                None,
                time(),
            )
            .unwrap();
            m.store().flush().unwrap();
        }
        // 模拟崩溃半写：追加一行列数不对/时间戳损坏的坏行（无换行结尾）
        let day = "20260610";
        let path = dir.path().join(day).join("signal.csv");
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"sig_z,0,not-a-number,xx,{\"incomplete\"")
            .unwrap();

        let gw2 = Arc::new(StubGateway::default());
        gw2.queries.lock().unwrap().push(OrderSnapshot {
            order_id: "ord_x".into(),
            state: OrderState::Filled,
            filled_size: 3.0,
            avg_price: 99.0,
            exchange_order_id: None,
            filled_quantity_raw: None,
            place: None,
        });
        let mut m2 = mgr(dir.path(), gw2);
        let fills = m2.execute_recovery(time()).unwrap();
        assert_eq!(m2.status_of("sig_x"), Some(SignalStatus::Filled));
        assert_eq!(fills.len(), 1);
        assert!((fills[0].filled_size - 3.0).abs() < 1e-9);

        // 版本连续：Accepted(v4) → Filled(v5)
        let row = m2.table.get("sig_x").unwrap();
        assert_eq!(row.version, 5);
        assert_eq!(row.status, SignalStatus::Filled);
        // 坏行未入表
        assert_eq!(m2.table.get("sig_z"), None);
    }

    /// 多策略实例共享同一张表：集合查询严格按 strategy_name 隔离。
    #[test]
    fn shared_table_isolates_instances_by_strategy_name() {
        let dir = tempfile::tempdir().unwrap();
        let store = fixed_store(dir.path());

        let gw_a = Arc::new(StubGateway::default());
        gw_a.push_ok("ord_A1");
        gw_a.push_ok("ord_A2");
        let mut a = SignalManager::new("strat_A", Arc::clone(&store), Some(Box::new(gw_a.clone())))
            .with_flush_every(100_000);
        let gw_b = Arc::new(StubGateway::default());
        gw_b.push_ok("ord_B1");
        let mut b = SignalManager::new("strat_B", Arc::clone(&store), Some(Box::new(gw_b.clone())))
            .with_flush_every(100_000);

        // 两实例各自在途 Open
        a.receive_intent(intent("sig_A1", 100.0)).unwrap();
        b.receive_intent(intent("sig_B1", 100.0)).unwrap();

        // B 的在途不得计入 A 的 B5 判定——A 的第二笔 Open 是被 A 自己的
        // sig_A1 拒绝的，网关只提交过一次
        a.receive_intent(intent("sig_A2", 101.0)).unwrap();
        assert_eq!(a.status_of("sig_A2"), Some(SignalStatus::Rejected));
        assert_eq!(gw_a.submits.lock().unwrap().len(), 1);
        // B 仍只看到自己的 1 行
        assert_eq!(b.len(), 1);
        assert_eq!(a.len(), 2);

        // A 的 sig_A1 成交：filled_open_orders 只在 A 可见
        a.handle_order_update(
            "ord_A1",
            None,
            OrderState::Filled,
            1.0,
            100.0,
            None,
            None,
            None,
            time(),
        )
        .unwrap();
        assert_eq!(
            a.filled_open_orders(),
            vec![("sig_A1".to_string(), "ord_A1".to_string())]
        );
        assert!(b.filled_open_orders().is_empty());
        // order_index 按实例隔离：B 反查不到 A 的订单
        assert!(b.intent_for_order("ord_A1").is_none());
        assert!(a.intent_for_order("ord_A1").is_some());

        // 同一 signal_id 在共享表中全局唯一：B 再入同 id 视为重复，不提交
        b.receive_intent(intent("sig_A1", 100.0)).unwrap();
        assert_eq!(gw_b.submits.lock().unwrap().len(), 1, "B 只提交过 sig_B1");
        assert_eq!(b.len(), 1, "重复 id 不建行");

        // 落盘后用新 Store 恢复：两实例的对账动作各自隔离
        store.flush().unwrap();
        let store2 = fixed_store(dir.path());
        let mut a2 =
            SignalManager::new("strat_A", Arc::clone(&store2), None).with_flush_every(100_000);
        let mut b2 =
            SignalManager::new("strat_B", Arc::clone(&store2), None).with_flush_every(100_000);
        let acts_a = a2.recover().unwrap();
        let acts_b = b2.recover().unwrap();
        // A：sig_A1 Filled 无动作；sig_A2 Rejected 无动作
        assert!(acts_a.is_empty());
        // B：sig_B1 Submitted → Query
        assert_eq!(acts_b.len(), 1);
        match &acts_b[0] {
            RecoveryAction::Query {
                signal_id,
                order_id,
            } => {
                assert_eq!(signal_id, "sig_B1");
                assert_eq!(order_id, "ord_B1");
            }
            other => panic!("期望 Query，实际 {other:?}"),
        }
    }

    /// 落盘→恢复：当前态（含 Failed 的 attempts/重试时刻）跨进程还原；
    /// 终态审计行保留；重试队列可重建。
    #[test]
    fn flush_recover_restores_current_state_and_retry_queue() {
        let dir = tempfile::tempdir().unwrap();
        {
            let gw = Arc::new(StubGateway::default());
            gw.push_err(FailureKind::Retryable, "timeout");
            let mut m = mgr(dir.path(), gw);
            m.receive_intent(intent("sig_retry", 100.0)).unwrap(); // Failed, attempts=1
            m.receive_intent(intent("sig_bad", 0.0)).unwrap(); // Rejected
            m.store().flush().unwrap();
        }

        let mut m2 = mgr(dir.path(), Arc::new(StubGateway::default()));
        let _ = m2.recover().unwrap();

        // Failed 行：尝试次数与重试时刻跨重启保留（重试预算不会因重启刷新）
        let due = m2.due_retries(time() + chrono::Duration::seconds(1));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].signal_id, "sig_retry");
        assert_eq!(due[0].attempts, 2, "第二次提交 attempts=2，预算延续");
        // 未到点不重试
        assert!(m2.due_retries(time()).is_empty());

        // 终态审计行不再跨日盘载（裁剪语义：Rejected/Filled 无启动对账用途，
        // 盘载时按 is_terminal 过滤）；重试中的 Failed 行是活动行，必须还原。
        assert_eq!(m2.status_of("sig_bad"), None);
        let names: Vec<String> = m2
            .table
            .list()
            .iter()
            .map(|r| r.intent.id.clone())
            .collect();
        assert!(!names.contains(&"sig_bad".to_string()));
        assert!(names.contains(&"sig_retry".to_string()));
    }

    /// B11 同进程监督重建：gen1 已 flush 旧态后又在内存推进（未再 flush），
    /// gen2（新 manager、同一进程级 Store）recover 不得用日文件旧快照把
    /// 更新的内存行回滚。
    #[test]
    fn recovery_within_process_keeps_unflushed_newer_rows() {
        let dir = tempfile::tempdir().unwrap();
        let store = fixed_store(dir.path());

        let gw1 = Arc::new(StubGateway::default());
        gw1.push_ok("ord_u");
        {
            let mut g1 = SignalManager::new("st", Arc::clone(&store), Some(Box::new(gw1)))
                .with_flush_every(100_000);
            g1.receive_intent(intent("sig_u", 100.0)).unwrap(); // Submitted v3
            g1.store().flush().unwrap(); // 日文件停在 Submitted
            g1.handle_order_update(
                "ord_u",
                None,
                OrderState::Filled,
                1.0,
                100.0,
                None,
                None,
                None,
                time(),
            )
            .unwrap(); // 内存 Filled v5，未 flush
            assert_eq!(g1.status_of("sig_u"), Some(SignalStatus::Filled));
        }

        // panic 后整代重建：新 manager 复用存活的进程级 Store
        let mut g2 = SignalManager::new(
            "st",
            Arc::clone(&store),
            Some(Box::new(Arc::new(StubGateway::default()))),
        )
        .with_flush_every(100_000);
        let actions = g2.recover().unwrap();
        assert_eq!(g2.status_of("sig_u"), Some(SignalStatus::Filled));
        assert!(
            actions.is_empty(),
            "Filled 终态不得因旧快照回滚而产生对账动作：{actions:?}"
        );
    }

    /// 阈值 flush 标志：到达事件数置位，取出即清。
    #[test]
    fn flush_threshold_sets_requested_flag() {
        let dir = tempfile::tempdir().unwrap();
        let gw = Arc::new(StubGateway::default());
        gw.push_ok("ord_t");
        let mut m = SignalManager::new("st", fixed_store(dir.path()), Some(Box::new(gw)))
            .with_flush_every(3);
        // Created+Validated+Submitted = 3 个事件 → 阈值到达
        m.receive_intent(intent("sig_t", 100.0)).unwrap();
        assert!(m.take_flush_requested());
        assert!(!m.take_flush_requested());
    }

    /// 表驱动：is_terminal 与 recover 重提条件互斥（spec B5）。
    /// recover 的 Close+Failed 重提分支条件在此逐字镜像，任一改动跑本测试即红。
    fn mk_failed_close(kind: FailureKind, last_error: Option<&str>) -> ManagedSignal {
        ManagedSignal {
            strategy_name: "st".into(),
            intent: SignalIntent {
                id: "sig_f".into(),
                strategy_id: "st".into(),
                symbol: "BTCUSDT".into(),
                kind: IntentKind::Close,
                direction: strategy_core::Side::Long,
                price: 1.0,
                size: 1.0,
                size_raw: None,
                stop_price: None,
                position_id: Some("pos_1".into()),
                stop_order_id: None,
                created_event_time: time(),
                status: SignalStatus::Created,
            },
            version: 1,
            status: SignalStatus::Failed,
            order_id: None,
            attempts: 1,
            next_retry_at: None,
            last_error: last_error.map(String::from),
            last_failure_kind: Some(kind),
            exchange_order_id: None,
            filled_price: None,
            filled_quantity: None,
            filled_quantity_raw: None,
            order_type: None,
            order_price: None,
            order_quantity: None,
        }
    }

    #[test]
    fn resubmitable_failed_close_is_never_terminal() {
        // recover 会 Resubmit 的唯一 Failed 形态：Close + 非 Rejected + 非 ReduceOnly
        for err in [None, Some("ack timeout"), Some("connection lost")] {
            let m = mk_failed_close(FailureKind::Retryable, err);
            assert!(
                !m.is_terminal(),
                "可重提的 Failed Close 不可裁: err={err:?}"
            );
        }
    }

    #[test]
    fn terminal_signal_status_is_terminal() {
        // 真终态粘滞
        for st in [
            SignalStatus::Filled,
            SignalStatus::Cancelled,
            SignalStatus::Rejected,
        ] {
            let mut m = mk_failed_close(FailureKind::Rejected, Some("ReduceOnly"));
            m.status = st;
            assert!(m.is_terminal());
        }
        // Failed 但 recover 不重提的形态：可裁
        assert!(mk_failed_close(FailureKind::Rejected, Some("ReduceOnly")).is_terminal());
        // Failed 开仓信号（recover 不重提、无挂起重试）：可裁
        let mut open = mk_failed_close(FailureKind::Retryable, None);
        open.intent.kind = IntentKind::Open;
        assert!(open.next_retry_at.is_none());
        assert!(open.is_terminal());
        // Failed + 挂起重试（next_retry_at 有值）= 在途：不裁（开平不论）
        let mut retrying = mk_failed_close(FailureKind::Retryable, Some("timeout"));
        retrying.next_retry_at = Some(time() + chrono::Duration::seconds(5));
        assert!(!retrying.is_terminal());
        // 在途：不裁
        let mut created = mk_failed_close(FailureKind::Retryable, None);
        created.status = SignalStatus::Created;
        assert!(!created.is_terminal());
    }
    /// 跨日裁剪后 Filled Open 行从旧日文件恢复（交叉对账不丢）：
    /// day1 落盘 Filled Open → rollover 裁剪 → 新 Store recover →
    /// filled_open_orders 仍可见（旧日文件是终态行唯一备份）。
    #[test]
    fn filled_open_survives_rollover_via_historical_day_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = fixed_store(dir.path());
        {
            // 直写 Filled Open 行（状态到达方式与本测试无关——测的是
            // 「盘上的 Filled Open 行跨裁剪式 recover 仍可见」）
            let m = SignalManager::new("st", Arc::clone(&store), None);
            let it = intent("sig_x", 100.0);
            m.table
                .insert(ManagedSignal {
                    strategy_name: "st".into(),
                    intent: it,
                    version: 5,
                    status: SignalStatus::Filled,
                    order_id: Some("ord_x".into()),
                    attempts: 1,
                    next_retry_at: None,
                    last_error: None,
                    last_failure_kind: None,
                    exchange_order_id: None,
                    filled_price: Some(100.0),
                    filled_quantity: Some(1.0),
                    filled_quantity_raw: Some("1".into()),
                    order_type: None,
                    order_price: None,
                    order_quantity: None,
                })
                .unwrap();
            store.flush().unwrap(); // day1：Filled Open 落盘
        }
        // rollover 裁剪（固定时钟 FixedClock 不动 → 手动构造跨日：直接建 day2 空文件目录不行，
        // 改用真实时钟 store 做 rollover）
        let store2 = Arc::new(Store::new_with_policy(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
            true,
        ));
        store2.flush().unwrap(); // 可能同日 → 不裁
        let mut m2 = SignalManager::new("st", Arc::clone(&store2), None);
        let _ = m2.recover().unwrap();
        // FixedClock 单日场景下 recover 主路径已按 is_terminal 过滤 Filled 行，
        // load_filled_open_from_disk 必须从 day1 文件装回
        assert_eq!(
            m2.filled_open_orders(),
            vec![("sig_x".to_string(), "ord_x".to_string())],
            "Filled Open 行跨 recover 必须可见（交叉对账来源）"
        );
    }
}
