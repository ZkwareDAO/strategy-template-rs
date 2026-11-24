//! 横截面单标的运行时切片与组合轮次编排（PR-2/PR-3，Task 9/10）。
//!
//! [`StrategySlot`] 是单标的运行时切片（复用引擎的 `PositionSync` /
//! `RiskGuard` / `AggregationState`）；[`CrossSectionalRunnerGroup`] 在
//! 其上做多标的对齐轮次编排——base TF 是决策节奏，1m bar 逐根驱动聚合/
//! 成交/盯市，同一 `at` 汇聚一轮、下一时间戳到达（或 `finish`）时 flush。
//! 子仓位动作（开/平/调整）在 [`super::dispatch`]，子仓位生命周期状态机
//! 在 [`super::model`]。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use strategy_core::risk::RiskControlConfig;
use strategy_core::signal::Side;
use strategy_positions::{HistoryPosition, PositionState, Side as PosSide};

use crate::bridge::CandleSeries;
use crate::core::aggregation::AggregationState;
use std::path::Path;

use crate::core::bar::{Bar, BarEventSource, MarketEvent, Timeframe, VecEventSource};
use crate::core::error::{Result, SpiError};
use crate::core::exchange::ExchangeParts;
use crate::core::execution::Fill;
use crate::core::position_sync::{book_for, PositionSync};
use crate::core::risk_guard::RiskGuard;
use crate::core::session::{SessionRules, TradingDay};
use crate::core::trading::Offset;

use super::align::AlignedEvent;
use super::config::StrategyConfig;
use super::dispatch::{self, StrategyIds};
use super::evaluator::effective_rules;
use super::features::FeaturePlan;
use super::model::{
    advance_master_state, advance_sub_state, master_position_view, MasterPosition,
    MasterPositionEvent, MasterPositionState, MasterPositionView, PortfolioPhase,
    StrategyPortfolio, SubPosition, SubPositionEvent, SubPositionState,
};
use super::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, MasterContext,
    PortfolioContext,
};
use super::signal_id::SubIntent;

/// 装配一个 `StrategySlot` 所需的全部输入。
pub struct SlotInit {
    pub parts: ExchangeParts,
    /// 含 base TF；已按配置解析
    pub timeframes: Vec<Timeframe>,
    pub feature_plan: FeaturePlan,
    /// L1：仅 fixed_stop_loss_pct，trailing/TP 关闭
    pub risk: RiskControlConfig,
    pub strategy_type: String,
    pub max_closed_len: usize,
}

/// 横截面单标的运行时切片（含执行端口与子仓位账本）。
pub struct StrategySlot {
    pub parts: ExchangeParts,
    pub state: AggregationState,
    pub pos_sync: PositionSync,
    pub risk_guard: RiskGuard,
    pub feature_plan: FeaturePlan,
    /// 已闭合 K 线快照
    pub candle_cache: CandleSeries,
    pub sub_positions: Vec<SubPosition>,
    pub last_mark: Option<f64>,
    pub last_bar_at: Option<DateTime<Utc>>,
    pub stale: bool,
    pub pending_newly: Vec<Timeframe>,
    pub blocked_closed: Vec<Timeframe>,
    pub signal_blocked_prev: bool,
    /// 最近一根已处理 bar 计算出的 `signal_resume`（= !no_trade 且上一根
    /// no_trade，与 `ExchangeRunner::aggregate_and_gate` 同口径；快照直接
    /// 读它，避免把「更新后的 signal_blocked_prev」当上一根用）。
    pub last_signal_resume: bool,
    pub last_day: Option<TradingDay>,
}

impl StrategySlot {
    pub fn new(init: SlotInit) -> Self {
        let book = book_for(&init.parts.instrument, init.parts.position.as_ref());
        Self {
            state: AggregationState::new(&init.timeframes, init.max_closed_len),
            pos_sync: PositionSync::new(book),
            risk_guard: RiskGuard::with_config(init.risk),
            parts: init.parts,
            feature_plan: init.feature_plan,
            candle_cache: CandleSeries::default(),
            sub_positions: Vec::new(),
            last_mark: None,
            last_bar_at: None,
            stale: false,
            pending_newly: Vec::new(),
            blocked_closed: Vec::new(),
            signal_blocked_prev: false,
            last_signal_resume: false,
            last_day: None,
        }
    }

    pub fn instrument_key(&self) -> String {
        self.parts.instrument.namespaced_key()
    }

    /// 取走行情源（替换为内存空源）。组合循环换源 / 收尾时用。
    pub fn take_source(&mut self) -> Box<dyn BarEventSource> {
        std::mem::replace(&mut self.parts.source, Box::new(VecEventSource::default()))
    }

    /// 逐根喂已闭合 1m：聚合 + 会话门控（与 `ExchangeRunner::preload` 同口径）。
    pub fn preload(&mut self, bars: &[Bar]) -> Result<()> {
        let Self { state, parts, .. } = self;
        for b in bars {
            state.on_closed_1m(
                b.clone(),
                parts.aggregation.as_ref(),
                parts.session.as_ref(),
            )?;
        }
        Ok(())
    }
}

// ─── RunnerGroup：组合轮次编排（PR-2/PR-3，Task 10）────────────────────────

/// 一次 flush 产出的轮次报告（app 逐 1m 采净值 / 回测收口）。
#[derive(Clone, Debug)]
pub struct RoundReport {
    pub at: DateTime<Utc>,
    pub nav: f64,
    pub cash: f64,
    pub fills: Vec<Fill>,
    pub actions: u64,
    pub rejected: u64,
    /// true = base TF 闭合触发的决策轮；false = 仅 1m 盯市/成交同步。
    pub decision_round: bool,
}

/// 组合运行的收口摘要（`finish()` 返回）。
#[derive(Clone)]
pub struct StrategyRunSummary {
    pub strategy_id: String,
    pub bars_processed: u64,
    pub provisional_bars: u64,
    pub bars_skipped_stale: u64,
    pub actions: u64,
    pub rejected_actions: u64,
    pub fills: Vec<Fill>,
    pub final_sub_positions: Vec<SubPosition>,
    pub final_portfolio: StrategyPortfolio,
}

/// 远程账本对账报告（PR-5 T3）。
#[derive(Clone, Debug, Default)]
pub struct ReconcileReport {
    /// 完成对账的标的（slot）数量
    pub synced_instruments: u32,
    /// `(instrument_key, kind, detail)`；
    /// kind ∈ {closed, qty_corrected, legacy_remote_only}
    pub drifts: Vec<(String, String, String)>,
}

/// 正在积累的决策轮：base TF 闭合的 `at` + 该 at 上闭合的 slot 集合。
struct PendingRound {
    at: DateTime<Utc>,
    /// 本轮回合触发的最后一个 1m bar 时间（换时间戳 = 换轮次）。
    last_bar_at: DateTime<Utc>,
    /// 本轮 base TF 闭合的 slot 下标（stale 判定真源）。
    slots: Vec<usize>,
    /// 本轮 base TF 闭合的 `newly_closed` 并集（含 M1 与 base TF）。
    newly: Vec<Timeframe>,
}

/// 横截面组合运行器（复数标的对齐轮次编排）。
///
/// base TF（`config.timeframes[0]`）是决策节奏：1m bar 逐根驱动聚合/成交/
/// 盯市，只有 base TF 闭合才进决策轮（L1/L2/L3/策略）。同一 `at` 上各 slot
/// 的 bar 汇聚一轮，下一时间戳到达（或 `finish`）时 `flush_round`。
/// 平仓历史投影：Store `history_position` 行 + `history_positions/{name}/{YYYYMMDD}.csv`
/// （22/24 列固定列序，下游报表契约）。
struct HistoryProjection {
    logger: strategy_positions::HistoryPositionLogger,
    /// 落盘名（`strategy_id` 中的 `:` 替换为 `_`，保证跨平台路径安全）。
    strategy_name: String,
    trading_mode: String,
}

pub struct CrossSectionalRunnerGroup {
    pub strategy_id: String,
    pub strategy_type: String,
    pub config: StrategyConfig,
    pub slots: Vec<StrategySlot>,
    pub strategy: Box<dyn CrossSectionalStrategyPort>,
    pub portfolio: StrategyPortfolio,
    /// 本策略的总仓账本（按 master_id 索引；与子仓一起持久化）
    masters: BTreeMap<String, MasterPosition>,
    pub phase: PortfolioPhase,
    base_tf: Timeframe,
    bars_since_rebalance: u64,
    pending_round: Option<PendingRound>,
    paused_after_panic: bool,
    summary: StrategyRunSummary,
    last_round: Option<RoundReport>,
    store: Option<Arc<strategy_store::Store>>,
    /// 平仓历史投影（回测/实盘共用；`None` = 不输出 history_position）。
    history: Option<HistoryProjection>,
    subs_changed: bool,
    last_snapshots: Vec<InstrumentSnapshot>,
    /// Phase 1 已提交、等待成交确认的减仓腿 signal_id（含 `_leg{i}`）。
    reduction_ids: std::collections::BTreeSet<String>,
    /// 本进程成交落账、尚未被远程空仓快照复核过的子仓 id。首拍读到同向空仓
    /// 时交易所仓位可能还没落库，先放过一拍；非空观测/下一拍即出表。
    fresh_opens: BTreeSet<String>,
    /// Phase 2 待派发的加仓动作（已按 key 排序；bool = 派发时刻的 entry_eligible）。
    pending_adds: Vec<(InstrumentAction, bool)>,
    /// 每个 slot 都以该总额虚拟起账时，NAV/现金汇总需扣除重复部分；`None` 表示各 slot 独立起账。
    initial_cash_total: Option<f64>,
    /// slot 是否各自以完整总额起账。合并总仓为 true（汇总时去重）；
    /// 逐标的独立总仓为 false（每个 slot 只拿自己那份，直接汇总）。
    slots_seed_total: bool,
    /// 启动首轮决策是否已触发（保证每实例只补一次，避免与实时 base 闭合重复）。
    startup_primed: bool,
}

impl CrossSectionalRunnerGroup {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        strategy_id: &str,
        strategy_type: &str,
        config: StrategyConfig,
        slots: Vec<StrategySlot>,
        strategy: Box<dyn CrossSectionalStrategyPort>,
        store: Option<Arc<strategy_store::Store>>,
    ) -> Result<Self> {
        let timeframes = &config.timeframes;
        let base_tf = Timeframe::parse(
            timeframes
                .first()
                .ok_or_else(|| SpiError::Config("strategy runner: timeframes 空".into()))?,
        )?;
        let initial_nav: f64 = slots
            .iter()
            .map(|s| {
                s.parts
                    .execution
                    .account_snapshot()
                    .map(|a| a.equity())
                    .unwrap_or(0.0)
            })
            .sum();
        let portfolio = StrategyPortfolio {
            strategy_id: strategy_id.to_string(),
            phase: PortfolioPhase::Running,
            active_master_id: None,
            master_seq: 0,
            nav: initial_nav,
            high_watermark: initial_nav,
            drawdown: 0.0,
            peak_unrealized_pnl: 0.0,
            trail_armed: false,
            target_holdings: Vec::new(),
            max_total_notional: config.risk.max_total_notional,
            last_rebalance_at: None,
            rebalance_epoch: 0,
            bars_since_rebalance: 0,
            scope_risk: BTreeMap::new(),
        };
        let summary = StrategyRunSummary {
            strategy_id: strategy_id.to_string(),
            bars_processed: 0,
            provisional_bars: 0,
            bars_skipped_stale: 0,
            actions: 0,
            rejected_actions: 0,
            fills: Vec::new(),
            final_sub_positions: Vec::new(),
            final_portfolio: portfolio.clone(),
        };
        let g = Self {
            strategy_id: strategy_id.to_string(),
            strategy_type: strategy_type.to_string(),
            config,
            slots,
            strategy,
            portfolio,
            masters: BTreeMap::new(),
            phase: PortfolioPhase::Running,
            base_tf,
            bars_since_rebalance: 0,
            pending_round: None,
            paused_after_panic: false,
            summary,
            last_round: None,
            store,
            history: None,
            subs_changed: false,
            last_snapshots: Vec::new(),
            reduction_ids: Default::default(),
            fresh_opens: Default::default(),
            pending_adds: Vec::new(),
            initial_cash_total: None,
            slots_seed_total: true,
            startup_primed: false,
        };
        // 初始账本不在此落盘：实盘装配会把构造期空组合写进 Store 活表，
        // 使随后的 recover 因同 id 已存在而跳过盘载状态（重启即丢失
        // 高水位/纪元）。落盘由 recover 末尾或首个决策轮负责。
        Ok(g)
    }

    /// 声明每个 slot 都以同一笔总资金虚拟起账：汇总 NAV/现金时扣除
    /// `initial_cash_total * (slot 数 - 1)`，避免重复现金抬高净值与仓位上限。
    pub fn set_initial_cash_total(&mut self, total: f64) {
        self.set_initial_cash(total, true);
    }

    /// 逐标的独立总仓：每个 slot 已按 `total / slot 数` 起账，汇总 NAV
    /// 不再扣除重复现金。
    pub fn set_initial_cash_split(&mut self, total: f64) {
        self.set_initial_cash(total, false);
    }

    fn set_initial_cash(&mut self, total: f64, slots_seed_total: bool) {
        self.initial_cash_total = Some(total);
        self.slots_seed_total = slots_seed_total;
        let nav = self.current_nav();
        self.portfolio.nav = nav;
        // 起账时点没有交易，净值回撤口径从去重后的真实总额重新起算。
        self.portfolio.high_watermark = nav;
        self.portfolio.drawdown = 0.0;
        self.summary.final_portfolio = self.portfolio.clone();
    }

    /// 启用平仓历史投影（回测/实盘共用）：平仓时写 Store `history_position`
    /// 行并旁路追加 `history_positions/{name}/{YYYYMMDD}.csv`。`None` = 不输出。
    pub fn enable_history_projection(&mut self, state_dir: impl AsRef<Path>, trading_mode: &str) {
        self.history = Some(HistoryProjection {
            logger: strategy_positions::HistoryPositionLogger::new(state_dir),
            strategy_name: self.strategy_id.replace(':', "_"),
            trading_mode: trading_mode.to_string(),
        });
    }

    /// 逐 slot 喂 warmup（已闭合 1m），不触发任何决策。
    pub fn preload(&mut self, warmup: &[Vec<Bar>]) -> Result<()> {
        for (slot, bars) in self.slots.iter_mut().zip(warmup.iter()) {
            slot.preload(bars)?;
        }
        Ok(())
    }

    /// 启动首轮决策：用各 slot 已闭合的最后一根 base bar 立即 flush 一轮，
    /// 让开机第一拍就参与决策（否则要等下一个 base TF 闭合；空仓会白等
    /// 一整个 `every_bars`）。每实例只补一次，pending 已存在时跳过。
    pub fn prime_decision_round(&mut self) -> Result<Option<RoundReport>> {
        if self.startup_primed || self.pending_round.is_some() {
            return Ok(None);
        }
        let base = self.base_tf;
        let mut at: Option<DateTime<Utc>> = None;
        let mut slots: Vec<usize> = Vec::new();
        for (i, slot) in self.slots.iter().enumerate() {
            if let Some(b) = slot.state.closed(base).back() {
                slots.push(i);
                at = Some(at.map_or(b.open_time, |a| a.max(b.open_time)));
            }
        }
        let Some(at) = at else {
            return Ok(None); // 预热不足：没有可判的 base bar，等实时闭合
        };
        self.startup_primed = true;
        self.pending_round = Some(PendingRound {
            at,
            last_bar_at: at,
            slots,
            newly: vec![base],
        });
        self.flush_round().map(Some)
    }

    /// 依次 take 每个 slot 的行情源，key 用 `instrument_key()`。
    pub fn sources(&mut self) -> (Vec<Box<dyn BarEventSource>>, Vec<String>) {
        let mut srcs = Vec::with_capacity(self.slots.len());
        let mut keys = Vec::with_capacity(self.slots.len());
        for slot in self.slots.iter_mut() {
            keys.push(slot.instrument_key());
            srcs.push(slot.take_source());
        }
        (srcs, keys)
    }

    /// 导出用快照：每个 slot 的标的 + 全部已闭合大周期 `Bar`（含
    /// `extensions.trading_day`）。CSV 写入留在 app 层（engine 不依赖 data）。
    pub fn closed_exports(
        &self,
    ) -> Vec<(crate::core::identity::InstrumentId, Timeframe, Vec<Bar>)> {
        let mut out = Vec::new();
        for slot in &self.slots {
            for &tf in slot.state.timeframes() {
                if tf.0 <= 1 {
                    continue;
                }
                let bars = slot.state.closed(tf).iter().cloned().collect();
                out.push((slot.parts.instrument.clone(), tf, bars));
            }
        }
        out
    }

    /// 消费一个已对齐事件。返回该事件触发的轮次报告（切换轮次时返回上一个
    /// pending 决策轮的报告；换时间戳到达由调用方保证事件有序性）。
    pub fn on_event(&mut self, ev: AlignedEvent) -> Result<Option<RoundReport>> {
        match ev.event {
            MarketEvent::Bar(bar) => self.on_bar(ev.slot, bar),
            MarketEvent::SessionGap(gap) => {
                let slot = self.slots.get_mut(ev.slot).ok_or_else(|| {
                    SpiError::Execution(format!("strategy runner: 无效 slot 下标 {}", ev.slot))
                })?;
                let closed = slot.state.on_gap(&gap, slot.parts.aggregation.as_ref());
                queue_pending(&mut slot.pending_newly, closed);
                Ok(None)
            }
            MarketEvent::DayBoundary(day) => {
                let slot = self.slots.get_mut(ev.slot).ok_or_else(|| {
                    SpiError::Execution(format!("strategy runner: 无效 slot 下标 {}", ev.slot))
                })?;
                let closed = slot.state.on_day_boundary(slot.parts.aggregation.as_ref());
                queue_pending(&mut slot.pending_newly, closed);
                slot.parts.position.rollover(&mut slot.pos_sync.book, day);
                slot.last_day = Some(day);
                Ok(None)
            }
            MarketEvent::Roll(roll) => {
                let slot = self.slots.get_mut(ev.slot).ok_or_else(|| {
                    SpiError::Execution(format!("strategy runner: 无效 slot 下标 {}", ev.slot))
                })?;
                let next = slot
                    .parts
                    .subscription
                    .on_roll(&roll, &slot.parts.subscriptions)?;
                slot.parts.subscriptions = next;
                Ok(None)
            }
            MarketEvent::Reconnected | MarketEvent::SourceIdle => Ok(None),
        }
    }

    /// 收口：flush 最后一个 pending 轮次，同步未落账成交，固化 summary。
    pub fn finish(&mut self) -> Result<StrategyRunSummary> {
        if self.pending_round.is_some() {
            self.flush_round()?;
        }
        for i in 0..self.slots.len() {
            self.sync_slot_fills(i)?;
        }
        let settle_at = self
            .slots
            .iter()
            .filter_map(|s| s.last_bar_at)
            .max()
            .unwrap_or_else(Utc::now);
        self.settle_masters(settle_at);
        self.persist_round();
        self.subs_changed = false;
        self.summary.final_sub_positions = self.all_sub_positions();
        self.summary.final_portfolio = self.portfolio.clone();
        Ok(self.summary.clone())
    }

    /// 取走最后一次 flush 的轮次报告（`finish` 后调用可补采最后一根净值）。
    pub fn take_last_round(&mut self) -> Option<RoundReport> {
        self.last_round.take()
    }

    /// 组内交易日口径（取首个 slot 的 SessionRules）：横截面组按同市场装配；
    /// crypto 返回 UTC 自然日，期货返回交易日。用于日报/导出的交易日标注。
    pub fn trading_day_at(&self, at: DateTime<Utc>) -> Option<TradingDay> {
        self.slots
            .first()
            .map(|s| s.parts.session.trading_day_of(at))
    }

    /// 取当前活动总仓；没有可用总仓时创建 `master_{seq:06}`。
    ///
    /// 只在确实要开仓/加仓时调用——纯平仓轮次不得创建空总仓。只复用
    /// `Open` 总仓：`Closing` 中的总仓不得接收新仓，否则新子仓会逃过
    /// 本次总仓平仓。
    fn ensure_active_master(&mut self, at: DateTime<Utc>, scope: Option<&str>) -> String {
        if scope.is_none() {
            if let Some(id) = self.portfolio.active_master_id.clone() {
                if self
                    .masters
                    .get(&id)
                    .is_some_and(|m| m.scope.is_none() && m.state == MasterPositionState::Open)
                {
                    return id;
                }
            }
        }
        if let Some(existing) = self
            .masters
            .values()
            .find(|m| m.scope.as_deref() == scope && m.state == MasterPositionState::Open)
        {
            let id = existing.master_id.clone();
            if scope.is_none() {
                self.portfolio.active_master_id = Some(id.clone());
            }
            return id;
        }
        let seq = self.portfolio.master_seq.saturating_add(1);
        self.portfolio.master_seq = seq;
        let master_id = format!("master_{seq:06}");
        if scope.is_none() {
            self.portfolio.active_master_id = Some(master_id.clone());
        }
        self.masters.insert(
            master_id.clone(),
            MasterPosition {
                master_id: master_id.clone(),
                strategy_id: self.strategy_id.clone(),
                scope: scope.map(str::to_string),
                state: MasterPositionState::Open,
                opened_at: Some(at),
                closed_at: None,
            },
        );
        master_id
    }

    /// 成交归属总仓：优先活动总仓（`Closing` 也复用——在途开仓成交必须
    /// 落在原总仓，随后立即补平），其次任一非 `Closed` 总仓，最后新建。
    /// 与 [`Self::ensure_active_master`] 的差异是允许 `Closing` 归属：成交
    /// 是既成事实，不能因为总仓正在平仓而改写归属。
    fn master_for_fill(&mut self, at: DateTime<Utc>, scope: Option<&str>) -> String {
        if let Some(id) = self.portfolio.active_master_id.clone() {
            if self.masters.get(&id).is_some_and(|m| {
                m.scope.as_deref() == scope && m.state != MasterPositionState::Closed
            }) {
                return id;
            }
        }
        if let Some(existing) = self
            .masters
            .values()
            .find(|m| m.scope.as_deref() == scope && m.state != MasterPositionState::Closed)
        {
            let id = existing.master_id.clone();
            if scope.is_none() {
                self.portfolio.active_master_id = Some(id.clone());
            }
            return id;
        }
        self.ensure_active_master(at, scope)
    }

    /// 总仓是否处于 `Closing`（新开/加仓必须被拦截）。
    fn master_is_closing(&self, master_id: &str) -> bool {
        self.masters
            .get(master_id)
            .is_some_and(|m| m.state == MasterPositionState::Closing)
    }

    /// 把总仓推进到 `Closing`；返回 true = 本次发生状态变化。
    fn start_master_closing(&mut self, master_id: &str) -> bool {
        let Some(master) = self.masters.get_mut(master_id) else {
            return false;
        };
        let Some(next) = advance_master_state(master.state, MasterPositionEvent::StartClosing)
        else {
            return false;
        };
        master.state = next;
        true
    }

    /// 总仓收口：只有关联子仓全部 `Closed`（无在途开仓/减仓）才允许
    /// `AllSubsClosed`。空总仓（开仓被拒后遗留）同样在这里收口。
    /// 返回 true = 本轮有关闭总仓。
    fn settle_masters(&mut self, at: DateTime<Utc>) -> bool {
        let pending: Vec<String> = self
            .masters
            .values()
            .filter(|m| m.state != MasterPositionState::Closed)
            .map(|m| m.master_id.clone())
            .collect();
        let mut changed = false;
        for master_id in pending {
            let has_live_sub = self
                .slots
                .iter()
                .flat_map(|s| s.sub_positions.iter())
                .any(|s| s.master_id == master_id && s.state != SubPositionState::Closed);
            if has_live_sub {
                continue;
            }
            let Some(master) = self.masters.get_mut(&master_id) else {
                continue;
            };
            let Some(next) = advance_master_state(master.state, MasterPositionEvent::AllSubsClosed)
            else {
                continue;
            };
            master.state = next;
            master.closed_at = Some(at);
            if self.portfolio.active_master_id.as_deref() == Some(master_id.as_str()) {
                self.portfolio.active_master_id = None;
            }
            tracing::info!(
                strategy_id = %self.strategy_id,
                master_id = %master_id,
                "总仓全部子仓已收口 → Closed"
            );
            changed = true;
        }
        changed
    }

    /// 平总仓：先置 `Closing`，再对每个关联的活动子仓发平仓单。
    /// 在途开仓（`PendingOpen`/`OpenRequested`）不发单，成交落地后由
    /// [`Self::close_inflight_under_closing`] 补平；`CloseRequested` 由
    /// `close_position` 的防重复守卫跳过。返回 `(actions, rejected)`。
    fn close_master(
        &mut self,
        master_id: &str,
        prices: &BTreeMap<String, f64>,
        reason: &str,
        is_stop_loss: bool,
        intent: SubIntent,
    ) -> Result<(u64, u64)> {
        match self.masters.get(master_id).map(|m| m.state) {
            Some(MasterPositionState::Open) => {
                let _ = self.start_master_closing(master_id);
            }
            Some(MasterPositionState::Closing) => {}
            _ => return Ok((0, 0)),
        }

        let mut actions = 0;
        let mut rejected = 0;
        for idx in 0..self.slots.len() {
            let needs_close = self.slots[idx].sub_positions.iter().any(|s| {
                s.master_id == master_id
                    && matches!(
                        s.state,
                        SubPositionState::Active | SubPositionState::CloseRequested
                    )
            });
            if !needs_close {
                continue;
            }
            let key = self.slots[idx].instrument_key();
            let price = prices
                .get(&key)
                .copied()
                .or(self.slots[idx].last_mark)
                .unwrap_or(0.0);
            let ids = StrategyIds {
                strategy_type: &self.strategy_type,
                strategy_id: &self.strategy_id,
                master_id,
                rebalance_epoch: self.portfolio.rebalance_epoch,
            };
            let out = dispatch::close_position(
                &mut self.slots[idx],
                &ids,
                price,
                reason,
                is_stop_loss,
                intent,
            )?;
            actions += out.actions;
            rejected += out.rejected;
            if out.actions > 0 {
                self.subs_changed = true;
            }
        }
        Ok((actions, rejected))
    }

    /// `Closing` 总仓的晚期开仓成交（下单在平仓前、成交在平仓后）必须
    /// 立即补平，否则总仓会等待一个策略已放弃的子仓。
    fn close_inflight_under_closing(&mut self, slot_idx: usize) -> Result<()> {
        if self.slots[slot_idx].last_bar_at.is_none() {
            return Ok(());
        }
        let closing: Vec<String> = self
            .masters
            .values()
            .filter(|m| m.state == MasterPositionState::Closing)
            .map(|m| m.master_id.clone())
            .collect();
        for master_id in closing {
            let has_active = self.slots[slot_idx]
                .sub_positions
                .iter()
                .any(|s| s.master_id == master_id && s.state == SubPositionState::Active);
            if !has_active {
                continue;
            }
            let price = self.slots[slot_idx].last_mark.unwrap_or(0.0);
            let ids = StrategyIds {
                strategy_type: &self.strategy_type,
                strategy_id: &self.strategy_id,
                master_id: &master_id,
                rebalance_epoch: self.portfolio.rebalance_epoch,
            };
            let out = dispatch::close_position(
                &mut self.slots[slot_idx],
                &ids,
                price,
                "master closing: 在途开仓成交补平",
                false,
                SubIntent::RiskCloseAll,
            )?;
            if out.actions > 0 {
                self.subs_changed = true;
            }
        }
        Ok(())
    }

    pub fn master_position(&self) -> MasterPositionView {
        let subs = self.all_sub_positions();
        let mut marks = BTreeMap::new();
        for s in &self.slots {
            if let Some(m) = s.last_mark {
                marks.insert(s.instrument_key(), m);
            }
        }
        master_position_view(&subs, &marks, &self.multiplier_map())
    }

    /// 逐标的独立总仓时，仓位/NAV/容量都按该 slot 的作用域计算；
    /// 合并总仓仍走策略级汇总。
    fn position_scope(&self, idx: usize) -> Option<String> {
        (!self.config.merge_positions).then(|| self.slots[idx].instrument_key())
    }

    fn master_id_for_slot(&self, idx: usize) -> String {
        if !self.config.merge_positions {
            let key = self.slots[idx].instrument_key();
            if let Some(sub) = self.slots[idx]
                .sub_positions
                .iter()
                .find(|s| s.state != SubPositionState::Closed && !s.master_id.is_empty())
            {
                return sub.master_id.clone();
            }
            if let Some(master) = self.masters.values().find(|m| {
                m.scope.as_deref() == Some(key.as_str()) && m.state != MasterPositionState::Closed
            }) {
                return master.master_id.clone();
            }
            return String::new();
        }
        self.portfolio.active_master_id.clone().unwrap_or_default()
    }

    fn master_position_for_slot(&self, idx: usize) -> MasterPositionView {
        if self.config.merge_positions {
            return self.master_position();
        }
        let mut marks = BTreeMap::new();
        if let Some(mark) = self.slots[idx].last_mark {
            marks.insert(self.slots[idx].instrument_key(), mark);
        }
        master_position_view(
            &self.slots[idx].sub_positions,
            &marks,
            &self.multiplier_map(),
        )
    }

    fn current_nav_for_slot(&self, idx: usize) -> f64 {
        if self.config.merge_positions {
            return self.portfolio.nav;
        }
        self.slots[idx]
            .parts
            .execution
            .account_snapshot()
            .map(|a| a.equity())
            .unwrap_or(0.0)
    }

    /// 逐标的 L3：以该标的子仓聚合为组合边界，回落/止损只影响该标的。
    /// 返回 `(派发动作数, 拒单数, 是否阻断该标的后续决策)`。
    fn evaluate_scope_l3(
        &mut self,
        scope: &str,
        price: f64,
        snapshots: &[InstrumentSnapshot],
    ) -> Result<(u64, u64, bool)> {
        let masters: Vec<String> = self
            .masters
            .values()
            .filter(|m| m.scope.as_deref() == Some(scope) && m.state != MasterPositionState::Closed)
            .map(|m| m.master_id.clone())
            .collect();
        let has_live_sub = self
            .slots
            .iter()
            .flat_map(|s| s.sub_positions.iter())
            .any(|s| {
                s.state != SubPositionState::Closed && masters.iter().any(|m| m == &s.master_id)
            });
        if self.scope_is_paused(scope) {
            if masters.is_empty() && !has_live_sub {
                // 已清零：跟踪基准复位，暂停状态保留到重新放行。
                if let Some(state) = self.portfolio.scope_risk.get_mut(scope) {
                    state.peak_unrealized_pnl = 0.0;
                    state.trail_armed = false;
                }
            }
            return Ok((0, 0, true));
        }
        let Some(idx) = self.slots.iter().position(|s| s.instrument_key() == scope) else {
            return Ok((0, 0, false));
        };
        let view = self.master_position_for_slot(idx);
        let pnl_pct = if view.gross_notional > 1e-12 {
            view.unrealized_pnl / view.gross_notional * 100.0
        } else {
            0.0
        };
        let state = self
            .portfolio
            .scope_risk
            .entry(scope.to_string())
            .or_default();
        if view.unrealized_pnl > state.peak_unrealized_pnl {
            state.peak_unrealized_pnl = view.unrealized_pnl;
        }
        let mut fired = false;
        if let Some(tr) = &self.config.portfolio_exit.trailing {
            if pnl_pct >= tr.arm_pnl_pct {
                state.trail_armed = true;
            }
            if state.trail_armed
                && state.peak_unrealized_pnl > 0.0
                && view.unrealized_pnl
                    <= state.peak_unrealized_pnl * (1.0 - tr.giveback_pct / 100.0)
            {
                fired = true;
            }
        }
        if let Some(sl) = self.config.portfolio_exit.stop_loss_pct {
            if pnl_pct <= -sl {
                fired = true;
            }
        }
        if !fired {
            return Ok((0, 0, false));
        }
        state.paused = true;
        tracing::warn!(
            strategy_id = %self.strategy_id,
            scope,
            pnl_pct,
            "L3 portfolio_exit 触发 → 该标的独立总仓全平（scope Paused）"
        );
        let mut prices = BTreeMap::new();
        for slot in &self.slots {
            let key = slot.instrument_key();
            let price = snapshots
                .iter()
                .find(|sn| sn.instrument.namespaced_key() == key)
                .map(|sn| sn.bar.close)
                .or(slot.last_mark)
                .unwrap_or(price);
            prices.insert(key, price);
        }
        let mut actions = 0;
        let mut rejected = 0;
        for master_id in &masters {
            let (a, r) = self.close_master(
                master_id,
                &prices,
                "portfolio_exit: CloseScope",
                false,
                SubIntent::RiskCloseAll,
            )?;
            actions += a;
            rejected += r;
        }
        Ok((actions, rejected, true))
    }

    fn scope_is_paused(&self, scope: &str) -> bool {
        self.portfolio
            .scope_risk
            .get(scope)
            .is_some_and(|s| s.paused)
    }

    fn dispatch_nav(&self, idx: usize) -> f64 {
        if self.config.merge_positions {
            self.portfolio.nav
        } else {
            self.current_nav_for_slot(idx)
        }
    }

    pub fn is_paused(&self) -> bool {
        self.paused_after_panic
    }

    /// 决策/兜底是否允许下单：panic 暂停、Liquidating（清算中禁止开新仓）、
    /// L3 清零后的 Paused、活动总仓 `Closing` 一律拦截；仅 Running 放行。
    fn can_trade(&self) -> bool {
        if self.paused_after_panic || self.phase != PortfolioPhase::Running {
            return false;
        }
        if !self.config.merge_positions {
            // 逐标的独立总仓：某个标的的总仓在 Closing 只拦该标的，
            // 由 dispatch_sub 按 slot 作用域兜底，不阻断其他标的。
            return true;
        }
        !self
            .portfolio
            .active_master_id
            .as_deref()
            .is_some_and(|id| self.master_is_closing(id))
    }

    pub fn take_fills(&mut self) -> Vec<Fill> {
        std::mem::take(&mut self.summary.fills)
    }

    /// 启动恢复（live/回测共用，PR-5 T3）：Store 行重建账本 + 远程对账。
    ///
    /// 只信任 Store 落盘行 + 远程账本；内存中间态（含旧策略实例）一律丢弃
    /// ——`fresh_strategy` 必须是调用方新建的实例。`after_panic=true` 保留
    /// 原 `resume_from_store` 语义（panic 后恢复）；`false` = 正常启动路径。
    /// 两条路径终态一致：Store 行 → 子仓/持仓态重建 → `reconcile_remote`
    /// → Running（先账本，后消费——调用方在事件泵启动前执行本方法）。
    pub fn recover_from_store(
        &mut self,
        store: &strategy_store::Store,
        fresh_strategy: Box<dyn CrossSectionalStrategyPort>,
        after_panic: bool,
    ) -> Result<ReconcileReport> {
        if after_panic {
            tracing::warn!(
                strategy_id = %self.strategy_id,
                "recover：panic 后恢复，内存中间态一律丢弃（只信 Store 行 + 远程账本）"
            );
        } else {
            tracing::info!(
                strategy_id = %self.strategy_id,
                "recover：正常启动恢复（Store 行重建 + 远程对账，先账本后消费）"
            );
        }
        // 盘载主（T7 重启演练暴露）：冷启动内存表为空，先把落盘行载入
        // （`Store::recover` 只填空缺 id——同进程监督重建安全，不覆盖活表）。
        store
            .recover::<StrategyPortfolio>()
            .map_err(|e| SpiError::Execution(format!("strategy_portfolio 盘载失败: {e}")))?;
        store
            .recover::<SubPosition>()
            .map_err(|e| SpiError::Execution(format!("sub_position 盘载失败: {e}")))?;
        store
            .recover::<MasterPosition>()
            .map_err(|e| SpiError::Execution(format!("master_position 盘载失败: {e}")))?;
        self.masters = store
            .table::<MasterPosition>()
            .by_scope(&self.strategy_id)
            .into_iter()
            .map(|m| (m.master_id.clone(), m))
            .collect();
        self.strategy = fresh_strategy;
        if let Some(p) = store.table::<StrategyPortfolio>().get(&self.strategy_id) {
            self.portfolio = p;
        }
        // 调仓计数随组合行持久化：不恢复会让每个进程重启都重新等满
        // `every_bars`（实盘每天重启即永不下单）。
        self.bars_since_rebalance = self.portfolio.bars_since_rebalance;
        // 风险预算是当前配置而非历史状态：盘载值只在同一配置下有意义，
        // 重启改配置时必须让新预算生效，否则容量缓冲会被旧值静默覆盖。
        self.portfolio.max_total_notional = self.config.risk.max_total_notional;
        let stored = store.table::<SubPosition>().by_scope(&self.strategy_id);
        for slot in self.slots.iter_mut() {
            let key = slot.instrument_key();
            let mut mine: Vec<SubPosition> = stored
                .iter()
                .filter(|s| s.instrument.namespaced_key() == key)
                .cloned()
                .collect();
            mine.sort_by(|a, b| a.id.cmp(&b.id));
            slot.sub_positions = mine;
            slot.pos_sync.pos = None;
            slot.pos_sync.open_meta.clear();
            // 执行层持仓状态由非 Closed 子仓重建（PositionState 单一事实来源）。
            if let Some(active) = slot
                .sub_positions
                .iter()
                .find(|s| s.state != SubPositionState::Closed)
            {
                let entry_time = active.opened_at.unwrap_or_default();
                slot.pos_sync.pos = Some(PositionState {
                    position_id: active.id.clone(),
                    symbol: key.clone(),
                    side: match active.side {
                        Side::Long => PosSide::Long,
                        Side::Short => PosSide::Short,
                    },
                    entry_price: active.entry_price,
                    entry_time,
                    entry_timestamp: entry_time.timestamp(),
                    peak_price: active.entry_price,
                    stop_price: active.stop_price.unwrap_or(0.0),
                    max_pnl_pct: 0.0,
                    min_pnl_pct: 0.0,
                    atr_at_entry: 0.0,
                    total_size: active.qty,
                    remaining_size: active.qty,
                    realized_pnl: active.realized_pnl.unwrap_or(0.0),
                    trail_activated: false,
                    trail_trigger_pct: 0.0,
                    // raw 字面量无法从子仓账本恢复（宁缺勿伪，live 以逐笔回报为准）
                    entry_size_raw: None,
                    remaining_size_raw: None,
                    exit_active: false,
                    exit_price: 0.0,
                    strategy_id: Some(self.strategy_id.clone()),
                });
            }
        }
        self.migrate_legacy_masters(store);
        self.phase = PortfolioPhase::Running;
        self.paused_after_panic = false;
        // 逐标的暂停与合并模式一致：重启/resume 视为放行；仍在平仓的总仓
        // 由 dispatch 的 Closing 闸门拦截，不会提前接新仓。
        for state in self.portfolio.scope_risk.values_mut() {
            state.paused = false;
        }
        // 在途保护先于远程对账：把盘载信号行先载入内核，否则对账读不到
        // Submitted/Accepted 的未成交开仓，会把 OpenRequested 误判成远程
        // 空仓而收口（重启时交易所回报还没到的窗口期）。
        for slot in self.slots.iter_mut() {
            let Some(live) = slot.parts.execution.as_live_mut() else {
                continue;
            };
            if let Err(e) = live.manager_mut().recover() {
                tracing::warn!(error = %e, "recover：信号行盘载失败，在途保护可能失效");
            }
        }
        // 远程账本是最终事实：先对账（含 Store 行与远程的平仓/数量漂移），
        // 再交还事件泵消费（「先账本后消费」）。
        let report = self.reconcile_remote()?;
        self.persist_round();
        Ok(report)
    }

    /// 旧数据迁移：旧 `SubPosition` 没有 `master_id`，恢复时统一归入一个
    /// legacy 总仓；已迁移数据幂等（全部有主则只校正指针/序号）。
    fn migrate_legacy_masters(&mut self, store: &strategy_store::Store) {
        // 落盘指针可能指向已 Closed/不存在的总仓，先失效化。
        if self.portfolio.active_master_id.as_ref().is_some_and(|id| {
            self.masters
                .get(id)
                .is_none_or(|m| m.state == MasterPositionState::Closed)
        }) {
            self.portfolio.active_master_id = None;
        }

        // 逐标的独立总仓：旧数据没有 master_id，按标的分到各自作用域，
        // 不能沿用合并路径把所有标的塞进一个 legacy 总仓。
        if !self.config.merge_positions {
            let targets: Vec<(usize, String)> = self
                .slots
                .iter()
                .enumerate()
                .flat_map(|(i, s)| {
                    s.sub_positions
                        .iter()
                        .filter(|p| p.master_id.is_empty())
                        .map(move |p| (i, p.id.clone()))
                })
                .collect();
            for (slot_idx, sub_id) in targets {
                let scope = self.slots[slot_idx].instrument_key();
                let at = self.slots[slot_idx]
                    .sub_positions
                    .iter()
                    .find(|s| s.id == sub_id)
                    .and_then(|s| s.opened_at.or(s.closed_at))
                    .unwrap_or_else(Utc::now);
                let master_id = self.ensure_active_master(at, Some(&scope));
                if let Some(sub) = self.slots[slot_idx]
                    .sub_positions
                    .iter_mut()
                    .find(|s| s.id == sub_id)
                {
                    sub.master_id = master_id;
                    store.table::<SubPosition>().upsert(sub.clone());
                    self.subs_changed = true;
                }
            }
        }

        let unassigned: Vec<SubPosition> = if self.config.merge_positions {
            self.slots
                .iter()
                .flat_map(|s| s.sub_positions.iter())
                .filter(|s| s.master_id.is_empty())
                .cloned()
                .collect()
        } else {
            // 逐标的路径已在上方补齐；不走合并 legacy 分支。
            Vec::new()
        };
        if !unassigned.is_empty() {
            let mut target = self.portfolio.active_master_id.clone().filter(|id| {
                self.masters
                    .get(id)
                    .is_some_and(|m| m.state != MasterPositionState::Closed)
            });
            if target.is_none() {
                target = self
                    .masters
                    .values()
                    .find(|m| m.state != MasterPositionState::Closed)
                    .map(|m| m.master_id.clone());
            }
            if target.is_none() {
                let live = unassigned
                    .iter()
                    .any(|s| s.state != SubPositionState::Closed);
                let opened_at = unassigned.iter().filter_map(|s| s.opened_at).min();
                let latest_closed_at = unassigned.iter().filter_map(|s| s.closed_at).max();
                let master =
                    self.masters
                        .entry("master_legacy".to_string())
                        .or_insert(MasterPosition {
                            master_id: "master_legacy".to_string(),
                            strategy_id: self.strategy_id.clone(),
                            scope: None,
                            state: if live {
                                MasterPositionState::Open
                            } else {
                                MasterPositionState::Closed
                            },
                            opened_at,
                            closed_at: if live { None } else { latest_closed_at },
                        });
                // 同一 legacy 总仓可能已关闭；新出现的活子仓需要重新打开。
                if live && master.state == MasterPositionState::Closed {
                    master.state = MasterPositionState::Open;
                    master.closed_at = None;
                }
                target = Some("master_legacy".to_string());
            }
            let target_id = target.expect("legacy 总仓刚刚已创建");
            for slot in &mut self.slots {
                for sub in &mut slot.sub_positions {
                    if sub.master_id.is_empty() {
                        sub.master_id = target_id.clone();
                        store.table::<SubPosition>().upsert(sub.clone());
                        self.subs_changed = true;
                    }
                }
            }
        }

        // 有活动子仓但没有指针：指向其总仓，保证重启后开仓复用同一总仓。
        if self.portfolio.active_master_id.is_none() {
            self.portfolio.active_master_id = self
                .slots
                .iter()
                .flat_map(|s| s.sub_positions.iter())
                .find(|s| {
                    s.state != SubPositionState::Closed
                        && !s.master_id.is_empty()
                        && self
                            .masters
                            .get(&s.master_id)
                            .is_some_and(|m| m.state != MasterPositionState::Closed)
                })
                .map(|s| s.master_id.clone());
        }

        // `master_seq` 不得低于任何已落盘总仓的序号（防止重启后撞 id）。
        let max_seq = self
            .masters
            .keys()
            .filter_map(|id| {
                id.strip_prefix("master_")
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .max()
            .unwrap_or(0);
        if self.portfolio.master_seq < max_seq {
            self.portfolio.master_seq = max_seq;
        }
    }

    /// panic 后恢复的薄封装（既有调用与测试零改动，PR-5 T3）。
    pub fn resume_from_store(
        &mut self,
        store: &strategy_store::Store,
        fresh_strategy: Box<dyn CrossSectionalStrategyPort>,
    ) -> Result<()> {
        self.recover_from_store(store, fresh_strategy, true)
            .map(|_| ())
    }

    /// 远程账本对账（PR-5 T3）：live 周期/恢复期调用，**永不发单**。
    ///
    /// - `pos_sync.book` ← 远程 lots（最终事实，含 `remaining_raw` 原样）；
    /// - 本地非 Closed 子仓而同向远程已平 → `RemoteFlat` advance 到 `Closed`
    ///   （`closed_at` 取该 slot 最近 bar 时间，禁止 `now()`）+ 告警；
    ///   但本进程刚成交的 Active 腿除外：首拍空仓快照时交易所仓位可能尚未
    ///   落库，等下拍复核；
    /// - 数量不符 → 子仓按**同向**远程剩余记 `qty_corrected`（Decimal 口径
    ///   不变：book 的 raw 字面量原样，未知不伪造）；
    /// - 远程有仓而本组无对应非 Closed 子仓 → `legacy_remote_only` 仅审计
    ///   （不建 SubPosition、不入 NAV/portfolio 度量）。
    pub fn reconcile_remote(&mut self) -> Result<ReconcileReport> {
        let mut report = ReconcileReport::default();
        let strategy_id = self.strategy_id.clone();
        // 已 Closed 的子仓不会再被判收口，顺手清掉它们的待复核标记。
        let live_ids: BTreeSet<String> = self
            .slots
            .iter()
            .flat_map(|s| s.sub_positions.iter())
            .filter(|p| p.state != SubPositionState::Closed)
            .map(|p| p.id.clone())
            .collect();
        self.fresh_opens.retain(|id| live_ids.contains(id));
        // 远程对平（外部平仓）没有成交回报：收集终态后统一投影 history_position。
        // 元组只在函数内传递，拆 struct 只增加调用点噪音。
        #[allow(clippy::type_complexity)]
        let mut external_closed: Vec<(
            usize,
            String,
            DateTime<Utc>,
            f64,
            f64,
            Option<PositionState>,
        )> = Vec::new();
        for (slot_idx, slot) in self.slots.iter_mut().enumerate() {
            let instrument = slot.parts.instrument.clone();
            let key = slot.instrument_key();
            let remote = slot.parts.execution.remote_positions(&instrument)?;
            let remote_active = remote.iter().any(|l| l.remaining > 1e-12);
            slot.pos_sync.sync_book(remote.clone());
            report.synced_instruments += 1;

            // G1：每标的至多一条非 Closed 子仓。
            let Some(idx) = slot
                .sub_positions
                .iter()
                .position(|s| s.state != SubPositionState::Closed)
            else {
                if remote_active {
                    report.drifts.push((
                        key,
                        "legacy_remote_only".into(),
                        format!(
                            "remote_lots={} remote_rem={:.6}（仅审计，不入账本）",
                            remote.len(),
                            slot.pos_sync.total_remaining()
                        ),
                    ));
                }
                continue;
            };

            // 对冲账本多空并存：数量换算只和子仓同向批次比。
            let sub_side = slot.sub_positions[idx].side;
            let remote_side_qty: f64 = remote
                .iter()
                .filter(|l| l.side == sub_side && l.remaining > 0.0)
                .map(|l| l.remaining)
                .sum();

            // 远程已观测到该腿 → 成交回补完成，后续空仓快照可以作数。
            if remote_side_qty > 1e-12 {
                self.fresh_opens.remove(&slot.sub_positions[idx].id);
            }

            if remote_side_qty <= 1e-12 {
                let prev = slot.sub_positions[idx].state;
                // 实盘成交回补有秒级~分钟级延迟：开仓订单仍在途时，远程暂空
                // 不是最终事实，过早收口会让随后到达的成交变成 legacy 无主仓。
                let open_in_flight = matches!(
                    prev,
                    SubPositionState::PendingOpen | SubPositionState::OpenRequested
                ) && slot
                    .parts
                    .execution
                    .order_in_flight(&slot.sub_positions[idx].id);
                // 成交 ack 先于交易所仓位落库时状态已是 Active，在途保护不生效；
                // 本进程刚成交的腿首拍读到空仓不算事实（实盘观测到 ack 后约
                // 2.7 分钟才查到仓位），等下一拍复核。
                let fresh_fill = prev == SubPositionState::Active
                    && self.fresh_opens.remove(&slot.sub_positions[idx].id);
                if open_in_flight {
                    tracing::debug!(
                        strategy_id = %strategy_id,
                        instrument = %key,
                        prev = ?prev,
                        "reconcile：远程同向空仓但开仓订单在途，暂不收口"
                    );
                } else if fresh_fill {
                    tracing::debug!(
                        strategy_id = %strategy_id,
                        instrument = %key,
                        prev = ?prev,
                        "reconcile：本进程刚成交、首拍远程空仓，等下一拍复核"
                    );
                } else if let Some(next) = advance_sub_state(prev, &SubPositionEvent::RemoteFlat) {
                    let last_bar_at = slot.last_bar_at;
                    // 快照必须在清空 pos 之前取：peak/MFE/ATR/trail 由执行层跟踪。
                    let pos_snapshot = slot.pos_sync.pos.clone();
                    let sub_id = slot.sub_positions[idx].id.clone();
                    let exit_qty = slot.sub_positions[idx].qty;
                    // 外部平仓无成交价：用最近 mark（无 mark 时退回开仓价）。
                    let exit_price = slot
                        .last_mark
                        .unwrap_or(slot.sub_positions[idx].entry_price);
                    let s = &mut slot.sub_positions[idx];
                    s.state = next;
                    s.closed_at = s.closed_at.or(last_bar_at);
                    // 执行层持仓视图同步收敛（终态无持仓）。
                    if slot
                        .pos_sync
                        .pos
                        .as_ref()
                        .is_some_and(|p| p.position_id == s.id)
                    {
                        slot.pos_sync.pos = None;
                    }
                    external_closed.push((
                        slot_idx,
                        sub_id,
                        last_bar_at.unwrap_or_else(Utc::now),
                        exit_price,
                        exit_qty,
                        pos_snapshot,
                    ));
                    tracing::warn!(
                        strategy_id = %strategy_id,
                        instrument = %key,
                        prev = ?prev,
                        "reconcile：远程同向无仓，子仓置 Closed"
                    );
                    report.drifts.push((
                        key,
                        "closed".into(),
                        format!("{prev:?}→Closed（RemoteFlat）"),
                    ));
                    self.subs_changed = true;
                }
            } else if (slot.sub_positions[idx].qty - remote_side_qty).abs() > 1e-9 {
                let prev_qty = slot.sub_positions[idx].qty;
                slot.sub_positions[idx].qty = remote_side_qty;
                if let Some(pos) = slot.pos_sync.pos.as_mut() {
                    pos.total_size = remote_side_qty;
                    pos.remaining_size = remote_side_qty;
                }
                tracing::warn!(
                    strategy_id = %strategy_id,
                    instrument = %key,
                    prev_local = prev_qty,
                    remote = remote_side_qty,
                    "reconcile：数量按远程账本修正（raw 字面量不伪造）"
                );
                report.drifts.push((
                    key,
                    "qty_corrected".into(),
                    format!("local={prev_qty:.6} remote={remote_side_qty:.6}"),
                ));
                self.subs_changed = true;
            }
        }
        for (slot_idx, id, at, price, qty, pos) in external_closed {
            self.record_history_close(
                slot_idx,
                &id,
                at,
                price,
                false,
                qty,
                Some("external_close"),
                pos.as_ref(),
            );
        }
        // 远程平仓事实可能让最后一个子仓落地 Closed → 总仓同步收口。
        let settle_at = self
            .slots
            .iter()
            .filter_map(|s| s.last_bar_at)
            .max()
            .unwrap_or_else(Utc::now);
        let settled = self.settle_masters(settle_at);
        if self.subs_changed || settled {
            self.persist_round();
        }
        Ok(report)
    }

    // ─── 轮次编排 ─────────────────────────────────────────────────────────

    fn on_bar(&mut self, slot_idx: usize, bar: Bar) -> Result<Option<RoundReport>> {
        if !bar.is_final() {
            self.summary.provisional_bars += 1;
            return Ok(None);
        }
        let ev_at = bar.open_time;
        // 未来函数防御：换时间戳先 flush 上一 pending 决策轮——此刻序列只闭合到
        // 该轮 bar；若先喂当前 bar 再 flush，`t_i` 的决策会看见 `t_{i+1}`（G5 parity 实测）。
        let mut prev: Option<RoundReport> = None;
        if let Some(p) = &self.pending_round {
            if p.last_bar_at != ev_at {
                prev = Some(self.flush_round()?);
            }
        }
        let newly = {
            let slot = self.slots.get_mut(slot_idx).ok_or_else(|| {
                SpiError::Execution(format!("strategy runner: 无效 slot 下标 {slot_idx}"))
            })?;
            self.summary.bars_processed += 1;
            slot.parts.execution.on_bar(&bar);
            slot.last_bar_at = Some(bar.open_time);
            slot.last_mark = Some(bar.close);
            let session = slot.parts.session.clone();
            let day = session.trading_day_of(bar.open_time);
            handle_slot_session(slot, &bar, day)?;
            aggregate_and_gate_slot(slot, &bar, day, session.as_ref())?
        };
        let base = self.base_tf;
        let decision = newly.contains(&base);

        if decision {
            let round_at = self.round_at_for(slot_idx, ev_at);
            match &mut self.pending_round {
                Some(p) if p.at == round_at => {
                    if !p.slots.contains(&slot_idx) {
                        p.slots.push(slot_idx);
                    }
                    for tf in &newly {
                        if !p.newly.contains(tf) {
                            p.newly.push(*tf);
                        }
                    }
                    p.newly.sort();
                }
                _ => {
                    self.pending_round = Some(PendingRound {
                        at: round_at,
                        last_bar_at: ev_at,
                        slots: vec![slot_idx],
                        newly,
                    });
                }
            }
            return Ok(prev);
        }

        // 非决策 bar：只做成交同步 + 盯市/净值；无同刻 pending 时返回非决策报告。
        let fills = self.sync_slot_fills(slot_idx)?;
        if let Some(r) = prev {
            return Ok(Some(r));
        }
        if self
            .pending_round
            .as_ref()
            .is_some_and(|p| p.last_bar_at == ev_at)
        {
            return Ok(None);
        }
        let report = self.build_non_decision_report(ev_at, fills)?;
        Ok(Some(report))
    }

    /// 本轮 base TF 闭合的基准：该 slot 最近一根已闭合 base bar 的 `open_time`。
    fn round_at_for(&self, slot_idx: usize, ev_at: DateTime<Utc>) -> DateTime<Utc> {
        self.slots[slot_idx]
            .state
            .closed(self.base_tf)
            .iter()
            .rev()
            .find(|b| b.open_time <= ev_at)
            .map(|b| b.open_time)
            .unwrap_or(ev_at)
    }

    /// 决策轮 flush（顺序固定：① 成交落账 → ② L3 占位 → ③ L2 占位 →
    /// ④ quorum + 策略 → ⑤ L1 兜底 → dispatch）。
    fn flush_round(&mut self) -> Result<RoundReport> {
        let round = self.pending_round.take().ok_or_else(|| {
            SpiError::Execution("strategy runner: flush_round 无 pending 轮次".into())
        })?;
        let at = round.at;

        // ① 成交同步（含子仓位落账）+ book 镜像。
        let mut report_fills = Vec::new();
        for i in 0..self.slots.len() {
            report_fills.extend(self.sync_slot_fills(i)?);
        }

        // 策略前刷新各 slot 的 CandleSeries 快照。
        for slot in self.slots.iter_mut() {
            slot.candle_cache.refresh(&slot.state);
        }
        let mut snapshots = self.build_snapshots(&round);
        let no_trade = snapshots.iter().any(|s| s.no_trade);
        let n = self.slots.len();
        let non_stale = round.slots.len();
        let quorum = (4 * n).div_ceil(5);
        let quorum_ok = non_stale >= quorum;

        // 净值先刷新（策略与 PortfolioContext 用的 nav）。
        let nav = self.current_nav();
        self.update_portfolio(nav);

        // 轮换计数：每轮 base TF 闭合 +1。
        self.bars_since_rebalance += 1;
        let rebalance_due = self.bars_since_rebalance >= self.rebalance_every();

        let mut actions_out = 0u64;
        let mut rejected_out = 0u64;
        let mut traded = false;

        // ② L3 portfolio_exit（§7.3，先于 ③/④；度量对象 = MasterPosition）。
        // 口径：pnl% = unrealized / 成本名义 × 100（N=1 与旧 PositionState trail 同分母）。
        let mut l3_block = false;
        // 逐标的独立总仓：L3 以单标的为组合边界，某个标的触发只平该标的。
        let mut blocked_scopes: std::collections::BTreeSet<String> = Default::default();
        if !self.config.merge_positions {
            for idx in 0..self.slots.len() {
                let scope = self.slots[idx].instrument_key();
                let price = snapshots
                    .iter()
                    .find(|sn| sn.instrument.namespaced_key() == scope)
                    .map(|sn| sn.bar.close)
                    .or(self.slots[idx].last_mark)
                    .unwrap_or(0.0);
                let (actions, rejected, blocked) =
                    self.evaluate_scope_l3(&scope, price, &snapshots)?;
                actions_out += actions;
                rejected_out += rejected;
                if blocked {
                    blocked_scopes.insert(scope);
                }
            }
        }
        if self.config.merge_positions {
            let master = self.master_position();
            let pnl_pct = if master.gross_notional > 1e-12 {
                master.unrealized_pnl / master.gross_notional * 100.0
            } else {
                0.0
            };
            if master.unrealized_pnl > self.portfolio.peak_unrealized_pnl {
                self.portfolio.peak_unrealized_pnl = master.unrealized_pnl;
            }
            let mut fired = false;
            if let Some(tr) = &self.config.portfolio_exit.trailing {
                if pnl_pct >= tr.arm_pnl_pct {
                    self.portfolio.trail_armed = true;
                }
                if self.portfolio.trail_armed
                    && self.portfolio.peak_unrealized_pnl > 0.0
                    && master.unrealized_pnl
                        <= self.portfolio.peak_unrealized_pnl * (1.0 - tr.giveback_pct / 100.0)
                {
                    fired = true;
                }
            }
            if let Some(sl) = self.config.portfolio_exit.stop_loss_pct {
                if pnl_pct <= -sl {
                    fired = true;
                }
            }
            if self.phase != PortfolioPhase::Paused {
                if self.phase == PortfolioPhase::Liquidating {
                    l3_block = true; // 清算中：抑制 L2/策略/L1，仅重试补平
                } else if fired {
                    self.phase = PortfolioPhase::Liquidating;
                    l3_block = true;
                    // L3 抢跑：放弃排队中的加仓（本轮只清仓）。
                    self.pending_adds.clear();
                    self.reduction_ids.clear();
                    tracing::warn!(
                        strategy_id = %self.strategy_id,
                        pnl_pct,
                        "L3 portfolio_exit 触发 → 全平（Liquidating）"
                    );
                }
                if self.phase == PortfolioPhase::Liquidating {
                    let mut prices = BTreeMap::new();
                    for slot in &self.slots {
                        let key = slot.instrument_key();
                        let price = snapshots
                            .iter()
                            .find(|sn| sn.instrument.namespaced_key() == key)
                            .map(|sn| sn.bar.close)
                            .or(slot.last_mark)
                            .unwrap_or(0.0);
                        prices.insert(key, price);
                    }
                    // 总仓是平仓的单一入口：先置 Closing，再逐子仓发平仓单。
                    let master_ids: Vec<String> = self
                        .masters
                        .values()
                        .filter(|m| m.state != MasterPositionState::Closed)
                        .map(|m| m.master_id.clone())
                        .collect();
                    for master_id in &master_ids {
                        let (actions, rejected) = self.close_master(
                            master_id,
                            &prices,
                            "portfolio_exit: CloseAll",
                            false,
                            SubIntent::RiskCloseAll,
                        )?;
                        actions_out += actions;
                        rejected_out += rejected;
                    }
                    let any_live_sub = self
                        .slots
                        .iter()
                        .flat_map(|s| s.sub_positions.iter())
                        .any(|s| s.state != SubPositionState::Closed);
                    let any_open_master = self
                        .masters
                        .values()
                        .any(|m| m.state != MasterPositionState::Closed);
                    if !any_live_sub && !any_open_master {
                        // 确认清零 → Paused；跟踪基准复位（新一轮从干净状态起算）。
                        self.phase = PortfolioPhase::Paused;
                        self.portfolio.peak_unrealized_pnl = 0.0;
                        self.portfolio.trail_armed = false;
                        tracing::info!(
                            strategy_id = %self.strategy_id,
                            "Liquidating 清零 → Paused（恢复交易须 resume/重启放行）"
                        );
                        l3_block = true; // 本轮起策略持续被 is_paused 门控
                    }
                }
            }
        }

        // ③ L2 标的级 exit 规则（§7.2）：quorum 达标、!no_trade 且该 slot 本轮
        // base TF 闭合时评估；触发 → 代发 Close(intent=ExitRule) 并登记抑制集
        // （本轮该标的策略动作全部丢弃；无仓时 close 自然 no-op，但抑制防重开）。
        let mut exit_suppressed: std::collections::BTreeSet<String> = Default::default();
        if quorum_ok && !no_trade && !l3_block && self.can_trade() {
            let mut hits: Vec<usize> = Vec::new();
            for (si, snap) in snapshots.iter().enumerate() {
                let key = snap.instrument.namespaced_key();
                // 该标的的 L3 已暂停/清算中：不再评估 L2，避免重复发平仓单。
                if blocked_scopes.contains(&key) {
                    continue;
                }
                let Some(idx) = self.slots.iter().position(|s| s.instrument_key() == key) else {
                    continue;
                };
                if !round.slots.contains(&idx) {
                    continue;
                }
                let rules = effective_rules(&self.config.exit, &snap.instrument.symbol);
                if rules.is_empty() {
                    continue;
                }
                if crate::strategy::evaluator::exit_triggered(rules, &snap.features).is_some() {
                    hits.push(si);
                }
            }
            for si in hits {
                let reason = crate::strategy::evaluator::exit_triggered(
                    effective_rules(&self.config.exit, &snapshots[si].instrument.symbol),
                    &snapshots[si].features,
                )
                .expect("刚判定过必命中");
                snapshots[si].exit_triggered = Some(reason.clone());
                let key = snapshots[si].instrument.namespaced_key();
                let price = snapshots[si].bar.close;
                let idx = self
                    .slots
                    .iter()
                    .position(|s| s.instrument_key() == key)
                    .expect("snapshot 来自 slot");
                exit_suppressed.insert(key.clone());
                let master_id = self.master_id_for_slot(idx);
                let ids = StrategyIds {
                    strategy_type: &self.strategy_type,
                    strategy_id: &self.strategy_id,
                    master_id: &master_id,
                    rebalance_epoch: self.portfolio.rebalance_epoch,
                };
                let slot = &mut self.slots[idx];
                let out = dispatch::close_position(
                    slot,
                    &ids,
                    price,
                    &reason,
                    false,
                    SubIntent::ExitRule,
                )?;
                actions_out += out.actions;
                rejected_out += out.rejected;
                if out.actions > 0 {
                    self.subs_changed = true;
                }
            }
        }

        let mut actions: Vec<InstrumentAction> = Vec::new();
        let mut strategy_called = false;
        if quorum_ok {
            if !no_trade && self.can_trade() && !l3_block {
                strategy_called = true;
                let port = self.build_portfolio_ctx(&snapshots);
                // ④ 策略决策：panic 只暂停本 RunnerGroup，事件循环继续服务其他组。
                // AssertUnwindSafe 的依据：panic 后本实例内存（含 SubPosition 中间态）
                // 一律视为可疑，恢复必须走 `resume_from_store` 重建账本后放行。
                // 逐标的独立总仓按 slot 逐个调用，策略只看到本标的的快照、NAV 和总仓。
                let called = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    if self.config.merge_positions {
                        self.strategy
                            .on_cross_section_bars(&round.newly, &snapshots, &port)
                    } else {
                        let mut out = Vec::new();
                        for snap in &snapshots {
                            let key = snap.instrument.namespaced_key();
                            if blocked_scopes.contains(&key) {
                                continue;
                            }
                            // 快照可能跳过无 base bar 的 slot，必须按标的反查 slot 序号。
                            let Some(idx) =
                                self.slots.iter().position(|s| s.instrument_key() == key)
                            else {
                                continue;
                            };
                            let ctx =
                                self.build_portfolio_ctx_for_slot(idx, std::slice::from_ref(snap));
                            out.extend(self.strategy.on_cross_section_bars(
                                &round.newly,
                                std::slice::from_ref(snap),
                                &ctx,
                            ));
                        }
                        out
                    }
                }));
                match called {
                    Ok(a) => {
                        actions = a;
                        actions.sort_by_key(|a| a.instrument_key());
                    }
                    Err(_) => {
                        self.phase = PortfolioPhase::Paused;
                        self.paused_after_panic = true;
                        tracing::error!(
                            strategy_id = %self.strategy_id,
                            "策略 panic，已暂停该 RunnerGroup（不发新动作；恢复须 resume_from_store 重建账本）"
                        );
                    }
                }
            }
        } else {
            self.summary.bars_skipped_stale += 1;
        }

        // 决策轮闸门全量可见：策略没被调用 / 调仓窗口未到 / 停开窗，都是
        // 实盘“为何不下单”的常见原因，聚合成一行避免逐条翻日志。
        tracing::debug!(
            strategy_id = %self.strategy_id,
            at = %at,
            strategy_called,
            quorum_ok,
            non_stale,
            slots = n,
            no_trade,
            l3_block,
            can_trade = self.can_trade(),
            base_tf = %self.base_tf,
            base_bars = self.slots[0].state.closed(self.base_tf).len(),
            rebalance_due,
            bars_since_rebalance = self.bars_since_rebalance,
            rebalance_every = self.rebalance_every(),
            actions = actions.len(),
            "决策轮闸门与调仓窗口"
        );

        // 减仓腿在上一 tick 全部确认 → 补派 Phase 2（在 L3 之后：清算抢跑时
        // pending 已被清空，不会先加后平）。
        self.try_phase2(at, &mut actions_out, &mut rejected_out, &mut traded)?;

        // L2 抑制（§7.2）：触发标的本轮全部策略动作丢弃并计 rejected——
        // exit 已代发 Close，再放行 Open/Adjust 就是同轮双重决策。
        if !exit_suppressed.is_empty() {
            let mut kept: Vec<InstrumentAction> = Vec::with_capacity(actions.len());
            for a in actions.drain(..) {
                match a.instrument_key() {
                    Some(k) if exit_suppressed.contains(&k) => {
                        rejected_out += 1;
                    }
                    _ => kept.push(a),
                }
            }
            actions = kept;
        }

        // ④b 两阶段再平衡（§9.2，rebalance 轮）：
        // Phase 1 先派减仓并登记在途腿；Phase 2 加仓排队，等减仓腿全部
        // 成交确认（reduction_ids 清空）后按 epoch+1 派发。无减仓时同轮直派。
        let rebalance_split = self.config.merge_positions
            && rebalance_due
            && self.reduction_ids.is_empty()
            && self.pending_adds.is_empty()
            && !l3_block;
        if rebalance_split && !actions.is_empty() {
            let mut reductions: Vec<InstrumentAction> = Vec::new();
            let mut adds: Vec<(InstrumentAction, bool)> = Vec::new();
            for a in actions.into_iter() {
                let key = a.instrument_key();
                let cur = key
                    .as_deref()
                    .and_then(|k| {
                        self.slots
                            .iter()
                            .find(|s| s.instrument_key() == k)
                            .and_then(|s| {
                                s.sub_positions
                                    .iter()
                                    .find(|p| p.state != SubPositionState::Closed)
                            })
                    })
                    .map(|p| match p.side {
                        Side::Long => p.qty,
                        Side::Short => -p.qty,
                    })
                    .unwrap_or(0.0);
                let is_add = match &a {
                    InstrumentAction::Open { .. } => true,
                    InstrumentAction::Adjust { target_qty, .. } => {
                        *target_qty * cur >= 0.0 && target_qty.abs() > cur.abs() + 1e-12
                    }
                    _ => false,
                };
                if is_add {
                    // 进入 pending 前复核 entry 硬门与 L2 抑制（T18 裁决：
                    // 存 action 时刻的 eligible 供 Phase 2 使用）。
                    let eligible = key
                        .as_deref()
                        .and_then(|k| {
                            snapshots
                                .iter()
                                .find(|sn| sn.instrument.namespaced_key() == k)
                        })
                        .is_some_and(|sn| sn.entry_eligible && sn.exit_triggered.is_none());
                    if !eligible {
                        rejected_out += 1;
                    } else {
                        adds.push((a, eligible));
                    }
                } else {
                    reductions.push(a);
                }
            }
            let mut accepted: Vec<String> = Vec::new();
            self.dispatch_actions(
                &round,
                &snapshots,
                &reductions,
                &mut actions_out,
                &mut rejected_out,
                &mut traded,
                &mut accepted,
            )?;
            self.reduction_ids.extend(accepted);
            self.pending_adds = adds;
            self.pending_adds.sort_by_key(|(a, _)| a.instrument_key());
            // L1 静默判定基于本轮已派发（减仓）动作集合。
            actions = reductions;
            if self.reduction_ids.is_empty() {
                // Phase 1 为空 → Phase 2 当轮直接完成（try_phase2 内推进 epoch）。
                self.try_phase2(at, &mut actions_out, &mut rejected_out, &mut traded)?;
            }
        } else {
            self.dispatch_actions(
                &round,
                &snapshots,
                &actions,
                &mut actions_out,
                &mut rejected_out,
                &mut traded,
                &mut Vec::new(),
            )?;
        }
        if self.can_trade() && !no_trade && !l3_block {
            self.dispatch_l1_fallback(&snapshots, &actions, &mut actions_out, &mut rejected_out)?;
        }

        // 轮换 epoch 统一由 try_phase2 在 Phase 2 完成时推进（两阶段裁决）。
        let _ = traded;

        // 派发后收口：最后子仓已 Closed、或开仓被拒留下空总仓。
        self.settle_masters(at);

        let final_nav = self.current_nav();
        self.update_portfolio(final_nav);
        let cash = self.current_cash();

        self.persist_round();
        self.subs_changed = false;

        self.summary.actions += actions_out;
        self.summary.rejected_actions += rejected_out;
        let report = RoundReport {
            at,
            nav: self.portfolio.nav,
            cash,
            fills: report_fills,
            actions: actions_out,
            rejected: rejected_out,
            decision_round: true,
        };
        self.last_round = Some(report.clone());
        self.last_snapshots = snapshots;
        Ok(report)
    }

    fn build_non_decision_report(
        &mut self,
        at: DateTime<Utc>,
        fills: Vec<Fill>,
    ) -> Result<RoundReport> {
        let nav = self.current_nav();
        self.update_portfolio(nav);
        let cash = self.current_cash();
        // 非决策 tick 也推进两阶段状态机：减仓腿全部成交则立即派发 Phase 2。
        let mut extra_actions = 0u64;
        let mut extra_rejected = 0u64;
        let mut t = false;
        self.try_phase2(at, &mut extra_actions, &mut extra_rejected, &mut t)?;
        // Phase 2 的开仓被拒可能留下空总仓，非决策 tick 也要收口。
        self.settle_masters(at);
        self.summary.actions += extra_actions;
        self.summary.rejected_actions += extra_rejected;
        self.persist_round();
        self.subs_changed = false;
        let report = RoundReport {
            at,
            nav,
            cash,
            fills,
            actions: extra_actions,
            rejected: extra_rejected,
            decision_round: false,
        };
        self.last_round = Some(report.clone());
        Ok(report)
    }

    // ─── snapshots / portfolio ctx ────────────────────────────────────────

    fn build_snapshots(&self, round: &PendingRound) -> Vec<InstrumentSnapshot> {
        let base = self.base_tf;
        let mut snaps = Vec::with_capacity(self.slots.len());
        for (i, slot) in self.slots.iter().enumerate() {
            let decision = round.slots.contains(&i);
            let base_bars = slot.state.closed(base);
            let base_bar = if decision {
                base_bars
                    .iter()
                    .rev()
                    .find(|b| b.open_time == round.at)
                    .or_else(|| base_bars.back())
            } else {
                base_bars.back()
            };
            let Some(base_bar) = base_bar else {
                // 该 slot 从未闭合过 base：无快照可建（本轮恒 stale）。
                continue;
            };
            let session = slot.parts.session.as_ref();
            let day = session.trading_day_of(base_bar.open_time);
            let no_trade = session.is_no_trade_window(base_bar.open_time);
            let entry_confirm = session.is_entry_confirm_time(base_bar.open_time);
            let stale = !decision;
            let features = slot.feature_plan.compute(&slot.state);
            let rules = effective_rules(&self.config.entry, &slot.parts.instrument.symbol);
            let (entry_eligible, entry_reason) = if stale {
                (false, Some("stale".into()))
            } else {
                crate::strategy::evaluator::entry_eligible(rules, &features)
            };
            snaps.push(InstrumentSnapshot {
                instrument: slot.parts.instrument.clone(),
                bar: base_bar.clone(),
                position: slot.pos_sync.position_view(),
                stale,
                features,
                entry_eligible,
                entry_reason,
                exit_triggered: None,
                trading_day: day,
                no_trade,
                entry_confirm,
                signal_resume: slot.last_signal_resume,
                cooldown: slot.pos_sync.cooldown.clone(),
                session_open_price: slot.risk_guard.session_open_price,
                closed: Some(slot.candle_cache.clone()),
            });
        }
        snaps
    }

    fn build_portfolio_ctx(&self, snapshots: &[InstrumentSnapshot]) -> PortfolioContext {
        let view = self.master_position();
        let portfolio_pnl_pct = if view.gross_notional > 1e-12 {
            view.unrealized_pnl / view.gross_notional * 100.0
        } else {
            0.0
        };
        // 优先活动总仓指针；指针缺失时退到任一非 Closed 总仓（恢复/迁移兜底）。
        let master = self
            .portfolio
            .active_master_id
            .as_deref()
            .and_then(|id| self.masters.get(id))
            .filter(|m| m.state != MasterPositionState::Closed)
            .or_else(|| {
                self.masters
                    .values()
                    .find(|m| m.state != MasterPositionState::Closed)
            })
            .map(|m| MasterContext {
                master_id: m.master_id.clone(),
                state: m.state,
                view: view.clone(),
            });
        PortfolioContext {
            strategy_id: self.strategy_id.clone(),
            nav: self.portfolio.nav,
            drawdown: self.portfolio.drawdown,
            portfolio_pnl_pct,
            sub_positions: self.all_sub_positions(),
            master,
            multipliers: self.multiplier_map(),
            max_total_notional: self.portfolio.max_total_notional,
            rebalance_due: self.bars_since_rebalance >= self.rebalance_every()
                && self.reduction_ids.is_empty()
                && self.pending_adds.is_empty(),
            no_trade: snapshots.iter().any(|s| s.no_trade),
        }
    }

    /// 逐标的独立总仓的上下文：NAV、总仓视图、子仓集合、容量都只属于
    /// 当前 slot；合并总仓仍复用策略级上下文。
    fn build_portfolio_ctx_for_slot(
        &self,
        idx: usize,
        snapshots: &[InstrumentSnapshot],
    ) -> PortfolioContext {
        if self.config.merge_positions {
            return self.build_portfolio_ctx(snapshots);
        }
        let view = self.master_position_for_slot(idx);
        let portfolio_pnl_pct = if view.gross_notional > 1e-12 {
            view.unrealized_pnl / view.gross_notional * 100.0
        } else {
            0.0
        };
        let master_id = self.master_id_for_slot(idx);
        let master = self
            .masters
            .get(&master_id)
            .filter(|m| m.state != MasterPositionState::Closed)
            .map(|m| MasterContext {
                master_id: m.master_id.clone(),
                state: m.state,
                view,
            });
        PortfolioContext {
            strategy_id: self.strategy_id.clone(),
            nav: self.current_nav_for_slot(idx),
            drawdown: self.portfolio.drawdown,
            portfolio_pnl_pct,
            sub_positions: self.slots[idx].sub_positions.clone(),
            master,
            multipliers: self.multiplier_map(),
            max_total_notional: self.portfolio.max_total_notional,
            // 逐标的独立总仓没有跨标的调仓窗口，每轮都按本标的判定。
            rebalance_due: true,
            no_trade: snapshots.first().is_some_and(|s| s.no_trade),
        }
    }

    // ─── dispatch ─────────────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)] // 编排面参数即语义（snapshots/门控/计数/在途腿收集）
    fn dispatch_actions(
        &mut self,
        round: &PendingRound,
        snapshots: &[InstrumentSnapshot],
        actions: &[InstrumentAction],
        actions_out: &mut u64,
        rejected_out: &mut u64,
        traded_out: &mut bool,
        accepted_out: &mut Vec<String>,
    ) -> Result<()> {
        for action in actions {
            // 组合级平仓先于标的动作派发：总仓 Closing 后本轮任何 Open/加仓
            // 都会被 dispatch_sub 兜底拦截，语义即「平仓优先」。
            if let InstrumentAction::CloseMaster {
                reason,
                is_stop_loss,
            } = action
            {
                let prices: BTreeMap<String, f64> = snapshots
                    .iter()
                    .map(|s| (s.instrument.namespaced_key(), s.bar.close))
                    .collect();
                let master_ids: Vec<String> = self
                    .masters
                    .values()
                    .filter(|m| m.state != MasterPositionState::Closed)
                    .map(|m| m.master_id.clone())
                    .collect();
                for master_id in master_ids {
                    let (closed, rejected) = self.close_master(
                        &master_id,
                        &prices,
                        reason,
                        *is_stop_loss,
                        SubIntent::RiskCloseAll,
                    )?;
                    *actions_out += closed;
                    *rejected_out += rejected;
                    if closed > 0 {
                        self.subs_changed = true;
                        *traded_out = true;
                    }
                }
                continue;
            }
            let Some(key) = action.instrument_key() else {
                continue; // Hold
            };
            // 排序后 action 与 slot 一一对应；找不到 = 未知标的 → 软拒。
            let Some(idx) = self.slots.iter().position(|s| s.instrument_key() == key) else {
                *rejected_out += 1;
                continue;
            };
            let snap = snapshots
                .iter()
                .find(|s| s.instrument.namespaced_key() == key);
            let eligible = snap.map(|s| s.entry_eligible).unwrap_or(false);
            // entry 硬门：Open 与「增大持仓幅值的 Adjust」在 entry 不达标时丢弃。
            // 用 |target| > |current|（幅值增大 = 风险加仓，Short 方向同样拦截：
            // −2→−3 加仓、−2→−1 减仓；空仓时 target≠0 恒算加仓）。
            let current = current_signed(&self.slots[idx]);
            let gated = match action {
                InstrumentAction::Open { .. } => true,
                InstrumentAction::Adjust { target_qty, .. } => {
                    target_qty.abs() > current.abs() + 1e-12
                }
                _ => false,
            };
            if gated && !eligible {
                *rejected_out += 1;
                continue;
            }
            let Some(snap) = snap else {
                *rejected_out += 1;
                continue;
            };
            self.dispatch_sub(
                idx,
                round.at,
                snap.bar.close,
                action,
                actions_out,
                rejected_out,
                traded_out,
                accepted_out,
                None,
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_sub(
        &mut self,
        idx: usize,
        at: DateTime<Utc>,
        base_close: f64,
        action: &InstrumentAction,
        actions_out: &mut u64,
        rejected_out: &mut u64,
        traded_out: &mut bool,
        accepted_out: &mut Vec<String>,
        epoch_override: Option<u64>,
    ) -> Result<()> {
        // 只有确实新建子仓的动作才需要总仓：Open 恒需要；Adjust 仅在
        // 空仓且目标非零时走 open_position，其余路径不创建空总仓。
        let needs_master = match action {
            InstrumentAction::Open { .. } => true,
            InstrumentAction::Adjust { target_qty, .. } => {
                let has_live = self.slots[idx]
                    .sub_positions
                    .iter()
                    .any(|s| s.state != SubPositionState::Closed);
                !has_live && target_qty.abs() > 1e-12
            }
            _ => false,
        };
        let scope = self.position_scope(idx);
        if needs_master && scope.as_deref().is_some_and(|s| self.scope_is_paused(s)) {
            tracing::debug!(
                instrument = %self.slots[idx].parts.instrument,
                "拒开仓：该标的 L3 已触发并暂停交易"
            );
            *rejected_out += 1;
            return Ok(());
        }
        let closing_master = if self.config.merge_positions {
            self.portfolio
                .active_master_id
                .clone()
                .filter(|id| self.master_is_closing(id))
        } else {
            self.masters
                .values()
                .find(|m| {
                    m.scope.as_deref() == scope.as_deref()
                        && m.state == MasterPositionState::Closing
                })
                .map(|m| m.master_id.clone())
        };
        let master_id = if needs_master {
            // Closing 总仓不得接收新子仓；策略/L1 若绕过 can_trade 也在这里兜底。
            if let Some(id) = closing_master {
                tracing::debug!(
                    instrument = %self.slots[idx].parts.instrument,
                    master_id = %id,
                    "拒开仓：作用域总仓处于 Closing，本轮不接新子仓"
                );
                *rejected_out += 1;
                return Ok(());
            }
            self.ensure_active_master(at, scope.as_deref())
        } else {
            self.master_id_for_slot(idx)
        };
        let ids = StrategyIds {
            strategy_type: &self.strategy_type,
            strategy_id: &self.strategy_id,
            master_id: &master_id,
            rebalance_epoch: epoch_override.unwrap_or(self.portfolio.rebalance_epoch),
        };
        match action {
            InstrumentAction::Open {
                side,
                price,
                stop_price,
                reason,
                ..
            } => {
                let nav = self.dispatch_nav(idx);
                let mult = self.multiplier_for(idx);
                let unit = (*price * mult).max(1e-12);
                // 单仓名义取 per_position_pct 与子仓硬上限中的更小者。
                let pct = self
                    .config
                    .sizing
                    .max_position_pct
                    .map_or(self.config.sizing.per_position_pct, |cap| {
                        self.config.sizing.per_position_pct.min(cap)
                    });
                let mut qty = nav * pct / unit;
                // 按 max_total_notional 剩余容量裁剪：已满仓/超仓 → 直接拒，
                // 绝不超发（容量以「非 Closed 子仓位的成本名义 = gross」计）。
                let master = self.master_position_for_slot(idx);
                let remaining = self.portfolio.max_total_notional * nav - master.gross_notional;
                match self.slots[idx].parts.execution.venue_order_notional() {
                    Some(venue_notional) => {
                        // live 场所按固定名义下单（忽略本地 qty），影子 qty 必须
                        // 按场所名义生成，容量按真实名义校验。单仓预算只在启动期
                        // 按 initial_cash_total 校验：NAV 随盈亏小幅回落时再比对
                        // 当前预算会每轮拒单，而场所无法缩小固定名义。
                        if remaining + 1e-9 < venue_notional {
                            tracing::debug!(
                                instrument = %self.slots[idx].parts.instrument,
                                nav,
                                remaining,
                                venue_notional,
                                "拒开仓：总仓剩余容量不足场所固定单笔名义"
                            );
                            *rejected_out += 1;
                            return Ok(());
                        }
                        qty = venue_notional / unit;
                    }
                    None => {
                        if remaining <= 0.0 {
                            tracing::debug!(
                                instrument = %self.slots[idx].parts.instrument,
                                nav,
                                remaining,
                                "拒开仓：总仓容量已满（remaining<=0）"
                            );
                            *rejected_out += 1;
                            return Ok(());
                        }
                        qty = qty.min(remaining / unit);
                        if qty <= 1e-12 {
                            tracing::debug!(
                                instrument = %self.slots[idx].parts.instrument,
                                nav,
                                remaining,
                                qty,
                                "拒开仓：容量裁剪后数量为 0"
                            );
                            *rejected_out += 1;
                            return Ok(());
                        }
                    }
                }
                tracing::debug!(
                    instrument = %self.slots[idx].parts.instrument,
                    nav,
                    pct,
                    unit,
                    qty,
                    remaining,
                    "开仓数量计算（nav*pct/unit，按总仓剩余容量裁剪）"
                );
                let slot = &mut self.slots[idx];
                let out = dispatch::open_position(
                    slot,
                    &ids,
                    *side,
                    *price,
                    qty,
                    *stop_price,
                    at,
                    reason,
                    SubIntent::Open,
                )?;
                *actions_out += out.actions;
                *rejected_out += out.rejected;
                accepted_out.extend(out.signal_ids);
                if out.actions > 0 {
                    self.subs_changed = true;
                    *traded_out = true;
                }
                Ok(())
            }
            InstrumentAction::Close {
                price,
                reason,
                is_stop_loss,
                ..
            } => {
                // 只作用于 Active 子仓位；PendingOpen/OpenRequested 拒。
                if sub_state_not_active(&self.slots[idx]) {
                    tracing::debug!(
                        instrument = %self.slots[idx].parts.instrument,
                        "拒平仓：子仓非 Active（在途/未成交）"
                    );
                    *rejected_out += 1;
                    return Ok(());
                }
                let slot = &mut self.slots[idx];
                let out = dispatch::close_position(
                    slot,
                    &ids,
                    *price,
                    reason,
                    *is_stop_loss,
                    SubIntent::ExitRule,
                )?;
                *actions_out += out.actions;
                *rejected_out += out.rejected;
                accepted_out.extend(out.signal_ids);
                if out.actions > 0 {
                    self.subs_changed = true;
                    *traded_out = true;
                }
                Ok(())
            }
            InstrumentAction::Adjust {
                target_qty, reason, ..
            } => {
                // 在途子仓（PendingOpen/OpenRequested/CloseRequested）本轮目标
                // 建立在未落定的仓位上，无法安全下单；静默延后到成交回补后
                // 下轮重算即可，这是等待而非拒单。
                if sub_state_not_active(&self.slots[idx]) {
                    tracing::debug!(
                        instrument = %self.slots[idx].parts.instrument,
                        "调仓延后：子仓在途（PendingOpen/OpenRequested/CloseRequested），待成交后下轮重算"
                    );
                    return Ok(());
                }
                // 杠杆上限同样约束调仓加仓（成本名义口径，与 Open 一致）。
                let mut target = *target_qty;
                let cur_signed = self.slots[idx]
                    .sub_positions
                    .iter()
                    .find(|p| p.state != SubPositionState::Closed)
                    .map(|p| match p.side {
                        Side::Long => p.qty,
                        Side::Short => -p.qty,
                    })
                    .unwrap_or(0.0);
                let is_add = target.abs() > cur_signed.abs() + 1e-12 && target * cur_signed >= 0.0
                    || (cur_signed.abs() <= 1e-12 && target != 0.0);
                if is_add {
                    let mult = self.multiplier_for(idx);
                    let nav = self.dispatch_nav(idx);
                    let unit = base_close.max(1e-12) * mult;
                    // 子仓硬上限：已超限仓位只禁止继续加仓，不强制减仓。
                    let allowed_abs = self.max_sub_qty(nav, unit).max(cur_signed.abs());
                    if target.abs() > allowed_abs {
                        target = target.signum() * allowed_abs;
                    }
                    let master = self.master_position_for_slot(idx);
                    let remaining = self.portfolio.max_total_notional * nav - master.gross_notional;
                    match self.slots[idx].parts.execution.venue_order_notional() {
                        Some(venue_notional) => {
                            let pct = self
                                .config
                                .sizing
                                .max_position_pct
                                .map_or(self.config.sizing.per_position_pct, |cap| {
                                    self.config.sizing.per_position_pct.min(cap)
                                });
                            if cur_signed.abs() <= 1e-12 {
                                // 空仓 Adjust 等价 Open：按场所真实名义建影子行；
                                // 单仓预算由启动期校验，运行期只检查总仓剩余容量。
                                if remaining + 1e-9 < venue_notional {
                                    tracing::debug!(
                                        instrument = %self.slots[idx].parts.instrument,
                                        nav,
                                        remaining,
                                        venue_notional,
                                        "拒调仓开仓：总仓剩余容量不足场所固定名义"
                                    );
                                    *rejected_out += 1;
                                    return Ok(());
                                }
                                target = target.signum() * (venue_notional / unit);
                            } else {
                                let want_add_cost =
                                    (target.abs() - cur_signed.abs()).max(0.0) * unit;
                                // 场所只能按固定名义加仓：差额小于单笔名义会越过目标，
                                // 容量/单仓预算不足也无法执行；三者都视为已达标静默跳过。
                                if want_add_cost + 1e-9 < venue_notional
                                    || remaining + 1e-9 < venue_notional
                                    || cur_signed.abs() * unit + venue_notional > nav * pct + 1e-9
                                {
                                    return Ok(());
                                }
                            }
                        }
                        None => {
                            if remaining <= 0.0 {
                                tracing::debug!(
                                    instrument = %self.slots[idx].parts.instrument,
                                    nav,
                                    remaining,
                                    "拒调仓加仓：总仓容量已满（remaining<=0）"
                                );
                                *rejected_out += 1;
                                return Ok(());
                            }
                            let want_add_cost = (target.abs() - cur_signed.abs()).max(0.0) * unit;
                            if want_add_cost > remaining {
                                let max_add_qty = remaining / unit;
                                target = cur_signed + max_add_qty * target.signum();
                            }
                        }
                    }
                }
                tracing::debug!(
                    instrument = %self.slots[idx].parts.instrument,
                    target,
                    cur_signed,
                    is_add,
                    base_close,
                    "调仓目标数量计算（策略 target_qty → 容量/杠杆裁剪后）"
                );
                let slot = &mut self.slots[idx];
                let out = dispatch::adjust_position(
                    slot,
                    &ids,
                    target,
                    base_close,
                    at,
                    reason,
                    SubIntent::RebalanceReduce,
                )?;
                *actions_out += out.actions;
                *rejected_out += out.rejected;
                accepted_out.extend(out.signal_ids);
                if out.actions > 0 {
                    *traded_out = true;
                }
                Ok(())
            }
            InstrumentAction::UpdateStop { stop_price, .. } => {
                let slot = &mut self.slots[idx];
                if let Some(p) = slot.pos_sync.pos.as_mut() {
                    p.stop_price = *stop_price;
                }
                if let Some(s) = slot.sub_positions.iter_mut().find(|s| {
                    s.instrument == slot.parts.instrument && s.state != SubPositionState::Closed
                }) {
                    if s.stop_price != Some(*stop_price) {
                        s.stop_price = Some(*stop_price);
                        self.subs_changed = true;
                    }
                }
                Ok(())
            }
            InstrumentAction::UpdateExit {
                exit_active,
                exit_price,
                ..
            } => {
                let slot = &mut self.slots[idx];
                if let Some(p) = slot.pos_sync.pos.as_mut() {
                    p.exit_active = *exit_active;
                    p.exit_price = *exit_price;
                }
                Ok(())
            }
            InstrumentAction::Reverse { .. } => {
                // 新命名空间不支持 Reverse（调仓用 Adjust）→ 软拒。
                tracing::debug!(
                    instrument = %self.slots[idx].parts.instrument,
                    "拒动作：Reverse 已废弃（调仓请用 Adjust）"
                );
                *rejected_out += 1;
                Ok(())
            }
            // 组合级动作在 dispatch_actions 先行处理；单标的入口不可达。
            InstrumentAction::CloseMaster { .. } => Ok(()),
            InstrumentAction::Hold => Ok(()),
        }
    }

    fn dispatch_l1_fallback(
        &mut self,
        snapshots: &[InstrumentSnapshot],
        actions: &[InstrumentAction],
        actions_out: &mut u64,
        rejected_out: &mut u64,
    ) -> Result<()> {
        for idx in 0..self.slots.len() {
            if self.slots[idx].pos_sync.pos.is_none() {
                continue;
            }
            let key = self.slots[idx].instrument_key();
            // 该标的独立总仓已由 L3 接管平仓：L1 不再重复触发。
            if self.scope_is_paused(&key) {
                continue;
            }
            if actions
                .iter()
                .any(|a| a.instrument_key().as_deref() == Some(key.as_str()))
            {
                continue;
            }
            let Some(snap) = snapshots
                .iter()
                .find(|s| s.instrument.namespaced_key() == key)
            else {
                continue;
            };
            // L1 硬止损同样服从禁交易窗口：本 slot 的 base bar 落在 no_trade
            // 时段内不明发平仓（避免禁窗内下单）。
            if snap.no_trade {
                continue;
            }
            let exit = {
                let slot = &mut self.slots[idx];
                slot.risk_guard
                    .check_fallback(&snap.bar, slot.pos_sync.pos.as_mut())
            };
            let Some(exit) = exit else {
                continue;
            };
            let master_id = self.master_id_for_slot(idx);
            let ids = StrategyIds {
                strategy_type: &self.strategy_type,
                strategy_id: &self.strategy_id,
                master_id: &master_id,
                rebalance_epoch: self.portfolio.rebalance_epoch,
            };
            let slot = &mut self.slots[idx];
            let out = dispatch::close_position(
                slot,
                &ids,
                snap.bar.close,
                &exit.reason,
                exit.is_stop_loss,
                SubIntent::SubStop,
            )?;
            *actions_out += out.actions;
            *rejected_out += out.rejected;
            if out.actions > 0 {
                self.subs_changed = true;
            }
        }
        Ok(())
    }

    // ─── 成交落账 ─────────────────────────────────────────────────────────

    /// 撤出「开仓已明确失败且零成交」的挂单行（交易所拒单/重试预算耗尽）：
    /// 零成交不可能长出持仓，行留在 OpenRequested 会被对账当在途、被决策
    /// 当占位。返回是否删行。
    fn discard_abandoned_opens(&mut self, slot_idx: usize) -> bool {
        let candidates: Vec<String> = self.slots[slot_idx]
            .sub_positions
            .iter()
            .filter(|s| {
                matches!(
                    s.state,
                    SubPositionState::PendingOpen | SubPositionState::OpenRequested
                )
            })
            .map(|s| s.id.clone())
            .collect();
        if candidates.is_empty() {
            return false;
        }
        let Some(live) = self.slots[slot_idx].parts.execution.as_live_mut() else {
            return false;
        };
        let dead: Vec<String> = candidates
            .into_iter()
            .filter(|id| live.manager().is_abandoned_open(id))
            .collect();
        if dead.is_empty() {
            return false;
        }
        let slot = &mut self.slots[slot_idx];
        slot.sub_positions.retain(|s| !dead.contains(&s.id));
        for id in &dead {
            slot.pos_sync.open_meta.remove(id);
            if let Some(store) = &self.store {
                store.table::<SubPosition>().delete(id);
            }
        }
        true
    }

    fn sync_slot_fills(&mut self, slot_idx: usize) -> Result<Vec<Fill>> {
        // 失败开仓行不需要成交事件也会终结，故在轮首无条件清理。
        if self.discard_abandoned_opens(slot_idx) {
            self.subs_changed = true;
        }
        let fills = self.slots[slot_idx].parts.execution.drain_fills();
        if fills.is_empty() {
            return Ok(Vec::new());
        }
        let mut any = false;
        let mut fresh_ids: Vec<String> = Vec::new();
        // 平仓历史投影：整批成交前快照执行层持仓（apply_fill 会清空 pos），
        // 供本批内终态子仓的 history_position 记录取 peak/MFE/ATR/trail。
        let pos_before = self.slots[slot_idx].pos_sync.pos.clone();
        let mut closed: Vec<(String, DateTime<Utc>, f64, bool, f64)> = Vec::new();
        for f in &fills {
            if f.offset != Offset::Open {
                self.reduction_ids.remove(&f.signal_id);
            }
            // 开仓成交必须归属总仓；无活动总仓时补建（外部/手工作业成交）。
            // `Closing` 总仓也复用，成交落地后立即补平。逐标的独立总仓时
            // 成交只能落进该标的自己的总仓，不能拿策略级活动指针。
            let master_id = if f.offset == Offset::Open {
                let scope = self.position_scope(slot_idx);
                self.master_for_fill(f.at, scope.as_deref())
            } else if !self.config.merge_positions {
                self.master_id_for_slot(slot_idx)
            } else {
                self.portfolio.active_master_id.clone().unwrap_or_default()
            };
            let slot = &mut self.slots[slot_idx];
            // 平仓成交前记录可能被收口的子仓 id：成交后用来判定是否进入终态。
            let close_target = if f.offset == Offset::Open {
                None
            } else {
                slot.sub_positions
                    .iter()
                    .find(|s| s.instrument == f.instrument && s.state != SubPositionState::Closed)
                    .map(|s| s.id.clone())
            };
            if apply_sub_fill(slot, &self.strategy_id, &master_id, f) {
                any = true;
                if f.offset == Offset::Open {
                    // 本进程成交落账：交易所仓位可能滞后于 ack，标记待远程复核。
                    if let Some(sub) = slot.sub_positions.iter().find(|s| {
                        s.instrument == f.instrument && s.state != SubPositionState::Closed
                    }) {
                        fresh_ids.push(sub.id.clone());
                    }
                } else if let Some(id) = close_target {
                    if slot
                        .sub_positions
                        .iter()
                        .any(|s| s.id == id && s.state == SubPositionState::Closed)
                    {
                        closed.push((id, f.at, f.price, f.is_stop_loss, f.qty));
                    }
                }
            }
        }
        for id in fresh_ids {
            self.fresh_opens.insert(id);
        }
        if any {
            self.subs_changed = true;
        }
        for (id, at, price, is_stop_loss, qty) in closed {
            self.record_history_close(
                slot_idx,
                &id,
                at,
                price,
                is_stop_loss,
                qty,
                None,
                pos_before.as_ref(),
            );
        }
        for f in &fills {
            self.slots[slot_idx].pos_sync.apply_fill(f);
        }
        if let Ok(lots) = {
            let instrument = self.slots[slot_idx].parts.instrument.clone();
            self.slots[slot_idx]
                .parts
                .execution
                .remote_positions(&instrument)
        } {
            self.slots[slot_idx].pos_sync.sync_book(lots);
        }
        self.summary.fills.extend(fills.clone());
        // 总仓 Closing 后新落地的开仓成交立即补平；随后按子仓终态收口总仓。
        self.close_inflight_under_closing(slot_idx)?;
        let settle_at = fills
            .last()
            .map(|f| f.at)
            .or(self.slots[slot_idx].last_bar_at)
            .unwrap_or_else(Utc::now);
        self.settle_masters(settle_at);
        Ok(fills)
    }

    /// 子仓进入终态时落 `history_position` Store 行 + 对外 22 列 CSV。
    ///
    /// 数据口径：开平价/时间/方向取自 `SubPosition`，成交价量取自平仓 `Fill`，
    /// peak/MFE/ATR/trail 取自平仓前的 `PositionState` 快照（执行层逐 bar 跟踪）。
    /// 幂等：Store 已有同 id 行则跳过（重启/重放不重复追加 CSV）。
    // 参数是平仓终态的一组事实字段，拆 struct 只增加调用点噪音。
    #[allow(clippy::too_many_arguments)]
    fn record_history_close(
        &self,
        slot_idx: usize,
        sub_id: &str,
        exit_at: DateTime<Utc>,
        exit_price: f64,
        is_stop_loss: bool,
        exit_qty: f64,
        reason_override: Option<&str>,
        pos: Option<&PositionState>,
    ) {
        let (Some(store), Some(hproj)) = (self.store.as_ref(), self.history.as_ref()) else {
            return;
        };
        let Some(sub) = self.slots[slot_idx]
            .sub_positions
            .iter()
            .find(|s| s.id == sub_id)
        else {
            return;
        };
        let history = store.table::<HistoryPosition>();
        if history.get(sub_id).is_some() {
            return;
        }
        let rec = HistoryPosition {
            position_id: sub_id.to_string(),
            strategy_name: hproj.strategy_name.clone(),
            symbol: sub.instrument.symbol.clone(),
            position_type: match sub.side {
                Side::Long => PosSide::Long,
                Side::Short => PosSide::Short,
            },
            entry_price: sub.entry_price,
            exit_price,
            entry_time: sub.opened_at,
            exit_time: exit_at,
            entry_timestamp: sub.opened_at.map(|t| t.timestamp()).unwrap_or(0),
            exit_timestamp: exit_at.timestamp(),
            peak_price: pos.map(|p| p.peak_price).unwrap_or(sub.entry_price),
            stop_price: sub
                .stop_price
                .or_else(|| pos.map(|p| p.stop_price))
                .unwrap_or(0.0),
            max_pnl_pct: pos.map(|p| p.max_pnl_pct).unwrap_or(0.0),
            min_pnl_pct: pos.map(|p| p.min_pnl_pct).unwrap_or(0.0),
            exit_reason: reason_override.map(str::to_string).unwrap_or_else(|| {
                if is_stop_loss {
                    "stop_loss".into()
                } else {
                    format!("{}_close", hproj.trading_mode)
                }
            }),
            is_stop_loss,
            atr_at_entry: pos.map(|p| p.atr_at_entry).unwrap_or(0.0),
            trail_activated: pos.map(|p| p.trail_activated).unwrap_or(false),
            trading_mode: hproj.trading_mode.clone(),
            entry_size: pos.map(|p| p.total_size).unwrap_or(exit_qty),
            exit_size: exit_qty,
        };
        if let Err(e) = history.insert(rec.clone()) {
            tracing::warn!(position_id = sub_id, error = %e, "history_position 行写入失败");
            return;
        }
        if let Err(e) = hproj.logger.log_exit(&rec) {
            tracing::warn!(position_id = sub_id, error = %e, "history_position CSV 追加失败");
        }
    }

    // ─── 净值 / 持久化 helpers ────────────────────────────────────────────

    fn current_nav(&self) -> f64 {
        let gross: f64 = self
            .slots
            .iter()
            .map(|s| {
                s.parts
                    .execution
                    .account_snapshot()
                    .map(|a| a.equity())
                    .unwrap_or(0.0)
            })
            .sum();
        gross - self.repeated_initial_cash()
    }

    fn current_cash(&self) -> f64 {
        let gross: f64 = self
            .slots
            .iter()
            .map(|s| {
                s.parts
                    .execution
                    .account_snapshot()
                    .map(|a| a.cash)
                    .unwrap_or(0.0)
            })
            .sum();
        gross - self.repeated_initial_cash()
    }

    /// 每个 slot 以同一总额虚拟起账时，多出来的重复起账现金。
    fn repeated_initial_cash(&self) -> f64 {
        match self.initial_cash_total {
            Some(total) if self.slots_seed_total && self.slots.len() > 1 => {
                total * (self.slots.len() - 1) as f64
            }
            _ => 0.0,
        }
    }

    fn update_portfolio(&mut self, nav: f64) {
        self.portfolio.nav = nav;
        if nav > self.portfolio.high_watermark {
            self.portfolio.high_watermark = nav;
        }
        let hw = self.portfolio.high_watermark;
        self.portfolio.drawdown = if hw > 1e-12 { (hw - nav) / hw } else { 0.0 };
    }

    /// Phase 2 派发（§9.2）：所有减仓腿成交确认后，按 key 序补发加仓，
    /// 使用 `rebalance_epoch + 1` 的 signal id；完成后推进 epoch、记录
    /// `last_rebalance_at` 并清零计数。清算/暂停状态下直接放弃排队。
    fn try_phase2(
        &mut self,
        at: DateTime<Utc>,
        actions_out: &mut u64,
        rejected_out: &mut u64,
        traded_out: &mut bool,
    ) -> Result<()> {
        if self.pending_adds.is_empty() || !self.reduction_ids.is_empty() {
            return Ok(());
        }
        if !self.can_trade() {
            self.pending_adds.clear();
            return Ok(());
        }
        let adds = std::mem::take(&mut self.pending_adds);
        let epoch = self.portfolio.rebalance_epoch + 1;
        let mut dispatched = false;
        for (a, eligible) in adds {
            if !eligible {
                *rejected_out += 1;
                continue;
            }
            let Some(k) = a.instrument_key() else {
                continue;
            };
            let Some(idx) = self.slots.iter().position(|sl| sl.instrument_key() == k) else {
                *rejected_out += 1;
                continue;
            };
            let price = self.slots[idx].last_mark.unwrap_or(0.0);
            if price <= 0.0 {
                *rejected_out += 1;
                continue;
            }
            self.dispatch_sub(
                idx,
                at,
                price,
                &a,
                actions_out,
                rejected_out,
                traded_out,
                &mut Vec::new(),
                Some(epoch),
            )?;
            dispatched = true;
        }
        if dispatched {
            self.portfolio.rebalance_epoch = epoch;
            self.portfolio.last_rebalance_at = Some(at);
            self.bars_since_rebalance = 0;
        }
        Ok(())
    }

    fn rebalance_every(&self) -> u64 {
        self.config
            .rebalance
            .as_ref()
            .map(|r| r.every_bars)
            .unwrap_or(u64::MAX)
    }

    fn all_sub_positions(&self) -> Vec<SubPosition> {
        self.slots
            .iter()
            .flat_map(|s| s.sub_positions.clone())
            .collect()
    }

    fn multiplier_for(&self, idx: usize) -> f64 {
        self.slots[idx]
            .parts
            .trading
            .spec(&self.slots[idx].parts.instrument)
            .map(|s| s.multiplier)
            .unwrap_or(1.0)
    }

    /// 子仓硬上限对应的绝对数量；未配置 `sizing.max_position_pct` 时不限制。
    fn max_sub_qty(&self, nav: f64, unit_notional: f64) -> f64 {
        match self.config.sizing.max_position_pct {
            Some(cap) => cap * nav / unit_notional.max(1e-12),
            None => f64::INFINITY,
        }
    }

    fn multiplier_map(&self) -> BTreeMap<String, f64> {
        let mut m = BTreeMap::new();
        for s in &self.slots {
            let mult = s
                .parts
                .trading
                .spec(&s.parts.instrument)
                .map(|sp| sp.multiplier)
                .unwrap_or(1.0);
            m.insert(s.instrument_key(), mult);
        }
        m
    }

    /// 平仓归属日是否早于 `today`（`YYYYMMDD`，UTC，与 Store::flush 同口径）。
    /// `closed_at` 缺失时保守返回 false：宁可多写一行，也不把当日终态丢出快照。
    fn closed_before(closed_at: Option<DateTime<Utc>>, today: &str) -> bool {
        closed_at.is_some_and(|t| t.format("%Y%m%d").to_string().as_str() < today)
    }

    fn persist_round(&self) {
        if let Some(store) = &self.store {
            let today = store.current_day();
            // 组合行带调仓计数：计数是本轮决策刚推进的值，写入后再 flush。
            let mut portfolio = self.portfolio.clone();
            portfolio.bars_since_rebalance = self.bars_since_rebalance;
            // 阶段是 runner 内存态（重启按设计回到 Running 放行），但落盘行必须
            // 与当轮真实阶段一致，否则快照会永远显示 Running 而误导排查。
            portfolio.phase = self.phase;
            store.table::<StrategyPortfolio>().upsert(portfolio);
            for master in self.masters.values() {
                // 归属日早于今天的终态只在它自己的日文件里；账本为了
                // master_seq/对账仍保留对象，但不能写回当日快照——否则次日
                // 文件会带上旧交易日仓位，且新插入的 created_at 被重置。
                if master.state == MasterPositionState::Closed
                    && Self::closed_before(master.closed_at, &today)
                {
                    continue;
                }
                store.table::<MasterPosition>().upsert(master.clone());
            }
            if self.subs_changed {
                for slot in &self.slots {
                    for sub in &slot.sub_positions {
                        if sub.state == SubPositionState::Closed
                            && Self::closed_before(sub.closed_at, &today)
                        {
                            continue;
                        }
                        store.table::<SubPosition>().upsert(sub.clone());
                    }
                }
            }
        }
    }
}

/// 把一笔成交落进子仓位账本。返回 true = 该行发生新建/状态/数量变化
///（调用方置 `subs_changed` 用于持久化）。开仓：
/// - 按 `signal_id` 命中 `PendingOpen/OpenRequested` → `FillOpen` 落 `Active`；
/// - 未命中但有该 instrument 非 `Closed` 子仓位 → **加权并入既有行**（G1），
///   `entry_price = (old*qty_old + fill.price*fill.qty)/new_qty`，不新建第二行；
/// - 两者皆无 → 新建 `Active` 行（id = `Fill.signal_id`）。
///
/// 平仓：找该 instrument 唯一非 `Closed` 子仓位，扣 qty / 累计
/// `realized_pnl`；按 `remaining_after` 是否归零决定 `Closed`。
fn apply_sub_fill(
    slot: &mut StrategySlot,
    strategy_id: &str,
    master_id: &str,
    fill: &Fill,
) -> bool {
    let instrument = &fill.instrument;
    if fill.offset == Offset::Open {
        // 1) 按 signal_id 命中未成交开仓行。
        if let Some(sub) = slot.sub_positions.iter_mut().find(|s| {
            s.id == fill.signal_id
                && (s.state == SubPositionState::PendingOpen
                    || s.state == SubPositionState::OpenRequested)
        }) {
            if let Some(next) = advance_sub_state(
                sub.state,
                &SubPositionEvent::FillOpen {
                    qty: fill.qty,
                    price: fill.price,
                    at: fill.at,
                },
            ) {
                sub.state = next;
            }
            sub.qty = fill.qty;
            sub.entry_price = fill.price;
            sub.opened_at = Some(fill.at);
            return true;
        }
        // 2) 无匹配 pending：加权并入既有非 Closed 子仓位（add-to-existing）。
        if let Some(sub) = slot
            .sub_positions
            .iter_mut()
            .find(|s| s.instrument == *instrument && s.state != SubPositionState::Closed)
        {
            let new_qty = sub.qty + fill.qty;
            if new_qty > 1e-12 {
                sub.entry_price = (sub.entry_price * sub.qty + fill.price * fill.qty) / new_qty;
            }
            sub.qty = new_qty;
            return true;
        }
        // 3) 全新 Active 行。
        slot.sub_positions.push(SubPosition {
            id: fill.signal_id.clone(),
            strategy_id: strategy_id.to_string(),
            master_id: master_id.to_string(),
            instrument: instrument.clone(),
            side: fill.side,
            qty: fill.qty,
            entry_price: fill.price,
            stop_price: None,
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: Some(fill.at),
            closed_at: None,
            realized_pnl: None,
        });
        true
    } else {
        // 平仓成交。
        let Some(sub) = slot
            .sub_positions
            .iter_mut()
            .find(|s| s.instrument == *instrument && s.state != SubPositionState::Closed)
        else {
            return false; // 无子仓位：忽略该平仓成交（先平后开等陈旧腿）。
        };
        let new_qty = (sub.qty - fill.qty).max(0.0);
        sub.realized_pnl = Some(sub.realized_pnl.unwrap_or(0.0) + fill.realized_pnl);
        let event = SubPositionEvent::FillClose {
            qty: fill.qty,
            realized_pnl: fill.realized_pnl,
            at: fill.at,
            remaining_after: new_qty,
        };
        if let Some(next) = advance_sub_state(sub.state, &event) {
            sub.state = next;
            if sub.state == SubPositionState::Closed {
                sub.closed_at = Some(fill.at);
            }
        }
        sub.qty = new_qty;
        true
    }
}

// ─── 自由函数 helpers ───────────────────────────────────────────────────────

fn queue_pending(v: &mut Vec<Timeframe>, closed: Vec<Timeframe>) {
    for tf in closed {
        if !v.contains(&tf) {
            v.push(tf);
        }
    }
}

fn handle_slot_session(slot: &mut StrategySlot, bar: &Bar, day: TradingDay) -> Result<()> {
    if slot.last_day.is_some_and(|d| d != day) {
        let closed = slot.state.on_day_boundary(slot.parts.aggregation.as_ref());
        queue_pending(&mut slot.pending_newly, closed);
        slot.parts.position.rollover(&mut slot.pos_sync.book, day);
    }
    slot.last_day = Some(day);
    let session = slot.parts.session.as_ref();
    if session.is_session_open_time(bar.open_time) {
        slot.risk_guard.record_session_open(bar.open);
    }
    slot.pos_sync.on_price(bar.high, bar.low);
    Ok(())
}

/// 与 `ExchangeRunner::aggregate_and_gate` 同口径：聚合 + pending/blocked 合并。
fn aggregate_and_gate_slot(
    slot: &mut StrategySlot,
    bar: &Bar,
    _day: TradingDay,
    session: &dyn SessionRules,
) -> Result<Vec<Timeframe>> {
    let mut newly =
        slot.state
            .on_closed_1m(bar.clone(), slot.parts.aggregation.as_ref(), session)?;
    for tf in std::mem::take(&mut slot.pending_newly) {
        if !newly.contains(&tf) {
            newly.push(tf);
        }
    }
    newly.sort();
    let no_trade = session.is_no_trade_window(bar.open_time);
    // signal_resume 必须取自「上一根 bar」的 no_trade（先算再用，与
    // `ExchangeRunner::aggregate_and_gate` 同口径），再更新给下一根。
    let signal_resume = !no_trade && slot.signal_blocked_prev;
    slot.signal_blocked_prev = no_trade;
    slot.last_signal_resume = signal_resume;
    if no_trade {
        for tf in &newly {
            if tf.0 > 1 && !slot.blocked_closed.contains(tf) {
                slot.blocked_closed.push(*tf);
            }
        }
    } else {
        let mut merged = std::mem::take(&mut slot.blocked_closed);
        for tf in &newly {
            if !merged.contains(tf) {
                merged.push(*tf);
            }
        }
        merged.sort();
        newly = merged;
    }
    newly.dedup();
    Ok(newly)
}

/// 该 slot 目标子仓位的带符号净数量（Long 正 / Short 负；无 → 0）。
fn current_signed(slot: &StrategySlot) -> f64 {
    slot.sub_positions
        .iter()
        .find(|s| s.instrument == slot.parts.instrument && s.state != SubPositionState::Closed)
        .map(|s| match s.side {
            Side::Long => s.qty,
            Side::Short => -s.qty,
        })
        .unwrap_or(0.0)
}

/// true = 存在非 `Active` 子仓位（Close/Adjust 应拒）；false = 无仓或 `Active`。
/// 子仓不在 Active（订单在途或正在平仓）：平仓必须拒，调仓必须延后。
fn sub_state_not_active(slot: &StrategySlot) -> bool {
    slot.sub_positions
        .iter()
        .find(|s| s.instrument == slot.parts.instrument && s.state != SubPositionState::Closed)
        .map(|s| !matches!(s.state, SubPositionState::Active))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use crate::core::bar::{Bar, BarExtensions, BarFinality};
    use crate::core::exchange::{Exchange, SourceRequest};
    use crate::core::execution::{
        Account, ExecutionPort, OrderAck, OrderContext, OrderRequest, OrderSnapshot, OrderState,
    };
    use crate::core::identity::{ExchangeId, InstrumentId};
    use crate::core::position::PositionLot;
    use crate::core::session::{
        ContinuousSession, SessionGap, SessionStatus, SessionWindow, TradingDay,
    };
    use crate::core::trading::{InstrumentSpec, SpecDrivenTradingRules};
    use crate::exchanges::binance::BinanceExchange;
    use crate::strategy::config::{self, StrategyConfig};
    use crate::strategy::features::FeaturePlan;
    use crate::strategy::port::{
        CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
    };
    use chrono::{Duration, FixedOffset, NaiveDate, TimeZone, Timelike};
    use strategy_core::risk::{RiskControlConfig, TrailingProfitConfig};

    /// 测试用 ExecutionPort 桩：受理全部订单、即时产出成交、固定账户 1000。
    /// 不校验撮合账本（预置子仓位做减仓腿时不报 CloseSplit）。
    struct TestExecution {
        orders: Arc<Mutex<Vec<OrderRequest>>>,
        fills: Mutex<Vec<Fill>>,
        accept: bool,
        /// PR-5 T3：`remote_positions` 注入（对账最终事实）
        remote: Vec<PositionLot>,
        /// 模拟「订单仍在途」的 signal_id 集合（对账收口闸门）
        in_flight: Vec<String>,
        /// 场所侧单笔开仓名义；`Some` 时模拟 live 的 order_cash 口径
        venue_notional: Option<f64>,
    }

    impl ExecutionPort for TestExecution {
        fn submit(&mut self, order: &OrderRequest, ctx: &OrderContext) -> Result<OrderAck> {
            self.orders.lock().unwrap().push(order.clone());
            if !self.accept {
                return Ok(OrderAck {
                    order_id: String::new(),
                    accepted: false,
                    reason: Some("mock rejected".into()),
                });
            }
            self.fills.lock().unwrap().push(Fill {
                order_id: format!("ord_{}", order.signal_id),
                signal_id: order.signal_id.clone(),
                instrument: order.instrument.clone(),
                side: order.side,
                offset: if order.offset == Offset::Open {
                    Offset::Open
                } else {
                    Offset::Auto
                },
                qty: order.qty,
                price: order.price,
                fee: 0.0,
                realized_pnl: 0.0,
                margin_release: 0.0,
                at: ctx.at,
                trading_day: ctx.trading_day,
                is_stop_loss: order.is_stop_loss,
                qty_raw: None,
            });
            Ok(OrderAck {
                order_id: format!("ord_{}", order.signal_id),
                accepted: true,
                reason: None,
            })
        }
        fn query(&mut self, _order_id: &str) -> Result<OrderSnapshot> {
            Ok(OrderSnapshot {
                order_id: String::new(),
                state: OrderState::Filled,
                filled_qty: 0.0,
                avg_price: 0.0,
                fee: 0.0,
            })
        }
        fn remote_positions(&mut self, _instrument: &InstrumentId) -> Result<Vec<PositionLot>> {
            Ok(self.remote.clone())
        }
        fn order_in_flight(&self, signal_id: &str) -> bool {
            self.in_flight.iter().any(|s| s == signal_id)
        }
        fn venue_order_notional(&self) -> Option<f64> {
            self.venue_notional
        }
        fn drain_fills(&mut self) -> Vec<Fill> {
            std::mem::take(&mut *self.fills.lock().unwrap())
        }
        fn account_snapshot(&self) -> Option<Account> {
            Some(Account::new("USDT", 1000.0))
        }
    }

    fn utc(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    fn strategy_config(base: &str, every_bars: Option<u64>) -> StrategyConfig {
        StrategyConfig {
            strategy_type: "xsec_momentum".into(),
            strategy_id: Some("xsec_momentum:default".into()),
            mode: config::StrategyMode::CrossSectional,
            merge_positions: true,
            universe: vec![config::UniverseEntry::Symbol("BTCUSDT".into())],
            timeframes: vec![base.to_string()],
            indicators: BTreeMap::new(),
            entry: config::RuleSet {
                default: Vec::new(),
                overrides: BTreeMap::new(),
            },
            exit: config::RuleSet {
                default: Vec::new(),
                overrides: BTreeMap::new(),
            },
            portfolio_exit: config::PortfolioExitCfg {
                trailing: None,
                stop_loss_pct: None,
            },
            rebalance: every_bars.map(|b| config::RebalanceCfg {
                every_bars: b,
                top_k: 1,
                bottom_k: 0,
                weight_scheme: config::WeightScheme::Equal,
            }),
            risk: config::RiskCfg {
                max_drawdown: None,
                max_total_notional: 1.0,
            },
            sizing: config::SizingCfg {
                per_position_pct: 0.5,
                max_position_pct: None,
            },
            params: None,
        }
    }

    fn build_slot(symbol: &str, cash: f64, base_tf: Timeframe) -> StrategySlot {
        let ex = BinanceExchange::new(0.0);
        let inst = ex.parse_instrument(symbol).unwrap();
        let base = match base_tf {
            Timeframe::M1 => "1m",
            Timeframe::M15 => "15m",
            _ => "1m",
        };
        let cfg = strategy_config(base, None);
        let feature_plan = FeaturePlan::from_config(&cfg).unwrap();
        let parts = ex
            .build(&SourceRequest::backtest(inst.clone(), Vec::new(), cash))
            .unwrap();
        StrategySlot::new(SlotInit {
            parts,
            timeframes: vec![base_tf],
            feature_plan,
            risk: RiskControlConfig {
                enabled: false, // 测试禁用 L1，避免价格波动误触硬止损
                fixed_stop_loss_pct: 0.0,
                trailing_profit: TrailingProfitConfig::default(),
                fixed_take_profit_pct: 0.0,
            },
            strategy_type: "xsec_momentum".to_string(),
            max_closed_len: 300,
        })
    }

    fn bar_at(inst: &InstrumentId, t: DateTime<Utc>, close: f64) -> Bar {
        Bar {
            instrument: inst.clone(),
            open_time: t,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
            finality: BarFinality::Final,
            extensions: BarExtensions::default(),
        }
    }

    /// 测试用 SessionRules 桩：委托 `ContinuousSession`，仅按分钟拦截
    /// `is_no_trade_window`（`t.minute() == no_trade_minute` 为禁交易窗口）。
    #[derive(Clone)]
    struct StubSession {
        inner: ContinuousSession,
        no_trade_minute: u32,
    }

    impl SessionRules for StubSession {
        fn timezone(&self) -> FixedOffset {
            self.inner.timezone
        }
        fn trading_day_of(&self, t: DateTime<Utc>) -> TradingDay {
            self.inner.trading_day_of(t)
        }
        fn is_trading_day(&self, d: NaiveDate) -> bool {
            self.inner.is_trading_day(d)
        }
        fn sessions_on(&self, day: TradingDay) -> Vec<SessionWindow> {
            self.inner.sessions_on(day)
        }
        fn session_at(&self, t: DateTime<Utc>) -> SessionStatus {
            self.inner.session_at(t)
        }
        fn is_session_boundary(&self, t: DateTime<Utc>) -> bool {
            self.inner.is_session_boundary(t)
        }
        fn backfill_start(&self, now: DateTime<Utc>, tf: Timeframe, bars: usize) -> DateTime<Utc> {
            self.inner.backfill_start(now, tf, bars)
        }
        fn gap_after(&self, t: DateTime<Utc>) -> Option<SessionGap> {
            self.inner.gap_after(t)
        }
        fn is_no_trade_window(&self, t: DateTime<Utc>) -> bool {
            t.minute() == self.no_trade_minute
        }
    }

    /// 录制策略：调用次数/快照计数组 + 预排响应队列（Sub 命名空间）。
    struct RecordingStrategy {
        calls: Arc<Mutex<Vec<Vec<InstrumentSnapshot>>>>,
        contexts: Arc<Mutex<Vec<PortfolioContext>>>,
        responses: VecDeque<Vec<InstrumentAction>>,
    }

    impl RecordingStrategy {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                contexts: Arc::new(Mutex::new(Vec::new())),
                responses: VecDeque::new(),
            }
        }
        fn queue(&mut self, actions: Vec<InstrumentAction>) {
            self.responses.push_back(actions);
        }
    }

    impl CrossSectionalStrategyPort for RecordingStrategy {
        fn strategy_type(&self) -> &str {
            "recording"
        }

        fn on_cross_section_bars(
            &mut self,
            _newly: &[Timeframe],
            snapshots: &[InstrumentSnapshot],
            port: &PortfolioContext,
        ) -> Vec<InstrumentAction> {
            self.calls.lock().unwrap().push(snapshots.to_vec());
            self.contexts.lock().unwrap().push(port.clone());
            self.responses.pop_front().unwrap_or_default()
        }
    }

    type RecordingCalls = Arc<Mutex<Vec<Vec<InstrumentSnapshot>>>>;

    fn recording_strategy(
        responses: Vec<Vec<InstrumentAction>>,
    ) -> (RecordingStrategy, RecordingCalls) {
        let mut s = RecordingStrategy::new();
        for r in responses {
            s.queue(r);
        }
        let calls = s.calls.clone();
        (s, calls)
    }

    fn recording_group(
        slots: Vec<StrategySlot>,
        cfg: StrategyConfig,
    ) -> (CrossSectionalRunnerGroup, RecordingCalls) {
        let (strategy, calls) = recording_strategy(Vec::new());
        let g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            slots,
            Box::new(strategy),
            None,
        )
        .unwrap();
        (g, calls)
    }

    fn drive(
        g: &mut CrossSectionalRunnerGroup,
        events: &[(usize, DateTime<Utc>, Bar)],
    ) -> Vec<Option<RoundReport>> {
        events
            .iter()
            .map(|(slot, at, bar)| {
                g.on_event(AlignedEvent {
                    at: *at,
                    slot: *slot,
                    event: MarketEvent::Bar(bar.clone()),
                })
                .unwrap()
            })
            .collect()
    }

    #[test]
    fn round_processes_all_slots_at_same_timestamp_once() {
        let t0 = utc(2026, 1, 5, 0, 0);
        let t1 = t0 + Duration::minutes(1);
        let (mut g, calls) = recording_group(
            vec![
                build_slot("BTCUSDT", 500.0, Timeframe::M1),
                build_slot("ETHUSDT", 500.0, Timeframe::M1),
            ],
            strategy_config("1m", None),
        );
        let inst0 = g.slots[0].parts.instrument.clone();
        let inst1 = g.slots[1].parts.instrument.clone();
        let events = vec![
            (0usize, t0, bar_at(&inst0, t0, 100.0)),
            (1usize, t0, bar_at(&inst1, t0, 100.0)),
            (0usize, t1, bar_at(&inst0, t1, 101.0)),
            (1usize, t1, bar_at(&inst1, t1, 101.0)),
        ];
        let reports = drive(&mut g, &events);
        assert!(reports[0].is_none(), "t0 slot0：pending 积累");
        assert!(reports[1].is_none(), "t0 slot1：pending 积累");
        let r_t0 = reports[2].as_ref().expect("t+1 到达 flush t0 轮次");
        assert!(r_t0.decision_round, "base=1m 每根闭合都是决策轮");
        assert!(r_t0.fills.is_empty(), "Hold 策略无成交");
        assert_eq!(r_t0.actions, 0);
        assert_eq!(r_t0.at, t0, "决策轮 at = base bar open_time");
        assert!(reports[3].is_none(), "t1 slot1：进入新一轮 pending");
        assert_eq!(calls.lock().unwrap().len(), 1, "t0 轮次策略恰好调用 1 次");
        assert_eq!(calls.lock().unwrap()[0].len(), 2, "t0 轮次快照 2 个");
        let summary = g.finish().unwrap();
        assert_eq!(summary.bars_processed, 4);
    }

    #[test]
    fn source_idle_and_reconnected_are_ignored_without_round() {
        // PR-5 Task 1：SourceIdle 是 try_next_event 的「该源暂无」进程内信号，
        // 与 Reconnected 同形——不进聚合、不触发决策轮、不调策略、不计数。
        let t0 = utc(2026, 1, 5, 0, 0);
        let (mut g, calls) = recording_group(
            vec![
                build_slot("BTCUSDT", 500.0, Timeframe::M1),
                build_slot("ETHUSDT", 500.0, Timeframe::M1),
            ],
            strategy_config("1m", None),
        );
        let inst0 = g.slots[0].parts.instrument.clone();
        for ev in [MarketEvent::SourceIdle, MarketEvent::Reconnected].iter() {
            let out = g
                .on_event(AlignedEvent {
                    at: t0,
                    slot: 0,
                    event: ev.clone(),
                })
                .unwrap();
            assert!(out.is_none(), "{ev:?} 不应产生决策轮");
        }
        assert!(calls.lock().unwrap().is_empty(), "忽略事件不得调用策略");
        assert!(g.take_last_round().is_none(), "忽略事件不得留下轮次报告");
        // 后续正常 bar 流不受影响：t0 双标的照常成轮
        let inst1 = g.slots[1].parts.instrument.clone();
        let reports = drive(
            &mut g,
            &[
                (0usize, t0, bar_at(&inst0, t0, 100.0)),
                (1usize, t0, bar_at(&inst1, t0, 100.0)),
                (
                    0usize,
                    t0 + Duration::minutes(1),
                    bar_at(&inst0, t0 + Duration::minutes(1), 101.0),
                ),
                (
                    1usize,
                    t0 + Duration::minutes(1),
                    bar_at(&inst1, t0 + Duration::minutes(1), 101.0),
                ),
            ],
        );
        assert!(
            reports[2].is_some(),
            "忽略事件不应破坏 flush-before-ingest 节拍"
        );
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "策略仍只被真实轮次调用 1 次"
        );
        let summary = g.finish().unwrap();
        assert_eq!(
            summary.bars_processed, 4,
            "SourceIdle/Reconnected 不计入 bars"
        );
    }

    #[test]
    fn base_tf_close_gates_strategy_and_1m_ticks_do_not() {
        let t0 = utc(2026, 1, 5, 0, 0);
        let (mut g, calls) = recording_group(
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M15)],
            strategy_config("15m", None),
        );
        let inst = g.slots[0].parts.instrument.clone();
        let events: Vec<(usize, DateTime<Utc>, Bar)> = (0..15)
            .map(|i| {
                let t = t0 + Duration::minutes(i);
                (0usize, t, bar_at(&inst, t, 100.0))
            })
            .collect();
        let reports = drive(&mut g, &events);
        assert_eq!(reports.len(), 15);
        for (i, r) in reports[..14].iter().enumerate() {
            let r = r
                .as_ref()
                .unwrap_or_else(|| panic!("第 {i} 根应为非决策报告"));
            assert!(!r.decision_round, "第 {i} 根 1m 不触发决策");
            assert_eq!(r.actions, 0);
        }
        assert!(
            reports[14].is_none(),
            "第 15 根 = base 闭合，pending 等待 flush"
        );
        assert_eq!(calls.lock().unwrap().len(), 0, "flush 前策略 0 次调用");
        let summary = g.finish().unwrap();
        assert_eq!(summary.bars_processed, 15);
        assert_eq!(calls.lock().unwrap().len(), 1, "finish 时 flush 决策轮");
        let last = g.take_last_round().unwrap();
        assert!(last.decision_round, "take_last_round 补采决策报告");
        assert_eq!(last.at, t0, "决策轮 at = base bar open_time（桶起点）");
    }

    #[test]
    fn missing_slot_is_marked_stale_and_quorum_skips_strategy() {
        let t0 = utc(2026, 1, 5, 0, 0);
        let (mut g, calls) = recording_group(
            vec![
                build_slot("BTCUSDT", 1000.0, Timeframe::M15),
                build_slot("ETHUSDT", 1000.0, Timeframe::M15),
                build_slot("SOLUSDT", 1000.0, Timeframe::M15),
            ],
            strategy_config("15m", None),
        );
        // preload 闭合第一个 15m 桶：3 标的都有历史 base bar。
        let warmup: Vec<Vec<Bar>> = g
            .slots
            .iter()
            .map(|s| {
                let inst = s.parts.instrument.clone();
                (0..15)
                    .map(|i| bar_at(&inst, t0 + Duration::minutes(i), 100.0))
                    .collect()
            })
            .collect();
        g.preload(&warmup).unwrap();
        // 第二个桶：BTC/ETH 满 15 根，SOL 缺 00:29（base 闭合时缺 bar）。
        let mut events = Vec::new();
        for i in 15..29 {
            let t = t0 + Duration::minutes(i);
            for si in 0..3 {
                let inst = g.slots[si].parts.instrument.clone();
                events.push((si, t, bar_at(&inst, t, 100.0)));
            }
        }
        let t29 = t0 + Duration::minutes(29);
        for si in 0..2 {
            let inst = g.slots[si].parts.instrument.clone();
            events.push((si, t29, bar_at(&inst, t29, 100.0)));
        }
        drive(&mut g, &events);
        let summary = g.finish().unwrap();
        assert_eq!(
            summary.bars_skipped_stale, 1,
            "non_stale=2 < quorum=3 只计一次 stale skip"
        );
        assert_eq!(calls.lock().unwrap().len(), 0, "quorum 不达标策略 0 次调用");
        assert_eq!(
            summary.bars_processed,
            3 * 14 + 2,
            "on_event 44 根，preload 不计入"
        );
        let snaps = &g.last_snapshots;
        assert_eq!(snaps.len(), 3, "三标的都有历史 base bar，快照三份");
        let sol = snaps
            .iter()
            .find(|s| s.instrument.symbol == "SOLUSDT")
            .unwrap();
        assert!(sol.stale, "缺失 slot 必须 stale");
        assert!(!sol.entry_eligible, "stale slot entry_eligible=false");
        let btc = snaps
            .iter()
            .find(|s| s.instrument.symbol == "BTCUSDT")
            .unwrap();
        assert!(!btc.stale, "闭合 slot 非 stale");
        assert!(btc.entry_eligible);
    }

    #[test]
    fn nav_and_high_watermark_update_after_round() {
        let t0 = utc(2026, 1, 5, 0, 0);
        let btc_inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let (strategy, _) = recording_strategy(vec![vec![InstrumentAction::Open {
            instrument: btc_inst,
            side: Side::Long,
            price: 100.0,
            stop_price: 95.0,
            atr: 0.0,
            strength: 0.0,
            reason: "entry".into(),
        }]]);
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            strategy_config("1m", None),
            vec![
                build_slot("BTCUSDT", 500.0, Timeframe::M1),
                build_slot("ETHUSDT", 500.0, Timeframe::M1),
            ],
            Box::new(strategy),
            None,
        )
        .unwrap();
        assert!(
            (g.portfolio.nav - 1000.0).abs() < 1e-9,
            "初始 nav = Σ equity = 1000"
        );

        let t1 = t0 + Duration::minutes(1);
        let t2 = t0 + Duration::minutes(2);
        let t3 = t0 + Duration::minutes(3);
        let inst0 = g.slots[0].parts.instrument.clone();
        let inst1 = g.slots[1].parts.instrument.clone();
        let events = vec![
            (0usize, t0, bar_at(&inst0, t0, 100.0)),
            (1usize, t0, bar_at(&inst1, t0, 100.0)),
            (0usize, t1, bar_at(&inst0, t1, 110.0)),
            (1usize, t1, bar_at(&inst1, t1, 100.0)),
            (0usize, t2, bar_at(&inst0, t2, 95.0)),
            (1usize, t2, bar_at(&inst1, t2, 100.0)),
            (0usize, t3, bar_at(&inst0, t3, 110.0)),
            (1usize, t3, bar_at(&inst1, t3, 100.0)),
        ];
        let reports = drive(&mut g, &events);
        let r0 = reports[2].as_ref().unwrap();
        assert!((r0.nav - 1000.0).abs() < 1e-9, "r0.nav={}", r0.nav);
        // 未来函数修复后：决策轮 t_i 的 NAV = t_i 自身 bar 收盘盯市
        // （t1: BTC 浮盈 +10×5=50 → 1050；t2: BTC 回吐 → 975）。
        let r1 = reports[4].as_ref().unwrap();
        assert!((r1.nav - 1050.0).abs() < 1e-9, "r1.nav={}", r1.nav);
        let r2 = reports[6].as_ref().unwrap();
        assert!((r2.nav - 975.0).abs() < 1e-9, "r2.nav={}", r2.nav);
        let summary = g.finish().unwrap();
        assert_eq!(summary.bars_processed, 8);
        let last = g.take_last_round().unwrap();
        assert!(
            (last.nav - 1050.0).abs() < 1e-9,
            "finish flush nav={}",
            last.nav
        );
        assert!(
            (g.portfolio.nav - last.nav).abs() < 1e-9,
            "portfolio.nav == report.nav"
        );
        assert!(
            (g.portfolio.high_watermark - 1050.0).abs() < 1e-9,
            "high_watermark == max(历史 nav) = 1050"
        );
        assert!(g.portfolio.drawdown.abs() < 1e-9, "末轮 nav==hw，回撤 0");
    }

    #[test]
    fn base_tf_close_increments_rebalance_counter_and_epoch() {
        let t0 = utc(2026, 1, 5, 0, 0);
        let btc_inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let (strategy, _) = recording_strategy(vec![
            vec![InstrumentAction::Adjust {
                instrument: btc_inst.clone(),
                target_qty: 1.0,
                reason: "rebalance".into(),
            }],
            vec![InstrumentAction::Adjust {
                instrument: btc_inst,
                target_qty: 2.0,
                reason: "rebalance".into(),
            }],
        ]);
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            strategy_config("15m", Some(2)),
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M15)],
            Box::new(strategy),
            None,
        )
        .unwrap();
        let inst = g.slots[0].parts.instrument.clone();
        let events: Vec<(usize, DateTime<Utc>, Bar)> = (0..30)
            .map(|i| {
                let t = t0 + Duration::minutes(i);
                (0usize, t, bar_at(&inst, t, 100.0))
            })
            .collect();
        drive(&mut g, &events);
        let summary = g.finish().unwrap();
        assert_eq!(summary.bars_processed, 30);
        assert_eq!(summary.actions, 2, "两轮 Adjust 各成一笔（首建 + 加仓）");
        assert_eq!(
            g.portfolio.rebalance_epoch, 1,
            "第二个 base 闭合 due 且成交 → epoch+1"
        );
        assert_eq!(
            g.portfolio.last_rebalance_at,
            Some(t0 + Duration::minutes(15)),
            "epoch 绑定第二个桶起点"
        );
        assert_eq!(g.bars_since_rebalance, 0, "epoch 递增后计数清零");
    }

    /// 启动首轮决策：预热已有闭合 base bar 时，不必等下一个 base TF 闭合，
    /// 实例化后即可调用策略一次（每实例一次）。
    #[test]
    fn prime_decision_round_runs_strategy_from_preloaded_base_bar() {
        let t0 = utc(2026, 1, 5, 0, 0);
        let (strategy, calls) = recording_strategy(vec![vec![]]);
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            strategy_config("15m", Some(96)),
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M15)],
            Box::new(strategy),
            None,
        )
        .unwrap();
        let inst = g.slots[0].parts.instrument.clone();
        let bars: Vec<Bar> = (0..15)
            .map(|i| bar_at(&inst, t0 + Duration::minutes(i), 100.0))
            .collect();
        g.preload(&[bars]).unwrap();
        let report = g
            .prime_decision_round()
            .unwrap()
            .expect("预热含闭合 base bar 时必须产出一轮");
        assert!(report.decision_round);
        assert_eq!(report.at, t0, "决策轮时间 = 最后一根 base bar 起点");
        assert_eq!(calls.lock().unwrap().len(), 1, "策略必须被调用一次");
        assert!(
            g.prime_decision_round().unwrap().is_none(),
            "同一实例只补一次，避免与实时闭合重复决策"
        );
    }

    /// 重启必须恢复「距上次再平衡的 base TF 计数」：否则每个进程重启都把
    /// 调仓窗口清零，`every_bars` 长于此周期时永远等不到 due（实盘空转）。
    #[test]
    fn recover_restores_bars_since_rebalance() {
        let dir = tempfile::tempdir().unwrap();
        let store1 = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
        ));
        let (strategy, _) = recording_strategy(vec![vec![], vec![], vec![]]);
        let cfg = strategy_config("15m", Some(5));
        let mut g1 = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg.clone(),
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M15)],
            Box::new(strategy),
            Some(store1.clone()),
        )
        .unwrap();
        let inst = g1.slots[0].parts.instrument.clone();
        let t0 = utc(2026, 1, 5, 0, 0);
        let events: Vec<(usize, DateTime<Utc>, Bar)> = (0..50)
            .map(|i| {
                let t = t0 + Duration::minutes(i);
                (0usize, t, bar_at(&inst, t, 100.0))
            })
            .collect();
        drive(&mut g1, &events);
        assert_eq!(g1.bars_since_rebalance, 3, "50 分钟 = 3 根 15m 闭合");
        store1.flush().unwrap();

        let store2 = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
        ));
        let (fresh, _) = recording_strategy(vec![vec![]]);
        let mut g2 = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M15)],
            Box::new(fresh),
            Some(store2.clone()),
        )
        .unwrap();
        g2.recover_from_store(&store2, Box::new(RecordingStrategy::new()), false)
            .unwrap();
        assert_eq!(
            g2.bars_since_rebalance, 3,
            "重启后计数必须从盘载恢复，而不是从 0 重等一轮"
        );
    }

    /// Ruling 1：Open fill 无匹配 PendingOpen 时，加权并入既有非 Closed 子仓位
    ///（不新建第二行，G1）。
    #[test]
    fn open_fill_without_matching_pending_adds_to_existing_sub() {
        let at = utc(2026, 1, 5, 0, 0);
        let mut slot = build_slot("BTCUSDT", 1000.0, Timeframe::M1);
        slot.sub_positions.push(SubPosition {
            id: "sub_existing".into(),
            strategy_id: "xsec_momentum:default".into(),
            master_id: "master_000001".into(),
            instrument: slot.parts.instrument.clone(),
            side: Side::Long,
            qty: 2.0,
            entry_price: 100.0,
            stop_price: None,
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: None,
            closed_at: None,
            realized_pnl: None,
        });
        let td: TradingDay = slot.parts.session.trading_day_of(at);
        let fill = Fill {
            order_id: "o1".into(),
            signal_id: "sub_unknown".into(),
            instrument: slot.parts.instrument.clone(),
            side: Side::Long,
            offset: Offset::Open,
            qty: 1.0,
            price: 103.0,
            fee: 0.0,
            realized_pnl: 0.0,
            margin_release: 0.0,
            at,
            trading_day: td,
            is_stop_loss: false,
            qty_raw: None,
        };
        assert!(apply_sub_fill(
            &mut slot,
            "xsec_momentum:default",
            "master_000001",
            &fill
        ));
        assert_eq!(slot.sub_positions.len(), 1, "G1：不得新建第二行");
        let s = &slot.sub_positions[0];
        assert!((s.qty - 3.0).abs() < 1e-12, "qty = 2+1 = 3");
        assert!(
            (s.entry_price - (200.0 + 103.0) / 3.0).abs() < 1e-12,
            "entry = (100*2 + 103*1)/3 = 101，实际 {}",
            s.entry_price
        );
        assert_eq!(s.state, SubPositionState::Active, "既有行状态保持");
        assert_eq!(s.id, "sub_existing", "并入既有行，不改 id");
    }

    /// Finding 1: L1 兜底必须服从禁交易窗口——no_trade 轮次即使触发硬止损也不得
    /// 发平仓（旧 runner 的整个 dispatch 以 `!ctx.no_trade` 门控）。
    #[test]
    fn l1_fallback_suppressed_in_no_trade_window() {
        let t0 = utc(2026, 1, 5, 0, 0);
        let btc = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let (strategy, _) = recording_strategy(vec![vec![InstrumentAction::Open {
            instrument: btc,
            side: Side::Long,
            price: 100.0,
            stop_price: 0.0,
            atr: 0.0,
            strength: 0.0,
            reason: "entry".into(),
        }]]);
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            strategy_config("1m", None),
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(strategy),
            None,
        )
        .unwrap();
        // 换桩：分钟 1 = 禁交易窗口；启用 L1（固定止损 20%，minute 1 收盘 80 会触发）。
        g.slots[0].parts.session = Arc::new(StubSession {
            inner: ContinuousSession::default(),
            no_trade_minute: 1,
        });
        g.slots[0].risk_guard = RiskGuard::with_config(RiskControlConfig {
            enabled: true,
            fixed_stop_loss_pct: 20.0,
            trailing_profit: TrailingProfitConfig::default(),
            fixed_take_profit_pct: 0.0,
        });
        let inst = g.slots[0].parts.instrument.clone();
        let t1 = t0 + Duration::minutes(1);
        let events = vec![
            (0usize, t0, bar_at(&inst, t0, 100.0)),
            (0usize, t1, bar_at(&inst, t1, 80.0)),
        ];
        let reports = drive(&mut g, &events);
        // reports[0]=None（t0 pending），reports[1] = flush t0（开仓）。
        let r0 = reports[1].as_ref().unwrap();
        assert_eq!(r0.actions, 1, "分钟 0（可交易）开仓受理");
        assert_eq!(r0.rejected, 0);
        let summary = g.finish().unwrap();
        // minute 1 轮次 no_trade：策略静默 + L1 被禁 → 不得产生平仓成交。
        assert_eq!(
            summary.fills.len(),
            1,
            "只有 1 笔 Open 成交，L1 平仓不得在禁窗内发出"
        );
        assert_eq!(summary.fills[0].offset, Offset::Open, "唯一成交是开仓");
        assert_eq!(g.slots[0].sub_positions[0].state, SubPositionState::Active);
        assert!(
            (g.slots[0].sub_positions[0].qty - 5.0).abs() < 1e-9,
            "子仓位保持满仓，未被 L1 减仓"
        );
        assert_eq!(summary.actions, 1);
        assert_eq!(summary.rejected_actions, 0);
    }

    /// Finding 2: 容量 clamp 在 `remaining <= 0` 时必须拒单（不得静默超发）。
    #[test]
    fn open_rejected_when_capacity_reached() {
        let t0 = utc(2026, 1, 5, 0, 0);
        let btc = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let eth = InstrumentId::perpetual(&ExchangeId::new("binance"), "ETHUSDT");
        let (strategy, _) = recording_strategy(vec![vec![
            InstrumentAction::Open {
                instrument: btc.clone(),
                side: Side::Long,
                price: 100.0,
                stop_price: 0.0,
                atr: 0.0,
                strength: 0.0,
                reason: "r1".into(),
            },
            InstrumentAction::Open {
                instrument: eth,
                side: Side::Long,
                price: 100.0,
                stop_price: 0.0,
                atr: 0.0,
                strength: 0.0,
                reason: "r2".into(),
            },
        ]]);
        let mut cfg = strategy_config("1m", None);
        // 容量 cap = 0.4 × nav；per_position_pct=0.5 单仓名义 0.5×nav → 第二个 Open 超容量。
        cfg.risk.max_total_notional = 0.4;
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            vec![
                build_slot("BTCUSDT", 500.0, Timeframe::M1),
                build_slot("ETHUSDT", 500.0, Timeframe::M1),
            ],
            Box::new(strategy),
            None,
        )
        .unwrap();
        let t1 = t0 + Duration::minutes(1);
        let inst0 = g.slots[0].parts.instrument.clone();
        let inst1 = g.slots[1].parts.instrument.clone();
        let events = vec![
            (0usize, t0, bar_at(&inst0, t0, 100.0)),
            (1usize, t0, bar_at(&inst1, t0, 100.0)),
            (0usize, t1, bar_at(&inst0, t1, 100.0)),
            (1usize, t1, bar_at(&inst1, t1, 100.0)),
        ];
        let reports = drive(&mut g, &events);
        let r0 = reports[2].as_ref().unwrap();
        assert_eq!(r0.actions, 1, "BTC 开仓受理（qty clamp 到 cap）");
        assert_eq!(r0.rejected, 1, "ETH 超容量拒单");
        let summary = g.finish().unwrap();
        assert_eq!(summary.actions, 1);
        assert_eq!(summary.rejected_actions, 1);
        assert_eq!(g.slots[0].sub_positions.len(), 1, "BTC 有开仓行");
        assert!(
            (g.slots[0].sub_positions[0].qty - 4.0).abs() < 1e-9,
            "qty = min(5, cap 400/(100*1)=4) = 4"
        );
        assert!(
            g.slots[1].sub_positions.is_empty(),
            "ETH 开仓被容量拒，不得建行"
        );
    }

    /// Finding 3: signal_resume 必须取自「上一根 bar」的 no_trade（先算再用）。
    #[test]
    fn signal_resume_uses_previous_bar_no_trade() {
        let t0 = utc(2026, 1, 5, 0, 0);
        let mut slot = build_slot("BTCUSDT", 1000.0, Timeframe::M1);
        slot.parts.session = Arc::new(StubSession {
            inner: ContinuousSession::default(),
            no_trade_minute: 1,
        });
        let session: Arc<dyn SessionRules> = slot.parts.session.clone();
        let inst = slot.parts.instrument.clone();

        // minute 0：no_trade=false，prev=false → resume=false。
        let b0 = bar_at(&inst, t0, 100.0);
        aggregate_and_gate_slot(&mut slot, &b0, session.trading_day_of(t0), session.as_ref())
            .unwrap();
        assert!(!slot.last_signal_resume);
        assert!(!slot.signal_blocked_prev);

        // minute 1：no_trade=true → resume 仍 false，但 blocked 置位（下一根读它）。
        let b1 = bar_at(&inst, t0 + Duration::minutes(1), 100.0);
        let t1 = t0 + Duration::minutes(1);
        aggregate_and_gate_slot(&mut slot, &b1, session.trading_day_of(t1), session.as_ref())
            .unwrap();
        assert!(!slot.last_signal_resume, "禁窗内 resume=false");
        assert!(slot.signal_blocked_prev, "禁窗已记录，供下一根");

        // minute 2：no_trade=false，上一根 no_trade=true → resume=true。
        let b2 = bar_at(&inst, t0 + Duration::minutes(2), 100.0);
        let t2 = t0 + Duration::minutes(2);
        aggregate_and_gate_slot(&mut slot, &b2, session.trading_day_of(t2), session.as_ref())
            .unwrap();
        assert!(slot.last_signal_resume, "禁窗解除后首根 resume=true");
        assert!(!slot.signal_blocked_prev);
    }

    /// Finding 4: entry 硬门对 Adjust 用 |target| vs |current|（幅值）——
    /// Short −2→−1 减仓放行，−2→−3 加仓拦截（entry_eligible=false 时）。
    #[test]
    fn adjust_entry_gate_short_reduce_allowed_add_rejected() {
        let t0 = utc(2026, 1, 5, 0, 0);
        // entry 规则引用未声明指标 adx → 特征缺失 → entry_eligible=false。
        let mut cfg = strategy_config("1m", None);
        cfg.entry = config::RuleSet {
            default: vec![config::Rule {
                ind: "adx".into(),
                op: config::RuleOp::Gt,
                value: config::RuleValue::Number(20.0),
            }],
            overrides: BTreeMap::new(),
        };
        let btc = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let (strategy, _) = recording_strategy(vec![
            vec![InstrumentAction::Adjust {
                instrument: btc.clone(),
                target_qty: -1.0,
                reason: "reduce".into(),
            }], // 减仓
            vec![InstrumentAction::Adjust {
                instrument: btc,
                target_qty: -3.0,
                reason: "add".into(),
            }], // 加仓
        ]);
        let mut slot0 = build_slot("BTCUSDT", 1000.0, Timeframe::M1);
        // 换 mock 出口：不校验撮合账本，预置子仓位可直接发减仓腿。
        slot0.parts.execution = Box::new(TestExecution {
            orders: Arc::new(Mutex::new(Vec::new())),
            fills: Mutex::new(Vec::new()),
            accept: true,
            venue_notional: None,
            remote: Vec::new(),
            in_flight: Vec::new(),
        });
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            vec![slot0],
            Box::new(strategy),
            None,
        )
        .unwrap();
        // 预置 Short Active qty=2（entry_eligible=false 下应只拦加仓）。
        let short_instr = g.slots[0].parts.instrument.clone();
        g.slots[0].sub_positions.push(SubPosition {
            id: "sub_short".into(),
            strategy_id: "xsec_momentum:default".into(),
            master_id: "master_000001".into(),
            instrument: short_instr,
            side: Side::Short,
            qty: 2.0,
            entry_price: 100.0,
            stop_price: None,
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: None,
            closed_at: None,
            realized_pnl: None,
        });
        let t1 = t0 + Duration::minutes(1);
        let t2 = t0 + Duration::minutes(2);
        let t3 = t0 + Duration::minutes(3);
        let inst = g.slots[0].parts.instrument.clone();
        let events = vec![
            (0usize, t0, bar_at(&inst, t0, 100.0)),
            (0usize, t1, bar_at(&inst, t1, 100.0)),
            (0usize, t2, bar_at(&inst, t2, 100.0)),
            (0usize, t3, bar_at(&inst, t3, 100.0)),
        ];
        let reports = drive(&mut g, &events);
        // reports[i] = 第 i 个事件返回的 flush 报告。
        let r0 = reports[1].as_ref().unwrap(); // flush t0 → Adjust −1（减仓）
        assert_eq!(r0.actions, 1, "Short −2→−1 减仓不受 entry 硬门限制");
        assert_eq!(r0.rejected, 0);
        let r1 = reports[2].as_ref().unwrap(); // flush t1 → Adjust −3（加仓）
        assert_eq!(r1.actions, 0, "Short −2→−3 加仓被 entry 硬门拦截");
        assert_eq!(r1.rejected, 1);
        let summary = g.finish().unwrap();
        assert_eq!(summary.actions, 1, "只有减仓腿成交");
        assert_eq!(summary.rejected_actions, 1, "加仓被拒一次");
        // 减仓成交后：Short 从 2 减到 1。
        let sub = &g.slots[0].sub_positions[0];
        assert!(
            (sub.qty - 1.0).abs() < 1e-9,
            "减仓 1 后在仓 qty=1，实际 {}",
            sub.qty
        );
        assert_eq!(sub.side, Side::Short);
    }

    // ─── Task 17：组合杠杆上限与按 NAV sizing ──────────────────────────────

    #[test]
    fn adjust_respects_cap() {
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.25; // cap = 0.25 × 1000 = 250
        cfg.sizing.per_position_pct = 0.2; // open qty = 1000×0.2/100 = 2（成本 200）
        let r0 = vec![opens_of("BTCUSDT", 100.0)];
        let r1 = vec![InstrumentAction::Adjust {
            instrument: match opens_of("BTCUSDT", 100.0) {
                InstrumentAction::Open { instrument, .. } => instrument,
                _ => unreachable!(),
            },
            target_qty: 3.0, // 想 +1，但剩余额度只有 50 → 裁到 +0.5
            reason: "want +1".into(),
        }];
        let (mut g, _calls) = pe_group2(cfg, &[r0, r1]);
        let reports = drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let _ = reports;
        let fills: Vec<(u8, f64)> = g
            .summary
            .fills
            .iter()
            .map(|f| (if f.offset == Offset::Open { 0u8 } else { 1u8 }, f.qty))
            .collect();
        assert_eq!(fills.len(), 2, "open 2.0 + 加仓裁到 0.5，实际 {fills:?}");
        assert!((fills[0].1 - 2.0).abs() < 1e-9);
        assert!(
            (fills[1].1 - 0.5).abs() < 1e-9,
            "加仓应被裁到剩余额度 0.5，实际 {:?}",
            fills[1]
        );
    }

    #[test]
    fn venue_open_uses_order_notional_as_shadow_qty() {
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.4; // cap 400
        cfg.sizing.per_position_pct = 0.5;
        let (mut g, _calls) = pe_group2(cfg, &[vec![opens_of("BTCUSDT", 100.0)]]);
        inject_venue(&mut g, 0, 200.0);
        drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let summary = g.finish().unwrap();
        assert_eq!(summary.rejected_actions, 0);
        assert_eq!(summary.fills.len(), 1);
        assert!(
            (summary.fills[0].qty - 2.0).abs() < 1e-9,
            "场所 200 / 100 = 2 张影子数量，实际 {:?}",
            summary.fills[0].qty
        );
    }

    #[test]
    fn venue_open_rejected_when_remaining_below_order_notional() {
        let mut cfg = strategy_config("1m", None);
        // 桩每槽固定权益 1000 → 双槽 nav 2000；cap 300，开仓 200 后剩 100。
        cfg.risk.max_total_notional = 0.15;
        cfg.sizing.per_position_pct = 0.5;
        let (mut g, _calls) = pe_group(
            cfg,
            &["BTCUSDT", "ETHUSDT"],
            vec![vec![opens_of("BTCUSDT", 100.0), opens_of("ETHUSDT", 100.0)]],
        );
        inject_venue(&mut g, 0, 200.0);
        inject_venue(&mut g, 1, 200.0);
        drive_prices(&mut g, &[&[100.0, 100.0], &[100.0, 100.0], &[100.0, 100.0]]);
        let summary = g.finish().unwrap();
        assert_eq!(summary.actions, 1, "BTC 按场所名义预留后占满容量");
        assert_eq!(
            summary.rejected_actions, 1,
            "ETH 剩余 50 < 场所单笔 200 必须拒"
        );
        assert_eq!(summary.fills.len(), 1);
        assert!(
            (summary.fills[0].qty - 2.0).abs() < 1e-9,
            "首个开仓影子数量 = 200/100，实际 {:?}",
            summary.fills[0].qty
        );
    }

    #[test]
    fn venue_open_not_rejected_when_nav_dips_below_order_budget() {
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.5;
        // nav 轻微回落到预算线以下（199 < 场所 250）不应转为每轮拒单；
        // 单仓预算在启动期按 initial_cash_total 校验。
        cfg.sizing.per_position_pct = 0.199;
        let (mut g, _calls) = pe_group2(cfg, &[vec![opens_of("BTCUSDT", 100.0)]]);
        inject_venue(&mut g, 0, 250.0);
        drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let summary = g.finish().unwrap();
        assert_eq!(summary.rejected_actions, 0, "NAV 回落不应导致拒单");
        assert_eq!(summary.fills.len(), 1);
        assert!(
            (summary.fills[0].qty - 2.5).abs() < 1e-9,
            "场所 250 / 100 = 2.5 张影子数量，实际 {:?}",
            summary.fills[0].qty
        );
    }

    #[test]
    fn venue_adjust_from_empty_uses_order_notional() {
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.5;
        // nav 轻微回落：199 < 场所 200，不应拒绝空仓 Adjust。
        cfg.sizing.per_position_pct = 0.199;
        let adjust = InstrumentAction::Adjust {
            instrument: match opens_of("BTCUSDT", 100.0) {
                InstrumentAction::Open { instrument, .. } => instrument,
                _ => unreachable!(),
            },
            target_qty: 5.0,
            reason: "empty adjust".into(),
        };
        let (mut g, _calls) = pe_group2(cfg, &[vec![adjust]]);
        inject_venue(&mut g, 0, 200.0);
        drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let summary = g.finish().unwrap();
        assert_eq!(summary.rejected_actions, 0);
        assert_eq!(summary.fills.len(), 1);
        assert!(
            (summary.fills[0].qty - 2.0).abs() < 1e-9,
            "空仓 Adjust 按场所名义开 2 张，实际 {:?}",
            summary.fills[0].qty
        );
    }

    #[test]
    fn venue_adjust_add_below_order_notional_is_noop() {
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.5;
        cfg.sizing.per_position_pct = 0.5;
        let r0 = vec![opens_of("BTCUSDT", 100.0)];
        let r1 = vec![InstrumentAction::Adjust {
            instrument: match opens_of("BTCUSDT", 100.0) {
                InstrumentAction::Open { instrument, .. } => instrument,
                _ => unreachable!(),
            },
            target_qty: 2.1, // 与影子持仓 2.0 的差额 10 < 场所单笔 200
            reason: "tiny add".into(),
        }];
        let (mut g, _calls) = pe_group2(cfg, &[r0, r1]);
        inject_venue(&mut g, 0, 200.0);
        drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let summary = g.finish().unwrap();
        assert_eq!(summary.rejected_actions, 0, "差额小于场所单笔不算拒单");
        assert_eq!(summary.fills.len(), 1, "仅开仓成交，微小加仓视为已达标");
    }

    #[test]
    fn venue_adjust_add_skipped_when_remaining_below_order_notional() {
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.3; // cap 300，开仓 200 后剩余 100
        cfg.sizing.per_position_pct = 0.5;
        let r0 = vec![opens_of("BTCUSDT", 100.0)];
        let r1 = vec![InstrumentAction::Adjust {
            instrument: match opens_of("BTCUSDT", 100.0) {
                InstrumentAction::Open { instrument, .. } => instrument,
                _ => unreachable!(),
            },
            target_qty: 6.0, // 想加 4 张（400）> 场所单笔 200
            reason: "add beyond remaining".into(),
        }];
        let (mut g, _calls) = pe_group2(cfg, &[r0, r1]);
        inject_venue(&mut g, 0, 200.0);
        drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let summary = g.finish().unwrap();
        assert_eq!(summary.rejected_actions, 0, "容量不足的加仓静默跳过");
        assert_eq!(summary.fills.len(), 1, "仅开仓成交");
    }

    #[test]
    fn venue_adjust_add_at_order_notional_is_submitted() {
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.5; // cap 500，开仓 200 后剩余 300
        cfg.sizing.per_position_pct = 0.5;
        let r0 = vec![opens_of("BTCUSDT", 100.0)];
        let r1 = vec![InstrumentAction::Adjust {
            instrument: match opens_of("BTCUSDT", 100.0) {
                InstrumentAction::Open { instrument, .. } => instrument,
                _ => unreachable!(),
            },
            target_qty: 4.0, // 影子 2.0 → 差额 2 张 = 场所单笔 200
            reason: "add one venue lot".into(),
        }];
        let (mut g, _calls) = pe_group2(cfg, &[r0, r1]);
        inject_venue(&mut g, 0, 200.0);
        drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let summary = g.finish().unwrap();
        assert_eq!(summary.rejected_actions, 0);
        assert_eq!(summary.fills.len(), 2);
        assert_eq!(summary.fills[1].offset, Offset::Open);
        assert!(
            (summary.fills[1].qty - 2.0).abs() < 1e-9,
            "加仓腿按场所单笔 2 张，实际 {:?}",
            summary.fills[1].qty
        );
    }

    #[test]
    fn adjust_on_inflight_sub_is_deferred_not_rejected() {
        // 实盘成交回补可能跨下一个调仓轮：在途开仓（OpenRequested）无法加仓，
        // 应静默延后到成交后重算目标；计入 rejected 会让启动期每轮都出现假拒单。
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.5;
        cfg.sizing.per_position_pct = 0.5;
        let adjust = InstrumentAction::Adjust {
            instrument: match opens_of("BTCUSDT", 100.0) {
                InstrumentAction::Open { instrument, .. } => instrument,
                _ => unreachable!(),
            },
            target_qty: 5.0,
            reason: "adjust while open in flight".into(),
        };
        let (mut g, _calls) = pe_group2(cfg, &[vec![adjust]]);
        let inst = g.slots[0].parts.instrument.clone();
        let mut sub = active_sub_of(&inst, "sub_inflight", Side::Long, 2.0);
        sub.state = SubPositionState::OpenRequested;
        g.slots[0].sub_positions.push(sub);
        insert_master(
            &mut g,
            "master_000001",
            MasterPositionState::Open,
            utc(2026, 1, 5, 0, 0),
        );
        let reports = drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let rejected: u64 = reports.iter().map(|r| r.rejected).sum();
        assert_eq!(rejected, 0, "在途开仓的调仓应静默延后，不计拒单");
        let summary = g.finish().unwrap();
        assert!(summary.fills.is_empty(), "在途期间不得追加下单");
        assert_eq!(
            summary.final_sub_positions[0].state,
            SubPositionState::OpenRequested,
            "在途子仓状态不得被调仓改写"
        );
    }

    #[test]
    fn venue_reduce_below_qty_step_is_noop_not_rejected() {
        // 场所数量步长 0.001：目标 2.999823 与持仓 3.0 的差额 0.000177
        // 低于一步，交易所无法表达。应按已达标静默跳过，而不是发单被
        // QuantityOutOfRange 拒绝后把信号打入 Failed 终态（fix5 实盘 13:47）。
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.5;
        cfg.sizing.per_position_pct = 0.5;
        let adjust = InstrumentAction::Adjust {
            instrument: match opens_of("BTCUSDT", 100.0) {
                InstrumentAction::Open { instrument, .. } => instrument,
                _ => unreachable!(),
            },
            target_qty: 2.999823,
            reason: "reduce below venue step".into(),
        };
        let (mut g, _calls) = pe_group2(cfg, &[vec![adjust]]);
        let inst = g.slots[0].parts.instrument.clone();
        let mut spec = InstrumentSpec::crypto_perpetual(0.0);
        spec.qty_step = 0.001;
        spec.min_qty = 0.001;
        spec.qty_precision = 3;
        g.slots[0].parts.trading = Arc::new(SpecDrivenTradingRules::new([(inst.clone(), spec)]));
        g.slots[0]
            .sub_positions
            .push(active_sub_of(&inst, "sub_step", Side::Long, 3.0));
        insert_master(
            &mut g,
            "master_000001",
            MasterPositionState::Open,
            utc(2026, 1, 5, 0, 0),
        );
        let reports = drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let actions: u64 = reports.iter().map(|r| r.actions).sum();
        let rejected: u64 = reports.iter().map(|r| r.rejected).sum();
        assert_eq!(actions, 0, "低于场所步长的减仓差额不得发单");
        assert_eq!(rejected, 0, "这是已达标，不是拒单");
        let summary = g.finish().unwrap();
        assert!(summary.fills.is_empty(), "不得产生成交");
        assert!((summary.final_sub_positions[0].qty - 3.0).abs() < 1e-12);
    }

    #[test]
    fn open_respects_sub_position_cap() {
        let mut cfg = strategy_config("1m", None);
        cfg.sizing.per_position_pct = 0.5; // 单价名义 5 张
        cfg.sizing.max_position_pct = Some(0.2); // 子仓硬上限 2 张
        let r0 = vec![opens_of("BTCUSDT", 100.0)];
        let (mut g, _calls) = pe_group2(cfg, &[r0]);
        drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let fills: Vec<f64> = g.summary.fills.iter().map(|f| f.qty).collect();
        assert_eq!(fills.len(), 1);
        assert!(
            (fills[0] - 2.0).abs() < 1e-9,
            "0.2×1000/100=2 张，实际 {fills:?}"
        );
    }

    #[test]
    fn total_cap_and_sub_cap_take_smaller_qty() {
        let mut cfg = strategy_config("1m", None);
        cfg.sizing.per_position_pct = 0.5;
        cfg.sizing.max_position_pct = Some(0.6);
        cfg.risk.max_total_notional = 0.15; // 总仓容量 1.5 张，小于子仓上限 6 张
        let r0 = vec![opens_of("BTCUSDT", 100.0)];
        let (mut g, _calls) = pe_group2(cfg, &[r0]);
        drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let fills: Vec<f64> = g.summary.fills.iter().map(|f| f.qty).collect();
        assert_eq!(fills.len(), 1);
        assert!(
            (fills[0] - 1.5).abs() < 1e-9,
            "总仓与子仓上限同时触发取更小值 1.5，实际 {fills:?}"
        );
    }

    #[test]
    fn adjust_add_respects_sub_position_cap() {
        let mut cfg = strategy_config("1m", None);
        cfg.sizing.per_position_pct = 0.2; // 开仓 2 张
        cfg.sizing.max_position_pct = Some(0.25); // 子仓上限 2.5 张
        let instrument = match opens_of("BTCUSDT", 100.0) {
            InstrumentAction::Open { instrument, .. } => instrument,
            _ => unreachable!(),
        };
        let r0 = vec![opens_of("BTCUSDT", 100.0)];
        let r1 = vec![InstrumentAction::Adjust {
            instrument,
            target_qty: 3.0, // 想加到 3，被硬上限裁到 2.5
            reason: "want 3".into(),
        }];
        let (mut g, _calls) = pe_group2(cfg, &[r0, r1]);
        drive_prices(&mut g, &[&[100.0], &[100.0], &[100.0], &[100.0]]);
        let fills: Vec<f64> = g.summary.fills.iter().map(|f| f.qty).collect();
        assert_eq!(fills.len(), 2, "开 2 + 加仓裁到 0.5，实际 {fills:?}");
        assert!((fills[0] - 2.0).abs() < 1e-9);
        assert!(
            (fills[1] - 0.5).abs() < 1e-9,
            "加仓应裁到 2.5，实际增量 {:?}",
            fills[1]
        );
    }

    #[test]
    fn nav_dedupes_repeated_virtual_cash() {
        let cfg = strategy_config("1m", None);
        let slots = vec![
            build_slot_cfg("BTCUSDT", 1000.0, &cfg),
            build_slot_cfg("ETHUSDT", 1000.0, &cfg),
        ];
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            slots,
            Box::new(RecordingStrategy::new()),
            None,
        )
        .unwrap();
        g.set_initial_cash_total(1000.0);
        assert!(
            (g.current_nav() - 1000.0).abs() < 1e-9,
            "两个 slot 各虚拟起账 1000，去重后 NAV=1000"
        );
        assert!((g.portfolio.nav - 1000.0).abs() < 1e-9);
        assert!((g.portfolio.high_watermark - 1000.0).abs() < 1e-9);
        assert!(g.portfolio.drawdown.abs() < 1e-12);
    }

    fn pe_group2(
        cfg: StrategyConfig,
        responses: &[Vec<InstrumentAction>],
    ) -> (CrossSectionalRunnerGroup, RecordingCalls) {
        let mut strategy = RecordingStrategy::new();
        for r in responses {
            strategy.queue(r.clone());
        }
        let calls = strategy.calls.clone();
        let slots = vec![build_slot_cfg("BTCUSDT", 1000.0, &cfg)];
        let g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            slots,
            Box::new(strategy),
            None,
        )
        .unwrap();
        (g, calls)
    }

    // ─── Task 18：两阶段再平衡（Phase 1 减仓 → 确认 → Phase 2 加仓）────────

    fn rotate_cfg() -> StrategyConfig {
        let mut cfg = strategy_config("1m", None);
        cfg.rebalance = Some(config::RebalanceCfg {
            every_bars: 2,
            top_k: 1,
            bottom_k: 0,
            weight_scheme: config::WeightScheme::Equal,
        });
        cfg
    }

    #[test]
    fn rebalance_reduces_before_adds_in_separate_rounds() {
        let cfg = rotate_cfg(); // base 1m, every_bars=1
        let eth_zero = InstrumentAction::Adjust {
            instrument: {
                let ex = BinanceExchange::new(0.0);
                ex.parse_instrument("ETHUSDT").unwrap()
            },
            target_qty: 0.0,
            reason: "rotate out".into(),
        };
        let (mut g, _calls) = pe_group(
            cfg.clone(),
            &["BTCUSDT", "ETHUSDT"],
            vec![
                vec![opens_of("ETHUSDT", 100.0)],
                vec![opens_of("BTCUSDT", 100.0), eth_zero],
            ],
        );
        let base = utc(2026, 1, 5, 0, 0);
        for m in 0..40 {
            let at = base + Duration::minutes(m);
            for slot in 0..2 {
                let inst = g.slots[slot].parts.instrument.clone();
                g.on_event(AlignedEvent {
                    at,
                    slot,
                    event: MarketEvent::Bar(bar_at(&inst, at, 100.0)),
                })
                .unwrap();
            }
        }
        let _ = g.finish().unwrap();
        let order: Vec<(&str, &str)> = g
            .summary
            .fills
            .iter()
            .map(|f| {
                (
                    f.instrument.symbol.as_str(),
                    if f.offset == Offset::Open {
                        "open"
                    } else {
                        "close"
                    },
                )
            })
            .collect();
        let eth_close = order
            .iter()
            .position(|(s, k)| *s == "ETHUSDT" && *k == "close");
        let btc_open = order
            .iter()
            .position(|(s, k)| *s == "BTCUSDT" && *k == "open");
        assert!(
            eth_close.is_some() && btc_open.is_some(),
            "应有 ETH 平仓与 BTC 加仓两笔：{order:?}"
        );
        assert!(
            eth_close < btc_open,
            "Phase 1 减仓成交必须先于 Phase 2 加仓成交：{order:?}"
        );
        assert_eq!(g.portfolio.rebalance_epoch, 1, "Phase 2 完成 → epoch+1");
    }

    #[test]
    fn l3_trigger_during_pending_rebalance_drops_adds() {
        let cfg = pe_cfg(None, Some(8.0), false); // stop_loss 8%，无轮换（走非 due 直派）
        let (mut g, _calls) = pe_group(
            cfg,
            &["BTCUSDT", "ETHUSDT"],
            vec![vec![opens_of("ETHUSDT", 100.0)]],
        );
        let base = utc(2026, 1, 5, 0, 0);
        for slot in 0..2 {
            let inst = g.slots[slot].parts.instrument.clone();
            g.on_event(AlignedEvent {
                at: base,
                slot,
                event: MarketEvent::Bar(bar_at(&inst, base, 100.0)),
            })
            .unwrap();
        }
        let t1 = base + Duration::minutes(1);
        for slot in 0..2 {
            let inst = g.slots[slot].parts.instrument.clone();
            g.on_event(AlignedEvent {
                at: t1,
                slot,
                event: MarketEvent::Bar(bar_at(&inst, t1, if slot == 1 { 91.0 } else { 100.0 })),
            })
            .unwrap();
        }
        // flush t1（在 t2 到达）之前种 pending：②L3（ETH -9%）抢跑并作废排队。
        g.pending_adds = vec![(opens_of("BTCUSDT", 91.0), true)];
        let t2 = base + Duration::minutes(2);
        for slot in 0..2 {
            let inst = g.slots[slot].parts.instrument.clone();
            g.on_event(AlignedEvent {
                at: t2,
                slot,
                event: MarketEvent::Bar(bar_at(&inst, t2, 91.0)),
            })
            .unwrap();
        }
        let t3 = base + Duration::minutes(3);
        for slot in 0..2 {
            let inst = g.slots[slot].parts.instrument.clone();
            g.on_event(AlignedEvent {
                at: t3,
                slot,
                event: MarketEvent::Bar(bar_at(&inst, t3, 91.0)),
            })
            .unwrap();
        }
        let _ = g.finish().unwrap();
        assert!(g.pending_adds.is_empty(), "L3 触发时 pending 加仓作废");
        let btc_opens = g
            .summary
            .fills
            .iter()
            .filter(|f| f.instrument.symbol == "BTCUSDT" && f.offset == Offset::Open)
            .count();
        assert_eq!(btc_opens, 0, "L3 抢跑窗口不得先加后平");
        let eth_closes = g
            .summary
            .fills
            .iter()
            .filter(|f| f.instrument.symbol == "ETHUSDT" && f.offset != Offset::Open)
            .count();
        assert_eq!(eth_closes, 1, "L3 全平恰一笔");
        assert_eq!(g.phase, PortfolioPhase::Paused, "清零后 Paused");
    }

    // ─── Task 16：L3 portfolio_exit 与 N=1 退化等价 ────────────────────────

    fn pe_cfg(trailing: Option<(f64, f64)>, stop: Option<f64>, exit_eth: bool) -> StrategyConfig {
        let mut cfg = strategy_config("1m", None);
        cfg.portfolio_exit = config::PortfolioExitCfg {
            trailing: trailing.map(|(a, g)| config::TrailingCfg {
                arm_pnl_pct: a,
                giveback_pct: g,
            }),
            stop_loss_pct: stop,
        };
        if exit_eth {
            cfg.indicators.insert(
                "sma".into(),
                config::IndicatorCfg {
                    tf: "1m".into(),
                    params: std::collections::BTreeMap::from_iter([(
                        "period".into(),
                        serde_json::json!(1),
                    )]),
                },
            );
            // sma < 95：t0（100）不触发，t1（91）触发——与 L3 同轮竞争，验证 L3 抢跑。
            cfg.exit.overrides.insert(
                "ETHUSDT".into(),
                vec![config::Rule {
                    ind: "sma".into(),
                    op: config::RuleOp::Lt,
                    value: config::RuleValue::Number(95.0),
                }],
            );
        }
        cfg
    }

    fn pe_group(
        cfg: StrategyConfig,
        syms: &[&str],
        responses: Vec<Vec<InstrumentAction>>,
    ) -> (CrossSectionalRunnerGroup, RecordingCalls) {
        let mut strategy = RecordingStrategy::new();
        for r in responses {
            strategy.queue(r);
        }
        let calls = strategy.calls.clone();
        let slots = syms
            .iter()
            .map(|sym| build_slot_cfg(sym, 1000.0 / syms.len() as f64, &cfg))
            .collect::<Vec<_>>();
        let g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            slots,
            Box::new(strategy),
            None,
        )
        .unwrap();
        (g, calls)
    }

    fn drive_prices(g: &mut CrossSectionalRunnerGroup, prices: &[&[f64]]) -> Vec<RoundReport> {
        let t0 = utc(2026, 1, 5, 0, 0);
        let mut out = Vec::new();
        for (i, ps) in prices.iter().enumerate() {
            for (slot, price) in ps.iter().enumerate() {
                let inst = g.slots[slot].parts.instrument.clone();
                let at = t0 + Duration::minutes(i as i64);
                let r = g
                    .on_event(AlignedEvent {
                        at,
                        slot,
                        event: MarketEvent::Bar(bar_at(&inst, at, *price)),
                    })
                    .unwrap();
                if let Some(r) = r {
                    out.push(r);
                }
            }
        }
        out
    }

    #[test]
    fn n1_portfolio_exit_matches_legacy_trailing_exactly() {
        let cfg = pe_cfg(Some((10.0, 40.0)), None, false);
        let btc_open = opens_of("BTCUSDT", 100.0);
        let (mut g, _calls) = pe_group(cfg, &["BTCUSDT"], vec![vec![btc_open]]);
        let reports = drive_prices(&mut g, &[&[100.0], &[120.0], &[110.0], &[110.0]]);
        // t2（110）：旧口径 max_pnl 20% → trigger 12% ≥ pnl 10% 触发；组合口径
        // peak=100 金额、giveback 40% → 60 ≥ unrealized 50 同根触发。
        let fire = reports
            .iter()
            .find(|r| r.actions == 1 && r.at == utc(2026, 1, 5, 0, 2))
            .expect("t2 恰一次 CloseAll（与旧 trail 同根）");
        let _ = fire;
        // 旧 RiskController 对照：同参数逐根回放，触发索引必须一致。
        let rc = strategy_core::risk::RiskController::new(strategy_core::risk::RiskControlConfig {
            enabled: true,
            fixed_stop_loss_pct: 1000.0,
            trailing_profit: strategy_core::risk::TrailingProfitConfig {
                enabled: true,
                activation_pct: 10.0,
                drawdown_pct: 40.0,
            },
            fixed_take_profit_pct: 0.0,
        });
        let mut rs = strategy_core::risk::RiskState::default();
        let mut legacy_fire = None;
        for (i, p) in [100.0, 120.0, 110.0, 110.0].iter().enumerate() {
            let pnl = (p - 100.0) / 100.0 * 100.0;
            let max_pnl = if p > &100.0 {
                20.0_f64.max(pnl)
            } else {
                pnl.max(0.0)
            };
            let mx = max_pnl.max(0.0);
            if rc.check_exit(pnl, mx, &mut rs).is_some() {
                legacy_fire = Some(i);
                break;
            }
        }
        assert_eq!(legacy_fire, Some(2), "旧 trail 也在第 3 根（t2）触发");
        // close fill 落地（t3 sync）→ Closed
        drive_prices(&mut g, &[&[110.0]]);
        assert!(g.slots[0]
            .sub_positions
            .iter()
            .all(|s| s.state == SubPositionState::Closed));
    }

    #[test]
    fn portfolio_stop_loss_triggers_close_all() {
        let cfg = pe_cfg(None, Some(8.0), false);
        let btc_open = opens_of("BTCUSDT", 100.0);
        let (mut g, calls) = pe_group(cfg, &["BTCUSDT"], vec![vec![btc_open], vec![], vec![]]);
        let reports = drive_prices(&mut g, &[&[100.0], &[91.0], &[91.0], &[91.0]]);
        let fire = reports
            .iter()
            .find(|r| r.actions == 1 && r.at == utc(2026, 1, 5, 0, 1))
            .expect("t1 CloseAll");
        let _ = fire;
        assert_eq!(g.phase, PortfolioPhase::Paused, "清零后 Paused");
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "L3 在 ② 段先触发，策略自 r1 起被门控（仅 r0 一次）"
        );
        let f = &g.summary.fills[g.summary.fills.len() - 1];
        assert_eq!(f.at, utc(2026, 1, 5, 0, 1), "close 于 t1 轮提交");
    }

    #[test]
    fn l3_precedes_l2_and_strategy_and_suppresses_both() {
        let cfg = pe_cfg(None, Some(8.0), true); // ETH exit 恒真 + 组合止损 8%
        let r0 = vec![opens_of("BTCUSDT", 100.0), opens_of("ETHUSDT", 100.0)];
        let r1 = vec![opens_of("BTCUSDT", 91.0)]; // 策略还想开 → 必须整轮被抑制
        let (mut g, calls) = pe_group(cfg, &["BTCUSDT", "ETHUSDT"], vec![r0, r1]);
        let reports = drive_prices(
            &mut g,
            &[&[100.0, 100.0], &[91.0, 91.0], &[91.0, 91.0], &[91.0, 91.0]],
        );
        let fire = reports
            .iter()
            .find(|r| r.at == utc(2026, 1, 5, 0, 1))
            .expect("t1 决策轮");
        assert_eq!(
            fire.actions, 2,
            "只发 2 腿 CloseAll（BTC+ETH），无 ExitRule 单"
        );
        assert_eq!(fire.rejected, 0, "策略整轮未调用，无抑制计数");
        // ExitRule 意图的 base id 不得出现在任何 fill
        for f in &g.summary.fills {
            let expect_exit_rule = crate::strategy::signal_id::generate_sub_signal_id(
                "xsec_momentum",
                "xsec_momentum:default",
                &f.instrument.namespaced_key(),
                utc(2026, 1, 5, 0, 1),
                Side::Short,
                crate::strategy::signal_id::SubIntent::ExitRule,
                0,
            );
            assert!(
                !f.signal_id.starts_with(&expect_exit_rule),
                "L3 抢跑后不得出现 exit_rule 单：{}",
                f.signal_id
            );
        }
        assert_eq!(calls.lock().unwrap().len(), 1, "t1 起策略被 l3 门控");
    }

    #[test]
    fn liquidating_blocks_new_opens_until_flat() {
        let cfg = pe_cfg(None, Some(8.0), false);
        let r0 = vec![opens_of("BTCUSDT", 100.0)];
        let r_again = || vec![opens_of("BTCUSDT", 91.0)];
        let (mut g, calls) = pe_group(
            cfg,
            &["BTCUSDT"],
            vec![r0, r_again(), r_again(), Vec::new()],
        );
        // t0 开仓；t1 触发 L3（close 已提交未成交）；t2 同步 close fill。
        let t0 = utc(2026, 1, 5, 0, 0);
        let inst = g.slots[0].parts.instrument.clone();
        for (i, price) in [(0, 100.0), (1, 91.0), (2, 91.0), (3, 91.0)] {
            let at = t0 + Duration::minutes(i);
            g.on_event(AlignedEvent {
                at,
                slot: 0,
                event: MarketEvent::Bar(bar_at(&inst, at, price)),
            })
            .unwrap();
        }
        let _ = g.finish().unwrap();
        // 策略只被调用 1 次（t0）；t1 起 Liquidating/Paused 门控，
        // 队列里的 Open（含 t1 的重复开仓企图）从未被消费。
        assert_eq!(calls.lock().unwrap().len(), 1);
        let opens: Vec<_> = g
            .summary
            .fills
            .iter()
            .filter(|f| f.offset == Offset::Open)
            .collect();
        assert_eq!(opens.len(), 1, "清算期间不得新增开仓");
    }

    // ─── Task 15：L2 标的级 exit 规则接入 ──────────────────────────────────

    /// 带 sma(1m, period1) 指标 + ETH exit 规则（sma > 120）的组合配置。
    fn exit_config() -> StrategyConfig {
        let mut cfg = strategy_config("1m", None);
        cfg.indicators.insert(
            "sma".into(),
            config::IndicatorCfg {
                tf: "1m".into(),
                params: std::collections::BTreeMap::from_iter([(
                    "period".into(),
                    serde_json::json!(1),
                )]),
            },
        );
        cfg.exit.overrides.insert(
            "ETHUSDT".into(),
            vec![config::Rule {
                ind: "sma".into(),
                op: config::RuleOp::Gt,
                value: config::RuleValue::Number(120.0),
            }],
        );
        cfg
    }

    fn build_slot_cfg(symbol: &str, cash: f64, cfg: &StrategyConfig) -> StrategySlot {
        let ex = BinanceExchange::new(0.0);
        let inst = ex.parse_instrument(symbol).unwrap();
        let feature_plan = FeaturePlan::from_config(cfg).unwrap();
        let parts = ex
            .build(&SourceRequest::backtest(inst.clone(), Vec::new(), cash))
            .unwrap();
        StrategySlot::new(SlotInit {
            parts,
            timeframes: vec![Timeframe::M1],
            feature_plan,
            risk: RiskControlConfig {
                enabled: false,
                ..RiskControlConfig::default()
            },
            strategy_type: "xsec_momentum".to_string(),
            max_closed_len: 300,
        })
    }

    fn opens_of(key: &str, price: f64) -> InstrumentAction {
        let ex = BinanceExchange::new(0.0);
        let inst = ex
            .parse_instrument(key.strip_prefix("binance/crypto_perpetual/").unwrap_or(key))
            .unwrap();
        InstrumentAction::Open {
            instrument: inst,
            side: Side::Long,
            price,
            stop_price: price * 0.98,
            atr: 0.0,
            strength: 1.0,
            reason: "scripted".into(),
        }
    }

    fn exit_group(
        cfg: StrategyConfig,
        responses: Vec<Vec<InstrumentAction>>,
    ) -> (CrossSectionalRunnerGroup, RecordingCalls) {
        let mut strategy = RecordingStrategy::new();
        for r in responses {
            strategy.queue(r);
        }
        let calls = strategy.calls.clone();
        let slots = vec![
            build_slot_cfg("BTCUSDT", 500.0, &cfg),
            build_slot_cfg("ETHUSDT", 500.0, &cfg),
        ];
        let g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            slots,
            Box::new(strategy),
            None,
        )
        .unwrap();
        (g, calls)
    }

    #[test]
    fn exit_rule_closes_only_its_instrument_and_suppresses_strategy_open() {
        let cfg = exit_config();
        let eth_open = opens_of("ETHUSDT", 100.0);
        let btc_open = opens_of("BTCUSDT", 130.0);
        let eth_open_again = opens_of("ETHUSDT", 130.0);
        let (mut g, calls) = exit_group(
            cfg.clone(),
            vec![vec![eth_open], vec![btc_open, eth_open_again]],
        );
        let inst_btc = g.slots[0].parts.instrument.clone();
        let inst_eth = g.slots[1].parts.instrument.clone();
        let t0 = utc(2026, 1, 5, 0, 0);
        let events = [
            (0usize, t0, bar_at(&inst_btc, t0, 100.0)),
            (1usize, t0, bar_at(&inst_eth, t0, 100.0)),
            (
                0usize,
                t0 + Duration::minutes(1),
                bar_at(&inst_btc, t0 + Duration::minutes(1), 130.0),
            ),
            (
                1usize,
                t0 + Duration::minutes(1),
                bar_at(&inst_eth, t0 + Duration::minutes(1), 130.0),
            ),
            (
                0usize,
                t0 + Duration::minutes(2),
                bar_at(&inst_btc, t0 + Duration::minutes(2), 130.0),
            ),
        ];
        let mut r1 = None;
        for (slot, at, bar) in events {
            let r = g
                .on_event(AlignedEvent {
                    at,
                    slot,
                    event: MarketEvent::Bar(bar),
                })
                .unwrap();
            if r.as_ref()
                .is_some_and(|x| x.at == t0 + Duration::minutes(1))
            {
                r1 = r;
            }
        }
        let r1 = r1.expect("t1 决策轮报告");
        // t1 轮：先同步 t0 的 ETH open fill；L2 触发 → ETH close(1 腿) + BTC open
        assert_eq!(r1.actions, 2, "close(ETH)+open(BTC)，实际 {r1:?}");
        assert_eq!(r1.rejected, 1, "ETH 的重复 Open 被 L2 抑制");
        let eth_exit_visible = {
            let calls_guard = calls.lock().unwrap();
            calls_guard[1]
                .iter()
                .find(|s| s.instrument.symbol == "ETHUSDT")
                .and_then(|s| s.exit_triggered.clone())
        };
        assert!(eth_exit_visible.is_some(), "策略可见 exit_triggered（§8）");
        let eth_sub = g.slots[1]
            .sub_positions
            .iter()
            .find(|s| s.state != SubPositionState::Closed)
            .expect("ETH 子仓存在");
        assert_eq!(eth_sub.state, SubPositionState::CloseRequested);
    }

    #[test]
    fn empty_exit_rules_never_trigger() {
        let cfg = strategy_config("1m", None);
        let btc_open = opens_of("BTCUSDT", 100.0);
        let (mut g, _calls) = exit_group(
            cfg.clone(),
            vec![vec![btc_open.clone(), opens_of("ETHUSDT", 100.0)]],
        );
        let inst_btc = g.slots[0].parts.instrument.clone();
        let inst_eth = g.slots[1].parts.instrument.clone();
        let t0 = utc(2026, 1, 5, 0, 0);
        let t1 = t0 + Duration::minutes(1);
        for (slot, at, bar) in [
            (0usize, t0, bar_at(&inst_btc, t0, 100.0)),
            (1usize, t0, bar_at(&inst_eth, t0, 100.0)),
            (0usize, t1, bar_at(&inst_btc, t1, 100.0)),
        ] {
            g.on_event(AlignedEvent {
                at,
                slot,
                event: MarketEvent::Bar(bar),
            })
            .unwrap();
        }
        assert_eq!(g.slots[0].sub_positions.len(), 1, "default=[] 不抑制开仓");
        assert_eq!(g.slots[1].sub_positions.len(), 1);
        assert!(g.slots[0]
            .sub_positions
            .iter()
            .all(|s| s.state == SubPositionState::OpenRequested));
    }

    #[test]
    fn exit_and_strategy_close_do_not_double_close() {
        let cfg = exit_config();
        let eth_open = opens_of("ETHUSDT", 100.0);
        let (mut g, _calls) = exit_group(cfg.clone(), vec![vec![eth_open], Vec::new()]);
        let inst_btc = g.slots[0].parts.instrument.clone();
        let inst_eth = g.slots[1].parts.instrument.clone();
        let t0 = utc(2026, 1, 5, 0, 0);
        let t1 = t0 + Duration::minutes(1);
        let t2 = t0 + Duration::minutes(2);
        let mut close_reports = Vec::new();
        for (slot, at, bar) in [
            (0usize, t0, bar_at(&inst_btc, t0, 100.0)),
            (1usize, t0, bar_at(&inst_eth, t0, 100.0)),
            (0usize, t1, bar_at(&inst_btc, t1, 130.0)),
            (1usize, t1, bar_at(&inst_eth, t1, 130.0)),
            // 决策轮 t2 前：ETH 已在 t1 被 L2 置 CloseRequested；再来一轮高价的
            // t2（exit 继续命中），close_position 的防重复守卫必须跳过二次平仓。
            (0usize, t2, bar_at(&inst_btc, t2, 130.0)),
            (1usize, t2, bar_at(&inst_eth, t2, 130.0)),
            (
                0usize,
                t2 + Duration::minutes(1),
                bar_at(&inst_btc, t2 + Duration::minutes(1), 130.0),
            ),
        ] {
            if let Some(r) = g
                .on_event(AlignedEvent {
                    at,
                    slot,
                    event: MarketEvent::Bar(bar),
                })
                .unwrap()
            {
                close_reports.push((r.at, r.actions, r.rejected));
            }
        }
        // t1 轮 1 次 close；t2 轮 exit 再次命中但守卫跳过（close_position 对
        // CloseRequested 直接返回 actions=0），只有被抑制计数的 rejected。
        let t1_report = close_reports
            .iter()
            .find(|(at, ..)| *at == t1)
            .expect("t1 决策轮");
        assert_eq!(t1_report.1, 1, "恰一次 close，实际 {t1_report:?}");
        let t2_report = close_reports
            .iter()
            .find(|(at, ..)| *at == t2)
            .expect("t2 决策轮");
        assert_eq!(
            t2_report.1, 0,
            "重复 exit 不得再发 close，实际 {t2_report:?}"
        );
    }

    // ─── Task 11：panic 断路器、Store 重建恢复 ─────────────────────────────

    struct PanicStrategy;
    impl CrossSectionalStrategyPort for PanicStrategy {
        fn strategy_type(&self) -> &str {
            "panic"
        }

        fn on_cross_section_bars(
            &mut self,
            _newly: &[Timeframe],
            _snapshots: &[InstrumentSnapshot],
            _port: &PortfolioContext,
        ) -> Vec<InstrumentAction> {
            panic!("test strategy panic");
        }
    }

    #[test]
    fn panicking_strategy_pauses_only_its_group() {
        let t0 = utc(2026, 1, 5, 0, 0);
        let cfg = strategy_config("1m", None);
        let bars: Vec<Bar> = {
            let ex = BinanceExchange::new(0.0);
            let inst = ex.parse_instrument("BTCUSDT").unwrap();
            (0..4)
                .map(|i| bar_at(&inst, t0 + Duration::minutes(i), 100.0 + i as f64))
                .collect()
        };
        let mut a = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg.clone(),
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(PanicStrategy),
            None,
        )
        .unwrap();
        for b in &bars {
            let r = a
                .on_event(AlignedEvent {
                    at: b.open_time,
                    slot: 0,
                    event: MarketEvent::Bar(b.clone()),
                })
                .unwrap();
            if let Some(r) = r {
                assert_eq!(r.actions, 0, "panic 组不得再发策略动作");
                assert!(r.fills.is_empty(), "panic 组不得有成交");
            }
        }
        let sa = a.finish().unwrap();
        assert!(a.is_paused(), "panic 后本组必须暂停");
        assert_eq!(sa.actions, 0);
        assert!(sa.fills.is_empty());

        // 兄弟组照常服务
        let (mut b, calls_b) =
            recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        for bar in &bars {
            b.on_event(AlignedEvent {
                at: bar.open_time,
                slot: 0,
                event: MarketEvent::Bar(bar.clone()),
            })
            .unwrap();
        }
        let sb = b.finish().unwrap();
        assert!(!b.is_paused());
        assert_eq!(sb.bars_processed, 4);
        assert!(!calls_b.lock().unwrap().is_empty(), "兄弟组策略正常被调用");
    }

    // ─── PR-5 T3：远程对账 / 启动恢复 ──────────────────────────────────

    fn active_sub_of(inst: &InstrumentId, id: &str, side: Side, qty: f64) -> SubPosition {
        SubPosition {
            id: id.into(),
            strategy_id: "xsec_momentum:default".into(),
            master_id: "master_000001".into(),
            instrument: inst.clone(),
            side,
            qty,
            entry_price: 100.0,
            stop_price: None,
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: Some(utc(2026, 1, 4, 23, 0)),
            closed_at: None,
            realized_pnl: None,
        }
    }

    fn remote_lot_of(inst: &InstrumentId, side: Side, remaining: f64) -> PositionLot {
        PositionLot {
            id: "lot_remote".into(),
            instrument: inst.clone(),
            side,
            entry_price: 100.0,
            qty: remaining,
            remaining,
            opened_at: utc(2026, 1, 5, 1, 0),
            trading_day: TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
            is_today: true,
            qty_raw: None,
            remaining_raw: None,
        }
    }

    /// 换 mock 出口（remote 注入）并返回 orders 句柄（断言「对账永不发单」）。
    fn inject_remote(
        g: &mut CrossSectionalRunnerGroup,
        slot: usize,
        remote: Vec<PositionLot>,
    ) -> Arc<Mutex<Vec<OrderRequest>>> {
        let orders = Arc::new(Mutex::new(Vec::new()));
        g.slots[slot].parts.execution = Box::new(TestExecution {
            orders: orders.clone(),
            fills: Mutex::new(Vec::new()),
            accept: true,
            venue_notional: None,
            remote,
            in_flight: Vec::new(),
        });
        orders
    }

    /// 同 [`inject_remote`]，额外声明仍在途的 signal_id（对账收口闸门）。
    fn inject_remote_in_flight(
        g: &mut CrossSectionalRunnerGroup,
        slot: usize,
        remote: Vec<PositionLot>,
        in_flight: Vec<String>,
    ) -> Arc<Mutex<Vec<OrderRequest>>> {
        let orders = Arc::new(Mutex::new(Vec::new()));
        g.slots[slot].parts.execution = Box::new(TestExecution {
            orders: orders.clone(),
            fills: Mutex::new(Vec::new()),
            accept: true,
            venue_notional: None,
            remote,
            in_flight,
        });
        orders
    }

    /// 换 mock 出口并声明场所单笔开仓名义（模拟 live `order_cash` 口径）。
    fn inject_venue(g: &mut CrossSectionalRunnerGroup, slot: usize, notional: f64) {
        g.slots[slot].parts.execution = Box::new(TestExecution {
            orders: Arc::new(Mutex::new(Vec::new())),
            fills: Mutex::new(Vec::new()),
            accept: true,
            remote: Vec::new(),
            in_flight: Vec::new(),
            venue_notional: Some(notional),
        });
    }

    #[test]
    fn per_symbol_masters_are_scoped_and_reused() {
        let mut cfg = strategy_config("1m", None);
        cfg.merge_positions = false;
        let (mut g, _) = recording_group(
            vec![
                build_slot("BTCUSDT", 1000.0, Timeframe::M1),
                build_slot("ETHUSDT", 1000.0, Timeframe::M1),
            ],
            cfg,
        );
        let at = utc(2026, 1, 5, 1, 0);
        let btc_key = g.slots[0].instrument_key();
        let eth_key = g.slots[1].instrument_key();
        let btc = g.ensure_active_master(at, Some(&btc_key));
        let eth = g.ensure_active_master(at, Some(&eth_key));
        assert_ne!(btc, eth, "逐标的独立总仓不得跨标的复用");
        assert_eq!(g.masters[&btc].scope.as_deref(), Some(btc_key.as_str()));
        assert_eq!(g.masters[&eth].scope.as_deref(), Some(eth_key.as_str()));
        assert_eq!(
            g.ensure_active_master(at, Some(&btc_key)),
            btc,
            "同标的 Open 总仓必须复用"
        );
    }

    #[test]
    fn per_symbol_mode_calls_strategy_once_per_slot_with_own_nav() {
        let mut cfg = strategy_config("15m", None);
        cfg.merge_positions = false;
        let slots = vec![
            build_slot("BTCUSDT", 1000.0, Timeframe::M15),
            build_slot("ETHUSDT", 1000.0, Timeframe::M15),
        ];
        let (strategy, calls) = recording_strategy(Vec::new());
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            slots,
            Box::new(strategy),
            None,
        )
        .unwrap();
        g.set_initial_cash_split(2000.0);
        let t0 = utc(2026, 1, 5, 0, 0);
        let btc = g.slots[0].parts.instrument.clone();
        let eth = g.slots[1].parts.instrument.clone();
        let btc_bars: Vec<Bar> = (0..15)
            .map(|i| bar_at(&btc, t0 + Duration::minutes(i), 100.0))
            .collect();
        let eth_bars: Vec<Bar> = (0..15)
            .map(|i| bar_at(&eth, t0 + Duration::minutes(i), 100.0))
            .collect();
        g.preload(&[btc_bars, eth_bars]).unwrap();
        let report = g
            .prime_decision_round()
            .unwrap()
            .expect("两个 slot 的 base bar 已闭合，必须产出一轮");
        assert!(report.decision_round);
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2, "逐标的模式每个 slot 独立调用一次");
        assert!(
            calls.iter().all(|snaps| snaps.len() == 1),
            "策略每次只能看到本标的快照，不能看到跨标的批次"
        );
        assert_eq!(g.current_nav(), 2000.0, "独立起账后不得再去重");
        assert_eq!(g.current_nav_for_slot(0), 1000.0);
        assert_eq!(g.current_nav_for_slot(1), 1000.0);
    }

    /// 逐标的 L3：一个标的触及组合止损只平该标的，并暂停该标的；
    /// 其他标的的总仓/子仓不受影响。
    #[test]
    fn per_symbol_l3_closes_only_triggered_scope() {
        let mut cfg = strategy_config("1m", None);
        cfg.merge_positions = false;
        cfg.portfolio_exit.stop_loss_pct = Some(1.0);
        let slots = vec![
            build_slot("BTCUSDT", 500.0, Timeframe::M1),
            build_slot("ETHUSDT", 500.0, Timeframe::M1),
        ];
        let (mut g, _) = recording_group(slots, cfg);
        g.set_initial_cash_split(1000.0);
        let at = utc(2026, 1, 5, 0, 0);
        let btc_key = g.slots[0].instrument_key();
        let eth_key = g.slots[1].instrument_key();
        let btc_master = g.ensure_active_master(at, Some(&btc_key));
        let eth_master = g.ensure_active_master(at, Some(&eth_key));
        let btc_inst = g.slots[0].parts.instrument.clone();
        let eth_inst = g.slots[1].parts.instrument.clone();
        g.slots[0].sub_positions.push(SubPosition {
            id: "sub_btc".into(),
            strategy_id: g.strategy_id.clone(),
            master_id: btc_master.clone(),
            instrument: btc_inst,
            side: Side::Long,
            qty: 1.0,
            entry_price: 100.0,
            stop_price: None,
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: Some(at),
            closed_at: None,
            realized_pnl: None,
        });
        g.slots[1].sub_positions.push(SubPosition {
            id: "sub_eth".into(),
            strategy_id: g.strategy_id.clone(),
            master_id: eth_master.clone(),
            instrument: eth_inst,
            side: Side::Long,
            qty: 1.0,
            entry_price: 100.0,
            stop_price: None,
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: Some(at),
            closed_at: None,
            realized_pnl: None,
        });
        // 预置子仓不走撮合账本校验：换 TestExecution 桩。
        inject_remote(&mut g, 0, Vec::new());
        inject_remote(&mut g, 1, Vec::new());
        let t0 = utc(2026, 1, 5, 0, 0);
        let t1 = t0 + Duration::minutes(1);
        let btc = g.slots[0].parts.instrument.clone();
        let eth = g.slots[1].parts.instrument.clone();
        let events = vec![
            (0usize, t0, bar_at(&btc, t0, 90.0)),
            (1usize, t0, bar_at(&eth, t0, 100.0)),
            (0usize, t1, bar_at(&btc, t1, 90.0)),
            (1usize, t1, bar_at(&eth, t1, 100.0)),
        ];
        drive(&mut g, &events);
        assert!(
            g.portfolio
                .scope_risk
                .get(&btc_key)
                .is_some_and(|s| s.paused),
            "BTC 跌破 -1% 必须触发该标的 L3 并暂停"
        );
        assert!(
            !g.portfolio
                .scope_risk
                .get(&eth_key)
                .is_some_and(|s| s.paused),
            "ETH 无亏损不得被 BTC 的 L3 连带暂停"
        );
        assert_eq!(
            g.masters[&btc_master].state,
            MasterPositionState::Closing,
            "BTC 总仓必须进入平仓"
        );
        assert_eq!(
            g.masters[&eth_master].state,
            MasterPositionState::Open,
            "ETH 总仓保持 Open"
        );
        assert!(
            g.slots[1]
                .sub_positions
                .iter()
                .any(|s| s.state == SubPositionState::Active),
            "ETH 子仓不得被平"
        );
    }

    #[test]
    fn per_symbol_legacy_migration_assigns_scope_masters() {
        let dir = tempfile::tempdir().unwrap();
        let store = strategy_store::Store::new(dir.path(), Arc::new(strategy_store::EpochClock));
        let mut cfg = strategy_config("1m", None);
        cfg.merge_positions = false;
        let (mut g, _) = recording_group(
            vec![
                build_slot("BTCUSDT", 500.0, Timeframe::M1),
                build_slot("ETHUSDT", 500.0, Timeframe::M1),
            ],
            cfg,
        );
        let at = utc(2026, 1, 5, 0, 0);
        for idx in 0..2 {
            let mut sub = active_sub_of(
                &g.slots[idx].parts.instrument,
                &format!("legacy_{idx}"),
                Side::Long,
                1.0,
            );
            sub.master_id.clear();
            sub.opened_at = Some(at);
            g.slots[idx].sub_positions.push(sub);
        }
        g.migrate_legacy_masters(&store);
        let btc_key = g.slots[0].instrument_key();
        let eth_key = g.slots[1].instrument_key();
        let btc_master = g.slots[0].sub_positions[0].master_id.clone();
        let eth_master = g.slots[1].sub_positions[0].master_id.clone();
        assert_ne!(btc_master, eth_master, "旧数据必须按标的拆到不同总仓");
        assert_eq!(
            g.masters[&btc_master].scope.as_deref(),
            Some(btc_key.as_str())
        );
        assert_eq!(
            g.masters[&eth_master].scope.as_deref(),
            Some(eth_key.as_str())
        );
    }

    #[test]
    fn per_symbol_master_for_fill_is_scoped() {
        let mut cfg = strategy_config("1m", None);
        cfg.merge_positions = false;
        let (mut g, _) = recording_group(
            vec![
                build_slot("BTCUSDT", 1000.0, Timeframe::M1),
                build_slot("ETHUSDT", 1000.0, Timeframe::M1),
            ],
            cfg,
        );
        let at = utc(2026, 1, 5, 1, 0);
        let btc_key = g.slots[0].instrument_key();
        let eth_key = g.slots[1].instrument_key();
        let btc = g.master_for_fill(at, Some(&btc_key));
        let eth = g.master_for_fill(at, Some(&eth_key));
        assert_ne!(btc, eth, "成交归属不得跨标的");
        assert_eq!(g.masters[&btc].scope.as_deref(), Some(btc_key.as_str()));
        assert_eq!(g.masters[&eth].scope.as_deref(), Some(eth_key.as_str()));
        assert_eq!(g.master_for_fill(at, Some(&btc_key)), btc);
    }

    #[test]
    fn ensure_active_master_reuses_open_and_allocates_after_close() {
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        let at = utc(2026, 1, 5, 1, 0);
        let first = g.ensure_active_master(at, None);
        assert_eq!(first, "master_000001");
        assert_eq!(g.portfolio.master_seq, 1);
        assert_eq!(g.ensure_active_master(at, None), first, "Open 总仓必须复用");

        g.masters.get_mut(&first).unwrap().state = MasterPositionState::Closed;
        let second = g.ensure_active_master(at, None);
        assert_eq!(second, "master_000002", "Closed 后必须分配新总仓");
        assert_eq!(g.portfolio.master_seq, 2);
        assert_eq!(g.masters[&second].state, MasterPositionState::Open);
    }

    #[test]
    fn recover_backfills_legacy_subposition_into_master() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
        ));
        let cfg = strategy_config("1m", None);
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(RecordingStrategy::new()),
            Some(store.clone()),
        )
        .unwrap();
        let inst = g.slots[0].parts.instrument.clone();
        let mut legacy = active_sub_of(&inst, "sub_legacy", Side::Long, 1.0);
        legacy.master_id.clear();
        store.table::<SubPosition>().upsert(legacy);
        inject_remote(&mut g, 0, vec![remote_lot_of(&inst, Side::Long, 1.0)]);

        let fresh: Box<dyn CrossSectionalStrategyPort> = Box::new(RecordingStrategy::new());
        g.recover_from_store(&store, fresh, false).unwrap();

        let sub = &g.slots[0].sub_positions[0];
        assert!(!sub.master_id.is_empty(), "旧子仓恢复时必须回填 master_id");
        assert!(g.masters.contains_key(&sub.master_id), "回填的总仓必须存在");
        assert_eq!(
            g.portfolio.active_master_id.as_deref(),
            Some(sub.master_id.as_str()),
            "有活动子仓时 active_master_id 必须指向其总仓"
        );
        assert!(store
            .table::<MasterPosition>()
            .get(&sub.master_id)
            .is_some());
        assert_eq!(
            store.table::<SubPosition>().get(&sub.id).unwrap().master_id,
            sub.master_id,
            "迁移后的子仓必须落盘"
        );
    }

    // ─── 批次 2：总仓生命周期、平仓与收口 ────────────────────────────────

    fn insert_master(
        g: &mut CrossSectionalRunnerGroup,
        master_id: &str,
        state: MasterPositionState,
        at: DateTime<Utc>,
    ) {
        g.masters.insert(
            master_id.into(),
            MasterPosition {
                master_id: master_id.into(),
                strategy_id: g.strategy_id.clone(),
                scope: None,
                state,
                opened_at: Some(at),
                closed_at: None,
            },
        );
        if state != MasterPositionState::Closed {
            g.portfolio.active_master_id = Some(master_id.into());
        }
    }

    fn push_sub(
        g: &mut CrossSectionalRunnerGroup,
        slot: usize,
        id: &str,
        master_id: &str,
        state: SubPositionState,
        qty: f64,
    ) -> InstrumentId {
        let inst = g.slots[slot].parts.instrument.clone();
        let mut sub = active_sub_of(&inst, id, Side::Long, qty);
        sub.master_id = master_id.into();
        sub.state = state;
        g.slots[slot].sub_positions.push(sub);
        inst
    }

    #[test]
    fn ensure_active_master_never_reuses_a_closing_master() {
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        let at = utc(2026, 1, 5, 1, 0);
        let first = g.ensure_active_master(at, None);
        g.masters.get_mut(&first).unwrap().state = MasterPositionState::Closing;
        let second = g.ensure_active_master(at, None);
        assert_ne!(second, first, "Closing 总仓不得接收新仓");
        assert_eq!(g.masters[&second].state, MasterPositionState::Open);
        assert_eq!(
            g.portfolio.active_master_id.as_deref(),
            Some(second.as_str())
        );
    }

    #[test]
    fn settle_master_waits_for_all_subs_including_inflight_open() {
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(
            vec![
                build_slot("BTCUSDT", 500.0, Timeframe::M1),
                build_slot("ETHUSDT", 500.0, Timeframe::M1),
            ],
            cfg,
        );
        let at = utc(2026, 1, 5, 1, 0);
        insert_master(&mut g, "master_000001", MasterPositionState::Open, at);
        push_sub(
            &mut g,
            0,
            "sub_btc",
            "master_000001",
            SubPositionState::Active,
            1.0,
        );
        push_sub(
            &mut g,
            1,
            "sub_eth",
            "master_000001",
            SubPositionState::PendingOpen,
            1.0,
        );

        assert!(!g.settle_masters(at), "有活动/在途子仓 → 不得收口");
        g.slots[0].sub_positions[0].state = SubPositionState::Closed;
        assert!(
            !g.settle_masters(at),
            "在途开仓未落地 → 总仓不得提前 Closed"
        );
        g.slots[1].sub_positions[0].state = SubPositionState::Closed;
        assert!(g.settle_masters(at), "全部子仓收口 → 总仓 Closed");
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Closed
        );
        assert!(g.portfolio.active_master_id.is_none());
    }

    #[test]
    fn close_master_closes_every_sub_once_then_settles() {
        let cfg = strategy_config("1m", None);
        let btc_orders = Arc::new(Mutex::new(Vec::new()));
        let eth_orders = Arc::new(Mutex::new(Vec::new()));
        let mut btc = build_slot("BTCUSDT", 500.0, Timeframe::M1);
        let mut eth = build_slot("ETHUSDT", 500.0, Timeframe::M1);
        btc.parts.execution = Box::new(TestExecution {
            orders: btc_orders.clone(),
            fills: Mutex::new(Vec::new()),
            accept: true,
            venue_notional: None,
            remote: Vec::new(),
            in_flight: Vec::new(),
        });
        eth.parts.execution = Box::new(TestExecution {
            orders: eth_orders.clone(),
            fills: Mutex::new(Vec::new()),
            accept: true,
            venue_notional: None,
            remote: Vec::new(),
            in_flight: Vec::new(),
        });
        let (mut g, _) = recording_group(vec![btc, eth], cfg);
        let at = utc(2026, 1, 5, 1, 0);
        insert_master(&mut g, "master_000001", MasterPositionState::Open, at);
        let btc_inst = push_sub(
            &mut g,
            0,
            "sub_btc",
            "master_000001",
            SubPositionState::Active,
            1.0,
        );
        let eth_inst = push_sub(
            &mut g,
            1,
            "sub_eth",
            "master_000001",
            SubPositionState::Active,
            1.0,
        );
        for slot in &mut g.slots {
            slot.last_bar_at = Some(at);
            slot.last_mark = Some(100.0);
        }
        let prices = BTreeMap::from([
            (btc_inst.namespaced_key(), 100.0),
            (eth_inst.namespaced_key(), 100.0),
        ]);

        let (actions, rejected) = g
            .close_master(
                "master_000001",
                &prices,
                "test close master",
                false,
                SubIntent::RiskCloseAll,
            )
            .unwrap();
        assert_eq!((actions, rejected), (2, 0), "1:N 每个子仓各一笔平仓单");
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Closing
        );
        assert!(!g.can_trade(), "Closing 总仓必须拦截新开/加仓");
        let btc_order = btc_orders.lock().unwrap()[0].clone();
        assert_eq!(eth_orders.lock().unwrap().len(), 1);
        assert!(btc_order.reduce_only, "总仓平仓腿必须 reduce_only");
        assert_ne!(btc_order.offset, Offset::Open, "总仓平仓必须走平仓腿");

        // 重复调用必须幂等：CloseRequested 子仓不得重复下单。
        let again = g
            .close_master(
                "master_000001",
                &prices,
                "test close master",
                false,
                SubIntent::RiskCloseAll,
            )
            .unwrap();
        assert_eq!(again, (0, 0), "CloseRequested 防重复平仓");

        for i in 0..g.slots.len() {
            g.sync_slot_fills(i).unwrap();
        }
        assert!(g
            .slots
            .iter()
            .flat_map(|s| s.sub_positions.iter())
            .all(|s| s.state == SubPositionState::Closed));
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Closed
        );
        assert!(g.portfolio.active_master_id.is_none());
        assert!(g.can_trade(), "总仓收口后恢复可交易");
    }

    #[test]
    fn close_master_only_closes_subs_of_that_master() {
        let cfg = strategy_config("1m", None);
        let btc_orders = Arc::new(Mutex::new(Vec::new()));
        let eth_orders = Arc::new(Mutex::new(Vec::new()));
        let mut btc = build_slot("BTCUSDT", 500.0, Timeframe::M1);
        let mut eth = build_slot("ETHUSDT", 500.0, Timeframe::M1);
        btc.parts.execution = Box::new(TestExecution {
            orders: btc_orders.clone(),
            fills: Mutex::new(Vec::new()),
            accept: true,
            venue_notional: None,
            remote: Vec::new(),
            in_flight: Vec::new(),
        });
        eth.parts.execution = Box::new(TestExecution {
            orders: eth_orders.clone(),
            fills: Mutex::new(Vec::new()),
            accept: true,
            venue_notional: None,
            remote: Vec::new(),
            in_flight: Vec::new(),
        });
        let (mut g, _) = recording_group(vec![btc, eth], cfg);
        let at = utc(2026, 1, 5, 1, 0);
        insert_master(&mut g, "master_000001", MasterPositionState::Open, at);
        insert_master(&mut g, "master_000002", MasterPositionState::Open, at);
        let btc_inst = push_sub(
            &mut g,
            0,
            "sub_btc",
            "master_000001",
            SubPositionState::Active,
            1.0,
        );
        let eth_inst = push_sub(
            &mut g,
            1,
            "sub_eth",
            "master_000002",
            SubPositionState::Active,
            1.0,
        );
        for slot in &mut g.slots {
            slot.last_bar_at = Some(at);
            slot.last_mark = Some(100.0);
        }
        let prices = BTreeMap::from([
            (btc_inst.namespaced_key(), 100.0),
            (eth_inst.namespaced_key(), 100.0),
        ]);

        let (actions, _) = g
            .close_master(
                "master_000001",
                &prices,
                "close m1",
                false,
                SubIntent::RiskCloseAll,
            )
            .unwrap();
        assert_eq!(actions, 1, "只平 M1 的 BTC 子仓");
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Closing
        );
        assert_eq!(g.masters["master_000002"].state, MasterPositionState::Open);
        assert_eq!(
            g.slots[1].sub_positions[0].state,
            SubPositionState::Active,
            "其他总仓子仓不得被平"
        );
        assert!(eth_orders.lock().unwrap().is_empty());
        assert_eq!(btc_orders.lock().unwrap().len(), 1);

        g.sync_slot_fills(0).unwrap();
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Closed
        );
        assert_eq!(g.masters["master_000002"].state, MasterPositionState::Open);
        assert_eq!(g.slots[1].sub_positions[0].state, SubPositionState::Active);
    }

    #[test]
    fn rejected_open_does_not_leave_an_active_empty_master() {
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.0; // 容量恒为 0 → 开仓被拒
        let (mut g, _) = pe_group(cfg, &["BTCUSDT"], vec![vec![opens_of("BTCUSDT", 100.0)]]);
        drive_prices(&mut g, &[&[100.0], &[100.0]]);
        g.finish().unwrap();

        assert!(g.portfolio.active_master_id.is_none(), "不得留下活动空总仓");
        assert!(
            g.masters
                .values()
                .all(|m| m.state == MasterPositionState::Closed),
            "开仓被拒产生的空总仓必须收口"
        );
        assert!(g.slots[0]
            .sub_positions
            .iter()
            .all(|s| s.state == SubPositionState::Closed));
    }

    #[test]
    fn l3_closes_master_and_settles_after_every_sub_fill() {
        let cfg = pe_cfg(None, Some(8.0), false);
        let r0 = vec![opens_of("BTCUSDT", 100.0), opens_of("ETHUSDT", 100.0)];
        let (mut g, calls) = pe_group(cfg, &["BTCUSDT", "ETHUSDT"], vec![r0]);
        let t0 = utc(2026, 1, 5, 0, 0);
        let reports = drive_prices(
            &mut g,
            &[&[100.0, 100.0], &[91.0, 91.0], &[91.0, 91.0], &[91.0, 91.0]],
        );
        let fire = reports
            .iter()
            .find(|r| r.at == t0 + Duration::minutes(1))
            .expect("t1 已 flush L3");
        assert_eq!(fire.actions, 2, "BTC+ETH 各一笔 CloseAll");
        assert_eq!(fire.rejected, 0, "策略整轮被 L3 抑制");
        assert_eq!(calls.lock().unwrap().len(), 1, "L3 后策略不再被调用");

        // t2 同步两笔 close fills → 子仓全部 Closed → 总仓收口 → Paused。
        assert!(g
            .slots
            .iter()
            .flat_map(|s| s.sub_positions.iter())
            .all(|s| s.state == SubPositionState::Closed));
        assert!(g
            .masters
            .values()
            .all(|m| m.state == MasterPositionState::Closed));
        assert!(g.portfolio.active_master_id.is_none());
        assert_eq!(g.phase, PortfolioPhase::Paused);
    }

    #[test]
    fn inflight_open_fill_under_closing_master_is_closed_and_settled() {
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        let at = utc(2026, 1, 5, 1, 0);
        let inst = g.slots[0].parts.instrument.clone();
        let orders = Arc::new(Mutex::new(Vec::new()));
        let mut sub = active_sub_of(&inst, "sub_inflight", Side::Long, 0.0);
        sub.state = SubPositionState::OpenRequested;
        g.slots[0].sub_positions.push(sub.clone());
        insert_master(&mut g, "master_000001", MasterPositionState::Closing, at);
        g.slots[0].last_bar_at = Some(at);
        g.slots[0].last_mark = Some(100.0);
        g.slots[0].parts.execution = Box::new(TestExecution {
            orders: orders.clone(),
            fills: Mutex::new(vec![Fill {
                order_id: "ord_inflight".into(),
                signal_id: sub.id.clone(),
                instrument: inst,
                side: Side::Long,
                offset: Offset::Open,
                qty: 1.0,
                price: 100.0,
                fee: 0.0,
                realized_pnl: 0.0,
                margin_release: 0.0,
                at,
                trading_day: TradingDay(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
                is_stop_loss: false,
                qty_raw: None,
            }]),
            accept: true,
            venue_notional: None,
            remote: Vec::new(),
            in_flight: Vec::new(),
        });

        g.sync_slot_fills(0).unwrap();
        assert_eq!(
            g.slots[0].sub_positions[0].state,
            SubPositionState::CloseRequested,
            "Closing 期间落地的开仓成交必须立即补平"
        );
        assert_eq!(orders.lock().unwrap().len(), 1, "补平必须发平仓单");
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Closing,
            "子仓尚在平仓途中，总仓不得提前 Closed"
        );

        g.sync_slot_fills(0).unwrap();
        assert_eq!(g.slots[0].sub_positions[0].state, SubPositionState::Closed);
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Closed
        );
        assert!(g.portfolio.active_master_id.is_none());
    }

    #[test]
    fn persist_round_skips_closed_rows_before_today() {
        struct FixedClock(i64);
        impl strategy_store::Clock for FixedClock {
            fn now_ms(&self) -> i64 {
                self.0
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let today = utc(2026, 1, 5, 1, 0);
        let store = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(FixedClock(today.timestamp_millis())),
        ));
        let cfg = strategy_config("1m", None);
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(RecordingStrategy::new()),
            Some(store.clone()),
        )
        .unwrap();
        let yesterday = today - chrono::Duration::days(1);
        g.masters.insert(
            "master_prev".into(),
            MasterPosition {
                master_id: "master_prev".into(),
                strategy_id: g.strategy_id.clone(),
                scope: None,
                state: MasterPositionState::Closed,
                opened_at: Some(yesterday),
                closed_at: Some(yesterday),
            },
        );
        g.masters.insert(
            "master_today".into(),
            MasterPosition {
                master_id: "master_today".into(),
                strategy_id: g.strategy_id.clone(),
                scope: None,
                state: MasterPositionState::Closed,
                opened_at: Some(today),
                closed_at: Some(today),
            },
        );
        g.persist_round();
        let table = store.table::<MasterPosition>();
        assert_eq!(table.get("master_prev"), None, "昨日终态不得写回当日快照");
        assert!(table.get("master_today").is_some(), "当日终态保留");
    }

    #[test]
    fn recovery_preserves_closing_master_until_subs_settle() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
        ));
        let cfg = strategy_config("1m", None);
        let at = utc(2026, 1, 5, 1, 0);
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg.clone(),
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(RecordingStrategy::new()),
            Some(store.clone()),
        )
        .unwrap();
        let inst = g.slots[0].parts.instrument.clone();
        let sub = active_sub_of(&inst, "sub_close", Side::Long, 1.0);
        g.slots[0].sub_positions.push(sub.clone());
        g.slots[0].last_bar_at = Some(at);
        g.portfolio.active_master_id = Some("master_000001".into());
        g.masters.insert(
            "master_000001".into(),
            MasterPosition {
                master_id: "master_000001".into(),
                strategy_id: g.strategy_id.clone(),
                scope: None,
                state: MasterPositionState::Closing,
                opened_at: Some(at),
                closed_at: None,
            },
        );
        g.subs_changed = true;
        g.persist_round();

        let mut g2 = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(RecordingStrategy::new()),
            Some(store.clone()),
        )
        .unwrap();
        // new() 用初始组合行落盘，覆盖了重启前的指针；恢复前写回。
        store
            .table::<StrategyPortfolio>()
            .upsert(g.portfolio.clone());
        inject_remote(&mut g2, 0, vec![remote_lot_of(&inst, Side::Long, 1.0)]);
        g2.recover_from_store(&store, Box::new(RecordingStrategy::new()), false)
            .unwrap();

        assert_eq!(
            g2.masters["master_000001"].state,
            MasterPositionState::Closing,
            "重启必须保留 Closing 状态"
        );
        assert_eq!(
            g2.portfolio.active_master_id.as_deref(),
            Some("master_000001")
        );
        assert!(!g2.can_trade(), "Closing 恢复后仍不得开新仓");

        g2.slots[0].sub_positions[0].state = SubPositionState::Closed;
        assert!(g2.settle_masters(at));
        assert_eq!(
            g2.masters["master_000001"].state,
            MasterPositionState::Closed
        );
        assert!(g2.portfolio.active_master_id.is_none());
    }

    #[test]
    fn close_master_action_closes_every_sub_then_settles() {
        let cfg = strategy_config("1m", None);
        let r0 = vec![opens_of("BTCUSDT", 100.0), opens_of("ETHUSDT", 100.0)];
        let r1 = vec![InstrumentAction::CloseMaster {
            reason: "strategy close master".into(),
            is_stop_loss: false,
        }];
        let (mut g, _calls) = pe_group(cfg, &["BTCUSDT", "ETHUSDT"], vec![r0, r1]);
        let reports = drive_prices(
            &mut g,
            &[
                &[100.0, 100.0],
                &[100.0, 100.0],
                &[100.0, 100.0],
                &[100.0, 100.0],
            ],
        );

        let fire = reports
            .iter()
            .find(|r| r.at == utc(2026, 1, 5, 0, 1))
            .expect("t1 CloseMaster 决策轮");
        assert_eq!(fire.actions, 2, "CloseMaster 平掉总仓下全部子仓");
        assert_eq!(fire.rejected, 0);
        // t2 同步两笔 close fills → 子仓 Closed → 总仓收口。
        assert!(g
            .slots
            .iter()
            .flat_map(|s| s.sub_positions.iter())
            .all(|s| s.state == SubPositionState::Closed));
        assert!(g
            .masters
            .values()
            .all(|m| m.state == MasterPositionState::Closed));
        assert!(g.portfolio.active_master_id.is_none());
        assert_eq!(g.phase, PortfolioPhase::Running, "策略平仓不触发 L3 Paused");
    }

    #[test]
    fn portfolio_context_exposes_active_master_view() {
        let cfg = strategy_config("1m", None);
        let mut strategy = RecordingStrategy::new();
        strategy.queue(vec![opens_of("BTCUSDT", 100.0), opens_of("ETHUSDT", 100.0)]);
        let contexts = strategy.contexts.clone();
        let slots = vec![
            build_slot_cfg("BTCUSDT", 500.0, &cfg),
            build_slot_cfg("ETHUSDT", 500.0, &cfg),
        ];
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            slots,
            Box::new(strategy),
            None,
        )
        .unwrap();
        drive_prices(&mut g, &[&[100.0, 100.0], &[100.0, 100.0], &[100.0, 100.0]]);

        let seen = contexts.lock().unwrap();
        assert_eq!(seen.len(), 2, "t0/t1 各一次决策");
        assert!(seen[0].master.is_none(), "开仓前无总仓");
        let master = seen[1].master.as_ref().expect("成交落地后应看到活动总仓");
        assert_eq!(
            Some(master.master_id.as_str()),
            g.portfolio.active_master_id.as_deref()
        );
        assert_eq!(master.state, MasterPositionState::Open);
        assert_eq!(master.view.sub_position_count, 2);
        assert!(master.view.gross_notional > 0.0, "总仓视图按子仓聚合");
    }

    #[test]
    fn reconcile_closes_sub_when_remote_flat() {
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        let inst = g.slots[0].parts.instrument.clone();
        let orders = inject_remote(&mut g, 0, Vec::new());
        g.slots[0]
            .sub_positions
            .push(active_sub_of(&inst, "sub_a", Side::Long, 1.0));
        g.slots[0].last_bar_at = Some(utc(2026, 1, 5, 1, 0));

        let report = g.reconcile_remote().unwrap();
        assert_eq!(report.synced_instruments, 1);
        let kinds: Vec<&str> = report.drifts.iter().map(|d| d.1.as_str()).collect();
        assert_eq!(kinds, vec!["closed"], "remote 平 → closed drift");
        assert_eq!(
            g.slots[0].sub_positions[0].state,
            SubPositionState::Closed,
            "RemoteFlat 推进到终态"
        );
        assert_eq!(
            g.slots[0].sub_positions[0].closed_at,
            Some(utc(2026, 1, 5, 1, 0)),
            "closed_at 取最近 bar 时间（禁 now()）"
        );
        assert!(
            orders.lock().unwrap().is_empty(),
            "对账永不发单（只收敛账本）"
        );
    }

    #[test]
    fn reconcile_keeps_open_requested_sub_while_order_in_flight() {
        // 实盘成交回补有延迟：远程暂空 ≠ 开仓失败。订单仍在途时不得收口，
        // 否则随后到达的成交会变成 legacy 无主仓。
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        let inst = g.slots[0].parts.instrument.clone();
        let at = utc(2026, 1, 5, 1, 0);
        let orders = inject_remote_in_flight(&mut g, 0, Vec::new(), vec!["sub_a".into()]);
        let mut sub = active_sub_of(&inst, "sub_a", Side::Long, 1.0);
        sub.state = SubPositionState::OpenRequested;
        g.slots[0].sub_positions.push(sub);
        insert_master(&mut g, "master_000001", MasterPositionState::Open, at);
        g.slots[0].last_bar_at = Some(at);

        let report = g.reconcile_remote().unwrap();
        assert!(
            report.drifts.is_empty(),
            "在途开仓不得产生收口漂移：{:?}",
            report.drifts
        );
        assert_eq!(
            g.slots[0].sub_positions[0].state,
            SubPositionState::OpenRequested,
            "订单在途期间子仓保持 OpenRequested"
        );
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Open,
            "总仓不得提前收口"
        );
        assert!(orders.lock().unwrap().is_empty(), "对账永不发单");
    }

    #[test]
    fn reconcile_keeps_fresh_fill_on_first_flat_snapshot() {
        // 成交 ack 已到（Active）而交易所仓位落库滞后：首拍读到远程空仓
        // 不是事实，收口会让随后出现的仓位变成 legacy 无主仓（实盘 11-14 事故）。
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        let inst = g.slots[0].parts.instrument.clone();
        let at = utc(2026, 1, 5, 1, 0);
        let _orders = inject_remote(&mut g, 0, Vec::new());
        g.slots[0]
            .sub_positions
            .push(active_sub_of(&inst, "sub_a", Side::Long, 1.0));
        g.fresh_opens.insert("sub_a".into());
        insert_master(&mut g, "master_000001", MasterPositionState::Open, at);
        g.slots[0].last_bar_at = Some(at);

        let report = g.reconcile_remote().unwrap();
        assert!(
            report.drifts.is_empty(),
            "刚成交的腿不得收口：{:?}",
            report.drifts
        );
        assert_eq!(
            g.slots[0].sub_positions[0].state,
            SubPositionState::Active,
            "保持 Active 等交易所仓位落库"
        );
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Open,
            "总仓不得提前收口"
        );
    }

    #[test]
    fn reconcile_closes_fresh_fill_on_second_flat_snapshot() {
        // 复核拍仍同向空仓 → 空仓快照成立，正常收口（不得卡死）。
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        let inst = g.slots[0].parts.instrument.clone();
        let at = utc(2026, 1, 5, 1, 0);
        let _orders = inject_remote(&mut g, 0, Vec::new());
        g.slots[0]
            .sub_positions
            .push(active_sub_of(&inst, "sub_a", Side::Long, 1.0));
        g.fresh_opens.insert("sub_a".into());
        insert_master(&mut g, "master_000001", MasterPositionState::Open, at);
        g.slots[0].last_bar_at = Some(at);

        assert!(g.reconcile_remote().unwrap().drifts.is_empty(), "首拍放过");
        let report = g.reconcile_remote().unwrap();
        let kinds: Vec<&str> = report.drifts.iter().map(|d| d.1.as_str()).collect();
        assert_eq!(kinds, vec!["closed"], "复核拍空仓收口");
        assert_eq!(g.slots[0].sub_positions[0].state, SubPositionState::Closed);
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Closed,
            "总仓同步收口"
        );
    }

    #[test]
    fn reconcile_closes_open_requested_sub_when_order_terminal() {
        // 订单已到终态（不在途）而远程仍空 → 远程事实可安全收口。
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        let inst = g.slots[0].parts.instrument.clone();
        let at = utc(2026, 1, 5, 1, 0);
        let _orders = inject_remote(&mut g, 0, Vec::new());
        let mut sub = active_sub_of(&inst, "sub_a", Side::Long, 1.0);
        sub.state = SubPositionState::OpenRequested;
        g.slots[0].sub_positions.push(sub);
        insert_master(&mut g, "master_000001", MasterPositionState::Open, at);
        g.slots[0].last_bar_at = Some(at);

        let report = g.reconcile_remote().unwrap();
        let kinds: Vec<&str> = report.drifts.iter().map(|d| d.1.as_str()).collect();
        assert_eq!(kinds, vec!["closed"]);
        assert_eq!(g.slots[0].sub_positions[0].state, SubPositionState::Closed);
        assert_eq!(
            g.masters["master_000001"].state,
            MasterPositionState::Closed,
            "子仓收口后总仓同步收口"
        );
    }

    #[test]
    fn reconcile_corrects_qty_toward_remote() {
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        let inst = g.slots[0].parts.instrument.clone();
        // 本地子仓 qty=1、远程同向剩余 0.4 → 按远程修正。
        inject_remote(&mut g, 0, vec![remote_lot_of(&inst, Side::Long, 0.4)]);
        g.slots[0]
            .sub_positions
            .push(active_sub_of(&inst, "sub_a", Side::Long, 1.0));

        let report = g.reconcile_remote().unwrap();
        let kinds: Vec<&str> = report.drifts.iter().map(|d| d.1.as_str()).collect();
        assert_eq!(kinds, vec!["qty_corrected"]);
        assert!((g.slots[0].sub_positions[0].qty - 0.4).abs() < 1e-12);
        assert!(
            (g.slots[0].pos_sync.total_remaining() - 0.4).abs() < 1e-12,
            "pos_sync.book 同步远程 lots"
        );
    }

    #[test]
    fn reconcile_audits_foreign_remote_without_ledger() {
        let cfg = strategy_config("1m", None);
        let (mut g, _) = recording_group(vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)], cfg);
        let inst = g.slots[0].parts.instrument.clone();
        inject_remote(&mut g, 0, vec![remote_lot_of(&inst, Side::Long, 1.0)]);

        let report = g.reconcile_remote().unwrap();
        let kinds: Vec<&str> = report.drifts.iter().map(|d| d.1.as_str()).collect();
        assert_eq!(kinds, vec!["legacy_remote_only"], "仅审计");
        assert!(
            g.slots[0].sub_positions.is_empty(),
            "不建 SubPosition（不伪造账本行）"
        );
        assert!(
            g.master_position().gross_notional.abs() < 1e-12,
            "不进入 NAV/portfolio 度量"
        );
        assert!(
            (g.slots[0].pos_sync.total_remaining() - 1.0).abs() < 1e-12,
            "但 book 仍以远程为最终事实"
        );
    }

    #[test]
    fn recover_running_and_after_panic_paths_agree() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
        ));
        let cfg = strategy_config("1m", None);
        let inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        // Store 行：Active qty=1；远程同向剩余 1.0（一致，无 drift）。
        store
            .table::<SubPosition>()
            .upsert(active_sub_of(&inst, "sub_good", Side::Long, 1.0));

        let mk = |after_panic: bool| {
            let (mut g, _) = recording_group(
                vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
                cfg.clone(),
            );
            let slot_inst = g.slots[0].parts.instrument.clone();
            inject_remote(&mut g, 0, vec![remote_lot_of(&slot_inst, Side::Long, 1.0)]);
            let fresh: Box<dyn CrossSectionalStrategyPort> = Box::new(RecordingStrategy::new());
            let report = g.recover_from_store(&store, fresh, after_panic).unwrap();
            assert!(
                report.drifts.is_empty(),
                "Store 行与远程一致时必须零 drift（{after_panic}）"
            );
            assert_eq!(g.phase, PortfolioPhase::Running);
            assert!(!g.is_paused(), "恢复后放行");
            (
                g.slots[0].sub_positions.clone(),
                g.slots[0].pos_sync.pos.clone(),
            )
        };

        let (subs_normal, pos_normal) = mk(false);
        let (subs_panic, pos_panic) = mk(true);
        assert_eq!(
            subs_normal, subs_panic,
            "after_panic=false/true 两路径最终子仓一致"
        );
        assert_eq!(
            pos_normal, pos_panic,
            "两路径 PositionState 重建一致（同一事实来源）"
        );
        assert_eq!(subs_normal.len(), 1);
        assert_eq!(subs_normal[0].id, "sub_good");
        assert_eq!(
            subs_normal[0].state,
            SubPositionState::Active,
            "远程同向有仓"
        );
    }

    #[test]
    fn recover_takes_risk_budget_from_current_config_not_store() {
        // 风险预算是运行期配置而非状态：重启时若沿用盘载旧值，改配置
        // 增加容量缓冲会静默失效，实盘继续按旧预算拒单。
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
        ));
        let mut cfg = strategy_config("1m", None);
        cfg.risk.max_total_notional = 0.5;
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg.clone(),
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(RecordingStrategy::new()),
            Some(store.clone()),
        )
        .unwrap();
        let mut stale = g.portfolio.clone();
        stale.max_total_notional = 0.04;
        store.table::<StrategyPortfolio>().upsert(stale);

        let fresh: Box<dyn CrossSectionalStrategyPort> = Box::new(RecordingStrategy::new());
        g.recover_from_store(&store, fresh, false).unwrap();
        assert!(
            (g.portfolio.max_total_notional - 0.5).abs() < 1e-12,
            "恢复后总仓预算必须跟随当前配置 0.5，实际 {}",
            g.portfolio.max_total_notional
        );
    }

    #[test]
    fn restart_recovers_disk_portfolio_not_constructor_placeholder() {
        // 构造器会先把一份全新组合账本写进 Store 活表；随后 recover 的
        // load_merged 因同 id 已存在而跳过盘载旧行，重启后高水位/回撤/
        // 再平衡纪元被静默清零。跨进程重启必须采用盘载状态。
        let dir = tempfile::tempdir().unwrap();
        let cfg = strategy_config("1m", None);
        let store1 = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
        ));
        let g1 = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg.clone(),
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(RecordingStrategy::new()),
            Some(store1.clone()),
        )
        .unwrap();
        let mut prior = g1.portfolio.clone();
        prior.nav = 12345.0;
        prior.high_watermark = 20000.0;
        prior.drawdown = 0.38;
        prior.rebalance_epoch = 7;
        prior.last_rebalance_at = Some(utc(2026, 1, 5, 0, 0));
        store1.table::<StrategyPortfolio>().upsert(prior);
        store1.flush().unwrap();

        // 新 Store 实例模拟跨进程重启：活表为空，盘载有旧组合。
        let store2 = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
        ));
        let mut g2 = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(RecordingStrategy::new()),
            Some(store2.clone()),
        )
        .unwrap();
        let fresh: Box<dyn CrossSectionalStrategyPort> = Box::new(RecordingStrategy::new());
        g2.recover_from_store(&store2, fresh, false).unwrap();
        assert!(
            (g2.portfolio.nav - 12345.0).abs() < 1e-9,
            "重启必须恢复盘载 nav 12345，实际 {}",
            g2.portfolio.nav
        );
        assert!(
            (g2.portfolio.high_watermark - 20000.0).abs() < 1e-9,
            "重启必须恢复盘载高水位 20000，实际 {}",
            g2.portfolio.high_watermark
        );
        assert_eq!(
            g2.portfolio.rebalance_epoch, 7,
            "重启必须恢复盘载再平衡纪元"
        );
        assert_eq!(
            g2.portfolio.last_rebalance_at,
            Some(utc(2026, 1, 5, 0, 0)),
            "重启必须恢复盘载上次再平衡时间"
        );
    }

    #[test]
    fn resume_from_store_discards_untrusted_memory_and_rebuilds() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
        ));
        let cfg = strategy_config("1m", None);
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg.clone(),
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(RecordingStrategy::new()),
            Some(store.clone()),
        )
        .unwrap();
        let inst = g.slots[0].parts.instrument.clone();
        let good = SubPosition {
            id: "sub_good".into(),
            strategy_id: "xsec_momentum:default".into(),
            master_id: "master_000001".into(),
            instrument: inst.clone(),
            side: Side::Long,
            qty: 1.0,
            entry_price: 100.0,
            stop_price: Some(98.0),
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: Some(utc(2026, 1, 4, 23, 0)),
            closed_at: None,
            realized_pnl: None,
        };
        store.table::<SubPosition>().upsert(good.clone());
        g.slots[0].sub_positions = vec![SubPosition {
            qty: 999.0,
            ..good.clone()
        }];
        g.slots[0].pos_sync.pos = None;
        g.phase = PortfolioPhase::Paused;
        g.paused_after_panic = true;

        let fresh: Box<dyn CrossSectionalStrategyPort> = Box::new(RecordingStrategy::new());
        g.resume_from_store(&store, fresh).unwrap();

        assert!(!g.is_paused(), "恢复后放行");
        assert_eq!(g.phase, PortfolioPhase::Running);
        let subs = &g.slots[0].sub_positions;
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].qty, 1.0, "内存 999 必须被 Store 行覆盖");
        assert_eq!(
            subs[0].state,
            SubPositionState::Closed,
            "远程无仓（drift）→ 对账置 Closed"
        );
        assert_eq!(subs[0].id, "sub_good");
        assert_eq!(
            g.slots[0].sub_positions[0].strategy_id,
            "xsec_momentum:default"
        );
    }

    /// 本地平仓成交：写入 `history_position` Store 行 + 22 列 CSV，
    /// 字段取自平仓前的 `PositionState` 快照（peak/MFE/ATR/trail）。
    #[test]
    fn close_fill_writes_history_position_row_and_csv() {
        struct FixedClock(i64);
        impl strategy_store::Clock for FixedClock {
            fn now_ms(&self) -> i64 {
                self.0
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let at = utc(2026, 1, 5, 1, 0);
        let store = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(FixedClock(at.timestamp_millis())),
        ));
        let cfg = strategy_config("1m", None);
        let (strategy, _) = recording_strategy(Vec::new());
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(strategy),
            Some(store.clone()),
        )
        .unwrap();
        g.enable_history_projection(dir.path(), "live");

        let inst = g.slots[0].parts.instrument.clone();
        let sub = active_sub_of(&inst, "sub_hist_1", Side::Long, 1.0);
        g.slots[0].sub_positions.push(sub);
        let opened = utc(2026, 1, 4, 23, 0);
        g.slots[0].pos_sync.pos = Some(strategy_positions::PositionState {
            position_id: "sub_hist_1".into(),
            symbol: inst.namespaced_key(),
            side: strategy_positions::Side::Long,
            entry_price: 100.0,
            entry_time: opened,
            entry_timestamp: opened.timestamp(),
            peak_price: 105.0,
            stop_price: 98.0,
            max_pnl_pct: 5.0,
            min_pnl_pct: -1.0,
            strategy_id: Some("xsec_momentum:default".into()),
            atr_at_entry: 1.5,
            total_size: 1.0,
            remaining_size: 1.0,
            realized_pnl: 0.0,
            trail_activated: true,
            trail_trigger_pct: 0.0,
            entry_size_raw: None,
            remaining_size_raw: None,
            exit_active: false,
            exit_price: 0.0,
        });
        g.slots[0].parts.execution = Box::new(TestExecution {
            orders: Arc::new(Mutex::new(Vec::new())),
            fills: Mutex::new(vec![Fill {
                order_id: "ord_close".into(),
                signal_id: "sub_hist_1".into(),
                instrument: inst,
                side: Side::Long,
                offset: Offset::Close,
                qty: 1.0,
                price: 103.0,
                fee: 0.0,
                realized_pnl: 3.0,
                margin_release: 0.0,
                at,
                trading_day: TradingDay(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
                is_stop_loss: false,
                qty_raw: None,
            }]),
            accept: true,
            remote: Vec::new(),
            in_flight: Vec::new(),
            venue_notional: None,
        });

        g.sync_slot_fills(0).unwrap();

        let rec = store
            .table::<strategy_positions::HistoryPosition>()
            .get("sub_hist_1")
            .expect("平仓行必须落 Store");
        assert_eq!(rec.exit_price, 103.0);
        assert_eq!(rec.peak_price, 105.0);
        assert!((rec.max_pnl_pct - 5.0).abs() < 1e-9);
        assert!((rec.min_pnl_pct + 1.0).abs() < 1e-9);
        assert_eq!(rec.exit_reason, "live_close");
        assert_eq!(rec.strategy_name, "xsec_momentum_default");
        assert!(!rec.is_stop_loss);
        assert!(rec.trail_activated);
        assert_eq!(rec.entry_size, 1.0);
        assert_eq!(rec.exit_size, 1.0);

        let csv = dir
            .path()
            .join("history_positions/xsec_momentum_default/20260105.csv");
        assert!(csv.exists(), "22 列 CSV 必须写出: {}", csv.display());
        let text = std::fs::read_to_string(&csv).unwrap();
        assert!(text.contains("sub_hist_1"));
    }

    /// 远程同向空仓（外部平仓）也要落 history_position：exit_price 取最近
    /// mark，exit_reason=external_close，没有成交回报也要有终态历史行。
    #[test]
    fn remote_flat_writes_external_close_history_row() {
        let dir = tempfile::tempdir().unwrap();
        let at = utc(2026, 1, 5, 1, 0);
        let store = Arc::new(strategy_store::Store::new(
            dir.path(),
            Arc::new(strategy_store::EpochClock),
        ));
        let cfg = strategy_config("1m", None);
        let (strategy, _) = recording_strategy(Vec::new());
        let mut g = CrossSectionalRunnerGroup::new(
            "xsec_momentum:default",
            "xsec_momentum",
            cfg,
            vec![build_slot("BTCUSDT", 1000.0, Timeframe::M1)],
            Box::new(strategy),
            Some(store.clone()),
        )
        .unwrap();
        g.enable_history_projection(dir.path(), "live");

        let inst = g.slots[0].parts.instrument.clone();
        g.slots[0]
            .sub_positions
            .push(active_sub_of(&inst, "sub_ext_1", Side::Long, 1.0));
        g.slots[0].last_mark = Some(102.0);
        g.slots[0].last_bar_at = Some(at);
        let opened = utc(2026, 1, 4, 23, 0);
        g.slots[0].pos_sync.pos = Some(strategy_positions::PositionState {
            position_id: "sub_ext_1".into(),
            symbol: inst.namespaced_key(),
            side: strategy_positions::Side::Long,
            entry_price: 100.0,
            entry_time: opened,
            entry_timestamp: opened.timestamp(),
            peak_price: 104.0,
            stop_price: 99.0,
            max_pnl_pct: 4.0,
            min_pnl_pct: -2.0,
            strategy_id: Some("xsec_momentum:default".into()),
            atr_at_entry: 1.0,
            total_size: 1.0,
            remaining_size: 1.0,
            realized_pnl: 0.0,
            trail_activated: false,
            trail_trigger_pct: 0.0,
            entry_size_raw: None,
            remaining_size_raw: None,
            exit_active: false,
            exit_price: 0.0,
        });
        g.slots[0].parts.execution = Box::new(TestExecution {
            orders: Arc::new(Mutex::new(Vec::new())),
            fills: Mutex::new(Vec::new()),
            accept: true,
            remote: Vec::new(), // 远程同向空仓 = 外部平仓
            in_flight: Vec::new(),
            venue_notional: None,
        });

        g.reconcile_remote().unwrap();

        let rec = store
            .table::<strategy_positions::HistoryPosition>()
            .get("sub_ext_1")
            .expect("外部平仓也必须落 history_position");
        assert_eq!(rec.exit_price, 102.0);
        assert_eq!(rec.peak_price, 104.0);
        assert_eq!(rec.exit_reason, "external_close");
        assert!(!rec.is_stop_loss);
        assert_eq!(rec.exit_size, 1.0);
    }
}
