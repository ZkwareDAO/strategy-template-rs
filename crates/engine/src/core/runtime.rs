//! 统一主循环 —— 实盘与回测跑的是**同一段代码**。
//!
//! v2.1 / v2.6 历史注释见 git log。v2.12（P1-4）重构：
//! `on_bar` 的三类职责拆分到独立模块——
//! - [`RiskGuard`]：风控兜底（固定止损 > 回落止盈 > 固定止盈）+ 开盘价记录
//! - [`ActionDispatcher`]：动作分发（Open/Close/Reverse/UpdateStop/UpdateExit）+ sizing + signal_id
//! - [`PositionSync`]：成交回报落地 + 仓位状态机维护 + 持仓账本镜像
//!
//! 公共类型（`StrategyPort`/`DecisionContext`/`StrategyAction`/`RunSummary`）
//! 已拆至 [`types`] 模块。

use strategy_core::risk::RiskControlConfig;
use strategy_core::strategy::Cooldown;
use strategy_positions::PositionState;

use crate::core::action_dispatcher::ActionDispatcher;
use crate::core::aggregation::AggregationState;
use crate::core::bar::{Bar, BarEventSource, MarketEvent, Timeframe};
use crate::core::error::Result;
use crate::core::exchange::ExchangeParts;
use crate::core::execution::Account;
use crate::core::position::PositionBook;
use crate::core::position_sync::{book_for, PositionSync};
use crate::core::risk_guard::RiskGuard;
use crate::core::session::TradingDay;
use crate::core::trading::SizingParams;
use crate::core::types::{DecisionContext, RunSummary, StrategyAction, StrategyPort};

/// 一个标的的运行时。多标的 = 多个 `ExchangeRunner`。
pub struct ExchangeRunner {
    pub parts: ExchangeParts,
    pub state: AggregationState,
    pub timeframes: Vec<Timeframe>,
    pub summary: RunSummary,
    pub pos_sync: PositionSync,
    pub risk_guard: RiskGuard,
    pub dispatcher: ActionDispatcher,
    last_day: Option<TradingDay>,
    pending_newly: Vec<Timeframe>,
    blocked_closed: Vec<Timeframe>,
    signal_blocked_prev: bool,
}

// ─── 向后兼容的字段访问代理 ───

impl ExchangeRunner {
    pub fn pos(&self) -> Option<&PositionState> {
        self.pos_sync.pos.as_ref()
    }
    pub fn book(&self) -> &PositionBook {
        &self.pos_sync.book
    }
    pub fn cooldown(&self) -> Option<&Cooldown> {
        self.pos_sync.cooldown.as_ref()
    }
    pub fn risk(&self) -> &strategy_core::risk::RiskController {
        &self.risk_guard.risk
    }
    pub fn sizing(&self) -> &SizingParams {
        &self.dispatcher.sizing
    }
}

impl ExchangeRunner {
    pub fn new(parts: ExchangeParts, timeframes: Vec<Timeframe>, max_closed_len: usize) -> Self {
        Self::with_strategy_type(parts, timeframes, max_closed_len, "unknown")
    }

    pub fn with_strategy_type(
        parts: ExchangeParts,
        timeframes: Vec<Timeframe>,
        max_closed_len: usize,
        strategy_type: impl Into<String>,
    ) -> Self {
        let book = book_for(&parts.instrument, parts.position.as_ref());
        let instrument = Some(parts.instrument.clone());
        Self {
            state: AggregationState::new(&timeframes, max_closed_len),
            pos_sync: PositionSync::new(book),
            risk_guard: RiskGuard::new(),
            dispatcher: ActionDispatcher::new(strategy_type),
            parts,
            timeframes,
            summary: RunSummary {
                instrument,
                ..RunSummary::default()
            },
            last_day: None,
            pending_newly: Vec::new(),
            blocked_closed: Vec::new(),
            signal_blocked_prev: false,
        }
    }

    pub fn with_risk_config(mut self, cfg: RiskControlConfig) -> Self {
        self.risk_guard = RiskGuard::with_config(cfg);
        self
    }

    pub fn with_sizing_params(mut self, p: SizingParams) -> Self {
        self.dispatcher.sizing = p;
        self
    }

    pub fn preload(&mut self, bars: &[Bar]) -> Result<()> {
        for b in bars {
            self.state.on_closed_1m(
                b.clone(),
                self.parts.aggregation.as_ref(),
                self.parts.session.as_ref(),
            )?;
        }
        Ok(())
    }

    pub fn set_source(&mut self, source: Box<dyn BarEventSource>) {
        self.parts.source = source;
    }

    pub fn subscriptions(&self) -> &[crate::core::subscription::Subscription] {
        &self.parts.subscriptions
    }

    pub fn run(&mut self, strategy: &mut dyn StrategyPort) -> Result<RunSummary> {
        while let Some(event) = self.parts.source.next_event()? {
            self.on_event(event, strategy)?;
        }
        self.finish()
    }

    pub fn on_event(&mut self, event: MarketEvent, strategy: &mut dyn StrategyPort) -> Result<()> {
        match event {
            MarketEvent::Bar(bar) => self.on_bar(bar, strategy),
            MarketEvent::SessionGap(gap) => {
                self.summary.session_gaps += 1;
                let closed = self.state.on_gap(&gap, self.parts.aggregation.as_ref());
                self.queue_pending_newly(closed);
                Ok(())
            }
            MarketEvent::DayBoundary(day) => {
                self.summary.day_boundaries += 1;
                let closed = self.state.on_day_boundary(self.parts.aggregation.as_ref());
                self.queue_pending_newly(closed);
                self.parts.position.rollover(&mut self.pos_sync.book, day);
                self.last_day = Some(day);
                Ok(())
            }
            MarketEvent::Roll(roll) => {
                self.summary.rolls += 1;
                let next = self
                    .parts
                    .subscription
                    .on_roll(&roll, &self.parts.subscriptions)?;
                self.parts.subscriptions = next;
                Ok(())
            }
            // 非阻塞轮询的空转信号：与 Reconnected 一样无决策动作。
            MarketEvent::Reconnected | MarketEvent::SourceIdle => Ok(()),
        }
    }

    pub fn finish(&mut self) -> Result<RunSummary> {
        self.summary.final_positions = self
            .parts
            .execution
            .remote_positions(&self.parts.instrument)
            .unwrap_or_default();
        self.summary.account = self.parts.execution.account_snapshot();
        Ok(self.summary.clone())
    }

    pub fn account(&self) -> Option<Account> {
        self.parts.execution.account_snapshot()
    }

    pub fn seed_position(&mut self, pos: Option<PositionState>, cooldown: Option<Cooldown>) {
        self.pos_sync.seed(pos, cooldown);
    }

    pub fn take_open_meta(&mut self, signal_id: &str) -> Option<(f64, f64, f64)> {
        self.pos_sync.take_open_meta(signal_id)
    }

    // ─── on_bar 编排薄壳（~23 行）───

    fn on_bar(&mut self, bar: Bar, strategy: &mut dyn StrategyPort) -> Result<()> {
        if !bar.is_final() {
            self.summary.provisional_bars += 1;
            strategy.on_tick(&bar);
            return Ok(());
        }
        self.summary.bars_processed += 1;
        self.parts.execution.on_bar(&bar);
        let session = self.parts.session.clone();
        let day = session.trading_day_of(bar.open_time);

        self.handle_session(&bar, day, session.as_ref())?;
        let (newly, ctx) = self.aggregate_and_gate(&bar, day, session.as_ref())?;

        if !ctx.no_trade && !newly.is_empty() {
            self.dispatch_actions(bar.clone(), ctx.clone(), newly, strategy)?;
        }
        self.sync_fills_and_book()
    }

    fn handle_session(
        &mut self,
        bar: &Bar,
        day: TradingDay,
        session: &dyn crate::core::session::SessionRules,
    ) -> Result<()> {
        if self.last_day.is_some_and(|d| d != day) {
            let closed = self.state.on_day_boundary(self.parts.aggregation.as_ref());
            self.queue_pending_newly(closed);
            self.parts.position.rollover(&mut self.pos_sync.book, day);
            self.summary.day_boundaries += 1;
        }
        self.last_day = Some(day);
        if session.is_session_open_time(bar.open_time) {
            self.risk_guard.record_session_open(bar.open);
        }
        self.pos_sync.on_price(bar.high, bar.low);
        Ok(())
    }

    fn aggregate_and_gate(
        &mut self,
        bar: &Bar,
        day: TradingDay,
        session: &dyn crate::core::session::SessionRules,
    ) -> Result<(Vec<Timeframe>, DecisionContext)> {
        let mut newly =
            self.state
                .on_closed_1m(bar.clone(), self.parts.aggregation.as_ref(), session)?;
        for tf in std::mem::take(&mut self.pending_newly) {
            if !newly.contains(&tf) {
                newly.push(tf);
            }
        }
        newly.sort();

        let no_trade = session.is_no_trade_window(bar.open_time);
        let signal_resume = !no_trade && self.signal_blocked_prev;
        self.signal_blocked_prev = no_trade;
        let ctx = self.build_ctx(bar, day, no_trade, signal_resume, session);

        if no_trade {
            for tf in &newly {
                if tf.0 > 1 && !self.blocked_closed.contains(tf) {
                    self.blocked_closed.push(*tf);
                }
            }
        } else {
            let mut merged = std::mem::take(&mut self.blocked_closed);
            for tf in &newly {
                if !merged.contains(tf) {
                    merged.push(*tf);
                }
            }
            merged.sort();
            newly = merged;
        }
        Ok((newly, ctx))
    }

    fn dispatch_actions(
        &mut self,
        bar: Bar,
        ctx: DecisionContext,
        newly: Vec<Timeframe>,
        strategy: &mut dyn StrategyPort,
    ) -> Result<()> {
        let mut actions = strategy.on_closed_bars(&newly, &self.state, &ctx);
        let strategy_silent =
            actions.is_empty() || actions.iter().all(|a| matches!(a, StrategyAction::Hold));
        if strategy_silent && self.pos_sync.pos.is_some() {
            if let Some(exit) = self
                .risk_guard
                .check_fallback(&bar, self.pos_sync.pos.as_mut())
            {
                actions = vec![StrategyAction::Close {
                    price: bar.close,
                    reason: exit.reason,
                    is_stop_loss: exit.is_stop_loss,
                    strength: 0.8,
                }];
            }
        }
        for action in actions {
            let outcome = self.dispatcher.dispatch(
                action,
                &bar,
                &ctx,
                self.parts.trading.as_ref(),
                self.parts.execution.as_mut(),
                self.parts.position.as_ref(),
                self.parts.session.as_ref(),
                self.pos_sync.position_view(),
                &self.pos_sync.book,
                &mut self.pos_sync.open_meta,
            )?;
            self.summary.actions += outcome.actions;
            self.summary.rejected_actions += outcome.rejected;
            if let Some(stop) = outcome.update_stop {
                if let Some(p) = self.pos_sync.pos.as_mut() {
                    p.stop_price = stop;
                }
            }
            if let Some((active, price)) = outcome.update_exit {
                if let Some(p) = self.pos_sync.pos.as_mut() {
                    p.exit_active = active;
                    p.exit_price = price;
                }
            }
        }
        Ok(())
    }

    fn sync_fills_and_book(&mut self) -> Result<()> {
        let fills = self.parts.execution.drain_fills();
        for f in &fills {
            self.pos_sync.apply_fill(f);
        }
        self.summary.fills.extend(fills);
        if let Ok(lots) = self
            .parts
            .execution
            .remote_positions(&self.parts.instrument)
        {
            self.pos_sync.sync_book(lots);
        }
        Ok(())
    }

    fn queue_pending_newly(&mut self, closed: Vec<Timeframe>) {
        for tf in closed {
            if !self.pending_newly.contains(&tf) {
                self.pending_newly.push(tf);
            }
        }
    }

    fn build_ctx(
        &self,
        bar: &Bar,
        day: TradingDay,
        no_trade: bool,
        signal_resume: bool,
        session: &dyn crate::core::session::SessionRules,
    ) -> DecisionContext {
        DecisionContext {
            instrument: self.parts.instrument.clone(),
            at: bar.open_time,
            price: bar.close,
            trading_day: day,
            no_trade,
            entry_confirm: session.is_entry_confirm_time(bar.open_time),
            signal_resume,
            bar: bar.clone(),
            position: self.pos_sync.position_view(),
            cooldown: self.pos_sync.cooldown.clone(),
            session_open_price: self.risk_guard.session_open_price,
        }
    }
}
