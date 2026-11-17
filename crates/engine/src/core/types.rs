//! 运行时公共类型：策略端口、决策上下文、策略动作、运行摘要。
//!
//! v2.12（P1-4）从 `runtime.rs` 拆出——`runtime.rs` 只保留
//! `ExchangeRunner` 编排壳与子模块调度。

use chrono::DateTime;
use strategy_core::signal::Side;
use strategy_core::strategy::{Cooldown, PositionView};

use crate::core::aggregation::AggregationState;
use crate::core::bar::Bar;
use crate::core::bar::Timeframe;
use crate::core::execution::Account;
use crate::core::identity::InstrumentId;
use crate::core::session::TradingDay;

// ─── 策略端口 ───

/// 策略与引擎之间的接口：策略实现此 trait，引擎逐 bar 调用。
pub trait StrategyPort {
    fn on_closed_bars(
        &mut self,
        newly_closed: &[Timeframe],
        state: &AggregationState,
        ctx: &DecisionContext,
    ) -> Vec<StrategyAction>;

    fn on_tick(&mut self, _bar: &Bar) {}
}

// ─── 决策上下文 ───

#[derive(Debug, Clone)]
pub struct DecisionContext {
    pub instrument: InstrumentId,
    pub at: DateTime<chrono::Utc>,
    pub price: f64,
    pub trading_day: TradingDay,
    pub no_trade: bool,
    pub entry_confirm: bool,
    pub signal_resume: bool,
    pub bar: Bar,
    pub position: Option<PositionView>,
    pub cooldown: Option<Cooldown>,
    pub session_open_price: Option<f64>,
}

// ─── 策略动作 ───

#[derive(Debug, Clone, PartialEq)]
pub enum StrategyAction {
    Open {
        side: Side,
        price: f64,
        stop_price: f64,
        atr: f64,
        strength: f64,
        reason: String,
    },
    Close {
        price: f64,
        reason: String,
        is_stop_loss: bool,
        strength: f64,
    },
    Reverse {
        side: Side,
        price: f64,
        stop_price: f64,
        atr: f64,
        strength: f64,
        close_reason: String,
        is_stop_loss: bool,
    },
    UpdateStop {
        stop_price: f64,
    },
    UpdateExit {
        exit_active: bool,
        exit_price: f64,
    },
    Hold,
}

// ─── 运行摘要 ───

#[derive(Debug, Clone, Default)]
pub struct RunSummary {
    pub instrument: Option<InstrumentId>,
    pub bars_processed: u64,
    pub provisional_bars: u64,
    pub actions: u64,
    pub rejected_actions: u64,
    pub fills: Vec<crate::core::execution::Fill>,
    pub account: Option<Account>,
    pub final_positions: Vec<crate::core::position::PositionLot>,
    pub session_gaps: u64,
    pub day_boundaries: u64,
    pub rolls: u64,
}
