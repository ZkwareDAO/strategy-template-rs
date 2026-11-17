//! 仓位同步模块 —— 从 `ExchangeRunner::on_bar` 提取的职责 #3。
//!
//! 成交回报落地到仓位状态（开/加/平），止损成交记冷却日，
//! 持仓账本镜像同步。复用 `positions::PositionState`，避免出现第四套盈亏公式。

use std::collections::HashMap;

use strategy_core::signal::Side;
use strategy_core::strategy::Cooldown;
use strategy_positions::{PositionState, Side as PosSide};

use crate::core::execution::Fill;
use crate::core::identity::InstrumentId;
use crate::core::position::PositionBook;
use crate::core::trading::Offset;

/// 仓位同步子系统。
///
/// 持有仓位状态机 (`pos`)、持仓账本 (`book`)、冷却日 (`cooldown`)、
/// 开仓侧信道 (`open_meta`)。
pub struct PositionSync {
    /// 仓位状态机（峰值 / 盈亏极值 / 止损 / 回落止盈 / SAR 离场）。
    pub pos: Option<PositionState>,
    /// 持仓账本（镜像 `remote_positions`）。
    pub book: PositionBook,
    /// 止损平仓后的冷却日。
    pub cooldown: Option<Cooldown>,
    /// signal_id -> (stop_price, atr, strength)：成交落地时建持仓状态用。
    pub open_meta: HashMap<String, (f64, f64, f64)>,
}

fn to_pos_side(side: Side) -> PosSide {
    match side {
        Side::Long => PosSide::Long,
        Side::Short => PosSide::Short,
    }
}

impl PositionSync {
    pub fn new(book: PositionBook) -> Self {
        Self {
            pos: None,
            book,
            cooldown: None,
            open_meta: HashMap::new(),
        }
    }

    /// 外部注入仓位状态机（live 恢复用）。
    pub fn seed(&mut self, pos: Option<PositionState>, cooldown: Option<Cooldown>) {
        self.pos = pos;
        self.cooldown = cooldown;
    }

    /// 读取并清除开仓侧信道（stop/atr/strength）。
    pub fn take_open_meta(&mut self, signal_id: &str) -> Option<(f64, f64, f64)> {
        self.open_meta.remove(signal_id)
    }

    /// 峰值 / 盈亏极值逐根维护（禁交易窗口内同样更新）。
    pub fn on_price(&mut self, high: f64, low: f64) {
        if let Some(p) = self.pos.as_mut() {
            p.on_price(high, low);
        }
    }

    /// 成交回报落地到仓位状态（开/加/平）。开仓的 stop/atr 来自动作侧信道
    /// （`open_meta`），与引擎 `apply_open_fill(fill, stop_price, atr)` 同构。
    pub fn apply_fill(&mut self, f: &Fill) {
        match f.offset {
            Offset::Open => {
                let (stop, atr, _strength) = self
                    .open_meta
                    .remove(&f.signal_id)
                    .unwrap_or((0.0, 0.0, 0.0));
                match self.pos.as_mut() {
                    Some(p) if p.side == to_pos_side(f.side) && p.remaining_size > 0.0 => {
                        // 同向加仓：均价摊入，止损取最新设定（引擎同义）
                        let new_total = p.total_size + f.qty;
                        if new_total > 1e-12 {
                            p.entry_price =
                                (p.entry_price * p.total_size + f.price * f.qty) / new_total;
                        }
                        p.total_size = new_total;
                        p.remaining_size += f.qty;
                        p.stop_price = stop;
                        // v2.2 C7：加仓跨多笔成交，剩余字面量无法从单笔回报
                        // 积分 → 置 None（宁缺勿伪），live 以逐笔回报为准。
                        p.remaining_size_raw = None;
                    }
                    _ => {
                        self.pos = Some(PositionState {
                            position_id: f.signal_id.clone(),
                            symbol: f.instrument.namespaced_key(),
                            side: to_pos_side(f.side),
                            entry_price: f.price,
                            entry_time: f.at,
                            entry_timestamp: f.at.timestamp(),
                            peak_price: f.price,
                            stop_price: stop,
                            max_pnl_pct: 0.0,
                            min_pnl_pct: 0.0,
                            strategy_id: None,
                            atr_at_entry: atr,
                            total_size: f.qty,
                            remaining_size: f.qty,
                            realized_pnl: 0.0,
                            trail_activated: false,
                            trail_trigger_pct: 0.0,
                            entry_size_raw: f.qty_raw.clone(),
                            remaining_size_raw: f.qty_raw.clone(),
                            exit_active: false,
                            exit_price: 0.0,
                        });
                    }
                }
            }
            _ => {
                if let Some(p) = self.pos.as_mut() {
                    p.remaining_size = (p.remaining_size - f.qty).max(0.0);
                    p.realized_pnl += f.realized_pnl;
                    let closed_raw = f.qty_raw.clone().unwrap_or_else(|| f.qty.to_string());
                    p.remaining_size_raw = strategy_positions::subtract_qty_raw(
                        p.remaining_size_raw.as_deref(),
                        &closed_raw,
                    );
                    if f.is_stop_loss {
                        self.cooldown =
                            Some(Cooldown::UntilDate(f.at.format("%Y-%m-%d").to_string()));
                    }
                    if p.remaining_size <= 1e-12 {
                        self.pos = None;
                    }
                }
            }
        }
    }

    /// 从 `ExecutionPort::remote_positions` 同步持仓账本。
    pub fn sync_book(&mut self, lots: Vec<crate::core::position::PositionLot>) {
        self.book.lots = lots;
    }

    /// 仓位总剩余量（平仓请求用）。
    pub fn total_remaining(&self) -> f64 {
        self.book.total_remaining()
    }

    /// 持仓视图（给 `DecisionContext` 用）。
    pub fn position_view(&self) -> Option<strategy_core::strategy::PositionView> {
        self.pos
            .as_ref()
            .map(|p| strategy_core::strategy::PositionView {
                side: match p.side {
                    PosSide::Long => Side::Long,
                    PosSide::Short => Side::Short,
                },
                entry_price: p.entry_price,
                stop_price: p.stop_price,
                peak_price: p.peak_price,
                max_pnl_pct: p.max_pnl_pct,
                min_pnl_pct: p.min_pnl_pct,
                exit_active: p.exit_active,
                exit_price: p.exit_price,
            })
    }
}

/// 辅助：构造 `PositionBook`（从 `ExchangeParts`）。
pub fn book_for(
    instrument: &InstrumentId,
    rules: &dyn crate::core::position::PositionRules,
) -> PositionBook {
    PositionBook::new(instrument.clone(), rules.mode())
}
