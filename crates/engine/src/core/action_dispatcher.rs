//! 动作分派模块 —— 从 `ExchangeRunner::on_bar` 提取的职责 #2。
//!
//! 策略动作（Open/Close/Reverse/UpdateStop/UpdateExit/Hold）的分发与执行。
//! sizing 计算、数量规整、signal_id 生成、OrderRequest 构造与提交。

use strategy_core::signal::{generate_signal_id, Side, SignalType};
use strategy_core::strategy::PositionView;

use crate::core::bar::Bar;
use crate::core::error::Result;
use crate::core::execution::{OrderContext, OrderRequest};
use crate::core::identity::InstrumentId;
use crate::core::position::{CloseRequest, PositionBook};
use crate::core::trading::{Offset, Rounding, SizingParams, SizingRequest};
use crate::core::types::{DecisionContext, StrategyAction};

/// 动作分派子系统。
///
/// 持有开仓装配参数 (`sizing`) 与策略类型名 (`strategy_type`)。
/// 订单出口 (`execution`)、交易规则 (`trading`)、仓位规则 (`position`)
/// 均在 `ExchangeParts` 中，本模块通过 `&ExchangeParts` 引用访问。
pub struct ActionDispatcher {
    /// 开仓装配参数（leverage / 每单名义）。
    pub sizing: SizingParams,
    /// 策略类型名（进入 signal_id 哈希）。
    pub strategy_type: String,
}

impl ActionDispatcher {
    pub fn new(strategy_type: impl Into<String>) -> Self {
        Self {
            sizing: SizingParams::default(),
            strategy_type: strategy_type.into(),
        }
    }

    /// 确定性 signal_id：SHA256(策略类型_命名空间键_K线时间_信号类型)。
    fn next_signal_id(
        &self,
        instrument: &InstrumentId,
        ctx: &DecisionContext,
        signal_type: SignalType,
    ) -> String {
        generate_signal_id(
            &self.strategy_type,
            &instrument.namespaced_key(),
            ctx.at,
            signal_type,
        )
    }

    fn order_ctx(&self, ctx: &DecisionContext, bar: &Bar) -> OrderContext {
        OrderContext {
            at: ctx.at,
            trigger_price: ctx.price,
            reference_bar: Some(bar.clone()),
            trading_day: ctx.trading_day,
        }
    }

    /// 分派策略动作。`pos_view` 用于平仓 signal_type 判定，
    /// `book`/`open_meta` 通过可变引用传入（成交落地侧信道写入）。
    #[allow(clippy::too_many_arguments)]
    pub fn dispatch(
        &mut self,
        action: StrategyAction,
        bar: &Bar,
        ctx: &DecisionContext,
        // ExchangeParts 的三个端口引用
        trading: &dyn crate::core::trading::TradingRules,
        execution: &mut dyn crate::core::execution::ExecutionPort,
        position: &dyn crate::core::position::PositionRules,
        session: &dyn crate::core::session::SessionRules,
        // 仓位状态
        pos_view: Option<PositionView>,
        book: &PositionBook,
        open_meta: &mut std::collections::HashMap<String, (f64, f64, f64)>,
        // summary 计数器（&mut u64 引用太碎，改返回值）
    ) -> Result<DispatchOutcome> {
        match action {
            StrategyAction::Hold => Ok(DispatchOutcome::default()),
            StrategyAction::UpdateStop { stop_price } => {
                // 就地更新由 PositionSync 负责，这里只返回意图
                Ok(DispatchOutcome {
                    actions: 0,
                    rejected: 0,
                    update_stop: Some(stop_price),
                    update_exit: None,
                })
            }
            StrategyAction::UpdateExit {
                exit_active,
                exit_price,
            } => Ok(DispatchOutcome {
                actions: 0,
                rejected: 0,
                update_stop: None,
                update_exit: Some((exit_active, exit_price)),
            }),
            StrategyAction::Open {
                side,
                price,
                stop_price,
                atr,
                strength,
                ..
            } => {
                let accepted = self.submit_open(
                    side,
                    price,
                    stop_price,
                    atr,
                    strength,
                    ctx,
                    bar,
                    trading,
                    execution,
                    &ctx.instrument,
                    open_meta,
                )?;
                Ok(DispatchOutcome {
                    actions: 1,
                    rejected: if accepted { 0 } else { 1 },
                    ..Default::default()
                })
            }
            StrategyAction::Close {
                price,
                is_stop_loss,
                ..
            } => {
                let accepted = self.submit_close(
                    price,
                    is_stop_loss,
                    ctx,
                    bar,
                    trading,
                    execution,
                    position,
                    session,
                    &ctx.instrument,
                    pos_view,
                    book,
                )?;
                Ok(DispatchOutcome {
                    actions: 1,
                    rejected: if accepted { 0 } else { 1 },
                    ..Default::default()
                })
            }
            StrategyAction::Reverse {
                side,
                price,
                stop_price,
                atr,
                strength,
                close_reason: _,
                is_stop_loss,
            } => {
                let closed_ok = self.submit_close(
                    price,
                    is_stop_loss,
                    ctx,
                    bar,
                    trading,
                    execution,
                    position,
                    session,
                    &ctx.instrument,
                    pos_view,
                    book,
                )?;
                if closed_ok {
                    self.submit_open(
                        side,
                        price,
                        stop_price,
                        atr,
                        strength,
                        ctx,
                        bar,
                        trading,
                        execution,
                        &ctx.instrument,
                        open_meta,
                    )?;
                }
                Ok(DispatchOutcome {
                    actions: if closed_ok { 2 } else { 1 },
                    rejected: if closed_ok { 0 } else { 1 },
                    ..Default::default()
                })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_open(
        &mut self,
        side: Side,
        price: f64,
        stop_price: f64,
        atr: f64,
        strength: f64,
        ctx: &DecisionContext,
        bar: &Bar,
        trading: &dyn crate::core::trading::TradingRules,
        execution: &mut dyn crate::core::execution::ExecutionPort,
        instrument: &InstrumentId,
        open_meta: &mut std::collections::HashMap<String, (f64, f64, f64)>,
    ) -> Result<bool> {
        let qty = if execution.venue_sized() {
            1.0
        } else {
            trading.sizing(&SizingRequest {
                instrument: instrument.clone(),
                price,
                equity: execution
                    .account_snapshot()
                    .map(|a| a.equity())
                    .unwrap_or(0.0),
                leverage: self.sizing.leverage,
                notional_per_order: self.sizing.notional_per_order,
            })?
        };
        let qty = trading.normalize_qty(instrument, qty, Rounding::Floor)?;
        if qty <= 0.0 {
            return Ok(false);
        }
        let price = trading.normalize_price(instrument, price)?;
        let signal_type = match side {
            Side::Long => SignalType::Buy,
            Side::Short => SignalType::Sell,
        };
        let signal_id = self.next_signal_id(instrument, ctx, signal_type);
        open_meta.insert(signal_id.clone(), (stop_price, atr, strength));
        let order = OrderRequest {
            signal_id,
            instrument: instrument.clone(),
            side,
            offset: Offset::Open,
            price,
            qty,
            close_leg: None,
            reduce_only: false,
            is_stop_loss: false,
            qty_raw: None,
            strategy_id: None,
            intent: None,
        };
        let ack = execution.submit(&order, &self.order_ctx(ctx, bar))?;
        Ok(ack.accepted)
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_close(
        &mut self,
        price: f64,
        is_stop_loss: bool,
        ctx: &DecisionContext,
        bar: &Bar,
        trading: &dyn crate::core::trading::TradingRules,
        execution: &mut dyn crate::core::execution::ExecutionPort,
        position: &dyn crate::core::position::PositionRules,
        session: &dyn crate::core::session::SessionRules,
        instrument: &InstrumentId,
        pos_view: Option<PositionView>,
        book: &PositionBook,
    ) -> Result<bool> {
        let legs = position.split_close(
            book,
            &CloseRequest {
                instrument: instrument.clone(),
                qty: book.total_remaining(),
                at: ctx.at,
            },
            session,
        )?;
        let price = trading.normalize_price(instrument, price)?;
        let signal_type = match pos_view.map(|p| p.side) {
            Some(Side::Long) => SignalType::SellClose,
            Some(Side::Short) => SignalType::BuyClose,
            None => SignalType::Flat,
        };
        let base_id = self.next_signal_id(instrument, ctx, signal_type);
        let mut all_accepted = true;
        let mut emitted = 0usize;
        for mut leg in legs {
            let qty = trading.normalize_qty(instrument, leg.qty, Rounding::Floor)?;
            if qty <= 1e-12 {
                all_accepted = false;
                continue;
            }
            leg.qty = qty;
            let leg_raw = leg.qty_raw.clone();
            let order = OrderRequest {
                signal_id: format!("{base_id}_leg{emitted}"),
                instrument: instrument.clone(),
                side: leg.side,
                offset: leg.offset,
                price,
                qty,
                close_leg: Some(leg),
                reduce_only: true,
                is_stop_loss,
                qty_raw: leg_raw,
                strategy_id: None,
                intent: None,
            };
            emitted += 1;
            let ack = execution.submit(&order, &self.order_ctx(ctx, bar))?;
            if !ack.accepted {
                all_accepted = false;
            }
        }
        Ok(all_accepted && emitted > 0)
    }
}

/// 动作分派结果（给 runner 更新 summary 与仓位状态用）。
#[derive(Debug, Default, Clone)]
pub struct DispatchOutcome {
    pub actions: u64,
    pub rejected: u64,
    /// UpdateStop 的目标价位（由 runner 回写 PositionSync）。
    pub update_stop: Option<f64>,
    /// UpdateExit 的 (active, price)（由 runner 回写 PositionSync）。
    pub update_exit: Option<(bool, f64)>,
}
