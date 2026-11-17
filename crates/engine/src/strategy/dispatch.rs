//! 子仓位下单分发（PR-2/PR-3，Task 9）。
//!
//! 横截面策略的单标的动作——开 / 平 / 调仓——落到 `sub_` 命名的
//! `OrderRequest`。三条铁律：
//! - 1:1 约束：同一 `strategy_id + instrument` 至多一个非 `Closed` 子仓位
//!   （`open_position` 直接拒、`close_position` 防重复平仓）；
//! - 数量/价格全部经 `TradingRules` 规整后才提交；
//! - 平仓腿用 `PositionRules::split_close` 按子仓位账本拆（与旧链同一来源），
//!   腿 ID = `{base}_leg{i}`（`base` 为 `sub_` 确定性 ID，deterministic）。
//!
//! 本模块只做下单，不处理成交回报（`SubPosition` 状态推进在 Task 10+ 由
//! 回报/对账驱动）。

use chrono::{DateTime, Utc};
use strategy_core::signal::Side;

use crate::core::error::{Result, SpiError};
use crate::core::execution::{OrderContext, OrderRequest};
use crate::core::position::{CloseRequest, PositionBook, PositionLot, PositionMode};
use crate::core::trading::{Offset, Rounding};

use super::model::{advance_sub_state, SubPosition, SubPositionEvent, SubPositionState};
use super::runner::StrategySlot;
use super::signal_id::{generate_sub_signal_id, SubIntent};

/// 下单侧标识（进入 `sub_` 哈希的确定性输入）。
pub struct StrategyIds<'a> {
    pub strategy_type: &'a str,
    pub strategy_id: &'a str,
    /// 新子仓归属的总仓；纯平仓路径不使用
    pub master_id: &'a str,
    pub rebalance_epoch: u64,
}

/// 一次派发的结果。`signal_ids` = 被交易所受理（accepted）的腿 signal_id。
#[derive(Debug, Default, Clone, PartialEq)]
pub struct DispatchOutcome {
    pub actions: u64,
    pub rejected: u64,
    pub signal_ids: Vec<String>,
}

/// 开仓：1:1 约束 + 子仓位 PendingOpen → OpenRequested / 拒单删行。
#[allow(clippy::too_many_arguments)]
pub fn open_position(
    slot: &mut StrategySlot,
    ids: &StrategyIds<'_>,
    side: Side,
    price: f64,
    qty: f64,
    stop_price: f64,
    at: DateTime<Utc>,
    reason: &str,
    intent: SubIntent,
) -> Result<DispatchOutcome> {
    let _ = reason;
    let StrategySlot {
        parts,
        pos_sync,
        sub_positions,
        ..
    } = slot;
    let instrument = &parts.instrument;
    let trading = parts.trading.as_ref();
    let session = parts.session.as_ref();

    if ids.master_id.is_empty() {
        return Err(SpiError::Execution(
            "open_position: master_id 未设置".into(),
        ));
    }

    // 1:1 约束：已有非 Closed 子仓位 → 直接拒（组合层应只对空仓发 Open）。
    if sub_positions
        .iter()
        .any(|s| s.instrument == *instrument && s.state != SubPositionState::Closed)
    {
        return Ok(DispatchOutcome {
            rejected: 1,
            ..DispatchOutcome::default()
        });
    }

    let qty = trading.normalize_qty(instrument, qty, Rounding::Floor)?;
    if qty <= 1e-12 {
        return Ok(DispatchOutcome {
            rejected: 1,
            ..DispatchOutcome::default()
        });
    }
    let price = trading.normalize_price(instrument, price)?;

    let signal_id = generate_sub_signal_id(
        ids.strategy_type,
        ids.strategy_id,
        &instrument.namespaced_key(),
        at,
        side,
        intent,
        ids.rebalance_epoch,
    );

    // 先建行：PendingOpen + open_meta 侧信道，成交落地（Task 10+）读侧信道建仓。
    sub_positions.push(SubPosition {
        id: signal_id.clone(),
        strategy_id: ids.strategy_id.to_string(),
        master_id: ids.master_id.to_string(),
        instrument: instrument.clone(),
        side,
        qty,
        entry_price: price,
        stop_price: Some(stop_price),
        target_weight: 0.0,
        rebalance_epoch: ids.rebalance_epoch,
        state: SubPositionState::PendingOpen,
        opened_at: None,
        closed_at: None,
        realized_pnl: None,
    });
    pos_sync
        .open_meta
        .insert(signal_id.clone(), (stop_price, 0.0, 0.0));

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
        strategy_id: Some(ids.strategy_id.to_string()),
        intent: Some(intent.as_str().to_string()),
    };
    let ctx = OrderContext {
        at,
        trigger_price: price,
        reference_bar: None,
        trading_day: session.trading_day_of(at),
    };
    let ack = parts.execution.as_mut().submit(&order, &ctx)?;
    if ack.accepted {
        if let Some(s) = sub_positions
            .iter_mut()
            .find(|s| s.instrument == *instrument && s.state == SubPositionState::PendingOpen)
        {
            if let Some(next) = advance_sub_state(s.state, &SubPositionEvent::SubmitAccepted) {
                s.state = next;
            }
        }
        Ok(DispatchOutcome {
            actions: 1,
            signal_ids: vec![order.signal_id],
            ..DispatchOutcome::default()
        })
    } else {
        // 拒单：删这一行（PendingOpen 不占位），计 rejected；open_meta 侧信道
        // 同 key 同步清理，避免成交落地读到幽灵条目。
        sub_positions.retain(|s| s.id != order.signal_id);
        pos_sync.open_meta.remove(&order.signal_id);
        Ok(DispatchOutcome {
            rejected: 1,
            ..DispatchOutcome::default()
        })
    }
}

/// 平仓：防重复平仓 + 子仓位 CloseRequested → 拆腿下单；
/// 全部腿被拒则回滚 `Active`（下一轮可重试）。
///
/// 只允许在 `Active` 子仓位上动作：未成交（`PendingOpen`/`OpenRequested`）
/// 直接拒（不发单、不改状态）。`ids.master_id` 非空时只平该总仓关联的
/// 子仓——总仓先 `Closing`、再逐子仓平仓的语义由调用方保证；空串表示
/// 兼容旧链路（无总仓指针时不按归属过滤）。`at` 取 `slot.last_bar_at`——
/// dispatch 层不携带 bar 上下文，平仓时点由最近一根已闭合 bar 给出；
/// 未设置属调用/装配顺序错误，报引擎错误（不 unwrap/panic）。
pub fn close_position(
    slot: &mut StrategySlot,
    ids: &StrategyIds<'_>,
    price: f64,
    reason: &str,
    is_stop_loss: bool,
    intent: SubIntent,
) -> Result<DispatchOutcome> {
    let _ = reason;
    let at = slot
        .last_bar_at
        .ok_or_else(|| SpiError::Execution("StrategySlot.last_bar_at 未设置，无法平仓".into()))?;

    // 1:1 约束 + 总仓归属：无子仓位、已 CloseRequested / Closed、或属于其他
    // 总仓 → 直接返回（防重复平仓，也防止平错总仓）。
    let Some(sub) = slot.sub_positions.iter().find(|s| {
        s.instrument == slot.parts.instrument
            && s.state != SubPositionState::Closed
            && (ids.master_id.is_empty() || s.master_id == ids.master_id)
    }) else {
        return Ok(DispatchOutcome::default());
    };
    if sub.state == SubPositionState::CloseRequested {
        return Ok(DispatchOutcome::default());
    }
    if sub.state != SubPositionState::Active {
        // 未成交（PendingOpen/OpenRequested）：不发单、不改状态，计一次拒。
        return Ok(DispatchOutcome {
            rejected: 1,
            ..DispatchOutcome::default()
        });
    }
    let qty = sub.qty;

    let mut out = close_qty_inner(slot, ids, qty, price, at, is_stop_loss, intent)?;
    if out.actions == 0 {
        out.rejected += 1;
        return Ok(out);
    }
    // 提交成功后推进：拆腿/下单 Err 时保持 Active，下一轮可重试。
    if let Some(s) = slot.sub_positions.iter_mut().find(|s| {
        s.instrument == slot.parts.instrument
            && s.state == SubPositionState::Active
            && (ids.master_id.is_empty() || s.master_id == ids.master_id)
    }) {
        if let Some(next) = advance_sub_state(s.state, &SubPositionEvent::CloseRequested) {
            s.state = next;
        }
    }
    Ok(out)
}

/// 调仓到目标**带符号净数量**（`target_qty`：多 +、空 −、0 = 平）。
///
/// 侧感知分派（`current_signed` = +qty / −qty）：
/// - 已在目标（|target−current| <= 1e-12）→ no-op；
/// - 空仓 & target≠0 → 按 `sign(target)` 开仓（qty=|target|，`open_position`）；
/// - target≈0 且持仓 → 全平（`close_qty_inner` 平满仓）；
/// - 同号：`|target|>|current|` → `add_to_position` 顺向加仓；`|target|<|current|`
///   → `close_qty_inner` 部分平仓；`|target|==|current|` → no-op；
/// - **异号（多↔空）本期不支持**：soft-reject（`rejected=1`、无订单），
///   错误语义 `SpiError::Execution("adjust_position 不支持同轮反向翻仓")`。
///
/// 上调差额按仓位状态分流：**无**既有子仓位 → `open_position`（全新 1:1 开仓）；
/// **有**非 Closed 子仓位 → `add_to_position` 加仓——提交差额 Open 单，
/// 不新建第二行（G1 约束），Task 10 成交按 instrument 加权并入既有子仓位。
pub fn adjust_position(
    slot: &mut StrategySlot,
    ids: &StrategyIds<'_>,
    target_qty: f64,
    price: f64,
    at: DateTime<Utc>,
    reason: &str,
    intent: SubIntent,
) -> Result<DispatchOutcome> {
    let _ = intent;
    let existing = slot
        .sub_positions
        .iter()
        .find(|s| s.instrument == slot.parts.instrument && s.state != SubPositionState::Closed);
    let current_side = existing.map(|s| s.side);
    let current_signed: f64 = existing
        .map(|s| match s.side {
            Side::Long => s.qty,
            Side::Short => -s.qty,
        })
        .unwrap_or(0.0);

    // 1) 已在目标净数量 → no-op。
    if (target_qty - current_signed).abs() <= 1e-12 {
        return Ok(DispatchOutcome::default());
    }
    // 2) 空仓 → 按符号开仓（多/空都可通过 adjust 从 0 开）。
    if current_signed == 0.0 {
        let side = if target_qty > 0.0 {
            Side::Long
        } else {
            Side::Short
        };
        return open_position(
            slot,
            ids,
            side,
            price,
            target_qty.abs(),
            0.0,
            at,
            reason,
            SubIntent::Open,
        );
    }
    // 3) target ≈ 0（持仓）→ 全平。
    if target_qty.abs() <= 1e-12 {
        return close_qty_inner(
            slot,
            ids,
            current_signed.abs(),
            price,
            at,
            false,
            SubIntent::RebalanceReduce,
        );
    }
    // 4) 异号 → 同轮反向翻仓本期不支持：soft-reject，不发单。
    if (current_signed > 0.0) != (target_qty > 0.0) {
        return Ok(DispatchOutcome {
            rejected: 1,
            ..DispatchOutcome::default()
        });
    }
    // 5) 同号：|target| 加深 → 顺向加仓；减浅 → 部分平仓。
    let side = current_side.unwrap_or(Side::Long); // current_signed≠0 必有既有子仓位
    let spread = target_qty.abs() - current_signed.abs();
    if spread > 1e-12 {
        add_to_position(slot, ids, side, price, spread, at, reason)
    } else if spread < -1e-12 {
        close_qty_inner(
            slot,
            ids,
            -spread,
            price,
            at,
            false,
            SubIntent::RebalanceReduce,
        )
    } else {
        Ok(DispatchOutcome::default())
    }
}

/// 内部：向既有子仓位加仓（rebalance 目标上调时专用）。
///
/// 提交一笔差额 **Open** 订单（新 `sub_` signal_id、`intent=Open`、
/// `strategy_id` 置位），**不新建** `SubPosition` 行、不写 `open_meta`——
/// 该 Open 成交通报无匹配 PendingOpen 行，Task 10 的 fill handler 按
/// instrument 加权并入既有 Active 子仓位，不产生第二行（G1）。
fn add_to_position(
    slot: &mut StrategySlot,
    ids: &StrategyIds<'_>,
    side: Side,
    price: f64,
    qty: f64,
    at: DateTime<Utc>,
    reason: &str,
) -> Result<DispatchOutcome> {
    let _ = reason;
    let StrategySlot { parts, .. } = slot;
    let instrument = &parts.instrument;
    let trading = parts.trading.as_ref();
    let session = parts.session.as_ref();

    let qty = trading.normalize_qty(instrument, qty, Rounding::Floor)?;
    if qty <= 1e-12 {
        // 加仓差额低于最小步长（floor 归零）：目标已视为达标，属 no-op，
        // 不得计拒单——否则每次调仓轮都产生一笔“拒单”噪音（减仓同口径）。
        return Ok(DispatchOutcome::default());
    }
    let price = trading.normalize_price(instrument, price)?;

    let signal_id = generate_sub_signal_id(
        ids.strategy_type,
        ids.strategy_id,
        &instrument.namespaced_key(),
        at,
        side,
        SubIntent::Open,
        ids.rebalance_epoch,
    );
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
        strategy_id: Some(ids.strategy_id.to_string()),
        intent: Some(SubIntent::Open.as_str().to_string()),
    };
    let ctx = OrderContext {
        at,
        trigger_price: price,
        reference_bar: None,
        trading_day: session.trading_day_of(at),
    };
    let ack = parts.execution.as_mut().submit(&order, &ctx)?;
    if ack.accepted {
        Ok(DispatchOutcome {
            actions: 1,
            signal_ids: vec![order.signal_id],
            ..DispatchOutcome::default()
        })
    } else {
        Ok(DispatchOutcome {
            rejected: 1,
            ..DispatchOutcome::default()
        })
    }
}

/// 内部：对子仓位发 `qty` 的平仓腿（`split_close` 拆腿，`{base}_leg{i}` 命名）。
///
/// 子仓位是 1:1 的开仓 signal_id，账本由子仓位视图构造（本层未接成交回报，
/// 不依赖 `pos_sync.book` 镜像）；腿侧 `base` = 持仓方向反向的确定性 `sub_` ID。
fn close_qty_inner(
    slot: &mut StrategySlot,
    ids: &StrategyIds<'_>,
    qty: f64,
    price: f64,
    at: DateTime<Utc>,
    is_stop_loss: bool,
    intent: SubIntent,
) -> Result<DispatchOutcome> {
    let StrategySlot {
        parts,
        pos_sync,
        sub_positions,
        ..
    } = slot;
    let instrument = &parts.instrument;
    let trading = parts.trading.as_ref();
    let session = parts.session.as_ref();
    let position_rules = parts.position.as_ref();

    let qty = trading.normalize_qty(instrument, qty, Rounding::Floor)?;
    if qty <= 1e-12 {
        return Ok(DispatchOutcome::default());
    }
    let price = trading.normalize_price(instrument, price)?;

    let sub = sub_positions
        .iter()
        .find(|s| s.instrument == *instrument && s.state != SubPositionState::Closed)
        .ok_or_else(|| SpiError::Execution("StrategySlot: 无可用子仓位可平".into()))?;

    // 双仓（CTP）优先用交易所成交账本拆腿：`on_close_fill` 只按腿声明的
    // `PositionLot.id` 精确扣减，而子仓位 ID 是 signal_id，与成交账本
    // 批次 ID 不同源——用它拆腿回填时会「超出腿声明批次」。净仓模式
    // 继续用子仓位视图：腿的 entry_price 取子仓位加权均价，成交账本
    // 首批次价格会在多次加仓后算错平仓盈亏。账本尚未同步（单测 /
    // 首笔回报之前）也退回视图，保持既有测试路径。
    let book = if position_rules.mode() == PositionMode::Dual
        && pos_sync.book.lots.iter().any(|l| l.remaining > 1e-12)
    {
        pos_sync.book.clone()
    } else {
        let mut view = PositionBook::new(instrument.clone(), position_rules.mode());
        view.lots.push(PositionLot {
            id: sub.id.clone(),
            instrument: instrument.clone(),
            side: sub.side,
            entry_price: sub.entry_price,
            qty: sub.qty,
            remaining: sub.qty,
            opened_at: sub.opened_at.unwrap_or(at),
            trading_day: session.trading_day_of(at),
            is_today: true,
            qty_raw: None,
            remaining_raw: None,
        });
        view
    };

    let legs = position_rules.split_close(
        &book,
        &CloseRequest {
            instrument: instrument.clone(),
            qty,
            at,
        },
        session,
    )?;

    let close_side = match sub.side {
        Side::Long => Side::Short,
        Side::Short => Side::Long,
    };
    let base = generate_sub_signal_id(
        ids.strategy_type,
        ids.strategy_id,
        &instrument.namespaced_key(),
        at,
        close_side,
        intent,
        ids.rebalance_epoch,
    );

    let ctx = OrderContext {
        at,
        trigger_price: price,
        reference_bar: None,
        trading_day: session.trading_day_of(at),
    };

    let mut out = DispatchOutcome::default();
    let mut emitted = 0usize;
    for mut leg in legs {
        let leg_qty = trading.normalize_qty(instrument, leg.qty, Rounding::Floor)?;
        if leg_qty <= 1e-12 {
            continue;
        }
        leg.qty = leg_qty;
        let leg_raw = leg.qty_raw.clone();
        let signal_id = format!("{base}_leg{emitted}");
        emitted += 1;
        let order = OrderRequest {
            signal_id,
            instrument: instrument.clone(),
            side: leg.side,
            offset: leg.offset,
            price,
            qty: leg_qty,
            close_leg: Some(leg),
            reduce_only: true,
            is_stop_loss,
            qty_raw: leg_raw,
            strategy_id: Some(ids.strategy_id.to_string()),
            intent: Some(intent.as_str().to_string()),
        };
        let ack = parts.execution.as_mut().submit(&order, &ctx)?;
        if ack.accepted {
            out.actions += 1;
            out.signal_ids.push(order.signal_id.clone());
        } else {
            out.rejected += 1;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::bar::Timeframe;
    use crate::core::error::Result;
    use crate::core::exchange::{Exchange, ExchangeParts};
    use crate::core::execution::{ExecutionPort, OrderAck, OrderSnapshot};
    use crate::core::identity::{ExchangeId, InstrumentId, RunMode};
    use crate::core::position::{CloseLeg, PositionMode, PositionRules};
    use crate::core::session::TradingDay;
    use crate::core::trading::OrderDraft;
    use crate::exchanges::binance::BinanceExchange;
    use crate::strategy::config::{self, StrategyConfig};
    use crate::strategy::features::FeaturePlan;
    use crate::strategy::runner::SlotInit;
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use strategy_core::risk::RiskControlConfig;

    /// 可配置受理/拒单的 mock：`requests` 为共享记录，随时读回断言。
    struct RecordingExecution {
        requests: Arc<Mutex<Vec<OrderRequest>>>,
        accept: bool,
    }

    impl ExecutionPort for RecordingExecution {
        fn submit(&mut self, order: &OrderRequest, _ctx: &OrderContext) -> Result<OrderAck> {
            let accepted = self.accept;
            let mut reqs = self.requests.lock().unwrap();
            let seq = reqs.len() + 1;
            reqs.push(order.clone());
            Ok(OrderAck {
                order_id: if accepted {
                    format!("ord_{seq}")
                } else {
                    String::new()
                },
                accepted,
                reason: if accepted {
                    None
                } else {
                    Some("mock rejected".into())
                },
            })
        }
        fn query(&mut self, order_id: &str) -> Result<OrderSnapshot> {
            Ok(OrderSnapshot {
                order_id: order_id.to_string(),
                state: crate::core::execution::OrderState::Filled,
                filled_qty: 0.0,
                avg_price: 0.0,
                fee: 0.0,
            })
        }
        fn remote_positions(&mut self, _instrument: &InstrumentId) -> Result<Vec<PositionLot>> {
            Ok(Vec::new())
        }
    }

    fn test_feature_plan() -> FeaturePlan {
        let cfg = StrategyConfig {
            strategy_type: "xsec_momentum".into(),
            strategy_id: Some("xsec_momentum:default".into()),
            mode: config::StrategyMode::CrossSectional,
            merge_positions: true,
            universe: vec![config::UniverseEntry::Symbol("BTCUSDT".into())],
            timeframes: vec!["15m".into()],
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
            rebalance: None,
            risk: config::RiskCfg {
                max_drawdown: None,
                max_total_notional: 1.0,
            },
            sizing: config::SizingCfg {
                per_position_pct: 0.1,
                max_position_pct: None,
            },
            params: None,
        };
        FeaturePlan::from_config(&cfg).unwrap()
    }

    fn binance_slot() -> (StrategySlot, Arc<Mutex<Vec<OrderRequest>>>) {
        let ex = BinanceExchange::new(0.0004);
        let inst = ex.parse_instrument("BTCUSDT").unwrap();
        let records = Arc::new(Mutex::new(Vec::new()));
        let exec = RecordingExecution {
            requests: records.clone(),
            accept: true,
        };
        let parts = ExchangeParts {
            exchange_id: ExchangeId::new("binance"),
            instrument: inst.clone(),
            mode: RunMode::Backtest,
            session: ex.session_rules(&inst),
            aggregation: ex.aggregation_policy(&inst),
            subscription: ex.subscription_plan(),
            subscriptions: Vec::new(),
            trading: ex.trading_rules(),
            position: ex.position_rules(),
            simulation: ex.simulation_policy(),
            source: Box::new(crate::core::bar::VecEventSource::default()),
            execution: Box::new(exec),
        };
        let slot = StrategySlot::new(SlotInit {
            parts,
            timeframes: vec![Timeframe::M15],
            feature_plan: test_feature_plan(),
            risk: RiskControlConfig::default(),
            strategy_type: "xsec_momentum".to_string(),
            max_closed_len: 300,
        });
        (slot, records)
    }

    fn recorded(reqs: &Mutex<Vec<OrderRequest>>) -> Vec<OrderRequest> {
        reqs.lock().unwrap().clone()
    }

    fn ids() -> StrategyIds<'static> {
        StrategyIds {
            strategy_type: "xsec_momentum",
            strategy_id: "xsec_momentum:default",
            master_id: "master_000001",
            rebalance_epoch: 0,
        }
    }

    fn t() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 5, 1, 30, 0).unwrap()
    }

    #[test]
    fn open_submits_expected_order_and_creates_subposition() {
        let (mut slot, records) = binance_slot();
        let at = t();
        let out = open_position(
            &mut slot,
            &ids(),
            Side::Long,
            30_000.0,
            2.5,
            29_000.0,
            at,
            "entry",
            SubIntent::Open,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected), (1, 0));

        let want_id = generate_sub_signal_id(
            "xsec_momentum",
            "xsec_momentum:default",
            "binance/crypto_perpetual/BTCUSDT",
            at,
            Side::Long,
            SubIntent::Open,
            0,
        );
        assert!(want_id.starts_with("sub_"));

        let resp = recorded(&records);
        let order = resp.first().expect("1 笔开仓单");
        assert_eq!(
            order.signal_id, want_id,
            "signal_id 与 generate_sub_signal_id 一致"
        );
        assert_eq!(order.offset, Offset::Open);
        assert!(!order.reduce_only);
        assert_eq!(order.qty, 2.5, "qty 规整到 step");
        assert_eq!(order.side, Side::Long);
        assert_eq!(order.strategy_id.as_deref(), Some("xsec_momentum:default"));
        assert_eq!(order.intent.as_deref(), Some("open"));

        let sub = &slot.sub_positions[0];
        assert_eq!(sub.id, want_id);
        assert_eq!(
            sub.state,
            SubPositionState::OpenRequested,
            "ack 后 = OpenRequested"
        );
        assert_eq!(sub.qty, 2.5);
        assert_eq!(sub.side, Side::Long);
        assert_eq!(sub.stop_price, Some(29_000.0));
        assert_eq!(
            slot.pos_sync.open_meta.get(&want_id),
            Some(&(29_000.0, 0.0, 0.0))
        );
    }

    #[test]
    fn rejected_open_removes_subposition_and_counts() {
        // execution 返回 accepted=false → 行删除、rejected=1、actions=0。
        let (mut slot, _records) = binance_slot();
        slot.parts.execution = Box::new(RecordingExecution {
            requests: Arc::new(Mutex::new(Vec::new())),
            accept: false,
        });
        let at = t();
        let out = open_position(
            &mut slot,
            &ids(),
            Side::Long,
            30_000.0,
            1.0,
            29_000.0,
            at,
            "entry",
            SubIntent::Open,
        )
        .unwrap();
        assert_eq!(out.rejected, 1);
        assert_eq!(out.actions, 0);
        assert!(slot.sub_positions.is_empty(), "拒单不得残留 PendingOpen 行");
        assert!(
            slot.pos_sync.open_meta.is_empty(),
            "拒单须同时清理 open_meta 侧信道（否则成交落地会读到幽灵条目）"
        );
    }

    /// 拆腿必失败的规则桩：用于验证 Err 时子仓位不推进。
    struct FailingSplitRules;

    impl PositionRules for FailingSplitRules {
        fn mode(&self) -> PositionMode {
            PositionMode::Net
        }
        fn split_close(
            &self,
            _book: &PositionBook,
            _req: &CloseRequest,
            _session: &dyn crate::core::session::SessionRules,
        ) -> Result<Vec<CloseLeg>> {
            Err(SpiError::Execution("测试注入：拆腿失败".into()))
        }
        fn on_open_fill(
            &self,
            _book: &mut PositionBook,
            _draft: &OrderDraft,
            _filled_qty: f64,
            _fill_price: f64,
            _at: DateTime<Utc>,
            _session: &dyn crate::core::session::SessionRules,
        ) -> Result<()> {
            Ok(())
        }
        fn on_close_fill(
            &self,
            _book: &mut PositionBook,
            _leg: &CloseLeg,
            _filled_qty: f64,
            _filled_raw: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }
        fn rollover(&self, _book: &mut PositionBook, _new_day: TradingDay) {}
    }

    #[test]
    fn close_error_keeps_sub_position_active() {
        let (mut slot, _) = binance_slot();
        let at = t();
        open_position(
            &mut slot,
            &ids(),
            Side::Long,
            30_000.0,
            1.0,
            29_000.0,
            at,
            "entry",
            SubIntent::Open,
        )
        .unwrap();
        slot.sub_positions[0].state = SubPositionState::Active;
        slot.last_bar_at = Some(at);
        slot.parts.position = Arc::new(FailingSplitRules);

        let err = close_position(
            &mut slot,
            &ids(),
            30_500.0,
            "exit",
            false,
            SubIntent::ExitRule,
        );
        assert!(err.is_err(), "拆腿失败必须向上传播");
        assert_eq!(
            slot.sub_positions[0].state,
            SubPositionState::Active,
            "拆腿失败不得把子仓位卡在 CloseRequested（下一轮可重试）"
        );
    }

    #[test]
    fn close_emits_leg_ids_and_reduce_only() {
        let (mut slot, records) = binance_slot();
        let at = t();
        open_position(
            &mut slot,
            &ids(),
            Side::Long,
            30_000.0,
            1.0,
            29_000.0,
            at,
            "entry",
            SubIntent::Open,
        )
        .unwrap();
        // 模拟开仓成交：子仓位推进为 Active（1 lot）。
        slot.sub_positions[0].state = SubPositionState::Active;
        slot.last_bar_at = Some(at);

        let out = close_position(
            &mut slot,
            &ids(),
            30_500.0,
            "exit",
            false,
            SubIntent::ExitRule,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected), (1, 0));

        let base = generate_sub_signal_id(
            "xsec_momentum",
            "xsec_momentum:default",
            "binance/crypto_perpetual/BTCUSDT",
            at,
            Side::Short,
            SubIntent::ExitRule,
            0,
        );
        let reqs = recorded(&records);
        let close = &reqs[1];
        assert_eq!(close.signal_id, format!("{base}_leg0"));
        assert!(close.reduce_only, "平仓腿必须 reduce_only");
        assert_eq!(close.side, Side::Short, "平多 = 卖出");
        assert_eq!(
            close.offset,
            Offset::Auto,
            "净仓模式 offset 由 split_close 给出"
        );
        assert_eq!(close.intent.as_deref(), Some("exit_rule"));
        assert_eq!(
            slot.sub_positions[0].state,
            SubPositionState::CloseRequested
        );

        // 确定性：同输入重置到 Active 再平，base 逐字节一致。
        slot.sub_positions[0].state = SubPositionState::Active;
        close_position(
            &mut slot,
            &ids(),
            30_500.0,
            "exit",
            false,
            SubIntent::ExitRule,
        )
        .unwrap();
        let reqs = recorded(&records);
        assert_eq!(
            reqs[2].signal_id, reqs[1].signal_id,
            "同一输入第二次调用 base 相同"
        );
    }

    /// CTP 平仓腿的批次 ID 必须来自交易所成交账本：模拟执行
    /// `on_close_fill` 按 `PositionLot.id` 精确扣减，用子仓位 signal_id
    /// 会报「平仓成交超出腿声明批次」。
    #[test]
    fn ctp_close_splits_against_exchange_book_ids() {
        use crate::core::execution::SimulationExecution;
        use crate::exchanges::ctp::CtpExchange;

        let ex = CtpExchange::new();
        let inst = ex.parse_instrument("CU2601").unwrap();
        let session = ex.session_rules(&inst);
        let at = t();
        let td = session.trading_day_of(at);
        let mut exec = SimulationExecution::new(
            inst.clone(),
            1_000_000.0,
            ex.trading_rules(),
            ex.position_rules(),
            session.clone(),
            ex.simulation_policy(),
        );
        let lot_id = format!("{}-{}", inst.namespaced_key(), at.timestamp_millis());
        exec.book.lots.push(PositionLot {
            id: lot_id.clone(),
            instrument: inst.clone(),
            side: Side::Long,
            entry_price: 100.0,
            qty: 2.0,
            remaining: 2.0,
            opened_at: at,
            trading_day: td,
            is_today: true,
            qty_raw: None,
            remaining_raw: None,
        });
        let parts = ExchangeParts {
            exchange_id: ExchangeId::new("SHFE"),
            instrument: inst.clone(),
            mode: RunMode::Backtest,
            session,
            aggregation: ex.aggregation_policy(&inst),
            subscription: ex.subscription_plan(),
            subscriptions: Vec::new(),
            trading: ex.trading_rules(),
            position: ex.position_rules(),
            simulation: ex.simulation_policy(),
            source: Box::new(crate::core::bar::VecEventSource::default()),
            execution: Box::new(exec),
        };
        let mut slot = StrategySlot::new(SlotInit {
            parts,
            timeframes: vec![Timeframe::M15],
            feature_plan: test_feature_plan(),
            risk: RiskControlConfig::default(),
            strategy_type: "xsec_momentum".to_string(),
            max_closed_len: 300,
        });
        // runner 在成交后按 remote_positions 同步交易所账本。
        let lots = slot.parts.execution.remote_positions(&inst).unwrap();
        slot.pos_sync.sync_book(lots);
        slot.sub_positions.push(SubPosition {
            id: "sub_open".into(),
            strategy_id: "xsec_momentum:default".into(),
            master_id: "master_000001".into(),
            instrument: inst.clone(),
            side: Side::Long,
            qty: 2.0,
            entry_price: 100.0,
            stop_price: None,
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: Some(at),
            closed_at: None,
            realized_pnl: None,
        });
        slot.last_bar_at = Some(at);

        let out = close_position(&mut slot, &ids(), 100.0, "exit", false, SubIntent::ExitRule)
            .expect("CTP 平仓不得因批次 ID 不匹配失败");
        assert_eq!((out.actions, out.rejected), (1, 0));
        assert_eq!(
            slot.parts.execution.remote_positions(&inst).unwrap().len(),
            0,
            "交易所账本批次已被腿 ID 精确扣减"
        );
    }

    #[test]
    fn adjust_up_opens_diff_and_adjust_down_closes_diff() {
        let (mut slot, records) = binance_slot();
        let at = t();

        // current 0, target 2 → 1 笔 Open，qty == 2（intent=open）
        let out = adjust_position(
            &mut slot,
            &ids(),
            2.0,
            30_000.0,
            at,
            "rebalance",
            SubIntent::RebalanceReduce,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected, out.signal_ids.len()), (1, 0, 1));
        let open = &recorded(&records)[0];
        assert_eq!(open.offset, Offset::Open);
        assert_eq!(open.qty, 2.0);
        assert_eq!(open.strategy_id.as_deref(), Some("xsec_momentum:default"));
        assert_eq!(open.intent.as_deref(), Some("open"));

        // 模拟开仓成交后 current=2, target 0.5 → 1 笔 Close，qty == 1.5
        slot.sub_positions[0].state = SubPositionState::Active;
        let out = adjust_position(
            &mut slot,
            &ids(),
            0.5,
            30_500.0,
            at,
            "rebalance",
            SubIntent::RebalanceReduce,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected, out.signal_ids.len()), (1, 0, 1));
        let close = &recorded(&records)[1];
        assert_eq!(close.offset, Offset::Auto);
        assert_eq!(close.qty, 1.5, "部分平仓差额（2.0 - 0.5）");
        assert!(close.reduce_only);
        assert_eq!(close.strategy_id.as_deref(), Some("xsec_momentum:default"));
        assert_eq!(close.intent.as_deref(), Some("rebalance_reduce"));
    }

    #[test]
    fn add_below_qty_step_is_noop_not_rejected() {
        let (mut slot, records) = binance_slot();
        let at = t();
        adjust_position(
            &mut slot,
            &ids(),
            2.0,
            30_000.0,
            at,
            "rebalance",
            SubIntent::RebalanceReduce,
        )
        .unwrap();
        slot.sub_positions[0].state = SubPositionState::Active;
        // 差额 5e-7 < 默认步长 1e-6：floor 后为 0，属已达标，不得计拒单。
        let out = adjust_position(
            &mut slot,
            &ids(),
            2.000_000_5,
            30_500.0,
            at,
            "rebalance",
            SubIntent::RebalanceReduce,
        )
        .unwrap();
        assert_eq!(
            (out.actions, out.rejected),
            (0, 0),
            "低于步长差额视为已达标"
        );
        assert_eq!(recorded(&records).len(), 1, "不得追加第二笔订单");
    }

    #[test]
    fn adjust_up_adds_to_existing_sub_without_second_row() {
        let (mut slot, records) = binance_slot();
        let at = t();

        // 先建 2 手（空仓首建 → open_position），并模拟成交 → Active。
        let out = adjust_position(
            &mut slot,
            &ids(),
            2.0,
            30_000.0,
            at,
            "rebalance",
            SubIntent::RebalanceReduce,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected), (1, 0));
        slot.sub_positions[0].state = SubPositionState::Active;

        // target 3 > current 2：走 add_to_position——只发差额 Open 单，
        // 不新建第二行。
        let out = adjust_position(
            &mut slot,
            &ids(),
            3.0,
            30_100.0,
            at,
            "rebalance",
            SubIntent::RebalanceReduce,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected, out.signal_ids.len()), (1, 0, 1));

        let reqs = recorded(&records);
        let add = &reqs[1];
        assert_eq!(add.offset, Offset::Open);
        assert!(!add.reduce_only);
        assert_eq!(add.qty, 1.0, "差额 3.0 - 2.0");
        assert!(add.signal_id.starts_with("sub_"));
        assert_eq!(
            add.signal_id,
            generate_sub_signal_id(
                "xsec_momentum",
                "xsec_momentum:default",
                "binance/crypto_perpetual/BTCUSDT",
                at,
                Side::Long,
                SubIntent::Open,
                0,
            ),
            "加仓仍走 sub_ 确定性 ID（intent=open）"
        );
        assert_eq!(add.strategy_id.as_deref(), Some("xsec_momentum:default"));
        assert_eq!(add.intent.as_deref(), Some("open"));

        assert_eq!(
            slot.sub_positions.len(),
            1,
            "G1：不得为加仓新建第二行 SubPosition"
        );
        assert_eq!(
            slot.sub_positions[0].state,
            SubPositionState::Active,
            "既有行状态不变"
        );
    }

    #[test]
    fn close_ignores_unfilled_pending_or_open_requested() {
        let (mut slot, records) = binance_slot();
        let at = t();
        open_position(
            &mut slot,
            &ids(),
            Side::Long,
            30_000.0,
            1.0,
            29_000.0,
            at,
            "entry",
            SubIntent::Open,
        )
        .unwrap();
        slot.last_bar_at = Some(at);

        // OpenRequested（未成交）：不发平仓单、不改状态、计一次拒。
        let before = recorded(&records).len();
        let out = close_position(
            &mut slot,
            &ids(),
            30_500.0,
            "exit",
            false,
            SubIntent::ExitRule,
        )
        .unwrap();
        assert_eq!(out.actions, 0);
        assert_eq!(out.rejected, 1, "未成交仓位平仓 = 拒，不得静默");
        assert_eq!(recorded(&records).len(), before, "不得提交平仓单");
        assert_eq!(
            slot.sub_positions[0].state,
            SubPositionState::OpenRequested,
            "状态不得改变"
        );

        // PendingOpen 同理：拒、不改状态、不发单。
        slot.sub_positions[0].state = SubPositionState::PendingOpen;
        let before = recorded(&records).len();
        let out = close_position(
            &mut slot,
            &ids(),
            30_500.0,
            "exit",
            false,
            SubIntent::ExitRule,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected), (0, 1));
        assert_eq!(recorded(&records).len(), before);
        assert_eq!(slot.sub_positions[0].state, SubPositionState::PendingOpen);
    }

    #[test]
    fn close_respects_master_id_filter() {
        let (mut slot, records) = binance_slot();
        let at = t();
        slot.last_bar_at = Some(at);
        slot.sub_positions.push(SubPosition {
            id: "sub_m1".into(),
            strategy_id: "xsec_momentum:default".into(),
            master_id: "master_000001".into(),
            instrument: slot.parts.instrument.clone(),
            side: Side::Long,
            qty: 1.0,
            entry_price: 30_000.0,
            stop_price: None,
            target_weight: 0.0,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: Some(at),
            closed_at: None,
            realized_pnl: None,
        });

        // 平另一个总仓：不得动 M1 的子仓，也不得发单。
        let other_master = StrategyIds {
            master_id: "master_000002",
            ..ids()
        };
        let out = close_position(
            &mut slot,
            &other_master,
            30_500.0,
            "exit",
            false,
            SubIntent::ExitRule,
        )
        .unwrap();
        assert_eq!(out, DispatchOutcome::default());
        assert!(recorded(&records).is_empty(), "其他总仓不得被平");
        assert_eq!(slot.sub_positions[0].state, SubPositionState::Active);

        // 平本总仓：正常拆腿，状态推进到 CloseRequested。
        let out = close_position(
            &mut slot,
            &ids(),
            30_500.0,
            "exit",
            false,
            SubIntent::ExitRule,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected), (1, 0));
        assert_eq!(
            slot.sub_positions[0].state,
            SubPositionState::CloseRequested
        );
    }

    /// 建一个 Short qty 的 Active 子仓位（开空后手动推进成交）。
    fn short_active(slot: &mut StrategySlot, qty: f64, at: DateTime<Utc>) {
        open_position(
            slot,
            &ids(),
            Side::Short,
            30_000.0,
            qty,
            30_100.0,
            at,
            "short",
            SubIntent::Open,
        )
        .unwrap();
        slot.sub_positions[0].state = SubPositionState::Active;
    }

    #[test]
    fn adjust_short_higher_target_reduces_by_buying_back() {
        // Short 2 → target −0.5：买回 1.5，不得再开空。
        let (mut slot, records) = binance_slot();
        let at = t();
        short_active(&mut slot, 2.0, at);
        slot.last_bar_at = Some(at);

        let out = adjust_position(
            &mut slot,
            &ids(),
            -0.5,
            30_050.0,
            at,
            "rebalance",
            SubIntent::RebalanceReduce,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected, out.signal_ids.len()), (1, 0, 1));
        let reqs = recorded(&records);
        assert_eq!(reqs.len(), 2, "仅开空 + 一腿平仓，不追加空单");
        let close = &reqs[1];
        assert!(close.reduce_only, "平仓腿必须 reduce_only");
        assert_eq!(close.side, Side::Long, "平空 = 买入");
        assert_eq!(close.qty, 1.5, "2 − 0.5");
    }

    #[test]
    fn adjust_short_deeper_target_adds_short() {
        // Short 2 → target −3：加空 1（Open Short）。
        let (mut slot, records) = binance_slot();
        let at = t();
        short_active(&mut slot, 2.0, at);

        let out = adjust_position(
            &mut slot,
            &ids(),
            -3.0,
            30_100.0,
            at,
            "rebalance",
            SubIntent::RebalanceReduce,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected), (1, 0));
        let reqs = recorded(&records);
        assert_eq!(reqs.len(), 2);
        let add = &reqs[1];
        assert_eq!(add.offset, Offset::Open);
        assert!(!add.reduce_only);
        assert_eq!(add.side, Side::Short);
        assert_eq!(add.qty, 1.0, "3 − 2");
        assert_eq!(slot.sub_positions.len(), 1, "加空不得新建第二行（G1）");
    }

    #[test]
    fn adjust_from_flat_opens_short() {
        let (mut slot, records) = binance_slot();
        let at = t();
        let out = adjust_position(
            &mut slot,
            &ids(),
            -2.0,
            30_000.0,
            at,
            "rebalance",
            SubIntent::RebalanceReduce,
        )
        .unwrap();
        assert_eq!((out.actions, out.rejected), (1, 0));
        let open = &recorded(&records)[0];
        assert_eq!(open.offset, Offset::Open);
        assert_eq!(open.side, Side::Short);
        assert_eq!(open.qty, 2.0);
        assert_eq!(slot.sub_positions[0].side, Side::Short);
        assert_eq!(slot.sub_positions[0].qty, 2.0);
    }

    #[test]
    fn adjust_rejects_opposite_sign_flip_without_orders() {
        // Long 2 → target −1：同轮反向翻仓不支持，拒且不发单。
        let (mut slot, records) = binance_slot();
        let at = t();
        open_position(
            &mut slot,
            &ids(),
            Side::Long,
            30_000.0,
            2.0,
            29_000.0,
            at,
            "long",
            SubIntent::Open,
        )
        .unwrap();
        slot.sub_positions[0].state = SubPositionState::Active;
        let before = recorded(&records).len();

        let out = adjust_position(
            &mut slot,
            &ids(),
            -1.0,
            30_000.0,
            at,
            "rebalance",
            SubIntent::RebalanceReduce,
        )
        .unwrap();
        assert_eq!(out.rejected, 1, "同轮翻仓必须拒");
        assert_eq!(out.actions, 0);
        assert_eq!(recorded(&records).len(), before, "不提交任何订单");
        assert_eq!(slot.sub_positions[0].state, SubPositionState::Active);
        assert_eq!(slot.sub_positions[0].qty, 2.0);
    }
}
