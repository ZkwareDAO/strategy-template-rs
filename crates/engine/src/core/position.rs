//! 持仓模型与平仓规则（平今 / 平昨）。
//!
//! 现状：全仓只有 `remaining_size` 一个净仓位，没有今昨仓区分；
//! 「平今」只在 `BacktestBroker::Position.open_trading_day` 里用来选费率，
//! 实盘 `wire.rs` 的平仓帧完全没有 offset 字段，靠外部服务自行决定；
//! `positions::PositionState` 与 `broker::backtest::Position` 是两套独立模型。
//!
//! 本模块把「一次平仓意图 → 若干带 offset 的平仓腿」变成交易所能力。
//! 引擎只说「平 X 手」，实盘网关用腿生成订单帧，回测撮合用腿计费，
//! 规则只实现一次。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use strategy_core::signal::Side;

use crate::core::error::{Result, SpiError};
use crate::core::identity::InstrumentId;
use crate::core::session::{SessionRules, TradingDay};
use crate::core::trading::{Offset, OrderDraft};

/// 净仓（crypto）还是双向持仓 + 今昨分仓（CTP）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionMode {
    Net,
    Dual,
}

/// 平仓时优先消耗哪一类仓。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClosePriority {
    /// CTP 默认：先平今（可能触发高平今费），也可配置为先平昨省费
    TodayFirst,
    YesterdayFirst,
    /// 按比例分摊
    ProRata,
}

/// 一笔开仓批次。期货必须按「交易日 + 今/昨」分桶，
/// 否则平今判定只能靠 `open_trading_day == ctp_trading_day(t)` 单点比较。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionLot {
    pub id: String,
    pub instrument: InstrumentId,
    pub side: Side,
    pub entry_price: f64,
    pub qty: f64,
    pub remaining: f64,
    pub opened_at: DateTime<Utc>,
    pub trading_day: TradingDay,
    pub is_today: bool,
    /// 开仓成交数量字面量（v2.2 C7；live = 回报原样，sim = 申报派生）
    #[serde(default)]
    pub qty_raw: Option<String>,
    /// 剩余可平量字面量——live 平仓帧 quantity 的真源
    /// （对齐 `positions::state::remaining_size_raw`；成交时 Decimal 精确递减，
    /// 无法精确分解时置 None，**绝不伪造**）。
    #[serde(default)]
    pub remaining_raw: Option<String>,
}

/// 统一的持仓账本。实盘对账、回测撮合、报表共用同一份。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionBook {
    pub instrument: InstrumentId,
    pub mode: PositionMode,
    pub lots: Vec<PositionLot>,
}

impl PositionBook {
    pub fn new(instrument: InstrumentId, mode: PositionMode) -> Self {
        Self {
            instrument,
            mode,
            lots: Vec::new(),
        }
    }

    pub fn net_qty(&self) -> f64 {
        self.lots
            .iter()
            .map(|l| match l.side {
                Side::Long => l.remaining,
                Side::Short => -l.remaining,
            })
            .sum()
    }

    pub fn total_remaining(&self) -> f64 {
        self.lots.iter().map(|l| l.remaining).sum()
    }

    pub fn is_flat(&self) -> bool {
        self.lots.iter().all(|l| l.remaining <= 1e-12)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CloseRequest {
    pub instrument: InstrumentId,
    /// 期望平掉的数量（引擎给出，可能大于可用持仓，由实现夹紧）
    pub qty: f64,
    pub at: DateTime<Utc>,
}

/// 一条平仓腿 —— 直接对应一笔交易所委托。
///
/// v2.1 C4：合并同 (offset, side) 的多批次时，`entry_price` 为**加权均价**、
/// `lot_ids` 记录全部贡献批次——修复「只取首批次价算全量盈亏」的聚合错算。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CloseLeg {
    pub offset: Offset,
    pub qty: f64,
    pub lot_ids: Vec<String>,
    pub side: Side,
    pub entry_price: f64,
    /// 本腿数量字面量（v2.2 C7）：全部贡献批次持有 `remaining_raw` 时
    /// Decimal 精确求和；含无 raw 批次则为 None（gateway 回退 f64 申报）。
    #[serde(default)]
    pub qty_raw: Option<String>,
}

/// 把一笔批次消耗并入腿序列：同 (offset, side) 相邻时按加权均价合并。
fn add_leg(legs: &mut Vec<CloseLeg>, lot: &PositionLot, offset: Offset, qty: f64) {
    let close_side = match lot.side {
        Side::Long => Side::Short,
        Side::Short => Side::Long,
    };
    // v2.2 C7：整批次消耗才继承其 remaining_raw；部分消耗无精确字面量来源 → None。
    let lot_raw = if (lot.remaining - qty).abs() <= 1e-12 {
        lot.remaining_raw.clone()
    } else {
        None
    };
    if let Some(last) = legs.last_mut() {
        if last.offset == offset && last.side == close_side {
            let total = last.qty + qty;
            last.entry_price = if total > 1e-12 {
                (last.entry_price * last.qty + lot.entry_price * qty) / total
            } else {
                lot.entry_price
            };
            last.qty = total;
            last.lot_ids.push(lot.id.clone());
            // raw 合并用 Decimal 精确加法；任一侧缺字面量则整腿 None（不伪造）
            last.qty_raw = match (last.qty_raw.as_deref(), lot_raw.as_deref()) {
                (Some(a), Some(b)) => add_raw(a, b),
                _ => None,
            };
            return;
        }
    }
    legs.push(CloseLeg {
        offset,
        qty,
        lot_ids: vec![lot.id.clone()],
        side: close_side,
        entry_price: lot.entry_price,
        qty_raw: lot_raw,
    });
}

/// 数量字面量精确加法（与 `strategy_positions::subtract_qty_raw` 同引擎同口径）。
fn add_raw(a: &str, b: &str) -> Option<String> {
    use std::str::FromStr;
    let x = rust_decimal::Decimal::from_str(a).ok()?;
    let y = rust_decimal::Decimal::from_str(b).ok()?;
    Some(format!("{}", x + y))
}

/// 持仓规则端口。
pub trait PositionRules: Send + Sync {
    fn mode(&self) -> PositionMode;

    /// 把平仓请求拆成交易所认可的腿。crypto 恒返回一条 `Offset::Auto`；
    /// CTP 按今/昨仓拆成 1~2 条 `CloseToday` / `CloseYesterday`。
    fn split_close(
        &self,
        book: &PositionBook,
        req: &CloseRequest,
        session: &dyn SessionRules,
    ) -> Result<Vec<CloseLeg>>;

    /// 开仓成交入账。
    fn on_open_fill(
        &self,
        book: &mut PositionBook,
        draft: &OrderDraft,
        filled_qty: f64,
        fill_price: f64,
        at: DateTime<Utc>,
        session: &dyn SessionRules,
    ) -> Result<()>;

    /// 平仓成交扣减对应批次。`filled_raw` = 成交数量字面量（v2.2 C7）：
    /// live 传回报 `filled_quantity` 原样；单批次部分消耗时用它对
    /// `remaining_raw` 做 Decimal 精确递减，其余情形置 None（未知不伪造）。
    fn on_close_fill(
        &self,
        book: &mut PositionBook,
        leg: &CloseLeg,
        filled_qty: f64,
        filled_raw: Option<&str>,
    ) -> Result<()>;

    /// 交易日切换：今仓 → 昨仓。
    fn rollover(&self, book: &mut PositionBook, new_day: TradingDay);
}

// ─── 参考实现 A：净仓（crypto） ───────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct NetPositionRules;

impl PositionRules for NetPositionRules {
    fn mode(&self) -> PositionMode {
        PositionMode::Net
    }

    fn split_close(
        &self,
        book: &PositionBook,
        req: &CloseRequest,
        _session: &dyn SessionRules,
    ) -> Result<Vec<CloseLeg>> {
        let net = book.net_qty();
        if net.abs() <= 1e-12 {
            return Err(SpiError::CloseSplit("净仓为空，无可平数量".into()));
        }
        let side = if net > 0.0 { Side::Short } else { Side::Long };
        let qty = req.qty.min(net.abs());
        let lot = book.lots.iter().find(|l| l.remaining > 0.0);
        // v2.2 C7：单批次整腿消耗时继承其 remaining_raw 作平仓帧字面量。
        let leg_raw = lot
            .filter(|l| (l.remaining - qty).abs() <= 1e-12)
            .and_then(|l| l.remaining_raw.clone());
        Ok(vec![CloseLeg {
            // crypto 用 reduce_only / Auto，由柜台自行处理
            offset: Offset::Auto,
            qty,
            lot_ids: lot.map(|l| vec![l.id.clone()]).unwrap_or_default(),
            side,
            entry_price: lot.map(|l| l.entry_price).unwrap_or(0.0),
            qty_raw: leg_raw,
        }])
    }

    fn on_open_fill(
        &self,
        book: &mut PositionBook,
        draft: &OrderDraft,
        filled_qty: f64,
        fill_price: f64,
        at: DateTime<Utc>,
        session: &dyn SessionRules,
    ) -> Result<()> {
        // 净仓：同标的反向开仓先对冲，剩余部分才开新批次。
        let opposite = match draft.side {
            Side::Long => Side::Short,
            Side::Short => Side::Long,
        };
        let mut left = filled_qty;
        for lot in book.lots.iter_mut().filter(|l| l.side == opposite) {
            let before = lot.remaining;
            let used = left.min(lot.remaining);
            lot.remaining -= used;
            left -= used;
            // v2.2 C7：对冲部分消耗 → 该批次字面量不再可分解，置 None。
            if used < before - 1e-12 {
                lot.remaining_raw = None;
            }
            if left <= 1e-12 {
                break;
            }
        }
        if left > 1e-12 {
            book.lots.push(PositionLot {
                id: format!(
                    "{}-{}",
                    draft.instrument.namespaced_key(),
                    at.timestamp_millis()
                ),
                instrument: draft.instrument.clone(),
                side: draft.side,
                entry_price: fill_price,
                qty: left,
                remaining: left,
                opened_at: at,
                trading_day: session.trading_day_of(at),
                is_today: true,
                qty_raw: draft.qty_raw.clone(),
                remaining_raw: draft.qty_raw.clone(),
            });
        }
        book.lots.retain(|l| l.remaining > 1e-12);
        Ok(())
    }

    fn on_close_fill(
        &self,
        book: &mut PositionBook,
        leg: &CloseLeg,
        filled_qty: f64,
        filled_raw: Option<&str>,
    ) -> Result<()> {
        let mut left = filled_qty;
        for lot in book.lots.iter_mut() {
            let before = lot.remaining;
            let used = left.min(lot.remaining);
            lot.remaining -= used;
            left -= used;
            if used > 1e-12 {
                // v2.2 C7：单批次部分平 → Decimal 精确递减；其余 → 未知。
                let fully = used >= before - 1e-12;
                lot.remaining_raw = if leg.lot_ids.len() <= 1 && !fully {
                    strategy_positions::subtract_qty_raw(
                        lot.remaining_raw.as_deref(),
                        filled_raw.unwrap_or_default(),
                    )
                } else {
                    None
                };
            }
            if left <= 1e-12 {
                break;
            }
        }
        book.lots.retain(|l| l.remaining > 1e-12);
        if left > 1e-12 {
            return Err(SpiError::CloseSplit(format!(
                "平仓成交 {} 超过持仓，剩余 {} 无法扣减（leg={:?}）",
                filled_qty, left, leg.offset
            )));
        }
        Ok(())
    }

    fn rollover(&self, _book: &mut PositionBook, _new_day: TradingDay) {}
}

// ─── 参考实现 B：CTP 今昨分仓 ─────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub struct CtpPositionRules {
    pub priority: ClosePriority,
}

impl Default for CtpPositionRules {
    fn default() -> Self {
        Self {
            priority: ClosePriority::TodayFirst,
        }
    }
}

impl PositionRules for CtpPositionRules {
    fn mode(&self) -> PositionMode {
        PositionMode::Dual
    }

    fn split_close(
        &self,
        book: &PositionBook,
        req: &CloseRequest,
        session: &dyn SessionRules,
    ) -> Result<Vec<CloseLeg>> {
        if book.is_flat() {
            return Err(SpiError::CloseSplit("无持仓可平".into()));
        }
        let today = session.trading_day_of(req.at);
        let offset_of = |l: &PositionLot| -> Offset {
            if l.trading_day == today {
                Offset::CloseToday
            } else {
                Offset::CloseYesterday
            }
        };
        let mut live: Vec<&PositionLot> = book.lots.iter().filter(|l| l.remaining > 0.0).collect();
        live.sort_by_key(|l| l.opened_at);
        let avail: f64 = live.iter().map(|l| l.remaining).sum();
        let want = req.qty.min(avail);

        // v2.1 C4：ProRata 按各批次剩余量比例分摊后补齐取整残差；
        // Today/YesterdayFirst 按优先级贪心消耗。分摊结果统一交给 add_leg
        // 以加权均价合并，杜绝首批次价算全量盈亏。
        let mut alloc: Vec<f64> = vec![0.0; live.len()];
        match self.priority {
            ClosePriority::ProRata => {
                if avail > 1e-12 {
                    for (i, l) in live.iter().enumerate() {
                        alloc[i] = (want * l.remaining / avail).min(l.remaining);
                    }
                }
                let mut left = want - alloc.iter().sum::<f64>();
                for (i, l) in live.iter().enumerate() {
                    if left <= 1e-12 {
                        break;
                    }
                    let add = left.min(l.remaining - alloc[i]);
                    alloc[i] += add;
                    left -= add;
                }
                // 合并需要同 (offset, side) 相邻：先按 offset、再按 side 稳定重排
                let mut order: Vec<usize> = (0..live.len()).filter(|i| alloc[*i] > 1e-12).collect();
                order.sort_by_key(|i| {
                    let l = live[*i];
                    (
                        if offset_of(l) == Offset::CloseToday {
                            0
                        } else {
                            1
                        },
                        match l.side {
                            Side::Long => 0,
                            Side::Short => 1,
                        },
                        l.opened_at,
                    )
                });
                let mut legs = Vec::new();
                for i in order {
                    let lot = live[i];
                    add_leg(&mut legs, lot, offset_of(lot), alloc[i]);
                }
                if legs.is_empty() {
                    return Err(SpiError::CloseSplit("拆分结果为空".into()));
                }
                return Ok(legs);
            }
            _ => {
                live.sort_by_key(|l| {
                    let rank = match (self.priority, offset_of(l)) {
                        (ClosePriority::TodayFirst, Offset::CloseToday) => 0,
                        (ClosePriority::TodayFirst, _) => 1,
                        (ClosePriority::YesterdayFirst, Offset::CloseYesterday) => 0,
                        (ClosePriority::YesterdayFirst, _) => 1,
                        _ => 0,
                    };
                    (rank, l.opened_at)
                });
                let mut left = want;
                for (i, l) in live.iter().enumerate() {
                    if left <= 1e-12 {
                        break;
                    }
                    let q = left.min(l.remaining);
                    alloc[i] = q;
                    left -= q;
                }
            }
        }

        let mut legs = Vec::new();
        for (i, lot) in live.iter().enumerate() {
            if alloc[i] <= 1e-12 {
                continue;
            }
            add_leg(&mut legs, lot, offset_of(lot), alloc[i]);
        }
        if legs.is_empty() {
            return Err(SpiError::CloseSplit("拆分结果为空".into()));
        }
        Ok(legs)
    }

    fn on_open_fill(
        &self,
        book: &mut PositionBook,
        draft: &OrderDraft,
        filled_qty: f64,
        fill_price: f64,
        at: DateTime<Utc>,
        session: &dyn SessionRules,
    ) -> Result<()> {
        book.lots.push(PositionLot {
            id: format!(
                "{}-{}",
                draft.instrument.namespaced_key(),
                at.timestamp_millis()
            ),
            instrument: draft.instrument.clone(),
            side: draft.side,
            entry_price: fill_price,
            qty: filled_qty,
            remaining: filled_qty,
            opened_at: at,
            trading_day: session.trading_day_of(at),
            is_today: true,
            qty_raw: draft.qty_raw.clone(),
            remaining_raw: draft.qty_raw.clone(),
        });
        Ok(())
    }

    fn on_close_fill(
        &self,
        book: &mut PositionBook,
        leg: &CloseLeg,
        filled_qty: f64,
        filled_raw: Option<&str>,
    ) -> Result<()> {
        let mut left = filled_qty;
        // v2.1 C4：只按腿声明的批次集合精确扣减，不再盲扣其他批次——
        // 跨 offset 扣减会让今昨仓与 CTP 柜台实际批次漂移。
        for id in &leg.lot_ids {
            if left <= 1e-12 {
                break;
            }
            if let Some(lot) = book.lots.iter_mut().find(|l| &l.id == id) {
                let before = lot.remaining;
                let used = left.min(lot.remaining);
                lot.remaining -= used;
                left -= used;
                if used > 1e-12 {
                    // v2.2 C7：单批次腿且部分消耗 → Decimal 精确递减；
                    // 多批次腿（成交字面量无法分解到单批）→ 未知，置 None。
                    let fully = used >= before - 1e-12;
                    lot.remaining_raw = if leg.lot_ids.len() == 1 && !fully {
                        strategy_positions::subtract_qty_raw(
                            lot.remaining_raw.as_deref(),
                            filled_raw.unwrap_or_default(),
                        )
                    } else {
                        None
                    };
                }
            }
        }
        book.lots.retain(|l| l.remaining > 1e-12);
        if left > 1e-12 {
            return Err(SpiError::CloseSplit(format!(
                "平仓成交 {filled_qty} 超出腿声明批次（剩余 {left} 无法扣减，leg={:?}）",
                leg.offset
            )));
        }
        Ok(())
    }

    fn rollover(&self, book: &mut PositionBook, new_day: TradingDay) {
        for lot in &mut book.lots {
            lot.is_today = lot.trading_day == new_day;
        }
    }
}
