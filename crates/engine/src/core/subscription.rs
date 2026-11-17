//! 订阅规则与主力 / 次主力 / 连续合约。
//!
//! 现状：主链路完全没有「主力」概念，`symbol` 是裸字符串，
//! 旧 kline-service 契约 §5.1（已归档 `docs/archive/kline-service.md`）明确把拼接推给上游，
//! `roll_build.rs` 是唯一实现但独立成 CLI bin，且换月价格不复权、
//! 交易日口径又是另一套 20:00 近似。
//!
//! 本模块让「策略交易逻辑标的 → 实际订阅哪些挂牌合约」成为交易所能力：
//! - crypto：一对一透传；
//! - 期货：按持仓量选主力/次主力、剔除交割月、换月时切换订阅，
//!   并可选用连续合约拼接器复权。

use std::sync::Arc;

use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};

use crate::core::bar::{Bar, Timeframe};
use crate::core::error::{Result, SpiError};
use crate::core::identity::{ContractRole, InstrumentId};
use crate::core::session::{SessionRules, TradingDay};

/// 一次订阅请求。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Subscription {
    /// 发给上游的订阅 topic（通常是挂牌合约码）
    pub topic: String,
    /// 规范化后的标的
    pub instrument: InstrumentId,
    pub role: ContractRole,
    pub timeframe: Timeframe,
}

/// 主力选择依据。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum RollPolicy {
    /// 持仓量最大
    #[default]
    OpenInterest,
    /// 成交量最大
    Volume,
    /// 固定换月日（如交割月前一月的第 N 个交易日）
    FixedDate,
    /// 由上游直接提供主连，模板不参与选择
    External,
}

/// 换月复权方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdjustMode {
    None,
    Ratio,
    Difference,
}

/// 某合约在某一交易日收盘时的快照。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContractSnapshot {
    pub instrument: InstrumentId,
    pub open_interest: f64,
    pub volume: f64,
    pub last_price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollEvent {
    pub trading_day: TradingDay,
    /// 换月生效时刻（D 日收盘观测，D+1 交易日开盘生效，避免未来函数）
    pub effective_at: DateTime<Utc>,
    pub from: InstrumentId,
    pub to: InstrumentId,
    pub policy: RollPolicy,
}

/// 订阅计划。引擎只持有逻辑标的（如 `CU/主力`），不自己拼合约字符串。
pub trait SubscriptionPlan: Send + Sync {
    /// 根据当日合约快照，返回需要建立的全部订阅。
    fn resolve(
        &self,
        logical: &InstrumentId,
        as_of: DateTime<Utc>,
        universe: &[ContractSnapshot],
    ) -> Result<Vec<Subscription>>;

    /// 换月时计算订阅切换（退订旧主力 + 订阅新主力/次主力）。
    fn on_roll(&self, ev: &RollEvent, current: &[Subscription]) -> Result<Vec<Subscription>>;

    /// 可选：连续合约拼接 / 复权。返回 None 表示上游已提供主连。
    fn continuous_series(&self) -> Option<Arc<dyn ContinuousSeriesBuilder>> {
        None
    }

    fn roll_policy(&self) -> RollPolicy {
        RollPolicy::External
    }
}

/// 连续合约构造器：选主 + 拼接 + 复权。
pub trait ContinuousSeriesBuilder: Send + Sync {
    /// 返回 (新主力, 换月事件)；无换月返回 None。
    ///
    /// v2.1 C11：`session` 用于把换月**生效时点**算成「下一交易日首个交易
    /// 窗口开盘」——因果口径（D 日收盘观测、D+1 生效）的唯一定义点。
    fn detect_roll(
        &self,
        day: TradingDay,
        universe: &[ContractSnapshot],
        current: Option<&InstrumentId>,
        session: &dyn SessionRules,
    ) -> Option<(InstrumentId, RollEvent)>;

    fn stitch(&self, bars: &[Bar], roll: &RollEvent, mode: AdjustMode) -> Vec<Bar>;
}

// ─── 参考实现 A：一对一透传（crypto / 指定合约） ──────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct DirectSubscriptionPlan;

impl SubscriptionPlan for DirectSubscriptionPlan {
    fn resolve(
        &self,
        logical: &InstrumentId,
        _as_of: DateTime<Utc>,
        _universe: &[ContractSnapshot],
    ) -> Result<Vec<Subscription>> {
        Ok(vec![Subscription {
            topic: logical.symbol.clone(),
            instrument: logical.clone(),
            role: ContractRole::Main,
            timeframe: Timeframe::M1,
        }])
    }

    fn on_roll(&self, _ev: &RollEvent, current: &[Subscription]) -> Result<Vec<Subscription>> {
        Ok(current.to_vec())
    }
}

// ─── 参考实现 B：按持仓量选主力 / 次主力（国内期货） ──────────────────────

#[derive(Debug, Clone)]
pub struct MainContractPlan {
    pub policy: RollPolicy,
    /// 是否同时订阅次主力（价差 / 跨期策略需要）
    pub include_second_main: bool,
    /// 进入交割月即剔除
    pub exclude_delivery_month: bool,
    pub timeframe: Timeframe,
}

impl Default for MainContractPlan {
    fn default() -> Self {
        Self {
            policy: RollPolicy::OpenInterest,
            include_second_main: false,
            exclude_delivery_month: true,
            timeframe: Timeframe::M1,
        }
    }
}

impl MainContractPlan {
    fn rank<'a>(
        &'a self,
        logical: &InstrumentId,
        as_of: DateTime<Utc>,
        universe: &'a [ContractSnapshot],
    ) -> Vec<&'a ContractSnapshot> {
        let mut rows: Vec<&ContractSnapshot> = universe
            .iter()
            .filter(|s| s.instrument.product == logical.product)
            .filter(|s| s.instrument.market == logical.market)
            .filter(|s| {
                if !self.exclude_delivery_month {
                    return true;
                }
                match s.instrument.contract_month {
                    // 只要合约月 > 当前自然月即保留
                    Some(m) => (m.year, m.month) > (as_of.year() as u16, as_of.month() as u8),
                    None => true,
                }
            })
            .filter(|s| match self.policy {
                RollPolicy::OpenInterest => s.open_interest.is_finite() && s.open_interest > 0.0,
                RollPolicy::Volume => s.volume.is_finite() && s.volume > 0.0,
                _ => true,
            })
            .collect();
        rows.sort_by(|a, b| {
            let key = |s: &&ContractSnapshot| match self.policy {
                RollPolicy::Volume => s.volume,
                _ => s.open_interest,
            };
            key(b)
                .total_cmp(&key(a))
                .then_with(|| a.instrument.symbol.cmp(&b.instrument.symbol))
        });
        rows
    }
}

impl SubscriptionPlan for MainContractPlan {
    fn resolve(
        &self,
        logical: &InstrumentId,
        as_of: DateTime<Utc>,
        universe: &[ContractSnapshot],
    ) -> Result<Vec<Subscription>> {
        let ranked = self.rank(logical, as_of, universe);
        if ranked.is_empty() {
            return Err(SpiError::Subscription(format!(
                "{} 在 {} 无可用合约快照",
                logical.product, as_of
            )));
        }
        let want = if self.include_second_main { 2 } else { 1 };
        Ok(ranked
            .into_iter()
            .take(want)
            .enumerate()
            .map(|(i, s)| Subscription {
                topic: s.instrument.symbol.clone(),
                instrument: s.instrument.clone(),
                role: if i == 0 {
                    ContractRole::Main
                } else {
                    ContractRole::SecondMain
                },
                timeframe: self.timeframe,
            })
            .collect())
    }

    fn on_roll(&self, ev: &RollEvent, current: &[Subscription]) -> Result<Vec<Subscription>> {
        let mut out = Vec::new();
        for sub in current {
            let (role, inst) = match sub.role {
                ContractRole::Main => (ContractRole::Main, ev.to.clone()),
                // 旧主力降为次主力（简单映射；生产可按 OI 重排）
                ContractRole::SecondMain => (ContractRole::SecondMain, ev.from.clone()),
                other => (other, sub.instrument.clone()),
            };
            out.push(Subscription {
                topic: inst.symbol.clone(),
                instrument: inst,
                role,
                timeframe: sub.timeframe,
            });
        }
        if out.is_empty() {
            out.push(Subscription {
                topic: ev.to.symbol.clone(),
                instrument: ev.to.clone(),
                role: ContractRole::Main,
                timeframe: self.timeframe,
            });
        }
        Ok(out)
    }

    fn roll_policy(&self) -> RollPolicy {
        self.policy
    }
}

/// 不做复权的连续合约拼接（对应现有 `roll_build.rs --backadjust none`）。
#[derive(Debug, Clone, Copy, Default)]
pub struct NoAdjustContinuousSeries;

impl ContinuousSeriesBuilder for NoAdjustContinuousSeries {
    fn detect_roll(
        &self,
        day: TradingDay,
        universe: &[ContractSnapshot],
        current: Option<&InstrumentId>,
        session: &dyn SessionRules,
    ) -> Option<(InstrumentId, RollEvent)> {
        let best = universe
            .iter()
            .filter(|s| s.open_interest > 0.0)
            .max_by(|a, b| {
                a.open_interest
                    .total_cmp(&b.open_interest)
                    .then_with(|| b.instrument.symbol.cmp(&a.instrument.symbol))
            })?;
        if current == Some(&best.instrument) {
            return None;
        }
        let from = current.cloned().unwrap_or_else(|| best.instrument.clone());
        // v2.1 C11（因果口径修复）：D 日收盘观测到的新主力，**下一交易日
        // 首个交易窗口开盘才生效**——旧实现取观察日当日 00:00（早于观测
        // 时点，未来函数），此处对齐 roll_build.rs 的 D+1 语义。
        let mut d = day.0 + chrono::Duration::days(1);
        for _ in 0..15 {
            if session.is_trading_day(d) {
                break;
            }
            d += chrono::Duration::days(1);
        }
        let new_day = TradingDay(d);
        let effective_at = session
            .sessions_on(new_day)
            .first()
            .map(|w| w.start)
            .unwrap_or_else(Utc::now);
        Some((
            best.instrument.clone(),
            RollEvent {
                trading_day: new_day,
                effective_at,
                from,
                to: best.instrument.clone(),
                policy: RollPolicy::OpenInterest,
            },
        ))
    }

    fn stitch(&self, bars: &[Bar], _roll: &RollEvent, _mode: AdjustMode) -> Vec<Bar> {
        // 按时间排序去重；价格不做复权，换月处保留断档（与现状一致）。
        let mut out = bars.to_vec();
        out.sort_by_key(|b| (b.open_time, b.instrument.symbol.clone()));
        out.dedup_by(|a, b| a.open_time == b.open_time && a.instrument == b.instrument);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::identity::{ContractMonth, ExchangeId};
    use crate::core::session::cn_futures_default;
    use chrono::{DateTime, TimeZone, Utc};

    fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
    }

    /// C11：换月生效时点 = 下一交易日首个窗口开盘（D 日收盘观测、D+1 生效）。
    #[test]
    fn roll_effective_at_next_trading_day_open() {
        use crate::core::subscription::ContinuousSeriesBuilder;
        use crate::core::subscription::{ContractSnapshot, NoAdjustContinuousSeries};
        let session = cn_futures_default();
        let mk = |ym: (u16, u8)| {
            InstrumentId::futures_contract(
                &ExchangeId::new("SHFE"),
                "CU",
                ContractMonth {
                    year: ym.0,
                    month: ym.1,
                },
            )
        };
        let old = mk((2025, 12));
        let new = mk((2026, 1));
        let universe = vec![
            ContractSnapshot {
                instrument: new.clone(),
                open_interest: 1000.0,
                volume: 1.0,
                last_price: 50000.0,
            },
            ContractSnapshot {
                instrument: old.clone(),
                open_interest: 100.0,
                volume: 1.0,
                last_price: 49000.0,
            },
        ];
        let observed = TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap());
        let (_, roll) = NoAdjustContinuousSeries
            .detect_roll(observed, &universe, Some(&old), &session)
            .expect("OI 换主应触发 RollEvent");
        // 旧实现取观察日当日 00:00（早于 15:00 观测收盘 = 未来函数）；
        // 修复后必须晚于观测时点，且落在下一交易日（周二）首个窗口开盘。
        assert!(
            roll.effective_at > utc(2026, 1, 5, 7, 0),
            "生效时点必须晚于 D 日收盘观测（北京 15:00）"
        );
        assert_eq!(
            roll.effective_at,
            utc(2026, 1, 5, 13, 0),
            "D+1 夜盘开盘生效"
        );
        assert_eq!(
            roll.trading_day,
            TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 6).unwrap())
        );
    }
}
