//! 横截面策略端口（PR-2，Task 7）。
//!
//! 定义 [`CrossSectionalStrategyPort`] 的输入/输出类型：
//! - [`InstrumentSnapshot`]：单标的一根已闭合 bar 的全部决策输入；
//! - [`PortfolioContext`]：组合级账户快照（multipliers 为 instrument
//!   `namespaced_key()` → 合约乘数）；
//! - [`InstrumentAction`]：组合策略动作全集（Open/Close 带 price/atr，
//!   Reverse 保留「先平后开、平仓失败则放弃开仓」语义，另有新策略专用
//!   `Adjust`；`Hold` 无标的）。
//!
//! 本模块只含类型与动作排序口径，不含任何交易/结算规则。

use std::collections::BTreeMap;

use strategy_core::signal::Side;
use strategy_core::strategy::{Cooldown, PositionView};

use crate::bridge::CandleSeries;
use crate::core::bar::{Bar, Timeframe};
use crate::core::identity::InstrumentId;
use crate::core::session::TradingDay;

use super::features::FeatureMap;
use super::model::{MasterPositionState, MasterPositionView, SubPosition};

/// 策略可见的总仓快照。
///
/// `view` 与组合级指标同口径（只聚合非 Closed 子仓，按 namespaced_key 排序）；
/// `state` 是总仓生命周期状态，策略据此在 `Closing` 时停止开仓/加仓。
#[derive(Debug, Clone, PartialEq)]
pub struct MasterContext {
    pub master_id: String,
    pub state: MasterPositionState,
    pub view: MasterPositionView,
}

/// 单标的一根已闭合 bar 的决策输入快照。
#[derive(Debug, Clone)]
pub struct InstrumentSnapshot {
    pub instrument: InstrumentId,
    pub bar: Bar,
    pub position: Option<PositionView>,
    /// 该标的数据是否已过期（流动性枯竭 / 上行中断快照）。
    pub stale: bool,
    pub features: FeatureMap,
    pub entry_eligible: bool,
    pub entry_reason: Option<String>,
    pub exit_triggered: Option<String>,
    // 策略决策所需的持仓/账本扩展字段。
    pub trading_day: TradingDay,
    pub no_trade: bool,
    pub entry_confirm: bool,
    pub signal_resume: bool,
    pub cooldown: Option<Cooldown>,
    pub session_open_price: Option<f64>,
    /// 已闭合序列视图（只读）；策略不得在其中写入或取 `now()`。
    pub closed: Option<CandleSeries>,
}

/// 组合级账户上下文（跨标的聚合输入）。
#[derive(Debug, Clone)]
pub struct PortfolioContext {
    pub strategy_id: String,
    pub nav: f64,
    pub drawdown: f64,
    pub portfolio_pnl_pct: f64,
    pub sub_positions: Vec<SubPosition>,
    /// 活动总仓快照；无活动总仓时为 `None`。
    pub master: Option<MasterContext>,
    /// instrument `namespaced_key()` → 合约乘数（`MasterPositionView` 计价口径）。
    pub multipliers: BTreeMap<String, f64>,
    /// 组合总名义预算（NAV 比例，即 `risk.max_total_notional`）。
    /// 策略调仓目标必须与执行层容量闸门同口径：等权目标按本预算而非
    /// 全额 NAV 计，否则目标恒高于容量，每个调仓轮都产生无谓拒单。
    pub max_total_notional: f64,
    pub rebalance_due: bool,
    pub no_trade: bool,
}

/// 组合策略动作全集（统一策略 port 的出口）。
#[derive(Debug, Clone, PartialEq)]
pub enum InstrumentAction {
    // price/atr 为 G5 扩展：旧 `Action::Open` 携带它们，Adapter 必须无损映射；
    // 新横截面策略通常 price = snapshot.bar.close、atr = 0.0。
    Open {
        instrument: InstrumentId,
        side: Side,
        price: f64,
        stop_price: f64,
        atr: f64,
        strength: f64,
        reason: String,
    },
    Close {
        instrument: InstrumentId,
        price: f64,
        reason: String,
        is_stop_loss: bool,
        strength: f64,
    },
    // Reverse 语义：先平后开，平仓失败则放弃开仓（由 dispatch 层保证）。
    Reverse {
        instrument: InstrumentId,
        side: Side,
        price: f64,
        stop_price: f64,
        atr: f64,
        strength: f64,
        close_reason: String,
        is_stop_loss: bool,
    },
    /// 新横截面专用：调整到目标权重（不产生信号 ID，纯仓位调整）。
    Adjust {
        instrument: InstrumentId,
        target_qty: f64,
        reason: String,
    },
    /// 组合级平仓：平掉本策略全部非 `Closed` 总仓及其子仓；无总仓时 no-op。
    CloseMaster { reason: String, is_stop_loss: bool },
    UpdateStop {
        instrument: InstrumentId,
        stop_price: f64,
    },
    UpdateExit {
        instrument: InstrumentId,
        exit_active: bool,
        exit_price: f64,
    },
    /// 无标的：组合级 no-op。
    Hold,
}

impl InstrumentAction {
    /// 变体对应标的的命名空间键（`Hold` 为 `None`）。
    ///
    /// 排序口径与 [`InstrumentId::namespaced_key`] 一致：组合动作流派发前
    /// 必须先按此键升序排序——f64 逐位确定、与构造顺序无关（spec §数值口径）。
    pub fn instrument_key(&self) -> Option<String> {
        match self {
            InstrumentAction::Open { instrument, .. }
            | InstrumentAction::Close { instrument, .. }
            | InstrumentAction::Reverse { instrument, .. }
            | InstrumentAction::Adjust { instrument, .. }
            | InstrumentAction::UpdateStop { instrument, .. }
            | InstrumentAction::UpdateExit { instrument, .. } => Some(instrument.namespaced_key()),
            InstrumentAction::CloseMaster { .. } | InstrumentAction::Hold => None,
        }
    }
}

/// 横截面策略端口。
///
/// 输入为「一批标的在同一已闭合事件下的快照 + 组合级上下文」，输出为
/// 组合动作流（按 [`InstrumentAction::instrument_key`] 升序后派发）。
pub trait CrossSectionalStrategyPort: Send {
    /// 策略类型名（进入 signal_id/报表行键；必须与注册名一致）。
    fn strategy_type(&self) -> &str;

    /// 声明需要的 K 线周期（engine 类型；含 1m）。默认仅 1m。
    fn timeframes(&self) -> Vec<Timeframe> {
        vec![Timeframe::M1]
    }

    fn on_cross_section_bars(
        &mut self,
        newly_closed: &[Timeframe],
        snapshots: &[InstrumentSnapshot],
        portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction>;
}

/// 通用策略接口别名：`CrossSectionalStrategyPort` 是历史命名，新代码用 `StrategyPort`。
pub use self::CrossSectionalStrategyPort as StrategyPort;

impl InstrumentSnapshot {
    /// 当前是否处于止损冷却（口径：bar 结束时刻 = open_time + 1m 的日历日 == 冷却日）。
    pub fn in_stop_loss_cooldown(&self) -> bool {
        match &self.cooldown {
            Some(Cooldown::UntilDate(d)) => {
                let bar_end = self.bar.open_time + chrono::Duration::minutes(1);
                &bar_end.format("%Y-%m-%d").to_string() == d
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InstrumentAction, InstrumentSnapshot, PortfolioContext};
    use crate::core::bar::{Bar, BarExtensions, BarFinality};
    use crate::core::identity::{ExchangeId, InstrumentId};
    use crate::core::session::TradingDay;
    use crate::strategy::features::FeatureMap;
    use chrono::{NaiveDate, TimeZone, Utc};
    use strategy_core::signal::Side;

    fn instr(product: &str) -> InstrumentId {
        InstrumentId::perpetual(&ExchangeId::new("binance"), product)
    }

    fn bar_for(inst: &InstrumentId, close: f64) -> Bar {
        Bar {
            instrument: inst.clone(),
            open_time: Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
            open: close - 1.0,
            high: close + 1.0,
            low: close - 2.0,
            close,
            volume: 1.0,
            finality: BarFinality::Final,
            extensions: BarExtensions::default(),
        }
    }

    fn snapshot(inst: &InstrumentId) -> InstrumentSnapshot {
        InstrumentSnapshot {
            instrument: inst.clone(),
            bar: bar_for(inst, 100.0),
            position: None,
            stale: false,
            features: FeatureMap::default(),
            entry_eligible: true,
            entry_reason: None,
            exit_triggered: None,
            trading_day: TradingDay(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
            no_trade: false,
            entry_confirm: true,
            signal_resume: false,
            cooldown: None,
            session_open_price: None,
            closed: None,
        }
    }

    #[test]
    fn constructs_instrument_snapshot_and_portfolio_context() {
        let btc = instr("BTCUSDT");
        let snap = snapshot(&btc);
        assert_eq!(
            snap.instrument.namespaced_key(),
            "binance/crypto_perpetual/BTCUSDT"
        );
        assert_eq!(snap.bar.close, 100.0);
        assert!(snap.closed.is_none());
        assert!(snap.position.is_none());
        assert!(snap.entry_eligible);
        assert_eq!(
            snap.trading_day,
            TradingDay(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap())
        );

        let pf = PortfolioContext {
            strategy_id: "st1".to_string(),
            nav: 1_000_000.0,
            drawdown: 0.05,
            portfolio_pnl_pct: 0.02,
            sub_positions: Vec::new(),
            master: None,
            multipliers: std::collections::BTreeMap::from([(
                "binance/crypto_perpetual/BTCUSDT".to_string(),
                1.0_f64,
            )]),
            max_total_notional: 1.0,
            rebalance_due: true,
            no_trade: false,
        };
        assert_eq!(
            *pf.multipliers
                .get("binance/crypto_perpetual/BTCUSDT")
                .unwrap(),
            1.0
        );
        assert!(pf.rebalance_due);
        assert_eq!(pf.strategy_id, "st1");
    }

    /// 组合动作流必先按 instrument `namespaced_key()` 字典序升序排序再派发
    /// （spec §数值口径）：同一动作集以不同插入顺序构造，排序后必须逐位一致。
    #[test]
    fn action_vec_sorts_by_namespaced_key_regardless_of_insertion_order() {
        let ada = instr("ADAUSDT");
        let btc = instr("BTCUSDT");
        let doge = instr("DOGEUSDT");
        let eth = instr("ETHUSDT");
        let sol = instr("SOLUSDT");
        let xrp = instr("XRPUSDT");

        let key = |a: &InstrumentAction| a.instrument_key();
        let mut fwd = vec![
            InstrumentAction::Open {
                instrument: btc.clone(),
                side: Side::Long,
                price: 100.0,
                stop_price: 98.0,
                atr: 1.0,
                strength: 0.8,
                reason: "r".into(),
            },
            InstrumentAction::Close {
                instrument: eth.clone(),
                price: 110.0,
                reason: "tp".into(),
                is_stop_loss: false,
                strength: 1.0,
            },
            InstrumentAction::Reverse {
                instrument: sol.clone(),
                side: Side::Short,
                price: 105.0,
                stop_price: 107.0,
                atr: 1.0,
                strength: 0.9,
                close_reason: "flip".into(),
                is_stop_loss: false,
            },
            InstrumentAction::Adjust {
                instrument: ada.clone(),
                target_qty: 0.2,
                reason: "rebalance".into(),
            },
            InstrumentAction::UpdateStop {
                instrument: doge.clone(),
                stop_price: 99.5,
            },
            InstrumentAction::UpdateExit {
                instrument: xrp.clone(),
                exit_active: true,
                exit_price: 100.0,
            },
            InstrumentAction::Hold,
        ];
        let mut rev = fwd.clone();
        rev.reverse();

        fwd.sort_by_key(key);
        rev.sort_by_key(key);

        assert_eq!(fwd, rev, "不同插入顺序排序后必须逐位一致");
        assert!(
            matches!(fwd[0], InstrumentAction::Hold),
            "Hold（无 instrument）排最前"
        );
        let expected = [
            "binance/crypto_perpetual/ADAUSDT",
            "binance/crypto_perpetual/BTCUSDT",
            "binance/crypto_perpetual/DOGEUSDT",
            "binance/crypto_perpetual/ETHUSDT",
            "binance/crypto_perpetual/SOLUSDT",
            "binance/crypto_perpetual/XRPUSDT",
        ];
        for (i, k) in expected.iter().enumerate() {
            assert_eq!(
                fwd[i + 1].instrument_key().as_deref(),
                Some(*k),
                "排序后第 {} 位应为 {k}",
                i + 1
            );
        }
    }

    /// 冷却口径与旧 `Ctx::in_stop_loss_cooldown` 一致：bar 结束时刻
    /// （open_time + 1m）的日历日 == 冷却日。
    #[test]
    fn snapshot_cooldown_uses_bar_open_plus_one_minute() {
        use strategy_core::strategy::Cooldown;

        let btc = instr("BTCUSDT");
        let mut snap = snapshot(&btc);
        snap.cooldown = Some(Cooldown::UntilDate("2026-01-06".into()));

        snap.bar.open_time = Utc.with_ymd_and_hms(2026, 1, 5, 23, 59, 0).unwrap();
        assert!(snap.in_stop_loss_cooldown(), "bar_end 落在冷却日");

        snap.bar.open_time = Utc.with_ymd_and_hms(2026, 1, 5, 23, 58, 0).unwrap();
        assert!(!snap.in_stop_loss_cooldown(), "bar_end 仍在冷却日前一日");
    }
}
