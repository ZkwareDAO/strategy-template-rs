//! 阶段 3 桥接适配层：让 `CrossSectionalStrategyPort` 跑进
//! `ExchangeRunner`（§13 迁移表 / §17 最小嵌入路径-1）。
//!
//! 三件事：
//! 1. [`CandleSeries`]——engine 聚合状态里的 `Bar` 序列桥接为
//!    `Candle` 序列，作为快照 `closed` 数据面（口径冻结于
//!    `crates/app/tests/golden/retirement/`）；
//! 2. [`PortStrategyBridge`]——`StrategyPort` 实现：把统一 port 的
//!    单标快照接口适配为 SPI 单标动作流；
//! 3. [`spec_from_contract`]——`ContractSpec` → `InstrumentSpec`
//!    （§13 适配层契约：`config/contracts.yaml` 数据直通 engine 规格表，
//!    「保持不变」清单承诺的 `Candle`/合约参数读取兼容）。
//!
//! 本模块只做**类型互转与调用路由**，不含任何交易/结算规则——规则
//! 仍然只有 `TradingRules`/`PositionRules` 一个来源（第一公理）。

use std::collections::BTreeMap;

use strategy_core::candle::{Candle, Timeframe as CoreTimeframe};
use strategy_core::contract::ContractSpec;

use crate::core::aggregation::AggregationState;
use crate::core::bar::{Bar, Timeframe};
use crate::core::trading::{FeeMode, FeeSchedule, InstrumentSpec, SizingMode};
use crate::core::types::{DecisionContext, StrategyAction, StrategyPort};
use crate::strategy::features::FeatureMap;
use crate::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};

/// `Bar` → 现有 `Candle`（§13 `Candle <-> Bar` 桥接的反方向；
/// `Bar::from_candle` 已提供正方向）。
///
/// `active_buy_quote_volume` 在 `BarExtensions` 无对应扩展位，按旧
/// `from_candle` 的丢弃方向回填 0——策略侧不消费该字段（仅报表），
/// 双跑差异表登记。
pub fn bar_to_candle(b: &Bar) -> Candle {
    Candle {
        open_time: b.open_time,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
        volume: b.volume,
        quote_volume: b.extensions.quote_volume.unwrap_or(0.0),
        trade_num: b.extensions.trade_count.unwrap_or(0),
        active_buy_volume: b.extensions.taker_buy_volume.unwrap_or(0.0),
        active_buy_quote_volume: 0.0,
        is_closed: b.is_final(),
    }
}

/// `AggregationState` 的 `Candle` 序列视图。
///
/// 增量刷新：某 tf 的根数与末根开线时间和缓存一致 → 跳过重建。
/// 语义对齐旧 `Feed::closed_map`：升序、全为完整桶、`max_len` 截断。
#[derive(Debug, Default, Clone)]
pub struct CandleSeries {
    series: BTreeMap<CoreTimeframe, Vec<Candle>>,
    last_price: Option<f64>,
}

impl CandleSeries {
    /// 与聚合状态同步（每次 `on_closed_bars` 前调用）。
    pub fn refresh(&mut self, state: &AggregationState) {
        for &tf in state.timeframes() {
            let q = state.closed(tf);
            let key: CoreTimeframe = tf.into();
            let cached = self.series.get(&key);
            let fresh = match cached {
                Some(v) => {
                    v.len() == q.len()
                        && v.last().map(|c| c.open_time) == q.back().map(|b| b.open_time)
                }
                None => q.is_empty(),
            };
            if !fresh {
                self.series
                    .insert(key, q.iter().map(bar_to_candle).collect());
            }
        }
        // 旧 Feed::last_price = 最新已闭合 1m 收盘价（1m 粒度实时价语义）
        self.last_price = state.closed(Timeframe::M1).back().map(|b| b.close);
    }

    /// 用单个周期的已闭合序列构造（测试/单标桥 fixture 用）。
    pub fn from_closed(tf: CoreTimeframe, candles: Vec<Candle>) -> Self {
        let mut series = Self::default();
        series.insert_closed(tf, candles);
        series
    }

    /// 写入/替换某周期的已闭合序列；M1 同步最新价。
    pub fn insert_closed(&mut self, tf: CoreTimeframe, candles: Vec<Candle>) {
        if tf == CoreTimeframe::M1 {
            self.last_price = candles.last().map(|c| c.close);
        }
        self.series.insert(tf, candles);
    }

    /// 取某周期的已闭合序列（只读）。
    pub fn closed(&self, tf: CoreTimeframe) -> Option<&[Candle]> {
        self.series.get(&tf).map(|v| v.as_slice())
    }

    /// 最新已闭合 1m 收盘价。
    pub fn last_price(&self) -> Option<f64> {
        self.last_price
    }
}

/// 统一 [`CrossSectionalStrategyPort`] → [`StrategyPort`] 适配器（SPI 单标路径）。
///
/// 把 `DecisionContext` 包成恰一个 `InstrumentSnapshot`（`closed` 为聚合
/// 状态视图）与中性 `PortfolioContext`；`Adjust` 在 SPI 动作集中无对应，
/// 显式降级为 `Hold`，不得被误映射成开/平仓。
pub struct PortStrategyBridge {
    strategy: Box<dyn CrossSectionalStrategyPort>,
    source: CandleSeries,
}

impl PortStrategyBridge {
    pub fn new(strategy: Box<dyn CrossSectionalStrategyPort>) -> Self {
        Self {
            strategy,
            source: CandleSeries::default(),
        }
    }

    /// 策略声明的周期（engine 类型；`ExchangeRunner` 构造入参）。
    pub fn timeframes(&self) -> Vec<Timeframe> {
        self.strategy.timeframes()
    }

    pub fn strategy_type(&self) -> &str {
        self.strategy.strategy_type()
    }

    /// 桥接后的已闭合序列（测试/报表断言用，与策略读到的同一份缓存）。
    pub fn closed_source(&self) -> &CandleSeries {
        &self.source
    }
}

impl StrategyPort for PortStrategyBridge {
    fn on_closed_bars(
        &mut self,
        newly_closed: &[Timeframe],
        state: &AggregationState,
        ctx: &DecisionContext,
    ) -> Vec<StrategyAction> {
        self.source.refresh(state);
        let snapshots = [InstrumentSnapshot {
            instrument: ctx.instrument.clone(),
            bar: ctx.bar.clone(),
            position: ctx.position,
            stale: false,
            features: FeatureMap::default(),
            entry_eligible: true,
            entry_reason: None,
            exit_triggered: None,
            trading_day: ctx.trading_day,
            no_trade: ctx.no_trade,
            entry_confirm: ctx.entry_confirm,
            signal_resume: ctx.signal_resume,
            cooldown: ctx.cooldown.clone(),
            session_open_price: ctx.session_open_price,
            closed: Some(self.source.clone()),
        }];
        let portfolio = PortfolioContext {
            strategy_id: self.strategy.strategy_type().to_string(),
            nav: 0.0,
            drawdown: 0.0,
            portfolio_pnl_pct: 0.0,
            sub_positions: Vec::new(),
            master: None,
            multipliers: BTreeMap::new(),
            max_total_notional: 1.0,
            rebalance_due: false,
            no_trade: ctx.no_trade,
        };
        self.strategy
            .on_cross_section_bars(newly_closed, &snapshots, &portfolio)
            .into_iter()
            .map(port_action)
            .collect()
    }
}

/// [`InstrumentAction`] → [`StrategyAction`]（去 instrument；`Adjust` 无对应）。
fn port_action(a: InstrumentAction) -> StrategyAction {
    match a {
        InstrumentAction::Open {
            side,
            price,
            stop_price,
            atr,
            strength,
            reason,
            ..
        } => StrategyAction::Open {
            side,
            price,
            stop_price,
            atr,
            strength,
            reason,
        },
        InstrumentAction::Close {
            price,
            reason,
            is_stop_loss,
            strength,
            ..
        } => StrategyAction::Close {
            price,
            reason,
            is_stop_loss,
            strength,
        },
        InstrumentAction::Reverse {
            side,
            price,
            stop_price,
            atr,
            strength,
            close_reason,
            is_stop_loss,
            ..
        } => StrategyAction::Reverse {
            side,
            price,
            stop_price,
            atr,
            strength,
            close_reason,
            is_stop_loss,
        },
        InstrumentAction::UpdateStop { stop_price, .. } => {
            StrategyAction::UpdateStop { stop_price }
        }
        InstrumentAction::UpdateExit {
            exit_active,
            exit_price,
            ..
        } => StrategyAction::UpdateExit {
            exit_active,
            exit_price,
        },
        // SPI 动作集无 Adjust/CloseMaster；单标桥无法执行组合级动作。
        InstrumentAction::Adjust { .. }
        | InstrumentAction::CloseMaster { .. }
        | InstrumentAction::Hold => StrategyAction::Hold,
    }
}

/// `ContractSpec` → `InstrumentSpec`（§13「现有 `config/contracts.yaml`
/// 数据保持不变，映射到 `InstrumentSpec`」的落点）。
///
/// crypto 不走本映射（由 `exchanges/binance` 配置直建规格）。期货侧字段
/// 逐项对应；国内期货整数手合同 `qty_step=1 / min_lots≥1 / 精度 0`；
/// `close_yesterday` 无旧配置来源 → None（= close 档，不伪造）。
pub fn spec_from_contract(c: &ContractSpec) -> InstrumentSpec {
    InstrumentSpec {
        multiplier: c.multiplier,
        tick_size: c.tick_size,
        qty_step: 1.0,
        min_qty: c.min_lots.max(1) as f64,
        max_qty: None,
        price_precision: 0,
        qty_precision: 0,
        margin_rate: c.margin_rate,
        fee: FeeSchedule {
            mode: match c.commission.mode {
                strategy_core::contract::FeeMode::Rate => FeeMode::Rate,
                strategy_core::contract::FeeMode::PerLot => FeeMode::PerLot,
            },
            open: c.commission.open,
            close: c.commission.close,
            close_today: c.commission.close_today,
            close_yesterday: None,
        },
        currency: "CNY".into(),
        sizing_mode: SizingMode::MarginLots,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::aggregation::AggregationState;
    use crate::core::identity::{ExchangeId, InstrumentId};
    use crate::core::session::TradingDay;
    use crate::strategy::features::FeatureMap;
    use crate::strategy::port::{
        CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
    };
    use chrono::{NaiveDate, TimeZone, Utc};
    use std::sync::{Arc, Mutex};
    use strategy_core::contract::{CommissionSpec, FeeMode as CoreFeeMode};
    use strategy_core::signal::Side;
    use strategy_core::strategy::Cooldown;

    fn fut() -> InstrumentId {
        InstrumentId::futures_contract(
            &ExchangeId::new("SHFE"),
            "CU",
            crate::core::identity::ContractMonth {
                year: 2026,
                month: 1,
            },
        )
    }

    /// §13 桥接兼容承诺：`Bar::from_candle(c) |> bar_to_candle == c`
    /// （回测 CSV 帧，`is_closed=true`、策略消费面字段逐项相等）。
    #[test]
    fn candle_bar_roundtrip_is_lossless_for_strategy_fields() {
        let c = Candle {
            open_time: Utc.with_ymd_and_hms(2026, 1, 5, 1, 30, 0).unwrap(),
            open: 78_000.0,
            high: 78_010.0,
            low: 77_990.0,
            close: 78_005.0,
            volume: 120.0,
            quote_volume: 9_360_600.0,
            trade_num: 42,
            active_buy_volume: 60.0,
            active_buy_quote_volume: 4_680_300.0,
            is_closed: true,
        };
        let back = bar_to_candle(&Bar::from_candle(fut(), c));
        assert_eq!(back.open_time, c.open_time);
        assert_eq!(
            (back.open, back.high, back.low, back.close, back.volume),
            (c.open, c.high, c.low, c.close, c.volume)
        );
        assert_eq!(back.quote_volume, c.quote_volume);
        assert_eq!(back.trade_num, c.trade_num);
        assert_eq!(back.active_buy_volume, c.active_buy_volume);
        assert!(back.is_closed);
    }

    #[test]
    fn spec_from_contract_maps_fee_and_sizing() {
        let c = ContractSpec {
            name: Some("铜".into()),
            multiplier: 5.0,
            tick_size: 10.0,
            margin_rate: 0.11,
            min_lots: 1,
            session_opens: vec!["09:00".into(), "21:00".into()],
            no_trade_after_open_minutes: 5,
            session_closes: vec!["15:00".into(), "01:00".into()],
            no_trade_before_close_minutes: 5,
            commission: CommissionSpec {
                mode: CoreFeeMode::Rate,
                open: 0.00005,
                close: 0.00005,
                close_today: Some(0.0001),
            },
        };
        let s = spec_from_contract(&c);
        assert_eq!(
            (s.multiplier, s.tick_size, s.margin_rate),
            (5.0, 10.0, 0.11)
        );
        assert_eq!(s.sizing_mode, SizingMode::MarginLots);
        assert_eq!(s.fee.mode, FeeMode::Rate);
        assert_eq!(s.fee.open, 0.00005);
        assert_eq!(s.fee.close_today, Some(0.0001));
        assert!(s.fee.close_yesterday.is_none(), "旧配置无此档，不伪造");
        assert_eq!(s.qty_step, 1.0, "整数手合同");
    }

    /// PortStrategyBridge 的单标输入夹具：flat、非 stale、可入场。
    fn port_decision_ctx(cooldown: Option<Cooldown>) -> DecisionContext {
        let inst = fut();
        let at = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
        let bar = Bar {
            instrument: inst.clone(),
            open_time: at,
            open: 99.0,
            high: 101.0,
            low: 98.0,
            close: 100.0,
            volume: 1.0,
            finality: crate::core::bar::BarFinality::Final,
            extensions: crate::core::bar::BarExtensions::default(),
        };
        DecisionContext {
            instrument: inst,
            at,
            price: 100.0,
            trading_day: TradingDay(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap()),
            no_trade: false,
            entry_confirm: true,
            signal_resume: false,
            bar,
            position: None,
            cooldown,
            session_open_price: Some(99.5),
        }
    }

    /// 单标 DecisionContext → 恰一个快照；Open 去 instrument 后映射；
    /// 组合上下文除 strategy_id/no_trade 外保持中性。
    #[test]
    fn port_bridge_maps_flat_context_to_one_snapshot_and_open_action() {
        #[allow(clippy::type_complexity)]
        struct RecordingPort {
            seen: Arc<Mutex<Vec<(Vec<InstrumentSnapshot>, PortfolioContext)>>>,
        }
        impl CrossSectionalStrategyPort for RecordingPort {
            fn strategy_type(&self) -> &str {
                "recording"
            }
            fn timeframes(&self) -> Vec<Timeframe> {
                vec![Timeframe::M1, Timeframe::H1]
            }
            fn on_cross_section_bars(
                &mut self,
                _newly: &[Timeframe],
                snapshots: &[InstrumentSnapshot],
                portfolio: &PortfolioContext,
            ) -> Vec<InstrumentAction> {
                self.seen
                    .lock()
                    .unwrap()
                    .push((snapshots.to_vec(), portfolio.clone()));
                let s = &snapshots[0];
                vec![InstrumentAction::Open {
                    instrument: s.instrument.clone(),
                    side: Side::Long,
                    price: s.bar.close,
                    stop_price: 98.0,
                    atr: 1.0,
                    strength: 0.8,
                    reason: "entry".into(),
                }]
            }
        }

        let ctx = port_decision_ctx(Some(Cooldown::UntilDate("2026-01-05".into())));
        let state = AggregationState::new(&[Timeframe::M1, Timeframe::H1], 300);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut bridge = PortStrategyBridge::new(Box::new(RecordingPort { seen: seen.clone() }));

        let out = bridge.on_closed_bars(&[Timeframe::M1], &state, &ctx);
        assert_eq!(
            out,
            vec![StrategyAction::Open {
                side: Side::Long,
                price: 100.0,
                stop_price: 98.0,
                atr: 1.0,
                strength: 0.8,
                reason: "entry".into(),
            }]
        );
        assert_eq!(bridge.strategy_type(), "recording");
        assert_eq!(bridge.timeframes(), vec![Timeframe::M1, Timeframe::H1]);
        assert!(bridge.closed_source().closed(CoreTimeframe::M1).is_none());

        let seen = seen.lock().unwrap();
        let (snapshots, portfolio) = &seen[0];
        assert_eq!(snapshots.len(), 1, "单标桥每次只产生一个快照");
        let s = &snapshots[0];
        assert_eq!(
            s.instrument.namespaced_key(),
            ctx.instrument.namespaced_key()
        );
        assert_eq!(s.bar.close, 100.0);
        assert!(s.position.is_none());
        assert!(!s.stale);
        assert!(s.entry_eligible);
        assert_eq!(s.features, FeatureMap::default());
        assert!(s.closed.is_some());
        assert_eq!(s.trading_day, ctx.trading_day);
        assert_eq!(s.no_trade, ctx.no_trade);
        assert_eq!(s.entry_confirm, ctx.entry_confirm);
        assert_eq!(s.signal_resume, ctx.signal_resume);
        assert_eq!(s.cooldown, ctx.cooldown);
        assert_eq!(s.session_open_price, ctx.session_open_price);
        assert_eq!(portfolio.strategy_id, "recording");
        assert_eq!(portfolio.nav, 0.0);
        assert_eq!(portfolio.drawdown, 0.0);
        assert_eq!(portfolio.portfolio_pnl_pct, 0.0);
        assert!(portfolio.sub_positions.is_empty());
        assert!(portfolio.multipliers.is_empty());
        assert!(!portfolio.rebalance_due);
        assert_eq!(portfolio.no_trade, ctx.no_trade);
    }

    /// SPI `StrategyAction` 无 `Adjust`：必须显式降级为 `Hold`，不得误改仓。
    #[test]
    fn port_bridge_maps_adjust_to_hold() {
        struct AdjustPort;
        impl CrossSectionalStrategyPort for AdjustPort {
            fn strategy_type(&self) -> &str {
                "adjust"
            }
            fn on_cross_section_bars(
                &mut self,
                _newly: &[Timeframe],
                snapshots: &[InstrumentSnapshot],
                _portfolio: &PortfolioContext,
            ) -> Vec<InstrumentAction> {
                vec![InstrumentAction::Adjust {
                    instrument: snapshots[0].instrument.clone(),
                    target_qty: 5.0,
                    reason: "rebalance".into(),
                }]
            }
        }

        let ctx = port_decision_ctx(None);
        let state = AggregationState::new(&[Timeframe::M1], 300);
        let mut bridge = PortStrategyBridge::new(Box::new(AdjustPort));
        let out = bridge.on_closed_bars(&[Timeframe::M1], &state, &ctx);
        assert_eq!(out, vec![StrategyAction::Hold]);
    }

    /// 增量刷新：桥接序列与聚合状态逐项一致；C8 首桶语义直通。
    #[test]
    fn candle_series_refresh_matches_state_and_keeps_aligned_head() {
        use crate::core::aggregation::UtcGridPolicy;
        use crate::core::bar::{BarExtensions, BarFinality};
        use crate::core::session::ContinuousSession;
        let inst = fut();
        let mut state = AggregationState::new(&[Timeframe::M1, Timeframe::H1], 300);
        let policy = UtcGridPolicy;
        let session = ContinuousSession::default();
        let mut series = CandleSeries::default();
        let base = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
        for i in 0..61 {
            let t = base + chrono::Duration::minutes(i);
            state
                .on_closed_1m(
                    Bar {
                        instrument: inst.clone(),
                        open_time: t,
                        open: 100.0,
                        high: 101.0,
                        low: 99.0,
                        close: 100.5,
                        volume: 1.0,
                        finality: BarFinality::Final,
                        extensions: BarExtensions::default(),
                    },
                    &policy,
                    &session,
                )
                .unwrap();
            series.refresh(&state);
        }
        assert_eq!(series.closed(CoreTimeframe::M1).unwrap().len(), 61);
        // H1：首桶（00:00 起）恰在桶起点（C8 保留），第 61 根尚不闭合第二桶
        let h1 = series.closed(CoreTimeframe::H1).unwrap();
        assert_eq!(h1.len(), 1);
        assert_eq!(h1[0].open_time, base);
        assert_eq!(series.last_price(), Some(100.5));
    }
}
