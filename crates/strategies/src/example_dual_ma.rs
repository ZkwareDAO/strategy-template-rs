//! example_dual_ma — 双均线交叉入门教学策略（脚手架起点）
//!
//! **从这里开始写你的第一个策略**：本文件是完整可跑的最小示例，
//! 复制为 `src/my_strategy.rs` 改名字和逻辑即可，不需要修改任何其他文件。
//! 配置里增加 `universe` 行即可从单标的升级为横截面：本策略按快照逐标的
//! 独立决策，组合层负责排序与派发。
//!
//! 阅读顺序：`docs/strategy/QUICKSTART.md` → 本文件 → `docs/strategy/DEVELOPMENT_GUIDE.md`
//!
//! ## 策略逻辑（教科书式，无盈利预期）
//! - 入场（空仓时，1m 闭合驱动）：
//!   主周期（如 4h）上，快线上穿慢线 → 开多；下穿 → 开空。
//!   初始止损 = 入场价 ± `stop_loss_pct`%。
//! - 出场（持仓时）：
//!   止损触发 → 平仓；均线反向交叉 → 趋势离场。
//!
//! ## 本文件展示的 5 件事
//! 1. `Params` 私有参数：`deny_unknown_fields` 让写错字段名在启动时就报错
//! 2. `register_strategy!` 自注册：放文件尾即可，`build.rs` 自动发现
//! 3. `CrossSectionalStrategyPort` 三方法：`strategy_type` / `timeframes` /
//!    `on_cross_section_bars`
//! 4. 只读已闭合数据：`s.closed.closed(tf)`——未闭合 K 线在类型层不可见
//! 5. 测试样板：`CandleSeries::from_closed` 喂合成已闭合 K 线

use serde::Deserialize;

use strategy_core::candle::Timeframe;
use strategy_core::signal::Side;
use strategy_core::strategy::PositionView;
use strategy_engine::bridge::CandleSeries;
use strategy_engine::core::bar::{Bar, Timeframe as EngineTimeframe};
use strategy_engine::strategy::port::{
    CrossSectionalStrategyPort, InstrumentAction, InstrumentSnapshot, PortfolioContext,
};
use strategy_indicators::talib;

/// 最少主周期 K 线数（快慢线 + 计算缓冲；数据不足时策略保持沉默）
const MIN_ROWS: usize = 60;

/// 策略私有参数（配置文件 params 节透传；deny_unknown_fields：写错字段名启动即报错）
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Params {
    /// 快线周期
    pub fast_period: usize,
    /// 慢线周期
    pub slow_period: usize,
    /// 初始止损百分比（入场价的 ±%）
    pub stop_loss_pct: f64,
    /// 止损触碰判定：close（默认）或 high_low（盘中插针即触发）
    pub exit_price_source: ExitPriceSource,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            fast_period: 10,
            slow_period: 30,
            stop_loss_pct: 2.0,
            exit_price_source: ExitPriceSource::Close,
        }
    }
}

/// 止损触发价来源
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitPriceSource {
    /// 用 1m 收盘价判定（默认；噪声小）
    #[default]
    Close,
    /// 用 1m high/low 判定（保守；盘中触及即触发）
    HighLow,
}

/// 运行时完整配置（Params + 引擎注入项合并后的产物）
#[derive(Debug, Clone)]
pub struct Config {
    pub fast_period: usize,
    pub slow_period: usize,
    pub stop_loss_pct: f64,
    pub exit_price_source: ExitPriceSource,
    /// 主周期（CLI/配置顶层 timeframe 透传，策略自行 parse）
    pub main_timeframe: Timeframe,
}

pub struct ExampleDualMa {
    pub config: Config,
}

impl ExampleDualMa {
    /// 快慢线交叉事件：Some(true)=本根上穿，Some(false)=本根下穿，None=无交叉或数据不足
    ///
    /// **交叉事件判定**（非状态判定）：比较最新一根与前一根的快慢关系。
    /// 前一根 快<=慢 且 最新根 快>慢 → 上穿；反之 → 下穿。
    /// 状态持续在某一侧时不触发——避免"仅因快线在慢线上方就反复开仓"。
    fn cross_signal(&self, closed: &CandleSeries) -> Option<bool> {
        let closed = closed.closed(self.config.main_timeframe)?;
        let closes: Vec<f64> = closed.iter().map(|c| c.close).collect();
        if closes.len() < MIN_ROWS {
            return None;
        }
        let fast = talib::ema(&closes, self.config.fast_period).ok()?;
        let slow = talib::ema(&closes, self.config.slow_period).ok()?;
        let n = fast.len().min(slow.len());
        if n < 2 {
            return None;
        }
        let (fast_now, slow_now) = (fast[n - 1], slow[n - 1]);
        let (fast_prev, slow_prev) = (fast[n - 2], slow[n - 2]);
        if fast_prev <= slow_prev && fast_now > slow_now {
            return Some(true); // 上穿
        }
        if fast_prev >= slow_prev && fast_now < slow_now {
            return Some(false); // 下穿
        }
        None // 无交叉
    }

    /// 止损价是否被当前 1m 触碰（按 exit_price_source 判定）
    fn stop_hit(&self, bar: &Bar, side: Side, stop_price: f64) -> bool {
        match (side, self.config.exit_price_source) {
            (Side::Long, ExitPriceSource::Close) => bar.close <= stop_price,
            (Side::Long, ExitPriceSource::HighLow) => bar.low <= stop_price,
            (Side::Short, ExitPriceSource::Close) => bar.close >= stop_price,
            (Side::Short, ExitPriceSource::HighLow) => bar.high >= stop_price,
        }
    }

    /// 空仓入场：止损冷却中或数据不足时保持沉默。
    fn entry_action(&self, s: &InstrumentSnapshot, closed: &CandleSeries) -> Option<InstrumentAction> {
        if s.in_stop_loss_cooldown() {
            return None;
        }
        let cross_up = self.cross_signal(closed)?;
        let side = if cross_up { Side::Long } else { Side::Short };
        let price = s.bar.close;
        let stop_price = match side {
            Side::Long => price * (1.0 - self.config.stop_loss_pct / 100.0),
            Side::Short => price * (1.0 + self.config.stop_loss_pct / 100.0),
        };
        Some(InstrumentAction::Open {
            instrument: s.instrument.clone(),
            side,
            price,
            stop_price,
            atr: 0.0,
            strength: 1.0,
            reason: format!(
                "双均线交叉：快{}{}慢{}（上穿/下穿事件）",
                self.config.fast_period,
                if cross_up { ">" } else { "<" },
                self.config.slow_period
            ),
        })
    }

    /// 持仓出场：硬止损优先，其次均线反向交叉事件。
    fn exit_action(
        &self,
        s: &InstrumentSnapshot,
        closed: &CandleSeries,
        pos: &PositionView,
    ) -> Option<InstrumentAction> {
        let price = s.bar.close;

        // ① 硬止损（pos.stop_price 是引擎持久化的最新止损价）
        if self.stop_hit(&s.bar, pos.side, pos.stop_price) {
            return Some(InstrumentAction::Close {
                instrument: s.instrument.clone(),
                price,
                reason: "止损触发".into(),
                is_stop_loss: true,
                strength: 1.0,
            });
        }

        // ② 均线反向交叉事件 → 趋势离场
        let cross_up = self.cross_signal(closed)?;
        let against = match pos.side {
            Side::Long => !cross_up, // 多头持仓遇到下穿 → 离场
            Side::Short => cross_up, // 空头持仓遇到上穿 → 离场
        };
        against.then(|| InstrumentAction::Close {
            instrument: s.instrument.clone(),
            price,
            reason: "均线反向交叉".into(),
            is_stop_loss: false,
            strength: 1.0,
        })
    }
}

impl CrossSectionalStrategyPort for ExampleDualMa {
    fn strategy_type(&self) -> &str {
        "example_dual_ma"
    }

    fn timeframes(&self) -> Vec<EngineTimeframe> {
        vec![
            EngineTimeframe::M1,
            EngineTimeframe::from(self.config.main_timeframe),
        ]
    }

    /// 逐快照决策：空仓走入场，有持仓走出场；缺失已闭合序列的标的保持沉默。
    /// 单标的与 N>1 走同一份代码——增加 universe 行即成为横截面策略。
    fn on_cross_section_bars(
        &mut self,
        _newly_closed: &[EngineTimeframe],
        snapshots: &[InstrumentSnapshot],
        _portfolio: &PortfolioContext,
    ) -> Vec<InstrumentAction> {
        let mut out = Vec::new();
        for s in snapshots {
            let Some(closed) = s.closed.as_ref() else {
                continue;
            };
            let action = match s.position {
                None => self.entry_action(s, closed),
                Some(pos) => self.exit_action(s, closed, &pos),
            };
            if let Some(a) = action {
                out.push(a);
            }
        }
        out
    }
}

// ─── 注册表接线（整个文件唯一必须的"模板胶水"）────────────────────────
crate::register_strategy! {
    name: "example_dual_ma",
    factory: |args| {
        let main_timeframe = Timeframe::parse(args.timeframe)?;
        let params: Params = crate::registry::params_or_default(args.params)?;
        Ok(Box::new(ExampleDualMa {
            config: Config {
                fast_period: params.fast_period,
                slow_period: params.slow_period,
                stop_loss_pct: params.stop_loss_pct,
                exit_price_source: params.exit_price_source,
                main_timeframe,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, TimeZone, Utc};
    use strategy_core::candle::Candle;
    use strategy_engine::core::bar::{BarExtensions, BarFinality};
    use strategy_engine::core::identity::{ExchangeId, InstrumentId};
    use strategy_engine::core::session::TradingDay;
    use strategy_engine::strategy::features::FeatureMap;

    fn inst(symbol: &str) -> InstrumentId {
        InstrumentId::perpetual(&ExchangeId::new("binance"), symbol)
    }

    fn make_candle(close: f64) -> Candle {
        Candle {
            open_time: Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
            open: close,
            high: close,
            low: close,
            close,
            volume: 0.0,
            quote_volume: 0.0,
            trade_num: 0,
            active_buy_volume: 0.0,
            active_buy_quote_volume: 0.0,
            is_closed: true,
        }
    }

    /// 已闭合序列：主周期用给定收盘序列，1m 用末值（只读封闭桶）。
    fn series(closes: &[f64]) -> CandleSeries {
        let last = closes.last().copied().unwrap_or(0.0);
        let mut out = CandleSeries::from_closed(Timeframe::M1, vec![make_candle(last)]);
        out.insert_closed(Timeframe::H4, closes.iter().map(|&c| make_candle(c)).collect());
        out
    }

    fn bar(symbol: &str, close: f64) -> Bar {
        Bar {
            instrument: inst(symbol),
            open_time: Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap(),
            open: close,
            high: close,
            low: close,
            close,
            volume: 0.0,
            finality: BarFinality::Final,
            extensions: BarExtensions::default(),
        }
    }

    fn snapshot(
        symbol: &str,
        closed: Option<CandleSeries>,
        position: Option<PositionView>,
        close: f64,
    ) -> InstrumentSnapshot {
        InstrumentSnapshot {
            instrument: inst(symbol),
            bar: bar(symbol, close),
            position,
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
            closed,
        }
    }

    fn portfolio() -> PortfolioContext {
        PortfolioContext {
            strategy_id: "example_dual_ma:default".into(),
            nav: 1000.0,
            drawdown: 0.0,
            portfolio_pnl_pct: 0.0,
            sub_positions: Vec::new(),
            master: None,
            multipliers: Default::default(),
            max_total_notional: 1.0,
            rebalance_due: false,
            no_trade: false,
        }
    }

    fn long_position(stop_price: f64) -> Option<PositionView> {
        Some(PositionView {
            side: Side::Long,
            entry_price: 100.0,
            stop_price,
            peak_price: 105.0,
            max_pnl_pct: 5.0,
            min_pnl_pct: -2.0,
            exit_active: false,
            exit_price: 0.0,
        })
    }

    fn strat() -> ExampleDualMa {
        ExampleDualMa {
            config: Config {
                fast_period: 10,
                slow_period: 30,
                stop_loss_pct: 2.0,
                exit_price_source: ExitPriceSource::Close,
                main_timeframe: Timeframe::H4,
            },
        }
    }

    fn run(snaps: &[InstrumentSnapshot]) -> Vec<InstrumentAction> {
        strat().on_cross_section_bars(&[EngineTimeframe::M1], snaps, &portfolio())
    }

    #[test]
    fn uptrend_opens_long_with_stop_below() {
        // 100 根走平（快线≈慢线，无交叉）+ 末根跳升 → 快线上穿慢线
        let mut closes: Vec<f64> = vec![100.0; 100];
        closes.push(120.0);
        let snaps = [snapshot("BTCUSDT", Some(series(&closes)), None, 120.0)];
        let actions = run(&snaps);
        assert_eq!(actions.len(), 1, "末端上穿事件应开多，实际: {actions:?}");
        match &actions[0] {
            InstrumentAction::Open {
                instrument,
                side,
                stop_price,
                price,
                ..
            } => {
                assert_eq!(instrument.namespaced_key(), "binance/crypto_perpetual/BTCUSDT");
                assert_eq!(*side, Side::Long);
                assert!(*price > 0.0);
                assert!(stop_price < price, "多头止损应在入场价下方");
            }
            a => panic!("应开多，实际: {a:?}"),
        }
    }

    #[test]
    fn insufficient_data_is_silent() {
        let closes: Vec<f64> = (0..20).map(|i| 100.0 + i as f64).collect();
        let snaps = [snapshot("BTCUSDT", Some(series(&closes)), None, 119.0)];
        assert!(run(&snaps).is_empty(), "数据不足应保持沉默");
    }

    #[test]
    fn stop_hit_closes_position() {
        let closes: Vec<f64> = (0..100).map(|i| 100.0 + i as f64).collect();
        // 多头持仓，stop_price=98，当前 1m close=95 → 触发止损平仓
        let snaps = [snapshot(
            "BTCUSDT",
            Some(series(&closes)),
            long_position(98.0),
            95.0,
        )];
        let actions = run(&snaps);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            InstrumentAction::Close {
                instrument,
                is_stop_loss,
                ..
            } => {
                assert_eq!(instrument.namespaced_key(), "binance/crypto_perpetual/BTCUSDT");
                assert!(*is_stop_loss);
            }
            a => panic!("应止损平仓，实际: {a:?}"),
        }
    }

    /// 快照缺失已闭合序列（引擎未提供封闭桶）→ 类型层无法读未来数据，
    /// 策略保持沉默而不是 panic。
    #[test]
    fn missing_closed_series_is_silent() {
        let snaps = [snapshot("BTCUSDT", None, None, 199.0)];
        assert!(run(&snaps).is_empty());
    }

    /// 交叉事件判定：持续上升序列（快线长期在慢线上方）末端不应再开仓——
    /// 仅当最新一根出现"上穿"事件时才入场。当前实现若只看状态（快>慢→开多）
    /// 此测试会失败，暴露"文档说交叉事件、代码是状态判定"的缺陷。
    #[test]
    fn no_cross_at_end_stays_flat() {
        // 100 根持续上升：快线长期在慢线上方（状态=多头），但末端无新交叉
        let closes: Vec<f64> = (0..100).map(|i| 100.0 + i as f64).collect();
        let snaps = [snapshot("BTCUSDT", Some(series(&closes)), None, 199.0)];
        assert!(
            run(&snaps).is_empty(),
            "末端无交叉事件时不应开仓（仅状态在慢线上方不够）"
        );
    }

    /// 空头入场：100 根走平 + 末根跳降 → 下穿事件 → 开空，止损在入场价上方
    #[test]
    fn downtrend_opens_short_with_stop_above() {
        let mut closes: Vec<f64> = vec![100.0; 100];
        closes.push(80.0); // 末根跳降 → 下穿
        let snaps = [snapshot("BTCUSDT", Some(series(&closes)), None, 80.0)];
        let actions = run(&snaps);
        assert_eq!(actions.len(), 1, "末端下穿事件应开空，实际: {actions:?}");
        match &actions[0] {
            InstrumentAction::Open {
                side,
                stop_price,
                price,
                ..
            } => {
                assert_eq!(*side, Side::Short);
                assert!(stop_price > price, "空头止损应在入场价上方");
            }
            a => panic!("应开空，实际: {a:?}"),
        }
    }

    /// 反向交叉出场：多头持仓遇下穿事件 → 平仓（非止损）
    #[test]
    fn reverse_cross_exits_long_position() {
        let mut closes: Vec<f64> = vec![100.0; 100];
        closes.push(80.0); // 末根跳降 → 下穿（多头反向信号）
        let snaps = [snapshot(
            "BTCUSDT",
            Some(series(&closes)),
            long_position(95.0),
            98.0,
        )];
        let actions = run(&snaps);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            InstrumentAction::Close {
                is_stop_loss,
                reason,
                ..
            } => {
                assert!(!is_stop_loss, "反转离场不是止损");
                assert!(reason.contains("交叉"), "离场原因应是交叉反转: {reason}");
            }
            a => panic!("应平仓，实际: {a:?}"),
        }
    }

    /// 同一份代码接受多个快照：增加 universe 行即从单标升级为横截面。
    #[test]
    fn multiple_snapshots_emit_actions_per_instrument() {
        let mut closes: Vec<f64> = vec![100.0; 100];
        closes.push(120.0);
        let snaps = [
            snapshot("BTCUSDT", Some(series(&closes)), None, 120.0),
            snapshot("ETHUSDT", Some(series(&closes)), None, 120.0),
        ];
        let actions = run(&snaps);
        assert_eq!(
            actions.len(),
            2,
            "两个标的上穿 → 两个开仓动作: {actions:?}"
        );
    }
}
