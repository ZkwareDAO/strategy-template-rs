//! example_mtf_trend — 多周期趋势教学策略（脚手架多周期样板）
//!
//! **教学要点**：`timeframes()` 多周期、跨周期 `closed_data`、ATR 初始止损、反转出场。
//! 阅读顺序：`docs/strategy/EXAMPLES.md` 本文件章节 → 对照源码。
//!
//! ## 策略逻辑（对齐 Python example_mtf_trend 的三层条件）
//! - 趋势过滤（1d）：收盘价 > EMA50 允许做多；< EMA50 允许做空
//! - 触发（4h）：MA10 上穿 MA30 → 开多；下穿 → 开空
//! - 初始止损：入场价 ± `atr_stop_mult` × ATR14(4h)
//! - 出场：止损触发，或 4h 快慢线反向交叉
//!
//! ## 多周期约定
//! - `timeframes()` 返回 `[M1, H4, D1]`：1m 是引擎时钟（必须），H4 触发，D1 趋势
//! - 读数据一律 `ctx.closed_data_min(tf, min_rows)`，每个周期独立做长度检查
//! - 指标全量重算：每次从完整已闭合序列算，不缓存增量

use serde::Deserialize;

use strategy_core::candle::Timeframe;
use strategy_core::signal::{Action, Side};
use strategy_core::strategy::{Ctx, Strategy};
use strategy_indicators::talib;

/// 1d 趋势层最少根数（EMA50 + 缓冲）
const MIN_ROWS_TREND: usize = 60;
/// 4h 信号层最少根数（MA30 + ATR14 + 缓冲）
const MIN_ROWS_SIGNAL: usize = 40;

/// 策略私有参数（配置 params 节透传；写错字段名启动即报错）
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Params {
    /// 4h 快线 EMA 周期
    pub fast_period: usize,
    /// 4h 慢线 EMA 周期
    pub slow_period: usize,
    /// 1d 趋势 EMA 周期
    pub trend_ema_period: usize,
    /// 4h ATR 周期
    pub atr_period: usize,
    /// 初始止损距离 = atr_stop_mult × ATR
    pub atr_stop_mult: f64,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            fast_period: 10,
            slow_period: 30,
            trend_ema_period: 50,
            atr_period: 14,
            atr_stop_mult: 2.0,
        }
    }
}

/// 运行时配置
#[derive(Debug, Clone)]
pub struct Config {
    pub fast_period: usize,
    pub slow_period: usize,
    pub trend_ema_period: usize,
    pub atr_period: usize,
    pub atr_stop_mult: f64,
    /// 触发主周期（配置 `timeframe` 透传；H4 层的交叉/ATR 均用此周期）
    pub main_timeframe: Timeframe,
}

pub struct ExampleMtfTrend {
    pub config: Config,
}

impl ExampleMtfTrend {
    /// 1d 趋势过滤：Some(true)=允许做多，Some(false)=允许做空，None=数据不足
    fn trend_allows_long(&self, ctx: &Ctx<'_>) -> Option<bool> {
        let closed = ctx.closed_data_min(Timeframe::D1, MIN_ROWS_TREND)?;
        let closes: Vec<f64> = closed.iter().map(|c| c.close).collect();
        let ema = talib::ema(&closes, self.config.trend_ema_period).ok()?;
        Some(closed.last()?.close > *ema.last()?)
    }

    /// 4h 快慢线交叉事件：Some(true)=本根上穿，Some(false)=本根下穿，None=无交叉或数据不足
    fn cross_signal(&self, ctx: &Ctx<'_>) -> Option<bool> {
        let closed = ctx.closed_data_min(self.config.main_timeframe, MIN_ROWS_SIGNAL)?;
        let closes: Vec<f64> = closed.iter().map(|c| c.close).collect();
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
        None
    }

    /// 4h ATR（初始止损距离基准）
    fn atr(&self, ctx: &Ctx<'_>) -> Option<f64> {
        let closed = ctx.closed_data_min(self.config.main_timeframe, MIN_ROWS_SIGNAL)?;
        let highs: Vec<f64> = closed.iter().map(|c| c.high).collect();
        let lows: Vec<f64> = closed.iter().map(|c| c.low).collect();
        let closes: Vec<f64> = closed.iter().map(|c| c.close).collect();
        let atr = talib::atr(&highs, &lows, &closes, self.config.atr_period).ok()?;
        atr.last().copied()
    }
}

impl Strategy for ExampleMtfTrend {
    fn strategy_type(&self) -> &str {
        "example_mtf_trend"
    }

    fn timeframes(&self) -> Vec<Timeframe> {
        vec![Timeframe::M1, self.config.main_timeframe, Timeframe::D1]
    }

    /// 入场：1d 趋势过滤 + 4h 均线交叉触发 + ATR 初始止损
    fn on_candle(&mut self, ctx: &Ctx<'_>) -> Vec<Action> {
        if ctx.in_stop_loss_cooldown() {
            return vec![];
        }
        let (Some(trend_up), Some(cross_up), Some(atr)) = (
            self.trend_allows_long(ctx),
            self.cross_signal(ctx),
            self.atr(ctx),
        ) else {
            return vec![];
        };
        let side = if cross_up && trend_up {
            Side::Long
        } else if !cross_up && !trend_up {
            Side::Short
        } else {
            return vec![]; // 趋势与信号冲突或无交叉事件：保持空仓
        };
        let price = ctx.current_candle.close;
        let dist = self.config.atr_stop_mult * atr;
        let stop_price = match side {
            Side::Long => price - dist,
            Side::Short => price + dist,
        };
        vec![Action::Open {
            side,
            price,
            stop_price,
            atr,
            strength: 1.0,
            reason: format!(
                "MTF: 1d趋势{} + 4h MA{}/{} {}",
                if trend_up { "上" } else { "下" },
                self.config.fast_period,
                self.config.slow_period,
                if cross_up { "金叉" } else { "死叉" }
            ),
        }]
    }

    /// 出场：硬止损 或 4h 反向交叉
    fn on_realtime_tick(&mut self, ctx: &Ctx<'_>) -> Vec<Action> {
        let Some(pos) = ctx.position else {
            return vec![];
        };
        let price = ctx.current_candle.close;

        // ① 硬止损
        let hit = match pos.side {
            Side::Long => price <= pos.stop_price,
            Side::Short => price >= pos.stop_price,
        };
        if hit {
            return vec![Action::Close {
                price,
                reason: "ATR 止损触发".into(),
                is_stop_loss: true,
                strength: 1.0,
            }];
        }

        // ② 4h 反向交叉事件 → 趋势离场
        if let Some(cross_up) = self.cross_signal(ctx) {
            let against = match pos.side {
                Side::Long => !cross_up,
                Side::Short => cross_up,
            };
            if against {
                return vec![Action::Close {
                    price,
                    reason: "4h 均线反向交叉".into(),
                    is_stop_loss: false,
                    strength: 1.0,
                }];
            }
        }
        vec![]
    }
}

// ─── 注册表接线 ─────────────────────────────────────────────────
crate::register_strategy! {
    name: "example_mtf_trend",
    factory: |args| {
        let main_timeframe = Timeframe::parse(args.timeframe)?;
        let params: Params = crate::registry::params_or_default(args.params)?;
        Ok(Box::new(ExampleMtfTrend {
            config: Config {
                fast_period: params.fast_period,
                slow_period: params.slow_period,
                trend_ema_period: params.trend_ema_period,
                atr_period: params.atr_period,
                atr_stop_mult: params.atr_stop_mult,
                main_timeframe,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use strategy_core::candle::{Candle, Timeframe};
    use strategy_core::signal::{Action, Side};
    use strategy_core::strategy::{ClosedSource, Ctx, PositionView, Strategy};

    use super::*;

    /// 多周期已闭合数据源：分别喂 1d 与 4h 序列
    struct FakeMtfSource {
        d1_closes: Vec<f64>,
        h4_closes: Vec<f64>,
    }

    fn make_candle(close: f64) -> Candle {
        Candle {
            open_time: chrono::Utc::now(),
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

    fn make_candles(closes: &[f64]) -> Vec<Candle> {
        closes.iter().map(|&c| make_candle(c)).collect()
    }

    impl ClosedSource for FakeMtfSource {
        fn closed(&self, tf: Timeframe) -> Option<&[Candle]> {
            // Box::leak 是测试专用：让 &'a [Candle] 活得够久
            match tf {
                Timeframe::D1 => Some(Box::leak(make_candles(&self.d1_closes).into_boxed_slice())),
                Timeframe::H4 => Some(Box::leak(make_candles(&self.h4_closes).into_boxed_slice())),
                _ => None,
            }
        }
        fn last_price(&self) -> Option<f64> {
            self.h4_closes.last().copied()
        }
    }

    fn strat() -> ExampleMtfTrend {
        ExampleMtfTrend {
            config: Config {
                fast_period: 10,
                slow_period: 30,
                trend_ema_period: 50,
                atr_period: 14,
                atr_stop_mult: 2.0,
                main_timeframe: Timeframe::H4,
            },
        }
    }

    /// 1d 上升趋势 + 4h 末根上穿 → 开多
    #[test]
    fn uptrend_and_golden_cross_opens_long() {
        let d1: Vec<f64> = (0..80).map(|i| 100.0 + i as f64).collect(); // 趋势向上
        let mut h4: Vec<f64> = vec![100.0; 80]; // 4h 走平（无交叉）
        h4.push(120.0); // 末根跳升 → 上穿
        let src = FakeMtfSource { d1_closes: d1, h4_closes: h4 };
        let ctx = Ctx::new(&src, 120.0, make_candle(120.0), chrono::Utc::now(), None, None);
        let actions = strat().on_candle(&ctx);
        assert_eq!(actions.len(), 1, "4h 上穿事件应开多，实际: {actions:?}");
        match &actions[0] {
            Action::Open { side, stop_price, price, .. } => {
                assert_eq!(*side, Side::Long);
                assert!(*stop_price < *price, "多头止损应在入场价下方");
            }
            a => panic!("应开多，实际: {a:?}"),
        }
    }

    /// 1d 下降趋势 + 4h 末根下穿 → 开空
    #[test]
    fn downtrend_and_death_cross_opens_short() {
        let d1: Vec<f64> = (0..80).map(|i| 200.0 - i as f64).collect(); // 趋势向下
        let mut h4: Vec<f64> = vec![100.0; 80]; // 4h 走平
        h4.push(80.0); // 末根跳降 → 下穿
        let src = FakeMtfSource { d1_closes: d1, h4_closes: h4 };
        let ctx = Ctx::new(&src, 80.0, make_candle(80.0), chrono::Utc::now(), None, None);
        let actions = strat().on_candle(&ctx);
        assert_eq!(actions.len(), 1, "4h 下穿事件应开空，实际: {actions:?}");
        match &actions[0] {
            Action::Open { side, stop_price, price, .. } => {
                assert_eq!(*side, Side::Short);
                assert!(*stop_price > *price, "空头止损应在入场价上方");
            }
            a => panic!("应开空，实际: {a:?}"),
        }
    }

    /// 1d 下降（不允许做多）但 4h 上升（金叉）→ 不开仓
    #[test]
    fn trend_conflict_stays_flat() {
        let d1: Vec<f64> = (0..80).map(|i| 200.0 - i as f64).collect();
        let h4: Vec<f64> = (0..80).map(|i| 100.0 + i as f64).collect();
        let src = FakeMtfSource { d1_closes: d1, h4_closes: h4 };
        let ctx = Ctx::new(&src, 179.0, make_candle(179.0), chrono::Utc::now(), None, None);
        assert!(strat().on_candle(&ctx).is_empty(), "趋势与信号冲突应保持空仓");
    }

    /// 任一周期数据不足 → 静默不入场（不能 panic）
    #[test]
    fn insufficient_data_is_silent() {
        let d1: Vec<f64> = (0..10).map(|i| 100.0 + i as f64).collect();
        let h4: Vec<f64> = (0..80).map(|i| 100.0 + i as f64).collect();
        let src = FakeMtfSource { d1_closes: d1, h4_closes: h4 };
        let ctx = Ctx::new(&src, 109.0, make_candle(109.0), chrono::Utc::now(), None, None);
        assert!(strat().on_candle(&ctx).is_empty());
    }

    /// 持仓中 4h 反向交叉事件 → 平仓（非止损）
    /// 夹具：145 在止损 140 上方（避免硬止损先触发）；4h 末根跳降产生下穿事件
    #[test]
    fn reverse_cross_exits_position() {
        let d1: Vec<f64> = (0..80).map(|i| 100.0 + i as f64).collect();
        let mut h4: Vec<f64> = vec![100.0; 80];
        h4.push(80.0); // 末根跳降 → 下穿（多头持仓的反向信号）
        let src = FakeMtfSource { d1_closes: d1, h4_closes: h4 };
        let pos = PositionView {
            side: Side::Long,
            entry_price: 150.0,
            stop_price: 140.0,
            peak_price: 160.0,
            max_pnl_pct: 6.0,
            min_pnl_pct: -1.0,
            exit_active: false,
            exit_price: 0.0,
        };
        let ctx = Ctx::new(&src, 145.0, make_candle(145.0), chrono::Utc::now(), Some(pos), None);
        let actions = strat().on_realtime_tick(&ctx);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Close { is_stop_loss, .. } => assert!(!is_stop_loss, "反转离场不是止损"),
            a => panic!("应平仓，实际: {a:?}"),
        }
    }

    /// H4 数据不足（D1 足够）→ 静默不入场
    #[test]
    fn h4_insufficient_data_is_silent() {
        let d1: Vec<f64> = (0..80).map(|i| 100.0 + i as f64).collect(); // 足够
        let h4: Vec<f64> = (0..10).map(|i| 100.0 + i as f64).collect(); // 不足 40
        let src = FakeMtfSource { d1_closes: d1, h4_closes: h4 };
        let ctx = Ctx::new(&src, 109.0, make_candle(109.0), chrono::Utc::now(), None, None);
        assert!(strat().on_candle(&ctx).is_empty(), "H4 数据不足应保持沉默");
    }

    /// 硬止损出场：多头持仓价格跌破 stop_price → is_stop_loss=true
    #[test]
    fn hard_stop_exits_with_stop_loss_flag() {
        let d1: Vec<f64> = (0..80).map(|i| 100.0 + i as f64).collect();
        let h4: Vec<f64> = (0..80).map(|i| 100.0 + i as f64).collect();
        let src = FakeMtfSource { d1_closes: d1, h4_closes: h4 };
        let pos = PositionView {
            side: Side::Long,
            entry_price: 150.0,
            stop_price: 140.0,
            peak_price: 160.0,
            max_pnl_pct: 6.0,
            min_pnl_pct: -2.0,
            exit_active: false,
            exit_price: 0.0,
        };
        let ctx = Ctx::new(&src, 135.0, make_candle(135.0), chrono::Utc::now(), Some(pos), None);
        let actions = strat().on_realtime_tick(&ctx);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Close { is_stop_loss, reason, .. } => {
                assert!(*is_stop_loss, "硬止损应标记 is_stop_loss=true");
                assert!(reason.contains("止损"), "离场原因应含止损: {reason}");
            }
            a => panic!("应止损平仓，实际: {a:?}"),
        }
    }
}
