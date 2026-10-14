//! example_dual_ma — 双均线交叉入门教学策略（脚手架起点）
//!
//! **从这里开始写你的第一个策略**：本文件是完整可跑的最小示例，
//! 复制为 `src/my_strategy.rs` 改名字和逻辑即可，不需要修改任何其他文件。
//!
//! 阅读顺序：`docs/strategy/QUICKSTART.md` → 本文件 → `docs/strategy/DEVELOPMENT_GUIDE.md`
//!
//! ## 策略逻辑（教科书式，无盈利预期）
//! - 入场（`on_candle`，空仓时每根已闭合 1m 触发一次）：
//!   主周期（如 4h）上，快线上穿慢线 → 开多；下穿 → 开空。
//!   初始止损 = 入场价 ± `stop_loss_pct`%。
//! - 出场（`on_realtime_tick`，持仓时每根 1m/tick 触发一次）：
//!   止损触发 → 平仓；均线反向交叉 → 趋势离场。
//!
//! ## 本文件展示的 5 件事
//! 1. `Params` 私有参数：`deny_unknown_fields` 让写错字段名在启动时就报错
//! 2. `register_strategy!` 自注册：放文件尾即可，`build.rs` 自动发现
//! 3. `Strategy` trait 四方法：`strategy_type` / `timeframes` / `on_candle` / `on_realtime_tick`
//! 4. 只读已闭合数据：`ctx.closed_data_min(tf, n)`——未闭合 K 线在类型层不可见
//! 5. 测试样板：`FakeSource: ClosedSource` 喂合成已闭合 K 线
//!
//! ## 红线（框架在类型层面强制，违反即无法编译或启动）
//! - 只能用 [`Ctx::closed_data`] 拿已闭合 K 线——杜绝未来函数
//! - 信号时间用 `ctx.event_time`（K 线时间）——`Ctx` 没有 `now()`
//! - `timeframes()` 必须含 [`Timeframe::M1`]——聚合器据此注册
//! - 指标全量重算（输入是完整已闭合序列），不要在策略里存序列增量

use serde::Deserialize;

use strategy_core::candle::Timeframe;
use strategy_core::signal::{Action, Side};
use strategy_core::strategy::{Ctx, Strategy};
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
    fn cross_signal(&self, ctx: &Ctx<'_>) -> Option<bool> {
        let closed = ctx.closed_data_min(self.config.main_timeframe, MIN_ROWS)?;
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
        None // 无交叉
    }

    /// 止损价是否被当前 1m 触碰（按 exit_price_source 判定）
    fn stop_hit(&self, ctx: &Ctx<'_>, side: Side, stop_price: f64) -> bool {
        let c = &ctx.current_candle;
        match (side, self.config.exit_price_source) {
            (Side::Long, ExitPriceSource::Close) => c.close <= stop_price,
            (Side::Long, ExitPriceSource::HighLow) => c.low <= stop_price,
            (Side::Short, ExitPriceSource::Close) => c.close >= stop_price,
            (Side::Short, ExitPriceSource::HighLow) => c.high >= stop_price,
        }
    }
}

impl Strategy for ExampleDualMa {
    fn strategy_type(&self) -> &str {
        "example_dual_ma"
    }

    fn timeframes(&self) -> Vec<Timeframe> {
        vec![Timeframe::M1, self.config.main_timeframe]
    }

    /// 入场：空仓时每根已闭合 1m 调用。
    /// **交叉事件判定**：仅当最新一根出现快慢线交叉时才开仓。
    fn on_candle(&mut self, ctx: &Ctx<'_>) -> Vec<Action> {
        if ctx.in_stop_loss_cooldown() {
            return vec![];
        }
        let Some(cross_up) = self.cross_signal(ctx) else {
            return vec![]; // 无交叉事件或数据不足，保持沉默
        };
        let side = if cross_up { Side::Long } else { Side::Short };
        let price = ctx.current_candle.close;
        let stop_price = match side {
            Side::Long => price * (1.0 - self.config.stop_loss_pct / 100.0),
            Side::Short => price * (1.0 + self.config.stop_loss_pct / 100.0),
        };
        vec![Action::Open {
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
        }]
    }

    /// 出场：持仓时每根 1m/tick 调用。返回空 = 继续持有。
    fn on_realtime_tick(&mut self, ctx: &Ctx<'_>) -> Vec<Action> {
        let Some(pos) = ctx.position else {
            return vec![];
        };
        let price = ctx.current_candle.close;

        // ① 硬止损（pos.stop_price 是引擎持久化的最新止损价）
        if self.stop_hit(ctx, pos.side, pos.stop_price) {
            return vec![Action::Close {
                price,
                reason: "止损触发".into(),
                is_stop_loss: true,
                strength: 1.0,
            }];
        }

        // ② 均线反向交叉事件 → 趋势离场
        if let Some(cross_up) = self.cross_signal(ctx) {
            let against = match pos.side {
                Side::Long => !cross_up,  // 多头持仓遇到下穿 → 离场
                Side::Short => cross_up,  // 空头持仓遇到上穿 → 离场
            };
            if against {
                return vec![Action::Close {
                    price,
                    reason: "均线反向交叉".into(),
                    is_stop_loss: false,
                    strength: 1.0,
                }];
            }
        }

        vec![] // 继续持有
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
    use strategy_core::candle::Candle;
    use strategy_core::strategy::{ClosedSource, PositionView};

    /// 测试用已闭合数据源：按 close 序列合成主周期 K 线
    struct FakeSource {
        closes: Vec<f64>,
    }
    impl ClosedSource for FakeSource {
        fn closed(&self, tf: Timeframe) -> Option<&[Candle]> {
            if tf != Timeframe::H4 {
                return None;
            }
            Some(Box::leak(
                self.closes
                    .iter()
                    .map(|&c| Candle {
                        open_time: chrono::Utc::now(),
                        open: c,
                        high: c,
                        low: c,
                        close: c,
                        volume: 0.0,
                        quote_volume: 0.0,
                        trade_num: 0,
                        active_buy_volume: 0.0,
                        active_buy_quote_volume: 0.0,
                        is_closed: true,
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ))
        }
        fn last_price(&self) -> Option<f64> {
            self.closes.last().copied()
        }
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

    #[test]
    fn uptrend_opens_long_with_stop_below() {
        // 100 根走平（快线≈慢线，无交叉）+ 末根跳升 → 快线上穿慢线
        let mut closes: Vec<f64> = vec![100.0; 100];
        closes.push(120.0);
        let src = FakeSource { closes };
        let ctx = Ctx::new(&src, 120.0, make_candle(120.0), chrono::Utc::now(), None, None);
        let actions = strat().on_candle(&ctx);
        assert_eq!(actions.len(), 1, "末端上穿事件应开多，实际: {actions:?}");
        match &actions[0] {
            Action::Open {
                side, stop_price, price, ..
            } => {
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
        let src = FakeSource { closes };
        let ctx = Ctx::new(&src, 119.0, make_candle(119.0), chrono::Utc::now(), None, None);
        assert!(strat().on_candle(&ctx).is_empty(), "数据不足应保持沉默");
    }

    #[test]
    fn stop_hit_closes_position() {
        let closes: Vec<f64> = (0..100).map(|i| 100.0 + i as f64).collect();
        let src = FakeSource { closes };
        // 多头持仓，stop_price=98，当前 1m close=95 → 触发止损平仓
        let pos = PositionView {
            side: Side::Long,
            entry_price: 100.0,
            stop_price: 98.0,
            peak_price: 105.0,
            max_pnl_pct: 5.0,
            min_pnl_pct: -2.0,
            exit_active: false,
            exit_price: 0.0,
        };
        let ctx = Ctx::new(&src, 95.0, make_candle(95.0), chrono::Utc::now(), Some(pos), None);
        let actions = strat().on_realtime_tick(&ctx);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Close { is_stop_loss, .. } => assert!(*is_stop_loss),
            a => panic!("应止损平仓，实际: {a:?}"),
        }
    }

    /// 教学样板：FakeSource 只喂已闭合 K 线（is_closed=true）。
    /// 策略侧只能经 ctx.closed_data*() 看到它们——未来函数在类型层被隔离。
    #[test]
    fn fake_source_exposes_only_closed_candles() {
        let closes: Vec<f64> = (0..100).map(|i| 100.0 + i as f64).collect();
        let src = FakeSource { closes };
        let ctx = Ctx::new(&src, 199.0, make_candle(199.0), chrono::Utc::now(), None, None);
        let closed = ctx.closed_data(Timeframe::H4).expect("应有已闭合数据");
        assert!(closed.iter().all(|c| c.is_closed), "ClosedSource 只应暴露已闭合 K 线");
        assert!(ctx.closed_data_min(Timeframe::H4, 200).is_none(), "根数不足应返回 None");
    }

    /// 交叉事件判定：持续上升序列（快线长期在慢线上方）末端不应再开仓——
    /// 仅当最新一根出现"上穿"事件时才入场。当前实现若只看状态（快>慢→开多）
    /// 此测试会失败，暴露"文档说交叉事件、代码是状态判定"的缺陷。
    #[test]
    fn no_cross_at_end_stays_flat() {
        // 100 根持续上升：快线长期在慢线上方（状态=多头），但末端无新交叉
        let closes: Vec<f64> = (0..100).map(|i| 100.0 + i as f64).collect();
        let src = FakeSource { closes };
        let ctx = Ctx::new(&src, 199.0, make_candle(199.0), chrono::Utc::now(), None, None);
        assert!(
            strat().on_candle(&ctx).is_empty(),
            "末端无交叉事件时不应开仓（仅状态在慢线上方不够）"
        );
    }

    /// 空头入场：100 根走平 + 末根跳降 → 下穿事件 → 开空，止损在入场价上方
    #[test]
    fn downtrend_opens_short_with_stop_above() {
        let mut closes: Vec<f64> = vec![100.0; 100];
        closes.push(80.0); // 末根跳降 → 下穿
        let src = FakeSource { closes };
        let ctx = Ctx::new(&src, 80.0, make_candle(80.0), chrono::Utc::now(), None, None);
        let actions = strat().on_candle(&ctx);
        assert_eq!(actions.len(), 1, "末端下穿事件应开空，实际: {actions:?}");
        match &actions[0] {
            Action::Open { side, stop_price, price, .. } => {
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
        let src = FakeSource { closes };
        let pos = PositionView {
            side: Side::Long,
            entry_price: 100.0,
            stop_price: 95.0,
            peak_price: 105.0,
            max_pnl_pct: 5.0,
            min_pnl_pct: -1.0,
            exit_active: false,
            exit_price: 0.0,
        };
        let ctx = Ctx::new(&src, 98.0, make_candle(98.0), chrono::Utc::now(), Some(pos), None);
        let actions = strat().on_realtime_tick(&ctx);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Close { is_stop_loss, reason, .. } => {
                assert!(!is_stop_loss, "反转离场不是止损");
                assert!(reason.contains("交叉"), "离场原因应是交叉反转: {reason}");
            }
            a => panic!("应平仓，实际: {a:?}"),
        }
    }
}
