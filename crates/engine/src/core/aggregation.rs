//! K 线聚合与「大周期桶是否完整」的判定。
//!
//! 退役背景（原痛点，实现已随 D1b 删除）：旧数据层的 1m 分发器用二值枚举在
//! `merge` / 时段合并 / 日线合并三条分支间硬分派；旧窗口表写死在源码且只支持
//! 60/120/240 分钟；日线恒走境内期货交易日切分，crypto 的日线语义被污染，
//! 与 `aggregate.rs` 的 UTC 午夜口径冲突。
//!
//! 本模块把「桶边界」和「桶闭合条件」变成策略对象：
//! - [`UtcGridPolicy`]：7x24 UTC epoch 网格（crypto）；
//! - [`SessionWindowPolicy`]：按交易日/交易时段切桶，午休强制断桶（期货）；
//! - 新增交易所只需实现 [`BarAggregationPolicy`]，无需改 `Feed`。

use std::collections::{BTreeMap, VecDeque};

use chrono::{DateTime, Duration, TimeZone, Utc};

use crate::core::bar::{require_final, Bar, BarFinality, Timeframe};
use crate::core::error::Result;
use crate::core::session::{SessionGap, SessionRules, TradingDay};

/// 一个聚合桶的身份。桶不能只用「起始时间戳」表达：期货 21:00 与次日
/// 9:00 可能属于同一交易日但不同窗口，D1 桶的起点是当日首个窗口开盘。
#[derive(Debug, Clone, PartialEq)]
pub struct BucketId {
    pub tf: Timeframe,
    pub start: DateTime<Utc>,
    pub trading_day: TradingDay,
    /// 该桶所属交易时段的 [start, end)；UTC 网格市场为 None。
    pub window_end: Option<DateTime<Utc>>,
}

/// 聚合策略。所有方法都必须无状态或有只读状态；可变状态统一放在
/// [`AggregationState`]，这样同一个 `Arc<dyn BarAggregationPolicy>`
/// 可被多个品种/多个 Actor 共享。
pub trait BarAggregationPolicy: Send + Sync {
    fn supports(&self, tf: Timeframe) -> bool;

    /// 这根 1m 属于哪个桶。返回 None 表示该时刻不属于任何桶（休市/无效帧）。
    fn bucket_of(
        &self,
        tf: Timeframe,
        bar_time: DateTime<Utc>,
        session: &dyn SessionRules,
    ) -> Option<BucketId>;

    /// 这根 1m 是否恰好收满当前桶。
    fn is_last_bar_of(
        &self,
        bucket: &BucketId,
        bar_time: DateTime<Utc>,
        session: &dyn SessionRules,
    ) -> bool;

    /// warmup 起点落在桶中间时是否丢弃首桶（对齐现有 `drop_partial_head`）。
    fn drop_partial_head(&self) -> bool {
        true
    }

    /// 会话断开是否强制闭合当前桶。期货午休必须 true；crypto 为 false。
    fn closes_on_gap(&self, _gap: &SessionGap) -> bool {
        false
    }

    /// 交易日切换是否强制闭合 D1 桶。期货 true；crypto false（按 1440 网格）。
    fn closes_on_day_boundary(&self) -> bool {
        false
    }
}

// ─── 策略 A：UTC epoch 网格 ───────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct UtcGridPolicy;

impl BarAggregationPolicy for UtcGridPolicy {
    fn supports(&self, _tf: Timeframe) -> bool {
        true
    }

    fn bucket_of(
        &self,
        tf: Timeframe,
        bar_time: DateTime<Utc>,
        session: &dyn SessionRules,
    ) -> Option<BucketId> {
        let size = tf.0 as i64 * 60;
        let secs = bar_time.timestamp();
        let start = Utc
            .timestamp_opt(secs - secs.rem_euclid(size), 0)
            .single()?;
        Some(BucketId {
            tf,
            start,
            trading_day: session.trading_day_of(bar_time),
            window_end: None,
        })
    }

    fn is_last_bar_of(
        &self,
        bucket: &BucketId,
        bar_time: DateTime<Utc>,
        _session: &dyn SessionRules,
    ) -> bool {
        let bar_end = bar_time + Duration::minutes(1);
        bar_end == bucket.start + Duration::minutes(bucket.tf.0 as i64)
    }
}

// ─── 策略 B：交易时段窗口 ─────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
pub struct SessionWindowPolicy;

impl BarAggregationPolicy for SessionWindowPolicy {
    fn supports(&self, _tf: Timeframe) -> bool {
        true
    }

    fn bucket_of(
        &self,
        tf: Timeframe,
        bar_time: DateTime<Utc>,
        session: &dyn SessionRules,
    ) -> Option<BucketId> {
        let day = session.trading_day_of(bar_time);
        let windows = session.sessions_on(day);
        let window = windows.iter().find(|w| w.contains(bar_time))?;

        // D1：整条交易日合成一根，起点是当日首个窗口开盘。
        if tf == Timeframe::D1 {
            let first = windows.first()?;
            let last = windows.last()?;
            return Some(BucketId {
                tf,
                start: first.start,
                trading_day: day,
                window_end: Some(last.end),
            });
        }

        // 其余周期：在所属时段窗口内按 tf 分钟切分，绝不跨越窗口边界。
        let offset_min = (bar_time - window.start).num_minutes();
        let idx = offset_min.div_euclid(tf.0 as i64);
        let start = window.start + Duration::minutes(idx * tf.0 as i64);
        let end = (start + Duration::minutes(tf.0 as i64)).min(window.end);
        Some(BucketId {
            tf,
            start,
            trading_day: day,
            window_end: Some(end),
        })
    }

    fn is_last_bar_of(
        &self,
        bucket: &BucketId,
        bar_time: DateTime<Utc>,
        session: &dyn SessionRules,
    ) -> bool {
        let bar_end = bar_time + Duration::minutes(1);
        if bucket.tf == Timeframe::D1 {
            let windows = session.sessions_on(bucket.trading_day);
            return windows.last().is_some_and(|w| w.end == bar_end);
        }
        if let Some(we) = bucket.window_end {
            if bar_end == we {
                return true;
            }
        }
        bar_end == bucket.start + Duration::minutes(bucket.tf.0 as i64)
    }

    fn closes_on_gap(&self, gap: &SessionGap) -> bool {
        gap.closes_bar
    }

    fn closes_on_day_boundary(&self) -> bool {
        true
    }
}

// ─── 有状态聚合器（实盘 / 回测共用） ───────────────────────────────────────

#[derive(Debug, Clone)]
struct PartialBucket {
    id: BucketId,
    bar: Bar,
    /// 首桶从中间开始（warmup 或订阅中途接入），闭合时丢弃
    tainted: bool,
}

/// 多周期增量聚合状态。等价于现有 `Feed`，但不含任何市场判断——
/// 全部委托给 [`BarAggregationPolicy`] 与 [`SessionRules`]。
pub struct AggregationState {
    timeframes: Vec<Timeframe>,
    max_len: usize,
    partials: BTreeMap<Timeframe, PartialBucket>,
    closed: BTreeMap<Timeframe, VecDeque<Bar>>,
    watermark: Option<DateTime<Utc>>,
    initialized: bool,
}

impl AggregationState {
    pub fn new(timeframes: &[Timeframe], max_len: usize) -> Self {
        let mut tfs = timeframes.to_vec();
        if !tfs.contains(&Timeframe::M1) {
            tfs.push(Timeframe::M1);
        }
        tfs.sort();
        tfs.dedup();
        let closed = tfs.iter().map(|&tf| (tf, VecDeque::new())).collect();
        Self {
            timeframes: tfs,
            max_len,
            partials: BTreeMap::new(),
            closed,
            watermark: None,
            initialized: false,
        }
    }

    pub fn closed(&self, tf: Timeframe) -> &VecDeque<Bar> {
        self.closed.get(&tf).expect("timeframe registered")
    }

    /// 已注册的全部时间框架（升序，恒含 1m）。阶段 3 桥接适配器
    /// （`bridge::CandleSeries`）据此增量刷新 Candle 缓存。
    pub fn timeframes(&self) -> &[Timeframe] {
        &self.timeframes
    }

    /// 喂入一根**已闭合** 1m，返回本次新闭合的时间框架（升序，恒含 1m）。
    pub fn on_closed_1m(
        &mut self,
        bar: Bar,
        policy: &dyn BarAggregationPolicy,
        session: &dyn SessionRules,
    ) -> Result<Vec<Timeframe>> {
        require_final(&bar)?;
        if let Some(wm) = self.watermark {
            if bar.open_time <= wm {
                return Ok(Vec::new()); // 幂等：preload / 重连补缺重复帧
            }
        }
        self.watermark = Some(bar.open_time);

        let mut newly = Vec::new();
        for tf in self.timeframes.clone() {
            if tf == Timeframe::M1 {
                self.push_closed(tf, bar.clone());
                newly.push(tf);
                continue;
            }
            if !policy.supports(tf) {
                continue;
            }
            let Some(id) = policy.bucket_of(tf, bar.open_time, session) else {
                continue;
            };

            let switched = self
                .partials
                .get(&tf)
                .is_some_and(|p| p.id.start != id.start);
            if switched {
                // 惰性闭合：新窗口首根到达时闭合上一窗口（期货缺 bar / 休市
                // 导致上一根不是窗口末根，仍能正确收线）。
                self.finalize(tf, policy, &mut newly);
            }

            match self.partials.get_mut(&tf) {
                Some(p) => merge_into(&mut p.bar, &bar),
                None => {
                    let mut first = bar.clone();
                    first.open_time = id.start;
                    first.finality = BarFinality::Final;
                    // v2.3 C8：首桶只有「从桶中间接入」才标污——warmup 恰落在
                    // 桶起点时首桶是完整的，不该丢（对齐旧 Feed 的 drop_partial_head 语义）。
                    let tainted = !self.initialized
                        && policy.drop_partial_head()
                        && bar.open_time != id.start;
                    self.partials.insert(
                        tf,
                        PartialBucket {
                            id,
                            bar: first,
                            tainted,
                        },
                    );
                }
            }

            let last = policy.is_last_bar_of(
                &self.partials.get(&tf).expect("partial exists").id,
                bar.open_time,
                session,
            );
            if last {
                self.finalize(tf, policy, &mut newly);
            }
        }
        self.initialized = true;
        newly.sort();
        newly.dedup();
        Ok(newly)
    }

    /// 会话断开：期货午休 / 夜盘结束时强制闭合当前桶，
    /// 避免把 10:15-13:30 的空档「跳过」后拼成一根畸形小时线。
    pub fn on_gap(
        &mut self,
        gap: &SessionGap,
        policy: &dyn BarAggregationPolicy,
    ) -> Vec<Timeframe> {
        if !policy.closes_on_gap(gap) {
            return Vec::new();
        }
        let mut newly = Vec::new();
        for tf in self.timeframes.clone() {
            if tf != Timeframe::M1 && self.partials.contains_key(&tf) {
                self.finalize(tf, policy, &mut newly);
            }
        }
        newly.sort();
        newly
    }

    /// 交易日切换：先驱动 D1 闭合，再由调用方执行今仓 → 昨仓 rollover。
    pub fn on_day_boundary(&mut self, policy: &dyn BarAggregationPolicy) -> Vec<Timeframe> {
        if !policy.closes_on_day_boundary() {
            return Vec::new();
        }
        let mut newly = Vec::new();
        for tf in self.timeframes.clone() {
            if tf == Timeframe::D1 && self.partials.contains_key(&tf) {
                self.finalize(tf, policy, &mut newly);
            }
        }
        newly
    }

    fn finalize(
        &mut self,
        tf: Timeframe,
        policy: &dyn BarAggregationPolicy,
        newly: &mut Vec<Timeframe>,
    ) {
        let Some(p) = self.partials.remove(&tf) else {
            return;
        };
        let tainted = p.tainted && policy.drop_partial_head();
        if tainted {
            return;
        }
        self.push_closed(tf, p.bar);
        newly.push(tf);
    }

    fn push_closed(&mut self, tf: Timeframe, bar: Bar) {
        let q = self.closed.entry(tf).or_default();
        q.push_back(bar);
        while q.len() > self.max_len.max(1) {
            q.pop_front();
        }
    }
}

fn merge_into(dst: &mut Bar, src: &Bar) {
    dst.high = dst.high.max(src.high);
    dst.low = dst.low.min(src.low);
    dst.close = src.close;
    dst.volume += src.volume;
    dst.open_time = dst.open_time.min(src.open_time);
    if let (Some(a), Some(b)) = (
        dst.extensions.open_interest.as_mut(),
        src.extensions.open_interest,
    ) {
        *a = b; // OI 是快照而非累加量，取桶内最后一根
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::bar::BarExtensions;
    use crate::core::identity::{ExchangeId, InstrumentId};
    use crate::core::session::{cn_futures_default, ContinuousSession};
    use chrono::{DateTime, NaiveDate, TimeZone, Utc};

    fn bar(inst: &InstrumentId, t: DateTime<Utc>, close: f64) -> Bar {
        Bar {
            instrument: inst.clone(),
            open_time: t,
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
            finality: BarFinality::Final,
            extensions: BarExtensions::default(),
        }
    }

    fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
    }
    /// 承接 旧时段表案 `h1_day_windows_across_break`（日盘跨午休分桶）+
    /// 旧时段表案 `window_end_cases`（`BucketId.window_end` 边界）：
    /// 午休两侧不得同桶 = 日盘「不跨休市」命题；`window_end` 断言即旧
    /// `window_end()` 的等价口径。注：旧表日盘 H1 锚「窗口段」（10:15/10:30 归
    /// 同一段），engine 锚「窗口内分钟网格」，日盘锚点差异为差异表 #3 的设计后果，
    /// 由 `spi_dual_run_parity` 的 golden 对继续钉住。
    #[test]
    fn session_window_never_spans_lunch_break() {
        let session = cn_futures_default();
        let policy = SessionWindowPolicy;
        let before = policy
            .bucket_of(Timeframe::H1, utc(2026, 1, 5, 2, 10), &session) // 北京 10:10
            .unwrap();
        let after = policy
            .bucket_of(Timeframe::H1, utc(2026, 1, 5, 5, 35), &session) // 北京 13:35
            .unwrap();
        assert!(before.window_end.unwrap() <= utc(2026, 1, 5, 3, 30));
        assert!(after.start >= utc(2026, 1, 5, 5, 30));
        assert_ne!(before.start, after.start, "午休两侧不得落在同一桶");
    }
    /// C8：首桶只在「从桶中间接入」时丢弃；warmup 恰在桶起点 → 首桶保留。
    #[test]
    fn aligned_first_bucket_kept_mid_start_dropped() {
        let inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let policy = UtcGridPolicy;
        let session = ContinuousSession::default();
        // 对齐起点 00:00 喂满 2 小时 → 两根完整 H1 都在（修复前首根被丢）
        let mut st = AggregationState::new(&[Timeframe::M1, Timeframe::H1], 300);
        for i in 0..120 {
            let b = bar(
                &inst,
                utc(2026, 1, 5, 0, 0) + chrono::Duration::minutes(i),
                100.0,
            );
            st.on_closed_1m(b, &policy, &session).unwrap();
        }
        assert_eq!(
            st.closed(Timeframe::H1).len(),
            2,
            "C8：桶起点对齐的首桶必须保留"
        );
        // 中途接入 00:30 → 首个 00:00 桶缺头根，闭合时丢弃
        let mut st2 = AggregationState::new(&[Timeframe::M1, Timeframe::H1], 300);
        for i in 0..65 {
            let b = bar(
                &inst,
                utc(2026, 1, 5, 0, 30) + chrono::Duration::minutes(i),
                100.0,
            );
            st2.on_closed_1m(b, &policy, &session).unwrap();
        }
        assert!(
            st2.closed(Timeframe::H1).is_empty(),
            "中途接入首桶丢弃、次桶尚未收满"
        );
    }

    // ─── D1b/D2 承接用例（原宿主 `crates/data/src/feed.rs` 18 案 +
    //     旧硬编码时段表模块 6 案，共 24 案随实现同葬）
    //     每条注明承接的旧案名。行为口径按 engine 语义重写，不复用旧断言文本。
    //     时段表 6 案去向：
    //       h1_night_windows_anchored_at_21 → h1_night_buckets_anchor_from_21
    //       h2_windows                       → h2_buckets_anchor_within_windows
    //       h4_windows                       → h4_bucket_spans_night_and_truncates_at_break
    //       h1_day_windows_across_break      → session_window_never_spans_lunch_break（补标注）
    //       window_end_cases                 → 同上 + ctp_4h_and_8h 的 window_end 断言（补标注）
    //       is_ctp_supported_only_h1_h2_h4   → **退役**：engine `supports` 恒真，
    //                                          二值回落语义不存在，无实现可测
    //     差账登记：docs/exchange-spi-retirement.md §5.1
    //     另两条旧 Feed 专有能力同样登记为退役（非丢失）：
    //     `ctp_unsupported_tf_falls_back_to_fixed`（二值枚举回落）、
    //     `out_of_coverage_falls_back_to_heuristic`（日历覆盖外启发式，
    //     engine 改注入 `WindowedSession.holidays`）。

    /// 承接 `preload_populates_all_timeframes` + `realtime_close_fires_newly_closed`
    #[test]
    fn feed_populates_all_timeframes_and_reports_newly_closed() {
        let inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let policy = UtcGridPolicy;
        let session = ContinuousSession::default();
        let mut st = AggregationState::new(&[Timeframe::M1, Timeframe::H1, Timeframe::D1], 300);
        let mut newly_h1 = 0;
        for i in 0..61 {
            let closed_tfs = st
                .on_closed_1m(
                    bar(
                        &inst,
                        utc(2026, 1, 5, 0, 0) + chrono::Duration::minutes(i),
                        100.0,
                    ),
                    &policy,
                    &session,
                )
                .unwrap();
            // M1 每根即时收线
            assert!(
                closed_tfs.contains(&Timeframe::M1),
                "第 {i} 根 M1 应即时收线"
            );
            if closed_tfs.contains(&Timeframe::H1) {
                newly_h1 += 1;
            }
        }
        assert_eq!(newly_h1, 1, "61 根 1m 只在满一小时时收一根 H1");
        assert_eq!(st.closed(Timeframe::M1).len(), 61);
        assert_eq!(st.closed(Timeframe::H1).len(), 1);
        assert!(
            st.closed(Timeframe::D1).is_empty(),
            "不足一整日，D1 不得提前收线"
        );
    }

    /// 承接 `repeated_preload_same_window_is_idempotent` +
    /// `overlapping_preload_only_ingests_new_candles`
    #[test]
    fn watermark_rejects_repeated_and_overlapping_bars() {
        let inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let policy = UtcGridPolicy;
        let session = ContinuousSession::default();
        let mut st = AggregationState::new(&[Timeframe::M1], 300);
        for i in 0..10 {
            st.on_closed_1m(bar(&inst, utc(2026, 1, 5, 0, i), 100.0), &policy, &session)
                .unwrap();
        }
        assert_eq!(st.closed(Timeframe::M1).len(), 10);
        // 重喂已见过的一根：拒绝且序列不增长
        let again = st
            .on_closed_1m(bar(&inst, utc(2026, 1, 5, 0, 5), 999.0), &policy, &session)
            .unwrap();
        assert!(again.is_empty(), "重喂不得再收线");
        assert_eq!(st.closed(Timeframe::M1).len(), 10, "重喂不得增长");
        assert_eq!(
            st.closed(Timeframe::M1).back().unwrap().close,
            100.0,
            "重喂不得改写已有桶"
        );
    }

    /// 承接 `max_len_trim_keeps_complete_buckets`
    #[test]
    fn max_len_trim_keeps_only_complete_buckets() {
        let inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let policy = UtcGridPolicy;
        let session = ContinuousSession::default();
        let mut st = AggregationState::new(&[Timeframe::M1, Timeframe::H1], 2);
        for i in 0..300 {
            st.on_closed_1m(
                bar(
                    &inst,
                    utc(2026, 1, 5, 0, 0) + chrono::Duration::minutes(i),
                    100.0,
                ),
                &policy,
                &session,
            )
            .unwrap();
        }
        assert_eq!(st.closed(Timeframe::H1).len(), 2, "max_len 生效");
        // 保留的是最新两根（04:00-05:00 与 03:00-04:00），不是最旧
        assert_eq!(
            st.closed(Timeframe::H1).back().unwrap().open_time,
            utc(2026, 1, 5, 4, 0),
            "裁剪必须保新弃旧"
        );
    }

    /// 承接 `feed::eight_hour_buckets` + `feed::ctp_4h_two_bars_per_trading_day`
    /// （策略层口径）；同时承接 旧时段表案 `window_end_cases`——本案的
    /// `window_end` 断言即旧 `window_end()` 的等价物
    #[test]
    fn ctp_4h_and_8h_buckets_anchor_within_windows() {
        let session = cn_futures_default();
        let policy = SessionWindowPolicy;
        // 北京 09:30 = UTC 01:30（北京 = UTC+8）
        // 北京 = UTC+8：09:00→01:00，10:00→02:00，10:15→02:15，13:30→05:30
        // 4h：日盘首窗内一律锚 09:00，且绝不跨过窗口右端（午休 10:15）
        let b4 = policy
            .bucket_of(Timeframe(240), utc(2026, 1, 5, 1, 30), &session)
            .expect("北京 09:30 在日盘窗口内");
        assert_eq!(b4.start, utc(2026, 1, 5, 1, 0), "4h 桶锚日盘首窗开盘");
        assert_eq!(
            b4.window_end,
            Some(utc(2026, 1, 5, 2, 15)),
            "4h 桶不得跨过午休"
        );
        // 13:30 起是另一窗口 → 另起一桶
        let b4b = policy
            .bucket_of(Timeframe(240), utc(2026, 1, 5, 5, 35), &session)
            .expect("北京 13:35 在午后窗口内");
        assert_eq!(b4b.start, utc(2026, 1, 5, 5, 30), "午后窗口另起 4h 桶");
        // 8h：同样被窗口边界截断
        let b8 = policy
            .bucket_of(Timeframe(480), utc(2026, 1, 5, 2, 0), &session)
            .expect("北京 10:00 在日盘窗口内");
        assert_eq!(b8.start, utc(2026, 1, 5, 1, 0));
        assert_eq!(b8.window_end, Some(utc(2026, 1, 5, 2, 15)));
    }

    /// 承接 `d1_uses_ctp_trading_day_and_closes_at_day_session_close`（策略层口径）
    #[test]
    fn d1_spans_from_trading_day_night_open_to_day_close() {
        let session = cn_futures_default();
        let policy = SessionWindowPolicy;
        // 周一 09:30（北京）：所属交易日的 D1 桶起点 = 该交易日夜盘开盘（上周五 21:00）
        let d1 = policy
            .bucket_of(Timeframe::D1, utc(2026, 1, 5, 1, 30), &session)
            .expect("周一日盘在窗口内");
        assert_eq!(
            d1.start,
            utc(2026, 1, 2, 13, 0),
            "D1 起点=交易日夜盘开盘（上周五 21:00 北京）"
        );
        assert_eq!(
            d1.trading_day,
            TradingDay(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap())
        );
        assert_eq!(
            d1.window_end,
            Some(utc(2026, 1, 5, 7, 0)),
            "D1 终点=日盘末窗收盘（15:00 北京）"
        );
    }

    /// 承接 `d1_keeps_weekend_night_session_with_next_monday` +
    /// `d1_night_session_ending_23h_merges_into_next_trading_day`
    #[test]
    fn weekend_and_night_bars_roll_into_next_trading_day() {
        let session = cn_futures_default();
        let bj_date = |y, mo, d| NaiveDate::from_ymd_opt(y, mo, d).unwrap();
        let mon = TradingDay(bj_date(2026, 1, 5));
        // 周五 21:00（北京）夜盘 → 归周一交易日
        assert_eq!(
            session.trading_day_of(utc(2026, 1, 2, 13, 0)),
            mon,
            "周五夜盘归下一交易日"
        );
        // 周日 21:30 → 同样归周一（顺延跨周六日）
        assert_eq!(
            session.trading_day_of(utc(2026, 1, 4, 13, 30)),
            mon,
            "周日夜盘顺延到周一"
        );
        // 23:00（北京）收盘的短夜盘品种：仍归下一交易日（>= day_cutover 20:00）
        assert_eq!(
            session.trading_day_of(utc(2026, 1, 5, 15, 0)),
            TradingDay(bj_date(2026, 1, 6)),
            "23:00 北京属次日交易日"
        );
    }

    /// 承接 `holiday_d1_uses_calendar_not_wallclock`
    #[test]
    fn holiday_table_suppresses_windows_and_rolls_trading_day() {
        use std::collections::BTreeSet;
        let mut session = cn_futures_default();
        let mon = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        session.holidays = BTreeSet::from([mon]);
        assert!(!session.is_trading_day(mon), "节假日非交易日");
        assert!(
            session.sessions_on(TradingDay(mon)).is_empty(),
            "节假日不得产生任何窗口（含顺延而来的周五夜盘）"
        );
        // 周五夜盘原本归周一，周一休市 → 顺延到周二
        assert_eq!(
            session.trading_day_of(utc(2026, 1, 2, 13, 0)),
            TradingDay(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap()),
            "顺延必须由日历驱动，而非墙钟"
        );
        // 休市日内的时刻不属于任何桶
        assert!(
            SessionWindowPolicy
                .bucket_of(Timeframe::H1, utc(2026, 1, 5, 1, 30), &session)
                .is_none(),
            "休市日 bucket_of 必须为 None"
        );
    }

    /// 承接 `d1_tainted_partial_head_dropped` + `ctp_tainted_partial_head_dropped`
    #[test]
    fn d1_head_bucket_dropped_when_ingested_mid_trading_day() {
        let inst = InstrumentId::futures_contract(
            &ExchangeId::new("SHFE"),
            "CU",
            crate::core::identity::ContractMonth {
                year: 2026,
                month: 1,
            },
        );
        let session = cn_futures_default();
        let policy = SessionWindowPolicy;
        // 从周一午后中途接入（缺上周五夜盘 + 周一日盘首两段）→ D1 首桶残缺
        let mut st = AggregationState::new(&[Timeframe::M1, Timeframe::D1], 200);
        for i in 0..90 {
            st.on_closed_1m(
                bar(
                    &inst,
                    utc(2026, 1, 5, 5, 30) + chrono::Duration::minutes(i), // 北京 13:30 起
                    100.0,
                ),
                &policy,
                &session,
            )
            .unwrap();
        }
        assert_eq!(st.closed(Timeframe::M1).len(), 90, "1m 侧摄入正常");
        assert!(
            st.closed(Timeframe::D1).is_empty(),
            "缺头的 D1 首桶不得进入 closed（残缺桶不得当完整日线喂给策略）"
        );
    }

    /// 承接 旧时段表案 `h1_night_windows_anchored_at_21`。
    /// 与旧表**逐点等价**：夜盘窗口 21:00–01:00 内 H1 按整点切，跨 0 点归次日零点。
    /// 北京 = UTC+8：21:05→UTC 13:05，锚 21:00→UTC 13:00。
    #[test]
    fn h1_night_buckets_anchor_from_21() {
        let session = cn_futures_default();
        let policy = SessionWindowPolicy;
        let anchor = |h: u32, mi: u32| {
            policy
                .bucket_of(Timeframe::H1, utc(2026, 1, 7, h, mi), &session)
                .map(|b| b.start)
        };
        assert_eq!(
            anchor(13, 5),
            Some(utc(2026, 1, 7, 13, 0)),
            "北京 21:05 → 夜盘首根锚 21:00"
        );
        assert_eq!(
            anchor(14, 30),
            Some(utc(2026, 1, 7, 14, 0)),
            "北京 22:30 → 锚 22:00"
        );
        assert_eq!(
            anchor(15, 59),
            Some(utc(2026, 1, 7, 15, 0)),
            "北京 23:59 → 锚 23:00"
        );
        // 北京次日 00:30 = UTC 当日 16:30：跨零点仍属同一夜盘窗口的第 4 根
        assert_eq!(
            anchor(16, 30),
            Some(utc(2026, 1, 7, 16, 0)),
            "北京 00:30 → 锚当日 00:00（UTC 16:00），跨零点不另起窗口"
        );
        // 夜盘窗口右端 01:00（UTC 17:00）已在场外 → 不属于任何桶
        assert_eq!(anchor(17, 0), None, "北京 01:00 为夜盘收盘时刻，不得有桶");
    }

    /// 承接 旧时段表案 `h2_windows`。夜盘三点与旧表等价；日盘 11:29 一处
    /// **有意分叉**：旧 2h 日盘为 09:00–11:30（跨午休一根），engine 绝不跨窗口
    /// 边界 → 归 10:30–11:30。分叉为差异表 #3 同源的设计后果，不是回归。
    #[test]
    fn h2_buckets_anchor_within_windows() {
        let session = cn_futures_default();
        let policy = SessionWindowPolicy;
        let anchor = |d: u32, h: u32, mi: u32| {
            policy
                .bucket_of(Timeframe(120), utc(2026, 1, d, h, mi), &session)
                .map(|b| b.start)
        };
        // 夜盘（周三 21:00–周四 01:00，UTC 侧同属周四交易日）
        assert_eq!(
            anchor(7, 13, 30),
            Some(utc(2026, 1, 7, 13, 0)),
            "北京 21:30 → 2h 锚 21:00"
        );
        assert_eq!(
            anchor(7, 15, 30),
            Some(utc(2026, 1, 7, 15, 0)),
            "北京 23:30 → 2h 锚 23:00"
        );
        assert_eq!(
            anchor(7, 16, 59),
            Some(utc(2026, 1, 7, 15, 0)),
            "北京次日 00:59 → 仍归 23:00 那根（跨零点不另起 2h 桶）"
        );
        // 日盘
        assert_eq!(
            anchor(8, 1, 0),
            Some(utc(2026, 1, 8, 1, 0)),
            "北京 09:00 → 2h 锚 09:00"
        );
        assert_eq!(
            anchor(8, 5, 30),
            Some(utc(2026, 1, 8, 5, 30)),
            "北京 13:30 → 午后窗口另起 2h 桶"
        );
        assert_eq!(
            anchor(8, 6, 59),
            Some(utc(2026, 1, 8, 5, 30)),
            "北京 14:59 → 仍锚 13:30"
        );
        // 分叉点：旧表期望 09:00（跨午休），engine 期望 10:30（不跨窗）
        assert_eq!(
            anchor(8, 3, 29),
            Some(utc(2026, 1, 8, 2, 30)),
            "北京 11:29 归 10:30 窗口而非 09:00（旧表跨午休口径已废）"
        );
    }

    /// 承接 旧时段表案 `h4_windows`。夜盘 240 分钟恰为一根 4h（与旧表「跨日一根」
    /// 等价）；日盘 4h 被窗口右端截断（旧表为 09:00–15:00 一根），
    /// `window_end` 断言同时承接 `window_end_cases` 口径。
    #[test]
    fn h4_bucket_spans_night_and_truncates_at_break() {
        let session = cn_futures_default();
        let policy = SessionWindowPolicy;
        // 北京次日 00:30 = UTC 当日 16:30 → 锚回周三 21:00（UTC 13:00）
        let night = policy
            .bucket_of(Timeframe(240), utc(2026, 1, 7, 16, 30), &session)
            .expect("夜盘 00:30 在窗口内");
        assert_eq!(
            night.start,
            utc(2026, 1, 7, 13, 0),
            "4h 夜盘跨日一根：锚周三 21:00"
        );
        assert_eq!(
            night.window_end,
            Some(utc(2026, 1, 7, 17, 0)),
            "夜盘 4h 桶终点 = 窗口右端 01:00（北京）"
        );
        // 日盘首窗：起点 09:00，被午休 10:15 截断（旧表此处为 09:00–15:00 一根）
        let day = policy
            .bucket_of(Timeframe(240), utc(2026, 1, 8, 2, 0), &session)
            .expect("北京 10:00 在日盘窗口内");
        assert_eq!(day.start, utc(2026, 1, 8, 1, 0), "日盘 4h 锚 09:00");
        assert_eq!(
            day.window_end,
            Some(utc(2026, 1, 8, 2, 15)),
            "日盘 4h 被午休 10:15 截断，不得吞掉午休后时段"
        );
        // 午后窗口另起一根
        assert_eq!(
            policy
                .bucket_of(Timeframe(240), utc(2026, 1, 8, 6, 0), &session)
                .map(|b| b.start),
            Some(utc(2026, 1, 8, 5, 30)),
            "北京 14:00 归 13:30 起的另一根"
        );
    }
}
