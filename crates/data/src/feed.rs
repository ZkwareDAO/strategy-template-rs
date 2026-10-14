//! 1m 事件分发器 — 实盘 WS / 回测回放共用同一条管线
//!
//! 回测一致性的核心机制：
//! - 实盘：每根已闭合 1m K 线（WS `x=true`）→ [`Feed::on_closed_1m`]
//! - 回测：CSV 逐根 1m → [`Feed::on_closed_1m`]（同函数，同一分发语义）
//! - 增量聚合：每个时间框架维护「已闭合桶序列 + 形成中桶」，O(1) 摊销
//! - 只有大周期桶**恰好完整**时才进入 closed 序列并标记 newly_closed
//!   （对齐 Python get_closed_data 的已闭合语义）
//! - 预载起点在桶中间时，首个残缺桶永不进入 closed（对齐 Python
//!   drop_partial_head：源起点晚于首桶边界 → 丢弃首桶）

use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use tracing::debug;

use strategy_core::candle::{Candle, Timeframe};

/// 单时间框架聚合状态
#[derive(Default)]
struct TfState {
    /// 已闭合桶（升序）
    closed: Vec<Candle>,
    /// 形成中的桶（尚未收满）
    partial: Option<Candle>,
    /// 该 partial 是否从桶中间开始（仅可能是首个桶；完整时丢弃不入 closed）
    tainted: bool,
}

impl TfState {
    fn merge(&mut self, tf: Timeframe, c: &Candle) -> bool {
        // 返回 true = 因这根 1m 桰恰好完整（应进入 closed）
        let size_secs = tf.0 as i64 * 60;
        let bs_secs = c.open_time.timestamp() - c.open_time.timestamp().rem_euclid(size_secs);

        match self.partial.as_mut() {
            Some(p) if p.open_time.timestamp() == bs_secs => {
                // 同桶累积
                if c.high > p.high {
                    p.high = c.high;
                }
                if c.low < p.low {
                    p.low = c.low;
                }
                p.close = c.close;
                p.volume += c.volume;
                // 非累加字段随最新一根覆盖（桶闭合时即末根值）
                p.quote_volume = c.quote_volume;
                p.trade_num = c.trade_num;
                p.active_buy_volume = c.active_buy_volume;
                p.active_buy_quote_volume = c.active_buy_quote_volume;
                // 桶是否恰好收满（这根 1m 是桶内最后一分钟）
                (c.open_time.timestamp() + 60) - bs_secs == size_secs
            }
            _ => {
                // 新桶开启；若它是整个序列的第一个桶且起点不在桶边界 → tainted
                self.tainted = self.closed.is_empty() && c.open_time.timestamp() != bs_secs;
                self.partial = Some(Candle {
                    open_time: chrono::Utc.timestamp_opt(bs_secs, 0).unwrap(),
                    open: c.open,
                    high: c.high,
                    low: c.low,
                    close: c.close,
                    volume: c.volume,
                    quote_volume: c.quote_volume,
                    trade_num: c.trade_num,
                    active_buy_volume: c.active_buy_volume,
                    active_buy_quote_volume: c.active_buy_quote_volume,
                    is_closed: true,
                });
                false
            }
        }
    }

    fn complete(&mut self) -> Option<Candle> {
        let p = self.partial.take()?;
        if self.tainted {
            self.tainted = false; // 残缺首桶：丢弃
            return None;
        }
        self.closed.push(p);
        self.closed.last().cloned()
    }
}

/// 分发器状态
pub struct Feed {
    /// 全部时间框架（含 1m，升序）
    timeframes: Vec<Timeframe>,
    states: HashMap<Timeframe, TfState>,
    /// closed 序列上限（内存保护；0 = 不限）
    max_len: usize,
    /// 已摄入最新 1m 的 open_time 水位：≤ 水位的帧整体跳过。
    /// 让启动预热/重连同窗口回填、WS 重复闭合帧的重放幂等——
    /// 大周期闭合序列不会因重复 preload 翻倍污染指标。
    watermark: Option<DateTime<Utc>>,
}

/// 一次分发事件
pub struct FeedEvent {
    /// 刚闭合的 1m K 线
    pub candle_1m: Candle,
    /// 本次 1m 收盘导致新闭合的时间框架（恒含 1m，升序）
    pub newly_closed: Vec<Timeframe>,
}

impl Feed {
    pub fn new(timeframes: Vec<Timeframe>, max_len: usize) -> Self {
        let mut tfs = timeframes;
        if !tfs.contains(&Timeframe::M1) {
            tfs.push(Timeframe::M1);
        }
        tfs.sort();
        tfs.dedup();
        Self {
            states: tfs.iter().map(|tf| (*tf, TfState::default())).collect(),
            timeframes: tfs,
            max_len,
            watermark: None,
        }
    }

    /// 预加载历史（warmup）：逐根走同一增量路径，不产生事件。
    /// 幂等：open_time 不新于已摄入水位的帧跳过，故重连同窗口回填
    /// 可直接再次调用本函数而不会让闭合序列重复（序列需升序）。
    pub fn preload(&mut self, candles_1m: &[Candle]) {
        debug!(
            n = candles_1m.len(),
            "Feed preload 开始（历史预热，逐根走同一摄入路径）"
        );
        let mut ingested = 0usize;
        for c in candles_1m {
            if self.ingest(c).is_empty() {
                // 水位去重跳过，不计入
            } else {
                ingested += 1;
            }
        }
        debug!(
            ingested,
            skipped = candles_1m.len() - ingested,
            "Feed preload 完成"
        );
    }

    /// 喂入一根已闭合 1m（实盘 WS x=true / 回测逐根），返回分发事件。
    /// 每根 1m 都触发一次分发 —— 与实盘 WS 每根触发一次 analyze 对齐。
    pub fn on_closed_1m(&mut self, candle: Candle) -> FeedEvent {
        debug_assert!(candle.is_closed, "只允许喂入已闭合 1m");
        let newly_closed = self.ingest(&candle);
        FeedEvent {
            candle_1m: candle,
            newly_closed,
        }
    }

    /// 核心摄入路径：增量聚合 + 判定 newly_closed。
    /// open_time ≤ 水位的帧（重复 preload/重连同窗口回填/重复闭合帧）跳过，
    /// 因此要求摄入序列整体按 open_time 非严格递交给本函数。
    fn ingest(&mut self, c: &Candle) -> Vec<Timeframe> {
        if let Some(wm) = self.watermark {
            if c.open_time <= wm {
                debug!(bar_time = %c.open_time, watermark = %wm, "Feed 摄入：1m 不新于水位，跳过（重复帧幂等）");
                return Vec::new();
            }
        }
        self.watermark = Some(c.open_time);

        let mut newly_closed = Vec::new();
        for tf in &self.timeframes {
            let state = self.states.get_mut(tf).expect("tf state");
            if tf.0 == 1 {
                // 1m：每根即完整桶
                state.closed.push(*c);
                newly_closed.push(*tf);
            } else if state.merge(*tf, c) && state.complete().is_some() {
                newly_closed.push(*tf);
            }
        }
        newly_closed.sort();
        self.trim();
        newly_closed
    }

    /// 只读访问 per-timeframe 已闭合序列（策略 Ctx 数据源；升序，全为完整桶）
    pub fn closed_map(&self) -> HashMap<Timeframe, &Vec<Candle>> {
        self.states.iter().map(|(k, v)| (*k, &v.closed)).collect()
    }

    /// 最近一根已闭合 1m 的收盘价（1m 粒度下的实时价语义）
    pub fn last_price(&self) -> Option<f64> {
        self.states
            .get(&Timeframe::M1)?
            .closed
            .last()
            .map(|c| c.close)
    }

    fn trim(&mut self) {
        if self.max_len == 0 {
            return;
        }
        for st in self.states.values_mut() {
            if st.closed.len() > self.max_len {
                let drain = st.closed.len() - self.max_len;
                st.closed.drain(..drain);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn m1_at(minute: usize) -> Candle {
        Candle {
            open_time: Utc
                .timestamp_opt(1_781_049_600 + minute as i64 * 60, 0)
                .unwrap(),
            open: 100.0,
            high: 101.0,
            low: 99.0,
            close: 100.5,
            volume: 1.0,
            quote_volume: 0.0,
            trade_num: 0,
            active_buy_volume: 0.0,
            active_buy_quote_volume: 0.0,
            is_closed: true,
        }
    }

    fn m1(n: usize) -> Vec<Candle> {
        (0..n).map(m1_at).collect()
    }

    #[test]
    fn preload_populates_all_timeframes() {
        let mut feed = Feed::new(vec![Timeframe::H1], 0);
        feed.preload(&m1(120)); // 00:00..01:59 → 2 个完整 1h 桶
        assert_eq!(feed.closed_map()[&Timeframe::M1].len(), 120);
        assert_eq!(feed.closed_map()[&Timeframe::H1].len(), 2);
    }

    #[test]
    fn realtime_close_fires_newly_closed() {
        let mut feed = Feed::new(vec![Timeframe::H1], 0);
        feed.preload(&m1(119)); // 00:00..01:58 → 1h 桶 00:00 完整

        // 第 120 根（01:59）→ 1h 桶 01:00 闭合
        let ev = feed.on_closed_1m(m1_at(119));
        assert!(ev.newly_closed.contains(&Timeframe::H1));
        assert_eq!(feed.closed_map()[&Timeframe::H1].len(), 2);

        // 下一根（02:00）→ 仅 1m 闭合
        let ev2 = feed.on_closed_1m(m1_at(120));
        assert_eq!(ev2.newly_closed, vec![Timeframe::M1]);
        assert_eq!(feed.closed_map()[&Timeframe::H1].len(), 2); // 未变
    }

    #[test]
    fn tainted_head_bucket_never_enters_closed() {
        // 预载从 00:30 开始（首桶残缺）→ 00:00 桶 tainted
        let mut feed = Feed::new(vec![Timeframe::H1], 0);
        // 00:30..01:59 共 90 根
        feed.preload(&m1(120).into_iter().skip(30).collect::<Vec<_>>());
        // 00:30..00:59 组成残缺首桶 00:00（缺前 30 分钟）→ 丢弃；
        // 01:00 桶完整（01:00..01:59 = 60 根）→ 保留
        let h1 = &feed.closed_map()[&Timeframe::H1];
        assert_eq!(h1.len(), 1);
        assert_eq!(
            h1[0].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 1, 0, 0).unwrap()
        );
    }

    #[test]
    fn max_len_trim_keeps_complete_buckets() {
        let mut feed = Feed::new(vec![Timeframe::M1, Timeframe::H1], 10);
        for c in m1(600) {
            feed.on_closed_1m(c);
        }
        assert_eq!(feed.closed_map()[&Timeframe::M1].len(), 10);
        assert_eq!(feed.closed_map()[&Timeframe::H1].len(), 10); // 10 个完整 1h 桶
    }

    #[test]
    fn eight_hour_buckets() {
        let mut feed = Feed::new(vec![Timeframe::parse("8h").unwrap()], 0);
        feed.preload(&m1(1440)); // 3 个完整 8h 桶
        let h8 = &feed.closed_map()[&Timeframe::parse("8h").unwrap()];
        assert_eq!(h8.len(), 3);
        assert_eq!(
            h8[0].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
        );
        assert_eq!(
            h8[1].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 8, 0, 0).unwrap()
        );
        assert_eq!(
            h8[2].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 16, 0, 0).unwrap()
        );
    }

    #[test]
    fn repeated_preload_same_window_is_idempotent() {
        // 实盘启动后首帧 Reconnected 会用同一 30d 窗口再补缺一次
        // （runtime_actor 的 Reconnected 路径）：重复窗口必须整体跳过，
        // 已闭合大周期桶不能翻倍或乱序。
        let bars = m1(2880); // 恰好 6 个完整 8h 桶
        let mut feed = Feed::new(vec![Timeframe::parse("8h").unwrap()], 0); // 不截断
        feed.preload(&bars);
        feed.preload(&bars);

        let h8 = &feed.closed_map()[&Timeframe::parse("8h").unwrap()];
        assert_eq!(h8.len(), 6, "同窗口二次 preload 不得产生重复闭合桶");
        assert!(
            h8.windows(2).all(|w| w[0].open_time < w[1].open_time),
            "闭合序列必须保持升序无重复"
        );
        assert_eq!(feed.closed_map()[&Timeframe::M1].len(), 2880);
    }

    #[test]
    fn overlapping_preload_only_ingests_new_candles() {
        // 第二次窗口与第一次重叠但多 1 根（两次 backfill 间隔跨过整点）：
        // 只摄入新根，已闭合桶不重复，新根进入形成中的下一桶。
        let mut feed = Feed::new(vec![Timeframe::H1], 0);
        feed.preload(&m1(120)); // 00:00..01:59 → 2 个完整 1h 桶
        feed.preload(&m1(121)); // 重叠 120 根 + 新根 02:00（新桶 partial）

        assert_eq!(feed.closed_map()[&Timeframe::H1].len(), 2);
        assert_eq!(feed.closed_map()[&Timeframe::M1].len(), 121);
    }
}
