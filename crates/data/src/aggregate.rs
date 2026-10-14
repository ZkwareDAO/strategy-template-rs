//! 1m → 大周期聚合
//!
//! 对齐 Python `resample_ohlcv`（klines_loader.py:281）的语义：
//! - 桶 = open_time 左闭区间（UTC 整点网格：1h 整分、1d 午夜）
//! - open=first, high=max, low=min, close=last, volume=sum
//! - `drop_partial_head`：源数据起点晚于首桶边界 → 首桶残缺，丢弃
//! - 末尾未闭合桶由调用方（feed/strategy 的 closed_data）负责剔除

use chrono::{DateTime, TimeZone, Utc};

use strategy_core::candle::{Candle, Timeframe};

/// 计算包含 t 的 tf 桶起点（UTC 网格对齐：所有周期从 epoch 起算）
pub fn bucket_start(t: DateTime<Utc>, tf: Timeframe) -> DateTime<Utc> {
    let secs = t.timestamp();
    let size = tf.0 as i64 * 60;
    Utc.timestamp_opt(secs - secs.rem_euclid(size), 0).unwrap()
}

/// 将已闭合 1m K 线聚合到大周期。
///
/// 输入必须全部 `is_closed=true` 且升序。桶全部视为已闭合
/// （桶内最后 1m 已收盘 → 桶已收盘；末尾的 partial 桶由调用方剔除）。
///
/// `drop_partial_head=true` 时，若输入起点未落在首桶边界（首桶残缺），丢弃该桶。
pub fn aggregate_1m(candles_1m: &[Candle], tf: Timeframe, drop_partial_head: bool) -> Vec<Candle> {
    debug_assert!(
        candles_1m.iter().all(|c| c.is_closed),
        "聚合输入必须全部已闭合"
    );
    if candles_1m.is_empty() {
        return Vec::new();
    }

    let source_start = candles_1m[0].open_time;

    let mut buckets: Vec<Candle> = Vec::new();
    for c in candles_1m {
        let bs = bucket_start(c.open_time, tf);
        match buckets.last_mut() {
            Some(last) if last.open_time == bs => {
                // 同桶累积
                if c.high > last.high {
                    last.high = c.high;
                }
                if c.low < last.low {
                    last.low = c.low;
                }
                last.close = c.close;
                last.volume += c.volume;
            }
            _ => buckets.push(Candle {
                open_time: bs,
                open: c.open,
                high: c.high,
                low: c.low,
                close: c.close,
                volume: c.volume,
                // 非累加字段随桶内最后一根（对齐 Python resample last）
                quote_volume: c.quote_volume,
                trade_num: c.trade_num,
                active_buy_volume: c.active_buy_volume,
                active_buy_quote_volume: c.active_buy_quote_volume,
                is_closed: true,
            }),
        }
    }

    // 丢弃残缺首桶：源起点晚于首桶边界 → 首桶只含后半段
    if drop_partial_head {
        if let Some(first) = buckets.first() {
            if source_start > first.open_time {
                buckets.remove(0);
            }
        }
    }

    buckets
}

/// 大周期桶是否恰好因输入的最后一根 1m 而闭合
/// （最后一根 1m 是该桶最后一分钟时，返回桶起点）
pub fn bucket_closed_at(candles_1m: &[Candle], tf: Timeframe) -> Option<DateTime<Utc>> {
    let last = candles_1m.last()?;
    let bs = bucket_start(last.open_time, tf);
    let size_min = tf.0 as i64;
    let offset_min = (last.open_time - bs).num_minutes() + 1;
    if offset_min == size_min {
        Some(bs)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// 生成 2026-06-10 00:00 起连续 n 根 1m，价格递增
    fn m1_series(n: usize) -> Vec<Candle> {
        (0..n)
            .map(|i| Candle {
                open_time: Utc.timestamp_opt(1_781_049_600 + i as i64 * 60, 0).unwrap(),
                open: 100.0 + i as f64,
                high: 101.0 + i as f64,
                low: 99.0 + i as f64,
                close: 100.5 + i as f64,
                volume: 1.0,
                quote_volume: 0.0,
                trade_num: 0,
                active_buy_volume: 0.0,
                active_buy_quote_volume: 0.0,
                is_closed: true,
            })
            .collect()
    }

    #[test]
    fn aggregate_hour_from_7_minutes() {
        // 00:00..00:06 共 7 根 → 1 个 1h 桶（partial，仅测试聚合值）
        let m1 = m1_series(7);
        let out = aggregate_1m(&m1, Timeframe::H1, false);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
        );
        assert_eq!(out[0].open, 100.0);
        assert_eq!(out[0].close, 106.5);
        assert_eq!(out[0].high, 107.0);
        assert_eq!(out[0].low, 99.0);
        assert!((out[0].volume - 7.0).abs() < 1e-9);

        // bucket_closed_at：7 根 ≠ 60 根，未闭合
        assert!(bucket_closed_at(&m1, Timeframe::H1).is_none());
    }

    #[test]
    fn full_hour_bucket_closes() {
        let m1 = m1_series(60);
        assert_eq!(
            bucket_closed_at(&m1, Timeframe::H1).unwrap(),
            Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn drop_partial_head_when_source_starts_mid_bucket() {
        // 源数据从 00:30 开始（首桶残缺）→ 丢 00:00 桶
        let m1: Vec<Candle> = m1_series(90).into_iter().skip(30).collect();
        let out = aggregate_1m(&m1, Timeframe::H1, true);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 1, 0, 0).unwrap()
        );

        // 不丢时保留残缺桶
        let out2 = aggregate_1m(&m1, Timeframe::H1, false);
        assert_eq!(out2.len(), 2);
    }

    #[test]
    fn eight_hour_boundary() {
        // 3 个完整 8h 桶 = 1440 根 1m
        let m1 = m1_series(1440);
        let out = aggregate_1m(&m1, Timeframe::parse("8h").unwrap(), true);
        assert_eq!(out.len(), 3);
        assert_eq!(
            out[0].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
        );
        assert_eq!(
            out[1].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 8, 0, 0).unwrap()
        );
        assert_eq!(
            out[2].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 16, 0, 0).unwrap()
        );
    }

    #[test]
    fn day_boundary_utc_midnight() {
        let m1 = m1_series(1440);
        let out = aggregate_1m(&m1, Timeframe::D1, true);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].open_time,
            Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
        );
    }
}
