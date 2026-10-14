//! live 预热装配：本地 CSV 打底 + 远程回填补缺（对齐 Python
//! data_manager：本地有历史先读本地，远程只补本地末根之后的窗口）。
//!
//! - 本地 CSV 是 K 线持久层（WS 闭合帧实时追加，跨重启累积）；
//! - 远程 backfill 取最近窗口，与本地按 `open_time` 合并去重
//!   （同时间戳远程值胜出——它可能修正本地末根）；
//! - 合并结果升序，尾部截取 `warmup_bars` 根喂给 Feed；
//! - 远程新于本地末根的 K 线同步追加进 CSV，使本地文件持续保持最新。

use std::collections::BTreeMap;
use std::path::Path;

use chrono::{DateTime, Utc};
use strategy_core::candle::{Candle, Timeframe};
use tracing::{debug, warn};

use crate::client::KlineClient;
use crate::csv_store::{self, LiveCsvSink};

/// 预热来源统计（启动日志用）
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WarmupStats {
    /// 本地 CSV 读到的根数
    pub local_bars: usize,
    /// 远程回填返回的根数（失败为 0）
    pub remote_bars: usize,
    /// 远程回填是否成功
    pub remote_ok: bool,
    /// 本次新追加进本地 CSV 的根数
    pub persisted_new: usize,
    /// 合并去重 + 截取后实际预热根数
    pub total_bars: usize,
}

/// 两路 K 线按 open_time 合并去重（同时间戳 remote 胜出），升序，
/// 尾部保留最多 max_bars 根。max_bars=0 表示不截断。
pub fn merge_candles(local: &[Candle], remote: &[Candle], max_bars: usize) -> Vec<Candle> {
    // BTreeMap 按 open_time 排序去重；后插的 remote 覆盖同时间戳本地值
    let mut by_ts: BTreeMap<DateTime<Utc>, Candle> = BTreeMap::new();
    for c in local {
        by_ts.insert(c.open_time, *c);
    }
    for c in remote {
        by_ts.insert(c.open_time, *c);
    }
    let all: Vec<Candle> = by_ts.into_values().collect();
    if max_bars > 0 && all.len() > max_bars {
        all[all.len() - max_bars..].to_vec()
    } else {
        all
    }
}

/// live 启动预热：读本地 1m CSV → 远程 backfill → 合并去重 →
/// 远程新 K 线追加落盘 → 返回（预热序列, 统计）。
///
/// 远程失败不致命：只用本地数据（比旧行为"远程失败 warn 后空预热"
/// 多了本地兜底）。
pub async fn load_warmup(
    data_dir: &Path,
    client: &dyn KlineClient,
    symbol: &str,
    warmup_bars: usize,
) -> (Vec<Candle>, WarmupStats) {
    debug!(
        symbol,
        warmup_bars, "历史预热开始：本地 CSV 打底 + 远程回填补缺"
    );
    let path = csv_store::csv_path(data_dir, symbol, Timeframe::M1);
    let local = match csv_store::load_csv(&path) {
        Ok(v) => {
            debug!(symbol, n = v.len(), "本地 1m CSV 已读取");
            v
        }
        Err(e) => {
            warn!(path = ?path, error = %e, "本地 1m CSV 读取失败，按空历史继续");
            Vec::new()
        }
    };

    // 远程回填取最近 warmup_bars 窗口（服务端可能截断更短）；失败仅本地兜底
    let (remote, remote_ok) = match client
        .backfill(symbol, Timeframe::M1, warmup_bars.max(1))
        .await
    {
        Ok(v) => {
            debug!(symbol, n = v.len(), "远程 backfill 成功返回");
            (v, true)
        }
        Err(e) => {
            warn!(symbol, error = %e, "远程历史回填失败，仅使用本地 CSV 预热");
            (Vec::new(), false)
        }
    };

    // 远程新 K 线追加落盘（sink 以盘载末根为水位，重叠帧自动跳过）
    let mut persisted_new = 0usize;
    if remote_ok {
        match LiveCsvSink::open(&path) {
            Ok(mut sink) => {
                for c in &remote {
                    match sink.push(c) {
                        Ok(true) => persisted_new += 1,
                        Ok(false) => {}
                        Err(e) => {
                            warn!(path = ?path, error = %e, "1m CSV 追加失败，停止本次落盘");
                            break;
                        }
                    }
                }
            }
            Err(e) => warn!(path = ?path, error = %e, "打开 1m CSV 追加器失败"),
        }
    }

    let merged = merge_candles(&local, &remote, warmup_bars);
    debug!(
        symbol,
        local_bars = local.len(),
        remote_bars = remote.len(),
        persisted_new,
        total_bars = merged.len(),
        "历史预热完成：合并去重（同时间戳远程胜出），截取窗口如上"
    );
    let stats = WarmupStats {
        local_bars: local.len(),
        remote_bars: remote.len(),
        remote_ok,
        persisted_new,
        total_bars: merged.len(),
    };
    (merged, stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::client::{BackfillFuture, KlineStream};

    fn bar(min: i64, close: f64) -> Candle {
        Candle {
            open_time: Utc.timestamp_opt(min * 60, 0).unwrap(),
            open: close,
            high: close,
            low: close,
            close,
            volume: 1.0,
            quote_volume: 0.0,
            trade_num: 0,
            active_buy_volume: 0.0,
            active_buy_quote_volume: 0.0,
            is_closed: true,
        }
    }

    #[test]
    fn merge_empty_local_uses_remote() {
        let remote = vec![bar(1, 10.0), bar(2, 11.0)];
        let merged = merge_candles(&[], &remote, 0);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].open_time, remote[0].open_time);
    }

    #[test]
    fn merge_overlap_remote_wins_and_sorted_dedup() {
        // 本地 00:00/00:01/00:02，远程 00:02（修正收盘价）/00:03/00:04（乱序）
        let local = vec![bar(0, 10.0), bar(1, 11.0), bar(2, 12.0)];
        let remote = vec![bar(4, 14.0), bar(2, 99.0), bar(3, 13.0)];
        let merged = merge_candles(&local, &remote, 0);
        assert_eq!(merged.len(), 5, "00:02 去重后共 5 根");
        assert!(
            merged.windows(2).all(|w| w[0].open_time < w[1].open_time),
            "升序"
        );
        assert_eq!(merged[2].close, 99.0, "重叠时间戳远程值胜出");
        assert_eq!(merged[4].close, 14.0);
    }

    #[test]
    fn merge_tail_caps_to_max_bars() {
        let local: Vec<Candle> = (0..10).map(|m| bar(m, m as f64)).collect();
        let merged = merge_candles(&local, &[], 3);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].close, 7.0, "保留尾部最新 3 根");
    }

    /// backfill 返回固定序列（可配置失败）；attach 不被预热使用
    struct FakeClient {
        bars: Vec<Candle>,
        fail: bool,
        called: AtomicBool,
        last_bars: AtomicUsize,
    }

    impl KlineClient for FakeClient {
        fn backfill<'a>(
            &'a self,
            _symbol: &'a str,
            _tf: Timeframe,
            bars: usize,
        ) -> BackfillFuture<'a> {
            self.called.store(true, Ordering::SeqCst);
            self.last_bars.store(bars, Ordering::SeqCst);
            if self.fail {
                Box::pin(async { Err(anyhow::anyhow!("remote down")) })
            } else {
                let out = self.bars.clone();
                Box::pin(async move { Ok(out) })
            }
        }

        fn attach(&self, _symbol: &str, _tf: Timeframe) -> KlineStream {
            let (tx, rx) = tokio::sync::broadcast::channel(1);
            // 空流保持存活：sender 由 Box::leak 持有（防 recv 返回 Closed）
            let _keepalive = Box::leak(Box::new(tx));
            KlineStream { rx }
        }
    }

    #[tokio::test]
    async fn load_warmup_local_base_plus_remote_gap_persisted() {
        let dir = tempfile::tempdir().unwrap();
        // 本地有 00:00..00:02 三根
        csv_store::save_csv(
            csv_store::csv_path(dir.path(), "BTCUSDT", Timeframe::M1),
            &[bar(0, 10.0), bar(1, 11.0), bar(2, 12.0)],
        )
        .unwrap();
        // 远程：重叠 00:02 修正 + 补缺 00:03/00:04
        let client = FakeClient {
            bars: vec![bar(2, 99.0), bar(3, 13.0), bar(4, 14.0)],
            fail: false,
            called: AtomicBool::new(false),
            last_bars: AtomicUsize::new(0),
        };

        let (warm, stats) = load_warmup(dir.path(), &client, "BTCUSDT", 43200).await;

        assert!(client.called.load(Ordering::SeqCst));
        assert_eq!(
            client.last_bars.load(Ordering::SeqCst),
            43200,
            "按 warmup_bars 请求远程"
        );
        assert_eq!(stats.local_bars, 3);
        assert_eq!(stats.remote_bars, 3);
        assert!(stats.remote_ok);
        assert_eq!(stats.persisted_new, 2, "仅 00:03/00:04 追加，00:02 已存在");
        assert_eq!(stats.total_bars, 5);
        assert_eq!(warm.len(), 5);
        assert_eq!(warm[2].close, 99.0, "重叠值用远程修正");

        // CSV 已被追加到 5 根，且无重复时间戳
        let on_disk =
            csv_store::load_csv(csv_store::csv_path(dir.path(), "BTCUSDT", Timeframe::M1)).unwrap();
        assert_eq!(on_disk.len(), 5);
        assert!(on_disk.windows(2).all(|w| w[0].open_time < w[1].open_time));
    }

    #[tokio::test]
    async fn load_warmup_remote_failure_falls_back_to_local() {
        let dir = tempfile::tempdir().unwrap();
        csv_store::save_csv(
            csv_store::csv_path(dir.path(), "BTCUSDT", Timeframe::M1),
            &[bar(0, 10.0), bar(1, 11.0)],
        )
        .unwrap();
        let client = FakeClient {
            bars: vec![],
            fail: true,
            called: AtomicBool::new(false),
            last_bars: AtomicUsize::new(0),
        };

        let (warm, stats) = load_warmup(dir.path(), &client, "BTCUSDT", 100).await;
        assert!(!stats.remote_ok);
        assert_eq!(stats.local_bars, 2);
        assert_eq!(stats.total_bars, 2);
        assert_eq!(warm.len(), 2);
        assert_eq!(stats.persisted_new, 0);
    }

    #[tokio::test]
    async fn load_warmup_no_local_uses_remote_and_creates_csv() {
        let dir = tempfile::tempdir().unwrap();
        let client = FakeClient {
            bars: vec![bar(0, 10.0), bar(1, 11.0)],
            fail: false,
            called: AtomicBool::new(false),
            last_bars: AtomicUsize::new(0),
        };
        let (warm, stats) = load_warmup(dir.path(), &client, "BTCUSDT", 100).await;
        assert_eq!(stats.local_bars, 0);
        assert_eq!(stats.total_bars, 2);
        assert_eq!(stats.persisted_new, 2, "无本地文件时远程序列全部落盘");
        assert_eq!(warm.len(), 2);
        assert_eq!(
            csv_store::load_csv(csv_store::csv_path(dir.path(), "BTCUSDT", Timeframe::M1))
                .unwrap()
                .len(),
            2
        );
    }
}
