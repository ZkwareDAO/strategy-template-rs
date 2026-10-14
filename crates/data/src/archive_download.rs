//! 币安公共数据源历史 K 线下载 — Rust 版 `download_data`
//!
//! 数据源（无需 API key）：
//! - **归档包** `data.binance.vision/data/futures/um`：monthly zip（完整自然月，已结束）、
//!   daily zip（早于今天的整天）。一次请求顶一个月/一天，远快于 fapi 逐页翻。
//! - **fapi** `/fapi/v1/klines`：分页拉取，兜住归档未覆盖的零头与今日尾部；
//!   归档尚未发布（404）时自动降级 fapi 补齐该块。
//!
//! 分块规划与 Python 版 `plan_chunks` 语义一致：
//! 完整已结束自然月 → monthly；其余整天 → daily；未对齐零头只 fapi 到下一个整天
//! 边界（避免整个缺口退化成单个巨型 fapi 范围）；最后一段尾巴 → fapi。
//!
//! 落盘复用 [`csv_store::save_csv`]（原子写，格式与回测/实盘完全一致）。
//!
//! 两个入口：
//! - [`Downloader::download_range`]：CLI `download` 显式下载指定区间；
//! - [`Downloader::ensure_recent_history`]：run 启动自动确保近 N 天（默认
//!   [`ENSURE_HISTORY_DAYS`]=365），按现有 CSV 首/末根只补缺，已最新则零请求。

use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::time::Duration;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, TimeZone, Utc};

use strategy_core::candle::{Candle, Timeframe};

use crate::csv_store;

/// 归档基地址（USDT 永续合约）
pub const ARCHIVE_BASE: &str = "https://data.binance.vision/data/futures/um";
/// fapi 基地址
pub const FAPI_BASE: &str = "https://fapi.binance.com";
/// 币安单页上限
pub const PAGE_LIMIT: usize = 1500;
/// 启动时自动确保的历史天数（近一年）
pub const ENSURE_HISTORY_DAYS: i64 = 365;
/// 网络 GET 重试次数（归档链路实测会间歇性 SSL EOF）
const MAX_RETRIES: u32 = 3;
const RETRY_BACKOFF_MS: u64 = 2000;

/// 归档分块：整月 / 整天 / fapi 零头（毫秒区间）
#[derive(Debug, Clone, PartialEq)]
enum Chunk {
    Monthly { year: i32, month: u32 },
    Daily(NaiveDate),
    Fapi { start_ms: i64, end_ms: i64 },
}

/// 归档未发布（HTTP 404）——调用方据此降级 fapi，不视为错误
#[derive(Debug)]
struct ArchiveNotFound;

impl fmt::Display for ArchiveNotFound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "archive not found (404)")
    }
}

impl std::error::Error for ArchiveNotFound {}

/// 自动补缺统计（启动日志用）
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EnsureStats {
    /// 补缺前本地 CSV 根数
    pub existing_bars: usize,
    /// 本次规划出的下载区间数
    pub ranges: usize,
    /// 各区间实际拉回的行数（含与本地重叠的末根）
    pub fetched_bars: usize,
    /// 落盘后全量根数
    pub total_bars: usize,
}

/// 下载器
pub struct Downloader {
    client: reqwest::Client,
    archive_base: String,
    fapi_base: String,
}

impl Default for Downloader {
    fn default() -> Self {
        Self::new()
    }
}

impl Downloader {
    pub fn new() -> Self {
        Self {
            // reqwest 默认读取 HTTPS_PROXY/HTTP_PROXY 环境变量（国内网络需要）
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .build()
                .expect("reqwest client"),
            archive_base: ARCHIVE_BASE.into(),
            fapi_base: FAPI_BASE.into(),
        }
    }

    // ============================================================
    // 分块规划（纯函数，不发网络请求）
    // ============================================================

    /// 把 [start, end) 切成归档/fapi 块，按时间升序。
    ///
    /// `end` 同时充当"今天"边界（归档只发布已完结区间：monthly 仅完整月、
    /// daily 仅到昨天），下载流程里 end = now，与 Python 版注入 today 等价。
    fn plan_chunks(start: DateTime<Utc>, end: DateTime<Utc>) -> Vec<Chunk> {
        let today = end.date_naive();
        let mut chunks = Vec::new();
        let mut cursor = start;
        while cursor < end {
            // 整个自然月都落在缺口内、且该月已结束 → monthly
            let month_start = Utc
                .with_ymd_and_hms(cursor.year(), cursor.month(), 1, 0, 0, 0)
                .unwrap();
            let next_month = next_month_start(cursor.year(), cursor.month());
            if cursor == month_start && next_month <= end && next_month.date_naive() <= today {
                chunks.push(Chunk::Monthly {
                    year: cursor.year(),
                    month: cursor.month(),
                });
                cursor = next_month;
                continue;
            }

            // 整天且早于今天 → daily
            let day_start = Utc
                .with_ymd_and_hms(cursor.year(), cursor.month(), cursor.day(), 0, 0, 0)
                .unwrap();
            let next_day = day_start + ChronoDuration::days(1);
            if cursor == day_start && next_day <= end && cursor.date_naive() < today {
                chunks.push(Chunk::Daily(cursor.date_naive()));
                cursor = next_day;
                continue;
            }

            // 未对齐零头：只 fapi 到下一个整天边界，然后继续切块。
            // 若在此直接 fapi 到 end，一个 03:18 这样的起点会让整个跨月缺口
            // 退化成单个巨型 fapi 范围，归档完全用不上。
            if cursor < next_day && next_day < end {
                chunks.push(Chunk::Fapi {
                    start_ms: cursor.timestamp_millis(),
                    end_ms: next_day.timestamp_millis(),
                });
                cursor = next_day;
                continue;
            }

            // 真正的尾巴（今天，或最后一段不足整天）→ fapi
            chunks.push(Chunk::Fapi {
                start_ms: cursor.timestamp_millis(),
                end_ms: end.timestamp_millis(),
            });
            break;
        }
        chunks
    }

    // ============================================================
    // 归档包
    // ============================================================

    fn archive_url(&self, symbol: &str, tf_str: &str, chunk: &Chunk) -> String {
        let sym = symbol.to_uppercase();
        match chunk {
            Chunk::Monthly { year, month } => format!(
                "{}/monthly/klines/{sym}/{tf_str}/{sym}-{tf_str}-{year:04}-{month:02}.zip",
                self.archive_base
            ),
            Chunk::Daily(d) => format!(
                "{}/daily/klines/{sym}/{tf_str}/{sym}-{tf_str}-{}.zip",
                self.archive_base,
                d.format("%Y-%m-%d")
            ),
            Chunk::Fapi { .. } => unreachable!("fapi 块不走归档 URL"),
        }
    }

    /// 下载一个归档 zip 并解析；404 → `ArchiveNotFound`
    async fn fetch_archive_chunk(
        &self,
        symbol: &str,
        tf_str: &str,
        chunk: &Chunk,
    ) -> anyhow::Result<Vec<Candle>> {
        let url = self.archive_url(symbol, tf_str, chunk);
        match self.get_bytes_with_retry(&url).await {
            Ok(bytes) => Self::parse_archive_zip(&bytes),
            Err(e) => {
                if e.downcast_ref::<ArchiveNotFound>().is_some() {
                    Err(e)
                } else {
                    Err(anyhow::anyhow!("归档下载失败: {url} — {e}"))
                }
            }
        }
    }

    /// 归档 zip（内含单个 12 列 CSV）→ Candle 列表。
    ///
    /// 归档 CSV 首行是表头（`open_time,...`）；旧归档无表头，按首格是否可转
    /// 数字判断，两种都能吃。
    fn parse_archive_zip(bytes: &[u8]) -> anyhow::Result<Vec<Candle>> {
        let mut zf = zip::ZipArchive::new(std::io::Cursor::new(bytes))
            .map_err(|e| anyhow::anyhow!("zip 打开失败: {e}"))?;
        if zf.is_empty() {
            return Ok(Vec::new());
        }
        let mut file = zf
            .by_index(0)
            .map_err(|e| anyhow::anyhow!("zip 读取失败: {e}"))?;
        let mut raw = String::new();
        file.read_to_string(&mut raw)?;

        let mut out = Vec::new();
        // 归档 CSV 首行可能是表头（open_time 不可转数字）；旧归档无表头。逐行判断，
        // 不能盲 skip(1) —— 否则无表头格式会丢第一根 K 线。
        for line in raw.lines() {
            let mut cols = line.split(',');
            let ot: i64 = match cols.next().and_then(|s| s.trim().parse().ok()) {
                Some(v) => v,
                None => continue, // 表头行
            };
            let parse = |s: Option<&str>| s.and_then(|v| v.trim().parse().ok()).unwrap_or(0.0);
            out.push(Candle {
                open_time: Utc.timestamp_millis_opt(ot).unwrap(),
                open: parse(cols.next()),
                high: parse(cols.next()),
                low: parse(cols.next()),
                close: parse(cols.next()),
                volume: parse(cols.next()),
                // 位 6 close_time 跳过，位 7/8/9/10 为扩展列
                quote_volume: {
                    let _close_time = cols.next();
                    parse(cols.next())
                },
                trade_num: parse(cols.next()) as u64,
                active_buy_volume: parse(cols.next()),
                active_buy_quote_volume: parse(cols.next()),
                is_closed: true,
            });
        }
        Ok(out)
    }

    // ============================================================
    // fapi 分页
    // ============================================================

    /// 拉取一页 K 线
    async fn fetch_page(
        &self,
        symbol: &str,
        tf_str: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> anyhow::Result<Vec<Candle>> {
        let url = format!(
            "{}/fapi/v1/klines?symbol={}&interval={tf_str}&startTime={start_ms}&endTime={end_ms}&limit={PAGE_LIMIT}",
            self.fapi_base,
            symbol.to_uppercase()
        );
        #[derive(serde::Deserialize)]
        #[allow(dead_code)] // 12 列按位置反序列化，仅消费前 6 列
        struct RawKline(
            i64,
            String,
            String,
            String,
            String,
            String,
            i64,
            String,
            i64,
            String,
            String,
            String,
        );
        let raw: Vec<RawKline> = self
            .client
            .get(&url)
            .timeout(Duration::from_secs(30))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(raw
            .iter()
            .map(|r| Candle {
                open_time: Utc.timestamp_millis_opt(r.0).unwrap(),
                open: r.1.parse().unwrap_or(0.0),
                high: r.2.parse().unwrap_or(0.0),
                low: r.3.parse().unwrap_or(0.0),
                close: r.4.parse().unwrap_or(0.0),
                volume: r.5.parse().unwrap_or(0.0),
                quote_volume: r.7.parse().unwrap_or(0.0),
                trade_num: r.8 as u64,
                active_buy_volume: r.9.parse().unwrap_or(0.0),
                active_buy_quote_volume: r.10.parse().unwrap_or(0.0),
                is_closed: true,
            })
            .collect())
    }

    /// 分页拉取 [start, end)
    async fn fetch_fapi_range(
        &self,
        symbol: &str,
        tf_str: &str,
        step_ms: i64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<Vec<Candle>> {
        let mut all = Vec::new();
        let mut cursor = start;
        while cursor < end {
            let page = self
                .fetch_page(
                    symbol,
                    tf_str,
                    cursor.timestamp_millis(),
                    end.timestamp_millis(),
                )
                .await?;
            if page.is_empty() {
                break;
            }
            let last = page.last().unwrap().open_time;
            all.extend(page);
            cursor = last + ChronoDuration::milliseconds(step_ms);
            tracing::info!(count = all.len(), %cursor, "fapi 拉取中");
            tokio::time::sleep(Duration::from_millis(200)).await; // 轻微限速
        }
        Ok(all)
    }

    // ============================================================
    // 对外主流程
    // ============================================================

    /// 拉取一个 [start, end) 区间的全部 K 线（分块：归档优先，404/零头 fapi
    /// 兜底），不落盘。供单区间下载与多区间自动补缺共用。
    async fn fetch_rows_in_range(
        &self,
        symbol: &str,
        tf_str: &str,
        step_ms: i64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> anyhow::Result<Vec<Candle>> {
        let mut new_rows: Vec<Candle> = Vec::new();
        for chunk in Self::plan_chunks(start, end) {
            match chunk {
                Chunk::Fapi { start_ms, end_ms } => {
                    let s = Utc.timestamp_millis_opt(start_ms).unwrap();
                    let e = Utc.timestamp_millis_opt(end_ms).unwrap();
                    let rows = self.fetch_fapi_range(symbol, tf_str, step_ms, s, e).await?;
                    new_rows.extend(rows);
                }
                archive_chunk => {
                    match self
                        .fetch_archive_chunk(symbol, tf_str, &archive_chunk)
                        .await
                    {
                        Ok(rows) => {
                            tracing::info!(
                                chunk = %archive_chunk_label(&archive_chunk),
                                n = rows.len(),
                                "归档命中"
                            );
                            new_rows.extend(rows);
                        }
                        Err(e) if e.downcast_ref::<ArchiveNotFound>().is_some() => {
                            // 归档未发布 → fapi 兜住该块，避免静默丢一整块
                            let (s, e) = chunk_bounds(&archive_chunk, end);
                            tracing::info!(
                                chunk = %archive_chunk_label(&archive_chunk),
                                "归档未发布，改用 fapi 补齐"
                            );
                            let rows = self.fetch_fapi_range(symbol, tf_str, step_ms, s, e).await?;
                            new_rows.extend(rows);
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Ok(new_rows)
    }

    /// 下载 [start, end) 并 merge 进现有 CSV，返回落盘后全量根数。
    pub async fn download_range(
        &self,
        symbol: &str,
        tf: Timeframe,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        data_dir: &std::path::Path,
    ) -> anyhow::Result<usize> {
        anyhow::ensure!(end > start, "end 必须晚于 start");
        let path = csv_store::csv_path(data_dir, symbol, tf);
        let existing = csv_store::load_csv(&path)?;
        tracing::info!(
            symbol, %tf, %start, %end,
            existing = existing.len(),
            "开始下载（归档优先，fapi 兜底）"
        );

        let new_rows = self
            .fetch_rows_in_range(symbol, &tf.to_string(), tf.0 as i64 * 60_000, start, end)
            .await?;

        let existing_n = existing.len();
        let fetched_n = new_rows.len();
        let merged = merge_rows(existing, new_rows);
        csv_store::save_csv(&path, &merged)?;
        tracing::info!(
            path = %path.display(),
            total = merged.len(),
            fetched = fetched_n,
            added = merged.len() - existing_n,
            "下载完成"
        );
        Ok(merged.len())
    }

    /// 自动确保本地 CSV 覆盖最近 `days` 天到现在（幂等，可每次启动调用）：
    /// 无历史 → 整段下；首根太晚 → 补头；末根陈旧 → 补尾；已覆盖且最新 →
    /// 零网络请求。多区间只读写 CSV 各一次。
    pub async fn ensure_recent_history(
        &self,
        symbol: &str,
        tf: Timeframe,
        days: i64,
        data_dir: &std::path::Path,
    ) -> anyhow::Result<EnsureStats> {
        let now = Utc::now();
        let target_start = now - ChronoDuration::days(days);
        let step = ChronoDuration::minutes(tf.0 as i64);
        let path = csv_store::csv_path(data_dir, symbol, tf);

        let existing = csv_store::load_csv(&path)?;
        let bounds = existing
            .first()
            .zip(existing.last())
            .map(|(a, b)| (a.open_time, b.open_time));
        let ranges = Self::plan_ensure_ranges(bounds, target_start, now, step);

        if ranges.is_empty() {
            tracing::info!(
                symbol, %tf, existing = existing.len(),
                "历史已覆盖近 {days} 天且为最新，跳过下载"
            );
            return Ok(EnsureStats {
                existing_bars: existing.len(),
                ranges: 0,
                fetched_bars: 0,
                total_bars: existing.len(),
            });
        }

        tracing::info!(
            symbol, %tf, existing = existing.len(), ranges = ranges.len(),
            ?target_start, ?now, "近 {days} 天历史补缺开始（归档优先，fapi 兜底）"
        );
        let tf_str = tf.to_string();
        let step_ms = tf.0 as i64 * 60_000;
        let mut fetched = Vec::new();
        for (s, e) in &ranges {
            fetched.extend(
                self.fetch_rows_in_range(symbol, &tf_str, step_ms, *s, *e)
                    .await?,
            );
        }

        let stats = EnsureStats {
            existing_bars: existing.len(),
            ranges: ranges.len(),
            fetched_bars: fetched.len(),
            total_bars: 0,
        };
        let merged = merge_rows(existing, fetched);
        csv_store::save_csv(&path, &merged)?;
        tracing::info!(
            path = %path.display(),
            ranges = stats.ranges,
            fetched = stats.fetched_bars,
            total = merged.len(),
            "历史补缺完成"
        );
        Ok(EnsureStats {
            total_bars: merged.len(),
            ..stats
        })
    }

    /// 补缺区间规划（纯函数）：按现有 CSV 首/末根 open_time 产出升序、互不
    /// 重叠的待下载 [start, end) 区间。
    ///
    /// - 无历史 → 整段 `[target_start, now)`；
    /// - 首根晚于 `target_start` → 补头 `[target_start, first+step)`
    ///   （end 含 first 那一分钟，重叠值由 merge 以新数据覆盖）；
    /// - 末根加一个桶仍早于 `now` → 补尾 `[last, now)`；
    /// - 头尾区间重叠（本地历史不足一个桶跨度）→ 合并成整段，避免反向区间；
    /// - 已覆盖且最新 → 空 Vec（零请求）。
    fn plan_ensure_ranges(
        existing: Option<(DateTime<Utc>, DateTime<Utc>)>,
        target_start: DateTime<Utc>,
        now: DateTime<Utc>,
        step: ChronoDuration,
    ) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
        let Some((first, last)) = existing else {
            return vec![(target_start, now)];
        };
        let head = (first > target_start).then_some((target_start, first + step));
        let tail = (last + step < now).then_some((last, now));
        match (head, tail) {
            (Some((hs, he)), Some((ts, te))) if he > ts => vec![(hs, te)],
            (Some(h), Some(t)) => vec![h, t],
            (Some(h), None) => vec![h],
            (None, Some(t)) => vec![t],
            (None, None) => Vec::new(),
        }
    }

    /// 带重试的 GET；404 → `ArchiveNotFound`（不重试），其余错误重试。
    async fn get_bytes_with_retry(&self, url: &str) -> anyhow::Result<Vec<u8>> {
        let mut last: Option<anyhow::Error> = None;
        for attempt in 0..MAX_RETRIES {
            match self.client.get(url).send().await {
                Ok(r) => match r.error_for_status() {
                    Ok(r) => return Ok(r.bytes().await?.to_vec()),
                    Err(e) if e.status() == Some(reqwest::StatusCode::NOT_FOUND) => {
                        return Err(ArchiveNotFound.into());
                    }
                    Err(e) => last = Some(e.into()),
                },
                Err(e) => last = Some(e.into()),
            }
            if attempt < MAX_RETRIES - 1 {
                tokio::time::sleep(Duration::from_millis(
                    RETRY_BACKOFF_MS * (attempt as u64 + 1),
                ))
                .await;
            }
        }
        Err(last.unwrap_or_else(|| anyhow::anyhow!("GET 失败")))
    }
}

/// 现有行与新拉回行按 open_time 合并去重（同时间戳新值覆盖旧值——崩溃时
/// 残缺末根可被完整值覆盖），升序返回。
fn merge_rows(existing: Vec<Candle>, fetched: Vec<Candle>) -> Vec<Candle> {
    let mut by_time: HashMap<DateTime<Utc>, Candle> =
        existing.into_iter().map(|c| (c.open_time, c)).collect();
    for c in fetched {
        by_time.insert(c.open_time, c);
    }
    let mut merged: Vec<Candle> = by_time.into_values().collect();
    merged.sort_by_key(|c| c.open_time);
    merged
}

/// 归档块的 [start, end) 边界（404 降级 fapi 时用）
fn chunk_bounds(chunk: &Chunk, end: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    match chunk {
        Chunk::Monthly { year, month } => {
            let s = Utc.with_ymd_and_hms(*year, *month, 1, 0, 0, 0).unwrap();
            let next = next_month_start(*year, *month);
            (s, next.min(end))
        }
        Chunk::Daily(d) => {
            let s = Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap());
            (s, (s + ChronoDuration::days(1)).min(end))
        }
        Chunk::Fapi { .. } => unreachable!(),
    }
}

fn next_month_start(year: i32, month: u32) -> DateTime<Utc> {
    let (y, m) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    Utc.with_ymd_and_hms(y, m, 1, 0, 0, 0).unwrap()
}

fn archive_chunk_label(chunk: &Chunk) -> String {
    match chunk {
        Chunk::Monthly { year, month } => format!("monthly {year:04}-{month:02}"),
        Chunk::Daily(d) => format!("daily {d}"),
        Chunk::Fapi { .. } => "fapi".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_chunks_full_months() {
        // 整月缺口 → 全 monthly
        let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
        assert_eq!(
            Downloader::plan_chunks(start, end),
            vec![
                Chunk::Monthly {
                    year: 2026,
                    month: 1
                },
                Chunk::Monthly {
                    year: 2026,
                    month: 2
                },
            ]
        );
    }

    #[test]
    fn plan_chunks_mixed() {
        // 零头 fapi → 整天 daily → 尾巴 fapi
        let start = Utc.with_ymd_and_hms(2026, 7, 15, 3, 18, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 7, 17, 10, 0, 0).unwrap();
        assert_eq!(
            Downloader::plan_chunks(start, end),
            vec![
                Chunk::Fapi {
                    start_ms: start.timestamp_millis(),
                    end_ms: Utc
                        .with_ymd_and_hms(2026, 7, 16, 0, 0, 0)
                        .unwrap()
                        .timestamp_millis()
                },
                Chunk::Daily(NaiveDate::from_ymd_opt(2026, 7, 16).unwrap()),
                Chunk::Fapi {
                    start_ms: Utc
                        .with_ymd_and_hms(2026, 7, 17, 0, 0, 0)
                        .unwrap()
                        .timestamp_millis(),
                    end_ms: end.timestamp_millis()
                },
            ]
        );
    }

    #[test]
    fn plan_chunks_today_tail_is_fapi() {
        // 当天的整天不走 daily（归档未发布），落 fapi
        let start = Utc.with_ymd_and_hms(2026, 9, 8, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 9, 8, 12, 0, 0).unwrap();
        assert_eq!(
            Downloader::plan_chunks(start, end),
            vec![Chunk::Fapi {
                start_ms: start.timestamp_millis(),
                end_ms: end.timestamp_millis()
            }]
        );
    }

    #[test]
    fn archive_url_layout() {
        let dl = Downloader::new();
        assert_eq!(
            dl.archive_url(
                "btcusdt",
                "1m",
                &Chunk::Monthly { year: 2026, month: 7 }
            ),
            "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-07.zip"
        );
        assert_eq!(
            dl.archive_url("BTCUSDT", "1m", &Chunk::Daily(NaiveDate::from_ymd_opt(2026, 7, 16).unwrap())),
            "https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-07-16.zip"
        );
    }

    fn dt(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, 0).unwrap()
    }

    // plan_ensure_ranges 的固定参照：now=2026-09-18，目标起点=一年前
    fn ensure_fixture() -> (DateTime<Utc>, DateTime<Utc>, ChronoDuration) {
        (
            dt(2025, 9, 18, 0, 0),
            dt(2026, 9, 18, 0, 0),
            ChronoDuration::minutes(1),
        )
    }

    #[test]
    fn plan_ensure_no_existing_downloads_full_year() {
        let (target, now, step) = ensure_fixture();
        assert_eq!(
            Downloader::plan_ensure_ranges(None, target, now, step),
            vec![(target, now)]
        );
    }

    #[test]
    fn plan_ensure_head_gap_tail_fresh_downloads_head_only() {
        // 首根晚于一年前（头缺）；末根+1 根 == now（尾部已是最新）
        let (target, now, step) = ensure_fixture();
        let first = dt(2026, 3, 1, 0, 0);
        let last = dt(2026, 9, 17, 23, 59);
        assert_eq!(
            Downloader::plan_ensure_ranges(Some((first, last)), target, now, step),
            vec![(target, first + step)]
        );
    }

    #[test]
    fn plan_ensure_head_and_tail_gaps_are_two_ranges() {
        let (target, now, step) = ensure_fixture();
        let first = dt(2025, 12, 1, 0, 0);
        let last = dt(2026, 9, 10, 0, 0);
        assert_eq!(
            Downloader::plan_ensure_ranges(Some((first, last)), target, now, step),
            vec![(target, first + step), (last, now)]
        );
    }

    #[test]
    fn plan_ensure_tail_gap_with_old_history_downloads_tail_only() {
        // 首根早于一年前（头不缺），末根陈旧 → 仅补尾（重叠末根）
        let (target, now, step) = ensure_fixture();
        let first = dt(2025, 9, 1, 0, 0);
        let last = dt(2026, 9, 1, 0, 0);
        assert_eq!(
            Downloader::plan_ensure_ranges(Some((first, last)), target, now, step),
            vec![(last, now)]
        );
    }

    #[test]
    fn plan_ensure_overlapping_gaps_merge_into_full_range() {
        // 本地只有一根：头区间与尾区间重叠 → 合并成整段，避免退化出反向区间
        let (target, now, step) = ensure_fixture();
        let only = dt(2026, 9, 1, 0, 0);
        assert_eq!(
            Downloader::plan_ensure_ranges(Some((only, only)), target, now, step),
            vec![(target, now)]
        );
    }

    #[test]
    fn plan_ensure_covered_and_fresh_is_empty() {
        // 首根恰好等于目标起点、尾部最新 → 不产生任何下载
        let (target, now, step) = ensure_fixture();
        let first = target;
        let last = dt(2026, 9, 17, 23, 59);
        assert_eq!(
            Downloader::plan_ensure_ranges(Some((first, last)), target, now, step),
            vec![]
        );

        // 首根早于目标起点同样不算头缺
        let first_old = dt(2025, 9, 1, 0, 0);
        assert_eq!(
            Downloader::plan_ensure_ranges(Some((first_old, last)), target, now, step),
            vec![]
        );
    }

    fn mk_candle(t: DateTime<Utc>, close: f64) -> Candle {
        Candle {
            open_time: t,
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
    fn merge_rows_dedups_overrides_and_sorts() {
        let existing = vec![
            mk_candle(dt(2026, 9, 1, 0, 0), 10.0),
            mk_candle(dt(2026, 9, 2, 0, 0), 20.0),
        ];
        // 乱序 + 重叠时间戳（残缺末根用新值 99.0 覆盖）+ 一根新增
        let fetched = vec![
            mk_candle(dt(2026, 9, 3, 0, 0), 30.0),
            mk_candle(dt(2026, 9, 2, 0, 0), 99.0),
        ];
        let merged = merge_rows(existing, fetched);
        assert_eq!(merged.len(), 3);
        assert!(merged.windows(2).all(|w| w[0].open_time < w[1].open_time));
        assert_eq!(merged[1].close, 99.0, "同 open_time 新值覆盖旧值");
        assert_eq!(merged[2].close, 30.0);
    }

    #[test]
    fn parse_zip_with_and_without_header() {
        // 带表头（新归档）与无表头（旧归档）都能解析
        let build_zip = |content: &[u8]| {
            let mut buf = std::io::Cursor::new(Vec::new());
            {
                let mut zw = zip::ZipWriter::new(&mut buf);
                zw.start_file("test.csv", zip::write::SimpleFileOptions::default())
                    .unwrap();
                std::io::Write::write_all(&mut zw, content).unwrap();
                zw.finish().unwrap();
            }
            buf.into_inner()
        };
        let header_row = b"open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n\
                          1781049600000,87463.1,87476.2,87434.0,87446.0,62.655,0,0,0,0,0,0\n";
        let bare_row = b"1781049600000,87463.1,87476.2,87434.0,87446.0,62.655,0,0,0,0,0,0\n";

        for bytes in [build_zip(header_row), build_zip(bare_row)] {
            let candles = Downloader::parse_archive_zip(&bytes).unwrap();
            assert_eq!(candles.len(), 1);
            assert_eq!(candles[0].open_time.timestamp_millis(), 1_781_049_600_000);
            assert_eq!(candles[0].close, 87446.0);
            assert!(candles[0].is_closed);
        }
    }
}
