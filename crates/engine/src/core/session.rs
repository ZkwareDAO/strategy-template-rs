//! 交易日 / 交易时段 / 会话门控。
//!
//! 现状的痛点：
//! - 旧数据层把 21:00-01:00、9:00-15:00 的窗口表**写死在源码**（该模块已退役删除），
//!   读不到 `config/contracts.yaml` 的 `session_opens/session_closes`，
//!   AU/AG 的 02:30 收盘无法表达；
//! - 交易日有两套口径：`contract::ctp_trading_day`（20:00 切日、仅周末近似，
//!   用于平今）与 `TradingCalendar::trading_day_of`（18:00 切日、有节假日表，
//!   用于日线聚合），导致同一笔单在两处可能落到不同交易日；
//! - 禁交易窗口（迁移前）在旧 `Engine::dispatch` 与旧 `RuntimeActor::drive_bar`
//!   各实现一遍（两者均已随 R4 退役删除）。
//!
//! 本模块把三者收敛为一个 object-safe 的 [`SessionRules`]：
//! 交易日只有一个权威定义，时段窗口由配置构造，门控实盘/回测共用。

use std::collections::BTreeSet;

use chrono::{
    DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveTime, TimeZone, Timelike, Utc,
};
use serde::{Deserialize, Serialize};

use crate::core::bar::Timeframe;
use crate::core::error::{Result, SpiError};

/// 强类型交易日。用 `NaiveDate` 的地方全部换它，避免「自然日」与
/// 「交易日」混用（夜盘归属、平今判定、日线切桶都依赖这个区分）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TradingDay(pub NaiveDate);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionKind {
    Day,
    Night,
    AfterHours,
}

/// 一个连续可交易的时段（左闭右开）。`trading_day` 是它归属的交易日，
/// 夜盘窗口的 trading_day 是**次日**，不是自然日。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionWindow {
    pub kind: SessionKind,
    pub trading_day: TradingDay,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl SessionWindow {
    pub fn contains(&self, t: DateTime<Utc>) -> bool {
        self.start <= t && t < self.end
    }
    pub fn minutes(&self) -> i64 {
        (self.end - self.start).num_minutes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GapReason {
    /// 日内断开（午休）：不切交易日，但必须切断 K 线桶
    LunchBreak,
    /// 收盘到下一交易日开盘
    Overnight,
    Weekend,
    Holiday,
    PreOpen,
}

/// 「中间断开」事件。crypto 永不产生；期货在午休 / 夜盘结束 / 节假日产生。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionGap {
    pub from: DateTime<Utc>,
    pub to: Option<DateTime<Utc>>,
    pub reason: GapReason,
    /// 断开是否强制闭合当前聚合桶。午休必须 true，否则 10:15 与 13:30
    /// 的 1m 会被拼进同一根小时线。
    pub closes_bar: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SessionStatus {
    Open(SessionWindow),
    Break {
        reason: GapReason,
        until: Option<DateTime<Utc>>,
    },
    Closed {
        reason: GapReason,
        next_open: Option<DateTime<Utc>>,
    },
}

impl SessionStatus {
    pub fn is_open(&self) -> bool {
        matches!(self, SessionStatus::Open(_))
    }
}

/// 交易日历 + 时段 + 门控。所有返回值都基于 UTC 时间点，
/// 时区只是把「交易所本地墙钟」翻译成 UTC 的实现细节。
pub trait SessionRules: Send + Sync {
    fn timezone(&self) -> FixedOffset;

    /// 唯一的交易日权威定义。平今判定、日线切桶、今昨仓 rollover 都调它。
    fn trading_day_of(&self, t: DateTime<Utc>) -> TradingDay;

    fn is_trading_day(&self, d: NaiveDate) -> bool;

    /// 某交易日的全部交易窗口，按 start 升序（夜盘在前）。
    fn sessions_on(&self, day: TradingDay) -> Vec<SessionWindow>;

    fn session_at(&self, t: DateTime<Utc>) -> SessionStatus;

    /// t 是否恰好是某个窗口的收尾时刻。用于无 `is_final` 字段的交易所
    /// 在时段末尾显式确认最后一根 1m 已收线。
    fn is_session_boundary(&self, t: DateTime<Utc>) -> bool;

    /// 开盘后 / 收盘前禁止交易窗口（现有 `is_no_trade_window`）。
    fn is_no_trade_window(&self, _t: DateTime<Utc>) -> bool {
        false
    }

    /// 是否允许入场确认（现有 `is_entry_confirm_time`）；crypto 恒 true。
    fn is_entry_confirm_time(&self, _t: DateTime<Utc>) -> bool {
        true
    }

    /// t 是否恰好是某个**真实开盘口**的开盘时刻（分钟对齐）。
    /// 镜像 `ContractSpec::is_session_open_time`：引擎用它记录
    /// `Ctx::session_open_price`（阶段 3 双跑必需，§9.1 桥接零翻译损耗）。
    /// crypto / 7x24 恒 false（旧引擎无合约时同样不记录）。
    fn is_session_open_time(&self, _t: DateTime<Utc>) -> bool {
        false
    }

    /// live 预热回填起点。crypto = 墙钟倒推；期货 = 按交易日窗口倒推，
    /// 不能用 `now - bars * tf`（休市时间会被算进去）。
    fn backfill_start(&self, now: DateTime<Utc>, tf: Timeframe, bars: usize) -> DateTime<Utc>;

    /// t 之后最近的断开：回放源用它发出 [`crate::core::bar::MarketEvent::SessionGap`]。
    fn gap_after(&self, t: DateTime<Utc>) -> Option<SessionGap>;
}

// ─── 参考实现 A：7x24 连续市场 ────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ContinuousSession {
    pub timezone: FixedOffset,
}

impl Default for ContinuousSession {
    fn default() -> Self {
        Self {
            timezone: FixedOffset::east_opt(0).expect("UTC"),
        }
    }
}

impl SessionRules for ContinuousSession {
    fn timezone(&self) -> FixedOffset {
        self.timezone
    }

    fn trading_day_of(&self, t: DateTime<Utc>) -> TradingDay {
        TradingDay(t.with_timezone(&self.timezone).date_naive())
    }

    fn is_trading_day(&self, _d: NaiveDate) -> bool {
        true
    }

    fn sessions_on(&self, day: TradingDay) -> Vec<SessionWindow> {
        let midnight = day
            .0
            .and_hms_opt(0, 0, 0)
            .and_then(|dt| self.timezone.from_local_datetime(&dt).single())
            .map(|dt| dt.with_timezone(&Utc));
        match midnight {
            Some(start) => vec![SessionWindow {
                kind: SessionKind::Day,
                trading_day: day,
                start,
                end: start + Duration::days(1),
            }],
            None => Vec::new(),
        }
    }

    fn session_at(&self, t: DateTime<Utc>) -> SessionStatus {
        SessionStatus::Open(SessionWindow {
            kind: SessionKind::Day,
            trading_day: self.trading_day_of(t),
            start: t,
            end: t + Duration::days(1),
        })
    }

    fn is_session_boundary(&self, _t: DateTime<Utc>) -> bool {
        false
    }

    fn backfill_start(&self, now: DateTime<Utc>, tf: Timeframe, bars: usize) -> DateTime<Utc> {
        now - Duration::minutes(bars as i64 * tf.0 as i64)
    }

    fn gap_after(&self, _t: DateTime<Utc>) -> Option<SessionGap> {
        None
    }
}

// ─── 参考实现 B：配置驱动的窗口市场（国内期货等） ──────────────────────────

/// 由配置构造，替代旧硬编码窗口表（该模块已随 D2 退役删除）。
/// `windows` 直接来自 `contracts.yaml` 的 `session_opens` / `session_closes`。
#[derive(Debug, Clone)]
pub struct WindowedSession {
    pub timezone: FixedOffset,
    /// (开盘, 收盘, 时段类型)，均为交易所本地墙钟；收盘 <= 开盘表示跨零点。
    pub windows: Vec<(NaiveTime, NaiveTime, SessionKind)>,
    /// 本地时间 >= 该值即归属下一交易日（把 18:00 / 20:00 的口径显式配置化）。
    pub day_cutover: NaiveTime,
    pub no_trade_after_open_minutes: i64,
    pub no_trade_before_close_minutes: i64,
    pub holidays: BTreeSet<NaiveDate>,
}

impl WindowedSession {
    /// 以某自然日为锚，展开所有窗口的 UTC 起止。
    fn windows_on_date(&self, d: NaiveDate) -> Vec<SessionWindow> {
        // 非交易日（周末 / 节假日）不产生任何窗口。否则周日的日盘窗口
        // 会被 trading_day_of 顺延后错误地算进周一交易日。
        if !self.is_trading_day(d) {
            return Vec::new();
        }
        let mut out = Vec::new();
        for &(open, close, kind) in &self.windows {
            let start_local = d.and_time(open).and_local_timezone(self.timezone).single();
            let Some(start_local) = start_local else {
                continue;
            };
            let mut days_span = 0i64;
            if close <= open {
                days_span = 1;
            }
            let end_local = (d + Duration::days(days_span))
                .and_time(close)
                .and_local_timezone(self.timezone)
                .single();
            let Some(end_local) = end_local else { continue };
            let start = start_local.with_timezone(&Utc);
            let end = end_local.with_timezone(&Utc);
            if end <= start {
                continue;
            }
            out.push(SessionWindow {
                kind,
                trading_day: self.trading_day_of(start),
                start,
                end,
            });
        }
        out.sort_by_key(|w| w.start);
        out
    }

    fn window_containing(&self, t: DateTime<Utc>) -> Option<SessionWindow> {
        let local_date = t.with_timezone(&self.timezone).date_naive();
        // 跨零点窗口的锚是前一日，所以查当日 + 前一日 + 前两日（覆盖长假）。
        for back in 0..=3 {
            let d = local_date - Duration::days(back);
            if let Some(w) = self.windows_on_date(d).into_iter().find(|w| w.contains(t)) {
                return Some(w);
            }
        }
        None
    }

    /// 当日「真实开收盘口」：日盘**首窗开盘**与**夜盘开/收**是开收盘口；
    /// 日盘**末窗收盘**与夜盘收盘是收盘口。午休/小节衔接点（10:15/10:30/
    /// 13:30 等）**不是**——对齐 `ContractSpec::session_opens/session_closes`
    /// 的登记口径与交易所集合竞价事实。
    /// v2.6（阶段 3 双跑修正）：旧实现把每个窗口端点都当边界，会在午休
    /// 衔接点误开 5 分钟禁交易窗，与旧引擎逐笔必分叉。
    fn real_boundaries(&self, t: DateTime<Utc>) -> (Vec<DateTime<Utc>>, Vec<DateTime<Utc>>) {
        let windows = self.sessions_on(self.trading_day_of(t));
        let mut opens: Vec<DateTime<Utc>> = Vec::new();
        let mut closes: Vec<DateTime<Utc>> = Vec::new();
        for w in &windows {
            if w.kind != SessionKind::Day {
                // 夜盘/盘后：两端都是真实开收盘口
                opens.push(w.start);
                closes.push(w.end);
            }
        }
        if let Some(first) = windows.iter().find(|w| w.kind == SessionKind::Day) {
            opens.push(first.start);
        }
        if let Some(last) = windows.iter().rev().find(|w| w.kind == SessionKind::Day) {
            closes.push(last.end);
        }
        (opens, closes)
    }
}

impl SessionRules for WindowedSession {
    fn timezone(&self) -> FixedOffset {
        self.timezone
    }

    fn is_trading_day(&self, d: NaiveDate) -> bool {
        use chrono::Weekday;
        if matches!(d.weekday(), Weekday::Sat | Weekday::Sun) {
            return false;
        }
        !self.holidays.contains(&d)
    }

    fn trading_day_of(&self, t: DateTime<Utc>) -> TradingDay {
        let local = t.with_timezone(&self.timezone);
        let mut d = local.date_naive();
        if local.time() >= self.day_cutover {
            d += Duration::days(1);
        }
        // 非交易日顺延到下一交易日（节假日表由此生效）。
        for _ in 0..15 {
            if self.is_trading_day(d) {
                return TradingDay(d);
            }
            d += Duration::days(1);
        }
        TradingDay(d)
    }

    fn sessions_on(&self, day: TradingDay) -> Vec<SessionWindow> {
        // 归属该交易日的窗口可能锚在当日或前一日（夜盘），故扫描前 3 天。
        let mut out = Vec::new();
        for back in 0..=3 {
            let d = day.0 - Duration::days(back);
            out.extend(
                self.windows_on_date(d)
                    .into_iter()
                    .filter(|w| w.trading_day == day),
            );
        }
        out.sort_by_key(|w| w.start);
        out.dedup_by_key(|w| w.start);
        out
    }

    fn session_at(&self, t: DateTime<Utc>) -> SessionStatus {
        if let Some(w) = self.window_containing(t) {
            return SessionStatus::Open(w);
        }
        let day = self.trading_day_of(t);
        let windows = self.sessions_on(day);
        let next = windows.iter().find(|w| w.start > t).map(|w| w.start);
        let reason = if self.is_trading_day(t.with_timezone(&self.timezone).date_naive()) {
            GapReason::LunchBreak
        } else {
            GapReason::Weekend
        };
        SessionStatus::Break {
            reason,
            until: next,
        }
    }

    fn is_session_boundary(&self, t: DateTime<Utc>) -> bool {
        let local_date = t.with_timezone(&self.timezone).date_naive();
        (0..=3).any(|back| {
            let d = local_date - Duration::days(back);
            self.windows_on_date(d).iter().any(|w| w.end == t)
        })
    }

    fn is_no_trade_window(&self, t: DateTime<Utc>) -> bool {
        // v2.6：只看真实开收盘口（午休端点不拦——与旧引擎 contract 口径一致）。
        // 区间端点逐项镜像 `ContractSpec::is_no_trade_window`：
        // 开盘后 [open, open+N)；收盘前 [close-N, close]（闭端）。
        let (opens, closes) = self.real_boundaries(t);
        if self.no_trade_after_open_minutes > 0 {
            for o in &opens {
                let end = *o + Duration::minutes(self.no_trade_after_open_minutes);
                if t >= *o && t < end {
                    return true;
                }
            }
        }
        if self.no_trade_before_close_minutes > 0 {
            for c in &closes {
                let start = *c - Duration::minutes(self.no_trade_before_close_minutes);
                if t >= start && t <= *c {
                    return true;
                }
            }
        }
        false
    }

    fn is_entry_confirm_time(&self, t: DateTime<Utc>) -> bool {
        // 镜像 `ContractSpec::is_entry_confirm_time`：开盘 + N 分钟的
        // 精确一分钟脉冲（不是全程 true）——双跑桥接零翻译损耗的前提。
        let (opens, _) = self.real_boundaries(t);
        let mins = self.no_trade_after_open_minutes;
        opens.into_iter().any(|o| {
            let confirm = o + Duration::minutes(mins);
            t >= confirm && t < confirm + Duration::minutes(1)
        })
    }

    fn is_session_open_time(&self, t: DateTime<Utc>) -> bool {
        let (opens, _) = self.real_boundaries(t);
        opens
            .into_iter()
            .any(|o| t >= o && t < o + Duration::minutes(1))
    }

    fn backfill_start(&self, now: DateTime<Utc>, tf: Timeframe, bars: usize) -> DateTime<Utc> {
        // 精确做法应逐窗口倒推；这里按「每日可交易分钟数」估算交易日数，
        // 再取该交易日首个窗口的起点，避免把休市时间当成可回填 bar。
        let need_minutes = bars as i64 * tf.0 as i64;
        let per_day: i64 = self
            .windows
            .iter()
            .map(|(o, c, _)| {
                let mut mins = (c.signed_duration_since(*o)).num_minutes();
                if mins <= 0 {
                    mins += 24 * 60;
                }
                mins
            })
            .sum::<i64>()
            .max(1);
        let days = (need_minutes / per_day + 2).max(1);
        let mut d = self.trading_day_of(now).0;
        let mut counted = 0;
        while counted < days {
            d -= Duration::days(1);
            if self.is_trading_day(d) {
                counted += 1;
            }
        }
        self.sessions_on(TradingDay(d))
            .first()
            .map(|w| w.start)
            .unwrap_or(now - Duration::minutes(need_minutes))
    }

    fn gap_after(&self, t: DateTime<Utc>) -> Option<SessionGap> {
        let cur = self.window_containing(t)?;
        let day_windows = self.sessions_on(cur.trading_day);
        let next_same_day = day_windows.iter().find(|w| w.start >= cur.end);
        if let Some(next) = next_same_day {
            return Some(SessionGap {
                from: cur.end,
                to: Some(next.start),
                reason: GapReason::LunchBreak,
                closes_bar: true,
            });
        }
        let mut d = cur.trading_day.0 + Duration::days(1);
        for _ in 0..15 {
            if self.is_trading_day(d) {
                break;
            }
            d += Duration::days(1);
        }
        let next_open = self.sessions_on(TradingDay(d)).first().map(|w| w.start);
        Some(SessionGap {
            from: cur.end,
            to: next_open,
            reason: GapReason::Overnight,
            closes_bar: true,
        })
    }
}

/// 便捷构造：UTC+8 的国内期货默认时段（可由 `contracts.yaml` 覆盖）。
pub fn cn_futures_default() -> WindowedSession {
    let hms = |h, m| NaiveTime::from_hms_opt(h, m, 0).expect("valid time");
    WindowedSession {
        timezone: FixedOffset::east_opt(8 * 3600).expect("UTC+8"),
        windows: vec![
            (hms(9, 0), hms(10, 15), SessionKind::Day),
            (hms(10, 30), hms(11, 30), SessionKind::Day),
            (hms(13, 30), hms(15, 0), SessionKind::Day),
            (hms(21, 0), hms(1, 0), SessionKind::Night),
        ],
        day_cutover: hms(20, 0),
        no_trade_after_open_minutes: 5,
        no_trade_before_close_minutes: 5,
        holidays: BTreeSet::new(),
    }
}

// ─── 阶段 2（v2.5）：配置驱动时段表（§8 `exchanges.*.session(s)`） ──────────

/// [`WindowedSession`] 的 serde 输入形态。与运行时结构分离：
/// 配置口径是「整小时偏移 + 字符串时刻 + 小写时段名」，运行时是
/// `FixedOffset + NaiveTime + SessionKind`，转换只在装配期发生一次。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SessionCfg {
    /// UTC 偏移小时（境内 = 8）。
    pub timezone_offset_hours: i32,
    /// 窗口列表；`close <= open` 表示跨零点（如夜盘 21:00→01:00）。
    pub windows: Vec<SessionWindowCfg>,
    /// 本地时间 >= 该时刻归属下一交易日（把 18:00/20:00 口径显式化）。
    pub day_cutover: String,
    pub no_trade_after_open_minutes: i64,
    pub no_trade_before_close_minutes: i64,
    /// 节假日，格式 `YYYY-MM-DD`（v2 §14-4：外部日历可注入的落点）。
    pub holidays: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionWindowCfg {
    /// `HH:MM` 本地墙钟。
    pub open: String,
    pub close: String,
    /// `day` | `night` | `after_hours`。
    pub kind: String,
}

impl Default for SessionCfg {
    /// 缺省即 `cn_futures_default()` 的序列化形态。
    fn default() -> Self {
        let kind_name = |k: SessionKind| match k {
            SessionKind::Day => "day",
            SessionKind::Night => "night",
            SessionKind::AfterHours => "after_hours",
        };
        let fmt_t = |t: NaiveTime| format!("{:02}:{:02}", t.hour(), t.minute());
        Self {
            timezone_offset_hours: 8,
            windows: cn_futures_default()
                .windows
                .into_iter()
                .map(|(o, c, k)| SessionWindowCfg {
                    open: fmt_t(o),
                    close: fmt_t(c),
                    kind: kind_name(k).into(),
                })
                .collect(),
            day_cutover: "20:00".into(),
            no_trade_after_open_minutes: 5,
            no_trade_before_close_minutes: 5,
            holidays: Vec::new(),
        }
    }
}

impl SessionCfg {
    /// 配置 → 运行时时段表。非法时刻/时区/时段名在装配期报错，不留到盘中。
    pub fn to_windowed(&self) -> Result<WindowedSession> {
        let timezone =
            FixedOffset::east_opt(self.timezone_offset_hours * 3600).ok_or_else(|| {
                SpiError::Config(format!("非法时区偏移: {}", self.timezone_offset_hours))
            })?;
        let parse_t = |s: &str| -> Result<NaiveTime> {
            NaiveTime::parse_from_str(s, "%H:%M")
                .map_err(|e| SpiError::Config(format!("非法时刻 {s}: {e}")))
        };
        let mut windows = Vec::with_capacity(self.windows.len());
        for w in &self.windows {
            let kind = match w.kind.to_ascii_lowercase().as_str() {
                "day" => SessionKind::Day,
                "night" => SessionKind::Night,
                "after_hours" => SessionKind::AfterHours,
                other => return Err(SpiError::Config(format!("非法时段类型: {other}"))),
            };
            windows.push((parse_t(&w.open)?, parse_t(&w.close)?, kind));
        }
        let mut holidays = BTreeSet::new();
        for h in &self.holidays {
            holidays.insert(
                NaiveDate::parse_from_str(h, "%Y-%m-%d")
                    .map_err(|e| SpiError::Config(format!("非法节假日 {h}: {e}")))?,
            );
        }
        Ok(WindowedSession {
            timezone,
            windows,
            day_cutover: parse_t(&self.day_cutover)?,
            no_trade_after_open_minutes: self.no_trade_after_open_minutes,
            no_trade_before_close_minutes: self.no_trade_before_close_minutes,
            holidays,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};

    fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
    }

    #[test]
    fn windowed_session_backfill_skips_closed_hours() {
        let s: WindowedSession = cn_futures_default();
        let now = utc(2026, 1, 5, 7, 0); // 北京 15:00 收盘
        let start = s.backfill_start(now, Timeframe::M1, 300);
        assert!(
            start < now - chrono::Duration::hours(5),
            "回填起点必须跨过休市，不能只倒推 300 分钟"
        );
    }

    /// 阶段 2：缺省 SessionCfg 必须无损还原 cn_futures_default 的窗口表，
    /// 且非法字符串在装配期报错（不留到盘中炸）。
    #[test]
    fn session_cfg_roundtrips_cn_default_and_rejects_garbage() {
        let cfg = SessionCfg::default();
        let built = cfg.to_windowed().expect("默认配置必须可构造");
        let cn = cn_futures_default();
        assert_eq!(built.windows, cn.windows);
        assert_eq!(built.day_cutover, cn.day_cutover);
        assert_eq!(built.timezone, cn.timezone);
        // 跨零点夜盘 + 显式节假日
        let cfg2 = SessionCfg {
            windows: vec![SessionWindowCfg {
                open: "21:00".into(),
                close: "02:30".into(),
                kind: "night".into(),
            }],
            holidays: vec!["2026-10-01".into()],
            ..Default::default()
        };
        let s2 = cfg2.to_windowed().unwrap();
        assert_eq!(
            s2.windows[0],
            (hms2(21, 0), hms2(2, 30), SessionKind::Night)
        );
        assert!(s2
            .holidays
            .contains(&NaiveDate::from_ymd_opt(2026, 10, 1).unwrap()));
        // 非法输入
        let bad = SessionCfg {
            windows: vec![SessionWindowCfg {
                open: "25:00".into(),
                close: "01:00".into(),
                kind: "night".into(),
            }],
            ..Default::default()
        };
        assert!(bad.to_windowed().is_err(), "非法时刻必须在装配期报错");
        let bad_kind = SessionCfg {
            windows: vec![SessionWindowCfg {
                open: "09:00".into(),
                close: "15:00".into(),
                kind: "midnight".into(),
            }],
            ..Default::default()
        };
        assert!(bad_kind.to_windowed().is_err());
    }

    /// v2.6 双跑修正回归：禁交易/确认/开盘脉冲只认**真实开收盘口**。
    /// 旧实现把午休衔接点（10:15/10:30/13:30）也当边界，与旧引擎
    /// contract 口径（opens=["09:00","21:00"]、closes=["15:00","01:00"]）
    /// 逐笔必分叉；AU 式夜盘收盘 02:30 属于夜盘端点，必须仍被拦截。
    #[test]
    fn no_trade_and_confirm_only_at_real_boundaries() {
        let s = cn_futures_default();
        // 日盘开盘后 5 分钟拦（北京 09:00-09:05 = UTC 01:00-01:05）
        assert!(s.is_no_trade_window(utc(2026, 1, 5, 1, 0)));
        assert!(s.is_no_trade_window(utc(2026, 1, 5, 1, 4)));
        assert!(!s.is_no_trade_window(utc(2026, 1, 5, 1, 5)));
        // 午休两端不拦（旧实现在这里误拦）
        assert!(!s.is_no_trade_window(utc(2026, 1, 5, 2, 11))); // 10:11（10:15 午休前）
        assert!(!s.is_no_trade_window(utc(2026, 1, 5, 2, 14))); // 10:14
        assert!(!s.is_no_trade_window(utc(2026, 1, 5, 2, 31))); // 10:31（午休后）
        assert!(!s.is_no_trade_window(utc(2026, 1, 5, 5, 31))); // 13:31（下午盘初）
                                                                // 日盘收盘前 5 分钟拦（14:55-15:00 含两端点）
        assert!(s.is_no_trade_window(utc(2026, 1, 5, 6, 55)));
        assert!(s.is_no_trade_window(utc(2026, 1, 5, 7, 0))); // 15:00 恰收盘（闭端）
        assert!(!s.is_no_trade_window(utc(2026, 1, 5, 6, 54)));
        // 夜盘开盘 21:00 拦（周五夜盘归周一交易日）
        assert!(s.is_no_trade_window(utc(2026, 1, 2, 13, 1))); // 北京周五 21:01
                                                               // 夜盘收盘 01:00 前拦（周五夜盘尾 = 北京周六 00:57，归周一交易日）
        assert!(s.is_no_trade_window(utc(2026, 1, 2, 16, 57))); // UTC 周六 00:57(北京)
                                                                // entry_confirm：开盘+5 的精确分钟脉冲，其余为 false
        assert!(s.is_entry_confirm_time(utc(2026, 1, 5, 1, 5))); // 09:05
        assert!(!s.is_entry_confirm_time(utc(2026, 1, 5, 1, 6)));
        assert!(!s.is_entry_confirm_time(utc(2026, 1, 5, 2, 35))); // 10:35 不是确认口
                                                                   // session_open_time：真实开盘分钟（引擎 session_open_price 的记录点）
        assert!(s.is_session_open_time(utc(2026, 1, 5, 1, 0))); // 09:00
        assert!(!s.is_session_open_time(utc(2026, 1, 5, 2, 30))); // 10:30 不是
        assert!(s.is_session_open_time(utc(2026, 1, 2, 13, 0))); // 21:00 夜盘开盘
                                                                 // AU 型夜盘（02:30 收盘）：夜盘端点语义随窗口走
        let au = WindowedSession {
            windows: s
                .windows
                .iter()
                .map(|(o, c, k)| {
                    if *k == SessionKind::Night {
                        (*o, hms2(2, 30), *k)
                    } else {
                        (*o, *c, *k)
                    }
                })
                .collect(),
            ..s.clone()
        };
        assert!(au.is_no_trade_window(utc(2026, 1, 2, 18, 26))); // 北京 02:26
        assert!(au.is_no_trade_window(utc(2026, 1, 2, 18, 30))); // 02:30 闭端
        assert!(!au.is_no_trade_window(utc(2026, 1, 2, 18, 31))); // 窗口外
    }

    fn hms2(h: u32, m: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(h, m, 0).unwrap()
    }
}
