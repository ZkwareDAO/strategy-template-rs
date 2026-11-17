//! 国内期货合约参数（回测撮合用）
//!
//! 由配置文件 `config/contracts.yaml` 提供，回测撮合直接读取：
//! - `multiplier`：合约乘数（1 手对应的标的数量，如铜 5 吨/手）
//! - `tick_size`：最小变动价位（用于校验/展示，价格本身来自行情）
//! - `margin_rate`：交易所保证金率（0.11 = 11%）
//! - `commission`：开仓/平仓/平今手续费（比例或每手固定）
//!
//! 期货开仓手数必须为整数，且占用的保证金不得超过账户权益。

use chrono::{DateTime, Datelike, Duration, FixedOffset, Timelike, Utc, Weekday};
use serde::Deserialize;

/// 手续费计费方式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeMode {
    /// 按成交金额比例（如 0.00005 = 万分之 0.5）
    Rate,
    /// 按手数固定收费（元/手）
    PerLot,
}

/// 手续费收取时点
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeKind {
    Open,
    Close,
    CloseToday,
}

/// 手续费配置
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommissionSpec {
    pub mode: FeeMode,
    /// 开仓手续费（rate 模式为比例，per_lot 模式为元/手）
    pub open: f64,
    /// 平昨仓手续费（rate 模式为比例，per_lot 模式为元/手）
    pub close: f64,
    /// 平今仓手续费；缺省 = `close`
    #[serde(default)]
    pub close_today: Option<f64>,
}

impl CommissionSpec {
    /// 计算一笔成交的手续费。
    /// - `notional`：成交金额（价格 × 乘数 × 手数）
    /// - `lots`：手数
    pub fn fee(&self, notional: f64, lots: f64, kind: FeeKind) -> f64 {
        let value = match kind {
            FeeKind::Open => self.open,
            FeeKind::Close => self.close,
            FeeKind::CloseToday => self.close_today.unwrap_or(self.close),
        };
        match self.mode {
            FeeMode::Rate => notional * value,
            FeeMode::PerLot => lots * value,
        }
    }
}

fn default_min_lots() -> u64 {
    1
}

fn default_no_trade_minutes() -> u32 {
    5
}

fn default_no_trade_after_open_minutes() -> u32 {
    5
}

/// 合约参数
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractSpec {
    /// 品种中文名（仅用于报告/日志）
    #[serde(default)]
    pub name: Option<String>,
    /// 合约乘数（1 手对应的标的数量）
    pub multiplier: f64,
    /// 最小变动价位
    pub tick_size: f64,
    /// 保证金率（0.11 = 11%）
    pub margin_rate: f64,
    /// 最小开仓手数（缺省 1）
    #[serde(default = "default_min_lots")]
    pub min_lots: u64,
    /// 每日/夜盘开盘时刻（北京时间 HH:MM，如 ["09:00", "21:00"]）
    #[serde(default)]
    pub session_opens: Vec<String>,
    /// 开盘后禁止交易的分钟数（缺省 5；0 = 关闭）
    #[serde(default = "default_no_trade_after_open_minutes")]
    pub no_trade_after_open_minutes: u32,
    /// 每日/夜盘收盘时刻（北京时间 HH:MM，如 ["15:00", "01:00"]）
    #[serde(default)]
    pub session_closes: Vec<String>,
    /// 收盘前禁止交易的分钟数（缺省 5；0 = 关闭）
    #[serde(default = "default_no_trade_minutes")]
    pub no_trade_before_close_minutes: u32,
    /// 手续费
    pub commission: CommissionSpec,
}

impl ContractSpec {
    /// 参数合法性校验（配置错误 fail-fast）
    pub fn validate(&self) -> Result<(), String> {
        if !self.multiplier.is_finite() || self.multiplier <= 0.0 {
            return Err(format!("multiplier 必须 > 0，实际 {}", self.multiplier));
        }
        if !self.tick_size.is_finite() || self.tick_size <= 0.0 {
            return Err(format!("tick_size 必须 > 0，实际 {}", self.tick_size));
        }
        if !self.margin_rate.is_finite() || self.margin_rate <= 0.0 || self.margin_rate > 1.0 {
            return Err(format!(
                "margin_rate 必须在 (0, 1] 区间，实际 {}",
                self.margin_rate
            ));
        }
        if !self.commission.open.is_finite()
            || !self.commission.close.is_finite()
            || !self.commission.close_today.unwrap_or(0.0).is_finite()
            || self.commission.open < 0.0
            || self.commission.close < 0.0
            || self.commission.close_today.unwrap_or(0.0) < 0.0
        {
            return Err("手续费必须是有限非负数".into());
        }
        if self.no_trade_before_close_minutes > 120 {
            return Err("no_trade_before_close_minutes 不能超过 120".into());
        }
        if self.no_trade_after_open_minutes > 120 {
            return Err("no_trade_after_open_minutes 不能超过 120".into());
        }
        for s in &self.session_opens {
            parse_hhmm(s).ok_or_else(|| format!("session_opens 非法时刻: {s:?}（期望 HH:MM）"))?;
        }
        for s in &self.session_closes {
            parse_hhmm(s).ok_or_else(|| format!("session_closes 非法时刻: {s:?}（期望 HH:MM）"))?;
        }
        Ok(())
    }

    /// 是否恰为日盘/夜盘开盘时刻（HH:MM 精确到分钟）。
    pub fn is_session_open_time(&self, t: DateTime<Utc>) -> bool {
        if self.session_opens.is_empty() {
            return false;
        }
        let cst = FixedOffset::east_opt(8 * 3600).expect("UTC+8");
        let local = t.with_timezone(&cst).naive_local();
        for raw in &self.session_opens {
            let Some((h, m)) = parse_hhmm(raw) else {
                continue;
            };
            for offset in -1..=1 {
                let date = local.date() + Duration::days(offset);
                if let Some(open) = date.and_hms_opt(h, m, 0) {
                    if local >= open && local < open + Duration::minutes(1) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// 是否到达“开盘后 N 分钟”的信号再确认时点。
    /// 无合约/未配置 session_opens 时返回 true（crypto 兼容）。
    pub fn is_entry_confirm_time(&self, t: DateTime<Utc>) -> bool {
        if self.session_opens.is_empty() {
            return true;
        }
        let cst = FixedOffset::east_opt(8 * 3600).expect("UTC+8");
        let local = t.with_timezone(&cst).naive_local();
        let minutes = self.no_trade_after_open_minutes as i64;
        for raw in &self.session_opens {
            let Some((h, m)) = parse_hhmm(raw) else {
                continue;
            };
            for offset in -1..=1 {
                let date = local.date() + Duration::days(offset);
                let Some(open) = date.and_hms_opt(h, m, 0) else {
                    continue;
                };
                let confirm = open + Duration::minutes(minutes);
                if local >= confirm && local < confirm + Duration::minutes(1) {
                    return true;
                }
            }
        }
        false
    }

    /// 当前时刻是否处于“收盘前 N 分钟”或“开盘后 N 分钟”禁交易窗口。
    ///
    /// `session_opens` / `session_closes` 只填每日/夜盘的真正开盘、收盘时刻，
    /// 例如 CU：开盘 09:00、21:00，收盘 15:00、01:00。
    /// 窗口按自然日 + 前后一天匹配，夜盘跨零点也能正确判断。
    pub fn is_no_trade_window(&self, t: DateTime<Utc>) -> bool {
        let before_close = self.no_trade_before_close_minutes;
        let after_open = self.no_trade_after_open_minutes;
        if before_close == 0 && after_open == 0 {
            return false;
        }
        let cst = FixedOffset::east_opt(8 * 3600).expect("UTC+8");
        let local = t.with_timezone(&cst).naive_local();

        if before_close > 0 {
            for raw in &self.session_closes {
                let Some((h, m)) = parse_hhmm(raw) else {
                    continue;
                };
                for offset in -1..=1 {
                    let date = local.date() + Duration::days(offset);
                    let Some(close) = date.and_hms_opt(h, m, 0) else {
                        continue;
                    };
                    let window_start = close - Duration::minutes(before_close as i64);
                    if local >= window_start && local <= close {
                        return true;
                    }
                }
            }
        }

        if after_open > 0 {
            for raw in &self.session_opens {
                let Some((h, m)) = parse_hhmm(raw) else {
                    continue;
                };
                for offset in -1..=1 {
                    let date = local.date() + Duration::days(offset);
                    let Some(open) = date.and_hms_opt(h, m, 0) else {
                        continue;
                    };
                    let window_end = open + Duration::minutes(after_open as i64);
                    if local >= open && local < window_end {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// 开仓整数手数（回测撮合与 mock/paper 共用的唯一口径）。
    ///
    /// - `price`：开仓成交价；`equity`：可用权益（回测传入 `cash`）；`leverage`：名义敞口上限
    /// - 同时受两个上限约束，取较小值并向下取整（1e-9 容差吸收浮点误差）：
    ///   - 保证金：`手数 × (每手保证金 + 开仓手续费) ≤ 权益`，每手保证金 = 价格×乘数×保证金率
    ///   - 杠杆：`手数 × 每手名义金额 ≤ 权益 × leverage`（leverage 0 按 1 处理）
    /// - 结果 < `max(min_lots, 1)` 时返回 `None`（一手都开不起，调用方应放弃开仓）
    pub fn sizing_lots(&self, price: f64, equity: f64, leverage: u32) -> Option<u64> {
        let lots = self.max_lots(price, equity, leverage);
        if lots < self.min_lots.max(1) {
            None
        } else {
            Some(lots)
        }
    }

    fn max_lots(&self, price: f64, equity: f64, leverage: u32) -> u64 {
        if !equity.is_finite() || equity <= 0.0 || !price.is_finite() || price <= 0.0 {
            return 0;
        }
        let notional_per_lot = price * self.multiplier;
        let margin_per_lot = notional_per_lot * self.margin_rate;
        if margin_per_lot <= 0.0 {
            return 0;
        }
        // 手续费对“每手”是线性的（rate 按名义额、per_lot 按手数），
        // 因此可直接用「每手总成本 = 保证金 + 开仓手续费」求整数手。
        let fee_per_lot = self.commission.fee(notional_per_lot, 1.0, FeeKind::Open);
        let cost_per_lot = margin_per_lot + fee_per_lot;
        if cost_per_lot <= 0.0 {
            return 0;
        }
        let margin_lots = (equity / cost_per_lot + 1e-9).floor().max(0.0) as u64;
        // leverage 是名义敞口上限（1 = 不放大），保证金率是交易所允许的上限
        let max_notional = equity * leverage.max(1) as f64;
        let leverage_lots = (max_notional / notional_per_lot + 1e-9).floor().max(0.0) as u64;
        margin_lots.min(leverage_lots)
    }
}

/// 一笔持仓在 CTP 口径下的交易日（北京时区，近似处理周末）。
///
/// 夜盘（20:00 起）归属下一交易日；周五夜盘顺延至下周一。
/// 节假日/临时休市无法在不引入交易日历的情况下判断，按自然周近似。
pub fn ctp_trading_day(t: DateTime<Utc>) -> chrono::NaiveDate {
    let cst = FixedOffset::east_opt(8 * 3600).expect("UTC+8");
    let local = t.with_timezone(&cst);
    let mut day = local.date_naive();
    if local.hour() >= 20 {
        if let Some(next) = day.succ_opt() {
            day = next;
        }
    }
    while matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
        match day.succ_opt() {
            Some(next) => day = next,
            None => break,
        }
    }
    day
}

fn parse_hhmm(s: &str) -> Option<(u32, u32)> {
    let (h, m) = s.trim().split_once(':')?;
    let h: u32 = h.parse().ok()?;
    let m: u32 = m.parse().ok()?;
    if h < 24 && m < 60 {
        Some((h, m))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ContractSpec {
        ContractSpec {
            name: Some("铜".into()),
            multiplier: 5.0,
            tick_size: 10.0,
            margin_rate: 0.1,
            min_lots: 1,
            session_opens: vec![],
            no_trade_after_open_minutes: 5,
            session_closes: vec![],
            no_trade_before_close_minutes: 5,
            commission: CommissionSpec {
                mode: FeeMode::Rate,
                open: 0.00005,
                close: 0.00005,
                close_today: Some(0.000025),
            },
        }
    }

    #[test]
    fn rate_fee_uses_notional_and_close_today_override() {
        let c = spec();
        // 100 × 5 × 2 手 = 1000 名义
        let open = c.commission.fee(1000.0, 2.0, FeeKind::Open);
        assert!((open - 0.05).abs() < 1e-12);
        let close = c.commission.fee(1000.0, 2.0, FeeKind::Close);
        assert!((close - 0.05).abs() < 1e-12);
        let today = c.commission.fee(1000.0, 2.0, FeeKind::CloseToday);
        assert!((today - 0.025).abs() < 1e-12);
    }

    #[test]
    fn per_lot_fee_uses_lots() {
        let mut c = spec();
        c.commission = CommissionSpec {
            mode: FeeMode::PerLot,
            open: 20.0,
            close: 20.0,
            close_today: Some(10.0),
        };
        assert!((c.commission.fee(999_999.0, 3.0, FeeKind::Open) - 60.0).abs() < 1e-12);
        assert!((c.commission.fee(999_999.0, 3.0, FeeKind::CloseToday) - 30.0).abs() < 1e-12);
    }

    #[test]
    fn sizing_lots_are_integer_and_cover_margin_plus_fee() {
        let c = spec();
        // leverage=10：每手名义 500，保证金 50，手续费 0.025
        // 权益 1060 → 保证金上限 21 手；名义上限 floor(10600/500)=21 → 21
        assert_eq!(c.sizing_lots(100.0, 1060.0, 10), Some(21));
        // 权益只够 1 手保证金但不够手续费 → 0 手
        assert_eq!(c.sizing_lots(100.0, 50.0, 10), None);
        assert_eq!(c.sizing_lots(100.0, -1.0, 10), None);
    }

    #[test]
    fn leverage_caps_notional_exposure() {
        let c = spec();
        // leverage=1：权益 10000 / 每手名义 500 = 20 手
        assert_eq!(c.sizing_lots(100.0, 10_000.0, 1), Some(20));
        // leverage=0 按不放大处理
        assert_eq!(c.sizing_lots(100.0, 10_000.0, 0), Some(20));
        // leverage=10：保证金上限 floor(10000/50.025)=199 手
        assert_eq!(c.sizing_lots(100.0, 10_000.0, 10), Some(199));
    }

    #[test]
    fn sizing_lots_margin_binds_at_high_leverage() {
        let c = spec();
        // 每手名义 100×5=500，保证金 50，开仓手续费 500×0.00005=0.025，每手成本 50.025
        // 保证金约束 floor(10000/50.025)=199；杠杆名义约束 floor(10000×10/500)=200 → 取小 = 199
        assert_eq!(c.sizing_lots(100.0, 10_000.0, 10), Some(199));
    }

    #[test]
    fn sizing_lots_cash_binds_at_low_cash() {
        let c = spec();
        // 公式中“现金”即权益：leverage=1 时名义约束 floor(10000/500)=20 小于保证金约束 199 → 20
        assert_eq!(c.sizing_lots(100.0, 10_000.0, 1), Some(20));
        // 权益 50 连 1 手的保证金+手续费（50.025）都不够 → None（一手都开不起）
        assert_eq!(c.sizing_lots(100.0, 50.0, 10), None);
    }

    #[test]
    fn sizing_lots_exact_one_lot_boundary() {
        let c = spec();
        // 权益恰等于 1 手名义 500（leverage=1）→ 名义约束 floor(500/500)=1，保证金约束 9 → Some(1)
        assert_eq!(c.sizing_lots(100.0, 500.0, 1), Some(1));
        // 低于 1 手名义 1 个单位 → None
        assert_eq!(c.sizing_lots(100.0, 499.0, 1), None);
    }

    #[test]
    fn trading_day_groups_night_session_with_next_weekday() {
        let cst = FixedOffset::east_opt(8 * 3600).unwrap();
        let t = |s: &str| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .unwrap()
                .and_local_timezone(cst)
                .single()
                .unwrap()
                .with_timezone(&Utc)
        };
        // 周五夜盘 + 周六凌晨 + 周一日盘 = 同一交易日（周一）
        let mon = chrono::NaiveDate::from_ymd_opt(2026, 1, 12).unwrap();
        assert_eq!(ctp_trading_day(t("2026-01-09 21:00:00")), mon);
        assert_eq!(ctp_trading_day(t("2026-01-10 00:30:00")), mon);
        assert_eq!(ctp_trading_day(t("2026-01-12 09:00:00")), mon);
        // 周一夜盘 = 周二交易日
        assert_eq!(
            ctp_trading_day(t("2026-01-12 21:00:00")),
            chrono::NaiveDate::from_ymd_opt(2026, 1, 13).unwrap()
        );
    }

    #[test]
    fn no_trade_window_blocks_last_five_minutes_of_session_close() {
        let cst = FixedOffset::east_opt(8 * 3600).unwrap();
        let t = |s: &str| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .unwrap()
                .and_local_timezone(cst)
                .single()
                .unwrap()
                .with_timezone(&Utc)
        };
        let mut c = spec();
        c.session_opens = vec!["09:00".into(), "21:00".into()];
        c.session_closes = vec!["15:00".into(), "01:00".into()];
        // 日盘开盘后 5 分钟禁止：09:00–09:04:59
        assert!(!c.is_no_trade_window(t("2026-01-05 08:59:59")));
        assert!(c.is_no_trade_window(t("2026-01-05 09:00:00")));
        assert!(c.is_no_trade_window(t("2026-01-05 09:04:59")));
        assert!(!c.is_no_trade_window(t("2026-01-05 09:05:00")));
        // 夜盘开盘后 5 分钟禁止：21:00–21:04:59
        assert!(!c.is_no_trade_window(t("2026-01-05 20:59:59")));
        assert!(c.is_no_trade_window(t("2026-01-05 21:00:00")));
        assert!(c.is_no_trade_window(t("2026-01-05 21:04:59")));
        assert!(!c.is_no_trade_window(t("2026-01-05 21:05:00")));
        // 日盘收盘前：14:55 起禁止
        assert!(!c.is_no_trade_window(t("2026-01-05 14:54:59")));
        assert!(c.is_no_trade_window(t("2026-01-05 14:55:00")));
        assert!(c.is_no_trade_window(t("2026-01-05 14:59:59")));
        assert!(c.is_no_trade_window(t("2026-01-05 15:00:00")));
        // 夜盘跨零点：00:55 起禁止
        assert!(!c.is_no_trade_window(t("2026-01-06 00:54:59")));
        assert!(c.is_no_trade_window(t("2026-01-06 00:55:00")));
        assert!(c.is_no_trade_window(t("2026-01-06 00:59:59")));
        assert!(c.is_no_trade_window(t("2026-01-06 01:00:00")));
    }

    #[test]
    fn session_open_time_matches_exact_minute() {
        let cst = FixedOffset::east_opt(8 * 3600).unwrap();
        let t = |s: &str| {
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .unwrap()
                .and_local_timezone(cst)
                .single()
                .unwrap()
                .with_timezone(&Utc)
        };
        let mut c = spec();
        c.session_opens = vec!["09:00".into(), "21:00".into()];
        assert!(c.is_session_open_time(t("2026-01-05 09:00:00")));
        assert!(c.is_session_open_time(t("2026-01-05 21:00:00")));
        // 夜盘开盘落在自然日的前一天 21:00，跨日匹配
        assert!(!c.is_session_open_time(t("2026-01-05 20:59:59")));
        assert!(!c.is_session_open_time(t("2026-01-05 09:05:00")));
        assert!(!c.is_session_open_time(t("2026-01-05 13:00:00")));
    }
}
