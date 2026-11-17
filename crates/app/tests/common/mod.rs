//! 集成测试共享 fixture：CTP 形态合成 1m K 线 + 仓库真实合约参数。
//!
//! - [`synthetic_ctp_1m`]：CU（上期所铜）日盘/夜盘时段，午休 10:15-10:30、
//!   11:30-13:30 与隔夜 15:00→21:00 留空；连续自然日，不含周五夜盘→周一缺口、
//!   长假周（交易日历规则由 Task 5 单独钉死，本 fixture 不覆盖）。
//! - [`repo_contracts_yaml`]：直接复制仓库内真实跟踪的合约文件，
//!   不手写 ContractSpec 字面量。

use std::path::{Path, PathBuf};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};
use strategy_core::candle::Candle;

/// 仓库内真实跟踪的合约参数文件
pub fn repo_contracts_yaml() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/contracts.yaml")
}

/// 上海（UTC+8）
pub fn cst() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).expect("UTC+8")
}

/// 预热/交易分界（UTC 午夜，与 CLI `--start` 的 `parse_date` 口径一致）
///
/// 定点豁免理由：本模块是**跨测试二进制共享**的 fixture（各 binary `mod common;`
/// 后只用其中一部分），`trading_start_utc` 仅经 `warmup_len` 被
/// `backtest_paper_parity` 用到，在 `ctp_backtest_e2e` 等二进制内不被引用。
/// 原整文件 `#![allow(dead_code)]` 会连带罩住今后新增的死助手，故改为定点标注。
#[allow(dead_code)]
pub fn trading_start_utc() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 9, 0, 0, 0).unwrap()
}

/// 第一根合成 K 线所在自然日（CST）
pub fn first_day() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 6, 1).unwrap()
}

/// 单个自然日的 CU 闭合 1m 时刻（CST 墙钟）：
/// 日盘 09:00-10:15、10:30-11:30、13:30-15:00；夜盘 21:00 起至次日 01:00。
fn session_minutes(day: NaiveDate) -> Vec<NaiveDateTime> {
    let mut out = Vec::new();
    let mut push_range = |from: NaiveDateTime, to: NaiveDateTime| {
        let mut t = from;
        while t < to {
            out.push(t);
            t += chrono::Duration::minutes(1);
        }
    };
    let at = |d: NaiveDate, h: u32, m: u32| d.and_hms_opt(h, m, 0).unwrap();
    push_range(at(day, 9, 0), at(day, 10, 15));
    push_range(at(day, 10, 30), at(day, 11, 30));
    push_range(at(day, 13, 30), at(day, 15, 0));
    let next = day.succ_opt().unwrap();
    push_range(at(day, 21, 0), at(next, 1, 0));
    out
}

/// 生成 `days` 个连续自然日的 CTP 形态 1m 序列（升序、无重复）。
///
/// 价格：围绕 CU 量级（~7.8 万元/吨）的确定性正弦叠加，
/// 振幅约 ±0.9%（远小于 example_dual_ma 的 2% 初始止损，止损不在本 fixture 触发），
/// 按**交易分钟序号**而非墙钟驱动，因此时段缺口不改变价格形态。
pub fn synthetic_ctp_1m(days: i64) -> Vec<Candle> {
    let mut times: Vec<DateTime<Utc>> = Vec::new();
    for d in 0..days {
        let day = first_day() + chrono::Duration::days(d);
        for ndt in session_minutes(day) {
            let utc = cst().from_local_datetime(&ndt).unwrap().with_timezone(&Utc);
            times.push(utc);
        }
    }
    times.sort();
    times.dedup();
    times
        .into_iter()
        .enumerate()
        .map(|(k, t)| {
            let k = k as f64;
            let v = 78_000.0
                + 500.0 * (2.0 * std::f64::consts::PI * k / 600.0).sin()
                + 200.0 * (2.0 * std::f64::consts::PI * k / 137.0).sin();
            Candle {
                open_time: t,
                open: v,
                high: v + 5.0,
                low: v - 5.0,
                close: v,
                volume: 10.0,
                quote_volume: 0.0,
                trade_num: 0,
                active_buy_volume: 0.0,
                active_buy_quote_volume: 0.0,
                is_closed: true,
            }
        })
        .collect()
}

/// 交易分界索引：第一根 `open_time >= trading_start_utc()` 的 K 线下标（即预热根数）。
/// 定点豁免理由同 [`trading_start_utc`]（共享 fixture，非所有 binary 都用）。
#[allow(dead_code)]
pub fn warmup_len(bars: &[Candle]) -> usize {
    let start = trading_start_utc();
    bars.iter().take_while(|c| c.open_time < start).count()
}
