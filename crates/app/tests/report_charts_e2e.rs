//! `report_charts::generate_charts` 集成测试：三张 PNG 的真实绘图分支。
//!
//! 覆盖：日权益曲线 + 价格叠加（D1 CSV 命中）、回撤、信号周期权益曲线，
//! 以及日/周期序列为空时的早退分支。绘图依赖系统 DejaVu 字体，缺失则跳过。

use std::path::Path;

use chrono::{Duration, TimeZone, Utc};
use strategy_app::report::EquityPoint;
use strategy_app::report_charts::generate_charts;
use strategy_core::candle::{Candle, Timeframe};
use strategy_data::csv_store;

const FONT_DEJAVU: &str = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";
const VENUE: &str = "binance/crypto_perpetual/BTCUSDT";

fn daily_points(n: i64) -> Vec<EquityPoint> {
    (0..n)
        .map(|i| {
            let day = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap() + Duration::days(i);
            EquityPoint {
                id: format!("eqd_T_{}", day.format("%Y-%m-%d")),
                strategy_name: "T".into(),
                kind: "daily".into(),
                label: day.format("%Y-%m-%d").to_string(),
                bucket_secs: day.timestamp(),
                // 先涨后跌，回撤分支不会退化为全 0。
                equity: 100.0 + 8.0 * (i as f64) - 0.4 * (i as f64).powi(2),
                cash: 0.0,
            }
        })
        .collect()
}

fn tf_points(n: i64) -> Vec<EquityPoint> {
    (0..n)
        .map(|i| {
            let t = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap() + Duration::hours(i);
            EquityPoint {
                id: format!("eqt_T_{}", t.timestamp()),
                strategy_name: "T".into(),
                kind: "tf".into(),
                label: t.format("%Y-%m-%d %H:%M").to_string(),
                bucket_secs: t.timestamp(),
                equity: 100.0 + 2.0 * i as f64,
                cash: 0.0,
            }
        })
        .collect()
}

fn daily_candles(n: i64) -> Vec<Candle> {
    (0..n)
        .map(|i| {
            let t = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap() + Duration::days(i);
            Candle {
                open_time: t,
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
        })
        .collect()
}

#[test]
fn generate_charts_writes_all_three_pngs_with_price_overlay() {
    if !Path::new(FONT_DEJAVU).exists() {
        eprintln!("skip: 缺少 DejaVu 字体，plotters 无法出图");
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("klines");
    csv_store::save_csv(
        csv_store::csv_path(&data_dir, VENUE, Timeframe::D1),
        &daily_candles(20),
    )
    .unwrap();
    let run_dir = tmp.path().join("run");

    generate_charts(
        &run_dir,
        "BTCUSDT",
        "backtest",
        "15m",
        &daily_points(20),
        &tf_points(40),
        &data_dir,
        VENUE,
        "2026-01-02",
        "2026-01-21",
    )
    .expect("图表生成应成功");

    let charts = run_dir.join("charts");
    for name in [
        "backtest_equity_curve.png",
        "backtest_drawdown.png",
        "backtest_15m_equity_curve.png",
    ] {
        let p = charts.join(name);
        assert!(p.is_file(), "缺 {name}");
        assert!(
            std::fs::metadata(&p).unwrap().len() > 0,
            "{name} 不得为空文件"
        );
    }
}

#[test]
fn empty_series_still_creates_charts_dir_without_files() {
    let tmp = tempfile::tempdir().unwrap();
    let run_dir = tmp.path().join("run");
    generate_charts(
        &run_dir,
        "BTCUSDT",
        "backtest",
        "15m",
        &[],
        &[],
        tmp.path(),
        VENUE,
        "2026-01-02",
        "2026-01-21",
    )
    .expect("空序列应早退而非报错");
    let charts = run_dir.join("charts");
    assert!(charts.is_dir(), "charts 目录必须创建");
    assert!(
        std::fs::read_dir(&charts).unwrap().next().is_none(),
        "空序列不应产出任何文件"
    );
}
