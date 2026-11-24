//! 回测图表生成 — 对齐 Python analyzer.py 的三张 PNG
//!
//! - backtest_equity_curve.png：权益曲线 + 价格叠加（日收盘，右轴缩放）
//! - backtest_drawdown.png：rolling-peak 回撤百分比
//! - backtest_{tf}_equity_curve.png：信号周期权益曲线
//!
//! 价格数据优先取本地日线 CSV（{data_dir}/1d/{SYMBOL}_1d.csv）；
//! 日线缺失时用 1m CSV 按 UTC 日聚合兜底（只导入 1m 数据的项目也能画价格）。
//! 价格点按日期与日净值对齐，而不是按数组下标硬对齐。

use std::collections::HashMap;
use std::path::Path;

use chrono::NaiveDate;

use strategy_core::candle::{Candle, Timeframe};
use strategy_data::aggregate::aggregate_1m;
use strategy_data::csv_store;

use crate::report::EquityPoint;

const FONT_DEJAVU: &str = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

/// 生成三张图到 {run_dir}/charts/；日线缺失时价格叠加自动省略
///
/// `venue_key` = §6.1 命名空间键（数据路径维度）；`symbol` 保留裸挂牌代码
/// （图内标注/文件名展示维度）。
#[allow(clippy::too_many_arguments)]
pub fn generate_charts(
    run_dir: &Path,
    symbol: &str,
    prefix: &str,
    tf_key: &str,
    daily_equity: &[EquityPoint],
    tf_equity: &[EquityPoint],
    data_dir: &Path,
    venue_key: &str,
    start_date: &str,
    end_date: &str,
) -> std::io::Result<()> {
    let charts_dir = run_dir.join("charts");
    std::fs::create_dir_all(&charts_dir)?;

    let daily_ohlc = load_daily_ohlc(data_dir, venue_key, start_date, end_date);

    plot_equity_curve(
        &charts_dir.join(format!("{prefix}_equity_curve.png")),
        symbol,
        daily_equity,
        daily_ohlc.as_deref(),
    )?;
    plot_drawdown(
        &charts_dir.join(format!("{prefix}_drawdown.png")),
        symbol,
        daily_equity,
    )?;
    plot_tf_equity(
        &charts_dir.join(format!("{prefix}_{tf_key}_equity_curve.png")),
        symbol,
        tf_key,
        tf_equity,
    )?;
    Ok(())
}

/// 日线 OHLC 序列（过滤到权益时间范围）。
/// 优先读 1d CSV；缺失时用 1m 聚合出 UTC 日线，避免只有 1m 数据时图上没有 K 线。
/// 日期标签优先用 CSV 的 `trading_day` 列（期货夜盘归次日交易日），
/// 缺失时回落 `open_time` 的 UTC 日期（crypto / 旧 10 列文件 / 1m 兜底）。
fn load_daily_ohlc(
    data_dir: &Path,
    venue_key: &str,
    start_date: &str,
    end_date: &str,
) -> Option<Vec<(NaiveDate, Candle)>> {
    let path = csv_store::resolve_csv_path(data_dir, venue_key, Timeframe::D1);
    let mut candles = csv_store::load_csv_annotated(&path).unwrap_or_default();
    if candles.is_empty() {
        let m1 = csv_store::load_or_empty(data_dir, venue_key, Timeframe::M1).ok()?;
        if m1.is_empty() {
            return None;
        }
        candles = aggregate_1m(&m1, Timeframe::D1, true)
            .into_iter()
            .map(|c| (c, None))
            .collect();
    }
    if candles.is_empty() {
        return None;
    }
    let start = NaiveDate::parse_from_str(start_date, "%Y-%m-%d").ok()?;
    let end = NaiveDate::parse_from_str(end_date, "%Y-%m-%d").ok()?;
    let rows: Vec<(NaiveDate, Candle)> = candles
        .into_iter()
        .filter_map(|(c, trading_day)| {
            let d = trading_day.unwrap_or_else(|| c.open_time.date_naive());
            (d >= start && d <= end).then_some((d, c))
        })
        .collect();
    (!rows.is_empty()).then_some(rows)
}

/// 图1：权益曲线 + 价格叠加（双坐标轴：左轴权益，右轴价格，Python twinx 同构）
fn plot_equity_curve(
    path: &Path,
    symbol: &str,
    daily: &[EquityPoint],
    daily_ohlc: Option<&[(NaiveDate, Candle)]>,
) -> std::io::Result<()> {
    use plotters::prelude::*;

    let eq: Vec<f64> = daily.iter().map(|r| r.equity).collect();
    if eq.is_empty() {
        return Ok(());
    }
    // K 线按日期映射到日净值的 x 下标；两边日期数不一致时不会错位。
    let price: Option<Vec<(f64, Candle)>> = daily_ohlc.and_then(|rows| {
        let by_date: HashMap<NaiveDate, Candle> = rows.iter().copied().collect();
        let pts: Vec<(f64, Candle)> = daily
            .iter()
            .enumerate()
            .filter_map(|(i, r)| {
                let d = NaiveDate::parse_from_str(&r.label, "%Y-%m-%d").ok()?;
                by_date.get(&d).map(|c| (i as f64, *c))
            })
            .collect();
        (pts.len() >= 2).then_some(pts)
    });
    let n = eq.len().max(2);

    let mut y_min = eq.iter().cloned().fold(f64::MAX, f64::min);
    let mut y_max = eq.iter().cloned().fold(f64::MIN, f64::max);
    // 无交易时权益是一条水平线，零高度坐标系会导致整张图空白。
    if (y_max - y_min).abs() < 1e-9 {
        let pad = (y_max.abs() * 0.005).max(1.0);
        y_min -= pad;
        y_max += pad;
    }

    let root = BitMapBackend::new(path, (1200, 600)).into_drawing_area();
    root.fill(&WHITE)
        .map_err(|e| io_err("权益图底色填充失败", e))?;

    let mut chart = ChartBuilder::on(&root)
        .caption(format!("{symbol} Backtest Equity Curve"), (FONT_DEJAVU, 24))
        .margin(15)
        .x_label_area_size(40)
        .y_label_area_size(80)
        .right_y_label_area_size(80)
        .build_cartesian_2d(0f64..(n - 1) as f64, y_min..y_max)
        .map_err(|e| io_err("权益图坐标系构建失败", e))?;

    // 左轴网格（权益）
    chart
        .configure_mesh()
        .x_labels(10)
        .y_labels(8)
        .x_label_formatter(&|x| x_axis_label(daily, *x as usize, n))
        .y_label_formatter(&|y| format!("{y:.0}"))
        .y_label_style(("sans-serif", 12, &BLUE))
        .disable_x_mesh()
        .draw()
        .map_err(|e| io_err("权益图网格绘制失败", e))?;

    // 权益曲线
    let pts: Vec<(f64, f64)> = eq.iter().enumerate().map(|(i, v)| (i as f64, *v)).collect();
    chart
        .draw_series(LineSeries::new(pts, &BLUE))
        .map_err(|e| io_err("权益曲线绘制失败", e))?
        .label("Equity")
        .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 15, y + 5)], BLUE.filled()));

    // 初始权益水平线
    // 灰色初始线用虚线，避免权益无波动时把蓝色权益线完全盖住。
    chart
        .draw_series(plotters::series::DashedLineSeries::new(
            vec![(0.0, eq[0]), (n as f64 - 1.0, eq[0])],
            8,
            8,
            RGBColor(128, 128, 128).stroke_width(1),
        ))
        .map_err(|e| io_err("初始线绘制失败", e))?
        .label("Initial")
        .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 15, y + 5)], RGBColor(128, 128, 128)));

    // 价格叠加：右轴 = 价格（plotters 双坐标轴，Python twinx 同构）
    if let Some(p) = &price {
        let p_min = p.iter().map(|(_, c)| c.low).fold(f64::MAX, f64::min);
        let p_max = p.iter().map(|(_, c)| c.high).fold(f64::MIN, f64::max);
        let p_pad = (p_max - p_min).max(1e-12) * 0.05;
        let mut chart2 =
            chart.set_secondary_coord(0f64..(n - 1) as f64, (p_min - p_pad)..(p_max + p_pad));
        chart2
            .configure_secondary_axes()
            .y_labels(8)
            .label_style(("sans-serif", 12, &RGBColor(255, 140, 0)))
            .y_label_formatter(&|v: &f64| {
                // 大额整数价（如 BTC 8 万级）不带小数，小额保留两位
                if *v >= 1000.0 {
                    format!("{v:.0}")
                } else {
                    format!("{v:.2}")
                }
            })
            .draw()
            .map_err(|e| io_err("价格右轴绘制失败", e))?;

        // 日 K 线：红涨绿跌；宽度按 x 轴密度收缩，避免蜡烛互相覆盖。
        let candle_width = (1200.0 / p.len() as f64 * 0.55).clamp(1.0, 12.0) as u32;
        chart2
            .draw_secondary_series(p.iter().map(|(x, c)| {
                plotters::element::CandleStick::new(
                    *x,
                    c.open,
                    c.high,
                    c.low,
                    c.close,
                    RED,
                    GREEN,
                    candle_width,
                )
            }))
            .map_err(|e| io_err("K 线叠加绘制失败", e))?
            .label("K-line")
            .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 15, y + 5)], RED.filled()));

        chart2
            .configure_series_labels()
            .position(SeriesLabelPosition::UpperLeft)
            .draw()
            .map_err(|e| io_err("图例绘制失败", e))?;
    } else {
        chart
            .configure_series_labels()
            .position(SeriesLabelPosition::UpperLeft)
            .draw()
            .map_err(|e| io_err("图例绘制失败", e))?;
    }
    root.present().map_err(|e| io_err("权益图保存失败", e))
}

/// 图2：rolling-peak 回撤
fn plot_drawdown(path: &Path, symbol: &str, daily: &[EquityPoint]) -> std::io::Result<()> {
    use plotters::prelude::*;

    if daily.is_empty() {
        return Ok(());
    }

    let mut peak = f64::MIN;
    let dd: Vec<f64> = daily
        .iter()
        .map(|r| {
            peak = peak.max(r.equity);
            if peak > 0.0 {
                (r.equity - peak) / peak * 100.0
            } else {
                0.0
            }
        })
        .collect();
    let n = daily.len().max(2);
    let d_min = dd.iter().cloned().fold(0.0f64, f64::min);
    // 全程零回撤时给一个可见的纵轴范围。
    let d_lo = if d_min >= 0.0 { -1.0 } else { d_min * 1.05 };

    let root = BitMapBackend::new(path, (1200, 400)).into_drawing_area();
    root.fill(&WHITE)
        .map_err(|e| io_err("回撤图底色填充失败", e))?;

    let mut chart = ChartBuilder::on(&root)
        .caption(format!("{symbol} Drawdown"), (FONT_DEJAVU, 24))
        .margin(15)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0f64..(n - 1) as f64, d_lo..0f64)
        .map_err(|e| io_err("回撤图坐标系构建失败", e))?;

    chart
        .configure_mesh()
        .x_labels(10)
        .y_labels(6)
        .x_label_formatter(&|x| x_axis_label(daily, *x as usize, n))
        .y_label_formatter(&|y| format!("{y:.0}%"))
        .draw()
        .map_err(|e| io_err("回撤图网格绘制失败", e))?;

    let pts: Vec<(f64, f64)> = dd.iter().enumerate().map(|(i, v)| (i as f64, *v)).collect();
    chart
        .draw_series(LineSeries::new(pts, &RED))
        .map_err(|e| io_err("回撤曲线绘制失败", e))?;
    root.present().map_err(|e| io_err("回撤图保存失败", e))
}

/// 图3：信号周期权益曲线
fn plot_tf_equity(
    path: &Path,
    symbol: &str,
    tf_key: &str,
    tf: &[EquityPoint],
) -> std::io::Result<()> {
    use plotters::prelude::*;

    if tf.is_empty() {
        return Ok(());
    }
    let eq: Vec<f64> = tf.iter().map(|r| r.equity).collect();
    let n = eq.len().max(2);
    let mut y_min = eq.iter().cloned().fold(f64::MAX, f64::min);
    let mut y_max = eq.iter().cloned().fold(f64::MIN, f64::max);
    if (y_max - y_min).abs() < 1e-9 {
        let pad = (y_max.abs() * 0.005).max(1.0);
        y_min -= pad;
        y_max += pad;
    }

    let root = BitMapBackend::new(path, (1200, 600)).into_drawing_area();
    root.fill(&WHITE)
        .map_err(|e| io_err("周期权益图底色填充失败", e))?;

    let mut chart = ChartBuilder::on(&root)
        .caption(format!("{symbol} {tf_key} Equity Curve"), (FONT_DEJAVU, 24))
        .margin(15)
        .x_label_area_size(40)
        .y_label_area_size(80)
        .build_cartesian_2d(0f64..(n - 1) as f64, y_min..y_max)
        .map_err(|e| io_err("周期权益图坐标系构建失败", e))?;

    chart
        .configure_mesh()
        .x_labels(10)
        .y_labels(8)
        .x_label_formatter(&|x| {
            tf.get(*x as usize)
                .map(|r| r.label.get(..10).unwrap_or(&r.label).to_string())
                .unwrap_or_default()
        })
        .y_label_formatter(&|y| format!("{y:.0}"))
        .draw()
        .map_err(|e| io_err("周期权益图网格绘制失败", e))?;

    let pts: Vec<(f64, f64)> = eq.iter().enumerate().map(|(i, v)| (i as f64, *v)).collect();
    chart
        .draw_series(LineSeries::new(pts, &BLUE))
        .map_err(|e| io_err("周期权益曲线绘制失败", e))?;
    chart
        .draw_series(plotters::series::DashedLineSeries::new(
            vec![(0.0, eq[0]), (n as f64 - 1.0, eq[0])],
            8,
            8,
            RGBColor(128, 128, 128).stroke_width(1),
        ))
        .map_err(|e| io_err("周期权益初始线绘制失败", e))?;
    root.present().map_err(|e| io_err("周期权益图保存失败", e))
}

/// x 轴刻度 → 日期标签（权益 index 对应日期；越界取最近有效行或空）
fn x_axis_label(daily: &[EquityPoint], idx: usize, n: usize) -> String {
    if daily.is_empty() {
        return String::new();
    }
    let i = idx.min(n.saturating_sub(1)).min(daily.len() - 1);
    daily[i].label.clone()
}

fn io_err(msg: &'static str, detail: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(format!("{msg}: {detail}"))
}
