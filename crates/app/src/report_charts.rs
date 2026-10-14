//! 回测图表生成 — 对齐 Python analyzer.py 的三张 PNG
//!
//! - backtest_equity_curve.png：权益曲线 + 价格叠加（日收盘，右轴缩放）
//! - backtest_drawdown.png：rolling-peak 回撤百分比
//! - backtest_{tf}_equity_curve.png：信号周期权益曲线
//!
//! 价格数据来自本地日线 CSV（{data_dir}/1d/{SYMBOL}_1d.csv），
//! 缺失时只画权益（对齐 Python：日线文件不存在则跳过价格叠加）。

use std::path::Path;

use chrono::NaiveDate;

use strategy_core::candle::{Candle, Timeframe};
use strategy_data::csv_store;

use crate::report::EquityPoint;

const FONT_DEJAVU: &str = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

/// 生成三张图到 {run_dir}/charts/；日线缺失时价格叠加自动省略
#[allow(clippy::too_many_arguments)]
pub fn generate_charts(
    run_dir: &Path,
    symbol: &str,
    prefix: &str,
    tf_key: &str,
    daily_equity: &[EquityPoint],
    tf_equity: &[EquityPoint],
    data_dir: &Path,
    start_date: &str,
    end_date: &str,
) -> std::io::Result<()> {
    let charts_dir = run_dir.join("charts");
    std::fs::create_dir_all(&charts_dir)?;

    let daily_close = load_daily_close(data_dir, symbol, start_date, end_date);

    plot_equity_curve(
        &charts_dir.join(format!("{prefix}_equity_curve.png")),
        symbol,
        daily_equity,
        daily_close.as_deref(),
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

/// 日线收盘序列（过滤到权益时间范围）；文件缺失 → None
fn load_daily_close(
    data_dir: &Path,
    symbol: &str,
    start_date: &str,
    end_date: &str,
) -> Option<Vec<(NaiveDate, f64)>> {
    let candles: Vec<Candle> =
        csv_store::load_or_empty(data_dir, symbol, Timeframe::D1).unwrap_or_default();
    if candles.is_empty() {
        return None;
    }
    let start = NaiveDate::parse_from_str(start_date, "%Y-%m-%d").ok()?;
    let end = NaiveDate::parse_from_str(end_date, "%Y-%m-%d").ok()?;
    let rows: Vec<(NaiveDate, f64)> = candles
        .iter()
        .filter_map(|c| {
            let d = c.open_time.date_naive();
            (d >= start && d <= end).then_some((d, c.close))
        })
        .collect();
    (!rows.is_empty()).then_some(rows)
}

/// 图1：权益曲线 + 价格叠加（双坐标轴：左轴权益，右轴价格，Python twinx 同构）
fn plot_equity_curve(
    path: &Path,
    symbol: &str,
    daily: &[EquityPoint],
    daily_close: Option<&[(NaiveDate, f64)]>,
) -> std::io::Result<()> {
    use plotters::prelude::*;

    let eq: Vec<f64> = daily.iter().map(|r| r.equity).collect();
    if eq.is_empty() {
        return Ok(());
    }
    let price: Option<Vec<f64>> = daily_close.map(|rows| rows.iter().map(|(_, c)| *c).collect());
    let n = eq.len().max(price.as_ref().map_or(0, |p| p.len())).max(2);

    let y_min = eq.iter().cloned().fold(f64::MAX, f64::min);
    let y_max = eq.iter().cloned().fold(f64::MIN, f64::max);

    let root = BitMapBackend::new(path, (1200, 600)).into_drawing_area();
    root.fill(&WHITE)
        .map_err(|e| io_err("权益图底色填充失败", e))?;

    let mut chart = ChartBuilder::on(&root)
        .caption(format!("{symbol} Backtest Equity Curve"), (FONT_DEJAVU, 24))
        .margin(15)
        .x_label_area_size(40)
        .y_label_area_size(80)
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
    chart
        .draw_series(std::iter::once(PathElement::new(
            vec![(0.0, eq[0]), (n as f64 - 1.0, eq[0])],
            ShapeStyle::from(&RGBColor(128, 128, 128)).stroke_width(1),
        )))
        .map_err(|e| io_err("初始线绘制失败", e))?
        .label("Initial")
        .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 15, y + 5)], RGBColor(128, 128, 128)));

    // 价格叠加：右轴 = 价格（plotters 双坐标轴，Python twinx 同构）
    if let Some(p) = &price {
        let p_min = p.iter().cloned().fold(f64::MAX, f64::min);
        let p_max = p.iter().cloned().fold(f64::MIN, f64::max);
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

        let pts: Vec<(f64, f64)> = p.iter().enumerate().map(|(i, v)| (i as f64, *v)).collect();
        let orange = RGBColor(255, 140, 0);
        chart2
            .draw_secondary_series(LineSeries::new(pts, orange.mix(0.7)))
            .map_err(|e| io_err("价格叠加绘制失败", e))?
            .label("Price")
            .legend(move |(x, y)| {
                Rectangle::new([(x, y - 5), (x + 15, y + 5)], orange.mix(0.7).filled())
            });

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

    let root = BitMapBackend::new(path, (1200, 400)).into_drawing_area();
    root.fill(&WHITE)
        .map_err(|e| io_err("回撤图底色填充失败", e))?;

    let mut chart = ChartBuilder::on(&root)
        .caption(format!("{symbol} Drawdown"), (FONT_DEJAVU, 24))
        .margin(15)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(0f64..(n - 1) as f64, d_min..0f64)
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
    let y_min = eq.iter().cloned().fold(f64::MAX, f64::min);
    let y_max = eq.iter().cloned().fold(f64::MIN, f64::max);

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
        .draw_series(std::iter::once(PathElement::new(
            vec![(0.0, eq[0]), (n as f64 - 1.0, eq[0])],
            ShapeStyle::from(&RGBColor(128, 128, 128)).stroke_width(1),
        )))
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
