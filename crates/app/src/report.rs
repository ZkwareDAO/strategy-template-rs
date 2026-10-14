//! 回测报告层 — 对齐 Python backtest_reporter.py + analyzer.py 产物
//!
//! S5：回测成交与净值全部 model 化进统一 Store（内存唯一真相，
//! flush 到 `{state_base}/{YYYYMMDD}/{model}.csv` 通用 5 列格式）：
//! - [`TradeRow`]     `NAME="trade"`：一次成交一行（只增不改），id=trade_{signal_id}
//! - [`EquityPoint`]  `NAME="equity"`：净值点，kind=daily|tf（同桶 upsert 刷新）
//!
//! 旧的 backtest_trades.csv / backtest_equity.csv / {tf}_equity.csv
//! 三张固定列 CSV 已废弃；result.json / backtest_report.txt /
//! backtest_analysis_report.md / charts/*.png 作为回测报告产物保留，
//! 其数据全部从 Store 行汇总（目录结构仍对齐 Python run_backtest.py）。
//!
//! 指标口径逐条照搬 Python `_compute_metrics`（sharpe=日收益×√252、
//! 年化=(1+r)^(365/天数)−1、胜率只统计 pnl≠0 的平仓单）。

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use strategy_store::{Model, ModelTable, Store};

/// trade 行业务主键：信号 id 是 (策略, symbol, K线时间, 信号类型) 的确定性哈希，
/// 回测一次意图至多一笔成交 → 成交 id 全局唯一且两次回放逐字节一致。
pub fn trade_row_id(signal_id: &str) -> String {
    format!("trade_{signal_id}")
}

/// daily 净值点 id（date = YYYY-MM-DD，UTC）。
pub fn daily_equity_id(strategy_name: &str, date: &str) -> String {
    format!("eqd_{strategy_name}_{date}")
}

/// 信号周期净值点 id（bucket_secs = 对齐桶起点的 epoch 秒）。
pub fn tf_equity_id(strategy_name: &str, bucket_secs: i64) -> String {
    format!("eqt_{strategy_name}_{bucket_secs}")
}

/// 一笔成交（Store `trade` model 一行；开仓行 pnl=0，只增不改）。
/// 字段保留旧 backtest_trades.csv 全部信息（含序号 trade_id）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeRow {
    /// 业务主键 trade_{signal_id}
    pub id: String,
    /// 运行时策略实例名（共享表过滤键）
    pub strategy_name: String,
    /// 旧报表保留字段：成交序号 T%08（回放确定性）
    pub trade_id: String,
    pub strategy_id: String,
    pub symbol: String,
    /// BUY / SELL / SELL_CLOSE / BUY_CLOSE
    pub side: String,
    pub quantity: f64,
    pub price: f64,
    pub commission: f64,
    pub slippage: f64,
    pub pnl: f64,
    /// ISO-8601（如 2025-01-02T10:00:00，K 线时间）
    pub timestamp: String,
    pub comment: String,
}

impl TradeRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        seq: usize,
        strategy_name: &str,
        strategy_id: &str,
        symbol: &str,
        side: &str,
        quantity: f64,
        price: f64,
        commission: f64,
        slippage: f64,
        pnl: f64,
        timestamp: &str,
        comment: &str,
        signal_id: &str,
    ) -> Self {
        Self {
            id: trade_row_id(signal_id),
            strategy_name: strategy_name.into(),
            trade_id: format!("T{seq:08}"),
            strategy_id: strategy_id.into(),
            symbol: symbol.into(),
            side: side.into(),
            quantity,
            price,
            commission,
            slippage,
            pnl,
            timestamp: timestamp.into(),
            comment: comment.into(),
        }
    }
}

impl Model for TradeRow {
    const NAME: &'static str = "trade";

    fn id(&self) -> &str {
        &self.id
    }
}

/// 净值点（Store `equity` model 一行）。
/// daily（UTC 日，桶=0 点）与 tf（信号周期对齐桶）用 `kind` 同表区分；
/// 同一桶内每根 1m upsert 刷新，落盘行保留桶内最后一根 1m 的值（旧日末语义）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EquityPoint {
    /// 业务主键（eqd_/eqt_ 前缀防跨 kind 碰撞）
    pub id: String,
    /// 运行时策略实例名（共享表过滤键）
    pub strategy_name: String,
    /// daily / tf
    pub kind: String,
    /// daily: "YYYY-MM-DD"；tf: "YYYY-MM-DD HH:MM"（桶起点，UTC）
    pub label: String,
    /// 桶起点 epoch 秒（确定性排序键）
    pub bucket_secs: i64,
    pub equity: f64,
    pub cash: f64,
}

impl Model for EquityPoint {
    const NAME: &'static str = "equity";

    fn id(&self) -> &str {
        &self.id
    }
}

/// 取本策略实例全部成交（按序号 trade_id 升序）。
pub fn trade_rows(table: &ModelTable<TradeRow>, strategy_name: &str) -> Vec<TradeRow> {
    let mut rows: Vec<TradeRow> = table
        .list()
        .into_iter()
        .filter(|r| r.strategy_name == strategy_name)
        .collect();
    rows.sort_by(|a, b| a.trade_id.cmp(&b.trade_id));
    rows
}

/// 取本策略实例全部净值点（kind=daily/tf，按桶起点升序）。
pub fn equity_points(
    table: &ModelTable<EquityPoint>,
    strategy_name: &str,
    kind: &str,
) -> Vec<EquityPoint> {
    let mut rows: Vec<EquityPoint> = table
        .list()
        .into_iter()
        .filter(|r| r.strategy_name == strategy_name && r.kind == kind)
        .collect();
    rows.sort_by_key(|p| p.bucket_secs);
    rows
}

/// 净值写入端：引擎每根 1m mark-to-market 后喂入，daily/tf 两桶各自 upsert。
/// Store 时钟必须锚定当前 K 线时间（回测 CandleClock，禁止 Utc::now）。
#[derive(Clone)]
pub struct EquitySink {
    strategy_name: Arc<str>,
    table: Arc<ModelTable<EquityPoint>>,
}

impl EquitySink {
    pub fn new(strategy_name: impl Into<String>, store: &Arc<Store>) -> Self {
        Self {
            strategy_name: Arc::from(strategy_name.into()),
            table: store.table::<EquityPoint>(),
        }
    }

    /// 记录一根 1m 收盘时刻的权益（同桶幂等刷新）。
    /// `tf_minutes`: 信号周期分钟数（桶按分钟数对齐）。
    pub fn on_1m_close(&self, t: DateTime<Utc>, equity: f64, cash: f64, tf_minutes: u32) {
        let name = self.strategy_name.as_ref();
        let date = t.format("%Y-%m-%d").to_string();
        let day_bucket = t.timestamp() / 86_400 * 86_400;
        self.table.upsert(EquityPoint {
            id: daily_equity_id(name, &date),
            strategy_name: name.into(),
            kind: "daily".into(),
            label: date,
            bucket_secs: day_bucket,
            equity,
            cash,
        });

        let span = tf_minutes as i64 * 60;
        let bucket = if span > 0 {
            t.timestamp() / span * span
        } else {
            t.timestamp()
        };
        let bucket_start = DateTime::from_timestamp(bucket, 0).unwrap_or(t);
        self.table.upsert(EquityPoint {
            id: tf_equity_id(name, bucket),
            strategy_name: name.into(),
            kind: "tf".into(),
            label: bucket_start.format("%Y-%m-%d %H:%M").to_string(),
            bucket_secs: bucket,
            equity,
            cash,
        });
    }

    pub fn daily(&self) -> Vec<EquityPoint> {
        equity_points(&self.table, &self.strategy_name, "daily")
    }

    pub fn tf(&self) -> Vec<EquityPoint> {
        equity_points(&self.table, &self.strategy_name, "tf")
    }
}

/// 指标集（字段与 Python metrics 完全一致）
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct Metrics {
    pub total_return: f64,
    pub roe: f64,
    pub annualized_return: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub avg_trade_pnl: f64,
    pub largest_win: f64,
    pub largest_loss: f64,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub profit_trades_pnl: f64,
    pub loss_trades_pnl: f64,
    pub trading_days: usize,
    pub daily_return_mean: f64,
    pub daily_return_std: f64,
}

/// 指标计算 — 逐条对齐 Python `_compute_metrics`
pub fn compute_metrics(
    initial_cash: f64,
    daily_equity: &[EquityPoint],
    trades: &[TradeRow],
) -> Metrics {
    let initial = if initial_cash != 0.0 {
        initial_cash
    } else {
        daily_equity.first().map(|r| r.equity).unwrap_or(0.0)
    };
    let final_equity = daily_equity.last().map(|r| r.equity).unwrap_or(initial);

    let total_return = if initial != 0.0 {
        (final_equity - initial) / initial
    } else {
        0.0
    };

    let trading_days = daily_equity.len();
    let annualized = if trading_days > 1 && initial != 0.0 {
        (1.0 + total_return).powf(365.0 / trading_days as f64) - 1.0
    } else {
        0.0
    };

    // 日收益序列
    let mut daily_returns: Vec<f64> = Vec::new();
    for w in daily_equity.windows(2) {
        if w[0].equity > 0.0 {
            daily_returns.push((w[1].equity - w[0].equity) / w[0].equity);
        }
    }
    let daily_mean = mean(&daily_returns);
    let daily_std = sample_std(&daily_returns);
    let sharpe = if daily_std > 0.0 {
        daily_mean / daily_std * 252f64.sqrt()
    } else {
        0.0
    };

    let neg_returns: Vec<f64> = daily_returns.iter().copied().filter(|r| *r < 0.0).collect();
    let neg_std = sample_std(&neg_returns);
    let sortino = if neg_std > 0.0 {
        daily_mean / neg_std * 252f64.sqrt()
    } else {
        0.0
    };

    // 最大回撤（rolling peak）
    let mut peak = initial;
    let mut max_dd = 0.0f64;
    for row in daily_equity {
        if row.equity > peak {
            peak = row.equity;
        }
        if peak > 0.0 {
            let dd = (peak - row.equity) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }

    // 只统计 pnl != 0 的平仓单（Python close_trades 口径）
    let close_trades: Vec<&TradeRow> = trades.iter().filter(|t| t.pnl != 0.0).collect();
    let winning: Vec<&TradeRow> = close_trades
        .iter()
        .copied()
        .filter(|t| t.pnl > 0.0)
        .collect();
    let losing: Vec<&TradeRow> = close_trades
        .iter()
        .copied()
        .filter(|t| t.pnl < 0.0)
        .collect();

    let total = close_trades.len();
    let win_count = winning.len();
    let loss_count = losing.len();
    let win_rate = if total > 0 {
        win_count as f64 / total as f64
    } else {
        0.0
    };

    let profit_pnl: f64 = winning.iter().map(|t| t.pnl).sum();
    let loss_pnl: f64 = losing.iter().map(|t| t.pnl).sum();
    let profit_factor = if loss_pnl != 0.0 {
        (profit_pnl / loss_pnl).abs()
    } else {
        0.0
    };

    let avg_pnl = if total > 0 {
        close_trades.iter().map(|t| t.pnl).sum::<f64>() / total as f64
    } else {
        0.0
    };
    let avg_win = if win_count > 0 {
        profit_pnl / win_count as f64
    } else {
        0.0
    };
    let avg_loss = if loss_count > 0 {
        (loss_pnl / loss_count as f64).abs()
    } else {
        0.0
    };

    Metrics {
        total_return,
        roe: total_return,
        annualized_return: annualized,
        sharpe_ratio: sharpe,
        sortino_ratio: sortino,
        max_drawdown: max_dd,
        win_rate,
        profit_factor,
        total_trades: total,
        winning_trades: win_count,
        losing_trades: loss_count,
        avg_trade_pnl: avg_pnl,
        largest_win: winning.iter().map(|t| t.pnl).fold(0.0, f64::max),
        largest_loss: losing.iter().map(|t| t.pnl).fold(0.0, f64::min),
        avg_win,
        avg_loss,
        profit_trades_pnl: profit_pnl,
        loss_trades_pnl: loss_pnl,
        trading_days,
        daily_return_mean: daily_mean,
        daily_return_std: daily_std,
    }
}

fn mean(v: &[f64]) -> f64 {
    if v.is_empty() {
        0.0
    } else {
        v.iter().sum::<f64>() / v.len() as f64
    }
}

/// 样本标准差（n-1，对齐 Python `_std`）
fn sample_std(v: &[f64]) -> f64 {
    if v.len() < 2 {
        return 0.0;
    }
    let m = mean(v);
    let var = v.iter().map(|x| (x - m) * (x - m)).sum::<f64>() / (v.len() - 1) as f64;
    var.sqrt()
}

/// 报告生成器
pub struct ReportGenerator {
    /// 回测运行目录（.../{strategy}/{date}/{time}/{SYMBOL}/）
    run_dir: PathBuf,
}

impl ReportGenerator {
    pub fn new(run_dir: impl AsRef<Path>) -> Self {
        Self {
            run_dir: run_dir.as_ref().to_path_buf(),
        }
    }

    pub fn run_dir(&self) -> &Path {
        &self.run_dir
    }

    /// 生成全部产物（图表失败仅告警，不阻断）
    #[allow(clippy::too_many_arguments)]
    pub fn generate(
        &self,
        strategy_id: &str,
        symbol: &str,
        start_date: &str,
        end_date: &str,
        initial_cash: f64,
        data_dir: &Path,
        tf_key: &str,
        daily_equity: &[EquityPoint],
        tf_equity: &[EquityPoint],
        trades: &[TradeRow],
        klines_processed: usize,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        config_yaml: Option<&str>,
    ) -> io::Result<PathBuf> {
        fs::create_dir_all(&self.run_dir)?;

        // config.yaml 复制
        if let Some(yaml) = config_yaml {
            let _ = fs::write(self.run_dir.join("config.yaml"), yaml);
        }

        // S5：成交/净值只落统一 Store（state/{day}/trade.csv、equity.csv），
        // 不再写 backtest_trades.csv / backtest_equity.csv / {tf}_equity.csv。

        // 1. 指标（从 Store 行汇总）
        let metrics = compute_metrics(initial_cash, daily_equity, trades);
        let duration = (end_time - start_time).num_milliseconds() as f64 / 1000.0;
        let peak_equity = daily_equity
            .iter()
            .map(|r| r.equity)
            .fold(initial_cash, f64::max);
        let max_dd = metrics.max_drawdown;

        // 4. backtest_result.json
        self.write_result_json(
            strategy_id,
            symbol,
            start_date,
            end_date,
            initial_cash,
            data_dir,
            daily_equity,
            trades,
            &metrics,
            klines_processed,
            start_time,
            end_time,
            duration,
            peak_equity,
        )?;

        // 5. backtest_report.txt
        self.write_report_txt(
            strategy_id,
            symbol,
            start_date,
            end_date,
            initial_cash,
            daily_equity,
            trades,
            &metrics,
            klines_processed,
            duration,
            peak_equity,
            max_dd,
        )?;

        // 6. 图表 + analysis_report.md（失败仅告警）
        if let Err(e) = crate::report_charts::generate_charts(
            &self.run_dir,
            symbol,
            "backtest",
            tf_key,
            daily_equity,
            tf_equity,
            data_dir,
            start_date,
            end_date,
        ) {
            tracing::warn!(error = %e, "图表生成失败（跳过 charts/）");
        }
        self.write_analysis_report(symbol, daily_equity, &metrics);

        Ok(self.run_dir.to_path_buf())
    }

    #[allow(clippy::too_many_arguments)]
    fn write_result_json(
        &self,
        strategy_id: &str,
        symbol: &str,
        start_date: &str,
        end_date: &str,
        initial_cash: f64,
        data_dir: &Path,
        daily_equity: &[EquityPoint],
        trades: &[TradeRow],
        metrics: &Metrics,
        klines_processed: usize,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        duration: f64,
        peak_equity: f64,
    ) -> io::Result<()> {
        let daily_json: Vec<_> = daily_equity
            .iter()
            .map(|r| {
                json!({
                    "date": r.label,
                    "equity": r.equity,
                    "cash": r.cash,
                })
            })
            .collect();
        let bt_config = json!({
            "name": format!("{strategy_id} 回测"),
            "start_date": start_date,
            "end_date": end_date,
            "initial_cash": initial_cash,
            "symbols": [symbol],
            "data_dir": data_dir.display().to_string(),
        });
        let accounts = json!([{
            "strategy_id": strategy_id,
            "cash": final_cash(daily_equity, initial_cash),
            "frozen_cash": 0.0,
            "total_equity": daily_equity.last().map(|r| r.equity).unwrap_or(initial_cash),
            "peak_equity": peak_equity,
            "max_drawdown": metrics.max_drawdown,
            "position_count": 0,
            "trade_count": trades.len(),
        }]);
        let result = json!({
            "config": bt_config,
            "start_time": start_time.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
            "end_time": end_time.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
            "duration_seconds": duration,
            "accounts": accounts,
            "metrics": metrics, // serde::Serialize 按字段序输出（对齐 Python key 序）
            "daily_equity": daily_json,
            "trades_count": trades.len(),
            "klines_processed": klines_processed,
            "signals_processed": trades.len(),
            "status": "success",
            "performance_summary": perf_summary(metrics),
        });
        let path = self.run_dir.join("backtest_result.json");
        fs::write(
            &path,
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn write_report_txt(
        &self,
        strategy_id: &str,
        _symbol: &str,
        start_date: &str,
        end_date: &str,
        initial_cash: f64,
        daily_equity: &[EquityPoint],
        trades: &[TradeRow],
        m: &Metrics,
        klines_processed: usize,
        duration: f64,
        _peak_equity: f64,
        max_dd: f64,
    ) -> io::Result<()> {
        let sep = "=".repeat(80);
        let dash = "-".repeat(40);
        let mut lines: Vec<String> = Vec::new();
        lines.push(sep.clone());
        lines.push("CTA 策略回测报告".into());
        lines.push(sep.clone());
        lines.push(String::new());
        lines.push("回测概况".into());
        lines.push(dash.clone());
        lines.push(format!("  回测名称：    {strategy_id} 回测"));
        lines.push(format!("  回测区间：    {start_date} 至 {end_date}"));
        lines.push(format!("  初始资金：    {}", fmt_comma(initial_cash, 2)));
        lines.push(format!("  耗时：        {duration:.2} 秒"));
        lines.push(format!("  处理 K 线数：   {klines_processed}"));
        lines.push(format!("  处理信号数：  {}", trades.len()));
        lines.push(String::new());

        let final_eq = daily_equity
            .last()
            .map(|r| r.equity)
            .unwrap_or(initial_cash);
        lines.push("账户摘要".into());
        lines.push(dash.clone());
        lines.push(format!("  策略：{strategy_id}"));
        lines.push(format!("    期末权益：    {}", fmt_comma(final_eq, 2)));
        lines.push(format!("    可用现金：    {}", fmt_comma(final_eq, 2)));
        lines.push("    持仓保证金：  0.00".into());
        lines.push(format!("    最大回撤：    {:.2}%", max_dd * 100.0));
        lines.push(format!("    交易次数：    {}", trades.len()));
        lines.push(String::new());

        lines.push("绩效指标".into());
        lines.push(dash.clone());
        lines.push(format!(
            "  总收益率：            {:.2}%",
            m.total_return * 100.0
        ));
        lines.push(format!(
            "  年化收益率：          {:.2}%",
            m.annualized_return * 100.0
        ));
        lines.push(format!("  夏普比率：              {:.2}", m.sharpe_ratio));
        lines.push(format!("  索提诺比率：            {:.2}", m.sortino_ratio));
        lines.push(format!(
            "  最大回撤：            {:.2}%",
            m.max_drawdown * 100.0
        ));
        lines.push(String::new());

        lines.push("交易统计".into());
        lines.push(dash.clone());
        lines.push(format!("  总交易次数：               {}", m.total_trades));
        lines.push(format!("  盈利次数：                 {}", m.winning_trades));
        lines.push(format!("  亏损次数：                 {}", m.losing_trades));
        lines.push(format!(
            "  胜率：                {:.2}%",
            m.win_rate * 100.0
        ));
        lines.push(format!("  盈亏比：               {:.2}", m.profit_factor));
        lines.push(String::new());

        lines.push("盈亏分析".into());
        lines.push(dash.clone());
        lines.push(format!("  平均盈亏：           {:.2}", m.avg_trade_pnl));
        lines.push(format!("  平均盈利：          {:.2}", m.avg_win));
        lines.push(format!("  平均亏损：           {:.2}", m.avg_loss));
        lines.push(format!("  最大盈利：          {:.2}", m.largest_win));
        lines.push(format!("  最大亏损：          {:.2}", m.largest_loss));
        lines.push(format!("  盈利总额：          {:.2}", m.profit_trades_pnl));
        lines.push(format!("  亏损总额：          {:.2}", m.loss_trades_pnl));
        lines.push(String::new());

        lines.push("期末持仓".into());
        lines.push(dash.clone());
        lines.push("  无持仓".into());
        lines.push(String::new());

        lines.push("最近交易记录（最多 20 条）".into());
        lines.push(dash.clone());
        lines.push("  时间                  策略              标的         方向               数量         价格         盈亏".into());
        lines.push(format!("  {}", "-".repeat(85)));
        let recent = if trades.len() > 20 {
            &trades[trades.len() - 20..]
        } else {
            trades
        };
        for t in recent {
            let ts = t.timestamp.replace('T', " ");
            let ts = ts.get(..19).unwrap_or(&ts).to_string();
            let pnl_str = if t.pnl != 0.0 {
                format!("{:.2}", t.pnl)
            } else {
                "-".into()
            };
            lines.push(format!(
                "  {} {:<20} {:<12} {:<18} {:>10.2} {:>12.2} {:>10}",
                ts, "", t.symbol, t.side, t.quantity, t.price, pnl_str
            ));
        }
        lines.push(String::new());
        lines.push(sep.clone());
        lines.push(format!(
            "报告生成时间：{}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S")
        ));
        lines.push(sep.clone());
        lines.push(String::new());
        fs::write(self.run_dir.join("backtest_report.txt"), lines.join("\n"))
    }

    fn write_analysis_report(&self, symbol: &str, daily_equity: &[EquityPoint], m: &Metrics) {
        let Some(first) = daily_equity.first() else {
            return;
        };
        // 峰值/谷值（对齐 analyzer.py：全局 max/min）
        let peak_row = daily_equity
            .iter()
            .max_by(|a, b| a.equity.total_cmp(&b.equity))
            .unwrap_or(first);
        let trough_row = daily_equity
            .iter()
            .min_by(|a, b| a.equity.total_cmp(&b.equity))
            .unwrap_or(first);
        // analyzer.py 的回撤口径：(peak-trough)/peak（全局极值差，非 rolling peak）
        let analyzer_dd = if peak_row.equity > 0.0 {
            (peak_row.equity - trough_row.equity) / peak_row.equity * 100.0
        } else {
            0.0
        };
        let verdict = if m.total_return > 0.0 {
            "盈利"
        } else {
            "亏损"
        };
        let risk = if analyzer_dd < 20.0 {
            "风险较低"
        } else if analyzer_dd < 40.0 {
            "风险中等"
        } else {
            "风险较高"
        };
        let content = format!(
            r#"# 回测分析报告

**策略**: {symbol}
**生成时间**: {now}

---

## 1. 概况

| 指标 | 值 |
|------|------|
| 回测天数 | {days} |
| 初始权益 | {init} |
| 期末权益 | {final} |
| 总收益率 | {tr:.2}% |
| 最大回撤 | {dd:.2}% |

---

## 2. 权益曲线

![权益曲线](charts/backtest_equity_curve.png)

---

## 3. 回撤分析

![回撤图](charts/backtest_drawdown.png)

| 指标 | 值 |
|------|------|
| 峰值权益 | {peak} |
| 峰值日期 | {peak_d} |
| 谷值权益 | {trough} |
| 谷值日期 | {trough_d} |

---

## 4. 分析结论

- **收益表现**: {verdict}
- **风险控制**: 最大回撤 {dd:.2}%，{risk}

---

*报告由 BacktestAnalyzer 自动生成*
"#,
            now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            days = m.trading_days,
            init = fmt_comma(first.equity, 2),
            final = fmt_comma(daily_equity.last().map(|r| r.equity).unwrap_or(0.0), 2),
            tr = m.total_return * 100.0,
            dd = analyzer_dd,
            peak = fmt_comma(peak_row.equity, 2),
            peak_d = peak_row.label,
            trough = fmt_comma(trough_row.equity, 2),
            trough_d = trough_row.label,
            verdict = verdict,
            risk = risk,
        );
        let _ = fs::write(self.run_dir.join("backtest_analysis_report.md"), content);
    }
}

/// result.json accounts.cash：期末现金（回测末通常空仓 → 等于期末权益）
fn final_cash(daily_equity: &[EquityPoint], initial_cash: f64) -> f64 {
    daily_equity.last().map(|r| r.cash).unwrap_or(initial_cash)
}

/// 千分位格式化（对齐 Python `{v:,.2f}`）：1234567.891 → "1,234,567.89"
fn fmt_comma(v: f64, decimals: usize) -> String {
    let s = format!("{v:.decimals$}");
    let (int_part, frac) = match s.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (s.as_str(), None),
    };
    let neg = int_part.starts_with('-');
    let digits = int_part.trim_start_matches('-');
    let mut grouped = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(c);
    }
    let sign = if neg { "-" } else { "" };
    match frac {
        Some(f) => format!("{sign}{grouped}.{f}"),
        None => format!("{sign}{grouped}"),
    }
}

/// 绩效分析摘要（result.json 的 performance_summary 字段）
fn perf_summary(m: &Metrics) -> String {
    let sep = "=".repeat(60);
    format!(
        "{sep}\n绩效分析摘要\n{sep}\n\n\
收益指标:\n  \
ROE (权益回报率):       {:.2}%  (总收益率：{:.2}%)\n  \
年化收益率：         {:.2}%\n\n\
风险指标:\n  \
最大回撤：           {:.2}%\n  \
日波动率：            {:.2}%\n\n\
风险调整后收益:\n  \
夏普比率：             {:.2}\n  \
索提诺比率：           {:.2}\n\n\
交易统计:\n  \
总交易次数：              {}\n  \
盈利次数：                {}\n  \
亏损次数：                {}\n  \
胜率：               {:.2}%\n  \
盈亏比：              {:.2}\n\n\
盈亏分析:\n  \
平均盈亏：          {:.2}\n  \
平均盈利：         {:.2}\n  \
平均亏损：          {:.2}\n  \
最大盈利：         {:.2}\n  \
最大亏损：         {:.2}\n\n{sep}",
        m.total_return * 100.0,
        m.total_return * 100.0,
        m.annualized_return * 100.0,
        m.max_drawdown * 100.0,
        m.daily_return_std * 100.0,
        m.sharpe_ratio,
        m.sortino_ratio,
        m.total_trades,
        m.winning_trades,
        m.losing_trades,
        m.win_rate * 100.0,
        m.profit_factor,
        m.avg_trade_pnl,
        m.avg_win,
        m.avg_loss,
        m.largest_win,
        m.largest_loss,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
    use strategy_store::Clock;

    fn t(y: i32, mo: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, 0, 0, 0).unwrap()
    }

    fn day(date: &str, eq: f64) -> EquityPoint {
        let d = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        let secs = Utc
            .from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap())
            .timestamp();
        EquityPoint {
            id: daily_equity_id("st", date),
            strategy_name: "st".into(),
            kind: "daily".into(),
            label: date.into(),
            bucket_secs: secs,
            equity: eq,
            cash: eq,
        }
    }

    fn trade(pnl: f64) -> TradeRow {
        TradeRow::new(
            1,
            "st",
            "s",
            "BTCUSDT",
            "SELL_CLOSE",
            1.0,
            100.0,
            0.0,
            0.0,
            pnl,
            "2026-01-01T00:00:00",
            "close",
            "sig_x",
        )
    }

    /// K 线时钟（与引擎 CandleClock 同构：喂入时间即 Store 元数据时间）。
    struct FixedClock(AtomicI64);
    impl Clock for FixedClock {
        fn now_ms(&self) -> i64 {
            self.0.load(AtomicOrdering::SeqCst)
        }
    }

    fn sink_at(clock_ms: i64) -> (tempfile::TempDir, Arc<Store>, EquitySink) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::new(
            dir.path(),
            Arc::new(FixedClock(AtomicI64::new(clock_ms))),
        ));
        let sink = EquitySink::new("st", &store);
        (dir, store, sink)
    }

    #[test]
    fn metrics_known_series() {
        // 1000 → 1100：三天，日收益 +5%, +4.545%
        let de = vec![
            day("2026-01-01", 1000.0),
            day("2026-01-02", 1050.0),
            day("2026-01-03", 1100.0),
        ];
        let m = compute_metrics(1000.0, &de, &[]);
        assert!((m.total_return - 0.10).abs() < 1e-9);
        assert_eq!(m.trading_days, 3);
        // sharpe: mean/std*sqrt(252)
        let r: [f64; 2] = [0.05, 0.05 / 1.05];
        let mean = (r[0] + r[1]) / 2.0;
        let var = ((r[0] - mean).powi(2) + (r[1] - mean).powi(2)) / 1.0;
        let expected = (mean / var.sqrt()) * 252f64.sqrt();
        assert!((m.sharpe_ratio - expected).abs() < 1e-9);
        // 年化 (1.1)^(365/3)-1
        let expected_ann = 1.1f64.powf(365.0 / 3.0) - 1.0;
        assert!((m.annualized_return - expected_ann).abs() < 1e-9);
    }

    #[test]
    fn metrics_win_rate_ignores_zero_pnl() {
        // 开仓行 pnl=0 不计入胜率（Python close_trades 口径）
        let trades = vec![trade(0.0), trade(100.0), trade(-50.0)];
        let de = vec![day("2026-01-01", 1000.0)];
        let m = compute_metrics(1000.0, &de, &trades);
        assert_eq!(m.total_trades, 2);
        assert_eq!(m.winning_trades, 1);
        assert_eq!(m.losing_trades, 1);
        assert!((m.win_rate - 0.5).abs() < 1e-9);
        // 盈亏比 = 100/50
        assert!((m.profit_factor - 2.0).abs() < 1e-9);
        assert!((m.largest_win - 100.0).abs() < 1e-9);
        assert!((m.largest_loss + 50.0).abs() < 1e-9);
    }

    #[test]
    fn metrics_drawdown() {
        // 峰 1200 → 谷 900：回撤 25%
        let de = vec![
            day("2026-01-01", 1000.0),
            day("2026-01-02", 1200.0),
            day("2026-01-03", 900.0),
        ];
        let m = compute_metrics(1000.0, &de, &[]);
        assert!((m.max_drawdown - 0.25).abs() < 1e-9);
    }

    #[test]
    fn equity_sink_daily_and_tf_buckets() {
        let (_dir, _store, sink) = sink_at(t(2026, 1, 1).timestamp_millis());
        // 1h 周期：00:00 桶（00:00 与 10:59 同桶），次日 00:00 新桶
        sink.on_1m_close(t(2026, 1, 1), 100.0, 100.0, 60);
        let t2 = Utc.with_ymd_and_hms(2026, 1, 1, 10, 59, 0).unwrap();
        sink.on_1m_close(t2, 110.0, 110.0, 60);
        let t3 = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
        sink.on_1m_close(t3, 120.0, 120.0, 60);

        let daily = sink.daily();
        assert_eq!(daily.len(), 2); // 01-01, 01-02
        assert_eq!(daily[0].label, "2026-01-01");
        assert_eq!(daily[0].equity, 110.0); // 日内 upsert 刷新
        assert_eq!(daily[1].equity, 120.0);

        // 1h 桶：01-01 00:00、01-01 10:00、01-02 00:00 → 3 行
        let tf = sink.tf();
        assert_eq!(tf.len(), 3);
        assert_eq!(tf[0].label, "2026-01-01 00:00");
        assert_eq!(tf[0].equity, 100.0); // 00:00 桶只有一根
        assert_eq!(tf[1].label, "2026-01-01 10:00"); // 桶起点，非 10:59
        assert_eq!(tf[1].equity, 110.0); // 10:00 桶刷新到 10:59 的值
        assert_eq!(tf[2].label, "2026-01-02 00:00");
    }

    #[test]
    fn trade_id_is_deterministic_and_duplicate_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let clock_ms = t(2026, 1, 1).timestamp_millis();
        let store = Arc::new(Store::new(
            dir.path(),
            Arc::new(FixedClock(AtomicI64::new(clock_ms))),
        ));
        let table = store.table::<TradeRow>();
        assert_eq!(trade_row_id("sig_1"), "trade_sig_1");
        let row = TradeRow::new(
            1,
            "st",
            "s",
            "BTCUSDT",
            "BUY",
            1.0,
            100.0,
            0.04,
            0.0,
            0.0,
            "2026-01-01T00:00:00",
            "open",
            "sig_1",
        );
        table.insert(row.clone()).unwrap();
        // 同信号二次成交 = 主键冲突（确定性 id 不允许覆盖）
        assert!(table.insert(row).is_err());
        let got = table.get("trade_sig_1").unwrap();
        assert_eq!(got.trade_id, "T00000001");
        assert_eq!(got.comment, "open");
    }

    /// 确定性红线：同序列 K 线时间喂两遍（各自独立 Store），
    /// flush 出的 trade.csv / equity.csv 逐字节一致。
    #[test]
    fn sink_replay_flushes_byte_identical() {
        let run = || -> Vec<u8> {
            let dir = tempfile::tempdir().unwrap();
            let clock = Arc::new(FixedClock(AtomicI64::new(0)));
            let store = Arc::new(Store::new(dir.path(), Arc::clone(&clock) as Arc<dyn Clock>));
            let sink = EquitySink::new("st", &store);
            let trades = store.table::<TradeRow>();
            let inputs = [
                (t(2026, 1, 1), 100.0, 100.0),
                (
                    Utc.with_ymd_and_hms(2026, 1, 1, 10, 59, 0).unwrap(),
                    110.0,
                    110.0,
                ),
                (t(2026, 1, 2), 120.0, 120.0),
            ];
            for (i, (ts, eq, cash)) in inputs.into_iter().enumerate() {
                clock.0.store(ts.timestamp_millis(), AtomicOrdering::SeqCst);
                sink.on_1m_close(ts, eq, cash, 60);
                trades
                    .insert(TradeRow::new(
                        i + 1,
                        "st",
                        "s",
                        "BTCUSDT",
                        "BUY",
                        1.0,
                        eq,
                        0.04,
                        0.0,
                        eq - 100.0,
                        &ts.format("%Y-%m-%dT%H:%M:%S").to_string(),
                        "open",
                        &format!("sig_{i}"),
                    ))
                    .unwrap();
            }
            store.flush().unwrap();
            // flush 日 = 时钟最后停在的 01-02；两表都落该日目录
            let mut bytes = std::fs::read(dir.path().join("20260102/trade.csv")).unwrap();
            bytes.extend(std::fs::read(dir.path().join("20260102/equity.csv")).unwrap());
            // 持有 TempDir 到读取完成（dir 随函数结束删除）
            drop(dir);
            bytes
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn report_generator_no_longer_writes_legacy_csvs() {
        let dir = tempfile::tempdir().unwrap();
        let g = ReportGenerator::new(dir.path());
        // 空输入也能产出报告；旧三张 CSV 文件名不得出现
        g.generate(
            "s",
            "BTCUSDT",
            "2026-01-01",
            "2026-01-02",
            1000.0,
            Path::new("."),
            "1h",
            &[],
            &[],
            &[],
            0,
            t(2026, 1, 1),
            t(2026, 1, 2),
            None,
        )
        .unwrap();
        for stale in [
            "backtest_equity.csv",
            "backtest_trades.csv",
            "1h_equity.csv",
        ] {
            assert!(!dir.path().join(stale).exists(), "{stale} 不应再产出");
        }
        assert!(dir.path().join("backtest_result.json").exists());
    }
}
