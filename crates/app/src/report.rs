//! 回测报告层 — 对齐 Python backtest_reporter.py + analyzer.py 产物
//!
//! S5：回测成交与净值全部 model 化进统一 Store（内存唯一真相，
//! flush 到 `{state_base}/{YYYYMMDD}/{model}.csv` 通用 5 列格式）：
//! - [`TradeRow`]     `NAME="trade"`：一次成交一行（只增不改），id=trade_{signal_id}
//! - [`EquityPoint`]  `NAME="equity"`：净值点，kind=daily|tf（同桶 upsert 刷新）
//!
//! 成交/净值的主口径落 Store（`state/{day}/trade.csv`、`equity.csv`）；
//! `backtest_equity.csv` / `backtest_trades.csv` 作为**扁平报表导出仍产出**
//! （`write_flat_exports`，对齐外部参考格式，钉于本文件测试与
//! `tests/backtest_e2e.rs`），有信号时另附 `backtest_signals.csv` 副本。
//! 只有旧的 `{tf}_equity.csv`（按周期分文件）不再产出。result.json /
//! backtest_report.txt / backtest_analysis_report.md / charts/*.png 作为回测
//! 报告产物保留，其数据全部从 Store 行汇总（目录结构仍对齐 Python
//! run_backtest.py）。
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

    /// P1-7：共享表二级索引键——多策略同表时按策略名快速过滤。
    fn scope(&self) -> Option<&str> {
        Some(&self.strategy_name)
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

    /// P1-7：共享表二级索引键——多策略同表时按策略名快速过滤。
    fn scope(&self) -> Option<&str> {
        Some(&self.strategy_name)
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
    ///
    /// `venue_key` = §6.1 命名空间键（本地 K 线数据路径维度，图表读数以它为准）。
    #[allow(clippy::too_many_arguments)]
    pub fn generate(
        &self,
        strategy_id: &str,
        symbol: &str,
        start_date: &str,
        end_date: &str,
        initial_cash: f64,
        data_dir: &Path,
        venue_key: &str,
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

        // S5：成交/净值主口径落统一 Store（state/{day}/trade.csv、equity.csv）；
        // 下方 write_flat_exports 仍产出 backtest_equity.csv / backtest_trades.csv
        // 两份扁平报表导出（外部脚本消费），仅旧的 {tf}_equity.csv 不再产出。

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
            venue_key,
            start_date,
            end_date,
        ) {
            tracing::warn!(error = %e, "图表生成失败（跳过 charts/）");
        }
        self.write_analysis_report(symbol, daily_equity, &metrics, initial_cash);

        // 7. 扁平导出（backtest_equity / backtest_trades / backtest_signals）
        if let Err(e) = self.write_flat_exports(daily_equity, trades) {
            tracing::warn!(error = %e, "扁平导出失败（backtest_*.csv）");
        }

        Ok(self.run_dir.to_path_buf())
    }

    /// 旧版三件套扁平导出（对齐外部参考格式）：
    /// - `backtest_equity.csv`：date,equity,cash（daily 净值）
    /// - `backtest_trades.csv`：trade_id,strategy_id,symbol,side,quantity,price,
    ///   commission,slippage,pnl,timestamp,comment
    /// - `backtest_signals.csv`：engine/signals/{name}/signal.csv 的扁平副本
    ///   （14 列 SignalRecord 口径；无信号时不生成）
    fn write_flat_exports(
        &self,
        daily_equity: &[EquityPoint],
        trades: &[TradeRow],
    ) -> io::Result<()> {
        use std::collections::BTreeMap;

        // equity：按日期去重（BTreeMap 按日期升序；同日后行覆盖前行）
        let mut eq: BTreeMap<String, (String, String)> = BTreeMap::new();
        for r in daily_equity {
            if r.kind == "daily" {
                eq.insert(
                    r.label.clone(),
                    (Self::fmt_num(r.equity), Self::fmt_num(r.cash)),
                );
            }
        }
        let mut w = csv::Writer::from_path(self.run_dir.join("backtest_equity.csv"))?;
        w.write_record(["date", "equity", "cash"])?;
        for (date, (e, c)) in &eq {
            w.write_record([date, e, c])?;
        }
        w.flush()?;

        // trades
        let mut w = csv::Writer::from_path(self.run_dir.join("backtest_trades.csv"))?;
        w.write_record([
            "trade_id",
            "strategy_id",
            "symbol",
            "side",
            "quantity",
            "price",
            "commission",
            "slippage",
            "pnl",
            "timestamp",
            "comment",
        ])?;
        for t in trades {
            w.write_record([
                &t.trade_id,
                &t.strategy_id,
                &t.symbol,
                &t.side,
                &Self::fmt_num(t.quantity),
                &Self::fmt_num(t.price),
                &Self::fmt_num(t.commission),
                &Self::fmt_num(t.slippage),
                &Self::fmt_num(t.pnl),
                &t.timestamp,
                &t.comment,
            ])?;
        }
        w.flush()?;

        // signals：复制 engine/signals/{name}/signal.csv（含 header，无则不生成）
        let src = self.run_dir.join("engine").join("signals");
        if let Ok(entries) = fs::read_dir(&src) {
            for e in entries.flatten() {
                let f = e.path().join("signal.csv");
                if f.exists() {
                    return fs::copy(&f, self.run_dir.join("backtest_signals.csv")).map(|_| ());
                }
            }
        }
        Ok(())
    }

    /// 数值输出：整数不带小数点，否则原样（对齐 signals::writer::fmt_f64 口径）
    fn fmt_num(v: f64) -> String {
        if v == v.trunc() && v.abs() < 1e15 {
            format!("{}", v as i64)
        } else {
            format!("{v}")
        }
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
            // 引擎不设冻结/保证金科目（现金台账只记已实现盈亏），故如实为 0；
            // 持仓是否在手由下面 position_count 表达，勿再用"末必空仓"假设。
            "frozen_cash": 0.0,
            "total_equity": daily_equity.last().map(|r| r.equity).unwrap_or(initial_cash),
            "peak_equity": peak_equity,
            "max_drawdown": metrics.max_drawdown,
            "position_count": end_positions(trades).0,
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
        symbol: &str,
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
        let final_cash = final_cash(daily_equity, initial_cash);
        let (pos_count, long_qty, short_qty) = end_positions(trades);
        let total_commission = trades.iter().fold(0.0_f64, |acc, t| acc + t.commission);
        lines.push("账户摘要".into());
        lines.push(dash.clone());
        lines.push(format!("  策略：{strategy_id}"));
        lines.push(format!("    期末权益：    {}", fmt_comma(final_eq, 2)));
        // 可用现金必须印**现金台账**的数，不是期末权益的副本：窗口末段常有未平仓单
        // （此时 equity = cash + 浮动盈亏），旧写法把两个不同科目印成同一个数。
        lines.push(format!("    可用现金：    {}", fmt_comma(final_cash, 2)));
        // 原「持仓保证金：0.00」为硬编码占位、且与未平仓事实矛盾，改印可由流水推导的两项。
        lines.push(format!(
            "    期末持仓：    {} 笔（{}）",
            pos_count,
            if pos_count == 0 {
                "空仓".to_string()
            } else {
                let mut parts = Vec::new();
                if long_qty > 0.0 {
                    parts.push(format!("多 {}", fmt_comma(long_qty, 6)));
                }
                if short_qty > 0.0 {
                    parts.push(format!("空 {}", fmt_comma(short_qty, 6)));
                }
                parts.join(" / ")
            }
        ));
        lines.push(format!("    未实现盈亏：  {:+.2}", final_eq - final_cash));
        lines.push(format!("    最大回撤：    {:.2}%", max_dd * 100.0));
        lines.push(format!("    交易次数：    {}", trades.len()));
        lines.push(format!(
            "    总手续费：    {}",
            fmt_comma(total_commission, 2)
        ));
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
        // 与账户摘要同一份流水推导（end_positions）：在途则列 标的+方向+数量，
        // 空仓才印「无持仓」。此前本段体硬编码「无持仓」——7ae0464 修了摘要/现金/
        // 保证金三处占位，这里是漏网的第四处，CU 实跑（末段在途多 1 手）自相矛盾暴露。
        if pos_count == 0 {
            lines.push("  无持仓".into());
        } else {
            if long_qty > 0.0 {
                lines.push(format!("  {symbol}  多 {}", fmt_comma(long_qty, 6)));
            }
            if short_qty > 0.0 {
                lines.push(format!("  {symbol}  空 {}", fmt_comma(short_qty, 6)));
            }
        }
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

    fn write_analysis_report(
        &self,
        symbol: &str,
        daily_equity: &[EquityPoint],
        m: &Metrics,
        initial_cash: f64,
    ) {
        let Some(first) = daily_equity.first() else {
            return;
        };
        // 峰值/谷值（全局 max/min，沿用 analyzer.py 的取样口径，但**不再叫最大回撤**）
        let peak_row = daily_equity
            .iter()
            .max_by(|a, b| a.equity.total_cmp(&b.equity))
            .unwrap_or(first);
        let trough_row = daily_equity
            .iter()
            .min_by(|a, b| a.equity.total_cmp(&b.equity))
            .unwrap_or(first);
        // analyzer.py 的原始口径：(全局峰 − 全局谷)/全局峰。
        // 它不是回撤——谷值可以出现在峰值**之前**（BTCUSDT 4h 实跑：谷 2024-01-04
        // 早于峰 2024-03-04 ⇒ "35.17%" 而真回撤只有 7.43%，风险被夸大约 4.7 倍）。
        // 数值保留为"权益振幅"，最大回撤一律用滚动峰谷口径（与 result.json 同源）。
        let amplitude = if peak_row.equity > 0.0 {
            (peak_row.equity - trough_row.equity) / peak_row.equity * 100.0
        } else {
            0.0
        };
        let drawdown = m.max_drawdown * 100.0;
        // 谷值早于峰值 ⇒ 振幅必大于回撤，此时值得对读者明说，避免拿振幅当回撤去比策略
        let amplitude_note = if trough_row.bucket_secs < peak_row.bucket_secs
            && amplitude > drawdown + 1e-9
        {
            format!(
                "\n> ⚠️ 谷值（{trough_d}）早于峰值（{peak_d}）：权益振幅 {amp:.2}% 大于最大回撤 {dd:.2}%，两者口径不同，比较策略时请用**最大回撤**。\n",
                trough_d = trough_row.label,
                peak_d = peak_row.label,
                amp = amplitude,
                dd = drawdown
            )
        } else {
            String::new()
        };
        let verdict = if m.total_return > 0.0 {
            "盈利"
        } else {
            "亏损"
        };
        let risk = if drawdown < 20.0 {
            "风险较低"
        } else if drawdown < 40.0 {
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
| 初始资金（收益基准） | {base} |
| 首日权益样本 | {init} |
| 期末权益 | {final} |
| 总收益率（期末权益 ÷ 初始资金 − 1） | {tr:.2}% |
| 最大回撤（滚动峰谷） | {dd:.2}% |
| 权益振幅（全局峰谷差，非回撤） | {amp:.2}% |

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
{note}
---

## 4. 分析结论

- **收益表现**: {verdict}
- **风险控制**: 最大回撤 {dd:.2}%，{risk}

---

*报告由 BacktestAnalyzer 自动生成*
"#,
            now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            days = m.trading_days,
            base = fmt_comma(initial_cash, 2),
            init = fmt_comma(first.equity, 2),
            final = fmt_comma(daily_equity.last().map(|r| r.equity).unwrap_or(0.0), 2),
            tr = m.total_return * 100.0,
            dd = drawdown,
            amp = amplitude,
            peak = fmt_comma(peak_row.equity, 2),
            peak_d = peak_row.label,
            trough = fmt_comma(trough_row.equity, 2),
            trough_d = trough_row.label,
            note = amplitude_note,
            verdict = verdict,
            risk = risk,
        );
        let _ = fs::write(self.run_dir.join("backtest_analysis_report.md"), content);
    }
}

/// result.json accounts.cash：期末现金（现金台账只记已实现盈亏，空仓时等于期末权益）
fn final_cash(daily_equity: &[EquityPoint], initial_cash: f64) -> f64 {
    daily_equity.last().map(|r| r.cash).unwrap_or(initial_cash)
}

/// 期末在途持仓：由成交流水推导，**不是硬编码 0**。
///
/// 方向语义取自 `spi_backtest::fills_to_trade_rows`：开多 `BUY`、平多 `SELL_CLOSE`、
/// 开空 `SELL`、平空 `BUY_CLOSE`。返回 `(笔数, 多量, 空量)`，空仓时为 `(0, 0.0, 0.0)`。
///
/// 为什么要有这个函数：回测窗口结束时最后一段仓位常未平仓（BTCUSDT 4h 实跑最后一笔
/// `BUY` 在 2024-03-24，窗口 03-31 收），此前 `position_count`/可用现金/持仓保证金
/// 三处都按"末必空仓"写死，于是同一份产物自相矛盾——`equity 7,363.10 ≠ cash 6,939.69`
/// 却报 0 笔持仓、现金印成权益。逐日核验后确认权益序列是对的
/// （浮动盈亏 = 净量 ×(当日末 1m 收盘 − 开仓价)，03-29/03-31 分别 272.77/423.41 全对上），
/// **错的是报表占位值**，故此处只改报表口径，不动引擎记账。
fn end_positions(trades: &[TradeRow]) -> (usize, f64, f64) {
    let (mut long, mut short) = (0.0_f64, 0.0_f64);
    for t in trades {
        match t.side.as_str() {
            "BUY" => long += t.quantity,
            "SELL_CLOSE" => long -= t.quantity,
            "SELL" => short += t.quantity,
            "BUY_CLOSE" => short -= t.quantity,
            // 未知标签（外部注入/新方向）不猜方向，宁可由下面的空仓判定暴露
            _ => {}
        }
    }
    // 1e-9 容差：浮点累加的整进整出不应留残余仓位（与 remote_matches_local 同口径）
    let eps = 1e-9;
    let l = long.max(0.0);
    let s = short.max(0.0);
    let (l, s) = (if l > eps { l } else { 0.0 }, if s > eps { s } else { 0.0 });
    ((l > 0.0) as usize + (s > 0.0) as usize, l, s)
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
        // 空输入也能产出报告；旧 {tf}_equity.csv 不再产出。
        // backtest_equity/backtest_trades 已恢复为扁平导出（空输入生成仅含 header 的文件），
        // backtest_signals 无信号时不生成。
        g.generate(
            "s",
            "BTCUSDT",
            "2026-01-01",
            "2026-01-02",
            1000.0,
            Path::new("."),
            "binance/crypto_perpetual/BTCUSDT",
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
        assert!(
            !dir.path().join("1h_equity.csv").exists(),
            "1h_equity.csv 不应再产出"
        );
        for flat in ["backtest_equity.csv", "backtest_trades.csv"] {
            assert!(dir.path().join(flat).exists(), "{flat} 应始终产出");
        }
        assert!(
            !dir.path().join("backtest_signals.csv").exists(),
            "无信号时 backtest_signals.csv 不生成"
        );
        assert!(dir.path().join("backtest_result.json").exists());
    }

    /// 扁平导出内容核验：equity 按日去重升序、trades 全列、signals 复制 signal.csv
    #[test]
    fn flat_exports_contents() {
        let dir = tempfile::tempdir().unwrap();
        let g = ReportGenerator::new(dir.path());
        let eq = vec![day("2026-01-01", 1000.0), day("2026-01-02", 1010.5)];
        let trades = vec![
            TradeRow::new(
                1,
                "st",
                "S",
                "CU",
                "BUY",
                0.5,
                100.0,
                0.25,
                0.0,
                0.0,
                "2026-01-01T00:00:00",
                "open",
                "sig_a",
            ),
            TradeRow::new(
                2,
                "st",
                "S",
                "CU",
                "SELL_CLOSE",
                0.5,
                101.0,
                0.25,
                0.0,
                0.5,
                "2026-01-02T00:00:00",
                "close",
                "sig_b",
            ),
        ];
        // 准备一份 signal.csv 供复制
        let sig_dir = dir.path().join("engine").join("signals").join("st");
        std::fs::create_dir_all(&sig_dir).unwrap();
        std::fs::write(
            sig_dir.join("signal.csv"),
            "strategy_name,signal_id\nst,sig_a\n",
        )
        .unwrap();

        g.generate(
            "st",
            "CU",
            "2026-01-01",
            "2026-01-02",
            1000.0,
            Path::new("."),
            "binance/cn_futures/CU",
            "2h",
            &eq,
            &[],
            &trades,
            10,
            t(2026, 1, 1),
            t(2026, 1, 2),
            None,
        )
        .unwrap();

        let eq_csv = std::fs::read_to_string(dir.path().join("backtest_equity.csv")).unwrap();
        assert_eq!(
            eq_csv,
            "date,equity,cash\n2026-01-01,1000,1000\n2026-01-02,1010.5,1010.5\n"
        );
        let tr_csv = std::fs::read_to_string(dir.path().join("backtest_trades.csv")).unwrap();
        assert!(tr_csv.starts_with("trade_id,strategy_id,symbol,side,quantity,price,commission,slippage,pnl,timestamp,comment\n"));
        assert!(tr_csv.contains("T00000001,S,CU,BUY,0.5,100,0.25,0,0,2026-01-01T00:00:00,open"));
        assert!(tr_csv
            .contains("T00000002,S,CU,SELL_CLOSE,0.5,101,0.25,0,0.5,2026-01-02T00:00:00,close"));
        let sig_csv = std::fs::read_to_string(dir.path().join("backtest_signals.csv")).unwrap();
        assert_eq!(sig_csv, "strategy_name,signal_id\nst,sig_a\n");
        // 报告账户摘要必须包含总手续费（两笔成交各 0.25）
        let report = std::fs::read_to_string(dir.path().join("backtest_report.txt")).unwrap();
        assert!(
            report.contains("总手续费：    0.50"),
            "backtest_report.txt 缺少总手续费字段: {report}"
        );
    }

    /// 现金 ≠ 权益时的净值样本（`day()` 把两者写死相等，正是旧缺陷漏网的原因）
    fn day_cash(date: &str, equity: f64, cash: f64) -> EquityPoint {
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
            equity,
            cash,
        }
    }

    fn row(side: &str, qty: f64, price: f64, pnl: f64) -> TradeRow {
        TradeRow::new(
            1,
            "st",
            "S",
            "BTCUSDT",
            side,
            qty,
            price,
            0.0,
            0.0,
            pnl,
            "2026-01-02T00:00:00",
            "",
            "sig_x",
        )
    }

    #[test]
    fn end_positions_derives_open_quantity_from_ledger() {
        // 全平 → 0 笔（浮点整进整出不留残渣）
        let flat = vec![
            row("BUY", 0.5, 100.0, 0.0),
            row("SELL_CLOSE", 0.5, 101.0, 0.5),
        ];
        assert_eq!(end_positions(&flat), (0, 0.0, 0.0));
        // 末笔开多在途
        let open_long = vec![
            row("BUY", 0.5, 100.0, 0.0),
            row("SELL_CLOSE", 0.5, 101.0, 0.5),
            row("BUY", 0.3, 102.0, 0.0),
        ];
        assert_eq!(end_positions(&open_long).0, 1);
        assert!((end_positions(&open_long).1 - 0.3).abs() < 1e-12);
        // 末笔开空在途（空头落在第三位）
        let open_short = vec![
            row("SELL", 0.7, 100.0, 0.0),
            row("BUY_CLOSE", 0.2, 99.0, 0.2),
        ];
        let (n, long, short) = end_positions(&open_short);
        assert_eq!(n, 1);
        assert_eq!(long, 0.0);
        assert!((short - 0.5).abs() < 1e-12);
        // 多空各一笔在途 ⇒ 2 笔
        let both = vec![row("BUY", 0.4, 100.0, 0.0), row("SELL", 0.6, 100.0, 0.0)];
        assert_eq!(end_positions(&both), (2, 0.4, 0.6));
    }

    /// 窗口末段未平仓时，报表三个科目必须各自为真：现金印现金、持仓数非 0、
    /// 权益−现金 = 未实现盈亏（旧写法把可用现金印成期末权益、持仓写死 0 笔）。
    #[test]
    fn open_position_at_end_is_reported_truthfully() {
        let dir = tempfile::tempdir().unwrap();
        let g = ReportGenerator::new(dir.path());
        let eq = vec![
            day_cash("2026-01-01", 1000.0, 1000.0),
            day_cash("2026-01-02", 1150.0, 1100.0),
        ];
        let trades = vec![
            row("BUY", 0.5, 1000.0, 0.0),
            row("SELL_CLOSE", 0.5, 1200.0, 100.0),
            row("BUY", 0.5, 1200.0, 0.0), // 在途，未平
        ];
        g.generate(
            "st",
            "BTCUSDT",
            "2026-01-01",
            "2026-01-02",
            1000.0,
            Path::new("."),
            "binance/crypto_perpetual/BTCUSDT",
            "2h",
            &eq,
            &[],
            &trades,
            10,
            t(2026, 1, 1),
            t(2026, 1, 2),
            None,
        )
        .unwrap();

        let report = std::fs::read_to_string(dir.path().join("backtest_report.txt")).unwrap();
        assert!(
            report.contains("可用现金：    1,100.00"),
            "可用现金必须是现金台账（1,100.00），不是期末权益的副本: {report}"
        );
        assert!(
            !report.contains("可用现金：    1,150.00"),
            "可用现金不得再等于期末权益: {report}"
        );
        assert!(
            report.contains("期末持仓：    1 笔（多 0.500000）"),
            "在途多头必须见数: {report}"
        );
        // 「期末持仓」专段必须与摘要同源流水：有在途多头时列出，不得宣称「无持仓」
        // （7ae0464 改了摘要/现金/保证金三处占位，专段是漏网的第四处——CU 实跑抓出）
        assert!(
            report.contains("  BTCUSDT  多 0.500000"),
            "期末持仓专段应列出在途多头: {report}"
        );
        assert!(
            !report.contains("无持仓"),
            "有持仓时专段不得宣称无持仓: {report}"
        );
        assert!(
            report.contains("未实现盈亏：  +50.00"),
            "权益−现金 = 浮动盈亏: {report}"
        );

        let raw = std::fs::read_to_string(dir.path().join("backtest_result.json")).unwrap();
        let json: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(json["accounts"][0]["position_count"].as_u64(), Some(1));
        assert_eq!(json["accounts"][0]["cash"], 1100.0);
        assert_eq!(json["accounts"][0]["total_equity"], 1150.0);
    }

    /// 空仓收尾（常见路径）：数字必须与改动前一致——现金=权益、0 笔、盈亏 0.00。
    #[test]
    fn flat_close_still_reports_cash_equal_equity() {
        let dir = tempfile::tempdir().unwrap();
        let g = ReportGenerator::new(dir.path());
        let eq = vec![
            day_cash("2026-01-01", 1000.0, 1000.0),
            day_cash("2026-01-02", 1100.0, 1100.0),
        ];
        let trades = vec![
            row("BUY", 0.5, 1000.0, 0.0),
            row("SELL_CLOSE", 0.5, 1200.0, 100.0),
        ];
        g.generate(
            "st",
            "BTCUSDT",
            "2026-01-01",
            "2026-01-02",
            1000.0,
            Path::new("."),
            "binance/crypto_perpetual/BTCUSDT",
            "2h",
            &eq,
            &[],
            &trades,
            10,
            t(2026, 1, 1),
            t(2026, 1, 2),
            None,
        )
        .unwrap();
        let report = std::fs::read_to_string(dir.path().join("backtest_report.txt")).unwrap();
        assert!(report.contains("可用现金：    1,100.00"), "{report}");
        assert!(report.contains("期末持仓：    0 笔（空仓）"), "{report}");
        assert!(
            report.contains("  无持仓"),
            "空仓收尾时专段应报无持仓: {report}"
        );
        assert!(report.contains("未实现盈亏：  +0.00"), "{report}");
        let json: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join("backtest_result.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(json["accounts"][0]["position_count"].as_u64(), Some(0));
    }

    /// 在途空头收尾：专段与摘要同样必须列「空」量（占位修复的对称分支）。
    #[test]
    fn open_short_at_end_lists_short_in_dedicated_section() {
        let dir = tempfile::tempdir().unwrap();
        let g = ReportGenerator::new(dir.path());
        let eq = vec![
            day_cash("2026-01-01", 1000.0, 1000.0),
            day_cash("2026-01-02", 1300.0, 1050.0),
        ];
        let trades = vec![
            row("SELL", 0.7, 1000.0, 0.0),
            row("BUY_CLOSE", 0.2, 990.0, 100.0), // 平空 0.2，余 0.5 在途
        ];
        g.generate(
            "st",
            "BTCUSDT",
            "2026-01-01",
            "2026-01-02",
            1000.0,
            Path::new("."),
            "binance/crypto_perpetual/BTCUSDT",
            "2h",
            &eq,
            &[],
            &trades,
            10,
            t(2026, 1, 1),
            t(2026, 1, 2),
            None,
        )
        .unwrap();
        let report = std::fs::read_to_string(dir.path().join("backtest_report.txt")).unwrap();
        assert!(
            report.contains("期末持仓：    1 笔（空 0.500000）"),
            "摘要段应见在途空头: {report}"
        );
        assert!(
            report.contains("  BTCUSDT  空 0.500000"),
            "期末持仓专段应列出在途空头: {report}"
        );
        assert!(
            !report.contains("无持仓"),
            "有持仓时专段不得宣称无持仓: {report}"
        );
    }

    /// 谷值早于峰值时，"全局峰谷差"必须标成权益振幅，不能顶"最大回撤"的名
    /// （BTCUSDT 实跑：振幅 35.17% vs 真回撤 7.43%，风险被夸大约 4.7 倍）。
    #[test]
    fn analysis_report_separates_drawdown_from_amplitude() {
        let dir = tempfile::tempdir().unwrap();
        let g = ReportGenerator::new(dir.path());
        // 100 → 70 → 200：滚动最大回撤 (100-70)/100 = 30%；全局振幅 (200-70)/200 = 65%
        // 取 30% 而非 40% 是为避开 20/40 分档边界：振幅会把判据推到"风险较高"，
        // 真回撤只应到"风险中等"——这一档差正是本案的断言点。
        let eq = vec![
            day_cash("2026-01-01", 100.0, 100.0),
            day_cash("2026-01-02", 70.0, 70.0),
            day_cash("2026-01-03", 200.0, 200.0),
        ];
        g.generate(
            "st",
            "BTCUSDT",
            "2026-01-01",
            "2026-01-03",
            100.0,
            Path::new("."),
            "binance/crypto_perpetual/BTCUSDT",
            "2h",
            &eq,
            &[],
            &[],
            10,
            t(2026, 1, 1),
            t(2026, 1, 3),
            None,
        )
        .unwrap();
        let md = std::fs::read_to_string(dir.path().join("backtest_analysis_report.md")).unwrap();
        assert!(
            md.contains("| 最大回撤（滚动峰谷） | 30.00% |"),
            "最大回撤须用滚动口径: {md}"
        );
        assert!(
            md.contains("| 权益振幅（全局峰谷差，非回撤） | 65.00% |"),
            "全局峰谷差须另列并标明非回撤: {md}"
        );
        assert!(
            md.contains(
                "谷值（2026-01-02）早于峰值（2026-01-03）：权益振幅 65.00% 大于最大回撤 30.00%"
            ),
            "两口径背离时须点名日期与数值: {md}"
        );
        assert!(
            md.contains("最大回撤 30.00%，风险中等"),
            "风险判据须取真回撤（30% ⇒ 风险中等），而非 65% 的振幅: {md}"
        );
        assert!(
            md.contains("总收益率（期末权益 ÷ 初始资金 − 1） | 100.00%"),
            "收益率须标明基准: {md}"
        );
    }
}
