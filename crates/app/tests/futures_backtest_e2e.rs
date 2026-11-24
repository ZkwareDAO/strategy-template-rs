//! 期货策略（futures_single_trend / futures_xsec_trend）生产 CLI 回测端到端。
//!
//! 覆盖：`config/ctp_single_cu` 与 `config/ctp_multi_xsec` 真实样例配置 →
//! 合成 CTP 形态 1m（按品种价格量级缩放）→ `strategy-rs backtest` →
//! 统一报告 + 图表产物（`charts/backtest_*`）。图表依赖系统 DejaVu 字体，
//! 缺失时只断言 charts 目录已创建（plotters 无字体无法出图）。

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use common::{repo_contracts_yaml, synthetic_ctp_1m};
use strategy_core::candle::{Candle, Timeframe};
use strategy_data::csv_store;

const FONT_DEJAVU: &str = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";

/// 递归查找首个名为 `name` 的文件
fn find_file(root: &Path, name: &str) -> Option<PathBuf> {
    let rd = std::fs::read_dir(root).ok()?;
    let mut dirs = Vec::new();
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            dirs.push(p);
        } else if p.file_name().and_then(|s| s.to_str()) == Some(name) {
            return Some(p);
        }
    }
    dirs.into_iter().find_map(|d| find_file(&d, name))
}

/// 按目标价格量级缩放合成 CU 形态 1m（仅测试用：品种价格水平影响手数取整）。
fn scaled_ctp_1m(days: i64, base: f64) -> Vec<Candle> {
    let factor = base / 78_000.0;
    synthetic_ctp_1m(days)
        .into_iter()
        .map(|c| Candle {
            open: c.open * factor,
            high: c.high * factor,
            low: c.low * factor,
            close: c.close * factor,
            ..c
        })
        .collect()
}

/// 把仓库样例配置目录复制到临时 config root（父目录带 contracts.yaml）。
fn stage_config(tmp: &Path, group: &str) -> PathBuf {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
    let cfg_root = tmp.join("config");
    std::fs::create_dir_all(cfg_root.join(group)).unwrap();
    std::fs::copy(repo_contracts_yaml(), cfg_root.join("contracts.yaml"))
        .expect("复制仓库 config/contracts.yaml");
    for f in ["strategy.yaml", "_global.yaml"] {
        std::fs::copy(repo.join(group).join(f), cfg_root.join(group).join(f))
            .expect("复制样例配置");
    }
    cfg_root.join(group)
}

/// 按 spec 的 venue_key 写命名空间布局 1m CSV。
fn seed_klines(cfg: &Path, data_dir: &Path, base_prices: &[(&str, f64)]) {
    let spec = strategy_app::strategy_config::load_strategy(cfg).expect("样例配置应可加载");
    assert_eq!(spec.instruments.len(), base_prices.len(), "universe 行数");
    for (ispec, &(symbol, base)) in spec.instruments.iter().zip(base_prices) {
        assert_eq!(ispec.symbol, symbol);
        csv_store::save_csv(
            csv_store::csv_path(data_dir, &ispec.venue_key, Timeframe::M1),
            &scaled_ctp_1m(16, base),
        )
        .unwrap_or_else(|e| panic!("写入 {symbol} 合成 1m 失败: {e}"));
    }
}

fn run_backtest(cfg: &Path, data_dir: &Path, out_dir: &Path) {
    let output = Command::new(env!("CARGO_BIN_EXE_strategy-rs"))
        .arg("backtest")
        .arg("--config")
        .arg(cfg)
        .args(["--warmup-start", "20260601"])
        .args(["--start", "20260609"])
        .args(["--end", "20260613"])
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--output-dir")
        .arg(out_dir)
        .output()
        .expect("启动 strategy-rs backtest");
    assert!(
        output.status.success(),
        "backtest 应成功：\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn run_dir_and_assert_charts(out_dir: &Path) -> PathBuf {
    let result = find_file(out_dir, "backtest_result.json").expect("backtest_result.json");
    let run_dir = result.parent().unwrap().to_path_buf();
    assert!(
        run_dir.join("backtest_report.txt").is_file(),
        "缺 backtest_report.txt"
    );
    let charts = run_dir.join("charts");
    assert!(charts.is_dir(), "缺 charts 目录：{charts:?}");
    if Path::new(FONT_DEJAVU).exists() {
        // 有字体才可能真正出图；缺字体时 generate_charts 返回 Err 被吞掉。
        let mut pngs = 0;
        for e in std::fs::read_dir(&charts).unwrap().flatten() {
            if e.path().extension().and_then(|x| x.to_str()) == Some("png") {
                pngs += 1;
            }
        }
        assert!(
            pngs >= 2,
            "charts 至少应有权益曲线+回撤两张 PNG，实得 {pngs}"
        );
        assert!(
            charts.join("backtest_drawdown.png").is_file(),
            "缺回撤图：{charts:?}"
        );
    }
    run_dir
}

#[test]
fn futures_single_trend_cli_backtest_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = stage_config(tmp.path(), "tpl_trend_futures");
    let data_dir = tmp.path().join("klines");
    seed_klines(&cfg, &data_dir, &[("CU", 78_000.0)]);
    let out_dir = tmp.path().join("out");
    run_backtest(&cfg, &data_dir, &out_dir);
    let run_dir = run_dir_and_assert_charts(&out_dir);
    eprintln!("futures_single 摘要：run_dir={}", run_dir.display());
}

#[test]
fn futures_xsec_trend_cli_backtest_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = stage_config(tmp.path(), "tpl_xsec_futures");
    let data_dir = tmp.path().join("klines");
    // 品种价格量级接近真实，避免 1 手名义额远超组合预算导致永不成交。
    seed_klines(
        &cfg,
        &data_dir,
        &[("RB", 3_500.0), ("I", 800.0), ("CU", 78_000.0)],
    );
    let out_dir = tmp.path().join("out");
    run_backtest(&cfg, &data_dir, &out_dir);
    let run_dir = run_dir_and_assert_charts(&out_dir);
    eprintln!("futures_xsec 摘要：run_dir={}", run_dir.display());
}
