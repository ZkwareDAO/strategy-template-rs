//! crypto 样例配置族的 CLI 回测矩阵：每个 `config/<group>/strategy.yaml` 真跑一遍。
//!
//! 覆盖 `tpl_trend_crypto`（单标）在统一端口下的完整装配、聚合、决策与报告链路。
//! 只断言运行成功与统一产物落盘，不断言收益（合成行情不追求 alpha）。

use std::path::{Path, PathBuf};
use std::process::Command;

use chrono::{Duration, TimeZone, Utc};
use strategy_core::candle::{Candle, Timeframe};
use strategy_data::csv_store;

/// 24/7 crypto 形态 1m：双正弦叠加，不同 symbol 相位/频率不同以产生横截面差异。
fn crypto_1m(days: i64, phase: f64, slow: f64) -> Vec<Candle> {
    let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    (0..days * 1440)
        .map(|k| {
            let t = start + Duration::minutes(k);
            let k = k as f64;
            let v = 100.0
                + 4.0 * (2.0 * std::f64::consts::PI * k / 480.0 + phase).sin()
                + 1.5 * (2.0 * std::f64::consts::PI * k / slow).sin();
            Candle {
                open_time: t,
                open: v,
                high: v + 0.2,
                low: v - 0.2,
                close: v,
                volume: 5.0,
                quote_volume: 0.0,
                trade_num: 0,
                active_buy_volume: 0.0,
                active_buy_quote_volume: 0.0,
                is_closed: true,
            }
        })
        .collect()
}

fn stage_config(tmp: &Path, group: &str) -> PathBuf {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
    let cfg_root = tmp.join("config");
    std::fs::create_dir_all(cfg_root.join(group)).unwrap();
    std::fs::copy(
        repo.join(group).join("strategy.yaml"),
        cfg_root.join(group).join("strategy.yaml"),
    )
    .expect("复制 strategy.yaml");
    let global = repo.join(group).join("_global.yaml");
    if global.is_file() {
        std::fs::copy(global, cfg_root.join(group).join("_global.yaml")).unwrap();
    }
    cfg_root.join(group)
}

fn seed_klines(cfg: &Path, data_dir: &Path) {
    let spec = strategy_app::strategy_config::load_strategy(cfg).expect("样例配置应可加载");
    for (i, ispec) in spec.instruments.iter().enumerate() {
        let phase = i as f64 * 1.7;
        csv_store::save_csv(
            csv_store::csv_path(data_dir, &ispec.venue_key, Timeframe::M1),
            &crypto_1m(16, phase, 337.0 + 61.0 * i as f64),
        )
        .unwrap_or_else(|e| panic!("写入 {} 合成 1m 失败: {e}", ispec.symbol));
    }
}

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

#[test]
fn crypto_sample_configs_run_cli_backtest_end_to_end() {
    for group in ["tpl_trend_crypto"] {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = stage_config(tmp.path(), group);
        let data_dir = tmp.path().join("klines");
        seed_klines(&cfg, &data_dir);
        let out_dir = tmp.path().join("out");

        let output = Command::new(env!("CARGO_BIN_EXE_strategy-rs"))
            .arg("backtest")
            .arg("--config")
            .arg(&cfg)
            .args(["--warmup-start", "20260101"])
            .args(["--start", "20260109"])
            .args(["--end", "20260113"])
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--output-dir")
            .arg(&out_dir)
            .output()
            .expect("启动 strategy-rs backtest");
        assert!(
            output.status.success(),
            "config/{group} 回测应成功：\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let run_dir = find_file(&out_dir, "backtest_result.json")
            .unwrap_or_else(|| panic!("config/{group} 缺 backtest_result.json"))
            .parent()
            .unwrap()
            .to_path_buf();
        assert!(
            run_dir.join("backtest_report.txt").is_file(),
            "config/{group} 缺 backtest_report.txt"
        );
        // 是否产生仓位取决于合成行情是否触发该策略；history_position 由其他 e2e 钉死。
    }
}
