//! CLI 入口守卫 + `download` 增量语义的端到端钉死（不触网）。
//!
//! 覆盖 `main.rs` 中 Download 分支（空 symbol/坏区间/坏日期/已是最新跳过）、
//! Backtest/Run 的旧 per-symbol 退役守卫与横截面覆盖参数拒启。
//! 网络下载路径（`Downloader::download_range`）无注入点，不在本文件覆盖。

use std::process::{Command, Output};

use strategy_core::candle::{Candle, Timeframe};
use strategy_data::csv_store;

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_strategy-rs"))
        .args(args)
        .output()
        .expect("启动 strategy-rs")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).to_string()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn expect_failure(out: Output, needle: &str) {
    assert!(!out.status.success(), "应失败退出：{}", stdout(&out));
    assert!(
        stderr(&out).contains(needle),
        "stderr 缺「{needle}」：\n{}",
        stderr(&out)
    );
}

fn candle_open_at_2026_06_01() -> Candle {
    Candle {
        open_time: chrono::TimeZone::with_ymd_and_hms(&chrono::Utc, 2026, 6, 1, 0, 0, 0).unwrap(),
        open: 100.0,
        high: 101.0,
        low: 99.0,
        close: 100.0,
        volume: 1.0,
        quote_volume: 0.0,
        trade_num: 0,
        active_buy_volume: 0.0,
        active_buy_quote_volume: 0.0,
        is_closed: true,
    }
}

#[test]
fn download_requires_at_least_one_symbol() {
    let out = run(&["download", "--start", "20260101", "--end", "20260102"]);
    expect_failure(out, "必须提供至少一个 --symbol");
}

#[test]
fn download_rejects_invalid_interval() {
    let out = run(&["download", "--symbol", "btcusdt", "--interval", "7x"]);
    expect_failure(out, "非法时间框架");
}

#[test]
fn download_rejects_invalid_date_format() {
    let out = run(&["download", "--symbol", "btcusdt", "--start", "2026-01-05"]);
    expect_failure(out, "日期格式应为 YYYYMMDD");
}

#[test]
fn download_skips_when_existing_csv_reaches_end() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("klines");
    // 与 main.rs 完全同口径：symbol 大写 + venue_key_for 派生命名空间。
    let venue_key = strategy_app::spi_backtest::venue_key_for("binance", None, "BTCUSDT");
    let path = csv_store::csv_path(&data_dir, &venue_key, Timeframe::M1);
    csv_store::save_csv(&path, &[candle_open_at_2026_06_01()]).unwrap();

    let out = run(&[
        "download",
        "--symbol",
        "btcusdt",
        "--end",
        "20260601",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "增量已是最新必须成功退出（不得触网）：{}",
        stderr(&out)
    );
    assert!(
        !stdout(&out).contains("共"),
        "已是最新时不应有下载汇总行：{}",
        stdout(&out)
    );
    // CSV 不得被改写。
    let after = csv_store::load_csv(&path).unwrap();
    assert_eq!(after.len(), 1, "已是最新时 CSV 不应被改写");
    assert_eq!(after[0].open_time, candle_open_at_2026_06_01().open_time);
}

#[test]
fn download_skips_when_explicit_start_not_earlier_than_end() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("klines");
    let out = run(&[
        "download",
        "--symbol",
        "ethusdt",
        "--start",
        "20260601",
        "--end",
        "20260601",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "start >= end 应跳过而非报错：{}",
        stderr(&out)
    );
    assert!(!stdout(&out).contains("共"));
    assert!(!data_dir.join("1m").exists(), "跳过路径不得创建数据目录");
}

#[test]
fn backtest_without_config_hits_legacy_retirement_guard() {
    let out = run(&["backtest", "--start", "20260101", "--end", "20260102"]);
    expect_failure(out, "旧 per-symbol 入口已退役");
}

#[test]
fn backtest_strategy_yaml_rejects_single_symbol_overrides() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
    std::fs::create_dir_all(tmp.path().join("cfg")).unwrap();
    let cfg = tmp.path().join("cfg/strategy.yaml");
    std::fs::copy(repo.join("tpl_xsec_crypto/strategy.yaml"), &cfg).unwrap();

    let out = run(&[
        "backtest",
        "--config",
        cfg.to_str().unwrap(),
        "--strategy",
        "template_xsec",
        "--start",
        "20260101",
        "--end",
        "20260102",
    ]);
    expect_failure(out, "不支持 --strategy/--symbol/--cash");
}

#[test]
fn backtest_set_dir_is_refused_with_run_hint() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
    let set = tmp.path().join("set");
    std::fs::create_dir_all(set.join("g1")).unwrap();
    std::fs::copy(
        repo.join("tpl_xsec_crypto/strategy.yaml"),
        set.join("g1/strategy.yaml"),
    )
    .unwrap();

    let out = run(&[
        "backtest",
        "--config",
        set.to_str().unwrap(),
        "--start",
        "20260101",
        "--end",
        "20260102",
    ]);
    expect_failure(out, "仅支持 run");
}

#[test]
fn run_strategy_yaml_rejects_universe_overrides() {
    let tmp = tempfile::tempdir().unwrap();
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
    std::fs::create_dir_all(tmp.path().join("cfg")).unwrap();
    let cfg = tmp.path().join("cfg/strategy.yaml");
    std::fs::copy(repo.join("tpl_xsec_crypto/strategy.yaml"), &cfg).unwrap();

    let out = run(&[
        "run",
        "--config",
        cfg.to_str().unwrap(),
        "--symbol",
        "BTCUSDT",
        "--mode",
        "paper_trading",
    ]);
    expect_failure(out, "横截面 run 不支持");
}

#[test]
fn run_without_config_hits_legacy_retirement_guard() {
    let out = run(&["run", "--mode", "paper_trading"]);
    expect_failure(out, "旧 per-symbol 入口已退役");
}
