//! strategy-app 集成测试 — CTP（CU）回测端到端（统一端口 + 生产 CLI）
//!
//! 链路：临时 config 目录（`example_dual_ma_cu/strategy.yaml` 副本 + 仓库 contracts.yaml）
//! → `strategy_config::load_strategy` 校验合约透传 → 合成 CTP 形态 1m 写入命名空间
//! `data_dir/1m/SHFE/cn_futures/CU_1m.csv` → 调用 `strategy-rs backtest` 子命令
//! → 校验统一报告与 `history_position` 产物。

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use common::{repo_contracts_yaml, synthetic_ctp_1m};
use strategy_core::candle::Timeframe;
use strategy_data::csv_store;

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

/// 递归查找第一个满足谓词的文件
fn find_where(root: &Path, pred: &dyn Fn(&Path) -> bool) -> Option<PathBuf> {
    let rd = std::fs::read_dir(root).ok()?;
    let mut dirs = Vec::new();
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            dirs.push(p);
        } else if pred(&p) {
            return Some(p);
        }
    }
    dirs.into_iter().find_map(|d| find_where(&d, pred))
}

#[test]
fn ctp_backtest_cli_end_to_end_cu() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg_root = tmp.path().join("config");
    std::fs::create_dir_all(cfg_root.join("example_dual_ma_cu")).unwrap();
    std::fs::copy(repo_contracts_yaml(), cfg_root.join("contracts.yaml"))
        .expect("复制仓库 config/contracts.yaml");
    let repo_cfg = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/example_dual_ma_cu");
    for f in ["strategy.yaml", "_global.yaml"] {
        std::fs::copy(
            repo_cfg.join(f),
            cfg_root.join("example_dual_ma_cu").join(f),
        )
        .expect("复制统一端口示例配置");
    }
    let cfg = cfg_root.join("example_dual_ma_cu/strategy.yaml");

    // ① 生产配置解析 + 合约透传
    let spec = strategy_app::strategy_config::load_strategy(&cfg).expect("横截面配置应可解析");
    assert_eq!(spec.instruments.len(), 1);
    assert_eq!(spec.instruments[0].symbol, "CU");
    assert_eq!(spec.instruments[0].timeframe, "1h");
    let contract = spec.instruments[0]
        .contract
        .as_ref()
        .expect("CU 合约须经 config/contracts.yaml 自动加载");
    assert_eq!(contract.multiplier, 5.0, "CU 乘数 5 吨/手");

    // ② 合成 CTP 形态 1m 写入命名空间布局
    let data_dir = tmp.path().join("klines");
    let bars = synthetic_ctp_1m(12);
    csv_store::save_csv(
        csv_store::csv_path(&data_dir, "SHFE/cn_futures/CU", Timeframe::M1),
        &bars,
    )
    .expect("写入合成 1m");

    // ③ 生产 CLI 回测（统一入口只接受 strategy.yaml）
    let out_dir = tmp.path().join("out");
    let output = Command::new(env!("CARGO_BIN_EXE_strategy-rs"))
        .current_dir(tmp.path())
        .arg("backtest")
        .arg("--config")
        .arg(&cfg)
        .args(["--warmup-start", "20260601"])
        .args(["--start", "20260609"])
        .args(["--end", "20260612"])
        .arg("--data-dir")
        .arg(&data_dir)
        .arg("--output-dir")
        .arg(&out_dir)
        .output()
        .expect("启动 strategy-rs backtest");
    assert!(
        output.status.success(),
        "backtest 应成功：\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    // ④ 统一报告产物
    let result = find_file(&out_dir, "backtest_result.json").expect("backtest_result.json");
    let run_dir = result.parent().unwrap().to_path_buf();
    assert!(
        run_dir.join("backtest_report.txt").is_file(),
        "缺 backtest_report.txt"
    );

    // ⑤ history_position：Store 快照 + 对外 22 列 CSV（回测也写）
    assert!(
        find_file(&out_dir, "history_position.csv").is_some(),
        "Store 平仓历史快照必须落盘"
    );
    let hist_csv = find_where(&out_dir, &|p| {
        p.to_string_lossy().contains("history_positions")
            && p.extension().and_then(|e| e.to_str()) == Some("csv")
    });
    if let Some(p) = hist_csv {
        let header = std::fs::read_to_string(&p)
            .unwrap()
            .lines()
            .next()
            .unwrap_or("")
            .to_string();
        assert!(header.contains("position_id") && header.contains("exit_price"));
        assert!(header.contains("entry_size") && header.contains("exit_size"));
    }

    eprintln!("e2e 摘要：run_dir={}", run_dir.display());
}
