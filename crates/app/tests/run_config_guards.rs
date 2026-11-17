//! `run` 启动期配置校验的 CLI 级钉死案（统一端口 strategy.yaml 口径）。
//!
//! 背景见 `docs/exchange-spi-retirement.md` §7 风险 8：交易端点未配置时必须
//! live 拒绝启动、非 live 出声告警；真实端点 + `submit_enabled: false` 的
//! 紧急熔断不得被误杀。
//!
//! 每个子进程都 `current_dir` 到临时目录，避开仓库根 `config/_global.yaml` 与 `.env`。

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tempfile::TempDir;

/// 统一端口策略体（可被加载/校验；策略用真实注册名）。
const STRATEGY_BODY: &str = r#"strategy_type: example_dual_ma
strategy_id: example_dual_ma:guard
universe:
  - BTCUSDT
timeframes: [1h, 1m]
indicators: {}
entry: { default: [] }
exit: { default: [] }
portfolio_exit: { stop_loss_pct: 8 }
risk: { max_total_notional: 0.8 }
sizing: { per_position_pct: 0.1 }
params:
  fast_period: 10
  slow_period: 30
  stop_loss_pct: 2.0
  exit_price_source: close
"#;

/// 组基线：交易端点块在但字段全缺（回落占位 ws_url）。
const NO_TRADING_GLOBAL: &str = r#"services:
  trading:
    request_timeout_ms: 500
"#;

/// 仍带 `services.kline` 的存量组基线（klines-service 已退役）。
const TOMBSTONE_GLOBAL: &str = r#"services:
  kline:
    backfill_url: https://example.invalid/api
    ws_url: wss://example.invalid/ws
    market: crypto
  trading:
    ws_url: ws://127.0.0.1:59100/ws
    strategy_secret: x
"#;

/// 真实端点 + `submit_enabled: false`（RUNBOOK §2.1 紧急熔断形态）。
const FUSE_GLOBAL: &str = r#"services:
  trading:
    ws_url: ws://127.0.0.1:59100/ws
    submit_enabled: false
    strategy_secret: fuse-secret
"#;

fn write_cfg(dir: &Path, name: &str, global: Option<&str>) -> String {
    let group = dir.join(name);
    std::fs::create_dir_all(&group).expect("创建配置组目录");
    std::fs::write(group.join("strategy.yaml"), STRATEGY_BODY).expect("写入 strategy.yaml");
    if let Some(g) = global {
        std::fs::write(group.join("_global.yaml"), g).expect("写入 _global.yaml");
    }
    group.to_string_lossy().to_string()
}

/// 跑 `run`：等待启动期校验的结论（拒绝会立刻退出；放行则超时后发 SIGINT 走优雅关停）。
fn run_cli(cfg: &str, mode: &str, tmp: &Path) -> (Option<std::process::ExitStatus>, String) {
    run_cli_inner(cfg, Some(mode), tmp)
}

/// 不传 `--mode` 的变体：用于钉死「各组 mode 解析不一致」的拒启。
#[cfg(unix)]
fn run_cli_no_mode(cfg: &str, tmp: &Path) -> (Option<std::process::ExitStatus>, String) {
    run_cli_inner(cfg, None, tmp)
}

fn run_cli_inner(
    cfg: &str,
    mode: Option<&str>,
    tmp: &Path,
) -> (Option<std::process::ExitStatus>, String) {
    let bin = env!("CARGO_BIN_EXE_strategy-rs");
    let data = tmp.join("klines");
    let out = tmp.join("out");
    let mut cmd = Command::new(bin);
    cmd.current_dir(tmp)
        .args(["run", "--config", cfg])
        .args(["--data-dir", &data.to_string_lossy()])
        .args(["--output-dir", &out.to_string_lossy()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(m) = mode {
        cmd.args(["--mode", m]);
    }
    let mut child = cmd.spawn().expect("启动 strategy-rs run");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut status = None;
    loop {
        match child.try_wait().expect("try_wait") {
            Some(s) => {
                status = Some(s);
                break;
            }
            None if Instant::now() >= deadline => break,
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    if status.is_none() {
        // SIGINT 走 Ctrl-C 优雅关停分支（会 flush 状态并正常退出），否则只能硬杀。
        #[cfg(unix)]
        {
            let _ = Command::new("kill")
                .args(["-INT", &child.id().to_string()])
                .status();
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                match child.try_wait().expect("try_wait") {
                    Some(s) => {
                        status = Some(s);
                        break;
                    }
                    None => std::thread::sleep(Duration::from_millis(50)),
                }
            }
        }
    }
    if status.is_none() {
        let _ = child.kill();
    }
    let collected = child
        .wait_with_output()
        .expect("收集子进程输出（kill 后仍可读）");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&collected.stdout),
        String::from_utf8_lossy(&collected.stderr)
    );
    (status, text)
}

#[test]
fn layer_one_missing_services_block_fails_fast() {
    let tmp = TempDir::new().unwrap();
    let cfg = write_cfg(tmp.path(), "no_services", None);
    let (status, text) = run_cli(&cfg, "paper_trading", tmp.path());
    let st = status.expect("层一：services 缺失必须立即退出，不得挂起");
    assert!(
        !st.success(),
        "层一：无 services 应失败退出，实得 {st:?}\n输出：{text}"
    );
    assert!(
        text.contains("services.trading"),
        "层一文案应指向 services.trading 缺失，实际输出：{text}"
    );
}

#[test]
fn services_kline_tombstone_refused_at_startup() {
    let tmp = TempDir::new().unwrap();
    let cfg = write_cfg(tmp.path(), "tombstone", Some(TOMBSTONE_GLOBAL));
    let (status, text) = run_cli(&cfg, "paper_trading", tmp.path());
    let st = status.expect("tombstone 守卫：services.kline 存在必须立即退出");
    assert!(
        !st.success(),
        "services.kline 应拒启，实得 {st:?}\n输出：{text}"
    );
    assert!(
        text.contains("已退役") && text.contains("services.kline"),
        "报错必须点名退役与配置节，实际输出：{text}"
    );
}

#[test]
fn layer_two_live_with_unconfigured_trading_is_refused() {
    let tmp = TempDir::new().unwrap();
    let cfg = write_cfg(tmp.path(), "no_trading", Some(NO_TRADING_GLOBAL));
    let (status, text) = run_cli(&cfg, "live", tmp.path());
    let st = status.expect("方案 1：live + 占位交易端点必须被启动期拒绝");
    assert!(
        !st.success(),
        "方案 1：live + 未配置 trading 应拒绝启动，实得 {st:?}\n输出：{text}"
    );
    assert!(
        text.contains("拒绝启动") && text.contains("services.trading"),
        "方案 1 文案必须点名 services.trading 未配置，实际输出：{text}"
    );
    assert!(
        text.contains("submit_enabled") && text.contains("RUNBOOK"),
        "拒绝文案应指向紧急熔断的正确写法，实际输出：{text}"
    );
}

#[test]
fn layer_two_paper_with_unconfigured_trading_starts_and_warns() {
    let tmp = TempDir::new().unwrap();
    let cfg = write_cfg(tmp.path(), "no_trading_paper", Some(NO_TRADING_GLOBAL));
    let (status, text) = run_cli(&cfg, "paper_trading", tmp.path());
    if let Some(st) = status {
        assert!(
            !text.contains("拒绝启动"),
            "方案 2：paper 不得因未配置 trading 被拒绝启动（要出声，不是致命），\
             实得 {st:?}\n输出：{text}"
        );
    }
    assert!(
        text.contains("不会产生任何真实下单") || text.contains("未配置真实端点"),
        "方案 2：非 live 命中占位端点时必须有明确告警，实际输出：{text}"
    );
}

#[test]
fn emergency_fuse_with_real_endpoint_still_starts() {
    let tmp = TempDir::new().unwrap();
    let cfg = write_cfg(tmp.path(), "fuse", Some(FUSE_GLOBAL));
    let (_, text) = run_cli(&cfg, "live", tmp.path());
    assert!(
        !text.contains("拒绝启动"),
        "熔断形态（真实端点 + submit_enabled=false）不得被拒绝启动，实际输出：{text}"
    );
    assert!(
        !text.contains("未配置真实端点"),
        "熔断形态不应触发未配置告警，实际输出：{text}"
    );
}

/// 配置集目录（多组）走 `run_live_strategy_set` 多组启动路径：
/// 每组都须起得来；任何一组被拒或进程提前退出都算失败。
#[test]
fn set_dir_run_starts_all_groups_in_paper() {
    let tmp = TempDir::new().unwrap();
    let set = tmp.path().join("set");
    std::fs::create_dir_all(&set).unwrap();
    std::fs::write(
        set.join("_global.yaml"),
        "mode: paper_trading\naccount: acct_shared\nservices:\n  trading:\n    request_timeout_ms: 500\n",
    )
    .unwrap();
    for (name, symbol) in [("g1", "BTCUSDT"), ("g2", "ETHUSDT")] {
        let group = set.join(name);
        std::fs::create_dir_all(&group).unwrap();
        let body = STRATEGY_BODY
            .replace("example_dual_ma:guard", &format!("example_dual_ma:{name}"))
            .replace("- BTCUSDT", &format!("- {symbol}"));
        std::fs::write(group.join("strategy.yaml"), body).unwrap();
    }

    let (status, text) = run_cli(&set.to_string_lossy(), "paper_trading", tmp.path());
    // SIGINT 优雅关停后应正常退出；若实现退化为 SIGKILL 则 status 缺失。
    if let Some(st) = status {
        assert!(
            st.success(),
            "配置集 SIGINT 关停应成功退出：{st:?}\n输出：{text}"
        );
    }
    assert!(!text.contains("拒绝启动"), "配置集不得被拒启：{text}");
    assert!(
        !text.contains("旧 per-symbol"),
        "配置集不得落入旧单标的退役守卫：{text}"
    );
}

/// 各组未显式 `--mode` 覆盖时 mode 基线不一致必须拒启（不得挑一组悄悄跑）。
#[cfg(unix)]
#[test]
fn set_group_mode_mismatch_is_refused() {
    let tmp = TempDir::new().unwrap();
    let set = tmp.path().join("set");
    std::fs::create_dir_all(&set).unwrap();
    std::fs::write(
        set.join("_global.yaml"),
        "mode: paper_trading\naccount: acct_shared\nservices:\n  trading:\n    request_timeout_ms: 500\n",
    )
    .unwrap();
    for name in ["g1", "g2"] {
        let group = set.join(name);
        std::fs::create_dir_all(&group).unwrap();
        let body =
            STRATEGY_BODY.replace("example_dual_ma:guard", &format!("example_dual_ma:{name}"));
        std::fs::write(group.join("strategy.yaml"), body).unwrap();
    }
    // g2 组基线改为 live → 与 g1（继承 paper）解析结果不一致。
    std::fs::write(set.join("g2/_global.yaml"), "mode: live\n").unwrap();

    let (status, text) = run_cli_no_mode(&set.to_string_lossy(), tmp.path());
    let st = status.expect("组 mode 不一致必须立即退出");
    assert!(
        !st.success(),
        "组 mode 不一致应拒启，实得 {st:?}\n输出：{text}"
    );
    assert!(
        text.contains("不一致"),
        "报错须点名 mode 不一致，实际输出：{text}"
    );
}
