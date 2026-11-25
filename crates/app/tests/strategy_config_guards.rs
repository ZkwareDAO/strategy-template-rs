//! 横截面配置 loader 守卫测试（PR-2 验收矩阵：拒启面 + universe 解析 + 双读）。

use std::path::Path;

use strategy_app::config as legacy_config;
use strategy_app::strategy_config::{
    build_strategy, is_strategy_config_path, is_strategy_set_dir, load_strategy, load_strategy_set,
    validate_strategy_set,
};

fn base_yaml() -> String {
    r#"
strategy_type: template_xsec_ls
strategy_id: template_xsec_ls:default
timeframes: [15m, 1h]
indicators:
  adx: { tf: 15m, period: 14 }
  macd: { tf: 1h, fast: 12, slow: 26, signal: 9 }
entry:
  default: [ { ind: adx, op: ">", value: 20 } ]
exit:
  default: []
portfolio_exit:
  trailing: { arm_pnl_pct: 10, giveback_pct: 40 }
  stop_loss_pct: 8
rebalance: { every_bars: 96, top_k: 2, bottom_k: 0, weight_scheme: equal }
risk: { max_total_notional: 0.8 }
sizing: { per_position_pct: 0.1 }
"#
    .to_string()
}

fn universe(symbols: &str) -> String {
    format!("universe:\n{symbols}\n")
}

fn write(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
    std::fs::create_dir_all(dir).ok();
    let p = dir.join(name);
    std::fs::write(&p, content).unwrap();
    p
}

#[test]
fn parses_strategy_yaml_file_and_resolves_two_instruments() {
    let tmp = tempfile::tempdir().unwrap();
    let p = write(
        &tmp.path().join("strategy"),
        "strategy.yaml",
        &format!("{}{}", base_yaml(), universe("  - BTCUSDT\n  - ETHUSDT")),
    );
    let spec = load_strategy(&p).expect("双标的横截面配置应可加载");
    assert_eq!(spec.instruments.len(), 2);
    assert_eq!(
        spec.instruments[0].venue_key,
        "binance/crypto_perpetual/BTCUSDT"
    );
    assert_eq!(
        spec.instruments[1].venue_key,
        "binance/crypto_perpetual/ETHUSDT"
    );
    assert_eq!(spec.strategy_id, "template_xsec_ls:default");
    assert_eq!(
        spec.instruments[0].timeframe, "15m",
        "base TF = timeframes[0]"
    );
    assert_eq!(spec.instruments[0].mode, "backtest");
}

#[test]
fn parses_strategy_directory_with_global_fields() {
    let tmp = tempfile::tempdir().unwrap();
    // `_global.yaml` 放在策略目录的父目录（探测口径：同目录 → 父目录）。
    write(
        tmp.path(),
        "_global.yaml",
        "exchange: binance\naccount: acct_x\nleverage: 3\ncash: 42.0\nwarmup_days: 10\n",
    );
    let dir = tmp.path().join("strategy");
    write(
        &dir,
        "strategy.yaml",
        &format!("{}{}", base_yaml(), universe("  - BTCUSDT")),
    );
    let spec = load_strategy(&dir).expect("目录形式应可加载");
    // 按裁决：以 _global 派生字段断言回落生效（不新增 services 字段）。
    assert_eq!(spec.instruments[0].account, "acct_x");
    assert_eq!(spec.instruments[0].leverage, 3);
    assert_eq!(spec.instruments[0].cash, 42.0);
    assert_eq!(spec.warmup_days, 10);
    assert_eq!(spec.instruments[0].exchange, "binance");
}

#[test]
fn mixed_exchange_is_rejected_and_names_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let p = write(
        tmp.path(),
        "strategy.yaml",
        &format!(
            "{}{}",
            base_yaml(),
            universe(
                "  - { symbol: BTCUSDT, exchange: binance }\n  - { symbol: ETHUSDT, exchange: okx }"
            )
        ),
    );
    let err = load_strategy(&p).unwrap_err().to_string();
    assert!(err.contains("单一 exchange"), "{err}");
    assert!(err.contains("BTCUSDT") && err.contains("ETHUSDT"), "{err}");
}

#[test]
fn duplicate_symbol_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let p = write(
        tmp.path(),
        "strategy.yaml",
        &format!("{}{}", base_yaml(), universe("  - BTCUSDT\n  - BTCUSDT")),
    );
    let err = load_strategy(&p).unwrap_err().to_string();
    assert!(err.contains("重复"), "{err}");
    assert!(err.contains("binance/crypto_perpetual/BTCUSDT"), "{err}");
}

#[test]
fn multi_instance_mode_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let p = write(
        tmp.path(),
        "strategy.yaml",
        &format!(
            "mode: multi_instance\n{}{}",
            base_yaml(),
            universe("  - BTCUSDT")
        ),
    );
    let err = load_strategy(&p).unwrap_err().to_string();
    assert!(err.contains("multi_instance"), "{err}");
}

#[test]
fn directory_without_strategy_yaml_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    write(tmp.path(), "other.yaml", "strategy: whatever\n");
    let err = load_strategy(tmp.path()).unwrap_err().to_string();
    assert!(err.contains("strategy.yaml"), "{err}");
}

#[test]
fn non_whitelisted_exchange_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let p = write(
        tmp.path(),
        "strategy.yaml",
        &format!(
            "{}{}",
            base_yaml(),
            universe("  - { symbol: BTCUSDT, exchange: kraken }")
        ),
    );
    let err = load_strategy(&p).unwrap_err().to_string();
    assert!(err.contains("binance/SHFE"), "{err}");
}

#[test]
fn n1_old_strategy_uses_unified_registry() {
    let tmp = tempfile::tempdir().unwrap();
    let body = base_yaml()
        .replace(
            "strategy_type: template_xsec_ls\nstrategy_id: template_xsec_ls:default\n",
            "",
        )
        .replace(
            "rebalance: { every_bars: 96, top_k: 2, bottom_k: 0, weight_scheme: equal }\n",
            "",
        );
    let p = write(
        tmp.path(),
        "strategy.yaml",
        &format!(
            "strategy_type: template_trend\n{}{}",
            body,
            universe("  - BTCUSDT")
        ),
    );
    let spec = load_strategy(&p).expect("N=1 旧策略名可加载");
    // 统一后 N=1 与横截面共用 L1 仅硬止损配置（trailing 由 L3 负责）。
    assert!(
        !spec.instruments[0].risk.trailing_profit.enabled,
        "N=1 旧策略必须走横截面 L1（trailing 关闭）"
    );
    let strategy = build_strategy(&spec).expect("旧策略必须已在统一注册表");
    assert_eq!(strategy.strategy_type(), "template_trend");
}

#[test]
fn n2_unknown_strategy_lists_all_registered() {
    let tmp = tempfile::tempdir().unwrap();
    let p = write(
        tmp.path(),
        "strategy.yaml",
        &format!(
            "strategy_type: nope\n{}{}",
            base_yaml().replace("strategy_type: template_xsec_ls\n", ""),
            universe("  - BTCUSDT\n  - ETHUSDT")
        ),
    );
    let spec = load_strategy(&p).unwrap();
    let err = match build_strategy(&spec) {
        Ok(_) => panic!("未知策略必须报错"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("template_xsec_ls"), "{err}");
    assert!(err.contains("template_trend"), "{err}");
}

#[test]
fn old_dir_loader_ignores_strategy_yaml() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("config");
    write(
        &root.join("template_trend"),
        "BTCUSDT.yaml",
        "strategy: template_trend\ntimeframe: 8h\n",
    );
    let strategy_file = write(
        &root.join("template_trend").join("strategy"),
        "strategy.yaml",
        &format!("{}{}", base_yaml(), universe("  - BTCUSDT")),
    );
    let cfg = legacy_config::load(Some(&root)).expect("旧 loader 必须无视 strategy.yaml 正常合并");
    assert_eq!(cfg.symbols.len(), 1);
    assert_eq!(cfg.symbols[0].symbol, "BTCUSDT");
    assert!(is_strategy_config_path(&strategy_file));
    assert!(!is_strategy_config_path(
        &root.join("template_trend/BTCUSDT.yaml")
    ));
}

// ── PR-5 T6：run_mode 解析与单 account 守卫 ──

use strategy_app::strategy_config::{ensure_single_account, resolve_run_mode};

#[test]
fn strategy_run_mode_resolution_and_override() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("strategy");
    let p = write(
        &dir,
        "strategy.yaml",
        &format!("{}{}", base_yaml(), universe("  - BTCUSDT\n  - ETHUSDT")),
    );
    // 文件/全局均无 mode → 默认 paper_trading。
    let spec = load_strategy(&p).expect("加载");
    assert_eq!(spec.run_mode, "paper_trading");
    assert_eq!(resolve_run_mode(None, &spec).unwrap(), "paper_trading");
    // CLI 显式覆盖优先。
    assert_eq!(resolve_run_mode(Some("live"), &spec).unwrap(), "live");
    // smoking 仅属旧单标的演练模式 → 拒启且提示合法取值。
    let err = resolve_run_mode(Some("smoking"), &spec)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("paper_trading"),
        "smoking 报错必须提示合法模式，实际: {err}"
    );
    // _global.mode 作为基线生效；CLI 仍可覆盖回 paper。
    write(tmp.path(), "_global.yaml", "mode: live\n");
    let spec = load_strategy(&p).expect("加载（_global mode:live）");
    assert_eq!(spec.run_mode, "live", "_global.mode 生效");
    assert_eq!(resolve_run_mode(None, &spec).unwrap(), "live");
    assert_eq!(
        resolve_run_mode(Some("paper_trading"), &spec).unwrap(),
        "paper_trading",
        "CLI 覆盖 _global"
    );
}

#[test]
fn strategy_single_account_guard() {
    // 防御位直接单测：不同 account 必须点名两个值与 symbol。
    let pairs = vec![
        ("BTCUSDT".to_string(), "acct_a".to_string()),
        ("ETHUSDT".to_string(), "acct_b".to_string()),
    ];
    let err = ensure_single_account(&pairs).unwrap_err();
    assert!(
        err.contains("acct_a") && err.contains("acct_b") && err.contains("ETHUSDT"),
        "必须点名两 account 值与 symbol: {err}"
    );
    // 同 account → 放行；空 universe → 放行。
    assert!(ensure_single_account(&[
        ("BTCUSDT".into(), "acct_a".into()),
        ("ETHUSDT".into(), "acct_a".into()),
    ])
    .is_ok());
    assert!(ensure_single_account(&[]).is_ok());
}

// ── PR-6：配置集目录加载与跨组校验 ──

fn set_group_yaml(strategy_id: &str, symbols: &str) -> String {
    format!(
        "{}{}",
        base_yaml().replace(
            "strategy_id: template_xsec_ls:default",
            &format!("strategy_id: {strategy_id}")
        ),
        universe(symbols)
    )
}

/// 两组目录：a_group=BTCUSDT、b_group=ETHUSDT。
fn write_set(root: &Path) {
    for (name, symbols) in [("a_group", "  - BTCUSDT"), ("b_group", "  - ETHUSDT")] {
        write(
            &root.join(name),
            "strategy.yaml",
            &set_group_yaml(&format!("set_{name}"), symbols),
        );
    }
}

#[test]
fn set_dir_loads_groups_in_directory_name_order() {
    let tmp = tempfile::tempdir().unwrap();
    let set = tmp.path().join("set");
    write(
        &set,
        "_global.yaml",
        "account: acct_shared\nmode: paper_trading\n",
    );
    write_set(&set);
    assert!(is_strategy_set_dir(&set));
    let specs = load_strategy_set(&set).expect("set loads");
    let ids: Vec<&str> = specs.iter().map(|s| s.strategy_id.as_str()).collect();
    assert_eq!(ids, vec!["set_a_group", "set_b_group"]);
    validate_strategy_set(&specs, "paper_trading").expect("same account/mode/services");
}

#[test]
fn set_dir_rejects_stray_subdirectory_without_strategy_yaml() {
    let tmp = tempfile::tempdir().unwrap();
    let set = tmp.path().join("set");
    write_set(&set);
    std::fs::create_dir_all(set.join("legacy")).unwrap();
    write(&set.join("legacy"), "BTCUSDT.yaml", "strategy: example\n");
    let err = load_strategy_set(&set).unwrap_err().to_string();
    assert!(err.contains("legacy"), "报错须点名字目录: {err}");
}

#[test]
fn single_config_dir_is_not_a_set_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("one");
    write(
        &dir,
        "strategy.yaml",
        &format!("{}{}", base_yaml(), universe("  - BTCUSDT")),
    );
    assert!(is_strategy_config_path(&dir));
    assert!(!is_strategy_set_dir(&dir));
}

#[test]
fn set_validation_rejects_duplicate_strategy_id() {
    let tmp = tempfile::tempdir().unwrap();
    let set = tmp.path().join("set");
    write_set(&set);
    write(
        &set.join("b_group"),
        "strategy.yaml",
        &set_group_yaml("set_a_group", "  - ETHUSDT"),
    );
    let specs = load_strategy_set(&set).unwrap();
    let err = validate_strategy_set(&specs, "paper_trading")
        .unwrap_err()
        .to_string();
    assert!(err.contains("strategy_id"), "{err}");
}

#[test]
fn set_validation_rejects_cross_group_inconsistencies() {
    // b_group 自己的 _global.yaml 覆盖父目录；每 case 断言错误含对应字段名。
    // mode 基线不在此比较：未覆盖时由调用方按 resolve_run_mode 解析结果拒启，
    // CLI --mode 可统一覆盖（见 run_config_guards 的 CLI 级用例）。
    let cases = [
        ("account: acct_b\n", "account"),
        (
            "services:\n  trading:\n    ws_url: ws://other.example/ws\n",
            "services",
        ),
        (
            "state_flush_interval_secs: 5\n",
            "state_flush_interval_secs",
        ),
    ];
    for (group_global, needle) in cases {
        let tmp = tempfile::tempdir().unwrap();
        let set = tmp.path().join("set");
        write(
            &set,
            "_global.yaml",
            "account: acct_a\nmode: paper_trading\n",
        );
        write_set(&set);
        write(&set.join("b_group"), "_global.yaml", group_global);
        let specs = load_strategy_set(&set).unwrap();
        let err = validate_strategy_set(&specs, "paper_trading")
            .unwrap_err()
            .to_string();
        assert!(err.contains(needle), "case {needle}: {err}");
    }
}

#[test]
fn set_validation_live_requires_disjoint_symbols_paper_allows_overlap() {
    let tmp = tempfile::tempdir().unwrap();
    let set = tmp.path().join("set");
    write(
        &set,
        "_global.yaml",
        "account: acct_shared\nmode: paper_trading\n",
    );
    write(
        &set.join("a_group"),
        "strategy.yaml",
        &set_group_yaml("set_a", "  - BTCUSDT"),
    );
    write(
        &set.join("b_group"),
        "strategy.yaml",
        &set_group_yaml("set_b", "  - BTCUSDT"),
    );
    let specs = load_strategy_set(&set).unwrap();
    validate_strategy_set(&specs, "paper_trading").expect("paper overlap allowed");
    let err = validate_strategy_set(&specs, "live")
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("BTCUSDT") && err.contains("set_a") && err.contains("set_b"),
        "{err}"
    );
}

/// 组目录用符号链接指向别处是运维常见布局；必须跟随解析，不能静默漏载。
#[cfg(unix)]
#[test]
fn set_dir_follows_symlinked_group_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let set = tmp.path().join("set");
    write(
        &set,
        "_global.yaml",
        "account: acct_shared\nmode: paper_trading\n",
    );
    write(
        &set.join("a_group"),
        "strategy.yaml",
        &set_group_yaml("set_a_group", "  - BTCUSDT"),
    );
    let real = tmp.path().join("real_b");
    write(
        &real,
        "strategy.yaml",
        &set_group_yaml("set_b_group", "  - ETHUSDT"),
    );
    std::os::unix::fs::symlink(&real, set.join("b_group")).unwrap();

    assert!(is_strategy_set_dir(&set));
    let specs = load_strategy_set(&set).expect("符号链接组目录必须正常装载");
    let ids: Vec<&str> = specs.iter().map(|s| s.strategy_id.as_str()).collect();
    assert_eq!(ids, vec!["set_a_group", "set_b_group"]);
}

#[test]
fn template_xsec_ls_sample_config_loads_and_instantiates() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../config/tpl_xsec_futures/strategy.yaml");
    let spec = load_strategy(&path).expect("示例配置必须可加载");
    assert_eq!(spec.strategy_type, "template_xsec_ls");
    assert_eq!(spec.strategy_id, "template_xsec_ls:shfe");
    assert_eq!(spec.instruments.len(), 5);
    build_strategy(&spec).expect("模板必须已注册");
}

#[test]
fn tpl_trend_crypto_config_loads_without_rebalance() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../config/tpl_trend_crypto/strategy.yaml");
    let spec = load_strategy(&path).expect("多标配置必须可加载");
    assert_eq!(spec.strategy_type, "template_trend");
    assert_eq!(spec.strategy_id, "template_trend:multi");
    assert_eq!(spec.instruments.len(), 3);
    assert!(
        spec.config.rebalance.is_none(),
        "merge_positions=false 的逐标的独立总仓可不配 rebalance"
    );
    build_strategy(&spec).expect("模板必须已注册");
}

/// 仓库 `config/` 下每个带 strategy.yaml 的样例目录都必须能加载并实例化。
/// 新增样例配置忘注册/写错字段时，这里第一时间点名目录。
#[test]
fn every_repo_config_dir_loads_and_instantiates() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config");
    let mut checked = 0;
    for entry in std::fs::read_dir(&root).expect("读取 config/").flatten() {
        let dir = entry.path();
        if !dir.is_dir() || !dir.join("strategy.yaml").is_file() {
            continue;
        }
        let name = dir.file_name().unwrap().to_string_lossy().to_string();
        let spec = load_strategy(&dir).unwrap_or_else(|e| panic!("config/{name} 必须可加载：{e}"));
        build_strategy(&spec).unwrap_or_else(|e| panic!("config/{name} 必须可实例化：{e}"));
        checked += 1;
    }
    assert!(
        checked >= 3,
        "仓库样例配置目录过少（{checked}），扫描逻辑可能失效"
    );
}

#[test]
fn futures_templates_load_and_instantiate() {
    for (rel, strategy_type) in [
        (
            "../../config/tpl_trend_futures/strategy.yaml",
            "template_trend",
        ),
        (
            "../../config/tpl_xsec_futures/strategy.yaml",
            "template_xsec_ls",
        ),
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
        let spec = load_strategy(&path).expect("期货模板配置必须可加载");
        assert_eq!(spec.strategy_type, strategy_type);
        build_strategy(&spec).expect("期货模板必须已注册");
    }
}
