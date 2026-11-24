//! 阶段 2（配置体系）app 侧验收：`_global.yaml` 的 `exchanges:` 分组 +
//! `instruments:` 清单能被加载、合并并装配出双交易所注册表（混跑入口）。
//! 旧单文件链路（无 exchanges）必须零回归。

use std::path::Path;

use strategy_app::config;

fn write(path: &Path, text: &str) {
    std::fs::write(path, text).expect("写入测试配置失败");
}

/// §8 形态的共享基线：两个交易所 + 混跑清单（ctp 不配 subscription，
/// 走直连计划——选主维度已由 engine 层测试覆盖）。
const GLOBAL_MIXED: &str = r#"
strategy: template_trend
timeframe: 8h

exchanges:
  binance:
    market: crypto_perpetual
    aggregation: utc_grid
    fee_rate: 0.0004
    symbols: [BTCUSDT]
  ctp:
    id: SHFE
    aggregation: session_window
    session:
      timezone_offset_hours: 8
      day_cutover: "20:00"
      windows:
        - { open: "09:00", close: "10:15", kind: day }
        - { open: "10:30", close: "11:30", kind: day }
        - { open: "13:30", close: "15:00", kind: day }
        - { open: "21:00", close: "01:00", kind: night }
    sessions:
      AU:
        windows:
          - { open: "09:00", close: "10:15", kind: day }
          - { open: "10:30", close: "11:30", kind: day }
          - { open: "13:30", close: "15:00", kind: day }
          - { open: "21:00", close: "02:30", kind: night }
    contracts:
      CU: { multiplier: 5, tick_size: 10, margin_rate: 0.11, min_lots: 1,
            commission: { mode: rate, open: 0.00005, close: 0.00005 } }

instruments:
  - { exchange: binance, product: BTCUSDT }
  - { exchange: ctp, product: CU2601, role: contract, strategy: cta_turtle }
"#;

#[test]
fn mixed_exchanges_config_assembles_in_one_process() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir(root.join("template_trend")).unwrap();
    write(&root.join("_global.yaml"), GLOBAL_MIXED);
    // 目录模式要求至少一个 per-symbol 文件（扁平 schema，symbol 取文件名；
    // 旧链路形状不变）
    write(
        &root.join("template_trend/BTCUSDT.yaml"),
        "timeframe: 1h\n",
    );

    let cfg = config::load(Some(root)).expect("目录模式加载必须成功");
    assert_eq!(cfg.exchanges.len(), 2, "exchanges 合并进 FileConfig");
    assert_eq!(cfg.instruments.len(), 2);

    let registry = config::build_exchange_registry(&cfg).expect("双交易所装配");
    assert_eq!(registry.len(), 2);
    // 实例名 ctp → 场所 id SHFE 的别名解析在 app 侧同样可用
    let resolved = config::resolve_instruments(&cfg, &registry).expect("标的清单解析");
    assert_eq!(resolved.len(), 2);
    let keys: Vec<String> = resolved.iter().map(|r| r.id.namespaced_key()).collect();
    assert_eq!(keys[0], "binance/crypto_perpetual/BTCUSDT");
    assert_eq!(keys[1], "SHFE/cn_futures/CU2601");
    // role/strategy 透传（阶段 3 引擎适配器消费的字段）
    assert_eq!(resolved[1].role.as_deref(), Some("contract"));
    assert_eq!(resolved[1].strategy.as_deref(), Some("cta_turtle"));

    // per-product 时段表经配置承重：AU 夜盘收到 02:30，CU 兜底 01:00
    let ctp = registry.get_by_name("ctp").unwrap();
    let au = ctp.parse_instrument("AU2606").unwrap();
    let cu = ctp.parse_instrument("CU2601").unwrap();
    use strategy_engine::prelude::{SessionKind, TradingDay};
    let day = TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap());
    let au_end = ctp
        .session_rules(&au)
        .sessions_on(day)
        .into_iter()
        .find(|w| w.kind == SessionKind::Night)
        .expect("AU 夜盘")
        .end;
    let cu_end = ctp
        .session_rules(&cu)
        .sessions_on(day)
        .into_iter()
        .find(|w| w.kind == SessionKind::Night)
        .expect("CU 夜盘")
        .end;
    assert_eq!(au_end.format("%H:%M").to_string(), "18:30");
    assert_eq!(cu_end.format("%H:%M").to_string(), "17:00");
}

#[test]
fn instruments_validation_fails_loud() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir(root.join("template_trend")).unwrap();
    write(
        &root.join("_global.yaml"),
        r#"
exchanges:
  ctp: { id: SHFE }
instruments:
  - { exchange: dce, product: I2601 }
"#,
    );
    write(&root.join("template_trend/X.yaml"), "timeframe: 1h\n");
    let cfg = config::load(Some(root)).unwrap();
    let registry = config::build_exchange_registry(&cfg).unwrap();
    // 未注册实例名：报错点名条目
    let err = config::resolve_instruments(&cfg, &registry).unwrap_err();
    assert!(err.to_string().contains("`dce`"), "实际: {err}");
}

/// 零回归：旧配置（无 exchanges/instruments）照常加载，混跑入口报错清晰。
#[test]
fn legacy_config_without_exchanges_still_loads() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir(root.join("template_trend")).unwrap();
    write(&root.join("_global.yaml"), "strategy: template_trend\n");
    write(
        &root.join("template_trend/BTCUSDT.yaml"),
        "timeframe: 8h\n",
    );
    let cfg = config::load(Some(root)).unwrap();
    assert!(cfg.exchanges.is_empty() && cfg.instruments.is_empty());
    // ExchangeRegistry 含 trait object 不可 Debug，错误走 match 断言
    let err = match config::build_exchange_registry(&cfg) {
        Ok(_) => panic!("缺 exchanges 应报错，而不是返回空注册表"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("配置缺少 `exchanges:`"), "实际: {err}");
    // 旧 symbol 链路字段完全不受影响（symbol 由文件名推导）
    assert_eq!(cfg.symbols.len(), 1);
    assert_eq!(cfg.symbols[0].symbol, "BTCUSDT");
}

/// R2 迁入（原 live_orchestrator.rs:526，断言逐字）：纯配置单元案落 refcheck:ignore
/// config 套件，不再挂 live 编排宿主。
#[test]
fn services_config_defaults_to_none() {
    let cfg: strategy_app::config::FileConfig = Default::default();
    assert!(cfg.services.is_none());
}
