//! 统一注册表守卫：注册名 = 策略 `strategy_type()`，且每个策略都声明 1m。

use strategy_strategies::registry::{self, FactoryArgs};

fn args(timeframe: &str) -> FactoryArgs<'_> {
    FactoryArgs {
        symbol: "BTCUSDT",
        timeframe,
        cash: 200.0,
        params: None,
    }
}

#[test]
fn all_entries_factory_matches_name_and_metadata() {
    for e in registry::all() {
        let s = registry::create(e.name, &args("1h")).expect("注册表工厂必须可创建");
        assert_eq!(s.strategy_type(), e.name, "注册名与策略元数据必须一致");
        assert!(
            s.timeframes()
                .contains(&strategy_engine::core::bar::Timeframe::M1),
            "{} 必须声明 1m 周期",
            e.name
        );
    }

    let names: Vec<&str> = registry::all().iter().map(|e| e.name).collect();
    for expected in ["template_trend", "template_xsec_ls"] {
        assert!(
            names.contains(&expected),
            "缺少注册项 {expected}: {names:?}"
        );
    }
}

#[test]
fn unknown_strategy_lists_all_registered_names() {
    let err = match registry::create("nope", &args("1h")) {
        Ok(_) => panic!("未知策略必须报错"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("未知策略: nope"), "{err}");
    assert!(err.contains("template_xsec_ls"), "{err}");
    assert!(err.contains("template_trend"), "{err}");
}
