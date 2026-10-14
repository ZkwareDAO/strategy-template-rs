//! 策略注册表 — 模板冻结文件，新增策略不修改
//!
//! 三个角色：
//! - [`StrategyEntry`]: 注册表条目（name / factory）
//! - [`register_strategy!`]: 策略文件尾部的自注册宏
//! - [`create`] / [`all`]: 运行时工厂查询（app 调用）
//!
//! build.rs 生成 `strategy_registry_gen.rs`（mod 清单 + `generated_factories()`），
//! 本文件提供运行时 API，两者在 lib.rs 汇合。

use strategy_core::strategy::Strategy;

/// 工厂参数（CLI/配置文件聚合后的运行时取值）
#[derive(Debug, Clone)]
pub struct FactoryArgs<'a> {
    pub symbol: &'a str,
    /// 主周期字符串（如 "8h"），由各策略自行 parse（所需主周期不同）
    pub timeframe: &'a str,
    /// 每单名义金额
    pub cash: f64,
    /// 策略私有参数（配置文件 params 原样透传；None = 全默认）
    pub params: Option<&'a serde_json::Value>,
}

/// 注册表条目
pub struct StrategyEntry {
    /// 策略名（小写蛇形；进入 signal_id 与 strategy_name/CSV 路径）
    pub name: &'static str,
    /// 工厂：参数 → 策略实例
    #[allow(clippy::type_complexity)]
    pub factory: fn(&FactoryArgs) -> anyhow::Result<Box<dyn Strategy + Send>>,
}

/// 策略文件尾部的自注册宏（每个策略文件恰好调用一次）
///
/// ```ignore
/// strategy_strategies::register_strategy! {
///     name: "my_strat",
///     factory: |args| {
///         let cfg = MyConfig::from_args(args)?;
///         Ok(Box::new(MyStrat::new(args.symbol, cfg)))
///     }
/// }
/// ```
#[macro_export]
macro_rules! register_strategy {
    (name: $name:literal, factory: $factory:expr) => {
        #[allow(unused_imports)]
        pub(crate) fn __strategy_entry() -> $crate::registry::StrategyEntry {
            $crate::registry::StrategyEntry {
                name: $name,
                factory: $factory,
            }
        }
    };
}

/// 按名字创建策略实例；未知名字时错误信息动态列出可用策略
pub fn create(name: &str, args: &FactoryArgs) -> anyhow::Result<Box<dyn Strategy + Send>> {
    let known = all();
    let entry = known
        .iter()
        .find(|e| e.name == name)
        .ok_or_else(|| unknown_strategy_err(name, &known))?;
    (entry.factory)(args)
}

fn unknown_strategy_err(name: &str, known: &[StrategyEntry]) -> anyhow::Error {
    let mut names: Vec<&str> = known.iter().map(|e| e.name).collect();
    names.sort_unstable();
    anyhow::anyhow!("未知策略: {name}（可用: {}）", names.join(", "))
}

/// 全部已注册策略（来自 build.rs 生成列表）
pub fn all() -> Vec<StrategyEntry> {
    crate::generated_factories()
}

/// 策略私有参数解析：params 存在则反序列化，否则全默认。
/// 策略工厂内统一用这个，避免每家手写 match None。
pub fn params_or_default<T: serde::de::DeserializeOwned + Default>(
    params: Option<&serde_json::Value>,
) -> anyhow::Result<T> {
    match params {
        Some(v) => Ok(serde_json::from_value(v.clone())
            .map_err(|e| anyhow::anyhow!("params 反序列化失败: {e}"))?),
        None => Ok(T::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_dual_ma_registered() {
        let names: Vec<&str> = all().iter().map(|e| e.name).collect();
        assert!(
            names.contains(&"example_dual_ma"),
            "example_dual_ma 应在注册表: {names:?}"
        );
    }

    #[test]
    fn names_are_lowercase_snake() {
        for e in all() {
            assert!(
                e.name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c == '_' || c.is_ascii_digit()),
                "策略名必须小写蛇形: {}",
                e.name
            );
        }
    }

    #[test]
    fn create_example_dual_ma_ok() {
        let args = FactoryArgs {
            symbol: "BTCUSDT",
            timeframe: "8h",
            cash: 200.0,
            params: None,
        };
        let s = create("example_dual_ma", &args).unwrap();
        assert_eq!(s.strategy_type(), "example_dual_ma");
    }

    #[test]
    fn create_unknown_lists_available() {
        let args = FactoryArgs {
            symbol: "BTCUSDT",
            timeframe: "8h",
            cash: 200.0,
            params: None,
        };
        let err = match create("nope", &args) {
            Err(e) => e,
            Ok(_) => panic!("未知策略应返回 Err"),
        };
        let err = err.to_string();
        assert!(err.contains("未知策略: nope"));
        assert!(
            err.contains("example_dual_ma"),
            "错误应动态列出可用策略: {err}"
        );
    }
}
