//! strategy-strategies — 策略插件 crate
//!
//! 新增策略 = 在 src/ 下放一个 .rs 文件（文件尾用 [`register_strategy!`] 自注册）。
//! 模块声明与注册表工厂列表由 build.rs 自动生成，**无需修改任何其他文件**。
//!
//! - [`registry`]: 策略注册表（工厂 / available / create / params 解析）
//!
//! 教学 example 见同目录 `example_dual_ma.rs`（入门）与 `example_mtf_trend.rs`（多周期）。

// ─── 模板冻结区 ───────────────────────────────────────────────────────────
// 以下两行是接线代码，新增策略永远不碰本文件。

/// 策略注册表（StrategyEntry / register_strategy! / create / available）
pub mod registry;

// build.rs 扫描 src/*.rs（排除本文件与 registry.rs）自动生成：
//   pub mod <each strategy file>;
//   fn generated_factories() -> Vec<StrategyEntry> { ... }
include!(concat!(env!("OUT_DIR"), "/strategy_registry_gen.rs"));
