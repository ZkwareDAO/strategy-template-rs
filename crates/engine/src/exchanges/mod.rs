//! 交易所实现层：一个交易所一个文件夹（§4/§7）。
//! 新增交易所 = 新建目录实现 `Exchange` + 此处加一行 `pub mod`，
//! 不动 `core/` 任何文件。
//!
//! 本模块同时是**配置装配的组合根**：`core/` 不知道任何具体交易所，
//! 而按 §8 `exchanges:` 分组装配注册表需要枚举全部类型——这一层
//! 依赖方向（exchanges → registry/core）合法，引擎与 CLI 不感知。

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::core::error::{Result, SpiError};
use crate::core::exchange::Exchange;
use crate::registry::ExchangeRegistry;

pub mod binance;
pub mod ctp;
// 阶段 4（v2.7，§13-4/§17）：live 端口桥接原语（ExecutionPort→信号管道、
// BarEventSource→WS channel）。交易所文件夹内的 live_*.rs 是它的薄 instantiation。
pub mod live;

/// §8 启动流程第 2 步：`exchanges: { name: {...} }` → [`ExchangeRegistry`]。
///
/// 键 = 配置实例名（`instruments:` 引用的名字）；类型由键名或子树内
/// `kind:` 判别。实例名与交易所 id 不同名时（如 `ctp` → `SHFE`），
/// 注册表内登记别名，`build_all` 仍按标的的 `ExchangeId` 精确分派。
pub fn build_registry(entries: &BTreeMap<String, serde_json::Value>) -> Result<ExchangeRegistry> {
    let mut registry = ExchangeRegistry::new();
    for (name, value) in entries {
        let kind = value
            .get("kind")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| name.clone());
        let exchange: Arc<dyn Exchange> = match kind.to_ascii_lowercase().as_str() {
            "binance" => Arc::new(binance::config::BinanceConfig::from_value(value)?.build()?),
            "ctp" => Arc::new(ctp::config::CtpConfig::from_value(value)?.build()?),
            other => {
                return Err(SpiError::Config(format!(
                    "未知交易所类型: {other}（当前装配器支持 binance | ctp；\
                     新增类型 = exchanges/{other}/ + 此处加一个 match 分支）"
                )))
            }
        };
        registry.register_named(name, exchange);
    }
    Ok(registry)
}
