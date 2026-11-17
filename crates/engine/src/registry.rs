//! 交易所注册表：按 `ExchangeId` 分派与批量装配（框架层入口之一，
//! 新增交易所只需 `register`，不改引擎 / CLI / 报表）。

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::core::error::{Result, SpiError};
use crate::core::exchange::{Exchange, ExchangeParts, SourceRequest};
use crate::core::identity::ExchangeId;

/// 交易所注册表。运行时按 `ExchangeId` 分派，新增交易所只需 `register`，
/// 不改引擎、不改 CLI、不改报表。
///
/// 阶段 2（v2.5）：`exchanges:` 配置键（实例名，如 `ctp`）可以与
/// [`ExchangeId`]（场所 id，如 `SHFE`）不同名——`register_named` 额外登记
/// 实例名别名，`instruments:` 引用实例名即可解析。
#[derive(Default)]
pub struct ExchangeRegistry {
    map: BTreeMap<String, Arc<dyn Exchange>>,
    aliases: BTreeMap<String, String>,
}

impl ExchangeRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, exchange: Arc<dyn Exchange>) {
        self.map
            .insert(exchange.id().as_str().to_string(), exchange);
    }

    /// 注册并绑定配置实例名（键归一为小写；实例名与 id 同名时别名表不重复记账）。
    pub fn register_named(&mut self, name: &str, exchange: Arc<dyn Exchange>) {
        let id = exchange.id().as_str().to_string();
        self.map.insert(id.clone(), exchange);
        let key = name.to_ascii_lowercase();
        if key != id {
            self.aliases.insert(key, id);
        }
    }

    pub fn get(&self, id: &ExchangeId) -> Result<Arc<dyn Exchange>> {
        self.map
            .get(id.as_str())
            .cloned()
            .ok_or_else(|| SpiError::UnknownExchange(id.to_string()))
    }

    /// 按配置实例名 / 交易所 id 解析：id 精确 → 实例名别名 → 小写兜底。
    /// （id 可能大写如 `SHFE`，别名键已归一小写，两步查找大小写口径必须一致。）
    pub fn get_by_name(&self, name: &str) -> Result<Arc<dyn Exchange>> {
        let lower = name.to_ascii_lowercase();
        let found = self
            .map
            .get(name)
            .or_else(|| self.aliases.get(&lower).and_then(|id| self.map.get(id)))
            .or_else(|| self.map.get(&lower));
        found
            .cloned()
            .ok_or_else(|| SpiError::UnknownExchange(name.to_string()))
    }

    /// 已登记交易所的 id 列表（诊断 / 测试用）。
    pub fn ids(&self) -> Vec<String> {
        self.map.keys().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// 批量装配：一次运行可以同时包含 Binance 永续 + SHFE 期货，
    /// 每个标的一份独立的 `ExchangeParts`（含独立数据源与订单出口）。
    pub fn build_all(
        &self,
        requests: Vec<SourceRequest>,
    ) -> Result<BTreeMap<String, ExchangeParts>> {
        let mut out = BTreeMap::new();
        for req in requests {
            let ex = self.get(&req.instrument.exchange)?;
            let parts = ex.build(&req)?;
            out.insert(req.instrument.namespaced_key(), parts);
        }
        Ok(out)
    }
}
