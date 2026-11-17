//! Binance 配置驱动装配（阶段 2，§8 `exchanges.binance:` 子树）。
//!
//! 交易所专有逻辑留在本交易所文件夹：crypto 的缺省 id / fee_rate /
//! 交易对清单只在这里定义，`core/` 不感知。

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::core::error::{Result, SpiError};
use crate::core::identity::{ExchangeId, InstrumentId};
use crate::core::trading::InstrumentSpec;
use crate::exchanges::binance::BinanceExchange;

/// §8 示例形态：
/// ```yaml
/// binance:
///   market: crypto_perpetual
///   aggregation: utc_grid
///   fee_rate: 0.0004
///   symbols: [BTCUSDT, ETHUSDT]
/// ```
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct BinanceConfig {
    /// 类型判别（缺省 = `exchanges:` 的键名）。
    pub kind: Option<String>,
    /// 交易所 id；缺省 `binance`。多实例（如 binance_us）时区分命名空间。
    pub id: Option<String>,
    /// 市场形态；阶段 2 仅支持 `crypto_perpetual`（spot 预留）。
    pub market: Option<String>,
    /// 费率（taker 口径）；缺省 0.0004。
    pub fee_rate: Option<f64>,
    /// 聚合策略；阶段 2 仅允许 `utc_grid`。
    pub aggregation: Option<String>,
    /// 交易对清单；缺省 `[BTCUSDT, ETHUSDT]`。
    pub symbols: Option<Vec<String>>,
    /// 逐标的规格覆盖（键 = 交易对）；阶段 2 支持 `fee_rate`。
    #[serde(default)]
    pub specs: BTreeMap<String, BinanceSpecCfg>,
    /// 连接参数（阶段 4 v2.7 债务闭环：类型化透传，live 装配经
    /// `BinanceExchange::live_services` 消费；未知端点/键不丢失）。
    pub services: Option<crate::core::exchange::ServicesCfg>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct BinanceSpecCfg {
    pub fee_rate: Option<f64>,
    /// 场所数量步长（如币安 BTCUSDT 永续 0.001）；缺省沿用 crypto 默认。
    /// 低于该步长的平仓差额在 `normalize_qty` 下取整为 0（视为已达标）。
    pub qty_step: Option<f64>,
    /// 场所最小下单量；缺省沿用 crypto 默认。
    pub min_qty: Option<f64>,
    /// 数量精度（与 step 对应的小数位）；缺省沿用 crypto 默认。
    pub qty_precision: Option<u8>,
}

impl BinanceConfig {
    pub fn from_value(v: &serde_json::Value) -> Result<Self> {
        serde_json::from_value(v.clone())
            .map_err(|e| SpiError::Config(format!("binance 配置解析失败: {e}")))
    }

    pub fn build(self) -> Result<BinanceExchange> {
        if let Some(m) = &self.market {
            if m != "crypto_perpetual" {
                return Err(SpiError::Config(format!(
                    "binance 阶段 2 仅支持 market=crypto_perpetual（{m} 未实现）"
                )));
            }
        }
        if let Some(a) = &self.aggregation {
            if a != "utc_grid" {
                return Err(SpiError::Config(format!(
                    "binance 聚合策略仅支持 utc_grid（{a} 未实现）"
                )));
            }
        }
        let id = ExchangeId::new(self.id.unwrap_or_else(|| "binance".into()));
        let fee_rate = self.fee_rate.unwrap_or(0.0004);
        let symbols = self
            .symbols
            .unwrap_or_else(|| vec!["BTCUSDT".into(), "ETHUSDT".into()]);
        let mut ex = BinanceExchange::named(id.clone(), fee_rate);
        for s in symbols {
            let own = self.specs.get(&s.to_ascii_uppercase());
            let own_fee = own.and_then(|c| c.fee_rate).unwrap_or(fee_rate);
            let mut spec = InstrumentSpec::crypto_perpetual(own_fee);
            if let Some(v) = own.and_then(|c| c.qty_step) {
                spec.qty_step = v;
            }
            if let Some(v) = own.and_then(|c| c.min_qty) {
                spec.min_qty = v;
            }
            if let Some(v) = own.and_then(|c| c.qty_precision) {
                spec.qty_precision = v;
            }
            ex = ex.with_symbol_spec(InstrumentId::perpetual(&id, &s), spec);
        }
        // 阶段 4（§8 注③）：services 类型化透传
        if let Some(services) = self.services {
            ex = ex.with_services(services);
        }
        Ok(ex)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::exchange::Exchange;

    #[test]
    fn spec_qty_step_and_min_qty_are_configurable() {
        // 交易所真实数量步长必须能经配置进入 normalize_qty：默认 1e-6
        // 会把低于场所步长的平仓差额当成合法单发出去，被场所拒绝
        // （fix5 实盘 BTCUSDT 0.000177 → QuantityOutOfRange）。
        let v = serde_json::json!({
            "symbols": ["BTCUSDT"],
            "specs": {"BTCUSDT": {"qty_step": 0.001, "min_qty": 0.001, "qty_precision": 3}}
        });
        let ex = BinanceConfig::from_value(&v).unwrap().build().unwrap();
        let inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let spec = ex.trading_rules().spec(&inst).unwrap();
        assert_eq!(spec.qty_step, 0.001);
        assert_eq!(spec.min_qty, 0.001);
        assert_eq!(spec.qty_precision, 3);
    }
}
