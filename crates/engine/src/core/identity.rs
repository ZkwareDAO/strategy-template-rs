//! 标的身份：把 `String symbol` 升级为「交易所 + 市场 + 合约」的强类型键。
//!
//! 现状：`symbol` 是全链路唯一索引（`SignalIntent.symbol`、`csv_path`、
//! `contracts.yaml` 的键、`Store` 命名空间），`exchange` 只是透传字符串，
//! `market` 是进程全局单值。因此两个交易所的同名 symbol 会互相覆盖，
//! crypto 与期货也无法混跑。
//!
//! 本模块给出最小可用的命名空间的键，所有路由 / 路径 / 报表 / Store 过滤
//! 都应基于 [`InstrumentId::namespaced_key`]，而不再基于裸 symbol。

use std::fmt;

use serde::{Deserialize, Serialize};

/// 交易所 / 场所标识。注意这不是「市场类型」：SHFE 与 CFFEX 是不同 exchange，
/// 但都是 `CnFutures`；Binance 现货与 Binance U 本位永续同属一个 exchange，
/// 却是不同 market，费率与持仓规则都不一样。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ExchangeId(pub String);

impl ExchangeId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// 市场形态。决定「规则族」而不是连接参数，必须是 per-instrument 维度。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketKind {
    /// 7x24 连续、净仓、按名义金额收费（Binance U 本位永续）
    CryptoPerpetual,
    /// 7x24 连续、无杠杆/无做空或独立借币
    CryptoSpot,
    /// 国内期货：交易日 + 交易时段 + 今昨仓 + 整数手
    CnFutures,
    /// 国内期权（预留）
    CnOptions,
    /// 境外期货（预留：时区、交易日、结算价规则均不同）
    GlobalFutures,
}

/// 合约月份，用于 `CU2601` 这类带到期月的合约。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ContractMonth {
    pub year: u16,
    pub month: u8,
}

impl fmt::Display for ContractMonth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}{:02}", self.year, self.month)
    }
}

/// 全系统唯一的标的键。
///
/// `logical` 与 `listed` 的区分很关键：
/// - 策略面对的是逻辑标的（`product = "CU"`，`role = Main`）；
/// - 交易所要求订阅的是挂牌合约（`symbol = "CU2601"`）。
///   二者由 [`crate::core::subscription::SubscriptionPlan`] 解析，不在引擎里做字符串拼接。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InstrumentId {
    pub exchange: ExchangeId,
    pub market: MarketKind,
    /// 交易所侧挂牌代码（大写规范化后），如 `BTCUSDT` / `CU2601` / `CU_CONT`
    pub symbol: String,
    /// 品种码 / 交易对根，如 `CU` / `BTCUSDT`
    pub product: String,
    pub contract_month: Option<ContractMonth>,
}

impl InstrumentId {
    pub fn perpetual(exchange: &ExchangeId, product: &str) -> Self {
        Self {
            exchange: exchange.clone(),
            market: MarketKind::CryptoPerpetual,
            symbol: product.to_ascii_uppercase(),
            product: product.to_ascii_uppercase(),
            contract_month: None,
        }
    }

    pub fn futures_contract(exchange: &ExchangeId, product: &str, month: ContractMonth) -> Self {
        Self {
            exchange: exchange.clone(),
            market: MarketKind::CnFutures,
            symbol: format!(
                "{}{:02}{:02}",
                product.to_ascii_uppercase(),
                month.year % 100,
                month.month
            ),
            product: product.to_ascii_uppercase(),
            contract_month: Some(month),
        }
    }

    /// 用作 Store / CSV / 报告 / 订单路由的命名空间键。
    /// 例如 `SHFE/cn_futures/CU2601`，不同交易所同名合约不再冲突。
    /// 格式实现在 [`namespaced_key_of`] 集中（阶段 5c v2.9，§6.1）：不持有
    /// `InstrumentId` 的行键生成路径（旧 CLI 链）与类型化路径同源，永不分叉。
    pub fn namespaced_key(&self) -> String {
        namespaced_key_of(self.exchange.as_str(), self.market, &self.symbol)
    }
}

/// 行键命名空间格式 `{exchange}/{market}/{symbol}` 的唯一实现
/// （阶段 5c v2.9，§6.1/§14-3）。
///
/// 旧 CLI 链（`Engine`/`RuntimeActor`）经 app 的 `venue_key_for` 用本函数
/// 派生 signal_id 哈希输入；wire 帧仍携带裸挂牌代码（`InstrumentId::symbol`
/// 的字段语义），**行键命名空间化与帧内裸代码是两个角色**（§6.1），
/// 本函数只服务前者。
pub fn namespaced_key_of(exchange: &str, market: MarketKind, symbol: &str) -> String {
    let market = match market {
        MarketKind::CryptoPerpetual => "crypto_perpetual",
        MarketKind::CryptoSpot => "crypto_spot",
        MarketKind::CnFutures => "cn_futures",
        MarketKind::CnOptions => "cn_options",
        MarketKind::GlobalFutures => "global_futures",
    };
    format!("{}/{}/{}", exchange, market, symbol)
}

impl fmt::Display for InstrumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.namespaced_key())
    }
}

/// 主力 / 次主力 / 连续合约 / 指定合约。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ContractRole {
    Main,
    SecondMain,
    /// 由外部拼接好的连续合约（换月逻辑不在模板内）
    Continuous,
    /// 指定第 N 个月份合约
    Specific(u8),
}

/// 运行模式。规则实现与运行模式正交：同一个 `Exchange` 用同一套
/// [`crate::core::trading::TradingRules`] / [`crate::core::position::PositionRules`]，
/// 只是 [`crate::core::exchange::Exchange::build`] 装配出不同的数据源与订单出口。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Live,
    /// 实盘行情 + 本地撮合（现有 `paper_trading`）
    Paper,
    Backtest,
}
