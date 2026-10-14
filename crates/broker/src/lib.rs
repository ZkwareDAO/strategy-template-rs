//! strategy-broker — 订单网关层
//!
//! - [`order_log`]：exchange_orders.csv 追加写（回测回执格式）
//! - [`backtest`]：BacktestBroker 撮合内核（触发价±滑点即时成交）
//! - [`backtest_client`]：BacktestOrderClient —— 回测的 OrderGateway 实现
//! - [`trading_service`]：TradingServiceClient —— trading-service WS 客户端
//!   （真实 SouthCommand / ClientEvent 线协议，按策略类型懒建认证连接池）
//! - [`mock`]：本地 trading-service 模拟器（支持开仓 order_cash 换算/平仓
//!   reduce_only）；测试/本地联调组件，**非生产服务，禁止在 live 装配中引用**
//!
//! 端口 [`OrderGateway`] 定义在 strategy-signals（信号管理的出口）。

pub mod auth;
pub mod backtest;
pub mod backtest_client;
pub mod mock;
pub mod order_log;
pub mod trading_service;
pub mod wire;

use chrono::{DateTime, Utc};

use strategy_core::signal::Action;

pub use backtest::{BacktestBroker, BacktestConfig};
pub use backtest_client::BacktestOrderClient;
pub use mock::{MockScenario, MockServer};
pub use order_log::{ExchangeOrderRecord, ExchangeOrderWriter, OrderStatus, PosType, SideCode};
pub use trading_service::{
    ClientEvent, OrderUpdate, OrderVenueCfg, TradingConfig, TradingGateway, TradingServiceClient,
};

/// 账户快照（权益追踪用；mark 价格由调用方按当前 K 线给定）
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AccountSnapshot {
    /// 可用现金（未计浮动盈亏）
    pub cash: f64,
    /// 当前持仓数量（无仓位 = 0）
    pub position_qty: f64,
    /// 开仓均价（无仓位无意义）
    pub entry_price: f64,
    /// 持仓方向（true=long）
    pub is_long: bool,
}

/// 撮合内核抽象（回测引擎直接使用；信号管道经 BacktestOrderClient）。
/// 同步返回成交结果，失败由调用方决策。
pub trait Broker {
    fn execute(
        &mut self,
        signal_id: &str,
        action: &Action,
        update_time: DateTime<Utc>,
    ) -> std::io::Result<backtest::ExecResult>;

    /// 账户快照（回测 broker 提供真实现金/仓位）
    fn account(&self) -> Option<AccountSnapshot> {
        None
    }
}
