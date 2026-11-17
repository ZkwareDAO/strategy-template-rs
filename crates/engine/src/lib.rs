//! # Engine crate：统一主循环引擎
//!
//! 目标：**新增一个交易所，只实现 [`Exchange`] 及其规则对象，实盘与回测
//! 自动跑通同一套主循环**；同时天然支持多品种、多交易所在同一进程混跑。
//!
//! ## 分层（§4）
//!
//! | 层 | 模块 | 职责 | 改动频率 |
//! |---|---|---|---|
//! | 框架层 | [`core`] | trait 定义 + 通用实现 + 主循环 | 接口稳定后基本不动 |
//! | 交易所层 | [`exchanges`] | 每个交易所一个文件夹（规则组装 + 实盘适配） | 新增交易所时才加 |
//! | 注册层 | [`registry`] | 运行时按 `ExchangeId` 分派与批量装配 | 加一行 `register` |
//!
//! 铁律：**exchanges/ 可依赖 core/，反之不行**（core 层测试也不得引用具体交易所）。
//!
//! ## 设计公理
//!
//! 1. **规则只实现一次**。`TradingRules` / `PositionRules` / `SessionRules`
//!    是纯规则，由 `Exchange` 以 `Arc<dyn _>` 提供；live 与 backtest 装配到
//!    的是同一批对象。
//! 2. **实盘 / 回测差异只允许出现在两个端口**：[`BarEventSource`]（数据
//!    从哪来）与 [`ExecutionPort`]（订单到哪去）。[`ExchangeRunner::run`]
//!    里没有任何 `if mode == backtest`。
//! 3. **两个层次的「完整 K 线」分开**：单根 1m 收线由**源侧**给出
//!    （`Candle::is_closed` → `Bar::from_candle`，engine 不做通用帧解码——原
//!    `BarDecoder` 端口因生产零消费已退役）；大周期桶收满由
//!    [`BarAggregationPolicy`] 判定。
//! 4. **标的是命名空间键**：[`InstrumentId`]，路由/存储/报表基于
//!    `namespaced_key()`。
//!
//! ## 最小实现集
//!
//! 回测 / paper：**只实现规则方法**（`session_rules` ... `simulation_policy`），
//! `make_source` / `make_execution` / `build` 都有默认实现。
//! 实盘：再覆写两个端口工厂，规则代码一行都不用改。

pub mod bridge;
pub mod core;
pub mod exchanges;
pub mod registry;
pub mod strategy;

pub use core::error::{Result, SpiError};

/// 常用导入（含框架公共类型）。
pub mod prelude {
    pub use crate::core::aggregation::{
        AggregationState, BarAggregationPolicy, SessionWindowPolicy, UtcGridPolicy,
    };
    pub use crate::core::bar::{
        Bar, BarEventSource, BarExtensions, BarFinality, MarketEvent, Money, Timeframe,
        VecEventSource,
    };
    pub use crate::core::error::{Result, SpiError};
    pub use crate::core::exchange::{
        default_timeframes, EndpointCfg, Exchange, ExchangeParts, LivePortFactory, ServicesCfg,
        SourceRequest,
    };
    pub use crate::core::execution::{
        ExecutionPort, Fill, OrderAck, OrderContext, OrderRequest, OrderSnapshot, OrderState,
        SimpleFillModel, SimulationExecution, SimulationPolicy,
    };
    pub use crate::core::identity::{
        namespaced_key_of, ContractMonth, ContractRole, ExchangeId, InstrumentId, MarketKind,
        RunMode,
    };
    pub use crate::core::position::{
        CloseLeg, ClosePriority, CloseRequest, CtpPositionRules, NetPositionRules, PositionBook,
        PositionLot, PositionMode, PositionRules,
    };
    // 阶段 3（v2.6）：历史回放事件注入 + 现有策略桥接（接入面即 prelude 面）
    pub use crate::bridge::{bar_to_candle, spec_from_contract, CandleSeries, PortStrategyBridge};
    pub use crate::core::replay::events_from_bars;
    // 阶段 4（v2.7）：live 端口桥原语 + 各所薄装配（§13-4/§17）
    pub use crate::core::runtime::ExchangeRunner;
    pub use crate::core::session::{
        cn_futures_default, ContinuousSession, GapReason, SessionCfg, SessionKind, SessionRules,
        SessionStatus, SessionWindow, SessionWindowCfg, TradingDay, WindowedSession,
    };
    pub use crate::core::subscription::{
        ContractSnapshot, DirectSubscriptionPlan, MainContractPlan, NoAdjustContinuousSeries,
        RollEvent, RollPolicy, Subscription, SubscriptionPlan,
    };
    pub use crate::core::trading::{
        FeeMode, FeeRequest, FeeSchedule, InstrumentSpec, Offset, OrderDraft, Rounding, SizingMode,
        SizingParams, SizingRequest, SpecDrivenTradingRules, TradingRules,
    };
    pub use crate::core::types::{DecisionContext, RunSummary, StrategyAction, StrategyPort};
    pub use crate::exchanges::binance::live_gateway::binance_live_execution;
    pub use crate::exchanges::binance::live_source::binance_live_source;
    pub use crate::exchanges::ctp::live_gateway::ctp_live_execution;
    pub use crate::exchanges::ctp::live_source::ctp_live_source;
    pub use crate::exchanges::live::{ChannelBarSource, LiveExecution, LiveOrderUpdate};
    pub use crate::registry::ExchangeRegistry;
}
