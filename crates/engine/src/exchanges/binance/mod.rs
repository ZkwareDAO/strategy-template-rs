//! Binance 参考实现：7x24 crypto 永续/现货（§7.1 模板）。
//!
//! 回测 / paper 只需本文件（`make_source` / `make_execution` 走默认）；
//! 接实盘时增加 `decoder.rs` / `live_source.rs` / `live_gateway.rs`
//! 并覆写两个端口工厂——规则一行不改。

pub mod config;
// 阶段 4（v2.7，§13-4）：币安实盘端口薄装配（桥原语见 exchanges::live）
pub mod live_gateway;
pub mod live_source;

use crate::prelude::*;
use std::sync::Arc;

pub struct BinanceExchange {
    id: ExchangeId,
    rules: Arc<SpecDrivenTradingRules>,
    fee_rate: f64,
    /// 阶段 4：live 端口工厂（None = 未接实盘，`build(Live)` 报错）。
    live: Option<Arc<dyn LivePortFactory>>,
    /// v2.7（阶段 4 契约测试逼出，与 CTP 同步整改）：规则对象常驻 Arc，
    /// live/backtest 装配的必须是同一批对象（首公理）。
    session: Arc<ContinuousSession>,
    position: Arc<NetPositionRules>,
    /// 阶段 4（§8 注③债务闭环）：`services:` 子树类型化透传。
    services: Option<ServicesCfg>,
}

impl BinanceExchange {
    pub fn new(fee_rate: f64) -> Self {
        let id = ExchangeId::new("binance");
        let btc = InstrumentId::perpetual(&id, "BTCUSDT");
        let eth = InstrumentId::perpetual(&id, "ETHUSDT");
        let spec = InstrumentSpec::crypto_perpetual(fee_rate);
        let rules = SpecDrivenTradingRules::new([(btc, spec.clone()), (eth, spec)]);
        Self {
            id,
            rules: Arc::new(rules),
            fee_rate,
            live: None,
            session: Arc::new(ContinuousSession::default()),
            position: Arc::new(NetPositionRules),
            services: None,
        }
    }

    /// 阶段 2：配置装配入口——空规格表的命名实例（表由
    /// [`config::BinanceConfig::build`] 按 `symbols:` 填充）。
    pub fn named(id: ExchangeId, fee_rate: f64) -> Self {
        Self {
            id,
            rules: Arc::new(SpecDrivenTradingRules::new([])),
            fee_rate,
            live: None,
            session: Arc::new(ContinuousSession::default()),
            position: Arc::new(NetPositionRules),
            services: None,
        }
    }

    /// 阶段 4：`services:` 子树注入（config 装配路径专用）。
    pub fn with_services(mut self, services: ServicesCfg) -> Self {
        self.services = Some(services);
        self
    }

    /// live 装配消费口（§8 注③）：不静默造默认地址。
    pub fn live_services(&self) -> Option<&ServicesCfg> {
        self.services.as_ref()
    }

    /// 阶段 4（v2.7，§13-4）：注入 live 端口工厂；规则 Arc 不经工厂。
    pub fn with_live_factory(mut self, factory: Arc<dyn LivePortFactory>) -> Self {
        self.live = Some(factory);
        self
    }

    /// 登记一个交易对的规格（Arc::make_mut copy-on-write，不影响已装配部件）。
    pub fn with_symbol_spec(mut self, instrument: InstrumentId, spec: InstrumentSpec) -> Self {
        Arc::make_mut(&mut self.rules).insert(&instrument, spec);
        self
    }
}

impl Exchange for BinanceExchange {
    fn id(&self) -> &ExchangeId {
        &self.id
    }

    fn markets(&self) -> Vec<MarketKind> {
        vec![MarketKind::CryptoPerpetual, MarketKind::CryptoSpot]
    }

    fn parse_instrument(&self, raw: &str) -> Result<InstrumentId> {
        Ok(InstrumentId::perpetual(&self.id, raw))
    }

    fn session_rules(&self, _inst: &InstrumentId) -> Arc<dyn SessionRules> {
        self.session.clone()
    }

    fn aggregation_policy(&self, _instrument: &InstrumentId) -> Arc<dyn BarAggregationPolicy> {
        Arc::new(UtcGridPolicy)
    }

    fn subscription_plan(&self) -> Arc<dyn SubscriptionPlan> {
        Arc::new(DirectSubscriptionPlan)
    }

    fn trading_rules(&self) -> Arc<dyn TradingRules> {
        self.rules.clone()
    }

    fn position_rules(&self) -> Arc<dyn PositionRules> {
        self.position.clone()
    }

    fn simulation_policy(&self) -> Arc<dyn SimulationPolicy> {
        let _ = self.fee_rate;
        Arc::new(SimpleFillModel::default())
    }

    /// 阶段 4：Live 模式走注入的工厂；回测保持 trait 缺省。
    /// 规则实现一行不用改——变的只有两个端口工厂。
    fn make_source(&self, req: &SourceRequest) -> Result<Box<dyn BarEventSource>> {
        if req.mode == RunMode::Live {
            let f = self.live.as_ref().ok_or_else(|| {
                SpiError::Config("binance 未注入 live 端口工厂（with_live_factory）".into())
            })?;
            return f.make_source(req);
        }
        Ok(Box::new(VecEventSource::from_bars(req.bars.clone())))
    }

    fn make_execution(&self, req: &SourceRequest) -> Result<Box<dyn ExecutionPort>> {
        if req.mode == RunMode::Live {
            let f = self.live.as_ref().ok_or_else(|| {
                SpiError::Config("binance 未注入 live 端口工厂（with_live_factory）".into())
            })?;
            return f.make_execution(req);
        }
        Ok(Box::new(SimulationExecution::new(
            req.instrument.clone(),
            req.initial_cash,
            self.trading_rules(),
            self.position_rules(),
            self.session_rules(&req.instrument),
            self.simulation_policy(),
        )))
    }
}
