//! CTP / 国内期货参考实现（§7.2 模板）。
//!
//! 时段表按 **product** 组织（v2.1 C2：AU 夜盘 02:30 与 CU 01:00 各查各表），
//! 数据源为 contracts.yaml 的 session_opens/session_closes；
//! 交割/换月的 per-product 规则待阶段 2 下沉（§16 C12）。

pub mod config;
// 阶段 4（v2.7，§13-4）：CTP 实盘端口薄装配（桥原语见 exchanges::live）
pub mod live_gateway;
pub mod live_source;

use crate::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct CtpExchange {
    id: ExchangeId,
    /// v2.1 C2：同一交易所内不同品种时段不同（AU 夜盘至 02:30、CU 至 01:00、
    /// 部分品种无夜盘）。默认表兜底，product 级覆盖对应 contracts.yaml 的
    /// session_opens/session_closes。
    default_session: Arc<WindowedSession>,
    product_sessions: BTreeMap<String, Arc<WindowedSession>>,
    rules: Arc<SpecDrivenTradingRules>,
    /// 是否按持仓量选主力（示例代码；开启时需要提供合约快照）
    main_contract: bool,
    /// 阶段 2：配置装配的订阅计划覆盖（`subscription:` 节；None = 走内置两档）。
    subscription: Option<Arc<dyn SubscriptionPlan>>,
    /// 阶段 4：live 端口工厂（None = 未接实盘，`build(Live)` 报错）。
    live: Option<Arc<dyn LivePortFactory>>,
    /// v2.7（阶段 4 契约测试逼出）：持仓规则**常驻 Arc**——
    /// 每次调用新建 Arc 会让「live/backtest 同一批规则对象」的首公理
    /// 在 `position_rules` 上破防（`live_build_shares_rule_arcs_…` 坐实）。
    position: Arc<CtpPositionRules>,
    /// 阶段 4（§8 注③债务闭环）：`services:` 子树类型化透传，
    /// live 装配（app 组装根）经 [`Self::live_services`] 取端点。
    services: Option<ServicesCfg>,
}

impl CtpExchange {
    pub fn new() -> Self {
        let id = ExchangeId::new("SHFE");
        let cu = InstrumentId::futures_contract(
            &id,
            "CU",
            ContractMonth {
                year: 2026,
                month: 1,
            },
        );
        let spec = InstrumentSpec {
            multiplier: 5.0,
            tick_size: 10.0,
            qty_step: 1.0, // 整数手
            min_qty: 1.0,
            max_qty: None,
            price_precision: 0,
            qty_precision: 0,
            margin_rate: 0.11,
            fee: FeeSchedule {
                mode: crate::core::trading::FeeMode::Rate,
                open: 0.00005,
                close: 0.00005,
                close_today: Some(0.0001),
                close_yesterday: None,
            },
            currency: "CNY".into(),
            sizing_mode: SizingMode::MarginLots,
        };
        let rules = SpecDrivenTradingRules::new([(cu, spec)]);
        Self {
            id,
            default_session: Arc::new(cn_futures_default()),
            product_sessions: BTreeMap::new(),
            rules: Arc::new(rules),
            main_contract: false,
            subscription: None,
            live: None,
            position: Arc::new(CtpPositionRules::default()),
            services: None,
        }
    }

    /// 阶段 2：配置装配入口——空规格表的命名实例（表由
    /// [`config::CtpConfig::build`] 按 `contracts:` 逐品种填充）。
    pub fn named(id: ExchangeId) -> Self {
        Self {
            id,
            default_session: Arc::new(cn_futures_default()),
            product_sessions: BTreeMap::new(),
            rules: Arc::new(SpecDrivenTradingRules::new([])),
            main_contract: false,
            subscription: None,
            live: None,
            position: Arc::new(CtpPositionRules::default()),
            services: None,
        }
    }

    /// 阶段 4：`services:` 子树注入（config 装配路径专用）。
    pub fn with_services(mut self, services: ServicesCfg) -> Self {
        self.services = Some(services);
        self
    }

    /// live 装配消费口（§8 注③）：端点缺失返回 None，由组装根回落
    /// 全局 `services:`（旧形态兼容）；**不静默造默认地址**。
    pub fn live_services(&self) -> Option<&ServicesCfg> {
        self.services.as_ref()
    }

    /// 阶段 4（v2.7，§13-4）：注入 live 端口工厂（覆写两个端口工厂的
    /// 数据化形态）；规则 Arc 不经工厂——首公理保持。
    pub fn with_live_factory(mut self, factory: Arc<dyn LivePortFactory>) -> Self {
        self.live = Some(factory);
        self
    }

    /// 替换全所默认时段表（`session:` 节）。
    pub fn with_default_session(mut self, session: WindowedSession) -> Self {
        self.default_session = Arc::new(session);
        self
    }

    /// 显式订阅计划覆盖（`subscription:` 节）；优先于 `main_contract` 两档。
    pub fn with_subscription_plan(mut self, plan: MainContractPlan) -> Self {
        self.subscription = Some(Arc::new(plan));
        self
    }

    pub fn with_main_contract(mut self) -> Self {
        self.main_contract = true;
        self
    }

    /// 按品种覆盖交易时段（v2.1 C2）：AU/AG 的 02:30 夜盘收盘等差异
    /// 不再被全所一张表吞掉；数据源即 contracts.yaml 的
    /// session_opens/session_closes，替代旧的硬编码窗口表（已随 D2 退役删除）。
    pub fn with_session(mut self, product: &str, session: WindowedSession) -> Self {
        self.product_sessions
            .insert(product.to_ascii_uppercase(), Arc::new(session));
        self
    }

    /// 品种查表：无专属时段表时回落默认（同交易日口径，不再同窗口表）。
    fn session_for(&self, inst: &InstrumentId) -> Arc<WindowedSession> {
        self.product_sessions
            .get(&inst.product)
            .cloned()
            .unwrap_or_else(|| self.default_session.clone())
    }

    /// 注册更多品种规格（同一交易所多品种）。Arc::make_mut 走 copy-on-write，
    /// 不会破坏已 build 出去的运行时部件。
    pub fn with_spec(mut self, instrument: InstrumentId, spec: InstrumentSpec) -> Self {
        Arc::make_mut(&mut self.rules).insert(&instrument, spec);
        self
    }
}

impl Default for CtpExchange {
    fn default() -> Self {
        Self::new()
    }
}

impl Exchange for CtpExchange {
    fn id(&self) -> &ExchangeId {
        &self.id
    }

    fn markets(&self) -> Vec<MarketKind> {
        vec![MarketKind::CnFutures]
    }

    fn parse_instrument(&self, raw: &str) -> Result<InstrumentId> {
        let upper = raw.to_ascii_uppercase();
        let digits = upper.find(|c: char| c.is_ascii_digit());
        match digits {
            Some(pos) if upper.len() >= pos + 4 => {
                let product = &upper[..pos];
                let yy: u16 =
                    upper[pos..pos + 2]
                        .parse()
                        .map_err(|_| SpiError::InstrumentParse {
                            exchange: self.id.to_string(),
                            raw: raw.into(),
                        })?;
                let mm: u8 =
                    upper[pos + 2..pos + 4]
                        .parse()
                        .map_err(|_| SpiError::InstrumentParse {
                            exchange: self.id.to_string(),
                            raw: raw.into(),
                        })?;
                Ok(InstrumentId::futures_contract(
                    &self.id,
                    product,
                    ContractMonth {
                        year: 2000 + yy,
                        month: mm,
                    },
                ))
            }
            // 逻辑品种码：交给 SubscriptionPlan 解析成具体挂牌合约
            _ => Ok(InstrumentId {
                exchange: self.id.clone(),
                market: MarketKind::CnFutures,
                symbol: upper.clone(),
                product: upper,
                contract_month: None,
            }),
        }
    }

    fn session_rules(&self, inst: &InstrumentId) -> Arc<dyn SessionRules> {
        self.session_for(inst)
    }

    fn aggregation_policy(&self, _instrument: &InstrumentId) -> Arc<dyn BarAggregationPolicy> {
        Arc::new(SessionWindowPolicy)
    }

    fn subscription_plan(&self) -> Arc<dyn SubscriptionPlan> {
        // 阶段 2：配置 `subscription:` 显式计划优先；否则回两档内置。
        if let Some(plan) = &self.subscription {
            return plan.clone();
        }
        if self.main_contract {
            Arc::new(MainContractPlan::default())
        } else {
            Arc::new(DirectSubscriptionPlan)
        }
    }

    fn trading_rules(&self) -> Arc<dyn TradingRules> {
        self.rules.clone()
    }

    fn position_rules(&self) -> Arc<dyn PositionRules> {
        self.position.clone()
    }

    fn simulation_policy(&self) -> Arc<dyn SimulationPolicy> {
        Arc::new(SimpleFillModel::default())
    }

    /// 阶段 4：Live 模式走注入的工厂；回测保持 trait 缺省（回放 req.bars）。
    /// 规则实现一行不用改——变的只有两个端口工厂。
    fn make_source(&self, req: &SourceRequest) -> Result<Box<dyn BarEventSource>> {
        if req.mode == RunMode::Live {
            let f = self.live.as_ref().ok_or_else(|| {
                SpiError::Config("CTP 未注入 live 端口工厂（with_live_factory）".into())
            })?;
            return f.make_source(req);
        }
        Ok(Box::new(VecEventSource::from_bars(req.bars.clone())))
    }

    fn make_execution(&self, req: &SourceRequest) -> Result<Box<dyn ExecutionPort>> {
        if req.mode == RunMode::Live {
            let f = self.live.as_ref().ok_or_else(|| {
                SpiError::Config("CTP 未注入 live 端口工厂（with_live_factory）".into())
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use strategy_core::signal::Side;

    fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
    }

    #[test]
    fn ctp_split_close_produces_today_and_yesterday_legs() {
        let ex = CtpExchange::new();
        let inst = ex.parse_instrument("CU2601").unwrap();
        let session = ex.session_rules(&inst);
        let rules = CtpPositionRules {
            priority: ClosePriority::TodayFirst,
        };
        let mut book = PositionBook::new(inst.clone(), rules.mode());
        let day = session.trading_day_of(utc(2026, 1, 5, 1, 0));
        book.lots.push(PositionLot {
            id: "old".into(),
            instrument: inst.clone(),
            side: Side::Long,
            entry_price: 50000.0,
            qty: 3.0,
            remaining: 3.0,
            opened_at: utc(2026, 1, 2, 1, 0),
            trading_day: TradingDay(chrono::NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
            is_today: false,
            qty_raw: None,
            remaining_raw: None,
        });
        book.lots.push(PositionLot {
            id: "new".into(),
            instrument: inst.clone(),
            side: Side::Long,
            entry_price: 50100.0,
            qty: 2.0,
            remaining: 2.0,
            opened_at: utc(2026, 1, 5, 1, 0),
            trading_day: day,
            is_today: true,
            qty_raw: None,
            remaining_raw: None,
        });
        let legs = rules
            .split_close(
                &book,
                &CloseRequest {
                    instrument: inst,
                    qty: 5.0,
                    at: utc(2026, 1, 5, 1, 10),
                },
                session.as_ref(),
            )
            .unwrap();
        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].offset, Offset::CloseToday);
        assert_eq!(legs[0].qty, 2.0);
        assert_eq!(legs[0].lot_ids, vec!["new".to_string()]);
        assert_eq!(legs[1].offset, Offset::CloseYesterday);
        assert_eq!(legs[1].qty, 3.0);
        assert_eq!(legs[1].lot_ids, vec!["old".to_string()]);
    }

    #[test]
    fn integer_lots_and_tick_rounding() {
        let ex = CtpExchange::new();
        let inst = ex.parse_instrument("CU2601").unwrap();
        let rules = ex.trading_rules();
        let qty = rules
            .sizing(&SizingRequest {
                instrument: inst.clone(),
                price: 50000.0,
                equity: 1_000_000.0,
                leverage: 1.0,
                notional_per_order: None,
            })
            .unwrap();
        assert_eq!(qty, 4.0, "1 倍杠杆下 4 手");
        assert_eq!(rules.normalize_price(&inst, 50007.0).unwrap(), 50010.0);
        assert!(
            rules
                .validate(&OrderDraft {
                    instrument: inst.clone(),
                    side: Side::Long,
                    offset: Offset::Open,
                    price: 50005.0,
                    qty: 4.0,
                    qty_raw: None,
                })
                .is_err(),
            "非 tick_size 整数倍必须拒绝"
        );
    }
    fn lot(id: &str, qty: f64, entry: f64, day: TradingDay, opened: DateTime<Utc>) -> PositionLot {
        PositionLot {
            id: id.into(),
            instrument: InstrumentId::futures_contract(
                &ExchangeId::new("SHFE"),
                "CU",
                ContractMonth {
                    year: 2026,
                    month: 1,
                },
            ),
            side: Side::Long,
            entry_price: entry,
            qty,
            remaining: qty,
            opened_at: opened,
            trading_day: day,
            is_today: true,
            qty_raw: None,
            remaining_raw: None,
        }
    }
    /// C2：同交易所内 AU（夜盘至 02:30）与 CU（01:00）各拿各的时段表。
    #[test]
    fn per_product_session_windows_differ() {
        use crate::core::session::SessionKind;
        use chrono::{FixedOffset, NaiveTime};
        let t = |h, m| NaiveTime::from_hms_opt(h, m, 0).unwrap();
        let au = WindowedSession {
            timezone: FixedOffset::east_opt(8 * 3600).unwrap(),
            windows: vec![
                (t(9, 0), t(10, 15), SessionKind::Day),
                (t(10, 30), t(11, 30), SessionKind::Day),
                (t(13, 30), t(15, 0), SessionKind::Day),
                (t(21, 0), t(2, 30), SessionKind::Night),
            ],
            day_cutover: t(20, 0),
            no_trade_after_open_minutes: 5,
            no_trade_before_close_minutes: 5,
            holidays: Default::default(),
        };
        let ex = CtpExchange::new().with_session("AU", au);
        let au_inst = ex.parse_instrument("AU2606").unwrap();
        let cu_inst = ex.parse_instrument("CU2601").unwrap();
        let day = TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap());
        let au_night = ex
            .session_rules(&au_inst)
            .sessions_on(day)
            .into_iter()
            .find(|w| w.kind == SessionKind::Night)
            .expect("AU 夜盘窗口存在");
        let cu_night = ex
            .session_rules(&cu_inst)
            .sessions_on(day)
            .into_iter()
            .find(|w| w.kind == SessionKind::Night)
            .expect("CU 夜盘窗口存在");
        // 周五夜盘归属周一交易日：AU 收在周六凌晨 02:30（UTC 18:30），CU 01:00（UTC 17:00）
        assert_eq!(au_night.end, utc(2026, 1, 2, 18, 30));
        assert_eq!(cu_night.end, utc(2026, 1, 2, 17, 0));
        // 兜底：无专属表的品种落回默认表（ptr 同一 Arc，保持规则单一来源）
        let other = ex.parse_instrument("AL2601").unwrap();
        assert!(Arc::ptr_eq(
            &ex.session_rules(&cu_inst),
            &ex.session_rules(&other)
        ));
    }

    /// C4：同 offset 多批次合并为一腿时——加权均价 + 全批次 id 精确扣减。
    #[test]
    fn merged_close_leg_uses_weighted_entry() {
        let ex = CtpExchange::new();
        let inst = ex.parse_instrument("CU2601").unwrap();
        let session = ex.session_rules(&inst);
        let today = session.trading_day_of(utc(2026, 1, 5, 1, 0));
        let rules = CtpPositionRules {
            priority: ClosePriority::TodayFirst,
        };
        let mut book = PositionBook::new(inst.clone(), rules.mode());
        book.lots
            .push(lot("a", 2.0, 50000.0, today, utc(2026, 1, 5, 1, 5)));
        book.lots
            .push(lot("b", 3.0, 50100.0, today, utc(2026, 1, 5, 1, 6)));
        let legs = rules
            .split_close(
                &book,
                &CloseRequest {
                    instrument: inst.clone(),
                    qty: 5.0,
                    at: utc(2026, 1, 5, 1, 10),
                },
                session.as_ref(),
            )
            .unwrap();
        assert_eq!(legs.len(), 1, "同日两批次合并成一笔 CloseToday");
        assert_eq!(legs[0].qty, 5.0);
        assert_eq!(legs[0].entry_price, (2.0 * 50000.0 + 3.0 * 50100.0) / 5.0);
        assert_eq!(legs[0].lot_ids, vec!["a".to_string(), "b".to_string()]);
        rules.on_close_fill(&mut book, &legs[0], 5.0, None).unwrap();
        assert!(book.is_flat(), "两个批次都被精确扣减");
    }
    /// C4：ProRata 按剩余量比例分摊到今/昨仓（此前声明未实现）。
    #[test]
    fn prorata_splits_across_offsets() {
        let ex = CtpExchange::new();
        let inst = ex.parse_instrument("CU2601").unwrap();
        let session = ex.session_rules(&inst);
        let today = session.trading_day_of(utc(2026, 1, 5, 1, 0));
        let rules = CtpPositionRules {
            priority: ClosePriority::ProRata,
        };
        let mut book = PositionBook::new(inst.clone(), rules.mode());
        book.lots
            .push(lot("t", 2.0, 50000.0, today, utc(2026, 1, 5, 1, 5)));
        book.lots.push(lot(
            "y",
            3.0,
            49000.0,
            TradingDay(chrono::NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()),
            utc(2025, 12, 31, 1, 5),
        ));
        let legs = rules
            .split_close(
                &book,
                &CloseRequest {
                    instrument: inst.clone(),
                    qty: 5.0,
                    at: utc(2026, 1, 5, 1, 10),
                },
                session.as_ref(),
            )
            .unwrap();
        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].offset, Offset::CloseToday);
        assert_eq!(legs[0].qty, 2.0);
        assert_eq!(legs[1].offset, Offset::CloseYesterday);
        assert_eq!(legs[1].qty, 3.0);
    }
    /// C7：平仓腿继承整消耗批次的 `remaining_raw`（Decimal 精确求和）；
    /// 单批次部分平用成交字面量精确递减——全程无 f64 往返。
    #[test]
    fn raw_qty_inherited_and_subtracted_exactly() {
        let ex = CtpExchange::new();
        let inst = ex.parse_instrument("CU2601").unwrap();
        let session = ex.session_rules(&inst);
        let today = session.trading_day_of(utc(2026, 1, 5, 1, 0));
        let rules = CtpPositionRules::default();

        // 两批次整消耗合并成一腿：raw 走 Decimal 加法，杜绝 f64 求和漂移
        let mut book = PositionBook::new(inst.clone(), rules.mode());
        let mut a = lot("a", 0.00361122, 50000.0, today, utc(2026, 1, 5, 1, 5));
        a.qty_raw = Some("0.00361122".into());
        a.remaining_raw = Some("0.00361122".into());
        let mut b = lot("b", 0.002, 50100.0, today, utc(2026, 1, 5, 1, 6));
        b.qty_raw = Some("0.002".into());
        b.remaining_raw = Some("0.002".into());
        book.lots.push(a);
        book.lots.push(b);
        let legs = rules
            .split_close(
                &book,
                &CloseRequest {
                    instrument: inst.clone(),
                    qty: 0.00361122 + 0.002,
                    at: utc(2026, 1, 5, 1, 10),
                },
                session.as_ref(),
            )
            .unwrap();
        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0].qty_raw.as_deref(), Some("0.00561122"));

        // 单批次部分平：腿不伪造 raw；成交字面量精确递减
        let mut book2 = PositionBook::new(inst.clone(), rules.mode());
        let mut c = lot("c", 0.003, 50000.0, today, utc(2026, 1, 5, 1, 7));
        c.remaining_raw = Some("0.003".into());
        book2.lots.push(c);
        let legs2 = rules
            .split_close(
                &book2,
                &CloseRequest {
                    instrument: inst.clone(),
                    qty: 0.001,
                    at: utc(2026, 1, 5, 1, 10),
                },
                session.as_ref(),
            )
            .unwrap();
        assert_eq!(legs2[0].qty_raw, None, "部分消耗不得伪造腿字面量");
        rules
            .on_close_fill(&mut book2, &legs2[0], 0.001, Some("0.001"))
            .unwrap();
        assert_eq!(
            book2.lots[0].remaining_raw.as_deref(),
            Some("0.002"),
            "C7：Decimal 精确递减，无 f64 漂移"
        );
        // 缺成交字面量 → 未知（None），不写入错误值（manager 同语义）
        rules
            .on_close_fill(&mut book2, &legs2[0], 0.001, None)
            .unwrap();
        assert_eq!(book2.lots[0].remaining_raw, None);
    }
}
