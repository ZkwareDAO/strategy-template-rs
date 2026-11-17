//! 阶段 2（配置体系）验收测试（§13-2）：
//! `exchanges:` 分组 → Registry 装配 → per-product session/contracts
//! → **binance + ctp 在单进程内混跑**。§8 的 yaml 形态在此逐字段承重。

use chrono::{DateTime, TimeZone, Utc};
use std::collections::BTreeMap;
use std::sync::Arc;
use strategy_core::signal::Side;
use strategy_engine::exchanges::build_registry;
use strategy_engine::prelude::*;

/// `ExchangeRegistry` 含 trait object 不可 Debug，错误断言走 to_string。
fn build_err(entries: BTreeMap<String, serde_json::Value>) -> String {
    match build_registry(&entries) {
        Ok(_) => panic!("配置错误应在装配期报错，而不是装配成功"),
        Err(e) => e.to_string(),
    }
}

fn bar(inst: &InstrumentId, t: DateTime<Utc>, close: f64) -> Bar {
    Bar {
        instrument: inst.clone(),
        open_time: t,
        open: close,
        high: close,
        low: close,
        close,
        volume: 1.0,
        finality: BarFinality::Final,
        extensions: BarExtensions::default(),
    }
}

fn utc(y: i32, m: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, h, mi, 0).unwrap()
}

fn entries(json: serde_json::Value) -> BTreeMap<String, serde_json::Value> {
    serde_json::from_value(json).expect("测试配置必须是 exchanges 映射")
}

/// 一次性「开多 → 平仓」演示策略（与 cross_exchange.rs 同形）。
struct OpenThenClose {
    calls: usize,
}

impl StrategyPort for OpenThenClose {
    fn on_closed_bars(
        &mut self,
        _newly: &[Timeframe],
        _state: &AggregationState,
        ctx: &DecisionContext,
    ) -> Vec<StrategyAction> {
        self.calls += 1;
        match self.calls {
            1 => vec![StrategyAction::Open {
                side: Side::Long,
                price: ctx.price,
                stop_price: ctx.price * 0.9,
                atr: 0.0,
                strength: 1.0,
                reason: "stage2".into(),
            }],
            2 => vec![StrategyAction::Close {
                price: ctx.price,
                reason: "stage2".into(),
                is_stop_loss: false,
                strength: 1.0,
            }],
            _ => vec![StrategyAction::Hold],
        }
    }
}

/// §8 完整示例形态（含 `services` 透传：阶段 2 接受不解析）。
fn stage8_config() -> serde_json::Value {
    serde_json::json!({
        "binance": {
            "market": "crypto_perpetual",
            "services": { "kline": { "url": "ws://example/binance" } },
            "aggregation": "utc_grid",
            "fee_rate": 0.0004,
            "symbols": ["BTCUSDT"]
        },
        "ctp": {
            "market": "cn_futures",
            "services": { "kline": { "url": "ws://example/ctp" } },
            "aggregation": "session_window",
            "session": {
                "timezone_offset_hours": 8,
                "day_cutover": "20:00",
                "windows": [
                    { "open": "09:00", "close": "10:15", "kind": "day" },
                    { "open": "10:30", "close": "11:30", "kind": "day" },
                    { "open": "13:30", "close": "15:00", "kind": "day" },
                    { "open": "21:00", "close": "01:00", "kind": "night" }
                ]
            },
            "sessions": {
                "AU": {
                    "windows": [
                        { "open": "09:00", "close": "10:15", "kind": "day" },
                        { "open": "10:30", "close": "11:30", "kind": "day" },
                        { "open": "13:30", "close": "15:00", "kind": "day" },
                        { "open": "21:00", "close": "02:30", "kind": "night" }
                    ]
                }
            },
            "contracts": {
                "CU": {
                    "multiplier": 5, "tick_size": 10, "margin_rate": 0.11, "min_lots": 1,
                    "commission": { "mode": "rate", "open": 0.00005, "close": 0.00005, "close_today": 0.0001 }
                },
                "AU": {
                    "multiplier": 1000, "tick_size": 0.02, "margin_rate": 0.16, "min_lots": 1,
                    "price_precision": 2,
                    "commission": { "mode": "per_lot", "open": 20.0, "close": 20.0, "close_today": 0.0 }
                }
            },
            "subscription": { "policy": "open_interest", "include_second_main": true }
        }
    })
}

/// 混跑/装配用例走直连计划：`subscription:` 选主在 `build()` 里按
/// `SourceRequest.as_of`（v2.6 阶段 3 已闭环时钟债）+ 合约快照解析，
/// 空快照在**历史 as_of** 下必报错——该维度由下方
/// `as_of_injection_makes_main_contract_backtest_assemblable` 覆盖；
/// 本 fixture 供不需要选主语义的用例使用。
fn config_direct_subs() -> serde_json::Value {
    let mut cfg = stage8_config();
    cfg["ctp"]
        .as_object_mut()
        .expect("fixture")
        .remove("subscription");
    cfg
}

/// 阶段 2 入口验收：`exchanges:` 装配出双交易所注册表；实例名 `ctp`
/// 与场所 id `SHFE` 经别名表互相解析；未知类型报错并提示扩展位。
#[test]
fn stage8_config_assembles_both_exchanges() {
    let registry = build_registry(&entries(stage8_config())).unwrap();
    assert_eq!(registry.len(), 2);
    let mut ids = registry.ids();
    ids.sort();
    assert_eq!(ids, vec!["SHFE".to_string(), "binance".to_string()]);
    // 实例名 / id 双通道解析
    let ctp = registry.get_by_name("ctp").unwrap();
    assert_eq!(ctp.id().as_str(), "SHFE");
    assert!(registry.get_by_name("SHFE").is_ok(), "id 直查同样可用");
    assert!(registry.get_by_name("nope").is_err());

    // `subscription:` 配置生效：open_interest 选主 + 次主力订阅。
    // 快照 CU2602 OI 大于 CU2603（CU2601 已进入交割月，缺省剔除）。
    let snap = |sym: &str, oi: f64| -> ContractSnapshot {
        ContractSnapshot {
            instrument: ctp.parse_instrument(sym).unwrap(),
            open_interest: oi,
            volume: oi * 0.5,
            last_price: 50_000.0,
        }
    };
    let universe = vec![snap("CU2602", 100_000.0), snap("CU2603", 50_000.0)];
    let logical = ctp.parse_instrument("CU").unwrap();
    let subs = ctp
        .subscription_plan()
        .resolve(&logical, utc(2026, 1, 5, 1, 0), &universe)
        .unwrap();
    assert_eq!(subs.len(), 2, "include_second_main=true 必须双订阅");
    let main = subs
        .iter()
        .find(|s| s.role == ContractRole::Main)
        .expect("主力订阅存在");
    assert_eq!(main.instrument.symbol, "CU2602", "持仓量最大者当选");

    // 未知类型：报错文案给出扩展路径
    let err = build_err(entries(serde_json::json!({ "cme": {} })));
    assert!(err.contains("未知交易所类型: cme"), "实际: {err}");
}

/// 阶段 4（§8 注③债务闭环）：`services:` 子树**类型化消费**——
/// §8 示例的两所 endpoints 落到强类型（url 可直读、市场/闸门口径可
/// 表达），未知键进 `extra` 不丢失；未声明 services 的实例返回 None
/// （回落全局旧形态的兼容路径由组装根负责，SPI 不静默造地址）。
#[test]
fn services_subtree_is_typed_and_preserved() {
    use strategy_engine::exchanges::binance::config::BinanceConfig;
    use strategy_engine::exchanges::ctp::config::CtpConfig;

    let ctp_cfg = serde_json::to_value(&stage8_config()["ctp"]).unwrap();
    let ctp_ex = CtpConfig::from_value(&ctp_cfg)
        .unwrap()
        .build()
        .expect("§8 形态的 ctp services 必须解析成功");
    let services = ctp_ex.live_services().expect("ctp services 已类型化挂载");
    assert_eq!(
        services.kline.as_ref().and_then(|k| k.url.as_deref()),
        Some("ws://example/ctp"),
        "kline.url 类型化可达"
    );
    assert!(services.trading.is_none(), "§8 示例未声明 trading 端点");

    let bin_cfg = serde_json::to_value(&stage8_config()["binance"]).unwrap();
    let bin_ex = BinanceConfig::from_value(&bin_cfg)
        .unwrap()
        .build()
        .expect("binance services 同样类型化");
    let services = bin_ex.live_services().unwrap();
    assert_eq!(
        services.kline.as_ref().and_then(|k| k.url.as_deref()),
        Some("ws://example/binance")
    );

    // 未知键不丢：submit_enabled 类型化 + 未知字段/端点进 extra 原样保留
    let mut cfg = stage8_config();
    cfg["ctp"]["services"].as_object_mut().unwrap().insert(
        "trading".into(),
        serde_json::json!({
            "url": "ws://example/trade",
            "submit_enabled": true,
            "strategy_secret_env": "STRATEGY_SECRET",
        }),
    );
    cfg["ctp"]["services"]
        .as_object_mut()
        .unwrap()
        .insert("quote".into(), serde_json::json!({ "url": "ws://quote" }));
    let ctp_cfg = serde_json::to_value(&cfg["ctp"]).unwrap();
    let ex = CtpConfig::from_value(&ctp_cfg).unwrap().build().unwrap();
    let services = ex.live_services().unwrap();
    let trading = services.trading.as_ref().expect("trading 端点");
    assert_eq!(trading.url.as_deref(), Some("ws://example/trade"));
    assert_eq!(trading.submit_enabled, Some(true));
    assert_eq!(
        trading
            .extra
            .get("strategy_secret_env")
            .and_then(|v| v.as_str()),
        Some("STRATEGY_SECRET"),
        "未知键进 extra 原样保留（不静默吞）"
    );
    assert_eq!(
        services
            .extra
            .get("quote")
            .and_then(|v| v.get("url"))
            .and_then(|v| v.as_str()),
        Some("ws://quote"),
        "未知端点名同样保留"
    );

    // 未声明 = None：不造默认地址
    let bare = serde_json::json!({ "market": "cn_futures", "contracts": {} });
    let ex = CtpConfig::from_value(&bare).unwrap().build().unwrap();
    assert!(ex.live_services().is_none());
}

/// C2/C12 的配置载体 + C13 品种回落：
/// 配置装配后 AU 与 CU 各查各的时段表；品种级 contracts 服务具体合约；
/// AU 的 per_lot 费率与价格精度按配置生效。
#[test]
fn ctp_config_per_product_and_product_level_specs() {
    let registry = build_registry(&entries(config_direct_subs())).unwrap();
    let ctp = registry.get_by_name("ctp").unwrap();

    let au = ctp.parse_instrument("AU2606").unwrap();
    let cu = ctp.parse_instrument("CU2601").unwrap();
    let day = TradingDay(chrono::NaiveDate::from_ymd_opt(2026, 1, 5).unwrap());
    // 周五夜盘归周一交易日：AU 收周六凌晨 02:30（UTC 18:30），CU 01:00（UTC 17:00）
    let au_end = ctp
        .session_rules(&au)
        .sessions_on(day)
        .into_iter()
        .find(|w| w.kind == SessionKind::Night)
        .expect("AU 夜盘")
        .end;
    let cu_end = ctp
        .session_rules(&cu)
        .sessions_on(day)
        .into_iter()
        .find(|w| w.kind == SessionKind::Night)
        .expect("CU 夜盘")
        .end;
    assert_eq!(au_end, utc(2026, 1, 2, 18, 30));
    assert_eq!(cu_end, utc(2026, 1, 2, 17, 0));

    // C13：contracts 只登记品种 `CU`，具体合约 `CU2601` 精确 miss 后回落命中
    let spec = ctp.trading_rules().spec(&cu).expect("品种级回落");
    assert_eq!(spec.multiplier, 5.0);
    assert_eq!(spec.fee.mode, FeeMode::Rate);
    assert_eq!(spec.fee.rate_for(Offset::CloseToday), 0.0001);
    // AU：per_lot 费率 + 2 位价格精度（tick 0.02）
    let au_spec = ctp.trading_rules().spec(&au).unwrap();
    assert_eq!(au_spec.tick_size, 0.02);
    assert_eq!(au_spec.fee.mode, FeeMode::PerLot);
    assert_eq!(au_spec.fee.open, 20.0);
    assert_eq!(au_spec.price_precision, 2);

    // 第一公理在配置装配下同样成立：两次 build 的规则是同一批 Arc
    let req = SourceRequest::backtest(cu.clone(), vec![], 1_000_000.0);
    let p1 = ctp.build(&req).unwrap();
    let p2 = ctp.build(&req).unwrap();
    assert!(
        Arc::ptr_eq(&p1.trading, &p2.trading),
        "配置装配不得破坏规则单一来源"
    );
    assert!(Arc::ptr_eq(&p1.session, &p2.session));
}

/// 阶段 2 验收主案：binance + ctp 在**同一进程、同一注册表**下装配
/// 并各自跑通 ExchangeRunner 主循环（回测注入源），互不污染。
#[test]
fn binance_and_ctp_mixed_run_in_one_process() {
    let registry = build_registry(&entries(config_direct_subs())).unwrap();

    let bin_inst = registry
        .get_by_name("binance")
        .unwrap()
        .parse_instrument("BTCUSDT")
        .unwrap();
    let ctp_inst = registry
        .get_by_name("ctp")
        .unwrap()
        .parse_instrument("CU2601")
        .unwrap();

    // 4 根 1m：binance UTC 连续；ctp 落在北京日盘（01:30Z = 09:30）
    let bin_bars: Vec<Bar> = (0..4)
        .map(|i| bar(&bin_inst, utc(2026, 1, 5, 0, 1 + i), 60_000.0 + i as f64))
        .collect();
    let ctp_bars: Vec<Bar> = (0..4)
        .map(|i| bar(&ctp_inst, utc(2026, 1, 5, 1, 30 + i), 50_000.0))
        .collect();

    let mut parts = registry
        .build_all(vec![
            SourceRequest::backtest(bin_inst.clone(), bin_bars, 10_000.0),
            SourceRequest::backtest(ctp_inst.clone(), ctp_bars, 1_000_000.0),
        ])
        .unwrap();
    assert_eq!(parts.len(), 2);
    let bin_key = bin_inst.namespaced_key();
    let ctp_key = ctp_inst.namespaced_key();
    assert!(parts.contains_key(&bin_key) && parts.contains_key(&ctp_key));

    let mut bin_runner =
        ExchangeRunner::new(parts.remove(&bin_key).unwrap(), vec![Timeframe::M1], 200);
    let mut ctp_runner =
        ExchangeRunner::new(parts.remove(&ctp_key).unwrap(), vec![Timeframe::M1], 200);

    let bin_sum = bin_runner.run(&mut OpenThenClose { calls: 0 }).unwrap();
    let ctp_sum = ctp_runner.run(&mut OpenThenClose { calls: 0 }).unwrap();

    // 两边都完成 开+平 两笔成交
    assert_eq!(bin_sum.fills.len(), 2, "binance 开平两笔");
    assert_eq!(ctp_sum.fills.len(), 2, "ctp 开平两笔");
    // 口径隔离：期货整数手，crypto 小数数量——同一主循环、不同规则表
    assert!(
        ctp_sum.fills.iter().all(|f| f.qty.fract() == 0.0),
        "CTP 必须整数手: {:?}",
        ctp_sum.fills.iter().map(|f| f.qty).collect::<Vec<_>>()
    );
    assert!(
        bin_sum.fills[0].qty.fract() != 0.0,
        "crypto 应为小数数量: {}",
        bin_sum.fills[0].qty
    );
    // 各自终态平
    assert!(ctp_sum
        .final_positions
        .iter()
        .all(|l| l.remaining <= f64::EPSILON));
    assert!(bin_sum
        .final_positions
        .iter()
        .all(|l| l.remaining <= f64::EPSILON));
}

/// 配置错误必须在**装配期**爆炸，而不是留到盘中：
/// 非法时段名 / 缺关键字段 / 非法费率模式 / 拼错的键都拒绝。
#[test]
fn bad_config_fails_at_assembly() {
    // 非法时段类型
    let mut cfg = stage8_config();
    cfg["ctp"]["sessions"]["AU"]["windows"][0]["kind"] = serde_json::json!("midnight");
    assert!(
        build_registry(&entries(cfg)).is_err(),
        "非法 kind 必须装配期报错"
    );

    // 缺 tick_size（关键参数缺省 0 拒绝）
    let mut cfg = stage8_config();
    cfg["ctp"]["contracts"]["CU"]
        .as_object_mut()
        .unwrap()
        .remove("tick_size");
    let err = build_err(entries(cfg));
    assert!(err.contains("tick_size"), "实际: {err}");

    // 非法手续费模式
    let mut cfg = stage8_config();
    cfg["ctp"]["contracts"]["CU"]["commission"]["mode"] = serde_json::json!("flat");
    assert!(build_err(entries(cfg)).contains("非法手续费模式"));

    // deny_unknown_fields：拼错的键不被静默吞掉
    let mut cfg = stage8_config();
    cfg["binance"]["fee_ratee"] = serde_json::json!(0.001);
    let err = build_err(entries(cfg));
    assert!(err.contains("binance 配置解析失败"), "实际: {err}");
}

/// 阶段 3（v2.6）时钟债闭环验收：`SourceRequest.as_of` 注入历史时刻 +
/// 当日快照后，带 `subscription:`（MainContractPlan）的配置可以在
/// **回测模式**装配成功——阶段 2 时 `build()` 硬用 `Utc::now()`，历史
/// 口径在装配期必报「无可用合约快照」（§8 注登记，阶段 3 范围）。
/// 同时验证：as_of 语义是纯注入，缺省路径（live 墙钟）行为不变。
#[test]
fn as_of_injection_makes_main_contract_backtest_assemblable() {
    let registry = build_registry(&entries(stage8_config())).unwrap();
    let ctp = registry.get_by_name("ctp").unwrap();
    let logical = ctp.parse_instrument("CU").unwrap();
    let snap = |sym: &str, oi: f64| -> ContractSnapshot {
        ContractSnapshot {
            instrument: ctp.parse_instrument(sym).unwrap(),
            open_interest: oi,
            volume: oi * 0.5,
            last_price: 50_000.0,
        }
    };

    // 历史 as_of：2026-01-05（CU2601 交割月中被缺省剔除，CU2602 主、CU2603 次）
    let mut req = SourceRequest::backtest(logical.clone(), vec![], 1_000_000.0);
    req.as_of = Some(utc(2026, 1, 5, 1, 0));
    req.universe = vec![snap("CU2602", 100_000.0), snap("CU2603", 50_000.0)];
    let parts = ctp
        .build(&req)
        .expect("历史 as_of + 快照：回测装配必须成功");
    assert_eq!(
        parts.subscriptions.len(),
        2,
        "include_second_main=true 双订阅必须过装配"
    );
    let main = parts
        .subscriptions
        .iter()
        .find(|s| s.role == ContractRole::Main)
        .expect("主力存在");
    assert_eq!(main.instrument.symbol, "CU2602", "选主按注入时点解析");

    // 快照相对 as_of 全部过期 → 仍是装配期报错（错误不吞，语义纯注入）
    let mut stale = SourceRequest::backtest(logical, vec![], 1_000_000.0);
    stale.as_of = Some(utc(2026, 6, 1, 1, 0));
    stale.universe = vec![snap("CU2602", 100_000.0), snap("CU2603", 50_000.0)];
    let err = match ctp.build(&stale) {
        Ok(_) => panic!("交割月过后的快照必须报错"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("快照"), "实际: {err}");
}
