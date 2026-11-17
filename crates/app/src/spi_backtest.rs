//! engine 回测后端装配（§13 阶段 3：「新增 engine 回测为可选后端，与旧 `Engine` 双跑」）。
//!
//! 这里是**组合根**——只有 app 层可以同时看见 `engine`、`strategy-data`
//! （CSV 存储）与统一 `CrossSectionalStrategyPort` 面：
//! - 数据面：[`csv_1m_candles`] 读命名空间布局 `{data_dir}/{tf}/{exchange}/{market}/
//!   {SYMBOL}_{tf}.csv`（§6.1/§17 最小嵌入路径-2：包住 `csv_store`，非重写；
//!   阶段 5c 命名空间已交付，旧布局经 `resolve_csv_path` 一次迁移）；
//! - 事件面：[`csv_backtest_events`] 经 `core::replay::events_from_bars` 注入
//!   午休/隔夜断开与交易日切换（第一公理：与实盘同一段主循环消费）；
//! - 策略面：`bridge::PortStrategyBridge` 把统一策略 port 接入
//!   `ExchangeRunner`。
//!
//! 本模块**不含任何规则与结算逻辑**——规则全部来自 `Exchange` 装配的
//! `Arc<dyn Rules>`，撮合全部在 `SimulationExecution`。

use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use strategy_core::candle::Candle;
use strategy_core::risk::RiskControlConfig;
use strategy_core::signal::Side;
use strategy_engine::bridge::PortStrategyBridge;
use strategy_engine::core::execution::Account;
use strategy_engine::core::replay::events_from_bars;
use strategy_engine::exchanges::binance::BinanceExchange;
use strategy_engine::exchanges::ctp::CtpExchange;
use strategy_engine::prelude::*;
use strategy_engine::strategy::port::CrossSectionalStrategyPort;
use strategy_store::Store;

use crate::config::SymbolSpec;
use crate::report::{EquityPoint, EquitySink, TradeRow};

/// `Candle` 序列 → `Bar` 序列（§13 `Candle <-> Bar` 适配；
/// CSV 帧 `is_closed=true` → `BarFinality::Final`）。
pub fn candles_to_bars(instrument: &InstrumentId, candles: &[Candle]) -> Vec<Bar> {
    candles
        .iter()
        .map(|c| Bar::from_candle(instrument.clone(), *c))
        .collect()
}

/// 读 1m CSV（命名空间布局 §6.1；裸 symbol 兼容旧布局并自动迁移），
/// 过滤到 `[from, to)`。
pub fn csv_1m_candles(
    data_dir: &Path,
    venue_key: &str,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> Vec<Candle> {
    let all = strategy_data::csv_store::load_or_empty(
        data_dir,
        venue_key,
        strategy_core::candle::Timeframe::M1,
    )
    .unwrap_or_default();
    all.into_iter()
        .filter(|c| from.is_none_or(|f| c.open_time >= f))
        .filter(|c| to.is_none_or(|t| c.open_time < t))
        .collect()
}

/// CSV 回放事件流：1m 帧 + 按 `SessionRules` 注入断开/日界事件。
/// 与 live 的 WS 源同形状——主循环不感知差别。
/// 数据路径键 = `instrument.namespaced_key()`（§6.1，两角色分离：
/// 帧内挂牌代码由 instrument.symbol 承载，路径/行键走命名空间键）。
pub fn csv_backtest_events(
    data_dir: &Path,
    instrument: &InstrumentId,
    session: &dyn SessionRules,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
) -> Vec<MarketEvent> {
    let candles = csv_1m_candles(data_dir, &instrument.namespaced_key(), from, to);
    events_from_bars(candles_to_bars(instrument, &candles), session)
}

/// 一次 engine 回测装配：`Exchange::build` → `ExchangeRunner` +
/// 现有策略桥。与旧 `Engine` 完全同数据可双跑对比（阶段 3 验收）。
pub struct SpiBacktest {
    pub runner: ExchangeRunner,
    pub bridge: PortStrategyBridge,
}

impl SpiBacktest {
    /// 装配。`req` 决定模式/标的/初始资金/事件源（bars 或自定义 source）；
    /// `sizing`/`risk` 是 C6（`--leverage`/名义口径）与统一风控兜底的注入通道。
    pub fn assemble(
        exchange: &Arc<dyn Exchange>,
        req: SourceRequest,
        strategy: Box<dyn CrossSectionalStrategyPort>,
        sizing: SizingParams,
        risk: RiskControlConfig,
        max_closed_len: usize,
    ) -> Result<Self> {
        let parts = exchange.build(&req)?;
        let bridge = PortStrategyBridge::new(strategy);
        let tfs = bridge.timeframes();
        let runner =
            ExchangeRunner::with_strategy_type(parts, tfs, max_closed_len, bridge.strategy_type())
                .with_risk_config(risk)
                .with_sizing_params(sizing);
        Ok(Self { runner, bridge })
    }

    /// warmup 注入（旧 `Engine::preload` 同构）。
    pub fn preload(&mut self, bars: &[Bar]) -> Result<()> {
        self.runner.preload(bars)
    }

    /// 替换行情源（CSV 事件流注入点；build 之后、run 之前一次赋值）。
    pub fn set_source(&mut self, source: Box<dyn BarEventSource>) {
        self.runner.set_source(source);
    }

    /// 跑完整回测（同步主循环，无 `if mode == backtest`）。
    pub fn run(&mut self) -> Result<RunSummary> {
        self.runner.run(&mut self.bridge)
    }

    /// 阶段 5（v2.8）单步接缝：消费一个市场事件（run 的步进形态）。
    /// CLI/编排层用它两根 1m 之间采样 equity，时点口径与旧 `Engine::on_equity_mark`
    /// （该模块已随 R4 退役删除）一致：本根收盘盯市后取账户权益——等价性由
    /// `tests/backtest_paper_parity.rs` 的权益曲线对照承重。
    pub fn step(&mut self, ev: MarketEvent) -> Result<()> {
        self.runner.on_event(ev, &mut self.bridge)
    }

    /// 流尽收口（终态持仓/账户入 summary）。
    pub fn finish(&mut self) -> Result<RunSummary> {
        self.runner.finish()
    }

    /// 运行中账户视图（equity 采样接缝）。
    pub fn account_view(&self) -> Option<Account> {
        self.runner.account()
    }

    /// 账户终态（现金/权益/费用汇总，供双跑与报表）。
    pub fn account(&self) -> Option<Account> {
        self.runner.parts.execution.account_snapshot()
    }

    /// 阶段 5（v2.8）：聚合大周期闭合 K 线导出（原与旧 `Engine::export_merged_klines`
    /// 同构，该模块已随 R4 退役删除；本方法为其唯一存续实现——命名空间布局 `{base}/{tf}/{venue...}/{SYMBOL}_{tf}.csv`，§6.1，
    /// 1m 跳过）。数据面就是桥接的 `CandleSeries`（ClosedSource），与策略看到
    /// 的是同一份序列。
    pub fn export_merged_klines(&self, base: &Path, venue_key: &str) -> std::io::Result<usize> {
        use strategy_core::candle::Timeframe as CoreTf;
        let mut written = 0usize;
        for tf in self.bridge.timeframes() {
            if tf.0 <= 1 {
                continue;
            }
            let core_tf = CoreTf::from(tf);
            let Some(bars) = self.bridge.closed_source().closed(core_tf) else {
                continue;
            };
            if bars.is_empty() {
                continue;
            }
            strategy_data::csv_store::save_csv(
                strategy_data::csv_store::csv_path(base, venue_key, core_tf),
                bars,
            )?;
            written += 1;
        }
        Ok(written)
    }
}

// ─── 阶段 5（v2.8）：engine 回测装配面（§13-5「两模式共用主循环」；原
//     `--engine` CLI 参数已随 R4/D1 退役删除，本路径现为唯一路径）──

/// 从 `SymbolSpec` 合成 engine `Exchange`——与旧 CLI 链路同一份输入
/// （`config/contracts.yaml` 的合约参数 ⇒ `spec_from_contract` 映射；
/// 无合约 ⇒ crypto 永续）。**规则只在此装配一次**，live/backtest
/// 共用（首公理）；阶段 5b live 编排同样吃这个函数。
pub fn exchange_for_spec(
    spec: &SymbolSpec,
    fee_rate: Option<f64>,
) -> anyhow::Result<(Arc<dyn Exchange>, InstrumentId)> {
    match &spec.contract {
        Some(c) => {
            let base = CtpExchange::named(ExchangeId::new("SHFE"));
            let inst = base
                .parse_instrument(&spec.symbol)
                .map_err(|e| anyhow::anyhow!("标的解析失败: {e}"))?;
            let ex = base.with_spec(inst.clone(), spec_from_contract(c));
            Ok((Arc::new(ex), inst))
        }
        None => {
            let rate = fee_rate.unwrap_or(0.0004);
            let base = BinanceExchange::named(ExchangeId::new("binance"), rate);
            let inst = base
                .parse_instrument(&spec.symbol)
                .map_err(|e| anyhow::anyhow!("标的解析失败: {e}"))?;
            let mut ispec = strategy_engine::core::trading::InstrumentSpec::crypto_perpetual(rate);
            // 场所真实数量约束优先于适配器默认：低于步长的平仓差额由
            // normalize_qty 取整为 0（视为已达标），不发单被场所拒绝。
            if let Some(v) = spec.venue_qty_step {
                ispec.qty_step = v;
            }
            if let Some(v) = spec.venue_min_qty {
                ispec.min_qty = v;
            }
            if let Some(v) = spec.venue_qty_precision {
                ispec.qty_precision = v;
            }
            let ex = base.with_symbol_spec(inst.clone(), ispec);
            Ok((Arc::new(ex), inst))
        }
    }
}

/// 旧 CLI 链（`Engine`/`RuntimeActor`）的行键命名空间键（§13-5c v2.9）：
/// market kind 由合约有无派生（期货 ⇒ `cn_futures`，否则 ⇒
/// `crypto_perpetual`），格式委托 engine `namespaced_key_of` 单一实现——与
/// `exchange_for_spec` 装配的 `InstrumentId::namespaced_key()` 同源不分叉。
pub fn venue_key_for(
    exchange: &str,
    contract: Option<&strategy_core::contract::ContractSpec>,
    symbol: &str,
) -> String {
    let market = if contract.is_some() {
        MarketKind::CnFutures
    } else {
        MarketKind::CryptoPerpetual
    };
    namespaced_key_of(exchange, market, symbol)
}

/// `Fill` 序列 → `TradeRow`（§17 app 行「`TradeRow` 从 `Fill` 派生」）。
/// SignalType 标签口径（与旧 `Engine::record_trade` 一致，该模块已随 R4 删除，
/// 等价性由 `tests/spi_dual_run_parity.rs` 的 TradeRow 冻结对钉死）：
/// 开仓 Buy/SELL 按方向；平仓按**成交方向**带 `_CLOSE` 后缀
/// （买平空=BUY_CLOSE，卖平多=SELL_CLOSE）。
/// 逐笔一致验收字段（时刻/方向/量/价/pnl）与旧链路同构；
/// `id/trade_id` 因 signal_id 方案不同（差异表 #2）不在对比面；
/// comment 仅止损标注（旧 comment 携带策略 reason 文本，Fill 通道
/// 不搬运——报表口径差异如实声明）。
pub fn fills_to_trade_rows(
    fills: &[strategy_engine::core::execution::Fill],
    strategy_name: &str,
    strategy_type: &str,
    symbol: &str,
) -> Vec<TradeRow> {
    fills
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let side = match (f.offset, f.side) {
                (Offset::Open, Side::Long) => "BUY",
                (Offset::Open, Side::Short) => "SELL",
                (_, Side::Long) => "BUY_CLOSE",
                (_, Side::Short) => "SELL_CLOSE",
            };
            let comment = if f.is_stop_loss { "stop_loss" } else { "" };
            TradeRow::new(
                i + 1,
                strategy_name,
                strategy_type,
                symbol,
                side,
                f.qty,
                f.price,
                f.fee,
                0.0,
                f.realized_pnl,
                &f.at.format("%Y-%m-%dT%H:%M:%S").to_string(),
                comment,
                &f.signal_id,
            )
        })
        .collect()
}

/// engine 回测一把梭的全部报表输入（main.rs 两个引擎分支喂同一
/// `ReportGenerator`——报表层「共用」自此成立）。
pub struct SpiBacktestRun {
    pub summary: RunSummary,
    pub trade_rows: Vec<TradeRow>,
    pub daily: Vec<EquityPoint>,
    pub tf_points: Vec<EquityPoint>,
    /// 报表初始资金 = 内核实际起始现金（与旧分支同口径注释）。
    pub initial_cash: f64,
    pub klines_exported: usize,
}

/// CLI 装配参数（旧分支的 spec/candles/cash 全量对应；无隐藏默认）。
pub struct SpiRunArgs<'a> {
    pub spec: &'a SymbolSpec,
    pub strategy: Box<dyn CrossSectionalStrategyPort>,
    /// 运行时实例名（equity/trade 行过滤键，同 `runtime_name(...)`）
    pub strategy_name: &'a str,
    pub warmup: &'a [Candle],
    pub trading: &'a [Candle],
    pub initial_cash: f64,
    pub leverage: f64,
    pub fee_rate: Option<f64>,
    pub risk: RiskControlConfig,
    /// 状态目录（equity 表落盘基；回测重建语义由调用方清目录保证）
    pub state_dir: &'a Path,
    /// 大周期 K 线导出根目录（= data_dir，与旧分支一致）
    pub data_dir: &'a Path,
}

/// 阶段 5 验收「两模式共用主循环」的回测侧闭环：spec → Exchange 合成 →
/// `ExchangeRunner`（与 live 同一段代码）步进 + 每根 1m 权益采样 +
/// Fill→TradeRow 派生。撮合/风控/止损机全在 runner（§9.1），本函数
/// 只做编排与报表搬运。
pub fn run_from_spec(args: SpiRunArgs) -> anyhow::Result<SpiBacktestRun> {
    use strategy_core::candle::Timeframe as CoreTf;
    let (exchange, inst) = exchange_for_spec(args.spec, args.fee_rate)?;
    // 全仓语义与旧回测内核一致（main.rs 硬编码 notional_per_order=0 ⇒ 全仓）
    let sizing = SizingParams {
        leverage: args.leverage,
        notional_per_order: None,
    };
    let req = SourceRequest::backtest(inst.clone(), vec![], args.initial_cash);
    let mut bt = SpiBacktest::assemble(&exchange, req, args.strategy, sizing, args.risk, 2000)
        .map_err(|e| anyhow::anyhow!("engine 装配失败: {e}"))?;
    bt.preload(&candles_to_bars(&inst, args.warmup))
        .map_err(|e| anyhow::anyhow!("engine warmup 失败: {e}"))?;

    // 状态行时间戳禁止墙钟（与横截面回测同一红线）：按日裁剪需要「今天」
    // 与 K 线时间同源，否则模拟行情一开机就被判成历史日、当日终态全被裁掉。
    let first_ms = args
        .trading
        .first()
        .map(|c| c.open_time.timestamp_millis())
        .unwrap_or(0);
    let store = Arc::new(Store::new(
        args.state_dir,
        Arc::new(BacktestClock(first_ms)),
    ));
    let sink = EquitySink::new(args.strategy_name, &store);
    let tf_minutes = CoreTf::parse(&args.spec.timeframe)?.0;

    let mut last_eq = None;
    for c in args.trading {
        let bar = Bar::from_candle(inst.clone(), *c);
        let t = bar.open_time;
        bt.step(MarketEvent::Bar(bar))
            .map_err(|e| anyhow::anyhow!("engine 主循环失败: {e}"))?;
        if let Some(acct) = bt.account_view() {
            // 本根收盘盯市后取权益——与旧 `on_equity_mark(c)` 同一时点
            sink.on_1m_close(t, acct.equity(), acct.cash, tf_minutes);
            last_eq = Some((acct.equity(), acct.cash));
        }
    }
    store
        .flush()
        .map_err(|e| anyhow::anyhow!("回测状态 flush 失败: {e}"))?;
    let summary = bt
        .finish()
        .map_err(|e| anyhow::anyhow!("engine 收口失败: {e}"))?;
    let _ = last_eq;

    let trade_rows = fills_to_trade_rows(
        &summary.fills,
        args.strategy_name,
        &args.spec.strategy,
        &args.spec.symbol,
    );
    let initial_cash = summary
        .account
        .as_ref()
        .map(|a| a.initial_cash)
        .unwrap_or(args.initial_cash);
    let klines_exported = bt
        .export_merged_klines(args.data_dir, &args.spec.venue_key)
        .unwrap_or_default();

    Ok(SpiBacktestRun {
        summary,
        trade_rows,
        daily: sink.daily(),
        tf_points: sink.tf(),
        initial_cash,
        klines_exported,
    })
}

// ─── 横截面回测装配（PR-2/PR-4，设计 §3 RunnerGroup）─────────────────────────

/// 回测 Store 固定时钟：锚定序列首根 K 线时间（红线：状态行时间戳禁止墙钟）。
struct BacktestClock(i64);
impl strategy_store::Clock for BacktestClock {
    fn now_ms(&self) -> i64 {
        self.0
    }
}

/// `run_strategy_from_spec` 输入（与 `SpiRunArgs` 平行；横截面专属）。
pub struct StrategyRunArgs<'a> {
    pub spec: &'a crate::strategy_config::StrategySpec,
    pub strategy: Box<dyn strategy_engine::strategy::port::CrossSectionalStrategyPort>,
    /// 每标的 warmup（[warmup_start, start)），与 spec.instruments 同序等长
    pub warmup: &'a [Vec<strategy_core::candle::Candle>],
    /// 每标的交易段（[start, end)）
    pub trading: &'a [Vec<strategy_core::candle::Candle>],
    pub initial_cash: f64,
    pub fee_rate: Option<f64>,
    pub state_dir: &'a Path,
    pub data_dir: &'a Path,
}

/// 横截面回测产物（组合口径：单一 NAV 曲线 + 全标的成交行）。
pub struct StrategyBacktestRun {
    pub summary: strategy_engine::strategy::runner::StrategyRunSummary,
    pub trade_rows: Vec<crate::report::TradeRow>,
    pub daily: Vec<crate::report::EquityPoint>,
    pub tf_points: Vec<crate::report::EquityPoint>,
    pub initial_cash: f64,
}

/// `Fill` → `TradeRow`（横截面：symbol 取各 fill 自身标的；其余口径与
/// `fills_to_trade_rows` 一致）。
pub fn fills_to_strategy_trade_rows(
    fills: &[strategy_engine::core::execution::Fill],
    strategy_name: &str,
    strategy_type: &str,
) -> Vec<crate::report::TradeRow> {
    use strategy_core::signal::Side;
    use strategy_engine::core::trading::Offset;
    fills
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let side = match (f.offset, f.side) {
                (Offset::Open, Side::Long) => "BUY",
                (Offset::Open, Side::Short) => "SELL",
                (_, Side::Long) => "BUY_CLOSE",
                (_, Side::Short) => "SELL_CLOSE",
            };
            let comment = if f.is_stop_loss { "stop_loss" } else { "" };
            crate::report::TradeRow::new(
                i + 1,
                strategy_name,
                strategy_type,
                &f.instrument.symbol,
                side,
                f.qty,
                f.price,
                f.fee,
                0.0,
                f.realized_pnl,
                &f.at.format("%Y-%m-%dT%H:%M:%S").to_string(),
                comment,
                &f.signal_id,
            )
        })
        .collect()
}

/// 横截面回测一把梭：N 份 `ExchangeParts`（每 slot 资金 = 总额/N，等分记账，
/// NAV = Σ equity）→ `CrossSectionalRunnerGroup` 唯一主循环（与 live 同段代码，
/// G3 公理在回测端成立）。
pub fn run_strategy_from_spec(args: StrategyRunArgs<'_>) -> anyhow::Result<StrategyBacktestRun> {
    use strategy_engine::core::bar::{Timeframe, VecEventSource};
    use strategy_engine::core::exchange::SourceRequest;
    use strategy_engine::strategy::align::AlignedEventSource;
    use strategy_engine::strategy::features::FeaturePlan;
    use strategy_engine::strategy::runner::{CrossSectionalRunnerGroup, SlotInit, StrategySlot};
    use strategy_store::Store;

    let spec = args.spec;
    let n = spec.instruments.len();
    anyhow::ensure!(n >= 1, "横截面 universe 为空");
    anyhow::ensure!(
        args.warmup.len() == n && args.trading.len() == n,
        "warmup/trading 与 instruments 长度不一致"
    );
    let timeframes: Vec<Timeframe> = spec
        .config
        .timeframes
        .iter()
        .map(|s| Timeframe::parse(s).map_err(|e| anyhow::anyhow!("timeframe `{s}` 非法: {e}")))
        .collect::<anyhow::Result<_>>()?;
    let base_tf = *timeframes
        .first()
        .ok_or_else(|| anyhow::anyhow!("timeframes 为空"))?;
    // 合并总仓：每个 slot 以完整总资金虚拟起账，汇总层扣除重复现金；
    // 逐标的独立总仓：每个 slot 只拿自己那份，直接汇总 NAV。
    let slot_cash = if spec.config.merge_positions {
        args.initial_cash
    } else {
        args.initial_cash / n as f64
    };

    let mut slots: Vec<StrategySlot> = Vec::with_capacity(n);
    for (i, ispec) in spec.instruments.iter().enumerate() {
        let feature_plan = FeaturePlan::from_config(&spec.config)
            .map_err(|errs| anyhow::anyhow!("特征计划构建失败:\n- {}", errs.join("\n- ")))?;
        // 复用单标装配的规格表（crypto 费率/合约规格），避免两处 drift。
        let (exchange, inst) = exchange_for_spec(ispec, args.fee_rate)?;
        let mut parts = exchange
            .build(&SourceRequest::backtest(
                inst.clone(),
                Vec::new(),
                slot_cash,
            ))
            .map_err(|e| anyhow::anyhow!("{}: 装配失败: {e}", ispec.symbol))?;
        // 行情源 = 该标的交易段事件流（含 session gap/day 注入，与单标回测同法）。
        let events = events_from_bars(
            candles_to_bars(&inst, &args.trading[i]),
            parts.session.as_ref(),
        );
        parts.source = Box::new(VecEventSource::new(events));
        let mut slot = StrategySlot::new(SlotInit {
            parts,
            timeframes: timeframes.clone(),
            feature_plan,
            risk: ispec.risk.clone(),
            strategy_type: spec.strategy_type.clone(),
            max_closed_len: 2000,
        });
        slot.preload(&candles_to_bars(&inst, &args.warmup[i]))
            .map_err(|e| anyhow::anyhow!("{}: warmup 失败: {e}", ispec.symbol))?;
        slots.push(slot);
    }

    // Store 时钟锚定首根交易 bar（跨标的取最小），非墙钟。
    let first_ms = args
        .trading
        .iter()
        .filter_map(|v| v.first())
        .map(|c| c.open_time.timestamp_millis())
        .min()
        .unwrap_or(0);
    let store = std::sync::Arc::new(Store::new(
        args.state_dir,
        std::sync::Arc::new(BacktestClock(first_ms)),
    ));
    let mut group = CrossSectionalRunnerGroup::new(
        &spec.strategy_id,
        &spec.strategy_type,
        spec.config.clone(),
        slots,
        args.strategy,
        Some(store.clone()),
    )
    .map_err(|e| anyhow::anyhow!("RunnerGroup 装配失败: {e}"))?;
    if spec.config.merge_positions {
        group.set_initial_cash_total(args.initial_cash);
    } else {
        group.set_initial_cash_split(args.initial_cash);
    }
    group.enable_history_projection(args.state_dir, "backtest");
    let (taken, keys) = group.sources();
    let mut aligned = AlignedEventSource::new(taken, keys);
    let sink = crate::report::EquitySink::new(&spec.strategy_id, &store);
    while let Some(ev) = aligned
        .next_aligned()
        .map_err(|e| anyhow::anyhow!("对齐源失败: {e}"))?
    {
        if let Some(r) = group
            .on_event(ev)
            .map_err(|e| anyhow::anyhow!("主循环失败: {e}"))?
        {
            sink.on_1m_close(r.at, r.nav, r.cash, base_tf.0);
        }
    }
    let summary = group
        .finish()
        .map_err(|e| anyhow::anyhow!("收口失败: {e}"))?;
    if let Some(last) = group.take_last_round() {
        sink.on_1m_close(last.at, last.nav, last.cash, base_tf.0);
    }
    store
        .flush()
        .map_err(|e| anyhow::anyhow!("回测状态 flush 失败: {e}"))?;
    let trade_rows =
        fills_to_strategy_trade_rows(&summary.fills, &spec.strategy_id, &spec.strategy_type);
    Ok(StrategyBacktestRun {
        summary,
        trade_rows,
        daily: sink.daily(),
        tf_points: sink.tf(),
        initial_cash: args.initial_cash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_for_spec_applies_venue_qty_constraints() {
        // 配置中的场所步长必须进入装配后的交易规则：默认 1e-6 会让
        // 低于场所步长的平仓差额通过 normalize，被交易所按数量越界
        // 拒绝（fix5 实盘 BTCUSDT 0.000177 → QuantityOutOfRange）。
        let spec = SymbolSpec {
            strategy: "once_then_exit".into(),
            symbol: "BTCUSDT".into(),
            timeframe: "1m".into(),
            cash: 200.0,
            initial_cash: None,
            params: None,
            warmup_days: 0,
            exchange: "binance".into(),
            venue_key: venue_key_for("binance", None, "BTCUSDT"),
            account: "default".into(),
            leverage: 1,
            mode: "backtest".into(),
            contract: None,
            risk: RiskControlConfig::default(),
            venue_qty_step: Some(0.001),
            venue_min_qty: Some(0.001),
            venue_qty_precision: Some(3),
        };
        let (exchange, inst) = exchange_for_spec(&spec, Some(0.0004)).unwrap();
        let rules = exchange.trading_rules().spec(&inst).unwrap();
        assert_eq!(rules.qty_step, 0.001, "场所步长必须来自运行规格");
        assert_eq!(rules.min_qty, 0.001, "场所最小下单量必须来自运行规格");
        assert_eq!(rules.qty_precision, 3, "数量精度必须来自运行规格");
    }
}
