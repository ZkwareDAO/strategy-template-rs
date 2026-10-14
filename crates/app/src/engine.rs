//! 回测引擎
//!
//! 单 symbol 回测事件循环（多 symbol 由调用方并行跑多个引擎实例；
//! live 的策略驱动循环在 RuntimeActor 内，不经本引擎）：
//! 1. Feed 预载 warmup 1m（不触发事件）
//! 2. 每根已闭合 1m → Feed::on_closed_1m → 事件
//! 3. 有持仓 → strategy.on_realtime_tick；空仓 → strategy.on_candle
//! 4. Action → broker.execute（撮合/真实下单）→ exchange_orders.csv
//! 5. 撮合成功 → PositionManager 开/平仓 → 统一 Store（position/stop/cooldown/
//!    history_position）+ 平仓时旁路导出 history_positions 固定列 CSV；
//!    成交本身写 Store `trade` 表
//! 6. 开仓信号 → signal.csv（signal_id 由 K 线时间确定性生成）
//! 7. 每根 1m mark-to-market → Store `equity` 表（daily/tf 两桶 upsert）
//!
//! 确定性核心：Store 时钟锚定 K 线 open_time，全程无 Utc::now，回放可复现。

use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};

use strategy_broker::BacktestOrderClient;
use strategy_core::candle::Candle;
use strategy_core::risk::{RiskControlConfig, RiskController};
use strategy_core::signal::{
    generate_signal_id, intents_from_action, Action, IntentCtx, Side, SignalIntent, SignalType,
};
use strategy_core::strategy::{Ctx, PositionView, Strategy};
use strategy_core::FillEvent;
use strategy_data::feed::Feed;
use strategy_positions::PositionManager;
use strategy_signals::record::{OrderType, PosType, Side as CsvSide, SignalRecord};
use strategy_signals::writer::SignalWriter;
use strategy_signals::SignalManager;
use strategy_store::{Clock, ModelTable, Store};

use crate::report::{trade_rows, EquityPoint, EquitySink, TradeRow};

/// 引擎配置
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// 运行时策略名（进入 CSV 路径与 position_id，如 sar_snt3_8H_3_BTCUSDT_LIVE）
    pub strategy_name: String,
    /// 策略类型名（进入 signal_id 哈希，如 sar_snt3）
    pub strategy_type: String,
    pub symbol: String,
    /// live / paper_trading / smoking / backtest
    pub trading_mode: String,
    pub user_id: u64,
    pub leverage: u32,
    pub cash: f64,
    pub exchange: String,
    /// 每时间框架保留的已闭合根数（内存保护）
    pub max_closed_len: usize,
    /// 统一风控配置（策略特有出场不触发时的兜底；镜像 Python risk 节）
    pub risk: RiskControlConfig,
    /// 信号周期（如 "8h"；equity 表 tf 净值点按该周期分桶）
    pub signal_tf: String,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            strategy_name: "sar_snt3_8H_3_BTCUSDT_LIVE".into(),
            strategy_type: "sar_snt3".into(),
            symbol: "BTCUSDT".into(),
            trading_mode: "paper_trading".into(),
            user_id: 1,
            leverage: 1,
            cash: 200.0,
            exchange: "binance".into(),
            max_closed_len: 2000,
            risk: RiskControlConfig::default(),
            signal_tf: "1h".into(),
        }
    }
}

/// 引擎运行结果统计
#[derive(Debug, Default, Clone)]
pub struct RunStats {
    pub bars: usize,
    pub signals_written: usize,
    pub orders_written: usize,
    pub positions_opened: usize,
    pub positions_closed: usize,
}

/// 引擎：策略 + 信号管道 + 仓位管理 + 回测订单客户端 + 统一 Store（含
/// signal/position/stop/cooldown/history_position/trade/equity）
pub struct Engine {
    pub config: EngineConfig,
    feed: Feed,
    strategy: Box<dyn Strategy>,
    positions: PositionManager,
    /// 回测订单网关（与 SignalManager 共享同一撮合内核）
    client: BacktestOrderClient,
    /// 信号全生命周期（当前态常驻统一 Store，结束 flush 到当日快照）
    signals: SignalManager,
    /// 回测独立 Store（不与 live 进程共享；元数据时钟由 K 线时间驱动）
    store: Arc<Store>,
    /// 确定性时钟：始终指向当前回放 K 线 open_time（禁止 Utc::now）
    clock: Arc<CandleClock>,
    signal_writer: SignalWriter,
    risk: RiskController,
    cooldown: Option<strategy_core::strategy::Cooldown>,
    stats: RunStats,
    /// 回测成交（统一 Store `trade` 表；每次成交 insert 一行，开仓 pnl=0）
    trades: Arc<ModelTable<TradeRow>>,
    /// 净值写入端（统一 Store `equity` 表；daily/tf 同桶 upsert）
    equity: EquitySink,
    /// 成交序号（T%08 旧报表字段；随成交顺序确定性递增）
    trade_seq: usize,
    /// 上次记录权益的 1m 时间（防止同一根 1m 重复喂快照）
    last_equity_time: Option<DateTime<Utc>>,
}

/// 回测确定性时钟：Store 元数据时间戳始终取当前回放 K 线 open_time。
/// 每次 dispatch 前推进，全链路无 Utc::now()。
struct CandleClock(AtomicI64);

impl CandleClock {
    fn new() -> Self {
        Self(AtomicI64::new(0))
    }
    fn set(&self, t: DateTime<Utc>) {
        self.0.store(t.timestamp_millis(), Ordering::Relaxed);
    }
}

impl Clock for CandleClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// 成交角色（一条意图对应开仓或平仓）
#[derive(Clone, Copy)]
enum FillRole {
    Open {
        stop_price: f64,
        atr: f64,
        comment: &'static str,
    },
    Close {
        is_stop_loss: bool,
        reason: &'static str,
    },
}

fn role_comment(role: FillRole) -> &'static str {
    match role {
        FillRole::Open { comment, .. }
        | FillRole::Close {
            reason: comment, ..
        } => comment,
    }
}

impl Engine {
    pub fn new(
        config: EngineConfig,
        strategy: Box<dyn Strategy>,
        client: BacktestOrderClient,
        data_base: impl AsRef<Path>,
        out_base: impl AsRef<Path>,
    ) -> Self {
        let feed = Feed::new(strategy.timeframes(), config.max_closed_len);
        // 回测确定性：Store 时钟由 K 线时间驱动（created_at/updated_at 可逐字节
        // 复现，禁止 Utc::now）；关闭周期 flush，只在回测结束落一次当日快照。
        let clock = Arc::new(CandleClock::new());
        let state_base = out_base.as_ref().join("state");
        let store = Arc::new(Store::new(
            &state_base,
            Arc::clone(&clock) as Arc<dyn Clock>,
        ));
        // 仓位当前态（position/stop/cooldown/history_position）全部进统一 Store；
        // 对外 history_positions 固定列 CSV 仍以 data_base 为根（生产 = state/）。
        let positions = PositionManager::new(
            &config.strategy_name,
            &config.symbol,
            &config.trading_mode,
            Arc::clone(&store),
            data_base,
        );
        let signals = SignalManager::new(
            &config.strategy_name,
            Arc::clone(&store),
            Some(Box::new(client.clone())),
        )
        .with_flush_every(usize::MAX);
        let signal_writer = SignalWriter::new(out_base.as_ref().join("signals"));
        let risk = RiskController::new(config.risk.clone());
        // S5：成交/净值表在构造时即注册（即使本轮回测无成交/无净值喂入，
        // flush 也会落表头文件，两次回放产物清单逐字节一致）。
        let trades = store.table::<TradeRow>();
        let equity = EquitySink::new(&config.strategy_name, &store);
        Self {
            config,
            feed,
            strategy,
            positions,
            client,
            signals,
            store,
            clock,
            signal_writer,
            risk,
            cooldown: None,
            stats: RunStats::default(),
            trades,
            equity,
            trade_seq: 0,
            last_equity_time: None,
        }
    }

    /// 启动恢复：Store recover 当前持仓/止损单/冷却日/历史仓
    pub fn restore(&mut self) -> std::io::Result<()> {
        self.positions.restore()?;
        self.cooldown = self.positions.stop_loss_cooldown();
        Ok(())
    }

    /// 预载 warmup（1m 已闭合序列，不触发任何事件）
    pub fn preload(&mut self, warmup_1m: &[Candle]) {
        self.feed.preload(warmup_1m);
    }

    /// 驱动一根已闭合 1m K 线（回测回放入口）
    pub fn on_closed_1m(&mut self, candle: Candle) -> std::io::Result<()> {
        self.stats.bars += 1;
        let event = self.feed.on_closed_1m(candle);
        let c1 = event.candle_1m;
        self.dispatch(c1)
    }

    /// 记录一次 mark-to-market 权益快照（回测主循环每根 1m 收盘调用；
    /// 同一时间戳幂等）。日/周期桶末值由 equity 表同 id upsert 落行。
    pub fn on_equity_mark(&mut self, candle: &Candle) {
        if self.last_equity_time == Some(candle.open_time) {
            return;
        }
        self.last_equity_time = Some(candle.open_time);
        let Some(acc) = self.client.account() else {
            return; // 实盘客户端无快照 → 不追踪
        };
        let (equity, cash) = match self.positions.position() {
            Some(p) if acc.position_qty != 0.0 => {
                // 浮动盈亏按数量与标记价（1m close）计算
                let pnl = match (acc.is_long, p.side) {
                    (true, strategy_positions::Side::Long) => {
                        (candle.close - acc.entry_price) * acc.position_qty
                    }
                    (false, strategy_positions::Side::Short) => {
                        (acc.entry_price - candle.close) * acc.position_qty
                    }
                    _ => 0.0,
                };
                (acc.cash + pnl, acc.cash)
            }
            _ => (acc.cash, acc.cash),
        };
        let tf_minutes = strategy_core::candle::Timeframe::parse(&self.config.signal_tf)
            .map(|tf| tf.0)
            .unwrap_or(60);
        // equity 表 upsert：Store 时钟在本根 dispatch 前置为 open_time，
        // created_at/updated_at 全部来自 K 线时间（确定性红线）。
        self.equity
            .on_1m_close(candle.open_time, equity, cash, tf_minutes);
    }

    /// 已收集成交（Store trade 表按本实例过滤，序号升序）。
    pub fn trade_rows(&self) -> Vec<TradeRow> {
        trade_rows(&self.trades, &self.config.strategy_name)
    }

    /// 日净值点（Store equity 表 kind=daily，桶起点升序）。
    pub fn daily_equity(&self) -> Vec<EquityPoint> {
        self.equity.daily()
    }

    /// 信号周期净值点（kind=tf，桶起点升序）。
    pub fn tf_equity(&self) -> Vec<EquityPoint> {
        self.equity.tf()
    }

    /// 共用分发：构造 position_view + Ctx → 策略 on_candle/on_realtime_tick → apply_action。
    /// 调用前 candle 已经 `Feed::on_closed_1m` 喂入（见 [`Engine::on_closed_1m`]）。
    /// 每次调用：① 先 positions.on_price 更新 peak/pnl 极值（Ctx 看到最新仓位）；
    /// ② 策略返回 UpdateStop → apply_action 落盘 stop_price；
    /// ③ Ctx 注入的 stop_price 始终为最新落盘值。
    fn dispatch(&mut self, candle: Candle) -> std::io::Result<()> {
        let event_time = candle.open_time;
        let price = candle.close;

        // Store 时钟锚定本根 K 线：信号行 created/updated 时间戳确定性可复现
        self.clock.set(event_time);

        // ① 先更新仓位 peak/pnl 极值（持仓中每根 1m 同步，Ctx 看到最新 peak）
        self.positions.on_price(candle.high, candle.low)?;

        // 仓位状态 → 策略视图（positions::Side → core::Side 转换）
        // stop_price 取最新落盘值（经上次 UpdateStop 已同步）
        let position_view = self.positions.position().map(|p| PositionView {
            side: match p.side {
                strategy_positions::Side::Long => Side::Long,
                strategy_positions::Side::Short => Side::Short,
            },
            entry_price: p.entry_price,
            stop_price: p.stop_price,
            peak_price: p.peak_price,
            max_pnl_pct: p.max_pnl_pct,
            min_pnl_pct: p.min_pnl_pct,
            exit_active: p.exit_active,
            exit_price: p.exit_price,
        });

        // ② 上下文（注入 current_candle 完整 OHLC + 最新 stop_price）
        let source = FeedSource { feed: &self.feed };
        let ctx = Ctx::new(
            &source,
            price,
            candle,
            event_time,
            position_view,
            self.cooldown.clone(),
        );

        // 分发：有持仓 → 出场逻辑（策略特有优先，统一风控兜底，镜像 Python _check_exit）；
        // 空仓 → 入场逻辑
        let actions = if position_view.is_some() {
            let mut actions = self.strategy.on_realtime_tick(&ctx);
            if actions.is_empty() {
                if let Some(exit) = self.check_risk_fallback(&candle) {
                    actions.push(Action::Close {
                        price,
                        reason: exit.reason,
                        is_stop_loss: exit.is_stop_loss,
                        strength: 0.8,
                    });
                }
            }
            actions
        } else {
            self.strategy.on_candle(&ctx)
        };

        // ③ 执行动作（UpdateStop → positions.update_stop 落盘；
        //   Open/Close/Reverse → broker + 仓位生命周期）
        for action in actions {
            self.apply_action(action, event_time)?;
        }
        Ok(())
    }

    /// 统一风控兜底（镜像 Python `RiskController.check_exit`）：
    /// 检测价用 current close（不用 bar high/low —— 那是策略特有层的事）；
    /// 回落止盈状态就地更新到 PositionState 并落盘。
    fn check_risk_fallback(&mut self, candle: &Candle) -> Option<strategy_core::risk::ExitSignal> {
        let p = self.positions.position()?;
        let pnl_pct = p.pnl_pct(candle.close);
        let mut rs = strategy_core::risk::RiskState {
            trail_activated: p.trail_activated,
            trail_trigger_pct: p.trail_trigger_pct,
        };
        let exit = self.risk.check_exit(pnl_pct, p.max_pnl_pct, &mut rs)?;
        // 同步回落止盈状态回仓位记录（trail_activated 变化才落盘）
        if p.trail_activated != rs.trail_activated || p.trail_trigger_pct != rs.trail_trigger_pct {
            let _ = self
                .positions
                .update_trail_state(rs.trail_activated, rs.trail_trigger_pct);
        }
        Some(exit)
    }

    /// 策略动作 → 交易意图管道（设计文档 §5.2 职责链）：
    /// Action → SignalIntent(s) → SignalManager（落盘+验证+提交）
    /// → 回测即时成交 FillEvent → 仓位管理（开/加/减/平）。
    fn apply_action(&mut self, action: Action, event_time: DateTime<Utc>) -> std::io::Result<()> {
        if let Action::UpdateStop { stop_price } = action {
            self.positions.update_stop(stop_price)?;
            return Ok(());
        }
        if let Action::UpdateExit {
            exit_active,
            exit_price,
        } = action
        {
            self.positions.update_exit(exit_active, exit_price)?;
            return Ok(());
        }
        if matches!(action, Action::Hold) {
            return Ok(());
        }

        let held = self.positions.position().map(|p| {
            let side = match p.side {
                strategy_positions::Side::Long => Side::Long,
                strategy_positions::Side::Short => Side::Short,
            };
            // 可平量与 raw 口径同 live（回测 broker 仍按持仓量撮合，size 仅语义对齐）
            let size = if p.remaining_size > 0.0 {
                p.remaining_size
            } else {
                p.total_size
            };
            (p.position_id.clone(), side, size, p.entry_size_raw.clone())
        });

        let ictx = IntentCtx {
            strategy_type: &self.config.strategy_type,
            symbol: &self.config.symbol,
            event_time,
            position_id: held.as_ref().map(|(id, ..)| id.as_str()),
            position_side: held.as_ref().map(|(_, s, ..)| *s),
            position_size: held.as_ref().map(|(.., s, _)| *s),
            position_size_raw: held.as_ref().and_then(|(.., r)| r.as_deref()),
        };
        let intents = intents_from_action(&action, &ictx);
        let mut by_id: std::collections::HashMap<String, SignalIntent> =
            intents.into_iter().map(|i| (i.id.clone(), i)).collect();

        // 由 Action 确定每条意图的对外信号类型与成交角色（与旧直调路径一一对应）
        let mut jobs: Vec<(SignalType, FillRole)> = Vec::new();
        match &action {
            Action::Open {
                side,
                stop_price,
                atr,
                ..
            } => {
                let st = match side {
                    Side::Long => SignalType::Buy,
                    Side::Short => SignalType::Sell,
                };
                jobs.push((
                    st,
                    FillRole::Open {
                        stop_price: *stop_price,
                        atr: *atr,
                        comment: "open",
                    },
                ));
            }
            Action::Close { is_stop_loss, .. } => {
                // 无持仓时 intents_from_action 返回空（旧代码同：直接返回不写信号）
                let Some((_, held_side, _, _)) = &held else {
                    return Ok(());
                };
                let st = match held_side {
                    Side::Long => SignalType::SellClose,
                    Side::Short => SignalType::BuyClose,
                };
                jobs.push((
                    st,
                    FillRole::Close {
                        is_stop_loss: *is_stop_loss,
                        reason: "close",
                    },
                ));
            }
            Action::Reverse {
                side: new_side,
                stop_price,
                atr,
                is_stop_loss,
                ..
            } => {
                // 先平（仅在有持仓时——旧路径空仓反手不写平仓信号），后开
                if let Some((_, held_side, _, _)) = &held {
                    let close_type = match held_side {
                        Side::Long => SignalType::SellClose,
                        Side::Short => SignalType::BuyClose,
                    };
                    jobs.push((
                        close_type,
                        FillRole::Close {
                            is_stop_loss: *is_stop_loss,
                            reason: "reverse",
                        },
                    ));
                }
                let open_type = match new_side {
                    Side::Long => SignalType::Buy,
                    Side::Short => SignalType::Sell,
                };
                jobs.push((
                    open_type,
                    FillRole::Open {
                        stop_price: *stop_price,
                        atr: *atr,
                        comment: "reverse",
                    },
                ));
            }
            Action::Hold | Action::UpdateStop { .. } | Action::UpdateExit { .. } => {}
        }

        for (st, role) in jobs {
            // 意图 ID 是 (策略, symbol, K线时间, 信号类型) 的确定性哈希
            let id = generate_signal_id(
                &self.config.strategy_type,
                &self.config.symbol,
                event_time,
                st,
            );
            let Some(intent) = by_id.remove(&id) else {
                // 反手空仓时没有平仓意图（intents 只生成 1 条开仓），跳过该 job
                continue;
            };

            // 1) 对外 signal.csv（列序契约；意图产生即写，Rejected/Failed 不另加行）
            self.write_signal(&intent.id, st, intent.price, event_time)?;
            self.stats.signals_written += 1;

            // 2) 意图管道：Created 落盘 → 验证 → 网关提交 →（回测）即时成交
            let fills = self.signals.receive_intent(intent)?;
            self.stats.orders_written += fills.len();

            for fill in fills {
                let pnl = match role {
                    FillRole::Close { .. } => self.close_pnl(&fill),
                    FillRole::Open { .. } => 0.0,
                };
                self.record_trade(&fill, st, event_time, role_comment(role), pnl)?;

                match role {
                    FillRole::Open {
                        stop_price, atr, ..
                    } => {
                        self.positions
                            .apply_open_fill(&fill, event_time, stop_price, atr)?;
                        self.stats.positions_opened += 1;
                    }
                    FillRole::Close {
                        is_stop_loss,
                        reason,
                    } => {
                        self.positions
                            .apply_close_fill(&fill, is_stop_loss, reason)?;
                        if is_stop_loss {
                            // close() 已写当日冷却（bar_end 日历日判定，见 Ctx）
                            self.cooldown = self.positions.stop_loss_cooldown();
                        }
                        self.stats.positions_closed += 1;
                    }
                }
            }
        }
        Ok(())
    }

    /// 平仓盈亏（成交前持仓均价 × 成交量；与旧 broker Fill.pnl 同公式）
    fn close_pnl(&self, fill: &FillEvent) -> f64 {
        let Some(p) = self.positions.position() else {
            return 0.0;
        };
        let side = match p.side {
            strategy_positions::Side::Long => Side::Long,
            strategy_positions::Side::Short => Side::Short,
        };
        let held = if p.remaining_size > 0.0 {
            p.remaining_size
        } else {
            p.total_size
        };
        let qty = fill.filled_size.min(held);
        strategy_core::realized_pnl(side, p.entry_price, qty, fill.filled_price)
    }

    /// 成交 → Store `trade` 行（开仓 pnl=0，平仓带 pnl；只增不改）。
    /// 行 id=trade_{signal_id}：信号 id 确定性 → 回放逐字节一致。
    fn record_trade(
        &mut self,
        fill: &FillEvent,
        signal_type: SignalType,
        event_time: DateTime<Utc>,
        comment: &str,
        pnl: f64,
    ) -> std::io::Result<()> {
        let side = match signal_type {
            SignalType::Buy | SignalType::ReverseLong => "BUY",
            SignalType::Sell | SignalType::Flat => "SELL",
            SignalType::SellClose | SignalType::ReverseShort => "SELL_CLOSE",
            SignalType::BuyClose => "BUY_CLOSE",
        };
        self.trade_seq += 1;
        let row = TradeRow::new(
            self.trade_seq,
            &self.config.strategy_name,
            &self.config.strategy_type,
            &self.config.symbol,
            side,
            fill.filled_size,
            fill.filled_price,
            fill.fee,
            0.0,
            pnl,
            &event_time.format("%Y-%m-%dT%H:%M:%S").to_string(),
            comment,
            &fill.signal_id,
        );
        self.trades
            .insert(row)
            .map_err(|e| std::io::Error::other(format!("trade 行写入失败: {e}")))
    }

    fn write_signal(
        &self,
        signal_id: &str,
        signal_type: SignalType,
        trigger_price: f64,
        event_time: DateTime<Utc>,
    ) -> std::io::Result<()> {
        let side = match signal_type {
            SignalType::Buy | SignalType::ReverseLong => CsvSide::Buy,
            _ => CsvSide::Sell,
        };
        let rec = SignalRecord {
            strategy_name: self.config.strategy_name.clone(),
            signal_id: signal_id.to_string(),
            timestamp: event_time,
            user_id: self.config.user_id,
            signal_action: signal_type.to_string(),
            symbol: self.config.symbol.clone(),
            pos_type: PosType::FUTURES,
            cash: self.config.cash,
            leverage: self.config.leverage,
            side,
            exchange: self.config.exchange.clone(),
            trigger_price,
            order_type: OrderType::MARKET,
            quantity: 0.0,
        };
        self.signal_writer.write(&rec)
    }

    /// 回测结束：把全部已注册 model（signal/position/stop/cooldown/
    /// history_position/trade/equity）flush 到当日快照（时钟停在最后一根
    /// K 线，落盘目录/时间戳确定性）。回测主循环同步调用（非 live 阻塞池
    /// 纪律范围），全程只此一个 flush 点。
    pub fn flush_state(&self) -> std::io::Result<()> {
        self.store.flush().map_err(std::io::Error::other)
    }

    pub fn stats(&self) -> &RunStats {
        &self.stats
    }

    pub fn positions(&self) -> &PositionManager {
        &self.positions
    }
}

/// Feed 的 ClosedSource 适配器
struct FeedSource<'a> {
    feed: &'a Feed,
}

impl<'a> strategy_core::strategy::ClosedSource for FeedSource<'a> {
    fn closed(&self, tf: strategy_core::candle::Timeframe) -> Option<&[Candle]> {
        self.feed.closed_map().get(&tf).map(|v| v.as_slice())
    }
    fn last_price(&self) -> Option<f64> {
        self.feed.last_price()
    }
}
