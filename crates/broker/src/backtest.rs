//! BacktestBroker — 模拟撮合（回测/paper 共用）
//!
//! 语义（对齐 Python binance_trader + backtest 桥接）：
//! - 市价单按 触发价±滑点 即时成交（buy 偏贵、sell 偏便宜 = 不利方向）
//! - 反手 = 先平后开；平仓失败放弃开仓
//! - 每次成交写 exchange_orders.csv（status=FILLED，合成订单号 bt_{signal_id}）
//! - 未配置合约（crypto）：手续费按名义金额比例计（fee_rate），数量可小数
//! - 配置合约（国内期货）：整数手、保证金占用、开/平/平今手续费、合约乘数

use chrono::{DateTime, NaiveDate, Utc};
use strategy_core::contract::{ctp_trading_day, ContractSpec, FeeKind};
use strategy_core::signal::Side;

use crate::Action;

use crate::order_log::{ExchangeOrderRecord, ExchangeOrderWriter, OrderStatus, PosType, SideCode};
use crate::{AccountSnapshot, Broker};

#[derive(Debug, Clone)]
pub struct BacktestConfig {
    /// 初始资金（quote 币）
    pub cash: f64,
    /// 单边手续费率（0.0004 = 4bps；仅未配置合约的 crypto 模式生效）
    pub fee_rate: f64,
    /// 单边滑点比例
    pub slippage: f64,
    /// 杠杆（名义敞口上限：crypto = 现金×杠杆；期货 = min(权益×杠杆, 保证金允许上限)）
    pub leverage: u32,
    /// 每单名义金额（strategy_cash）；**0 = 全仓**（每次开仓用全部可用现金×杠杆，
    /// 回测语义：不按 per-symbol cash 分仓，直接满仓进出）
    pub notional_per_order: f64,
    /// 国内期货合约参数；None = crypto/兼容模式（小数数量 + fee_rate）
    pub contract: Option<ContractSpec>,
    pub exchange: String,
    pub pos_type: PosType,
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            cash: 5000.0,
            fee_rate: 0.0004,
            slippage: 0.0,
            leverage: 1,
            notional_per_order: 100.0,
            contract: None,
            exchange: "binance".into(),
            pos_type: PosType::Futures,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Fill {
    pub signal_id: String,
    pub side: SideCode,
    pub fill_price: f64,
    pub quantity: f64,
    pub fee: f64,
    /// 本笔成交盈亏（开仓=0；平仓由 broker 按开仓价计算，供 trade model 行）
    pub pnl: f64,
}

/// 回测成交结果：一次 Action 可能产生 1-2 笔成交（反手 = 平+开）
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExecResult {
    pub fills: Vec<Fill>,
}

/// 回测 broker：撮合 + 回执落盘
pub struct BacktestBroker {
    pub config: BacktestConfig,
    pub cash: f64,
    pub position: Option<Position>,
    order_writer: ExchangeOrderWriter,
    strategy_name: String,
}

impl Broker for BacktestBroker {
    fn execute(
        &mut self,
        signal_id: &str,
        action: &Action,
        update_time: chrono::DateTime<chrono::Utc>,
    ) -> std::io::Result<ExecResult> {
        BacktestBroker::execute(self, signal_id, action, update_time)
    }

    fn account(&self) -> Option<AccountSnapshot> {
        Some(match &self.position {
            None => AccountSnapshot {
                cash: self.cash,
                position_qty: 0.0,
                entry_price: 0.0,
                is_long: true,
            },
            Some(p) => AccountSnapshot {
                cash: self.cash,
                position_qty: p.quantity,
                entry_price: p.entry_price,
                is_long: p.side == Side::Long,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub side: Side,
    pub entry_price: f64,
    /// 数量：合约模式 = 整数手数；crypto 模式 = 标的数量
    pub quantity: f64,
    pub signal_id: String,
    /// 开仓时的 CTP 交易日（平今手续费判定；crypto 模式仅作记录）
    pub open_trading_day: NaiveDate,
}

impl BacktestBroker {
    pub fn new(
        config: BacktestConfig,
        strategy_name: impl Into<String>,
        out_base: impl AsRef<std::path::Path>,
    ) -> Self {
        let cash = config.cash;
        Self {
            config,
            cash,
            position: None,
            order_writer: ExchangeOrderWriter::new(out_base),
            strategy_name: strategy_name.into(),
        }
    }

    /// 成交价：buy 用 ask（触发价上滑），sell 用 bid（触发价下滑）
    fn fill_price(&self, side: SideCode, trigger: f64) -> f64 {
        match side {
            SideCode::Buy => trigger * (1.0 + self.config.slippage),
            SideCode::Sell => trigger * (1.0 - self.config.slippage),
        }
    }

    /// 执行策略动作（引擎唯一入口）。
    /// `signal_id` 来自引擎按 K 线时间生成的确定性 ID。
    pub fn execute(
        &mut self,
        signal_id: &str,
        action: &Action,
        update_time: chrono::DateTime<chrono::Utc>,
    ) -> std::io::Result<ExecResult> {
        let mut out = ExecResult::default();
        match action {
            Action::Hold => {}
            Action::UpdateStop { .. } => {} // 纯状态更新，无成交
            Action::UpdateExit { .. } => {} // 持仓子状态回写，无成交
            Action::Open { side, price, .. } => {
                if self.position.is_none() {
                    let side_code = long_to_code(*side);
                    if let Some(f) =
                        self.open_position(signal_id, *side, *price, side_code, update_time)?
                    {
                        out.fills.push(f);
                    }
                }
            }
            Action::Close { price, .. } => {
                if let Some(pos) = self.position.take() {
                    let side_code = long_to_code(pos.side);
                    let fill =
                        self.close_position(&pos, signal_id, *price, side_code, update_time)?;
                    out.fills.push(fill);
                }
            }
            Action::Reverse { side, price, .. } => {
                // 反手：先平后开（对齐 Python：平仓失败放弃开仓）
                if let Some(pos) = self.position.take() {
                    let close_code = long_to_code(pos.side);
                    let fill =
                        self.close_position(&pos, signal_id, *price, close_code, update_time)?;
                    out.fills.push(fill);
                }
                if self.position.is_none() {
                    let side_code = long_to_code(*side);
                    if let Some(f) =
                        self.open_position(signal_id, *side, *price, side_code, update_time)?
                    {
                        out.fills.push(f);
                    }
                }
            }
        }
        Ok(out)
    }

    fn open_position(
        &mut self,
        signal_id: &str,
        side: Side,
        trigger: f64,
        side_code: SideCode,
        t: DateTime<Utc>,
    ) -> std::io::Result<Option<Fill>> {
        let fp = self.fill_price(side_code, trigger);
        if trigger <= 0.0 || fp <= 0.0 {
            return Ok(None);
        }

        // 期货模式：整数手 + 保证金约束 + 合约手续费
        if let Some(c) = self.config.contract.clone() {
            let Some(lots) = c.sizing_lots(fp, self.cash, self.config.leverage) else {
                let per_lot_margin = fp * c.multiplier * c.margin_rate;
                tracing::warn!(
                    symbol = %symbol_of(&self.strategy_name),
                    cash = self.cash,
                    price = fp,
                    per_lot_margin,
                    "保证金不足，放弃开仓（需要至少 {} 手）",
                    c.min_lots.max(1)
                );
                return Ok(None);
            };
            let notional = fp * c.multiplier * lots as f64;
            let fee = c.commission.fee(notional, lots as f64, FeeKind::Open);
            self.cash -= fee;
            self.position = Some(Position {
                side,
                entry_price: fp,
                quantity: lots as f64,
                signal_id: signal_id.into(),
                open_trading_day: ctp_trading_day(t),
            });
            self.log_order(
                signal_id,
                side_code,
                trigger,
                fp,
                lots as f64,
                OrderStatus::Filled,
                t,
            )?;
            return Ok(Some(Fill {
                signal_id: signal_id.into(),
                side: side_code,
                fill_price: fp,
                quantity: lots as f64,
                fee,
                pnl: 0.0,
            }));
        }

        // crypto 兼容模式：维持原有全仓/每单名义金额语义
        let max_notional = self.cash.max(0.0) * self.config.leverage as f64;
        // notional_per_order <= 0 = 全仓模式（回测）：每次用全部可用现金×杠杆
        let notional = if self.config.notional_per_order <= 0.0 {
            max_notional
        } else {
            self.config.notional_per_order.min(max_notional)
        };
        if notional <= 0.0 {
            return Ok(None);
        }
        let qty = notional / fp;
        let fee = notional * self.config.fee_rate;
        self.cash -= fee;
        self.position = Some(Position {
            side,
            entry_price: fp,
            quantity: qty,
            signal_id: signal_id.into(),
            open_trading_day: ctp_trading_day(t),
        });

        self.log_order(
            signal_id,
            side_code,
            trigger,
            fp,
            qty,
            OrderStatus::Filled,
            t,
        )?;
        Ok(Some(Fill {
            signal_id: signal_id.into(),
            side: side_code,
            fill_price: fp,
            quantity: qty,
            fee,
            pnl: 0.0,
        }))
    }

    fn close_position(
        &mut self,
        pos: &Position,
        signal_id: &str,
        trigger: f64,
        close_side: SideCode,
        t: DateTime<Utc>,
    ) -> std::io::Result<Fill> {
        let fp = self.fill_price(close_side, trigger);
        let (fee, pnl) = if let Some(c) = self.config.contract.as_ref() {
            // 期货：按平仓成交金额计费；同一 CTP 交易日平仓 = 平今
            let notional = fp * c.multiplier * pos.quantity;
            let close_today = pos.open_trading_day == ctp_trading_day(t);
            let kind = if close_today {
                FeeKind::CloseToday
            } else {
                FeeKind::Close
            };
            let fee = c.commission.fee(notional, pos.quantity, kind);
            let diff = match pos.side {
                Side::Long => fp - pos.entry_price,
                Side::Short => pos.entry_price - fp,
            };
            (fee, diff * pos.quantity * c.multiplier)
        } else {
            let notional = pos.entry_price * pos.quantity; // 兼容口径：按开仓名义
            let fee = notional * self.config.fee_rate;
            let pnl = match pos.side {
                Side::Long => (fp - pos.entry_price) * pos.quantity,
                Side::Short => (pos.entry_price - fp) * pos.quantity,
            };
            (fee, pnl)
        };
        self.cash += pnl - fee;

        self.log_order(
            signal_id,
            close_side,
            trigger,
            fp,
            pos.quantity,
            OrderStatus::Filled,
            t,
        )?;
        Ok(Fill {
            signal_id: signal_id.into(),
            side: close_side,
            fill_price: fp,
            quantity: pos.quantity,
            fee,
            pnl,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn log_order(
        &self,
        signal_id: &str,
        side: SideCode,
        trigger: f64,
        fill_price: f64,
        qty: f64,
        status: OrderStatus,
        t: chrono::DateTime<chrono::Utc>,
    ) -> std::io::Result<()> {
        self.order_writer.write(&ExchangeOrderRecord {
            strategy_name: self.strategy_name.clone(),
            signal_id: signal_id.into(),
            symbol: symbol_of(&self.strategy_name),
            exchange: self.config.exchange.clone(),
            leverage: self.config.leverage,
            pos_type: self.config.pos_type,
            price: trigger,
            side,
            exchange_order_id: format!("bt_{signal_id}"),
            status,
            exchange_order_price: fill_price,
            exchange_order_quantity: qty,
            exchange_update_time: t,
        })
    }
}

fn long_to_code(side: Side) -> SideCode {
    match side {
        Side::Long => SideCode::Buy,
        Side::Short => SideCode::Sell,
    }
}

/// 从 strategy_name 提取 symbol（命名约定 {prefix}_{tf}_{ver}_{SYMBOL}_{MODE}）。
/// 提取失败返回占位符（不影响撮合，只影响回执列）。
fn symbol_of(strategy_name: &str) -> String {
    strategy_name
        .split('_')
        .rev()
        .nth(1)
        .unwrap_or("UNKNOWN")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn cfg() -> BacktestConfig {
        BacktestConfig {
            cash: 1000.0,
            leverage: 1,
            notional_per_order: 100.0,
            ..Default::default()
        }
    }

    fn broker(dir: &std::path::Path) -> BacktestBroker {
        BacktestBroker::new(cfg(), "sar_8H_3_BTCUSDT_BACKTEST", dir)
    }

    fn t() -> chrono::DateTime<chrono::Utc> {
        chrono::Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
    }

    #[test]
    fn open_then_close_long_pnl() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = broker(dir.path());

        let open = Action::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 98.0,
            atr: 1.0,
            strength: 0.8,
            reason: "test".into(),
        };
        let r = b.execute("sig_open", &open, t()).unwrap();
        assert_eq!(r.fills.len(), 1);
        assert!(b.position.is_some());
        assert!(b.cash < 1000.0); // 扣手续费

        let close = Action::Close {
            price: 110.0,
            reason: "trailing".into(),
            is_stop_loss: false,
            strength: 0.8,
        };
        let r = b.execute("sig_close", &close, t()).unwrap();
        assert_eq!(r.fills.len(), 1);
        assert!(b.position.is_none());
        assert!(b.cash > 1000.0); // 盈利

        // 回执文件（逐行精确断言）
        let path = dir
            .path()
            .join("sar_8H_3_BTCUSDT_BACKTEST/exchange_orders.csv");
        let content = std::fs::read_to_string(path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3); // header + 2 笔成交
        assert!(lines[0].starts_with("signal_id,symbol"));
        assert!(lines[1].starts_with("sig_open,BTCUSDT,binance,1,2,100,1,bt_sig_open,FILLED,"));
        assert!(lines[2].starts_with("sig_close,BTCUSDT,binance,1,2,110,1,bt_sig_close,FILLED,"));
    }

    #[test]
    fn reverse_closes_then_opens() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = broker(dir.path());
        let open_long = Action::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 98.0,
            atr: 1.0,
            strength: 0.8,
            reason: "r".into(),
        };
        b.execute("sig_1", &open_long, t()).unwrap();

        let rev = Action::Reverse {
            side: Side::Short,
            price: 105.0,
            stop_price: 107.0,
            atr: 1.0,
            strength: 0.8,
            close_reason: "reverse".into(),
            is_stop_loss: false,
        };
        let r = b.execute("sig_2", &rev, t()).unwrap();
        assert_eq!(r.fills.len(), 2); // 平多 + 开空
        assert_eq!(b.position.as_ref().unwrap().side, Side::Short);
    }

    #[test]
    fn open_ignored_when_position_exists() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = broker(dir.path());
        let open_long = Action::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 98.0,
            atr: 1.0,
            strength: 0.8,
            reason: "r".into(),
        };
        b.execute("sig_1", &open_long, t()).unwrap();
        let cash_after = b.cash;

        // 已持仓时再次 Open 被忽略
        b.execute("sig_2", &open_long, t()).unwrap();
        assert_eq!(b.cash, cash_after);
    }

    #[test]
    fn slippage_applied_unfavorably() {
        let mut cfg = cfg();
        cfg.slippage = 0.001;
        let dir = tempfile::tempdir().unwrap();
        let mut b = BacktestBroker::new(cfg, "s_BTCUSDT_BT", dir.path());
        let open_long = Action::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 98.0,
            atr: 0.0,
            strength: 0.8,
            reason: "r".into(),
        };
        let r = b.execute("sig_s", &open_long, t()).unwrap();
        assert!((r.fills[0].fill_price - 100.1).abs() < 1e-9); // 买贵
    }

    /// 期货合约：整数手、保证金约束、乘数盈亏、开/平/平今手续费
    fn futures_cfg() -> BacktestConfig {
        use strategy_core::contract::{CommissionSpec, FeeMode};
        BacktestConfig {
            cash: 10_000.0,
            contract: Some(ContractSpec {
                name: Some("测试期货".into()),
                multiplier: 5.0,
                tick_size: 1.0,
                margin_rate: 0.1,
                min_lots: 1,
                session_opens: vec!["09:00".into(), "21:00".into()],
                no_trade_after_open_minutes: 5,
                session_closes: vec!["15:00".into(), "01:00".into()],
                no_trade_before_close_minutes: 5,
                commission: CommissionSpec {
                    mode: FeeMode::Rate,
                    open: 0.0001,
                    close: 0.0001,
                    close_today: Some(0.0002),
                },
            }),
            ..Default::default()
        }
    }

    #[test]
    fn futures_open_is_integer_lots_and_charges_margin_aware_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = BacktestBroker::new(futures_cfg(), "x_CU_BT", dir.path());
        let open = Action::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 98.0,
            atr: 1.0,
            strength: 0.8,
            reason: "r".into(),
        };
        let r = b.execute("sig_open", &open, t()).unwrap();
        assert_eq!(r.fills.len(), 1);
        // leverage=1：每手名义 100×5=500，10000 权益最多 20 手
        assert_eq!(r.fills[0].quantity, 20.0);
        assert!(r.fills[0].quantity.fract() == 0.0, "期货手数必须为整数");
        // 开仓手续费 = 100×5×20 × 0.0001 = 1.0
        assert!((r.fills[0].fee - 1.0).abs() < 1e-9);
        assert!((b.cash - 9_999.0).abs() < 1e-9);
    }

    #[test]
    fn futures_margin_binds_when_leverage_is_high() {
        let mut cfg = futures_cfg();
        cfg.leverage = 10;
        let dir = tempfile::tempdir().unwrap();
        let mut b = BacktestBroker::new(cfg, "x_CU_BT", dir.path());
        let open = Action::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 98.0,
            atr: 1.0,
            strength: 0.8,
            reason: "r".into(),
        };
        let r = b.execute("sig_open", &open, t()).unwrap();
        // 保证金上限：floor(10000 / (50 + 0.05)) = 199 手
        assert_eq!(r.fills[0].quantity, 199.0);
        assert!(r.fills[0].quantity.fract() == 0.0);
    }

    #[test]
    fn futures_insufficient_margin_skips_open() {
        let mut cfg = futures_cfg();
        cfg.cash = 49.0; // 不够 1 手保证金（50）+ 手续费
        let dir = tempfile::tempdir().unwrap();
        let mut b = BacktestBroker::new(cfg, "x_CU_BT", dir.path());
        let open = Action::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 98.0,
            atr: 1.0,
            strength: 0.8,
            reason: "r".into(),
        };
        let r = b.execute("sig_open", &open, t()).unwrap();
        assert!(r.fills.is_empty());
        assert!(b.position.is_none());
        assert!((b.cash - 49.0).abs() < 1e-9);
    }

    #[test]
    fn futures_pnl_uses_multiplier_and_close_today_fee() {
        let dir = tempfile::tempdir().unwrap();
        let mut b = BacktestBroker::new(futures_cfg(), "x_CU_BT", dir.path());
        let open = Action::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 98.0,
            atr: 1.0,
            strength: 0.8,
            reason: "r".into(),
        };
        let r = b.execute("sig_open", &open, t()).unwrap();
        let lots = r.fills[0].quantity;
        // 同日平仓 → 平今费率；盈亏 = (110-100)×5×手数
        let close = Action::Close {
            price: 110.0,
            reason: "close".into(),
            is_stop_loss: false,
            strength: 0.8,
        };
        let r = b.execute("sig_close", &close, t()).unwrap();
        let f = &r.fills[0];
        assert!((f.pnl - 10.0 * 5.0 * lots).abs() < 1e-9);
        let expected_fee = 110.0 * 5.0 * lots * 0.0002;
        assert!((f.fee - expected_fee).abs() < 1e-9);
    }
}
