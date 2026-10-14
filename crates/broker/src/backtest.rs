//! BacktestBroker — 模拟撮合（回测/paper 共用）
//!
//! 语义（对齐 Python binance_trader + backtest 桥接）：
//! - 市价单按 触发价±滑点 即时成交（buy 偏贵、sell 偏便宜 = 不利方向）
//! - 反手 = 先平后开；平仓失败放弃开仓
//! - 每次成交写 exchange_orders.csv（status=FILLED，合成订单号 bt_{signal_id}）
//! - 手续费按名义金额比例计（fee_rate）

use strategy_core::signal::{Action, Side};

use crate::order_log::{ExchangeOrderRecord, ExchangeOrderWriter, OrderStatus, PosType, SideCode};
use crate::{AccountSnapshot, Broker};

#[derive(Debug, Clone)]
pub struct BacktestConfig {
    /// 初始资金（quote 币）
    pub cash: f64,
    /// 单边手续费率（0.0004 = 4bps）
    pub fee_rate: f64,
    /// 单边滑点比例
    pub slippage: f64,
    /// 杠杆
    pub leverage: u32,
    /// 每单名义金额（strategy_cash）；**0 = 全仓**（每次开仓用全部可用现金×杠杆，
    /// 回测语义：不按 per-symbol cash 分仓，直接满仓进出）
    pub notional_per_order: f64,
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
    pub quantity: f64,
    pub signal_id: String,
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
        t: chrono::DateTime<chrono::Utc>,
    ) -> std::io::Result<Option<Fill>> {
        let max_notional = self.cash.max(0.0) * self.config.leverage as f64;
        // notional_per_order <= 0 = 全仓模式（回测）：每次用全部可用现金×杠杆
        let notional = if self.config.notional_per_order <= 0.0 {
            max_notional
        } else {
            self.config.notional_per_order.min(max_notional)
        };
        if notional <= 0.0 || trigger <= 0.0 {
            return Ok(None);
        }
        let fp = self.fill_price(side_code, trigger);
        let qty = notional / fp;
        let fee = notional * self.config.fee_rate;
        self.cash -= fee;
        self.position = Some(Position {
            side,
            entry_price: fp,
            quantity: qty,
            signal_id: signal_id.into(),
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
        t: chrono::DateTime<chrono::Utc>,
    ) -> std::io::Result<Fill> {
        let fp = self.fill_price(close_side, trigger);
        let notional = pos.entry_price * pos.quantity; // 按开仓名义
        let fee = notional * self.config.fee_rate;
        let pnl = match pos.side {
            Side::Long => (fp - pos.entry_price) * pos.quantity,
            Side::Short => (pos.entry_price - fp) * pos.quantity,
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
}
