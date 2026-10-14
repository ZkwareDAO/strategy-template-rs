//! 信号模型 — 与 Python 版 `signal_logging/storage.py` 对齐

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

/// 信号类型（与 Python SignalType 枚举一一对应）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignalType {
    Buy,
    Sell,
    Flat,
    BuyClose,
    SellClose,
    ReverseLong,
    ReverseShort,
}

impl SignalType {
    pub fn as_str(&self) -> &'static str {
        match self {
            SignalType::Buy => "buy",
            SignalType::Sell => "sell",
            SignalType::Flat => "flat",
            SignalType::BuyClose => "buy_close",
            SignalType::SellClose => "sell_close",
            SignalType::ReverseLong => "reverse_long",
            SignalType::ReverseShort => "reverse_short",
        }
    }
}

impl std::fmt::Display for SignalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for SignalType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "buy" => SignalType::Buy,
            "sell" => SignalType::Sell,
            "flat" => SignalType::Flat,
            "buy_close" => SignalType::BuyClose,
            "sell_close" => SignalType::SellClose,
            "reverse_long" => SignalType::ReverseLong,
            "reverse_short" => SignalType::ReverseShort,
            other => return Err(Error::InvalidSignalType(other.to_string())),
        })
    }
}

/// 策略动作（Strategy::on_candle / on_realtime_tick 的返回值；由引擎翻译成信号）
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// 开仓（price=触发价=主周期已闭合收盘价；stop_price=初始止损；atr=入场ATR）
    Open {
        side: Side,
        price: f64,
        stop_price: f64,
        atr: f64,
        strength: f64,
        reason: String,
    },
    /// 平仓（price=平仓触发价）
    Close {
        price: f64,
        reason: String,
        is_stop_loss: bool,
        strength: f64,
    },
    /// 反手 = 先平后开（平仓失败则放弃开仓，由 broker/引擎保证）
    Reverse {
        side: Side,
        price: f64,
        stop_price: f64,
        atr: f64,
        strength: f64,
        close_reason: String,
        is_stop_loss: bool,
    },
    /// 移动止损到新价位（SAR 跟踪止损等；只往有利方向由策略保证）
    UpdateStop {
        stop_price: f64,
    },
    /// 回写 SAR 触及制离场状态到持仓行（引擎直接更新 position，不产生订单）
    UpdateExit {
        exit_active: bool,
        exit_price: f64,
    },
    Hold,
}

impl Action {
    pub fn is_hold(&self) -> bool {
        matches!(self, Action::Hold)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Long,
    Short,
}

/// 交易信号
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signal {
    /// 确定性 signal_id（同数据同 ID，回测/实盘一致）
    pub signal_id: String,
    pub strategy_id: String,
    pub signal_type: SignalType,
    pub symbol: String,
    /// 信号价格（触发价）
    pub price: f64,
    /// K 线时间（红线：禁止 now()）
    pub timestamp: DateTime<Utc>,
    pub volume: f64,
    pub strength: f64,
    pub direction: Option<Side>,
}

/// 生成确定性 signal_id — 与 Python `generate_signal_id` 逐字节一致
///
/// key = `{strategy_type.lower()}_{symbol.upper()}_{1m时间戳ms}_{signal_type.lower()}`
/// ID = `sig_` + SHA256(key) 前 16 hex
pub fn generate_signal_id(
    strategy_type: &str,
    symbol: &str,
    kline_timestamp: DateTime<Utc>,
    signal_type: SignalType,
) -> String {
    let ts_ms = kline_timestamp.timestamp_millis();
    let key = format!(
        "{}_{}_{}_{}",
        strategy_type.to_lowercase(),
        symbol.to_uppercase(),
        ts_ms,
        signal_type.as_str()
    );
    let hash = Sha256::digest(key.as_bytes());
    let hex: String = hash[..8].iter().map(|b| format!("{b:02x}")).collect();
    format!("sig_{hex}")
}

// ─── 交易意图（SignalIntent）───────────────────────────────────────────────
//
// 与上面 [`Signal`]（对外 signal.csv 行）的关系：
// - [`Action`]：策略每根 K 线的决策
// - [`SignalIntent`]：决策进入交易管道后的"意图记录"，带全生命周期状态
//   （[`crate::lifecycle::SignalStatus`]），由信号管理 Actor 持有并持久化
// - [`Signal`]：对外 signal.csv 的报表行（列序契约锁定，见设计文档 D6）

use crate::lifecycle::SignalStatus;

/// 意图类型（决定订单方向语义与仓位处理）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentKind {
    /// 开仓
    Open,
    /// 平仓
    Close,
    /// 反手 = 先平后开（产出两条意图，平仓在前）
    Reverse,
}

/// 进入信号管道的交易意图（设计文档 §5.2 / 附录 A）
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalIntent {
    /// 确定性信号 ID（沿用 generate_signal_id）
    pub id: String,
    /// 策略类型名（进入 ID 哈希，如 sar_snt3）
    pub strategy_id: String,
    pub symbol: String,
    pub kind: IntentKind,
    /// 下单方向（开仓=新仓方向；平仓=与持仓相反方向）
    pub direction: Side,
    /// 信号触发价
    pub price: f64,
    /// 委托量（开仓恒 0；平仓 = 持仓成交量）
    pub size: f64,
    /// 数量原始字面量（开仓成交回报 filled_quantity 原样保存；平仓帧优先
    /// 使用它，避免 f64 往返修改精确数量）
    #[serde(default)]
    pub size_raw: Option<String>,
    /// 开仓附带的初始止损价（仅 Open 有意义）
    pub stop_price: Option<f64>,
    /// 关联仓位（平仓/止损信号携带）
    pub position_id: Option<String>,
    /// 触发本平仓意图的止盈止损单（止损信号携带）
    pub stop_order_id: Option<String>,
    /// 产生时间（K 线时间，禁止 now()）
    pub created_event_time: DateTime<Utc>,
    /// 当前生命周期状态（初值 Created）
    pub status: SignalStatus,
}

/// Action → SignalIntent 转换上下文
#[derive(Debug, Clone, Copy)]
pub struct IntentCtx<'a> {
    pub strategy_type: &'a str,
    pub symbol: &'a str,
    pub event_time: DateTime<Utc>,
    /// 当前持仓 ID（平仓/反手时关联）
    pub position_id: Option<&'a str>,
    /// 当前持仓方向（决定平仓单方向；反手且无持仓时用新方向反向推导）
    pub position_side: Option<Side>,
    /// 当前持仓可平量（remaining_size 优先，与 apply_close_fill held 同口径）
    pub position_size: Option<f64>,
    /// 当前持仓可平量的原始字面量（开仓 FILLED 回报原样保存）
    pub position_size_raw: Option<&'a str>,
}

impl<'a> IntentCtx<'a> {
    pub fn new(strategy_type: &'a str, symbol: &'a str, event_time: DateTime<Utc>) -> Self {
        Self {
            strategy_type,
            symbol,
            event_time,
            position_id: None,
            position_side: None,
            position_size: None,
            position_size_raw: None,
        }
    }
}

fn opposite(side: Side) -> Side {
    match side {
        Side::Long => Side::Short,
        Side::Short => Side::Long,
    }
}

#[allow(clippy::too_many_arguments)]
fn new_intent(
    ctx: &IntentCtx<'_>,
    kind: IntentKind,
    direction: Side,
    signal_type: SignalType,
    price: f64,
    stop_price: Option<f64>,
    size: f64,
    size_raw: Option<&str>,
    link_position: bool,
    stop_order_id: Option<&str>,
) -> SignalIntent {
    SignalIntent {
        id: generate_signal_id(ctx.strategy_type, ctx.symbol, ctx.event_time, signal_type),
        strategy_id: ctx.strategy_type.to_string(),
        symbol: ctx.symbol.to_string(),
        kind,
        direction,
        price,
        size,
        size_raw: size_raw.map(String::from),
        stop_price,
        position_id: link_position
            .then(|| ctx.position_id.map(String::from))
            .flatten(),
        stop_order_id: stop_order_id.map(String::from),
        created_event_time: ctx.event_time,
        status: SignalStatus::Created,
    }
}

/// 策略 Action → 0..=2 条交易意图（按管道顺序：平仓永远在开仓前）。
///
/// - [`Action::Hold`] / [`Action::UpdateStop`] / [`Action::UpdateExit`]：无下单
///   意图（UpdateStop/UpdateExit 由引擎直接更新持仓行，不经过订单管道）
/// - [`Action::Open`]：1 条
/// - [`Action::Close`]：1 条（方向由持仓方向反向得出）
/// - [`Action::Reverse`]：2 条（先平后开）
pub fn intents_from_action(action: &Action, ctx: &IntentCtx<'_>) -> Vec<SignalIntent> {
    match action {
        Action::Hold | Action::UpdateStop { .. } | Action::UpdateExit { .. } => Vec::new(),
        Action::Open {
            side,
            price,
            stop_price,
            ..
        } => {
            let st = match side {
                Side::Long => SignalType::Buy,
                Side::Short => SignalType::Sell,
            };
            vec![new_intent(
                ctx,
                IntentKind::Open,
                *side,
                st,
                *price,
                Some(*stop_price),
                0.0,
                None,
                false,
                None,
            )]
        }
        Action::Close {
            price, is_stop_loss, ..
        } => {
            let Some(held) = ctx.position_side else {
                return Vec::new(); // 无持仓的平仓意图无意义
            };
            let st = match held {
                Side::Long => SignalType::SellClose,
                Side::Short => SignalType::BuyClose,
            };
            let stop_order_id = is_stop_loss
                .then(|| ctx.position_id)
                .flatten()
                .map(|pid| format!("stop_{pid}_sl"));
            vec![new_intent(
                ctx,
                IntentKind::Close,
                opposite(held),
                st,
                *price,
                None,
                ctx.position_size.unwrap_or(0.0),
                ctx.position_size_raw,
                true,
                stop_order_id.as_deref(),
            )]
        }
        Action::Reverse {
            side,
            price,
            stop_price,
            is_stop_loss,
            ..
        } => {
            // 先平：优先用实际持仓方向，无持仓信息时按新方向反向推导
            let held = ctx.position_side.unwrap_or_else(|| opposite(*side));
            let close_type = match held {
                Side::Long => SignalType::SellClose,
                Side::Short => SignalType::BuyClose,
            };
            let open_type = match side {
                Side::Long => SignalType::Buy,
                Side::Short => SignalType::Sell,
            };
            let stop_order_id = is_stop_loss
                .then(|| ctx.position_id)
                .flatten()
                .map(|pid| format!("stop_{pid}_sl"));
            vec![
                new_intent(
                    ctx,
                    IntentKind::Reverse,
                    opposite(held),
                    close_type,
                    *price,
                    None,
                    ctx.position_size.unwrap_or(0.0),
                    ctx.position_size_raw,
                    true,
                    stop_order_id.as_deref(),
                ),
                new_intent(
                    ctx,
                    IntentKind::Reverse,
                    *side,
                    open_type,
                    *price,
                    Some(*stop_price),
                    0.0,
                    None,
                    false,
                    None,
                ),
            ]
        }
    }
}

#[cfg(test)]
mod intent_tests {
    use super::*;
    use chrono::TimeZone;

    fn ctx() -> IntentCtx<'static> {
        // IntentCtx 只借用 &'static str，时间为 Copy
        IntentCtx {
            strategy_type: "sar_snt3",
            symbol: "BTCUSDT",
            event_time: Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap(),
            position_id: None,
            position_side: None,
            position_size: None,
            position_size_raw: None,
        }
    }

    #[test]
    fn hold_and_update_stop_produce_no_intents() {
        let c = ctx();
        assert!(intents_from_action(&Action::Hold, &c).is_empty());
        assert!(intents_from_action(&Action::UpdateStop { stop_price: 99.0 }, &c).is_empty());
    }

    #[test]
    fn update_exit_produces_no_intents() {
        // exit 状态是持仓子状态，由引擎直接回写 position 行，不产生订单意图
        let c = ctx();
        assert!(intents_from_action(
            &Action::UpdateExit {
                exit_active: true,
                exit_price: 99.0
            },
            &c
        )
        .is_empty());
    }

    #[test]
    fn open_long_intent_uses_buy_id_and_carries_stop() {
        let c = ctx();
        let action = Action::Open {
            side: Side::Long,
            price: 100.0,
            stop_price: 98.0,
            atr: 1.0,
            strength: 0.8,
            reason: "r".into(),
        };
        let intents = intents_from_action(&action, &c);
        assert_eq!(intents.len(), 1);
        let i = &intents[0];
        assert_eq!(i.kind, IntentKind::Open);
        assert_eq!(i.direction, Side::Long);
        assert_eq!(i.stop_price, Some(98.0));
        assert_eq!(i.status, SignalStatus::Created);
        assert_eq!(
            i.id,
            generate_signal_id("sar_snt3", "BTCUSDT", c.event_time, SignalType::Buy)
        );
    }

    #[test]
    fn close_long_sells_and_links_position() {
        let mut c = ctx();
        c.position_side = Some(Side::Long);
        c.position_id = Some("sig_open");
        let action = Action::Close {
            price: 110.0,
            reason: "tp".into(),
            is_stop_loss: false,
            strength: 1.0,
        };
        let intents = intents_from_action(&action, &c);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].direction, Side::Short);
        assert_eq!(intents[0].kind, IntentKind::Close);
        assert_eq!(intents[0].position_id.as_deref(), Some("sig_open"));
    }

    #[test]
    fn close_without_position_is_dropped() {
        let c = ctx();
        let action = Action::Close {
            price: 110.0,
            reason: "tp".into(),
            is_stop_loss: false,
            strength: 1.0,
        };
        assert!(intents_from_action(&action, &c).is_empty());
    }

    #[test]
    fn reverse_is_close_then_open() {
        let mut c = ctx();
        c.position_side = Some(Side::Long);
        c.position_id = Some("sig_open");
        let action = Action::Reverse {
            side: Side::Short,
            price: 105.0,
            stop_price: 107.0,
            atr: 1.0,
            strength: 0.9,
            close_reason: "flip".into(),
            is_stop_loss: false,
        };
        let intents = intents_from_action(&action, &c);
        assert_eq!(intents.len(), 2);
        assert_eq!(intents[0].direction, Side::Short, "先平多");
        assert_eq!(intents[0].position_id.as_deref(), Some("sig_open"));
        assert_eq!(intents[1].direction, Side::Short, "再开空");
        assert_eq!(intents[1].stop_price, Some(107.0));
        assert_ne!(intents[0].id, intents[1].id, "平/开信号 ID 不同");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    /// 与 Python 版对同一输入产出相同 ID（黄金值在集成时用 Python 侧复核）
    #[test]
    fn deterministic_and_stable() {
        let ts = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
        let id1 = generate_signal_id("sar_snt3", "BTCUSDT", ts, SignalType::Buy);
        let id2 = generate_signal_id("sar_snt3", "BTCUSDT", ts, SignalType::Buy);
        assert_eq!(id1, id2);
        assert_eq!(id1.len(), 20); // "sig_" + 16 hex
        assert!(id1.starts_with("sig_"));

        // 大小写不敏感：strategy_type 小写化、symbol 大写化后等价
        let id3 = generate_signal_id("SAR_SNT3", "btcusdt", ts, SignalType::Buy);
        assert_eq!(id1, id3);

        // 不同输入 → 不同 ID
        let id4 = generate_signal_id("sar_snt3", "BTCUSDT", ts, SignalType::Sell);
        assert_ne!(id1, id4);
    }

    #[test]
    fn signal_type_roundtrip() {
        for s in [
            "buy",
            "sell",
            "flat",
            "buy_close",
            "sell_close",
            "reverse_long",
            "reverse_short",
        ] {
            assert_eq!(s.parse::<SignalType>().unwrap().as_str(), s);
        }
        assert!("bad".parse::<SignalType>().is_err());
    }
}

#[cfg(test)]
mod golden_tests {
    use super::*;
    use chrono::TimeZone;

    /// 跨语言黄金值：Python `generate_signal_id("sar_snt3","BTCUSDT",
    /// datetime(2026,6,10,tzinfo=utc),"buy")` → sig_9c1b75861b10144f
    /// key = "sar_snt3_BTCUSDT_1781049600000_buy"
    #[test]
    fn python_golden_value() {
        let ts = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
        let id = generate_signal_id("sar_snt3", "BTCUSDT", ts, SignalType::Buy);
        assert_eq!(id, "sig_9c1b75861b10144f");
    }
}
