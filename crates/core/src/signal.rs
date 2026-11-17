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

/// 生成确定性 signal_id（Store `signal` 行键；与 Python `generate_signal_id`
/// 同输入同输出）
///
/// key = `{strategy_type.lower()}_{venue_key.upper()}_{1m时间戳ms}_{signal_type.lower()}`
/// ID = `sig_` + SHA256(key) 前 16 hex
///
/// **阶段 5c（v2.9，§13）调用方约定**：`venue_key` 位置喂命名空间键
/// （`{exchange}/{market}/{symbol}`，`namespaced_key_of` 单一格式），不再喂
/// 裸 symbol——跨交易所同名标的不再共享行键（§14-3 验收）。函数对任意
/// 字符串的行为逐字节未动，故 Python 黄金值在函数级契约层面继续成立；
/// 跨语言对齐要求 Python 侧按同一约定喂键（登记于 §13 迁移策略注记）。
pub fn generate_signal_id(
    strategy_type: &str,
    venue_key: &str,
    kline_timestamp: DateTime<Utc>,
    signal_type: SignalType,
) -> String {
    let ts_ms = kline_timestamp.timestamp_millis();
    let key = format!(
        "{}_{}_{}_{}",
        strategy_type.to_lowercase(),
        venue_key.to_uppercase(),
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
    /// 开平偏移（阶段 4 v2.7，§13-4「实盘订单帧带平今/平昨」）：
    /// `"open" | "close" | "close_today" | "close_yesterday"`。
    /// **过渡口径（§13 v2 迁移策略）**：字符串形态、serde default，
    /// 信号意图默认 None——服务端维持既有隐式开平
    /// 判定，行为零回归；SPI live 桥（`exchanges::live`）由
    /// `OrderRequest.offset` 填入，`trading_service` 出帧带 `offset` 字段。
    /// 阶段 5 与 exchange/market 一起类型化（全链路 `InstrumentId`）。
    #[serde(default)]
    pub offset: Option<String>,
}

#[cfg(test)]
mod id_namespace_tests {
    use super::*;
    use chrono::TimeZone;

    /// §14-3 行键命名空间（阶段 5c v2.9）：同一裸 symbol、同策略、同 K 线
    /// 时刻，跨交易所必须得到不同行键；各自确定性不变（幂等键）；且不得
    /// 等于迁移前裸 symbol 哈希——旧格式行与新行在共享表零碰撞。
    #[test]
    fn venue_key_namespaces_signal_id_across_exchanges() {
        let ts = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
        let a = generate_signal_id(
            "sar_snt3",
            "binance/crypto_perpetual/BTCUSDT",
            ts,
            SignalType::Buy,
        );
        let b = generate_signal_id(
            "sar_snt3",
            "bybit/crypto_perpetual/BTCUSDT",
            ts,
            SignalType::Buy,
        );
        assert_ne!(a, b, "跨交易所同名 symbol 不得共享行键");
        assert_eq!(
            a,
            generate_signal_id(
                "sar_snt3",
                "binance/crypto_perpetual/BTCUSDT",
                ts,
                SignalType::Buy
            ),
            "确定性（幂等重提）未破坏"
        );
        assert_ne!(
            a,
            generate_signal_id("sar_snt3", "BTCUSDT", ts, SignalType::Buy),
            "回归钉：新口径不得复现迁移前的裸 symbol 行键"
        );
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
