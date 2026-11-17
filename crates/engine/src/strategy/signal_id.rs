//! 策略运行时 `sub_` 确定性信号 ID（PR-1）。
//!
//! `sub_` 是独立命名空间：与 [`strategy_core::signal::generate_signal_id`]
//! 的 `sig_` 前缀互不竞争，旧信号链路零回归（G4）。

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use strategy_core::signal::Side;

/// 子信号意图（进入 `sub_` 哈希）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubIntent {
    Open,
    RebalanceReduce,
    RiskCloseAll,
    SubStop,
    ExitRule,
}

impl SubIntent {
    pub fn as_str(&self) -> &'static str {
        match self {
            SubIntent::Open => "open",
            SubIntent::RebalanceReduce => "rebalance_reduce",
            SubIntent::RiskCloseAll => "risk_close_all",
            SubIntent::SubStop => "sub_stop",
            SubIntent::ExitRule => "exit_rule",
        }
    }
}

/// 生成确定性 `sub_` 信号 ID（`SubPosition.id` 开仓行键；PR-1）。
///
/// key = `format!("{}|{}|{}|{}|{}|{}|{}", strategy_type.to_lowercase(),
/// strategy_id, venue_key.to_uppercase(), ts_ms, side_str,
/// intent.as_str(), rebalance_epoch)`，其中 `side_str` = `"long"|"short"`，
/// `ts_ms = aligned_bar_time.timestamp_millis()`。
/// ID = `"sub_"` + SHA256(key) 前 8 字节 hex（长度 20）。
///
/// `sub_` 独立命名空间（G4）：`strategy_core::signal::generate_signal_id` 的
/// `sig_` 链路逐字节不动；回测/实盘同输入同 ID。
pub fn generate_sub_signal_id(
    strategy_type: &str,
    strategy_id: &str,
    venue_key: &str,
    aligned_bar_time: DateTime<Utc>,
    side: Side,
    intent: SubIntent,
    rebalance_epoch: u64,
) -> String {
    let ts_ms = aligned_bar_time.timestamp_millis();
    let side_str = match side {
        Side::Long => "long",
        Side::Short => "short",
    };
    let key = format!(
        "{}|{}|{}|{}|{}|{}|{}",
        strategy_type.to_lowercase(),
        strategy_id,
        venue_key.to_uppercase(),
        ts_ms,
        side_str,
        intent.as_str(),
        rebalance_epoch
    );
    let hash = Sha256::digest(key.as_bytes());
    let hex: String = hash[..8].iter().map(|b| format!("{b:02x}")).collect();
    format!("sub_{hex}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn deterministic_and_namespaced() {
        let t = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
        let id = generate_sub_signal_id(
            "xsec_momentum",
            "xsec_momentum:default",
            "binance/crypto_perpetual/BTCUSDT",
            t,
            Side::Long,
            SubIntent::Open,
            0,
        );
        assert_eq!(
            id, "sub_b526b953d8bc66f0",
            "黄金值：key=xsec_momentum|xsec_momentum:default|BINANCE/CRYPTO_PERPETUAL/BTCUSDT|1781049600000|long|open|0"
        );
        assert!(id.starts_with("sub_") && id.len() == 20);
        assert_ne!(
            id,
            generate_sub_signal_id(
                "xsec_momentum",
                "xsec_momentum:default",
                "okx/crypto_perpetual/BTCUSDT",
                t,
                Side::Long,
                SubIntent::Open,
                0
            )
        );
        assert_ne!(
            id,
            generate_sub_signal_id(
                "xsec_momentum",
                "xsec_momentum:default",
                "binance/crypto_perpetual/BTCUSDT",
                t,
                Side::Long,
                SubIntent::ExitRule,
                0
            )
        );
    }

    #[test]
    fn sig_namespace_untouched() {
        let t = Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap();
        assert_eq!(
            strategy_core::signal::generate_signal_id(
                "sar_snt3",
                "BTCUSDT",
                t,
                strategy_core::signal::SignalType::Buy
            ),
            "sig_9c1b75861b10144f"
        );
    }
}
