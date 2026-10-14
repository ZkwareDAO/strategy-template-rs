//! 仓位状态（镜像 Python BaseState 的仓位相关字段）

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 持仓方向
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Long,
    Short,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Long => "long",
            Side::Short => "short",
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 当前仓位状态（统一 Store `position` 行的 payload）
///
/// 红线对齐：禁止缓存字段进入持久化（这里全部是有状态恢复语义的字段）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionState {
    pub position_id: String,
    pub symbol: String,
    pub side: Side,
    pub entry_price: f64,
    /// 开仓时间（K 线时间，RFC3339）
    pub entry_time: DateTime<Utc>,
    /// 开仓时间戳（秒）
    pub entry_timestamp: i64,
    /// 持仓期间极值：long→最高价，short→最低价
    pub peak_price: f64,
    /// 止损价（策略设定，随行情更新）
    pub stop_price: f64,
    /// 最大盈利百分比（>0）
    pub max_pnl_pct: f64,
    /// 最大亏损百分比（<0）
    pub min_pnl_pct: f64,
    /// 入场时 ATR（历史仓位记录用）
    #[serde(default)]
    pub atr_at_entry: f64,
    /// 总开仓量（含同向加仓；Phase 5 起由 ApplyFill 维护，当前过渡阶段为 0）
    #[serde(default)]
    pub total_size: f64,
    /// 剩余可平量（减仓/部分成交扣减；Phase 5 起维护）
    #[serde(default)]
    pub remaining_size: f64,
    /// 累计已实现盈亏（减仓时累加；Phase 5 起维护）
    #[serde(default)]
    pub realized_pnl: f64,
    /// 移动止盈是否已激活
    #[serde(default)]
    pub trail_activated: bool,
    /// 回落止盈触发百分比（最大盈利 × (1 - drawdown/100)）
    #[serde(default)]
    pub trail_trigger_pct: f64,
    /// 开仓成交量原始字面量（FILLED 回报 filled_quantity 原样保存；
    /// 平仓帧 quantity 以此为准，避免 f64 往返改变精确数量）
    #[serde(default)]
    pub entry_size_raw: Option<String>,
    /// 剩余可平量原始字面量：开仓时 = entry_size_raw，每次平仓成交按回报
    /// filled_quantity 精确（Decimal）递减。**平仓帧 quantity 的真正来源**
    /// （entry_size_raw 是开仓量，不随平仓变化）。
    #[serde(default)]
    pub remaining_size_raw: Option<String>,
    /// SAR 触及制离场状态：true = 已挂出离场价（重启后继续保护）
    #[serde(default)]
    pub exit_active: bool,
    /// SAR 离场价（exit_active=true 有效；多头在上方/空头在下方）
    #[serde(default)]
    pub exit_price: f64,
}

impl PositionState {
    /// 未实现盈亏百分比（long: (p-e)/e；short: (e-p)/e）
    pub fn pnl_pct(&self, price: f64) -> f64 {
        if self.entry_price <= 0.0 {
            return 0.0;
        }
        let diff = match self.side {
            Side::Long => price - self.entry_price,
            Side::Short => self.entry_price - price,
        };
        diff / self.entry_price * 100.0
    }

    /// 行情更新：跟踪峰值与盈亏极值（不落盘，由 manager 决定持久化时机）。
    /// 传入 1m K 线的 high/low：
    /// - 峰值 = 有利端极值（long→最高价 high，short→最低价 low）
    /// - max_pnl_pct = 有利端价的最优盈亏（long→high，short→low）
    /// - min_pnl_pct = 不利端价的最差盈亏（long→low，short→high）
    pub fn on_price(&mut self, high: f64, low: f64) {
        match self.side {
            Side::Long => {
                if high > self.peak_price {
                    self.peak_price = high;
                }
            }
            Side::Short => {
                // peak_price 初始化为 entry_price（见 open/apply_open_fill），
                // 任何有效 low 都会小于 entry_price，因此无需额外哨兵值。
                if low < self.peak_price {
                    self.peak_price = low;
                }
            }
        }
        // 盈亏极值：最优取有利端，最差取不利端（单根 1m 内双端都检查）
        let (best, worst) = match self.side {
            Side::Long => (high, low),
            Side::Short => (low, high),
        };
        let pnl_best = self.pnl_pct(best);
        if pnl_best > self.max_pnl_pct {
            self.max_pnl_pct = pnl_best;
        }
        let pnl_worst = self.pnl_pct(worst);
        if pnl_worst < self.min_pnl_pct {
            self.min_pnl_pct = pnl_worst;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn long_state(entry: f64) -> PositionState {
        PositionState {
            position_id: "sig_x".into(),
            symbol: "BTCUSDT".into(),
            side: Side::Long,
            entry_price: entry,
            entry_time: chrono::Utc::now(),
            entry_timestamp: 0,
            peak_price: entry,
            stop_price: entry * 0.98,
            max_pnl_pct: 0.0,
            min_pnl_pct: 0.0,
            atr_at_entry: 0.0,
            total_size: 0.0,
            remaining_size: 0.0,
            realized_pnl: 0.0,
            trail_activated: false,
            trail_trigger_pct: 0.0,
            entry_size_raw: None,
            remaining_size_raw: None,
            exit_active: false,
            exit_price: 0.0,
        }
    }

    #[test]
    fn exit_fields_default_when_missing_in_old_snapshot() {
        // 旧快照 JSON（无 exit_active/exit_price）须可恢复：默认 false/0.0
        let json = serde_json::json!({
            "position_id": "sig_x", "symbol": "BTCUSDT", "side": "Long",
            "entry_price": 100.0,
            "entry_time": "2026-06-10T00:00:00Z", "entry_timestamp": 0,
            "peak_price": 100.0, "stop_price": 98.0,
            "max_pnl_pct": 0.0, "min_pnl_pct": 0.0
        });
        let s: PositionState = serde_json::from_value(json).unwrap();
        assert!(!s.exit_active, "旧快照 exit_active 默认 false");
        assert_eq!(s.exit_price, 0.0, "旧快照 exit_price 默认 0");

        // 往返：新字段进入持久化
        let mut s = s;
        s.exit_active = true;
        s.exit_price = 103.5;
        let back: PositionState =
            serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert!(back.exit_active);
        assert_eq!(back.exit_price, 103.5);
    }

    #[test]
    fn long_price_tracking() {
        let mut s = long_state(100.0);
        // on_price(high, low)：long 峰值取 high，盈亏极值用 high
        s.on_price(102.0, 99.0); // high=102, low=99 → peak=102, pnl_max=+2%
        s.on_price(105.0, 101.0); // high=105 → peak=105, pnl_max=+5%；low=101 → pnl_min=-1%?
        assert_eq!(s.peak_price, 105.0);
        assert!((s.max_pnl_pct - 5.0).abs() < 1e-9);
        // long 的 min_pnl 用 low 端：第一根 low=99 → -1%
        assert!((s.min_pnl_pct + 1.0).abs() < 1e-9);
    }

    #[test]
    fn short_price_tracking() {
        let mut s = long_state(100.0);
        s.side = Side::Short;
        s.peak_price = 100.0;
        s.stop_price = 102.0;
        // short：峰值=low，盈亏极值用 low
        s.on_price(101.0, 98.0); // low=98 → peak=98, pnl_max=+2%
        s.on_price(99.0, 97.0); // low=97 → pnl_max=+3%
        assert_eq!(s.peak_price, 97.0); // short 峰值 = 最低价
        assert!((s.max_pnl_pct - 3.0).abs() < 1e-9); // 97 时 +3%
    }
}
