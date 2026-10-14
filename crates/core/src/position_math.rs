//! 仓位加/减仓纯计算（设计文档 §5.5）
//!
//! 无 IO、无状态，供 PositionManager 单写者调用与单元测试。

use crate::signal::Side;

/// 同向加仓后的持仓均价：
/// `new_avg = (old_avg*old_size + fill_price*add_size) / (old_size+add_size)`
pub fn weighted_avg(old_size: f64, old_avg: f64, add_size: f64, fill_price: f64) -> f64 {
    let total = old_size + add_size;
    if total <= 0.0 {
        return 0.0;
    }
    (old_avg * old_size + fill_price * add_size) / total
}

/// 反向减仓的已实现盈亏（不含手续费）：
/// long: `(exit-entry)*size`；short: `(entry-exit)*size`
pub fn realized_pnl(side: Side, entry_avg: f64, reduce_size: f64, exit_price: f64) -> f64 {
    let diff = match side {
        Side::Long => exit_price - entry_avg,
        Side::Short => entry_avg - exit_price,
    };
    diff * reduce_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_position_recomputes_average() {
        // 100@100 + 100@110 → 均价 105
        let avg = weighted_avg(100.0, 100.0, 100.0, 110.0);
        assert!((avg - 105.0).abs() < 1e-9);
        // 不等量
        let avg = weighted_avg(10.0, 100.0, 30.0, 200.0);
        assert!((avg - 175.0).abs() < 1e-9);
    }

    #[test]
    fn zero_total_returns_zero() {
        assert_eq!(weighted_avg(0.0, 0.0, 0.0, 100.0), 0.0);
    }

    #[test]
    fn long_reduce_pnl() {
        assert!((realized_pnl(Side::Long, 100.0, 2.0, 105.0) - 10.0).abs() < 1e-9);
        assert!((realized_pnl(Side::Long, 100.0, 2.0, 90.0) + 20.0).abs() < 1e-9);
    }

    #[test]
    fn short_reduce_pnl() {
        assert!((realized_pnl(Side::Short, 100.0, 2.0, 95.0) - 10.0).abs() < 1e-9);
        assert!((realized_pnl(Side::Short, 100.0, 2.0, 110.0) + 20.0).abs() < 1e-9);
    }
}
