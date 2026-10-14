//! efficiency_ratio — Kaufman 效率比（震荡检测）
//!
//! ER = |close[n-1] − close[n-1-period]| / Σ|close[i] − close[i-1]|
//!
//! - ER → 1：单边趋势（净位移 ≈ 路径总长）
//! - ER → 0：纯震荡（价格来回磨，净位移 ≈ 0）
//!
//! 无平滑、当根可见（对比 ADX 的 Wilder 平滑滞后），无量纲，
//! 适合作为"该不该参与行情"的主开关；方向性交给趋势指标。

/// 计算最近一根 K 线的 Kaufman 效率比。
/// `period`: 回看窗口（含当前根共 period+1 个 close 参与计算）。
/// 数据不足（len < period+1）→ Err；路径总长为 0（全横盘）→ 0。
pub fn efficiency_ratio(close: &[f64], period: usize) -> Result<f64, String> {
    if period == 0 {
        return Err("period 不能为 0".into());
    }
    if close.len() < period + 1 {
        return Err(format!(
            "数据不足: 需要 {} 根, 实际 {} 根",
            period + 1,
            close.len()
        ));
    }
    let n = close.len();
    let net = (close[n - 1] - close[n - 1 - period]).abs();
    // 路径：从 n-1-period 到 n-1 共 period 个相邻差，需 period+1 个点
    let path: f64 = close[n - 1 - period..]
        .windows(2)
        .map(|w| (w[1] - w[0]).abs())
        .sum();
    if path == 0.0 {
        // 全横盘：0/0 无定义；横盘显然不是趋势 → 0
        return Ok(0.0);
    }
    Ok(net / path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perfect_trend_is_one() {
        // 单边上涨：每根 +1，净位移 = 路径总长 → ER = 1
        let c: Vec<f64> = (0..20).map(|i| 100.0 + i as f64).collect();
        let er = efficiency_ratio(&c, 10).unwrap();
        assert!((er - 1.0).abs() < 1e-9);
    }

    #[test]
    fn pure_chop_is_zero() {
        // 来回磨：+1 −1 交替，窗口末净位移 = 0 → ER = 0
        let c: Vec<f64> = (0..20)
            .map(|i| 100.0 + if i % 2 == 0 { 0.0 } else { 1.0 })
            .collect();
        let er = efficiency_ratio(&c, 10).unwrap();
        assert!(er.abs() < 1e-9);
    }

    #[test]
    fn flat_series_is_zero() {
        // 全横盘：路径总长 0 → 0（非趋势）
        let c = vec![100.0; 20];
        let er = efficiency_ratio(&c, 10).unwrap();
        assert!(er.abs() < 1e-9);
    }

    #[test]
    fn partial_trend_is_fractional() {
        // 净位移 2.5，路径 7.5 → ER = 1/3
        let c: Vec<f64> = vec![
            100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 104.0, 103.0, 102.5,
        ];
        let er = efficiency_ratio(&c, 8).unwrap();
        assert!((er - 2.5 / 7.5).abs() < 1e-9);
    }

    #[test]
    fn insufficient_data_is_err() {
        let c = vec![100.0, 101.0];
        assert!(efficiency_ratio(&c, 10).is_err());
    }

    #[test]
    fn zero_period_is_err() {
        let c = vec![100.0; 5];
        assert!(efficiency_ratio(&c, 0).is_err());
    }

    #[test]
    fn uses_only_last_window() {
        // 窗口外（早期）的剧烈波动不影响结果
        let mut c = vec![50.0; 5]; // 早期杂讯
        c.extend((0..11).map(|i| 100.0 + i as f64)); // 末 11 根单边
        let er = efficiency_ratio(&c, 10).unwrap();
        assert!((er - 1.0).abs() < 1e-9, "ER 应只看最后 period 根");
    }
}
