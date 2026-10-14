//! talib-rs 函数薄封装
//!
//! 统一约定：
//! - 输入切片按时间升序
//! - 输出与输入等长，前 lookback 个值为 NaN（TA-Lib 语义）
//! - 错误统一为指标错误字符串（由 talib-rs TaError 转换）

use talib_rs as ta;

/// EMA 指数移动平均（lookback = period - 1）
pub fn ema(input: &[f64], period: usize) -> Result<Vec<f64>, String> {
    ta::overlap::ema(input, period).map_err(|e| e.to_string())
}

/// SMA 简单移动平均（lookback = period - 1）
pub fn sma(input: &[f64], period: usize) -> Result<Vec<f64>, String> {
    ta::overlap::sma(input, period).map_err(|e| e.to_string())
}

/// RSI（Wilder 平滑，lookback = period）
pub fn rsi(input: &[f64], period: usize) -> Result<Vec<f64>, String> {
    ta::momentum::rsi(input, period).map_err(|e| e.to_string())
}

/// SAR 抛物线（默认 acceleration=0.02, maximum=0.2，lookback = 1）
pub fn sar(high: &[f64], low: &[f64], acceleration: f64, maximum: f64) -> Result<Vec<f64>, String> {
    ta::overlap::sar(high, low, acceleration, maximum).map_err(|e| e.to_string())
}

/// ATR（Wilder 平滑，lookback = period）
pub fn atr(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>, String> {
    ta::volatility::atr(high, low, close, period).map_err(|e| e.to_string())
}

/// ADX（lookback = 2 * period - 1）
pub fn adx(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>, String> {
    ta::momentum::adx(high, low, close, period).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// EMA 数值语义（与 TA-Lib 一致：seed=SMA，k=2/(n+1)）
    #[test]
    fn ema_semantics() {
        let input = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        let out = ema(&input, 3).unwrap();
        assert_eq!(out.len(), input.len());
        // lookback=2：前两个 NaN
        assert!(out[0].is_nan() && out[1].is_nan());
        // TA-Lib 语义：seed=SMA(1,2,3)=2，随后递推 prev = k*(x-prev)+prev
        // out[2]=2, out[3]=2+0.5*(4-2)=3
        // 线性序列每步 +1、k=0.5 → EMA 恒落后 1：out[i] = input[i-1]
        assert!((out[2] - 2.0).abs() < 1e-9);
        assert!((out[3] - 3.0).abs() < 1e-9);
        assert!((out[9] - 9.0).abs() < 1e-9);
    }

    #[test]
    fn rsi_all_gains_is_100() {
        let input: Vec<f64> = (1..=20).map(|i| i as f64).collect();
        let out = rsi(&input, 14).unwrap();
        assert!(out[..14].iter().all(|v| v.is_nan()));
        assert!((out[14] - 100.0).abs() < 1e-9);
    }

    #[test]
    fn sma_basic() {
        let input = [1.0, 2.0, 3.0, 4.0];
        let out = sma(&input, 2).unwrap();
        assert!(out[0].is_nan());
        assert!((out[1] - 1.5).abs() < 1e-9);
        assert!((out[3] - 3.5).abs() < 1e-9);
    }

    #[test]
    fn sar_produces_output() {
        // 单调上升序列 → SAR 应在价格下方
        let high: Vec<f64> = (0..30).map(|i| 100.0 + i as f64 * 1.5).collect();
        let low: Vec<f64> = (0..30).map(|i| 99.0 + i as f64 * 1.5).collect();
        let out = sar(&high, &low, 0.02, 0.2).unwrap();
        assert_eq!(out.len(), 30);
        assert!(out.iter().any(|v| !v.is_nan()));
        // 上升趋势中 SAR 小于 low
        let last = out.last().unwrap();
        assert!(*last < *low.last().unwrap());
    }

    #[test]
    fn atr_basic() {
        let high = [11.0, 12.0, 13.0, 14.0, 15.0];
        let low = [9.0, 10.0, 11.0, 12.0, 13.0];
        let close = [10.0, 11.0, 12.0, 13.0, 14.0];
        let out = atr(&high, &low, &close, 3).unwrap();
        assert_eq!(out.len(), 5);
        assert!(out[..3].iter().all(|v| v.is_nan()));
        assert!(out[3] > 0.0);
    }

    #[test]
    fn adx_produces_output() {
        let n = 60;
        let high: Vec<f64> = (0..n)
            .map(|i| 100.0 + (i as f64 * 0.7).sin() * 5.0)
            .collect();
        let low: Vec<f64> = (0..n)
            .map(|i| 99.0 + (i as f64 * 0.7).sin() * 5.0)
            .collect();
        let close: Vec<f64> = (0..n)
            .map(|i| 99.5 + (i as f64 * 0.7).sin() * 5.0)
            .collect();
        let out = adx(&high, &low, &close, 14).unwrap();
        assert_eq!(out.len(), n);
        // lookback = 2*14-1 = 27：前 27 个 NaN
        assert!(out[..27].iter().all(|v| v.is_nan()));
        assert!(out[27] > 0.0);
    }
}
