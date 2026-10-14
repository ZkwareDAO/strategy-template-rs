//! Supertrend — ATR 通道趋势指标（纯 Rust 实现；TA-Lib 无此指标）
//!
//! 标准定义（Olivier Seban）：
//! - basic_upper = (high + low) / 2 + factor * ATR(period)
//! - basic_lower = (high + low) / 2 - factor * ATR(period)
//! - 上/下带 ratchet：多头趋势中下带只上移不下移，空头趋势中上带只下移不上移
//! - 趋势翻转：收盘越过对侧带（close < 上一根下带 → 翻空；close > 上一根上带 → 翻多）
//! - 输出：supertrend 线（多头=下带，空头=上带）+ direction（1=多, -1=空）
//!
//! 输出语义与 talib-rs 封装约定一致：前 period 个值未就绪（line=NaN, direction=0）。

/// Supertrend 输出
pub struct Supertrend {
    /// 趋势线：多头=下带，空头=上带
    pub line: Vec<f64>,
    /// 1=多头（线在价格下方）, -1=空头, 0=未就绪
    pub direction: Vec<i32>,
    /// ATR 序列（供策略做入场止损等派生计算）
    pub atr: Vec<f64>,
}

/// 计算 Supertrend
///
/// - `period`: ATR 周期
/// - `factor`: ATR 乘数（如 3.0）
/// - 输出与输入等长；前 `period` 个未就绪（line=NaN, direction=0）
pub fn supertrend(
    high: &[f64],
    low: &[f64],
    close: &[f64],
    period: usize,
    factor: f64,
) -> Result<Supertrend, String> {
    let n = high.len();
    if low.len() != n || close.len() != n {
        return Err("supertrend: 输入长度不一致".to_string());
    }
    if period == 0 {
        return Err("supertrend: period 不能为 0".to_string());
    }
    if n < period + 1 {
        // ATR 需要 period 根，且趋势判定需要上一根带值（额外 1 根）
        return Ok(Supertrend {
            line: vec![f64::NAN; n],
            direction: vec![0; n],
            atr: vec![f64::NAN; n],
        });
    }

    let atr = crate::talib::atr(high, low, close, period)?;
    let mut line = vec![f64::NAN; n];
    let mut direction = vec![0i32; n];
    let mut upper = vec![f64::NAN; n];
    let mut lower = vec![f64::NAN; n];

    // 起始方向：收盘 >= 中轨则多，否则空
    let mut dir: i32 = if close[period] >= (high[period] + low[period]) / 2.0 {
        1
    } else {
        -1
    };

    for i in period..n {
        let mid = (high[i] + low[i]) / 2.0;
        let a = atr[i];
        if a.is_nan() {
            continue; // ATR 未就绪（防御；period 之后不应发生）
        }
        let basic_upper = mid + factor * a;
        let basic_lower = mid - factor * a;

        // band ratchet：多头下带只上移，空头上带只下移；翻转时放开重置
        let prev_upper = if i > period { upper[i - 1] } else { f64::NAN };
        let prev_lower = if i > period { lower[i - 1] } else { f64::NAN };
        let prev_dir = if i > period { direction[i - 1] } else { dir };

        upper[i] = if prev_upper.is_nan()
            || basic_upper < prev_upper
            || (prev_dir == 1 && close[i - 1] > prev_upper)
        {
            basic_upper
        } else {
            prev_upper
        };
        lower[i] = if prev_lower.is_nan()
            || basic_lower > prev_lower
            || (prev_dir == -1 && close[i - 1] < prev_lower)
        {
            basic_lower
        } else {
            prev_lower
        };

        // 趋势维持/翻转（标准 TradingView 语义：用上一根带值判定）
        dir = if prev_dir == 1 {
            if !prev_lower.is_nan() && close[i] < prev_lower {
                -1
            } else {
                1
            }
        } else if !prev_upper.is_nan() && close[i] > prev_upper {
            1
        } else {
            -1
        };

        direction[i] = dir;
        line[i] = if dir == 1 { lower[i] } else { upper[i] };
    }

    Ok(Supertrend {
        line,
        direction,
        atr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 合成序列：先单调上涨后单调下跌
    fn series(n: usize) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
        (
            (0..n).map(|i| 100.0 + trend_val(i, n) + 1.0).collect(),
            (0..n).map(|i| 100.0 + trend_val(i, n) - 1.0).collect(),
            (0..n).map(|i| 100.0 + trend_val(i, n) + 0.3).collect(),
        )
    }
    fn trend_val(i: usize, n: usize) -> f64 {
        if i < n / 2 {
            i as f64 * 1.5
        } else {
            (n - i) as f64 * 1.5
        }
    }

    #[test]
    fn output_lengths_and_nan_prefix() {
        let (h, l, c) = series(60);
        let st = supertrend(&h, &l, &c, 10, 3.0).unwrap();
        assert_eq!(st.line.len(), 60);
        assert_eq!(st.direction.len(), 60);
        assert_eq!(st.atr.len(), 60);
        assert!(st.line[..10].iter().all(|v| v.is_nan()));
        assert!(st.direction[..10].iter().all(|d| *d == 0));
    }

    #[test]
    fn uptrend_is_long_below_price() {
        let (h, l, c) = series(60);
        let st = supertrend(&h, &l, &c, 10, 3.0).unwrap();
        // 前半段单调上涨 → 趋势多头，线在价格下方
        for ((dir, line), price) in st.direction[12..28]
            .iter()
            .zip(&st.line[12..28])
            .zip(&c[12..28])
        {
            assert_eq!(*dir, 1);
            assert!(*line < *price, "line 应在价格下方");
        }
    }

    #[test]
    fn flips_short_on_downtrend() {
        let (h, l, c) = series(60);
        let st = supertrend(&h, &l, &c, 10, 3.0).unwrap();
        let flipped = st.direction[30..]
            .iter()
            .position(|d| *d == -1)
            .expect("下跌段应出现空头");
        let i = 30 + flipped;
        assert!(st.line[i] > c[i], "空头线应在价格上方: i={i}");
    }

    #[test]
    fn lower_band_never_decreases_in_uptrend() {
        let (h, l, c) = series(60);
        let st = supertrend(&h, &l, &c, 10, 3.0).unwrap();
        // 多头趋势中 supertrend 线（=下带）只上移不下移
        for i in 11..30 {
            if st.direction[i] == 1 && st.direction[i - 1] == 1 {
                assert!(
                    st.line[i] >= st.line[i - 1] - 1e-9,
                    "多头下带不应下移: i={i}"
                );
            }
        }
    }

    #[test]
    fn insufficient_data_gives_all_nan() {
        let (h, l, c) = series(8);
        let st = supertrend(&h, &l, &c, 10, 3.0).unwrap();
        assert!(st.line.iter().all(|v| v.is_nan()));
        assert!(st.direction.iter().all(|d| *d == 0));
    }

    #[test]
    fn bad_inputs_rejected() {
        let (h, l, c) = series(30);
        assert!(supertrend(&h, &l[..20], &c, 10, 3.0).is_err());
        assert!(supertrend(&h, &l, &c, 0, 3.0).is_err());
    }
}
