//! SAR（逐字对齐 Python cta-strategy-code 的 sar_talib）
//!
//! Python 参考回测使用手写实现（其文档称"talib-rs sar_ext 语义"，但初始化、
//! 严格不等号、out[0] 均与 talib_rs 的 sar/sar_ext 不同）。回测对齐必须
//! 使用本移植，而非 talib_rs 自带 SAR。

/// 抛物线 SAR（Python sar_supertrend_core.sar_talib 的逐行移植）。
///
/// 初始化：high[1] > high[0] → 多头（sar=low[0], ep=high[0]），否则空头。
/// 每根先按翻转判定产出当根 SAR（翻转时=旧 EP），再递推下一根 SAR 并限制
/// 不越过当根/前一根的极值。数据不足（<2）返回 Err。
pub fn sar_compat(
    high: &[f64],
    low: &[f64],
    acceleration: f64,
    maximum: f64,
) -> Result<Vec<f64>, String> {
    let n = high.len();
    if n != low.len() {
        return Err("sar: high/low 长度不一致".into());
    }
    if n < 2 {
        return Err("sar: 至少需要 2 根 K 线".into());
    }

    let mut out = vec![f64::NAN; n];

    // 初始化
    let (mut is_long, mut ep, mut sar_val) = if high[1] > high[0] {
        (true, high[0], low[0])
    } else {
        (false, low[0], high[0])
    };
    let mut af = acceleration;
    out[0] = sar_val;

    for i in 1..n {
        if is_long {
            if low[i] < sar_val {
                // 多头反转为空头：SAR = 之前的极值点
                is_long = false;
                sar_val = ep;
                ep = low[i];
                af = acceleration;
            } else if high[i] > ep {
                ep = high[i];
                af = (af + acceleration).min(maximum);
            }
        } else if high[i] > sar_val {
            // 空头反转为多头
            is_long = true;
            sar_val = ep;
            ep = high[i];
            af = acceleration;
        } else if low[i] < ep {
            ep = low[i];
            af = (af + acceleration).min(maximum);
        }

        out[i] = sar_val;

        // 递推下一根 SAR
        sar_val += af * (ep - sar_val);

        // SAR 不能越过当根/前一根 K 线的极值
        if is_long {
            sar_val = sar_val.min(low[i]).min(low[i - 1]);
        } else {
            sar_val = sar_val.max(high[i]).max(high[i - 1]);
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 与 Python sar_talib 同输入（seed=7 的 60 根随机序列）的逐值对齐
    #[test]
    fn matches_python_vectors() {
        let v: serde_json::Value =
            serde_json::from_str(include_str!("tests_data/sar_vec.json")).unwrap();
        let high: Vec<f64> = v["high"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_f64().unwrap())
            .collect();
        let low: Vec<f64> = v["low"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_f64().unwrap())
            .collect();
        let expected: Vec<Option<f64>> = v["sar"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_f64())
            .collect();

        let out = sar_compat(&high, &low, 0.02, 0.2).unwrap();
        assert_eq!(out.len(), expected.len());
        for (got, exp) in out.iter().zip(expected) {
            match exp {
                None => assert!(got.is_nan()),
                Some(e) => assert!((got - e).abs() < 1e-6, "got {} exp {}", got, e),
            }
        }
    }

    #[test]
    fn errors_on_short_input() {
        assert!(sar_compat(&[1.0], &[1.0], 0.02, 0.2).is_err());
        assert!(sar_compat(&[1.0, 2.0], &[1.0], 0.02, 0.2).is_err());
    }
}
