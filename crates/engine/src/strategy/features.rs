//! 特征流水线与两级周期（PR-2，Task 5）。
//!
//! [`FeaturePlan`] 把 [`StrategyConfig`] 中声明的指标解析成语义明确的计算计划：
//! 每个指标只在其声明的 tf 的**已闭合**序列上计算（[`AggregationState::closed`]），
//! 两次闭合之间恒定取最后已闭合值（聚合层不产生中途快照，天然满足）。
//! 指标输出 NaN 或历史不足 → 该特征不写入 [`FeatureMap`]（不伪造 0 / NaN）。
//!
//! `macd` 额外派生出固定键 `macd_cross` 标签：`macd_line > signal -> "bullish"`、
//! `< -> "bearish"`、`== -> "equal"`；可用 MACD 样本不足 `signal` 期时，
//! `macd` 与 `macd_cross` 一并省略（warmup 不足不伪造）。
//!
//! 本模块是纯只读计算：只消费 [`AggregationState`]，无 `now()`、无运行时。

use std::collections::BTreeMap;

use strategy_indicators::{adx, atr, ema, rsi, sma};

use crate::core::aggregation::AggregationState;

use super::config::{self, IndicatorKind, ResolvedIndicator, StrategyConfig};

/// `macd` 派生的固定标签键（设计文档规则引用点）。
const MACD_CROSS: &str = "macd_cross";

/// 某时刻的全部已计算特征。
///
/// `values` 键 = 配置中的指标名（如 `adx`）；`labels` 键 = 派生标签
/// （当前仅 `macd_cross`）。从不写入代表「无效」的数值。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FeatureMap {
    pub values: BTreeMap<String, f64>,
    pub labels: BTreeMap<String, String>,
}

impl FeatureMap {
    /// 按指标名取数值；该特征不可用（空 / NaN / warmup 不足）时返回 `None`。
    pub fn number(&self, ind: &str) -> Option<f64> {
        self.values.get(ind).copied()
    }

    /// 按派生键取标签；无该标签时返回 `None`。
    pub fn label(&self, ind: &str) -> Option<&str> {
        self.labels.get(ind).map(String::as_str)
    }
}

/// 单条已解析指标：配置键名 + 已解析（tf + 种类）。
#[derive(Debug)]
struct FeatureSpec {
    name: String,
    resolved: ResolvedIndicator,
}

/// 特征计算计划。
///
/// [`FeaturePlan::from_config`] 一次性解析全部指标并聚合错误；[`FeaturePlan::compute`]
/// 可对任意时刻的 [`AggregationState`] 重算最新特征（幂等、只读）。
#[derive(Debug)]
pub struct FeaturePlan {
    specs: Vec<FeatureSpec>,
}

impl FeaturePlan {
    /// 解析配置中的全部指标；任何指标解析失败都聚合进 `Err(Vec<String>)`。
    ///
    /// 复用 [`config::resolve_indicator`] 的 tf/参数解析，并额外拒绝未知参数键。
    /// 另有任何一个指标含未知参数 → 整个计划失败（不产出部分计划）。
    pub fn from_config(cfg: &StrategyConfig) -> Result<Self, Vec<String>> {
        let mut errs = Vec::new();
        let mut specs = Vec::new();
        for (name, icfg) in &cfg.indicators {
            let mut valid = true;
            match config::resolve_indicator(name, icfg) {
                Ok(resolved) => {
                    let allowed = allowed_params(name);
                    for key in icfg.params.keys() {
                        if !allowed.contains(&key.as_str()) {
                            errs.push(format!("指标 {name} 未知参数: {key}"));
                            valid = false;
                        }
                    }
                    if valid {
                        specs.push(FeatureSpec {
                            name: name.clone(),
                            resolved,
                        });
                    }
                }
                Err(e) => errs.push(e),
            }
        }
        if errs.is_empty() {
            Ok(Self { specs })
        } else {
            Err(errs)
        }
    }

    /// 对给定聚合状态计算全部特征（只读，无副作用）。
    pub fn compute(&self, state: &AggregationState) -> FeatureMap {
        let mut values = BTreeMap::new();
        let mut labels = BTreeMap::new();
        for spec in &self.specs {
            compute_one(spec, state, &mut values, &mut labels);
        }
        FeatureMap { values, labels }
    }
}

/// 各指标允许的参数键（与 [`super::config::validate_config`] 的白名单一致）。
fn allowed_params(name: &str) -> &'static [&'static str] {
    match name {
        "adx" | "rsi" | "ema" | "sma" | "atr" => &["period"],
        "macd" => &["fast", "slow", "signal"],
        _ => &[],
    }
}

/// 取切片最后一个非 NaN 值；全是 NaN / 空切片 → `None`。
fn non_nan_last(v: &[f64]) -> Option<f64> {
    v.iter().rev().find(|x| !x.is_nan()).copied()
}

/// 把指标函数输出收敛为单个「最后有效值」；函数出错或无可读数 → `None`。
fn single_value(result: Result<Vec<f64>, String>) -> Option<f64> {
    let values = result.ok()?;
    non_nan_last(&values)
}

/// 计算单条指标并写入 `values` / `labels`（按规则省略不可用特征）。
fn compute_one(
    spec: &FeatureSpec,
    state: &AggregationState,
    values: &mut BTreeMap<String, f64>,
    labels: &mut BTreeMap<String, String>,
) {
    let bars = state.closed(spec.resolved.tf);
    if bars.is_empty() {
        return;
    }
    let high: Vec<f64> = bars.iter().map(|b| b.high).collect();
    let low: Vec<f64> = bars.iter().map(|b| b.low).collect();
    let close: Vec<f64> = bars.iter().map(|b| b.close).collect();

    match &spec.resolved.kind {
        IndicatorKind::Adx { period } => {
            if let Some(v) = single_value(adx(&high, &low, &close, *period)) {
                values.insert(spec.name.clone(), v);
            }
        }
        IndicatorKind::Rsi { period } => {
            if let Some(v) = single_value(rsi(&close, *period)) {
                values.insert(spec.name.clone(), v);
            }
        }
        IndicatorKind::Ema { period } => {
            if let Some(v) = single_value(ema(&close, *period)) {
                values.insert(spec.name.clone(), v);
            }
        }
        IndicatorKind::Sma { period } => {
            if let Some(v) = single_value(sma(&close, *period)) {
                values.insert(spec.name.clone(), v);
            }
        }
        IndicatorKind::Atr { period } => {
            if let Some(v) = single_value(atr(&high, &low, &close, *period)) {
                values.insert(spec.name.clone(), v);
            }
        }
        IndicatorKind::Macd { fast, slow, signal } => {
            compute_macd(spec, &close, *fast, *slow, *signal, values, labels)
        }
    }
}

/// MACD + `macd_cross` 派生标签（warmup 不足时二者一并省略）。
fn compute_macd(
    spec: &FeatureSpec,
    close: &[f64],
    fast: usize,
    slow: usize,
    signal: usize,
    values: &mut BTreeMap<String, f64>,
    labels: &mut BTreeMap<String, String>,
) {
    let Ok(fast_line) = ema(close, fast) else {
        return;
    };
    let Ok(slow_line) = ema(close, slow) else {
        return;
    };
    // 从两序列都非 NaN 的首个索引起计算 line = ema_fast - ema_slow
    let Some(first) = (0..close.len()).find(|&i| !fast_line[i].is_nan() && !slow_line[i].is_nan())
    else {
        return;
    };
    let line_tail: Vec<f64> = fast_line
        .iter()
        .zip(&slow_line)
        .skip(first)
        .map(|(f, s)| f - s)
        .collect();
    if line_tail.len() < signal {
        return; // signal 期未满：warmup 不足，不伪造 macd / macd_cross
    }
    let Some(signal_last) = single_value(ema(&line_tail, signal)) else {
        return;
    };
    let Some(macd_last) = line_tail.last().copied().filter(|v| !v.is_nan()) else {
        return;
    };
    let label = if macd_last > signal_last {
        "bullish"
    } else if macd_last < signal_last {
        "bearish"
    } else {
        "equal"
    };
    values.insert(spec.name.clone(), macd_last);
    labels.insert(MACD_CROSS.to_string(), label.to_string());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::aggregation::{AggregationState, UtcGridPolicy};
    use crate::core::bar::{Bar, BarExtensions, BarFinality, Timeframe};
    use crate::core::identity::{ExchangeId, InstrumentId};
    use crate::core::session::ContinuousSession;
    use chrono::{Duration, TimeZone, Utc};

    // ─── helpers ─────────────────────────────────────────────

    /// 用 YAML 拼 `StrategyConfig`（indicators 段为块式 YAML，由调用方缩进）。
    fn config(indicators_yaml: &str) -> StrategyConfig {
        let yaml = format!(
            r#"
strategy_type: xsec_momentum
strategy_id: "test"
universe:
  - BTCUSDT
timeframes:
  - 1m
  - 1h
indicators:
{indicators_yaml}
entry:
  default: []
  overrides: {{}}
exit:
  default: []
  overrides: {{}}
portfolio_exit:
  trailing: null
  stop_loss_pct: null
rebalance: null
risk:
  max_drawdown: null
  max_total_notional: 1.0
sizing:
  per_position_pct: 0.5
"#
        );
        serde_yaml::from_str(&yaml).expect("yaml 必须合法")
    }

    fn new_state() -> AggregationState {
        AggregationState::new(&[Timeframe::M1, Timeframe::H1], 100_000)
    }

    /// 喂入已闭合 1m 分钟线（M1 恒收线，H1 按整点网格聚合）。
    fn feed_minutes(
        state: &mut AggregationState,
        from_min: usize,
        count: usize,
        price: &dyn Fn(usize) -> f64,
    ) {
        let inst = InstrumentId::perpetual(&ExchangeId::new("binance"), "BTCUSDT");
        let policy = UtcGridPolicy;
        let session = ContinuousSession::default();
        let t0 = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
        for i in from_min..from_min + count {
            let mid = price(i);
            let bar = Bar {
                instrument: inst.clone(),
                open_time: t0 + Duration::minutes(i as i64),
                open: mid,
                high: mid + 1.0,
                low: mid - 1.0,
                close: mid,
                volume: 1.0,
                finality: BarFinality::Final,
                extensions: BarExtensions::default(),
            };
            state.on_closed_1m(bar, &policy, &session).unwrap();
        }
    }

    /// 读取 state 中 H1 已闭合序列的 high/low/close 三元组。
    fn h1_series(state: &AggregationState) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
        let high = state.closed(Timeframe::H1).iter().map(|b| b.high).collect();
        let low = state.closed(Timeframe::H1).iter().map(|b| b.low).collect();
        let close = state
            .closed(Timeframe::H1)
            .iter()
            .map(|b| b.close)
            .collect();
        (high, low, close)
    }

    const ADX_CFG: &str = "  adx:\n    tf: 1h\n    period: 14\n";
    const MACD_CFG: &str = "  macd:\n    tf: 1h\n    fast: 12\n    slow: 26\n    signal: 9\n";

    // ─── tests ───────────────────────────────────────────────

    #[test]
    fn features_use_last_closed_value_on_declared_tf() {
        // adx 声明在 H1：只应在 H1 已闭合序列上计算，两次闭合之间取最后已闭合值。
        let price = |i: usize| 100.0 + (i as f64 * 0.05).sin() * 10.0 + i as f64 * 0.003;
        let mut state = new_state();
        feed_minutes(&mut state, 0, 60 * 40, &price); // 40 根已闭合 H1

        let plan = FeaturePlan::from_config(&config(ADX_CFG)).unwrap();
        let map = plan.compute(&state);

        let (high, low, close) = h1_series(&state);
        assert_eq!(close.len(), 40, "40 根已闭合 H1");
        let ref_last = *strategy_indicators::adx(&high, &low, &close, 14)
            .unwrap()
            .last()
            .unwrap();
        assert!(
            !ref_last.is_nan(),
            "40 根 H1 足够 adx(14)，参考值必须非 NaN"
        );

        let value = map
            .number("adx")
            .expect("adx 已进入 FeatureMap（warmup 充足）");
        assert!(
            (value - ref_last).abs() < 1e-9,
            "adx 必须等于已闭合序列最后一根: {value} vs {ref_last}"
        );

        // 同一 tf 再加 30 分钟（下一根 H1 尚未闭合）：特征值必须不变。
        feed_minutes(&mut state, 60 * 40, 30, &price);
        assert_eq!(state.closed(Timeframe::H1).len(), 40, "H1 数量不变");
        let again = plan
            .compute(&state)
            .number("adx")
            .expect("adx 仍在 FeatureMap");
        assert_eq!(again, value, "两次闭合之间必须恒定取最后已闭合值");
    }

    #[test]
    fn warmup_insufficient_omits_feature_instead_of_zero() {
        // 只有 10 根 H1：adx(14) 整个输出为 NaN → FeatureMap 无 "adx"（不伪造 0）。
        let price = |i: usize| 100.0 + (i as f64).sin();
        let mut state = new_state();
        feed_minutes(&mut state, 0, 60 * 10, &price);
        assert_eq!(state.closed(Timeframe::H1).len(), 10);

        let plan = FeaturePlan::from_config(&config(ADX_CFG)).unwrap();
        let map = plan.compute(&state);

        assert!(!map.values.contains_key("adx"), "warmup 不足时不得写入 adx");
        assert!(map.number("adx").is_none(), "number() 必须返回 None");
        assert!(map.labels.is_empty(), "不得伪造任何派生标签");
    }

    #[test]
    fn macd_cross_label_tracks_last_relation() {
        // 先涨后跌的 H1 序列：values["macd"] 与直接 EMA 计算一致（1e-9），
        // macd_cross 反映最后一根 macd_line 与 signal 的大小关系。
        let price = |i: usize| {
            let h = (i / 60) as f64;
            if h < 30.0 {
                100.0 + h * 2.0
            } else {
                160.0 - (h - 30.0) * 2.0
            }
        };
        let mut state = new_state();
        feed_minutes(&mut state, 0, 60 * 60, &price); // 60 根已闭合 H1
        assert_eq!(state.closed(Timeframe::H1).len(), 60);

        let plan = FeaturePlan::from_config(&config(MACD_CFG)).unwrap();
        let map = plan.compute(&state);

        // 参考：直接 EMA 计算（与实现同公式）。
        let (_, _, close) = h1_series(&state);
        let fast = strategy_indicators::ema(&close, 12).unwrap();
        let slow = strategy_indicators::ema(&close, 26).unwrap();
        let first = (0..close.len())
            .find(|&i| !fast[i].is_nan() && !slow[i].is_nan())
            .expect("必有有效点");
        let line_tail: Vec<f64> = fast
            .iter()
            .zip(&slow)
            .skip(first)
            .map(|(f, s)| f - s)
            .collect();
        assert!(line_tail.len() >= 9, "样本足够算 signal");
        let signal_ref = *strategy_indicators::ema(&line_tail, 9)
            .unwrap()
            .last()
            .unwrap();
        let macd_ref = *line_tail.last().unwrap();

        let macd = map.number("macd").expect("macd 已进入 FeatureMap");
        assert!(
            (macd - macd_ref).abs() < 1e-9,
            "macd 与直接 EMA 计算一致: {macd} vs {macd_ref}"
        );

        let label = map.label(MACD_CROSS).expect("必须产出 macd_cross");
        let expected = if macd_ref > signal_ref {
            "bullish"
        } else if macd_ref < signal_ref {
            "bearish"
        } else {
            "equal"
        };
        assert_eq!(label, expected, "标签必须取自最后一根的有效关系");
        assert!(
            label == "bullish" || label == "bearish",
            "先涨后跌末端不能是 equal: {label}"
        );
    }

    #[test]
    fn feature_plan_rejects_unknown_indicator_param() {
        // adx 配置 period + bogus 未知参数 + 未知名 mystery：
        // 两个错误必须聚合返回（adx 走未知参数检查，mystery 走 resolve_indicator）。
        let cfg =
            config("  adx:\n    tf: 1h\n    period: 14\n    bogus: 1\n  mystery:\n    tf: 1h\n");
        let errs = FeaturePlan::from_config(&cfg).unwrap_err();
        assert!(errs.iter().any(|e| e.contains("bogus")), "{errs:?}");
        assert!(errs.iter().any(|e| e.contains("mystery")), "{errs:?}");
        assert_eq!(errs.len(), 2, "两个错误必须全部聚合: {errs:?}");
    }
}
