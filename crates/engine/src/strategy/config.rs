//! 横截面策略配置 schema 与加载期结构校验（PR-1/PR-2）。
//!
//! `StrategyConfig` 是策略级单文件（设计文档 §5.1）的内存表示；[`validate_config`]
//! 在装配期执行全部结构校验（错误聚合、不早停），[`resolved_rule_summary`]
//! 输出确定性条件集摘要供审计日志与快照测试。
//!
//! 本模块只做**配置类型 + 校验**，不含特征计算 / 运行时逻辑（Task 5+）。

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::core::bar::Timeframe;

/// 策略级单文件配置（设计文档 §5.1 的内存表示）。
///
/// `timeframes[0]` = base TF（决策节奏），其余为两级周期的上级 tf。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub strategy_type: String,
    pub strategy_id: Option<String>,
    /// 组合式 / 复制式；本期仅 `CrossSectional`（`multi_instance` 校验时明确拒绝）
    #[serde(default)]
    pub mode: StrategyMode,
    /// 同文件多标的的仓位归属：
    /// - `true`（默认）= 合并为一个组合总仓，各标的是该总仓下的子仓；
    /// - `false` = 每个标的一个独立总仓，各自管理自己的子仓与资金。
    ///
    /// 单标的时该字段无意义。
    #[serde(default = "default_merge_positions")]
    pub merge_positions: bool,
    pub universe: Vec<UniverseEntry>,
    pub timeframes: Vec<String>,
    pub indicators: BTreeMap<String, IndicatorCfg>,
    pub entry: RuleSet,
    pub exit: RuleSet,
    pub portfolio_exit: PortfolioExitCfg,
    pub rebalance: Option<RebalanceCfg>,
    pub risk: RiskCfg,
    pub sizing: SizingCfg,
    /// 策略私有参数（Task 12 示例策略等消费），加载期不校验
    pub params: Option<serde_json::Value>,
}

/// 运行形态（设计文档 §5.2）：本期的 `StrategyMode` 是**组合形态**字段，
/// 不是旧的 runtime `mode`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyMode {
    /// 一个实例一个组合、资金共享（默认，单标 → 横截面的渐进路径）
    #[default]
    CrossSectional,
    /// 复制式：同模板 N 份独立实例；本期未实现，加载期拒绝
    MultiInstance,
}

/// 旧配置没有该字段时保持“一个组合总仓”的历史行为。
fn default_merge_positions() -> bool {
    true
}

/// 标的条目：字符串简写，或完整对象（可选 exchange）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum UniverseEntry {
    Symbol(String),
    Detailed {
        symbol: String,
        exchange: Option<String>,
    },
}

/// 单个指标的配置：`tf` 时间框架 + `#[serde(flatten)]` 参数表。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndicatorCfg {
    pub tf: String,
    #[serde(flatten)]
    pub params: BTreeMap<String, serde_json::Value>,
}

/// `default` + 按标的 `overrides` 的条件集（AND 语义）。
/// 两字段均可省略（§5.1 示例只写 `default:`）；serde(default) 缺失 = 空集。
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RuleSet {
    pub default: Vec<Rule>,
    pub overrides: BTreeMap<String, Vec<Rule>>,
}

/// 单条条件规则。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Rule {
    pub ind: String,
    pub op: RuleOp,
    pub value: RuleValue,
}

/// 比较操作符，serde 形式为 `">" "<" ">=" "<=" "==" "!="`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleOp {
    Gt,
    Lt,
    Ge,
    Le,
    Eq,
    Ne,
}

impl<'de> Deserialize<'de> for RuleOp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            ">" => Ok(RuleOp::Gt),
            "<" => Ok(RuleOp::Lt),
            ">=" => Ok(RuleOp::Ge),
            "<=" => Ok(RuleOp::Le),
            "==" => Ok(RuleOp::Eq),
            "!=" => Ok(RuleOp::Ne),
            other => Err(D::Error::custom(format!("非法比较操作符: {other}"))),
        }
    }
}

impl Serialize for RuleOp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            RuleOp::Gt => ">",
            RuleOp::Lt => "<",
            RuleOp::Ge => ">=",
            RuleOp::Le => "<=",
            RuleOp::Eq => "==",
            RuleOp::Ne => "!=",
        })
    }
}

/// 规则值：数值或文本（untagged number/string）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RuleValue {
    Number(f64),
    Text(String),
}

/// 组合级总仓止盈止损。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioExitCfg {
    pub trailing: Option<TrailingCfg>,
    pub stop_loss_pct: Option<f64>,
}

/// 组合浮盈回落跟踪。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrailingCfg {
    pub arm_pnl_pct: f64,
    pub giveback_pct: f64,
}

/// 横截面再平衡。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RebalanceCfg {
    pub every_bars: u64,
    pub top_k: usize,
    pub bottom_k: usize,
    pub weight_scheme: WeightScheme,
}

/// 权重方案；本期仅 `equal`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WeightScheme {
    Equal,
}

/// 组合风险项。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskCfg {
    /// L4 NAV 回撤层本期推迟：加载时告警、不生效（校验不报错）
    pub max_drawdown: Option<f64>,
    pub max_total_notional: f64,
}

/// 单标的资金份额（占 NAV 比例）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SizingCfg {
    pub per_position_pct: f64,
    /// 子仓硬上限（占策略 NAV 比例）；缺省不启用。
    #[serde(default)]
    pub max_position_pct: Option<f64>,
}

/// 解析后的指标（tf + 种类），供特征流水线（Task 5）与校验复用。
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedIndicator {
    pub tf: Timeframe,
    pub kind: IndicatorKind,
}

/// 指标种类与其参数。
#[derive(Debug, Clone, PartialEq)]
pub enum IndicatorKind {
    Adx {
        period: usize,
    },
    Rsi {
        period: usize,
    },
    Ema {
        period: usize,
    },
    Sma {
        period: usize,
    },
    Atr {
        period: usize,
    },
    Macd {
        fast: usize,
        slow: usize,
        signal: usize,
    },
}

/// 按名字解析指标：`adx|rsi|ema|sma|atr` 仅 `period`（默认 14），
/// `macd` 仅 `fast|slow|signal`（默认 12/26/9）；未知名/参数/类型报错。
pub fn resolve_indicator(name: &str, cfg: &IndicatorCfg) -> Result<ResolvedIndicator, String> {
    let tf = Timeframe::parse(&cfg.tf)
        .map_err(|e| format!("指标 {name} 时间框架 {} 解析失败: {e}", cfg.tf))?;
    let kind = match name {
        "adx" | "rsi" | "ema" | "sma" | "atr" => {
            let period = positive_param(name, &cfg.params, "period", 14)?;
            match name {
                "adx" => IndicatorKind::Adx { period },
                "rsi" => IndicatorKind::Rsi { period },
                "ema" => IndicatorKind::Ema { period },
                "sma" => IndicatorKind::Sma { period },
                _ => IndicatorKind::Atr { period },
            }
        }
        "macd" => IndicatorKind::Macd {
            fast: positive_param(name, &cfg.params, "fast", 12)?,
            slow: positive_param(name, &cfg.params, "slow", 26)?,
            signal: positive_param(name, &cfg.params, "signal", 9)?,
        },
        other => return Err(format!("未知指标: {other}")),
    };
    Ok(ResolvedIndicator { tf, kind })
}

/// 读取正整数参数；缺省用 `default`。
fn positive_param(
    ind: &str,
    params: &BTreeMap<String, serde_json::Value>,
    key: &str,
    default: usize,
) -> Result<usize, String> {
    match params.get(key) {
        None => Ok(default),
        Some(v) => match v.as_u64() {
            Some(n) if n >= 1 && n <= usize::MAX as u64 => Ok(n as usize),
            _ => Err(format!("指标 {ind} 参数 {key} 必须为正整数，得到 {v}")),
        },
    }
}

/// 加载期结构校验：聚合**全部**错误，不早停。
///
/// 规则见任务 brief 1–8：`strategy_type`/`universe`/`mode`；`timeframes`；
/// 指标白名单与参数；entry/exit 规则引用与取值；rebalance 必填与边界；
/// 杠杆上限覆盖；`max_drawdown` 不报错；trailing/stop_loss 边界。
pub fn validate_config(cfg: &StrategyConfig) -> Result<(), Vec<String>> {
    let mut errs: Vec<String> = Vec::new();

    // 1. 顶层非空 + 组合形态
    if cfg.strategy_type.trim().is_empty() {
        errs.push("strategy_type 不能为空".into());
    }
    if cfg.universe.is_empty() {
        errs.push("universe 不能为空".into());
    }
    if cfg.mode != StrategyMode::CrossSectional {
        errs.push("multi_instance 未实现：本期仅支持 cross_sectional 组合式".into());
    }

    // 2. timeframes 非空且均可 parse；指标 tf ⊆ timeframes 且为 base 整数倍
    if cfg.timeframes.is_empty() {
        errs.push("timeframes 不能为空".into());
    }
    let parsed_tfs: Vec<u32> = cfg
        .timeframes
        .iter()
        .filter_map(|t| Timeframe::parse(t).ok().map(|tf| tf.0))
        .collect();
    for (i, tf) in cfg.timeframes.iter().enumerate() {
        if let Err(e) = Timeframe::parse(tf) {
            errs.push(format!("timeframes[{i}] 解析失败 {tf}: {e}"));
        }
    }
    if let Some(base) = parsed_tfs.first().copied() {
        for (name, icfg) in &cfg.indicators {
            match Timeframe::parse(&icfg.tf) {
                Err(_) => errs.push(format!("指标 {name} 时间框架 {} 解析失败", icfg.tf)),
                Ok(tf) => {
                    if !parsed_tfs.contains(&tf.0) {
                        errs.push(format!(
                            "指标 {name} 时间框架 {} 不在 timeframes 内: [{}]",
                            icfg.tf,
                            cfg.timeframes.join(", ")
                        ));
                    }
                    if tf.0 % base != 0 {
                        errs.push(format!(
                            "指标 {name} 时间框架 {} 必须是 base 时间框架({}) 的整数倍",
                            icfg.tf, cfg.timeframes[0]
                        ));
                    }
                }
            }
        }
    }

    // 3. 指标白名单 + 参数键 + 参数值
    for (name, icfg) in &cfg.indicators {
        let allowed: &[&str] = match name.as_str() {
            "adx" | "rsi" | "ema" | "sma" | "atr" => &["period"],
            "macd" => &["fast", "slow", "signal"],
            other => {
                errs.push(format!("未知指标: {other}"));
                continue;
            }
        };
        for key in icfg.params.keys() {
            if !allowed.contains(&key.as_str()) {
                errs.push(format!("指标 {name} 未知参数: {key}"));
            }
        }
        if let Err(e) = resolve_indicator(name, icfg) {
            errs.push(e);
        }
    }

    // 4. entry/exit 规则
    validate_ruleset(cfg, "entry", &cfg.entry, &mut errs);
    validate_ruleset(cfg, "exit", &cfg.exit, &mut errs);

    // 5. rebalance 必填与边界：只有合并总仓才需要跨标的调仓节奏；
    // 逐标的独立总仓每轮按标的各自判定，不依赖 rebalance 窗口。
    if cfg.universe.len() >= 2 && cfg.merge_positions && cfg.rebalance.is_none() {
        errs.push("universe.len() >= 2 且 merge_positions=true 时必须配置 rebalance".into());
    }
    if let Some(reb) = &cfg.rebalance {
        if reb.every_bars == 0 {
            errs.push("rebalance.every_bars 必须 > 0".into());
        }
        if reb.top_k == 0 {
            errs.push("rebalance.top_k 必须 > 0".into());
        }
        if reb.bottom_k != 0 {
            errs.push("rebalance.bottom_k 本期未实现（空头侧推迟），必须为 0".into());
        }
    }

    // 6. 杠杆上限必须覆盖全 universe（按 per_position 与子仓上限中更小者）
    if cfg.sizing.per_position_pct <= 0.0 {
        errs.push("sizing.per_position_pct 必须 > 0".into());
    }
    let effective_pct = match cfg.sizing.max_position_pct {
        Some(cap) if cap <= 0.0 => {
            errs.push("sizing.max_position_pct 必须 > 0".into());
            cfg.sizing.per_position_pct
        }
        Some(cap) => cfg.sizing.per_position_pct.min(cap),
        None => cfg.sizing.per_position_pct,
    };
    // 逐标的独立总仓时每个标的只持有一个子仓，容量按单标的而不是整个
    // universe 计；合并总仓仍按全部标的同向最大暴露校验。
    let exposure_slots = if cfg.merge_positions {
        cfg.universe.len() as f64
    } else {
        1.0
    };
    if effective_pct > 0.0 && effective_pct * exposure_slots > cfg.risk.max_total_notional {
        errs.push(format!(
            "sizing 有效单仓比例({:.6}) * exposure_slots({}) 超过 risk.max_total_notional({:.6})",
            effective_pct, exposure_slots, cfg.risk.max_total_notional
        ));
    }

    // 7. max_drawdown：L4 推迟，加载时告警由装配层发（校验不报错）

    // 8. portfolio_exit 边界
    if let Some(t) = &cfg.portfolio_exit.trailing {
        if t.arm_pnl_pct <= 0.0 {
            errs.push("portfolio_exit.trailing.arm_pnl_pct 必须 > 0".into());
        }
        if t.giveback_pct <= 0.0 || t.giveback_pct > 100.0 {
            errs.push("portfolio_exit.trailing.giveback_pct 必须 > 0 且 <= 100".into());
        }
    }
    if let Some(s) = cfg.portfolio_exit.stop_loss_pct {
        if s <= 0.0 {
            errs.push("portfolio_exit.stop_loss_pct 必须 > 0".into());
        }
    }

    if errs.is_empty() {
        Ok(())
    } else {
        Err(errs)
    }
}

fn validate_ruleset(cfg: &StrategyConfig, section: &str, rs: &RuleSet, errs: &mut Vec<String>) {
    for r in &rs.default {
        validate_rule(cfg, section, "default", r, errs);
    }
    for (symbol, rules) in &rs.overrides {
        for r in rules {
            validate_rule(cfg, section, symbol, r, errs);
        }
    }
}

fn validate_rule(cfg: &StrategyConfig, section: &str, sym: &str, r: &Rule, errs: &mut Vec<String>) {
    let where_ = format!("{section}[{sym}]");
    if r.ind == "macd_cross" {
        if !cfg.indicators.contains_key("macd") {
            errs.push(format!("{where_} 使用 macd_cross 但未声明 macd"));
        }
        match &r.value {
            RuleValue::Text(t) => {
                if t != "bullish" && t != "bearish" {
                    errs.push(format!(
                        "{where_} macd_cross 文本值只允许 bullish|bearish，得到 {t}"
                    ));
                }
            }
            RuleValue::Number(_) => {
                errs.push(format!("{where_} macd_cross 只接受文本值 bullish|bearish"));
            }
        }
        if r.op != RuleOp::Eq && r.op != RuleOp::Ne {
            errs.push(format!("{where_} 文本比较只允许 == / !="));
        }
    } else {
        if !cfg.indicators.contains_key(&r.ind) {
            errs.push(format!("{where_} 引用了未声明指标: {}", r.ind));
        }
        if !matches!(r.value, RuleValue::Number(_)) {
            errs.push(format!("{where_} 指标 {} 只接受数值 value", r.ind));
        }
    }
}

/// 确定性条件集摘要：按 universe 符号字典序输出
/// `symbol: entry=[...] exit=[...]`；规则按配置文件顺序，数值 `{:.6}`。
///
/// 语义：某符号有 `overrides` 时用 override 条件组（替换 default），
/// 否则用 default——与 §5.3「list 整组替换」一致。
pub fn resolved_rule_summary(cfg: &StrategyConfig) -> String {
    let mut symbols: Vec<String> = cfg.universe.iter().map(universe_symbol).collect();
    symbols.sort();
    symbols.dedup();

    let mut out = String::new();
    for (i, sym) in symbols.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        let entry = cfg.entry.overrides.get(sym).unwrap_or(&cfg.entry.default);
        let exit = cfg.exit.overrides.get(sym).unwrap_or(&cfg.exit.default);
        let _ = write!(
            out,
            "{sym}: entry=[{}] exit=[{}]",
            format_rules(entry),
            format_rules(exit)
        );
    }
    out
}

fn universe_symbol(u: &UniverseEntry) -> String {
    match u {
        UniverseEntry::Symbol(s) => s.clone(),
        UniverseEntry::Detailed { symbol, .. } => symbol.clone(),
    }
}

fn format_rules(rules: &[Rule]) -> String {
    rules.iter().map(format_rule).collect::<Vec<_>>().join("; ")
}

fn format_rule(r: &Rule) -> String {
    let op = match r.op {
        RuleOp::Gt => ">",
        RuleOp::Lt => "<",
        RuleOp::Ge => ">=",
        RuleOp::Le => "<=",
        RuleOp::Eq => "==",
        RuleOp::Ne => "!=",
    };
    match &r.value {
        RuleValue::Number(n) => format!("{}{}{:.6}", r.ind, op, n),
        RuleValue::Text(t) => format!("{}{}{}", r.ind, op, t),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 设计文档 §5.1 的完整示例（universe 收窄为 BTC/ETH，全部校验可过）。
    const DOC_YAML: &str = r#"
strategy_type: xsec_momentum
strategy_id: "xsec_momentum:default"
universe:
  - BTCUSDT
  - ETHUSDT
timeframes: [15m, 1h]
indicators:
  adx:  { tf: 15m, period: 14 }
  macd: { tf: 1h, fast: 12, slow: 26, signal: 9 }
entry:
  default:
    - { ind: adx, op: ">", value: 20 }
  overrides:
    BTCUSDT:
      - { ind: adx, op: ">", value: 25 }
    ETHUSDT:
      - { ind: adx, op: ">", value: 50 }
      - { ind: macd_cross, op: "==", value: bearish }
exit:
  default: []
  overrides:
    ETHUSDT:
      - { ind: macd_cross, op: "==", value: bullish }
portfolio_exit:
  trailing: { arm_pnl_pct: 10, giveback_pct: 40 }
  stop_loss_pct: 8
rebalance: { every_bars: 96, top_k: 3, bottom_k: 0, weight_scheme: equal }
risk: { max_drawdown: 0.10, max_total_notional: 0.80 }
sizing: { per_position_pct: 0.10 }
"#;

    fn doc_example() -> StrategyConfig {
        serde_yaml::from_str(DOC_YAML).unwrap()
    }

    #[test]
    fn parses_doc_example_and_resolves_indicators() {
        let cfg = doc_example();
        assert_eq!(cfg.universe.len(), 2);
        assert_eq!(cfg.timeframes[0], "15m");
        assert_eq!(
            resolve_indicator("adx", &cfg.indicators["adx"])
                .unwrap()
                .kind,
            IndicatorKind::Adx { period: 14 }
        );
        assert!(validate_config(&cfg).is_ok());
    }

    #[test]
    fn multi_instance_is_rejected() {
        let err = validate_config(&StrategyConfig {
            mode: StrategyMode::MultiInstance,
            ..doc_example()
        })
        .unwrap_err()
        .join(" ");
        assert!(err.contains("multi_instance"), "{err}");
    }

    #[test]
    fn rule_ind_must_be_declared() {
        let mut cfg = doc_example();
        cfg.entry.default[0].ind = "not_declared".into();
        let err = validate_config(&cfg).unwrap_err().join("\n");
        assert!(
            err.contains("not_declared"),
            "错误必须点名未声明指标: {err}"
        );
    }

    #[test]
    fn indicator_tf_must_be_integer_multiple_of_base() {
        let mut cfg = doc_example();
        cfg.timeframes = vec!["15m".into()];
        cfg.indicators.get_mut("adx").unwrap().tf = "20m".into();
        let err = validate_config(&cfg).unwrap_err().join("\n");
        assert!(err.contains("20m") && err.contains("15m"), "{err}");
    }

    #[test]
    fn timeframes_must_be_non_empty() {
        let mut cfg = doc_example();
        cfg.timeframes = vec![];
        let err = validate_config(&cfg).unwrap_err().join("\n");
        assert!(
            err.contains("timeframes"),
            "空 timeframes 必须被拒绝: {err}"
        );
    }

    #[test]
    fn universe_two_requires_rebalance_only_when_positions_merge() {
        let mut cfg = doc_example();
        cfg.rebalance = None;
        let err = validate_config(&cfg).unwrap_err().join("\n");
        assert!(err.contains("rebalance"), "{err}");

        cfg.merge_positions = false;
        assert!(
            validate_config(&cfg).is_ok(),
            "逐标的独立总仓不依赖跨标的 rebalance 窗口"
        );
    }

    #[test]
    fn merge_positions_defaults_to_true_for_legacy_yaml() {
        let cfg: StrategyConfig = serde_yaml::from_str(DOC_YAML).unwrap();
        assert!(cfg.merge_positions, "旧配置缺字段时保持合并总仓");
    }

    #[test]
    fn leverage_cap_must_cover_universe() {
        let mut cfg = doc_example();
        cfg.sizing.per_position_pct = 0.6;
        cfg.risk.max_total_notional = 0.8;
        let err = validate_config(&cfg).unwrap_err().join("\n");
        assert!(err.contains("max_total_notional"), "{err}");
    }

    #[test]
    fn leverage_cap_uses_effective_sizing_pct() {
        let mut cfg = doc_example();
        cfg.sizing.per_position_pct = 0.6;
        cfg.sizing.max_position_pct = Some(0.3);
        assert!(
            validate_config(&cfg).is_ok(),
            "子仓上限 0.3 为有效比例：0.3×2=0.6 ≤ 0.8"
        );

        cfg.sizing.max_position_pct = Some(0.5);
        let err = validate_config(&cfg).unwrap_err().join("\n");
        assert!(err.contains("max_total_notional"), "{err}");
    }

    #[test]
    fn max_position_pct_must_be_positive() {
        let mut cfg = doc_example();
        cfg.sizing.max_position_pct = Some(0.0);
        let err = validate_config(&cfg).unwrap_err().join("\n");
        assert!(err.contains("max_position_pct"), "{err}");
    }

    #[test]
    fn macd_cross_text_rules_are_validated() {
        let mut cfg = doc_example();
        cfg.exit.overrides.insert(
            "ETHUSDT".into(),
            vec![Rule {
                ind: "macd_cross".into(),
                op: RuleOp::Eq,
                value: RuleValue::Text("bearish".into()),
            }],
        );
        assert!(validate_config(&cfg).is_ok(), "bearish 必须合法");
        cfg.exit.overrides.insert(
            "ETHUSDT".into(),
            vec![Rule {
                ind: "macd_cross".into(),
                op: RuleOp::Eq,
                value: RuleValue::Text("sideways".into()),
            }],
        );
        let err = validate_config(&cfg).unwrap_err().join("\n");
        assert!(err.contains("sideways"), "{err}");
    }

    #[test]
    fn rule_summary_is_deterministic() {
        let a = resolved_rule_summary(&doc_example());
        let b = resolved_rule_summary(&doc_example());
        assert_eq!(a, b, "同一配置摘要必须逐字节一致");
        assert!(
            a.contains("BTCUSDT") && a.contains("ETHUSDT") && a.contains("adx"),
            "{a}"
        );
    }

    #[test]
    fn bottom_k_must_be_zero_this_milestone() {
        let mut cfg = doc_example();
        cfg.rebalance.as_mut().unwrap().bottom_k = 1;
        let err = validate_config(&cfg).unwrap_err().join("\n");
        assert!(err.contains("bottom_k"), "{err}");
    }
}
