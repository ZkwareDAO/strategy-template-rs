//! entry/exit 规则纯函数求值（PR-2/PR-3，Task 6）。
//!
//! 规则集为 **AND**：单条规则[`rule_matches`] 全命中才进入/触发。
//! 本模块是纯只读求值，无 `now()`、无 I/O、无运行时——只消费
//! [`FeatureMap`]（Task 5 产物）返回决策与原因字符串。
//!
//! [`effective_rules`] 按 `symbol` 解析生效规则（`overrides` ASCII
//! 大小写不敏感、整组替换 `default`），[`entry_eligible`] / [`exit_triggered`]
//! 输出确定性布尔 + 原因（缺特征不伪造 0 / NaN）。

use std::cmp::Ordering;

use super::config::{Rule, RuleOp, RuleSet, RuleValue};
use super::features::FeatureMap;

/// `overrides` 命中 `symbol`（ASCII 大小写不敏感）则整组替换 `default`，
/// 否则用 `default`。未覆盖符号与本符号表内不同命名情况保持确定性。
pub fn effective_rules<'a>(set: &'a RuleSet, symbol: &str) -> &'a [Rule] {
    set.overrides
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(symbol))
        .map(|(_, rules)| rules.as_slice())
        .unwrap_or(&set.default)
}

/// 全部规则命中返回 `(true, None)`，否则第一个不满足的规则给出
/// `(false, Some(reason))`。缺特征（含 RuleValue/特征类型不匹配）优先报
/// `"missing feature: <ind>"`，数值/文本不满足则报 `"{ind} {op} {value}"`。
pub fn entry_eligible(rules: &[Rule], features: &FeatureMap) -> (bool, Option<String>) {
    for r in rules {
        match rule_matches(r, features) {
            Some(true) => {}
            None => return (false, Some(format!("missing feature: {}", r.ind))),
            Some(false) => return (false, Some(format_rule(r))),
        }
    }
    (true, None)
}

/// 全部规则命中时返回规则摘要（与不命中原因同格式，`"; "` 连接），
/// 否则 `None`。空规则集 → `None`（平仓只由策略 / 组合风控决定）。
pub fn exit_triggered(rules: &[Rule], features: &FeatureMap) -> Option<String> {
    if rules.is_empty() {
        return None;
    }
    for r in rules {
        if rule_matches(r, features) != Some(true) {
            return None;
        }
    }
    Some(rules.iter().map(format_rule).collect::<Vec<_>>().join("; "))
}

/// 单条规则求值：`Some(bool)`；特征不可用（键不在对应表 / RuleValue 类型
/// 与特征不匹配）→ `None`（不可判）。
///
/// 数值特征：`Gt/Lt/Ge/Le/Eq/Ne` 用 `partial_cmp`；任一操作数 NaN 都不命中
/// （含 `Ne`）。文本标签：仅 `Eq/Ne`，ASCII 大小写不敏感；其余操作符不命中。
/// `RuleValue::Number` 查 `values`、`RuleValue::Text` 查 `labels`。
pub fn rule_matches(rule: &Rule, features: &FeatureMap) -> Option<bool> {
    match &rule.value {
        RuleValue::Number(n) => features
            .number(&rule.ind)
            .map(|v| number_compare(v, *n, rule.op)),
        RuleValue::Text(t) => features
            .label(&rule.ind)
            .map(|l| text_compare(l, t, rule.op)),
    }
}

/// 两个 f64 的规则比较；`partial_cmp` 为 `None`（NaN）时一切操作符均不命中。
fn number_compare(value: f64, target: f64, op: RuleOp) -> bool {
    let ord = value.partial_cmp(&target);
    match op {
        RuleOp::Gt => ord == Some(Ordering::Greater),
        RuleOp::Lt => ord == Some(Ordering::Less),
        RuleOp::Ge => matches!(ord, Some(Ordering::Greater | Ordering::Equal)),
        RuleOp::Le => matches!(ord, Some(Ordering::Less | Ordering::Equal)),
        RuleOp::Eq => ord == Some(Ordering::Equal),
        RuleOp::Ne => matches!(ord, Some(Ordering::Less | Ordering::Greater)),
    }
}

/// 文本比较：仅 `Eq/Ne`（ASCII 大小写不敏感）；其余操作符一律不命中。
fn text_compare(label: &str, target: &str, op: RuleOp) -> bool {
    match op {
        RuleOp::Eq => label.eq_ignore_ascii_case(target),
        RuleOp::Ne => !label.eq_ignore_ascii_case(target),
        RuleOp::Gt | RuleOp::Lt | RuleOp::Ge | RuleOp::Le => false,
    }
}

/// `"{ind} {op} {value}"` 确定性格式（数值用 Rust 紧凑表示）。
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
        RuleValue::Number(n) => format!("{} {} {}", r.ind, op, n),
        RuleValue::Text(t) => format!("{} {} {}", r.ind, op, t),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::config::{Rule, RuleOp, RuleSet, RuleValue};
    use crate::strategy::features::FeatureMap;

    fn nrule(ind: &str, op: RuleOp, v: f64) -> Rule {
        Rule {
            ind: ind.into(),
            op,
            value: RuleValue::Number(v),
        }
    }

    fn trule(ind: &str, op: RuleOp, v: &str) -> Rule {
        Rule {
            ind: ind.into(),
            op,
            value: RuleValue::Text(v.into()),
        }
    }

    fn fmap(values: &[(&str, f64)], labels: &[(&str, &str)]) -> FeatureMap {
        FeatureMap {
            values: values.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn ruleset(default: Vec<Rule>, overrides: &[(&str, Vec<Rule>)]) -> RuleSet {
        RuleSet {
            default,
            overrides: overrides
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    #[test]
    fn gt_is_strict_and_gte_includes_boundary() {
        let fm = fmap(&[("adx", 20.0)], &[]);
        assert_eq!(
            rule_matches(&nrule("adx", RuleOp::Gt, 20.0), &fm),
            Some(false)
        );
        assert_eq!(
            rule_matches(&nrule("adx", RuleOp::Ge, 20.0), &fm),
            Some(true)
        );
        assert_eq!(
            rule_matches(&nrule("adx", RuleOp::Lt, 20.0), &fm),
            Some(false)
        );
        assert_eq!(
            rule_matches(&nrule("adx", RuleOp::Le, 20.0), &fm),
            Some(true)
        );
        assert_eq!(
            rule_matches(&nrule("adx", RuleOp::Eq, 20.0), &fm),
            Some(true)
        );
        assert_eq!(
            rule_matches(&nrule("adx", RuleOp::Ne, 20.0), &fm),
            Some(false)
        );
        // NaN 任何比较（含 !=）都不命中
        let nan = fmap(&[("adx", f64::NAN)], &[]);
        assert_eq!(
            rule_matches(&nrule("adx", RuleOp::Ne, 20.0), &nan),
            Some(false)
        );
        assert_eq!(
            rule_matches(&nrule("adx", RuleOp::Ge, 20.0), &nan),
            Some(false)
        );
    }

    #[test]
    fn missing_feature_fails_entry_with_reason() {
        let (ok, reason) =
            entry_eligible(&[nrule("adx", RuleOp::Gt, 20.0)], &FeatureMap::default());
        assert!(!ok);
        assert_eq!(reason.as_deref(), Some("missing feature: adx"));
        // 缺特征优先报 missing：第一条缺失时不再逐个判后续
        let (_, reason) = entry_eligible(
            &[
                nrule("adx", RuleOp::Gt, 20.0),
                nrule("macd", RuleOp::Gt, 1.0),
            ],
            &FeatureMap::default(),
        );
        assert_eq!(reason.as_deref(), Some("missing feature: adx"));
    }

    #[test]
    fn all_rules_must_match() {
        let fm = fmap(&[("adx", 25.0), ("rsi", 80.0)], &[]);
        let rules = vec![
            nrule("adx", RuleOp::Gt, 20.0),
            nrule("rsi", RuleOp::Lt, 70.0),
        ];
        let (ok, reason) = entry_eligible(&rules, &fm);
        assert!(!ok);
        assert_eq!(
            reason.as_deref(),
            Some("rsi < 70"),
            "首个不命中规则按 {{ind}} {{op}} {{value}} 格式"
        );
        // 全部命中 → (true, None)
        let good = fmap(&[("adx", 25.0), ("rsi", 40.0)], &[]);
        assert_eq!(entry_eligible(&rules, &good), (true, None));
    }

    #[test]
    fn overrides_replace_default_wholesale() {
        let default = vec![nrule("adx", RuleOp::Gt, 20.0)];
        let btc = vec![nrule("adx", RuleOp::Gt, 25.0)];
        let rs = ruleset(default.clone(), &[("BTCUSDT", btc.clone())]);
        assert_eq!(effective_rules(&rs, "BTCUSDT"), btc.as_slice());
        assert_eq!(
            effective_rules(&rs, "btcusdt"),
            btc.as_slice(),
            "override 键对 symbol 为 ASCII 大小写不敏感"
        );
        assert_eq!(
            effective_rules(&rs, "ETHUSDT"),
            default.as_slice(),
            "未覆盖用 default"
        );
        assert_eq!(effective_rules(&rs, ""), default.as_slice());
    }

    #[test]
    fn text_equality_for_macd_cross() {
        let fm = fmap(&[], &[("macd_cross", "bearish")]);
        assert_eq!(
            rule_matches(&trule("macd_cross", RuleOp::Eq, "bearish"), &fm),
            Some(true)
        );
        assert_eq!(
            rule_matches(&trule("macd_cross", RuleOp::Eq, "bullish"), &fm),
            Some(false)
        );
        assert_eq!(
            rule_matches(&trule("macd_cross", RuleOp::Ne, "bearish"), &fm),
            Some(false)
        );
        assert_eq!(
            rule_matches(&trule("macd_cross", RuleOp::Ne, "bullish"), &fm),
            Some(true)
        );
        assert_eq!(
            rule_matches(&trule("macd_cross", RuleOp::Eq, "BEARISH"), &fm),
            Some(true),
            "文本比较大小写不敏感"
        );
        // 标签缺失 → None（不可判）
        assert_eq!(
            rule_matches(
                &trule("macd_cross", RuleOp::Eq, "bearish"),
                &FeatureMap::default()
            ),
            None
        );
        // 文本只允许 Eq/Ne；其余操作符不命中（不报 missing）
        assert_eq!(
            rule_matches(&trule("macd_cross", RuleOp::Gt, "bearish"), &fm),
            Some(false)
        );
        // RuleValue 类型不匹配：数值规则查不到标签 → None
        let num_label = Rule {
            ind: "macd_cross".into(),
            op: RuleOp::Eq,
            value: RuleValue::Number(1.0),
        };
        assert_eq!(rule_matches(&num_label, &fm), None);
    }

    #[test]
    fn exit_triggers_only_when_all_rules_match() {
        // 空规则集 → None（平仓只由策略/组合风控决定）
        assert_eq!(exit_triggered(&[], &FeatureMap::default()), None);
        assert_eq!(exit_triggered(&[], &fmap(&[("adx", 30.0)], &[])), None);
        // 单条命中 → 摘要
        let fm = fmap(&[("adx", 10.0)], &[]);
        assert_eq!(
            exit_triggered(&[nrule("adx", RuleOp::Lt, 20.0)], &fm),
            Some("adx < 20".to_string())
        );
        // 两条都命中 → 全量摘要（"; " 连接）；缺一条 / 缺特征 → None
        let rules = vec![
            nrule("adx", RuleOp::Gt, 20.0),
            nrule("rsi", RuleOp::Lt, 90.0),
        ];
        let both = fmap(&[("adx", 30.0), ("rsi", 80.0)], &[]);
        assert_eq!(
            exit_triggered(&rules, &both),
            Some("adx > 20; rsi < 90".to_string())
        );
        let one = fmap(&[("adx", 30.0), ("rsi", 95.0)], &[]);
        assert_eq!(exit_triggered(&rules, &one), None);
        assert_eq!(exit_triggered(&rules, &fmap(&[("adx", 30.0)], &[])), None);
    }
}
