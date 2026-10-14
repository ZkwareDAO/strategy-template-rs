//! 信号生命周期状态机（设计文档 §5.2.2）
//!
//! 信号是策略的"意图记录"，与订单是**两个独立状态机**。信号一旦产生即落盘，
//! 后续下单结果只改变其状态：
//!
//! ```text
//! Created → Validated → Submitted → Accepted → Filled
//!                  \          \           \
//!                   Rejected   Failed       Cancelled
//!                              Failed → Submitted（可重试）
//! ```
//!
//! 终态：[`SignalStatus::Rejected`] / [`SignalStatus::Filled`] /
//! [`SignalStatus::Cancelled`]，不可再转移。

use serde::{Deserialize, Serialize};

/// 信号自身生命周期状态（字符串名稳定，随信号当前态行进入 Store 快照 JSON）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalStatus {
    /// 已产生，待验证
    Created,
    /// 验证通过，准备下单
    Validated,
    /// 验证失败（参数非法/风控拦截），不会下单
    Rejected,
    /// 已提交下单（已发往 trading-service，等待确认）
    Submitted,
    /// 下单失败（网络/拒绝）；可重试信号允许 Failed → Submitted
    Failed,
    /// 下单被接受，等待交易所成交
    Accepted,
    /// 已成交（仓位已建立/变更）
    Filled,
    /// 已取消（撤销或超时）
    Cancelled,
}

impl SignalStatus {
    /// CSV/事件存储用稳定字符串
    pub fn as_str(self) -> &'static str {
        match self {
            SignalStatus::Created => "created",
            SignalStatus::Validated => "validated",
            SignalStatus::Rejected => "rejected",
            SignalStatus::Submitted => "submitted",
            SignalStatus::Failed => "failed",
            SignalStatus::Accepted => "accepted",
            SignalStatus::Filled => "filled",
            SignalStatus::Cancelled => "cancelled",
        }
    }

    /// 是否终态（不可再转移）
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            SignalStatus::Rejected | SignalStatus::Filled | SignalStatus::Cancelled
        )
    }

    /// 重启恢复时需要对账的未完结状态（设计文档 §5.2.5）
    pub fn is_pending(self) -> bool {
        matches!(
            self,
            SignalStatus::Created
                | SignalStatus::Validated
                | SignalStatus::Submitted
                | SignalStatus::Accepted
        )
    }

    /// 严格状态转移表；非法转移返回 false（调用方必须拒绝并暴露错误）
    pub fn can_transit(self, to: SignalStatus) -> bool {
        use SignalStatus::*;
        matches!(
            (self, to),
            (Created, Validated | Rejected)
                | (Validated, Submitted | Rejected)
                // submit 调用边界即失败（WS 不可用/本地拒发），无 order_id：
                // 直接 Failed（设计文档 §5.2.2 表格的落地补充边）
                | (Validated, Failed)
                | (Submitted, Accepted | Failed | Cancelled)
                | (Accepted, Filled | Cancelled)
                | (Failed, Submitted)     // 可重试
                | (Failed, Failed) // 重试再次失败：审计自环（version 递增）
        )
    }
}

impl std::fmt::Display for SignalStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for SignalStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "created" => SignalStatus::Created,
            "validated" => SignalStatus::Validated,
            "rejected" => SignalStatus::Rejected,
            "submitted" => SignalStatus::Submitted,
            "failed" => SignalStatus::Failed,
            "accepted" => SignalStatus::Accepted,
            "filled" => SignalStatus::Filled,
            "cancelled" => SignalStatus::Cancelled,
            other => return Err(format!("未知 SignalStatus: {other}")),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SignalStatus::*;
    use super::*;

    #[test]
    fn happy_path_transitions() {
        assert!(Created.can_transit(Validated));
        assert!(Validated.can_transit(Submitted));
        assert!(Submitted.can_transit(Accepted));
        assert!(Accepted.can_transit(Filled));
    }

    #[test]
    fn rejection_and_failure_edges() {
        assert!(Created.can_transit(Rejected));
        assert!(Validated.can_transit(Rejected));
        assert!(Submitted.can_transit(Failed));
        assert!(Submitted.can_transit(Cancelled));
        assert!(Accepted.can_transit(Cancelled));
    }

    #[test]
    fn validated_can_fail_at_submit_boundary() {
        assert!(Validated.can_transit(Failed));
        assert!(Failed.can_transit(Submitted));
    }

    #[test]
    fn failed_can_retry_to_submitted() {
        assert!(Failed.can_transit(Submitted));
        assert!(!Failed.can_transit(Accepted), "重试必须重新提交");
    }

    #[test]
    fn illegal_transitions_rejected() {
        // 不可跳级
        assert!(!Created.can_transit(Submitted));
        assert!(!Validated.can_transit(Filled));
        assert!(!Submitted.can_transit(Filled));
        // 不可回退
        assert!(!Accepted.can_transit(Submitted));
        assert!(!Filled.can_transit(Accepted));
        // 终态不可出
        for terminal in [Rejected, Filled, Cancelled] {
            for next in [
                Created, Validated, Rejected, Submitted, Failed, Accepted, Filled, Cancelled,
            ] {
                assert!(
                    !terminal.can_transit(next),
                    "{terminal:?} → {next:?} 必须拒绝"
                );
            }
            assert!(terminal.is_terminal());
        }
    }

    #[test]
    fn pending_set_matches_recovery_doc() {
        assert!(Created.is_pending() && Validated.is_pending());
        assert!(Submitted.is_pending() && Accepted.is_pending());
        assert!(!Failed.is_pending() && !Filled.is_pending());
        assert!(!Rejected.is_pending() && !Cancelled.is_pending());
    }

    #[test]
    fn string_roundtrip() {
        for s in [
            Created, Validated, Rejected, Submitted, Failed, Accepted, Filled, Cancelled,
        ] {
            assert_eq!(s.as_str().parse::<SignalStatus>().unwrap(), s);
        }
        assert!("nope".parse::<SignalStatus>().is_err());
    }
}
