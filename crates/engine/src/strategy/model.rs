//! 横截面策略 Store 模型（PR-1）。
//!
//! `SubPosition` 子仓位 5 态生命周期、`StrategyPortfolio` 组合账本、
//! `MasterPositionView` 派生聚合视图（只读，不落表）。
//!
//! 组合聚合数值口径（spec §数值口径）：**所有 f64 求和必须先按 instrument
//! `namespaced_key()` 字典序排序**——同一组子仓位以不同插入顺序构造时，
//! `MasterPositionView` 必须逐位一致。`gross_notional` 用**成本价**名义
//! （`entry_price * qty * multiplier`，L3 百分比分母）。
//!
//! `SubPosition` / `StrategyPortfolio` 走既有 `strategy_store::Store` 泛型
//! `ModelTable`；`MasterPositionView` 是派生视图，不落表。

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use strategy_core::signal::Side;
use strategy_store::Model;

use crate::core::identity::InstrumentId;

/// 子仓位 5 态生命周期。
///
/// 语义分层：`PendingOpen`/`OpenRequested` = 下单未成交（否给位）；
/// `Active` = 持仓；`CloseRequested` = 平仓在途；`Closed` = 终态
/// （跨日 flush 时物理裁剪，防复活结构依赖 [`Model::is_terminal`]）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubPositionState {
    PendingOpen,
    OpenRequested,
    Active,
    CloseRequested,
    Closed,
}

/// 驱动 [`advance_sub_state`] 的状态机事件。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubPositionEvent {
    /// 交易所已受理（撤单判定由链条上层负责，不在状态位内）
    SubmitAccepted,
    /// 开仓成交
    FillOpen {
        qty: f64,
        price: f64,
        at: DateTime<Utc>,
    },
    /// 策略发起平仓请求
    CloseRequested,
    /// 平仓成交（部分平仓也走此事件）
    FillClose {
        qty: f64,
        realized_pnl: f64,
        at: DateTime<Utc>,
        /// 本次平仓后剩余 qty；`<=1e-12` 视为全平
        remaining_after: f64,
    },
    /// 远程账本归零（对账事实）：drift 修正，未成交/在途平仓统一收敛到终态
    RemoteFlat,
    /// 拒单/撤单：行删除语义，不占状态位
    Rejected,
}

/// 状态机转移。返回 `None` = 无效事件（调用方忽略该事件）。
///
/// 规则（spec PR-1）：
/// - `PendingOpen + SubmitAccepted -> OpenRequested`；`+ Rejected -> None`。
/// - `OpenRequested + FillOpen -> Active`；`+ Rejected -> None`。
/// - `Active + CloseRequested -> CloseRequested`；
///   `Active + FillClose{remaining_after>1e-12} -> Active`（无平仓请求的部分减仓）；
///   `Active + FillClose{remaining_after<=1e-12} -> Closed`（外部全平/对账事实）。
/// - `CloseRequested + FillClose{remaining_after>1e-12} -> CloseRequested`；
///   `CloseRequested + FillClose{remaining_after<=1e-12} -> Closed`
///   （全平即终态，待对账确认由 `RemoteFlat` 幂等处理）。
/// - `RemoteFlat` 是远程账本最终事实：上述任一非终端态 `+ RemoteFlat -> Closed`。
/// - `Closed` 为终态，任何事件返回 `None`。
pub fn advance_sub_state(
    current: SubPositionState,
    event: &SubPositionEvent,
) -> Option<SubPositionState> {
    use SubPositionEvent::*;
    use SubPositionState::*;
    match (current, event) {
        (PendingOpen, SubmitAccepted) => Some(OpenRequested),
        (PendingOpen | OpenRequested, Rejected) => None,
        (OpenRequested, FillOpen { .. }) => Some(Active),
        (Active, SubPositionEvent::CloseRequested) => Some(SubPositionState::CloseRequested),
        (
            Active,
            FillClose {
                remaining_after, ..
            },
        ) => {
            if *remaining_after > 1e-12 {
                Some(Active)
            } else {
                Some(Closed)
            }
        }
        (
            SubPositionState::CloseRequested,
            FillClose {
                remaining_after, ..
            },
        ) => {
            if *remaining_after > 1e-12 {
                Some(SubPositionState::CloseRequested)
            } else {
                Some(Closed)
            }
        }
        (PendingOpen | OpenRequested | Active | SubPositionState::CloseRequested, RemoteFlat) => {
            Some(Closed)
        }
        // 终态/无效组合统一视为无效事件
        _ => None,
    }
}

/// 子仓位（1:1 对应开仓 signal_id）。
///
/// G1：`instrument = 1:1`——同一 `strategy_id + instrument` 至多一个非
/// `Closed` 子仓位。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubPosition {
    /// = 开仓 signal_id（确定性，回测/实盘一致）
    pub id: String,
    pub strategy_id: String,
    /// 所属总仓；旧数据为空串，恢复时由迁移回填
    #[serde(default)]
    pub master_id: String,
    pub instrument: InstrumentId,
    /// `strategy_core::signal::Side`；`qty` 恒为正幅值，方向由 `side` 给出
    pub side: Side,
    pub qty: f64,
    pub entry_price: f64,
    pub stop_price: Option<f64>,
    pub target_weight: f64,
    pub rebalance_epoch: u64,
    pub state: SubPositionState,
    pub opened_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
    /// 减仓/平仓累计已实现盈亏；未平过 = None
    pub realized_pnl: Option<f64>,
}

impl Model for SubPosition {
    const NAME: &'static str = "sub_position";

    fn id(&self) -> &str {
        &self.id
    }

    fn scope(&self) -> Option<&str> {
        Some(&self.strategy_id)
    }

    fn is_terminal(&self) -> bool {
        self.state == SubPositionState::Closed
    }

    fn terminal_at_ms(&self) -> Option<i64> {
        self.closed_at.map(|t| t.timestamp_millis())
    }
}

/// 总仓 3 态生命周期：开仓中 → 平仓中 → 终态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MasterPositionState {
    Open,
    Closing,
    Closed,
}

/// 驱动总仓状态机的事件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MasterPositionEvent {
    /// 策略或 L3 发起平总仓
    StartClosing,
    /// 所有关联子仓已 Closed，且无在途开仓/减仓
    AllSubsClosed,
}

/// 总仓状态机。返回 `None` = 无效事件。
///
/// 不变式：`Closed` 是终态；只有 `AllSubsClosed` 能进入 `Closed`。
pub fn advance_master_state(
    current: MasterPositionState,
    event: MasterPositionEvent,
) -> Option<MasterPositionState> {
    use MasterPositionEvent::*;
    use MasterPositionState::*;
    match (current, event) {
        (Open, StartClosing) => Some(Closing),
        (Open | Closing, AllSubsClosed) => Some(Closed),
        (Closed, _) => None,
        // Closing 不再重复进入 Closing；Open 不会直接收到 AllSubsClosed
        // 之外的事件组合，其余归为无效。
        (Closing, StartClosing) => None,
    }
}

/// 总仓实体：只存生命周期与关联键，不重复存子仓指标。
///
/// 同一策略同一时刻至多一个 `active_master_id`；总仓指标由关联子仓派生。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MasterPosition {
    pub master_id: String,
    pub strategy_id: String,
    /// 仓位归属作用域：`None` = 合并总仓（策略级唯一）；
    /// `Some(instrument_key)` = 逐标的独立总仓，各自只聚合该标的子仓。
    #[serde(default)]
    pub scope: Option<String>,
    pub state: MasterPositionState,
    pub opened_at: Option<DateTime<Utc>>,
    pub closed_at: Option<DateTime<Utc>>,
}

impl Model for MasterPosition {
    const NAME: &'static str = "master_position";

    fn id(&self) -> &str {
        &self.master_id
    }

    fn scope(&self) -> Option<&str> {
        Some(&self.strategy_id)
    }

    fn is_terminal(&self) -> bool {
        self.state == MasterPositionState::Closed
    }

    fn terminal_at_ms(&self) -> Option<i64> {
        self.closed_at.map(|t| t.timestamp_millis())
    }
}

/// 组合账本阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioPhase {
    Running,
    Paused,
    Liquidating,
}

/// 单标的的目标持仓权重。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TargetHolding {
    pub instrument: InstrumentId,
    pub target_weight: f64,
}

/// 逐标的独立总仓时的单标的风控跟踪（L3 回落/止损按标的重置与暂停）。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ScopeRiskState {
    #[serde(default)]
    pub peak_unrealized_pnl: f64,
    #[serde(default)]
    pub trail_armed: bool,
    /// L3 触发并清零后该标的暂停交易，等待重新放行。
    #[serde(default)]
    pub paused: bool,
}

/// 组合账本（每策略一行，`id = strategy_id`）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyPortfolio {
    pub strategy_id: String,
    pub phase: PortfolioPhase,
    /// 当前活动总仓；`None` = 无总仓（空仓/已全部收口）
    #[serde(default)]
    pub active_master_id: Option<String>,
    /// 总仓序号；新总仓 id = `master_{seq:06}`
    #[serde(default)]
    pub master_seq: u64,
    pub nav: f64,
    pub high_watermark: f64,
    pub drawdown: f64,
    pub peak_unrealized_pnl: f64,
    /// v2.4 扩展：回落跟踪是否已激活（重启恢复必需）
    pub trail_armed: bool,
    pub target_holdings: Vec<TargetHolding>,
    pub max_total_notional: f64,
    pub last_rebalance_at: Option<DateTime<Utc>>,
    pub rebalance_epoch: u64,
    /// 距上次再平衡的 base TF 闭合数；随组合行持久化，重启后接续计数
    /// （否则每次重启清零，`every_bars` 长于进程存活期时永远等不到 due）。
    #[serde(default)]
    pub bars_since_rebalance: u64,
    /// `merge_positions=false` 时按 instrument_key 记录各标的的 L3 状态；
    /// 合并总仓沿用工策略级 `peak_unrealized_pnl` / `trail_armed`。
    #[serde(default)]
    pub scope_risk: BTreeMap<String, ScopeRiskState>,
}

impl Model for StrategyPortfolio {
    const NAME: &'static str = "strategy_portfolio";

    fn id(&self) -> &str {
        &self.strategy_id
    }

    fn scope(&self) -> Option<&str> {
        Some(&self.strategy_id)
    }
}

/// `MasterPositionView` 派生视图：跨 `SubPosition` 的只读聚合，不落表。
///
/// 聚合口径（spec §数值口径）：
/// - 只聚合 `state != Closed` 的子仓位；
/// - 先按 `instrument.namespaced_key()` **字典序排序**再 f64 求和——插入顺序
///   无关，数值逐位确定；
/// - `gross_notional` 用**成本名义** `|qty * entry_price * multiplier|`
///   （L3 百分比分母，成本价口径）；
/// - `unrealized_pnl` 用 mark 价与 entry 差乘以 multiplier；mark 缺失用
///   entry 兜底，不产生 NaN；
/// - `avg_entry_price` 按 qty **幅值**加权（`qty` 恒为正，方向由 `side` 给出）。
#[derive(Debug, Clone, PartialEq)]
pub struct MasterPositionView {
    pub strategy_id: String,
    /// 成本名义 `Σ|qty*entry_price*multiplier|`（L3 百分比分母）
    pub gross_notional: f64,
    /// 净名义：带方向 qty（Long 正 / Short 负）的 `Σ qty*entry_price*multiplier`
    pub net_notional: f64,
    /// 按 qty 幅值加权的平均入场价
    pub avg_entry_price: f64,
    /// 已实现盈亏（`realized_pnl` 之 Σ）
    pub realized_pnl: f64,
    /// 未实现盈亏：`Σ signed_qty * (mark - entry) * multiplier`
    pub unrealized_pnl: f64,
    /// 参与聚合的非 Closed 子仓位条数
    pub sub_position_count: u32,
}

/// 计算组合主视图。`marks` / `multipliers` 的键为 instrument `namespaced_key()`。
pub fn master_position_view(
    subs: &[SubPosition],
    marks: &BTreeMap<String, f64>,
    multipliers: &BTreeMap<String, f64>,
) -> MasterPositionView {
    // 只聚合非终态子仓位，并按 namespaced_key 升序排序（f64 求和顺序固定）。
    let mut active: Vec<&SubPosition> = subs
        .iter()
        .filter(|s| s.state != SubPositionState::Closed)
        .collect();
    active.sort_by_key(|s| s.instrument.namespaced_key());

    let mut gross_notional = 0.0_f64;
    let mut net_notional = 0.0_f64;
    let mut avg_num = 0.0_f64;
    let mut avg_den = 0.0_f64;
    let mut realized_pnl = 0.0_f64;
    let mut unrealized_pnl = 0.0_f64;

    for s in &active {
        let key = s.instrument.namespaced_key();
        let signed_qty = match s.side {
            Side::Long => s.qty,
            Side::Short => -s.qty,
        };
        let mult = multipliers.get(&key).copied().unwrap_or(1.0);
        // mark 缺失用 entry 兜底：unrealized 贡献为 0，不产生 NaN。
        let mark = marks.get(&key).copied().unwrap_or(s.entry_price);

        let cost_notional = signed_qty * s.entry_price * mult;
        gross_notional += cost_notional.abs();
        net_notional += cost_notional;
        // avg_entry_price 按 qty **幅值**加权（brief「按 qty 加权」）：
        // qty 恒为正，方向由 side 给出；不得用带方向 qty——混合多空书会
        // 相互抵消 avg_den，产出越界均价甚至 0.0。
        avg_num += s.qty * s.entry_price;
        avg_den += s.qty;
        unrealized_pnl += signed_qty * (mark - s.entry_price) * mult;
        realized_pnl += s.realized_pnl.unwrap_or(0.0);
    }

    // qty 为幅值、恒非负：den=0 只可能出现在空聚合（sub_position_count=0）。
    let avg_entry_price = if avg_den > 1e-12 {
        avg_num / avg_den
    } else {
        0.0
    };

    MasterPositionView {
        strategy_id: active
            .first()
            .map(|s| s.strategy_id.clone())
            .unwrap_or_default(),
        gross_notional,
        net_notional,
        avg_entry_price,
        realized_pnl,
        unrealized_pnl,
        sub_position_count: active.len() as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::identity::ExchangeId;
    use chrono::{DateTime, Utc};

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    fn instr(product: &str) -> InstrumentId {
        InstrumentId::perpetual(&ExchangeId::new("binance"), product)
    }

    fn sub(state: SubPositionState) -> SubPosition {
        SubPosition {
            id: format!("sig_{state:?}").to_ascii_lowercase(),
            strategy_id: "st1".to_string(),
            master_id: "master_000001".to_string(),
            instrument: instr("BTCUSDT"),
            side: Side::Long,
            qty: 1.0,
            entry_price: 100.0,
            stop_price: None,
            target_weight: 0.5,
            rebalance_epoch: 0,
            state,
            opened_at: None,
            closed_at: None,
            realized_pnl: None,
        }
    }

    fn long_sub(id: &str, product: &str, qty: f64, entry: f64) -> SubPosition {
        SubPosition {
            id: id.to_string(),
            strategy_id: "st1".to_string(),
            master_id: "master_000001".to_string(),
            instrument: instr(product),
            side: Side::Long,
            qty,
            entry_price: entry,
            stop_price: None,
            target_weight: 0.5,
            rebalance_epoch: 0,
            state: SubPositionState::Active,
            opened_at: None,
            closed_at: None,
            realized_pnl: None,
        }
    }

    fn short_sub(id: &str, product: &str, qty: f64, entry: f64) -> SubPosition {
        let mut s = long_sub(id, product, qty, entry);
        s.side = Side::Short;
        s
    }

    fn portfolio() -> StrategyPortfolio {
        StrategyPortfolio {
            strategy_id: "st1".to_string(),
            phase: PortfolioPhase::Running,
            active_master_id: None,
            master_seq: 0,
            nav: 1_000_000.0,
            high_watermark: 1_000_000.0,
            drawdown: 0.0,
            peak_unrealized_pnl: 0.0,
            trail_armed: false,
            target_holdings: Vec::new(),
            max_total_notional: 500_000.0,
            last_rebalance_at: None,
            rebalance_epoch: 0,
            bars_since_rebalance: 0,
            scope_risk: BTreeMap::new(),
        }
    }

    #[test]
    fn state_machine_matrix() {
        use SubPositionEvent::*;
        use SubPositionState::*;
        let cases = [
            (PendingOpen, SubmitAccepted, Some(OpenRequested)),
            (PendingOpen, Rejected, None),
            (OpenRequested, Rejected, None),
            (
                OpenRequested,
                FillOpen {
                    qty: 1.0,
                    price: 100.0,
                    at: now(),
                },
                Some(Active),
            ),
            // `CloseRequested` 在 State/Event 两处同名：此处全部显式限定，
            // 避免双重 glob 导入的歧义名（E0659）。
            (
                Active,
                SubPositionEvent::CloseRequested,
                Some(SubPositionState::CloseRequested),
            ),
            (
                Active,
                FillClose {
                    qty: 0.4,
                    realized_pnl: 1.0,
                    at: now(),
                    remaining_after: 0.6,
                },
                Some(Active),
            ),
            (
                Active,
                FillClose {
                    qty: 1.0,
                    realized_pnl: 2.0,
                    at: now(),
                    remaining_after: 0.0,
                },
                Some(Closed),
            ),
            // 远程归零 = 最终事实（drift 修正）
            (Active, RemoteFlat, Some(Closed)),
            // 未成交但远程已无仓（拒单/撤单后对账）
            (OpenRequested, RemoteFlat, Some(Closed)),
            (
                SubPositionState::CloseRequested,
                FillClose {
                    qty: 0.4,
                    realized_pnl: 1.0,
                    at: now(),
                    remaining_after: 0.6,
                },
                Some(SubPositionState::CloseRequested),
            ),
            (
                SubPositionState::CloseRequested,
                FillClose {
                    qty: 0.6,
                    realized_pnl: 2.0,
                    at: now(),
                    remaining_after: 0.0,
                },
                Some(Closed),
            ),
            (SubPositionState::CloseRequested, RemoteFlat, Some(Closed)),
            (Closed, SubmitAccepted, None),
        ];
        for (from, ev, want) in cases {
            assert_eq!(advance_sub_state(from, &ev), want, "{from:?} + {ev:?}");
        }
    }

    #[test]
    fn closed_is_terminal_for_store_pruning() {
        use SubPositionState::*;
        assert!(sub(SubPositionState::Closed).is_terminal());
        for s in [PendingOpen, OpenRequested, Active, CloseRequested] {
            assert!(!sub(s).is_terminal(), "{s:?} 不得被跨日裁剪");
        }
    }

    #[test]
    fn master_view_sums_in_namespaced_key_order() {
        // BTC: qty=2, entry=50, mark=60；ETH: qty=1, entry=100, mark=110；mult=1
        // 期望 gross_notional=200、unrealized_pnl=30、avg_entry_price=(100+100)/3
        // 把 subs 顺序反转后再次调用，四个字段必须逐位相等（字典序求和固定）。
        let btc = long_sub("sig_btc", "BTCUSDT", 2.0, 50.0);
        let eth = long_sub("sig_eth", "ETHUSDT", 1.0, 100.0);
        let marks = BTreeMap::from([
            ("binance/crypto_perpetual/BTCUSDT".to_string(), 60.0_f64),
            ("binance/crypto_perpetual/ETHUSDT".to_string(), 110.0_f64),
        ]);
        let mults = BTreeMap::from([
            ("binance/crypto_perpetual/BTCUSDT".to_string(), 1.0_f64),
            ("binance/crypto_perpetual/ETHUSDT".to_string(), 1.0_f64),
        ]);

        let fwd = master_position_view(&[btc.clone(), eth.clone()], &marks, &mults);
        let rev = master_position_view(&[eth, btc], &marks, &mults);

        assert_eq!(fwd.strategy_id, "st1");
        assert_eq!(fwd.gross_notional, 200.0);
        assert_eq!(fwd.net_notional, 200.0);
        assert_eq!(fwd.unrealized_pnl, 30.0);
        assert!((fwd.avg_entry_price - 200.0 / 3.0).abs() < 1e-12);
        assert_eq!(fwd.realized_pnl, 0.0);
        assert_eq!(fwd.sub_position_count, 2);

        // 字典序求和固定：反转输入顺序后聚合字段逐位一致。
        assert_eq!(fwd.gross_notional, rev.gross_notional);
        assert_eq!(fwd.net_notional, rev.net_notional);
        assert_eq!(fwd.unrealized_pnl, rev.unrealized_pnl);
        assert_eq!(fwd.avg_entry_price, rev.avg_entry_price);
        assert_eq!(fwd.realized_pnl, rev.realized_pnl);
        assert_eq!(fwd.sub_position_count, rev.sub_position_count);
    }

    #[test]
    fn master_view_excludes_closed_and_falls_back_to_entry_mark() {
        // 已平子仓位（巨额名义）不得进入聚合
        let closed_btc = SubPosition {
            qty: 1000.0,
            entry_price: 50_000.0,
            state: SubPositionState::Closed,
            ..long_sub("sig_closed", "BTCUSDT", 0.0, 0.0)
        };
        // 无 mark 的子仓位：用 entry 兜底，unrealized 贡献 0，不产生 NaN
        let unmarked_long = long_sub("sig_sol", "SOLUSDT", 3.0, 200.0);
        // 空头：unrealized = -(mark-entry) * qty（多空用同一签名 qty 口径）
        let short_ada = short_sub("sig_ada", "ADAUSDT", 2.0, 1.0);

        let marks = BTreeMap::from([("binance/crypto_perpetual/ADAUSDT".to_string(), 0.9_f64)]);
        let mults = BTreeMap::from([
            ("binance/crypto_perpetual/SOLUSDT".to_string(), 1.0_f64),
            ("binance/crypto_perpetual/ADAUSDT".to_string(), 1.0_f64),
            ("binance/crypto_perpetual/BTCUSDT".to_string(), 1.0_f64),
        ]);

        let view = master_position_view(&[closed_btc, unmarked_long, short_ada], &marks, &mults);
        assert_eq!(view.sub_position_count, 2, "Closed 子仓位排除在聚合外");
        assert_eq!(
            view.gross_notional,
            3.0 * 200.0 + 2.0 * 1.0,
            "SOL 成本 + ADA 成本"
        );
        assert!(
            (view.unrealized_pnl - 0.2).abs() < 1e-12,
            "SOL=entry(0) + ADA 空头 +0.2"
        );
        assert!(view.unrealized_pnl.is_finite(), "mark 缺失不得产生 NaN");
    }

    #[test]
    fn master_view_avg_entry_price_is_magnitude_weighted() {
        // 混合多空书：qty 恒为正幅值，方向由 side 给出；avg_entry_price 必须
        // 按 qty 幅值加权（brief「按 qty 加权」）。
        // long 2×100 + short 1×99 → (2*100 + 1*99)/3 = 299/3 ≈ 99.6667；
        // 带方向加权会得出 (200-99)/(2-1) = 101——落在两个入场价之外，必为错。
        let btc_long = long_sub("sig_btc", "BTCUSDT", 2.0, 100.0);
        let eth_short = short_sub("sig_eth", "ETHUSDT", 1.0, 99.0);
        let marks = BTreeMap::new();
        let mults = BTreeMap::from([
            ("binance/crypto_perpetual/BTCUSDT".to_string(), 1.0_f64),
            ("binance/crypto_perpetual/ETHUSDT".to_string(), 1.0_f64),
        ]);

        let fwd = master_position_view(&[btc_long.clone(), eth_short.clone()], &marks, &mults);
        let rev = master_position_view(&[eth_short, btc_long], &marks, &mults);

        assert!(
            (fwd.avg_entry_price - 299.0 / 3.0).abs() < 1e-12,
            "avg_entry_price={} 应≈99.6667（幅值加权）",
            fwd.avg_entry_price
        );
        assert!(
            fwd.avg_entry_price > 99.0 && fwd.avg_entry_price < 100.0,
            "avg 必须落在两个入场价之间，实际 {}",
            fwd.avg_entry_price
        );
        // 字典序求和固定仍成立；net_notional 保持带方向（净名义），与 avg 口径独立。
        assert_eq!(fwd.net_notional, 200.0 - 99.0);
        assert_eq!(fwd.avg_entry_price, rev.avg_entry_price);
    }

    #[test]
    fn store_roundtrip_and_recover() {
        use SubPositionState::*;
        let tmp = tempfile::tempdir().unwrap();
        let clock = std::sync::Arc::new(strategy_store::EpochClock);
        let store = strategy_store::Store::new(tmp.path(), clock.clone());
        store.table::<SubPosition>().upsert(sub(Active));
        store.table::<StrategyPortfolio>().upsert(portfolio());
        store.flush().unwrap();
        let recovered = strategy_store::Store::new(tmp.path(), clock.clone());
        assert_eq!(recovered.recover::<SubPosition>().unwrap(), 1);
        assert_eq!(recovered.recover::<StrategyPortfolio>().unwrap(), 1);
        // 同日终态属于「当天终态 + 活行」快照：同日重启应一并恢复。
        // 跨日后旧终态不复活由 store 的 terminal_pruning 集成测试锁死。
        store.table::<SubPosition>().upsert(sub(Closed));
        store.flush().unwrap();
        let recovered2 = strategy_store::Store::new(tmp.path(), clock);
        assert_eq!(recovered2.recover::<SubPosition>().unwrap(), 2);
    }

    #[test]
    fn master_position_model_identity_and_terminal() {
        let master = MasterPosition {
            master_id: "master_000001".into(),
            strategy_id: "st1".into(),
            scope: None,
            state: MasterPositionState::Open,
            opened_at: Some(now()),
            closed_at: None,
        };
        assert_eq!(MasterPosition::NAME, "master_position");
        assert_eq!(master.id(), "master_000001");
        assert_eq!(master.scope(), Some("st1"));
        assert!(!master.is_terminal());
        let closed = MasterPosition {
            state: MasterPositionState::Closed,
            ..master
        };
        assert!(closed.is_terminal());
    }

    #[test]
    fn master_state_machine_only_closes_after_all_subs_closed() {
        use MasterPositionEvent::*;
        use MasterPositionState::*;
        assert_eq!(advance_master_state(Open, StartClosing), Some(Closing));
        assert_eq!(advance_master_state(Open, AllSubsClosed), Some(Closed));
        assert_eq!(advance_master_state(Closing, AllSubsClosed), Some(Closed));
        assert_eq!(advance_master_state(Closing, StartClosing), None);
        assert_eq!(advance_master_state(Closed, StartClosing), None);
        assert_eq!(advance_master_state(Closed, AllSubsClosed), None);
    }

    #[test]
    fn legacy_sub_position_payload_without_master_id_loads_as_empty() {
        let mut payload = serde_json::to_value(sub(SubPositionState::Active)).unwrap();
        payload.as_object_mut().unwrap().remove("master_id");
        let legacy: SubPosition = serde_json::from_value(payload).unwrap();
        assert_eq!(legacy.master_id, "", "旧 payload 缺 master_id 时默认空串");
    }

    #[test]
    fn legacy_portfolio_payload_without_master_fields_loads_as_empty() {
        let mut payload = serde_json::to_value(portfolio()).unwrap();
        let obj = payload.as_object_mut().unwrap();
        obj.remove("active_master_id");
        obj.remove("master_seq");
        let legacy: StrategyPortfolio = serde_json::from_value(payload).unwrap();
        assert_eq!(legacy.active_master_id, None);
        assert_eq!(legacy.master_seq, 0);
    }
}
