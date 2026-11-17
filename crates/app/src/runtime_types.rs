//! runtime 共享类型 —— 命令面 / 看门狗 / 对账比对原语（退役方案 R1 抽离，
//! 见 `docs/exchange-spi-retirement.md` §5）。旧 `runtime_actor.rs` 与
//! `spi_live_actor.rs` **共用同一套**这些定义；R4 删除旧 actor 模块后本模块
//! 为唯一宿主。R1 为纯移动：字段、注释、测试断言逐字保留自原模块。

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use strategy_core::order::OrderState;
use strategy_core::risk::RiskControlConfig;
use strategy_engine::strategy::port::CrossSectionalStrategyPort;
#[cfg(test)]
use strategy_positions::Side as PositionSide;
#[cfg(test)]
use strategy_signals::gateway::RemotePosition;
use strategy_signals::gateway::{AsyncOrderGateway, OrderPlace};
use tokio::sync::mpsc;

/// 实例静态规格（监督重启时复用）
#[derive(Clone)]
pub struct RuntimeSpec {
    /// 进入 CSV 路径与 position_id 的运行时名
    pub name: String,
    /// 策略类型名（进入 signal_id）
    pub strategy_type: String,
    pub symbol: String,
    /// 行键命名空间键（§13-5c v2.9；构造点由 `spi_backtest::venue_key_for`
    /// 按 exchange+contract 派生，进 signal_id 哈希；wire 帧仍裸 symbol）
    pub venue_key: String,
    pub mode: String,
    pub state_dir: PathBuf,
    /// 1m K 线 CSV 根目录（live 预热打底 + 闭合帧实时追加）
    pub kline_data_dir: PathBuf,
    pub warmup_bars: usize,
    /// 状态定时全 Store flush 间隔（S6）；独立于 1s 到期重试 ticker。
    /// 0 视为缺省 60s（防御配置误填导致 interval panic）。
    pub state_flush_interval: Duration,
    /// 在途看门狗：意图创建后多少秒内不主动查（给 order_event 推送留宽限）
    pub poll_grace_secs: u64,
    /// 在途看门狗：两次主动查询的最小间隔（去重，秒）
    pub poll_interval_secs: u64,
    /// 远程仓位对账间隔（秒）：每 N 秒 get_positions 比对交易所侧真实仓位
    pub position_check_interval_secs: u64,
    /// 期货合约参数（CTP 路径）：驱动收盘前/开盘后禁交易窗口门控，语义与
    /// SPI 侧 `ExchangeRunner::on_bar` 的门控步一致（`engine/src/core/runtime.rs`
    /// 的 `aggregate_and_gate`）。None（crypto/UTC 网格）时门控关闭。
    pub contract: Option<strategy_core::contract::ContractSpec>,
    /// 统一风控配置（顶层 risk 节）：策略未主动平仓时按 `RiskController::check_exit`
    /// 同源规则兜底（`core/src/risk.rs:103-106`）。
    pub risk: RiskControlConfig,
}

/// 看门狗生产默认：提交宽限 60s / 查询最小间隔 60s
pub const POLL_GRACE_SECS: u64 = 60;
/// 看门狗生产默认：查询最小间隔 60s
pub const POLL_INTERVAL_SECS: u64 = 60;

/// 远程仓位对账默认间隔（每分钟对每个代币查一次 get_positions）
pub const POSITION_CHECK_INTERVAL_SECS: u64 = 60;

/// 两数量是否在相对容差内相等（1e-9，对极小量给绝对下限）。
#[cfg(test)]
fn qty_close(a: f64, b: f64) -> bool {
    let tol = 1e-9_f64.max(1e-9 * b.abs());
    (a - b).abs() <= tol
}

/// 本地仓是否与远程仓一致：存在 instrument 相同、方向相同、数量容差内
/// 相等的远程元素。远程为空（已平仓）或数量不符（小仓可能不是本策略的）
/// 均判 false。
/// 远程仓位三要素比对（阶段 5b 起两条 actor 共用——pub(crate) 仅为
/// `spi_live_actor` 复用，行为零变化）。
#[cfg(test)]
pub(crate) fn remote_matches_local(
    instrument: &str,
    side: PositionSide,
    remaining_size: f64,
    remote: &[RemotePosition],
) -> bool {
    let want_side = match side {
        PositionSide::Long => "BUY",
        PositionSide::Short => "SELL",
    };
    remote.iter().any(|r| {
        // deleted=true 是服务端当日已平仓行：必须排除，否则会把幽灵仓误判为存在
        !r.deleted
            && r.instrument == instrument
            && r.side.eq_ignore_ascii_case(want_side)
            && qty_close(r.quantity, remaining_size)
    })
}

/// 订单回报命令负载（阶段 4 v2.7：增 `fee` 后变体达 205 字节，按
/// clippy `large_enum_variant` 建议装箱为结构体——Reconcile/Shutdown
/// 保持零大小，命令枚举本体回归指针宽度）。
#[derive(Debug)]
pub struct OrderUpdateCmd {
    pub order_id: String,
    pub exchange_order_id: Option<String>,
    pub state: OrderState,
    pub filled_size: f64,
    pub avg_price: f64,
    pub filled_quantity_raw: Option<String>,
    /// 帧内 signal_id：回报早于 ack/绑定时按它找回信号
    pub signal_id: Option<String>,
    /// Rejected 帧交易所原文（识别 ReduceOnly 拒单→外部已平仓对账）
    pub message: Option<String>,
    /// 实际委托要素（受理回写审计）
    pub place: Option<OrderPlace>,
    /// 回报帧真实手续费（阶段 4 v2.7，§13-4「FillEvent.fee 不再恒 0」）
    pub fee: Option<f64>,
    pub event_time: DateTime<Utc>,
}

/// 外部命令（仅订单回报 / 重连对账 / 关闭；其余全部在 bar 驱动内完成）
#[derive(Debug)]
pub enum RuntimeCommand {
    OrderUpdate(Box<OrderUpdateCmd>),
    /// 交易 WS（重）连成功：在途信号先查后发对账
    Reconcile,
    /// 优雅关闭：处理完箱内命令后 flush 快照退出
    Shutdown,
}

#[derive(Clone)]
pub struct RuntimeHandle {
    tx: mpsc::Sender<RuntimeCommand>,
}

/// live actor 命令通道投递失败的统一文案：点名通道 + 点名具体命令。
///
/// 刻意由三处共用一个函数：文案格式不会各写各的而漂移，测试也只需钉这一处
/// （上一版让测试用 `include_str!` 扫源码，结果把测试自己的字面量也数了进去——
/// 自引用假失败，故改为跑真路径）。`RuntimeActor` 已随 R4 退役删除，
/// 运行时文案不得再引用不存在的组件名（`docs/exchange-spi-retirement.md` §5.1 零残留）。
fn channel_closed(cmd: &str, e: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!("live actor 命令通道已关闭（{cmd} 未投递）: {e}")
}

impl RuntimeHandle {
    #[allow(clippy::too_many_arguments)]
    pub async fn order_update(
        &self,
        order_id: String,
        exchange_order_id: Option<String>,
        state: OrderState,
        filled_size: f64,
        avg_price: f64,
        filled_quantity_raw: Option<String>,
        signal_id: Option<String>,
        message: Option<String>,
        place: Option<OrderPlace>,
        fee: Option<f64>,
        event_time: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::OrderUpdate(Box::new(OrderUpdateCmd {
                order_id,
                exchange_order_id,
                state,
                filled_size,
                avg_price,
                filled_quantity_raw,
                signal_id,
                message,
                place,
                fee,
                event_time,
            })))
            .await
            .map_err(|e| channel_closed("OrderUpdate", e))
    }

    pub async fn reconcile(&self) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::Reconcile)
            .await
            .map_err(|e| channel_closed("Reconcile", e))
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.tx
            .send(RuntimeCommand::Shutdown)
            .await
            .map_err(|e| channel_closed("Shutdown", e))
    }

    /// 测试用：直接包裹一个命令发送端
    #[cfg(test)]
    pub(crate) fn for_test(tx: mpsc::Sender<RuntimeCommand>) -> Self {
        Self { tx }
    }

    /// 阶段 5b（v2.11）：从命令发送端构造句柄——`spi_live_actor` 复用
    /// 本模块的**命令面**（RuntimeCommand/RuntimeHandle/RouteBind 原样），
    /// T7 路由与优雅关停代码因此对新旧两条 actor 同形。
    pub fn from_command_tx(tx: mpsc::Sender<RuntimeCommand>) -> Self {
        Self { tx }
    }
}

/// ack 后向 T7 注册 order_id → 本 Actor 的精确路由（B8）
pub struct RouteBind {
    pub order_id: String,
    pub actor: RuntimeHandle,
}

/// 监督重启工厂：每代产出全新网关与策略（内核由 Actor 自建）
pub type RuntimeFactory = Box<
    dyn FnMut() -> (
            Box<dyn AsyncOrderGateway>,
            Box<dyn CrossSectionalStrategyPort>,
        ) + Send,
>;

/// 在途看门狗轮询状态：signal_id → 上次主动查询时刻（内存态，每代重建）。
/// 阶段 5b 起 `spi_live_actor` 共用（类型 `pub` 因其为 engine 内核 pub 字段
/// 类型；方法保持 crate 内可见——逻辑零改动）。
#[derive(Default)]
pub struct PollWatch {
    last_poll: HashMap<String, DateTime<Utc>>,
}

impl PollWatch {
    /// 本轮是否应查询该单：距意图创建已过宽限，且距上次查询 ≥ min_interval。
    pub(crate) fn is_due(
        &self,
        signal_id: &str,
        created: DateTime<Utc>,
        now: DateTime<Utc>,
        grace: chrono::Duration,
        min_interval: chrono::Duration,
    ) -> bool {
        if now - created < grace {
            return false;
        }
        if let Some(t) = self.last_poll.get(signal_id) {
            if now - *t < min_interval {
                return false;
            }
        }
        true
    }

    pub(crate) fn mark(&mut self, signal_id: &str, now: DateTime<Utc>) {
        self.last_poll.insert(signal_id.into(), now);
    }
}

#[cfg(test)]
mod match_tests {
    use super::*;
    use strategy_signals::gateway::RemotePosition;

    fn rp(instrument: &str, side: &str, quantity: f64, deleted: bool) -> RemotePosition {
        RemotePosition {
            instrument: instrument.into(),
            side: side.into(),
            quantity,
            avg_price: 0.0,
            deleted,
        }
    }

    #[test]
    fn matched_when_instrument_side_and_quantity_align() {
        let remote = [rp("BTCUSDT", "SELL", 0.002, false)];
        assert!(remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &remote
        ));
        // side 大小写容忍
        let remote = [rp("BTCUSDT", "sell", 0.002, false)];
        assert!(remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &remote
        ));
    }

    #[test]
    fn deleted_true_means_position_closed_even_if_other_fields_match() {
        // 实盘 bug：服务端元素 quantity/side/instrument 全一致但 deleted=true
        // （当日已平仓行），不得判匹配
        let remote = [rp("BTCUSDT", "SELL", 0.002, true)];
        assert!(!remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &remote
        ));
        // 已平行与活跃行同时存在：以活跃行为准
        let remote = [
            rp("BTCUSDT", "SELL", 0.002, true),
            rp("BTCUSDT", "SELL", 0.002, false),
        ];
        assert!(remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &remote
        ));
    }

    #[test]
    fn empty_remote_means_position_closed() {
        assert!(!remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &[]
        ));
    }

    #[test]
    fn smaller_remote_quantity_means_closed() {
        // 远程 1、本地 2：数量不符 → 已平仓（小仓可能不是本策略的）
        let remote = [rp("BTCUSDT", "SELL", 1.0, false)];
        assert!(!remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            2.0,
            &remote
        ));
    }

    #[test]
    fn opposite_side_or_other_instrument_does_not_match() {
        let remote = [
            rp("BTCUSDT", "BUY", 0.002, false),
            rp("ETHUSDT", "SELL", 0.002, false),
        ];
        assert!(!remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            0.002,
            &remote
        ));
    }

    #[test]
    fn float_tolerance_does_not_reject_near_equal_quantity() {
        let remote = [rp("BTCUSDT", "SELL", 2143.0000000001, false)];
        assert!(remote_matches_local(
            "BTCUSDT",
            strategy_positions::Side::Short,
            2143.0,
            &remote
        ));
    }
}

#[cfg(test)]
mod handle_error_wording_tests {
    //! 投递失败文案的钉死案（决策「把那三处报错字符串也改掉」）。
    //!
    //! 为什么值得单钉：`RuntimeActor` 已随 R4 退役删除，日志里留着这个名字会让运维
    //! 去 grep 一个不存在的组件；而文案本身是**排障线索**，退化成"某某已关闭"这种
    //! 不含通道与命令名的泛词，就等于把三次故障压成一条无信息日志。
    //! 故这里钉三件事：点名 live actor 命令通道、点名具体命令、不得再出现已删组件名。
    use super::*;

    /// 接收端已 drop：任何投递必走 `map_err` 分支。
    fn closed_handle() -> RuntimeHandle {
        let (tx, rx) = mpsc::channel::<RuntimeCommand>(1);
        drop(rx);
        RuntimeHandle::for_test(tx)
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime")
            .block_on(f)
    }

    #[test]
    fn reconcile_failure_names_channel_and_command() {
        let err = block_on(closed_handle().reconcile())
            .unwrap_err()
            .to_string();
        assert!(err.contains("命令通道已关闭"), "须点名通道：{err}");
        assert!(err.contains("Reconcile 未投递"), "须点名命令：{err}");
        assert!(
            !err.contains("RuntimeActor"),
            "不得再引用已随 R4 删除的组件名：{err}"
        );
    }

    #[test]
    fn shutdown_failure_names_channel_and_command() {
        let err = block_on(closed_handle().shutdown())
            .unwrap_err()
            .to_string();
        assert!(err.contains("命令通道已关闭"), "须点名通道：{err}");
        assert!(err.contains("Shutdown 未投递"), "须点名命令：{err}");
        assert!(!err.contains("RuntimeActor"), "不得引用已删组件名：{err}");
    }

    #[test]
    fn order_update_failure_names_channel_and_command() {
        let err = block_on(closed_handle().order_update(
            "o-1".into(),
            None,
            OrderState::Ack,
            0.0,
            0.0,
            None,
            Some("sig-1".into()),
            None,
            None,
            None,
            chrono::Utc::now(),
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains("命令通道已关闭"), "须点名通道：{err}");
        assert!(err.contains("OrderUpdate 未投递"), "须点名命令：{err}");
        assert!(!err.contains("RuntimeActor"), "不得引用已删组件名：{err}");
    }

    #[test]
    fn shared_message_format_covers_all_three_commands() {
        // 三处共用 `channel_closed` ⇒ 钉格式一处生效三处；跑真函数而非扫源码，
        // 避免测试把自己的字面量数进去（上一版的自引用假失败）。
        for cmd in ["OrderUpdate", "Reconcile", "Shutdown"] {
            let msg = channel_closed(cmd, "channel closed").to_string();
            assert_eq!(
                msg,
                format!("live actor 命令通道已关闭（{cmd} 未投递）: channel closed"),
                "文案格式必须统一且点名命令"
            );
            assert!(!msg.contains("RuntimeActor"), "不得引用已删组件名：{msg}");
        }
    }
}
