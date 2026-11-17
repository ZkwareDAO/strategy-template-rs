//! 标准行情事件与收线状态标记。
//!
//! 历史上曾把两个不同层次的「完整」混在一起（迁移前实测）：
//! - 单根 1m 是否收线：WS `is_final` → `Candle.is_closed`，CSV 恒 `true`；
//! - 大周期桶是否收满：旧 `Feed::ingest`（已随 D1b 删除）里 `merge`/`merge_ctp`/
//!   `merge_d1` 返回 bool。
//!
//! 现在两者分层明确：**单根 1m 的收线由源侧给出**（`Bar::from_candle` 读
//! `Candle::is_closed`，回测 CSV 恒 `true`），engine 不做通用帧解码——原
//! `BarDecoder` / `RawFrame` 端口因生产链路零消费已退役删除；本模块只负责
//! **事件形状**（[`Bar`] / [`MarketEvent`]）与聚合层的收桶判定分界（见
//! [`crate::core::aggregation`]）。

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::error::{Result, SpiError};
use crate::core::identity::InstrumentId;
use crate::core::session::{SessionGap, TradingDay};
use crate::core::subscription::RollEvent;

/// 金额。生产实现建议换成 `rust_decimal::Decimal`；此处保持与现有
/// `BacktestBroker`/`PositionState` 的 f64 口径一致，降低接入成本。
pub type Money = f64;

/// 时间框架（分钟数）。
///
/// engine 自带定义而非直接复用 `core::candle::Timeframe`：后者未实现
/// serde，而 `Subscription` / 配置等跨边界结构需要序列化。二者通过
/// `From` 无损互转，迁移期可逐步替换。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timeframe(pub u32);

impl Timeframe {
    pub const M1: Timeframe = Timeframe(1);
    pub const M15: Timeframe = Timeframe(15);
    pub const M30: Timeframe = Timeframe(30);
    pub const H1: Timeframe = Timeframe(60);
    pub const H2: Timeframe = Timeframe(120);
    pub const H4: Timeframe = Timeframe(240);
    pub const D1: Timeframe = Timeframe(1440);

    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim().to_ascii_lowercase();
        let (num, unit) = s.split_at(
            s.len()
                .checked_sub(1)
                .ok_or_else(|| SpiError::Config(format!("非法时间框架: {s}")))?,
        );
        let n: u32 = num
            .parse()
            .map_err(|_| SpiError::Config(format!("非法时间框架: {s}")))?;
        let minutes = match unit {
            "m" => Some(n),
            "h" => n.checked_mul(60),
            "d" => n.checked_mul(1440),
            "w" => n.checked_mul(10080),
            _ => return Err(SpiError::Config(format!("非法时间框架单位: {s}"))),
        }
        .ok_or_else(|| SpiError::Config(format!("时间框架溢出: {s}")))?;
        if minutes == 0 {
            return Err(SpiError::Config("时间框架不能为 0".into()));
        }
        Ok(Timeframe(minutes))
    }
}

impl fmt::Display for Timeframe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let m = self.0;
        if m.is_multiple_of(1440) {
            write!(f, "{}d", m / 1440)
        } else if m.is_multiple_of(60) {
            write!(f, "{}h", m / 60)
        } else {
            write!(f, "{}m", m)
        }
    }
}

impl From<strategy_core::candle::Timeframe> for Timeframe {
    fn from(v: strategy_core::candle::Timeframe) -> Self {
        Timeframe(v.0)
    }
}

impl From<Timeframe> for strategy_core::candle::Timeframe {
    fn from(v: Timeframe) -> Self {
        strategy_core::candle::Timeframe(v.0)
    }
}

/// 单根 1m 的收线状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BarFinality {
    /// 已闭合，可以进入聚合与策略。
    Final,
    /// 形成中快照，只能更新最新价 / 触发风控，绝不驱动策略。
    Provisional,
    /// 上游未给出收线标记时的占位。**当前生产链路既不产生也不匹配它**：
    /// `Bar::from_candle` 按 `Candle::is_closed` 直接定 `Final`/`Provisional`
    /// （engine 不做通用帧解码——原 `BarDecoder::infer_finality` 推断路径已随该
    /// 端口退役删除，见 `lib.rs` 不变量 3）。
    /// 保留此变体的唯一理由是：它是本库 `pub enum` 的公开取值，删除属破坏性
    /// API 变更（模板仓外部使用者可能匹配）；**不是**因为落盘兼容——实测本仓
    /// 没有任何持久化模型注册 `Bar`/`BarFinality`。
    Unknown,
}

impl BarFinality {
    pub fn is_final(self) -> bool {
        matches!(self, BarFinality::Final)
    }
}

/// 与交易所无关的精简 K 线。
///
/// 现有 `core::Candle` 的 `quote_volume` / `trade_num` / `active_buy_*`
/// 是币安专有字段，期货只能填 0；这里把它们降级为可选扩展，
/// 并补上换月必需的 `open_interest`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bar {
    pub instrument: InstrumentId,
    pub open_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub finality: BarFinality,
    pub extensions: BarExtensions,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BarExtensions {
    pub quote_volume: Option<f64>,
    pub trade_count: Option<u64>,
    pub taker_buy_volume: Option<f64>,
    /// 持仓量：主力合约判定与换月的输入（期货必需，crypto 可选）
    pub open_interest: Option<f64>,
    /// 结算价（日结算 / 保证金盯市）
    pub settlement_price: Option<f64>,
}

impl Bar {
    pub fn is_final(&self) -> bool {
        self.finality.is_final()
    }

    /// 从现有 `strategy_core::candle::Candle` 桥接（迁移期用）。
    /// `is_closed=false` 映射为 `Provisional`；CSV 路径恒 `true` 即 `Final`。
    pub fn from_candle(instrument: InstrumentId, c: strategy_core::candle::Candle) -> Self {
        Self {
            instrument,
            open_time: c.open_time,
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            volume: c.volume,
            finality: if c.is_closed {
                BarFinality::Final
            } else {
                BarFinality::Provisional
            },
            extensions: BarExtensions {
                quote_volume: Some(c.quote_volume),
                trade_count: Some(c.trade_num),
                taker_buy_volume: Some(c.active_buy_volume),
                open_interest: None,
                settlement_price: None,
            },
        }
    }
}

/// 统一的行情事件流。
///
/// 关键设计：**实盘与回测只在这里分叉**。`BarEventSource` 的实现分别是
/// WS 订阅适配器与历史回放器；引擎只消费 `next_event()`，不关心来源，
/// 因此回测与实盘跑的是同一段主循环。
#[derive(Debug, Clone)]
pub enum MarketEvent {
    Bar(Bar),
    /// 期货「中间断开」：午休、夜盘结束、节假日。聚合器据此决定是否
    /// 立即闭合当前桶（而不是把跨休市的两段时间拼进同一根 K 线）。
    SessionGap(SessionGap),
    /// 交易日切换：驱动今仓 → 昨仓的 rollover。
    DayBoundary(TradingDay),
    /// 主力换月：驱动订阅切换与连续合约拼接。
    Roll(RollEvent),
    /// 重连成功：触发历史补缺。
    Reconnected,
    /// 非阻塞轮询（`try_next_event`）时该源暂无事件。进程内信号，**不上线**、
    /// 不进聚合/决策（runner 直接忽略）；与「源耗尽」（`Ok(None)`）语义严格区分。
    SourceIdle,
}

/// 行情事件源端口（同步拉取，object-safe）。
///
/// 实盘实现内部可用 tokio channel 的 `blocking_recv()` 或 Actor 邮箱桥接；
/// 现链 `app::spi_live_actor` 的 `ChannelBarSource` 正是此形状（把 WS 闭合 1m
/// 转成同步 `MarketEvent` 逐个处理；旧 `RuntimeActor` 已随 R4 退役删除）。
pub trait BarEventSource: Send {
    fn next_event(&mut self) -> Result<Option<MarketEvent>>;

    /// 非阻塞取一个事件：无可用 → `Ok(Some(MarketEvent::SourceIdle))`；
    /// 真耗尽（channel 关闭）→ `Ok(None)`。默认实现回退阻塞式 `next_event`
    /// （回测/CSV 源零改动；仅 live channel 源覆写）。
    fn try_next_event(&mut self) -> Result<Option<MarketEvent>> {
        self.next_event()
    }

    /// 幂等水位：重连补缺或 preload 时，重复帧由数据源负责去重。
    fn watermark(&self) -> Option<DateTime<Utc>> {
        None
    }
}

/// 内存回放源（回测 / 单测用）。生产实现把它换成 CSV / parquet / DB 游标。
#[derive(Debug, Default)]
pub struct VecEventSource {
    events: Vec<MarketEvent>,
    cursor: usize,
}

impl VecEventSource {
    pub fn new(events: Vec<MarketEvent>) -> Self {
        Self { events, cursor: 0 }
    }

    pub fn from_bars(bars: impl IntoIterator<Item = Bar>) -> Self {
        Self::new(bars.into_iter().map(MarketEvent::Bar).collect())
    }
}

impl BarEventSource for VecEventSource {
    fn next_event(&mut self) -> Result<Option<MarketEvent>> {
        let ev = self.events.get(self.cursor).cloned();
        self.cursor += usize::from(ev.is_some());
        Ok(ev)
    }
}

/// 校验：非 Final 的 Bar 不得进入聚合（防御实现错误）。
pub fn require_final(bar: &Bar) -> Result<()> {
    if bar.is_final() {
        Ok(())
    } else {
        Err(SpiError::BarNotFinal {
            instrument: bar.instrument.to_string(),
            open_time: bar.open_time,
        })
    }
}
