//! trading-service 南向线协议（镜像 trading-core/src/protocol.rs 的
//! SouthCommand / ClientEvent；broker 只保留自己消费的字段）。

use serde::{Deserialize, Serialize};
use strategy_core::order::OrderState;

/// 出站：place_order —— 开仓帧。
/// 身份（strategy_name）不在帧内，由 WS 握手绑定；数量改由服务端按
/// `order_cash / 市价` 换算，故帧里只带名义金额与杠杆。
/// `offset`（阶段 4 v2.7，§13-4）：显式开平标志；**None 时字段整体不出现**
/// ——旧服务端/旧链路帧逐字节不变（§14-3 外部协议升级前向兼容）。
#[derive(Debug, Serialize)]
pub struct PlaceOrderOpen<'a> {
    pub option: &'static str,
    /// 目标交易所名（服务端 registry 键）
    pub exchange: &'a str,
    pub account: &'a str,
    pub instrument: &'a str,
    /// 标的类型：恒 "futures"
    pub instrument_type: &'static str,
    /// long / short（服务端兼容 buy/sell，但我们发规范值）
    pub side: &'a str,
    pub order_type: &'static str,
    /// 每单名义金额（USDT）；**字符串**形态（服务端 decimal 兼容）
    pub order_cash: &'a str,
    /// 杠杆倍数（>1 时服务端先 set_leverage）
    pub leverage: u32,
    pub signal_id: &'a str,
    /// 开平标志（`open`；SPI live 桥显式给出时携带）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<&'a str>,
}

/// 出站：place_order —— 平仓帧。
/// `quantity` 必须精确等于开仓成交回报的 filled_quantity，且 reduce_only 恒 true。
/// `offset`（阶段 4 v2.7）：`close` / `close_today` / `close_yesterday`——
/// 「实盘订单帧带平今/平昨」验收字段的落点；None ⇒ 旧服务端隐式判定。
#[derive(Debug, Serialize)]
pub struct PlaceOrderClose<'a> {
    pub option: &'static str,
    pub exchange: &'a str,
    pub account: &'a str,
    pub instrument: &'a str,
    pub instrument_type: &'static str,
    /// 与持仓方向相反
    pub side: &'a str,
    pub order_type: &'static str,
    /// 平仓数量；**字符串**形态，值取自 FILLED 回报的 filled_quantity
    pub quantity: &'a str,
    /// 恒 true
    pub reduce_only: bool,
    pub signal_id: &'a str,
    /// 开平标志（平今/平昨显式化）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<&'a str>,
}

/// 入站：trading-service -> strategy 的统一事件体（ack/order_event/error/positions）。
/// 字段全部宽松（缺失/null 容忍），只解析我们消费的部分。
#[derive(Debug, Deserialize)]
pub struct InEvent {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub client_order_id: Option<String>,
    #[serde(default)]
    pub exchange_order_id: Option<String>,
    #[serde(default)]
    pub strategy_name: Option<String>,
    #[serde(default)]
    pub signal_id: Option<String>,
    #[serde(default)]
    pub reduce_only: Option<bool>,
    #[serde(default)]
    pub filled_price: Option<serde_json::Value>,
    #[serde(default)]
    pub filled_quantity: Option<serde_json::Value>,
    /// 实际委托类型 limit/market（审计）
    #[serde(default)]
    pub order_type: Option<String>,
    /// 实际委托价（市价单为 0）
    #[serde(default)]
    pub order_price: Option<serde_json::Value>,
    /// 实际委托数量（金额单换算结果）
    #[serde(default)]
    pub order_quantity: Option<serde_json::Value>,
    /// 本笔手续费（阶段 4 v2.7，§13-4「FillEvent.fee 不再恒 0」）：
    /// 服务端升级后在 ack/order_event/order 帧回传真实手续费；
    /// 数字或数字字符串（与其余 decimal 字段同宽容口径），缺失容忍。
    #[serde(default)]
    pub fee: Option<serde_json::Value>,
    /// positions：get_positions 应答仓位数组（仅解析 instrument/side/quantity/pos_price）
    #[serde(default)]
    pub positions: Option<Vec<WirePosition>>,
    /// type=kline_1m 的定稿 bar 载荷（trading-core KlineEvent 的宽松镜像）
    #[serde(default)]
    pub kline: Option<WireKline>,
    /// type=kline_1m_batch 的同所批量 bar 载荷
    #[serde(default)]
    pub klines: Option<Vec<WireKline>>,
    /// kline_history 帧回显：查询标的（get_klines 关联键，§10.4）；其他帧省略
    #[serde(default)]
    pub symbol: Option<String>,
    /// kline_history 分页标记：true = 服务端还有更早数据
    #[serde(default)]
    pub has_more: Option<bool>,
    #[serde(default)]
    pub message: Option<String>,
}

/// `kline_1m(_batch)` 帧内 bar：12 个公共强类型字段 + binance extras；
/// decimal 字段数字/字符串双口径（与 f64_or_zero 家族一致），未知键容忍。
#[derive(Debug, Clone, Deserialize)]
pub struct WireKline {
    #[serde(default)]
    pub venue: Option<String>,
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub interval: Option<String>,
    #[serde(default)]
    pub trading_day: Option<u32>,
    /// 桶开始时间（UTC 毫秒，含）
    #[serde(default)]
    pub start_time: Option<i64>,
    #[serde(default)]
    pub end_time: Option<i64>,
    #[serde(default)]
    pub open: Option<serde_json::Value>,
    #[serde(default)]
    pub high: Option<serde_json::Value>,
    #[serde(default)]
    pub low: Option<serde_json::Value>,
    #[serde(default)]
    pub close: Option<serde_json::Value>,
    #[serde(default)]
    pub volume: Option<serde_json::Value>,
    #[serde(default)]
    pub quote_volume: Option<serde_json::Value>,
    // —— binance extras（其余所为 null/缺失，容忍）——
    #[serde(default)]
    pub trade_num: Option<serde_json::Value>,
    #[serde(default)]
    pub active_buy_volume: Option<serde_json::Value>,
    #[serde(default)]
    pub active_buy_quote_volume: Option<serde_json::Value>,
}

/// get_positions 应答元素（宽松解析，只取对账字段）。
#[derive(Debug, Deserialize)]
pub struct WirePosition {
    #[serde(default)]
    pub instrument: String,
    /// 交易所方向原值（大写 BUY/SELL）
    #[serde(default)]
    pub side: String,
    /// 当前持仓量（数字或数字字符串）
    #[serde(default)]
    pub quantity: Option<serde_json::Value>,
    /// 开仓均价
    #[serde(default, rename = "pos_price")]
    pub pos_price: Option<serde_json::Value>,
    /// 服务端存活标记：true=当日已平仓行
    #[serde(default)]
    pub deleted: bool,
}

/// JSON 数字或数字字符串 → f64；其余 0.0（mock/真实服务出参均为数字，字符串为兼容路径）。
pub fn f64_or_zero(v: &serde_json::Value) -> f64 {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0.0)
}

/// 可选 JSON 值 → f64（null/缺失/非法 → 0.0）。
pub fn opt_f64(v: &Option<serde_json::Value>) -> f64 {
    v.as_ref().map_or(0.0, f64_or_zero)
}

/// JSON 值中的数量原始字面量：字符串原样返回；数字取其 JSON 文本
/// （保留服务端精度，平仓帧 quantity 必须与 filled_quantity 逐字一致）。
pub fn number_raw(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// 可选 JSON 值 → 数量原始字面量。
pub fn opt_number_raw(v: &Option<serde_json::Value>) -> Option<String> {
    v.as_ref().and_then(number_raw)
}

/// 服务端状态串 → 本地 OrderState；UNKNOWN/无法识别返回 None（调用方跳过该帧）。
pub fn map_status(raw: &str) -> Option<OrderState> {
    Some(match raw.trim().to_ascii_uppercase().as_str() {
        "NEW" => OrderState::Ack,
        "PLACING" => OrderState::Sent,
        "PARTIAL" => OrderState::PartiallyFilled,
        "FILLED" => OrderState::Filled,
        "CANCELED" => OrderState::Canceled,
        "REJECTED" | "EXPIRED" => OrderState::Rejected,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use strategy_core::order::OrderState;

    #[test]
    fn place_order_open_serializes_target_wire_shape() {
        let cmd = PlaceOrderOpen {
            option: "place_order",
            exchange: "binance",
            account: "test_binance",
            instrument: "BTCUSDT",
            instrument_type: "futures",
            side: "long",
            order_type: "market",
            order_cash: "100",
            leverage: 3,
            signal_id: "sig_1",
            offset: None,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert_eq!(
            json,
            r#"{"option":"place_order","exchange":"binance","account":"test_binance","instrument":"BTCUSDT","instrument_type":"futures","side":"long","order_type":"market","order_cash":"100","leverage":3,"signal_id":"sig_1"}"#,
            "offset=None 时旧帧逐字节不变（前向兼容钉死）"
        );
    }

    #[test]
    fn place_order_close_serializes_target_wire_shape() {
        let cmd = PlaceOrderClose {
            option: "place_order",
            exchange: "binance",
            account: "test_binance",
            instrument: "BTCUSDT",
            instrument_type: "futures",
            side: "short",
            order_type: "market",
            quantity: "0.01",
            reduce_only: true,
            signal_id: "sig_2",
            offset: None,
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert_eq!(
            json,
            r#"{"option":"place_order","exchange":"binance","account":"test_binance","instrument":"BTCUSDT","instrument_type":"futures","side":"short","order_type":"market","quantity":"0.01","reduce_only":true,"signal_id":"sig_2"}"#
        );
    }

    /// 阶段 4 验收「实盘订单帧带平今/平昨」：offset 显式给出时字段落帧。
    #[test]
    fn place_order_frames_carry_offset_when_present() {
        let close = PlaceOrderClose {
            option: "place_order",
            exchange: "ctp",
            account: "acc",
            instrument: "CU2601",
            instrument_type: "futures",
            side: "short",
            order_type: "market",
            quantity: "2",
            reduce_only: true,
            signal_id: "sig_3",
            offset: Some("close_today"),
        };
        let json = serde_json::to_string(&close).unwrap();
        assert!(json.contains(r#""offset":"close_today""#), "实际: {json}");
        let open = PlaceOrderOpen {
            option: "place_order",
            exchange: "ctp",
            account: "acc",
            instrument: "CU2601",
            instrument_type: "futures",
            side: "long",
            order_type: "market",
            order_cash: "100000",
            leverage: 1,
            signal_id: "sig_4",
            offset: Some("open"),
        };
        let json = serde_json::to_string(&open).unwrap();
        assert!(json.contains(r#""offset":"open""#), "实际: {json}");
    }

    #[test]
    fn in_event_lenient_on_missing_and_aliases() {
        let ev: InEvent = serde_json::from_str(r#"{"type":"order_event"}"#).unwrap();
        assert_eq!(ev.kind, "order_event");
        assert!(ev.status.is_none());
        // 真实服务 decimal 出参是 JSON 数字；历史/兼容路径可能是字符串
        let ev: InEvent = serde_json::from_str(
            r#"{"type":"order_event","status":"FILLED","client_order_id":"0xab",
                "filled_price":"80000.5","filled_quantity":2}"#,
        )
        .unwrap();
        assert_eq!(opt_f64(&ev.filled_price), 80000.5);
        assert_eq!(opt_f64(&ev.filled_quantity), 2.0);
        assert!(ev.fee.is_none(), "旧服务端无 fee 字段：缺失容忍");
        // 阶段 4：fee 数字/数字字符串双口径
        let ev: InEvent =
            serde_json::from_str(r#"{"type":"order_event","status":"FILLED","fee":3.25}"#).unwrap();
        assert_eq!(opt_f64(&ev.fee), 3.25);
        let ev: InEvent = serde_json::from_str(r#"{"type":"order_event","fee":"7.5"}"#).unwrap();
        assert_eq!(opt_f64(&ev.fee), 7.5);
    }

    #[test]
    fn numeric_coercion_accepts_number_string_else_zero() {
        assert_eq!(f64_or_zero(&serde_json::json!(1.5)), 1.5);
        assert_eq!(f64_or_zero(&serde_json::json!("1.5")), 1.5);
        assert_eq!(f64_or_zero(&serde_json::json!("nope")), 0.0);
        assert_eq!(opt_f64(&None), 0.0);
        assert_eq!(opt_f64(&Some(serde_json::Value::Null)), 0.0);
    }

    #[test]
    fn status_mapping_covers_real_states() {
        assert_eq!(map_status("NEW"), Some(OrderState::Ack));
        assert_eq!(map_status("new"), Some(OrderState::Ack));
        assert_eq!(map_status("PLACING"), Some(OrderState::Sent));
        assert_eq!(map_status("PARTIAL"), Some(OrderState::PartiallyFilled));
        assert_eq!(map_status("FILLED"), Some(OrderState::Filled));
        assert_eq!(map_status("CANCELED"), Some(OrderState::Canceled));
        assert_eq!(map_status("REJECTED"), Some(OrderState::Rejected));
        assert_eq!(map_status("EXPIRED"), Some(OrderState::Rejected));
        assert_eq!(map_status("UNKNOWN"), None);
        assert_eq!(map_status("garbage"), None);
    }
}
