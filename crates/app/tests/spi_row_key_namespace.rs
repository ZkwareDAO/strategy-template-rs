//! 阶段 5c（v2.9，§13-5「行键命名空间化 schema 一次迁完」）验收。
//!
//! 钉三件事：
//! 1. **§14-3 跨交易所不覆盖**：同名裸 symbol 在两个 exchange 下，
//!    signal/position/stop/trade 行键互不相同，共享 Store 表可同存两行
//!    （旧裸 symbol 哈希口径下这两行会 `AlreadyExists` 互相吞掉）；
//! 2. **迁移零重写**：行键是不透明 ID、随行持久化——历史行（迁移前
//!    格式）与新行同表共存、各自可按 id 读回，恢复路径无需数据重写
//!    （「schema 集中一次迁完」= 一次格式变更 + opaque id 承接存量）；
//! 3. **单一格式源**：`venue_key_for`（旧 CLI 链）与 `InstrumentId::
//!    namespaced_key()`（engine 链）经 `namespaced_key_of` 同源，
//!    §6.1 文档例串逐字钉死。
//!
//! wire 帧零回归的根据也在本文件：`intent.symbol` 仍是裸挂牌代码，
//! 命名空间只进**行键**哈希（§6.1 两角色分离）。

use chrono::{DateTime, TimeZone, Utc};
use std::sync::Arc;
use strategy_app::config;
use strategy_app::spi_backtest;
use strategy_core::lifecycle::SignalStatus;
use strategy_core::signal::{generate_signal_id, IntentKind, SignalIntent, SignalType};
use strategy_core::Side;
use strategy_positions::manager::stop_order_id;
use strategy_signals::ManagedSignal;
use strategy_store::{EpochClock, Store};

fn utc_ts() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 10, 0, 0, 0).unwrap()
}

fn open_intent(
    strategy_type: &str,
    symbol: &str,
    venue_key: &str,
    event_time: DateTime<Utc>,
) -> SignalIntent {
    SignalIntent {
        id: generate_signal_id(strategy_type, venue_key, event_time, SignalType::Buy),
        strategy_id: strategy_type.into(),
        symbol: symbol.into(),
        kind: IntentKind::Open,
        direction: Side::Long,
        price: 100.0,
        size: 0.0,
        size_raw: None,
        stop_price: Some(50.0),
        position_id: None,
        stop_order_id: None,
        created_event_time: event_time,
        status: SignalStatus::Created,
        offset: None,
    }
}

fn row(intent: SignalIntent, name: &str) -> ManagedSignal {
    ManagedSignal {
        strategy_name: name.into(),
        intent,
        version: 1,
        status: SignalStatus::Created,
        order_id: None,
        attempts: 0,
        next_retry_at: None,
        last_error: None,
        last_failure_kind: None,
        exchange_order_id: None,
        filled_price: None,
        filled_quantity: None,
        filled_quantity_raw: None,
        order_type: None,
        order_price: None,
        order_quantity: None,
    }
}

/// 验收 1（§14-3 核心案）：同名裸 symbol 跨交易所 ⇒ 行键互异、
/// 共享表同存、幂等拒绝仍按同键生效；派生键（stop/trade）随源键。
#[test]
fn same_symbol_two_venues_never_share_store_rows() {
    let ts = utc_ts();
    let i_bin = open_intent("once", "BTCUSDT", "binance/crypto_perpetual/BTCUSDT", ts);
    let i_bit = open_intent("once", "BTCUSDT", "bybit/crypto_perpetual/BTCUSDT", ts);

    // wire 角色：裸挂牌代码不变（帧零回归的根据）
    assert_eq!(i_bin.symbol, "BTCUSDT");
    assert_eq!(i_bit.symbol, "BTCUSDT");
    // 行键角色：不同
    assert_ne!(
        i_bin.id, i_bit.id,
        "同名 symbol 跨交易所必须产生不同行键（§14-3）"
    );
    // 回归钉：迁移前裸 symbol 哈希口径不再可达
    assert_ne!(
        i_bin.id,
        generate_signal_id("once", "BTCUSDT", ts, SignalType::Buy),
        "新口径不得复现旧裸 symbol 行键（否则旧行会被静默复用）"
    );

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::new(dir.path(), Arc::new(EpochClock)));
    let table = store.table::<ManagedSignal>();
    table
        .insert(row(i_bin.clone(), "bin_inst"))
        .expect("binance 行落表");
    table
        .insert(row(i_bit.clone(), "bit_inst"))
        .expect("bybit 同名 symbol 行落同一张共享表——旧口径此处必 AlreadyExists");
    assert_eq!(table.list().len(), 2, "两行共存不被覆盖");
    assert!(table.get(&i_bin.id).is_some() && table.get(&i_bit.id).is_some());
    // 幂等钉：同键重复 insert 仍被拒（命名空间化未削弱去重）
    assert!(
        table.insert(row(i_bin.clone(), "bin_inst")).is_err(),
        "同键重插必须 AlreadyExists（幂等键语义不变）"
    );

    // 派生行键随源键：stop（positions 表）与 trade（报表表）
    assert_ne!(stop_order_id(&i_bin.id), stop_order_id(&i_bit.id));
    assert_ne!(format!("trade_{}", i_bin.id), format!("trade_{}", i_bit.id));
}

/// 验收 2（迁移口径）：历史行（迁移前裸 symbol id）与新命名空间行
/// 同表共存、各按 id 读回——恢复与对账路径零重写。
#[test]
fn legacy_rows_coexist_with_namespaced_rows() {
    let ts = utc_ts();
    let new_intent = open_intent("once", "BTCUSDT", "binance/crypto_perpetual/BTCUSDT", ts);
    let mut legacy = new_intent.clone();
    legacy.id = generate_signal_id("once", "BTCUSDT", ts, SignalType::Buy);

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::new(dir.path(), Arc::new(EpochClock)));
    let table = store.table::<ManagedSignal>();
    table.insert(row(legacy.clone(), "upgraded_inst")).unwrap();
    table
        .insert(row(new_intent.clone(), "upgraded_inst"))
        .unwrap();
    assert_eq!(table.list().len(), 2, "旧格式行与新格式行同表共存");
    assert!(table.get(&legacy.id).is_some(), "存量行按落盘 id 恢复");
    assert!(table.get(&new_intent.id).is_some());
}

/// 验收 3（单一格式源）：旧 CLI 链派生键 == engine 类型化链
/// `namespaced_key()`（经 registry 解析真值互验），且 §6.1 文档例逐字成立。
#[test]
fn venue_key_single_source_matches_instrument_id_and_doc_examples() {
    use std::path::Path;
    use strategy_engine::exchanges::binance::BinanceExchange;
    use strategy_engine::exchanges::ctp::CtpExchange;
    use strategy_engine::prelude::{Exchange, ExchangeId};

    // §6.1 例串逐字
    assert_eq!(
        spi_backtest::venue_key_for("binance", None, "BTCUSDT"),
        "binance/crypto_perpetual/BTCUSDT"
    );

    // crypto：与 engine 类型化路径（parse_instrument → namespaced_key）同值
    let bin = BinanceExchange::named(ExchangeId::new("binance"), 0.0004);
    let bin_inst = bin.parse_instrument("BTCUSDT").unwrap();
    assert_eq!(
        bin_inst.namespaced_key(),
        spi_backtest::venue_key_for("binance", None, "BTCUSDT")
    );

    // futures：真 contracts.yaml 合约 + CTP 解析器互验
    let cfg = config::load(Some(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config")
            .as_path(),
    ))
    .expect("仓库 config");
    let contract = config::contract_for(&cfg.contracts, "CU2601")
        .cloned()
        .expect("contracts.yaml 含 CU");
    let ctp = CtpExchange::named(ExchangeId::new("SHFE"));
    let ctp_inst = ctp.parse_instrument("CU2601").unwrap();
    assert_eq!(
        ctp_inst.namespaced_key(),
        spi_backtest::venue_key_for("SHFE", Some(&contract), "CU2601"),
        "§6.1 例 SHFE/cn_futures/CU2601 双链同值"
    );
    assert_eq!(ctp_inst.namespaced_key(), "SHFE/cn_futures/CU2601");
}
