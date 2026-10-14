//! 集成验收：跨日裁剪端到端（spec 2026-10-09-store-terminal-pruning C 层）。
//! 用本文件定义的镜像 model 驱动 pub API，不依赖内部实现。
//! store 不能依赖 positions（依赖方向：positions→store），HistoryPosition
//! 恒真终态由镜像 PruneHistory 等价覆盖；真 HistoryPosition 的 is_terminal
//! 已在 positions 单元测试锁死。

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use strategy_store::{Clock, Model, Store};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct PrunePosition {
    id: String,
    alive: bool,
}
impl Model for PrunePosition {
    const NAME: &'static str = "prune_position";
    fn id(&self) -> &str {
        &self.id
    }
    fn is_terminal(&self) -> bool {
        !self.alive // 镜像 close 软删语义
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct PruneStop {
    id: String,
    lifecycle: String,
}
impl Model for PruneStop {
    const NAME: &'static str = "prune_stop";
    fn id(&self) -> &str {
        &self.id
    }
    fn is_terminal(&self) -> bool {
        self.lifecycle == "cancelled"
    }
}

/// 镜像 ManagedSignal::is_terminal 的三态载体：
/// - InFlight：Created..Accepted，不裁
/// - Terminal：Filled/Cancelled/Rejected，裁
/// - ResubmitableFailedClose：Close+Retryable+非ReduceOnly，不裁（重提保护）
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct PruneSignal {
    id: String,
    state: SignalMirror,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
enum SignalMirror {
    InFlight,
    Terminal,
    ResubmitableFailedClose,
}
impl Model for PruneSignal {
    const NAME: &'static str = "prune_signal";
    fn id(&self) -> &str {
        &self.id
    }
    fn is_terminal(&self) -> bool {
        matches!(self.state, SignalMirror::Terminal)
    }
}

/// 镜像 HistoryPosition：每行都是已完成成交，恒终态
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct PruneHistory {
    id: String,
}
impl Model for PruneHistory {
    const NAME: &'static str = "prune_history";
    fn id(&self) -> &str {
        &self.id
    }
    fn is_terminal(&self) -> bool {
        true
    }
}

struct AtomicClock(AtomicI64);
impl AtomicClock {
    fn set(&self, ms: i64) {
        self.0.store(ms, Ordering::SeqCst);
    }
}
impl Clock for AtomicClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn day_ms(y: i32, m: u32, d: u32) -> i64 {
    Utc.from_utc_datetime(
        &chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .timestamp_millis()
}

/// C1+C2：开平仓 → 跨日 flush → 内存/快照只余活行；重启 recover 一致
#[test]
fn day_rollover_trims_memory_snapshot_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicClock(AtomicI64::new(day_ms(2026, 10, 8))));
    let store = Arc::new(Store::new(dir.path(), clock.clone() as Arc<dyn Clock>));

    let pos = store.table::<PrunePosition>();
    let stop = store.table::<PruneStop>();
    let sig = store.table::<PruneSignal>();
    let hist = store.table::<PruneHistory>();

    // 活动态：1 个在途仓位 + 在途止损单 + 在途信号
    pos.insert(PrunePosition {
        id: "p_open".into(),
        alive: true,
    })
    .unwrap();
    stop.insert(PruneStop {
        id: "p_open".into(),
        lifecycle: "created".into(),
    })
    .unwrap();
    sig.insert(PruneSignal {
        id: "s_open".into(),
        state: SignalMirror::InFlight,
    })
    .unwrap();

    // 已平仓一日：终态四件套
    pos.insert(PrunePosition {
        id: "p_done".into(),
        alive: false,
    })
    .unwrap();
    stop.insert(PruneStop {
        id: "p_done".into(),
        lifecycle: "cancelled".into(),
    })
    .unwrap();
    sig.insert(PruneSignal {
        id: "s_done".into(),
        state: SignalMirror::Terminal,
    })
    .unwrap();
    hist.insert(PruneHistory {
        id: "p_done".into(),
    })
    .unwrap();

    // 同日 flush 建立 last_flush_day（rollover 需要 prev 日存在）
    store.flush().unwrap();
    // 跨日 flush：终态四件套全部裁掉，活动三件套保留（C1）
    clock.set(day_ms(2026, 10, 9));
    store.flush().unwrap();
    assert_eq!(pos.get("p_done"), None);
    assert_eq!(stop.get("p_done"), None);
    assert_eq!(sig.get("s_done"), None);
    assert_eq!(hist.get("p_done"), None);
    assert_eq!(
        pos.get("p_open"),
        Some(PrunePosition {
            id: "p_open".into(),
            alive: true
        })
    );
    assert_eq!(
        stop.get("p_open"),
        Some(PruneStop {
            id: "p_open".into(),
            lifecycle: "created".into()
        })
    );

    // 重启 recover：只拿回活动行（C2）
    let store2 = Store::new(
        dir.path(),
        Arc::new(AtomicClock(AtomicI64::new(day_ms(2026, 10, 9)))),
    );
    assert_eq!(store2.recover::<PrunePosition>().unwrap(), 1);
    assert_eq!(store2.recover::<PruneStop>().unwrap(), 1);
    assert_eq!(store2.recover::<PruneSignal>().unwrap(), 1);
    assert_eq!(store2.recover::<PruneHistory>().unwrap(), 0);
    assert_eq!(
        store2
            .table::<PruneSignal>()
            .get("s_open")
            .map(|s| s.state == SignalMirror::InFlight),
        Some(true)
    );
}

/// C3+C4：可重提 Failed Close 信号存活、Open Failed 被裁
/// （镜像 model 层面；真 ManagedSignal 互斥由 signals B5 单元测试锁死）
#[test]
fn failed_close_resubmitable_survives_open_failed_pruned() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicClock(AtomicI64::new(day_ms(2026, 10, 8))));
    let store = Arc::new(Store::new(dir.path(), clock.clone() as Arc<dyn Clock>));
    let sig = store.table::<PruneSignal>();
    sig.insert(PruneSignal {
        id: "close_retry".into(),
        state: SignalMirror::ResubmitableFailedClose,
    })
    .unwrap();
    sig.insert(PruneSignal {
        id: "open_failed".into(),
        state: SignalMirror::Terminal,
    })
    .unwrap();

    store.flush().unwrap(); // 同日 flush 建立 last_flush_day
    clock.set(day_ms(2026, 10, 9));
    store.flush().unwrap();
    assert!(
        sig.get("close_retry").is_some(),
        "可重提 Failed Close 必须存活"
    );
    assert_eq!(sig.get("open_failed"), None, "Open Failed 可裁");
}

/// C7：30 个 UTC 日，内存与快照大小有界不随天数增长
#[test]
fn growth_is_bounded_across_many_days() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicClock(AtomicI64::new(day_ms(2026, 1, 1))));
    let store = Arc::new(Store::new(dir.path(), clock.clone() as Arc<dyn Clock>));
    let pos = store.table::<PrunePosition>();

    // 恒定 1 个活动仓位；每天平 1 笔再开 1 笔
    pos.insert(PrunePosition {
        id: "open_d1".into(),
        alive: true,
    })
    .unwrap();
    store.flush().unwrap(); // 同日 flush 建立 last_flush_day
    for d in 2..=31 {
        pos.insert(PrunePosition {
            id: format!("done_d{d}"),
            alive: false,
        })
        .unwrap();
        clock.set(day_ms(2026, 1, d));
        store.flush().unwrap();
        // 裁剪后内存行数有界：活动 1 + 当日新终态 ≤ 3
        let in_mem = pos.list().len();
        assert!(in_mem <= 3, "day{d}: 内存行数 {in_mem} 无界增长");
    }
    // 最终快照行数与天数无关
    let final_snap =
        std::fs::read_to_string(dir.path().join("20260131").join("prune_position.csv")).unwrap();
    assert!(final_snap.lines().count() <= 4, "快照大小应与天数无关");
}

/// C8：3 天真实生命周期——开/平仓交替 + 中途重启，验证
/// 同日落盘 → 跨日裁剪 → 重启恢复 → 再交易 → 再跨日的完整闭环
#[test]
fn three_day_lifecycle_with_restart() {
    let dir = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicClock(AtomicI64::new(day_ms(2026, 10, 9))));
    let store = Arc::new(Store::new(dir.path(), clock.clone() as Arc<dyn Clock>));

    let pos = store.table::<PrunePosition>();
    let hist = store.table::<PruneHistory>();

    // ── Day1 (10-09)：开仓 + 平仓 → 同日快照含全部行
    pos.insert(PrunePosition {
        id: "pos_a".into(),
        alive: true,
    })
    .unwrap();
    pos.insert(PrunePosition {
        id: "pos_b".into(),
        alive: false,
    })
    .unwrap();
    hist.insert(PruneHistory { id: "pos_b".into() }).unwrap();
    store.flush().unwrap();
    let d1p = std::fs::read_to_string(dir.path().join("20261009/prune_position.csv")).unwrap();
    assert!(
        d1p.contains("pos_a,0,") && d1p.contains("pos_b,0,"),
        "Day1 同日快照含开仓+平仓"
    );

    // ── Day2 (10-10) 跨日：rollover 裁剪 → 终态消失，在途保留
    clock.set(day_ms(2026, 10, 10));
    store.flush().unwrap();
    assert_eq!(pos.get("pos_b"), None, "跨日后已平仓行被裁");
    assert_eq!(hist.get("pos_b"), None, "跨日后历史行被裁");
    assert_eq!(
        pos.get("pos_a"),
        Some(PrunePosition {
            id: "pos_a".into(),
            alive: true
        }),
        "在途仓位保留"
    );
    let d2p = std::fs::read_to_string(dir.path().join("20261010/prune_position.csv")).unwrap();
    assert!(
        d2p.contains("pos_a,0,") && !d2p.contains("pos_b"),
        "Day2 快照只含活行"
    );

    // ── 重启：从最新存在日文件恢复，只有活仓，被裁行不复活
    let store2 = Store::new(
        dir.path(),
        Arc::new(AtomicClock(AtomicI64::new(day_ms(2026, 10, 10)))),
    );
    assert_eq!(
        store2.recover::<PrunePosition>().unwrap(),
        1,
        "重启只恢复活仓"
    );
    assert_eq!(
        store2.recover::<PruneHistory>().unwrap(),
        0,
        "历史表恢复为空"
    );

    // ── Day2 内继续交易：pos_a 平仓、pos_c 开仓
    let p2 = store2.table::<PrunePosition>();
    let h2 = store2.table::<PruneHistory>();
    p2.upsert(PrunePosition {
        id: "pos_a".into(),
        alive: false,
    }); // 平仓
    h2.insert(PruneHistory { id: "pos_a".into() }).unwrap();
    p2.insert(PrunePosition {
        id: "pos_c".into(),
        alive: true,
    })
    .unwrap();
    store2.flush().unwrap();
    let d2p2 = std::fs::read_to_string(dir.path().join("20261010/prune_position.csv")).unwrap();
    assert!(d2p2.contains("pos_a,0,"), "同日新终态行写入快照");
    assert!(d2p2.contains("pos_c,0,"), "同日新活行写入快照");

    // ── Day3 (10-11)：重启进程再跨日——Day2 新终态被裁，pos_c 保留
    let clock3 = Arc::new(AtomicClock(AtomicI64::new(day_ms(2026, 10, 10))));
    let store3 = Store::new(dir.path(), clock3.clone() as Arc<dyn Clock>);
    store3.recover::<PrunePosition>().unwrap();
    store3.recover::<PruneHistory>().unwrap();
    assert_eq!(
        store3.table::<PrunePosition>().get("pos_a"),
        None,
        "重启后已裁行不复活"
    );
    clock3.set(day_ms(2026, 10, 11));
    store3.flush().unwrap();
    assert_eq!(
        store3.table::<PrunePosition>().get("pos_c"),
        Some(PrunePosition {
            id: "pos_c".into(),
            alive: true
        }),
        "在途仓跨日保留"
    );
    let d3p = std::fs::read_to_string(dir.path().join("20261011/prune_position.csv")).unwrap();
    assert!(
        d3p.contains("pos_c,0,") && !d3p.contains("pos_a"),
        "Day3 快照有界"
    );

    // ── 审计不丢：每笔平仓都在其当日文件永久留存
    let d1h = std::fs::read_to_string(dir.path().join("20261009/prune_history.csv")).unwrap();
    assert!(d1h.contains("pos_b"), "Day1 平仓审计在 Day1 文件");
    let d2h = std::fs::read_to_string(dir.path().join("20261010/prune_history.csv")).unwrap();
    assert!(d2h.contains("pos_a"), "Day2 平仓审计在 Day2 文件");
}
