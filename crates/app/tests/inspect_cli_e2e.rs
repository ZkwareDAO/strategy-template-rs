//! `inspect` 子命令端到端：state 快照展开 → 表格/纵向/JSONL/扁平 CSV 四种输出。
//!
//! 覆盖 `crates/app/src/inspect.rs` 的渲染层与 `main.rs` Inspect 分支：
//! 单文件、目录批处理、`--limit` 截断、`--json`、`--out`、无 CSV 报错。

use std::process::{Command, Output};

/// 快照格式 CSV：id,deleted,created_at_ms,updated_at_ms,payload_json。
/// 2026-01-05T00:00:00Z = 1767571200000 ms。
fn snapshot_csv(rows: &[(&str, bool, i64, &str)]) -> String {
    let mut wtr = csv::Writer::from_writer(Vec::new());
    wtr.write_record([
        "id",
        "deleted",
        "created_at_ms",
        "updated_at_ms",
        "payload_json",
    ])
    .unwrap();
    for (id, deleted, ts, payload) in rows {
        let ts_s = ts.to_string();
        wtr.write_record([
            *id,
            if *deleted { "1" } else { "0" },
            ts_s.as_str(),
            ts_s.as_str(),
            *payload,
        ])
        .unwrap();
    }
    String::from_utf8(wtr.into_inner().unwrap()).unwrap()
}

fn run_inspect(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_strategy-rs"))
        .arg("inspect")
        .args(args)
        .output()
        .expect("启动 strategy-rs inspect")
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).to_string()
}

#[test]
fn snapshot_expands_to_aligned_table_with_formatted_time() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = tmp.path().join("state.csv");
    std::fs::write(
        &csv,
        snapshot_csv(&[
            (
                "r1",
                false,
                1767571200000,
                r#"{"pos":{"qty":3,"px":100.5},"side":"long"}"#,
            ),
            (
                "r2",
                false,
                1767571200000,
                r#"{"pos":{"qty":1},"side":"short"}"#,
            ),
        ]),
    )
    .unwrap();

    let out = run_inspect(&[csv.to_str().unwrap()]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let text = stdout(&out);
    // 表头：id + 展开列并集；嵌套对象以点号拼键。
    assert!(text.contains("pos.qty"), "缺展开列 pos.qty：\n{text}");
    assert!(text.contains("side"), "缺展开列 side：\n{text}");
    assert!(text.contains("r1"), "缺行 id：\n{text}");
}

#[test]
fn jsonl_output_is_one_record_per_line_with_typed_deleted_and_null() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = tmp.path().join("state.csv");
    std::fs::write(
        &csv,
        snapshot_csv(&[
            ("r1", true, 1767571200000, r#"{"qty":3,"flag":null}"#),
            ("r2", false, 1767571200000, r#"{"qty":1,"flag":"x"}"#),
        ]),
    )
    .unwrap();

    let out = run_inspect(&[csv.to_str().unwrap(), "--json"]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let lines: Vec<serde_json::Value> = stdout(&out)
        .lines()
        .map(|l| serde_json::from_str(l).expect("每行必须是合法 JSON"))
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["id"], "r1");
    assert_eq!(lines[0]["deleted"], true);
    assert_eq!(lines[0]["qty"], "3");
    // 毫秒时间戳 → 可读 UTC。
    assert_eq!(lines[0]["created_at"], "2026-01-05 00:00:00");
    assert!(lines[0]["flag"].is_null(), "null 域必须输出 JSON null");
    assert_eq!(lines[1]["deleted"], false);
}

#[test]
fn out_writes_flat_csv_per_input_file() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = tmp.path().join("state.csv");
    std::fs::write(
        &csv,
        snapshot_csv(&[("r1", false, 1767571200000, r#"{"pos":{"qty":3}}"#)]),
    )
    .unwrap();
    let out_dir = tmp.path().join("flat");

    let out = run_inspect(&[csv.to_str().unwrap(), "--out", out_dir.to_str().unwrap()]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let written = out_dir.join("state.flat.csv");
    assert!(written.is_file(), "缺 {written:?}");
    let header = std::fs::read_to_string(&written)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .to_string();
    assert!(
        header.contains("pos.qty"),
        "扁平 CSV 表头缺展开列：{header}"
    );
    assert!(stdout(&out).contains("已写出"));
}

#[test]
fn directory_mode_prints_every_csv_and_skips_non_csv() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("state");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("a.csv"),
        snapshot_csv(&[("r1", false, 1767571200000, r#"{"k":1}"#)]),
    )
    .unwrap();
    std::fs::write(
        dir.join("b.csv"),
        snapshot_csv(&[
            ("r2", false, 1767571200000, r#"{"k":2}"#),
            ("r3", false, 1767571200000, r#"{"k":3}"#),
        ]),
    )
    .unwrap();
    std::fs::write(dir.join("notes.txt"), "not a csv").unwrap();

    let out = run_inspect(&[dir.to_str().unwrap(), "--limit", "1"]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("══ a.csv ══"), "缺 a.csv 头：\n{text}");
    assert!(text.contains("══ b.csv ══"), "缺 b.csv 头：\n{text}");
    assert!(!text.contains("notes.txt"), "非 CSV 不得读取：\n{text}");
    assert!(
        text.contains("共 2 行，显示前 1 行"),
        "--limit 截断提示缺失：\n{text}"
    );
}

#[test]
fn wide_columns_fall_back_to_vertical_blocks() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = tmp.path().join("wide.csv");
    // 13 个展开列 > TABLE_COL_THRESHOLD(12) → 纵向键值块。
    let payload = serde_json::json!({
        "k1": 1, "k2": 2, "k3": 3, "k4": 4, "k5": 5, "k6": 6, "k7": 7,
        "k8": 8, "k9": 9, "k10": 10, "k11": 11, "k12": 12, "k13": 13,
    });
    std::fs::write(
        &csv,
        snapshot_csv(&[("rec1", false, 1767571200000, &payload.to_string())]),
    )
    .unwrap();

    let out = run_inspect(&[csv.to_str().unwrap()]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("── rec1 ──"), "纵向块头缺失：\n{text}");
    assert!(text.contains("created_at"), "纵向块缺 created_at：\n{text}");
    assert!(text.contains("k13"), "纵向块缺域字段：\n{text}");
}

#[test]
fn flat_csv_passes_through_without_payload_column() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = tmp.path().join("plain.csv");
    std::fs::write(&csv, "a,b\n1,2\n").unwrap();

    let out = run_inspect(&[csv.to_str().unwrap()]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let text = stdout(&out);
    // 非快照格式：原样保留列，首列同时作为 id。
    assert!(text.contains('a'), "缺原列 a：\n{text}");
    assert!(text.contains('b'), "缺原列 b：\n{text}");
}

#[test]
fn empty_directory_is_an_error() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("empty");
    std::fs::create_dir_all(&dir).unwrap();

    let out = run_inspect(&[dir.to_str().unwrap()]);
    assert!(!out.status.success(), "无 CSV 目录必须失败退出");
    assert!(
        stderr(&out).contains("无 CSV 文件"),
        "报错文案缺失：{}",
        stderr(&out)
    );
}
