#!/usr/bin/env python3
"""prepare_ctp_1m.py — 主力连续 1m CSV → 引擎命名空间布局。

为什么需要它：回测加载器只认 `{dst}/1m/{exchange}/{market}/{SYMBOL}_1m.csv`
的十列 schema（timestamp 必须带 UTC 偏移）；CTP 源文件是 `bob_dt` 北京时间
裸时间戳 + `*_adj` 前复权列，两者不兼容，必须一次性转换而不是让加载器
猜时区（猜错会把夜盘跨日的事件流整体错位一小时）。

前复权列是唯一的连续价格口径：主力换月当天原始价会跳空，用原始列会把
换月当成真实趋势信号。
"""

import argparse
import csv
import os


HEADER = [
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_num",
    "active_buy_volume",
    "active_buy_quote_volume",
]

CST_SUFFIX = "+08:00"


def convert_one(src_path, dst_dir, symbol, from_date=None):
    os.makedirs(dst_dir, exist_ok=True)
    dst_path = os.path.join(dst_dir, f"{symbol}_1m.csv")
    rows = 0
    with open(src_path, newline="", encoding="utf-8") as f, open(
        dst_path, "w", newline="", encoding="utf-8"
    ) as out:
        rdr = csv.DictReader(f)
        wtr = csv.writer(out)
        wtr.writerow(HEADER)
        for row in rdr:
            if from_date and row["bob_dt"][:10] < from_date:
                continue
            # 北京时间加显式偏移再交给引擎 parse_ts；不加偏移会被当成 UTC。
            ts = row["bob_dt"] + CST_SUFFIX
            wtr.writerow(
                [
                    ts,
                    row["open_adj"],
                    row["high_adj"],
                    row["low_adj"],
                    row["close_adj"],
                    row["volume"],
                    "0",
                    "0",
                    "0",
                    "0",
                ]
            )
            rows += 1
    return dst_path, rows


def main(argv=None):
    ap = argparse.ArgumentParser(description="CTP 主力连续 1m → 引擎命名空间 CSV")
    ap.add_argument("--src", required=True, help="fwd_1m_output 目录")
    ap.add_argument("--dst", required=True, help="目标 data_dir 根")
    ap.add_argument(
        "--symbols",
        required=True,
        help="逗号分隔品种代码，如 CU,RB,I",
    )
    ap.add_argument(
        "--from-date",
        default=None,
        help="只保留该自然日及之后的 1m（YYYY-MM-DD；用于缩短回测装载）",
    )
    args = ap.parse_args(argv)
    out_dir = os.path.join(args.dst, "1m", "SHFE", "cn_futures")
    for raw in args.symbols.split(","):
        symbol = raw.strip().upper()
        if not symbol:
            continue
        src_path = os.path.join(args.src, f"{symbol}_main_1m.csv")
        if not os.path.isfile(src_path):
            raise SystemExit(f"源文件不存在: {src_path}")
        dst_path, rows = convert_one(src_path, out_dir, symbol, args.from_date)
        print(f"{symbol}: {rows} 行 → {dst_path}")


if __name__ == "__main__":
    main()
