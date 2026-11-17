#!/usr/bin/env python3
"""collect_benchmark.py — 从扁平回测产物聚合基准指标（通用版）。

扫描 --root 下同时包含 backtest_equity.csv 与 backtest_trades.csv 的目录，
按权益曲线计算期末权益/最大回撤，并统计成交行数，输出 CSV：
  run, dir, initial_equity, final_equity, max_drawdown, trade_rows, equity_rows

用法:
  python3 -I scripts/collect_benchmark.py --root reports [--out reports/benchmark.csv] [--dry-run]

说明:
 - 只依赖标准库；equity CSV 列要求含 equity（忽略大小写与首尾空白）。
 - --dry-run 仅打印发现项，不写文件。
"""

import argparse
import csv
import os
import sys


def find_pairs(root):
    for dirpath, _dirnames, filenames in os.walk(root):
        if "backtest_equity.csv" in filenames and "backtest_trades.csv" in filenames:
            yield dirpath


def read_equity(path):
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        field = None
        for name in reader.fieldnames or []:
            if name.strip().lower() == "equity":
                field = name
                break
        if field is None:
            raise ValueError(f"{path}: no 'equity' column")
        return [float(row[field]) for row in reader if row.get(field) not in (None, "")]


def max_drawdown(values):
    peak = None
    worst = 0.0
    for v in values:
        peak = v if peak is None else max(peak, v)
        if peak and peak > 0:
            worst = max(worst, (peak - v) / peak)
    return worst


def count_rows(path):
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return max(sum(1 for _ in fh) - 1, 0)


def row_of(dirpath):
    eq_path = os.path.join(dirpath, "backtest_equity.csv")
    tr_path = os.path.join(dirpath, "backtest_trades.csv")
    values = read_equity(eq_path)
    return {
        "run": os.path.basename(dirpath),
        "dir": dirpath,
        "initial_equity": values[0] if values else "",
        "final_equity": values[-1] if values else "",
        "max_drawdown": f"{max_drawdown(values):.6f}" if values else "",
        "trade_rows": count_rows(tr_path),
        "equity_rows": len(values),
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="Aggregate flat backtest equity/trades into a CSV")
    ap.add_argument("--root", required=True, help="directory to scan recursively")
    ap.add_argument("--out", default=None, help="output CSV (default: <root>/benchmark.csv)")
    ap.add_argument("--dry-run", action="store_true", help="list found runs only, write nothing")
    args = ap.parse_args(argv)

    if not os.path.isdir(args.root):
        print(f"error: root is not a directory: {args.root}", file=sys.stderr)
        return 2
    dirs = sorted(find_pairs(args.root))
    if not dirs:
        print(f"error: no backtest_equity.csv + backtest_trades.csv under {args.root}", file=sys.stderr)
        return 1

    rows = [row_of(d) for d in dirs]
    if args.dry_run:
        for r in rows:
            print(f"{r['run']}\t{r['final_equity']}\t{r['max_drawdown']}\t{r['trade_rows']}")
        return 0

    fields = ["run", "dir", "initial_equity", "final_equity", "max_drawdown", "trade_rows", "equity_rows"]
    out = args.out or os.path.join(args.root, "benchmark.csv")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} row(s) -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
