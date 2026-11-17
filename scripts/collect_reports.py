#!/usr/bin/env python3
"""collect_reports.py — 汇总回测产物（通用版）。

递归扫描 --root 下的 backtest_result.json，抽取核心指标写 CSV：
  run, strategy, symbol, start, end, total_return, max_drawdown,
  trade_count, final_equity, result_path

用法:
  python3 -I scripts/collect_reports.py --root reports [--out reports/summary.csv] [--dry-run]

说明:
 - 只依赖标准库；--dry-run 仅列出发现的 result.json，不写文件。
 - 同一策略/品种多次运行（不同时间戳目录）会各占一行，run 取结果目录名。
"""

import argparse
import csv
import json
import os
import sys


def find_results(root):
    for dirpath, _dirnames, filenames in os.walk(root):
        if "backtest_result.json" in filenames:
            yield os.path.join(dirpath, "backtest_result.json")


def row_of(path):
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    cfg = data.get("config") or {}
    accounts = data.get("accounts") or [{}]
    acc = accounts[0] if accounts else {}
    metrics = data.get("metrics") or {}
    symbols = cfg.get("symbols") or []
    return {
        "run": os.path.basename(os.path.dirname(path)),
        "strategy": acc.get("strategy_id", ""),
        "symbol": ",".join(symbols),
        "start": cfg.get("start_date", ""),
        "end": cfg.get("end_date", ""),
        "total_return": metrics.get("total_return", ""),
        "max_drawdown": acc.get("max_drawdown", ""),
        "trade_count": acc.get("trade_count", data.get("trades_count", "")),
        "final_equity": acc.get("total_equity", ""),
        "result_path": path,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="Collect backtest_result.json into a CSV summary")
    ap.add_argument("--root", required=True, help="directory to scan recursively")
    ap.add_argument("--out", default=None, help="output CSV (default: <root>/summary.csv)")
    ap.add_argument("--dry-run", action="store_true", help="list results only, write nothing")
    args = ap.parse_args(argv)

    if not os.path.isdir(args.root):
        print(f"error: root is not a directory: {args.root}", file=sys.stderr)
        return 2
    paths = sorted(find_results(args.root))
    if not paths:
        print(f"error: no backtest_result.json under {args.root}", file=sys.stderr)
        return 1

    fields = [
        "run", "strategy", "symbol", "start", "end",
        "total_return", "max_drawdown", "trade_count", "final_equity", "result_path",
    ]
    rows = [row_of(p) for p in paths]

    if args.dry_run:
        for r in rows:
            print(f"{r['run']}\t{r['strategy']}\t{r['symbol']}\t{r['result_path']}")
        return 0

    out = args.out or os.path.join(args.root, "summary.csv")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} row(s) -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
