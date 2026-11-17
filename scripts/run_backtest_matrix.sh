#!/usr/bin/env bash
# run_backtest_matrix.sh — 回测矩阵批跑（通用版，无策略/品种硬编码）
#
# 用法:
#   scripts/run_backtest_matrix.sh [options] <config-file-or-dir>...
#
# 选项:
#   --symbols "CU AG"     逐品种运行（空格/逗号分隔；缺省 = 每个 config 只跑一次）
#   --start YYYYMMDD      回测开始（缺省 20260101）
#   --end   YYYYMMDD      回测结束（缺省 20261231）
#   --data-dir DIR        1m K 线根目录（缺省 data/klines）
#   --out DIR             报告输出根目录（缺省 reports）
#   --bin CMD             回测命令前缀（缺省 "cargo run -q -p strategy-app --"）
#   --dry-run             只打印将执行的命令，不实际运行
#   -h, --help            显示帮助
#
# 输出: 每个 (config, symbol) 组合执行一次:
#   <bin> backtest --config <cfg> --symbol <SYM> --start ... --end ... --data-dir ... --output-dir ...
#
# 说明:
# - config 为目录时交给 CLI 的目录模式（目录内每个 {strategy}/{SYMBOL}.yaml 各自生效）；
#   此时 --symbols 若给出，会按 symbol 过滤（--symbol 逐个传）。
# - 期货与 crypto 可同批混跑（旧 services.kline.market 全局字段已随 klines-service 退役删除）。
set -euo pipefail

usage() {
  sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
}

SYMBOLS=""
START="20260101"
END="20261231"
DATA_DIR="data/klines"
OUT="reports"
BIN="cargo run -q -p strategy-app --"
DRY_RUN=0
CONFIGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --symbols) SYMBOLS="${2:?--symbols requires a value}"; shift 2 ;;
    --start) START="${2:?--start requires a value}"; shift 2 ;;
    --end) END="${2:?--end requires a value}"; shift 2 ;;
    --data-dir) DATA_DIR="${2:?--data-dir requires a value}"; shift 2 ;;
    --out) OUT="${2:?--out requires a value}"; shift 2 ;;
    --bin) BIN="${2:?--bin requires a value}"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    *) CONFIGS+=("$1"); shift ;;
  esac
done

if [[ ${#CONFIGS[@]} -eq 0 ]]; then
  echo "error: at least one config file/dir is required" >&2
  usage >&2
  exit 2
fi

# "CU AG" / "CU,AG" -> 数组
read -r -a SYM_ARR <<<"${SYMBOLS//,/ }"

for cfg in "${CONFIGS[@]}"; do
  if [[ ! -e "$cfg" ]]; then
    echo "error: config not found: $cfg" >&2
    exit 2
  fi
  if [[ ${#SYM_ARR[@]} -eq 0 ]]; then
    cmd=($BIN backtest --config "$cfg" --start "$START" --end "$END" \
      --data-dir "$DATA_DIR" --output-dir "$OUT")
    printf '==> %s\n' "${cmd[*]}"
    if [[ $DRY_RUN -eq 0 ]]; then "${cmd[@]}"; fi
  else
    for sym in "${SYM_ARR[@]}"; do
      cmd=($BIN backtest --config "$cfg" --symbol "$sym" --start "$START" --end "$END" \
        --data-dir "$DATA_DIR" --output-dir "$OUT")
      printf '==> %s\n' "${cmd[*]}"
      if [[ $DRY_RUN -eq 0 ]]; then "${cmd[@]}"; fi
    done
  fi
done
