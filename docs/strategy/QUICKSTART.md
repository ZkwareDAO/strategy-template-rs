# QUICKSTART — 10 分钟跑通

目标：clone 后跑通一次回测，然后复制 example 写出你自己的策略。

## 0. 环境

```bash
rustup show            # 应为 1.98.1（rust-toolchain.toml 钉死）
cargo test --workspace # 基线应全绿
```

## 1. 准备数据（回测硬前置）

仓库只带 gitignore 占位文件，真实 1m 数据要自己准备：

- crypto：放到 `data/klines/1m/binance/crypto_perpetual/<SYMBOL>_1m.csv`；
  手工放置的 CSV 需标准 10 列（格式见 [AI_WORKFLOW.md](AI_WORKFLOW.md) §4）；
  缺失可下载：

  ```bash
  cargo run -p strategy-app -- download --symbol BTCUSDT --interval 1m --days 40
  ```

- 期货：**没有自动下载**，必须由用户提供逐合约 CSV 目录或已生成的 1m CSV，
  再按 [AI_WORKFLOW.md](AI_WORKFLOW.md) §4 放到
  `data/klines/1m/<EXCHANGE>/cn_futures/<SYMBOL>_1m.csv`。
- 回测区间内没有 1m 数据会带行键与期望路径报「区间无 1m 数据」并拒跑——这是
  fail-fast，不是 bug，不要用别的区间/品种数据绕过。

## 2. 跑一次回测

```bash
# 教学样例（统一端口 strategy.yaml，N=1）
cargo run -p strategy-app -- backtest \
  --config config/example_dual_ma/strategy.yaml \
  --start 20260801 --end 20260826

# 横截面示例
cargo run -p strategy-app -- backtest \
  --config config/xsec_momentum/strategy.yaml \
  --start 20260801 --end 20260826 --initial-cash 10000

# 期货示例（CU/SHFE，15m 触发 + 1h 趋势；数据需先 roll_build）
cargo run -p strategy-app -- backtest \
  --config config/ctp_single_cu/strategy.yaml \
  --start 20260101 --end 20260630
```

期货的合约乘数、整数手、日盘/夜盘、平今与数据落位见
[CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)。

`strategy.yaml` 不接受 `--strategy/--symbol/--cash` 覆盖：universe、sizing、
risk 都写在配置里。

## 3. 读报告

输出目录 `{output_dir}/{strategy_type}/{运行日期}/{运行时刻}/{strategy_id}/`
（`output_dir` 缺省 `./backtest_output`）：

| 文件 | 内容 |
|---|---|
| `backtest_result.json` | 指标全集（收益/夏普/回撤/胜率…） |
| `backtest_report.txt` | 人类可读摘要 |
| `backtest_analysis_report.md` | 分析报告 |
| `charts/*.png` | 净值曲线 / 回撤 / 分时净值 |

**读「回撤」时别混两个口径**：`backtest_analysis_report.md` 里
`最大回撤（滚动峰谷）` 才是通常意义的回撤（与 `result.json` 的 `max_drawdown`、
`report.txt` 同源）；`权益振幅（全局峰谷差，非回撤）` 是全局最高与全局最低之差，
**谷值可能出现在峰值之前**（BTCUSDT 4h / 2024-01-01..04-01 实跑即如此：
回撤 7.43%，振幅 35.17%）。跨策略比较请用最大回撤。
另注意回测窗口末尾**可能留有未平仓单**：此时 `可用现金 ≠ 期末权益`，
差额即 `未实现盈亏`，持仓笔数见 `report.txt`「期末持仓」或
`result.json` 的 `accounts[].position_count`。

## 4. 写你自己的策略

推荐用 `StrategyBase`（`common/mod.rs`）实现 `StrategyLogic` 五个钩子；
最小可跑起点直接复制 `xsec_momentum.rs`（直接实现 port）；
需要完全自定义决策流时，再复制 `example_dual_ma.rs` 或 `xsec_template.rs`；
期货起点用 `futures_single_trend.rs` / `futures_xsec_trend.rs`：

```bash
cp crates/strategies/src/example_dual_ma.rs crates/strategies/src/my_strategy.rs
```

1. 改 `Params` 与逻辑；止损必须明确：用 `StrategyBase` 时 `stop()` 返回
   `StopSpec`，直接实现 port 时 `Open` 必须带 `stop_price`；否则本轮不开仓
2. 文件尾 `register_strategy!(name: "my_strategy", ...)` —— **不要改任何其他文件**
3. 建配置：
   - 统一端口（推荐）：`config/<group>/strategy.yaml`（复制 `config/xsec_momentum/strategy.yaml`）
   - 旧单标（已标记退役，仅兼容历史配置）：`config/my_strategy/BTCUSDT.yaml`
     （文件名就是标的身份，见 `docs/RUNBOOK.md` §7.1）
4. `cargo test --workspace` 后跑回测

详细规范 → [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md)；
仓位与容量口径 → [POSITION_MODEL.md](POSITION_MODEL.md)。
