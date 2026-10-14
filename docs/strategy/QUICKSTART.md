# QUICKSTART — 10 分钟跑通

目标：clone 后跑通一次回测，然后复制 example 写出你自己的策略。

## 0. 环境

```bash
rustup show            # 应为 1.98.1（rust-toolchain.toml 钉死）
cargo test --workspace # 基线应全绿
```

## 1. 准备数据

样例数据已在 `data/klines/`。若缺目标币种：

```bash
cargo run -p strategy-app -- download --symbol BTCUSDT --interval 1m --days 40
```

数据落到 `data/klines/{interval}/{SYMBOL}_{interval}.csv`。

## 2. 跑一次回测

```bash
cargo run -p strategy-app -- backtest \
  --config config/example_dual_ma --symbol BTCUSDT \
  --start 20260801 --end 20260826
```

## 3. 读报告

输出目录 `{output_dir}/{strategy}/{end:YYYYMMDD}/{end:YYYYMMDD}T{HHMMSS}/{SYMBOL}/`：

| 文件 | 内容 |
|---|---|
| `backtest_result.json` | 指标全集（收益/夏普/回撤/胜率…） |
| `backtest_report.txt` | 人类可读摘要 |
| `backtest_analysis_report.md` | 分析报告 |
| `charts/*.png` | 净值曲线 / 回撤 / 分时净值 |

## 4. 写你自己的策略

```bash
cp crates/strategies/src/example_dual_ma.rs crates/strategies/src/my_strategy.rs
```

1. 改 `Params` 与逻辑
2. 文件尾 `register_strategy!(name: "my_strategy", ...)` —— **不要改任何其他文件**
3. 建配置 `config/my_strategy/BTCUSDT.yaml`（复制 `config/example_dual_ma/example.yaml`）
4. `cargo test --workspace` 后跑回测

详细规范 → [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md)
