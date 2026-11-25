# AGENTS.md — AI 会话引导（自动注入入口）

本仓库是 **CTA 策略模板**（Rust；crypto + 国内期货）。策略开发的全部规范、
红线与验收方式以 `docs/strategy/` 为契约层，代码以 `crates/` 源码为准。

## 按任务路由

| 任务场景 | 必读 |
|---|---|
| 任何策略开发/修改（开工前） | [docs/strategy/README.md](docs/strategy/README.md)（含 11 步阅读顺序） |
| 生成策略代码（硬红线，逐条过） | [docs/strategy/AI_CONSTRAINTS.md](docs/strategy/AI_CONSTRAINTS.md)（23 禁 + 12 必须） |
| 从用户逻辑到落地的工作流 | [docs/strategy/AI_WORKFLOW.md](docs/strategy/AI_WORKFLOW.md) |
| 配置字段 / 单标 vs 多标 | [docs/strategy/CONFIG_REFERENCE.md](docs/strategy/CONFIG_REFERENCE.md) |
| 国内期货任务（合约/时段/数据） | [docs/strategy/CRYPTO_VS_FUTURES.md](docs/strategy/CRYPTO_VS_FUTURES.md) |
| 提交前自检 | [docs/strategy/REVIEW_CHECKLIST.md](docs/strategy/REVIEW_CHECKLIST.md) |

## 三条铁律（细节见 AI_WORKFLOW.md §0）

1. **只实现用户明确说出的逻辑**——用户没说的指标/过滤/止损/止盈一律不加，缺失信息先问。
2. **只读已闭合 K 线**——禁 `now()`、禁未闭合数据、禁绕过 `snapshot.closed`。
3. **期货 K 线路径必须由用户提供**——禁止 mock/合成/其他品种数据顶替，缺数据就停，不回测。

## 验证命令

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

提交前完整自检清单：[docs/strategy/REVIEW_CHECKLIST.md](docs/strategy/REVIEW_CHECKLIST.md)。
