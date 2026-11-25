# 策略开发指南（AI 协作入口）

从这里开始写你的第一个 CTA 策略。本目录是**策略开发契约层**：只写接口、不变量、
验收方式，代码以 `crates/` 源码为准；面向人和 AI 共同消费。
框架内部设计见 [`../design-package/`](../design-package/)——注意它是**设计时快照**
（2026-11-25 加注），仅用于理解设计意图，策略清单与行号已过期；
`../design.md` 等历史文档不再更新。

模板仓库只保留 **2 个模板策略**，覆盖 crypto 单标/多周期、横截面、期货 1:1/1:N。
其余实验策略已移除；新实验请从模板拷贝，不要把一次性策略长期塞回 `crates/strategies/src/`。

## 阅读顺序

1. [AI_WORKFLOW.md](AI_WORKFLOW.md) — **AI 如何接收用户策略逻辑并落地**（先读）
2. [QUICKSTART.md](QUICKSTART.md) — 10 分钟跑通回测（crypto + 期货各一条命令）
3. [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md) — `strategy.yaml` / `_global.yaml` 全字段、单标 vs 多标
4. [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md) — 统一 port / 注册 / 配置 / 测试
5. [XSEC_GUIDE.md](XSEC_GUIDE.md) — 单标/横截面接口、`StrategyBase`、配置与升级路径
6. [POSITION_MODEL.md](POSITION_MODEL.md) — 总仓/子仓账本、开平仓链路与容量口径
7. [CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md) — **加密货币与国内期货的差异与检查点**
8. [EXAMPLES.md](EXAMPLES.md) — 2 个模板的定位与源码导读
9. [STRATEGY_SPEC.md](STRATEGY_SPEC.md) — `.strategy-spec.yaml` 字段 schema
10. [AI_CONSTRAINTS.md](AI_CONSTRAINTS.md) — 编码红线（AI 生成策略时必读）
11. [REVIEW_CHECKLIST.md](REVIEW_CHECKLIST.md) — 提交前自检

AI 生成策略时只实现用户明确说出的逻辑：不自行增加指标、过滤、止损或止盈；
缺失信息先问，不猜。落地步骤与输入清单见 [AI_WORKFLOW.md](AI_WORKFLOW.md)。

## 两个模板

| 策略 | 市场 | 形态 | 用途 |
|---|---|---|---|
| `template_trend` | crypto / 期货 | 单标逐标 | EMA 交叉 / 趋势过滤 + 百分比或 ATR 止损；同一代码跑 `BTCUSDT` 与 `CU` |
| `template_xsec_ls` | crypto / 期货 | N=1 ↔ N>1 | 多空横截面：前 `top_k` 做多、后 `bottom_k` 做空；N=1 按趋势方向单标 |

选起点：
- crypto / 期货单标 → `template_trend`
- 需要多空排名/轮换（期货样例）→ `template_xsec_ls`
- crypto 与期货的配置、合约、时段差异 → [CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)

## AI 辅助开发流

```
阶段1 逻辑确认 → specs/<name>.strategy-spec.yaml   （人机对话产出，含 market 字段）
阶段2 代码实现 → crates/strategies/src/<name>.rs + config/<name>/   （AI 消费 spec）
阶段3 验证     → cargo test --workspace + backtest 对照 spec
```

规格 schema 见 [STRATEGY_SPEC.md](STRATEGY_SPEC.md)；
参考实现见 [specs/](specs/)。

## 新增策略三步走

1. 选起点（见上表）：
   - 推荐：实现 `StrategyLogic`（`name/top_k/score/entry_allowed/stop`），用
     `common::StrategyBase` 包装；基类契约见 `common/mod.rs` 与
     `crates/strategies/tests/strategy_base.rs`。当前 2 个模板都直接实现 port，
     最小可跑起点是 `template_xsec_ls.rs`。注意 `StrategyBase` 当前只发多单；
     需要 `bottom_k` 双端空头时直接实现 port。
   - 或用 `template_trend.rs` / `template_xsec_ls.rs` 直接实现 port。
   - 期货：先读 [CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)，再参考
     `template_trend.rs` / `template_xsec_ls.rs`。
2. 文件尾 `register_strategy!` 自注册——**无需修改任何其他文件**；
   `strategy_type()`、注册名、配置名三者必须一致。
3. 建配置：
   - 统一端口策略：`config/<group>/strategy.yaml`（样例 `config/tpl_xsec_futures/strategy.yaml`）。
   - 期货：`universe` 写 `{ symbol, exchange }`，合约参数写 `config/contracts.yaml`。
   - 旧单标路径：`config/<strategy>/<SYMBOL>.yaml`，目录模式按**文件名**认领标的。

## 由 trading-dev skill 驱动时的分工

当开发流程由 `trading-dev` skill 编排（`/trading-dev new` 等）时，验收分两层，
**叠加生效、互不替代**：

| 层 | 负责方 | 判据 |
|---|---|---|
| 代码正确可跑 | 本目录 DoD（AI_WORKFLOW.md §6） | spec/测试/配置齐全，`cargo test --workspace` 全绿，回测与 spec 对照 |
| 策略表现达标 | trading-dev skill（Phase 3） | 短/中/长三窗口复合验收（收益/回撤/交易次数）+ benchmark 输出 |

注意：**「回测跑通」≠「验收通过」**。模板 DoD 只保证代码语义正确；表现层验收
（默认 费后收益 ≥20%、最大回撤 ≤30%、交易次数 ≥10 笔）由 skill 的回测窗口与
阈值变量判定，未达标进入 skill 的 loop 调参闭环。两层各自的产物（DoD 清单、
benchmark.md/json）都要交付。
