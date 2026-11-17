# 策略开发指南（AI 协作入口）

从这里开始写你的第一个 CTA 策略。本目录是**策略开发契约层**：只写接口、不变量、
验收方式，代码以 `crates/` 源码为准；面向人和 AI 共同消费。
框架内部设计见 [`../design-package/`](../design-package/)（`../design.md` 已登记为
历史文档，不再更新）。

模板仓库只保留 **6 个模板策略**，覆盖 crypto 单标/多周期、横截面、期货 1:1/1:N。
其余实验策略已移除；新实验请从模板拷贝，不要把一次性策略长期塞回 `crates/strategies/src/`。

## 阅读顺序

1. [AI_WORKFLOW.md](AI_WORKFLOW.md) — **AI 如何接收用户策略逻辑并落地**（先读）
2. [QUICKSTART.md](QUICKSTART.md) — 10 分钟跑通回测（crypto + 期货各一条命令）
3. [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md) — `strategy.yaml` / `_global.yaml` 全字段、单标 vs 多标
4. [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md) — 统一 port / 注册 / 配置 / 测试
5. [XSEC_GUIDE.md](XSEC_GUIDE.md) — 单标/横截面接口、`StrategyBase`、配置与升级路径
6. [POSITION_MODEL.md](POSITION_MODEL.md) — 总仓/子仓账本、开平仓链路与容量口径
7. [CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md) — **加密货币与国内期货的差异与检查点**
8. [EXAMPLES.md](EXAMPLES.md) — 6 个模板的定位与源码导读
9. [STRATEGY_SPEC.md](STRATEGY_SPEC.md) — `.strategy-spec.yaml` 字段 schema
10. [AI_CONSTRAINTS.md](AI_CONSTRAINTS.md) — 编码红线（AI 生成策略时必读）
11. [REVIEW_CHECKLIST.md](REVIEW_CHECKLIST.md) — 提交前自检

AI 生成策略时只实现用户明确说出的逻辑：不自行增加指标、过滤、止损或止盈；
缺失信息先问，不猜。落地步骤与输入清单见 [AI_WORKFLOW.md](AI_WORKFLOW.md)。

## 六个模板

| 策略 | 市场 | 形态 | 用途 |
|---|---|---|---|
| `example_dual_ma` | crypto / 期货 | 单标 N=1 | 入门：最小完整策略；同一代码可跑 `BTCUSDT` 与 `CU` |
| `example_mtf_trend` | crypto | 单标多周期 | 多周期样板：`[M1, 主周期, D1]` 与跨周期取数 |
| `xsec_template` | crypto | N=1 ↔ N>1 | 直接实现 port 的模板；单标/横截面双路径 |
| `xsec_momentum` | crypto | 横截面 | 直接实现 port：MACD 排名 + 等权 `Adjust`，新策略最省事的起点 |
| `futures_single_trend` | 期货 | 1:1 | 期货单品种：整数手、CTP 时段聚合、ATR 止损 |
| `futures_xsec_trend` | 期货 | 1:N | 期货多品种：一个组合总仓 + N 个子仓、动量轮换 |

选起点：
- crypto 单标最小实现 → `example_dual_ma`
- 需要排名/轮换 → `xsec_momentum` 或 `xsec_template`（均直接实现 port）
- 期货单品种 → `futures_single_trend`；期货组合 → `futures_xsec_trend`
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
     `crates/strategies/tests/strategy_base.rs`。当前 6 个模板都直接实现 port，
     最小可跑起点是 `xsec_momentum.rs`。
   - 或用 `example_dual_ma.rs` / `xsec_template.rs` 直接实现 port。
   - 期货：先读 [CRYPTO_VS_FUTURES.md](CRYPTO_VS_FUTURES.md)，再参考
     `futures_single_trend.rs` / `futures_xsec_trend.rs`。
2. 文件尾 `register_strategy!` 自注册——**无需修改任何其他文件**；
   `strategy_type()`、注册名、配置名三者必须一致。
3. 建配置：
   - 统一端口策略：`config/<group>/strategy.yaml`（样例 `config/xsec_momentum/strategy.yaml`）。
   - 期货：`universe` 写 `{ symbol, exchange }`，合约参数写 `config/contracts.yaml`。
   - 旧单标路径：`config/<strategy>/<SYMBOL>.yaml`，目录模式按**文件名**认领标的。
