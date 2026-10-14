# 策略开发指南（脚手架入口）

从这里开始写你的第一个 CTA 策略。本目录是**策略开发层**；
框架内部设计见 [`../design.md`](../design.md)（不讲怎么写策略）。

## 阅读顺序

1. [QUICKSTART.md](QUICKSTART.md) — 10 分钟跑通回测
2. [EXAMPLES.md](EXAMPLES.md) — 两个 example 逐段讲解
3. [DEVELOPMENT_GUIDE.md](DEVELOPMENT_GUIDE.md) — trait / Ctx / 注册 / 配置 / 测试
4. [AI_CONSTRAINTS.md](AI_CONSTRAINTS.md) — 编码红线（AI 生成策略时必读）
5. [REVIEW_CHECKLIST.md](REVIEW_CHECKLIST.md) — 提交前自检

## AI 辅助开发流

```
阶段1 逻辑确认 → specs/<name>.strategy-spec.yaml   （人机对话产出）
阶段2 代码实现 → src/<name>.rs + config/<name>/    （AI 消费 spec）
阶段3 验证     → cargo test + backtest 对照 spec
```

规格 schema 见 [STRATEGY_SPEC.md](STRATEGY_SPEC.md)；
参考实现见 [specs/](specs/)。

## 新增策略三步走

1. 复制 `crates/strategies/src/example_dual_ma.rs` 为你的策略文件
2. 改策略名与逻辑，文件尾 `register_strategy!` 自注册——**无需修改任何其他文件**
3. 建配置 `config/<strategy>/<SYMBOL>.yaml`（样例见 `config/example_dual_ma/example.yaml`）
