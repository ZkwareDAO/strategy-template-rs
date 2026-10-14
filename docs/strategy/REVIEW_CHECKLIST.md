# 提交前检查单

## A. 红线自检（对照 AI_CONSTRAINTS.md）

- [ ] 无 `now()` / 系统时间；时间只用 `ctx.event_time`
- [ ] 只读 `ctx.closed_data*()`；无绕过 `Ctx` 的数据源
- [ ] 策略内无 IO
- [ ] 未改 `build.rs` / `registry.rs` / `lib.rs`（接线区）
- [ ] `Params` 有 `deny_unknown_fields, default`
- [ ] 指标前有 `closed_data_min` 长度检查
- [ ] `timeframes()` 含 `Timeframe::M1`
- [ ] 主路径无 `unwrap()` / `expect()`
- [ ] 未绕过引擎改仓位状态
- [ ] 指标全量重算，无增量缓存

## B. 注册与命名

- [ ] `strategy_type()` == `register_strategy!` name == 配置 `strategy:`
- [ ] 文件名 == `rust_module` == 模块名
- [ ] `Params::default()` 与 `config/*/example.yaml` 一致

## C. 测试

- [ ] 入场测试（至少一个多头/一个空头）
- [ ] 出场测试（止损或反转）
- [ ] 数据不足静默测试
- [ ] `cargo test --workspace` 全绿
- [ ] `cargo clippy --all-targets -- -D warnings` 零告警

## D. 配置与文档

- [ ] `config/<name>/example.yaml` 存在且可跑
- [ ] `docs/strategy/specs/<name>.strategy-spec.yaml` 存在
- [ ] 若改了行为，`docs/strategy/` 相关文档已更新
