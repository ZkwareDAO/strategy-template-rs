# 提交前检查单

## A. 红线自检（对照 AI_CONSTRAINTS.md）

- [ ] 无 `now()` / 系统时间；时间只用 `snapshot.bar.open_time`
- [ ] 只读 `snapshot.closed`；无绕过快照的数据源
- [ ] 策略内无 IO
- [ ] 未改 `build.rs` / `registry.rs` / `lib.rs`（接线区）
- [ ] `Params` 有 `deny_unknown_fields, default`
- [ ] 指标前有 `closed(tf)` 序列长度检查
- [ ] `timeframes()` 含 `Timeframe::M1`
- [ ] 主路径无 `unwrap()` / `expect()`
- [ ] 未绕过引擎改仓位状态（未自持 `master_id`/子仓状态）
- [ ] 指标全量重算，无增量缓存
- [ ] `portfolio.master.state == Closing` 时不开仓/不加仓/不调仓
- [ ] 等权目标乘 `portfolio.max_total_notional`，未用全额 NAV
- [ ] 组合平仓用 `CloseMaster`，未用逐标的 `Close` 模拟

## B. 注册与命名

- [ ] `strategy_type()` == `register_strategy!` name == 配置名
      （`strategy.yaml` 的 `strategy_type:` / 旧 per-symbol 的 `strategy:`）
- [ ] 文件名 == `rust_module` == 模块名
- [ ] `Params::default()` 与 `config/*/example.yaml` 一致

## C. 测试

- [ ] 入场测试（至少一个多头/一个空头）
- [ ] 出场测试（止损或反转）
- [ ] 数据不足 / `stale` / 缺失特征静默测试
- [ ] 横截面：`rebalance_due` 门控、掉出 `top_k` 清零、风险预算目标数量
- [ ] `cargo test --workspace` 全绿
- [ ] `cargo clippy --all-targets -- -D warnings` 零告警

## D. 配置与文档

- [ ] 配置存在且可跑：统一端口用 `config/<group>/strategy.yaml`，
      旧单标用 `config/<name>/<SYMBOL>.yaml`（或 `example.yaml`）
- [ ] `docs/strategy/specs/<name>.strategy-spec.yaml` 存在
- [ ] 若改了行为，`docs/strategy/` 相关文档已更新（仓位/容量改动同步 POSITION_MODEL.md）

## E. 市场差异（crypto / 期货，详见 CRYPTO_VS_FUTURES.md）

- [ ] 目标数量从 `portfolio.multipliers` 取乘数，未写死 `multiplier`
- [ ] 期货 symbol 已在 `config/contracts.yaml` 声明（乘数/保证金/费率/时段）
- [ ] 期货配置显式 `initial_cash`，且保证金至少能开 1 手
- [ ] 未假设 7x24；未自行拼接日盘/夜盘/午休，未自行按 UTC 断交易日
- [ ] 未在策略内模拟平今/平昨；平仓走 `Close`/`CloseMaster`/`Adjust`
- [ ] `params` 不含费率、保证金率、合约乘数、交易时段
- [ ] 期货用例覆盖 `multiplier != 1` 与整数手取整
- [ ] 期货数据位置已由用户确认（逐合约目录或 1m CSV），未用占位文件回测
