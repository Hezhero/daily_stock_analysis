# 02: `auto` 默认口径主实现

**What to build:** 端到端落地「不传 `--regime-mode` 即逐日自动判断」：从 CLI 输入、逐日选档、启动日志标注、到 `config_hash` 登记与文档同步，形成可独立 demo 的垂直切片。显式传 `enh2` / `enh` / `strict` 时恒定覆盖并带标记；`enh2` 退化为显式覆盖值，同时是 `auto` 的基准与冲突回退档；既有 `--strict` 与显式档位的优先级不变。实现后消除设计轮文档里「当前代码尚未实现 `auto`」的标注。

**Blocked by:** 01（逐日市况观测基建：选档结果需可离线复算才能断言）

**Gate:** `v1-signal`
**Signal set:** `changed`
**Gate rationale:** `docs/adr/0006-auto-regime-mode-default.md`「后果」段记录用户 2026-11-12 对默认翻转的显式豁免（ADR 0004「不翻转任何默认值」、《指标契约》预注册放行规则、X5「不做」）；本票改变可开仓判定列，故按 v1-signal 登记，不再重开该争论。

**Status:** done

- [x] 默认 `auto`：逐日只用截至当日 as-of 的市况列值选档；显式传档位时恒定覆盖，启动日志打印覆盖标记。
- [x] 保守门控：基准与冲突回退均为 `enh2`；仅 `regime=bull` 且 `breadth≥0.45` 且 `vol_pct≤0.85` 时放宽到 `enh`；`bear` / 高波动 / 数据缺失 / 判定冲突一律回退 `enh2` 并对缺失打 WARNING；`strict` 永不自动选中。
- [x] 前视约束可验证：改动「未来行」不影响历史行的选档结果；不使用每轮按最近 N 日选一次、全期占比、`stationarity_score`、季度 WF 权重等事后量。
- [x] 治理：三列算法定义冻结；`auto` 规则版本 `auto_v1`、阈值与回退行为按既有 `_REGISTERED_EXPERIMENT_DEFAULTS` 登记为 `regime_selector`；规则版本单值计入 `config_hash`，逐日序列为数据确定性函数不单独入哈希；默认翻转的「默认翻转不改变哈希」陷阱按《指标契约》§2.1「保持已发布哈希 + 运行时注入」处置并补修订行。
- [x] 显式传 `--regime-mode enh2` 时，冻结 PIT 基线、10 月四张实验卡、2026-10-29 起 0/5 日志的口径与哈希逐位可复现。
- [x] `BACKTEST_REGIME_SOFT` / `--regime-soft` / `--regime-filter` / `--defensive-half-pos` 行为不变，且不与自动选档叠加。
- [x] 文档同步：`docs/CHANGELOG.md` 实现条目、《指标契约》与 `docs/backtest-runbook.md` / ADR 0006 / spec 去掉「代码尚未实现」标注；`.env.example` 无新增可见配置（auto 是 CLI 默认值、非 env 开关）；`scripts/check_ai_assets.py` / `check_spec_tickets.py` 通过。
- [x] 按已确认的「1 主 seam（口径解析统一入口）+ 1 观测 seam（启动日志/逐日市况表）」写测试，只断言外部行为；`py_compile`、定向 `pytest.ps1` 通过；全量 gate 见最终交付说明。

## Comments

- 主 seam = `select_auto_regime_mode()`（逐日 as-of 纯数据函数）+ `resolve_regime_mode()`（CLI 统一解析）
  + `_regime_col("auto") -> "market_ok_auto"`；下游 `_entry_mask` / 验证统计 / handoff 无新分支。
- 观测 seam = 启动日志「口径标记 / 规则版本 / 逐日选档序列」+ `result/daily_regime.csv`（追加
  `auto_mode` / `market_ok_auto` 两列）；复用票 01 出口，不新增平行目录。
- `config_hash`：见《指标契约》§2.1；显式 `enh2` 哈希 = `5eea0519…`（测试断言）。
- 测试：`tests/test_auto_regime_default.py`（16 passed）、`tests/test_auto_regime_observability.py`
  （11 passed）、`test_portfolio_sim_integration.py`（12 passed，冻结哈希守卫）。
- 未跑：实盘 `main.py` 子进程链路、完整 5 年回测实跑（按 R4/Q8 未授权）；A/B 对照留票 03。

**Note:** 不做「减少 0 推荐 / 提高收益」的产品语义反转；`0 只股票` 仍是 runbook §4 的正常观测。不处理共振 / handoff 层的零推荐成因。术语遵循 `GLOSSARY.md`；取舍见 `docs/adr/0006`。
