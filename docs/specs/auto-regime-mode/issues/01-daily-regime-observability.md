# 01: 逐日市况观测基建（prefactor）

**What to build:** 在「默认 `enh2` 不变、信号集不变」的前提下，让一次回测即可产出可离线复算的逐日市况数据：把 `compute_market_ok` 已算出的逐日六列（`date` + `market_ok` / `market_ok_enh` / `market_ok_enh2` / `regime` / `breadth` + `vol_pct`）写入既有 `result/` 产物出口，并让启动日志能表达规则版本与逐日序列摘要。这是让后续选档「选得对不对」可被断言的前置观测手段，不新增平行目录、不改任何可开仓判定。

**Blocked by:** None (can start immediately)

**Gate:** `none`
**Signal set:** `none`

**Status:** done

- [x] 逐日市况表落盘到既有 `result/` 产物出口：行数等于交易日数，含 `date`、三档布尔列、`regime`、`breadth`、`vol_pct`；不新建平行目录。
- [x] 启动日志在既有「市场环境」串上可表达规则版本与逐日序列摘要，保持 runbook「先确认这两行」的可读性。
- [x] 现有 `enh2` 默认下全流程信号集逐位不变，冻结基线 `config_hash` 与已发布值一致。
- [x] 有定向测试覆盖：缺列/空数据时的行为可观测，不静默返回空。
- [x] `py_compile` 与定向 `pwsh -File scripts/pytest.ps1 <files>` 通过；按 `docs/agents/validation-matrix.md` 说明全量 gate 覆盖与缺口。

## Comments

- 实现：`compute_market_ok` 返回值新增 `vol_pct`（enh2 定义所用原始分位，值不变）；
  `write_daily_regime_table` / `format_regime_log_summary` / `export_daily_regime`
  三个函数构成观测 seam，main() 在 :6604 合并处单点调用，写入既有 `_PORTFOLIO_SIM_OUT`
  （默认 `result/`）下的 `daily_regime.csv`，并打一行「规则版本 + 覆盖标记 + 三列计数」日志。
- 规则版本常量 `RULE_VERSION_FIXED` / `RULE_VERSION_AUTO` 随票 01 落地，票 02 复用。
- 测试：`tests/test_auto_regime_observability.py`（11 passed）；
  `tests/test_portfolio_sim_integration.py`（12 passed）确认冻结基线哈希
  `5eea0519…` 逐位不变。全量 gate 见最终交付说明。

**Note:** prefactor，先于 02 落地；术语遵循 `GLOSSARY.md`「市场环境与市况」段。
