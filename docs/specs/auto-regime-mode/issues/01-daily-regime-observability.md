# 01: 逐日市况观测基建（prefactor）

**What to build:** 在「默认 `enh2` 不变、信号集不变」的前提下，让一次回测即可产出可离线复算的逐日市况数据：把 `compute_market_ok` 已算出的逐日六列（`date` + `market_ok` / `market_ok_enh` / `market_ok_enh2` / `regime` / `breadth` + `vol_pct`）写入既有 `result/` 产物出口，并让启动日志能表达规则版本与逐日序列摘要。这是让后续选档「选得对不对」可被断言的前置观测手段，不新增平行目录、不改任何可开仓判定。

**Blocked by:** None (can start immediately)

**Gate:** `none`
**Signal set:** `none`

**Status:** ready-for-agent

- [ ] 逐日市况表落盘到既有 `result/` 产物出口：行数等于交易日数，含 `date`、三档布尔列、`regime`、`breadth`、`vol_pct`；不新建平行目录。
- [ ] 启动日志在既有「市场环境」串上可表达规则版本与逐日序列摘要，保持 runbook「先确认这两行」的可读性。
- [ ] 现有 `enh2` 默认下全流程信号集逐位不变，冻结基线 `config_hash` 与已发布值一致。
- [ ] 有定向测试覆盖：缺列/空数据时的行为可观测，不静默返回空。
- [ ] `py_compile` 与定向 `pwsh -File scripts/pytest.ps1 <files>` 通过；按 `docs/agents/validation-matrix.md` 说明全量 gate 覆盖与缺口。

**Note:** prefactor，先于 02 落地；术语遵循 `GLOSSARY.md`「市场环境与市况」段。
