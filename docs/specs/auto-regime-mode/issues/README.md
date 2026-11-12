# 票索引

市场环境口径默认改为逐日自动判断（auto）的票按编号推进；**下一个可用编号：04**。

状态取值见 `docs/agents/triage-labels.md`（含终态 `done`）。`ready-for-agent` 且依赖全清的那一张就是当前 frontier。

| 票 | 主题 | 状态 | 依赖 |
| --- | --- | --- | --- |
| [01](01-daily-regime-observability.md) | 逐日市况观测基建（prefactor） | ready-for-agent | — |
| [02](02-auto-regime-default.md) | `auto` 默认口径主实现 | ready-for-agent | 01 |
| [03](03-three-mode-ab-experiment.md) | 同一天三口径 A/B 实验卡 | ready-for-agent | 02 |

## 开新票前的两句提醒

- 每张票必须声明 `Gate:` / `Signal set:` /（条件性）`Gate rationale:`，并让 `Status:` 与本索引一致；`python scripts/check_spec_tickets.py` 在 CI 的 `ai-governance` 里强校验。
- 03 取数依赖 02，但 02 不依赖 03；用户 2026-11-12 裁决「与实现并行」，不得以 03 未完成为由阻塞 02。
- 默认翻转的豁免见 `docs/adr/0006-auto-regime-mode-default.md`；术语见 `GLOSSARY.md`「市场环境与市况」段。
