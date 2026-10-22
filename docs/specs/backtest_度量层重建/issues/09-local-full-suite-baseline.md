# 09: 本地全量 pytest 基线不可用（195 failed / 4 collection errors）

**What to build:** 让 `python -m pytest -m "not network"` 在项目约定的 conda 环境里成为一个可用的信号——要么跑绿，要么把已知失败收敛成一份显式、有理由、被 CI 强制的白名单。当前状态是：全量红灯，无法区分"我引入的失败"和"既有失败"，智能体与贡献者只能退化为跑定向测试，仓库最重要的一层回归保护在本地形同不存在。

**Blocked by:** None。

**Status:** needs-triage

## 观测到的现状（2026-10-22，`daily_stock_analysis` 环境）

```
python -m pytest -m "not network" -q
→ 7780 passed / 195 failed / 6 skipped / 4 collection errors
```

失败疑似集中在：

- `tests/` 下 `system_config_service` / `api` 相关用例
- `usage_api` 相关用例
- `tickflow_routing` 相关用例

同一批用例在 CI（`backend-gate`，三 shard）中是绿的，因此高度怀疑是**用例间顺序依赖/共享状态污染**，或本地环境差异（依赖版本、缺失的外部服务桩）。这一点尚未验证。

## 待办

- [ ] 复现并记录原始失败清单（完整 nodeid + 报错首行），入库为 ticket 附件或 `docs/` 下的一节，不贴截图。
- [ ] 分组归因：逐个失败组单独运行（`pytest <file> -q`），区分三类——(a) 单跑也失败（环境/真 bug）、(b) 单跑通过但全量失败（顺序/共享状态）、(c) 依赖网络或外部服务（应打 `network` marker）。
- [ ] 对 (b) 类定位污染源（模块级可变全局、未清理的 DB/缓存 fixture、`sys.modules` 或环境变量泄漏），按根因修复；不得用 `-p no:randomly`、打乱顺序开关或 `xfail` 掩盖。
- [ ] 对 (a) 类判定是环境缺口还是真实缺陷：环境缺口写进 `AGENTS.md` 的环境说明，真实缺陷单独立项。
- [ ] 收敛后把基线数字写进 `AGENTS.md` §4（"当前全量基线：N passed"），并确认 `./scripts/ci_gate.sh` 在本地可通过。

## 验收标准

1. `python -m pytest -m "not network"` 在干净检出 + 文档所述环境准备步骤下**全绿**；或
2. 存在一份显式白名单（文件 + 理由 + 到期复核条件），被 `ci_gate` 读取，且白名单之外任何失败都阻断。两个选项都不允许"跳过就算过"。

## Notes

- 这不是本次门禁工作的副产品，而是独立的基础设施欠账：任何"跑全量确认"的指令当前都无法执行，只能靠定向测试兜底。
- 与 `AGENTS.md` §6 验证矩阵直接冲突：矩阵把 `./scripts/ci_gate.sh` 列为 Python 改动的优先验证手段，而它内部就跑全量。
- 修复过程中禁止为了让基线变绿而删除或弱化既有断言。
