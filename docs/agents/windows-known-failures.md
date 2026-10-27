# native Windows 已知失败白名单

`python -m pytest -m "not network"` 在 native Windows 上不构成全量基线（见
`docs/agents/validation-matrix.md` 的 Platform boundary）。本文件登记**已经归因**的既有失败，
让"新增失败""白名单过期""未填理由"三类问题各自显性，而不是混在同一堆红字里。

- 生成/更新：`python scripts/check_windows_test_baseline.py --update`
- 校验：`python scripts/check_windows_test_baseline.py`
- `类别` 只能取 `product-excluded` / `stdlib-missing` / `posix-mode-bits`（定义见 validation-matrix）。
- `理由` 写作 `TODO` 表示尚未归因；checker 会因此失败，**不要**把未分类失败当成已归因。
- 本文件只登记失败，不改变测试结果；真正的平台契约排除仍应写成 `skipif`。
- `test` 用 JUnit 的 `classname::name`，与 `pytest -rf` 打印的路径形式不逐字相同，新增条目请走 `--update`。
- **登记前提是该用例在目标平台上确定性失败**。偶发失败（同机两次跑结果不同）不是已知失败，
  必须修掉或显式隔离，不能靠白名单吸收；否则 `NEW` / `STALE` 会随机抖动。

| test | 类别 | 理由 |
| --- | --- | --- |

当前为空。历史种子（66 条）已在票 09「分类结果」里按根因归桶：14 条 Codex 已改为平台 skip，其余 52 条是真实缺陷或机器环境问题，不属于"已接受的 Windows 失败"，因此不入表。
