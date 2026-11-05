# native Windows 已知失败白名单

`python -m pytest -m "not network"` 在 native Windows 上不构成全量基线（见
`docs/agents/validation-matrix.md` 的 Platform boundary）。本文件登记**已经归因**的既有失败，
让"新增失败""白名单过期""未填理由"三类问题各自显性，而不是混在同一堆红字里。

- 生成/更新：`python scripts/check_windows_test_baseline.py --update`
- 校验：`python scripts/check_windows_test_baseline.py`
- `类别` 取 `product-excluded` / `stdlib-missing` / `posix-mode-bits`（定义见 validation-matrix），
  或 `known-unattributed`：既有失败、根因已记录但**不属于**任何平台契约。后者只用于把已知失败固定住，
  让新增失败显性；它不是"平台允许的差异"，不允许用它掩盖平台契约问题。
- `理由` 写作 `TODO` 表示尚未归因；checker 会因此失败，**不要**把未分类失败当成已归因。
- 本文件只登记失败，不改变测试结果；真正的平台契约排除仍应写成 `skipif`。
- `test` 用 JUnit 的 `classname::name`，与 `pytest -rf` 打印的路径形式不逐字相同，新增条目请走 `--update`。
- **登记前提是该用例在目标平台上确定性失败**。偶发失败（同机两次跑结果不同）不是已知失败，
  必须修掉或显式隔离，不能靠白名单吸收；否则 `NEW` / `STALE` 会随机抖动。
- **子测试级失败**（`unittest.subTest`）不会出现在 JUnit 报告里。checker 用 pytest 汇总的
  `N failed` 减去 JUnit 失败数得到 `UNREPRESENTED`；当前基线为 0。`--max-unrepresented`
  （默认 0）在出现子测试级失败时立即失败，避免它被静默吞掉。
- `--leak-probe` 会以 `DSA_ENV_LEAK_PROBE=1` 运行；该模式**关闭**逐用例环境还原，让
  `pytest_runtest_setup` 能观察到每个用例真正留下的 env 写入，并把过渡视为失败；单例过渡只
  作为信息输出。默认关闭，避免把既有泄漏直接变成红基线。

| test | 类别 | 理由 |
| --- | --- | --- |
| `tests.test_packaging_build_scripts::test_macos_pyinstaller_command_preserves_real_bash_argv` | known-unattributed | macos 打包脚本按 bash argv 断言，Windows 无该语义（票 12） |
| `tests.test_runtime_scheduler_service.RuntimeSchedulerServiceTestCase::test_child_restores_due_occurrence_and_returns_skip_status` | known-unattributed | 调度子进程时序断言（未定性，票 12） |
