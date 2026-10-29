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

| test | 类别 | 理由 |
| --- | --- | --- |
| `tests.test_agent_frozen_context.DesktopBackendPackagingAssetsTestCase::test_builtin_strategy_yaml_inventory_matches_expected_desktop_bundle` | known-unattributed | 既有失败，根因未分类（票 12） |
| `tests.test_fundamental_adapter_contracts::test_capital_flow_real_akshare_missing_sector_amounts_reach_manager_and_agent` | known-unattributed | 既有失败，根因未分类（票 12） |
| `tests.test_fundamental_timeout_guard::test_capital_flow_quarantine_preserves_other_blocks_and_recovers` | known-unattributed | 时序断言：超时/隔离时序在慢机上不稳定（未定性，票 12） |
| `tests.test_fundamental_timeout_guard::test_repeated_capital_flow_timeouts_do_not_spawn_more_background_workers` | known-unattributed | 时序断言：超时/隔离时序在慢机上不稳定（未定性，票 12） |
| `tests.test_intelligence_service.IntelligenceServiceTestCase::test_fetch_enabled_sources_is_fail_open` | known-unattributed | 既有失败，根因未分类（票 12） |
| `tests.test_miaoxiang_fetcher.TestSupplementRuntimeContracts::test_concurrent_burst_does_not_leave_late_quota_requests[False]` | known-unattributed | 并发配额时序断言（未定性，票 12） |
| `tests.test_miaoxiang_fetcher.TestSupplementRuntimeContracts::test_concurrent_burst_does_not_leave_late_quota_requests[True]` | known-unattributed | 并发配额时序断言（未定性，票 12） |
| `tests.test_notification.TestNotificationServiceReportGeneration::test_generate_aggregate_report_routes_by_report_type` | known-unattributed | 渲染断言依赖 litellm 桩；另有子测试级失败 junit 不记录（票 12） |
| `tests.test_notification.TestNotificationServiceReportGeneration::test_source_labels_render_in_all_supported_languages` | known-unattributed | 渲染断言依赖 litellm 桩；另有子测试级失败 junit 不记录（票 12） |
| `tests.test_packaging_build_scripts::test_macos_pyinstaller_command_preserves_real_bash_argv` | known-unattributed | macos 打包脚本按 bash argv 断言，Windows 无该语义（票 12） |
| `tests.test_pipeline_index_targets.PipelineDailySourceAttributionTestCase::test_v10_traditional_branch_persists_and_renders_daily_source` | known-unattributed | 数据源归因渲染断言（未定性，票 12） |
| `tests.test_runtime_scheduler_service.RuntimeSchedulerServiceTestCase::test_child_restores_due_occurrence_and_returns_skip_status` | known-unattributed | 调度子进程时序断言（未定性，票 12） |
