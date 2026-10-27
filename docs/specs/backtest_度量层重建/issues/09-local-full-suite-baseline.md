# 09: 本地全量 pytest 基线不可用（195 failed / 4 collection errors）

**What to build:** 让 `python -m pytest -m "not network"` 成为一个可判读的信号：要么跑绿，要么把已知失败收敛成一份显式、有理由、被 CI 强制的白名单。当前状态是：全量红灯，无法区分"我引入的失败"和"既有失败"，贡献者只能退化为跑定向测试。

**Blocked by:** None。

**Gate:** `none`
**Signal set:** `none`

**Status:** ready-for-agent（决策 1 / 2 口径已定，见「已定口径」；剩余 3 项可 AFK 推进）

## 实测归因（2026-10-22，`daily_stock_analysis` 环境，native Windows）

复现命令（collection error 会中断整轮，必须显式继续）：

```
python -m pytest -m "not network" -q --tb=no -rf --continue-on-collection-errors
→ 195 failed / 7804 passed / 6 skipped / 4 deselected / 4 errors (917s)
```

隔离方式：失败组逐个单独跑（`pytest <file>`），对比全量结果，区分三类：

| 组 | 全量失败 | 单跑失败 | 根因 | 处置 |
| --- | --- | --- | --- | --- |
| `test_codex_app_server_gate_a.py` | 31 | 31 | `os.killpg` 在 native Windows 不存在 | 模块级 `skipif(os.name == "nt")` |
| `test_codex_tool_process.py` | 12 | 12 | `os.killpg` / `os.getpgid` 同上 | 模块级 `skipif(os.name == "nt")` |
| `test_screening_daily_freshness.py` | 21 | 21 | `time.tzset` 在 native Windows 不存在 | 模块级 `skipif(os.name != "posix")` |
| `test_intelligence_api.py` | 10 | 10 | 读到开发者根目录 `.env` 的 `ADMIN_AUTH_ENABLED=true` → 全站 401 | `ENV_FILE` 指向不存在路径，隔离 ambient 环境 |
| `test_multi_agent.py` / `test_agent_backend_status_service.py` / `test_agent_backend.py` | 43 | 43 | `ModuleNotFoundError: fxmacrodata_public`（生产模块硬导入） | 口径已定：可选导入（ADR 0005） |
| `test_local_cli_backend.py` | 10 | 10 | **4 类根因**，原归因不准确，见下方「归因更正」 | 口径已定 |
| `test_system_config_service.py` | 8 | 0 | 用例间顺序/共享状态污染 | 未修 |
| 其余零散（storage / llm_usage / market_analyzer 等） | 约 60 | 待逐项分类 | 混合 | 未修 |

关键结论：**195 个失败的主因不是顺序污染**，而是测试与部分被测路径只在 POSIX 下成立。`test_system_config_service.py` 是唯一确证的顺序污染组（单跑 225 passed）。

### `test_local_cli_backend.py` 归因更正（2026-10-25 实测）

原表把 10 个失败归为"权限断言 + 中文编码"。逐条复跑（`pytest tests/test_local_cli_backend.py`：
10 failed / 355 passed）后实际是 4 类根因，所以"探针中文编码同理"这个前提不成立：

| 根因 | 数量 | 位置 | 性质 |
| --- | --- | --- | --- |
| `0o600` / `0o700` 权限位断言 | 1 | 测试 `:382-383`；生产 `local_cli_backend.py:2157` / `:2170`，均 `except OSError: pass` | 断言方式与平台不符 |
| 子进程输出编码 | 1 | 子进程 stdio 落到文件后按 UTF-8 读回（`_read_text_file`），但子进程默认按 locale(gbk) 写；`:2209` 的 `text=True` 对文件句柄无效 | 真实跨平台缺陷，Windows 下中文输出乱码 |
| 存活探测 `os.kill(pid, 0)` | 1 | 测试 `:1376`（另两处 `:1019` / `:1084` 同形）；生产 `:2802` 的 `os.killpg` **已有** `os.name == "nt"` 分支 | 真实缺陷：Windows 上 `os.kill(pid, 0)` 会真的终止进程并抛 `WinError 87` |
| `\r\n` vs `\n` | 7 | 断言硬编码 `'...\n'`，预览保留子进程 stderr 的 CRLF | 平台差异落在断言上，不是缺陷 |

## 4 个 collection error

1. `fxmacrodata_public` 缺失（3 个文件）。该包在 `requirements.txt` 里固定为 GitHub Release 的 wheel，PyPI 与国内镜像均无包；本机到 `github.com:443` 不可达，`pip`/`curl` 都超时。→ 网络受限的环境缺口，不是"忘了装"。
2. `tests/test_baostock_history_xr.py` 导入 `scripts/data_collection/baostock_history_xr.py`，该模块在 `c28b4343`（2026-08-01，统一改用 tushare）被删除，测试未同步删除。`c28b4343` 是 HEAD 的祖先 → **HEAD 在任意平台都无法完成 collection**，CI 同样命中。

## 本轮后基线（同机同命令，2026-10-22）

```
→ 119 failed / 7613 passed / 273 skipped / 3 errors (1149s)
```

失败数 195 → 119（−76），collection error 4 → 3（剩余 3 个均为 `fxmacrodata_public` 缺失导致的模块级导入失败，一个文件一个 error）。skipped 6 → 273，增量全部来自上述平台 skip 与该文件内既有的 POSIX-only 用例。

## 本轮已修

- 删除 `tests/test_baostock_history_xr.py`：被测模块已不存在，保留测试只能永久 collection error，不构成保护。
- `tests/test_intelligence_api.py`：把 `ENV_FILE` 指向临时目录下的不存在路径，测试不再依赖开发者本地 `.env`，10 个 401 用例转绿。
- Codex 两个模块：`resolve_command()` 本就在 native Windows 抛出 `capability_unsupported`（该能力显式不支持 Windows），且 `CodexToolProcessRunner` 只被这条路径使用，故按同仓既有惯例（`tests/test_agent_backend_status_service.py`）加平台 skip，而不是改生产代码去"支持"一个被明确排除的能力。
- `AGENTS.md` §4/§6：写明 native Windows 不构成全量基线、完整验证走 WSL/Docker，并记录 `fxmacrodata-public-client` 仅 GitHub Release 分发。

## 已定口径（2026-10-25，用户显式授权）

### 决策 1：`fxmacrodata_public` 降为可选导入，发布产物继续必需

采纳"可选导入"，把发布产物完整性交给既有打包探针。完整理由、补偿控制与备选方案见
[ADR 0005](../../../adr/0005-fxmacrodata-optional-import-with-packaged-probe.md)。

- 包缺失时 `fxmacrodata_tools` 导入成功、导出空 `ALL_FXMACRODATA_TOOLS` + 一次性显式信号；
  不得让 `ImportError` 冒泡到 `factory.get_tool_registry()`（否则 `risk_agent` /
  `portfolio_agent` / `intel_agent` 连带失败）。
- `fxmacrodata_tool_names(*operations, require_integration=True)` 在目录不可用时：无参返回 `[]`；
  点名 operation 默认显式报错。`risk_agent` / `portfolio_agent` 在类体里点名了具体操作、必须能
  在缺依赖时正常导入，故这两处显式传 `require_integration=False`——降级由一次性信号覆盖，
  不是无声例外。
- `main.py` 的冻结产物探针（`DSA_PACKAGED_IMPORT_PROBE=src.agent.factory`）保持并继续断言
  `fxmacrodata_` 工具存在，作为本决策的补偿控制。
- `requirements.txt` 的 pin 不变。

### 决策 2：`test_local_cli_backend.py` 按根因分开处理，不整文件 skip

- **子进程编码**（1 个）与 **存活探测**（1 个）在 Windows 上真实可达，按缺陷修复，不得 skip：
  编码在子进程环境里强制 `PYTHONIOENCODING=utf-8`，与 `_read_text_file` 的 UTF-8 读取对齐；
  存活探测改为跨平台实现，超时终止在 Windows 上改用 `taskkill /F /T` 走完整个进程树
  （`terminate()` 只杀直接子进程，会留下 CLI 自己派生的工具进程）。
  这与 Codex App Server 那组相反：后者由 `resolve_command()` 在 Windows 显式抛
  `capability_unsupported`，属被产品排除的能力。
- **CRLF vs LF**（7 个）在 `redact_diagnostic_text()`（`local_cli_backend.py:1702`，所有预览经
  `_preview_diagnostics()` 走这里）单点规范化为 LF，不逐条改断言——诊断文本本身也应平台稳定。
- **权限位**（1 个）只把 `prompt_mode` / `cwd_mode` 两行挂 `skipif(os.name != "posix")`；同一测试的
  prompt 内容、argv 顺序、配置键、环境剥离断言在 Windows 上照常执行。
- 不引入 Windows ACL：`chmod(0o600/0o700)` 在 Windows 上无法表达该语义，生产侧本已
  `except OSError: pass`，说明它不是 fail-fast 契约；`%TEMP%` 的默认 ACL 已提供每用户隔离。

## 本轮已实现（2026-10-25，native Windows 实测）

- `src/agent/tools/fxmacrodata_tools.py`：客户端改为可选导入；新增 `fxmacrodata_availability()`
  区分 `package_unavailable` / `catalogue_unavailable` / `available`，导入时打一条一次性降级
  信号；`fxmacrodata_tool_names()` 新增 `require_integration` 关键字；`risk_agent` / `portfolio_agent`
  两处类体调用显式传 `False`。`main.py` 冻结产物探针未改动。
- `src/llm/local_cli_backend.py`：`build_local_cli_env()` 强制子进程 `PYTHONIOENCODING=utf-8`；
  `redact_diagnostic_text()` 单点把 CRLF 归一化为 LF；Windows 分支先用 `CTRL_BREAK_EVENT` 请求
  优雅退出，但**不据此提前返回**——CTRL_BREAK 成功只证明直接子进程退出，CLI 自己派生的工具进程
  可能存活；随后调 `taskkill /F /T` 扫掉仍挂在父进程下的子树（`_terminate_windows_process_tree`）。
  这个提前返回在全量跑里表现为间歇失败（单跑绿、全量偶发红）。
  **残余风险**：父进程已退出时 `taskkill /T` 无法枚举其遗留子树，PID 回收后还可能指向他人；
  要彻底消除需在 spawn 时把子进程放进 Job Object（`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`），
  本票不做。
- 测试：`tests/test_local_cli_backend.py` 的存活探测改为跨平台 helper（`_process_is_alive`，
  Windows 上 `os.kill(pid, 0)` 会真的终止进程），mode 位断言收进 `_assert_posix_mode_bits`
  （非 POSIX 直接返回，其余断言照常执行），三处终止等待统一到 `_wait_until_process_exits`；
  新增 `tests/test_fxmacrodata_optional_import.py`；`tests/test_fxmacrodata_tools.py`
  在没有 wheel 时按契约 skip 而不是 collection error。

实测：

```
pytest tests/test_local_cli_backend.py                      → 365 passed（原 10 failed，连跑 3 次稳定）
pytest tests/test_fxmacrodata_optional_import.py            → 1 passed, 1 skipped
pytest tests/test_fxmacrodata_tools.py                      → 1 skipped（wheel 不可用）
pytest tests/test_multi_agent.py tests/test_agent_backend_status_service.py
       tests/test_agent_backend.py --co                     → 250 collected（原 collection error）
pytest 同上三个文件                                          → 14 failed, 234 passed, 2 skipped
pytest -m "not network" --continue-on-collection-errors      → 70~71 failed / 7705 passed / 275 skipped / 0 errors
                                                               （原 119 failed / 3 errors）
```

全量两次复跑的失败集只差一个无关的既有 flaky（`test_screening_api.py` 的截止时间用例）；
`test_local_cli_backend.py` / `test_fxmacrodata*` / 三个 agent 文件在全量下均无失败。

单文件复跑中还观察到 1 次 `test_output_stat_error_is_structured_and_kills_process_group` 以
`process_start_failed` 失败——该 reason 是执行块 `except OSError` 的包装，即 spawn 期瞬时错误，
随后 8 次连跑未复现。归为环境 flake，不是逻辑回归；若要在 CI 上根治，应把该用例对"子进程启动
成功"的前提显式化（重试或跳过），而不是继续放宽终止等待。

新增暴露项：`test_agent_backend_status_service.py` 有 14 个 Codex 状态用例在 native Windows 上
断言 `available is True`，但该后端在 Windows 由产品排除（同文件 `:238` / `:266` 已对该类用例用
`skipif(os.name == "nt")`）。这批失败原先被 `ModuleNotFoundError` 掩盖，现归入下方"剩余待决"分类，
不在决策 1 / 2 范围内处理。

## 分类结果（2026-10-26，按 junit 失败正文逐条归桶）

52 条 `TODO` 的归因。**只有第 1 桶属于平台契约**；其余都是真实缺陷或机器环境问题，不能进白名单：

| 桶 | 数量 | 证据（失败首行） | 处置 |
| --- | --- | --- | --- |
| Codex App Server 平台排除 | 14 | `'platform_unsupported' == 'capability_unsupported'` | **已改为 `skipif(os.name == "nt")`**；同文件 `test_native_windows_is_rejected_before_command_probe` 正是该契约 |
| sqlite 路径被当成目录 | 6 | `NotADirectoryError: WinError 267 …legacy.sqlite` / `…sqlite_daily_concurrency.db` | 真实缺陷，需修（`test_llm_usage` ×5、`test_storage` ×1） |
| 开发者 `.env` 环境污染 | 5 | `401 != 200`、`'tencent,efinance,tushare' != 'tencent,akshare_sina,…'`、`'true' != 'false'` | 与 `test_intelligence_api.py` 同款，隔离 `ENV_FILE` |
| `ConfigValidationError` | 7 | `Configuration validation failed` / `400 validation_failed` | 待定位（`system_config_service` ×6、`system_config_api` ×1） |
| 缺 POSIX shell | 4 | `FileNotFoundError: [WinError 2]` | `test_docker_entrypoint` ×3、macos 打包 argv ×1 |
| analyzer 回退文案 | 5 | `'fallback ok' == '安全兜底'` | 待定位（疑似 litellm stub 抢占真实路径） |
| 其余零散 | 11 | 各样 | 待逐条定位 |

**跨平台发现（重要）**：`test_check_env_encoding::test_requirements_file_is_ascii_decodable` 只是
`read_bytes().decode("ascii")`，而 `requirements.txt` 有 22 行中文注释（如 `# 时间支持`），因此它
**在任何平台都会失败**。票 09 的前提"POSIX 全绿"因此从未被验证、很可能不成立。处置二选一：把那
22 行注释改成 ASCII，或放宽该测试——**这条不属于 Windows 白名单**。

**计数口径差异（实测）**：Codex 改 skip 后复跑为 `56 failed / 7720 passed / 289 skipped`，但
`--junitxml` 只记录 **51** 条失败——差的 5 条是子测试级失败
（`test_notification::test_source_labels_render_in_all_supported_languages` 的参数化 subtest），
junit 不为其生成 testcase。因此白名单机制当前看不见子测试级失败，见票 10 的已知限制。

**白名单终态**：`docs/agents/windows-known-failures.md` 收敛为**空表**。只有"在 Windows 上难以
修复、且已明确接受"的失败才该入表；当前 52 条没有一条符合。在它们被修完前，checker 会把它们报成
`NEW`——这是正确信号，不是噪声。

### 修复进展（2026-10-26）

- **sqlite 文件句柄桶（6 条）：已修。** 根因是临时目录清理发生在引擎 dispose **之前**——Windows
  不允许 unlink 仍被打开的 sqlite 文件（POSIX 允许，所以只在 Windows 红）。`tests/test_llm_usage.py`
  新增 `_release_sqlite_handles()` 并在 5 个用例的 `with` 块末尾调用；`tests/test_storage.py` 的并发
  用例把 `reset_instance()` 移到 `temp_dir.cleanup()` 之前。
- **`.env` 桶（5 条）：只修了 2 条。** `test_data_capability_service` / `test_usage_api` 的 401 用
  autouse fixture 把 `ENV_FILE` 指向不存在路径解决（与 `test_intelligence_api.py` 同款）。另外 3 条
  根因不同：
  - `test_config_env_compat` / `test_tickflow_manager_routing` **既不是 `.env` 也不是派生环境变量**：
    `Config._resolve_realtime_source_priority()` 的代码默认值就是 `tencent,efinance,tushare`，而两个
    用例断言 `tencent,akshare_sina,efinance,akshare_em`，且用 `patch.dict(clear=True)` 清空了环境
    ——属于**断言与代码默认值不同步**，任何平台都会红。
  - `test_system_config_service::test_get_config_defaults_public_searxng_instances_off` 单独跑通过，
    属跨文件顺序污染（票 12）。
- **`ConfigValidationError` 桶（7 条）：归因修正。** `tests/test_system_config_service.py` 单跑
  **225 passed**，`tests/test_system_config_api.py` 单跑只剩 1 条失败——所以这 7 条里 6 条是
  **跨文件顺序污染**，1 条
  （`test_preview_agent_backend_status_uses_draft_without_persisting`）是真实失败。原表把整桶写成
  "待定位 ConfigValidationError"是错的。
- **`test_storage.py` 另有 3 条同症状、不同根因**：`finally` 里顺序已经正确，仍报 `WinError 32`，
  怀疑迁移代码里裸 `sqlite3.connect` 未关闭；属"其余零散"，未修。

复跑（native Windows，同一命令）：`48 failed / 7728 passed / 289 skipped`（修复前 `56 / 7720 / 289`），
junit 可见失败 `43`（修复前 `51`）；`python scripts/check_windows_test_baseline.py` 报
`0 registered / 43 failing`、`NEW (43)`、退出码 1——即"还剩 43 条未归因的真实失败"，与减法一致。

## 剩余待决

1. 上表 7 个桶里除第 1 桶外的 38 条，按桶推进：先修"sqlite 路径"与"`.env` 污染"这两组（有明确
   根因与同仓先例），再定位 `ConfigValidationError` 与 analyzer 回退。
2. `requirements.txt` 的 ASCII 化 vs 放宽测试，需要一个明确选择。
3. 修完后重跑 `python scripts/check_windows_test_baseline.py`，`NEW` 归零即达成票 09 验收标准 1。

## 后续票（不在本票范围）

- **native Windows 失败白名单机制**：验收标准 1 要求"新增失败立即显性"，但目前只有
  `docs/agents/validation-matrix.md` 的散文，没有任何脚本或 CI 在核对失败集合。待本票实现完成、
  剩余零散失败分类清楚后再单开一票（建议编号 10）。

## 验收标准

1. POSIX 环境（CI / WSL / Docker）下 `python -m pytest -m "not network"` 全绿；native Windows 下失败集合必须与本文档的白名单一致，且新增失败立即显性。
2. collection error 数为 0（已定：降级为可选导入，见「决策 1」与 ADR 0005）。
3. 任何 skip 必须写明平台契约理由；禁止用 `xfail`、关闭随机顺序或删除断言来"变绿"。

## Notes

- 修 `os.killpg` 一类问题时要分清：如果某能力在 Windows 上被产品显式排除（`resolve_command` 抛 `capability_unsupported`），正确做法是对齐平台 skip；只有在 Windows 上真实可达的路径才需要补平台分支。
- 该平台边界同时解释了一个反直觉现象：本机全量偏红既可能是环境缺口（依赖拉不到、POSIX-only 测试），也可能是真实回归，二者必须靠本票的白名单区分。
