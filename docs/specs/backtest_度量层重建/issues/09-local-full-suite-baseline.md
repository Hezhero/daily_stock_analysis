# 09: 本地全量 pytest 基线不可用（195 failed / 4 collection errors）

**What to build:** 让 `python -m pytest -m "not network"` 成为一个可判读的信号：要么跑绿，要么把已知失败收敛成一份显式、有理由、被 CI 强制的白名单。当前状态是：全量红灯，无法区分"我引入的失败"和"既有失败"，贡献者只能退化为跑定向测试。

**Blocked by:** None。

**Gate:** `none`
**Signal set:** `none`

**Status:** ready-for-human（剩余项需要先定口径，见末节）

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
| `test_multi_agent.py` / `test_agent_backend_status_service.py` / `test_agent_backend.py` | 43 | 43 | `ModuleNotFoundError: fxmacrodata_public`（生产模块硬导入） | 环境缺口，未修 |
| `test_local_cli_backend.py` | 10 | 10 | 断言 `0o600`/`0o700` 文件权限 + 探针脚本中文编码 | 未修 |
| `test_system_config_service.py` | 8 | 0 | 用例间顺序/共享状态污染 | 未修 |
| 其余零散（storage / llm_usage / market_analyzer 等） | 约 60 | 待逐项分类 | 混合 | 未修 |

关键结论：**195 个失败的主因不是顺序污染**，而是测试与部分被测路径只在 POSIX 下成立。`test_system_config_service.py` 是唯一确证的顺序污染组（单跑 225 passed）。

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

## 剩余待决

1. **`fxmacrodata_public` 级联（约 43 个用例）**：是把该依赖改成可选导入（生产代码降级），还是在仓库文档里保持"必需依赖 + 只支持可访问 GitHub 的环境"？前者会改变 `main.py` 对工具注册表的断言语义。
2. **`test_local_cli_backend.py` 的 POSIX 权限断言**：改为按平台断言（Windows 下跳过 `0o600` 检查），还是让生产代码在 Windows 上用 ACL？探针脚本的中文编码同理。
3. **`test_system_config_service.py` 顺序污染（8 个）**：可用 `pytest <file>` 单独复现差异，定位污染源（模块级可变全局 / 未清理的 fixture / env 泄漏）。
4. **零散失败逐项分类**：`test_storage.py`、`test_llm_usage.py`、`test_market_analyzer_generate_text.py` 等尚未逐条判定。

## 验收标准

1. POSIX 环境（CI / WSL / Docker）下 `python -m pytest -m "not network"` 全绿；native Windows 下失败集合必须与本文档的白名单一致，且新增失败立即显性。
2. collection error 数为 0（`fxmacrodata_public` 缺失要么被认定必需并在文档中前置说明，要么被降级为可选导入）。
3. 任何 skip 必须写明平台契约理由；禁止用 `xfail`、关闭随机顺序或删除断言来"变绿"。

## Notes

- 修 `os.killpg` 一类问题时要分清：如果某能力在 Windows 上被产品显式排除（`resolve_command` 抛 `capability_unsupported`），正确做法是对齐平台 skip；只有在 Windows 上真实可达的路径才需要补平台分支。
- 该平台边界同时解释了一个反直觉现象：本机全量偏红既可能是环境缺口（依赖拉不到、POSIX-only 测试），也可能是真实回归，二者必须靠本票的白名单区分。
