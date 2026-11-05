# Validation matrix

Read this before delivering any change: pick the row that matches the touched surface,
run its commands, and state the gap when a check could not run.

> **完成判据**：POSIX 验证 = CI 对当前 HEAD 有成功 run，或 WSL/Docker 的 `ci_gate.sh` 通过。
> “pending CI” 不是验证状态；先用 `python scripts/ci_status.py` 确认 HEAD 是否真的有 run。

## CI coverage

| 检查项 | 来源 | 说明 | 是否阻断 |
| --- | --- | --- | --- |
| `ai-governance` | `.github/workflows/ci.yml` | 校验 `AGENTS.md` / `CLAUDE.md` / `.github` 指令 / `.claude/skills` 关系，并执行 `scripts/check_spec_tickets.py` | 是 |
| `backend-gate` | `.github/workflows/ci.yml` | 执行 `./scripts/ci_gate.sh` | 是 |
| `docker-build` | `.github/workflows/ci.yml` | Docker 构建与关键模块导入 smoke | 是 |
| `web-gate` | `.github/workflows/ci.yml` | 前端改动时执行 `npm run lint` + `npm run build` | 是（触发时） |
| `network-smoke` | `.github/workflows/network-smoke.yml` | `pytest -m network` + `scripts/test.sh quick` | 否，观测项 |
| `windows-baseline` | `.github/workflows/windows-baseline.yml` | native Windows 全量对照白名单，夜间 + 手动触发 | 否，观测项（稳定前 `continue-on-error`） |
| `pr-review` | `.github/workflows/pr-review.yml` | PR 静态检查 + AI 审查 + 自动标签 | 否，辅助项 |

若 PR 上已有对应 CI 结果，可直接引用 CI 结论；若 CI 未覆盖改动面，或本地与 CI 环境差异较大，
需要补充说明本地验证与缺口。

## Commands

```bash
pip install -r .github/requirements-ci.txt   # requirements.txt + flake8 + pytest + pytest-timeout
./scripts/ci_gate.sh                          # POSIX / Git Bash / WSL
pwsh -File ./scripts/ci_gate.ps1              # Windows PowerShell wrapper (resolves the interpreter + Git Bash)
pwsh -File ./scripts/ci_gate_wsl.ps1 -Phase offline-tests   # POSIX fallback when CI is unavailable
```

`ci_gate.sh` uses `${PYTHON:-python}` and preflights its tools; it exits `2` with the
install command when the interpreter, `flake8`, `pytest`, or `pytest_timeout` is missing.
Flake8 also runs as `"$PYTHON" -m flake8`, so a stray base-environment flake8 cannot mask
a missing dependency. The `governance` phase runs `check_ai_assets.py` and
`check_spec_tickets.py`, so the two checks that used to live only in the `ai-governance`
CI job still run locally when Actions is unavailable.
On native Windows (`MINGW*`/`MSYS*`/`CYGWIN*`) the `offline-tests` phase runs
`scripts/check_windows_test_baseline.py` instead of raw pytest, so known platform
differences do not hide new failures. The `.ps1` wrapper resolves the interpreter and
locates Git Bash; it does not install anything.

On Windows, `ci_gate.ps1` resolves the project interpreter in this order: `-Python`,
`DSA_PYTHON`, `conda run -n daily_stock_analysis python -c "import sys; print(sys.executable)"`,
then `python`. It sets `PYTHON` and prepends the interpreter's directory to `PATH`, so the
Git Bash child no longer silently falls back to the base environment. If resolution fails,
activate `daily_stock_analysis` first or pass `-Python <path>`.

### Fast iteration loop

The full suite is delivery evidence, not an edit loop:

1. Run the touched files: `pwsh -File scripts/pytest.ps1 <test files> -q`.
2. Re-run only the last failures by appending `--lf` to the same file list.
3. Before commit/delivery run the full gate: `pwsh -File scripts/ci_gate_wsl.ps1 -Phase offline-tests`
   (POSIX) and `pwsh -File scripts/ci_gate.ps1 offline-tests` (native Windows).

`--lf` is an iteration aid; never quote it as the verification result, and re-run the full
gate after the last fix.

## CI unavailable: WSL fallback

GitHub Actions can be unavailable (billing limit, disabled workflow, fork settings).
Do not report "pending CI" as a verification state. Confirm whether HEAD has a run, then
use WSL when it does not:

```powershell
python scripts/ci_status.py                              # exit 1 when HEAD has no run
pwsh -File scripts/ci_gate_wsl.ps1 -Phase offline-tests   # POSIX fallback
```

`ci_gate_wsl.ps1` creates `/tmp/dsa-venv` and installs the CI toolchain when the
dependency files change. It copies the worktree to an ext4 mirror (`/tmp/dsa-src`,
excluding `.git` / `data` / `logs` / `node_modules`) and runs the gate there: on
`/mnt/<drive>` (DrvFs) a `multiprocessing` child re-importing pandas can take longer
than the 5s startup grace used by the scheduler / codex tests, which produces failures
that do not exist on a POSIX filesystem. Keep `apps/dsa-web/public/stocks.index.json`
in the mirror — the stock/index identity tests read it. Pass `-InPlace` to run against
the mounted worktree when debugging filesystem-specific behavior. The optional
`fxmacrodata-public-client` GitHub-Release wheel returns 403 from WSL, so the installer
strips that line and relies on the optional-import fallback documented in ADR 0005.
Pass `-PipIndex <url>` when the default PyPI endpoint is slow. Ubuntu 24.04 provides
Python 3.12 while CI uses 3.11; treat the result as a POSIX behavior signal, not a
replacement for the CI version matrix.

If neither CI nor WSL is available, state the gap explicitly in the delivery and do not
claim POSIX coverage.

When `gh` is available, the PR/CI evidence commands are:

```bash
gh pr view <pr_number>
gh pr checks <pr_number>
gh run view <run_id> --log-failed
```

`gh` unavailable: use `ci_status.py` (public repo, no token). A missing run is not
"pending".

### Local pre-commit guardrail

Enable the fast hook once per clone:

```bash
git config core.hooksPath .githooks
```

The hook runs `ci_gate.sh flake8` (or `ci_gate.ps1 flake8`) and adds the `governance`
phase when staged files touch `AGENTS.md`, the Copilot mirror, `.claude/skills/`, or
`docs/specs/`. The full suite stays a delivery step, not a commit step; bypass with
`git commit --no-verify` only when you knowingly accept the gap.

## Per-surface checks

- **Python 后端改动**：
  - 适用范围：`main.py`、`src/`、`data_provider/`、`api/`、`bot/`、`tests/`
  - 优先执行：`./scripts/ci_gate.sh`（Windows：`pwsh -File ./scripts/ci_gate.ps1`）
  - 最低要求：`python -m py_compile <changed_python_files>`
  - 若影响 API、任务编排、报告生成、通知发送、数据源 fallback、认证、调度，交付说明中要写明是否覆盖了对应路径。

- **Web 前端改动**：
  - 适用范围：`apps/dsa-web/`
  - 默认执行：`cd apps/dsa-web && npm ci && npm run lint && npm run build`
  - 若涉及 API 联调、路由、状态管理、Markdown/图表渲染或认证状态，交付说明中要明确说明联动面和未覆盖风险。

- **桌面端改动**：
  - 适用范围：`apps/dsa-desktop/`、`scripts/run-desktop.ps1`、`scripts/build-desktop*.ps1`、`scripts/build-*.sh`、`docs/desktop-package.md`
  - 默认执行：先构建 Web，再构建桌面端
  - 如受平台限制未能完整验证，需要明确说明是否验证了 Web 构建产物、Electron 构建以及 Release 工作流影响。

- **API / Schema / 认证联动改动**：
  - 适用范围：`api/**`、`src/schemas/**`、`src/services/**`、`apps/dsa-web/**`、`apps/dsa-desktop/**`
  - 至少覆盖对应后端验证 + 受影响客户端构建验证。
  - 若涉及登录、Cookie、会话、轮询状态、字段增删或枚举变化，必须明确写出兼容性影响。

- **文档与治理文件改动**：
  - 适用范围：`README.md`、`docs/**`、`AGENTS.md`、`.github/copilot-instructions.md`、`.github/instructions/**`、`.claude/skills/**`
  - 不强制代码测试。
  - 需确认命令、配置项、文件名、工作流名称与实际仓库一致。
  - 改动 AI 协作治理资产时，执行 `python scripts/check_ai_assets.py`。
  - 改动 `docs/specs/*/issues/` 时，执行 `python scripts/check_spec_tickets.py`。

- **工作流 / 脚本 / Docker 改动**：
  - 适用范围：`.github/**`、`scripts/**`、`docker/**`
  - 运行最接近改动面的本地验证。
  - 交付时说明影响了哪条流水线、发布路径或部署路径。
  - 若未执行 Docker / GitHub Actions 相关验证，明确说明原因与潜在风险。

- **网络或三方依赖相关改动**：
  - 先跑离线或确定性检查。
  - 优先确认 timeout、retry、fallback、异常文案、降级路径是否仍然成立。
  - 若未执行在线验证，必须明确写出原因。

## Platform boundary

`python -m pytest -m "not network"` 只在 POSIX 上构成全量基线。native Windows 下，
Codex App Server 相关用例（`os.killpg`/`getpgid`）、`time.tzset` 时区用例按契约跳过，
因此本机全量结果偏红不代表改动引入回归；需要全量基线时在 WSL 或 Docker 里跑同一条命令。
新增依赖时优先选择 PyPI 可解析的包，避免让本地基线依赖单一分发渠道。

native Windows 的差异只有三类是**契约允许**的：

1. **产品在 Windows 上显式排除的能力**。判据是被测入口自己抛 `capability_unsupported`
   （例如 `src/agent/codex_*` 的 `resolve_command()`），这类用例按平台 skip。
2. **平台缺失的 stdlib 接口或 POSIX 工具链**，例如 `time.tzset` 仅 POSIX、Windows 默认 PATH 没有
   `sh`（`docker/entrypoint.sh` 只能在 POSIX 下执行）。能力可用 `shutil.which` 探测时就用能力探测
   skip，而不是写死平台；只有当测试无法用跨平台等价物表达
   同一契约时才允许 skip；能表达就必须改成跨平台实现——存活探测不能再用 `os.kill(pid, 0)`，
   它在 Windows 上会真的终止进程。
3. **只能由 POSIX mode 位表达的断言**（`0o600` / `0o700`）。生产侧若已 `except OSError: pass`，
   说明它不是 fail-fast 契约，跳过 mode 断言即可；同一用例的其余断言必须继续在 Windows 上执行。

不在上述三类的失败一律按根因修复。**禁止**为了"全量变绿"整文件 skip 或放宽断言：新增的模块级
skip 必须属于上述三类并写明理由，否则视为契约漂移。已知反例（都必须修）：

- 子进程 stdio 按 UTF-8 读回（`_read_text_file`），因此必须让子进程也按 UTF-8 写
  （`build_local_cli_env()` 设 `PYTHONIOENCODING`），而不是放宽断言或给 `text=True` 补 `encoding`。
- Windows 超时终止要覆盖整棵进程树：`terminate()` 只杀直接子进程。
- 诊断预览的 CRLF/LF 差异在预览生成处统一为 LF，而不是逐条放宽断言。

`fxmacrodata-public-client` 只从 GitHub Release 分发 wheel（PyPI 无包），按 ADR 0005 降级为可选
导入：开发环境缺失时工具集为空并输出一次性原因信号，发布完整性由桌面端打包探针承担。

native Windows 的既有失败由 `docs/agents/windows-known-failures.md` 登记，用
`python scripts/check_windows_test_baseline.py` 校验：`NEW`（失败但未登记）、`STALE`（登记了但已
不再失败）、`UNRESOLVED`（理由仍是 `TODO`）与 collection error 会分别报出，并以退出码 1 失败。
native Windows 上 `ci_gate.sh offline-tests` 会调用该脚本，不再直接跑 pytest；夜间
`windows-baseline` workflow 用同一脚本防止白名单漂移。

JUnit 无法表达 `unittest.subTest` 失败，所以 checker 还会比较 pytest 汇总的 `N failed` 与
JUnit 失败数，得到 `UNREPRESENTED`；`--max-unrepresented`（默认 0）在出现子测试级失败时立即失败。
`--leak-probe` 可选开启 `DSA_ENV_LEAK_PROBE=1`：该模式关闭逐用例环境还原，把测试之间真实的
env 写入视为失败；singleton 过渡只输出为 informational。
