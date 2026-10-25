# Validation matrix

Read this before delivering any change: pick the row that matches the touched surface,
run its commands, and state the gap when a check could not run.

## CI coverage

| 检查项 | 来源 | 说明 | 是否阻断 |
| --- | --- | --- | --- |
| `ai-governance` | `.github/workflows/ci.yml` | 校验 `AGENTS.md` / `CLAUDE.md` / `.github` 指令 / `.claude/skills` 关系，并执行 `scripts/check_spec_tickets.py` | 是 |
| `backend-gate` | `.github/workflows/ci.yml` | 执行 `./scripts/ci_gate.sh` | 是 |
| `docker-build` | `.github/workflows/ci.yml` | Docker 构建与关键模块导入 smoke | 是 |
| `web-gate` | `.github/workflows/ci.yml` | 前端改动时执行 `npm run lint` + `npm run build` | 是（触发时） |
| `network-smoke` | `.github/workflows/network-smoke.yml` | `pytest -m network` + `scripts/test.sh quick` | 否，观测项 |
| `pr-review` | `.github/workflows/pr-review.yml` | PR 静态检查 + AI 审查 + 自动标签 | 否，辅助项 |

若 PR 上已有对应 CI 结果，可直接引用 CI 结论；若 CI 未覆盖改动面，或本地与 CI 环境差异较大，
需要补充说明本地验证与缺口。

## Commands

```bash
pip install -r .github/requirements-ci.txt   # requirements.txt + flake8 + pytest + pytest-timeout
./scripts/ci_gate.sh                          # POSIX / Git Bash / WSL
pwsh -File ./scripts/ci_gate.ps1              # Windows PowerShell wrapper (locates Git Bash)
```

`ci_gate.sh` preflights its tools and exits `2` with the install command when `python`,
`python3`, `flake8`, `pytest`, or `pytest_timeout` is missing. The `.ps1` wrapper only
locates Git Bash; it does not install anything.

On Windows, activate the project environment first: `conda activate daily_stock_analysis`.
The wrapper and the Git Bash it spawns inherit the current `PATH`; without activation the
gate silently runs under the base environment's Python (cross-environment risk that the
repository rules explicitly forbid).

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
`fxmacrodata-public-client` 又只从 GitHub Release 分发 wheel（PyPI 无包），因此本机全量
结果偏红不代表改动引入回归；需要全量基线时在 WSL 或 Docker 里跑同一条命令。新增依赖时
优先选择 PyPI 可解析的包，避免让本地基线依赖单一分发渠道。
