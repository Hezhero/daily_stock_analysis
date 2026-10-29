# Repository Instructions

Canonical source: [`AGENTS.md`](../AGENTS.md).

If any instruction in this file conflicts with `AGENTS.md`, follow `AGENTS.md`.

## Core Rules
- Decision authority: only stop for a human decision when the choice is a product semantic, a default value, a user-visible behaviour change, an irreversible/high-cost remote action, or a contract conflict the evidence cannot settle. Otherwise follow the documented recommendation, execute it, and record what was decided and why.

- Respect directory boundaries:
  - Backend: `src/`, `data_provider/`, `api/`, `bot/`
  - Web: `apps/dsa-web/`
  - Desktop: `apps/dsa-desktop/`
  - Deployment/workflows: `scripts/`, `.github/workflows/`, `docker/`
- Do not run `git commit`, `git tag`, or `git push` without explicit user confirmation.
- Before creating/updating PRs, PR review, or issue analysis, refresh the latest code baseline with `git fetch --all --prune`; if the worktree is clean and the current branch can fast-forward, run `git pull --ff-only`. If local changes, conflicts, missing upstream, or non-fast-forward history make that unsafe, do not stash/reset/overwrite local state; analyze against fetched remote refs or record the baseline gap before proceeding.
- PR titles should use `<type>: <change summary>` such as `fix: 修复大盘分析历史记录丢失`; use `fix`/`feat`/`refactor`/`docs`/`chore`/`test`/`ci` where possible, and avoid `[codex]`, `codex`, `autocode`, `copilot`, or other tool/agent source prefixes. Treat this as process guidance and do not use title format mismatches as a hard review blocker.
- Do not hardcode secrets, accounts, ports, model names, absolute environment-specific paths, or environment-specific branches.
- Reuse existing modules, configuration entrypoints, scripts, and tests instead of adding parallel implementations.
- For user-visible behavior changes, CLI/API changes, deployment changes, notification changes, or report-structure changes, update the relevant docs and `docs/CHANGELOG.md`.
- When editing `docs/CHANGELOG.md`, follow `docs/agents/changelog-format.md`.
- Use `README.md` only for project positioning, high-level capabilities, quick start, main entrypoints, and sponsorship/cooperation information; avoid updating README unless the change is homepage-level.
- Put detailed module behavior, page interaction, topic configuration, troubleshooting, field contracts, implementation semantics, and edge cases in the appropriate `docs/*.md` file instead of README.
- When config semantics change, sync `.env.example` and assess impact on local runs, Docker, GitHub Actions, API, Web, and Desktop.

## Validation

Per-surface commands, CI gates, and the Windows/POSIX platform boundary live in `docs/agents/validation-matrix.md`. On Windows, run `pwsh -File ./scripts/ci_gate.ps1` after activating the project conda env.

## AI Asset Governance

- `AGENTS.md` is the single source of truth for repository AI collaboration rules.
- `CLAUDE.md` must remain a symlink to `AGENTS.md`.
- Use `.github/instructions/*.instructions.md` for path-specific guidance.
- Current repository collaboration skills live in `.claude/skills/`; keep them aligned with `AGENTS.md`.
