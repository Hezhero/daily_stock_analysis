# CHANGELOG format

Authoritative format for `docs/CHANGELOG.md`. Read this before editing the file.

## `[Unreleased]` is flat

Each entry is one independent line:

```
- [类型] 描述
```

Allowed types: `新功能` / `改进` / `修复` / `文档` / `测试` / `chore`.

**Do not add `###` category headings inside `[Unreleased]`.** Headings multiply merge
conflicts when several PRs touch the section; a flat list lets git merge line-by-line.

Example:

```
## [Unreleased]
- [修复] `scripts/backtest_5y_23strategies.py` 修复 Windows 控制台编码导致的退出码异常。
- [测试] 新增 `tests/test_backtest_stdio_encoding.py`。
```

## Release-time consolidation

A maintainer consolidates the flat list into the categorized, versioned format when
cutting a release. Contributors do not pre-group entries.
