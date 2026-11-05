"""Guard against machine-specific paths and the fork account in shipped code/docs.

``AGENTS.md`` section 1 bans hardcoded accounts and absolute environment-specific
paths, but review is the only thing enforcing it today. Scan the shipped
directories (not ``tests/``, where fixtures may legitimately name a remote) for
the patterns that already slipped through once.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCAN_DIRS = ("scripts", "src", "docs/agents")
FORBIDDEN = (
    ("absolute workspace path", "F:\\workspace"),
    ("absolute user path", "C:\\Users\\"),
    ("hardcoded fork account", "Hezhero/daily_stock_analysis"),
)
BINARY_SUFFIXES = {
    ".bin",
    ".db",
    ".dll",
    ".exe",
    ".gif",
    ".gz",
    ".ico",
    ".jpeg",
    ".jpg",
    ".nbc",
    ".parquet",
    ".png",
    ".pyc",
    ".so",
    ".sqlite",
    ".woff",
    ".woff2",
    ".zip",
}


def test_no_machine_specific_paths_or_fork_account():
    offenders = []
    for dirname in SCAN_DIRS:
        for path in (REPO_ROOT / dirname).rglob("*"):
            if not path.is_file() or path.suffix.lower() in BINARY_SUFFIXES:
                continue
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            for label, needle in FORBIDDEN:
                if needle in text:
                    offenders.append(f"{path.relative_to(REPO_ROOT)}: {label}")

    assert not offenders, "\n".join(offenders)
