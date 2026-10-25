# -*- coding: utf-8 -*-
"""Validate experiment-ticket metadata under ``docs/specs/*/issues/``.

Every ticket must declare which flip gate may adjudicate it and whether it changes
the signal set. The rules encode the applicability boundary in
``docs/backtest-flip-gate-spec.md``: the portfolio estimator (v2) only covers
"same signal set" cards, and a signal-changing v1 comparison needs a written
rationale (an ADR or a spec section).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Iterable, Optional

GATE_VALUES = {"none", "v1-signal", "v2-portfolio", "wfo"}
SIGNAL_VALUES = {"none", "changed", "unchanged"}
TICKET_GLOB = "docs/specs/*/issues/[0-9][0-9]-*.md"

_GATE = re.compile(r"^\*\*Gate:\*\*\s*`?([a-z0-9-]+)`?\s*$", re.MULTILINE)
_SIGNAL = re.compile(r"^\*\*Signal set:\*\*\s*`?([a-z]+)`?\s*$", re.MULTILINE)
_RATIONALE = re.compile(r"^\*\*Gate rationale:\*\*\s*(\S.*)$", re.MULTILINE)


def _field(pattern: re.Pattern[str], text: str) -> Optional[str]:
    match = pattern.search(text)
    return match.group(1).strip() if match else None


def check_ticket(path: Path) -> list[str]:
    """Return the contract violations for one ticket file."""
    text = path.read_text(encoding="utf-8")
    errors: list[str] = []
    gate = _field(_GATE, text)
    signal_set = _field(_SIGNAL, text)

    if gate is None:
        errors.append("missing **Gate:**")
    elif gate not in GATE_VALUES:
        errors.append(f"invalid Gate {gate!r}; expected one of {sorted(GATE_VALUES)}")

    if signal_set is None:
        errors.append("missing **Signal set:**")
    elif signal_set not in SIGNAL_VALUES:
        errors.append(
            f"invalid Signal set {signal_set!r}; expected one of {sorted(SIGNAL_VALUES)}"
        )

    if gate == "v2-portfolio" and signal_set == "changed":
        errors.append(
            "v2-portfolio is limited to unchanged signal sets (flip-gate spec §9)"
        )
    if gate == "none" and signal_set in {"changed", "unchanged"}:
        errors.append("Gate: none requires Signal set: none (non-experiment ticket)")
    if gate == "v1-signal" and signal_set == "changed" and _field(_RATIONALE, text) is None:
        errors.append("v1-signal on a changed signal set requires **Gate rationale:**")
    return errors


def check_tickets(root: Path) -> dict[Path, list[str]]:
    """Map each violating ticket to its errors."""
    failures: dict[Path, list[str]] = {}
    for path in sorted(root.glob(TICKET_GLOB)):
        errors = check_ticket(path)
        if errors:
            failures[path] = errors
    return failures


def _ticket_paths(root: Path) -> Iterable[Path]:
    return sorted(root.glob(TICKET_GLOB))


def main(argv: Optional[list[str]] = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    root = Path(args[0]).resolve() if args else Path(__file__).resolve().parents[1]
    failures = check_tickets(root)
    if failures:
        for path, errors in failures.items():
            print(f"FAIL {path.relative_to(root)}")
            for error in errors:
                print(f"  - {error}")
        return 1
    print(f"OK: {len(list(_ticket_paths(root)))} ticket(s) declare gate + signal set")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
