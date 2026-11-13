# -*- coding: utf-8 -*-
"""Ticket 03 real-run wiring: turn four actual backtest runs into an observation record.

This is the *post-run* companion to ``auto_regime_ab_compare.py``. It reads
the artifacts an authorized run already produced (portfolio_sim summary JSON +
the driver's log file), derives the pre-registered ``zero_open_days_in_window``
estimator from the log, verifies all four arms share one data snapshot, and
writes an **observation record** to ``result/gates/auto-regime-ab/``.

ADR 0003 discipline: the pre-registration froze *descriptive* criteria and
"human fills overall_pass"; there is NO automatic numeric PASS gate here.
``overall_pass`` stays null and ``human_verdict`` is a blank slot for a human.
It does NOT run backtests, connect to PG, fabricate arms, change any default,
or roll back ADR 0006. Criteria and variant set come from
``auto_regime_ab_compare.py`` unchanged.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict

# Running this file directly puts ``scripts/`` on sys.path[0]; make the repo
# root importable so ``scripts.auto_regime_ab_compare`` resolves identically
# to how the test suite imports it (mirrors backtest_5y_23strategies.py).
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.auto_regime_ab_compare import (  # noqa: E402
    MODES,
    build_verdict,
    compare_four_modes,
    load_arms,
)

# Matches the driver's log line, e.g.:
#   "2026-11-11 06:29:11 [INFO] 验证区间 market_ok_enh2 天数: 0/5"
ZERO_OPEN_REGEX = re.compile(
    r"验证区间\s+(?P<col>\w+)\s+天数:\s*(?P<open>\d+)\s*/\s*(?P<total>\d+)"
)

# Column each --regime-mode run uses for its verification-window line.
_REGIME_COL = {
    "auto": "market_ok_auto",
    "enh2": "market_ok_enh2",
    "enh": "market_ok_enh",
    "strict": "market_ok",
}

# Real artifact stem: portfolio_sim_<start>_<end>_<universe>.json. The <end>
# date is the data-snapshot cutoff; all four arms must share it.
_ARTIFACT_STEM = re.compile(r"portfolio_sim_\d{8}_(\d{8})_([a-z0-9\-]+)\.json$")

# Measured window fields the driver writes into metrics; cross-checked across
# arms so a filename date alone cannot claim "same data snapshot".
_WINDOW_KEYS = ("n_days", "oos_start", "oos_days")


def derive_snapshot_id(summary_path: str | Path) -> str:
    """Derive ``<end_date>_<universe>`` from a real portfolio_sim artifact name.

    The driver's summary JSON does not carry a ``data_snapshot_hash`` field, so
    the snapshot identity is taken from the artifact stem (the data cutoff date
    printed by ``today_str``) plus the universe. This is evidence from the run
    itself, not a fabricated field.
    """
    name = Path(summary_path).name
    match = _ARTIFACT_STEM.search(name)
    if not match:
        raise ValueError(f"无法从产物名推导数据快照: {name}（预注册 §2 要求四臂同一快照）")
    return f"{match.group(1)}_{match.group(2)}"


def extract_zero_open_days(log_path: str | Path, regime_col: str) -> int:
    """Derive 0-open-day count from a real driver log line.

    The driver prints ``验证区间 <col> 天数: <open>/<total>``. The pre-registered
    estimator is the number of days with NO open: ``total - open``. A missing
    line, a mismatched column, or a non-5-day window is an error (never a
    silent 0), per the pre-registration card §7.
    """
    text = Path(log_path).read_text(encoding="utf-8", errors="replace")
    candidates = []
    for match in ZERO_OPEN_REGEX.finditer(text):
        if match.group("col") == regime_col:
            candidates.append(match)
    if not candidates:
        raise ValueError(f"日志缺「验证区间 {regime_col} 天数」行，无法提取 0/5 观测")
    # Use the last matching line (a re-run appends to the same log file).
    last = candidates[-1]
    open_days, total = int(last.group("open")), int(last.group("total"))
    if total != 5:
        raise ValueError(f"验证区间应为 5 个交易日，日志给出 {total}（不得静默换算）")
    if not 0 <= open_days <= total:
        raise ValueError(f"可开仓天数 {open_days} 越界（总天数 {total}）")
    return total - open_days


def assemble_run_evidence(arms: Dict[str, Dict[str, str]],
                          snapshot_id: str,
                          out_path: str | Path = "result/gates/auto-regime-ab/verdict.json") -> Dict[str, Any]:
    """Assemble four real run artifacts into an adjudicated verdict.

    ``arms`` maps each mode to ``{summary, log, regime_col}`` paths. The
    summary JSON supplies cagr_oos / maxDD_pct / win_rate / config_hash; the
    log supplies the 0/5 estimator. ``snapshot_id`` is the caller-declared
    data snapshot (e.g. ``20261112_pit``); each arm's artifact stem must derive
    the same value (pre-registration §2) or this raises.
    """
    missing = [m for m in MODES if m not in arms]
    if missing:
        raise ValueError(f"四口径实跑缺臂: {', '.join(missing)}（不得事后补跑或伪造）")
    unknown = [m for m in arms if m not in MODES]
    if unknown:
        raise ValueError(f"预注册外变体: {', '.join(unknown)}（禁止事后加变体）")

    # 1. Load the four portfolio_sim summaries via the existing pure loader.
    summaries = load_arms({m: arms[m]["summary"] for m in MODES})

    # 2. Snapshot alignment gate. Two independent checks so a filename date
    #    alone cannot assert "same snapshot":
    #    (a) declared snapshot_id == each arm's artifact-derived id;
    #    (b) MEASURED window (n_days / oos_start / oos_days) agrees across arms.
    snapshots = {m: derive_snapshot_id(arms[m]["summary"]) for m in MODES}
    bad = {m: s for m, s in snapshots.items() if s != snapshot_id}
    if bad:
        raise ValueError(
            f"四口径数据快照不一致: 声明={snapshot_id} 不符={bad}（预注册 §2 要求同一快照）"
        )
    measured = {
        m: tuple(summaries[m].get(k) for k in _WINDOW_KEYS) for m in MODES
    }
    distinct = set(measured.values())
    if len(distinct) != 1 or any(v is None for v in next(iter(distinct))):
        raise ValueError(
            f"四口径实测窗口不一致（n_days/oos_start/oos_days）: {measured}"
            "（文件名日期不足以证明同一快照）"
        )
    for m in MODES:
        summaries[m]["data_snapshot_hash"] = snapshot_id

    # 3. Derive the 0/5 estimator from each run's log, using its own column.
    for m in MODES:
        col = arms[m].get("regime_col") or _REGIME_COL[m]
        summaries[m]["zero_open_days_in_window"] = extract_zero_open_days(
            arms[m]["log"], col
        )

    # 4. Comparison + observation record. ADR 0003: NO automatic PASS gate.
    #    overall_pass stays null; a human fills human_verdict (and, if they
    #    choose, overall_pass) after reading the recorded observations.
    comparison = compare_four_modes(summaries)
    verdict = build_verdict(comparison)
    verdict["status"] = "observations-recorded"
    verdict["adjudicated"] = False
    verdict["overall_pass"] = None
    verdict["variants"] = {
        "tested_count": len(MODES),
        "preregistered_primary": True,
        "registry_id": None,
    }
    verdict["human_verdict"] = {
        "overall_pass": None,
        "reason": "TO BE FILLED BY HUMAN after reading comparison (ADR 0003)",
        "filled_by": None,
        "filled_at": None,
    }
    verdict["auto_default_rollback"] = (
        "no automatic rollback; any rollback requires a separate ticket + "
        "ADR 0006 deprecation"
    )
    verdict["arms_snapshot"] = snapshots
    verdict["next_step"] = "human reads comparison, fills human_verdict; backfill spec/ticket"

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(verdict, ensure_ascii=False, indent=2), encoding="utf-8")
    return verdict


def main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Assemble four real --regime-mode runs into an adjudicated verdict."
    )
    for m in MODES:
        parser.add_argument(f"--{m}-summary", required=True, metavar="JSON")
        parser.add_argument(f"--{m}-log", required=True, metavar="LOG")
    parser.add_argument("--snapshot-id", required=True, metavar="ID",
                        help="declared data snapshot, e.g. 20261112_pit (all four stems must match)")
    parser.add_argument("--out", default="result/gates/auto-regime-ab/verdict.json")
    args = parser.parse_args(argv)
    arms = {
        m: {
            "summary": getattr(args, f"{m}_summary"),
            "log": getattr(args, f"{m}_log"),
            "regime_col": _REGIME_COL[m],
        }
        for m in MODES
    }
    verdict = assemble_run_evidence(arms, snapshot_id=args.snapshot_id, out_path=args.out)
    print(f"observation record written: {args.out} (overall_pass=null, human_verdict pending)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
