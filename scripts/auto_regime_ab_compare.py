# -*- coding: utf-8 -*-
"""Ticket 03 scaffolding: pre-registered four-mode A/B comparison.

This module is a *pure* consumer of already-produced ``--portfolio-sim``
result summaries (per ``--regime-mode`` run). It does NOT:

  * connect to PostgreSQL or load market data,
  * invoke ``backtest_5y_23strategies.py`` / run a backtest,
  * fabricate or backfill a missing arm,
  * change any default or adjudicate a winner.

Its only outputs are a comparison table and a pre-registered verdict skeleton
(``wfo: {required: false, ...}``). The four backtest runs and the adjudication
remain a separate, explicitly authorized step (ADR 0003 / spec Further Notes).
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

MODES = ("auto", "enh2", "enh", "strict")
_BASELINE = "enh2"
_WFO_REASON = (
    "ADR 0006 default flip is user-exempted from the metric contract's "
    "pre-registration release gate; this A/B card is confirmatory evidence, "
    "not a gate that can roll back the default. Full WFO not required here."
)

_METRIC_KEYS = ("cagr_oos", "maxDD_pct", "win_rate")
_ZERO_CAUSE_SPLIT = {
    "market_gate": "verification-window 0/5 open days (market_ok_auto=False)",
    "signal_sparsity": "0 stocks after resonance/handoff despite open days",
}

_PREREGISTRATION = {
    "estimator": "same-day / same-data-snapshot four-arm comparison of "
                 "portfolio_sim metrics (cagr_oos, maxDD_pct, win_rate)",
    "comparison": "auto vs fixed enh2/enh/strict, all four arms on identical "
                  "data snapshot and aligned config_hash",
    "criteria": [
        "delta of cagr_oos / maxDD_pct / win_rate vs enh2 baseline",
        "change in verification-window 0-open days (0/5)",
        "zero-cause split: market gate vs signal/handoff sparsity",
        "overfitting risk under bull 70.3% / range 20.0% / bear 9.7%, "
        "58 regime segments; same-type regime-switch trail was previously falsified",
    ],
    "variant_count": len(MODES),
    "rollback": "result unfavourable to auto does NOT auto-rollback; "
                "requires a separate ticket + ADR 0006 deprecation",
    "lookahead_guard": "no post-hoc criterion change, no extra auto-favouring variant "
                       "added after seeing results (ADR 0003)",
}


def compare_four_modes(arms: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Build the comparison table from four per-mode result summaries.

    ``arms`` maps each of ``MODES`` to a metrics dict (the shape written by
    ``_write_portfolio_sim_artifacts``: ``cagr_oos`` / ``maxDD_pct`` /
    ``win_rate`` plus ``zero_open_days_in_window`` for the 0/5 observation).
    Missing/unknown arms are rejected rather than silently backfilled.
    """
    missing = [m for m in MODES if m not in arms]
    if missing:
        raise ValueError(f"A/B 对照缺口径: {', '.join(missing)}（不得事后补跑或伪造）")
    unknown = [m for m in arms if m not in MODES]
    if unknown:
        raise ValueError(f"A/B 对照含预注册外口径: {', '.join(unknown)}（禁止事后加变体）")
    # 核心估计量必须显式提供，缺一即报错，绝不静默补 0（预注册卡 §7）。
    for m in MODES:
        absent = [k for k in (*_METRIC_KEYS, "zero_open_days_in_window")
                  if k not in arms[m] or arms[m][k] is None]
        if absent:
            raise ValueError(f"{m} summary 缺估计量: {', '.join(absent)}（不得默认补 0）")

    zero_days = {m: int(arms[m]["zero_open_days_in_window"]) for m in MODES}
    deltas: Dict[str, Dict[str, float]] = {}
    for m in MODES:
        deltas[m] = {
            key: float(arms[m][key]) - float(arms[_BASELINE][key])
            for key in _METRIC_KEYS
        }

    return {
        "modes": list(MODES),
        "baseline": _BASELINE,
        "data_snapshot_hashes": {m: arms[m].get("data_snapshot_hash") for m in MODES},
        "config_hashes": {m: arms[m].get("config_hash") for m in MODES},
        "metrics": {m: {k: arms[m][k] for k in _METRIC_KEYS} for m in MODES},
        "zero_open_days": {
            **zero_days,
            "delta_vs_enh2": zero_days["auto"] - zero_days[_BASELINE],
        },
        "zero_cause_split": dict(_ZERO_CAUSE_SPLIT),
        "deltas_vs_enh2": deltas,
        "overfitting_risk": (
            "bull 70.3% / range 20.0% / bear 9.7%, only 58 regime contiguous "
            "segments; a same-type regime-switch trail was previously falsified. "
            "Treat any auto gain as provisional until out-of-sample confirmation."
        ),
    }


def build_verdict(comparison: Dict[str, Any]) -> Dict[str, Any]:
    """Wrap a comparison in a pre-registered verdict skeleton (not adjudicated)."""
    return {
        "status": "pre-registered",
        "card_id": "auto-regime-ab",
        "spec": "docs/specs/auto-regime-mode/spec.md",
        "adr": "docs/adr/0006-auto-regime-mode-default.md",
        "preregistration": dict(_PREREGISTRATION),
        "comparison": comparison,
        "wfo": {
            "required": False,
            "folds": None,
            "reason": _WFO_REASON,
        },
        # Explicit: no silent skip, and no automatic default rollback.
        "overall_pass": None,
        "adjudicated": False,
        "auto_default_rollback": "requires separate ticket + ADR 0006 deprecation",
        "next_step": "explicitly authorize four --portfolio-sim runs on one data "
                     "snapshot, load their JSON summaries, then adjudicate here",
    }


def load_arms(paths: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """Load four per-mode ``--portfolio-sim`` JSON summaries from disk.

    ``paths`` maps each mode to a JSON file path. This is the ONLY I/O in this
    module: it reads already-produced artifacts, never produces them.
    """
    arms: Dict[str, Dict[str, Any]] = {}
    for mode, path in paths.items():
        summary = json.loads(Path(path).read_text(encoding="utf-8"))
        metrics = dict(summary.get("metrics", {}))
        # Top-level summary hashes are authoritative; only use metrics-embedded
        # values as a fallback so a mismatched embedded hash never wins.
        for key in ("config_hash", "data_snapshot_hash", "zero_open_days_in_window"):
            value = summary.get(key)
            if value is None:
                value = metrics.get(key)
            if value is not None:
                metrics[key] = value
        arms[mode] = metrics
    return arms


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Pre-registered four-mode A/B verdict (does NOT run backtests)."
    )
    for mode in MODES:
        parser.add_argument(f"--{mode}", required=True, metavar="JSON",
                            help=f"path to the --regime-mode {mode} portfolio_sim summary JSON")
    parser.add_argument("--out", default="result/gates/auto-regime-ab/verdict.json",
                        help="where to write the pre-registered verdict skeleton")
    args = parser.parse_args(argv)
    paths = {mode: getattr(args, mode) for mode in MODES}
    arms = load_arms(paths)
    # Fail loudly if any arm has zero_open_days missing (do not silently assume 0).
    for mode in MODES:
        if "zero_open_days_in_window" not in arms[mode]:
            raise ValueError(
                f"{mode} summary 缺 zero_open_days_in_window；该值须由结果显式提供，"
                "不得默认补 0（0/5 观测是本卡的核心估计量）"
            )
    verdict = build_verdict(compare_four_modes(arms))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(verdict, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"pre-registered verdict written: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
