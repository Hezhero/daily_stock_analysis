# -*- coding: utf-8 -*-
"""Ticket 03: A/B comparison scaffolding (no backtest is executed).

The scaffold consumes four already-produced per-mode result summaries (the
``--portfolio-sim`` JSON exports) and emits a pre-registered verdict skeleton.
It never connects to PG, never runs the driver, and never backfills missing
arms with a fabricated run. Expected values come from worked literals.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.auto_regime_ab_compare import (
    MODES,
    build_verdict,
    compare_four_modes,
    load_arms,
    main,
)


def _metrics(cagr: float, maxdd: float, win: float, zero_days: int, trades: int) -> dict:
    return {
        "cagr_oos": cagr,
        "maxDD_pct": maxdd,
        "win_rate": win / 100.0,
        "n_trades": trades,
        "zero_open_days_in_window": zero_days,
        "config_hash": "hash_" + str(trades),
        "data_snapshot_hash": "snap",
    }


def _arms() -> dict:
    return {
        "auto": _metrics(0.06, 12.0, 55.0, 1, 560),
        "enh2": _metrics(0.042, 13.9, 54.0, 5, 560),
        "enh": _metrics(0.07, 15.0, 53.0, 0, 600),
        "strict": _metrics(0.03, 11.0, 56.0, 7, 400),
    }


def test_modes_are_the_four_pre_registered_arms() -> None:
    assert tuple(MODES) == ("auto", "enh2", "enh", "strict")


def test_compare_requires_all_four_arms() -> None:
    arms = _arms()
    del arms["strict"]
    with pytest.raises(ValueError, match="strict"):
        compare_four_modes(arms)


def test_compare_rejects_unknown_arm() -> None:
    arms = _arms()
    arms["enh3"] = arms["enh2"]
    with pytest.raises(ValueError, match="enh3"):
        compare_four_modes(arms)


def test_compare_reports_zero_open_day_change_and_cause_split() -> None:
    arms = _arms()
    report = compare_four_modes(arms)

    # auto reduced 0-open days from 5 (enh2 baseline) to 1 -> delta -4.
    assert report["zero_open_days"]["enh2"] == 5
    assert report["zero_open_days"]["auto"] == 1
    assert report["zero_open_days"]["delta_vs_enh2"] == -4
    # The card must distinguish market-gate zeros from signal/handoff sparsity.
    assert report["zero_cause_split"] == {
        "market_gate": "verification-window 0/5 open days (market_ok_auto=False)",
        "signal_sparsity": "0 stocks after resonance/handoff despite open days",
    }


def test_compare_reports_cagr_maxdd_winrate_deltas() -> None:
    report = compare_four_modes(_arms())
    d = report["deltas_vs_enh2"]["auto"]
    assert abs(d["cagr_oos"] - (0.06 - 0.042)) < 1e-9
    assert abs(d["maxDD_pct"] - (12.0 - 13.9)) < 1e-9
    assert abs(d["win_rate"] - (0.55 - 0.54)) < 1e-9


def test_compare_reports_overfitting_risk_note() -> None:
    report = compare_four_modes(_arms())
    note = report["overfitting_risk"]
    assert "58" in note
    assert "regime" in note.lower()


def test_verdict_records_wfo_not_required_explicitly() -> None:
    verdict = build_verdict(compare_four_modes(_arms()))
    assert verdict["wfo"]["required"] is False
    assert verdict["wfo"]["folds"] is None
    assert "reason" in verdict["wfo"]
    assert verdict["overall_pass"] is None  # pre-registered, not yet adjudicated


def test_verdict_is_a_pre_registration_not_a_result() -> None:
    verdict = build_verdict(compare_four_modes(_arms()))
    assert verdict["status"] == "pre-registered"
    assert "preregistration" in verdict
    assert "estimator" in verdict["preregistration"]
    # No automatic rollback of the auto default, even if a metric looks worse.
    assert verdict["auto_default_rollback"] == "requires separate ticket + ADR 0006 deprecation"


def test_verdict_json_roundtrip(tmp_path: Path) -> None:
    verdict = build_verdict(compare_four_modes(_arms()))
    path = tmp_path / "verdict.json"
    path.write_text(json.dumps(verdict, ensure_ascii=False, indent=2), encoding="utf-8")
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded == verdict


def _write_arm_summaries(tmp_path: Path) -> dict:
    paths: dict = {}
    for mode, metrics in _arms().items():
        p = tmp_path / f"{mode}.json"
        p.write_text(json.dumps({"metrics": metrics, "config_hash": metrics["config_hash"],
                                "data_snapshot_hash": metrics["data_snapshot_hash"]}),
                     encoding="utf-8")
        paths[mode] = str(p)
    return paths


def test_load_arms_reads_four_summaries(tmp_path: Path) -> None:
    arms = load_arms(_write_arm_summaries(tmp_path))
    assert set(arms) == set(MODES)
    assert arms["auto"]["cagr_oos"] == 0.06
    assert arms["enh2"]["zero_open_days_in_window"] == 5


def test_cli_writes_pre_registered_verdict(tmp_path: Path) -> None:
    paths = _write_arm_summaries(tmp_path)
    out = tmp_path / "gates" / "verdict.json"
    rc = main([
        "--auto", paths["auto"], "--enh2", paths["enh2"],
        "--enh", paths["enh"], "--strict", paths["strict"], "--out", str(out),
    ])
    assert rc == 0
    verdict = json.loads(out.read_text(encoding="utf-8"))
    assert verdict["status"] == "pre-registered"
    assert verdict["wfo"]["required"] is False
    assert verdict["comparison"]["zero_open_days"]["delta_vs_enh2"] == -4


def test_cli_rejects_missing_zero_open_days_field(tmp_path: Path) -> None:
    paths = _write_arm_summaries(tmp_path)
    # Drop the core estimator from one arm; CLI must fail rather than assume 0.
    p = Path(paths["enh"])
    d = json.loads(p.read_text(encoding="utf-8"))
    del d["metrics"]["zero_open_days_in_window"]
    p.write_text(json.dumps(d), encoding="utf-8")
    with pytest.raises(ValueError, match="zero_open_days_in_window"):
        main([
            "--auto", paths["auto"], "--enh2", paths["enh2"],
            "--enh", paths["enh"], "--strict", paths["strict"],
            "--out", str(tmp_path / "gates" / "verdict.json"),
        ])
