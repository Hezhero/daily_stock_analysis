# -*- coding: utf-8 -*-
"""Ticket 03 real-run wiring: log-derived estimator + adjudicated verdict.

Seam: the public boundary between an actual ``--regime-mode`` backtest run
(its log file + portfolio_sim summary JSON) and the pre-registered comparison.
Expected values come from real log lines captured in ``logs/`` (worked examples),
not from re-running the implementation's own regex.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.auto_regime_ab_run import (
    ZERO_OPEN_REGEX,
    assemble_run_evidence,
    derive_snapshot_id,
    extract_zero_open_days,
)


# ── red slice 1: extract 0/5 open days from a real log line ───────────────────

def test_extract_zero_open_days_from_real_log_line(tmp_path: Path) -> None:
    # Exact line copied from logs/backtest_5y_20261111.log
    log = tmp_path / "auto.log"
    log.write_text(
        "2026-11-11 06:29:11 [INFO] 验证区间 market_ok_enh2 天数: 0/5\n",
        encoding="utf-8",
    )
    assert extract_zero_open_days(log, "market_ok_enh2") == 5


def test_extract_zero_open_days_uses_selected_column(tmp_path: Path) -> None:
    log = tmp_path / "auto.log"
    log.write_text(
        "2026-11-13 09:00:00 [INFO] 验证区间 market_ok_auto 天数: 3/5\n"
        "2026-11-13 09:00:01 [INFO] 验证区间 market_ok_enh2 天数: 0/5\n",
        encoding="utf-8",
    )
    assert extract_zero_open_days(log, "market_ok_auto") == 2
    assert extract_zero_open_days(log, "market_ok_enh2") == 5


def test_extract_zero_open_days_rejects_missing_line(tmp_path: Path) -> None:
    log = tmp_path / "auto.log"
    log.write_text("no relevant line here\n", encoding="utf-8")
    with pytest.raises(ValueError, match="验证区间"):
        extract_zero_open_days(log, "market_ok_auto")


def test_extract_zero_open_days_rejects_malformed_ratio(tmp_path: Path) -> None:
    log = tmp_path / "auto.log"
    log.write_text("[INFO] 验证区间 market_ok_auto 天数: 2/4\n", encoding="utf-8")
    with pytest.raises(ValueError, match="5"):
        extract_zero_open_days(log, "market_ok_auto")


def test_zero_open_regex_is_exported() -> None:
    assert ZERO_OPEN_REGEX.search(
        "[INFO] 验证区间 market_ok_auto 天数: 3/5"
    )


# ── red slice 2: assemble four arms + adjudicated verdict ───────────────────

def _summary(mode: str, cagr: float, maxdd: float, win: float) -> dict:
    return {
        "metrics": {
            "cagr_oos": cagr,
            "maxDD_pct": maxdd,
            "win_rate": win,
            # Measured window fields: the snapshot gate cross-checks these
            # across arms so a stem date alone cannot assert "same snapshot".
            "n_days": 1419,
            "oos_start": "2022-01-17",
            "oos_days": 1166,
        },
        "config_hash": f"hash-{mode}",
        "data_snapshot_hash": "snap-2026-11-12",
    }


def _write_arm(tmp_path: Path, mode: str, col: str, open_days: int,
                cagr: float, maxdd: float, win: float,
                stem: str = "portfolio_sim_20210514_20261112_pit") -> dict:
    d = tmp_path / mode
    d.mkdir()
    # Real artifact name carries the data snapshot end date (2026-11-12).
    summary = d / f"{stem}.json"
    summary.write_text(json.dumps(_summary(mode, cagr, maxdd, win)), encoding="utf-8")
    log = d / "run.log"
    log.write_text(f"[INFO] 验证区间 {col} 天数: {open_days}/5\n", encoding="utf-8")
    return {"summary": str(summary), "log": str(log), "regime_col": col}


# ── red slice 3: snapshot identity from real artifact names ──────────────────

def test_derive_snapshot_id_from_artifact_stem() -> None:
    assert derive_snapshot_id("portfolio_sim_20210514_20261112_pit.json") == "20261112_pit"


def test_derive_snapshot_id_rejects_unknown_stem() -> None:
    with pytest.raises(ValueError, match="快照|snapshot"):
        derive_snapshot_id("random_file.json")


def test_assemble_run_evidence_builds_observation_record(tmp_path: Path) -> None:
    """ADR 0003: no automatic PASS gate. The wiring produces an observation
    record; overall_pass stays null until a human fills human_verdict.
    """
    arms = {
        "auto": _write_arm(tmp_path, "auto", "market_ok_auto", 3, 0.06, 12.0, 0.55),
        "enh2": _write_arm(tmp_path, "enh2", "market_ok_enh2", 0, 0.042, 13.9, 0.54),
        "enh": _write_arm(tmp_path, "enh", "market_ok_enh", 5, 0.07, 15.0, 0.53,
                          stem="portfolio_sim_20210428_20261112_pit"),
        "strict": _write_arm(tmp_path, "strict", "market_ok", 0, 0.03, 11.0, 0.56,
                             stem="portfolio_sim_20210528_20261112_pit"),
    }
    verdict = assemble_run_evidence(arms, snapshot_id="20261112_pit",
                                    out_path=str(tmp_path / "verdict.json"))

    assert verdict["status"] == "observations-recorded"
    assert verdict["adjudicated"] is False
    assert verdict["overall_pass"] is None          # no automatic gate
    assert verdict["wfo"]["required"] is False
    assert verdict["variants"]["tested_count"] == 4
    assert verdict["variants"]["preregistered_primary"] is True
    assert verdict["comparison"]["zero_open_days"]["auto"] == 2   # 5 - 3
    assert verdict["comparison"]["zero_open_days"]["enh2"] == 5
    assert verdict["comparison"]["zero_open_days"]["delta_vs_enh2"] == -3
    assert verdict["arms_snapshot"]["auto"] == "20261112_pit"
    assert "human_verdict" in verdict
    assert (tmp_path / "verdict.json").exists()


def test_assemble_run_evidence_rejects_mismatched_snapshot(tmp_path: Path) -> None:
    arms = {
        "auto": _write_arm(tmp_path, "auto", "market_ok_auto", 3, 0.06, 12.0, 0.55),
        "enh2": _write_arm(tmp_path, "enh2", "market_ok_enh2", 0, 0.042, 13.9, 0.54),
        "enh": _write_arm(tmp_path, "enh", "market_ok_enh", 5, 0.07, 15.0, 0.53,
                          stem="portfolio_sim_20210428_20261111_pit"),  # 11-11 != 11-12
        "strict": _write_arm(tmp_path, "strict", "market_ok", 0, 0.03, 11.0, 0.56,
                             stem="portfolio_sim_20210528_20261112_pit"),
    }
    with pytest.raises(ValueError, match="快照|snapshot"):
        assemble_run_evidence(arms, snapshot_id="20261112_pit",
                              out_path=str(tmp_path / "verdict.json"))


# ── red slice 4: snapshot identity must be MEASURED, not just asserted ──────

def test_assemble_run_evidence_rejects_measured_window_mismatch(tmp_path: Path) -> None:
    """A stem can lie; the summary's own n_days/oos_start must agree across arms.

    This is the anti-fabrication check: same filename date but genuinely
    different measurement windows must be rejected.
    """
    arms = {
        "auto": _write_arm(tmp_path, "auto", "market_ok_auto", 3, 0.06, 12.0, 0.55),
        "enh2": _write_arm(tmp_path, "enh2", "market_ok_enh2", 0, 0.042, 13.9, 0.54),
        "enh": _write_arm(tmp_path, "enh", "market_ok_enh", 5, 0.07, 15.0, 0.53,
                          stem="portfolio_sim_20210428_20261112_pit"),
        "strict": _write_arm(tmp_path, "strict", "market_ok", 0, 0.03, 11.0, 0.56,
                             stem="portfolio_sim_20210528_20261112_pit"),
    }
    # Tamper: enh arm reports a different measured window (n_days / oos_start).
    p = Path(arms["enh"]["summary"])
    d = json.loads(p.read_text(encoding="utf-8"))
    d["metrics"]["n_days"] = 1200
    d["metrics"]["oos_start"] = "2022-06-01"
    p.write_text(json.dumps(d), encoding="utf-8")
    with pytest.raises(ValueError, match="n_days|oos_start|窗口|快照"):
        assemble_run_evidence(arms, snapshot_id="20261112_pit",
                              out_path=str(tmp_path / "verdict.json"))
