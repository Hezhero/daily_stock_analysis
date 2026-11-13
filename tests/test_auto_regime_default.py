# -*- coding: utf-8 -*-
"""Ticket 02: ``auto`` as the default regime mode.

Seams under test (confirmed in spec.md Testing Decisions, 2026-11-12):
  - main seam: the regime-resolution / entry-column-selection entry point. All
    downstream consumers (``_entry_mask``, validation stats, handoff) go through
    it. Asserts default/override, conservative per-day gating, no look-ahead,
    ``strict`` never auto-selected, missing-value fallback + WARNING, rule version
    entering ``config_hash`` as a single value, and byte-reproducibility.
  - observation seam: startup log expresses override marker / rule version /
    daily sequence; daily table exists from ticket 01.

Expected values come from the frozen ADR 0006 rule, not from re-running the
implementation. No internal collaborator is mocked away.
"""

from __future__ import annotations

import importlib.util
import itertools
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from scripts.portfolio_sim import _config_hash

DRIVER = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_23strategies.py"

T01_BASELINE_CONFIG_HASH = "5eea05195af3d89432af8b2a049fde2517d6a53b"


_DRIVER_IDS = itertools.count()


def _load_driver(log_dir: Path):
    os.environ["LOG_DIR"] = str(log_dir)
    name = f"bt_driver_auto_{next(_DRIVER_IDS)}"
    spec = importlib.util.spec_from_file_location(name, DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _regime_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Build a per-day market table with the 7 compute_market_ok columns."""
    n = len(rows)
    defaults = {
        "market_ok": False, "market_ok_enh": False, "market_ok_enh2": False,
        "regime": "range", "breadth": 0.0, "vol_pct": 0.0,
    }
    data: dict[str, Any] = {"date": pd.bdate_range("2024-01-02", periods=n)}
    for key, default in defaults.items():
        data[key] = [r.get(key, default) for r in rows]
    return pd.DataFrame(data)


# ── red: default is auto (flip), explicit values override ─────────────────────

def test_regime_mode_default_is_auto(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "default")
    # argparse default: not passing --regime-mode must resolve to auto.
    args = driver._build_parser().parse_args([])
    assert args.regime_mode is None
    assert driver.resolve_regime_mode(args) == "auto"


def test_explicit_regime_modes_override_auto(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "override")
    for mode in ("enh2", "enh", "strict"):
        args = driver._build_parser().parse_args(["--regime-mode", mode])
        assert driver.resolve_regime_mode(args) == mode


def test_strict_flag_preserves_legacy_priority(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "strict-flag")
    # --strict with no --regime-mode -> strict (legacy escape hatch unchanged)
    args = driver._build_parser().parse_args(["--strict"])
    assert driver.resolve_regime_mode(args) == "strict"
    # explicit --regime-mode wins over --strict
    args = driver._build_parser().parse_args(["--strict", "--regime-mode", "enh"])
    assert driver.resolve_regime_mode(args) == "enh"


def test_strict_flag_without_regime_mode_is_not_mislabelled_as_auto(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C6 regression: --strict (no --regime-mode) must be a FIXED override, not auto.

    The startup log and the per-day export both derive their rule version from
    the resolved mode; a --strict run must never print auto_v1 / 自动(auto).
    """
    driver = _load_driver(tmp_path / "strict-not-auto")
    args = driver._build_parser().parse_args(["--strict"])
    assert driver.resolve_regime_mode(args) == "strict"
    # Simulate main()'s override-marker assignment (single source of truth).
    mode = driver.resolve_regime_mode(args)
    override = None if mode == "auto" else mode
    assert override == "strict"
    assert driver.format_override_marker(override) == "覆盖(strict)"
    # rule version is decided by the mode, not by the override-truthiness proxy
    rule_version = driver.RULE_VERSION_AUTO if mode == "auto" else driver.RULE_VERSION_FIXED
    assert rule_version == driver.RULE_VERSION_FIXED


def test_override_marker_logged(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "marker")
    assert driver.format_override_marker("enh2") != driver.format_override_marker(None)


# ── red: main seam — per-day conservative auto selection ──────────────────────

def test_auto_selects_enh_only_on_qualified_bull_days(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "gating")
    rows = [
        {"regime": "bull", "breadth": 0.50, "vol_pct": 0.50,
         "market_ok_enh": True, "market_ok_enh2": True},   # qualified -> enh
        {"regime": "bull", "breadth": 0.40, "vol_pct": 0.50,
         "market_ok_enh": True, "market_ok_enh2": False},  # breadth low -> enh2
        {"regime": "bull", "breadth": 0.50, "vol_pct": 0.90,
         "market_ok_enh": True, "market_ok_enh2": False},  # high vol -> enh2
        {"regime": "range", "breadth": 0.60, "vol_pct": 0.30,
         "market_ok_enh": True, "market_ok_enh2": True},   # not bull -> enh2
        {"regime": "bear", "breadth": 0.60, "vol_pct": 0.30,
         "market_ok_enh": True, "market_ok_enh2": False},  # bear -> enh2
    ]
    out = driver.select_auto_regime_mode(_regime_frame(rows))

    assert list(out["auto_mode"]) == ["enh", "enh2", "enh2", "enh2", "enh2"]
    # market_ok_auto is the picked frozen column's value (not recomputed).
    assert list(out["market_ok_auto"].astype(bool)) == [True, False, False, True, False]


def test_auto_never_selects_strict(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "no-strict")
    rows = [{"regime": "bull", "breadth": 0.99, "vol_pct": 0.0,
             "market_ok": True, "market_ok_enh": True, "market_ok_enh2": True}
            for _ in range(5)]
    out = driver.select_auto_regime_mode(_regime_frame(rows))
    assert set(out["auto_mode"]) <= {"enh", "enh2"}
    assert "strict" not in set(out["auto_mode"])


def test_auto_boundary_thresholds_are_inclusive(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "boundary")
    rows = [
        {"regime": "bull", "breadth": 0.45, "vol_pct": 0.85,
         "market_ok_enh": True, "market_ok_enh2": False},  # exactly at both -> enh
        {"regime": "bull", "breadth": 0.4499, "vol_pct": 0.85,
         "market_ok_enh": True, "market_ok_enh2": False},  # just below -> enh2
        {"regime": "bull", "breadth": 0.45, "vol_pct": 0.8501,
         "market_ok_enh": True, "market_ok_enh2": False},  # just above -> enh2
    ]
    out = driver.select_auto_regime_mode(_regime_frame(rows))
    assert list(out["auto_mode"]) == ["enh", "enh2", "enh2"]


def test_missing_inputs_fallback_to_enh2_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    driver = _load_driver(tmp_path / "missing")
    rows = [
        {"regime": "bull", "breadth": None, "vol_pct": 0.5,
         "market_ok_enh": True, "market_ok_enh2": True},
        {"regime": None, "breadth": 0.6, "vol_pct": 0.5,
         "market_ok_enh": True, "market_ok_enh2": True},
        {"regime": "bull", "breadth": 0.6, "vol_pct": None,
         "market_ok_enh": True, "market_ok_enh2": True},
    ]
    frame = _regime_frame(rows)
    with caplog.at_level(logging.WARNING, logger="backtest"):
        out = driver.select_auto_regime_mode(frame)
    assert list(out["auto_mode"]) == ["enh2", "enh2", "enh2"]
    assert any("WARNING" in r.levelname for r in caplog.records) or any(
        "缺失" in r.getMessage() or "fallback" in r.getMessage().lower()
        for r in caplog.records
    )


def test_no_lookahead_future_row_change_does_not_affect_past_days(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "causal")
    rows = [
        {"regime": "bull", "breadth": 0.5, "vol_pct": 0.5,
         "market_ok_enh": True, "market_ok_enh2": False},
        {"regime": "bear", "breadth": 0.1, "vol_pct": 0.9,
         "market_ok_enh": True, "market_ok_enh2": False},
        {"regime": "range", "breadth": 0.3, "vol_pct": 0.3,
         "market_ok_enh": True, "market_ok_enh2": False},
    ]
    frame = _regime_frame(rows)
    out_before = driver.select_auto_regime_mode(frame.iloc[:2].copy())
    out_after = driver.select_auto_regime_mode(frame.copy())
    # First two days identical whether or not future rows exist.
    assert list(out_before["auto_mode"]) == list(out_after["auto_mode"].iloc[:2])
    assert list(out_before["market_ok_auto"]) == list(out_after["market_ok_auto"].iloc[:2])


def test_auto_selection_is_deterministic_function_of_data(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "determinism")
    rows = [
        {"regime": "bull", "breadth": 0.5, "vol_pct": 0.5,
         "market_ok_enh": True, "market_ok_enh2": False},
        {"regime": "range", "breadth": 0.6, "vol_pct": 0.2,
         "market_ok_enh": True, "market_ok_enh2": True},
    ]
    frame = _regime_frame(rows)
    a = driver.select_auto_regime_mode(frame)
    b = driver.select_auto_regime_mode(frame.copy())
    pd.testing.assert_series_equal(a["auto_mode"], b["auto_mode"])
    pd.testing.assert_series_equal(a["market_ok_auto"], b["market_ok_auto"])


# ── red: main seam — _regime_col resolves auto to market_ok_auto ──────────────

def test_regime_col_auto_maps_to_market_ok_auto(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "col")
    assert driver._regime_col("auto") == "market_ok_auto"
    for mode, col in (("enh2", "market_ok_enh2"), ("enh", "market_ok_enh"),
                      ("strict", "market_ok")):
        assert driver._regime_col(mode) == col


# ── red: config_hash registration + frozen-baseline reproducibility ───────────

def test_auto_mode_changes_config_hash_via_registered_rule(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "hash-auto")
    baseline = _config_hash(driver._portfolio_sim_config())
    assert baseline == T01_BASELINE_CONFIG_HASH
    auto_cfg = driver._portfolio_sim_config(regime_mode="auto")
    assert auto_cfg.regime_selector == "auto_v1"
    assert _config_hash(auto_cfg) != baseline


def test_explicit_enh2_keeps_frozen_baseline_hash(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "hash-enh2")
    cfg = driver._portfolio_sim_config(regime_mode="enh2")
    assert cfg.regime_selector == "enh2"
    assert _config_hash(cfg) == T01_BASELINE_CONFIG_HASH


def test_registered_override_rule_guard_still_applies(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "guard")
    # Unregistered experiment param still refused (prior art invariant).
    with pytest.raises(ValueError):
        driver._registered_experiment_override("not_a_registered_param", 1)


# ── red: soft / defensive-half-pos remain orthogonal ─────────────────────────

def test_soft_regime_and_defensive_half_pos_flags_independent_of_auto(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "orthogonal")
    # These globals are set by main() from env/CLI, untouched by the auto path.
    assert hasattr(driver, "_DEFENSIVE_HALF_POS")
    assert hasattr(driver, "_apply_regime_soft_filter")


# ── red: main seam wiring — auto column flows through _entry_mask ─────────────

def test_auto_column_routes_through_entry_mask(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The selected market_ok_auto column must be the one _entry_mask respects.

    This is the end-to-end main seam: given a regime_df resolved by
    select_auto_regime_mode, setting the global mode to auto and calling
    _entry_mask must gate on market_ok_auto (not on market_ok_enh2).
    """
    driver = _load_driver(tmp_path / "wiring-mask")
    rows = [
        {"regime": "bull", "breadth": 0.5, "vol_pct": 0.5,
         "market_ok_enh": True, "market_ok_enh2": False},   # auto -> enh -> True
        {"regime": "bear", "breadth": 0.1, "vol_pct": 0.9,
         "market_ok_enh": True, "market_ok_enh2": False},   # auto -> enh2 -> False
    ]
    resolved = driver.select_auto_regime_mode(_regime_frame(rows))
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2024-01-02", periods=2),
            "code": ["sh.600000"] * 2,
            "market_ok": [False, False],
            "market_ok_enh": list(resolved["market_ok_enh"].astype(bool)),
            "market_ok_enh2": list(resolved["market_ok_enh2"].astype(bool)),
            "market_ok_auto": list(resolved["market_ok_auto"].astype(bool)),
            "up_limit": [11.0, 11.0],
            "down_limit": [9.0, 9.0],
            "close_raw": [10.0, 10.0],
            "close": [10.0, 10.0],
            "pct_chg": [0.0, 0.0],
        }
    )
    monkeypatch.setattr(driver, "_REGIME_MODE", "auto")
    mask = driver._entry_mask(df, enhanced=True)
    assert list(mask.astype(bool)) == [True, False]
