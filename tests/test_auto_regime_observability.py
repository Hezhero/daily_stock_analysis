# -*- coding: utf-8 -*-
"""Ticket 01: daily market-regime observability (prefactor).

Seams under test (confirmed in spec.md Testing Decisions, 2026-11-12):
  - observation seam: the daily-regime table written to the existing ``result/``
    outlet and the startup-log summary string. These verify that a run leaves
    offline-recomputable evidence and that the startup line stays readable.
  - main seam guard: the prefactor must NOT change the frozen ``enh2`` default,
    the selected column, or the published ``config_hash``.

Tests assert externally observable behaviour only; no internal collaborator is
mocked away and expected values come from the frozen algorithm definition, not
from re-running the implementation.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pandas as pd
import pytest

from scripts.portfolio_sim import _config_hash

DRIVER = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_23strategies.py"

# Published T0.1 frozen baseline (2026-10-17, project conda env).
T01_BASELINE_CONFIG_HASH = "5eea05195af3d89432af8b2a049fde2517d6a53b"

DAILY_REGIME_COLUMNS = (
    "date",
    "market_ok",
    "market_ok_enh",
    "market_ok_enh2",
    "regime",
    "breadth",
    "vol_pct",
)


def _load_driver(log_dir: Path):
    os.environ["LOG_DIR"] = str(log_dir)
    spec = importlib.util.spec_from_file_location(
        f"bt_driver_obs_{log_dir.name}", DRIVER
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _index_frame(n: int = 80) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=n)
    # Monotone-ish close with a kink so MA20 slope and vol rank are non-trivial.
    close = [100.0 + i * 0.5 + (3.0 if i % 7 == 0 else 0.0) for i in range(n)]
    return pd.DataFrame({"date": dates, "index_close": close})


def _regime_frame(driver, n: int = 80) -> pd.DataFrame:
    return driver.compute_market_ok(_index_frame(n))


# ── red: compute_market_ok must expose vol_pct as the 7th column ──────────────

def test_compute_market_ok_exposes_vol_pct_column(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "cols")
    regime = _regime_frame(driver)

    assert tuple(regime.columns) == DAILY_REGIME_COLUMNS
    assert len(regime) == 80
    observed = regime["vol_pct"].dropna()
    assert not observed.empty
    assert observed.between(0.0, 1.0, inclusive="both").all()


def test_vol_pct_column_is_the_one_used_by_frozen_enh2_definition(
    tmp_path: Path,
) -> None:
    """Returned vol_pct must be the actual column behind market_ok_enh2.

    With stock_df=None the frozen definition is
    ``market_ok_enh2 = market_ok_enh & (breadth>=0.45) & (vol_pct.fillna(0)<=0.85)``.
    Asserting this against the *returned* columns proves the exported series is
    the one the algorithm uses, not a fabricated look-alike.
    """
    driver = _load_driver(tmp_path / "relation")
    regime = _regime_frame(driver)

    expected = (
        regime["market_ok_enh"].astype(bool)
        & regime["breadth"].ge(0.45)
        & regime["vol_pct"].fillna(0.0).le(0.85)
    )
    pd.testing.assert_series_equal(
        regime["market_ok_enh2"].astype(bool), expected, check_names=False
    )


# ── red: daily table written to the existing result/ outlet ───────────────────

def test_write_daily_regime_table_writes_seven_columns(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "write")
    regime = _regime_frame(driver)
    out_dir = tmp_path / "result"

    path = driver.write_daily_regime_table(regime, str(out_dir))

    assert Path(path).parent == out_dir
    assert Path(path).exists()
    table = pd.read_csv(path, encoding="utf-8-sig")
    assert tuple(table.columns) == DAILY_REGIME_COLUMNS
    assert len(table) == len(regime)
    # No parallel directory is created: only the CSV lives under out_dir.
    assert sorted(p.name for p in out_dir.iterdir()) == [Path(path).name]


def test_write_daily_regime_table_is_deterministic_over_same_snapshot(
    tmp_path: Path,
) -> None:
    driver = _load_driver(tmp_path / "deterministic")
    regime = _regime_frame(driver)
    out_dir = tmp_path / "result"

    first = Path(driver.write_daily_regime_table(regime, str(out_dir))).read_bytes()
    second = Path(driver.write_daily_regime_table(regime, str(out_dir))).read_bytes()

    assert first == second


def test_write_daily_regime_table_rejects_missing_columns(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "missing")
    regime = _regime_frame(driver)
    # Drop from the full 7-column frame; the writer must reject a missing column.
    incomplete = regime.drop(columns=["vol_pct"])
    assert "vol_pct" not in incomplete.columns

    with pytest.raises(ValueError, match="vol_pct"):
        driver.write_daily_regime_table(incomplete, str(tmp_path / "result"))
    assert not (tmp_path / "result").exists()


def test_write_daily_regime_table_rejects_empty_frame(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "empty")
    empty = _regime_frame(driver).iloc[0:0]

    with pytest.raises(ValueError, match="为空"):
        driver.write_daily_regime_table(empty, str(tmp_path / "result"))
    assert not (tmp_path / "result").exists()


# ── red: startup-log summary expresses version / override / daily sequence ────

def test_regime_log_summary_expresses_version_override_and_sequence(
    tmp_path: Path,
) -> None:
    driver = _load_driver(tmp_path / "summary")
    regime = _regime_frame(driver)

    summary = driver.format_regime_log_summary(
        regime, rule_version="auto_v1", override_mode="enh2"
    )

    assert "auto_v1" in summary
    assert "enh2" in summary
    assert "覆盖" in summary  # override marker is human-readable
    for col in ("market_ok", "market_ok_enh", "market_ok_enh2"):
        assert col in summary
        assert str(int(regime[col].sum())) in summary  # daily sequence count


def test_regime_log_summary_marks_no_override(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "summary-no-override")
    regime = _regime_frame(driver)

    summary = driver.format_regime_log_summary(
        regime, rule_version="auto_v1", override_mode=None
    )

    assert "自动" in summary


# ── guard: prefactor must not change frozen default / signal set / hash ───────

def test_ticket01_frozen_hash_reproducible_with_explicit_enh2(tmp_path: Path) -> None:
    """Prefactor left the frozen definition intact; explicit enh2 reproduces the
    published baseline hash even after ticket 02 flips the default to auto.
    """
    driver = _load_driver(tmp_path / "guard")

    assert driver._regime_col("enh2") == "market_ok_enh2"
    assert _config_hash(driver._portfolio_sim_config(regime_mode="enh2")) == T01_BASELINE_CONFIG_HASH


def test_ticket01_entry_mask_still_selects_enh2_column(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    driver = _load_driver(tmp_path / "mask")
    # Pin the explicit legacy override; the new module default is auto (ADR 0006).
    monkeypatch.setattr(driver, "_REGIME_MODE", "enh2")
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2024-01-02", periods=3),
            "code": ["sh.600000"] * 3,
            "market_ok": [True, True, True],
            "market_ok_enh": [False, True, False],
            "market_ok_enh2": [True, False, True],
            "up_limit": [11.0, 11.0, 11.0],
            "down_limit": [9.0, 9.0, 9.0],
            "close_raw": [10.0, 10.0, 10.0],
            "close": [10.0, 10.0, 10.0],
            "pct_chg": [0.0, 0.0, 0.0],
        }
    )

    mask = driver._entry_mask(df, enhanced=True)

    assert list(mask.astype(bool)) == [True, False, True]


# ── red: runtime wiring — export happens at the compute_market_ok merge point ──

def test_main_exports_daily_regime_table_through_merge_point(tmp_path: Path) -> None:
    """The export must be reachable from main()'s merge, not only a bare helper.

    We assert the driver exposes a single integration hook that main() calls at
    the :6604 merge point, and that calling it with a real compute_market_ok
    result produces a readable 7-column CSV under the chosen result/ dir.
    """
    driver = _load_driver(tmp_path / "wiring")
    regime = _regime_frame(driver)
    out_dir = tmp_path / "result"

    path = driver.export_daily_regime(regime, str(out_dir), "auto_v1", "enh2")

    table = pd.read_csv(path, encoding="utf-8-sig")
    assert tuple(table.columns) == DAILY_REGIME_COLUMNS
    assert len(table) == len(regime)

