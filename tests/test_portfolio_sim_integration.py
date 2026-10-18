# -*- coding: utf-8 -*-
"""S4 end-to-end wiring test: driver exit path -> candidate funnel -> simulator."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

DRIVER = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_23strategies.py"


def _load_driver(log_dir: Path):
    os.environ["LOG_DIR"] = str(log_dir)
    spec = importlib.util.spec_from_file_location("bt_driver_s4_test", DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _bars(codes: list[str], dates: list[pd.Timestamp]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for code_idx, code in enumerate(codes):
        for day_idx, day in enumerate(dates):
            price = 10.0 + code_idx * 2.0 + day_idx * 0.02
            rows.append(
                {
                    "code": code,
                    "date": day,
                    "open": price,
                    "high": price * 1.02,
                    "low": price * 0.98,
                    "close": price,
                    "close_raw": price,
                    "up_limit": price * 1.1,
                    "down_limit": price * 0.9,
                    "pct_chg": 0.0,
                    "amount": 1_000_000_000.0,
                    "volume": 1_000_000.0,
                    "atr20": 0.5,
                }
            )
    return pd.DataFrame(rows)


def test_s4_chain_reconciles_funnel_and_cash(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path)
    codes = ["sh.600000", "sz.000001", "sh.601318"]
    dates = list(pd.bdate_range("2024-01-02", periods=80))
    bars = _bars(codes, dates)

    exits = driver.compute_dynamic_exit_returns(
        bars,
        dual_timing=True,
        min_hold_bars=1,
        exit_path_period=max(driver.HOLDING_PERIODS),
        exit_path_timings=("next_open",),
    )
    bars = pd.concat([bars, exits], axis=1)

    signal_start, signal_end = dates[5], dates[75]
    book = {}
    for name in ("fc_pos_break", "volume_breakout"):
        series = pd.Series(False, index=bars.index)
        series.loc[bars["date"].between(signal_start, signal_end)] = True
        book[name] = series

    payload = driver._run_portfolio_sim_chain(
        bars,
        book,
        bars,
        None,
        f"{dates[60].date()}:{dates[75].date()}",
        str(tmp_path),
    )

    funnel = payload["funnel"]
    assert funnel["raw"] == funnel["filled"] + funnel["rejects_total"]
    assert funnel["reconciled"] is True
    assert funnel["filled"] == len(payload["result"].trades)
    assert funnel["filled"] > 0
    assert payload["equity_cash_max_abs_diff"] <= 1e-9
    assert any("debug window" in warning for warning in payload["warnings"])

    checks = payload["spot_checks"]
    assert checks
    assert all(check["entry_ok"] and check["exit_ok"] and check["exit_date_ok"] for check in checks)

    artifacts = payload["artifacts"]
    assert Path(artifacts["candidates_csv"]).exists()
    assert Path(artifacts["trades_csv"]).exists()
    assert Path(artifacts["equity_csv"]).exists()
    summary = json.loads(Path(artifacts["summary_json"]).read_text(encoding="utf-8"))
    assert summary["config_hash"]
    assert summary["funnel"]["reconciled"] is True


def test_s4_config_matches_contract_defaults(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "config")
    cfg = driver._portfolio_sim_config()
    assert cfg.explicit_cost_roundtrip == 0.0035
    assert cfg.adv_window == 20
    assert cfg.min_resonance_families == 2
    assert cfg.max_holding_bars == 10
    assert dict(cfg.family_cap) == {"trend": 3}
    assert cfg.industry_cap == 2


def test_s4_window_parser_rejects_bad_input(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "window")
    assert driver._parse_portfolio_sim_window(None) is None
    start, end = driver._parse_portfolio_sim_window("2024-03-01:2024-03-29")
    assert start == pd.Timestamp("2024-03-01")
    assert end == pd.Timestamp("2024-03-29")
    try:
        driver._parse_portfolio_sim_window("2024-03-29:2024-03-01")
    except ValueError:
        pass
    else:
        raise AssertionError("reversed window must raise")
