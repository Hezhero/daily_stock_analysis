"""Regression tests for the nightly Tushare incremental path.

Tushare's ``daily`` endpoint returns an empty frame when ``ts_code`` contains a
comma-separated list. The nightly collector must therefore fetch by
``trade_date`` (one day, all stocks) instead of batching codes.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "data_collection" / "incremental_daily.py"
MODULE_DIR = MODULE_PATH.parent


def _load_module():
    sys.path.insert(0, str(MODULE_DIR))
    try:
        spec = importlib.util.spec_from_file_location(
            "incremental_daily_under_test", MODULE_PATH
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.remove(str(MODULE_DIR))


class FakeClient:
    def __init__(self):
        self.calls = []

    def query(self, api_name, fields="", **kwargs):
        self.calls.append((api_name, kwargs))
        if api_name == "daily":
            return pd.DataFrame(
                [
                    {
                        "ts_code": "000001.SZ",
                        "trade_date": "20261105",
                        "change": 0.1,
                    }
                ]
            )
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20261105",
                    "adj_factor": 1.0,
                }
            ]
        )


def test_pull_daily_and_adj_fetches_by_trade_date(monkeypatch):
    module = _load_module()
    inserted = []

    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        module,
        "insert_dataframe",
        lambda conn, table, df, conflict: (
            inserted.append((table, df.copy())) or len(df)
        ),
    )

    client = FakeClient()
    module.pull_daily_and_adj(client, object(), ["20261105"])

    assert client.calls, "expected at least one API call"
    assert all("ts_code" not in kwargs for _, kwargs in client.calls)
    assert {kwargs["trade_date"] for _, kwargs in client.calls} == {"20261105"}
    assert {table for table, _ in inserted} == {
        "tushare_daily",
        "tushare_adj_factor",
    }

    daily = next(df for table, df in inserted if table == "tushare_daily")
    assert "change_val" in daily.columns
    assert "change" not in daily.columns
