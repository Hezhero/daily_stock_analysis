# -*- coding: utf-8 -*-

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_22strategies_concurrent.py"


def load_backtest_concurrent_script_module():
    spec = spec_from_file_location("backtest_5y_22strategies_concurrent", SCRIPT_PATH)
    module = module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_get_top_stocks_by_win_rate_uses_buy_date_name_for_same_code() -> None:
    module = load_backtest_concurrent_script_module()
    dates = pd.date_range("2026-03-22", periods=6, freq="D")
    df_week = pd.DataFrame(
        [
            {"code": "sz.000001", "name": "旧名称", "date": dates[0], "open": 10.0, "close": 10.1, "pct_chg": 1.0},
            {"code": "sz.000001", "name": "买入日名称", "date": dates[1], "open": 10.2, "close": 10.3, "pct_chg": 1.0},
            {"code": "sz.000001", "name": "买入日名称", "date": dates[2], "open": 10.4, "close": 10.5, "pct_chg": 1.0},
            {"code": "sz.000001", "name": "买入日名称", "date": dates[3], "open": 10.6, "close": 10.7, "pct_chg": 1.0},
            {"code": "sz.000001", "name": "买入日名称", "date": dates[4], "open": 10.8, "close": 10.9, "pct_chg": 1.0},
            {"code": "sz.000001", "name": "买入日名称", "date": dates[5], "open": 11.0, "close": 11.1, "pct_chg": 1.0},
        ]
    )

    module.STRATEGIES["name_mismatch_strategy"] = lambda frame: pd.Series([True, True, False, False, False, False], index=frame.index)

    result = module.get_top_stocks_by_win_rate(
        df_week,
        [{"strategy": "name_mismatch_strategy", "win_rate": 80.0, "total_trades": 2}],
        top_n=10,
    )

    assert result[0]["code"] == "sz.000001"
    assert result[0]["name"] == "买入日名称"
