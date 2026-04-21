# -*- coding: utf-8 -*-

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import py_compile

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_22strategies.py"


def load_backtest_script_module():
    spec = spec_from_file_location("backtest_5y_22strategies", SCRIPT_PATH)
    module = module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_script_test_module_exposes_loader_helper() -> None:
    assert callable(load_backtest_script_module)


def test_backtest_script_compiles() -> None:
    py_compile.compile(str(SCRIPT_PATH), doraise=True)


def test_apply_forward_adjustment_normalizes_datetime_precision_before_merge_asof() -> None:
    module = load_backtest_script_module()
    df_market = pd.DataFrame(
        {
            "code": ["sz.000001"],
            "date": pd.Series(["2024-01-02 09:30:00.123"], dtype="datetime64[ms]"),
            "open": [10.0],
            "high": [12.0],
            "low": [9.0],
            "close": [11.0],
        }
    )
    df_factor = pd.DataFrame(
        {
            "code": ["sz.000001"],
            "dividOperateDate": pd.Series(["2024-01-02 09:30:00"], dtype="datetime64[s]"),
            "foreAdjustFactor": [2.0],
        }
    )

    adjusted = module.apply_forward_adjustment(df_market, df_factor)

    assert len(adjusted) == 1
    assert adjusted.loc[0, "code"] == "sz.000001"
    assert adjusted.loc[0, "date"] == pd.Timestamp("2024-01-02 09:30:00.123")
    assert adjusted.loc[0, "open"] == 20.0
    assert adjusted.loc[0, "high"] == 24.0
    assert adjusted.loc[0, "low"] == 18.0
    assert adjusted.loc[0, "close"] == 22.0
