# -*- coding: utf-8 -*-

from pathlib import Path

import pandas as pd

from tests.test_backtest_5y_22strategies_script import load_backtest_script_module


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_22strategies.py"


def test_cache_ready_data_keeps_next_day_recommendations_output_stable() -> None:
    module = load_backtest_script_module()
    latest_date = pd.Timestamp("2026-03-27")
    df_latest = pd.DataFrame(
        {
            "code": ["sz.000001", "sh.600000"],
            "name": ["平安银行", "浦发银行"],
            "date": [latest_date, latest_date],
            "close": [10.5, 12.3],
            "pct_chg": [1.2, -0.5],
            "volume": [1000.0, 900.0],
            "ma5": [10.2, 12.1],
            "ma20": [9.8, 12.0],
            "rsi6": [55.0, 48.0],
        }
    )
    strategy_results = [{"strategy": "good_strategy", "win_rate": 80.0, "total_trades": 12}]
    original_strategy = module.STRATEGIES.get("good_strategy")
    module.STRATEGIES["good_strategy"] = lambda frame: pd.Series([True, False], index=frame.index)

    try:
        recommendations, unique_strategies = module.get_next_day_recommendations(
            df_latest.copy(),
            strategy_results,
            top_n=10,
        )
    finally:
        if original_strategy is None:
            module.STRATEGIES.pop("good_strategy", None)
        else:
            module.STRATEGIES["good_strategy"] = original_strategy

    assert unique_strategies == ["good_strategy"]
    assert [item["code"] for item in recommendations] == ["sz.000001"]
