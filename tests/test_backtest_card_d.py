# -*- coding: utf-8 -*-
"""Card D guardrails: the layered money-flow factor is opt-in and reuses existing paths.

The factor was measured at signal level on 2026-08-24 (37.8% win rate) and removed from
``STRATEGIES``. Card D re-registers it only behind ``--enable-smart-money-flow`` so the
default strategy pool and every frozen ``config_hash`` stay byte-identical.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pandas as pd
import pytest

from scripts.portfolio_sim import STRATEGY_FAMILY

DRIVER = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_23strategies.py"

pytestmark = pytest.mark.usefixtures("restore_env_after_test")


@pytest.fixture(autouse=True)
def _restore_shared_family_map():
    """Opt-in registration mutates the shared family map; keep other test modules clean."""
    before = dict(STRATEGY_FAMILY)
    yield
    STRATEGY_FAMILY.clear()
    STRATEGY_FAMILY.update(before)


def _load_driver(log_dir: Path):
    os.environ["LOG_DIR"] = str(log_dir)
    spec = importlib.util.spec_from_file_location("bt_driver_card_d_test", DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _moneyflow_frame(net_big: list[float], *, with_column: bool = True) -> pd.DataFrame:
    """Synthetic single-stock frame satisfying every non-moneyflow condition."""
    data = {
        "code": ["sh.600000"] * len(net_big),
        "close": [110.0] * len(net_big),
        "open": [100.0] * len(net_big),
        "ma20": [100.0] * len(net_big),
        "buy_sm_amount": [10.0] * len(net_big),
        "sell_sm_amount": [20.0] * len(net_big),
    }
    if with_column:
        data["net_big_amount"] = net_big
    return pd.DataFrame(data)


def test_default_registry_excludes_smart_money_flow(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "default")
    assert driver.SMART_MONEY_FLOW_STRATEGY not in driver.STRATEGIES
    assert driver.SMART_MONEY_FLOW_STRATEGY not in driver.STRATEGY_MASKS
    assert len(driver.STRATEGIES) == 24
    assert len(driver.STRATEGY_MASKS) == 24


def test_default_strategy_registries_share_keys(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "registry")
    assert set(driver.STRATEGIES) == set(driver.STRATEGY_MASKS) == set(STRATEGY_FAMILY)
    assert len(driver.STRATEGIES) == 24


def test_factor_family_is_fundamental_and_default_map_is_untouched(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "family")
    assert "smart_money_flow" not in STRATEGY_FAMILY
    baseline = len(STRATEGY_FAMILY)

    driver._register_smart_money_flow()

    assert len(STRATEGY_FAMILY) == baseline + 1
    assert STRATEGY_FAMILY["smart_money_flow"] == "fundamental"


def test_register_is_idempotent_and_reuses_existing_signal_function(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "register")
    baseline = len(driver.STRATEGIES)

    driver._register_smart_money_flow()
    driver._register_smart_money_flow()

    assert len(driver.STRATEGIES) == baseline + 1
    assert driver.STRATEGIES[driver.SMART_MONEY_FLOW_STRATEGY] is driver.sig_smart_money_flow
    assert driver.STRATEGY_MASKS[driver.SMART_MONEY_FLOW_STRATEGY] == "standard"
    assert set(driver.STRATEGIES) == set(driver.STRATEGY_MASKS) == set(STRATEGY_FAMILY)


def test_signal_requires_three_day_inflow_and_retail_outflow(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "signal")
    # Row 3 is the first row with today + two prior days all positive.
    frame = _moneyflow_frame([-1.0, 1.0, 1.0, 1.0])
    signal = driver.sig_smart_money_flow(frame)
    assert signal.tolist() == [False, False, False, True]

    # Retail must be net buyers for the signal to stay off.
    retail_in = frame.copy()
    retail_in["buy_sm_amount"] = 30.0
    assert not driver.sig_smart_money_flow(retail_in).any()


@pytest.mark.parametrize("missing", ["net_big_amount", "buy_sm_amount", "sell_sm_amount"])
def test_signal_fails_closed_when_a_layered_column_is_missing(tmp_path: Path, missing: str) -> None:
    driver = _load_driver(tmp_path / f"missing-{missing}")
    frame = _moneyflow_frame([1.0, 1.0, 1.0], with_column=missing != "net_big_amount")
    if missing != "net_big_amount":
        frame = frame.drop(columns=[missing])
    assert driver.sig_smart_money_flow(frame).tolist() == [False, False, False]


def test_cli_rejects_factor_without_portfolio_sim(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "cli")
    with pytest.raises(SystemExit) as excinfo:
        driver.main(["--enable-smart-money-flow"])
    assert excinfo.value.code == 2
