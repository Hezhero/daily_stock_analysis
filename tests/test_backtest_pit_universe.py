# -*- coding: utf-8 -*-
"""T0.4: PIT universe predicate, CLI default, and config-hash guards."""

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import logging
import os
from pathlib import Path

from scripts.portfolio_sim import SimConfig, _config_hash
import pytest

DRIVER = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_23strategies.py"

pytestmark = pytest.mark.usefixtures("restore_env_after_test")

# 已发布 T0.1 基线（ADR 0001）。l-only 默认必须逐位复现。
T01_BASELINE_CONFIG_HASH = "5eea05195af3d89432af8b2a049fde2517d6a53b"


def _load_driver(log_dir: Path):
    os.environ["LOG_DIR"] = str(log_dir)
    spec = importlib.util.spec_from_file_location("bt_driver_pit_test", DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _parser_with_universe():
    parser = argparse.ArgumentParser()
    return parser


def test_universe_flag_defaults_to_pit(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "flag")
    parser = _parser_with_universe()
    driver._add_universe_argument(parser)

    assert parser.parse_args([]).universe == "pit"
    assert parser.parse_args(["--universe", "l-only"]).universe == "l-only"


def test_unknown_universe_is_rejected(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "flag-invalid")
    parser = _parser_with_universe()
    driver._add_universe_argument(parser)

    try:
        parser.parse_args(["--universe", "survivor"])
    except SystemExit:
        pass
    else:
        raise AssertionError("unknown --universe value must fail argparse")


def test_l_only_predicate_is_byte_identical_to_legacy_filter(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "predicate-l")
    assert driver._universe_filter_sql("l-only") == "\n".join([
        "          AND s.list_status = 'L'",
        "          AND s.exchange IN ('SSE', 'SZSE')",
        "          AND s.market IN ('主板', '中小板', '创业板')",
    ])


def test_pit_predicate_drops_list_status_and_bounds_tradable_window(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "predicate-pit")
    sql = driver._universe_filter_sql("pit")

    assert "list_status" not in sql
    assert "s.exchange IN ('SSE', 'SZSE')" in sql
    assert "s.market IN ('主板', '中小板', '创业板')" in sql
    assert "s.delist_date IS NULL OR d.trade_date <= s.delist_date" in sql


def test_unknown_universe_predicate_raises(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "predicate-invalid")
    try:
        driver._universe_filter_sql("survivor")
    except ValueError as exc:
        assert "survivor" in str(exc)
    else:
        raise AssertionError("unknown universe must raise")


class _FakeEngine:
    def __init__(self) -> None:
        self.disposed = False

    def connect(self):
        return contextlib.nullcontext(object())

    def dispose(self) -> None:
        self.disposed = True


def _capture_load_sql(driver, monkeypatch):
    captured = {}

    def fake_read_sql(sql, conn, params=None, parse_dates=None, chunksize=None):
        captured["sql"] = sql
        captured["params"] = params
        return iter(())

    monkeypatch.setattr(driver, "_get_pg_engine", lambda: _FakeEngine())
    monkeypatch.setattr(driver.pd, "read_sql", fake_read_sql)
    return captured


def test_load_data_pit_sql_uses_pit_predicate_and_same_binds(tmp_path, monkeypatch) -> None:
    driver = _load_driver(tmp_path / "load-pit")
    captured = _capture_load_sql(driver, monkeypatch)

    driver.load_data("2021-05-14", "2026-10-16", universe="pit")

    assert "list_status" not in captured["sql"]
    assert "s.delist_date IS NULL OR d.trade_date <= s.delist_date" in captured["sql"]
    assert captured["params"] == (
        "2021-05-14", "2026-10-16", driver.MIN_DAILY_AMOUNT_K, driver.MIN_LISTING_DAYS,
    )


def test_load_data_l_only_sql_keeps_legacy_filter(tmp_path, monkeypatch) -> None:
    driver = _load_driver(tmp_path / "load-l")
    captured = _capture_load_sql(driver, monkeypatch)

    driver.load_data("2021-05-14", "2026-10-16", universe="l-only")

    assert "AND s.list_status = 'L'" in captured["sql"]
    assert "delist_date" not in captured["sql"]


def test_prepared_cache_path_is_isolated_for_pit(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "cache")
    l_only = driver._prepared_cache_path("2021-05-14", "2026-10-16", universe="l-only")
    pit = driver._prepared_cache_path("2021-05-14", "2026-10-16", universe="pit")

    assert l_only.name == f"backtest_20210514_20261016_{driver.CACHE_VERSION}.parquet"
    assert pit.name == f"backtest_20210514_20261016_{driver.CACHE_VERSION}upit.parquet"
    assert l_only != pit


def test_config_hash_default_is_frozen_baseline_and_pit_differs(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "hash")
    baseline = _config_hash(SimConfig())

    assert baseline == T01_BASELINE_CONFIG_HASH
    assert driver._universe_config_hash(baseline, "l-only") == T01_BASELINE_CONFIG_HASH

    pit = driver._universe_config_hash(baseline, "pit")
    assert pit != T01_BASELINE_CONFIG_HASH
    assert pit == driver._universe_config_hash(baseline, "pit")
    # 默认口径 = pit（ADR 0001 冻结度量口径，2026-10-19 由 l-only 切换）
    assert driver._universe_config_hash(baseline) == pit


def test_artifact_stem_isolates_pit_from_l_only(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "stem")

    l_only = driver._portfolio_sim_artifact_stem("2021-05-14", "2026-10-16", "l-only")
    pit = driver._portfolio_sim_artifact_stem("2021-05-14", "2026-10-16", "pit")

    assert l_only == "portfolio_sim_20210514_20261016"
    assert pit == "portfolio_sim_20210514_20261016_pit"
    assert l_only != pit


def test_pit_without_delisted_rows_warns_explicitly(tmp_path, monkeypatch, caplog) -> None:
    driver = _load_driver(tmp_path / "warn")
    monkeypatch.setattr(driver, "_count_delisted_stock_rows", lambda: 0)

    with caplog.at_level(logging.WARNING, logger="backtest"):
        message = driver._check_pit_universe_availability()

    assert message is not None
    assert "list_status='D'" in message
    assert "等价于 L-only" in message
    assert any("等价于 L-only" in record.getMessage() for record in caplog.records)


def test_pit_probe_failure_warns_instead_of_passing(tmp_path, monkeypatch, caplog) -> None:
    driver = _load_driver(tmp_path / "probe-fail")

    def boom():
        raise RuntimeError("db down")

    monkeypatch.setattr(driver, "_count_delisted_stock_rows", boom)

    with caplog.at_level(logging.WARNING, logger="backtest"):
        message = driver._check_pit_universe_availability()

    assert message is not None
    assert "无法验证退市股数据" in message
    assert "db down" in message


def test_pit_with_delisted_rows_is_silent(tmp_path, monkeypatch, caplog) -> None:
    driver = _load_driver(tmp_path / "probe-ok")
    monkeypatch.setattr(driver, "_count_delisted_stock_rows", lambda: 332)

    with caplog.at_level(logging.WARNING, logger="backtest"):
        message = driver._check_pit_universe_availability()

    assert message is None
    assert not any("PIT 宇宙告警" in record.getMessage() for record in caplog.records)
