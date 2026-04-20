# -*- coding: utf-8 -*-

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import py_compile

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_22strategies.py"
ADJUST_FACTOR_CACHE_SQL_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "data_collection" / "create_adjust_factor_cache_table.sql"
)


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


def test_adjust_factor_cache_table_sql_exists() -> None:
    assert ADJUST_FACTOR_CACHE_SQL_PATH.exists()


def test_adjust_factor_cache_table_sql_defines_primary_key_and_index() -> None:
    ddl = ADJUST_FACTOR_CACHE_SQL_PATH.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS adjust_factor_cache" in ddl
    assert "PRIMARY KEY (code, divid_operate_date)" in ddl
    assert "CREATE INDEX IF NOT EXISTS idx_adjust_factor_cache_date" in ddl


def test_cache_version_constants_are_defined() -> None:
    module = load_backtest_script_module()
    assert module.MARKET_CACHE_VERSION
    assert module.ADJUSTMENT_LOGIC_VERSION
    assert module.INDICATOR_CACHE_VERSION
    assert module.FLOAT_PRECISION_VERSION


def test_get_cache_dir_creates_named_directory(monkeypatch, tmp_path) -> None:
    module = load_backtest_script_module()
    monkeypatch.setattr(module, "CACHE_DIR", tmp_path)
    market_dir = module.get_cache_dir("market_data")
    indicator_dir = module.get_cache_dir("indicators")
    assert market_dir == tmp_path / "market_data"
    assert indicator_dir == tmp_path / "indicators"
    assert market_dir.exists()
    assert indicator_dir.exists()


def test_build_market_cache_key_changes_when_version_changes(monkeypatch) -> None:
    module = load_backtest_script_module()
    key1 = module.build_market_cache_key("2026-03-01", "2026-03-31")
    monkeypatch.setattr(module, "MARKET_CACHE_VERSION", "v2")
    key2 = module.build_market_cache_key("2026-03-01", "2026-03-31")
    assert key1 != key2


def test_get_market_cache_path_uses_market_data_directory(monkeypatch, tmp_path) -> None:
    module = load_backtest_script_module()
    monkeypatch.setattr(module, "CACHE_DIR", tmp_path)
    path = module.get_market_cache_path("2026-03-01", "2026-03-31")
    assert path.parent == tmp_path / "market_data"
    assert path.suffix == ".parquet"


def test_indicator_cache_key_changes_with_version(monkeypatch) -> None:
    module = load_backtest_script_module()
    key1 = module.build_indicator_cache_key("market-key-v1")
    monkeypatch.setattr(module, "INDICATOR_CACHE_VERSION", "v2")
    key2 = module.build_indicator_cache_key("market-key-v1")
    assert key1 != key2
    assert "market-key-v1" not in key1
    assert len(key1) == 32


def test_indicator_cache_key_changes_with_adjustment_logic_version(monkeypatch) -> None:
    module = load_backtest_script_module()
    key1 = module.build_indicator_cache_key("market-key-v1")
    monkeypatch.setattr(module, "ADJUSTMENT_LOGIC_VERSION", "v2")
    key2 = module.build_indicator_cache_key("market-key-v1")
    assert key1 != key2


def test_get_indicator_cache_path_uses_indicators_directory(monkeypatch, tmp_path) -> None:
    module = load_backtest_script_module()
    monkeypatch.setattr(module, "CACHE_DIR", tmp_path)
    path = module.get_indicator_cache_path("market-key-v1")
    assert path.parent == tmp_path / "indicators"
    assert path.suffix == ".parquet"


def test_load_or_build_indicator_cache_reads_cached_file(monkeypatch, tmp_path) -> None:
    module = load_backtest_script_module()
    monkeypatch.setattr(module, "CACHE_DIR", tmp_path)
    df = pd.DataFrame({"code": ["sz.000001"], "ma5": [10.2]})
    cache_path = module.get_indicator_cache_path("market-key-v1")
    df.to_parquet(cache_path, index=False)
    monkeypatch.setattr(module, "compute_indicators", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("compute_indicators should not be called")))
    cached = module.load_or_build_indicator_cache(pd.DataFrame({"code": ["ignored"]}), "market-key-v1")
    pd.testing.assert_frame_equal(cached, df)


def test_load_or_build_indicator_cache_writes_parquet_on_miss(monkeypatch, tmp_path) -> None:
    module = load_backtest_script_module()
    monkeypatch.setattr(module, "CACHE_DIR", tmp_path)
    df_adjusted = pd.DataFrame({"code": ["sz.000001"], "close": [10.2]})
    expected = pd.DataFrame({"code": ["sz.000001"], "ma5": [10.2]})
    compute_calls = []

    def fake_compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
        compute_calls.append(df.copy())
        return expected.copy()

    monkeypatch.setattr(module, "compute_indicators", fake_compute_indicators)
    cached = module.load_or_build_indicator_cache(df_adjusted, "market-key-v1")
    pd.testing.assert_frame_equal(cached, expected)
    assert len(compute_calls) == 1
    pd.testing.assert_frame_equal(compute_calls[0], df_adjusted)
    persisted = pd.read_parquet(module.get_indicator_cache_path("market-key-v1"))
    pd.testing.assert_frame_equal(persisted, expected)


def test_load_or_build_market_data_cache_reads_parquet_when_present(monkeypatch, tmp_path) -> None:
    module = load_backtest_script_module()
    monkeypatch.setattr(module, "CACHE_DIR", tmp_path)
    df = pd.DataFrame({"code": ["sz.000001"], "date": [pd.Timestamp("2026-03-27")], "close": [10.2]})
    cache_path = module.get_market_cache_path("2026-03-01", "2026-03-31")
    df.to_parquet(cache_path, index=False)
    monkeypatch.setattr(module, "load_data", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("db should not be called")))
    cached = module.load_or_build_market_data_cache("2026-03-01", "2026-03-31")
    assert cached.loc[0, "close"] == 10.2


def test_load_or_build_market_data_cache_writes_parquet_on_miss(monkeypatch, tmp_path) -> None:
    module = load_backtest_script_module()
    monkeypatch.setattr(module, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        module,
        "load_data",
        lambda *_args, **_kwargs: pd.DataFrame({
            "code": ["sz.000001"],
            "date": [pd.Timestamp("2026-03-27")],
            "close": [10.2],
            "open": [10.0],
            "high": [10.5],
            "low": [9.8],
            "volume": [1000.0],
            "amount": [10200.0],
            "pct_chg": [2.0],
            "turn": [1.5],
            "pe_ttm": [15.0],
            "pb_mrq": [2.0],
        }),
    )
    cached = module.load_or_build_market_data_cache("2026-03-01", "2026-03-31")
    assert not cached.empty
    assert module.get_market_cache_path("2026-03-01", "2026-03-31").exists()


def test_load_or_fill_adjust_factor_cache_reuses_db_rows(monkeypatch) -> None:
    module = load_backtest_script_module()
    cached_df = pd.DataFrame({
        "code": ["sz.000001", "sz.000001", "sh.600000", "sh.600000"],
        "dividOperateDate": [
            pd.Timestamp("2026-03-01"),
            pd.Timestamp("2026-03-31"),
            pd.Timestamp("2026-03-01"),
            pd.Timestamp("2026-03-31"),
        ],
        "foreAdjustFactor": [1.1, 1.1, 0.95, 0.95],
    })

    monkeypatch.setattr(module, "load_adjust_factor_cache_from_db", lambda codes, start, end: cached_df.copy())
    monkeypatch.setattr(
        module,
        "fetch_adjust_factors_from_baostock",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("baostock should not be called when cache is complete")),
    )
    monkeypatch.setattr(
        module,
        "upsert_adjust_factor_cache",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("upsert should not be called when cache is complete")),
    )

    result = module.load_or_fill_adjust_factor_cache(["sz.000001", "sh.600000"], "2026-03-01", "2026-03-31")

    pd.testing.assert_frame_equal(result.reset_index(drop=True), cached_df.reset_index(drop=True))


def test_load_or_fill_adjust_factor_cache_fetches_codes_with_incomplete_range(monkeypatch) -> None:
    module = load_backtest_script_module()
    first_db_df = pd.DataFrame({
        "code": ["sz.000001", "sz.000001", "sh.600000"],
        "dividOperateDate": [
            pd.Timestamp("2026-03-01"),
            pd.Timestamp("2026-03-31"),
            pd.Timestamp("2026-03-02"),
        ],
        "foreAdjustFactor": [1.1, 1.1, 0.95],
    })
    fetched_df = pd.DataFrame({
        "code": ["sh.600000", "sh.600000"],
        "dividOperateDate": [pd.Timestamp("2026-03-01"), pd.Timestamp("2026-03-31")],
        "foreAdjustFactor": [0.95, 0.95],
    })
    final_db_df = pd.concat([first_db_df, fetched_df], ignore_index=True)
    db_calls = []
    upsert_calls = []

    def fake_load_adjust_factor_cache_from_db(codes, start, end):
        db_calls.append((list(codes), start, end))
        if len(db_calls) == 1:
            return first_db_df.copy()
        return final_db_df.copy()

    def fake_fetch_adjust_factors_from_baostock(codes, start, end):
        assert codes == ["sh.600000"]
        assert start == "2026-03-01"
        assert end == "2026-03-31"
        return fetched_df.copy()

    def fake_upsert_adjust_factor_cache(df_factor):
        upsert_calls.append(df_factor.copy())

    monkeypatch.setattr(module, "load_adjust_factor_cache_from_db", fake_load_adjust_factor_cache_from_db)
    monkeypatch.setattr(module, "fetch_adjust_factors_from_baostock", fake_fetch_adjust_factors_from_baostock)
    monkeypatch.setattr(module, "upsert_adjust_factor_cache", fake_upsert_adjust_factor_cache)

    result = module.load_or_fill_adjust_factor_cache(["sz.000001", "sh.600000"], "2026-03-01", "2026-03-31")

    assert db_calls == [
        (["sz.000001", "sh.600000"], "2026-03-01", "2026-03-31"),
        (["sz.000001", "sh.600000"], "2026-03-01", "2026-03-31"),
    ]
    assert len(upsert_calls) == 1
    pd.testing.assert_frame_equal(upsert_calls[0].reset_index(drop=True), fetched_df.reset_index(drop=True))
    pd.testing.assert_frame_equal(result.reset_index(drop=True), final_db_df.reset_index(drop=True))


def test_load_or_fill_adjust_factor_cache_fetches_missing_codes(monkeypatch) -> None:
    module = load_backtest_script_module()
    first_db_df = pd.DataFrame({
        "code": ["sz.000001", "sz.000001"],
        "dividOperateDate": [pd.Timestamp("2026-03-01"), pd.Timestamp("2026-03-31")],
        "foreAdjustFactor": [1.1, 1.1],
    })
    fetched_df = pd.DataFrame({
        "code": ["sh.600000", "sh.600000"],
        "dividOperateDate": [pd.Timestamp("2026-03-01"), pd.Timestamp("2026-03-31")],
        "foreAdjustFactor": [0.95, 0.95],
    })
    final_db_df = pd.concat([first_db_df, fetched_df], ignore_index=True)
    db_calls = []
    upsert_calls = []

    def fake_load_adjust_factor_cache_from_db(codes, start, end):
        db_calls.append((list(codes), start, end))
        if len(db_calls) == 1:
            return first_db_df.copy()
        return final_db_df.copy()

    def fake_fetch_adjust_factors_from_baostock(codes, start, end):
        assert codes == ["sh.600000"]
        assert start == "2026-03-01"
        assert end == "2026-03-31"
        return fetched_df.copy()

    def fake_upsert_adjust_factor_cache(df_factor):
        upsert_calls.append(df_factor.copy())

    monkeypatch.setattr(module, "load_adjust_factor_cache_from_db", fake_load_adjust_factor_cache_from_db)
    monkeypatch.setattr(module, "fetch_adjust_factors_from_baostock", fake_fetch_adjust_factors_from_baostock)
    monkeypatch.setattr(module, "upsert_adjust_factor_cache", fake_upsert_adjust_factor_cache)

    result = module.load_or_fill_adjust_factor_cache(["sz.000001", "sh.600000"], "2026-03-01", "2026-03-31")

    assert db_calls == [
        (["sz.000001", "sh.600000"], "2026-03-01", "2026-03-31"),
        (["sz.000001", "sh.600000"], "2026-03-01", "2026-03-31"),
    ]
    assert len(upsert_calls) == 1
    pd.testing.assert_frame_equal(upsert_calls[0].reset_index(drop=True), fetched_df.reset_index(drop=True))
    pd.testing.assert_frame_equal(result.reset_index(drop=True), final_db_df.reset_index(drop=True))


def test_load_or_fill_adjust_factor_cache_returns_empty_dataframe_for_empty_codes(monkeypatch) -> None:
    module = load_backtest_script_module()
    monkeypatch.setattr(
        module,
        "load_adjust_factor_cache_from_db",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("db should not be called for empty code list")),
    )
    monkeypatch.setattr(
        module,
        "fetch_adjust_factors_from_baostock",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("baostock should not be called for empty code list")),
    )
    monkeypatch.setattr(
        module,
        "upsert_adjust_factor_cache",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("upsert should not be called for empty code list")),
    )

    result = module.load_or_fill_adjust_factor_cache([], "2026-03-01", "2026-03-31")

    assert result.empty
    assert result.columns.tolist() == ["code", "dividOperateDate", "foreAdjustFactor"]


def test_resolve_max_workers_defaults_to_one(monkeypatch) -> None:
    module = load_backtest_script_module()
    monkeypatch.delenv("BACKTEST_MAX_WORKERS", raising=False)

    assert module.resolve_max_workers() == 1


def test_resolve_max_workers_reads_environment_override(monkeypatch) -> None:
    module = load_backtest_script_module()
    monkeypatch.setenv("BACKTEST_MAX_WORKERS", "3")

    assert module.resolve_max_workers() == 3


def test_main_skip_email_still_validates_week(monkeypatch) -> None:
    module = load_backtest_script_module()
    tracked_calls = []
    market_df = pd.DataFrame({
        "code": ["sz.000001"] * 6,
        "name": ["平安银行"] * 6,
        "date": pd.date_range("2026-03-20", periods=6, freq="D"),
        "open": [10.0, 10.1, 10.2, 10.3, 10.4, 10.5],
        "high": [10.1, 10.2, 10.3, 10.4, 10.5, 10.6],
        "low": [9.9, 10.0, 10.1, 10.2, 10.3, 10.4],
        "close": [10.0, 10.1, 10.2, 10.3, 10.4, 10.5],
        "volume": [1000.0] * 6,
        "amount": [10000.0] * 6,
        "pct_chg": [0.0, 1.0, 0.99, 0.98, 0.97, 0.96],
        "turn": [1.0] * 6,
        "pe_ttm": [10.0] * 6,
        "pb_mrq": [1.0] * 6,
    })
    factor_df = pd.DataFrame({
        "code": ["sz.000001"],
        "dividOperateDate": [pd.Timestamp("2026-03-20")],
        "foreAdjustFactor": [1.0],
    })
    indicator_df = market_df.copy()

    monkeypatch.setattr(module, "load_or_build_market_data_cache", lambda *_args, **_kwargs: market_df.copy())
    monkeypatch.setattr(module, "load_or_fill_adjust_factor_cache", lambda codes, *_args, **_kwargs: factor_df.copy())
    monkeypatch.setattr(module, "apply_forward_adjustment", lambda df_market, df_factor: df_market.copy())
    monkeypatch.setattr(module, "build_market_cache_key", lambda *_args, **_kwargs: "market-cache-key")
    monkeypatch.setattr(module, "load_or_build_indicator_cache", lambda df_adjusted, market_cache_key: indicator_df.copy())
    monkeypatch.setattr(module, "run_backtests", lambda df_bt: [{"strategy": "demo", "total_trades": 1, "win_rate": 50.0}])

    def fake_validate_week(df_week, results, top_n):
        tracked_calls.append((df_week.copy(), list(results), top_n))
        return [{"strategy": "demo", "week_trades": 1, "week_win_rate": 50.0, "week_avg_ret": 1.0}]

    monkeypatch.setattr(module, "validate_week", fake_validate_week)
    monkeypatch.setattr(module, "print_results", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(module, "get_top_stocks_by_win_rate", lambda *_args, **_kwargs: [{"code": "sz.000001"}])
    monkeypatch.setattr(
        module,
        "get_next_day_recommendations",
        lambda *_args, **_kwargs: ([{
            "code": "sz.000001",
            "name": "平安银行",
            "strategy_count": 1,
            "avg_win_rate": 50.0,
            "reasons": ["demo reason"],
        }], ["demo"]),
    )
    monkeypatch.setattr(
        module,
        "send_backtest_email",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("send_backtest_email should be skipped")),
    )

    module.main(["--skip-email"])

    assert len(tracked_calls) == 1
