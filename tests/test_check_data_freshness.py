"""Pure-function tests for the data freshness checker; no database required."""
from __future__ import annotations

from datetime import date, datetime

from scripts.check_data_freshness import (
    TableStatus,
    _as_date,
    _pg_dbname,
    evaluate,
    missing_trade_days,
    parse_tables,
    render_statuses,
)


def test_as_date_accepts_date_datetime_and_string():
    assert _as_date(date(2026, 11, 4)) == date(2026, 11, 4)
    assert _as_date(datetime(2026, 11, 4, 15, 30)) == date(2026, 11, 4)
    assert _as_date("2026-11-04 00:00:00") == date(2026, 11, 4)
    assert _as_date(None) is None


def test_missing_trade_days_without_any_data_returns_every_open_day():
    days = [date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)]

    assert missing_trade_days(days, None, date(2026, 11, 4)) == days


def test_missing_trade_days_only_returns_days_after_the_latest_row():
    days = [date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)]

    assert missing_trade_days(days, date(2026, 11, 2), date(2026, 11, 4)) == [
        date(2026, 11, 3),
        date(2026, 11, 4),
    ]


def test_missing_trade_days_ignores_the_future():
    days = [date(2026, 11, 2), date(2026, 11, 5)]

    assert missing_trade_days(days, date(2026, 11, 2), date(2026, 11, 4)) == []


def test_evaluate_is_fresh_at_the_allowed_lag():
    statuses = [
        TableStatus("tushare_daily", "trade_date", date(2026, 11, 3), 1),
        TableStatus("tushare_cyq", "trade_date", date(2026, 11, 4), 0),
    ]

    assert evaluate(statuses, allow_lag=1) == (True, [])
    assert evaluate(statuses, allow_lag=0)[0] is False


def test_evaluate_treats_a_missing_table_as_stale():
    statuses = [TableStatus("tushare_cyq", "trade_date", None, 0, exists=False)]

    fresh, stale = evaluate(statuses, allow_lag=5)

    assert fresh is False
    assert stale == statuses


def test_pg_dbname_matches_the_shared_pg_settings_precedence(monkeypatch):
    monkeypatch.delenv("PG_DBNAME", raising=False)
    monkeypatch.delenv("PG_DATABASE", raising=False)
    assert _pg_dbname() == "tushare"

    monkeypatch.setenv("PG_DATABASE", "warehouse")
    assert _pg_dbname() == "warehouse"

    monkeypatch.setenv("PG_DBNAME", "custom")
    assert _pg_dbname() == "custom"

    monkeypatch.setenv("PG_DBNAME", "  ")
    assert _pg_dbname() == "tushare"


def test_parse_tables_defaults_and_overrides():
    assert parse_tables(None)[0] == ("tushare_daily", "trade_date")
    assert parse_tables(["tushare_cyq"]) == [("tushare_cyq", "trade_date")]
    assert parse_tables(["tushare_daily:end_date"]) == [("tushare_daily", "end_date")]


def test_parse_tables_rejects_non_identifier_input():
    for spec in ("tushare_daily; DROP TABLE x", "tushare-daily", "1daily"):
        try:
            parse_tables([spec])
        except ValueError:
            continue
        raise AssertionError(f"expected ValueError for {spec!r}")


def test_render_statuses_marks_missing_and_empty_tables():
    text = render_statuses(
        [
            TableStatus("tushare_daily", "trade_date", date(2026, 11, 3), 1),
            TableStatus("tushare_cyq", "trade_date", None, 0, exists=False),
            TableStatus("tushare_moneyflow", "trade_date", None, 2),
        ],
        date(2026, 11, 4),
        0,
    )

    assert "expected_trade_date=2026-11-04 allow_lag=0" in text
    assert "tushare_daily: latest=2026-11-03 missing_trade_days=1" in text
    assert "tushare_cyq: MISSING TABLE" in text
    assert "tushare_moneyflow: latest=EMPTY missing_trade_days=2" in text
