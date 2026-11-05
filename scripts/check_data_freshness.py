#!/usr/bin/env python3
"""检查本地 PostgreSQL 行情数据是否跟上最近交易日。

回测前先跑这个脚本，避免用“看起来有数据”的库跑出过期结论：

    python scripts/check_data_freshness.py
    python scripts/check_data_freshness.py --allow-lag 1

数据源为 ``tushare_trade_cal``（SSE 交易日历）与各表的 ``trade_date`` 最大值。
退出码：``0`` 全部新鲜，``1`` 有表落后，``2`` 配置或连接错误。
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Sequence

import psycopg2
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_TABLES: tuple[tuple[str, str], ...] = (
    ("tushare_daily", "trade_date"),
    ("tushare_adj_factor", "trade_date"),
    ("tushare_daily_basic", "trade_date"),
    ("tushare_cyq", "trade_date"),
    ("tushare_moneyflow", "trade_date"),
    ("tushare_index_daily", "trade_date"),
)

EXIT_OK = 0
EXIT_STALE = 1
EXIT_ERROR = 2

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class TableStatus:
    table: str
    column: str
    latest: Optional[date]
    missing_days: int
    exists: bool = True


def _as_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()


def missing_trade_days(
    open_days: Iterable[date], latest: Optional[date], expected: date
) -> list[date]:
    """返回 ``latest`` 之后、``expected`` 之前（含）尚未落库的交易日。"""
    days = [day for day in open_days if day <= expected]
    if latest is None:
        return sorted(days)
    return sorted(day for day in days if day > latest)


def evaluate(
    statuses: Sequence[TableStatus], allow_lag: int = 0
) -> tuple[bool, list[TableStatus]]:
    """返回 (是否新鲜, 落后超过阈值的表)。"""
    stale = [
        status
        for status in statuses
        if not status.exists or status.missing_days > max(0, allow_lag)
    ]
    return not stale, stale


def parse_tables(items: Optional[Sequence[str]]) -> list[tuple[str, str]]:
    """把 ``--table NAME[:COLUMN]`` 解析成 (表名, 日期列)，并拒绝非法标识符。"""
    if not items:
        return list(DEFAULT_TABLES)
    tables = []
    for item in items:
        name, _, column = item.partition(":")
        name = name.strip()
        column = (column or "trade_date").strip()
        if not _IDENTIFIER.match(name) or not _IDENTIFIER.match(column):
            raise ValueError(f"invalid table spec: {item!r}")
        tables.append((name, column))
    return tables


def _connect():
    load_dotenv(REPO_ROOT / ".env")
    password = os.getenv("PG_PASSWORD", "")
    if not password:
        raise ValueError("PG_PASSWORD 未设置，请在 .env 中配置数据库连接")
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5432")),
        user=os.getenv("PG_USER", "root"),
        password=password,
        dbname=os.getenv("PG_DBNAME", "tushare"),
        connect_timeout=10,
    )


def _latest_open_day(conn, as_of: date) -> Optional[date]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(cal_date) FROM tushare_trade_cal "
            "WHERE exchange='SSE' AND is_open=1 AND cal_date <= %s",
            (as_of,),
        )
        return _as_date(cur.fetchone()[0])


def _open_days(conn, start: date, end: date) -> list[date]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cal_date FROM tushare_trade_cal "
            "WHERE exchange='SSE' AND is_open=1 AND cal_date BETWEEN %s AND %s "
            "ORDER BY cal_date",
            (start, end),
        )
        return [_as_date(row[0]) for row in cur.fetchall()]


def _table_latest(conn, table: str, column: str) -> tuple[Optional[date], bool]:
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX({column}) FROM {table}")  # noqa: S608 - fixed names
            return _as_date(cur.fetchone()[0]), True
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        return None, False


def collect_statuses(
    conn,
    tables: Sequence[tuple[str, str]],
    expected: date,
    lookback_days: int = 60,
) -> list[TableStatus]:
    open_days = _open_days(conn, expected - timedelta(days=lookback_days), expected)
    statuses = []
    for table, column in tables:
        latest, exists = _table_latest(conn, table, column)
        missing = missing_trade_days(open_days, latest, expected)
        statuses.append(
            TableStatus(
                table=table,
                column=column,
                latest=latest,
                missing_days=len(missing),
                exists=exists,
            )
        )
    return statuses


def render_statuses(
    statuses: Sequence[TableStatus], expected: Optional[date], allow_lag: int
) -> str:
    lines = [
        f"data-freshness: expected_trade_date={expected or 'unknown'} allow_lag={allow_lag}"
    ]
    for status in statuses:
        if not status.exists:
            lines.append(f"  - {status.table}: MISSING TABLE")
            continue
        latest = status.latest.isoformat() if status.latest else "EMPTY"
        lines.append(
            f"  - {status.table}: latest={latest} missing_trade_days={status.missing_days}"
        )
    return "\n".join(lines)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--table",
        action="append",
        metavar="NAME[:COLUMN]",
        help="只检查指定表；可重复。默认检查日线/复权/指标/筹码/资金流/指数",
    )
    parser.add_argument(
        "--allow-lag",
        type=int,
        default=0,
        help="允许落后的交易日数（默认 0：必须跟上最近交易日）",
    )
    parser.add_argument(
        "--as-of",
        help="以该日期作为“今天”（YYYY-MM-DD），用于补跑或验证",
    )
    parser.add_argument("--json", action="store_true", help="输出机器可读 JSON")
    args = parser.parse_args(argv)

    if args.allow_lag < 0:
        print("data-freshness: --allow-lag must be >= 0", file=sys.stderr)
        return EXIT_ERROR

    try:
        tables = parse_tables(args.table)
        as_of = (
            datetime.strptime(args.as_of, "%Y-%m-%d").date()
            if args.as_of
            else date.today()
        )
        conn = _connect()
    except ValueError as exc:
        print(f"data-freshness: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except Exception as exc:  # noqa: BLE001 - surfaced as a config error
        print(f"data-freshness: cannot connect: {exc}", file=sys.stderr)
        return EXIT_ERROR

    try:
        expected = _latest_open_day(conn, as_of)
        if expected is None:
            print(
                "data-freshness: tushare_trade_cal has no open day at or before "
                f"{as_of}; is the calendar loaded?",
                file=sys.stderr,
            )
            return EXIT_ERROR
        statuses = collect_statuses(conn, tables, expected)
    except Exception as exc:  # noqa: BLE001 - surfaced as a query error
        print(f"data-freshness: query failed: {exc}", file=sys.stderr)
        return EXIT_ERROR
    finally:
        conn.close()

    fresh, stale = evaluate(statuses, allow_lag=args.allow_lag)
    if args.json:
        import json

        print(
            json.dumps(
                {
                    "expected_trade_date": expected.isoformat(),
                    "allow_lag": args.allow_lag,
                    "fresh": fresh,
                    "tables": [
                        {
                            "table": status.table,
                            "latest": status.latest.isoformat() if status.latest else None,
                            "missing_trade_days": status.missing_days,
                            "exists": status.exists,
                        }
                        for status in statuses
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(render_statuses(statuses, expected, args.allow_lag))

    if fresh:
        print("\nOK: all checked tables are fresh.", file=sys.stderr if args.json else sys.stdout)
        return EXIT_OK

    stream = sys.stderr if args.json else sys.stdout
    print("\nSTALE: run the daily collector before using this database for backtests:", file=stream)
    print("  conda run --live-stream -n daily_stock_analysis python "
          "scripts/data_collection/run_all_incremental.py", file=stream)
    for status in stale:
        print(f"  - {status.table} (missing_trade_days={status.missing_days})", file=stream)
    return EXIT_STALE


if __name__ == "__main__":
    raise SystemExit(main())
