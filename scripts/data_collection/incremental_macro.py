# -*- coding: utf-8 -*-
"""
宏观利率类数据拉取脚本（tushare 2000 积分）

覆盖 1 张扩展表:
  shibor

实测约束（2026-08 实测）:
  - shibor:            单次返回上限 2000 行，限频宽松（500次/分），按年拉取即可

用法:
  python scripts/data_collection/incremental_macro.py            # 增量（默认自 2016-01-01）
  python scripts/data_collection/incremental_macro.py 20160101 20260806  # 指定范围
"""

import logging
import sys
import time
from datetime import date

from tushare_pg_utils import (
    TushareClient,
    get_pg_connection,
    insert_dataframe,
    row_count,
    TUSHARE_TOKEN,
)
import bootstrap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("inc_macro")

DEFAULT_START = "20160101"
SLEEP_BETWEEN = 0.3
RATE_LIMIT = 480


def _fmt(d) -> str:
    return d.strftime("%Y%m%d")


def get_existing_segments(conn, table: str, col: str) -> set[str]:
    """返回已有数据的年份段标识集合（如 "2016"）。"""
    if not bootstrap.table_exists(conn, table):
        return set()
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT DISTINCT EXTRACT(YEAR FROM {col})::int FROM {table}")
        return {str(int(row[0])) for row in cur.fetchall()}
    finally:
        cur.close()


def pull_year_loop(
    client: TushareClient, conn,
    api_name: str, table_name: str, conflict: str,
    start_year: int, end_year: int,
    date_col: str = "date",
) -> int:
    """按年段轮询拉取，返回新增行数。

    每年 1 段（0101-1231），适合行数远低于单次上限的接口（如 shibor）。
    """
    existing = get_existing_segments(conn, table_name, date_col)
    segments = [str(y) for y in range(start_year, end_year + 1)]
    missing = [s for s in segments if s not in existing]
    if not missing:
        logger.info("--- %s -> %s: 已有数据，跳过 ---", api_name, table_name)
        return 0

    logger.info("--- %s -> %s: 待补段 %s ---", api_name, table_name, missing)

    total = 0
    for seg in missing:
        start_date, end_date = f"{seg}0101", f"{seg}1231"
        try:
            df = client.query(
                api_name,
                start_date=start_date,
                end_date=end_date,
            )
            if not df.empty:
                total += insert_dataframe(conn, table_name, df, conflict)
                logger.info("  %s %s 写入 %d 行", api_name, seg, len(df))
            else:
                logger.info("  %s %s 无数据", api_name, seg)
        except Exception as exc:
            logger.warning("  %s %s 失败: %s", api_name, seg, str(exc)[:150])
        time.sleep(SLEEP_BETWEEN)
    logger.info("  %s 完成: %s 行", table_name, f"{total:,}")
    return total


def _print_summary(conn):
    tables = [
        ("tushare_shibor", "上海银行间同业拆放利率"),
    ]
    logger.info("=== 当前数据概况 ===")
    for table, label in tables:
        cnt = row_count(conn, table)
        logger.info("  %s: %s 行", label, f"{cnt:,}" if cnt else "空")


def main():
    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置，请在 .env 中配置")
        sys.exit(1)

    start_arg = sys.argv[1] if len(sys.argv) > 1 else None
    end_arg = sys.argv[2] if len(sys.argv) > 2 else None

    t0 = time.time()
    client = TushareClient(TUSHARE_TOKEN, rate_limit=RATE_LIMIT)
    conn = get_pg_connection()

    try:
        bootstrap.ensure_schema(conn)
        bootstrap.ensure_extra_schema(conn)

        _print_summary(conn)

        today = date.today()
        until = end_arg or _fmt(today)
        since = start_arg or DEFAULT_START
        start_year = int(since[:4])
        end_year = int(until[:4])

        grand_total = 0
        grand_total += pull_year_loop(client, conn, "shibor", "tushare_shibor",
                                      "(date)", start_year, end_year)

        elapsed = time.time() - t0
        logger.info("=== 宏观利率数据刷新完成 耗时 %.0f 秒  新增: %s ===", elapsed, f"{grand_total:,}")
        _print_summary(conn)

    except KeyboardInterrupt:
        logger.warning("用户中断")
    except Exception as exc:
        logger.error("执行失败: %s", exc, exc_info=True)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()
    logger.info("done!")


if __name__ == "__main__":
    main()
