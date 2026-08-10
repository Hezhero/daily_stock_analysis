# -*- coding: utf-8 -*-
"""
宏观利率类数据拉取脚本（tushare 2000 积分）

覆盖 2 张扩展表:
  shibor / shibor_quote

实测约束（重要，2026-08 实测）:
  - shibor:            单次返回上限 2000 行，限频宽松（500次/分），按年拉取即可
  - shibor_quote:      单次上限 4000 行（约 1 年），限频实测 1次/小时

限频接口策略（1次/小时）:
  - 只补"最近缺失年份"（最新优先），每次运行最多补 RATE_LIMIT_MAX_YEARS 年，
    年与年之间 sleep 3600s，避免触发限频；运行可随时中断，幂等续跑。
  - 全量历史回补需要多次运行（每次约 1 小时/年），可由定时任务逐次推进。

用法:
  python scripts/data_collection/incremental_macro.py            # 增量
  python scripts/data_collection/incremental_macro.py 20100101 20260806  # 指定范围
"""

import logging
import os
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

DEFAULT_START = "20100101"
SLEEP_BETWEEN = 0.3
RATE_LIMIT = 480
RATE_LIMITED_SLEEP = 3600  # 实测 1次/小时 接口的调用间隔
RATE_LIMIT_MAX_YEARS = int(os.environ.get("RATE_LIMIT_MAX_YEARS", "2"))  # 每次运行限频接口最多补最近 N 年


def _fmt(d) -> str:
    return d.strftime("%Y%m%d")


def get_existing_years(conn, table: str, col: str) -> set[int]:
    if not bootstrap.table_exists(conn, table):
        return set()
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT DISTINCT EXTRACT(YEAR FROM {col})::int FROM {table}")
        return {int(row[0]) for row in cur.fetchall()}
    finally:
        cur.close()


class RateLimitClock:
    """限频时钟：记录最近一次调用时间，保证相邻调用间隔 >= RATE_LIMITED_SLEEP。

    每个 1次/小时 接口使用独立实例（实测限频按接口隔离，接口间可并行）。
    """

    def __init__(self, interval: int = RATE_LIMITED_SLEEP):
        self.interval = interval
        self.last_call = 0.0

    def wait_if_needed(self):
        now = time.time()
        elapsed = now - self.last_call
        if self.last_call and elapsed < self.interval:
            wait = self.interval - elapsed
            logger.info("  限频接口，等待 %ds...", round(wait))
            time.sleep(wait)
        self.last_call = time.time()


def pull_year_loop(
    client: TushareClient, conn,
    api_name: str, table_name: str, conflict: str,
    start_year: int, end_year: int,
    rate_limited: bool = False, date_col: str = "date",
    clock: RateLimitClock | None = None,
) -> int:
    """按年轮询拉取，返回新增行数。

    限频接口（1次/小时）: 只补最近 RATE_LIMIT_MAX_YEARS 个缺失年份（最新优先），
    年份之间经共享 clock 保证 1 小时间隔；非限频接口按年全量补齐。
    """
    existing = get_existing_years(conn, table_name, date_col)
    years = [y for y in range(start_year, end_year + 1) if y not in existing]
    if not years:
        logger.info("--- %s -> %s: 已有数据，跳过 ---", api_name, table_name)
        return 0

    if rate_limited:
        years = sorted(years, reverse=True)[:RATE_LIMIT_MAX_YEARS]
        logger.info("--- %s -> %s: 限频接口，本次补最近 %d 年: %s ---",
                    api_name, table_name, len(years), years)
    else:
        logger.info("--- %s -> %s: 待补年份 %s ---", api_name, table_name, years)

    total = 0
    for i, y in enumerate(years):
        if rate_limited and clock is not None:
            clock.wait_if_needed()
        try:
            df = client.query(
                api_name,
                start_date=f"{y}0101",
                end_date=f"{y}1231",
            )
            if not df.empty:
                total += insert_dataframe(conn, table_name, df, conflict)
                logger.info("  %s %d 写入 %d 行", api_name, y, len(df))
            else:
                logger.info("  %s %d 无数据", api_name, y)
        except Exception as exc:
            logger.warning("  %s %d 失败: %s", api_name, y, str(exc)[:150])
            if rate_limited and clock is not None:
                # 接口可能仍在冷却期（上次外部调用未满 1 小时），等满 1 小时重试一次
                clock.wait_if_needed()
                try:
                    df = client.query(
                        api_name,
                        start_date=f"{y}0101",
                        end_date=f"{y}1231",
                    )
                    if not df.empty:
                        total += insert_dataframe(conn, table_name, df, conflict)
                        logger.info("  %s %d 重试成功，写入 %d 行", api_name, y, len(df))
                except Exception as exc2:
                    logger.warning("  %s %d 重试仍失败: %s", api_name, y, str(exc2)[:150])
        time.sleep(SLEEP_BETWEEN)
    logger.info("  %s 完成: %s 行", table_name, f"{total:,}")
    return total


def _print_summary(conn):
    tables = [
        ("tushare_shibor", "上海银行间同业拆放利率"),
        ("tushare_shibor_quote", "Shibor 报价"),
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
        # 实测限频按接口隔离：每个 1次/小时 接口用独立时钟，接口间无需互相等待
        clock_quote = RateLimitClock()

        # 非限频，按年全量补齐
        grand_total += pull_year_loop(client, conn, "shibor", "tushare_shibor",
                                      "(date)", start_year, end_year, rate_limited=False)
        # 限频 1次/小时，最近年份优先，每次运行最多补 RATE_LIMIT_MAX_YEARS 年
        grand_total += pull_year_loop(client, conn, "shibor_quote", "tushare_shibor_quote",
                                      "(date, bank)", start_year, end_year,
                                      rate_limited=True, clock=clock_quote)

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