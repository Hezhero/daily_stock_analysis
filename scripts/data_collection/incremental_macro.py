# -*- coding: utf-8 -*-
"""
宏观利率类数据拉取脚本（tushare 2000 积分）

覆盖 2 张扩展表:
  shibor / shibor_quote

实测约束（重要，2026-08 实测）:
  - shibor:            单次返回上限 2000 行，限频宽松（500次/分），按年拉取即可
  - shibor_quote:      单次上限 4000 行，但一年实际约 4500 行（250 交易日 x 18 家报价行），
                       按年拉取会被截断（实测 2016 全年仅返回 4000 行、丢失年初数据），
                       必须按半年拉取（每年 2 段，每段约 2250 行）；限频实测 1次/分钟
                       （code=40203: 频率超限(1次/分钟)）

限频接口策略（1次/分钟）:
  - 只补"最近缺失段"（最新优先），每次运行最多补 RATE_LIMIT_MAX_YEARS 年，
    段与段之间经共享 clock 保证 >= 60s 间隔，避免触发限频；运行可随时中断，幂等续跑。
  - 全量历史回补（如 2016~2025 共 10 年 = 20 个半年段）约 20 分钟即可完成，
    单次运行即可推进多年，无需跨天续跑。

用法:
  python scripts/data_collection/incremental_macro.py            # 增量（默认自 2016-01-01）
  python scripts/data_collection/incremental_macro.py 20160101 20260806  # 指定范围
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

DEFAULT_START = "20160101"  # shibor_quote 从 2016-01-01 起获取；shibor 已有更早数据时不受影响（按已有段跳过）
SLEEP_BETWEEN = 0.3
RATE_LIMIT = 480
RATE_LIMITED_SLEEP = 60  # 实测 shibor_quote 限频 1次/分钟（code=40203）
RATE_LIMIT_MAX_YEARS = int(os.environ.get("RATE_LIMIT_MAX_YEARS", "10"))  # 每次运行限频接口最多补最近 N 年


def _fmt(d) -> str:
    return d.strftime("%Y%m%d")


def get_existing_segments(conn, table: str, col: str, period: str) -> set[str]:
    """返回已有数据的段标识集合。

    period="year": 段标识为年份字符串（如 "2016"）；
    period="half": 段标识为 "2016H1"/"2016H2"（上半年/下半年）。
    """
    if not bootstrap.table_exists(conn, table):
        return set()
    cur = conn.cursor()
    try:
        if period == "half":
            cur.execute(
                f"SELECT DISTINCT EXTRACT(YEAR FROM {col})::int, "
                f"CASE WHEN to_char({col}, 'MMDD') <= '0630' THEN 'H1' ELSE 'H2' END "
                f"FROM {table}"
            )
            return {f"{int(row[0])}{row[1]}" for row in cur.fetchall()}
        cur.execute(f"SELECT DISTINCT EXTRACT(YEAR FROM {col})::int FROM {table}")
        return {str(int(row[0])) for row in cur.fetchall()}
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
    period: str = "year",
) -> int:
    """按年/半年段轮询拉取，返回新增行数。

    period="year": 每年 1 段（0101-1231），适合行数远低于单次上限的接口（如 shibor）；
    period="half": 每年 2 段（H1: 0101-0630，H2: 0701-1231），适合一年行数超过单次上限
                   的接口（如 shibor_quote 一年约 4500 行 > 4000 行上限）。

    限频接口（实测 1次/分钟）: 只补最近 RATE_LIMIT_MAX_YEARS 个缺失年份的段（最新优先），
    段之间经共享 clock 保证限频间隔；非限频接口按段全量补齐。
    """
    existing = get_existing_segments(conn, table_name, date_col, period)
    if period == "half":
        segments = [f"{y}{h}" for y in range(start_year, end_year + 1) for h in ("H1", "H2")]
    else:
        segments = [str(y) for y in range(start_year, end_year + 1)]
    missing = [s for s in segments if s not in existing]
    if not missing:
        logger.info("--- %s -> %s: 已有数据，跳过 ---", api_name, table_name)
        return 0

    if rate_limited:
        missing = sorted(missing, reverse=True)[:RATE_LIMIT_MAX_YEARS * (2 if period == "half" else 1)]
        logger.info("--- %s -> %s: 限频接口，本次补最近 %d 段: %s ---",
                    api_name, table_name, len(missing), missing)
    else:
        logger.info("--- %s -> %s: 待补段 %s ---", api_name, table_name, missing)

    total = 0
    for seg in missing:
        if rate_limited and clock is not None:
            clock.wait_if_needed()
        if period == "half":
            year, half = int(seg[:-2]), seg[-2:]
            start_date, end_date = (f"{year}0101", f"{year}0630") if half == "H1" else (f"{year}0701", f"{year}1231")
        else:
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
            if rate_limited and clock is not None:
                # 接口可能仍在冷却期（上次外部调用未满 1 分钟），等满限频间隔重试一次
                clock.wait_if_needed()
                try:
                    df = client.query(
                        api_name,
                        start_date=start_date,
                        end_date=end_date,
                    )
                    if not df.empty:
                        total += insert_dataframe(conn, table_name, df, conflict)
                        logger.info("  %s %s 重试成功，写入 %d 行", api_name, seg, len(df))
                except Exception as exc2:
                    logger.warning("  %s %s 重试仍失败: %s", api_name, seg, str(exc2)[:150])
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
        # 实测限频按接口隔离：每个 1次/分钟 接口用独立时钟，接口间无需互相等待
        clock_quote = RateLimitClock()

        # 非限频，按年全量补齐
        grand_total += pull_year_loop(client, conn, "shibor", "tushare_shibor",
                                      "(date)", start_year, end_year, rate_limited=False)
        # 限频 1次/分钟；一年约 4500 行超 4000 行单次上限，按半年段拉取；
        # 最近段优先，每次运行最多补 RATE_LIMIT_MAX_YEARS 年
        grand_total += pull_year_loop(client, conn, "shibor_quote", "tushare_shibor_quote",
                                      "(date, bank)", start_year, end_year,
                                      rate_limited=True, clock=clock_quote, period="half")

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