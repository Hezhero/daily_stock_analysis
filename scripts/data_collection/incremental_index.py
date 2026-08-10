# -*- coding: utf-8 -*-
"""
指数类数据增量拉取脚本（tushare 2000 积分）

覆盖 7 张扩展表:
  index_basic / index_classify / index_member_all / index_daily /
  index_weekly / index_monthly / index_weight

策略:
  - index_basic: 按市场轮询（SSE/SZSE/CSI/SW），分页
  - index_classify: 按层级轮询（L1/L2/L3）
  - index_member_all: 按主要指数逐只拉取（3000 行/页）
  - index_daily: 按交易日轮询，单日约 1.1 万行，分页 8000
  - index_weekly / index_monthly: 按周末/月末交易日轮询，分页 8000
  - index_weight: 按主要指数 × 交易日轮询（仅回补最近 WINDOW_DAYS 个交易日）
  - 全部 ON CONFLICT DO NOTHING，幂等可重跑；列名 change 重命名为 change_val

容错: 与 incremental_factor.py 相同（TushareClient 重试 + 逐行降级 + 单日失败不中断）

用法:
  python scripts/data_collection/incremental_index.py            # 增量
  python scripts/data_collection/incremental_index.py 20160101 20260806  # 指定范围
"""

import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

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
logger = logging.getLogger("inc_index")

DEFAULT_START = "20160101"
SLEEP_BETWEEN = 0.15
RATE_LIMIT = 480
PAGE_LIMIT = 8000
WINDOW_DAYS = 500  # index_weight 回补的最近交易日数

# 主要指数（权重 / 成分拉取对象）
KEY_INDICES = [
    "000001.SH",  # 上证指数
    "000016.SH",  # 上证50
    "000300.SH",  # 沪深300
    "000905.SH",  # 中证500
    "000852.SH",  # 中证1000
    "399001.SZ",  # 深证成指
    "399006.SZ",  # 创业板指
    "000688.SH",  # 科创50
]


def _fmt(d) -> str:
    return d.strftime("%Y%m%d")


def get_trade_dates(conn, since: str, until: str) -> list[str]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT cal_date FROM tushare_trade_cal "
            "WHERE exchange='SSE' AND is_open=1 "
            "AND cal_date BETWEEN %s AND %s ORDER BY cal_date",
            (since, until),
        )
        return [
            row[0].strftime("%Y%m%d") if hasattr(row[0], "strftime") else str(row[0])
            for row in cur.fetchall()
        ]
    finally:
        cur.close()


def get_existing_dates(conn, table: str, col: str) -> set[str]:
    if not bootstrap.table_exists(conn, table):
        return set()
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT DISTINCT {col} FROM {table}")
        return {
            row[0].strftime("%Y%m%d") if hasattr(row[0], "strftime") else str(row[0])
            for row in cur.fetchall()
        }
    finally:
        cur.close()


def group_period_dates(trade_dates: list[str], period: str) -> list[str]:
    """从交易日序列中取每个周期最后一个交易日（week=按ISO周, month=按自然月）。"""
    seen = {}
    for td in trade_dates:
        d = datetime.strptime(td, "%Y%m%d")
        key = d.isocalendar()[:2] if period == "week" else (d.year, d.month)
        seen[key] = td
    return [seen[k] for k in sorted(seen.keys())]


def query_paginated(client: TushareClient, api_name: str, **params):
    rows = []
    offset = 0
    while True:
        df = client.query(api_name, limit=PAGE_LIMIT, offset=offset, **params)
        if df.empty:
            break
        rows.append(df)
        n = len(df)
        offset += n
        if n < PAGE_LIMIT:
            break
        if offset > 200000:
            logger.warning("  %s %s 分页超过 20 万行保护阈值，停止", api_name, params)
            break
    return rows


def rename_change(df, table_name: str):
    if "change" in df.columns and "change_val" not in df.columns:
        df = df.rename(columns={"change": "change_val"})
    return df


def pull_by_dates(
    client: TushareClient, conn,
    api_name: str, table_name: str, date_col: str, conflict: str,
    dates: list[str], rename: bool = False,
) -> int:
    existing = get_existing_dates(conn, table_name, date_col)
    need = [d for d in dates if d not in existing]
    logger.info("--- %s -> %s: 已有 %d 期，待补 %d 期 ---", api_name, table_name, len(existing), len(need))
    if not need:
        return 0

    total = 0
    for i, td in enumerate(need):
        try:
            for df in query_paginated(client, api_name, **{date_col: td}):
                if df.empty:
                    continue
                if rename:
                    df = rename_change(df, table_name)
                total += insert_dataframe(conn, table_name, df, conflict)
        except Exception as exc:
            if (i + 1) % 20 == 0:
                logger.warning("  %s %s 失败: %s", api_name, td, exc)
        if (i + 1) % 50 == 0:
            logger.info("  %s [%d/%d] %s", table_name, i + 1, len(need), f"{total:,}")
        time.sleep(SLEEP_BETWEEN)
    logger.info("  %s 完成: %s 行", table_name, f"{total:,}")
    return total


def pull_index_weight(
    client: TushareClient, conn,
    indices: list[str], trade_dates: list[str],
) -> int:
    table_name = "tushare_index_weight"
    conflict = "(index_code, con_code, trade_date)"
    total = 0
    for idx in indices:
        logger.info("--- index_weight -> %s (%s) ---", table_name, idx)
        for j, td in enumerate(trade_dates):
            try:
                df = client.query("index_weight", index_code=idx, trade_date=td)
                if not df.empty:
                    total += insert_dataframe(conn, table_name, df, conflict)
            except Exception as exc:
                if (j + 1) % 50 == 0:
                    logger.warning("  %s %s %s 失败: %s", idx, td, exc)
            time.sleep(SLEEP_BETWEEN)
        logger.info("  %s 累计: %s 行", idx, f"{total:,}")
    logger.info("  %s 完成: %s 行", table_name, f"{total:,}")
    return total


def pull_member_all(
    client: TushareClient, conn,
    indices: list[str],
) -> int:
    table_name = "tushare_index_member_all"
    conflict = "(ts_code, l1_code, l2_code, l3_code, in_date)"
    total = 0
    for idx in indices:
        try:
            for df in query_paginated(client, "index_member_all", index_code=idx):
                if not df.empty:
                    total += insert_dataframe(conn, table_name, df, conflict)
        except Exception as exc:
            logger.warning("  %s 失败: %s", idx, exc)
        time.sleep(SLEEP_BETWEEN)
    logger.info("  %s 完成: %s 行", table_name, f"{total:,}")
    return total


def pull_by_market(client: TushareClient, conn, api_name: str, table_name: str, conflict: str, markets: list[str]) -> int:
    total = 0
    for mkt in markets:
        try:
            for df in query_paginated(client, api_name, market=mkt):
                if not df.empty:
                    total += insert_dataframe(conn, table_name, df, conflict)
        except Exception as exc:
            logger.warning("  %s %s 失败: %s", api_name, mkt, exc)
        time.sleep(SLEEP_BETWEEN)
    logger.info("  %s 完成: %s 行", table_name, f"{total:,}")
    return total


def pull_classify(client: TushareClient, conn) -> int:
    table_name = "tushare_index_classify"
    conflict = "(index_code, src)"
    total = 0
    for level in ("L1", "L2", "L3"):
        try:
            df = client.query("index_classify", level=level)
            if not df.empty:
                total += insert_dataframe(conn, table_name, df, conflict)
        except Exception as exc:
            logger.warning("  index_classify %s 失败: %s", level, exc)
        time.sleep(SLEEP_BETWEEN)
    logger.info("  %s 完成: %s 行", table_name, f"{total:,}")
    return total


def _print_summary(conn):
    tables = [
        ("tushare_index_basic", "指数基本信息"),
        ("tushare_index_daily", "指数日线"),
        ("tushare_index_weekly", "指数周线"),
        ("tushare_index_monthly", "指数月线"),
        ("tushare_index_weight", "指数成分权重"),
        ("tushare_index_classify", "指数分类"),
        ("tushare_index_member_all", "指数全成分"),
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
        bootstrap.ensure_trade_cal(client, conn)
        bootstrap.ensure_extra_schema(conn)

        _print_summary(conn)

        today = date.today()
        until = end_arg or _fmt(today)
        since = start_arg or DEFAULT_START
        trade_dates = get_trade_dates(conn, since, until)
        logger.info("交易日范围: %s ~ %s (%d 日)", since, until, len(trade_dates))
        if not trade_dates:
            logger.warning("无交易日数据，请先运行 bootstrap/基础增量脚本")
            sys.exit(1)

        grand_total = 0

        # 指数基本信息 / 分类 / 全成分
        grand_total += pull_by_market(client, conn, "index_basic", "tushare_index_basic",
                                      "(ts_code)", ["SSE", "SZSE", "CSI", "SW"])
        grand_total += pull_classify(client, conn)
        grand_total += pull_member_all(client, conn, KEY_INDICES)

        # 日线 / 周线 / 月线
        grand_total += pull_by_dates(client, conn, "index_daily", "tushare_index_daily",
                                     "trade_date", "(ts_code, trade_date)", trade_dates, rename=True)
        week_dates = group_period_dates(trade_dates, "week")
        grand_total += pull_by_dates(client, conn, "index_weekly", "tushare_index_weekly",
                                     "trade_date", "(ts_code, trade_date)", week_dates, rename=True)
        month_dates = group_period_dates(trade_dates, "month")
        grand_total += pull_by_dates(client, conn, "index_monthly", "tushare_index_monthly",
                                     "trade_date", "(ts_code, trade_date)", month_dates, rename=True)

        # 权重: 主要指数 × 月末交易日（指数权重仅在调仓/月末更新）
        month_end_dates = group_period_dates(trade_dates, "month")
        recent = month_end_dates[-WINDOW_DAYS:]
        grand_total += pull_index_weight(client, conn, KEY_INDICES, recent)

        elapsed = time.time() - t0
        logger.info("=== 指数数据刷新完成 耗时 %.0f 秒  新增: %s ===", elapsed, f"{grand_total:,}")
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