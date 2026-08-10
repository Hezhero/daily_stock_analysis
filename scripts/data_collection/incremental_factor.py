# -*- coding: utf-8 -*-
"""
股票衍生 / 情绪 / 博弈类数据增量拉取脚本（tushare 2000 积分）

覆盖 12 张扩展表：
  按交易日/公告日逐日拉取: moneyflow / margin / margin_detail / top_list /
    top_inst / block_trade / stk_limit / repurchase /
    stk_holdertrade / pledge_detail
  按股票逐只拉取: stk_holdernumber / pledge_stat

策略:
  - 按日表: 从 tushare_trade_cal 取交易日，与表内已有日期求差集，断点续传
  - 按股表: 每张表取 MAX(end_date) 前推 LOOKBACK_DAYS 作为起点，逐股拉取
  - 全部使用 ON CONFLICT DO NOTHING，幂等可重跑
  - 不传 fields 参数，以 API 实际返回列与表列自动对齐

容错:
  - TushareClient.query() 内置 3 次重试（指数退避）
  - get_pg_connection() 内置 3 次重试
  - insert_dataframe() 批量失败自动降级为逐行插入
  - 单日/单股失败不中断整体流程

用法:
  python scripts/data_collection/incremental_factor.py            # 增量
  python scripts/data_collection/incremental_factor.py 20160101 20260806  # 指定范围
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

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("inc_factor")

# 拉取范围与频率
DEFAULT_START = "20160101"
LOOKBACK_DAYS = 180
SLEEP_BETWEEN = 0.15
RATE_LIMIT = 480
PAGE_LIMIT = 8000  # 单次 API 返回行数上限（超过需分页）

# 按日 / 按公告日拉取（全市场）
DAILY_APIS = [
    ("moneyflow", "tushare_moneyflow", "trade_date", "(ts_code, trade_date)"),
    ("margin", "tushare_margin", "trade_date", "(trade_date, exchange_id)"),
    ("margin_detail", "tushare_margin_detail", "trade_date", "(ts_code, trade_date)"),
    ("top_list", "tushare_top_list", "trade_date", "(ts_code, trade_date)"),
    ("top_inst", "tushare_top_inst", "trade_date", "(ts_code, trade_date, exalter, side, reason)"),
    ("block_trade", "tushare_block_trade", "trade_date", "(ts_code, trade_date, buyer, seller, price, vol)"),
    ("stk_limit", "tushare_stk_limit", "trade_date", "(ts_code, trade_date)"),
    ("repurchase", "tushare_repurchase", "ann_date", "(ts_code, ann_date, end_date, proc)"),
    ("stk_holdertrade", "tushare_stk_holdertrade", "ann_date", "(ts_code, ann_date, holder_name, in_de)"),
    ("pledge_detail", "tushare_pledge_detail", "ann_date", "(ts_code, ann_date, holder_name, start_date, end_date)"),
]

# 按股票拉取（API 不支持全市场按日）
STOCK_APIS = [
    ("stk_holdernumber", "tushare_stk_holdernumber", "(ts_code, end_date, ann_date)"),
    ("pledge_stat", "tushare_pledge_stat", "(ts_code, end_date)"),
]

STOCK_BATCH = 1  # 该类接口不支持批量 ts_code


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


def get_codes(conn) -> list[str]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT ts_code FROM tushare_stock_basic WHERE list_status='L' ORDER BY ts_code"
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def get_max_end_date(conn, table: str) -> date | None:
    if not bootstrap.table_exists(conn, table):
        return None
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT MAX(end_date) FROM {table}")
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    finally:
        cur.close()


def query_paginated(client: TushareClient, api_name: str, **params):
    """分页拉取单日/单股数据，API 单次返回上限约 8000 行。"""
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


def pull_daily_api(
    client: TushareClient, conn,
    api_name: str, table_name: str, date_col: str, conflict: str,
    trade_dates: list[str],
) -> int:
    existing = get_existing_dates(conn, table_name, date_col)
    need = [d for d in trade_dates if d not in existing]
    logger.info("--- %s -> %s: 已有 %d 日，待补 %d 日 ---", api_name, table_name, len(existing), len(need))
    if not need:
        return 0

    total = 0
    for i, td in enumerate(need):
        try:
            for df in query_paginated(client, api_name, **{date_col: td}):
                if not df.empty:
                    total += insert_dataframe(conn, table_name, df, conflict)
        except Exception as exc:
            if (i + 1) % 20 == 0:
                logger.warning("  %s %s 失败: %s", api_name, td, exc)
        if (i + 1) % 50 == 0:
            logger.info("  %s [%d/%d] %s", table_name, i + 1, len(need), f"{total:,}")
        time.sleep(SLEEP_BETWEEN)
    logger.info("  %s 完成: %s 行", table_name, f"{total:,}")
    return total


def pull_stock_api(
    client: TushareClient, conn,
    api_name: str, table_name: str, conflict: str,
    codes: list[str], start: str, end: str,
) -> int:
    logger.info("--- %s -> %s [%s ~ %s] ---", api_name, table_name, start, end)
    total = 0
    for i, code in enumerate(codes):
        try:
            df = client.query(api_name, ts_code=code, start_date=start, end_date=end)
            if not df.empty:
                total += insert_dataframe(conn, table_name, df, conflict)
        except Exception as exc:
            if (i + 1) % 500 == 0:
                logger.warning("  %s %s 失败: %s", api_name, code, exc)
        if (i + 1) % 1000 == 0:
            logger.info("  %s [%d/%d] %s", table_name, i + 1, len(codes), f"{total:,}")
        time.sleep(SLEEP_BETWEEN)
    logger.info("  %s 完成: %s 行", table_name, f"{total:,}")
    return total


def _print_summary(conn):
    tables = [
        ("tushare_moneyflow", "资金流向"),
        ("tushare_margin", "融资融券汇总"),
        ("tushare_margin_detail", "融资融券明细"),
        ("tushare_stk_holdernumber", "股东人数"),
        ("tushare_top_list", "龙虎榜明细"),
        ("tushare_top_inst", "龙虎榜机构"),
        ("tushare_pledge_detail", "股权质押明细"),
        ("tushare_pledge_stat", "股权质押统计"),
        ("tushare_repurchase", "股票回购"),
        ("tushare_block_trade", "大宗交易"),
        ("tushare_stk_holdertrade", "股东增减持"),
        ("tushare_stk_limit", "涨跌停价格"),
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
        bootstrap.ensure_stock_basic(client, conn)
        bootstrap.ensure_trade_cal(client, conn)
        bootstrap.ensure_extra_schema(conn)

        _print_summary(conn)

        today = date.today()
        until = end_arg or _fmt(today)
        default_start = _fmt(today - timedelta(days=LOOKBACK_DAYS))

        # ── 按日表 ──
        if start_arg:
            since = start_arg
        else:
            # 增量模式: 从各表已有最晚日期起算；空表回退默认起点
            since = DEFAULT_START
        trade_dates = get_trade_dates(conn, since, until)
        logger.info("交易日范围: %s ~ %s (%d 日)", since, until, len(trade_dates))

        grand_total = 0
        for api_name, table_name, date_col, conflict in DAILY_APIS:
            n = pull_daily_api(client, conn, api_name, table_name, date_col, conflict, trade_dates)
            grand_total += n

        # ── 按股表 ──
        codes = get_codes(conn)
        if not codes:
            logger.error("无上市股票代码，请先运行基础数据导入")
            sys.exit(1)
        logger.info("上市股票: %d 只", len(codes))

        for api_name, table_name, conflict in STOCK_APIS:
            max_d = get_max_end_date(conn, table_name)
            if max_d and not start_arg:
                start = _fmt(max(max_d - timedelta(days=LOOKBACK_DAYS), date(2015, 1, 1)))
                start = min(start, _fmt(today - timedelta(days=LOOKBACK_DAYS)))
            else:
                start = start_arg or DEFAULT_START
            if start >= until:
                logger.info("  %s 无需更新，跳过", table_name)
                continue
            n = pull_stock_api(client, conn, api_name, table_name, conflict, codes, start, until)
            grand_total += n

        elapsed = time.time() - t0
        logger.info("=== 股票衍生数据刷新完成 耗时 %.0f 秒  新增: %s ===", elapsed, f"{grand_total:,}")
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