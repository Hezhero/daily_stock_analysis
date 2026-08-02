# -*- coding: utf-8 -*-
"""
基础数据增量刷新脚本

每天定时执行，全量刷新以下表（覆盖式更新）：
  - tushare_stock_basic  股票基础信息（新股上市、退市、名称/状态变更）
  - tushare_hs_const     沪深港通成分股（季度调整）
  - tushare_ipo_list      IPO 新股列表

策略:
  - 表数据量小（<10000行），直接全量刷新
  - 事务内先删后插，确保数据与 Tushare 完全一致
  - trade_cal 仅在新一年到来时补充

容错:
  - TushareClient.query() 内置 3 次重试（指数退避 1s/2s）
  - get_pg_connection() 内置 3 次重试（指数退避 1s/2s/4s）
  - 单品 API 失败不中断整体流程

用法: python scripts/data_collection/incremental_base.py
"""

import logging
import os
import sys
import time
from datetime import datetime

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
logger = logging.getLogger("inc_base")


def truncate_table(conn, table_name: str):
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {table_name}")
        conn.commit()
    finally:
        cur.close()


def refresh_table(
    client: TushareClient, conn,
    api_name: str, table_name: str,
    conflict: str, fields: str = "",
    **api_kwargs,
):
    """全量拉取并覆盖一张小表。"""
    logger.info("--- %s -> %s ---", api_name, table_name)
    try:
        df = client.query(api_name, fields=fields, **api_kwargs)
        if df.empty:
            logger.warning("  %s 返回空数据，跳过", api_name)
            return False
        truncate_table(conn, table_name)
        n = insert_dataframe(conn, table_name, df, conflict, batch_size=1000)
        logger.info("  %s: %s 行", table_name, f"{n:,}")
        return True
    except Exception as exc:
        logger.error("  %s 刷新失败: %s", table_name, exc)
        return False


def refresh_stock_basic(client: TushareClient, conn):
    """刷新股票基础信息列表。"""
    return refresh_table(
        client, conn,
        "stock_basic", "tushare_stock_basic", "(ts_code)",
        fields=(
            "ts_code,symbol,name,area,industry,cnspell,market,list_status,"
            "list_date,delist_date,is_hs,act_name,act_ent_type,fullname,"
            "exchange,curr_type,enname"
        ),
        list_status="L",
    )


def refresh_hs_const(client: TushareClient, conn):
    """刷新沪深港通成分股。"""
    logger.info("--- hs_const -> tushare_hs_const ---")
    total = 0
    for hs_type in ["SH", "SZ"]:
        try:
            df = client.query(
                "hs_const", hs_type=hs_type,
                fields="ts_code,hs_type,in_date,out_date,is_new",
            )
            if df.empty:
                continue
            if total == 0:
                truncate_table(conn, "tushare_hs_const")
            n = insert_dataframe(
                conn, "tushare_hs_const", df, "(ts_code, hs_type, in_date)",
            )
            total += n
            logger.info("  hs_const %s: %s 行", hs_type, f"{n:,}")
        except Exception as exc:
            logger.warning("  hs_const %s 失败: %s", hs_type, exc)
        time.sleep(0.3)
    logger.info("  tushare_hs_const 总计: %s", f"{total:,}")
    return total > 0


def refresh_ipo_list(client: TushareClient, conn):
    """刷新 IPO 新股列表。"""
    return refresh_table(
        client, conn,
        "new_share", "tushare_ipo_list", "(ts_code, ipo_date)",
    )


def ensure_trade_cal(client: TushareClient, conn):
    """确保交易日历覆盖到足够新的日期。"""
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(cal_date) FROM tushare_trade_cal WHERE exchange='SSE'")
        row = cur.fetchone()
        max_date = row[0] if row and row[0] else None
    finally:
        cur.close()

    now = datetime.now()
    current_year = now.year
    need_year = current_year if not max_date else max(max_date.year, current_year)
    max_dt = datetime.combine(max_date, datetime.min.time()) if max_date else None

    # 判断是否需要刷新：年份未覆盖 或 最新日期距今超过 3 天
    need_refresh = (
        not max_dt
        or max_dt.year < current_year
        or (now - max_dt).days > 1
    )
    if not need_refresh:
        logger.info("  trade_cal 已覆盖到 %s (距今 %d 天)，跳过", max_date, (now - max_dt).days)
        return

    logger.info("--- trade_cal 补充 %s 年度 ---", need_year)
    end_y = f"{need_year}1231"
    try:
        df = client.query(
            "trade_cal", exchange="SSE",
            start_date=f"{need_year}0101", end_date=end_y,
            fields="exchange,cal_date,is_open,pretrade_date",
        )
        if not df.empty:
            n = insert_dataframe(
                conn, "tushare_trade_cal", df, "(exchange, cal_date)",
            )
            logger.info("  trade_cal %s: %s 行", need_year, f"{n:,}")
    except Exception as exc:
        logger.warning("  trade_cal %s 失败: %s", need_year, exc)


def _print_summary(conn):
    tables = [
        ("tushare_stock_basic", "股票基础信息"),
        ("tushare_trade_cal", "交易日历"),
        ("tushare_hs_const", "沪深港通"),
        ("tushare_ipo_list", "IPO列表"),
    ]
    logger.info("=== 当前数据概况 ===")
    for table, label in tables:
        cnt = row_count(conn, table)
        logger.info("  %s: %s 行", label, f"{cnt:,}" if cnt else "空")


def main():
    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置，请在 .env 中配置")
        sys.exit(1)

    t0 = time.time()
    client = TushareClient(TUSHARE_TOKEN)
    conn = get_pg_connection()

    try:
        # ── 自举：首次运行时自动建表 + 填充基础数据 ──
        bootstrap.ensure_schema(conn)
        bootstrap.ensure_stock_basic(client, conn)
        bootstrap.ensure_trade_cal(client, conn)

        _print_summary(conn)

        refresh_stock_basic(client, conn)
        refresh_hs_const(client, conn)
        refresh_ipo_list(client, conn)
        ensure_trade_cal(client, conn)

        elapsed = time.time() - t0
        logger.info("=== 基础数据刷新完成 耗时 %.0f 秒 ===", elapsed)
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
