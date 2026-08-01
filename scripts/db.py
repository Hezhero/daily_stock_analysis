# -*- coding: utf-8 -*-
"""
daily_basic 优化拉取脚本
策略: 按 trade_date 逐日全市场拉取（不传 ts_code）
      Tushare daily_basic 不支持批量 ts_code，但支持按日全市场返回（单日约5000条）
      10年约2500个交易日 × 1 API调用 = ~2500次，在 480/min 速率下约5分钟完成

用法: python scripts/db.py
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd

from scripts.tushare_pg_utils import (
    TushareClient,
    get_pg_connection,
    insert_dataframe,
    row_count,
    TUSHARE_TOKEN,
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("db")

FIELDS = (
    "ts_code,trade_date,close,turnover_rate,turnover_rate_f,"
    "volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,"
    "total_share,float_share,free_share,total_mv,circ_mv"
)


def get_trade_dates(conn):
    """从已缓存的 trade_cal 获取所有交易日（倒序）"""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT cal_date FROM tushare_trade_cal "
            "WHERE exchange='SSE' AND is_open=1 "
            "AND cal_date BETWEEN '2016-07-01' AND '2026-07-29' "
            "ORDER BY cal_date DESC"
        )
        dates = [
            row[0].strftime("%Y%m%d") if hasattr(row[0], "strftime") else str(row[0])
            for row in cur.fetchall()
        ]
        return dates
    finally:
        cur.close()


def main():
    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置，请在 .env 中配置")
        sys.exit(1)

    client = TushareClient(TUSHARE_TOKEN)
    conn = get_pg_connection()

    try:
        existing = row_count(conn, "tushare_daily_basic")
        start = "20160701"

        if existing > 0:
            logger.info("daily_basic 已有 %s 行，获取已有日期范围...", f"{existing:,}")
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT MIN(trade_date), MAX(trade_date) FROM tushare_daily_basic"
                )
                min_date, max_date = cur.fetchone()
                logger.info("  已有范围: %s ~ %s", min_date, max_date)
                if max_date:
                    start = (max_date + timedelta(days=1)).strftime("%Y%m%d")
            finally:
                cur.close()

        trade_dates = get_trade_dates(conn)
        logger.info("交易日总数: %d", len(trade_dates))

        if existing > 0:
            trade_dates = [d for d in trade_dates if d >= start]
            logger.info("待补日期: %d", len(trade_dates))

        if not trade_dates:
            logger.info("无缺失日期，跳过")
            return

        total = 0
        for i, trade_date in enumerate(trade_dates):
            if (i + 1) % 100 == 0:
                logger.info(
                    "  [%d/%d] %s total=%s", i + 1, len(trade_dates), trade_date, f"{total:,}"
                )

            try:
                df = client.query("daily_basic", trade_date=trade_date, fields=FIELDS)
                if not df.empty:
                    n = insert_dataframe(
                        conn,
                        "tushare_daily_basic",
                        df,
                        conflict_clause="(ts_code, trade_date)",
                    )
                    total += n
            except Exception as exc:
                if (i + 1) % 50 == 0:
                    logger.warning("  %s 失败: %s", trade_date, exc)

        # 最终统计
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM tushare_daily_basic"
            )
            cnt, min_d, max_d = cur.fetchone()
            logger.info("完成: %s 行 [%s ~ %s]", f"{cnt:,}", min_d, max_d)
        finally:
            cur.close()

        logger.info("done!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
