# -*- coding: utf-8 -*-
"""
dividend / disclosure_date 定向回填脚本（per-code ts_code-only）

根因: Tushare dividend/disclosure_date 接口不接受逗号批量 ts_code 与 end_date 参数
（传入会返回空），incremental_fin.py 原有的批量拉取对这两张表恒为空。
本脚本按股票逐只以 ts_code-only 拉取全量历史，ON CONFLICT DO NOTHING 幂等。

用法: python scripts/data_collection/gapfill_fin_dates.py
"""
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
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
logger = logging.getLogger("gapfill_fin_dates")

THROTTLE = 0.35

FIELDS_MAP = {
    "dividend": (
        "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
        "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,"
        "imp_ann_date,base_date,base_share",
        "(ts_code, end_date, ann_date, div_proc)",
    ),
    "disclosure_date": (
        "ts_code,ann_date,end_date,pre_date,actual_date,modify_date",
        "(ts_code, end_date, pre_date)",
    ),
}


def main():
    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置，请在 .env 中配置")
        sys.exit(1)

    client = TushareClient(TUSHARE_TOKEN, rate_limit=360)
    conn = get_pg_connection()

    try:
        bootstrap.ensure_schema(conn)

        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT ts_code FROM tushare_stock_basic "
                "WHERE list_status='L' ORDER BY ts_code"
            )
            codes = [row[0] for row in cur.fetchall()]
        finally:
            cur.close()

        if not codes:
            logger.error("无上市股票代码，请先运行基础数据导入")
            sys.exit(1)
        logger.info("上市股票: %d 只", len(codes))

        for api_name, table_name in [
            ("dividend", "tushare_dividend"),
            ("disclosure_date", "tushare_disclosure_date"),
        ]:
            fields, conflict = FIELDS_MAP[api_name]
            before = row_count(conn, table_name)

            cur = conn.cursor()
            try:
                cur.execute(
                    f"SELECT DISTINCT ts_code FROM {table_name}"
                )
                covered = {row[0] for row in cur.fetchall()}
            finally:
                cur.close()
            todo = [c for c in codes if c not in covered]
            logger.info(
                "--- %s -> %s (当前 %s 行, 已覆盖 %d/%d, 待拉 %d) ---",
                api_name, table_name, f"{before:,}", len(covered), len(codes), len(todo),
            )
            if not todo:
                logger.info("  全部覆盖，跳过")
                continue

            total = 0
            t0 = time.time()
            for i, code in enumerate(todo):
                try:
                    df = client.query(api_name, ts_code=code, fields=fields)
                    if not df.empty:
                        total += insert_dataframe(conn, table_name, df, conflict)
                except Exception as exc:
                    if (i + 1) % 500 == 0:
                        logger.warning("  %s %s 失败: %s", api_name, code, exc)
                time.sleep(THROTTLE)

                if (i + 1) % 1000 == 0:
                    elapsed = time.time() - t0
                    rate = (i + 1) / elapsed if elapsed > 0 else 0
                    eta = (len(todo) - i - 1) / rate / 60 if rate > 0 else 0
                    logger.info(
                        "  %s [%d/%d] %s 行, 速率 %.1f 股/s, ETA %.0f 分钟",
                        table_name, i + 1, len(todo), f"{total:,}", rate, eta,
                    )

            after = row_count(conn, table_name)
            logger.info(
                "  %s 完成: 新增 %s 行, 总计 %s -> %s",
                table_name, f"{total:,}", f"{before:,}", f"{after:,}",
            )
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