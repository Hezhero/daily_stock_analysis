# -*- coding: utf-8 -*-
"""
分红数据增量拉取脚本

每周定时执行，补全当年及上一年的分红送股数据。

与 fast_dividend_backfill.py 的区别：
  - fast_dividend_backfill: 全量回填 2015~2026，一次性使用
  - incremental_dividend: 只拉当年+前一年，用于定期增量刷新

策略:
  - 按年分页拉取全市场（Tushare dividend 接口支持 start_date/end_date 全市场查询）
  - 单次最多 2000 行，按 offset 分页
  - ON CONFLICT DO NOTHING 幂等

容错:
  - TushareClient.query() 内置 3 次重试（指数退避 1s/2s）
  - get_pg_connection() 内置 3 次重试（指数退避 1s/2s/4s）

用法:
  python scripts/data_collection/incremental_dividend.py              # 默认当年+前一年
  python scripts/data_collection/incremental_dividend.py 2025 2026    # 指定年份范围
"""

import logging
import os
import sys
import time
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tushare_pg_utils import TushareClient, get_pg_connection, insert_dataframe, row_count, TUSHARE_TOKEN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("inc_dividend")

FIELDS = (
    "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
    "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,"
    "imp_ann_date,base_date,base_share"
)
CONFLICT = "(ts_code, end_date, ann_date, div_proc)"
PAGE = 2000
MAX_ROWS_SAFETY = 200000


def fetch_year(client: TushareClient, year_start: str, year_end: str) -> list:
    """分页拉取指定日期范围全市场 dividend。"""
    rows = []
    offset = 0
    while True:
        df = client.query(
            "dividend", fields=FIELDS,
            start_date=year_start, end_date=year_end,
            offset=offset,
        )
        n = len(df)
        if n == 0:
            break
        rows.append(df)
        offset += n
        if n < PAGE:
            break
        if offset > MAX_ROWS_SAFETY:
            logger.warning("[%s~%s] 超过 %d 行保护阈值，停止", year_start, year_end, MAX_ROWS_SAFETY)
            break
    return rows


def main():
    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置，请在 .env 中配置")
        sys.exit(1)

    t0 = time.time()

    today = date.today()
    start_year = int(sys.argv[1]) if len(sys.argv) > 1 else today.year - 1
    end_year = int(sys.argv[2]) if len(sys.argv) > 2 else today.year

    if start_year > end_year:
        logger.error("起始年份 %d 不能大于结束年份 %d", start_year, end_year)
        sys.exit(1)

    client = TushareClient(TUSHARE_TOKEN, rate_limit=55)
    conn = get_pg_connection()

    try:
        before = row_count(conn, "tushare_dividend")
        logger.info("分红表当前: %s 行  拉取范围: %d~%d 年", f"{before:,}" if before else "空", start_year, end_year)

        grand_total = 0
        for year in range(start_year, end_year + 1):
            year_start = f"{year}0101"
            year_end = f"{year}1231"
            try:
                pages = fetch_year(client, year_start, year_end)
                y_total = 0
                for df in pages:
                    n = insert_dataframe(conn, "tushare_dividend", df, CONFLICT)
                    y_total += n
                grand_total += y_total
                logger.info("  [%d] %s~%s: %d 行（累计 %d）", year, year_start, year_end, y_total, grand_total)
            except Exception as exc:
                logger.warning("  [%d] 失败: %s", year, exc)

        elapsed = time.time() - t0
        after = row_count(conn, "tushare_dividend")
        logger.info("=== 分红刷新完成 耗时 %.0f 秒  新增: %s  总计: %s ===",
                     elapsed, f"{grand_total:,}", f"{after:,}")

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
