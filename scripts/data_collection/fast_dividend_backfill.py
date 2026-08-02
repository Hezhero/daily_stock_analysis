# -*- coding: utf-8 -*-
"""
dividend 快速回填：按年分页拉取全市场（替代逐股拉取，提速 ~5500 倍）

- Tushare dividend 接口支持 start_date/end_date 全市场查询，单次最多 2000 行
- 按年 + offset 分页拉全量，ON CONFLICT DO NOTHING 幂等
- 2015~2026 共 12 年，每年约 8~10 次调用，总计 ~100 次调用，几分钟完成
"""
import logging
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tushare_pg_utils import TushareClient, get_pg_connection, insert_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fast_dividend")

FIELDS = (
    "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
    "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,"
    "imp_ann_date,base_date,base_share"
)
CONFLICT = "(ts_code, end_date, ann_date, div_proc)"
PAGE = 2000


def fetch_year(client, start: str, end: str) -> list:
    """分页拉取一年全市场 dividend。"""
    rows = []
    offset = 0
    while True:
        df = client.query(
            "dividend", fields=FIELDS,
            start_date=start, end_date=end,
            offset=offset,
        )
        n = len(df)
        if n == 0:
            break
        rows.append(df)
        offset += n
        if n < PAGE:
            break
        # 保险：防止死循环
        if offset > 200000:
            logger.warning("[%s~%s] 超过 20 万行保护阈值，停止", start, end)
            break
    return rows


def main():
    start_year = int(sys.argv[1]) if len(sys.argv) > 1 else 2015
    end_year = int(sys.argv[2]) if len(sys.argv) > 2 else 2026
    rate = int(sys.argv[3]) if len(sys.argv) > 3 else 55

    client = TushareClient(os.getenv("TUSHARE_TOKEN", ""), rate_limit=rate)
    conn = get_pg_connection()

    t0 = time.time()
    grand_total = 0
    for year in range(start_year, end_year + 1):
        start = f"{year}0101"
        end = f"{year}1231"
        try:
            pages = fetch_year(client, start, end)
            y_total = 0
            for df in pages:
                n = insert_dataframe(conn, "tushare_dividend", df, CONFLICT)
                y_total += n
            grand_total += y_total
            logger.info("[%d] %s~%s: %d 行（累计 %d）", year, start, end, y_total, grand_total)
        except Exception as exc:
            logger.error("[%d] 失败: %s", year, exc)

    conn.close()
    logger.info("完成! 新增 %d 行，总耗时 %.1f 分钟", grand_total, (time.time() - t0) / 60)


if __name__ == "__main__":
    main()
