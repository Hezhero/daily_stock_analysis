# -*- coding: utf-8 -*-
"""
基础数据增量刷新脚本

每天定时执行，全量刷新以下表（覆盖式更新）：
  - tushare_stock_basic  股票基础信息（上市 L / 退市 D / 暂停 P，名称/状态变更）
  - tushare_ipo_list      IPO 新股列表

策略:
  - 表数据量小（<10000行），直接全量刷新
  - stock_basic 合并 list_status=L/D/P 后，事务内先删后插，确保数据与
    Tushare 完全一致；退市股（D）因此能持久化，不会被下一次日常刷新抹掉
  - 单个状态返回空时可容忍（P 通常为 0 行）；但若该状态库内已有数据而
    API 返回空，则放弃本次刷新，避免静默丢失已有 D 行
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

import pandas as pd

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


STOCK_BASIC_STATUSES = ("L", "D", "P")
STOCK_BASIC_FIELDS = (
    "ts_code,symbol,name,area,industry,cnspell,market,list_status,"
    "list_date,delist_date,is_hs,act_name,act_ent_type,fullname,"
    "exchange,curr_type,enname"
)


def _existing_status_counts(conn) -> dict[str, int]:
    """库内各 list_status 的行数，用于判断空返回是否异常。"""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT list_status, COUNT(*) FROM tushare_stock_basic "
            "GROUP BY list_status"
        )
        return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        cur.close()


def _drop_oversized_symbols(conn, df):
    """丢弃 symbol 超出表定义长度的行，并显式告警。

    Tushare 个别退市代码的 symbol 为 7 位（如 T600018.SH），而库内
    symbol 为 varchar(6)。在不改表结构的前提下无法写入，只能跳过；
    这里提前过滤并打印代码，避免整批插入失败后退化为逐行插入。
    """
    if df.empty or "symbol" not in df.columns:
        return df
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT character_maximum_length FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='tushare_stock_basic' "
            "AND column_name='symbol'"
        )
        row = cur.fetchone()
    finally:
        cur.close()
    if not row or not row[0]:
        return df

    limit = int(row[0])
    too_long = df["symbol"].astype(str).str.len() > limit
    if too_long.any():
        logger.warning(
            "  跳过 %d 行 symbol 超过 varchar(%d) 的记录，无法写入: %s",
            int(too_long.sum()), limit,
            ", ".join(df.loc[too_long, "ts_code"].tolist()),
        )
        return df.loc[~too_long]
    return df


def refresh_stock_basic(client: TushareClient, conn):
    """全量刷新股票基础信息，合并 L/D/P 三种状态。

    退市股（D）与暂停上市（P）必须一并落库，否则 PIT 回测无法构造历史宇宙。
    P 通常返回 0 行，按空处理；若某状态库内已有数据而 API 返回空，视为拉取
    失败并放弃本次刷新，避免静态空返回把已有 D 行删掉。
    """
    logger.info("--- stock_basic -> tushare_stock_basic (L/D/P) ---")
    existing = _existing_status_counts(conn)
    frames = []
    for status in STOCK_BASIC_STATUSES:
        try:
            df = client.query(
                "stock_basic", fields=STOCK_BASIC_FIELDS, list_status=status,
            )
        except Exception as exc:
            logger.error("  stock_basic list_status=%s 拉取失败: %s", status, exc)
            return False

        if df is None or df.empty:
            if existing.get(status, 0) > 0:
                logger.error(
                    "  stock_basic list_status=%s 返回空，但库内已有 %d 行；"
                    "放弃本次刷新以保留已有数据",
                    status, existing[status],
                )
                return False
            logger.info("  stock_basic list_status=%s 返回空，跳过", status)
            continue

        logger.info("  stock_basic list_status=%s: %s 行", status, f"{len(df):,}")
        frames.append(df)

    if not frames:
        logger.error("stock_basic 所有状态均返回空，保留现有数据不刷新")
        return False

    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["ts_code"], keep="first")
    df = _drop_oversized_symbols(conn, df)

    try:
        truncate_table(conn, "tushare_stock_basic")
        n = insert_dataframe(conn, "tushare_stock_basic", df, "(ts_code)", batch_size=1000)
        logger.info("  tushare_stock_basic: %s 行", f"{n:,}")
        return True
    except Exception as exc:
        logger.error("  tushare_stock_basic 刷新失败: %s", exc)
        return False


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
    days_since = (now - max_dt).days if max_dt else None
    need_refresh = (
        max_dt is None
        or max_dt.year < current_year
        or (days_since is not None and days_since > 1)
    )
    if max_dt is not None and not need_refresh:
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
