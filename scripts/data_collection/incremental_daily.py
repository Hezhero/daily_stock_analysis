# -*- coding: utf-8 -*-
"""
日线行情增量拉取脚本

每天定时执行，自动补全 tushare_daily、tushare_adj_factor、tushare_daily_basic
三张表中缺失的交易日数据。

策略:
  - 默认（夜间增量）: daily_basic 按 trade_date 逐日全市场拉取（API 支持不传
    ts_code）；daily / adj_factor 按上市股票批次 + 日期范围拉取
  - 退市股回填（opt-in，一次性）: 通过 --codes 或 --include-delisted 启用，
    daily / adj_factor / daily_basic 全部按 ts_code 逐只拉取，避免长区间
    批量请求超过 API 单次返回上限；默认夜间行为完全不变

容错:
  - TushareClient.query() 内置 3 次重试（指数退避 1s/2s）
  - get_pg_connection() 内置 3 次重试（指数退避 1s/2s/4s）
  - insert_dataframe() 批量失败时自动降级为逐行插入
  - 单品/单日 API 失败不中断整体流程

用法:
  python scripts/data_collection/incremental_daily.py        # 夜间增量（L-only）
  python scripts/data_collection/incremental_daily.py --include-delisted
  python scripts/data_collection/incremental_daily.py --codes 605081.SH,603963.SH \\
      --start-date 20200101 --end-date 20261016
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd

from tushare_pg_utils import (
    TushareClient,
    get_pg_connection,
    insert_dataframe,
    normalize_ts_code,
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
logger = logging.getLogger("inc_daily")


def _fmt(d: date) -> str:
    return d.strftime("%Y%m%d")


DAILY_FIELDS = (
    "ts_code,trade_date,open,high,low,close,pre_close,change,"
    "pct_chg,vol,amount"
)
ADJ_FACTOR_FIELDS = "ts_code,trade_date,adj_factor"
DAILY_BASIC_FIELDS = (
    "ts_code,trade_date,close,turnover_rate,turnover_rate_f,"
    "volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,"
    "total_share,float_share,free_share,total_mv,circ_mv"
)

STOCK_BATCH = 200
SLEEP_BETWEEN = 0.15

# 退市股一次性回填：每日线约 1400 行，逐只请求可稳定低于 API 单次返回上限；
# 默认夜间批量（200 只/批）只覆盖很短的增量窗口，不受此限制。
BACKFILL_BATCH = 1
BACKFILL_DEFAULT_START = "20200101"

MARKET_APIS = [
    ("daily", "tushare_daily", DAILY_FIELDS, "(ts_code, trade_date)"),
    ("adj_factor", "tushare_adj_factor", ADJ_FACTOR_FIELDS, "(ts_code, trade_date)"),
]
BACKFILL_APIS = MARKET_APIS + [
    ("daily_basic", "tushare_daily_basic", DAILY_BASIC_FIELDS, "(ts_code, trade_date)"),
]


def get_codes(conn, include_delisted: bool = False) -> list[str]:
    where = "list_status IN ('L','D')" if include_delisted else "list_status='L'"
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT ts_code FROM tushare_stock_basic WHERE {where} ORDER BY ts_code")
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def get_latest_date(conn, table: str, col: str) -> date | None:
    """获取表中已有数据的最新日期。"""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT MAX({col}) FROM {table}")
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    finally:
        cur.close()


def get_trade_dates(conn, since: str, until: str) -> list[str]:
    """从 trade_cal 获取指定范围内的交易日（正序）。"""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT cal_date FROM tushare_trade_cal "
            "WHERE exchange='SSE' AND is_open=1 "
            "AND cal_date BETWEEN %s AND %s "
            "ORDER BY cal_date ASC",
            (since, until),
        )
        return [
            row[0].strftime("%Y%m%d") if hasattr(row[0], "strftime") else str(row[0])
            for row in cur.fetchall()
        ]
    except Exception:
        return []
    finally:
        cur.close()


def pull_daily_basic(client: TushareClient, conn, trade_dates: list[str]):
    """逐日拉取 daily_basic（API 支持按日全市场返回）。"""
    logger.info("--- daily_basic: %d 个待补日期 ---", len(trade_dates))
    if not trade_dates:
        return
    total = 0
    for i, td in enumerate(trade_dates):
        try:
            df = client.query("daily_basic", trade_date=td, fields=DAILY_BASIC_FIELDS)
            if not df.empty:
                n = insert_dataframe(
                    conn, "tushare_daily_basic", df,
                    conflict_clause="(ts_code, trade_date)",
                )
                total += n
        except Exception as exc:
            if (i + 1) % 30 == 0:
                logger.warning("  daily_basic %s 失败: %s", td, exc)
        if (i + 1) % 50 == 0:
            logger.info("  daily_basic [%d/%d] total=%s", i + 1, len(trade_dates), f"{total:,}")
    logger.info("  daily_basic 完成: %s", f"{total:,}")


def pull_market_by_code(
    client: TushareClient, conn,
    codes: list[str],
    trade_dates: list[str],
    apis: list[tuple[str, str, str, str]],
    batch_size: int = STOCK_BATCH,
):
    """按 ts_code 批次 + 日期范围拉取行情类接口并幂等入库。"""
    if not trade_dates or not codes:
        return

    start_d = trade_dates[0]
    end_d = trade_dates[-1]
    names = [api[0] for api in apis]
    logger.info(
        "--- %s: %s ~ %s (%d 日, %d 只) ---",
        " + ".join(names), start_d, end_d, len(trade_dates), len(codes),
    )

    totals: dict[str, int] = {name: 0 for name in names}
    fails: dict[str, int] = {name: 0 for name in names}
    batches = max(1, (len(codes) + batch_size - 1) // batch_size)

    for bi, i in enumerate(range(0, len(codes), batch_size), start=1):
        batch = codes[i:i + batch_size]
        cs = ",".join(batch)
        for api_name, table_name, fields, conflict in apis:
            try:
                df = client.query(
                    api_name,
                    ts_code=cs,
                    start_date=start_d,
                    end_date=end_d,
                    fields=fields,
                )
                if not df.empty:
                    if "change" in df.columns:
                        df.rename(columns={"change": "change_val"}, inplace=True)
                    n = insert_dataframe(conn, table_name, df, conflict)
                    totals[api_name] += n
            except Exception as exc:
                fails[api_name] += 1
                if fails[api_name] <= 1 or bi % 10 == 0:
                    logger.warning("  %s b%d 失败: %s", api_name, i, exc)
            time.sleep(SLEEP_BETWEEN)

        if bi % 20 == 0 or bi == batches:
            logger.info(
                "  batch [%d/%d] %s",
                bi, batches,
                "  ".join(f"{name}={totals[name]:,}" for name in names),
            )

    logger.info(
        "  %s  失败批次: %s",
        "  ".join(f"{name}: {totals[name]:,}" for name in names),
        "  ".join(f"{name}={fails[name]}" for name in names),
    )


def pull_daily_and_adj(
    client: TushareClient, conn,
    codes: list[str],
    trade_dates: list[str],
):
    """默认夜间路径：按股票批次拉取 daily 和 adj_factor。"""
    pull_market_by_code(
        client, conn, codes, trade_dates, MARKET_APIS, batch_size=STOCK_BATCH,
    )


def _print_summary(conn):
    tables = [
        ("tushare_daily", "日线行情", "trade_date"),
        ("tushare_adj_factor", "复权因子", "trade_date"),
        ("tushare_daily_basic", "每日指标", "trade_date"),
    ]
    logger.info("=== 当前数据概况 ===")
    for table, label, dc in tables:
        cnt = row_count(conn, table)
        last_date = get_latest_date(conn, table, dc)
        logger.info("  %s: %s 行  最新: %s", label, f"{cnt:,}", last_date or "无")


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Tushare 日线/复权/每日指标增量采集（支持退市股一次性回填）",
    )
    parser.add_argument(
        "--codes", default="",
        help="逗号分隔的 ts_code 或 6 位代码；指定后只回填这些股票",
    )
    parser.add_argument(
        "--include-delisted", action="store_true",
        help="把 list_status='D' 的退市股纳入本次回填（opt-in，默认不启用）",
    )
    parser.add_argument(
        "--start-date", default="",
        help=f"回填起始日 YYYYMMDD（默认 {BACKFILL_DEFAULT_START}）",
    )
    parser.add_argument(
        "--end-date", default="",
        help="回填结束日 YYYYMMDD（默认今天，且不超过 2026-12-31）",
    )
    return parser.parse_args(argv)


def main():
    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置，请在 .env 中配置")
        sys.exit(1)

    args = _parse_args()
    explicit_codes = [
        code for code in (normalize_ts_code(x) for x in args.codes.split(",")) if code
    ]
    code_mode = bool(explicit_codes) or args.include_delisted

    t0 = time.time()
    client = TushareClient(TUSHARE_TOKEN)
    conn = get_pg_connection()

    try:
        # ── 自举：首次运行时自动建表 + 填充基础数据 ──
        bootstrap.ensure_schema(conn)
        bootstrap.ensure_stock_basic(client, conn)
        bootstrap.ensure_trade_cal(client, conn)
        if not code_mode:
            bootstrap.ensure_market_data(client, conn)

        _print_summary(conn)

        # ── 退市股一次性回填（opt-in，不进入夜间默认链路） ──
        if code_mode:
            codes = explicit_codes or get_codes(conn, include_delisted=True)
            if not codes:
                logger.error("没有可回填的股票代码")
                sys.exit(1)
            start = args.start_date or BACKFILL_DEFAULT_START
            until = args.end_date or _fmt(min(date.today(), date(2026, 12, 31)))
            trade_dates = get_trade_dates(conn, start, until)
            if not trade_dates:
                logger.error("回填区间 %s~%s 没有交易日，请检查 trade_cal", start, until)
                sys.exit(1)
            logger.info(
                "退市股回填模式: %d 只 (%s ~ %s)，股票池=%s",
                len(codes), start, until,
                "显式代码" if explicit_codes else "L+D",
            )
            pull_market_by_code(
                client, conn, codes, trade_dates, BACKFILL_APIS,
                batch_size=BACKFILL_BATCH,
            )
            logger.info("=== 回填完成 耗时 %.0f 秒 ===", time.time() - t0)
            _print_summary(conn)
            return

        # ── 确定日期范围 ──
        today = date.today()
        last_daily = get_latest_date(conn, "tushare_daily", "trade_date")
        last_adj = get_latest_date(conn, "tushare_adj_factor", "trade_date")
        last_basic = get_latest_date(conn, "tushare_daily_basic", "trade_date")

        # daily / adj_factor: 起点取两表中较旧日期的下一天，
        # 避免一张表进度领先时另一张表的缺口被永久跳过
        da_start = "20100101"
        if last_daily and last_adj:
            da_start = _fmt(min(last_daily, last_adj) + timedelta(days=1))
        elif last_daily:
            da_start = _fmt(last_daily + timedelta(days=1))
        elif last_adj:
            da_start = _fmt(last_adj + timedelta(days=1))

        # daily_basic: 同样逻辑
        db_start = "20100101"
        if last_basic:
            db_start = _fmt(last_basic + timedelta(days=1))

        until = _fmt(min(today, date(2026, 12, 31)))
        logger.info("今日: %s  拉取范围: daily/adj=%s~%s  basic=%s~%s", today, da_start, until, db_start, until)

        # ── 交易日列表 ──
        da_trade_dates = get_trade_dates(conn, da_start, until)
        db_trade_dates = get_trade_dates(conn, db_start, until)
        logger.info(
            "待补: daily/adj=%d 日  basic=%d 日",
            len(da_trade_dates), len(db_trade_dates),
        )

        need_da = da_trade_dates
        need_db = db_trade_dates

        if not need_da and not need_db:
            logger.info("无需更新")
            return

        # ── 获取股票代码 ──
        codes = get_codes(conn)
        if not codes:
            logger.error("无上市股票代码，请先运行 ts2pg.py P2")
            sys.exit(1)
        logger.info("上市股票: %d 只", len(codes))

        # ── 拉取 daily_basic ──
        if need_db:
            pull_daily_basic(client, conn, need_db)

        # ── 拉取 daily + adj_factor ──
        if need_da:
            pull_daily_and_adj(client, conn, codes, need_da)

        # ── 最终统计 ──
        elapsed = time.time() - t0
        logger.info("=== 更新完成 耗时 %.0f 秒 ===", elapsed)
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
