# -*- coding: utf-8 -*-
"""
定时任务自举模块 —— 首次运行时自动建表 + 填充基础数据，避免"表不存在"报错。

职责:
  1. ensure_schema()         — 执行 tushare_postgres_schema.sql 建表（幂等）
  2. ensure_stock_basic()    — 若 stock_basic 表为空则全量拉取
  3. ensure_trade_cal()      — 若 trade_cal 缺失年份则补充
  4. ensure_market_data()    — 若行情三表中任一为空，按交易日全市场回填（~20 分钟）
  5. ensure_financial_data() — 若财务表为空，单股逐只回填（慢，依赖 timeout 续传）

调用方（各 incremental_*.py）按需调用：
  - incremental_base.py   → ensure_schema + stock_basic / trade_cal / hs_const 已含刷新逻辑
  - incremental_daily.py  → ensure_schema + stock_basic + trade_cal + (空表时 ensure_market_data)
  - incremental_fin.py    → ensure_schema + stock_basic + (空表时 ensure_financial_data)
  - incremental_dividend.py → ensure_schema
"""

import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

from tushare_pg_utils import (
    TushareClient,
    execute_sql_file,
    get_pg_connection,
    insert_dataframe,
    row_count,
    table_exists,
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMA_FILE = os.path.join(PROJECT_ROOT, "docs", "tushare_postgres_schema.sql")

logger = logging.getLogger("bootstrap")

MARKET_START = "20160101"
MARKET_BACKFILL_LIMIT = 4000

MARKET_APIS = [
    (
        "daily", "tushare_daily",
        "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        "(ts_code, trade_date)",
        True,
    ),
    (
        "adj_factor", "tushare_adj_factor",
        "ts_code,trade_date,adj_factor",
        "(ts_code, trade_date)",
        False,
    ),
    (
        "daily_basic", "tushare_daily_basic",
        "ts_code,trade_date,close,turnover_rate,turnover_rate_f,"
        "volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,"
        "total_share,float_share,free_share,total_mv,circ_mv",
        "(ts_code, trade_date)",
        False,
    ),
]


# ── 1. 建表 ──────────────────────────────────────────────────
def ensure_schema(conn) -> bool:
    """执行建表 SQL，若主表已存在则跳过。"""
    if table_exists(conn, "tushare_stock_basic") and row_count(conn, "tushare_stock_basic") > 0:
        logger.info("表结构已存在，跳过建表")
        return False
    logger.info("执行建表脚本: %s", SCHEMA_FILE)
    execute_sql_file(conn, SCHEMA_FILE)
    logger.info("建表完成")
    return True


# ── 2. 基础数据 ──────────────────────────────────────────────
def ensure_stock_basic(client: TushareClient, conn) -> bool:
    """若 stock_basic 表为空，全量拉取上市股票列表。"""
    if row_count(conn, "tushare_stock_basic") > 0:
        logger.info("stock_basic 已有数据，跳过")
        return False
    logger.info("stock_basic 为空，全量拉取...")
    df = client.query(
        "stock_basic",
        fields=(
            "ts_code,symbol,name,area,industry,cnspell,market,list_status,"
            "list_date,delist_date,is_hs,act_name,act_ent_type,fullname,"
            "exchange,curr_type,enname"
        ),
        list_status="L",
    )
    if df.empty:
        logger.error("stock_basic API 返回空，请检查 Tushare Token 权限")
        return False
    n = insert_dataframe(conn, "tushare_stock_basic", df, "(ts_code)")
    logger.info("stock_basic: %s 行", f"{n:,}")
    return True


def ensure_trade_cal(client: TushareClient, conn, start_year: int = 2015) -> bool:
    """若 trade_cal 缺失年份，补充交易日历。"""
    cur = conn.cursor()
    try:
        cur.execute("SELECT MIN(cal_date), MAX(cal_date) FROM tushare_trade_cal WHERE exchange='SSE'")
        row = cur.fetchone()
        mn, mx = row if row else (None, None)
    finally:
        cur.close()

    now = date.today()
    end_year = now.year
    missing_years = [
        y for y in range(start_year, end_year + 1)
        if not (mn and mn.year <= y <= mx.year)
    ]
    if not missing_years:
        logger.info("trade_cal 已覆盖 %d~%d，跳过", start_year, end_year)
        return False

    logger.info("trade_cal 补充: %s", ", ".join(str(y) for y in missing_years))
    added = 0
    for year in missing_years:
        try:
            df = client.query(
                "trade_cal", exchange="SSE",
                start_date=f"{year}0101", end_date=f"{year}1231",
                fields="exchange,cal_date,is_open,pretrade_date",
            )
            if not df.empty:
                n = insert_dataframe(conn, "tushare_trade_cal", df, "(exchange, cal_date)")
                added += n
        except Exception as exc:
            logger.warning("trade_cal %d 失败: %s", year, exc)
        time.sleep(0.15)
    logger.info("trade_cal 补充完成: %s 行", f"{added:,}")
    return added > 0


# ── 3. 行情回填 ──────────────────────────────────────────────
def get_trade_dates(conn, start: str, end: str) -> list[str]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT cal_date FROM tushare_trade_cal "
            "WHERE exchange='SSE' AND is_open=1 "
            "AND cal_date BETWEEN %s AND %s ORDER BY cal_date",
            (start, end),
        )
        return [
            row[0].strftime("%Y%m%d") if hasattr(row[0], "strftime") else str(row[0])
            for row in cur.fetchall()
        ]
    finally:
        cur.close()


def get_existing_dates(conn, table: str, col: str) -> set[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT DISTINCT {col} FROM {table}")
        return {
            row[0].strftime("%Y%m%d") if hasattr(row[0], "strftime") else str(row[0])
            for row in cur.fetchall()
        }
    finally:
        cur.close()


def ensure_market_data(client: TushareClient, conn) -> bool:
    """若行情三表任一为空，按交易日全市场回填。断点续传，超时后下次继续。"""
    empty_tables = []
    for _, table_name, _, _, _ in MARKET_APIS:
        if row_count(conn, table_name) == 0:
            empty_tables.append(table_name)

    if not empty_tables:
        logger.info("行情三表均有数据，跳过回填")
        return False

    logger.info("行情表 %s 为空，启动回填...", ", ".join(empty_tables))
    until = date.today().strftime("%Y%m%d")
    trade_dates = get_trade_dates(conn, MARKET_START, until)
    logger.info("交易日范围: %s ~ %s (%d 日)", MARKET_START, until, len(trade_dates))
    if not trade_dates:
        logger.error("trade_cal 无交易日数据，请先确保基础数据已填充")
        return False

    any_filled = False
    for api_name, table_name, fields, conflict, need_rename in MARKET_APIS:
        existing = get_existing_dates(conn, table_name, "trade_date")
        need = [d for d in trade_dates if d not in existing]
        if not need:
            logger.info("  %s: 已完整，跳过", table_name)
            continue
        logger.info("  %s: 已有 %d 日，待补 %d 日", table_name, len(existing), len(need))

        total = 0
        for i, td in enumerate(need):
            try:
                df = client.query(api_name, trade_date=td, fields=fields)
                if not df.empty:
                    if need_rename and "change" in df.columns:
                        df.rename(columns={"change": "change_val"}, inplace=True)
                    n = insert_dataframe(conn, table_name, df, conflict)
                    total += n
                    any_filled = True
            except Exception as exc:
                if (i + 1) % 20 == 0:
                    logger.warning("  %s %s 失败: %s", api_name, td, exc)
            if (i + 1) % 100 == 0:
                logger.info("  %s [%d/%d] %s total=%s", table_name, i + 1, len(need), td, f"{total:,}")

        logger.info("  %s 完成: %s 行", table_name, f"{total:,}")

    return any_filled


# ── 4. 财务回填 ──────────────────────────────────────────────
FIN_START = "20150101"
FIN_STOCK_BATCH = 1  # Tushare 财务接口不支持批量 ts_code，必须单股请求

FIN_APIS = [
    ("income", "tushare_income",
     "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
     "total_revenue,revenue,int_income,prem_earned,comm_income,total_cogs,"
     "oper_cost,sell_exp,admin_exp,fin_exp,assets_impair_loss,"
     "invest_income,non_oper_income,"
     "non_oper_exp,total_profit,income_tax,n_income,n_income_attr_p,"
     "minority_gain,basic_eps,diluted_eps",
     "(ts_code, end_date, report_type, comp_type)"),
    ("balancesheet", "tushare_balancesheet",
     "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
     "total_assets,total_cur_assets,money_cap,trad_asset,notes_receiv,"
     "accounts_receiv,prepayment,inventories,"
     "fix_assets,goodwill,"
     "total_liab,total_cur_liab,notes_payable,"
     "minority_int",
     "(ts_code, end_date, report_type, comp_type)"),
    ("cashflow", "tushare_cashflow",
     "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
     "c_fr_sale_sg,st_cash_out_act,n_cashflow_act",
     "(ts_code, end_date, report_type, comp_type)"),
    ("fina_indicator", "tushare_fina_indicator",
     "ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,"
     "capital_rese_ps,surplus_rese_ps,undist_profit_ps,grossprofit_margin,"
     "netprofit_margin,roe,roe_dt,roa,roa_yearly,roic,or_yoy,op_yoy,"
     "equity_yoy,assets_yoy,debt_to_assets,current_ratio,"
     "quick_ratio,inv_turn,ar_turn,assets_turn",
     "(ts_code, end_date)"),
    ("forecast", "tushare_forecast",
     "ts_code,ann_date,end_date,type,p_change_min,p_change_max,"
     "net_profit_min,net_profit_max,last_parent_net,notice_date,notice_reason",
     "(ts_code, end_date, ann_date)"),
    ("express", "tushare_express",
     "ts_code,ann_date,end_date,revenue,operate_profit,total_profit,"
     "n_income,total_assets,diluted_eps",
     "(ts_code, end_date)"),
    ("dividend", "tushare_dividend",
     "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
     "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,"
     "imp_ann_date,base_date,base_share",
     "(ts_code, end_date, ann_date, div_proc)"),
    ("fina_audit", "tushare_fina_audit",
     "ts_code,ann_date,end_date,audit_result,audit_fees,audit_agency,audit_sign",
     "(ts_code, end_date)"),
    ("fina_mainbz", "tushare_fina_mainbz",
     "ts_code,end_date,bz_item,bz_code,bz_sales,bz_profit,bz_cost,curr_type,update_date",
     "(ts_code, end_date, bz_item, bz_code)"),
]


def ensure_financial_data(client: TushareClient, conn) -> bool:
    """若财务表为空，单股逐只回填（用 ON CONFLICT DO NOTHING 断点续传）。"""
    empty_count = sum(1 for _, table_name, _, _ in FIN_APIS if row_count(conn, table_name) == 0)
    if empty_count == 0:
        logger.info("财务表均有数据，跳过回填")
        return False

    logger.info("财务表 %d/9 为空，启动回填...", empty_count)

    cur = conn.cursor()
    try:
        cur.execute("SELECT ts_code FROM tushare_stock_basic ORDER BY ts_code")
        codes = [row[0] for row in cur.fetchall()]
    finally:
        cur.close()

    if not codes:
        logger.error("无股票代码，请先确保 stock_basic 已填充")
        return False
    logger.info("股票总数: %d", len(codes))

    until = date.today().strftime("%Y%m%d")
    any_filled = False

    for api_name, table_name, fields, conflict in FIN_APIS:
        existing = row_count(conn, table_name)
        if existing > 0:
            logger.info("  %s: 已有 %s 行，跳过", table_name, f"{existing:,}")
            continue
        logger.info("  %s -> %s [%s ~ %s]", api_name, table_name, FIN_START, until)

        total = 0
        t0 = time.time()
        for i, code in enumerate(codes):
            try:
                df = client.query(
                    api_name, ts_code=code,
                    start_date=FIN_START, end_date=until,
                    fields=fields,
                )
                if not df.empty:
                    n = insert_dataframe(conn, table_name, df, conflict)
                    total += n
                    any_filled = True
            except Exception as exc:
                if (i + 1) % 500 == 0:
                    logger.warning("  %s %s 失败: %s", api_name, code, exc)
            if (i + 1) % 1000 == 0:
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                eta = (len(codes) - i - 1) / rate / 60 if rate > 0 else 0
                logger.info(
                    "  %s [%d/%d] %s 行, 速率 %.1f 股/s, ETA %.0f 分钟",
                    table_name, i + 1, len(codes), f"{total:,}", rate, eta,
                )
        logger.info("  %s 完成: %s 行", table_name, f"{total:,}")

    return any_filled
