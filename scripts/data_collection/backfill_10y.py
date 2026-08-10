# -*- coding: utf-8 -*-
"""
Tushare 10年历史数据回填脚本（断点续传）

背景: ts2pg.py 的 P3/P4 遇到表已有数据会跳过，无法补历史缺口。
本脚本独立回填以下内容:

  Phase 0 - 基础表补全
    tushare_trade_cal    2015~2025 年度日历（2026 已有）
    tushare_stock_company 公司概况（全量）
    tushare_namechange    股票曾用名（全量）

  Phase 1 - 行情三表按交易日全市场回填（2016-07-01 ~ 2026-07-31）
    tushare_daily         日线行情（trade_date 维度，单日全市场）
    tushare_adj_factor    复权因子（同上）
    tushare_daily_basic   每日指标（同上）
    已有日期自动跳过（断点续传）

  Phase 2 - 财务九表单股回填（2015-01-01 ~ 2026-07-31）
    income/balancesheet/cashflow/fina_indicator/forecast/
    express/dividend/fina_audit/fina_mainbz
    财务接口不支持批量 ts_code（返回空），必须单股请求。
    ON CONFLICT DO NOTHING 防重，中断后可重跑续传。

用法: nohup python3 backfill_10y.py > logs/backfill_10y.log 2>&1 &
"""

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
    row_count,
    TUSHARE_TOKEN,
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill_10y")

# ── 日期范围: 最近10年（对齐自然月）──
MARKET_START = "20160701"   # 行情回填起点
FIN_START = "20150101"      # 财务回填起点（财务按报告期，留一点余量）
END_DATE = "20260731"       # 截至日（与库内最新数据一致）

STOCK_BATCH = 100           # 行情按日全市场，不需要股票批次
SLEEP_BETWEEN = 0.12

# ── 行情接口定义 ──
MARKET_APIS = [
    (
        "daily", "tushare_daily",
        "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        "(ts_code, trade_date)",
        True,   # 需要 rename change -> change_val
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

# ── 财务接口定义（含 fina_indicator 遗漏的）──
FIN_APIS = [
    (
        "income", "tushare_income",
        "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
        "total_revenue,revenue,int_income,prem_earned,comm_income,total_cogs,"
        "oper_cost,sell_exp,admin_exp,fin_exp,assets_impair_loss,"
        "fair_value_inter_gain,invest_income,oper_profit,non_oper_income,"
        "non_oper_exp,total_profit,income_tax,n_income,n_income_attr_p,"
        "minority_gain,basic_eps,diluted_eps",
        "(ts_code, end_date, report_type, comp_type)",
    ),
    (
        "balancesheet", "tushare_balancesheet",
        "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
        "total_assets,total_cur_assets,money_cap,trad_asset,notes_receiv,"
        "accounts_receiv,prepayment,inventories,total_non_cur_assets,"
        "fix_assets,constru_in_process,intangible_assets,goodwill,"
        "total_liab,total_cur_liab,short_borrow,notes_payable,"
        "accounts_payable,total_non_cur_liab,long_borrow,"
        "total_hldr_eqy_exc_min,minority_int,total_hldr_eqy_inc_min",
        "(ts_code, end_date, report_type, comp_type)",
    ),
    (
        "cashflow", "tushare_cashflow",
        "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
        "c_fr_sale_sg,net_cf_oper_act,net_cf_inv_act,net_cf_fin_act,free_cf,"
        "st_cash_out_act,st_cash_in_act,st_cash_out_inv,st_cash_in_inv,"
        "st_cash_out_fin,st_cash_in_fin,n_cashflow_act,c_change,c_bal_end",
        "(ts_code, end_date, report_type, comp_type)",
    ),
    (
        "fina_indicator", "tushare_fina_indicator",
        "ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,"
        "capital_rese_ps,surplus_rese_ps,undist_profit_ps,grossprofit_margin,"
        "netprofit_margin,roe,roe_dt,roa,roa_yearly,roic,or_yoy,op_yoy,"
        "profit_yoy,equity_yoy,assets_yoy,debt_to_assets,current_ratio,"
        "quick_ratio,equity_ratio,inv_turn,ar_turn,assets_turn",
        "(ts_code, end_date)",
    ),
    (
        "forecast", "tushare_forecast",
        "ts_code,ann_date,end_date,type,p_change_min,p_change_max,"
        "net_profit_min,net_profit_max,last_parent_net,notice_date,notice_reason",
        "(ts_code, end_date, ann_date)",
    ),
    (
        "express", "tushare_express",
        "ts_code,ann_date,end_date,revenue,operate_profit,total_profit,"
        "n_income,total_assets,total_hldr_eqy,diluted_eps,weighted_roe",
        "(ts_code, end_date)",
    ),
    (
        "dividend", "tushare_dividend",
        "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
        "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,"
        "imp_ann_date,base_date,base_share",
        "(ts_code, end_date, ann_date, div_proc)",
    ),
    (
        "fina_audit", "tushare_fina_audit",
        "ts_code,ann_date,end_date,audit_result,audit_fees,audit_agency,audit_sign",
        "(ts_code, end_date)",
    ),
    (
        "fina_mainbz", "tushare_fina_mainbz",
        "ts_code,end_date,bz_item,bz_code,bz_sales,bz_profit,bz_cost,curr_type,update_date",
        "(ts_code, end_date, bz_item, bz_code)",
    ),
]


def get_codes(conn) -> list[str]:
    """全部上市股票代码（含历史退市的需要历史行情，故取 L + D）。"""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT ts_code FROM tushare_stock_basic ORDER BY ts_code"
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def get_trade_dates(conn, start: str, end: str) -> list[str]:
    """从 trade_cal 获取交易日（正序）。"""
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
    """查询表中已有日期集合，用于断点续传。"""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT DISTINCT {col} FROM {table}")
        return {
            row[0].strftime("%Y%m%d") if hasattr(row[0], "strftime") else str(row[0])
            for row in cur.fetchall()
        }
    finally:
        cur.close()


# ── Phase 0: 基础表 ──
def phase0_basic(client: TushareClient, conn):
    logger.info("--- P0: 基础表补全 ---")

    # trade_cal 2015~2025（2026 已有）
    cur = conn.cursor()
    try:
        cur.execute("SELECT MIN(cal_date), MAX(cal_date) FROM tushare_trade_cal WHERE exchange='SSE'")
        mn, mx = cur.fetchone()
        logger.info("  trade_cal 现有范围: %s ~ %s", mn, mx)
    finally:
        cur.close()

    missing_years = [y for y in range(2015, 2026) if not (mn and mn.year <= y <= mx.year)]
    for year in missing_years:
        try:
            df = client.query(
                "trade_cal", exchange="SSE",
                start_date=f"{year}0101", end_date=f"{year}1231",
                fields="exchange,cal_date,is_open,pretrade_date",
            )
            if not df.empty:
                n = insert_dataframe(conn, "tushare_trade_cal", df, "(exchange, cal_date)")
                logger.info("  trade_cal %d: %d 行", year, n)
        except Exception as exc:
            logger.warning("  trade_cal %d 失败: %s", year, exc)
        time.sleep(0.15)

    # stock_company 全量
    if row_count(conn, "tushare_stock_company") == 0:
        df = client.query(
            "stock_company",
            fields="ts_code,com_name,chairman,manager,secretary,reg_capital,"
                   "setup_date,province,city,introduction,website,email,office,"
                   "employees,main_business,business_scope,phone,fax",
        )
        n = insert_dataframe(conn, "tushare_stock_company", df, "(ts_code)")
        logger.info("  stock_company: %d", n)
    else:
        logger.info("  stock_company: 已有数据，跳过")

    # namechange 全量
    if row_count(conn, "tushare_namechange") == 0:
        df = client.query(
            "namechange", fields="ts_code,name,start_date,end_date,change_reason"
        )
        n = insert_dataframe(conn, "tushare_namechange", df, "(ts_code, start_date)")
        logger.info("  namechange: %d", n)
    else:
        logger.info("  namechange: 已有数据，跳过")

    logger.info("P0 完成")


# ── Phase 1: 行情三表按日回填 ──
def phase1_market(client: TushareClient, conn):
    logger.info("--- P1: 行情三表按日回填 [%s ~ %s] ---", MARKET_START, END_DATE)

    trade_dates = get_trade_dates(conn, MARKET_START, END_DATE)
    logger.info("  交易日总数: %d", len(trade_dates))
    if not trade_dates:
        logger.warning("  trade_cal 无数据，请先跑 P0")
        return

    for api_name, table_name, fields, conflict, need_rename in MARKET_APIS:
        existing = get_existing_dates(conn, table_name, "trade_date")
        need = [d for d in trade_dates if d not in existing]
        logger.info(
            "  %s: 已有 %d 日，待补 %d 日", table_name, len(existing), len(need)
        )
        if not need:
            continue

        total = 0
        for i, td in enumerate(need):
            try:
                df = client.query(api_name, trade_date=td, fields=fields)
                if not df.empty:
                    if need_rename and "change" in df.columns:
                        df.rename(columns={"change": "change_val"}, inplace=True)
                    n = insert_dataframe(conn, table_name, df, conflict)
                    total += n
            except Exception as exc:
                if (i + 1) % 20 == 0:
                    logger.warning("  %s %s 失败: %s", api_name, td, exc)
            if (i + 1) % 50 == 0:
                logger.info(
                    "  %s [%d/%d] %s total=%s",
                    table_name, i + 1, len(need), td, f"{total:,}",
                )
        logger.info("  %s 完成: %s 行", table_name, f"{total:,}")

    logger.info("P1 完成")


# ── Phase 2: 财务九表单股回填 ──
def phase2_financial(client: TushareClient, conn):
    logger.info("--- P2: 财务九表单股回填 [%s ~ %s] ---", FIN_START, END_DATE)
    codes = get_codes(conn)
    logger.info("  股票总数(含退市): %d", len(codes))
    if not codes:
        logger.error("  无股票代码")
        return

    for api_name, table_name, fields, conflict in FIN_APIS:
        existing = row_count(conn, table_name)
        logger.info(
            "  %s -> %s (已有 %s 行，单股拉取补齐)",
            api_name, table_name, f"{existing:,}",
        )
        total = 0
        t0 = time.time()
        for i, code in enumerate(codes):
            try:
                df = client.query(
                    api_name, ts_code=code,
                    start_date=FIN_START, end_date=END_DATE,
                    fields=fields,
                )
                if not df.empty:
                    n = insert_dataframe(conn, table_name, df, conflict)
                    total += n
            except Exception as exc:
                if (i + 1) % 200 == 0:
                    logger.warning("  %s %s 失败: %s", api_name, code, exc)
            if (i + 1) % 500 == 0:
                elapsed = time.time() - t0
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                eta = (len(codes) - i - 1) / rate / 60 if rate > 0 else 0
                logger.info(
                    "  %s [%d/%d] %s 行, 速率 %.1f 股/s, ETA %.0f 分钟",
                    table_name, i + 1, len(codes), f"{total:,}", rate, eta,
                )
        logger.info("  %s 完成: %s 行", table_name, f"{total:,}")

    logger.info("P2 完成")


# ── Phase 3: 验证 ──
def phase3_verify(conn):
    logger.info("\n" + "=" * 60)
    logger.info("P3: 验证")
    logger.info("=" * 60)
    tables = [
        ("tushare_trade_cal", "交易日历", "cal_date"),
        ("tushare_stock_company", "公司概况", None),
        ("tushare_namechange", "曾用名", None),
        ("tushare_daily", "日线行情", "trade_date"),
        ("tushare_adj_factor", "复权因子", "trade_date"),
        ("tushare_daily_basic", "每日指标", "trade_date"),
        ("tushare_income", "利润表", "end_date"),
        ("tushare_balancesheet", "资产负债表", "end_date"),
        ("tushare_cashflow", "现金流量表", "end_date"),
        ("tushare_fina_indicator", "财务指标", "end_date"),
        ("tushare_forecast", "业绩预告", "end_date"),
        ("tushare_express", "业绩快报", "end_date"),
        ("tushare_dividend", "分红送股", "end_date"),
        ("tushare_fina_audit", "审计意见", "end_date"),
        ("tushare_fina_mainbz", "主营构成", "end_date"),
    ]
    cur = conn.cursor()
    try:
        for table, label, date_col in tables:
            cnt = row_count(conn, table)
            date_info = ""
            if cnt > 0 and date_col:
                try:
                    cur.execute(
                        f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}"
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        date_info = f" [{row[0]} ~ {row[1]}]"
                except Exception:
                    pass
            status = "OK" if cnt > 0 else "EMPTY"
            logger.info(
                "  [%s] %-30s %12s%s", status, table, f"{cnt:,}", date_info
            )
    finally:
        cur.close()


def main():
    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置！")
        sys.exit(1)

    client = TushareClient(TUSHARE_TOKEN, rate_limit=480)
    conn = get_pg_connection()
    t0 = time.time()
    try:
        phase0_basic(client, conn)
        phase1_market(client, conn)
        phase2_financial(client, conn)
        phase3_verify(conn)
        logger.info("=== 全部完成 总耗时 %.1f 分钟 ===", (time.time() - t0) / 60)
    except KeyboardInterrupt:
        logger.warning("用户中断")
    except Exception as exc:
        logger.error("执行失败: %s", exc, exc_info=True)
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
