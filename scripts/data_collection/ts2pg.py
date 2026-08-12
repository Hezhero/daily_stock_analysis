# -*- coding: utf-8 -*-
"""
Tushare → PostgreSQL 数据导入工具
用法: python scripts/ts2pg.py
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd

from tushare_pg_utils import (
    TushareClient,
    execute_sql_file,
    get_pg_connection,
    insert_dataframe,
    row_count,
    table_exists,
    TUSHARE_API_URL,
    TUSHARE_TOKEN,
)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ts2pg")

END_DATE = datetime.now().strftime("%Y%m%d")
START_DATE = (datetime.now() - timedelta(days=365 * 10 + 30)).strftime("%Y%m%d")
RATE_LIMIT = 480
BATCH_SIZE = 500
STOCK_BATCH = 100
FIN_BATCH = 50

logger.info("Tushare->PG | %s~%s", START_DATE, END_DATE)


def get_codes(conn):
    """获取所有上市股票代码。"""
    cur = conn.cursor()
    try:
        cur.execute("SELECT ts_code FROM tushare_stock_basic WHERE list_status='L'")
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def month_ranges(start: str, end: str):
    """将日期范围按月份拆分。"""
    s = datetime.strptime(start, "%Y%m%d")
    e = datetime.strptime(end, "%Y%m%d")
    months = []
    cur = s.replace(day=1)
    while cur <= e:
        me = (cur.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        if me > e:
            me = e
        months.append((cur.strftime("%Y%m%d"), me.strftime("%Y%m%d")))
        cur = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
    return months


# ── Phase 1: 建表 ──
def phase1_schema(conn):
    schema_file = os.path.join(PROJECT_ROOT, "docs", "tushare_postgres_schema.sql")
    if table_exists(conn, "tushare_stock_basic") and row_count(conn, "tushare_stock_basic") > 0:
        logger.info("P1 跳过: 表已存在且有数据")
        return
    execute_sql_file(conn, schema_file)
    logger.info("P1 完成")


# ── Phase 2: 基础数据 ──
def phase2_basic(client: TushareClient, conn):
    logger.info("--- P2: 基础数据 ---")

    if row_count(conn, "tushare_stock_basic") == 0:
        df = client.query(
            "stock_basic",
            fields="ts_code,symbol,name,area,industry,cnspell,market,list_status,"
                   "list_date,delist_date,is_hs,act_name,act_ent_type,fullname,"
                   "exchange,curr_type,enname",
        )
        n = insert_dataframe(conn, "tushare_stock_basic", df, "(ts_code)")
        logger.info("  stock_basic: %d", n)
    else:
        logger.info("  stock_basic: 跳过")

    if row_count(conn, "tushare_trade_cal") == 0:
        logger.info("  trade_cal SSE 2015~now ...")
        all_cal = []
        for year in range(2015, datetime.now().year + 1):
            end_y = f"{year}1231" if year < datetime.now().year else END_DATE
            try:
                df = client.query(
                    "trade_cal",
                    exchange="SSE",
                    start_date=f"{year}0101",
                    end_date=end_y,
                    fields="exchange,cal_date,is_open,pretrade_date",
                )
                if not df.empty:
                    all_cal.append(df)
            except Exception as exc:
                logger.warning("    cal %d: %s", year, exc)
            time.sleep(0.15)
        if all_cal:
            df = pd.concat(all_cal, ignore_index=True).drop_duplicates()
            n = insert_dataframe(conn, "tushare_trade_cal", df, "(exchange, cal_date)")
            logger.info("  trade_cal: %d", n)
    else:
        logger.info("  trade_cal: 跳过")

    if row_count(conn, "tushare_stock_company") == 0:
        df = client.query(
            "stock_company",
            fields="ts_code,com_name,chairman,manager,secretary,reg_capital,"
                   "setup_date,province,city,introduction,website,email,office,"
                   "employees,main_business,business_scope",
        )
        n = insert_dataframe(conn, "tushare_stock_company", df, "(ts_code)")
        logger.info("  stock_company: %d", n)
    else:
        logger.info("  stock_company: 跳过")

    if row_count(conn, "tushare_namechange") == 0:
        df = client.query(
            "namechange",
            fields="ts_code,name,start_date,end_date,change_reason",
        )
        n = insert_dataframe(conn, "tushare_namechange", df, "(ts_code, start_date)")
        logger.info("  namechange: %d", n)
    else:
        logger.info("  namechange: 跳过")

    logger.info("P2 完成")


# ── Phase 3: 行情数据 ──
def phase3_market(client: TushareClient, conn):
    logger.info("--- P3: 行情数据 ---")
    if row_count(conn, "tushare_daily") > 0:
        logger.info("  跳过: daily=%s", f"{row_count(conn, 'tushare_daily'):,}")
        return

    codes = get_codes(conn)
    if not codes:
        logger.error("无股票代码，请先运行 P2")
        return

    months = month_ranges(START_DATE, END_DATE)
    logger.info("  股票=%d 月份=%d", len(codes), len(months))

    apis = [
        (
            "daily", "tushare_daily",
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
            "(ts_code, trade_date)",
        ),
        (
            "adj_factor", "tushare_adj_factor",
            "ts_code,trade_date,adj_factor",
            "(ts_code, trade_date)",
        ),
        (
            "daily_basic", "tushare_daily_basic",
            "ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,"
            "pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,"
            "free_share,total_mv,circ_mv",
            "(ts_code, trade_date)",
        ),
    ]

    totals = {"daily": 0, "adj_factor": 0, "daily_basic": 0}
    for mi, (ms, me) in enumerate(months):
        logger.info(
            "  [%d/%d] %s d=%s a=%s b=%s",
            mi + 1, len(months), ms[:6],
            f"{totals['daily']:,}", f"{totals['adj_factor']:,}", f"{totals['daily_basic']:,}",
        )
        for i in range(0, len(codes), STOCK_BATCH):
            batch = codes[i:i + STOCK_BATCH]
            cs = ",".join(batch)
            for api_name, table_name, fields, conflict in apis:
                try:
                    df = client.query(
                        api_name, ts_code=cs, start_date=ms, end_date=me, fields=fields
                    )
                    if not df.empty:
                        if "change" in df.columns:
                            df.rename(columns={"change": "change_val"}, inplace=True)
                        n = insert_dataframe(conn, table_name, df, conflict)
                        totals[api_name] += n
                except Exception as exc:
                    logger.warning("    %s b%d: %s", api_name, i, exc)
                time.sleep(0.12)
    logger.info("P3 完成: %s", totals)


# ── Phase 4: 财务数据 ──
def phase4_financial(client: TushareClient, conn):
    logger.info("--- P4: 财务数据 ---")
    codes = get_codes(conn)
    if not codes:
        logger.error("无股票代码，请先运行 P2")
        return

    years = list(range(2015, datetime.now().year + 1))

    apis = [
        (
            "income", "tushare_income",
            "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
            "total_revenue,revenue,int_income,prem_earned,comm_income,total_cogs,"
            "oper_cost,sell_exp,admin_exp,fin_exp,assets_impair_loss,"
            "invest_income,non_oper_income,"
            "non_oper_exp,total_profit,income_tax,n_income,n_income_attr_p,"
            "minority_gain,basic_eps,diluted_eps",
            "(ts_code, end_date, report_type, comp_type)",
        ),
        (
            "balancesheet", "tushare_balancesheet",
            "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
            "total_assets,total_cur_assets,money_cap,trad_asset,notes_receiv,"
            "accounts_receiv,prepayment,inventories,"
            "fix_assets,goodwill,"
            "total_liab,total_cur_liab,notes_payable,"
            "minority_int",
            "(ts_code, end_date, report_type, comp_type)",
        ),
        (
            "cashflow", "tushare_cashflow",
            "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
            "c_fr_sale_sg,"
            "st_cash_out_act,n_cashflow_act",
            "(ts_code, end_date, report_type, comp_type)",
        ),
        (
            "fina_indicator", "tushare_fina_indicator",
            "ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,"
            "capital_rese_ps,surplus_rese_ps,undist_profit_ps,grossprofit_margin,"
            "netprofit_margin,roe,roe_dt,roa,roa_yearly,roic,or_yoy,op_yoy,"
            "equity_yoy,assets_yoy,debt_to_assets,current_ratio,"
            "quick_ratio,inv_turn,ar_turn,assets_turn",
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
            "n_income,total_assets,diluted_eps",
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

    for api_name, table_name, fields, conflict in apis:
        existing = row_count(conn, table_name)
        if existing > 0:
            logger.info("  跳过 %s: %s", table_name, f"{existing:,}")
            continue

        logger.info("  %s -> %s ...", api_name, table_name)
        total = 0
        for year in years:
            period_start = f"{year}0101"
            for i in range(0, len(codes), FIN_BATCH):
                batch = codes[i:i + FIN_BATCH]
                cs = ",".join(batch)
                try:
                    df = client.query(
                        api_name,
                        ts_code=cs,
                        start_date=period_start,
                        end_date=f"{year}1231",
                        fields=fields,
                    )
                    if not df.empty:
                        n = insert_dataframe(conn, table_name, df, conflict)
                        total += n
                except Exception as exc:
                    logger.warning("    %s y%d b%d: %s", api_name, year, i, exc)
                time.sleep(0.18)
        logger.info("  %s: %s", table_name, f"{total:,}")

    logger.info("P4 完成")


# ── Phase 5: 验证 ──
def phase5_verify(conn):
    logger.info("\n" + "=" * 60)
    logger.info("P5: 验证")
    logger.info("=" * 60)

    tables = [
        ("tushare_stock_basic", "基础-列表", None),
        ("tushare_trade_cal", "基础-日历", "cal_date"),
        ("tushare_stock_company", "基础-公司", None),
        ("tushare_namechange", "基础-曾用名", None),
        ("tushare_daily", "行情-日线", "trade_date"),
        ("tushare_adj_factor", "行情-复权", "trade_date"),
        ("tushare_daily_basic", "行情-每日指标", "trade_date"),
        ("tushare_income", "财务-利润表", "end_date"),
        ("tushare_balancesheet", "财务-资产负债表", "end_date"),
        ("tushare_cashflow", "财务-现金流量表", "end_date"),
        ("tushare_forecast", "财务-业绩预告", "end_date"),
        ("tushare_express", "财务-业绩快报", "end_date"),
        ("tushare_dividend", "财务-分红送股", "end_date"),
        ("tushare_fina_indicator", "财务-财务指标", "end_date"),
        ("tushare_fina_audit", "财务-审计意见", "end_date"),
        ("tushare_fina_mainbz", "财务-主营构成", "end_date"),
    ]

    for table_name, label, date_col in tables:
        cnt = row_count(conn, table_name)
        status = "OK" if cnt > 0 else "EMPTY"
        date_info = ""
        if cnt > 0 and date_col:
            cur = conn.cursor()
            try:
                cur.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table_name}")
                row = cur.fetchone()
                if row and row[0]:
                    date_info = f" [{row[0]} ~ {row[1]}]"
            except Exception:
                pass
            finally:
                cur.close()
        logger.info("  [%s] %-30s %10s%s", status, table_name, f"{cnt:,}", date_info)


def main():
    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置！")
        sys.exit(1)

    client = TushareClient(TUSHARE_TOKEN, rate_limit=RATE_LIMIT)
    logger.info("测试 Tushare 连接...")
    test_df = client.query("stock_basic", limit=1)
    if test_df.empty:
        logger.error("Tushare API 连接失败！")
        sys.exit(1)
    logger.info("Tushare 连接正常")

    conn = get_pg_connection()
    try:
        phase1_schema(conn)
        phase2_basic(client, conn)
        phase3_market(client, conn)
        phase4_financial(client, conn)
        phase5_verify(conn)
    except KeyboardInterrupt:
        logger.warning("用户中断")
    except Exception as exc:
        logger.error("执行失败: %s", exc, exc_info=True)
        conn.rollback()
    finally:
        conn.close()
    logger.info("done!")


if __name__ == "__main__":
    main()
