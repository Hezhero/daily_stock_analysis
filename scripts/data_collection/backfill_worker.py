# -*- coding: utf-8 -*-
"""
并行回填 worker（单表模式）

用法:
  # 行情表按日回填
  python3 backfill_worker.py market daily --start 20160701 --end 20260731 --rate 20
  python3 backfill_worker.py market adj_factor --start 20160701 --end 20260731 --rate 20
  python3 backfill_worker.py market daily_basic --start 20160701 --end 20260731 --rate 20

  # 财务表单股回填
  python3 backfill_worker.py fin income --start 20150101 --end 20260731 --rate 40
  python3 backfill_worker.py fin balancesheet --start 20150101 --end 20260731 --rate 40
  ...

幂等: 已有日期/股票自动跳过（ON CONFLICT DO NOTHING + 已存在日期过滤）
"""

import argparse
import logging
import os
import sys
import time

import pandas as pd

from tushare_pg_utils import (
    TushareClient,
    get_pg_connection,
    insert_dataframe,
    row_count,
    TUSHARE_TOKEN,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("worker")

MARKET_APIS = {
    "daily": (
        "daily", "tushare_daily",
        "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        "(ts_code, trade_date)", True,
    ),
    "adj_factor": (
        "adj_factor", "tushare_adj_factor",
        "ts_code,trade_date,adj_factor",
        "(ts_code, trade_date)", False,
    ),
    "daily_basic": (
        "daily_basic", "tushare_daily_basic",
        "ts_code,trade_date,close,turnover_rate,turnover_rate_f,"
        "volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,"
        "total_share,float_share,free_share,total_mv,circ_mv",
        "(ts_code, trade_date)", False,
    ),
}

FIN_APIS = {
    "income": (
        "income", "tushare_income",
        "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
        "total_revenue,revenue,int_income,prem_earned,comm_income,total_cogs,"
        "oper_cost,sell_exp,admin_exp,fin_exp,assets_impair_loss,"
        "fair_value_inter_gain,invest_income,oper_profit,non_oper_income,"
        "non_oper_exp,total_profit,income_tax,n_income,n_income_attr_p,"
        "minority_gain,basic_eps,diluted_eps",
        "(ts_code, end_date, report_type, comp_type)",
    ),
    "balancesheet": (
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
    "cashflow": (
        "cashflow", "tushare_cashflow",
        "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,"
        "c_fr_sale_sg,net_cf_oper_act,net_cf_inv_act,net_cf_fin_act,free_cf,"
        "st_cash_out_act,st_cash_in_act,st_cash_out_inv,st_cash_in_inv,"
        "st_cash_out_fin,st_cash_in_fin,n_cashflow_act,c_change,c_bal_end",
        "(ts_code, end_date, report_type, comp_type)",
    ),
    "fina_indicator": (
        "fina_indicator", "tushare_fina_indicator",
        "ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,"
        "capital_rese_ps,surplus_rese_ps,undist_profit_ps,grossprofit_margin,"
        "netprofit_margin,roe,roe_dt,roa,roa_yearly,roic,or_yoy,op_yoy,"
        "profit_yoy,equity_yoy,assets_yoy,debt_to_assets,current_ratio,"
        "quick_ratio,equity_ratio,inv_turn,ar_turn,assets_turn",
        "(ts_code, end_date)",
    ),
    "forecast": (
        "forecast", "tushare_forecast",
        "ts_code,ann_date,end_date,type,p_change_min,p_change_max,"
        "net_profit_min,net_profit_max,last_parent_net,notice_date,notice_reason",
        "(ts_code, end_date, ann_date)",
    ),
    "express": (
        "express", "tushare_express",
        "ts_code,ann_date,end_date,revenue,operate_profit,total_profit,"
        "n_income,total_assets,total_hldr_eqy,diluted_eps,weighted_roe",
        "(ts_code, end_date)",
    ),
    "dividend": (
        "dividend", "tushare_dividend",
        "ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,"
        "cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,"
        "imp_ann_date,base_date,base_share",
        "(ts_code, end_date, ann_date, div_proc)",
        True,  # split_by_year: dividend 长日期范围返回空，必须按年拉取
    ),
    "fina_audit": (
        "fina_audit", "tushare_fina_audit",
        "ts_code,ann_date,end_date,audit_result,audit_fees,audit_agency,audit_sign",
        "(ts_code, end_date)",
    ),
    "fina_mainbz": (
        "fina_mainbz", "tushare_fina_mainbz",
        "ts_code,end_date,bz_item,bz_code,bz_sales,bz_profit,bz_cost,curr_type,update_date",
        "(ts_code, end_date, bz_item, bz_code)",
    ),
}


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


def get_codes(conn) -> list[str]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT ts_code FROM tushare_stock_basic ORDER BY ts_code")
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def work_market(client, conn, key, start, end):
    api_name, table_name, fields, conflict, need_rename = MARKET_APIS[key]
    existing = get_existing_dates(conn, table_name, "trade_date")
    trade_dates = get_trade_dates(conn, start, end)
    need = [d for d in trade_dates if d not in existing]
    logger.info(
        "[%s] %s: 已有 %d 日 / %d，待补 %d 日",
        key, table_name, len(existing), len(trade_dates), len(need),
    )
    if not need:
        logger.info("[%s] 无需补充，退出", key)
        return

    total = 0
    t0 = time.time()
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
                logger.warning("[%s] %s 失败: %s", key, td, exc)
        if (i + 1) % 50 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(need) - i - 1) / rate / 60 if rate > 0 else 0
            logger.info(
                "[%s] [%d/%d] %s total=%s 速率 %.1f日/s ETA %.0f分",
                key, i + 1, len(need), td, f"{total:,}", rate, eta,
            )
    logger.info("[%s] 完成: %s 行", key, f"{total:,}")


def work_fin(client, conn, key, start, end):
    info = FIN_APIS[key]
    api_name, table_name, fields, conflict = info[:4]
    split_year = len(info) > 4 and info[4]
    codes = get_codes(conn)
    logger.info(
        "[%s] %s: 股票 %d 只，单股拉取 %s~%s%s",
        key, table_name, len(codes), start, end,
        "（按年拆分）" if split_year else "",
    )

    # 统计已有行数（含退市股票，无法按股票精确去重，靠 ON CONFLICT）
    existing = row_count(conn, table_name)
    logger.info("[%s] 表已有 %s 行", key, f"{existing:,}")

    # 按年拆分范围（dividend 长区间返回空）
    year_ranges = [(start, end)]
    if split_year:
        sy = int(start[:4])
        ey = int(end[:4])
        year_ranges = [
            (f"{y}0101", f"{y}1231" if y < ey else end)
            for y in range(sy, ey + 1)
        ]

    total = 0
    t0 = time.time()
    for i, code in enumerate(codes):
        for ys, ye in year_ranges:
            try:
                df = client.query(
                    api_name, ts_code=code,
                    start_date=ys, end_date=ye, fields=fields,
                )
                if not df.empty:
                    n = insert_dataframe(conn, table_name, df, conflict)
                    total += n
            except Exception as exc:
                if (i + 1) % 100 == 0:
                    logger.warning("[%s] %s %s 失败: %s", key, code, ys, exc)
        if (i + 1) % 300 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(codes) - i - 1) / rate / 60 if rate > 0 else 0
            logger.info(
                "[%s] [%d/%d] %s 行 速率 %.1f股/s ETA %.0f分",
                key, i + 1, len(codes), f"{total:,}", rate, eta,
            )
    logger.info("[%s] 完成: %s 行", key, f"{total:,}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["market", "fin"])
    parser.add_argument("table", help="daily/adj_factor/daily_basic 或 income/balancesheet/...")
    parser.add_argument("--start", default="20160101")
    parser.add_argument("--end", default="20260731")
    parser.add_argument("--rate", type=int, default=20, help="每分钟请求上限")
    args = parser.parse_args()

    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置")
        sys.exit(1)

    client = TushareClient(TUSHARE_TOKEN, rate_limit=args.rate)
    conn = get_pg_connection()
    try:
        if args.kind == "market":
            if args.table not in MARKET_APIS:
                logger.error("未知行情表: %s", args.table)
                sys.exit(1)
            work_market(client, conn, args.table, args.start, args.end)
        else:
            if args.table not in FIN_APIS:
                logger.error("未知财务表: %s", args.table)
                sys.exit(1)
            work_fin(client, conn, args.table, args.start, args.end)
    except KeyboardInterrupt:
        logger.warning("[%s] 用户中断", args.table)
    except Exception as exc:
        logger.error("[%s] 执行失败: %s", args.table, exc, exc_info=True)
        sys.exit(1)
    finally:
        conn.close()
    logger.info("[%s] done!", args.table)


if __name__ == "__main__":
    main()
