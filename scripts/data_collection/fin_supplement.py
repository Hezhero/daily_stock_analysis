# -*- coding: utf-8 -*-
"""
Tushare 财务数据补充拉取（单股模式，逐只请求）

当 ts2pg.py 按批量的 P4 阶段因财务接口限制无法完整拉取时，
使用本脚本逐只请求进行补充。

用法: python scripts/fin_supplement.py
"""

import logging
import os
import sys
import time

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
logger = logging.getLogger("fin_supplement")

# 财务 API 定义
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


def main():
    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置！")
        sys.exit(1)

    client = TushareClient(TUSHARE_TOKEN)
    conn = get_pg_connection()

    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT ts_code FROM tushare_stock_basic "
                "WHERE list_status='L' ORDER BY ts_code"
            )
            codes = [row[0] for row in cur.fetchall()]
        finally:
            cur.close()

        logger.info("上市股票: %d 只", len(codes))

        for api_name, table_name, fields, conflict in FIN_APIS:
            existing = row_count(conn, table_name)
            if existing > 0:
                logger.info("跳过 %s: %s", table_name, f"{existing:,}")
                continue

            logger.info("--- %s -> %s ---", api_name, table_name)
            total = 0
            for i, code in enumerate(codes):
                if (i + 1) % 500 == 0:
                    logger.info("  [%d/%d] %s", i + 1, len(codes), f"{total:,}")
                try:
                    df = client.query(
                        api_name,
                        ts_code=code,
                        start_date="20150101",
                        end_date="20260729",
                        fields=fields,
                    )
                    if not df.empty:
                        total += insert_dataframe(conn, table_name, df, conflict)
                except Exception as exc:
                    if (i + 1) % 1000 == 0:
                        logger.warning("  %s 失败: %s", code, exc)
            logger.info("  %s: %s", table_name, f"{total:,}")

        logger.info("=== 最终统计 ===")
        for _, table_name, _, _ in FIN_APIS:
            logger.info("  %s: %s", table_name, f"{row_count(conn, table_name):,}")

    except KeyboardInterrupt:
        logger.warning("用户中断")
    finally:
        conn.close()
    logger.info("done!")


if __name__ == "__main__":
    main()
