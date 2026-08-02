# -*- coding: utf-8 -*-
"""
财务数据增量拉取脚本

每周执行，自动补全 9 张财务表中截至当天的所有缺失数据：

  利润表/资产负债表/现金流量表/财务指标/业绩预告/业绩快报/分红送股/审计意见/主营构成

策略:
  - 每张表查询 MAX(end_date)，从该日期前推 6 个月作为拉取起点
  - 按股票批次（50只/批）＋年度维度逐批请求
  - ON CONFLICT DO NOTHING 防止重复插入

用法: python scripts/data_collection/incremental_fin.py
"""

import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

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
logger = logging.getLogger("inc_fin")

FIN_BATCH = 50
SLEEP_BETWEEN = 0.18
LOOKBACK_DAYS = 180

FIN_APIS = [
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


def _fmt(d: date) -> str:
    return d.strftime("%Y%m%d")


def get_codes(conn) -> list[str]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT ts_code FROM tushare_stock_basic "
            "WHERE list_status='L' ORDER BY ts_code"
        )
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()


def get_max_end_date(conn, table: str) -> date | None:
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT MAX(end_date) FROM {table}")
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    finally:
        cur.close()


def pull_fin_table(
    client: TushareClient, conn,
    api_name: str, table_name: str,
    fields: str, conflict: str,
    codes: list[str],
    start_date: str, end_date: str,
):
    """按股票批次拉取单张财务表。"""
    logger.info(
        "--- %s -> %s [%s ~ %s] ---", api_name, table_name, start_date, end_date,
    )
    total = 0
    for i in range(0, len(codes), FIN_BATCH):
        batch = codes[i:i + FIN_BATCH]
        cs = ",".join(batch)
        try:
            df = client.query(
                api_name, ts_code=cs,
                start_date=start_date, end_date=end_date,
                fields=fields,
            )
            if not df.empty:
                n = insert_dataframe(conn, table_name, df, conflict)
                total += n
        except Exception as exc:
            if (i // FIN_BATCH) % 20 == 0:
                logger.warning("  %s b%d 失败: %s", api_name, i // FIN_BATCH, exc)
        time.sleep(SLEEP_BETWEEN)

        if (i // FIN_BATCH) % 40 == 0:
            logger.info(
                "  %s [%d/%d] %s",
                api_name,
                i // FIN_BATCH + 1,
                (len(codes) + FIN_BATCH - 1) // FIN_BATCH,
                f"{total:,}",
            )
    logger.info("  %s 完成: %s", table_name, f"{total:,}")
    return total


def _print_summary(conn):
    tables = [
        ("tushare_income", "利润表"),
        ("tushare_balancesheet", "资产负债表"),
        ("tushare_cashflow", "现金流量表"),
        ("tushare_fina_indicator", "财务指标"),
        ("tushare_forecast", "业绩预告"),
        ("tushare_express", "业绩快报"),
        ("tushare_dividend", "分红送股"),
        ("tushare_fina_audit", "审计意见"),
        ("tushare_fina_mainbz", "主营构成"),
    ]
    logger.info("=== 当前财务数据概况 ===")
    for table, label in tables:
        cnt = row_count(conn, table)
        max_d = get_max_end_date(conn, table)
        logger.info("  %s: %s 行  最新报告期: %s", label, f"{cnt:,}" if cnt else "空", max_d or "无")


def main():
    if not TUSHARE_TOKEN:
        logger.error("TUSHARE_TOKEN 未设置，请在 .env 中配置")
        sys.exit(1)

    t0 = time.time()
    client = TushareClient(TUSHARE_TOKEN)
    conn = get_pg_connection()

    try:
        _print_summary(conn)

        codes = get_codes(conn)
        if not codes:
            logger.error("无上市股票代码，请先运行基础数据导入")
            sys.exit(1)
        logger.info("上市股票: %d 只", len(codes))

        today = date.today()
        until = _fmt(today)
        default_start = _fmt(today - timedelta(days=LOOKBACK_DAYS))

        grand_total = 0
        for api_name, table_name, fields, conflict in FIN_APIS:
            max_d = get_max_end_date(conn, table_name)
            if max_d:
                start_d = max_d - timedelta(days=LOOKBACK_DAYS)
                start = _fmt(max(start_d, date(2015, 1, 1)))
            else:
                start = "20150101"
            start = min(start, default_start)

            logger.info(
                "%s: 最新报告期=%s  拉取范围=%s~%s",
                table_name, max_d or "无", start, until,
            )

            if start >= until:
                logger.info("  无需更新，跳过")
                continue

            n = pull_fin_table(
                client, conn,
                api_name, table_name, fields, conflict,
                codes, start, until,
            )
            grand_total += n

        elapsed = time.time() - t0
        logger.info("=== 财务数据刷新完成 耗时 %.0f 秒  新增: %s ===", elapsed, f"{grand_total:,}")
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
