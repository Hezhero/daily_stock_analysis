# -*- coding: utf-8 -*-
"""
Tushare PG 表空字段分析脚本

扫描所有 tushare_* 表，找出数据始终为 NULL 的字段，
判断是否为无用字段，生成 DROP COLUMN SQL。

用法:
  python scripts/data_collection/check_empty_fields.py           # 仅分析，报告结果
  python scripts/data_collection/check_empty_fields.py --drop     # 分析后直接执行删除
"""

import logging
import sys
from collections import defaultdict

import psycopg2

from tushare_pg_utils import get_pg_connection, table_exists

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("check_empty")

# 始终保留的系统列（不参与空值判断）
SYSTEM_COLUMNS = {"id", "created_at", "updated_at"}

# 即使全空也保留的列及其原因
PRESERVE_EMPTY_COLUMNS: dict[str, str] = {
    "delist_date": "退市日期，只有已退市股票才有值",
    "out_date": "沪深港通剔除日期，只有被剔除的才有值",
    "end_type": "报告期类型，Tushare 新接口已不返回但代码仍拉取",
    "f_ann_date": "实际公告日期，Tushare 部分股票不返回",
    "extra_fields": "扩展字段 JSONB，按需填充",
    "change_val": "涨跌额，由 trigger/代码计算而不是 Tushare 直接返回",

    "is_new": "沪深港通成分股是否最新，全量刷新后可能均为 0",
    "issue_date": "IPO 申购日期，部分新股不返回",
    "limit_amount": "IPO 网上发行量，部分新股不返回",
    "ballot": "IPO 中签率，部分新股不返回",
    "notice_date": "业绩预告公告日期，部分记录不返回",
    "notice_reason": "业绩变动原因，部分记录不返回",
    "div_listdate": "红股上市日，未实施时不返回",
    "imp_ann_date": "分红实施公告日，未实施时不返回",
    "base_date": "分红基准日，部分记录不返回",
    "base_share": "分红基准股本，部分记录不返回",
    "stk_div": "每股送转，部分分红方案不涉及送转",
    "stk_bo_rate": "每股送股，部分分红方案不涉及送股",
    "stk_co_rate": "每股转增，部分分红方案不涉及转增",
    "cash_div": "每股派息，部分分红方案不涉及派息",
    "cash_div_tax": "每股派息税后，部分分红方案不涉及派息",
    "record_date": "股权登记日，未实施方案不返回",
    "ex_date": "除权除息日，未实施方案不返回",
    "pay_date": "派息日，未实施方案不返回",
    "audit_sign": "签字会计师，Tushare 部分记录不返回",
    "bz_code": "主营构成项目代码，部分记录不返回",
    "update_date": "主营构成更新日期，部分记录不返回",
}


def get_tushare_tables(conn) -> list[str]:
    """获取所有 tushare_* 表（排除视图和分区子表）。"""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT tablename FROM pg_tables "
            "WHERE schemaname='public' AND tablename LIKE 'tushare_%' "
            "ORDER BY tablename"
        )
        tables = [row[0] for row in cur.fetchall()]

        # 排除默认分区表（它们是分区子表，查询主表即可）
        tables = [t for t in tables if not t.endswith("_default")]

        # 排除年份分区表
        import re
        tables = [t for t in tables if not re.search(r"_\d{4}$", t)]

        return tables
    finally:
        cur.close()


def get_table_columns(conn, table: str) -> list[dict]:
    """获取表的所有列信息。"""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=%s "
            "ORDER BY ordinal_position",
            (table,),
        )
        return [
            {"name": row[0], "type": row[1], "nullable": row[2]}
            for row in cur.fetchall()
        ]
    finally:
        cur.close()


def get_row_count(conn, table: str) -> int:
    """获取表行数。"""
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
    except Exception as exc:
        logger.warning("  获取 %s 行数失败: %s", table, exc)
        return 0
    finally:
        cur.close()


def get_non_null_count(conn, table: str, column: str) -> int:
    """获取某列的非 NULL 行数。"""
    cur = conn.cursor()
    try:
        cur.execute(
            f'SELECT COUNT(*) FROM {table} WHERE "{column}" IS NOT NULL'
        )
        return cur.fetchone()[0]
    except Exception as exc:
        logger.warning("    %s.%s 查询失败: %s", table, column, exc)
        return -1
    finally:
        cur.close()


def analyze_table(conn, table: str, row_cnt: int) -> list[dict]:
    """分析单个表中各列的空值情况。"""
    columns = get_table_columns(conn, table)
    empty_columns = []

    for col in columns:
        col_name = col["name"]

        if col_name in SYSTEM_COLUMNS:
            continue

        non_null = get_non_null_count(conn, table, col_name)
        if non_null < 0:
            continue

        null_cnt = row_cnt - non_null
        null_pct = (null_cnt / row_cnt * 100) if row_cnt > 0 else 0

        if non_null == 0:
            empty_columns.append({
                "table": table,
                "column": col_name,
                "type": col["type"],
                "total_rows": row_cnt,
                "non_null": non_null,
                "null_pct": null_pct,
            })

    return empty_columns


def main():
    drop_mode = "--drop" in sys.argv

    conn = get_pg_connection()
    try:
        tables = get_tushare_tables(conn)
        logger.info("发现 %d 个 tushare 表", len(tables))

        all_empty: list[dict] = []
        table_rows: dict[str, int] = {}

        # 先获取所有表的行数
        for table in tables:
            row_cnt = get_row_count(conn, table)
            table_rows[table] = row_cnt
            logger.info("  表 %-30s  %s 行", table, f"{row_cnt:,}" if row_cnt > 0 else "空")

        logger.info("\n" + "=" * 70)
        logger.info("开始逐列分析空值...")
        logger.info("=" * 70)

        for table in tables:
            row_cnt = table_rows[table]
            if row_cnt == 0:
                logger.info("  跳过空表: %s", table)
                continue

            empty_cols = analyze_table(conn, table, row_cnt)
            if empty_cols:
                all_empty.extend(empty_cols)
                for ec in empty_cols:
                    logger.warning(
                        "  [空] %-30s %-25s  %s rows",
                        table, ec["column"], f"{row_cnt:,}",
                    )

        # ── 汇总 ──
        logger.info("\n" + "=" * 70)
        logger.info("空字段分析结果")
        logger.info("=" * 70)

        if not all_empty:
            logger.info("所有表的所有字段均有数据，无需清理。")
            return

        # 分类：可删除 vs 建议保留
        can_drop: list[dict] = []
        should_preserve: list[dict] = []

        for ec in all_empty:
            key = ec["column"]
            if key in PRESERVE_EMPTY_COLUMNS:
                should_preserve.append(ec)
            else:
                can_drop.append(ec)

        if should_preserve:
            logger.info("\n--- 全空但建议保留（有业务含义） ---")
            for ec in should_preserve:
                reason = PRESERVE_EMPTY_COLUMNS.get(ec["column"], "")
                logger.info(
                    "  %-30s %-25s | %s",
                    ec["table"], ec["column"], reason,
                )

        if can_drop:
            logger.info("\n--- 可删除的全空字段 ---")
            for ec in can_drop:
                logger.info(
                    "  %-30s %-25s (%s)",
                    ec["table"], ec["column"], ec["type"],
                )

            # 生成 DROP 语句
            logger.info("\n--- 生成的 DROP COLUMN SQL ---")
            drop_sqls = defaultdict(list)
            for ec in can_drop:
                drop_sqls[ec["table"]].append(ec["column"])

            for table, columns in drop_sqls.items():
                for col in columns:
                    sql = f'ALTER TABLE {table} DROP COLUMN IF EXISTS "{col}";'
                    logger.info("  %s", sql)

            if drop_mode:
                logger.info("\n--- 执行 DROP COLUMN ---")
                cur = conn.cursor()
                try:
                    for table, columns in drop_sqls.items():
                        for col in columns:
                            sql = f'ALTER TABLE {table} DROP COLUMN IF EXISTS "{col}";'
                            try:
                                cur.execute(sql)
                                conn.commit()
                                logger.info("  已删除: %s.%s", table, col)
                            except Exception as exc:
                                conn.rollback()
                                logger.error("  删除失败 %s.%s: %s", table, col, exc)
                finally:
                    cur.close()
                logger.info("\n删除操作完成。")
            else:
                logger.info("\n提示: 使用 --drop 参数自动执行以上 DROP 语句")
        else:
            logger.info("\n无可删除的空字段（所有全空字段均在保留名单中）。")

    except Exception as exc:
        logger.error("执行失败: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        conn.close()
    logger.info("done!")


if __name__ == "__main__":
    main()
