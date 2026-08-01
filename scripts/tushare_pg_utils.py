# -*- coding: utf-8 -*-
"""
Tushare → PostgreSQL 共享工具模块

提供 Tushare API 客户端、PostgreSQL 连接、数据插入等公共功能。
供 scripts/db.py、scripts/ts2pg.py、scripts/fin_supplement.py 等脚本复用。

数据库连接参数通过环境变量配置（.env）：
  PG_HOST     - PostgreSQL 主机地址（默认 127.0.0.1）
  PG_PORT     - PostgreSQL 端口（默认 5432）
  PG_USER     - PostgreSQL 用户名（默认 root）
  PG_PASSWORD - PostgreSQL 密码（必填）
  PG_DBNAME   - PostgreSQL 数据库名（默认 tushare）
"""

import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN", "").strip()
TUSHARE_API_URL = os.getenv("TUSHARE_HTTP_URL", "http://api.tushare.pro").strip()
if not TUSHARE_API_URL.startswith("http"):
    TUSHARE_API_URL = "http://api.tushare.pro"

# 数据库连接参数（从环境变量读取，不硬编码）
PG_CONFIG = {
    "host": os.getenv("PG_HOST", "127.0.0.1"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "user": os.getenv("PG_USER", "root"),
    "password": os.getenv("PG_PASSWORD", ""),
    "dbname": os.getenv("PG_DBNAME", "tushare"),
}

logger = logging.getLogger("tushare_pg_utils")


class TushareClient:
    """Tushare API 客户端，内置简单速率限制。

    Attributes:
        token: Tushare API token。
        url: Tushare API 地址。
        rate_limit: 每分钟最大请求数。
    """

    def __init__(self, token: str, url: str = TUSHARE_API_URL, rate_limit: int = 480):
        self.token = token
        self.url = url
        self.rate_limit = rate_limit
        self._call_count = 0
        self._minute_start: float | None = None

    def _rate_limit_wait(self):
        """简单时间窗口速率限制。"""
        now = time.time()
        if self._minute_start is None:
            self._minute_start = now
            self._call_count = 0
        elif now - self._minute_start >= 60:
            self._minute_start = now
            self._call_count = 0

        if self._call_count >= self.rate_limit:
            wait_sec = max(0, 60 - (now - self._minute_start)) + 2
            logger.warning("速率限制触发，等待 %.0f 秒", wait_sec)
            time.sleep(wait_sec)
            self._minute_start = time.time()
            self._call_count = 0
        self._call_count += 1

    def query(self, api_name: str, fields: str = "", **kwargs) -> pd.DataFrame:
        """调用 Tushare API 并返回 DataFrame。

        Args:
            api_name: Tushare API 名称（如 daily_basic、income）。
            fields: 逗号分隔的字段列表。
            **kwargs: API 参数（如 ts_code、trade_date 等）。

        Returns:
            pandas DataFrame，失败时返回空 DataFrame 并记录错误日志。
        """
        self._rate_limit_wait()
        payload = {
            "api_name": api_name,
            "token": self.token,
            "params": kwargs,
            "fields": fields,
        }

        for attempt in range(3):
            try:
                resp = requests.post(self.url, json=payload, timeout=90)
                if resp.status_code != 200:
                    raise Exception(f"HTTP {resp.status_code}")

                result = json.loads(resp.text)
                if result.get("code") != 0:
                    raise Exception(result.get("msg") or f"code={result.get('code')}")

                data = result.get("data") or {}
                items = data.get("items") or []
                columns = data.get("fields") or []
                return pd.DataFrame(items, columns=columns)

            except Exception as exc:
                logger.warning("[%s] API 调用失败 (重试 %d/3): %s", api_name, attempt + 1, exc)
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    logger.error("[%s] API 调用最终失败，返回空 DataFrame", api_name)
                    return pd.DataFrame()


def get_pg_connection():
    """获取 PostgreSQL 连接。

    Returns:
        psycopg2 connection 对象。

    Raises:
        ValueError: PG_PASSWORD 未设置时抛出。
    """
    if not PG_CONFIG["password"]:
        raise ValueError(
            "PG_PASSWORD 环境变量未设置，请在 .env 文件中配置数据库密码"
        )
    return psycopg2.connect(**PG_CONFIG)


def table_exists(conn, table_name: str) -> bool:
    """检查表是否存在。"""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s)",
            (table_name,),
        )
        return cur.fetchone()[0]
    finally:
        cur.close()


def row_count(conn, table_name: str) -> int:
    """查询表行数，表不存在时返回 0。"""
    if not table_exists(conn, table_name):
        return 0
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cur.fetchone()[0]
    except Exception:
        return 0
    finally:
        cur.close()


def execute_sql_file(conn, filepath: str):
    """执行 SQL 文件。"""
    logger.info("执行 SQL 文件: %s", filepath)
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
        logger.info("SQL 执行成功")
    except Exception as exc:
        conn.rollback()
        logger.error("SQL 执行失败: %s", exc)
        raise
    finally:
        cur.close()


def insert_dataframe(
    conn,
    table_name: str,
    df: pd.DataFrame,
    conflict_clause: str = "",
    batch_size: int = 500,
) -> int:
    """将 DataFrame 插入 PostgreSQL 表。

    - 自动匹配 DataFrame 列与表列
    - 自动转换日期列（_date / cal_date / update_date / setup_date 结尾）
    - 支持 ON CONFLICT 子句（避免重复插入）
    - 批量插入失败时自动降级为逐行插入

    Args:
        conn: psycopg2 连接。
        table_name: 目标表名。
        df: 要插入的 DataFrame。
        conflict_clause: ON CONFLICT 子句参数（如 "(ts_code, trade_date)"）。
        batch_size: 批量插入的 page_size。

    Returns:
        成功插入的行数。
    """
    if df.empty:
        return 0

    # 列名统一小写
    df.columns = [col.lower() for col in df.columns]

    # 获取表中实际存在的列
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name=%s AND table_schema='public'",
            (table_name,),
        )
        valid_columns = {row[0] for row in cur.fetchall()}
    finally:
        cur.close()

    cols = [col for col in df.columns if col in valid_columns]
    if not cols:
        return 0

    df = df[cols].copy()

    # 日期列转换
    date_column_suffixes = ("_date",)
    date_column_names = ("cal_date", "update_date", "setup_date")
    for col in cols:
        if col.endswith(date_column_suffixes) or col in date_column_names:
            df[col] = df[col].apply(_parse_date)

    # NaN → None
    df = df.where(pd.notnull(df), None)

    records = [tuple(row) for row in df.values]
    if not records:
        return 0

    col_sql = ", ".join(f'"{col}"' for col in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    on_conflict = f" ON CONFLICT {conflict_clause} DO NOTHING" if conflict_clause else ""
    sql = f"INSERT INTO {table_name} ({col_sql}) VALUES %s{on_conflict}"

    cur = conn.cursor()
    try:
        psycopg2.extras.execute_values(
            cur, sql, records, template=f"({placeholders})", page_size=batch_size
        )
        conn.commit()
        return len(records)
    except Exception as exc:
        conn.rollback()
        logger.error("批量插入 %s 失败: %s，降级为逐行插入", table_name, exc)

        # 逐行回退
        row_sql = (
            f"INSERT INTO {table_name} ({col_sql}) VALUES ({placeholders}){on_conflict}"
        )
        ok = 0
        for row in records:
            try:
                cur.execute(row_sql, row)
                conn.commit()
                ok += 1
            except Exception:
                conn.rollback()
        logger.warning("  %s 逐行插入: %d/%d", table_name, ok, len(records))
        return ok
    finally:
        cur.close()


def _parse_date(value):
    """将 YYYYMMDD 格式的字符串/数值转换为 datetime.date。"""
    if pd.isna(value):
        return None
    val_str = str(value).strip()
    if not val_str or val_str == "None":
        return None
    try:
        return datetime.strptime(val_str[:10], "%Y%m%d").date()
    except (ValueError, TypeError):
        return None
