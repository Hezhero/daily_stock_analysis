# -*- coding: utf-8 -*-
"""
筹码分布（CYQ）数据采集脚本

数据来源：akshare.stock_cyq_em()（东方财富，A 股专属接口）
目标表：akshare_cyq（PostgreSQL，tushare 库）

需求要点:
  1. 自动连接本地 PostgreSQL（tushare 库），幂等创建筹码分布表；
  2. 接口入参股票代码取 tushare_stock_basic.symbol（6 位纯数字，如 600519），
     入库使用 tushare_stock_basic.ts_code（Tushare 格式，如 600519.SH）；
  3. 接口入参 adjust 固定传空字符串 ""，获取不复权数据。

接口契约（akshare 源码 stock_cyq_em）:
  stock_cyq_em(symbol="000001", adjust="")
    - symbol: 6 位纯数字股票代码
    - adjust: {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
  - 返回最近 90 个交易日筹码分布，列：
    日期 / 获利比例 / 平均成本 / 90成本-低 / 90成本-高 / 90集中度
    / 70成本-低 / 70成本-高 / 70集中度

容错:
  - 单只股票接口失败不中断整体流程，记录日志后继续
  - ON CONFLICT (ts_code, trade_date) DO NOTHING 幂等，可重复执行
  - 支持 --limit / --symbol / --sleep 便于测试与分批执行
  - .env 中 ENABLE_EASTMONEY_PATCH=true 时自动启用东财补丁（随机 UA + NID），
    接口调用带指数退避重试，缓解 RemoteDisconnected 等限流断连

用法:
  python scripts/data_collection/incremental_cyq.py                # 全量上市股票
  python scripts/data_collection/incremental_cyq.py --limit 100    # 前 100 只（测试）
  python scripts/data_collection/incremental_cyq.py --symbol 600519
  python scripts/data_collection/incremental_cyq.py --sleep 1.0    # 请求间隔秒数
"""

import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from tushare_pg_utils import (
    PROJECT_ROOT,
    get_pg_connection,
    insert_dataframe,
    retry_call,
    table_exists,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("inc_cyq")

# 目标表名
TABLE = "akshare_cyq"

# akshare 返回列 → 表列名映射
COLUMN_MAP = {
    "日期": "trade_date",
    "获利比例": "profit_ratio",
    "平均成本": "avg_cost",
    "90成本-低": "cost_90_low",
    "90成本-高": "cost_90_high",
    "90集中度": "concentration_90",
    "70成本-低": "cost_70_low",
    "70成本-高": "cost_70_high",
    "70集中度": "concentration_70",
}

# 入库列顺序
INSERT_COLS = [
    "ts_code", "trade_date", "profit_ratio", "avg_cost",
    "cost_90_low", "cost_90_high", "concentration_90",
    "cost_70_low", "cost_70_high", "concentration_70",
]

# 建表 DDL（与 docs/tushare_postgres_schema.sql 保持一致，脚本内幂等建表保证独立可运行）
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    id                  BIGSERIAL PRIMARY KEY,
    ts_code             VARCHAR(12)  NOT NULL,   -- 股票代码（Tushare 格式，如 600519.SH）
    trade_date          DATE         NOT NULL,   -- 交易日期
    profit_ratio        NUMERIC(12,6),           -- 获利比例（0~1 小数）
    avg_cost            NUMERIC(14,4),           -- 平均成本
    cost_90_low         NUMERIC(14,4),           -- 90成本-低
    cost_90_high        NUMERIC(14,4),           -- 90成本-高
    concentration_90    NUMERIC(12,6),           -- 90集中度（0~1 小数）
    cost_70_low         NUMERIC(14,4),           -- 70成本-低
    cost_70_high        NUMERIC(14,4),           -- 70成本-高
    concentration_70    NUMERIC(12,6),           -- 70集中度（0~1 小数）
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT uix_ts_cyq_code_date UNIQUE (ts_code, trade_date)
);

COMMENT ON TABLE {TABLE} IS '筹码分布缓存（akshare stock_cyq_em，东方财富）';

COMMENT ON COLUMN {TABLE}.id IS '自增主键';
COMMENT ON COLUMN {TABLE}.ts_code IS '股票代码（Tushare 格式，如 600519.SH）';
COMMENT ON COLUMN {TABLE}.trade_date IS '交易日期';
COMMENT ON COLUMN {TABLE}.profit_ratio IS '获利比例（0~1 小数）';
COMMENT ON COLUMN {TABLE}.avg_cost IS '平均成本';
COMMENT ON COLUMN {TABLE}.cost_90_low IS '90成本-低';
COMMENT ON COLUMN {TABLE}.cost_90_high IS '90成本-高';
COMMENT ON COLUMN {TABLE}.concentration_90 IS '90集中度（0~1 小数）';
COMMENT ON COLUMN {TABLE}.cost_70_low IS '70成本-低';
COMMENT ON COLUMN {TABLE}.cost_70_high IS '70成本-高';
COMMENT ON COLUMN {TABLE}.concentration_70 IS '70集中度（0~1 小数）';
COMMENT ON COLUMN {TABLE}.created_at IS '记录创建时间';
COMMENT ON COLUMN {TABLE}.updated_at IS '记录最近更新时间';

CREATE INDEX IF NOT EXISTS ix_ts_cyq_code ON {TABLE}(ts_code);
CREATE INDEX IF NOT EXISTS ix_ts_cyq_date ON {TABLE}(trade_date);
"""

# updated_at 触发器（仅当公共函数存在时注册，幂等）
CREATE_TRIGGER_SQL = f"""
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'fn_tushare_updated_at') THEN
        DROP TRIGGER IF EXISTS trg_{TABLE}_updated_at ON {TABLE};
        CREATE TRIGGER trg_{TABLE}_updated_at
            BEFORE UPDATE ON {TABLE}
            FOR EACH ROW
            EXECUTE FUNCTION fn_tushare_updated_at();
    END IF;
END $$;
"""


def ensure_cyq_table(conn) -> bool:
    """幂等创建 akshare_cyq 表及索引、触发器。返回是否新创建。"""
    existed = table_exists(conn, TABLE)
    cur = conn.cursor()
    try:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(CREATE_TRIGGER_SQL)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        logger.error("建表 %s 失败: %s", TABLE, exc)
        raise
    finally:
        cur.close()
    if not existed:
        logger.info("已创建表 %s", TABLE)
    else:
        logger.info("表 %s 已存在，跳过建表", TABLE)
    return not existed


def get_stock_list(conn) -> list[tuple[str, str]]:
    """从 tushare_stock_basic 读取上市股票 (symbol, ts_code) 列表。

    symbol 为 6 位纯数字（接口入参），ts_code 为 Tushare 格式（入库用）。
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT symbol, ts_code FROM tushare_stock_basic "
            "WHERE list_status='L' ORDER BY ts_code"
        )
        rows = [(str(sym), str(code)) for sym, code in cur.fetchall()]
        return rows
    finally:
        cur.close()


def _apply_eastmoney_patch() -> None:
    """按 .env 配置启用东财补丁（随机 User-Agent + NID 令牌）。

    stock_cyq_em 内部为无 UA 裸请求，开启补丁可显著降低
    RemoteDisconnected / 连接被关闭类限流问题；补丁幂等，失败仅告警不阻断。
    """
    if os.getenv("ENABLE_EASTMONEY_PATCH", "false").lower() != "true":
        return
    try:
        if PROJECT_ROOT not in sys.path:
            sys.path.insert(0, PROJECT_ROOT)
        from src.patches.eastmoney_patch import eastmoney_patch

        eastmoney_patch()
        logger.info("已启用东财补丁（随机 User-Agent + NID），降低被限流概率")
    except Exception as exc:
        logger.warning("启用东财补丁失败，继续使用裸请求: %s", exc)


@retry_call(max_attempts=3, base_delay=2.0, backoff=2.0)
def fetch_cyq(symbol: str) -> pd.DataFrame:
    """调用 akshare stock_cyq_em 获取不复权筹码分布数据。

    Args:
        symbol: 6 位纯数字股票代码（如 "600519"）。

    Returns:
        DataFrame（列已映射为英文表列名），失败返回空 DataFrame。
    """
    try:
        import akshare as ak
    except ImportError as exc:
        logger.error("未安装 akshare，请先执行: pip install akshare")
        raise exc

    logger.info("[API调用] ak.stock_cyq_em(symbol=%s, adjust=\"\") 不复权...", symbol)
    df = ak.stock_cyq_em(symbol=symbol, adjust="")  # adjust 固定传空 = 不复权
    if df is None or df.empty:
        logger.warning("[API返回] %s 返回空数据", symbol)
        return pd.DataFrame()

    df = df.rename(columns=COLUMN_MAP)
    return df


def save_cyq(conn, ts_code: str, df: pd.DataFrame) -> int:
    """将单只股票筹码数据写入 akshare_cyq 表（幂等）。"""
    if df.empty:
        return 0
    df = df.copy()
    df["ts_code"] = ts_code
    # 日期转 YYYYMMDD 字符串，兼容 tushare_pg_utils._parse_date
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")
    df = df[INSERT_COLS]
    return insert_dataframe(
        conn, TABLE, df,
        conflict_clause="(ts_code, trade_date)",
    )


def main():
    parser = argparse.ArgumentParser(description="筹码分布（CYQ）数据采集")
    parser.add_argument("--limit", type=int, default=0, help="最多处理的股票数量（0=全部）")
    parser.add_argument("--symbol", type=str, default="", help="指定单只股票 6 位代码（如 600519）")
    parser.add_argument("--sleep", type=float, default=0.5, help="每次请求间隔秒数（默认 0.5）")
    args = parser.parse_args()

    # 需在首次调用 akshare 之前启用（tushare_pg_utils 已加载 .env）
    _apply_eastmoney_patch()

    conn = get_pg_connection()
    try:
        ensure_cyq_table(conn)

        if args.symbol:
            # 指定单只：按 symbol 查 ts_code
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT symbol, ts_code FROM tushare_stock_basic "
                    "WHERE list_status='L' AND symbol=%s",
                    (args.symbol,),
                )
                rows = [(str(s), str(t)) for s, t in cur.fetchall()]
            finally:
                cur.close()
            if not rows:
                logger.error("未在 tushare_stock_basic 找到 symbol=%s", args.symbol)
                sys.exit(1)
        else:
            rows = get_stock_list(conn)

        if args.limit > 0:
            rows = rows[:args.limit]

        logger.info("待处理股票: %d 只", len(rows))
        if not rows:
            logger.info("无待处理股票，退出")
            return

        ok = 0
        fail = 0
        total_rows = 0
        t0 = time.time()
        for i, (symbol, ts_code) in enumerate(rows, start=1):
            try:
                df = fetch_cyq(symbol)
                n = save_cyq(conn, ts_code, df)
                if n > 0:
                    ok += 1
                    total_rows += n
                    logger.info(
                        "[%d/%d] %s: 入库 %d 行", i, len(rows), ts_code, n
                    )
                else:
                    fail += 1
                    logger.warning("[%d/%d] %s: 无数据写入", i, len(rows), ts_code)
            except Exception as exc:
                fail += 1
                logger.warning("[%d/%d] %s: 处理失败: %s", i, len(rows), ts_code, exc)

            if args.sleep > 0:
                time.sleep(args.sleep)

        elapsed = time.time() - t0
        logger.info("=== 完成: 成功 %d 只 / 失败 %d 只, 共入库 %s 行, 耗时 %.0f 秒 ===",
                    ok, fail, f"{total_rows:,}", elapsed)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
