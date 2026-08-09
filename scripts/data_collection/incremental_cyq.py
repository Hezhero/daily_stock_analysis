# -*- coding: utf-8 -*-
"""
筹码分布（CYQ）数据采集脚本（本地计算版）

数据来源：本地 PostgreSQL 的 tushare_daily（日线 OHLCV）与 tushare_daily_basic（换手率）
目标表：tushare_cyq（PostgreSQL，tushare 库）

需求要点:
  1. 自动连接本地 PostgreSQL（tushare 库），幂等创建筹码分布表 tushare_cyq；
  2. 股票代码取 tushare_stock_basic.symbol / ts_code（Tushare 格式，如 600519.SH）；
  3. 使用三角形分布法在本地计算筹码分布：
     - 每个交易日以 [low, high] 为区间、((low+high+close)/3) 为峰值，将当日成交量
       按三角形权重分配到离散价格档位；
     - 历史筹码按换手率逐日衰减（1 - turnover_rate），形成累计筹码分布；
     - 基于累计筹码分布计算获利比例、平均成本、90/70 成本区间与集中度。

算法说明:
  三角形分布法是筹码分布的常见近似（东财/通达信同源思路），仅需
  low/high/close/vol 即可建模，配合 daily_basic 的换手率衰减历史筹码。

容错:
  - 单只股票计算失败不中断整体流程，记录日志后继续
  - ON CONFLICT (ts_code, trade_date) DO NOTHING 幂等，可重复执行
  - 支持 --limit / --symbol / --days / --sleep 便于测试与分批执行

用法:
  python scripts/data_collection/incremental_cyq.py                # 全量上市股票
  python scripts/data_collection/incremental_cyq.py --limit 100    # 前 100 只（测试）
  python scripts/data_collection/incremental_cyq.py --symbol 600519
  python scripts/data_collection/incremental_cyq.py --days 250     # 计算窗口天数
  python scripts/data_collection/incremental_cyq.py --sleep 0.2    # 每只股票处理间隔秒数
"""

import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd

from tushare_pg_utils import (
    PROJECT_ROOT,
    get_pg_connection,
    insert_dataframe,
    table_exists,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("inc_cyq")

# 目标表名
TABLE = "tushare_cyq"

# 价格档位数（三角形分布离散化的粒度）
PRICE_BINS = 100

# 建表 DDL（与 docs/tushare_postgres_schema.sql 2.4 保持一致，脚本内幂等建表保证独立可运行）
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

    CONSTRAINT uix_tushare_cyq_code_date UNIQUE (ts_code, trade_date)
);

COMMENT ON TABLE {TABLE} IS '筹码分布缓存（本地计算：三角形分布法，基于 tushare_daily + tushare_daily_basic）';

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
    """幂等创建 tushare_cyq 表及索引、触发器。返回是否新创建。"""
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

    symbol 为 6 位纯数字，ts_code 为 Tushare 格式（入库用）。
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


def load_daily_data(conn, ts_code: str, days: int) -> pd.DataFrame:
    """从 tushare_daily 读取最近 N 个交易日的日线数据（升序）。

    Args:
        conn: psycopg2 连接。
        ts_code: Tushare 格式股票代码（如 600519.SH）。
        days: 计算窗口天数（最近 N 个交易日）。

    Returns:
        DataFrame（ts_code/trade_date/open/high/low/close/vol），按 trade_date 升序。
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT ts_code, trade_date, open, high, low, close, vol "
            "FROM tushare_daily "
            "WHERE ts_code=%s "
            "ORDER BY trade_date DESC LIMIT %s",
            (ts_code, days),
        )
        rows = cur.fetchall()
        df = pd.DataFrame(
            rows,
            columns=["ts_code", "trade_date", "open", "high", "low", "close", "vol"],
        )
    finally:
        cur.close()
    if df.empty:
        return df
    df = df.sort_values("trade_date").reset_index(drop=True)
    return df


def load_daily_basic(conn, ts_code: str, days: int) -> pd.DataFrame:
    """从 tushare_daily_basic 读取最近 N 个交易日的换手率数据（升序）。

    Args:
        conn: psycopg2 连接。
        ts_code: Tushare 格式股票代码。
        days: 计算窗口天数。

    Returns:
        DataFrame: ts_code/trade_date/turnover_rate/float_share，按 trade_date 升序。
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT ts_code, trade_date, turnover_rate, float_share "
            "FROM tushare_daily_basic "
            "WHERE ts_code=%s "
            "ORDER BY trade_date DESC LIMIT %s",
            (ts_code, days),
        )
        rows = cur.fetchall()
        df = pd.DataFrame(
            rows,
            columns=["ts_code", "trade_date", "turnover_rate", "float_share"],
        )
    finally:
        cur.close()
    if df.empty:
        return df
    df = df.sort_values("trade_date").reset_index(drop=True)
    return df


def compute_chip_distribution(daily_df: pd.DataFrame, basic_df: pd.DataFrame) -> dict:
    """基于 tushare_daily 与 tushare_daily_basic 本地计算筹码分布（纯函数，无 IO）。

    算法（三角形分布法）:
      1. 对每个交易日，在 [low, high] 区间上以 (low+high+close)/3 为峰值做三角形分布，
         将该日成交量按三角形权重分配到离散价格档位（PRICE_BINS 档）；
      2. 历史筹码按换手率逐日衰减：decay = 1 - turnover_rate/100；
         累计筹码 chips[t] = 当日分配 + decay * 前日累计；
         某日 basic 缺失时 decay 按 1.0 处理（不衰减）；
      3. 基于累计筹码分布计算：
         - profit_ratio: 价格 <= 当日收盘价 close 的筹码占比（0~1）
         - avg_cost: 筹码量加权平均价格
         - cost_90_low/high: 累积占比 5%/95% 分位价格；concentration_90 = (high-low)/(high+low)
         - cost_70_low/high: 累积占比 15%/85% 分位价格；concentration_70 = (high-low)/(high+low)

    Args:
        daily_df: 日线 DataFrame（ts_code/trade_date/open/high/low/close/vol），升序。
        basic_df: 每日基本面 DataFrame（ts_code/trade_date/turnover_rate/float_share），升序。

    Returns:
        dict: trade_date/profit_ratio/avg_cost/cost_90_low/cost_90_high/concentration_90
              /cost_70_low/cost_70_high/concentration_70。
              空输入返回空 dict。
    """
    if daily_df is None or daily_df.empty:
        return {}

    df = daily_df.copy()
    df = df.sort_values("trade_date").reset_index(drop=True)

    # 合并换手率（basic 缺失时 turnover_rate=NaN → 衰减按 1.0）
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    if basic_df is not None and not basic_df.empty:
        basic = basic_df.copy()
        basic["trade_date"] = pd.to_datetime(basic["trade_date"])
        df = df.merge(
            basic[["trade_date", "turnover_rate"]],
            on="trade_date",
            how="left",
        )
        turnover = df["turnover_rate"].fillna(0.0).astype(float)
    else:
        turnover = pd.Series([0.0] * len(df), index=df.index)

    # 构造价格档位：覆盖窗口内 [min(low), max(high)]
    low_min = float(df["low"].min())
    high_max = float(df["high"].max())
    if not np.isfinite(low_min) or not np.isfinite(high_max) or high_max <= low_min:
        # 单日无振幅或数据异常：退化为单点分布（构造一个宽度为 1 的档位）
        price = float(df["close"].iloc[-1])
        bins = np.array([price - 0.5, price + 0.5])
        bin_width = 1.0
    else:
        bins = np.linspace(low_min, high_max, PRICE_BINS + 1)
        bin_width = bins[1] - bins[0]

    # 逐日三角形分布累计
    chips = np.zeros(len(bins) - 1, dtype=float)
    for i in range(len(df)):
        row = df.iloc[i]
        low = float(row["low"])
        high = float(row["high"])
        close = float(row["close"])
        vol = float(row["vol"]) if pd.notna(row["vol"]) else 0.0
        if vol <= 0 or high < low:
            continue

        # 三角形分布：峰值在 peak = (low+high+close)/3
        peak = (low + high + close) / 3.0

        # 三角形概率密度：在 [low, peak] 线性上升，[peak, high] 线性下降
        # 对每个价格档的中心点计算三角形权重
        centers = (bins[:-1] + bins[1:]) / 2.0
        weights = np.zeros(len(centers), dtype=float)
        if high > low:
            # 归一化三角形：总面积 = (high - low) / 2 * peak_height，peak_height = 2/(high-low)
            # 权重 = 三角形高度函数 * 档宽
            for j, c in enumerate(centers):
                if c < low or c > high:
                    continue
                if c <= peak:
                    h = (c - low) / (peak - low) if peak > low else 1.0
                else:
                    h = (high - c) / (high - peak) if high > peak else 1.0
                weights[j] = h * bin_width
            # 归一化到当日成交量（三角形面积近似 = sum(weights)）
            total_w = weights.sum()
            if total_w > 0:
                weights = weights / total_w * vol
        else:
            # high == low：全部筹码落在该价格档
            idx = int(np.searchsorted(bins, low, side="right") - 1)
            idx = max(0, min(idx, len(weights) - 1))
            weights[idx] = vol

        # 历史筹码衰减
        decay = 1.0 - turnover.iloc[i] / 100.0 if i > 0 else 0.0
        decay = max(0.0, min(1.0, decay))
        chips = chips * decay + weights

    total_chips = chips.sum()
    if total_chips <= 0:
        return {}

    # 各档位价格（取档位中心）
    centers = (bins[:-1] + bins[1:]) / 2.0
    # 当日收盘价（目标交易日 = 最新交易日）
    latest_close = float(df["close"].iloc[-1])
    latest_date = df["trade_date"].iloc[-1]

    # 排序（价格升序）
    order = np.argsort(centers)
    sorted_prices = centers[order]
    sorted_chips = chips[order]

    cum = np.cumsum(sorted_chips)
    cum_ratio = cum / total_chips

    # 获利比例：价格 <= 收盘价的筹码占比
    profit_ratio = float(sorted_chips[sorted_prices <= latest_close].sum() / total_chips)

    # 平均成本：加权平均价格
    avg_cost = float((sorted_prices * sorted_chips).sum() / total_chips)

    def _percentile_price(target_pct: float) -> float:
        """返回累积占比达到 target_pct 的分位价格。"""
        idx = int(np.searchsorted(cum_ratio, target_pct / 100.0, side="left"))
        idx = min(idx, len(sorted_prices) - 1)
        return float(sorted_prices[idx])

    def _concentration(low_p, high_p) -> float:
        if (high_p + low_p) == 0:
            return 0.0
        return float((high_p - low_p) / (high_p + low_p))

    cost_90_low = _percentile_price(5)
    cost_90_high = _percentile_price(95)
    cost_70_low = _percentile_price(15)
    cost_70_high = _percentile_price(85)

    try:
        latest_date_str = latest_date.strftime("%Y-%m-%d") if hasattr(latest_date, "strftime") else str(latest_date)
    except Exception:
        latest_date_str = str(latest_date)

    return {
        "trade_date": latest_date_str,
        "profit_ratio": round(profit_ratio, 6),
        "avg_cost": round(avg_cost, 4),
        "cost_90_low": round(cost_90_low, 4),
        "cost_90_high": round(cost_90_high, 4),
        "concentration_90": round(_concentration(cost_90_low, cost_90_high), 6),
        "cost_70_low": round(cost_70_low, 4),
        "cost_70_high": round(cost_70_high, 4),
        "concentration_70": round(_concentration(cost_70_low, cost_70_high), 6),
    }


def save_cyq(conn, ts_code: str, metrics: dict) -> int:
    """将单只股票筹码指标写入 tushare_cyq 表（幂等）。

    Args:
        conn: psycopg2 连接。
        ts_code: Tushare 格式股票代码。
        metrics: compute_chip_distribution 返回的指标 dict（含 trade_date）。

    Returns:
        写入行数（0 表示无数据写入）。
    """
    if not metrics:
        return 0
    df = pd.DataFrame([metrics])
    df["ts_code"] = ts_code
    # 日期转 YYYYMMDD 字符串，兼容 tushare_pg_utils._parse_date
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")
    insert_cols = [
        "ts_code", "trade_date", "profit_ratio", "avg_cost",
        "cost_90_low", "cost_90_high", "concentration_90",
        "cost_70_low", "cost_70_high", "concentration_70",
    ]
    df = df[insert_cols]
    return insert_dataframe(
        conn, TABLE, df,
        conflict_clause="(ts_code, trade_date)",
    )


def main():
    parser = argparse.ArgumentParser(description="筹码分布（CYQ）本地计算采集")
    parser.add_argument("--limit", type=int, default=0, help="最多处理的股票数量（0=全部）")
    parser.add_argument("--symbol", type=str, default="", help="指定单只股票 6 位代码（如 600519）")
    parser.add_argument("--days", type=int, default=100, help="计算窗口交易日数（默认 100）")
    parser.add_argument("--sleep", type=float, default=0.0, help="每只股票处理间隔秒数（默认 0）")
    args = parser.parse_args()

    if args.days < 10:
        logger.warning("--days=%d 过小，使用默认 100", args.days)
        args.days = 100

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
                daily_df = load_daily_data(conn, ts_code, args.days)
                if daily_df.empty:
                    logger.warning("[%d/%d] %s: tushare_daily 无数据", i, len(rows), ts_code)
                    fail += 1
                    continue
                basic_df = load_daily_basic(conn, ts_code, args.days)
                metrics = compute_chip_distribution(daily_df, basic_df)
                n = save_cyq(conn, ts_code, metrics)
                if n > 0:
                    ok += 1
                    total_rows += n
                    logger.info(
                        "[%d/%d] %s: 入库 %d 行 (trade_date=%s, profit_ratio=%.4f, avg_cost=%.2f)",
                        i, len(rows), ts_code, n,
                        metrics.get("trade_date", "?"),
                        metrics.get("profit_ratio", 0.0),
                        metrics.get("avg_cost", 0.0),
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