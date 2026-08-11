# -*- coding: utf-8 -*-
"""
筹码分布（CYQ）数据采集脚本（本地计算版，支持逐日全量/增量）

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
  4. 逐日计算：每个交易日一行指标写入 tushare_cyq（不再只写最新一天）；
  5. 增量更新：已有数据的股票从 start_date 全量重算（与全量回填共用同一价格档位
     网格与同一累积深度，保证结果一致），仅插入最新日期之后的新行。

算法说明:
  三角形分布法是筹码分布的常见近似（东财/通达信同源思路），仅需
  low/high/close/vol 即可建模，配合 daily_basic 的换手率衰减历史筹码。
  权重矩阵与指标计算向量化，仅衰减累积按日循环，性能满足全 A 股全量回填。

容错:
  - 单只股票计算失败不中断整体流程，记录日志后继续
  - ON CONFLICT (ts_code, trade_date) DO NOTHING 幂等，可重复执行
  - 支持 --limit / --symbol / --start-date / --end-date / --sleep
    便于测试与分批执行

用法:
  python scripts/data_collection/incremental_cyq.py                    # 全量上市股票（2016-01-01 至今，增量）
  python scripts/data_collection/incremental_cyq.py --limit 100        # 前 100 只（测试）
  python scripts/data_collection/incremental_cyq.py --symbol 600519
  python scripts/data_collection/incremental_cyq.py --start-date 20260101 --end-date 20261231
  python scripts/data_collection/incremental_cyq.py --sleep 0.2        # 每只股票处理间隔秒数
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime

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
# 采用对数间距档位：对 [min(low), max(high)] 全历史价格区间做对数等分，
# 保证任意价位下的相对分辨率恒定（10 年跨度价格可相差近 10 倍，
# 线性等分会导致早期低价区间每个档位过宽、三角形分布退化为单点）。
# 500 档下每档相对宽度约 0.5%~0.9%，足以覆盖单日 [low, high] 振幅。
PRICE_BINS = 500

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

# 逐日结果 DataFrame 的列（trade_date 为 YYYY-MM-DD 字符串）
DAILY_COLUMNS = [
    "trade_date",
    "profit_ratio",
    "avg_cost",
    "cost_90_low",
    "cost_90_high",
    "concentration_90",
    "cost_70_low",
    "cost_70_high",
    "concentration_70",
]


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


def load_daily_data(conn, ts_code: str, start_date=None, end_date=None) -> pd.DataFrame:
    """从 tushare_daily 读取指定日期范围的日线数据（升序）。

    Args:
        conn: psycopg2 连接。
        ts_code: Tushare 格式股票代码（如 600519.SH）。
        start_date: 起始日期（YYYYMMDD / YYYY-MM-DD 字符串或 date），None 表示不限。
        end_date: 结束日期（同上），None 表示不限。

    Returns:
        DataFrame（ts_code/trade_date/open/high/low/close/vol），按 trade_date 升序。
    """
    cur = conn.cursor()
    try:
        sql = (
            "SELECT ts_code, trade_date, open, high, low, close, vol "
            "FROM tushare_daily WHERE ts_code=%s"
        )
        params: list = [ts_code]
        if start_date is not None:
            sql += " AND trade_date >= %s"
            params.append(start_date)
        if end_date is not None:
            sql += " AND trade_date <= %s"
            params.append(end_date)
        sql += " ORDER BY trade_date"
        cur.execute(sql, params)
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


def get_full_price_range(conn, ts_code: str) -> tuple[float, float] | None:
    """查询 ts_code 全历史日线价格区间 (min(low), max(high))。

    用于固定筹码分布的价格档位网格，保证全量回填与增量计算共用同一网格。
    无有效数据时返回 None（调用方退化为当日区间）。
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MIN(low), MAX(high) FROM tushare_daily "
            "WHERE ts_code=%s AND low IS NOT NULL AND high IS NOT NULL",
            (ts_code,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
    if row is None or row[0] is None or row[1] is None:
        return None
    low_min, high_max = float(row[0]), float(row[1])
    if not (low_min > 0 and high_max > low_min):
        return None
    return (low_min, high_max)


def load_daily_basic(conn, ts_code: str, start_date=None, end_date=None) -> pd.DataFrame:
    """从 tushare_daily_basic 读取指定日期范围的换手率数据（升序）。

    Args:
        conn: psycopg2 连接。
        ts_code: Tushare 格式股票代码。
        start_date / end_date: 日期范围（同 load_daily_data）。

    Returns:
        DataFrame: ts_code/trade_date/turnover_rate/float_share，按 trade_date 升序。
    """
    cur = conn.cursor()
    try:
        sql = (
            "SELECT ts_code, trade_date, turnover_rate, float_share "
            "FROM tushare_daily_basic WHERE ts_code=%s"
        )
        params: list = [ts_code]
        if start_date is not None:
            sql += " AND trade_date >= %s"
            params.append(start_date)
        if end_date is not None:
            sql += " AND trade_date <= %s"
            params.append(end_date)
        sql += " ORDER BY trade_date"
        cur.execute(sql, params)
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


def get_latest_cyq_date(conn, ts_code: str):
    """查询 tushare_cyq 中该股票已有的最大 trade_date。

    Returns:
        datetime.date 或 None（无历史）。
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MAX(trade_date) FROM tushare_cyq WHERE ts_code=%s",
            (ts_code,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def get_earliest_cyq_date(conn, ts_code: str):
    """查询 tushare_cyq 中该股票已有的最小 trade_date。

    Returns:
        datetime.date 或 None（无历史）。
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MIN(trade_date) FROM tushare_cyq WHERE ts_code=%s",
            (ts_code,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def get_earliest_daily_date(conn, ts_code: str):
    """查询 tushare_daily 中该股票的最小 trade_date（用于判断历史是否缺失）。

    Returns:
        datetime.date 或 None（无日线数据）。
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MIN(trade_date) FROM tushare_daily WHERE ts_code=%s",
            (ts_code,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


def _compute_weights(lows, highs, closes, vols, bins) -> np.ndarray:
    """向量化计算逐日三角形权重矩阵 W: (N, BINS)。

    每个交易日以 (low+high+close)/3 为峰值做三角形分布，权重归一化到当日成交量；
    high==low 的交易日筹码全部落在该价格档；vol<=0 或 high<low 的交易日权重为 0。
    支持对数间距档位（各档宽度不同，按每档实际宽度加权）。
    """
    centers = (bins[:-1] + bins[1:]) / 2.0
    bin_widths = bins[1:] - bins[:-1]                            # 每档实际宽度
    n = len(lows)
    n_bins = len(centers)

    c = centers[None, :]                                   # (1, BINS)
    lo = lows[:, None]                                     # (N, 1)
    hi = highs[:, None]                                    # (N, 1)
    peak = ((lows + highs + closes) / 3.0)[:, None]        # (N, 1)
    vol = vols[:, None]                                    # (N, 1)

    in_range = (c >= lo) & (c <= hi)
    rise = (c <= peak) & (peak > lo)
    fall = (c > peak) & (hi > peak)

    h = np.zeros((n, n_bins), dtype=float)
    h = np.where(rise, (c - lo) / np.where(peak > lo, peak - lo, 1.0), h)
    h = np.where(fall, (hi - c) / np.where(hi > peak, hi - peak, 1.0), h)
    h = np.where(in_range, h, 0.0)

    w = h * bin_widths[None, :]
    row_sum = w.sum(axis=1, keepdims=True)
    w = np.where(row_sum > 0, w / np.where(row_sum > 0, row_sum, 1.0) * vol, 0.0)

    # high == low：全部筹码落在该价格档
    flat = highs == lows
    if flat.any():
        idx = np.searchsorted(bins, lows[flat], side="right") - 1
        idx = np.clip(idx, 0, n_bins - 1)
        w_flat = np.zeros((int(flat.sum()), n_bins), dtype=float)
        w_flat[np.arange(len(idx)), idx] = vols[flat]
        w[flat] = w_flat

    # 无效日（vol<=0 或 high<low）：权重置 0
    valid = (vols > 0) & (highs >= lows)
    w[~valid] = 0.0
    return w


def compute_chip_distribution_daily(
    daily_df: pd.DataFrame,
    basic_df: pd.DataFrame,
    price_range: tuple[float, float] | None = None,
) -> pd.DataFrame:
    """基于 tushare_daily 与 tushare_daily_basic 逐日计算筹码分布（纯函数，无 IO）。

    算法（三角形分布法）:
      1. 构造固定价格档位（PRICE_BINS 档）：
         - 传入 price_range=(low_min, high_max) 时使用该区间（保证全量回填与
           增量计算共用同一档位网格，结果一致）；
         - 未传时退化为当前 daily_df 的 [min(low), max(high)]；
         档位采用对数间距等分，保证任意价位下的相对分辨率恒定（线性等分在
         价格跨度近 10 倍时，早期低价区间档位过宽、三角形分布退化为单点）；
      2. 向量化计算逐日三角形权重矩阵 W (N, BINS)，归一化到当日成交量；
      3. 历史筹码按换手率逐日衰减：decay = 1 - turnover_rate/100；
         累计筹码 chips[t] = 当日分配 + decay * 前日累计；
         某日 basic 缺失时 decay 按 1.0 处理（不衰减）；vol<=0 / high<low 的交易日
         既不分配筹码也不衰减（与旧版单日逻辑一致）；
      4. 基于每日累计筹码分布计算：
         - profit_ratio: 价格 <= 当日收盘价 close 的筹码占比（0~1）
         - avg_cost: 筹码量加权平均价格
         - cost_90_low/high: 累积占比 5%/95% 分位价格；concentration_90 = (high-low)/(high+low)
         - cost_70_low/high: 累积占比 15%/85% 分位价格；concentration_70 = (high-low)/(high+low)

    Args:
        daily_df: 日线 DataFrame（ts_code/trade_date/open/high/low/close/vol），升序。
        basic_df: 每日基本面 DataFrame（ts_code/trade_date/turnover_rate/float_share），升序。
        price_range: 可选 (low_min, high_max)，用于固定价格档位网格。

    Returns:
        DataFrame: DAILY_COLUMNS 各列，每个交易日一行（trade_date 为 YYYY-MM-DD 字符串）。
        空输入返回空 DataFrame。
    """
    if daily_df is None or daily_df.empty:
        return pd.DataFrame(columns=DAILY_COLUMNS)

    df = daily_df.copy()
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    # 合并换手率（basic 缺失时 turnover_rate=NaN → 衰减按 1.0）
    if basic_df is not None and not basic_df.empty:
        basic = basic_df.copy()
        basic["trade_date"] = pd.to_datetime(basic["trade_date"])
        df = df.merge(
            basic[["trade_date", "turnover_rate"]],
            on="trade_date",
            how="left",
        )
        turnover = df["turnover_rate"].fillna(0.0).astype(float).to_numpy()
    else:
        turnover = np.zeros(len(df), dtype=float)

    lows = df["low"].to_numpy(dtype=float)
    highs = df["high"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    vols = np.nan_to_num(df["vol"].to_numpy(dtype=float), nan=0.0)

    # 构造价格档位：默认覆盖整个计算区间 [min(low), max(high)]；
    # 传入 price_range 时使用全历史价格区间（全量回填与增量共用同一网格）。
    # 对数间距等分：价格跨度可达近 10 倍，线性等分会让早期低价区间档位过宽、
    # 三角形分布退化为单点；对数等分保证相对分辨率恒定。
    if price_range is not None:
        low_min, high_max = float(price_range[0]), float(price_range[1])
    else:
        finite_lows = lows[np.isfinite(lows)]
        finite_highs = highs[np.isfinite(highs)]
        low_min = float(finite_lows.min()) if finite_lows.size else float("nan")
        high_max = float(finite_highs.max()) if finite_highs.size else float("nan")
    if not np.isfinite(low_min) or not np.isfinite(high_max) or high_max <= low_min or low_min <= 0:
        # 无有效振幅或数据异常：退化为单点分布（构造一个宽度为 1 的档位）
        price = float(closes[-1])
        bins = np.array([price - 0.5, price + 0.5])
    else:
        bins = np.exp(np.linspace(np.log(low_min), np.log(high_max), PRICE_BINS + 1))
    centers = (bins[:-1] + bins[1:]) / 2.0
    n_bins = len(centers)

    w = _compute_weights(lows, highs, closes, vols, bins)

    # 逐日衰减累积（仅此步按日循环，避免数值下溢；其余全部向量化）
    n = len(df)
    chips_mat = np.zeros((n, n_bins), dtype=float)
    chips = np.zeros(n_bins, dtype=float)
    invalid = ~((vols > 0) & (highs >= lows))
    for i in range(n):
        if i > 0 and not invalid[i]:
            decay = 1.0 - turnover[i] / 100.0
            decay = max(0.0, min(1.0, decay))
        else:
            decay = 0.0 if i == 0 else 1.0  # 首日无历史；无效日不衰减（与旧版 continue 一致）
        chips = chips * decay + w[i]
        chips_mat[i] = chips

    totals = chips_mat.sum(axis=1)

    # 获利比例：价格 <= 当日收盘价的筹码占比（centers 升序）
    close_pos = np.searchsorted(centers, closes, side="right")
    cum = np.cumsum(chips_mat, axis=1)
    profit = np.zeros(n, dtype=float)
    mask = close_pos > 0
    profit[mask] = cum[np.arange(n)[mask], close_pos[mask] - 1] / np.where(
        totals[mask] > 0, totals[mask], 1.0
    )
    profit = np.clip(profit, 0.0, 1.0)

    # 平均成本：筹码量加权平均价格
    safe_total = np.where(totals > 0, totals, 1.0)
    avg_cost = np.where(
        totals > 0,
        (chips_mat * centers[None, :]).sum(axis=1) / safe_total,
        0.0,
    )

    # 分位价格：累积占比达到 target 的档位价格
    cum_ratio = cum / safe_total[:, None]

    def _pct_price(target: float) -> np.ndarray:
        idx = np.argmax(cum_ratio >= target, axis=1)
        reached = cum_ratio[:, -1] >= target
        idx = np.where(reached, idx, n_bins - 1)
        return centers[idx]

    cost_90_low = _pct_price(0.05)
    cost_90_high = _pct_price(0.95)
    cost_70_low = _pct_price(0.15)
    cost_70_high = _pct_price(0.85)

    def _concentration(low_p: np.ndarray, high_p: np.ndarray) -> np.ndarray:
        denom = high_p + low_p
        return np.where(denom != 0, (high_p - low_p) / np.where(denom != 0, denom, 1.0), 0.0)

    result = pd.DataFrame(
        {
            "trade_date": df["trade_date"].dt.strftime("%Y-%m-%d"),
            "profit_ratio": np.round(profit, 6),
            "avg_cost": np.round(avg_cost, 4),
            "cost_90_low": np.round(cost_90_low, 4),
            "cost_90_high": np.round(cost_90_high, 4),
            "concentration_90": np.round(_concentration(cost_90_low, cost_90_high), 6),
            "cost_70_low": np.round(cost_70_low, 4),
            "cost_70_high": np.round(cost_70_high, 4),
            "concentration_70": np.round(_concentration(cost_70_low, cost_70_high), 6),
        }
    )
    return result[DAILY_COLUMNS]


def compute_chip_distribution(daily_df: pd.DataFrame, basic_df: pd.DataFrame) -> dict:
    """兼容包装：返回最新一个交易日的筹码分布指标 dict。

    内部调用 compute_chip_distribution_daily 并取最后一行，保持旧调用方（测试等）兼容。
    空输入返回空 dict。
    """
    df = compute_chip_distribution_daily(daily_df, basic_df)
    if df.empty:
        return {}
    row = df.iloc[-1]
    return {col: row[col] for col in DAILY_COLUMNS}


def save_cyq(conn, ts_code: str, df: pd.DataFrame) -> int:
    """将单只股票的逐日筹码指标批量写入 tushare_cyq 表（幂等）。

    Args:
        conn: psycopg2 连接。
        ts_code: Tushare 格式股票代码。
        df: compute_chip_distribution_daily 返回的逐日 DataFrame。

    Returns:
        写入行数（0 表示无数据写入）。
    """
    if df is None or df.empty:
        return 0
    df = df.copy()
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


def _normalize_date(value: str) -> str:
    """将 YYYYMMDD 归一化为 YYYY-MM-DD（数据库比较用）。"""
    s = str(value).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def main():
    parser = argparse.ArgumentParser(description="筹码分布（CYQ）本地计算采集（逐日，支持增量）")
    parser.add_argument("--limit", type=int, default=0, help="最多处理的股票数量（0=全部）")
    parser.add_argument("--symbol", type=str, default="", help="指定单只股票 6 位代码（如 600519）")
    parser.add_argument("--start-date", type=str, default="20160101",
                        help="起始日期 YYYYMMDD（默认 20160101，首次全量起点）")
    parser.add_argument("--end-date", type=str, default="",
                        help="结束日期 YYYYMMDD（默认今天）")
    parser.add_argument("--sleep", type=float, default=0.0, help="每只股票处理间隔秒数（默认 0）")
    args = parser.parse_args()

    start_date = _normalize_date(args.start_date)
    end_date = _normalize_date(args.end_date) if args.end_date else date.today().strftime("%Y-%m-%d")

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

        logger.info("待处理股票: %d 只（区间 %s ~ %s）", len(rows), start_date, end_date)
        if not rows:
            logger.info("无待处理股票，退出")
            return

        ok = 0
        fail = 0
        total_rows = 0
        t0 = time.time()
        for i, (symbol, ts_code) in enumerate(rows, start=1):
            try:
                latest = get_latest_cyq_date(conn, ts_code)
                earliest_cyq = get_earliest_cyq_date(conn, ts_code)
                earliest_daily = get_earliest_daily_date(conn, ts_code)
                if latest is None:
                    # 无任何历史 → 从 start_date 全量计算
                    calc_start = start_date
                    insert_from = None
                elif earliest_daily is not None and earliest_cyq is not None and earliest_cyq > earliest_daily:
                    # 历史缺口：cyq 最早日期晚于 daily 最早日期（如旧版只写了最新一天）
                    # → 从 start_date 全量重算，ON CONFLICT 幂等跳过已有行
                    calc_start = start_date
                    insert_from = None
                    logger.info("[%d/%d] %s: 检测到历史缺口（cyq 最早 %s < daily 最早 %s），全量重算",
                                i, len(rows), ts_code,
                                earliest_cyq.strftime("%Y-%m-%d"), earliest_daily.strftime("%Y-%m-%d"))
                else:
                    # 正常增量：从 start_date 全量重算（与全量回填共用同一价格档位网格与
                    # 同一累积深度，保证结果一致），仅插入 latest 之后的新日期
                    calc_start = start_date
                    insert_from = latest.strftime("%Y-%m-%d")

                daily_df = load_daily_data(conn, ts_code, start_date=calc_start, end_date=end_date)
                if daily_df.empty:
                    if latest is not None:
                        # 已有历史且区间内无新数据 → 已是最新
                        ok += 1
                        logger.info("[%d/%d] %s: 已是最新（latest=%s），跳过",
                                    i, len(rows), ts_code, latest.strftime("%Y-%m-%d"))
                    else:
                        logger.warning("[%d/%d] %s: tushare_daily 无数据", i, len(rows), ts_code)
                        fail += 1
                    continue

                basic_df = load_daily_basic(conn, ts_code, start_date=calc_start, end_date=end_date)
                price_range = get_full_price_range(conn, ts_code)
                result_df = compute_chip_distribution_daily(daily_df, basic_df, price_range)
                if result_df.empty:
                    logger.warning("[%d/%d] %s: 无有效筹码数据", i, len(rows), ts_code)
                    fail += 1
                    continue

                if insert_from is not None:
                    result_df = result_df[result_df["trade_date"] > insert_from]
                    if result_df.empty:
                        ok += 1
                        logger.info("[%d/%d] %s: 已是最新（latest=%s），无新增",
                                    i, len(rows), ts_code, insert_from)
                        continue

                n = save_cyq(conn, ts_code, result_df)
                if n > 0:
                    ok += 1
                    total_rows += n
                    logger.info(
                        "[%d/%d] %s: 入库 %d 行 (%s ~ %s)",
                        i, len(rows), ts_code, n,
                        result_df["trade_date"].iloc[0],
                        result_df["trade_date"].iloc[-1],
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