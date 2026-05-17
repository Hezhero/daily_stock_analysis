#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
22 策略 5 年回测 + 本周验证
回测区间: 2021-01-01 ~ 2026-03-20
验证区间: 2026-03-20 ~ 2026-03-27

优化点:
1. 并行回测: ThreadPoolExecutor (19策略并行，向量化释放GIL)
2. 向量化验证: 移除 validate_week 中的 iterrows()
3. 预计算共享指标: vol_std_20d 等
4. Numba加速: calc_metrics

修改点:
- 移除邮件发送功能
- 移除三步过滤功能
- 回测完成后自动执行主程序进行大盘复盘和个股决策
"""

import argparse
import logging
import os
import sys
import time
import gc
import subprocess
import hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import baostock as bs

# 添加项目根目录到路径，以便导入主程序
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

# ─── 配置 ──────────────────────────────────────────────────────────────────────
INITIAL_CAPITAL = 1000000.0
TOP_N_VALIDATE = 5
HOLDING_PERIODS = [1, 3, 5, 10]
VALIDATE_DAYS = 5
MARKET_CACHE_VERSION = "v1"
MARKET_FILTER_VERSION = "main-board-and-chinext-v1"
MARKET_COLUMNS_VERSION = "ohlcv-basic-v1"
ADJUSTMENT_LOGIC_VERSION = "v1"
INDICATOR_CACHE_VERSION = "v1"
FLOAT_PRECISION_VERSION = "float32-v1"
CACHE_DIR = BASE_DIR / "cache"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("backtest")


def get_cache_dir(name: str) -> Path:
    cache_dir = CACHE_DIR / name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def build_market_cache_key(start: str, end: str) -> str:
    raw_key = "|".join([
        MARKET_CACHE_VERSION,
        MARKET_FILTER_VERSION,
        MARKET_COLUMNS_VERSION,
        "baostock_daily_history_xr",
        start,
        end,
    ])
    return hashlib.md5(raw_key.encode("utf-8")).hexdigest()


def get_market_cache_path(start: str, end: str) -> Path:
    return get_cache_dir("market_data") / f"{build_market_cache_key(start, end)}.parquet"


def convert_market_numeric_columns_to_float32(df: pd.DataFrame) -> pd.DataFrame:
    float_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pct_chg",
        "turn",
        "pe_ttm",
        "pb_mrq",
    ]
    converted = df.copy()
    for col in float_columns:
        if col in converted.columns:
            converted[col] = converted[col].astype("float32")
    return converted


def get_postgres_engine():
    from sqlalchemy import create_engine
    from urllib.parse import quote_plus

    host = os.environ.get("PG_HOST", "127.0.0.1")
    port = int(os.environ.get("PG_PORT", 5431))
    database = os.environ.get("PG_DATABASE", "baostock")
    user = os.environ.get("PG_USER", "root")
    password = os.environ.get("PG_PASSWORD", "")
    quoted_user = quote_plus(user)
    quoted_password = quote_plus(password)
    return create_engine(f"postgresql+psycopg2://{quoted_user}:{quoted_password}@{host}:{port}/{database}")


def load_data(start: str, end: str) -> pd.DataFrame:
    """使用 SQLAlchemy connectable 加载 PostgreSQL 数据"""
    t0 = time.time()
    engine = get_postgres_engine()

    logger.info(f"加载数据 {start} ~ {end} (PostgreSQL)...")
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                r"""
                SELECT code, name, date, open, high, low, close,
                       volume, amount, pct_chg, turn, pe_ttm, pb_mrq
                FROM baostock_daily_history_xr
                WHERE date BETWEEN %s AND %s
                  AND trade_status = '1'
                  AND is_st = '0'
                  AND adjust_flag = '3'
                  AND code ~ '^(sh\.6(?!88)|sh\.8|sh\.4|sz\.0|sz\.2|sz\.3(?!9))'
                ORDER BY code, date
                """,
                conn,
                params=(start, end),
                parse_dates=["date"],
            )
    finally:
        engine.dispose()

    for col in ["open", "high", "low", "close", "volume", "amount",
                "pct_chg", "turn", "pe_ttm", "pb_mrq"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info(f"总计 {len(df):,} 行 × {df['code'].nunique()} 只股票，耗时 {time.time()-t0:.1f}s")
    return df


def empty_adjust_factor_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["code", "dividOperateDate", "foreAdjustFactor"])


def load_adjust_factor_cache_from_db(codes: List[str], start: str, end: str) -> pd.DataFrame:
    if not codes:
        return empty_adjust_factor_frame()

    from sqlalchemy import bindparam, text

    engine = get_postgres_engine()
    query = text(
        """
        SELECT code,
               divid_operate_date AS "dividOperateDate",
               fore_adjust_factor AS "foreAdjustFactor"
        FROM adjust_factor_cache
        WHERE code IN :codes
          AND divid_operate_date BETWEEN :start AND :end
        ORDER BY code, dividOperate_date
        """
    ).bindparams(bindparam("codes", expanding=True))

    try:
        with engine.connect() as conn:
            df_factor = pd.read_sql(
                query,
                conn,
                params={"codes": codes, "start": start, "end": end},
                parse_dates=["dividOperateDate"],
            )
    finally:
        engine.dispose()

    if df_factor.empty:
        return empty_adjust_factor_frame()
    return df_factor


def upsert_adjust_factor_cache(df_factor: pd.DataFrame) -> None:
    if df_factor.empty:
        return

    from sqlalchemy import text

    records = [
        {
            "code": row["code"],
            "divid_operate_date": pd.Timestamp(row["dividOperateDate"]).date(),
            "fore_adjust_factor": float(row["foreAdjustFactor"]),
        }
        for row in df_factor[["code", "dividOperateDate", "foreAdjustFactor"]].to_dict("records")
    ]

    engine = get_postgres_engine()
    statement = text(
        """
        INSERT INTO adjust_factor_cache (code, divid_operate_date, fore_adjust_factor)
        VALUES (:code, :divid_operate_date, :fore_adjust_factor)
        ON CONFLICT (code, divid_operate_date)
        DO UPDATE SET
            fore_adjust_factor = EXCLUDED.fore_adjust_factor,
            updated_at = CURRENT_TIMESTAMP
        """
    )

    try:
        with engine.begin() as conn:
            conn.execute(statement, records)
    finally:
        engine.dispose()


def fetch_adjust_factors_from_baostock(codes: List[str], start: str, end: str) -> pd.DataFrame:
    """
    通过 baostock query_adjust_factor 获取前复权因子
    """
    login_result = bs.login()
    if login_result.error_code != '0':
        raise RuntimeError(f"BaoStock 登录失败: {login_result.error_msg}")

    all_factors = []
    try:
        for code in codes:
            try:
                rs_factor = bs.query_adjust_factor(code=code, start_date=start, end_date=end)
                while (rs_factor.error_code == '0') & rs_factor.next():
                    row = rs_factor.get_row_data()
                    all_factors.append({
                        "code": row[0],
                        "dividOperateDate": row[1],
                        "foreAdjustFactor": float(row[2]) if row[2] else 1.0,
                    })
            except Exception as e:
                logger.warning(f"获取 {code} 前复权因子失败: {e}")
                continue
    finally:
        bs.logout()

    if not all_factors:
        return empty_adjust_factor_frame()

    df_factor = pd.DataFrame(all_factors)
    df_factor["dividOperateDate"] = pd.to_datetime(df_factor["dividOperateDate"])
    return df_factor


def load_or_fill_adjust_factor_cache(codes: List[str], start: str, end: str) -> pd.DataFrame:
    if not codes:
        return empty_adjust_factor_frame()

    cached_df = load_adjust_factor_cache_from_db(codes, start, end)
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    covered_codes = set()

    if not cached_df.empty:
        coverage = cached_df.groupby("code", sort=False)["dividOperateDate"].agg(["min", "max"])
        covered_codes = {
            code
            for code, row in coverage.iterrows()
            if row["min"] <= start_ts and row["max"] >= end_ts
        }

    missing_codes = [code for code in codes if code not in covered_codes]

    if not missing_codes:
        return cached_df

    fetched_df = fetch_adjust_factors_from_baostock(missing_codes, start, end)
    upsert_adjust_factor_cache(fetched_df)
    return load_adjust_factor_cache_from_db(codes, start, end)


def apply_forward_adjustment(df: pd.DataFrame, df_factor: pd.DataFrame) -> pd.DataFrame:
    """
    基于复权因子将不复权价格转换为前复权价格（原地操作，低内存）
    """
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")

    if not df_factor.empty:
        factor_subset = df_factor[["code", "dividOperateDate", "foreAdjustFactor"]].copy()
        factor_subset["dividOperateDate"] = pd.to_datetime(factor_subset["dividOperateDate"]).astype("datetime64[ns]")
        factor_subset.sort_values("dividOperateDate", inplace=True)

        df = pd.merge_asof(
            df.sort_values("date"),
            factor_subset,
            left_on="date",
            right_on="dividOperateDate",
            by="code",
            direction="backward",
        )
        df["foreAdjustFactor"] = df["foreAdjustFactor"].fillna(1.0)
        df.drop(columns=["dividOperateDate"], inplace=True, errors="ignore")

        for col in ["open", "high", "low", "close"]:
            df[col] = df[col] * df["foreAdjustFactor"]
        df.drop(columns=["foreAdjustFactor"], inplace=True)
    else:
        pass

    g = df.groupby("code", sort=False)
    df["pre_close"] = g["close"].shift(1)
    df["pct_chg"] = (df["close"] / df["pre_close"] - 1) * 100

    return df


def build_indicator_cache_key(market_cache_key: str) -> str:
    raw_key = "|".join([
        market_cache_key,
        ADJUSTMENT_LOGIC_VERSION,
        INDICATOR_CACHE_VERSION,
    ])
    return hashlib.md5(raw_key.encode("utf-8")).hexdigest()


def get_indicator_cache_path(market_cache_key: str) -> Path:
    return get_cache_dir("indicators") / f"{build_indicator_cache_key(market_cache_key)}.parquet"


def load_or_build_indicator_cache(df_adjusted: pd.DataFrame, market_cache_key: str) -> pd.DataFrame:
    cache_path = get_indicator_cache_path(market_cache_key)
    if cache_path.exists():
        df = pd.read_parquet(cache_path)
        float_cols = [c for c in df.columns if c not in ("code", "name", "date") and df[c].dtype == "float64"]
        for c in float_cols:
            df[c] = df[c].astype("float32")
        return df
    indicator_df = compute_indicators(df_adjusted)
    indicator_df.to_parquet(cache_path, index=False)
    return indicator_df


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    t0 = time.time()
    df = df.sort_values(["code", "date"]).reset_index(drop=True)

    g = df.groupby("code", group_keys=False)

    # ─── 移动平均线 ─────────────────────────────────────────────────────────────
    for w in [5, 10, 20, 60, 90, 120]:
        df[f"ma{w}"] = g["close"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    # ─── 成交量移动平均 ─────────────────────────────────────────────────────────
    for w in [5, 10, 20]:
        df[f"vol_ma{w}"] = g["volume"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    # ─── 共享指标（预计算） ─────────────────────────────────────────────────────────
    df["vol_std_20d"] = g["volume"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["vol_threshold"] = df["vol_ma20"] + 2 * df["vol_std_20d"]

    df["high_20d_max"] = g["high"].transform(lambda x: x.shift(1).rolling(20, min_periods=1).max())
    df["vol_60d_min"] = g["volume"].transform(lambda x: x.rolling(60, min_periods=20).min())
    df["vol_10d_mean"] = g["volume"].transform(lambda x: x.rolling(10, min_periods=1).mean())
    df["close_4d_ago"] = g["close"].shift(4)

    # ─── RSI ───────────────────────────────────────────────────────────────────
    for w in [6, 12, 24]:
        delta = g["close"].diff()
        gain = delta.where(delta > 0, 0).transform(lambda x: x.rolling(w, min_periods=1).mean())
        loss = (-delta.where(delta < 0, 0)).transform(lambda x: x.rolling(w, min_periods=1).mean())
        df[f"rsi{w}"] = 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))

    # ─── MACD (12, 26, 9) ─────────────────────────────────────────────────────────
    ema12 = g["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_dif"] = ema12 - ema26
    df["macd_dea"] = g["macd_dif"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    df["macd_hist"] = 2 * (df["macd_dif"] - df["macd_dea"])

    # ─── MACD (14, 53, 5) ─────────────────────────────────────────────────────────
    ema14 = g["close"].transform(lambda x: x.ewm(span=14, adjust=False).mean())
    ema53 = g["close"].transform(lambda x: x.ewm(span=53, adjust=False).mean())
    df["macd_dif2"] = ema14 - ema53
    df["macd_dea2"] = g["macd_dif2"].transform(lambda x: x.ewm(span=5, adjust=False).mean())
    df["macd_hist2"] = 2 * (df["macd_dif2"] - df["macd_dea2"])

    # ─── Bollinger Bands ──────────────────────────────────────────────────────────
    df["boll_mid"] = g["close"].transform(lambda x: x.rolling(20, min_periods=1).mean())
    std20 = g["close"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["boll_upper"] = df["boll_mid"] + 2 * std20
    df["boll_lower"] = df["boll_mid"] - 2 * std20

    # ─── 未来收益率 ─────────────────────────────────────────────────────────────
    for p in HOLDING_PERIODS:
        df[f"ret_{p}d"] = g["close"].transform(lambda x: x.pct_change(p, fill_method=None).shift(-p))

    df["ret_5d_open_to_close"] = (g["close"].shift(-4) / df["open"] - 1)

    indicator_cols = [c for c in df.columns if c not in ("code", "name", "date")]
    for c in indicator_cols:
        if df[c].dtype == "float64":
            df[c] = df[c].astype("float32")

    logger.info(f"指标计算完成，耗时 {time.time()-t0:.1f}s")
    gc.collect()
    return df


# ─── 策略信号函数 ─────────────────────────────────────────────────────────────

def _ma_cross(df):
    ma5 = df["ma5"]
    ma20 = df["ma20"]
    m5p = df.groupby("code")["ma5"].shift(1)
    m20p = df.groupby("code")["ma20"].shift(1)
    return ((ma5 > ma20) & (m5p <= m20p)).astype(int) * 3 + \
           (df["volume"] > df["vol_ma5"] * 1.5).astype(int) * 2 + \
           (df["macd_hist"] > 0).astype(int) * 2 + \
           ((df["rsi6"] > 30) & (df["rsi6"] < 70)).astype(int) * 1.5 + \
           (df["close"] > df["ma60"]).astype(int) * 1.5 + \
           (df["close"] > df["open"]).astype(int)


def _vol_surge(df):
    return ((df["volume"] > df["vol_threshold"]).astype(int) * 4 +
            (df["close"] > df["high_20d_max"]).astype(int) * 4 +
            (df["close"] > df["open"]).astype(int) * 2 +
            ((df["rsi6"] > 30) & (df["rsi6"] < 70)).astype(int) * 2)


def sig_ma_crossover(df):
    return _ma_cross(df) >= 6


def sig_volume_surge_std(df):
    return _vol_surge(df) >= 6


def sig_wonderful_9_turn(df):
    close = df["close"]
    close_4d = df["close_4d_ago"]
    streak = close.groupby(df["code"]).transform(
        lambda x: (x < x.shift(4)).rolling(9, min_periods=9).min().fillna(0).astype(bool)
    )
    mp = df.groupby("code")["macd_hist"].shift(1)
    m20p = df.groupby("code")["ma20"].shift(1)
    m60p = df.groupby("code")["ma60"].shift(1)
    score = (streak.astype(int) * 4 +
             (df["rsi6"] < 35).astype(int) * 3 +
             ((df["macd_hist"] > 0) & (df["macd_hist"] < mp)).astype(int) * 3 +
             (df["volume"] > df["vol_ma5"] * 1.2).astype(int) * 2 +
             ((df["ma20"] > m60p) & (df["ma20"] > m20p) & (df["ma60"] > m60p)).astype(int) * 2 +
             (close >= df["ma20"] * 0.98).astype(int) * 2 +
             (close > df["open"]).astype(int))
    return score >= 10


def sig_n_pattern(df):
    sh = df.groupby("code")["high"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).max())
    score = ((df["close"] > sh).astype(int) * 6 +
             (df["volume"] > df["vol_ma5"]).astype(int) * 4 +
             (df["close"] > df["open"]).astype(int) * 3 +
             (df["close"] > df["ma20"]).astype(int) * 2)
    return score >= 13


def sig_limit_up_pullback(df):
    pct = df["pct_chg"]
    hi = df["high"].where(pct >= 9.5).groupby(df["code"]).ffill().fillna(0)
    vl = df["volume"].where(pct >= 9.5).groupby(df["code"]).ffill().fillna(0.1)
    pct_prev = df.groupby("code")["pct_chg"].shift(1).fillna(0)
    score = (pct_prev >= 9.5).astype(int) * 2 + \
            (df["volume"] / vl.replace(0, 0.1) < 0.5).astype(int) * 3 + \
            (df["close"] >= hi * 0.97).astype(int) * 3 + \
            (df["volume"] > df["vol_ma5"] * 1.5).astype(int) * 4 + \
            (pct > 0).astype(int) * 2
    return score >= 8


def sig_stable_then_limit_up(df):
    mx = df.groupby("code")["pct_chg"].transform(lambda x: x.shift(1).rolling(10, min_periods=1).max())
    mn = df.groupby("code")["pct_chg"].transform(lambda x: x.shift(1).rolling(10, min_periods=1).min())
    stable = (mx < 5) & (mn > -5)
    return stable & (df["pct_chg"] >= 9.5) & (df["volume"] > df["vol_10d_mean"] * 1.5)


def sig_monthly_macd_20ma(df):
    dp = df.groupby("code")["macd_dif"].shift(1)
    dep = df.groupby("code")["macd_dea"].shift(1)
    m20p = df.groupby("code")["ma20"].shift(1)
    score = (((df["macd_dif"] > df["macd_dea"]) & (dp <= dep)).astype(int) * 5 +
             (df["ma20"] > m20p).astype(int) * 4 +
             (df["close"] >= df["ma20"] * 0.97).astype(int) * 3 +
             (df["volume"] > df["vol_ma5"] * 1.5).astype(int) * 4 +
             (df["close"] > df["open"]).astype(int) * 2 +
             ((df["rsi6"] > 40) & (df["rsi6"] < 70)).astype(int) * 2)
    return score >= 10


def sig_low_position_limit_up(df):
    pct = df["pct_chg"]
    h20 = df["high_20d_max"]
    no_lim = ~(df.groupby("code")["pct_chg"].transform(lambda x: (x >= 9.5).rolling(20, min_periods=1).max().shift(1).fillna(0).astype(bool)))
    return (pct >= 9.5) & (df["close"] < h20 * 0.9) & (df["turn"] >= 5) & (df["close"] < 50) & no_lim


def sig_limit_up_resonance(df):
    pct = df["pct_chg"]
    m20p = df.groupby("code")["ma20"].shift(1)
    r6p = df.groupby("code")["rsi6"].shift(1)
    limit_prev = df.groupby("code")["pct_chg"].shift(1).fillna(0) >= 9.5
    return (limit_prev &
            (df["ma20"] > m20p) &
            (df["close"] >= df["ma20"] * 0.97) &
            (df["volume"] > df["vol_ma5"] * 1.5) &
            (pct > 0) &
            (df["rsi6"] > r6p))


def sig_bullish_engulfing(df):
    pc = df.groupby("code")["close"].shift(1)
    po = df.groupby("code")["open"].shift(1)
    ph = df.groupby("code")["high"].shift(1)
    pl = df.groupby("code")["low"].shift(1)
    pp = df.groupby("code")["pct_chg"].shift(1)
    body = (pc - po).abs()
    rng = ph - pl
    big = (pp <= -7) & (body / rng.replace(0, 1) >= 0.7)
    return big & (df["open"] >= po * 1.02) & (df["close"] > df["open"])


def sig_multi_ma_resonance(df):
    m5p = df.groupby("code")["ma5"].shift(1)
    m10p = df.groupby("code")["ma10"].shift(1)
    dp2 = df.groupby("code")["macd_dif"].shift(1)
    dep2 = df.groupby("code")["macd_dea"].shift(1)
    mp = df.groupby("code")["macd_hist"].shift(1)
    r6p = df.groupby("code")["rsi6"].shift(1)
    r12p = df.groupby("code")["rsi12"].shift(1)
    r24p = df.groupby("code")["rsi24"].shift(1)
    bup = df.groupby("code")["boll_upper"].shift(1)
    bmp = df.groupby("code")["boll_mid"].shift(1)

    bull_ma = ((df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"]) &
               (df["ma20"] > df["ma60"]) & (df["ma60"] > df["ma90"]) &
               (df["ma90"] > df["ma120"]) & (df["ma5"] > m5p))
    cross_5_10 = (df["ma5"] > df["ma10"]) & (m5p <= m10p)
    score = (bull_ma.astype(int) * 4 +
             cross_5_10.astype(int) * 4 +
             (df["volume"] > df["vol_ma20"] * 1.5).astype(int) * 3 +
             ((df["macd_dif"] > df["macd_dea"]) & (dp2 <= dep2)).astype(int) * 3 +
             ((df["macd_hist"] > 0) & (mp <= 0)).astype(int) * 2 +
             ((df["rsi6"] > r6p) & (df["rsi12"] > r12p) & (df["rsi24"] > r24p) &
              (df["rsi6"] < 70)).astype(int) * 3 +
             ((df["boll_upper"] - df["boll_mid"]) > (bup - bmp)).astype(int) * 2)
    return score >= 10


def sig_ensemble(df):
    return (sig_ma_crossover(df).astype(int) +
            sig_volume_surge_std(df).astype(int) +
            sig_multi_ma_resonance(df).astype(int)) >= 2


def sig_volume_breakout(df):
    score = ((df["volume"] > df["vol_ma5"] * 2).astype(int) * 5 +
             (df["close"] > df["ma20"]).astype(int) * 4 +
             (df["close"] > df["open"]).astype(int) * 2 +
             (df["close"] > df["ma60"]).astype(int) * 2)
    return score >= 8


def sig_bull_trend(df):
    c = df["close"]
    ma5 = df["ma5"]
    ma10 = df["ma10"]
    ma20 = df["ma20"]
    return (c > ma5) & (ma5 > ma10) & (ma10 > ma20) & (df["rsi6"] > 40) & (df["rsi6"] < 70) & (df["volume"] > df["vol_ma5"])


def sig_ma_golden_cross(df):
    m5p = df.groupby("code")["ma5"].shift(1)
    m10p = df.groupby("code")["ma10"].shift(1)
    cross = (df["ma5"] > df["ma10"]) & (m5p <= m10p)
    return cross & (df["volume"] > df["vol_ma5"] * 1.2) & (df["close"] > df["ma10"])


def sig_shrink_pullback(df):
    s1 = df["volume"] < df["vol_ma5"] * 0.3
    s2 = (df["close"] - df["ma10"]).abs() / df["ma10"] < 0.03
    s3 = df["pct_chg"] > 0
    return s1 & s2 & s3


def sig_dragon_head(df):
    pp = df.groupby("code")["pct_chg"].shift(1)
    pc = df.groupby("code")["close"].shift(1)
    return (pp >= 9.5) & (df["open"] > pc * 1.02)


def sig_emotion_cycle(df):
    return (df["rsi6"] < 35) & (df["close"] > df["open"]) & (df["volume"] > df["vol_ma5"])


def sig_bottom_volume(df):
    return (df["volume"] <= df["vol_60d_min"]) & (df["rsi6"] < 40) & (df["close"] > df["open"])


def sig_one_yang_three_yin(df):
    grouped = df.groupby("code")
    yang = grouped["pct_chg"].shift(4) > 3
    base_volume = grouped["volume"].shift(4)
    three_shrink = (
        (grouped["volume"].shift(3) < base_volume * 0.7) &
        (grouped["volume"].shift(2) < base_volume * 0.7) &
        (grouped["volume"].shift(1) < base_volume * 0.7)
    )
    rise = (df["pct_chg"] > 0) & (df["close"] > grouped["close"].shift(4))
    return yang & three_shrink & rise


def sig_box_oscillation(df):
    boll_mid = df["boll_mid"]
    price_near_mid = (df["close"] - boll_mid).abs() / boll_mid < 0.02
    rsi_ok = (df["rsi6"] > 30) & (df["rsi6"] < 70)
    vol_shrink = df["volume"] < df["vol_ma20"] * 0.8
    trend_up = df["close"] > df["ma20"]
    return price_near_mid & rsi_ok & vol_shrink & trend_up


def sig_wave_theory(df):
    breakout = df["close"] > df["high_20d_max"]
    vol_surge = df["volume"] > df["vol_ma5"] * 1.5
    rsi_strong = (df["rsi6"] > 45) & (df["rsi6"] < 75)
    macd_bullish = df["macd_hist"] > 0
    price_above_ma20 = df["close"] > df["ma20"]
    score = breakout.astype(int) * 4 + vol_surge.astype(int) * 3 + rsi_strong.astype(int) * 2 + macd_bullish.astype(int) * 2 + price_above_ma20.astype(int)
    return score >= 8


def sig_chan_theory(df):
    pc = df.groupby("code")["close"].shift(1)
    new_low = df["close"] < pc
    dif = df["macd_dif"]
    difp = df.groupby("code")["macd_dif"].shift(1)
    macd_not_new_low = dif >= difp
    vol_ok = df["volume"] > df["vol_ma5"]
    rsi_oversold = df["rsi6"] < 50
    close_above_open = df["close"] > df["open"]
    return new_low & macd_not_new_low & vol_ok & rsi_oversold & close_above_open


# ─── 策略注册表 ───────────────────────────────────────────────────────────────

STRATEGIES = {
    "ma_crossover": sig_ma_crossover,
    "volume_surge_std": sig_volume_surge_std,
    "wonderful_9_turn": sig_wonderful_9_turn,
    "n_pattern": sig_n_pattern,
    "limit_up_pullback": sig_limit_up_pullback,
    "stable_then_limit_up": sig_stable_then_limit_up,
    "monthly_macd_20ma": sig_monthly_macd_20ma,
    "low_position_limit_up": sig_low_position_limit_up,
    "limit_up_resonance": sig_limit_up_resonance,
    "bullish_engulfing": sig_bullish_engulfing,
    "multi_ma_resonance": sig_multi_ma_resonance,
    "ensemble": sig_ensemble,
    "volume_breakout": sig_volume_breakout,
    "bull_trend": sig_bull_trend,
    "ma_golden_cross": sig_ma_golden_cross,
    "shrink_pullback": sig_shrink_pullback,
    "dragon_head": sig_dragon_head,
    "emotion_cycle": sig_emotion_cycle,
    "bottom_volume": sig_bottom_volume,
    "one_yang_three_yin": sig_one_yang_three_yin,
    "box_oscillation": sig_box_oscillation,
    "wave_theory": sig_wave_theory,
    "chan_theory": sig_chan_theory,
}


# ─── 绩效计算（Numba加速）────────────────────────────────────────────────────────

try:
    import numba
    _HAS_NUMBA = True
except ImportError:
    _HAS_NUMBA = False


if _HAS_NUMBA:
    @numba.njit(cache=True)
    def _calc_max_drawdown(r: np.ndarray) -> float:
        n = len(r)
        if n < 2:
            return 0.0
        cur_eq = 0.0
        peak = 0.0
        max_dd = 0.0
        for i in range(n):
            cur_eq += r[i]
            if cur_eq > peak:
                peak = cur_eq
            dd = (peak - cur_eq) / (abs(peak) + 1e-10)
            if dd > max_dd:
                max_dd = dd
        return max_dd * 100.0


    @numba.njit(cache=True)
    def _calc_metrics_core(r: np.ndarray, n: int, avg_holding: float) -> Tuple:
        mask = np.abs(r) < 5
        r = r[mask]

        wins = r[r > 0]
        losses = r[r < 0]
        n_valid = len(r)

        if n_valid == 0:
            return (0.0,) * 9

        win_rate = len(wins) / n_valid * 100.0
        avg_win = float(wins.mean() * 100.0) if len(wins) > 0 else 0.0
        avg_loss = float(abs(losses.mean()) * 100.0) if len(losses) > 0 else 0.0
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 1e-10 else 0.0

        mean_ret = float(r.mean())
        total_return = mean_ret * n_valid
        total_return_pct = total_return * 100.0

        years = n_valid * avg_holding / 252.0
        annualized_return = (total_return / max(years, 0.001)) * 100.0

        sharpe = 0.0
        std_ret = float(np.std(r))
        if n_valid > 1 and std_ret > 1e-10:
            sharpe = float(mean_ret / std_ret * np.sqrt(252.0 / avg_holding))

        sample = r[:min(n_valid, 20000)]
        max_drawdown = _calc_max_drawdown(sample)

        return (win_rate, avg_win, avg_loss, profit_loss_ratio,
                total_return_pct, annualized_return, max_drawdown, sharpe, float(n_valid))
else:
    def _calc_max_drawdown(r: np.ndarray) -> float:
        n = len(r)
        if n < 2:
            return 0.0
        cur_eq = 0.0
        peak = 0.0
        max_dd = 0.0
        for ri in r:
            cur_eq += ri
            if cur_eq > peak:
                peak = cur_eq
            dd = (peak - cur_eq) / (abs(peak) + 1e-10)
            if dd > max_dd:
                max_dd = dd
        return max_dd * 100.0


    def _calc_metrics_core(r: np.ndarray, n: int, avg_holding: float):
        r = r[~np.isnan(r)]
        r = r[np.abs(r) < 5]
        n_valid = len(r)
        if n_valid == 0:
            return (0.0,) * 9

        wins = r[r > 0]
        losses = r[r < 0]
        win_rate = len(wins) / n_valid * 100.0
        avg_win = float(wins.mean() * 100.0) if len(wins) > 0 else 0.0
        avg_loss = float(abs(losses.mean()) * 100.0) if len(losses) > 0 else 0.0
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 1e-10 else 0.0

        mean_ret = float(r.mean())
        total_return = mean_ret * n_valid
        total_return_pct = total_return * 100.0

        years = n_valid * avg_holding / 252.0
        annualized_return = (total_return / max(years, 0.001)) * 100.0

        sharpe = 0.0
        std_ret = float(np.std(r))
        if n_valid > 1 and std_ret > 1e-10:
            sharpe = float(mean_ret / std_ret * np.sqrt(252.0 / avg_holding))

        sample = r[:min(n_valid, 20000)]
        max_drawdown = _calc_max_drawdown(sample)

        return (win_rate, avg_win, avg_loss, profit_loss_ratio,
                total_return_pct, annualized_return, max_drawdown, sharpe, float(n_valid))


def calc_metrics(returns: np.ndarray) -> Dict:
    r = np.asarray(returns, dtype=float).flatten()
    n = len(r)
    if n == 0:
        return {}

    avg_holding = sum(HOLDING_PERIODS) / len(HOLDING_PERIODS)

    if _HAS_NUMBA:
        wr, aw, al, pl, tr, ann, mxd, sr, n_valid = _calc_metrics_core(r, n, avg_holding)
    else:
        wr, aw, al, pl, tr, ann, mxd, sr, n_valid = _calc_metrics_core(r, n, avg_holding)

    return {
        "total_trades": int(n_valid),
        "win_rate": round(wr, 2),
        "avg_win": round(aw, 2),
        "avg_loss": round(al, 2),
        "profit_loss_ratio": round(pl, 2),
        "total_return": round(float(np.clip(tr, -1e10, 1e10)), 2),
        "annualized_return": round(float(np.clip(ann, -1e10, 1e10)), 2),
        "max_drawdown": round(float(np.clip(mxd, 0, 100)), 2),
        "sharpe_ratio": round(float(np.clip(sr, -100, 100)), 2),
    }


# ─── 并行回测函数 ──────────────────────────────────────────────────────────────

def _backtest_single(name: str, df: pd.DataFrame) -> Dict:
    t0 = time.time()
    try:
        sig = STRATEGIES[name](df)
        signals = df[sig]
        n = signals["code"].count()
        if n == 0:
            return {"strategy": name, "total_trades": 0, "win_rate": 0,
                    "avg_win": 0, "avg_loss": 0, "profit_loss_ratio": 0,
                    "total_return": 0, "annualized_return": 0,
                    "max_drawdown": 0, "sharpe_ratio": 0, "time_s": round(time.time()-t0, 1)}

        all_returns = []
        for p in HOLDING_PERIODS:
            col = f"ret_{p}d"
            if col in signals.columns:
                vals = signals[col].dropna().values
                if len(vals) > 0:
                    all_returns.extend(vals)

        m = calc_metrics(np.array(all_returns) if all_returns else np.array([0]))
        m["strategy"] = name
        m["time_s"] = round(time.time() - t0, 1)
        return m
    except Exception as e:
        return {"strategy": name, "error": str(e), "time_s": round(time.time()-t0, 1)}


def resolve_max_workers() -> int:
    raw_value = os.environ.get("BACKTEST_MAX_WORKERS")
    if raw_value is None:
        return 1

    try:
        parsed = int(raw_value)
    except ValueError:
        logger.warning("BACKTEST_MAX_WORKERS=%s 不是有效整数，回退到 1", raw_value)
        return 1

    return max(parsed, 1)


def run_backtests(df_bt: pd.DataFrame) -> List[Dict]:
    t0 = time.time()
    n_strategies = len(STRATEGIES)
    n_workers = min(resolve_max_workers(), n_strategies)
    logger.info(f"并行回测 {n_strategies} 策略（{n_workers} 线程）...")

    strategy_names = list(STRATEGIES.keys())
    results = []

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(_backtest_single, name, df_bt): name for name in strategy_names}
        for i, future in enumerate(as_completed(futures)):
            r = future.result()
            results.append(r)
            if "error" not in r:
                logger.info(f"  [{i+1}/{n_strategies}] {r['strategy']}: {r['total_trades']} 笔, "
                            f"胜率{r['win_rate']:.1f}%, 总收益{r['total_return']:.1f}%")
            else:
                logger.error(f"  [{i+1}/{n_strategies}] {r['strategy']}: {r['error']}")

    valid = sorted([r for r in results if "error" not in r],
                   key=lambda x: x.get("total_return", 0), reverse=True)
    logger.info(f"回测完成，耗时 {time.time()-t0:.1f}s")
    return valid


# ─── 最近5日验证 ──────────────────────────────────────────────────────────────

def validate_week(df_week, top_results, top_n=5):
    top_names = [r["strategy"] for r in top_results[:top_n]]
    val = []
    logger.info(f"5日验证 {df_week['date'].min().date()} ~ {df_week['date'].max().date()}")

    all_dates = sorted(df_week["date"].unique())
    if len(all_dates) < 5:
        logger.warning("验证区间不足5个交易日")
        return val

    buy_date = all_dates[-5]
    sell_date = all_dates[-1]

    for name in top_names:
        try:
            sig = STRATEGIES[name](df_week)
            matched_rows = df_week.loc[sig.values].copy()
            matched_stocks = matched_rows.loc[matched_rows["date"] == buy_date, ["code", "name"]].drop_duplicates()
            n = len(matched_stocks)

            if n == 0:
                val.append({"strategy": name, "week_trades": 0, "week_win_rate": 0, "week_avg_ret": 0})
                continue

            buy_prices = df_week.loc[df_week["date"] == buy_date, ["code", "open"]].rename(columns={"open": "buy_price"})
            sell_prices = df_week.loc[df_week["date"] == sell_date, ["code", "close"]].rename(columns={"close": "sell_price"})

            merged_df = matched_stocks[["code", "name"]].merge(buy_prices, on="code", how="left") \
                                                       .merge(sell_prices, on="code", how="left")

            valid_df = merged_df.dropna(subset=["buy_price", "sell_price"])
            valid_df = valid_df[valid_df["buy_price"] > 0]
            rets = (valid_df["sell_price"] / valid_df["buy_price"] - 1).values

            if len(rets) > 0:
                win_rate = (rets > 0).sum() / len(rets) * 100
                avg_ret = rets.mean() * 100
            else:
                win_rate = 0.0
                avg_ret = 0.0

            sigs_df = matched_rows.loc[matched_rows["date"] == buy_date, ["code", "name", "date", "close", "pct_chg"]].head(8)
            sigs_out = sigs_df.to_dict("records")

            val.append({
                "strategy": name,
                "week_trades": int(n),
                "week_win_rate": round(win_rate, 2),
                "week_avg_ret": round(avg_ret, 2),
                "week_signals": sigs_out,
            })
        except Exception as e:
            val.append({"strategy": name, "error": str(e)})
    return val


# ─── 输出函数 ─────────────────────────────────────────────────────────────────

def print_results(results, val_results, backtest_start, backtest_end):
    print("\n" + "=" * 110)
    print(f"{'策略':<28} {'交易':>7} {'胜率%':>7} {'均盈%':>7} {'均亏%':>7} "
          f"{'盈亏比':>7} {'总收益%':>9} {'年化%':>8} {'最大回撤%':>10} {'夏普':>6} {'耗时':>5}")
    print("-" * 110)
    for r in results:
        print(
            f"  {r['strategy']:<26} {r['total_trades']:>7} "
            f"{r['win_rate']:>7.1f} {r['avg_win']:>7.2f} {r['avg_loss']:>7.2f} "
            f"{r['profit_loss_ratio']:>7.2f} {r['total_return']:>9.1f} "
            f"{r['annualized_return']:>8.1f} {r['max_drawdown']:>10.1f} "
            f"{r['sharpe_ratio']:>6.2f} {r.get('time_s',0):>5.1f}s"
        )

    print("=" * 110)
    print(f"\n回测区间: {backtest_start.date()} ~ {backtest_end.date()}")
    print(f"初始资金: {INITIAL_CAPITAL:,.0f} 元")
    if results:
        valid_results = [r for r in results if r["total_trades"] > 0]
        print(f"有信号策略: {len(valid_results)}/{len(results)}")
        print(f"平均胜率:   {np.mean([r['win_rate'] for r in valid_results]):.1f}%")
        print(f"平均总收益: {np.mean([r['total_return'] for r in valid_results]):.1f}%")
        print(f"平均年化:   {np.mean([r['annualized_return'] for r in valid_results]):.1f}%")

    if val_results:
        print("\n" + "=" * 75)
        print(f"{'5日验证 Top-5':^75}")
        print("-" * 75)
        print(f"{'策略':<26} {'5日交易':>9} {'胜率%':>9} {'平均收益%':>11}")
        print("-" * 75)
        for r in val_results:
            if "error" in r:
                print(f"  {r['strategy']:<24} [ERROR] {r['error']}")
            else:
                print(
                    f"  {r['strategy']:<24} {r['week_trades']:>9} "
                    f"{r['week_win_rate']:>9.1f} {r['week_avg_ret']:>11.2f}%"
                )
                for s in r.get("week_signals", [])[:5]:
                    dt = s["date"].date() if hasattr(s["date"], "date") else s["date"]
                    print(f"    -> {s['code']} {s['name']} @{dt} 涨幅{s['pct_chg']:.2f}%")
        print("=" * 75)


# ─── 获取胜率前10股票 ─────────────────────────────────────────────────────────

def get_top_stocks_by_win_rate(df_week, results, top_n=10):
    stock_info: Dict[str, dict] = {}

    all_dates = sorted(df_week["date"].unique())
    if len(all_dates) < 5:
        logger.warning("验证区间不足5个交易日")
        return []

    buy_date = all_dates[-5]
    sell_date = all_dates[-1]

    for r in results:
        strategy_name = r["strategy"]
        win_rate = r.get("win_rate", 0)
        total_trades = r.get("total_trades", 0)

        if total_trades == 0:
            continue

        try:
            sig = STRATEGIES[strategy_name](df_week)
            mask = sig.values
            if mask.sum() == 0:
                continue

            matched = df_week.loc[mask, ["code", "name", "open", "close"]].to_dict("records")
        except Exception:
            continue

        for record in matched:
            code = record.get("code", "")
            if not code:
                continue

            buy_price_row = df_week.loc[
                (df_week["code"] == code) & (df_week["date"] == buy_date),
                ["name", "open"],
            ]
            if buy_price_row.empty:
                continue
            buy_name = buy_price_row.iloc[0]["name"]
            buy_price = buy_price_row.iloc[0]["open"]

            sell_price_row = df_week.loc[
                (df_week["code"] == code) & (df_week["date"] == sell_date),
                ["close"],
            ]
            if sell_price_row.empty:
                continue
            sell_price = sell_price_row.iloc[0]["close"]

            sell_return = (sell_price / buy_price - 1) if buy_price > 0 else 0

            if code not in stock_info:
                stock_info[code] = {
                    "code": code,
                    "name": buy_name,
                    "strategies": [],
                    "win_rates": [],
                    "total_trades_list": [],
                    "buy_date": buy_date,
                    "buy_price": buy_price,
                    "sell_date": sell_date,
                    "sell_price": sell_price,
                    "sell_return": sell_return,
                }
            else:
                if sell_return > (stock_info[code]["sell_return"] or -999):
                    stock_info[code]["sell_return"] = sell_return

            stock_info[code]["strategies"].append(strategy_name)
            stock_info[code]["win_rates"].append(win_rate)
            stock_info[code]["total_trades_list"].append(total_trades)

    stock_list = []
    for code, info in stock_info.items():
        if not info["strategies"]:
            continue
        avg_win_rate = sum(info["win_rates"]) / len(info["win_rates"])
        stock_list.append({
            "code": code,
            "name": info["name"],
            "matched_strategies": info["strategies"],
            "win_rate": round(avg_win_rate, 2),
            "strategy_count": len(info["strategies"]),
            "buy_date": info["buy_date"],
            "buy_price": info["buy_price"],
            "sell_date": info["sell_date"],
            "sell_price": info["sell_price"],
            "sell_return": info["sell_return"],
        })

    stock_list.sort(key=lambda x: (x["sell_return"] if x["sell_return"] is not None else -999), reverse=True)
    return stock_list[:top_n]


def get_unique_strategies_from_results(results, top_n=10):
    return [r["strategy"] for r in results[:top_n] if r.get("total_trades", 0) > 0]


def get_next_day_recommendations(df_latest, top_stocks_or_results, results=None, top_n=10):
    strategy_results = results if results is not None else top_stocks_or_results
    unique_strategies = get_unique_strategies_from_results(strategy_results, top_n=top_n)
    logger.info(f"回测前{top_n}策略: {unique_strategies}")

    strategy_win_rate = {r["strategy"]: r.get("win_rate", 0) for r in strategy_results}

    if df_latest is None or df_latest.empty:
        logger.warning("没有最新数据可用于推荐")
        return [], []

    latest_date = df_latest["date"].max()
    logger.info(f"最新交易日: {latest_date.date()}, 股票数: {(df_latest['date'] == latest_date).sum()}")

    recommendations = []
    stock_strategy_scores: Dict[str, dict] = {}

    for strategy_name in unique_strategies:
        try:
            sig = STRATEGIES[strategy_name](df_latest)
            latest_mask = sig.values & (df_latest["date"] == latest_date).values
            if latest_mask.sum() == 0:
                continue

            matched = df_latest.loc[latest_mask, ["code", "name", "close", "pct_chg", "volume", "ma5", "ma20", "rsi6"]]
            win_rate = strategy_win_rate.get(strategy_name, 0)

            for _, row in matched.iterrows():
                code = row["code"]
                if code not in stock_strategy_scores:
                    stock_strategy_scores[code] = {
                        "code": code,
                        "name": row["name"],
                        "close": row["close"],
                        "pct_chg": row["pct_chg"],
                        "volume": row["volume"],
                        "rsi6": row["rsi6"],
                        "matched_strategies": [],
                        "win_rates": [],
                        "total_score": 0,
                    }
                stock_strategy_scores[code]["matched_strategies"].append(strategy_name)
                stock_strategy_scores[code]["win_rates"].append(win_rate)
                stock_strategy_scores[code]["total_score"] += win_rate

        except Exception as e:
            logger.error(f"策略 {strategy_name} 应用失败: {e}")
            continue

    for code, info in stock_strategy_scores.items():
        if not info["matched_strategies"]:
            continue

        avg_win_rate = sum(info["win_rates"]) / len(info["win_rates"])
        strategy_count = len(info["matched_strategies"])

        recommendations.append({
            "code": code,
            "name": info["name"],
            "close": info["close"],
            "pct_chg": info["pct_chg"],
            "rsi6": info["rsi6"],
            "matched_strategies": info["matched_strategies"],
            "avg_win_rate": round(avg_win_rate, 2),
            "strategy_count": strategy_count,
            "total_score": round(info["total_score"], 2),
        })

    recommendations.sort(key=lambda x: (x["strategy_count"], x["avg_win_rate"]), reverse=True)

    return recommendations[:top_n], unique_strategies


# ─── 执行主程序进行大盘复盘和个股决策 ─────────────────────────────────────────

def run_main_program_for_stocks(stocks: List[Dict]):
    """
    将股票代码转换为不带前缀的格式，然后执行主程序
    """
    if not stocks:
        logger.warning("没有股票需要分析")
        return False

    # 转换股票代码格式：sh.600519 -> 600519, sz.000001 -> 000001
    stock_codes = []
    for s in stocks:
        code = s.get("code", "")
        if code.startswith("sh.") or code.startswith("sz."):
            stock_codes.append(code.split(".", 1)[1])
        else:
            stock_codes.append(code)

    if not stock_codes:
        logger.warning("没有有效的股票代码")
        return False

    stock_code_str = ",".join(stock_codes)
    logger.info(f"准备分析的股票: {stock_code_str}")

    # 构建命令
    main_script = BASE_DIR / "main.py"
    cmd = [
        sys.executable,
        str(main_script),
        "--stocks", stock_code_str,
    ]

    logger.info(f"执行命令: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, cwd=str(BASE_DIR), check=True)
        logger.info(f"主程序执行完成，退出码: {result.returncode}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"主程序执行失败: {e}")
        return False
    except Exception as e:
        logger.error(f"执行主程序时出错: {e}")
        return False


# ─── 交易日判断 ─────────────────────────────────────────────────────────────

def is_trading_day(date):
    lg = bs.login()
    if lg.error_code != '0':
        logger.warning("baostock未正确初始化，无法判断交易日")
        return True

    if isinstance(date, datetime):
        date_str = date.strftime('%Y-%m-%d')
    else:
        date_str = str(date)

    rs = bs.query_trade_dates(start_date=date_str, end_date=date_str)

    if rs.error_code != '0':
        logger.warning(f"查询交易日失败: {rs.error_msg}")
        return True

    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())

    if not data_list:
        logger.warning(f"未获取到交易日数据: {date_str}")
        return True

    is_trading = data_list[0][1] == '1'
    return is_trading


# ─── 主函数 ─────────────────────────────────────────────────────────────────

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="强制运行（非交易日也执行）")
    args = parser.parse_args(argv)

    if not args.force and not is_trading_day(datetime.now()):
        logger.error("非交易日，程序退出（使用 --force 可强制运行）")
        return

    # 计算日期
    five_years_ago = (datetime.now() - pd.DateOffset(years=5)).strftime("%Y-%m-%d")
    today_str = datetime.now().strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info("22 策略 5 年回测 + 最近5日验证")
    logger.info(f"Numba加速: {'启用' if _HAS_NUMBA else '未安装（pip install numba）'}")
    logger.info("=" * 60)

    # 加载数据
    df_market = load_or_build_market_data_cache(five_years_ago, today_str)

    # 获取复权因子并计算前复权价格
    logger.info("获取复权因子并计算前复权价格...")
    codes = df_market["code"].unique().tolist()
    logger.info(f"共 {len(codes)} 只股票需要获取复权因子...")
    df_factor = load_or_fill_adjust_factor_cache(codes, five_years_ago, today_str)
    logger.info(f"获取到 {len(df_factor)} 条复权因子记录")
    df_adjusted = apply_forward_adjustment(df_market, df_factor)
    del df_market
    del df_factor
    gc.collect()

    market_cache_key = build_market_cache_key(five_years_ago, today_str)
    df_all = load_or_build_indicator_cache(df_adjusted, market_cache_key)
    del df_adjusted
    gc.collect()

    # 获取所有交易日并排序
    all_dates = sorted(df_all["date"].unique())
    if len(all_dates) < 6:
        logger.error(f"数据不足，最近交易日数量: {len(all_dates)}，需要至少6个")
        return

    # 动态计算日期
    backtest_end_date = all_dates[-6]
    backtest_start_date = all_dates[0]
    validate_start_date = all_dates[-5]
    validate_end_date = all_dates[-1]

    logger.info(f"回测区间: {backtest_start_date.date()} ~ {backtest_end_date.date()}")
    logger.info(f"验证区间: {validate_start_date.date()} ~ {validate_end_date.date()}")

    df_week = df_all[df_all["date"] >= pd.Timestamp(validate_start_date)].copy()
    mask_bt = df_all["date"] <= pd.Timestamp(backtest_end_date)
    df_bt = df_all.loc[mask_bt].reset_index(drop=True)
    del df_all
    gc.collect()

    results = run_backtests(df_bt)
    val_results = validate_week(df_week, results, TOP_N_VALIDATE)
    print_results(results, val_results, backtest_start_date, backtest_end_date)

    # 获取胜率前10股票
    top_stocks = get_top_stocks_by_win_rate(df_week, results, top_n=10)
    logger.info(f"5日验证胜率前10股票: {len(top_stocks)} 只")

    for idx, s in enumerate(top_stocks, 1):
        ret_str = f"{s['sell_return'] * 100:.2f}%" if s['sell_return'] is not None and pd.notna(s['sell_return']) else "N/A"
        logger.info(f"  [{idx}] {s['code']} {s['name']} - 胜率{s['win_rate']:.1f}% - 策略数{s['strategy_count']} - 收益{ret_str}")

    # 执行主程序进行大盘复盘和个股决策
    if top_stocks:
        logger.info("\n" + "=" * 60)
        logger.info("开始执行主程序进行大盘复盘和个股决策")
        logger.info("=" * 60)
        run_main_program_for_stocks(top_stocks)
    else:
        logger.warning("没有找到符合条件的股票，跳过主程序执行")

    logger.info("完成")


def load_or_build_market_data_cache(start: str, end: str) -> pd.DataFrame:
    cache_path = get_market_cache_path(start, end)
    if cache_path.exists():
        df = pd.read_parquet(cache_path)
        df = convert_market_numeric_columns_to_float32(df)
        return df
    market_data = load_data(start, end)
    market_data = convert_market_numeric_columns_to_float32(market_data)
    market_data.to_parquet(cache_path, index=False)
    return market_data


if __name__ == "__main__":
    main()
