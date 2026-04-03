#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
22 策略 5 年回测 + 本周验证
回测区间: 2021-01-01 ~ 2026-03-20
验证区间: 2026-03-20 ~ 2026-03-27

优化点:
1. 并行回测: ThreadPoolExecutor (19策略并行，向量化释放GIL)
2. 向量化验证: 移除 validate_week 中的 iterrows()
3. 预计算共享指标: vol_std_20 等
4. Numba加速: calc_metrics
"""

import argparse
import logging
import os
import time
import gc
import ssl
import smtplib
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# ─── 配置 ────────────────────────────────────────────────────────────
INITIAL_CAPITAL = 1000000.0
TOP_N_VALIDATE = 5
HOLDING_PERIODS = [1, 3, 5, 10]
VALIDATE_DAYS = 5  # 验证区间交易日数量

# ═══════════════════════════════════════════════════════════════════════
# 邮件配置
# ═══════════════════════════════════════════════════════════════════════

class EmailConfig:
    SMTP_SERVER = os.environ.get('SMTP_SERVER', 'smtp.qq.com')
    SMTP_PORT = int(os.environ.get('SMTP_PORT', 465))
    SMTP_USER = os.environ.get('SMTP_USER')
    SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')
    RECIPIENTS = [r.strip() for r in os.environ.get('EMAIL_RECEIVERS', '').split(',') if r.strip()]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backtest")

# ═══════════════════════════════════════════════════════════════════════
# 数据加载（DuckDB）
# ═══════════════════════════════════════════════════════════════════════

def load_data(start: str, end: str) -> pd.DataFrame:
    """直接使用 psycopg2 加载数据"""
    t0 = time.time()
    import psycopg2

    logger.info(f"加载数据 {start} ~ {end} (PostgreSQL) ...")
    conn = psycopg2.connect(
        host=os.environ.get('PG_HOST', '127.0.0.1'),
        port=int(os.environ.get('PG_PORT', 5431)),
        database=os.environ.get('PG_DATABASE', 'baostock'),
        user=os.environ.get('PG_USER', 'root'),
        password=os.environ.get('PG_PASSWORD')
    )
    df = pd.read_sql(
        """
        SELECT code, name, date, open, high, low, close,
               volume, amount, pct_chg, turn, pe_ttm, pb_mrq
        FROM baostock_daily_history
        WHERE date BETWEEN %s AND %s
          AND trade_status = '1'
          AND is_st = '0'
          AND code ~ '^(sh\.6(?!88)|sh\.8|sh\.4|sz\.0|sz\.2|sz\.3(?!9))'
        ORDER BY code, date
        """,
        conn,
        params=(start, end),
        parse_dates=["date"],
    )
    conn.close()

    for col in ["open", "high", "low", "close", "volume", "amount",
                "pct_chg", "turn", "pe_ttm", "pb_mrq"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info(f"总计 {len(df):,} 行 × {df['code'].nunique()} 股，耗时 {time.time()-t0:.1f}s")
    return df


# ═══════════════════════════════════════════════════════════════════════
# 技术指标（向量化 + 预计算共享指标）
# ═══════════════════════════════════════════════════════════════════════

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    t0 = time.time()
    df = df.sort_values(["code", "date"]).reset_index(drop=True)

    g = df.groupby("code", group_keys=False)

    # ── 移动平均 ──
    for w in [5, 10, 20, 60, 90, 120]:
        df[f"ma{w}"] = g["close"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    # ── 成交量移动平均 ──
    for w in [5, 10, 20]:
        df[f"vol_ma{w}"] = g["volume"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    df["vol_ma20"] = g["volume"].transform(lambda x: x.rolling(20, min_periods=1).mean())

    # ══ 预计算共享指标（策略间共用） ══
    # 成交量20日标准差（用于 vol_surge 等策略）
    df["vol_std_20"] = g["volume"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["vol_threshold"] = df["vol_ma20"] + 2 * df["vol_std_20"]

    # 20日最高价（shift后）
    df["high_20d_max"] = g["high"].transform(lambda x: x.shift(1).rolling(20, min_periods=1).max())

    # 60日最低成交量
    df["vol_60d_min"] = g["volume"].transform(lambda x: x.rolling(60, min_periods=20).min())

    # 10日平均成交量
    df["vol_10d_mean"] = g["volume"].transform(lambda x: x.rolling(10, min_periods=1).mean())

    # 4日前收盘价
    df["close_4d_ago"] = g["close"].shift(4)

    # ── RSI ──
    for w in [6, 12, 24]:
        delta = g["close"].diff()
        gain = delta.where(delta > 0, 0).transform(lambda x: x.rolling(w, min_periods=1).mean())
        loss = (-delta.where(delta < 0, 0)).transform(lambda x: x.rolling(w, min_periods=1).mean())
        df[f"rsi{w}"] = 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))

    # ── MACD(12,26,9) ──
    ema12 = g["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_dif"] = ema12 - ema26
    df["macd_dea"] = g["macd_dif"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    df["macd_hist"] = 2 * (df["macd_dif"] - df["macd_dea"])

    # ── MACD(14,53,5) ──
    ema14 = g["close"].transform(lambda x: x.ewm(span=14, adjust=False).mean())
    ema53 = g["close"].transform(lambda x: x.ewm(span=53, adjust=False).mean())
    df["macd_dif2"] = ema14 - ema53
    df["macd_dea2"] = g["macd_dif2"].transform(lambda x: x.ewm(span=5, adjust=False).mean())
    df["macd_hist2"] = 2 * (df["macd_dif2"] - df["macd_dea2"])

    # ── BOLL ──
    df["boll_mid"] = g["close"].transform(lambda x: x.rolling(20, min_periods=1).mean())
    std20 = g["close"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["boll_upper"] = df["boll_mid"] + 2 * std20
    df["boll_lower"] = df["boll_mid"] - 2 * std20

    # ── 未来收益率 ──
    for p in HOLDING_PERIODS:
        df[f"ret_{p}d"] = g["close"].transform(lambda x: x.pct_change(p, fill_method=None).shift(-p))

    # ── 5日持仓收益率（第1天开盘买，第5天收盘卖）────
    # shift(-4) 表示取4天后的收盘价（从买入当天算起第5天）
    df["ret_5d_open_to_close"] = (g["close"].shift(-4) / df["open"] - 1)

    logger.info(f"指标计算完成，耗时 {time.time()-t0:.1f}s")
    gc.collect()
    return df


# ═══════════════════════════════════════════════════════════════════════
# 策略信号（使用预计算的共享指标）
# ═══════════════════════════════════════════════════════════════════════

def _ma_cross(df):
    ma5 = df["ma5"]; ma20 = df["ma20"]
    m5p = df.groupby("code")["ma5"].shift(1)
    m20p = df.groupby("code")["ma20"].shift(1)
    return ((ma5 > ma20) & (m5p <= m20p)).astype(int) * 3 + \
           (df["volume"] > df["vol_ma5"] * 1.5).astype(int) * 2 + \
           (df["macd_hist"] > 0).astype(int) * 2 + \
           ((df["rsi6"] > 30) & (df["rsi6"] < 70)).astype(int) * 1.5 + \
           (df["close"] > df["ma60"]).astype(int) * 1.5 + \
           (df["close"] > df["open"]).astype(int)

def _vol_surge(df):
    # 使用预计算的 vol_threshold
    return ((df["volume"] > df["vol_threshold"]).astype(int) * 4 +
            (df["close"] > df["high_20d_max"]).astype(int) * 4 +
            (df["close"] > df["open"]).astype(int) * 2 +
            ((df["rsi6"] > 30) & (df["rsi6"] < 70)).astype(int) * 2)

def sig_ma_crossover(df): return _ma_cross(df) >= 6
def sig_volume_surge_std(df):
    return _vol_surge(df) >= 6

def sig_wonderful_9_turn(df):
    close = df["close"]
    close_4d = df["close_4d_ago"]
    # 连续9天
    streak = close.groupby(df["code"]).transform(lambda x: (x < x.shift(4)).rolling(9).min().astype(bool))
    mp = df.groupby("code")["macd_hist"].shift(1)
    m20p = df.groupby("code")["ma20"].shift(1)
    m60p = df.groupby("code")["ma60"].shift(1)
    score = (streak.astype(int) * 4 +
             (df["rsi6"] < 35).astype(int) * 3 +
             ((df["macd_hist"] < 0) & (df["macd_hist"] > mp)).astype(int) * 3 +
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
    score = (pct.shift(1).fillna(0) >= 9.5).astype(int) * 2 + \
            (df["volume"] / vl.replace(0, 0.1) < 0.5).astype(int) * 3 + \
            (df["close"] >= hi * 0.97).astype(int) * 3 + \
            (df["volume"] > df["vol_ma5"] * 1.5).astype(int) * 4 + \
            (pct > 0).astype(int) * 2
    return score >= 8

def sig_stable_then_limitup(df):
    # 看前10日是否平稳（不含今日），今日是否涨停+放量
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

def sig_low_position_limitup(df):
    pct = df["pct_chg"]
    h20 = df["high_20d_max"]
    no_lim = ~(df.groupby("code")["pct_chg"].transform(lambda x: (x >= 9.5).rolling(20, min_periods=1).max().shift(1).fillna(0).astype(bool)))
    return (pct >= 9.5) & (df["close"] < h20 * 0.9) & (df["turn"] >= 5) & (df["close"] < 50) & no_lim

def sig_limitup_resonance(df):
    pct = df["pct_chg"]
    m20p = df.groupby("code")["ma20"].shift(1)
    r6p = df.groupby("code")["rsi6"].shift(1)
    limit_prev = pct.shift(1).fillna(0) >= 9.5
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
    big = (pp <= -7) & (body / rng.replace(0, 1)) >= 0.7
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
    c = df["close"]; m5 = df["ma5"]; m10 = df["ma10"]; m20 = df["ma20"]
    return (c > m5) & (m5 > m10) & (m10 > m20) & (df["rsi6"] > 40) & (df["rsi6"] < 70) & (df["volume"] > df["vol_ma5"])

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
    yang = df.groupby("code")["pct_chg"].shift(4) > 3
    yv = df.groupby("code")["volume"].shift(4)
    # vol[i] < vol[i-4] * 0.7 for each of 3 consecutive days
    vol_small = df["volume"] < yv * 0.7
    three_shrink = vol_small.groupby(df["code"]).transform(lambda x: x.rolling(3, min_periods=3).min())
    rise = (df["pct_chg"] > 0) & (df["close"] > df.groupby("code")["close"].shift(4))
    return yang & three_shrink & rise


# ═══════════════════════════════════════════════════════════════════════
# 策略注册表
# ═══════════════════════════════════════════════════════════════════════

STRATEGIES = {
    "ma_crossover":          sig_ma_crossover,
    "volume_surge_std":      sig_volume_surge_std,
    "wonderful_9_turn":      sig_wonderful_9_turn,
    "n_pattern":             sig_n_pattern,
    "limit_up_pullback":     sig_limit_up_pullback,
    "stable_then_limitup":  sig_stable_then_limitup,
    "monthly_macd_20ma":    sig_monthly_macd_20ma,
    "low_position_limitup": sig_low_position_limitup,
    "limitup_resonance":    sig_limitup_resonance,
    "bullish_engulfing":     sig_bullish_engulfing,
    "multi_ma_resonance":    sig_multi_ma_resonance,
    "ensemble":              sig_ensemble,
    "volume_breakout":       sig_volume_breakout,
    "bull_trend":            sig_bull_trend,
    "ma_golden_cross":       sig_ma_golden_cross,
    "shrink_pullback":       sig_shrink_pullback,
    "dragon_head":           sig_dragon_head,
    "emotion_cycle":         sig_emotion_cycle,
    "bottom_volume":         sig_bottom_volume,
}


# ═══════════════════════════════════════════════════════════════════════
# 绩效计算（Numba加速）
# ═══════════════════════════════════════════════════════════════════════

try:
    import numba
    _HAS_NUMBA = True
except ImportError:
    _HAS_NUMBA = False


if _HAS_NUMBA:
    @numba.njit(cache=True)
    def _calc_max_drawdown(r: np.ndarray) -> float:
        """Numba加速的最大回撤计算"""
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
        """Numba核心计算（返回原始值元组）"""
        # 过滤极端值
        mask = np.abs(r) < 5
        r = r[mask]

        wins = r[r > 0]
        losses = r[r < 0]
        n_valid = len(r)

        if n_valid == 0:
            return (0.0,) * 9

        wr = len(wins) / n_valid * 100.0
        aw = float(wins.mean() * 100.0) if len(wins) > 0 else 0.0
        al = float(abs(losses.mean()) * 100.0) if len(losses) > 0 else 0.0
        pl = aw / al if al > 1e-10 else 0.0

        mean_ret = float(r.mean())
        total_r = mean_ret * n_valid
        tr = total_r * 100.0

        yrs = n_valid * avg_holding / 252.0
        ann = (total_r / max(yrs, 0.001)) * 100.0

        sr = 0.0
        std_r = float(np.std(r))
        if n_valid > 1 and std_r > 1e-10:
            sr = float(mean_ret / std_r * np.sqrt(252.0 / avg_holding))

        # 最大回撤（Numba加速）
        sample = r[:min(n_valid, 20000)]
        mxd = _calc_max_drawdown(sample)

        return (wr, aw, al, pl, tr, ann, mxd, sr, float(n_valid))
else:
    # 无Numba时的纯Python回退
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
        wr = len(wins) / n_valid * 100.0
        aw = float(wins.mean() * 100.0) if len(wins) > 0 else 0.0
        al = float(abs(losses.mean()) * 100.0) if len(losses) > 0 else 0.0
        pl = aw / al if al > 1e-10 else 0.0

        mean_ret = float(r.mean())
        total_r = mean_ret * n_valid
        tr = total_r * 100.0

        yrs = n_valid * avg_holding / 252.0
        ann = (total_r / max(yrs, 0.001)) * 100.0

        sr = 0.0
        std_r = float(np.std(r))
        if n_valid > 1 and std_r > 1e-10:
            sr = float(mean_ret / std_r * np.sqrt(252.0 / avg_holding))

        sample = r[:min(n_valid, 20000)]
        mxd = _calc_max_drawdown(sample)

        return (wr, aw, al, pl, tr, ann, mxd, sr, float(n_valid))


def calc_metrics(returns: np.ndarray) -> Dict:
    """
    纯统计指标（Numba加速版）
    """
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


# ═══════════════════════════════════════════════════════════════════════
# 并行回测（ThreadPoolExecutor，向量化操作释放GIL）
# ═══════════════════════════════════════════════════════════════════════

def _backtest_single(name: str, df: pd.DataFrame) -> Dict:
    """单策略回测"""
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

        all_r = []
        for p in HOLDING_PERIODS:
            col = f"ret_{p}d"
            if col in signals.columns:
                vals = signals[col].dropna().values
                if len(vals) > 0:
                    all_r.extend(vals)

        m = calc_metrics(np.array(all_r) if all_r else np.array([0]))
        m["strategy"] = name
        m["time_s"] = round(time.time() - t0, 1)
        return m
    except Exception as e:
        return {"strategy": name, "error": str(e), "time_s": round(time.time()-t0, 1)}


def run_backtests(df_bt: pd.DataFrame) -> List[Dict]:
    """
    并行回测所有策略（ThreadPoolExecutor，向量化操作释放GIL）
    """
    t0 = time.time()
    n_strategies = len(STRATEGIES)
    import os
    n_workers = min(os.cpu_count() or 8, n_strategies)
    logger.info(f"并行回测 {n_strategies} 策略（{n_workers} threads）...")

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


# ═══════════════════════════════════════════════════════════════════════
# 最近5日验证（向量化）
# ═══════════════════════════════════════════════════════════════════════

def validate_week(df_week, top_results, top_n=5):
    """
    向量化验证：使用布尔索引直接提取收益
    验证方法：统一在倒数第5个交易日开盘买入，倒数第1个交易日收盘卖
    """
    top_names = [r["strategy"] for r in top_results[:top_n]]
    val = []
    logger.info(f"5日验证 {df_week['date'].min().date()} ~ {df_week['date'].max().date()}")

    # 获取统一买卖日期
    all_dates = sorted(df_week["date"].unique())
    if len(all_dates) < 5:
        logger.warning("验证区间不足5个交易日")
        return val

    buy_date = all_dates[-5]
    sell_date = all_dates[-1]

    for name in top_names:
        try:
            sig = STRATEGIES[name](df_week)
            mask = sig.values
            n = mask.sum()

            if n == 0:
                val.append({"strategy": name, "week_trades": 0, "week_win_rate": 0, "week_avg_ret": 0})
                continue

            # 计算每只股票的收益率：卖出价/买入价-1
            rets = []
            matched_stocks = df_week.loc[mask, ["code", "name"]].drop_duplicates()
            for _, row in matched_stocks.iterrows():
                code = row["code"]
                buy_row = df_week[(df_week["code"] == code) & (df_week["date"] == buy_date)]
                sell_row = df_week[(df_week["code"] == code) & (df_week["date"] == sell_date)]
                if not buy_row.empty and not sell_row.empty:
                    buy_price = buy_row.iloc[0]["open"]
                    sell_price = sell_row.iloc[0]["close"]
                    if buy_price > 0:
                        ret = sell_price / buy_price - 1
                        rets.append(ret)

            rets = np.array(rets) if rets else np.array([])

            if len(rets) > 0:
                wr = (rets > 0).sum() / len(rets) * 100
                avg_r = rets.mean() * 100
            else:
                wr = 0.0
                avg_r = 0.0

            # 获取信号详情
            sigs_df = df_week.loc[mask, ["code", "name", "date", "close", "pct_chg"]].head(8)
            sigs_out = sigs_df.to_dict("records")

            val.append({
                "strategy": name, "week_trades": int(n),
                "week_win_rate": round(wr, 2), "week_avg_ret": round(avg_r, 2),
                "week_signals": sigs_out,
            })
        except Exception as e:
            val.append({"strategy": name, "error": str(e)})
    return val


# ═══════════════════════════════════════════════════════════════════════
# 输出
# ═══════════════════════════════════════════════════════════════════════

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
        v = [r for r in results if r["total_trades"] > 0]
        print(f"有信号策略: {len(v)}/{len(results)}")
        print(f"平均胜率:   {np.mean([r['win_rate'] for r in v]):.1f}%")
        print(f"平均总收益: {np.mean([r['total_return'] for r in v]):.1f}%")
        print(f"平均年化:   {np.mean([r['annualized_return'] for r in v]):.1f}%")

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
                    print(f"    -> {s['code']} {s['name']} @{dt}  涨幅{s['pct_chg']:.2f}%")
        print("=" * 75)


# ═══════════════════════════════════════════════════════════════════════
# 邮件发送：胜率前10股票及匹配策略
# ═══════════════════════════════════════════════════════════════════════

def get_top_stocks_by_win_rate(df_week, results, top_n=10):
    """
    从验证结果中统计每只股票被哪些策略匹配，计算胜率并返回前N名
    验证方法：统一在倒数第5个交易日开盘买入，倒数第1个交易日收盘卖
    按实际卖出收益率降序排列
    """
    stock_info: Dict[str, dict] = {}

    # 获取验证区间的所有交易日
    all_dates = sorted(df_week["date"].unique())
    if len(all_dates) < 5:
        logger.warning("验证区间不足5个交易日")
        return []

    # 统一买卖日期：倒数第5个交易日开盘买，倒数第1个交易日收盘卖
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

            # 获取买入价格（统一用buy_date的开盘价）
            buy_price_row = df_week[(df_week["code"] == code) & (df_week["date"] == buy_date)]
            if buy_price_row.empty:
                continue
            buy_price = buy_price_row.iloc[0]["open"]

            # 获取卖出价格（统一用sell_date的收盘价）
            sell_price_row = df_week[(df_week["code"] == code) & (df_week["date"] == sell_date)]
            if sell_price_row.empty:
                continue
            sell_price = sell_price_row.iloc[0]["close"]

            # 计算收益率
            sell_return = (sell_price / buy_price - 1) if buy_price > 0 else 0

            if code not in stock_info:
                stock_info[code] = {
                    "code": code,
                    "name": record.get("name", ""),
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
                # 如果同一只股票被多个策略匹配，保留收益率最高的记录
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

    # 按实际卖出收益率降序排列
    stock_list.sort(key=lambda x: (x["sell_return"] if x["sell_return"] is not None else -999), reverse=True)
    return stock_list[:top_n]


def get_unique_strategies_from_top_stocks(top_stocks):
    """
    从胜率前10股票中提取去重后的策略列表
    """
    unique_strategies = set()
    for stock in top_stocks:
        for strategy in stock.get("matched_strategies", []):
            unique_strategies.add(strategy)
    return sorted(list(unique_strategies))


# 策略原因描述映射
STRATEGY_REASONS = {
    "ma_crossover": "均线金叉（5日均线上穿20日均线）+ 成交量放大 + MACD多头",
    "volume_surge_std": "成交量突破20日均值2倍标准差，放量上涨确认",
    "wonderful_9_turn": "神奇九转形态满足（9连跌后反弹征兆）+ RSI超卖",
    "n_pattern": "N字反包形态（回调后突破前高）",
    "limit_up_pullback": "涨停回调策略（涨停后缩量回调企稳）",
    "stable_then_limitup": "连续平稳后涨停（10日窄幅震荡后放量涨停）",
    "monthly_macd_20ma": "MACD月线金叉 + 20日线放量突破",
    "low_position_limitup": "低位涨停换手策略（低位放量涨停）",
    "limitup_resonance": "涨停回调量价共振（涨停后缩量+20日均线上行）",
    "bullish_engulfing": "孕阳线策略（大阴后阳线反包）",
    "multi_ma_resonance": "多均线共振（6条均线齐头向上）",
    "ensemble": "多策略共振（同时满足多个策略）",
    "volume_breakout": "成交量突破策略（量价齐升）",
    "bull_trend": "牛市趋势策略（均线多头排列）",
    "ma_golden_cross": "均线黄金交叉（5日金叉10日）",
    "shrink_pullback": "缩量回调策略（地量见地价）",
    "dragon_head": "龙头股策略（昨日涨停后高开）",
    "emotion_cycle": "情绪周期策略（RSI超卖+阳线）",
    "bottom_volume": "底部放量策略（地量后放量上涨）",
}

# 策略中文名称映射
STRATEGY_NAMES_CN = {
    "ma_crossover": "均线交叉策略",
    "volume_surge_std": "成交量突破策略",
    "wonderful_9_turn": "神奇九转策略",
    "n_pattern": "N字反包策略",
    "limit_up_pullback": "涨停回调策略",
    "stable_then_limitup": "连续平稳后涨停策略",
    "monthly_macd_20ma": "MACD月线金叉+20日线策略",
    "low_position_limitup": "低位涨停换手率策略",
    "limitup_resonance": "涨停回调量价共振策略",
    "bullish_engulfing": "孕阳线策略",
    "multi_ma_resonance": "多均线共振策略",
    "ensemble": "多策略 Ensemble 策略",
    "volume_breakout": "成交量突破策略",
    "bull_trend": "牛市趋势策略",
    "ma_golden_cross": "均线黄金交叉策略",
    "shrink_pullback": "缩量回调策略",
    "dragon_head": "龙头股策略",
    "emotion_cycle": "情绪周期策略",
    "bottom_volume": "底部放量策略",
}


def get_next_day_recommendations(df_latest, top_stocks, results, top_n=10):
    """
    基于胜率前10股票匹配的策略，在最新交易日数据中找出下个交易日推荐买的股票
    返回每只股票的推荐理由
    """
    # 1. 获取去重后的策略列表
    unique_strategies = get_unique_strategies_from_top_stocks(top_stocks)
    logger.info(f"胜率前10股票涉及去重策略: {unique_strategies}")

    # 2. 获取策略与胜率的映射
    strategy_win_rate = {r["strategy"]: r.get("win_rate", 0) for r in results}

    # 3. 获取最新交易日
    if df_latest is None or df_latest.empty:
        logger.warning("没有最新数据可用于推荐")
        return [], []

    latest_date = df_latest["date"].max()
    df_today = df_latest[df_latest["date"] == latest_date].copy()
    logger.info(f"最新交易日: {latest_date.date()}, 股票数: {len(df_today)}")

    # 4. 收集每只股票的推荐信息
    recommendations = []
    stock_strategy_scores: Dict[str, dict] = {}

    for strategy_name in unique_strategies:
        try:
            sig = STRATEGIES[strategy_name](df_today)
            mask = sig.values
            if mask.sum() == 0:
                continue

            matched = df_today.loc[mask, ["code", "name", "close", "pct_chg", "volume", "ma5", "ma20", "rsi6"]]
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

    # 5. 构建推荐列表
    for code, info in stock_strategy_scores.items():
        if not info["matched_strategies"]:
            continue

        # 计算平均胜率
        avg_win_rate = sum(info["win_rates"]) / len(info["win_rates"])

        # 生成推荐理由
        reasons = []
        for strat in info["matched_strategies"]:
            reason = STRATEGY_REASONS.get(strat, strat)
            reasons.append(reason)

        # 策略共振得分
        strategy_resonance = len(info["matched_strategies"])

        recommendations.append({
            "code": code,
            "name": info["name"],
            "close": info["close"],
            "pct_chg": info["pct_chg"],
            "rsi6": info["rsi6"],
            "matched_strategies": info["matched_strategies"],
            "avg_win_rate": round(avg_win_rate, 2),
            "strategy_count": strategy_resonance,
            "total_score": round(info["total_score"], 2),
            "reasons": reasons,
        })

    # 6. 按综合得分排序
    recommendations.sort(key=lambda x: (x["strategy_count"], x["avg_win_rate"]), reverse=True)

    return recommendations[:top_n], unique_strategies


def send_backtest_email(top_stocks, results, recommendations, unique_strategies, validate_start_date, validate_end_date, backtest_start_date, backtest_end_date):
    """
    发送邮件：胜率前10股票、下个交易日推荐及推荐理由
    """
    if not top_stocks:
        logger.warning("没有股票数据可发送邮件")
        return False

    date_str = datetime.now().strftime("%Y-%m-%d")

    # 创建邮件
    msg = MIMEMultipart()
    msg['From'] = EmailConfig.SMTP_USER
    msg['To'] = ", ".join(EmailConfig.RECIPIENTS)
    msg['Subject'] = f"{date_str}-22策略5日验证回测报告"

    # 构建HTML邮件正文
    body_parts = []

    # 标题
    validate_range = f"{validate_start_date.strftime('%Y-%m-%d')} ~ {validate_end_date.strftime('%Y-%m-%d')}"
    backtest_range = f"{backtest_start_date.strftime('%Y-%m-%d')} ~ {backtest_end_date.strftime('%Y-%m-%d')}"
    body_parts.append(f"<h2>{date_str} 22策略回测报告（5日验证）</h2>")
    body_parts.append(f"<p style='color:#666;'>回测区间: {backtest_range} | 验证区间: {validate_range} | 验证方法: {validate_start_date.strftime('%m-%d')}开盘买→{validate_end_date.strftime('%m-%d')}收盘卖</p>")
    body_parts.append("<hr>")

    # ===== 下交易日推荐 =====（放在最前面，最重要）
    if recommendations:
        body_parts.append("<h3 style='color:red;'>下个交易日推荐股票（基于胜率前10策略共振）</h3>")
        unique_strategies_cn = [STRATEGY_NAMES_CN.get(s, s) for s in unique_strategies]
        body_parts.append(f"<p style='color:#666;'>涉及策略: {' | '.join(unique_strategies_cn)}</p>")
        body_parts.append("<table border='1' cellpadding='4' cellspacing='0' style='border-collapse:collapse;'>")
        body_parts.append("<thead><tr>")
        body_parts.append("<th>排名</th><th>代码</th><th>名称</th><th>现价</th><th>涨跌幅</th><th>RSI</th>")
        body_parts.append("<th>策略共振数</th><th>平均胜率%</th><th>推荐理由</th>")
        body_parts.append("</tr></thead><tbody>")

        for idx, rec in enumerate(recommendations, 1):
            reasons_str = " | ".join(rec["reasons"])
            rsi_val = f"{rec['rsi6']:.1f}" if pd.notna(rec.get("rsi6")) else "N/A"
            body_parts.append("<tr>")
            body_parts.append(f"<td>{idx}</td>")
            body_parts.append(f"<td><b>{rec['code']}</b></td>")
            body_parts.append(f"<td>{rec['name']}</td>")
            body_parts.append(f"<td>{rec['close']:.2f}</td>" if pd.notna(rec.get("close")) else "<td>N/A</td>")
            body_parts.append(f"<td style='color:{'red' if rec.get('pct_chg', 0) > 0 else 'green'};'>"
                              f"{rec.get('pct_chg', 0):.2f}%</td>")
            body_parts.append(f"<td>{rsi_val}</td>")
            body_parts.append(f"<td><b>{rec['strategy_count']}</b></td>")
            body_parts.append(f"<td><b>{rec['avg_win_rate']:.1f}</b></td>")
            body_parts.append(f"<td style='font-size:11px;'>{reasons_str}</td>")
            body_parts.append("</tr>")

        body_parts.append("</tbody></table><br><hr>")
    else:
        body_parts.append("<h3>下个交易日推荐股票</h3><p>暂无推荐（数据不足或无符合条件股票）</p><hr>")

    # 策略汇总表
    body_parts.append("<h3>策略表现汇总（按总收益排序）</h3>")
    body_parts.append("<table border='1' cellpadding='4' cellspacing='0' style='border-collapse:collapse;'>")
    body_parts.append("<thead><tr>")
    body_parts.append("<th>策略</th><th>交易次数</th><th>胜率%</th><th>均盈%</th><th>均亏%</th><th>总收益%</th>")
    body_parts.append("</tr></thead><tbody>")
    for r in results[:15]:
        strategy_name_cn = STRATEGY_NAMES_CN.get(r['strategy'], r['strategy'])
        body_parts.append("<tr>")
        body_parts.append(f"<td>{strategy_name_cn}</td>")
        body_parts.append(f"<td>{r.get('total_trades', 0)}</td>")
        body_parts.append(f"<td>{r.get('win_rate', 0):.1f}</td>")
        body_parts.append(f"<td>{r.get('avg_win', 0):.2f}</td>")
        body_parts.append(f"<td>{r.get('avg_loss', 0):.2f}</td>")
        body_parts.append(f"<td>{r.get('total_return', 0):.1f}</td>")
        body_parts.append("</tr>")
    body_parts.append("</tbody></table><br><hr>")

    # 胜率前10股票表（5日验证）
    buy_str = validate_start_date.strftime("%m-%d")
    sell_str = validate_end_date.strftime("%m-%d")
    body_parts.append(f"<h3>5日验证胜率前10股票（{buy_str}开盘买→{sell_str}收盘卖）</h3>")
    body_parts.append("<table border='1' cellpadding='4' cellspacing='0' style='border-collapse:collapse;'>")
    body_parts.append("<thead><tr>")
    body_parts.append("<th>排名</th><th>代码</th><th>名称</th><th>胜率%</th><th>匹配策略数</th>")
    body_parts.append("<th>买入日期</th><th>买入价(开)</th><th>卖出日期</th><th>卖出价(收)</th><th>收益率%</th>")
    body_parts.append("<th>匹配的策略</th>")
    body_parts.append("</tr></thead><tbody>")

    for idx, stock in enumerate(top_stocks, 1):
        # 去重策略
        unique_strategies_set = list(set(stock["matched_strategies"]))
        strategies_cn = [STRATEGY_NAMES_CN.get(s, s) for s in unique_strategies_set]
        strategies_str = " | ".join(strategies_cn)

        buy_date = stock.get("buy_date")
        buy_price = stock.get("buy_price")
        sell_date = stock.get("sell_date")
        sell_price = stock.get("sell_price")
        sell_return = stock.get("sell_return")

        buy_date_str = buy_date.strftime("%Y-%m-%d") if buy_date and hasattr(buy_date, "strftime") else str(buy_date) if buy_date else "N/A"
        buy_price_str = f"{buy_price:.2f}" if buy_price is not None and pd.notna(buy_price) else "N/A"
        sell_date_str = sell_date.strftime("%Y-%m-%d") if sell_date and hasattr(sell_date, "strftime") else str(sell_date) if sell_date else "N/A"
        sell_price_str = f"{sell_price:.2f}" if sell_price is not None and pd.notna(sell_price) else "N/A"

        return_str = "N/A"
        return_color = ""
        if sell_return is not None and pd.notna(sell_return):
            return_val = sell_return * 100
            return_str = f"{return_val:.2f}%"
            return_color = "color:red;" if return_val > 0 else "color:green;"

        body_parts.append("<tr>")
        body_parts.append(f"<td>{idx}</td>")
        body_parts.append(f"<td>{stock['code']}</td>")
        body_parts.append(f"<td>{stock['name']}</td>")
        body_parts.append(f"<td><b>{stock['win_rate']:.1f}</b></td>")
        body_parts.append(f"<td>{stock['strategy_count']}</td>")
        body_parts.append(f"<td>{buy_date_str}</td>")
        body_parts.append(f"<td>{buy_price_str}</td>")
        body_parts.append(f"<td>{sell_date_str}</td>")
        body_parts.append(f"<td>{sell_price_str}</td>")
        body_parts.append(f"<td style='{return_color}font-weight:bold;'>{return_str}</td>")
        body_parts.append(f"<td style='font-size:11px;'>{strategies_str}</td>")
        body_parts.append("</tr>")

    body_parts.append("</tbody></table>")

    body = "".join(body_parts)
    msg.attach(MIMEText(body, 'html', 'utf-8'))

    # 发送邮件
    max_retries = 3
    for attempt in range(max_retries):
        try:
            context = ssl.create_default_context()
            server = smtplib.SMTP_SSL(EmailConfig.SMTP_SERVER, EmailConfig.SMTP_PORT, context=context)
            server.login(EmailConfig.SMTP_USER, EmailConfig.SMTP_PASSWORD)
            server.sendmail(EmailConfig.SMTP_USER, EmailConfig.RECIPIENTS, msg.as_string())
            server.quit()
            logger.info(f"回测邮件发送成功（含下交易日推荐 {len(recommendations)} 只）")
            return True
        except Exception as e:
            logger.error(f"发送邮件失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)

    return False


# ═══════════════════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    # 计算回测和验证日期
    # 加载足够回测的数据（从数据库取5年前至今的数据）
    five_years_ago = (datetime.now() - pd.DateOffset(years=5)).strftime("%Y-%m-%d")
    today_str = datetime.now().strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info("22 策略 5 年回测 + 最近5日验证")
    logger.info(f"Numba加速: {'启用' if _HAS_NUMBA else '未安装（pip install numba）'}")
    logger.info(f"CPU核心: {os.cpu_count()}")
    logger.info("=" * 60)

    # 加载数据（从5年前到今天）
    df_all = load_data(five_years_ago, today_str)
    df_all = compute_indicators(df_all)

    # 获取所有交易日并排序
    all_dates = sorted(df_all["date"].unique())
    if len(all_dates) < 6:
        logger.error(f"数据不足，最近交易日数量: {len(all_dates)}，需要至少6个")
        return

    # 动态计算日期
    # 回测结束日期：从今天倒数第6个交易日
    backtest_end_date = all_dates[-6]
    # 回测开始日期：5年前第一个交易日
    backtest_start_date = all_dates[0]
    # 验证开始日期：从今天倒数第5个交易日
    validate_start_date = all_dates[-5]
    # 验证结束日期：最后一个交易日（今天）
    validate_end_date = all_dates[-1]

    logger.info(f"回测区间: {backtest_start_date.date()} ~ {backtest_end_date.date()}")
    logger.info(f"验证区间: {validate_start_date.date()} ~ {validate_end_date.date()} (共{VALIDATE_DAYS}个交易日)")

    # 回测数据
    df_bt = df_all[df_all["date"] <= pd.Timestamp(backtest_end_date)].copy()

    # 验证数据
    df_week = df_all[df_all["date"] >= pd.Timestamp(validate_start_date)].copy()

    del df_all
    gc.collect()

    results = run_backtests(df_bt)
    val_results = validate_week(df_week, results, TOP_N_VALIDATE)
    print_results(results, val_results, backtest_start_date, backtest_end_date)

    # 胜率前10股票
    top_stocks = get_top_stocks_by_win_rate(df_week, results, top_n=10)
    logger.info(f"5日验证胜率前10股票: {len(top_stocks)} 只")

    # 下个交易日推荐（基于胜率前10策略）
    recommendations, unique_strategies = get_next_day_recommendations(df_week, top_stocks, results, top_n=10)
    if recommendations:
        logger.info(f"下交易日推荐股票 {len(recommendations)} 只:")
        for rec in recommendations:
            logger.info(f"  {rec['code']} {rec['name']} - 策略共振{rec['strategy_count']}个 - 胜率{rec['avg_win_rate']:.1f}%")
            for reason in rec["reasons"]:
                logger.info(f"    - {reason}")

    # 发送邮件
    if top_stocks:
        send_backtest_email(top_stocks, results, recommendations, unique_strategies, validate_start_date, validate_end_date, backtest_start_date, backtest_end_date)
    else:
        logger.warning("没有找到符合条件的股票，跳过邮件发送")

    logger.info("完成")


if __name__ == "__main__":
    main()
