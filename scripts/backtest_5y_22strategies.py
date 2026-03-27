#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
22 策略 5 年回测 + 本周验证
回测区间: 2021-01-01 ~ 2026-03-20
验证区间: 2026-03-20 ~ 2026-03-27
"""

import argparse
import logging
import os
import time
import gc
import traceback
from datetime import date, datetime, timedelta

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

# ─── 配置 ────────────────────────────────────────────────────────────
DB_URL = "postgresql://root:123629He@127.0.0.1:5431/baostock"
BACKTEST_START = "2021-01-01"
BACKTEST_END   = "2026-03-20"
VALIDATE_END   = "2026-03-27"
INITIAL_CAPITAL = 1000000.0
TOP_N_VALIDATE = 5
HOLDING_PERIODS = [1, 3, 5, 10]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backtest")


# ═══════════════════════════════════════════════════════════════════════
# 数据加载（chunked）
# ═══════════════════════════════════════════════════════════════════════

def load_data(start: str, end: str) -> pd.DataFrame:
    t0 = time.time()
    from sqlalchemy import create_engine
    engine = create_engine(DB_URL, pool_size=1, max_overflow=0)

    logger.info(f"加载数据 {start} ~ {end} ...")
    df = pd.read_sql(
        """
        SELECT code, name, date, open, high, low, close,
               volume, amount, pct_chg, turn, pe_ttm, pb_mrq
        FROM baostock_daily_history
        WHERE date BETWEEN %s AND %s
          AND trade_status = '1'
          AND is_st = '0'
          -- 仅保留 A 股股票，排除指数/ETF/北证指数等
          AND code ~ '^(sh\.6|sh\.688|sh\.8|sh\.4|sz\.0|sz\.2|sz\.3(?!9)|bj\.8)'
        ORDER BY code, date
        """,
        engine,
        params=(start, end),
        parse_dates=["date"],
        chunksize=100000,
    )

    chunks = []
    for i, chunk in enumerate(df):
        for col in ["open", "high", "low", "close", "volume", "amount",
                    "pct_chg", "turn", "pe_ttm", "pb_mrq"]:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
        chunks.append(chunk)
        logger.info(f"  chunk {i+1} loaded ({len(chunks[-1]):,} rows)")

    result = pd.concat(chunks, ignore_index=True)
    del chunks
    gc.collect()
    engine.dispose()

    logger.info(f"总计 {len(result):,} 行 × {result['code'].nunique()} 股，耗时 {time.time()-t0:.1f}s")
    return result


# ═══════════════════════════════════════════════════════════════════════
# 技术指标（向量化）
# ═══════════════════════════════════════════════════════════════════════

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    t0 = time.time()
    df = df.sort_values(["code", "date"]).reset_index(drop=True)

    g = df.groupby("code", group_keys=False)

    for w in [5, 10, 20, 60, 90, 120]:
        df[f"ma{w}"] = g["close"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    for w in [5, 10, 20]:
        df[f"vol_ma{w}"] = g["volume"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    df["vol_ma20"] = g["volume"].transform(lambda x: x.rolling(20, min_periods=1).mean())

    # RSI
    for w in [6, 12, 24]:
        delta = g["close"].diff()
        gain = delta.where(delta > 0, 0).transform(lambda x: x.rolling(w, min_periods=1).mean())
        loss = (-delta.where(delta < 0, 0)).transform(lambda x: x.rolling(w, min_periods=1).mean())
        df[f"rsi{w}"] = 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))

    # MACD(12,26,9)
    ema12 = g["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_dif"] = ema12 - ema26
    df["macd_dea"] = g["macd_dif"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    df["macd_hist"] = 2 * (df["macd_dif"] - df["macd_dea"])

    # MACD(14,53,5)
    ema14 = g["close"].transform(lambda x: x.ewm(span=14, adjust=False).mean())
    ema53 = g["close"].transform(lambda x: x.ewm(span=53, adjust=False).mean())
    df["macd_dif2"] = ema14 - ema53
    df["macd_dea2"] = g["macd_dif2"].transform(lambda x: x.ewm(span=5, adjust=False).mean())
    df["macd_hist2"] = 2 * (df["macd_dif2"] - df["macd_dea2"])

    # BOLL
    df["boll_mid"] = g["close"].transform(lambda x: x.rolling(20, min_periods=1).mean())
    std20 = g["close"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["boll_upper"] = df["boll_mid"] + 2 * std20
    df["boll_lower"] = df["boll_mid"] - 2 * std20

    # 辅助
    df["close_4d_ago"] = g["close"].shift(4)
    df["high_20d_max"] = g["high"].transform(lambda x: x.shift(1).rolling(20, min_periods=1).max())
    df["vol_60d_min"] = g["volume"].transform(lambda x: x.rolling(60, min_periods=20).min())
    df["vol_10d_mean"] = g["volume"].transform(lambda x: x.rolling(10, min_periods=1).mean())

    # 未来收益率
    for p in HOLDING_PERIODS:
        df[f"ret_{p}d"] = g["close"].transform(lambda x: x.pct_change(p).shift(-p))

    logger.info(f"指标计算完成，耗时 {time.time()-t0:.1f}s")
    gc.collect()
    return df


# ═══════════════════════════════════════════════════════════════════════
# 策略信号
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
    vs = df["vol_ma20"] + 2 * df.groupby("code")["volume"].transform(lambda x: x.rolling(20, min_periods=1).std())
    return ((df["volume"] > vs).astype(int) * 4 +
            (df["close"] > df.groupby("code")["high"].transform(lambda x: x.shift(1).rolling(20, min_periods=1).max())).astype(int) * 4 +
            (df["close"] > df["open"]).astype(int) * 2 +
            ((df["rsi6"] > 30) & (df["rsi6"] < 70)).astype(int) * 2)

def sig_ma_crossover(df): return _ma_cross(df) >= 6
def sig_volume_surge_std(df):
    vs = df["vol_ma20"] + 2 * df.groupby("code")["volume"].transform(lambda x: x.rolling(20, min_periods=1).std())
    return ((df["volume"] > vs).astype(int) * 4 +
            (df["close"] > df.groupby("code")["high"].transform(lambda x: x.shift(1).rolling(20, min_periods=1).max())).astype(int) * 4 +
            (df["close"] > df["open"]).astype(int) * 2 +
            ((df["rsi6"] > 30) & (df["rsi6"] < 70)).astype(int) * 2) >= 6

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
    g = df.groupby("code")["pct_chg"]
    # rolling 前10日（含昨收）
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
    "volume_surge_std":     sig_volume_surge_std,
    "wonderful_9_turn":     sig_wonderful_9_turn,
    "n_pattern":            sig_n_pattern,
    "limit_up_pullback":    sig_limit_up_pullback,
    "stable_then_limitup":   sig_stable_then_limitup,
    "monthly_macd_20ma":    sig_monthly_macd_20ma,
    "low_position_limitup": sig_low_position_limitup,
    "limitup_resonance":    sig_limitup_resonance,
    "bullish_engulfing":    sig_bullish_engulfing,
    "multi_ma_resonance":   sig_multi_ma_resonance,
    "ensemble":             sig_ensemble,
    "volume_breakout":      sig_volume_breakout,
    "bull_trend":           sig_bull_trend,
    "ma_golden_cross":      sig_ma_golden_cross,
    "shrink_pullback":      sig_shrink_pullback,
    "dragon_head":          sig_dragon_head,
    "emotion_cycle":        sig_emotion_cycle,
    "bottom_volume":        sig_bottom_volume,
}


# ═══════════════════════════════════════════════════════════════════════
# 绩效计算
# ═══════════════════════════════════════════════════════════════════════

def calc_metrics(returns: np.ndarray) -> Dict:
    """
    纯统计指标，避免溢出：
    - 总收益 = sum(r_i) / N * N = sum(r_i)，即累计算术收益
    - 年化 = 总收益 / (N * avg_holding / 252)
    - 夏普 = mean/std * sqrt(252/avg_holding)
    - 最大回撤 = 从有序收益序列估算（避免排序溢出）
    """
    r = np.asarray(returns, dtype=float)
    r = r[~np.isnan(r)]
    r = r[np.abs(r) < 5]  # 过滤极端值
    n = len(r)
    if n == 0:
        return {}

    wins = r[r > 0]
    losses = r[r < 0]
    wr = len(wins) / n * 100
    aw = float(wins.mean() * 100) if len(wins) else 0.0
    al = float(abs(losses.mean()) * 100) if len(losses) else 0.0
    pl = aw / al if al > 1e-10 else 0.0

    mean_ret = float(r.mean())
    total_r = mean_ret * n  # 累计算术收益率
    tr = total_r * 100

    avg_holding = sum(HOLDING_PERIODS) / len(HOLDING_PERIODS)
    yrs = n * avg_holding / 252.0
    ann = (total_r / max(yrs, 0.001)) * 100

    sr = 0.0
    if n > 1 and float(np.std(r)) > 1e-10:
        sr = float(mean_ret / np.std(r) * np.sqrt(252.0 / avg_holding))

    # 最大回撤（用有序序列的累计收益估算，避免排序溢出）
    sample = r[:min(n, 20000)]
    if len(sample) > 1:
        cur_eq = 0.0
        peak = 0.0
        max_dd = 0.0
        for ri in sample:
            cur_eq += ri
            if cur_eq > peak:
                peak = cur_eq
            dd = (peak - cur_eq) / (abs(peak) + 1e-10)
            if dd > max_dd:
                max_dd = dd
        mxd = max_dd * 100
    else:
        mxd = 0.0

    return {
        "total_trades": n,
        "win_rate": round(wr, 2),
        "avg_win": round(aw, 2),
        "avg_loss": round(al, 2),
        "profit_loss_ratio": round(pl, 2),
        "total_return": round(float(np.clip(tr, -1e10, 1e10)), 2),
        "annualized_return": round(float(np.clip(ann, -1e10, 1e10)), 2),
        "max_drawdown": round(float(np.clip(mxd, 0, 100)), 2),
        "sharpe_ratio": round(float(np.clip(sr, -100, 100)), 2),
    }


def backtest_single(args) -> Dict:
    name, df = args
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
                all_r.extend(signals[col].dropna().values.tolist())

        m = calc_metrics(np.array(all_r) if all_r else np.array([0]))
        m["strategy"] = name
        m["time_s"] = round(time.time() - t0, 1)
        return m
    except Exception as e:
        return {"strategy": name, "error": str(e), "time_s": round(time.time()-t0, 1)}


def run_backtests(df_bt: pd.DataFrame) -> List[Dict]:
    t0 = time.time()
    logger.info(f"顺序回测 {len(STRATEGIES)} 策略...")
    results = []
    for i, name in enumerate(STRATEGIES):
        r = backtest_single((name, df_bt))
        results.append(r)
        if "error" not in r:
            logger.info(f"  [{i+1}/{len(STRATEGIES)}] {name}: {r['total_trades']} 笔, "
                        f"胜率{r['win_rate']:.1f}%, 总收益{r['total_return']:.1f}%")
        else:
            logger.error(f"  [{i+1}/{len(STRATEGIES)}] {name}: {r['error']}")
    valid = sorted([r for r in results if "error" not in r],
                   key=lambda x: x.get("total_return", 0), reverse=True)
    logger.info(f"回测完成，耗时 {time.time()-t0:.1f}s")
    return valid


# ═══════════════════════════════════════════════════════════════════════
# 本周验证
# ═══════════════════════════════════════════════════════════════════════

def validate_week(df_week, top_results, top_n=5):
    top_names = [r["strategy"] for r in top_results[:top_n]]
    val = []
    logger.info(f"本周验证 {df_week['date'].min().date()} ~ {df_week['date'].max().date()}")
    for name in top_names:
        try:
            sig = STRATEGIES[name](df_week)
            s = df_week[sig]
            n = s["code"].count()
            if n == 0:
                val.append({"strategy": name, "week_trades": 0, "week_win_rate": 0, "week_avg_ret": 0})
                continue
            rets = []
            for _, row in s.iterrows():
                if "ret_1d" in df_week.columns:
                    r = df_week.at[row.name, "ret_1d"]
                    if not np.isnan(r):
                        rets.append(r)
            rets = np.array(rets)
            wr = (rets > 0).sum() / len(rets) * 100 if len(rets) > 0 else 0
            avg_r = rets.mean() * 100 if len(rets) > 0 else 0
            sigs_out = s[["code", "name", "date", "close", "pct_chg"]].head(8)
            val.append({
                "strategy": name, "week_trades": n,
                "week_win_rate": round(wr, 2), "week_avg_ret": round(avg_r, 2),
                "week_signals": sigs_out.to_dict("records"),
            })
        except Exception as e:
            val.append({"strategy": name, "error": str(e)})
    return val


# ═══════════════════════════════════════════════════════════════════════
# 输出
# ═══════════════════════════════════════════════════════════════════════

def print_results(results, val_results):
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
    print(f"\n回测区间: {BACKTEST_START} ~ {BACKTEST_END}")
    print(f"初始资金: {INITIAL_CAPITAL:,.0f} 元")
    if results:
        v = [r for r in results if r["total_trades"] > 0]
        print(f"有信号策略: {len(v)}/{len(results)}")
        print(f"平均胜率:   {np.mean([r['win_rate'] for r in v]):.1f}%")
        print(f"平均总收益: {np.mean([r['total_return'] for r in v]):.1f}%")
        print(f"平均年化:   {np.mean([r['annualized_return'] for r in v]):.1f}%")

    if val_results:
        print("\n" + "=" * 75)
        print(f"{'本周验证 Top-5 (2026-03-20 ~ 2026-03-27)':^75}")
        print("-" * 75)
        print(f"{'策略':<26} {'本周交易':>9} {'胜率%':>9} {'平均收益%':>11}")
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
                    print(f"    → {s['code']} {s['name']} @{dt}  涨幅{s['pct_chg']:.2f}%")
        print("=" * 75)


# ═══════════════════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("22 策略 5 年回测 + 本周验证")
    logger.info(f"回测区间: {BACKTEST_START} ~ {BACKTEST_END}")
    logger.info(f"验证区间: {BACKTEST_END} ~ {VALIDATE_END}")
    logger.info("=" * 60)

    df = load_data(BACKTEST_START, VALIDATE_END)
    df = compute_indicators(df)

    df_bt = df[df["date"] <= pd.Timestamp(BACKTEST_END)].copy()
    df_week = df[df["date"] >= pd.Timestamp(BACKTEST_END)].copy()

    del df
    gc.collect()

    results = run_backtests(df_bt)
    val_results = validate_week(df_week, results, TOP_N_VALIDATE)
    print_results(results, val_results)
    logger.info("完成")


if __name__ == "__main__":
    main()
