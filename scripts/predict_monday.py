#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
预测下周一（2026-03-30）买入候选股
基于回测最优策略：low_position_limitup / emotion_cycle / stable_then_limitup
筛选最近交易日（2026-03-27）触发信号的股票
"""

import logging
import time
import gc
import sys
import os

import numpy as np
import pandas as pd
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("predict")

# ─── 配置 ────────────────────────────────────────────────────────────
DB_HOST   = "127.0.0.1"
DB_PORT   = 5431
DB_NAME   = "baostock"
DB_USER   = "root"
DB_PASS   = "123629He"

# 加载近90个交易日数据（保证指标计算准确）
DATA_END   = "2026-03-27"
DATA_START = "2025-12-01"

# 触发信号的日期（最近一个交易日）
SIGNAL_DATE = "2026-03-27"

# 输出Top N
TOP_N = 30

# 重点策略（按回测评分加权）
STRATEGY_WEIGHTS = {
    "low_position_limitup":  10.0,   # 胜率65.2%，夏普3.31，最优
    "stable_then_limitup":    7.0,   # 年化54.5%，夏普0.70
    "emotion_cycle":          7.0,   # 本周胜率77.5%，最强
    "shrink_pullback":        6.0,   # 夏普2.09，回撤最小
    "ma_golden_cross":        4.0,   # 年化23.6%
    "multi_ma_resonance":     3.5,
    "n_pattern":              3.0,
    "monthly_macd_20ma":      3.0,
    "wonderful_9_turn":       2.5,
    "ma_crossover":           2.0,
    "volume_breakout":        2.0,
    "bull_trend":             1.5,
    "ensemble":               1.5,
    "volume_surge_std":       1.0,
    "limit_up_pullback":      1.0,
}

# ─── 数据加载 ─────────────────────────────────────────────────────────
def load_data(start: str, end: str) -> pd.DataFrame:
    t0 = time.time()
    logger.info(f"加载数据 {start} ~ {end} (PostgreSQL) ...")
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    df = pd.read_sql(
        """
        SELECT code, name, date, open, high, low, close,
               volume, amount, pct_chg, turn, pe_ttm, pb_mrq
        FROM baostock_daily_history
        WHERE date BETWEEN %s AND %s
          AND trade_status = '1'
          AND is_st = '0'
          AND code ~ '^(sh\\.6|sh\\.688|sh\\.8|sh\\.4|sz\\.0|sz\\.2|sz\\.3(?!9)|bj\\.8)'
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


# ─── 技术指标 ─────────────────────────────────────────────────────────
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    t0 = time.time()
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    g = df.groupby("code", group_keys=False)

    for w in [5, 10, 20, 60, 90, 120]:
        df[f"ma{w}"] = g["close"].transform(lambda x: x.rolling(w, min_periods=1).mean())
    for w in [5, 10, 20]:
        df[f"vol_ma{w}"] = g["volume"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    df["vol_std_20"]   = g["volume"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["vol_threshold"]= df["vol_ma20"] + 2 * df["vol_std_20"]
    df["high_20d_max"] = g["high"].transform(lambda x: x.shift(1).rolling(20, min_periods=1).max())
    df["vol_60d_min"]  = g["volume"].transform(lambda x: x.rolling(60, min_periods=20).min())
    df["vol_10d_mean"] = g["volume"].transform(lambda x: x.rolling(10, min_periods=1).mean())
    df["close_4d_ago"] = g["close"].shift(4)

    for w in [6, 12, 24]:
        delta = g["close"].diff()
        gain  = delta.where(delta > 0, 0).transform(lambda x: x.rolling(w, min_periods=1).mean())
        loss  = (-delta.where(delta < 0, 0)).transform(lambda x: x.rolling(w, min_periods=1).mean())
        df[f"rsi{w}"] = 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))

    ema12 = g["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_dif"]  = ema12 - ema26
    df["macd_dea"]  = g["macd_dif"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    df["macd_hist"] = 2 * (df["macd_dif"] - df["macd_dea"])

    df["boll_mid"]   = g["close"].transform(lambda x: x.rolling(20, min_periods=1).mean())
    std20            = g["close"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["boll_upper"] = df["boll_mid"] + 2 * std20
    df["boll_lower"] = df["boll_mid"] - 2 * std20

    logger.info(f"指标计算完成，耗时 {time.time()-t0:.1f}s")
    gc.collect()
    return df


# ─── 策略信号（复用 backtest_5y_22strategies.py 逻辑） ────────────────
def sig_ma_crossover(df):
    ma5 = df["ma5"]; ma20 = df["ma20"]
    m5p  = df.groupby("code")["ma5"].shift(1)
    m20p = df.groupby("code")["ma20"].shift(1)
    return ((ma5 > ma20) & (m5p <= m20p)).astype(int) * 3 + \
           (df["volume"] > df["vol_ma5"] * 1.5).astype(int) * 2 + \
           (df["macd_hist"] > 0).astype(int) * 2 + \
           ((df["rsi6"] > 30) & (df["rsi6"] < 70)).astype(int) * 1.5 + \
           (df["close"] > df["ma60"]).astype(int) * 1.5 + \
           (df["close"] > df["open"]).astype(int) >= 6

def sig_volume_surge_std(df):
    return ((df["volume"] > df["vol_threshold"]).astype(int) * 4 +
            (df["close"] > df["high_20d_max"]).astype(int) * 4 +
            (df["close"] > df["open"]).astype(int) * 2 +
            ((df["rsi6"] > 30) & (df["rsi6"] < 70)).astype(int) * 2) >= 6

def sig_wonderful_9_turn(df):
    streak = df["close"].groupby(df["code"]).transform(
        lambda x: (x < x.shift(4)).rolling(9).min().astype(bool))
    mp   = df.groupby("code")["macd_hist"].shift(1)
    m20p = df.groupby("code")["ma20"].shift(1)
    m60p = df.groupby("code")["ma60"].shift(1)
    score = (streak.astype(int) * 4 +
             (df["rsi6"] < 35).astype(int) * 3 +
             ((df["macd_hist"] < 0) & (df["macd_hist"] > mp)).astype(int) * 3 +
             (df["volume"] > df["vol_ma5"] * 1.2).astype(int) * 2 +
             ((df["ma20"] > m60p) & (df["ma20"] > m20p) & (df["ma60"] > m60p)).astype(int) * 2 +
             (df["close"] >= df["ma20"] * 0.98).astype(int) * 2 +
             (df["close"] > df["open"]).astype(int))
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
    hi  = df["high"].where(pct >= 9.5).groupby(df["code"]).ffill().fillna(0)
    vl  = df["volume"].where(pct >= 9.5).groupby(df["code"]).ffill().fillna(0.1)
    score = (pct.shift(1).fillna(0) >= 9.5).astype(int) * 2 + \
            (df["volume"] / vl.replace(0, 0.1) < 0.5).astype(int) * 3 + \
            (df["close"] >= hi * 0.97).astype(int) * 3 + \
            (df["volume"] > df["vol_ma5"] * 1.5).astype(int) * 4 + \
            (pct > 0).astype(int) * 2
    return score >= 8

def sig_stable_then_limitup(df):
    mx     = df.groupby("code")["pct_chg"].transform(lambda x: x.shift(1).rolling(10, min_periods=1).max())
    mn     = df.groupby("code")["pct_chg"].transform(lambda x: x.shift(1).rolling(10, min_periods=1).min())
    stable = (mx < 5) & (mn > -5)
    return stable & (df["pct_chg"] >= 9.5) & (df["volume"] > df["vol_10d_mean"] * 1.5)

def sig_monthly_macd_20ma(df):
    dp   = df.groupby("code")["macd_dif"].shift(1)
    dep  = df.groupby("code")["macd_dea"].shift(1)
    m20p = df.groupby("code")["ma20"].shift(1)
    score = (((df["macd_dif"] > df["macd_dea"]) & (dp <= dep)).astype(int) * 5 +
             (df["ma20"] > m20p).astype(int) * 4 +
             (df["close"] >= df["ma20"] * 0.97).astype(int) * 3 +
             (df["volume"] > df["vol_ma5"] * 1.5).astype(int) * 4 +
             (df["close"] > df["open"]).astype(int) * 2 +
             ((df["rsi6"] > 40) & (df["rsi6"] < 70)).astype(int) * 2)
    return score >= 10

def sig_low_position_limitup(df):
    pct   = df["pct_chg"]
    h20   = df["high_20d_max"]
    no_lim= ~(df.groupby("code")["pct_chg"].transform(
        lambda x: (x >= 9.5).rolling(20, min_periods=1).max().shift(1).fillna(0).astype(bool)))
    return (pct >= 9.5) & (df["close"] < h20 * 0.9) & (df["turn"] >= 5) & (df["close"] < 50) & no_lim

def sig_multi_ma_resonance(df):
    m5p  = df.groupby("code")["ma5"].shift(1)
    m10p = df.groupby("code")["ma10"].shift(1)
    dp2  = df.groupby("code")["macd_dif"].shift(1)
    dep2 = df.groupby("code")["macd_dea"].shift(1)
    mp   = df.groupby("code")["macd_hist"].shift(1)
    r6p  = df.groupby("code")["rsi6"].shift(1)
    r12p = df.groupby("code")["rsi12"].shift(1)
    r24p = df.groupby("code")["rsi24"].shift(1)
    bup  = df.groupby("code")["boll_upper"].shift(1)
    bmp  = df.groupby("code")["boll_mid"].shift(1)
    bull_ma  = ((df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"]) &
                (df["ma20"] > df["ma60"]) & (df["ma60"] > df["ma90"]) &
                (df["ma90"] > df["ma120"]) & (df["ma5"] > m5p))
    cross_5_10 = (df["ma5"] > df["ma10"]) & (m5p <= m10p)
    score = (bull_ma.astype(int) * 4 + cross_5_10.astype(int) * 4 +
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
    return (c > df["ma5"]) & (df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"]) & \
           (df["rsi6"] > 40) & (df["rsi6"] < 70) & (df["volume"] > df["vol_ma5"])

def sig_ma_golden_cross(df):
    m5p  = df.groupby("code")["ma5"].shift(1)
    m10p = df.groupby("code")["ma10"].shift(1)
    cross = (df["ma5"] > df["ma10"]) & (m5p <= m10p)
    return cross & (df["volume"] > df["vol_ma5"] * 1.2) & (df["close"] > df["ma10"])

def sig_shrink_pullback(df):
    s1 = df["volume"] < df["vol_ma5"] * 0.3
    s2 = (df["close"] - df["ma10"]).abs() / df["ma10"] < 0.03
    s3 = df["pct_chg"] > 0
    return s1 & s2 & s3

def sig_emotion_cycle(df):
    return (df["rsi6"] < 35) & (df["close"] > df["open"]) & (df["volume"] > df["vol_ma5"])


STRATEGIES = {
    "low_position_limitup": sig_low_position_limitup,
    "stable_then_limitup":  sig_stable_then_limitup,
    "emotion_cycle":        sig_emotion_cycle,
    "shrink_pullback":      sig_shrink_pullback,
    "ma_golden_cross":      sig_ma_golden_cross,
    "multi_ma_resonance":   sig_multi_ma_resonance,
    "n_pattern":            sig_n_pattern,
    "monthly_macd_20ma":    sig_monthly_macd_20ma,
    "wonderful_9_turn":     sig_wonderful_9_turn,
    "ma_crossover":         sig_ma_crossover,
    "volume_breakout":      sig_volume_breakout,
    "bull_trend":           sig_bull_trend,
    "ensemble":             sig_ensemble,
    "volume_surge_std":     sig_volume_surge_std,
    "limit_up_pullback":    sig_limit_up_pullback,
}


# ─── 主流程 ───────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info(f"下周一候选股预测 — 信号日: {SIGNAL_DATE}，目标交易日: 2026-03-30")
    logger.info("=" * 60)

    df = load_data(DATA_START, DATA_END)
    df = compute_indicators(df)

    # 只取信号日当天数据
    sig_df = df[df["date"] == SIGNAL_DATE].copy()
    if sig_df.empty:
        logger.warning(f"信号日 {SIGNAL_DATE} 没有数据，请确认该日为交易日！")
        sys.exit(1)

    logger.info(f"信号日 {SIGNAL_DATE} 共 {len(sig_df)} 只股票")

    # 对每只股票计算综合评分
    results = []
    triggered_strategies = {name: set() for name in STRATEGIES}

    for name, sig_fn in STRATEGIES.items():
        weight = STRATEGY_WEIGHTS.get(name, 1.0)
        try:
            mask = sig_fn(df)
            day_mask = mask & (df["date"] == SIGNAL_DATE)
            codes = df[day_mask]["code"].unique()
            triggered_strategies[name] = set(codes)
        except Exception as e:
            logger.warning(f"策略 {name} 出错: {e}")

    # 汇总每只股票的综合评分
    all_codes = sig_df["code"].unique()
    stock_scores = {}

    for code in all_codes:
        score = 0.0
        hit_strategies = []
        for name, codes_set in triggered_strategies.items():
            if code in codes_set:
                w = STRATEGY_WEIGHTS.get(name, 1.0)
                score += w
                hit_strategies.append(name)
        if score > 0:
            stock_scores[code] = {"score": score, "strategies": hit_strategies}

    # 合并股票基本信息
    sig_info = sig_df.set_index("code")[["name", "close", "pct_chg", "volume", "turn",
                                          "rsi6", "ma5", "ma20", "ma60"]].to_dict("index")

    for code, info in stock_scores.items():
        si = sig_info.get(code, {})
        info.update(si)

    # 按评分排序，取Top N
    sorted_stocks = sorted(stock_scores.items(), key=lambda x: x[1]["score"], reverse=True)
    top_stocks = sorted_stocks[:TOP_N]

    # ── 打印结果 ──
    print("\n" + "=" * 100)
    print(f"  下周一（2026-03-30）买入候选股 Top {TOP_N}  |  基于 {SIGNAL_DATE} 收盘信号")
    print("=" * 100)
    print(f"{'排名':<4} {'代码':<12} {'名称':<10} {'综合评分':>8} {'收盘价':>8} {'涨跌幅':>7} "
          f"{'换手率':>6} {'RSI6':>6}  触发策略")
    print("-" * 100)

    for rank, (code, info) in enumerate(top_stocks, 1):
        name_s    = str(info.get("name", ""))[:8]
        close_s   = f"{info.get('close', 0):.2f}"
        pct_s     = f"{info.get('pct_chg', 0):.2f}%"
        turn_s    = f"{info.get('turn', 0):.1f}%"
        rsi_s     = f"{info.get('rsi6', 0):.1f}"
        score_s   = f"{info['score']:.1f}"
        strats    = ", ".join(info["strategies"])

        # 高权重策略高亮
        star = "***" if info["score"] >= 10 else ("**" if info["score"] >= 6 else " ")
        print(f"{rank:<4} {code:<12} {name_s:<10} {score_s:>8} {close_s:>8} {pct_s:>7} "
              f"{turn_s:>6} {rsi_s:>6}  {star} {strats}")

    print("=" * 100)
    print(f"\n说明:")
    print(f"  *** 综合评分>=10分：强烈关注，多策略共振")
    print(f"  **  综合评分6-10分：重点关注")
    print(f"  评分权重：low_position_limitup(10) > stable_then_limitup/emotion_cycle(7) > shrink_pullback(6) > ...")
    print(f"\n[警告] 以上仅为量化模型信号，不构成投资建议，买卖需自行判断！")
    print()

    # 特别列出被最优策略命中的股票
    lpl = triggered_strategies.get("low_position_limitup", set())
    stl = triggered_strategies.get("stable_then_limitup", set())
    ec  = triggered_strategies.get("emotion_cycle", set())

    if lpl:
        print(f"\n[TOP] low_position_limitup（胜率65.2%，夏普3.31）信号股：")
        for code in lpl:
            si = sig_info.get(code, {})
            print(f"   {code} {str(si.get('name',''))[:8]}  收盘:{si.get('close',0):.2f}  "
                  f"涨幅:{si.get('pct_chg',0):.2f}%  换手:{si.get('turn',0):.1f}%")

    if stl:
        print(f"\n[好] stable_then_limitup（年化54.5%）信号股：")
        for code in stl:
            si = sig_info.get(code, {})
            print(f"   {code} {str(si.get('name',''))[:8]}  收盘:{si.get('close',0):.2f}  "
                  f"涨幅:{si.get('pct_chg',0):.2f}%")

    if ec:
        print(f"\n[热] emotion_cycle（本周胜率77.5%）信号股（Top 20）：")
        ec_list = list(ec)[:20]
        for code in ec_list:
            si = sig_info.get(code, {})
            print(f"   {code} {str(si.get('name',''))[:8]}  收盘:{si.get('close',0):.2f}  "
                  f"RSI6:{si.get('rsi6',0):.1f}  涨幅:{si.get('pct_chg',0):.2f}%")

    print()
    logger.info("预测完成！")


if __name__ == "__main__":
    main()
