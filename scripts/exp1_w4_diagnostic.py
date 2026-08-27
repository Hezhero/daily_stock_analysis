#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Exp1: W4 零胜率诊断 —— 数据问题 or 真信号失效?
A. aux 表数据新鲜度 (fc_pos_break/holder_conc_break 依赖 forecast/holdernumber 等表)
B. market_ok 状态 2026-07-01~08-14 (W4 OOS 有效交易日只有 1 天,先确认是否过滤全关)
C. 重建 W4 OOS 交易明细 (部署模型 8 策略, 输出 date/code/ret/market_ok)
用法: python scripts/exp1_w4_diagnostic.py
"""
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd
from dotenv import load_dotenv

load_dotenv(BASE_DIR / ".env", encoding="utf-8")

import backtest_5y_23strategies as bt
from backtest_5y_23strategies import (
    _get_pg_engine, _strategy_signal, _apply_cooldown,
    load_data, load_adj_factors_from_db, apply_forward_adjustment,
    load_signal_aux, compute_indicators, load_index_daily, compute_market_ok,
    load_fina_indicator, merge_fina_by_ann_date, compute_dynamic_exit_returns,
)

# W4 IS 选择结果 (来自 wfo_report_20260816.md §2 W4 表)
W4_SELECTED = [
    {"name": "stable_then_limit_up", "best_p": 10},
    {"name": "ma_golden_cross", "best_p": 10},
    {"name": "wave_theory", "best_p": 3},
    {"name": "volume_surge_std", "best_p": 10},
    {"name": "ensemble", "best_p": 3},
    {"name": "holder_conc_break", "best_p": 10},
    {"name": "n_pattern", "best_p": 10},
    {"name": "fc_pos_break", "best_p": 10},
]
W4_ENSEMBLE_WEIGHTS = {"ma_crossover": 0.340, "volume_surge_std": 0.349,
                       "multi_ma_resonance": 0.310}

OOS_START = "2026-07-01"
OOS_END = "2026-08-14"
DATA_START = "2025-06-01"  # 指标预热 ~260 交易日


def check_aux_freshness():
    print("=" * 70)
    print("A. aux 表数据新鲜度")
    print("=" * 70)
    tables = [
        "tushare_forecast", "tushare_stk_holdernumber", "tushare_top_inst",
        "tushare_pledge_stat", "tushare_cyq", "tushare_moneyflow",
        "tushare_stk_limit", "tushare_margin_detail", "tushare_fina_indicator",
        "tushare_daily_basic", "tushare_adj_factor",
    ]
    engine = _get_pg_engine()
    try:
        with engine.connect() as conn:
            for t in tables:
                try:
                    df = pd.read_sql(
                        f"SELECT count(*) AS n, min(trade_date) AS mn, "
                        f"max(trade_date) AS mx FROM {t}", conn)
                    r = df.iloc[0]
                    print(f"{t:32s} rows={r['n']:>10,}  min={r['mn']}  max={r['mx']}")
                except Exception as e:
                    print(f"{t:32s} ERROR: {e}")
    finally:
        engine.dispose()


def build_frame():
    df_market = load_data(DATA_START, OOS_END)
    df_factor = load_adj_factors_from_db(DATA_START, OOS_END)
    df_adjusted = apply_forward_adjustment(df_market, df_factor)
    df_adjusted = load_signal_aux(df_adjusted)
    df_all = compute_indicators(df_adjusted)
    try:
        df_index = load_index_daily(DATA_START, OOS_END)
        regime_df = compute_market_ok(df_index)
        df_all = df_all.merge(regime_df, on="date", how="left")
        df_all["market_ok"] = df_all["market_ok"].fillna(False).astype(bool)
    except Exception as e:
        print(f"[warn] regime 加载失败: {e}")
        df_all["market_ok"] = True
    try:
        df_fina = load_fina_indicator(DATA_START, OOS_END)
        if not df_fina.empty:
            df_all = merge_fina_by_ann_date(df_all, df_fina)
            df_all.rename(columns={"roe": "fin_roe",
                                   "grossprofit_margin": "fin_gross_margin",
                                   "or_yoy": "fin_or_yoy"}, inplace=True)
    except Exception as e:
        print(f"[warn] 财务加载失败: {e}")
    dyn_ret = compute_dynamic_exit_returns(df_all)
    df_all = pd.concat([df_all, dyn_ret], axis=1)
    return df_all


def check_market_ok(df_all):
    print("=" * 70)
    print("B. market_ok 状态 (2026-07-01 ~ 2026-08-14)")
    print("=" * 70)
    oos = df_all[df_all["date"] >= OOS_START]
    days = oos.groupby("date")["market_ok"].first()
    print(f"总交易日: {len(days)} | market_ok=True: {days.sum()} | "
          f"False: {(~days).sum()}")
    off_days = days[~days]
    if len(off_days):
        print(f"market_ok=False 的日期: {list(off_days.index.astype(str))}")
    on_days = days[days]
    if len(on_days):
        print(f"market_ok=True 的日期: {list(on_days.index.astype(str))}")


def reconstruct_w4_trades(df_all):
    print("=" * 70)
    print("C. W4 OOS 交易重建 (2026-07-01 ~ 2026-08-14)")
    print("=" * 70)
    bt._ENSEMBLE_WEIGHTS.clear()
    bt._ENSEMBLE_WEIGHTS.update(W4_ENSEMBLE_WEIGHTS)

    oos = df_all[(df_all["date"] >= OOS_START) & (df_all["date"] <= OOS_END)]
    all_trades = []
    for s in W4_SELECTED:
        name = s["name"]
        best_p = s["best_p"]
        sig = _strategy_signal(oos, name)
        raw_n = int(sig.sum())
        kept = _apply_cooldown(oos, sig)
        kept_n = int(kept.sum())
        col = f"dyn_ret_{best_p}d"
        if col in oos.columns:
            sub = oos[kept][["date", "code", "market_ok", col]].dropna()
            trades_n = len(sub)
            if trades_n:
                sub["strategy"] = name
                sub["best_p"] = best_p
                sub["ret"] = sub[col].values - bt.TRADING_COST_PCT / 100.0
                all_trades.append(sub)
            win = (sub[col] > 0).sum() if trades_n else 0
            exp = sub[col].mean() - bt.TRADING_COST_PCT / 100.0 if trades_n else 0
            print(f"{name:24s} raw_sig={raw_n:4d}  kept={kept_n:4d}  "
                  f"trades={trades_n:3d}  win={win:3d}  exp={exp:+.3f}%  "
                  f"mkt_ok_days={sub['market_ok'].sum() if trades_n else 0}")
        else:
            print(f"{name:24s} raw_sig={raw_n:4d}  kept={kept_n:4d}  "
                  f"col={col} 缺失")
    if all_trades:
        trades = pd.concat(all_trades, ignore_index=True)
        print("-" * 70)
        print(f"合计交易: {len(trades)} 笔 | 唯一交易日: "
              f"{trades['date'].nunique()} | 唯一股票: {trades['code'].nunique()}")
        print(f"市场状态: market_ok=True 的交易 {trades['market_ok'].sum()} 笔 / "
              f"False {len(trades) - trades['market_ok'].sum()} 笔")
        cols = ["date", "code", "strategy", "best_p", "market_ok", "ret"]
        print(trades[cols].sort_values(["date", "code"]).to_string(index=False))
        dup = trades.groupby(["date", "code"]).size().reset_index(name="n_strategies")
        print("-" * 70)
        print("同股票多策略命中分布:")
        print(dup["n_strategies"].value_counts().sort_index().to_string())
    else:
        print("OOS 区间无任何交易")


def main():
    check_aux_freshness()
    print()
    df_all = build_frame()
    print()
    check_market_ok(df_all)
    print()
    reconstruct_w4_trades(df_all)


if __name__ == "__main__":
    main()