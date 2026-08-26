#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WFO 滚动向前回测（4Y 训练 / 1Y 验证 / 6M 步长）

设计文档: docs/wfo-backtest-design.md（已审阅定稿）
复用管线: backtest_5y_23strategies.py（数据/指标/信号/动态退出/绩效全部原样复用）

三种模式:
  python scripts/backtest_wfo.py --mode research   # 全量 WFO:窗口网格 + IS/OOS + 聚合 + 部署工件 + 报告
  python scripts/backtest_wfo.py --mode deploy     # 仅重建部署工件（--force-retrain 强制）
  python scripts/backtest_wfo.py --mode daily      # 每日选股（读部署工件 + 最新数据）

核心机制:
  - 固化逻辑: 策略阈值/成本/冷却期/持有期集合/选择超参数全部冻结为全局常量,窗口间不重调参
  - 滚动变量: 每窗口只在 IS 上训练 3 项 —— 组合权重 / 最优持有期 best_p / 入选策略集
  - 激活映射: 交易日 d ∈ [val_start_k, val_start_{k+1}) 由 Wk 的 IS 选择驱动,
    聚合 OOS 按天去重（评估口径 = 生产口径）
  - best_p 选期口径: expectation（已确认,见设计文档 §14）
"""

import argparse
import gc
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import backtest_5y_23strategies as bt
from backtest_5y_23strategies import (
    STRATEGIES,
    ENSEMBLE_COMPONENTS,
    TRADING_COST_PCT,
    _apply_cooldown,
    _backtest_single,
    _compute_ensemble_weights,
    _get_pg_engine,
    _strategy_signal,
    apply_forward_adjustment,
    calc_metrics,
    compute_benchmark_metrics,
    compute_dynamic_exit_returns,
    compute_indicators,
    compute_market_ok,
    get_next_day_recommendations,
    load_adj_factors_from_db,
    load_data,
    load_fina_indicator,
    load_index_daily,
    load_signal_aux,
    merge_fina_by_ann_date,
    resolve_parallel_config,
    logger,
)

# ─── WFO 配置（固化超参数,见设计文档 §5） ───────────────────────────────────────
WFO_TRAIN_YEARS = 4              # 训练窗口长度（年）
WFO_VAL_YEARS = 1                # 验证窗口长度（年）
WFO_STEP_MONTHS = 6              # 滚动步长（月）
WFO_ANCHOR_START = "2021-01-01"  # 数据锚定起点（与现有 5Y 回测一致）
WFO_TOP_K = 8                    # 每窗口入选策略数
WFO_MIN_IS_TRADES = 50           # IS 入选最低交易笔数
WFO_BURN_IN_TRADING_DAYS = 120   # train 切片前 N 个交易日的信号剔除（warmup）
WFO_MIN_OOS_TRADING_DAYS = 60    # OOS 有效交易日门槛（低于则不参与聚合）
WFO_IS_METRIC = "expectation"    # 选期/排序口径
WFO_DAILY_LOOKBACK_DAYS = 400    # 每日选股加载最近日历天数（含指标预热）
WFO_MODEL_PATH = BASE_DIR / "result" / "wfo_model_latest.json"
WFO_REPORT_DIR = BASE_DIR / "result"

# ─── 日志（独立文件,与 backtest_5y 日志并存） ───────────────────────────────────
_LOG_DIR = Path(os.environ.get("LOG_DIR") or "logs")
if not _LOG_DIR.is_absolute():
    _LOG_DIR = BASE_DIR / _LOG_DIR
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_WFO_LOG_FILE = _LOG_DIR / f"backtest_wfo_{datetime.now():%Y%m%d}.log"
_wfo_handler = RotatingFileHandler(
    _WFO_LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
_wfo_handler.setLevel(logging.INFO)
_wfo_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
))
logger.addHandler(_wfo_handler)


# ═══════════════════════════════════════════════════════════════════════════════
# 窗口网格
# ═══════════════════════════════════════════════════════════════════════════════

def _snap_trading_day(engine, date_str: str, direction: str = "next") -> str:
    """将日期吸附到最近交易日（direction: next=向后取最近交易日, prev=向前取最近交易日）。

    查询 tushare_trade_cal（复用 is_trading_day 的查询方式）；
    查询失败或未找到时原样返回输入日期。
    """
    op = ">=" if direction == "next" else "<="
    order = "ASC" if direction == "next" else "DESC"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                f"SELECT cal_date FROM tushare_trade_cal WHERE exchange='SSE' AND is_open=1 "
                f"AND cal_date {op} %s ORDER BY cal_date {order} LIMIT 1",
                conn, params=(date_str,),
            )
        if df.empty:
            logger.warning(f"未找到交易日: {date_str}，原样使用")
            return date_str
        return str(df.iloc[0]["cal_date"])
    except Exception as e:
        logger.warning(f"交易日吸附查询失败: {e}，原样使用 {date_str}")
        return date_str


def build_window_grid(data_end: str) -> List[Dict]:
    """构造 WFO 窗口网格。

    Wk: train = [T0 + k*6M, T0 + k*6M + 4Y)  IS
        val   = [T0 + k*6M + 4Y, T0 + k*6M + 5Y)  OOS
    T0 = WFO_ANCHOR_START；边界吸附到实际交易日；
    窗口生成条件：train 起点 + 4Y <= 数据最新交易日（train 完整才生成）。
    返回窗口 dict 列表（含吸附后的 train_start/train_end/val_start/val_end）。
    """
    t0 = pd.Timestamp(WFO_ANCHOR_START)
    data_end_ts = pd.Timestamp(data_end)
    engine = _get_pg_engine()
    windows = []
    try:
        k = 0
        while True:
            train_start = t0 + pd.DateOffset(months=WFO_STEP_MONTHS * k)
            train_end = train_start + pd.DateOffset(years=WFO_TRAIN_YEARS) - pd.Timedelta(days=1)
            val_start = train_end + pd.Timedelta(days=1)
            val_end = val_start + pd.DateOffset(years=WFO_VAL_YEARS) - pd.Timedelta(days=1)
            if train_end > data_end_ts:
                break
            windows.append({
                "k": k + 1,
                "train_start": _snap_trading_day(engine, train_start.strftime("%Y-%m-%d"), "next"),
                "train_end": _snap_trading_day(engine, train_end.strftime("%Y-%m-%d"), "prev"),
                "val_start": _snap_trading_day(engine, val_start.strftime("%Y-%m-%d"), "next"),
                "val_end": _snap_trading_day(engine, val_end.strftime("%Y-%m-%d"), "prev"),
            })
            k += 1
    finally:
        engine.dispose()
    logger.info(f"WFO 窗口网格: {len(windows)} 个窗口（数据终点 {data_end}）")
    return windows


# ═══════════════════════════════════════════════════════════════════════════════
# 全量帧构建（一次加载、多次切片）
# ═══════════════════════════════════════════════════════════════════════════════

def build_full_frame(start: str, end: str) -> pd.DataFrame:
    """一次加载、多次切片：数据 -> 复权 -> aux -> 指标 -> regime -> 财务 as-of -> 动态退出。

    与 backtest_5y_23strategies.main() 管线完全同码（全部因果,无前视）；
    返回全量 df_all,各窗口按日期切片复用,不重算指标。
    """
    df_market = load_data(start, end)
    df_factor = load_adj_factors_from_db(start, end)
    df_adjusted = apply_forward_adjustment(df_market, df_factor)
    del df_market, df_factor
    gc.collect()

    df_adjusted = load_signal_aux(df_adjusted)
    df_all = compute_indicators(df_adjusted)
    del df_adjusted
    gc.collect()

    try:
        df_index = load_index_daily(start, end)
        regime_df = compute_market_ok(df_index)
        df_all = df_all.merge(regime_df, on="date", how="left")
        df_all["market_ok"] = df_all["market_ok"].fillna(False).astype(bool)
        del df_index, regime_df
        gc.collect()
    except MemoryError:
        logger.error("市场环境数据加载失败: 内存不足(MemoryError), 中止运行以免静默降级污染结果")
        raise
    except Exception as e:
        logger.warning(f"市场环境数据加载失败，跳过 regime 过滤: {e}")
        df_all["market_ok"] = True

    try:
        df_fina = load_fina_indicator(start, end)
        if not df_fina.empty:
            df_all = merge_fina_by_ann_date(df_all, df_fina)
            df_all.rename(columns={"roe": "fin_roe", "grossprofit_margin": "fin_gross_margin",
                                   "or_yoy": "fin_or_yoy"}, inplace=True)
            logger.info(f"财务指标字段: fin_roe 覆盖率 {df_all['fin_roe'].notna().mean()*100:.1f}%")
        del df_fina
        gc.collect()
    except MemoryError:
        logger.error("财务数据加载失败: 内存不足(MemoryError), 中止运行以免静默降级污染结果")
        raise
    except Exception as e:
        logger.warning(f"财务数据加载失败，跳过财务过滤: {e}")

    dyn_ret = compute_dynamic_exit_returns(df_all)
    df_all = pd.concat([df_all, dyn_ret], axis=1)
    del dyn_ret
    gc.collect()
    return df_all


# ═══════════════════════════════════════════════════════════════════════════════
# IS 训练（每窗口,只读 IS）
# ═══════════════════════════════════════════════════════════════════════════════

def run_is_selection(df_all: pd.DataFrame, window: Dict) -> Dict:
    """在窗口 train 切片上执行 IS 训练,返回选择结果。

    流程（全部只读 IS）:
      1. 剔除 train 切片前 BURN_IN_TRADING_DAYS 个交易日的信号（warmup）;
      2. 对全部策略跑 _backtest_single(select_period_by=expectation);
      3. 组件胜率归一化 -> 组合权重（in-place 更新模块级 _ENSEMBLE_WEIGHTS）;
      4. 过滤 total_trades >= WFO_MIN_IS_TRADES,按 expectation 降序取 Top-K;
      5. 为入选策略记录 best_p（IS expectation 最大持有期）。
    返回 selection dict（含 is_metrics / selected / ensemble_weights / burn_in）。
    """
    train_start = pd.Timestamp(window["train_start"])
    train_end = pd.Timestamp(window["train_end"])
    df_is = df_all[(df_all["date"] >= train_start) & (df_all["date"] <= train_end)].copy()

    is_dates = sorted(df_is["date"].unique())
    if len(is_dates) > WFO_BURN_IN_TRADING_DAYS:
        burn_cutoff = is_dates[WFO_BURN_IN_TRADING_DAYS - 1]
        df_is = df_is[df_is["date"] >= burn_cutoff].copy()
        burn_info = {"burned_days": WFO_BURN_IN_TRADING_DAYS,
                     "effective_start": str(burn_cutoff.date())}
    else:
        burn_info = {"burned_days": 0,
                     "effective_start": str(is_dates[0].date()) if is_dates else None}

    # 组件策略先串行（需先确定组合权重,供 sig_ensemble 使用）
    component_results = {}
    for name in ENSEMBLE_COMPONENTS:
        r = _backtest_single(name, df_is, select_period_by=WFO_IS_METRIC)
        component_results[name] = r
    weights = _compute_ensemble_weights(component_results)
    # 模块级全局原地更新（sig_ensemble 运行时读取）
    bt._ENSEMBLE_WEIGHTS.clear()
    bt._ENSEMBLE_WEIGHTS.update(weights)

    # 其余策略（含 ensemble）并行回测,窗口内只读已冻结的权重,无竞态
    is_metrics = dict(component_results)
    remaining = [n for n in STRATEGIES if n not in ENSEMBLE_COMPONENTS]
    enable_parallel, max_workers = resolve_parallel_config()
    n_workers = min(max_workers, len(remaining))

    def _run_one(name):
        return name, _backtest_single(name, df_is, select_period_by=WFO_IS_METRIC)

    if enable_parallel and n_workers > 1 and len(remaining) > 1:
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            for name, r in executor.map(_run_one, remaining):
                is_metrics[name] = r
    else:
        for name in remaining:
            is_metrics[name] = _run_one(name)[1]

    # 过滤 + 排序 + Top-K
    candidates = [r for r in is_metrics.values()
                  if "error" not in r and r.get("total_trades", 0) >= WFO_MIN_IS_TRADES]
    candidates.sort(key=lambda x: x.get(WFO_IS_METRIC, -999), reverse=True)
    selected = candidates[:WFO_TOP_K]

    selected_info = []
    for r in selected:
        selected_info.append({
            "name": r["strategy"],
            "best_period": r.get("best_period"),
            "is_win_rate": r.get("win_rate", 0),
            "is_expectation": r.get(WFO_IS_METRIC, 0),
            "is_total_trades": r.get("total_trades", 0),
        })

    logger.info(f"窗口 W{window['k']} IS 训练完成: 入选 {len(selected_info)} 个策略, "
                f"burn-in {burn_info['burned_days']} 交易日")
    return {
        "window": window,
        "burn_in": burn_info,
        "is_metrics": is_metrics,
        "selected": selected_info,
        "ensemble_weights": weights,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# OOS 评估（每窗口,冻结选择）
# ═══════════════════════════════════════════════════════════════════════════════

def _find_exit_day_offset(close: np.ndarray, high: np.ndarray, low: np.ndarray,
                          atr: np.ndarray, entry_idx: int, best_p: int) -> int:
    """按 compute_dynamic_exit_returns 同码逻辑求实际出场日偏移（1..best_p）。

    入场日 entry_idx 收盘买入,持有期内逐日检查:
      - ATR 止损:某日 low <= close[entry] - 2.5*atr[entry]
      - 移动止盈:某日 close <= 持有期最高价*0.95
      - 时间止损:持有 best_p 日仍未触发
    返回出场日偏移 k（入场日 + k 个交易日）,与 dyn_ret_{best_p}d 的退出时点一致。
    出场参数 (2.5, 0.95) 与生产函数 compute_dynamic_exit_returns 保持一致
    （WFO 实验 Exp3 回写,见 result/wfo_experiments_20260820.md）。
    """
    stop = close[entry_idx] - 2.5 * atr[entry_idx]
    if not np.isfinite(stop):
        return best_p
    w = min(best_p, len(close) - 1 - entry_idx)
    if w < 1:
        return best_p
    win_low = low[entry_idx + 1: entry_idx + 1 + w]
    win_high = high[entry_idx + 1: entry_idx + 1 + w]
    win_close = close[entry_idx + 1: entry_idx + 1 + w]
    peaks = np.maximum.accumulate(win_high)
    atr_hit = win_low <= stop
    trail_hit = win_close <= peaks * 0.95
    hit = atr_hit | trail_hit
    if hit.any():
        return int(np.argmax(hit)) + 1
    return best_p


def _build_price_paths(df: pd.DataFrame) -> Dict[str, Dict]:
    """按 code 预计算价格路径查找表（组合曲线重建用）。

    返回 {code: {"dates": ndarray, "close": ndarray, "high": ndarray,
                 "low": ndarray, "atr": ndarray}},按日期升序。
    """
    paths = {}
    cols = ["date", "close", "high", "low", "atr20"]
    for code, g in df[["code"] + cols].groupby("code", sort=False):
        g = g.sort_values("date")
        paths[code] = {
            "dates": g["date"].to_numpy(dtype="datetime64[ns]"),
            "close": g["close"].to_numpy(dtype=np.float64),
            "high": g["high"].to_numpy(dtype=np.float64),
            "low": g["low"].to_numpy(dtype=np.float64),
            "atr": g["atr20"].to_numpy(dtype=np.float64),
        }
    return paths


def _trade_returns(df: pd.DataFrame, price_paths: Dict[str, Dict],
                   name: str, best_p: int) -> pd.DataFrame:
    """按冻结 best_p 提取单策略的交易级收益（信号 + 冷却期 + 动态退出 - 成本）。

    与 _backtest_single 同码,但只取冻结持有期列,用于 OOS 聚合按天去重;
    额外输出 exit_date（实际出场日,组合曲线重建用）。
    """
    sig = _strategy_signal(df, name)
    signals = df[_apply_cooldown(df, sig)]
    col = f"dyn_ret_{best_p}d" if f"dyn_ret_{best_p}d" in signals.columns else f"ret_{best_p}d"
    if col not in signals.columns:
        return pd.DataFrame(columns=["date", "code", "strategy", "ret", "exit_date"])
    sub = signals[["date", "code", col]].dropna().copy()
    if sub.empty:
        return pd.DataFrame(columns=["date", "code", "strategy", "ret", "exit_date"])
    sub["strategy"] = name
    sub["ret"] = sub[col].values - TRADING_COST_PCT / 100.0

    exit_dates = []
    for _, row in sub.iterrows():
        path = price_paths.get(row["code"])
        if path is None:
            exit_dates.append(pd.NaT)
            continue
        dates = path["dates"]
        entry_ts = np.datetime64(row["date"])
        idx = int(np.searchsorted(dates, entry_ts))
        if idx >= len(dates) or dates[idx] != entry_ts:
            exit_dates.append(pd.NaT)
            continue
        k = _find_exit_day_offset(path["close"], path["high"], path["low"],
                                  path["atr"], idx, best_p)
        exit_idx = min(idx + k, len(dates) - 1)
        exit_dates.append(pd.Timestamp(dates[exit_idx]))
    sub["exit_date"] = exit_dates
    return sub[["date", "code", "strategy", "ret", "exit_date"]]


def compute_portfolio_curve(trades: pd.DataFrame,
                            price_paths: Dict[str, Dict]) -> Dict:
    """按天持仓市值重建真实组合曲线（每笔交易等权 1 单位资金）。

    入场日收盘买入（市值 1.0）,持有期逐日按收盘价 mark-to-market,
    出场日按实际退出价结算（ret 已含动态退出与成本）;
    组合日收益 = Σ(当日持仓市值变动) / Σ(前日持仓市值),无持仓日为 0（现金）。
    返回: total_return / annualized_return / max_drawdown / sharpe_ratio /
    n_days / n_trades / daily_returns。
    """
    if trades.empty or "exit_date" not in trades.columns:
        return {}
    t = trades.dropna(subset=["exit_date"]).copy()
    if t.empty:
        return {}

    all_dates = set()
    for code in t["code"].unique():
        path = price_paths.get(code)
        if path is not None:
            all_dates.update(pd.DatetimeIndex(path["dates"]))
    if not all_dates:
        return {}
    calendar = np.array(sorted(all_dates), dtype="datetime64[ns]")
    cal_idx = {d: i for i, d in enumerate(calendar)}
    n = len(calendar)

    entry_idx = t["date"].map(lambda d: cal_idx.get(np.datetime64(d), -1)).to_numpy()
    exit_idx = t["exit_date"].map(lambda d: cal_idx.get(np.datetime64(d), -1)).to_numpy()
    valid = (entry_idx >= 0) & (exit_idx > entry_idx)
    if not valid.any():
        return {}
    t = t[valid].reset_index(drop=True)
    entry_idx = entry_idx[valid]
    exit_idx = exit_idx[valid]
    rets = t["ret"].to_numpy()

    prev_sum = np.zeros(n)
    gain_sum = np.zeros(n)
    for i in range(len(t)):
        path = price_paths.get(t.iloc[i]["code"])
        if path is None:
            continue
        dates = path["dates"]
        close = path["close"]
        e = int(entry_idx[i])
        x = int(exit_idx[i])
        e_pos = int(np.searchsorted(dates, calendar[e]))
        if e_pos >= len(dates):
            continue
        entry_close = close[e_pos]
        if not np.isfinite(entry_close) or entry_close <= 0:
            continue
        prev_val = 1.0
        for j in range(e + 1, x + 1):
            if j == x:
                cur_val = 1.0 + rets[i]
            else:
                d_pos = int(np.searchsorted(dates, calendar[j]))
                if d_pos < len(dates) and np.isfinite(close[d_pos]) and close[d_pos] > 0:
                    cur_val = close[d_pos] / entry_close
                else:
                    cur_val = prev_val
            prev_sum[j] += prev_val
            gain_sum[j] += cur_val - prev_val
            prev_val = cur_val

    daily_ret = np.zeros(n)
    for j in range(1, n):
        if prev_sum[j] > 1e-12:
            daily_ret[j] = gain_sum[j] / prev_sum[j]

    equity = np.cumprod(1.0 + daily_ret)
    total_return = float(equity[-1] - 1.0)
    n_days = int(np.count_nonzero(prev_sum > 1e-12))
    annualized = (1.0 + total_return) ** (252.0 / n_days) - 1.0 if n_days > 0 else 0.0

    peak = float(equity[0])
    mdd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak
            if dd > mdd:
                mdd = dd

    active = daily_ret[prev_sum > 1e-12]
    sharpe = 0.0
    if len(active) > 1 and np.std(active) > 1e-10:
        sharpe = float(np.mean(active) / np.std(active) * np.sqrt(252.0))

    return {
        "total_return": total_return * 100.0,
        "annualized_return": annualized * 100.0,
        "max_drawdown": mdd * 100.0,
        "sharpe_ratio": sharpe,
        "n_days": n_days,
        "n_trades": int(len(t)),
        "daily_returns": daily_ret,
    }


def run_oos_eval(df_all: pd.DataFrame, window: Dict, selection: Dict,
                 price_paths: Dict[str, Dict]) -> Dict:
    """在窗口 val 切片上用冻结选择评估 OOS,返回逐策略指标 + 交易级收益表。

    逐窗口 OOS 区间互相重叠（步长 6M < 验证 1Y）,仅作诊断；
    聚合口径由 aggregate_oos 按激活映射按天去重。
    """
    val_start = pd.Timestamp(window["val_start"])
    val_end = pd.Timestamp(window["val_end"])
    df_oos = df_all[(df_all["date"] >= val_start) & (df_all["date"] <= val_end)].copy()

    oos_metrics = {}
    trades_list = []
    for s in selection["selected"]:
        name = s["name"]
        best_p = s.get("best_period")
        if best_p is None:
            continue
        trades = _trade_returns(df_oos, price_paths, name, best_p)
        if trades.empty:
            oos_metrics[name] = {"total_trades": 0, "win_rate": 0, "expectation": 0,
                                 "annualized_return": 0, "max_drawdown": 0, "sharpe_ratio": 0}
            continue
        m = calc_metrics(trades["ret"].values, avg_holding=best_p)
        m["total_trades"] = int(len(trades))
        m["strategy"] = name
        oos_metrics[name] = m
        trades_list.append(trades)

    trades_df = pd.concat(trades_list, ignore_index=True) if trades_list else pd.DataFrame(
        columns=["date", "code", "strategy", "ret"])

    # 逐窗口 OOS 聚合（诊断用,重叠区间）
    window_agg = {}
    if not trades_df.empty:
        window_agg = calc_metrics(trades_df["ret"].values, avg_holding=None)
        window_agg["total_trades"] = int(len(trades_df))

    return {"window": window, "oos_metrics": oos_metrics,
            "trades": trades_df, "window_agg": window_agg}


# ═══════════════════════════════════════════════════════════════════════════════
# 聚合 OOS（激活映射,按天去重）
# ═══════════════════════════════════════════════════════════════════════════════

def aggregate_oos(windows: List[Dict], oos_results: List[Dict], data_end: str,
                  price_paths: Dict[str, Dict]) -> Dict:
    """按每日激活映射聚合 OOS（生产一致口径,按天去重）。

    交易日 d ∈ [val_start_k, val_start_{k+1}) 由 Wk 的 IS 选择驱动;
    每个窗口取其激活段 [val_start_k, min(val_start_{k+1}, data_end)) 内的交易;
    OOS 有效交易日 < WFO_MIN_OOS_TRADING_DAYS 的窗口只用于产出部署模型,不参与聚合。
    交易级指标（胜率/期望/盈亏比）沿用 calc_metrics;
    回撤/年化/夏普改用真实组合曲线（compute_portfolio_curve,按天持仓市值重建）。
    """
    data_end_ts = pd.Timestamp(data_end)
    combined = []
    used_windows = []
    for i, (w, oos) in enumerate(zip(windows, oos_results)):
        seg_end = windows[i + 1]["val_start"] if i + 1 < len(windows) else data_end
        seg_start = w["val_start"]
        trades = oos["trades"]
        if trades.empty:
            continue
        seg = trades[(trades["date"] >= pd.Timestamp(seg_start)) &
                     (trades["date"] < pd.Timestamp(seg_end))]
        n_days = seg["date"].nunique()
        if n_days < WFO_MIN_OOS_TRADING_DAYS:
            logger.info(f"窗口 W{w['k']} OOS 有效交易日 {n_days} < "
                        f"{WFO_MIN_OOS_TRADING_DAYS},不参与聚合")
            continue
        combined.append(seg)
        used_windows.append(w["k"])

    if not combined:
        logger.warning("无满足门槛的 OOS 交易,聚合结果为空")
        return {"metrics": {}, "portfolio": {}, "used_windows": [], "total_trades": 0}

    all_trades = pd.concat(combined, ignore_index=True)
    # 激活映射已按天去重,此处校验兜底
    dup_days = all_trades.duplicated(subset=["date", "code", "strategy"]).sum()
    if dup_days:
        logger.warning(f"聚合 OOS 发现 {dup_days} 条重复交易（激活段应不相交,请检查窗口网格）")
    metrics = calc_metrics(all_trades["ret"].values, avg_holding=None)
    metrics["total_trades"] = int(len(all_trades))

    portfolio = compute_portfolio_curve(all_trades, price_paths)
    if portfolio:
        metrics["annualized_return"] = portfolio["annualized_return"]
        metrics["max_drawdown"] = portfolio["max_drawdown"]
        metrics["sharpe_ratio"] = portfolio["sharpe_ratio"]

    logger.info(f"聚合 OOS: {len(all_trades)} 笔交易 / {all_trades['date'].nunique()} 个交易日, "
                f"参与窗口 W{used_windows} | 组合曲线回撤 {portfolio.get('max_drawdown', 0):.1f}%")
    return {
        "metrics": metrics,
        "portfolio": portfolio,
        "used_windows": used_windows,
        "total_trades": int(len(all_trades)),
        "n_days": all_trades["date"].nunique(),
        "period_start": str(all_trades["date"].min().date()),
        "period_end": str(all_trades["date"].max().date()),
    }


def _spearman_rank_corr(is_vals: List[float], oos_vals: List[float]) -> Optional[float]:
    """IS 与 OOS 期望的 Spearman 秩相关系数;样本不足或常数序列返回 None。"""
    if len(is_vals) < 3 or len(is_vals) != len(oos_vals):
        return None
    try:
        corr = pd.Series(is_vals).corr(pd.Series(oos_vals), method="spearman")
    except Exception:
        return None
    return round(float(corr), 3) if pd.notna(corr) else None


# ═══════════════════════════════════════════════════════════════════════════════
# 部署工件
# ═══════════════════════════════════════════════════════════════════════════════

def save_deployed_model(selection: Dict) -> Path:
    """将最新完整 train 窗口的选择结果写入部署工件。"""
    model = {
        "trained_at": datetime.now().strftime("%Y-%m-%d"),
        "window": {
            "train_start": selection["window"]["train_start"],
            "train_end": selection["window"]["train_end"],
        },
        "strategies": [
            {"name": s["name"], "best_period": int(s["best_period"]) if s["best_period"] is not None else None,
             "is_win_rate": float(s["is_win_rate"]), "is_expectation": float(s["is_expectation"]),
             "is_total_trades": int(s["is_total_trades"])}
            for s in selection["selected"]
        ],
        "ensemble_weights": {k: float(v) for k, v in selection["ensemble_weights"].items()},
    }
    WFO_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(WFO_MODEL_PATH, "w", encoding="utf-8") as f:
        json.dump(model, f, ensure_ascii=False, indent=2)
    logger.info(f"部署工件已写入: {WFO_MODEL_PATH}")
    return WFO_MODEL_PATH


def load_deployed_model() -> Optional[Dict]:
    """读取部署工件;缺失或损坏返回 None。"""
    if not WFO_MODEL_PATH.exists():
        return None
    try:
        with open(WFO_MODEL_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"部署工件读取失败: {e}")
        return None


def _should_retrain(model: Optional[Dict], windows: List[Dict], force: bool) -> bool:
    """判断是否需要重建部署工件。

    规则: --force-retrain 强制;工件缺失兜底重建;
    否则仅当存在比工件训练窗口更新的完整 train 窗口（日历驱动）时重建。
    """
    if force:
        return True
    if model is None:
        return True
    trained_end = model.get("window", {}).get("train_end")
    if not trained_end:
        return True
    latest = windows[-1]
    return latest["train_end"] > trained_end


# ═══════════════════════════════════════════════════════════════════════════════
# 每日选股（生产模式）
# ═══════════════════════════════════════════════════════════════════════════════

def run_daily_select(top_n: int = 10) -> List[Dict]:
    """每日选股:读部署工件 + 加载最近 ~1 年数据跑信号 + IS 胜率打分。

    对入选策略逐日跑信号（复用 _strategy_signal）,冷却期照常;
    打分与 get_next_day_recommendations 同款:total_score = Σ 命中策略的 IS 胜率。
    """
    model = load_deployed_model()
    if model is None:
        raise RuntimeError("部署工件缺失,请先运行 --mode research 或 --mode deploy")

    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - pd.DateOffset(days=WFO_DAILY_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    df_all = build_full_frame(start, end)

    # 设置组合权重（部署工件中的 IS 权重）
    bt._ENSEMBLE_WEIGHTS.clear()
    bt._ENSEMBLE_WEIGHTS.update(model.get("ensemble_weights", {}))

    # 构造与 get_next_day_recommendations 兼容的 results 结构（按 IS 胜率）
    results = [
        {"strategy": s["name"], "win_rate": s["is_win_rate"],
         "total_trades": s.get("is_total_trades", 1), "expectation": s["is_expectation"]}
        for s in model["strategies"]
    ]

    recs, _ = get_next_day_recommendations(df_all, results, results=results, top_n=top_n)
    return recs


def _print_daily(recs: List[Dict]) -> None:
    """打印每日选股结果。"""
    if not recs:
        logger.warning("今日无命中策略的推荐股票")
        return
    logger.info(f"今日推荐 {len(recs)} 只（按命中策略数与平均胜率降序）:")
    for i, r in enumerate(recs, 1):
        logger.info(f"  [{i}] {r['code']} {r['name']} - 策略数{r['strategy_count']} "
                    f"- 平均胜率{r['avg_win_rate']:.1f}% - 命中{r['matched_strategies']}")


# ═══════════════════════════════════════════════════════════════════════════════
# 全量 WFO 研究
# ═══════════════════════════════════════════════════════════════════════════════

def run_research(data_end: str) -> Dict:
    """全量 WFO:窗口网格 -> 逐窗口 IS 训练 -> OOS 评估 -> 聚合 -> 基准对比。"""
    t0 = time.time()
    windows = build_window_grid(data_end)
    if not windows:
        raise RuntimeError("无完整 train 窗口可生成（数据不足）")

    df_all = build_full_frame(WFO_ANCHOR_START, data_end)
    price_paths = _build_price_paths(df_all)

    selections = []
    oos_results = []
    for w in windows:
        logger.info(f"窗口 W{w['k']}: IS {w['train_start']}~{w['train_end']} | "
                    f"OOS {w['val_start']}~{w['val_end']}")
        selection = run_is_selection(df_all, w)
        selections.append(selection)
        oos = run_oos_eval(df_all, w, selection, price_paths)
        oos_results.append(oos)
        for s in selection["selected"]:
            name = s["name"]
            oos_m = oos["oos_metrics"].get(name, {})
            logger.info(f"  {name}: IS期望{s['is_expectation']:.3f} -> "
                        f"OOS期望{oos_m.get('expectation', 0):.3f} | "
                        f"IS胜率{s['is_win_rate']:.1f}% -> OOS胜率{oos_m.get('win_rate', 0):.1f}%")

    agg = aggregate_oos(windows, oos_results, data_end, price_paths)

    # 基准对比（沪深300,聚合 OOS 期间）
    benchmark = {}
    if agg.get("total_trades", 0) > 0:
        try:
            idx = load_index_daily(agg["period_start"], agg["period_end"], ts_code="000300.SH")
            benchmark = compute_benchmark_metrics(None, index_df=idx) if not idx.empty else {}
        except Exception as e:
            logger.warning(f"基准指数加载失败: {e}")

    # IS/OOS 排序相关性（Spearman,逐窗口）
    spearman = []
    for w, selection, oos in zip(windows, selections, oos_results):
        is_vals = [s["is_expectation"] for s in selection["selected"]]
        oos_vals = [oos["oos_metrics"].get(s["name"], {}).get("expectation", 0)
                    for s in selection["selected"]]
        spearman.append(_spearman_rank_corr(is_vals, oos_vals))

    # 策略集 churn（相邻窗口 Jaccard 相似度）
    churn = []
    for i in range(1, len(selections)):
        prev_set = {s["name"] for s in selections[i - 1]["selected"]}
        curr_set = {s["name"] for s in selections[i]["selected"]}
        union = prev_set | curr_set
        jac = len(prev_set & curr_set) / len(union) if union else 0
        churn.append({"from": f"W{windows[i-1]['k']}", "to": f"W{windows[i]['k']}",
                      "jaccard": round(jac, 3)})

    logger.info(f"WFO 全流程完成,耗时 {time.time()-t0:.1f}s")
    return {
        "data_end": data_end,
        "windows": windows,
        "selections": selections,
        "oos_results": oos_results,
        "aggregate": agg,
        "benchmark": benchmark,
        "spearman": spearman,
        "churn": churn,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 报告渲染
# ═══════════════════════════════════════════════════════════════════════════════

def render_report(research: Dict, path: Path) -> None:
    """生成 WFO 研究报告 markdown。"""
    lines = []
    lines.append("# WFO 滚动向前回测报告（4Y 训练 / 1Y 验证 / 6M 步长）")
    lines.append("")
    lines.append(f"> 数据终点: {research['data_end']} | 生成时间: {datetime.now():%Y-%m-%d %H:%M:%S}")
    lines.append("")

    # 1. 窗口网格
    lines.append("## 1. 窗口网格")
    lines.append("")
    lines.append("| 窗口 | 训练区间（IS） | 验证区间（OOS） | 状态 |")
    lines.append("|---|---|---|---|")
    data_end_ts = pd.Timestamp(research["data_end"])
    for w in research["windows"]:
        status = "完整" if pd.Timestamp(w["val_end"]) <= data_end_ts else "部分"
        lines.append(f"| W{w['k']} | {w['train_start']} ~ {w['train_end']} | "
                     f"{w['val_start']} ~ {w['val_end']} | {status} |")
    lines.append("")

    # 2. 逐窗口 IS 选择
    lines.append("## 2. 逐窗口 IS 选择（best_p 口径: expectation）")
    lines.append("")
    for w, selection in zip(research["windows"], research["selections"]):
        lines.append(f"### W{w['k']} IS {w['train_start']} ~ {w['train_end']}")
        lines.append("")
        lines.append("| 策略 | best_p | IS胜率% | IS期望 | IS交易数 |")
        lines.append("|---|---|---|---|---|")
        for s in selection["selected"]:
            lines.append(f"| {s['name']} | {s['best_period']} | {s['is_win_rate']:.1f} | "
                         f"{s['is_expectation']:.3f} | {s['is_total_trades']} |")
        ew = selection["ensemble_weights"]
        ew_str = ", ".join(f"{k}={v:.3f}" for k, v in ew.items()) if ew else "等权"
        lines.append("")
        lines.append(f"组合权重: {ew_str}")
        lines.append("")

    # 3. 逐窗口 OOS 与 IS→OOS 衰减
    lines.append("## 3. 逐窗口 OOS 与 IS→OOS 衰减（区间重叠,仅诊断）")
    lines.append("")
    for w, selection, oos in zip(research["windows"], research["selections"],
                                 research["oos_results"]):
        lines.append(f"### W{w['k']} OOS {w['val_start']} ~ {w['val_end']}")
        lines.append("")
        lines.append("| 策略 | IS期望 | OOS期望 | Δ期望 | IS胜率% | OOS胜率% | Δ胜率 | WFE |")
        lines.append("|---|---|---|---|---|---|---|---|")
        for s in selection["selected"]:
            name = s["name"]
            oos_m = oos["oos_metrics"].get(name, {})
            is_exp = s["is_expectation"]
            oos_exp = oos_m.get("expectation", 0)
            is_wr = s["is_win_rate"]
            oos_wr = oos_m.get("win_rate", 0)
            is_ann = selection["is_metrics"].get(name, {}).get("annualized_return", 0) or 0
            oos_ann = oos_m.get("annualized_return", 0) or 0
            wfe = round(oos_ann / is_ann, 2) if abs(is_ann) > 1e-10 else None
            lines.append(f"| {name} | {is_exp:.3f} | {oos_exp:.3f} | {oos_exp - is_exp:+.3f} | "
                         f"{is_wr:.1f} | {oos_wr:.1f} | {oos_wr - is_wr:+.1f} | "
                         f"{wfe if wfe is not None else '-'} |")
        lines.append("")

    # 4. 聚合 OOS（头条结论）
    agg = research["aggregate"]
    lines.append("## 4. 聚合 OOS（激活映射按天去重,生产一致口径）")
    lines.append("")
    if agg.get("total_trades", 0) > 0:
        m = agg["metrics"]
        pf = agg.get("portfolio", {})
        lines.append(f"- 参与窗口: W{agg['used_windows']}")
        lines.append(f"- 交易笔数: {agg['total_trades']} | 交易日数: {agg['n_days']}")
        lines.append(f"- 胜率: {m.get('win_rate', 0):.1f}% | 期望: {m.get('expectation', 0):.3f}")
        if pf:
            lines.append(f"- 组合曲线(按天持仓市值): 总收益 {pf.get('total_return', 0):.1f}% | "
                         f"年化 {pf.get('annualized_return', 0):.1f}% | "
                         f"最大回撤 {pf.get('max_drawdown', 0):.1f}% | "
                         f"夏普 {pf.get('sharpe_ratio', 0):.2f} | "
                         f"持仓天数 {pf.get('n_days', 0)}")
        else:
            lines.append(f"- 年化: {m.get('annualized_return', 0):.1f}% | "
                         f"最大回撤: {m.get('max_drawdown', 0):.1f}% | "
                         f"夏普: {m.get('sharpe_ratio', 0):.2f}")
        bench = research["benchmark"]
        if bench:
            excess = m.get("annualized_return", 0) - bench.get("benchmark_annualized", 0)
            lines.append(f"- 基准(沪深300): 总收益 {bench.get('benchmark_return', 0):.1f}% | "
                         f"年化 {bench.get('benchmark_annualized', 0):.1f}%")
            lines.append(f"- 超额年化: {excess:+.1f}%")
    else:
        lines.append("- 无满足门槛的 OOS 交易（完整 OOS 窗口不足）")
    lines.append("")

    # 5. 排序稳定性与策略集 churn
    lines.append("## 5. 排序稳定性（Spearman）与策略集 churn")
    lines.append("")
    lines.append("| 窗口 | IS/OOS 期望 Spearman |")
    lines.append("|---|---|")
    for w, sp in zip(research["windows"], research["spearman"]):
        lines.append(f"| W{w['k']} | {sp if sp is not None else '-'} |")
    lines.append("")
    if research["churn"]:
        lines.append("| 相邻窗口 | 入选策略 Jaccard |")
        lines.append("|---|---|")
        for c in research["churn"]:
            lines.append(f"| {c['from']} → {c['to']} | {c['jaccard']} |")
    lines.append("")

    # 6. 部署工件
    lines.append("## 6. 部署工件")
    lines.append("")
    lines.append(f"- 路径: `{WFO_MODEL_PATH}`")
    lines.append(f"- 来源: 最新完整 train 窗口 W{research['windows'][-1]['k']}")

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info(f"WFO 报告已生成: {path}")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main(argv=None):
    """CLI 入口:research / deploy / daily 三种模式。"""
    parser = argparse.ArgumentParser(description="WFO 滚动向前回测（4Y 训练 / 1Y 验证 / 6M 步长）")
    parser.add_argument("--mode", choices=["research", "deploy", "daily"], default="research",
                        help="research=全量WFO+报告+工件; deploy=仅重建部署工件; daily=每日选股")
    parser.add_argument("--force-retrain", action="store_true", help="强制重训并重建部署工件")
    parser.add_argument("--top-n", type=int, default=10, help="每日选股输出条数（默认10）")
    parser.add_argument("--data-end", default=None, help="数据终点 YYYY-MM-DD（默认今天）")
    args = parser.parse_args(argv)

    if args.mode == "daily":
        recs = run_daily_select(top_n=args.top_n)
        _print_daily(recs)
        return

    data_end = args.data_end or datetime.now().strftime("%Y-%m-%d")
    # 数据终点吸附到最近交易日
    engine = _get_pg_engine()
    try:
        data_end = _snap_trading_day(engine, data_end, "prev")
    finally:
        engine.dispose()

    windows = build_window_grid(data_end)
    if not windows:
        logger.error("无完整 train 窗口可生成（数据不足）")
        return

    if args.mode == "deploy":
        model = load_deployed_model()
        if not _should_retrain(model, windows, args.force_retrain):
            logger.info("部署工件已是最新（无新完整 train 窗口且未强制重训）,跳过")
            return
        latest = windows[-1]
        df_all = build_full_frame(WFO_ANCHOR_START, data_end)
        selection = run_is_selection(df_all, latest)
        save_deployed_model(selection)
        logger.info(f"部署完成: 窗口 W{latest['k']} IS {latest['train_start']}~{latest['train_end']}")
        return

    # research: 全量 WFO
    research = run_research(data_end)
    report_path = WFO_REPORT_DIR / f"wfo_report_{datetime.now():%Y%m%d}.md"
    render_report(research, report_path)
    # 更新部署工件（最新完整 train 窗口）
    save_deployed_model(research["selections"][-1])


if __name__ == "__main__":
    main()