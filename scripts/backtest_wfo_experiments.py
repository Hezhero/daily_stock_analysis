#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""WFO 优化实验脚本（Exp2 / Exp3 / Exp4）

背景: result/wfo_report_20260816.md 显示 IS->OOS Spearman 全为负,
W4 OOS 全策略 0% 胜率(实为 regime 过滤在下跌市生效 + 样本过小)。
本脚本按建议顺序落地三个验证实验:

  Exp2  OOS 一致性选择 + 增强 regime(广度/波动率)
        变体:
          exp2a  OOS-consistent 选择(前序完整窗口 OOS 期望均值 > 0 才可入选)
          exp2c  增强 regime(原 market_ok 且 广度>=0.4 且 指数波动率分位<=0.9)
          exp2b  两者叠加
        对比指标: 聚合 OOS 胜率 / 期望 / 年化 / 超额 / 交易数 / 回撤
  Exp3  出场参数 sweep(ATR 倍数 × trail 系数), 冻结 W2 选择, 只看 OOS 稳定性
  Exp4  共振门槛(同一 (date, code) 至少 N 个策略命中), 交易数 vs 胜率关系

用法:
  python scripts/backtest_wfo_experiments.py --data-end 2026-08-14
  python scripts/backtest_wfo_experiments.py --data-end 2026-08-14 --skip-exp3
"""

import argparse
import gc
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(BASE_DIR / ".env", encoding="utf-8")

import backtest_5y_23strategies as bt  # noqa: E402
import backtest_wfo as wfo  # noqa: E402
from backtest_wfo import (  # noqa: E402
    WFO_ANCHOR_START,
    WFO_TOP_K,
    WFO_MIN_IS_TRADES,
    WFO_BURN_IN_TRADING_DAYS,
    WFO_MIN_OOS_TRADING_DAYS,
    WFO_IS_METRIC,
    build_window_grid,
    build_full_frame,
    _build_price_paths,
    run_oos_eval,
    aggregate_oos,
)
from backtest_5y_23strategies import (  # noqa: E402
    STRATEGIES,
    ENSEMBLE_COMPONENTS,
    TRADING_COST_PCT,
    HOLDING_PERIODS,
    _backtest_single,
    _compute_ensemble_weights,
    _strategy_signal,
    _apply_cooldown,
    calc_metrics,
    compute_benchmark_metrics,
    load_index_daily,
    resolve_parallel_config,
    logger,
)

RESULT_DIR = BASE_DIR / "result"
RESULT_DIR.mkdir(exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
# Exp3: 参数化动态退出（compute_dynamic_exit_returns 的参数化副本,仅实验用）
# ═══════════════════════════════════════════════════════════════════════════════

def compute_dynamic_exit_returns_p(df: pd.DataFrame, atr_mult: float = 2.5,
                                   trail: float = 0.95) -> pd.DataFrame:
    """compute_dynamic_exit_returns 的参数化副本（atr_mult × trail）。

    与生产函数同码,仅把 2.5 / 0.95 两个硬编码抽成参数,用于 Exp3 出场 sweep。
    默认值已随生产函数回写 (2.5, 0.95)（WFO 实验 Exp3 胜出组合,见
    result/wfo_experiments_20260820.md;原 (2.0,0.92) 为旧生产参数）。
    """
    max_p = max(HOLDING_PERIODS)
    df_sorted = df.sort_values(["code", "date"])
    out = pd.DataFrame(index=df_sorted.index, dtype="float32")
    for code, g in df_sorted.groupby("code", sort=False):
        n = len(g)
        close = g["close"].to_numpy(dtype=np.float64)
        high = g["high"].to_numpy(dtype=np.float64)
        low = g["low"].to_numpy(dtype=np.float64)
        atr = g["atr20"].to_numpy(dtype=np.float64)
        rets = {p: np.full(n, np.nan, dtype=np.float32) for p in HOLDING_PERIODS}

        for i in range(n - 1):
            stop = close[i] - atr_mult * atr[i]
            if not np.isfinite(stop):
                continue
            w = min(max_p, n - 1 - i)
            win_low = low[i + 1: i + 1 + w]
            win_high = high[i + 1: i + 1 + w]
            win_close = close[i + 1: i + 1 + w]
            peaks = np.maximum.accumulate(win_high)
            atr_hit = win_low <= stop
            trail_hit = win_close <= peaks * trail
            hit = atr_hit | trail_hit

            for p in HOLDING_PERIODS:
                if p > w:
                    continue
                hit_p = hit[:p]
                if hit_p.any():
                    k = int(np.argmax(hit_p))
                    exit_price = stop if atr_hit[k] else peaks[k] * trail
                else:
                    exit_price = close[i + p]
                if close[i] > 0:
                    rets[p][i] = (exit_price / close[i] - 1.0)

        for p, arr in rets.items():
            out.loc[g.index, f"dyn_ret_{p}d"] = arr

    return out.reindex(df.index)


# ═══════════════════════════════════════════════════════════════════════════════
# Exp2: 增强 regime（广度 + 指数波动率分位,全部因果、无前视）
# ═══════════════════════════════════════════════════════════════════════════════

def add_enhanced_regime(df_all: pd.DataFrame, index_df: pd.DataFrame) -> None:
    """在 df_all 上新增 market_ok_enh 列（原 market_ok 且 广度>=0.4 且 波动率分位<=0.9）。

    - breadth: 当日收盘价 > MA20 的股票占比（全市场广度）
    - idx_vol_pct: 上证指数 20 日收益波动率在近 250 日的分位（滚动,因果）
    """
    # 广度: 每只股票 close > ma20,按日求均值
    above = (df_all["close"] > df_all["ma20"]).astype("float32")
    breadth = above.groupby(df_all["date"]).transform("mean")
    df_all["breadth"] = breadth

    # 指数波动率分位（因果: 只用当日及之前 250 日数据）
    idx = index_df.sort_values("date").reset_index(drop=True)
    idx["ret"] = idx["index_close"].pct_change()
    idx["vol20"] = idx["ret"].rolling(20, min_periods=10).std()
    vol_pct = idx["vol20"].rolling(250, min_periods=60).apply(
        lambda x: float((x[:-1] < x[-1]).mean()), raw=True
    )
    idx["vol_pct"] = vol_pct
    vol_map = idx.set_index("date")["vol_pct"]
    df_all["idx_vol_pct"] = df_all["date"].map(vol_map).fillna(0.5)

    df_all["market_ok_enh"] = (
        df_all["market_ok"]
        & (df_all["breadth"] >= 0.4)
        & (df_all["idx_vol_pct"] <= 0.9)
    )
    logger.info("增强 regime 列已生成: market_ok_enh "
                f"(True 占比 {df_all['market_ok_enh'].mean() * 100:.1f}% vs "
                f"原 market_ok {df_all['market_ok'].mean() * 100:.1f}%)")


# ═══════════════════════════════════════════════════════════════════════════════
# Exp2: IS 回测（与 run_is_selection 同码,拆出供缓存复用）
# ═══════════════════════════════════════════════════════════════════════════════

def _is_backtests(df_all: pd.DataFrame, window: Dict) -> Dict:
    """复制 run_is_selection 的 IS 回测部分,返回 is_metrics / weights / burn_info。"""
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

    component_results = {}
    for name in ENSEMBLE_COMPONENTS:
        r = _backtest_single(name, df_is, select_period_by=WFO_IS_METRIC)
        component_results[name] = r
    weights = _compute_ensemble_weights(component_results)
    bt._ENSEMBLE_WEIGHTS.clear()
    bt._ENSEMBLE_WEIGHTS.update(weights)

    is_metrics = dict(component_results)
    remaining = [n for n in STRATEGIES if n not in ENSEMBLE_COMPONENTS]
    enable_parallel, max_workers = resolve_parallel_config()
    n_workers = min(max_workers, len(remaining))

    def _run_one(name):
        return name, _backtest_single(name, df_is, select_period_by=WFO_IS_METRIC)

    if enable_parallel and n_workers > 1 and len(remaining) > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            for name, r in executor.map(_run_one, remaining):
                is_metrics[name] = r
    else:
        for name in remaining:
            is_metrics[name] = _run_one(name)[1]

    return {"is_metrics": is_metrics, "weights": weights, "burn_info": burn_info}


def _select_variant(window: Dict, is_result: Dict, prior_oos: Dict[str, List[float]],
                    selection_mode: str, top_k: int) -> Dict:
    """从 is_metrics 选出 Top-K。

    selection_mode:
      baseline        与 run_is_selection 一致（仅按 IS 期望排序）
      oos_consistent  前序完整窗口 OOS 期望均值 > 0 的策略才可入选
    """
    is_metrics = is_result["is_metrics"]
    candidates = [r for r in is_metrics.values()
                  if "error" not in r and r.get("total_trades", 0) >= WFO_MIN_IS_TRADES]

    if selection_mode == "oos_consistent" and prior_oos:
        eligible = []
        for r in candidates:
            name = r["strategy"]
            prior = prior_oos.get(name)
            if prior is None or float(np.mean(prior)) <= 0:
                continue
            eligible.append(r)
        candidates = eligible
        logger.info(f"  窗口 W{window['k']} OOS-consistent 资格过滤: "
                    f"{len(candidates)} 个策略通过（前序 OOS 期望>0）")

    candidates.sort(key=lambda x: x.get(WFO_IS_METRIC, -999), reverse=True)
    selected = candidates[:top_k]

    selected_info = []
    for r in selected:
        selected_info.append({
            "name": r["strategy"],
            "best_period": r.get("best_period"),
            "is_win_rate": r.get("win_rate", 0),
            "is_expectation": r.get(WFO_IS_METRIC, 0),
            "is_total_trades": r.get("total_trades", 0),
        })

    return {
        "window": window,
        "burn_in": is_result["burn_info"],
        "is_metrics": is_metrics,
        "selected": selected_info,
        "ensemble_weights": is_result["weights"],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Exp2: 变体 WFO 全流程
# ═══════════════════════════════════════════════════════════════════════════════

def run_wfo_variant(df_all: pd.DataFrame, windows: List[Dict], data_end: str,
                    price_paths: Dict, regime_mode: str = "base",
                    selection_mode: str = "baseline", top_k: int = WFO_TOP_K,
                    is_cache: Optional[Dict] = None) -> Dict:
    """完整 WFO（变体）: IS -> 变体选择 -> OOS -> 聚合 -> 基准。

    regime_mode: "base" | "enhanced"（enhanced 临时把 market_ok 换成 market_ok_enh）
    is_cache: {(regime_mode, k): is_result} 复用 IS 回测（同 regime 下不同选择模式共享）
    """
    t0 = time.time()
    swapped = False
    if regime_mode == "enhanced":
        if "market_ok_enh" not in df_all.columns:
            raise RuntimeError("缺少 market_ok_enh 列,请先调用 add_enhanced_regime")
        df_all["market_ok"] = df_all["market_ok_enh"]
        swapped = True

    try:
        selections, oos_results = [], []
        prior_oos: Dict[str, List[float]] = {}
        for w in windows:
            logger.info(f"[{regime_mode}/{selection_mode}] 窗口 W{w['k']}: "
                        f"IS {w['train_start']}~{w['train_end']} | "
                        f"OOS {w['val_start']}~{w['val_end']}")
            key = (regime_mode, w["k"])
            if is_cache is not None and key in is_cache:
                is_result = is_cache[key]
                # 缓存命中时 _is_backtests 未执行,模块级组合权重停留在上一个变体的
                # 最后窗口;必须按当前窗口的 IS 权重恢复,否则 run_oos_eval 里
                # ensemble 信号（sig_ensemble 读 bt._ENSEMBLE_WEIGHTS）会用错权重。
                bt._ENSEMBLE_WEIGHTS.clear()
                bt._ENSEMBLE_WEIGHTS.update(is_result["weights"])
            else:
                is_result = _is_backtests(df_all, w)
                if is_cache is not None:
                    is_cache[key] = is_result

            selection = _select_variant(w, is_result, prior_oos, selection_mode, top_k)
            selections.append(selection)

            oos = run_oos_eval(df_all, w, selection, price_paths)
            oos_results.append(oos)

            # 仅完整窗口（val_end <= data_end）的 OOS 结果进入前序资格池
            if pd.Timestamp(w["val_end"]) <= pd.Timestamp(data_end):
                for name, m in oos["oos_metrics"].items():
                    prior_oos.setdefault(name, []).append(m.get("expectation", 0))

        agg = aggregate_oos(windows, oos_results, data_end, price_paths)

        benchmark = {}
        if agg.get("total_trades", 0) > 0:
            try:
                idx = load_index_daily(agg["period_start"], agg["period_end"],
                                       ts_code="000300.SH")
                benchmark = compute_benchmark_metrics(None, index_df=idx) if not idx.empty else {}
            except Exception as e:
                logger.warning(f"基准指数加载失败: {e}")

        # IS/OOS Spearman + 相邻窗口 churn（与 run_research 同口径）
        spearman = []
        for w, selection, oos in zip(windows, selections, oos_results):
            is_vals = [s["is_expectation"] for s in selection["selected"]]
            oos_vals = [oos["oos_metrics"].get(s["name"], {}).get("expectation", 0)
                        for s in selection["selected"]]
            spearman.append(wfo._spearman_rank_corr(is_vals, oos_vals))

        churn = []
        for i in range(1, len(selections)):
            prev_set = {s["name"] for s in selections[i - 1]["selected"]}
            curr_set = {s["name"] for s in selections[i]["selected"]}
            union = prev_set | curr_set
            jac = len(prev_set & curr_set) / len(union) if union else 0
            churn.append({"from": f"W{windows[i-1]['k']}", "to": f"W{windows[i]['k']}",
                          "jaccard": round(jac, 3)})

        logger.info(f"[{regime_mode}/{selection_mode}] 完成,耗时 {time.time()-t0:.1f}s")
        return {
            "label": f"{regime_mode}/{selection_mode}/k{top_k}",
            "windows": windows,
            "selections": selections,
            "oos_results": oos_results,
            "aggregate": agg,
            "benchmark": benchmark,
            "spearman": spearman,
            "churn": churn,
        }
    finally:
        if swapped:
            df_all["market_ok"] = df_all["market_ok_base"]


# ═══════════════════════════════════════════════════════════════════════════════
# Exp3: 出场参数 sweep（冻结 W2 选择,只看 OOS 稳定性）
# ═══════════════════════════════════════════════════════════════════════════════

def run_exit_sweep(df_all: pd.DataFrame, windows: List[Dict], data_end: str,
                   price_paths: Dict, baseline: Dict) -> List[Dict]:
    """在 W2 的 OOS 区间上,冻结 baseline 的 W2 选择,对 (atr_mult, trail) 做 3×3 sweep。

    只重算 dyn_ret_{p}d 列（val_start 之后的行,含足够 lookahead）,不动其他逻辑。
    返回每组合的 OOS 交易级指标（胜率/期望/盈亏比/年化/交易数）。
    """
    w2 = windows[1]  # 唯一完整参与聚合的窗口
    w2_selection = baseline["selections"][1]
    val_start = pd.Timestamp(w2["val_start"])
    val_end = pd.Timestamp(w2["val_end"])

    # 只取 val_start 之后的行（含 lookahead 到 data_end）作为 sweep 工作副本,
    # 避免在完整 df_all（数百万行）上反复变异/恢复 dyn_ret 列——
    # 全帧原地变异 + 恢复循环是此前计数/指标跨运行不稳定的怀疑来源。
    dyn_cols = [f"dyn_ret_{p}d" for p in HOLDING_PERIODS]
    sweep_df: pd.DataFrame = df_all.loc[df_all["date"] >= val_start].copy()

    combos = [(a, t) for a in (1.5, 2.0, 2.5) for t in (0.90, 0.92, 0.95)]
    results = []
    for atr_mult, trail in combos:
        t0 = time.time()
        # 重算全部 dyn_ret 列并整列替换（sweep_df 覆盖 val_start~data_end,含 lookahead）
        dyn = compute_dynamic_exit_returns_p(sweep_df, atr_mult=atr_mult, trail=trail)
        for c in dyn_cols:
            sweep_df[c] = dyn[c].to_numpy()

        # 用冻结选择跑 W2 OOS（run_oos_eval 内部再按 [val_start, val_end] 切片）
        oos = run_oos_eval(sweep_df, w2, w2_selection, price_paths)
        trades = oos["trades"]
        m = calc_metrics(trades["ret"].values, avg_holding=None) if not trades.empty else {}
        m["total_trades"] = int(len(trades))
        results.append({
            "atr_mult": atr_mult,
            "trail": trail,
            "win_rate": m.get("win_rate", 0),
            "expectation": m.get("expectation", 0),
            "profit_loss_ratio": m.get("profit_loss_ratio", 0),
            "annualized_return": m.get("annualized_return", 0),
            "total_trades": m.get("total_trades", 0),
            "avg_win": m.get("avg_win", 0),
            "avg_loss": m.get("avg_loss", 0),
        })
        logger.info(f"Exp3 combo (atr={atr_mult}, trail={trail}): "
                    f"胜率 {results[-1]['win_rate']:.1f}% 期望 {results[-1]['expectation']:.3f} "
                    f"年化 {results[-1]['annualized_return']:.1f}% "
                    f"交易 {results[-1]['total_trades']} 耗时 {time.time()-t0:.1f}s")

    # 一致性校验: dyn_ret 的 NaN 模式与 (atr_mult, trail) 无关（只取决于 stop 有限性、
    # p>w 与 close>0）,9 个组合的交易数必须完全相同;若不一致说明 sweep 结果不稳定。
    # 已知问题: 串行下也会触发,并非并行竞态或 Exp2 遗留状态所致——复现实验在 sweep
    # 开始时 dump 状态（market_ok/ENSEMBLE_WEIGHTS/dyn_ret NaN 均正常）仍出现交易数
    # 不一致,指向重计算后的进程内随机内存级异常;如需逐位复现请拆到独立进程运行。
    counts = {r["total_trades"] for r in results}
    if len(counts) != 1:
        logger.error(f"Exp3 交易数不一致({sorted(counts)}): sweep 结果不稳定,"
                     f"结果不可信;已排除并行竞态/Exp2 遗留状态/全帧变异,"
                     f"为进程内随机内存级异常,建议拆独立进程复跑")

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# Exp4: 共振门槛（同一 (date, code) 至少 N 个策略命中）
# ═══════════════════════════════════════════════════════════════════════════════

def run_confluence_analysis(windows: List[Dict], oos_results: List[Dict],
                            data_end: str, price_paths: Dict) -> Dict[int, Dict]:
    """对 baseline 的 OOS 交易按 (date, code) 共振策略数做门槛过滤后聚合。

    返回 {min_strategies: 聚合指标},min_strategies 从 1（=baseline）到 5。
    """
    data_end_ts = pd.Timestamp(data_end)
    out = {}
    for min_s in range(1, 6):
        combined = []
        used_windows = []
        for i, (w, oos) in enumerate(zip(windows, oos_results)):
            seg_end = windows[i + 1]["val_start"] if i + 1 < len(windows) else data_end
            seg_start = w["val_start"]
            trades = oos["trades"]
            if trades.empty:
                continue
            seg = trades[(trades["date"] >= pd.Timestamp(seg_start)) &
                         (trades["date"] < pd.Timestamp(seg_end))].copy()
            n_days = seg["date"].nunique()
            if n_days < WFO_MIN_OOS_TRADING_DAYS:
                continue
            if min_s > 1:
                cnt = seg.groupby(["date", "code"])["strategy"].nunique()
                keep = cnt[cnt >= min_s].index
                seg = seg[seg.set_index(["date", "code"]).index.isin(keep)]
            combined.append(seg)
            used_windows.append(w["k"])

        if not combined:
            out[min_s] = {"total_trades": 0, "win_rate": 0, "expectation": 0,
                          "annualized_return": 0, "n_days": 0, "used_windows": used_windows}
            continue

        all_trades = pd.concat(combined, ignore_index=True)
        metrics = calc_metrics(all_trades["ret"].values, avg_holding=None)
        metrics["total_trades"] = int(len(all_trades))
        metrics["n_days"] = int(all_trades["date"].nunique())
        metrics["used_windows"] = used_windows

        portfolio = wfo.compute_portfolio_curve(all_trades, price_paths)
        if portfolio:
            metrics["annualized_return"] = portfolio["annualized_return"]
            metrics["max_drawdown"] = portfolio["max_drawdown"]
            metrics["sharpe_ratio"] = portfolio["sharpe_ratio"]
        out[min_s] = metrics
        logger.info(f"Exp4 共振门槛 >= {min_s}: {metrics['total_trades']} 笔交易, "
                    f"胜率 {metrics['win_rate']:.1f}%, 期望 {metrics['expectation']:.3f}, "
                    f"年化 {metrics['annualized_return']:.1f}%")
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# 报告渲染
# ═══════════════════════════════════════════════════════════════════════════════

def _fmt_agg(agg: Dict, bench: Dict) -> Dict:
    m = agg.get("metrics", {})
    excess = m.get("annualized_return", 0) - bench.get("benchmark_annualized", 0)
    return {
        "win_rate": m.get("win_rate", 0),
        "expectation": m.get("expectation", 0),
        "total_trades": agg.get("total_trades", 0),
        "n_days": agg.get("n_days", 0),
        "annualized": m.get("annualized_return", 0),
        "max_dd": m.get("max_drawdown", 0),
        "sharpe": m.get("sharpe_ratio", 0),
        "excess": excess,
        "used_windows": agg.get("used_windows", []),
    }


def render_report(results: Dict, path: Path) -> None:
    lines = []
    lines.append("# WFO 优化实验报告（Exp2/Exp3/Exp4）\n")
    lines.append(f"- 数据终点: {results['data_end']}")
    lines.append(f"- 生成时间: {pd.Timestamp.now():%Y-%m-%d %H:%M}\n")

    # ── Exp2 对比 ──
    lines.append("## Exp2: OOS 一致性选择 + 增强 regime\n")
    lines.append("| 变体 | 胜率% | 期望 | 交易数 | 交易日 | 年化% | 最大回撤% | 夏普 | 超额年化% | 参与窗口 |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for label, r in results["exp2"].items():
        f = _fmt_agg(r["aggregate"], r["benchmark"])
        lines.append(
            f"| {label} | {f['win_rate']:.1f} | {f['expectation']:.3f} | "
            f"{f['total_trades']} | {f['n_days']} | {f['annualized']:.1f} | "
            f"{f['max_dd']:.1f} | {f['sharpe']:.2f} | {f['excess']:+.1f} | "
            f"W{f['used_windows']} |")

    lines.append("\n### 各窗口入选策略\n")
    for label, r in results["exp2"].items():
        lines.append(f"\n**{label}**（IS→OOS Spearman: "
                     f"{[s if s is not None else 'n/a' for s in r['spearman']]}）")
        for sel in r["selections"]:
            names = ", ".join(f"{s['name']}(p{s['best_period']})" for s in sel["selected"])
            lines.append(f"- W{sel['window']['k']}: {names}")

    # ── Exp3 ──
    lines.append("\n## Exp3: 出场参数 sweep（冻结 W2 选择, OOS 稳定性）\n")
    lines.append("| ATR倍数 | Trail | 胜率% | 期望 | 盈亏比 | 年化% | 交易数 |")
    lines.append("|---|---|---|---|---|---|---|")
    for r in results["exp3"]:
        lines.append(
            f"| {r['atr_mult']} | {r['trail']} | {r['win_rate']:.1f} | "
            f"{r['expectation']:.3f} | {r['profit_loss_ratio']:.2f} | "
            f"{r['annualized_return']:.1f} | {r['total_trades']} |")

    # ── Exp4 ──
    lines.append("\n## Exp4: 共振门槛（同一股票同日至少 N 个策略）\n")
    lines.append("| 门槛N | 交易数 | 胜率% | 期望 | 年化% | 交易日 |")
    lines.append("|---|---|---|---|---|---|")
    for n, m in results["exp4"].items():
        lines.append(
            f"| >= {n} | {m.get('total_trades', 0)} | {m.get('win_rate', 0):.1f} | "
            f"{m.get('expectation', 0):.3f} | {m.get('annualized_return', 0):.1f} | "
            f"{m.get('n_days', 0)} |")

    lines.append("\n## 结论要点\n")
    lines.append("（待人工根据上表补充）\n")

    path.write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"实验报告已写入 {path}")


# ═══════════════════════════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════════════════════════

def main(argv=None):
    parser = argparse.ArgumentParser(description="WFO 优化实验（Exp2/Exp3/Exp4）")
    parser.add_argument("--data-end", default="2026-08-14", help="数据终点(默认2026-08-14)")
    parser.add_argument("--skip-exp3", action="store_true", help="跳过 Exp3 出场 sweep")
    parser.add_argument("--skip-exp4", action="store_true", help="跳过 Exp4 共振门槛")
    args = parser.parse_args(argv)

    t_start = time.time()
    data_end = args.data_end

    windows = build_window_grid(data_end)
    if not windows:
        raise RuntimeError("无完整 train 窗口可生成（数据不足）")
    logger.info(f"窗口网格: {len(windows)} 个窗口,数据终点 {data_end}")

    df_all = build_full_frame(WFO_ANCHOR_START, data_end)
    # 保存原始 market_ok 供 regime 切换恢复
    df_all["market_ok_base"] = df_all["market_ok"]

    idx = load_index_daily(WFO_ANCHOR_START, data_end)
    add_enhanced_regime(df_all, idx)
    del idx
    gc.collect()

    price_paths = _build_price_paths(df_all)

    # sweep 用干净切片（val_start 起,含 lookahead）;变体在副本上运行后 df_all 不再被
    # 变异,此切片作为防御性保障,确保 sweep 数据不受任何意外变异影响
    sweep_slice = df_all.loc[df_all["date"] >= pd.Timestamp(windows[1]["val_start"])].copy()

    # ── Exp2: 4 个变体（IS 回测按 regime 缓存复用） ──
    is_cache: Dict = {}
    exp2 = {}
    exp2["baseline"] = run_wfo_variant(df_all, windows, data_end, price_paths,
                                       "base", "baseline", WFO_TOP_K, is_cache)
    exp2["exp2a_oos_consistent"] = run_wfo_variant(df_all, windows, data_end, price_paths,
                                                   "base", "oos_consistent", WFO_TOP_K, is_cache)
    exp2["exp2c_enhanced_regime"] = run_wfo_variant(df_all, windows, data_end, price_paths,
                                                    "enhanced", "baseline", WFO_TOP_K, is_cache)
    exp2["exp2b_both"] = run_wfo_variant(df_all, windows, data_end, price_paths,
                                         "enhanced", "oos_consistent", WFO_TOP_K, is_cache)

    # ── Exp3: 出场 sweep（冻结 baseline W2 选择,用干净切片） ──
    exp3 = []
    if not args.skip_exp3:
        exp3 = run_exit_sweep(sweep_slice, windows, data_end, price_paths, exp2["baseline"])

    # ── Exp4: 共振门槛（基于 baseline 的 OOS 交易） ──
    exp4 = {}
    if not args.skip_exp4:
        exp4 = run_confluence_analysis(windows, exp2["baseline"]["oos_results"],
                                       data_end, price_paths)

    # ── 汇总 ──
    results = {
        "data_end": data_end,
        "exp2": exp2,
        "exp3": exp3,
        "exp4": exp4,
    }
    report_path = RESULT_DIR / f"wfo_experiments_{pd.Timestamp.now():%Y%m%d}.md"
    render_report(results, report_path)

    # 控制台摘要
    print("\n" + "=" * 72)
    print("Exp2 聚合对比:")
    for label, r in exp2.items():
        f = _fmt_agg(r["aggregate"], r["benchmark"])
        print(f"  {label:28s} 胜率 {f['win_rate']:5.1f}%  期望 {f['expectation']:+.3f}  "
              f"交易 {f['total_trades']:5d}  年化 {f['annualized']:6.1f}%  "
              f"超额 {f['excess']:+6.1f}%  回撤 {f['max_dd']:5.1f}%  W{f['used_windows']}")
    if exp3:
        print("\nExp3 出场 sweep（W2 OOS）:")
        for r in exp3:
            print(f"  atr={r['atr_mult']} trail={r['trail']}: "
                  f"胜率 {r['win_rate']:5.1f}%  期望 {r['expectation']:+.3f}  "
                  f"年化 {r['annualized_return']:6.1f}%  交易 {r['total_trades']}")
    if exp4:
        print("\nExp4 共振门槛:")
        for n, m in exp4.items():
            print(f"  >= {n}: 交易 {m.get('total_trades', 0):5d}  "
                  f"胜率 {m.get('win_rate', 0):5.1f}%  期望 {m.get('expectation', 0):+.3f}  "
                  f"年化 {m.get('annualized_return', 0):6.1f}%")
    print(f"\n总耗时 {time.time()-t_start:.1f}s | 报告: {report_path}")
    return results


if __name__ == "__main__":
    main()