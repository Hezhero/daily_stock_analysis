#!/usr/bin/env python3
"""ML 信号过滤器（P3-11）：LightGBM 二分类，预测信号后 dyn_ret_3d>0 概率。

训练：以各策略过滤后信号点的并集为样本（enhanced+行业动量默认口径，
冷却期前以保证样本量），时间切分（最后 12 个月为 OOS），输出：
  - result/ml_filter_model.txt   LightGBM 模型
  - result/ml_filter_meta.json   特征清单/OOS 指标/置信度阈值
推理：apply_ml_filter(df, sig_dict) 按 meta.threshold 过滤低置信信号。

用法:
  python scripts/train_signal_filter.py                # 全量训练
  python scripts/train_signal_filter.py --stage pre    # 仅构建缓存帧
"""
import os, sys, gc, time, json, argparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT = os.path.join(BASE_DIR, "result")
CACHE_PRE = os.path.join(OUT, "p3_cache_pre.parquet")
MODEL_PATH = os.path.join(OUT, "ml_filter_model.txt")
META_PATH = os.path.join(OUT, "ml_filter_meta.json")

OOS_DAYS = 365          # OOS 验证窗口（天）
THRESHOLD_QUANTILE = 0.60  # 置信度阈值 = OOS 预测概率的 60 分位（保留前 40% 样本）
SEED = 42

# 特征清单：仅使用信号日 t 及之前的信息，无前视
RATIO_EPS = 1e-9


def feature_cols() -> list:
    return [
        # 技术面
        "rsi6", "rsi12", "rsi24", "macd_dif", "macd_dea", "macd_hist",
        "macd_dif2", "macd_hist2",
        "ma5_dev", "ma20_dev", "ma60_dev", "ma120_dev",
        "boll_pos", "atr_pct", "vol_surge", "high20_dev", "pct_chg",
        # 量能/流动性
        "turnover_rate_f", "volume_ratio",
        # 基本面
        "pe_ttm", "pb_mrq", "ps_ttm", "dv_ratio", "fin_roe", "fin_or_yoy",
        # 筹码
        "profit_ratio", "concentration_90", "concentration_70", "avg_cost_dev",
        # 资金
        "net_mf_amount", "net_big_amount", "rzye_chg5",
        # 行业/市况
        "ind_rank", "regime_code",
    ]


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """向量化构造特征列，返回仅含特征列的 DataFrame。

    可选列（筹码/资金/行业等）缺失时以 NaN 填充——LightGBM 原生支持缺失值，
    且推理端允许在数据不全时降级打分。
    """
    out = pd.DataFrame(index=df.index)
    close = df["close"]
    passthrough = [
        "rsi6", "rsi12", "rsi24", "macd_dif", "macd_dea", "macd_hist",
        "macd_dif2", "macd_hist2", "pct_chg", "turnover_rate_f",
        "volume_ratio", "pe_ttm", "pb_mrq", "ps_ttm", "dv_ratio",
        "fin_roe", "fin_or_yoy", "profit_ratio", "concentration_90",
        "concentration_70", "net_mf_amount", "net_big_amount",
        "rzye_chg5", "ind_rank",
    ]
    for c in passthrough:
        out[c] = df[c] if c in df.columns else np.nan
    for w in (5, 20, 60, 120):
        out[f"ma{w}_dev"] = close / df[f"ma{w}"] - 1.0
    rng = (df["boll_upper"] - df["boll_lower"]).abs()
    out["boll_pos"] = (close - df["boll_lower"]) / (rng + RATIO_EPS)
    out["atr_pct"] = df["atr20"] / (close + RATIO_EPS)
    out["vol_surge"] = df["volume"] / (df["vol_ma5"] + RATIO_EPS)
    out["high20_dev"] = close / (df["high_20d_max"] + RATIO_EPS) - 1.0
    out["avg_cost_dev"] = close / (df["avg_cost"] + RATIO_EPS) - 1.0 \
        if "avg_cost" in df.columns else np.nan
    out["regime_code"] = df["regime"].map({"bear": 0, "range": 1, "bull": 2}) \
        if "regime" in df.columns else np.nan
    out = out.replace([np.inf, -np.inf], np.nan)
    return out[feature_cols()]


def _build_pre_exit_frame(anchored_start: str, today_str: str) -> pd.DataFrame:
    """构建特征所需的最小完整帧（含 ind_rank 与 dyn_ret_3d 标签列）。"""
    from scripts import backtest_5y_23strategies as m

    t0 = time.time()
    print("step: load_data ...", flush=True)
    df_market = m.load_data(anchored_start, today_str)
    df_factor = m.load_adj_factors_from_db(anchored_start, today_str)
    df_adjusted = m.apply_forward_adjustment(df_market, df_factor)
    del df_market, df_factor
    gc.collect()

    print("step: load_signal_aux ...", flush=True)
    df_adjusted = m.load_signal_aux(df_adjusted)
    gc.collect()

    print("step: compute_indicators ...", flush=True)
    df_all = m.compute_indicators(df_adjusted)
    del df_adjusted
    gc.collect()

    print("step: regime ...", flush=True)
    df_index = m.load_index_daily(anchored_start, today_str)
    regime_df = m.compute_market_ok(df_index)
    df_all = df_all.merge(regime_df, on="date", how="left")
    df_all["market_ok"] = df_all["market_ok"].fillna(False).astype(bool)
    df_all["market_ok_enh"] = df_all["market_ok_enh"].fillna(False).astype(bool)
    df_all["regime"] = df_all["regime"].fillna("range")
    del df_index, regime_df
    gc.collect()

    print("step: fina ...", flush=True)
    try:
        df_fina = m.load_fina_indicator(anchored_start, today_str)
        if not df_fina.empty:
            df_all = m.merge_fina_by_ann_date(df_all, df_fina)
            df_all.rename(columns={"roe": "fin_roe", "grossprofit_margin": "fin_gross_margin",
                                   "or_yoy": "fin_or_yoy"}, inplace=True)
        del df_fina
        gc.collect()
    except Exception as e:
        print(f"  fina skipped: {e}", flush=True)

    print("step: industry context ...", flush=True)
    try:
        rank_df, member_df = m.load_industry_context(anchored_start, today_str)
        df_all = m.apply_industry_momentum(df_all, rank_df, member_df)
        del rank_df, member_df
        gc.collect()
    except Exception as e:
        print(f"  industry skipped: {e}", flush=True)
        df_all["ind_rank"] = np.nan

    print("step: dynamic exits (default, for label) ...", flush=True)
    dyn_ret = m.compute_dynamic_exit_returns(df_all)
    df_all = pd.concat([df_all, dyn_ret], axis=1)
    del dyn_ret
    gc.collect()

    print(f"pre frame done in {time.time()-t0:.0f}s, rows={len(df_all):,}", flush=True)
    return df_all


def cmd_stage_pre(anchored_start: str, today_str: str):
    if os.path.exists(CACHE_PRE) and (time.time() - os.path.getmtime(CACHE_PRE)) / 86400 < 1:
        print("pre 缓存仍新鲜，跳过")
        return
    df = _build_pre_exit_frame(anchored_start, today_str)
    df.to_parquet(CACHE_PRE, index=False)
    print(f"pre 帧已缓存: {len(df):,} 行")


def cmd_shard(year: int):
    """单分片子进程：构建该年切片并保存样本 parquet。

    独立进程运行以规避本仓库已文档化的"同进程重度重计算后随机信号抑制"
    （见 result/wfo_experiments_20260820.md 已知问题）——父进程只做拼接与训练。
    """
    from scripts import backtest_5y_23strategies as m

    five_years_ago = (pd.Timestamp.now() - pd.DateOffset(years=5)).strftime("%Y-%m-%d")
    anchored_start = f"{five_years_ago[:4]}-01-01"
    today_str = pd.Timestamp.now().strftime("%Y-%m-%d")

    warm_start = anchored_start if year == int(anchored_start[:4]) else f"{year - 1}-06-01"
    end = min(today_str, f"{year}-12-31")
    print(f"[shard {year}] {warm_start} ~ {end}", flush=True)
    df = _build_pre_exit_frame(warm_start, end)

    out_path = os.path.join(OUT, f"p3_shard_{year}.parquet")
    if os.path.exists(out_path):
        os.remove(out_path)

    union = np.zeros(len(df), dtype=bool)
    for name in m.STRATEGIES:
        sig = m._strategy_signal(df, name, enhanced=True, industry_filter=True)
        union |= sig.astype(bool).to_numpy()
    sub = df.loc[df.index[union], ["code", "date", "dyn_ret_3d"]].copy()
    sub = sub[sub["date"] >= pd.Timestamp(f"{year}-01-01")]
    feats = build_features(df.loc[sub.index])
    sub = pd.concat([sub, feats], axis=1)
    del df
    gc.collect()
    sub.to_parquet(out_path, index=False)
    print(f"[shard {year}] 样本 {len(sub):,} → {out_path}", flush=True)


def cmd_train(cutoff_days: int = OOS_DAYS):
    """父进程：逐分片拉起独立子进程（规避同进程内存级异常），拼接后训练。"""
    import lightgbm as lgb
    import subprocess

    five_years_ago = (pd.Timestamp.now() - pd.DateOffset(years=5)).strftime("%Y-%m-%d")
    anchored_start = f"{five_years_ago[:4]}-01-01"
    today_str = pd.Timestamp.now().strftime("%Y-%m-%d")

    this_year = pd.Timestamp.now().year
    years = list(range(int(anchored_start[:4]), this_year + 1))
    for y in years:
        part = os.path.join(OUT, f"p3_shard_{y}.parquet")
        if os.path.exists(part):
            print(f"[train] 分片 {y} 已存在，跳过", flush=True)
            continue
        cmd = [sys.executable, os.path.abspath(__file__), "--shard", str(y)]
        print(f"[train] 拉起分片子进程 {y}: {' '.join(cmd)}", flush=True)
        r = subprocess.run(cmd, cwd=str(BASE_DIR))
        if r.returncode != 0:
            raise SystemExit(f"分片 {y} 失败（exit={r.returncode}）")

    parts = [pd.read_parquet(os.path.join(OUT, f"p3_shard_{y}.parquet"))
             for y in years if os.path.exists(os.path.join(OUT, f"p3_shard_{y}.parquet"))]
    if not parts:
        raise SystemExit("无可用分片样本")
    data = pd.concat(parts, ignore_index=True)
    del parts
    gc.collect()
    data = data.dropna(subset=["dyn_ret_3d"])
    label = (data["dyn_ret_3d"] > 0).astype(int)
    dates = data["date"]
    cols = feature_cols()
    feats_all = data[cols]

    cutoff = dates.max() - pd.Timedelta(days=cutoff_days)
    is_train = (dates < cutoff).to_numpy()
    X_tr, y_tr = feats_all[is_train], label[is_train]
    X_va, y_va = feats_all[~is_train], label[~is_train]
    print(f"\n总样本 {len(data):,} | train {len(X_tr):,} (正率 {y_tr.mean()*100:.1f}%) | "
          f"valid(OOS {cutoff.date()}~) {len(X_va):,} (正率 {y_va.mean()*100:.1f}%)",
          flush=True)
    if len(X_va) < 300 or len(X_tr) < 1200:
        raise SystemExit(f"样本不足（train={len(X_tr)}, valid={len(X_va)}），无法训练")

    model = lgb.LGBMClassifier(
        n_estimators=400, learning_rate=0.05, num_leaves=15,
        subsample=0.8, colsample_bytree=0.8,
        min_child_samples=50, random_state=SEED, n_jobs=-1,
    )
    model.fit(X_tr, y_tr, eval_set=[(X_va, y_va)], eval_metric="auc",
              callbacks=[lgb.early_stopping(50, verbose=False)])

    from sklearn.metrics import roc_auc_score
    p_va = model.predict_proba(X_va)[:, 1]
    auc = roc_auc_score(y_va, p_va)
    base_wr = y_va.mean() * 100

    print("\nOOS 置信度十分位 → 胜率 / 平均收益(dyn_ret_3d):")
    dec = pd.qcut(p_va, 10, labels=False, duplicates="drop")
    tbl = []
    va_ret = data.loc[X_va.index, "dyn_ret_3d"] * 100
    for q in sorted(pd.unique(dec)):
        msk = dec == q
        tbl.append((int(q) + 1, int(msk.sum()), float(y_va[msk].mean() * 100),
                    float(va_ret[msk].mean())))
        print(f"  D{q+1}: n={msk.sum():>7,} 胜率={y_va[msk].mean()*100:>5.1f}% "
              f"均收益={va_ret[msk].mean():>6.2f}%")
    thr = float(np.quantile(p_va, THRESHOLD_QUANTILE))
    keep = p_va >= thr
    kept_wr = y_va[keep].mean() * 100
    print(f"\n阈值(P{THRESHOLD_QUANTILE:.0f})={thr:.4f} → 保留 {keep.mean()*100:.1f}% 样本, "
          f"OOS 胜率 {base_wr:.1f}%→{kept_wr:.1f}%")

    model.booster_.save_model(MODEL_PATH)
    meta = {
        "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "feature_cols": cols,
        "threshold": thr,
        "threshold_quantile": THRESHOLD_QUANTILE,
        "oos_auc": round(float(auc), 4),
        "oos_base_win_rate": round(float(base_wr), 2),
        "oos_kept_win_rate": round(float(kept_wr), 2),
        "oos_keep_ratio": round(float(keep.mean()), 4),
        "cutoff": str(cutoff.date()),
        "n_train": int(len(X_tr)), "n_valid": int(len(X_va)),
        "deciles": [{"d": d_, "n": n_, "wr": round(w, 2), "ret": round(r, 2)}
                    for d_, n_, w, r in tbl],
    }
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"模型已保存: {MODEL_PATH}\n元数据已保存: {META_PATH}")


def load_meta(path: str = META_PATH) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def apply_ml_filter(df: pd.DataFrame, sig_dict: dict,
                    meta_path: str = META_PATH) -> dict:
    """按工件阈值过滤低置信信号；模型/依赖缺失时原样返回（不误杀）。"""
    try:
        import lightgbm as lgb
    except ImportError:
        print("[ml-filter] lightgbm 未安装，跳过过滤")
        return sig_dict
    if not (os.path.exists(MODEL_PATH) and os.path.exists(meta_path)):
        print("[ml-filter] 工件缺失，跳过过滤")
        return sig_dict
    meta = load_meta(meta_path)
    booster = lgb.Booster(model_file=MODEL_PATH)
    cols = meta["feature_cols"]
    thr = float(meta["threshold"])

    out = {}
    hit_rows = None
    conf_of_row = None
    first = True
    for name, sig in sig_dict.items():
        mask = sig.astype(bool)
        if not mask.any():
            out[name] = sig
            continue
        if first:
            feats = build_features(df)[cols]
            conf_of_row = pd.Series(
                booster.predict(feats), index=df.index)
            hit_rows = mask.to_numpy()
            first = False
        keep = conf_of_row.reindex(df.index[mask]).to_numpy() >= thr
        new_sig = sig.copy()
        new_sig.iloc[:] = False
        kept_idx = df.index[mask][keep]
        new_sig.loc[kept_idx] = True
        dropped = int(mask.sum() - len(kept_idx))
        print(f"[ml-filter] {name}: {int(mask.sum())}→{len(kept_idx)} (-{dropped})")
        out[name] = new_sig
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=["pre"], default=None)
    parser.add_argument("--shard", type=int, default=None,
                        help="只构建指定年份的分片样本（独立子进程模式）")
    parser.add_argument("--cutoff-days", type=int, default=OOS_DAYS,
                        help="OOS 验证窗口天数（默认 365）")
    args = parser.parse_args()

    if args.shard is not None:
        cmd_shard(args.shard)
        return

    five_years_ago = (pd.Timestamp.now() - pd.DateOffset(years=5)).strftime("%Y-%m-%d")
    anchored_start = f"{five_years_ago[:4]}-01-01"
    today_str = pd.Timestamp.now().strftime("%Y-%m-%d")

    if args.stage == "pre":
        cmd_stage_pre(anchored_start, today_str)
        return
    cmd_train(cutoff_days=args.cutoff_days)


if __name__ == "__main__":
    main()
