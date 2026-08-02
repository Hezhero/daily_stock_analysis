#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
23 策略 5 年回测 + 本周验证
回测区间: 2021-01-01 ~ 最近第6个交易日
验证区间: 最近第5个交易日 ~ 最近第1个交易日

优化点:
1. 并行回测: ThreadPoolExecutor（向量化释放GIL）
2. 向量化验证: 移除 iterrows()
3. 预计算共享指标: vol_std_20d 等
4. Numba加速: calc_metrics
5. 全量 float32 降精度节省内存
6. 自动资源检测: 根据 CPU/GPU/内存 自动选择串行/并行模式

修改点:
- 移除邮件发送功能
- 移除三步过滤功能
- 移除多级缓存机制，每次运行重新计算
- 移除 baostock 依赖，全部替换为 Tushare Pro API
- 回测完成后自动执行主程序进行大盘复盘和个股决策
"""

import argparse
import logging
import os
import sys
import time
import gc
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

# ─── 配置 ──────────────────────────────────────────────────────────────────────
INITIAL_CAPITAL = 1000000.0
TOP_N_VALIDATE = 5
HOLDING_PERIODS = [1, 3, 5, 10]
VALIDATE_DAYS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("backtest")


# ═══════════════════════════════════════════════════════════════════════════════
# 代码格式转换 & 数据库连接
# ═══════════════════════════════════════════════════════════════════════════════

def _from_ts_code(ts_code: str) -> str:
    num, suffix = ts_code.split(".")
    return f"{suffix.lower()}.{num}"


def _get_pg_engine():
    host = os.environ.get("PG_HOST", "127.0.0.1")
    port = os.environ.get("PG_PORT", "5432")
    dbname = os.environ.get("PG_DBNAME", "tushare")
    user = os.environ.get("PG_USER", "root")
    password = os.environ.get("PG_PASSWORD", "")
    if not password:
        raise RuntimeError("PG_PASSWORD 未设置，请检查 .env 文件")
    quoted_user = quote_plus(user)
    quoted_password = quote_plus(password)
    return create_engine(f"postgresql+psycopg2://{quoted_user}:{quoted_password}@{host}:{port}/{dbname}")


# ═══════════════════════════════════════════════════════════════════════════════
# 系统资源检测
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_total_memory_gb() -> Optional[float]:
    try:
        import psutil
        return psutil.virtual_memory().total / (1024 ** 3)
    except ImportError:
        pass
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]
        mem_status = MEMORYSTATUSEX()
        mem_status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        kernel32.GlobalMemoryStatusEx(ctypes.byref(mem_status))
        return mem_status.ullTotalPhys / (1024 ** 3)
    except Exception:
        pass
    return None


def _detect_available_memory_gb() -> Optional[float]:
    try:
        import psutil
        return psutil.virtual_memory().available / (1024 ** 3)
    except ImportError:
        pass
    return None


def _detect_swap_memory_gb() -> Optional[Dict[str, float]]:
    """返回 {'total': float, 'free': float, 'used': float, 'pct': float} 或 None"""
    try:
        import psutil
        swap = psutil.swap_memory()
        return {
            "total": swap.total / (1024 ** 3),
            "free": swap.free / (1024 ** 3),
            "used": swap.used / (1024 ** 3),
            "pct": swap.percent,
        }
    except ImportError:
        pass
    try:
        import ctypes
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]
        mem_status = MEMORYSTATUSEX()
        mem_status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(mem_status))
        total = mem_status.ullTotalPageFile / (1024 ** 3)
        free = mem_status.ullAvailPageFile / (1024 ** 3)
        used = total - free
        pct = (used / total * 100) if total > 0 else 0
        return {"total": total, "free": free, "used": used, "pct": pct}
    except Exception:
        pass
    return None


def _detect_extended_available_memory_gb() -> Dict[str, float]:
    """返回 {'ram_available': float|None, 'swap_free': float|None, 'total_effective': float|None}"""
    ram = _detect_available_memory_gb()
    swap = _detect_swap_memory_gb()
    swap_free = swap["free"] if swap else None
    effective = None
    if ram is not None:
        effective = ram + (swap_free or 0)
    return {
        "ram_available": ram,
        "swap_free": swap_free,
        "total_effective": effective,
    }


def _detect_gpu() -> Tuple[bool, str]:
    """
    返回 (has_gpu, gpu_description)。
    检测 NVIDIA GPU（通过 nvidia-smi）和 CUDA 可用性。
    """
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            lines = result.stdout.strip().split("\n")
            gpu_names = []
            for line in lines:
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 2:
                    gpu_names.append(f"{parts[0]} ({parts[1]}MB)")
            return True, "; ".join(gpu_names)
    except Exception:
        pass
    return False, ""


def detect_resource_config() -> dict:
    """
    自动检测系统资源配置并返回推荐配置。

    返回 dict:
      - enable_parallel: bool
      - max_workers: int
      - cpu_count: int
      - total_memory_gb: float | None
      - available_memory_gb: float | None
      - total_swap_gb: float | None
      - free_swap_gb: float | None
      - has_gpu: bool
      - gpu_description: str
      - mode_description: str
    """
    cpu_count = os.cpu_count() or 1
    total_memory_gb = _detect_total_memory_gb()
    available_memory_gb = _detect_available_memory_gb()
    swap = _detect_swap_memory_gb()
    total_swap_gb = swap["total"] if swap else None
    free_swap_gb = swap["free"] if swap else None
    has_gpu, gpu_description = _detect_gpu()

    # 有效可用内存 = RAM 可用 + Swap 空闲（含 swap 信息）
    if available_memory_gb is not None:
        effective_memory = available_memory_gb + (free_swap_gb or 0)
    else:
        effective_memory = total_memory_gb

    swap_note = ""
    if free_swap_gb is not None and free_swap_gb > 0.5 and effective_memory is not None:
        ram_only = available_memory_gb or 0
        ram_pct = (ram_only / effective_memory * 100) if effective_memory > 0 else 100
        if ram_pct < 60:
            swap_note = f" (RAM仅{ram_only:.1f}GB，Swap补充{effective_memory - ram_only:.1f}GB，性能可能下降)"

    if effective_memory is not None and effective_memory < 4:
        enable_parallel = False
        max_workers = 1
        reason = f"可用内存仅 {effective_memory:.1f}GB{swap_note}，强制串行避免 OOM"
    elif effective_memory is not None and effective_memory < 8:
        enable_parallel = True
        max_workers = max(1, min(cpu_count, 2))
        reason = f"可用内存 {effective_memory:.1f}GB{swap_note}，受限并行 ({max_workers} 线程)"
    elif effective_memory is not None and effective_memory < 16:
        enable_parallel = True
        max_workers = max(1, min(cpu_count, 4))
        reason = f"可用内存 {effective_memory:.1f}GB{swap_note}，标准并行 ({max_workers} 线程)"
    elif cpu_count >= 8:
        enable_parallel = True
        max_workers = max(1, min(cpu_count, 8))
        reason = f"{cpu_count} 核 + {effective_memory:.1f}GB{' 有效内存' if swap_note else ' 内存'}{swap_note}，全速并行 ({max_workers} 线程)"
    elif cpu_count >= 2:
        enable_parallel = True
        max_workers = min(cpu_count, 4)
        reason = f"{cpu_count} 核 CPU，并行 ({max_workers} 线程)"
    else:
        enable_parallel = False
        max_workers = 1
        reason = "单核 CPU，串行运行"

    return {
        "enable_parallel": enable_parallel,
        "max_workers": max_workers,
        "cpu_count": cpu_count,
        "total_memory_gb": total_memory_gb,
        "available_memory_gb": available_memory_gb,
        "total_swap_gb": total_swap_gb,
        "free_swap_gb": free_swap_gb,
        "has_gpu": has_gpu,
        "gpu_description": gpu_description,
        "mode_description": reason,
    }


def resolve_parallel_config() -> Tuple[bool, int]:
    env_parallel = os.environ.get("BACKTEST_ENABLE_PARALLEL", "").strip().lower()
    env_workers = os.environ.get("BACKTEST_MAX_WORKERS", "").strip()

    has_env_override = bool(env_parallel or env_workers)

    if has_env_override:
        if env_parallel == "false":
            enable = False
        elif env_parallel == "true":
            enable = True
        else:
            enable = True

        if env_workers:
            try:
                workers = max(1, int(env_workers))
            except ValueError:
                logger.warning("BACKTEST_MAX_WORKERS=%s 无效，使用自动检测", env_workers)
                workers = None
        else:
            workers = None

        if enable and workers is None:
            config = detect_resource_config()
            workers = config["max_workers"]

        return enable, workers if workers else 1

    config = detect_resource_config()
    return config["enable_parallel"], config["max_workers"]


# ═══════════════════════════════════════════════════════════════════════════════
# 数据加载（Tushare PostgreSQL 本地数据库）
# ═══════════════════════════════════════════════════════════════════════════════

def _convert_columns_to_float32(df: pd.DataFrame) -> pd.DataFrame:
    float_columns = [
        "open", "high", "low", "close",
        "volume", "amount", "pct_chg", "turn", "pe_ttm", "pb_mrq",
    ]
    converted = df.copy()
    for col in float_columns:
        if col in converted.columns:
            converted[col] = converted[col].astype("float32")
    return converted


def load_data(start: str, end: str) -> pd.DataFrame:
    t0 = time.time()
    logger.info(f"加载数据 {start} ~ {end} (Tushare PostgreSQL)...")

    sql = """
        SELECT
            d.ts_code,
            s.name,
            d.trade_date AS date,
            d.open,
            d.high,
            d.low,
            d.close,
            (d.vol * 100)::DOUBLE PRECISION AS volume,
            (d.amount * 1000)::DOUBLE PRECISION AS amount,
            d.pct_chg,
            b.turnover_rate AS turn,
            b.pe_ttm,
            b.pb AS pb_mrq
        FROM tushare_daily d
        JOIN tushare_stock_basic s ON d.ts_code = s.ts_code
        LEFT JOIN tushare_daily_basic b ON d.ts_code = b.ts_code AND d.trade_date = b.trade_date
        WHERE d.trade_date BETWEEN %s AND %s
          AND s.list_status = 'L'
          AND s.exchange IN ('SSE', 'SZSE')
          AND s.market IN ('主板', '中小板', '创业板')
        ORDER BY d.ts_code, d.trade_date
    """

    engine = _get_pg_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=(start, end), parse_dates=["date"])
    finally:
        engine.dispose()

    df["code"] = df["ts_code"].apply(_from_ts_code)
    df.drop(columns=["ts_code"], inplace=True)

    for col in ["open", "high", "low", "close", "volume", "amount",
                "pct_chg", "turn", "pe_ttm", "pb_mrq"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = _convert_columns_to_float32(df)
    logger.info(f"总计 {len(df):,} 行 x {df['code'].nunique()} 只股票，耗时 {time.time()-t0:.1f}s")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 复权因子（Tushare 本地数据库 tushare_adj_factor）
# ═══════════════════════════════════════════════════════════════════════════════

def load_adj_factors_from_db(start: str, end: str) -> pd.DataFrame:
    t0 = time.time()
    logger.info(f"从本地数据库加载复权因子 {start} ~ {end}...")

    sql = """
        SELECT ts_code, trade_date AS "dividOperateDate", adj_factor AS "foreAdjustFactor"
        FROM tushare_adj_factor
        WHERE trade_date BETWEEN %s AND %s
        ORDER BY ts_code, trade_date
    """

    engine = _get_pg_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=(start, end), parse_dates=["dividOperateDate"])
    finally:
        engine.dispose()

    if df.empty:
        return pd.DataFrame(columns=["code", "dividOperateDate", "foreAdjustFactor"])

    df["code"] = df["ts_code"].apply(_from_ts_code)
    df.drop(columns=["ts_code"], inplace=True)
    df["foreAdjustFactor"] = pd.to_numeric(df["foreAdjustFactor"], errors="coerce").fillna(1.0)
    df = df[["code", "dividOperateDate", "foreAdjustFactor"]]

    logger.info(f"复权因子加载完成，共 {len(df):,} 条记录，耗时 {time.time()-t0:.1f}s")
    return df


def apply_forward_adjustment(df: pd.DataFrame, df_factor: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")

    if not df_factor.empty:
        factor_subset = df_factor[["code", "dividOperateDate", "foreAdjustFactor"]].copy()
        factor_subset["dividOperateDate"] = pd.to_datetime(
            factor_subset["dividOperateDate"]
        ).astype("datetime64[ns]")
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

    g = df.groupby("code", sort=False)
    df["pre_close"] = g["close"].shift(1)
    df["pct_chg"] = (df["close"] / df["pre_close"] - 1) * 100

    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 指标计算（每次全量重新计算）
# ═══════════════════════════════════════════════════════════════════════════════

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    t0 = time.time()
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    g = df.groupby("code", group_keys=False)

    for w in [5, 10, 20, 60, 90, 120]:
        df[f"ma{w}"] = g["close"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    for w in [5, 10, 20]:
        df[f"vol_ma{w}"] = g["volume"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    df["vol_std_20d"] = g["volume"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["vol_threshold"] = df["vol_ma20"] + 2 * df["vol_std_20d"]
    df["high_20d_max"] = g["high"].transform(lambda x: x.shift(1).rolling(20, min_periods=1).max())
    df["vol_60d_min"] = g["volume"].transform(lambda x: x.rolling(60, min_periods=20).min())
    df["vol_10d_mean"] = g["volume"].transform(lambda x: x.rolling(10, min_periods=1).mean())
    df["close_4d_ago"] = g["close"].shift(4)

    for w in [6, 12, 24]:
        delta = g["close"].diff()
        gain = delta.where(delta > 0, 0).transform(lambda x: x.rolling(w, min_periods=1).mean())
        loss = (-delta.where(delta < 0, 0)).transform(lambda x: x.rolling(w, min_periods=1).mean())
        df[f"rsi{w}"] = 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))

    ema12 = g["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_dif"] = ema12 - ema26
    df["macd_dea"] = g["macd_dif"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    df["macd_hist"] = 2 * (df["macd_dif"] - df["macd_dea"])

    ema14 = g["close"].transform(lambda x: x.ewm(span=14, adjust=False).mean())
    ema53 = g["close"].transform(lambda x: x.ewm(span=53, adjust=False).mean())
    df["macd_dif2"] = ema14 - ema53
    df["macd_dea2"] = g["macd_dif2"].transform(lambda x: x.ewm(span=5, adjust=False).mean())
    df["macd_hist2"] = 2 * (df["macd_dif2"] - df["macd_dea2"])

    df["boll_mid"] = g["close"].transform(lambda x: x.rolling(20, min_periods=1).mean())
    std20 = g["close"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["boll_upper"] = df["boll_mid"] + 2 * std20
    df["boll_lower"] = df["boll_mid"] - 2 * std20

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


# ═══════════════════════════════════════════════════════════════════════════════
# 策略信号函数（23 个策略）
# ═══════════════════════════════════════════════════════════════════════════════

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
    no_lim = ~(df.groupby("code")["pct_chg"].transform(
        lambda x: (x >= 9.5).rolling(20, min_periods=1).max().shift(1).fillna(0).astype(bool)
    ))
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


# ═══════════════════════════════════════════════════════════════════════════════
# 绩效计算（Numba 加速）
# ═══════════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════════
# 内存监控辅助
# ═══════════════════════════════════════════════════════════════════════════════

def log_memory_usage(stage: str):
    try:
        import psutil
        process = psutil.Process()
        mem_info = process.memory_info()
        logger.info(f"[{stage}] 内存使用: {mem_info.rss / 1024 / 1024:.1f} MB")
    except ImportError:
        logger.debug("psutil not installed, skipping memory usage logging")


# ═══════════════════════════════════════════════════════════════════════════════
# 回测（自动串行/并行）
# ═══════════════════════════════════════════════════════════════════════════════

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


def run_backtests(df_bt: pd.DataFrame) -> List[Dict]:
    t0 = time.time()
    n_strategies = len(STRATEGIES)

    enable_parallel, max_workers = resolve_parallel_config()
    n_workers = min(max_workers, n_strategies)

    if enable_parallel and n_workers > 1:
        logger.info(f"并行回测 {n_strategies} 策略（{n_workers} 线程）...")
    else:
        logger.info(f"串行回测 {n_strategies} 策略...")

    strategy_names = list(STRATEGIES.keys())
    results = []

    if enable_parallel and n_workers > 1:
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
                log_memory_usage(f"策略 {i+1} 完成后")
    else:
        for i, name in enumerate(strategy_names):
            r = _backtest_single(name, df_bt)
            results.append(r)
            if "error" not in r:
                logger.info(f"  [{i+1}/{n_strategies}] {r['strategy']}: {r['total_trades']} 笔, "
                            f"胜率{r['win_rate']:.1f}%, 总收益{r['total_return']:.1f}%")
            else:
                logger.error(f"  [{i+1}/{n_strategies}] {r['strategy']}: {r['error']}")
            log_memory_usage(f"策略 {i+1} 完成后")

    valid = sorted([r for r in results if "error" not in r],
                   key=lambda x: x.get("total_return", 0), reverse=True)
    logger.info(f"回测完成，耗时 {time.time()-t0:.1f}s")
    return valid


# ═══════════════════════════════════════════════════════════════════════════════
# 最近5日验证
# ═══════════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════════
# 输出
# ═══════════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════════
# 胜率前10股票 & 推荐
# ═══════════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════════
# 执行主程序进行大盘复盘和个股决策
# ═══════════════════════════════════════════════════════════════════════════════

def run_main_program_for_stocks(stocks: List[Dict]):
    if not stocks:
        logger.warning("没有股票需要分析")
        return False

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

    main_script = BASE_DIR / "main.py"
    cmd = [
        sys.executable,
        str(main_script),
        "--stocks", stock_code_str, "--force-run"
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


# ═══════════════════════════════════════════════════════════════════════════════
# 交易日判断（本地 tushare_trade_cal 表）
# ═══════════════════════════════════════════════════════════════════════════════

def is_trading_day(date):
    if isinstance(date, datetime):
        date_str = date.strftime("%Y-%m-%d")
    else:
        date_str = str(date)

    try:
        engine = _get_pg_engine()
    except RuntimeError as e:
        logger.warning(f"数据库连接失败: {e}")
        return True

    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                "SELECT is_open FROM tushare_trade_cal WHERE exchange = 'SSE' AND cal_date = %s",
                conn,
                params=(date_str,),
            )
        if df.empty:
            logger.info(f"未找到交易日数据: {date_str}，假定为交易日")
            return True
        return int(df.iloc[0]["is_open"]) == 1
    except Exception as e:
        logger.warning(f"查询交易日失败: {e}，假定为交易日")
        return True
    finally:
        engine.dispose()


# ═══════════════════════════════════════════════════════════════════════════════
# 主函数
# ═══════════════════════════════════════════════════════════════════════════════

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="强制运行（非交易日也执行）")
    args = parser.parse_args(argv)

    if not args.force and not is_trading_day(datetime.now()):
        logger.error("非交易日，程序退出（使用 --force 可强制运行）")
        return

    five_years_ago = (datetime.now() - pd.DateOffset(years=5)).strftime("%Y-%m-%d")
    today_str = datetime.now().strftime("%Y-%m-%d")
    anchored_start = f"{five_years_ago[:4]}-01-01"

    enable_parallel, max_workers = resolve_parallel_config()
    config = detect_resource_config()

    logger.info("=" * 60)
    logger.info("23 策略 5 年回测 + 最近5日验证")
    logger.info(f"Numba加速: {'启用' if _HAS_NUMBA else '未安装（pip install numba）'}")
    logger.info(f"CPU核心: {config['cpu_count']}")
    if config["total_memory_gb"] is not None:
        logger.info(f"总内存: {config['total_memory_gb']:.1f} GB")
    if config["available_memory_gb"] is not None:
        logger.info(f"可用内存: {config['available_memory_gb']:.1f} GB")
    if config["total_swap_gb"] is not None and config["total_swap_gb"] > 0:
        logger.info(f"Swap 总量: {config['total_swap_gb']:.1f} GB  空闲: {config['free_swap_gb']:.1f} GB")
    if config["has_gpu"]:
        logger.info(f"GPU: {config['gpu_description']}")
    logger.info(f"回测模式: {'并行 ' + str(max_workers) + ' 线程' if enable_parallel and max_workers > 1 else '串行'}")
    logger.info(f"模式说明: {config['mode_description']}")
    logger.info(f"回测起始: {anchored_start} ~ {today_str}")
    logger.info("=" * 60)

    log_memory_usage("开始")

    df_market = load_data(anchored_start, today_str)

    logger.info("从数据库加载复权因子并计算前复权价格...")
    df_factor = load_adj_factors_from_db(anchored_start, today_str)
    logger.info(f"获取到 {len(df_factor)} 条复权因子记录")
    df_adjusted = apply_forward_adjustment(df_market, df_factor)
    del df_market, df_factor
    gc.collect()
    log_memory_usage("复权计算后")

    df_all = compute_indicators(df_adjusted)
    del df_adjusted
    gc.collect()
    log_memory_usage("指标计算后")

    all_dates = sorted(df_all["date"].unique())
    if len(all_dates) < 6:
        logger.error(f"数据不足，最近交易日数量: {len(all_dates)}，需要至少6个")
        return

    backtest_end_date = all_dates[-6]
    backtest_start_date = all_dates[0]
    validate_start_date = all_dates[-5]
    validate_end_date = all_dates[-1]

    logger.info(f"回测区间: {backtest_start_date.date()} ~ {backtest_end_date.date()}")
    logger.info(f"验证区间: {validate_start_date.date()} ~ {validate_end_date.date()}")

    df_week = df_all[df_all["date"] >= pd.Timestamp(validate_start_date)].copy()
    log_memory_usage("验证数据拆分后")
    mask_bt = df_all["date"] <= pd.Timestamp(backtest_end_date)
    df_bt = df_all.loc[mask_bt].reset_index(drop=True)
    del df_all
    gc.collect()
    log_memory_usage("回测数据准备后")

    results = run_backtests(df_bt)
    val_results = validate_week(df_week, results, TOP_N_VALIDATE)
    print_results(results, val_results, backtest_start_date, backtest_end_date)

    top_stocks = get_top_stocks_by_win_rate(df_week, results, top_n=10)
    logger.info(f"5日验证胜率前10股票: {len(top_stocks)} 只")

    for idx, s in enumerate(top_stocks, 1):
        ret_str = f"{s['sell_return'] * 100:.2f}%" if s['sell_return'] is not None and pd.notna(s['sell_return']) else "N/A"
        logger.info(f"  [{idx}] {s['code']} {s['name']} - 胜率{s['win_rate']:.1f}% - 策略数{s['strategy_count']} - 收益{ret_str}")

    if top_stocks:
        logger.info("\n" + "=" * 60)
        logger.info("开始执行主程序进行大盘复盘和个股决策")
        logger.info("=" * 60)
        run_main_program_for_stocks(top_stocks)
    else:
        logger.warning("没有找到符合条件的股票，跳过主程序执行")

    logger.info("完成")


if __name__ == "__main__":
    main()
