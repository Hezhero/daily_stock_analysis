#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
23 策略 5 年回测 + 本周验证
回测区间: 2021-01-01 ~ 最近第6个交易日
验证区间: 最近第5个交易日 ~ 最近第1个交易日

整体流程（pipeline）:
  1. 从本地 Tushare PostgreSQL 加载日线行情 + 每日基本面指标
  2. 加载复权因子，计算前复权价格与涨跌幅
  3. 全量计算技术指标（均线/RSI/MACD/BOLL/量能等）
  4. 对 23 个策略逐个生成信号，统计各持有期收益并计算绩效指标
  5. 用最近 5 个交易日对回测结果 Top-5 策略做本周验证
  6. 汇总 5 日验证胜率前 10 股票，自动调用主程序做大盘复盘与个股决策

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
- 回测完成后自动执行主程序进行全市场复盘和个股决策
- 数据质量过滤: SQL 层剔除日成交额 < 500 万的样本（MIN_DAILY_AMOUNT_K）
  与上市不足 120 天的次新股（MIN_LISTING_DAYS），减少失真样本
- 可成交性过滤: 涨停封板日按收盘价无法买入，统一剔除涨停信号
  （_is_limit_up，主板阈值 9.5% / 创业板 19.5%），回测与 5 日验证/推荐共用
- 收益统计口径: 各持有期（1/3/5/10 日）独立统计，避免多期收益混合
  （P0-4）；最大回撤按日期排序后计算，恢复时间序列语义（P0-2）
- 基准对比: 新增等权市场基准（compute_benchmark_metrics），结果表打印
  超额收益列与基准行，用于区分 alpha/beta（P1-8）
"""

import argparse
import logging
import os
import sys
import time
import gc
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dotenv import load_dotenv
from typing_extensions import deprecated

load_dotenv()

from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

# ─── 配置 ──────────────────────────────────────────────────────────────────────
INITIAL_CAPITAL = 1000000.0   # 初始资金（元），用于回测收益口径展示
TOP_N_VALIDATE = 5            # 本周验证取回测收益前 N 个策略
HOLDING_PERIODS = [1, 3, 5, 10]  # 回测持有期（交易日），每个信号分别统计各持有期收益
VALIDATE_DAYS = 5             # 本周验证的交易日窗口长度

# ─── 数据质量与可成交性过滤 ────────────────────────────────────────────────────
MIN_DAILY_AMOUNT_K = 5000        # 最小日成交额（单位：千元，即 500 万元），过滤流动性不足的样本
MIN_LISTING_DAYS = 120           # 上市最短天数，过滤次新股（上市初期涨跌结构特殊）
LIMIT_UP_PCT_MAIN = 9.5          # 主板/中小板涨停判定阈值（%）
LIMIT_UP_PCT_GEM = 19.5          # 创业板涨停判定阈值（%），2020-08 起涨跌幅扩至 20%

# ─── 交易成本 ──────────────────────────────────────────────────────────────────
TRADING_COST_PCT = 0.15          # 单边往返交易成本（%）：佣金+印花税+滑点合计约 0.15%

# ─── 信号质量过滤（P1） ────────────────────────────────────────────────────────
MIN_CIRC_MV_W = 200000           # 最小流通市值（万元，即 20 亿），过滤易被操纵的小盘股
MAX_CIRC_MV_W = 5000000          # 最大流通市值（万元，即 500 亿），过滤弹性差的超大盘股
MIN_VOLUME_RATIO = 1.5           # 信号日最低量比（Tushare 官方归一化活跃度指标）
MONEYFLOW_LOOKBACK = 3           # 主力资金净流入确认的回看天数

# ─── 信号去重/冷却期（C3） ─────────────────────────────────────────────────────
SIGNAL_COOLDOWN_DAYS = 5         # 同一股票同一策略 N 日内只取第一次信号

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
    """将 Tushare 代码格式（如 '600519.SH'）转换为内部代码格式（如 'sh.600519'）。

    Tushare 的 ts_code 形如 "数字.交易所后缀"（SH/SZ），
    本项目内部统一使用 "交易所小写.数字"（sh./sz.）的格式。
    """
    num, suffix = ts_code.split(".")
    return f"{suffix.lower()}.{num}"


def _get_pg_engine():
    """根据环境变量创建 PostgreSQL 连接引擎（Tushare 本地数据库）。

    依赖 PG_HOST / PG_PORT / PG_DBNAME / PG_USER / PG_PASSWORD 环境变量；
    PG_PASSWORD 未设置时直接抛错，避免静默失败。
    """
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
    """检测物理内存总量（GB）。

    优先使用 psutil；psutil 不可用时在 Windows 上回退到 ctypes 调用
    GlobalMemoryStatusEx；均失败返回 None。
    """
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
    """检测当前可用物理内存（GB），仅支持 psutil，失败返回 None。"""
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
    """解析并行配置，返回 (是否并行, 线程数)。

    优先级：
      1. 环境变量 BACKTEST_ENABLE_PARALLEL / BACKTEST_MAX_WORKERS 显式覆盖
         （BACKTEST_ENABLE_PARALLEL=false 强制串行；true 或仅设置线程数时开启）
      2. 未设置环境变量时，依据 CPU 核数与可用内存自动检测（detect_resource_config）
    """
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
    """将行情/基本面数值列统一降为 float32，节省约一半内存。

    注意：只转换数值列，code/name/date 等标识列保持原类型。
    """
    float_columns = [
        "open", "high", "low", "close",
        "volume", "amount", "pct_chg", "turn", "pe_ttm", "pb_mrq",
        "up_limit", "down_limit", "circ_mv", "volume_ratio", "net_mf_amount",
    ]
    converted = df.copy()
    for col in float_columns:
        if col in converted.columns:
            converted[col] = converted[col].astype("float32")
    return converted


def load_data(start: str, end: str) -> pd.DataFrame:
    """从本地 Tushare PostgreSQL 加载区间内的日线行情与每日基本面数据。

    数据来源：
      - tushare_daily        ：日线行情（开高低收、量额、涨跌幅）
      - tushare_stock_basic  ：股票名称、上市状态、交易所、市场板块、上市日期
      - tushare_daily_basic  ：换手率、PE(TTM)、PB

    过滤条件：
      - 仅沪深主板/中小板/创业板、上市状态为 L（正常上市）的股票
      - 日成交额不低于 MIN_DAILY_AMOUNT_K（流动性过滤，P2-11）
      - 上市满 MIN_LISTING_DAYS 天（次新股过滤，P2-12）
    返回字段含 code（内部格式）、name、date 及各行情/基本面数值列。
    """
    t0 = time.time()
    logger.info(f"加载数据 {start} ~ {end} (Tushare PostgreSQL)...")

    # 注意单位换算：vol 以"手"为单位 ×100 转股数；amount 以"千元"为单位 ×1000 转元
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
            b.pb AS pb_mrq,
            b.circ_mv,
            b.volume_ratio,
            m.net_mf_amount,
            l.up_limit,
            l.down_limit
        FROM tushare_daily d
        JOIN tushare_stock_basic s ON d.ts_code = s.ts_code
        LEFT JOIN tushare_daily_basic b ON d.ts_code = b.ts_code AND d.trade_date = b.trade_date
        LEFT JOIN tushare_moneyflow m ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
        LEFT JOIN tushare_stk_limit l ON d.ts_code = l.ts_code AND d.trade_date = l.trade_date
        WHERE d.trade_date BETWEEN %s AND %s
          AND s.list_status = 'L'
          AND s.exchange IN ('SSE', 'SZSE')
          AND s.market IN ('主板', '中小板', '创业板')
          AND d.amount >= %s
          AND d.trade_date >= (s.list_date + %s)
        ORDER BY d.ts_code, d.trade_date
    """

    engine = _get_pg_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=(start, end, MIN_DAILY_AMOUNT_K, MIN_LISTING_DAYS), parse_dates=["date"])
    finally:
        engine.dispose()

    df["code"] = df["ts_code"].apply(_from_ts_code)
    df.drop(columns=["ts_code"], inplace=True)

    for col in ["open", "high", "low", "close", "volume", "amount",
                "pct_chg", "turn", "pe_ttm", "pb_mrq", "up_limit", "down_limit",
                "circ_mv", "volume_ratio", "net_mf_amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = _convert_columns_to_float32(df)
    logger.info(f"总计 {len(df):,} 行 x {df['code'].nunique()} 只股票，耗时 {time.time()-t0:.1f}s")
    return df


# ─── 市场环境过滤(上证指数 regime) ─────────────────────────────────────────────

def load_index_daily(start: str, end: str, ts_code: str = "000001.SH") -> pd.DataFrame:
    """从本地 tushare_index_daily 加载指数日线数据。

    默认使用上证指数(000001.SH)作为 A 股市场环境判定基准。
    返回列: date、index_close(指数收盘价)。
    """
    t0 = time.time()
    logger.info(f"加载指数 {ts_code} 日线 {start} ~ {end}...")

    sql = """
        SELECT trade_date AS date, close AS index_close
        FROM tushare_index_daily
        WHERE ts_code = %s AND trade_date BETWEEN %s AND %s
        ORDER BY trade_date
    """

    engine = _get_pg_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=(ts_code, start, end), parse_dates=["date"])
    finally:
        engine.dispose()

    df["index_close"] = pd.to_numeric(df["index_close"], errors="coerce").astype("float32")
    logger.info(f"指数加载完成,共 {len(df)} 条,耗时 {time.time()-t0:.1f}s")
    return df


def compute_market_ok(index_df: pd.DataFrame) -> pd.DataFrame:
    """根据指数日线计算每日 market_ok(适合开仓的市场环境)。

    判定规则(组合应用):
      - 指数收盘价站在 MA20 和 MA60 之上
      - MA20 上行(今 > 昨)
      - MA5 在 MA20 之上(未死叉)
    返回 DataFrame,含 date、market_ok(bool)两列。
    """
    df = index_df.sort_values("date").reset_index(drop=True).copy()
    close = df["index_close"]
    df["idx_ma5"] = close.rolling(5, min_periods=1).mean()
    df["idx_ma20"] = close.rolling(20, min_periods=1).mean()
    df["idx_ma60"] = close.rolling(60, min_periods=1).mean()
    df["idx_ma20_prev"] = df["idx_ma20"].shift(1).fillna(df["idx_ma20"].iloc[0])

    df["market_ok"] = (
        (close > df["idx_ma20"])
        & (close > df["idx_ma60"])
        & (df["idx_ma20"] > df["idx_ma20_prev"])
        & (df["idx_ma5"] > df["idx_ma20"])
    )
    return df[["date", "market_ok"]]


# ─── 财务质量过滤(ann_date 对齐防前视偏差,B3) ─────────────────────────────────

def load_fina_indicator(start: str, end: str) -> pd.DataFrame:
    """从本地 tushare_fina_indicator 加载财务指标。

    返回列: code(内部格式)、ann_date(公告日)、roe、grossprofit_margin、or_yoy。
    注意:必须以 ann_date 对齐交易日,否则会偷看未来信息(前视偏差)。
    """
    t0 = time.time()
    logger.info(f"加载财务指标 {start} ~ {end}...")

    sql = """
        SELECT ts_code, ann_date, roe, grossprofit_margin, or_yoy
        FROM tushare_fina_indicator
        WHERE ann_date BETWEEN %s AND %s
        ORDER BY ts_code, ann_date
    """

    engine = _get_pg_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=(start, end), parse_dates=["ann_date"])
    finally:
        engine.dispose()

    if df.empty:
        return pd.DataFrame(columns=["code", "ann_date", "roe", "grossprofit_margin", "or_yoy"])

    df["code"] = df["ts_code"].apply(_from_ts_code)
    df.drop(columns=["ts_code"], inplace=True)
    for col in ["roe", "grossprofit_margin", "or_yoy"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[["code", "ann_date", "roe", "grossprofit_margin", "or_yoy"]]
    logger.info(f"财务指标加载完成,共 {len(df):,} 条,耗时 {time.time()-t0:.1f}s")
    return df


def merge_fina_by_ann_date(df: pd.DataFrame, fina_df: pd.DataFrame) -> pd.DataFrame:
    """按公告日(ann_date)向后对齐财务数据到行情日期,防止前视偏差。

    使用 merge_asof(direction='backward'):每个交易日只使用已公告
    (ann_date <= trade_date)的最新一期财务数据。
    按 code 分组逐组对齐,规避 merge_asof 对 left 全局排序的要求。
    """
    if fina_df is None or fina_df.empty or "ann_date" not in fina_df.columns:
        return df
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    fina = fina_df.sort_values(["code", "ann_date"]).reset_index(drop=True)
    fina_by_code = {code: g for code, g in fina.groupby("code", sort=False)}
    pieces = []
    for code, g in df.groupby("code", sort=False):
        f = fina_by_code.get(code)
        if f is None or f.empty:
            g = g.copy()
            g["ann_date"] = pd.NaT
            pieces.append(g)
            continue
        f = f.drop(columns=["code"])
        merged = pd.merge_asof(
            g, f,
            left_on="date", right_on="ann_date", direction="backward",
        )
        pieces.append(merged)
    out = pd.concat(pieces, ignore_index=True)
    out.drop(columns=["ann_date"], inplace=True, errors="ignore")
    return out


def _financial_ok(df: pd.DataFrame) -> pd.Series:
    """财务质量过滤:剔除 ROE 为负或营收大幅下滑(or_yoy < -30%)的信号。

    财务数据缺失(无公告)时放行,避免误杀无覆盖样本。
    """
    if "fin_roe" not in df.columns:
        return pd.Series(True, index=df.index)
    roe_ok = df["fin_roe"].isna() | (df["fin_roe"] >= 0)
    rev_ok = df["fin_or_yoy"].isna() | (df["fin_or_yoy"] >= -30)
    return roe_ok & rev_ok


# ═══════════════════════════════════════════════════════════════════════════════
# 复权因子（Tushare 本地数据库 tushare_adj_factor）
# ═══════════════════════════════════════════════════════════════════════════════

def load_adj_factors_from_db(start: str, end: str) -> pd.DataFrame:
    """从本地 tushare_adj_factor 表加载区间内的复权因子。

    返回列：code（内部格式）、dividOperateDate（除权除息日）、foreAdjustFactor（前复权因子）。
    字段名沿用 Tushare 原始命名，便于后续 apply_forward_adjustment 使用。
    """
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
    """对行情数据应用前复权，并重算涨跌幅、前收盘价。

    复权逻辑：
      - 用 merge_asof（direction="backward"）按 code 匹配每个交易日最近一次
        除权除息日的前复权因子（因子按日期向后滚动填充）
      - 开/高/低/收四价乘以复权因子，得到前复权价格
      - 复权后按股票分组重算 pre_close 与 pct_chg（收盘价环比涨跌幅）

    注意：因子缺失时默认取 1.0（即不复权），保证全市场数据不因个别股票
    因子缺失而整行丢弃。
    """
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
    """全量计算回测/验证所需的技术指标（按股票分组、向量化计算）。

    产出指标：
      - 均线 ma{5,10,20,60,90,120} 与量均线 vol_ma{5,10,20}
      - 量能阈值 vol_threshold（ma20 + 2*标准差）、20 日最高价 high_20d_max、
        60 日最低量 vol_60d_min、10 日均量 vol_10d_mean、4 日前收盘 close_4d_ago
      - RSI{6,12,24}（SMA 平滑口径）
      - MACD 两组：标准 (12,26,9) 与快线组 (14,53,5)
      - BOLL（20 日，2 倍标准差）：boll_mid / boll_upper / boll_lower
      - 前瞻收益 ret_{1,3,5,10}d（未来 p 日收盘涨跌幅，供回测取信号后收益用）
        ret_5d_open_to_close（5 日后收盘 / 今开 - 1）
    所有 float64 列最后统一转 float32 节省内存。
    """
    t0 = time.time()
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    g = df.groupby("code", group_keys=False)

    # ── 均线族 ──
    for w in [5, 10, 20, 60, 90, 120]:
        df[f"ma{w}"] = g["close"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    # ── 量能指标 ──
    for w in [5, 10, 20]:
        df[f"vol_ma{w}"] = g["volume"].transform(lambda x: x.rolling(w, min_periods=1).mean())

    # vol_threshold: 放量突破的判定线（20日均量 + 2倍标准差）
    df["vol_std_20d"] = g["volume"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["vol_threshold"] = df["vol_ma20"] + 2 * df["vol_std_20d"]
    # high_20d_max: 不含当日的 20 日最高价（shift(1) 避免前视偏差）
    df["high_20d_max"] = g["high"].transform(lambda x: x.shift(1).rolling(20, min_periods=1).max())
    df["vol_60d_min"] = g["volume"].transform(lambda x: x.rolling(60, min_periods=20).min())
    df["vol_10d_mean"] = g["volume"].transform(lambda x: x.rolling(10, min_periods=1).mean())
    df["close_4d_ago"] = g["close"].shift(4)

    # ── RSI（SMA 平滑口径，loss 用极小值防除零） ──
    for w in [6, 12, 24]:
        delta = g["close"].diff()
        gain = delta.where(delta > 0, 0).transform(lambda x: x.rolling(w, min_periods=1).mean())
        loss = (-delta.where(delta < 0, 0)).transform(lambda x: x.rolling(w, min_periods=1).mean())
        df[f"rsi{w}"] = 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))

    # ── MACD 标准组 (12, 26, 9) ──
    ema12 = g["close"].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = g["close"].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df["macd_dif"] = ema12 - ema26
    df["macd_dea"] = g["macd_dif"].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    df["macd_hist"] = 2 * (df["macd_dif"] - df["macd_dea"])

    # ── MACD 快线组 (14, 53, 5)，对短线波动更敏感 ──
    ema14 = g["close"].transform(lambda x: x.ewm(span=14, adjust=False).mean())
    ema53 = g["close"].transform(lambda x: x.ewm(span=53, adjust=False).mean())
    df["macd_dif2"] = ema14 - ema53
    df["macd_dea2"] = g["macd_dif2"].transform(lambda x: x.ewm(span=5, adjust=False).mean())
    df["macd_hist2"] = 2 * (df["macd_dif2"] - df["macd_dea2"])

    # ── BOLL 布林带（20 日，2 倍标准差） ──
    df["boll_mid"] = g["close"].transform(lambda x: x.rolling(20, min_periods=1).mean())
    std20 = g["close"].transform(lambda x: x.rolling(20, min_periods=1).std())
    df["boll_upper"] = df["boll_mid"] + 2 * std20
    df["boll_lower"] = df["boll_mid"] - 2 * std20

    # ── ATR(20):真实波幅均值,用于动态止损 ──
    prev_close = g["close"].shift(1)
    true_range = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr20"] = true_range.groupby(df["code"]).transform(
        lambda x: x.rolling(20, min_periods=1).mean()
    )

    # ── 前瞻收益（信号产生后各持有期的实际收益，回测绩效的数据来源） ──
    for p in HOLDING_PERIODS:
        df[f"ret_{p}d"] = g["close"].transform(lambda x: x.pct_change(p, fill_method=None).shift(-p))

    df["ret_5d_open_to_close"] = (g["close"].shift(-4) / df["open"] - 1)

    # 数值列统一降精度：float64 -> float32
    indicator_cols = [c for c in df.columns if c not in ("code", "name", "date")]
    for c in indicator_cols:
        if df[c].dtype == "float64":
            df[c] = df[c].astype("float32")

    logger.info(f"指标计算完成，耗时 {time.time()-t0:.1f}s")
    gc.collect()
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 动态退出收益（ATR 止损 + 移动止盈 + 时间止损，C2）
# ═══════════════════════════════════════════════════════════════════════════════

def compute_dynamic_exit_returns(df: pd.DataFrame) -> pd.DataFrame:
    """按股票分组计算各持有期的动态退出收益,替换固定持有期收益。

    对每个持有期 p,入场日 t(收盘买入)在持有期内逐日检查:
      - ATR 止损:某日 low <= close[t] - 2*atr20[t],以止损价退出
      - 移动止盈:某日 close <= 持有期最高价*0.92,以回撤位退出
      - 时间止损:持有 p 日仍未触发,按 close[t+p] 退出
    输出列 dyn_ret_{p}d(与 ret_{p}d 同口径的浮点收益)。
    按 code 分组,组内对每个交易日一次性计算 max(HOLDING_PERIODS)
    天的窗口并复用,避免逐持有期重复扫描。
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
            stop = close[i] - 2.0 * atr[i]
            if not np.isfinite(stop):
                continue
            w = min(max_p, n - 1 - i)
            win_low = low[i + 1: i + 1 + w]
            win_high = high[i + 1: i + 1 + w]
            win_close = close[i + 1: i + 1 + w]
            peaks = np.maximum.accumulate(win_high)
            atr_hit = win_low <= stop
            trail_hit = win_close <= peaks * 0.92
            hit = atr_hit | trail_hit

            for p in HOLDING_PERIODS:
                if p > w:
                    continue
                hit_p = hit[:p]
                if hit_p.any():
                    k = int(np.argmax(hit_p))
                    exit_price = stop if atr_hit[k] else peaks[k] * 0.92
                else:
                    exit_price = close[i + p]
                if close[i] > 0:
                    rets[p][i] = (exit_price / close[i] - 1.0)

        for p, arr in rets.items():
            out.loc[g.index, f"dyn_ret_{p}d"] = arr

    # 对齐到原 df 的行顺序(函数内做了重排)
    return out.reindex(df.index)


# ═══════════════════════════════════════════════════════════════════════════════
# 可成交性判断（涨停封板无法买入）
# ═══════════════════════════════════════════════════════════════════════════════

def _is_limit_up(df: pd.DataFrame) -> pd.Series:
    """判断当日是否涨停封板(涨停时按收盘价无法买入,回测应剔除该类信号)。

    优先使用 tushare_stk_limit.up_limit 精确判定:
      - 收盘价 >= 涨停价 * 0.999 即视为已封板(含 ST/科创板等不同涨跌幅限制)
    当 up_limit 缺失(如停牌、数据缺失)时回退到近似阈值:
      - 主板/中小板 9.5%,创业板 19.5%(留出四舍五入余量)
    返回布尔 Series(True = 涨停,不可成交)。
    """
    if "up_limit" in df.columns:
        exact = df["close"] >= df["up_limit"] * 0.999
        gem = df["code"].str.startswith(("sz.300", "sz.301"))
        approx = df["pct_chg"] >= np.where(gem, LIMIT_UP_PCT_GEM, LIMIT_UP_PCT_MAIN)
        # 精确值优先;缺失时用近似阈值兜底
        return exact.fillna(approx)
    else:
        gem = df["code"].str.startswith(("sz.300", "sz.301"))
        threshold = np.where(gem, LIMIT_UP_PCT_GEM, LIMIT_UP_PCT_MAIN)
        return df["pct_chg"] >= threshold


def _is_limit_down(df: pd.DataFrame) -> pd.Series:
    """判断当日是否跌停(跌停日买入流动性差且为接飞刀,回测应剔除该类信号)。

    优先使用 tushare_stk_limit.down_limit 精确判定:
      - 收盘价 <= 跌停价 * 1.001 即视为已封板
    当 down_limit 缺失时回退到近似阈值(-9.5% / -19.5%)。
    返回布尔 Series(True = 跌停,不可作为买点)。
    """
    if "down_limit" in df.columns:
        exact = df["close"] <= df["down_limit"] * 1.001
        gem = df["code"].str.startswith(("sz.300", "sz.301"))
        approx = df["pct_chg"] <= np.where(gem, -LIMIT_UP_PCT_GEM, -LIMIT_UP_PCT_MAIN)
        return exact.fillna(approx)
    else:
        gem = df["code"].str.startswith(("sz.300", "sz.301"))
        threshold = np.where(gem, -LIMIT_UP_PCT_GEM, -LIMIT_UP_PCT_MAIN)
        return df["pct_chg"] <= threshold


def _moneyflow_ok(df: pd.DataFrame) -> pd.Series:
    """主力资金净流入确认:当日净流入 > 0 或近 N 日累计净流入 > 0。

    过滤"放量但主力出货"的假突破;数据缺失时放行(不误杀)。
    """
    if "net_mf_amount" not in df.columns:
        return pd.Series(True, index=df.index)
    mf = df["net_mf_amount"]
    cum3 = mf.groupby(df["code"]).transform(
        lambda x: x.rolling(MONEYFLOW_LOOKBACK, min_periods=1).sum()
    )
    ok = (mf > 0) | (cum3 > 0)
    return ok.fillna(True)


def _size_ok(df: pd.DataFrame) -> pd.Series:
    """流通市值过滤:剔除 < 20 亿(易操纵、滑点大)与 > 500 亿(弹性差)个股。

    circ_mv 单位为万元;数据缺失时放行。
    """
    if "circ_mv" not in df.columns:
        return pd.Series(True, index=df.index)
    mv = df["circ_mv"]
    ok = (mv >= MIN_CIRC_MV_W) & (mv <= MAX_CIRC_MV_W)
    return ok.fillna(True)


def _volume_ratio_ok(df: pd.DataFrame) -> pd.Series:
    """量比确认:信号日量比 >= MIN_VOLUME_RATIO(相对近期活跃)。

    用 Tushare 官方归一化指标替代/补充自算 vol_ma5 放量判断;缺失时放行。
    """
    if "volume_ratio" not in df.columns:
        return pd.Series(True, index=df.index)
    ok = df["volume_ratio"] >= MIN_VOLUME_RATIO
    return ok.fillna(True)


def _entry_mask(df: pd.DataFrame) -> pd.Series:
    """统一的买入可成交性掩码:剔除涨跌停日,并叠加市场环境、
    主力资金、市值、量比、财务质量过滤(回测/验证/推荐共用)。"""
    mask = ~_is_limit_up(df) & ~_is_limit_down(df)
    if "market_ok" in df.columns:
        mask &= df["market_ok"].fillna(True).astype(bool)
    mask &= _moneyflow_ok(df) & _size_ok(df) & _volume_ratio_ok(df) & _financial_ok(df)
    return mask


def _apply_cooldown(df: pd.DataFrame, sig: pd.Series, cooldown_days: int = SIGNAL_COOLDOWN_DAYS) -> pd.Series:
    """对信号施加冷却期:同一股票同一策略 N 日内只取第一次信号。

    按 code 分组,保留信号后 N 日内的后续信号被抑制,降低样本
    自相关与同一股票重复贡献,使胜率统计更真实(C3)。
    """
    sig = sig & _entry_mask(df)
    kept = pd.Series(False, index=df.index)
    tmp = df.copy()
    tmp["sig"] = sig.astype(bool)
    tmp["kept"] = False
    for code, g in tmp.groupby("code", sort=False):
        sig_days = g.loc[g["sig"], "date"].to_numpy()
        if len(sig_days) == 0:
            continue
        keep_mask = np.zeros(len(sig_days), dtype=bool)
        last_kept = None
        for j, d in enumerate(sig_days):
            if last_kept is None or (d - last_kept) / np.timedelta64(1, "D") >= cooldown_days:
                keep_mask[j] = True
                last_kept = d
        kept_dates = sig_days[keep_mask]
        if len(kept_dates):
            kept.loc[g.index[g["date"].isin(kept_dates)]] = True
    return kept


# ═══════════════════════════════════════════════════════════════════════════════
# 策略信号函数（23 个策略）
# ═══════════════════════════════════════════════════════════════════════════════

def _ma_cross(df):
    """均线金叉组合打分（供 sig_ma_crossover 使用）。

    各条件按权重累加：
      MA5 上穿 MA20（金叉） +3
      放量（量 > 1.5*vol_ma5）      +2
      MACD 红柱（macd_hist>0）      +2
      RSI6 处于 30~70 健康区间      +1.5
      收盘价站上 MA60（中期趋势）   +1.5
      阳线（收 > 开）               +1
    最终分数 >= 6 视为有效信号。
    """
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
    """放量突破组合打分（供 sig_volume_surge_std 使用）。

    量能突破 vol_threshold（20日均量+2倍标准差） +4
    创 20 日新高（收盘 > high_20d_max）          +4
    阳线（收 > 开）                              +2
    RSI6 处于 30~70 健康区间                     +2
    最终分数 >= 6 视为有效信号。
    """
    return ((df["volume"] > df["vol_threshold"]).astype(int) * 4 +
            (df["close"] > df["high_20d_max"]).astype(int) * 4 +
            (df["close"] > df["open"]).astype(int) * 2 +
            ((df["rsi6"] > 30) & (df["rsi6"] < 70)).astype(int) * 2)


def sig_ma_crossover(df):
    """策略1：均线金叉——MA5 上穿 MA20，配合放量、MACD 与趋势确认。"""
    return _ma_cross(df) >= 6


def sig_volume_surge_std(df):
    """策略2：放量突破——成交量突破统计阈值且价格创 20 日新高。"""
    return _vol_surge(df) >= 6


def sig_wonderful_9_turn(df):
    """策略3：神奇九转——连续 9 个交易日收盘低于 4 日前收盘（下跌计数到 9），
    叠加超卖 RSI、MACD 红柱回落、放量与均线多头排列，捕捉阶段性反转买点。

    打分权重：
      九转计数成立（连续9日走弱）             +4
      RSI6 < 35（超卖）                       +3
      MACD 红柱仍在但较前一日收窄（动能蓄势） +3
      放量（量 > 1.2*vol_ma5）                +2
      MA20 上穿 MA60 且 MA20 上行（多头排列） +2
      收盘不低于 MA20*0.98（回踩到位）        +2
      阳线                                   +1
    总分 >= 10 触发信号。
    """
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
    """策略4：N 字形态——价格突破 5 日最高点形成 N 字中继突破。

    打分权重：
      收盘突破前 5 日最高价   +6
      放量（量 > vol_ma5）    +4
      阳线                   +3
      站上 MA20              +2
    总分 >= 13 触发信号。
    """
    sh = df.groupby("code")["high"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).max())
    score = ((df["close"] > sh).astype(int) * 6 +
             (df["volume"] > df["vol_ma5"]).astype(int) * 4 +
             (df["close"] > df["open"]).astype(int) * 3 +
             (df["close"] > df["ma20"]).astype(int) * 2)
    return score >= 13


def sig_limit_up_pullback(df):
    """策略5：涨停回调（缩量回踩）——涨停后缩量回踩不破涨停价 97%，再次放量启涨。

    打分权重：
      前一日涨停（pct_chg >= 9.5%）      +2
      缩量（量 < 涨停日量的一半）        +3
      收盘不低于涨停日最高价的 97%       +3
      再次放量（量 > 1.5*vol_ma5）       +4
      当日上涨（pct_chg > 0）            +2
    总分 >= 8 触发信号。
    """
    pct = df["pct_chg"]
    # hi/vl：以 ffill 方式回溯最近一次涨停日的最高价与成交量，作为回踩基准
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
    """策略6：先稳后涨停——前 10 日振幅收敛（无 ±5% 大波动）后突然涨停放量。

    逻辑：
      - 前 10 日（不含当日）最大涨幅 < 5% 且最大跌幅 > -5%（横盘整理）
      - 当日涨停（pct_chg >= 9.5%）
      - 当日成交量 > 10 日均量 * 1.5（放量启动）
    """
    mx = df.groupby("code")["pct_chg"].transform(lambda x: x.shift(1).rolling(10, min_periods=1).max())
    mn = df.groupby("code")["pct_chg"].transform(lambda x: x.shift(1).rolling(10, min_periods=1).min())
    stable = (mx < 5) & (mn > -5)
    return stable & (df["pct_chg"] >= 9.5) & (df["volume"] > df["vol_10d_mean"] * 1.5)


def sig_monthly_macd_20ma(df):
    """策略7：月线 MACD + 20 日均线——MACD 金叉叠加 MA20 上行，中期趋势启动。

    打分权重：
      MACD 金叉（DIF 上穿 DEA）        +5
      MA20 上行（今 MA20 > 昨 MA20）   +4
      收盘不低于 MA20*0.97（回踩不破） +3
      放量（量 > 1.5*vol_ma5）         +4
      阳线                            +2
      RSI6 处于 40~70 区间             +2
    总分 >= 10 触发信号。
    """
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
    """策略8：低位涨停——股价处于 20 日低位区域的首个涨停，要求换手与价格门槛。

    逻辑：
      - 当日涨停（pct_chg >= 9.5%）
      - 收盘价低于 20 日最高价的 90%（处于低位）
      - 换手率 >= 5%（有资金参与）
      - 股价 < 50 元（低价股偏好）
      - 前 20 日内无涨停（no_lim，排除连板/高位接力）
    """
    pct = df["pct_chg"]
    h20 = df["high_20d_max"]
    no_lim = ~(df.groupby("code")["pct_chg"].transform(
        lambda x: (x >= 9.5).rolling(20, min_periods=1).max().shift(1).fillna(0).astype(bool)
    ))
    return (pct >= 9.5) & (df["close"] < h20 * 0.9) & (df["turn"] >= 5) & (df["close"] < 50) & no_lim

@deprecated("该函数已废弃，胜率太低")
def sig_limit_up_resonance(df):
    """策略9：涨停共振——前日涨停后次日继续走强（趋势延续）。

    逻辑（全部满足）：
      - 前一日涨停（pct_chg >= 9.5%）
      - MA20 上行（ma20 > 昨 ma20）
      - 收盘不低于 MA20*0.97（强势整理）
      - 放量（量 > 1.5*vol_ma5）
      - 当日上涨（pct_chg > 0）
      - RSI6 较前一日走高（动能延续）
    """
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


@deprecated("该函数已废弃，胜率太低")
def sig_bullish_engulfing(df):
    """策略10：阳包阴（看涨吞没）——前一日大幅下跌后当日高开收阳吞没阴线。

    逻辑（全部满足）：
      - 前一日大跌（pct_chg <= -7%）且阴线实体占比 >= 70%（恐慌性抛售）
      - 当日高开（open >= 前收 * 1.02）
      - 当日阳线（close > open）
    """
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
    """策略11：多均线共振——长中短均线多头排列 + MA5/10 金叉，多指标共振确认。

    打分权重：
      多头排列（MA5>MA10>MA20>MA60>MA90>MA120 且 MA5 上行）  +4
      MA5 上穿 MA10（金叉）                                 +4
      放量（量 > 1.5*vol_ma20）                            +3
      MACD 金叉（DIF 上穿 DEA）                            +3
      MACD 红柱由负转正                                    +2
      RSI6/12/24 同步上行且 RSI6 < 70                      +3
      布林带开口扩大（上轨-中轨间距增大）                  +2
    总分 >= 10 触发信号。
    """
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


# 组合策略的组件与权重缓存:权重由 run_backtests 按各组件历史胜率填充
ENSEMBLE_COMPONENTS = ["ma_crossover", "volume_surge_std", "multi_ma_resonance"]
ENSEMBLE_MIN_WEIGHTED_SCORE = 0.5   # 加权分触发阈值(权重和为 1)
_ENSEMBLE_WEIGHTS: Dict[str, float] = {}


def sig_ensemble(df):
    """策略12：组合策略——按各组件策略历史胜率加权,加权分 >= 阈值触发。

    替代简单"≥2 个组件同时触发":权重由 run_backtests 依据组件策略
    5 年回测胜率归一化后填充(_ENSEMBLE_WEIGHTS);未填充时退化为等权。
    """
    weights = np.array([_ENSEMBLE_WEIGHTS.get(c, 1.0) for c in ENSEMBLE_COMPONENTS], dtype=float)
    w_sum = weights.sum()
    if w_sum > 0:
        weights = weights / w_sum
    hits = np.column_stack([
        sig_ma_crossover(df).astype(float),
        sig_volume_surge_std(df).astype(float),
        sig_multi_ma_resonance(df).astype(float),
    ])
    score = hits @ weights
    return score >= ENSEMBLE_MIN_WEIGHTED_SCORE


def sig_volume_breakout(df):
    """策略13：放量突破——成交量放大 2 倍以上并站上均线，量价齐升。

    打分权重：
      量 > 2*vol_ma5（显著放量）  +5
      收盘站上 MA20             +4
      阳线                     +2
      收盘站上 MA60             +2
    总分 >= 8 触发信号。
    """
    score = ((df["volume"] > df["vol_ma5"] * 2).astype(int) * 5 +
             (df["close"] > df["ma20"]).astype(int) * 4 +
             (df["close"] > df["open"]).astype(int) * 2 +
             (df["close"] > df["ma60"]).astype(int) * 2)
    return score >= 8


def sig_bull_trend(df):
    """策略14：多头趋势——短均线多头排列 + RSI 健康 + 放量，趋势跟随型信号。

    逻辑（全部满足）：收盘 > MA5 > MA10 > MA20（多头排列）、
    RSI6 处于 40~70、成交量 > vol_ma5。
    """
    c = df["close"]
    ma5 = df["ma5"]
    ma10 = df["ma10"]
    ma20 = df["ma20"]
    return (c > ma5) & (ma5 > ma10) & (ma10 > ma20) & (df["rsi6"] > 40) & (df["rsi6"] < 70) & (df["volume"] > df["vol_ma5"])


def sig_ma_golden_cross(df):
    """策略15：均线金叉（MA5/MA10）——短线金叉 + 放量 + 站稳 MA10 的买入信号。

    逻辑（全部满足）：
      - MA5 上穿 MA10（金叉）
      - 放量（量 > 1.2*vol_ma5）
      - 收盘价站上 MA10
    """
    m5p = df.groupby("code")["ma5"].shift(1)
    m10p = df.groupby("code")["ma10"].shift(1)
    cross = (df["ma5"] > df["ma10"]) & (m5p <= m10p)
    return cross & (df["volume"] > df["vol_ma5"] * 1.2) & (df["close"] > df["ma10"])


def sig_shrink_pullback(df):
    """策略16：缩量回调——极度缩量（量 < 30% vol_ma5）且价格贴近 MA10 的回踩企稳信号。

    逻辑（全部满足）：
      - 量 < 0.3*vol_ma5（地量）
      - 收盘价距 MA10 偏离 < 3%（回踩到位）
      - 当日上涨（pct_chg > 0）
    """
    s1 = df["volume"] < df["vol_ma5"] * 0.3
    s2 = (df["close"] - df["ma10"]).abs() / df["ma10"] < 0.03
    s3 = df["pct_chg"] > 0
    return s1 & s2 & s3


def sig_dragon_head(df):
    """策略17：龙头战法——前日涨停后次日大幅高开（>2%），追强势龙头股。

    逻辑（全部满足）：
      - 前一日涨停（pct_chg >= 9.5%）
      - 当日开盘价 > 前收 * 1.02（高开 2% 以上）
    """
    pp = df.groupby("code")["pct_chg"].shift(1)
    pc = df.groupby("code")["close"].shift(1)
    return (pp >= 9.5) & (df["open"] > pc * 1.02)


def sig_emotion_cycle(df):
    """策略18：情绪周期——超卖后情绪反转（RSI 低位 + 阳线 + 放量）。

    逻辑（全部满足）：RSI6 < 35（超卖）、阳线（close > open）、
    放量（量 > vol_ma5）。
    """
    return (df["rsi6"] < 35) & (df["close"] > df["open"]) & (df["volume"] > df["vol_ma5"])

@deprecated("该函数已废弃，胜率太低")
def sig_bottom_volume(df):
    """策略19：地量见底——成交量创 60 日新低（地量）+ 超卖 + 阳线，底部信号。

    逻辑（全部满足）：量 <= vol_60d_min、RSI6 < 40、阳线（close > open）。
    """
    return (df["volume"] <= df["vol_60d_min"]) & (df["rsi6"] < 40) & (df["close"] > df["open"])


def sig_one_yang_three_yin(df):
    """策略20：一阳三阴——4 日前大阳线（>3%）后连续 3 日缩量阴线，今日放量回升。

    逻辑（全部满足）：
      - 4 日前涨幅 > 3%（启动阳线）
      - 其后 3 日成交量均 < 启动日量的 70%（缩量洗盘）
      - 当日上涨且收盘高于 4 日前收盘（重回启动平台）
    """
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
    """策略21：箱体震荡——价格贴近布林中轨、RSI 中性、缩量的横盘蓄势信号。

    逻辑（全部满足）：
      - 收盘价距布林中轨偏离 < 2%（箱体中部）
      - RSI6 处于 30~70（中性区）
      - 缩量（量 < 0.8*vol_ma20）
      - 收盘站上 MA20（偏多箱体）
    """
    boll_mid = df["boll_mid"]
    price_near_mid = (df["close"] - boll_mid).abs() / boll_mid < 0.02
    rsi_ok = (df["rsi6"] > 30) & (df["rsi6"] < 70)
    vol_shrink = df["volume"] < df["vol_ma20"] * 0.8
    trend_up = df["close"] > df["ma20"]
    return price_near_mid & rsi_ok & vol_shrink & trend_up


def sig_wave_theory(df):
    """策略22：波浪理论——突破 20 日高点（第 3 浪启动）+ 量价配合。

    打分权重：
      收盘突破 20 日新高        +4
      放量（量 > 1.5*vol_ma5） +3
      RSI6 处于 45~75（强势区） +2
      MACD 红柱（动能向上）     +2
      收盘站上 MA20            +1
    总分 >= 8 触发信号。
    """
    breakout = df["close"] > df["high_20d_max"]
    vol_surge = df["volume"] > df["vol_ma5"] * 1.5
    rsi_strong = (df["rsi6"] > 45) & (df["rsi6"] < 75)
    macd_bullish = df["macd_hist"] > 0
    price_above_ma20 = df["close"] > df["ma20"]
    score = breakout.astype(int) * 4 + vol_surge.astype(int) * 3 + rsi_strong.astype(int) * 2 + macd_bullish.astype(int) * 2 + price_above_ma20.astype(int)
    return score >= 8


def sig_chan_theory(df):
    """策略23：缠论底背驰——价格创新低但 MACD 未创新低，下跌动能衰竭的买点。

    逻辑（全部满足）：
      - 收盘创新低（close < 昨收）
      - MACD DIF 未创新低（dif >= 昨 dif，底背驰核心条件）
      - 放量（量 > vol_ma5）
      - RSI6 < 50（仍处弱势但未极端）
      - 阳线（close > open）
    """
    pc = df.groupby("code")["close"].shift(1)
    new_low = df["close"] < pc
    dif = df["macd_dif"]
    difp = df.groupby("code")["macd_dif"].shift(1)
    macd_not_new_low = dif >= difp
    vol_ok = df["volume"] > df["vol_ma5"]
    rsi_oversold = df["rsi6"] < 50
    close_above_open = df["close"] > df["open"]
    return new_low & macd_not_new_low & vol_ok & rsi_oversold & close_above_open


# 策略注册表：策略名 -> 信号函数。回测、验证、推荐均通过该表调度。
STRATEGIES = {
    "ma_crossover": sig_ma_crossover,
    "volume_surge_std": sig_volume_surge_std,
    "wonderful_9_turn": sig_wonderful_9_turn,
    "n_pattern": sig_n_pattern,
    "limit_up_pullback": sig_limit_up_pullback,
    "stable_then_limit_up": sig_stable_then_limit_up,
    "monthly_macd_20ma": sig_monthly_macd_20ma,
    "low_position_limit_up": sig_low_position_limit_up,
    # "limit_up_resonance": sig_limit_up_resonance,
    # "bullish_engulfing": sig_bullish_engulfing,
    "multi_ma_resonance": sig_multi_ma_resonance,
    "ensemble": sig_ensemble,
    "volume_breakout": sig_volume_breakout,
    "bull_trend": sig_bull_trend,
    "ma_golden_cross": sig_ma_golden_cross,
    "shrink_pullback": sig_shrink_pullback,
    "dragon_head": sig_dragon_head,
    "emotion_cycle": sig_emotion_cycle,
    # "bottom_volume": sig_bottom_volume,
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
        """计算最大回撤（%）。

        以等权累加收益序列模拟资金曲线，跟踪历史峰值，返回
        (峰值-当前)/峰值 的最大值。Numba 加速版本，要求输入为 numpy 数组。
        """
        n = len(r)
        if n < 2:
            return 0.0
        cur_eq = r[0]
        peak = cur_eq
        max_dd = 0.0
        for i in range(1, n):
            cur_eq += r[i]
            if cur_eq > peak:
                peak = cur_eq
            if peak > 0:
                dd = (peak - cur_eq) / peak
                if dd > max_dd:
                    max_dd = dd
        return max_dd * 100.0


    @numba.njit(cache=True)
    def _calc_metrics_core(r: np.ndarray, n: int, avg_holding: float) -> Tuple:
        """绩效指标核心计算（Numba JIT 加速）。

        输入 r 为单笔收益率的 numpy 数组；先过滤 |r| >= 5 的异常值
        （多为除权/停牌导致的失真收益），再计算：
          胜率、平均盈/亏、盈亏比、累计收益、年化收益、最大回撤、夏普比率。
        返回值固定 9 元组：(win_rate, avg_win, avg_loss, profit_loss_ratio,
        total_return_pct, annualized_return, max_drawdown, sharpe, n_valid)。
        """
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
        cur_eq = r[0]
        peak = cur_eq
        max_dd = 0.0
        for ri in r[1:]:
            cur_eq += ri
            if cur_eq > peak:
                peak = cur_eq
            if peak > 0:
                dd = (peak - cur_eq) / peak
                if dd > max_dd:
                    max_dd = dd
        return max_dd * 100.0


    def _calc_metrics_core(r: np.ndarray, n: int, avg_holding: float):
        """绩效指标核心计算的纯 Python 回退版本（未安装 Numba 时使用）。

        与 Numba 版本逻辑一致：先过滤 NaN 与 |r| >= 5 的异常收益，
        再计算胜率、盈亏比、累计/年化收益、最大回撤与夏普比率。
        """
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


def calc_metrics(returns: np.ndarray, avg_holding: Optional[float] = None) -> Dict:
    """对单持有期的信号收益序列计算绩效指标字典。

    收益来源：信号触发日的某个持有期前瞻收益（ret_{p}d）序列，
    调用方应保证只传入单一持有期的收益（P0-4：避免多期收益混合）。
    avg_holding 为该持有期天数，用于年化收益与夏普的换算；
    为 None 时回退到 HOLDING_PERIODS 均值（向后兼容）。
    输出指标：total_trades / win_rate / avg_win / avg_loss /
    profit_loss_ratio / total_return / annualized_return / max_drawdown / sharpe_ratio。
    """
    r = np.asarray(returns, dtype=float).flatten()
    n = len(r)
    if n == 0:
        return {}

    if avg_holding is None:
        avg_holding = sum(HOLDING_PERIODS) / len(HOLDING_PERIODS)

    wr, aw, al, pl, tr, ann, mxd, sr, n_valid = _calc_metrics_core(r, n, avg_holding)

    # 期望值(每笔平均期望收益%)与凯利分数(风险提示3:胜率不是唯一目标)
    wr_dec = wr / 100.0
    expectation = wr_dec * aw - (1 - wr_dec) * al
    kelly = 0.0
    if pl > 1e-10:
        b = pl
        kelly = (b * wr_dec - (1 - wr_dec)) / b * 100.0

    return {
        "total_trades": int(n_valid),
        "win_rate": round(wr, 2),
        "avg_win": round(aw, 2),
        "avg_loss": round(al, 2),
        "profit_loss_ratio": round(pl, 2),
        "expectation": round(expectation, 4),
        "kelly": round(kelly, 4),
        "total_return": round(float(np.clip(tr, -1e10, 1e10)), 2),
        "annualized_return": round(float(np.clip(ann, -1e10, 1e10)), 2),
        "max_drawdown": round(float(np.clip(mxd, 0, 100)), 2),
        "sharpe_ratio": round(float(np.clip(sr, -100, 100)), 2),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 内存监控辅助
# ═══════════════════════════════════════════════════════════════════════════════

def log_memory_usage(stage: str):
    """记录当前进程 RSS 内存占用（MB），用于监控大数据量阶段的峰值内存。"""
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
    """单策略回测：生成信号 -> 剔除不可成交（涨停）信号 -> 按持有期分别统计绩效。

    修复要点：
      - P2-13：涨停封板日无法按收盘价买入，先剔除涨停信号
      - P0-4：各持有期收益分开统计，不再混合进同一序列（样本独立）
      - P0-2：每个持有期的收益按日期排序后再计算最大回撤（时间序列回撤）
    异常时返回 {"strategy": name, "error": ...}，不中断整体回测；
    无信号时返回全 0 指标。
    """
    t0 = time.time()
    try:
        sig = STRATEGIES[name](df)
        signals = df[_apply_cooldown(df, sig)]
        n = signals["code"].count()
        if n == 0:
            return {"strategy": name, "total_trades": 0, "win_rate": 0,
                    "avg_win": 0, "avg_loss": 0, "profit_loss_ratio": 0,
                    "total_return": 0, "annualized_return": 0,
                    "max_drawdown": 0, "sharpe_ratio": 0, "time_s": round(time.time() - t0, 1)}

        # 按持有期分别统计（P0-4），每个持有期的收益按日期排序后计算（P0-2）
        # 优先使用动态退出收益（ATR止损/移动止盈），否则回退固定持有期收益
        period_metrics: Dict[int, Dict] = {}
        for p in HOLDING_PERIODS:
            col = f"dyn_ret_{p}d" if f"dyn_ret_{p}d" in signals.columns else f"ret_{p}d"
            if col not in signals.columns:
                continue
            sub = signals[["date", col]].dropna()
            if len(sub) == 0:
                continue
            sub = sub.sort_values("date")
            # 扣减往返交易成本后再统计绩效
            net_ret = sub[col].values - TRADING_COST_PCT / 100.0
            m = calc_metrics(net_ret, avg_holding=p)
            m["total_trades"] = int(len(sub))
            # 分年度评估（D1）：按信号年份拆分胜率/收益,识别过拟合或牛市 beta
            yearly = {}
            sub_y = sub.copy()
            sub_y["year"] = sub_y["date"].dt.year
            for yr, g in sub_y.groupby("year"):
                gy = calc_metrics(g[col].values - TRADING_COST_PCT / 100.0, avg_holding=p)
                yearly[int(yr)] = {
                    "trades": gy["total_trades"],
                    "win_rate": gy["win_rate"],
                    "total_return": gy["total_return"],
                    "expectation": gy["expectation"],
                }
            m["yearly"] = yearly
            period_metrics[p] = m

        if not period_metrics:
            return {"strategy": name, "total_trades": 0, "win_rate": 0,
                    "avg_win": 0, "avg_loss": 0, "profit_loss_ratio": 0,
                    "total_return": 0, "annualized_return": 0,
                    "max_drawdown": 0, "sharpe_ratio": 0, "time_s": round(time.time() - t0, 1)}

        # 聚合：取最优持有期（总收益最高）作为该策略代表指标，同时保留各持有期明细
        best_p = max(period_metrics, key=lambda p: period_metrics[p]["total_return"])
        m = dict(period_metrics[best_p])
        m["strategy"] = name
        m["total_trades"] = n
        m["best_period"] = best_p
        m["periods"] = period_metrics
        m["time_s"] = round(time.time() - t0, 1)
        return m
    except Exception as e:
        return {"strategy": name, "error": str(e), "time_s": round(time.time() - t0, 1)}


def compute_benchmark_metrics(df: pd.DataFrame, index_df: Optional[pd.DataFrame] = None) -> Dict:
    """计算市场基准绩效指标,用于区分 alpha/beta(D2)。

    优先使用真实指数日线(tushare_index_daily,沪深300)计算基准,
    指数数据缺失时回退到全股票等权平均(旧逻辑)。
    返回: benchmark_return / benchmark_annualized / benchmark_max_drawdown。
    """
    if index_df is not None and not index_df.empty and "index_close" in index_df.columns:
        idx = index_df.sort_values("date").reset_index(drop=True)
        idx["ret"] = idx["index_close"].pct_change()
        daily = idx.dropna(subset=["ret"])["ret"].to_numpy(dtype=float)
    elif df is not None and not df.empty and "date" in df.columns and "pct_chg" in df.columns:
        daily = df.groupby("date")["pct_chg"].mean().sort_index().dropna().to_numpy(dtype=float) / 100.0
    else:
        return {}

    if len(daily) < 2:
        return {}

    total_return = float((np.prod(1 + daily) - 1) * 100.0)
    years = len(daily) / 252.0
    annualized = float((np.prod(1 + daily) ** (1.0 / max(years, 1e-10)) - 1) * 100.0)
    max_drawdown = float(_calc_max_drawdown(daily))

    return {
        "benchmark_return": round(total_return, 2),
        "benchmark_annualized": round(annualized, 2),
        "benchmark_max_drawdown": round(max_drawdown, 2),
    }


def run_backtests(df_bt: pd.DataFrame, index_df: Optional[pd.DataFrame] = None) -> List[Dict]:
    """对全部 23 个策略执行回测，返回按期望值降序的有效结果列表。

    依据 resolve_parallel_config 自动选择串行 / ThreadPoolExecutor 并行；
    单个策略失败不影响其他策略（错误结果单独标记）。
    每个有效结果附加基准对比字段（D2）：
      - benchmark_return：同期基准总收益(沪深300,缺失时回退等权)
      - benchmark_annualized：基准年化收益
      - excess_return：策略总收益 - 基准总收益（超额收益）
    """
    t0 = time.time()
    n_strategies = len(STRATEGIES)

    enable_parallel, max_workers = resolve_parallel_config()
    n_workers = min(max_workers, n_strategies)

    if enable_parallel and n_workers > 1:
        logger.info(f"并行回测 {n_strategies} 策略（{n_workers} 线程）...")
    else:
        logger.info(f"串行回测 {n_strategies} 策略...")

    # 市场基准（D2）：优先沪深300真实指数,缺失时回退等权
    benchmark = compute_benchmark_metrics(df_bt, index_df=index_df)
    if benchmark:
        logger.info(f"市场基准: 总收益 {benchmark['benchmark_return']:.2f}%, "
                    f"年化 {benchmark['benchmark_annualized']:.2f}%, "
                    f"最大回撤 {benchmark['benchmark_max_drawdown']:.2f}%")
    else:
        logger.warning("基准计算失败（数据不足），跳过超额收益对比")

    strategy_names = list(STRATEGIES.keys())
    results = []

    # 先串行计算组合策略的组件,按其胜率填充 _ENSEMBLE_WEIGHTS(C1)
    global _ENSEMBLE_WEIGHTS
    _ENSEMBLE_WEIGHTS = {}
    component_results = {}
    for name in ENSEMBLE_COMPONENTS:
        r = _backtest_single(name, df_bt)
        component_results[name] = r
        results.append(r)
        if "error" not in r:
            logger.info(f"  [component] {r['strategy']}: {r['total_trades']} 笔, "
                        f"胜率{r['win_rate']:.1f}%, 总收益{r['total_return']:.1f}%")
    wr = np.array([component_results[c].get("win_rate", 0) if "error" not in component_results[c] else 0
                   for c in ENSEMBLE_COMPONENTS], dtype=float)
    wr_sum = wr.sum()
    if wr_sum > 0:
        _ENSEMBLE_WEIGHTS = {c: float(w / wr_sum) for c, w in zip(ENSEMBLE_COMPONENTS, wr)}
        logger.info(f"组合策略权重: {', '.join(f'{c}={_ENSEMBLE_WEIGHTS[c]:.3f}' for c in ENSEMBLE_COMPONENTS)}")
    else:
        logger.warning("组件策略无有效胜率,组合策略退化为等权")

    remaining = [n for n in strategy_names if n not in ENSEMBLE_COMPONENTS]

    def _run_one(name):
        r = _backtest_single(name, df_bt)
        results.append(r)
        if "error" not in r:
            logger.info(f"  [{len(results)}/{n_strategies}] {r['strategy']}: {r['total_trades']} 笔, "
                        f"胜率{r['win_rate']:.1f}%, 总收益{r['total_return']:.1f}%")
        else:
            logger.error(f"  [{len(results)}/{n_strategies}] {r['strategy']}: {r['error']}")
        log_memory_usage(f"策略 {len(results)} 完成后")

    if enable_parallel and n_workers > 1 and len(remaining) > 1:
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = [executor.submit(_run_one, name) for name in remaining]
            for f in futures:
                f.result()
    else:
        for name in remaining:
            _run_one(name)

    # 附加基准对比与超额收益字段（P1-8）
    for r in results:
        if "error" not in r and benchmark:
            r["benchmark_return"] = benchmark["benchmark_return"]
            r["benchmark_annualized"] = benchmark["benchmark_annualized"]
            r["benchmark_max_drawdown"] = benchmark["benchmark_max_drawdown"]
            r["excess_return"] = round(r["total_return"] - benchmark["benchmark_return"], 2)

    # 按期望值(每笔平均收益)降序排序,兼顾胜率与盈亏比(风险提示3)
    valid = sorted([r for r in results if "error" not in r],
                   key=lambda x: x.get("expectation", -999), reverse=True)
    logger.info(f"回测完成，耗时 {time.time()-t0:.1f}s")
    return valid


# ═══════════════════════════════════════════════════════════════════════════════
# 最近5日验证
# ═══════════════════════════════════════════════════════════════════════════════

def validate_week(df_week, top_results, top_n=5):
    """最近 5 个交易日的本周验证：对回测 Top-N 策略做实际买入/卖出收益统计。

    验证口径：
      - 买入日 = 区间第 5 个交易日（开盘买入）
      - 卖出日 = 区间最后 1 个交易日（收盘卖出）
      - 仅统计买入日触发信号且买卖价均存在的股票
    返回每个策略的：交易数、胜率、平均收益、买入日信号明细（前 8 只）。
    """
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
            sig = STRATEGIES[name](df_week) & _entry_mask(df_week)
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
            rets = (valid_df["sell_price"] / valid_df["buy_price"] - 1).values - TRADING_COST_PCT / 100.0

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
    """以表格形式打印回测绩效汇总与 5 日验证 Top-5 结果。

    回测表新增"超额%"列（策略总收益 - 等权市场基准总收益，P1-8），
    并打印基准行，用于判断策略收益是否真正跑赢市场（alpha）。
    """
    print("\n" + "=" * 140)
    print(f"{'策略':<28} {'交易':>7} {'胜率%':>7} {'均盈%':>7} {'均亏%':>7} "
          f"{'盈亏比':>7} {'期望%':>8} {'凯利%':>7} {'总收益%':>9} {'超额%':>8} {'年化%':>8} {'最大回撤%':>10} {'夏普':>6} {'耗时':>5}")
    print("-" * 140)
    for r in results:
        excess = r.get("excess_return", 0) if "error" not in r else 0
        print(
            f"  {r['strategy']:<26} {r['total_trades']:>7} "
            f"{r['win_rate']:>7.1f} {r['avg_win']:>7.2f} {r['avg_loss']:>7.2f} "
            f"{r['profit_loss_ratio']:>7.2f} {r.get('expectation', 0):>8.3f} "
            f"{r.get('kelly', 0):>7.2f} {r['total_return']:>9.1f} "
            f"{excess:>8.1f} "
            f"{r['annualized_return']:>8.1f} {r['max_drawdown']:>10.1f} "
            f"{r['sharpe_ratio']:>6.2f} {r.get('time_s',0):>5.1f}s"
        )

    print("=" * 140)
    print(f"\n回测区间: {backtest_start.date()} ~ {backtest_end.date()}")
    print(f"初始资金: {INITIAL_CAPITAL:,.0f} 元")
    if results:
        valid_results = [r for r in results if r["total_trades"] > 0]
        print(f"有信号策略: {len(valid_results)}/{len(results)}")
        print(f"平均胜率:   {np.mean([r['win_rate'] for r in valid_results]):.1f}%")
        print(f"平均总收益: {np.mean([r['total_return'] for r in valid_results]):.1f}%")
        print(f"平均年化:   {np.mean([r['annualized_return'] for r in valid_results]):.1f}%")
        # 基准对比（P1-8）：取首个结果携带的基准字段
        if valid_results and "benchmark_return" in valid_results[0]:
            br = valid_results[0]["benchmark_return"]
            ba = valid_results[0]["benchmark_annualized"]
            bd = valid_results[0].get("benchmark_max_drawdown", 0)
            above = sum(1 for r in valid_results if r.get("excess_return", 0) > 0)
            print(f"等权市场基准: 总收益 {br:.1f}%  年化 {ba:.1f}%  最大回撤 {bd:.1f}%")
            print(f"跑赢基准策略: {above}/{len(valid_results)}")

        # 分年度评估（D1）：各策略逐年胜率,识别过拟合/牛市 beta
        years = sorted({y for r in valid_results for y in (r.get("yearly") or {})})
        if years:
            print("\n" + "=" * 140)
            print("分年度胜率% (逐年信号样本胜率,空=该年无信号)")
            print(f"{'策略':<28}" + "".join(f"{y:>10}" for y in years))
            print("-" * 140)
            for r in valid_results:
                yr = r.get("yearly") or {}
                row = f"  {r['strategy']:<26}"
                for y in years:
                    info = yr.get(y)
                    if info and info["trades"] > 0:
                        row += f"{info['win_rate']:>10.1f}"
                    else:
                        row += f"{'-':>10}"
                print(row)
            print("=" * 140)

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
    """按本周实际收益对股票排序，返回 5 日验证收益最高的前 top_n 只股票。

    统计口径：
      - 对每个有信号的回测策略，取本周（买入日）触发的股票
      - 买入价 = 买入日开盘价；卖出价 = 最后交易日收盘价
      - 同一股票被多个策略命中时合并：记录命中策略数、平均胜率，
        收益取各策略计算值中的最大值
    返回按 sell_return 降序的股票推荐列表。
    """
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
            sig = STRATEGIES[strategy_name](df_week) & _entry_mask(df_week)
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
    """取回测结果前 top_n 个有交易记录的策略名（去重顺序列表）。"""
    return [r["strategy"] for r in results[:top_n] if r.get("total_trades", 0) > 0]


def get_next_day_recommendations(df_latest, top_stocks_or_results, results=None, top_n=10):
    """基于回测 Top-N 策略，生成最新交易日的个股推荐。

    对每个入选策略，在最新交易日数据上重新应用信号函数，命中则累计
    该策略的历史胜率作为评分：total_score = Σ(命中策略的胜率)。
    返回按（命中策略数, 平均胜率）降序的推荐列表 + 入选策略名列表。
    """
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
            sig = STRATEGIES[strategy_name](df_latest) & _entry_mask(df_latest)
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
    """调用主程序 main.py 对推荐股票执行大盘复盘与个股决策。

    将股票代码（内部格式 sh.xxx / sz.xxx）还原为纯数字后，以
    `python main.py --stocks <codes> --force-run` 子进程方式执行。
    """
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
    """判断指定日期是否为 A 股交易日（查询本地 tushare_trade_cal 表）。

    查询失败或未找到数据时保守返回 True（假定为交易日），避免误拦截；
    数据库未配置（无 PG_PASSWORD）时同样返回 True。
    """
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
    """主流程：交易日检查 -> 数据加载 -> 复权 -> 指标 -> 回测 -> 验证 -> 推荐 -> 主程序。

    区间划分（基于最近交易日集合）：
      - 回测区间：最早交易日 ~ 倒数第 6 个交易日
      - 验证区间：倒数第 5 个交易日 ~ 最后 1 个交易日
    非交易日默认退出，--force 可强制运行。
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="强制运行（非交易日也执行）")
    args = parser.parse_args(argv)

    if not args.force and not is_trading_day(datetime.now()):
        logger.error("非交易日，程序退出（使用 --force 可强制运行）")
        return

    # 回测起始日锚定到 5 年前的 1 月 1 日，保证自然年度对齐
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

    # 市场环境过滤:加载上证指数日线 -> 计算 regime -> 合并到 df_all
    try:
        df_index = load_index_daily(anchored_start, today_str)
        regime_df = compute_market_ok(df_index)
        ok_days = regime_df.loc[regime_df["market_ok"], "date"]
        ok_ratio = len(ok_days) / len(regime_df) if len(regime_df) > 0 else 0
        logger.info(f"市场环境: {len(ok_days)}/{len(regime_df)} 个交易日可开仓 ({ok_ratio*100:.1f}%)")
        df_all = df_all.merge(regime_df, on="date", how="left")
        df_all["market_ok"] = df_all["market_ok"].fillna(False).astype(bool)
        del df_index, regime_df
        gc.collect()
    except Exception as e:
        logger.warning(f"市场环境数据加载失败，跳过 regime 过滤: {e}")
        df_all["market_ok"] = True

    # 财务质量过滤:按 ann_date 对齐(防前视偏差)
    try:
        df_fina = load_fina_indicator(anchored_start, today_str)
        if not df_fina.empty:
            df_all = merge_fina_by_ann_date(df_all, df_fina)
            df_all.rename(columns={"roe": "fin_roe", "grossprofit_margin": "fin_gross_margin",
                                   "or_yoy": "fin_or_yoy"}, inplace=True)
            logger.info(f"财务过滤字段: fin_roe 覆盖率 {df_all['fin_roe'].notna().mean()*100:.1f}%")
        del df_fina
        gc.collect()
    except Exception as e:
        logger.warning(f"财务数据加载失败，跳过财务过滤: {e}")

    # 动态退出收益(ATR止损 + 移动止盈 + 时间止损)
    logger.info("计算动态退出收益(ATR止损/移动止盈)...")
    t_dyn = time.time()
    dyn_ret = compute_dynamic_exit_returns(df_all)
    df_all = pd.concat([df_all, dyn_ret], axis=1)
    del dyn_ret
    gc.collect()
    logger.info(f"动态退出收益计算完成，耗时 {time.time()-t_dyn:.1f}s")

    # 按最近交易日划分回测 / 验证区间：最后 5 个交易日留给验证，其余用于回测
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

    # 基准指数(沪深300, D2):用于超额收益对比;加载失败时回退等权基准
    df_bench_index = None
    try:
        df_bench_index = load_index_daily(anchored_start, today_str, ts_code="000300.SH")
        if df_bench_index.empty:
            df_bench_index = None
    except Exception as e:
        logger.warning(f"基准指数加载失败，回退等权基准: {e}")
        df_bench_index = None

    results = run_backtests(df_bt, index_df=df_bench_index)
    val_results = validate_week(df_week, results, TOP_N_VALIDATE)
    print_results(results, val_results, backtest_start_date, backtest_end_date)

    # 汇总本周验证胜率前 10 股票，作为主程序个股决策的输入
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
