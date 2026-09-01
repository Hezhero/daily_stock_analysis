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
- 基准对比: 新增市场基准（compute_benchmark_metrics，沪深300真实指数，
  缺失时回退等权），结果表打印超额收益列与基准行，用于区分 alpha/beta（P1-8/D2）
- 数据增强过滤(P2): 接入筹码分布（tushare_cyq 获利盘/90%集中度/平均成本）、
  自由流通换手率/股息率/PS（tushare_daily_basic）、融资余额（tushare_margin_detail）、
  股东人数变化（tushare_stk_holdernumber）、业绩预告（tushare_forecast）、
  质押比例（tushare_pledge_stat）、机构龙虎榜净买（tushare_top_inst），
  按策略族差异化应用质量过滤（趋势/涨停/超跌三族）；新增 5 个数据驱动策略
  （washout_break / low_profit_hold / holder_conc_break / inst_smart_break /
  fc_pos_break），废弃胜率无法提升的 bull_trend 与无信号的 shrink_pullback
- 推荐输出新增 recommended_hold_days（命中策略最优持有期 best_period 的胜率加权
  平均取整到 1/3/5/10 候选期）；推荐收益口径改为命中策略 best_period 前瞻收益
  （dyn_ret 优先/ret 兜底，与回测/出场规则同口径；最优期 5/10 日超出验证窗口
  无法测量时回退固定窗口收益）
"""

import argparse
import contextlib
import logging
import os
import sys
import time
import gc
import subprocess
from logging.handlers import RotatingFileHandler
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dotenv import load_dotenv
from typing_extensions import deprecated

load_dotenv()

from typing import Dict, List, Optional, Tuple, Callable

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
MIN_TRADES_FOR_RANKING = 30   # 策略参与排名/汇总的最小样本笔数；不足视为小样本，不参与期望值排名与跨策略平均
HANDOFF_MIN_STRATEGIES = 3    # 5日验证股票自动送入主程序实盘分析的最小命中策略数（共振门槛）
HANDOFF_MIN_STRATEGY_TRADES = 30  # 命中策略的历史回测最小笔数，低于此的策略不计入共振、不支撑 handoff

# ─── 数据质量与可成交性过滤 ────────────────────────────────────────────────────
MIN_DAILY_AMOUNT_K = 5000        # 最小日成交额（单位：千元，即 500 万元），过滤流动性不足的样本
MIN_LISTING_DAYS = 120           # 上市最短天数，过滤次新股（上市初期涨跌结构特殊）
LIMIT_UP_PCT_MAIN = 9.5          # 主板/中小板涨停判定阈值（%）
LIMIT_UP_PCT_GEM = 19.5          # 创业板涨停判定阈值（%），2020-08 起涨跌幅扩至 20%

# ─── 交易成本 ──────────────────────────────────────────────────────────────────
TRADING_COST_PCT = 0.15          # 单边往返交易成本（%）：佣金+印花税+滑点合计约 0.15%

# ─── 信号质量过滤（P1） ────────────────────────────────────────────────────────
MIN_CIRC_MV_W = 200000           # 最小流通市值（万元，即 20 亿），过滤易被操纵的小盘股
MAX_CIRC_MV_W = 8000000          # 最大流通市值（万元，即 800 亿），过滤弹性差的超大盘股（P0-3 从 500 亿放宽）
MIN_VOLUME_RATIO = 1.5           # 信号日最低量比（Tushare 官方归一化活跃度指标）
MONEYFLOW_LOOKBACK = 3           # 主力资金净流入确认的回看天数

# ─── 信号去重/冷却期（C3） ─────────────────────────────────────────────────────
SIGNAL_COOLDOWN_DAYS = 5         # 同一股票同一策略 N 日内只取第一次信号
MIN_RESONANCE_STRATEGIES = 2     # 共振门槛：同一股票同日至少 N 个策略命中才保留（P0-2，Exp4 最优）

# ─── 数据增强过滤（P2，基于 tushare 扩展表，按策略族差异化应用） ────────────────
MIN_TURNOVER_RATE_F = 1.0        # 最小自由流通换手率（%），过低=流动性差/无人问津
MAX_TURNOVER_RATE_F = 12.0       # 最大自由流通换手率（%），过高=情绪过热/分歧巨大
MIN_PROFIT_RATIO = 0.2           # 最小获利盘比例（筹码分布 0~1），过低=深度套牢盘压制
MAX_PROFIT_RATIO = 0.8           # 最大获利盘比例（0~1），过高=高位接盘风险
MAX_PLEDGE_RATIO = 40.0          # 最大累计质押占总股本比例（%），过高=股权质押爆仓风险
MIN_DIV_YIELD = 1.0              # 最低股息率（%），基本面安全垫（剔除纯炒作无分红股）
MAX_CONC_90_LIMITUP = 0.15       # 涨停族策略的 90% 筹码集中度上限（0~1），涨停日筹码需集中
MIN_INST_NET_BUY = 500.0         # 机构龙虎榜近 5 日累计净买入阈值（万元）
MAX_HOLDER_CHG_PCT = -3.0        # 股东人数环比下降阈值（%），负值=筹码集中
MAX_DRAGON_PROFIT_RATIO = 0.9    # 龙头策略获利盘上限，剔除纯高位接力的涨停
WASHOUT_PROFIT_RATIO = 0.2       # 超跌族策略的获利盘上限（绝大部分筹码被套）

# ─── 日志配置 ──────────────────────────────────────────────────────────────────
_LOG_DIR = Path(os.environ.get("LOG_DIR") or "logs")
if not _LOG_DIR.is_absolute():
    _LOG_DIR = BASE_DIR / _LOG_DIR
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LOG_FILE = _LOG_DIR / f"backtest_5y_{datetime.now():%Y%m%d}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("backtest")

# 同时写一份回测日志到 logs/ 目录（10MB 轮转，命名风格与主程序 stock_analysis_*.log 一致）
_file_handler = RotatingFileHandler(
    _LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
))
logger.addHandler(_file_handler)


class _Tee:
    """同时写入多个流（控制台 + 日志文件），用于 print 输出的双写。"""

    def __init__(self, *streams):
        self._streams = streams

    def write(self, text: str) -> int:
        for stream in self._streams:
            stream.write(text)
        return len(text)

    def flush(self):
        for stream in self._streams:
            stream.flush()


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
         （BACKTEST_ENABLE_PARALLEL=true 开启并行；false 或未设置时默认串行）
      2. 未设置环境变量时默认串行：共享 DataFrame 的多线程并发回测
         曾引发间歇性内存损坏/竞态（组件指标漂移、策略 0 交易），
         并行改为显式 opt-in——需要并行时设 BACKTEST_ENABLE_PARALLEL=true
         或 BACKTEST_MAX_WORKERS=<n>，线程数按 detect_resource_config 自动检测。
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

    return False, 1


# ═══════════════════════════════════════════════════════════════════════════════
# 数据加载（Tushare PostgreSQL 本地数据库）
# ═══════════════════════════════════════════════════════════════════════════════

def _convert_columns_to_float32(df: pd.DataFrame) -> pd.DataFrame:
    """将行情/基本面数值列统一降为 float32，节省约一半内存。

    注意：只转换数值列，code/name/date 等标识列保持原类型。
    原地转换（不产生副本），供分块加载时逐块调用，避免大表加载时内存翻倍。
    先 to_numeric 兜底（PG 全 NULL 列可能以 object 返回），再降精度。
    """
    float_columns = [
        "open", "high", "low", "close",
        "volume", "amount", "pct_chg", "turn", "pe_ttm", "pb_mrq",
        "up_limit", "down_limit", "circ_mv", "volume_ratio", "net_mf_amount",
        "buy_lg_amount", "buy_elg_amount", "buy_sm_amount", "sell_sm_amount",
        "net_big_amount",
        "turnover_rate_f", "ps_ttm", "dv_ratio", "total_mv",
        "profit_ratio", "avg_cost", "cost_90_low", "cost_90_high",
        "concentration_90", "concentration_70", "rzye", "rzye_chg5",
    ]
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
    return df


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

    内存优化（OOM 修复）：
      - 分块读取（chunksize）+ 逐块降 float32，避免 5 年全市场数据以
        float64 一次性驻留内存（此前 2 核小内存机器在加载阶段被杀）
      - SQL 已按 ts_code, trade_date 排序，code 由 ts_code 一一映射而来，
        顺序一致，无需再 sort_values（避免全量副本）
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
            b.turnover_rate_f,
            b.ps_ttm,
            b.dv_ratio,
            b.total_mv,
            m.net_mf_amount,
            m.buy_lg_amount,
            m.buy_elg_amount,
            m.buy_sm_amount,
            m.sell_sm_amount,
            (COALESCE(m.buy_lg_amount, 0) + COALESCE(m.buy_elg_amount, 0)
             - COALESCE(m.sell_lg_amount, 0) - COALESCE(m.sell_elg_amount, 0)) AS net_big_amount,
            l.up_limit,
            l.down_limit,
            cy.profit_ratio,
            cy.avg_cost,
            cy.cost_90_low,
            cy.cost_90_high,
            cy.concentration_90,
            cy.concentration_70,
            md.rzye
        FROM tushare_daily d
        JOIN tushare_stock_basic s ON d.ts_code = s.ts_code
        LEFT JOIN tushare_daily_basic b ON d.ts_code = b.ts_code AND d.trade_date = b.trade_date
        LEFT JOIN tushare_moneyflow m ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
        LEFT JOIN tushare_stk_limit l ON d.ts_code = l.ts_code AND d.trade_date = l.trade_date
        LEFT JOIN tushare_cyq cy ON d.ts_code = cy.ts_code AND d.trade_date = cy.trade_date
        LEFT JOIN tushare_margin_detail md ON d.ts_code = md.ts_code AND d.trade_date = md.trade_date
        WHERE d.trade_date BETWEEN %s AND %s
          AND s.list_status = 'L'
          AND s.exchange IN ('SSE', 'SZSE')
          AND s.market IN ('主板', '中小板', '创业板')
          AND d.amount >= %s
          AND d.trade_date >= (s.list_date + %s)
        ORDER BY d.ts_code, d.trade_date
    """

    engine = _get_pg_engine()
    chunks = []
    try:
        with engine.connect() as conn:
            # 分块读取 + 逐块降 float32：峰值内存 = 单块 float64 + 累计 float32，
            # 而非整表 float64 + 多份全量副本
            for chunk in pd.read_sql(
                sql, conn,
                params=(start, end, MIN_DAILY_AMOUNT_K, MIN_LISTING_DAYS),
                parse_dates=["date"],
                chunksize=200_000,
            ):
                chunks.append(_convert_columns_to_float32(chunk))
    finally:
        engine.dispose()

    if chunks:
        df = pd.concat(chunks, ignore_index=True)
        chunks.clear()
        gc.collect()
        log_memory_usage("行情数据加载后")
    else:
        # 查询无结果时返回带完整 schema 的空帧，保持与单次 read_sql 一致的下游行为
        # （下游会在交易日数量检查处优雅退出，而非 KeyError）
        df = pd.DataFrame(columns=[
            "ts_code", "name", "date", "open", "high", "low", "close",
            "volume", "amount", "pct_chg", "turn", "pe_ttm", "pb_mrq",
            "circ_mv", "volume_ratio", "turnover_rate_f", "ps_ttm",
            "dv_ratio", "total_mv", "net_mf_amount",
            "buy_lg_amount", "buy_elg_amount", "buy_sm_amount", "sell_sm_amount",
            "net_big_amount",
            "up_limit", "down_limit", "profit_ratio", "avg_cost", "cost_90_low",
            "cost_90_high", "concentration_90", "concentration_70", "rzye",
        ])

    df["code"] = df["ts_code"].apply(_from_ts_code)
    df.drop(columns=["ts_code"], inplace=True)

    # 融资余额 5 日变化率（杠杆资金边际变化，按股票分组；
    # SQL 已按 ts_code 排序，code 顺序一致，无需再次排序）
    df["rzye_chg5"] = df.groupby("code")["rzye"].pct_change(5, fill_method=None).astype("float32")

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
      - market_ok（严格）: 指数收盘价站在 MA20 和 MA60 之上、
        MA20 上行(今 > 昨)、MA5 在 MA20 之上(未死叉)
      - market_ok_enh（增强, P0-1）: 严格条件 OR (指数 > MA20 且 MA20 上行)，
        放宽 MA60/MA5 要求以减少空仓天数（WFO Exp2c：年化 4.6%→18-21%，
        回撤 12.7%→10.8%，代价是胜率微降 ~1.5pct）
      - regime（P1-4）: bull/range/bear 三态市况，供策略分族调度
    返回 DataFrame,含 date、market_ok(bool)、market_ok_enh(bool)、regime(str)四列。
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
    # 增强条件：放宽为"指数 > MA20 且 MA20 上行"（不要求 MA60 与 MA5 关系）
    df["market_ok_enh"] = df["market_ok"] | (
        (close > df["idx_ma20"])
        & (df["idx_ma20"] > df["idx_ma20_prev"])
    )

    # 市况三态（P1-4）：bull=指数>MA60 且 MA20 五日斜率上行；
    # bear=指数<MA60 且 MA20 下行；其余为 range
    slope5 = df["idx_ma20"].pct_change(5) * 100
    regime = pd.Series("range", index=df.index)
    regime[(close > df["idx_ma60"]) & (slope5 > 0.2)] = "bull"
    regime[(close < df["idx_ma60"]) & (slope5 < 0)] = "bear"
    df["regime"] = regime

    return df[["date", "market_ok", "market_ok_enh", "regime"]]


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
    仅对 (code, date) 键表逐组 merge_asof,再按行序回填 3 个财务列,
    避免对全宽 df 逐组复制 + concat 造成峰值内存翻倍(大表下易 OOM)。
    """
    if fina_df is None or fina_df.empty or "ann_date" not in fina_df.columns:
        return df
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    fina = fina_df.sort_values(["code", "ann_date"]).reset_index(drop=True)
    fina_by_code = {code: g for code, g in fina.groupby("code", sort=False)}
    fina_cols = ["roe", "grossprofit_margin", "or_yoy"]
    keys = df[["code", "date"]]
    pieces = []
    for code, g in keys.groupby("code", sort=False):
        f = fina_by_code.get(code)
        if f is None or f.empty:
            g = g.copy()
            for c in fina_cols:
                g[c] = np.nan
            pieces.append(g)
            continue
        f = f[["ann_date"] + fina_cols]
        merged = pd.merge_asof(
            g, f,
            left_on="date", right_on="ann_date", direction="backward",
        )
        pieces.append(merged)
    aligned = pd.concat(pieces, ignore_index=True)
    # aligned 与 df 同源排序、同序分组,行序一一对应,按位置回填财务列
    out = df.copy()
    out[fina_cols] = aligned[fina_cols].to_numpy()
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


# ─── 行业动量上下文(P2-7:申万 L1 指数动量排名 + 股票成分映射) ──────────────────

INDUSTRY_MOMENTUM_LOOKBACK = 20   # 行业动量回看交易日数
INDUSTRY_TOP_N = 3                # 仅保留动量排名前 N 的行业


def load_industry_context(start: str, end: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """加载申万 L1 行业动量排名与股票→行业映射。

    返回:
      rank_df: date(datetime64), l1_code, ret_20d, ind_rank(1=最强)
      member_df: code(内部格式), l1_code
    局限: 成分映射取 out_date IS NULL 的当前口径，历史行业变更不回溯
    （point-in-time 偏差，对 5 年回测影响有限但存在）。
    """
    t0 = time.time()
    engine = _get_pg_engine()
    try:
        with engine.connect() as conn:
            idx_sql = """
                SELECT d.ts_code, d.trade_date AS date, d.close
                FROM tushare_index_daily d
                JOIN tushare_index_classify c
                  ON d.ts_code = c.index_code AND c.level = 'L1'
                WHERE d.trade_date BETWEEN %s AND %s
                ORDER BY d.ts_code, d.trade_date
            """
            idx = pd.read_sql(idx_sql, conn, params=(start, end), parse_dates=["date"])
            mem_sql = """
                SELECT ts_code, l1_code FROM tushare_index_member_all
                WHERE out_date IS NULL
            """
            mem = pd.read_sql(mem_sql, conn)
    finally:
        engine.dispose()

    if idx.empty or mem.empty:
        return pd.DataFrame(columns=["date", "l1_code", "ind_rank"]), \
               pd.DataFrame(columns=["code", "l1_code"])

    idx = idx.sort_values(["ts_code", "date"])
    idx["ret_20d"] = idx.groupby("ts_code")["close"].pct_change(
        INDUSTRY_MOMENTUM_LOOKBACK)
    idx["ind_rank"] = idx.groupby("date")["ret_20d"].rank(ascending=False)

    mem["code"] = mem["ts_code"].apply(_from_ts_code)
    member_df = mem[["code", "l1_code"]].drop_duplicates("code")
    rank_df = idx[["date", "ts_code", "ind_rank"]].rename(columns={"ts_code": "l1_code"})
    logger.info(f"行业动量上下文: {rank_df['l1_code'].nunique()} 个 L1 行业, "
                f"{len(member_df)} 只股票映射, 耗时 {time.time()-t0:.1f}s")
    return rank_df, member_df


def apply_industry_momentum(df: pd.DataFrame, rank_df: pd.DataFrame,
                            member_df: pd.DataFrame) -> pd.DataFrame:
    """将股票的行业动量排名合并为 ind_rank 列（无映射/无排名的日子为 NaN=放行）。"""
    if rank_df.empty or member_df.empty:
        df["ind_rank"] = np.nan
        return df
    # 统一 date 为 date-only（归一化到午夜），避免 merge 时因时间分量/ dtype 差异不匹配
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    rank_df = rank_df.copy()
    rank_df["date"] = pd.to_datetime(rank_df["date"]).dt.normalize()
    n_before = len(df)
    df = df.merge(member_df, on="code", how="left")
    df = df.merge(rank_df[["date", "l1_code", "ind_rank"]],
                  on=["date", "l1_code"], how="left")
    df.drop(columns=["l1_code"], inplace=True, errors="ignore")
    if len(df) != n_before:
        logger.warning(f"行业动量 merge 行数变化 {n_before} -> {len(df)}（member/rank 可能有重复键）")
    # 覆盖率：全区间平均 + 最新交易日，避免只看最新日因边界/预热误判为 0
    latest = df["date"].max()
    cov_latest = df.loc[df["date"] == latest, "ind_rank"].notna().mean()
    cov_all = df["ind_rank"].notna().mean()
    logger.info(f"行业动量覆盖: 最新交易日 {cov_latest*100:.1f}% / 全区间均值 {cov_all*100:.1f}% 个股有排名")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 增强信号数据加载（P2：股东人数/业绩预告/质押/机构龙虎榜，按公告日 as-of 对齐防前视）
# ═══════════════════════════════════════════════════════════════════════════════

AUX_MAX_AGE_DAYS = 400  # 事件类数据最大有效期（天），超过视为过期置 NaN


def _merge_aux_by_date(df: pd.DataFrame, right: pd.DataFrame,
                       right_on: str, cols: List[str]) -> pd.DataFrame:
    """逐 code 分组的 merge_asof（方向 backward），把低频事件数据对齐到交易日。

    right 需含 code 列与 right_on 日期列；未命中的组补 NaN（不过滤整行）。
    """
    df = df.sort_values(["code", "date"]).reset_index(drop=True)
    right = right.sort_values(["code", right_on]).reset_index(drop=True)
    right[right_on] = pd.to_datetime(right[right_on])
    right2 = pd.concat([right[["code", right_on]], right[cols]], axis=1)
    by_code = {code: g.drop(columns=["code"]) for code, g in right2.groupby("code", sort=False)}
    pieces = []
    for code, g in df.groupby("code", sort=False):
        f = by_code.get(code)
        if f is None or f.empty:
            g = g.copy()
            for c in cols:
                # 显式 float dtype，避免 concat 全 NaN 列触发 pandas all-NA FutureWarning
                g[c] = pd.Series(np.nan, index=g.index, dtype="float64")
            pieces.append(g)
            continue
        pieces.append(pd.merge_asof(g, f, left_on="date", right_on=right_on,
                                    direction="backward").drop(columns=[right_on], errors="ignore"))
    return pd.concat(pieces, ignore_index=True)


def load_signal_aux(df: pd.DataFrame) -> pd.DataFrame:
    """加载并合并增强信号字段（股东人数环比 / 业绩预告方向 / 质押比例 / 机构龙虎榜净买）。

    全部按公告口径 as-of 对齐（ann_date / end_date+30d），避免前视偏差；
    事件数据超过 AUX_MAX_AGE_DAYS 视为过期置 NaN（保留列但过滤时放行）。
    """
    t0 = time.time()
    start = df["date"].min().strftime("%Y-%m-%d")
    end = df["date"].max().strftime("%Y-%m-%d")
    engine = _get_pg_engine()

    try:
        with engine.connect() as conn:
            # 股东人数（ann_date 对齐 + 环比变化，环比下降=筹码集中）
            hol = pd.read_sql(
                "SELECT ts_code, ann_date, end_date, holder_num FROM tushare_stk_holdernumber "
                "WHERE ann_date BETWEEN %s AND %s ORDER BY ts_code, ann_date",
                conn, params=(start, end),
            )
            # 业绩预告（ann_date 对齐，type 分类为正向/负向）
            fc = pd.read_sql(
                "SELECT ts_code, ann_date, type, p_change_min FROM tushare_forecast "
                "WHERE ann_date BETWEEN %s AND %s ORDER BY ts_code, ann_date",
                conn, params=(start, end),
            )
            # 质押统计（end_date 后移 30 天近似公告日）
            pl = pd.read_sql(
                "SELECT ts_code, end_date, pledge_ratio FROM tushare_pledge_stat "
                "WHERE end_date BETWEEN %s AND %s ORDER BY ts_code, end_date",
                conn, params=(start, end),
            )
            # 机构龙虎榜（按日聚合净买入）
            ti = pd.read_sql(
                "SELECT ts_code, trade_date, net_buy FROM tushare_top_inst "
                "WHERE trade_date BETWEEN %s AND %s ORDER BY ts_code, trade_date",
                conn, params=(start, end),
            )
    finally:
        engine.dispose()

    # ── 股东人数 ──
    hol["code"] = hol["ts_code"].apply(_from_ts_code)
    hol.drop(columns=["ts_code"], inplace=True)
    hol["holder_num"] = pd.to_numeric(hol["holder_num"], errors="coerce")
    hol = hol.sort_values(["code", "ann_date"]).reset_index(drop=True)
    hol["holder_chg"] = hol.groupby("code")["holder_num"].pct_change(fill_method=None).astype("float32")
    df = _merge_aux_by_date(df, hol, "ann_date", ["end_date", "holder_num", "holder_chg"])
    df = df.rename(columns={"end_date": "holder_end_date"})
    df["holder_end_date"] = pd.to_datetime(df["holder_end_date"], errors="coerce")
    stale_holder = (df["date"] - df["holder_end_date"]).dt.days > AUX_MAX_AGE_DAYS
    df.loc[stale_holder, ["holder_num", "holder_chg"]] = np.nan

    # ── 业绩预告 ──
    POS_TYPES = {"预增", "略增", "续盈", "扭亏", "减亏"}
    NEG_TYPES = {"预减", "略减", "首亏", "续亏", "增亏"}
    fc["code"] = fc["ts_code"].apply(_from_ts_code)
    fc.drop(columns=["ts_code"], inplace=True)
    fc["forecast_pos"] = fc["type"].isin(POS_TYPES).astype("float32")
    fc["forecast_neg"] = fc["type"].isin(NEG_TYPES).astype("float32")
    fc["forecast_pmin"] = pd.to_numeric(fc["p_change_min"], errors="coerce").astype("float32")
    df = _merge_aux_by_date(df, fc, "ann_date", ["forecast_pos", "forecast_neg", "forecast_pmin"])
    fc_last = fc.groupby("code")["ann_date"].max().rename("fc_last_ann")
    df = df.merge(fc_last, on="code", how="left")
    df["fc_last_ann"] = pd.to_datetime(df["fc_last_ann"], errors="coerce")
    stale_fc = (df["date"] - df["fc_last_ann"]).dt.days > AUX_MAX_AGE_DAYS
    df.loc[stale_fc, ["forecast_pos", "forecast_neg", "forecast_pmin"]] = np.nan
    df.drop(columns=["fc_last_ann"], inplace=True)

    # ── 质押比例 ──
    pl["code"] = pl["ts_code"].apply(_from_ts_code)
    pl.drop(columns=["ts_code"], inplace=True)
    pl["pledge_ratio"] = pd.to_numeric(pl["pledge_ratio"], errors="coerce").astype("float32")
    pl["end_date"] = pd.to_datetime(pl["end_date"])
    pl["pub_date"] = pl["end_date"] + pd.Timedelta(days=30)
    df = _merge_aux_by_date(df, pl, "pub_date", ["pledge_ratio"])

    # ── 机构龙虎榜近 5 日净买入 ──
    ti["code"] = ti["ts_code"].apply(_from_ts_code)
    ti.drop(columns=["ts_code"], inplace=True)
    ti["net_buy"] = pd.to_numeric(ti["net_buy"], errors="coerce")
    ti = ti.groupby(["code", "trade_date"], as_index=False)["net_buy"].sum()
    ti = ti.sort_values(["code", "trade_date"]).reset_index(drop=True)
    ti["inst_buy5"] = (ti.groupby("code")["net_buy"].rolling(5).sum()
                       .reset_index(level=0, drop=True).astype("float32"))
    df = _merge_aux_by_date(df, ti, "trade_date", ["inst_buy5"])

    logger.info(f"增强信号合并完成，耗时 {time.time()-t0:.1f}s")
    return df


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
        # 次日开盘入场变体（P3-13）：entry=open[t+1]，exit=close[t+p]，隔离纯入场价效应
        df[f"ret_{p}d_no"] = (g["close"].transform(lambda x: x.shift(-p))
                              / g["open"].transform(lambda x: x.shift(-1)) - 1)

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
# 动态退出收益（ATR 止损 + 移动止盈 + 时间止损，C2；P2-10 分组参数化）
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_EXIT_PARAMS = (2.5, 0.95)   # WFO Exp3 全局最优 (atr_mult, trail)

# 策略族差异化出场参数（P2-10）：趋势族宽止损防震出、反转族紧止损快认错、
# 涨停族超紧防回撤；未列出的策略用 DEFAULT_EXIT_PARAMS。
STRATEGY_EXIT_PARAMS: Dict[str, Tuple[float, float]] = {
    # 趋势/突破族：宽止损
    "ma_crossover": (3.0, 0.97),
    "volume_surge_std": (3.0, 0.97),
    "multi_ma_resonance": (3.0, 0.97),
    "wave_theory": (3.0, 0.97),
    "n_pattern": (3.0, 0.97),
    "ma_golden_cross": (3.0, 0.97),
    "volume_breakout": (3.0, 0.97),
    "monthly_macd_20ma": (3.0, 0.97),
    "ensemble": (3.0, 0.97),
    # 反转/超跌族：紧止损
    "wonderful_9_turn": (1.5, 0.92),
    "emotion_cycle": (1.5, 0.92),
    "one_yang_three_yin": (1.5, 0.92),
    "washout_break": (1.5, 0.92),
    "low_profit_hold": (1.5, 0.92),
    "chan_theory": (1.5, 0.92),
    "box_oscillation": (1.5, 0.92),
    "rsi_bullish_divergence": (1.5, 0.92),
    # 涨停族：超紧止损
    "dragon_head": (1.0, 0.95),
    "limit_up_pullback": (1.0, 0.95),
    "stable_then_limit_up": (1.0, 0.95),
    "low_position_limit_up": (1.0, 0.95),
}


def _exit_param_key(atr_mult: float, trail: float) -> str:
    """参数组 → 列名后缀键（生成与选择两侧必须使用同一格式）。"""
    return f"a{atr_mult:g}_t{trail:g}"


def compute_dynamic_exit_returns(df: pd.DataFrame,
                                 atr_mult: float = DEFAULT_EXIT_PARAMS[0],
                                 trail: float = DEFAULT_EXIT_PARAMS[1],
                                 entry_timing: str = "close") -> pd.DataFrame:
    """按股票分组计算各持有期的动态退出收益,替换固定持有期收益。

    对每个持有期 p,入场日在持有期内逐日检查:
      - ATR 止损:某日 low <= entry - atr_mult*atr20[t],以止损价退出
      - 移动止盈:某日 close <= 持有期最高价*trail,以回撤位退出
      - 时间止损:持有 p 日仍未触发,按 close[t+p] 退出
    输出列 dyn_ret_{p}d(与 ret_{p}d 同口径的浮点收益)。
    按 code 分组,组内对每个交易日一次性计算 max(HOLDING_PERIODS)
    天的窗口并复用,避免逐持有期重复扫描。

    默认参数 (2.5, 0.95) 来自 WFO 实验 result/wfo_experiments_20260820.md
    Exp3 sweep（3x3 网格中胜率 53.1% 最高）；P2-10 起支持按策略族传入
    不同参数，非默认组的输出列由调用方追加 __a{atr}_t{trail} 后缀区分。

    entry_timing（P3-13）："close"=信号日收盘入场（默认）；"next_open"=次日
    开盘入场——仅替换入场价为 open[t+1]（止损基准同步改为 entry），风险窗口
    与出场日保持与 close 口径一致，隔离纯入场价效应。atr 取信号日值，无前视。
    """
    max_p = max(HOLDING_PERIODS)
    # 用位置索引计算并按逆置换还原行序：df 原始 index 可能非唯一（多次
    # concat/merge 后常见），直接 out.reindex(df.index) 会按重复标签错位，导致
    # dyn_ret 的 NaN 模式漂移、回测交易数不确定。这里排序后全程用 0..n-1 位置，
    # 返回时按 sorted->original 的逆位置映射还原。
    sort_order = np.lexsort((df["date"].to_numpy(), df["code"].to_numpy()))
    inv_order = np.empty_like(sort_order)
    inv_order[sort_order] = np.arange(len(sort_order))
    df_sorted = df.iloc[sort_order].reset_index(drop=True)
    out = pd.DataFrame(index=df_sorted.index, dtype="float32")
    for code, g in df_sorted.groupby("code", sort=False):
        n = len(g)
        close = g["close"].to_numpy(dtype=np.float64)
        op = g["open"].to_numpy(dtype=np.float64)
        high = g["high"].to_numpy(dtype=np.float64)
        low = g["low"].to_numpy(dtype=np.float64)
        atr = g["atr20"].to_numpy(dtype=np.float64)
        rets = {p: np.full(n, np.nan, dtype=np.float32) for p in HOLDING_PERIODS}

        for i in range(n - 1):
            if entry_timing == "next_open":
                if i + 1 >= n:
                    break
                entry = op[i + 1]
            else:
                entry = close[i]
            stop = entry - atr_mult * atr[i]
            if not np.isfinite(stop) or not np.isfinite(entry) or entry <= 0:
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
                    # 移动止盈触发日按 peak*trail 退出（修复：原硬编码 0.95，
                    # 导致 per-strategy trail 参数 0.97/0.92 在该分支失效）
                    exit_price = stop if atr_hit[k] else peaks[k] * trail
                else:
                    exit_price = close[i + p]
                if entry > 0:
                    rets[p][i] = (exit_price / entry - 1.0)

        for p, arr in rets.items():
            out.loc[g.index, f"dyn_ret_{p}d"] = arr

    # 按逆置换还原到原 df 行序（位置对齐，规避非唯一 index 的 reindex 错位）
    out = out.iloc[inv_order].reset_index(drop=True)
    out.index = df.index
    return out


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


def _entry_mask(df: pd.DataFrame, enhanced: bool = False) -> pd.Series:
    """统一的买入可成交性掩码:剔除涨跌停日,并叠加市场环境、
    主力资金、市值、量比、财务质量过滤(回测/验证/推荐共用)。

    enhanced=True 时使用 market_ok_enh(放宽 regime, P0-1)，
    False 时使用 market_ok(严格 regime)。
    """
    regime_col = "market_ok_enh" if enhanced else "market_ok"
    mask = ~_is_limit_up(df) & ~_is_limit_down(df)
    if regime_col in df.columns:
        mask &= df[regime_col].fillna(True).astype(bool)
    mask &= _moneyflow_ok(df) & _size_ok(df) & _volume_ratio_ok(df) & _financial_ok(df)
    return mask


def _apply_cooldown(df: pd.DataFrame, sig: pd.Series, cooldown_days: int = SIGNAL_COOLDOWN_DAYS,
                    enhanced: bool = False) -> pd.Series:
    """对信号施加冷却期:同一股票同一策略 N 日内只取第一次信号。

    按 code 分组,保留信号后 N 日内的后续信号被抑制,降低样本
    自相关与同一股票重复贡献,使胜率统计更真实(C3)。
    enhanced=True 时使用 market_ok_enh(放宽 regime, P0-1)。
    """
    sig = sig & _entry_mask(df, enhanced=enhanced)
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


def _apply_resonance(df: pd.DataFrame, sig_dict: Dict[str, pd.Series],
                     min_strategies: int = MIN_RESONANCE_STRATEGIES) -> Dict[str, pd.Series]:
    """同一股票同日至少 min_strategies 个策略命中才保留信号（P0-2）。

    基于各策略冷却期后的布尔信号逐行求和，将命中数不足门槛的 (code, date)
    在所有策略中统一置 False。min_strategies <= 1 时原样返回（不启用）。
    所有 sig 必须与 df 行对齐（来自 _strategy_signal 的同源信号）。
    """
    if min_strategies <= 1 or not sig_dict:
        return sig_dict

    hit = np.zeros(len(df), dtype=np.int16)
    for sig in sig_dict.values():
        hit += sig.astype(bool).to_numpy()
    keep = pd.Series(hit >= min_strategies, index=df.index)
    return {name: sig & keep for name, sig in sig_dict.items()}


# ═══════════════════════════════════════════════════════════════════════════════
# 策略族质量过滤（P2：按策略特性差异化应用增强数据过滤，数据缺失时放行）
# ═══════════════════════════════════════════════════════════════════════════════

def _quality_mask(df: pd.DataFrame, kind: str) -> pd.Series:
    """按策略族返回质量过滤掩码。

    族定义：
      - standard：趋势/突破族——换手率带 + 获利盘带 + 质押上限 + 股息率下限
      - washout ：超跌/反抽族——换手率带 + 质押上限 + 股息率下限（不含获利盘带，
                  因信号本身要求获利盘 < 0.2，两者互斥）
      - limitup_break ：涨停启动族——质押上限 + 涨停日筹码集中度上限
      - limitup_pullback：涨停回调族——换手率带 + 质押上限
      - chase   ：龙头接力族——换手率带 + 质押上限（获利盘条件内置于信号）
      - base    ：无额外过滤（沿用原买入掩码）
    """
    trf = df["turnover_rate_f"]
    pr = df["profit_ratio"]
    pl = df["pledge_ratio"]
    dv = df["dv_ratio"]
    cc90 = df["concentration_90"]

    trf_ok = (trf >= MIN_TURNOVER_RATE_F) & (trf <= MAX_TURNOVER_RATE_F)
    pledge_ok = pl.isna() | (pl <= MAX_PLEDGE_RATIO)

    if kind == "standard":
        return (trf_ok & (pr >= MIN_PROFIT_RATIO) & (pr <= MAX_PROFIT_RATIO)
                & pledge_ok & (dv >= MIN_DIV_YIELD))
    if kind == "washout":
        return trf_ok & pledge_ok & (dv >= MIN_DIV_YIELD)
    if kind == "limitup_break":
        return pledge_ok & (cc90 < MAX_CONC_90_LIMITUP)
    if kind == "limitup_pullback":
        return trf_ok & pledge_ok
    if kind == "chase":
        # 涨停次日高开日换手率天然偏高，仅用质押过滤（获利盘/集中度条件内置于信号）
        return pledge_ok
    return pd.Series(True, index=df.index)


STRATEGY_MASKS: Dict[str, str] = {
    "ma_crossover": "standard",
    "volume_surge_std": "standard",
    "wonderful_9_turn": "standard",
    "n_pattern": "standard",
    "limit_up_pullback": "limitup_pullback",
    "stable_then_limit_up": "limitup_break",
    "monthly_macd_20ma": "standard",
    "low_position_limit_up": "base",
    "multi_ma_resonance": "standard",
    "ensemble": "standard",
    "volume_breakout": "standard",
    "ma_golden_cross": "standard",
    "dragon_head": "chase",
    "emotion_cycle": "standard",
    "one_yang_three_yin": "washout",
    "box_oscillation": "standard",
    "wave_theory": "standard",
    "chan_theory": "base",
    "washout_break": "washout",
    "low_profit_hold": "washout",
    "holder_conc_break": "standard",
    "inst_smart_break": "standard",
    "fc_pos_break": "standard",
    "rsi_bullish_divergence": "standard",
}


# 策略按市况分族（P1-4）：仅列出的策略在对应市况下激活，全部 23 策略全覆盖。
# 同一策略可属多个市况；未启用 --regime-filter 时该表不生效。
REGIME_STRATEGIES: Dict[str, List[str]] = {
    "bull": [
        "ma_crossover", "volume_surge_std", "multi_ma_resonance",
        "wave_theory", "n_pattern", "ma_golden_cross",
        "volume_breakout", "monthly_macd_20ma", "ensemble", "dragon_head",
    ],
    "range": [
        "wonderful_9_turn", "emotion_cycle", "one_yang_three_yin",
        "box_oscillation", "washout_break", "low_profit_hold",
        "chan_theory", "limit_up_pullback", "stable_then_limit_up",
        "low_position_limit_up", "rsi_bullish_divergence",
    ],
    "bear": [
        "low_profit_hold", "holder_conc_break", "fc_pos_break",
        "box_oscillation", "inst_smart_break", "rsi_bullish_divergence",
    ],
}

STRATEGY_ALLOWED_REGIMES: Dict[str, set] = {}
for _regime, _names in REGIME_STRATEGIES.items():
    for _name in _names:
        STRATEGY_ALLOWED_REGIMES.setdefault(_name, set()).add(_regime)


def _strategy_signal(df: pd.DataFrame, name: str, enhanced: bool = False,
                     regime_filter: bool = False,
                     industry_filter: bool = False) -> pd.Series:
    """策略信号 = 信号函数 & 买入掩码 & 该策略族的质量过滤（回测/验证/推荐共用）。

    enhanced=True 时使用 market_ok_enh(放宽 regime, P0-1)；
    regime_filter=True 时按 STRATEGY_ALLOWED_REGIMES 过滤当前市况下不允许的策略(P1-4)，
    regime 列缺失时放行（不误杀）；
    industry_filter=True 时仅保留行业动量前 INDUSTRY_TOP_N 的信号(P2-7)，
    ind_rank 缺失(NaN=无映射/无排名)时放行。
    """
    sig = STRATEGIES[name](df)
    mask = sig & _entry_mask(df, enhanced=enhanced) & _quality_mask(df, STRATEGY_MASKS.get(name, "base"))
    if regime_filter and "regime" in df.columns:
        allowed = STRATEGY_ALLOWED_REGIMES.get(name)
        if allowed:
            mask = mask & df["regime"].isin(allowed).fillna(False)
    if industry_filter and "ind_rank" in df.columns:
        mask = mask & (df["ind_rank"].isna() | (df["ind_rank"] <= INDUSTRY_TOP_N))
    return mask


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


def _wonderful_9_score(df):
    """神奇九转打分（供 sig_wonderful_9_turn 与强度注册表共用），满分 17。"""
    close = df["close"]
    streak = close.groupby(df["code"]).transform(
        lambda x: (x < x.shift(4)).rolling(9, min_periods=9).min().fillna(0).astype(bool)
    )
    mp = df.groupby("code")["macd_hist"].shift(1)
    m20p = df.groupby("code")["ma20"].shift(1)
    m60p = df.groupby("code")["ma60"].shift(1)
    return (streak.astype(int) * 4 +
            (df["rsi6"] < 35).astype(int) * 3 +
            ((df["macd_hist"] > 0) & (df["macd_hist"] < mp)).astype(int) * 3 +
            (df["volume"] > df["vol_ma5"] * 1.2).astype(int) * 2 +
            ((df["ma20"] > m60p) & (df["ma20"] > m20p) & (df["ma60"] > m60p)).astype(int) * 2 +
            (close >= df["ma20"] * 0.98).astype(int) * 2 +
            (close > df["open"]).astype(int))


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
    return _wonderful_9_score(df) >= 10


def _n_pattern_score(df):
    """N 字形态打分（供 sig_n_pattern 与强度注册表共用），满分 15。"""
    sh = df.groupby("code")["high"].transform(lambda x: x.shift(1).rolling(5, min_periods=1).max())
    return ((df["close"] > sh).astype(int) * 6 +
            (df["volume"] > df["vol_ma5"]).astype(int) * 4 +
            (df["close"] > df["open"]).astype(int) * 3 +
            (df["close"] > df["ma20"]).astype(int) * 2)


def sig_n_pattern(df):
    """策略4：N 字形态——价格突破 5 日最高点形成 N 字中继突破。

    打分权重：
      收盘突破前 5 日最高价   +6
      放量（量 > vol_ma5）    +4
      阳线                   +3
      站上 MA20              +2
    总分 >= 13 触发信号。
    """
    return _n_pattern_score(df) >= 13


def _limit_up_pullback_score(df):
    """涨停回调打分（供 sig_limit_up_pullback 与强度注册表共用），满分 14。"""
    pct = df["pct_chg"]
    # hi/vl：以 ffill 方式回溯最近一次涨停日的最高价与成交量，作为回踩基准
    hi = df["high"].where(pct >= 9.5).groupby(df["code"]).ffill().fillna(0)
    vl = df["volume"].where(pct >= 9.5).groupby(df["code"]).ffill().fillna(0.1)
    pct_prev = df.groupby("code")["pct_chg"].shift(1).fillna(0)
    return (pct_prev >= 9.5).astype(int) * 2 + \
           (df["volume"] / vl.replace(0, 0.1) < 0.5).astype(int) * 3 + \
           (df["close"] >= hi * 0.97).astype(int) * 3 + \
           (df["volume"] > df["vol_ma5"] * 1.5).astype(int) * 4 + \
           (pct > 0).astype(int) * 2


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
    return _limit_up_pullback_score(df) >= 8


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


def _monthly_macd_score(df):
    """月线 MACD + 20 日均线打分（供 sig_monthly_macd_20ma 与强度注册表共用），满分 20。"""
    dp = df.groupby("code")["macd_dif"].shift(1)
    dep = df.groupby("code")["macd_dea"].shift(1)
    m20p = df.groupby("code")["ma20"].shift(1)
    return (((df["macd_dif"] > df["macd_dea"]) & (dp <= dep)).astype(int) * 5 +
            (df["ma20"] > m20p).astype(int) * 4 +
            (df["close"] >= df["ma20"] * 0.97).astype(int) * 3 +
            (df["volume"] > df["vol_ma5"] * 1.5).astype(int) * 4 +
            (df["close"] > df["open"]).astype(int) * 2 +
            ((df["rsi6"] > 40) & (df["rsi6"] < 70)).astype(int) * 2)


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
    return _monthly_macd_score(df) >= 10


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


def _multi_ma_resonance_score(df):
    """多均线共振打分（供 sig_multi_ma_resonance 与强度注册表共用），满分 21。"""
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
    return (bull_ma.astype(int) * 4 +
            cross_5_10.astype(int) * 4 +
            (df["volume"] > df["vol_ma20"] * 1.5).astype(int) * 3 +
            ((df["macd_dif"] > df["macd_dea"]) & (dp2 <= dep2)).astype(int) * 3 +
            ((df["macd_hist"] > 0) & (mp <= 0)).astype(int) * 2 +
            ((df["rsi6"] > r6p) & (df["rsi12"] > r12p) & (df["rsi24"] > r24p) &
             (df["rsi6"] < 70)).astype(int) * 3 +
            ((df["boll_upper"] - df["boll_mid"]) > (bup - bmp)).astype(int) * 2)


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
    return _multi_ma_resonance_score(df) >= 10


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


@deprecated("该函数已废弃，胜率无法通过过滤提升（41.9% 为大盘组最低）")
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


@deprecated("该函数已废弃，缩量条件与买入掩码量比过滤矛盾导致 5 年零信号")
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
      - 获利盘 <= 90%（剔除纯高位接力）
      - 90% 筹码集中度 < 15%（筹码集中，未过度分散）
    """
    pp = df.groupby("code")["pct_chg"].shift(1)
    pc = df.groupby("code")["close"].shift(1)
    return (pp >= 9.5) & (df["open"] > pc * 1.02) \
        & (df["profit_ratio"] <= MAX_DRAGON_PROFIT_RATIO) \
        & (df["concentration_90"] < MAX_CONC_90_LIMITUP)


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


def _wave_theory_score(df):
    """波浪理论打分（供 sig_wave_theory 与强度注册表共用），满分 12。"""
    breakout = df["close"] > df["high_20d_max"]
    vol_surge = df["volume"] > df["vol_ma5"] * 1.5
    rsi_strong = (df["rsi6"] > 45) & (df["rsi6"] < 75)
    macd_bullish = df["macd_hist"] > 0
    price_above_ma20 = df["close"] > df["ma20"]
    return (breakout.astype(int) * 4 + vol_surge.astype(int) * 3 + rsi_strong.astype(int) * 2
            + macd_bullish.astype(int) * 2 + price_above_ma20.astype(int))


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
    return _wave_theory_score(df) >= 8


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


def sig_washout_break(df):
    """策略24：超跌突破——获利盘 < 20%（筹码深度套牢）后放量阳线站上 MA20，洗盘后的反转突破。

    逻辑（全部满足）：
      - 获利盘 < 20%（绝大部分筹码被套，抛压枯竭）
      - 阳线（收 > 开）
      - 放量（量 > 1.5*vol_ma5）
      - 收盘站上 MA20
    """
    return ((df["profit_ratio"] < WASHOUT_PROFIT_RATIO) & (df["close"] > df["open"])
            & (df["volume"] > df["vol_ma5"] * 1.5) & (df["close"] > df["ma20"]))


def sig_low_profit_hold(df):
    """策略25：超跌+筹码集中——获利盘 < 20% 且股东人数下降（筹码向少数人集中），阳线站上 MA20。

    逻辑（全部满足）：
      - 获利盘 < 20%（深度套牢）
      - 最近一期股东人数环比下降（筹码集中）
      - 收盘站上 MA20
      - 阳线（收 > 开）
    """
    return ((df["profit_ratio"] < WASHOUT_PROFIT_RATIO) & (df["holder_chg"] < 0)
            & (df["close"] > df["ma20"]) & (df["close"] > df["open"]))


def sig_holder_conc_break(df):
    """策略26：股东人数下降突破——股东人数环比下降超 3%（显著集中）+ 放量阳线站上 MA20。

    逻辑（全部满足）：
      - 股东人数环比下降 > 3%
      - 收盘站上 MA20
      - 阳线（收 > 开）
      - 放量（量 > 1.2*vol_ma5）
    """
    return ((df["holder_chg"] < MAX_HOLDER_CHG_PCT / 100.0) & (df["close"] > df["ma20"])
            & (df["close"] > df["open"]) & (df["volume"] > df["vol_ma5"] * 1.2))


def sig_inst_smart_break(df):
    """策略27：机构龙虎榜净买——机构席位近 5 日累计净买入超 500 万，阳线站上 MA20。

    逻辑（全部满足）：
      - 机构龙虎榜近 5 日累计净买入 > 500 万元
      - 收盘站上 MA20
      - 阳线（收 > 开）
    """
    return ((df["inst_buy5"] > MIN_INST_NET_BUY) & (df["close"] > df["ma20"])
            & (df["close"] > df["open"]))


def sig_fc_pos_break(df):
    """策略28：业绩预增突破——最新业绩预告为正向（预增/略增/续盈/扭亏/减亏），量价配合站上 MA20。

    逻辑（全部满足）：
      - 最近一期业绩预告为正向类型
      - 收盘站上 MA20
      - 放量（量 > 1.2*vol_ma5）
      - RSI6 处于 40~70 健康区间
    """
    return ((df["forecast_pos"] == 1) & (df["close"] > df["ma20"])
            & (df["volume"] > df["vol_ma5"] * 1.2)
            & (df["rsi6"] > 40) & (df["rsi6"] < 70))


def sig_smart_money_flow(df):
    """策略29：主力分层持续流入（P2-9 已废弃，2026-08-24 实测胜率 37.8% 弱于其余策略）。

    曾逻辑：net_big_amount（大单+超大单净额）连续 3 日 > 0、小单净流出、
    收盘站上 MA20 且阳线。保留函数定义仅便于对照与回滚；不再注册进 STRATEGIES。
    数据缺失（net_big_amount 列不存在）时返回全 False，不误触发。
    """
    if "net_big_amount" not in df.columns:
        return pd.Series(False, index=df.index)
    net_big = df["net_big_amount"]
    g = df.groupby("code")["net_big_amount"]
    big_3d = ((net_big > 0)
              & (g.shift(1) > 0)
              & (g.shift(2) > 0))
    retail_out = df["buy_sm_amount"] < df["sell_sm_amount"]
    return (big_3d & retail_out
            & (df["close"] > df["ma20"]) & (df["close"] > df["open"]))


def _rsi_bullish_divergence_score(df):
    """RSI 底背离打分（供 sig_rsi_bullish_divergence 与强度注册表共用），满分 13。"""
    low_20 = df.groupby("code")["close"].transform(
        lambda x: x.shift(1).rolling(20, min_periods=1).min())
    rsi_low_20 = df.groupby("code")["rsi6"].transform(
        lambda x: x.shift(1).rolling(20, min_periods=1).min())
    return ((df["close"] < low_20).astype(int) * 4 +
            (df["rsi6"] >= rsi_low_20).astype(int) * 4 +
            (df["volume"] > df["vol_ma5"] * 1.2).astype(int) * 3 +
            (df["close"] > df["open"]).astype(int) * 2)


def sig_rsi_bullish_divergence(df):
    """策略30：RSI 底背离（P2-8）——价格创 20 日新低但 RSI 未创新低，下跌动能衰竭。

    打分权重：
      价格创 20 日新低（不含当日的 20 日最低收盘）  +4
      RSI6 未创新低（RSI6 >= 前 20 日最低 RSI6）    +4
      放量（量 > 1.2*vol_ma5）                      +3
      阳线（close > open）                          +2
    总分 >= 9 触发信号。
    """
    return _rsi_bullish_divergence_score(df) >= 9


# 策略注册表：策略名 -> 信号函数。回测、验证、推荐均通过该表调度。
# 各策略的差异化质量过滤见 STRATEGY_MASKS（P2）。
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
    # "bull_trend": sig_bull_trend,
    "ma_golden_cross": sig_ma_golden_cross,
    # "shrink_pullback": sig_shrink_pullback,
    "dragon_head": sig_dragon_head,
    "emotion_cycle": sig_emotion_cycle,
    # "bottom_volume": sig_bottom_volume,
    "one_yang_three_yin": sig_one_yang_three_yin,
    "box_oscillation": sig_box_oscillation,
    "wave_theory": sig_wave_theory,
    "chan_theory": sig_chan_theory,
    "washout_break": sig_washout_break,
    "low_profit_hold": sig_low_profit_hold,
    "holder_conc_break": sig_holder_conc_break,
    "inst_smart_break": sig_inst_smart_break,
    "fc_pos_break": sig_fc_pos_break,
    # "smart_money_flow": sig_smart_money_flow,   # 2026-08-24 废弃：胜率 37.8% 弱
    "rsi_bullish_divergence": sig_rsi_bullish_divergence,
}


# 信号强度注册表（P1-5）：策略名 -> 归一化打分函数（0~1，满分归一）。
# 未注册的策略触发时强度恒为 1.0；仅用于推荐排序加权，不影响信号判定。
STRATEGY_SCORE_FUNCS: Dict[str, Callable] = {
    "ma_crossover": lambda df: (_ma_cross(df) / 11.0).clip(0, 1),
    "volume_surge_std": lambda df: (_vol_surge(df) / 12.0).clip(0, 1),
    "wonderful_9_turn": lambda df: (_wonderful_9_score(df) / 17.0).clip(0, 1),
    "n_pattern": lambda df: (_n_pattern_score(df) / 15.0).clip(0, 1),
    "limit_up_pullback": lambda df: (_limit_up_pullback_score(df) / 14.0).clip(0, 1),
    "monthly_macd_20ma": lambda df: (_monthly_macd_score(df) / 20.0).clip(0, 1),
    "multi_ma_resonance": lambda df: (_multi_ma_resonance_score(df) / 21.0).clip(0, 1),
    "wave_theory": lambda df: (_wave_theory_score(df) / 12.0).clip(0, 1),
    "rsi_bullish_divergence": lambda df: (_rsi_bullish_divergence_score(df) / 13.0).clip(0, 1),
}


def _signal_strength(df: pd.DataFrame, name: str) -> pd.Series:
    """返回与 df 行对齐的信号强度序列（0~1）；未注册打分的策略恒为 1.0。"""
    func = STRATEGY_SCORE_FUNCS.get(name)
    if func is None:
        return pd.Series(1.0, index=df.index)
    return func(df)


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


def calc_portfolio_metrics(dates: pd.Series, returns: np.ndarray,
                           avg_holding: float) -> Optional[Dict[str, float]]:
    """日历日等权组合复利净值口径的绩效指标（与基准 compute_benchmark_metrics 同口径）。

    策略信号是选股信号、持有期重叠，不能对逐笔收益直接 cumprod（会把重叠信号
    重复复利成虚高净值），也不能简单累加（旧口径：总收益=Σ单笔、回撤在累加曲线
    上算且被 clip 成 100%、年化用串行笔数折算）。这里把每笔信号的持有期收益
    均匀分摊到其持有期内的每个交易日，按日历日对齐后取当日所有持仓的等权平均
    作为组合日收益，再用复利净值 cumprod 计算总收益/年化/最大回撤/夏普——
    与基准侧日频收益 cumprod 口径一致，正确反映重叠持仓与真实时间跨度。

    Args:
        dates: 与 returns 对齐的信号触发日（pd.Series / DatetimeIndex）。
        returns: 单笔持有期净收益率（小数，已扣交易成本）。
        avg_holding: 该持有期交易日数，用于把单笔收益分摊到日。

    Returns:
        含 total_return / annualized_return / max_drawdown / sharpe_ratio 的字典；
        有效信号不足或无日期时返回 None（调用方回退逐笔口径）。
    """
    r = np.asarray(returns, dtype=float).flatten()
    if dates is None or len(r) == 0 or avg_holding is None or avg_holding <= 0:
        return None

    d = pd.to_datetime(pd.Series(dates).values, errors="coerce")
    mask = (~np.isnan(r)) & (np.abs(r) < 5) & pd.notna(d)
    r = r[mask]
    d = d[mask]
    if len(r) < 2:
        return None

    # 全部交易日历（信号日的并集，按日排序）
    trade_days = pd.DatetimeIndex(np.sort(pd.unique(d.values)))
    if len(trade_days) < 2:
        return None
    day_pos = {day: i for i, day in enumerate(trade_days)}
    n_days = len(trade_days)

    # 每笔信号把持有期收益均匀分摊到 avg_holding 个交易日：daily_contrib[i] += r/holding
    daily_sum = np.zeros(n_days, dtype=float)
    daily_cnt = np.zeros(n_days, dtype=int)
    per_day = r / float(avg_holding)
    max_forward = int(avg_holding)
    for sig_day, contrib in zip(d, per_day):
        start = day_pos.get(pd.Timestamp(sig_day))
        if start is None:
            continue
        end = min(start + max_forward, n_days)
        if end <= start:
            continue
        idx = np.arange(start, end)
        daily_sum[idx] += contrib
        daily_cnt[idx] += 1

    active = daily_cnt > 0
    if active.sum() < 2:
        return None
    # 当日等权组合收益 = 当日持仓分摊收益的平均（满仓等权，空仓日收益 0 不补）
    port_daily = np.zeros(n_days, dtype=float)
    port_daily[active] = daily_sum[active] / daily_cnt[active]

    eq = np.cumprod(1.0 + port_daily)
    total_return = float(eq[-1] - 1.0) * 100.0
    years = n_days / 252.0
    annualized = float(eq[-1] ** (1.0 / max(years, 1e-10)) - 1.0) * 100.0
    peak = np.maximum.accumulate(eq)
    max_dd = float(((peak - eq) / peak).max() * 100.0)
    std = float(np.std(port_daily))
    sharpe = float(np.mean(port_daily) / std * np.sqrt(252.0)) if std > 1e-12 else 0.0

    return {
        "total_return": round(total_return, 2),
        "annualized_return": round(annualized, 2),
        "max_drawdown": round(max_dd, 2),
        "sharpe_ratio": round(sharpe, 2),
    }


def calc_metrics(returns: np.ndarray, avg_holding: Optional[float] = None,
                 dates: Optional[pd.Series] = None) -> Dict:
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

    metrics = {
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

    # 组合级指标（总收益/年化/回撤/夏普）改用日历日等权组合复利净值口径，
    # 与基准 compute_benchmark_metrics 一致；逐笔指标（胜率/盈亏比/期望/凯利）保留。
    # dates 缺失（如 WFO/诊断旧调用）时回退逐笔累加口径。
    if dates is not None:
        port = calc_portfolio_metrics(dates, r, avg_holding)
        if port is not None:
            metrics["total_return"] = port["total_return"]
            metrics["annualized_return"] = port["annualized_return"]
            metrics["max_drawdown"] = port["max_drawdown"]
            metrics["sharpe_ratio"] = port["sharpe_ratio"]
            metrics["portfolio_metric"] = "daily_equal_weight_compound"

    return metrics


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

def _backtest_single(name: str, df: pd.DataFrame, sig: Optional[pd.Series] = None,
                     select_period_by: str = "total_return",
                     entry_timing: str = "close",
                     industry_filter: bool = False) -> Dict:
    """单策略回测：生成信号 -> 剔除不可成交（涨停）信号 -> 按持有期分别统计绩效。

    sig 为 None 时内部按 _strategy_signal + _apply_cooldown 现算
    （保持 WFO/诊断脚本的旧调用兼容）；调用方预计算并传入 sig
    （含冷却期/共振过滤/增强 regime）时直接使用，不再重复过滤。
    entry_timing="next_open" 时优先读取 ret_{p}d_no / dyn_ret_{p}d_no
    （次日开盘入场列，P3-13），缺失该列的持有期跳过。

    修复要点：
      - P2-13：涨停封板日无法按收盘价买入，先剔除涨停信号
      - P0-4：各持有期收益分开统计，不再混合进同一序列（样本独立）
      - P0-2：每个持有期的收益按日期排序后再计算最大回撤（时间序列回撤）
    异常时返回 {"strategy": name, "error": ...}，不中断整体回测；
    无信号时返回全 0 指标。

    select_period_by：最优持有期（best_p）的选取口径，默认 "total_return"（保持
    历史行为）；WFO 回测传 "expectation"（单笔期望不受样本量影响，选期更稳）。
    """
    t0 = time.time()
    try:
        if sig is None:
            sig = _apply_cooldown(df, _strategy_signal(df, name, industry_filter=industry_filter))
        signals = df[sig.astype(bool)]
        n = signals["code"].count()
        if n == 0:
            return {"strategy": name, "total_trades": 0, "win_rate": 0,
                    "avg_win": 0, "avg_loss": 0, "profit_loss_ratio": 0,
                    "total_return": 0, "annualized_return": 0,
                    "max_drawdown": 0, "sharpe_ratio": 0, "time_s": round(time.time() - t0, 1)}

        # 按持有期分别统计（P0-4），每个持有期的收益按日期排序后计算（P0-2）
        # 优先使用动态退出收益（ATR止损/移动止盈）；P2-10 起非默认出场组的
        # 策略优先读取带 __a{atr}_t{trail} 后缀的分组列，缺失时回退默认列
        period_metrics: Dict[int, Dict] = {}
        params = STRATEGY_EXIT_PARAMS.get(name)
        use_alt = (params is not None and tuple(params) != DEFAULT_EXIT_PARAMS)
        key = _exit_param_key(*params) if use_alt else ""
        sfx = "_no" if entry_timing == "next_open" else ""
        for p in HOLDING_PERIODS:
            col = None
            if use_alt:
                cand = f"dyn_ret_{p}d__{key}{sfx}"
                if cand in signals.columns:
                    col = cand
            if col is None:
                for cand in (f"dyn_ret_{p}d{sfx}", f"ret_{p}d{sfx}"):
                    if cand in signals.columns:
                        col = cand
                        break
            if col is None:
                continue
            sub = signals[["date", col]].dropna()
            if len(sub) == 0:
                continue
            sub = sub.sort_values("date")
            # 扣减往返交易成本后再统计绩效
            net_ret = sub[col].values - TRADING_COST_PCT / 100.0
            m = calc_metrics(net_ret, avg_holding=p, dates=sub["date"])
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

        # 聚合：取最优持有期（默认总收益最高，WFO 传 expectation）作为该策略代表指标，同时保留各持有期明细
        best_p = max(period_metrics, key=lambda p: period_metrics[p].get(select_period_by, period_metrics[p]["total_return"]))
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
    # 用复利净值曲线(从 1.0 起步)计算最大回撤:不复用 _calc_max_drawdown,它是
    # 简单累加模型(从 r[0]≈0 起步),指数下跌时峰值趋近 0 会算出 >100% 的虚假回撤
    # (如 沪深300 总收益 -12.8% 却报 557.96%)。
    eq = np.cumprod(1.0 + daily)
    peak = np.maximum.accumulate(eq)
    max_drawdown = float(((peak - eq) / peak).max() * 100.0)

    return {
        "benchmark_return": round(total_return, 2),
        "benchmark_annualized": round(annualized, 2),
        "benchmark_max_drawdown": round(max_drawdown, 2),
    }


def _compute_ensemble_weights(component_results: Dict[str, Dict]) -> Dict[str, float]:
    """按组件策略历史胜率归一化计算组合策略权重（C1）。

    输入为各组件策略的回测结果 dict（键为组件名），
    返回 {组件名: 权重}。

    容错：报错或胜率<=0 的组件视为"本窗口无效"，**排除**后对剩余有效组件按胜率
    归一化（而不是赋权重 0——赋 0 会把该组件从 ensemble 打分中静默剔除、使加权分
    被其余组件拉低，导致权重塌缩如 ma_crossover=0.000）。全部组件无效时返回空
    dict，由 sig_ensemble 回退等权。
    """
    valid_wr: Dict[str, float] = {}
    for c in ENSEMBLE_COMPONENTS:
        r = component_results.get(c)
        if r is None or "error" in r:
            continue
        wr = float(r.get("win_rate", 0) or 0)
        if wr > 0:
            valid_wr[c] = wr
    if not valid_wr:
        logger.warning("组件策略均无有效胜率,组合策略退化为等权")
        return {}
    wr_sum = sum(valid_wr.values())
    weights = {c: w / wr_sum for c, w in valid_wr.items()}
    # 被排除的无效组件权重记 0 仅用于日志展示；sig_ensemble 按 .get(c,1.0) 兜底
    logger.info(
        "组合策略权重: "
        + ", ".join(f"{c}={weights.get(c, 0.0):.3f}" for c in ENSEMBLE_COMPONENTS)
        + (f"（无效组件已排除: {[c for c in ENSEMBLE_COMPONENTS if c not in valid_wr]}）"
           if len(valid_wr) < len(ENSEMBLE_COMPONENTS) else "")
    )
    return weights


def run_backtests(df_bt: pd.DataFrame, index_df: Optional[pd.DataFrame] = None,
                  enhanced_regime: bool = False,
                  resonance_min: int = 1,
                  regime_filter: bool = False,
                  industry_filter: bool = False,
                  entry_timing: str = "close") -> List[Dict]:
    """对全部 23 个策略执行回测，返回按期望值降序的有效结果列表。

    enhanced_regime=True 时使用 market_ok_enh（放宽 regime, P0-1）；
    resonance_min>1 时启用同股同日多策略共振过滤（P0-2）；
    regime_filter=True 时按市况分族调度策略（P1-4，需 df_bt 含 regime 列）；
    industry_filter=True 时仅保留行业动量前 N 行业的信号（P2-7，需 ind_rank 列）；
    entry_timing="next_open" 时以次日开盘入场列统计（P3-13）。

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

    # 预计算全部策略信号（含冷却期），组合策略先按等权回退生成（权重随后填充）
    global _ENSEMBLE_WEIGHTS
    _ENSEMBLE_WEIGHTS = {}
    cooled_signals: Dict[str, pd.Series] = {}
    for name in strategy_names:
        raw = _strategy_signal(df_bt, name, enhanced=enhanced_regime,
                               regime_filter=regime_filter,
                               industry_filter=industry_filter)
        cooled_signals[name] = _apply_cooldown(df_bt, raw, enhanced=enhanced_regime)

    # 先串行计算组合策略的组件,按其胜率填充 _ENSEMBLE_WEIGHTS(C1)
    component_results = {}
    for name in ENSEMBLE_COMPONENTS:
        r = _backtest_single(name, df_bt, cooled_signals[name],
                             entry_timing=entry_timing)
        component_results[name] = r
        results.append(r)
        if "error" not in r:
            logger.info(f"  [component] {r['strategy']}: {r['total_trades']} 笔, "
                        f"胜率{r['win_rate']:.1f}%, 总收益{r['total_return']:.1f}%")
    _ENSEMBLE_WEIGHTS = _compute_ensemble_weights(component_results)

    # 用真实权重重算组合策略信号 + 冷却期（组件权重依赖其回测胜率）
    if "ensemble" in strategy_names:
        ens_raw = _strategy_signal(df_bt, "ensemble", enhanced=enhanced_regime,
                                   regime_filter=regime_filter,
                                   industry_filter=industry_filter)
        cooled_signals["ensemble"] = _apply_cooldown(df_bt, ens_raw, enhanced=enhanced_regime)

    # 同股同日多策略共振过滤（P0-2）
    if resonance_min > 1:
        cooled_signals = _apply_resonance(df_bt, cooled_signals, min_strategies=resonance_min)

    remaining = [n for n in strategy_names if n not in ENSEMBLE_COMPONENTS]

    def _run_one(name):
        r = _backtest_single(name, df_bt, cooled_signals.get(name),
                             entry_timing=entry_timing)
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

    # 附加基准对比与超额收益字段（P1-8）+ 小样本标记
    for r in results:
        if "error" not in r:
            r["small_sample"] = r.get("total_trades", 0) < MIN_TRADES_FOR_RANKING
            if benchmark:
                r["benchmark_return"] = benchmark["benchmark_return"]
                r["benchmark_annualized"] = benchmark["benchmark_annualized"]
                r["benchmark_max_drawdown"] = benchmark["benchmark_max_drawdown"]
                r["excess_return"] = round(r["total_return"] - benchmark["benchmark_return"], 2)

    # 按期望值(每笔平均收益)降序排序,兼顾胜率与盈亏比(风险提示3)。
    # 小样本策略（< MIN_TRADES_FOR_RANKING 笔）统计意义不足，标记 small_sample 并
    # 统一排在样本充足策略之后，不参与头部排名/Top-N 验证与跨策略汇总平均。
    valid = sorted(
        [r for r in results if "error" not in r],
        key=lambda x: (1 if x.get("small_sample") else 0, x.get("expectation", -999)),
        reverse=True,
    )
    n_small = sum(1 for r in valid if r.get("small_sample"))
    logger.info(
        f"回测完成，耗时 {time.time()-t0:.1f}s（样本充足 {len(valid)-n_small} 个，"
        f"小样本 <{MIN_TRADES_FOR_RANKING} 笔 {n_small} 个不参与头部排名）"
    )
    return valid


# ═══════════════════════════════════════════════════════════════════════════════
# 最近5日验证
# ═══════════════════════════════════════════════════════════════════════════════

def validate_week(df_full, df_week, top_results, top_n=5, signals=None):
    """最近 5 个交易日的本周验证：对回测 Top-N 策略做实际买入/卖出收益统计。

    验证口径：
      - 买入日 = 区间第 5 个交易日（开盘买入）
      - 卖出日 = 区间最后 1 个交易日（收盘卖出）
      - 仅统计买入日触发信号且买卖价均存在的股票
    信号在完整历史 df_full 上计算后再按买入日筛选（修复：仅给 5 日切片会
    丢失 rolling 窗口与前一日数据，导致 wonderful_9_turn / stable_then_limit_up
    等策略信号系统性消失）；signals 为预计算的 {策略名: 信号Series} 时直接复用，
    否则回退到 df_full 上现算。
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
            if signals is not None and name in signals:
                sig = signals[name]
            else:
                sig = _strategy_signal(df_full, name)
            matched_rows = df_full.loc[sig.values].copy()
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

def print_results(results, val_results, backtest_start, backtest_end, market_ok_days=None):
    """以表格形式打印回测绩效汇总与 5 日验证 Top-5 结果。

    回测表新增"超额%"列（策略总收益 - 市场基准总收益，沪深300），
    并打印基准行，用于判断策略收益是否真正跑赢市场（alpha）。
    market_ok_days 为 (可开仓天数, 验证区间总天数)，非 None 时在
    5 日验证表下方输出市况提示，区分"空仓市况"与"策略无信号"。
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
        # 跨策略平均只用样本充足（>= MIN_TRADES_FOR_RANKING 笔）的策略，
        # 避免 1~10 笔的小样本策略（100% 胜率/虚高年化）污染汇总统计。
        rankable = [r for r in valid_results if not r.get("small_sample")]
        stat_base = rankable if rankable else valid_results
        print(f"有信号策略: {len(valid_results)}/{len(results)}"
              f"（其中样本充足 ≥{MIN_TRADES_FOR_RANKING} 笔 {len(rankable)} 个，小样本 {len(valid_results)-len(rankable)} 个）")
        print(f"平均胜率:   {np.mean([r['win_rate'] for r in stat_base]):.1f}%")
        print(f"平均总收益: {np.mean([r['total_return'] for r in stat_base]):.1f}%")
        print(f"平均年化:   {np.mean([r['annualized_return'] for r in stat_base]):.1f}%")
        # 基准对比（P1-8）：取首个结果携带的基准字段
        if valid_results and "benchmark_return" in valid_results[0]:
            br = valid_results[0]["benchmark_return"]
            ba = valid_results[0]["benchmark_annualized"]
            bd = valid_results[0].get("benchmark_max_drawdown", 0)
            above = sum(1 for r in stat_base if r.get("excess_return", 0) > 0)
            print(f"市场基准(沪深300): 总收益 {br:.1f}%  年化 {ba:.1f}%  最大回撤 {bd:.1f}%")
            print(f"跑赢基准策略: {above}/{len(stat_base)}（按样本充足策略统计）")

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
        if market_ok_days is not None:
            ok_n, total_n = market_ok_days
            suffix = "（全部为空仓市况，信号被市场环境过滤）" if ok_n == 0 else f"（{total_n - ok_n} 天为空仓市况）"
            print(f"市况提示: 验证区间 {total_n} 个交易日中 {ok_n} 天可开仓 (market_ok){suffix}")
        print("=" * 75)


# ═══════════════════════════════════════════════════════════════════════════════
# 胜率前10股票 & 推荐
# ═══════════════════════════════════════════════════════════════════════════════

def compute_position_size(atr20, close, kelly, max_position_pct: float = 0.25) -> float:
    """单笔建议仓位比例（P1-6）：半凯利 × ATR 波动率调整，上限 max_position_pct。

    - 波动率调整：目标日波动 2% / 实际日波动（ATR20/close 近似），高波动降仓
    - 半凯利：kelly(%)/100 * 0.5，取整凯利一半的保守口径
    - atr20/close/kelly 缺失、非有限或非正时返回 0.0（不建议建仓）
    """
    try:
        atr20_f = float(atr20) if atr20 is not None else 0.0
        close_f = float(close)
        kelly_f = float(kelly)
    except (TypeError, ValueError):
        return 0.0
    if not np.isfinite(atr20_f) or not np.isfinite(close_f) or close_f <= 0 or atr20_f <= 0:
        return 0.0
    daily_vol = atr20_f / close_f
    vol_adj = min(1.0, 0.02 / daily_vol) if daily_vol > 0 else 1.0
    kelly_adj = max(0.0, min(kelly_f / 100.0 * 0.5, max_position_pct))
    return round(min(kelly_adj * vol_adj, max_position_pct), 4)


def optimize_portfolio(stock_list: List[Dict], industry_map: pd.DataFrame,
                       max_positions: int = 10, max_per_industry: int = 2) -> List[Dict]:
    """组合层面优化（P3-12）：行业分散 + 持仓上限的贪心筛选。

    输入列表需已按优先级降序（如 sell_return）；按序贪心保留：
      - 同一申万 L1 行业最多 max_per_industry 只
      - 总持仓不超过 max_positions
      - 无行业映射的个股不受行业上限约束（避免映射缺失误杀）
    返回保持原顺序的筛选子集。
    """
    if not stock_list:
        return stock_list
    ind_of = (dict(zip(industry_map["code"], industry_map["l1_code"]))
              if industry_map is not None and not industry_map.empty else {})
    kept: List[Dict] = []
    ind_count: Dict[str, int] = {}
    for s in stock_list:
        if len(kept) >= max_positions:
            break
        ind = ind_of.get(s.get("code", ""))
        if ind is not None:
            if ind_count.get(ind, 0) >= max_per_industry:
                continue
            ind_count[ind] = ind_count.get(ind, 0) + 1
        kept.append(s)
    logger.info(f"组合优化(P3-12): {len(stock_list)}→{len(kept)} "
                f"(行业≤{max_per_industry}, 持仓≤{max_positions})")
    return kept


def get_top_stocks_by_win_rate(df_full, df_week, results, top_n=10, signals=None):
    """按本周实际收益对股票排序，返回 5 日验证收益最高的前 top_n 只股票。

    统计口径：
      - 对每个有信号的回测策略，取本周（买入日）触发的股票
      - 收益：优先使用该策略回测最优持有期（best_period）对应的前瞻收益
        （dyn_ret_{best_p}d，缺失时回退 ret_{best_p}d，与回测/动态出场规则
        同口径）；最优期 5/10 日超出 5 日验证窗口无法测量时，回退固定窗口
        收益（买入日开盘 ~ 最后交易日收盘）
      - 推荐持仓天数（recommended_hold_days）：命中策略 best_period 的
        胜率加权平均，四舍五入到最近候选持有期（HOLDING_PERIODS）
      - 同一股票被多个策略命中时合并：记录命中策略数、平均胜率、
        收益取各策略持仓期收益（不可测时用固定窗口收益）中的最大值
      - position_pct（P1-6）= 半凯利×ATR 波动调整的建议仓位，
        凯利取命中策略中的最大值
    信号在完整历史 df_full 上计算后再按买入日筛选（修复：仅给 5 日切片会
    丢失 rolling 窗口与前一日数据）；signals 为预计算的 {策略名: 信号Series}
    时直接复用，否则回退到 df_full 上现算。
    返回按 sell_return 降序的股票推荐列表。
    """
    stock_info: Dict[str, dict] = {}

    all_dates = sorted(df_week["date"].unique())
    if len(all_dates) < 5:
        logger.warning("验证区间不足5个交易日")
        return []

    buy_date = all_dates[-5]
    sell_date = all_dates[-1]

    # 买入/卖出价映射一次性构建，避免逐记录全表扫描
    buy_cols = ["name", "open"] + (["atr20"] if "atr20" in df_week.columns else [])
    buy_info = df_week.loc[df_week["date"] == buy_date].set_index("code")[buy_cols]
    sell_prices = df_week.loc[df_week["date"] == sell_date].set_index("code")["close"]
    strategy_kelly = {r["strategy"]: r.get("kelly", 0) for r in results}
    # 各策略回测选出的最优持有期（best_period∈HOLDING_PERIODS）
    strategy_best_period = {r["strategy"]: r.get("best_period") for r in results}

    for r in results:
        strategy_name = r["strategy"]
        win_rate = r.get("win_rate", 0)
        total_trades = r.get("total_trades", 0)
        best_p = strategy_best_period.get(strategy_name)

        if total_trades == 0:
            continue

        try:
            if signals is not None and strategy_name in signals:
                sig = signals[strategy_name]
            else:
                sig = _strategy_signal(df_full, strategy_name)
            mask = sig.values & (df_full["date"] == buy_date).values
            if mask.sum() == 0:
                continue
            # 该策略最优持有期对应的前瞻收益列（与 _backtest_single 同口径：
            # 分组出场参数列优先，再回退默认 dyn_ret / ret）
            fwd_col = None
            if best_p:
                params = STRATEGY_EXIT_PARAMS.get(strategy_name)
                use_alt = (params is not None and tuple(params) != DEFAULT_EXIT_PARAMS)
                key = _exit_param_key(*params) if use_alt else ""
                if use_alt:
                    cand = f"dyn_ret_{best_p}d__{key}"
                    if cand in df_full.columns:
                        fwd_col = cand
                if fwd_col is None:
                    for cand in (f"dyn_ret_{best_p}d", f"ret_{best_p}d"):
                        if cand in df_full.columns:
                            fwd_col = cand
                            break
            rec_cols = ["code", "name", "open", "close"] + (["atr20"] if "atr20" in df_full.columns else [])
            if fwd_col:
                rec_cols.append(fwd_col)
            matched = df_full.loc[mask, rec_cols].to_dict("records")
        except Exception:
            continue

        for record in matched:
            code = record.get("code", "")
            if not code:
                continue
            if code not in buy_info.index or code not in sell_prices.index:
                continue
            buy_name = buy_info.loc[code, "name"]
            buy_price = buy_info.loc[code, "open"]
            # 固定窗口收益（原口径）：买入日开盘 ~ 最后交易日收盘，仅在持仓期
            # 前瞻收益不可测（最优期 5/10 日超出验证窗口）时作为兜底
            fixed_ret = (sell_prices.loc[code] / buy_price - 1) if buy_price > 0 else 0
            # 该策略最优持有期对应的前瞻收益（含动态出场，与回测同口径）；
            # 超出验证窗口无法测量时为 None
            fwd_ret = None
            if fwd_col:
                v = record.get(fwd_col)
                if v is not None and pd.notna(v):
                    fwd_ret = float(v)

            if code not in stock_info:
                stock_info[code] = {
                    "code": code,
                    "name": buy_name,
                    "strategies": [],
                    "win_rates": [],
                    "best_periods": [],
                    "best_period_weights": [],
                    "total_trades_list": [],
                    "buy_date": buy_date,
                    "buy_price": buy_price,
                    "sell_date": sell_date,
                    "sell_price": sell_prices.loc[code],
                    "sell_return": None,
                    "atr20": record.get("atr20"),
                    "max_kelly": 0.0,
                }

            # 收益取各策略持仓期收益（不可测时用固定窗口收益）中的最大值
            ret_this = fwd_ret if fwd_ret is not None else fixed_ret
            cur = stock_info[code]["sell_return"]
            if cur is None or ret_this > cur:
                stock_info[code]["sell_return"] = ret_this

            stock_info[code]["strategies"].append(strategy_name)
            stock_info[code]["win_rates"].append(win_rate)
            if best_p:
                stock_info[code]["best_periods"].append(best_p)
                stock_info[code]["best_period_weights"].append(win_rate)
            stock_info[code]["total_trades_list"].append(total_trades)
            stock_info[code]["max_kelly"] = max(
                stock_info[code]["max_kelly"], strategy_kelly.get(strategy_name, 0))

    stock_list = []
    for code, info in stock_info.items():
        if not info["strategies"]:
            continue
        avg_win_rate = sum(info["win_rates"]) / len(info["win_rates"])
        # 推荐持仓天数 = 命中策略最优持有期的胜率加权平均，四舍五入到最近候选期
        recommended_hold_days = None
        if info["best_periods"]:
            total_w = sum(info["best_period_weights"])
            if total_w > 0:
                weighted = sum(p * w for p, w in
                               zip(info["best_periods"], info["best_period_weights"])) / total_w
            else:
                weighted = sum(info["best_periods"]) / len(info["best_periods"])
            recommended_hold_days = min(HOLDING_PERIODS, key=lambda p: abs(p - weighted))
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
            "recommended_hold_days": recommended_hold_days,
            "position_pct": compute_position_size(
                atr20=info.get("atr20"), close=info["buy_price"], kelly=info["max_kelly"]),
        })

    stock_list.sort(key=lambda x: (x["sell_return"] if x["sell_return"] is not None else -999), reverse=True)

    # handoff 实盘门槛：仅把"验证期收益为正 + 足够共振 + 命中策略有统计意义"的股票
    # 自动送入主程序实盘分析，避免把验证期已亏损（如 -2.81%）或仅 1~2 个小样本策略
    # 命中的股票自动实盘化。
    #   - sell_return > 0：验证窗口内实际收益为正
    #   - robust_strategy_count >= HANDOFF_MIN_STRATEGIES：由历史样本充足
    #     （>= HANDOFF_MIN_STRATEGY_TRADES 笔）的策略命中数达到共振门槛
    qualified = []
    filtered_out = []
    for s in stock_list:
        ret_ok = s["sell_return"] is not None and pd.notna(s["sell_return"]) and s["sell_return"] > 0
        robust_count = sum(
            1 for t in s.get("total_trades_list", [])
            if t is not None and t >= HANDOFF_MIN_STRATEGY_TRADES
        )
        s["robust_strategy_count"] = robust_count
        if ret_ok and robust_count >= HANDOFF_MIN_STRATEGIES:
            qualified.append(s)
        else:
            reasons = []
            if not ret_ok:
                reasons.append(f"验证期收益{s['sell_return']}<=0")
            if robust_count < HANDOFF_MIN_STRATEGIES:
                reasons.append(f"有效共振策略{robust_count}<{HANDOFF_MIN_STRATEGIES}")
            filtered_out.append((s["code"], s["name"], "; ".join(reasons)))

    if filtered_out:
        logger.info(
            f"handoff 门槛过滤：{len(filtered_out)} 只股票未达实盘标准"
            f"（收益>0 且 有效策略数≥{HANDOFF_MIN_STRATEGIES}），不送入主程序："
        )
        for code, name, reason in filtered_out[:top_n]:
            logger.info(f"  - {code} {name}: {reason}")

    return qualified[:top_n]


def get_unique_strategies_from_results(results, top_n=10):
    """取回测结果前 top_n 个有交易记录的策略名（去重顺序列表）。"""
    return [r["strategy"] for r in results[:top_n] if r.get("total_trades", 0) > 0]


def get_next_day_recommendations(df_latest, top_stocks_or_results, results=None, top_n=10,
                                 strength_weighted: bool = False,
                                 enhanced: bool = False, regime_filter: bool = False):
    """基于回测 Top-N 策略，生成最新交易日的个股推荐。

    对每个入选策略，在最新交易日数据上重新应用信号函数，命中则累计
    该策略的历史胜率作为评分：total_score = Σ(命中策略的胜率)。
    strength_weighted=True 时（P1-5）评分改为 Σ(胜率×信号强度)，
    且排序键从 (策略数, 平均胜率) 变为 (策略数, 加权总分)；默认 False 保持
    旧排序（backtest_wfo.py 每日选股依赖该顺序）。
    返回按排序键降序的推荐列表 + 入选策略名列表。
    """
    strategy_results = results if results is not None else top_stocks_or_results
    unique_strategies = get_unique_strategies_from_results(strategy_results, top_n=top_n)
    logger.info(f"回测前{top_n}策略: {unique_strategies}")

    strategy_win_rate = {r["strategy"]: r.get("win_rate", 0) for r in strategy_results}
    strategy_kelly = {r["strategy"]: r.get("kelly", 0) for r in strategy_results}

    if df_latest is None or df_latest.empty:
        logger.warning("没有最新数据可用于推荐")
        return [], []

    latest_date = df_latest["date"].max()
    logger.info(f"最新交易日: {latest_date.date()}, 股票数: {(df_latest['date'] == latest_date).sum()}")

    recommendations = []
    stock_strategy_scores: Dict[str, dict] = {}

    for strategy_name in unique_strategies:
        try:
            sig = _strategy_signal(df_latest, strategy_name,
                                   enhanced=enhanced, regime_filter=regime_filter)
            latest_mask = sig.values & (df_latest["date"] == latest_date).values
            if latest_mask.sum() == 0:
                continue

            matched = df_latest.loc[latest_mask, ["code", "name", "close", "pct_chg", "volume",
                                                  "ma5", "ma20", "rsi6", "atr20"]]
            win_rate = strategy_win_rate.get(strategy_name, 0)
            strength = _signal_strength(df_latest, strategy_name) if strength_weighted else None

            for idx, row in matched.iterrows():
                code = row["code"]
                s_val = float(strength.loc[idx]) if strength is not None else 1.0
                if code not in stock_strategy_scores:
                    stock_strategy_scores[code] = {
                        "code": code,
                        "name": row["name"],
                        "close": row["close"],
                        "pct_chg": row["pct_chg"],
                        "volume": row["volume"],
                        "rsi6": row["rsi6"],
                        "atr20": row.get("atr20"),
                        "matched_strategies": [],
                        "win_rates": [],
                        "strengths": [],
                        "max_kelly": 0.0,
                        "total_score": 0,
                    }
                stock_strategy_scores[code]["matched_strategies"].append(strategy_name)
                stock_strategy_scores[code]["win_rates"].append(win_rate)
                stock_strategy_scores[code]["strengths"].append(s_val)
                stock_strategy_scores[code]["max_kelly"] = max(
                    stock_strategy_scores[code]["max_kelly"], strategy_kelly.get(strategy_name, 0))
                stock_strategy_scores[code]["total_score"] += win_rate * s_val

        except Exception as e:
            logger.error(f"策略 {strategy_name} 应用失败: {e}")
            continue

    for code, info in stock_strategy_scores.items():
        if not info["matched_strategies"]:
            continue

        avg_win_rate = sum(info["win_rates"]) / len(info["win_rates"])
        avg_strength = sum(info["strengths"]) / len(info["strengths"])
        strategy_count = len(info["matched_strategies"])
        position_pct = compute_position_size(
            atr20=info.get("atr20"), close=info["close"], kelly=info["max_kelly"])

        recommendations.append({
            "code": code,
            "name": info["name"],
            "close": info["close"],
            "pct_chg": info["pct_chg"],
            "rsi6": info["rsi6"],
            "matched_strategies": info["matched_strategies"],
            "avg_win_rate": round(avg_win_rate, 2),
            "avg_signal_strength": round(avg_strength, 3),
            "position_pct": position_pct,
            "strategy_count": strategy_count,
            "total_score": round(info["total_score"], 2),
        })

    if strength_weighted:
        recommendations.sort(key=lambda x: (x["strategy_count"], x["total_score"]), reverse=True)
    else:
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
    parser.add_argument("--strict", action="store_true",
                        help="使用严格 regime（market_ok）替代默认的 enhanced regime（market_ok_enh，P0-1）")
    parser.add_argument("--resonance", type=int, default=MIN_RESONANCE_STRATEGIES,
                        help="共振门槛：同一股票同日至少 N 个策略命中才保留（默认 2；传 1 关闭，P0-2）")
    parser.add_argument("--regime-filter", action="store_true",
                        help="按市况分族调度策略（bull/range/bear 激活不同策略族，P1-4）")
    parser.add_argument("--no-industry-momentum", action="store_true",
                        help="关闭行业动量过滤（默认开启仅保留动量前 3 行业信号，P2-7）")
    parser.add_argument("--per-strategy-exit", action="store_true",
                        help="按策略族差异化出场参数（P2-10，实测劣于统一 (2.5,0.95)，默认关闭）")
    parser.add_argument("--entry-timing", choices=["close", "next_open"], default="close",
                        help="入场时点：close=信号日收盘（默认）；next_open=次日开盘（P3-13）")
    parser.add_argument("--portfolio-opt", action="store_true",
                        help="推荐列表组合优化：行业分散(≤2/行业)+持仓上限(≤10)（P3-12）")
    parser.add_argument("--ml-filter", action="store_true",
                        help="按 ML 置信度阈值过滤信号（P3-11，需先运行 train_signal_filter.py 生成工件）")
    args = parser.parse_args(argv)

    # P0 默认启用 enhanced regime + 共振≥2；P2 默认启用行业动量（P2-7 实测最优）。
    # 旧行为分别通过 --strict / --resonance 1 / --no-industry-momentum 显式回退。
    enhanced = not args.strict
    industry_on = not args.no_industry_momentum

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
    if enable_parallel and max_workers > 1:
        logger.info(f"回测模式: 并行 {max_workers} 线程")
        logger.info(f"模式说明: {config['mode_description']}")
    else:
        # detect_resource_config 的 mode_description 按自动检测的并行推荐生成,但
        # resolve_parallel_config 无环境变量时默认串行,直接打印会自相矛盾(如
        # 32 核却显示"全速并行");按实际串行模式重建说明并保留资源上下文。
        resources = config["mode_description"].rsplit("，", 1)[0]
        logger.info("回测模式: 串行")
        logger.info(f"模式说明: {resources}，串行（未设置 BACKTEST_ENABLE_PARALLEL 默认串行,"
                    f"并行需显式设 BACKTEST_ENABLE_PARALLEL=true）")
    logger.info(f"回测起始: {anchored_start} ~ {today_str}")
    logger.info(f"市场环境: {'enhanced regime (market_ok_enh)' if enhanced else 'strict regime (market_ok)'}"
                f" | 共振门槛: {'同股同日≥' + str(args.resonance) + '策略' if args.resonance > 1 else '未启用'}"
                f" | 市况分族: {'启用' if args.regime_filter else '未启用'}"
                f" | 行业动量: {'启用' if industry_on else '未启用'}"
                f" | 分组止损: {'启用' if args.per_strategy_exit else '未启用'}")
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

    logger.info("加载增强信号数据（股东人数/业绩预告/质押/机构龙虎榜）...")
    df_adjusted = load_signal_aux(df_adjusted)
    gc.collect()
    log_memory_usage("增强信号合并后")

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

    # 行业动量上下文(P2-7):申万 L1 排名 + 成分映射 → ind_rank 列（默认启用）
    if industry_on:
        try:
            rank_df, member_df = load_industry_context(anchored_start, today_str)
            df_all = apply_industry_momentum(df_all, rank_df, member_df)
            del rank_df, member_df
            gc.collect()
        except Exception as e:
            logger.warning(f"行业动量上下文加载失败，ind_rank 缺失将全量放行: {e}")

    # 动态退出收益(ATR止损 + 移动止盈 + 时间止损)；P2-10 分组模式追加非默认参数组列
    logger.info("计算动态退出收益(ATR止损/移动止盈)...")
    t_dyn = time.time()
    dyn_ret = compute_dynamic_exit_returns(df_all)
    df_all = pd.concat([df_all, dyn_ret], axis=1)
    del dyn_ret
    gc.collect()
    if args.entry_timing == "next_open":
        logger.info("计算次日开盘入场动态退出收益 (next_open, P3-13) ...")
        dyn_no = compute_dynamic_exit_returns(df_all, entry_timing="next_open")
        dyn_no.columns = [f"{c}_no" for c in dyn_no.columns]
        df_all = pd.concat([df_all, dyn_no], axis=1)
        del dyn_no
        gc.collect()
    if args.per_strategy_exit:
        alt_groups = sorted({tuple(v) for v in STRATEGY_EXIT_PARAMS.values()}
                            - {DEFAULT_EXIT_PARAMS})
        for a_mult, t_rate in alt_groups:
            key = _exit_param_key(a_mult, t_rate)
            logger.info(f"计算分组动态退出收益 {key} ...")
            extra = compute_dynamic_exit_returns(df_all, atr_mult=a_mult, trail=t_rate)
            extra.columns = [f"{c}__{key}" for c in extra.columns]
            df_all = pd.concat([df_all, extra], axis=1)
            del extra
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

    # 市况统计：验证窗口 regime 天数（按日去重），用于区分空仓市况与策略无信号
    week_mask = df_all["date"] >= pd.Timestamp(validate_start_date)
    week_market_ok = None
    regime_col = "market_ok_enh" if enhanced else "market_ok"
    if regime_col in df_all.columns:
        week_market_ok = df_all.loc[week_mask, ["date", regime_col]].drop_duplicates("date")[regime_col]
        market_ok_in_week = int(week_market_ok.sum())
        logger.info(f"验证区间 {regime_col} 天数: {market_ok_in_week}/{len(week_market_ok)}")
    else:
        market_ok_in_week = None

    df_week = df_all[week_mask].copy()
    log_memory_usage("验证数据拆分后")
    mask_bt = df_all["date"] <= pd.Timestamp(backtest_end_date)
    df_bt = df_all.loc[mask_bt].reset_index(drop=True)
    # df_all 保留到验证结束：验证信号需在完整历史上计算（rolling/shift 特征），
    # 不能只给 5 日切片；验证完成后统一释放
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

    results = run_backtests(df_bt, index_df=df_bench_index,
                            enhanced_regime=enhanced, resonance_min=args.resonance,
                            regime_filter=args.regime_filter,
                            industry_filter=industry_on,
                            entry_timing=args.entry_timing)
    # 回测切片（580 万行级）回测后不再需要，立即释放，避免与后续验证信号计算
    # 叠加内存峰值，并降低 handoff 子进程 spawn 时父进程 RSS（0 可用内存 + swap 问题）。
    del df_bt
    gc.collect()
    log_memory_usage("回测完成后")
    # 验证信号统一基于完整历史 df_all 计算（修复：5 日切片导致 rolling/shift 特征丢失），
    # 并与回测同口径：enhanced regime + 冷却期 + 共振过滤 + 市况分族 + 行业动量（如启用）
    signals = {name: _strategy_signal(df_all, name, enhanced=enhanced,
                                      regime_filter=args.regime_filter,
                                      industry_filter=industry_on)
               for name in STRATEGIES}
    signals = {name: _apply_cooldown(df_all, sig, enhanced=enhanced) for name, sig in signals.items()}
    if args.resonance > 1:
        signals = _apply_resonance(df_all, signals, min_strategies=args.resonance)
    if args.ml_filter:
        try:
            from scripts import train_signal_filter as mlf
            signals = mlf.apply_ml_filter(df_all, signals)
            logger.info("ML 信号过滤已应用 (P3-11)")
        except Exception as e:
            logger.warning(f"ML 过滤失败，使用未过滤信号: {e}")
    val_results = validate_week(df_all, df_week, results, TOP_N_VALIDATE, signals=signals)
    # 绩效表格经 print 输出，双写到控制台与回测日志文件
    with open(_LOG_FILE, "a", encoding="utf-8") as log_fp:
        with contextlib.redirect_stdout(_Tee(sys.stdout, log_fp)):
            print_results(results, val_results, backtest_start_date, backtest_end_date,
                          market_ok_days=(market_ok_in_week, len(week_market_ok))
                          if market_ok_in_week is not None else None)

    # 汇总本周验证胜率前 10 股票，作为主程序个股决策的输入
    top_stocks = get_top_stocks_by_win_rate(df_all, df_week, results, top_n=10, signals=signals)
    if args.portfolio_opt and top_stocks:
        try:
            _rank_df, member_df = load_industry_context(anchored_start, today_str)
            top_stocks = optimize_portfolio(top_stocks, member_df)
            del _rank_df, member_df
        except Exception as e:
            logger.warning(f"组合优化失败，使用原始推荐: {e}")
    logger.info(f"5日验证胜率前10股票: {len(top_stocks)} 只")

    for idx, s in enumerate(top_stocks, 1):
        ret_str = f"{s['sell_return'] * 100:.2f}%" if s['sell_return'] is not None and pd.notna(s['sell_return']) else "N/A"
        pos_str = f" - 建议仓位{s['position_pct']*100:.1f}%" if s.get('position_pct') else ""
        hold_str = f" - 推荐持仓{s['recommended_hold_days']}日" if s.get('recommended_hold_days') else ""
        logger.info(f"  [{idx}] {s['code']} {s['name']} - 胜率{s['win_rate']:.1f}% - 策略数{s['strategy_count']} - 持仓期收益{ret_str}{hold_str}{pos_str}")

    # 验证完成，释放所有大 DataFrame 与中间结果，仅保留小的 top_stocks/results。
    # handoff 会 spawn main.py 子进程；若父进程仍持有 580 万行级 DataFrame，
    # 子进程启动时可用物理内存为 0、被迫全程 swap。这里在子进程前彻底归还内存。
    # df_bt 已在回测后释放，df_all/signals 已在上一步 del；此处释放验证期大对象。
    try:
        del df_week
    except NameError:
        pass
    try:
        del val_results
    except NameError:
        pass
    try:
        del df_bench_index
    except NameError:
        pass
    gc.collect()
    log_memory_usage("handoff 主程序前（已释放大数据）")

    if top_stocks:
        logger.info("\n" + "=" * 60)
        logger.info("开始执行主程序进行大盘复盘和个股决策")
        logger.info("=" * 60)
        run_main_program_for_stocks(top_stocks)
    elif market_ok_in_week == 0:
        logger.warning(
            f"验证区间 {validate_start_date.date()} ~ {validate_end_date.date()} "
            "全部处于空仓市况（market_ok=False），按策略纪律跳过主程序执行"
        )
    else:
        logger.warning("没有找到符合条件的股票，跳过主程序执行")

    logger.info("完成")


if __name__ == "__main__":
    main()
