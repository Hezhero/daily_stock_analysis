# -*- coding: utf-8 -*-
"""
PostgreSQL 本地缓存数据源（Tushare 数据仓库的运行时兜底）。

本数据源只读访问由 ``scripts/data_collection/`` 采集脚本维护的 PostgreSQL
``tushare`` 库，在在线数据源（akshare / efinance / tushare pro 等）因权限、
网络或限流失败时提供本地兜底，避免主分析流程因外部接口不可用而缺失关键数据。

覆盖三类数据：
1. 筹码分布：读取 ``tushare_cyq``（本地三角形分布法计算结果），
   提供 :meth:`PgCacheFetcher.get_chip_distribution`。
2. 行业板块资金流 / 涨跌榜：按 ``tushare_stock_basic.industry`` 聚合
   ``tushare_daily.pct_chg`` 与 ``tushare_moneyflow.net_mf_amount``，
   提供 :meth:`PgCacheFetcher.get_sector_rankings`。
3. 个股资金流：读取 ``tushare_moneyflow``，
   提供 :meth:`PgCacheFetcher.get_capital_flow`。

设计原则（遵循 data_provider 稳定性护栏）：
- fail-open：未配置 PG、缺 psycopg2、表缺失或查询异常时一律返回 None / 空结果，
  绝不抛断主流程；由调用方继续走既有降级路径。
- 末位兜底：priority 默认很高（数字大 = 优先级低），仅在所有在线源失败后才被使用。
- 不写库：本类只有只读查询，不修改数据仓库内容。
- 连接懒加载 + 短连接：每次查询独立连接并关闭，避免长连接在批量分析中持有资源；
  连接失败会被缓存为"不可用"，避免对每张股票反复尝试连接。
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .base import BaseFetcher, normalize_stock_code
from .realtime_types import ChipDistribution

logger = logging.getLogger(__name__)

# 万元 -> 元 的换算（tushare moneyflow 金额单位为万元，在线 akshare 资金流单位为元）
_WAN_TO_YUAN = 10000.0


def _to_ts_code(stock_code: str) -> Optional[str]:
    """将归一化后的 A 股代码转为 Tushare ts_code（如 600519 -> 600519.SH）。

    仅处理 A 股 / ETF / 北交所代码；港股 / 美股返回 None（本地 tushare 库不覆盖）。
    """
    code = normalize_stock_code(stock_code)
    if not code or not code.isdigit() or len(code) != 6:
        return None
    if code.startswith(("600", "601", "603", "605", "688", "510", "511", "512", "513", "515", "516", "518", "588")):
        return f"{code}.SH"
    if code.startswith(("000", "001", "002", "003", "300", "301", "159", "150", "160", "161", "162", "163", "164", "165", "166", "167", "168", "169")):
        return f"{code}.SZ"
    if code.startswith(("8", "4", "920")):
        return f"{code}.BJ"
    # 兜底：6 开头按沪市，其余按深市
    return f"{code}.SH" if code.startswith("6") else f"{code}.SZ"


class PgCacheFetcher(BaseFetcher):
    """PostgreSQL 本地 Tushare 数据仓库兜底数据源。"""

    name = "PgCacheFetcher"

    @staticmethod
    def is_configured() -> bool:
        """是否配置了可用的 PG 连接（host + 密码齐备）。未配置时不注册该数据源。"""
        host = os.getenv("PG_HOST", "127.0.0.1").strip()
        password = os.getenv("PG_PASSWORD", "").strip()
        return bool(host and password)

    def __init__(self, priority: Optional[int] = None) -> None:
        # 末位兜底：默认优先级 90，仅在所有在线源失败后使用；可用 PG_CACHE_PRIORITY 调整
        self.priority = int(priority if priority is not None else os.getenv("PG_CACHE_PRIORITY", "90"))
        self._pg_config: Optional[Dict[str, Any]] = None
        self._config_loaded = False
        self._unavailable = False  # 连接/依赖不可用后置 True，避免反复重试
        super().__init__()

    # ------------------------------------------------------------------
    # BaseFetcher 抽象接口（本数据源不提供日线，给出最小实现）
    # ------------------------------------------------------------------
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        # 本地缓存数据源不参与日线获取链路
        return pd.DataFrame()

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # 连接管理
    # ------------------------------------------------------------------
    def _load_config(self) -> Optional[Dict[str, Any]]:
        """从环境变量读取 PG 连接配置（与 scripts/data_collection/tushare_pg_utils.py 对齐）。"""
        if self._config_loaded:
            return self._pg_config
        self._config_loaded = True
        host = os.getenv("PG_HOST", "127.0.0.1").strip()
        password = os.getenv("PG_PASSWORD", "").strip()
        dbname = os.getenv("PG_DBNAME", os.getenv("PG_DATABASE", "tushare")).strip()
        # 未配置密码视为未启用本地库（数据仓库通常需要认证）
        if not host or not password:
            self._pg_config = None
            return None
        try:
            port = int(os.getenv("PG_PORT", "5432"))
        except ValueError:
            port = 5432
        self._pg_config = {
            "host": host,
            "port": port,
            "user": os.getenv("PG_USER", "root").strip(),
            "password": password,
            "dbname": dbname,
            "connect_timeout": int(os.getenv("PG_CACHE_TIMEOUT", "5")),
        }
        return self._pg_config

    def _connect(self):
        """建立短连接；不可用时返回 None 并标记 _unavailable。"""
        if self._unavailable:
            return None
        cfg = self._load_config()
        if cfg is None:
            self._unavailable = True
            return None
        try:
            import psycopg2  # 延迟导入，未安装时不影响其它数据源
        except ImportError:
            logger.debug("[PgCacheFetcher] 未安装 psycopg2，跳过本地 PG 兜底")
            self._unavailable = True
            return None
        try:
            return psycopg2.connect(**cfg)
        except Exception as exc:  # 连接失败不致命，降级到在线源
            logger.warning("[PgCacheFetcher] PG 连接失败，跳过本地兜底: %s", exc)
            self._unavailable = True
            return None

    def is_available(self) -> bool:
        """供管理器探测：PG 是否配置且可连接（会尝试一次轻量连接）。"""
        conn = self._connect()
        if conn is None:
            return False
        try:
            conn.close()
        except Exception:
            pass
        return not self._unavailable

    # ------------------------------------------------------------------
    # 1) 筹码分布兜底（tushare_cyq）
    # ------------------------------------------------------------------
    def get_chip_distribution(self, stock_code: str) -> Optional[ChipDistribution]:
        ts_code = _to_ts_code(stock_code)
        if ts_code is None or ts_code.endswith((".HK", ".US")):
            return None
        conn = self._connect()
        if conn is None:
            return None
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT trade_date, profit_ratio, avg_cost,
                       cost_90_low, cost_90_high, concentration_90,
                       cost_70_low, cost_70_high, concentration_70
                FROM tushare_cyq
                WHERE ts_code = %s
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (ts_code,),
            )
            row = cur.fetchone()
            cur.close()
            if not row:
                return None
            (trade_date, profit_ratio, avg_cost, c90_low, c90_high, conc90,
             c70_low, c70_high, conc70) = row

            def _f(v: Any) -> float:
                try:
                    return float(v) if v is not None else 0.0
                except (TypeError, ValueError):
                    return 0.0

            chip = ChipDistribution(
                code=normalize_stock_code(stock_code),
                date=str(trade_date) if trade_date is not None else "",
                source="pg_tushare_cyq",
                profit_ratio=_f(profit_ratio),
                avg_cost=_f(avg_cost),
                cost_90_low=_f(c90_low),
                cost_90_high=_f(c90_high),
                concentration_90=_f(conc90),
                cost_70_low=_f(c70_low),
                cost_70_high=_f(c70_high),
                concentration_70=_f(conc70),
            )
            return chip
        except Exception as exc:
            logger.warning("[PgCacheFetcher] 读取 tushare_cyq 失败 %s: %s", ts_code, exc)
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 2) 申万行业资金流榜（本地聚合，仅用于资金流语境）
    # ------------------------------------------------------------------
    def get_sector_rankings(self, n: int = 5) -> None:
        """刻意不提供板块**涨跌**榜。

        市场复盘涨跌榜（``get_sector_rankings`` 链路）在线侧为东财/同花顺
        行业板块 + 概念板块，分类口径与本地申万行业资金流聚合不同；为避免把
        申万行业资金流误当涨跌榜展示，这里显式返回 None 跳过该链路。
        申万行业资金流榜见 :meth:`get_sector_fund_flow_rankings`。
        """
        return None

    def get_sector_fund_flow_rankings(
        self, n: int = 5, sort_by: str = "net_inflow"
    ) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        """申万（Tushare）行业资金流榜：按行业聚合最新交易日个股资金流与涨跌幅。

        注意：这是**行业资金流排行**，不是市场复盘用的"板块涨跌榜"
        （东财/同花顺行业板块 + 概念板块）。分类口径为 Tushare 申万行业
        （``tushare_stock_basic.industry``），与在线板块榜分类体系不同，
        因此仅作为资金流（capital_flow）语境下在线行业资金流接口失败时的
        兜底，不接入 ``get_sector_rankings`` 涨跌榜链路。

        Args:
            n: 每侧返回的行业数。
            sort_by: 排序依据。``"net_inflow"``（默认）按主力净流入额排序，
                top=净流入最多、bottom=净流出最多；``"change_pct"`` 按行业内
                个股均涨幅排序，top=涨幅最大、bottom=跌幅最大。

        返回 ``{"top": [...], "bottom": [...]}``。每项含 name / net_inflow
        （元）/ change_pct（行业内个股均涨幅）/ constituent_count / source /
        rank_type / classification。
        """
        sort_key = "change_pct" if str(sort_by).strip().lower() in ("change_pct", "pct", "change") else "net_inflow"
        conn = self._connect()
        if conn is None:
            return None
        try:
            cur = conn.cursor()
            cur.execute(
                """
                WITH latest AS (
                    SELECT MAX(trade_date) AS td FROM tushare_daily
                ),
                agg AS (
                    SELECT b.industry AS industry,
                           AVG(d.pct_chg) AS avg_pct,
                           SUM(COALESCE(m.net_mf_amount, 0)) AS net_mf,
                           COUNT(d.ts_code) AS cnt
                    FROM tushare_daily d
                    CROSS JOIN latest
                    JOIN tushare_stock_basic b
                         ON b.ts_code = d.ts_code AND b.list_status = 'L'
                    LEFT JOIN tushare_moneyflow m
                         ON m.ts_code = d.ts_code AND m.trade_date = d.trade_date
                    WHERE d.trade_date = latest.td
                      AND b.industry IS NOT NULL AND b.industry <> ''
                    GROUP BY b.industry
                    HAVING COUNT(d.ts_code) >= 3
                )
                SELECT industry, avg_pct, net_mf, cnt FROM agg
                """
            )
            rows = cur.fetchall()
            cur.close()
            if not rows:
                return None

            items: List[Dict[str, Any]] = []
            for industry, avg_pct, net_mf, cnt in rows:
                items.append({
                    "name": str(industry),
                    # tushare moneyflow 单位万元 -> 元
                    "net_inflow": round(float(net_mf) * _WAN_TO_YUAN, 2) if net_mf is not None else 0.0,
                    "change_pct": round(float(avg_pct), 2) if avg_pct is not None else 0.0,
                    "constituent_count": int(cnt),
                    "source": "pg_sw_industry_fund_flow",
                    "rank_type": sort_key,
                    "classification": "sw_industry",
                })

            ordered = sorted(items, key=lambda x: x[sort_key], reverse=True)
            top = ordered[:n]
            bottom = list(reversed(ordered[-n:]))
            return {"top": top, "bottom": bottom}
        except Exception as exc:
            logger.warning("[PgCacheFetcher] 本地聚合申万行业资金流榜失败: %s", exc)
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # 3) 个股资金流兜底（tushare_moneyflow）
    # ------------------------------------------------------------------
    def get_capital_flow(self, stock_code: str) -> Optional[Dict[str, float]]:
        """读取个股最新交易日及近 5/10 日资金流。

        返回 ``{"main_net_inflow", "inflow_5d", "inflow_10d"}``（单位：元，与在线
        akshare 资金流字段对齐）；无数据返回 None。

        - main_net_inflow：主力（大单+特大单）净流入；若大单字段缺失则回退为
          当日全单净流入 net_mf_amount。
        - inflow_5d / inflow_10d：近 5 / 10 个交易日全单净流入累计。
        """
        ts_code = _to_ts_code(stock_code)
        if ts_code is None or ts_code.endswith((".HK", ".US")):
            return None
        conn = self._connect()
        if conn is None:
            return None
        try:
            cur = conn.cursor()
            # 最新交易日 + 近 10 日（含当日）的资金流
            cur.execute(
                """
                SELECT trade_date,
                       COALESCE(buy_lg_amount, 0) + COALESCE(buy_elg_amount, 0)
                         - COALESCE(sell_lg_amount, 0) - COALESCE(sell_elg_amount, 0) AS main_net,
                       COALESCE(net_mf_amount, 0) AS net_mf
                FROM tushare_moneyflow
                WHERE ts_code = %s
                ORDER BY trade_date DESC
                LIMIT 10
                """,
                (ts_code,),
            )
            rows = cur.fetchall()
            cur.close()
            if not rows:
                return None

            # 最新一日：主力净额优先，缺失则用全单净额
            _, main_net_latest, net_mf_latest = rows[0]
            main_today = float(main_net_latest) if main_net_latest not in (None, 0) else float(net_mf_latest or 0.0)

            # 近 N 日累计全单净流入（万元）
            def _sum_net(limit: int) -> float:
                return sum(float(r[2] or 0.0) for r in rows[:limit])

            inflow_5d = _sum_net(5)
            inflow_10d = _sum_net(10)

            # 全为 0 / 缺失视为无有效数据
            if main_today == 0.0 and inflow_5d == 0.0 and inflow_10d == 0.0:
                return None

            return {
                # 万元 -> 元
                "main_net_inflow": round(main_today * _WAN_TO_YUAN, 2),
                "inflow_5d": round(inflow_5d * _WAN_TO_YUAN, 2),
                "inflow_10d": round(inflow_10d * _WAN_TO_YUAN, 2),
            }
        except Exception as exc:
            logger.warning("[PgCacheFetcher] 读取 tushare_moneyflow 失败 %s: %s", ts_code, exc)
            return None
        finally:
            try:
                conn.close()
            except Exception:
                pass
