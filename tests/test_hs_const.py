# -*- coding: utf-8 -*-
"""tushare hs_const（沪深股通成份股）接口历史覆盖能力测试。

背景：回测需要沪深股通成份股的完整历史变更（纳入/剔除），本测试验证
hs_const 接口能否返回近 10 年的历史数据。

接口语义（tushare hs_const）：
  - is_new="1"（默认）：当前仍在沪深股通名单内的成份
  - is_new="0"：完整历史变更记录（含已剔除的，out_date 非空）

断言策略：
  - SH 沪股通 2014-11-17 开通 → 历史应覆盖至少近 10 年
  - SZ 深股通 2016-12-05 开通 → 历史应覆盖自开通以来的完整记录
    （SZ 开通至今不足 10 年，硬性 10 年断言会误报，故按开通日校验）
  - 当前成份（is_new=1）SH/SZ 均非空

运行方式：
  - 需要 TUSHARE_TOKEN 与网络，标记 @pytest.mark.network
  - 阻断 CI 跑 pytest -m "not network"（scripts/ci_gate.sh），本文件不会进入
  - Network Smoke cron 跑 pytest -m network（非阻断，.github/workflows/network-smoke.yml）
"""

import datetime
import os
import sys
import unittest

import pandas as pd
import pytest
from dotenv import load_dotenv

# 项目根目录入 path（与 tests/test_tushare_fetcher_get_stock_list.py 一致）
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

import tushare as ts  # noqa: E402

_SH_LAUNCH = datetime.date(2014, 11, 17)  # 沪股通开通日
_SZ_LAUNCH = datetime.date(2016, 12, 5)  # 深股通开通日
_REQUIRED_COLS = {"ts_code", "hs_type", "in_date", "out_date", "is_new"}


def _pro_or_skip():
    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        raise unittest.SkipTest("TUSHARE_TOKEN 未配置，跳过 hs_const 网络测试")
    return ts.pro_api(token)


@pytest.mark.network
class TestHsConstHistoryCoverage(unittest.TestCase):
    """hs_const 接口历史覆盖能力验证（真实调用，需 token + 网络）。"""

    @staticmethod
    def _fetch_or_skip(hs_type, is_new):
        """调用 hs_const 并校验返回结构；网络/接口异常统一跳过，结构异常直接失败。"""
        try:
            df = _pro_or_skip().hs_const(hs_type=hs_type, is_new=is_new)
        except Exception as exc:  # 网络/接口异常 → 静默跳过，不污染 Network Smoke 观测
            raise unittest.SkipTest(f"hs_const({hs_type}, is_new={is_new}) 调用失败: {exc}") from exc
        if df is None or df.empty:
            raise AssertionError(f"hs_const({hs_type}, is_new={is_new}) 返回空数据")
        missing = _REQUIRED_COLS - set(df.columns)
        if missing:
            raise AssertionError(f"hs_const({hs_type}) 缺少字段: {sorted(missing)}")
        return df

    @staticmethod
    def _earliest_in_date(df):
        dates = pd.to_datetime(df["in_date"], format="%Y%m%d", errors="coerce")
        if dates.isna().any():
            raise AssertionError("in_date 存在无法解析的值")
        return dates.min().date()

    def test_sh_history_covers_ten_years(self):
        """沪股通完整历史变更应覆盖至少近 10 年（最早纳入日期 ≤ 今天-10年）。"""
        df = self._fetch_or_skip("SH", "0")
        earliest = self._earliest_in_date(df)
        cutoff = datetime.date.today() - datetime.timedelta(days=365 * 10)
        span_years = (datetime.date.today() - earliest).days / 365.25
        self.assertLessEqual(
            earliest,
            cutoff,
            f"SH 最早纳入日期 {earliest} 距今仅 {span_years:.1f} 年，不足 10 年",
        )

    def test_sz_history_covers_since_launch(self):
        """深股通完整历史应覆盖自开通日（2016-12-05）以来的全部变更。"""
        df = self._fetch_or_skip("SZ", "0")
        earliest = self._earliest_in_date(df)
        self.assertLessEqual(
            earliest,
            _SZ_LAUNCH,
            f"SZ 最早纳入日期 {earliest} 晚于开通日 {_SZ_LAUNCH}，历史不完整",
        )

    def test_current_constituents_non_empty(self):
        """当前成份（is_new=1）SH/SZ 均非空。"""
        for hs_type in ("SH", "SZ"):
            df = self._fetch_or_skip(hs_type, "1")
            self.assertTrue(len(df) > 0, f"hs_const({hs_type}, is_new=1) 当前成份为空")


if __name__ == "__main__":
    unittest.main()