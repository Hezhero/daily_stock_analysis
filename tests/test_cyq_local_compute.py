# -*- coding: utf-8 -*-
"""筹码分布本地计算（三角形分布法）纯函数测试。

覆盖 compute_chip_distribution 的行为：
  1. 合成日线数据 → 指标在合理范围
  2. 缺失 daily_basic 行 → 衰减按 1.0 处理，不报错
  3. 手算小样本一致性（独立预期）
  4. 空 DataFrame 输入 → 返回空/None 不抛出
  5. high==low 单日无振幅 → 不除零，集中度 0.0
  6. 集中度分母为 0 保护
"""

import datetime

import pandas as pd
import pytest

from scripts.data_collection.incremental_cyq import (
    compute_chip_distribution,
    compute_chip_distribution_daily,
)


def _make_daily(rows):
    """构造 tushare_daily 风格 DataFrame（列与 load_daily_data 一致）。"""
    return pd.DataFrame(
        rows,
        columns=["ts_code", "trade_date", "open", "high", "low", "close", "vol"],
    )


def _make_basic(rows):
    """构造 tushare_daily_basic 风格 DataFrame。"""
    return pd.DataFrame(
        rows,
        columns=["ts_code", "trade_date", "turnover_rate", "float_share"],
    )


def test_empty_daily_input_returns_empty():
    df = _make_daily([])
    basic = _make_basic([])
    assert compute_chip_distribution(df, basic) == {}


def test_empty_basic_input_no_exception():
    """daily 有数据但 basic 为空 → 全部按 decay=1.0 处理，不报错。"""
    daily = _make_daily(
        [
            ("600519.SH", datetime.date(2026, 8, 5), 10.0, 12.0, 9.0, 11.0, 100.0),
            ("600519.SH", datetime.date(2026, 8, 6), 11.0, 13.0, 10.0, 12.0, 120.0),
            ("600519.SH", datetime.date(2026, 8, 7), 12.0, 14.0, 11.0, 13.0, 130.0),
        ]
    )
    result = compute_chip_distribution(daily, _make_basic([]))
    assert result, "非空输入应返回非空结果"
    assert 0.0 <= result["profit_ratio"] <= 1.0
    assert result["avg_cost"] > 0.0
    assert result["cost_90_low"] <= result["cost_90_high"]
    assert 0.0 <= result["concentration_90"] <= 1.0
    assert 0.0 <= result["concentration_70"] <= 1.0


def test_metrics_in_valid_ranges():
    daily = _make_daily(
        [
            ("600519.SH", "2026-08-03", 10.0, 12.0, 9.0, 11.0, 100.0),
            ("600519.SH", "2026-08-04", 11.0, 13.0, 10.0, 12.0, 120.0),
            ("600519.SH", "2026-08-05", 12.0, 14.0, 11.0, 13.0, 130.0),
            ("600519.SH", "2026-08-06", 13.0, 15.0, 12.0, 14.0, 140.0),
            ("600519.SH", "2026-08-07", 14.0, 16.0, 13.0, 15.0, 150.0),
        ]
    )
    basic = _make_basic(
        [
            ("600519.SH", "2026-08-03", 1.0, 1000.0),
            ("600519.SH", "2026-08-04", 1.0, 1000.0),
            ("600519.SH", "2026-08-05", 1.0, 1000.0),
            ("600519.SH", "2026-08-06", 1.0, 1000.0),
            ("600519.SH", "2026-08-07", 1.0, 1000.0),
        ]
    )
    result = compute_chip_distribution(daily, basic)
    assert result["trade_date"] == "2026-08-07"
    assert 0.0 <= result["profit_ratio"] <= 1.0
    assert result["avg_cost"] > 0.0
    assert result["cost_90_low"] <= result["cost_90_high"]
    assert result["cost_70_low"] <= result["cost_70_high"]
    assert 0.0 <= result["concentration_90"] <= 1.0
    assert 0.0 <= result["concentration_70"] <= 1.0


def test_single_day_no_amplitude_no_division_by_zero():
    """high==low 单日：三角形退化为单点，集中度应为 0.0，不除零。"""
    daily = _make_daily(
        [("600519.SH", "2026-08-07", 10.0, 10.0, 10.0, 10.0, 100.0)]
    )
    basic = _make_basic([("600519.SH", "2026-08-07", 1.0, 1000.0)])
    result = compute_chip_distribution(daily, basic)
    assert result["avg_cost"] == 10.0
    assert result["concentration_90"] == 0.0
    assert result["concentration_70"] == 0.0


def test_missing_basic_day_uses_decay_one():
    """中间某日缺 basic → 该日不衰减，结果仍可计算。"""
    daily = _make_daily(
        [
            ("600519.SH", "2026-08-05", 10.0, 12.0, 9.0, 11.0, 100.0),
            ("600519.SH", "2026-08-06", 11.0, 13.0, 10.0, 12.0, 120.0),
            ("600519.SH", "2026-08-07", 12.0, 14.0, 11.0, 13.0, 130.0),
        ]
    )
    # 只有 8-05 与 8-07 有 basic，8-06 缺失
    basic = _make_basic(
        [
            ("600519.SH", "2026-08-05", 1.0, 1000.0),
            ("600519.SH", "2026-08-07", 1.0, 1000.0),
        ]
    )
    result = compute_chip_distribution(daily, basic)
    assert result
    assert 0.0 <= result["profit_ratio"] <= 1.0


def test_hand_computed_small_sample_consistency():
    """手算小样本独立预期：单日数据，三角形分布。

    单日：low=10, high=14, close=11, vol=1000，turnover=100%（全换手，无历史）。
    三角形分布峰值（众数） = (low+high+close)/3 = 11.667；
    三角形分布均值 = (low+high+peak)/3 = (10+14+11.667)/3 = 11.889。
    平均成本 = 筹码量加权平均价格 ≈ 三角形分布均值 11.889（容差 0.2）。
    profit_ratio：价格 <= close(11) 的筹码占比，应落在 (0, 1) 之间。
    """
    daily = _make_daily(
        [("600519.SH", "2026-08-07", 10.0, 14.0, 10.0, 11.0, 1000.0)]
    )
    basic = _make_basic([("600519.SH", "2026-08-07", 100.0, 1000.0)])
    result = compute_chip_distribution(daily, basic)
    peak = (10.0 + 14.0 + 11.0) / 3.0  # 11.667
    expected_mean = (10.0 + 14.0 + peak) / 3.0  # 11.889
    assert abs(result["avg_cost"] - expected_mean) < 0.2
    assert 0.0 <= result["profit_ratio"] <= 1.0
    # 90% 成本区应覆盖大部分价格区间
    assert result["cost_90_low"] >= 10.0 - 0.1
    assert result["cost_90_high"] <= 14.0 + 0.1


def test_concentration_zero_denominator_guard():
    """极端情况：所有价格相同 → 集中度 0.0 且不除零。"""
    daily = _make_daily(
        [
            ("600519.SH", "2026-08-06", 10.0, 10.0, 10.0, 10.0, 100.0),
            ("600519.SH", "2026-08-07", 10.0, 10.0, 10.0, 10.0, 100.0),
        ]
    )
    basic = _make_basic(
        [
            ("600519.SH", "2026-08-06", 1.0, 1000.0),
            ("600519.SH", "2026-08-07", 1.0, 1000.0),
        ]
    )
    result = compute_chip_distribution(daily, basic)
    assert result["concentration_90"] == 0.0
    assert result["concentration_70"] == 0.0
    assert result["avg_cost"] == 10.0


# ---- compute_chip_distribution_daily（逐日版）----


def test_daily_returns_one_row_per_trading_day():
    """逐日版：每个交易日一行，行数与输入一致，trade_date 升序。"""
    daily = _make_daily(
        [
            ("600519.SH", "2026-08-03", 10.0, 12.0, 9.0, 11.0, 100.0),
            ("600519.SH", "2026-08-04", 11.0, 13.0, 10.0, 12.0, 120.0),
            ("600519.SH", "2026-08-05", 12.0, 14.0, 11.0, 13.0, 130.0),
            ("600519.SH", "2026-08-06", 13.0, 15.0, 12.0, 14.0, 140.0),
            ("600519.SH", "2026-08-07", 14.0, 16.0, 13.0, 15.0, 150.0),
        ]
    )
    basic = _make_basic(
        [
            ("600519.SH", "2026-08-03", 1.0, 1000.0),
            ("600519.SH", "2026-08-04", 1.0, 1000.0),
            ("600519.SH", "2026-08-05", 1.0, 1000.0),
            ("600519.SH", "2026-08-06", 1.0, 1000.0),
            ("600519.SH", "2026-08-07", 1.0, 1000.0),
        ]
    )
    result = compute_chip_distribution_daily(daily, basic)
    assert len(result) == 5
    assert result["trade_date"].tolist() == [
        "2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06", "2026-08-07",
    ]
    assert result["profit_ratio"].between(0.0, 1.0).all()
    assert (result["avg_cost"] > 0.0).all()
    assert (result["cost_90_low"] <= result["cost_90_high"]).all()
    assert (result["cost_70_low"] <= result["cost_70_high"]).all()
    assert result["concentration_90"].between(0.0, 1.0).all()
    assert result["concentration_70"].between(0.0, 1.0).all()


def test_daily_last_row_matches_single_day_wrapper():
    """逐日版最后一行应与兼容包装 compute_chip_distribution 的返回值一致。"""
    daily = _make_daily(
        [
            ("600519.SH", "2026-08-03", 10.0, 12.0, 9.0, 11.0, 100.0),
            ("600519.SH", "2026-08-04", 11.0, 13.0, 10.0, 12.0, 120.0),
            ("600519.SH", "2026-08-05", 12.0, 14.0, 11.0, 13.0, 130.0),
            ("600519.SH", "2026-08-06", 13.0, 15.0, 12.0, 14.0, 140.0),
            ("600519.SH", "2026-08-07", 14.0, 16.0, 13.0, 15.0, 150.0),
        ]
    )
    basic = _make_basic(
        [
            ("600519.SH", "2026-08-03", 1.0, 1000.0),
            ("600519.SH", "2026-08-04", 1.0, 1000.0),
            ("600519.SH", "2026-08-05", 1.0, 1000.0),
            ("600519.SH", "2026-08-06", 1.0, 1000.0),
            ("600519.SH", "2026-08-07", 1.0, 1000.0),
        ]
    )
    daily_result = compute_chip_distribution_daily(daily, basic)
    wrapper = compute_chip_distribution(daily, basic)
    last = daily_result.iloc[-1].to_dict()
    for col in last:
        assert last[col] == wrapper[col], f"列 {col} 不一致: {last[col]} != {wrapper[col]}"


def test_daily_empty_input_returns_empty_dataframe():
    """逐日版：空输入返回空 DataFrame（不抛异常）。"""
    result = compute_chip_distribution_daily(_make_daily([]), _make_basic([]))
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_daily_missing_basic_day_uses_decay_one():
    """逐日版：中间某日缺 basic → 该日不衰减，逐日结果仍可计算。"""
    daily = _make_daily(
        [
            ("600519.SH", "2026-08-05", 10.0, 12.0, 9.0, 11.0, 100.0),
            ("600519.SH", "2026-08-06", 11.0, 13.0, 10.0, 12.0, 120.0),
            ("600519.SH", "2026-08-07", 12.0, 14.0, 11.0, 13.0, 130.0),
        ]
    )
    basic = _make_basic(
        [
            ("600519.SH", "2026-08-05", 1.0, 1000.0),
            ("600519.SH", "2026-08-07", 1.0, 1000.0),
        ]
    )
    result = compute_chip_distribution_daily(daily, basic)
    assert len(result) == 3
    assert result["profit_ratio"].between(0.0, 1.0).all()


def test_daily_single_day_no_amplitude():
    """逐日版：high==low 单日 → 平均成本等于该价格，集中度 0.0。"""
    daily = _make_daily(
        [("600519.SH", "2026-08-07", 10.0, 10.0, 10.0, 10.0, 100.0)]
    )
    basic = _make_basic([("600519.SH", "2026-08-07", 1.0, 1000.0)])
    result = compute_chip_distribution_daily(daily, basic)
    assert len(result) == 1
    assert result["avg_cost"].iloc[0] == 10.0
    assert result["concentration_90"].iloc[0] == 0.0
    assert result["concentration_70"].iloc[0] == 0.0


def test_daily_zero_vol_day_skipped_in_accumulation():
    """逐日版：某日 vol=0（停牌）→ 该日不分配筹码也不衰减，后续日仍正常。"""
    daily = _make_daily(
        [
            ("600519.SH", "2026-08-05", 10.0, 12.0, 9.0, 11.0, 100.0),
            ("600519.SH", "2026-08-06", 11.0, 13.0, 10.0, 12.0, 0.0),
            ("600519.SH", "2026-08-07", 12.0, 14.0, 11.0, 13.0, 130.0),
        ]
    )
    basic = _make_basic(
        [
            ("600519.SH", "2026-08-05", 1.0, 1000.0),
            ("600519.SH", "2026-08-06", 1.0, 1000.0),
            ("600519.SH", "2026-08-07", 1.0, 1000.0),
        ]
    )
    result = compute_chip_distribution_daily(daily, basic)
    assert len(result) == 3
    assert result["profit_ratio"].between(0.0, 1.0).all()
    assert (result["avg_cost"] > 0.0).all()


def test_daily_low_price_day_with_wide_range_keeps_valid_chips():
    """回归：全历史价格跨度近 10 倍时，早期低价交易日筹码不可退化为空。

    线性等分档位下，低价区间会整个落在第一个档位中心之下（in_range 全 False），
    导致当日 avg_cost=0、分位价格取到最高档。对数间距档位必须覆盖低价区间。
    """
    daily = _make_daily(
        [
            ("600519.SH", "2016-07-01", 286.0, 291.88, 284.0, 286.17, 26014.95),
            ("600519.SH", "2016-07-04", 290.0, 296.0, 288.0, 293.0, 100.0),
            ("600519.SH", "2016-07-05", 295.03, 303.1, 293.0, 300.58, 200.0),
            # 高价区间（决定全历史 max(high)），成交量足够大以进入分位区间
            ("600519.SH", "2026-08-07", 2600.0, 2627.88, 2590.0, 2610.0, 30000.0),
        ]
    )
    basic = _make_basic(
        [
            ("600519.SH", "2016-07-01", 1.0, 1000.0),
            ("600519.SH", "2016-07-04", 1.0, 1000.0),
            ("600519.SH", "2016-07-05", 1.0, 1000.0),
            ("600519.SH", "2026-08-07", 1.0, 1000.0),
        ]
    )
    price_range = (284.0, 2627.88)
    result = compute_chip_distribution_daily(daily, basic, price_range)
    assert len(result) == 4
    first = result.iloc[0]
    # 早期低价日：平均成本必须在当日价格区间附近，不能为 0 或取到最高档
    assert 284.0 <= first["avg_cost"] <= 291.88, f"avg_cost={first['avg_cost']}"
    assert first["cost_90_low"] < first["cost_90_high"]
    assert first["cost_90_high"] < 400.0, f"cost_90_high={first['cost_90_high']}"
    assert first["concentration_90"] > 0.0
    assert first["concentration_70"] > 0.0
    # 高价日：高价筹码进入分位区间（95 分位贴近高价区），avg_cost 为全部累积筹码加权均值
    last = result.iloc[-1]
    assert 284.0 <= last["avg_cost"] <= 2627.88, f"avg_cost={last['avg_cost']}"
    assert last["cost_90_high"] > 2000.0, f"cost_90_high={last['cost_90_high']}"