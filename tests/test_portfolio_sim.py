# -*- coding: utf-8 -*-
"""Unit tests for the S2 capital-constrained daily MTM simulator."""

from __future__ import annotations

from collections import Counter
from typing import Any, Sequence

import numpy as np
import pandas as pd
import pytest

from scripts.portfolio_sim import (
    FAMILY_CAP,
    STRATEGY_FAMILY,
    _CAND_COLUMNS,
    SimConfig,
    _config_hash,
    apply_declustered_resonance,
    apply_lead_expectation,
    build_candidate_table,
    collapse_candidates,
    compute_metrics,
    simulate,
)


def _dates(n: int) -> list[pd.Timestamp]:
    return list(pd.bdate_range("2024-01-02", periods=n))


def _bars(codes: Sequence[str], dates: Sequence[pd.Timestamp], closes: float | dict[str, Sequence[float]] = 100.0) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for code in codes:
        path = closes[code] if isinstance(closes, dict) else [float(closes)] * len(dates)
        for day, close in zip(dates, path):
            price = float(close)
            rows.append(
                {
                    "code": code,
                    "date": day,
                    "open": price,
                    "high": price * 1.02,
                    "low": price * 0.98,
                    "close": price,
                }
            )
    return pd.DataFrame(rows)


def _cand(
    code: str,
    dates: Sequence[pd.Timestamp],
    *,
    strategy: str = "fc_pos_break",
    family: str = "fundamental",
    entry_idx: int = 1,
    exit_idx: int = 2,
    entry_px: float = 100.0,
    exit_px: float | None = None,
    slip_entry: float = 0.0,
    slip_exit: float = 0.0,
    ind_l1: str | None = None,
    strength: float = 1.0,
    n_families: int = 2,
    lead_exp: float = 0.0,
    exit_reason: int = 4,
    exit_deferred: int = 0,
    exit_open: float | None = None,
    unbuyable: bool = False,
) -> dict[str, Any]:
    exit_price = entry_px if exit_px is None else exit_px
    return {
        "strategy": strategy,
        "family": family,
        "signal_date": dates[max(entry_idx - 1, 0)],
        "code": code,
        "entry_date": dates[entry_idx],
        "entry_px_raw": entry_px,
        "entry_open_raw": entry_px,
        "unbuyable": unbuyable,
        "exit_date": dates[exit_idx],
        "exit_offset": exit_idx - entry_idx,
        "exit_px_intended": exit_price,
        "exit_reason": exit_reason,
        "exit_deferred": exit_deferred,
        "exit_open": exit_price if exit_open is None else exit_open,
        "exit_close": exit_price,
        "adv20": 2_000_000_000.0,
        "adv_tier": 0,
        "slip_entry": slip_entry,
        "slip_exit": slip_exit,
        "strength": strength,
        "ind_l1": ind_l1,
        "n_families": n_families,
        "lead_exp": lead_exp,
        "realized_ret": 0.0,
    }


def _bars_with_exit_quote(
    dates: Sequence[pd.Timestamp],
    *,
    exit_open: float,
    exit_high: float,
    exit_low: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for idx, day in enumerate(dates):
        if idx == 2:
            open_, high, low, close = exit_open, exit_high, exit_low, 100.0
        else:
            open_, high, low, close = 100.0, 102.0, 98.0, 100.0
        rows.append({"code": "AAA", "date": day, "open": open_, "high": high, "low": low, "close": close})
    return pd.DataFrame(rows)


def _fill_for(reason: int, intended: float, exit_open: float, deferred: int = 0) -> tuple[float, dict[str, int]]:
    dates = _dates(4)
    bars = _bars_with_exit_quote(dates, exit_open=exit_open, exit_high=120.0, exit_low=80.0)
    cands = pd.DataFrame(
        [
            _cand(
                "AAA",
                dates,
                entry_idx=1,
                exit_idx=2,
                exit_px=intended,
                exit_reason=reason,
                exit_deferred=deferred,
                exit_open=exit_open,
            )
        ]
    )
    result = simulate(cands, bars, SimConfig())
    return float(result.trades.iloc[0]["exit_fill_px"]), result.rejects


def test_strategy_family_contract() -> None:
    assert len(STRATEGY_FAMILY) == 24
    assert FAMILY_CAP == {"trend": 3}
    assert STRATEGY_FAMILY["ma_crossover"] == "trend"
    assert STRATEGY_FAMILY["dragon_head"] == "limitup"
    assert STRATEGY_FAMILY["fc_pos_break"] == "fundamental"


def test_t_plus_one_never_sells_on_entry_date() -> None:
    dates = _dates(6)
    bars = _bars(["AAA", "BBB"], dates)
    cands = pd.DataFrame(
        [
            _cand("AAA", dates, strategy="ma_crossover", family="trend", entry_idx=1, exit_idx=2),
            # Malformed same-bar candidate: simulator must still defer the sale past entry.
            _cand("BBB", dates, strategy="ma_golden_cross", family="trend", entry_idx=2, exit_idx=2),
        ]
    )

    result = simulate(cands, bars, SimConfig(max_positions=2))

    assert len(result.trades) == 2
    assert (result.trades["exit_date"] > result.trades["entry_date"]).all()
    day_one = result.equity.loc[result.equity["date"] == dates[1]].iloc[0]
    assert day_one["n_positions"] == 1
    assert day_one["position_value"] > 0.0


def test_capital_cap_and_slot_reuse() -> None:
    dates = _dates(8)
    codes = [f"C{i:02d}" for i in range(15)]
    bars = _bars(codes + ["C15"], dates)
    cands = pd.DataFrame(
        [
            _cand(
                code,
                dates,
                strategy="washout_break",
                family="reversal",
                entry_idx=1,
                exit_idx=3 if code == "C00" else 7,
                ind_l1=f"ind_{code}",
                strength=float(15 - idx),
                lead_exp=float(15 - idx),
            )
            for idx, code in enumerate(codes)
        ]
        + [
            _cand(
                "C15",
                dates,
                strategy="washout_break",
                family="reversal",
                entry_idx=4,
                exit_idx=7,
                ind_l1="ind_C15",
                strength=1.0,
                lead_exp=1.0,
            )
        ]
    )

    result = simulate(cands, bars, SimConfig())

    first_day = result.equity.loc[result.equity["date"] == dates[1]].iloc[0]
    assert first_day["n_positions"] == 10
    assert result.rejects["max_positions"] == 5
    assert "C15" in set(result.trades["code"])
    assert "C00" in set(result.trades["code"])

    reuse_day = result.equity.loc[result.equity["date"] == dates[4]].iloc[0]
    assert reuse_day["n_positions"] == 10
    c15 = result.trades.loc[result.trades["code"] == "C15"].iloc[0]
    assert c15["entry_date"] == dates[4]


def test_cash_sizing_and_non_negative_cash() -> None:
    dates = _dates(6)
    codes = ["AAA", "BBB", "CCC"]
    bars = _bars(codes, dates)
    cands = pd.DataFrame(
        [
            _cand(code, dates, strategy="holder_conc_break", family="fundamental", ind_l1=f"ind_{code}")
            for code in codes
        ]
    )

    result = simulate(cands, bars, SimConfig(initial_capital=1_000_000.0, max_positions=10))

    assert len(result.trades) == 3
    for notional in result.trades["entry_notional"]:
        assert notional == pytest.approx(100_000.0, rel=0.01)
    assert (result.equity["cash"] >= 0.0).all()


def test_mtm_flat_and_up_day() -> None:
    dates = _dates(6)
    closes = [100.0, 100.0, 110.0, 110.0, 110.0, 110.0]
    bars = _bars(["AAA"], dates, closes={"AAA": closes})
    cands = pd.DataFrame(
        [_cand("AAA", dates, strategy="fc_pos_break", family="fundamental", entry_idx=1, exit_idx=5, exit_px=110.0)]
    )

    result = simulate(cands, bars, SimConfig(max_positions=10))
    equity = result.equity.set_index("date")["equity"]
    shares = float(result.trades.iloc[0]["shares"])

    assert equity.loc[dates[3]] == pytest.approx(equity.loc[dates[2]], abs=1e-9)
    assert equity.loc[dates[2]] - equity.loc[dates[1]] == pytest.approx(shares * (110.0 - 100.0), abs=1e-6)


def test_flat_day_sharpe_uses_zeros() -> None:
    returns = np.array([0.01] + [0.0] * 9)
    equity_values = 1_000_000.0 * np.cumprod(1.0 + returns)
    equity = pd.DataFrame(
        {
            "date": _dates(10),
            "cash": 0.0,
            "position_value": equity_values,
            "equity": equity_values,
            "n_positions": 1,
            "exposure": 1.0,
            "daily_ret": returns,
            "drawdown": 0.0,
        }
    )

    metrics = compute_metrics(equity, pd.DataFrame(), SimConfig())
    expected = float(pd.Series(returns).mean() / pd.Series(returns).std(ddof=1) * np.sqrt(252))

    assert metrics["sharpe"] == pytest.approx(expected, rel=1e-12)
    assert np.isfinite(metrics["sharpe"])


def test_roundtrip_cost_matches_additive_contract() -> None:
    dates = _dates(5)
    bars = _bars(["AAA"], dates)
    slip_entry, slip_exit = 0.0005, 0.0005
    cands = pd.DataFrame(
        [
            _cand(
                "AAA",
                dates,
                strategy="fc_pos_break",
                family="fundamental",
                entry_idx=1,
                exit_idx=2,
                entry_px=100.0,
                exit_px=100.0,
                slip_entry=slip_entry,
                slip_exit=slip_exit,
            )
        ]
    )

    result = simulate(cands, bars, SimConfig())
    trade = result.trades.iloc[0]

    assert trade["ret"] == pytest.approx(-(0.0035 + slip_entry + slip_exit), abs=1e-9)


@pytest.mark.parametrize(
    ("reason", "intended", "exit_open", "deferred", "expected"),
    [
        (1, 95.0, 90.0, 0, 90.0),
        (3, 96.0, 90.0, 0, 90.0),
        (2, 105.0, 110.0, 0, 110.0),
        (4, 97.0, 90.0, 0, 97.0),
        (2, 105.0, 110.0, 1, 105.0),
    ],
)
def test_exit_fill_table(reason: int, intended: float, exit_open: float, deferred: int, expected: float) -> None:
    fill, rejects = _fill_for(reason, intended, exit_open, deferred)

    assert fill == pytest.approx(expected)
    assert rejects == {}


def test_exit_fill_is_clamped_into_exit_bar_and_counted() -> None:
    dates = _dates(4)
    bars = _bars_with_exit_quote(dates, exit_open=100.0, exit_high=105.0, exit_low=95.0)
    cands = pd.DataFrame(
        [_cand("AAA", dates, entry_idx=1, exit_idx=2, exit_px=200.0, exit_reason=4, exit_open=100.0)]
    )

    result = simulate(cands, bars, SimConfig())

    assert float(result.trades.iloc[0]["exit_fill_px"]) == pytest.approx(105.0)
    assert result.rejects == {"fill_out_of_range": 1}


def test_determinism_under_input_shuffle() -> None:
    dates = _dates(8)
    codes = [f"D{i}" for i in range(6)]
    bars = _bars(codes, dates)
    rows = [
        _cand(
            code,
            dates,
            strategy="washout_break",
            family="reversal",
            entry_idx=1 + (idx % 3),
            exit_idx=5 + (idx % 2),
            ind_l1=f"ind_{idx}",
            strength=float(idx + 1),
            lead_exp=float(6 - idx),
            n_families=2 + (idx % 2),
        )
        for idx, code in enumerate(codes)
    ]

    ordered = simulate(pd.DataFrame(rows), bars, SimConfig())
    shuffled = simulate(pd.DataFrame(rows).sample(frac=1.0, random_state=42), bars, SimConfig())

    pd.testing.assert_frame_equal(ordered.equity, shuffled.equity)
    assert len(ordered.trades) == len(shuffled.trades)
    assert list(ordered.trades["code"]) == list(shuffled.trades["code"])
    assert ordered.rejects == shuffled.rejects


def test_entries_run_before_exits() -> None:
    dates = _dates(5)
    bars = _bars(["AAA", "BBB", "CCC", "DDD"], dates)
    cands = pd.DataFrame(
        [
            _cand("AAA", dates, strategy="fc_pos_break", family="fundamental", entry_idx=1, exit_idx=2),
            _cand("BBB", dates, strategy="holder_conc_break", family="fundamental", entry_idx=1, exit_idx=4),
            _cand("CCC", dates, strategy="inst_smart_break", family="fundamental", entry_idx=1, exit_idx=4),
            # The vacated slot is not usable until the next calendar day.
            _cand("DDD", dates, strategy="fc_pos_break", family="fundamental", entry_idx=2, exit_idx=3),
        ]
    )

    result = simulate(cands, bars, SimConfig(max_positions=3))

    assert result.rejects == {"max_positions": 1}
    assert "DDD" not in set(result.trades["code"])
    aaa = result.trades.loc[result.trades["code"] == "AAA"].iloc[0]
    assert aaa["exit_date"] == dates[2]
    exit_day = result.equity.loc[result.equity["date"] == dates[2]].iloc[0]
    assert exit_day["n_positions"] == 2


def test_single_position_per_code() -> None:
    dates = _dates(5)
    bars = _bars(["AAA", "BBB", "CCC"], dates)
    cands = pd.DataFrame(
        [
            _cand("AAA", dates, strategy="fc_pos_break", family="fundamental", entry_idx=1, exit_idx=3),
            _cand("BBB", dates, strategy="holder_conc_break", family="fundamental", entry_idx=1, exit_idx=4),
            _cand("CCC", dates, strategy="inst_smart_break", family="fundamental", entry_idx=1, exit_idx=4),
            # Same code while still held: rejected even though a slot is free.
            _cand("AAA", dates, strategy="fc_pos_break", family="fundamental", entry_idx=2, exit_idx=3),
        ]
    )

    result = simulate(cands, bars, SimConfig(max_positions=4))

    assert result.rejects == {"already_held": 1}
    assert result.trades["code"].is_unique
    assert len(result.trades) == 3
    aaa = result.trades.loc[result.trades["code"] == "AAA"].iloc[0]
    assert aaa["entry_date"] == dates[1]


# ---------------------------------------------------------------------------
# S3: candidate construction, de-clustered resonance, causal lead expectation
# ---------------------------------------------------------------------------


def _panel_bars(
    codes: Sequence[str],
    dates: Any,
    *,
    amount: float | dict[str, float] = 5_000_000_000.0,
    volume: float = 1_000_000.0,
) -> pd.DataFrame:
    """Full-panel bars with S1 ``next_open`` exit-path columns for period 10."""
    rows: list[dict[str, Any]] = []
    for code in codes:
        amount_value = amount[code] if isinstance(amount, dict) else amount
        for idx, day in enumerate(dates):
            price = 100.0 + idx
            rows.append(
                {
                    "code": code,
                    "date": day,
                    "open": price,
                    "high": price * 1.02,
                    "low": price * 0.98,
                    "close": price,
                    "close_raw": price,
                    "up_limit": price * 1.1,
                    "down_limit": price * 0.9,
                    "pct_chg": 0.0,
                    "amount": float(amount_value),
                    "volume": float(volume),
                    "atr20": 1.0,
                }
            )
    frame = pd.DataFrame(rows).sort_values(["code", "date"], kind="mergesort").reset_index(drop=True)

    entry: list[float] = []
    bar: list[Any] = []
    px: list[float] = []
    reason: list[int] = []
    defer: list[int] = []
    ret: list[float] = []
    for _code, group in frame.groupby("code", sort=False):
        opens = group["open"].to_numpy(dtype=float)
        closes = group["close"].to_numpy(dtype=float)
        n = len(group)
        for idx in range(n):
            if idx + 2 < n:
                entry.append(opens[idx + 1])
                bar.append(1)
                px.append(closes[idx + 2])
                reason.append(4)
                defer.append(0)
                ret.append(closes[idx + 2] / opens[idx + 1] - 1.0)
            else:
                entry.append(np.nan)
                bar.append(pd.NA)
                px.append(np.nan)
                reason.append(0)
                defer.append(0)
                ret.append(np.nan)

    frame["dyn_path_entry_10d_no"] = entry
    frame["dyn_path_bar_10d_no"] = pd.array(bar, dtype="Int16")
    frame["dyn_path_px_10d_no"] = px
    frame["dyn_path_reason_10d_no"] = np.asarray(reason, dtype=np.int8)
    frame["dyn_path_defer_10d_no"] = np.asarray(defer, dtype=np.int8)
    frame["dyn_ret_10d_no"] = ret
    return frame


def _signals(bars: pd.DataFrame, marks: Sequence[tuple[str, Any]]) -> pd.Series:
    signal = pd.Series(False, index=bars.index)
    for code, day in marks:
        signal.loc[(bars["code"] == code) & (bars["date"] == day)] = True
    return signal


def test_build_candidate_table_row_count_and_columns() -> None:
    dates = _dates(6)
    bars = _panel_bars(["AAA", "BBB"], dates)
    book = {
        "fc_pos_break": _signals(bars, [("AAA", dates[0]), ("AAA", dates[2]), ("BBB", dates[1])]),
        "ma_crossover": _signals(bars, [("BBB", dates[0]), ("AAA", dates[3])]),
    }
    rejects: Counter[str] = Counter()

    out = build_candidate_table(bars, book, cfg=SimConfig(adv_window=1), rejects=rejects)

    assert len(out) == 5
    assert list(out.columns) == list(_CAND_COLUMNS)
    assert set(out["strategy"]) == {"fc_pos_break", "ma_crossover"}
    assert (out.loc[out["strategy"] == "ma_crossover", "family"] == "trend").all()
    assert rejects == {"industry_cap_disabled": 1}


def test_lead_expectation_is_strictly_causal() -> None:
    dates = _dates(10)
    cands = pd.DataFrame(
        [
            {**_cand("AAA", dates, strategy="fc_pos_break", entry_idx=1, exit_idx=2), "realized_ret": 0.05},
            # Signal exactly on AAA's exit date: must NOT see that trade.
            {**_cand("CCC", dates, strategy="fc_pos_break", entry_idx=3, exit_idx=5), "realized_ret": 0.10},
            # Signal after AAA's exit date: sees exactly one closed trade.
            {**_cand("BBB", dates, strategy="fc_pos_break", entry_idx=4, exit_idx=7), "realized_ret": 0.20},
        ]
    )
    rejects: Counter[str] = Counter()

    out = apply_lead_expectation(cands, SimConfig(expectation_min_trades=1), rejects=rejects)

    assert len(out) == 1
    assert out.iloc[0]["code"] == "BBB"
    assert float(out.iloc[0]["lead_exp"]) == pytest.approx(0.05)
    assert rejects == {"lead_expectation_ineligible": 2}


def test_n_families_counts_distinct_non_excluded_families() -> None:
    dates = _dates(6)
    bars = _panel_bars(["AAA"], dates)
    mark = [("AAA", dates[0])]
    book = {
        "ma_crossover": _signals(bars, mark),
        "fc_pos_break": _signals(bars, mark),
        "dragon_head": _signals(bars, mark),
    }
    cands = build_candidate_table(bars, book, cfg=SimConfig(adv_window=1))

    out = apply_declustered_resonance(cands, SimConfig(min_resonance_families=2))

    assert len(cands) == 3
    assert set(out["strategy"]) == {"ma_crossover", "fc_pos_break"}
    assert (out["n_families"] == 2).all()


def test_vote_exclude_row_is_dropped_even_when_n_families_passes() -> None:
    dates = _dates(6)
    bars = _panel_bars(["AAA"], dates)
    mark = [("AAA", dates[0])]
    book = {
        "ma_crossover": _signals(bars, mark),
        "dragon_head": _signals(bars, mark),
    }
    cands = build_candidate_table(bars, book, cfg=SimConfig(adv_window=1))
    rejects: Counter[str] = Counter()

    out = apply_declustered_resonance(
        cands, SimConfig(min_resonance_families=1), rejects=rejects
    )

    assert list(out["strategy"]) == ["ma_crossover"]
    assert int(out.iloc[0]["n_families"]) == 1
    assert rejects == {"decluster_dropped": 1}


def test_collapse_keeps_best_row_per_entry_and_code() -> None:
    dates = _dates(6)
    rows = [
        {
            **_cand("AAA", dates, strategy="ma_crossover", family="trend", entry_idx=1, exit_idx=2),
            "lead_exp": 0.10,
            "n_families": 2,
            "strength": 9.0,
        },
        {
            **_cand("AAA", dates, strategy="fc_pos_break", family="fundamental", entry_idx=1, exit_idx=3),
            "lead_exp": 0.20,
            "n_families": 2,
            "strength": 1.0,
        },
        {
            **_cand("AAA", dates, strategy="inst_smart_break", family="fundamental", entry_idx=1, exit_idx=3),
            "lead_exp": 0.20,
            "n_families": 2,
            "strength": 1.0,
        },
    ]

    out = collapse_candidates(pd.DataFrame(rows))

    assert len(out) == 1
    assert out.iloc[0]["strategy"] == "fc_pos_break"
    assert list(out.columns) == list(_CAND_COLUMNS)


def test_entry_lag_and_missing_exit_are_rejected() -> None:
    lag_dates = pd.to_datetime(["2024-01-02", "2024-01-15", "2024-01-16", "2024-01-17"])
    ok_dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"])
    bars = pd.concat(
        [_panel_bars(["AAA"], lag_dates), _panel_bars(["BBB"], ok_dates)],
        ignore_index=True,
    )
    bars.loc[
        (bars["code"] == "BBB") & (bars["date"] == ok_dates[0]), "dyn_path_bar_10d_no"
    ] = pd.NA
    book = {"fc_pos_break": _signals(bars, [("AAA", lag_dates[0]), ("BBB", ok_dates[0])])}
    rejects: Counter[str] = Counter()

    out = build_candidate_table(bars, book, cfg=SimConfig(adv_window=1), rejects=rejects)

    assert out.empty
    assert rejects["entry_lag"] == 1
    assert rejects["no_exit"] == 1


def test_adv_tier_boundaries_drive_slippage() -> None:
    dates = _dates(4)
    bars = pd.concat(
        [
            _panel_bars(["HI"], dates, amount={"HI": 1_000_000_000.0}),
            _panel_bars(["LO"], dates, amount={"LO": 999_999_999.0}),
        ],
        ignore_index=True,
    )
    book = {"fc_pos_break": _signals(bars, [("HI", dates[1]), ("LO", dates[1])])}
    cfg = SimConfig(adv_window=2)

    out = build_candidate_table(bars, book, cfg=cfg).set_index("code")

    assert int(out.loc["HI", "adv_tier"]) == 0
    assert int(out.loc["LO", "adv_tier"]) == 1
    assert float(out.loc["HI", "adv20"]) == pytest.approx(1_000_000_000.0)
    assert float(out.loc["HI", "slip_entry"]) == cfg.adv_tiers[0][1]
    assert float(out.loc["HI", "slip_exit"]) == cfg.adv_tiers[0][1]
    assert float(out.loc["LO", "slip_entry"]) == cfg.adv_tiers[1][1]
    assert float(out.loc["LO", "slip_exit"]) == cfg.adv_tiers[1][1]


def _three_same_day_cands(dates: Sequence[pd.Timestamp]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            _cand(code, dates, strategy="holder_conc_break", family="fundamental", ind_l1=f"ind_{code}")
            for code in ("AAA", "BBB", "CCC")
        ]
    )


def test_frozen_config_hash_survives_sizing_defaults() -> None:
    """Legacy default config must keep the frozen hash: baselines stay comparable."""
    assert _config_hash(SimConfig()) == "5eea05195af3d89432af8b2a049fde2517d6a53b"


def test_deploy_aware_sizing_divides_equity_by_target_positions() -> None:
    dates = _dates(6)
    bars = _bars(["AAA", "BBB", "CCC"], dates)
    cfg = SimConfig(size_mode="deploy_aware", target_positions=5, cash_buffer_pct=0.0)

    result = simulate(_three_same_day_cands(dates), bars, cfg)

    assert len(result.trades) == 3
    for notional in result.trades["entry_notional"]:
        assert notional == pytest.approx(200_000.0, rel=0.01)  # 1,000,000 / 5
    assert (result.equity["cash"] >= 0.0).all()


def test_deploy_aware_sizing_keeps_cash_buffer_and_never_exceeds_cash() -> None:
    dates = _dates(6)
    bars = _bars(["AAA", "BBB", "CCC"], dates)
    # target 2 => 500k per entry; a 10% buffer keeps 100k of the 1M equity in cash,
    # so the third entry cannot be funded.
    cfg = SimConfig(size_mode="deploy_aware", target_positions=2, cash_buffer_pct=0.10)

    result = simulate(_three_same_day_cands(dates), bars, cfg)

    assert len(result.trades) == 2
    assert (result.equity["cash"] >= 0.0).all()
    assert float(result.equity["cash"].min()) >= 0.0


def test_deploy_aware_sizing_is_deterministic() -> None:
    dates = _dates(6)
    bars = _bars(["AAA", "BBB", "CCC"], dates)
    cands = _three_same_day_cands(dates)
    cfg = SimConfig(size_mode="deploy_aware", target_positions=5, cash_buffer_pct=0.05)

    first = simulate(cands, bars, cfg)
    second = simulate(cands, bars, cfg)

    pd.testing.assert_frame_equal(first.trades, second.trades)
    pd.testing.assert_frame_equal(first.equity, second.equity)


def test_deploy_aware_sizing_changes_config_hash() -> None:
    assert _config_hash(SimConfig(size_mode="deploy_aware", target_positions=5)) != _config_hash(SimConfig())


def _rank_mode_cands(dates: Sequence[pd.Timestamp]) -> pd.DataFrame:
    """Three same-day candidates whose lead_exp and resonance orders disagree."""
    return pd.DataFrame(
        [
            _cand("AAA", dates, lead_exp=0.10, n_families=1, ind_l1="ind_a"),
            _cand("BBB", dates, lead_exp=0.01, n_families=3, ind_l1="ind_b"),
            _cand("CCC", dates, lead_exp=0.05, n_families=2, ind_l1="ind_c"),
        ]
    )


def test_rank_mode_only_swaps_first_two_sort_keys() -> None:
    """Flipping the mode picks BBB instead of AAA; the later tie-breaks are untouched."""
    dates = _dates(6)
    bars = _bars(["AAA", "BBB", "CCC"], dates)
    cands = _rank_mode_cands(dates)

    lead = simulate(cands, bars, SimConfig(max_positions=1))
    resonance = simulate(cands, bars, SimConfig(max_positions=1, rank_mode="resonance_first"))

    assert list(lead.trades["code"]) == ["AAA"]
    assert list(resonance.trades["code"]) == ["BBB"]
    assert lead.rejects == resonance.rejects == Counter({"max_positions": 2})


def test_resonance_first_rank_mode_is_deterministic_under_input_shuffle() -> None:
    """Mergesort on the frozen key list keeps ties stable across row order."""
    dates = _dates(8)
    codes = [f"R{i}" for i in range(6)]
    bars = _bars(codes, dates)
    rows = [
        _cand(
            code,
            dates,
            strategy="washout_break",
            family="reversal",
            entry_idx=1 + (idx % 3),
            exit_idx=5 + (idx % 2),
            ind_l1=f"ind_{idx}",
            strength=float(idx + 1),
            lead_exp=float(6 - idx),
            n_families=2 + (idx % 3),
        )
        for idx, code in enumerate(codes)
    ]
    cfg = SimConfig(rank_mode="resonance_first")

    ordered = simulate(pd.DataFrame(rows), bars, cfg)
    shuffled = simulate(pd.DataFrame(rows).sample(frac=1.0, random_state=42), bars, cfg)

    pd.testing.assert_frame_equal(ordered.equity, shuffled.equity)
    assert list(ordered.trades["code"]) == list(shuffled.trades["code"])
    assert ordered.rejects == shuffled.rejects


def test_rank_mode_default_keeps_frozen_hash_and_override_changes_it() -> None:
    """The default stays out of ``config_hash``; an explicit non-default changes it."""
    assert SimConfig().rank_mode == "lead_exp_first"
    assert _config_hash(SimConfig(rank_mode="lead_exp_first")) == "5eea05195af3d89432af8b2a049fde2517d6a53b"
    assert _config_hash(SimConfig(rank_mode="resonance_first")) != _config_hash(SimConfig())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"size_mode": "deploy_aware"},
        {"size_mode": "deploy_aware", "target_positions": 0},
        {"size_mode": "deploy_aware", "target_positions": 11},
        {"size_mode": "deploy_aware", "target_positions": 5, "cash_buffer_pct": 1.0},
        {"size_mode": "bogus"},
        {"target_positions": 5},
        {"cash_buffer_pct": 0.05},
        {"rank_mode": "strength_first"},
    ],
)
def test_invalid_sim_config_is_rejected(kwargs: dict[str, Any]) -> None:
    """Every ``SimConfig.__post_init__`` guard rejects out-of-contract kwargs."""
    with pytest.raises(ValueError):
        SimConfig(**kwargs)
