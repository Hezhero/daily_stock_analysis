# -*- coding: utf-8 -*-
"""Behavioral tests for the pre-registered portfolio flip gate."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from scripts.portfolio_gate import (
    GATE_SPEC_HASH,
    PORTFOLIO_ESTIMATOR_ID,
    PORTFOLIO_GATE_SPEC_HASH,
    block_length,
    build_observations,
    main,
    moving_block_bootstrap,
    run_gate,
)

COST = 0.0


def make_run(
    n_dates: int,
    signals_per_date: int,
    holding_bars: int,
    mean_return: float,
    noise_sd: float,
    extra_mean: float,
    seed: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build a synthetic signal table plus matching run summary metrics."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2022-01-03", periods=n_dates)
    codes = [f"{600000 + index:06d}" for index in range(signals_per_date)]
    rows: list[dict[str, Any]] = []
    for day in dates:
        # Common date shock makes the date, not the signal, the resampling unit.
        date_shock = rng.normal(0.0, noise_sd)
        for code in codes:
            realized = mean_return + extra_mean + date_shock + rng.normal(0.0, noise_sd)
            rows.append(
                {
                    "strategy": "synthetic",
                    "signal_date": day,
                    "code": code,
                    "exit_offset": holding_bars,
                    "slip_entry": 0.0,
                    "slip_exit": 0.0,
                    "realized_ret": realized,
                }
            )

    signals = pd.DataFrame(rows)
    total = len(signals)
    config_hash = hashlib.sha1(f"{seed}|{extra_mean}|{holding_bars}".encode("utf-8")).hexdigest()
    summary: dict[str, Any] = {
        "run_id": f"run-{seed}-{extra_mean:+.4f}",
        "config_hash": config_hash,
        "config": {
            "explicit_cost_roundtrip": COST,
            "min_resonance_families": 1 if extra_mean > 0 else 2,
        },
        "metrics": {
            "cagr_oos": mean_return + extra_mean,
            "sharpe": 1.0 + extra_mean * 10.0,
            "maxDD_pct": 10.0,
            "n_trades": total,
            "oos_start": str(dates[0])[:10],
            "oos_end": str(dates[-1])[:10],
        },
        "window": f"{str(dates[0])[:10]}:{str(dates[-1])[:10]}",
    }
    return signals, summary


def _run(
    baseline: tuple[pd.DataFrame, dict[str, Any]],
    candidate: tuple[pd.DataFrame, dict[str, Any]],
    out_dir: Path,
    *,
    card_id: str = "card_a",
    data_hash: str = "synthetic-data-hash",
) -> dict[str, Any]:
    baseline_signals, baseline_summary = baseline
    candidate_signals, candidate_summary = candidate
    return run_gate(
        baseline_signals,
        candidate_signals,
        baseline_summary,
        candidate_summary,
        card_id=card_id,
        data_hash=data_hash,
        out_dir=out_dir,
    )


def _observations(holdings: list[int]) -> pd.DataFrame:
    dates = pd.bdate_range("2022-01-03", periods=len(holdings))
    signals = pd.DataFrame(
        {
            "strategy": ["synthetic"] * len(holdings),
            "signal_date": dates,
            "code": [f"{600000 + index:06d}" for index in range(len(holdings))],
            "exit_offset": holdings,
            "slip_entry": 0.0,
            "slip_exit": 0.0,
            "realized_ret": 0.01,
        }
    )
    return build_observations(signals, dates[0], COST)


def test_positive_effect_passes_gate(tmp_path: Path) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=11)
    candidate = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=12)

    payload = _run(baseline, candidate, tmp_path)

    assert payload["overall_pass"] is True
    assert payload["estimator"]["ci_low"] > 0.0
    assert payload["estimator"]["delta"] > 0.0
    assert "ci_excludes_zero" not in payload["rejection_reasons"]
    verdict_path = (
        tmp_path / "card_a" / payload["gate"]["version"]
        / f"{candidate[1]['config_hash']}.json"
    )
    assert verdict_path.exists()
    assert (tmp_path / "registry.jsonl").read_text(encoding="utf-8").strip()


def test_zero_effect_fails_ci_criterion(tmp_path: Path) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.02, 0.0, seed=21)
    candidate = make_run(100, 4, 1, 0.0, 0.02, 0.0, seed=22)
    offset = baseline[0]["realized_ret"].mean() - candidate[0]["realized_ret"].mean()
    candidate[0]["realized_ret"] = candidate[0]["realized_ret"] + offset

    payload = _run(baseline, candidate, tmp_path)

    assert payload["overall_pass"] is False
    assert "ci_excludes_zero" in payload["rejection_reasons"]
    assert payload["estimator"]["ci_low"] <= 0.0 <= payload["estimator"]["ci_high"]


def test_negative_effect_fails_ci_criterion(tmp_path: Path) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=31)
    candidate = make_run(100, 4, 1, 0.0, 0.002, -0.01, seed=32)

    payload = _run(baseline, candidate, tmp_path)

    assert payload["overall_pass"] is False
    assert payload["estimator"]["ci_high"] < 0.0
    assert "ci_excludes_zero" in payload["rejection_reasons"]


def test_resampling_is_by_date_not_by_signal(tmp_path: Path) -> None:
    def width(n_dates: int, signals_per_date: int) -> tuple[float, int, int]:
        baseline = make_run(n_dates, signals_per_date, 1, 0.0, 0.01, 0.0, seed=41)
        candidate = make_run(n_dates, signals_per_date, 1, 0.0, 0.01, 0.005, seed=42)
        oos_start = baseline[1]["metrics"]["oos_start"]
        obs_baseline = build_observations(baseline[0], oos_start, COST)
        obs_candidate = build_observations(candidate[0], oos_start, COST)
        length, _, _ = block_length(obs_baseline, obs_candidate)
        result = moving_block_bootstrap(obs_baseline, obs_candidate, length, 2000, 12345)
        return result["ci_high"] - result["ci_low"], len(obs_baseline), result["n_dates"]

    clustered_width, clustered_signals, clustered_dates = width(10, 10)
    spread_width, spread_signals, spread_dates = width(100, 1)

    assert clustered_signals == spread_signals == 100
    assert clustered_dates == 10
    assert spread_dates == 100
    assert clustered_width > spread_width


def test_block_length_uses_actual_holding_bars() -> None:
    baseline_one = _observations([1] * 50)
    candidate_one = _observations([1] * 50)
    assert block_length(baseline_one, candidate_one)[0] == 1

    baseline_ten = _observations([10] * 50)
    candidate_ten = _observations([10] * 50)
    assert block_length(baseline_ten, candidate_ten)[0] == 10

    mixed_baseline = _observations(list(range(1, 6)))
    mixed_candidate = _observations(list(range(1, 11)))
    length, p99_baseline, p99_candidate = block_length(mixed_baseline, mixed_candidate)
    assert length == max(int(np.ceil(p99_baseline)), int(np.ceil(p99_candidate))) == 10


def test_guardrail_failures_are_reported(tmp_path: Path) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=51)
    candidate = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=52)

    def with_metrics(**changes: float) -> tuple[pd.DataFrame, dict[str, Any]]:
        signals, summary = copy.deepcopy(candidate)
        summary["metrics"].update(changes)
        return signals, summary

    cases = [
        ("cagr_non_inferior", with_metrics(cagr_oos=-1.0)),
        ("sharpe_non_inferior", with_metrics(sharpe=-1.0)),
        ("maxdd_non_inferior", with_metrics(maxDD_pct=11.0)),
        ("maxdd_cap", with_metrics(maxDD_pct=26.0)),
        ("min_trades", with_metrics(n_trades=99)),
    ]
    for criterion_id, candidate_variant in cases:
        case_baseline = copy.deepcopy(baseline)
        if criterion_id == "maxdd_cap":
            # Keep the non-inferiority guardrail satisfied so only the cap fires.
            case_baseline[1]["metrics"]["maxDD_pct"] = 30.0
        payload = _run(case_baseline, candidate_variant, tmp_path / criterion_id)

        assert payload["overall_pass"] is False
        assert criterion_id in payload["rejection_reasons"]


def test_verdict_hash_is_deterministic_and_order_independent(tmp_path: Path) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=61)
    candidate = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=62)

    first = _run(baseline, candidate, tmp_path)
    second = _run(baseline, candidate, tmp_path)
    shuffled_baseline = (
        baseline[0].sample(frac=1.0, random_state=7).reset_index(drop=True),
        baseline[1],
    )
    shuffled_candidate = (
        candidate[0].sample(frac=1.0, random_state=8).reset_index(drop=True),
        candidate[1],
    )
    third = _run(shuffled_baseline, shuffled_candidate, tmp_path)

    assert first["verdict_hash"] == second["verdict_hash"]
    assert first["verdict_hash"] == third["verdict_hash"]


FROZEN_SIGNAL_VERDICT_HASH = "2c70774a05b85794c04be3446289b01fde322766785e7d8631115803744f68b2"
FROZEN_SIGNAL_TOP_LEVEL_KEYS = frozenset(
    {
        "criteria",
        "estimator",
        "gate",
        "inputs",
        "limitations",
        "overall_pass",
        "portfolio",
        "rejection_reasons",
        "variants",
        "verdict_hash",
        "warnings",
        "wfo",
    }
)
FROZEN_SIGNAL_ESTIMATOR_KEYS = frozenset(
    {
        "block_length",
        "block_length_inputs",
        "block_length_rule",
        "ci_high",
        "ci_level",
        "ci_low",
        "ci_method",
        "date_index_rule",
        "degenerate",
        "delta",
        "n_blocks",
        "n_dates_union",
        "name",
        "numpy_version",
        "resamples",
        "resampling_unit",
        "rng",
        "seed",
        "seed_derivation",
        "se",
        "signal_weighting",
        "valid_resamples",
    }
)
FROZEN_SIGNAL_CRITERIA_IDS = [
    "ci_excludes_zero",
    "cagr_non_inferior",
    "sharpe_non_inferior",
    "maxdd_non_inferior",
    "maxdd_cap",
    "min_trades",
]
FROZEN_REGISTRY_ROW_KEYS = frozenset(
    {
        "baseline_config_hash",
        "block_length",
        "candidate_config_hash",
        "card_id",
        "ci_high",
        "ci_low",
        "data_snapshot_hash",
        "delta",
        "gate_name",
        "gate_version",
        "oos_end",
        "oos_start",
        "overall_pass",
        "recorded_at",
        "registry_id",
        "rejection_reasons",
        "resamples",
        "se",
        "seed",
        "verdict_hash",
    }
)


def test_default_signal_verdict_is_frozen(tmp_path: Path) -> None:
    """Pin the default (signal) verdict so silent drift cannot ship.

    ``GATE_SPEC_HASH`` only covers the pre-registered spec text, so a change to
    payload assembly, seed derivation or the registry row can alter every
    published verdict while leaving it untouched. Freezing the observable
    output keeps ``git bisect`` meaningful: the same synthetic inputs must keep
    yielding the same hash, payload keys and registry fields.
    """
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=61)
    candidate = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=62)

    payload = _run(baseline, candidate, tmp_path)

    assert GATE_SPEC_HASH == FROZEN_GATE_SPEC_HASH
    assert payload["verdict_hash"] == FROZEN_SIGNAL_VERDICT_HASH
    assert set(payload) == FROZEN_SIGNAL_TOP_LEVEL_KEYS
    assert set(payload["estimator"]) == FROZEN_SIGNAL_ESTIMATOR_KEYS
    assert [criterion["id"] for criterion in payload["criteria"]] == FROZEN_SIGNAL_CRITERIA_IDS
    assert set(payload["gate"]) == {
        "card_id",
        "code_commit",
        "created_at",
        "name",
        "spec_hash",
        "version",
    }

    registry_rows = [
        json.loads(line)
        for line in (tmp_path / "registry.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(registry_rows) == 1
    assert set(registry_rows[0]) == FROZEN_REGISTRY_ROW_KEYS
    assert registry_rows[0]["verdict_hash"] == FROZEN_SIGNAL_VERDICT_HASH


def test_wfo_is_explicitly_not_required(tmp_path: Path) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=71)
    candidate = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=72)

    payload = _run(baseline, candidate, tmp_path)

    assert payload["wfo"]["required"] is False
    assert payload["wfo"]["folds"] is None
    assert "§8.1" in payload["wfo"]["reason"]
    assert "§4.5" in payload["wfo"]["reason"]


def _write_run_artifacts(
    directory: Path,
    stem: str,
    run: tuple[pd.DataFrame, dict[str, Any]],
) -> tuple[Path, Path]:
    """Persist a synthetic run as the CSV + JSON pair the CLI consumes."""
    signals, summary = run
    signals_path = directory / f"{stem}_signals.csv"
    summary_path = directory / f"{stem}.json"
    signals.to_csv(signals_path, index=False)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
    return summary_path, signals_path


def _cli_args(summary_b: Path, signals_b: Path, summary_c: Path, signals_c: Path, out_dir: Path) -> list[str]:
    return [
        "--baseline-summary", str(summary_b),
        "--baseline-signals", str(signals_b),
        "--candidate-summary", str(summary_c),
        "--candidate-signals", str(signals_c),
        "--card-id", "card-a",
        "--data-hash", "synthetic-data-hash",
        "--out-dir", str(out_dir),
    ]


def test_cli_passing_gate_exits_zero_and_writes_verdict(tmp_path: Path, capsys: Any) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=81)
    candidate = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=82)
    summary_b, signals_b = _write_run_artifacts(tmp_path, "base", baseline)
    summary_c, signals_c = _write_run_artifacts(tmp_path, "cand", candidate)
    out_dir = tmp_path / "gates"

    exit_code = main(_cli_args(summary_b, signals_b, summary_c, signals_c, out_dir))

    assert exit_code == 0
    assert "OVERALL: PASS" in capsys.readouterr().out
    verdicts = list((out_dir / "card-a" / "v1").glob("*.json"))
    assert len(verdicts) == 1
    assert (out_dir / "registry.jsonl").exists()


def test_cli_failing_gate_exits_one(tmp_path: Path, capsys: Any) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=83)
    candidate = make_run(100, 4, 1, 0.0, 0.002, -0.01, seed=84)
    summary_b, signals_b = _write_run_artifacts(tmp_path, "base", baseline)
    summary_c, signals_c = _write_run_artifacts(tmp_path, "cand", candidate)
    out_dir = tmp_path / "gates"

    exit_code = main(_cli_args(summary_b, signals_b, summary_c, signals_c, out_dir))

    assert exit_code == 1
    assert "OVERALL: FAIL" in capsys.readouterr().out
    verdicts = list((out_dir / "card-a" / "v1").glob("*.json"))
    assert len(verdicts) == 1
    written = json.loads(verdicts[0].read_text(encoding="utf-8"))
    assert written["overall_pass"] is False
    assert "ci_excludes_zero" in written["rejection_reasons"]


def test_cli_reports_unusable_inputs(tmp_path: Path, capsys: Any) -> None:
    missing = tmp_path / "missing.json"

    exit_code = main(_cli_args(missing, missing, missing, missing, tmp_path / "gates"))

    assert exit_code == 2
    assert "portfolio_gate:" in capsys.readouterr().err


FROZEN_GATE_SPEC_HASH = "88e87917bde12c345dd4d166108527f8db3858844b9cb8000e149b820315a800"


def make_equity_pair(
    n_dates: int,
    spread: float,
    noise_sd: float,
    seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build two equity curves sharing one return-noise path, candidate shifted by spread."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2022-01-03", periods=n_dates)
    base_rets = rng.normal(0.0, noise_sd, size=n_dates)
    base_rets[0] = 0.0
    cand_rets = base_rets + spread
    cand_rets[0] = 0.0

    def curve(rets: np.ndarray) -> pd.DataFrame:
        equity = 1_000_000.0 * np.cumprod(1.0 + rets)
        return pd.DataFrame(
            {
                "date": dates,
                "cash": 0.0,
                "position_value": equity,
                "equity": equity,
                "n_positions": 0,
                "exposure": 0.0,
                "daily_ret": rets,
                "drawdown": 0.0,
            }
        )

    return curve(base_rets), curve(cand_rets)


def _with_metrics(
    run: tuple[pd.DataFrame, dict[str, Any]],
    *,
    cagr: float,
    sharpe: float,
    max_dd: float,
    n_trades: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    signals, summary = copy.deepcopy(run)
    summary["metrics"].update(
        {"cagr_oos": cagr, "sharpe": sharpe, "maxDD_pct": max_dd, "n_trades": n_trades}
    )
    return signals, summary


def _portfolio_run(
    out_dir: Path,
    baseline_equity: pd.DataFrame,
    candidate_equity: pd.DataFrame,
    *,
    baseline_metrics: tuple[float, float, float, int] = (0.05, 1.0, 12.0, 200),
    candidate_metrics: tuple[float, float, float, int] = (0.30, 1.5, 8.0, 200),
    baseline_run: tuple[pd.DataFrame, dict[str, Any]] | None = None,
    candidate_run: tuple[pd.DataFrame, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    baseline_signals, baseline_summary = _with_metrics(
        baseline_run or make_run(300, 2, 1, 0.0, 0.002, 0.0, seed=101),
        cagr=baseline_metrics[0],
        sharpe=baseline_metrics[1],
        max_dd=baseline_metrics[2],
        n_trades=baseline_metrics[3],
    )
    candidate_signals, candidate_summary = _with_metrics(
        candidate_run or make_run(300, 2, 1, 0.0, 0.002, 0.0, seed=102),
        cagr=candidate_metrics[0],
        sharpe=candidate_metrics[1],
        max_dd=candidate_metrics[2],
        n_trades=candidate_metrics[3],
    )
    return run_gate(
        baseline_signals,
        candidate_signals,
        baseline_summary,
        candidate_summary,
        card_id="card_portfolio",
        data_hash="synthetic-data-hash",
        out_dir=out_dir,
        estimator="portfolio",
        equity_baseline=baseline_equity,
        equity_candidate=candidate_equity,
    )


def test_portfolio_estimator_detects_better_equity_curve(tmp_path: Path) -> None:
    baseline_equity, candidate_equity = make_equity_pair(300, spread=0.001, noise_sd=0.001, seed=7)

    payload = _portfolio_run(tmp_path, baseline_equity, candidate_equity)

    assert payload["gate"]["spec_hash"] == PORTFOLIO_GATE_SPEC_HASH
    assert payload["estimator"]["name"] == PORTFOLIO_ESTIMATOR_ID
    assert payload["estimator"]["delta_unit"] == "percentage_points_annualized_return"
    assert payload["estimator"]["resampling_unit"] == "oos_trading_day"
    assert payload["estimator"]["n_dates_union"] == 299
    assert payload["estimator"]["cagr_candidate_pct"] > payload["estimator"]["cagr_baseline_pct"]
    assert payload["estimator"]["delta"] > 0.0
    assert payload["estimator"]["ci_low"] > 0.0
    assert payload["overall_pass"] is True
    assert any("selection bias" in warning for warning in payload["warnings"])


def test_portfolio_estimator_identical_curves_are_degenerate(tmp_path: Path) -> None:
    baseline_equity, _ = make_equity_pair(300, spread=0.0, noise_sd=0.001, seed=8)

    payload = _portfolio_run(tmp_path, baseline_equity, baseline_equity.copy())

    assert payload["estimator"]["delta"] == 0.0
    assert payload["estimator"]["degenerate"] is True
    assert payload["overall_pass"] is False
    assert "ci_excludes_zero" in payload["rejection_reasons"]


def test_portfolio_estimator_rejects_date_mismatch(tmp_path: Path) -> None:
    baseline_equity, candidate_equity = make_equity_pair(300, spread=0.001, noise_sd=0.001, seed=9)

    with pytest.raises(ValueError, match="lengths must match"):
        _portfolio_run(tmp_path, baseline_equity, candidate_equity.iloc[:-1])


def test_portfolio_block_length_floor_is_one_trading_month(tmp_path: Path) -> None:
    baseline_equity, candidate_equity = make_equity_pair(300, spread=0.001, noise_sd=0.001, seed=10)

    payload = _portfolio_run(tmp_path, baseline_equity, candidate_equity)

    assert payload["estimator"]["block_length"] == 21
    assert payload["estimator"]["block_length_rule"] == "max(21, ceil(p99 holding_bars of each arm))"
    assert payload["estimator"]["block_length_inputs"] == {"baseline_p99": 1.0, "candidate_p99": 1.0}


def test_portfolio_estimator_is_deterministic(tmp_path: Path) -> None:
    baseline_equity, candidate_equity = make_equity_pair(300, spread=0.001, noise_sd=0.001, seed=11)

    first = _portfolio_run(tmp_path / "first", baseline_equity, candidate_equity)
    second = _portfolio_run(tmp_path / "second", baseline_equity, candidate_equity)

    assert first["estimator"] == second["estimator"]
    assert first["estimator"]["delta"] == second["estimator"]["delta"]
    assert first["estimator"]["ci_low"] == second["estimator"]["ci_low"]
    assert first["estimator"]["ci_high"] == second["estimator"]["ci_high"]


def test_registry_counts_variants_per_card_across_estimators(tmp_path: Path) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=111)
    candidate = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=112)
    baseline_equity, candidate_equity = make_equity_pair(100, spread=0.001, noise_sd=0.001, seed=12)

    signal_payload = run_gate(
        baseline[0],
        candidate[0],
        baseline[1],
        candidate[1],
        card_id="card_mixed",
        data_hash="synthetic-data-hash",
        out_dir=tmp_path,
    )
    portfolio_payload = run_gate(
        baseline[0],
        candidate[0],
        baseline[1],
        candidate[1],
        card_id="card_mixed",
        data_hash="synthetic-data-hash",
        out_dir=tmp_path,
        estimator="portfolio",
        equity_baseline=baseline_equity,
        equity_candidate=candidate_equity,
    )

    assert signal_payload["variants"]["tested_count"] == 1
    assert portfolio_payload["variants"]["tested_count"] == 2


def test_default_signal_estimator_payload_is_unchanged(tmp_path: Path) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=121)
    candidate = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=122)

    payload = _run(baseline, candidate, tmp_path)

    assert payload["estimator"]["name"] == "pooled_oos_unpaired_mean_net_return_delta"
    assert payload["gate"]["spec_hash"] == FROZEN_GATE_SPEC_HASH
    assert GATE_SPEC_HASH == FROZEN_GATE_SPEC_HASH
    assert "delta_unit" not in payload["estimator"]


def _write_equity(path: Path, equity: pd.DataFrame) -> Path:
    equity.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def test_cli_portfolio_requires_equity_args(tmp_path: Path, capsys: Any) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=131)
    candidate = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=132)
    summary_b, signals_b = _write_run_artifacts(tmp_path, "base", baseline)
    summary_c, signals_c = _write_run_artifacts(tmp_path, "cand", candidate)

    exit_code = main(_cli_args(summary_b, signals_b, summary_c, signals_c, tmp_path / "gates") + ["--estimator", "portfolio"])

    assert exit_code == 2
    assert "requires equity_baseline and equity_candidate" in capsys.readouterr().err


def test_cli_signal_rejects_equity_args(tmp_path: Path, capsys: Any) -> None:
    baseline = make_run(100, 4, 1, 0.0, 0.002, 0.0, seed=133)
    candidate = make_run(100, 4, 1, 0.0, 0.002, 0.01, seed=134)
    summary_b, signals_b = _write_run_artifacts(tmp_path, "base", baseline)
    summary_c, signals_c = _write_run_artifacts(tmp_path, "cand", candidate)
    baseline_equity, _ = make_equity_pair(100, spread=0.0, noise_sd=0.001, seed=13)
    equity_path = _write_equity(tmp_path / "equity.csv", baseline_equity)

    exit_code = main(
        _cli_args(summary_b, signals_b, summary_c, signals_c, tmp_path / "gates")
        + ["--baseline-equity", str(equity_path)]
    )

    assert exit_code == 2
    assert "only accepted when estimator='portfolio'" in capsys.readouterr().err
