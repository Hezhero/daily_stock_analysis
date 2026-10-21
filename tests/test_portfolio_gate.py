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

from scripts.portfolio_gate import (
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
