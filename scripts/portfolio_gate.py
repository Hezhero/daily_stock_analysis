# -*- coding: utf-8 -*-
"""Pre-registered date-block bootstrap flip gate for portfolio runs.

Side-effect free at import time: no logging configuration, no environment reads,
no file I/O, and no dependency on the backtest driver or the capital-constrained
simulator. Runtime functions consume the additive signal table and the matching
run summary produced by ``scripts.portfolio_sim`` / the backtest driver.

The estimand is the unpaired, signal-weighted difference between the candidate
and baseline pooled out-of-sample (OOS) per-signal net returns. Resampling runs
over the sorted union of signal dates with a non-circular moving-block scheme;
the drawn date index is applied jointly to both arms.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence, Tuple, cast

import numpy as np
import pandas as pd

GATE_NAME = "portfolio_flip_gate"
GATE_VERSION = "v1"
ESTIMATOR_ID = "pooled_oos_unpaired_mean_net_return_delta"
PORTFOLIO_ESTIMATOR_ID = "oos_daily_portfolio_cagr_block_delta"
ESTIMATORS: Tuple[str, ...] = ("signal", "portfolio")

RESAMPLES = 2000
CI_LEVEL = 0.95
CI_QUANTILES: Tuple[float, float] = (2.5, 97.5)
MAX_DD_CAP_PCT = 25.0
MIN_FILLED_TRADES = 100
DEFAULT_OUT_DIR = "result/gates"
REGISTRY_FILENAME = "registry.jsonl"

_OBS_COLUMNS: Tuple[str, ...] = (
    "strategy",
    "signal_date",
    "code",
    "net_return",
    "holding_bars",
)
_REQUIRED_SIGNAL_COLUMNS: Tuple[str, ...] = (
    "strategy",
    "signal_date",
    "code",
    "exit_offset",
    "slip_entry",
    "slip_exit",
    "realized_ret",
)
_REQUIRED_METRIC_KEYS: Tuple[str, ...] = ("cagr_oos", "sharpe", "maxDD_pct", "n_trades", "oos_start")

# Frozen estimator description. Its hash is recorded in every verdict so a
# verdict cannot be silently reinterpreted after the gate implementation changes.
_GATE_SPEC: Mapping[str, Any] = {
    "gate_name": GATE_NAME,
    "gate_version": GATE_VERSION,
    "estimator_id": ESTIMATOR_ID,
    "observations": "pooled OOS signals keyed by (strategy, signal_date, code)",
    "net_return": "realized_ret - slip_entry - slip_exit - explicit_cost_roundtrip",
    "estimand": "mean(candidate net_return) - mean(baseline net_return), unpaired",
    "resampling": "non-circular moving-block bootstrap over the sorted union of signal dates",
    "date_index_rule": "k=ceil(n_dates/L); starts uniform over 1..(n_dates-L+1); joint across arms",
    "block_length_rule": "max(ceil(p99 of each arm's actual holding_bars))",
    "ci_method": "percentile",
    "ci_quantiles": list(CI_QUANTILES),
    "resamples": RESAMPLES,
    "signal_weighting": "signal_count",
    "rng": "numpy.random.PCG64",
}

GATE_SPEC_HASH = hashlib.sha256(
    json.dumps(_GATE_SPEC, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
).hexdigest()

# Frozen portfolio-level estimator description, selected only with ``--estimator
# portfolio``. Kept separate so the signal-level spec hash above stays stable.
PORTFOLIO_BLOCK_MIN = 21
TRADING_DAYS_PER_YEAR = 252

_PORTFOLIO_GATE_SPEC: Mapping[str, Any] = {
    "gate_name": GATE_NAME,
    "gate_version": GATE_VERSION,
    "estimator_id": PORTFOLIO_ESTIMATOR_ID,
    "observations": "OOS trading days of the portfolio equity curve keyed by date",
    "daily_return": "equity_t / equity_{t-1} - 1 on the full series, then sliced to oos_start..oos_end",
    "estimand": "CAGR(candidate) - CAGR(baseline), annualized geometric return difference in percentage points",
    "resampling": "non-circular moving-block bootstrap over the shared OOS trading-day axis",
    "date_index_rule": "k=ceil(n_dates/L); starts uniform over 1..(n_dates-L+1); joint across arms",
    "block_length_rule": "max(21, ceil(p99 holding_bars of each arm))",
    "ci_method": "percentile",
    "ci_quantiles": list(CI_QUANTILES),
    "resamples": RESAMPLES,
    "trading_days_per_year": TRADING_DAYS_PER_YEAR,
    "rng": "numpy.random.PCG64",
}

PORTFOLIO_GATE_SPEC_HASH = hashlib.sha256(
    json.dumps(_PORTFOLIO_GATE_SPEC, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
).hexdigest()

_LIMITATIONS: Tuple[str, ...] = (
    "The estimand is a strategy-level per-signal mean contrast, not a paired "
    "treatment effect on the same signals.",
    "Margin-0 guardrails are descriptive directional constraints, not inferential "
    "non-inferiority tests.",
    "Effective independent blocks are approximately n_dates/L; n_dates is the "
    "resampling unit, not the independent sample size.",
    "The per-signal net return is a signal-level path return (realized_ret minus the "
    "same cost terms the trade row uses), not the realized fill return.",
)

_WFO_REASON = (
    "The metric contract §8.1 fold/purge/embargo clauses govern the WFO engine, and "
    "docs/backtest-experiment-cards.md §4.5 reserves WFO for changes to signal or exit "
    "definitions; this parameter/capital change is adjudicated by the bootstrap gate "
    "because no complete WFO engine exists."
)


def _require_columns(frame: pd.DataFrame, columns: Sequence[str], label: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def _require_mapping(container: Mapping[str, Any], key: str, label: str) -> Mapping[str, Any]:
    value = container.get(key)
    if not isinstance(value, Mapping):
        raise ValueError(f"{label}.{key} must be a mapping")
    return value


def _finite_float(value: Any, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be numeric, got {value!r}") from exc
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite, got {value!r}")
    return result


def _numeric_float(value: Any, label: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be numeric, got {value!r}") from exc


def _normalized_date(value: Any, label: str) -> str:
    if value is None:
        raise ValueError(f"{label} is missing")
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} is not a valid date: {value!r}") from exc
    if bool(pd.isna(stamp)):
        raise ValueError(f"{label} is not a valid date: {value!r}")
    stamp = cast(pd.Timestamp, stamp)
    return stamp.strftime("%Y-%m-%d")


def derive_seed(
    gate_version: str,
    card_id: str,
    baseline_config_hash: str,
    candidate_config_hash: str,
    data_hash: str,
    oos_start: str,
    oos_end: str,
    block_length: int,
    resamples: int,
    estimator_id: str,
) -> int:
    """Derive the single gate seed from the frozen pre-registration inputs."""
    joined = "|".join(
        str(value)
        for value in (
            gate_version,
            card_id,
            baseline_config_hash,
            candidate_config_hash,
            data_hash,
            oos_start,
            oos_end,
            int(block_length),
            int(resamples),
            estimator_id,
        )
    )
    return int.from_bytes(hashlib.sha256(joined.encode("utf-8")).digest()[:8], "big")


def build_observations(
    signals: pd.DataFrame,
    oos_start: Any,
    explicit_cost_roundtrip: float,
) -> pd.DataFrame:
    """Normalize an additive signal table into OOS net-return observations."""
    _require_columns(signals, _REQUIRED_SIGNAL_COLUMNS, "signals")
    cost = _finite_float(explicit_cost_roundtrip, "explicit_cost_roundtrip")
    if cost < 0.0:
        raise ValueError("explicit_cost_roundtrip must be non-negative")

    frame = signals.loc[:, list(_REQUIRED_SIGNAL_COLUMNS)].copy()
    frame["signal_date"] = pd.to_datetime(frame["signal_date"], errors="raise")

    if bool(np.asarray(frame.loc[:, list(_REQUIRED_SIGNAL_COLUMNS)].isna()).any()):
        raise ValueError("signals contain NaN in required fields")

    frame["strategy"] = frame["strategy"].astype(str)
    frame["code"] = frame["code"].astype(str)

    start = pd.Timestamp(oos_start)
    if bool(pd.isna(start)):
        raise ValueError("oos_start is not a valid date")
    frame = frame.loc[frame["signal_date"] >= start].copy()

    for column in ("exit_offset", "slip_entry", "slip_exit", "realized_ret"):
        frame[column] = pd.to_numeric(frame[column], errors="raise")

    frame["holding_bars"] = frame["exit_offset"].astype("int64")
    frame["net_return"] = (
        frame["realized_ret"].astype(np.float64)
        - frame["slip_entry"].astype(np.float64)
        - frame["slip_exit"].astype(np.float64)
        - cost
    )

    # Value-based tie-breakers make duplicate resolution independent of row order.
    frame = frame.sort_values(
        ["signal_date", "code", "strategy", "net_return", "holding_bars"],
        kind="mergesort",
    )
    frame = frame.drop_duplicates(subset=["strategy", "signal_date", "code"], keep="first")
    frame = frame.loc[:, list(_OBS_COLUMNS)]
    return frame.sort_values(["signal_date", "code", "strategy"], kind="mergesort").reset_index(drop=True)


def _p99_holding(observations: pd.DataFrame, label: str) -> float:
    if observations.empty:
        raise ValueError(f"{label} has zero OOS signals")
    bars = observations["holding_bars"].to_numpy(dtype=np.float64)
    return float(np.quantile(bars, 0.99))


def block_length(
    obs_baseline: pd.DataFrame,
    obs_candidate: pd.DataFrame,
) -> Tuple[int, float, float]:
    """Return ``(L, baseline_p99, candidate_p99)`` using actual holding bars."""
    p99_baseline = _p99_holding(obs_baseline, "baseline")
    p99_candidate = _p99_holding(obs_candidate, "candidate")
    length = max(1, int(math.ceil(p99_baseline)), int(math.ceil(p99_candidate)))
    return length, p99_baseline, p99_candidate


def _date_counts(
    observations: pd.DataFrame,
    dates: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    positions = np.searchsorted(
        dates,
        observations["signal_date"].to_numpy(dtype="datetime64[ns]"),
    )
    values = observations["net_return"].to_numpy(dtype=np.float64)
    sums = np.bincount(positions, weights=values, minlength=len(dates))
    counts = np.bincount(positions, minlength=len(dates))
    return sums, counts


def moving_block_bootstrap(
    obs_baseline: pd.DataFrame,
    obs_candidate: pd.DataFrame,
    L: int,
    resamples: int,
    seed: int,
) -> dict[str, Any]:
    """Non-circular moving-block bootstrap over the joint sorted date union."""
    if int(L) < 1:
        raise ValueError("block length L must be >= 1")
    if int(resamples) < 1:
        raise ValueError("resamples must be >= 1")
    if obs_baseline.empty:
        raise ValueError("baseline has zero OOS signals")
    if obs_candidate.empty:
        raise ValueError("candidate has zero OOS signals")

    block = int(L)
    dates = np.sort(
        np.unique(
            np.concatenate(
                [
                    obs_baseline["signal_date"].to_numpy(dtype="datetime64[ns]"),
                    obs_candidate["signal_date"].to_numpy(dtype="datetime64[ns]"),
                ]
            )
        )
    )
    n_dates = int(len(dates))
    if n_dates < block:
        raise ValueError(f"n_dates ({n_dates}) must be >= block length ({block})")

    base_sums, base_counts = _date_counts(obs_baseline, dates)
    cand_sums, cand_counts = _date_counts(obs_candidate, dates)

    base_net = obs_baseline["net_return"].to_numpy(dtype=np.float64)
    cand_net = obs_candidate["net_return"].to_numpy(dtype=np.float64)
    point_delta = float(cand_net.mean() - base_net.mean())

    n_blocks = int(math.ceil(n_dates / block))
    start_high = n_dates - block + 2  # integers in [1, n_dates - L + 1]
    rng = np.random.Generator(np.random.PCG64(int(seed)))
    deltas = np.empty(int(resamples), dtype=np.float64)
    valid = 0

    for replicate in range(int(resamples)):
        starts = rng.integers(low=1, high=start_high, size=n_blocks)
        index = np.concatenate(
            [np.arange(start - 1, start - 1 + block, dtype=np.int64) for start in starts]
        )[:n_dates]
        base_n = int(base_counts[index].sum())
        cand_n = int(cand_counts[index].sum())
        if base_n == 0 or cand_n == 0:
            continue
        base_mean = float(base_sums[index].sum()) / base_n
        cand_mean = float(cand_sums[index].sum()) / cand_n
        deltas[valid] = cand_mean - base_mean
        valid += 1

    valid_deltas = deltas[:valid]
    if valid < 1:
        degenerate = True
    else:
        degenerate = bool(float(np.ptp(valid_deltas)) == 0.0)
    if valid < 1:
        ci_low = float("nan")
        ci_high = float("nan")
        se = float("nan")
    else:
        ci_low, ci_high = (float(value) for value in np.percentile(valid_deltas, CI_QUANTILES))
        se = float(np.std(valid_deltas, ddof=1)) if valid > 1 else 0.0

    return {
        "delta": point_delta,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "se": se,
        "n_dates": n_dates,
        "n_blocks": n_blocks,
        "valid_resamples": int(valid),
        "degenerate": bool(degenerate),
        "resamples": int(resamples),
        "block_length": block,
        "ci_level": CI_LEVEL,
    }


def build_portfolio_returns(
    equity: pd.DataFrame,
    oos_start: Any,
    oos_end: Any,
) -> pd.DataFrame:
    """Compute daily equity returns on the full series, then slice to the OOS window."""
    _require_columns(equity, ("date", "equity"), "equity")
    frame = equity.loc[:, ["date", "equity"]].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    frame["equity"] = pd.to_numeric(frame["equity"], errors="raise")
    frame = frame.sort_values("date", kind="mergesort").reset_index(drop=True)
    if bool(frame["date"].duplicated().any()):
        raise ValueError("equity contains duplicate dates")
    values = frame["equity"].to_numpy(dtype=np.float64)
    if not bool(np.isfinite(values).all()) or bool((values <= 0.0).any()):
        raise ValueError("equity must be finite and positive")

    start = pd.Timestamp(oos_start)
    end = pd.Timestamp(oos_end)
    if bool(pd.isna(start)) or bool(pd.isna(end)):
        raise ValueError("oos_start and oos_end must be valid dates")

    frame["ret"] = frame["equity"] / frame["equity"].shift(1) - 1.0
    sliced = frame.loc[(frame["date"] >= start) & (frame["date"] <= end), ["date", "ret"]]
    sliced = sliced.dropna(subset=["ret"]).reset_index(drop=True)
    if sliced.empty:
        raise ValueError("OOS window contains no equity returns")
    return sliced


def _cagr_pct(returns: np.ndarray) -> float:
    growth = float(np.prod(1.0 + returns))
    if not math.isfinite(growth) or growth <= 0.0:
        raise ValueError("per-arm growth factor must be finite and positive")
    return float((growth ** (TRADING_DAYS_PER_YEAR / len(returns)) - 1.0) * 100.0)


def portfolio_block_bootstrap(
    base_returns: pd.DataFrame,
    cand_returns: pd.DataFrame,
    block: int,
    resamples: int,
    seed: int,
) -> dict[str, Any]:
    """Joint non-circular moving-block bootstrap of the annualized return delta."""
    if int(block) < 1:
        raise ValueError("block length must be >= 1")
    if int(resamples) < 1:
        raise ValueError("resamples must be >= 1")
    _require_columns(base_returns, ("date", "ret"), "baseline returns")
    _require_columns(cand_returns, ("date", "ret"), "candidate returns")

    n_dates = int(len(base_returns))
    if n_dates < 1:
        raise ValueError("baseline returns are empty")
    if int(len(cand_returns)) != n_dates:
        raise ValueError("baseline and candidate OOS return lengths must match")
    base_dates = base_returns["date"].to_numpy(dtype="datetime64[ns]")
    cand_dates = cand_returns["date"].to_numpy(dtype="datetime64[ns]")
    if not bool(np.array_equal(base_dates, cand_dates)):
        raise ValueError("baseline and candidate OOS date arrays must match exactly")

    block = int(block)
    if n_dates < block:
        raise ValueError(f"n_dates ({n_dates}) must be >= block length ({block})")

    base_ret = base_returns["ret"].to_numpy(dtype=np.float64)
    cand_ret = cand_returns["ret"].to_numpy(dtype=np.float64)
    if not bool(np.isfinite(base_ret).all()) or not bool(np.isfinite(cand_ret).all()):
        raise ValueError("portfolio returns must be finite")

    cagr_baseline_pct = _cagr_pct(base_ret)
    cagr_candidate_pct = _cagr_pct(cand_ret)
    point_delta = cagr_candidate_pct - cagr_baseline_pct

    n_blocks = int(math.ceil(n_dates / block))
    start_high = n_dates - block + 2  # integers in [1, n_dates - L + 1]
    rng = np.random.Generator(np.random.PCG64(int(seed)))
    deltas = np.empty(int(resamples), dtype=np.float64)
    valid = 0

    for _ in range(int(resamples)):
        starts = rng.integers(low=1, high=start_high, size=n_blocks)
        index = np.concatenate(
            [np.arange(start - 1, start - 1 + block, dtype=np.int64) for start in starts]
        )[:n_dates]
        base_growth = float(np.prod(1.0 + base_ret[index]))
        cand_growth = float(np.prod(1.0 + cand_ret[index]))
        if (
            not math.isfinite(base_growth)
            or not math.isfinite(cand_growth)
            or base_growth <= 0.0
            or cand_growth <= 0.0
        ):
            continue
        base_cagr_pct = (base_growth ** (TRADING_DAYS_PER_YEAR / n_dates) - 1.0) * 100.0
        cand_cagr_pct = (cand_growth ** (TRADING_DAYS_PER_YEAR / n_dates) - 1.0) * 100.0
        deltas[valid] = cand_cagr_pct - base_cagr_pct
        valid += 1

    valid_deltas = deltas[:valid]
    if valid < 1:
        degenerate = True
        ci_low = float("nan")
        ci_high = float("nan")
        se = float("nan")
    else:
        degenerate = bool(float(np.ptp(valid_deltas)) == 0.0)
        ci_low, ci_high = (float(value) for value in np.percentile(valid_deltas, CI_QUANTILES))
        se = float(np.std(valid_deltas, ddof=1)) if valid > 1 else 0.0

    return {
        "delta": point_delta,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "se": se,
        "n_dates": n_dates,
        "n_blocks": n_blocks,
        "valid_resamples": int(valid),
        "degenerate": bool(degenerate),
        "resamples": int(resamples),
        "block_length": block,
        "ci_level": CI_LEVEL,
        "cagr_baseline_pct": cagr_baseline_pct,
        "cagr_candidate_pct": cagr_candidate_pct,
        "delta_unit": "percentage_points_annualized_return",
    }


def evaluate_criteria(
    delta_ci: Mapping[str, Any],
    baseline_metrics: Mapping[str, Any],
    candidate_metrics: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Apply the five pre-registered criteria in a fixed order."""
    ci_low = _numeric_float(delta_ci.get("ci_low"), "delta_ci.ci_low")
    cagr_base = _finite_float(baseline_metrics.get("cagr_oos"), "baseline.metrics.cagr_oos")
    cagr_cand = _finite_float(candidate_metrics.get("cagr_oos"), "candidate.metrics.cagr_oos")
    sharpe_base = _finite_float(baseline_metrics.get("sharpe"), "baseline.metrics.sharpe")
    sharpe_cand = _finite_float(candidate_metrics.get("sharpe"), "candidate.metrics.sharpe")
    maxdd_base = _finite_float(baseline_metrics.get("maxDD_pct"), "baseline.metrics.maxDD_pct")
    maxdd_cand = _finite_float(candidate_metrics.get("maxDD_pct"), "candidate.metrics.maxDD_pct")
    filled_cand = _finite_float(candidate_metrics.get("n_trades"), "candidate.metrics.n_trades")

    return [
        {
            "id": "ci_excludes_zero",
            "type": "superiority",
            "label": "Pooled OOS delta 95% CI lower bound is above zero",
            "observed": ci_low,
            "comparator": ">",
            "threshold": 0.0,
            "margin": 0.0,
            "pass": bool(math.isfinite(ci_low) and ci_low > 0.0),
        },
        {
            "id": "cagr_non_inferior",
            "type": "guardrail",
            "label": "OOS CAGR is not below the baseline (descriptive, margin 0)",
            "observed": cagr_cand,
            "comparator": ">=",
            "threshold": cagr_base,
            "margin": 0.0,
            "pass": bool(cagr_cand >= cagr_base),
        },
        {
            "id": "sharpe_non_inferior",
            "type": "guardrail",
            "label": "Sharpe is not below the baseline (descriptive, margin 0)",
            "observed": sharpe_cand,
            "comparator": ">=",
            "threshold": sharpe_base,
            "margin": 0.0,
            "pass": bool(sharpe_cand >= sharpe_base),
        },
        {
            "id": "maxdd_non_inferior",
            "type": "guardrail",
            "label": "Max drawdown is not above the baseline (descriptive, margin 0)",
            "observed": maxdd_cand,
            "comparator": "<=",
            "threshold": maxdd_base,
            "margin": 0.0,
            "pass": bool(maxdd_cand <= maxdd_base),
        },
        {
            "id": "maxdd_cap",
            "type": "absolute",
            "label": "Max drawdown is at or below the 25% cap",
            "observed": maxdd_cand,
            "comparator": "<=",
            "threshold": MAX_DD_CAP_PCT,
            "margin": 0.0,
            "pass": bool(maxdd_cand <= MAX_DD_CAP_PCT),
        },
        {
            "id": "min_trades",
            "type": "absolute",
            "label": "Filled candidate trades meet the 100-trade floor",
            "observed": filled_cand,
            "comparator": ">=",
            "threshold": float(MIN_FILLED_TRADES),
            "margin": 0.0,
            "pass": bool(filled_cand >= MIN_FILLED_TRADES),
        },
    ]


def _window_end(summary: Mapping[str, Any]) -> str | None:
    window = summary.get("window")
    if isinstance(window, str) and ":" in window:
        return _normalized_date(window.rsplit(":", 1)[1], "summary.window end")
    return None


def _resolve_oos_end(summary: Mapping[str, Any], explicit: str | None) -> str:
    if explicit is not None:
        return _normalized_date(explicit, "oos_end")
    metrics = _require_mapping(summary, "metrics", "summary")
    value = metrics.get("oos_end")
    if value is not None:
        return _normalized_date(value, "summary.metrics.oos_end")
    value = _window_end(summary)
    if value is not None:
        return value
    raise ValueError("cannot determine oos_end; pass oos_end explicitly or provide summary.window")


def _run_id(summary: Mapping[str, Any], data_hash: str, config_hash: str) -> str:
    value = summary.get("run_id")
    if isinstance(value, str) and value:
        return value
    return f"{data_hash[:12]}:{config_hash[:12]}"


def _n_filled(summary: Mapping[str, Any], metrics: Mapping[str, Any], label: str) -> int:
    funnel = summary.get("funnel")
    if isinstance(funnel, Mapping) and funnel.get("filled") is not None:
        return int(funnel["filled"])
    value = metrics.get("n_trades")
    if value is None:
        raise ValueError(f"{label} summary is missing both funnel.filled and metrics.n_trades")
    return int(value)


def _registered_variant_ids(
    registry_path: Path,
    card_id: str,
    gate_version: str,
) -> set[str]:
    """Distinct registry ids for one card across gate versions and estimators."""
    if not registry_path.exists():
        return set()
    identifiers: set[str] = set()
    with registry_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            entry = json.loads(line)
            # Entries written before the portfolio estimator have no estimator_id
            # field and count as signal-level runs for the same card.
            if entry.get("card_id") != card_id:
                continue
            identifier = entry.get("registry_id")
            if isinstance(identifier, str) and identifier:
                identifiers.add(identifier)
    return identifiers


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.floating, float)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def _canonical_hash(payload: Mapping[str, Any]) -> str:
    clone = copy.deepcopy(dict(payload))
    gate = clone.get("gate")
    if isinstance(gate, dict):
        gate.pop("created_at", None)
    text = json.dumps(
        _json_safe(clone),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def verdict_path(payload: Mapping[str, Any], out_dir: str | Path) -> Path:
    """Content-addressed verdict location: <out_dir>/<card_id>/<gate_version>/<candidate_hash>.json."""
    gate = _require_mapping(payload, "gate", "payload")
    inputs = _require_mapping(payload, "inputs", "payload")
    candidate = _require_mapping(inputs, "candidate", "payload.inputs")
    return Path(out_dir) / str(gate["card_id"]) / str(gate["version"]) / f"{candidate['config_hash']}.json"


def write_verdict(payload: Mapping[str, Any], out_dir: str | Path) -> str:
    """Write the verdict to its content-addressed path and return the path."""
    path = verdict_path(payload, out_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(
            _json_safe(payload),
            handle,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        handle.write("\n")
    return str(path)


def append_registry(entry: Mapping[str, Any], path: str | Path) -> None:
    """Append one variant entry to the append-only JSONL registry."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(_json_safe(entry), ensure_ascii=False, sort_keys=True, allow_nan=False)
    with target.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(line + "\n")


def run_gate(
    signals_baseline: pd.DataFrame,
    signals_candidate: pd.DataFrame,
    summary_baseline: Mapping[str, Any],
    summary_candidate: Mapping[str, Any],
    *,
    card_id: str,
    data_hash: str,
    out_dir: str | Path = DEFAULT_OUT_DIR,
    oos_end: str | None = None,
    gate_version: str = GATE_VERSION,
    code_commit: str = "unknown",
    preregistered_primary: bool = True,
    tested_count: int | None = None,
    run_id_baseline: str | None = None,
    run_id_candidate: str | None = None,
    estimator: str = "signal",
    equity_baseline: pd.DataFrame | None = None,
    equity_candidate: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Evaluate the gate, write the verdict, and append the variant registry row."""
    if estimator not in ESTIMATORS:
        raise ValueError(f"estimator must be one of {ESTIMATORS}, got {estimator!r}")
    if estimator == "portfolio":
        if equity_baseline is None or equity_candidate is None:
            raise ValueError("estimator='portfolio' requires equity_baseline and equity_candidate")
        estimator_id = PORTFOLIO_ESTIMATOR_ID
        spec_hash = PORTFOLIO_GATE_SPEC_HASH
    else:
        if equity_baseline is not None or equity_candidate is not None:
            raise ValueError("equity inputs are only accepted when estimator='portfolio'")
        estimator_id = ESTIMATOR_ID
        spec_hash = GATE_SPEC_HASH

    baseline_metrics = _require_mapping(summary_baseline, "metrics", "baseline summary")
    candidate_metrics = _require_mapping(summary_candidate, "metrics", "candidate summary")
    baseline_config = _require_mapping(summary_baseline, "config", "baseline summary")
    candidate_config = _require_mapping(summary_candidate, "config", "candidate summary")

    for key in _REQUIRED_METRIC_KEYS:
        if key not in baseline_metrics or key not in candidate_metrics:
            raise ValueError(f"run summary metrics is missing required key: {key}")

    cost_baseline = _finite_float(
        baseline_config.get("explicit_cost_roundtrip"),
        "baseline.config.explicit_cost_roundtrip",
    )
    cost_candidate = _finite_float(
        candidate_config.get("explicit_cost_roundtrip"),
        "candidate.config.explicit_cost_roundtrip",
    )
    if not math.isclose(cost_baseline, cost_candidate, rel_tol=0.0, abs_tol=1e-15):
        raise ValueError("baseline and candidate explicit_cost_roundtrip must match")

    baseline_hash = summary_baseline.get("config_hash")
    candidate_hash = summary_candidate.get("config_hash")
    if not isinstance(baseline_hash, str) or not baseline_hash:
        raise ValueError("baseline summary is missing config_hash")
    if not isinstance(candidate_hash, str) or not candidate_hash:
        raise ValueError("candidate summary is missing config_hash")
    if not isinstance(data_hash, str) or not data_hash:
        raise ValueError("data_hash must be a non-empty string")
    if not isinstance(card_id, str) or not card_id:
        raise ValueError("card_id must be a non-empty string")

    oos_start = _normalized_date(baseline_metrics.get("oos_start"), "baseline.metrics.oos_start")
    resolved_oos_end = _resolve_oos_end(summary_baseline, oos_end)
    warnings: list[str] = [
        "The four experiment cards are adjudicated independently; the family-wise "
        "error rate is not controlled (~18.5% under the global null across four 5% tests)."
    ]
    candidate_oos_start = candidate_metrics.get("oos_start")
    if candidate_oos_start is not None:
        normalized_candidate_start = _normalized_date(candidate_oos_start, "candidate.metrics.oos_start")
        if normalized_candidate_start != oos_start:
            warnings.append(
                "baseline and candidate report different oos_start values; "
                f"the gate uses baseline {oos_start}"
            )
    if estimator == "portfolio":
        warnings.append(
            "The portfolio-level estimator quantifies sampling noise in the OOS daily "
            "equity curve only; it does not remove selection bias and does not account "
            "for capacity."
        )

    obs_baseline = build_observations(signals_baseline, oos_start, cost_baseline)
    obs_candidate = build_observations(signals_candidate, oos_start, cost_candidate)
    length, p99_baseline, p99_candidate = block_length(obs_baseline, obs_candidate)
    if estimator == "portfolio":
        length = max(PORTFOLIO_BLOCK_MIN, int(length))

    seed = derive_seed(
        gate_version,
        card_id,
        baseline_hash,
        candidate_hash,
        data_hash,
        oos_start,
        resolved_oos_end,
        length,
        RESAMPLES,
        estimator_id,
    )
    if estimator == "portfolio":
        bootstrap = portfolio_block_bootstrap(
            build_portfolio_returns(cast(pd.DataFrame, equity_baseline), oos_start, resolved_oos_end),
            build_portfolio_returns(cast(pd.DataFrame, equity_candidate), oos_start, resolved_oos_end),
            length,
            RESAMPLES,
            seed,
        )
    else:
        bootstrap = moving_block_bootstrap(obs_baseline, obs_candidate, length, RESAMPLES, seed)
    n_filled_baseline = _n_filled(summary_baseline, baseline_metrics, "baseline")
    n_filled_candidate = _n_filled(summary_candidate, candidate_metrics, "candidate")
    criteria_candidate_metrics = dict(candidate_metrics)
    criteria_candidate_metrics["n_trades"] = n_filled_candidate
    criteria = evaluate_criteria(bootstrap, baseline_metrics, criteria_candidate_metrics)
    overall_pass = all(bool(criterion["pass"]) for criterion in criteria)
    rejection_reasons = [str(criterion["id"]) for criterion in criteria if not criterion["pass"]]

    if bootstrap["degenerate"]:
        warnings.append("bootstrap delta has zero variance or no valid resamples; CI is not informative")
    if baseline_hash == candidate_hash:
        warnings.append("baseline and candidate config_hash are identical")
    if tested_count is not None and int(tested_count) < 1:
        raise ValueError("tested_count must be >= 1")

    registry_path = Path(out_dir) / REGISTRY_FILENAME
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    registry_id = hashlib.sha256(
        json.dumps(
            {
                "gate_version": gate_version,
                "card_id": card_id,
                "baseline_config_hash": baseline_hash,
                "candidate_config_hash": candidate_hash,
                "data_hash": data_hash,
                "oos_start": oos_start,
                "oos_end": resolved_oos_end,
                "block_length": int(length),
                "resamples": RESAMPLES,
                "estimator_id": estimator_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    prior_ids = _registered_variant_ids(registry_path, card_id, gate_version)
    resolved_tested = (
        int(tested_count)
        if tested_count is not None
        else len(prior_ids | {registry_id})
    )

    n_dates_baseline = int(obs_baseline["signal_date"].nunique())
    n_dates_candidate = int(obs_candidate["signal_date"].nunique())

    payload: dict[str, Any] = {
        "gate": {
            "name": GATE_NAME,
            "version": gate_version,
            "spec_hash": spec_hash,
            "card_id": card_id,
            "created_at": created_at,
            "code_commit": code_commit,
        },
        "inputs": {
            "baseline": {
                "run_id": run_id_baseline or _run_id(summary_baseline, data_hash, baseline_hash),
                "config_hash": baseline_hash,
                "n_signals": int(len(obs_baseline)),
                "n_dates": n_dates_baseline,
                "n_filled": n_filled_baseline,
            },
            "candidate": {
                "run_id": run_id_candidate or _run_id(summary_candidate, data_hash, candidate_hash),
                "config_hash": candidate_hash,
                "n_signals": int(len(obs_candidate)),
                "n_dates": n_dates_candidate,
                "n_filled": n_filled_candidate,
            },
            "data_snapshot_hash": data_hash,
            "oos_start": oos_start,
            "oos_end": resolved_oos_end,
        },
        "estimator": {
            "name": estimator_id,
            "delta": float(bootstrap["delta"]),
            "ci_level": CI_LEVEL,
            "ci_method": "percentile",
            "ci_low": float(bootstrap["ci_low"]),
            "ci_high": float(bootstrap["ci_high"]),
            "se": float(bootstrap["se"]),
            "block_length": int(length),
            "block_length_rule": "max(ceil(p99 holding_bars of each arm))",
            "block_length_inputs": {
                "baseline_p99": p99_baseline,
                "candidate_p99": p99_candidate,
            },
            "resamples": RESAMPLES,
            "resampling_unit": "signal_date",
            "date_index_rule": (
                "k=ceil(n_dates/L); each replicate draws k starts uniformly from "
                "1..(n_dates-L+1) and applies the index jointly to both arms; "
                "blocks may repeat; signal-weighted"
            ),
            "n_dates_union": int(bootstrap["n_dates"]),
            "n_blocks": int(bootstrap["n_blocks"]),
            "seed": seed,
            "seed_derivation": {
                "gate_version": gate_version,
                "card_id": card_id,
                "baseline_config_hash": baseline_hash,
                "candidate_config_hash": candidate_hash,
                "data_hash": data_hash,
                "oos_start": oos_start,
                "oos_end": resolved_oos_end,
                "block_length": int(length),
                "resamples": RESAMPLES,
                "estimator_id": estimator_id,
                "join": "pipe-joined ordered inputs; sha256 digest first 8 bytes as big-endian uint64",
            },
            "rng": "numpy.random.PCG64",
            "numpy_version": np.__version__,
            "signal_weighting": "signal_count",
            "valid_resamples": int(bootstrap["valid_resamples"]),
            "degenerate": bool(bootstrap["degenerate"]),
        },
        "portfolio": {
            "baseline": {
                "cagr": float(baseline_metrics["cagr_oos"]),
                "sharpe": float(baseline_metrics["sharpe"]),
                "max_dd": float(baseline_metrics["maxDD_pct"]),
            },
            "candidate": {
                "cagr": float(candidate_metrics["cagr_oos"]),
                "sharpe": float(candidate_metrics["sharpe"]),
                "max_dd": float(candidate_metrics["maxDD_pct"]),
            },
        },
        "criteria": criteria,
        "variants": {
            "tested_count": resolved_tested,
            "preregistered_primary": bool(preregistered_primary),
            "registry_id": registry_id,
        },
        "wfo": {
            "required": False,
            "folds": None,
            "reason": _WFO_REASON,
        },
        "overall_pass": overall_pass,
        "rejection_reasons": rejection_reasons,
        "warnings": warnings,
        "limitations": list(_LIMITATIONS),
    }
    if estimator == "portfolio":
        payload["estimator"].update(
            {
                "block_length_rule": "max(21, ceil(p99 holding_bars of each arm))",
                "resampling_unit": "oos_trading_day",
                "date_index_rule": (
                    "k=ceil(n_dates/L); each replicate draws k starts uniformly from "
                    "1..(n_dates-L+1) and applies the index jointly to both equity arms; "
                    "blocks may repeat; equal-weighted by trading day"
                ),
                "signal_weighting": "not_applicable",
                "delta_unit": "percentage_points_annualized_return",
                "cagr_baseline_pct": float(bootstrap["cagr_baseline_pct"]),
                "cagr_candidate_pct": float(bootstrap["cagr_candidate_pct"]),
            }
        )
    payload["verdict_hash"] = _canonical_hash(payload)

    write_verdict(payload, out_dir)
    append_registry(
        {
            "registry_id": registry_id,
            "recorded_at": created_at,
            "card_id": card_id,
            "gate_name": GATE_NAME,
            "gate_version": gate_version,
            "baseline_config_hash": baseline_hash,
            "candidate_config_hash": candidate_hash,
            "data_snapshot_hash": data_hash,
            "oos_start": oos_start,
            "oos_end": resolved_oos_end,
            "block_length": int(length),
            "resamples": RESAMPLES,
            "seed": seed,
            "delta": float(bootstrap["delta"]),
            "ci_low": float(bootstrap["ci_low"]),
            "ci_high": float(bootstrap["ci_high"]),
            "se": float(bootstrap["se"]),
            "overall_pass": overall_pass,
            "rejection_reasons": rejection_reasons,
            "verdict_hash": payload["verdict_hash"],
        },
        registry_path,
    )
    return payload


def _fmt_pct(value: Any, digits: int = 3, scale: float = 1.0) -> str:
    if value is None:
        return "n/a"
    number = float(value) * scale
    if not math.isfinite(number):
        return "n/a"
    return f"{number:+.{digits}f}%"


def _fmt_hash(value: Any) -> str:
    text = str(value)
    return text[:12] if len(text) > 12 else text


def _fmt_metric(value: Any, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    number = float(value)
    if not math.isfinite(number):
        return "n/a"
    return f"{number:.{digits}f}"


def load_signals(path: str | Path) -> pd.DataFrame:
    """Read a de-clustered signal table produced by the portfolio sim driver."""
    frame = pd.read_csv(Path(path))
    if frame.empty:
        raise ValueError(f"signal table is empty: {path}")
    return frame


def load_equity(path: str | Path) -> pd.DataFrame:
    """Read a portfolio equity CSV into a date-sorted ``date``/``equity`` frame."""
    frame = pd.read_csv(Path(path), encoding="utf-8-sig")
    _require_columns(frame, ("date", "equity"), "equity")
    frame = frame.loc[:, ["date", "equity"]].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")
    frame["equity"] = pd.to_numeric(frame["equity"], errors="raise")
    if bool(frame["date"].isna().any()):
        raise ValueError(f"equity contains an invalid date: {path}")
    frame = frame.sort_values("date", kind="mergesort").reset_index(drop=True)
    if bool(frame["date"].duplicated().any()):
        raise ValueError(f"equity contains duplicate dates: {path}")
    values = frame["equity"].to_numpy(dtype=np.float64)
    if not bool(np.isfinite(values).all()):
        raise ValueError(f"equity contains non-finite values: {path}")
    if bool((values <= 0.0).any()):
        raise ValueError(f"equity must be positive: {path}")
    return frame


def load_summary(path: str | Path) -> dict[str, Any]:
    """Read a portfolio sim run summary JSON."""
    with Path(path).open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"run summary must be a JSON object: {path}")
    return payload


def format_human(payload: Mapping[str, Any]) -> str:
    """Render a one-screen human-readable verdict."""
    gate = _require_mapping(payload, "gate", "payload")
    inputs = _require_mapping(payload, "inputs", "payload")
    estimator = _require_mapping(payload, "estimator", "payload")
    portfolio = _require_mapping(payload, "portfolio", "payload")
    variants = _require_mapping(payload, "variants", "payload")
    wfo = _require_mapping(payload, "wfo", "payload")
    baseline = _require_mapping(inputs, "baseline", "payload.inputs")
    candidate = _require_mapping(inputs, "candidate", "payload.inputs")
    base_portfolio = _require_mapping(portfolio, "baseline", "payload.portfolio")
    cand_portfolio = _require_mapping(portfolio, "candidate", "payload.portfolio")
    criteria = payload.get("criteria", [])
    rejection_reasons = payload.get("rejection_reasons", [])
    warnings = payload.get("warnings", [])
    limitations = payload.get("limitations", [])
    delta_scale = 1.0 if estimator.get("delta_unit") == "percentage_points_annualized_return" else 100.0

    lines = [
        f"{str(gate['name']).upper()} v{gate['version']}  [{str(gate['card_id'])}]",
        f"spec={_fmt_hash(gate['spec_hash'])} commit={str(gate['code_commit'])[:12]}",
        (
            f"OOS {inputs['oos_start']} -> {inputs['oos_end']}  "
            f"data={_fmt_hash(inputs['data_snapshot_hash'])}"
        ),
        (
            f"baseline  cfg={_fmt_hash(baseline['config_hash'])} n_signals={baseline['n_signals']} "
            f"n_dates={baseline['n_dates']} n_filled={baseline['n_filled']}"
        ),
        (
            f"candidate cfg={_fmt_hash(candidate['config_hash'])} n_signals={candidate['n_signals']} "
            f"n_dates={candidate['n_dates']} n_filled={candidate['n_filled']}"
        ),
        (
            f"L={estimator['block_length']} B={estimator['resamples']} "
            f"seed={estimator['seed']} rng={estimator['rng']}"
        ),
        (
            f"delta={_fmt_pct(estimator['delta'], scale=delta_scale)}  "
            f"{int(round(float(estimator['ci_level']) * 100))}% CI "
            f"[{_fmt_pct(estimator['ci_low'], scale=delta_scale)}, "
            f"{_fmt_pct(estimator['ci_high'], scale=delta_scale)}]  "
            f"se={_fmt_pct(estimator['se'], scale=delta_scale)}  "
            f"n_dates={estimator['n_dates_union']} n_blocks={estimator['n_blocks']} "
            f"valid={estimator['valid_resamples']}"
        ),
        (
            f"portfolio baseline cagr={_fmt_pct(base_portfolio['cagr'], scale=100.0)} "
            f"sharpe={_fmt_metric(base_portfolio['sharpe'])} "
            f"maxDD={_fmt_pct(base_portfolio['max_dd'])}"
        ),
        (
            f"portfolio candidate cagr={_fmt_pct(cand_portfolio['cagr'], scale=100.0)} "
            f"sharpe={_fmt_metric(cand_portfolio['sharpe'])} "
            f"maxDD={_fmt_pct(cand_portfolio['max_dd'])}"
        ),
        "criteria:",
    ]
    for criterion in criteria:
        status = "PASS" if criterion.get("pass") else "FAIL"
        observed = _fmt_metric(criterion.get("observed"))
        threshold = _fmt_metric(criterion.get("threshold"))
        lines.append(
            f"  {status} {criterion.get('id')}: observed={observed} "
            f"{criterion.get('comparator')} threshold={threshold} "
            f"(margin={_fmt_metric(criterion.get('margin'))}, type={criterion.get('type')})"
        )
    lines.extend(
        [
            (
                f"variants tested={variants['tested_count']} "
                f"preregistered_primary={'yes' if variants['preregistered_primary'] else 'no'} "
                f"registry_id={_fmt_hash(variants['registry_id'])}"
            ),
            (
                f"WFO required={wfo['required']} folds={wfo['folds']} "
                f"reason={wfo['reason']}"
            ),
            f"OVERALL: {'PASS' if payload.get('overall_pass') else 'FAIL'}",
            f"rejections: {', '.join(str(reason) for reason in rejection_reasons) or 'none'}",
        ]
    )
    if warnings:
        lines.append("warnings: " + " | ".join(str(warning) for warning in warnings))
    lines.append(
        "caveat: margin-0 guardrails are descriptive directional constraints, "
        "not inferential non-inferiority tests."
    )
    for limitation in limitations:
        lines.append(f"limitation: {limitation}")
    return "\n".join(lines)


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for evaluating one pre-registered card comparison."""
    parser = argparse.ArgumentParser(
        prog="portfolio_gate",
        description=(
            "Evaluate the date-block bootstrap flip gate on a baseline and a candidate "
            "portfolio-sim run, then write a content-addressed verdict and registry row."
        ),
        epilog=(
            "Exit codes: 0 = gate PASS, 1 = gate FAIL (a valid result, not an error), "
            "2 = unusable inputs."
        ),
    )
    parser.add_argument("--baseline-summary", required=True, help="baseline run summary JSON")
    parser.add_argument("--baseline-signals", required=True, help="baseline de-clustered signals CSV")
    parser.add_argument("--candidate-summary", required=True, help="candidate run summary JSON")
    parser.add_argument("--candidate-signals", required=True, help="candidate de-clustered signals CSV")
    parser.add_argument("--card-id", required=True, help="pre-registered card id, e.g. card-a")
    parser.add_argument(
        "--data-hash",
        required=True,
        help="caller-supplied data snapshot identity; part of the bootstrap seed",
    )
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="verdict + registry root")
    parser.add_argument("--oos-end", default=None, help="override the OOS end date recorded in the verdict")
    parser.add_argument("--code-commit", default="unknown", help="git commit the gate code was run from")
    parser.add_argument(
        "--estimator",
        choices=ESTIMATORS,
        default="signal",
        help="estimand: signal-level pooled mean difference (default) or portfolio-level OOS CAGR delta",
    )
    parser.add_argument(
        "--baseline-equity",
        default=None,
        help="baseline OOS daily equity CSV; required only with --estimator portfolio",
    )
    parser.add_argument(
        "--candidate-equity",
        default=None,
        help="candidate OOS daily equity CSV; required only with --estimator portfolio",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns the process exit code (see the parser epilog).

    The variant count is deliberately not settable from the CLI: it is derived from the
    card's own ``registry.jsonl`` so a run cannot reset the count that guards against
    data snooping (see docs/backtest-flip-gate-spec.md §7).
    """
    args = build_arg_parser().parse_args(argv)
    try:
        equity_baseline = load_equity(args.baseline_equity) if args.baseline_equity is not None else None
        equity_candidate = load_equity(args.candidate_equity) if args.candidate_equity is not None else None
        payload = run_gate(
            load_signals(args.baseline_signals),
            load_signals(args.candidate_signals),
            load_summary(args.baseline_summary),
            load_summary(args.candidate_summary),
            card_id=args.card_id,
            data_hash=args.data_hash,
            out_dir=args.out_dir,
            oos_end=args.oos_end,
            code_commit=args.code_commit,
            estimator=args.estimator,
            equity_baseline=equity_baseline,
            equity_candidate=equity_candidate,
        )
        resolved_verdict = verdict_path(payload, args.out_dir)
    except (OSError, ValueError) as exc:
        print(f"portfolio_gate: {exc}", file=sys.stderr)
        return 2

    print(format_human(payload))
    print(f"verdict: {resolved_verdict}")
    return 0 if payload.get("overall_pass") else 1


if __name__ == "__main__":
    raise SystemExit(main())
