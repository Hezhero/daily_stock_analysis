# -*- coding: utf-8 -*-
"""Capital-constrained daily mark-to-market portfolio simulator (increment S2).

Side-effect free: no logging configuration, no environment reads, no import-time I/O,
and no dependency on the historical backtest driver. The candidate table is produced
by S3; this module only consumes it.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Mapping, Tuple

import numpy as np
import pandas as pd

STRATEGY_FAMILY: dict[str, str] = {
    # trend
    "ma_crossover": "trend",
    "volume_surge_std": "trend",
    "multi_ma_resonance": "trend",
    "ma_golden_cross": "trend",
    "volume_breakout": "trend",
    "monthly_macd_20ma": "trend",
    "wave_theory": "trend",
    "n_pattern": "trend",
    "ensemble": "trend",
    # reversal
    "wonderful_9_turn": "reversal",
    "emotion_cycle": "reversal",
    "one_yang_three_yin": "reversal",
    "washout_break": "reversal",
    "low_profit_hold": "reversal",
    "chan_theory": "reversal",
    "box_oscillation": "reversal",
    "rsi_bullish_divergence": "reversal",
    # limitup
    "limit_up_pullback": "limitup",
    "stable_then_limit_up": "limitup",
    "low_position_limit_up": "limitup",
    "dragon_head": "limitup",
    # fundamental
    "holder_conc_break": "fundamental",
    "inst_smart_break": "fundamental",
    "fc_pos_break": "fundamental",
}


_SIZE_MODES = ("fixed_1n", "deploy_aware")
_HASH_OPTIONAL_KEYS = ("size_mode", "target_positions", "cash_buffer_pct")


@dataclass(frozen=True)
class SimConfig:
    initial_capital: float = 1_000_000.0
    max_positions: int = 10
    family_cap: Tuple[Tuple[str, int], ...] = (("trend", 3),)
    industry_cap: int = 2
    min_resonance_families: int = 2
    explicit_cost_roundtrip: float = 0.0035
    cost_split: float = 0.5
    adv_window: int = 20
    adv_tiers: Tuple[Tuple[float, float], ...] = (
        (1_000_000_000, 0.0005),
        (300_000_000, 0.0010),
        (100_000_000, 0.0020),
        (30_000_000, 0.0035),
        (0.0, 0.0050),
    )
    max_entry_lag_days: int = 5
    lot_size: int = 1
    expectation_min_trades: int = 30
    warmup_days: int = 252
    max_dd_floor_pct: float = 25.0
    min_trades: int = 100
    max_holding_bars: int = 10
    # Defaults are excluded from the config hash so frozen baseline hashes stay byte-identical.
    size_mode: str = "fixed_1n"
    target_positions: int | None = None
    cash_buffer_pct: float = 0.0
    vote_exclude: Tuple[str, ...] = (
        "box_oscillation",
        "chan_theory",
        "dragon_head",
        "emotion_cycle",
        "ensemble",
        "one_yang_three_yin",
        "wonderful_9_turn",
    )

    def __post_init__(self) -> None:
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be positive")
        if self.max_positions <= 0:
            raise ValueError("max_positions must be positive")
        if self.lot_size <= 0:
            raise ValueError("lot_size must be positive")
        if not 0.0 <= self.cost_split <= 1.0:
            raise ValueError("cost_split must be in [0, 1]")
        if self.size_mode not in _SIZE_MODES:
            raise ValueError(f"size_mode must be one of {_SIZE_MODES}, got {self.size_mode!r}")
        if self.size_mode == "deploy_aware":
            if self.target_positions is None:
                raise ValueError("target_positions is required when size_mode='deploy_aware'")
            if not 1 <= self.target_positions <= self.max_positions:
                raise ValueError("target_positions must be in [1, max_positions]")
        if not 0.0 <= self.cash_buffer_pct < 1.0:
            raise ValueError("cash_buffer_pct must be in [0, 1)")


FAMILY_CAP: dict[str, int] = dict(SimConfig().family_cap)

_BAR_COLUMNS = ("code", "date", "open", "high", "low", "close")
_CAND_COLUMNS = (
    "strategy",
    "family",
    "signal_date",
    "code",
    "entry_date",
    "entry_px_raw",
    "entry_open_raw",
    "unbuyable",
    "exit_date",
    "exit_offset",
    "exit_px_intended",
    "exit_reason",
    "exit_deferred",
    "exit_open",
    "exit_close",
    "adv20",
    "adv_tier",
    "slip_entry",
    "slip_exit",
    "strength",
    "ind_l1",
    "n_families",
    "lead_exp",
    "realized_ret",
)
_EQUITY_COLUMNS = (
    "date",
    "cash",
    "position_value",
    "equity",
    "n_positions",
    "exposure",
    "daily_ret",
    "drawdown",
)
_TRADE_COLUMNS = (
    "code",
    "strategy",
    "family",
    "ind_l1",
    "entry_date",
    "exit_date",
    "holding_days",
    "entry_px_raw",
    "entry_px_fill",
    "exit_fill_px",
    "exit_px_net",
    "shares",
    "entry_notional",
    "exit_notional",
    "explicit_cost",
    "slippage_cost",
    "gross_ret",
    "ret",
    "pnl",
    "exit_reason",
    "exit_deferred",
    "n_families",
    "lead_exp",
)


@dataclass(frozen=True)
class _CodeView:
    dates: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    index: Mapping[datetime, int]

    def _position(self, day: datetime) -> int:
        idx = self.index.get(day)
        if idx is not None:
            return idx
        pos = int(self.dates.searchsorted(np.datetime64(day), side="right")) - 1
        return min(max(pos, 0), len(self.close) - 1)

    def close_at(self, day: datetime) -> float:
        return float(self.close[self._position(day)])

    def bar_at(self, day: datetime) -> Tuple[float, float] | None:
        idx = self.index.get(day)
        if idx is None:
            return None
        return float(self.low[idx]), float(self.high[idx])


@dataclass(frozen=True)
class _Cand:
    strategy: str
    family: str
    signal_date: datetime
    code: str
    entry_date: datetime
    entry_px_raw: float
    entry_open_raw: float
    unbuyable: bool
    exit_date: datetime
    exit_offset: int
    exit_px_intended: float
    exit_reason: int
    exit_deferred: int
    exit_open: float
    exit_close: float
    adv20: float
    adv_tier: int
    slip_entry: float
    slip_exit: float
    strength: float
    ind_l1: str | None
    n_families: int
    lead_exp: float
    realized_ret: float


@dataclass(frozen=True)
class _Pos:
    shares: float
    fill_px: float
    cand: _Cand


@dataclass(frozen=True)
class SimResult:
    equity: pd.DataFrame
    trades: pd.DataFrame
    metrics: dict[str, Any]
    rejects: dict[str, int]
    meta: dict[str, Any]


def _require_columns(frame: pd.DataFrame, columns: Tuple[str, ...], label: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def _as_datetime(value: Any) -> datetime:
    result = pd.Timestamp(value).to_pydatetime()
    if not isinstance(result, datetime):
        raise ValueError(f"invalid datetime value: {value!r}")
    return result


def _code_views(bars: pd.DataFrame) -> dict[str, _CodeView]:
    """Build per-code price arrays and an exact date -> row index map."""
    _require_columns(bars, _BAR_COLUMNS, "bars")
    frame = bars.loc[:, list(_BAR_COLUMNS)].copy()
    frame["code"] = frame["code"].astype(str)
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values(["code", "date"], kind="mergesort").drop_duplicates(["code", "date"], keep="last")

    views: dict[str, _CodeView] = {}
    for code, group in frame.groupby("code", sort=False):
        group = group.sort_values("date", kind="mergesort")
        dates = np.asarray(group["date"], dtype="datetime64[ns]")
        close_series = pd.Series(np.asarray(group["close"], dtype=np.float64), index=group.index)
        close = close_series.ffill().bfill().to_numpy(dtype=np.float64)
        views[code] = _CodeView(
            dates=dates,
            open=np.asarray(group["open"], dtype=np.float64),
            high=np.asarray(group["high"], dtype=np.float64),
            low=np.asarray(group["low"], dtype=np.float64),
            close=close,
            index={_as_datetime(value): row for row, value in enumerate(dates)},
        )
    return views


def _to_cand(row: Any) -> _Cand:
    ind_l1 = row.ind_l1
    return _Cand(
        strategy=str(row.strategy),
        family=str(row.family),
        signal_date=_as_datetime(row.signal_date),
        code=str(row.code),
        entry_date=_as_datetime(row.entry_date),
        entry_px_raw=float(row.entry_px_raw),
        entry_open_raw=float(row.entry_open_raw),
        unbuyable=bool(row.unbuyable),
        exit_date=_as_datetime(row.exit_date),
        exit_offset=int(row.exit_offset),
        exit_px_intended=float(row.exit_px_intended),
        exit_reason=int(row.exit_reason),
        exit_deferred=int(row.exit_deferred),
        exit_open=float(row.exit_open),
        exit_close=float(row.exit_close),
        adv20=float(row.adv20),
        adv_tier=int(row.adv_tier),
        slip_entry=float(row.slip_entry),
        slip_exit=float(row.slip_exit),
        strength=float(row.strength),
        ind_l1=None if pd.isna(ind_l1) or str(ind_l1) == "" else str(ind_l1),
        n_families=int(row.n_families),
        lead_exp=float(row.lead_exp),
        realized_ret=float(row.realized_ret),
    )


def _ordered_candidates(cands: pd.DataFrame) -> list[_Cand]:
    ordered = cands.sort_values(
        by=["entry_date", "lead_exp", "n_families", "strength", "code", "strategy"],
        ascending=[True, False, False, False, True, True],
        kind="mergesort",
        na_position="last",
    )
    return [_to_cand(row) for row in ordered.itertuples(index=False)]


def _entry_reject(
    cand: _Cand,
    positions: Mapping[str, _Pos],
    family_counts: Counter[str],
    industry_counts: Counter[str],
    family_cap: Mapping[str, int],
    cfg: SimConfig,
    views: Mapping[str, _CodeView],
) -> str | None:
    if len(positions) >= cfg.max_positions:
        return "max_positions"
    if cand.code in positions:
        return "already_held"
    if family_counts[cand.family] >= family_cap.get(cand.family, math.inf):
        return "family_cap"
    if cand.ind_l1 is not None and industry_counts[cand.ind_l1] >= cfg.industry_cap:
        return "industry_cap"
    if cand.unbuyable:
        return "unbuyable"
    if cand.code not in views:
        return "no_price_data"
    return None


def _resolve_fill(cand: _Cand, views: Mapping[str, _CodeView], rejects: Counter[str]) -> float:
    """Apply the exit fill table, then clamp into the exit bar's [low, high]."""
    if cand.exit_deferred == 1 or cand.exit_reason == 4:
        fill = cand.exit_px_intended
    elif cand.exit_reason in (1, 3):
        fill = min(cand.exit_px_intended, cand.exit_open)
    elif cand.exit_reason == 2:
        fill = max(cand.exit_px_intended, cand.exit_open)
    else:
        fill = cand.exit_px_intended

    if not math.isfinite(fill):
        rejects["invalid_exit_price"] += 1
        fill = cand.exit_close if math.isfinite(cand.exit_close) else cand.exit_open

    view = views.get(cand.code)
    bar = view.bar_at(cand.exit_date) if view is not None else None
    if bar is not None:
        low, high = bar
        if math.isfinite(low) and math.isfinite(high) and low <= high and not low <= fill <= high:
            rejects["fill_out_of_range"] += 1
            fill = min(max(fill, low), high)
    return float(fill)


def _trade_row(pos: _Pos, fill: float, net_px: float, day: datetime, cfg: SimConfig) -> dict[str, Any]:
    cand = pos.cand
    half_cost = cfg.cost_split * cfg.explicit_cost_roundtrip
    entry_notional = pos.shares * pos.fill_px
    exit_notional = pos.shares * net_px
    entry_cash = entry_notional * (1.0 + half_cost)
    exit_cash = exit_notional * (1.0 - half_cost)
    gross_ret = fill / cand.entry_px_raw - 1.0 if cand.entry_px_raw > 0 else 0.0
    # Reporting contract: slippage and explicit costs are additive drags on entry notional.
    net_ret = gross_ret - cand.slip_entry - cand.slip_exit - cfg.explicit_cost_roundtrip
    return {
        "code": cand.code,
        "strategy": cand.strategy,
        "family": cand.family,
        "ind_l1": cand.ind_l1,
        "entry_date": cand.entry_date,
        "exit_date": day,
        "holding_days": int((day - cand.entry_date).days),
        "entry_px_raw": cand.entry_px_raw,
        "entry_px_fill": pos.fill_px,
        "exit_fill_px": fill,
        "exit_px_net": net_px,
        "shares": pos.shares,
        "entry_notional": entry_notional,
        "exit_notional": exit_notional,
        "explicit_cost": (entry_notional + exit_notional) * half_cost,
        "slippage_cost": pos.shares * cand.entry_px_raw * cand.slip_entry + pos.shares * fill * cand.slip_exit,
        "gross_ret": gross_ret,
        "ret": net_ret,
        "pnl": exit_cash - entry_cash,
        "exit_reason": cand.exit_reason,
        "exit_deferred": cand.exit_deferred,
        "n_families": cand.n_families,
        "lead_exp": cand.lead_exp,
    }


def _empty_frame(columns: Tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="float64") for column in columns})


def _config_hash(cfg: SimConfig) -> str:
    payload = asdict(cfg)
    defaults = SimConfig()
    for key in _HASH_OPTIONAL_KEYS:
        if payload[key] == getattr(defaults, key):
            payload.pop(key)
    text = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _entry_budget(equity_prev: float, cash: float, cfg: SimConfig) -> float:
    """Cash available for one entry under the configured sizing rule."""
    if cfg.size_mode == "fixed_1n":
        return min(equity_prev / cfg.max_positions, cash)
    target = cfg.target_positions
    if target is None:
        raise ValueError("size_mode='deploy_aware' requires target_positions")
    buffer = cfg.cash_buffer_pct * equity_prev
    return max(0.0, min(equity_prev / target, cash - buffer))


def simulate(cands: pd.DataFrame, bars: pd.DataFrame, cfg: SimConfig = SimConfig()) -> SimResult:
    """Run the capital-constrained daily MTM simulation."""
    _require_columns(cands, _CAND_COLUMNS, "cands")
    views = _code_views(bars)
    cal = [_as_datetime(value) for value in sorted(pd.to_datetime(bars["date"]).unique())]
    if not cal:
        raise ValueError("bars contain no dates")

    by_day: dict[datetime, list[_Cand]] = defaultdict(list)
    for cand in _ordered_candidates(cands):
        by_day[cand.entry_date].append(cand)

    family_cap = dict(cfg.family_cap)
    cash = float(cfg.initial_capital)
    equity_prev = float(cfg.initial_capital)
    peak = float(cfg.initial_capital)
    positions: dict[str, _Pos] = {}
    family_counts: Counter[str] = Counter()
    industry_counts: Counter[str] = Counter()
    rejects: Counter[str] = Counter()
    trade_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    entry_count = 0

    for day in cal:
        # A) entries at the open; only start-of-day cash is available.
        for cand in by_day.get(day, ()):
            reason = _entry_reject(cand, positions, family_counts, industry_counts, family_cap, cfg, views)
            if reason is not None:
                rejects[reason] += 1
                continue
            budget = _entry_budget(equity_prev, cash, cfg)
            px = cand.entry_px_raw * (1.0 + cand.slip_entry)
            if not math.isfinite(px) or px <= 0:
                rejects["invalid_price"] += 1
                continue
            shares = math.floor(budget / (px * (1.0 + cfg.explicit_cost_roundtrip * cfg.cost_split)))
            if cfg.lot_size > 1:
                shares = (shares // cfg.lot_size) * cfg.lot_size
            if shares <= 0:
                rejects["no_cash"] += 1
                continue
            cash -= shares * px * (1.0 + cfg.cost_split * cfg.explicit_cost_roundtrip)
            positions[cand.code] = _Pos(float(shares), px, cand)
            entry_count += 1
            family_counts[cand.family] += 1
            if cand.ind_l1 is not None:
                industry_counts[cand.ind_l1] += 1

        # B) exits due today, after entries so today's exit cash cannot fund an open fill.
        for code, pos in list(positions.items()):
            if day < pos.cand.exit_date or day <= pos.cand.entry_date:
                continue
            fill = _resolve_fill(pos.cand, views, rejects)
            net_px = fill * (1.0 - pos.cand.slip_exit)
            cash += pos.shares * net_px * (1.0 - cfg.cost_split * cfg.explicit_cost_roundtrip)
            trade_rows.append(_trade_row(pos, fill, net_px, day, cfg))
            del positions[code]
            family_counts[pos.cand.family] -= 1
            if pos.cand.ind_l1 is not None:
                industry_counts[pos.cand.ind_l1] -= 1

        # C) mark to market at the close, including flat days.
        position_value = sum(pos.shares * views[code].close_at(day) for code, pos in positions.items())
        equity = cash + position_value
        peak = max(peak, equity)
        equity_rows.append(
            {
                "date": day,
                "cash": cash,
                "position_value": position_value,
                "equity": equity,
                "n_positions": len(positions),
                "exposure": position_value / equity if equity else 0.0,
                "daily_ret": equity / equity_prev - 1.0 if equity_prev else 0.0,
                "drawdown": (peak - equity) / peak if peak > 0 else 0.0,
            }
        )
        equity_prev = equity

    equity_frame = pd.DataFrame(equity_rows, columns=pd.Index(list(_EQUITY_COLUMNS)))
    trades_frame = (
        pd.DataFrame(trade_rows, columns=pd.Index(list(_TRADE_COLUMNS)))
        if trade_rows
        else _empty_frame(_TRADE_COLUMNS)
    )
    metrics = compute_metrics(equity_frame, trades_frame, cfg)
    meta = {
        "config": asdict(cfg),
        "config_hash": _config_hash(cfg),
        "n_days": len(cal),
        "start": cal[0].strftime("%Y-%m-%d"),
        "end": cal[-1].strftime("%Y-%m-%d"),
        "n_candidates": int(len(cands)),
        "n_entries": entry_count,
        "n_closed_trades": len(trade_rows),
        "n_rejects": int(sum(rejects.values())),
        "final_equity": float(equity_frame["equity"].iloc[-1]),
    }
    return SimResult(
        equity=equity_frame,
        trades=trades_frame,
        metrics=metrics,
        rejects=dict(sorted(rejects.items())),
        meta=meta,
    )


def _series_sum(trades: pd.DataFrame, column: str) -> float:
    if column not in trades.columns or trades.empty:
        return 0.0
    return float(np.asarray(trades[column], dtype=np.float64).sum())


def compute_metrics(equity: pd.DataFrame, trades: pd.DataFrame, cfg: SimConfig = SimConfig()) -> dict[str, Any]:
    """Compute performance metrics, always reporting the gate explicitly."""
    if equity.empty:
        return {
            "sharpe": 0.0,
            "cagr_oos": 0.0,
            "cagr_full": 0.0,
            "maxDD": 0.0,
            "maxDD_pct": 0.0,
            "gate": "insufficient_trades" if len(trades) < cfg.min_trades else "fail",
            "n_trades": int(len(trades)),
            "win_rate": 0.0,
            "avg_holding_days": 0.0,
            "avg_positions": 0.0,
            "avg_cash_share": 0.0,
            "cost_drag": 0.0,
            "turnover": 0.0,
            "total_return": 0.0,
            "final_equity": 0.0,
            "n_days": 0,
            "oos_start": None,
            "oos_days": 0,
        }

    eq = pd.Series(np.asarray(equity["equity"], dtype=np.float64))
    if "daily_ret" in equity.columns:
        returns = pd.Series(np.nan_to_num(np.asarray(equity["daily_ret"], dtype=np.float64), nan=0.0))
    else:
        returns = eq.pct_change().fillna(0.0)

    n_days = len(eq)
    end_equity = float(eq.iloc[-1])
    std = float(returns.std(ddof=1)) if n_days > 1 else 0.0
    sharpe = float(returns.mean() / std * math.sqrt(252)) if std > 0 else 0.0

    full_days = n_days - 1
    if full_days > 0 and end_equity > 0:
        cagr_full = (end_equity / cfg.initial_capital) ** (252.0 / full_days) - 1.0
    else:
        cagr_full = 0.0

    oos_index = min(cfg.warmup_days, n_days - 1)
    oos_start_equity = float(eq.iloc[oos_index])
    oos_days = n_days - 1 - oos_index
    if oos_days > 0 and oos_start_equity > 0 and end_equity > 0:
        cagr_oos = (end_equity / oos_start_equity) ** (252.0 / oos_days) - 1.0
    else:
        cagr_oos = 0.0

    peak = eq.cummax()
    drawdown = (peak - eq) / peak.where(peak != 0)
    max_dd = float(drawdown.max(skipna=True)) if drawdown.notna().any() else 0.0
    max_dd_pct = max_dd * 100.0

    n_trades = int(len(trades))
    if n_trades and "ret" in trades.columns:
        trade_ret = np.asarray(trades["ret"], dtype=np.float64)
        win_rate = float((trade_ret > 0).mean())
    else:
        win_rate = 0.0
    if n_trades and "holding_days" in trades.columns:
        avg_holding_days = float(np.asarray(trades["holding_days"], dtype=np.float64).mean())
    else:
        avg_holding_days = 0.0

    avg_positions = (
        float(np.asarray(equity["n_positions"], dtype=np.float64).mean()) if "n_positions" in equity.columns else 0.0
    )
    if "cash" in equity.columns:
        cash_series = pd.Series(np.asarray(equity["cash"], dtype=np.float64))
        cash_share = cash_series / eq.where(eq != 0)
        avg_cash_share = float(cash_share.dropna().mean()) if cash_share.notna().any() else 0.0
    else:
        avg_cash_share = 0.0

    mean_equity = float(eq.mean()) if n_days else 0.0
    if n_trades and mean_equity > 0:
        total_cost = _series_sum(trades, "explicit_cost") + _series_sum(trades, "slippage_cost")
        cost_drag = total_cost / mean_equity
        turnover = (_series_sum(trades, "entry_notional") + _series_sum(trades, "exit_notional")) / mean_equity
    else:
        cost_drag = 0.0
        turnover = 0.0

    if cagr_oos > 0 and max_dd_pct <= cfg.max_dd_floor_pct and n_trades >= cfg.min_trades:
        gate = "pass"
    elif n_trades < cfg.min_trades:
        gate = "insufficient_trades"
    else:
        gate = "fail"

    oos_start = _as_datetime(equity["date"].iloc[oos_index]) if "date" in equity.columns else None
    return {
        "sharpe": sharpe,
        "cagr_oos": float(cagr_oos),
        "cagr_full": float(cagr_full),
        "maxDD": max_dd,
        "maxDD_pct": max_dd_pct,
        "gate": gate,
        "n_trades": n_trades,
        "win_rate": win_rate,
        "avg_holding_days": avg_holding_days,
        "avg_positions": avg_positions,
        "avg_cash_share": avg_cash_share,
        "cost_drag": float(cost_drag),
        "turnover": float(turnover),
        "total_return": end_equity / cfg.initial_capital - 1.0,
        "final_equity": end_equity,
        "n_days": n_days,
        "oos_start": oos_start.strftime("%Y-%m-%d") if oos_start is not None else None,
        "oos_days": oos_days,
    }


# ---------------------------------------------------------------------------
# S3: candidate construction, family de-clustering, causal lead expectation
# ---------------------------------------------------------------------------

#: Ordering key used by both :func:`collapse_candidates` and ``simulate()``.
RANK_SCORE: Tuple[str, ...] = ("lead_exp", "n_families", "strength")

_PANEL_COLUMNS = ("close_raw", "up_limit", "volume", "amount")


def _dyn_columns(period: int) -> Tuple[str, ...]:
    """Return the ``next_open`` exit-path columns emitted by S1 for ``period``."""
    return (
        f"dyn_path_entry_{period}d_no",
        f"dyn_path_bar_{period}d_no",
        f"dyn_path_px_{period}d_no",
        f"dyn_path_reason_{period}d_no",
        f"dyn_path_defer_{period}d_no",
        f"dyn_ret_{period}d_no",
    )


def _aligned_series(values: Any, index: pd.Index) -> pd.Series:
    """Align an index-keyed Series (or a positional array) to ``index``."""
    if isinstance(values, pd.Series):
        return values.reindex(index)
    array = np.asarray(values)
    if len(array) != len(index):
        raise ValueError("signal/reference series length does not match bars")
    return pd.Series(array, index=index)


def _bump(rejects: Counter[str] | None, key: str, amount: int = 1) -> None:
    if rejects is not None:
        rejects[key] += amount


def _canonical_panel(bars: pd.DataFrame) -> Tuple[pd.DataFrame, np.ndarray]:
    """Sort/dedupe the panel exactly like ``_code_views`` and return source positions."""
    frame = bars.copy()
    frame["code"] = frame["code"].astype(str)
    frame["date"] = pd.to_datetime(frame["date"])
    frame["_orig_pos"] = np.arange(len(frame))
    frame = frame.sort_values(["code", "date"], kind="mergesort")
    frame = frame.loc[~frame.duplicated(["code", "date"], keep="last")]
    positions = frame.pop("_orig_pos").to_numpy(dtype=np.int64)
    return frame.reset_index(drop=True), positions


def _adv20(frame: pd.DataFrame, cfg: SimConfig) -> np.ndarray:
    """Causal rolling ADV on the panel, using only bars up to and including each row."""
    rolled = (
        frame.groupby("code", sort=False)["amount"]
        .rolling(window=cfg.adv_window, min_periods=cfg.adv_window)
        .mean()
    )
    aligned = rolled.droplevel(0).reindex(frame.index)
    return aligned.to_numpy(dtype=np.float64)


def _adv_tier(adv20: float, tiers: Tuple[Tuple[float, float], ...]) -> int:
    for tier, (min_adv, _slip) in enumerate(tiers):
        if min_adv <= adv20:
            return tier
    return len(tiers) - 1


def _industry_l1(drafts: list[dict[str, Any]], members: pd.DataFrame) -> list[str | None]:
    """Point-in-time L1 industry via backward ``merge_asof`` on ``in_date``."""
    left = pd.DataFrame(
        {
            "code": [draft["code"] for draft in drafts],
            "signal_date": pd.to_datetime([draft["signal_date"] for draft in drafts]),
        }
    )
    left["_row"] = np.arange(len(left))
    left = left.sort_values("signal_date", kind="mergesort")

    right = members.loc[:, ["code", "l1_code", "in_date", "out_date"]].copy()
    right["code"] = right["code"].astype(str)
    right["in_date"] = pd.to_datetime(right["in_date"])
    right["out_date"] = pd.to_datetime(right["out_date"], errors="coerce")
    right = right.sort_values("in_date", kind="mergesort")

    merged = pd.merge_asof(
        left,
        right,
        left_on="signal_date",
        right_on="in_date",
        by="code",
        direction="backward",
    )
    active = merged["in_date"].notna() & (
        merged["out_date"].isna() | (merged["out_date"] > merged["signal_date"])
    )
    resolved: list[str | None] = [None] * len(drafts)
    for row, l1_code, keep in zip(merged["_row"], merged["l1_code"], active):
        if bool(keep) and l1_code is not None and not pd.isna(l1_code):
            resolved[int(row)] = str(l1_code)
    return resolved


def build_candidate_table(
    bars: pd.DataFrame,
    signal_book: Mapping[str, Any],
    *,
    cfg: SimConfig = SimConfig(),
    exit_period: int = 10,
    industry_members: pd.DataFrame | None = None,
    strength_book: Mapping[str, Any] | None = None,
    rejects: Counter[str] | None = None,
) -> pd.DataFrame:
    """Emit one row per ``(strategy, signal_date, code)`` from pre-resonance signals.

    ``bars`` is the full panel (one row per ``(code, date)``) carrying the S1
    ``dyn_path_*_no`` exit columns. ``signal_book`` is the post-cooldown,
    pre-legacy-resonance book. ``strength_book`` is injected by the caller so this
    module never imports the historical backtest driver.
    """
    dyn = _dyn_columns(exit_period)
    _require_columns(bars, _BAR_COLUMNS + _PANEL_COLUMNS, "bars")
    _require_columns(bars, dyn, "bars")

    frame, positions = _canonical_panel(bars)
    views = _code_views(frame)
    starts: dict[str, int] = {}
    cursor = 0
    for code, view in views.items():
        starts[code] = cursor
        cursor += len(view.dates)

    codes = frame["code"].to_numpy(dtype=object)
    dates = frame["date"].to_numpy(dtype="datetime64[ns]")
    opens = frame["open"].to_numpy(dtype=np.float64)
    closes = frame["close"].to_numpy(dtype=np.float64)
    close_raw = frame["close_raw"].to_numpy(dtype=np.float64)
    up_limits = frame["up_limit"].to_numpy(dtype=np.float64)
    volumes = frame["volume"].to_numpy(dtype=np.float64)
    adv20 = _adv20(frame, cfg)

    entry_px_col = frame[dyn[0]].to_numpy(dtype=np.float64, na_value=np.nan)
    exit_bar_col = frame[dyn[1]].to_numpy(dtype=np.float64, na_value=np.nan)
    exit_px_col = frame[dyn[2]].to_numpy(dtype=np.float64, na_value=np.nan)
    exit_reason_col = frame[dyn[3]].to_numpy(dtype=np.float64, na_value=np.nan)
    exit_defer_col = frame[dyn[4]].to_numpy(dtype=np.float64, na_value=np.nan)
    realized_col = frame[dyn[5]].to_numpy(dtype=np.float64, na_value=np.nan)

    drafts: list[dict[str, Any]] = []
    for strategy, series in signal_book.items():
        strategy_name = str(strategy)
        family = STRATEGY_FAMILY.get(strategy_name)
        signals = _aligned_series(series, bars.index).fillna(False).astype(bool).to_numpy()
        rows = np.flatnonzero(signals[positions])
        if rows.size == 0:
            continue
        if family is None:
            _bump(rejects, "unknown_strategy", int(rows.size))
            continue

        strength_values = None
        if strength_book is not None and strategy_name in strength_book:
            strength_values = (
                _aligned_series(strength_book[strategy_name], bars.index)
                .astype(float)
                .to_numpy()[positions]
            )

        for pos in rows:
            code = str(codes[pos])
            view = views[code]
            signal_dt = _as_datetime(dates[pos])
            ordinal = view.index.get(signal_dt)
            if ordinal is None:
                _bump(rejects, "no_next_bar")
                continue
            if ordinal + 1 >= len(view.dates):
                _bump(rejects, "no_next_bar")
                continue

            entry_row = starts[code] + ordinal + 1
            entry_date = dates[entry_row]
            lag = int((entry_date - dates[pos]) / np.timedelta64(1, "D"))
            if lag > cfg.max_entry_lag_days:
                _bump(rejects, "entry_lag")
                continue

            entry_px_raw = float(entry_px_col[pos])
            if not math.isfinite(entry_px_raw):
                _bump(rejects, "no_entry_price")
                continue

            entry_open_raw = float(opens[entry_row] * close_raw[entry_row] / closes[entry_row])
            unbuyable = bool(
                entry_open_raw >= float(up_limits[entry_row]) * 0.999
                or float(volumes[entry_row]) == 0.0
            )

            bar_offset = exit_bar_col[pos]
            if not math.isfinite(bar_offset):
                _bump(rejects, "no_exit")
                continue
            exit_row = entry_row + int(bar_offset)
            if exit_row < entry_row:
                _bump(rejects, "exit_not_after_entry")
                continue
            if exit_row >= len(frame) or codes[exit_row] != code:
                _bump(rejects, "no_exit")
                continue
            if dates[exit_row] <= entry_date:
                _bump(rejects, "exit_not_after_entry")
                continue

            adv_value = float(adv20[pos])
            if not math.isfinite(adv_value):
                _bump(rejects, "no_adv")
                continue
            tier = _adv_tier(adv_value, cfg.adv_tiers)
            slip = float(cfg.adv_tiers[tier][1])

            strength = 1.0
            if strength_values is not None:
                value = float(strength_values[pos])
                if math.isfinite(value):
                    strength = value

            drafts.append(
                {
                    "strategy": strategy_name,
                    "family": family,
                    "signal_date": pd.Timestamp(dates[pos]),
                    "code": code,
                    "entry_date": pd.Timestamp(entry_date),
                    "entry_px_raw": entry_px_raw,
                    "entry_open_raw": entry_open_raw,
                    "unbuyable": unbuyable,
                    "exit_date": pd.Timestamp(dates[exit_row]),
                    "exit_offset": int(bar_offset),
                    "exit_px_intended": float(exit_px_col[pos]),
                    "exit_reason": int(exit_reason_col[pos]),
                    "exit_deferred": int(exit_defer_col[pos]),
                    "exit_open": float(opens[exit_row]),
                    "exit_close": float(closes[exit_row]),
                    "adv20": adv_value,
                    "adv_tier": tier,
                    "slip_entry": slip,
                    "slip_exit": slip,
                    "strength": strength,
                    "ind_l1": None,
                    "n_families": 1,
                    "lead_exp": math.nan,
                    "realized_ret": float(realized_col[pos]),
                }
            )

    if industry_members is None:
        _bump(rejects, "industry_cap_disabled")
    elif drafts:
        for draft, ind_l1 in zip(drafts, _industry_l1(drafts, industry_members)):
            draft["ind_l1"] = ind_l1

    return pd.DataFrame(drafts, columns=list(_CAND_COLUMNS))


def apply_declustered_resonance(
    cands: pd.DataFrame,
    cfg: SimConfig = SimConfig(),
    *,
    rejects: Counter[str] | None = None,
) -> pd.DataFrame:
    """Keep non-vote-excluded rows whose DISTINCT non-excluded family count passes."""
    _require_columns(cands, _CAND_COLUMNS, "cands")
    if cands.empty:
        return cands.loc[:, list(_CAND_COLUMNS)].reset_index(drop=True)

    work = cands.copy()
    excluded = work["strategy"].isin(cfg.vote_exclude)
    counts = (
        work.loc[~excluded, ["signal_date", "code", "family"]]
        .drop_duplicates()
        .groupby(["signal_date", "code"], sort=False)["family"]
        .nunique()
    )
    count_map = counts.to_dict()
    work["n_families"] = [
        int(count_map.get((signal_date, code), 0))
        for signal_date, code in zip(work["signal_date"], work["code"])
    ]

    keep = (~excluded) & (work["n_families"] >= cfg.min_resonance_families)
    dropped = int((~keep).sum())
    _bump(rejects, "decluster_dropped", dropped)
    return work.loc[keep, list(_CAND_COLUMNS)].reset_index(drop=True)


def apply_lead_expectation(
    cands: pd.DataFrame,
    cfg: SimConfig = SimConfig(),
    *,
    rejects: Counter[str] | None = None,
) -> pd.DataFrame:
    """Attach strictly causal ``lead_exp`` and drop rows without enough history.

    ``lead_exp`` for a signal at ``t`` is the mean ``realized_ret`` of the same
    strategy's candidates whose ``exit_date < t``. ``np.searchsorted`` on each
    strategy's sorted exit dates keeps this O(n log n).
    """
    _require_columns(cands, _CAND_COLUMNS, "cands")
    if cands.empty:
        return cands.loc[:, list(_CAND_COLUMNS)].reset_index(drop=True)

    work = cands.copy()
    signal_dates = work["signal_date"].to_numpy(dtype="datetime64[ns]")
    exit_dates = work["exit_date"].to_numpy(dtype="datetime64[ns]")
    realized = work["realized_ret"].to_numpy(dtype=np.float64)
    lead = np.full(len(work), np.nan, dtype=np.float64)
    min_trades = max(int(cfg.expectation_min_trades), 1)

    for indices in work.groupby("strategy", sort=False).indices.values():
        idx = np.asarray(indices, dtype=np.int64)
        order = np.argsort(exit_dates[idx], kind="mergesort")
        sorted_exits = exit_dates[idx][order]
        cumulative = np.concatenate(((0.0,), np.cumsum(realized[idx][order], dtype=np.float64)))
        seen = np.searchsorted(sorted_exits, signal_dates[idx], side="left")
        valid = seen >= min_trades
        if valid.any():
            lead[idx[valid]] = cumulative[seen[valid]] / seen[valid]

    work["lead_exp"] = lead
    keep = work["lead_exp"].notna()
    _bump(rejects, "lead_expectation_ineligible", int((~keep).sum()))
    return work.loc[keep, list(_CAND_COLUMNS)].reset_index(drop=True)


def collapse_candidates(
    cands: pd.DataFrame,
    *,
    rank_score: Tuple[str, ...] = RANK_SCORE,
) -> pd.DataFrame:
    """Collapse to one row per ``(entry_date, code)`` using ``rank_score`` then code/strategy."""
    _require_columns(cands, _CAND_COLUMNS, "cands")
    if cands.empty:
        return cands.loc[:, list(_CAND_COLUMNS)].reset_index(drop=True)

    ordered = cands.sort_values(
        by=["entry_date", *rank_score, "code", "strategy"],
        ascending=[True] + [False] * len(rank_score) + [True, True],
        kind="mergesort",
        na_position="last",
    )
    collapsed = ordered.drop_duplicates(subset=["entry_date", "code"], keep="first")
    return collapsed.loc[:, list(_CAND_COLUMNS)].reset_index(drop=True)
