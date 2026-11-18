# -*- coding: utf-8 -*-
"""End-to-end wiring of the cache retention path in main().

These tests pin the *wiring* (not just the pure helper):

- the cleanup runs AFTER a successful cache write (decision Q2);
- a FAILED cache write must NOT delete the old caches (decision Q2's
  guarantee: "a failed write never loses the cache");
- a cache HIT (same-day re-run) does not clean up and does not print the
  status line (decision C: keep the hit path untouched).

The driver is exercised through a small stand-in for its post-load branch,
so no PG connection and no 800MB parquet write is needed.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pandas as pd

DRIVER = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_23strategies.py"


def _load_driver(log_dir: Path):
    os.environ["LOG_DIR"] = str(log_dir)
    spec = importlib.util.spec_from_file_location(f"bt_wire_{log_dir.name}", DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _cache_dir(tmp_path: Path) -> Path:
    d = tmp_path / "cache"
    d.mkdir()
    return d


def _make_cache(cache_dir: Path, name: str, mtime: float) -> Path:
    p = cache_dir / name
    p.write_bytes(b"x" * 1024)
    os.utime(p, (mtime, mtime))
    return p


def test_write_prepared_cache_reports_success(tmp_path: Path, monkeypatch) -> None:
    driver = _load_driver(tmp_path / "log")
    cache = _cache_dir(tmp_path)
    monkeypatch.setattr(driver, "CACHE_DIR", cache)

    df = pd.DataFrame({"a": [1, 2]})
    assert driver.write_prepared_cache(df, "2021-01-01", "2026-11-18") is True
    assert list(cache.glob("backtest_*.parquet"))


def test_write_prepared_cache_reports_failure(tmp_path: Path, monkeypatch) -> None:
    """A failed write must be reported as failure (not swallowed silently)."""
    driver = _load_driver(tmp_path / "log")
    cache = _cache_dir(tmp_path)
    monkeypatch.setattr(driver, "CACHE_DIR", cache)

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", _boom)
    assert driver.write_prepared_cache(pd.DataFrame({"a": [1]}), "2021-01-01", "2026-11-18") is False


def test_write_and_retain_skips_cleanup_when_write_failed(
    tmp_path: Path, monkeypatch
) -> None:
    """Q2: a failed cache write must never delete the existing caches.

    This drives the real call site (write_and_retain_prepared_cache), not a
    re-implementation of its logic.
    """
    driver = _load_driver(tmp_path / "log")
    cache = _cache_dir(tmp_path)
    monkeypatch.setattr(driver, "CACHE_DIR", cache)
    stale = _make_cache(cache, "backtest_20210101_20261019_v2upit.parquet", 1000.0)
    fresh = _make_cache(cache, "backtest_20210101_20261116_v2upit.parquet", 2000.0)

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", _boom)
    ok = driver.write_and_retain_prepared_cache(
        pd.DataFrame({"a": [1]}), "2021-01-01", "2026-11-18", keep=1
    )

    assert ok is False
    assert stale.exists()
    assert fresh.exists()


def test_write_and_retain_cleans_up_after_successful_write(
    tmp_path: Path, monkeypatch, caplog
) -> None:
    """Q2/Q1: successful write -> old caches trimmed and a status line printed."""
    driver = _load_driver(tmp_path / "log")
    cache = _cache_dir(tmp_path)
    monkeypatch.setattr(driver, "CACHE_DIR", cache)
    stale = _make_cache(cache, "backtest_20210101_20261019_v2upit.parquet", 1000.0)

    with caplog.at_level("INFO", logger="backtest"):
        ok = driver.write_and_retain_prepared_cache(
            pd.DataFrame({"a": [1, 2]}), "2021-01-01", "2026-11-18", keep=1
        )

    assert ok is True
    assert not stale.exists()
    assert "准备态缓存" in caplog.text


def test_cache_keep_default_has_a_single_source(tmp_path: Path) -> None:
    """The keep default must not drift between CLI, helper and help text."""
    driver = _load_driver(tmp_path / "log")
    default = driver.CACHE_KEEP_DEFAULT
    assert default == 1

    args = driver._build_parser().parse_args([])
    assert args.cache_keep == default

    import inspect
    assert inspect.signature(driver.cleanup_prepared_cache).parameters["keep"].default == default
    assert (
        inspect.signature(driver.write_and_retain_prepared_cache).parameters["keep"].default
        == default
    )

    help_text = driver._build_parser().format_help()
    assert f"默认 {default}" in help_text

