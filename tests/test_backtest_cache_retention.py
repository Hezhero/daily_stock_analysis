# -*- coding: utf-8 -*-
"""Prepared-state cache retention policy (grill 2026-11-18).

The driver writes one ~800MB parquet per distinct (start, end, universe,
CACHE_VERSION) into ``data/cache/``. Because the file name embeds the run
date, every run on a new day misses the cache and adds a new file, so the
directory grows without bound (13GB / 16 files observed).

These tests pin the externally observable retention behaviour:

- only the driver's own ``backtest_*.parquet`` files are candidates;
- the newest by file mtime is kept (real write time, not the name's date);
- ``keep=0`` deletes nothing;
- deletion failures degrade to a warning, never raise;
- non-parquet files in the same directory are never touched;
- a file vanishing mid-scan never aborts the sweep or lies about the count.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

DRIVER = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_23strategies.py"


def _load_driver(log_dir: Path):
    os.environ["LOG_DIR"] = str(log_dir)
    spec = importlib.util.spec_from_file_location(f"bt_cache_{log_dir.name}", DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_cache(cache_dir: Path, name: str, mtime: float) -> Path:
    p = cache_dir / name
    p.write_bytes(b"x" * 1024)
    os.utime(p, (mtime, mtime))
    return p


def _cache_dir(tmp_path: Path) -> Path:
    d = tmp_path / "cache"
    d.mkdir()
    return d


def test_cleanup_keeps_newest_by_mtime(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "log")
    cache = _cache_dir(tmp_path)
    old = _make_cache(cache, "backtest_20210101_20261019_v2upit.parquet", 1000.0)
    mid = _make_cache(cache, "backtest_20210101_20261113_v2upit.parquet", 2000.0)
    new = _make_cache(cache, "backtest_20210101_20261116_v2upit.parquet", 3000.0)

    result = driver.cleanup_prepared_cache(cache, keep=1)

    assert result["kept"] == 1
    assert result["deleted"] == 2
    assert result["deleted_bytes"] == 2 * 1024
    assert new.exists()
    assert not old.exists()
    assert not mid.exists()


def test_cleanup_keep_zero_deletes_nothing(tmp_path: Path) -> None:
    """keep=0 means "do not delete anything" — no file may be removed."""
    driver = _load_driver(tmp_path / "log")
    cache = _cache_dir(tmp_path)
    f = _make_cache(cache, "backtest_20210101_20261116_v2upit.parquet", 1000.0)

    result = driver.cleanup_prepared_cache(cache, keep=0)

    assert result["deleted"] == 0
    assert result["deleted_bytes"] == 0
    assert f.exists()


def test_cleanup_never_touches_other_files(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "log")
    cache = _cache_dir(tmp_path)
    name_map = cache / "akshare_name_map.json"
    name_map.write_text("{}", encoding="utf-8")
    index_json = cache / "stocks.index.json"
    index_json.write_text("{}", encoding="utf-8")
    stale = _make_cache(cache, "backtest_20210101_20261019_v2upit.parquet", 1000.0)
    fresh = _make_cache(cache, "backtest_20210101_20261116_v2upit.parquet", 2000.0)

    result = driver.cleanup_prepared_cache(cache, keep=1)

    assert result["deleted"] == 1
    assert name_map.exists()
    assert index_json.exists()
    assert fresh.exists()
    assert not stale.exists()


def test_cleanup_missing_dir_is_noop(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path / "log")
    missing = tmp_path / "nope"

    result = driver.cleanup_prepared_cache(missing, keep=1)

    assert result["deleted"] == 0
    assert result["kept"] == 0


def test_cleanup_failure_degrades_to_warning(tmp_path: Path, monkeypatch) -> None:
    """A delete failure must not abort the backtest (cache cleanup is optional)."""
    driver = _load_driver(tmp_path / "log")
    cache = _cache_dir(tmp_path)
    stale = _make_cache(cache, "backtest_20210101_20261019_v2upit.parquet", 1000.0)
    fresh = _make_cache(cache, "backtest_20210101_20261116_v2upit.parquet", 2000.0)

    def _boom(p: Path) -> None:
        raise OSError("locked")

    monkeypatch.setattr(Path, "unlink", _boom)
    result = driver.cleanup_prepared_cache(cache, keep=1)

    # The failure is reported, not raised.
    assert result["failed"] == 1
    assert result["deleted"] == 0
    assert stale.exists()
    assert fresh.exists()


def test_cleanup_survives_file_vanishing_during_scan(
    tmp_path: Path, monkeypatch
) -> None:
    """A file removed between glob and stat must not abort the sweep.

    Regression: the sort key called ``p.stat()``, so one vanished file raised
    and the outer handler returned kept=0 — the log then claimed "保留 0 个"
    while files were still on disk.
    """
    driver = _load_driver(tmp_path / "log")
    cache = _cache_dir(tmp_path)
    stale = _make_cache(cache, "backtest_20210101_20261019_v2upit.parquet", 1000.0)
    fresh = _make_cache(cache, "backtest_20210101_20261116_v2upit.parquet", 2000.0)

    real_stat = Path.stat

    def _stat_then_vanish(self, *args, **kwargs):
        if self.name == stale.name:
            raise FileNotFoundError(self)
        return real_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", _stat_then_vanish)
    result = driver.cleanup_prepared_cache(cache, keep=1)

    assert result["failed"] == 0
    assert result["deleted"] >= 0
    # The surviving newest file must still be kept, and the count must not lie.
    assert result["kept"] >= 1
    assert fresh.exists()
