# -*- coding: utf-8 -*-
"""Shared test helper: load the backtest driver as a fresh module.

The driver is a 7k-line script, not an importable package module, so tests
load it by path. Loading it re-executes module-level setup (logging config,
``LOG_DIR`` creation), so ``LOG_DIR`` must be pointed at a temp dir first.

Each load gets a unique module name: re-using one name would let a later load
silently reuse the earlier module object, hiding "this test re-imported the
driver" behind a cache.
"""

from __future__ import annotations

import importlib.util
import itertools
import os
from pathlib import Path

DRIVER = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_23strategies.py"

_IDS = itertools.count()


def load_driver(log_dir: Path):
    """Import the backtest driver with ``LOG_DIR`` pointed at ``log_dir``."""
    os.environ["LOG_DIR"] = str(log_dir)
    name = f"bt_driver_{next(_IDS)}"
    spec = importlib.util.spec_from_file_location(name, DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
