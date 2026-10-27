# -*- coding: utf-8 -*-
"""Windows console guard: report text must survive a cp936 stream."""

from __future__ import annotations

import importlib.util
import io
import os
from pathlib import Path
import pytest

DRIVER = Path(__file__).resolve().parents[1] / "scripts" / "backtest_5y_23strategies.py"

pytestmark = pytest.mark.usefixtures("restore_env_after_test")


def _load_driver(log_dir: Path):
    os.environ["LOG_DIR"] = str(log_dir)
    spec = importlib.util.spec_from_file_location("bt_driver_stdio_test", DRIVER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_configure_stdio_forces_utf8_and_writes_non_gbk_text(tmp_path: Path) -> None:
    driver = _load_driver(tmp_path)
    raw = io.BytesIO()
    stream = io.TextIOWrapper(raw, encoding="gbk")

    driver._configure_stdio_utf8(streams=(stream,))

    assert stream.encoding.lower() == "utf-8"
    stream.write("全期期望 − 0.5×标准差（P1-❶）↔")
    stream.flush()
    assert "↔" in raw.getvalue().decode("utf-8")
