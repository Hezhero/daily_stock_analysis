# -*- coding: utf-8 -*-
"""Tests for scripts.fetch_tushare_stock_list A-share rt_k fix flow."""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

if "dotenv" not in sys.modules:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *args, **kwargs: None
    dotenv_stub.dotenv_values = lambda *args, **kwargs: {}
    sys.modules["dotenv"] = dotenv_stub

if "tushare" not in sys.modules:
    tushare_stub = types.ModuleType("tushare")
    tushare_stub.pro_api = lambda *args, **kwargs: MagicMock()
    sys.modules["tushare"] = tushare_stub

fetch_tushare_stock_list = importlib.import_module("fetch_tushare_stock_list")


def test_should_fix_a_stock_name_matches_status_prefixes():
    assert fetch_tushare_stock_list.should_fix_a_stock_name("XD西藏药")
    assert fetch_tushare_stock_list.should_fix_a_stock_name("XR浦东建")
    assert fetch_tushare_stock_list.should_fix_a_stock_name("DR罗曼股")
    assert fetch_tushare_stock_list.should_fix_a_stock_name("N惠康")
    assert fetch_tushare_stock_list.should_fix_a_stock_name("C天海")
    assert not fetch_tushare_stock_list.should_fix_a_stock_name("平安银行")
    assert not fetch_tushare_stock_list.should_fix_a_stock_name("ST罗顿")
    assert not fetch_tushare_stock_list.should_fix_a_stock_name("*ST铖昌")


def test_fix_a_stock_names_with_rt_k_replaces_candidate_names():
    api = MagicMock()
    source_df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600848.SH", "300001.SZ"],
            "name": ["XD西藏药", "平安银行", "N惠康"],
        }
    )

    with patch.object(
        fetch_tushare_stock_list,
        "fetch_rt_k_names",
        return_value={"000001.SZ": "西藏药", "300001.SZ": "惠康"},
    ) as fetch_rt_k_names:
        fixed_df = fetch_tushare_stock_list.fix_a_stock_names_with_rt_k(api, source_df)

    assert fixed_df.loc[fixed_df["ts_code"] == "000001.SZ", "name"].iloc[0] == "西藏药"
    assert fixed_df.loc[fixed_df["ts_code"] == "600848.SH", "name"].iloc[0] == "平安银行"
    assert fixed_df.loc[fixed_df["ts_code"] == "300001.SZ", "name"].iloc[0] == "惠康"
    fetch_rt_k_names.assert_called_once_with(api, ["000001.SZ", "300001.SZ"])


def test_fetch_rt_k_names_batches_and_collects_results():
    api = MagicMock()
    api.rt_k.return_value = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "300001.SZ"],
            "name": ["西藏药", "惠康"],
            "close": [1.0, 2.0],
            "pre_close": [1.0, 2.0],
            "trade_time": ["10:00:00", "10:00:00"],
        }
    )

    with patch.object(fetch_tushare_stock_list, "random_sleep") as random_sleep:
        name_map = fetch_tushare_stock_list.fetch_rt_k_names(api, ["000001.SZ", "300001.SZ"])

    assert name_map == {"000001.SZ": "西藏药", "300001.SZ": "惠康"}
    api.rt_k.assert_called_once_with(
        ts_code="000001.SZ,300001.SZ",
        fields="ts_code,name,close,pre_close,trade_time",
    )
    random_sleep.assert_not_called()


def test_main_default_flow_keeps_original_filename():
    api = MagicMock()
    a_df = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["平安银行"]})
    hk_df = pd.DataFrame({"ts_code": ["00001.HK"], "name": ["长和"]})
    us_df = pd.DataFrame({"ts_code": ["AAPL"], "name": ["苹果"]})

    with (
        patch.object(fetch_tushare_stock_list, "get_tushare_api", return_value=api),
        patch.object(fetch_tushare_stock_list, "fetch_a_stock_list", return_value=a_df) as fetch_a,
        patch.object(fetch_tushare_stock_list, "save_to_csv") as save_to_csv,
        patch.object(fetch_tushare_stock_list, "fetch_hk_stock_list", return_value=hk_df) as fetch_hk,
        patch.object(fetch_tushare_stock_list, "fetch_us_stock_list", return_value=us_df) as fetch_us,
        patch.object(fetch_tushare_stock_list, "generate_data_documentation") as generate_doc,
        patch.object(fetch_tushare_stock_list, "random_sleep") as random_sleep,
        patch.object(fetch_tushare_stock_list, "fix_a_stock_names_with_rt_k") as fix_a_stock_names,
    ):
        exit_code = fetch_tushare_stock_list.main([])

    assert exit_code == 0
    fetch_a.assert_called_once_with(api, list_status="L")
    save_to_csv.assert_any_call(a_df, "stock_list_a.csv", "A股")
    fetch_hk.assert_called_once_with(api)
    fetch_us.assert_called_once_with(api)
    fix_a_stock_names.assert_not_called()
    generate_doc.assert_called_once_with(a_df, hk_df, us_df, a_filename="stock_list_a.csv", a_title="A股列表")
    assert random_sleep.call_count == 2


def test_fetch_a_stock_list_merges_multiple_statuses():
    api = MagicMock()
    listed = pd.DataFrame(
        {
            "ts_code": ["600519.SH"],
            "symbol": ["600519"],
            "name": ["贵州茅台"],
            "exchange": ["SSE"],
            "list_status": ["L"],
            "list_date": ["20010827"],
            "delist_date": [None],
        }
    )
    delisted = pd.DataFrame(
        {
            "ts_code": ["605081.SH"],
            "symbol": ["605081"],
            "name": ["退市某某"],
            "exchange": ["SSE"],
            "list_status": ["D"],
            "list_date": ["20200901"],
            "delist_date": ["20251016"],
        }
    )
    api.stock_basic.side_effect = [listed, delisted, pd.DataFrame()]

    result = fetch_tushare_stock_list.fetch_a_stock_list(api, list_status="L,D,P")

    assert result is not None
    assert len(result) == 2
    assert set(result["list_status"].tolist()) == {"L", "D"}
    assert api.stock_basic.call_count == 3
    assert [c.kwargs["list_status"] for c in api.stock_basic.call_args_list] == ["L", "D", "P"]


def test_main_list_status_is_forwarded():
    api = MagicMock()
    a_df = pd.DataFrame({"ts_code": ["605081.SH"], "name": ["退市某某"]})
    hk_df = pd.DataFrame({"ts_code": ["00001.HK"], "name": ["长和"]})
    us_df = pd.DataFrame({"ts_code": ["AAPL"], "name": ["苹果"]})

    with (
        patch.object(fetch_tushare_stock_list, "get_tushare_api", return_value=api),
        patch.object(fetch_tushare_stock_list, "fetch_a_stock_list", return_value=a_df) as fetch_a,
        patch.object(fetch_tushare_stock_list, "save_to_csv"),
        patch.object(fetch_tushare_stock_list, "fetch_hk_stock_list", return_value=hk_df),
        patch.object(fetch_tushare_stock_list, "fetch_us_stock_list", return_value=us_df),
        patch.object(fetch_tushare_stock_list, "generate_data_documentation"),
        patch.object(fetch_tushare_stock_list, "random_sleep"),
    ):
        exit_code = fetch_tushare_stock_list.main(["--list-status", "L,D"])

    assert exit_code == 0
    fetch_a.assert_called_once_with(api, list_status="L,D")


def test_main_non_l_status_writes_separate_artifact():
    api = MagicMock()
    a_df = pd.DataFrame({"ts_code": ["605081.SH"], "name": ["退市某某"]})
    hk_df = pd.DataFrame({"ts_code": ["00001.HK"], "name": ["长和"]})
    us_df = pd.DataFrame({"ts_code": ["AAPL"], "name": ["苹果"]})

    with (
        patch.object(fetch_tushare_stock_list, "get_tushare_api", return_value=api),
        patch.object(fetch_tushare_stock_list, "fetch_a_stock_list", return_value=a_df),
        patch.object(fetch_tushare_stock_list, "save_to_csv") as save_to_csv,
        patch.object(fetch_tushare_stock_list, "fetch_hk_stock_list", return_value=hk_df),
        patch.object(fetch_tushare_stock_list, "fetch_us_stock_list", return_value=us_df),
        patch.object(fetch_tushare_stock_list, "generate_data_documentation") as generate_doc,
        patch.object(fetch_tushare_stock_list, "random_sleep"),
    ):
        exit_code = fetch_tushare_stock_list.main(["--list-status", "D"])

    assert exit_code == 0
    save_to_csv.assert_any_call(a_df, "stock_list_a_D.csv", "A股")
    assert all(call.args[1] != "stock_list_a.csv" for call in save_to_csv.call_args_list)
    generate_doc.assert_not_called()


def test_main_a_rk_flow_overwrites_a_filename_and_rt_k():
    api = MagicMock()
    a_df = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["XD西藏药"]})
    fixed_df = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["西藏药"]})
    hk_df = pd.DataFrame({"ts_code": ["00001.HK"], "name": ["长和"]})
    us_df = pd.DataFrame({"ts_code": ["AAPL"], "name": ["苹果"]})

    with (
        patch.object(fetch_tushare_stock_list, "get_tushare_api", return_value=api),
        patch.object(fetch_tushare_stock_list, "fetch_a_stock_list", return_value=a_df) as fetch_a,
        patch.object(fetch_tushare_stock_list, "fix_a_stock_names_with_rt_k", return_value=fixed_df) as fix_a_stock_names,
        patch.object(fetch_tushare_stock_list, "save_to_csv") as save_to_csv,
        patch.object(fetch_tushare_stock_list, "fetch_hk_stock_list", return_value=hk_df) as fetch_hk,
        patch.object(fetch_tushare_stock_list, "fetch_us_stock_list", return_value=us_df) as fetch_us,
        patch.object(fetch_tushare_stock_list, "generate_data_documentation") as generate_doc,
        patch.object(fetch_tushare_stock_list, "random_sleep") as random_sleep,
    ):
        exit_code = fetch_tushare_stock_list.main(["--a-rk"])

    assert exit_code == 0
    fetch_a.assert_called_once_with(api, list_status="L")
    fix_a_stock_names.assert_called_once_with(api, a_df)
    fixed_save_call = next(
        call for call in save_to_csv.call_args_list if call.args[1] == "stock_list_a.csv"
    )
    pd.testing.assert_frame_equal(fixed_save_call.args[0], fixed_df)
    assert fixed_save_call.args[2] == "A股"
    fetch_hk.assert_called_once_with(api)
    fetch_us.assert_called_once_with(api)
    generate_doc.assert_called_once_with(
        fixed_df,
        hk_df,
        us_df,
        a_filename="stock_list_a.csv",
        a_title="A股列表（修正后）",
    )
    assert random_sleep.call_count == 2
