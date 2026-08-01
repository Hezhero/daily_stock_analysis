# -*- coding: utf-8 -*-
"""Tushare daily_basic API 快速测试脚本。

测试 daily_basic 接口在不同参数组合下的行为：
- 按日全市场（不传 ts_code）
- 按日期范围全市场
- 单股
- 批量多股

用法: python scripts/test_db.py
"""

import json
import os
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), ".env"))

TOKEN = os.getenv("TUSHARE_TOKEN")
URL = os.getenv("TUSHARE_HTTP_URL", "http://api.tushare.pro")
if not URL.startswith("http"):
    URL = "http://api.tushare.pro"

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RESULT_DIR = os.path.join(PROJECT_ROOT, "result")
os.makedirs(RESULT_DIR, exist_ok=True)

with open(os.path.join(RESULT_DIR, "test_db.txt"), "w") as f:
    # Test1: 按日全市场（不传ts_code，这是最优方案）
    req = {
        "api_name": "daily_basic",
        "token": TOKEN,
        "params": {"trade_date": "20260728", "limit": "5"},
        "fields": "ts_code,trade_date,pe,pb,total_mv",
    }
    r = requests.post(URL, json=req, timeout=30)
    d = json.loads(r.text)
    items = (d.get("data") or {}).get("items") or []
    f.write(f"T1(all_by_date_day): code={d.get('code')} rows={len(items)}\n")

    # Test2: 按日期范围全市场
    req["params"] = {"start_date": "20260720", "end_date": "20260728", "limit": "10"}
    r = requests.post(URL, json=req, timeout=30)
    d = json.loads(r.text)
    items = (d.get("data") or {}).get("items") or []
    f.write(
        f"T2(all_by_range): code={d.get('code')} rows={len(items)} "
        f"msg={(d.get('msg') or '')[:80]}\n"
    )

    # Test3: 单股
    req["params"] = {
        "ts_code": "600519.SH",
        "start_date": "20240101",
        "end_date": "20260728",
        "limit": "3",
    }
    r = requests.post(URL, json=req, timeout=30)
    d = json.loads(r.text)
    items = (d.get("data") or {}).get("items") or []
    f.write(f"T3(single): code={d.get('code')} rows={len(items)}\n")

    # Test4: 批量2只
    req["params"] = {
        "ts_code": "600519.SH,000001.SZ",
        "trade_date": "20260728",
        "limit": "5",
    }
    r = requests.post(URL, json=req, timeout=30)
    d = json.loads(r.text)
    items = (d.get("data") or {}).get("items") or []
    f.write(
        f"T4(batch2): code={d.get('code')} rows={len(items)} "
        f"msg={(d.get('msg') or '')[:80]}\n"
    )

    # Test5: 批量7只
    req["params"] = {
        "ts_code": "600519.SH,000001.SZ,000002.SZ,000333.SZ,000651.SZ,000858.SZ,002415.SZ",
        "trade_date": "20260728",
        "limit": "20",
    }
    r = requests.post(URL, json=req, timeout=30)
    d = json.loads(r.text)
    items = (d.get("data") or {}).get("items") or []
    f.write(
        f"T5(batch7): code={d.get('code')} rows={len(items)} "
        f"msg={(d.get('msg') or '')[:80]}\n"
    )

print("done")
