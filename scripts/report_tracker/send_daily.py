#!/opt/daily_stock_analysis/venv/bin/python3
# -*- coding: utf-8 -*-
"""
每天16:10运行：更新数据 → 在独立session中发送Excel到飞书
"""
import sys, os, json
from datetime import datetime

sys.path.insert(0, '/opt/daily_stock_analysis/scripts/report_tracker')
import update_tracker as ut

SEND_FLAG = "/opt/daily_stock_analysis/scripts/report_tracker/send_flag.json"

print(f"[{datetime.now().strftime('%H:%M:%S')}] === Daily Tracker ===")

# 1. 更新数据
try:
    ut.main()
except Exception as e:
    print(f"Tracker error: {e}")

excel_path = "/opt/daily_stock_analysis/scripts/report_tracker/stock_tracker_latest.xlsx"
if not os.path.exists(excel_path):
    print(f"Excel not found: {excel_path}")
    sys.exit(1)

# 2. 写入发送标记
today = datetime.now().strftime('%Y-%m-%d')
flag = {
    "action": "send_feishu",
    "file": excel_path,
    "filename": f"stock_tracker_{today}.xlsx",
    "target": "ou_e0d9f67aafee182f0fffc2883036e249",
    "message": f"📊 每日回测推荐股票追踪（{today}）已更新，请查收附件。",
    "created_at": datetime.now().isoformat()
}
with open(SEND_FLAG, 'w') as f:
    json.dump(flag, f, ensure_ascii=False)

print(f"Flag written, will be sent by main session")
print(f"[DONE] {datetime.now().strftime('%H:%M:%S')}")
