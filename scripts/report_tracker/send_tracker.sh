#!/bin/bash
# 发送今日精选板块Excel到飞书
EXCEL="/opt/daily_stock_analysis/scripts/report_tracker/stock_tracker_latest.xlsx"
openclaw message send \
  --channel feishu \
  --to "user:ou_e0d9f67aafee182f0fffc2883036e249" \
  --file "$EXCEL" \
  --message "📊 今日精选板块股票跟踪（$(date +%Y-%m-%d)）" 2>&1
