#!/bin/bash
# 并行回填启动脚本 - 12个worker
# 总限速: 行情3×30 + 财务9×55 = 585/min 客户端上限
# 实际自然吞吐 ~350/min（行情10-15/min + 财务35/min），安全低于服务端480/min
cd /opt/daily_stock_analysis/scripts/data_collection

W=/opt/daily_stock_analysis/scripts/data_collection/backfill_worker.py
L=/opt/daily_stock_analysis/scripts/data_collection/logs

# ── 行情3表（按日全市场）──
nohup python3 -u $W market daily       --start 20160701 --end 20260731 --rate 30 > $L/w_daily.log 2>&1 &
nohup python3 -u $W market adj_factor  --start 20160701 --end 20260731 --rate 30 > $L/w_adj.log 2>&1 &
nohup python3 -u $W market daily_basic --start 20160701 --end 20260731 --rate 30 > $L/w_db.log 2>&1 &

# ── 财务9表（单股）──
nohup python3 -u $W fin income        --start 20150101 --end 20260731 --rate 55 > $L/w_income.log 2>&1 &
nohup python3 -u $W fin balancesheet  --start 20150101 --end 20260731 --rate 55 > $L/w_bs.log 2>&1 &
nohup python3 -u $W fin cashflow      --start 20150101 --end 20260731 --rate 55 > $L/w_cf.log 2>&1 &
nohup python3 -u $W fin fina_indicator --start 20150101 --end 20260731 --rate 55 > $L/w_fi.log 2>&1 &
nohup python3 -u $W fin forecast      --start 20150101 --end 20260731 --rate 55 > $L/w_fc.log 2>&1 &
nohup python3 -u $W fin express       --start 20150101 --end 20260731 --rate 55 > $L/w_ex.log 2>&1 &
nohup python3 -u $W fin dividend      --start 20150101 --end 20260731 --rate 55 > $L/w_dv.log 2>&1 &
nohup python3 -u $W fin fina_audit    --start 20150101 --end 20260731 --rate 55 > $L/w_fa.log 2>&1 &
nohup python3 -u $W fin fina_mainbz   --start 20150101 --end 20260731 --rate 55 > $L/w_mb.log 2>&1 &

echo "已启动 12 个 worker"
sleep 2
pgrep -f "backfill_worker.py" | grep -v $$ | wc -l
