#!/usr/bin/env bash
# Ticket 03 authorized four-arm A/B run. Serial on purpose: the first arm builds
# the prepared-state parquet cache; the other three reuse it. Concurrent runs
# would contend on the same PG connection and cache write.
#
# All four arms share one data snapshot (PG max trade_date = 2026-11-12,
# verified fresh via check_data_freshness.py --as-of 2026-11-12) and the same
# .env static switches (BACKTEST_CONFLUENCE=1 / BACKTEST_REGIME_SOFT=1).
# --no-handoff skips the real main.py subprocess; portfolio_sim artifacts are
# written earlier, inside run_backtests.
set -u

cd "$(dirname "$0")/.." || exit 1
ROOT=$(pwd)
PY="conda run --live-stream -n daily_stock_analysis python"
DRIVER="scripts/backtest_5y_23strategies.py"
BASE="result/gates/auto-regime-ab/runs"
LOGBASE="logs/auto-regime-ab"
mkdir -p "$BASE" "$LOGBASE"

for MODE in auto enh2 enh strict; do
    OUT="$BASE/$MODE"
    LOGDIR="$LOGBASE/$MODE"
    mkdir -p "$OUT" "$LOGDIR"
    echo "=== [$(date +%H:%M:%S)] START --regime-mode $MODE ==="
    LOG_DIR="$LOGDIR" $PY "$DRIVER" \
        --regime-mode "$MODE" \
        --portfolio-sim \
        --portfolio-sim-out "$OUT" \
        --no-handoff \
        > "$LOGDIR/stdout.log" 2>&1
    RC=$?
    echo "=== [$(date +%H:%M:%S)] DONE  --regime-mode $MODE rc=$RC ==="
    if [ "$RC" -ne 0 ]; then
        echo "ABORT: arm $MODE failed (rc=$RC); see $LOGDIR/stdout.log"
        exit "$RC"
    fi
done
echo "ALL FOUR ARMS COMPLETE"
