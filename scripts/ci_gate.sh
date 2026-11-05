#!/usr/bin/env bash

set -euo pipefail

PYTHON="${PYTHON:-python}"

install_hint() {
  printf "backend-gate: missing %s.\nInstall the backend-gate toolchain with:\n  pip install -r .github/requirements-ci.txt\n" "$1" >&2
}

require_python() {
  if ! "$PYTHON" --version >/dev/null 2>&1; then
    install_hint "'$PYTHON'"
    exit 2
  fi
}

require_python_module() {
  if ! "$PYTHON" -c "import $1" >/dev/null 2>&1; then
    install_hint "Python module '$1' (interpreter: $PYTHON)"
    exit 2
  fi
}

require_python

syntax_check() {
  echo "==> backend-gate: Python syntax check ($PYTHON)"
  "$PYTHON" -m py_compile main.py src/config.py src/auth.py src/analyzer.py src/notification.py
  "$PYTHON" -m py_compile src/storage.py src/scheduler.py src/search_service.py
  "$PYTHON" -m py_compile src/market_analyzer.py src/stock_analyzer.py
  "$PYTHON" -m py_compile data_provider/*.py
}

flake8_checks() {
  require_python_module flake8
  echo "==> backend-gate: flake8 critical checks ($PYTHON)"
  "$PYTHON" -m flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
}

deterministic_checks() {
  require_python
  echo "==> backend-gate: local deterministic checks"
  ./scripts/test.sh code
  ./scripts/test.sh yfinance
}

offline_test_suite() {
  require_python_module pytest
  require_python_module pytest_timeout
  echo "==> backend-gate: offline test suite ($PYTHON)"

  # Native Windows does not have a full-suite baseline (see
  # docs/agents/validation-matrix.md). The checker runs pytest and compares the
  # failure set with the documented whitelist, so known platform differences do
  # not mask new failures.
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*)
      echo "==> backend-gate: native Windows baseline checker"
      "$PYTHON" scripts/check_windows_test_baseline.py
      return
      ;;
  esac

  # Timeout / faulthandler defaults live in setup.cfg so targeted runs get the
  # same hang guard as the gate. Keep CI-only reporting flags here.
  pytest_args=(
    -m "not network"
    --durations=30 --durations-min=0.5
  )

  if [[ -n "${PYTEST_SPLITS:-}" || -n "${PYTEST_GROUP:-}" ]]; then
    if [[ ! "${PYTEST_SPLITS:-}" =~ ^[1-9][0-9]*$ ]] \
      || [[ ! "${PYTEST_GROUP:-}" =~ ^[1-9][0-9]*$ ]] \
      || (( PYTEST_GROUP > PYTEST_SPLITS )); then
      echo "PYTEST_SPLITS and PYTEST_GROUP must be positive integers with group <= splits." >&2
      exit 2
    fi
    if [[ ! -f .github/ci-test-durations.json ]]; then
      echo "Missing .github/ci-test-durations.json required for balanced CI shards." >&2
      exit 2
    fi
    echo "==> backend-gate: pytest shard ${PYTEST_GROUP}/${PYTEST_SPLITS}"
    "$PYTHON" scripts/ci_test_shard.py \
      --splits "${PYTEST_SPLITS}" \
      --group "${PYTEST_GROUP}" \
      --first-shard-overhead "${PYTEST_FIRST_SHARD_OVERHEAD:-0}" \
      -- "${pytest_args[@]}"
    return
  fi

  "$PYTHON" -m pytest "${pytest_args[@]}"
}

run_all() {
  syntax_check
  flake8_checks
  deterministic_checks
  offline_test_suite
  echo "==> backend-gate: all checks passed"
}

phase="${1:-all}"

case "$phase" in
  all)
    run_all
    ;;
  syntax)
    syntax_check
    ;;
  flake8)
    flake8_checks
    ;;
  deterministic)
    deterministic_checks
    ;;
  offline-tests)
    offline_test_suite
    ;;
  *)
    echo "Usage: $0 [all|syntax|flake8|deterministic|offline-tests]" >&2
    exit 2
    ;;
esac
