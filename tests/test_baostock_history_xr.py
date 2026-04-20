from contextlib import nullcontext
from datetime import date
import importlib
import queue
import sys
import threading
import types
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest


def _install_dependency_stubs():
    if 'psycopg2' not in sys.modules:
        psycopg2_stub = types.ModuleType('psycopg2')
        psycopg2_stub.Error = Exception

        psycopg2_pool_stub = types.ModuleType('psycopg2.pool')
        psycopg2_pool_stub.ThreadedConnectionPool = object

        psycopg2_sql_stub = types.ModuleType('psycopg2.sql')
        psycopg2_sql_stub.SQL = lambda value: value
        psycopg2_sql_stub.Identifier = lambda value: value

        psycopg2_stub.pool = psycopg2_pool_stub

        sys.modules['psycopg2'] = psycopg2_stub
        sys.modules['psycopg2.pool'] = psycopg2_pool_stub
        sys.modules['psycopg2.sql'] = psycopg2_sql_stub

    if 'backoff' not in sys.modules:
        backoff_stub = types.ModuleType('backoff')
        backoff_stub.expo = object()
        backoff_stub.on_exception = lambda *args, **kwargs: (lambda func: func)
        sys.modules['backoff'] = backoff_stub

    if 'baostock' not in sys.modules:
        baostock_stub = types.ModuleType('baostock')
        baostock_stub.login = lambda: SimpleNamespace(error_code='0', error_msg='')
        baostock_stub.logout = lambda: None
        baostock_stub.query_trade_dates = lambda **kwargs: None
        baostock_stub.query_history_k_data_plus = lambda **kwargs: None
        baostock_stub.query_stock_basic = lambda: None
        sys.modules['baostock'] = baostock_stub


_install_dependency_stubs()
module = importlib.import_module('scripts.data_collection.baostock_history_xr')


class DummyQueryResult:
    def __init__(self, error_code='0', error_msg='', rows=None, fields=None):
        self.error_code = error_code
        self.error_msg = error_msg
        self._rows = list(rows or [])
        self.fields = list(fields or [])
        self._index = -1

    def next(self):
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self):
        return self._rows[self._index]


@pytest.mark.unit
def test_get_daily_data_raises_clear_exception_after_retry_exhaustion(monkeypatch):
    monkeypatch.setattr(module.time, 'sleep', lambda *_: None)
    monkeypatch.setattr(
        module.bs,
        'query_history_k_data_plus',
        lambda **kwargs: DummyQueryResult(error_code='1001', error_msg='temporary failure'),
    )

    with pytest.raises(RuntimeError, match='sh\\.600000.*temporary failure'):
        module.BaoStockAPI.get_daily_data(
            code='sh.600000',
            name='浦发银行',
            ipo_date='1999-11-10',
            out_date='',
            type='1',
            status='1',
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2),
        )


@pytest.mark.unit
def test_close_pool_closes_all_connections_and_clears_pool():
    fake_pool = Mock()
    module.DatabaseManager._pool = fake_pool

    module.DatabaseManager.close_pool()

    fake_pool.closeall.assert_called_once_with()
    assert module.DatabaseManager._pool is None


@pytest.mark.unit
def test_is_trading_day_skips_logout_when_login_fails(monkeypatch):
    logout = Mock()
    monkeypatch.setattr(module.bs, 'login', lambda: SimpleNamespace(error_code='1001', error_msg='login failed'))
    monkeypatch.setattr(module.bs, 'logout', logout)

    assert module.is_trading_day('2024-01-02') is True
    logout.assert_not_called()


@pytest.mark.unit
@pytest.mark.parametrize(
    ('query_result', 'expected_return'),
    [
        (DummyQueryResult(error_code='1001', error_msg='query failed'), True),
        (DummyQueryResult(error_code='0', error_msg='', rows=[]), True),
        (DummyQueryResult(error_code='0', error_msg='', rows=[['2024-01-02', '1']]), True),
        (DummyQueryResult(error_code='0', error_msg='', rows=[['2024-01-02', '0']]), False),
    ],
)
def test_is_trading_day_logs_out_after_successful_login(monkeypatch, query_result, expected_return):
    logout = Mock()
    monkeypatch.setattr(module.bs, 'login', lambda: SimpleNamespace(error_code='0', error_msg=''))
    monkeypatch.setattr(module.bs, 'query_trade_dates', lambda **kwargs: query_result)
    monkeypatch.setattr(module.bs, 'logout', logout)

    assert module.is_trading_day('2024-01-02') is expected_return
    logout.assert_called_once_with()


@pytest.mark.unit
def test_baostock_manager_reuses_thread_local_session(monkeypatch):
    module.BaoStockManager.logout()
    login = Mock(return_value=SimpleNamespace(error_code='0', error_msg=''))
    logout = Mock()
    monkeypatch.setattr(module.bs, 'login', login)
    monkeypatch.setattr(module.bs, 'logout', logout)

    module.BaoStockManager.login()
    module.BaoStockManager.login()
    module.BaoStockManager.logout()
    module.BaoStockManager.logout()

    assert login.call_count == 1
    logout.assert_called_once_with()


@pytest.mark.unit
def test_baostock_manager_logout_clears_thread_local_state_when_bs_logout_fails(monkeypatch):
    module.BaoStockManager.logout()
    logger_exception = Mock()
    monkeypatch.setattr(module.bs, 'login', Mock(return_value=SimpleNamespace(error_code='0', error_msg='')))
    monkeypatch.setattr(module.bs, 'logout', Mock(side_effect=RuntimeError('logout failed')))
    monkeypatch.setattr(module.logger, 'exception', logger_exception)

    module.BaoStockManager.login()
    module.BaoStockManager.logout()
    module.BaoStockManager.logout()

    assert module.BaoStockManager._is_logged_in() is False
    logger_exception.assert_called_once()


@pytest.mark.unit
def test_worker_process_stocks_reuses_session_and_logs_out_once(monkeypatch):
    module.BaoStockManager.logout()
    task_queue = queue.Queue()
    result_queue = queue.Queue()
    rows = [
        {'code': 'sh.600000', 'code_name': 'A'},
        {'code': 'sh.600001', 'code_name': 'B'},
    ]
    for row in rows:
        task_queue.put(row)
    task_queue.put(module._WORKER_SENTINEL)

    login = Mock(return_value=SimpleNamespace(error_code='0', error_msg=''))
    logout = Mock()
    monkeypatch.setattr(module.bs, 'login', login)
    monkeypatch.setattr(module.bs, 'logout', logout)

    def fake_process(args, row, stock_latest_dates=None):
        module.BaoStockManager.login()
        return row['code'], 1, None

    monkeypatch.setattr(module, 'process_single_stock', fake_process)

    module.worker_process_stocks(0, SimpleNamespace(), task_queue, result_queue, {})

    assert login.call_count == 1
    logout.assert_called_once_with()
    assert result_queue.get_nowait() == ('sh.600000', 1, None)
    assert result_queue.get_nowait() == ('sh.600001', 1, None)


@pytest.mark.unit
def test_run_stock_workers_processes_rows_concurrently(monkeypatch):
    stock_list = pd.DataFrame([
        {'code': 'sh.600000', 'code_name': 'A'},
        {'code': 'sh.600001', 'code_name': 'B'},
        {'code': 'sh.600002', 'code_name': 'C'},
    ])
    args = SimpleNamespace(max_workers=3)

    started = threading.Event()
    release = threading.Event()
    state = {'active': 0, 'peak': 0}
    state_lock = threading.Lock()

    def fake_process(args, row, stock_latest_dates=None):
        with state_lock:
            state['active'] += 1
            state['peak'] = max(state['peak'], state['active'])
            if state['active'] == 3:
                started.set()
        started.wait(timeout=1)
        release.wait(timeout=1)
        with state_lock:
            state['active'] -= 1
        return row['code'], 1, None

    monkeypatch.setattr(module, 'process_single_stock', fake_process)
    monkeypatch.setattr(module.BaoStockManager, 'logout', lambda: None)

    worker_thread = threading.Thread(
        target=lambda: module.run_stock_workers(args, stock_list, {}),
        daemon=True,
    )
    worker_thread.start()
    assert started.wait(timeout=1), 'workers did not overlap as expected'
    release.set()
    worker_thread.join(timeout=1)

    assert state['peak'] >= 2


@pytest.mark.unit
def test_main_cleans_up_when_stock_list_retrieval_fails(monkeypatch):
    args = SimpleNamespace(max_workers=2, stock_codes=None, start_date=None, end_date=None)
    logout = Mock()
    close_pool = Mock()

    monkeypatch.setattr(module.DatabaseManager, 'check_connection', lambda: None)
    monkeypatch.setattr(module.DatabaseManager, 'init_pool', lambda min_connections, max_connections: None)
    monkeypatch.setattr(module.DatabaseManager, 'get_connection', lambda: nullcontext(object()))
    monkeypatch.setattr(module.DatabaseOperations, 'init_schema', lambda conn: None)
    monkeypatch.setattr(module.BaoStockManager, 'login', lambda: None)
    monkeypatch.setattr(module.BaoStockManager, 'logout', logout)
    monkeypatch.setattr(module.DatabaseManager, 'close_pool', close_pool)
    monkeypatch.setattr(module.BaoStockAPI, 'get_stock_list', lambda: (_ for _ in ()).throw(RuntimeError('stock list failed')))

    with pytest.raises(SystemExit) as exc_info:
        module.main(args)

    assert exc_info.value.code == 1
    logout.assert_called_once_with()
    close_pool.assert_called_once_with()


@pytest.mark.unit
def test_process_single_stock_skips_bulk_insert_when_daily_data_empty(monkeypatch):
    args = SimpleNamespace(start_date=None, end_date=None)
    row = {'code': 'sh.600000', 'code_name': 'A', 'ipoDate': '1999-11-10', 'outDate': '', 'type': '1', 'status': '1'}
    bulk_insert = Mock()

    monkeypatch.setattr(module.DatabaseManager, 'get_connection', lambda: nullcontext(object()))
    monkeypatch.setattr(module.DateManager, 'get_date_range_fast', lambda *args, **kwargs: (date(2024, 1, 1), date(2024, 1, 2)))
    monkeypatch.setattr(module.BaoStockManager, 'login', lambda: None)
    monkeypatch.setattr(module.BaoStockAPI, 'get_daily_data', lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(module.DatabaseOperations, 'bulk_insert', bulk_insert)

    assert module.process_single_stock(args, row, {}) == ('sh.600000', 0, None)
    bulk_insert.assert_not_called()


@pytest.mark.unit
def test_process_single_stock_inserts_fetched_rows_and_returns_insert_count(monkeypatch):
    args = SimpleNamespace(start_date=None, end_date=None)
    row = {'code': 'sh.600000', 'code_name': 'A', 'ipoDate': '1999-11-10', 'outDate': '', 'type': '1', 'status': '1'}
    data = pd.DataFrame([
        {
            'date': '2024-01-02',
            'code': 'sh.600000',
            'open': '10.0',
            'high': '11.0',
            'low': '9.8',
            'close': '10.5',
            'preclose': '9.9',
            'volume': '1000',
            'amount': '10000',
            'adjustflag': '3',
            'turn': '1.2',
            'tradestatus': '1',
            'pctChg': '6.0',
            'peTTM': '8.0',
            'pbMRQ': '1.0',
            'psTTM': '2.0',
            'pcfNcfTTM': '3.0',
            'isST': '0',
            'name': 'A',
            'ipo_date': '1999-11-10',
            'out_date': '',
            'type': '1',
            'status': '1',
        }
    ])
    bulk_insert = Mock(return_value=7)

    monkeypatch.setattr(module.DatabaseManager, 'get_connection', lambda: nullcontext(object()))
    monkeypatch.setattr(module.DateManager, 'get_date_range_fast', lambda *args, **kwargs: (date(2024, 1, 1), date(2024, 1, 2)))
    monkeypatch.setattr(module.BaoStockManager, 'login', lambda: None)
    monkeypatch.setattr(module.BaoStockAPI, 'get_daily_data', lambda *args, **kwargs: data)
    monkeypatch.setattr(module.DatabaseOperations, 'bulk_insert', bulk_insert)

    assert module.process_single_stock(args, row, {}) == ('sh.600000', 7, None)
    bulk_insert.assert_called_once()


@pytest.mark.unit
def test_main_exits_with_error_when_any_stock_worker_fails(monkeypatch):
    args = SimpleNamespace(max_workers=2, stock_codes=None, start_date=None, end_date=None)
    logout = Mock()
    close_pool = Mock()
    stock_list = pd.DataFrame([
        {'code': 'sh.600000', 'code_name': 'A'},
        {'code': 'sh.600001', 'code_name': 'B'},
    ])
    run_stock_workers = Mock(return_value=[
        ('sh.600000', 3, None),
        ('sh.600001', 0, 'boom'),
    ])

    monkeypatch.setattr(module.DatabaseManager, 'check_connection', lambda: None)
    monkeypatch.setattr(module.DatabaseManager, 'init_pool', lambda min_connections, max_connections: None)
    monkeypatch.setattr(module.DatabaseManager, 'get_connection', lambda: nullcontext(object()))
    monkeypatch.setattr(module.DatabaseOperations, 'init_schema', lambda conn: None)
    monkeypatch.setattr(module.BaoStockManager, 'login', lambda: None)
    monkeypatch.setattr(module.BaoStockManager, 'logout', logout)
    monkeypatch.setattr(module.DatabaseManager, 'close_pool', close_pool)
    monkeypatch.setattr(module.BaoStockAPI, 'get_stock_list', lambda: stock_list)
    monkeypatch.setattr(module.DateManager, 'batch_get_latest_dates', lambda conn, codes: {'sh.600000': date(2024, 1, 1)})
    monkeypatch.setattr(module, 'run_stock_workers', run_stock_workers)

    with pytest.raises(SystemExit) as exc_info:
        module.main(args)

    assert exc_info.value.code == 1
    run_stock_workers.assert_called_once_with(args, stock_list, {'sh.600000': date(2024, 1, 1)})
    logout.assert_called_once_with()
    close_pool.assert_called_once_with()


@pytest.mark.unit
def test_main_aggregates_worker_results_and_cleans_up_once(monkeypatch):
    args = SimpleNamespace(max_workers=2, stock_codes=None, start_date=None, end_date=None)
    logout = Mock()
    close_pool = Mock()
    stock_list = pd.DataFrame([
        {'code': 'sh.600000', 'code_name': 'A'},
        {'code': 'sh.600001', 'code_name': 'B'},
    ])
    run_stock_workers = Mock(return_value=[
        ('sh.600000', 3, None),
        ('sh.600001', 0, None),
    ])

    monkeypatch.setattr(module.DatabaseManager, 'check_connection', lambda: None)
    monkeypatch.setattr(module.DatabaseManager, 'init_pool', lambda min_connections, max_connections: None)
    monkeypatch.setattr(module.DatabaseManager, 'get_connection', lambda: nullcontext(object()))
    monkeypatch.setattr(module.DatabaseOperations, 'init_schema', lambda conn: None)
    monkeypatch.setattr(module.BaoStockManager, 'login', lambda: None)
    monkeypatch.setattr(module.BaoStockManager, 'logout', logout)
    monkeypatch.setattr(module.DatabaseManager, 'close_pool', close_pool)
    monkeypatch.setattr(module.BaoStockAPI, 'get_stock_list', lambda: stock_list)
    monkeypatch.setattr(module.DateManager, 'batch_get_latest_dates', lambda conn, codes: {'sh.600000': date(2024, 1, 1)})
    monkeypatch.setattr(module, 'run_stock_workers', run_stock_workers)

    module.main(args)

    run_stock_workers.assert_called_once_with(args, stock_list, {'sh.600000': date(2024, 1, 1)})
    logout.assert_called_once_with()
    close_pool.assert_called_once_with()
