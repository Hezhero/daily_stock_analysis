# -*- coding: utf-8 -*-
"""Pytest compatibility hooks."""

from __future__ import annotations

import asyncio
import concurrent.futures
import importlib
import os
import tempfile
import time
import threading
from collections.abc import Awaitable, Callable
from contextvars import copy_context
from functools import wraps
from typing import Any, TypeVar
from warnings import warn

import anyio.to_thread
import dotenv
import pytest
import fastapi.testclient
import httpx
import starlette.testclient
from anyio._backends import _asyncio

# Ticket 11: the suite points ENV_FILE at a missing path, but bare
# ``load_dotenv()`` calls (notably scripts/backtest_5y_23strategies.py) search
# upward from the caller and would still read the developer's repo-root .env.
# Route bare calls through the same ENV_FILE contract; an explicit dotenv_path
# remains the opt-in.
_REAL_LOAD_DOTENV = dotenv.load_dotenv


def _load_dotenv_with_env_file(*args, **kwargs):
    env_file = os.environ.get("ENV_FILE")
    dotenv_path = kwargs.get("dotenv_path")
    if dotenv_path is None and args:
        dotenv_path = args[0]
    if dotenv_path is None and env_file:
        if not os.path.exists(env_file):
            return False
        kwargs["dotenv_path"] = env_file
    return _REAL_LOAD_DOTENV(*args, **kwargs)


dotenv.load_dotenv = _load_dotenv_with_env_file

# Leak probe (ticket 12, D1). With ``DSA_ENV_LEAK_PROBE=1`` the autouse fixture
# below deliberately stops restoring the environment, and each test's setup is
# compared with the previous setup, so the probe names the test that left an env
# entry behind. Singleton transitions are reported separately as informational:
# fixtures create and reset singletons by design, so they are not gating.
# Inert unless the variable is set.
_PROBE_VARS = (
    "ENV_FILE",
    "DATABASE_PATH",
    "REALTIME_SOURCE_PRIORITY",
    "ADMIN_AUTH_ENABLED",
    "LLM_CHANNELS",
)
_PROBE_SINGLETONS = {
    "Config._instance": ("src.config", "Config"),
    "DatabaseManager._instance": ("src.storage", "DatabaseManager"),
}
_PROBE_ENV_STATE: dict[str, str] | None = None
_PROBE_SINGLETON_STATE: dict[str, str] | None = None
_PROBE_LAST_NODEID = "<session start>"
_PROBE_ENV_LEAKS: list[tuple[str, str, Any, Any]] = []
_PROBE_SINGLETON_TRANSITIONS: list[tuple[str, str, Any, Any]] = []


def _singleton_marker(module_name: str, class_name: str) -> str:
    """Lazy on purpose: importing these at conftest import would run setup_env()."""
    try:
        module = importlib.import_module(module_name)
        instance = getattr(getattr(module, class_name), "_instance", None)
    except Exception:
        return "unimported"
    return "none" if instance is None else "set"


def _probe_env_snapshot() -> dict[str, str]:
    return {name: repr(os.environ.get(name)) for name in _PROBE_VARS}


def _probe_singleton_snapshot() -> dict[str, str]:
    return {
        label: _singleton_marker(module_name, class_name)
        for label, (module_name, class_name) in _PROBE_SINGLETONS.items()
    }


# Session env baseline (ticket 11). ``src.config`` calls ``setup_env()`` at import
# time, which copies the repo-root ``.env`` into ``os.environ``; this runs while
# conftest is loaded, i.e. before any test module triggers that import. Tests that
# need a real env file assign ``ENV_FILE`` themselves — that is the opt-in.
_AMBIENT_KEYS = (
    "ADMIN_AUTH_ENABLED",
    "REALTIME_SOURCE_PRIORITY",
    "SEARXNG_PUBLIC_INSTANCES_ENABLED",
    "LLM_CHANNELS",
)

# Keep the offline suite offline: litellm otherwise fetches its remote model-cost
# map on first use, which shows up as multi-second hangs and load-dependent flakes.
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")

_BASELINE_ENV_FILE = os.path.join(tempfile.mkdtemp(prefix="dsa-test-env-"), "no-such.env")
os.environ["ENV_FILE"] = _BASELINE_ENV_FILE
for _ambient_key in _AMBIENT_KEYS:
    os.environ.pop(_ambient_key, None)


@pytest.fixture(autouse=True)
def _enforce_ambient_env_baseline():
    """Snapshot ``os.environ`` for every test and restore it afterwards.

    Several modules call bare ``load_dotenv()`` / ``setup_env()``, which reads the
    repository ``.env`` (``DATABASE_PATH``, ``PG_*``, ``LLM_*``) and bypasses the
    ``ENV_FILE`` baseline set above. Without a full restore, later tests inherit a
    real database path (SQLite on /mnt/f then reports "disk I/O error" under load)
    or live PostgreSQL credentials (capital-flow tests hit the PG fallback instead
    of the mocked online source).

    ``DSA_ENV_LEAK_PROBE=1`` disables both the ambient-key scrub and the restore
    so ``pytest_runtest_setup`` can observe what each test actually leaves behind;
    do not use it as the normal pass/fail mode.

    ``ENV_FILE`` is deliberately not forced: several modules set it at import
    time on purpose, and a per-test override would break them.
    """
    probe = os.environ.get("DSA_ENV_LEAK_PROBE") == "1"
    if not probe:
        before = dict(os.environ)
        for name in _AMBIENT_KEYS:
            os.environ.pop(name, None)
    yield
    if probe:
        return
    for key in list(os.environ):
        if key not in before:
            os.environ.pop(key, None)
    os.environ.update(before)


def pytest_runtest_setup(item):
    global _PROBE_ENV_STATE, _PROBE_LAST_NODEID, _PROBE_SINGLETON_STATE
    if os.environ.get("DSA_ENV_LEAK_PROBE") != "1":
        return
    current_env = _probe_env_snapshot()
    current_singletons = _probe_singleton_snapshot()
    if _PROBE_ENV_STATE is None:
        _PROBE_ENV_STATE = current_env
        _PROBE_SINGLETON_STATE = current_singletons
        _PROBE_LAST_NODEID = item.nodeid
        return
    for name, value in current_env.items():
        previous = _PROBE_ENV_STATE.get(name)
        if value != previous:
            _PROBE_ENV_LEAKS.append((_PROBE_LAST_NODEID, name, previous, value))
    for name, value in current_singletons.items():
        previous = _PROBE_SINGLETON_STATE.get(name)
        if value != previous:
            _PROBE_SINGLETON_TRANSITIONS.append(
                (_PROBE_LAST_NODEID, name, previous, value)
            )
    _PROBE_ENV_STATE = current_env
    _PROBE_SINGLETON_STATE = current_singletons
    _PROBE_LAST_NODEID = item.nodeid


def pytest_sessionfinish(session, exitstatus):
    if os.environ.get("DSA_ENV_LEAK_PROBE") != "1":
        return
    if not _PROBE_ENV_LEAKS:
        print("\n[env-leak-probe] clean: no env transitions between tests")
    else:
        print(
            f"\n[env-leak-probe] {len(_PROBE_ENV_LEAKS)} state transition(s) not restored:"
        )
        for nodeid, name, before, after in _PROBE_ENV_LEAKS:
            print(f"  {nodeid} :: {name}: {before!r} -> {after!r}")
    if _PROBE_SINGLETON_TRANSITIONS:
        print(
            f"[env-leak-probe] singleton transitions: "
            f"{len(_PROBE_SINGLETON_TRANSITIONS)} (informational)"
        )


T = TypeVar("T")

_original_call_soon_threadsafe = asyncio.BaseEventLoop.call_soon_threadsafe


async def _shutdown_default_executor_inline(
    self: asyncio.BaseEventLoop,
    timeout: float | None = None,
) -> None:
    """Avoid lost wakeups while asyncio.run() tears down test-only executors."""
    del timeout
    executor = getattr(self, "_default_executor", None)
    if executor is None:
        return
    self._executor_shutdown_called = True
    self._default_executor = None
    executor.shutdown(wait=True)


def _call_soon_threadsafe_with_extra_wakeup(
    self: asyncio.BaseEventLoop,
    callback,
    *args,
    context=None,
):
    """Wake the selector again for sandboxed test runs where the first wake is lost."""
    handle = _original_call_soon_threadsafe(self, callback, *args, context=context)
    write_to_self = getattr(self, "_write_to_self", None)
    if callable(write_to_self):
        write_to_self()
        threading.Timer(0.001, write_to_self).start()
    return handle


asyncio.BaseEventLoop.call_soon_threadsafe = _call_soon_threadsafe_with_extra_wakeup
asyncio.BaseEventLoop.shutdown_default_executor = _shutdown_default_executor_inline


async def _run_sync_via_asyncio_to_thread(
    func: Callable[..., T],
    *args: Any,
    abandon_on_cancel: bool = False,
    cancellable: bool | None = None,
    limiter: Any = None,
) -> T:
    """Use asyncio's executor path when AnyIO worker queues miss wakeups."""
    del abandon_on_cancel, limiter
    if cancellable is not None:
        warn(
            "The `cancellable=` keyword argument to `anyio.to_thread.run_sync` is "
            "deprecated since AnyIO 4.1.0; use `abandon_on_cancel=` instead",
            DeprecationWarning,
            stacklevel=2,
        )
    future: concurrent.futures.Future[T] = concurrent.futures.Future()
    context = copy_context()

    def runner() -> None:
        try:
            future.set_result(context.run(func, *args))
        except BaseException as exc:
            future.set_exception(exc)

    threading.Thread(target=runner, name="pytest-anyio-worker", daemon=True).start()
    while not future.done():
        await asyncio.sleep(0.001)
    return future.result()


def _wait_for_cross_thread_result(loop: asyncio.AbstractEventLoop, future: concurrent.futures.Future[T]) -> T:
    write_to_self = getattr(loop, "_write_to_self", None)
    while not future.done():
        if callable(write_to_self):
            write_to_self()
        time.sleep(0.001)
    return future.result()


def _run_sync_from_thread_with_wakeup(
    cls,
    func: Callable[..., T],
    args: tuple[Any, ...],
    token: object,
) -> T:
    @wraps(func)
    def wrapper() -> None:
        try:
            _asyncio.set_current_async_library("asyncio")
            future.set_result(func(*args))
        except BaseException as exc:
            future.set_exception(exc)
            if not isinstance(exc, Exception):
                raise

    loop = token or _asyncio.threadlocals.current_token.native_token
    if loop.is_closed():
        raise _asyncio.RunFinishedError
    future: concurrent.futures.Future[T] = concurrent.futures.Future()
    loop.call_soon_threadsafe(wrapper)
    return _wait_for_cross_thread_result(loop, future)


def _run_async_from_thread_with_wakeup(
    cls,
    func: Callable[..., Awaitable[T]],
    args: tuple[Any, ...],
    token: object,
) -> T:
    loop = token or _asyncio.threadlocals.current_token.native_token
    if loop.is_closed():
        raise _asyncio.RunFinishedError
    context = copy_context()
    context.run(_asyncio.set_current_async_library, "asyncio")
    future = context.run(asyncio.run_coroutine_threadsafe, func(*args), loop=loop)
    return _wait_for_cross_thread_result(loop, future)


anyio.to_thread.run_sync = _run_sync_via_asyncio_to_thread
_asyncio.AsyncIOBackend.run_sync_from_thread = classmethod(_run_sync_from_thread_with_wakeup)
_asyncio.AsyncIOBackend.run_async_from_thread = classmethod(_run_async_from_thread_with_wakeup)


class _ThreadlessTestClient:
    """Small TestClient replacement that avoids AnyIO's cross-thread portal."""

    def __init__(
        self,
        app,
        base_url: str = "http://testserver",
        raise_server_exceptions: bool = True,
        follow_redirects: bool = True,
        **_: Any,
    ) -> None:
        self.app = app
        self.base_url = base_url
        self.raise_server_exceptions = raise_server_exceptions
        self.follow_redirects = follow_redirects
        self.cookies = httpx.Cookies()
        self._lifespan_ctx = None
        self._lifespan_enter_count = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._async_client: httpx.AsyncClient | None = None

    def _get_lifespan_context(self):
        return getattr(getattr(self.app, "router", None), "lifespan_context", None)

    def _build_async_client(self, follow_redirects: bool) -> httpx.AsyncClient:
        transport = httpx.ASGITransport(
            app=self.app,
            raise_app_exceptions=self.raise_server_exceptions,
        )
        return httpx.AsyncClient(
            transport=transport,
            base_url=self.base_url,
            follow_redirects=follow_redirects,
            cookies=self.cookies,
        )

    def __enter__(self):
        if self._lifespan_enter_count == 0:
            self._loop = asyncio.new_event_loop()
            lifespan_context = self._get_lifespan_context()
            if callable(lifespan_context):
                self._lifespan_ctx = lifespan_context(self.app)
                self._loop.run_until_complete(self._lifespan_ctx.__aenter__())
            self._async_client = self._build_async_client(self.follow_redirects)
        self._lifespan_enter_count += 1
        return self

    def __exit__(self, *args: Any) -> None:
        if self._lifespan_enter_count == 0:
            return None

        self._lifespan_enter_count -= 1
        if self._lifespan_enter_count == 0 and self._loop is not None:
            try:
                async def _close() -> None:
                    if self._async_client is not None:
                        await self._async_client.aclose()
                    if self._lifespan_ctx is not None:
                        await self._lifespan_ctx.__aexit__(*args)

                self._loop.run_until_complete(_close())
            finally:
                self._lifespan_ctx = None
                self._async_client = None
                self._loop.close()
                self._loop = None
        return None

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        follow_redirects = kwargs.pop("follow_redirects", self.follow_redirects)
        kwargs.pop("allow_redirects", None)

        if self._lifespan_enter_count > 0 and self._loop is not None and self._async_client is not None:
            response = self._loop.run_until_complete(
                self._async_client.request(method, url, follow_redirects=follow_redirects, **kwargs)
            )
            self.cookies = httpx.Cookies(self._async_client.cookies)
            return response

        async def _send() -> httpx.Response:
            async with self._build_async_client(follow_redirects) as client:
                response = await client.request(method, url, **kwargs)
                self.cookies = httpx.Cookies(client.cookies)
                return response

        return asyncio.run(_send())

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", url, **kwargs)

    def patch(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("PATCH", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("DELETE", url, **kwargs)

    def head(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("HEAD", url, **kwargs)


fastapi.testclient.TestClient = _ThreadlessTestClient
starlette.testclient.TestClient = _ThreadlessTestClient
