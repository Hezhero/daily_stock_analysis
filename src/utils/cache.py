# -*- coding: utf-8 -*-
"""
TTL-based LRU cache for AI analysis results.

Reduces repeated AI calls for the same stock+date combination
by caching AnalysisResult objects with a configurable TTL.

Usage:
    from src.utils.cache import AnalysisCache

    cache = AnalysisCache(max_entries=500, ttl_seconds=3600)
    cache.set("600519", "2026-03-27", result)
    cached = cache.get("600519", "2026-03-27")
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class _CacheEntry:
    value: Any
    expires_at: float  # unix timestamp


class AnalysisCache:
    """
    Thread-safe TTL + LRU cache for AI analysis results.

    Key format: (stock_code, date_str) → cached value.
    Entries expire after ``ttl_seconds`` seconds.
    When ``max_entries`` is exceeded, oldest entries are evicted.
    """

    def __init__(
        self,
        max_entries: int = 500,
        ttl_seconds: int = 3600,
    ):
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._lock = threading.RLock()
        self._cache: Dict[str, _CacheEntry] = {}
        self._access_order: list[str] = []

    # ---- public API ----

    def get(self, code: str, date_str: str) -> Optional[Any]:
        """
        Retrieve cached result for (code, date_str).

        Returns None if key not found or entry has expired.
        """
        key = self._make_key(code, date_str)
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            if time.time() > entry.expires_at:
                self._evict_key_unlocked(key)
                return None
            self._bump_key_unlocked(key)
            return entry.value

    def set(self, code: str, date_str: str, value: Any) -> None:
        """
        Store result in cache with current TTL.
        """
        key = self._make_key(code, date_str)
        with self._lock:
            if len(self._cache) >= self._max_entries and key not in self._cache:
                self._evict_oldest_unlocked()
            entry = _CacheEntry(value=value, expires_at=time.time() + self._ttl_seconds)
            self._cache[key] = entry
            self._bump_key_unlocked(key)

    def invalidate(self, code: str, date_str: str) -> None:
        """Remove a specific entry from cache."""
        key = self._make_key(code, date_str)
        with self._lock:
            self._evict_key_unlocked(key)

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._access_order.clear()

    @property
    def size(self) -> int:
        """Current number of cache entries (including expired ones pending removal)."""
        with self._lock:
            return len(self._cache)

    def stats(self) -> Dict[str, Any]:
        """Return cache statistics for monitoring."""
        with self._lock:
            now = time.time()
            expired = sum(1 for e in self._cache.values() if now > e.expires_at)
            return {
                "max_entries": self._max_entries,
                "ttl_seconds": self._ttl_seconds,
                "total_entries": len(self._cache),
                "expired_pending": expired,
                "active": len(self._cache) - expired,
            }

    # ---- internal helpers ----

    @staticmethod
    def _make_key(code: str, date_str: str) -> str:
        """Normalize and hash the cache key."""
        code_norm = code.strip().lstrip("0").lower()
        date_norm = str(date_str)
        raw = f"{code_norm}::{date_norm}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def _bump_key_unlocked(self, key: str) -> None:
        """Move key to end of access order (most recently used). Caller must hold lock."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    def _evict_key_unlocked(self, key: str) -> None:
        """Remove key from cache and access order. Caller must hold lock."""
        self._cache.pop(key, None)
        if key in self._access_order:
            self._access_order.remove(key)

    def _evict_oldest_unlocked(self) -> None:
        """Evict the least recently used entry. Caller must hold lock."""
        if not self._access_order:
            return
        oldest = self._access_order.pop(0)
        self._cache.pop(oldest, None)


# ---- Global singleton ----

_cache: Optional[AnalysisCache] = None
_cache_lock = threading.Lock()


def get_analysis_cache(
    max_entries: int = 500,
    ttl_seconds: int = 3600,
) -> AnalysisCache:
    """
    Get the global AnalysisCache singleton.

    Parameters are only applied on first call (subsequent calls return
    the existing instance regardless of arguments).
    """
    global _cache
    if _cache is None:
        with _cache_lock:
            if _cache is None:
                _cache = AnalysisCache(
                    max_entries=max_entries,
                    ttl_seconds=ttl_seconds,
                )
                logger.info(
                    f"AnalysisCache initialized: max_entries={max_entries}, "
                    f"ttl_seconds={ttl_seconds}"
                )
    return _cache
