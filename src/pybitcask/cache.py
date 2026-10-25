"""O(1) thread-safe LRU read cache with TTL expiration awareness."""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any

from pybitcask.keydir import current_time_ms


class LRUCache:
    """Least-Recently-Used (LRU) in-memory cache for hot key-value reads.

    Backed by `collections.OrderedDict` so `get`, `put`, and `invalidate` all
    execute in O(1) average time complexity.
    """

    def __init__(self, capacity: int = 1024, max_value_bytes: int = 1024 * 1024) -> None:
        if capacity < 0:
            raise ValueError(f"capacity must be >= 0, got {capacity}")
        self.capacity: int = capacity
        self.max_value_bytes: int = max_value_bytes
        self._store: OrderedDict[str, tuple[bytes, int]] = OrderedDict()
        self._lock = threading.RLock()
        self.hits: int = 0
        self.misses: int = 0
        self.evictions: int = 0

    def get(self, key: str, now_ms: int | None = None) -> bytes | None:
        """Fetch value bytes from cache if present and not expired."""
        if self.capacity <= 0:
            self.misses += 1
            return None
        with self._lock:
            item = self._store.get(key)
            if item is None:
                self.misses += 1
                return None
            value_bytes, expire_at = item
            if expire_at > 0:
                check_ms = current_time_ms() if now_ms is None else now_ms
                if check_ms >= expire_at:
                    del self._store[key]
                    self.misses += 1
                    return None
            self._store.move_to_end(key)
            self.hits += 1
            return value_bytes

    def put(self, key: str, value_bytes: bytes, expire_at: int = 0) -> None:
        """Insert or update a key in the cache, evicting the LRU entry if full."""
        if self.capacity <= 0:
            return
        if len(value_bytes) > self.max_value_bytes:
            # Avoid polluting the cache with huge blobs; invalidate any stale entry instead.
            self.invalidate(key)
            return
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
                self._store[key] = (value_bytes, expire_at)
                return
            if len(self._store) >= self.capacity:
                self._store.popitem(last=False)
                self.evictions += 1
            self._store[key] = (value_bytes, expire_at)

    def invalidate(self, key: str) -> None:
        """Remove a key from the cache if present."""
        if self.capacity <= 0:
            return
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        """Clear all entries from the cache."""
        with self._lock:
            self._store.clear()

    def stats(self) -> dict[str, Any]:
        """Return cache telemetry metrics."""
        with self._lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total) if total > 0 else 0.0
            return {
                "capacity": self.capacity,
                "size": len(self._store),
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
                "hit_rate": round(hit_rate, 4),
            }

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)
