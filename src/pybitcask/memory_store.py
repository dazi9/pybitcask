"""In-memory key-value storage engine compatible with both CaskDB and BitcaskEngine APIs."""

from __future__ import annotations

import threading
from typing import Iterator

from pybitcask.exceptions import DatabaseClosedError, KeyNotFoundError, ValidationError
from pybitcask.keydir import current_time_ms


class MemoryStorage:
    """Thread-safe in-memory key-value store supporting TTL, prefix/range scan, and dict API."""

    def __init__(self) -> None:
        self.data: dict[str, str] = {}
        self._expires: dict[str, int] = {}
        self._lock = threading.RLock()
        self._closed: bool = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise DatabaseClosedError("MemoryStorage is closed")

    def _is_expired(self, key: str, now_ms: int | None = None) -> bool:
        exp = self._expires.get(key, 0)
        if exp <= 0:
            return False
        check_ms = current_time_ms() if now_ms is None else now_ms
        if check_ms >= exp:
            self.data.pop(key, None)
            self._expires.pop(key, None)
            return True
        return False

    def set(self, key: str, value: str, *, ttl_ms: int | None = None) -> None:
        self._ensure_open()
        if not isinstance(key, str) or key == "":
            raise ValidationError("Key must be a non-empty string")
        if not isinstance(value, str):
            raise ValidationError("Value must be a string")
        with self._lock:
            self.data[key] = value
            if ttl_ms is not None:
                if ttl_ms <= 0:
                    raise ValidationError("ttl_ms must be > 0")
                self._expires[key] = current_time_ms() + ttl_ms
            else:
                self._expires.pop(key, None)

    def get(self, key: str, default: str = "") -> str:
        self._ensure_open()
        with self._lock:
            if key not in self.data or self._is_expired(key):
                return default
            return self.data[key]

    def delete(self, key: str) -> bool:
        self._ensure_open()
        with self._lock:
            if key not in self.data or self._is_expired(key):
                return False
            del self.data[key]
            self._expires.pop(key, None)
            return True

    def keys(self) -> list[str]:
        self._ensure_open()
        with self._lock:
            now_ms = current_time_ms()
            for k in list(self.data.keys()):
                self._is_expired(k, now_ms)
            return sorted(self.data.keys())

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def __setitem__(self, key: str, value: str) -> None:
        self.set(key, value)

    def __getitem__(self, key: str) -> str:
        self._ensure_open()
        with self._lock:
            if key not in self.data or self._is_expired(key):
                raise KeyNotFoundError(key)
            return self.data[key]

    def __delitem__(self, key: str) -> None:
        if not self.delete(key):
            raise KeyNotFoundError(key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        with self._lock:
            return key in self.data and not self._is_expired(key)

    def __len__(self) -> int:
        return len(self.keys())

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())
