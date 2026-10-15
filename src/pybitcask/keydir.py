"""In-memory KeyDir hash table index with TTL expiration, dead-byte tracking, and range/prefix scans."""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Iterator

from pybitcask.format import KeyEntry


def current_time_ms() -> int:
    """Return current Unix epoch time in milliseconds."""
    return time.time_ns() // 1_000_000


class KeyDir:
    """In-memory hash index mapping keys to their latest on-disk KeyEntry.

    In addition to O(1) point lookups, updates, and deletions, KeyDir tracks:
    - Lazy TTL expiration on access.
    - Per-segment dead (reclaimable) bytes when keys are overwritten or deleted.
    - Sorted lexicographical prefix and range queries over active keys.
    """

    def __init__(self) -> None:
        self._entries: dict[str, KeyEntry] = {}
        self._ttl_keys: set[str] = set()
        self._dead_bytes_by_file: dict[int, int] = defaultdict(int)

    def get(self, key: str, now_ms: int | None = None) -> KeyEntry | None:
        """Retrieve the live KeyEntry for `key`, or None if missing or expired."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expire_at > 0:
            check_ms = current_time_ms() if now_ms is None else now_ms
            if entry.is_expired(check_ms):
                del self._entries[key]
                self._ttl_keys.discard(key)
                self._dead_bytes_by_file[entry.file_id] += entry.total_size
                return None
        return entry

    def put(self, key: str, entry: KeyEntry) -> KeyEntry | None:
        """Insert or update `key` with `entry`.

        If `key` already existed, its previous record size is added to the
        dead-byte counter for its segment file.
        """
        old = self._entries.get(key)
        if old is not None:
            self._dead_bytes_by_file[old.file_id] += old.total_size
        self._entries[key] = entry
        if entry.expire_at > 0:
            self._ttl_keys.add(key)
        else:
            self._ttl_keys.discard(key)
        return old

    def delete(
        self,
        key: str,
        *,
        tombstone_file_id: int | None = None,
        tombstone_size: int = 0,
    ) -> KeyEntry | None:
        """Remove `key` from KeyDir and account for dead bytes."""
        old = self._entries.pop(key, None)
        self._ttl_keys.discard(key)
        if old is not None:
            self._dead_bytes_by_file[old.file_id] += old.total_size
        if tombstone_file_id is not None and tombstone_size > 0:
            self._dead_bytes_by_file[tombstone_file_id] += tombstone_size
        return old

    def purge_expired(self, now_ms: int | None = None) -> int:
        """Actively sweep and remove all expired keys from KeyDir in O(T) time.

        Returns:
            Number of expired keys removed.
        """
        if not self._ttl_keys:
            return 0
        check_ms = current_time_ms() if now_ms is None else now_ms
        expired_keys: list[str] = []
        for k in self._ttl_keys:
            entry = self._entries.get(k)
            if entry is not None and entry.is_expired(check_ms):
                expired_keys.append(k)
        for k in expired_keys:
            self._ttl_keys.discard(k)
            entry = self._entries.pop(k, None)
            if entry is not None:
                self._dead_bytes_by_file[entry.file_id] += entry.total_size
        return len(expired_keys)

    def keys(self, now_ms: int | None = None) -> list[str]:
        """Return all live (non-expired) keys in sorted lexicographical order."""
        check_ms = current_time_ms() if now_ms is None else now_ms
        self.purge_expired(check_ms)
        return sorted(self._entries.keys())

    def items(self, now_ms: int | None = None) -> list[tuple[str, KeyEntry]]:
        """Return all live (key, KeyEntry) pairs sorted by key."""
        check_ms = current_time_ms() if now_ms is None else now_ms
        self.purge_expired(check_ms)
        return sorted(self._entries.items(), key=lambda kv: kv[0])

    def scan_prefix(
        self,
        prefix: str,
        *,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> list[tuple[str, KeyEntry]]:
        """Return live (key, KeyEntry) pairs whose key starts with `prefix`, sorted by key."""
        check_ms = current_time_ms() if now_ms is None else now_ms
        self.purge_expired(check_ms)
        matched = [
            (k, entry)
            for k, entry in self._entries.items()
            if k.startswith(prefix)
        ]
        matched.sort(key=lambda kv: kv[0])
        if limit is not None and limit >= 0:
            return matched[:limit]
        return matched

    def scan_range(
        self,
        start_key: str | None = None,
        end_key: str | None = None,
        *,
        include_start: bool = True,
        include_end: bool = True,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> list[tuple[str, KeyEntry]]:
        """Return live (key, KeyEntry) pairs within the specified lexicographical key range."""
        check_ms = current_time_ms() if now_ms is None else now_ms
        self.purge_expired(check_ms)
        matched: list[tuple[str, KeyEntry]] = []
        for k, entry in self._entries.items():
            if start_key is not None:
                if include_start and k < start_key:
                    continue
                if not include_start and k <= start_key:
                    continue
            if end_key is not None:
                if include_end and k > end_key:
                    continue
                if not include_end and k >= end_key:
                    continue
            matched.append((k, entry))
        matched.sort(key=lambda kv: kv[0])
        if limit is not None and limit >= 0:
            return matched[:limit]
        return matched

    def live_bytes(self, now_ms: int | None = None) -> int:
        """Return total on-disk record bytes occupied by currently live keys."""
        check_ms = current_time_ms() if now_ms is None else now_ms
        self.purge_expired(check_ms)
        return sum(entry.total_size for entry in self._entries.values())

    def dead_bytes(self, file_ids: set[int] | None = None) -> int:
        """Return total reclaimable dead bytes across all or specified segment files."""
        if file_ids is None:
            return sum(self._dead_bytes_by_file.values())
        return sum(
            sz for fid, sz in self._dead_bytes_by_file.items() if fid in file_ids
        )

    def update_compacted_entry(self, key: str, new_entry: KeyEntry) -> None:
        """Update `key` to point to its new compacted segment location without adding dead bytes."""
        if key in self._entries:
            self._entries[key] = new_entry
            if new_entry.expire_at > 0:
                self._ttl_keys.add(key)
            else:
                self._ttl_keys.discard(key)

    def clear_dead_bytes_for_files(self, file_ids: set[int]) -> None:
        """Clear dead byte counters for compacted/removed segment files."""
        for fid in file_ids:
            self._dead_bytes_by_file.pop(fid, None)

    def clear(self) -> None:
        """Remove all entries and reset dead byte counters."""
        self._entries.clear()
        self._ttl_keys.clear()
        self._dead_bytes_by_file.clear()

    def __len__(self) -> int:
        self.purge_expired()
        return len(self._entries)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        return self.get(key) is not None

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())
