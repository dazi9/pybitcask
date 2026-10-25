"""Core Bitcask Log-Structured Hash Table Storage Engine."""

from __future__ import annotations

import json
import os
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from pybitcask.cache import LRUCache
from pybitcask.config import BitcaskConfig
from pybitcask.exceptions import (
    CorruptDataError,
    DatabaseClosedError,
    DatabaseLockedError,
    KeyNotFoundError,
    ValidationError,
)
from pybitcask.format import (
    RECORD_HEADER_SIZE,
    KeyEntry,
    encode_record,
)
from pybitcask.keydir import KeyDir, current_time_ms
from pybitcask.logger import get_logger
from pybitcask.segment import DataSegment, hint_file_path, segment_file_path

_SEGMENT_FILENAME_RE = re.compile(r"^data_(\d{8})\.bitcask$")

# Process-local registry of active canonical database paths to detect double-locking
_ACTIVE_LOCKS: set[str] = set()
_ACTIVE_LOCKS_GUARD = threading.Lock()


@dataclass(slots=True, frozen=True)
class BatchOperation:
    """Represents a single put or delete operation in a batch write."""

    op: str  # 'put' or 'delete'
    key: str
    value: str | bytes = b""
    ttl_ms: int | None = None


class BitcaskEngine:
    """Persistent, log-structured key-value storage engine based on the Riak Bitcask paper.

    Features:
    - O(1) disk reads and append-only sequential writes.
    - IEEE CRC32 checksum verification on every record.
    - Multi-segment file rotation (`data_00000001.bitcask`, ...) or single-file mode.
    - Hint files (`.hint`) and seek-over-value startup optimization for fast cold boot.
    - Tombstone deletion and TTL (Time-To-Live) key expiration.
    - Online compaction / garbage collection (`compact()` / `merge()`) to reclaim stale space.
    - O(1) in-memory LRU read cache.
    - Thread-safe operations via RLock and directory/file locking.
    """

    def __init__(
        self,
        file_name: str | Path | None = None,
        *,
        config: BitcaskConfig | None = None,
        single_file_mode: bool | None = None,
        **config_overrides: Any,
    ) -> None:
        if config is not None:
            if file_name is not None:
                config_dict = config.to_dict()
                config_dict["db_path"] = str(file_name)
                config_dict.update(config_overrides)
                self.config = BitcaskConfig(**config_dict)
            elif config_overrides:
                config_dict = config.to_dict()
                config_dict.update(config_overrides)
                self.config = BitcaskConfig(**config_dict)
            else:
                self.config = config
        else:
            db_path = file_name if file_name is not None else config_overrides.pop("db_path", "bitcask_data")
            self.config = BitcaskConfig(db_path=db_path, **config_overrides)

        self.logger = get_logger("pybitcask.engine", self.config.log_level)
        self._lock = threading.RLock()
        self._closed: bool = False

        raw_path = Path(self.config.db_path).resolve()
        if single_file_mode is None:
            # Automatically use single-file mode if path is an existing regular file
            # or has a file extension like .db / .cask / .dat
            self.single_file_mode = raw_path.is_file() or raw_path.suffix.lower() in (
                ".db",
                ".cask",
                ".dat",
                ".bitcask",
            )
        else:
            self.single_file_mode = single_file_mode

        self.path: Path = raw_path
        self.file_name: str = str(self.config.db_path)
        self._lock_key: str = str(self.path)
        self._lock_file_path: Path = (
            self.path.with_suffix(self.path.suffix + ".lock")
            if self.single_file_mode
            else self.path / "bitcask.lock"
        )
        self._acquire_lock()

        self.key_dir = KeyDir()
        self.cache = LRUCache(capacity=self.config.cache_capacity)
        self._segments: dict[int, DataSegment] = {}
        self._active_file_id: int = 1
        self._active_segment: DataSegment
        self._active_dirty: bool = False

        try:
            self._initialize_storage()
        except Exception:
            for seg in self._segments.values():
                seg.close()
            self._segments.clear()
            self._release_lock()
            raise

    def _acquire_lock(self) -> None:
        with _ACTIVE_LOCKS_GUARD:
            if self._lock_key in _ACTIVE_LOCKS:
                raise DatabaseLockedError(
                    f"Database at {self.path} is already open in this process"
                )
            if not self.single_file_mode:
                self.path.mkdir(parents=True, exist_ok=True)
            else:
                self.path.parent.mkdir(parents=True, exist_ok=True)

            if self._lock_file_path.exists():
                try:
                    pid_str = self._lock_file_path.read_text(encoding="utf-8").strip()
                    pid = int(pid_str)
                    if pid != os.getpid() and self._is_pid_alive(pid):
                        raise DatabaseLockedError(
                            f"Database at {self.path} is locked by active process PID={pid}"
                        )
                except (ValueError, OSError):
                    pass

            try:
                self._lock_file_path.write_text(str(os.getpid()), encoding="utf-8")
            except OSError:
                pass
            _ACTIVE_LOCKS.add(self._lock_key)

    @staticmethod
    def _is_pid_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def _release_lock(self) -> None:
        with _ACTIVE_LOCKS_GUARD:
            _ACTIVE_LOCKS.discard(self._lock_key)
            try:
                if self._lock_file_path.exists():
                    self._lock_file_path.unlink()
            except OSError:
                pass

    def _initialize_storage(self) -> None:
        """Scan existing segment files (or hint files) to build the in-memory KeyDir."""
        if self.single_file_mode:
            seg = DataSegment(
                file_id=1,
                path=self.path,
                writable=True,
                sync_policy=self.config.sync_policy,
                sync_interval_records=self.config.sync_interval_records,
                logger=self.logger,
            )
            if self.path.exists() and self.path.stat().st_size > 0:
                try:
                    seg.scan_into_keydir(
                        self.key_dir,
                        verify_crc=self.config.verify_checksum,
                        skip_values=self.config.skip_values_on_startup and not self.config.verify_checksum,
                        recover_tail=self.config.recover_tail,
                        use_hint=self.config.use_hint_files,
                    )
                except Exception:
                    seg.close()
                    raise
            self._segments[1] = seg
            self._active_file_id = 1
            self._active_segment = seg
            return

        # Multi-segment directory mode
        discovered_ids: list[int] = []
        for child in self.path.iterdir():
            if child.is_file():
                m = _SEGMENT_FILENAME_RE.match(child.name)
                if m:
                    discovered_ids.append(int(m.group(1)))

        discovered_ids.sort()
        if not discovered_ids:
            self._active_file_id = 1
            seg_path = segment_file_path(self.path, 1)
            seg = DataSegment(
                file_id=1,
                path=seg_path,
                writable=True,
                sync_policy=self.config.sync_policy,
                sync_interval_records=self.config.sync_interval_records,
                logger=self.logger,
            )
            self._segments[1] = seg
            self._active_segment = seg
            return

        for idx, fid in enumerate(discovered_ids):
            is_last = idx == len(discovered_ids) - 1
            seg_path = segment_file_path(self.path, fid)
            seg = DataSegment(
                file_id=fid,
                path=seg_path,
                writable=is_last,
                sync_policy=self.config.sync_policy,
                sync_interval_records=self.config.sync_interval_records,
                logger=self.logger,
            )
            try:
                seg.scan_into_keydir(
                    self.key_dir,
                    verify_crc=self.config.verify_checksum,
                    skip_values=self.config.skip_values_on_startup and not self.config.verify_checksum,
                    recover_tail=self.config.recover_tail,
                    use_hint=self.config.use_hint_files,
                )
            except Exception:
                seg.close()
                raise
            self._segments[fid] = seg
            if is_last:
                self._active_file_id = fid
                self._active_segment = seg

    def _ensure_open(self) -> None:
        if self._closed:
            raise DatabaseClosedError("BitcaskEngine is closed")

    @property
    def write_position(self) -> int:
        """Current write byte offset of the active segment file."""
        return self._active_segment.write_position

    def _maybe_rotate_segment(self, upcoming_record_size: int) -> None:
        """Rotate the active segment if appending `upcoming_record_size` would exceed max_segment_size."""
        if self.single_file_mode:
            return
        if (
            self._active_segment.write_position > 0
            and self._active_segment.write_position + upcoming_record_size
            > self.config.max_segment_size
        ):
            self._rotate_active_segment()
            if self.config.auto_compact and self._should_auto_compact():
                self.compact()

    def _rotate_active_segment(self) -> None:
        """Seal the current active segment as read-only and open a new active segment."""
        old_seg = self._active_segment
        old_fid = self._active_file_id
        old_seg.mark_readonly()

        if self.config.use_hint_files:
            live_in_old = [
                (k, entry)
                for k, entry in self.key_dir.items()
                if entry.file_id == old_fid
            ]
            DataSegment.write_hint_file(old_seg.path, live_in_old)

        new_fid = old_fid + 1
        new_path = segment_file_path(self.path, new_fid)
        new_seg = DataSegment(
            file_id=new_fid,
            path=new_path,
            writable=True,
            sync_policy=self.config.sync_policy,
            sync_interval_records=self.config.sync_interval_records,
            logger=self.logger,
        )
        self._segments[new_fid] = new_seg
        self._active_file_id = new_fid
        self._active_segment = new_seg
        self._active_dirty = False
        self.logger.info("Rotated active segment from %d to %d", old_fid, new_fid)

    def _effective_dead_bytes(self) -> int:
        total_disk = self.total_disk_bytes()
        live = self.key_dir.live_bytes()
        return max(self.key_dir.dead_bytes(), max(0, total_disk - live))

    def _should_auto_compact(self) -> bool:
        total_disk = self.total_disk_bytes()
        if total_disk < self.config.max_segment_size:
            return False
        dead = self._effective_dead_bytes()
        return (dead / total_disk) >= self.config.compaction_threshold

    def set(
        self,
        key: str,
        value: str | bytes,
        *,
        ttl_ms: int | None = None,
    ) -> None:
        """Store a key-value pair on disk and update the in-memory KeyDir index."""
        self._ensure_open()
        if not isinstance(key, str):
            raise ValidationError(f"Key must be str, got {type(key).__name__}")
        if not isinstance(value, (str, bytes, bytearray)):
            raise ValidationError(
                f"Value must be str or bytes, got {type(value).__name__}"
            )
        if ttl_ms is not None and ttl_ms <= 0:
            raise ValidationError(f"ttl_ms must be > 0, got {ttl_ms}")

        now_ms = current_time_ms()
        expire_at = (now_ms + ttl_ms) if ttl_ms is not None else 0
        val_bytes = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        key_bytes = key.encode("utf-8")

        sz, raw = encode_record(
            timestamp=now_ms,
            key=key_bytes,
            value=val_bytes,
            is_tombstone=False,
            expire_at=expire_at,
            max_key_size=self.config.max_key_size,
            max_value_size=self.config.max_value_size,
        )

        with self._lock:
            self._maybe_rotate_segment(sz)
            rpos, vpos, total_sz = self._active_segment.append_bytes(
                raw, len(key_bytes)
            )
            entry = KeyEntry(
                timestamp=now_ms,
                position=rpos,
                total_size=total_sz,
                file_id=self._active_file_id,
                value_pos=vpos,
                value_size=len(val_bytes),
                expire_at=expire_at,
            )
            self.key_dir.put(key, entry)
            self.cache.put(key, val_bytes, expire_at=expire_at)
            self._active_dirty = True

    def get_bytes(self, key: str) -> bytes | None:
        """Retrieve raw value bytes for `key`, or None if key does not exist or expired."""
        self._ensure_open()
        if not isinstance(key, str) or key == "":
            return None

        with self._lock:
            entry = self.key_dir.get(key)
            if entry is None:
                self.cache.invalidate(key)
                return None

            cached = self.cache.get(key)
            if cached is not None:
                return cached

            seg = self._segments.get(entry.file_id)
            if seg is None:
                raise CorruptDataError(
                    f"KeyDir references missing segment file_id={entry.file_id} for key={key!r}"
                )

            if self.config.verify_checksum:
                rec = seg.read_record(
                    entry.position, entry.total_size, verify_crc=True
                )
                val_bytes = rec.value
            else:
                val_bytes = seg.read_value_only(entry.value_pos, entry.value_size)

            self.cache.put(key, val_bytes, expire_at=entry.expire_at)
            return val_bytes

    def get(self, key: str, default: str = "") -> str:
        """Retrieve string value for `key`, or `default` (empty string) if missing/expired."""
        val_bytes = self.get_bytes(key)
        if val_bytes is None:
            return default
        try:
            return val_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CorruptDataError(f"Stored value for key={key!r} is not UTF-8: {exc}") from exc

    def delete(self, key: str) -> bool:
        """Delete `key` by appending a tombstone record and removing it from KeyDir.

        Returns:
            True if the key existed and was deleted, False if the key did not exist.
        """
        self._ensure_open()
        if not isinstance(key, str) or key == "":
            raise ValidationError("Key must be a non-empty string")

        with self._lock:
            now_ms = current_time_ms()
            existing = self.key_dir.get(key, now_ms=now_ms)
            if existing is None:
                self.cache.invalidate(key)
                return False

            key_bytes = key.encode("utf-8")
            sz, raw = encode_record(
                timestamp=now_ms,
                key=key_bytes,
                value=b"",
                is_tombstone=True,
                expire_at=0,
                max_key_size=self.config.max_key_size,
                max_value_size=self.config.max_value_size,
            )
            self._maybe_rotate_segment(sz)
            _, _, total_sz = self._active_segment.append_bytes(raw, len(key_bytes))
            self.key_dir.delete(
                key,
                tombstone_file_id=self._active_file_id,
                tombstone_size=total_sz,
            )
            self.cache.invalidate(key)
            self._active_dirty = True
            return True

    def batch_write(
        self,
        operations: Iterable[BatchOperation | tuple[str, str, str | bytes | None]],
    ) -> int:
        """Execute a batch of put/delete operations atomically under the engine lock.

        Validates and encodes all records before writing any bytes to disk so that
        validation errors never leave a partially applied batch.

        Args:
            operations: Iterable of `BatchOperation` or `(op, key, value)` tuples where
                `op` is `'put'` or `'delete'`.

        Returns:
            Number of operations applied.
        """
        self._ensure_open()
        now_ms = current_time_ms()
        prepared: list[tuple[str, str, bytes, int, bytes, int]] = []

        for item in operations:
            if isinstance(item, BatchOperation):
                op, key, value, ttl_ms = item.op, item.key, item.value, item.ttl_ms
            elif isinstance(item, tuple) and len(item) >= 2:
                op = item[0]
                key = item[1]
                value = item[2] if len(item) > 2 and item[2] is not None else b""
                ttl_ms = None
            else:
                raise ValidationError(f"Invalid batch operation item: {item!r}")

            if op not in ("put", "delete"):
                raise ValidationError(f"Unsupported batch op {op!r}; expected 'put' or 'delete'")
            if not isinstance(key, str) or key == "":
                raise ValidationError("Batch key must be a non-empty string")
            if ttl_ms is not None and ttl_ms <= 0:
                raise ValidationError("ttl_ms must be > 0")

            expire_at = (now_ms + ttl_ms) if (op == "put" and ttl_ms is not None) else 0
            val_bytes = (
                b""
                if op == "delete"
                else (value.encode("utf-8") if isinstance(value, str) else bytes(value))
            )
            key_bytes = key.encode("utf-8")
            _, raw = encode_record(
                timestamp=now_ms,
                key=key_bytes,
                value=val_bytes,
                is_tombstone=(op == "delete"),
                expire_at=expire_at,
                max_key_size=self.config.max_key_size,
                max_value_size=self.config.max_value_size,
            )
            prepared.append((op, key, key_bytes, expire_at, val_bytes, raw))

        if not prepared:
            return 0

        with self._lock:
            saved_policy = self._active_segment.sync_policy
            # Defer per-record fsync until the entire batch has been appended
            self._active_segment.sync_policy = "os"
            try:
                for op, key, key_bytes, expire_at, val_bytes, raw in prepared:
                    self._maybe_rotate_segment(len(raw))
                    self._active_segment.sync_policy = "os"
                    rpos, vpos, total_sz = self._active_segment.append_bytes(
                        raw, len(key_bytes)
                    )
                    if op == "put":
                        entry = KeyEntry(
                            timestamp=now_ms,
                            position=rpos,
                            total_size=total_sz,
                            file_id=self._active_file_id,
                            value_pos=vpos,
                            value_size=len(val_bytes),
                            expire_at=expire_at,
                        )
                        self.key_dir.put(key, entry)
                        self.cache.put(key, val_bytes, expire_at=expire_at)
                    else:
                        self.key_dir.delete(
                            key,
                            tombstone_file_id=self._active_file_id,
                            tombstone_size=total_sz,
                        )
                        self.cache.invalidate(key)
            finally:
                self._active_segment.sync_policy = saved_policy

            if saved_policy in ("always", "batch"):
                self._active_segment.sync()
            self._active_dirty = True

        return len(prepared)

    def set_json(self, key: str, obj: Any, *, ttl_ms: int | None = None) -> None:
        """Serialize a JSON-compatible Python object and store it under `key`."""
        try:
            payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"Object is not JSON serializable: {exc}") from exc
        self.set(key, payload, ttl_ms=ttl_ms)

    def get_json(self, key: str, default: Any = None) -> Any:
        """Retrieve and deserialize a JSON document stored under `key`."""
        raw = self.get_bytes(key)
        if raw is None:
            return default
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorruptDataError(f"Stored value for {key!r} is not valid JSON: {exc}") from exc

    def exists(self, key: str) -> bool:
        """Return True if `key` exists and has not expired."""
        self._ensure_open()
        with self._lock:
            return self.key_dir.get(key) is not None

    def keys(self) -> list[str]:
        """Return all live keys sorted lexicographically."""
        self._ensure_open()
        with self._lock:
            return self.key_dir.keys()

    def _read_entry_str(self, key: str, entry: KeyEntry, now_ms: int) -> str:
        """Internal fast helper to read a decoded string for a known live KeyEntry."""
        cached = self.cache.get(key, now_ms=now_ms)
        if cached is not None:
            return cached.decode("utf-8")
        seg = self._segments.get(entry.file_id)
        if seg is None:
            raise CorruptDataError(
                f"KeyDir references missing segment file_id={entry.file_id} for key={key!r}"
            )
        if self.config.verify_checksum:
            val_bytes = seg.read_record(
                entry.position, entry.total_size, verify_crc=True
            ).value
        else:
            val_bytes = seg.read_value_only(entry.value_pos, entry.value_size)
        self.cache.put(key, val_bytes, expire_at=entry.expire_at)
        return val_bytes.decode("utf-8")

    def scan_prefix(
        self, prefix: str, *, limit: int | None = None
    ) -> list[tuple[str, str]]:
        """Return sorted `(key, value)` pairs for all live keys starting with `prefix`."""
        self._ensure_open()
        with self._lock:
            now_ms = current_time_ms()
            entries = self.key_dir.scan_prefix(prefix, limit=limit, now_ms=now_ms)
            return [(k, self._read_entry_str(k, entry, now_ms)) for k, entry in entries]

    def scan_range(
        self,
        start_key: str | None = None,
        end_key: str | None = None,
        *,
        include_start: bool = True,
        include_end: bool = True,
        limit: int | None = None,
    ) -> list[tuple[str, str]]:
        """Return sorted `(key, value)` pairs within `[start_key, end_key]`."""
        self._ensure_open()
        with self._lock:
            now_ms = current_time_ms()
            entries = self.key_dir.scan_range(
                start_key,
                end_key,
                include_start=include_start,
                include_end=include_end,
                limit=limit,
                now_ms=now_ms,
            )
            return [(k, self._read_entry_str(k, entry, now_ms)) for k, entry in entries]

    def compact(self) -> dict[str, int]:
        """Run garbage collection (Bitcask merge/compaction) to purge stale & deleted records.

        Rewrites only live, non-expired records into fresh segment file(s), generates
        `.hint` files for instant startup, atomically updates `KeyDir`, and deletes
        old segment files.

        Returns:
            Dictionary with compaction statistics.
        """
        self._ensure_open()
        with self._lock:
            self.key_dir.purge_expired()
            bytes_before = self.total_disk_bytes()

            if self.single_file_mode:
                return self._compact_single_file(bytes_before)
            return self._compact_multi_segment(bytes_before)

    def merge(self) -> dict[str, int]:
        """Alias for `compact()` matching the Bitcask paper terminology."""
        return self.compact()

    def _compact_single_file(self, bytes_before: int) -> dict[str, int]:
        tmp_path = self.path.with_suffix(self.path.suffix + ".compact.tmp")
        if tmp_path.exists():
            tmp_path.unlink()

        old_seg = self._active_segment
        new_seg = DataSegment(
            file_id=1,
            path=tmp_path,
            writable=True,
            sync_policy="os",
            logger=self.logger,
        )
        new_entries: dict[str, KeyEntry] = {}
        migrated = 0

        try:
            for key, entry in self.key_dir.items():
                val_bytes = old_seg.read_value_only(entry.value_pos, entry.value_size)
                key_bytes = key.encode("utf-8")
                _, raw = encode_record(
                    timestamp=entry.timestamp,
                    key=key_bytes,
                    value=val_bytes,
                    is_tombstone=False,
                    expire_at=entry.expire_at,
                    max_key_size=self.config.max_key_size,
                    max_value_size=self.config.max_value_size,
                )
                rpos, vpos, tsize = new_seg.append_bytes(raw, len(key_bytes))
                new_entries[key] = KeyEntry(
                    timestamp=entry.timestamp,
                    position=rpos,
                    total_size=tsize,
                    file_id=1,
                    value_pos=vpos,
                    value_size=len(val_bytes),
                    expire_at=entry.expire_at,
                )
                migrated += 1

            new_seg.sync()
            new_seg.close()
            old_seg.close()

            os.replace(tmp_path, self.path)
            reopened = DataSegment(
                file_id=1,
                path=self.path,
                writable=True,
                sync_policy=self.config.sync_policy,
                sync_interval_records=self.config.sync_interval_records,
                logger=self.logger,
            )
            self._segments[1] = reopened
            self._active_segment = reopened

            self.key_dir.clear()
            for key, entry in new_entries.items():
                self.key_dir.put(key, entry)

            if self.config.use_hint_files:
                DataSegment.write_hint_file(self.path, self.key_dir.items())

            bytes_after = self.total_disk_bytes()
            return {
                "live_keys_migrated": migrated,
                "segments_removed": 1,
                "bytes_before": bytes_before,
                "bytes_after": bytes_after,
                "reclaimed_bytes": max(0, bytes_before - bytes_after),
            }
        finally:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

    def _compact_multi_segment(self, bytes_before: int) -> dict[str, int]:
        # If the active segment also has dead bytes or multiple segments exist,
        # rotate the active segment so all existing segments become immutable candidates.
        if self._active_segment.write_position > 0 and (
            len(self._segments) > 1 or self._effective_dead_bytes() > 0
        ):
            self._rotate_active_segment()

        candidate_fids = sorted(
            fid for fid in self._segments if fid != self._active_file_id
        )
        if not candidate_fids:
            return {
                "live_keys_migrated": 0,
                "segments_removed": 0,
                "bytes_before": bytes_before,
                "bytes_after": bytes_before,
                "reclaimed_bytes": 0,
            }

        candidate_set = set(candidate_fids)
        live_to_migrate = [
            (key, entry)
            for key, entry in self.key_dir.items()
            if entry.file_id in candidate_set
        ]

        # Allocate new compacted segment(s) with IDs after current active_file_id
        next_fid = self._active_file_id + 1
        compacted_seg: DataSegment | None = None
        compacted_segments: dict[int, DataSegment] = {}
        compacted_entries_by_fid: dict[int, list[tuple[str, KeyEntry]]] = {}
        updated_entries: dict[str, KeyEntry] = {}
        migrated = 0

        try:
            for key, entry in live_to_migrate:
                src_seg = self._segments[entry.file_id]
                val_bytes = src_seg.read_value_only(entry.value_pos, entry.value_size)
                key_bytes = key.encode("utf-8")
                sz, raw = encode_record(
                    timestamp=entry.timestamp,
                    key=key_bytes,
                    value=val_bytes,
                    is_tombstone=False,
                    expire_at=entry.expire_at,
                    max_key_size=self.config.max_key_size,
                    max_value_size=self.config.max_value_size,
                )

                if (
                    compacted_seg is None
                    or (
                        compacted_seg.write_position > 0
                        and compacted_seg.write_position + sz > self.config.max_segment_size
                    )
                ):
                    if compacted_seg is not None:
                        compacted_seg.mark_readonly()
                    seg_path = segment_file_path(self.path, next_fid)
                    compacted_seg = DataSegment(
                        file_id=next_fid,
                        path=seg_path,
                        writable=True,
                        sync_policy="os",
                        logger=self.logger,
                    )
                    compacted_segments[next_fid] = compacted_seg
                    compacted_entries_by_fid[next_fid] = []
                    next_fid += 1

                rpos, vpos, tsize = compacted_seg.append_bytes(raw, len(key_bytes))
                new_entry = KeyEntry(
                    timestamp=entry.timestamp,
                    position=rpos,
                    total_size=tsize,
                    file_id=compacted_seg.file_id,
                    value_pos=vpos,
                    value_size=len(val_bytes),
                    expire_at=entry.expire_at,
                )
                updated_entries[key] = new_entry
                compacted_entries_by_fid[compacted_seg.file_id].append((key, new_entry))
                migrated += 1

            if compacted_seg is not None:
                compacted_seg.mark_readonly()

            # Write hint files for all newly created compacted segments
            if self.config.use_hint_files:
                for fid, seg in compacted_segments.items():
                    DataSegment.write_hint_file(seg.path, compacted_entries_by_fid[fid])
        except Exception:
            for seg in compacted_segments.values():
                seg.close()
                seg.path.unlink(missing_ok=True)
                hint_file_path(seg.path).unlink(missing_ok=True)
            raise

        # Register compacted segments and rotate active file ID to stay highest
        self._segments.update(compacted_segments)
        if compacted_segments and self._active_segment.write_position == 0:
            old_empty_active = self._active_segment
            old_empty_fid = self._active_file_id
            old_empty_active.close()
            try:
                old_empty_active.path.unlink(missing_ok=True)
            except OSError:
                pass
            del self._segments[old_empty_fid]

            new_active_fid = next_fid
            new_active_seg = DataSegment(
                file_id=new_active_fid,
                path=segment_file_path(self.path, new_active_fid),
                writable=True,
                sync_policy=self.config.sync_policy,
                sync_interval_records=self.config.sync_interval_records,
                logger=self.logger,
            )
            self._segments[new_active_fid] = new_active_seg
            self._active_file_id = new_active_fid
            self._active_segment = new_active_seg

        # Point KeyDir entries to the compacted segments without adding false dead bytes
        for key, new_entry in updated_entries.items():
            self.key_dir.update_compacted_entry(key, new_entry)

        # Close and remove old immutable segments and their hint files
        for fid in candidate_fids:
            old_seg = self._segments.pop(fid)
            old_seg.close()
            try:
                old_seg.path.unlink(missing_ok=True)
                hint_file_path(old_seg.path).unlink(missing_ok=True)
            except OSError as exc:
                self.logger.warning("Failed to unlink compacted segment %s: %s", old_seg.path, exc)

        self.key_dir.clear_dead_bytes_for_files(candidate_set)
        bytes_after = self.total_disk_bytes()
        return {
            "live_keys_migrated": migrated,
            "segments_removed": len(candidate_fids),
            "bytes_before": bytes_before,
            "bytes_after": bytes_after,
            "reclaimed_bytes": max(0, bytes_before - bytes_after),
        }

    def total_disk_bytes(self) -> int:
        """Return total byte size across all active and immutable data segment files."""
        total = 0
        for seg in self._segments.values():
            if seg.path.exists():
                total += seg.path.stat().st_size
        return total

    def stats(self) -> dict[str, Any]:
        """Return diagnostic metrics about keys, disk usage, dead bytes, segments, and cache."""
        self._ensure_open()
        with self._lock:
            total_bytes = self.total_disk_bytes()
            live_bytes = self.key_dir.live_bytes()
            dead_bytes = self._effective_dead_bytes()
            dead_ratio = (dead_bytes / total_bytes) if total_bytes > 0 else 0.0
            return {
                "db_path": str(self.path),
                "single_file_mode": self.single_file_mode,
                "live_keys": len(self.key_dir),
                "segment_count": len(self._segments),
                "active_file_id": self._active_file_id,
                "total_disk_bytes": total_bytes,
                "live_data_bytes": live_bytes,
                "dead_bytes": dead_bytes,
                "dead_ratio": round(dead_ratio, 4),
                "RecordHeaderSize": RECORD_HEADER_SIZE,
                "cache": self.cache.stats(),
            }

    def sync(self) -> None:
        """Force flush and fsync of the active segment to disk."""
        self._ensure_open()
        with self._lock:
            self._active_segment.sync()

    def close(self) -> None:
        """Flush, sync, and close all segment files and release the database lock."""
        with self._lock:
            if self._closed:
                return
            try:
                if (
                    self.config.use_hint_files
                    and self._active_dirty
                    and not self.single_file_mode
                    and self._active_segment.write_position > 0
                ):
                    live_in_active = [
                        (k, entry)
                        for k, entry in self.key_dir.items()
                        if entry.file_id == self._active_file_id
                    ]
                    DataSegment.write_hint_file(
                        self._active_segment.path, live_in_active
                    )
                    self._active_dirty = False
                elif (
                    self.config.use_hint_files
                    and self._active_dirty
                    and self.single_file_mode
                    and self._active_segment.write_position > 0
                ):
                    DataSegment.write_hint_file(self.path, self.key_dir.items())
                    self._active_dirty = False

                for seg in self._segments.values():
                    seg.close()
            finally:
                self._closed = True
                self._release_lock()

    def __setitem__(self, key: str, value: str | bytes) -> None:
        self.set(key, value)

    def __getitem__(self, item: str) -> str:
        self._ensure_open()
        val_bytes = self.get_bytes(item)
        if val_bytes is None:
            # For compatibility with CaskDB's dict API test (`store["name"] = "jojo"; store["name"]`),
            # returns decoded string when present; raises KeyNotFoundError (subclass of KeyError) when absent.
            raise KeyNotFoundError(item)
        return val_bytes.decode("utf-8")

    def __delitem__(self, key: str) -> None:
        if not self.delete(key):
            raise KeyNotFoundError(key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        return self.exists(key)

    def __len__(self) -> int:
        self._ensure_open()
        return len(self.key_dir)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __enter__(self) -> BitcaskEngine:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


# Alias DiskStorage to BitcaskEngine for 100% drop-in compatibility with avinassh/py-caskdb
DiskStorage = BitcaskEngine
