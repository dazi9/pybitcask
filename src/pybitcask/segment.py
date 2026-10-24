"""Log segment file management, hint file indexing, and crash tail recovery."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import BinaryIO, Iterable

from pybitcask.exceptions import CorruptDataError, DatabaseClosedError
from pybitcask.format import (
    FLAG_HAS_TTL,
    FLAG_NORMAL,
    FLAG_TOMBSTONE,
    HINT_HEADER_SIZE,
    RECORD_HEADER_SIZE,
    DecodedRecord,
    HintRecord,
    KeyEntry,
    compute_crc32,
    decode_hint_header,
    decode_hint_record,
    decode_record,
    decode_record_header,
    encode_hint_record,
)
from pybitcask.keydir import KeyDir


def segment_file_path(base_dir: Path, file_id: int) -> Path:
    """Return canonical path for a data segment file."""
    return base_dir / f"data_{file_id:08d}.bitcask"


def hint_file_path(segment_path: Path) -> Path:
    """Return companion .hint file path for a given segment file."""
    return segment_path.with_suffix(".hint")


class DataSegment:
    """Manages an append-only or immutable read-only Bitcask segment file on disk."""

    def __init__(
        self,
        file_id: int,
        path: Path,
        *,
        writable: bool = True,
        sync_policy: str = "os",
        sync_interval_records: int = 128,
        logger: logging.Logger | None = None,
    ) -> None:
        self.file_id: int = file_id
        self.path: Path = Path(path)
        self.writable: bool = writable
        self.sync_policy: str = sync_policy
        self.sync_interval_records: int = sync_interval_records
        self.logger: logging.Logger = logger or logging.getLogger("pybitcask.segment")
        self._unflushed_records: int = 0
        self._closed: bool = False

        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.touch()

        mode = "r+b" if writable else "rb"
        self._file: BinaryIO = open(self.path, mode)
        self._fd: int = self._file.fileno()
        self._has_pread: bool = hasattr(os, "pread")
        self._file.seek(0, os.SEEK_END)
        self.write_position: int = self._file.tell()
        self._cursor_at_end: bool = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise DatabaseClosedError(f"Segment {self.path} is closed")

    def append_bytes(self, data: bytes, key_byte_len: int) -> tuple[int, int, int]:
        """Append raw encoded record bytes to the end of the segment.

        Returns:
            Tuple of (record_pos, value_pos, total_size).
        """
        self._ensure_open()
        if not self.writable:
            raise PermissionError(f"Segment {self.path} is opened read-only")

        record_pos = self.write_position
        total_size = len(data)
        value_pos = record_pos + RECORD_HEADER_SIZE + key_byte_len

        if not self._cursor_at_end:
            self._file.seek(record_pos, os.SEEK_SET)
            self._cursor_at_end = True
        self._file.write(data)
        self.write_position += total_size
        self._unflushed_records += 1

        if self.sync_policy == "always":
            self._file.flush()
            os.fsync(self._fd)
            self._unflushed_records = 0
        elif self.sync_policy == "batch":
            self._file.flush()
            if self._unflushed_records >= self.sync_interval_records:
                os.fsync(self._fd)
                self._unflushed_records = 0
        else:
            # 'os' policy: flush Python userspace buffer to OS kernel page cache
            self._file.flush()

        return record_pos, value_pos, total_size

    def _pread_exact(self, size: int, offset: int) -> bytes:
        """Read up to `size` bytes starting at `offset` using os.pread, looping on partial reads."""
        chunks: list[bytes] = []
        remaining = size
        cur_offset = offset
        while remaining > 0:
            chunk = os.pread(self._fd, remaining, cur_offset)
            if not chunk:
                break
            chunks.append(chunk)
            n = len(chunk)
            remaining -= n
            cur_offset += n
        return b"".join(chunks) if len(chunks) != 1 else chunks[0]

    def read_record(
        self,
        record_pos: int,
        total_size: int,
        *,
        verify_crc: bool = True,
    ) -> DecodedRecord:
        """Read and decode a full record from `[record_pos, record_pos + total_size)`."""
        self._ensure_open()
        if self._has_pread:
            raw = self._pread_exact(total_size, record_pos)
        else:
            self._file.seek(record_pos, os.SEEK_SET)
            self._cursor_at_end = False
            raw = self._file.read(total_size)
        if len(raw) != total_size:
            raise CorruptDataError(
                f"Short read in segment {self.file_id} at offset {record_pos}: "
                f"expected {total_size} bytes, got {len(raw)}"
            )
        return decode_record(raw, verify_crc=verify_crc)

    def read_value_only(self, value_pos: int, value_size: int) -> bytes:
        """Fast-path read that reads `value_size` bytes at `value_pos` via os.pread."""
        self._ensure_open()
        if value_size == 0:
            return b""
        if self._has_pread:
            raw = self._pread_exact(value_size, value_pos)
        else:
            self._file.seek(value_pos, os.SEEK_SET)
            self._cursor_at_end = False
            raw = self._file.read(value_size)
        if len(raw) != value_size:
            raise CorruptDataError(
                f"Short value read in segment {self.file_id} at offset {value_pos}: "
                f"expected {value_size} bytes, got {len(raw)}"
            )
        return raw

    def mark_readonly(self) -> None:
        """Flush, sync, and reopen the segment in immutable read-only mode."""
        self._ensure_open()
        if not self.writable:
            return
        self.sync()
        self._file.close()
        self.writable = False
        self._file = open(self.path, "rb")
        self._fd = self._file.fileno()
        self._cursor_at_end = False

    def sync(self) -> None:
        """Flush buffers and force an OS fsync to disk."""
        self._ensure_open()
        if self.writable:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._unflushed_records = 0

    def close(self) -> None:
        """Safely flush and close the underlying file handle."""
        if self._closed:
            return
        try:
            if self.writable:
                self._file.flush()
                os.fsync(self._file.fileno())
        finally:
            self._file.close()
            self._closed = True

    def scan_into_keydir(
        self,
        keydir: KeyDir,
        *,
        verify_crc: bool = True,
        skip_values: bool = False,
        recover_tail: bool = True,
        use_hint: bool = True,
    ) -> int:
        """Load this segment's records into `keydir`.

        If `use_hint` is True and a valid `.hint` file exists for this segment,
        loads directly from the hint file without scanning value payloads.
        Otherwise scans the segment file sequentially, supporting `skip_values`
        (seeking past values when `verify_crc=False`) and `recover_tail` (truncating
        incomplete tail writes caused by sudden crashes).

        Returns:
            Number of records processed.
        """
        hpath = hint_file_path(self.path)
        if use_hint and hpath.exists():
            try:
                # A hint file may only be trusted while it exactly spans the
                # current data file: records appended after the hint was written
                # (hard crash without clean close) leave the file LARGER than the
                # hint covers, and a torn-tail truncation leaves it SMALLER. In
                # both cases trusting the stale hint would silently lose records.
                if self._hint_covers_file(hpath):
                    return self._load_from_hint_file(hpath, keydir)
                self.logger.warning(
                    "Hint file %s is stale relative to segment size; "
                    "falling back to data scan",
                    hpath,
                )
            except CorruptDataError as exc:
                self.logger.warning(
                    "Hint file %s is corrupt (%s); falling back to segment scan",
                    hpath,
                    exc,
                )

        return self._scan_data_file(
            keydir,
            verify_crc=verify_crc,
            skip_values=skip_values,
            recover_tail=recover_tail,
        )

    def _hint_covers_file(self, hpath: Path) -> bool:
        """Return True if the hint's recorded spans exactly cover the data file.

        Parses hint headers only (positions/sizes live in the header), so the
        check is O(record_count) without touching key or value payloads.
        """
        file_size = self.path.stat().st_size
        raw = hpath.read_bytes()
        offset = 0
        max_end = 0
        while offset < len(raw):
            if offset + HINT_HEADER_SIZE > len(raw):
                raise CorruptDataError(f"Truncated hint header in {hpath}")
            header_bytes = raw[offset : offset + HINT_HEADER_SIZE]
            (
                _crc,
                _ts,
                _flags,
                _exp,
                key_size,
                _vpos,
                _vsz,
                record_pos,
                record_size,
            ) = decode_hint_header(header_bytes)
            key_end = offset + HINT_HEADER_SIZE + key_size
            if key_end > len(raw):
                raise CorruptDataError(f"Truncated hint key in {hpath}")
            max_end = max(max_end, record_pos + record_size)
            offset = key_end
        return max_end == file_size

    def _load_from_hint_file(self, hpath: Path, keydir: KeyDir) -> int:
        raw = hpath.read_bytes()
        total_len = len(raw)
        offset = 0
        count = 0
        while offset < total_len:
            if offset + HINT_HEADER_SIZE > total_len:
                raise CorruptDataError(f"Truncated hint header in {hpath}")
            header_bytes = raw[offset : offset + HINT_HEADER_SIZE]
            (
                _crc,
                _ts,
                _flags,
                _exp,
                key_size,
                _vpos,
                _vsz,
                _rpos,
                _rsz,
            ) = decode_hint_header(header_bytes)
            key_start = offset + HINT_HEADER_SIZE
            key_end = key_start + key_size
            if key_end > total_len:
                raise CorruptDataError(f"Truncated hint key in {hpath}")
            key_bytes = raw[key_start:key_end]
            hint: HintRecord = decode_hint_record(header_bytes, key_bytes)
            if hint.flags & FLAG_TOMBSTONE:
                keydir.delete(
                    hint.key,
                    timestamp=hint.timestamp,
                    tombstone_file_id=self.file_id,
                    tombstone_size=hint.record_size,
                    tombstone_pos=hint.record_pos,
                )
            else:
                entry = KeyEntry(
                    timestamp=hint.timestamp,
                    position=hint.record_pos,
                    total_size=hint.record_size,
                    file_id=self.file_id,
                    value_pos=hint.value_pos,
                    value_size=hint.value_size,
                    expire_at=hint.expire_at,
                )
                keydir.put(hint.key, entry)
            offset = key_end
            count += 1
        return count

    def _scan_data_file(
        self,
        keydir: KeyDir,
        *,
        verify_crc: bool,
        skip_values: bool,
        recover_tail: bool,
    ) -> int:
        self._ensure_open()
        file_size = self.path.stat().st_size
        offset = 0
        count = 0

        with open(self.path, "rb") as f:
            while offset < file_size:
                header_bytes = f.read(RECORD_HEADER_SIZE)
                if not header_bytes:
                    break
                if len(header_bytes) < RECORD_HEADER_SIZE:
                    if recover_tail:
                        self.logger.warning(
                            "Truncated record header at tail offset %d of %s; truncating",
                            offset,
                            self.path,
                        )
                        self._truncate_corrupt_tail(offset)
                        break
                    raise CorruptDataError(
                        f"Truncated record header at offset {offset} in {self.path}"
                    )

                crc, timestamp, flags, expire_at, key_size, value_size = (
                    decode_record_header(header_bytes)
                )
                total_size = RECORD_HEADER_SIZE + key_size + value_size
                if offset + total_size > file_size:
                    if recover_tail:
                        self.logger.warning(
                            "Incomplete record payload at tail offset %d of %s "
                            "(needed %d bytes, file ends at %d); truncating",
                            offset,
                            self.path,
                            total_size,
                            file_size,
                        )
                        self._truncate_corrupt_tail(offset)
                        break
                    raise CorruptDataError(
                        f"Truncated record at offset {offset} in {self.path}: "
                        f"expected {total_size} bytes, only {file_size - offset} available"
                    )

                key_bytes = f.read(key_size)
                if skip_values and not verify_crc:
                    # Fast startup optimization: seek past value bytes without reading them into RAM
                    f.seek(value_size, os.SEEK_CUR)
                    value_bytes = b""
                else:
                    value_bytes = f.read(value_size)

                if verify_crc:
                    expected_crc = compute_crc32(
                        header_bytes[4:], key_bytes, value_bytes
                    )
                    if crc != expected_crc:
                        is_last_record = (offset + total_size) == file_size
                        if recover_tail and is_last_record:
                            self.logger.warning(
                                "CRC mismatch on tail record at offset %d of %s; truncating",
                                offset,
                                self.path,
                            )
                            self._truncate_corrupt_tail(offset)
                            break
                        raise CorruptDataError(
                            f"CRC32 mismatch at offset {offset} in {self.path}: "
                            f"stored=0x{crc:08x}, computed=0x{expected_crc:08x}"
                        )

                try:
                    key = key_bytes.decode("utf-8")
                except UnicodeDecodeError as exc:
                    if recover_tail and (offset + total_size) == file_size:
                        self._truncate_corrupt_tail(offset)
                        break
                    raise CorruptDataError(
                        f"Invalid UTF-8 key at offset {offset} in {self.path}: {exc}"
                    ) from exc

                if flags & FLAG_TOMBSTONE:
                    keydir.delete(
                        key,
                        timestamp=timestamp,
                        tombstone_file_id=self.file_id,
                        tombstone_size=total_size,
                        tombstone_pos=offset,
                    )
                else:
                    entry = KeyEntry(
                        timestamp=timestamp,
                        position=offset,
                        total_size=total_size,
                        file_id=self.file_id,
                        value_pos=offset + RECORD_HEADER_SIZE + key_size,
                        value_size=value_size,
                        expire_at=expire_at,
                    )
                    keydir.put(key, entry)

                offset += total_size
                count += 1

        return count

    def _truncate_corrupt_tail(self, valid_offset: int) -> None:
        """Truncate the segment file to `valid_offset` to heal a torn tail write."""
        self._file.close()
        os.truncate(self.path, valid_offset)
        mode = "r+b" if self.writable else "rb"
        self._file = open(self.path, mode)
        self._fd = self._file.fileno()
        self._file.seek(0, os.SEEK_END)
        self.write_position = self._file.tell()
        self._cursor_at_end = True

    @staticmethod
    def write_hint_file(
        segment_path: Path,
        entries: Iterable[tuple[str, KeyEntry]],
        tombstone_entries: Iterable[tuple[str, KeyEntry]] = (),
    ) -> Path:
        """Write an atomic `.hint` file for all given `(key, KeyEntry)` pairs.

        `tombstone_entries` carries the segment's own tombstone records so the
        hint is a complete replay surrogate for the segment (deletes included).
        """
        hpath = hint_file_path(segment_path)
        tmp_path = hpath.with_suffix(".hint.tmp")
        with open(tmp_path, "wb") as hf:
            for key, entry in entries:
                flags = FLAG_NORMAL | (FLAG_HAS_TTL if entry.expire_at > 0 else 0)
                raw = encode_hint_record(
                    timestamp=entry.timestamp,
                    flags=flags,
                    expire_at=entry.expire_at,
                    value_pos=entry.value_pos,
                    value_size=entry.value_size,
                    record_pos=entry.position,
                    record_size=entry.total_size,
                    key=key,
                )
                hf.write(raw)
            for key, entry in tombstone_entries:
                raw = encode_hint_record(
                    timestamp=entry.timestamp,
                    flags=FLAG_TOMBSTONE,
                    expire_at=0,
                    value_pos=0,
                    value_size=0,
                    record_pos=entry.position,
                    record_size=entry.total_size,
                    key=key,
                )
                hf.write(raw)
            hf.flush()
            os.fsync(hf.fileno())
        os.replace(tmp_path, hpath)
        return hpath
