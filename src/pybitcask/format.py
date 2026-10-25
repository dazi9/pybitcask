"""Binary serialization and deserialization protocol for PyBitcask.

Provides two binary protocols:
1. Bitcask v2 Record & Hint format (default for BitcaskEngine):
   - Includes 4-byte IEEE CRC32 checksum, 8-byte millisecond timestamp,
     1-byte flags (NORMAL / TOMBSTONE / TTL), 8-byte expire_at timestamp,
     4-byte key_size, 4-byte value_size, UTF-8 key bytes, and value bytes.
   - Hint file records (.hint) store metadata + key without value bytes for
     fast cold-start index recovery.
2. CaskDB v1 Compatible Format (encode_header, decode_header, encode_kv, decode_kv):
   - Compatible with avinassh/py-caskdb's 12-byte `<LLL` header format,
     while fixing upstream bugs with multi-byte UTF-8 strings and trailing slices.
"""

from __future__ import annotations

import struct
import typing
import zlib
from dataclasses import dataclass

from pybitcask.exceptions import CorruptDataError, ValidationError

# ============================================================================
# 1. CaskDB v1 Compatible Format (12-byte header: <LLL)
# ============================================================================
HEADER_FORMAT: typing.Final[str] = "<LLL"
HEADER_SIZE: typing.Final[int] = 12
MAX_UINT32: typing.Final[int] = (1 << 32) - 1
MAX_UINT64: typing.Final[int] = (1 << 64) - 1


@dataclass(slots=True)
class KeyEntry:
    """Metadata stored in KeyDir pointing to a record's location on disk.

    Compatible with both CaskDB v1 (timestamp, position, total_size) and
    multi-segment Bitcask v2 (file_id, value_pos, value_size, expire_at).
    """

    timestamp: int
    position: int
    total_size: int
    file_id: int = 0
    value_pos: int = 0
    value_size: int = 0
    expire_at: int = 0

    def is_expired(self, now_ms: int) -> bool:
        """Return True if this entry has an active TTL that has passed."""
        return self.expire_at > 0 and now_ms >= self.expire_at


def encode_header(timestamp: int, key_size: int, value_size: int) -> bytes:
    """Encode a 12-byte CaskDB v1 header (<LLL: timestamp, key_size, value_size)."""
    return struct.pack(HEADER_FORMAT, timestamp, key_size, value_size)


def decode_header(data: bytes) -> tuple[int, int, int]:
    """Decode a 12-byte CaskDB v1 header into (timestamp, key_size, value_size)."""
    if len(data) != HEADER_SIZE:
        raise struct.error(f"Expected {HEADER_SIZE} header bytes, got {len(data)}")
    timestamp, key_size, value_size = struct.unpack(HEADER_FORMAT, data)
    return timestamp, key_size, value_size


def encode_kv(timestamp: int, key: str, value: str) -> tuple[int, bytes]:
    """Encode a (timestamp, key, value) tuple into CaskDB v1 binary format.

    Fixes the upstream avinassh/py-caskdb bug where `len(key)` (character count)
    was encoded into the header instead of `len(key.encode('utf-8'))` (byte count).
    """
    if not isinstance(key, str) or not isinstance(value, str):
        raise ValidationError("encode_kv requires key and value to be str")
    key_bytes = key.encode("utf-8")
    value_bytes = value.encode("utf-8")
    header = encode_header(timestamp, len(key_bytes), len(value_bytes))
    payload = header + key_bytes + value_bytes
    return len(payload), payload


def decode_kv(data: bytes) -> tuple[int, str, str]:
    """Decode CaskDB v1 binary bytes into (timestamp, key, value).

    Validates exact payload boundaries and UTF-8 integrity.
    """
    if len(data) < HEADER_SIZE:
        raise CorruptDataError(
            f"Record too short: expected at least {HEADER_SIZE} bytes, got {len(data)}"
        )
    timestamp, key_size, value_size = decode_header(data[:HEADER_SIZE])
    expected_total = HEADER_SIZE + key_size + value_size
    if len(data) < expected_total:
        raise CorruptDataError(
            f"Truncated KV record: expected {expected_total} bytes, got {len(data)}"
        )
    key_bytes = data[HEADER_SIZE : HEADER_SIZE + key_size]
    value_bytes = data[HEADER_SIZE + key_size : expected_total]
    try:
        key = key_bytes.decode("utf-8")
        value = value_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CorruptDataError(f"Invalid UTF-8 in KV record: {exc}") from exc
    return timestamp, key, value


# ============================================================================
# 2. Bitcask v2 Record Format with CRC32, Tombstone Flags, and TTL
# ============================================================================
# Layout:
#   crc32      (4B, uint32) - IEEE CRC32 of header[4:] + key + value
#   timestamp  (8B, uint64) - Unix epoch milliseconds
#   flags      (1B, uint8)  - Bitflags (0x01 = TOMBSTONE, 0x02 = HAS_TTL)
#   expire_at  (8B, uint64) - Expiration timestamp in ms (0 = no expiration)
#   key_size   (4B, uint32) - Byte length of key
#   value_size (4B, uint32) - Byte length of value
RECORD_BODY_HEADER_FORMAT: typing.Final[str] = "<QBQII"
RECORD_HEADER_FORMAT: typing.Final[str] = "<IQBQII"
RECORD_HEADER_SIZE: typing.Final[int] = struct.calcsize(RECORD_HEADER_FORMAT)  # 29 bytes

FLAG_NORMAL: typing.Final[int] = 0x00
FLAG_TOMBSTONE: typing.Final[int] = 0x01
FLAG_HAS_TTL: typing.Final[int] = 0x02


@dataclass(slots=True, frozen=True)
class DecodedRecord:
    """Represents a decoded Bitcask v2 data record."""

    crc32: int
    timestamp: int
    flags: int
    expire_at: int
    key: str
    value: bytes
    total_size: int

    @property
    def is_tombstone(self) -> bool:
        return bool(self.flags & FLAG_TOMBSTONE)

    @property
    def has_ttl(self) -> bool:
        return bool(self.flags & FLAG_HAS_TTL)

    def value_text(self) -> str:
        """Decode value bytes as a UTF-8 string."""
        try:
            return self.value.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise CorruptDataError(f"Failed to decode value as UTF-8: {exc}") from exc


def compute_crc32(header_without_crc: bytes, key_bytes: bytes, value_bytes: bytes) -> int:
    """Compute unsigned 32-bit IEEE CRC32 checksum across header fields, key, and value."""
    crc = zlib.crc32(header_without_crc)
    crc = zlib.crc32(key_bytes, crc)
    crc = zlib.crc32(value_bytes, crc)
    return crc & 0xFFFFFFFF


def encode_record(
    timestamp: int,
    key: str | bytes,
    value: str | bytes,
    *,
    is_tombstone: bool = False,
    expire_at: int = 0,
    max_key_size: int = MAX_UINT32,
    max_value_size: int = MAX_UINT32,
) -> tuple[int, bytes]:
    """Encode a Bitcask v2 record with CRC32 protection.

    Returns:
        Tuple of (total_byte_length, encoded_bytes).
    """
    if isinstance(key, str):
        key_bytes = key.encode("utf-8")
    elif isinstance(key, (bytes, bytearray)):
        key_bytes = bytes(key)
        try:
            key_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValidationError(f"Key bytes must be valid UTF-8: {exc}") from exc
    else:
        raise ValidationError(f"Key must be str or bytes, got {type(key).__name__}")

    if isinstance(value, str):
        value_bytes = value.encode("utf-8")
    elif isinstance(value, (bytes, bytearray)):
        value_bytes = bytes(value)
    else:
        raise ValidationError(f"Value must be str or bytes, got {type(value).__name__}")

    key_size = len(key_bytes)
    value_size = len(value_bytes)

    if key_size == 0:
        raise ValidationError("Key cannot be empty")
    if key_size > max_key_size:
        raise ValidationError(
            f"Key byte length {key_size} exceeds maximum {max_key_size}"
        )
    if value_size > max_value_size:
        raise ValidationError(
            f"Value byte length {value_size} exceeds maximum {max_value_size}"
        )
    if not (0 <= timestamp <= MAX_UINT64):
        raise ValidationError(f"Timestamp out of uint64 range: {timestamp}")
    if not (0 <= expire_at <= MAX_UINT64):
        raise ValidationError(f"expire_at out of uint64 range: {expire_at}")

    flags = FLAG_TOMBSTONE if is_tombstone else FLAG_NORMAL
    if expire_at > 0:
        flags |= FLAG_HAS_TTL

    header_body = struct.pack(
        RECORD_BODY_HEADER_FORMAT,
        timestamp,
        flags,
        expire_at,
        key_size,
        value_size,
    )
    crc = compute_crc32(header_body, key_bytes, value_bytes)
    crc_bytes = struct.pack("<I", crc)
    payload = crc_bytes + header_body + key_bytes + value_bytes
    return len(payload), payload


def decode_record_header(header_bytes: bytes) -> tuple[int, int, int, int, int, int]:
    """Decode a 29-byte Bitcask v2 record header.

    Returns:
        Tuple of (crc32, timestamp, flags, expire_at, key_size, value_size).
    """
    if len(header_bytes) != RECORD_HEADER_SIZE:
        raise CorruptDataError(
            f"Invalid record header length: expected {RECORD_HEADER_SIZE}, got {len(header_bytes)}"
        )
    crc, timestamp, flags, expire_at, key_size, value_size = struct.unpack(
        RECORD_HEADER_FORMAT, header_bytes
    )
    return crc, timestamp, flags, expire_at, key_size, value_size


def decode_record(data: bytes, *, verify_crc: bool = True) -> DecodedRecord:
    """Decode a full Bitcask v2 record from bytes and optionally verify its CRC32."""
    if len(data) < RECORD_HEADER_SIZE:
        raise CorruptDataError(
            f"Data shorter than record header ({len(data)} < {RECORD_HEADER_SIZE})"
        )
    crc, timestamp, flags, expire_at, key_size, value_size = decode_record_header(
        data[:RECORD_HEADER_SIZE]
    )
    total_size = RECORD_HEADER_SIZE + key_size + value_size
    if len(data) < total_size:
        raise CorruptDataError(
            f"Truncated record payload: expected {total_size} bytes, got {len(data)}"
        )
    key_bytes = data[RECORD_HEADER_SIZE : RECORD_HEADER_SIZE + key_size]
    value_bytes = data[RECORD_HEADER_SIZE + key_size : total_size]

    if verify_crc:
        expected_crc = compute_crc32(
            data[4:RECORD_HEADER_SIZE], key_bytes, value_bytes
        )
        if crc != expected_crc:
            raise CorruptDataError(
                f"CRC32 mismatch: stored=0x{crc:08x}, computed=0x{expected_crc:08x}"
            )

    try:
        key = key_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CorruptDataError(f"Corrupt UTF-8 key bytes: {exc}") from exc

    return DecodedRecord(
        crc32=crc,
        timestamp=timestamp,
        flags=flags,
        expire_at=expire_at,
        key=key,
        value=value_bytes,
        total_size=total_size,
    )


# ============================================================================
# 3. Hint File Binary Format (.hint)
# ============================================================================
# Layout:
#   crc32       (4B, uint32)
#   timestamp   (8B, uint64)
#   flags       (1B, uint8)
#   expire_at   (8B, uint64)
#   key_size    (4B, uint32)
#   value_pos   (8B, uint64)
#   value_size  (4B, uint32)
#   record_pos  (8B, uint64)
#   record_size (4B, uint32)
#   key         (key_size B)
HINT_BODY_FORMAT: typing.Final[str] = "<QBQIQIQI"
HINT_HEADER_FORMAT: typing.Final[str] = "<IQBQIQIQI"
HINT_HEADER_SIZE: typing.Final[int] = struct.calcsize(HINT_HEADER_FORMAT)  # 49 bytes


@dataclass(slots=True, frozen=True)
class HintRecord:
    """Represents a decoded entry in a .hint index file."""

    timestamp: int
    flags: int
    expire_at: int
    value_pos: int
    value_size: int
    record_pos: int
    record_size: int
    key: str


def encode_hint_record(
    timestamp: int,
    flags: int,
    expire_at: int,
    value_pos: int,
    value_size: int,
    record_pos: int,
    record_size: int,
    key: str,
) -> bytes:
    """Encode a HintRecord into binary bytes with CRC32 protection."""
    key_bytes = key.encode("utf-8")
    body = struct.pack(
        HINT_BODY_FORMAT,
        timestamp,
        flags,
        expire_at,
        len(key_bytes),
        value_pos,
        value_size,
        record_pos,
        record_size,
    )
    crc = zlib.crc32(key_bytes, zlib.crc32(body)) & 0xFFFFFFFF
    return struct.pack("<I", crc) + body + key_bytes


def decode_hint_header(header_bytes: bytes) -> tuple[int, int, int, int, int, int, int, int, int]:
    """Decode the 49-byte fixed header of a HintRecord."""
    if len(header_bytes) != HINT_HEADER_SIZE:
        raise CorruptDataError(
            f"Invalid hint header size: expected {HINT_HEADER_SIZE}, got {len(header_bytes)}"
        )
    return struct.unpack(HINT_HEADER_FORMAT, header_bytes)


def decode_hint_record(header_bytes: bytes, key_bytes: bytes) -> HintRecord:
    """Verify CRC32 and decode a HintRecord from its header and key bytes."""
    (
        crc,
        timestamp,
        flags,
        expire_at,
        key_size,
        value_pos,
        value_size,
        record_pos,
        record_size,
    ) = decode_hint_header(header_bytes)
    if len(key_bytes) != key_size:
        raise CorruptDataError(
            f"Hint key truncated: expected {key_size} bytes, got {len(key_bytes)}"
        )
    expected_crc = zlib.crc32(key_bytes, zlib.crc32(header_bytes[4:])) & 0xFFFFFFFF
    if crc != expected_crc:
        raise CorruptDataError(
            f"Hint record CRC32 mismatch: stored=0x{crc:08x}, computed=0x{expected_crc:08x}"
        )
    try:
        key = key_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CorruptDataError(f"Corrupt UTF-8 key in hint file: {exc}") from exc
    return HintRecord(
        timestamp=timestamp,
        flags=flags,
        expire_at=expire_at,
        value_pos=value_pos,
        value_size=value_size,
        record_pos=record_pos,
        record_size=record_size,
        key=key,
    )
