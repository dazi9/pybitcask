"""Unit and boundary tests for pybitcask.format and pybitcask.config."""

from __future__ import annotations

import os
import random
import struct
import tempfile
import time
import unittest
import uuid

from pybitcask.config import BitcaskConfig
from pybitcask.exceptions import ConfigurationError, CorruptDataError, ValidationError
from pybitcask.format import (
    FLAG_HAS_TTL,
    FLAG_NORMAL,
    FLAG_TOMBSTONE,
    HEADER_SIZE,
    HINT_HEADER_SIZE,
    RECORD_HEADER_SIZE,
    KeyEntry,
    decode_header,
    decode_hint_record,
    decode_kv,
    decode_record,
    decode_record_header,
    encode_header,
    encode_hint_record,
    encode_kv,
    encode_record,
)


class TestCaskDBV1Compatibility(unittest.TestCase):
    """Tests for CaskDB v1 compatible functions and upstream bug fixes."""

    def test_header_roundtrip(self) -> None:
        cases = [
            (0, 0, 0),
            (10, 10, 10),
            (1700000000, 1024, 65536),
            ((1 << 32) - 1, (1 << 32) - 1, (1 << 32) - 1),
        ]
        for ts, ksz, vsz in cases:
            raw = encode_header(ts, ksz, vsz)
            self.assertEqual(len(raw), HEADER_SIZE)
            self.assertEqual(decode_header(raw), (ts, ksz, vsz))

    def test_header_overflow_and_short_bytes(self) -> None:
        with self.assertRaises(struct.error):
            encode_header(1 << 32, 5, 5)
        with self.assertRaises(struct.error):
            decode_header(b"short")

    def test_kv_ascii_and_random_roundtrip(self) -> None:
        for _ in range(50):
            ts = int(time.time())
            k = str(uuid.uuid4())
            v = str(uuid.uuid4())
            sz, data = encode_kv(ts, k, v)
            self.assertEqual(sz, len(data))
            self.assertEqual(decode_kv(data), (ts, k, v))

    def test_kv_multibyte_utf8_bugfix(self) -> None:
        """Verify fix for upstream avinassh/py-caskdb multi-byte UTF-8 bug."""
        cases = [
            ("姓名", "张三"),
            ("emoji_key_🚀🔥", "值包含中文与Emoji 🎉✨"),
            ("αβγδε", "АБВГД"),
            ("", ""),
        ]
        for k, v in cases:
            sz, data = encode_kv(1700000000, k, v)
            expected_sz = HEADER_SIZE + len(k.encode("utf-8")) + len(v.encode("utf-8"))
            self.assertEqual(sz, expected_sz)
            self.assertEqual(len(data), expected_sz)
            ts, dk, dv = decode_kv(data)
            self.assertEqual((ts, dk, dv), (1700000000, k, v))

    def test_kv_ignores_trailing_buffer_bytes(self) -> None:
        """Verify fix for upstream decode_kv slicing past value_size."""
        _, data = encode_kv(12345, "hello", "world")
        padded = data + b"EXTRA_TRAILING_BYTES"
        ts, k, v = decode_kv(padded)
        self.assertEqual((ts, k, v), (12345, "hello", "world"))

    def test_kv_corrupt_or_truncated(self) -> None:
        _, data = encode_kv(12345, "hello", "world")
        with self.assertRaises(CorruptDataError):
            decode_kv(data[:5])
        with self.assertRaises(CorruptDataError):
            decode_kv(data[:-2])
        # Corrupt UTF-8 bytes in key
        bad = encode_header(12345, 2, 0) + b"\xff\xfe"
        with self.assertRaises(CorruptDataError):
            decode_kv(bad)


class TestBitcaskV2RecordFormat(unittest.TestCase):
    """Tests for Bitcask v2 binary record encoding, CRC32 verification, flags, and hints."""

    def test_record_roundtrip_normal_and_utf8(self) -> None:
        sz, raw = encode_record(
            timestamp=1700000000123,
            key="user:1001:档案",
            value='{"name": "李四", "role": "student"}',
        )
        self.assertEqual(sz, len(raw))
        rec = decode_record(raw, verify_crc=True)
        self.assertEqual(rec.timestamp, 1700000000123)
        self.assertEqual(rec.key, "user:1001:档案")
        self.assertEqual(rec.value_text(), '{"name": "李四", "role": "student"}')
        self.assertFalse(rec.is_tombstone)
        self.assertFalse(rec.has_ttl)
        self.assertEqual(rec.expire_at, 0)

    def test_record_tombstone_and_ttl_flags(self) -> None:
        _, raw_tomb = encode_record(1000, "del_key", b"", is_tombstone=True)
        rec_tomb = decode_record(raw_tomb)
        self.assertTrue(rec_tomb.is_tombstone)
        self.assertEqual(rec_tomb.flags & FLAG_TOMBSTONE, FLAG_TOMBSTONE)

        _, raw_ttl = encode_record(1000, "ttl_key", "val", expire_at=5000)
        rec_ttl = decode_record(raw_ttl)
        self.assertTrue(rec_ttl.has_ttl)
        self.assertEqual(rec_ttl.flags & FLAG_HAS_TTL, FLAG_HAS_TTL)
        self.assertEqual(rec_ttl.expire_at, 5000)

    def test_crc32_detects_single_bit_flip(self) -> None:
        _, raw = encode_record(1700000000000, "account:balance", "9999.95")
        for byte_idx in [0, 5, RECORD_HEADER_SIZE, len(raw) - 1]:
            mutated = bytearray(raw)
            mutated[byte_idx] ^= 0x01
            with self.assertRaises(CorruptDataError):
                decode_record(bytes(mutated), verify_crc=True)

    def test_validation_errors(self) -> None:
        with self.assertRaises(ValidationError):
            encode_record(100, "", "val")
        with self.assertRaises(ValidationError):
            encode_record(100, "k" * 20, "val", max_key_size=10)
        with self.assertRaises(ValidationError):
            encode_record(100, "k", "v" * 20, max_value_size=10)
        with self.assertRaises(ValidationError):
            encode_record(-1, "k", "v")
        with self.assertRaises(ValidationError):
            encode_record(100, b"\xff\xfe", "v")

    def test_hint_record_roundtrip_and_crc(self) -> None:
        raw_hint = encode_hint_record(
            timestamp=1700000000999,
            flags=FLAG_NORMAL | FLAG_HAS_TTL,
            expire_at=1700000099999,
            value_pos=128,
            value_size=64,
            record_pos=80,
            record_size=112,
            key="course:cs201:数据结构",
        )
        header_bytes = raw_hint[:HINT_HEADER_SIZE]
        key_bytes = raw_hint[HINT_HEADER_SIZE:]
        hint = decode_hint_record(header_bytes, key_bytes)
        self.assertEqual(hint.timestamp, 1700000000999)
        self.assertEqual(hint.expire_at, 1700000099999)
        self.assertEqual(hint.value_pos, 128)
        self.assertEqual(hint.value_size, 64)
        self.assertEqual(hint.record_pos, 80)
        self.assertEqual(hint.record_size, 112)
        self.assertEqual(hint.key, "course:cs201:数据结构")

        # Flip one bit in key_bytes and verify CRC failure
        corrupt_key = bytearray(key_bytes)
        corrupt_key[0] ^= 0x04
        with self.assertRaises(CorruptDataError):
            decode_hint_record(header_bytes, bytes(corrupt_key))

    def test_key_entry_expiration(self) -> None:
        entry_no_ttl = KeyEntry(timestamp=100, position=0, total_size=50, expire_at=0)
        self.assertFalse(entry_no_ttl.is_expired(999999999))

        entry_ttl = KeyEntry(timestamp=100, position=0, total_size=50, expire_at=5000)
        self.assertFalse(entry_ttl.is_expired(4999))
        self.assertTrue(entry_ttl.is_expired(5000))
        self.assertTrue(entry_ttl.is_expired(6000))


class TestBitcaskConfig(unittest.TestCase):
    """Tests for configuration validation, JSON serialization, and env loading."""

    def test_default_and_invalid_config(self) -> None:
        cfg = BitcaskConfig()
        self.assertEqual(cfg.sync_policy, "os")

        with self.assertRaises(ConfigurationError):
            BitcaskConfig(db_path="")
        with self.assertRaises(ConfigurationError):
            BitcaskConfig(max_segment_size=64)
        with self.assertRaises(ConfigurationError):
            BitcaskConfig(sync_policy="invalid_mode")
        with self.assertRaises(ConfigurationError):
            BitcaskConfig(compaction_threshold=1.5)
        with self.assertRaises(ConfigurationError):
            BitcaskConfig(cache_capacity=-1)

    def test_json_and_env_loading(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = os.path.join(tmpdir, "config.json")
            cfg = BitcaskConfig(
                db_path=tmpdir,
                max_segment_size=2048,
                sync_policy="always",
                cache_capacity=256,
            )
            cfg.save_json(cfg_path)
            loaded = BitcaskConfig.from_json(cfg_path)
            self.assertEqual(loaded.max_segment_size, 2048)
            self.assertEqual(loaded.sync_policy, "always")
            self.assertEqual(loaded.cache_capacity, 256)

        os.environ["PYBITCASK_MAX_SEGMENT_SIZE"] = "4096"
        os.environ["PYBITCASK_VERIFY_CHECKSUM"] = "false"
        try:
            env_cfg = BitcaskConfig.from_env()
            self.assertEqual(env_cfg.max_segment_size, 4096)
            self.assertFalse(env_cfg.verify_checksum)
        finally:
            os.environ.pop("PYBITCASK_MAX_SEGMENT_SIZE", None)
            os.environ.pop("PYBITCASK_VERIFY_CHECKSUM", None)


if __name__ == "__main__":
    unittest.main()
