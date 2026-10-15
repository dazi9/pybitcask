"""Systematic boundary, error-state, idempotency, and security tests for PyBitcask."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from pybitcask.config import BitcaskConfig
from pybitcask.engine import BitcaskEngine
from pybitcask.exceptions import (
    ConfigurationError,
    CorruptDataError,
    DatabaseClosedError,
    DatabaseLockedError,
    KeyNotFoundError,
    ValidationError,
)
from pybitcask.segment import segment_file_path


class TestBoundaryAndErrorConditions(unittest.TestCase):
    """Covers empty inputs, duplicate operations, closed-state errors, mid-file corruption, and limits."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_dir = Path(self._tmpdir.name) / "edge_db"

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_empty_database_operations(self) -> None:
        with BitcaskEngine(db_path=self.db_dir) as db:
            self.assertEqual(len(db), 0)
            self.assertEqual(db.keys(), [])
            self.assertEqual(db.scan_prefix("any"), [])
            self.assertEqual(db.scan_range("a", "z"), [])
            self.assertEqual(db.get("nonexistent"), "")
            self.assertIsNone(db.get_bytes("nonexistent"))
            self.assertFalse(db.delete("nonexistent"))
            self.assertEqual(db.batch_write([]), 0)
            # Compacting an empty DB is a safe no-op
            res = db.compact()
            self.assertEqual(res["reclaimed_bytes"], 0)
            self.assertEqual(res["live_keys_migrated"], 0)

    def test_invalid_inputs_and_size_boundaries(self) -> None:
        cfg = BitcaskConfig(
            db_path=self.db_dir,
            max_key_size=16,
            max_value_size=32,
        )
        with BitcaskEngine(config=cfg) as db:
            # Empty key must fail
            with self.assertRaises(ValidationError):
                db.set("", "val")
            with self.assertRaises(ValidationError):
                db.delete("")

            # Non-string key or invalid value type must fail
            with self.assertRaises(ValidationError):
                db.set(123, "val")  # type: ignore[arg-type]
            with self.assertRaises(ValidationError):
                db.set("k", 12345)  # type: ignore[arg-type]

            # Negative or zero TTL must fail
            with self.assertRaises(ValidationError):
                db.set("k", "v", ttl_ms=0)
            with self.assertRaises(ValidationError):
                db.set("k", "v", ttl_ms=-500)

            # Exact boundary key (16 bytes) and value (32 bytes) succeed
            db.set("k" * 16, "v" * 32)
            self.assertEqual(db.get("k" * 16), "v" * 32)

            # Exceeding max_key_size by 1 byte fails
            with self.assertRaises(ValidationError):
                db.set("k" * 17, "v")

            # Exceeding max_value_size by 1 byte fails
            with self.assertRaises(ValidationError):
                db.set("k", "v" * 33)

            # Empty value (0 bytes) is allowed and distinct from missing key via exists()
            db.set("empty_val_key", "")
            self.assertTrue(db.exists("empty_val_key"))
            self.assertEqual(db.get("empty_val_key"), "")
            self.assertEqual(db.get_bytes("empty_val_key"), b"")

    def test_idempotent_and_repeated_operations(self) -> None:
        with BitcaskEngine(db_path=self.db_dir) as db:
            # Repeated overwrites of the same key
            for i in range(50):
                db.set("counter", str(i))
            self.assertEqual(db.get("counter"), "49")
            self.assertEqual(len(db), 1)

            # Repeated deletes of the same key
            self.assertTrue(db.delete("counter"))
            self.assertFalse(db.delete("counter"))
            self.assertFalse(db.delete("counter"))
            with self.assertRaises(KeyNotFoundError):
                del db["counter"]

            # Recreating a deleted key
            db.set("counter", "reborn")
            self.assertTrue(db.exists("counter"))
            self.assertEqual(db.get("counter"), "reborn")

            # Repeated compactions
            db.compact()
            res2 = db.compact()
            self.assertEqual(res2["reclaimed_bytes"], 0)
            self.assertEqual(db.get("counter"), "reborn")

    def test_operations_on_closed_database_raise_error(self) -> None:
        db = BitcaskEngine(db_path=self.db_dir)
        db.set("k", "v")
        db.close()
        # Closing twice is idempotent
        db.close()

        with self.assertRaises(DatabaseClosedError):
            db.get("k")
        with self.assertRaises(DatabaseClosedError):
            db.set("k2", "v2")
        with self.assertRaises(DatabaseClosedError):
            db.delete("k")
        with self.assertRaises(DatabaseClosedError):
            db.compact()
        with self.assertRaises(DatabaseClosedError):
            db.stats()

    def test_mid_file_bit_rot_detected_by_crc32(self) -> None:
        with BitcaskEngine(db_path=self.db_dir, use_hint_files=False) as db:
            db.set("k1", "first_record_value")
            db.set("k2", "second_record_value")

        # Flip a bit inside the first record's value on disk
        seg_path = segment_file_path(self.db_dir, 1)
        raw = bytearray(seg_path.read_bytes())
        raw[35] ^= 0xFF
        seg_path.write_bytes(bytes(raw))

        # Startup with verify_checksum=True must detect mid-file corruption even when recover_tail=True
        with self.assertRaises(CorruptDataError):
            BitcaskEngine(
                db_path=self.db_dir,
                verify_checksum=True,
                recover_tail=True,
                use_hint_files=False,
            )

    def test_stale_lock_file_from_dead_pid_is_recovered(self) -> None:
        self.db_dir.mkdir(parents=True, exist_ok=True)
        lock_file = self.db_dir / "bitcask.lock"
        # Write a non-existent high PID
        lock_file.write_text("99999999", encoding="utf-8")

        with BitcaskEngine(db_path=self.db_dir) as db:
            db.set("recovered", "true")
            self.assertEqual(db.get("recovered"), "true")

    def test_null_byte_in_config_path_rejected(self) -> None:
        with self.assertRaises(ConfigurationError):
            BitcaskConfig(db_path="bad\x00path")

    def test_dead_bytes_detected_and_compacted_after_hint_restart(self) -> None:
        with BitcaskEngine(db_path=self.db_dir, use_hint_files=True) as db:
            for v in range(5):
                db.set("k1", f"value_version_{v}_" + ("x" * 100))

        # Reopen via .hint file; dead bytes from the 4 overwritten versions must still be detected
        with BitcaskEngine(db_path=self.db_dir, use_hint_files=True) as db2:
            stats = db2.stats()
            self.assertGreater(stats["dead_bytes"], 400)
            res = db2.compact()
            self.assertGreater(res["reclaimed_bytes"], 400)
            self.assertEqual(db2.get("k1"), "value_version_4_" + ("x" * 100))


if __name__ == "__main__":
    unittest.main()
