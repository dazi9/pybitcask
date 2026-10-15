"""Comprehensive unit and integration tests for BitcaskEngine, DiskStorage, and MemoryStorage."""

from __future__ import annotations

import os
import tempfile
import threading
import time
import unittest
from pathlib import Path

from pybitcask.config import BitcaskConfig
from pybitcask.engine import BatchOperation, BitcaskEngine, DiskStorage
from pybitcask.exceptions import (
    DatabaseClosedError,
    DatabaseLockedError,
    KeyNotFoundError,
    ValidationError,
)
from pybitcask.memory_store import MemoryStorage


class TestDiskStorageCaskDBCompatibility(unittest.TestCase):
    """Verifies 100% compatibility with avinassh/py-caskdb test suite + tombstone delete."""

    def setUp(self) -> None:
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

    def tearDown(self) -> None:
        for p in [self.path, f"{self.path}.lock", str(Path(self.path).with_suffix(".hint"))]:
            if os.path.exists(p):
                os.remove(p)

    def test_get_set_and_missing(self) -> None:
        with DiskStorage(file_name=self.path) as store:
            store.set("name", "jojo")
            self.assertEqual(store.get("name"), "jojo")
            self.assertEqual(store.get("some key"), "")

    def test_dict_api(self) -> None:
        with DiskStorage(file_name=self.path) as store:
            store["name"] = "jojo"
            self.assertEqual(store["name"], "jojo")
            self.assertIn("name", store)
            self.assertEqual(len(store), 1)
            del store["name"]
            self.assertNotIn("name", store)
            with self.assertRaises(KeyNotFoundError):
                _ = store["name"]

    def test_persistence_and_tombstone_deletion(self) -> None:
        books = {
            "crime and punishment": "dostoevsky",
            "anna karenina": "tolstoy",
            "war and peace": "tolstoy",
            "hamlet": "shakespeare",
            "othello": "shakespeare",
            "brave new world": "huxley",
            "dune": "frank herbert",
            "平凡的世界": "路遥",
        }
        store = DiskStorage(file_name=self.path)
        for k, v in books.items():
            store.set(k, v)
            self.assertEqual(store.get(k), v)
        store.delete("hamlet")
        store.delete("dune")
        store.close()

        # Reopen from disk and verify live keys and deleted tombstones
        store2 = DiskStorage(file_name=self.path)
        try:
            self.assertEqual(store2.get("hamlet"), "")
            self.assertFalse(store2.exists("hamlet"))
            self.assertFalse(store2.exists("dune"))
            for k, v in books.items():
                if k not in ("hamlet", "dune"):
                    self.assertEqual(store2.get(k), v)
        finally:
            store2.close()


class TestMultiSegmentCompactionAndFeatures(unittest.TestCase):
    """Tests for multi-segment rotation, compaction, batch writes, TTL, JSON, and concurrency."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_dir = Path(self._tmpdir.name) / "bitcask_cluster"

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_segment_rotation_and_compaction_reclaims_space(self) -> None:
        cfg = BitcaskConfig(
            db_path=self.db_dir,
            max_segment_size=512,  # Small segment size to force frequent rotations
            sync_policy="os",
            use_hint_files=True,
        )
        with BitcaskEngine(config=cfg) as db:
            # Write 20 keys and overwrite them 5 times each -> 80% stale records
            for version in range(5):
                for i in range(20):
                    db.set(f"key_{i:03d}", f"value_version_{version}_payload_{i:03d}")
            # Delete 5 keys
            for i in range(5):
                self.assertTrue(db.delete(f"key_{i:03d}"))
            self.assertFalse(db.delete("non_existent_key"))

            pre_stats = db.stats()
            self.assertGreater(pre_stats["segment_count"], 3)
            self.assertGreater(pre_stats["dead_bytes"], 0)

            compaction_result = db.compact()
            self.assertEqual(compaction_result["live_keys_migrated"], 15)
            self.assertGreater(compaction_result["reclaimed_bytes"], 0)
            self.assertLess(
                compaction_result["bytes_after"],
                compaction_result["bytes_before"] // 2,
            )

            # Verify all 15 surviving keys have their latest version (version 4)
            for i in range(5):
                self.assertFalse(db.exists(f"key_{i:03d}"))
            for i in range(5, 20):
                self.assertEqual(
                    db.get(f"key_{i:03d}"),
                    f"value_version_4_payload_{i:03d}",
                )

        # Reopen after compaction and verify fast recovery via .hint files
        with BitcaskEngine(config=cfg) as db_reopened:
            self.assertEqual(len(db_reopened), 15)
            for i in range(5, 20):
                self.assertEqual(
                    db_reopened.get(f"key_{i:03d}"),
                    f"value_version_4_payload_{i:03d}",
                )

    def test_single_file_compaction(self) -> None:
        single_path = Path(self._tmpdir.name) / "single_compact.db"
        with BitcaskEngine(file_name=single_path) as db:
            for round_idx in range(10):
                for i in range(10):
                    db.set(f"k{i}", f"val_{round_idx}_" + ("x" * 50))
            res = db.compact()
            self.assertEqual(res["live_keys_migrated"], 10)
            self.assertGreater(res["reclaimed_bytes"], 0)
            for i in range(10):
                self.assertEqual(db.get(f"k{i}"), "val_9_" + ("x" * 50))

    def test_batch_write_and_atomic_validation(self) -> None:
        with BitcaskEngine(db_path=self.db_dir) as db:
            count = db.batch_write(
                [
                    BatchOperation("put", "u:1", "Alice"),
                    BatchOperation("put", "u:2", "Bob"),
                    BatchOperation("put", "u:3", "Charlie"),
                    BatchOperation("delete", "u:2"),
                ]
            )
            self.assertEqual(count, 4)
            self.assertEqual(db.get("u:1"), "Alice")
            self.assertFalse(db.exists("u:2"))
            self.assertEqual(db.get("u:3"), "Charlie")

            # A batch containing an invalid operation must not partially apply earlier items
            with self.assertRaises(ValidationError):
                db.batch_write(
                    [
                        BatchOperation("put", "u:4", "David"),
                        BatchOperation("put", "", "InvalidEmptyKey"),
                    ]
                )
            self.assertFalse(db.exists("u:4"))

    def test_ttl_expiration_and_json_api(self) -> None:
        with BitcaskEngine(db_path=self.db_dir) as db:
            db.set("ephemeral", "temp_token", ttl_ms=60)
            self.assertEqual(db.get("ephemeral"), "temp_token")
            time.sleep(0.08)
            self.assertEqual(db.get("ephemeral"), "")
            self.assertFalse(db.exists("ephemeral"))

            doc = {"course": "Data Structures", "credits": 4, "tags": ["B-Tree", "LSM", "Bitcask"]}
            db.set_json("course:cs201", doc)
            self.assertEqual(db.get_json("course:cs201"), doc)
            self.assertIsNone(db.get_json("course:missing"))

    def test_prefix_and_range_scan_integration(self) -> None:
        with BitcaskEngine(db_path=self.db_dir) as db:
            db.set("dept:cs:101", "Intro to CS")
            db.set("dept:cs:201", "Data Structures")
            db.set("dept:cs:301", "Operating Systems")
            db.set("dept:math:101", "Linear Algebra")

            cs_courses = db.scan_prefix("dept:cs:")
            self.assertEqual(
                cs_courses,
                [
                    ("dept:cs:101", "Intro to CS"),
                    ("dept:cs:201", "Data Structures"),
                    ("dept:cs:301", "Operating Systems"),
                ],
            )
            mid_range = db.scan_range("dept:cs:201", "dept:math:101", include_end=False)
            self.assertEqual(
                mid_range,
                [
                    ("dept:cs:201", "Data Structures"),
                    ("dept:cs:301", "Operating Systems"),
                ],
            )

    def test_concurrent_multithreaded_access_and_locking(self) -> None:
        with BitcaskEngine(db_path=self.db_dir, max_segment_size=2048) as db:
            # Attempting to open a second instance on the same path must raise DatabaseLockedError
            with self.assertRaises(DatabaseLockedError):
                BitcaskEngine(db_path=self.db_dir)

            errors: list[Exception] = []

            def worker(tid: int) -> None:
                try:
                    for i in range(40):
                        k = f"thread_{tid}_key_{i}"
                        v = f"val_{tid}_{i}"
                        db.set(k, v)
                        if db.get(k) != v:
                            raise AssertionError(f"Mismatch for {k}")
                except Exception as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(t,)) for t in range(4)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            self.assertEqual(errors, [])
            self.assertEqual(len(db), 160)


class TestMemoryStorage(unittest.TestCase):
    """Tests for MemoryStorage compatibility and TTL support."""

    def test_memory_crud_and_ttl(self) -> None:
        mem = MemoryStorage()
        mem.set("k1", "v1")
        mem["k2"] = "v2"
        self.assertEqual(mem.get("k1"), "v1")
        self.assertEqual(mem["k2"], "v2")
        self.assertEqual(mem.keys(), ["k1", "k2"])
        del mem["k1"]
        self.assertNotIn("k1", mem)

        mem.set("temp", "123", ttl_ms=40)
        self.assertEqual(mem.get("temp"), "123")
        time.sleep(0.06)
        self.assertEqual(mem.get("temp"), "")

        mem.close()
        with self.assertRaises(DatabaseClosedError):
            mem.get("k2")


if __name__ == "__main__":
    unittest.main()
