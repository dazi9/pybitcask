"""Unit tests for KeyDir index and LRUCache."""

from __future__ import annotations

import unittest

from pybitcask.cache import LRUCache
from pybitcask.format import KeyEntry
from pybitcask.keydir import KeyDir


class TestKeyDir(unittest.TestCase):
    """Tests for KeyDir CRUD, dead-byte accounting, TTL expiration, and scans."""

    def test_put_get_delete_and_dead_bytes(self) -> None:
        kd = KeyDir()
        e1 = KeyEntry(timestamp=100, position=0, total_size=40, file_id=1)
        e2 = KeyEntry(timestamp=200, position=40, total_size=60, file_id=1)

        self.assertIsNone(kd.put("k1", e1))
        self.assertEqual(kd.get("k1"), e1)
        self.assertEqual(kd.dead_bytes(), 0)
        self.assertEqual(kd.live_bytes(), 40)

        # Overwriting k1 should add e1.total_size (40) to dead bytes for file 1
        old = kd.put("k1", e2)
        self.assertEqual(old, e1)
        self.assertEqual(kd.dead_bytes(), 40)
        self.assertEqual(kd.live_bytes(), 60)

        # Deleting k1 with a 32-byte tombstone in file 2 should account for both
        kd.delete("k1", tombstone_file_id=2, tombstone_size=32)
        self.assertIsNone(kd.get("k1"))
        self.assertEqual(kd.dead_bytes({1}), 100)  # 40 + 60
        self.assertEqual(kd.dead_bytes({2}), 32)
        self.assertEqual(kd.dead_bytes(), 132)
        self.assertEqual(kd.live_bytes(), 0)

        kd.clear_dead_bytes_for_files({1})
        self.assertEqual(kd.dead_bytes(), 32)

    def test_lazy_and_active_ttl_expiration(self) -> None:
        kd = KeyDir()
        kd.put("short_ttl", KeyEntry(timestamp=100, position=0, total_size=50, file_id=1, expire_at=1000))
        kd.put("long_ttl", KeyEntry(timestamp=100, position=50, total_size=70, file_id=1, expire_at=5000))
        kd.put("forever", KeyEntry(timestamp=100, position=120, total_size=80, file_id=1, expire_at=0))

        # At t=999, short_ttl is still alive
        self.assertIsNotNone(kd.get("short_ttl", now_ms=999))
        # At t=1000, short_ttl expires lazily on get() and adds 50 dead bytes
        self.assertIsNone(kd.get("short_ttl", now_ms=1000))
        self.assertEqual(kd.dead_bytes(), 50)

        # Active purge at t=6000 removes long_ttl
        purged = kd.purge_expired(now_ms=6000)
        self.assertEqual(purged, 1)
        self.assertEqual(kd.keys(now_ms=6000), ["forever"])
        self.assertEqual(kd.dead_bytes(), 120)

    def test_prefix_and_range_scans(self) -> None:
        kd = KeyDir()
        for idx, k in enumerate(["user:10", "user:01", "user:02", "order:01", "admin:01"]):
            kd.put(k, KeyEntry(timestamp=100 + idx, position=idx * 20, total_size=20))

        user_keys = [k for k, _ in kd.scan_prefix("user:")]
        self.assertEqual(user_keys, ["user:01", "user:02", "user:10"])

        user_limited = [k for k, _ in kd.scan_prefix("user:", limit=2)]
        self.assertEqual(user_limited, ["user:01", "user:02"])

        inclusive_range = [
            k for k, _ in kd.scan_range("order:01", "user:02", include_start=True, include_end=True)
        ]
        self.assertEqual(inclusive_range, ["order:01", "user:01", "user:02"])

        exclusive_range = [
            k for k, _ in kd.scan_range("order:01", "user:02", include_start=False, include_end=False)
        ]
        self.assertEqual(exclusive_range, ["user:01"])


class TestLRUCache(unittest.TestCase):
    """Tests for LRUCache eviction order, TTL awareness, and oversized item bypass."""

    def test_lru_eviction_and_hit_rate(self) -> None:
        cache = LRUCache(capacity=3, max_value_bytes=128)
        cache.put("a", b"1")
        cache.put("b", b"2")
        cache.put("c", b"3")

        # Access "a" so "b" becomes the least-recently-used key
        self.assertEqual(cache.get("a"), b"1")
        cache.put("d", b"4")

        self.assertIsNone(cache.get("b"))
        self.assertEqual(cache.get("a"), b"1")
        self.assertEqual(cache.get("c"), b"3")
        self.assertEqual(cache.get("d"), b"4")

        stats = cache.stats()
        self.assertEqual(stats["evictions"], 1)
        self.assertEqual(stats["hits"], 4)
        self.assertEqual(stats["misses"], 1)

    def test_ttl_and_max_value_bytes(self) -> None:
        cache = LRUCache(capacity=4, max_value_bytes=10)
        cache.put("ttl_k", b"val", expire_at=2000)
        self.assertEqual(cache.get("ttl_k", now_ms=1999), b"val")
        self.assertIsNone(cache.get("ttl_k", now_ms=2000))

        # Oversized value should not be cached and should invalidate any prior value
        cache.put("big", b"small")
        self.assertEqual(cache.get("big"), b"small")
        cache.put("big", b"0123456789ABCDEF")
        self.assertIsNone(cache.get("big"))

    def test_zero_capacity_disabled_cache(self) -> None:
        cache = LRUCache(capacity=0)
        cache.put("k", b"v")
        self.assertIsNone(cache.get("k"))
        self.assertEqual(len(cache), 0)


if __name__ == "__main__":
    unittest.main()
