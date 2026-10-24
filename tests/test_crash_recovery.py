"""Regression tests for crash-consistency bugs found by fuzz/model_fuzz.py.

Each test reproduces a bug that once shipped:
1. tombstone resurrection via hint files that carried only live keys
2. data loss when a stale hint shadowed records appended after the last
   clean close (hard crash)
3. torn-tail truncation leaving stale keydir entries pointing past EOF
4. stale values / resurrection on replay because compacted segments carry
   higher file ids than the active segment
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pybitcask import BitcaskEngine  # noqa: E402

ENGINE_KW = dict(
    max_segment_size=1024,   # tiny segments force rotation constantly
    use_hint_files=True,
    verify_checksum=True,
    recover_tail=True,
    sync_policy="os",
    auto_compact=False,
    cache_capacity=16,
    log_level="ERROR",
)


class TestCrashRecovery(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "db"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _reopen(self) -> BitcaskEngine:
        return BitcaskEngine(str(self.db), **ENGINE_KW)

    def test_delete_survives_reopen_after_rotation(self) -> None:
        """Hint files must carry tombstones, or deleted keys resurrect."""
        eng = self._reopen()
        eng.set("k", "v1")               # segment 1
        eng.set("filler", "x" * 900)     # forces rotation -> segment 2
        eng.delete("k")                  # tombstone lands in segment 2
        eng.close()
        eng = self._reopen()
        self.assertFalse(eng.exists("k"), "deleted key resurrected after reopen")
        self.assertEqual(eng.keys(), ["filler"])
        eng.close()

    def test_stale_hint_does_not_shadow_records_appended_after_crash(self) -> None:
        """A hint from a clean close must not hide later flushed writes."""
        eng = self._reopen()
        eng.set("a", "1")
        eng.close()                      # hint written here

        eng = self._reopen()             # session B appends to the same segment
        eng.set("b", "2")
        eng._release_lock()              # simulate hard crash: no close()

        eng = self._reopen()
        self.assertEqual(eng.get_bytes("a"), b"1")
        self.assertEqual(eng.get_bytes("b"), b"2", "flushed write lost after crash")
        eng.close()

    def test_torn_tail_with_hint_heals_on_startup(self) -> None:
        """After a torn tail, no keydir entry may point past EOF."""
        eng = self._reopen()
        for i in range(12):
            eng.set(f"k{i}", f"value-{i}-" + "y" * 40)
        active_seg = eng._active_segment.path
        eng.close()

        size = active_seg.stat().st_size
        with open(active_seg, "r+b") as f:
            f.truncate(size - 40)        # tear the last record

        eng = self._reopen()
        for key in eng.keys():           # must not raise CorruptDataError
            eng.get_bytes(key)
        eng.close()

    def test_compacted_replay_cannot_overwrite_newer_values(self) -> None:
        """Compacted segments get higher file ids than the active segment;
        replay must resolve conflicts by timestamp, not scan order."""
        eng = self._reopen()
        eng.set("k", "v1")               # segment 1
        eng.set("f", "x" * 900)          # rotate -> segment 2
        eng.set("k", "v2")               # segment 2 (active)
        eng.compact()                    # k@v2 copied into a higher-fid segment
        eng.set("g", "z" * 900)          # rotate again
        eng.set("k", "v3")               # newest write, low-fid active segment
        eng.close()

        eng = self._reopen()
        self.assertEqual(eng.get_bytes("k"), b"v3", "stale compacted value won")
        eng.close()

    def test_compacted_replay_cannot_resurrect_deleted_keys(self) -> None:
        """Delete after compaction must survive replay (tombstone in a
        lower-fid segment, live snapshot in a higher-fid compacted segment)."""
        eng = self._reopen()
        eng.set("k", "v1")
        eng.set("f", "x" * 900)          # rotate
        eng.set("k", "v2")
        eng.compact()                    # k@v2 snapshot now in a high-fid segment
        eng.delete("k")                  # tombstone in the low-fid active segment
        eng.close()

        eng = self._reopen()
        self.assertFalse(eng.exists("k"), "compacted snapshot resurrected key")
        eng.close()

    def test_double_delete_and_reput_converge(self) -> None:
        """Interleaved delete/re-put across rotations must converge to the
        newest intent after reopen."""
        eng = self._reopen()
        eng.set("k", "v1")
        eng.delete("k")
        eng.set("k", "v2")
        eng.set("f", "x" * 900)          # rotate
        eng.delete("k")
        eng.delete("k")                  # double delete: second is a no-op
        eng.set("k", "v3")
        eng.close()

        eng = self._reopen()
        self.assertEqual(eng.get_bytes("k"), b"v3")
        eng.close()


if __name__ == "__main__":
    unittest.main()
