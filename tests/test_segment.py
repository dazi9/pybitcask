"""Unit and crash-recovery tests for DataSegment and .hint files."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from pybitcask.exceptions import CorruptDataError
from pybitcask.format import encode_record
from pybitcask.keydir import KeyDir
from pybitcask.segment import DataSegment, hint_file_path, segment_file_path


class TestDataSegment(unittest.TestCase):
    """Tests for segment file I/O, startup scan, hint files, and crash tail recovery."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._tmpdir.name)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_append_and_read_record_and_fast_value(self) -> None:
        seg_path = segment_file_path(self.base_dir, 1)
        seg = DataSegment(1, seg_path, writable=True, sync_policy="always")
        try:
            key = "student:1001"
            val = "计算机科学与技术-大二"
            key_bytes = key.encode("utf-8")
            val_bytes = val.encode("utf-8")
            _, raw = encode_record(1700000000000, key, val)

            rpos, vpos, tsize = seg.append_bytes(raw, len(key_bytes))
            self.assertEqual(rpos, 0)
            self.assertEqual(seg.write_position, tsize)

            # Full read with CRC check
            rec = seg.read_record(rpos, tsize, verify_crc=True)
            self.assertEqual(rec.key, key)
            self.assertEqual(rec.value_text(), val)

            # Fast value-only read
            fast_val = seg.read_value_only(vpos, len(val_bytes))
            self.assertEqual(fast_val.decode("utf-8"), val)
        finally:
            seg.close()

    def test_scan_with_skip_values_and_tombstones(self) -> None:
        seg_path = segment_file_path(self.base_dir, 2)
        seg = DataSegment(2, seg_path, writable=True)
        for k, v in [("k1", "v1"), ("k2", "v2"), ("k1", "v1_updated")]:
            _, raw = encode_record(100, k, v)
            seg.append_bytes(raw, len(k.encode("utf-8")))
        # Append tombstone for k2
        _, tomb = encode_record(200, "k2", b"", is_tombstone=True)
        seg.append_bytes(tomb, len(b"k2"))
        seg.close()

        # Reopen and scan using skip_values=True, verify_crc=False
        seg2 = DataSegment(2, seg_path, writable=False)
        try:
            kd = KeyDir()
            count = seg2.scan_into_keydir(
                kd, verify_crc=False, skip_values=True, use_hint=False
            )
            self.assertEqual(count, 4)
            self.assertEqual(kd.keys(), ["k1"])
            entry = kd.get("k1")
            self.assertIsNotNone(entry)
            assert entry is not None
            self.assertEqual(
                seg2.read_value_only(entry.value_pos, entry.value_size),
                b"v1_updated",
            )
        finally:
            seg2.close()

    def test_hint_file_generation_and_corrupt_hint_fallback(self) -> None:
        seg_path = segment_file_path(self.base_dir, 3)
        seg = DataSegment(3, seg_path, writable=True)
        for i in range(10):
            k = f"key_{i:02d}"
            v = f"value_payload_{i:02d}"
            _, raw = encode_record(1000 + i, k, v)
            seg.append_bytes(raw, len(k.encode("utf-8")))
        kd_initial = KeyDir()
        seg.scan_into_keydir(kd_initial, use_hint=False)
        DataSegment.write_hint_file(seg_path, kd_initial.items())
        seg.close()

        hpath = hint_file_path(seg_path)
        self.assertTrue(hpath.exists())

        # Load via hint file
        seg_reload = DataSegment(3, seg_path, writable=False)
        try:
            kd_hint = KeyDir()
            loaded = seg_reload.scan_into_keydir(kd_hint, use_hint=True)
            self.assertEqual(loaded, 10)
            self.assertEqual(kd_hint.keys(), kd_initial.keys())

            # Corrupt the hint file and verify automatic fallback to data segment scan
            raw_hint = bytearray(hpath.read_bytes())
            raw_hint[-1] ^= 0xFF
            hpath.write_bytes(bytes(raw_hint))

            kd_fallback = KeyDir()
            loaded_fb = seg_reload.scan_into_keydir(kd_fallback, use_hint=True)
            self.assertEqual(loaded_fb, 10)
            self.assertEqual(kd_fallback.keys(), kd_initial.keys())
        finally:
            seg_reload.close()

    def test_crash_tail_recovery_truncates_torn_write(self) -> None:
        seg_path = segment_file_path(self.base_dir, 4)
        seg = DataSegment(4, seg_path, writable=True)
        _, r1 = encode_record(100, "good_1", "val_1")
        _, r2 = encode_record(200, "good_2", "val_2")
        seg.append_bytes(r1, len(b"good_1"))
        seg.append_bytes(r2, len(b"good_2"))
        valid_size = seg.write_position
        seg.close()

        # Simulate a torn write at the end of the file (half of a record)
        _, torn = encode_record(300, "torn_key", "torn_value_payload")
        with open(seg_path, "ab") as f:
            f.write(torn[:15])  # Truncated header

        # With recover_tail=False, it must raise CorruptDataError
        seg_strict = DataSegment(4, seg_path, writable=True)
        try:
            with self.assertRaises(CorruptDataError):
                seg_strict.scan_into_keydir(KeyDir(), recover_tail=False)
        finally:
            seg_strict.close()

        # With recover_tail=True, it heals the file back to `valid_size` and loads good records
        seg_heal = DataSegment(4, seg_path, writable=True)
        try:
            kd = KeyDir()
            count = seg_heal.scan_into_keydir(kd, recover_tail=True)
            self.assertEqual(count, 2)
            self.assertEqual(kd.keys(), ["good_1", "good_2"])
            self.assertEqual(seg_heal.write_position, valid_size)
            self.assertEqual(os.path.getsize(seg_path), valid_size)
        finally:
            seg_heal.close()


if __name__ == "__main__":
    unittest.main()
