"""Reproducible benchmark suite for PyBitcask storage engine."""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import Any

from pybitcask import BatchOperation, BitcaskConfig, BitcaskEngine


def bench_write_policies(base_dir: Path) -> dict[str, Any]:
    results: dict[str, Any] = {}
    value_128b = "v" * 128

    # 1. sync_policy = "always" (200 writes with per-write fsync)
    n_always = 200
    p_always = base_dir / "write_always"
    with BitcaskEngine(db_path=p_always, sync_policy="always") as db:
        t0 = time.perf_counter()
        for i in range(n_always):
            db.set(f"k:{i:06d}", value_128b)
        elapsed = time.perf_counter() - t0
    results["sync_always"] = {
        "records": n_always,
        "elapsed_ms": round(elapsed * 1000, 2),
        "ops_per_sec": round(n_always / elapsed, 1),
    }

    # 2. sync_policy = "batch" (2,000 writes, fsync every 128 records)
    n_batch = 2000
    p_batch = base_dir / "write_batch_policy"
    with BitcaskEngine(
        db_path=p_batch, sync_policy="batch", sync_interval_records=128
    ) as db:
        t0 = time.perf_counter()
        for i in range(n_batch):
            db.set(f"k:{i:06d}", value_128b)
        db.sync()
        elapsed = time.perf_counter() - t0
    results["sync_batch_128"] = {
        "records": n_batch,
        "elapsed_ms": round(elapsed * 1000, 2),
        "ops_per_sec": round(n_batch / elapsed, 1),
    }

    # 3. sync_policy = "os" (5,000 writes, fsync on close/sync)
    n_os = 5000
    p_os = base_dir / "write_os_policy"
    with BitcaskEngine(db_path=p_os, sync_policy="os") as db:
        t0 = time.perf_counter()
        for i in range(n_os):
            db.set(f"k:{i:06d}", value_128b)
        db.sync()
        elapsed = time.perf_counter() - t0
    results["sync_os"] = {
        "records": n_os,
        "elapsed_ms": round(elapsed * 1000, 2),
        "ops_per_sec": round(n_os / elapsed, 1),
    }

    # 4. batch_write API with sync_policy = "always" (5,000 writes in batches of 500)
    n_bw = 5000
    p_bw = base_dir / "write_batch_api"
    with BitcaskEngine(db_path=p_bw, sync_policy="always") as db:
        t0 = time.perf_counter()
        for batch_start in range(0, n_bw, 500):
            ops = [
                BatchOperation("put", f"k:{i:06d}", value_128b)
                for i in range(batch_start, batch_start + 500)
            ]
            db.batch_write(ops)
        elapsed = time.perf_counter() - t0
    results["batch_write_500_sync_always"] = {
        "records": n_bw,
        "elapsed_ms": round(elapsed * 1000, 2),
        "ops_per_sec": round(n_bw / elapsed, 1),
    }

    return results


def bench_startup_recovery(base_dir: Path) -> dict[str, Any]:
    """Compare cold startup time across 500 keys updated 8 times (4,000 records x 4 KiB = ~16 MiB)."""
    n_keys = 500
    n_versions = 8
    val_4kb = "X" * 4096
    db_dir = base_dir / "startup_bench"

    with BitcaskEngine(
        db_path=db_dir,
        max_segment_size=32 * 1024 * 1024,
        sync_policy="os",
        use_hint_files=True,
    ) as db:
        for v in range(n_versions):
            for i in range(n_keys):
                db.set(f"doc:{i:06d}", f"v{v}:{val_4kb}")

    # 1. Startup with Hint File (.hint)
    t0 = time.perf_counter()
    with BitcaskEngine(
        db_path=db_dir,
        verify_checksum=True,
        use_hint_files=True,
    ) as db:
        assert len(db) == n_keys
    t_hint = time.perf_counter() - t0

    # 2. Startup scanning segment with skip_values_on_startup=True (lseek over 4KiB values)
    t0 = time.perf_counter()
    with BitcaskEngine(
        db_path=db_dir,
        verify_checksum=False,
        skip_values_on_startup=True,
        use_hint_files=False,
    ) as db:
        assert len(db) == n_keys
    t_skip_val = time.perf_counter() - t0

    # 3. Startup scanning segment reading full 4KiB values + CRC32 verification
    t0 = time.perf_counter()
    with BitcaskEngine(
        db_path=db_dir,
        verify_checksum=True,
        skip_values_on_startup=False,
        use_hint_files=False,
    ) as db:
        assert len(db) == n_keys
    t_full_scan = time.perf_counter() - t0

    return {
        "dataset": f"{n_keys} keys x {n_versions} updates x 4 KiB (~16 MiB log)",
        "full_value_crc_scan_ms": round(t_full_scan * 1000, 2),
        "seek_skip_values_scan_ms": round(t_skip_val * 1000, 2),
        "hint_file_load_ms": round(t_hint * 1000, 2),
        "hint_speedup_vs_full": round(t_full_scan / max(t_hint, 1e-6), 2),
    }


def bench_read_paths(base_dir: Path) -> dict[str, Any]:
    n_keys = 5000
    val_256b = "R" * 256
    db_dir = base_dir / "read_bench"

    cfg = BitcaskConfig(
        db_path=db_dir,
        max_segment_size=16 * 1024 * 1024,
        sync_policy="os",
        cache_capacity=1024,
        verify_checksum=True,
    )
    with BitcaskEngine(config=cfg) as db:
        for i in range(n_keys):
            db.set(f"item:{i:06d}", val_256b)

        # 1. Cold disk reads + full CRC32 verification (cache disabled)
        db.cache.capacity = 0
        db.cache.clear()
        db.config.verify_checksum = True
        t0 = time.perf_counter()
        for i in range(n_keys):
            _ = db.get(f"item:{i:06d}")
        t_cold_crc = time.perf_counter() - t0

        # 2. Cold disk reads value-only via os.pread (cache disabled, verify_checksum=False)
        db.config.verify_checksum = False
        t0 = time.perf_counter()
        for i in range(n_keys):
            _ = db.get(f"item:{i:06d}")
        t_cold_fast = time.perf_counter() - t0

        # 3. Hot working-set reads with LRU Cache (500 hot keys, capacity=1024)
        db.config.verify_checksum = True
        db.cache.capacity = 1024
        db.cache.clear()
        for i in range(500):
            _ = db.get(f"item:{i:06d}")
        t0 = time.perf_counter()
        for i in range(n_keys):
            _ = db.get(f"item:{i % 500:06d}")
        t_hot_cache = time.perf_counter() - t0

    return {
        "cold_read_with_crc32": {
            "reads": n_keys,
            "elapsed_ms": round(t_cold_crc * 1000, 2),
            "ops_per_sec": round(n_keys / t_cold_crc, 1),
        },
        "cold_read_value_only_pread": {
            "reads": n_keys,
            "elapsed_ms": round(t_cold_fast * 1000, 2),
            "ops_per_sec": round(n_keys / t_cold_fast, 1),
        },
        "hot_read_lru_cache": {
            "reads": n_keys,
            "elapsed_ms": round(t_hot_cache * 1000, 2),
            "ops_per_sec": round(n_keys / t_hot_cache, 1),
            "speedup_vs_cold_crc": round(t_cold_crc / max(t_hot_cache, 1e-6), 2),
        },
    }


def bench_compaction(base_dir: Path) -> dict[str, Any]:
    db_dir = base_dir / "compact_bench"
    val_512b = "C" * 512
    cfg = BitcaskConfig(
        db_path=db_dir,
        max_segment_size=64 * 1024,  # 64 KiB segments
        sync_policy="os",
        use_hint_files=True,
    )
    with BitcaskEngine(config=cfg) as db:
        # 400 keys overwritten 10 times = 4,000 writes
        for version in range(10):
            for k in range(400):
                db.set(f"record:{k:04d}", f"v{version}:{val_512b}")
        # Delete 100 keys
        for k in range(100):
            db.delete(f"record:{k:04d}")

        pre_stats = db.stats()
        t0 = time.perf_counter()
        compact_res = db.compact()
        t_compact = time.perf_counter() - t0
        post_stats = db.stats()

    reduction_pct = (
        (compact_res["reclaimed_bytes"] / compact_res["bytes_before"]) * 100.0
        if compact_res["bytes_before"] > 0
        else 0.0
    )
    return {
        "segments_before": pre_stats["segment_count"],
        "segments_after": post_stats["segment_count"],
        "bytes_before": compact_res["bytes_before"],
        "bytes_after": compact_res["bytes_after"],
        "reclaimed_bytes": compact_res["reclaimed_bytes"],
        "space_reduction_percent": round(reduction_pct, 2),
        "compaction_time_ms": round(t_compact * 1000, 2),
        "live_keys_verified": post_stats["live_keys"],
    }


def run_all_benchmarks() -> dict[str, Any]:
    tmp_root = Path("/tmp/opencode") if Path("/tmp/opencode").exists() else Path(tempfile.gettempdir())
    with tempfile.TemporaryDirectory(dir=tmp_root) as tmp:
        base_dir = Path(tmp)
        report = {
            "write_benchmarks": bench_write_policies(base_dir),
            "startup_benchmarks": bench_startup_recovery(base_dir),
            "read_benchmarks": bench_read_paths(base_dir),
            "compaction_benchmarks": bench_compaction(base_dir),
        }
    return report


if __name__ == "__main__":
    results = run_all_benchmarks()
    print(json.dumps(results, indent=2, ensure_ascii=False))
