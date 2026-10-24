#!/usr/bin/env python3
"""Child process for the 'kill' fuzz profile.

Opens a PyBitcask database, performs two write sessions, and dies via
os._exit(1) in the middle of session B without a clean close, simulating
a hard process crash. Every applied operation is appended to an oplog
JSONL file (flushed per line) so the parent can replay the model.
"""
from __future__ import annotations

import json
import os
import random
import sys
from pathlib import Path

FUZZ_DIR = Path(__file__).resolve().parent
REPO_DIR = FUZZ_DIR.parent
sys.path.insert(0, str(REPO_DIR / "src"))

from pybitcask import BitcaskEngine  # noqa: E402

ENGINE_KW = dict(
    max_segment_size=8192,
    use_hint_files=True,
    verify_checksum=True,
    recover_tail=True,
    sync_policy="os",
    auto_compact=False,
    cache_capacity=256,
    log_level="ERROR",
)

KEY_POOL = [f"k{i}" for i in range(60)] + [f"键{i}" for i in range(20)]


def random_value(rng: random.Random) -> str:
    n = rng.randint(0, 200)
    return "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789中文测试") for _ in range(n))


def main() -> None:
    db, seed, nops_a, nops_b = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
    oplog_path = Path(db).parent / "oplog.jsonl"
    rng = random.Random(seed)

    oplog = open(oplog_path, "a", encoding="utf-8")

    def emit(op: dict) -> None:
        oplog.write(json.dumps(op, ensure_ascii=False) + "\n")
        oplog.flush()

    # Session A: clean open, writes, clean close (hint file gets written here).
    eng = BitcaskEngine(db, **ENGINE_KW)
    for _ in range(nops_a):
        key = rng.choice(KEY_POOL)
        value = random_value(rng)
        eng.set(key, value)
        emit({"op": "put", "key": key, "value": value})
    eng.close()

    # Session B: reopen, write more, then hard-crash without close.
    eng = BitcaskEngine(db, **ENGINE_KW)
    for _ in range(nops_b):
        key = rng.choice(KEY_POOL)
        value = random_value(rng)
        eng.set(key, value)
        emit({"op": "put", "key": key, "value": value})
    sys.stdout.flush()
    os._exit(1)  # hard crash: no close(), no hint refresh, no lock cleanup


if __name__ == "__main__":
    main()
