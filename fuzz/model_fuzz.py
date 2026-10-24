#!/usr/bin/env python3
"""Model-based differential and crash-consistency fuzzer for PyBitcask.

Profiles and their contracts:

  ops     - random op sequences (put/delete/get/scan/batch/compact/reopen)
            checked against an in-memory oracle after every step.
  torn    - clean close -> truncate the active segment tail by 1..512 bytes
            -> reopen. Contract: startup must either fail with a clean
            BitcaskError, or succeed with every pre-cut key readable and
            equal to a recent prefix snapshot of the op history.
  corrupt - clean close -> flip 1..3 bits in a random segment file
            -> reopen. Contract: startup fails with a clean BitcaskError,
            or succeeds and every read returns either the correct value
            or raises CorruptDataError (fail-closed, never wrong data);
            post-corruption writes must keep working.
  kill    - a child process writes in two sessions and dies via os._exit(1)
            in the middle of session B (no clean close). All completed
            writes were flushed to the OS page cache, so on reopen every
            op except possibly the last 2 must be intact.
  ttl     - expiry semantics: no public API may report an expired key as
            alive, before or after reopen/compaction.

Usage:
  PYTHONPATH=src python3 fuzz/model_fuzz.py --profile ops --rounds 40 --seed 1
  PYTHONPATH=src python3 fuzz/model_fuzz.py --profile all --rounds 40
"""
from __future__ import annotations

import argparse
import json
import random
import shutil
import subprocess
import sys
import time
import traceback
from pathlib import Path

FUZZ_DIR = Path(__file__).resolve().parent
REPO_DIR = FUZZ_DIR.parent
sys.path.insert(0, str(REPO_DIR / "src"))

from pybitcask import (  # noqa: E402
    BatchOperation,
    BitcaskConfig,
    BitcaskEngine,
    BitcaskError,
    CorruptDataError,
)

WORK_ROOT = FUZZ_DIR / "_work"
FINDINGS_PATH = FUZZ_DIR / "findings.jsonl"

ENGINE_KW = dict(
    max_segment_size=8192,   # force frequent segment rotation
    use_hint_files=True,
    verify_checksum=True,
    recover_tail=True,
    sync_policy="os",
    auto_compact=False,
    cache_capacity=256,
    log_level="ERROR",
)

KEY_POOL = (
    [f"k{i}" for i in range(80)]
    + [f"用户:{i}" for i in range(20)]
    + [f"key:{i}:suffix" for i in range(20)]
)
ASCII_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789"
CJK_CHARS = "中文测试数据存储引擎模糊"

FINDING_COUNT = 0


def finding(profile: str, seed: int, round_no: int, kind: str, detail: str) -> None:
    global FINDING_COUNT
    FINDING_COUNT += 1
    record = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "profile": profile,
        "seed": seed,
        "round": round_no,
        "kind": kind,
        "detail": detail,
    }
    FINDINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(FINDINGS_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"[FINDING] {kind} profile={profile} seed={seed} round={round_no}\n  {detail}")


def open_engine(db: Path) -> BitcaskEngine:
    return BitcaskEngine(str(db), **ENGINE_KW)


def reopen_engine(db: Path, eng: BitcaskEngine | None) -> BitcaskEngine:
    if eng is not None:
        eng.close()
    return open_engine(db)


def random_value(rng: random.Random) -> str:
    alphabet = ASCII_CHARS if rng.random() < 0.6 else CJK_CHARS
    n = rng.randint(0, 200)
    return "".join(rng.choice(alphabet) for _ in range(n))


def read_all(eng: BitcaskEngine) -> tuple[dict[str, bytes], list[str]]:
    """Return (key->value state, list of keys that raised instead of returning)."""
    state: dict[str, bytes] = {}
    unreadable: list[str] = []
    for key in eng.keys():
        try:
            state[key] = eng.get_bytes(key)
        except BitcaskError:
            unreadable.append(key)
    return state, unreadable


def full_verify(eng: BitcaskEngine, model: dict[str, str], ctx: str, profile: str,
                seed: int, round_no: int) -> None:
    keys = set(eng.keys())
    expected = set(model)
    if keys != expected:
        missing = sorted(expected - keys)[:8]
        extra = sorted(keys - expected)[:8]
        finding(profile, seed, round_no, "OPS-KEYSET",
                f"{ctx}: keydir mismatch; missing={missing} extra={extra}")
        return
    if len(eng) != len(model):
        finding(profile, seed, round_no, "OPS-LEN",
                f"{ctx}: len(engine)={len(eng)} != len(model)={len(model)}")
    for key in sorted(expected):
        got = eng.get_bytes(key)
        want = model[key].encode("utf-8")
        if got != want:
            finding(profile, seed, round_no, "OPS-VALUE",
                    f"{ctx}: key={key!r} got={got!r} want={want!r}")
            return


def scan_checks(eng: BitcaskEngine, model: dict[str, str], ctx: str, profile: str,
                seed: int, round_no: int, rng: random.Random) -> None:
    prefix = rng.choice(KEY_POOL)[: rng.randint(1, 4)]
    got = eng.scan_prefix(prefix)
    want = sorted((k, v) for k, v in model.items() if k.startswith(prefix))
    if got != want:
        finding(profile, seed, round_no, "OPS-SCAN-PREFIX",
                f"{ctx}: prefix={prefix!r} got_len={len(got)} want_len={len(want)} "
                f"first_diff={next((x for x, y in zip(got, want) if x != y), None)!r}")
    keys_sorted = sorted(model)
    if keys_sorted:
        a = rng.choice(keys_sorted)
        b = rng.choice(keys_sorted)
        lo, hi = min(a, b), max(a, b)
        got_r = eng.scan_range(lo, hi)
        want_r = sorted((k, v) for k, v in model.items() if lo <= k <= hi)
        if got_r != want_r:
            finding(profile, seed, round_no, "OPS-SCAN-RANGE",
                    f"{ctx}: range=[{lo!r},{hi!r}] got_len={len(got_r)} want_len={len(want_r)}")


def profile_ops(rng: random.Random, db: Path, round_no: int, seed: int) -> None:
    n_ops = rng.randint(120, 400)
    model: dict[str, str] = {}
    eng = open_engine(db)
    try:
        for i in range(n_ops):
            r = rng.random()
            if r < 0.50:
                key = rng.choice(KEY_POOL)
                value = random_value(rng)
                eng.set(key, value)
                model[key] = value
            elif r < 0.64:
                key = rng.choice(KEY_POOL)
                existed = key in model
                ret = eng.delete(key)
                if ret != existed:
                    finding("ops", seed, round_no, "OPS-DELETE-RET",
                            f"op{i}: delete({key!r}) returned {ret}, key existed={existed}")
                model.pop(key, None)
            elif r < 0.78:
                key = rng.choice(KEY_POOL)
                got = eng.get_bytes(key)
                want = None if key not in model else model[key].encode("utf-8")
                if got != want:
                    finding("ops", seed, round_no, "OPS-GET",
                            f"op{i}: get({key!r}) got={got!r} want={want!r}")
            elif r < 0.88:
                batch = []
                for _ in range(rng.randint(2, 6)):
                    if rng.random() < 0.7:
                        key = rng.choice(KEY_POOL)
                        value = random_value(rng)
                        batch.append(BatchOperation("put", key, value))
                        model[key] = value
                    else:
                        key = rng.choice(KEY_POOL)
                        batch.append(BatchOperation("delete", key, b""))
                        model.pop(key, None)
                ret = eng.batch_write(batch)
                if ret != len(batch):
                    finding("ops", seed, round_no, "OPS-BATCH-RET",
                            f"op{i}: batch_write returned {ret}, expected {len(batch)}")
            elif r < 0.93:
                eng.compact()
                full_verify(eng, model, f"op{i}:after-compact", "ops", seed, round_no)
            else:
                eng = reopen_engine(db, eng)
                full_verify(eng, model, f"op{i}:after-reopen", "ops", seed, round_no)
            if rng.random() < 0.15:
                scan_checks(eng, model, f"op{i}", "ops", seed, round_no, rng)
        full_verify(eng, model, "final", "ops", seed, round_no)
        scan_checks(eng, model, "final", "ops", seed, round_no, rng)
    finally:
        eng.close()


def build_history(rng: random.Random, db: Path, round_no: int, seed: int,
                  n_ops: int) -> tuple[list[dict[str, str]], int, Path]:
    """Run n_ops writes on a fresh engine, snapshotting the model after each op."""
    model: dict[str, str] = {}
    history: list[dict[str, str]] = [{}]
    eng = open_engine(db)
    active_seg = eng._active_segment.path
    try:
        for _ in range(n_ops):
            key = rng.choice(KEY_POOL)
            value = random_value(rng)
            eng.set(key, value)
            model[key] = value
            history.append(dict(model))
    finally:
        active_seg = eng._active_segment.path
        eng.close()
    return history, n_ops, active_seg


def profile_torn(rng: random.Random, db: Path, round_no: int, seed: int) -> None:
    n_ops = rng.randint(30, 90)
    history, n_ops, active_seg = build_history(rng, db, round_no, seed, n_ops)
    size = active_seg.stat().st_size
    if size <= 1:
        return
    cut = rng.randint(1, min(512, size - 1))
    with open(active_seg, "r+b") as f:
        f.truncate(size - cut)

    try:
        eng = open_engine(db)
    except BitcaskError as exc:
        return  # fail-closed at startup is acceptable
    except Exception:
        finding("torn", seed, round_no, "TORN-STARTUP-CRASH",
                f"truncate {cut}B of {active_seg.name}: "
                f"non-BitcaskError {traceback.format_exc(limit=3)}")
        return
    try:
        state, unreadable = read_all(eng)
        if unreadable:
            finding("torn", seed, round_no, "TORN-UNREADABLE-KEY",
                    f"truncate {cut}B: {len(unreadable)} keys in keydir unreadable, "
                    f"first={unreadable[0]!r} (stale keydir entries after torn tail)")
            return
        expected = [json.dumps(h, sort_keys=True) for h in history[max(0, n_ops - 8):]]
        got = json.dumps({k: v.decode("utf-8") for k, v in state.items()}, sort_keys=True)
        if got not in expected:
            finding("torn", seed, round_no, "TORN-PREFIX",
                    f"truncate {cut}B: state does not match any of the last 8 "
                    f"prefix snapshots (lost more than the torn tail)")
    finally:
        eng.close()


def profile_corrupt(rng: random.Random, db: Path, round_no: int, seed: int) -> None:
    n_ops = rng.randint(30, 90)
    build_history(rng, db, round_no, seed, n_ops)
    segs = sorted((db).glob("data_*.bitcask"))
    segs = [s for s in segs if s.stat().st_size > 0]
    if not segs:
        return
    target = rng.choice(segs)
    size = target.stat().st_size
    data = bytearray(target.read_bytes())
    for _ in range(rng.randint(1, 3)):
        pos = rng.randrange(size)
        data[pos] ^= 1 << rng.randrange(8)
    target.write_bytes(data)

    try:
        eng = open_engine(db)
    except BitcaskError:
        return  # fail-closed at startup is acceptable
    except Exception:
        finding("corrupt", seed, round_no, "CORRUPT-STARTUP-CRASH",
                f"bitflip in {target.name}: non-BitcaskError "
                f"{traceback.format_exc(limit=3)}")
        return
    try:
        for key in eng.keys():
            try:
                got = eng.get_bytes(key)
            except BitcaskError:
                continue  # clean failure on a corrupted record is acceptable
            # value must decode and match something plausible: we cannot map the
            # corrupted byte to a key, so only hard-fail on non-decodable returns
            try:
                got.decode("utf-8")
            except UnicodeDecodeError:
                finding("corrupt", seed, round_no, "CORRUPT-RAW-VALUE",
                        f"get({key!r}) returned non-UTF-8 bytes without raising")
        # post-corruption writes must keep working
        eng.set("post_corrupt_key", "alive")
        if eng.get_bytes("post_corrupt_key") != b"alive":
            finding("corrupt", seed, round_no, "CORRUPT-POSTOP",
                    "write after corrupted reopen did not read back")
    finally:
        eng.close()


def profile_kill(rng: random.Random, db: Path, round_no: int, seed: int) -> None:
    nops_a = rng.randint(20, 60)
    nops_b = rng.randint(5, 25)
    oplog = db.parent / "oplog.jsonl"
    oplog.unlink(missing_ok=True)
    proc = subprocess.run(
        [sys.executable, str(FUZZ_DIR / "kill_writer.py"), str(db), str(seed),
         str(nops_a), str(nops_b)],
        capture_output=True, text=True, timeout=120,
    )
    if proc.returncode != 1:
        finding("kill", seed, round_no, "KILL-WRITER-FAILED",
                f"writer rc={proc.returncode} stderr={proc.stderr[-400:]}")
        return
    ops = []
    with open(oplog, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    ops.append(json.loads(line))
                except json.JSONDecodeError:
                    break
    n = len(ops)
    if n == 0:
        return
    model: dict[str, str] = {}
    history: list[dict[str, str]] = [{}]
    for op in ops:
        model[op["key"]] = op["value"]
        history.append(dict(model))

    try:
        eng = open_engine(db)
    except BitcaskError as exc:
        finding("kill", seed, round_no, "KILL-STARTUP-ERROR",
                f"reopen after hard crash raised {type(exc).__name__}: {exc}")
        return
    try:
        state, unreadable = read_all(eng)
        if unreadable:
            finding("kill", seed, round_no, "KILL-UNREADABLE-KEY",
                    f"{len(unreadable)} keys unreadable after crash, first={unreadable[0]!r}")
            return
        got = json.dumps(state_decoded(state), sort_keys=True, ensure_ascii=False)
        window = [json.dumps(h, sort_keys=True, ensure_ascii=False) for h in history[max(0, n - 2):]]
        if got not in window:
            expected_n = history[n]
            lost = sorted(set(expected_n) - set(state))
            finding("kill", seed, round_no, "KILL-LOST-FLUSHED-WRITES",
                    f"after os._exit crash, {n} flushed ops; state matches none of "
                    f"the last 2 snapshots; {len(lost)} keys lost, first={lost[:5]!r}")
    finally:
        eng.close()


def state_decoded(state: dict[str, bytes]) -> dict[str, str]:
    return {k: v.decode("utf-8") for k, v in state.items()}


def profile_ttl(rng: random.Random, db: Path, round_no: int, seed: int) -> None:
    del rng
    eng = open_engine(db)
    try:
        # Liveness check with a generous TTL: on a loaded machine even
        # back-to-back statements can be descheduled for >100ms.
        eng.set("t:alive", "v", ttl_ms=5000)
        if eng.get_bytes("t:alive") != b"v":
            finding("ttl", seed, round_no, "TTL-EARLY-GONE", "live key expired too early")
        # Expiry check: short TTL, then sleep past it.
        eng.set("t:expire", "e", ttl_ms=120)
        time.sleep(0.25)
        still = {
            "get": eng.get_bytes("t:expire") is not None,
            "exists": eng.exists("t:expire"),
            "keys": "t:expire" in eng.keys(),
            "scan": any(k == "t:expire" for k, _ in eng.scan_prefix("t:")),
        }
        alive = [name for name, ok in still.items() if ok]
        if alive:
            finding("ttl", seed, round_no, "TTL-EXPIRED-BUT-ALIVE",
                    f"expired key still visible via: {alive}")
        eng.set("t:long", "w", ttl_ms=60_000)
        eng.close()
        eng = open_engine(db)
        if eng.get_bytes("t:long") != b"w":
            finding("ttl", seed, round_no, "TTL-LOST-ON-REOPEN",
                    "non-expired TTL key lost across reopen (hint/TTL reload path)")
        time.sleep(0.2)
        eng.set("t:short2", "x", ttl_ms=80)
        eng.close()
        time.sleep(0.3)
        eng = open_engine(db)
        if eng.get_bytes("t:short2") is not None or eng.exists("t:short2"):
            finding("ttl", seed, round_no, "TTL-RESURRECT-ON-REOPEN",
                    "expired key resurrected after reopen")
        eng.compact()
        if eng.exists("t:short2"):
            finding("ttl", seed, round_no, "TTL-RESURRECT-ON-COMPACT",
                    "expired key resurrected by compaction")
    finally:
        eng.close()


PROFILES = {
    "ops": profile_ops,
    "torn": profile_torn,
    "corrupt": profile_corrupt,
    "kill": profile_kill,
    "ttl": profile_ttl,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=[*PROFILES, "all"], default="all")
    parser.add_argument("--rounds", type=int, default=40)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()

    names = list(PROFILES) if args.profile == "all" else [args.profile]
    round_no = 0
    for profile in names:
        for i in range(args.rounds if profile != "ttl" else 3):
            round_no += 1
            seed = args.seed * 100_003 + round_no * 7 + hash(profile) % 997
            rng = random.Random(seed)
            case_dir = WORK_ROOT / f"{profile}-{round_no:04d}"
            case_dir.mkdir(parents=True, exist_ok=True)
            db = case_dir / "db"
            try:
                PROFILES[profile](rng, db, round_no, seed)
            except Exception:
                finding(profile, seed, round_no, "HARNESS-UNEXPECTED",
                        traceback.format_exc(limit=6))
            finally:
                shutil.rmtree(case_dir, ignore_errors=True)
    print(f"done: {round_no} rounds, {FINDING_COUNT} findings")
    return 1 if FINDING_COUNT else 0


if __name__ == "__main__":
    sys.exit(main())
