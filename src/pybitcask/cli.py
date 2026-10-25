"""Command-line interface and interactive REPL shell for PyBitcask."""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from typing import Any, Sequence

from pybitcask.engine import BitcaskEngine
from pybitcask.exceptions import BitcaskError
from pybitcask.server import BitcaskHTTPServer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pybitcask",
        description="PyBitcask: Log-Structured Key-Value Storage Engine CLI",
    )
    parser.add_argument(
        "--db",
        default="bitcask_data",
        help="Database directory or file path (default: bitcask_data)",
    )
    parser.add_argument(
        "--sync-policy",
        choices=["os", "batch", "always"],
        default="os",
        help="Write durability policy (default: os)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_set = sub.add_parser("set", help="Set a key-value pair")
    p_set.add_argument("key", help="Key string")
    p_set.add_argument("value", help="Value string")
    p_set.add_argument("--ttl", type=int, default=None, help="Optional TTL in milliseconds")

    p_get = sub.add_parser("get", help="Get the value of a key")
    p_get.add_argument("key", help="Key string")

    p_del = sub.add_parser("delete", help="Delete a key")
    p_del.add_argument("key", help="Key string")

    p_keys = sub.add_parser("keys", help="List all live keys")
    p_keys.add_argument("--prefix", default="", help="Optional key prefix filter")

    p_scan = sub.add_parser("scan", help="Scan key-value pairs by prefix or range")
    p_scan.add_argument("--prefix", default=None, help="Key prefix filter")
    p_scan.add_argument("--start", default=None, help="Range start key (inclusive)")
    p_scan.add_argument("--end", default=None, help="Range end key (inclusive)")
    p_scan.add_argument("--limit", type=int, default=None, help="Maximum items to return")

    sub.add_parser("compact", help="Compact segments and reclaim dead disk space")
    sub.add_parser("stats", help="Display storage engine telemetry")
    sub.add_parser("shell", help="Launch interactive REPL shell")

    p_serve = sub.add_parser("serve", help="Start HTTP REST API server")
    p_serve.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    p_serve.add_argument("--port", type=int, default=7379, help="Bind port (default: 7379)")

    return parser


def run_repl(engine: BitcaskEngine, input_stream: Any = None, output_stream: Any = None) -> int:
    """Run an interactive Redis-style REPL session against `engine`."""
    inp = input_stream or sys.stdin
    out = output_stream or sys.stdout

    out.write("PyBitcask Interactive Shell. Type HELP for commands or EXIT to quit.\n")
    while True:
        out.write("bitcask> ")
        out.flush()
        line = inp.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue
        try:
            parts = shlex.split(line)
        except ValueError as exc:
            out.write(f"ERR parse error: {exc}\n")
            continue

        cmd = parts[0].upper()
        try:
            if cmd in ("EXIT", "QUIT"):
                out.write("BYE\n")
                break
            elif cmd == "HELP":
                out.write(
                    "Commands: SET <k> <v> [TTL_MS] | GET <k> | DEL <k> | EXISTS <k> | "
                    "KEYS [prefix] | SCAN <prefix> | COMPACT | STATS | EXIT\n"
                )
            elif cmd == "SET":
                if len(parts) < 3:
                    out.write("ERR usage: SET <key> <value> [ttl_ms]\n")
                    continue
                ttl = int(parts[3]) if len(parts) >= 4 else None
                engine.set(parts[1], parts[2], ttl_ms=ttl)
                out.write("OK\n")
            elif cmd == "GET":
                if len(parts) != 2:
                    out.write("ERR usage: GET <key>\n")
                    continue
                if not engine.exists(parts[1]):
                    out.write("(nil)\n")
                else:
                    out.write(f"{engine.get(parts[1])}\n")
            elif cmd in ("DEL", "DELETE"):
                if len(parts) != 2:
                    out.write("ERR usage: DEL <key>\n")
                    continue
                deleted = engine.delete(parts[1])
                out.write("(integer) 1\n" if deleted else "(integer) 0\n")
            elif cmd == "EXISTS":
                if len(parts) != 2:
                    out.write("ERR usage: EXISTS <key>\n")
                    continue
                out.write("(integer) 1\n" if engine.exists(parts[1]) else "(integer) 0\n")
            elif cmd == "KEYS":
                prefix = parts[1] if len(parts) >= 2 else ""
                keys = (
                    [k for k, _ in engine.scan_prefix(prefix)]
                    if prefix
                    else engine.keys()
                )
                for idx, k in enumerate(keys, 1):
                    out.write(f"{idx}) {k}\n")
                if not keys:
                    out.write("(empty list)\n")
            elif cmd == "SCAN":
                prefix = parts[1] if len(parts) >= 2 else ""
                for k, v in engine.scan_prefix(prefix):
                    out.write(f"{k} => {v}\n")
            elif cmd == "COMPACT":
                res = engine.compact()
                out.write(f"{json.dumps(res, ensure_ascii=False)}\n")
            elif cmd == "STATS":
                out.write(f"{json.dumps(engine.stats(), indent=2, ensure_ascii=False)}\n")
            else:
                out.write(f"ERR unknown command '{cmd}'\n")
        except (BitcaskError, ValueError) as exc:
            out.write(f"ERR {exc}\n")

    return 0


def main(argv: Sequence[str] | None = None, output_stream: Any = None) -> int:
    out = output_stream or sys.stdout
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        with BitcaskEngine(file_name=args.db, sync_policy=args.sync_policy) as engine:
            if args.command == "set":
                engine.set(args.key, args.value, ttl_ms=args.ttl)
                out.write("OK\n")
                return 0
            if args.command == "get":
                if not engine.exists(args.key):
                    return 1
                out.write(f"{engine.get(args.key)}\n")
                return 0
            if args.command == "delete":
                deleted = engine.delete(args.key)
                out.write("1\n" if deleted else "0\n")
                return 0 if deleted else 1
            if args.command == "keys":
                if args.prefix:
                    items = [k for k, _ in engine.scan_prefix(args.prefix)]
                else:
                    items = engine.keys()
                for k in items:
                    out.write(f"{k}\n")
                return 0
            if args.command == "scan":
                if args.prefix is not None:
                    pairs = engine.scan_prefix(args.prefix, limit=args.limit)
                else:
                    pairs = engine.scan_range(
                        args.start, args.end, limit=args.limit
                    )
                for k, v in pairs:
                    out.write(f"{k}\t{v}\n")
                return 0
            if args.command == "compact":
                res = engine.compact()
                out.write(f"{json.dumps(res, indent=2, ensure_ascii=False)}\n")
                return 0
            if args.command == "stats":
                out.write(f"{json.dumps(engine.stats(), indent=2, ensure_ascii=False)}\n")
                return 0
            if args.command == "shell":
                return run_repl(engine, output_stream=out)
            if args.command == "serve":
                server = BitcaskHTTPServer((args.host, args.port), engine)
                out.write(f"Serving PyBitcask on http://{args.host}:{args.port}\n")
                out.flush()
                try:
                    server.serve_forever()
                except KeyboardInterrupt:
                    pass
                finally:
                    server.server_close()
                return 0
    except BitcaskError as exc:
        sys.stderr.write(f"Error: {exc}\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
