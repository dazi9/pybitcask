"""Integration tests for HTTP REST server and CLI / REPL interface."""

from __future__ import annotations

import io
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from pybitcask.cli import main as cli_main
from pybitcask.cli import run_repl
from pybitcask.engine import BitcaskEngine
from pybitcask.server import BitcaskHTTPServer


class TestHTTPServerIntegration(unittest.TestCase):
    """End-to-end HTTP REST API tests over loopback socket."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmpdir.name) / "http_db"
        self.engine = BitcaskEngine(db_path=self.db_path, max_segment_size=1024)
        self.server = BitcaskHTTPServer(("127.0.0.1", 0), self.engine, max_body_bytes=4096)
        self.port = self.server.server_port
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self._thread.join(timeout=2)
        self.engine.close()
        self._tmpdir.cleanup()

    def _request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, object]]:
        req = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            headers=headers or {},
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            with exc:
                return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_crud_utf8_batch_and_compact_over_http(self) -> None:
        status, data = self._request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")

        # PUT UTF-8 key and JSON value
        encoded_key = urllib.parse.quote("课程:数据结构")
        status, data = self._request(
            "PUT",
            f"/kv/{encoded_key}",
            body=json.dumps({"value": "满绩4.0"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status, 200)

        status, data = self._request("GET", f"/kv/{encoded_key}")
        self.assertEqual(status, 200)
        self.assertEqual(data["value"], "满绩4.0")

        # Batch write
        status, data = self._request(
            "POST",
            "/batch",
            body=json.dumps(
                {
                    "operations": [
                        {"op": "put", "key": "user:1", "value": "A"},
                        {"op": "put", "key": "user:2", "value": "B"},
                    ]
                }
            ).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(data["applied"], 2)

        # Prefix query via /keys
        status, data = self._request("GET", "/keys?prefix=user:")
        self.assertEqual(status, 200)
        self.assertEqual(data["keys"], ["user:1", "user:2"])

        # DELETE existing and non-existing
        status, _ = self._request("DELETE", "/kv/user:1")
        self.assertEqual(status, 200)
        status, _ = self._request("DELETE", "/kv/user:1")
        self.assertEqual(status, 404)

        # Compact and Stats
        status, compact_res = self._request("POST", "/compact")
        self.assertEqual(status, 200)
        self.assertIn("reclaimed_bytes", compact_res)

        status, stats = self._request("GET", "/stats")
        self.assertEqual(status, 200)
        self.assertEqual(stats["live_keys"], 2)

    def test_http_error_and_payload_limit_handling(self) -> None:
        # Missing key -> 404
        status, _ = self._request("GET", "/kv/missing_key")
        self.assertEqual(status, 404)

        # Unknown route -> 404
        status, _ = self._request("GET", "/unknown")
        self.assertEqual(status, 404)

        # Invalid JSON -> 400
        status, _ = self._request(
            "PUT",
            "/kv/k1",
            body=b"{not_json",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(status, 400)

        # Oversized payload (> 4096 bytes) -> 413
        status, _ = self._request(
            "PUT",
            "/kv/big",
            body=b"x" * 5000,
            headers={"Content-Type": "text/plain"},
        )
        self.assertEqual(status, 413)


class TestCLIAndREPL(unittest.TestCase):
    """Tests for CLI subcommands and interactive REPL shell."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmpdir.name) / "cli_db")

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def test_cli_subcommands(self) -> None:
        out = io.StringIO()
        self.assertEqual(
            cli_main(["--db", self.db_path, "set", "lang", "Python3.14"], output_stream=out),
            0,
        )
        self.assertEqual(
            cli_main(["--db", self.db_path, "set", "os", "Linux"], output_stream=out),
            0,
        )

        out = io.StringIO()
        self.assertEqual(
            cli_main(["--db", self.db_path, "get", "lang"], output_stream=out),
            0,
        )
        self.assertEqual(out.getvalue().strip(), "Python3.14")

        out = io.StringIO()
        self.assertEqual(
            cli_main(["--db", self.db_path, "keys"], output_stream=out),
            0,
        )
        self.assertEqual(out.getvalue().strip().splitlines(), ["lang", "os"])

        out = io.StringIO()
        self.assertEqual(
            cli_main(["--db", self.db_path, "scan", "--prefix", "l"], output_stream=out),
            0,
        )
        self.assertEqual(out.getvalue().strip(), "lang\tPython3.14")

        out = io.StringIO()
        self.assertEqual(
            cli_main(["--db", self.db_path, "compact"], output_stream=out),
            0,
        )
        self.assertIn("reclaimed_bytes", json.loads(out.getvalue()))

        out = io.StringIO()
        self.assertEqual(
            cli_main(["--db", self.db_path, "delete", "lang"], output_stream=out),
            0,
        )
        self.assertEqual(
            cli_main(["--db", self.db_path, "get", "lang"], output_stream=out),
            1,
        )

    def test_interactive_repl(self) -> None:
        commands = "\n".join(
            [
                "HELP",
                'SET user:1 "Alice Smith"',
                "GET user:1",
                "EXISTS user:1",
                "KEYS user:",
                "SCAN user:",
                "DEL user:1",
                "GET user:1",
                "STATS",
                "COMPACT",
                "EXIT",
            ]
        ) + "\n"
        inp = io.StringIO(commands)
        out = io.StringIO()
        with BitcaskEngine(db_path=self.db_path) as engine:
            rc = run_repl(engine, input_stream=inp, output_stream=out)
            self.assertEqual(rc, 0)
        transcript = out.getvalue()
        self.assertIn("Alice Smith", transcript)
        self.assertIn("(nil)", transcript)
        self.assertIn("BYE", transcript)


if __name__ == "__main__":
    unittest.main()
