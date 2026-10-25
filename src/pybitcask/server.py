"""Lightweight HTTP REST/JSON Server for PyBitcask."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from pybitcask.engine import BatchOperation, BitcaskEngine
from pybitcask.exceptions import BitcaskError, ValidationError

DEFAULT_MAX_BODY_BYTES: int = 16 * 1024 * 1024  # 16 MiB


class BitcaskHTTPServer(ThreadingHTTPServer):
    """ThreadingHTTPServer bound to a BitcaskEngine instance."""

    def __init__(
        self,
        server_address: tuple[str, int],
        engine: BitcaskEngine,
        *,
        max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    ) -> None:
        self.engine: BitcaskEngine = engine
        self.max_body_bytes: int = max_body_bytes
        super().__init__(server_address, BitcaskRequestHandler)


class BitcaskRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler exposing REST endpoints for BitcaskEngine."""

    server: BitcaskHTTPServer  # type: ignore[assignment]

    def log_message(self, format: str, *args: Any) -> None:
        self.server.engine.logger.debug(
            "%s - %s", self.address_string(), format % args
        )

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> bytes | None:
        raw_len = self.headers.get("Content-Length")
        if raw_len is None:
            return b""
        try:
            length = int(raw_len)
        except ValueError:
            self._send_json(
                HTTPStatus.BAD_REQUEST, {"error": "Invalid Content-Length header"}
            )
            return None
        if length < 0:
            self._send_json(
                HTTPStatus.BAD_REQUEST, {"error": "Negative Content-Length header"}
            )
            return None
        if length > self.server.max_body_bytes:
            self._send_json(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                {
                    "error": f"Request body exceeds limit of {self.server.max_body_bytes} bytes"
                },
            )
            return None
        return self.rfile.read(length)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        engine = self.server.engine

        try:
            if path == "/health":
                self._send_json(HTTPStatus.OK, {"status": "ok"})
                return
            if path == "/stats":
                self._send_json(HTTPStatus.OK, engine.stats())
                return
            if path == "/keys":
                prefix = query.get("prefix", [""])[0]
                limit_raw = query.get("limit", [None])[0]
                limit = int(limit_raw) if limit_raw is not None else None
                if prefix:
                    pairs = engine.scan_prefix(prefix, limit=limit)
                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "keys": [k for k, _ in pairs],
                            "items": [{"key": k, "value": v} for k, v in pairs],
                        },
                    )
                else:
                    all_keys = engine.keys()
                    if limit is not None and limit >= 0:
                        all_keys = all_keys[:limit]
                    self._send_json(HTTPStatus.OK, {"keys": all_keys})
                return
            if path.startswith("/kv/"):
                key = unquote(path[len("/kv/") :])
                if not key:
                    self._send_json(
                        HTTPStatus.BAD_REQUEST, {"error": "Key cannot be empty"}
                    )
                    return
                if not engine.exists(key):
                    self._send_json(
                        HTTPStatus.NOT_FOUND,
                        {"error": "Key not found", "key": key},
                    )
                    return
                val = engine.get(key)
                self._send_json(HTTPStatus.OK, {"key": key, "value": val})
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Unknown endpoint"})
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except BitcaskError as exc:
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        engine = self.server.engine

        if not path.startswith("/kv/"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Unknown endpoint"})
            return

        key = unquote(path[len("/kv/") :])
        if not key:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Key cannot be empty"})
            return

        body = self._read_body()
        if body is None:
            return

        ttl_ms: int | None = None
        content_type = self.headers.get("Content-Type", "")
        try:
            if "application/json" in content_type:
                payload = json.loads(body.decode("utf-8"))
                if not isinstance(payload, dict) or "value" not in payload:
                    self._send_json(
                        HTTPStatus.BAD_REQUEST,
                        {"error": "JSON body must contain a 'value' field"},
                    )
                    return
                raw_val = payload["value"]
                value = (
                    raw_val
                    if isinstance(raw_val, str)
                    else json.dumps(raw_val, ensure_ascii=False)
                )
                if payload.get("ttl_ms") is not None:
                    ttl_ms = int(payload["ttl_ms"])
            else:
                value = body.decode("utf-8")

            engine.set(key, value, ttl_ms=ttl_ms)
            self._send_json(
                HTTPStatus.OK, {"status": "stored", "key": key}
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, ValidationError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except BitcaskError as exc:
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        engine = self.server.engine

        if not path.startswith("/kv/"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Unknown endpoint"})
            return

        key = unquote(path[len("/kv/") :])
        if not key:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Key cannot be empty"})
            return

        try:
            deleted = engine.delete(key)
            if not deleted:
                self._send_json(
                    HTTPStatus.NOT_FOUND,
                    {"error": "Key not found", "key": key},
                )
                return
            self._send_json(HTTPStatus.OK, {"status": "deleted", "key": key})
        except ValidationError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except BitcaskError as exc:
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        engine = self.server.engine

        try:
            if path == "/compact":
                result = engine.compact()
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/batch":
                body = self._read_body()
                if body is None:
                    return
                payload = json.loads(body.decode("utf-8"))
                if not isinstance(payload, dict) or not isinstance(
                    payload.get("operations"), list
                ):
                    self._send_json(
                        HTTPStatus.BAD_REQUEST,
                        {"error": "Expected JSON object with 'operations' list"},
                    )
                    return
                ops: list[BatchOperation] = []
                for item in payload["operations"]:
                    if not isinstance(item, dict):
                        raise ValidationError("Each batch operation must be an object")
                    ops.append(
                        BatchOperation(
                            op=str(item.get("op", "")),
                            key=str(item.get("key", "")),
                            value=str(item.get("value", "")),
                            ttl_ms=(
                                int(item["ttl_ms"])
                                if item.get("ttl_ms") is not None
                                else None
                            ),
                        )
                    )
                applied = engine.batch_write(ops)
                self._send_json(HTTPStatus.OK, {"status": "ok", "applied": applied})
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Unknown endpoint"})
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, ValidationError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except BitcaskError as exc:
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
