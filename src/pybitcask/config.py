"""Configuration management for PyBitcask."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from pybitcask.exceptions import ConfigurationError

VALID_SYNC_POLICIES: frozenset[str] = frozenset({"always", "batch", "os"})
MAX_UINT32: int = (1 << 32) - 1


@dataclass(slots=True)
class BitcaskConfig:
    """Configuration options for the PyBitcask storage engine.

    Attributes:
        db_path: Directory path (or single file path in legacy mode) for database storage.
        max_segment_size: Maximum size in bytes of an active segment file before rotation.
        max_key_size: Maximum allowed UTF-8 byte length for a key.
        max_value_size: Maximum allowed byte length for a value.
        sync_policy: Durability mode ('always' = fsync every write, 'batch' = periodic fsync,
            'os' = flush to OS buffer and fsync on close/explicit sync).
        sync_interval_records: Number of writes between fsync calls when sync_policy == 'batch'.
        verify_checksum: Whether to verify CRC32 checksum on every read operation.
        recover_tail: Whether to automatically recover/truncate incomplete tail records at startup.
        use_hint_files: Whether to generate and load .hint index files for fast startup.
        skip_values_on_startup: Whether to seek past value bytes instead of reading them during
            startup segment scans (significantly speeds up cold boot).
        cache_capacity: Maximum number of entries in the in-memory LRU read cache (0 disables cache).
        compaction_threshold: Ratio of stale bytes to total bytes (0.0-1.0) to trigger compaction.
        auto_compact: Whether to run automatic compaction on segment rotation when threshold is met.
        log_level: Logging verbosity ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
    """

    db_path: str | Path = "bitcask_data"
    max_segment_size: int = 4 * 1024 * 1024  # 4 MiB
    max_key_size: int = 64 * 1024  # 64 KiB
    max_value_size: int = 64 * 1024 * 1024  # 64 MiB
    sync_policy: str = "os"
    sync_interval_records: int = 128
    verify_checksum: bool = True
    recover_tail: bool = True
    use_hint_files: bool = True
    skip_values_on_startup: bool = True
    cache_capacity: int = 1024
    compaction_threshold: float = 0.5
    auto_compact: bool = False
    log_level: str = "WARNING"

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate all configuration parameters and raise ConfigurationError if invalid."""
        if not self.db_path or str(self.db_path).strip() == "":
            raise ConfigurationError("db_path cannot be empty")
        if "\x00" in str(self.db_path):
            raise ConfigurationError("db_path cannot contain null bytes")
        if self.max_segment_size < 128:
            raise ConfigurationError(
                f"max_segment_size must be at least 128 bytes, got {self.max_segment_size}"
            )
        if not (1 <= self.max_key_size <= MAX_UINT32):
            raise ConfigurationError(
                f"max_key_size must be between 1 and {MAX_UINT32}, got {self.max_key_size}"
            )
        if not (1 <= self.max_value_size <= MAX_UINT32):
            raise ConfigurationError(
                f"max_value_size must be between 1 and {MAX_UINT32}, got {self.max_value_size}"
            )
        if self.sync_policy not in VALID_SYNC_POLICIES:
            raise ConfigurationError(
                f"sync_policy must be one of {sorted(VALID_SYNC_POLICIES)}, got {self.sync_policy!r}"
            )
        if self.sync_interval_records < 1:
            raise ConfigurationError(
                f"sync_interval_records must be >= 1, got {self.sync_interval_records}"
            )
        if self.cache_capacity < 0:
            raise ConfigurationError(
                f"cache_capacity must be >= 0, got {self.cache_capacity}"
            )
        if not (0.0 <= self.compaction_threshold <= 1.0):
            raise ConfigurationError(
                f"compaction_threshold must be between 0.0 and 1.0, got {self.compaction_threshold}"
            )

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to a JSON-compatible dictionary."""
        data = asdict(self)
        data["db_path"] = str(self.db_path)
        return data

    def save_json(self, path: str | Path) -> None:
        """Save configuration to a JSON file."""
        target = Path(path)
        target.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")

    @classmethod
    def from_json(cls, path: str | Path) -> BitcaskConfig:
        """Load configuration from a JSON file."""
        target = Path(path)
        try:
            raw = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ConfigurationError(f"Failed to load config from {target}: {exc}") from exc
        if not isinstance(raw, dict):
            raise ConfigurationError("Config JSON root must be an object")
        try:
            return cls(**raw)
        except TypeError as exc:
            raise ConfigurationError(f"Unknown or invalid configuration fields: {exc}") from exc

    @classmethod
    def from_env(cls, prefix: str = "PYBITCASK_") -> BitcaskConfig:
        """Load configuration overrides from environment variables."""
        kwargs: dict[str, Any] = {}
        env_map: dict[str, tuple[str, Any]] = {
            f"{prefix}DB_PATH": ("db_path", str),
            f"{prefix}MAX_SEGMENT_SIZE": ("max_segment_size", int),
            f"{prefix}MAX_KEY_SIZE": ("max_key_size", int),
            f"{prefix}MAX_VALUE_SIZE": ("max_value_size", int),
            f"{prefix}SYNC_POLICY": ("sync_policy", str),
            f"{prefix}SYNC_INTERVAL_RECORDS": ("sync_interval_records", int),
            f"{prefix}VERIFY_CHECKSUM": ("verify_checksum", lambda v: v.lower() in ("1", "true", "yes")),
            f"{prefix}RECOVER_TAIL": ("recover_tail", lambda v: v.lower() in ("1", "true", "yes")),
            f"{prefix}USE_HINT_FILES": ("use_hint_files", lambda v: v.lower() in ("1", "true", "yes")),
            f"{prefix}CACHE_CAPACITY": ("cache_capacity", int),
            f"{prefix}COMPACTION_THRESHOLD": ("compaction_threshold", float),
            f"{prefix}AUTO_COMPACT": ("auto_compact", lambda v: v.lower() in ("1", "true", "yes")),
            f"{prefix}LOG_LEVEL": ("log_level", str),
        }
        for env_key, (field_name, converter) in env_map.items():
            if env_key in os.environ:
                try:
                    kwargs[field_name] = converter(os.environ[env_key])
                except ValueError as exc:
                    raise ConfigurationError(
                        f"Invalid environment variable {env_key}={os.environ[env_key]!r}: {exc}"
                    ) from exc
        return cls(**kwargs)
