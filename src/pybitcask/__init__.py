"""PyBitcask: Log-Structured Hash Table Key-Value Storage Engine.

A full-featured, zero-dependency Python reproduction and architectural enhancement
of Riak Bitcask and avinassh/py-caskdb.
"""

from __future__ import annotations

from pybitcask.cache import LRUCache
from pybitcask.config import BitcaskConfig
from pybitcask.engine import BatchOperation, BitcaskEngine, DiskStorage
from pybitcask.exceptions import (
    BitcaskError,
    ConfigurationError,
    CorruptDataError,
    DatabaseClosedError,
    DatabaseLockedError,
    KeyNotFoundError,
    ValidationError,
)
from pybitcask.format import (
    HEADER_FORMAT,
    HEADER_SIZE,
    RECORD_HEADER_SIZE,
    DecodedRecord,
    HintRecord,
    KeyEntry,
    decode_header,
    decode_kv,
    decode_record,
    encode_header,
    encode_kv,
    encode_record,
)
from pybitcask.keydir import KeyDir
from pybitcask.memory_store import MemoryStorage

__all__ = [
    "BatchOperation",
    "BitcaskConfig",
    "BitcaskEngine",
    "BitcaskError",
    "ConfigurationError",
    "CorruptDataError",
    "DatabaseClosedError",
    "DatabaseLockedError",
    "DecodedRecord",
    "DiskStorage",
    "HEADER_FORMAT",
    "HEADER_SIZE",
    "HintRecord",
    "KeyDir",
    "KeyEntry",
    "KeyNotFoundError",
    "LRUCache",
    "MemoryStorage",
    "RECORD_HEADER_SIZE",
    "ValidationError",
    "decode_header",
    "decode_kv",
    "decode_record",
    "encode_header",
    "encode_kv",
    "encode_record",
]

__version__ = "0.2.0"
