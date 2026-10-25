"""Custom exception hierarchy for PyBitcask storage engine."""

from __future__ import annotations


class BitcaskError(Exception):
    """Base exception for all PyBitcask errors."""


class CorruptDataError(BitcaskError):
    """Raised when disk records fail CRC32 verification, are truncated, or have invalid encoding."""


class ValidationError(BitcaskError, ValueError):
    """Raised when key/value inputs or paths fail validation checks."""


class ConfigurationError(BitcaskError, ValueError):
    """Raised when BitcaskConfig contains invalid parameter values."""


class DatabaseClosedError(BitcaskError, RuntimeError):
    """Raised when attempting an operation on a closed storage engine."""


class DatabaseLockedError(BitcaskError, RuntimeError):
    """Raised when the database directory is already locked by another active instance."""


class KeyNotFoundError(BitcaskError, KeyError):
    """Raised when a requested key does not exist or has expired."""
