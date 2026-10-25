"""Structured logger configuration for PyBitcask."""

from __future__ import annotations

import logging


def get_logger(name: str = "pybitcask", level: str = "WARNING") -> logging.Logger:
    """Create or retrieve a configured logger without duplicate handlers."""
    logger = logging.getLogger(name)
    numeric_level = getattr(logging, level.upper(), logging.WARNING)
    logger.setLevel(numeric_level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
    return logger
