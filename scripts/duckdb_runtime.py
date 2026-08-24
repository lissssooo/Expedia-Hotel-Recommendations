"""Laptop-safe DuckDB runtime configuration shared by project builders."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Mapping


MEMORY_LIMIT_PATTERN = re.compile(r"^[1-9][0-9]*(?:\.[0-9]+)?(?:KB|MB|GB|TB)$", re.I)


def runtime_settings(environment: Mapping[str, str] | None = None) -> tuple[int, str]:
    """Return validated thread and memory limits, using laptop-safe defaults."""
    values = os.environ if environment is None else environment
    raw_threads = values.get("EXPEDIA_DUCKDB_THREADS", "1")
    raw_memory_limit = values.get("EXPEDIA_DUCKDB_MEMORY_LIMIT", "1GB")
    try:
        threads = int(raw_threads)
    except ValueError as exc:
        raise ValueError("EXPEDIA_DUCKDB_THREADS must be a positive integer") from exc
    if threads < 1:
        raise ValueError("EXPEDIA_DUCKDB_THREADS must be a positive integer")
    if not MEMORY_LIMIT_PATTERN.fullmatch(raw_memory_limit):
        raise ValueError(
            "EXPEDIA_DUCKDB_MEMORY_LIMIT must look like 512MB, 1GB, or 1.5GB"
        )
    return threads, raw_memory_limit.upper()


def configure_duckdb(connection: Any, temp_directory: Path) -> None:
    """Apply bounded CPU/RAM use and enable spill-to-disk before heavy SQL."""
    threads, memory_limit = runtime_settings()
    temp_directory.mkdir(parents=True, exist_ok=True)
    temp_path = temp_directory.resolve().as_posix().replace("'", "''")
    connection.execute(f"PRAGMA threads={threads}")
    connection.execute(f"PRAGMA memory_limit='{memory_limit}'")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute(f"PRAGMA temp_directory='{temp_path}'")
