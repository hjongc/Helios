from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import Path
from typing import Dict, List, Optional


class SchemaResolveError(Exception):
    """한국어 주석: 스키마 조회 실패 시 사용."""


def _normalize_table_name(table: str) -> str:
    return table.strip()


def load_cache(cache_path: str) -> Dict[str, List[str]]:
    p = Path(cache_path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache_path: str, cache: Dict[str, List[str]]) -> None:
    Path(cache_path).write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def get_columns_from_cache(table: str, cache: Dict[str, List[str]]) -> Optional[List[str]]:
    key = _normalize_table_name(table)
    cols = cache.get(key)
    if cols and isinstance(cols, list) and all(isinstance(c, str) for c in cols):
        return cols
    return None


def get_columns_via_sparksql(table: str, spark_sql_bin: Optional[str] = None) -> Optional[List[str]]:
    bin_path = spark_sql_bin or os.getenv("SPARK_SQL_BIN", "spark-sql")
    # Use DESCRIBE to get columns (name, type, comment) until a blank line or a section header
    cmd = f"{shlex.quote(bin_path)} -S -e {shlex.quote(f'DESCRIBE {table}')}"
    try:
        proc = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except Exception as exc:  # noqa: BLE001
        return None
    cols: List[str] = []
    for line in proc.stdout.splitlines():
        s = line.strip()
        if not s:
            break
        if s.startswith("#"):
            break
        # Expect "col\tdata_type\tcomment" or with spaces
        parts = [p for p in s.split("\t") if p]
        if not parts:
            parts = s.split()
        if parts:
            col = parts[0].strip()
            # skip partition/date headers if any
            if col.lower() in {"#col_name", "col_name", "partition", "# partition information"}:
                continue
            cols.append(col)
    return cols or None


def resolve_table_columns(
    table: str,
    mode: str = "auto",  # auto|cache|spark-sql
    cache_path: str = "schema_cache.json",
    spark_sql_bin: Optional[str] = None,
) -> List[str]:
    """
    Resolve ordered column names for a table using cache or spark-sql.

    한국어 주석: 우선 캐시 조회, 실패 시 spark-sql로 DESCRIBE 후 캐시에 저장합니다.
    """
    table_key = _normalize_table_name(table)
    cache = load_cache(cache_path)

    if mode in ("auto", "cache"):
        cols = get_columns_from_cache(table_key, cache)
        if cols:
            return cols
        if mode == "cache":
            raise SchemaResolveError(f"Schema not in cache for {table_key}")

    if mode in ("auto", "spark-sql"):
        cols = get_columns_via_sparksql(table_key, spark_sql_bin=spark_sql_bin)
        if cols:
            cache[table_key] = cols
            save_cache(cache_path, cache)
            return cols
        if mode == "spark-sql":
            raise SchemaResolveError(f"spark-sql failed to resolve schema for {table_key}")

    raise SchemaResolveError(f"Could not resolve schema for {table_key}")
