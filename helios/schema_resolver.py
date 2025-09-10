from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

import pymysql


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
    cmd = f"{shlex.quote(bin_path)} -S -e {shlex.quote(f'DESCRIBE {table}')}"
    try:
        proc = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except Exception:
        return None
    cols: List[str] = []
    for line in proc.stdout.splitlines():
        s = line.strip()
        if not s:
            break
        if s.startswith("#"):
            break
        parts = [p for p in s.split("\t") if p]
        if not parts:
            parts = s.split()
        if parts:
            col = parts[0].strip()
            if col.lower() in {"#col_name", "col_name", "partition", "# partition information"}:
                continue
            cols.append(col)
    return cols or None


def get_columns_via_mysql(table: str) -> Optional[List[str]]:
    """
    Fetch columns from MySQL metadata. Supports two modes via env:
    - Direct table lookup: HELIOS_META_DB, HELIOS_META_TABLE (columns: table_name, column_name, ordinal)
    - Information_schema: MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE and parse <db.table>

    한국어 주석: 사내 메타 테이블 또는 information_schema에서 컬럼 조회합니다.
    """
    host = os.getenv("MYSQL_HOST")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")

    meta_db = os.getenv("HELIOS_META_DB")
    meta_table = os.getenv("HELIOS_META_TABLE")

    if not host or not user:
        return None
    conn = None
    try:
        conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset="utf8mb4")
        with conn.cursor() as cur:
            if meta_db and meta_table:
                # Expect a custom metadata table with (table_name, column_name, ordinal_position)
                sql = f"SELECT column_name FROM {meta_db}.{meta_table} WHERE table_name=%s ORDER BY ordinal_position"
                cur.execute(sql, (table,))
                rows = cur.fetchall()
                cols = [r[0] for r in rows]
                return cols or None
            # Fallback to information_schema
            if "." in table:
                db, tb = table.split(".", 1)
            else:
                db, tb = database, table
            info_sql = (
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION"
            )
            cur.execute(info_sql, (db, tb))
            rows = cur.fetchall()
            cols = [r[0] for r in rows]
            return cols or None
    except Exception:
        return None
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def resolve_table_columns(
    table: str,
    mode: str = "auto",  # auto|cache|spark-sql|mysql
    cache_path: str = "schema_cache.json",
    spark_sql_bin: Optional[str] = None,
) -> List[str]:
    table_key = _normalize_table_name(table)
    cache = load_cache(cache_path)

    if mode in ("auto", "cache"):
        cols = get_columns_from_cache(table_key, cache)
        if cols:
            return cols
        if mode == "cache":
            raise SchemaResolveError(f"Schema not in cache for {table_key}")

    if mode in ("auto", "mysql"):
        cols = get_columns_via_mysql(table_key)
        if cols:
            cache[table_key] = cols
            save_cache(cache_path, cache)
            return cols
        if mode == "mysql":
            raise SchemaResolveError(f"MySQL metadata failed for {table_key}")

    if mode in ("auto", "spark-sql"):
        cols = get_columns_via_sparksql(table_key, spark_sql_bin=spark_sql_bin)
        if cols:
            cache[table_key] = cols
            save_cache(cache_path, cache)
            return cols
        if mode == "spark-sql":
            raise SchemaResolveError(f"spark-sql failed to resolve schema for {table_key}")

    raise SchemaResolveError(f"Could not resolve schema for {table_key}")
