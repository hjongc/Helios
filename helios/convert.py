from __future__ import annotations

from pathlib import Path
from typing import List

from .extractor import extract_here_doc_sql, drop_diagnostics
from .binder import apply_bindings
from .splitter import split_sql_statements
from .rules import (
    is_hive_unsupported,
    drop_hints_and_normalize,
    minimal_safe_rewrites,
    annotate_failure,
    try_merge_to_insert_overwrite,
    transform_merge_to_insert_overwrite,
    transform_old_outer_join_simple,
    transform_delete_to_insert_overwrite,
    transform_update_to_insert_overwrite,
)
from .llm import convert_oracle_to_spark_sql, LLMUnavailable
from .schema_resolver import resolve_table_columns, SchemaResolveError


def convert_path(path_str: str, use_llm: bool = True, provider: str = "hive", schema_mode: str = "auto", schema_cache: str = "schema_cache.json") -> str:
    """
    Orchestrate conversion for a single .sql file.

    한국어 주석: 추출→정리→바인딩(무효)→분할→룰→(선택)LLM→출력. 
    MERGE/DELETE/UPDATE는 가능 시 INSERT OVERWRITE로 재작성, 불가 시 LLM 보조, 최종 실패 마커.
    """
    source_path = Path(path_str)
    if not source_path.exists() or source_path.suffix.lower() != ".sql":
        raise ValueError("Input must be an existing .sql file")

    raw_text = source_path.read_text(encoding="utf-8")

    # 1) Extract here-doc SQL if present; else use full text
    extracted, found = extract_here_doc_sql(raw_text)
    sql_text = extracted if found else raw_text

    # 2) Drop diagnostics
    sql_text = drop_diagnostics(sql_text)

    # 3) Apply bindings (noop per policy)
    sql_text = apply_bindings(sql_text, bindings=None)

    # 4) Split into statements
    stmts: List[str] = split_sql_statements(sql_text)

    # 5) Rules + optional LLM
    out_lines: List[str] = []
    for idx, stmt in enumerate(stmts, start=1):
        clean = drop_hints_and_normalize(stmt)
        clean = minimal_safe_rewrites(clean)
        upper = clean.lstrip().upper()

        # MERGE handling
        if upper.startswith("MERGE "):
            full = transform_merge_to_insert_overwrite(clean)
            if full:
                out_lines.append(full.rstrip("\n"))
                continue
            skeleton = try_merge_to_insert_overwrite(clean)
            if skeleton:
                out_lines.append(skeleton.rstrip("\n"))
                continue
            if use_llm:
                try:
                    converted = convert_oracle_to_spark_sql(clean, provider=provider)
                    out_lines.append(converted.rstrip("\n"))
                    continue
                except LLMUnavailable:
                    pass
            out_lines.append(annotate_failure("MERGE_REWRITE_NEEDED", f"stmt_{idx}"))
            continue

        # DELETE: try rule rewrite, fallback to LLM, else fail
        if upper.startswith("DELETE "):
            rewritten = transform_delete_to_insert_overwrite(clean)
            if rewritten:
                out_lines.append(rewritten.rstrip("\n"))
                continue
            if use_llm:
                try:
                    converted = convert_oracle_to_spark_sql(clean, provider=provider)
                    out_lines.append(converted.rstrip("\n"))
                    continue
                except LLMUnavailable:
                    pass
            out_lines.append(annotate_failure("UNSUPPORTED_DML_FOR_HIVE", f"stmt_{idx}"))
            continue

        # UPDATE: attempt rule rewrite with schema; fallback to LLM, else fail
        if upper.startswith("UPDATE "):
            # resolve table and columns
            # naive parse to get target table
            import re as _re
            m = _re.match(r"UPDATE\s+([A-Za-z_][\w\.$]*)\b", clean, flags=_re.IGNORECASE)
            columns: List[str] = []
            if m:
                table = m.group(1)
                try:
                    columns = resolve_table_columns(table, mode=schema_mode, cache_path=schema_cache)
                except SchemaResolveError:
                    columns = []
            if columns:
                rewritten = transform_update_to_insert_overwrite(clean, columns)
                if rewritten:
                    out_lines.append(rewritten.rstrip("\n"))
                    continue
            if use_llm:
                try:
                    converted = convert_oracle_to_spark_sql(clean, provider=provider)
                    out_lines.append(converted.rstrip("\n"))
                    continue
                except LLMUnavailable:
                    pass
            out_lines.append(annotate_failure("UNSUPPORTED_DML_FOR_HIVE", f"stmt_{idx}"))
            continue

        if is_hive_unsupported(upper):
            if use_llm:
                try:
                    converted = convert_oracle_to_spark_sql(clean, provider=provider)
                    out_lines.append(converted.rstrip("\n"))
                    continue
                except LLMUnavailable:
                    pass
            out_lines.append(annotate_failure("UNSUPPORTED_DML_FOR_HIVE", f"stmt_{idx}"))
            continue

        # Plain SELECT/INSERT etc.: normalize old-style outer joins if present
        clean = transform_old_outer_join_simple(clean)

        out_lines.append(clean.rstrip(";"))
        out_lines.append(";")

    target_path = source_path.with_name(f"{source_path.stem}_helios.sql")
    output_text = "\n".join(out_lines).rstrip() + ("\n" if out_lines else "")
    target_path.write_text(output_text, encoding="utf-8")

    return f"Wrote converted output: {target_path}"


