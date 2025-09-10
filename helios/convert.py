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
)


def convert_path(path_str: str) -> str:
    """
    Orchestrate conversion for a single .sql file.

    한국어 주석: 추출→정리→바인딩(무효)→분할→최소 룰 적용→출력까지 수행합니다.
    Hive-Spark 기준으로 MERGE는 가능한 경우 INSERT OVERWRITE로 완전 변환, 불가 시 스켈레톤 안내,
    UPDATE/DELETE는 보수적으로 실패 마커 처리합니다.
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

    # 5) Minimal rules and Hive constraints
    out_lines: List[str] = []
    for idx, stmt in enumerate(stmts, start=1):
        clean = drop_hints_and_normalize(stmt)
        clean = minimal_safe_rewrites(clean)
        upper = clean.lstrip().upper()

        if upper.startswith("MERGE "):
            full = transform_merge_to_insert_overwrite(clean)
            if full:
                out_lines.append(full.rstrip("\n"))
                continue
            skeleton = try_merge_to_insert_overwrite(clean)
            if skeleton:
                out_lines.append(skeleton.rstrip("\n"))
                continue
            out_lines.append(annotate_failure("MERGE_REWRITE_NEEDED", f"stmt_{idx}"))
            continue

        if upper.startswith("UPDATE ") or upper.startswith("DELETE "):
            out_lines.append(annotate_failure("UNSUPPORTED_DML_FOR_HIVE", f"stmt_{idx}"))
            continue

        if is_hive_unsupported(upper):
            out_lines.append(annotate_failure("UNSUPPORTED_DML_FOR_HIVE", f"stmt_{idx}"))
            continue

        out_lines.append(clean.rstrip(";"))
        out_lines.append(";")

    target_path = source_path.with_name(f"{source_path.stem}_helios.sql")
    output_text = "\n".join(out_lines).rstrip() + ("\n" if out_lines else "")
    target_path.write_text(output_text, encoding="utf-8")

    return f"Wrote converted output: {target_path}"


