from __future__ import annotations

from pathlib import Path


def convert_path(path_str: str) -> str:
    """
    Orchestrate conversion for a single .sql file.

    한국어 주석: 현재는 스캐폴드 단계로, 실제 변환 대신 자리표시자 파일을 생성합니다.
    실제 변환 파이프라인(extractor/splitter/rules/llm)은 이후 구현됩니다.
    """
    source_path = Path(path_str)
    if not source_path.exists() or source_path.suffix.lower() != ".sql":
        raise ValueError("Input must be an existing .sql file")

    target_path = source_path.with_name(f"{source_path.stem}_helios.sql")

    placeholder = (
        "-- Helios conversion output (placeholder)\n"
        f"-- Source: {source_path.name}\n"
        "-- Status: Converter scaffolded. See GUIDELINES.md for policies.\n"
    )
    target_path.write_text(placeholder, encoding="utf-8")

    return f"Wrote placeholder output: {target_path}"


