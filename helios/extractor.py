from __future__ import annotations

from typing import Tuple


def extract_here_doc_sql(file_text: str) -> Tuple[str, bool]:
    """
    Extract SQL content from a shell script using a here-doc block that starts with
    a line containing '<<!' and ends with a line that is exactly '!'.

    Returns (sql_text, found).

    한국어 주석: 쉘 스크립트에서 here-doc 구간만 추출합니다.
    """
    lines = file_text.splitlines()
    in_block = False
    buf: list[str] = []
    for line in lines:
        if not in_block:
            if "<<!" in line:
                in_block = True
            continue
        # in here-doc block
        if line.strip() == "!":
            # block ends
            break
        buf.append(line)
    if not buf:
        return "", False
    return "\n".join(buf) + "\n", True


def drop_diagnostics(sql_text: str) -> str:
    """
    Remove diagnostics and control statements commonly found in scripts,
    such as SELECT 'Sub ...' FROM DUAL; COMMIT; EXIT;

    한국어 주석: 진단용 SELECT/DUAL, commit, exit 등은 제거합니다.
    """
    out_lines: list[str] = []
    for raw in sql_text.splitlines():
        line = raw.strip()
        upper = line.upper()
        if not line:
            continue
        # Drop diagnostics like: SELECT 'Sub X start ...' FROM DUAL ;
        if upper.startswith("SELECT '") and "FROM DUAL" in upper:
            continue
        # Drop COMMIT / EXIT
        if upper == "COMMIT;" or upper == "COMMIT":
            continue
        if upper == "EXIT;" or upper == "EXIT":
            continue
        out_lines.append(raw)
    return "\n".join(out_lines) + ("\n" if out_lines else "")
