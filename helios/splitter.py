from __future__ import annotations

from typing import List


def split_sql_statements(sql_text: str) -> List[str]:
    """
    Split SQL text by semicolons while respecting single-quoted string literals and
    line comments starting with --.

    한국어 주석: 세미콜론 기준 분할. 문자열/라인 주석 내의 세미콜론은 무시합니다.
    """
    statements: List[str] = []
    buf: list[str] = []
    in_single = False
    i = 0
    while i < len(sql_text):
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < len(sql_text) else ""
        # Handle start of line comment
        if not in_single and ch == "-" and nxt == "-":
            # consume until end of line
            newline_idx = sql_text.find("\n", i)
            if newline_idx == -1:
                buf.append(sql_text[i:])
                break
            buf.append(sql_text[i:newline_idx + 1])
            i = newline_idx + 1
            continue
        # Toggle single-quote string
        if ch == "'":
            in_single = not in_single
            buf.append(ch)
            i += 1
            continue
        if ch == ";" and not in_single:
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements
