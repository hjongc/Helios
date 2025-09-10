from __future__ import annotations

from typing import List, Optional, Tuple


class ConversionIssue(Exception):
    """한국어 주석: 변환 실패를 명확히 표현하기 위한 예외."""


def annotate_failure(reason: str, chunk_id: str = "") -> str:
    """한국어 주석: 실패 마커 한 줄을 생성합니다."""
    return f"-- HELIOS_FAILURE: {reason} | chunk_id={chunk_id}"


def is_hive_unsupported(stmt_upper: str) -> bool:
    """
    Return True if the statement uses DML patterns that classic Hive (Spark SQL in Hive-compatible mode)
    cannot guarantee: MERGE, UPDATE, DELETE (without ACID table providers).

    한국어 주석: Hive-ACID 미사용 환경에서는 MERGE/UPDATE/DELETE를 실패 처리합니다.
    """
    if stmt_upper.startswith("MERGE "):
        return True
    if stmt_upper.startswith("UPDATE "):
        return True
    if stmt_upper.startswith("DELETE "):
        return True
    return False


def drop_hints_and_normalize(stmt: str) -> str:
    """
    Remove Oracle hints like /*+ parallel(...) */ safely.

    한국어 주석: 오라클 힌트는 제거합니다.
    """
    out: List[str] = []
    i = 0
    while i < len(stmt):
        if stmt[i : i + 3] == "/*+":
            end = stmt.find("*/", i + 3)
            if end == -1:
                break
            i = end + 2
            continue
        out.append(stmt[i])
        i += 1
    return "".join(out)


# ---------------- Oracle → Spark function rewrites (safe subset) ----------------


def _split_args(arg_str: str) -> List[str]:
    args: List[str] = []
    buf: List[str] = []
    depth = 0
    in_single = False
    i = 0
    while i < len(arg_str):
        ch = arg_str[i]
        if ch == "'":
            # handle escaped '' inside literals
            if in_single and i + 1 < len(arg_str) and arg_str[i + 1] == "'":
                buf.append("''")
                i += 2
                continue
            in_single = not in_single
            buf.append(ch)
            i += 1
            continue
        if not in_single:
            if ch == "(":
                depth += 1
            elif ch == ")":
                if depth > 0:
                    depth -= 1
            elif ch == "," and depth == 0:
                args.append("".join(buf).strip())
                buf = []
                i += 1
                continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        args.append(tail)
    return args


def _find_func_ranges(text: str, func_name_upper: str) -> List[Tuple[int, int, int, int]]:
    """
    Find ranges of function invocations like FUNC_NAME( ... ) in a case-insensitive manner.
    Returns list of tuples (name_start, name_end, lparen_index, rparen_index).

    한국어 주석: 함수 호출의 괄호 범위를 찾아냅니다.
    """
    t_up = text.upper()
    res: List[Tuple[int, int, int, int]] = []
    idx = 0
    while True:
        n = t_up.find(func_name_upper + "(", idx)
        if n == -1:
            break
        lpar = n + len(func_name_upper)
        # find matching right paren
        depth = 0
        in_single = False
        i = lpar
        rpar = -1
        while i < len(text):
            ch = text[i]
            if ch == "'":
                # skip doubled quotes
                if in_single and i + 1 < len(text) and text[i + 1] == "'":
                    i += 2
                    continue
                in_single = not in_single
                i += 1
                continue
            if in_single:
                i += 1
                continue
            if ch == "(":
                depth += 1
            elif ch == ")":
                if depth == 0:
                    rpar = i
                    break
                depth -= 1
            i += 1
        if rpar == -1:
            break
        res.append((n, n + len(func_name_upper), lpar, rpar))
        idx = rpar + 1
    return res


def _replace_ranges(text: str, ranges: List[Tuple[int, int, int, int]], repls: List[str]) -> str:
    out: List[str] = []
    last = 0
    for (name_s, _name_e, lpar, rpar), rep in zip(ranges, repls):
        out.append(text[last:name_s])
        out.append(rep)
        last = rpar + 1
    out.append(text[last:])
    return "".join(out)


def _map_oracle_format_to_spark(fmt: str) -> Optional[str]:
    """Map a subset of Oracle date formats to Spark formats. Return None if unsupported tokens found."""
    # Normalize quotes are already stripped by caller
    # Token replacements (order matters: longer first)
    mapping = [
        ("YYYY", "yyyy"),
        ("YY", "yy"),
        ("HH24", "HH"),
        ("HH12", "hh"),
        ("MI", "mm"),
        ("SS", "ss"),
        ("MM", "MM"),
        ("DD", "dd"),
    ]
    allowed_tokens = {m[0] for m in mapping}
    # Simple scan to validate tokens are from allowed set
    # We'll treat letters-only runs as tokens, ignore separators
    import re

    tokens = re.findall(r"[A-Z]+", fmt.upper())
    for tok in tokens:
        if tok not in allowed_tokens:
            return None
    out = fmt
    for src, dst in mapping:
        out = out.replace(src, dst)
    return out


def _transform_nvl(text: str) -> str:
    ranges = _find_func_ranges(text, "NVL")
    if not ranges:
        return text
    repls: List[str] = []
    for name_s, name_e, lpar, rpar in ranges:
        inner = text[lpar + 1 : rpar]
        args = _split_args(inner)
        if len(args) >= 2:
            repls.append(f"COALESCE({', '.join(args)})")
        else:
            repls.append(text[name_s : rpar + 1])
    return _replace_ranges(text, ranges, repls)


def _transform_decode(text: str) -> str:
    ranges = _find_func_ranges(text, "DECODE")
    if not ranges:
        return text
    repls: List[str] = []
    for name_s, name_e, lpar, rpar in ranges:
        inner = text[lpar + 1 : rpar]
        args = _split_args(inner)
        # DECODE(expr, val1, res1, val2, res2, ..., default)
        if len(args) >= 3:
            expr = args[0]
            pairs = args[1:]
            default = "NULL"
            if len(pairs) % 2 == 1:
                default = pairs[-1]
                pairs = pairs[:-1]
            when_parts = []
            for i in range(0, len(pairs), 2):
                val = pairs[i]
                res = pairs[i + 1]
                when_parts.append(f"WHEN {expr} = {val} THEN {res}")
            case_sql = f"CASE {' '.join(when_parts)} ELSE {default} END"
            repls.append(case_sql)
        else:
            repls.append(text[name_s : rpar + 1])
    return _replace_ranges(text, ranges, repls)


def _transform_to_char(text: str) -> str:
    ranges = _find_func_ranges(text, "TO_CHAR")
    if not ranges:
        return text
    repls: List[str] = []
    for name_s, name_e, lpar, rpar in ranges:
        inner = text[lpar + 1 : rpar]
        args = _split_args(inner)
        if len(args) == 2:
            expr, fmt = args
            fmt_s = fmt.strip()
            if fmt_s.startswith("'") and fmt_s.endswith("'") and len(fmt_s) >= 2:
                raw_fmt = fmt_s[1:-1]
                mapped = _map_oracle_format_to_spark(raw_fmt)
                if mapped is not None:
                    repls.append(f"date_format({expr}, '{mapped}')")
                    continue
        repls.append(text[name_s : rpar + 1])
    return _replace_ranges(text, ranges, repls)


def _transform_to_date(text: str) -> str:
    ranges = _find_func_ranges(text, "TO_DATE")
    if not ranges:
        return text
    repls: List[str] = []
    for name_s, name_e, lpar, rpar in ranges:
        inner = text[lpar + 1 : rpar]
        args = _split_args(inner)
        if len(args) == 2:
            expr, fmt = args
            fmt_s = fmt.strip()
            if fmt_s.startswith("'") and fmt_s.endswith("'") and len(fmt_s) >= 2:
                raw_fmt = fmt_s[1:-1]
                mapped = _map_oracle_format_to_spark(raw_fmt)
                if mapped is not None:
                    repls.append(f"to_date({expr}, '{mapped}')")
                    continue
        repls.append(text[name_s : rpar + 1])
    return _replace_ranges(text, ranges, repls)


def _transform_trunc_date(text: str) -> str:
    # TRUNC(date_expr) → date_trunc('DAY', date_expr)
    ranges = _find_func_ranges(text, "TRUNC")
    if not ranges:
        return text
    repls: List[str] = []
    for name_s, name_e, lpar, rpar in ranges:
        inner = text[lpar + 1 : rpar]
        args = _split_args(inner)
        if len(args) == 1:
            repls.append(f"date_trunc('DAY', {args[0]})")
        else:
            # two-arg TRUNC not supported here
            repls.append(text[name_s : rpar + 1])
    return _replace_ranges(text, ranges, repls)


def _transform_to_date_minus_n(text: str) -> str:
    """Rewrite TO_DATE(x, fmt) - N → date_sub(TO_DATE(x, fmt), N) before other to_date rewrites."""
    t = text
    i = 0
    out: List[str] = []
    while i < len(t):
        up = t.upper()
        n = up.find("TO_DATE(", i)
        if n == -1:
            out.append(t[i:])
            break
        out.append(t[i:n])
        # find matching right paren
        lpar = n + len("TO_DATE")
        depth = 0
        in_single = False
        j = lpar
        rpar = -1
        while j < len(t):
            ch = t[j]
            if ch == "'":
                if in_single and j + 1 < len(t) and t[j + 1] == "'":
                    j += 2
                    continue
                in_single = not in_single
                j += 1
                continue
            if in_single:
                j += 1
                continue
            if ch == "(":
                depth += 1
            elif ch == ")":
                if depth == 0:
                    rpar = j
                    break
                depth -= 1
            j += 1
        if rpar == -1:
            out.append(t[n:])
            break
        # look ahead for  - N
        k = rpar + 1
        while k < len(t) and t[k].isspace():
            k += 1
        if k < len(t) and t[k] == "-":
            k += 1
            while k < len(t) and t[k].isspace():
                k += 1
            num_start = k
            while k < len(t) and t[k].isdigit():
                k += 1
            if num_start < k:
                inner = t[lpar + 1 : rpar]
                num = t[num_start:k]
                out.append(f"date_sub(TO_DATE({inner}), {num})")
                i = k
                continue
        # no match pattern
        out.append(t[n : rpar + 1])
        i = rpar + 1
    return "".join(out)


def minimal_safe_rewrites(stmt: str) -> str:
    """
    Apply safe Oracle→Spark rewrites for common functions and date arithmetic.

    한국어 주석: NVL/DECODE/TO_CHAR/TO_DATE/TRUNC/날짜연산( - N )을 보수적으로 변환합니다.
    """
    s = stmt
    s = _transform_to_date_minus_n(s)
    s = _transform_nvl(s)
    s = _transform_decode(s)
    s = _transform_to_char(s)
    s = _transform_to_date(s)
    s = _transform_trunc_date(s)
    return s


# --- Hive-Spark MERGE transformation (INSERT OVERWRITE recomposition) ---

def _strip_alias(col: str) -> str:
    c = col.strip()
    if "." in c:
        return c.split(".", 1)[1]
    return c


def _parse_merge(stmt: str) -> Optional[dict]:
    up = stmt.upper()
    if not up.startswith("MERGE INTO "):
        return None
    # locate USING
    u_idx = up.find(" USING ")
    if u_idx == -1:
        return None
    into_part = stmt[len("MERGE INTO "):u_idx]
    into_tokens = into_part.strip().split()
    if not into_tokens:
        return None
    target_table = into_tokens[0].strip()
    target_alias = into_tokens[1].strip() if len(into_tokens) > 1 else "A"
    # locate source subquery
    # Expect: USING ( ... ) <alias>
    after_using = stmt[u_idx + len(" USING ") :]
    if not after_using.lstrip().startswith("("):
        return None
    # find matching ) for the first (
    off = after_using.find("(")
    base = after_using[off:]
    depth = 0
    in_single = False
    i = 0
    rpar = -1
    while i < len(base):
        ch = base[i]
        if ch == "'":
            if in_single and i + 1 < len(base) and base[i + 1] == "'":
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue
        if in_single:
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                rpar = i
                break
        i += 1
    if rpar == -1:
        return None
    source_subquery = base[1:rpar]
    rest_after_src = base[rpar + 1 :].lstrip()
    # source alias is next token
    parts = rest_after_src.split(None, 1)
    if not parts:
        return None
    source_alias = parts[0].strip()
    rest = parts[1] if len(parts) > 1 else ""
    # ON ( ... )
    on_up = rest.upper()
    on_idx = on_up.find(" ON ")
    if on_idx == -1:
        return None
    after_on = rest[on_idx + len(" ON ") :].lstrip()
    if not after_on.startswith("("):
        return None
    # find matching )
    depth = 0
    in_single = False
    j = 0
    rpar2 = -1
    while j < len(after_on):
        ch = after_on[j]
        if ch == "'":
            if in_single and j + 1 < len(after_on) and after_on[j + 1] == "'":
                j += 2
                continue
            in_single = not in_single
            j += 1
            continue
        if in_single:
            j += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                rpar2 = j
                break
        j += 1
    if rpar2 == -1:
        return None
    on_condition = after_on[1:rpar2].strip()
    rest2 = after_on[rpar2 + 1 :]
    up2 = rest2.upper()
    # WHEN MATCHED THEN UPDATE SET ...
    wm_idx = up2.find("WHEN MATCHED THEN")
    if wm_idx == -1:
        return None
    after_wm = rest2[wm_idx + len("WHEN MATCHED THEN") :]
    up3 = after_wm.upper()
    us_idx = up3.find("UPDATE SET")
    if us_idx == -1:
        return None
    after_us = after_wm[us_idx + len("UPDATE SET") :]
    # until WHEN NOT MATCHED or end
    up_after_us = after_us.upper()
    wnm_idx = up_after_us.find("WHEN NOT MATCHED")
    updates_blob = after_us if wnm_idx == -1 else after_us[:wnm_idx]
    after_updates = "" if wnm_idx == -1 else after_us[wnm_idx:]
    # parse updates as pairs separated by commas at depth 0
    pairs = _split_args(updates_blob)
    update_map: dict[str, str] = {}
    for p in pairs:
        if not p.strip():
            continue
        # expect like A.COL = expr
        if "=" in p:
            lhs, rhs = p.split("=", 1)
            col = _strip_alias(lhs)
            update_map[col.strip()] = rhs.strip()
    # WHEN NOT MATCHED THEN INSERT (cols) VALUES (values)
    insert_cols: List[str] = []
    insert_vals: List[str] = []
    if wnm_idx != -1:
        after_updates_up = after_us[wnm_idx:]
        # find INSERT
        ins_up = after_updates_up.upper()
        ins_idx = ins_up.find("INSERT")
        if ins_idx != -1:
            after_ins = after_updates_up[ins_idx + len("INSERT") :].lstrip()
            if after_ins.startswith("("):
                # parse cols
                depth = 0
                in_single = False
                i2 = 0
                rpar3 = -1
                while i2 < len(after_ins):
                    ch = after_ins[i2]
                    if ch == "'":
                        if in_single and i2 + 1 < len(after_ins) and after_ins[i2 + 1] == "'":
                            i2 += 2
                            continue
                        in_single = not in_single
                        i2 += 1
                        continue
                    if in_single:
                        i2 += 1
                        continue
                    if ch == "(":
                        depth += 1
                    elif ch == ")":
                        depth -= 1
                        if depth == 0:
                            rpar3 = i2
                            break
                    i2 += 1
                cols_str = after_ins[1:rpar3]
                insert_cols = [
                    _strip_alias(c).strip() for c in _split_args(cols_str)
                ]
                after_cols = after_ins[rpar3 + 1 :]
            else:
                after_cols = after_ins
            vals_up = after_cols.upper()
            v_idx = vals_up.find("VALUES")
            if v_idx != -1:
                after_vals = after_cols[v_idx + len("VALUES") :].lstrip()
                if after_vals.startswith("("):
                    depth = 0
                    in_single = False
                    i3 = 0
                    rpar4 = -1
                    while i3 < len(after_vals):
                        ch = after_vals[i3]
                        if ch == "'":
                            if in_single and i3 + 1 < len(after_vals) and after_vals[i3 + 1] == "'":
                                i3 += 2
                                continue
                            in_single = not in_single
                            i3 += 1
                            continue
                        if in_single:
                            i3 += 1
                            continue
                        if ch == "(":
                            depth += 1
                        elif ch == ")":
                            depth -= 1
                            if depth == 0:
                                rpar4 = i3
                                break
                        i3 += 1
                    vals_str = after_vals[1:rpar4]
                    insert_vals = [v.strip() for v in _split_args(vals_str)]
    # detect (+) near source alias in ON to choose LEFT JOIN
    on_condition_raw = on_condition
    left_join = f"{source_alias}." in on_condition_raw and "(+)" in on_condition_raw
    on_condition = on_condition_raw.replace("(+)", "")
    return {
        "target_table": target_table.strip(),
        "target_alias": target_alias,
        "source_subquery": source_subquery.strip(),
        "source_alias": source_alias,
        "on_condition": on_condition,
        "left_join": left_join,
        "update_map": update_map,
        "insert_cols": insert_cols,
        "insert_vals": insert_vals,
    }


def transform_merge_to_insert_overwrite(stmt: str) -> Optional[str]:
    """
    MERGE INTO ... USING (...) src ON (...) WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT (cols) VALUES (vals)
    → INSERT OVERWRITE TABLE target SELECT * FROM ( updated UNION ALL inserted UNION ALL preserved ) u;

    한국어 주석: INSERT OVERWRITE 재구성. 컬럼 순서는 INSERT 절의 컬럼 순서를 기준으로 합니다.
    업데이트되지 않는 컬럼은 기존 A.col 값을 유지합니다.
    """
    comp = _parse_merge(stmt)
    if not comp or not comp.get("insert_cols") or not comp.get("insert_vals"):
        return None
    tgt = comp["target_table"]
    ta = comp["target_alias"]
    srcq = comp["source_subquery"]
    sa = comp["source_alias"]
    on = comp["on_condition"]
    use_left = bool(comp.get("left_join"))
    upd = comp["update_map"]
    cols = comp["insert_cols"]
    vals = comp["insert_vals"]
    if len(cols) != len(vals):
        return None
    join_kw = "LEFT JOIN" if use_left else "JOIN"
    # updated rows select list
    updated_exprs: List[str] = []
    for c in cols:
        expr = upd.get(c, f"{ta}.{c}")
        updated_exprs.append(f"{expr} AS {c}")
    updated_sql = (
        f"SELECT {', '.join(updated_exprs)} FROM {tgt} {ta} {join_kw} (\n{srcq}\n) {sa} ON ({on})"
    )
    # inserted rows select list (use provided values)
    inserted_pairs = [f"{v} AS {c}" for c, v in zip(cols, vals)]
    inserted_sql = (
        f"SELECT {', '.join(inserted_pairs)} FROM (\n{srcq}\n) {sa} LEFT ANTI JOIN {tgt} {ta} ON ({on})"
    )
    # preserved rows (not updated)
    preserved_exprs = [f"{ta}.{c} AS {c}" for c in cols]
    preserved_sql = (
        f"SELECT {', '.join(preserved_exprs)} FROM {tgt} {ta} LEFT ANTI JOIN (\n{srcq}\n) {sa} ON ({on})"
    )
    final_sql = (
        f"INSERT OVERWRITE TABLE {tgt}\nSELECT * FROM (\n{updated_sql}\nUNION ALL\n{inserted_sql}\nUNION ALL\n{preserved_sql}\n) u"
    )
    return final_sql


# --- Hive-Spark MERGE skeleton (fallback) ---

def try_merge_to_insert_overwrite(stmt: str) -> Optional[str]:
    up = stmt.strip().upper()
    if not up.startswith("MERGE "):
        return None
    return (
        "-- HELIOS_NOTE: Converted MERGE into INSERT OVERWRITE skeleton for Hive-Spark\n"
        "-- Review required: ensure target columns and key semantics are preserved.\n"
        "-- Example pattern:\n"
        "-- INSERT OVERWRITE TABLE <target>\n"
        "-- SELECT * FROM (\n"
        "--   /* when matched: compose updated rows */\n"
        "--   SELECT <updated_columns...> FROM <source> s JOIN <target> t ON <keys>\n"
        "--   UNION ALL\n"
        "--   /* when not matched: insert new rows */\n"
        "--   SELECT <insert_columns...> FROM <source> s LEFT ANTI JOIN <target> t ON <keys>\n"
        "--   UNION ALL\n"
        "--   /* preserved rows not updated */\n"
        "--   SELECT <existing_columns...> FROM <target> t LEFT ANTI JOIN <source> s ON <keys>\n"
        "-- ) u;\n"
    )
