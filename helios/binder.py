from __future__ import annotations

from typing import Optional, Dict


def apply_bindings(sql_text: str, bindings: Optional[Dict[str, str]] = None) -> str:
    """
    Preserve variables like ${VAR} without substitution when bindings is None.

    한국어 주석: 사용자가 바인딩 포맷을 그대로 유지하길 원하므로, 별도 치환 없이 원문을 반환합니다.
    추후 필요 시 bindings가 주어지면 안전한 범위에서만 치환을 고려할 수 있습니다.
    """
    if not bindings:
        return sql_text
    # No substitution for now by policy
    return sql_text
