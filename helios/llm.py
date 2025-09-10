from __future__ import annotations

import os
from typing import Optional

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


class LLMUnavailable(Exception):
    """한국어 주석: LLM 호출 불가 시 사용."""


def _build_prompt(sql: str, provider: str) -> list[dict[str, str]]:
    provider_note = {
        "hive": "Use Spark SQL compatible with Hive metastore tables. Prefer INSERT OVERWRITE patterns for upserts.",
        "delta": "Assume Delta Lake tables are available. You may use MERGE INTO if appropriate.",
        "iceberg": "Assume Apache Iceberg tables are available. You may use MERGE INTO if appropriate.",
    }.get(provider, "Use Spark SQL that runs in spark-sql.")

    system = (
        "You are a precise SQL converter. Convert Oracle SQL into executable Spark SQL only. "
        "Do not output explanations or comments. Keep CTE structure and dependency order. "
        "No PySpark code, no markdown fences."
    )
    user = (
        f"Constraints:\n"
        f"- Output Spark SQL only.\n"
        f"- Preserve CTEs and statement ordering.\n"
        f"- If Oracle-specific constructs exist (e.g., (+), DECODE, date formats), rewrite them.\n"
        f"- {provider_note}\n\n"
        f"Oracle SQL to convert:\n{sql}"
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def convert_oracle_to_spark_sql(sql: str, provider: str = "hive", model: str = "gpt-4o") -> str:
    """
    Convert a single Oracle SQL statement to Spark SQL via OpenAI. Deterministic output.

    한국어 주석: 프롬프트는 영어, 결과는 순수 Spark SQL만 기대합니다.
    """
    if OpenAI is None:
        raise LLMUnavailable("openai client is not available")
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise LLMUnavailable("OPENAI_API_KEY not set")
    client = OpenAI(api_key=api_key)
    messages = _build_prompt(sql, provider)
    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0,
        top_p=1,
    )
    content = resp.choices[0].message.content or ""
    # Strip code fences if any
    if content.startswith("```"):
        content = content.strip().strip("`")
        # remove possible language tag on first line
        if "\n" in content:
            first, rest = content.split("\n", 1)
            if first.strip().isalpha():
                content = rest
    return content.strip()
