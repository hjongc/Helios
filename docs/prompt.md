# LLM Prompt Template (Draft)

- Language: English
- Determinism: temperature 0, top_p 1
- Output: Spark SQL only; no PySpark; no commentary
- Preserve CTE structure and dependency order
- Fail explicitly on unsupported/ambiguous Oracle features
- Do not alter semantics for readability/performance

Chunking: aim 8k–12k tokens per request including prompt + context; keep ~30% headroom.
