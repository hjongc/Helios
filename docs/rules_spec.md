# Oracle→Spark Rules (Draft)

Principle: apply only correctness-safe rewrites; fail on ambiguous/lossy cases.

Initial safe mappings:
- NVL → COALESCE
- DECODE → CASE WHEN
- SYSDATE → current_timestamp
- TRUNC(date) → date_trunc (Spark-supported patterns only)
- ROWNUM → ROW_NUMBER() OVER (...)
- TO_CHAR/TO_DATE: convert only with supported formats
- PIVOT/UNPIVOT: only if Spark semantics match exactly
- Sequences (seq.NEXTVAL): fail unless a precise equivalent is defined
- Hints: drop only when semantics are unaffected

Top-10 priority list: to be finalized after reviewing examples.
