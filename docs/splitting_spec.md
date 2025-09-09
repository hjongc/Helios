# Splitting Strategy (Draft)

Default: split by semicolons into statements.
Optional: CTE-aware splitting for large WITH chains when safe.

Policies:
- Preserve CTE structure and dependency order exactly.
- Do not duplicate or lift CTEs outside original scope.
- Keep DDL/DML/query types ordered as in source.

Decision will be finalized after examining provided examples.
