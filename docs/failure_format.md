# Failure Annotation Format

When a chunk cannot be converted safely, write an explicit marker in the output file:

-- HELIOS_FAILURE: <CODE> | reason=<short_reason>; location=<file>:<approx_line>; chunk_id=<id>

Examples:
-- HELIOS_FAILURE: UNSUPPORTED_SEQUENCE | reason=seq.NEXTVAL cannot be mapped; location=source.sql:123; chunk_id=abc123
-- HELIOS_FAILURE: AMBIGUOUS_DATE_FORMAT | reason=TO_DATE format not supported; location=source.sql:456; chunk_id=def456

Rules:
- Keep one-line SQL comments.
- Do not emit partial/heuristic conversions.
- Still produce an _helios.sql file containing all successful chunks and explicit failure markers.
