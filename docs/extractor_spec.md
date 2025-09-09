# Extractor Spec (Draft)

Goal: From company program files (.sql with embedded syntax), safely extract pure SQL blocks for conversion.

- Inputs: raw .sql (UTF-8), may include proprietary delimiters/variables/macros.
- Outputs: ordered list of SQL blocks, preserving original order.
- Must not: change semantics, inline unknown macros, or guess missing values.
- Unknown constructs: mark block as `UNSUPPORTED` with rationale and location.

Pending: refine with concrete patterns once examples are provided.
