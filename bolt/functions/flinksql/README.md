# Flink scalar functions

Register these functions with `flinksql::registerFunctions(prefix)`. A caller
using multiple dialects should register Flink functions last. Functions with
different semantics from existing Spark or Presto names have explicit native
names:

| Native function | Contract |
| --- | --- |
| `flink_rand([seed])` | Java `Random` sequence; a literal seed retains expression state, while a column seed starts a sequence for each row. |
| `flink_rand_integer(bound[, seed])` | Java bounded random integer. The native argument order is bound, then seed. |
| `flink_split_index(text, separator, index)` | Zero-based index; invalid indexes return NULL. Integer separators are Unicode code points; an empty string separator uses Java whitespace rules. |
| `flink_regexp_replace(text, pattern, replacement)` | Java-style patterns and literal replacement text, including `$` and backslashes. Invalid patterns return NULL. |
| `flink_truncate(value[, scale])` | Numeric truncation toward zero, preserving the input type. |
| `decimal_truncate(value[, scale])` | Decimal truncation with an unchanged precision and scale. |

`regexp_extract` and `icu_regexp_extract` accept constant or column patterns
and group indexes. The former uses RE2 syntax; the latter supports Java regex
features provided by ICU. Invalid patterns, invalid group indexes and unmatched
groups return NULL. Each dynamic-pattern expression retains at most 40 compiled
patterns. Patterns larger than 4 KiB are compiled without entering the cache.

`decimal_round` uses Flink's declared result type. A literal scale can change
precision and scale; a column scale keeps the input decimal type. Plans must
provide that result type in the typed expression. Values that exceed it return
NULL. Both decimal functions accept short and long decimals. Floating-point
TRUNCATE follows Flink's decimal conversion, including DECIMAL(38, 18) for REAL.

`to_timestamp(text, format)` preserves up to nine fractional-second digits.
The shared formatter's existing parsing APIs retain their dialect semantics.

HEX, substring and trim can use existing string kernels. Callers must preserve
the input types and route BOTH, LEADING and TRAILING trim to `trim`, `ltrim` and
`rtrim`, respectively. The two-argument trim kernels take trim characters before
the source string. A numeric cast applied before a string or decimal function
cannot be reversed by the native kernel.
