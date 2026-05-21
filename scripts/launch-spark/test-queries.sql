-- Copyright (c) ByteDance Ltd. and/or its affiliates.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Smoke-test queries for Spark + Gluten + Bolt.
-- Usage:
--   ./scripts/launch-spark.sh sql -f scripts/launch-spark/test-queries.sql
-- Compare against vanilla:
--   ./scripts/launch-spark.sh sql --vanilla -f scripts/launch-spark/test-queries.sql
--
-- Each section is followed by an EXPLAIN so you can spot whether Gluten
-- (look for `GlutenColumnar*` / `VeloxColumnar*` / native operator names) is
-- taking over. If a query falls back to vanilla Spark mid-plan, you'll see
-- `RowToColumnar` / `ColumnarToRow` glue.

-- Knobs reasonable for local debugging.
SET spark.sql.adaptive.enabled=true;
SET spark.sql.adaptive.coalescePartitions.enabled=true;

-- =========================================================================
-- 1. Trivial literals (sanity check beeline ↔ thrift wiring)
-- =========================================================================
SELECT 1 AS one, 'hello' AS s, 3.14 AS pi, true AS b, CAST(NULL AS INT) AS n;

-- =========================================================================
-- 2. Range scan + count (cheapest end-to-end columnar path)
-- =========================================================================
SELECT COUNT(*) AS c FROM range(1000000);
EXPLAIN SELECT COUNT(*) FROM range(1000000);

-- =========================================================================
-- 3. Filter + aggregate (exercise pushed-down predicate + columnar agg)
-- =========================================================================
SELECT COUNT(*) AS hits, SUM(id) AS total, AVG(id) AS mean
FROM range(1000000) WHERE id % 7 = 0;
EXPLAIN SELECT COUNT(*), SUM(id), AVG(id) FROM range(1000000) WHERE id % 7 = 0;

-- =========================================================================
-- 4. GROUP BY with sort (HashAgg + Sort exchange)
-- =========================================================================
SELECT id % 10 AS bucket, COUNT(*) AS c, SUM(id) AS s, AVG(id) AS avg_id
FROM range(100000)
GROUP BY id % 10
ORDER BY bucket;
EXPLAIN SELECT id % 10 AS bucket, COUNT(*), SUM(id), AVG(id)
        FROM range(100000) GROUP BY id % 10 ORDER BY bucket;

-- =========================================================================
-- 5. Inner join (BroadcastHashJoin or ShuffledHashJoin)
-- =========================================================================
WITH a AS (SELECT id, id % 10 AS k FROM range(10000)),
     b AS (SELECT id, id % 10 AS k FROM range(10000))
SELECT a.k, COUNT(*) AS c
FROM a JOIN b ON a.k = b.k
GROUP BY a.k
ORDER BY a.k;
EXPLAIN WITH a AS (SELECT id, id % 10 AS k FROM range(10000)),
             b AS (SELECT id, id % 10 AS k FROM range(10000))
        SELECT a.k, COUNT(*) FROM a JOIN b ON a.k = b.k GROUP BY a.k ORDER BY a.k;

-- =========================================================================
-- 6. Left outer join with nulls
-- =========================================================================
WITH a AS (SELECT id AS k FROM range(20)),
     b AS (SELECT id*2 AS k, id*100 AS v FROM range(15))
SELECT a.k, b.v
FROM a LEFT JOIN b ON a.k = b.k
ORDER BY a.k;

-- =========================================================================
-- 7. Window functions (ROW_NUMBER, RANK, partition AVG)
-- =========================================================================
SELECT id, id % 10 AS bucket,
       ROW_NUMBER() OVER (PARTITION BY id % 10 ORDER BY id) AS rn,
       RANK()       OVER (PARTITION BY id % 10 ORDER BY id DESC) AS r,
       AVG(id)      OVER (PARTITION BY id % 10) AS bucket_avg
FROM range(50)
ORDER BY id
LIMIT 20;
EXPLAIN SELECT id, ROW_NUMBER() OVER (PARTITION BY id % 10 ORDER BY id) AS rn
        FROM range(50);

-- =========================================================================
-- 8. String functions (concat, substr, upper, length, lpad, regex)
-- =========================================================================
SELECT id,
       CONCAT('row-', CAST(id AS STRING)) AS name,
       UPPER(SUBSTRING(CONCAT('row-', CAST(id AS STRING)), 1, 5)) AS prefix,
       LENGTH(CONCAT('row-', CAST(id AS STRING))) AS len,
       LPAD(CAST(id AS STRING), 4, '0') AS padded,
       regexp_replace(CONCAT('a', CAST(id AS STRING), 'b'), '[0-9]+', '#') AS scrubbed
FROM range(10)
ORDER BY id;

-- =========================================================================
-- 9. Cast / numeric (int↔string↔double, decimals, overflow)
-- =========================================================================
SELECT id,
       CAST(id AS STRING) AS s,
       CAST(id AS DOUBLE) / 3 AS d,
       CAST('42' AS INT) AS forty_two,
       CAST(123.456 AS DECIMAL(10,2)) AS dec_val,
       id * 1.0 / NULLIF(id % 3, 0) AS may_be_null
FROM range(5)
ORDER BY id;

-- =========================================================================
-- 10. Date / time arithmetic
-- =========================================================================
SELECT DATE '2026-05-12' AS d,
       TIMESTAMP '2026-05-12 10:30:00' AS ts,
       YEAR(DATE '2026-05-12') AS y,
       MONTH(DATE '2026-05-12') AS m,
       DAY(DATE '2026-05-12') AS day_,
       DATE_ADD(DATE '2026-05-12', 30) AS d_plus_30,
       DATEDIFF(DATE '2026-06-01', DATE '2026-05-12') AS days_between;

-- =========================================================================
-- 11. Complex types (array, map, struct, element access, explode)
-- =========================================================================
SELECT array(1, 2, 3) AS arr,
       size(array(1, 2, 3)) AS arr_len,
       array(1, 2, 3)[1] AS arr_at_1,
       map('a', 1, 'b', 2) AS m,
       map('a', 1, 'b', 2)['a'] AS m_at_a,
       struct(1 AS x, 'hi' AS y) AS s;

SELECT id, item
FROM (SELECT id, array(id, id*2, id*3) AS items FROM range(3))
LATERAL VIEW explode(items) t AS item
ORDER BY id, item;

-- =========================================================================
-- 12. DISTINCT + LIMIT + ORDER BY
-- =========================================================================
SELECT DISTINCT id % 100 AS k FROM range(100000) ORDER BY k LIMIT 10;

-- =========================================================================
-- 13. Subquery in WHERE (scalar subquery)
-- =========================================================================
SELECT id FROM range(100)
WHERE id > (SELECT AVG(id) FROM range(100))
ORDER BY id LIMIT 5;

-- =========================================================================
-- 14. EXISTS / IN subquery (correlated)
-- =========================================================================
WITH big AS (SELECT id FROM range(50)),
     small AS (SELECT id FROM range(10))
SELECT big.id FROM big
WHERE big.id IN (SELECT id FROM small)
ORDER BY big.id;

-- =========================================================================
-- 15. UNION ALL + UNION
-- =========================================================================
SELECT id, 'a' AS src FROM range(5)
UNION ALL
SELECT id, 'b' AS src FROM range(5)
ORDER BY id, src;

SELECT id FROM range(5)
UNION
SELECT id FROM range(3, 8)
ORDER BY id;

-- =========================================================================
-- 16. CASE WHEN / COALESCE / NULLIF / IF
-- =========================================================================
SELECT id,
       CASE WHEN id < 3 THEN 'low'
            WHEN id < 7 THEN 'mid'
            ELSE 'high' END AS bucket,
       COALESCE(NULLIF(id, 0), -1) AS safe_id,
       IF(id % 2 = 0, 'even', 'odd') AS parity
FROM range(10)
ORDER BY id;

-- =========================================================================
-- 17. Approx + percentile aggregates (often have native paths)
--     NOTE: results from approx_count_distinct / percentile_approx are
--     allowed to differ between implementations. Diff exact values at your
--     own risk; for cross-engine equivalence checks, compare to the exact
--     ground truth (COUNT(DISTINCT ...), PERCENTILE) within a tolerance.
-- =========================================================================
SELECT
  approx_count_distinct(id % 100) AS approx_card,
  percentile_approx(id, 0.5) AS p50,
  percentile_approx(id, ARRAY(0.5, 0.9, 0.99)) AS p_multi
FROM range(100000);

-- =========================================================================
-- 18. CTE + temp view roundtrip
-- =========================================================================
CREATE OR REPLACE TEMP VIEW v AS
SELECT id, id % 7 AS k, CAST(id AS STRING) AS s FROM range(10000);

SELECT k, COUNT(*) AS c, MAX(LENGTH(s)) AS max_len
FROM v GROUP BY k ORDER BY k;

DROP VIEW v;

-- =========================================================================
-- 19. Large sort (stresses native sort + shuffle).
--     Secondary key `id` is required: the hash collides ~10× per bucket, and
--     LIMIT 10 over tied rows would otherwise be nondeterministic across
--     engines and produce false Bolt-vs-vanilla diffs.
-- =========================================================================
SELECT id, (id * 2654435761) % 10000 AS h
FROM range(100000)
ORDER BY h DESC, id ASC
LIMIT 10;

-- =========================================================================
-- 20. Final sanity: confirm Gluten is on (or off, if --vanilla)
-- =========================================================================
SET spark.gluten.enabled;
SET spark.plugins;
SET spark.shuffle.manager;
