-- Direct path access: no CREATE EXTERNAL TABLE is required.
SELECT *
FROM 'kernel/tests/data/table-without-dv-small'
WHERE value >= 5;

-- delta_table() and delta_scan() are equivalent explicit table-function forms.
SELECT *
FROM delta_table('kernel/tests/data/table-without-dv-small')
WHERE value >= 5;

-- Metadata-only execution returns one row per live Delta AddFile.
SELECT path,
       size,
       "deletionVector" IS NOT NULL AS has_deletion_vector
FROM delta_metadata('kernel/tests/data/table-with-dv-small')
ORDER BY path;

-- EXPLAIN shows the metadata plan without executing it.
EXPLAIN
SELECT path, size
FROM delta_metadata('kernel/tests/data/basic_partitioned');

-- EXPLAIN ANALYZE executes that plan and adds rows and CPU time to each operator.
EXPLAIN ANALYZE
SELECT path, size
FROM delta_metadata('kernel/tests/data/basic_partitioned');

-- Normal DataFusion SQL works directly against a Delta path.
SELECT letter, count(*) AS rows, avg(number) AS avg_number
FROM 'kernel/tests/data/basic_partitioned'
GROUP BY letter
ORDER BY letter;

EXPLAIN ANALYZE
SELECT letter, count(*) AS rows, avg(number) AS avg_number
FROM 'kernel/tests/data/basic_partitioned'
WHERE number >= 2
GROUP BY letter
ORDER BY letter;

-- The source contains 10 physical rows. Kernel applies its deletion vector and exposes 8.
SELECT count(*) AS visible_rows, min(value), max(value)
FROM 'kernel/tests/data/table-with-dv-small';

EXPLAIN ANALYZE
SELECT count(*) AS visible_rows
FROM 'kernel/tests/data/table-with-dv-small';

-- Direct-path Delta relations can participate in joins.
SELECT n.value,
       d.value IS NOT NULL AS visible_after_deletes
FROM 'kernel/tests/data/table-without-dv-small' AS n
LEFT JOIN 'kernel/tests/data/table-with-dv-small' AS d
  ON n.value = d.value
ORDER BY n.value;
