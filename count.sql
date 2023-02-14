WITH tmp AS (SELECT CAST('i' AS VARCHAR(25)) AS name, (SELECT COUNT(*) FROM tall WHERE i IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE i = 42) AS hits

UNION

SELECT 'tinyint_col' AS name, (SELECT COUNT(*) FROM tall WHERE tinyint_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE tinyint_col = 42) AS hits

UNION

SELECT 'smallint_col' AS name, (SELECT COUNT(*) FROM tall WHERE smallint_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE smallint_col = 42) AS hits

UNION

SELECT 'int_col' AS name, (SELECT COUNT(*) FROM tall WHERE int_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE int_col = 42) AS hits

UNION

SELECT 'bigint_col' AS name, (SELECT COUNT(*) FROM tall WHERE bigint_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE bigint_col = 42) AS hits

UNION

SELECT 'hugeint_col' AS name, (SELECT COUNT(*) FROM tall WHERE hugeint_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE hugeint_col = 42) AS hits

UNION

SELECT 'real_col' AS name, (SELECT COUNT(*) FROM tall WHERE real_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE real_col = 42) AS hits

UNION

SELECT 'double_col' AS name, (SELECT COUNT(*) FROM tall WHERE double_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE double_col = 42) AS hits

UNION

SELECT 'decimal_col' AS name, (SELECT COUNT(*) FROM tall WHERE decimal_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE decimal_col = 42) AS hits

UNION

SELECT 'boolean_col' AS name, (SELECT COUNT(*) FROM tall WHERE boolean_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE boolean_col) AS hits

UNION

SELECT 'text_col' AS name, (SELECT COUNT(*) FROM tall WHERE text_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE LENGTH(text_col) > 4) AS hits

UNION

SELECT 'int_as_text_col' AS name, (SELECT COUNT(*) FROM tall WHERE text_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE LENGTH('xyz' || i) > 4) AS hits

UNION

SELECT 'uuid_col' AS name, (SELECT COUNT(*) FROM tall WHERE uuid_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE uuid_col = UUID '12345678-1234-5678-1234-567812345678') AS hits

UNION

SELECT 'blob_col' AS name, (SELECT COUNT(*) FROM tall WHERE blob_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE LENGTH(blob_col) > 4) AS hits

UNION

SELECT 'date_col' AS name, (SELECT COUNT(*) FROM tall WHERE date_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE EXTRACT(DAY FROM date_col) = 14) AS hits

UNION

SELECT 'time_col' AS name, (SELECT COUNT(*) FROM tall WHERE time_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE EXTRACT(MINUTE FROM time_col) = 42) AS hits

UNION

SELECT 'timetz_col' AS name, (SELECT COUNT(*) FROM tall WHERE timetz_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE EXTRACT(MINUTE FROM timetz_col) = 42) AS hits

UNION

SELECT 'timestamp_col' AS name, (SELECT COUNT(*) FROM tall WHERE timestamp_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE EXTRACT(MINUTE FROM timestamp_col) = 42) AS hits

UNION

SELECT 'timestamptz_col' AS name, (SELECT COUNT(*) FROM tall WHERE timestamptz_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE EXTRACT(MINUTE FROM timestamptz_col) = 42) AS hits

UNION

SELECT 'month_col' AS name, (SELECT COUNT(*) FROM tall WHERE month_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE month_col = 42 * INTERVAL '1' MONTH) AS hits

UNION

SELECT 'sec_col' AS name, (SELECT COUNT(*) FROM tall WHERE sec_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE sec_col = 42 * INTERVAL '1' SECOND) AS hits

UNION

SELECT 'day_col' AS name, (SELECT COUNT(*) FROM tall WHERE day_col IS NULL) as nulls,
(SELECT COUNT(*) FROM tall WHERE day_col = 42 * INTERVAL '1' DAY) AS hits
)
SELECT
    name,
	10 * nulls AS nulls10,
	10 * hits AS hits10,
	20 * nulls AS nulls20,
	20 * hits AS hits20,
	50 * nulls AS nulls50,
	50 * hits AS hits50
FROM tmp;