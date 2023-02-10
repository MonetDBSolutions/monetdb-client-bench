START TRANSACTION;

DROP TABLE IF EXISTS tall;
DROP TABLE IF EXISTS very_tall;

CREATE TEMPORARY TABLE nums
AS SELECT CAST(value AS INT) AS i
FROM sys.generate_series(CAST(0 AS INT), 100 * 1000);

UPDATE nums SET i = NULL WHERE (4 * i % 9) - (3 * i % 13) = 0;


CREATE TABLE tall AS SELECT
    i
    , CAST(i % 100 AS TINYINT)                            AS tinyint_col
    , CAST(i % 10000 AS SMALLINT)                         AS smallint_col
    , i                                                   AS int_col
    , CAST(i AS BIGINT)                                   AS bigint_col
    , CAST(i AS HUGEINT)                                  AS hugeint_col
    , CAST(i AS REAL)                                     AS real_col
    , CAST(i AS DOUBLE)                                   AS double_col
    , CAST(i AS DEC(8,3))                                 AS decimal_col
    , i % 2 = 0                                           AS boolean_col
    , CAST('xyz' || i AS VARCHAR(20))                     AS text_col
    , CASE WHEN i IS NULL THEN NULL ELSE CAST('12345678-1234-5678-1234-567812345678' AS UUID) END  AS uuid_col
    , CAST(SUBSTRING('0102030405060708', 0, 2 * i % 16) AS BLOB)  AS blob_col
    , CAST(NOW AS DATE) + i * INTERVAL '1' DAY            AS date_col
    , CAST(NOW AS TIME) + i * INTERVAL '1' MINUTE         AS time_col
    , CAST(NOW AS TIMETZ) + i * INTERVAL '1' MINUTE       AS timetz_col
    , CAST(NOW AS TIMESTAMP) + i * INTERVAL '1' MINUTE    AS timestamp_col
    , CAST(NOW AS TIMESTAMPTZ) + i * INTERVAL '1' MINUTE  AS timestamptz_col
    , i * INTERVAL '1' MONTH                              AS month_col
    , i * INTERVAL '1' SECOND                             AS sec_col
    , i * INTERVAL '1' DAY                                AS day_col
FROM nums;


CREATE TABLE very_tall(LIKE tall);
INSERT INTO very_tall SELECT * FROM tall;
INSERT INTO very_tall SELECT * FROM tall;
INSERT INTO very_tall SELECT * FROM tall;
INSERT INTO very_tall SELECT * FROM tall;
INSERT INTO very_tall SELECT * FROM tall;

COMMIT;
