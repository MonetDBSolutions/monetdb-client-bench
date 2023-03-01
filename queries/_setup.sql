START TRANSACTION;

DROP TABLE IF EXISTS tall;
DROP TABLE IF EXISTS very_tall;

CREATE TEMPORARY TABLE nums(i INT, idx INT);
INSERT INTO nums
SELECT value AS i, value AS idx
FROM sys.generate_series(CAST(0 AS INT), 100 * 1000);

UPDATE nums SET i = NULL WHERE (4 * i % 9) - (3 * i % 13) = 0;


-- sizes of the blobs we want to create: some small, some large
CREATE TEMPORARY TABLE blobsizes(nr INT, size INT);
INSERT INTO blobsizes VALUES
	(0, 0),
    (1, 128),
    (2, 1024 - 21),
    (3, 1024 - 14),
    (4, 1024 - 7),
    (5, 1024),
    (6, 1024 + 7),
    (7, 1024 + 14),
    (8, 1024 + 21),
    (9, 40000)
	;

CREATE TEMPORARY TABLE repeated(nr INT, size INT, i INT);
INSERT INTO repeated
SELECT
	nr,
	size,
	value
FROM blobsizes LEFT OUTER JOIN sys.generate_series(0,1000000)
ON value < size
;

CREATE TEMPORARY TABLE blobs(nr INT, size INT, b BLOB);
INSERT INTO blobs
SELECT
	nr,
	size,
	CAST(sys.group_concat((CASE WHEN i IS NOT NULL THEN '01' ELSE '' END), '') AS BLOB) AS b
FROM repeated
GROUP BY nr, size
ORDER BY nr
;






CREATE TABLE tall AS SELECT
    idx,
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
    , (SELECT b FROM blobs WHERE idx < 10000 AND nr = i % (SELECT COUNT(*) FROM blobs))  AS bigblob_col
    , DATE '2015-02-14' + i * INTERVAL '1' DAY            AS date_col
    , TIME '20:50:55' + i * INTERVAL '1' MINUTE           AS time_col
    , TIMETZ '20:50:55+01:00' + i * INTERVAL '1' MINUTE   AS timetz_col
    , TIMESTAMP '2015-02-14 20:50:55' + i * INTERVAL '1' MINUTE  AS timestamp_col
    , TIMESTAMPTZ '2015-02-14 20:50:55+01:00' + i * INTERVAL '1' MINUTE  AS timestamptz_col
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
