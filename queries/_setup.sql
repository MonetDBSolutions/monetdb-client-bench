START TRANSACTION;

DROP TABLE IF EXISTS tall;
DROP TABLE IF EXISTS very_tall;

CREATE TABLE tall(i INT, t VARCHAR(15));
INSERT INTO tall SELECT
	value AS i,
	'xyz' || value AS t
FROM
	sys.generate_series(CAST(0 AS INT), 100 * 1000)
;

UPDATE tall
SET i = NULL, t = NULL
WHERE (4 * i % 9) - (3 * i % 13) = 0;

CREATE TABLE very_tall
AS SELECT i, t
FROM tall, sys.generate_series(0, 5);

COMMIT;