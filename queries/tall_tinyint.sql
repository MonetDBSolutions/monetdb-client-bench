-- Result set with 10 tinyint columns
-- @EXPECTED=100000@

SELECT
	tinyint_col AS col0,
	tinyint_col AS col1,
	tinyint_col AS col2,
	tinyint_col AS col3,
	tinyint_col AS col4,
	tinyint_col AS col5,
	tinyint_col AS col6,
	tinyint_col AS col7,
	tinyint_col AS col8,
	tinyint_col AS col9
FROM tall;
