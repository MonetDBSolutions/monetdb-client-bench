-- Result set with 10 smallint columns
-- @EXPECTED=100000@

SELECT
	smallint_col AS col0,
	smallint_col AS col1,
	smallint_col AS col2,
	smallint_col AS col3,
	smallint_col AS col4,
	smallint_col AS col5,
	smallint_col AS col6,
	smallint_col AS col7,
	smallint_col AS col8,
	smallint_col AS col9
FROM tall;
