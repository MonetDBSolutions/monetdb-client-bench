-- Result set with 10 bigint columns
-- @EXPECTED=100000@

SELECT
	bigint_col AS col0,
	bigint_col AS col1,
	bigint_col AS col2,
	bigint_col AS col3,
	bigint_col AS col4,
	bigint_col AS col5,
	bigint_col AS col6,
	bigint_col AS col7,
	bigint_col AS col8,
	bigint_col AS col9
FROM tall;
