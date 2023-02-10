-- Result set with 10 date columns
-- @EXPECTED=100000@

SELECT
	date_col AS col0,
	date_col AS col1,
	date_col AS col2,
	date_col AS col3,
	date_col AS col4,
	date_col AS col5,
	date_col AS col6,
	date_col AS col7,
	date_col AS col8,
	date_col AS col9
FROM tall;
