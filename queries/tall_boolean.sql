-- Result set with 10 boolean columns
-- @EXPECTED=100000@

SELECT
	boolean_col AS col0,
	boolean_col AS col1,
	boolean_col AS col2,
	boolean_col AS col3,
	boolean_col AS col4,
	boolean_col AS col5,
	boolean_col AS col6,
	boolean_col AS col7,
	boolean_col AS col8,
	boolean_col AS col9
FROM tall;
