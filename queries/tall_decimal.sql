-- Result set with 10 decimal columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=10@

SELECT
	decimal_col AS col0,
	decimal_col AS col1,
	decimal_col AS col2,
	decimal_col AS col3,
	decimal_col AS col4,
	decimal_col AS col5,
	decimal_col AS col6,
	decimal_col AS col7,
	decimal_col AS col8,
	decimal_col AS col9
FROM tall;
