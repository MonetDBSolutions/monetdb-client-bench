-- Result set with 10 int columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=10@

SELECT
	int_col AS col0,
	int_col AS col1,
	int_col AS col2,
	int_col AS col3,
	int_col AS col4,
	int_col AS col5,
	int_col AS col6,
	int_col AS col7,
	int_col AS col8,
	int_col AS col9
FROM tall;
