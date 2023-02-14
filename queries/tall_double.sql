-- Result set with 10 double columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=10@

SELECT
	double_col AS col0,
	double_col AS col1,
	double_col AS col2,
	double_col AS col3,
	double_col AS col4,
	double_col AS col5,
	double_col AS col6,
	double_col AS col7,
	double_col AS col8,
	double_col AS col9
FROM tall;
