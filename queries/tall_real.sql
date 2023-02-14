-- Result set with 10 real columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=10@

SELECT
	real_col AS col0,
	real_col AS col1,
	real_col AS col2,
	real_col AS col3,
	real_col AS col4,
	real_col AS col5,
	real_col AS col6,
	real_col AS col7,
	real_col AS col8,
	real_col AS col9
FROM tall;
