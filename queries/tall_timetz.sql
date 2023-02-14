-- Result set with 10 timetz columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=15370@

SELECT
	timetz_col AS col0,
	timetz_col AS col1,
	timetz_col AS col2,
	timetz_col AS col3,
	timetz_col AS col4,
	timetz_col AS col5,
	timetz_col AS col6,
	timetz_col AS col7,
	timetz_col AS col8,
	timetz_col AS col9
FROM tall;
