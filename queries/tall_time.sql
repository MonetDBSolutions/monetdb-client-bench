-- Result set with 10 time columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=15370@

SELECT
	time_col AS col0,
	time_col AS col1,
	time_col AS col2,
	time_col AS col3,
	time_col AS col4,
	time_col AS col5,
	time_col AS col6,
	time_col AS col7,
	time_col AS col8,
	time_col AS col9
FROM tall;
