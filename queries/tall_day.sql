-- Result set with 10 day columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=10@

SELECT
	day_col AS col0,
	day_col AS col1,
	day_col AS col2,
	day_col AS col3,
	day_col AS col4,
	day_col AS col5,
	day_col AS col6,
	day_col AS col7,
	day_col AS col8,
	day_col AS col9
FROM tall;
