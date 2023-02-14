-- Result set with 10 timestamp columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=15370@

SELECT
	timestamp_col AS col0,
	timestamp_col AS col1,
	timestamp_col AS col2,
	timestamp_col AS col3,
	timestamp_col AS col4,
	timestamp_col AS col5,
	timestamp_col AS col6,
	timestamp_col AS col7,
	timestamp_col AS col8,
	timestamp_col AS col9
FROM tall;
