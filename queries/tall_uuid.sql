-- Result set with 10 uuid columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=923050@

SELECT
	uuid_col AS col0,
	uuid_col AS col1,
	uuid_col AS col2,
	uuid_col AS col3,
	uuid_col AS col4,
	uuid_col AS col5,
	uuid_col AS col6,
	uuid_col AS col7,
	uuid_col AS col8,
	uuid_col AS col9
FROM tall;
