-- Result set with 10 sec columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=10@

SELECT
	sec_col AS col0,
	sec_col AS col1,
	sec_col AS col2,
	sec_col AS col3,
	sec_col AS col4,
	sec_col AS col5,
	sec_col AS col6,
	sec_col AS col7,
	sec_col AS col8,
	sec_col AS col9
FROM tall;
