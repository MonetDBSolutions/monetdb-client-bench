-- Result set with 10 timestamptz columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=15370@

SELECT
	timestamptz_col AS col0,
	timestamptz_col AS col1,
	timestamptz_col AS col2,
	timestamptz_col AS col3,
	timestamptz_col AS col4,
	timestamptz_col AS col5,
	timestamptz_col AS col6,
	timestamptz_col AS col7,
	timestamptz_col AS col8,
	timestamptz_col AS col9
FROM tall;
