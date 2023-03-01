-- Result set with 10 bigblob columns
-- @EXPECTED=10000@ @NULLCOUNT=7700@ @HITCOUNT=83090@

SELECT
	bigblob_col AS col0,
	bigblob_col AS col1,
	bigblob_col AS col2,
	bigblob_col AS col3,
	bigblob_col AS col4,
	bigblob_col AS col5,
	bigblob_col AS col6,
	bigblob_col AS col7,
	bigblob_col AS col8,
	bigblob_col AS col9
FROM tall
WHERE idx < 10000;
