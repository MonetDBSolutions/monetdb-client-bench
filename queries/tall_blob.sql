-- Result set with 10 blob columns
-- @EXPECTED=10000@ @NULLCOUNT=7700@ @HITCOUNT=83090@

SELECT
	blob_col AS col0,
	blob_col AS col1,
	blob_col AS col2,
	blob_col AS col3,
	blob_col AS col4,
	blob_col AS col5,
	blob_col AS col6,
	blob_col AS col7,
	blob_col AS col8,
	blob_col AS col9
FROM tall
WHERE idx < 10000;
