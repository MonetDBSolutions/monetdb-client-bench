-- Result set with 10 blob columns
-- @EXPECTED=100000@

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
FROM tall;
