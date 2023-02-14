-- Result set with 10 text columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=922970@

SELECT
	text_col AS col0,
	text_col AS col1,
	text_col AS col2,
	text_col AS col3,
	text_col AS col4,
	text_col AS col5,
	text_col AS col6,
	text_col AS col7,
	text_col AS col8,
	text_col AS col9
FROM tall;
