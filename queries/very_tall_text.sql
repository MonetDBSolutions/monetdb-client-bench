-- Result set with 10 varchar columns
-- @EXPECTED=500000@ @NULLCOUNT=384750@ @HITCOUNT=4614850@

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
FROM very_tall;


