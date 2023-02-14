-- Result set with 10 int columns
-- @EXPECTED=500000@ @NULLCOUNT=384750@ @HITCOUNT=50@

SELECT
	i AS col0,
	i AS col1,
	i AS col2,
	i AS col3,
	i AS col4,
	i AS col5,
	i AS col6,
	i AS col7,
	i AS col8,
	i AS col9
FROM very_tall;


