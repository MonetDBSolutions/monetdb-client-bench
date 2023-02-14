-- Result set with 10 month columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=10@

SELECT
	month_col AS col0,
	month_col AS col1,
	month_col AS col2,
	month_col AS col3,
	month_col AS col4,
	month_col AS col5,
	month_col AS col6,
	month_col AS col7,
	month_col AS col8,
	month_col AS col9
FROM tall;
