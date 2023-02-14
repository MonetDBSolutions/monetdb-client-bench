-- Result set with 10 hugeint columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=10@

SELECT
	hugeint_col AS col0,
	hugeint_col AS col1,
	hugeint_col AS col2,
	hugeint_col AS col3,
	hugeint_col AS col4,
	hugeint_col AS col5,
	hugeint_col AS col6,
	hugeint_col AS col7,
	hugeint_col AS col8,
	hugeint_col AS col9
FROM tall;
