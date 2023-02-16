-- Result set with 10 varchar columns
-- @EXPECTED=100000@ @NULLCOUNT=76950@ @HITCOUNT=830750@

-- @ALL_TEXT@ Even though these are INTEGER results, the client should extract them
-- as strings.
-- We don't include a hit count because not all clients can do that.

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
FROM tall;


