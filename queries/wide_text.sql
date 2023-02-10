-- Result set with 20 varchar columns
-- @EXPECTED=100000@

SELECT
	text_col AS col00,
	text_col AS col01,
	text_col AS col02,
	text_col AS col03,
	text_col AS col04,
	text_col AS col05,
	text_col AS col06,
	text_col AS col07,
	text_col AS col08,
	text_col AS col09,
	text_col AS col10,
	text_col AS col11,
	text_col AS col12,
	text_col AS col13,
	text_col AS col14,
	text_col AS col15,
	text_col AS col16,
	text_col AS col17,
	text_col AS col18,
	text_col AS col19
FROM tall;


