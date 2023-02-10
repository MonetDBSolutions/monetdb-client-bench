#!/usr/bin/env python3

TEMPLATE = """\
-- Result set with 10 %(name)s columns
-- @EXPECTED=100000@

SELECT
	%(colname)s AS col0,
	%(colname)s AS col1,
	%(colname)s AS col2,
	%(colname)s AS col3,
	%(colname)s AS col4,
	%(colname)s AS col5,
	%(colname)s AS col6,
	%(colname)s AS col7,
	%(colname)s AS col8,
	%(colname)s AS col9
FROM tall;
"""

COLUMN_DEFINITIONS = []


def gen(name, expr, colname=None):
    if not colname:
        colname = name + "_col"

    COLUMN_DEFINITIONS.append(f"    , {expr:<50}  AS {colname}")
    text = TEMPLATE % dict(name=name, expr=expr, colname=colname)
    filename = f'queries/tall_{name}.sql'
    # print(f"WRITE TO {filename!r}")
    with open(filename, 'w') as f:
        f.write(text)


gen("tinyint", "CAST(i % 100 AS TINYINT)")
gen("smallint", "CAST(i % 10000 AS SMALLINT)")
gen("int", "i")
gen("bigint", "CAST(i AS BIGINT)")
gen("hugeint", "CAST(i AS HUGEINT)")

gen("real", "CAST(i AS REAL)")
gen("double", "CAST(i AS DOUBLE)")

gen("decimal", "CAST(i AS DEC(8,3))")

gen("boolean", "i % 2 = 0")
gen("text", "'xyz' || i")
gen("uuid", "CAST('12345678-1234-5678-1234-567812345678' AS UUID)")
gen("blob", "CAST(SUBSTRING('0102030405060708', 0, 2 * i % 16) AS BLOB)")

gen("date", "CAST(NOW AS DATE) + i * INTERVAL '1' DAY")
gen("time", "CAST(NOW AS TIME) + i * INTERVAL '1' MINUTE")
gen("timetz", "CAST(NOW AS TIMETZ) + i * INTERVAL '1' MINUTE")
gen("timestamp", "CAST(NOW AS TIMESTAMP) + i * INTERVAL '1' MINUTE")
gen("timestamptz", "CAST(NOW AS TIMESTAMPTZ) + i * INTERVAL '1' MINUTE")
gen("month", "i * INTERVAL '1' MONTH")
gen("sec", "i * INTERVAL '1' SECOND")
gen("day", "i * INTERVAL '1' DAY")



setup_file = 'queries/_setup.sql'

setup_code = open(setup_file).readlines()
start_line = None
end_line = None
for n, line in enumerate(setup_code):
    line = line.strip()
    if line == 'i':
        start_line = n + 1
    elif line == 'FROM nums;':
        end_line = n
setup_code[start_line : end_line] = [
    line + "\n"
    for line in COLUMN_DEFINITIONS
]
with open(setup_file, 'w') as f:
    for line in setup_code:
        f.write(line)
