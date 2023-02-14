#!/usr/bin/env python3

from dataclasses import dataclass
import dataclasses
import difflib
import sys
from typing import List
import pymonetdb

SETUP_FILE = 'queries/_setup.sql'
TALL_FILE = 'queries/tall_%s.sql'


@dataclass
class ColumnDef:
    typename: str
    gensql: str
    hitsql: str
    colname: str = 'banana'
    nullcount: int = -1
    hitcount: int = -1

    def __post_init__(self):
        if self.colname == 'banana':
            self.colname = self.typename + '_col'


COLUMN_DEFINITIONS: List[ColumnDef] = []


def gen(typename, gensql, hitsql):
    coldef = ColumnDef(typename, gensql, hitsql)
    COLUMN_DEFINITIONS.append(coldef)


gen("tinyint", "CAST(i % 100 AS TINYINT)", "%s = 42")
gen("smallint", "CAST(i % 10000 AS SMALLINT)", "%s = 42")
gen("int", "i", "%s = 42")
gen("bigint", "CAST(i AS BIGINT)", "%s = 42")
gen("hugeint", "CAST(i AS HUGEINT)", "%s = 42")

gen("real", "CAST(i AS REAL)", "%s = 42")
gen("double", "CAST(i AS DOUBLE)", "%s = 42")

gen("decimal", "CAST(i AS DEC(8,3))", "%s = 42")

gen("boolean", "i % 2 = 0", "%s")
gen("text", "CAST('xyz' || i AS VARCHAR(20))", "LENGTH(%s) > 4")
gen(
    "uuid",
    "CASE WHEN i IS NULL THEN NULL ELSE CAST('12345678-1234-5678-1234-567812345678' AS UUID) END",
    "%s = UUID '12345678-1234-5678-1234-567812345678'"
)
gen("blob", "CAST(SUBSTRING('0102030405060708', 0, 2 * i % 16) AS BLOB)", "LENGTH(%s) > 4")

gen("date", "DATE '2015-02-14' + i * INTERVAL '1' DAY", "EXTRACT(DAY FROM %s) = 14")
gen("time", "TIME '20:50:55' + i * INTERVAL '1' MINUTE",
    "EXTRACT(MINUTE FROM %s) = 42")
gen("timetz", "TIMETZ '20:50:55+01:00' + i * INTERVAL '1' MINUTE",
    "EXTRACT(MINUTE FROM %s) = 42")
gen("timestamp", "TIMESTAMP '2015-02-14 20:50:55' + i * INTERVAL '1' MINUTE",
    "EXTRACT(MINUTE FROM %s) = 42")
gen("timestamptz", "TIMESTAMPTZ '2015-02-14 20:50:55+01:00' + i * INTERVAL '1' MINUTE",
    "EXTRACT(MINUTE FROM %s) = 42")


gen("month", "i * INTERVAL '1' MONTH", "%s = 42 * INTERVAL '1' MONTH")
gen("sec", "i * INTERVAL '1' SECOND", "%s > 42 * INTERVAL '1' SECOND")
gen("day", "i * INTERVAL '1' DAY", "%s = 42 * INTERVAL '1' DAY")


def write_file(filename: str, new_content: str, overwrite: bool):
    print(f"FILE {filename}")
    try:
        old_content = open(filename).read()
    except FileNotFoundError:
        old_content = None

    if new_content == old_content:
        print("  -> unchanged")
    elif old_content is None:
        print("  -> new")
    elif overwrite:
        print("  -> overwrite")
    else:
        old_lines = old_content.splitlines()
        new_lines = new_content.splitlines()
        for line in difflib.unified_diff(old_lines, new_lines, 'Old', 'New', lineterm=''):
            print(line)
        return False

    with open(filename, 'w') as f:
        f.write(new_content)
    return True


def gen_setup(setup_file) -> str:
    setup_code = open(setup_file).readlines()
    start_line = None
    end_line = None
    for n, line in enumerate(setup_code):
        line = line.strip()
        if line == 'i':
            start_line = n + 1
        elif line == 'FROM nums;':
            end_line = n
    setup_code[start_line: end_line] = [
        f"    , {coldef.gensql:<50}  AS {coldef.colname}" + "\n"
        for coldef in COLUMN_DEFINITIONS
    ]
    return "".join(setup_code)


def set_counts(conn: pymonetdb.Connection, setup_code: str):
    print("EXECUTING SETUP CODE")
    cursor = conn.cursor()
    cursor.execute(setup_code)

    def run_query(q):
        print(f"EXECUTING {q}: ", end="", flush=True)
        cursor.execute(q)
        res = cursor.fetchone()[0]
        print(res)
        return res

    for coldef in COLUMN_DEFINITIONS:
        nulls = run_query(f"SELECT COUNT(*) FROM tall WHERE {coldef.colname} IS NULL")
        where = coldef.hitsql % coldef.colname
        hits = run_query(f"SELECT COUNT(*) FROM tall WHERE {where}")
        coldef.nullcount = nulls * 10
        coldef.hitcount = hits * 10

TALL_TEMPLATE = """\
-- Result set with 10 %(typename)s columns
-- @EXPECTED=100000@ @NULLCOUNT=%(nullcount)s@ @HITCOUNT=%(hitcount)s@

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


def gen_tall(coldef: ColumnDef) -> str:
    return TALL_TEMPLATE % coldef



if __name__ == "__main__":
    ok = True
    overwrite = ("-w" in sys.argv)
    setup_code = gen_setup(SETUP_FILE)
    ok &= write_file(SETUP_FILE, setup_code, overwrite)

    conn = pymonetdb.connect('foo', autocommit=True)
    set_counts(conn, setup_code)

    for coldef in COLUMN_DEFINITIONS:
        sql = gen_tall(dataclasses.asdict(coldef))
        ok &= write_file(TALL_FILE % coldef.typename, sql, overwrite)

    if not ok:
        sys.exit(1)





# def gen_tall(coldef: ColumnDef) -> str:





# def gen(name, expr, colname=None):
#     if not colname:
#         colname = name + "_col"

#     COLUMN_DEFINITIONS.append(f"    , {expr:<50}  AS {colname}")
#     text = TEMPLATE % dict(name=name, expr=expr, colname=colname)
#     filename = f'queries/tall_{name}.sql'
#     # print(f"WRITE TO {filename!r}")
#     with open(filename, 'w') as f:
#         f.write(text)
