#!/usr/bin/env python3

import sys
import time
import pymonetdb

COUNT = 0


def show_info():
    print("Python version:", sys.version)
    print("pymonetdb path:", pymonetdb.__path__)
    print("pymonetdb version:", pymonetdb.__version__)


def process_resultset(cursor, checkers):
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        for row in rows:
            for field, checker in zip(row, checkers):
                checker(field)


def run_benchmark(db_url, query_file, duration):
    with open(query_file) as f:
        query = f.read()

    conn = pymonetdb.connect(db_url, autocommit=True)
    cursor = conn.cursor()

    # Warmup and retrieve metadata
    cursor.execute(query)

    def process_int(i):
        if i == 42:
            global COUNT
            COUNT += 1

    def process_str(i):
        if len(i) > 4:
            global COUNT
            COUNT += 1

    # intended for setup.sql
    if duration is None:
        return

    checkers = []
    for col_desc in cursor.description:
        name = col_desc[0]
        type_code = col_desc[1]
        if type_code == pymonetdb.types.INT:
            checker = process_int
        elif type_code == pymonetdb.STRING:
            checker = process_str
        else:
            raise Exception(
                f"Cannot handle column {name} of type {type_code!r}")
        checkers.append(checker)

    cursor.close()

    # Run the query
    cursor = conn.cursor()
    deadline = time.time() + duration if duration > 0 else 0
    t0 = time.time()
    while time.time() < deadline:
        cursor.execute(query)
        process_resultset(cursor, checkers)
        t1 = time.time()
        elapsed = int(1e9 * (t1 - t0))
        print(elapsed, flush=False)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        show_info()
        sys.exit(0)
    if len(sys.argv) >= 3:
        db_url = sys.argv[1]
        query_file = sys.argv[2]
    if len(sys.argv) >= 4:
        duration = float(sys.argv[3])
    else:
        duration = None

    run_benchmark(db_url, query_file, duration)
