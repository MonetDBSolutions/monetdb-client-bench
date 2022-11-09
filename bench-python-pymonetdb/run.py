#!/usr/bin/env python3

import io
import re
import sys
from threading import Lock, Thread
import time
import traceback
from typing import Optional
import pymonetdb
from pymonetdb import types

COUNT = 0

WRITE_LOCK = Lock()


def connect_to(db_url, fetch_size):
    conn = pymonetdb.connect(db_url, autocommit=True)
    conn.set_replysize(fetch_size)
    return conn


def show_info(dburl=None):
    print("Python version:", sys.version)
    print("pymonetdb path:", pymonetdb.__path__)
    print("pymonetdb version:", pymonetdb.__version__)
    if dburl:
        conn = connect_to(dburl, 100)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT value FROM sys.environment WHERE name = 'monet_version'")
        version = cursor.fetchone()[0]
        print("MonetDB version: " + version)


class Benchmark:
    text: str
    use_prepared = False
    reconnect = False
    parallel = 1
    all_text = False
    expected = Optional[int]

    def __init__(self, text):
        self.text = text
        for m in re.finditer("@([A-Za-z0-9]+)(?:=([0-9]+))?@", text):
            name = m.group(1)
            value = m.group(2)
            if name == "PREPARE":
                self.prepare = True
            elif name == "RECONNECT":
                self.reconnect = True
            elif name == "PARALLEL":
                self.parallel = int(value)
            elif name == "ALL_TEXT":
                self.all_text = True
            elif name == "EXPECTED":
                self.expected = int(value)
            else:
                raise Exception(f"Invalid keyword {m.group(0)}")


TEXT_TYPES = set([
    types.CHAR,
    types.VARCHAR,
    types.CLOB,
])

INTEGER_TYPES = set([
    types.TINYINT,
    types.SMALLINT,
    types.INT,
    types.BIGINT,
    types.HUGEINT,
    types.SERIAL,
    types.SHORTINT,
    types.MEDIUMINT,
    types.LONGINT,
])


class ResultProcessor:
    count = 0

    def __init__(self, benchmark: Benchmark, type_codes, cursor=None):
        self.benchmark = benchmark
        if not type_codes:
            type_codes = []
            for col_desc in cursor.description:
                name = col_desc[0]
                type_code = col_desc[1]
                type_codes.append((name, type_code))
        self.type_codes = type_codes
        self.checkers = []
        for name, type_code in type_codes:
            if benchmark.all_text or type_code in TEXT_TYPES:
                checker = self.process_str
            elif type_code in INTEGER_TYPES:
                checker = self.process_int
            else:
                raise Exception(
                    f"Cannot handle column {name} of type {type_code!r}")
            self.checkers.append(checker)

    def clone(self):
        return ResultProcessor(self.benchmark, self.type_codes)

    def clear(self):
        self.count = 0

    def process_int(self, i):
        if i == 42:
            self.count += 1

    def process_str(self, i):
        if len(i) > 4:
            self.count += 1

    def process(self, cursor):
        rowcount = 0
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                rowcount += 1
                for field, checker in zip(row, self.checkers):
                    if field is not None:
                        checker(field)
                    else:
                        self.count += 1
        expected = self.benchmark.expected
        if expected is not None and rowcount != expected:
            raise Exception(f"Expected row count {expected}, got {rowcount}")


def run_benchmark(db_url, query_file, fetch_size, duration):
    with open(query_file) as f:
        benchmark = Benchmark(f.read())

    # Warmup and retrieve metadata
    conn = connect_to(db_url, fetch_size)
    cursor = conn.cursor()
    cursor.execute(benchmark.text)
    processor = ResultProcessor(benchmark, [], cursor=cursor)
    cursor.close()
    conn.close()

    if duration is None:
        return

    threads = []
    for i in range(benchmark.parallel):
        thread = start_worker(
            db_url, benchmark, processor.clone(), fetch_size, duration)
        threads.append(thread)
    for thread in threads:
        thread.join()


def start_worker(db_url, benchmark, processor, fetch_size, duration):
    t = Thread(daemon=True, target=lambda: run_queries(
        db_url, benchmark, processor, fetch_size, duration))
    t.start()
    return t


def run_queries(db_url, benchmark: Benchmark, processor: ResultProcessor, fetch_size, duration):
    text = benchmark.text
    out = io.StringIO()
    try:
        conn = None
        cursor = None
        t0 = time.time()
        deadline = t0 + duration
        while True:
            if benchmark.reconnect and cursor:
                cursor.close()
                conn.close()
                conn = None
                cursor = None
            if not cursor:
                conn = connect_to(db_url, fetch_size)
                cursor = conn.cursor()
            processor.clear()
            cursor.execute(text)
            processor.process(cursor)
            t1 = time.time()
            elapsed = int(1e9 * (t1 - t0))
            print(elapsed, file=out)
            if t1 >= deadline:
                break

        cursor.close()
        conn.close()
    except:
        traceback.print_exc()
        sys.exit(1)
    finally:
        with WRITE_LOCK:
            print(out.getvalue(), flush=True, end='')


if __name__ == "__main__":
    def usage(msg=None):
        if msg:
            print(msg, file=sys.stderr)
        print("Usage: run.py", file=sys.stderr)
        print("   or: run.py DBURL", file=sys.stderr)
        print("   or: run.py DBURL QUERY_FILE FETCH_SIZE DURATION", file=sys.stderr)
        sys.exit(1)
    argcount = len(sys.argv)
    if argcount == 1:
        show_info()
        sys.exit(0)
    elif argcount == 2:
        show_info(sys.argv[1])
        sys.exit(0)
    elif len(sys.argv) == 5:
        db_url = sys.argv[1]
        query_file = sys.argv[2]
        try:
            fetch_size = int(sys.argv[3])
        except ValueError:
            usage("Error: invalid fetch_size")
        try:
            duration = float(sys.argv[4])
        except ValueError:
            usage("Error: invalid duration")
    else:
        usage()

    run_benchmark(db_url, query_file, fetch_size, duration)
