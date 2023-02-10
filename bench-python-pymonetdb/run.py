#!/usr/bin/env python3

from datetime import date, timedelta
import io
import re
import sys
from threading import Lock, Thread
import time
import traceback
from typing import Optional
from uuid import UUID
import pymonetdb
from pymonetdb import types

ERROR_COUNT = 0

WRITE_LOCK = Lock()


def connect_to(db_url):
    conn = pymonetdb.connect(db_url, autocommit=True)
    return conn


def show_info(dburl=None):
    print("Python version:", sys.version)
    print("pymonetdb path:", pymonetdb.__path__)
    print("pymonetdb version:", pymonetdb.__version__)
    if dburl:
        conn = connect_to(dburl)
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
    expected: Optional[int] = None
    null_count: Optional[int] = None
    hit_count: Optional[int] = None

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
            elif name == "NULLCOUNT":
                self.null_count = int(value)
            elif name == "HITCOUNT":
                self.hit_count = int(value)
            else:
                raise Exception(f"Invalid keyword {m.group(0)}")


class ResultProcessor:
    null_count = 0
    hit_count = 0

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
            if benchmark.all_text:
                checker = self.process_str
            else:
                checker = self.TYPE_MAP.get(type_code)
                if not checker:
                    raise Exception(
                        f"Cannot handle column {name} of type {type_code!r}")
            self.checkers.append(checker)

    def clone(self):
        return ResultProcessor(self.benchmark, self.type_codes)

    def clear(self):
        self.null_count = 0
        self.hit_count = 0

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
                        checker(self, field)
                    else:
                        self.null_count += 1
        bench = self.benchmark
        if bench.expected is not None and rowcount != bench.expected:
            raise Exception(f"Expected row count {bench.expected}, got {rowcount}")
        if bench.hit_count is not None and self.hit_count != bench.hit_count:
            raise Exception(f"Expected hit count {bench.hit_count}, got {self.hit_count}")
        if bench.null_count is not None and self.null_count != bench.null_count:
            raise Exception(f"Expected null count {bench.null_count}, got {self.null_count}")

    def process_num(self, i):
        if i == 42:
            self.hit_count += 1

    def process_str(self, i):
        if len(i) > 4:
            self.hit_count += 1

    def process_bool(self, b):
        if b:
            self.hit_count += 1

    def process_day(self, d: date):
        if d.day == 14:
            self.hit_count += 1

    def process_minute(self, t):
        if t.minute == 42:
            self.hit_count += 1

    def process_timedelta(self, d: timedelta):
        if d.total_seconds() > 42:
            self.hit_count += 1

    reference_uuid = UUID('12345678-1234-5678-1234-567812345678')

    def process_uuid(self, u: UUID):
        if u == self.reference_uuid:
            self.hit_count += 1

    TYPE_MAP = {
        types.TINYINT: process_num,
        types.SMALLINT: process_num,
        types.INT: process_num,
        types.BIGINT: process_num,
        types.HUGEINT: process_num,
        types.SERIAL: process_num,
        types.SHORTINT: process_num,
        types.MEDIUMINT: process_num,
        types.LONGINT: process_num,
        types.REAL: process_num,
        types.DOUBLE: process_num,
        #
        types.DECIMAL: process_num,
        types.BOOLEAN: process_bool,
        types.UUID: process_uuid,
        types.BLOB: process_str,
        #
        types.DATE: process_day,
        types.TIME: process_minute,
        types.TIMETZ: process_minute,
        types.TIMESTAMP: process_minute,
        types.TIMESTAMPTZ: process_minute,
        types.SEC_INTERVAL: process_timedelta,
        types.DAY_INTERVAL: process_num,
        types.MONTH_INTERVAL: process_num,
        #
        types.CHAR: process_str,
        types.VARCHAR: process_str,
        types.CLOB: process_str,
    }


def run_benchmark(db_url, query_file, duration):
    with open(query_file) as f:
        benchmark = Benchmark(f.read())

    # Warmup and retrieve metadata
    conn = connect_to(db_url)
    cursor = conn.cursor()
    cursor.execute(benchmark.text)
    processor = ResultProcessor(benchmark, [], cursor=cursor)
    cursor.close()
    conn.close()

    if duration is None:
        return

    threads = []
    for i in range(benchmark.parallel):
        thread = start_worker(db_url, benchmark, processor.clone(), duration)
        threads.append(thread)
    for thread in threads:
        thread.join()


def start_worker(db_url, benchmark, processor, duration):
    t = Thread(
        daemon=True,
        target=lambda: run_queries(db_url, benchmark, processor, duration))
    t.start()
    return t


def run_queries(db_url, benchmark: Benchmark, processor: ResultProcessor, duration):
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
                conn = connect_to(db_url)
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
        with WRITE_LOCK:
            traceback.print_exc()
            global ERROR_COUNT
            ERROR_COUNT += 1
        sys.exit(1)
    finally:
        with WRITE_LOCK:
            print(out.getvalue(), flush=True, end='')


def usage(msg=None):
    if msg:
        print(msg, file=sys.stderr)
    print("Usage: run.py", file=sys.stderr)
    print("   or: run.py DBURL", file=sys.stderr)
    print("   or: run.py DBURL QUERY_FILE DURATION", file=sys.stderr)
    sys.exit(1)

def main(args):
    argcount = len(args)
    if argcount == 1:
        show_info()
        sys.exit(0)
    elif argcount == 2:
        show_info(args[1])
        sys.exit(0)
    elif len(args) == 4:
        db_url = args[1]
        query_file = args[2]
        try:
            duration = float(args[3])
        except ValueError:
            usage("Error: invalid duration")
    else:
        usage()

    run_benchmark(db_url, query_file, duration)


if __name__ == "__main__":
    main(sys.argv)
    if ERROR_COUNT > 0:
        sys.exit(1)