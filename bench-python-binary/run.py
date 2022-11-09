#!/usr/bin/env python3

import sys
sys.path.insert(0, '/home/jvr/src/pymonetdb')

from pymonetdb import types
import pymonetdb
from typing import Any, Optional
import traceback
import time
from threading import Lock, Thread
import re
import io
from ast import List
import array
import sys


# print(pymonetdb.__path__)

COUNT = 0

WRITE_LOCK = Lock()


WIDTH_TO_ARRAY_TYPE = {}
for code in 'bhilq':
    bit_width = 8 * array.array(code).itemsize
    WIDTH_TO_ARRAY_TYPE[bit_width] = code


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

    def copyinto_text(self, ncolumns):
        text = self.text
        mselect = re.search(R'\bselect\b', text, re.IGNORECASE)
        start = mselect.start()
        end = text.find(';', start)

        prefix = 'COPY ('
        suffix = (
            ') INTO NATIVE ENDIAN BINARY '
            + ', '.join(f"'{i}'" for i in range(ncolumns))
            + ' ON CLIENT'
        )

        query = (
            text[:start]
            + prefix
            + text[start:end]
            + suffix
            + text[end:]
        )

        return query


TEXT_TYPES = set([
    types.CHAR,
    types.VARCHAR,
    types.CLOB,
])

INTEGER_TYPES = {
    types.TINYINT: 8,
    types.SMALLINT: 16,
    types.INT: 32,
    types.BIGINT: 64,
    types.HUGEINT: 128,
    types.SERIAL: None,
    types.SHORTINT: None,
    types.MEDIUMINT: None,
    types.LONGINT: None,
}


class ResultProcessor(pymonetdb.Downloader):
    count = 0
    ncolumns: int
    descs: List(pymonetdb.sql.cursors.Description)
    columns: List(List(Any))

    def __init__(self, benchmark: Benchmark, descs, cursor=None):
        self.benchmark = benchmark
        if not descs:
            descs = []
            for col_desc in cursor.description:
                print(col_desc)
                descs.append(col_desc)
        self.descs = descs
        self.checkers = []
        for desc in descs:
            if benchmark.all_text or desc.type_code in TEXT_TYPES:
                checker = self.process_str
            elif desc.type_code in INTEGER_TYPES:
                checker = self.process_int
            else:
                raise Exception(
                    f"Cannot handle column {desc.name} of type {desc.type_code!r}")
            self.checkers.append(checker)
        self.ncolumns = len(self.checkers)

    def clone(self):
        return ResultProcessor(self.benchmark, self.descs)

    def clear(self):
        self.count = 0
        self.columns = [None] * self.ncolumns

    def process_int(self, i):
        if i == 42:
            self.count += 1

    def process_str(self, i):
        if len(i) > 4:
            self.count += 1

    def get(self, row, col):
        v = self.columns[col][row]
        if v == 0x8000_0000:
            v = None
        return v

    def process_binary(self):
        nrows = min(len(c) for c in self.columns)
        for r in range(nrows):
            for c, checker in enumerate(self.checkers):
                val = self.get(r, c)
                if val is not None:
                    checker(val)
                else:
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

    def handle_download(self, download: pymonetdb.Download, filename: str, text_mode: bool):
        n = int(filename)
        r = download.binary_reader()
        desc = self.descs[n]
        if desc.type_code in INTEGER_TYPES:
            col = self.parse_integer_download(desc, r)
        elif desc.type_code in TEXT_TYPES:
            col = self.parse_text_download(desc, r)
        self.columns[n] = col

    def parse_integer_download(self, desc, r):
        width = INTEGER_TYPES[desc.type_code]
        array_type = WIDTH_TO_ARRAY_TYPE[width]
        arr = array.array(array_type)
        #
        done = False
        while not done:
            try:
                arr.fromfile(r, 100_000_000)
            except EOFError:
                done = True
        #
        null_value = 1 << (desc.internal_size - 1)
        values = [v if v != null_value else None for v in arr]
        return values

    def parse_text_download(self, desc, r):
        null_value = b'\x80'
        all = r.read()
        parts = all.split(b'\x00')
        parts.pop() # empty tail element caused by trailing \x00
        values = [str(v, 'utf-8') if v != null_value else None for v in parts]
        return values

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
    text = benchmark.copyinto_text(processor.ncolumns)
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
                conn.set_downloader(processor)
            processor.clear()
            cursor.execute(text)
            processor.process_binary()
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
