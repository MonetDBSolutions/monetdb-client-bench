#!/usr/bin/env python3

# This script has become a bit of a beast.
# That's the nature of this sort of thing.


import argparse
from contextlib import redirect_stdout
import difflib
from glob import glob
import io
import numpy
import os
from os.path import join
import re
import shlex
import subprocess
import sys
import time
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import pymonetdb

BENCHMARK_VERSION = "0.2.0"

HERE = os.path.dirname(sys.argv[0]) or "."


# This class is a work in progress.
class DBSpec:
    """Convert between pymonetdb, jdbc and libmapi URLs and database names"""

    BARE_PATTERN = r'^[A-Za-z0-9_]+$'

    def __init__(self, name: str):
        if name.startswith('mapi:monetdb://'):
            dburl = name[5:]
        elif name.startswith('jdbc:monetdb:'):
            dburl = name[5:]
        elif re.match(self.BARE_PATTERN, name):
            dburl = f"monetdb://localhost/{name}"
        else:
            raise ValueError("invalid dbspec")
        spec = urlsplit(dburl)
        self.username = spec.username
        self.password = spec.password
        self.hostname = spec.hostname
        self.port = spec.port
        self.database = spec.path[1:] if spec.path else None
        self.options = dict()
        for k, vs in parse_qs(spec.query).items():
            self.options[k] = vs[-1]

        def get_and_drop(opts, key, existing_value):
            value = opts.get(key)
            if value is not None:
                del opts[key]
            return existing_value if existing_value is not None else value

        self.username = get_and_drop(self.options, 'user', self.username)
        self.username = get_and_drop(self.options, 'username', self.username)
        self.password = get_and_drop(self.options, 'password', self.password)

    def make_url(self, prefix, username_option=None, explicit_port=False, default_username=None, default_password=None):
        scheme = 'monetdb'

        netloc = ''
        if (self.username or self.password) and username_option is None:
            netloc = (self.username or '')
            if self.password:
                netloc += ':' + self.password  # quoting??
            netloc += '@'
        netloc += self.hostname
        port = self.port
        if explicit_port and not port:
            port = 50000
        if port:
            netloc += f':{port}'

        path = f'/{self.database}'

        opts = dict(self.options)
        if username_option:
            username = self.username or default_username
            password = self.password or default_password
            if username:
                opts[username_option] = username
            if password:
                opts['password'] = password
        query = urlencode(opts)

        return prefix + urlunsplit((scheme, netloc, path, query, None))

    def for_libmapi(self):
        if self.username is not None or self.password is not None:
            raise Exception(
                'User name and password not supported in C MAPI url')
        return self.make_url('mapi:', explicit_port=True)

    def for_python(self):
        return self.make_url('mapi:')

    def for_jdbc(self):
        # should really parse .monetdb
        return self.make_url('jdbc:',
                             username_option='user',
                             default_username='monetdb',
                             default_password='monetdb')


def pymonetdb_runner(u: DBSpec):
    return [sys.executable, 'run.py', u.for_python()]


def jdbc_runner(u: DBSpec):
    version = '1.0-SNAPSHOT'
    jarname = f'bench-java-jdbc-{version}-jar-with-dependencies.jar'
    return ['java', '-jar', join('target', jarname), u.for_jdbc()]


def mapi_runner(u: DBSpec):
    return ['./runner', u.for_libmapi()]


KNOWN_RUNNERS = {
    'bench-python-pymonetdb': pymonetdb_runner,
    'bench-java-jdbc': jdbc_runner,
    'bench-c-libmapi': mapi_runner,
}


def runner_name(s):
    if s.endswith('/'):
        s = s[:-1]
    return s


argparser = argparse.ArgumentParser()
argparser.add_argument('-d', '--database', required=True,
                       help='Database name, can also be specified as a MAPI- or JDBC URL')
argparser.add_argument('-o', '--output-dir', required=True)
argparser.add_argument('-r', '--runner', required=True, type=runner_name,
                       choices=KNOWN_RUNNERS.keys(),
                       help='Runner to invoke',)
argparser.add_argument('--overwrite', action='store_true',
                       help='Overwrite existing results instead of skipping them')
argparser.add_argument('--allow-errors', action='store_true',
                       help='Try to continue when a runner fails')
argparser.add_argument('-t', '--duration', type=float, required=True,
                       help='how long to run the runner, in seconds')
argparser.add_argument("-w", "--wait", type=float, default=0.0,
                      help="number of seconds to wait before running each benchmark")
argparser.add_argument('queries', nargs='*')


try:
    git_cmd = ['git', 'describe',
               '--all', '--always', '--dirty=-modified', '--match=v*']
    output = subprocess.check_output(git_cmd, cwd=HERE, encoding='us-ascii')
    git_rev = output.splitlines()[0].strip().removeprefix('tags/')
except (subprocess.CalledProcessError, FileNotFoundError):
    git_rev = 'not available'


if '--' in sys.argv:
    idx = sys.argv.index('--')
    our_args = sys.argv[1:idx]
    TOOL_ARGS = sys.argv[idx+1:]
else:
    our_args = sys.argv[1:]
    TOOL_ARGS = []
args = argparser.parse_args(our_args)
# print(args)

queries = args.queries
if not queries:
    queries = sorted(glob(os.path.join(HERE, 'queries', '[a-z]*.sql')))

spec = DBSpec(args.database)

runner_dir = os.path.join(HERE, args.runner)


def run_runner(additional_args, allow_errors=False):
    cmd = KNOWN_RUNNERS[args.runner](spec) + [str(a) for a in additional_args] + [*TOOL_ARGS]
    visual = shlex.join(cmd)
    print('    RUNNING', visual)
    try:
        output = subprocess.check_output(cmd, cwd=runner_dir, encoding='us-ascii')
    except FileNotFoundError as e:
        # exiting instead of raising an exception tends to give better error output here
        print()
        sys.exit(f"{e}")
    except subprocess.CalledProcessError as e:
        if allow_errors:
            print("    FAILED!")
            return None
        # exiting instead of raising an exception tends to give better error output here
        print()
        sys.exit(f"{e}")
    return output


with open(os.path.join(HERE, 'queries', '_setup.sql')) as f:
    setup_code = f.read()
conn = None
cursor = None
try:
    conn = pymonetdb.connect(spec.for_python(), autocommit=True)
    cursor = conn.cursor()
    cursor.execute(setup_code)
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

output_dir = os.path.abspath(args.output_dir)
if not os.path.isdir(output_dir):
    os.mkdir(output_dir)

metadata_file = os.path.join(output_dir, 'metadata.txt')
metadata = f"""\
Benchmark version: {BENCHMARK_VERSION}
Benchmark git revision: {git_rev}
Duration: {args.duration}
Additional arguments: {shlex.join(TOOL_ARGS)}
"""
metadata += run_runner([])
print(metadata)
if os.path.exists(metadata_file):
    existing_content = open(metadata_file).read()
    if metadata not in existing_content:
        metadata_lines = metadata.splitlines()
        existing_lines = existing_content.splitlines()
        for line in difflib.unified_diff(existing_lines, metadata_lines, metadata_file):
                print(line.rstrip(), file=sys.stderr)
        print(f'ERROR: Content of {metadata_file} seems to have changed, see above', file=sys.stderr)
        sys.exit(1)
else:
    with open(metadata_file, 'w') as f:
        f.write(metadata)


def output_path(query_file, extension):
    base = os.path.splitext(os.path.basename(query_file))[0]
    path = os.path.join(output_dir, base + '.' + extension)
    return path


failures = []
for qf in queries:
    if args.wait:
        now = time.time()
        then = time.localtime(now + args.wait)
        sleep_till = time.strftime("%H:%M:%S", then)
        print(f"SLEEP {args.wait:.1f}s until {sleep_till}")
        time.sleep(args.wait)

    print('QUERY', os.path.basename(qf))

    csv_file = output_path(qf, 'csv')
    if os.path.exists(csv_file) and not args.overwrite:
        print('    skipping because output file exist')
        continue

    qf_rel = os.path.relpath(qf, start=runner_dir)
    data = run_runner([qf_rel, args.duration], allow_errors=args.allow_errors)
    if data is not None:
        with open(csv_file, 'w') as f:
            f.write(data)
        print(f'    {len(data.splitlines())} measurements')
    else:
        failures.append(os.path.basename(qf))

# generate a new summary
with redirect_stdout(io.StringIO()) as summary:
    print('"name","count","total_seconds","mean_seconds"')
    for csv_file in sorted(glob(os.path.join(output_dir, '*.csv'))):
        name = os.path.splitext(os.path.basename(csv_file))[0]
        lines = open(csv_file).readlines()
        if lines:
            content_nanos = numpy.loadtxt(csv_file, 'f', ndmin=1)
            content = content_nanos / 1e9
            count = len(content)
            total_seconds = content.max()
            mean_seconds = total_seconds / count if count > 0 else None
        else:
            content = numpy.empty((0,), 'f')
            count = 0
            total_seconds = 0
            mean_seconds = 0
        print(f'"{name}",{count},{total_seconds},{mean_seconds}')

with open(output_path('summary.xxx', 'txt'), 'w') as f:
    f.write(summary.getvalue())


if failures:
    print()
    print(f"{len(failures)} runs failed: " + ", ".join(failures))
    sys.exit(1)


