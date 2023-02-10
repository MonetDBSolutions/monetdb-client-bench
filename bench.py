#!/usr/bin/env python3

# This script has become a bit of a beast.
# That's the nature of this sort of thing.


import argparse
from contextlib import redirect_stdout
from glob import glob
import io
import numpy
import os
from os.path import join
import re
import shlex
import subprocess
import sys
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import pymonetdb

BENCHMARK_VERSION = "0.2.0pre1"

HERE = os.path.dirname(sys.argv[0])


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
    return ['python3', 'run.py', u.for_python()]


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
argparser.add_argument('-t', '--duration', type=float, required=True,
                       help='how long to run the runner, in seconds')
argparser.add_argument('queries', nargs='*')


try:
    git_cmd = ['git', 'describe',
               '--all', '--always', '--dirty=-modified', '--match=v*']
    output = subprocess.check_output(git_cmd, cwd=HERE, encoding='us-ascii')
    git_rev = output.splitlines()[0].strip().removeprefix('tags/')
except (subprocess.CalledProcessError, FileNotFoundError):
    git_rev = 'not available'


args = argparser.parse_args()
# print(args)

queries = args.queries
if not queries:
    queries = sorted(glob(os.path.join(HERE, 'queries', '[a-z]*.sql')))

spec = DBSpec(args.database)

runner_dir = os.path.join(HERE, args.runner)


def run_runner(*additional_args):
    cmd = KNOWN_RUNNERS[args.runner](spec) + [str(a) for a in additional_args]
    visual = shlex.join(cmd)
    print('    RUNNING', visual)
    try:
        output = subprocess.check_output(cmd, cwd=runner_dir, encoding='us-ascii')
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        # in general exiting here instead of raising the exception is a bad
        # idea but in this case we do it anyway because it leads to much better
        # error output when this script fails.
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
"""
metadata += run_runner()
print(metadata)
if os.path.exists(metadata_file):
    existing_content = open(metadata_file).read()
    if metadata not in existing_content:
        print('\n'
              f'ERROR: Content of {metadata_file} does not seem to match {args.runner}:\n'
              f'{existing_content}',
              file=sys.stderr)
        sys.exit(1)
else:
    with open(metadata_file, 'w') as f:
        f.write(metadata)


def output_path(query_file, extension):
    base = os.path.splitext(os.path.basename(query_file))[0]
    path = os.path.join(output_dir, base + '.' + extension)
    return path


for qf in queries:
    print('QUERY', os.path.basename(qf))

    csv_file = output_path(qf, 'csv')
    if os.path.exists(csv_file) and not args.overwrite:
        print('    skipping because output file exist')
        continue

    qf_rel = os.path.relpath(qf, start=runner_dir)
    data = run_runner(qf_rel, args.duration)
    with open(csv_file, 'w') as f:
        f.write(data)
    print(f'    {len(data.splitlines())} measurements')

# generate a new summary
with redirect_stdout(io.StringIO()) as summary:
    print('"name","count","total_seconds","mean_seconds"')
    for csv_file in sorted(glob(os.path.join(output_dir, '*.csv'))):
        name = os.path.splitext(os.path.basename(csv_file))[0]
        content_nanos = numpy.loadtxt(csv_file, 'f', ndmin=1)
        content = content_nanos / 1e9
        count = len(content)
        total_seconds = content.max()
        mean_seconds = total_seconds / count if count > 0 else None
        print(f'"{name}",{count},{total_seconds},{mean_seconds}')

with open(output_path('summary.xxx', 'txt'), 'w') as f:
    f.write(summary.getvalue())