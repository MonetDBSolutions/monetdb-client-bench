MonetDB Client Benchmark
========================

Benchmark thhe MonetDB client libraries, in particular [pymonetdb],
[monetdb-jdbc] and [libmapi]. Maybe also [odbc]. The embedded versions
[monetdbe-python] and [monetdbe-java] might also be interesting.

The tests focus on result set retrieval for various combinations of row count,
column count and data types, but future version will also test frequent
reconnects and include file transfers.

[pymonetdb]: https://www.monetdb.org/documentation/user-guide/client-interfaces/libraries-drivers/python-library/

[monetdb-jdbc]: https://www.monetdb.org/documentation/user-guide/client-interfaces/libraries-drivers/jdbc-driver/

[libmapi]: https://www.monetdb.org/documentation/user-guide/client-interfaces/libraries-drivers/mapi-library/

[odbc]: https://www.monetdb.org/documentation/user-guide/client-interfaces/libraries-drivers/odbc-driver/

[monetdbe-python]: https://github.com/MonetDBSolutions/MonetDBe-Python

[monetdbe-java]: https://github.com/MonetDBSolutions/MonetDBe-Java

Typical questions to be answered:

1. Do my recent changes to server or client libraries have any impact on client
   performance?

2. How do the clients compare to each other. For example, at which point would
   it be better to switch from Python to a faster language?


About client side processing
----------------------------

We need to be careful with the amount of processing required on the client side.

With a language like Python, if we require the client to do too much processing
we are no longer measuring pymonetdb performance but only Python performance. On
the other hand, with a compiled language if we do too little processing the
compiler might realize the data isn't actually used and skip the text-to-int
conversions altogether.

With this in mind we require the client to

1. send the given SELECT queries,

2. retrieve the full result set,

3. extracting every field in every column as the relevant type, unless the test
   is explicitly marked as string-only,

4. and count the number of 42's in integer columns and the number of strings
   with length larger than 4 in a textual columns, plus all NULLs.

All text will be US ASCII.

All scripting must be done in Python so that in the future we can be reasonably
portable to Windows.


About host- and network configuration
-------------------------------------

The benchmarks can be run on small and big machines. Also, the benchmarks can be
run with everything on localhost, or with client and server on separate systems.
The latter case excludes the MonetDB/e variants, unless we run them in their
experimental remote mode.

If run on separate systems, we have many choices for network bandwidth and
latency and often it will be hard to determine what we even have.
Some cases:

1. Client and server on localhost.

2. Client and server on separate on a fast data center LAN.

3. Client and server side by side on whatever our cloud provider happens to
   offer today.

4. Client and server separated by the Internet. This is the Cumulus scenario
   with the client a Jupyter notebook on a laptop and the server somewhere in
   the cloud.

5. Many other scenarios.

> I am completely and utterly undecided on what to pick here.
> For the time being testing on localhost and on the company LAN is probably
> sufficient.


Test cases
----------

By default the server splits a result set into chunks which the client requests
on demand. This is useful if the client at some point decides it doesn't need
the rest of the result set anymore. This can be configured using a setting often
called reply size or fetch size. Most benchmarks should be run with multiple
settings:

1. the default reply size,
2. 1000,
3. 10_000 and
4. infinite reply size.

* [tall_text.sql](./queries/tall_text.sql): a table with a large number of rows
  and a moderate number of VARCHAR/TEXT columns. A limited number of items is
  NULL.

* [tall_int.sql](./queries/tall_int.sql): a table with a large number of rows
  and a moderate number of integer columns. A limited number of items is NULL.

* [tall_int_as_text.sql](./queries/tall_int_as_text.sql): the same table as
  [tall_int.sql](./queries/tall_int.sql) but retrieving the data as text without
  conversion. For comparison with [tall_int.sql](./queries/tall_int.sql).
  Interesting when we switch to a binary protocol.

> Not sure if its interesting to separately test the various integer widths.
> Also not sure if we should include DECIMAL here. How do the various client
> library standards even deal with decimals?

> For the time being I'd like to keep the exotic types such as the temporals,
> uuid's, json, etc. out of this. Unless someone specifically expresses interest
> in one of these.

> In Java [tall_int_as_text.sql](./queries/tall_int_as_text.sql) can be
> implemented by calling `ResultSet.getString` rather than `ResultSet.getInt`.
> Not sure how it can be done in Python and libmapi.

* [very_tall_text.sql](./queries/very_tall_text.sql) and
  [very_tall_int.sql](./queries/very_tall_int.sql): like their tall cousins but
  with more rows to see if the processing time grows linearly with the number of
  rows.

* [wide_text.sql](./queries/wide_text.sql) and
  [wide_int.sql](./queries/wide_int.sql): twice the number of columns to see
  if the processing time grows linearly with the number of columns.

* [one_row_100.sql](./queries/one_row_100.sql),
[one_row_1000.sql](./queries/one_row_1000.sql) and
[one_row_10000.sql](./queries/one_row_10000.sql): a single-row query, possibly
as a prepared statement, with a large number of columns and repeated a large
number of times on a single connection.

* [reconnect.sql](./queries/reconnect.sql): measure how long it takes to set up
  a new connection and execute `SELECT 42` on it. Also separately, how long it
  takes to close the connection afterward. Probably have to run this test with
  1, 2, 4 and 8 threads.


Query runners
-------------

The queries can be found in the queries/ directory, one per file.
The comments can contain special keywords:

* @PREPARE@ use a prepared statement, if available
* @RECONNECT@ disconnect and reconnect between each query sent
* @PARALLEL=n@ run n jobs in parallel
* @ALL_TEXT@ retrieve fields as text regardless of the column type
* @EXPEXTED=n@ expect n result rows

For each language/library combo we have a runner program that executes and times
the queries. There is a toplevel script `bench.py` that executes them. It knows
how to invoke the runners, for example prepend `java -jar` for Java programs and
`python3` for Python programs, and it can also translate between bare database
names, libmapi-style MAPI URLs, JDBC URLs, Pymonetdb-style MAPI URLs, etc.

The runners live in directories with names of the form
`bench-<language>-<clientlibrary>`. The toplevel script is not responsible for
building the runners as that is highly system- and experiment specific. However,
each runner directory should contain a README which clearly explains how to
build the runner and what needs to be installed on the system before.

The runner program is invoked with the following parameters:

* The URL of the database to connect to, in the appropriate URL dialect.
* The name of the query file. The runner should look for the keywords described
  above and exit if it finds any @KEYWORD@ that it doesn't recognize.
* The reply size, -1 for infinite
* The duration in seconds to run the test for. Repeat the query until the
  duration has expired.

If only the database url is given, or no parameter at all, it should print some
metadata including the language version, the library version and if the url was
given, the MonetDB version.

The runner should produce on standard out the durations in nanoseconds. The
actual precision may be less, but the duration must be expressed in nanoseconds.
Note that the one-row queries run very quickly, the runner should try to make
sure that writing the durations does not slow it down.

The toplevel runner `bench.py` is run with an `--output-dir` argument. Each
runner should get its own output directory. Bench.py writes a metadata.txt there
containing the version information from the runner. For each query, it also
writes a file QUERY.csv file containing the query timings expressed in
nanoseconds.

The user is encouraged to extend metadata.txt with more information about the
setup, for example the output of [inxi] and details of the network layout.

[inxi]: https://github.com/smxi/inxi


TODO explain how to actually run a full experiment

TODO explain how to analyze the results
