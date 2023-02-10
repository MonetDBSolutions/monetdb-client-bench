MonetDB Client Benchmark
========================

Benchmark for the MonetDB client libraries, in particular [pymonetdb],
[monetdb-jdbc] and [libmapi]. Maybe also [odbc]. The embedded versions
[monetdbe-python] and [monetdbe-java] might also be interesting.

The tests focus on result set retrieval for various combinations of row count,
column count and data types, but future version may also include file transfers.

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

3. extract every field in every column as the relevant type, unless the test
   is explicitly marked as strings-only,

4. apply a type-specific test to the extracted value according to the table below

5. count the number of fields which are either NULL or where the condition is true.

All text will be US ASCII.

Conditions to count:

| TYPE           | CONDITION                                             |
| -------------- | ----------------------------------------------------- |
| TINYINT        | value equals 42                                       |
| SMALLINT       | value equals 42                                       |
| INT            | value equals 42                                       |
| BIGINT         | value equals 42                                       |
| HUGEINT        | value equals 42                                       |
| SERIAL         | value equals 42                                       |
| SHORTINT       | value equals 42                                       |
| MEDIUMINT      | value equals 42                                       |
| LONGINT        | value equals 42                                       |
| REAL           | value equals 42                                       |
| DOUBLE         | value equals 42                                       |
| DECIMAL        | value equals 42                                       |
| TEXT / VARCHAR | length of value greater than 4                        |
| BOOLEAN        | value is TRUE                                         |
| UUID           | value equals 12345678-1234-5678-1234-567812345678     |
| BLOB           | length of value greater than 4                        |
| DATE           | day component equals 14                               |
| TIME           | minute component equals 42                            |
| TIMETZ         | minute component equals 42                            |
| TIMESTAMP      | minute component equals 42                            |
| TIMESTAMPTZ    | minute component equals 42                            |
| SEC_INTERVAL   | length in seconds > 42                                |
| DAY_INTERVAL   | value equals 42                                       |
| MONTH_INTERVAL | value equals 42                                       |


Test cases
==========

We have roughly three classes of tests.

- Tests that contain many rows
- Tests that contain a single row but with various numbers of columns
- Reconnect tests which disconnect after each query.

The test queries can be found in the queries/ directory, one per file. The
comments can contain special keywords:

| KEYWORD       | MEANING                                                |
| ----          | ----                                                   |
| @PREPARE@     | use a prepared statement, if available                 |
| @RECONNECT@   | disconnect and reconnect between each query sent       |
| @PARALLEL=n@  | run n jobs in parallel                                 |
| @ALL_TEXT@    | retrieve fields as text regardless of the column type  |
| @EXPECTED=n@  | expect n result rows                                   |

For each language/library combo we have a runner program that executes and times
the queries, see below.

Tall test cases
---------------

The tests with names of the form `tall_bigint.sql`, `tall_boolean.sql`, etc.,
test result sets with 10 columns and 100_000 rows of the given type.
Some rows are NULL.

We can use these tests to compare the performance of certain types within a
given client library, for example compare the performance of ints and decimals,
or we can compare different clients for a given type.

Test `tall_int_as_text.sql` is a result set of integers to be extracted as
strings. For example in Java that would mean calling `resultSet.getString()`
instead of `resultSet.getInt()`. These tests are interesting with the binary
protocol: without binary, we expect extracting as text to be faster, with binary
we expect extracting integers to be faster.

Not all client libraries support this, for example pymonetdb currently
unconditionally converts integer columns to `int`.  Conversely, libmapi
only supports extracting as text so the application has to do the conversions.

There's also `very_tall_int.sql` and `very_tall_text.sql`.  These have 5× the
number of rows and can be used to determine if the duration scales linearly with
the number of rows.

Finally, `wide_int.sql` and `wide_text.sql` have 2× the number of columns and
can be used if the duration scales linearly with the number of columns.


One-row test cases
------------------

The one-row tests have names like `one_row_prep_1000.sql`. They test result sets
containing a single row 100, 1000 and 10_000 columns. With the `_prep_` tests,
the client is supposed to use a prepared statement api if that is available.


Reconnect tests
---------------

The reconnect tests `reconnect1.sql`, `reconnect2.sql`, `reconnect4.sql`,
and `reconnect8.sql` perform the query `SELECT 42` and then hang up. We test
this with 1, 2, 4 and 8 worker threads.


Test runners
============

For each language/library combo we have a runner program that executes and times
the queries. There is a toplevel script `bench.py` that executes them. It knows
how to invoke the runners, for example prepend `java -jar` for Java programs and
`python3` for Python programs, and it can also translate between bare database
names, libmapi-style MAPI URLs, JDBC URLs and Pymonetdb-style MAPI URLs.

The runners live in directories with names of the form
`bench-<language>-<clientlibrary>`. The toplevel script is not responsible for
building the runners as that is highly system- and experiment specific. However,
each runner directory should contain a README which clearly explains how to
build the runner and what needs to be installed on the system before.

The runner program is invoked with the following parameters:

* The URL of the database to connect to, in the appropriate URL dialect.
* The name of the query file. The runner should look for the keywords described
  above and exit if it finds any @KEYWORD@ that it doesn't recognize.
* How long to run the test, in seconds as a floating point value. The runner
  should repeat the query until the duration has expired.

If only the database url is given, or no parameter at all, a runner should print
some metadata including the language version, the library version and if the url
was given, the MonetDB version.

The runner first runs the query once for warmup and to determine the column
types in the result set. Then it sets its internal clock to 0.0 and starts
running the query repeatedly until time runs out. After each query it prints the
elapsed time since the start of its run in nanoseconds.

This means that from the output of the runner we can quickly see exactly how long it
ran (maximum of the samples) and how often it managed to run the query (count of the
samples). By computing the difference with the previous sample we can also compute
more advanced statistics on the query times.

Note that the one-row queries run very quickly, the runner should use appropriate
buffering to make sure the I/O of writing the durations does not slow it down.

The toplevel runner `bench.py` is run with an `--output-dir` argument. Each
runner should get its own output directory. Before starting, `bench.py` writes a
metadata.txt there containing its parameters and the version information from
the runner. If metadata.txt already exists and with different contents the run
is aborted.

The user is encouraged to extend metadata.txt with more information about the
setup, for example the output of [inxi] and details of the network layout.

[inxi]: https://github.com/smxi/inxi

For each query, `bench.py` runs the specified runner and writes the query
timings to a file QUERY.csv in the output directory.  If that file already
exists the query is skipped unless `--overwrite` is given.

It also creates or updates a file summary.txt with information from all CSV
files in the directory. When generating summary.txt it always uses all CSV
files, not just the ones that were generated during this run.
