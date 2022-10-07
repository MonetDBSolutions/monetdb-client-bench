MonetDB Client Benchmark
========================

Benchmark thhe MonetDB client libraries, in particular [pymonetdb],
[monetdb-jdbc] and [libmapi]. Maybe also [odbc]. The embedded versions
[monetdbe-python] and [monetdbe-java] would also be interesting for comparison.

The tests focus particularly on result set retrieval for various combinations of
row count, column count and data types, but future version will also frequent
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

2. How do the client compare to each other. In particular, at which point would
   it be better to for example switch from Python to a faster language?


Client side processing
----------------------

We need to be careful with the amount of processing required on the client side.

With a language like Python, if we require the client to do too much processing
on the client side we are no longer measuring pymonetdb performance but only
Python performance, which is not what we're after. On the other hand, with the
compiled language if we do too little processing the compiler might realize the
data isn't actually used and skip the text-to-int conversions altogether.

With this in mind we require the client to

1. send the given SELECT queries,

2. retrieve the full result set,

3. extracting every field in every column as the relevant type, unless the test
   is explicitly marked as string-only,

4. and count for each colum the number of 42's if it is an integer column or the
   number of strings with length larger than 4 if it is a textual column,

5. also, to count the NULLs in each column.

All text must be ASCII-only.

All scripting must be done in Python so that in the future we can be reasonably
portable to Windows.


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


* **TALL_TEXT** a table with a large number of rows and a moderate number of
VARCHAR/TEXT columns. A limited number of items is NULL.

* **TALL_INT** a table with a large number of rows and a moderate number of
integer columns. A limited number of items is NULL.

* **TALL_INT_AS_TEXT** the same table as **TALL_INT** but retrieving the
  data as text without conversion. For comparison with **TALL_INT**. Interesting
  when we switch to a binary protocol.


> Not sure if its interesting to separately test the various integer widths.
> Also not sure if we should include DECIMAL here. How do the various client
> library standards even deal with decimals?

> For the time being I'd like to keep the exotic types such as the temporals,
> uuid's, json, etc. out of this. Unless someone specifically expresses interest
> in one of these.

> In Java **TALL_INT_AS_TEXT** can be implemented by calling
> `ResultSet.getString` rather than `ResultSet.getInt`. Not sure how it
> can be done in Python and libmapi.

* **VERY_TALL_TEXT** and **VERY_TALL_INT** are like their TALL cousins but with
  more rows to see if the processing time grows linearly with the number of
  rows.

* **WIDE_TEXT** and **WIDE_INT** have twice the number of columns to see if the
  processing time grows linearly with the number of columns.

* **QUICK_TEXT** and **QUICK_INT** a query like
`SELECT v AS col1, v AS col2, v AS col3 FROM (SELECT CAST(42 AS INT) as v) AS t`,
possibly as a prepared statement, repeated a large number of times on a single
connection.

> Should we test both with and withoud prepared statement? Or leave it to the
> implementer to pick whatever is most efficient?

* **RECONNECT** measure how long it takes to set up a new connection and execute
  `SELECT 42` on it. Also separately, how long it takes to close the connection
  afterward. Probably have to run this test with 1, 2, 4 and 8 threads.

> I seem to recall I once did a similar benchmark and found that closing took
> a significant amount of time. I added a special closer-thread that did nothing
> but close the connections discarded by other threads and it sped up things
> significantly. Have to check how and whether this is still the case. Running
> it multithreaded might remove the need for the closer-thread.
>
> We have to decide whether we make such a closer-thread (1) mandatory, (2) forbidden
> or (3) leave it up to the implementer.


Host- and network configuration
-------------------------------

The benchmarks can be run on small and big machines.

Also, the benchmarks can be run with everything on localhost, or with client and
server on separate systems. The latter case excludes the MonetDB/e variants, unless
we run them in their experimental remote mode.

If run on separate systems, we have many choices of network bandwidth and
latency.

1. Client and server on localhost.

2. Client and server side by side on a fast data center LAN.

3. Client and server side by side on whatever our cloud provider offers between
   VM's in the same availability region today.

4. Client and server separated by the Internet. This is the Cumulus scenario
   with the client a Jupyter notebook on a laptop and the server somewhere in
   the cloud.

5. Many other scenarios.

> I am completely and utterly undecided on what to pick here.
> For the time being testing on localhost and on the company LAN is probably
> sufficient.


Information to retain
---------------------

For every experiment we create a directory under results/ that holds the
results. Every experiment directory should contain a README that describes the
setup. This includes

* software versions: MonetDB version, pymonetdb version, Python version,
  monetdb-jdbc version, JDK version, etc.

* hardware configuration, [inxi] is a nice tool for that, comes as a package on
  Fedora and Debian

* network configuration, if relevant, might be hard to describe when for example
  on AWS.

[inxi]: https://github.com/smxi/inxi

For the longer running queries, we repeatedly run the query and record the run
times, probably discarding the first 5 or so.

For the quick queries we repeatedly run the query for a while. This will yield
hundreds of thousands of durations, too many to keep them all. Reducing it to
count/min/max/avg/stddev is a bit too restrictive, for example it would be know
if the distribution has two peaks instead of one. For this reason we also record
an approximation of the quantiles of the duration that we can plot and possibly
analyze further.


Implementations
---------------

Every implementation of the benchmark lives in its own directory. It probably
needs to be built before it can be used, it would be nice to put some
instructions in its README.

The toplevel driver script knows knows how to invoke the individual
implementations, for example `python3 clientbench.py ARGS` for the
python-pymonetdb/ directory and `java -jar clientbench.jar ARGS` for the
java-jdbc/ directory. It passes the mapi url, the path to the sql script to run
and the amount of time to repeat the query, or 0 to run it only once.

The benchmark runner is expected to connect to MonetDB, repeat the query until
the time runs out and write the duration of every execution to stdout, as
floating point, in seconds. The driver script will take care of computing the
statistics and writing the results to the results directory.

