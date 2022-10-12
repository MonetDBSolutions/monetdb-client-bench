MonetDB-JDBC client benchmark runner
====================================

This benchmark runner benchmarks the monetdb-java JDBC driver.

It is written for Java version 11 but does not intentionally use features from
Java versions newer than 8. To build it you need a JDK and Maven.

Run `mvn package` to build
`target/bench-java-jdbc-1.0-SNAPSHOT-jar-with-dependencies.jar`.
This jar will be invoked by bench.py.