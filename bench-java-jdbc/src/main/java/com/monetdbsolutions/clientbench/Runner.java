package com.monetdbsolutions.clientbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Properties;

public class Runner {
	private final String dbUrl;
	private final Benchmark benchmark;
	private final int fetchSize;
	private final ResultWriter writer;
	Connection _conn = null;
	private Statement _statement = null;
	private PreparedStatement _prepared = null;
	private long count;
	private ArrayList<ColumnKind> columns;

	public Runner(String dbUrl, Benchmark benchmark, int fetchSize, ResultWriter writer) {
		this.dbUrl = dbUrl;
		this.benchmark = benchmark;
		this.fetchSize = fetchSize;
		this.writer = writer;
	}

	private Connection getConnection() throws SQLException {
		if (_conn == null) {
			Properties props = new Properties(1);
			props.setProperty("fetchsize", String.valueOf(fetchSize));
			_conn = DriverManager.getConnection(dbUrl, props);
		}
		return _conn;
	}

	private Statement getStatement() throws SQLException {
		if (_statement == null) {
			_statement = getConnection().createStatement();
		}
		return _statement;
	}

	private PreparedStatement getPreparedStatement() throws SQLException {
		if (_prepared == null) {
			_prepared = getConnection().prepareStatement(benchmark.getQuery());
		}
		return _prepared;
	}

	private ResultSet executeBenchmarkQuery() throws SQLException {
		if (benchmark.usePrepareStatement()) {
			return getPreparedStatement().executeQuery();
		} else {
			return getStatement().executeQuery(benchmark.getQuery());
		}
	}

	private void disconnect() throws SQLException {
		if (_statement != null && !_statement.isClosed()) {
			_statement.close();
		}
		_statement = null;
		if (_prepared != null && !_prepared.isClosed()) {
			_prepared.close();
		}
		_prepared = null;
		if (_conn != null && !_conn.isClosed()) {
			_conn.close();
		}
		_conn = null;
	}

	public void run(Double duration) throws SQLException {
		{
			if (duration == null) {
				getStatement().execute(benchmark.getQuery());
			} else {
				// Warm up and retrieve metadata
				ResultSet rs = getStatement().executeQuery(benchmark.getQuery());
				ResultSetMetaData md = rs.getMetaData();
				columns = new ArrayList<>(md.getColumnCount());
				for (int i = 1; i <= md.getColumnCount(); i++) {
					final ColumnKind kind;
					if (benchmark.alwaysText()) {
						kind = ColumnKind.StringColumn;
					} else {
						int type = md.getColumnType(i);
						switch (type) {
							case Types.INTEGER:
								kind = ColumnKind.IntColumn;
								break;
							case Types.VARCHAR:
								kind = ColumnKind.StringColumn;
								break;
							default:
								throw new RuntimeException(MessageFormat.format("Column {0}({1}) has unknown type {2}", i, md.getColumnName(i), type));
						}
					}
					columns.add(kind);
				}
				rs.close();

				try (ResultWriter.Submitter submitter = writer.newSubmitter()) {
					long durationMillis = (long) (1000 * duration);
					long deadline = System.currentTimeMillis() + durationMillis;
					Long expected = benchmark.getExpected();
						if (benchmark.alwaysReconnect()) {
							disconnect();
						}
						long t0 = System.nanoTime();
						rs = executeBenchmarkQuery();
						long count = handleResultSet(rs);
						long t1 = System.nanoTime();
						if (expected != null && count != expected) {
							throw new RuntimeException("Unexpected row count: expected " + expected + ", got " + count);
						}
						submitter.submit((double) (t1 - t0) / 1.0e9);
					}
				}
				getStatement().execute("SELECT " + count);
				disconnect();
			}

		}
	}

	private long handleResultSet(ResultSet rs) throws SQLException {
		long count = 0;
		while (rs.next()) {
			handleResultRow(rs);
			count++;
		}
		return count;
	}

	private void handleResultRow(ResultSet rs) throws SQLException {
		int count = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= count; i++) {
			ColumnKind kind = columns.get(i - 1);
			switch (kind) {
				case IntColumn:
					int v = rs.getInt(i);
					if (rs.wasNull() || v == 42)
						this.count++;
					break;
				case StringColumn:
					String s = rs.getString(i);
					if (rs.wasNull() || s.length() > 4)
						this.count++;
					break;
			}
		}
	}
}
