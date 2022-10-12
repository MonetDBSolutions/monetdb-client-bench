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
	private ArrayList<ColumnKind> columns = null;

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
		try (ResultWriter.Submitter submitter = writer.newSubmitter()) {
			// The first time we run we do not collect timing info but we do record the column types.
			ResultSet rs = executeBenchmarkQuery();

			columns = extractColumnKinds(rs);
			handleResultSet(rs);
			rs.close();

			if (duration == null) {
				return;
			}

			Long expected = benchmark.getExpected();
			long deadline = (long)(1000 * duration) + System.currentTimeMillis();
			do {
				if (benchmark.alwaysReconnect()) {
					disconnect();
				}
				long t0 = System.nanoTime();
				rs = executeBenchmarkQuery();
				long rowCount = handleResultSet(rs);
				long t1 = System.nanoTime();
				rs.close();
				if (expected != null && rowCount != expected) {
					throw new RuntimeException("Unexpected row count: expected " + expected + ", got " + rowCount);
				}
				submitter.submit(t1 - t0);
			} while (System.currentTimeMillis() < deadline);
			// deter optimizer
			getStatement().execute("SELECT " + count);
		}
	}

	private ArrayList<ColumnKind> extractColumnKinds(ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		ArrayList<ColumnKind> cols = new ArrayList<>(md.getColumnCount());
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
			cols.add(kind);
		}
		return cols;
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
