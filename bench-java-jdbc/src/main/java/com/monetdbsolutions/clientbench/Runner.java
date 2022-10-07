package com.monetdbsolutions.clientbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.ArrayList;

public class Runner {
	private final String dbUrl;
	private final Benchmark benchmark;
	private final ResultWriter writer;
	private Statement _statement = null;
	private long count;
	private ArrayList<ColumnKind> columns;

	public Runner(String dbUrl, Benchmark benchmark, ResultWriter writer) {
		this.dbUrl = dbUrl;
		this.benchmark = benchmark;
		this.writer = writer;
	}

	private Statement getStatement() throws SQLException {
		if (_statement == null) {
			Connection conn = DriverManager.getConnection(dbUrl);
			_statement = conn.createStatement();
		}
		return _statement;
	}

	private void disconnect() throws SQLException {
		if (_statement != null && !_statement.isClosed()) {
			Connection conn = _statement.getConnection();
			_statement.close();
			conn.close();
		}
		_statement = null;
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
					while (System.currentTimeMillis() < deadline) {
						if (benchmark.alwaysReconnect()) {
							disconnect();
						}
						long t0 = System.nanoTime();
						rs = getStatement().executeQuery(benchmark.getQuery());
						handleResultSet(rs);
						long t1 = System.nanoTime();
						submitter.submit((double) (t1 - t0) / 1.0e9);
					}
				}
				getStatement().execute("SELECT " + count);
				disconnect();
			}

		}
	}

	private void handleResultSet(ResultSet rs) throws SQLException {
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
