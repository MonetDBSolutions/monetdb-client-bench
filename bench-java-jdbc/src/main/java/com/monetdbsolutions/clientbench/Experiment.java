package com.monetdbsolutions.clientbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.MessageFormat;
import java.util.Properties;

public class Experiment {
	private final String dbUrl;
	private final Benchmark benchmark;
	private final int fetchSize;
	private final ResultWriter writer;
	private final ColumnKind[] columns;
	private final long startTimeNanos;
	private final long deadlineNanos;

	public Experiment(String dbUrl, Benchmark benchmark, int fetchSize, double duration, ResultWriter writer) throws SQLException {
		this.dbUrl = dbUrl;
		this.benchmark = benchmark;
		this.fetchSize = fetchSize;
		this.writer = writer;
		try (Connection conn = connect(1); Statement stmt = conn.createStatement()) {
			ResultSet rs = stmt.executeQuery(benchmark.getQuery());
			this.columns = extractColumnKinds(benchmark, rs);
			rs.close();
		}
		this.startTimeNanos = System.nanoTime();
		this.deadlineNanos = (long) (1e9 * duration) + this.startTimeNanos;
	}

	private static ColumnKind[] extractColumnKinds(Benchmark benchmark, ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		int columnCount = md.getColumnCount();
		ColumnKind[] cols = new ColumnKind[columnCount];
		for (int i = 0; i < columnCount; i++) {
			final ColumnKind kind;
			if (benchmark.alwaysText()) {
				kind = ColumnKind.StringColumn;
			} else {
				int type = md.getColumnType(i + 1);
				switch (type) {
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
					case Types.BIGINT:
						kind = ColumnKind.IntColumn;
						break;
					case Types.VARCHAR:
						kind = ColumnKind.StringColumn;
						break;
					default:
						throw new RuntimeException(MessageFormat.format("Column {0}({1}) has unknown type {2}", i, md.getColumnName(i), type));
				}
			}
			cols[i] = kind;
		}
		return cols;
	}

	Connection connect() throws SQLException {
		return connect(this.fetchSize);
	}

	Connection connect(int fetchSize) throws SQLException {
		Properties props = new Properties(1);
		props.setProperty("fetchsize", String.valueOf(fetchSize));
		return DriverManager.getConnection(dbUrl, props);
	}

	public boolean deadlineExpired(long currentTimeNanos) {
		return currentTimeNanos >= deadlineNanos;
	}

	public Benchmark getBenchmark() {
		return benchmark;
	}

	public ResultWriter.Submitter newSubmitter() {
		return this.writer.newSubmitter();
	}

	public long getStartTime() {
		return startTimeNanos;
	}

	public ColumnKind getColumn(int i) {
		return columns[i];
	}
}