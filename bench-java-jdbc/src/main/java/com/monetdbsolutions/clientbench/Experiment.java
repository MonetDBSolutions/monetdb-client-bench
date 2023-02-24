package com.monetdbsolutions.clientbench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;

public class Experiment {
	private final String dbUrl;
	private final Benchmark benchmark;
	private final ResultWriter writer;
	private final ColumnKind[] columns;
	private final long startTimeNanos;
	private final long deadlineNanos;

	public Experiment(String dbUrl, Benchmark benchmark, double duration, ResultWriter writer) throws SQLException {
		this.dbUrl = dbUrl;
		this.benchmark = benchmark;
		this.writer = writer;
		try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
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
				String type = md.getColumnTypeName(i + 1);
				switch (type) {
					case "int":
					case "tinyint":
					case "smallint":
					case "bigint":
					case "interval month":
						kind = ColumnKind.IntegerColumn;
						break;

//					case Types.VARCHAR:
					case "char":
					case "varchar":
					case "clob":
						kind = ColumnKind.StringColumn;
						break;

					case "blob":
						kind = ColumnKind.BlobColumn;
						break;

					case "boolean":
						kind = ColumnKind.BoolColumn;
						break;

					case "day_interval":
					case "interval day":
						kind = ColumnKind.IntervalDayColumn;
						break;

					case "sec_interval":
					case "interval second":
					case "hugeint":
					case "decimal":
						kind = ColumnKind.DecimalColumn;
						break;

					case "date":
						kind = ColumnKind.DateColumn;
						break;

					case "time":
						kind = ColumnKind.TimeColumn;
						break;

					case "timetz":
						kind = ColumnKind.TimeTzColumn;
						break;

					case "timestamp":
						kind = ColumnKind.TimestampColumn;
						break;

					case "timestamptz":
						kind = ColumnKind.TimestampTzColumn;
						break;

					case "real":
					case "double":
						kind = ColumnKind.FloatColumn;
						break;

					case "uuid":
						kind = ColumnKind.UuidColumn;
						break;

					default:
						int sqltype = md.getColumnType(i + 1);
						String colname = md.getColumnName(i + 1);
						throw new RuntimeException(MessageFormat.format("Column {0}({1}) has unknown type: {2}/{3}", i, colname, type, sqltype));
				}
			}
			cols[i] = kind;
		}
		return cols;
	}

	Connection connect() throws SQLException {
		return DriverManager.getConnection(dbUrl);
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

	public ColumnKind getColumnKind(int i) {
		return columns[i];
	}
}