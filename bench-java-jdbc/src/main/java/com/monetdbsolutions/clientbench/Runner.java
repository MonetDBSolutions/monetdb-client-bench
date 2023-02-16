package com.monetdbsolutions.clientbench;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

public class Runner {
	private final Experiment experiment;
	private final BigDecimal decimal42 = new BigDecimal(1L).setScale(3);
	private final BigDecimal interval42day = new BigDecimal(42L * 24 * 3600).setScale(3);
	private final Calendar offset1Calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+1"));
	Connection _conn = null;
	private Statement _statement = null;
	private PreparedStatement _prepared = null;
	private long nullCount;
	private long hitCount;

	public Runner(Experiment experiment) {
		this.experiment = experiment;
	}

	private Connection getConnection() throws SQLException {
		if (_conn == null) {
			_conn = experiment.connect();
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
			_prepared = getConnection().prepareStatement(experiment.getBenchmark().getQuery());
		}
		return _prepared;
	}

	private ResultSet executeBenchmarkQuery() throws SQLException {
		if (experiment.getBenchmark().usePrepareStatement()) {
			return getPreparedStatement().executeQuery();
		} else {
			return getStatement().executeQuery(experiment.getBenchmark().getQuery());
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

	public void run() throws SQLException {
		try (ResultWriter.Submitter submitter = experiment.newSubmitter()) {
			Benchmark benchmark = experiment.getBenchmark();
			Long expectedRows = benchmark.getExpectedRows();
			Long expectedNullCount = benchmark.getExpectedNullCount();
			Long expectedHitCount = benchmark.getExpectedHitCount();
			long t1;
			do {
				if (benchmark.alwaysReconnect()) {
					disconnect();
				}
				nullCount = 0;
				hitCount = 0;
				ResultSet rs = executeBenchmarkQuery();
				long rowCount = handleResultSet(rs);
				rs.close();
				if (expectedRows != null && rowCount != expectedRows) {
					throw new RuntimeException("Unexpected row count: expected " + expectedRows + ", got " + rowCount);
				}
				if (expectedNullCount != null && nullCount != expectedNullCount) {
					throw new RuntimeException("Unexpected null count: expected " + expectedNullCount + ", got " + nullCount);
				}
				if (expectedHitCount != null && hitCount != expectedHitCount) {
					throw new RuntimeException("Unexpected hit count: expected " + expectedHitCount + ", got " + hitCount);
				}
				t1 = System.nanoTime();
				submitter.submit(t1 - experiment.getStartTime());
			} while (!experiment.deadlineExpired(t1));
			// deter optimizer
			getStatement().execute("SELECT " + nullCount + hitCount);
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
			ColumnKind kind = experiment.getColumnKind(i - 1);
			switch (kind) {
				case IntegerColumn:
					long longValue = rs.getLong(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (longValue == 42) {
						hitCount++;
					}
					break;
				case StringColumn:
					String stringValue = rs.getString(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (stringValue.length() > 4) {
						hitCount++;
					}
					break;

				case BlobColumn:
					byte[] bytesValue = rs.getBytes(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (bytesValue.length > 4) {
						hitCount++;
					}
					break;

				case BoolColumn:
					boolean booleanValue = rs.getBoolean(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (booleanValue) {
						hitCount++;
					}
					break;

				case DateColumn:
					Date dateValue = rs.getDate(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (dateValue.toLocalDate().getDayOfMonth() == 14) {
						hitCount++;
					}
					break;

				case TimeColumn:
					Time timeValue = rs.getTime(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (timeValue.toLocalTime().getMinute() == 42) {
						hitCount++;
					}
					break;

				case TimeTzColumn:
					Time timeTzValue = rs.getTime(i, offset1Calendar);
					if (rs.wasNull()) {
						nullCount++;
					} else if (timeTzValue.toLocalTime().getMinute() == 42) {
						hitCount++;
					}
					break;

				case TimestampColumn:
					Timestamp timestampValue = rs.getTimestamp(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (timestampValue.toLocalDateTime().getMinute() == 42) {
						hitCount++;
					}
					break;

				case TimestampTzColumn:
					Timestamp timestampTzValue = rs.getTimestamp(i, offset1Calendar);
					if (rs.wasNull()) {
						nullCount++;
					} else if (timestampTzValue.toLocalDateTime().getMinute() == 42) {
						hitCount++;
					}
					break;

				case IntervalDayColumn:
					BigDecimal intervalDayValue = rs.getBigDecimal(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (intervalDayValue.compareTo(interval42day) == 0) {
						hitCount++;
					}
					break;

				case DecimalColumn:
					BigDecimal decimalValue = rs.getBigDecimal(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (decimalValue.compareTo(decimal42) == 0) {
						hitCount++;
					}
					break;

				case FloatColumn:
					double doubleValue = rs.getDouble(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (doubleValue == 42.0) {
						hitCount++;
					}
					break;

				case UuidColumn:
					String uuidValue = rs.getString(i);
					if (rs.wasNull()) {
						nullCount++;
					} else if (uuidValue.equals("12345678-1234-5678-1234-567812345678")) {
						hitCount++;
					}
					break;

				default:
					throw new RuntimeException("don't know how to process a " + kind);
			}
		}
	}
}
