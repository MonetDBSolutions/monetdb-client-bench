package com.monetdbsolutions.clientbench;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Runner {
	private final Experiment experiment;
	Connection _conn = null;
	private Statement _statement = null;
	private PreparedStatement _prepared = null;
	private long count;

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
			Long expected = experiment.getBenchmark().getExpected();
			long t1;
			do {
				if (experiment.getBenchmark().alwaysReconnect()) {
					disconnect();
				}
				ResultSet rs = executeBenchmarkQuery();
				long rowCount = handleResultSet(rs);
				rs.close();
				if (expected != null && rowCount != expected) {
					throw new RuntimeException("Unexpected row count: expected " + expected + ", got " + rowCount);
				}
				t1 = System.nanoTime();
				submitter.submit(t1 - experiment.getStartTime());
			} while (!experiment.deadlineExpired(t1));
			// deter optimizer
			getStatement().execute("SELECT " + count);
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
			ColumnKind kind = experiment.getColumn(i - 1);
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
