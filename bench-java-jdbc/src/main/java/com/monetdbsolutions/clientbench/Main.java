package com.monetdbsolutions.clientbench;

import org.monetdb.jdbc.MonetDriver;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {

	public static final String DRIVER_VERSION = MonetDriver.getDriverVersion();

	public static void main(String[] args) throws Exception {
		String dbUrl = null;
		String queryFile = null;
		Double durationArg = null;
		Integer fetchSizeArg = null;

		switch (args.length) {
			default:
				System.err.println("Usage: bench-java-jdbc DB_URL QUERY_FILE FETCH_SIZE DURATION_SECONDS");
				System.exit(1);
				return;
			case 4:
				durationArg = Double.parseDouble(args[3]);
				/* fallthrough */
			case 3:
				fetchSizeArg = Integer.parseInt(args[2]);
				/* fallthrough */
			case 2:
				queryFile = args[1];
				/* fallthrough */
			case 1:
				dbUrl = args[0];
				/* fallthrough */
			case 0:
				break;

		}

		if (queryFile == null) {
			showInfo(dbUrl);
			return;
		}

		if (durationArg == null) {
			throw new RuntimeException("Duration must be specified");
		}

		// If we get here, all parameters have been given
		Benchmark benchmark = new Benchmark(Path.of(queryFile));
		final int fetchSize = fetchSizeArg;
		final double duration = durationArg;

		AtomicBoolean success = new AtomicBoolean(true);
		try (ResultWriter writer = new ResultWriter(System.out)) {
			int n = benchmark.getParallelism();
			Thread[] threads = new Thread[n];
			for (int i = 0; i < n; i++) {
				{
					Runner runner = new Runner(dbUrl, benchmark, fetchSize, writer);
					Thread worker = new Thread(() -> doWork(runner, duration, success));
					worker.start();
					threads[i] = worker;
				}
			}
			for (int i = 0; i < n; i++) {
				threads[i].join();
			}
		}

		System.exit(success.get() ? 0 : 1);
	}

	private static void doWork(Runner runner, double duration, AtomicBoolean success) {
		try {
			runner.run(duration);
		} catch (Exception e) {
			success.set(false);
			e.printStackTrace();
		}
	}

	private static void showInfo(String dbUrl) throws SQLException {
		System.out.println("Java version: " + System.getProperty("java.version"));
		System.out.println("JDBC driver version: " + DRIVER_VERSION);
		System.out.println("JDBC URL: " + dbUrl);

		if (dbUrl == null)
			return;

		try (Connection conn = DriverManager.getConnection(dbUrl); Statement stmt = conn.createStatement()) {
			ResultSet rs = stmt.executeQuery("SELECT value FROM sys.environment WHERE name = 'monet_version'");
			rs.next();
			String monetVersion = rs.getString(1);
			System.out.println("MonetDB version: " + monetVersion);
		}
	}

}
