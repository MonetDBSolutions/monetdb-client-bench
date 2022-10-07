package com.monetdbsolutions.clientbench;

import org.monetdb.jdbc.MonetDriver;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {

	public static final String DRIVER_VERSION = MonetDriver.getDriverVersion();

	public static void main(String[] args) throws Exception {
		String dbUrl = null;
		String queryFile = null;
		Double duration = null;

		switch (args.length) {
			default:
				System.err.println("Usage: bench-java-jdbc DB_URL QUERY_FILE DURATION_SECONDS");
				System.exit(1);
				return;
			case 3:
				duration = Double.parseDouble(args[2]);
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
		Benchmark benchmark = new Benchmark(Path.of(queryFile));

		try (ResultWriter writer = new ResultWriter(System.out)) {
			Runner runner = new Runner(dbUrl, benchmark, writer);
			runner.run(duration);
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
