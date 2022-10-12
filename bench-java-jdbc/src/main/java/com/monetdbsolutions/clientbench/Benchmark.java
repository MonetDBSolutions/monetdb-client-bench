package com.monetdbsolutions.clientbench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Benchmark {
	private static final Pattern keywordPattern = Pattern.compile("@([A-Za-z0-9]+)(?:=([0-9]+))?@");
	private final String query;
	private boolean allText;
	private boolean reconnect;
	private boolean prepare;

	private int parallelism = 1;
	private Long expected = null;

	public Benchmark(Path queryFile) throws IOException {
		List<String> lines = Files.readAllLines(queryFile);
		query = String.join("\n", lines);
		parse_keywords();
	}

	private void parse_keywords() {
		Matcher matcher = keywordPattern.matcher(query);
		while (matcher.find()) {
			String name = matcher.group(1);
			String value = matcher.group(2);
			switch (name) {
				case "ALL_TEXT":
					allText = true;
					break;
				case "RECONNECT":
					reconnect = true;
					break;
				case "PARALLEL":
					if (value != null) {
						parallelism = Integer.parseInt(value);
					} else {
						throw new RuntimeException("Invalid keyword in sql query, need @PARALLEL=number@");
					}
					break;
				case "PREPARE":
					prepare = true;
					break;
				case "EXPECTED":
					if (value != null) {
						expected = Long.parseLong(value);
					} else {
						throw new RuntimeException("Invalid keyword in sql query, need @EXPECTED=number@");
					}
					break;
				default:
					throw new RuntimeException("Invalid keyword in sql query: " + name);
			}
		}

	}

	public String getQuery() {
		return query;
	}

	public boolean alwaysText() {
		return allText;
	}

	public boolean alwaysReconnect() {
		return reconnect;
	}

	public int getParallelism() {
		return parallelism;
	}

	public boolean usePrepareStatement() {
		return prepare;
	}

	public Long getExpected() {
		return expected;
	}
}
