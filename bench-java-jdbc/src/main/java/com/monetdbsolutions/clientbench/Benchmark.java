package com.monetdbsolutions.clientbench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class Benchmark {
	private final String query;
	private final boolean allText;
	private final boolean reconnect;

	public Benchmark(Path queryFile) throws IOException {
		List<String> lines = Files.readAllLines(queryFile);
		query = String.join("\n", lines);
		allText = query.contains("@ALL_TEXT@");
		reconnect = query.contains("@RECONNECT");
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
}
