#include "runner.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

void
destroy_benchmark(struct benchmark *benchmark)
{
	free(benchmark->text);
	free(benchmark);
}


static const char *find_keyword(const char *start, char **keyword, bool *has_value, long *value);

struct benchmark*
read_benchmark(const char *filename)
{
	struct benchmark *benchmark = malloc(sizeof(struct benchmark));
	if (!benchmark)
		die_errno("malloc failed");
	FILE *f = fopen(filename, "r");

	char *buffer = NULL;
	size_t capacity = 0;
	size_t len = 0;

	while (1) {
		size_t free_space = capacity - len;
		if (free_space <= 1) {
			capacity = capacity + capacity / 2 + 1024;
			buffer = realloc(buffer, capacity);
			if (!buffer)
				die_errno("realloc failed");
			continue;
		}

		size_t nread = fread(&buffer[len], 1, capacity - len, f);
		if (nread < 0) {
			die_errno("read failed");
		} else if (nread == 0) {
			fclose(f);
			break;
		} else {
			len += nread;
		}
	}
	assert(len < capacity);
	buffer[len++] = '\0';

	*benchmark = (struct benchmark) {
		.text = buffer,
		.prepare = false,
		.reconnect = false,
		.parallel = 1,
		.all_text = false,
		.expected = -1,
	};

	const char *pos = benchmark->text;
	char *keyword;
	bool has_value;
	long value = -42;

	while ((pos = find_keyword(pos, &keyword, &has_value, &value))) {
		if (0 == strcmp(keyword, "PREPARE")) {
			benchmark->prepare = true;
		} else if (0 == strcmp(keyword, "RECONNECT")) {
			benchmark->reconnect = true;
		} else if (0 == strcmp(keyword, "PARALLEL") && has_value) {
			benchmark->parallel = value;
		} else if (0 == strcmp(keyword, "ALL_TEXT")) {
			benchmark->all_text = true;
		} else if (0 == strcmp(keyword, "EXPECTED") && has_value) {
			benchmark->expected = value;
		} else {
			fprintf(stderr, "Invalid keyword %s\n", keyword);
			exit(1);
		}
		free(keyword);
	}

	return benchmark;
}

static const char *
find_keyword(const char *start, char **keyword, bool *has_value, long *value)
{
	const char *kw_start = strchr(start, '@');
	if (!kw_start)
		return NULL;
	kw_start = kw_start + 1; // past the @

	const char *end = strchr(kw_start, '@');
	if (!end) {
		fprintf(stderr, "Unterminated @KEYWORD@\n");
		exit(1);
	}

	const char *kw_end = end;
	const char *eq = memchr(kw_start, '=', end - kw_start);
	if (eq) {
		kw_end = eq;
	}
	*keyword = strndup(kw_start, kw_end - kw_start);
	if (eq) {
		char *parsed_to;
		*value = strtol(eq + 1, &parsed_to, 10);
		if (parsed_to != end) {
			fprintf(stderr, "Invalid value for keyword %s\n", *keyword);
			exit(1);
		}
		*has_value = true;
	} else {
		*has_value = false;
	}

	return end + 1;
}