#include "runner.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>


static const char *prog_name = "<UNKNOWN>";

static int
usage(char *msg)
{
	if (msg)
		fprintf(stderr, "Error: %s\n", msg);
	fprintf(stderr, "Usage: %s DB_URL QUERY_FILE FETCH_SIZE DURATION_SECONDS\n", prog_name);
	return 1;
}

int
main(int argc, char *argv[])
{
	char *db_url = NULL;
	char *query_file = NULL;
	long fetch_size = 100;
	double duration;
	bool do_run = false;
	char *remainder;

	prog_name = argv[0];

	switch (argc) {
		default:
			return usage(NULL);
		case 5:
			do_run = true;
			duration = strtod(argv[4], &remainder);
			if (remainder == argv[4])
				return usage("invalid duration");
			/* fallthrough */
		case 4:
			fetch_size = strtol(argv[3], &remainder, 10);
			if (remainder == argv[3])
				return usage("invalid fetch size");
			/* fallthrough */
		case 3:
			query_file = argv[2];
			/* fallthrough */
		case 2:
			db_url = argv[1];
			/* fallthrough */
		case 1:
			break;
	}

	if (argc > 1)
		connect_to(db_url, fetch_size);

	if (!do_run) {
		if (argc > 2)
			return usage(NULL);
		return show_info(db_url);
	}

	struct benchmark *benchmark = read_benchmark(query_file);
	init_output();
	prepare_run(benchmark);
	run_executors(duration);
	finish_output();
	destroy_benchmark(benchmark);

	return 0;
}
