#include "runner.h"

#include <stdlib.h>
#include <string.h>

static long counter = 0;

typedef void (*field_handler)(const char *field);

struct {
	struct benchmark *benchmark;
	int ncols;
	field_handler *handlers;
	struct timespec start;
	long deadline;
} common;

struct worker {
	Mapi conn;
	MapiHdl handle;
};

void
get_time(struct timespec *spec)
{
	if (0 != clock_gettime(CLOCK_MONOTONIC, spec))
		die_errno("get time");
}
static inline long
nanos(struct timespec *spec)
{
	return 1000 * 1000 * 1000 * spec->tv_sec + spec->tv_nsec;
}

static long
get_relative_nanos(void)
{
	struct timespec spec;
	get_time(&spec);
	long sec_difference = spec.tv_sec - common.start.tv_sec;
	long nanosec_difference = spec.tv_nsec - common.start.tv_nsec;
	return 1000 * 1000 * 1000 * sec_difference + nanosec_difference;
}

long
get_resolution_nanos(void)
{
	struct timespec res;
	if (0 != clock_getres(CLOCK_MONOTONIC, &res))
		die_errno("get clock resolution");
	return nanos(&res);
}

static void
text_handler(const char *field)
{
	if (field == NULL || strlen(field) > 4)
		counter++;
}

static void
num_handler(const char *field)
{
	if (field == NULL) {
		counter++;
		return;
	}
	char *end;
	long n = strtol(field, &end, 10);
	if (*end != '\0') {
		fprintf(stderr, "Invalid integer '%s'\n", field);
	}
	if (n == 42)
		counter++;
}

static field_handler
pick_handler(struct benchmark *benchmark, const char *type_name)
{
	if (benchmark->all_text)
		return text_handler;

	static struct { const char *name; field_handler handler; } mapping[] = {
		{ "tinyint", num_handler },
		{ "smallint", num_handler },
		{ "int", num_handler },
		{ "bigint", num_handler },
		//
		{ "char", text_handler },
		{ "varchar", text_handler },
		{ "clob", text_handler },
	};

	for (int i = 0; i < sizeof(mapping) / sizeof(mapping[0]); i++) {
		if (0 == strcmp(mapping[i].name, type_name))
			return mapping[i].handler;
	}

	fprintf(stderr, "Unknown column type '%s'\n", type_name);
	exit(1);
}

void prepare_run(struct benchmark *benchmark)
{
	common.benchmark = benchmark;

	Mapi conn = connect_to_db();
	mapi_cache_limit(conn, 3);
	MapiHdl handle = mapi_query(conn, benchmark->text);
	abort_on_error(conn, handle);

	common.ncols = mapi_get_field_count(handle);
	if (common.ncols < 1) {
		fprintf(stderr, "Query did not return any columns\n");
		exit(1);
	}
	common.handlers = calloc(sizeof(field_handler), common.ncols);
	for (int i = 0; i < common.ncols; i++) {
		const char *type = mapi_get_type(handle, i);
		common.handlers[i] = pick_handler(common.benchmark, type);
	}

	mapi_close_handle(handle);
	mapi_destroy(conn);
}


static void
disconnect_worker(struct worker *worker)
{
	if (worker->handle) {
		mapi_close_handle(worker->handle);
		worker->handle = NULL;
	}
	if (worker->conn) {
		mapi_destroy(worker->conn);
		worker->conn = NULL;
	}
}

static MapiHdl
execute_query(struct worker *worker)
{
	if (worker->conn == NULL)
		worker->conn = connect_to_db();
	const char *query = common.benchmark->text;
	if (common.benchmark->prepare) {
		if (!worker->handle) {
			worker->handle = mapi_prepare(worker->conn, query);
			abort_on_error(worker->conn, worker->handle);
		}
		mapi_execute(worker->handle);
	} else {
		if (worker->handle)
			mapi_query_handle(worker->handle, query);
		else
			worker->handle = mapi_query(worker->conn, query);
	}
	abort_on_error(worker->conn, worker->handle);
	return worker->handle;
}

static void *
run_worker(void *arg)
{
	struct worker *worker = arg;
	struct buffer *output_buffer = NULL;

	long now;
	do {
		if (common.benchmark->reconnect)
			disconnect_worker(worker);

		MapiHdl handle = execute_query(worker);
		while (mapi_fetch_row(handle)) {
			abort_on_error(worker->conn, worker->handle);
			for (int i = 0; i < common.ncols; i++) {
				const char *field = mapi_fetch_field(handle, i);
				abort_on_error(worker->conn, worker->handle);
				field_handler handler = common.handlers[i];
				handler(field);
			}
		}
		now = get_relative_nanos();
		write_output(&output_buffer, now);
	} while (now < common.deadline);
	flush_output(&output_buffer);

	disconnect_worker(worker);
	free(worker);
	return NULL;
}

void
run_executors(double duration)
{
	int nthreads = common.benchmark->parallel;
	pthread_t *threads = calloc(nthreads, sizeof(pthread_t));
	if (!threads)
		die_errno("alloc");
	get_time(&common.start);
	common.deadline = (long)(1e9 * duration);
	for (int i = 0; i < nthreads; i++) {
		struct worker *worker = malloc(sizeof(struct worker));
		if (!worker)
			die_errno("alloc worker");
		worker->conn = NULL;
		worker->handle = NULL;

		if (0 != pthread_create(&threads[i], NULL, run_worker, worker))
			die_errno("start worker");
	}
	for (int i = 0; i < nthreads; i++) {
		if (0 != pthread_join(threads[i], NULL))
			die_errno("join worker thread");
	}
	free(threads);
}



