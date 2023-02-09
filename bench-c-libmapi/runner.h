#define _POSIX_C_SOURCE (200809L)

#include <mapi.h>

#include <pthread.h>
#include <time.h>

// conn.c

void die_errno(const char *msg);

void connect_to(char *url);
Mapi connect_to_db(void);
void abort_on_error(Mapi mapi, MapiHdl handle);
int advance_row(MapiHdl handle);


// bench.c
struct benchmark {
	char *text;
	bool prepare;
	bool reconnect;
	int parallel;
	bool all_text;
	long expected;
};

struct benchmark *read_benchmark(const char *filename);
void destroy_benchmark(struct benchmark *benchmark);

// output.c

struct buffer;

void init_output(void);
void write_output(struct buffer **buf_p, long timestamp);
void flush_output(struct buffer **buf_p);
void finish_output(void);
// showinfo.c

int show_info(const char *mapi_url);

// execute.c

long get_resolution_nanos(void);
void prepare_run(struct benchmark *benchmark);
void run_executors(double duration);
