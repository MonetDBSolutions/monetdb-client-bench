#include "runner.h"

#include <stdio.h>
#include <stdlib.h>


#include <monetdb_config.h>

static struct {
	char *db_url;
	int fetch_size;
} target_db = {
	.db_url = NULL,
	.fetch_size = -1,
 };


void
die_errno(const char *msg)
{
	fprintf(stderr, "Error: %s: %s\n", msg, strerror(errno));
	exit(1);
}


void
connect_to(char *url, int fetch_size)
{
	target_db.db_url = url;
	target_db.fetch_size = fetch_size;
}


Mapi
connect_to_db(void)
{
	// TODO fix hardcoded credentials
	Mapi conn = mapi_mapiuri(target_db.db_url, "monetdb", "monetdb", "sql");
	abort_on_error(conn, NULL);
	mapi_cache_limit(conn, target_db.fetch_size);
	abort_on_error(conn, NULL);
	mapi_reconnect(conn);
	abort_on_error(conn, NULL);
	return conn;
}


void
abort_on_error(Mapi mapi, MapiHdl handle)
{
	if (handle) {
		const char *msg = mapi_result_error(handle);
		if (msg) {
			fprintf(stderr, "Error: %s\n", msg); // no trailing newline needed
			exit(1);
		}
	}

	if (mapi == NULL) {
		if (handle != NULL) {
			return;
		} else {
			fprintf(stderr, "Unknown error: MAPI is unexpectedly NULL\n");
			exit(1);
		}
	}

	if (mapi_error(mapi) != MOK) {
		fprintf(stderr, "Error: %s\n", mapi_error_str(mapi));
		exit(1);
	}
}

int
advance_row(MapiHdl handle)
{
	int ncols = mapi_fetch_row(handle);
	abort_on_error(NULL, handle);
	return ncols;
}