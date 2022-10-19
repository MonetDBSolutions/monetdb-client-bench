#include "runner.h"

#include <stdio.h>
#include <stdlib.h>


#include <monetdb_config.h>


int
show_info(const char *db_url)
{
	printf("Compiled against MonetDB version: %s\n", MONETDB_VERSION);
	printf("MAPI version: %s\n", mapi_get_mapi_version());
	printf("Clock resolution nanos: %ld\n", get_resolution_nanos());

	if (!db_url)
		return 0;

	printf("MAPI URL: %s\n", db_url);

	Mapi conn = connect_to_db();

	MapiHdl handle = mapi_query(conn, "SELECT value FROM sys.environment WHERE name = 'monet_version'");
	abort_on_error(conn, handle);

	if (advance_row(handle)) {
		const char *version = mapi_fetch_field(handle, 0);
		abort_on_error(conn, handle);
		printf("MonetDB server version: %s\n", version);
	}

	mapi_close_handle(handle);
	mapi_destroy(conn);

	return 0;
}

