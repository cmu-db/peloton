#include "parser/parser/pg_query.h"
#include "parser/pg_query_internal.h"
#include "parser/mb/pg_wchar.h"

const char* progname = "pg_query";

void pg_query_init(void)
{
	MemoryContextInit();
	SetDatabaseEncoding(PG_UTF8);
}
