#include "parser/parser/pg_query.h"
#include "parser/pg_query_internal.h"
#include "parser/mb/pg_wchar.h"

#include "common/logger.h"

const char* progname = "pg_query";

void pg_query_init(void)
{
  peloton::LOG_INFO("Init PG Query");
	MemoryContextInit();
	SetDatabaseEncoding(PG_UTF8);
}

void pg_query_destroy(void)
{
  peloton::LOG_INFO("Destroy PG Query");
  MemoryContextDelete(TopMemoryContext);
}
