#ifndef PG_QUERY_INTERNAL_H
#define PG_QUERY_INTERNAL_H

#include "parser/postgres.h"
#include "parser/utils/memutils.h"
#include "parser/nodes/pg_list.h"

#define STDERR_BUFFER_LEN 4096
#define DEBUG

typedef struct {
  List *tree;
  char* stderr_buffer;
  PgQueryError* error;
} PgQueryInternalParsetreeAndError;

PgQueryInternalParsetreeAndError pg_query_raw_parse(const char* input);

MemoryContext pg_query_enter_memory_context(const char* ctx_name);
void pg_query_exit_memory_context(MemoryContext ctx);

#endif
