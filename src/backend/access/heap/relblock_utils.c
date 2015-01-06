/*-------------------------------------------------------------------------
 *
 * relblock_utils.c
 *	  relblock utilities
 *
 * Portions Copyright (c) 2014-2015, Carnegie Mellon University
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/relblock_utils.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <execinfo.h>

void pg_print_backtrace()
{
	int i;
	int trace_size;
	void * trace[32];
	char ** messages = (char **)NULL;

	trace_size = backtrace (trace, 32);
	messages = (char **) backtrace_symbols (trace, trace_size);

	elog(WARNING,"\n");
	elog(WARNING, "-----------------------------------------");

	for (i = 0; i < trace_size; i++) {
		elog(WARNING, "[%d] %s", i, messages[i]);
	}

	elog(WARNING, "-----------------------------------------\n");

	free(messages);
}
