#include "parser/parser/pg_query.h"
#include "parser/pg_query_internal.h"
#include "parser/pg_query_json.h"

#include "parser/parser/parser.h"
#include "parser/parser/scanner.h"
#include "parser/parser/scansup.h"

#include <unistd.h>
#include <fcntl.h>

MemoryContext pg_query_enter_memory_context(const char* ctx_name)
{
	MemoryContext ctx = NULL;

	ctx = AllocSetContextCreate(TopMemoryContext,
								ctx_name,
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(ctx);

	return ctx;
}

void pg_query_exit_memory_context(MemoryContext ctx)
{
	// Return to previous PostgreSQL memory context
	MemoryContextSwitchTo(TopMemoryContext);

	MemoryContextDelete(ctx);
}

PgQueryInternalParsetreeAndError pg_query_raw_parse(const char* input)
{
	PgQueryInternalParsetreeAndError result = {0};
	MemoryContext parse_context = CurrentMemoryContext;

	char stderr_buffer[STDERR_BUFFER_LEN + 1] = {0};
#ifndef DEBUG
	int stderr_global;
	int stderr_pipe[2];
#endif

#ifndef DEBUG
	// Setup pipe for stderr redirection
	if (pipe(stderr_pipe) != 0) {
		PgQueryError* error = malloc(sizeof(PgQueryError));

		error->message = strdup("Failed to open pipe, too many open file descriptors")

		result.error = error;

		return result;
	}

	fcntl(stderr_pipe[0], F_SETFL, fcntl(stderr_pipe[0], F_GETFL) | O_NONBLOCK);

	// Redirect stderr to the pipe
	stderr_global = dup(STDERR_FILENO);
	dup2(stderr_pipe[1], STDERR_FILENO);
	close(stderr_pipe[1]);
#endif

	PG_TRY();
	{
		result.tree = raw_parser(input);

#ifndef DEBUG
		// Save stderr for result
		read(stderr_pipe[0], stderr_buffer, STDERR_BUFFER_LEN);
#endif

		result.stderr_buffer = strdup(stderr_buffer);
	}
	PG_CATCH();
	{
		ErrorData* error_data;
		PgQueryError* error;

		MemoryContextSwitchTo(parse_context);
		error_data = CopyErrorData();

		// Note: This is intentionally malloc so exiting the memory context doesn't free this
		error = (PgQueryError *)malloc(sizeof(PgQueryError));
		error->message   = strdup(error_data->message);
		error->filename  = strdup(error_data->filename);
		error->lineno    = error_data->lineno;
		error->cursorpos = error_data->cursorpos;

		result.error = error;
		FlushErrorState();
	}
	PG_END_TRY();

#ifndef DEBUG
	// Restore stderr, close pipe
	dup2(stderr_global, STDERR_FILENO);
	close(stderr_pipe[0]);
	close(stderr_global);
#endif

	return result;
}

PgQueryParseResult pg_query_parse(const char* input)
{
	MemoryContext ctx = NULL;
	PgQueryInternalParsetreeAndError parsetree_and_error;
	PgQueryParseResult result = {0};

	ctx = pg_query_enter_memory_context("pg_query_parse");

	parsetree_and_error = pg_query_raw_parse(input);

	// These are all malloc-ed and will survive exiting the memory context, the caller is responsible to free them now
	result.stderr_buffer = parsetree_and_error.stderr_buffer;
	result.error = parsetree_and_error.error;

	if (parsetree_and_error.tree != NULL) {
		char *tree_json;

		tree_json = pg_query_nodes_to_json(parsetree_and_error.tree);

		result.parse_tree = strdup(tree_json);
		pfree(tree_json);
	} else {
		result.parse_tree = strdup("[]");
	}

	pg_query_exit_memory_context(ctx);

	return result;
}

void pg_query_free_parse_result(PgQueryParseResult result)
{
  if (result.error) {
    free(result.error->message);
    free(result.error->filename);
    free(result.error);
  }

  free(result.parse_tree);
  free(result.stderr_buffer);
}
