#include "pg_query.h"
#include "pg_query_internal.h"

#ifdef JSON_OUTPUT_V2
	#include "pg_query_json.h"
#endif

#include "parser/parser.h"
#include "parser/scanner.h"
#include "parser/scansup.h"

#include <unistd.h>
#include <fcntl.h>

PgQueryParseResult pg_query_parse(const char* input)
{
	MemoryContext ctx = NULL;
	PgQueryParseResult result = {0};

	char stderr_buffer[STDERR_BUFFER_LEN + 1] = {0};
#ifndef DEBUG
	int stderr_global;
	int stderr_pipe[2];
#endif

	ctx = AllocSetContextCreate(TopMemoryContext,
								"pg_query_raw_parse",
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(ctx);

#ifndef DEBUG
	// Setup pipe for stderr redirection
	if (pipe(stderr_pipe) != 0) {
		PgQueryError* error = malloc(sizeof(PgQueryError));

		error->message = strdup("Failed to open pipe, too many open file descriptors")

		result.error = error;
		result.parse_tree = strdup("[]");

		return result;
	}

	fcntl(stderr_pipe[0], F_SETFL, fcntl(stderr_pipe[0], F_GETFL) | O_NONBLOCK);

	// Redirect stderr to the pipe
	stderr_global = dup(STDERR_FILENO);
	dup2(stderr_pipe[1], STDERR_FILENO);
	close(stderr_pipe[1]);
#endif

	// Parse it!
	PG_TRY();
	{
		List *tree;
		char *tree_json;

		tree = raw_parser(input);

#ifdef JSON_OUTPUT_V2
		tree_json = pg_query_nodes_to_json(tree);
#else
		tree_json = nodeToJSONString(tree);
#endif

#ifndef DEBUG
		// Save stderr for result
		read(stderr_pipe[0], stderr_buffer, STDERR_BUFFER_LEN);
#endif

		result.parse_tree    = strdup(tree_json);
		result.stderr_buffer = strdup(stderr_buffer);

		pfree(tree_json);
	}
	PG_CATCH();
	{
		ErrorData* error_data;
		PgQueryError* error;

		MemoryContextSwitchTo(ctx);
		error_data = CopyErrorData();

		error = malloc(sizeof(PgQueryError));
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

	// Return to previous PostgreSQL memory context
	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(ctx);

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
