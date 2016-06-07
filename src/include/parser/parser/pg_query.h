#ifndef PG_QUERY_H
#define PG_QUERY_H

#include <stdbool.h>

typedef struct {
	char* message; // exception message
	char* filename; // source of exception (e.g. parse.l)
	int lineno; // source of exception (e.g. 104)
	int cursorpos; // char in query at which exception occurred
} PgQueryError;

typedef struct {
  char* parse_tree;
  char* stderr_buffer;
  PgQueryError* error;
} PgQueryParseResult;

typedef struct {
  char* hexdigest;
  char* stderr_buffer;
  PgQueryError* error;
} PgQueryFingerprintResult;

typedef struct {
  char* normalized_query;
  PgQueryError* error;
} PgQueryNormalizeResult;

#ifdef __cplusplus
extern "C" {
#endif

void pg_query_init(void);
PgQueryNormalizeResult pg_query_normalize(const char* input);
PgQueryParseResult pg_query_parse(const char* input);
void pg_query_free_normalize_result(PgQueryNormalizeResult result);
void pg_query_free_parse_result(PgQueryParseResult result);
void pg_query_free_fingerprint_result(PgQueryFingerprintResult result);

PgQueryFingerprintResult pg_query_fingerprint(const char* input);
PgQueryFingerprintResult pg_query_fingerprint_with_opts(const char* input, bool printTokens);

#ifdef __cplusplus
}
#endif

#endif
