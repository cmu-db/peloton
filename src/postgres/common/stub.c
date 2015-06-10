/*-------------------------------------------------------------------------
 *
 * stub.c
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/postgres/common/stub.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/nodes.h"
#include "mb/pg_wchar.h"
#include "utils/fmgrtab.h"
#include "utils/palloc.h"

// src/include/bootstrap/bootstrap.h

int  boot_yyparse(void) { }

// src/include/replication/walsender_private.h

Node *replication_parse_result;

int replication_yyparse(void) { }

void replication_scanner_init(const char *query_string) { }

// include/utils/fmgrtab

const FmgrBuiltin fmgr_builtins[];

const int fmgr_nbuiltins;  /* number of entries in table */
