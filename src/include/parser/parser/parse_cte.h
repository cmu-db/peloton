//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parse_cte.h
//
// Identification: src/include/parser/parser/parse_cte.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


/*-------------------------------------------------------------------------
 *
 * parse_cte.h
 *	  handle CTEs (common table expressions) in parser
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_cte.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_CTE_H
#define PARSE_CTE_H

#include "parser/parse_node.h"

extern List *transformWithClause(ParseState *pstate, WithClause *withClause);

extern void analyzeCTETargetList(ParseState *pstate, CommonTableExpr *cte,
					 List *tlist);

#endif   /* PARSE_CTE_H */
