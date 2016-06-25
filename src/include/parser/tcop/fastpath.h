//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// fastpath.h
//
// Identification: src/include/parser/tcop/fastpath.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


/*-------------------------------------------------------------------------
 *
 * fastpath.h
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/fastpath.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FASTPATH_H
#define FASTPATH_H

#include "parser/lib/stringinfo.h"

extern int GetOldFunctionMessage(StringInfo buf);
extern int	HandleFunctionRequest(StringInfo msgBuf);

#endif   /* FASTPATH_H */
