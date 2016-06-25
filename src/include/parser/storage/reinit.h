//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// reinit.h
//
// Identification: src/include/parser/storage/reinit.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


/*-------------------------------------------------------------------------
 *
 * reinit.h
 *	  Reinitialization of unlogged relations
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/reinit.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef REINIT_H
#define REINIT_H

extern void ResetUnloggedRelations(int op);

#define UNLOGGED_RELATION_CLEANUP		0x0001
#define UNLOGGED_RELATION_INIT			0x0002

#endif   /* REINIT_H */
