//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// conversioncmds.h
//
// Identification: src/include/parser/commands/conversioncmds.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


/*-------------------------------------------------------------------------
 *
 * conversioncmds.h
 *	  prototypes for conversioncmds.c.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/conversioncmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CONVERSIONCMDS_H
#define CONVERSIONCMDS_H

#include "parser/catalog/objectaddress.h"
#include "parser/nodes/parsenodes.h"

extern ObjectAddress CreateConversionCommand(CreateConversionStmt *parsetree);

#endif   /* CONVERSIONCMDS_H */
