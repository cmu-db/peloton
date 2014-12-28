/*-------------------------------------------------------------------------
 *
 * block_io.h
 *	  POSTGRES block io utilities definitions.
 *
 *
 * src/include/access/block_io.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOCK_IO_H
#define BLOCK_IO_H

#include "postgres.h"
#include "access/htup.h"
#include "utils/relcache.h"
#include "storage/buf.h"

#define BLOCK_FIXED_LENGTH_SIZE 100             /* In terms of number of tuples */
#define BLOCK_VARIABLE_LENGTH_SIZE 1024 * 16    /* Raw size in bytes */

#define BLOCK_POINTER_SIZE  8  /* 8 bytes */

// Storage backend information
typedef enum RelationBlockBackend{
	STORAGE_BACKEND_FS,
	STORAGE_BACKEND_VM,
	STORAGE_BACKEND_NVM
} RelationBlockBackend;

#define STORAGE_BACKEND_DEFAULT STORAGE_BACKEND_FS

extern void RelationInit(Relation relation);

extern void PrintAllRelationBlocks(Oid relationId);

extern Size RelationGetTupleLen(Relation relation);

#endif   /* BLOCK_IO_H */
