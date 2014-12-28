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

#include "access/heapam.h"
#include "access/htup.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "storage/buf.h"

#define BLOCK_FIXED_LENGTH_SIZE 100             /* In terms of number of tuples */
#define BLOCK_VARIABLE_LENGTH_SIZE 1024 * 16    /* Raw size in bytes */

#define BLOCK_POINTER_SIZE  8  /* 8 bytes */

/*
 * Possible block types.
 */
typedef enum RelationBlockType
{
	/* Used to store fixed-length tuples */
	RELATION_FIXED_BLOCK_TYPE,
	/* Used to store variable-length attributes */
	RELATION_VARIABLE_BLOCK_TYPE
} RelationBlockType;

/* RelationBlock structure */
typedef struct RelationBlockData{

	RelationBlockType relblocktype;

	RelationBlockBackend relblockbackend;

	void *relblockdata;

	Size relblocklen;
} RelationBlockData;

typedef RelationBlockData* RelationBlock;

extern void RelationInitAllocateBlock(Relation relation);

extern void PrintAllRelationBlocks(Relation relation);

#endif   /* BLOCK_IO_H */
