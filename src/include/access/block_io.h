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
#include "utils/relcache.h"
#include "storage/buf.h"

#define BLOCK_FIXED_LENGTH_SIZE 100             /* In terms of number of tuples */
#define BLOCK_VARIABLE_LENGTH_SIZE 1024 * 16    /* Raw size in bytes */

/*
 * Possible block types.
 */
typedef enum RelationBlockType
{
	/* Used to store fixed-length tuples */
	RelationFixedBlock,
	/* Used to store variable-length attributes */
	RelationVariableBlock
} RelationBlockType;

/* RelationBlock structure */
typedef struct RelationBlockData{
	Size relblocklen;

	RelationBlockType relblocktype;
} RelationBlockData;

typedef RelationBlockData* RelationBlock;

extern void RelationAllocateBlock(Relation relation);

#endif   /* BLOCK_IO_H */
