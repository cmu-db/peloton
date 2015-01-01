/*-------------------------------------------------------------------------
 *
 * relblock_fixed.c
 *	  Fixed-length block utilities.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/relblock_fixed.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relblock.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "storage/bufmgr.h"

// Internal helper functions

List** GetRelationBlockList(Relation relation, RelationBlockBackend relblockbackend,
							RelationBlockType relblocktype);
void PrintRelationBlockList(Relation relation, RelationBlockBackend relblockbackend,
							RelationBlockType relblocktype);
void PrintAllRelationBlocks(Relation relation);

RelationBlock RelationAllocateFixedLengthBlock(Relation relation,
											   RelationBlockBackend relblockbackend);

List** GetRelationBlockList(Relation relation, RelationBlockBackend relblockbackend,
							RelationBlockType relblocktype)
{
	List       **blockListPtr = NULL;

	// Pick relevant list based on backend and block type
	if(relblockbackend == STORAGE_BACKEND_VM)
	{
		if(relblocktype == RELATION_FIXED_BLOCK_TYPE)
			blockListPtr = &relation->rd_relblock_info->rel_fixed_blocks_on_VM;
		else if(relblocktype == RELATION_VARIABLE_BLOCK_TYPE)
			blockListPtr = &relation->rd_relblock_info->rel_variable_blocks_on_VM;
	}
	else if(relblockbackend == STORAGE_BACKEND_NVM)
	{
		if(relblocktype == RELATION_FIXED_BLOCK_TYPE)
			blockListPtr = &relation->rd_relblock_info->rel_fixed_blocks_on_NVM;
		else if(relblocktype == RELATION_VARIABLE_BLOCK_TYPE)
			blockListPtr = &relation->rd_relblock_info->rel_variable_blocks_on_NVM;
	}

	if(blockListPtr == NULL)
	{
		elog(ERROR, "blockListPtr must not be %p", blockListPtr);
	}

	return blockListPtr;
}

void PrintRelationBlockList(Relation relation, RelationBlockBackend relblockbackend,
							RelationBlockType relblocktype)
{
	List       **blockListPtr = NULL;
	List        *blockList = NULL;
	ListCell    *l = NULL;

	blockListPtr = GetRelationBlockList(relation, relblockbackend, relblocktype);
	blockList = *blockListPtr;

	elog(WARNING, "PR BLOCK :: Backend : %d Type : %d List : %p", relblockbackend, relblocktype, blockList);

	foreach (l, blockList)
	{
		RelationBlock relblock = lfirst(l);
		elog(WARNING, "[ %p ] ->", relblock);

		//if(relblock != NULL)
		//	elog(WARNING, "%zd %p", relblock->relblocklen, relblock->relblockdata);
	}
}

void PrintAllRelationBlocks(Relation relation)
{
	elog(WARNING, "--------------------------------------------");
	elog(WARNING, "PID :: %d", getpid());
	elog(WARNING, "ALL_BLOCKS :: relation :: %d %s", RelationGetRelid(relation),
		 RelationGetRelationName(relation));
	PrintRelationBlockList(relation, STORAGE_BACKEND_VM, RELATION_FIXED_BLOCK_TYPE);
	elog(WARNING, "--------------------------------------------\n");
}

RelationBlock RelationAllocateFixedLengthBlock(Relation relation,
											   RelationBlockBackend relblockbackend)
{
	MemoryContext oldcxt;
	RelationBlock     relblock = NULL;
	RelationBlockInfo relblockinfo = NULL;
	RelationColumnGroup rel_column_group = NULL;
	Size          cg_block_size = -1;
	Size          cg_tup_size = -1;
	List          *rel_column_groups;
	void          *blockData = NULL;
	List          **blockListPtr = NULL;
	ListCell      *l = NULL;

	relblockinfo = relation->rd_relblock_info;
	rel_column_groups = relblockinfo->rel_column_groups;

	// Allocate block in TSM context
	oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);

	relblock = (RelationBlock) palloc(sizeof(RelationBlockData));
	relblock->rb_type = RELATION_FIXED_BLOCK_TYPE;
	relblock->rb_backend = relblockbackend;

	relblock->rb_slotmap = (bool *) palloc0(NUM_REL_BLOCK_ENTRIES);
	relblock->rb_free_slots = NUM_REL_BLOCK_ENTRIES;

	foreach(l, rel_column_groups)
	{
		rel_column_group = lfirst(l);

		cg_tup_size = rel_column_group->cg_size;
		cg_block_size = cg_tup_size * NUM_REL_BLOCK_ENTRIES;
		relblock->rb_size += cg_block_size;

		blockData = (void *) palloc(cg_block_size);

		elog(WARNING, "CG size : %zd", cg_block_size);

		// Append cg block to cg locations
		relblock->rb_cg_locations = lappend(relblock->rb_cg_locations, blockData);
	}

	elog(WARNING, "RelationBlock Size : %zd Backend : %d Type : %d", relblock->rb_size,
		 relblock->rb_backend, relblock->rb_type);

	blockListPtr = GetRelationBlockList(relation, relblockbackend, RELATION_FIXED_BLOCK_TYPE);
	*blockListPtr = lappend(*blockListPtr, relblock);

	RelBlockTablePrint();

	MemoryContextSwitchTo(oldcxt);

	return relblock;
}

off_t GetFixedLengthSlot(Relation relation, RelationBlockBackend relblockbackend)
{
	off_t        relblock_offset = -1;
	List       **blockListPtr = NULL;
	List        *blockList = NULL;
	ListCell    *l = NULL;
	bool        blockfound;
	RelationBlock relblock = NULL;

	blockListPtr = GetRelationBlockList(relation, relblockbackend, RELATION_FIXED_BLOCK_TYPE);

	/* empty block list */
	if(*blockListPtr == NULL)
	{
		relblock = RelationAllocateFixedLengthBlock(relation, relblockbackend);
	}
	else
	{
		blockList = *blockListPtr;
		blockfound = false;

		/* check for a block with free space */
		foreach (l, blockList)
		{
			relblock = lfirst(l);

			if(relblock->rb_free_slots != 0)
			{
				blockfound = true;
				break;
			}
		}

		if(blockfound == false)
		{
			relblock = RelationAllocateFixedLengthBlock(relation, relblockbackend);
		}
	}

	/* Must have found the required block */

	elog(WARNING, "FL block :: Size : %zd Backend : %d Type : %d Free slots : %d", relblock->rb_size,
		 relblock->rb_backend, relblock->rb_type, relblock->rb_free_slots);

	return relblock_offset;
}
