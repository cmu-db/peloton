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
RelationBlock RelationAllocateFixedLengthBlock(Relation relation,
											   RelationBlockBackend relblockbackend);

RelationBlock GetFixedLengthBlockWithFreeSlot(Relation relation,
											  RelationBlockBackend relblockbackend);

OffsetNumber GetFixedLengthSlotInBlock(RelationBlock relblock);
bool ReleaseFixedLengthSlotInBlock(RelationBlock relblock, OffsetNumber slot_id);

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

	// bitmap tracking slot status
	relblock->rb_slotmap = (bool *) palloc0(NUM_REL_BLOCK_ENTRIES);
	// free slot counter
	relblock->rb_free_slots = NUM_REL_BLOCK_ENTRIES;
	// tuple headers
	relblock->rb_tuple_headers = (HeapTupleHeader *) palloc(NUM_REL_BLOCK_ENTRIES);

	foreach(l, rel_column_groups)
	{
		rel_column_group = lfirst(l);

		cg_tup_size = rel_column_group->cg_size;
		cg_block_size = cg_tup_size * NUM_REL_BLOCK_ENTRIES;
		relblock->rb_size += cg_block_size;

		blockData = (void *) palloc(cg_block_size);

		elog(WARNING, "CG size : %zd location : %p", cg_block_size, blockData);

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

OffsetNumber GetFixedLengthSlotInBlock(RelationBlock relblock)
{
	OffsetNumber  slot_itr = InvalidOffsetNumber;
	int    slot_offset;
	int    free_slots = relblock->rb_free_slots ;
	bool  *slotmap = relblock->rb_slotmap;

	if(free_slots == 0)
	{
		elog(ERROR, "No free slots in block %p", relblock);
		return InvalidOffsetNumber;
	}

	// Update bitmap and free slot counter
	for(slot_itr = FirstOffsetNumber ; slot_itr <= NUM_REL_BLOCK_ENTRIES ; slot_itr++)
	{
		slot_offset = slot_itr - 1;
		if(slotmap[slot_offset] == false)
		{
			slotmap[slot_offset] = true;
			relblock->rb_free_slots -= 1;
			break;
		}
	}

	if(slot_itr == NUM_REL_BLOCK_ENTRIES)
	{
		elog(ERROR, "No free slots in block %p", relblock);
		return InvalidOffsetNumber;
	}

	return slot_itr;
}

bool ReleaseFixedLengthSlotInBlock(RelationBlock relblock, OffsetNumber slot_id)
{
	bool  *slotmap = relblock->rb_slotmap;
	int   slot_offset = slot_id - 1;

	// Check if id makes sense
	if(slot_id == InvalidOffsetNumber || slot_id > NUM_REL_BLOCK_ENTRIES)
		return false;

	// Update bitmap and free slot counter
	slotmap[slot_offset] = false;
	relblock->rb_free_slots += 1;

	// XXX should we release the block if all slots are empty ?

	return true;
}


RelationBlock GetFixedLengthBlockWithFreeSlot(Relation relation,
											  RelationBlockBackend relblockbackend)
{
	List       **blockListPtr = NULL;
	List        *blockList = NULL;
	ListCell    *l = NULL;
	RelationBlock relblock = NULL;
	bool        blockfound;

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

			if(relblock->rb_free_slots > 0)
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

	return relblock;
}

RelBlockLocation GetFixedLengthSlot(Relation relation, RelationBlockBackend relblockbackend)
{
	OffsetNumber     relblock_offset = InvalidOffsetNumber;
	RelationBlock    relblock = NULL;
	RelBlockLocation location;

	relblock = GetFixedLengthBlockWithFreeSlot(relation, relblockbackend);

	relblock_offset = GetFixedLengthSlotInBlock(relblock);

	location.rb_location = relblock;
	location.rb_offset = relblock_offset;

	return location;
}
