/*-------------------------------------------------------------------------
 *
 * relblock_varlen.c
 *	  Variable-length block utilities.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/relblock_varlen.c
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

#include <math.h>

// Internal helper functions
Size GetAllocationSize(Size size);

RelationBlock RelationAllocateVariableLengthBlock(Relation relation,
												  RelationBlockBackend relblockbackend);
RelationBlock GetVariableLengthBlockWithFreeSpace(Relation relation,
												  RelationBlockBackend relblockbackend,
												  Size allocation_size);

void *GetVariableLengthSlotInBlock(RelationBlock relblock, Size allocation_size);
bool ReleaseVariableLengthSlotInBlock(RelationBlock relblock, void *location);

void PrintAllSlotsInVariableLengthBlock(RelationBlock relblock);

RelationBlock RelationAllocateVariableLengthBlock(Relation relation,
												  RelationBlockBackend relblockbackend)
{
	MemoryContext oldcxt;
	RelationBlock     relblock = NULL;
	List          **blockListPtr = NULL;
	void           *relblockdata = NULL;
	RelBlockVarlenHeader   slot_header = NULL;

	// Allocate block in TSM context
	oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);

	relblock = (RelationBlock) palloc(sizeof(RelationBlockData));
	relblock->rb_type = RELATION_VARIABLE_BLOCK_TYPE;
	relblock->rb_backend = relblockbackend;


	relblockdata = (void *) palloc(BLOCK_VARIABLE_LENGTH_SIZE);
	relblock->rb_location = relblockdata;

	// setup block header
	slot_header = relblockdata;

	slot_header->vb_slot_status = false;
	slot_header->vb_slot_length = BLOCK_VARIABLE_LENGTH_SIZE;
	slot_header->vb_prev_slot_length = 0;

	relblock->rb_size = BLOCK_VARIABLE_LENGTH_SIZE;
	relblock->rb_free_space = BLOCK_VARIABLE_LENGTH_SIZE;

	elog(WARNING, "RelationBlock Size : %zd Backend : %d Type : %d", relblock->rb_size,
		 relblock->rb_backend, relblock->rb_type);

	blockListPtr = GetRelationBlockList(relation, relblockbackend, RELATION_VARIABLE_BLOCK_TYPE);
	*blockListPtr = lappend(*blockListPtr, relblock);

	MemoryContextSwitchTo(oldcxt);

	return relblock;
}

void PrintAllSlotsInVariableLengthBlock(RelationBlock relblock)
{
	RelBlockVarlenHeader block_begin = NULL, block_end = NULL;
	RelBlockVarlenHeader slot_itr = NULL;
	Size    slot_size = -1;

	block_begin = relblock->rb_location;
	block_end = block_begin + relblock->rb_size;

	for(slot_itr = block_begin ; slot_itr < block_end ; )
	{
		slot_size = slot_itr->vb_slot_length;

		elog(WARNING, "Slot :: Status : %d Size : %d Prev Size : %d",
			 slot_itr->vb_slot_status, slot_itr->vb_slot_length, slot_itr->vb_prev_slot_length);

		slot_itr += slot_size;
	}
}

void *GetVariableLengthSlotInBlock(RelationBlock relblock, Size allocation_size)
{
	RelBlockVarlenHeader block_begin = NULL, block_end = NULL;
	RelBlockVarlenHeader slot_itr = NULL, next_slot = NULL;
	Size    slot_size = -1;
	void  *location = NULL;

	if(relblock->rb_free_space == 0)
	{
		elog(ERROR, "No free space in block %p", relblock);
		return NULL;
	}

	block_begin = relblock->rb_location;
	block_end = block_begin + relblock->rb_size;

	// Go over all slots to find the required slot
	for(slot_itr = block_begin ; slot_itr < block_end ; )
	{
		slot_size = slot_itr->vb_slot_length;

		if(slot_itr->vb_slot_status == false)
		{
			if(slot_size > allocation_size)
			{
				slot_itr->vb_slot_status = true;
				slot_itr->vb_slot_length = allocation_size;

				// update newly created slot
				next_slot = slot_itr + allocation_size;
				next_slot->vb_slot_length = slot_size - allocation_size;
				next_slot->vb_prev_slot_length = allocation_size;

				relblock->rb_free_space -= allocation_size;
				break;
			}
			else if(slot_size == allocation_size)
			{
				// just update current slot
				slot_itr->vb_slot_status = true;
				slot_itr->vb_slot_length = allocation_size;

				relblock->rb_free_space -= allocation_size;
				break;
			}
		}

		slot_itr += slot_size;
	}

	PrintAllSlotsInVariableLengthBlock(relblock);

	if(slot_itr >= block_end)
	{
		elog(ERROR, "No free space in block %p", relblock);
		return NULL;
	}

	location = slot_itr + RELBLOCK_VARLEN_HEADER_SIZE;

	return location;
}

bool ReleaseVariableLengthSlotInBlock(RelationBlock relblock, void *location)
{
	/*
	  bool  *slotmap = relblock->rb_slotmap;

	  // Check if id makes sense
	  if(slot_id < 0 || slot_id > NUM_REL_BLOCK_ENTRIES)
	  return false;

	  // Update bitmap and free slot counter
	  slotmap[slot_id] = false;
	  relblock->rb_free_slots += 1;
	*/

	return true;
}

RelationBlock GetVariableLengthBlockWithFreeSpace(Relation relation,
												  RelationBlockBackend relblockbackend,
												  Size allocation_size)
{
	List       **blockListPtr = NULL;
	List        *blockList = NULL;
	ListCell    *l = NULL;
	RelationBlock relblock = NULL;
	bool        blockfound;

	blockListPtr = GetRelationBlockList(relation, relblockbackend, RELATION_VARIABLE_BLOCK_TYPE);

	/* empty block list */
	if(*blockListPtr == NULL)
	{
		relblock = RelationAllocateVariableLengthBlock(relation, relblockbackend);
	}
	else
	{
		blockList = *blockListPtr;
		blockfound = false;

		/* check for a block with sufficient free space */
		foreach (l, blockList)
		{
			relblock = lfirst(l);
			elog(WARNING, "Free space : %zd", relblock->rb_free_space);

			if(relblock->rb_free_space > allocation_size)
			{
				blockfound = true;
				break;
			}
		}

		if(blockfound == false)
		{
			relblock = RelationAllocateVariableLengthBlock(relation, relblockbackend);
		}
	}

	return relblock;
}

Size GetAllocationSize(Size size)
{
	if(size <= 0)
	{
		elog(ERROR, "Requested size : %zd", size);
		return -1;
	}

	// add space for mm header
	size += RELBLOCK_VARLEN_HEADER_SIZE;

	elog(WARNING, "Allocation request size : %zd", size);

	return size;
}

void *GetVariableLengthSlot(Relation relation,	RelationBlockBackend relblockbackend,
							Size size)
{
	void *location = NULL;
	Size  allocation_size = -1;
	RelationBlock relblock = NULL;

	allocation_size = GetAllocationSize(size);

	relblock = GetVariableLengthBlockWithFreeSpace(relation, relblockbackend, allocation_size);

	/* Must have found the required block */

	elog(WARNING, "VL block :: Size : %zd Free space : %zd", relblock->rb_size,	relblock->rb_free_space);

	location = GetVariableLengthSlotInBlock(relblock, allocation_size);
	elog(WARNING, "VL block :: Location : %p", location);

	return location;
}
