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

#define RELBLOCK_VARLEN_HEADER_SIZE     4   /* 4 bytes */

// Internal helper functions
unsigned int RoundUpToNextPowerOfTwo(unsigned int x);
Size GetAllocationSize(Size size);

RelationBlock RelationAllocateVariableLengthBlock(Relation relation,
												  RelationBlockBackend relblockbackend);
RelationBlock GetVariableLengthBlockWithFreeSpace(Relation relation,
												  RelationBlockBackend relblockbackend,
												  Size allocation_size);

void *GetVariableLengthSlotInBlock(RelationBlock relblock, Size allocation_size);
bool ReleaseVariableLengthSlotInBlock(RelationBlock relblock, void *location);

unsigned int RoundUpToNextPowerOfTwo(unsigned int x)
{
	x--;
	x |= x >> 1;  // handle  2 bit numbers
	x |= x >> 2;  // handle  4 bit numbers
	x |= x >> 4;  // handle  8 bit numbers
	x |= x >> 8;  // handle 16 bit numbers
	x |= x >> 16; // handle 32 bit numbers
	x++;
	return x;
}

RelationBlock RelationAllocateVariableLengthBlock(Relation relation,
												  RelationBlockBackend relblockbackend)
{
	MemoryContext oldcxt;
	RelationBlock     relblock = NULL;
	List          **blockListPtr = NULL;

	// Allocate block in TSM context
	oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);

	relblock = (RelationBlock) palloc(sizeof(RelationBlockData));
	relblock->rb_type = RELATION_VARIABLE_BLOCK_TYPE;
	relblock->rb_backend = relblockbackend;
	relblock->rb_size = BLOCK_VARIABLE_LENGTH_SIZE;

	relblock->rb_location = (void *) palloc(BLOCK_VARIABLE_LENGTH_SIZE);
	relblock->rb_free_space = BLOCK_VARIABLE_LENGTH_SIZE;

	elog(WARNING, "RelationBlock Size : %zd Backend : %d Type : %d", relblock->rb_size,
		 relblock->rb_backend, relblock->rb_type);

	blockListPtr = GetRelationBlockList(relation, relblockbackend, RELATION_VARIABLE_BLOCK_TYPE);
	*blockListPtr = lappend(*blockListPtr, relblock);

	MemoryContextSwitchTo(oldcxt);

	return relblock;
}

void *GetVariableLengthSlotInBlock(RelationBlock relblock, Size allocation_size)
{
	/*
	off_t  slot_itr;
	int    free_slots = relblock->rb_free_slots ;
	bool  *slotmap = relblock->rb_slotmap;

	if(free_slots == 0)
	{
		elog(ERROR, "No free slots in block %p", relblock);
		return -1;
	}

	// Update bitmap and free slot counter
	for(slot_itr = 0 ; slot_itr < NUM_REL_BLOCK_ENTRIES ; slot_itr++)
	{
		if(slotmap[slot_itr] == false)
		{
			slotmap[slot_itr] = true;
			relblock->rb_free_slots -= 1;
			break;
		}
	}

	if(slot_itr == NUM_REL_BLOCK_ENTRIES)
	{
		elog(ERROR, "No free slots in block %p", relblock);
		return -1;
	}
	*/

q	return NULL;
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
	Size allocation_size = -1;

	if(size <= 0)
	{
		elog(ERROR, "Requested size : %zd", size);
		return -1;
	}

	// add space for mm header
	size += RELBLOCK_VARLEN_HEADER_SIZE;
	allocation_size = RoundUpToNextPowerOfTwo(size);

	elog(WARNING, "Allocation request size : %zd", allocation_size);

	return allocation_size;
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
	location = GetVariableLengthSlotInBlock(relblock);

	return location;
}
