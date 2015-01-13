/*-------------------------------------------------------------------------
 *
 * rel_block_varlen.c
 *	  Variable-length block utilities.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/rel_block_varlen.c
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
Size GetAllocationSize(Size size);

RelBlock RelAllocateVariableLengthBlock(Relation relation);
RelBlock GetVariableLengthBlockWithFreeSpace(Relation relation, Size allocation_size);

void *GetVariableLengthSlotInBlock(RelBlock relblock, Size allocation_size);

RelBlock GetVariableLengthBlockContainingSlot(Relation relation, void *location);
void PrintAllSlotsInVariableLengthBlock(RelBlock relblock);

bool CheckVariableLengthBlock(RelBlock relblock);

RelBlock RelAllocateVariableLengthBlock(Relation relation)
{
	MemoryContext oldcxt;
	RelBlock     rel_block = NULL;
	List          **block_list_ptr = NULL;
	void           *rel_block_data = NULL;
	RelBlockVarlenHeader   slot_header = NULL;

	// Allocate block in TSM context
	oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);

	rel_block = (RelBlock) palloc(sizeof(RelBlockData));
	rel_block->rb_type = RELATION_VARIABLE_BLOCK_TYPE;


	rel_block_data = (void *) palloc(BLOCK_VARIABLE_LENGTH_SIZE);
	rel_block->rb_location = rel_block_data;

	// setup block header
	slot_header = rel_block_data;

	slot_header->vb_slot_status = false;
	slot_header->vb_slot_length = BLOCK_VARIABLE_LENGTH_SIZE;
	slot_header->vb_prev_slot_length = 0;

	rel_block->rb_size = BLOCK_VARIABLE_LENGTH_SIZE;
	rel_block->rb_free_space = BLOCK_VARIABLE_LENGTH_SIZE;

	elog(WARNING, "RelationBlock Size : %zd Type : %d", rel_block->rb_size,
		 rel_block->rb_type);

	block_list_ptr = GetRelBlockList(relation, RELATION_VARIABLE_BLOCK_TYPE);
	*block_list_ptr = lappend(*block_list_ptr, rel_block);

	MemoryContextSwitchTo(oldcxt);

	return rel_block;
}

void PrintAllSlotsInVariableLengthBlock(RelBlock relblock)
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

void *GetVariableLengthSlotInBlock(RelBlock relblock, Size allocation_size)
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
				next_slot->vb_slot_status = false;
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

	if(slot_itr >= block_end)
	{
		elog(ERROR, "No free space in block %p", relblock);
		return NULL;
	}

	location = (void *) slot_itr + RELBLOCK_VARLEN_HEADER_SIZE;

	if(CheckVariableLengthBlock(relblock) != true)
	{
		elog(ERROR, "Sanity tests failed");
	}

	return location;
}

RelBlock GetVariableLengthBlockContainingSlot(Relation relation, void *location)
{
	RelBlockVarlenHeader block_begin = NULL, block_end = NULL;
	List       **block_list_ptr = NULL;
	List        *block_list = NULL;
	ListCell    *l = NULL;
	RelBlock    rel_block = NULL;
	bool        block_found;

	block_list_ptr = GetRelBlockList(relation, RELATION_VARIABLE_BLOCK_TYPE);

	/* empty block list */
	if(*block_list_ptr == NULL)
	{
		return NULL;
	}
	else
	{
		block_list = *block_list_ptr;
		block_found = false;

		/* check for a block containing location */
		foreach (l, block_list)
		{
			rel_block = lfirst(l);

			block_begin = rel_block->rb_location;
			block_end = block_begin + rel_block->rb_size;

			if(location > (void *) block_begin && location < (void *) block_end)
			{
				block_found = true;
				break;
			}
		}

		if(block_found == false)
		{
			return NULL;
		}
	}

	return rel_block;
}

void ReleaseVariableLengthSlot(Relation relation, void *location)
{
	RelBlockVarlenHeader cur_slot = NULL, next_slot = NULL, prev_slot = NULL, next_next_slot = NULL;
	RelBlockVarlenHeader block_begin = NULL, block_end = NULL;
	Size    slot_length, prev_slot_length, slot_size = -1;
	bool     merge_prev_slot = false, merge_next_slot = false;
	RelBlock relblock;

	// Find the relevant relation block
	relblock = GetVariableLengthBlockContainingSlot(relation, location);

	if(relblock == NULL)
	{
		elog(ERROR, "No block found containing location %p", location);
	}

	cur_slot = location - RELBLOCK_VARLEN_HEADER_SIZE;

	//elog(WARNING, "Cur slot : status %d length %d ", cur_slot->vb_slot_status,
	//	 cur_slot->vb_slot_length);

	slot_length = cur_slot->vb_slot_length;
	prev_slot_length = cur_slot->vb_prev_slot_length;

	// Check if prev and/or next slots can be merged with cur slot
	if(prev_slot_length != 0)
		prev_slot = cur_slot - prev_slot_length;

	block_begin = relblock->rb_location;
	block_end = block_begin + relblock->rb_size;
	next_slot = cur_slot + cur_slot->vb_slot_length;

	if(next_slot >= block_end)
		next_slot = NULL;

	if(prev_slot != NULL && prev_slot->vb_slot_status == false)
		merge_prev_slot = true;

	if(next_slot != NULL && next_slot->vb_slot_status == false)
		merge_next_slot = true;

	//elog(WARNING, "Merge prev slot : %d  Merge next slot : %d", merge_prev_slot, merge_next_slot);

	// Case - 1
	if(merge_prev_slot == false && merge_next_slot == false)
	{
		slot_size = slot_length;

		cur_slot->vb_slot_status = false;
	}
	// Case - 2
	else if(merge_prev_slot == true && merge_next_slot == false)
	{
		slot_size = slot_length + prev_slot->vb_slot_length;

		prev_slot->vb_slot_length = slot_size;
		next_slot->vb_prev_slot_length = slot_size;
	}
	// Case - 3
	else if(merge_prev_slot == false && merge_next_slot == true)
	{
		slot_size = slot_length + next_slot->vb_slot_length;

		cur_slot->vb_slot_status = false;
		cur_slot->vb_slot_length = slot_size;

		next_next_slot = next_slot + next_slot->vb_slot_length;
		if(next_next_slot < block_end)
			next_next_slot->vb_prev_slot_length = slot_size;
	}
	// Case - 4
	else
	{
		slot_size = prev_slot->vb_slot_length + slot_length +
			next_slot->vb_slot_length;

		prev_slot->vb_slot_length = slot_size;

		next_next_slot = next_slot + next_slot->vb_slot_length;
		if(next_next_slot < block_end)
			next_next_slot->vb_prev_slot_length = slot_size;
	}

	// Update block free space
	relblock->rb_free_space += slot_length;

	if(CheckVariableLengthBlock(relblock) != true)
	{
		elog(ERROR, "Sanity tests failed");
	}
}

RelBlock GetVariableLengthBlockWithFreeSpace(Relation relation, Size allocation_size)
{
	List       **block_list_ptr = NULL;
	List        *block_list = NULL;
	ListCell    *l = NULL;
	RelBlock    rel_block = NULL;
	bool        block_found;

	block_list_ptr = GetRelBlockList(relation, RELATION_VARIABLE_BLOCK_TYPE);

	/* empty block list */
	if(*block_list_ptr == NULL)
	{
		rel_block = RelAllocateVariableLengthBlock(relation);
	}
	else
	{
		block_list = *block_list_ptr;
		block_found = false;

		/* check for a block with sufficient free space */
		foreach (l, block_list)
		{
			rel_block = lfirst(l);
			//elog(WARNING, "Free space : %zd", relblock->rb_free_space);

			if(rel_block->rb_free_space > allocation_size)
			{
				block_found = true;
				break;
			}
		}

		if(block_found == false)
			rel_block = RelAllocateVariableLengthBlock(relation);
	}

	return rel_block;
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
	return size;
}

bool CheckVariableLengthBlock(RelBlock relblock)
{
	RelBlockVarlenHeader block_begin = NULL, block_end = NULL;
	RelBlockVarlenHeader slot_itr = NULL, next_slot = NULL;
	Size    slot_size = -1;
	bool    slot_status;
	Size    free_space = 0;

	if(relblock->rb_free_space < 0 || relblock->rb_free_space > relblock->rb_size)
	{
		elog(WARNING, "free space not valid : free space %zd size : %zd",
			 relblock->rb_free_space, relblock->rb_size);
		return false;
	}

	block_begin = relblock->rb_location;
	block_end = block_begin + relblock->rb_size;

	// Go over all slots and run sanity tests on each slot
	for(slot_itr = block_begin ; slot_itr < block_end ; )
	{
		slot_size = slot_itr->vb_slot_length;
		slot_status = slot_itr->vb_slot_status;

		if(slot_status == false)
			free_space += slot_size;

		if(slot_size < 0 || slot_size > relblock->rb_size)
		{
			elog(WARNING, "slot size not valid %zd", slot_size);
			return false;
		}

		// Parse next slot
		next_slot = slot_itr + slot_size;
		if(next_slot >= block_end)
			break;

		if(next_slot->vb_prev_slot_length != slot_size)
		{
			elog(WARNING, "next slot prev size does not match : actual %d expected %zd",
				 next_slot->vb_prev_slot_length, slot_size);
			return false;
		}

		if(slot_status == false && next_slot->vb_slot_status == false)
		{
			elog(WARNING, "two consecutive empty slots");
			return false;
		}

		slot_itr += slot_size;
	}

	if(free_space != relblock->rb_free_space)
	{
		elog(WARNING, "free space tally does not match : actual %zd  expected %zd",
			 free_space, relblock->rb_free_space);
		return false;
	}

	return true;
}

void *GetVariableLengthSlot(Relation relation, Size size)
{
	void *location = NULL;
	Size  allocation_size = -1;
	RelBlock rel_block = NULL;

	allocation_size = GetAllocationSize(size);

	rel_block = GetVariableLengthBlockWithFreeSpace(relation, allocation_size);

	location = GetVariableLengthSlotInBlock(rel_block, allocation_size);

	return location;
}
