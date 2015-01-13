/*-------------------------------------------------------------------------
 *
 * rel_block_fixed.c
 *	  Fixed-length block utilities.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/rel_block_fixed.c
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
RelBlock RelAllocateFixedLengthBlock(Relation relation);

RelBlock GetFixedLengthBlockWithFreeSlot(Relation relation);

OffsetNumber GetFixedLengthSlotInBlock(RelBlock relblock);
bool ReleaseFixedLengthSlotInBlock(RelBlock relblock, OffsetNumber slot_id);

RelBlock RelAllocateFixedLengthBlock(Relation relation)
{
	MemoryContext oldcxt;
	RelBlock     rel_block = NULL;
	RelInfo      rel_info = NULL;
	RelTile      rel_tile = NULL;
	Size         tile_size = -1;
	Size         tile_tup_size = -1;
	List         *rel_tiles;
	void         *tile_data = NULL;
	List         **block_list_ptr = NULL;
	ListCell     *l = NULL;

	rel_info = relation->rd_relblock_info;
	rel_tiles = rel_info->rel_tile_to_attrs_map;

	// Allocate block in TSM context
	oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);

	rel_block = (RelBlock) palloc(sizeof(RelBlockData));
	rel_block->rb_type = RELATION_FIXED_BLOCK_TYPE;

	// bitmap tracking slot status
	rel_block->rb_slot_bitmap = (bool *) palloc0(NUM_REL_BLOCK_ENTRIES);
	// free slot counter
	rel_block->rb_free_slots = NUM_REL_BLOCK_ENTRIES;
	// tuple headers
	rel_block->rb_tuple_headers = (HeapTupleHeader *) palloc(NUM_REL_BLOCK_ENTRIES * sizeof(HeapTupleHeaderData));

	foreach(l, rel_tiles)
	{
		rel_tile = lfirst(l);

		tile_tup_size = rel_tile->tile_size;
		tile_size = tile_tup_size * NUM_REL_BLOCK_ENTRIES;
		rel_block->rb_size += tile_size;

		tile_data = (void *) palloc(tile_size);

		elog(WARNING, "CG size : %zd location : %p", tile_size, tile_data);

		// Append tile to fixed-length block
		rel_block->rb_tile_locations = lappend(rel_block->rb_tile_locations, tile_data);
	}

	elog(WARNING, "RelationBlock Size : %zd Type : %d", rel_block->rb_size,
		 rel_block->rb_type);

	block_list_ptr = GetRelBlockList(relation, RELATION_FIXED_BLOCK_TYPE);
	*block_list_ptr = lappend(*block_list_ptr, rel_block);

	RelBlockTablePrint();

	MemoryContextSwitchTo(oldcxt);

	return rel_block;
}

OffsetNumber GetFixedLengthSlotInBlock(RelBlock relblock)
{
	OffsetNumber  slot_itr = InvalidOffsetNumber;
	int    slot_offset;
	int    free_slots = relblock->rb_free_slots ;
	bool  *slot_bitmap = relblock->rb_slot_bitmap;

	if(free_slots == 0)
	{
		elog(ERROR, "No free slots in block %p", relblock);
		return InvalidOffsetNumber;
	}

	// Update bitmap and free slot counter
	for(slot_itr = FirstOffsetNumber ; slot_itr <= NUM_REL_BLOCK_ENTRIES ; slot_itr++)
	{
		slot_offset = slot_itr - 1;
		if(slot_bitmap[slot_offset] == false)
		{
			slot_bitmap[slot_offset] = true;
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

bool ReleaseFixedLengthSlotInBlock(RelBlock relblock, OffsetNumber slot_id)
{
	bool  *slotmap = relblock->rb_slot_bitmap;
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


RelBlock GetFixedLengthBlockWithFreeSlot(Relation relation)
{
	List       **block_list_ptr = NULL;
	List        *block_list = NULL;
	ListCell    *l = NULL;
	RelBlock    rel_block = NULL;
	bool        block_found;

	block_list_ptr = GetRelBlockList(relation, RELATION_FIXED_BLOCK_TYPE);

	/* empty block list */
	if(*block_list_ptr == NULL)
	{
		rel_block = RelAllocateFixedLengthBlock(relation);
	}
	else
	{
		block_list = *block_list_ptr;
		block_found = false;

		/* check for a block with free space */
		foreach (l, block_list)
		{
			rel_block = lfirst(l);

			if(rel_block->rb_free_slots > 0)
			{
				block_found = true;
				break;
			}
		}

		if(block_found == false)
		{
			rel_block = RelAllocateFixedLengthBlock(relation);
		}
	}

	return rel_block;
}

TupleLocation GetFixedLengthSlot(Relation relation)
{
	OffsetNumber     relblock_offset = InvalidOffsetNumber;
	RelBlock         rel_block = NULL;
	TupleLocation location;

	rel_block = GetFixedLengthBlockWithFreeSlot(relation);

	relblock_offset = GetFixedLengthSlotInBlock(rel_block);

	location.rb_location = rel_block;
	location.rb_offset = relblock_offset;

	return location;
}
