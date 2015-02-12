/*-------------------------------------------------------------------------
 *
 * rel_block.c
 *	  POSTGRES block io utils.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/rel_block.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/rel_block.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "pgstat.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"

#include <sys/types.h>
#include <unistd.h>

// Internal helper functions

void PrintTuple(HeapTuple tup, TupleDesc tupdesc);
void PrintTupleDesc(TupleDesc tupdesc);
Size ComputeTupleLen(Relation relation);
void ComputeTiles(Relation relation, RelInfo rel_blockinfo);

void PrintRelBlockList(Relation relation, RelBlockType rel_blocktype);
void PrintAllRelBlocks(Relation relation);

void ConvertToScalar(Datum value, Oid valuetypid);

void ConvertToScalar(Datum value, Oid valuetypid)
{
	double val;
	char  *val_str;

	switch (valuetypid)
	{
		/*
		 * Built-in numeric types
		 */
	case BOOLOID:
	case INT2OID:
	case INT4OID:
	case INT8OID:
	case FLOAT4OID:
	case FLOAT8OID:
	case NUMERICOID:
	case OIDOID:
	{
		val = convert_numeric_to_scalar(value, valuetypid);
		elog(WARNING, "Type : %d Val : %.2f", valuetypid, val);
		break;
	}

	/*
	 * Built-in string types
	 */
	case CHAROID:
	case BPCHAROID:
	case VARCHAROID:
	case TEXTOID:
	case NAMEOID:
	{
		val_str = convert_string_datum(value, valuetypid);
		elog(WARNING, "Type : %d Val : --%s--", valuetypid, val_str);
		pfree(val_str);
		break;
	}

	/*
	 * Built-in time types
	 */
	case TIMESTAMPOID:
	case TIMESTAMPTZOID:
	case ABSTIMEOID:
	case DATEOID:
	case INTERVALOID:
	case RELTIMEOID:
	case TINTERVALOID:
	case TIMEOID:
	case TIMETZOID:
	{
		val = convert_timevalue_to_scalar(value, valuetypid);
		elog(WARNING, "Type : %d Val : %.2f", valuetypid, val);
		break;
	}

	default:
		elog(WARNING, "Type : %d not supported", valuetypid);
		break;
	}

}

List** GetRelBlockList(Relation relation, RelBlockType rel_blocktype)
{
	List       **block_list_ptr = NULL;

	// Pick relevant list based on backend and block type
	if(rel_blocktype == RELATION_FIXED_BLOCK_TYPE)
		block_list_ptr = &relation->rd_rel_info->rel_fl_blocks;
	else if(rel_blocktype == RELATION_VARIABLE_BLOCK_TYPE)
		block_list_ptr = &relation->rd_rel_info->rel_vl_blocks;

	if(block_list_ptr == NULL)
	{
		elog(ERROR, "block_list_ptr must not be %p", block_list_ptr);
	}

	return block_list_ptr;
}

void PrintTuple(HeapTuple tuple, TupleDesc tupdesc)
{
	Form_pg_attribute *att = tupdesc->attrs;
	int             natts = tupdesc->natts;
	int             attnum;
	Datum           value;
	bool            isnull;

	elog(WARNING, "PrintTuple");

	for (attnum = 1; attnum <= natts; attnum++)
	{
		Form_pg_attribute thisatt = att[attnum-1];

		value = heap_getattr(tuple, attnum, tupdesc, &isnull);
		ConvertToScalar(value, thisatt->atttypid);
	}
}

void PrintTupleDesc(TupleDesc tupdesc)
{
	int i;

	elog(WARNING, "tupdesc :: natts %3d tdtypeid %3d tdtypmod %3d ",
		 tupdesc->natts, tupdesc->tdtypeid, tupdesc->tdtypmod);
	elog(WARNING, "attnum  ::  attname atttypid attlen atttypmod");

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = tupdesc->attrs[i];
		elog(WARNING, "%d      :: %10s %3d %3d %3d", i, NameStr(attr->attname),
			 attr->atttypid, attr->attlen, attr->atttypmod);
	}
}

Size ComputeTupleLen(Relation relation)
{
	Size      tup_len;
	TupleDesc tup_desc;
	int       i;

	tup_len = relation->rd_rel_info->rel_tuple_len;
	tup_desc = RelationGetDescr(relation);
	//PrintTupleDesc(tupdesc);

	tup_len = 0;
	for (i = 0; i < tup_desc->natts; i++)
	{
		Form_pg_attribute attr = tup_desc->attrs[i];

		/* fixed length attributes */
		if(attr->attlen != -1)
			tup_len += attr->attlen;
		/* variable length attributes */
		else if(attr->atttypmod != -1)
			tup_len += BLOCK_POINTER_SIZE;
		else
			elog(ERROR, "type not supported : %s %3d %3d %3d", NameStr(attr->attname),
				 attr->atttypid, attr->attlen, attr->atttypmod);

	}

	return tup_len;
}

void ComputeTiles(Relation relation, RelInfo rel_blockinfo)
{
	int      *rel_attr_group = NULL;
	int       num_tiles = -1;
	Size      tuplen;
	Size      tile_size;
	int       attr_itr, nattrs;
	int       tile_id;
	int       tile_start_attr_id;
	TupleDesc tup_desc;
	RelTile   rel_tile;

	tuplen = rel_blockinfo->rel_tuple_len;
	tup_desc = RelationGetDescr(relation);
	nattrs = RelationGetNumberOfAttributes(relation);

	num_tiles = tuplen / RELBLOCK_CACHELINE_SIZE;
	if(tuplen % RELBLOCK_CACHELINE_SIZE != 0)
		num_tiles += 1;

	//elog(WARNING, "# of Column Groups : %d", num_column_groups);

	rel_attr_group = (int *) palloc(sizeof(int) * num_tiles);

	tile_id   = 0;
	tile_size = 0;
	tile_start_attr_id = 0;

	// Go over all attributes splitting at cache-line granularity and
	// record the column group information in the given rel_blockinfo structure
	for(attr_itr = 0 ; attr_itr < nattrs ; attr_itr++)
	{
		Form_pg_attribute attr = tup_desc->attrs[attr_itr];
		Size attr_size;

		if(attr->attlen != -1)
			attr_size = attr->attlen;
		else if(attr->atttypmod != -1)
			attr_size = BLOCK_POINTER_SIZE;
		else
			elog(ERROR, "type not supported : %s %3d %3d %3d", NameStr(attr->attname),
				 attr->atttypid, attr->attlen, attr->atttypmod);

		tile_size += attr_size;
		if(tile_size > RELBLOCK_CACHELINE_SIZE)
		{
			rel_tile = (RelTile) palloc(sizeof(RelTileData));

			// add column group info
			rel_tile->tile_id = tile_id;
			rel_tile->tile_size = tile_size - attr_size;
			rel_tile->tile_start_attr_id = tile_start_attr_id;

			//elog(WARNING, "Column Group %d : Size %zd Start attr %d", column_group_id,
			//	 column_group_size, column_group_start_attr_id);

			tile_id += 1;
			tile_size = attr_size;
			tile_start_attr_id = attr_itr;

			rel_blockinfo->rel_tile_to_attrs_map = lappend(rel_blockinfo->rel_tile_to_attrs_map, rel_tile);
		}

		rel_attr_group[attr_itr] = tile_id;
		//elog(WARNING, "Attribute %d : Column Group %d", attr_itr, column_group_id);
	}

	// last column group info
	rel_tile = (RelTile) palloc(sizeof(RelTileData));

	rel_tile->tile_id = tile_id;
	rel_tile->tile_size = tile_size;
	rel_tile->tile_start_attr_id = tile_start_attr_id;
	rel_blockinfo->rel_tile_to_attrs_map = lappend(rel_blockinfo->rel_tile_to_attrs_map, rel_tile);

	rel_blockinfo->rel_attr_to_tile_map = rel_attr_group;

}

void RelInitBlockTableEntry(Relation relation)
{
	RelInfoTag        rel_block_tag;
	RelInfoLookupEnt *entry;
	Oid                rel_id;
	uint32             hash_value;
	RelInfo       rel_block_info;
	MemoryContext      oldcxt;
	int                ret_id;
	Size               tup_len;

	// key
	rel_id = RelationGetRelid(relation);
	rel_block_tag.rel_id = rel_id;
	hash_value = RelBlockTableHashCode(&rel_block_tag);

	entry = RelBlockTableLookup(&rel_block_tag, hash_value);

	if(entry != NULL)
	{
		elog(WARNING, "InitBlockTableEntry :: entry already exists %p", entry);

		if(entry->rel_info == NULL)
		{
			elog(ERROR, "rel_blockinfo should not be %p", entry->rel_info);
		}

		// cache value in relation
		relation->rd_rel_info = entry->rel_info;
	}
	else
	{
		elog(WARNING, "InitBlockTableEntry :: entry not found inserting with hash_value :: %u", hash_value);

		// Allocate new entry in TSM context
		oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);

		tup_len = ComputeTupleLen(relation);
		//elog(WARNING, "tuplen : %zd", tuplen);

		rel_block_info = (RelInfo) palloc(sizeof(RelInfoData));
		rel_block_info->rel_id = rel_id;
		rel_block_info->rel_tuple_len = tup_len;

		// Column group information
		ComputeTiles(relation, rel_block_info);

		ret_id = RelBlockTableInsert(&rel_block_tag, hash_value, rel_block_info);
		if(ret_id != 0)
		{
			elog(WARNING, "InitBlockTableEntry :: entry cannot be inserted");
		}

		// cache value in relation
		relation->rd_rel_info = rel_block_info;

		MemoryContextSwitchTo(oldcxt);
	}
}

void PrintRelBlockList(Relation relation, RelBlockType rel_blocktype)
{
	List       **block_list_ptr = NULL;
	List        *block_list = NULL;
	ListCell    *l = NULL;

	block_list_ptr = GetRelBlockList(relation, rel_blocktype);
	block_list = *block_list_ptr;

	elog(WARNING, "PR BLOCK :: Type : %d List : %p", rel_blocktype, block_list);

	foreach (l, block_list)
	{
		RelBlock rel_block = lfirst(l);
		elog(WARNING, "[ %p ] ->", rel_block);

		//if(rel_block != NULL)
		//	elog(WARNING, "%zd %p", rel_block->rel_blocklen, rel_block->rel_blockdata);
	}
}

void PrintAllRelBlocks(Relation relation)
{
	elog(WARNING, "--------------------------------------------");
	elog(WARNING, "PID :: %d", getpid());
	elog(WARNING, "ALL_BLOCKS :: relation :: %d %s", RelationGetRelid(relation),
		 RelationGetRelationName(relation));
	PrintRelBlockList(relation, RELATION_FIXED_BLOCK_TYPE);
	elog(WARNING, "--------------------------------------------\n");
}

HeapTuple RelBlockGetHeapTuple(RelBlock rel_block, OffsetNumber offset)
{
	HeapTuple  tuple;

	/*
	  Datum      *values;
	  bool       *isnull;

	  values = (Datum *) palloc(natts * sizeof(Datum));
	  isnull  = (bool  *) palloc(natts * sizeof(bool));

	  // Form tuple copy
	  //tuple = heap_form_tuple(tupleDesc, values, isnull);
	  //PrintTuple(tuple, tupleDesc);
	  */

	return tuple;
}

// Based on heap_deform_tuple
void RelBlockPutHeapTuple(Relation relation, HeapTuple tuple)
{
	HeapTupleHeader tup_header = tuple->t_data;
	TupleDesc   tuple_desc = RelationGetDescr(relation);
	bool		hasnulls = HeapTupleHasNulls(tuple);
	Form_pg_attribute *att = tuple_desc->attrs;
	int			natts = tuple_desc->natts;
	int			attnum;
	char	   *tp;						/* ptr to tuple data */
	long		off;					/* offset in tuple data */
	bits8	   *bp = tup_header->t_bits;		/* ptr to null bitmap in tuple */
	bool		slow = false;			/* can we use/set attcacheoff? */
	Datum       value;

	TupleLocation  slot;
	RelBlock          rel_block;
	RelInfo      rel_block_info;
	OffsetNumber      rel_block_itr;
	int               rel_block_offset;

	int        tile_id, prev_tile_id = -1;
	void      *tile_location = NULL;
	RelTile    rel_tile = NULL;
	void      *location = NULL, *varlena_location = NULL;
	Size       tile_size = 0, tile_tuple_offset = 0;
	Size       att_len = 0, field_len = 0;
	Size       location_offset = 0;
	Size       val_str_len = 0;

	// Find free slot for fixed-length fields
	slot = GetFixedLengthSlot(relation);

	rel_block = slot.rb_location;
	rel_block_itr = slot.rb_offset;
	rel_block_offset = rel_block_itr - 1;

	rel_block_info = relation->rd_rel_info;

	// Copy tuple header into slot
	memcpy(&(rel_block->rb_tuple_headers[rel_block_offset]), tup_header,
		   sizeof(HeapTupleHeaderData));

	// Copy tuple data into slot
	tp = (char *) tup_header + tup_header->t_hoff;
	off = 0;

	for (attnum = 0; attnum < natts; attnum++)
	{
		Form_pg_attribute thisatt = att[attnum];
		att_len = thisatt->attlen;

		// Find relevant column group
		tile_id = rel_block_info->rel_attr_to_tile_map[attnum];

		if(tile_id != prev_tile_id)
		{
			prev_tile_id = tile_id;
			rel_tile = list_nth(rel_block_info->rel_tile_to_attrs_map, tile_id);
			tile_size = rel_tile->tile_size;
			tile_location = list_nth(rel_block->rb_tile_locations, tile_id);
			tile_tuple_offset = 0;
		}

		location_offset = (tile_size * rel_block_offset) + tile_tuple_offset;
		location = tile_location + location_offset;

		if(att_len != -1)
			field_len = att_len;
		else
			field_len = BLOCK_POINTER_SIZE;

		tile_tuple_offset += field_len;

		// Check for nulls
		if (hasnulls && att_isnull(attnum, bp))
		{
			value = (Datum) 0;
			slow = true;		/* can't use attcacheoff anymore */
			continue;
		}

		if (!slow && thisatt->attcacheoff >= 0)
			off = thisatt->attcacheoff;
		else if (att_len == -1)
		{
			/*
			 * We can only cache the offset for a varlena attribute if the
			 * offset is already suitably aligned, so that there would be no
			 * pad bytes in any case: then the offset will be valid for either
			 * an aligned or unaligned value.
			 */
			if (!slow &&
				off == att_align_nominal(off, thisatt->attalign))
				thisatt->attcacheoff = off;
			else
			{
				off = att_align_pointer(off, thisatt->attalign, -1,
										tp + off);
				slow = true;
			}
		}
		else
		{
			/* not varlena, so safe to use att_align_nominal */
			off = att_align_nominal(off, thisatt->attalign);

			if (!slow)
				thisatt->attcacheoff = off;
		}

		// Copy data or varlena pointer to slot
		if(att_len != -1)
		{
			memcpy(location, tp + off, field_len);

			//value = fetchatt(thisatt, location);
		}
		else
		{
			val_str_len = strlen(tp + off) + 1;

			// Find free slot for variable-length fields
			varlena_location = GetVariableLengthSlot(relation, val_str_len);
			memcpy(varlena_location, tp + off, val_str_len);

			// Store varlena pointer in slot
			memcpy(location, &varlena_location, BLOCK_POINTER_SIZE);

			//value = fetchatt(thisatt, ((char *) *(char **)location));
		}

		off = att_addlength_pointer(off, thisatt->attlen, tp + off);

		if (thisatt->attlen <= 0)
			slow = true;		/* can't use attcacheoff anymore */
	}

	PrintTuple(tuple, tuple_desc);
}

Oid  RelBlockInsertTuple(Relation relation, HeapTuple tup, CommandId cid,
						 int options, BulkInsertState bistate)
{
	TransactionId xid = GetCurrentTransactionId();
	HeapTuple heaptup;

	elog(WARNING, "Relation Insert :: %s", RelationGetRelationName(relation));

	/*
	 * Fill in tuple header fields, assign an OID, and toast the tuple if
	 * necessary.
	 *
	 * Note: below this point, heaptup is the data we actually intend to store
	 * into the relation; tup is the caller's original untoasted data.
	 */
	heaptup = heap_prepare_insert(relation, tup, xid, cid, options);

	/*
	 * We're about to do the actual insert -- but check for conflict first, to
	 * avoid possibly having to roll back work we've just done.
	 *
	 * For a heap insert, we only need to check for table-level SSI locks. Our
	 * new tuple can't possibly conflict with existing tuple locks, and heap
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do. So we don't need to identify a
	 * buffer before making the call.
	 */
	CheckForSerializableConflictIn(relation, NULL, InvalidBuffer);

	RelBlockPutHeapTuple(relation, heaptup);

	/*
	 * If tuple is cachable, mark it for invalidation from the caches in case
	 * we abort. Note it is OK to do this after releasing the buffer, because
	 * the heaptup data structure is all in local memory, not in the shared
	 * buffer.
	 */
	CacheInvalidateHeapTuple(relation, heaptup, NULL);

	pgstat_count_heap_insert(relation, 1);

	/*
	 * If heaptup is a private copy, release it. Don't forget to copy t_self
	 * back to the caller's image, too.
	 */
	if (heaptup != tup)
	{
		tup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}

	elog(WARNING, "Returing oid : %u", HeapTupleGetOid(tup));

	return HeapTupleGetOid(tup);
}
