/*-------------------------------------------------------------------------
 *
 * relblock.c
 *	  POSTGRES block io utils.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/relblock.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relblock.h"
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

void PrintTupleDesc(TupleDesc tupdesc);
Size ComputeTupleLen(Relation relation);
void ComputeColumnGroups(Relation relation, RelationBlockInfo relblockinfo);
void PrintRelationBlockList(Relation relation, RelationBlockBackend relblockbackend,
							RelationBlockType relblocktype);
void PrintAllRelationBlocks(Relation relation);
void RelationBlockPutHeapTuple(Relation relation, HeapTuple heaptup);
void ConvertToScalar(Datum value, Oid valuetypid);


void ConvertToScalar(Datum value, Oid valuetypid)
{
	double val;
	char  *valstr;

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
		valstr = convert_string_datum(value, valuetypid);
		elog(WARNING, "Type : %d Val : --%s--", valuetypid, valstr);
		pfree(valstr);
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
	Size      tuplen;
	TupleDesc tupdesc;
	int       i;

	tuplen = relation->rd_relblock_info->reltuplen;
	tupdesc = RelationGetDescr(relation);
//PrintTupleDesc(tupdesc);

	tuplen = 0;
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = tupdesc->attrs[i];

/* fixed length attributes */
		if(attr->attlen != -1)
			tuplen += attr->attlen;
/* variable length attributes */
		else if(attr->atttypmod != -1)
			tuplen += BLOCK_POINTER_SIZE;
		else
			elog(ERROR, "type not supported : %s %3d %3d %3d", NameStr(attr->attname),
				 attr->atttypid, attr->attlen, attr->atttypmod);

	}

	return tuplen;
}

void ComputeColumnGroups(Relation relation, RelationBlockInfo relblockinfo)
{
	int     * rel_attr_group = NULL;
	int       num_column_groups = -1;
	Size      tuplen;
	Size      column_group_size;
	int       attr_itr, nattrs;
	int       column_group_id;
	int       column_group_start_attr_id;
	TupleDesc tupdesc;
	RelationColumnGroup rel_column_group;

	tuplen = relblockinfo->reltuplen;
	tupdesc = RelationGetDescr(relation);
	nattrs = RelationGetNumberOfAttributes(relation);

	num_column_groups = tuplen / RELBLOCK_CACHELINE_SIZE;
	if(tuplen % RELBLOCK_CACHELINE_SIZE != 0)
		num_column_groups += 1;

//elog(WARNING, "# of Column Groups : %d", num_column_groups);

	rel_attr_group = (int *) palloc(sizeof(int) * num_column_groups);

	column_group_id   = 0;
	column_group_size = 0;
	column_group_start_attr_id = 0;

// Go over all attributes splitting at cache-line granularity and
// record the column group information in the given relblockinfo structure
	for(attr_itr = 0 ; attr_itr < nattrs ; attr_itr++)
	{
		Form_pg_attribute attr = tupdesc->attrs[attr_itr];
		Size attr_size;

		if(attr->attlen != -1)
			attr_size = attr->attlen;
		else if(attr->atttypmod != -1)
			attr_size = BLOCK_POINTER_SIZE;
		else
			elog(ERROR, "type not supported : %s %3d %3d %3d", NameStr(attr->attname),
				 attr->atttypid, attr->attlen, attr->atttypmod);

		column_group_size += attr_size;
		if(column_group_size > RELBLOCK_CACHELINE_SIZE)
		{
			rel_column_group = (RelationColumnGroup) palloc(sizeof(RelationColumnGroupData));

			// add column group info
			rel_column_group->cg_id = column_group_id;
			rel_column_group->cg_size = column_group_size - attr_size;
			rel_column_group->cg_start_attr_id = column_group_start_attr_id;

			//elog(WARNING, "Column Group %d : Size %zd Start attr %d", column_group_id,
			//	 column_group_size, column_group_start_attr_id);

			column_group_id += 1;
			column_group_size = attr_size;
			column_group_start_attr_id = attr_itr;

			relblockinfo->rel_column_groups = lappend(relblockinfo->rel_column_groups, rel_column_group);
		}

		rel_attr_group[attr_itr] = column_group_id;
		//elog(WARNING, "Attribute %d : Column Group %d", attr_itr, column_group_id);
	}

// last column group info
	rel_column_group = (RelationColumnGroup) palloc(sizeof(RelationColumnGroupData));

	rel_column_group->cg_id = column_group_id;
	rel_column_group->cg_size = column_group_size;
	rel_column_group->cg_start_attr_id = column_group_start_attr_id;
	relblockinfo->rel_column_groups = lappend(relblockinfo->rel_column_groups, rel_column_group);

	relblockinfo->rel_attr_group = rel_attr_group;

}

void RelationInitBlockTableEntry(Relation relation)
{
	RelBlockTag   relblocktag;
	RelBlockLookupEnt *entry;
	Oid            relid;
	uint32         hash_value;
	RelationBlockInfo relblockinfo;
	MemoryContext oldcxt;
	int           ret_id;
	Size          tuplen;

	// key
	relid = RelationGetRelid(relation);
	relblocktag.relid = relid;
	hash_value = RelBlockTableHashCode(&relblocktag);

	entry = RelBlockTableLookup(&relblocktag, hash_value);

	if(entry != NULL)
	{
		//elog(WARNING, "InitBlockTableEntry :: entry already exists %p", entry);

		if(entry->relblockinfo == NULL)
		{
			elog(ERROR, "relblockinfo should not be %p", entry->relblockinfo);
		}

		// cache value in relation
		relation->rd_relblock_info = entry->relblockinfo;
	}
	else
	{
		//elog(WARNING, "InitBlockTableEntry :: entry not found inserting with hash_value :: %u", hash_value);

		// Allocate new entry in TSM context
		oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);

		tuplen = ComputeTupleLen(relation);
		//elog(WARNING, "tuplen : %zd", tuplen);

		relblockinfo = (RelationBlockInfo) palloc(sizeof(RelationBlockInfoData));
		relblockinfo->relid = relid;
		relblockinfo->reltuplen = tuplen;

		// Column group information
		ComputeColumnGroups(relation, relblockinfo);

		ret_id = RelBlockTableInsert(&relblocktag, hash_value, relblockinfo);
		if(ret_id != 0)
		{
			elog(WARNING, "InitBlockTableEntry :: entry cannot be inserted");
		}

		// cache value in relation
		relation->rd_relblock_info = relblockinfo;

		MemoryContextSwitchTo(oldcxt);
	}
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

// Based on heap_deform_tuple
void RelationBlockPutHeapTuple(Relation relation, HeapTuple tuple)
{
	HeapTupleHeader tup = tuple->t_data;
	TupleDesc   tupleDesc = RelationGetDescr(relation);
	bool		hasnulls = HeapTupleHasNulls(tuple);
	Form_pg_attribute *att = tupleDesc->attrs;
	int			natts = tupleDesc->natts;
	int			attnum;
	char	   *tp;				/* ptr to tuple data */
	long		off;			/* offset in tuple data */
	bits8	   *bp = tup->t_bits;		/* ptr to null bitmap in tuple */
	bool		slow = false;	/* can we use/set attcacheoff? */
	Datum       value;

	RelBlockLocation  slot;
	RelationBlock     relblock;
	RelationBlockInfo relblockinfo;
	OffsetNumber      relblock_itr;
	int               relblock_offset;

	int        column_group_id, prev_column_group_id = -1;
	void      *column_group_location = NULL;
	RelationColumnGroup  column_group = NULL;
	void      *location = NULL, *varlena_location = NULL;
	Size       column_group_size = 0, column_group_tuple_offset = 0;
	Size       att_len = 0, field_len = 0;
	Size       location_offset = 0;
	Size       valstr_len = 0;

	// Find free slot for fixed-length fields
	slot = GetFixedLengthSlot(relation, STORAGE_BACKEND_VM);

	relblock = slot.rb_location;
	relblock_itr = slot.rb_offset;
	relblock_offset = relblock_itr - 1;

	relblockinfo = relation->rd_relblock_info;

	// Storing data in slot
	tp = (char *) tup + tup->t_hoff;
	off = 0;

	for (attnum = 0; attnum < natts; attnum++)
	{
		Form_pg_attribute thisatt = att[attnum];
		att_len = thisatt->attlen;

		// Find relevant column group
		column_group_id = relblockinfo->rel_attr_group[attnum];

		if(column_group_id != prev_column_group_id)
		{
			prev_column_group_id = column_group_id;
			column_group = list_nth(relblockinfo->rel_column_groups, column_group_id);
			column_group_size = column_group->cg_size;
			column_group_location = list_nth(relblock->rb_cg_locations, column_group_id);
			column_group_tuple_offset = 0;
		}

		location_offset = (column_group_size * relblock_offset) + column_group_tuple_offset;
		location = column_group_location + location_offset;

		if(att_len != -1)
			field_len = att_len;
		else
			field_len = BLOCK_POINTER_SIZE;

		column_group_tuple_offset += field_len;

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

			value = fetchatt(thisatt, location);
			ConvertToScalar(value, thisatt->atttypid);
		}
		else
		{
			valstr_len = strlen(tp + off)+1;

			// Find free slot for variable-length fields
			varlena_location = GetVariableLengthSlot(relation, STORAGE_BACKEND_VM, valstr_len);
			memcpy(varlena_location, tp + off, valstr_len);

			// Store varlena pointer in slot
			memcpy(location, &varlena_location, BLOCK_POINTER_SIZE);

			value = fetchatt(thisatt, ((char *) *(char **)location));
			ConvertToScalar(value, thisatt->atttypid);
		}


		off = att_addlength_pointer(off, thisatt->attlen, tp + off);

		if (thisatt->attlen <= 0)
			slow = true;		/* can't use attcacheoff anymore */
	}
}

Oid  RelationBlockInsertTuple(Relation relation, HeapTuple tup, CommandId cid,
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

	RelationBlockPutHeapTuple(relation, heaptup);

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
