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

#include "access/relblock.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "storage/bufmgr.h"

#include <sys/types.h>
#include <unistd.h>

// Internal helper functions

void PrintTupleDesc(TupleDesc tupdesc);
Size ComputeTupleLen(Relation relation);
void ComputeColumnGroups(Relation relation, RelationBlockInfo relblockinfo);

void PrintRelationBlockList(Relation relation, RelationBlockBackend relblockbackend,
							RelationBlockType relblocktype);
void PrintAllRelationBlocks(Relation relation);

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
	//ListCell *lc;

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

	/*
	// display column group info
	foreach (lc, relblockinfo->rel_column_groups)
	{
		RelationColumnGroup relcolumngroup = lfirst(lc);
		if(relcolumngroup != NULL)
			elog(WARNING, "[ %d %zd %d ]", relcolumngroup->cg_id, relcolumngroup->cg_size,
				 relcolumngroup->cg_start_attr_id );
	}
	*/
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

Oid RelationBlockInsertTuple(Relation relation, HeapTuple tup)
{
	Oid ret_id = -1;
	off_t relblock_offset = -1;
	void *relblock_location = NULL;

	elog(WARNING, "Relation Insert :: %s", RelationGetRelationName(relation));

	// Find free slot for fixed-length fields
	//relblock_offset = GetFixedLengthSlot(relation, STORAGE_BACKEND_VM);

	// Find free slot for variable-length fields
	relblock_location = GetVariableLengthSlot(relation, STORAGE_BACKEND_VM, 100);

	relblock_location = GetVariableLengthSlot(relation, STORAGE_BACKEND_VM, 10000);
	ReleaseVariableLengthSlot(relation, STORAGE_BACKEND_VM, relblock_location);

	return ret_id;
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
