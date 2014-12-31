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

void RelationAllocateBlock(Relation relation, RelationBlockBackend relblockbackend,
						   RelationBlockType relblocktype);

List** GetRelationBlockList(Relation relation, RelationBlockBackend relblockbackend,
							RelationBlockType relblocktype);

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


Size ComputeTupleLen(Relation relation)
{
	Size      tuplen;
	TupleDesc tupdesc;
	int       i;

	tuplen = relation->rd_relblock_info->reltuplen;
	tupdesc = RelationGetDescr(relation);
	PrintTupleDesc(tupdesc);

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
	int* rel_column_group = NULL;
	int num_column_groups = -1;
	Size tuplen, column_group_size;
	int attr_itr, nattrs, column_group_id;
	TupleDesc tupdesc;

	tuplen = relblockinfo->reltuplen;
	tupdesc = RelationGetDescr(relation);
	nattrs = RelationGetNumberOfAttributes(relation);

	num_column_groups = tuplen / RELBLOCK_CACHELINE_SIZE;
	if(tuplen % RELBLOCK_CACHELINE_SIZE != 0)
		num_column_groups += 1;

	elog(WARNING, "Column Groups %d", num_column_groups);

	rel_column_group = (int *) palloc(sizeof(int) * num_column_groups);

	column_group_id   = 0;
	column_group_size = 0;
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
			column_group_id += 1;
			column_group_size = attr_size;
		}

		rel_column_group[attr_itr] = column_group_id;
		elog(WARNING, "Attribute %d : Column Group %d", attr_itr, column_group_id);
	}

	relblockinfo->rel_column_group = rel_column_group;
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

void RelationAllocateBlock(Relation relation, RelationBlockBackend relblockbackend,
						   RelationBlockType relblocktype)
{
	Size          tuplen;
	Size          blockSize = -1;
	RelationBlock relblock;
	MemoryContext oldcxt;
	void          *blockData = NULL;
	List          **blockListPtr = NULL;

	tuplen = relation->rd_relblock_info->reltuplen;
	if(relblocktype == RELATION_FIXED_BLOCK_TYPE)
		blockSize = tuplen * BLOCK_FIXED_LENGTH_SIZE;
	else if(relblocktype == RELATION_VARIABLE_BLOCK_TYPE)
		blockSize = BLOCK_VARIABLE_LENGTH_SIZE;

	oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);

	relblock = (RelationBlock) palloc(sizeof(RelationBlockData));
	relblock->relblocktype = relblocktype;
	relblock->relblockbackend = relblockbackend;
	blockData = (void *) palloc(blockSize);
	relblock->relblockdata = blockData;
	relblock->relblocklen = blockSize;

	elog(WARNING, "RelationBlock Size : %zd Backend : %d Type : %d", relblock->relblocklen,
		 relblock->relblockbackend, relblock->relblocktype);

	blockListPtr = GetRelationBlockList(relation, relblockbackend, relblocktype);

	elog(WARNING, "Appending to list : %p", *blockListPtr);

	// Append here within TSMC
	*blockListPtr = lappend(*blockListPtr, relblock);

	RelBlockTablePrint();

	SHMContextStats(TopSharedMemoryContext);
	MemoryContextSwitchTo(oldcxt);
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
		elog(WARNING, "InitBlockTableEntry :: entry already exists %p", entry);

		if(entry->relblockinfo == NULL)
		{
			elog(ERROR, "relblockinfo should not be %p", entry->relblockinfo);
		}

        // cache value in relation
		relation->rd_relblock_info = entry->relblockinfo;
	}
	else
	{
		elog(WARNING, "InitBlockTableEntry :: entry not found inserting with hash_value :: %u", hash_value);

		// Allocate new entry in TSMC
		oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);

		tuplen = ComputeTupleLen(relation);
		elog(WARNING, "tuplen : %zd", tuplen);

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

		SHMContextStats(TopSharedMemoryContext);
		MemoryContextSwitchTo(oldcxt);
	}
}
