/*-------------------------------------------------------------------------
 *
 * block_io.c
 *	  POSTGRES block io utils.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/block_io.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "utils/palloc.h"
#include "utils/rel.h"
#include "utils/memutils.h"
#include "access/relblock.h"
#include "storage/bufmgr.h"

#include <sys/types.h>
#include <unistd.h>

// Prototypes

void PrintTupleDesc(TupleDesc tupdesc);

Size ComputeTupleLen(Relation relation);

void
PrintRelationBlocks(Relation relation, RelationBlockBackend relblockbackend, RelationBlockType relblocktype);

void
RelationAllocateBlock(Relation relation, RelationBlockBackend relblockbackend, RelationBlockType relblocktype);


void**
GetRelationBlockList(Relation relation, RelationBlockBackend relblockbackend, RelationBlockType relblocktype);

void
PrintTupleDesc(TupleDesc tupdesc)
{
	int i;

	elog(WARNING, "tupdesc :: natts %3d tdtypeid %3d tdtypmod %3d ", tupdesc->natts, tupdesc->tdtypeid, tupdesc->tdtypmod);
	elog(WARNING, "attnum  ::  attname atttypid attlen atttypmod");

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = tupdesc->attrs[i];
		elog(WARNING, "%d      :: %10s %3d %3d %3d", i, NameStr(attr->attname), attr->atttypid, attr->attlen, attr->atttypmod);
	}
}

void**
GetRelationBlockList(Relation relation, RelationBlockBackend relblockbackend, RelationBlockType relblocktype)
{
	void       **blockList = NULL;

	/*
	// Pick relevant list based on backend and block type
	if(relblockbackend == STORAGE_BACKEND_VM){
	if(relblocktype == RELATION_FIXED_BLOCK_TYPE)
	blockList = &relation->rd_fixed_blocks_on_VM;
	else if(relblocktype == RELATION_VARIABLE_BLOCK_TYPE)
	blockList = &relation->rd_variable_blocks_on_VM;
	}
	else if(relblockbackend == STORAGE_BACKEND_NVM){
	if(relblocktype == RELATION_FIXED_BLOCK_TYPE)
	blockList = &relation->rd_fixed_blocks_on_NVM;
	else if(relblocktype == RELATION_VARIABLE_BLOCK_TYPE)
	blockList = &relation->rd_variable_blocks_on_NVM;
	}
	*/

	return blockList;
}

void
PrintRelationBlocks(Relation relation, RelationBlockBackend relblockbackend, RelationBlockType relblocktype)
{
	/*
	  List       **blockList = NULL;
	  ListCell   *l = NULL;

	  blockList = GetRelationBlockList( relation, relblockbackend, relblocktype);

	  if(blockList == NULL)
	  {
	  //elog(WARNING, "blockList is NULL");
	  return;
	  }
	*/

	elog(WARNING, "PR_BLOCK :: Backend : %d Type : %d", relblockbackend, relblocktype);
	elog(WARNING, "Actual List : %d", relation->rd_num_fixed_blocks_on_VM);

	/*
	  foreach (l, (*blockList))
	  {
	  RelationBlock relblock = lfirst(l);
	  elog(WARNING, "[ %p ] ->", relblock);

	  if(relblock != NULL)
	  elog(WARNING, "%zd %p", relblock->relblocklen, relblock->relblockdata);
	  }*/

}


Size
ComputeTupleLen(Relation relation)
{
	Size      tuplen = relation->rd_tuplen;
	TupleDesc tupdesc;
	int       i;

	// Check if already computed
	if(tuplen != 0)
		return tuplen;

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
			elog(ERROR, "type not supported : %s %3d %3d %3d", NameStr(attr->attname), attr->atttypid, attr->attlen, attr->atttypmod);

	}

	// Store for future use
	relation->rd_tuplen = tuplen;

	return tuplen;
}

void
PrintAllRelationBlocks(Relation relation)
{
	elog(WARNING, "--------------------------------------------");
	elog(WARNING, "PID :: backend pid %d", getpid());
	elog(WARNING, "PR_ALL_BLOCKS :: relation :: %p %s", relation, RelationGetRelationName(relation));
	PrintRelationBlocks(relation, STORAGE_BACKEND_VM, RELATION_FIXED_BLOCK_TYPE);
	//PrintRelationBlocks(relation, STORAGE_BACKEND_VM, RELATION_VARIABLE_BLOCK_TYPE);
	//PrintRelationBlocks(relation, STORAGE_BACKEND_NVM, RELATION_FIXED_BLOCK_TYPE);
	//PrintRelationBlocks(relation, STORAGE_BACKEND_NVM, RELATION_VARIABLE_BLOCK_TYPE);
	elog(WARNING, "--------------------------------------------\n");
}

void
RelationAllocateBlock(Relation relation, RelationBlockBackend relblockbackend, RelationBlockType relblocktype)
{
	Size          tuplen;
	Size          blockSize = -1;
	RelationBlock relblock;
	MemoryContext oldcxt;
	void          *blockData = NULL;

	RelBlockTag   relblocktag;
	RelBlockLookupEnt *entry;
	Oid            relId;
	uint32         hash_value;
	int           pid = -1;
	int           ret_id = -1;

	tuplen = ComputeTupleLen(relation);
	//elog(WARNING, "tuplen : %zd", tuplen);

	if(relblocktype == RELATION_FIXED_BLOCK_TYPE)
		blockSize = tuplen * BLOCK_FIXED_LENGTH_SIZE;
	else if(relblocktype == RELATION_VARIABLE_BLOCK_TYPE)
		blockSize = BLOCK_VARIABLE_LENGTH_SIZE;


	oldcxt = MemoryContextSwitchTo(TopSharedMemoryContext);
	elog(WARNING, "TSMC :: %p", TopSharedMemoryContext);
	SHMContextStats(TopSharedMemoryContext);

	// Allocate block storage
	blockData = (void *) palloc(blockSize);

	relblock = (RelationBlock) palloc(sizeof(RelationBlockData));
	relblock->relblocktype = relblocktype;
	relblock->relblockbackend = relblockbackend;
	relblock->relblockdata = blockData;
	relblock->relblocklen = blockSize;

	elog(WARNING, "Block size : %zd Backend : %d Type : %d", relblock->relblocklen, relblock->relblockbackend, relblock->relblocktype);

	MemoryContextSwitchTo(oldcxt);
	oldcxt = MemoryContextSwitchTo(CacheMemoryContext);

	// Testing relblock hash table

	relId = RelationGetRelid(relation);
	relblocktag.relId = relId;

	pid = getpid();
	hash_value = RelBlockTableHashCode(&relblocktag);

	elog(WARNING, "Inserting key :: %u for relation %d", hash_value, relId);

	ret_id =  RelBlockTableInsert(&relblocktag, hash_value, pid);

	elog(WARNING, "RelBlockTableInsert :: returned pid %d", ret_id);

	entry = RelBlockTableLookup(&relblocktag, hash_value);

	if(entry != NULL)
	{
		elog(WARNING, "RelBlockTableLookup :: returned pid %d", entry->pid);
	}
	else
	{
		elog(WARNING, "RelBlockTableLookup :: entry not found");
	}

	RelBlockTablePrint();

	MemoryContextSwitchTo(oldcxt);
}

void
RelationInitAllocateBlock(Relation relation)
{
	//RelationAllocateBlock(relation, relation->rd_block_backend, RELATION_VARIABLE_BLOCK_TYPE);

	PrintAllRelationBlocks(relation);
}
