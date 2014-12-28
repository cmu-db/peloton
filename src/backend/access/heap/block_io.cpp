/*-------------------------------------------------------------------------
 *
 * block_io.cpp
 *	  POSTGRES block io utils.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/block_io.cpp
 *
 *-------------------------------------------------------------------------
 */

extern "C"
{
#include "access/block_io.h"
#include "utils/rel.h"
}

#include "map"
#include "vector"
#include "cassert"

/*
 * Possible block types.
 */
typedef enum RelationBlockType
{
/* Used to store fixed-length tuples */
	RELATION_FIXED_BLOCK_TYPE,
/* Used to store variable-length attributes */
	RELATION_VARIABLE_BLOCK_TYPE
} RelationBlockType;

/* RelationBlock structure */
typedef struct RelationBlockData{
/* Tyoe of block */
	RelationBlockType relblocktype;

/* Location of block */
	RelationBlockBackend relblockbackend;

/* Data contained in block */
	void *relblockdata;

/* Size of block */
	Size relblocklen;
} RelationBlockData;

typedef RelationBlockData* RelationBlock;

/* VM/NVM storage information */
typedef struct RelationBlockInfoData{
/* Oid of relation */
	Oid relid;

/* length of the tuple */
	Size reltuplen;

/* relation blocks on VM and NVM */
	std::vector<RelationBlock> relblock_lists[4];
} RelationBlockInfoData;

typedef RelationBlockInfoData* RelationBlockInfo;

// RelationBlockInfo directory
std::map<Oid, RelationBlockInfo> RelationBlocks;

// Prototypes

void PrintTupleDesc(TupleDesc tupdesc);

void PrintRelationBlocks(Relation relation, RelationBlockBackend relblockbackend,
						 RelationBlockType relblocktype);

void RelationAllocateBlock(Relation relation, RelationBlockBackend relblockbackend,
						   RelationBlockType relblocktype);

unsigned int GetRelationBlockList(Relation relation, RelationBlockBackend relblockbackend,
												 RelationBlockType relblocktype);

RelationBlockInfo GetRelationBlockInfo(Relation relation);

void PrintTupleDesc(TupleDesc tupdesc)
{
	elog(WARNING, "tupdesc :: natts %3d tdtypeid %3d tdtypmod %3d ", tupdesc->natts,
		 tupdesc->tdtypeid, tupdesc->tdtypmod);
	elog(WARNING, "attnum  ::  attname atttypid attlen atttypmod");

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = tupdesc->attrs[i];
		elog(WARNING, "%d      :: %10s %3d %3d %3d", i, NameStr(attr->attname),
			 attr->atttypid, attr->attlen, attr->atttypmod);
	}
}

RelationBlockInfo GetRelationBlockInfo(Relation relation)
{
	Oid relationId = RelationGetRelid(relation);

	auto RelationBlocksItr = RelationBlocks.find(relationId);
	if(RelationBlocksItr == RelationBlocks.end())
		return NULL;

	RelationBlockInfo relblockinfo = RelationBlocksItr->second;

	return relblockinfo;
}

unsigned int GetRelationBlockList(Relation relation, RelationBlockBackend relblockbackend,
								  RelationBlockType relblocktype)
{
	auto relblockinfo =  GetRelationBlockInfo(relation);
	if(relblockinfo == NULL)
	{
		elog(ERROR, "relblockinfo is NULL");
	}

	unsigned int relblock_list = 0;

	// Pick relevant list based on backend and block type
	if(relblockbackend == STORAGE_BACKEND_VM){
		if(relblocktype == RELATION_FIXED_BLOCK_TYPE)
			relblock_list = 0;
		else if(relblocktype == RELATION_VARIABLE_BLOCK_TYPE)
			relblock_list = 1;
	}
	else if(relblockbackend == STORAGE_BACKEND_NVM){
		if(relblocktype == RELATION_FIXED_BLOCK_TYPE)
			relblock_list = 2;
		else if(relblocktype == RELATION_VARIABLE_BLOCK_TYPE)
			relblock_list = 3;
	}

	return relblock_list;
}

void PrintRelationBlocks(Relation relation, RelationBlockBackend relblockbackend,
						 RelationBlockType relblocktype)
{
	auto relblockinfo =  GetRelationBlockInfo(relation);
	auto relblock_list = GetRelationBlockList(relation, relblockbackend, relblocktype);
	elog(WARNING, "PR_BLOCK :: Backend : %d Type : %d", relblockbackend, relblocktype);

	if(relblockinfo == NULL)
	{
		elog(ERROR, "relblockinfo is NULL");
	}

	elog(WARNING, "List : %d", relblock_list);
	elog(WARNING, "List length : %lu", relblockinfo->relblock_lists[relblock_list].size());

	for (RelationBlock relblock : relblockinfo->relblock_lists[relblock_list])
	{
		elog(WARNING, "[ %p ] ->", relblock);

		if(relblock != NULL)
			elog(WARNING, "%zd %p", relblock->relblocklen, relblock->relblockdata);
	}
}


Size RelationGetTupleLen(Relation relation)
{
	Size      tuplen = 0;
	TupleDesc tupdesc;
	int       i;

	auto relblockinfo =  GetRelationBlockInfo(relation);
	tuplen = relblockinfo->reltuplen;

	// Check if already computed
	if(tuplen != 0)
		return tuplen;

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
			elog(ERROR, "type not supported : %s %3d %3d %3d", NameStr(attr->attname), attr->atttypid, attr->attlen, attr->atttypmod);

	}

	// Cache for future use
	relblockinfo->reltuplen = tuplen;

	return tuplen;
}

void PrintAllRelationBlocks(Oid relationId)
{
	Relation relation = RelationIdGetRelation(relationId);

	elog(WARNING, "--------------------------------------------");
	elog(WARNING, "BLOCKS   :: %p %s ID :: [  %d  ]", relation, RelationGetRelationName(relation), relationId);

	PrintRelationBlocks(relation, STORAGE_BACKEND_VM, RELATION_FIXED_BLOCK_TYPE);
	PrintRelationBlocks(relation, STORAGE_BACKEND_VM, RELATION_VARIABLE_BLOCK_TYPE);
	elog(WARNING, "--------------------------------------------\n");
}


void RelationAllocateBlock(Relation relation, RelationBlockBackend relblockbackend, RelationBlockType relblocktype)
{
	Size          tuplen = 0;
	Size          blockSize = -1;
	RelationBlock relblock;

	tuplen = RelationGetTupleLen(relation);
	elog(WARNING, "tuplen : %zd", tuplen);

	if(relblocktype == RELATION_FIXED_BLOCK_TYPE)
		blockSize = tuplen * BLOCK_FIXED_LENGTH_SIZE;
	else if(relblocktype == RELATION_VARIABLE_BLOCK_TYPE)
		blockSize = BLOCK_VARIABLE_LENGTH_SIZE;

	relblock = new RelationBlockData;
	relblock->relblocktype = relblocktype;
	relblock->relblockbackend = relblockbackend;

	// Allocate block storage
	relblock->relblockdata = (void *) palloc(blockSize);
	relblock->relblocklen = blockSize;

	elog(WARNING, "Block size : %zd Backend : %d Type : %d", relblock->relblocklen, relblock->relblockbackend, relblock->relblocktype);

	auto relblock_list = GetRelationBlockList(relation, relblockbackend, relblocktype);
	elog(WARNING, "Appending block %p to list %d", relblock, relblock_list);

	auto relblockinfo =  GetRelationBlockInfo(relation);
	if(relblockinfo == NULL)
	{
		elog(ERROR, "relblockinfo is NULL");
	}

	relblockinfo->relblock_lists[relblock_list].push_back(relblock);
}

void RelationInit(Relation relation)
{
	Oid relationId = RelationGetRelid(relation);
	auto itr = RelationBlocks.find(relationId);

	if(itr == RelationBlocks.end()){
		RelationBlockInfo relblockinfo = new RelationBlockInfoData;
		relblockinfo->relid = relationId;
		relblockinfo->reltuplen = 0;

		RelationBlocks[relationId] = relblockinfo;

		RelationAllocateBlock(relation, STORAGE_BACKEND_VM, RELATION_FIXED_BLOCK_TYPE);
		RelationAllocateBlock(relation, STORAGE_BACKEND_VM, RELATION_FIXED_BLOCK_TYPE);
		RelationAllocateBlock(relation, STORAGE_BACKEND_VM, RELATION_VARIABLE_BLOCK_TYPE);
	}

}
