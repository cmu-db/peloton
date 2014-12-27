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
#include "utils/rel.h"
#include "access/block_io.h"


// Prototypes

void PrintTupleDesc(TupleDesc tupdesc);

Size ComputeTupleLen(Relation relation);

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

Size
ComputeTupleLen(Relation relation)
{
	Size tuplen = relation->rd_tuplen;
	TupleDesc tupdesc;
	int i;

	// Check if already computed
	if(tuplen != 0)
		return tuplen;

	tupdesc = RelationGetDescr(relation);
	PrintTupleDesc(tupdesc);

	tuplen = 0;
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = tupdesc->attrs[i];

		if(attr->attlen != -1) /* fixed length attributes */
			tuplen += attr->attlen;
		else if(attr->atttypmod != -1) /* variable length attributes */
			tuplen += attr->atttypmod;
		else{
			elog(ERROR, "type not supported : %s %3d %3d %3d", NameStr(attr->attname), attr->atttypid, attr->attlen, attr->atttypmod);
		}
	}

	// Store for future use
	relation->rd_tuplen = tuplen;

	return tuplen;
}


void
RelationAllocateBlock(Relation relation)
{
	Size tuplen = ComputeTupleLen(relation);
	Size fixed_block_size;
	Size variable_block_size;
	RelationBlock fixed;

 	elog(WARNING, "tuplen : %zd", tuplen);

	fixed_block_size = tuplen * BLOCK_FIXED_LENGTH_SIZE;
	fixed = (RelationBlock) palloc(fixed_block_size);
	fixed->relblocklen = fixed_block_size;
	fixed->relblocktype = RelationFixedBlock;

	elog(WARNING, "Fixed block size : %zd", fixed->relblocklen);

	variable_block_size = BLOCK_VARIABLE_LENGTH_SIZE;

	pfree(fixed);
}
