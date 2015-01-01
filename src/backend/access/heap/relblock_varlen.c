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

// Internal helper functions

void *GetVariableLengthSlot(Relation relation, Size size)
{
	void *location = NULL;


	return location;
}
