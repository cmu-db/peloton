/*-------------------------------------------------------------------------
 *
 * relblock_table.c
 *	  routines for mapping RelBlockTags to relation blocks.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate relation.
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/relblock_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relblock.h"

HTAB *SharedRelBlockHash = NULL;

/*
 * Estimate space needed for mapping hashtable
 */
Size
RelBlockTableShmemSize(int size)
{
	return hash_estimate_size(size, sizeof(RelBlockLookupEnt));
}

/*
 * Initialize shmem hash table for mapping entries
 */
void
InitRelBlockTable(int size)
{
	HASHCTL hash_ctl;

	elog(WARNING, "RelBlockInfoTable INIT");

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(RelBlockTag);
	hash_ctl.entrysize = sizeof(RelBlockLookupEnt);

	SharedRelBlockHash = ShmemInitHash("Shared RelBlock Lookup Table",
									   size, size,
									   &hash_ctl,
									   HASH_ELEM | HASH_BLOBS);

	elog(WARNING, "Shared RelBlockBlock Hash :: %p", SharedRelBlockHash);
}

/*
 * RelBlockTableHashCode
 *		Compute the hash code associated with a RelBlockTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
RelBlockTableHashCode(RelBlockTag *tagPtr)
{
	return get_hash_value(SharedRelBlockHash, (void *) tagPtr);
}

/*
 * RelBlockTableLookup
 *		Lookup the given RelBlockTag; return RelBlockLookupEnt, or NULL if not found
 */
RelBlockLookupEnt *
RelBlockTableLookup(RelBlockTag *tagPtr, uint32 hashcode)
{
	RelBlockLookupEnt *result;

	result = (RelBlockLookupEnt *)
		hash_search_with_hash_value(SharedRelBlockHash,
									(void *) tagPtr,
									hashcode,
									HASH_FIND,
									NULL);

	if (!result)
		return NULL;

	return result;
}

/*
 * RelBlockTableInsert
 *		Insert a hashtable entry for given tag and PID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the PID in that entry.
 */
int
RelBlockTableInsert(RelBlockTag *tagPtr, uint32 hashcode, int pid)
{
	RelBlockLookupEnt *result;
	bool		found;

	Assert(pid >= 0);		/* -1 is reserved for not-in-table */

	result = (RelBlockLookupEnt *)
		hash_search_with_hash_value(SharedRelBlockHash,
									(void *) tagPtr,
									hashcode,
									HASH_ENTER,
									&found);

	if (found)					/* found something already in the table */
		return result->pid;

	result->pid = pid;

	return -1;
}

/*
 * RelBlockTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 */
void
RelBlockTableDelete(RelBlockTag *tagPtr, uint32 hashcode)
{
	RelBlockLookupEnt *result;

	result = (RelBlockLookupEnt *)
		hash_search_with_hash_value(SharedRelBlockHash,
									(void *) tagPtr,
									hashcode,
									HASH_REMOVE,
									NULL);

	if (!result)				/* shouldn't happen */
		elog(ERROR, "shared relblock hash table corrupted");
}

/*
 * RelBlockPrint
 *		Display the hashtable
 */
void
RelBlockTablePrint()
{
	HASH_SEQ_STATUS status;
	RelBlockLookupEnt *entry;

	hash_seq_init(&status, SharedRelBlockHash);

	while ((entry = (RelBlockLookupEnt *) hash_seq_search(&status)) != NULL)
	{
		elog(WARNING, "Entry :: %p pid : %d", entry, entry->pid);
	}
}
