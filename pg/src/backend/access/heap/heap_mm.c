/*-------------------------------------------------------------------------
 *
 * heap_mm.c
 *	  heap backend for memory
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heap_mm.c
 *
 *
 * NOTES
 *	  This file contains the heap routines for relations stored in memory
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/rel_block.h"
#include "access/relscan.h"
#include "access/valid.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"

// Internal helper function prototypes

BlockNumber mm_nblocks(Relation rd);

void mm_relation_allocate(Relation rd)
{
	RelInitBlockTableEntry(rd);
}

BlockNumber mm_nblocks(Relation rd)
{
	BlockNumber num_blocks = 0;
	RelInfo rel_info;

	rel_info = rd->rd_rel_info;

	if(rel_info != NULL)
	{
		num_blocks = list_length(rel_info->rel_fl_blocks);
	}

	elog(WARNING, "mm_nbocks : %d", num_blocks);

	return num_blocks;
}

Relation mm_relation_open(Oid relationId, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

Relation mm_try_relation_open(Oid relationId, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

Relation mm_relation_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

Relation mm_relation_openrv_extended(const RangeVar *relation,
									 LOCKMODE lockmode, bool missing_ok)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

void mm_relation_close(Relation relation, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

Relation mm_heap_open(Oid relationId, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

Relation mm_heap_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

Relation mm_heap_openrv_extended(const RangeVar *relation,
								 LOCKMODE lockmode, bool missing_ok)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

HeapScanDesc mm_heap_beginscan(Relation relation, Snapshot snapshot,
							   int nkeys, ScanKey key)
{
	elog(WARNING, "BEGIN SCAN :: %s", RelationGetRelationName(relation));

	return heap_beginscan_internal(relation, snapshot, nkeys, key,
								   true, true, false, false);
}


// SCAN

HeapScanDesc mm_heap_beginscan_catalog(Relation relation, int nkeys,
									   ScanKey key)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}


HeapScanDesc mm_heap_beginscan_strat(Relation relation, Snapshot snapshot,
									 int nkeys, ScanKey key,
									 bool allow_strat, bool allow_sync)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}


HeapScanDesc mm_heap_beginscan_bm(Relation relation, Snapshot snapshot,
								  int nkeys, ScanKey key)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}


void mm_heap_setscanlimits(HeapScanDesc scan, BlockNumber startBlk,
						   BlockNumber endBlk)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

/* ----------------
 *		mm_heapgettup - fetch next heap tuple
 *
 *		Initialize the scan if not already done; then advance to the next
 *		tuple as indicated by "dir"; return the next tuple in scan->rs_ctup,
 *		or set scan->rs_ctup.t_data = NULL if no more tuples.
 *
 * ----------------
 */
static void
mm_heapgettup(HeapScanDesc scan,
			  ScanDirection dir,
			  int nkeys,
			  ScanKey key)
{
	HeapTuple	tuple = &(scan->rs_ctup);
	Snapshot	snapshot = scan->rs_snapshot;
	bool		backward = ScanDirectionIsBackward(dir);

	//TupleLocation    location;
	//RelBlock         rel_block;
	Relation         relation;
	RelInfo          rel_info;
	List            *rel_fl_blocks;

	elog(WARNING, "scan inited      : %d", scan->rs_inited);
	elog(WARNING, "scan cblock      : %d", scan->rs_cblock);
	elog(WARNING, "scan nblocks     : %d", scan->rs_nblocks);
	elog(WARNING, "scan backward    : %d", backward);

	/* TODO ::
	// Set up scan
	if (ScanDirectionIsForward(dir))
	{
		elog(WARNING, "Forward scan");

		if (!scan->rs_inited)
		{
			// return null immediately if relation is empty
			relation = scan->rs_rd;
			scan->rs_nblocks = mm_nblocks(relation);

			if (scan->rs_nblocks == 0)
			{
				tuple->t_data = NULL;
				return;
			}

			// first block
			rel_info = relation->rd_rel_info;
			rel_fl_blocks = rel_info->rel_fl_blocks;

			scan->rs_cblock = 0 ;
			scan->rs_rblock = list_nth(rel_fl_blocks, scan->rs_cblock);
			scan->rs_rblock_offset = InvalidOffsetNumber;

			elog(WARNING, "Finished init");

			scan->rs_inited = true;
		}
	}
	else if (backward)
	{
		elog(ERROR, "Backward scan not implemented");
		return;
	}
	else
	{
		elog(WARNING, "No movement scan");

		// ``no movement'' scan direction: refetch prior tuple
		if (!scan->rs_inited)
		{
			tuple->t_data = NULL;
			return;
		}

		tuple = RelBlockGetHeapTuple(scan->rs_rblock, scan->rs_rblock_offset);

		return;
	}

	// advance the scan until we find a qualifying tuple or run out of stuff
	// to scan
	for (;;)
	{
		bool  valid = false;

		// Keep track of next tuple to fetch
		scan->rs_rblock_offset = GetNextTupleInBlock(scan->rs_rblock, scan->rs_rblock_offset);

		elog(WARNING, "Offset %d", scan->rs_rblock_offset);

		if(scan->rs_rblock_offset == InvalidOffsetNumber)
		{
			// Go to next block
			if(scan->rs_cblock < scan->rs_nblocks)
			{
				elog(WARNING, "Go to next block");
				scan->rs_cblock += 1;

				relation = scan->rs_rd;
				rel_info = relation->rd_rel_info;
				rel_fl_blocks = rel_info->rel_fl_blocks;

				scan->rs_rblock = list_nth(rel_fl_blocks, scan->rs_cblock);
				scan->rs_rblock_offset = InvalidOffsetNumber;
			}
			// return NULL if we've exhausted all the pages
			else
			{
				scan->rs_cbuf = InvalidBuffer;
				scan->rs_cblock = InvalidBlockNumber;
				tuple->t_data = NULL;
				scan->rs_inited = false;
				return;
			}
		}

		elog(WARNING, "GetHeapTuple");

		tuple = RelBlockGetHeapTuple(scan->rs_rblock, scan->rs_rblock_offset);

		elog(WARNING, "Visibility check");

		// if current tuple qualifies, return it.
		valid = HeapTupleSatisfiesVisibility(tuple, snapshot, InvalidBuffer);

		elog(WARNING, "Checkforserializableconflictout");

		CheckForSerializableConflictOut(valid, scan->rs_rd, tuple,
										scan->rs_cbuf, snapshot);

		if (valid && key != NULL)
			HeapKeyTest(tuple, RelationGetDescr(scan->rs_rd),
						nkeys, key, valid);

		elog(WARNING, "HeapKeyTest");
	}
	*/

}


void mm_heap_rescan(HeapScanDesc scan, ScanKey key)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

void mm_heap_endscan(HeapScanDesc scan)
{
/* Note: no locking manipulations needed */

	elog(WARNING, "END SCAN");

/*
 * unpin scan buffers
 */
	if (BufferIsValid(scan->rs_cbuf))
		ReleaseBuffer(scan->rs_cbuf);

/*
 * decrement relation reference count and free scan descriptor storage
 */
	RelationDecrementReferenceCount(scan->rs_rd);

	if (scan->rs_key)
		pfree(scan->rs_key);

	if (scan->rs_strategy != NULL)
		FreeAccessStrategy(scan->rs_strategy);

	if (scan->rs_temp_snap)
		UnregisterSnapshot(scan->rs_snapshot);

	pfree(scan);
}

// FETCH

/* ----------------
 *		heap_getnext	- retrieve next tuple in scan
 *
 *		Fix to work with index relations.
 *		We don't return the buffer anymore, but you can get it from the
 *		returned HeapTuple.
 * ----------------
 */

#ifdef HEAPDEBUGALL
#define HEAPDEBUG_1														\
	elog(DEBUG2, "mm_heap_getnext([%s,nkeys=%d],dir=%d) called",		\
		 RelationGetRelationName(scan->rs_rd), scan->rs_nkeys, (int) direction)
#define HEAPDEBUG_2									\
	elog(DEBUG2, "mm_heap_getnext returning EOS")
#define HEAPDEBUG_3									\
	elog(DEBUG2, "mm_heap_getnext returning tuple")
#else
#define HEAPDEBUG_1
#define HEAPDEBUG_2
#define HEAPDEBUG_3
#endif   /* !defined(HEAPDEBUGALL) */


HeapTuple mm_heap_getnext(HeapScanDesc scan, ScanDirection direction)
{
	List       *select_vars;
	ListCell   *l;

	/* Note: no locking manipulations needed */

	HEAPDEBUG_1;				/* heap_getnext( info ) */

	elog(WARNING, "mm_heapgettup");
	select_vars = scan->rs_select_vars;

	foreach(l, select_vars)
	{
		int attnum = lfirst_int(l);
		elog(WARNING, "attnum %d", attnum);
	}

	mm_heapgettup(scan, direction, scan->rs_nkeys, scan->rs_key);


	if (scan->rs_ctup.t_data == NULL)
	{
		HEAPDEBUG_2;			/* heap_getnext returning EOS */
		return NULL;
	}

	/*
	 * if we get here it means we have a new current scan tuple, so point to
	 * the proper return buffer and return the tuple.
	 */
	HEAPDEBUG_3;				/* heap_getnext returning tuple */

	pgstat_count_heap_getnext(scan->rs_rd);

	return &(scan->rs_ctup);
}

bool mm_heap_fetch(Relation relation, Snapshot snapshot,
				   HeapTuple tuple, Buffer *userbuf, bool keep_buf,
				   Relation stats_relation)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return false;
}


bool mm_heap_hot_search_buffer(ItemPointer tid, Relation relation,
							   Buffer buffer, Snapshot snapshot, HeapTuple heapTuple,
							   bool *all_dead, bool first_call)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return false;
}


bool mm_heap_hot_search(ItemPointer tid, Relation relation,
						Snapshot snapshot, bool *all_dead)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return false;
}

void mm_heap_get_latest_tid(Relation relation, Snapshot snapshot,
							ItemPointer tid)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// INSERT

BulkInsertState mm_GetBulkInsertState(void)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

void mm_FreeBulkInsertState(BulkInsertState bistate)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

Oid mm_heap_insert(Relation relation, HeapTuple tup, CommandId cid,
				   int options, BulkInsertState bistate)
{
	Oid ret_val;

	ret_val =  RelBlockInsertTuple(relation, tup, cid, options, bistate);

	return ret_val;
}

void mm_heap_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
						  CommandId cid, int options, BulkInsertState bistate)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// DELETE

HTSU_Result mm_heap_delete(Relation relation, ItemPointer tid,
						   CommandId cid, Snapshot crosscheck, bool wait,
						   HeapUpdateFailureData *hufd)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return HeapTupleInvisible;
}

// UPDATE

HTSU_Result mm_heap_update(Relation relation, ItemPointer otid,
						   HeapTuple newtup,
						   CommandId cid, Snapshot crosscheck, bool wait,
						   HeapUpdateFailureData *hufd, LockTupleMode *lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return HeapTupleInvisible;
}

// LOCK

HTSU_Result mm_heap_lock_tuple(Relation relation, HeapTuple tuple,
							   CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
							   bool follow_update,
							   Buffer *buffer, HeapUpdateFailureData *hufd)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return HeapTupleInvisible;
}


void mm_heap_inplace_update(Relation relation, HeapTuple tuple)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// FREEZE

bool mm_heap_freeze_tuple(HeapTupleHeader tuple, TransactionId cutoff_xid,
						  TransactionId cutoff_multi)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return false;
}

bool mm_heap_tuple_needs_freeze(HeapTupleHeader tuple, TransactionId cutoff_xid,
								MultiXactId cutoff_multi, Buffer buf)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return false;
}

// WRAPPERS

Oid	mm_simple_heap_insert(Relation relation, HeapTuple tup)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return -1;
}

void mm_simple_heap_delete(Relation relation, ItemPointer tid)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

void mm_simple_heap_update(Relation relation, ItemPointer otid,
						   HeapTuple tup)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// SYNC

void mm_heap_sync(Relation relation)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// PAGE

void mm_heap_page_prune_opt(Relation relation, Buffer buffer)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

int mm_heap_page_prune(Relation relation, Buffer buffer,
					   TransactionId OldestXmin,
					   bool report_stats, TransactionId *latestRemovedXid)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return -1;
}

void mm_heap_page_prune_execute(Buffer buffer,
								OffsetNumber *redirected, int nredirected,
								OffsetNumber *nowdead, int ndead,
								OffsetNumber *nowunused, int nunused)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

void mm_heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// SYNC SCAN

void mm_ss_report_location(Relation rel, BlockNumber location)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

BlockNumber mm_ss_get_location(Relation rel, BlockNumber relnblocks)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return -1;
}

void mm_SyncScanShmemInit(void)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

Size mm_SyncScanShmemSize(void)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return -1;
}
