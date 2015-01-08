/*-------------------------------------------------------------------------
 *
 * heap_vm.c
 *	  heap backend for vm
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heap_vm.c
 *
 *
 * NOTES
 *	  This file contains the heap_ routines for relations stored in
 *    volatile memory (DRAM)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relblock.h"
#include "access/relscan.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/predicate.h"
#include "storage/smgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"

void vm_relation_allocate(Relation rd)
{
	RelationInitBlockTableEntry(rd);
}

BlockNumber vm_nblocks(Relation rd)
{
	BlockNumber numBlks = 0;
	RelationBlockInfo relblockinfo;

	relblockinfo = rd->rd_relblock_info;

	if(relblockinfo != NULL)
	{
		// Count blocks on VM and NVM
		numBlks += list_length(relblockinfo->rel_fixed_blocks_on_VM);
		numBlks += list_length(relblockinfo->rel_fixed_blocks_on_NVM);
	}

	elog(WARNING, "vm_nbocks : %d", numBlks);

	return numBlks;
}

Relation vm_relation_open(Oid relationId, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

Relation vm_try_relation_open(Oid relationId, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

Relation vm_relation_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

Relation vm_relation_openrv_extended(const RangeVar *relation,
									 LOCKMODE lockmode, bool missing_ok)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

void vm_relation_close(Relation relation, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

Relation vm_heap_open(Oid relationId, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

Relation vm_heap_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

Relation vm_heap_openrv_extended(const RangeVar *relation,
								 LOCKMODE lockmode, bool missing_ok)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

HeapScanDesc vm_heap_beginscan(Relation relation, Snapshot snapshot,
							   int nkeys, ScanKey key)
{
	elog(WARNING, "BEGIN SCAN :: %s", RelationGetRelationName(relation));

	return heap_beginscan_internal(relation, snapshot, nkeys, key,
								   true, true, false, false);
}


// SCAN

HeapScanDesc vm_heap_beginscan_catalog(Relation relation, int nkeys,
									   ScanKey key)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}


HeapScanDesc vm_heap_beginscan_strat(Relation relation, Snapshot snapshot,
									 int nkeys, ScanKey key,
									 bool allow_strat, bool allow_sync)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}


HeapScanDesc vm_heap_beginscan_bm(Relation relation, Snapshot snapshot,
								  int nkeys, ScanKey key)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}


void vm_heap_setscanlimits(HeapScanDesc scan, BlockNumber startBlk,
						   BlockNumber endBlk)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

/* ----------------
 *		vm_heapgettup - fetch next heap tuple
 *
 *		Initialize the scan if not already done; then advance to the next
 *		tuple as indicated by "dir"; return the next tuple in scan->rs_ctup,
 *		or set scan->rs_ctup.t_data = NULL if no more tuples.
 *
 * ----------------
 */
static void
vm_heapgettup(HeapScanDesc scan,
		   ScanDirection dir,
		   int nkeys,
		   ScanKey key)
{
	HeapTuple	tuple = &(scan->rs_ctup);
	Snapshot	snapshot = scan->rs_snapshot;
	bool		backward = ScanDirectionIsBackward(dir);

	BlockNumber page;
	Page		dp;

	//RelBlockLocation location;
	//RelationBlock    block;
	OffsetNumber     lineoff;

	bool		finished;
	int			lines;
	int			linesleft;
	ItemId		lpp;

	elog(WARNING, "scan inited      : %d", scan->rs_inited);
	elog(WARNING, "scan cblock      : %d", scan->rs_cblock);
	elog(WARNING, "scan startblock  : %d", scan->rs_startblock);
	elog(WARNING, "scan nblocks     : %d", scan->rs_nblocks);
	elog(WARNING, "scan backward    : %d", backward);

	return;

	/*
	 * calculate next starting lineoff, given scan direction
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}
			page = scan->rs_startblock; /* first page */
			//fs_heapgetpage(scan, page);
			lineoff = FirstOffsetNumber;		/* first offnum */
			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock;		/* current page */
			lineoff =			/* next offnum */
				OffsetNumberNext(ItemPointerGetOffsetNumber(&(tuple->t_self)));
		}

		//LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = PageGetMaxOffsetNumber(dp);
		/* page and lineoff now reference the physically next tid */

		linesleft = lines - lineoff + 1;
	}
	else if (backward)
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}

			/*
			 * Disable reporting to syncscan logic in a backwards scan; it's
			 * not very likely anyone else is doing the same thing at the same
			 * time, and much more likely that we'll just bollix things for
			 * forward scanners.
			 */
			scan->rs_syncscan = false;
			/* start from last page of the scan */
			if (scan->rs_startblock > 0)
				page = scan->rs_startblock - 1;
			else
				page = scan->rs_nblocks - 1;
			//fs_heapgetpage(scan, page);
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock;		/* current page */
		}

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = PageGetMaxOffsetNumber(dp);

		if (!scan->rs_inited)
		{
			lineoff = lines;	/* final offnum */
			scan->rs_inited = true;
		}
		else
		{
			lineoff =			/* previous offnum */
				OffsetNumberPrev(ItemPointerGetOffsetNumber(&(tuple->t_self)));
		}
		/* page and lineoff now reference the physically previous tid */

		linesleft = lineoff;
	}
	else
	{
		/*
		 * ``no movement'' scan direction: refetch prior tuple
		 */
		if (!scan->rs_inited)
		{
			Assert(!BufferIsValid(scan->rs_cbuf));
			tuple->t_data = NULL;
			return;
		}

		page = ItemPointerGetBlockNumber(&(tuple->t_self));
		//if (page != scan->rs_cblock)
			//fs_heapgetpage(scan, page);

		/* Since the tuple was previously fetched, needn't lock page here */
		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lineoff = ItemPointerGetOffsetNumber(&(tuple->t_self));
		lpp = PageGetItemId(dp, lineoff);
		Assert(ItemIdIsNormal(lpp));

		tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
		tuple->t_len = ItemIdGetLength(lpp);

		return;
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	lpp = PageGetItemId(dp, lineoff);
	for (;;)
	{
		while (linesleft > 0)
		{
			if (ItemIdIsNormal(lpp))
			{
				bool		valid = false;

				tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);
				tuple->t_len = ItemIdGetLength(lpp);
				ItemPointerSet(&(tuple->t_self), page, lineoff);

				/*
				 * if current tuple qualifies, return it.
				 */
				//valid = HeapTupleSatisfiesVisibility(tuple,
				//									 snapshot,
				//									 scan->rs_cbuf);

				CheckForSerializableConflictOut(valid, scan->rs_rd, tuple,
												scan->rs_cbuf, snapshot);

				//if (valid && key != NULL)
				//	HeapKeyTest(tuple, RelationGetDescr(scan->rs_rd),
				//				nkeys, key, valid);

				if (valid)
				{
					LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
					return;
				}
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			if (backward)
			{
				--lpp;			/* move back in this page's ItemId array */
				--lineoff;
			}
			else
			{
				++lpp;			/* move forward in this page's ItemId array */
				++lineoff;
			}
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

		/*
		 * advance to next/prior page and detect end of scan
		 */
		if (backward)
		{
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks <= 0 : false);
			if (page == 0)
				page = scan->rs_nblocks;
			page--;
		}
		else
		{
			page++;
			if (page >= scan->rs_nblocks)
				page = 0;
			finished = (page == scan->rs_startblock) ||
				(scan->rs_numblocks != InvalidBlockNumber ? --scan->rs_numblocks <= 0 : false);

			/*
			 * Report our new scan position for synchronization purposes. We
			 * don't do that when moving backwards, however. That would just
			 * mess up any other forward-moving scanners.
			 *
			 * Note: we do this before checking for end of scan so that the
			 * final state of the position hint is back at the start of the
			 * rel.  That's not strictly necessary, but otherwise when you run
			 * the same query multiple times the starting position would shift
			 * a little bit backwards on every invocation, which is confusing.
			 * We don't guarantee any specific ordering in general, though.
			 */
			if (scan->rs_syncscan)
				fs_ss_report_location(scan->rs_rd, page);
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return;
		}

		//fs_heapgetpage(scan, page);

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = PageGetMaxOffsetNumber((Page) dp);
		linesleft = lines;
		if (backward)
		{
			lineoff = lines;
			lpp = PageGetItemId(dp, lines);
		}
		else
		{
			lineoff = FirstOffsetNumber;
			lpp = PageGetItemId(dp, FirstOffsetNumber);
		}
	}
}


void vm_heap_rescan(HeapScanDesc scan, ScanKey key)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

void vm_heap_endscan(HeapScanDesc scan)
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
	elog(DEBUG2, "vm_heap_getnext([%s,nkeys=%d],dir=%d) called",		\
		 RelationGetRelationName(scan->rs_rd), scan->rs_nkeys, (int) direction)
#define HEAPDEBUG_2									\
	elog(DEBUG2, "vm_heap_getnext returning EOS")
#define HEAPDEBUG_3									\
	elog(DEBUG2, "vm_heap_getnext returning tuple")
#else
#define HEAPDEBUG_1
#define HEAPDEBUG_2
#define HEAPDEBUG_3
#endif   /* !defined(HEAPDEBUGALL) */


HeapTuple vm_heap_getnext(HeapScanDesc scan, ScanDirection direction)
{
	List       *selectVars;
	ListCell   *l;

	/* Note: no locking manipulations needed */

	HEAPDEBUG_1;				/* heap_getnext( info ) */

	elog(WARNING, "vm_heapgettup");
	selectVars = scan->rs_selectVars;

	foreach(l, selectVars)
	{
		int attnum = lfirst_int(l);
		elog(WARNING, "attnum %d", attnum);
	}

	vm_heapgettup(scan, direction, scan->rs_nkeys, scan->rs_key);


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

bool vm_heap_fetch(Relation relation, Snapshot snapshot,
				   HeapTuple tuple, Buffer *userbuf, bool keep_buf,
				   Relation stats_relation)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return false;
}


bool vm_heap_hot_search_buffer(ItemPointer tid, Relation relation,
							   Buffer buffer, Snapshot snapshot, HeapTuple heapTuple,
							   bool *all_dead, bool first_call)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return false;
}


bool vm_heap_hot_search(ItemPointer tid, Relation relation,
						Snapshot snapshot, bool *all_dead)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return false;
}

void vm_heap_get_latest_tid(Relation relation, Snapshot snapshot,
							ItemPointer tid)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// INSERT

BulkInsertState vm_GetBulkInsertState(void)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
}

void vm_FreeBulkInsertState(BulkInsertState bistate)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

Oid vm_heap_insert(Relation relation, HeapTuple tup, CommandId cid,
				   int options, BulkInsertState bistate)
{
	Oid ret_val;

	ret_val =  RelationBlockInsertTuple(relation, tup, cid, options, bistate);

	return ret_val;
}

void vm_heap_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
						  CommandId cid, int options, BulkInsertState bistate)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// DELETE

HTSU_Result vm_heap_delete(Relation relation, ItemPointer tid,
						   CommandId cid, Snapshot crosscheck, bool wait,
						   HeapUpdateFailureData *hufd)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return HeapTupleInvisible;
}

// UPDATE

HTSU_Result vm_heap_update(Relation relation, ItemPointer otid,
						   HeapTuple newtup,
						   CommandId cid, Snapshot crosscheck, bool wait,
						   HeapUpdateFailureData *hufd, LockTupleMode *lockmode)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return HeapTupleInvisible;
}

// LOCK

HTSU_Result vm_heap_lock_tuple(Relation relation, HeapTuple tuple,
							   CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
							   bool follow_update,
							   Buffer *buffer, HeapUpdateFailureData *hufd)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return HeapTupleInvisible;
}


void vm_heap_inplace_update(Relation relation, HeapTuple tuple)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// FREEZE

bool vm_heap_freeze_tuple(HeapTupleHeader tuple, TransactionId cutoff_xid,
						  TransactionId cutoff_multi)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return false;
}

bool vm_heap_tuple_needs_freeze(HeapTupleHeader tuple, TransactionId cutoff_xid,
								MultiXactId cutoff_multi, Buffer buf)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return false;
}

// WRAPPERS

Oid	vm_simple_heap_insert(Relation relation, HeapTuple tup)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return -1;
}

void vm_simple_heap_delete(Relation relation, ItemPointer tid)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

void vm_simple_heap_update(Relation relation, ItemPointer otid,
						   HeapTuple tup)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// SYNC

void vm_heap_sync(Relation relation)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// PAGE

void vm_heap_page_prune_opt(Relation relation, Buffer buffer)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

int vm_heap_page_prune(Relation relation, Buffer buffer,
					   TransactionId OldestXmin,
					   bool report_stats, TransactionId *latestRemovedXid)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return -1;
}

void vm_heap_page_prune_execute(Buffer buffer,
								OffsetNumber *redirected, int nredirected,
								OffsetNumber *nowdead, int ndead,
								OffsetNumber *nowunused, int nunused)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

void vm_heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// SYNC SCAN

void vm_ss_report_location(Relation rel, BlockNumber location)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

BlockNumber vm_ss_get_location(Relation rel, BlockNumber relnblocks)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return -1;
}

void vm_SyncScanShmemInit(void)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

Size vm_SyncScanShmemSize(void)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return -1;
}
