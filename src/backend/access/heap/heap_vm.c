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

#include "access/heapam.h"
#include "access/block_io.h"


void vm_relation_allocate(Relation rd)
{
	RelationInitAllocateBlock(rd);
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
	PrintAllRelationBlocks(relation);

	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
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


void vm_heap_rescan(HeapScanDesc scan, ScanKey key)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}


void vm_heap_endscan(HeapScanDesc scan)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
}

// FETCH

HeapTuple vm_heap_getnext(HeapScanDesc scan, ScanDirection direction)
{
	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return NULL;
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
	elog(WARNING, "Relation Insert :: %s", RelationGetRelationName(relation));
	PrintAllRelationBlocks(relation);

	elog(ERROR, "%s %d %s : function not implemented", __FILE__, __LINE__, __func__);
	return -1;
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
