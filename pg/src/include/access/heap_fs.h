/*-------------------------------------------------------------------------
 *
 * heap_fs.h
 *	  POSTGRES heap access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/heap_fs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HEAP_FS_H
#define HEAP_FS_H

#include "access/sdir.h"
#include "access/skey.h"
#include "nodes/primnodes.h"
#include "storage/bufpage.h"
#include "storage/lock.h"
#include "utils/lockwaitpolicy.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/* in heap/heap_fs.c */
extern Relation fs_relation_open(Oid relationId, LOCKMODE lockmode);
extern Relation fs_try_relation_open(Oid relationId, LOCKMODE lockmode);
extern Relation fs_relation_openrv(const RangeVar *relation, LOCKMODE lockmode);
extern Relation fs_relation_openrv_extended(const RangeVar *relation,
						 LOCKMODE lockmode, bool missing_ok);
extern void fs_relation_close(Relation relation, LOCKMODE lockmode);

extern Relation fs_heap_open(Oid relationId, LOCKMODE lockmode);
extern Relation fs_heap_openrv(const RangeVar *relation, LOCKMODE lockmode);
extern Relation fs_heap_openrv_extended(const RangeVar *relation,
					 LOCKMODE lockmode, bool missing_ok);

extern HeapScanDesc fs_heap_beginscan(Relation relation, Snapshot snapshot,
			   int nkeys, ScanKey key);
extern HeapScanDesc fs_heap_beginscan_catalog(Relation relation, int nkeys,
					   ScanKey key);
extern HeapScanDesc fs_heap_beginscan_strat(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key,
					 bool allow_strat, bool allow_sync);
extern HeapScanDesc fs_heap_beginscan_bm(Relation relation, Snapshot snapshot,
				  int nkeys, ScanKey key);
extern void fs_heap_setscanlimits(HeapScanDesc scan, BlockNumber startBlk,
		   BlockNumber endBlk);
extern void fs_heap_rescan(HeapScanDesc scan, ScanKey key);
extern void fs_heap_endscan(HeapScanDesc scan);
extern HeapTuple fs_heap_getnext(HeapScanDesc scan, ScanDirection direction);

extern bool fs_heap_fetch(Relation relation, Snapshot snapshot,
		   HeapTuple tuple, Buffer *userbuf, bool keep_buf,
		   Relation stats_relation);
extern bool fs_heap_hot_search_buffer(ItemPointer tid, Relation relation,
					   Buffer buffer, Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call);
extern bool fs_heap_hot_search(ItemPointer tid, Relation relation,
				Snapshot snapshot, bool *all_dead);

extern void fs_heap_get_latest_tid(Relation relation, Snapshot snapshot,
					ItemPointer tid);

extern BulkInsertState fs_GetBulkInsertState(void);
extern void fs_FreeBulkInsertState(BulkInsertState);

extern Oid fs_heap_insert(Relation relation, HeapTuple tup, CommandId cid,
			int options, BulkInsertState bistate);
extern void fs_heap_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
				  CommandId cid, int options, BulkInsertState bistate);
extern HTSU_Result fs_heap_delete(Relation relation, ItemPointer tid,
			CommandId cid, Snapshot crosscheck, bool wait,
			HeapUpdateFailureData *hufd);
extern HTSU_Result fs_heap_update(Relation relation, ItemPointer otid,
			HeapTuple newtup,
			CommandId cid, Snapshot crosscheck, bool wait,
			HeapUpdateFailureData *hufd, LockTupleMode *lockmode);
extern HTSU_Result fs_heap_lock_tuple(Relation relation, HeapTuple tuple,
				CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
				bool follow_update,
				Buffer *buffer, HeapUpdateFailureData *hufd);
extern void fs_heap_inplace_update(Relation relation, HeapTuple tuple);
extern bool fs_heap_freeze_tuple(HeapTupleHeader tuple, TransactionId cutoff_xid,
				  TransactionId cutoff_multi);
extern bool fs_heap_tuple_needs_freeze(HeapTupleHeader tuple, TransactionId cutoff_xid,
						MultiXactId cutoff_multi, Buffer buf);

extern Oid	fs_simple_heap_insert(Relation relation, HeapTuple tup);
extern void fs_simple_heap_delete(Relation relation, ItemPointer tid);
extern void fs_simple_heap_update(Relation relation, ItemPointer otid,
				   HeapTuple tup);

extern void fs_heap_sync(Relation relation);

/* in heap/pruneheap.c */
extern void fs_heap_page_prune_opt(Relation relation, Buffer buffer);
extern int fs_heap_page_prune(Relation relation, Buffer buffer,
				TransactionId OldestXmin,
				bool report_stats, TransactionId *latestRemovedXid);
extern void fs_heap_page_prune_execute(Buffer buffer,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused);
extern void fs_heap_get_root_tuples(Page page, OffsetNumber *root_offsets);

/* in heap/syncscan.c */
extern void fs_ss_report_location(Relation rel, BlockNumber location);
extern BlockNumber fs_ss_get_location(Relation rel, BlockNumber relnblocks);
extern void fs_SyncScanShmemInit(void);
extern Size fs_SyncScanShmemSize(void);

#endif   /* HEAP_FS_H */
