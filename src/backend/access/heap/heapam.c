/*-------------------------------------------------------------------------
 *
 * heap_am.c
 *	  heap access manager code
 *
 * Portions Copyright (c) 2014-2015, Carnegie Mellon University
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam.c
 *
 *
 * INTERFACE ROUTINES
 *		relation_open	- open any relation by relation OID
 *		relation_openrv - open any relation specified by a RangeVar
 *		relation_close	- close any relation
 *		heap_open		- open a heap relation by relation OID
 *		heap_openrv		- open a heap relation specified by a RangeVar
 *		heap_close		- (now just a macro for relation_close)
 *		heap_beginscan	- begin relation scan
 *		heap_rescan		- restart a relation scan
 *		heap_endscan	- end relation scan
 *		heap_getnext	- retrieve next tuple in scan
 *		heap_fetch		- retrieve tuple with given tid
 *		heap_insert		- insert tuple into a relation
 *		heap_multi_insert - insert multiple tuples into a relation
 *		heap_delete		- delete a tuple from a relation
 *		heap_update		- replace a tuple in a relation with another tuple
 *		heap_sync		- sync heap, for when no WAL has been written
 *
 * NOTES
 *	  This file contains the heap_ routines which implement
 *	  the POSTGRES heap access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/hio.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "pgstat.h"
#include "storage/lmgr.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

/*
 * This struct of function pointers defines the API between heapam.c and
 * any individual heap manager module.  Note that heap_mgr subfunctions are
 * generally expected to report problems via elog(ERROR).
 */
typedef struct f_heapam
{
	Relation (*relation_open) (Oid relationId, LOCKMODE lockmode);
	Relation (*try_relation_open) (Oid relationId, LOCKMODE lockmode);
	Relation (*relation_openrv) (const RangeVar *relation, LOCKMODE lockmode);
	Relation (*relation_openrv_extended) (const RangeVar *relation,
										  LOCKMODE lockmode, bool missing_ok);
	void (*relation_close) (Relation relation, LOCKMODE lockmode);
	Relation (*heap_open) (Oid relationId, LOCKMODE lockmode);
	Relation (*heap_openrv) (const RangeVar *relation, LOCKMODE lockmode);
	Relation (*heap_openrv_extended) (const RangeVar *relation,
									  LOCKMODE lockmode, bool missing_ok);
	HeapScanDesc (*heap_beginscan) (Relation relation, Snapshot snapshot,
									int nkeys, ScanKey key);
	HeapScanDesc (*heap_beginscan_catalog) (Relation relation, int nkeys,
											ScanKey key);
	HeapScanDesc (*heap_beginscan_strat) (Relation relation, Snapshot snapshot,
										  int nkeys, ScanKey key,
										  bool allow_strat, bool allow_sync);
	HeapScanDesc (*heap_beginscan_bm) (Relation relation, Snapshot snapshot,
									   int nkeys, ScanKey key);
	void (*heap_setscanlimits) (HeapScanDesc scan, BlockNumber startBlk,
								BlockNumber endBlk);
	void (*heap_rescan) (HeapScanDesc scan, ScanKey key);
	void (*heap_endscan) (HeapScanDesc scan);
	HeapTuple (*heap_getnext) (HeapScanDesc scan, ScanDirection direction);
	bool (*heap_fetch) (Relation relation, Snapshot snapshot,
						HeapTuple tuple, Buffer *userbuf, bool keep_buf,
						Relation stats_relation);
	bool (*heap_hot_search_buffer) (ItemPointer tid, Relation relation,
									Buffer buffer, Snapshot snapshot, HeapTuple heapTuple,
									bool *all_dead, bool first_call);
	bool (*heap_hot_search) (ItemPointer tid, Relation relation,
							 Snapshot snapshot, bool *all_dead);
	void (*heap_get_latest_tid) (Relation relation, Snapshot snapshot,
								 ItemPointer tid);
	BulkInsertState (*GetBulkInsertState) (void);
	void (*FreeBulkInsertState) (BulkInsertState);
	Oid (*heap_insert) (Relation relation, HeapTuple tup, CommandId cid,
						int options, BulkInsertState bistate);
	void (*heap_multi_insert) (Relation relation, HeapTuple *tuples, int ntuples,
							   CommandId cid, int options, BulkInsertState bistate);
	HTSU_Result (*heap_delete) (Relation relation, ItemPointer tid,
								CommandId cid, Snapshot crosscheck, bool wait,
								HeapUpdateFailureData *hufd);
	HTSU_Result (*heap_update) (Relation relation, ItemPointer otid,
								HeapTuple newtup,
								CommandId cid, Snapshot crosscheck, bool wait,
								HeapUpdateFailureData *hufd, LockTupleMode *lockmode);
	HTSU_Result (*heap_lock_tuple) (Relation relation, HeapTuple tuple,
									CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
									bool follow_update,
									Buffer *buffer, HeapUpdateFailureData *hufd);
	void (*heap_inplace_update) (Relation relation, HeapTuple tuple);
	bool (*heap_freeze_tuple) (HeapTupleHeader tuple, TransactionId cutoff_xid,
							   TransactionId cutoff_multi);
	bool (*heap_tuple_needs_freeze) (HeapTupleHeader tuple, TransactionId cutoff_xid,
									 MultiXactId cutoff_multi, Buffer buf);
	Oid	(*simple_heap_insert) (Relation relation, HeapTuple tup);
	void (*simple_heap_delete) (Relation relation, ItemPointer tid);
	void (*simple_heap_update) (Relation relation, ItemPointer otid,
								HeapTuple tup);
	void (*heap_sync) (Relation relation);
	void (*heap_page_prune_opt) (Relation relation, Buffer buffer);
	int (*heap_page_prune) (Relation relation, Buffer buffer,
							TransactionId OldestXmin,
							bool report_stats, TransactionId *latestRemovedXid);
	void (*heap_page_prune_execute) (Buffer buffer,
									 OffsetNumber *redirected, int nredirected,
									 OffsetNumber *nowdead, int ndead,
									 OffsetNumber *nowunused, int nunused);
	void (*heap_get_root_tuples) (Page page, OffsetNumber *root_offsets);
	void (*ss_report_location) (Relation rel, BlockNumber location);
	BlockNumber (*ss_get_location) (Relation rel, BlockNumber relnblocks);
	void (*SyncScanShmemInit) (void);
	Size (*SyncScanShmemSize) (void);
} f_heapam;

static const f_heapam f_heapam_backends[] = {
	/* FS */
	{ fs_relation_open, fs_try_relation_open, fs_relation_openrv,
	  fs_relation_openrv_extended, fs_relation_close, fs_heap_open,
	  fs_heap_openrv, fs_heap_openrv_extended, fs_heap_beginscan,
	  fs_heap_beginscan_catalog, fs_heap_beginscan_strat, fs_heap_beginscan_bm,
	  fs_heap_setscanlimits, fs_heap_rescan, fs_heap_endscan, fs_heap_getnext,
	  fs_heap_fetch, fs_heap_hot_search_buffer, fs_heap_hot_search,
	  fs_heap_get_latest_tid, fs_GetBulkInsertState, fs_FreeBulkInsertState,
	  fs_heap_insert, fs_heap_multi_insert, fs_heap_delete, fs_heap_update,
	  fs_heap_lock_tuple, fs_heap_inplace_update, fs_heap_freeze_tuple,
	  fs_heap_tuple_needs_freeze, fs_simple_heap_insert, fs_simple_heap_delete,
	  fs_simple_heap_update, fs_heap_sync, fs_heap_page_prune_opt, fs_heap_page_prune,
	  fs_heap_page_prune_execute, fs_heap_get_root_tuples, fs_ss_report_location,
	  fs_ss_get_location, fs_SyncScanShmemInit, fs_SyncScanShmemSize
	},
	/* MM */
	{ mm_relation_open, mm_try_relation_open, mm_relation_openrv,
	  mm_relation_openrv_extended, mm_relation_close, mm_heap_open,
	  mm_heap_openrv, mm_heap_openrv_extended, mm_heap_beginscan,
	  mm_heap_beginscan_catalog, mm_heap_beginscan_strat, mm_heap_beginscan_bm,
	  mm_heap_setscanlimits, mm_heap_rescan, mm_heap_endscan, mm_heap_getnext,
	  mm_heap_fetch, mm_heap_hot_search_buffer, mm_heap_hot_search,
	  mm_heap_get_latest_tid, mm_GetBulkInsertState, mm_FreeBulkInsertState,
	  mm_heap_insert, mm_heap_multi_insert, mm_heap_delete, mm_heap_update,
	  mm_heap_lock_tuple, mm_heap_inplace_update, mm_heap_freeze_tuple,
	  mm_heap_tuple_needs_freeze, mm_simple_heap_insert, mm_simple_heap_delete,
	  mm_simple_heap_update, mm_heap_sync, mm_heap_page_prune_opt, mm_heap_page_prune,
	  mm_heap_page_prune_execute, mm_heap_get_root_tuples, mm_ss_report_location,
	  mm_ss_get_location, mm_SyncScanShmemInit, mm_SyncScanShmemSize
	}
};

static const int N_heapam_backends = lengthof(f_heapam_backends);

/* ----------------------------------------------------------------
 *						 heap support routines
 * ----------------------------------------------------------------
 */

/* ----------------
 *		relation_open - open any relation by relation OID
 *
 *		If lockmode is not "NoLock", the specified kind of lock is
 *		obtained on the relation.  (Generally, NoLock should only be
 *		used if the caller knows it has some appropriate lock on the
 *		relation already.)
 *
 *		An error is raised if the relation does not exist.
 *
 *		NB: a "relation" is anything with a pg_class entry.  The caller is
 *		expected to check whether the relkind is something it can handle.
 * ----------------
 */
Relation
relation_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* Get the lock before trying to open the relcache entry */
	if (lockmode != NoLock)
		LockRelationOid(relationId, lockmode);

	/* The relcache does all the real work... */
	r = RelationIdGetRelation(relationId);

	if (!RelationIsValid(r))
		elog(ERROR, "could not open relation with OID %u", relationId);

	/* Make note that we've accessed a temporary relation */
	if (RelationUsesLocalBuffers(r))
		MyXactAccessedTempRel = true;

	pgstat_initstats(r);

	return r;
}

/* ----------------
 *		try_relation_open - open any relation by relation OID
 *
 *		Same as relation_open, except return NULL instead of failing
 *		if the relation does not exist.
 * ----------------
 */
Relation
try_relation_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* Get the lock first */
	if (lockmode != NoLock)
		LockRelationOid(relationId, lockmode);

	/*
	 * Now that we have the lock, probe to see if the relation really exists
	 * or not.
	 */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)))
	{
		/* Release useless lock */
		if (lockmode != NoLock)
			UnlockRelationOid(relationId, lockmode);

		return NULL;
	}

	/* Should be safe to do a relcache load */
	r = RelationIdGetRelation(relationId);

	if (!RelationIsValid(r))
		elog(ERROR, "could not open relation with OID %u", relationId);

	/* Make note that we've accessed a temporary relation */
	if (RelationUsesLocalBuffers(r))
		MyXactAccessedTempRel = true;

	pgstat_initstats(r);

	return r;
}

/* ----------------
 *		relation_openrv - open any relation specified by a RangeVar
 *
 *		Same as relation_open, but the relation is specified by a RangeVar.
 * ----------------
 */
Relation
relation_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	Oid			relOid;

	/*
	 * Check for shared-cache-inval messages before trying to open the
	 * relation.  This is needed even if we already hold a lock on the
	 * relation, because GRANT/REVOKE are executed without taking any lock on
	 * the target relation, and we want to be sure we see current ACL
	 * information.  We can skip this if asked for NoLock, on the assumption
	 * that such a call is not the first one in the current command, and so we
	 * should be reasonably up-to-date already.  (XXX this all could stand to
	 * be redesigned, but for the moment we'll keep doing this like it's been
	 * done historically.)
	 */
	if (lockmode != NoLock)
		AcceptInvalidationMessages();

	/* Look up and lock the appropriate relation using namespace search */
	relOid = RangeVarGetRelid(relation, lockmode, false);

	/* Let relation_open do the rest */
	return relation_open(relOid, NoLock);
}

/* ----------------
 *		relation_openrv_extended - open any relation specified by a RangeVar
 *
 *		Same as relation_openrv, but with an additional missing_ok argument
 *		allowing a NULL return rather than an error if the relation is not
 *		found.  (Note that some other causes, such as permissions problems,
 *		will still result in an ereport.)
 * ----------------
 */
Relation
relation_openrv_extended(const RangeVar *relation, LOCKMODE lockmode,
						 bool missing_ok)
{
	Oid			relOid;

	/*
	 * Check for shared-cache-inval messages before trying to open the
	 * relation.  See comments in relation_openrv().
	 */
	if (lockmode != NoLock)
		AcceptInvalidationMessages();

	/* Look up and lock the appropriate relation using namespace search */
	relOid = RangeVarGetRelid(relation, lockmode, missing_ok);

	/* Return NULL on not-found */
	if (!OidIsValid(relOid))
		return NULL;

	/* Let relation_open do the rest */
	return relation_open(relOid, NoLock);
}

/* ----------------
 *		relation_close - close any relation
 *
 *		If lockmode is not "NoLock", we then release the specified lock.
 *
 *		Note that it is often sensible to hold a lock beyond relation_close;
 *		in that case, the lock is released automatically at xact end.
 * ----------------
 */
void
relation_close(Relation relation, LOCKMODE lockmode)
{
	LockRelId	relid = relation->rd_lockInfo.lockRelId;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* The relcache does the real work... */
	RelationClose(relation);

	if (lockmode != NoLock)
		UnlockRelationId(&relid, lockmode);
}


/* ----------------
 *		heap_open - open a heap relation by relation OID
 *
 *		This is essentially relation_open plus check that the relation
 *		is not an index nor a composite type.  (The caller should also
 *		check that it's not a view or foreign table before assuming it has
 *		storage.)
 * ----------------
 */
Relation
heap_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	r = relation_open(relationId, lockmode);

	if (r->rd_rel->relkind == RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an index",
						RelationGetRelationName(r))));
	else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a composite type",
						RelationGetRelationName(r))));

	return r;
}

/* ----------------
 *		heap_openrv - open a heap relation specified
 *		by a RangeVar node
 *
 *		As above, but relation is specified by a RangeVar.
 * ----------------
 */
Relation
heap_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	Relation	r;

	r = relation_openrv(relation, lockmode);

	if (r->rd_rel->relkind == RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an index",
						RelationGetRelationName(r))));
	else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a composite type",
						RelationGetRelationName(r))));

	return r;
}

/* ----------------
 *		heap_openrv_extended - open a heap relation specified
 *		by a RangeVar node
 *
 *		As above, but optionally return NULL instead of failing for
 *		relation-not-found.
 * ----------------
 */
Relation
heap_openrv_extended(const RangeVar *relation, LOCKMODE lockmode,
					 bool missing_ok)
{
	Relation	r;

	r = relation_openrv_extended(relation, lockmode, missing_ok);

	if (r)
	{
		if (r->rd_rel->relkind == RELKIND_INDEX)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is an index",
							RelationGetRelationName(r))));
		else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a composite type",
							RelationGetRelationName(r))));
	}

	return r;
}

HeapScanDesc heap_beginscan(Relation relation, Snapshot snapshot,
							int nkeys, ScanKey key)
{
	HeapScanDesc HSdesc = (*(f_heapam_backends[relation->rd_storage_backend].heap_beginscan))
		(relation, snapshot, nkeys, key);

	return HSdesc;
}

HeapScanDesc heap_beginscan_catalog(Relation relation, int nkeys,
									ScanKey key)
{
	HeapScanDesc HSdesc = (*(f_heapam_backends[relation->rd_storage_backend].heap_beginscan_catalog))
		(relation, nkeys, key);

	return HSdesc;
}

HeapScanDesc heap_beginscan_strat(Relation relation, Snapshot snapshot,
								  int nkeys, ScanKey key,
								  bool allow_strat, bool allow_sync)
{
	HeapScanDesc HSdesc = (*(f_heapam_backends[relation->rd_storage_backend].heap_beginscan_strat))
		(relation, snapshot, nkeys, key, allow_strat, allow_sync);

	return HSdesc;
}

HeapScanDesc heap_beginscan_bm(Relation relation, Snapshot snapshot,
							   int nkeys, ScanKey key)
{
	HeapScanDesc HSdesc = (*(f_heapam_backends[relation->rd_storage_backend].heap_beginscan_bm))
		(relation, snapshot, nkeys, key);

	return HSdesc;
}


void heap_setscanlimits(HeapScanDesc scan, BlockNumber startBlk,
						BlockNumber endBlk)
{
	Relation relation = scan->rs_rd;

	(*(f_heapam_backends[relation->rd_storage_backend].heap_setscanlimits))
		(scan, startBlk, endBlk);
}


void heap_rescan(HeapScanDesc scan, ScanKey key)
{
	Relation relation = scan->rs_rd;

	(*(f_heapam_backends[relation->rd_storage_backend].heap_rescan)) (scan, key);
}


void heap_endscan(HeapScanDesc scan)
{
	Relation relation = scan->rs_rd;

	(*(f_heapam_backends[relation->rd_storage_backend].heap_endscan)) (scan);
}


HeapTuple heap_getnext(HeapScanDesc scan, ScanDirection direction)
{
	Relation relation = scan->rs_rd;

	HeapTuple heaptup = (*(f_heapam_backends[relation->rd_storage_backend].heap_getnext))
		(scan, direction);

	return heaptup;
}


bool heap_fetch(Relation relation, Snapshot snapshot,
				HeapTuple tuple, Buffer *userbuf, bool keep_buf,
				Relation stats_relation)
{
	bool ret = (*(f_heapam_backends[relation->rd_storage_backend].heap_fetch))
		(relation, snapshot, tuple, userbuf, keep_buf, stats_relation);

	return ret;
}


bool heap_hot_search_buffer(ItemPointer tid, Relation relation,
							Buffer buffer, Snapshot snapshot, HeapTuple heapTuple,
							bool *all_dead, bool first_call)
{
	bool ret = (*(f_heapam_backends[relation->rd_storage_backend].heap_hot_search_buffer))
		(tid, relation, buffer, snapshot, heapTuple, all_dead, first_call);

	return ret;
}


bool heap_hot_search(ItemPointer tid, Relation relation,
					 Snapshot snapshot, bool *all_dead)
{
	bool ret = (*(f_heapam_backends[relation->rd_storage_backend].heap_hot_search))
		(tid, relation, snapshot, all_dead);

	return ret;
}


void heap_get_latest_tid(Relation relation, Snapshot snapshot,
						 ItemPointer tid)
{
	(*(f_heapam_backends[relation->rd_storage_backend].heap_get_latest_tid))
		(relation, snapshot, tid);
}

BulkInsertState GetBulkInsertState(void)
{
	BulkInsertState bistate = (*(f_heapam_backends[STORAGE_BACKEND_DEFAULT].GetBulkInsertState))();

	return bistate;
}

void FreeBulkInsertState(BulkInsertState bistate)
{
	(*(f_heapam_backends[STORAGE_BACKEND_DEFAULT].FreeBulkInsertState))
		(bistate);
}

Oid heap_insert(Relation relation, HeapTuple tup, CommandId cid,
				int options, BulkInsertState bistate)
{
	Oid ret = (*(f_heapam_backends[relation->rd_storage_backend].heap_insert))
		(relation, tup, cid, options, bistate);

	return ret;
}

void heap_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
					   CommandId cid, int options, BulkInsertState bistate)
{
	(*(f_heapam_backends[relation->rd_storage_backend].heap_multi_insert))
		(relation, tuples, ntuples, cid, options, bistate);
}

HTSU_Result heap_delete(Relation relation, ItemPointer tid,
						CommandId cid, Snapshot crosscheck, bool wait,
						HeapUpdateFailureData *hufd)
{
	HTSU_Result htsu_result =  (*(f_heapam_backends[relation->rd_storage_backend].heap_delete))
		(relation, tid, cid, crosscheck, wait, hufd);

	return htsu_result;
}

HTSU_Result heap_update(Relation relation, ItemPointer otid, HeapTuple newtup,
						CommandId cid, Snapshot crosscheck, bool wait,
						HeapUpdateFailureData *hufd, LockTupleMode *lockmode)
{
	HTSU_Result htsu_result = (*(f_heapam_backends[relation->rd_storage_backend].heap_update))
		(relation, otid, newtup, cid, crosscheck, wait, hufd, lockmode);

	return htsu_result;
}

HTSU_Result heap_lock_tuple(Relation relation, HeapTuple tuple,
							CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy,
							bool follow_update,	Buffer *buffer, HeapUpdateFailureData *hufd)
{
	HTSU_Result htsu_result = (*(f_heapam_backends[relation->rd_storage_backend].heap_lock_tuple))
		(relation, tuple, cid, mode, wait_policy, follow_update, buffer, hufd);

	return htsu_result;
}

void heap_inplace_update(Relation relation, HeapTuple tuple)
{
	(*(f_heapam_backends[relation->rd_storage_backend].heap_inplace_update))
		(relation, tuple);
}

bool heap_freeze_tuple(HeapTupleHeader tuple, TransactionId cutoff_xid,
					   TransactionId cutoff_multi)
{
	bool ret = (*(f_heapam_backends[STORAGE_BACKEND_DEFAULT].heap_freeze_tuple))
		(tuple, cutoff_xid, cutoff_multi);

	return ret;
}

bool heap_tuple_needs_freeze(HeapTupleHeader tuple, TransactionId cutoff_xid,
							 MultiXactId cutoff_multi, Buffer buf)
{
	bool ret = (*(f_heapam_backends[STORAGE_BACKEND_DEFAULT].heap_tuple_needs_freeze))
		(tuple, cutoff_xid, cutoff_multi, buf);

	return ret;
}

Oid	simple_heap_insert(Relation relation, HeapTuple tup)
{
	Oid ret = (*(f_heapam_backends[relation->rd_storage_backend].simple_heap_insert))
		(relation, tup);

	return ret;
}

void simple_heap_delete(Relation relation, ItemPointer tid)
{
	(*(f_heapam_backends[relation->rd_storage_backend].simple_heap_delete))
		(relation, tid);
}


void simple_heap_update(Relation relation, ItemPointer otid,
						HeapTuple tup)
{
	(*(f_heapam_backends[relation->rd_storage_backend].simple_heap_update))
		(relation, otid, tup);
}


void heap_sync(Relation relation)
{
	(*(f_heapam_backends[relation->rd_storage_backend].heap_sync))
		(relation);
}


void heap_page_prune_opt(Relation relation, Buffer buffer)
{
	(*(f_heapam_backends[relation->rd_storage_backend].heap_page_prune_opt))
		(relation, buffer);
}

int heap_page_prune(Relation relation, Buffer buffer,
					TransactionId OldestXmin,
					bool report_stats, TransactionId *latestRemovedXid)
{
	int ret = (*(f_heapam_backends[STORAGE_BACKEND_DEFAULT].heap_page_prune))
		(relation, buffer, OldestXmin, report_stats, latestRemovedXid);

	return ret;
}

void heap_page_prune_execute(Buffer buffer,
							 OffsetNumber *redirected, int nredirected,
							 OffsetNumber *nowdead, int ndead,
							 OffsetNumber *nowunused, int nunused)
{
	(*(f_heapam_backends[STORAGE_BACKEND_DEFAULT].heap_page_prune_execute))
		(buffer, redirected, nredirected, nowdead, ndead, nowunused, nunused);
}


void heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
	(*(f_heapam_backends[STORAGE_BACKEND_DEFAULT].heap_get_root_tuples))
		(page, root_offsets);
}


void ss_report_location(Relation rel, BlockNumber location)
{
	(*(f_heapam_backends[rel->rd_storage_backend].ss_report_location))
		(rel, location);
}


BlockNumber ss_get_location(Relation rel, BlockNumber relnblocks)
{
	BlockNumber bn = (*(f_heapam_backends[rel->rd_storage_backend].ss_get_location))
		(rel, relnblocks);

	return bn;
}

void SyncScanShmemInit(void)
{
	(*(f_heapam_backends[STORAGE_BACKEND_DEFAULT].SyncScanShmemInit))();
}


Size SyncScanShmemSize(void)
{
	Size s = (*(f_heapam_backends[STORAGE_BACKEND_DEFAULT].SyncScanShmemSize))();
	return s;
}
