/*
 * Copyright (c) 2015, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY LOG OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * tx.c -- transactions implementation
 */

#include <errno.h>
#include <sys/queue.h>
#include <stdlib.h>

#include "libpmem.h"
#include "libpmemobj.h"
#include "util.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "obj.h"
#include "out.h"
#include "pmalloc.h"
#include "ctree.h"
#include "valgrind_internal.h"

struct tx_data {
	SLIST_ENTRY(tx_data) tx_entry;
	jmp_buf env;
	int errnum;
};

static __thread struct {
	enum pobj_tx_stage stage;
	struct lane_section *section;
} tx;

struct tx_lock_data {
	union {
		PMEMmutex *mutex;
		PMEMrwlock *rwlock;
	} lock;
	enum pobj_tx_lock lock_type;
	SLIST_ENTRY(tx_lock_data) tx_lock;
};

struct lane_tx_runtime {
	PMEMobjpool *pop;
	struct ctree *ranges;
	int cache_slot;
	SLIST_HEAD(txd, tx_data) tx_entries;
	SLIST_HEAD(txl, tx_lock_data) tx_locks;
};

struct tx_alloc_args {
	type_num_t type_num;
	size_t size;
};

struct tx_alloc_copy_args {
	type_num_t type_num;
	size_t size;
	const void *ptr;
	size_t copy_size;
};

struct tx_add_range_args {
	PMEMobjpool *pop;
	uint64_t offset;
	uint64_t size;
};

/*
 * constructor_tx_alloc -- (internal) constructor for normal alloc
 */
static void
constructor_tx_alloc(PMEMobjpool *pop, void *ptr, void *arg)
{
	LOG(3, NULL);

	ASSERTne(ptr, NULL);
	ASSERTne(arg, NULL);

	struct tx_alloc_args *args = arg;

	struct oob_header *oobh = OOB_HEADER_FROM_PTR(ptr);

	/* temporarily add the OOB header */
	VALGRIND_ADD_TO_TX(oobh, OBJ_OOB_SIZE);

	/*
	 * no need to flush and persist because this
	 * will be done in pre-commit phase
	 */
	oobh->data.internal_type = TYPE_NONE;
	oobh->data.user_type = args->type_num;

	VALGRIND_REMOVE_FROM_TX(oobh, OBJ_OOB_SIZE);

	/* do not report changes to the new object */
	VALGRIND_ADD_TO_TX(ptr, args->size);

	VALGRIND_DO_MAKE_MEM_NOACCESS(pop, &oobh->data.padding,
			sizeof (oobh->data.padding));
}

/*
 * constructor_tx_zalloc -- (internal) constructor for zalloc
 */
static void
constructor_tx_zalloc(PMEMobjpool *pop, void *ptr, void *arg)
{
	LOG(3, NULL);

	ASSERTne(ptr, NULL);
	ASSERTne(arg, NULL);

	struct tx_alloc_args *args = arg;

	struct oob_header *oobh = OOB_HEADER_FROM_PTR(ptr);

	/* temporarily add the OOB header */
	VALGRIND_ADD_TO_TX(oobh, OBJ_OOB_SIZE);

	/*
	 * no need to flush and persist because this
	 * will be done in pre-commit phase
	 */
	oobh->data.internal_type = TYPE_NONE;
	oobh->data.user_type = args->type_num;

	VALGRIND_REMOVE_FROM_TX(oobh, OBJ_OOB_SIZE);

	/* do not report changes to the new object */
	VALGRIND_ADD_TO_TX(ptr, args->size);

	VALGRIND_DO_MAKE_MEM_NOACCESS(pop, &oobh->data.padding,
			sizeof (oobh->data.padding));

	memset(ptr, 0, args->size);
}

/*
 * constructor_tx_add_range -- (internal) constructor for add_range
 */
static void
constructor_tx_add_range(PMEMobjpool *pop, void *ptr, void *arg)
{
	LOG(3, NULL);

	ASSERTne(ptr, NULL);
	ASSERTne(arg, NULL);

	struct tx_add_range_args *args = arg;
	struct tx_range *range = ptr;

	/* temporarily add the object copy to the transaction */
	VALGRIND_ADD_TO_TX(OOB_HEADER_FROM_PTR(ptr),
				sizeof (struct tx_range) + args->size
				+ OBJ_OOB_SIZE);

	range->offset = args->offset;
	range->size = args->size;

	void *src = OBJ_OFF_TO_PTR(args->pop, args->offset);

	/* flush offset and size */
	pop->flush(pop, range, sizeof (struct tx_range));
	/* memcpy data and persist */
	pop->memcpy_persist(pop, range->data, src, args->size);

	VALGRIND_REMOVE_FROM_TX(OOB_HEADER_FROM_PTR(ptr),
				sizeof (struct tx_range) + args->size
				+ OBJ_OOB_SIZE);

	/* do not report changes to the original object */
	VALGRIND_ADD_TO_TX(src, args->size);
}

/*
 * constructor_tx_copy -- (internal) copy constructor
 */
static void
constructor_tx_copy(PMEMobjpool *pop, void *ptr, void *arg)
{
	LOG(3, NULL);

	ASSERTne(ptr, NULL);
	ASSERTne(arg, NULL);

	struct tx_alloc_copy_args *args = arg;
	struct oob_header *oobh = OOB_HEADER_FROM_PTR(ptr);

	/* temporarily add the OOB header */
	VALGRIND_ADD_TO_TX(oobh, OBJ_OOB_SIZE);

	/*
	 * no need to flush and persist because this
	 * will be done in pre-commit phase
	 */
	oobh->data.internal_type = TYPE_NONE;
	oobh->data.user_type = args->type_num;

	VALGRIND_REMOVE_FROM_TX(oobh, OBJ_OOB_SIZE);

	/* do not report changes made to the copy */
	VALGRIND_ADD_TO_TX(ptr, args->size);

	VALGRIND_DO_MAKE_MEM_NOACCESS(pop, &oobh->data.padding,
			sizeof (oobh->data.padding));

	memcpy(ptr, args->ptr, args->copy_size);
}

/*
 * constructor_tx_copy_zero -- (internal) copy constructor which zeroes
 * the non-copied area
 */
static void
constructor_tx_copy_zero(PMEMobjpool *pop, void *ptr, void *arg)
{
	LOG(3, NULL);

	ASSERTne(ptr, NULL);
	ASSERTne(arg, NULL);

	struct tx_alloc_copy_args *args = arg;
	struct oob_header *oobh = OOB_HEADER_FROM_PTR(ptr);

	/* temporarily add the OOB header */
	VALGRIND_ADD_TO_TX(oobh, OBJ_OOB_SIZE);

	/*
	 * no need to flush and persist because this
	 * will be done in pre-commit phase
	 */
	oobh->data.internal_type = TYPE_NONE;
	oobh->data.user_type = args->type_num;

	VALGRIND_REMOVE_FROM_TX(oobh, OBJ_OOB_SIZE);

	/* do not report changes made to the copy */
	VALGRIND_ADD_TO_TX(ptr, args->size);

	VALGRIND_DO_MAKE_MEM_NOACCESS(pop, &oobh->data.padding,
			sizeof (oobh->data.padding));

	memcpy(ptr, args->ptr, args->copy_size);
	if (args->size > args->copy_size) {
		void *zero_ptr = (void *)((uintptr_t)ptr + args->copy_size);
		size_t zero_size = args->size - args->copy_size;
		memset(zero_ptr, 0, zero_size);
	}
}

/*
 * tx_set_state -- (internal) set transaction state
 */
static inline void
tx_set_state(PMEMobjpool *pop, struct lane_tx_layout *layout, uint64_t state)
{
	layout->state = state;
	pop->persist(pop, &layout->state, sizeof (layout->state));
}

/*
 * tx_clear_undo_log -- (internal) clear undo log pointed by head
 */
static int
tx_clear_undo_log(PMEMobjpool *pop, struct list_head *head, int vg_clean)
{
	LOG(3, NULL);

	int ret;
	PMEMoid obj;
	while (!OBJ_LIST_EMPTY(head)) {
		obj = head->pe_first;

#ifdef USE_VG_PMEMCHECK
		/*
		 * Clean the valgrind state of the underlying memory for
		 * allocated objects in the undo log, so that not-persisted
		 * modifications after abort are not reported.
		 */
		if (vg_clean) {
			struct oob_header *oobh = OOB_HEADER_FROM_OID(pop, obj);
			size_t size = pmalloc_usable_size(pop,
				obj.off - OBJ_OOB_SIZE);

			VALGRIND_SET_CLEAN(oobh, size);
		}
#endif

		/* remove and free all elements from undo log */
		ret = list_remove_free(pop, head, 0, NULL, &obj);

		ASSERTeq(ret, 0);
		if (ret) {
			LOG(2, "list_remove_free failed");
			return ret;
		}
	}

	return 0;
}

/*
 * tx_abort_alloc -- (internal) abort all allocated objects
 */
static int
tx_abort_alloc(PMEMobjpool *pop, struct lane_tx_layout *layout)
{
	LOG(3, NULL);

	return tx_clear_undo_log(pop, &layout->undo_alloc, 1);
}

/*
 * tx_abort_free -- (internal) abort all freeing objects
 */
static int
tx_abort_free(PMEMobjpool *pop, struct lane_tx_layout *layout)
{
	LOG(3, NULL);

	int ret;
	PMEMoid obj;
	while (!OBJ_LIST_EMPTY(&layout->undo_free)) {
		obj = layout->undo_free.pe_first;

		struct oob_header *oobh = OOB_HEADER_FROM_OID(pop, obj);
		ASSERT(oobh->data.user_type < PMEMOBJ_NUM_OID_TYPES);

		struct object_store_item *obj_list =
			&pop->store->bytype[oobh->data.user_type];

		/* move all objects back to object store */
		ret = list_move_oob(pop,
				&layout->undo_free, &obj_list->head, obj);

		ASSERTeq(ret, 0);
		if (ret) {
			LOG(2, "list_move_oob failed");
			return ret;
		}
	}

	return 0;
}

struct tx_range_data {
	void *begin;
	void *end;
	SLIST_ENTRY(tx_range_data) tx_range;
};

/*
 * tx_restore_range -- (internal) restore a single range from undo log
 *
 * If the snapshot contains any PMEM locks that are held by the current
 * transaction, they won't be overwritten with the saved data to avoid changing
 * their state.  Those locks will be released in tx_end().
 */
static void
tx_restore_range(PMEMobjpool *pop, struct tx_range *range)
{
	COMPILE_ERROR_ON(sizeof (PMEMmutex) != _POBJ_CL_ALIGNMENT);
	COMPILE_ERROR_ON(sizeof (PMEMrwlock) != _POBJ_CL_ALIGNMENT);
	COMPILE_ERROR_ON(sizeof (PMEMcond) != _POBJ_CL_ALIGNMENT);

	struct lane_tx_runtime *runtime =
			(struct lane_tx_runtime *)tx.section->runtime;
	ASSERTne(runtime, NULL);

	SLIST_HEAD(txr, tx_range_data) tx_ranges;
	SLIST_INIT(&tx_ranges);

	struct tx_range_data *txr;
	txr = Malloc(sizeof (*txr));
	if (txr == NULL) {
		FATAL("!Malloc");
	}

	txr->begin = OBJ_OFF_TO_PTR(pop, range->offset);
	txr->end = (char *)txr->begin + range->size;
	SLIST_INSERT_HEAD(&tx_ranges, txr, tx_range);

	struct tx_lock_data *txl;
	struct tx_range_data *txrn;

	/* check if there are any locks within given memory range */
	SLIST_FOREACH(txl, &(runtime->tx_locks), tx_lock) {
		void *lock_begin = txl->lock.mutex;
		/* all PMEM locks have the same size */
		void *lock_end = (char *)lock_begin + _POBJ_CL_ALIGNMENT;

		SLIST_FOREACH(txr, &tx_ranges, tx_range) {
			if ((lock_begin >= txr->begin &&
				lock_begin < txr->end) ||
				(lock_end >= txr->begin &&
				lock_end < txr->end)) {
				LOG(4, "detected PMEM lock"
					"in undo log; "
					"range %p-%p, lock %p-%p",
					txr->begin, txr->end,
					lock_begin, lock_end);

				/* split the range into new ones */
				if (lock_begin > txr->begin) {
					txrn = Malloc(sizeof (*txrn));
					if (txrn == NULL) {
						FATAL("!Malloc");
					}
					txrn->begin = txr->begin;
					txrn->end = lock_begin;
					LOG(4, "range split; %p-%p",
						txrn->begin, txrn->end);
					SLIST_INSERT_HEAD(&tx_ranges,
							txrn, tx_range);
				}

				if (lock_end < txr->end) {
					txrn = Malloc(sizeof (*txrn));
					if (txrn == NULL) {
						FATAL("!Malloc");
					}
					txrn->begin = lock_end;
					txrn->end = txr->end;
					LOG(4, "range split; %p-%p",
						txrn->begin, txrn->end);
					SLIST_INSERT_HEAD(&tx_ranges,
							txrn, tx_range);
				}

				/*
				 * remove the original range
				 * from the list
				 */
				SLIST_REMOVE(&tx_ranges, txr,
						tx_range_data, tx_range);
				Free(txr);
				break;
			}
		}
	}

	ASSERT(!SLIST_EMPTY(&tx_ranges));

	void *dst_ptr = OBJ_OFF_TO_PTR(pop, range->offset);

	while (!SLIST_EMPTY(&tx_ranges)) {
		txr = SLIST_FIRST(&tx_ranges);
		SLIST_REMOVE_HEAD(&tx_ranges, tx_range);
		/* restore partial range data from snapshot */
		ASSERT((char *)txr->begin >= (char *)dst_ptr);
		uint8_t *src = &range->data[
				(char *)txr->begin - (char *)dst_ptr];
		ASSERT((char *)txr->end >= (char *)txr->begin);
		size_t size = (size_t)((char *)txr->end - (char *)txr->begin);
		pop->memcpy_persist(pop, txr->begin, src, size);
		Free(txr);
	}
}

/*
 * tx_foreach_set -- (internal) iterates over every memory range
 */
static void
tx_foreach_set(PMEMobjpool *pop, struct lane_tx_layout *layout,
	void (*cb)(PMEMobjpool *pop, struct tx_range *range))
{
	LOG(3, NULL);

	struct tx_range *range = NULL;

	PMEMoid iter;
	for (iter = layout->undo_set.pe_first; !OBJ_OID_IS_NULL(iter);
		iter = oob_list_next(pop, &layout->undo_set, iter)) {

		range = OBJ_OFF_TO_PTR(pop, iter.off);
		cb(pop, range);
	}

	/* cached objects */
	for (iter = layout->undo_set_cache.pe_first; !OBJ_OID_IS_NULL(iter);
		iter = oob_list_next(pop, &layout->undo_set_cache, iter)) {

		struct tx_range_cache *cache = OBJ_OFF_TO_PTR(pop, iter.off);
		for (int i = 0; i < MAX_CACHED_RANGES; ++i) {
			range = (struct tx_range *)&cache->range[i];
			if (range->offset == 0 || range->size == 0)
				break;

			cb(pop, range);
		}
	}
}

/*
 * tx_abort_restore_range -- (internal) restores content of the memory range
 */
static void
tx_abort_restore_range(PMEMobjpool *pop, struct tx_range *range)
{
	tx_restore_range(pop, range);
}

/*
 * tx_abort_recover_range -- (internal) restores content while skipping locks
 */
static void
tx_abort_recover_range(PMEMobjpool *pop, struct tx_range *range)
{
	void *ptr = OBJ_OFF_TO_PTR(pop, range->offset);
	pop->memcpy_persist(pop, ptr, range->data, range->size);
}

/*
 * tx_abort_set -- (internal) abort all set operations
 */
static int
tx_abort_set(PMEMobjpool *pop, struct lane_tx_layout *layout, int recovery)
{
	LOG(3, NULL);

	if (recovery)
		tx_foreach_set(pop, layout, tx_abort_recover_range);
	else
		tx_foreach_set(pop, layout, tx_abort_restore_range);

	int ret = tx_clear_undo_log(pop, &layout->undo_set_cache, 0);
	ret |= tx_clear_undo_log(pop, &layout->undo_set, 0);

	return ret;
}

/*
 * tx_pre_commit_alloc -- (internal) do pre-commit operations for
 * allocated objects
 */
static void
tx_pre_commit_alloc(PMEMobjpool *pop, struct lane_tx_layout *layout)
{
	LOG(3, NULL);

	PMEMoid iter;
	for (iter = layout->undo_alloc.pe_first; !OBJ_OID_IS_NULL(iter);
		iter = oob_list_next(pop,
			&layout->undo_alloc, iter)) {

		struct oob_header *oobh = OOB_HEADER_FROM_OID(pop, iter);

		VALGRIND_ADD_TO_TX(oobh, OBJ_OOB_SIZE);

		/*
		 * Set object as allocated.
		 * This must be done in pre-commit phase instead of at
		 * allocation time in order to handle properly the case when
		 * the object is allocated and freed in the same transaction.
		 * In such case we need to know that the object
		 * is on undo log list and not in object store.
		 */
		oobh->data.internal_type = TYPE_ALLOCATED;

		VALGRIND_REMOVE_FROM_TX(oobh, OBJ_OOB_SIZE);

		size_t size = pmalloc_usable_size(pop,
				iter.off - OBJ_OOB_SIZE);

		VALGRIND_DO_MAKE_MEM_DEFINED(pop, oobh->data.padding,
				sizeof (oobh->data.padding));
		/* flush and persist the whole allocated area and oob header */
		pop->persist(pop, oobh, size);
		VALGRIND_DO_MAKE_MEM_NOACCESS(pop, oobh->data.padding,
				sizeof (oobh->data.padding));
	}
}

/*
 * tx_pre_commit_range_persist -- (internal) flushes memory range to persistence
 */
static void
tx_pre_commit_range_persist(PMEMobjpool *pop, struct tx_range *range)
{
	void *ptr = OBJ_OFF_TO_PTR(pop, range->offset);
	pop->persist(pop, ptr, range->size);
}

/*
 * tx_pre_commit_set -- (internal) do pre-commit operations for
 * set operations
 */
static void
tx_pre_commit_set(PMEMobjpool *pop, struct lane_tx_layout *layout)
{
	LOG(3, NULL);

	tx_foreach_set(pop, layout, tx_pre_commit_range_persist);
}

/*
 * tx_post_commit_alloc -- (internal) do post commit operations for
 * allocated objects
 */
static int
tx_post_commit_alloc(PMEMobjpool *pop, struct lane_tx_layout *layout)
{
	LOG(3, NULL);

	PMEMoid obj;
	int ret;
	while (!OBJ_LIST_EMPTY(&layout->undo_alloc)) {
		obj = layout->undo_alloc.pe_first;

		struct oob_header *oobh = OOB_HEADER_FROM_OID(pop, obj);
		ASSERT(oobh->data.user_type < PMEMOBJ_NUM_OID_TYPES);

		struct object_store_item *obj_list =
			&pop->store->bytype[oobh->data.user_type];

		/* move object to object store */
		ret = list_move_oob(pop,
				&layout->undo_alloc, &obj_list->head, obj);

		ASSERTeq(ret, 0);
		if (ret) {
			LOG(2, "list_move_oob failed");
			return ret;
		}
	}

	return 0;
}

/*
 * tx_post_commit_free -- (internal) do post commit operations for
 * freeing objects
 */
static int
tx_post_commit_free(PMEMobjpool *pop, struct lane_tx_layout *layout)
{
	LOG(3, NULL);

	return tx_clear_undo_log(pop, &layout->undo_free, 0);
}

/*
 * tx_post_commit_set -- (internal) do post commit operations for
 * add range
 */
static int
tx_post_commit_set(PMEMobjpool *pop, struct lane_tx_layout *layout)
{
	LOG(3, NULL);

	struct list_head *head = &layout->undo_set_cache;
	int ret = 0;
	PMEMoid obj = head->pe_first; /* first cache object */

	/* clear all the undo log caches except for the last one */
	while (head->pe_first.off != oob_list_last(pop, head).off) {
		obj = head->pe_first;

		ret = list_remove_free(pop, head, 0, NULL, &obj);

		ASSERTeq(ret, 0);
	}

	if (!OID_IS_NULL(obj)) {
		/* zero the cache, will be useful later on */
		struct tx_range_cache *cache = OBJ_OFF_TO_PTR(pop, obj.off);

		VALGRIND_ADD_TO_TX(cache, sizeof (struct tx_range_cache));
		pop->memset_persist(pop, cache, 0,
			sizeof (struct tx_range_cache));
		VALGRIND_REMOVE_FROM_TX(cache, sizeof (struct tx_range_cache));
	}

	ret |= tx_clear_undo_log(pop, &layout->undo_set, 0);

	return ret;
}

/*
 * tx_pre_commit -- (internal) do pre-commit operations
 */
static void
tx_pre_commit(PMEMobjpool *pop, struct lane_tx_layout *layout)
{
	LOG(3, NULL);

	tx_pre_commit_set(pop, layout);
	tx_pre_commit_alloc(pop, layout);
}

/*
 * tx_post_commit -- (internal) do post commit operations
 */
static int
tx_post_commit(PMEMobjpool *pop, struct lane_tx_layout *layout)
{
	LOG(3, NULL);

	int ret;

	ret = tx_post_commit_set(pop, layout);
	ASSERTeq(ret, 0);
	if (ret) {
		LOG(2, "tx_post_commit_set failed");
		return ret;
	}

	ret = tx_post_commit_alloc(pop, layout);
	ASSERTeq(ret, 0);
	if (ret) {
		LOG(2, "tx_post_commit_alloc failed");
		return ret;
	}

	ret = tx_post_commit_free(pop, layout);
	ASSERTeq(ret, 0);
	if (ret) {
		LOG(2, "tx_post_commit_free failed");
		return ret;
	}

	return 0;
}

/*
 * tx_abort -- (internal) abort all allocated objects
 */
static int
tx_abort(PMEMobjpool *pop, struct lane_tx_layout *layout, int recovery)
{
	LOG(3, NULL);

	int ret;

	ret = tx_abort_set(pop, layout, recovery);
	ASSERTeq(ret, 0);
	if (ret) {
		LOG(2, "tx_abort_set failed");
		return ret;
	}

	ret = tx_abort_alloc(pop, layout);
	ASSERTeq(ret, 0);
	if (ret) {
		LOG(2, "tx_abort_alloc failed");
		return ret;
	}

	ret = tx_abort_free(pop, layout);
	ASSERTeq(ret, 0);
	if (ret) {
		LOG(2, "tx_abort_free failed");
		return ret;
	}

	return 0;
}

/*
 * add_to_tx_and_lock -- (internal) add lock to the transaction and acquire it
 */
static int
add_to_tx_and_lock(struct lane_tx_runtime *lane, enum pobj_tx_lock type,
	void *lock)
{
	LOG(15, NULL);
	int retval = 0;
	struct tx_lock_data *txl;
	/* check if the lock is already on the list */
	SLIST_FOREACH(txl, &(lane->tx_locks), tx_lock) {
		if (memcmp(&txl->lock, &lock, sizeof (lock)) == 0)
			return retval;
	}

	txl = Malloc(sizeof (*txl));
	if (txl == NULL)
		return ENOMEM;

	txl->lock_type = type;
	switch (txl->lock_type) {
		case TX_LOCK_MUTEX:
			txl->lock.mutex = lock;
			retval = pmemobj_mutex_lock(lane->pop,
				txl->lock.mutex);
			break;
		case TX_LOCK_RWLOCK:
			txl->lock.rwlock = lock;
			retval = pmemobj_rwlock_wrlock(lane->pop,
				txl->lock.rwlock);
			break;
		default:
			ERR("Unrecognized lock type");
			ASSERT(0);
			break;
	}

	SLIST_INSERT_HEAD(&lane->tx_locks, txl, tx_lock);

	return retval;
}

/*
 * release_and_free_tx_locks -- (internal) release and remove all locks from the
 *				transaction
 */
static void
release_and_free_tx_locks(struct lane_tx_runtime *lane)
{
	LOG(15, NULL);

	while (!SLIST_EMPTY(&lane->tx_locks)) {
		struct tx_lock_data *tx_lock = SLIST_FIRST(&lane->tx_locks);
		SLIST_REMOVE_HEAD(&lane->tx_locks, tx_lock);
		switch (tx_lock->lock_type) {
			case TX_LOCK_MUTEX:
				pmemobj_mutex_unlock(lane->pop,
					tx_lock->lock.mutex);
				break;
			case TX_LOCK_RWLOCK:
				pmemobj_rwlock_unlock(lane->pop,
					tx_lock->lock.rwlock);
				break;
			default:
				ERR("Unrecognized lock type");
				ASSERT(0);
				break;
		}
		Free(tx_lock);
	}
}

/*
 * tx_alloc_common -- (internal) common function for alloc and zalloc
 */
static PMEMoid
tx_alloc_common(size_t size, type_num_t type_num,
	void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg))
{
	LOG(3, NULL);

	if (tx.stage != TX_STAGE_WORK) {
		ERR("invalid tx stage");
		errno = EINVAL;
		return OID_NULL;
	}

	if (size > PMEMOBJ_MAX_ALLOC_SIZE) {
		ERR("requested size too large");
		errno = ENOMEM;
		pmemobj_tx_abort(EINVAL);
		return OID_NULL;
	}

	/* callers should have checked this */
	ASSERT(type_num < PMEMOBJ_NUM_OID_TYPES);

	struct lane_tx_runtime *lane =
		(struct lane_tx_runtime *)tx.section->runtime;

	struct lane_tx_layout *layout =
		(struct lane_tx_layout *)tx.section->layout;

	struct tx_alloc_args args = {
		.type_num = type_num,
		.size = size,
	};

	/* allocate object to undo log */
	PMEMoid retoid = OID_NULL;
	list_insert_new(lane->pop, &layout->undo_alloc,
			0, NULL, OID_NULL, 0,
			size, constructor, &args, &retoid);

	if (OBJ_OID_IS_NULL(retoid) ||
		ctree_insert(lane->ranges, retoid.off, size) != 0)
		goto err_oom;

	return retoid;

err_oom:
	ERR("out of memory");
	errno = ENOMEM;
	pmemobj_tx_abort(ENOMEM);

	return OID_NULL;
}

/*
 * tx_alloc_copy_common -- (internal) common function for alloc with data copy
 */
static PMEMoid
tx_alloc_copy_common(size_t size, type_num_t type_num, const void *ptr,
	size_t copy_size, void (*constructor)(PMEMobjpool *pop, void *ptr,
	void *arg))
{
	LOG(3, NULL);

	if (size > PMEMOBJ_MAX_ALLOC_SIZE) {
		ERR("requested size too large");
		errno = ENOMEM;
		pmemobj_tx_abort(EINVAL);
		return OID_NULL;
	}

	/* callers should have checked this */
	ASSERT(type_num < PMEMOBJ_NUM_OID_TYPES);

	struct lane_tx_runtime *lane =
		(struct lane_tx_runtime *)tx.section->runtime;

	struct lane_tx_layout *layout =
		(struct lane_tx_layout *)tx.section->layout;

	struct tx_alloc_copy_args args = {
		.type_num = type_num,
		.size = size,
		.ptr = ptr,
		.copy_size = copy_size,
	};

	/* allocate object to undo log */
	PMEMoid retoid;
	int ret = list_insert_new(lane->pop, &layout->undo_alloc,
			0, NULL, OID_NULL, 0,
			size, constructor, &args, &retoid);

	if (ret || OBJ_OID_IS_NULL(retoid) ||
		ctree_insert(lane->ranges, retoid.off, size) != 0)
		goto err_oom;

	return retoid;

err_oom:
	ERR("out of memory");
	errno = ENOMEM;
	pmemobj_tx_abort(ENOMEM);

	return OID_NULL;
}

/*
 * tx_realloc_common -- (internal) common function for tx realloc
 */
static PMEMoid
tx_realloc_common(PMEMoid oid, size_t size, unsigned int type_num,
	void (*constructor_alloc)(PMEMobjpool *pop, void *ptr, void *arg),
	void (*constructor_realloc)(PMEMobjpool *pop, void *ptr, void *arg))
{
	LOG(3, NULL);

	if (tx.stage != TX_STAGE_WORK) {
		ERR("invalid tx stage");
		errno = EINVAL;
		return OID_NULL;
	}

	if (size > PMEMOBJ_MAX_ALLOC_SIZE) {
		ERR("requested size too large");
		errno = ENOMEM;
		pmemobj_tx_abort(EINVAL);
		return OID_NULL;
	}

	if (type_num >= PMEMOBJ_NUM_OID_TYPES) {
		ERR("invalid type_num %d", type_num);
		errno = EINVAL;
		pmemobj_tx_abort(EINVAL);
		return OID_NULL;
	}

	struct lane_tx_runtime *lane =
		(struct lane_tx_runtime *)tx.section->runtime;

	/* if oid is NULL just alloc */
	if (OBJ_OID_IS_NULL(oid))
		return tx_alloc_common(size, (type_num_t)type_num,
				constructor_alloc);

	ASSERT(OBJ_OID_IS_VALID(lane->pop, oid));

	/* if size is 0 just free */
	if (size == 0) {
		if (pmemobj_tx_free(oid)) {
			ERR("pmemobj_tx_free failed");
			return oid;
		} else {
			return OID_NULL;
		}
	}

	/* oid is not NULL and size is not 0 so do realloc by alloc and free */
	void *ptr = OBJ_OFF_TO_PTR(lane->pop, oid.off);
	size_t old_size = pmalloc_usable_size(lane->pop,
			oid.off - OBJ_OOB_SIZE) - OBJ_OOB_SIZE;

	size_t copy_size = old_size < size ? old_size : size;

	PMEMoid new_obj = tx_alloc_copy_common(size, (type_num_t)type_num,
			ptr, copy_size, constructor_realloc);

	if (!OBJ_OID_IS_NULL(new_obj)) {
		if (pmemobj_tx_free(oid)) {
			ERR("pmemobj_tx_free failed");
			struct lane_tx_layout *layout =
				(struct lane_tx_layout *)tx.section->layout;
			int ret = list_remove_free(lane->pop,
					&layout->undo_alloc,
					0, NULL, &new_obj);
			/* XXX fatal error */
			ASSERTeq(ret, 0);
			if (ret)
				ERR("list_remove_free failed");

			return OID_NULL;
		}
	}

	return new_obj;
}

/*
 * pmemobj_tx_begin -- initializes new transaction
 */
int
pmemobj_tx_begin(PMEMobjpool *pop, jmp_buf env, ...)
{
	LOG(3, NULL);
	VALGRIND_START_TX;

	int err = 0;

	struct lane_tx_runtime *lane = NULL;
	if (tx.stage == TX_STAGE_WORK) {
		lane = tx.section->runtime;
	} else if (tx.stage == TX_STAGE_NONE) {
		if ((err = lane_hold(pop, &tx.section,
			LANE_SECTION_TRANSACTION)) != 0)
			goto err_abort;

		lane = tx.section->runtime;
		SLIST_INIT(&lane->tx_entries);
		SLIST_INIT(&lane->tx_locks);
		lane->ranges = ctree_new();
		lane->cache_slot = 0;

		lane->pop = pop;
	} else {
		err = EINVAL;
		goto err_abort;
	}

	struct tx_data *txd = Malloc(sizeof (*txd));
	if (txd == NULL) {
		err = ENOMEM;
		goto err_abort;
	}

	txd->errnum = 0;
	if (env != NULL)
		memcpy(txd->env, env, sizeof (jmp_buf));
	else
		memset(txd->env, 0, sizeof (jmp_buf));

	SLIST_INSERT_HEAD(&lane->tx_entries, txd, tx_entry);

	/* handle locks */
	va_list argp;
	va_start(argp, env);
	enum pobj_tx_lock lock_type;

	while ((lock_type = va_arg(argp, enum pobj_tx_lock)) != TX_LOCK_NONE) {
		if ((err = add_to_tx_and_lock(lane,
				lock_type, va_arg(argp, void *))) != 0) {
			va_end(argp);
			goto err_abort;
		}
	}
	va_end(argp);

	tx.stage = TX_STAGE_WORK;
	ASSERT(err == 0);
	return 0;

err_abort:
	tx.stage = TX_STAGE_ONABORT;
	return err;
}

/*
 * pmemobj_tx_stage -- returns current transaction stage
 */
enum pobj_tx_stage
pmemobj_tx_stage()
{
	LOG(3, NULL);

	return tx.stage;
}

/*
 * pmemobj_tx_abort -- aborts current transaction
 */
void
pmemobj_tx_abort(int errnum)
{
	LOG(3, NULL);

	ASSERT(tx.section != NULL);
	ASSERT(tx.stage == TX_STAGE_WORK);

	tx.stage = TX_STAGE_ONABORT;
	struct lane_tx_runtime *lane = tx.section->runtime;
	struct tx_data *txd = SLIST_FIRST(&lane->tx_entries);

	if (SLIST_NEXT(txd, tx_entry) == NULL) {
		/* this is the outermost transaction */

		struct lane_tx_layout *layout =
				(struct lane_tx_layout *)tx.section->layout;

		/* process the undo log */
		tx_abort(lane->pop, layout, 0 /* abort */);
	}

	txd->errnum = errnum;
	if (!util_is_zeroed(txd->env, sizeof (jmp_buf)))
		longjmp(txd->env, errnum);
}

/*
 * pmemobj_tx_commit -- commits current transaction
 */
int
pmemobj_tx_commit()
{
	LOG(3, NULL);
	int ret = 0;

	ASSERT(tx.section != NULL);
	ASSERT(tx.stage == TX_STAGE_WORK);

	struct lane_tx_runtime *lane =
		(struct lane_tx_runtime *)tx.section->runtime;
	struct tx_data *txd = SLIST_FIRST(&lane->tx_entries);

	if (SLIST_NEXT(txd, tx_entry) == NULL) {
		/* this is the outermost transaction */

		struct lane_tx_layout *layout =
			(struct lane_tx_layout *)tx.section->layout;

		/* pre-commit phase */
		tx_pre_commit(lane->pop, layout);

		/* set transaction state as committed */
		tx_set_state(lane->pop, layout, TX_STATE_COMMITTED);

		/* post commit phase */
		ret = tx_post_commit(lane->pop, layout);
		ASSERTeq(ret, 0);

		if (!ret) {
			/* clear transaction state */
			tx_set_state(lane->pop, layout, TX_STATE_NONE);
		} else {
			/* XXX need to handle this case somehow */
			LOG(2, "tx_post_commit failed");
		}
	}

	tx.stage = TX_STAGE_ONCOMMIT;

	return ret;
}

/*
 * pmemobj_tx_end -- ends current transaction
 */
void
pmemobj_tx_end()
{
	LOG(3, NULL);
	ASSERT(tx.stage != TX_STAGE_WORK);

	if (tx.section == NULL) {
		tx.stage = TX_STAGE_NONE;
		return;
	}

	struct lane_tx_runtime *lane = tx.section->runtime;
	struct tx_data *txd = SLIST_FIRST(&lane->tx_entries);
	SLIST_REMOVE_HEAD(&lane->tx_entries, tx_entry);

	int errnum = txd->errnum;
	Free(txd);

	VALGRIND_END_TX;

	if (SLIST_EMPTY(&lane->tx_entries)) {
		/* this is the outermost transaction */
		struct lane_tx_layout *layout =
			(struct lane_tx_layout *)tx.section->layout;

		/* cleanup cache */
		ctree_delete(lane->ranges);
		lane->cache_slot = 0;

		/* the transaction state and undo log should be clear */
		ASSERTeq(layout->state, TX_STATE_NONE);
		if (layout->state != TX_STATE_NONE)
			LOG(2, "invalid transaction state");

		ASSERT(OBJ_LIST_EMPTY(&layout->undo_alloc));
		if (!OBJ_LIST_EMPTY(&layout->undo_alloc))
			LOG(2, "allocations undo log is not empty");

		tx.stage = TX_STAGE_NONE;
		release_and_free_tx_locks(lane);
		if (lane_release(lane->pop)) {
			LOG(2, "lane_release failed");
			ASSERT(0);
		}
		tx.section = NULL;
	} else {
		/* resume the next transaction */
		tx.stage = TX_STAGE_WORK;

		/* abort called within inner transaction, waterfall the error */
		if (errnum)
			pmemobj_tx_abort(errnum);
	}
}

/*
 * pmemobj_tx_process -- processes current transaction stage
 */
int
pmemobj_tx_process()
{
	LOG(3, NULL);

	ASSERT(tx.section != NULL);
	ASSERT(tx.stage != TX_STAGE_NONE);

	switch (tx.stage) {
	case TX_STAGE_NONE:
		break;
	case TX_STAGE_WORK:
		return pmemobj_tx_commit();
	case TX_STAGE_ONABORT:
	case TX_STAGE_ONCOMMIT:
		tx.stage = TX_STAGE_FINALLY;
		break;
	case TX_STAGE_FINALLY:
		tx.stage = TX_STAGE_NONE;
		break;
	case MAX_TX_STAGE:
		ASSERT(1);
	}

	return 0;
}

/*
 * pmemobj_tx_add_large -- (internal) adds large memory range to undo log
 */
static int
pmemobj_tx_add_large(struct lane_tx_layout *layout,
	struct tx_add_range_args *args)
{
	/* insert snapshot to undo log */
	PMEMoid snapshot;
	int ret = list_insert_new(args->pop, &layout->undo_set, 0,
			NULL, OID_NULL, 0,
			args->size + sizeof (struct tx_range),
			constructor_tx_add_range, args, &snapshot);

	return ret;
}

/*
 * constructor_tx_range_cache -- (internal) cache constructor
 */
static void
constructor_tx_range_cache(PMEMobjpool *pop, void *ptr, void *arg)
{
	LOG(3, NULL);

	ASSERTne(ptr, NULL);

	/* temporarily add the object copy to the transaction */
	VALGRIND_ADD_TO_TX(ptr, sizeof (struct tx_range_cache));

	pop->memset_persist(pop, ptr, 0, sizeof (struct tx_range_cache));

	VALGRIND_REMOVE_FROM_TX(ptr, sizeof (struct tx_range_cache));
}

/*
 * pmemobj_tx_get_range_cache -- (internal) returns first available cache
 */
static struct tx_range_cache *
pmemobj_tx_get_range_cache(PMEMobjpool *pop, struct lane_tx_layout *layout)
{
	PMEMoid last_cache = oob_list_last(pop, &layout->undo_set_cache);
	struct tx_range_cache *cache = NULL;
	/* get the last element from the caches list */
	if (!OBJ_OID_IS_NULL(last_cache))
		cache = OBJ_OFF_TO_PTR(pop, last_cache.off);

	/* verify if the cache exists and has at least one free slot */
	if (cache == NULL || cache->range[MAX_CACHED_RANGES - 1].offset != 0) {
		/* no existing cache, allocate a new one */
		PMEMoid ncache_oid;
		if (list_insert_new(pop, &layout->undo_set_cache,
			0, NULL, OID_NULL, 0, sizeof (struct tx_range_cache),
			constructor_tx_range_cache, NULL, &ncache_oid) != 0)
			return NULL;

		cache = OBJ_OFF_TO_PTR(pop, ncache_oid.off);

		/* since the cache is new, we start the count from 0 */
		struct lane_tx_runtime *runtime = tx.section->runtime;
		runtime->cache_slot = 0;
	}

	return cache;
}

/*
 * pmemobj_tx_add_small -- (internal) adds small memory range to undo log cache
 */
static int
pmemobj_tx_add_small(struct lane_tx_layout *layout,
	struct tx_add_range_args *args)
{
	PMEMobjpool *pop = args->pop;

	struct tx_range_cache *cache = pmemobj_tx_get_range_cache(pop, layout);

	if (cache == NULL) {
		ERR("Failed to create range cache");
		return 1;
	}

	struct lane_tx_runtime *runtime = tx.section->runtime;

	int n = runtime->cache_slot++; /* first free cache slot */

	ASSERT(n != MAX_CACHED_RANGES);

	/* those structures are binary compatible */
	struct tx_range *range = (struct tx_range *)&cache->range[n];
	VALGRIND_ADD_TO_TX(range,
		sizeof (struct tx_range) + MAX_CACHED_RANGE_SIZE);

	/* this isn't transactional so we have to keep the order */
	void *src = OBJ_OFF_TO_PTR(pop, args->offset);
	VALGRIND_ADD_TO_TX(src, args->size);

	pop->memcpy_persist(pop, range->data, src, args->size);

	/* the range is only valid if both size and offset are != 0 */
	range->size = args->size;
	range->offset = args->offset;
	pop->persist(pop, range,
		sizeof (range->offset) + sizeof (range->size));

	VALGRIND_REMOVE_FROM_TX(range,
		sizeof (struct tx_range) + MAX_CACHED_RANGE_SIZE);

	return 0;
}

/*
 * pmemobj_tx_add_common -- (internal) common code for adding persistent memory
 *				into the transaction
 */
static int
pmemobj_tx_add_common(struct tx_add_range_args *args)
{
	LOG(15, NULL);

	struct lane_tx_layout *layout =
			(struct lane_tx_layout *)tx.section->layout;

	if (args->offset < args->pop->heap_offset ||
		(args->offset + args->size) >
		(args->pop->heap_offset + args->pop->heap_size)) {
		ERR("object outside of heap");
		return EINVAL;
	}

	struct lane_tx_runtime *runtime = tx.section->runtime;

	/* starting from the end, search for all overlapping ranges */
	uint64_t spoint = args->offset + args->size - 1; /* start point */
	uint64_t apoint = 0; /* add point */
	int ret = 0;

	while (spoint >= args->offset) {
		apoint = spoint + 1;
		/* find range less than starting point */
		uint64_t range = ctree_find_le(runtime->ranges, &spoint);
		struct tx_add_range_args nargs;
		nargs.pop = args->pop;

		if (spoint < args->offset) { /* the found offset is earlier */
			nargs.size = apoint - args->offset;
			/* overlap on the left edge */
			if (spoint + range > args->offset) {
				nargs.offset = spoint + range;
				if (nargs.size <= nargs.offset - args->offset)
					break;
				nargs.size -= nargs.offset - args->offset;
			} else {
				nargs.offset = args->offset;
			}

			if (args->size == 0)
				break;

			spoint = 0; /* this is the end of our search */
		} else { /* found offset is equal or greater than offset */
			nargs.offset = spoint + range;
			spoint -= 1;
			if (nargs.offset >= apoint)
				continue;

			nargs.size = apoint - nargs.offset;
		}

		/*
		 * Depending on the size of the block, either allocate an
		 * entire new object or use cache.
		 */
		ret = nargs.size > MAX_CACHED_RANGE_SIZE ?
			pmemobj_tx_add_large(layout, &nargs) :
			pmemobj_tx_add_small(layout, &nargs);

		if (ret != 0)
			break;


		if (ctree_insert(runtime->ranges,
			nargs.offset, nargs.size) != 0)
			break;
	}

	if (ret != 0) {
		ERR("out of memory");
		errno = ENOMEM;
		pmemobj_tx_abort(ENOMEM);
	}

	return ret;
}

/*
 * pmemobj_tx_add_range_direct -- adds persistent memory range into the
 *					transaction
 */
int
pmemobj_tx_add_range_direct(void *ptr, size_t size)
{
	LOG(3, NULL);

	if (tx.stage != TX_STAGE_WORK) {
		ERR("invalid tx stage");
		return EINVAL;
	}

	struct lane_tx_runtime *lane =
		(struct lane_tx_runtime *)tx.section->runtime;

	if ((char *)ptr < (char *)lane->pop ||
			(char *)ptr >= (char *)lane->pop + lane->pop->size) {
		ERR("object outside of pool");
		return EINVAL;
	}

	struct tx_add_range_args args = {
		.pop = lane->pop,
		.offset = (uint64_t)((char *)ptr - (char *)lane->pop),
		.size = size
	};

	return pmemobj_tx_add_common(&args);
}

/*
 * pmemobj_tx_add_range -- adds persistent memory range into the transaction
 */
int
pmemobj_tx_add_range(PMEMoid oid, uint64_t hoff, size_t size)
{
	LOG(3, NULL);

	if (tx.stage != TX_STAGE_WORK) {
		ERR("invalid tx stage");
		return EINVAL;
	}

	struct lane_tx_runtime *lane =
		(struct lane_tx_runtime *)tx.section->runtime;

	if (oid.pool_uuid_lo != lane->pop->uuid_lo) {
		ERR("invalid pool uuid");
		pmemobj_tx_abort(EINVAL);

		return EINVAL;
	}
	ASSERT(OBJ_OID_IS_VALID(lane->pop, oid));

	struct oob_header *oobh = OOB_HEADER_FROM_OID(lane->pop, oid);

	struct tx_add_range_args args = {
		.pop = lane->pop,
		.offset = oid.off + hoff,
		.size = size
	};

	/*
	 * If internal type is not equal to TYPE_ALLOCATED it means
	 * the object was allocated within this transaction
	 * and there is no need to create a snapshot.
	 */
	if (oobh->data.internal_type == TYPE_ALLOCATED)
		return pmemobj_tx_add_common(&args);

	return 0;
}

/*
 * pmemobj_tx_alloc -- allocates a new object
 */
PMEMoid
pmemobj_tx_alloc(size_t size, unsigned int type_num)
{
	LOG(3, NULL);

	if (size == 0) {
		ERR("allocation with size 0");
		errno = EINVAL;
		pmemobj_tx_abort(EINVAL);
		return OID_NULL;
	}

	if (type_num >= PMEMOBJ_NUM_OID_TYPES) {
		ERR("invalid type_num %d", type_num);
		errno = EINVAL;
		pmemobj_tx_abort(EINVAL);
		return OID_NULL;
	}

	return tx_alloc_common(size, (type_num_t)type_num,
			constructor_tx_alloc);
}

/*
 * pmemobj_tx_zalloc -- allocates a new zeroed object
 */
PMEMoid
pmemobj_tx_zalloc(size_t size, unsigned int type_num)
{
	LOG(3, NULL);

	if (size == 0) {
		ERR("allocation with size 0");
		errno = EINVAL;
		pmemobj_tx_abort(EINVAL);
		return OID_NULL;
	}

	if (type_num >= PMEMOBJ_NUM_OID_TYPES) {
		ERR("invalid type_num %d", type_num);
		errno = EINVAL;
		pmemobj_tx_abort(EINVAL);
		return OID_NULL;
	}

	return tx_alloc_common(size, (type_num_t)type_num,
			constructor_tx_zalloc);
}

/*
 * pmemobj_tx_realloc -- resizes an existing object
 */
PMEMoid
pmemobj_tx_realloc(PMEMoid oid, size_t size, unsigned int type_num)
{
	LOG(3, NULL);

	return tx_realloc_common(oid, size, type_num,
			constructor_tx_alloc, constructor_tx_copy);
}


/*
 * pmemobj_zrealloc -- resizes an existing object, any new space is zeroed.
 */
PMEMoid
pmemobj_tx_zrealloc(PMEMoid oid, size_t size, unsigned int type_num)
{
	LOG(3, NULL);

	return tx_realloc_common(oid, size, type_num,
			constructor_tx_zalloc, constructor_tx_copy_zero);
}

/*
 * pmemobj_tx_strdup -- allocates a new object with duplicate of the string s.
 */
PMEMoid
pmemobj_tx_strdup(const char *s, unsigned int type_num)
{
	LOG(3, NULL);

	if (tx.stage != TX_STAGE_WORK) {
		ERR("invalid tx stage");
		errno = EINVAL;
		return OID_NULL;
	}

	if (NULL == s) {
		ERR("cannot duplicate NULL string");
		errno = EINVAL;
		pmemobj_tx_abort(EINVAL);
		return OID_NULL;
	}

	if (type_num >= PMEMOBJ_NUM_OID_TYPES) {
		ERR("invalid type_num %d", type_num);
		errno = EINVAL;
		pmemobj_tx_abort(EINVAL);
		return OID_NULL;
	}

	size_t len = strlen(s);

	if (len == 0)
		return tx_alloc_common(sizeof (char), (type_num_t)type_num,
				constructor_tx_zalloc);

	size_t size = (len + 1) * sizeof (char);

	return tx_alloc_copy_common(size, (type_num_t)type_num, s, size,
			constructor_tx_copy);
}

/*
 * pmemobj_tx_free -- frees an existing object
 */
int
pmemobj_tx_free(PMEMoid oid)
{
	LOG(3, NULL);

	if (tx.stage != TX_STAGE_WORK) {
		ERR("invalid tx stage");
		errno = EINVAL;
		return EINVAL;
	}

	if (OBJ_OID_IS_NULL(oid))
		return 0;

	struct lane_tx_runtime *lane =
		(struct lane_tx_runtime *)tx.section->runtime;

	if (lane->pop->uuid_lo != oid.pool_uuid_lo) {
		ERR("invalid pool uuid");
		errno = EINVAL;
		pmemobj_tx_abort(EINVAL);
		return EINVAL;
	}
	ASSERT(OBJ_OID_IS_VALID(lane->pop, oid));

	struct lane_tx_layout *layout =
		(struct lane_tx_layout *)tx.section->layout;

	struct oob_header *oobh = OOB_HEADER_FROM_OID(lane->pop, oid);
	ASSERT(oobh->data.user_type < PMEMOBJ_NUM_OID_TYPES);

	if (oobh->data.internal_type == TYPE_ALLOCATED) {
		/* the object is in object store */
		struct object_store_item *obj_list =
			&lane->pop->store->bytype[oobh->data.user_type];

		return list_move_oob(lane->pop, &obj_list->head,
				&layout->undo_free, oid);
	} else {
		ASSERTeq(oobh->data.internal_type, TYPE_NONE);
#ifdef USE_VG_PMEMCHECK
		size_t size = pmalloc_usable_size(lane->pop,
				oid.off - OBJ_OOB_SIZE);

		VALGRIND_SET_CLEAN(oobh, size);
#endif
		VALGRIND_REMOVE_FROM_TX(oobh, pmalloc_usable_size(lane->pop,
				oid.off - OBJ_OOB_SIZE));

		if (ctree_remove(lane->ranges, oid.off, 1) != oid.off) {
			ERR("TX undo state mismatch");
			ASSERT(0);
		}

		/*
		 * The object has been allocated within the same transaction
		 * so we can just remove and free the object from undo log.
		 */
		return list_remove_free(lane->pop, &layout->undo_alloc,
				0, NULL, &oid);
	}
}

/*
 * lane_transaction_construct -- create transaction lane section
 */
static int
lane_transaction_construct(PMEMobjpool *pop, struct lane_section *section)
{
	section->runtime = Malloc(sizeof (struct lane_tx_runtime));
	if (section->runtime == NULL)
		return ENOMEM;
	memset(section->runtime, 0, sizeof (struct lane_tx_runtime));

	return 0;
}

/*
 * lane_transaction_destruct -- destroy transaction lane section
 */
static int
lane_transaction_destruct(PMEMobjpool *pop, struct lane_section *section)
{
	Free(section->runtime);

	return 0;
}

#ifdef USE_VG_MEMCHECK
/*
 * tx_abort_register_valgrind -- tells Valgrind about objects from specified
 *				 undo list
 */
static void
tx_abort_register_valgrind(PMEMobjpool *pop, struct list_head *head)
{
	PMEMoid iter = head->pe_first;

	while (!OBJ_OID_IS_NULL(iter)) {
		/*
		 * Can't use pmemobj_direct and pmemobj_alloc_usable_size
		 * because pool has not been registered yet.
		 */
		void *p = (char *)pop + iter.off;
		size_t sz = pmalloc_usable_size(pop,
				iter.off - OBJ_OOB_SIZE) - OBJ_OOB_SIZE;

		VALGRIND_DO_MEMPOOL_ALLOC(pop, p, sz);
		VALGRIND_DO_MAKE_MEM_DEFINED(pop, p, sz);

		iter = oob_list_next(pop, head, iter);
	}
}
#endif

/*
 * lane_transaction_recovery -- recovery of transaction lane section
 */
static int
lane_transaction_recovery(PMEMobjpool *pop,
	struct lane_section_layout *section)
{
	struct lane_tx_layout *layout = (struct lane_tx_layout *)section;
	int ret = 0;

	if (layout->state == TX_STATE_COMMITTED) {
		/*
		 * The transaction has been committed so we have to
		 * process the undo log, do the post commit phase
		 * and clear the transaction state.
		 */
		ret = tx_post_commit(pop, layout);
		if (!ret) {
			tx_set_state(pop, layout, TX_STATE_NONE);
		} else {
			ERR("tx_post_commit failed");
		}
	} else {
#ifdef USE_VG_MEMCHECK
		if (On_valgrind) {
			tx_abort_register_valgrind(pop, &layout->undo_set);
			tx_abort_register_valgrind(pop, &layout->undo_alloc);
			tx_abort_register_valgrind(pop,
					&layout->undo_set_cache);
		}
#endif
		/* process undo log and restore all operations */
		tx_abort(pop, layout, 1 /* recovery */);
	}

	return ret;
}

/*
 * lane_transaction_check -- consistency check of transaction lane section
 */
static int
lane_transaction_check(PMEMobjpool *pop,
	struct lane_section_layout *section)
{
	LOG(3, "tx lane %p", section);

	struct lane_tx_layout *tx_sec = (struct lane_tx_layout *)section;

	if (tx_sec->state != TX_STATE_NONE &&
		tx_sec->state != TX_STATE_COMMITTED) {
		ERR("tx lane: invalid transaction state");
		return -1;
	}

	PMEMoid iter;
	/* check undo log for set operation */
	for (iter = tx_sec->undo_set.pe_first; !OBJ_OID_IS_NULL(iter);
		iter = oob_list_next(pop, &tx_sec->undo_set, iter)) {

		struct tx_range *range = OBJ_OFF_TO_PTR(pop, iter.off);

		if (!OBJ_OFF_FROM_HEAP(pop, range->offset) ||
			!OBJ_OFF_FROM_HEAP(pop, range->offset + range->size)) {
			ERR("tx_lane: invalid offset in tx range object");
			return -1;
		}
	}

	/* check undo log for allocations */
	for (iter = tx_sec->undo_alloc.pe_first; !OBJ_OID_IS_NULL(iter);
		iter = oob_list_next(pop, &tx_sec->undo_alloc, iter)) {

		struct oob_header *oobh = OOB_HEADER_FROM_OID(pop, iter);
		if (oobh->data.internal_type != TYPE_NONE) {
			ERR("tx lane: invalid internal type");
			return -1;
		}

		if (oobh->data.user_type >= PMEMOBJ_NUM_OID_TYPES) {
			ERR("tx lane: invalid user type");
			return -1;
		}
	}

	/* check undo log for free operation */
	for (iter = tx_sec->undo_free.pe_first; !OBJ_OID_IS_NULL(iter);
		iter = oob_list_next(pop, &tx_sec->undo_free, iter)) {

		struct oob_header *oobh = OOB_HEADER_FROM_OID(pop, iter);
		if (oobh->data.internal_type != TYPE_ALLOCATED) {
			ERR("tx lane: invalid internal type");
			return -1;
		}

		if (oobh->data.user_type >= PMEMOBJ_NUM_OID_TYPES) {
			ERR("tx lane: invalid user type");
			return -1;
		}
	}

	return 0;
}

/*
 * lane_transaction_init -- initializes transaction section
 */
static int
lane_transaction_boot(PMEMobjpool *pop)
{
	/* nop */
	return 0;
}

static struct section_operations transaction_ops = {
	.construct = lane_transaction_construct,
	.destruct = lane_transaction_destruct,
	.recover = lane_transaction_recovery,
	.check = lane_transaction_check,
	.boot = lane_transaction_boot
};

SECTION_PARM(LANE_SECTION_TRANSACTION, &transaction_ops);
