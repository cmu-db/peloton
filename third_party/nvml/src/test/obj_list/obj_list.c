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
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * obj_list.c -- unit tests for list module
 */

#include <stddef.h>
#include <sys/param.h>

#include "unittest.h"
#include "util.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "obj.h"
#include "pmalloc.h"

/* offset to "in band" item */
#define	OOB_OFF	 (sizeof (struct oob_header))
/* pmemobj initial heap offset */
#define	HEAP_OFFSET	8192

TOID_DECLARE(struct item, 0);
TOID_DECLARE(struct list, 1);
TOID_DECLARE(struct oob_list, 2);
TOID_DECLARE(struct oob_item, 3);

struct item {
	int id;
	POBJ_LIST_ENTRY(struct item) next;
};

struct oob_item {
	struct oob_header oob;
	struct item item;
};

struct oob_list {
	struct list_head head;
};

struct list {
	POBJ_LIST_HEAD(listhead, struct item) head;
};

enum redo_fail
{
	/* don't fail at all */
	NO_FAIL,
	/* fail after redo_log_store_last or redo_log_set_last */
	FAIL_AFTER_FINISH,
	/* fail before redo_log_store_last or redo_log_set_last */
	FAIL_BEFORE_FINISH,
	/* fail after redo_log_process */
	FAIL_AFTER_PROCESS
};

/* global handle to pmemobj pool */
PMEMobjpool *Pop;
/* pointer to heap offset */
uint64_t *Heap_offset;
/* list lane section */
struct lane_section Lane_section;
/* actual item id */
int *Id;

/* fail event */
enum redo_fail Redo_fail = NO_FAIL;

/* global "in band" lists */
TOID(struct list) List;
TOID(struct list) List_sec;

/* global "out of band" lists */
TOID(struct oob_list) List_oob;
TOID(struct oob_list) List_oob_sec;

TOID(struct oob_item) *Item;

/* usage macros */
#define	FATAL_USAGE()\
	FATAL("usage: obj_list <file> [PRnifr]")
#define	FATAL_USAGE_PRINT()\
	FATAL("usage: obj_list <file> P:<list>")
#define	FATAL_USAGE_PRINT_REVERSE()\
	FATAL("usage: obj_list <file> R:<list>")
#define	FATAL_USAGE_INSERT()\
	FATAL("usage: obj_list <file> i:<where>:<num>")
#define	FATAL_USAGE_REMOVE_FREE()\
	FATAL("usage: obj_list <file> f:<list>:<num>:<from>")
#define	FATAL_USAGE_REMOVE()\
	FATAL("usage: obj_list <file> r:<num>")
#define	FATAL_USAGE_MOVE()\
	FATAL("usage: obj_list <file> m:<num>:<where>:<num>")
#define	FATAL_USAGE_MOVE_OOB()\
	FATAL("usage: obj_list <file> o:<num>")
#define	FATAL_USAGE_REALLOC()\
	FATAL("usage: obj_list <file> s:<num>:<list>:<nlists>:<id>:<constr>")
#define	FATAL_USAGE_REALLOC_MOVE()\
	FATAL("usage: obj_list <file> "\
	"e:<num>:<from>:<before>:<num>:<to>:<size>:<id>:<constr>:<realloc>")
#define	FATAL_USAGE_FAIL()\
	FATAL("usage: obj_list <file> "\
	"F:<after_finish|before_finish|after_process>")

/*
 * pmem_drain_nop -- no operation for drain on non-pmem memory
 */
static void
pmem_drain_nop(void)
{
	/* nop */
}

/*
 * obj_persist -- pmemobj version of pmem_persist w/o replication
 */
static void
obj_persist(PMEMobjpool *pop, void *addr, size_t len)
{
	pop->persist_local(addr, len);
}

/*
 * obj_flush -- pmemobj version of pmem_flush w/o replication
 */
static void
obj_flush(PMEMobjpool *pop, void *addr, size_t len)
{
	pop->flush_local(addr, len);
}

/*
 * obj_drain -- pmemobj version of pmem_drain w/o replication
 */
static void
obj_drain(PMEMobjpool *pop)
{
	pop->drain_local();
}

/*
 * linear_alloc -- allocates `size` bytes (rounded up to 8 bytes) and returns
 * offset to the allocated object
 */
static uint64_t
linear_alloc(uint64_t *cur_offset, size_t size)
{
	uint64_t ret = *cur_offset;
	*cur_offset += roundup(size, sizeof (uint64_t));
	return ret;
}
/*
 * pmemobj_open -- pmemobj_open mock
 *
 * This function initializes the pmemobj pool for purposes of this
 * unittest.
 */
FUNC_MOCK(pmemobj_open, PMEMobjpool *, char *fname, char *layout)
FUNC_MOCK_RUN_DEFAULT {
	int fd = open(fname, O_RDWR);
	if (fd < 0) {
		OUT("!%s: open", fname);
		return NULL;
	}

	struct stat stbuf;
	if (fstat(fd, &stbuf) < 0) {
		OUT("!fstat");
		(void) close(fd);
		return NULL;
	}

	void *addr = pmem_map(fd);
	if (!addr) {
		OUT("!%s: pmem_map", fname);
		(void) close(fd);
		return NULL;
	}

	close(fd);
	Pop = (PMEMobjpool *)addr;
	Pop->addr = Pop;
	Pop->size = stbuf.st_size;
	Pop->is_pmem = pmem_is_pmem(addr, stbuf.st_size);
	Pop->rdonly = 0;
	Pop->uuid_lo = 0x12345678;

	if (Pop->is_pmem) {
		Pop->persist_local = pmem_persist;
		Pop->flush_local = pmem_flush;
		Pop->drain_local = pmem_drain;
	} else {
		Pop->persist_local = (persist_local_fn)pmem_msync;
		Pop->flush_local = (persist_local_fn)pmem_msync;
		Pop->drain_local = pmem_drain_nop;
	}

	Pop->persist = obj_persist;
	Pop->flush = obj_flush;
	Pop->drain = obj_drain;

	Pop->heap_offset = HEAP_OFFSET;
	Pop->heap_size = Pop->size - Pop->heap_offset;
	uint64_t heap_offset = HEAP_OFFSET;

	Heap_offset = (uint64_t *)((uintptr_t)Pop +
			linear_alloc(&heap_offset, sizeof (*Heap_offset)));

	Id = (int *)((uintptr_t)Pop + linear_alloc(&heap_offset, sizeof (*Id)));

	/* Alloc lane layout */
	Lane_section.layout = (void *)((uintptr_t)Pop +
			linear_alloc(&heap_offset, LANE_SECTION_LEN));

	/* Alloc in band lists */
	List.oid.pool_uuid_lo = Pop->uuid_lo;
	List.oid.off = linear_alloc(&heap_offset, sizeof (struct list));

	List_sec.oid.pool_uuid_lo = Pop->uuid_lo;
	List_sec.oid.off = linear_alloc(&heap_offset, sizeof (struct list));

	/* Alloc out of band lists */
	List_oob.oid.pool_uuid_lo = Pop->uuid_lo;
	List_oob.oid.off = linear_alloc(&heap_offset, sizeof (struct oob_list));

	List_oob_sec.oid.pool_uuid_lo = Pop->uuid_lo;
	List_oob_sec.oid.off =
			linear_alloc(&heap_offset, sizeof (struct oob_list));

	Item = (void *)((uintptr_t)Pop +
			linear_alloc(&heap_offset, sizeof (*Item)));
	Item->oid.pool_uuid_lo = Pop->uuid_lo;
	Item->oid.off = linear_alloc(&heap_offset, sizeof (struct oob_item));
	Pop->persist(Pop, Item, sizeof (*Item));

	if (*Heap_offset == 0) {
		*Heap_offset = heap_offset;
		Pop->persist(Pop, Heap_offset, sizeof (*Heap_offset));
	}

	Pop->persist(Pop, Pop, HEAP_OFFSET);
	return Pop;
}
FUNC_MOCK_END

/*
 * pmemobj_close -- pmemobj_close mock
 *
 * Just unmap the mapped area.
 */
FUNC_MOCK(pmemobj_close, void, PMEMobjpool *pop)
	_pobj_cached_pool.pop = NULL;
	_pobj_cached_pool.uuid_lo = 0;
	Pop = NULL;
	munmap(Pop, Pop->size);
FUNC_MOCK_END

int _pobj_cache_invalidate;
__thread struct _pobj_pcache _pobj_cached_pool;

FUNC_MOCK_RET_ALWAYS(pmemobj_pool_by_oid, PMEMobjpool *, Pop, PMEMoid oid);

/*
 * lane_hold -- lane_hold mock
 *
 * Returns pointer to list lane section. For other types returns error.
 */
FUNC_MOCK(lane_hold, int, PMEMobjpool *pop, struct lane_section **section,
		enum lane_section_type type)
	FUNC_MOCK_RUN_DEFAULT {
		int ret = 0;
		if (type != LANE_SECTION_LIST) {
			ret = -1;
			*section = NULL;
		} else {
			ret = 0;
			*section = &Lane_section;
		}
		return ret;
	}
FUNC_MOCK_END

/*
 * lane_release -- lane_release mock
 *
 * Always returns success.
 */
FUNC_MOCK_RET_ALWAYS(lane_release, int, 0, PMEMobjpool *pop);

/*
 * heap_boot -- heap_boot mock
 *
 * Always returns success.
 */
FUNC_MOCK_RET_ALWAYS(heap_boot, int, 0, PMEMobjpool *pop);

/*
 * pmemobj_alloc -- pmemobj_alloc mock
 *
 * Allocates an object using pmalloc and return PMEMoid.
 */
FUNC_MOCK(pmemobj_alloc, PMEMoid, PMEMobjpool *pop, PMEMoid *oidp,
		size_t size, int type_num,
		void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
		void *arg)
	FUNC_MOCK_RUN_DEFAULT {
		PMEMoid oid = {0, 0};
		oid.pool_uuid_lo = 0;
		pmalloc(NULL, &oid.off, size, OOB_OFF);
		oid.off += OOB_OFF;
		if (oidp) {
			*oidp = oid;
			Pop->persist(Pop, oidp, sizeof (*oidp));
		}
	return oid; }
FUNC_MOCK_END

/*
 * pmalloc -- pmalloc mock
 *
 * Allocates the memory using linear allocator.
 * Prints the id of allocated struct oob_item for tracking purposes.
 */
FUNC_MOCK(pmalloc, int, PMEMobjpool *pop, uint64_t *ptr, size_t size,
		uint64_t data_off)
	FUNC_MOCK_RUN_DEFAULT {
		size = 2 * (size - OOB_OFF) + OOB_OFF;
		uint64_t *alloc_size = (uint64_t *)((uintptr_t)Pop
				+ *Heap_offset);
		*alloc_size = size;
		Pop->persist(Pop, alloc_size, sizeof (*alloc_size));

		*ptr = *Heap_offset + sizeof (uint64_t);
		Pop->persist(Pop, ptr, sizeof (*ptr));

		struct oob_item *item =
			(struct oob_item *)((uintptr_t)Pop + *ptr);

		item->item.id = *Id;
		Pop->persist(Pop, &item->item.id, sizeof (item->item.id));

		(*Id)++;
		Pop->persist(Pop, Id, sizeof (*Id));

		*Heap_offset = *Heap_offset + sizeof (uint64_t) + size;
		Pop->persist(Pop, Heap_offset, sizeof (*Heap_offset));

		OUT("pmalloc(id = %d)", item->item.id);
		return 0;
	}
FUNC_MOCK_END

/*
 * pfree -- pfree mock
 *
 * Just prints freeing struct oob_item id. Doesn't free the memory.
 */
FUNC_MOCK(pfree, int, PMEMobjpool *pop, uint64_t *ptr, uint64_t data_off)
	FUNC_MOCK_RUN_DEFAULT {
		struct oob_item *item =
			(struct oob_item *)((uintptr_t)Pop + *ptr);
		OUT("pfree(id = %d)", item->item.id);
		*ptr = 0;
		Pop->persist(Pop, ptr, sizeof (*ptr));

		return 0;
	}
FUNC_MOCK_END

/*
 * pmalloc_construct -- pmalloc_construct mock
 *
 * Allocates the memory using linear allocator and invokes the constructor.
 * Prints the id of allocated struct oob_item for tracking purposes.
 */
FUNC_MOCK(pmalloc_construct, int, PMEMobjpool *pop, uint64_t *off,
	size_t size, void (*constructor)(PMEMobjpool *pop, void *ptr,
	void *arg), void *arg, uint64_t data_off)
	FUNC_MOCK_RUN_DEFAULT {
		size = 2 * (size - OOB_OFF) + OOB_OFF;
		uint64_t *alloc_size = (uint64_t *)((uintptr_t)Pop +
				*Heap_offset);
		*alloc_size = size;
		Pop->persist(Pop, alloc_size, sizeof (*alloc_size));

		*off = *Heap_offset + sizeof (uint64_t);
		Pop->persist(Pop, off, sizeof (*off));

		*Heap_offset = *Heap_offset + sizeof (uint64_t) + size;
		Pop->persist(Pop, Heap_offset, sizeof (*Heap_offset));

		void *ptr = (void *)((uintptr_t)Pop + *off + data_off);
		constructor(pop, ptr, arg);

		return 0;
	}
FUNC_MOCK_END

/*
 * prealloc -- prealloc mock
 */
FUNC_MOCK(prealloc, int, PMEMobjpool *pop, uint64_t *off, size_t size,
		uint64_t data_off)
	FUNC_MOCK_RUN_DEFAULT {
		uint64_t *alloc_size = (uint64_t *)((uintptr_t)Pop +
				*off - sizeof (uint64_t));
		struct item *item = (struct item *)((uintptr_t)Pop +
				*off + OOB_OFF);
		if (*alloc_size >= size) {
			*alloc_size = size;
			Pop->persist(Pop, alloc_size, sizeof (*alloc_size));

			OUT("prealloc(id = %d, size = %zu) = true",
				item->id,
				(size - OOB_OFF) / sizeof (struct item));
			return 0;
		} else {
			OUT("prealloc(id = %d, size = %zu) = false",
				item->id,
				(size - OOB_OFF) / sizeof (struct item));
			return -1;
		}
	}
FUNC_MOCK_END

/*
 * prealloc_construct -- prealloc_construct mock
 */
FUNC_MOCK(prealloc_construct, int, PMEMobjpool *pop, uint64_t *off,
	size_t size, void (*constructor)(PMEMobjpool *pop, void *ptr,
	void *arg), void *arg, uint64_t data_off)
	FUNC_MOCK_RUN_DEFAULT {
		int ret = prealloc(pop, off, size, data_off);
		if (!ret) {
			void *ptr = (void *)((uintptr_t)Pop + *off + data_off);
			constructor(pop, ptr, arg);
		}
		return ret;
	}
FUNC_MOCK_END

/*
 * pmalloc_usable_size -- pmalloc_usable_size mock
 */
FUNC_MOCK(pmalloc_usable_size, size_t, PMEMobjpool *pop, uint64_t off)
	FUNC_MOCK_RUN_DEFAULT {
		uint64_t *alloc_size = (uint64_t *)((uintptr_t)Pop +
				off - sizeof (uint64_t));
		return (size_t)*alloc_size;
	}
FUNC_MOCK_END

FUNC_MOCK(pmemobj_alloc_usable_size, size_t, PMEMoid oid)
	FUNC_MOCK_RUN_DEFAULT {
		size_t size = pmalloc_usable_size(Pop, oid.off - OOB_OFF);
		return size - OOB_OFF;
	}
FUNC_MOCK_END

/*
 * pmemobj_mutex_lock -- pmemobj_mutex_lock mock
 */
FUNC_MOCK_RET_ALWAYS(pmemobj_mutex_lock, int, 0, PMEMobjpool *pop,
		PMEMmutex *mutexp);

/*
 * pmemobj_mutex_unlock -- pmemobj_mutex_unlock mock
 */
FUNC_MOCK_RET_ALWAYS(pmemobj_mutex_unlock, int, 0, PMEMobjpool *pop,
		PMEMmutex *mutexp);

/*
 * lane_recover_and_section_boot -- lane_recover_and_section_boot mock
 */
FUNC_MOCK(lane_recover_and_section_boot, int, PMEMobjpool *pop)
	FUNC_MOCK_RUN_DEFAULT {
		return Section_ops[LANE_SECTION_LIST]->recover(Pop,
				Lane_section.layout);
	}
FUNC_MOCK_END

/*
 * redo_log_store_last -- redo_log_store_last mock
 */
FUNC_MOCK(redo_log_store_last, void, PMEMobjpool *pop,
		struct redo_log *redo, size_t index,
		uint64_t offset, uint64_t value)
	FUNC_MOCK_RUN_DEFAULT {
		switch (Redo_fail) {
		case FAIL_AFTER_FINISH:
			_FUNC_REAL(redo_log_store_last)(pop,
					redo, index, offset, value);
			DONE(NULL);
			break;
		case FAIL_BEFORE_FINISH:
			DONE(NULL);
			break;
		default:
			_FUNC_REAL(redo_log_store_last)(pop,
					redo, index, offset, value);
			break;
		}

	}
FUNC_MOCK_END

/*
 * redo_log_set_last -- redo_log_set_last mock
 */
FUNC_MOCK(redo_log_set_last, void, PMEMobjpool *pop,
		struct redo_log *redo, size_t index)
	FUNC_MOCK_RUN_DEFAULT {
		switch (Redo_fail) {
		case FAIL_AFTER_FINISH:
			_FUNC_REAL(redo_log_set_last)(pop, redo, index);
			DONE(NULL);
			break;
		case FAIL_BEFORE_FINISH:
			DONE(NULL);
			break;
		default:
			_FUNC_REAL(redo_log_set_last)(pop, redo, index);
			break;
		}

	}
FUNC_MOCK_END

FUNC_MOCK(redo_log_process, void, PMEMobjpool *pop,
		struct redo_log *redo, size_t nentries)
		FUNC_MOCK_RUN_DEFAULT {
			_FUNC_REAL(redo_log_process)(pop, redo, nentries);
			if (Redo_fail == FAIL_AFTER_PROCESS) {
				DONE(NULL);
			}
		}
FUNC_MOCK_END

/*
 * oob_get_first -- get first element from oob list
 */
static PMEMoid
oob_get_first(PMEMoid head)
{
	struct list_head *lhead = (struct list_head *)pmemobj_direct(head);

	if (lhead->pe_first.off) {
		PMEMoid ret = lhead->pe_first;
		ret.off -= OOB_OFF;
		return ret;
	}

	return OID_NULL;
}

/*
 * oob_get_prev -- get previous of element from oob list
 */
static PMEMoid
oob_get_prev(PMEMoid oid)
{
	struct oob_header *oobh = (struct oob_header *)pmemobj_direct(oid);

	if (oobh->oob.pe_prev.off) {
		PMEMoid ret = oobh->oob.pe_prev;
		ret.off -= OOB_OFF;
		return ret;
	}

	return OID_NULL;
}

/*
 * oob_get_next -- get next of element from oob list
 */
static PMEMoid
oob_get_next(PMEMoid oid)
{
	struct oob_header *oobh = (struct oob_header *)pmemobj_direct(oid);

	if (oobh->oob.pe_next.off) {
		PMEMoid ret = oobh->oob.pe_next;
		ret.off -= OOB_OFF;
		return ret;
	}

	return OID_NULL;
}

/*
 * for each element on list in normal order
 */
#define	LIST_FOREACH(item, list, head, field)\
for ((item) = \
	D_RW((list))->head.pe_first;\
	!TOID_IS_NULL((item));\
	TOID_ASSIGN((item),\
	TOID_EQUALS((item),\
	D_RW(D_RW((list))->head.pe_first)->field.pe_prev) ?\
	OID_NULL : \
	D_RW(item)->field.pe_next.oid))

/*
 * for each element on list in reverse order
 */
#define	LIST_FOREACH_REVERSE(item, list, head, field)\
for ((item) = \
	TOID_IS_NULL(D_RW((list))->head.pe_first) ? D_RW(list)->head.pe_first :\
	D_RW(D_RW(list)->head.pe_first)->field.pe_prev;\
	!TOID_IS_NULL((item));\
	TOID_ASSIGN((item),\
	TOID_EQUALS((item),\
	D_RW((list))->head.pe_first) ?\
	OID_NULL :\
	D_RW(item)->field.pe_prev.oid))

/*
 * for each element on oob list in normal order
 */
#define	LIST_FOREACH_OOB(item, list, head)\
for (TOID_ASSIGN((item), oob_get_first((list).oid));\
	!TOID_IS_NULL((item));\
	TOID_ASSIGN((item),\
	((item).oid.off ==\
	oob_get_prev(oob_get_first((list).oid)).off)\
	? OID_NULL :\
	oob_get_next((item).oid)))

/*
 * for each element on oob list in reverse order
 */
#define	LIST_FOREACH_REVERSE_OOB(item, list, head)\
for (TOID_ASSIGN((item),\
	(oob_get_first((list).oid).off ?\
	oob_get_prev(oob_get_first((list).oid)):\
	OID_NULL));\
	!TOID_IS_NULL((item));\
	TOID_ASSIGN((item),\
	((item).oid.off ==\
	oob_get_first((list).oid).off) ? OID_NULL :\
	oob_get_prev((item).oid)))

/*
 * get_item_list -- get nth item from list
 */
static PMEMoid
get_item_list(PMEMoid head, int n)
{
	TOID(struct list) list;
	TOID_ASSIGN(list, head);
	TOID(struct item) item;
	if (n >= 0) {
		LIST_FOREACH(item, list, head, next) {
			if (n == 0)
				return item.oid;
			n--;
		}
	} else {
		LIST_FOREACH_REVERSE(item, list, head, next) {
			n++;
			if (n == 0)
				return item.oid;
		}
	}

	return OID_NULL;
}
/*
 * get_item_oob_list -- get nth item from oob list
 */
static PMEMoid
get_item_oob_list(PMEMoid head, int n)
{
	TOID(struct oob_list) list;
	TOID_ASSIGN(list, head);
	TOID(struct oob_item) item;
	if (n >= 0) {
		LIST_FOREACH_OOB(item, list, head) {
			if (n == 0) {
				item.oid.off += OOB_OFF;
				return item.oid;
			}
			n--;
		}
	} else {
		LIST_FOREACH_REVERSE_OOB(item, list, head) {
			n++;
			if (n == 0) {
				item.oid.off += OOB_OFF;
				return item.oid;
			}
		}
	}

	return OID_NULL;
}

/*
 * do_print -- print list elements in normal order
 */
static void
do_print(PMEMobjpool *pop, const char *arg)
{
	int L;	/* which list */
	if (sscanf(arg, "P:%d", &L) != 1)
		FATAL_USAGE_PRINT();

	if (L == 1) {
		TOID(struct oob_item) oob_item;
		OUT("oob list:");
		LIST_FOREACH_OOB(oob_item, List_oob, head) {
			OUT("id = %d", D_RO(oob_item)->item.id);
		}
	} else if (L == 2) {
		TOID(struct item) item;
		OUT("list:");
		LIST_FOREACH(item, List, head, next) {
			OUT("id = %d", D_RO(item)->id);
		}
	} else if (L == 3) {
		TOID(struct oob_item) oob_item;
		OUT("oob list sec:");
		LIST_FOREACH_OOB(oob_item, List_oob_sec, head) {
			OUT("id = %d", D_RO(oob_item)->item.id);
		}
	} else if (L == 4) {
		TOID(struct item) item;
		OUT("list sec:");
		LIST_FOREACH(item, List_sec, head, next) {
			OUT("id = %d", D_RO(item)->id);
		}
	} else {
		FATAL_USAGE_PRINT();
	}
}

/*
 * do_print_reverse -- print list elements in reverse order
 */
static void
do_print_reverse(PMEMobjpool *pop, const char *arg)
{
	int L;	/* which list */
	if (sscanf(arg, "R:%d", &L) != 1)
		FATAL_USAGE_PRINT_REVERSE();
	if (L == 1) {
		TOID(struct oob_item) oob_item;
		OUT("oob list reverse:");
		LIST_FOREACH_REVERSE_OOB(oob_item,
			List_oob, head) {
			OUT("id = %d", D_RO(oob_item)->item.id);
		}
	} else if (L == 2) {
		TOID(struct item) item;
		OUT("list reverse:");
		LIST_FOREACH_REVERSE(item, List, head, next) {
			OUT("id = %d", D_RO(item)->id);
		}
	} else if (L == 3) {
		TOID(struct oob_item) oob_item;
		OUT("oob list sec reverse:");
		LIST_FOREACH_REVERSE_OOB(oob_item,
			List_oob_sec, head) {
			OUT("id = %d", D_RO(oob_item)->item.id);
		}
	} else if (L == 4) {
		TOID(struct item) item;
		OUT("list sec reverse:");
		LIST_FOREACH_REVERSE(item, List_sec, head, next) {
			OUT("id = %d", D_RO(item)->id);
		}
	} else {
		FATAL_USAGE_PRINT_REVERSE();
	}
}

/*
 * item_constructor -- constructor which sets the item's id to
 * new value
 */
static void
item_constructor(PMEMobjpool *pop, void *ptr, void *arg)
{
	int id = *(int *)arg;
	struct item *item = (struct item *)ptr;
	item->id = id;
	pop->persist(Pop, &item->id, sizeof (item->id));
	OUT("constructor(id = %d)", id);
}

struct realloc_arg {
	void *ptr;
	size_t new_size;
	size_t old_size;
};

/*
 * realloc_constructor -- constructor which prints only the item's
 * id and argument value
 */
static void
realloc_constructor(PMEMobjpool *pop, void *ptr, void *arg)
{
	struct realloc_arg *rarg = arg;
	struct item *item = (struct item *)ptr;
	if (ptr != rarg->ptr) {
		size_t cpy_size = rarg->old_size < rarg->new_size ?
			rarg->old_size : rarg->new_size;
		memcpy(ptr, rarg->ptr, cpy_size);
		pop->persist(pop, ptr, cpy_size);
	}
	OUT("realloc_constructor(id = %d)", item->id);
}

/*
 * do_insert_new -- insert new element to list
 */
static void
do_insert_new(PMEMobjpool *pop, const char *arg)
{
	int n;		/* which element on List */
	int before;
	int id;
	int ret = sscanf(arg, "n:%d:%d:%d", &before, &n, &id);
	if (ret == 3) {
		ret = list_insert_new(pop,
			(struct list_head *)&D_RW(List_oob)->head,
			offsetof(struct item, next),
			(struct list_head *)&D_RW(List)->head,
			get_item_list(List.oid, n),
			before,
			sizeof (struct item),
			item_constructor,
			&id, (PMEMoid *)Item);

		if (ret)
			FATAL("list_insert_new(List, List_oob) failed");
	} else if (ret == 2) {
		ret = list_insert_new(pop,
			(struct list_head *)&D_RW(List_oob)->head,
			offsetof(struct item, next),
			(struct list_head *)&D_RW(List)->head,
			get_item_list(List.oid, n),
			before,
			sizeof (struct item),
			NULL, NULL, (PMEMoid *)Item);

		if (ret)
			FATAL("list_insert_new(List, List_oob) failed");
	} else {
		ret = list_insert_new(pop,
			(struct list_head *)&D_RW(List_oob)->head,
			0, NULL, OID_NULL, 0,
			sizeof (struct item),
			NULL, NULL, (PMEMoid *)Item);

		if (ret)
			FATAL("list_insert_new(List_oob) failed");
	}
}


/*
 * do_insert -- insert element to list
 */
static void
do_insert(PMEMobjpool *pop, const char *arg)
{
	int before;
	int n;	/* which element */
	if (sscanf(arg, "i:%d:%d",
			&before, &n) != 2)
		FATAL_USAGE_INSERT();

	pmemobj_alloc(pop, (PMEMoid *)Item,
			sizeof (struct oob_item), 0, NULL, NULL);

	if (list_insert(pop,
		offsetof(struct item, next),
		(struct list_head *)&D_RW(List)->head,
		get_item_list(List.oid, n),
		before,
		Item->oid)) {
		FATAL("list_insert(List) failed");
	}
}

/*
 * do_remove_free -- remove and free element from list
 */
static void
do_remove_free(PMEMobjpool *pop, const char *arg)
{
	int L;	/* which list */
	int n;	/* which element */
	int N;	/* remove from single/both lists */
	if (sscanf(arg, "f:%d:%d:%d", &L, &n, &N) != 3)
		FATAL_USAGE_REMOVE_FREE();

	PMEMoid oid;
	if (L == 1) {
		oid = get_item_oob_list(List_oob.oid, n);
	} else if (L == 2) {
		oid = get_item_list(List.oid, n);
	} else {
		FATAL_USAGE_REMOVE_FREE();
	}

	if (N == 1) {
		if (list_remove_free(pop,
			(struct list_head *)&D_RW(List_oob)->head,
			0,
			NULL,
			&oid)) {
			FATAL("list_remove_free(List_oob) failed");
		}
	} else if (N == 2) {
		if (list_remove_free(pop,
			(struct list_head *)&D_RW(List_oob)->head,
			offsetof(struct item, next),
			(struct list_head *)&D_RW(List)->head,
			&oid)) {
			FATAL("list_remove_free(List_oob, List) failed");
		}
	} else {
		FATAL_USAGE_REMOVE_FREE();
	}
}

/*
 * do_remove -- remove element from list
 */
static void
do_remove(PMEMobjpool *pop, const char *arg)
{
	int n;	/* which element */
	if (sscanf(arg, "r:%d", &n) != 1)
		FATAL_USAGE_REMOVE();

	if (list_remove(pop,
		offsetof(struct item, next),
		(struct list_head *)&D_RW(List)->head,
		get_item_list(List.oid, n))) {
		FATAL("list_remove(List) failed");
	}
}

/*
 * do_move_oob -- move element from one list to another
 */
static void
do_move_oob(PMEMobjpool *pop, const char *arg)
{
	int n;
	if (sscanf(arg, "o:%d", &n) != 1)
		FATAL_USAGE_MOVE_OOB();

	if (list_move_oob(pop,
		(struct list_head *)&D_RW(List_oob)->head,
		(struct list_head *)&D_RW(List_oob_sec)->head,
		get_item_oob_list(List_oob.oid, n))) {
		FATAL("list_move_oob(List_oob, List_oob_sec) failed");
	}
}

/*
 * do_move -- move element from one list to another
 */
static void
do_move(PMEMobjpool *pop, const char *arg)
{
	int n;
	int d;
	int before;
	if (sscanf(arg, "m:%d:%d:%d", &n, &before, &d) != 3)
		FATAL_USAGE_MOVE();

	if (list_move(pop,
		offsetof(struct item, next),
		(struct list_head *)&D_RW(List)->head,
		offsetof(struct item, next),
		(struct list_head *)&D_RW(List_sec)->head,
		get_item_list(List_sec.oid, d),
		before,
		get_item_list(List.oid, n))) {
		FATAL("list_move(List, List_sec) failed");
	}
}

/*
 * do_realloc -- reallocate element on list
 */
static void
do_realloc(PMEMobjpool *pop, const char *arg)
{
	int n;
	int L;
	int N;
	int s;
	int id;
	if (sscanf(arg, "s:%d:%d:%d:%d:%d", &n, &L, &N, &s, &id) != 5)
		FATAL_USAGE_REALLOC();

	if (L == 1) {
		Item->oid = get_item_oob_list(List_oob.oid, n);
	} else if (L == 2) {
		Item->oid = get_item_list(List.oid, n);
	} else {
		FATAL_USAGE_REALLOC();
	}
	Pop->persist(Pop, Item, sizeof (*Item));

	size_t size = s * sizeof (struct item);

	struct realloc_arg rarg = {
		.ptr = OBJ_OFF_TO_PTR(pop, Item->oid.off),
		.old_size = pmemobj_alloc_usable_size(Item->oid),
		.new_size = size,
	};

	if (N == 1) {
		if (list_realloc(pop,
			(struct list_head *)&D_RW(List_oob)->head,
			0,
			NULL,
			size,
			realloc_constructor,
			&rarg,
			Item->oid.off + offsetof(struct item, id),
			id,
			(PMEMoid *)Item)) {
			FATAL("list_realloc(List) failed");
		}
	} else if (N == 2) {
		if (list_realloc(pop,
			(struct list_head *)&D_RW(List_oob)->head,
			offsetof(struct item, next),
			(struct list_head *)&D_RW(List)->head,
			size,
			realloc_constructor,
			&rarg,
			Item->oid.off + offsetof(struct item, id),
			id,
			(PMEMoid *)Item)) {
			FATAL("list_realloc(List, List_oob) failed");
		}
	} else {
		FATAL_USAGE_REALLOC();
	}
	TOID(struct item) item;
	TOID_ASSIGN(item, Item->oid);
	OUT("realloc(id = %d)", D_RO(item)->id);
}

/*
 * do_realloc_move -- reallocate and move object
 */
static void
do_realloc_move(PMEMobjpool *pop, const char *arg)
{
	int n;
	int s;
	int id;
	int r;
	if (sscanf(arg, "e:%d:%d:%d:%d",
			&n, &s, &id, &r) != 4)
		FATAL_USAGE_REALLOC_MOVE();
	uint64_t pe_offset = 0;
	struct list_head *head = NULL;
	if (r) {
		pe_offset = offsetof(struct item, next);
		head = (struct list_head *)&D_RW(List)->head;
	}
	Item->oid = get_item_oob_list(List_oob.oid, n);
	Pop->persist(Pop, Item, sizeof (*Item));
	size_t size = s * sizeof (struct item);
	struct realloc_arg rarg = {
		.ptr = OBJ_OFF_TO_PTR(pop, Item->oid.off),
		.old_size = pmemobj_alloc_usable_size(Item->oid),
		.new_size = size
	};
	if (list_realloc_move(pop,
		(struct list_head *)&D_RW(List_oob)->head,
		(struct list_head *)&D_RW(List_oob_sec)->head,
		pe_offset,
		head,
		size,
		realloc_constructor,
		&rarg,
		Item->oid.off + offsetof(struct item, id),
		id,
		(PMEMoid *)Item)) {
		FATAL("list_realloc_move(List_oob, List_oob_sec) failed");
	}
	TOID(struct item) item;
	TOID_ASSIGN(item, Item->oid);
	OUT("realloc_move(id = %d)", D_RO(item)->id);
}

/*
 * do_fail -- fail after specified event
 */
static void
do_fail(PMEMobjpool *pop, const char *arg)
{
	if (strcmp(arg, "F:before_finish") == 0) {
		Redo_fail = FAIL_BEFORE_FINISH;
	} else if (strcmp(arg, "F:after_finish") == 0) {
		Redo_fail = FAIL_AFTER_FINISH;
	} else if (strcmp(arg, "F:after_process") == 0) {
		Redo_fail = FAIL_AFTER_PROCESS;
	} else {
		FATAL_USAGE_FAIL();
	}
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_list");
	if (argc < 2)
		FATAL_USAGE();

	const char *path = argv[1];

	ASSERTeq(OOB_OFF, 48);
	PMEMobjpool *pop = pmemobj_open(path, NULL);
	ASSERTne(pop, NULL);

	ASSERT(!TOID_IS_NULL(List));
	ASSERT(!TOID_IS_NULL(List_oob));

	int i;
	for (i = 2; i < argc; i++) {
		switch (argv[i][0]) {
		case 'P':
			do_print(pop, argv[i]);
			break;
		case 'R':
			do_print_reverse(pop, argv[i]);
			break;
		case 'n':
			do_insert_new(pop, argv[i]);
			break;
		case 'i':
			do_insert(pop, argv[i]);
			break;
		case 'f':
			do_remove_free(pop, argv[i]);
			break;
		case 'r':
			do_remove(pop, argv[i]);
			break;
		case 'o':
			do_move_oob(pop, argv[i]);
			break;
		case 'm':
			do_move(pop, argv[i]);
			break;
		case 's':
			do_realloc(pop, argv[i]);
			break;
		case 'e':
			do_realloc_move(pop, argv[i]);
			break;
		case 'V':
			lane_recover_and_section_boot(pop);
			break;
		case 'F':
			do_fail(pop, argv[i]);
			break;
		default:
			FATAL_USAGE();
		}
	}

	pmemobj_close(pop);

	DONE(NULL);
}
