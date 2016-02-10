/*
 * Copyright (c) 2014-2015, Intel Corporation
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
 * libpmemobj.h -- definitions of libpmemobj entry points
 *
 * This library provides support for programming with persistent memory (pmem).
 *
 * libpmemobj provides a pmem-resident transactional object store.
 *
 * See libpmemobj(3) for details.
 */

#ifndef	LIBPMEMOBJ_H
#define	LIBPMEMOBJ_H 1

#ifdef __cplusplus
extern "C" {
#endif

#ifndef	__STDC_LIMIT_MACROS
#define	__STDC_LIMIT_MACROS
#endif

#include <sys/types.h>
#include <setjmp.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <pthread.h>

/*
 * opaque type internal to libpmemobj
 */
typedef struct pmemobjpool PMEMobjpool;

/*
 * Version checking...
 */

/*
 * PMEMOBJ_MAJOR_VERSION and PMEMOBJ_MINOR_VERSION provide the current version
 * of the libpmemobj API as provided by this header file.  Applications can
 * verify that the version available at run-time is compatible with the version
 * used at compile-time by passing these defines to pmemobj_check_version().
 */
#define	PMEMOBJ_MAJOR_VERSION 1
#define	PMEMOBJ_MINOR_VERSION 0
const char *pmemobj_check_version(
		unsigned major_required,
		unsigned minor_required);

#define	PMEMOBJ_MIN_POOL ((size_t)(1024 * 1024 * 8)) /* 8 MB */
#define	PMEMOBJ_MAX_ALLOC_SIZE ((size_t)0x3FFDFFFC0)
#define	PMEMOBJ_MAX_LAYOUT ((size_t)1024)
#define	PMEMOBJ_NUM_OID_TYPES ((unsigned)1024)

/*
 * Pool management...
 */
PMEMobjpool *pmemobj_open(const char *path, const char *layout);
PMEMobjpool *pmemobj_create(const char *path, const char *layout,
	size_t poolsize, mode_t mode);
void pmemobj_close(PMEMobjpool *pop);
int pmemobj_check(const char *path, const char *layout);

/*
 * Passing NULL to pmemobj_set_funcs() tells libpmemobj to continue to use the
 * default for that function.  The replacement functions must not make calls
 * back into libpmemobj.
 */
void pmemobj_set_funcs(
		void *(*malloc_func)(size_t size),
		void (*free_func)(void *ptr),
		void *(*realloc_func)(void *ptr, size_t size),
		char *(*strdup_func)(const char *s));

const char *pmemobj_errormsg(void);

/*
 * Locking...
 */
#define	_POBJ_CL_ALIGNMENT 64 /* cache line alignment for performance */

typedef union padded_pmemmutex {
	char padding[_POBJ_CL_ALIGNMENT];
	struct {
		uint64_t runid;
		pthread_mutex_t mutex;
	} pmemmutex;
} PMEMmutex;

typedef union padded_pmemrwlock {
	char padding[_POBJ_CL_ALIGNMENT];
	struct {
		uint64_t runid;
		pthread_rwlock_t rwlock;
	} pmemrwlock;
} PMEMrwlock;

typedef union padded_pmemcond {
	char padding[_POBJ_CL_ALIGNMENT];
	struct {
		uint64_t runid;
		pthread_cond_t cond;
	} pmemcond;
} PMEMcond;

void pmemobj_mutex_zero(PMEMobjpool *pop, PMEMmutex *mutexp);
int pmemobj_mutex_lock(PMEMobjpool *pop, PMEMmutex *mutexp);
int pmemobj_mutex_trylock(PMEMobjpool *pop, PMEMmutex *mutexp);
int pmemobj_mutex_unlock(PMEMobjpool *pop, PMEMmutex *mutexp);

void pmemobj_rwlock_zero(PMEMobjpool *pop, PMEMrwlock *rwlockp);
int pmemobj_rwlock_rdlock(PMEMobjpool *pop, PMEMrwlock *rwlockp);
int pmemobj_rwlock_wrlock(PMEMobjpool *pop, PMEMrwlock *rwlockp);
int pmemobj_rwlock_timedrdlock(PMEMobjpool *pop,
	PMEMrwlock *__restrict rwlockp,
	const struct timespec *__restrict abs_timeout);
int pmemobj_rwlock_timedwrlock(PMEMobjpool *pop,
	PMEMrwlock *__restrict rwlockp,
	const struct timespec *__restrict abs_timeout);
int pmemobj_rwlock_tryrdlock(PMEMobjpool *pop, PMEMrwlock *rwlockp);
int pmemobj_rwlock_trywrlock(PMEMobjpool *pop, PMEMrwlock *rwlockp);
int pmemobj_rwlock_unlock(PMEMobjpool *pop, PMEMrwlock *rwlockp);

void pmemobj_cond_zero(PMEMobjpool *pop, PMEMcond *condp);
int pmemobj_cond_broadcast(PMEMobjpool *pop, PMEMcond *condp);
int pmemobj_cond_signal(PMEMobjpool *pop, PMEMcond *condp);
int pmemobj_cond_timedwait(PMEMobjpool *pop, PMEMcond *__restrict condp,
	PMEMmutex *__restrict mutexp,
	const struct timespec *__restrict abstime);
int pmemobj_cond_wait(PMEMobjpool *pop, PMEMcond *condp,
	PMEMmutex *__restrict mutexp);

/*
 * Persistent memory object
 */

/*
 * Object handle
 */
typedef struct pmemoid {
	uint64_t pool_uuid_lo;
	uint64_t off;
} PMEMoid;

#define	OID_NULL	((PMEMoid) {0, 0})
#define	TOID_NULL(t)	((TOID(t))OID_NULL)
#define	OID_IS_NULL(o)	((o).off == 0)
#define	OID_EQUALS(lhs, rhs)\
((lhs).off == (rhs).off &&\
	(lhs).pool_uuid_lo == (rhs).pool_uuid_lo)

/*
 * Type safety macros
 */

#define	TOID_ASSIGN(o, value) (\
{\
	(o).oid = value;\
	(o);\
})
#define	TOID_EQUALS(lhs, rhs)\
((lhs).oid.off == (rhs).oid.off &&\
	(lhs).oid.pool_uuid_lo == (rhs).oid.pool_uuid_lo)

/* type number of root object */
#define	POBJ_ROOT_TYPE_NUM UINT16_MAX
#define	_toid_struct
#define	_toid_union
#define	_toid_enum
#define	_POBJ_LAYOUT_REF(name) (sizeof (_pobj_layout_##name##_ref))

/*
 * Typed OID
 */
#define	TOID(t)\
union _toid_##t##_toid

#ifdef	__cplusplus
#define	_TOID_CONSTR(t)\
_toid_##t##_toid()\
{ }\
_toid_##t##_toid(PMEMoid _oid) : oid(_oid)\
{ }
#else
#define	_TOID_CONSTR(t)
#endif

/*
 * This will cause a compile-time error for typed OID's declared
 * with a type number exceeding the limit.
 */
#define	_TOID_TYPE_NUM(i) ((i) < PMEMOBJ_NUM_OID_TYPES ? (i) : UINT64_MAX)

/*
 * Declaration of typed OID
 */
#define	_TOID_DECLARE(t, i)\
typedef uint8_t _toid_##t##_toid_type_num[(i)];\
TOID(t)\
{\
	_TOID_CONSTR(t)\
	PMEMoid oid;\
	t *_type;\
	_toid_##t##_toid_type_num *_type_num;\
}

/*
 * Declaration of typed OID of an object
 */
#define	TOID_DECLARE(t, i) _TOID_DECLARE(t, _TOID_TYPE_NUM(i))

/*
 * Declaration of typed OID of a root object
 */
#define	TOID_DECLARE_ROOT(t) _TOID_DECLARE(t, POBJ_ROOT_TYPE_NUM)

/*
 * Type number of specified type
 */
#define	TOID_TYPE_NUM(t) (sizeof (_toid_##t##_toid_type_num))

/*
 * Type number of object read from typed OID
 */
#define	TOID_TYPE_NUM_OF(o) (sizeof (*(o)._type_num))

/*
 * NULL check
 */
#define	TOID_IS_NULL(o)	((o).oid.off == 0)

/*
 * Validates whether type number stored in typed OID is the same
 * as type number stored in object's metadata
 */
#define	TOID_VALID(o) (TOID_TYPE_NUM_OF(o) == pmemobj_type_num((o).oid))

/*
 * Checks whether the object is of a given type
 */
#define	OID_INSTANCEOF(o, t) (TOID_TYPE_NUM(t) == pmemobj_type_num(o))

/*
 * Begin of layout declaration
 */
#define	POBJ_LAYOUT_BEGIN(name)\
typedef uint8_t _pobj_layout_##name##_ref[__COUNTER__]

/*
 * End of layout declaration
 */
#define	POBJ_LAYOUT_END(name)\
typedef char _pobj_layout_##name##_cnt[__COUNTER__ -\
1 - _POBJ_LAYOUT_REF(name)];

/*
 * Number of types declared inside layout without the root object
 */
#define	POBJ_LAYOUT_TYPES_NUM(name) (sizeof (_pobj_layout_##name##_cnt))

/*
 * Declaration of typed OID inside layout declaration
 */
#define	POBJ_LAYOUT_TOID(name, t)\
TOID_DECLARE(t, (__COUNTER__ - _POBJ_LAYOUT_REF(name)));

/*
 * Declaration of typed OID of root inside layout declaration
 */
#define	POBJ_LAYOUT_ROOT(name, t)\
TOID_DECLARE_ROOT(t);

/*
 * Name of declared layout
 */
#define	POBJ_LAYOUT_NAME(name) #name

PMEMobjpool *pmemobj_pool_by_ptr(const void *addr);
PMEMobjpool *pmemobj_pool_by_oid(PMEMoid oid);

extern int _pobj_cache_invalidate;
extern __thread struct _pobj_pcache {
	PMEMobjpool *pop;
	uint64_t uuid_lo;
	int invalidate;
} _pobj_cached_pool;

/*
 * Returns the direct pointer of an object.
 */
static inline void *
pmemobj_direct(PMEMoid oid)
{
	if (oid.off == 0 || oid.pool_uuid_lo == 0)
		return NULL;

	if (_pobj_cache_invalidate != _pobj_cached_pool.invalidate ||
		_pobj_cached_pool.uuid_lo != oid.pool_uuid_lo) {
		_pobj_cached_pool.invalidate = _pobj_cache_invalidate;

		if (!(_pobj_cached_pool.pop = pmemobj_pool_by_oid(oid))) {
			_pobj_cached_pool.uuid_lo = 0;
			return NULL;
		}

		_pobj_cached_pool.uuid_lo = oid.pool_uuid_lo;
	}

	return (void *)((uintptr_t)_pobj_cached_pool.pop + oid.off);
}

#define	DIRECT_RW(o) (\
{typeof (o) _o; _o._type = NULL; (void)_o;\
(typeof (*(o)._type) *)pmemobj_direct((o).oid); })
#define	DIRECT_RO(o) ((const typeof (*(o)._type) *)pmemobj_direct((o).oid))

#define	D_RW	DIRECT_RW
#define	D_RO	DIRECT_RO

/*
 * Non-transactional atomic allocations
 *
 * Those functions can be used outside transactions. The allocations are always
 * aligned to the cache-line boundary.
 */

/*
 * Allocates a new object from the pool and calls a constructor function before
 * returning. It is guaranteed that allocated object is either properly
 * initialized, or if it's interrupted before the constructor completes, the
 * memory reserved for the object is automatically reclaimed.
 */
int pmemobj_alloc(PMEMobjpool *pop, PMEMoid *oidp, size_t size,
	unsigned int type_num, void (*constructor)(PMEMobjpool *pop, void *ptr,
	void *arg), void *arg);

/*
 * Allocates a new zeroed object from the pool.
 */
int pmemobj_zalloc(PMEMobjpool *pop, PMEMoid *oidp, size_t size,
	unsigned int type_num);

/*
 * Resizes an existing object.
 */
int pmemobj_realloc(PMEMobjpool *pop, PMEMoid *oidp, size_t size,
	unsigned int type_num);

/*
 * Resizes an existing object, if extended new space is zeroed.
 */
int pmemobj_zrealloc(PMEMobjpool *pop, PMEMoid *oidp, size_t size,
	unsigned int type_num);

/*
 * Allocates a new object with duplicate of the string s.
 */
int pmemobj_strdup(PMEMobjpool *pop, PMEMoid *oidp, const char *s,
	unsigned int type_num);

/*
 * Frees an existing object.
 */
void pmemobj_free(PMEMoid *oidp);

/*
 * Returns the number of usable bytes in the object. May be greater than
 * the requested size of the object because of internal alignment.
 *
 * Can be used with objects allocated by any of the available methods.
 */
size_t pmemobj_alloc_usable_size(PMEMoid oid);

/*
 * Returns the type number of the object.
 */
int pmemobj_type_num(PMEMoid oid);

/*
 * If called for the first time on a newly created pool, the root object
 * of given size is allocated.  Otherwise, it returns the existing root object.
 * In such case, the size must be not less than the actual root object size
 * stored in the pool.  If it's larger, the root object is automatically
 * resized.
 *
 * This function is thread-safe.
 */
PMEMoid pmemobj_root(PMEMobjpool *pop, size_t size);

/*
 * Same as above, but calls the constructor function when the object is first
 * created and on all subsequent reallocations.
 */
PMEMoid pmemobj_root_construct(PMEMobjpool *pop, size_t size,
	void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg), void *arg);

/*
 * Returns the size in bytes of the root object. Always equal to the requested
 * size.
 */
size_t pmemobj_root_size(PMEMobjpool *pop);

/*
 * Pmemobj specific low-level memory manipulation functions.
 *
 * These functions are meant to be used with pmemobj pools, because they provide
 * additional functionality specific to this type of pool. These may include
 * for example replication support. They also take advantage of the knowledge
 * of the type of memory in the pool (pmem/non-pmem) to assure persistence.
 */

/*
 * Pmemobj version of memcpy. Data copied is made persistent.
 */
void *pmemobj_memcpy_persist(PMEMobjpool *pop, void *dest, const void *src,
	size_t len);

/*
 * Pmemobj version of memset. Data range set is made persistent.
 */
void *pmemobj_memset_persist(PMEMobjpool *pop, void *dest, int c, size_t len);

/*
 * Pmemobj version of pmem_persist.
 */
void pmemobj_persist(PMEMobjpool *pop, void *addr, size_t len);

/*
 * Pmemobj version of pmem_flush.
 */
void pmemobj_flush(PMEMobjpool *pop, void *addr, size_t len);

/*
 * Pmemobj version of pmem_drain.
 */
void pmemobj_drain(PMEMobjpool *pop);

/*
 * The following set of macros and functions allow access to the entire
 * collection of objects, or objects of given type.
 *
 * Use with conjunction with non-transactional allocations. Pmemobj pool acts
 * as a generic container (list) of objects that are not assigned to any
 * user-defined data structures.
 */

/*
 * Returns the first object of the specified type number.
 */
PMEMoid pmemobj_first(PMEMobjpool *pop, unsigned int type_num);

/*
 * Returns the next object (in order of allocations) of the same type.
 */
PMEMoid pmemobj_next(PMEMoid oid);

#define	POBJ_FIRST(pop, t) (\
{ TOID(t) _pobj_ret = (TOID(t))pmemobj_first((pop), TOID_TYPE_NUM(t));\
_pobj_ret; })

#define	POBJ_NEXT(o) (\
{ typeof (o) _pobj_ret = (typeof (o))pmemobj_next((o).oid);\
_pobj_ret; })

#define	POBJ_NEW(pop, o, t, constr, arg) (\
{ TOID(t) *_pobj_tmp = (o);\
PMEMoid *_pobj_oidp = _pobj_tmp ? &_pobj_tmp->oid : NULL;\
pmemobj_alloc((pop), _pobj_oidp, sizeof (t), TOID_TYPE_NUM(t), (constr),\
		(arg)); })

#define	POBJ_ALLOC(pop, o, t, size, constr, arg) (\
{ TOID(t) *_pobj_tmp = (o);\
PMEMoid *_pobj_oidp = _pobj_tmp ? &_pobj_tmp->oid : NULL;\
pmemobj_alloc((pop), _pobj_oidp, (size), TOID_TYPE_NUM(t), (constr), (arg)); })

#define	POBJ_ZNEW(pop, o, t) (\
{ TOID(t) *_pobj_tmp = (o);\
PMEMoid *_pobj_oidp = _pobj_tmp ? &_pobj_tmp->oid : NULL;\
pmemobj_zalloc((pop), _pobj_oidp, sizeof (t), TOID_TYPE_NUM(t)); })

#define	POBJ_ZALLOC(pop, o, t, size) (\
{ TOID(t) *_pobj_tmp = (o);\
PMEMoid *_pobj_oidp = _pobj_tmp ? &_pobj_tmp->oid : NULL;\
pmemobj_zalloc((pop), _pobj_oidp, (size), TOID_TYPE_NUM(t)); })

#define	POBJ_REALLOC(pop, o, t, size) (\
{ TOID(t) *_pobj_tmp = (o);\
PMEMoid *_pobj_oidp = _pobj_tmp ? &_pobj_tmp->oid : NULL;\
pmemobj_realloc((pop), _pobj_oidp, (size), TOID_TYPE_NUM(t)); })

#define	POBJ_ZREALLOC(pop, o, t, size) (\
{ TOID(t) *_pobj_tmp = (o);\
PMEMoid *_pobj_oidp = _pobj_tmp ? &_pobj_tmp->oid : NULL;\
pmemobj_zrealloc((pop), _pobj_oidp, (size), TOID_TYPE_NUM_OF(*(o))); })

#define	POBJ_FREE(o) pmemobj_free((PMEMoid *)(o))

#define	POBJ_ROOT(pop, t) (\
{ TOID(t) _pobj_ret = (TOID(t))pmemobj_root((pop), sizeof (t));\
_pobj_ret; })

/*
 * (debug helper function) logs notice message if used inside a transaction
 */
void _pobj_debug_notice(const char *func_name, const char *file, int line);

/*
 * Debug helper function and macros
 */
#ifdef	DEBUG

/*
 * (debug helper macro) logs notice message if used inside a transaction
 */
#define	_POBJ_DEBUG_NOTICE_IN_TX()\
	_pobj_debug_notice(__func__, NULL, 0)

/*
 * (debug helper macro) logs notice message if used inside a transaction
 *                      - to be used only in FOREACH macros
 */
#define	_POBJ_DEBUG_NOTICE_IN_TX_FOR(macro_name)\
	_pobj_debug_notice(macro_name, __FILE__, __LINE__),

#else
#define	_POBJ_DEBUG_NOTICE_IN_TX() do {} while (0)
#define	_POBJ_DEBUG_NOTICE_IN_TX_FOR(macro_name)
#endif /* DEBUG */

/*
 * Iterates through every existing allocated object.
 */
#define	POBJ_FOREACH(pop, varoid, vartype_num)\
for (_POBJ_DEBUG_NOTICE_IN_TX_FOR("POBJ_FOREACH")\
	vartype_num = 0; vartype_num < PMEMOBJ_NUM_OID_TYPES; ++vartype_num)\
	for (varoid = pmemobj_first(pop, vartype_num);\
		(varoid).off != 0; varoid = pmemobj_next(varoid))

/*
 * Safe variant of POBJ_FOREACH in which pmemobj_free on varoid is allowed
 */
#define	POBJ_FOREACH_SAFE(pop, varoid, nvaroid, vartype_num)\
for (_POBJ_DEBUG_NOTICE_IN_TX_FOR("POBJ_FOREACH_SAFE")\
	vartype_num = 0; vartype_num < PMEMOBJ_NUM_OID_TYPES; ++vartype_num)\
	for (varoid = pmemobj_first(pop, vartype_num);\
		(varoid).off != 0 && (nvaroid = pmemobj_next(varoid), 1);\
		varoid = nvaroid)

/*
 * Iterates through every object of the specified type.
 */
#define	POBJ_FOREACH_TYPE(pop, var)\
for (_POBJ_DEBUG_NOTICE_IN_TX_FOR("POBJ_FOREACH_TYPE")\
	var = (typeof (var))pmemobj_first((pop),\
		TOID_TYPE_NUM_OF(var));\
		TOID_IS_NULL(var) == 0;\
		var = POBJ_NEXT(var))

/*
 * Safe variant of POBJ_FOREACH_TYPE in which pmemobj_free on var
 * is allowed.
 */
#define	POBJ_FOREACH_SAFE_TYPE(pop, var, nvar)\
for (_POBJ_DEBUG_NOTICE_IN_TX_FOR("POBJ_FOREACH_SAFE_TYPE")\
	var = (typeof (var))pmemobj_first((pop),\
	TOID_TYPE_NUM_OF(var));\
	TOID_IS_NULL(var) == 0 &&\
	(nvar = POBJ_NEXT(var), 1);\
	var = nvar)

/*
 * Non-transactional persistent atomic circular doubly-linked list
 */
#define	POBJ_LIST_ENTRY(type)\
struct {\
	TOID(type) pe_next;\
	TOID(type) pe_prev;\
}

#define	POBJ_LIST_HEAD(name, type)\
struct name {\
	TOID(type) pe_first;\
	PMEMmutex lock;\
}

int pmemobj_list_insert(PMEMobjpool *pop, size_t pe_offset, void *head,
	PMEMoid dest, int before, PMEMoid oid);

PMEMoid pmemobj_list_insert_new(PMEMobjpool *pop, size_t pe_offset, void *head,
	PMEMoid dest, int before, size_t size, unsigned int type_num,
	void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg), void *arg);

int pmemobj_list_remove(PMEMobjpool *pop, size_t pe_offset, void *head,
	PMEMoid oid, int free);

int pmemobj_list_move(PMEMobjpool *pop, size_t pe_old_offset,
	void *head_old, size_t pe_new_offset, void *head_new,
	PMEMoid dest, int before, PMEMoid oid);

#define	POBJ_LIST_FIRST(head)	((head)->pe_first)
#define	POBJ_LIST_LAST(head, field) (\
TOID_IS_NULL((head)->pe_first) ?\
(head)->pe_first :\
D_RO((head)->pe_first)->field.pe_prev)

#define	POBJ_LIST_EMPTY(head)	(TOID_IS_NULL((head)->pe_first))
#define	POBJ_LIST_NEXT(elm, field)	(D_RO(elm)->field.pe_next)
#define	POBJ_LIST_PREV(elm, field)	(D_RO(elm)->field.pe_prev)
#define	POBJ_LIST_DEST_HEAD	1
#define	POBJ_LIST_DEST_TAIL	0

#define	POBJ_LIST_FOREACH(var, head, field)\
for (_POBJ_DEBUG_NOTICE_IN_TX_FOR("POBJ_LIST_FOREACH")\
	(var) =  POBJ_LIST_FIRST((head));\
	TOID_IS_NULL((var)) == 0;\
	TOID_EQUALS(POBJ_LIST_NEXT((var), field),\
	POBJ_LIST_FIRST((head))) ?\
	TOID_ASSIGN((var), OID_NULL) :\
	((var) = POBJ_LIST_NEXT((var), field)))

#define	POBJ_LIST_FOREACH_REVERSE(var, head, field)\
for (_POBJ_DEBUG_NOTICE_IN_TX_FOR("POBJ_LIST_FOREACH_REVERSE")\
	(var) = POBJ_LIST_LAST((head), field);\
	TOID_IS_NULL((var)) == 0;\
	TOID_EQUALS(POBJ_LIST_PREV((var), field),\
	POBJ_LIST_LAST((head), field)) ?\
	TOID_ASSIGN((var), OID_NULL) :\
	((var) = POBJ_LIST_PREV((var), field)))

#define	POBJ_LIST_INSERT_HEAD(pop, head, elm, field)\
pmemobj_list_insert((pop),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head), OID_NULL,\
	POBJ_LIST_DEST_HEAD, (elm).oid)

#define	POBJ_LIST_INSERT_TAIL(pop, head, elm, field)\
pmemobj_list_insert((pop),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head), OID_NULL,\
	POBJ_LIST_DEST_TAIL, (elm).oid)

#define	POBJ_LIST_INSERT_AFTER(pop, head, listelm, elm, field)\
pmemobj_list_insert((pop),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head), (listelm).oid,\
	0 /* after */, (elm).oid)

#define	POBJ_LIST_INSERT_BEFORE(pop, head, listelm, elm, field)\
pmemobj_list_insert((pop), \
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head), (listelm).oid,\
	1 /* before */, (elm).oid)

#define	POBJ_LIST_INSERT_NEW_HEAD(pop, head, field, size, constr, arg)\
pmemobj_list_insert_new((pop),\
	offsetof(typeof (*((head)->pe_first._type)), field),\
	(head), OID_NULL, POBJ_LIST_DEST_HEAD, (size),\
	TOID_TYPE_NUM_OF((head)->pe_first), (constr), (arg))

#define	POBJ_LIST_INSERT_NEW_TAIL(pop, head, field, size, constr, arg)\
pmemobj_list_insert_new((pop),\
	offsetof(typeof (*((head)->pe_first._type)), field),\
	(head), OID_NULL, POBJ_LIST_DEST_TAIL, (size),\
	TOID_TYPE_NUM_OF((head)->pe_first), (constr), (arg))

#define	POBJ_LIST_INSERT_NEW_AFTER(pop, head, listelm, field, size,\
	constr, arg)\
pmemobj_list_insert_new((pop),\
	offsetof(typeof (*((head)->pe_first._type)), field),\
	(head), (listelm).oid, 0 /* after */, (size),\
	TOID_TYPE_NUM_OF((head)->pe_first), (constr), (arg))

#define	POBJ_LIST_INSERT_NEW_BEFORE(pop, head, listelm, field, size,\
		constr, arg)\
pmemobj_list_insert_new((pop),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head), (listelm).oid, 1 /* before */, (size),\
	TOID_TYPE_NUM_OF((head)->pe_first), (constr), (arg))

#define	POBJ_LIST_REMOVE(pop, head, elm, field)\
pmemobj_list_remove((pop),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head), (elm).oid, 0 /* no free */)

#define	POBJ_LIST_REMOVE_FREE(pop, head, elm, field)\
pmemobj_list_remove((pop),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head), (elm).oid, 1 /* free */)

#define	POBJ_LIST_MOVE_ELEMENT_HEAD(pop, head, head_new, elm, field, field_new)\
pmemobj_list_move((pop),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head_new)._type)), field_new),\
	(head_new), OID_NULL, POBJ_LIST_DEST_HEAD, (elm).oid)

#define	POBJ_LIST_MOVE_ELEMENT_TAIL(pop, head, head_new, elm, field, field_new)\
pmemobj_list_move((pop),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head_new)._type)), field_new),\
	(head_new), OID_NULL, POBJ_LIST_DEST_TAIL, (elm).oid)

#define	POBJ_LIST_MOVE_ELEMENT_AFTER(pop,\
	head, head_new, listelm, elm, field, field_new)\
pmemobj_list_move((pop),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head_new)._type)), field_new),\
	(head_new),\
	(listelm).oid,\
	0 /* after */, (elm).oid)

#define	POBJ_LIST_MOVE_ELEMENT_BEFORE(pop,\
	head, head_new, listelm, elm, field, field_new)\
pmemobj_list_move((pop),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head)._type)), field),\
	(head),\
	offsetof(typeof (*(POBJ_LIST_FIRST(head_new)._type)), field_new),\
	(head_new),\
	(listelm).oid,\
	1 /* before */, (elm).oid)

/*
 * Transactions
 *
 * Stages are changed only by the pmemobj_tx_* functions, each transition
 * to the TX_STAGE_ONABORT is followed by a longjmp to the jmp_buf provided in
 * the pmemobj_tx_begin function.
 */
enum pobj_tx_stage {
	TX_STAGE_NONE,		/* no transaction in this thread */
	TX_STAGE_WORK,		/* transaction in progress */
	TX_STAGE_ONCOMMIT,	/* successfully committed */
	TX_STAGE_ONABORT,	/* tx_begin failed or transaction aborted */
	TX_STAGE_FINALLY,	/* always called */

	MAX_TX_STAGE
};

/*
 * Always returns the current transaction stage for a thread.
 */
enum pobj_tx_stage pmemobj_tx_stage(void);

enum pobj_tx_lock {
	TX_LOCK_NONE,
	TX_LOCK_MUTEX,	/* PMEMmutex */
	TX_LOCK_RWLOCK	/* PMEMrwlock */
};

/*
 * Starts a new transaction in the current thread.
 * If called within an open transaction, starts a nested transaction.
 *
 * If successful, transaction stage changes to TX_STAGE_WORK and function
 * returns zero. Otherwise, stage changes to TX_STAGE_ONABORT and an error
 * number is returned.
 */
int pmemobj_tx_begin(PMEMobjpool *pop, jmp_buf env, ...);

/*
 * Aborts current transaction
 *
 * Must be called during TX_STAGE_WORK. Otherwise, has no effect.
 *
 * Always causes transition to TX_STAGE_ONABORT.
 */
void pmemobj_tx_abort(int errnum);

/*
 * Commits current transaction
 *
 * If successful and called during TX_STAGE_WORK, transaction stage changes
 * to TX_STAGE_ONCOMMIT and function returns zero. Otherwise, stage changes
 * to TX_STAGE_ONABORT and an error number is returned.
 */
int pmemobj_tx_commit(void);

/*
 * Cleanups current transaction. Must always be called after pmemobj_tx_begin,
 * even if starting the transaction failed.
 *
 * If called during TX_STAGE_NONE, has no effect.
 *
 * Always causes transition to TX_STAGE_NONE.
 */
void pmemobj_tx_end(void);

/*
 * Performs the actions associated with current stage of the transaction,
 * and makes the transition to the next stage. Current stage must always
 * be obtained by calling pmemobj_tx_stage.
 *
 * If successful, function returns zero. Otherwise, an error number is returned.
 */
int pmemobj_tx_process(void);

#define	_POBJ_TX_BEGIN(pop, ...)\
{\
	jmp_buf _tx_env;\
	int _stage = TX_STAGE_NONE;\
	setjmp(_tx_env);\
	if (_stage == TX_STAGE_NONE)\
		pmemobj_tx_begin(pop, _tx_env, __VA_ARGS__, TX_LOCK_NONE);\
	while ((_stage = pmemobj_tx_stage()) != TX_STAGE_NONE) {\
		switch (_stage) {\
			case TX_STAGE_WORK:

#define	TX_BEGIN_LOCK(pop, ...)\
_POBJ_TX_BEGIN(pop, ##__VA_ARGS__)

#define	TX_BEGIN(pop) _POBJ_TX_BEGIN(pop, TX_LOCK_NONE)

#define	TX_ONABORT\
				pmemobj_tx_process();\
				break;\
			case TX_STAGE_ONABORT:

#define	TX_ONCOMMIT\
				pmemobj_tx_process();\
				break;\
			case TX_STAGE_ONCOMMIT:

#define	TX_FINALLY\
				pmemobj_tx_process();\
				break;\
			case TX_STAGE_FINALLY:

#define	TX_END\
				pmemobj_tx_process();\
				break;\
			default:\
				pmemobj_tx_process();\
				break;\
		}\
	}\
	pmemobj_tx_end();\
}

/*
 * Takes a "snapshot" of the memory block of given size and located at given
 * offset 'off' in the object 'oid' and saves it in the undo log.
 * The application is then free to directly modify the object in that memory
 * range. In case of failure or abort, all the changes within this range will
 * be rolled-back automatically.
 *
 * If successful and called during TX_STAGE_WORK, function returns zero.
 * Otherwise, state changes to TX_STAGE_ONABORT and an error number is returned.
 */
int pmemobj_tx_add_range(PMEMoid oid, uint64_t off, size_t size);

/*
 * Takes a "snapshot" of the given memory region and saves it in the undo log.
 * The application is then free to directly modify the object in that memory
 * range. In case of failure or abort, all the changes within this range will
 * be rolled-back automatically. The supplied block of memory has to be within
 * the given pool.
 *
 * If successful and called during TX_STAGE_WORK, function returns zero.
 * Otherwise, state changes to TX_STAGE_ONABORT and an error number is returned.
 */
int pmemobj_tx_add_range_direct(void *ptr, size_t size);

/*
 * Transactionally allocates a new object.
 *
 * If successful and called during TX_STAGE_WORK, function returns PMEMoid.
 * Otherwise, state changes to TX_STAGE_ONABORT and an OID_NULL is returned.
 */
PMEMoid pmemobj_tx_alloc(size_t size, unsigned int type_num);

/*
 * Transactionally allocates new zeroed object.
 *
 * If successful and called during TX_STAGE_WORK, function returns PMEMoid.
 * Otherwise, state changes to TX_STAGE_ONABORT and an OID_NULL is returned.
 */
PMEMoid pmemobj_tx_zalloc(size_t size, unsigned int type_num);

/*
 * Transactionally resizes an existing object.
 *
 * If successful and called during TX_STAGE_WORK, function returns PMEMoid.
 * Otherwise, state changes to TX_STAGE_ONABORT and an OID_NULL is returned.
 */
PMEMoid pmemobj_tx_realloc(PMEMoid oid, size_t size, unsigned int type_num);

/*
 * Transactionally resizes an existing object, if extended new space is zeroed.
 *
 * If successful and called during TX_STAGE_WORK, function returns PMEMoid.
 * Otherwise, state changes to TX_STAGE_ONABORT and an OID_NULL is returned.
 */
PMEMoid pmemobj_tx_zrealloc(PMEMoid oid, size_t size, unsigned int type_num);

/*
 * Transactionally allocates a new object with duplicate of the string s.
 *
 * If successful and called during TX_STAGE_WORK, function returns PMEMoid.
 * Otherwise, state changes to TX_STAGE_ONABORT and an OID_NULL is returned.
 */
PMEMoid pmemobj_tx_strdup(const char *s, unsigned int type_num);

/*
 * Transactionally frees an existing object.
 *
 * If successful and called during TX_STAGE_WORK, function returns zero.
 * Otherwise, state changes to TX_STAGE_ONABORT and an error number is returned.
 */
int pmemobj_tx_free(PMEMoid oid);

#define	TX_ADD(o)\
pmemobj_tx_add_range((o).oid, 0, sizeof (*(o)._type))

#define	TX_ADD_FIELD(o, field)\
pmemobj_tx_add_range((o).oid, offsetof(typeof (*(o)._type), field),\
		sizeof (D_RO(o)->field))

#define	TX_NEW(t) (\
{ TOID(t) _pobj_ret = (TOID(t))pmemobj_tx_alloc(sizeof (t),\
TOID_TYPE_NUM(t)); _pobj_ret; })

#define	TX_ALLOC(t, size) (\
{ TOID(t) _pobj_ret = (TOID(t))pmemobj_tx_alloc((size),\
TOID_TYPE_NUM(t)); _pobj_ret; })

#define	TX_ZNEW(t) (\
{ TOID(t) _pobj_ret = (TOID(t))pmemobj_tx_zalloc(sizeof (t),\
TOID_TYPE_NUM(t)); _pobj_ret; })

#define	TX_ZALLOC(t, size) (\
{ TOID(t) _pobj_ret = (TOID(t))pmemobj_tx_zalloc((size),\
TOID_TYPE_NUM(t)); _pobj_ret; })

#define	TX_REALLOC(o, size) (\
{ typeof (o) _pobj_ret = (typeof (o))pmemobj_tx_realloc((o).oid, (size),\
TOID_TYPE_NUM_OF(o)); _pobj_ret; })

#define	TX_ZREALLOC(o, size) (\
{ typeof (o) _pobj_ret = (typeof (o))pmemobj_tx_zrealloc((o).oid, (size),\
TOID_TYPE_NUM_OF(o)); _pobj_ret; })

#define	TX_STRDUP(s, type_num)\
pmemobj_tx_strdup(s, type_num)

#define	TX_FREE(o)\
pmemobj_tx_free((o).oid)

#define	TX_SET(o, field, value) (\
{\
	TX_ADD_FIELD(o, field);\
	D_RW(o)->field = value; })

static inline void *
TX_MEMCPY(void *dest, const void *src, size_t num)
{
	pmemobj_tx_add_range_direct(dest, num);
	return memcpy(dest, src, num);
}

static inline void *
TX_MEMSET(void *dest, int c, size_t num)
{
	pmemobj_tx_add_range_direct(dest, num);
	return memset(dest, c, num);
}

#ifdef __cplusplus
}
#endif
#endif	/* libpmemobj.h */
