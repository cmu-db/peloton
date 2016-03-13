/*
 * Copyright (c) 2015, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *      * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *      * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *      * Neither the name of Intel Corporation nor the names of its
 *        contributors may be used to endorse or promote products derived
 *        from this software without specific prior written permission.
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
 * obj_locks.c -- main source file for PMEM locks benchmark
 */

#include <assert.h>
#include <errno.h>

#include "libpmemobj.h"
#include "benchmark.h"

#include "util.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "obj.h"


#define	_BENCH_OPERATION_1BY1(flock, funlock, mb, type, ...) (\
{\
	for (unsigned _i = 0; _i < (mb)->pa->n_locks; (_i)++) {\
		type *_o = (type *)(&(mb)->locks[_i]);\
		(flock)(__VA_ARGS__(_o));\
		(funlock)(__VA_ARGS__(_o));\
	}\
})

#define	_BENCH_OPERATION_ALL_LOCK(flock, funlock, mb, type, ...) (\
{\
	for (unsigned _i = 0; _i < (mb)->pa->n_locks; (_i)++) {\
		type *_o = (type *)(&(mb)->locks[_i]);\
		(flock)(__VA_ARGS__(_o));\
	}\
	for (unsigned _i = 0; _i < (mb)->pa->n_locks; (_i)++) {\
		type *_o = (type *)(&(mb)->locks[_i]);\
		(funlock)(__VA_ARGS__(_o));\
	}\
})

#define	BENCH_OPERATION_1BY1(flock, funlock, mb, type, ...)\
	_BENCH_OPERATION_1BY1(flock, funlock, mb, type, ##__VA_ARGS__,\
)

#define	BENCH_OPERATION_ALL_LOCK(flock, funlock, mb, type, ...)\
	_BENCH_OPERATION_ALL_LOCK(flock, funlock, mb, type, ##__VA_ARGS__,\
)


struct prog_args {
	bool use_pthread;	/* use pthread locks instead of PMEM locks */
	unsigned n_locks;	/* number of mutex/rwlock objects */
	bool run_id_increment;	/* increment run_id after each lock/unlock */
	uint64_t runid_initial_value;	/* initial value of run_id */
	char *lock_mode;	/* "1by1" or "all-lock" */
	char *lock_type;	/* "mutex", "rwlock" or "ram-mutex" */
	bool use_rdlock;	/* use read lock, instead of write lock */
};

/*
 * mutex similar to PMEMmutex, but with pthread_mutex_t in RAM
 */
typedef union padded_volatile_pmemmutex {
	char padding[_POBJ_CL_ALIGNMENT];
	struct {
		uint64_t runid;
		pthread_mutex_t *mutexp; /* pointer to pthread mutex in RAM */
	} volatile_pmemmutex;
} PMEM_volatile_mutex;

typedef union lock_union {
	PMEMmutex pm_mutex;
	PMEMrwlock pm_rwlock;
	PMEM_volatile_mutex pm_vmutex;
	pthread_mutex_t pt_mutex;
	pthread_rwlock_t pt_rwlock;
} lock_t;

POBJ_LAYOUT_BEGIN(pmembench_lock_layout);
POBJ_LAYOUT_ROOT(pmembench_lock_layout, struct my_root);
POBJ_LAYOUT_TOID(pmembench_lock_layout, lock_t);
POBJ_LAYOUT_END(pmembench_lock_layout);

/*
 * my_root -- root object structure
 */
struct my_root {
	TOID(lock_t) locks;	/* an array of locks */
};

/*
 * lock usage
 */
enum operation_mode {
	OP_MODE_1BY1,		/* lock and unlock one lock at a time */
	OP_MODE_ALL_LOCK,	/* grab all locks, then unlock them all */
	OP_MODE_MAX,
};

/*
 * lock type
 */
enum benchmark_mode {
	BENCH_MODE_MUTEX,	/* PMEMmutex vs. pthread_mutex_t */
	BENCH_MODE_RWLOCK,	/* PMEMrwlock vs. pthread_rwlock_t */
	BENCH_MODE_VOLATILE_MUTEX, /* PMEMmutex with pthread mutex in RAM */
	BENCH_MODE_MAX
};

struct mutex_bench;

struct bench_ops {
	int (*bench_init)(struct mutex_bench *);
	int (*bench_exit)(struct mutex_bench *);
	int (*bench_op)(struct mutex_bench *);
};

/*
 * mutex_bench -- stores variables used in benchmark, passed within functions
 */
struct mutex_bench {
	PMEMobjpool *pop;		/* pointer to the persistent pool */
	TOID(struct my_root) root;	/* OID of the root object */
	struct prog_args *pa;		/* prog_args structure */
	enum operation_mode lock_mode;	/* lock usage mode */
	enum benchmark_mode lock_type;	/* lock type */
	lock_t *locks;			/* pointer to the array of locks */
	struct bench_ops *ops;
};

#define	GET_VOLATILE_MUTEX(pop, mutexp)\
get_lock((pop)->run_id,\
	&(mutexp)->volatile_pmemmutex.runid,\
	&(mutexp)->volatile_pmemmutex.mutexp,\
	(void *)volatile_mutex_init,\
	sizeof ((mutexp)->volatile_pmemmutex.mutexp))

/*
 * get_lock -- atomically initialize and return a lock
 */
static void *
get_lock(uint64_t pop_runid, volatile uint64_t *runid, void *lock,
	int (*init_lock)(void *lock, void *arg), size_t size)
{
	uint64_t tmp_runid;
	while ((tmp_runid = *runid) != pop_runid) {
		if ((tmp_runid != (pop_runid - 1))) {
			if (__sync_bool_compare_and_swap(runid,
					tmp_runid, (pop_runid - 1))) {
				if (init_lock(lock, NULL)) {
					__sync_fetch_and_and(runid, 0);
					return NULL;
				}

				if (__sync_bool_compare_and_swap(
						runid, (pop_runid - 1),
						pop_runid) == 0) {
					return NULL;
				}
			}
		}
	}
	return lock;
}

/*
 * volatile_mutex_init -- initialize the volatile mutex object
 *
 * Allocate memory for the pthread mutex and initialize it.
 * Set the runid to the same value as in the memory pool.
 */
static int
volatile_mutex_init(pthread_mutex_t **mutexp, void *attr)
{
	if (*mutexp == NULL) {
		*mutexp = malloc(sizeof (pthread_mutex_t));
		if (*mutexp == NULL) {
			perror("volatile_mutex_init alloc");
			return ENOMEM;
		}
	}

	return pthread_mutex_init(*mutexp, NULL);
}

/*
 * volatile_mutex_lock -- initialize the mutex object if needed and lock it
 */
static int
volatile_mutex_lock(PMEMobjpool *pop, PMEM_volatile_mutex *mutexp)
{
	pthread_mutex_t *mutex = GET_VOLATILE_MUTEX(pop, mutexp);
	if (mutex == NULL)
		return EINVAL;

	return pthread_mutex_lock(mutex);
}

/*
 * volatile_mutex_unlock -- unlock the mutex
 */
static int
volatile_mutex_unlock(PMEMobjpool *pop, PMEM_volatile_mutex *mutexp)
{
	pthread_mutex_t *mutex = GET_VOLATILE_MUTEX(pop, mutexp);
	if (mutex == NULL)
		return EINVAL;

	return pthread_mutex_unlock(mutex);
}

/*
 * volatile_mutex_destroy -- destroy pthread mutex and release memory
 */
static int
volatile_mutex_destroy(PMEMobjpool *pop, PMEM_volatile_mutex *mutexp)
{
	pthread_mutex_t *mutex = GET_VOLATILE_MUTEX(pop, mutexp);
	if (mutex == NULL)
		return EINVAL;

	int ret = pthread_mutex_destroy(mutex);
	if (ret != 0)
		return ret;

	free(mutex);

	return 0;
}

/*
 * init_bench_mutex -- allocate and initialize mutex objects
 */
static int
init_bench_mutex(struct mutex_bench *mb)
{
	POBJ_ZALLOC(mb->pop, &D_RW(mb->root)->locks, lock_t,
			mb->pa->n_locks * sizeof (lock_t));
	if (TOID_IS_NULL(D_RO(mb->root)->locks)) {
		perror("POBJ_ZALLOC");
		return -1;
	}

	mb->locks = D_RW(D_RW(mb->root)->locks);

	if (!mb->pa->use_pthread) {
		/* initialize PMEM mutexes */
		for (unsigned i = 0; i < mb->pa->n_locks; i++) {
			PMEMmutex *p = (PMEMmutex *)&mb->locks[i];
			p->pmemmutex.runid = mb->pa->runid_initial_value;
			pthread_mutex_init(&p->pmemmutex.mutex, NULL);
		}
	} else {
		/* initialize pthread mutexes */
		for (unsigned i = 0; i < mb->pa->n_locks; i++) {
			pthread_mutex_t *p = (pthread_mutex_t *)&mb->locks[i];
			pthread_mutex_init(p, NULL);
		}
	}

	return 0;
}

/*
 * exit_bench_mutex -- destroy the mutex objects and release memory
 */
static int
exit_bench_mutex(struct mutex_bench *mb)
{
	if (mb->pa->use_pthread) {
		/* deinitialize pthread mutex objects */
		for (unsigned i = 0; i < mb->pa->n_locks; i++) {
			pthread_mutex_t *p = (pthread_mutex_t *)&mb->locks[i];
			pthread_mutex_destroy(p);
		}
	}

	POBJ_FREE(&D_RW(mb->root)->locks);

	return 0;
}

/*
 * op_bench_mutex -- lock and unlock the mutex object
 *
 * If requested, increment the run_id of the memory pool.  In case of PMEMmutex
 * this will force the rwlock object(s) reinitialization at the lock operation.
 */
static int
op_bench_mutex(struct mutex_bench *mb)
{
	if (!mb->pa->use_pthread) {
		if (mb->lock_mode == OP_MODE_1BY1)
			BENCH_OPERATION_1BY1(pmemobj_mutex_lock,
				pmemobj_mutex_unlock, mb, PMEMmutex, mb->pop);
		else
			BENCH_OPERATION_ALL_LOCK(pmemobj_mutex_lock,
				pmemobj_mutex_unlock, mb, PMEMmutex, mb->pop);

		if (mb->pa->run_id_increment)
			mb->pop->run_id += 2; /* must be a multiple of 2 */
	} else {
		if (mb->lock_mode == OP_MODE_1BY1)
			BENCH_OPERATION_1BY1(pthread_mutex_lock,
				pthread_mutex_unlock, mb, pthread_mutex_t);
		else
			BENCH_OPERATION_ALL_LOCK(pthread_mutex_lock,
				pthread_mutex_unlock, mb, pthread_mutex_t);
	}

	return 0;
}

/*
 * init_bench_rwlock -- allocate and initialize rwlock objects
 */
static int
init_bench_rwlock(struct mutex_bench *mb)
{
	POBJ_ZALLOC(mb->pop, &D_RW(mb->root)->locks, lock_t,
			mb->pa->n_locks * sizeof (lock_t));
	if (TOID_IS_NULL(D_RO(mb->root)->locks)) {
		perror("POBJ_ZALLOC");
		return -1;
	}

	mb->locks = D_RW(D_RW(mb->root)->locks);

	if (!mb->pa->use_pthread) {
		/* initialize PMEM rwlocks */
		for (unsigned i = 0; i < mb->pa->n_locks; i++) {
			PMEMrwlock *p = (PMEMrwlock *)&mb->locks[i];
			p->pmemrwlock.runid = mb->pa->runid_initial_value;
			pthread_rwlock_init(&p->pmemrwlock.rwlock, NULL);
		}
	} else {
		/* initialize pthread rwlocks */
		for (unsigned i = 0; i < mb->pa->n_locks; i++) {
			pthread_rwlock_t *p = (pthread_rwlock_t *)&mb->locks[i];
			pthread_rwlock_init(p, NULL);
		}
	}

	return 0;
}

/*
 * exit_bench_rwlock -- destroy the rwlocks and release memory
 */
static int
exit_bench_rwlock(struct mutex_bench *mb)
{
	if (mb->pa->use_pthread) {
		/* deinitialize pthread mutex objects */
		for (unsigned int i = 0; i < mb->pa->n_locks; i++) {
			pthread_rwlock_t *p = (pthread_rwlock_t *)&mb->locks[i];
			pthread_rwlock_destroy(p);
		}
	}

	POBJ_FREE(&D_RW(mb->root)->locks);

	return 0;
}

/*
 * op_bench_rwlock -- lock and unlock the rwlock object
 *
 * If requested, increment the run_id of the memory pool.  In case of PMEMrwlock
 * this will force the rwlock object(s) reinitialization at the lock operation.
 */
static int
op_bench_rwlock(struct mutex_bench *mb)
{
	if (!mb->pa->use_pthread) {
		if (mb->lock_mode == OP_MODE_1BY1)
			BENCH_OPERATION_1BY1(!mb->pa->use_rdlock ?
				pmemobj_rwlock_wrlock : pmemobj_rwlock_rdlock,
				pmemobj_rwlock_unlock, mb, PMEMrwlock, mb->pop);
		else
			BENCH_OPERATION_ALL_LOCK(!mb->pa->use_rdlock ?
				pmemobj_rwlock_wrlock : pmemobj_rwlock_rdlock,
				pmemobj_rwlock_unlock, mb, PMEMrwlock, mb->pop);

		if (mb->pa->run_id_increment)
			mb->pop->run_id += 2; /* must be a multiple of 2 */
	} else {
		if (mb->lock_mode == OP_MODE_1BY1)
			BENCH_OPERATION_1BY1(!mb->pa->use_rdlock ?
				pthread_rwlock_wrlock : pthread_rwlock_rdlock,
				pthread_rwlock_unlock, mb, pthread_rwlock_t);
		else
			BENCH_OPERATION_ALL_LOCK(!mb->pa->use_rdlock ?
				pthread_rwlock_wrlock : pthread_rwlock_rdlock,
				pthread_rwlock_unlock, mb, pthread_rwlock_t);
	}
	return 0;
}

/*
 * init_bench_vmutex -- allocate and initialize mutexes
 */
static int
init_bench_vmutex(struct mutex_bench *mb)
{
	POBJ_ZALLOC(mb->pop, &D_RW(mb->root)->locks, lock_t,
			mb->pa->n_locks * sizeof (lock_t));
	if (TOID_IS_NULL(D_RO(mb->root)->locks)) {
		perror("POBJ_ZALLOC");
		return -1;
	}

	mb->locks = D_RW(D_RW(mb->root)->locks);

	/* initialize PMEM volatile mutexes */
	for (unsigned i = 0; i < mb->pa->n_locks; i++) {
		PMEM_volatile_mutex *p = (PMEM_volatile_mutex *)&mb->locks[i];
		p->volatile_pmemmutex.runid = mb->pa->runid_initial_value;
		volatile_mutex_init(&p->volatile_pmemmutex.mutexp, NULL);
	}

	return 0;
}

/*
 * exit_bench_vmutex -- destroy the mutex objects and release their
 * memory
 */
static int
exit_bench_vmutex(struct mutex_bench *mb)
{
	for (unsigned i = 0; i < mb->pa->n_locks; i++) {
		PMEM_volatile_mutex *p = (PMEM_volatile_mutex *)&mb->locks[i];
		volatile_mutex_destroy(mb->pop, p);
	}

	POBJ_FREE(&D_RW(mb->root)->locks);

	return 0;
}

/*
 * op_bench_volatile_mutex -- lock and unlock the mutex object
 */
static int
op_bench_vmutex(struct mutex_bench *mb)
{
	if (mb->lock_mode == OP_MODE_1BY1)
		BENCH_OPERATION_1BY1(volatile_mutex_lock,
				volatile_mutex_unlock,
				mb, PMEM_volatile_mutex, mb->pop);
	else
		BENCH_OPERATION_ALL_LOCK(volatile_mutex_lock,
				volatile_mutex_unlock, mb,
				PMEM_volatile_mutex, mb->pop);

	if (mb->pa->run_id_increment)
		mb->pop->run_id += 2; /* must be a multiple of 2 */

	return 0;
}

struct bench_ops benchmark_ops[BENCH_MODE_MAX] = {
	{ init_bench_mutex, exit_bench_mutex, op_bench_mutex },
	{ init_bench_rwlock, exit_bench_rwlock, op_bench_rwlock },
	{ init_bench_vmutex, exit_bench_vmutex, op_bench_vmutex }
};

/*
 * operation_mode -- parses command line "--mode" and returns
 * proper operation mode
 */
static enum operation_mode
parse_op_mode(const char *arg)
{
	if (strcmp(arg, "1by1") == 0)
		return OP_MODE_1BY1;
	else if (strcmp(arg, "all-lock") == 0)
		return OP_MODE_ALL_LOCK;
	else
		return OP_MODE_MAX;
}

/*
 * benchmark_mode -- parses command line "--bench_type" and returns
 * proper benchmark ops
 */
static struct bench_ops *
parse_benchmark_mode(const char *arg)
{
	if (strcmp(arg, "mutex") == 0)
		return &benchmark_ops[BENCH_MODE_MUTEX];
	else if (strcmp(arg, "rwlock") == 0)
		return &benchmark_ops[BENCH_MODE_RWLOCK];
	else if (strcmp(arg, "volatile-mutex") == 0)
		return &benchmark_ops[BENCH_MODE_VOLATILE_MUTEX];
	else
		return NULL;
}

/*
 * locks_init -- allocates persistent memory, maps it, creates the appropriate
 * objects in the allocated memory and initializes them
 */
static int
locks_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);

	int ret = 0;

	struct mutex_bench *mb = malloc(sizeof (*mb));
	if (mb == NULL) {
		perror("malloc");
		return -1;
	}

	mb->pa = args->opts;

	mb->lock_mode = parse_op_mode(mb->pa->lock_mode);
	if (mb->lock_mode >= OP_MODE_MAX) {
		fprintf(stderr, "Invalid mutex mode: %s\n", mb->pa->lock_mode);
		errno = EINVAL;
		goto err_free_mb;
	}

	mb->ops = parse_benchmark_mode(mb->pa->lock_type);
	if (mb->ops == NULL) {
		fprintf(stderr, "Invalid benchmark type: %s\n",
			mb->pa->lock_type);
		errno = EINVAL;
		goto err_free_mb;
	}

	/* reserve some space for metadata */
	size_t poolsize = mb->pa->n_locks * sizeof (lock_t) + PMEMOBJ_MIN_POOL;

	mb->pop = pmemobj_create(args->fname,
			POBJ_LAYOUT_NAME(pmembench_lock_layout),
			poolsize, args->fmode);

	if (mb->pop == NULL) {
		ret = -1;
		perror("pmemobj_create");
		goto err_free_mb;
	}

	mb->root = POBJ_ROOT(mb->pop, struct my_root);
	assert(!TOID_IS_NULL(mb->root));

	ret = mb->ops->bench_init(mb);
	if (ret != 0)
		goto err_free_pop;

	pmembench_set_priv(bench, mb);

	return 0;

err_free_pop:
	pmemobj_close(mb->pop);

err_free_mb:
	free(mb);
	return ret;
}

/*
 * locks_exit -- destroys allocated objects and release memory
 */
static int
locks_exit(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);

	struct mutex_bench *mb = pmembench_get_priv(bench);
	assert(mb != NULL);

	mb->ops->bench_exit(mb);

	pmemobj_close(mb->pop);
	free(mb);

	return 0;
}

/*
 * locks_op -- actual benchmark operation
 *
 * Performs lock and unlock as by the program arguments.
 */
static int
locks_op(struct benchmark *bench, struct operation_info *info)
{
	struct mutex_bench *mb = pmembench_get_priv(bench);
	assert(mb != NULL);
	assert(mb->pop != NULL);
	assert(!TOID_IS_NULL(mb->root));
	assert(mb->locks != NULL);
	assert(mb->lock_mode < OP_MODE_MAX);

	mb->ops->bench_op(mb);

	return 0;
}

/* structure to define command line arguments */
static struct benchmark_clo locks_clo[] = {
	{
		.opt_short	= 'p',
		.opt_long	= "use_pthread",
		.descr		= "Use pthread locks instead of PMEM, does not "
					"matter for volatile mutex",
		.def		= "false",
		.off		= clo_field_offset(struct prog_args,
							use_pthread),
		.type		= CLO_TYPE_FLAG,
	},
	{
		.opt_short	= 'm',
		.opt_long	= "numlocks",
		.descr		= "The number of lock objects used "
					"for benchmark",
		.def		= "1",
		.off		= clo_field_offset(struct prog_args, n_locks),
		.type		= CLO_TYPE_UINT,
		.type_uint	= {
			.size	= clo_field_size(struct prog_args, n_locks),
			.base	= CLO_INT_BASE_DEC,
			.min	= 1,
			.max	= UINT_MAX
		}
	},
	{
		.opt_short	= 0,
		.opt_long	= "mode",
		.descr		= "Locking mode",
		.type		= CLO_TYPE_STR,
		.off		= clo_field_offset(struct prog_args, lock_mode),
		.def		= "1by1",
	},
	{
		.opt_short	= 'r',
		.opt_long	= "run_id",
		.descr		= "Increment the run_id of PMEM object pool "
					"after each operation",
		.def		= "false",
		.off		= clo_field_offset(struct prog_args,
							run_id_increment),
		.type		= CLO_TYPE_FLAG,
	},
	{
		.opt_short	= 'i',
		.opt_long	= "run_id_init_val",
		.descr		= "Use this value for initializing the run_id "
					"of each PMEMmutex object",
		.def		= "2",
		.off		= clo_field_offset(struct prog_args,
							runid_initial_value),
		.type		= CLO_TYPE_UINT,
		.type_uint	= {
			.size	= clo_field_size(struct prog_args,
							runid_initial_value),
			.base	= CLO_INT_BASE_DEC,
			.min	= 0,
			.max	= UINT64_MAX
		}
	},
	{
		.opt_short	= 'b',
		.opt_long	= "bench_type",
		.descr		= "The Benchmark type: mutex, rwlock or "
					"volatile-mutex",
		.type		= CLO_TYPE_STR,
		.off		= clo_field_offset(struct prog_args, lock_type),
		.def		= "mutex",
	},
	{
		.opt_short	= 'R',
		.opt_long	= "rdlock",
		.descr		= "Select read over write lock, only valid "
					"when lock_type is \"rwlock\"",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct prog_args,
							use_rdlock),
	},
};

/* Stores information about benchmark. */
static struct benchmark_info locks_info = {
	.name		= "obj_locks",
	.brief		= "Benchmark for pmem locks operations",
	.init		= locks_init,
	.exit		= locks_exit,
	.multithread	= false,
	.multiops	= true,
	.operation	= locks_op,
	.measure_time	= true,
	.clos		= locks_clo,
	.nclos		= ARRAY_SIZE(locks_clo),
	.opts_size	= sizeof (struct prog_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(locks_info);
