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
 * pmemobj_gen.c -- benchmark for pmemobj_direct() and pmemobj_open() functions.
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "libpmemobj.h"
#include "benchmark.h"

#define	LAYOUT_NAME "benchmark"
#define	FACTOR 4
#define	DIR_MODE 0700
#define	FILE_MODE 0666
#define	PART_NAME "/part"
#define	MAX_DIGITS 2

struct pobj_bench;
struct pobj_worker;

typedef int (*fn_type_num) (struct pobj_bench *ob, int worker_idx,
								int op_idx);

typedef unsigned int (*fn_size) (struct pobj_bench *ob, unsigned int idx);

typedef unsigned int (*fn_num) (unsigned int idx);

/*
 * Enumeration used to determine the mode of the assigning type_number
 * value to the persistent objects.
 */
enum type_mode {
	TYPE_MODE_ONE,
	TYPE_MODE_PER_THREAD,
	TYPE_MODE_RAND,
	MAX_TYPE_MODE,
};

/*
 * pobj_args - Stores command line parsed arguments.
 *
 * rand_type	: Use random type number for every new allocated object.
 *		  Default, there is one type number for all objects.
 *
 * range	: Use random allocation size.
 *
 * min_size	: Minimum allocation size.
 *
 * n_objs	: Number of objects allocated per thread
 *
 * one_pool	: Use one common pool for all thread
 *
 * one_obj	: Create and use one object per thread
 *
 * obj_size	: Size of each allocated object
 *
 * n_ops	: Number of operations
 */
struct pobj_args {
	char *type_num;
	bool range;
	unsigned int min_size;
	unsigned int n_objs;
	bool one_pool;
	bool one_obj;
	size_t obj_size;
	size_t n_ops;
};

/*
 * pobj_bench - Stores variables used in benchmark, passed within functions.
 *
 * pop			: Pointer to the persistent pool.
 *
 * pa			: Stores pobj_args structure.
 *
 * sets			: Stores files names using to create pool per thread
 *
 * random_types		: Random type numbers for persistent objects.
 *
 * rand_sizes		: random values with allocation sizes.
 *
 * n_pools		: Number of created pools.
 *
 * n_objs		: Number of object created per thread.
 *
 * type_mode		: Type_mode enum value
 *
 * fn_type_num		: Function returning proper type number for each object.
 *
 * fn_size		: Function returning proper size of allocation.
 *
 * pool			: Functions returning number of thread if
 *			  one pool per thread created or index 0 if not.
 *
 * obj			: Function returning number of operation if flag set
 *			  to false or index 0 if set to true.
 */
struct pobj_bench {
	PMEMobjpool **pop;
	struct pobj_args *args_priv;
	const char **sets;
	size_t *random_types;
	size_t *rand_sizes;
	size_t n_pools;
	int type_mode;
	fn_type_num fn_type_num;
	fn_size fn_size;
	fn_num pool;
	fn_num obj;
};

/*
 * pobj_worker - Stores variables used by one thread.
 */
struct pobj_worker {
	PMEMoid *oids;
};

/* Array defining common command line arguments. */
static struct benchmark_clo pobj_direct_clo[] = {
	{
		.opt_short	= 'T',
		.opt_long	= "type-number",
		.descr		= "Type number mode - one, per-thread, rand",
		.def		= "one",
		.off		= clo_field_offset(struct pobj_args, type_num),
		.type		= CLO_TYPE_STR,
	},
	{
		.opt_short	= 'm',
		.opt_long	= "min-size",
		.type		= CLO_TYPE_UINT,
		.descr		= "Minimum allocation size",
		.off		= clo_field_offset(struct pobj_args,
						min_size),
		.def		= "0",
		.type_uint	= {
			.size	= clo_field_size(struct pobj_args,
						min_size),
			.base	= CLO_INT_BASE_DEC|CLO_INT_BASE_HEX,
			.min	= 0,
			.max	= UINT_MAX,
		},
	},
	{
		.opt_short	= 'P',
		.opt_long	= "one-pool",
		.descr		= "Create one pool for all threads",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct pobj_args,
								one_pool),
	},
	{
		.opt_short	= 'O',
		.opt_long	= "one-object",
		.descr		= "Use only one object per thread",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct pobj_args,
								one_obj),
	},
};

static struct benchmark_clo pobj_open_clo[] = {
	{
		.opt_short	= 'T',
		.opt_long	= "type-number",
		.descr		= "Type number mode - one, per-thread, rand",
		.def		= "one",
		.off		= clo_field_offset(struct pobj_args, type_num),
		.type		= CLO_TYPE_STR,
	},
	{
		.opt_short	= 'm',
		.opt_long	= "min-size",
		.type		= CLO_TYPE_UINT,
		.descr		= "Minimum allocation size",
		.off		= clo_field_offset(struct pobj_args,
						min_size),
		.def		= "0",
		.type_uint	= {
			.size	= clo_field_size(struct pobj_args,
						min_size),
			.base	= CLO_INT_BASE_DEC|CLO_INT_BASE_HEX,
			.min	= 0,
			.max	= UINT_MAX,
		},
	},
	{
		.opt_short	= 'o',
		.opt_long	= "objects",
		.type		= CLO_TYPE_UINT,
		.descr		= "Number of objects in each pool",
		.off		= clo_field_offset(struct pobj_args,
						n_objs),
		.def		= "1",
		.type_uint	= {
			.size	= clo_field_size(struct pobj_args,
						n_objs),
			.base	= CLO_INT_BASE_DEC|CLO_INT_BASE_HEX,
			.min	= 1,
			.max	= UINT_MAX,
		},
	},
};

/*
 * type_mode_one -- always returns 0, as in the mode TYPE_MODE_ONE
 * all of the persistent objects have the same type_number value.
 */
static int
type_mode_one(struct pobj_bench *bench_priv, int worker_idx, int op_idx)
{
	return 0;
}

/*
 * type_mode_per_thread -- always returns worker index, as in the mode
 * TYPE_MODE_PER_THREAD all persistent object allocated by the same thread
 * have the same type_number value.
 */
static int
type_mode_per_thread(struct pobj_bench *bench_priv, int worker_idx, int op_idx)
{
	return worker_idx;
}

/*
 * type_mode_rand -- returns the value from the random_types array assigned
 * for the specific operation in a specific thread.
 */
static int
type_mode_rand(struct pobj_bench *bench_priv, int worker_idx, int op_idx)
{
	return bench_priv->random_types[op_idx];
}

/*
 * range_size -- returns size of object allocation from rand_sizes array.
 */
static unsigned int
range_size(struct pobj_bench *bench_priv, unsigned int idx)
{
	return bench_priv->rand_sizes[idx];
}

/*
 * static_size -- returns always the same size of object allocation.
 */
static unsigned int
static_size(struct pobj_bench *bench_priv, unsigned int idx)
{
	return bench_priv->args_priv->obj_size;
}

/*
 * diff_num -- returns given index
 */
static unsigned int
diff_num(unsigned int idx)
{
	return idx;
}

/*
 * one_num -- returns always the same index.
 */
static unsigned int
one_num(unsigned int idx)
{
	return 0;
}

static fn_type_num type_mode_func[MAX_TYPE_MODE] = {type_mode_one,
				type_mode_per_thread, type_mode_rand};

const char *type_mode_names[MAX_TYPE_MODE] = {"one", "per-thread", "rand"};

/*
 * parse_type_mode -- parses command line "--type-number" argument
 * and returns proper type_mode enum value.
 */
static enum type_mode
parse_type_mode(const char *arg)
{
	enum type_mode i = 0;
	for (; i < MAX_TYPE_MODE && strcmp(arg, type_mode_names[i]) != 0; ++i)
	;
	return i;
}

/*
 * rand_sizes -- allocates array and calculates random values as allocation
 * sizes for each object. Used only when range flag set.
 */
static size_t *
rand_sizes(unsigned int min, unsigned int max, unsigned int n_ops)
{
	size_t *rand_sizes = malloc(n_ops * sizeof (size_t));
	if (rand_sizes == NULL) {
		perror("malloc");
		return NULL;
	}
	for (size_t i = 0; i < n_ops; i++) {
		rand_sizes[i] = RRAND(max, min);
	}
	return rand_sizes;
}

/*
 * random_types -- allocates array and calculates random values to assign
 * type_number for each object.
 */
static int
random_types(struct pobj_bench *bench_priv, struct benchmark_args *args)
{
	bench_priv->random_types = malloc(bench_priv->args_priv->n_objs *
							sizeof (size_t));
	if (bench_priv->random_types == NULL) {
		perror("malloc");
		return -1;
	}
	for (size_t i = 0; i < bench_priv->args_priv->n_objs; i++)
		bench_priv->random_types[i] = rand() % PMEMOBJ_NUM_OID_TYPES;
	return 0;
}

/*
 * pobj_init - common part of the benchmark initialization functions.
 * Parses command line arguments, set variables and creates persistent pools.
 */
static int
pobj_init(struct benchmark *bench, struct benchmark_args *args)
{
	int i = 0;
	assert(bench != NULL);
	assert(args != NULL);

	struct pobj_bench *bench_priv = malloc(sizeof (struct pobj_bench));
	if (bench_priv == NULL) {
		perror("malloc");
		return -1;
	}
	assert(args->opts != NULL);

	bench_priv->args_priv = args->opts;
	bench_priv->args_priv->obj_size = args->dsize;
	bench_priv->args_priv->range = bench_priv->args_priv->min_size > 0 ?
							true : false;
	bench_priv->n_pools = !bench_priv->args_priv->one_pool ?
							args->n_threads : 1;
	bench_priv->pool = bench_priv->n_pools > 1 ? diff_num : one_num;
	bench_priv->obj = !bench_priv->args_priv->one_obj ? diff_num : one_num;
	/*
	 * Multiplication by FACTOR prevents from out of memory error
	 * as the actual size of the allocated persistent objects
	 * is always larger than requested.
	 */
	size_t n_objs = bench_priv->args_priv->n_objs;
	if (bench_priv->n_pools == 1)
		n_objs *= args->n_threads;
	size_t psize = n_objs * args->dsize * args->n_threads * FACTOR;
	if (psize < PMEMOBJ_MIN_POOL)
		psize = PMEMOBJ_MIN_POOL;

	/* assign type_number determining function */
	bench_priv->type_mode =
			parse_type_mode(bench_priv->args_priv->type_num);
	switch (bench_priv->type_mode) {
		case MAX_TYPE_MODE:
			fprintf(stderr, "unknown type mode");
			goto free_bench_priv;
		case TYPE_MODE_RAND:
			if (random_types(bench_priv, args))
				goto free_bench_priv;
			break;
		default:
			bench_priv->random_types = NULL;
	}
	bench_priv->fn_type_num = type_mode_func[bench_priv->type_mode];

	/* assign size determining function */
	bench_priv->fn_size = bench_priv->args_priv->range ?
						range_size : static_size;
	bench_priv->rand_sizes = NULL;
	if (bench_priv->args_priv->range) {
		if (bench_priv->args_priv->min_size > args->dsize) {
			fprintf(stderr, "Invalid allocation size");
			goto free_random_types;
		}
		bench_priv->rand_sizes = rand_sizes(
						bench_priv->args_priv->min_size,
						bench_priv->args_priv->obj_size,
						bench_priv->args_priv->n_objs);
		if (bench_priv->rand_sizes == NULL)
			goto free_random_types;
	}

	bench_priv->pop = calloc(bench_priv->n_pools, sizeof (PMEMobjpool *));
	if (bench_priv->pop == NULL) {
		perror("calloc");
		goto free_random_sizes;
	}

	bench_priv->sets = calloc(bench_priv->n_pools, sizeof (const char *));
	if (bench_priv->sets == NULL) {
		perror("calloc");
		goto free_pop;
	}
	if (bench_priv->n_pools > 1) {
		mkdir(args->fname, DIR_MODE);
		if (access(args->fname, F_OK) != 0) {
			fprintf(stderr, "cannot create directory\n");
			goto free_sets;
		}
		size_t path_len = (strlen(PART_NAME) + strlen(args->fname))
							+ MAX_DIGITS + 1;
		for (i = 0; i < bench_priv->n_pools; i++) {
			bench_priv->sets[i] = malloc(path_len * sizeof (char));
			if (bench_priv->sets[i] == NULL) {
				perror("malloc");
				goto free_sets;
			}
			snprintf((char *)bench_priv->sets[i], path_len,
					"%s%s%02x", args->fname, PART_NAME, i);
			bench_priv->pop[i] = pmemobj_create(bench_priv->sets[i],
						LAYOUT_NAME, psize, FILE_MODE);
			if (bench_priv->pop[i] == NULL) {
				perror(pmemobj_errormsg());
				goto free_sets;
			}
		}
	} else {
		bench_priv->sets[0] = (const char *)args->fname;
		bench_priv->pop[0] = pmemobj_create(bench_priv->sets[0],
				LAYOUT_NAME, psize, FILE_MODE);
		if (bench_priv->pop[0] == NULL) {
			perror(pmemobj_errormsg());
			goto free_pools;
		}
	}
	pmembench_set_priv(bench, bench_priv);

	return 0;
free_sets:
	for (i--; i >= 0; i--) {
		pmemobj_close(bench_priv->pop[i]);
		free((char *)bench_priv->sets[i]);
	}
free_pools:
	free(bench_priv->sets);
free_pop:
	free(bench_priv->pop);
free_random_sizes:
	free(bench_priv->rand_sizes);
free_random_types:
	free(bench_priv->random_types);
free_bench_priv:
	free(bench_priv);

	return -1;
}

/*
 * pobj_direct_init -- special part of pobj_direct benchmark initialization.
 */
static int
pobj_direct_init(struct benchmark *bench, struct benchmark_args *args)
{
	struct pobj_args *pa = args->opts;
	pa->n_objs = pa->one_obj ? 1 : args->n_ops_per_thread;
	if (pobj_init(bench, args) != 0)
		return -1;
	return 0;
}

/*
 * pobj_exit -- common part for the benchmarks exit functions
 */
static int
pobj_exit(struct benchmark *bench, struct benchmark_args *args)
{
	size_t i;
	struct pobj_bench *bench_priv = pmembench_get_priv(bench);
	if (bench_priv->n_pools > 1) {
		for (i = 0; i < bench_priv->n_pools; i++) {
			pmemobj_close(bench_priv->pop[i]);
			free((char *)bench_priv->sets[i]);
		}
	} else {
		pmemobj_close(bench_priv->pop[0]);
	}
	free(bench_priv->sets);
	free(bench_priv->pop);
	free(bench_priv->rand_sizes);
	free(bench_priv->random_types);
	free(bench_priv);
	return 0;
}

/*
 * pobj_init_worker -- worker initialization
 */
static int
pobj_init_worker(struct benchmark *bench, struct benchmark_args
					*args, struct worker_info *worker)
{
	int i, idx = worker->index;
	struct pobj_bench *bench_priv = pmembench_get_priv(bench);
	struct pobj_worker *pw = calloc(1, sizeof (struct pobj_worker));
	if (pw == NULL) {
		perror("calloc");
		return -1;
	}

	worker->priv = pw;
	pw->oids = calloc(bench_priv->args_priv->n_objs, sizeof (PMEMoid));
	if (pw->oids == NULL) {
		free(pw);
		perror("calloc");
		return -1;
	}

	PMEMobjpool *pop = bench_priv->pop[bench_priv->pool(idx)];
	for (i = 0; i < bench_priv->args_priv->n_objs; i++) {
		size_t size = bench_priv->fn_size(bench_priv, i);
		unsigned int type = bench_priv->fn_type_num(bench_priv, idx, i);
		if (pmemobj_alloc(pop,	&pw->oids[i], size, type, NULL, NULL)
									!= 0) {
			perror("pmemobj_alloc");
			goto out;
		}
	}
	return 0;
out:
	for (i--; i >= 0; i--)
		pmemobj_free(&pw->oids[i]);
	free(pw->oids);
	free(pw);
	return -1;
}

/*
 * pobj_direct_op -- main operations of the obj_direct benchmark.
 */
static int
pobj_direct_op(struct benchmark *bench, struct operation_info *info)
{
	struct pobj_bench *bench_priv = pmembench_get_priv(bench);
	struct pobj_worker *pw = info->worker->priv;
	unsigned int idx = bench_priv->obj(info->index);
	if (pmemobj_direct(pw->oids[idx]) == NULL)
		return -1;
	return 0;
}

/*
 * pobj_open_op -- main operations of the obj_open benchmark.
 */
static int
pobj_open_op(struct benchmark *bench, struct operation_info *info)
{
	struct pobj_bench *bench_priv = pmembench_get_priv(bench);
	unsigned int idx = bench_priv->pool(info->worker->index);
	pmemobj_close(bench_priv->pop[idx]);
	bench_priv->pop[idx] = pmemobj_open(bench_priv->sets[idx], LAYOUT_NAME);
	if (bench_priv->pop[idx] == NULL)
		return -1;
	return 0;
}

/*
 * pobj_free_worker -- worker exit function
 */
static int
pobj_free_worker(struct benchmark *bench, struct benchmark_args
					*args, struct worker_info *worker)
{
	struct pobj_worker *pw = worker->priv;
	struct pobj_bench *bench_priv = pmembench_get_priv(bench);
	for (size_t i = 0; i < bench_priv->args_priv->n_objs; i++)
		pmemobj_free(&pw->oids[i]);
	free(pw->oids);
	free(pw);
	return 0;
}

static struct benchmark_info obj_open = {
	.name		= "obj_open",
	.brief		= "pmemobj_open() benchmark",
	.init		= pobj_init,
	.exit		= pobj_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= pobj_init_worker,
	.free_worker	= pobj_free_worker,
	.operation	= pobj_open_op,
	.measure_time	= true,
	.clos		= pobj_open_clo,
	.nclos		= ARRAY_SIZE(pobj_open_clo),
	.opts_size	= sizeof (struct pobj_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(obj_open);

static struct benchmark_info obj_direct = {
	.name		= "obj_direct",
	.brief		= "pmemobj_direct() benchmark",
	.init		= pobj_direct_init,
	.exit		= pobj_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= pobj_init_worker,
	.free_worker	= pobj_free_worker,
	.operation	= pobj_direct_op,
	.measure_time	= true,
	.clos		= pobj_direct_clo,
	.nclos		= ARRAY_SIZE(pobj_direct_clo),
	.opts_size	= sizeof (struct pobj_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(obj_direct);
