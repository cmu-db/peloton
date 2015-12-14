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
 * obj_pmalloc.c -- pmalloc benchmarks definition
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#include "libpmemobj.h"
#include "benchmark.h"
#include "pmalloc.h"

/*
 * The factor used for PMEM pool size calculation, accounts for metadata,
 * fragmentation and etc.
 */
#define	FACTOR 8

/* The minimum allocation size that pmalloc can perform */
#define	ALLOC_MIN_SIZE 64

/* OOB and allocation header size */
#define	OOB_HEADER_SIZE 64

/*
 * prog_args - command line parsed arguments
 */
struct prog_args {
	size_t minsize;		/* minimum size for random allocation size */
	bool use_random_size;	/* if set, use random size allocations */
	unsigned int seed;	/* PRNG seed */
};

POBJ_LAYOUT_BEGIN(pmalloc_layout);
POBJ_LAYOUT_ROOT(pmalloc_layout, struct my_root);
POBJ_LAYOUT_TOID(pmalloc_layout, uint64_t);
POBJ_LAYOUT_END(pmalloc_layout);

/*
 * my_root - root object
 */
struct my_root {
	TOID(uint64_t) offs;	/* vector of the allocated object offsets */
};

/*
 * obj_bench - variables used in benchmark, passed within functions
 */
struct obj_bench {
	PMEMobjpool *pop;		/* persistent pool handle */
	struct prog_args *pa;		/* prog_args structure */
	size_t *sizes;			/* sizes for allocations */
	TOID(struct my_root) root;	/* root object's OID */
	uint64_t *offs;			/* pointer to the vector of offsets */
};

/*
 * obj_init -- common part of the benchmark initialization for pmalloc and
 * pfree. It allocates the PMEM memory pool and the necessary offset vector.
 */
static int
obj_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);
	assert(args->opts != NULL);

	if (((struct prog_args *)(args->opts))->minsize >= args->dsize) {
		fprintf(stderr, "Wrong params - allocation size\n");
		return -1;
	}

	struct obj_bench *ob = malloc(sizeof (struct obj_bench));
	if (ob == NULL) {
		perror("malloc");
		return -1;
	}
	pmembench_set_priv(bench, ob);

	ob->pa = args->opts;

	uint64_t n_ops_total = args->n_ops_per_thread * args->n_threads;

	/* Create pmemobj pool. */
	size_t alloc_size = args->dsize;
	if (alloc_size < ALLOC_MIN_SIZE)
		alloc_size = ALLOC_MIN_SIZE;

	/* For data objects */
	size_t poolsize = n_ops_total * (alloc_size + OOB_HEADER_SIZE)
		/* for offsets */
		+ n_ops_total * sizeof (uint64_t);

	/* multiply by FACTOR for metadata, fragmentation, etc. */
	poolsize = poolsize * FACTOR;
	if (poolsize < PMEMOBJ_MIN_POOL)
		poolsize = PMEMOBJ_MIN_POOL;

	ob->pop = pmemobj_create(args->fname, POBJ_LAYOUT_NAME(pmalloc_layout),
			poolsize, args->fmode);
	if (ob->pop == NULL) {
		fprintf(stderr, "%s\n", pmemobj_errormsg());
		goto free_ob;
	}

	ob->root = POBJ_ROOT(ob->pop, struct my_root);
	if (TOID_IS_NULL(ob->root)) {
		fprintf(stderr, "POBJ_ROOT: %s\n", pmemobj_errormsg());
		goto free_pop;
	}

	POBJ_ZALLOC(ob->pop, &D_RW(ob->root)->offs, uint64_t,
			n_ops_total * sizeof (PMEMoid));
	if (TOID_IS_NULL(D_RW(ob->root)->offs)) {
		fprintf(stderr, "POBJ_ZALLOC off_vect: %s\n",
			pmemobj_errormsg());
		goto free_pop;
	}

	ob->offs = D_RW(D_RW(ob->root)->offs);

	ob->sizes = malloc(n_ops_total * sizeof (size_t));
	if (ob->sizes == NULL) {
		fprintf(stderr, "malloc rand size vect err\n");
		goto free_pop;
	}

	if (ob->pa->use_random_size) {
		size_t width = args->dsize - ob->pa->minsize;
		for (size_t i = 0; i < n_ops_total; i++) {
			uint32_t hr = (uint32_t)rand_r(&ob->pa->seed);
			uint32_t lr = (uint32_t)rand_r(&ob->pa->seed);
			uint64_t r64 = (uint64_t)hr << 32 | lr;
			ob->sizes[i] = r64 % width + ob->pa->minsize;
		}
	} else {
		for (size_t i = 0; i < n_ops_total; i++)
			ob->sizes[i] = args->dsize;
	}

	return 0;

free_pop:
	pmemobj_close(ob->pop);

free_ob:
	free(ob);
	return -1;
}

/*
 * obj_exit -- common part for the exit function for pmalloc and pfree
 * benchmarks. It frees the allocated offset vector and the memory pool.
 */
static int
obj_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct obj_bench *ob = pmembench_get_priv(bench);

	free(ob->sizes);

	POBJ_FREE(&D_RW(ob->root)->offs);
	pmemobj_close(ob->pop);

	return 0;
}

/*
 * pmalloc_init -- initialization for the pmalloc benchmark. Performs only the
 * common initialization.
 */
static int
pmalloc_init(struct benchmark *bench, struct benchmark_args *args)
{
	return obj_init(bench, args);
}

/*
 * pmalloc_op -- actual benchmark operation. Performs the pmalloc allocations.
 */
static int
pmalloc_op(struct benchmark *bench, struct operation_info *info)
{
	struct obj_bench *ob = pmembench_get_priv(bench);

	unsigned i = info->index + info->worker->index *
					info->args->n_ops_per_thread;

	int ret = pmalloc(ob->pop, &ob->offs[i], ob->sizes[i], 0);
	if (ret) {
		fprintf(stderr, "pmalloc ret: %d\n", ret);
		return ret;
	}

	return 0;
}

/*
 * pmalloc_exit -- the end of the pmalloc benchmark. Frees the memory allocated
 * during pmalloc_op and performs the common exit operations.
 */
static int
pmalloc_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct obj_bench *ob = pmembench_get_priv(bench);

	for (size_t i = 0; i < args->n_ops_per_thread * args->n_threads; i++) {
		if (ob->offs[i])
			pfree(ob->pop, &ob->offs[i], 0);
	}

	return obj_exit(bench, args);
}

/*
 * pfree_init -- initialization for the pfree benchmark. Performs the common
 * initialization and allocates the memory to be freed during pfree_op.
 */
static int
pfree_init(struct benchmark *bench, struct benchmark_args *args)
{
	int ret = obj_init(bench, args);
	if (ret)
		return ret;

	struct obj_bench *ob = pmembench_get_priv(bench);

	for (size_t i = 0; i < args->n_ops_per_thread * args->n_threads; i++) {
		ret = pmalloc(ob->pop, &ob->offs[i], ob->sizes[i], 0);
		if (ret) {
			fprintf(stderr, "pmalloc at idx %lu ret: %s\n", i,
				pmemobj_errormsg());
			/* free the allocated memory */
			while (i != 0) {
				pfree(ob->pop, &ob->offs[i - 1], 0);
				i--;
			}
			obj_exit(bench, args);
			return ret;
		}
	}

	return 0;
}

/*
 * pmalloc_op -- actual benchmark operation. Performs the pfree operation.
 */
static int
pfree_op(struct benchmark *bench, struct operation_info *info)
{
	struct obj_bench *ob = pmembench_get_priv(bench);

	unsigned i = info->index + info->worker->index *
					info->args->n_ops_per_thread;

	int ret = pfree(ob->pop, &ob->offs[i], 0);
	if (ret) {
		fprintf(stderr, "pfree ret: %d\n", ret);
		return ret;
	}

	return 0;
}

/* command line options definition */
static struct benchmark_clo pmalloc_clo[] = {
	{
		.opt_short	= 'r',
		.opt_long	= "random",
		.descr		= "Use random size allocations - from min-size"
					" to data-size",
		.off		= clo_field_offset(struct prog_args,
							use_random_size),
		.type		= CLO_TYPE_FLAG
	},
	{
		.opt_short	= 'm',
		.opt_long	= "min-size",
		.descr		= "Minimum size of allocation for random mode",
		.type		= CLO_TYPE_UINT,
		.off		= clo_field_offset(struct prog_args, minsize),
		.def		= "1",
		.type_uint	= {
			.size	= clo_field_size(struct prog_args, minsize),
			.base	= CLO_INT_BASE_DEC,
			.min	= 1,
			.max	= UINT64_MAX,
		},
	},
	{
		.opt_short	= 'S',
		.opt_long	= "seed",
		.descr		= "Random mode seed value",
		.off		= clo_field_offset(struct prog_args, seed),
		.def		= "1",
		.type		= CLO_TYPE_UINT,
		.type_uint	= {
			.size	= clo_field_size(struct prog_args, seed),
			.base	= CLO_INT_BASE_DEC,
			.min	= 1,
			.max	= UINT_MAX,
		}
	},
};

/*
 * Stores information about pmalloc benchmark.
 */
static struct benchmark_info pmalloc_info = {
	.name		= "pmalloc",
	.brief		= "Benchmark for internal pmalloc() operation",
	.init		= pmalloc_init,
	.exit		= pmalloc_exit,
	.multithread	= true,
	.multiops	= true,
	.operation	= pmalloc_op,
	.measure_time	= true,
	.clos		= pmalloc_clo,
	.nclos		= ARRAY_SIZE(pmalloc_clo),
	.opts_size	= sizeof (struct prog_args),
	.rm_file	= true
};

/*
 * Stores information about pfree benchmark.
 */
static struct benchmark_info pfree_info = {
	.name		= "pfree",
	.brief		= "Benchmark for internal pfree() operation",
	.init		= pfree_init,
	.exit		= pmalloc_exit, /* same as for pmalloc */
	.multithread	= true,
	.multiops	= true,
	.operation	= pfree_op,
	.measure_time	= true,
	.clos		= pmalloc_clo,
	.nclos		= ARRAY_SIZE(pmalloc_clo),
	.opts_size	= sizeof (struct prog_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(pmalloc_info);
REGISTER_BENCHMARK(pfree_info);
