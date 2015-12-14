/*
 * Copyright (c) 2015, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *	* Redistributions of source code must retain the above copyright
 *	  notice, this list of conditions and the following disclaimer.
 *
 *	* Redistributions in binary form must reproduce the above copyright
 *	  notice, this list of conditions and the following disclaimer in
 *	  the documentation and/or other materials provided with the
 *	  distribution.
 *
 *	* Neither the name of Intel Corporation nor the names of its
 *	  contributors may be used to endorse or promote products derived
 *	  from this software without specific prior written permission.
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
 *
 */

/*
 *
 * vmem.c -- vmem_malloc, vmem_free and vmem_realloc multithread benchmarks
 *
 */

#include "benchmark.h"
#include <libvmem.h>
#include <assert.h>
#include <sys/stat.h>

#define	DIR_MODE 0700
#define	MAX_POOLS 8
#define	FACTOR 2
#define	RRAND(max, min) (rand() % ((max) - (min)) + (min))

struct vmem_bench;
typedef int (*operation) (struct vmem_bench *vb, unsigned int worker_idx,
							unsigned int info_idx);

/*
 * vmem_args -- additional properties set as arguments opts
 */
struct vmem_args
{
	bool stdlib_alloc;	/* use stdlib allocator instead of vmem */
	bool no_warmup;		/* do not perform warmup */
	bool pool_per_thread;	/* create single pool per thread */
	int min_size;		/* size of min allocation in range mode */
	size_t rsize;		/* size of reallocation */
	int min_rsize;		/* size of min reallocation in range mode */

	/* perform operation on object allocated by other thread */
	bool mix;
};

/*
 * item -- structure representing single allocated object
 */
struct item
{
	void *buf;			/* buffer for operations */

	/* number of pool to which object is assigned */
	unsigned int pool_num;
};

/*
 * vmem_worker -- additional properties set as worker private
 */
struct vmem_worker
{
	/* array to store objects used in operations performed by worker */
	struct item *objs;
	unsigned int pool_number;	/* number of pool used by worker */
};

/*
 * vmem_bench -- additional properties set as benchmark private
 */
struct vmem_bench
{
	VMEM **pools;			/* handle for VMEM pools */
	struct vmem_worker *workers;	/* array with private workers data */
	size_t pool_size;		/* size of each pool */
	unsigned int npools;		/* number of created pools */
	unsigned int *alloc_sizes;	/* array with allocation sizes */
	unsigned int *realloc_sizes;	/* array with reallocation sizes */
	unsigned int *mix_ops;		/* array with random indexes */
	bool rand_alloc;		/* use range mode in allocation */
	bool rand_realloc;		/* use range mode in reallocation */
	int lib_mode;			/* library mode - vmem or stdlib */
};

static struct benchmark_clo vmem_clo[] = {
	{
		.opt_short	= 'a',
		.opt_long	= "stdlib-alloc",
		.descr		= "Use stdlib allocator",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct vmem_args,
							stdlib_alloc),
	},
	{
		.opt_short	= 'w',
		.opt_long	= "no-warmup",
		.descr		= "Do not perform warmup",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct vmem_args, no_warmup)
	},
	{
		.opt_short	= 'p',
		.opt_long	= "pool-per-thread",
		.descr		= "Create separate pool per thread",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct vmem_args,
							pool_per_thread),
	},
	{
		.opt_short	= 'm',
		.opt_long	= "alloc-min",
		.type		= CLO_TYPE_INT,
		.descr		= "Min allocation size",
		.off		= clo_field_offset(struct vmem_args,
							min_size),
		.def		= "-1",
		.type_int	= {
			.size	= clo_field_size(struct vmem_args, min_size),
			.base	= CLO_INT_BASE_DEC,
			.min	= (-1),
			.max	= INT_MAX,
		},
	},
	/*
	 * number of command line arguments is decremented to make below
	 * options available only for vmem_free and vmem_realloc benchmark
	 */
	{
		.opt_short	= 'T',
		.opt_long	= "mix-thread",
		.descr		= "Reallocate object allocated by another"
								"thread",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct vmem_args, mix),
	},
	/*
	 * number of command line arguments is decremented to make below
	 * options available only for vmem_realloc benchmark
	 */
	{
		.opt_short	= 'r',
		.opt_long	= "realloc-size",
		.type		= CLO_TYPE_UINT,
		.descr		= "Reallocation size",
		.off		= clo_field_offset(struct vmem_args, rsize),
		.def		= "512",
		.type_uint	= {
			.size	= clo_field_size(struct vmem_args, rsize),
			.base	= CLO_INT_BASE_DEC,
			.min	= 0,
			.max	= ~0,
		},
	},
	{
		.opt_short	= 'R',
		.opt_long	= "realloc-min",
		.type		= CLO_TYPE_INT,
		.descr		= "Min reallocation size",
		.off		= clo_field_offset(struct vmem_args,
							min_rsize),
		.def		= "-1",
		.type_int	= {
			.size	= clo_field_size(struct vmem_args,
								min_rsize),
			.base	= CLO_INT_BASE_DEC,
			.min	= -1,
			.max	= INT_MAX,
		},
	},

};

/*
 * lib_mode -- enumeration used to determine mode of the benchmark
 */
enum lib_mode {
	VMEM_MODE,
	STDLIB_MODE
};

/*
 * vmem_malloc_op -- malloc operation using vmem
 */
static int
vmem_malloc_op(struct vmem_bench *vb, unsigned int worker_idx,
							unsigned int info_idx)
{
	struct item *item = &vb->workers[worker_idx].objs[info_idx];
	item->buf = vmem_malloc(vb->pools[item->pool_num],
						vb->alloc_sizes[info_idx]);
	if (item->buf == NULL) {
		perror("vmem_malloc");
		return -1;
	}
	return 0;
}

/*
 * stdlib_malloc_op -- malloc operation using stdlib
 */
static int
stdlib_malloc_op(struct vmem_bench *vb, unsigned int worker_idx,
							unsigned int info_idx)
{
	struct item *item = &vb->workers[worker_idx].objs[info_idx];
	item->buf = malloc(vb->alloc_sizes[info_idx]);
	if (item->buf == NULL) {
		perror("malloc");
		return -1;
	}
	return 0;
}

/*
 * vmem_free_op -- free operation using vmem
 */
static int
vmem_free_op(struct vmem_bench *vb, unsigned int worker_idx,
							unsigned int info_idx)
{
	struct item *item = &vb->workers[worker_idx].objs[info_idx];
	if (item->buf != NULL)
		vmem_free(vb->pools[item->pool_num], item->buf);
	item->buf = NULL;
	return 0;
}

/*
 * stdlib_free_op -- free operation using stdlib
 */
static int
stdlib_free_op(struct vmem_bench *vb, unsigned int worker_idx,
							unsigned int info_idx)
{
	struct item *item = &vb->workers[worker_idx].objs[info_idx];
	if (item->buf != NULL)
		free(item->buf);
	item->buf = NULL;
	return 0;
}

/*
 * vmem_realloc_op -- realloc operation using vmem
 */
static int
vmem_realloc_op(struct vmem_bench *vb, unsigned int worker_idx,
							unsigned int info_idx)
{
	struct item *item = &vb->workers[worker_idx].objs[info_idx];
	item->buf = vmem_realloc(vb->pools[item->pool_num], item->buf,
						vb->realloc_sizes[info_idx]);
	if (vb->realloc_sizes[info_idx] != 0 && item->buf == NULL) {
		perror("vmem_realloc");
		return -1;
	}
	return 0;
}

/*
 * stdlib_realloc_op -- realloc operation using stdlib
 */
static int
stdlib_realloc_op(struct vmem_bench *vb, unsigned int worker_idx,
							unsigned int info_idx)
{
	struct item *item = &vb->workers[worker_idx].objs[info_idx];
	item->buf = realloc(item->buf, vb->realloc_sizes[info_idx]);
	if (vb->realloc_sizes[info_idx] != 0 && item->buf == NULL) {
		perror("realloc");
		return -1;
	}
	return 0;
}

static operation malloc_op[2] = {vmem_malloc_op, stdlib_malloc_op};
static operation free_op[2] = {vmem_free_op, stdlib_free_op};
static operation realloc_op[2] = {vmem_realloc_op, stdlib_realloc_op};

/*
 * vmem_create_pools -- use vmem_create to create pools
 */
static int
vmem_create_pools(struct vmem_bench *vb, struct benchmark_args *args)
{
	int i;
	struct vmem_args *va = args->opts;
	size_t dsize = args->dsize + va->rsize;
	vb->pool_size = dsize * args->n_ops_per_thread
			* args->n_threads / vb->npools;
	vb->pools = calloc(vb->npools, sizeof (VMEM *));
	if (vb->pools == NULL) {
		perror("calloc");
		return -1;
	}
	if (vb->pool_size < VMEM_MIN_POOL * args->n_threads)
		vb->pool_size = VMEM_MIN_POOL * args->n_threads;

	/* multiply pool size to prevent out of memory error  */
	vb->pool_size *= FACTOR;
	for (i = 0; i < vb->npools; i++) {
		vb->pools[i] = vmem_create(args->fname, vb->pool_size);
		if (vb->pools[i] == NULL) {
			perror("vmem_create");
			goto err;
		}
	}
	return 0;
err:
	for (int j = i - 1; j >= 0; j--)
		vmem_delete(vb->pools[j]);
	free(vb->pools);
	return -1;
}

/*
 * random_values -- calculates values for random sizes
 */
static void
random_values(unsigned int *alloc_sizes, struct benchmark_args *args,
						size_t max, size_t min)
{
	unsigned int i;
	if (args->seed != 0)
		srand(args->seed);

	for (i = 0; i < args->n_ops_per_thread; i++)
		alloc_sizes[i] = RRAND(max - min, min);
}

/*
 * static_values -- fulls array with the same value
 */
static void
static_values(unsigned int *alloc_sizes, size_t dsize, unsigned int nops)
{
	unsigned int i;
	for (i = 0; i < nops; i++)
		alloc_sizes[i] = dsize;
}

/*
 * vmem_do_warmup -- perform warm-up by malloc and free for every thread
 */
static int
vmem_do_warmup(struct vmem_bench *vb, struct benchmark_args *args)
{
	int i, j, ret = 0;
	for (i = 0; i < args->n_threads; i++) {
		for (j = 0; j < args->n_ops_per_thread; j++) {
			if (malloc_op[vb->lib_mode](vb, i, j) != 0) {
				ret = -1;
				fprintf(stderr, "warmup failed");
				break;
			}
		}

		for (j--; j >= 0; j--)
			free_op[vb->lib_mode](vb, i, j);
	}
	return ret;
}

/*
 * malloc_main_op -- main operations for vmem_malloc benchmark
 */
static int
malloc_main_op(struct benchmark *bench, struct operation_info *info)
{
	struct vmem_bench *vb = pmembench_get_priv(bench);
	return malloc_op[vb->lib_mode](vb, info->worker->index, info->index);
}

/*
 * free_main_op -- main operations for vmem_free benchmark
 */
static int
free_main_op(struct benchmark *bench, struct operation_info *info)
{
	struct vmem_bench *vb = pmembench_get_priv(bench);
	return free_op[vb->lib_mode](vb, info->worker->index, info->index);
}

/*
 * realloc_main_op -- main operations for vmem_realloc benchmark
 */
static int
realloc_main_op(struct benchmark *bench, struct operation_info *info)
{
	struct vmem_bench *vb = pmembench_get_priv(bench);
	return realloc_op[vb->lib_mode](vb, info->worker->index, info->index);
}

/*
 * vmem_mix_op -- main operations for vmem_mix benchmark
 */
static int
vmem_mix_op(struct benchmark *bench, struct operation_info *info)
{
	struct vmem_bench *vb = pmembench_get_priv(bench);
	unsigned int idx = vb->mix_ops[info->index];
	free_op[vb->lib_mode](vb, info->worker->index, idx);
	return malloc_op[vb->lib_mode](vb, info->worker->index, idx);
}

/*
 * vmem_init_worker_alloc -- initialize worker for vmem_free and
 * vmem_realloc benchmark when mix flag set to false
 */
static int
vmem_init_worker_alloc(struct vmem_bench *vb, struct benchmark_args *args,
					struct worker_info *worker)
{
	size_t i;
	for (i = 0; i < args->n_ops_per_thread; i++) {
		if (malloc_op[vb->lib_mode](vb, worker->index, i) != 0)
			goto out;
	}
	return 0;
out:
	for (int j = i - 1; j >= 0; j--)
		free_op[vb->lib_mode](vb, worker->index, i);
	return -1;
}

/*
 * vmem_init_worker_alloc_mix -- initialize worker for vmem_free and
 * vmem_realloc benchmark when mix flag set to true
 */
static int
vmem_init_worker_alloc_mix(struct vmem_bench *vb, struct benchmark_args *args,
					struct worker_info *worker)
{
	int i = 0, j = 0, idx = 0;
	size_t ops_per_thread = args->n_ops_per_thread / args->n_threads;
	for (i = 0; i < args->n_threads; i++) {
		for (j = 0; j < ops_per_thread; j++) {
			idx = ops_per_thread * worker->index + j;
			vb->workers[i].objs[idx].pool_num =
					vb->workers[i].pool_number;
			if (malloc_op[vb->lib_mode](vb, i, idx) != 0)
				goto out;
		}
	}
	for (idx = ops_per_thread * args->n_threads;
				idx < args->n_ops_per_thread; idx ++) {
		if (malloc_op[vb->lib_mode](vb, worker->index, idx) != 0)
			goto out_ops;
	}
	return 0;
out_ops:
	for (idx--; idx >= ops_per_thread; idx--)
		free_op[vb->lib_mode](vb, worker->index, idx);
out:

	for (i--; i >= 0; i--) {
		for (j--; j >= 0; j--) {
			idx = ops_per_thread * worker->index + j;
			free_op[vb->lib_mode](vb, i, idx);
		}
	}
	return -1;
}

/*
 * vmem_init_worker_alloc_mix -- initialize worker for vmem_free and
 * vmem_realloc benchmark
 */
static int
vmem_init_worker(struct benchmark *bench, struct benchmark_args *args,
					struct worker_info *worker)
{
	struct vmem_args *va = args->opts;
	struct vmem_bench *vb = pmembench_get_priv(bench);
	int ret = va->mix ? vmem_init_worker_alloc_mix(vb, args, worker) :
				vmem_init_worker_alloc(vb, args, worker);
	return ret;
}

/*
 * vmem_exit -- function for de-initialization benchmark
 */
static int
vmem_exit(struct benchmark *bench, struct benchmark_args *args)
{
	int i;
	struct vmem_bench *vb = pmembench_get_priv(bench);
	struct vmem_args *va = args->opts;
	if (!va->stdlib_alloc) {
		for (i = 0; i < vb->npools; i++) {
			vmem_delete(vb->pools[i]);
		}
		free(vb->pools);
	}
	for (i = 0; i < args->n_threads; i++)
		free(vb->workers[i].objs);
	free(vb->workers);
	free(vb->alloc_sizes);
	if (vb->realloc_sizes != NULL)
		free(vb->realloc_sizes);
	if (vb->mix_ops != NULL)
		free(vb->mix_ops);
	free(vb);
	return 0;
}

/*
 * vmem_exit_free -- frees worker with freeing elements
 */
static int
vmem_exit_free(struct benchmark *bench, struct benchmark_args *args)
{
	unsigned int i, j;
	struct vmem_bench *vb = pmembench_get_priv(bench);
	for (i = 0; i < args->n_threads; i++) {
		for (j = 0; j < args->n_ops_per_thread; j++) {
			free_op[vb->lib_mode](vb, i, j);
		}
	}
	return vmem_exit(bench, args);
}

/*
 * vmem_init -- function for initialization benchmark
 */
static int
vmem_init(struct benchmark *bench, struct benchmark_args *args)
{
	int i, j;
	assert(bench != NULL);
	assert(args != NULL);

	struct vmem_bench *vb = calloc(1, sizeof (struct vmem_bench));
	if (vb == NULL) {
		perror("malloc");
		return -1;
	}
	pmembench_set_priv(bench, vb);
	struct vmem_worker *vw;
	struct vmem_args *va = args->opts;
	vb->alloc_sizes = NULL;
	vb->lib_mode = va->stdlib_alloc ? STDLIB_MODE : VMEM_MODE;

	if (!va->stdlib_alloc && mkdir(args->fname, DIR_MODE) != 0)
		goto err;

	vb->npools = va->pool_per_thread ? args->n_threads : 1;

	vb->rand_alloc = va->min_size != -1;
	if (vb->rand_alloc && va->min_size > args->dsize) {
		fprintf(stderr, "invalid allocation size\n");
		goto err;
	}

	/* vmem library is enable to create limited number of pools */
	if (va->pool_per_thread && args->n_threads > MAX_POOLS) {
		fprintf(stderr, "Maximum number of threads is %d for"
				"pool-per-thread option\n", MAX_POOLS);
		goto err;
	}

	/* initializes buffers for operations for every thread */
	vb->workers = calloc(args->n_threads, sizeof (struct vmem_worker));
	if (vb->workers == NULL) {
		perror("calloc");
		goto err;
	}
	for (i = 0; i < args->n_threads; i++) {
		vw = &vb->workers[i];
		vw->objs = calloc(args->n_ops_per_thread,
						sizeof (struct item));
		if (vw->objs == NULL) {
			perror("calloc");
			goto err_free_workers;
		}

		vw->pool_number = va->pool_per_thread ? i : 0;
		for (j = 0; j < args->n_ops_per_thread; j++)
			vw->objs[j].pool_num = vw->pool_number;
	}

	if ((vb->alloc_sizes = malloc(sizeof (unsigned int)
			* args->n_ops_per_thread)) == NULL) {
		perror("malloc");
		goto err_free_buf;
	}
	if (vb->rand_alloc)
		random_values(vb->alloc_sizes, args, args->dsize, va->min_size);
	else
		static_values(vb->alloc_sizes, args->dsize,
						args->n_ops_per_thread);

	if (!va->stdlib_alloc && vmem_create_pools(vb, args) != 0)
			goto err_free_sizes;

	if (!va->no_warmup && vmem_do_warmup(vb, args) != 0)
		goto err_free_all;

	return 0;

err_free_all:
	if (!va->stdlib_alloc) {
		for (i = 0; i < vb->npools; i++)
			vmem_delete(vb->pools[i]);
		free(vb->pools);
	}
err_free_sizes:
	free(vb->alloc_sizes);
err_free_buf:
	for (j = i - 1; j >= 0; j--)
		free(vb->workers[j].objs);
err_free_workers:
	free(vb->workers);
err:
	free(vb);
	return -1;
}

/*
 * vmem_realloc_init -- function for initialization vmem_realloc benchmark
 */
static int
vmem_realloc_init(struct benchmark *bench, struct benchmark_args *args)
{
	if (vmem_init(bench, args) != 0)
		return -1;

	struct vmem_bench *vb = pmembench_get_priv(bench);
	struct vmem_args *va = args->opts;
	vb->rand_realloc = va->min_rsize != -1;

	if (vb->rand_realloc && va->min_rsize > va->rsize) {
		fprintf(stderr, "invalid reallocation size\n");
		goto err;
	}
	if ((vb->realloc_sizes = calloc(args->n_ops_per_thread,
				sizeof (unsigned int))) == NULL) {
		perror("calloc");
		goto err;
	}
	if (vb->rand_realloc)
		random_values(vb->realloc_sizes, args, va->rsize,
							va->min_rsize);
	else
		static_values(vb->realloc_sizes, va->rsize,
						args->n_ops_per_thread);
	return 0;
err:
	vmem_exit(bench, args);
	return -1;
}

/*
 * vmem_mix_init -- function for initialization vmem_realloc benchmark
 */
static int
vmem_mix_init(struct benchmark *bench, struct benchmark_args *args)
{
	if (vmem_init(bench, args) != 0)
		return -1;

	unsigned int i, idx, tmp;
	struct vmem_bench *vb = pmembench_get_priv(bench);
	if ((vb->mix_ops = calloc(args->n_ops_per_thread,
				sizeof (unsigned int))) == NULL) {
		perror("calloc");
		goto err;
	}
	for (i = 0; i < args->n_ops_per_thread; i++)
		vb->mix_ops[i] = i;

	if (args->seed != 0)
		srand(args->seed);

	for (i = 1; i < args->n_ops_per_thread; i++) {
		idx = RRAND(args->n_ops_per_thread - 1, 0);
		tmp = vb->mix_ops[idx];
		vb->mix_ops[i] = vb->mix_ops[idx];
		vb->mix_ops[idx] = tmp;
	}
	return 0;
err:
	vmem_exit(bench, args);
	return -1;
}

static struct benchmark_info vmem_malloc_bench = {

	.name		= "vmem_malloc",
	.brief		= "vmem_malloc() benchmark",
	.init		= vmem_init,
	.exit		= vmem_exit_free,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= NULL,
	.free_worker	= NULL,
	.operation	= malloc_main_op,
	.clos		= vmem_clo,
	.nclos		= ARRAY_SIZE(vmem_clo) - 3,
	.opts_size	= sizeof (struct vmem_args),
	.rm_file	= true,

};
REGISTER_BENCHMARK(vmem_malloc_bench);

static struct benchmark_info vmem_mix_bench = {

	.name		= "vmem_mix",
	.brief		= "vmem_malloc() and vmem_free() bechmark",
	.init		= vmem_mix_init,
	.exit		= vmem_exit_free,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= vmem_init_worker,
	.free_worker	= NULL,
	.operation	= vmem_mix_op,
	.clos		= vmem_clo,
	.nclos		= ARRAY_SIZE(vmem_clo) - 3,
	.opts_size	= sizeof (struct vmem_args),
	.rm_file	= true,

};
REGISTER_BENCHMARK(vmem_mix_bench);

static struct benchmark_info vmem_free_bench = {

	.name		= "vmem_free",
	.brief		= "vmem_free() benchmark",
	.init		= vmem_init,
	.exit		= vmem_exit,
	.multithread    = true,
	.multiops	= true,
	.init_worker    = vmem_init_worker,
	.free_worker    = NULL,
	.operation	= free_main_op,
	.clos		= vmem_clo,
	.nclos		= ARRAY_SIZE(vmem_clo) - 2,
	.opts_size	= sizeof (struct vmem_args),
	.rm_file	= true,

};
REGISTER_BENCHMARK(vmem_free_bench);

static struct benchmark_info vmem_realloc_bench = {

	.name		= "vmem_realloc",
	.brief		= "Multithread benchmark vmem - realloc",
	.init		= vmem_realloc_init,
	.exit		= vmem_exit_free,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= vmem_init_worker,
	.free_worker	= NULL,
	.operation	= realloc_main_op,
	.clos		= vmem_clo,
	.nclos		= ARRAY_SIZE(vmem_clo),
	.opts_size	= sizeof (struct vmem_args),
	.rm_file	= true,

};
REGISTER_BENCHMARK(vmem_realloc_bench);
