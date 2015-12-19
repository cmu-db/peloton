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
 * pmem_memset.c -- benchmark for pmem_memset function
 */

#include <libpmem.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <sys/mman.h>

#include "benchmark.h"

#define	MAX_OFFSET 63

struct memset_bench;

typedef size_t (*offset_fn) (struct memset_bench *mb,
					struct operation_info *info);
typedef int (*operation_fn) (void *dest, int c, size_t len);

/*
 * memset_args -- benchmark specific command line options
 */
struct memset_args
{
	char *mode;		/* operation mode: stat, seq, rand */
	bool memset;		/* use libc memset function */
	bool persist;		/* perform persist operation */
	bool no_warmup;		/* do not do warmup */
	size_t chunk_size;	/* elementary chunk size */
	size_t dest_off;	/* destination address offset */
	unsigned int seed;	/* seed for random numbers */
};

/*
 * memset_bench -- benchmark context
 */
struct memset_bench {
	struct memset_args *pargs; /* benchmark specific arguments */
	size_t *randoms;	/* random address offsets */
	int n_randoms;		/* number of random elements */
	int const_b;		/* memset() value */
	size_t fsize;		/* file size */
	int fd;			/* file descriptor */
	int flags;		/* flags for open() */
	void *pmem_addr;	/* mapped file address */
	offset_fn func_dest;	/* destination offset function */
	operation_fn func_op;	/* operation function */
};

struct memset_worker {
};

static struct benchmark_clo memset_clo[] = {
	{
		.opt_short	= 'M',
		.opt_long	= "mem-mode",
		.descr		= "Memory writing mode - stat, seq, rand",
		.def		= "seq",
		.off		= clo_field_offset(struct memset_args, mode),
		.type		= CLO_TYPE_STR,
	},
	{
		.opt_short	= 'm',
		.opt_long	= "memset",
		.descr		= "Use libc memset()",
		.def		= "false",
		.off		= clo_field_offset(struct memset_args, memset),
		.type		= CLO_TYPE_FLAG
	},
	{
		.opt_short	= 'p',
		.opt_long	= "persist",
		.descr		= "Use pmem_persist()",
		.def		= "true",
		.off		= clo_field_offset(struct memset_args, persist),
		.type		= CLO_TYPE_FLAG
	},
	{
		.opt_short	= 'D',
		.opt_long	= "dest-offset",
		.descr		= "Destination cache line alignment offset",
		.def		= "0",
		.off	= clo_field_offset(struct memset_args, dest_off),
		.type		= CLO_TYPE_UINT,
		.type_uint	= {
			.size	= clo_field_size(struct memset_args, dest_off),
			.base	= CLO_INT_BASE_DEC,
			.min	= 0,
			.max	= MAX_OFFSET
		}
	},
	{
		.opt_short	= 'w',
		.opt_long	= "no-warmup",
		.descr		= "Don't do warmup",
		.def		= false,
		.type		= CLO_TYPE_FLAG,
		.off	= clo_field_offset(struct memset_args, no_warmup),
	},
	{
		.opt_short	= 'S',
		.opt_long	= "seed",
		.descr		= "seed for random numbers",
		.def		= "1",
		.off		= clo_field_offset(struct memset_args, seed),
		.type		= CLO_TYPE_UINT,
		.type_uint	= {
			.size	= clo_field_size(struct memset_args, seed),
			.base	= CLO_INT_BASE_DEC,
			.min	= 1,
			.max	= UINT_MAX,
		},
	},
};

/*
 * operation_mode -- mode of operation of memset()
 */
enum operation_mode {
	OP_MODE_UNKNOWN,
	OP_MODE_STAT,	/* always use the same chunk */
	OP_MODE_SEQ,	/* use consecutive chunks */
	OP_MODE_RAND	/* use random chunks */
};

/*
 * parse_op_mode -- parse operation mode from string
 */
static enum operation_mode
parse_op_mode(const char *arg)
{
	if (strcmp(arg, "stat") == 0)
		return OP_MODE_STAT;
	else if (strcmp(arg, "seq") == 0)
		return OP_MODE_SEQ;
	else if (strcmp(arg, "rand") == 0)
		return OP_MODE_RAND;
	else
		return OP_MODE_UNKNOWN;
}

/*
 * mode_seq -- returns chunk index for sequential mode
 */
static uint64_t
mode_seq(struct memset_bench *mb, struct operation_info *info)
{
	return info->worker->index * info->args->n_ops_per_thread
		+ info->index;
}

/*
 * mode_stat -- returns chunk index for static mode
 */
static uint64_t
mode_stat(struct memset_bench *mb, struct operation_info *info)
{
	return info->worker->index * mb->pargs->chunk_size;
}

/*
 * mode_rand -- returns chunk index for random mode
 */
static uint64_t
mode_rand(struct memset_bench *mb, struct operation_info *info)
{
	assert(info->index < mb->n_randoms);
	return info->worker->index * info->args->n_ops_per_thread
					+ mb->randoms[info->index];
}

/*
 * assign_mode_func -- returns offset function based on operation string
 */
static offset_fn
assign_mode_func(const char *option)
{
	enum operation_mode op_mode = parse_op_mode(option);

	switch (op_mode) {
		case OP_MODE_STAT:
			return mode_stat;
		case OP_MODE_SEQ:
			return mode_seq;
		case OP_MODE_RAND:
			return mode_rand;
		default:
			return NULL;
	}
}

/*
 * do_warmup -- does the warmup by writing the whole pool area
 */
static int
do_warmup(struct memset_bench *mb, size_t nops)
{
	void *dest = mb->pmem_addr;
	int c = mb->const_b;
	size_t len = mb->fsize;

	pmem_memset_persist(dest, c, len);

	return 0;
}

/*
 * libpmem_memset_persist -- perform operation using libpmem
 * pmem_memset_persist().
 */
static int
libpmem_memset_persist(void *dest, int c, size_t len)
{
	pmem_memset_persist(dest, c, len);

	return 0;
}

/*
 * libpmem_memset_nodrain -- perform operation using libpmem
 * pmem_memset_nodrain().
 */
static int
libpmem_memset_nodrain(void *dest, int c, size_t len)
{
	pmem_memset_nodrain(dest, c, len);

	return 0;
}

/*
 * libc_memset_persist -- perform operation using libc memset() function
 * followed by pmem_persist().
 */
static int
libc_memset_persist(void *dest, int c, size_t len)
{
	memset(dest, c, len);

	pmem_persist(dest, len);

	return 0;
}

/*
 * libc_memset -- perform operation using libc memset() function
 * followed by pmem_flush().
 */
static int
libc_memset(void *dest, int c, size_t len)
{
	memset(dest, c, len);

	pmem_flush(dest, len);

	return 0;
}

/*
 * memset_op -- actual benchmark operation. It can have one of the four
 * functions assigned:
 *              libc_memset,
 *              libc_memset_persist,
 *              libpmem_memset_nodrain,
 *              libpmem_memset_persist.
 */
static int
memset_op(struct benchmark *bench, struct operation_info *info)
{
	struct memset_bench *mb =
		(struct memset_bench *)pmembench_get_priv(bench);
	uint64_t offset = mb->func_dest(mb, info);
	void *dest = (char *)mb->pmem_addr + offset + mb->pargs->dest_off;
	int c = mb->const_b;
	size_t len = mb->pargs->chunk_size;

	mb->func_op(dest, c, len);

	return 0;
}

/*
 * memset_init -- initialization function
 */
static int
memset_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);
	assert(args->opts != NULL);

	int ret = 0;

	struct memset_bench *mb = malloc(sizeof (struct memset_bench));
	if (!mb) {
		perror("malloc");
		return -1;
	}

	mb->pargs = args->opts;
	mb->pargs->chunk_size = args->dsize;

	enum operation_mode op_mode = parse_op_mode(mb->pargs->mode);
	if (op_mode == OP_MODE_UNKNOWN) {
		fprintf(stderr, "Invalid operation mode argument '%s'",
			mb->pargs->mode);
		ret = -1;
		goto err_free_mb;
	}

	size_t size = MAX_OFFSET + mb->pargs->chunk_size;
	size_t large = size * args->n_ops_per_thread * args->n_threads;

	size_t small = size * args->n_threads;

	mb->fsize = (op_mode == OP_MODE_STAT) ? small : large;

	mb->n_randoms = args->n_ops_per_thread * args->n_threads;
	mb->randoms = malloc(mb->n_randoms * sizeof (*mb->randoms));
	if (!mb->randoms) {
		perror("malloc");
		ret = -1;
		goto err_free_mb;
	}

	unsigned int seed = mb->pargs->seed;
	for (int i = 0; i < mb->n_randoms; i++)
		mb->randoms[i] = rand_r(&seed) % args->n_ops_per_thread;

	mb->flags = O_CREAT | O_EXCL | O_RDWR;

	/* create a pmem file */
	mb->fd = open(args->fname, mb->flags, args->fmode);
	if (mb->fd == -1) {
		perror(args->fname);
		ret = -1;
		goto err_free_randoms;
	}

	/* allocate the pmem */
	if ((errno = posix_fallocate(mb->fd, 0, mb->fsize)) != 0) {
		perror("posix_fallocate");
		ret = -1;
		goto err_close_file;
	}

	/* memory map it */
	mb->pmem_addr = pmem_map(mb->fd);
	if (mb->pmem_addr == NULL) {
		perror("pmem_map");
		ret = -1;
		goto err_close_file;
	}

	/* set proper func_dest() depending on benchmark args */
	mb->func_dest = assign_mode_func(mb->pargs->mode);
	if (mb->func_dest == NULL) {
		fprintf(stderr, "wrong mode parameter -- '%s'",
				mb->pargs->mode);
		ret = -1;
		goto err_unmap;
	}

	if (mb->pargs->memset)
		mb->func_op = (mb->pargs->persist) ?
				libc_memset_persist : libc_memset;
	else
		mb->func_op = (mb->pargs->persist) ?
				libpmem_memset_persist : libpmem_memset_nodrain;

	if (!mb->pargs->no_warmup) {
		if (do_warmup(
			mb, args->n_threads * args->n_ops_per_thread) != 0) {
			fprintf(stderr, "do_warmup() function failed.");
			ret = -1;
			goto err_unmap;
		}
	}

	close(mb->fd);

	pmembench_set_priv(bench, mb);

	return 0;

err_unmap:
	munmap(mb->pmem_addr, mb->fsize);
err_close_file:
	close(mb->fd);
err_free_randoms:
	free(mb->randoms);
err_free_mb:
	free(mb);

	return ret;
}

/*
 * memset_exit -- benchmark cleanup function
 */
static int
memset_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct memset_bench *mb =
		(struct memset_bench *)pmembench_get_priv(bench);
	munmap(mb->pmem_addr, mb->fsize);
	free(mb->randoms);
	free(mb);
	return 0;
}

/* Stores information about benchmark. */
static struct benchmark_info memset_info = {
	.name		= "pmem_memset",
	.brief		= "Benchmark for pmem_memset_persist() and"
				" pmem_memset_nodrain() operations",
	.init		= memset_init,
	.exit		= memset_exit,
	.multithread	= true,
	.multiops	= true,
	.operation	= memset_op,
	.measure_time	= true,
	.clos		= memset_clo,
	.nclos		= ARRAY_SIZE(memset_clo),
	.opts_size	= sizeof (struct memset_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(memset_info);
