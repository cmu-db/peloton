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
 * blk.c -- pmemblk benchmarks definitions
 */

#include "libpmemblk.h"
#include "benchmark.h"
#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <argp.h>
#include <errno.h>

struct blk_bench;
struct blk_worker;

/*
 * typedef for the worker function
 */
typedef int (*worker_fn)(struct blk_bench *, struct benchmark_args *,
		struct blk_worker *, off_t);

/*
 * blk_args -- benchmark specific arguments
 */
struct blk_args {
	bool file_io;		/* use file-io */
	unsigned int fsize;	/* file size */
	bool no_warmup;		/* don't do warmup */
	unsigned int seed;	/* seed for randomization */
	bool rand;		/* random blocks */
};

/*
 * blk_bench -- pmemblk benchmark context
 */
struct blk_bench {
	PMEMblkpool *pbp;		/* pmemblk handle */
	int fd;				/* file descr. for file io */
	size_t nblocks;			/* number of blocks */
	size_t blocks_per_thread;	/* number of blocks per thread */
	worker_fn worker;		/* worker function */
};

/*
 * struct blk_worker -- pmemblk worker context
 */
struct blk_worker {
	off_t *blocks;			/* array with block numbers */
	unsigned char *buff;		/* buffer for read/write */
	unsigned int seed;		/* worker seed */
};

static struct benchmark_clo blk_clo[] = {
	{
		.opt_short	= 'i',
		.opt_long	= "file-io",
		.descr		= "File I/O mode",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct blk_args, file_io),
		.def		= "false",
	},
	{
		.opt_short	= 'w',
		.opt_long	= "no-warmup",
		.descr		= "Don't do warmup",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct blk_args, no_warmup),
	},
	{
		.opt_short	= 'r',
		.opt_long	= "random",
		.descr		= "Use random sizes for append/read",
		.off		= clo_field_offset(struct blk_args, rand),
		.type		= CLO_TYPE_FLAG
	},
	{
		.opt_short	= 'S',
		.opt_long	= "seed",
		.descr		= "Random mode",
		.off		= clo_field_offset(struct blk_args, seed),
		.def		= "1",
		.type		= CLO_TYPE_UINT,
		.type_uint	= {
			.size	= clo_field_size(struct blk_args, seed),
			.base	= CLO_INT_BASE_DEC,
			.min	= 1,
			.max	= UINT_MAX,
		}
	},
	{
		.opt_short	= 's',
		.opt_long	= "file-size",
		.descr		= "File size in bytes - 0 means minimum",
		.type		= CLO_TYPE_UINT,
		.off		= clo_field_offset(struct blk_args,
						fsize),
		.def		= "0",
		.type_uint	= {
			.size	= clo_field_size(struct blk_args, fsize),
			.base	= CLO_INT_BASE_DEC,
			.min	= 0,
			.max	= ~0,
		},
	},
};

/*
 * blk_do_warmup -- perform warm-up by writing to each block
 */
static int
blk_do_warmup(struct blk_bench *bb, struct benchmark_args *args)
{
	struct blk_args *ba = args->opts;
	size_t lba;
	char buff[args->dsize];
	for (lba = 0; lba < bb->nblocks; ++lba) {
		if (ba->file_io) {
			size_t off = lba * args->dsize;
			if (pwrite(bb->fd, buff, args->dsize, off)
					!= args->dsize) {
				perror("pwrite");
				return -1;
			}
		} else {
			if (pmemblk_write(bb->pbp, buff, lba) < 0) {
				perror("pmemblk_write");
				return -1;
			}
		}
	}
	return 0;
}

/*
 * blk_read -- read function for pmemblk
 */
static int
blk_read(struct blk_bench *bb, struct benchmark_args *ba,
		struct blk_worker *bworker, off_t off)
{
	if (pmemblk_read(bb->pbp, bworker->buff, off) < 0) {
		perror("pmemblk_read");
		return -1;
	}
	return 0;
}

/*
 * fileio_read -- read function for file io
 */
static int
fileio_read(struct blk_bench *bb, struct benchmark_args *ba,
		struct blk_worker *bworker, off_t off)
{
	off_t file_off = off * ba->dsize;
	if (pread(bb->fd, bworker->buff, ba->dsize, file_off)
					!= ba->dsize) {
		perror("pread");
		return -1;
	}
	return 0;
}

/*
 * blk_write -- write function for pmemblk
 */
static int
blk_write(struct blk_bench *bb, struct benchmark_args *ba,
		struct blk_worker *bworker, off_t off)
{
	if (pmemblk_write(bb->pbp, bworker->buff, off) < 0) {
		perror("pmemblk_write");
		return -1;
	}
	return 0;
}

/*
 * fileio_write -- write function for file io
 */
static int
fileio_write(struct blk_bench *bb, struct benchmark_args *ba,
		struct blk_worker *bworker, off_t off)
{
	off_t file_off = off * ba->dsize;
	if (pwrite(bb->fd, bworker->buff, ba->dsize, file_off) != ba->dsize) {
		perror("pwrite");
		return -1;
	}
	return 0;
}

/*
 * blk_operation -- main operations for blk_read and blk_write benchmark
 */
static int
blk_operation(struct benchmark *bench, struct operation_info *info)
{
	struct blk_bench *bb = pmembench_get_priv(bench);
	struct blk_worker *bworker = info->worker->priv;

	off_t off = bworker->blocks[info->index];
	return bb->worker(bb, info->args, bworker, off);
}

/*
 * blk_init_worker -- initialize worker
 */
static int
blk_init_worker(struct benchmark *bench, struct benchmark_args *args,
		struct worker_info *worker)
{
	struct blk_worker *bworker = malloc(sizeof (*bworker));
	if (!bworker) {
		perror("malloc");
		return -1;
	}

	struct blk_bench *bb = pmembench_get_priv(bench);
	struct blk_args *bargs = args->opts;

	bworker->seed = rand_r(&bargs->seed);

	bworker->buff = malloc(args->dsize);
	if (!bworker->buff) {
		perror("malloc");
		goto err_buff;
	}

	/* fill buffer with some random data */
	memset(bworker->buff, bworker->seed, args->dsize);

	bworker->blocks = malloc(sizeof (bworker->blocks) *
			args->n_ops_per_thread);
	if (!bworker->blocks) {
		perror("malloc");
		goto err_blocks;
	}

	if (bargs->rand) {
		for (size_t i = 0; i < args->n_ops_per_thread; i++) {
			bworker->blocks[i] =
				worker->index * bb->blocks_per_thread +
				rand_r(&bworker->seed) % bb->blocks_per_thread;
		}
	} else {
		for (size_t i = 0; i < args->n_ops_per_thread; i++)
			bworker->blocks[i] = i;
	}

	worker->priv = bworker;
	return 0;
err_blocks:
	free(bworker->buff);
err_buff:
	free(bworker);

	return -1;
}

/*
 * blk_free_worker -- cleanup worker
 */
static int
blk_free_worker(struct benchmark *bench, struct benchmark_args *args,
		struct worker_info *worker)
{
	struct blk_worker *bworker = worker->priv;
	free(bworker->blocks);
	free(bworker->buff);
	free(bworker);

	return 0;
}

/*
 * blk_init -- function for initialization benchmark
 */
static int
blk_init(struct blk_bench *bb, struct benchmark_args *args)
{
	struct blk_args *ba = args->opts;
	assert(ba != NULL);

	if (ba->fsize == 0)
		ba->fsize = PMEMBLK_MIN_POOL;

	if (ba->fsize / args->dsize < args->n_threads ||
		ba->fsize < PMEMBLK_MIN_POOL) {
		fprintf(stderr, "too small file size\n");
		return -1;
	}

	if (args->dsize >= ba->fsize) {
		fprintf(stderr, "block size bigger than file size\n");
		return -1;
	}

	bb->fd = -1;
	/*
	 * Create pmemblk in order to get the number of blocks
	 * even for file-io mode.
	 */
	bb->pbp = pmemblk_create(args->fname, args->dsize,
			ba->fsize, args->fmode);
	if (bb->pbp == NULL) {
		perror("pmemblk_create");
	}

	bb->nblocks = pmemblk_nblock(bb->pbp);

	if (bb->nblocks < args->n_threads) {
		fprintf(stderr, "too small file size");
		goto out_close;
	}

	if (ba->file_io) {
		pmemblk_close(bb->pbp);
		bb->pbp = NULL;
		int flags = O_RDWR | O_CREAT | O_SYNC;
		bb->fd = open(args->fname, flags, args->fmode);
		if (bb->fd < 0) {
			perror("open");
			return -1;
		}
	}

	bb->blocks_per_thread = bb->nblocks / args->n_threads;

	if (!ba->no_warmup) {
		if (blk_do_warmup(bb, args) != 0)
			goto out_close;
	}

	return 0;
out_close:
	if (ba->file_io)
		close(bb->fd);
	else
		pmemblk_close(bb->pbp);
	return -1;
}

/*
 * blk_read_init - function for initializing blk_read benchmark
 */
static int
blk_read_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);

	int ret;
	struct blk_args *ba = args->opts;
	struct blk_bench *bb = malloc(sizeof (struct blk_bench));
	if (bb == NULL) {
		perror("malloc");
		return -1;
	}

	pmembench_set_priv(bench, bb);

	if (ba->file_io)
		bb->worker = fileio_read;
	else
		bb->worker = blk_read;

	ret = blk_init(bb, args);
	if (ret != 0)
		free(bb);

	return ret;
}

/*
 * blk_write_init - function for initializing blk_write benchmark
 */
static int
blk_write_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);

	int ret;
	struct blk_args *ba = args->opts;
	struct blk_bench *bb = malloc(sizeof (struct blk_bench));
	if (bb == NULL) {
		perror("malloc");
		return -1;
	}

	pmembench_set_priv(bench, bb);

	if (ba->file_io)
		bb->worker = fileio_write;
	else
		bb->worker = blk_write;

	ret = blk_init(bb, args);

	if (ret != 0)
		free(bb);

	return ret;
}

/*
 * blk_exit -- function for de-initialization benchmark
 */
static int
blk_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct blk_bench *bb = pmembench_get_priv(bench);
	struct blk_args *ba = args->opts;

	if (ba->file_io) {
		close(bb->fd);
	} else {
		pmemblk_close(bb->pbp);
		int result = pmemblk_check(args->fname, args->dsize);
		if (result < 0) {
			perror("pmemblk_check error");
			return -1;
		} else if (result == 0) {
			perror("pmemblk_check: not consistent");
			return -1;
		}
	}

	free(bb);
	return 0;
}

static struct benchmark_info blk_read_info = {

	.name		= "blk_read",
	.brief		= "Benchmark for blk_read() operation",
	.init		= blk_read_init,
	.exit		= blk_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= blk_init_worker,
	.free_worker	= blk_free_worker,
	.operation	= blk_operation,
	.clos		= blk_clo,
	.nclos		= ARRAY_SIZE(blk_clo),
	.opts_size	= sizeof (struct blk_args),
	.rm_file	= true,

};

REGISTER_BENCHMARK(blk_read_info);

static struct benchmark_info blk_write_info = {

	.name		= "blk_write",
	.brief		= "Benchmark for blk_write() operation",
	.init		= blk_write_init,
	.exit		= blk_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= blk_init_worker,
	.free_worker	= blk_free_worker,
	.operation	= blk_operation,
	.clos		= blk_clo,
	.nclos		= ARRAY_SIZE(blk_clo),
	.opts_size	= sizeof (struct blk_args),
	.rm_file	= true,

};

REGISTER_BENCHMARK(blk_write_info);
