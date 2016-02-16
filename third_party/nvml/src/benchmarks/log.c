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
 * log.c -- pmemlog benchmarks definitions
 */

#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "libpmemlog.h"
#include "benchmark.h"

/* size of pool header and pool descriptor */
#define	POOL_HDR_SIZE (2 * 4096)
#define	MIN_VEC_SIZE 1

/*
 * prog_args - benchmark's specific command line arguments
 */
struct prog_args
{
	unsigned int seed;	/* seed for pseudo-random generator */
	bool rand;		/* use random numbers */
	int vec_size;		/* vector size */
	size_t el_size;		/* size of single append */
	size_t min_size;	/* minimum size for random mode */
	bool no_warmup;		/* don't do warmup */
	bool fileio;		/* use file io instead of pmemlog */
};

/*
 * thread_info - thread specific data
 */
struct log_worker_info {
	unsigned int seed;
	struct iovec *iov;		/* io vector */
	char *buf;			/* buffer for write/read operations */
	size_t buf_size;		/* buffer size */
	size_t buf_ptr;			/* pointer for read operations */
	size_t *rand_sizes;
	size_t *vec_sizes;		/* sum of sizes in vector */
};

/*
 * log_bench - main context of benchmark
 */
struct log_bench
{
	size_t psize;		/* size of pool */
	PMEMlogpool *plp;	/* pmemlog handle */
	struct prog_args *args;	/* benchmark specific arguments */
	int fd;			/* file descriptor for file io mode */
	unsigned int seed;
	/*
	 * Pointer to the main benchmark operation. The appropriate function
	 * will be assigned depending on the benchmark specific arguments.
	 */
	int (*func_op) (struct benchmark *, struct operation_info *);
};

/* command line options definition */
static struct benchmark_clo log_clo[] = {
	{
		.opt_short	= 'r',
		.opt_long	= "random",
		.descr		= "Use random sizes for append/read",
		.off		= clo_field_offset(struct prog_args, rand),
		.type		= CLO_TYPE_FLAG
	},
	{
		.opt_short	= 'S',
		.opt_long	= "seed",
		.descr		= "Random mode",
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
	{
		.opt_short	= 'i',
		.opt_long	= "file-io",
		.descr		= "File I/O mode",
		.off		= clo_field_offset(struct prog_args, fileio),
		.type		= CLO_TYPE_FLAG
	},
	{
		.opt_short	= 'w',
		.opt_long	= "no-warmup",
		.descr		= "Don't do warmup",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct prog_args, no_warmup),
	},
	{
		.opt_short	= 'm',
		.opt_long	= "min-size",
		.descr		= "Minimum size of append/read for random mode",
		.type		= CLO_TYPE_UINT,
		.off		= clo_field_offset(struct prog_args, min_size),
		.def		= "1",
		.type_uint	= {
			.size	= clo_field_size(struct prog_args, min_size),
			.base	= CLO_INT_BASE_DEC,
			.min	= 1,
			.max	= UINT64_MAX,
		},
	},
	/* this one is only for log_append */
	{
		.opt_short	= 'v',
		.opt_long	= "vector",
		.descr		= "Vector size",
		.off		= clo_field_offset(struct prog_args, vec_size),
		.def		= "1",
		.type		= CLO_TYPE_INT,
		.type_int	= {
			.size	= clo_field_size(struct prog_args, vec_size),
			.base	= CLO_INT_BASE_DEC,
			.min	= MIN_VEC_SIZE,
			.max	= INT_MAX
		}
	},
};

/*
 * do_warmup -- do warmup by writing the whole pool area
 */
static int
do_warmup(struct log_bench *lb, size_t nops)
{
	int ret = 0;
	size_t bsize = lb->args->vec_size * lb->args->el_size;
	char *buf = malloc(bsize);
	if (!buf) {
		perror("malloc");
		return -1;
	}

	if (!lb->args->fileio) {
		for (size_t i = 0; i < nops; i++) {
			if (pmemlog_append(lb->plp,
				buf, lb->args->el_size) < 0) {
				ret = -1;
				perror("pmemlog_append");
				goto out;
			}
		}

		pmemlog_rewind(lb->plp);

	} else {
		for (size_t i = 0; i < nops; i++) {
			if (write(lb->fd, buf, lb->args->el_size) !=
					lb->args->el_size) {
				ret = -1;
				perror("write");
				close(lb->fd);
				goto out;
			}
		}

		lseek(lb->fd, 0, SEEK_SET);
	}

out:
	free(buf);

	return ret;
}

/*
 * log_append -- performs pmemlog_append operation
 */
static int
log_append(struct benchmark *bench, struct operation_info *info)
{
	struct log_bench *lb = pmembench_get_priv(bench);
	assert(lb);

	struct log_worker_info *worker_info = info->worker->priv;
	assert(worker_info);

	size_t size = lb->args->rand ?
		worker_info->rand_sizes[info->index] :
		lb->args->el_size;

	if (pmemlog_append(lb->plp, worker_info->buf, size) < 0) {
		perror("pmemlog_append");
		return -1;
	}

	return 0;
}

/*
 * log_appendv -- performs pmemlog_appendv operation
 */
static int
log_appendv(struct benchmark *bench, struct operation_info *info)
{
	struct log_bench *lb = pmembench_get_priv(bench);
	assert(lb);

	struct log_worker_info *worker_info = info->worker->priv;
	assert(worker_info);

	struct iovec *iov = &worker_info->iov[info->index * lb->args->vec_size];

	if (pmemlog_appendv(lb->plp, iov, lb->args->vec_size) < 0) {
		perror("pmemlog_appendv");
		return -1;
	}

	return 0;
}

/*
 * fileio_append -- performs fileio append operation
 */
static int
fileio_append(struct benchmark *bench, struct operation_info *info)
{
	struct log_bench *lb = pmembench_get_priv(bench);
	assert(lb);

	struct log_worker_info *worker_info = info->worker->priv;
	assert(worker_info);

	size_t size = lb->args->rand ?
		worker_info->rand_sizes[info->index] :
		lb->args->el_size;

	if (write(lb->fd, worker_info->buf, size) != size) {
		perror("write");
		return -1;
	}

	return 0;
}

/*
 * fileio_appendv -- performs fileio appendv operation
 */
static int
fileio_appendv(struct benchmark *bench, struct operation_info *info)
{
	struct log_bench *lb = (struct log_bench *)pmembench_get_priv(bench);
	assert(lb != NULL);

	struct log_worker_info *worker_info = info->worker->priv;
	assert(worker_info);

	struct iovec *iov = &worker_info->iov[info->index * lb->args->vec_size];
	size_t vec_size = worker_info->vec_sizes[info->index];

	if (writev(lb->fd, iov, lb->args->vec_size) != vec_size) {
		perror("writev");
		return -1;
	}

	return 0;
}

/*
 * log_process_data -- callback function for pmemlog_walk.
 */
static int
log_process_data(const void *buf, size_t len, void *arg)
{
	struct log_worker_info *worker_info = arg;
	size_t left = worker_info->buf_size - worker_info->buf_ptr;
	if (len > left) {
		worker_info->buf_ptr = 0;
		left = worker_info->buf_size;
	}

	len = len < left ? len : left;
	assert(len <= left);

	void *buff = &worker_info->buf[worker_info->buf_ptr];
	memcpy(buff, buf, len);
	worker_info->buf_ptr += len;

	return 1;
}

/*
 * fileio_read -- perform single fileio read
 */
static int
fileio_read(int fd, ssize_t len, struct log_worker_info *worker_info)
{
	ssize_t left = worker_info->buf_size - worker_info->buf_ptr;
	if (len > left) {
		worker_info->buf_ptr = 0;
		left = worker_info->buf_size;
	}

	len = len < left ? len : left;
	assert(len <= left);

	size_t off = worker_info->buf_ptr;
	void *buff = &worker_info->buf[off];
	if ((len = pread(fd, buff, len, off)) < 0)
		return -1;

	worker_info->buf_ptr += len;

	return 1;
}

/*
 * log_read_op -- perform read operation
 */
static int
log_read_op(struct benchmark *bench, struct operation_info *info)
{
	struct log_bench *lb = pmembench_get_priv(bench);
	assert(lb);

	struct log_worker_info *worker_info = info->worker->priv;
	assert(worker_info);

	worker_info->buf_ptr = 0;

	size_t chunk_size = lb->args->rand ?
		worker_info->rand_sizes[info->index] :
		lb->args->el_size;

	if (!lb->args->fileio) {
		pmemlog_walk(lb->plp, chunk_size,
				log_process_data, worker_info);
		return 0;
	}

	int ret;
	while ((ret = fileio_read(lb->fd, chunk_size, worker_info)) == 1)
		;

	return ret;
}

/*
 * log_init_worker -- init benchmark worker
 */
static int
log_init_worker(struct benchmark *bench, struct benchmark_args *args,
		struct worker_info *worker)
{
	int ret = 0;
	struct log_bench *lb = pmembench_get_priv(bench);
	assert(lb);

	struct log_worker_info *worker_info =
		malloc(sizeof (struct log_worker_info));
	if (!worker_info) {
		perror("malloc");
		return -1;
	}

	/* allocate buffer for append / read */
	worker_info->buf_size = lb->args->el_size * lb->args->vec_size;
	worker_info->buf = malloc(worker_info->buf_size);
	if (!worker_info->buf) {
		perror("malloc");
		ret = -1;
		goto err_free_worker_info;
	}

	/*
	 * For random mode, each operation has its own vector with
	 * random sizes. Otherwise there is only one vector with
	 * equal sizes.
	 */
	size_t n_vectors = args->n_ops_per_thread;
	worker_info->iov = malloc(n_vectors * lb->args->vec_size *
			sizeof (struct iovec));
	if (!worker_info->iov) {
		perror("malloc");
		ret = -1;
		goto err_free_buf;
	}

	if (lb->args->rand) {
		/* each thread has random seed */
		worker_info->seed = (unsigned int)rand_r(&lb->seed);

		/* each vector element has its own random size */
		uint64_t n_sizes = args->n_ops_per_thread * lb->args->vec_size;
		worker_info->rand_sizes = malloc(n_sizes *
				sizeof (*worker_info->rand_sizes));
		if (!worker_info->rand_sizes) {
			perror("malloc");
			ret = -1;
			goto err_free_iov;
		}

		/* generate append sizes */
		for (uint64_t i = 0; i < n_sizes; i++) {
			uint32_t hr = (uint32_t)rand_r(&worker_info->seed);
			uint32_t lr = (uint32_t)rand_r(&worker_info->seed);
			uint64_t r64 = (uint64_t)hr << 32 | lr;
			size_t width = lb->args->el_size - lb->args->min_size;
			worker_info->rand_sizes[i] = r64 % width +
				lb->args->min_size;
		}
	} else {
		worker_info->rand_sizes = NULL;
	}

	worker_info->vec_sizes = calloc(args->n_ops_per_thread,
			sizeof (*worker_info->vec_sizes));
	if (!worker_info->vec_sizes) {
		perror("malloc\n");
		ret = -1;
		goto err_free_rand_sizes;
	}

	/* fill up the io vectors */
	size_t i_size = 0;
	for (size_t n = 0; n < args->n_ops_per_thread; n++) {
		size_t buf_ptr = 0;
		size_t vec_off = n * lb->args->vec_size;
		for (int i = 0; i < lb->args->vec_size; ++i) {
			size_t el_size = lb->args->rand ?
				worker_info->rand_sizes[i_size++] :
				lb->args->el_size;

			worker_info->iov[vec_off + i].iov_base =
				&worker_info->buf[buf_ptr];
			worker_info->iov[vec_off + i].iov_len = el_size;

			worker_info->vec_sizes[n] += el_size;

			buf_ptr += el_size;
		}

	}

	worker->priv = worker_info;

	return 0;
err_free_rand_sizes:
	free(worker_info->rand_sizes);
err_free_iov:
	free(worker_info->iov);
err_free_buf:
	free(worker_info->buf);
err_free_worker_info:
	free(worker_info);

	return ret;
}

/*
 * log_free_worker -- cleanup benchmark worker
 */
static int
log_free_worker(struct benchmark *bench, struct benchmark_args *args,
		struct worker_info *worker)
{

	struct log_worker_info *worker_info = worker->priv;
	assert(worker_info);

	free(worker_info->buf);
	free(worker_info->iov);
	free(worker_info->rand_sizes);
	free(worker_info->vec_sizes);
	free(worker_info);

	return 0;
}

/*
 * log_init -- benchmark initialization function
 */
static int
log_init(struct benchmark *bench, struct benchmark_args *args)
{
	int ret = 0;
	assert(bench);
	assert(args != NULL);
	assert(args->opts != NULL);

	struct log_bench *lb = malloc(sizeof (struct log_bench));
	if (!lb) {
		perror("malloc");
		return -1;
	}

	lb->args = args->opts;
	lb->args->el_size = args->dsize;

	if (lb->args->vec_size == 0)
		lb->args->vec_size = 1;

	if (lb->args->rand &&
		lb->args->min_size > lb->args->el_size) {
		errno = EINVAL;
		return -1;
	}

	if (lb->args->rand &&
			lb->args->min_size == lb->args->el_size)
		lb->args->rand = false;

	lb->psize = POOL_HDR_SIZE
		+ args->n_ops_per_thread * args->n_threads
		* lb->args->vec_size * lb->args->el_size;

	/* calculate a required pool size */
	if (lb->psize < PMEMLOG_MIN_POOL)
		lb->psize = PMEMLOG_MIN_POOL;

	struct benchmark_info *bench_info = pmembench_get_info(bench);

	if (!lb->args->fileio) {
		if ((lb->plp = pmemlog_create(args->fname,
			lb->psize, args->fmode)) == NULL) {
			perror("pmemlog_create");
			ret = -1;
			goto err_free_lb;
		}

		bench_info->operation = (lb->args->vec_size > 1) ?
			log_appendv : log_append;
	} else {
		int flags = O_CREAT | O_RDWR | O_SYNC;

		/* Create a file if it does not exist. */
		if ((lb->fd = open(args->fname, flags, args->fmode)) < 0) {
			perror(args->fname);
			ret = -1;
			goto err_free_lb;
		}

		/* allocate the pmem */
		if ((errno = posix_fallocate(lb->fd, 0, lb->psize)) != 0) {
			perror("posix_fallocate");
			ret = -1;
			goto err_close;
		}
		bench_info->operation = (lb->args->vec_size > 1) ?
					fileio_appendv : fileio_append;
	}

	if (!lb->args->no_warmup) {
		size_t warmup_nops = args->n_threads * args->n_ops_per_thread;
		if (do_warmup(lb, warmup_nops)) {
			fprintf(stderr, "warmup failed\n");
			ret = -1;
			goto err_close;
		}
	}

	pmembench_set_priv(bench, lb);

	return 0;

err_close:
	if (lb->args->fileio)
		close(lb->fd);
	else
		pmemlog_close(lb->plp);
err_free_lb:
	free(lb);

	return ret;
}

/*
 * log_exit -- cleanup benchmark
 */
static int
log_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct log_bench *lb = pmembench_get_priv(bench);

	if (!lb->args->fileio)
		pmemlog_close(lb->plp);
	else
		close(lb->fd);

	free(lb);

	return 0;
}

/* log_append benchmark info */
static struct benchmark_info log_append_info = {
	.name		= "log_append",
	.brief		= "Benchmark for pmemlog_append() operation",
	.init		= log_init,
	.exit		= log_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= log_init_worker,
	.free_worker	= log_free_worker,
	.operation	= NULL, /* this will be assigned in log_init */
	.measure_time	= true,
	.clos		= log_clo,
	.nclos		= ARRAY_SIZE(log_clo),
	.opts_size	= sizeof (struct prog_args),
	.rm_file	= true
};

/* log_read benchmark info */
static struct benchmark_info log_read_info = {
	.name		= "log_read",
	.brief		= "Benchmark for pmemlog_walk() operation",
	.init		= log_init,
	.exit		= log_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= log_init_worker,
	.free_worker	= log_free_worker,
	.operation	= log_read_op,
	.measure_time	= true,
	.clos		= log_clo,
	.nclos		= ARRAY_SIZE(log_clo) - 1, /* without vector */
	.opts_size	= sizeof (struct prog_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(log_append_info);
REGISTER_BENCHMARK(log_read_info);
