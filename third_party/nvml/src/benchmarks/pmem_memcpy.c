/*
 * Copyright (c) 2015, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *	* Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *	* Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *	* Neither the name of Intel Corporation nor the names of its
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
 * pmem_memcpy.c -- benchmark implementation for pmem_memcpy
 */
#include <libpmem.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include <sys/mman.h>

#include "benchmark.h"

#define	FLUSH_ALIGN 64

#define	MAX_OFFSET (FLUSH_ALIGN - 1)

struct pmem_bench;

typedef size_t (*offset_fn) (struct pmem_bench *pmb, uint64_t index);

/*
 * pmem_args -- benchmark specific arguments
 */
struct pmem_args
{
	/*
	 * Defines the copy operation direction. Whether it is
	 * writing from RAM to PMEM (for argument value "write")
	 * or PMEM to RAM (for argument value "read").
	 */
	char *operation;

	/*
	 * The source address offset used to test pmem_memcpy()
	 * performance when source address is not aligned.
	 */
	size_t src_off;

	/*
	 * The destination address offset used to test
	 * pmem_memcpy() performance when destination address
	 * is not aligned.
	 */
	size_t dest_off;

	/* The size of data chunk. */
	size_t chunk_size;

	/*
	 * Specifies the order in which data chunks are selected
	 * to be copied. There are three modes supported:
	 * stat, seq, rand.
	 */
	char *src_mode;

	/*
	 * Specifies the order in which data chunks are written
	 * to the destination address. There are three modes
	 * supported: stat, seq, rand.
	 */
	char *dest_mode;

	/*
	 * When this flag is set to true, PMEM is not used.
	 * This option is useful, when comparing performance
	 * of pmem_memcpy() function to regular memcpy().
	 */
	bool memcpy;

	/*
	 * When this flag is set to true, pmem_persist()
	 * function is used, otherwise pmem_flush() is performed.
	 */
	bool persist;
};

/*
 * pmem_bench -- benchmark context
 */
struct pmem_bench
{
	/* random offsets */
	unsigned int *rand_offsets;

	/* number of elements in randoms array */
	size_t n_rand_offsets;

	/* The size of the allocated PMEM */
	size_t fsize;

	/* The size of the allocated buffer */
	size_t bsize;

	/*
	 * File descriptor used to allocate and memory map the piece
	 * of PMEM.
	 */
	int fd;

	/* Pointer to the allocated volatile memory */
	unsigned char *buf;

	/* Pointer to the allocated PMEM */
	unsigned char *pmem_addr;

	/*
	 * This field gets 'buf' or 'pmem_addr' fields assigned,
	 * depending on the prog_args operation direction.
	 */
	unsigned char *src_addr;

	/*
	 * This field gets 'buf' or 'pmem_addr' fields assigned,
	 * depending on the prog_args operation direction.
	 */
	unsigned char *dest_addr;

	/* Stores prog_args structure */
	struct pmem_args *pargs;

	/*
	 * Function which returns src offset. Matches src_mode.
	 */
	offset_fn func_src;

	/*
	 * Function which returns dst offset. Matches dst_mode.
	 */
	offset_fn func_dest;

	/*
	 * The actual operation performed based on benchmark specific
	 * arguments.
	 */
	int (*func_op) (void *dest, void *source, size_t len);
};

/*
 * operation_type -- type of operation relative to persistent memory
 */
enum operation_type {
	OP_TYPE_UNKNOWN,
	OP_TYPE_READ,
	OP_TYPE_WRITE
};

/*
 * operation_mode -- the mode of the copy process
 *
 *	* static - read/write always the same chunk,
 *	* sequential - read/write chunk by chunk,
 *	* random - read/write to chunks selected randomly.
 *
 *  It is used to determine source mode as well as the destination mode.
 */
enum operation_mode {
	OP_MODE_UNKNOWN,
	OP_MODE_STAT,
	OP_MODE_SEQ,
	OP_MODE_RAND
};

/*
 * parse_op_type -- parses command line "--operation" argument
 * and returns proper operation type.
 */
static enum operation_type
parse_op_type(const char *arg)
{
	if (strcmp(arg, "read") == 0)
		return OP_TYPE_READ;
	else if (strcmp(arg, "write") == 0)
		return OP_TYPE_WRITE;
	else
		return OP_TYPE_UNKNOWN;
}

/*
 * parse_op_mode -- parses command line "--src-mode" or "--dest-mode"
 * and returns proper operation mode.
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

/* structure to define command line arguments */
static struct benchmark_clo pmem_memcpy_clo[] = {
	{
		.opt_short	= 'o',
		.opt_long	= "operation",
		.descr		= "Operation type - write, read",
		.type		= CLO_TYPE_STR,
		.off		= clo_field_offset(struct pmem_args, operation),
		.def		= "write"
	},
	{
		.opt_short	= 'S',
		.opt_long	= "src-offset",
		.descr		= "Source cache line alignment offset",
		.type		= CLO_TYPE_UINT,
		.off		= clo_field_offset(struct pmem_args, src_off),
		.def		= "0",
		.type_uint	= {
			.size	= clo_field_size(struct pmem_args, src_off),
			.base	= CLO_INT_BASE_DEC,
			.min	= 0,
			.max	= MAX_OFFSET
		}
	},
	{
		.opt_short	= 'D',
		.opt_long	= "dest-offset",
		.descr		= "Destination cache line alignment offset",
		.type		= CLO_TYPE_UINT,
		.off		= clo_field_offset(struct pmem_args, dest_off),
		.def		= "0",
		.type_uint	= {
			.size	= clo_field_size(struct pmem_args, dest_off),
			.base	= CLO_INT_BASE_DEC,
			.min	= 0,
			.max	= MAX_OFFSET
		}
	},
	{
		.opt_short	= 0,
		.opt_long	= "src-mode",
		.descr		= "Source reading mode",
		.type		= CLO_TYPE_STR,
		.off		= clo_field_offset(struct pmem_args, src_mode),
		.def		= "seq",
	},
	{
		.opt_short	= 0,
		.opt_long	= "dest-mode",
		.descr		= "Destination writing mode",
		.type		= CLO_TYPE_STR,
		.off		= clo_field_offset(struct pmem_args, dest_mode),
		.def		= "seq",
	},
	{
		.opt_short	= 'm',
		.opt_long	= "libc-memcpy",
		.descr		= "Use libc memcpy()",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct pmem_args, memcpy),
		.def		= "false",
	},
	{
		.opt_short	= 'p',
		.opt_long	= "persist",
		.descr		= "Use pmem_persist()",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct pmem_args, persist),
		.def		= "true"
	}
};

/*
 * mode_seq -- if copy mode is sequential mode_seq() returns
 * index of a chunk.
 */
static uint64_t
mode_seq(struct pmem_bench *pmb, uint64_t index)
{
	return index;
}

/*
 * mode_stat -- if mode is static, the offset is always 0,
 * as only one block is used.
 */
static uint64_t
mode_stat(struct pmem_bench *pmb, uint64_t index)
{
	return 0;
}

/*
 * mode_rand -- if mode is random returns index of a random chunk
 */
static uint64_t
mode_rand(struct pmem_bench *pmb, uint64_t index)
{
	assert(index < pmb->n_rand_offsets);
	return pmb->rand_offsets[index];
}

/*
 * assign_mode_func -- parses "--src-mode" and "--dest-mode" command line
 * arguments and returns one of the above mode functions.
 */
static offset_fn
assign_mode_func(char *option)
{
	enum operation_mode op_mode = parse_op_mode(option);

	switch (op_mode) {
		case OP_MODE_STAT: return mode_stat;
		case OP_MODE_SEQ: return mode_seq;
		case OP_MODE_RAND: return mode_rand;
		default: return NULL;
	}
}


/*
 * libc_memcpy -- copy using libc memcpy() function
 * followed by pmem_flush().
 */
static int
libc_memcpy(void *dest, void *source, size_t len)
{
	memcpy(dest, source, len);

	pmem_flush(dest, len);

	return 0;
}

/*
 * libc_memcpy_persist -- copy using libc memcpy() function
 * followed by pmem_persist().
 */
static int
libc_memcpy_persist(void *dest, void *source, size_t len)
{
	memcpy(dest, source, len);

	pmem_persist(dest, len);

	return 0;
}

/*
 * lipmem_memcpy_nodrain -- copy using libpmem pmem_memcpy_no_drain()
 * function without pmem_persist().
 */
static int
libpmem_memcpy_nodrain(void *dest, void *source, size_t len)
{
	pmem_memcpy_nodrain(dest, source, len);

	return 0;
}

/*
 * libpmem_memcpy_persist -- copy using libpmem pmem_memcpy_persist() function.
 */
static int
libpmem_memcpy_persist(void *dest, void *source, size_t len)
{
	pmem_memcpy_persist(dest, source, len);

	return 0;
}

/*
 * assign_size -- assigns file and buffer size
 * depending on the operation mode and type.
 */
static int
assign_size(struct pmem_bench *pmb, struct benchmark_args *args,
		enum operation_type *op_type)
{
	*op_type = parse_op_type(pmb->pargs->operation);

	if (*op_type == OP_TYPE_UNKNOWN) {
		fprintf(stderr, "Invalid operation argument '%s'",
			pmb->pargs->operation);
		return -1;
	}
	enum operation_mode op_mode_src =
			parse_op_mode(pmb->pargs->src_mode);
	if (op_mode_src == OP_MODE_UNKNOWN) {
		fprintf(stderr, "Invalid source mode argument '%s'",
			pmb->pargs->src_mode);
		return -1;
	}
	enum operation_mode op_mode_dest =
			parse_op_mode(pmb->pargs->dest_mode);
	if (op_mode_dest == OP_MODE_UNKNOWN) {
		fprintf(stderr, "Invalid destination mode argument '%s'",
			pmb->pargs->dest_mode);
		return -1;
	}

	size_t large = args->n_ops_per_thread
				* pmb->pargs->chunk_size * args->n_threads;
	size_t small = pmb->pargs->chunk_size;

	if (*op_type == OP_TYPE_WRITE) {
		pmb->bsize = op_mode_src == OP_MODE_STAT ? small : large;
		pmb->fsize = op_mode_dest == OP_MODE_STAT ? small : large;

		if (pmb->pargs->src_off != 0)
			pmb->bsize += MAX_OFFSET;
		if (pmb->pargs->dest_off != 0)
			pmb->fsize += MAX_OFFSET;
	} else {
		pmb->fsize = op_mode_src == OP_MODE_STAT ? small : large;
		pmb->bsize = op_mode_dest == OP_MODE_STAT ? small : large;

		if (pmb->pargs->src_off != 0)
			pmb->fsize += MAX_OFFSET;
		if (pmb->pargs->dest_off != 0)
			pmb->bsize += MAX_OFFSET;
	}

	return 0;
}

/*
 * pmem_memcpy_init -- benchmark initialization
 *
 * Parses command line arguments, allocates persistent memory, and maps it.
 */
static int
pmem_memcpy_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);
	int ret = 0;

	struct pmem_bench *pmb = malloc(sizeof (struct pmem_bench));
	assert(pmb != NULL);

	pmb->pargs = args->opts;
	assert(pmb->pargs != NULL);

	pmb->pargs->chunk_size = args->dsize;

	enum operation_type op_type;
	/*
	 * Assign file and buffer size depending on the operation type
	 * (READ from PMEM or WRITE to PMEM)
	 */
	if (assign_size(pmb, args, &op_type) != 0) {
		ret = -1;
		goto err_free_pmb;
	}

	if ((errno = posix_memalign(
		(void **) &pmb->buf, FLUSH_ALIGN, pmb->bsize)) != 0) {
		perror("posix_memalign");
		ret = -1;
		goto err_free_pmb;
	}

	pmb->n_rand_offsets = args->n_ops_per_thread * args->n_threads;
	pmb->rand_offsets = malloc(pmb->n_rand_offsets *
			sizeof (*pmb->rand_offsets));

	for (size_t i = 0; i < pmb->n_rand_offsets; ++i)
		pmb->rand_offsets[i] = rand() % args->n_ops_per_thread;


	/* create a pmem file */
	pmb->fd = open(args->fname, O_CREAT|O_EXCL|O_RDWR, args->fmode);
	if (pmb->fd == -1) {
		perror(args->fname);
		ret = -1;
		goto err_free_buf;
	}

	/* allocate the pmem */
	if ((errno = posix_fallocate(pmb->fd, 0, pmb->fsize)) != 0) {
		perror("posix_fallocate");
		ret = -1;
		goto err_close_file;
	}
	/* memory map it */
	if ((pmb->pmem_addr = pmem_map(pmb->fd)) == NULL) {
		perror("pmem_map");
		ret = -1;
		goto err_close_file;
	}


	if (op_type == OP_TYPE_READ) {
		pmb->src_addr = pmb->pmem_addr;
		pmb->dest_addr = pmb->buf;
	} else {
		pmb->src_addr = pmb->buf;
		pmb->dest_addr = pmb->pmem_addr;
	}

	/* set proper func_src() and func_dest() depending on benchmark args */
	if ((pmb->func_src = assign_mode_func(pmb->pargs->src_mode)) == NULL) {
		fprintf(stderr, "wrong src_mode parameter -- '%s'",
						pmb->pargs->src_mode);
		ret = -1;
		goto err_unmap;
	}

	if ((pmb->func_dest = assign_mode_func(pmb->pargs->dest_mode))
								== NULL) {
		fprintf(stderr, "wrong dest_mode parameter -- '%s'",
						pmb->pargs->dest_mode);
		ret = -1;
		goto err_unmap;
	}

	close(pmb->fd);

	if (pmb->pargs->memcpy) {
		pmb->func_op = pmb->pargs->persist ?
					libc_memcpy_persist : libc_memcpy;
	} else {
		pmb->func_op = pmb->pargs->persist ?
			libpmem_memcpy_persist : libpmem_memcpy_nodrain;
	}

	pmembench_set_priv(bench, pmb);

	return 0;
err_unmap:
	munmap(pmb->pmem_addr, pmb->fsize);
err_close_file:
	close(pmb->fd);
err_free_buf:
	free(pmb->buf);
err_free_pmb:
	free(pmb);

	return ret;
}

/*
 * pmem_memcpy_operation -- actual benchmark operation
 *
 * Depending on the memcpy flag "-m" tested operation will be memcpy()
 * or pmem_memcpy_persist().
 */
static int
pmem_memcpy_operation(struct benchmark *bench, struct operation_info *info)
{
	struct pmem_bench *pmb = (struct pmem_bench *)pmembench_get_priv(bench);

	uint64_t src_index =
		info->args->n_ops_per_thread * info->worker->index
			+ pmb->func_src(pmb, info->index);

	uint64_t dest_index =
		info->args->n_ops_per_thread * info->worker->index
			+ pmb->func_dest(pmb, info->index);

	void *source = pmb->src_addr
		+ src_index * pmb->pargs->chunk_size
		+ pmb->pargs->src_off;
	void *dest =  pmb->dest_addr
		+ dest_index * pmb->pargs->chunk_size
		+ pmb->pargs->dest_off;
	size_t len = pmb->pargs->chunk_size;

	pmb->func_op(dest, source, len);
	return 0;
}

/*
 * pmem_memcpy_exit -- benchmark cleanup
 */
static int
pmem_memcpy_exit(struct benchmark *bench, struct benchmark_args  *args)
{
	struct pmem_bench *pmb = (struct pmem_bench *)pmembench_get_priv(bench);
	munmap(pmb->pmem_addr, pmb->fsize);
	free(pmb->buf);
	free(pmb->rand_offsets);
	free(pmb);
	return 0;
}

/* Stores information about benchmark. */
static struct benchmark_info pmem_memcpy = {
	.name		= "pmem_memcpy",
	.brief		= "Benchmark for pmem_memcpy_persist() and "
				"pmem_memcpy_nodrain() operations",
	.init		= pmem_memcpy_init,
	.exit		= pmem_memcpy_exit,
	.multithread	= true,
	.multiops	= true,
	.operation	= pmem_memcpy_operation,
	.measure_time	= true,
	.clos		= pmem_memcpy_clo,
	.nclos		= ARRAY_SIZE(pmem_memcpy_clo),
	.opts_size	= sizeof (struct pmem_args),
	.rm_file	= true,
};

REGISTER_BENCHMARK(pmem_memcpy);
