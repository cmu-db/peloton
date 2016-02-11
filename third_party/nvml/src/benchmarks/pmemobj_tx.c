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
 * pmemobj_tx.c -- pmemobj_tx_alloc(), pmemobj_tx_free(), pmemobj_tx_realloc(),
 * pmemobj_tx_add_range() benchmarks.
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

#define	LAYOUT_NAME "benchmark"
#define	FACTOR 16
/*
 * operations number is limited to prevent stack overflow during
 * performing recursive functions.
 */
#define	MAX_OPS 10000

TOID_DECLARE(struct item, 0);

struct obj_tx_bench;
struct obj_tx_worker;

int obj_tx_init(struct benchmark *bench, struct benchmark_args *args);
int obj_tx_exit(struct benchmark *bench, struct benchmark_args *args);

typedef int (*fn_type_num) (struct obj_tx_bench *obj_bench, int worker_idx,
								int op_idx);

typedef unsigned int (*fn_num) (unsigned int idx);

typedef int (*fn_op) (struct obj_tx_bench *obj_bench,
				struct worker_info *worker, unsigned int idx);

typedef struct offset (*fn_off) (struct obj_tx_bench *obj_bench,
							unsigned int idx);

typedef enum op_mode (*fn_parse) (const char *arg);

/*
 * obj_tx_args -- stores command line parsed arguments.
 */
struct obj_tx_args {

	/*
	 * operation which will be performed when flag io set to false.
	 *	modes for obj_tx_alloc, obj_tx_free and obj_tx_realloc:
	 *		- basic - transaction will be committed
	 *		- abort - 'external' transaction will be aborted.
	 *		- abort-nested - all nested transactions will be
	 *		  aborted.
	 *
	 *	modes for  obj_tx_add_range benchmark:
	 *		- basic - one object is added to undo log many times in
	 *		  one transaction.
	 *		- range - fields of one object are added to undo
	 *		  log many times in one transaction.
	 *		- all-obj - all objects are added to undo log in
	 *		  one transaction.
	 *		- range-nested - fields of one object are added to undo
	 *		  log many times in many nested transactions.
	 *		- one-obj-nested - one object is added to undo log many
	 *		  times in many nested transactions.
	 *		- all-obj-nested - all objects are added to undo log in
	 *		  many separate, nested transactions.
	 */
	char *operation;

	/*
	 * type number for each persistent object. There are three modes:
	 *		- one - all of objects have the same type number
	 *		- per-thread - all of object allocated by the same
	 *		  thread have the same type number
	 *		- rand - type numbers are assigned randomly for
	 *		  each persistent object
	 */
	char *type_num;

	/*
	 * define s which library will be used in main operations There are
	 * three modes in which benchmark can be run:
	 *		- tx - uses PMEM transactions
	 *		- pmem - uses PMEM without transactions
	 *		- dram - does not use PMEM
	 */
	char *lib;
	unsigned int nested;	/* number of nested transactions */
	unsigned int min_size;	/* minimum allocation size */
	unsigned int min_rsize;	/* minimum reallocation size */
	unsigned int rsize;	/* reallocation size */
	bool change_type;	/* change type number in reallocation */
	size_t obj_size;	/* size of each allocated object */
	size_t n_ops;		/* number of operations */
	int parse_mode;		/* type of parsing function */
};

/*
 * obj_tx_bench -- stores variables used in benchmark, passed within functions.
 */
struct obj_tx_bench {
	PMEMobjpool *pop;	/* handle to persistent pool */
	struct obj_tx_args *obj_args;	/* pointer to benchmark arguments */
	size_t *random_types;	/* array to store random type numbers */
	size_t *sizes;	/* array to store size of each allocation */
	size_t *resizes;	/* array to store size of each reallocation */
	size_t n_objs;		/* number of objects to allocate */
	int type_mode;		/* type number mode */
	int op_mode;		/* type of operation */
	int lib_mode;		/* type of operation used in initialization */
	int lib_op;	/* type of main operation */
	int nesting_mode;	/* type of nesting in main operation */
	fn_num n_oid;		/* returns object's number in array */
	fn_off fn_off;		/* returns offset for proper operation */

	/*
	 * fn_type_num gets proper function assigned, depending on the
	 * value of the type_mode argument, which returns proper type number for
	 * each persistent object. Possible functions are:
	 *	- type_mode_one,
	 *	- type_mode_rand.
	 */
	fn_type_num fn_type_num;

	/*
	 * fn_op gets proper array with functions pointer assigned, depending on
	 * function which is tested by benchmark. Possible arrays are:
	 *	-alloc_op
	 *	-free_op
	 *	-realloc_op
	 */
	fn_op *fn_op;
} obj_bench;

/*
 * item -- TOID's structure
 */
struct item;

/*
 * obj_tx_worker - stores variables used by one thread.
 */
struct obj_tx_worker {
	TOID(struct item) *oids;
	char **items;
	unsigned int tx_level;
	unsigned int max_level;
};

/*
 * offset - stores offset data used in pmemobj_tx_add_range()
 */
struct offset
{
	uint64_t off;
	size_t size;
};

/* Array defining common command line arguments. */
static struct benchmark_clo obj_tx_clo[] = {
	{
		.opt_short	= 'T',
		.opt_long	= "type-num",
		.descr		= "Type number - one, rand, per-thread",
		.def		= "one",
		.type		= CLO_TYPE_STR,
		.off		= clo_field_offset(struct obj_tx_args,
								type_num),
	},
	{
		.opt_short	= 'O',
		.opt_long	= "operation",
		.descr		= "Type of operation",
		.def		= "basic",
		.off		= clo_field_offset(struct obj_tx_args,
								operation),
		.type		= CLO_TYPE_STR,
	},
	{
		.opt_short	= 'm',
		.opt_long	= "min-size",
		.type		= CLO_TYPE_UINT,
		.descr		= "Minimum allocation size",
		.off		= clo_field_offset(struct obj_tx_args,
						min_size),
		.def		= "0",
		.type_uint	= {
			.size	= clo_field_size(struct obj_tx_args,
						min_size),
			.base	= CLO_INT_BASE_DEC|CLO_INT_BASE_HEX,
			.min	= 0,
			.max	= UINT_MAX,
		},
	},
	/*
	 * nclos field in benchmark_info structures is decremented to make this
	 * options available only for obj_tx_alloc, obj_tx_free and
	 * obj_tx_realloc benchmarks.
	 */
	{
		.opt_short	= 'L',
		.opt_long	= "lib",
		.descr		= "Type of library",
		.def		= "tx",
		.off		= clo_field_offset(struct obj_tx_args, lib),
		.type		= CLO_TYPE_STR,
	},
	{
		.opt_short	= 'N',
		.opt_long	= "nestings",
		.type		= CLO_TYPE_UINT,
		.descr		= "Number of nested transactions",
		.off		= clo_field_offset(struct obj_tx_args,
								nested),
		.def		= "0",
		.type_uint	= {
			.size	= clo_field_size(struct obj_tx_args,
								nested),
			.base	= CLO_INT_BASE_DEC|CLO_INT_BASE_HEX,
			.min	= 0,
			.max	= MAX_OPS,
		},
	},
	/*
	 * nclos field in benchmark_info structures is decremented to make this
	 * options available only for obj_tx_realloc benchmarks.
	 */
	{
		.opt_short	= 'r',
		.opt_long	= "min-rsize",
		.type		= CLO_TYPE_UINT,
		.descr		= "Minimum reallocation size",
		.off		= clo_field_offset(struct obj_tx_args,
						min_rsize),
		.def		= "0",
		.type_uint	= {
			.size	= clo_field_size(struct obj_tx_args,
						min_rsize),
			.base	= CLO_INT_BASE_DEC|CLO_INT_BASE_HEX,
			.min	= 0,
			.max	= UINT_MAX,
		},
	},
	{
		.opt_short	= 'R',
		.opt_long	= "realloc-size",
		.type		= CLO_TYPE_UINT,
		.descr		= "Reallocation size",
		.off		= clo_field_offset(struct obj_tx_args,
								rsize),
		.def		= "1",
		.type_uint	= {
			.size	= clo_field_size(struct obj_tx_args,
								rsize),
			.base	= CLO_INT_BASE_DEC|CLO_INT_BASE_HEX,
			.min	= 1,
			.max	= ULONG_MAX,
		},
	},
	{
		.opt_short	= 'c',
		.opt_long	= "changed-type",
		.descr		= "Use another type number in reallocation"
				"than in allocation",
		.type		= CLO_TYPE_FLAG,
		.off		= clo_field_offset(struct obj_tx_args,
								change_type),
	},
};

/*
 * type_num_mode -- type number mode
 */
enum type_num_mode {
	NUM_MODE_ONE,
	NUM_MODE_PER_THREAD,
	NUM_MODE_RAND,
	NUM_MODE_UNKNOWN
};

/*
 * op_mode -- operation type
 */
enum op_mode {
	OP_MODE_COMMIT,
	OP_MODE_ABORT,
	OP_MODE_ABORT_NESTED,
	OP_MODE_ONE_OBJ,
	OP_MODE_ONE_OBJ_NESTED,
	OP_MODE_ONE_OBJ_RANGE,
	OP_MODE_ONE_OBJ_NESTED_RANGE,
	OP_MODE_ALL_OBJ,
	OP_MODE_ALL_OBJ_NESTED,
	OP_MODE_UNKNOWN
};

/*
 * lib_mode -- operation type
 */
enum lib_mode {
	LIB_MODE_DRAM,
	LIB_MODE_OBJ_TX,
	LIB_MODE_OBJ_ATOMIC,
	LIB_MODE_UNKNOWN,
};

/*
 * nesting_mode -- nesting type
 */
enum nesting_mode {
	NESTING_MODE_SIM,
	NESTING_MODE_TX,
	NESTING_MODE_UNKNOWN,
};

/*
 * add_range_mode -- operation type for obj_add_range benchmark
 */
enum add_range_mode {
	ADD_RANGE_MODE_ONE_TX,
	ADD_RANGE_MODE_NESTED_TX
};

/*
 * parse_mode -- parsing function type
 */
enum parse_mode {
	PARSE_OP_MODE,
	PARSE_OP_MODE_ADD_RANGE
};

/*
 * alloc_dram -- main operations for obj_tx_alloc benchmark in dram mode
 */
static int
alloc_dram(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	struct obj_tx_worker *obj_worker = worker->priv;
	obj_worker->items[idx] = malloc(obj_bench->sizes[idx]);
	if (obj_worker->items[idx] == NULL) {
		perror("malloc");
		return -1;
	}
	return 0;
}

/*
 * alloc_pmem -- main operations for obj_tx_alloc benchmark in pmem mode
 */
static int
alloc_pmem(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	int type_num = obj_bench->fn_type_num(obj_bench, worker->index, idx);
	struct obj_tx_worker *obj_worker = worker->priv;
	if (pmemobj_alloc(obj_bench->pop, &obj_worker->oids[idx].oid,
			obj_bench->sizes[idx], type_num, NULL, NULL) != 0) {
		perror("pmemobj_alloc");
		return -1;
	}
	return 0;
}


/*
 * alloc_tx -- main operations for obj_tx_alloc benchmark in tx mode
 */
static int
alloc_tx(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	int type_num = obj_bench->fn_type_num(obj_bench, worker->index, idx);
	struct obj_tx_worker *obj_worker = worker->priv;
	obj_worker->oids[idx].oid = pmemobj_tx_alloc(obj_bench->sizes[idx],
								type_num);
	if (OID_IS_NULL(obj_worker->oids[idx].oid)) {
		perror("pmemobj_tx_alloc");
		return -1;
	}
	return 0;
}

/*
 * free_dram -- main operations for obj_tx_free benchmark in dram mode
 */
static int
free_dram(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	struct obj_tx_worker *obj_worker = worker->priv;
	free(obj_worker->items[idx]);
	return 0;
}

/*
 * free_pmem -- main operations for obj_tx_free benchmark in pmem mode
 */
static int
free_pmem(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	struct obj_tx_worker *obj_worker = worker->priv;
	if (!TOID_IS_NULL(obj_worker->oids[idx]))
		POBJ_FREE(&obj_worker->oids[idx]);
	return 0;
}

/*
 * free_tx -- main operations for obj_tx_free benchmark in tx mode
 */
static int
free_tx(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	struct obj_tx_worker *obj_worker = worker->priv;
	TX_FREE(obj_worker->oids[idx]);
	return 0;
}

/*
 * realloc_dram -- main operations for obj_tx_realloc benchmark in dram mode
 */
static int
realloc_dram(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	struct obj_tx_worker *obj_worker = worker->priv;
	char *tmp = realloc(obj_worker->items[idx],
					obj_bench->resizes[idx]);
	if (tmp == NULL) {
		perror("realloc");
		return -1;
	}
	obj_worker->items[idx] = tmp;
	return 0;
}

/*
 * realloc_pmem -- main operations for obj_tx_realloc benchmark in pmem mode
 */
static int
realloc_pmem(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	struct obj_tx_worker *obj_worker = worker->priv;
	int type_num = obj_bench->fn_type_num(obj_bench, worker->index, idx);
	if (obj_bench->obj_args->change_type)
		type_num++;
	if (pmemobj_realloc(obj_bench->pop, &obj_worker->oids[idx].oid,
			obj_bench->resizes[idx], type_num) != 0) {
		perror("pmemobj_realloc");
		return -1;
	}
	return 0;
}

/*
 * realloc_tx -- main operations for obj_tx_realloc benchmark in tx mode
 */
static int
realloc_tx(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	struct obj_tx_worker *obj_worker = worker->priv;
	int type_num = obj_bench->fn_type_num(obj_bench, worker->index, idx);
	if (obj_bench->obj_args->change_type)
		type_num++;
	obj_worker->oids[idx].oid =
			pmemobj_tx_realloc(obj_worker->oids[idx].oid,
			obj_bench->sizes[idx], type_num);
	if (OID_IS_NULL(obj_worker->oids[idx].oid)) {
		perror("pmemobj_tx_realloc");
		return -1;
	}
	return 0;
}

/*
 * add_range_nested_tx -- main operations of the obj_tx_add_range with nesting.
 */
static int
add_range_nested_tx(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	int ret = 0;
	struct obj_tx_worker *obj_worker = worker->priv;
	TX_BEGIN(obj_bench->pop) {
		if (obj_bench->obj_args->n_ops != obj_worker->tx_level) {
			size_t n_oid = obj_bench->n_oid(obj_worker->tx_level);
			struct offset offset = obj_bench->fn_off(obj_bench,
						obj_worker->tx_level);
			ret = pmemobj_tx_add_range(obj_worker->oids[n_oid].oid,
					offset.off, offset.size);
			obj_worker->tx_level++;
			ret = add_range_nested_tx(obj_bench, worker, idx);
		}
	} TX_ONABORT {
		fprintf(stderr, "transaction failed\n");
		return -1;
	} TX_END
	return ret;
}

/*
 * add_range_tx -- main operations of the obj_tx_add_range without nesting.
 */
static int
add_range_tx(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	int ret = 0;
	size_t i = 0;
	struct obj_tx_worker *obj_worker = worker->priv;
	TX_BEGIN(obj_bench->pop) {
		for (i = 0; i < obj_bench->obj_args->n_ops; i++) {
			size_t n_oid = obj_bench->n_oid(i);
			struct offset offset = obj_bench->fn_off(obj_bench, i);
			ret = pmemobj_tx_add_range(obj_worker->oids[n_oid].oid,
						offset.off, offset.size);
		}
	} TX_ONABORT {
		fprintf(stderr, "transaction failed\n");
		return -1;
	} TX_END
	return ret;
}

/*
 * obj_op_sim -- main function for benchmarks which simulates nested
 * transactions on dram or pmemobj atomic API by calling function recursively.
 */
static int
obj_op_sim(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	int ret = 0;
	struct obj_tx_worker *obj_worker = worker->priv;
	if (obj_worker->max_level == obj_worker->tx_level) {
		ret = obj_bench->fn_op[obj_bench->lib_op](obj_bench,
								worker, idx);
	} else {
		obj_worker->tx_level++;
		ret = obj_op_sim(obj_bench, worker, idx);
	}
	return ret;
}

/*
 * obj_op_tx -- main recursive function for transactional benchmarks
 */
static int
obj_op_tx(struct obj_tx_bench *obj_bench, struct worker_info *worker,
							unsigned int idx)
{
	int ret = 0;
	struct obj_tx_worker *obj_worker = worker->priv;
	TX_BEGIN(obj_bench->pop) {
		if (obj_worker->max_level == obj_worker->tx_level) {
			ret = obj_bench->fn_op[obj_bench->lib_op](
							obj_bench, worker, idx);
			if (obj_bench->op_mode == OP_MODE_ABORT_NESTED)
				pmemobj_tx_abort(-1);
		} else {
		obj_worker->tx_level++;
			ret = obj_op_tx(obj_bench, worker, idx);
			if (obj_worker->tx_level == 1 && obj_bench->op_mode ==
								OP_MODE_ABORT)
				pmemobj_tx_abort(-1);
		}
	} TX_ONABORT {
		if (obj_bench->op_mode != OP_MODE_ABORT &&
				obj_bench->op_mode != OP_MODE_ABORT_NESTED) {
			fprintf(stderr, "transaction failed\n");
			return -1;
		}
	} TX_END
	return ret;
}

/*
 * type_mode_one -- always returns 0, as in the mode NUM_MODE_ONE
 * all of the persistent objects have the same type_number value.
 */
static int
type_mode_one(struct obj_tx_bench *obj_bench, int worker_idx, int op_idx)
{
	return 0;
}

/*
 * type_mode_per_thread -- always returns worker index to all of the persistent
 * object allocated by the same thread have the same type number.
 */
static int
type_mode_per_thread(struct obj_tx_bench *obj_bench, int worker_idx, int op_idx)
{
	return worker_idx;
}

/*
 * type_mode_rand -- returns the value from the random_types array assigned
 * for the specific operation in a specific thread.
 */
static int
type_mode_rand(struct obj_tx_bench *obj_bench, int worker_idx, int op_idx)
{
	return obj_bench->random_types[op_idx];
}

/*
 * parse_op_mode_add_range -- parses command line "--operation" argument
 * and returns proper op_mode enum value for obj_tx_add_range.
 */
static enum op_mode
parse_op_mode_add_range(const char *arg)
{
	if (strcmp(arg, "basic") == 0)
		return OP_MODE_ONE_OBJ;
	else if (strcmp(arg, "one-obj-nested") == 0)
		return OP_MODE_ONE_OBJ_NESTED;
	else if (strcmp(arg, "range") == 0)
		return OP_MODE_ONE_OBJ_RANGE;
	else if (strcmp(arg, "range-nested") == 0)
		return OP_MODE_ONE_OBJ_NESTED_RANGE;
	else if (strcmp(arg, "all-obj") == 0)
		return OP_MODE_ALL_OBJ;
	else if (strcmp(arg, "all-obj-nested") == 0)
		return OP_MODE_ALL_OBJ_NESTED;
	else
		return OP_MODE_UNKNOWN;
}

/*
 * parse_op_mode -- parses command line "--operation" argument
 * and returns proper op_mode enum value.
 */
static enum op_mode
parse_op_mode(const char *arg)
{
	if (strcmp(arg, "basic") == 0)
		return OP_MODE_COMMIT;
	else if (strcmp(arg, "abort") == 0)
		return OP_MODE_ABORT;
	else if (strcmp(arg, "abort-nested") == 0)
		return OP_MODE_ABORT_NESTED;
	else
		return OP_MODE_UNKNOWN;
}

static fn_op alloc_op[] = {alloc_dram, alloc_tx, alloc_pmem};

static fn_op free_op[] = {free_dram, free_tx, free_pmem};

static fn_op realloc_op[] = {realloc_dram, realloc_tx, realloc_pmem};

static fn_op add_range_op[] = {add_range_tx, add_range_nested_tx};

static fn_parse parse_op[] = {parse_op_mode, parse_op_mode_add_range};

static fn_op nestings[] = {obj_op_sim, obj_op_tx};

/*
 * parse_type_num_mode -- converts string to type_num_mode enum
 */
static enum type_num_mode
parse_type_num_mode(const char *arg)
{
	if (strcmp(arg, "one") == 0)
		return NUM_MODE_ONE;
	else if (strcmp(arg, "per-thread") == 0)
		return NUM_MODE_PER_THREAD;
	else if (strcmp(arg, "rand") == 0)
		return NUM_MODE_RAND;
	fprintf(stderr, "unknown type number\n");
	return NUM_MODE_UNKNOWN;
}

/*
 * parse_lib_mode -- converts string to type_num_mode enum
 */
static enum lib_mode
parse_lib_mode(const char *arg)
{
	if (strcmp(arg, "dram") == 0)
		return LIB_MODE_DRAM;
	else if (strcmp(arg, "pmem") == 0)
		return LIB_MODE_OBJ_ATOMIC;
	else if (strcmp(arg, "tx") == 0)
		return LIB_MODE_OBJ_TX;
	fprintf(stderr, "unknown lib mode\n");
	return LIB_MODE_UNKNOWN;
}


static fn_type_num type_num_fn[] = {type_mode_one, type_mode_per_thread,
						type_mode_rand, NULL};

/*
 * one_num -- returns always the same number.
 */
static unsigned int
one_num(unsigned int idx)
{
	return 0;
}

/*
 * diff_num -- returns number given as argument.
 */
static unsigned int
diff_num(unsigned int idx)
{
	return idx;
}

/*
 * off_entire -- returns zero offset.
 */
static struct offset
off_entire(struct obj_tx_bench *obj_bench, unsigned int idx)
{
	struct offset offset;
	offset.off = 0;
	offset.size = obj_bench->sizes[obj_bench->n_oid(idx)];
	return offset;
}

/*
 * off_range -- returns offset for range in object.
 */
static struct offset
off_range(struct obj_tx_bench *obj_bench, unsigned int idx)
{
	struct offset offset;
	offset.size = obj_bench->sizes[0] / obj_bench->obj_args->n_ops;
	offset.off = offset.size * idx;
	return offset;
}

/*
 * rand_values -- allocates array and if range mode calculates random
 * values as allocation sizes for each object otherwise populates whole array
 * with max value. Used only when range flag set.
 */
static size_t *
rand_values(unsigned int min, unsigned int max, unsigned int n_ops)
{
	size_t size = max - min;
	size_t *sizes = calloc(n_ops, sizeof (size_t));
	if (sizes == NULL) {
		perror("calloc");
		return NULL;
	}
	for (size_t i = 0; i < n_ops; i++)
		sizes[i] = max;
	if (min) {
		if (min > max) {
			fprintf(stderr, "Invalid size\n");
			return NULL;
		}
		for (size_t i = 0; i < n_ops; i++)
			sizes[i] = (rand() % size) + min;
	}
	return sizes;
}

/*
 * obj_tx_add_range_op -- main operations of the obj_tx_add_range benchmark.
 */
static int
obj_tx_add_range_op(struct benchmark *bench, struct operation_info *info)
{
	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	struct obj_tx_worker *obj_worker = info->worker->priv;
	if (add_range_op[obj_bench->lib_op](obj_bench, info->worker,
							info->index) != 0)
		return -1;
	obj_worker->tx_level = 0;
	return 0;
}


/*
 * obj_tx_op -- main operation for obj_tx_alloc(), obj_tx_free() and
 * obj_tx_realloc() benchmarks.
 */
static int
obj_tx_op(struct benchmark *bench, struct operation_info *info)
{
	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	struct obj_tx_worker *obj_worker = info->worker->priv;
	int ret = nestings[obj_bench->nesting_mode](obj_bench,
						info->worker, info->index);
	obj_worker->tx_level = 0;
	return ret;
}

/*
 * obj_tx_init_worker -- common part for the worker initialization functions
 * for transactional benchmarks.
 */
static int
obj_tx_init_worker(struct benchmark *bench, struct benchmark_args *args,
						struct worker_info *worker)
{
	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	struct obj_tx_worker *obj_worker = calloc(1,
					sizeof (struct obj_tx_worker));
	if (obj_worker == NULL) {
		perror("calloc");
		return -1;
	}
	worker->priv = obj_worker;
	obj_worker->tx_level = 0;
	obj_worker->max_level = obj_bench->obj_args->nested;
	if (obj_bench->lib_mode != LIB_MODE_DRAM)
		obj_worker->oids = calloc(obj_bench->n_objs, sizeof (PMEMoid));
	else
		obj_worker->items = calloc(obj_bench->n_objs, sizeof (char *));
	if (obj_worker->oids == NULL && obj_worker->items == NULL) {
		free(obj_worker);
		perror("calloc");
		return -1;
	}
	return 0;
}

/*
 * obj_tx_free_init_worker_alloc_obj -- special part for the worker
 * initialization function for benchmarks which needs allocated objects
 * before operation.
 */
static int
obj_tx_init_worker_alloc_obj(struct benchmark *bench, struct benchmark_args
					*args, struct worker_info *worker)
{
	int i;
	if (obj_tx_init_worker(bench, args, worker) != 0)
		return -1;

	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	struct obj_tx_worker *obj_worker = worker->priv;
	for (i = 0; i < obj_bench->n_objs; i++) {
		if (alloc_op[obj_bench->lib_mode](obj_bench, worker, i) != 0)
			goto out;
	}
	return 0;
out:
	for (i--; i >= 0; i--)
		free_op[obj_bench->lib_mode](obj_bench, worker, i);
	if (obj_bench->lib_mode == LIB_MODE_DRAM)
		free(obj_worker->items);
	else
		free(obj_worker->oids);
	free(obj_worker);
	return -1;
}

/*
 * obj_tx_free_worker -- common part for the worker de-initialization.
 */
static int
obj_tx_free_worker(struct benchmark *bench, struct benchmark_args *args,
						struct worker_info *worker)
{
	struct obj_tx_worker *obj_worker = worker->priv;
	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	if (obj_bench->lib_mode == LIB_MODE_DRAM)
		free(obj_worker->items);
	else
		free(obj_worker->oids);
	free(obj_worker);
	return 0;
}

/*
 * obj_tx_alloc_free_worker_free_obj -- special part for the worker
 * de-initialization for benchmarks, which requires deallocation of
 * all of objects.
 */
static int
obj_tx_free_worker_free_obj(struct benchmark *bench, struct benchmark_args
					*args, struct worker_info *worker)
{
	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	for (unsigned int i = 0; i < obj_bench->n_objs; i++)
		free_op[obj_bench->lib_mode](obj_bench, worker, i);
	return obj_tx_free_worker(bench, args, worker);
}

/*
 * obj_tx_add_range_init -- specific part of the obj_tx_add_range
 * benchmark initialization.
 */
static int
obj_tx_add_range_init(struct benchmark *bench, struct benchmark_args *args)
{
	struct obj_tx_args *obj_args = args->opts;
	obj_args->parse_mode = PARSE_OP_MODE_ADD_RANGE;
	if (args->n_ops_per_thread > MAX_OPS)
		args->n_ops_per_thread = MAX_OPS;
	if (obj_tx_init(bench, args) != 0)
		return -1;

	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);

	obj_bench->n_oid = diff_num;
	if (obj_bench->op_mode < OP_MODE_ALL_OBJ) {
		obj_bench->n_oid = one_num;
		obj_bench->n_objs = 1;
	}
	obj_bench->fn_off = off_entire;
	if (obj_bench->op_mode == OP_MODE_ONE_OBJ_RANGE || obj_bench->op_mode
					== OP_MODE_ONE_OBJ_NESTED_RANGE) {
		obj_bench->fn_off = off_range;
		if (args->n_ops_per_thread > args->dsize)
			args->dsize = args->n_ops_per_thread;

		obj_bench->sizes[0] = args->dsize;
	}
	obj_bench->lib_op = (obj_bench->op_mode == OP_MODE_ONE_OBJ ||
				obj_bench->op_mode == OP_MODE_ALL_OBJ) ?
				ADD_RANGE_MODE_ONE_TX :
				ADD_RANGE_MODE_NESTED_TX;
	return 0;
}

/*
 * obj_tx_free_init -- specific part of the obj_tx_free initialization.
 */
static int
obj_tx_free_init(struct benchmark *bench, struct benchmark_args *args)
{
	if (obj_tx_init(bench, args) != 0)
		return -1;

	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	obj_bench->fn_op = free_op;
	return 0;
}

/*
 * obj_tx_alloc_init -- specific part of the obj_tx_alloc initialization.
 */
static int
obj_tx_alloc_init(struct benchmark *bench, struct benchmark_args *args)
{
	if (obj_tx_init(bench, args) != 0)
		return -1;

	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	obj_bench->fn_op = alloc_op;
	return 0;
}

/*
 * obj_tx_realloc_init -- specific part of the obj_tx_realloc initialization.
 */
static int
obj_tx_realloc_init(struct benchmark *bench, struct benchmark_args *args)
{
	if (obj_tx_init(bench, args) != 0)
		return -1;

	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	obj_bench->resizes = rand_values(obj_bench->obj_args->min_rsize,
						obj_bench->obj_args->rsize,
						args->n_ops_per_thread);
	if (obj_bench->resizes == NULL) {
		obj_tx_exit(bench, args);
		return -1;
	}
	obj_bench->fn_op = realloc_op;
	return 0;
}

/*
 * obj_tx_init -- common part of the benchmark initialization for transactional
 * benchmarks in their init functions. Parses command line arguments, set
 * variables and creates persistent pool.
 */
int
obj_tx_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);
	assert(args->opts != NULL);

	pmembench_set_priv(bench, &obj_bench);

	obj_bench.obj_args = args->opts;
	obj_bench.obj_args->obj_size = args->dsize;
	obj_bench.obj_args->n_ops = args->n_ops_per_thread;
	obj_bench.n_objs = args->n_ops_per_thread;

	obj_bench.lib_op = obj_bench.obj_args->lib != NULL ?
				parse_lib_mode(obj_bench.obj_args->lib) :
				LIB_MODE_OBJ_ATOMIC;

	if (obj_bench.lib_op == LIB_MODE_UNKNOWN)
			return -1;

	obj_bench.lib_mode = obj_bench.lib_op == LIB_MODE_DRAM ?
				LIB_MODE_DRAM : LIB_MODE_OBJ_ATOMIC;

	obj_bench.nesting_mode = obj_bench.lib_op == LIB_MODE_OBJ_TX ?
					NESTING_MODE_TX : NESTING_MODE_SIM;

	/*
	 * Multiplication by FACTOR prevents from out of memory error
	 * as the actual size of the allocated persistent objects
	 * is always larger than requested.
	 */
	size_t dsize = obj_bench.obj_args->rsize > args->dsize ?
					obj_bench.obj_args->rsize : args->dsize;
	size_t psize = args->n_ops_per_thread * dsize * args->n_threads;
	if (psize < PMEMOBJ_MIN_POOL)
		psize = PMEMOBJ_MIN_POOL;
	psize *= FACTOR;

	obj_bench.op_mode = parse_op[obj_bench.obj_args->parse_mode](
						obj_bench.obj_args->operation);
	if (obj_bench.op_mode == OP_MODE_UNKNOWN) {
		fprintf(stderr, "operation mode unknown\n");
		return -1;
	}

	obj_bench.type_mode = parse_type_num_mode(obj_bench.obj_args->type_num);
	if (obj_bench.type_mode == NUM_MODE_UNKNOWN)
		return -1;

	obj_bench.fn_type_num = type_num_fn[obj_bench.type_mode];
	if (obj_bench.type_mode == NUM_MODE_RAND) {
		obj_bench.random_types = rand_values(1, PMEMOBJ_NUM_OID_TYPES
						- 1, args->n_ops_per_thread);
			if (obj_bench.random_types == NULL)
				return -1;
	}
	obj_bench.sizes = rand_values(obj_bench.obj_args->min_size,
						obj_bench.obj_args->obj_size,
						args->n_ops_per_thread);
	if (obj_bench.sizes == NULL)
		goto free_random_types;

	if (obj_bench.lib_mode != LIB_MODE_DRAM) {
		/* Create pmemobj pool. */
		obj_bench.pop = pmemobj_create(args->fname, LAYOUT_NAME,
							psize, args->fmode);
		if (obj_bench.pop == NULL) {
			perror("pmemobj_create");
			goto free_all;
		}
	}

	return 0;
free_all:
	free(obj_bench.sizes);
free_random_types:
	if (obj_bench.type_mode == NUM_MODE_RAND)
		free(obj_bench.random_types);
	return -1;
}

/*
 * obj_tx_exit -- common part for the exit function of the transactional
 * benchmarks in their exit functions.
 */
int
obj_tx_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	if (obj_bench->lib_mode != LIB_MODE_DRAM)
		pmemobj_close(obj_bench->pop);

	free(obj_bench->sizes);
	if (obj_bench->type_mode == NUM_MODE_RAND)
		free(obj_bench->random_types);
	return 0;
}

/*
 * obj_tx_realloc_exit -- common part for the exit function of the transactional
 * benchmarks in their exit functions.
 */
static int
obj_tx_realloc_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct obj_tx_bench *obj_bench = pmembench_get_priv(bench);
	free(obj_bench->resizes);
	return obj_tx_exit(bench, args);
}

static struct benchmark_info obj_tx_alloc = {
	.name		= "obj_tx_alloc",
	.brief		= "pmemobj_tx_alloc() benchmark",
	.init		= obj_tx_alloc_init,
	.exit		= obj_tx_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= obj_tx_init_worker,
	.free_worker	= obj_tx_free_worker_free_obj,
	.operation	= obj_tx_op,
	.measure_time	= true,
	.clos		= obj_tx_clo,
	.nclos		= ARRAY_SIZE(obj_tx_clo) - 3,
	.opts_size	= sizeof (struct obj_tx_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(obj_tx_alloc);

static struct benchmark_info obj_tx_free = {
	.name		= "obj_tx_free",
	.brief		= "pmemobj_tx_free() benchmark",
	.init		= obj_tx_free_init,
	.exit		= obj_tx_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= obj_tx_init_worker_alloc_obj,
	.free_worker	= obj_tx_free_worker,
	.operation	= obj_tx_op,
	.measure_time	= true,
	.clos		= obj_tx_clo,
	.nclos		= ARRAY_SIZE(obj_tx_clo) - 3,
	.opts_size	= sizeof (struct obj_tx_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(obj_tx_free);

static struct benchmark_info obj_tx_realloc = {
	.name		= "obj_tx_realloc",
	.brief		= "pmemobj_tx_realloc() benchmark",
	.init		= obj_tx_realloc_init,
	.exit		= obj_tx_realloc_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= obj_tx_init_worker_alloc_obj,
	.free_worker	= obj_tx_free_worker_free_obj,
	.operation	= obj_tx_op,
	.measure_time	= true,
	.clos		= obj_tx_clo,
	.nclos		= ARRAY_SIZE(obj_tx_clo),
	.opts_size	= sizeof (struct obj_tx_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(obj_tx_realloc);

static struct benchmark_info obj_tx_add_range = {
	.name		= "obj_tx_add_range",
	.brief		= "pmemobj_tx_add_range() benchmark",
	.init		= obj_tx_add_range_init,
	.exit		= obj_tx_exit,
	.multithread	= true,
	.multiops	= false,
	.init_worker	= obj_tx_init_worker_alloc_obj,
	.free_worker	= obj_tx_free_worker_free_obj,
	.operation	= obj_tx_add_range_op,
	.measure_time	= true,
	.clos		= obj_tx_clo,
	.nclos		= ARRAY_SIZE(obj_tx_clo) - 5,
	.opts_size	= sizeof (struct obj_tx_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(obj_tx_add_range);
