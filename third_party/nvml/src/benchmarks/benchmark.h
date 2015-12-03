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
 * benchmark.h -- This file contains interface for creating benchmarks to the
 * pmembench framework. The _most_ important data structure is
 * struct benchmark_info which should be properly filled and registered by the
 * benchmark. Some fields should be filled by meta-data and information about
 * the benchmark like: name, brief description, supported operation modes etc.
 * The other group of fields are function callbacks which may be implemented by
 * the benchmark. Some callbacks are required, others are optional. This is
 * indicated in the structure description.
 *
 * To register a benchmark you can use the special macro
 * REGISTER_BENCHMARK() which takes static benchmark_info data structure as an
 * argument. You can also use the pmembench_register() function. Please note
 * that registering a benchmark should be done at initialization time. You can
 * achieve this by specifying pmembench_init macro in function attributes:
 *
 * static void pmembench_init my_benchmark_init()
 * {
 *	pmembench_register(&my_benchmark);
 * }
 *
 * However using the REGISTER_BENCHMARK() macro is recommended.
 */
#ifndef	_BENCHMARK_H
#define	_BENCHMARK_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>

#include "benchmark_time.h"

#ifndef	ARRAY_SIZE
#define	ARRAY_SIZE(x)	(sizeof (x) / sizeof (x[0]))
#endif
#define	RRAND(max, min) (rand() % ((max) - (min)) + (min))

struct benchmark;

/*
 * benchmark_args - Arguments for benchmark.
 *
 * It contains set of common arguments and pointer to benchmark's specific
 * arguments which are automatically processed by framework according to
 * clos, nclos and opt_size in benchmark_info structure.
 */
struct benchmark_args
{
	const char *fname;		/* path to test file */
	size_t fsize;			/* size of test file */
	mode_t fmode;			/* test file's permissions */
	unsigned int n_threads;		/* number of working threads */
	uint64_t n_ops_per_thread;	/* number of operations per thread */
	size_t dsize;			/* data size */
	unsigned int seed;		/* PRNG seed */
	unsigned int repeats;		/* number of repeats of one scenario */
	bool help;			/* print help for benchmark */
	void *opts;			/* benchmark specific arguments */
};

/*
 * benchmark_results - Benchmark's execution results.
 */
struct benchmark_results
{
	uint64_t nbytes;		/* number of bytes processed */
	uint64_t nops;			/* number of operations executed */
	benchmark_time_t time;		/* total execution time */
};

/*
 * Command Line Option integer value base.
 */
enum clo_int_base
{
	CLO_INT_BASE_NONE = 0x0,
	CLO_INT_BASE_DEC = 0x1,
	CLO_INT_BASE_HEX = 0x2,
	CLO_INT_BASE_OCT = 0x4,
	CLO_INT_BASE_ANY = 0xf,
};

/*
 * Command Line Option type.
 */
enum clo_type
{
	CLO_TYPE_FLAG,
	CLO_TYPE_STR,
	CLO_TYPE_INT,
	CLO_TYPE_UINT,

	CLO_TYPE_MAX,
};

/*
 * Description of command line option.
 *
 * This structure is used to declare command line options by the benchmark
 * which will be automatically parsed by the framework.
 *
 * opt_short	: Short option char. If there is no short option write 0.
 * opt_long	: Long option string.
 * descr	: Description of command line option.
 * off		: Offset in data structure in which the value should be stored.
 * type		: Type of command line option.
 * def		: Default value. If set to NULL, this options is required.
 * ignore_in_res: Do not print in results.
 * check	: Optional callback for checking the command line option value.
 * type_int	: Parameters for signed integer.
 * type_uint	: Parameters for unsigned integer.
 * type_str	: Parameters for string.
 *
 * size		: Size of integer value. Valid values: 1, 2, 4, 8.
 * base		: Integer base system from which the parsing should be
 *                performed. This field may be used as bit mask by logically
 *                adding different base types.
 * limit_min	: Indicates whether value should be limited by the minimum
 *                value.
 * limit_max	: Indicates whether value should be limited by the maximum
 *                value.
 * min		: Minimum value when limit_min is set.
 * max		: Maximum value when limit_min is set.
 *
 * alloc	: If set to true the framework should allocate memory for the
 *                value. The memory will be freed by the framework at the end of
 *                execution. Otherwise benchmark must provide valid pointer in
 *                opt_var and max_size parameter must be set properly.
 * max_size	: Maximum size of string.
 */
struct benchmark_clo
{
	int opt_short;
	const char *opt_long;
	enum clo_type type;
	const char *descr;
	size_t off;
	const char *def;
	bool ignore_in_res;
	struct
	{
		size_t size;
		enum clo_int_base base;
		int64_t min;
		int64_t max;
	} type_int;
	struct
	{
		size_t size;
		enum clo_int_base base;
		uint64_t min;
		uint64_t max;
	} type_uint;
	int used;
};

#define	clo_field_offset(s, f)	((size_t)&((s *)0)->f)
#define	clo_field_size(s, f)	(sizeof (((s *)0)->f))

/*
 * worker_info - Worker thread's information structure.
 */
struct worker_info
{
	unsigned int index;		/* index of worker thread */
	struct operation_info *opinfo;	/* operation info structure */
	size_t nops;			/* number of operations */
	void *priv;			/* worker's private data */
};

/*
 * operation_info - Information about operation.
 */
struct operation_info
{
	struct worker_info *worker;	/* worker's info */
	struct benchmark_args *args;	/* benchmark arguments */
	unsigned int index;		/* operation's index */
	benchmark_time_t t_diff;	/* timestamp of start */
};

/*
 * struct benchmark_info -- benchmark descriptor
 * name		: Name of benchmark.
 * brief	: Brief description of benchmark.
 * clos		: Command line options which will be automatically parsed by
 *                framework.
 * nclos	: Number of command line options.
 * opts_size	: Size of data structure where the parsed values should be
 *                stored in.
 * print_help	: Callback for printing help message.
 * pre_init	: Function for initialization of the benchmark before parsing
 *                command line arguments.
 * init		: Function for initialization of the benchmark after parsing
 *                command line arguments.
 * exit		: Function for de-initialization of the benchmark.
 * multithread	: Indicates whether the benchmark operation function may be
 *                run in many threads.
 * multiops	: Indicates whether the benchmark operation function may be
 *                run many time in a loop.
 * measure_time	: Indicates whether the benchmark framework should measure the
 *                execution time of operation function. If set to false, the
 *                benchmark must report the execution time by itself.
 * init_worker	: Callback for initialization thread specific data. Invoked in
 *                a single thread for every thread worker.
 * operation	: Callback function which does the main job of benchmark.
 * rm_file	: Indicates whether the test file should be removed by
 *                framework before the init function will be called.
 * According to multithread and single_operation flags it may be
 * invoked in different ways:
 *  +-------------+----------+-------------------------------------+
 *  | multithread | multiops |            description              |
 *  +-------------+----------+-------------------------------------+
 *  |    false    |  false   | invoked once, in one thread         |
 *  +-------------+----------+-------------------------------------+
 *  |    false    |  true    | invoked many times, in one thread   |
 *  +-------------+----------+-------------------------------------+
 *  |    true     |  false   | invoked once, in many threads       |
 *  +-------------+----------+-------------------------------------+
 *  |    true     |  true    | invoked many times, in many threads |
 *  +-------------+----------+-------------------------------------+
 *
 */
struct benchmark_info
{
	const char *name;
	const char *brief;
	struct benchmark_clo *clos;
	size_t nclos;
	size_t opts_size;
	void (*print_help)(struct benchmark *bench);
	int (*pre_init)(struct benchmark *bench);
	int (*init)(struct benchmark *bench, struct benchmark_args *args);
	int (*exit)(struct benchmark *bench, struct benchmark_args *args);
	int (*init_worker)(struct benchmark *bench, struct benchmark_args *args,
			struct worker_info *worker);
	int (*free_worker)(struct benchmark *bench, struct benchmark_args *args,
			struct worker_info *worker);
	int (*operation)(struct benchmark *bench, struct operation_info *info);
	int (*op_init)(struct benchmark *bench, struct operation_info *info);
	int (*op_exit)(struct benchmark *bench, struct operation_info *info);
	bool multithread;
	bool multiops;
	bool measure_time;
	bool rm_file;
};

void *pmembench_get_priv(struct benchmark *bench);
void pmembench_set_priv(struct benchmark *bench, void *priv);
struct benchmark_info *pmembench_get_info(struct benchmark *bench);
int pmembench_register(struct benchmark_info *bench_info);

#define	REGISTER_BENCHMARK(bench)					\
__attribute__((constructor))						\
static void								\
__register_benchmark_##bench(void)					\
{									\
	if (pmembench_register(&bench)) {				\
		fprintf(stderr, "Unable to register benchmark '%s'\n",	\
				bench.name);				\
	}								\
}

#endif /* _BENCHMARK_H */
