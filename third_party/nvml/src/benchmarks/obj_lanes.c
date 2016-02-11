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
 * obj_lanes.c -- lane benchmark definition
 */

#include <assert.h>
#include <errno.h>
#include <unistd.h>

#include "libpmemobj.h"
#include "benchmark.h"
#include "lane.h"

/*
 * The number of times to repeat the operation, used to get more accurate
 * results, because the operation time was minimal compared to the framework
 * overhead.
 */
#define	OPERATION_REPEAT_COUNT 10000

/*
 * prog_args - command line parsed arguments
 */
struct prog_args {
	char *lane_section_name;	/* lane section to be held */
};

/*
 * obj_bench - variables used in benchmark, passed within functions
 */
struct obj_bench {
	PMEMobjpool *pop;			/* persistent pool handle */
	struct prog_args *pa;			/* prog_args structure */
	enum lane_section_type lane_type;	/* lane section to be held */
};

/*
 * parse_lane_section -- parses command line "--lane_section" and returns
 * proper lane section type enum
 */
static enum lane_section_type
parse_lane_section(const char *arg)
{
	if (strcmp(arg, "allocator") == 0)
		return LANE_SECTION_ALLOCATOR;
	else if (strcmp(arg, "list") == 0)
		return LANE_SECTION_LIST;
	else if (strcmp(arg, "transaction") == 0)
		return LANE_SECTION_TRANSACTION;
	else
		return MAX_LANE_SECTION;
}

/*
 * lanes_init -- benchmark initialization
 */
static int
lanes_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench != NULL);
	assert(args != NULL);
	assert(args->opts != NULL);

	struct obj_bench *ob = malloc(sizeof (struct obj_bench));
	if (ob == NULL) {
		perror("malloc");
		return -1;
	}
	pmembench_set_priv(bench, ob);

	ob->pa = args->opts;

	/* create pmemobj pool */
	ob->pop = pmemobj_create(args->fname,
			"obj_lanes", PMEMOBJ_MIN_POOL, args->fmode);
	if (ob->pop == NULL) {
		fprintf(stderr, "%s\n", pmemobj_errormsg());
		goto err;
	}

	ob->lane_type = parse_lane_section(ob->pa->lane_section_name);
	if (ob->lane_type == MAX_LANE_SECTION) {
		fprintf(stderr, "wrong lane type\n");
		goto err;
	}

	return 0;

err:
	free(ob);
	return -1;
}

/*
 * lanes_exit -- benchmark clean up
 */
static int
lanes_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct obj_bench *ob = pmembench_get_priv(bench);

	pmemobj_close(ob->pop);
	free(ob);

	return 0;
}

/*
 * lanes_op -- performs the lane hold and release operations
 */
static int
lanes_op(struct benchmark *bench, struct operation_info *info)
{
	struct obj_bench *ob = pmembench_get_priv(bench);
	struct lane_section *section;

	for (int i = 0; i < OPERATION_REPEAT_COUNT; i++) {
		int ret = lane_hold(ob->pop, &section, ob->lane_type);
		if (ret != 0) {
			fprintf(stderr, "lane_hold error %d: %s\n", ret,
					pmemobj_errormsg());
			return ret;
		}

		ret = lane_release(ob->pop);
		if (ret != 0) {
			fprintf(stderr, "lane_release error %d: %s\n", ret,
					pmemobj_errormsg());
			return ret;
		}
	}

	return 0;
}

/* structure defining command line arguments */
static struct benchmark_clo lanes_clo[] = {
	{
		.opt_short	= 's',
		.opt_long	= "lane_section",
		.descr		= "The lane section type: allocator,"
					" list or transaction",
		.type		= CLO_TYPE_STR,
		.off		= clo_field_offset(struct prog_args,
							lane_section_name),
		.def		= "allocator",
	},
};

/*
 * stores information about lane benchmark
 */
static struct benchmark_info lanes_info = {
	.name		= "obj_lanes",
	.brief		= "Benchmark for internal lanes operation",
	.init		= lanes_init,
	.exit		= lanes_exit,
	.multithread	= true,
	.multiops	= true,
	.operation	= lanes_op,
	.measure_time	= true,
	.clos		= lanes_clo,
	.nclos		= ARRAY_SIZE(lanes_clo),
	.opts_size	= sizeof (struct prog_args),
	.rm_file	= true
};

REGISTER_BENCHMARK(lanes_info);
