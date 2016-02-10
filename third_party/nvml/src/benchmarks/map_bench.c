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
 * map_bench.c -- benchmarks for: ctree, btree, rbtree, hashmap_atomic
 * and hashmap_tx from examples.
 */
#include <assert.h>

#include "benchmark.h"
#include "map.h"
#include "map_ctree.h"
#include "map_btree.h"
#include "map_rbtree.h"
#include "map_hashmap_atomic.h"
#include "map_hashmap_tx.h"

TOID_DECLARE_ROOT(struct root);

struct root {
	TOID(struct map) map;
};

#define	swap(a, b) do {\
	typeof ((a)) _tmp = (a);\
	(a) = (b);\
	(b) = _tmp;\
} while (0)

#define	SIZE_PER_KEY	1024

static const struct {
	const char *str;
	const struct map_ops *ops;
} map_types[] = {
	{"ctree",		MAP_CTREE},
	{"btree",		MAP_BTREE},
	{"rbtree",		MAP_RBTREE},
	{"hashmap_tx",		MAP_HASHMAP_TX},
	{"hashmap_atomic",	MAP_HASHMAP_ATOMIC},
};

#define	MAP_TYPES_NUM	(sizeof (map_types) / sizeof (map_types[0]))

struct map_bench_args {
	unsigned int seed;
	uint64_t max_key;
	char *type;
	bool ext_tx;
};

struct map_bench_worker {
	uint64_t *keys;
	size_t nkeys;
};

struct map_bench {
	struct map_ctx *mapc;
	PMEMobjpool *pop;
	off_t pool_size;

	size_t nkeys;
	size_t init_nkeys;
	uint64_t *keys;

	TOID(struct root) root;
	TOID(struct map) map;
};

static struct benchmark_clo map_bench_clos[] = {
	{
		.opt_short	= 'T',
		.opt_long	= "type",
		.descr		= "Type of container "
			"[ctree|btree|rbtree|hashmap_tx|hashmap_atomic]",
		.off		= clo_field_offset(struct map_bench_args, type),
		.type		= CLO_TYPE_STR,
		.def		= "ctree",
	},
	{
		.opt_short	= 's',
		.opt_long	= "seed",
		.descr		= "PRNG seed",
		.off		= clo_field_offset(struct map_bench_args, seed),
		.type		= CLO_TYPE_UINT,
		.def		= "1",
		.type_uint = {
			.size	= clo_field_size(struct map_bench_args, seed),
			.base	= CLO_INT_BASE_DEC,
			.min	= 1,
			.max	= UINT_MAX,
		},
	},
	{
		.opt_short	= 'M',
		.opt_long	= "max-key",
		.descr		= "maximum key (0 means no limit)",
		.off		= clo_field_offset(struct map_bench_args,
						max_key),
		.type		= CLO_TYPE_UINT,
		.def		= "0",
		.type_uint = {
			.size	= clo_field_size(struct map_bench_args, seed),
			.base	= CLO_INT_BASE_DEC,
			.min	= 0,
			.max	= UINT64_MAX,
		}
	},
	{
		.opt_short	= 'x',
		.opt_long	= "external-tx",
		.descr		= "Use external transaction for all operations"
				" (works with single thread only)",
		.off		= clo_field_offset(struct map_bench_args,
						ext_tx),
		.type		= CLO_TYPE_FLAG,
	},
};

/*
 * get_key -- return 64-bit random key
 */
static uint64_t
get_key(unsigned int *seed, uint64_t max_key)
{
	unsigned int key_lo = rand_r(seed);
	unsigned int key_hi = rand_r(seed);
	uint64_t key = ((uint64_t)key_hi << 32) | key_lo;

	if (max_key)
		key = key % max_key;

	return key;
}

/*
 * parse_map_type -- parse type of map
 */
static const struct map_ops *
parse_map_type(const char *str)
{
	for (int i = 0; i < MAP_TYPES_NUM; i++) {
		if (strcmp(str, map_types[i].str) == 0)
			return map_types[i].ops;
	}

	return NULL;
}

/*
 * map_remove_op -- main operation for map_remove benchmark
 */
static int
map_remove_op(struct benchmark *bench, struct operation_info *info)
{
	struct map_bench *map_bench = pmembench_get_priv(bench);
	struct map_bench_worker *tworker = info->worker->priv;
	uint64_t key = tworker->keys[info->index];

	PMEMoid ret = map_remove(map_bench->mapc, map_bench->map, key);

	return !OID_IS_NULL(ret);
}

/*
 * map_insert_op -- main operation for map_insert benchmark
 */
static int
map_insert_op(struct benchmark *bench, struct operation_info *info)
{
	struct map_bench *map_bench = pmembench_get_priv(bench);
	struct map_bench_worker *tworker = info->worker->priv;
	uint64_t key = tworker->keys[info->index];

	return map_insert(map_bench->mapc, map_bench->map, key, OID_NULL);
}

/*
 * map_get_op -- main operation for map_get benchmark
 */
static int
map_get_op(struct benchmark *bench, struct operation_info *info)
{
	struct map_bench *map_bench = pmembench_get_priv(bench);
	struct map_bench_worker *tworker = info->worker->priv;
	uint64_t key = tworker->keys[info->index];

	PMEMoid ret = map_get(map_bench->mapc, map_bench->map, key);

	return !OID_IS_NULL(ret);
}

/*
 * map_common_init_worker -- common init worker function for map_* benchmarks
 */
static int
map_common_init_worker(struct benchmark *bench, struct benchmark_args *args,
		struct worker_info *worker)
{
	struct map_bench_worker *tworker = calloc(1, sizeof (*tworker));
	if (!tworker) {
		perror("calloc");
		return -1;
	}

	tworker->nkeys = args->n_ops_per_thread;
	tworker->keys = malloc(tworker->nkeys * sizeof (*tworker->keys));
	if (!tworker->keys) {
		perror("malloc");
		goto err_free_worker;
	}

	struct map_bench *tree = pmembench_get_priv(bench);
	struct map_bench_args *targs = args->opts;
	if (targs->ext_tx) {
		int ret = pmemobj_tx_begin(tree->pop, NULL);
		if (ret) {
			pmemobj_tx_end();
			goto err_free_keys;
		}
	}

	worker->priv = tworker;

	return 0;
err_free_keys:
	free(tworker->keys);
err_free_worker:
	free(tworker);
	return -1;
}

/*
 * map_common_free_worker -- common cleanup worker function for map_*
 * benchmarks
 */
static int
map_common_free_worker(struct benchmark *bench, struct benchmark_args *args,
		struct worker_info *worker)
{
	struct map_bench_worker *tworker = worker->priv;
	struct map_bench_args *targs = args->opts;
	int ret = 0;
	if (targs->ext_tx) {
		ret = pmemobj_tx_commit();
		pmemobj_tx_end();
	}
	free(tworker->keys);
	free(tworker);
	return ret;
}

/*
 * map_insert_init_worker -- init worker function for map_insert benchmark
 */
static int
map_insert_init_worker(struct benchmark *bench, struct benchmark_args *args,
		struct worker_info *worker)
{
	int ret = map_common_init_worker(bench, args, worker);
	if (ret)
		return ret;

	struct map_bench_args *targs = args->opts;
	assert(targs);
	struct map_bench_worker *tworker = worker->priv;
	assert(tworker);

	for (size_t i = 0; i < tworker->nkeys; i++)
		tworker->keys[i] = get_key(&targs->seed, targs->max_key);

	return 0;
}

/*
 * map_global_rand_keys_init -- assign random keys from global keys array
 */
static int
map_global_rand_keys_init(struct benchmark *bench, struct benchmark_args *args,
		struct worker_info *worker)
{

	struct map_bench *tree = pmembench_get_priv(bench);
	assert(tree);
	struct map_bench_args *targs = args->opts;
	assert(targs);
	struct map_bench_worker *tworker = worker->priv;
	assert(tworker);
	assert(tree->init_nkeys);

	/*
	 * Assign random keys from global tree->keys array without repetitions.
	 */
	for (size_t i = 0; i < tworker->nkeys; i++) {
		uint64_t index = get_key(&targs->seed, tree->init_nkeys);
		tworker->keys[i] = tree->keys[index];
		swap(tree->keys[index], tree->keys[tree->init_nkeys - 1]);
		tree->init_nkeys--;
	}

	return 0;
}

/*
 * map_remove_init_worker -- init worker function for map_remove benchmark
 */
static int
map_remove_init_worker(struct benchmark *bench, struct benchmark_args *args,
		struct worker_info *worker)
{
	int ret = map_common_init_worker(bench, args, worker);
	if (ret)
		return ret;

	ret = map_global_rand_keys_init(bench, args, worker);
	if (ret)
		goto err_common_free_worker;
	return 0;
err_common_free_worker:
	map_common_free_worker(bench, args, worker);
	return -1;
}

/*
 * map_bench_get_init_worker -- init worker function for map_get benchmark
 */
static int
map_bench_get_init_worker(struct benchmark *bench, struct benchmark_args *args,
		struct worker_info *worker)
{
	int ret = map_common_init_worker(bench, args, worker);
	if (ret)
		return ret;

	ret = map_global_rand_keys_init(bench, args, worker);
	if (ret)
		goto err_common_free_worker;
	return 0;
err_common_free_worker:
	map_common_free_worker(bench, args, worker);
	return -1;
}

/*
 * map_common_init -- common init function for map_* benchmarks
 */
static int
map_common_init(struct benchmark *bench, struct benchmark_args *args)
{
	assert(bench);
	assert(args);
	assert(args->opts);

	struct map_bench *map_bench = calloc(1, sizeof (*map_bench));
	if (!map_bench) {
		perror("calloc");
		return -1;
	}

	struct map_bench_args *targs = args->opts;

	const struct map_ops *ops = parse_map_type(targs->type);
	if (!ops) {
		fprintf(stderr, "invalid map type value specified -- '%s'\n",
				targs->type);
		goto err_free_bench;
	}

	if (targs->ext_tx && args->n_threads > 1) {
		fprintf(stderr, "external transaction "
			"requires single thread\n");
		goto err_free_bench;
	}

	map_bench->nkeys = args->n_threads * args->n_ops_per_thread;
	map_bench->init_nkeys = map_bench->nkeys;
	map_bench->pool_size = map_bench->nkeys * SIZE_PER_KEY;
	if (map_bench->pool_size < PMEMOBJ_MIN_POOL)
		map_bench->pool_size = PMEMOBJ_MIN_POOL;

	map_bench->pop = pmemobj_create(args->fname, "map_bench",
			map_bench->pool_size, args->fmode);
	if (!map_bench->pop) {
		fprintf(stderr, "pmemobj_create: %s\n", pmemobj_errormsg());
		goto err_free_bench;
	}

	map_bench->mapc = map_ctx_init(ops, map_bench->pop);
	if (!map_bench->mapc) {
		perror("map_ctx_init");
		goto err_close;
	}

	map_bench->root = POBJ_ROOT(map_bench->pop, struct root);
	if (TOID_IS_NULL(map_bench->root)) {
		fprintf(stderr, "pmemobj_root: %s\n", pmemobj_errormsg());
		goto err_free_map;
	}

	if (map_new(map_bench->mapc, &D_RW(map_bench->root)->map, NULL)) {
		perror("map_new");
		goto err_free_map;
	}

	map_bench->map = D_RO(map_bench->root)->map;

	pmembench_set_priv(bench, map_bench);
	return 0;
err_free_map:
	map_ctx_free(map_bench->mapc);
err_close:
	pmemobj_close(map_bench->pop);
err_free_bench:
	free(map_bench);
	return -1;
}

/*
 * map_common_exit -- common cleanup function for map_* benchmarks
 */
static int
map_common_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct map_bench *tree = pmembench_get_priv(bench);
	pmemobj_close(tree->pop);
	free(tree);
	return 0;
}

/*
 * map_keys_init -- initialize array with keys
 */
static int
map_keys_init(struct benchmark *bench, struct benchmark_args *args)
{
	struct map_bench *map_bench = pmembench_get_priv(bench);
	assert(map_bench);
	struct map_bench_args *targs = args->opts;
	assert(targs);

	map_bench->keys = malloc(map_bench->nkeys * sizeof (*map_bench->keys));
	if (!map_bench->keys) {
		perror("malloc");
		return -1;
	}

	int ret = 0;
	TX_BEGIN(map_bench->pop) {
		for (size_t i = 0; i < map_bench->nkeys; i++) {
			uint64_t key;
			PMEMoid oid;
			do {
				key = get_key(&targs->seed, targs->max_key);
				oid = map_get(map_bench->mapc,
						map_bench->map, key);
			} while (!OID_IS_NULL(oid));

			ret = map_insert(map_bench->mapc,
					map_bench->map, key, OID_NULL);
			if (ret)
				break;

			map_bench->keys[i] = key;
		}
	} TX_ONABORT {
		ret = -1;
	} TX_END

	if (!ret)
		return 0;

	free(map_bench->keys);
	return ret;
}

/*
 * map_keys_exit -- cleanup of keys array
 */
static int
map_keys_exit(struct benchmark *bench, struct benchmark_args *args)
{
	struct map_bench *tree = pmembench_get_priv(bench);
	free(tree->keys);
	return 0;
}

/*
 * map_remove_init -- init function for map_remove benchmark
 */
static int
map_remove_init(struct benchmark *bench, struct benchmark_args *args)
{
	int ret = map_common_init(bench, args);
	if (ret)
		return ret;
	ret = map_keys_init(bench, args);
	if (ret)
		goto err_exit_common;

	return 0;
err_exit_common:
	map_common_exit(bench, args);
	return -1;
}

/*
 * map_remove_exit -- cleanup function for map_remove benchmark
 */
static int
map_remove_exit(struct benchmark *bench, struct benchmark_args *args)
{
	map_keys_exit(bench, args);
	return map_common_exit(bench, args);
}

/*
 * map_bench_get_init -- init function for map_get benchmark
 */
static int
map_bench_get_init(struct benchmark *bench, struct benchmark_args *args)
{
	int ret = map_common_init(bench, args);
	if (ret)
		return ret;
	ret = map_keys_init(bench, args);
	if (ret)
		goto err_exit_common;

	return 0;
err_exit_common:
	map_common_exit(bench, args);
	return -1;
}

/*
 * map_get_exit -- exit function for map_get benchmark
 */
static int
map_get_exit(struct benchmark *bench, struct benchmark_args *args)
{
	map_keys_exit(bench, args);
	return map_common_exit(bench, args);
}

static struct benchmark_info map_insert_info = {
	.name		= "map_insert",
	.brief		= "Inserting to tree map",
	.init		= map_common_init,
	.exit		= map_common_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= map_insert_init_worker,
	.free_worker	= map_common_free_worker,
	.operation	= map_insert_op,
	.measure_time	= true,
	.clos		= map_bench_clos,
	.nclos		= ARRAY_SIZE(map_bench_clos),
	.opts_size	= sizeof (struct map_bench_args),
	.rm_file	= true
};
REGISTER_BENCHMARK(map_insert_info);

static struct benchmark_info map_remove_info = {
	.name		= "map_remove",
	.brief		= "Inserting to tree map",
	.init		= map_remove_init,
	.exit		= map_remove_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= map_remove_init_worker,
	.free_worker	= map_common_free_worker,
	.operation	= map_remove_op,
	.measure_time	= true,
	.clos		= map_bench_clos,
	.nclos		= ARRAY_SIZE(map_bench_clos),
	.opts_size	= sizeof (struct map_bench_args),
	.rm_file	= true
};
REGISTER_BENCHMARK(map_remove_info);

static struct benchmark_info map_get_info = {
	.name		= "map_get",
	.brief		= "Tree lookup",
	.init		= map_bench_get_init,
	.exit		= map_get_exit,
	.multithread	= true,
	.multiops	= true,
	.init_worker	= map_bench_get_init_worker,
	.free_worker	= map_common_free_worker,
	.operation	= map_get_op,
	.measure_time	= true,
	.clos		= map_bench_clos,
	.nclos		= ARRAY_SIZE(map_bench_clos),
	.opts_size	= sizeof (struct map_bench_args),
	.rm_file	= true
};
REGISTER_BENCHMARK(map_get_info);
