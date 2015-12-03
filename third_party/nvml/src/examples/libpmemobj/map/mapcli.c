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

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <libpmemobj.h>

#include "map.h"
#include "map_ctree.h"
#include "map_btree.h"
#include "map_rbtree.h"
#include "map_hashmap_atomic.h"
#include "map_hashmap_tx.h"
#include "hashmap/hashmap.h"

#define	PM_HASHSET_POOL_SIZE	(160 * 1024 * 1024)

POBJ_LAYOUT_BEGIN(map);
POBJ_LAYOUT_ROOT(map, struct root);
POBJ_LAYOUT_END(map);

struct root {
	TOID(struct map) map;
};

static PMEMobjpool *pop;
static struct map_ctx *mapc;
static TOID(struct root) root;
static TOID(struct map) map;

/*
 * str_insert -- hs_insert wrapper which works on strings
 */
static void
str_insert(const char *str)
{
	uint64_t key;
	if (sscanf(str, "%lu", &key) > 0)
		map_insert(mapc, map, key, OID_NULL);
	else
		fprintf(stderr, "insert: invalid syntax\n");
}

/*
 * str_remove -- hs_remove wrapper which works on strings
 */
static void
str_remove(const char *str)
{
	uint64_t key;
	if (sscanf(str, "%lu", &key) > 0) {
		int l = map_lookup(mapc, map, key);
		if (l)
			map_remove(mapc, map, key);
		else
			fprintf(stderr, "no such value\n");
	} else
		fprintf(stderr,	"remove: invalid syntax\n");
}

/*
 * str_check -- hs_check wrapper which works on strings
 */
static void
str_check(const char *str)
{
	uint64_t key;
	if (sscanf(str, "%lu", &key) > 0) {
		int r = map_lookup(mapc, map, key);
		printf("%d\n", r);
	} else {
		fprintf(stderr, "check: invalid syntax\n");
	}
}

/*
 * str_insert_random -- inserts specified (as string) number of random numbers
 */
static void
str_insert_random(const char *str)
{
	uint64_t val;
	if (sscanf(str, "%lu", &val) > 0)
		for (uint64_t i = 0; i < val; ) {
			uint64_t r = ((uint64_t)rand()) << 32 | rand();
			int ret = map_insert(mapc, map, r, OID_NULL);
			if (ret < 0)
				break;
			if (ret == 0)
				i += 1;
		}
	else
		fprintf(stderr, "random insert: invalid syntax\n");
}

/*
 * rebuild -- rebuilds hashmap and measures execution time
 */
static void
rebuild(void)
{
	printf("rebuild ");
	fflush(stdout);
	time_t t1 = time(NULL);

	map_cmd(mapc, map, HASHMAP_CMD_REBUILD, 0);

	printf("%lus\n", time(NULL) - t1);
}

/*
 * str_rebuild -- hs_rebuild wrapper which executes specified number of times
 */
static void
str_rebuild(const char *str)
{
	uint64_t val;

	if (sscanf(str, "%lu", &val) > 0) {
		for (uint64_t i = 0; i < val; ++i) {
			printf("%2lu ", i);
			rebuild();
		}
	} else {
		rebuild();
	}
}

static void
help(void)
{
	printf("h - help\n");
	printf("i $value - insert $value\n");
	printf("r $value - remove $value\n");
	printf("c $value - check $value, returns 0/1\n");
	printf("n $value - insert $value random values\n");
	printf("p - print all values\n");
	printf("d - print debug info\n");
	printf("b [$value] - rebuild $value (default: 1) times\n");
	printf("q - quit\n");
}

static void
unknown_command(const char *str)
{
	fprintf(stderr, "unknown command '%c', use 'h' for help\n", str[0]);
}

static int
hashmap_print(uint64_t key, PMEMoid value, void *arg)
{
	printf("%lu ", key);
	return 0;
}

static void
print_all(void)
{
	printf("count: %lu\n", map_count(mapc, map));
	map_foreach(mapc, map, hashmap_print, NULL);
	printf("\n");
}

#define	INPUT_BUF_LEN 1000
int
main(int argc, char *argv[])
{
	if (argc < 3 || argc > 4) {
		printf("usage: %s hashmap_tx|hashmap_atomic|ctree|btree|rbtree"
				" file-name [<seed>]\n", argv[0]);
		return 1;
	}

	const struct map_ops *ops = NULL;
	const char *path = argv[2];
	const char *type = argv[1];
	if (strcmp(type, "hashmap_tx") == 0) {
		ops = MAP_HASHMAP_TX;
	} else if (strcmp(type, "hashmap_atomic") == 0) {
		ops = MAP_HASHMAP_ATOMIC;
	} else if (strcmp(type, "ctree") == 0) {
		ops = MAP_CTREE;
	} else if (strcmp(type, "btree") == 0) {
		ops = MAP_BTREE;
	} else if (strcmp(type, "rbtree") == 0) {
		ops = MAP_RBTREE;
	} else {
		fprintf(stderr, "invalid hasmap type -- '%s'\n", type);
		return 1;
	}

	if (access(path, F_OK) != 0) {
		pop = pmemobj_create(path, POBJ_LAYOUT_NAME(map),
				PM_HASHSET_POOL_SIZE, S_IRUSR | S_IWUSR);
		if (pop == NULL) {
			fprintf(stderr, "failed to create pool: %s\n",
					pmemobj_errormsg());
			return 1;
		}

		struct hashmap_args args;

		if (argc > 3)
			args.seed = atoi(argv[3]);
		else
			args.seed = time(NULL);
		srand(args.seed);


		mapc = map_ctx_init(ops, pop);
		if (!mapc) {
			pmemobj_close(pop);
			perror("map_ctx_init");
			return 1;
		}

		root = POBJ_ROOT(pop, struct root);

		printf("seed: %u\n", args.seed);
		map_new(mapc, &D_RW(root)->map, &args);

		map = D_RO(root)->map;
	} else {
		pop = pmemobj_open(path, POBJ_LAYOUT_NAME(map));
		if (pop == NULL) {
			fprintf(stderr, "failed to open pool: %s\n",
					pmemobj_errormsg());
			return 1;
		}

		mapc = map_ctx_init(ops, pop);
		if (!mapc) {
			pmemobj_close(pop);
			perror("map_ctx_init");
			return 1;
		}
		root = POBJ_ROOT(pop, struct root);
		map = D_RO(root)->map;
	}

	char buf[INPUT_BUF_LEN];

	if (isatty(fileno(stdout)))
		printf("Type 'h' for help\n$ ");

	while (fgets(buf, sizeof (buf), stdin)) {
		if (buf[0] == 0 || buf[0] == '\n')
			continue;

		switch (buf[0]) {
			case 'i':
				str_insert(buf + 1);
				break;
			case 'r':
				str_remove(buf + 1);
				break;
			case 'c':
				str_check(buf + 1);
				break;
			case 'n':
				str_insert_random(buf + 1);
				break;
			case 'p':
				print_all();
				break;
			case 'd':
				map_cmd(mapc, map, HASHMAP_CMD_DEBUG,
						(uint64_t)stdout);
				break;
			case 'b':
				str_rebuild(buf + 1);
				break;
			case 'q':
				fclose(stdin);
				break;
			case 'h':
				help();
				break;
			default:
				unknown_command(buf);
				break;
		}

		if (isatty(fileno(stdout)))
			printf("$ ");
	}

	pmemobj_close(pop);

	return 0;
}
