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

/* integer hash set implementation which uses only atomic APIs */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <libpmemobj.h>
#include "hashmap_atomic.h"
#include "hashmap_internal.h"

/* layout definition */
TOID_DECLARE(struct buckets, HASHMAP_ATOMIC_TYPE_OFFSET + 1);
TOID_DECLARE(struct entry, HASHMAP_ATOMIC_TYPE_OFFSET + 2);

struct entry {
	uint64_t key;
	PMEMoid value;

	/* list pointer */
	POBJ_LIST_ENTRY(struct entry) list;
};

struct entry_args {
	uint64_t key;
	PMEMoid value;
};

struct buckets {
	/* number of buckets */
	size_t nbuckets;
	/* array of lists */
	POBJ_LIST_HEAD(entries_head, struct entry) bucket[];
};

struct hashmap_atomic {
	/* random number generator seed */
	uint32_t seed;

	/* hash function coefficients */
	uint32_t hash_fun_a;
	uint32_t hash_fun_b;
	uint64_t hash_fun_p;

	/* number of values inserted */
	uint64_t count;
	/* whether "count" should be updated */
	uint32_t count_dirty;

	/* buckets */
	TOID(struct buckets) buckets;
	/* buckets, used during rehashing, null otherwise */
	TOID(struct buckets) buckets_tmp;
};

/*
 * create_entry -- entry initializer
 */
static void
create_entry(PMEMobjpool *pop, void *ptr, void *arg)
{
	struct entry *e = ptr;
	struct entry_args *args = arg;

	e->key = args->key;
	e->value = args->value;

	memset(&e->list, 0, sizeof (e->list));

	pmemobj_persist(pop, e, sizeof (*e));
}

/*
 * create_buckets -- buckets initializer
 */
static void
create_buckets(PMEMobjpool *pop, void *ptr, void *arg)
{
	struct buckets *b = ptr;

	b->nbuckets = *((size_t *)arg);
	pmemobj_memset_persist(pop, &b->bucket, 0,
			b->nbuckets * sizeof (b->bucket[0]));
	pmemobj_persist(pop, &b->nbuckets, sizeof (b->nbuckets));
}

/*
 * create_hashmap -- hashmap initializer
 */
static void
create_hashmap(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		uint32_t seed)
{
	D_RW(hashmap)->seed = seed;
	D_RW(hashmap)->hash_fun_a = (uint32_t)(1000.0 * rand() / RAND_MAX) + 1;
	D_RW(hashmap)->hash_fun_b = (uint32_t)(100000.0 * rand() / RAND_MAX);
	D_RW(hashmap)->hash_fun_p = HASH_FUNC_COEFF_P;

	size_t len = INIT_BUCKETS_NUM;
	size_t sz = sizeof (struct buckets) +
			len * sizeof (struct entries_head);

	if (POBJ_ALLOC(pop, &D_RW(hashmap)->buckets, struct buckets, sz,
			create_buckets, &len)) {
		fprintf(stderr, "root alloc failed: %s\n", pmemobj_errormsg());
		abort();
	}

	pmemobj_persist(pop, D_RW(hashmap), sizeof (*D_RW(hashmap)));
}

/*
 * hash -- the simplest hashing function,
 * see https://en.wikipedia.org/wiki/Universal_hashing#Hashing_integers
 */
static uint64_t
hash(const TOID(struct hashmap_atomic) *hashmap,
		const TOID(struct buckets) *buckets,
	uint64_t value)
{
	uint32_t a = D_RO(*hashmap)->hash_fun_a;
	uint32_t b = D_RO(*hashmap)->hash_fun_b;
	uint64_t p = D_RO(*hashmap)->hash_fun_p;
	size_t len = D_RO(*buckets)->nbuckets;

	return ((a * value + b) % p) % len;
}

/*
 * hm_atomic_rebuild_finish -- finishes rebuild, assumes buckets_tmp is not null
 */
static void
hm_atomic_rebuild_finish(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap)
{
	TOID(struct buckets) cur = D_RO(hashmap)->buckets;
	TOID(struct buckets) tmp = D_RO(hashmap)->buckets_tmp;

	for (size_t i = 0; i < D_RO(cur)->nbuckets; ++i) {
		while (!POBJ_LIST_EMPTY(&D_RO(cur)->bucket[i])) {
			TOID(struct entry) en =
					POBJ_LIST_FIRST(&D_RO(cur)->bucket[i]);
			uint64_t h = hash(&hashmap, &tmp, D_RO(en)->key);

			if (POBJ_LIST_MOVE_ELEMENT_HEAD(pop,
					&D_RW(cur)->bucket[i],
					&D_RW(tmp)->bucket[h],
					en, list, list)) {
				fprintf(stderr, "move failed: %s\n",
						pmemobj_errormsg());
				abort();
			}
		}
	}

	POBJ_FREE(&D_RO(hashmap)->buckets);

	D_RW(hashmap)->buckets = D_RO(hashmap)->buckets_tmp;
	pmemobj_persist(pop, &D_RW(hashmap)->buckets,
			sizeof (D_RW(hashmap)->buckets));

	/*
	 * We have to set offset manually instead of substituting OID_NULL,
	 * because we won't be able to recover easily if crash happens after
	 * pool_uuid_lo, but before offset is set. Another reason why everyone
	 * should use transaction API.
	 * See recovery process in hm_init and TOID_IS_NULL macro definition.
	 */
	D_RW(hashmap)->buckets_tmp.oid.off = 0;
	pmemobj_persist(pop, &D_RW(hashmap)->buckets_tmp,
			sizeof (D_RW(hashmap)->buckets_tmp));
}

/*
 * hm_atomic_rebuild -- rebuilds the hashmap with a new number of buckets
 */
static void
hm_atomic_rebuild(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		size_t new_len)
{
	if (new_len == 0)
		new_len = D_RO(D_RO(hashmap)->buckets)->nbuckets;

	size_t sz = sizeof (struct buckets) +
			new_len * sizeof (struct entries_head);

	POBJ_ALLOC(pop, &D_RW(hashmap)->buckets_tmp, struct buckets, sz,
			create_buckets, &new_len);
	if (TOID_IS_NULL(D_RO(hashmap)->buckets_tmp)) {
		fprintf(stderr,
			"failed to allocate temporary space of size: %lu, %s\n",
			new_len, pmemobj_errormsg());
		return;
	}

	hm_atomic_rebuild_finish(pop, hashmap);
}

/*
 * hm_atomic_insert -- inserts specified value into the hashmap,
 * returns:
 * - 0 if successful,
 * - 1 if value already existed,
 * - -1 if something bad happened
 */
int
hm_atomic_insert(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		uint64_t key, PMEMoid value)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	uint64_t h = hash(&hashmap, &buckets, key);
	int num = 0;

	POBJ_LIST_FOREACH(var, &D_RO(buckets)->bucket[h], list) {
		if (D_RO(var)->key == key)
			return 1;
		num++;
	}

	D_RW(hashmap)->count_dirty = 1;
	pmemobj_persist(pop, &D_RW(hashmap)->count_dirty,
			sizeof (D_RW(hashmap)->count_dirty));

	struct entry_args args = {
		.key = key,
		.value = value,
	};
	PMEMoid oid = POBJ_LIST_INSERT_NEW_HEAD(pop,
			&D_RW(buckets)->bucket[h],
			list, sizeof (struct entry), create_entry, &args);
	if (OID_IS_NULL(oid)) {
		fprintf(stderr, "failed to allocate entry: %s\n",
			pmemobj_errormsg());
		return -1;
	}

	D_RW(hashmap)->count++;
	pmemobj_persist(pop, &D_RW(hashmap)->count,
			sizeof (D_RW(hashmap)->count));

	D_RW(hashmap)->count_dirty = 0;
	pmemobj_persist(pop, &D_RW(hashmap)->count_dirty,
			sizeof (D_RW(hashmap)->count_dirty));

	num++;
	if (num > MAX_HASHSET_THRESHOLD ||
			(num > MIN_HASHSET_THRESHOLD &&
			D_RO(hashmap)->count > 2 * D_RO(buckets)->nbuckets))
		hm_atomic_rebuild(pop, hashmap, D_RW(buckets)->nbuckets * 2);

	return 0;
}

/*
 * hm_atomic_remove -- removes specified value from the hashmap,
 * returns:
 * - 1 if successful,
 * - 0 if value didn't exist,
 * - -1 if something bad happened
 */
PMEMoid
hm_atomic_remove(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		uint64_t key)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	uint64_t h = hash(&hashmap, &buckets, key);
	POBJ_LIST_FOREACH(var, &D_RW(buckets)->bucket[h], list) {
		if (D_RO(var)->key == key)
			break;
	}

	if (TOID_IS_NULL(var))
		return OID_NULL;

	D_RW(hashmap)->count_dirty = 1;
	pmemobj_persist(pop, &D_RW(hashmap)->count_dirty,
			sizeof (D_RW(hashmap)->count_dirty));

	if (POBJ_LIST_REMOVE_FREE(pop, &D_RW(buckets)->bucket[h],
			var, list)) {
		fprintf(stderr, "list remove failed: %s\n",
			pmemobj_errormsg());
		return OID_NULL;
	}

	D_RW(hashmap)->count--;
	pmemobj_persist(pop, &D_RW(hashmap)->count,
			sizeof (D_RW(hashmap)->count));

	D_RW(hashmap)->count_dirty = 0;
	pmemobj_persist(pop, &D_RW(hashmap)->count_dirty,
			sizeof (D_RW(hashmap)->count_dirty));

	if (D_RO(hashmap)->count < D_RO(buckets)->nbuckets)
		hm_atomic_rebuild(pop, hashmap, D_RO(buckets)->nbuckets / 2);

	return D_RO(var)->value;
}

/*
 * hm_atomic_foreach -- prints all values from the hashmap
 */
int
hm_atomic_foreach(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	int ret = 0;
	for (size_t i = 0; i < D_RO(buckets)->nbuckets; ++i)
		POBJ_LIST_FOREACH(var, &D_RO(buckets)->bucket[i], list) {
			ret = cb(D_RO(var)->key, D_RO(var)->value, arg);
			if (ret)
				return ret;
		}

	return 0;
}

/*
 * hm_atomic_debug -- prints complete hashmap state
 */
static void
hm_atomic_debug(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		FILE *out)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	fprintf(out, "a: %u b: %u p: %lu\n", D_RO(hashmap)->hash_fun_a,
		D_RO(hashmap)->hash_fun_b, D_RO(hashmap)->hash_fun_p);
	fprintf(out, "count: %lu, buckets: %lu\n", D_RO(hashmap)->count,
		D_RO(buckets)->nbuckets);

	for (size_t i = 0; i < D_RO(buckets)->nbuckets; ++i) {
		if (POBJ_LIST_EMPTY(&D_RO(buckets)->bucket[i]))
			continue;

		int num = 0;
		fprintf(out, "%lu: ", i);
		POBJ_LIST_FOREACH(var, &D_RO(buckets)->bucket[i], list) {
			fprintf(out, "%lu ", D_RO(var)->key);
			num++;
		}
		fprintf(out, "(%d)\n", num);
	}
}

/*
 * hm_atomic_get -- checks whether specified value is in the hashmap
 */
PMEMoid
hm_atomic_get(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		uint64_t key)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	uint64_t h = hash(&hashmap, &buckets, key);

	POBJ_LIST_FOREACH(var, &D_RO(buckets)->bucket[h], list)
		if (D_RO(var)->key == key)
			return D_RO(var)->value;

	return OID_NULL;
}

/*
 * hm_atomic_lookup -- checks whether specified value is in the hashmap
 */
int
hm_atomic_lookup(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		uint64_t key)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	uint64_t h = hash(&hashmap, &buckets, key);

	POBJ_LIST_FOREACH(var, &D_RO(buckets)->bucket[h], list)
		if (D_RO(var)->key == key)
			return 1;

	return 0;
}

/*
 * hm_atomic_new --  initializes hashmap state, called after pmemobj_create
 */
int
hm_atomic_new(PMEMobjpool *pop, TOID(struct hashmap_atomic) *map, void *arg)
{
	struct hashmap_args *args = arg;
	uint32_t seed = args ? args->seed : 0;

	POBJ_ZNEW(pop, map, struct hashmap_atomic);

	create_hashmap(pop, *map, seed);

	return 0;
}

/*
 * hm_atomic_init -- recovers hashmap state, called after pmemobj_open
 */
int
hm_atomic_init(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap)
{
	srand(D_RO(hashmap)->seed);

	/* handle rebuild interruption */
	if (!TOID_IS_NULL(D_RO(hashmap)->buckets_tmp)) {
		printf("rebuild, previous attempt crashed\n");
		if (TOID_EQUALS(D_RO(hashmap)->buckets,
				D_RO(hashmap)->buckets_tmp)) {
			/* see comment in hm_rebuild_finish */
			D_RW(hashmap)->buckets_tmp.oid.off = 0;
			pmemobj_persist(pop, &D_RW(hashmap)->buckets_tmp,
					sizeof (D_RW(hashmap)->buckets_tmp));
		} else if (TOID_IS_NULL(D_RW(hashmap)->buckets)) {
			D_RW(hashmap)->buckets = D_RW(hashmap)->buckets_tmp;
			pmemobj_persist(pop, &D_RW(hashmap)->buckets,
					sizeof (D_RW(hashmap)->buckets));
			/* see comment in hm_rebuild_finish */
			D_RW(hashmap)->buckets_tmp.oid.off = 0;
			pmemobj_persist(pop, &D_RW(hashmap)->buckets_tmp,
					sizeof (D_RW(hashmap)->buckets_tmp));
		} else {
			hm_atomic_rebuild_finish(pop, hashmap);
		}
	}

	/* handle insert or remove interruption */
	if (D_RO(hashmap)->count_dirty) {
		printf("count dirty, recalculating\n");
		TOID(struct entry) var;
		TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
		uint64_t cnt = 0;

		for (size_t i = 0; i < D_RO(buckets)->nbuckets; ++i)
			POBJ_LIST_FOREACH(var, &D_RO(buckets)->bucket[i], list)
				cnt++;

		printf("old count: %lu, new count: %lu\n",
			D_RO(hashmap)->count, cnt);
		D_RW(hashmap)->count = cnt;
		pmemobj_persist(pop, &D_RW(hashmap)->count,
				sizeof (D_RW(hashmap)->count));

		D_RW(hashmap)->count_dirty = 0;
		pmemobj_persist(pop, &D_RW(hashmap)->count_dirty,
				sizeof (D_RW(hashmap)->count_dirty));
	}

	return 0;
}

/*
 * hm_atomic_check -- checks if specified persistent object is an
 * instance of hashmap
 */
int
hm_atomic_check(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap)
{
	return !TOID_VALID(hashmap);
}

/*
 * hm_atomic_count -- returns number of elements
 */
size_t
hm_atomic_count(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap)
{
	return D_RO(hashmap)->count;
}

/*
 * hm_atomic_cmd -- execute cmd for hashmap
 */
int
hm_atomic_cmd(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		unsigned cmd, uint64_t arg)
{
	switch (cmd) {
		case HASHMAP_CMD_REBUILD:
			hm_atomic_rebuild(pop, hashmap, arg);
			return 0;
		case HASHMAP_CMD_DEBUG:
			if (!arg)
				return -EINVAL;
			hm_atomic_debug(pop, hashmap, (FILE *)arg);
			return 0;
		default:
			return -EINVAL;
	}
}
