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

/* integer hash set implementation which uses only transaction APIs */

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include <libpmemobj.h>
#include "hashmap_tx.h"
#include "hashmap_internal.h"


/* layout definition */
TOID_DECLARE(struct buckets, HASHMAP_TX_TYPE_OFFSET + 1);
TOID_DECLARE(struct entry, HASHMAP_TX_TYPE_OFFSET + 2);

struct entry {
	uint64_t key;
	PMEMoid value;

	/* next entry list pointer */
	TOID(struct entry) next;
};

struct buckets {
	/* number of buckets */
	size_t nbuckets;
	/* array of lists */
	TOID(struct entry) bucket[];
};

struct hashmap_tx {
	/* random number generator seed */
	uint32_t seed;

	/* hash function coefficients */
	uint32_t hash_fun_a;
	uint32_t hash_fun_b;
	uint64_t hash_fun_p;

	/* number of values inserted */
	uint64_t count;

	/* buckets */
	TOID(struct buckets) buckets;
};

/*
 * create_hashmap -- hashmap initializer
 */
static void
create_hashmap(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, uint32_t seed)
{
	size_t len = INIT_BUCKETS_NUM;
	size_t sz = sizeof (struct buckets) +
			len * sizeof (TOID(struct entry));

	TX_BEGIN(pop) {
		TX_ADD(hashmap);

		D_RW(hashmap)->seed = seed;
		D_RW(hashmap)->hash_fun_a =
				(uint32_t)(1000.0 * rand() / RAND_MAX) + 1;
		D_RW(hashmap)->hash_fun_b =
				(uint32_t)(100000.0 * rand() / RAND_MAX);
		D_RW(hashmap)->hash_fun_p = HASH_FUNC_COEFF_P;

		D_RW(hashmap)->buckets = TX_ZALLOC(struct buckets, sz);
		D_RW(D_RW(hashmap)->buckets)->nbuckets = len;
	} TX_ONABORT {
		fprintf(stderr, "%s: transaction aborted: %s\n", __func__,
			pmemobj_errormsg());
		abort();
	} TX_END
}

/*
 * hash -- the simplest hashing function,
 * see https://en.wikipedia.org/wiki/Universal_hashing#Hashing_integers
 */
static uint64_t
hash(const TOID(struct hashmap_tx) *hashmap,
	const TOID(struct buckets) *buckets, uint64_t value)
{
	uint32_t a = D_RO(*hashmap)->hash_fun_a;
	uint32_t b = D_RO(*hashmap)->hash_fun_b;
	uint64_t p = D_RO(*hashmap)->hash_fun_p;
	size_t len = D_RO(*buckets)->nbuckets;

	return ((a * value + b) % p) % len;
}

/*
 * hm_tx_rebuild -- rebuilds the hashmap with a new number of buckets
 */
static void
hm_tx_rebuild(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, size_t new_len)
{
	TOID(struct buckets) buckets_old = D_RO(hashmap)->buckets;

	if (new_len == 0)
		new_len = D_RO(buckets_old)->nbuckets;

	size_t sz_old = sizeof (struct buckets) +
			D_RO(buckets_old)->nbuckets *
			sizeof (TOID(struct entry));
	size_t sz_new = sizeof (struct buckets) +
			new_len * sizeof (TOID(struct entry));

	TX_BEGIN(pop) {
		TX_ADD_FIELD(hashmap, buckets);
		TOID(struct buckets) buckets_new =
				TX_ZALLOC(struct buckets, sz_new);
		D_RW(buckets_new)->nbuckets = new_len;
		pmemobj_tx_add_range(buckets_old.oid, 0, sz_old);

		for (size_t i = 0; i < D_RO(buckets_old)->nbuckets; ++i) {
			while (!TOID_IS_NULL(D_RO(buckets_old)->bucket[i])) {
				TOID(struct entry) en =
					D_RO(buckets_old)->bucket[i];
				uint64_t h = hash(&hashmap, &buckets_new,
						D_RO(en)->key);

				D_RW(buckets_old)->bucket[i] = D_RO(en)->next;

				TX_ADD_FIELD(en, next);
				D_RW(en)->next = D_RO(buckets_new)->bucket[h];
				D_RW(buckets_new)->bucket[h] = en;
			}
		}

		D_RW(hashmap)->buckets = buckets_new;
		TX_FREE(buckets_old);
	} TX_ONABORT {
		fprintf(stderr, "%s: transaction aborted: %s\n", __func__,
			pmemobj_errormsg());
		/*
		 * We don't need to do anything here, because everything is
		 * consistent. The only thing affected is performance.
		 */
	} TX_END

}

/*
 * hm_tx_insert -- inserts specified value into the hashmap,
 * returns:
 * - 0 if successful,
 * - 1 if value already existed,
 * - -1 if something bad happened
 */
int
hm_tx_insert(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
	uint64_t key, PMEMoid value)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	uint64_t h = hash(&hashmap, &buckets, key);
	int num = 0;

	for (var = D_RO(buckets)->bucket[h];
			!TOID_IS_NULL(var);
			var = D_RO(var)->next) {
		if (D_RO(var)->key == key)
			return 1;
		num++;
	}

	int ret = 0;
	TX_BEGIN(pop) {
		TX_ADD_FIELD(D_RO(hashmap)->buckets, bucket[h]);
		TX_ADD_FIELD(hashmap, count);

		TOID(struct entry) e = TX_NEW(struct entry);
		D_RW(e)->key = key;
		D_RW(e)->value = value;
		D_RW(e)->next = D_RO(buckets)->bucket[h];
		D_RW(buckets)->bucket[h] = e;

		D_RW(hashmap)->count++;
		num++;
	} TX_ONABORT {
		fprintf(stderr, "transaction aborted: %s\n",
			pmemobj_errormsg());
		ret = -1;
	} TX_END

	if (ret)
		return ret;

	if (num > MAX_HASHSET_THRESHOLD ||
			(num > MIN_HASHSET_THRESHOLD &&
			D_RO(hashmap)->count > 2 * D_RO(buckets)->nbuckets))
		hm_tx_rebuild(pop, hashmap, D_RO(buckets)->nbuckets * 2);

	return 0;
}

/*
 * hm_tx_remove -- removes specified value from the hashmap,
 * returns:
 * - key's value if successful,
 * - OID_NULL if value didn't exist or if something bad happened
 */
PMEMoid
hm_tx_remove(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, uint64_t key)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var, prev = TOID_NULL(struct entry);

	uint64_t h = hash(&hashmap, &buckets, key);
	for (var = D_RO(buckets)->bucket[h];
			!TOID_IS_NULL(var);
			prev = var, var = D_RO(var)->next) {
		if (D_RO(var)->key == key)
			break;
	}

	if (TOID_IS_NULL(var))
		return OID_NULL;

	TX_BEGIN(pop) {
		if (TOID_IS_NULL(prev))
			TX_ADD_FIELD(D_RO(hashmap)->buckets, bucket[h]);
		else
			TX_ADD_FIELD(prev, next);
		TX_ADD_FIELD(hashmap, count);

		if (TOID_IS_NULL(prev))
			D_RW(buckets)->bucket[h] = D_RO(var)->next;
		else
			D_RW(prev)->next = D_RO(var)->next;
		D_RW(hashmap)->count--;
		TX_FREE(var);
	} TX_ONABORT {
		fprintf(stderr, "transaction aborted: %s\n",
			pmemobj_errormsg());
		return OID_NULL;
	} TX_END

	if (D_RO(hashmap)->count < D_RO(buckets)->nbuckets)
		hm_tx_rebuild(pop, hashmap, D_RO(buckets)->nbuckets / 2);

	return D_RO(var)->value;
}

/*
 * hm_tx_foreach -- prints all values from the hashmap
 */
int
hm_tx_foreach(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	int ret = 0;
	for (size_t i = 0; i < D_RO(buckets)->nbuckets; ++i) {
		if (TOID_IS_NULL(D_RO(buckets)->bucket[i]))
			continue;

		for (var = D_RO(buckets)->bucket[i]; !TOID_IS_NULL(var);
				var = D_RO(var)->next) {
			ret = cb(D_RO(var)->key, D_RO(var)->value, arg);
			if (ret)
				break;
		}
	}

	return ret;
}

/*
 * hm_tx_debug -- prints complete hashmap state
 */
static void
hm_tx_debug(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, FILE *out)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	fprintf(out, "a: %u b: %u p: %lu\n", D_RO(hashmap)->hash_fun_a,
		D_RO(hashmap)->hash_fun_b, D_RO(hashmap)->hash_fun_p);
	fprintf(out, "count: %lu, buckets: %lu\n", D_RO(hashmap)->count,
		D_RO(buckets)->nbuckets);

	for (size_t i = 0; i < D_RO(buckets)->nbuckets; ++i) {
		if (TOID_IS_NULL(D_RO(buckets)->bucket[i]))
			continue;

		int num = 0;
		fprintf(out, "%lu: ", i);
		for (var = D_RO(buckets)->bucket[i]; !TOID_IS_NULL(var);
				var = D_RO(var)->next) {
			fprintf(out, "%lu ", D_RO(var)->key);
			num++;
		}
		fprintf(out, "(%d)\n", num);
	}
}

/*
 * hm_tx_get -- checks whether specified value is in the hashmap
 */
PMEMoid
hm_tx_get(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, uint64_t key)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	uint64_t h = hash(&hashmap, &buckets, key);

	for (var = D_RO(buckets)->bucket[h];
			!TOID_IS_NULL(var);
			var = D_RO(var)->next)
		if (D_RO(var)->key == key)
			return D_RO(var)->value;

	return OID_NULL;
}

/*
 * hm_tx_lookup -- checks whether specified value exists
 */
int
hm_tx_lookup(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap, uint64_t key)
{
	TOID(struct buckets) buckets = D_RO(hashmap)->buckets;
	TOID(struct entry) var;

	uint64_t h = hash(&hashmap, &buckets, key);

	for (var = D_RO(buckets)->bucket[h];
			!TOID_IS_NULL(var);
			var = D_RO(var)->next)
		if (D_RO(var)->key == key)
			return 1;

	return 0;
}

/*
 * hm_tx_count -- returns number of elements
 */
size_t
hm_tx_count(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap)
{
	return D_RO(hashmap)->count;
}

/*
 * hm_tx_init -- recovers hashmap state, called after pmemobj_open
 */
int
hm_tx_init(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap)
{
	srand(D_RO(hashmap)->seed);
	return 0;
}

/*
 * hm_tx_new -- allocates new hashmap
 */
int
hm_tx_new(PMEMobjpool *pop, TOID(struct hashmap_tx) *map, void *arg)
{
	struct hashmap_args *args = arg;
	int ret = 0;
	TX_BEGIN(pop) {
		*map = TX_ZNEW(struct hashmap_tx);

		uint32_t seed = args ? args->seed : 0;
		create_hashmap(pop, *map, seed);
	} TX_ONABORT {
		ret = -1;
	} TX_END

	return ret;
}

/*
 * hm_tx_check -- checks if specified persistent object is an
 * instance of hashmap
 */
int
hm_tx_check(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap)
{
	return !TOID_VALID(hashmap);
}

/*
 * hm_tx_cmd -- execute cmd for hashmap
 */
int
hm_tx_cmd(PMEMobjpool *pop, TOID(struct hashmap_tx) hashmap,
		unsigned cmd, uint64_t arg)
{
	switch (cmd) {
		case HASHMAP_CMD_REBUILD:
			hm_tx_rebuild(pop, hashmap, arg);
			return 0;
		case HASHMAP_CMD_DEBUG:
			if (!arg)
				return -EINVAL;
			hm_tx_debug(pop, hashmap, (FILE *)arg);
			return 0;
		default:
			return -EINVAL;
	}
}
