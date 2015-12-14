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
 * map_hashmap_atomic.c -- common interface for maps
 */

#include <map.h>
#include <hashmap_atomic.h>

/*
 * map_hm_atomic_check -- wrapper for hm_atomic_check
 */
static int
map_hm_atomic_check(PMEMobjpool *pop, TOID(struct map) map)
{
	TOID(struct hashmap_atomic) hashmap_atomic;
	TOID_ASSIGN(hashmap_atomic, map.oid);

	return hm_atomic_check(pop, hashmap_atomic);
}

/*
 * map_hm_atomic_count -- wrapper for hm_atomic_count
 */
static size_t
map_hm_atomic_count(PMEMobjpool *pop, TOID(struct map) map)
{
	TOID(struct hashmap_atomic) hashmap_atomic;
	TOID_ASSIGN(hashmap_atomic, map.oid);

	return hm_atomic_count(pop, hashmap_atomic);
}

/*
 * map_hm_atomic_init -- wrapper for hm_atomic_init
 */
static int
map_hm_atomic_init(PMEMobjpool *pop, TOID(struct map) map)
{
	TOID(struct hashmap_atomic) hashmap_atomic;
	TOID_ASSIGN(hashmap_atomic, map.oid);

	return hm_atomic_init(pop, hashmap_atomic);
}

/*
 * map_hm_atomic_new -- wrapper for hm_atomic_new
 */
static int
map_hm_atomic_new(PMEMobjpool *pop, TOID(struct map) *map, void *arg)
{
	TOID(struct hashmap_atomic) *hashmap_atomic =
		(TOID(struct hashmap_atomic) *)map;

	return hm_atomic_new(pop, hashmap_atomic, arg);
}

/*
 * map_hm_atomic_insert -- wrapper for hm_atomic_insert
 */
static int
map_hm_atomic_insert(PMEMobjpool *pop, TOID(struct map) map,
		uint64_t key, PMEMoid value)
{
	TOID(struct hashmap_atomic) hashmap_atomic;
	TOID_ASSIGN(hashmap_atomic, map.oid);

	return hm_atomic_insert(pop, hashmap_atomic, key, value);
}

/*
 * map_hm_atomic_remove -- wrapper for hm_atomic_remove
 */
static PMEMoid
map_hm_atomic_remove(PMEMobjpool *pop, TOID(struct map) map, uint64_t key)
{
	TOID(struct hashmap_atomic) hashmap_atomic;
	TOID_ASSIGN(hashmap_atomic, map.oid);

	return hm_atomic_remove(pop, hashmap_atomic, key);
}

/*
 * map_hm_atomic_get -- wrapper for hm_atomic_get
 */
static PMEMoid
map_hm_atomic_get(PMEMobjpool *pop, TOID(struct map) map, uint64_t key)
{
	TOID(struct hashmap_atomic) hashmap_atomic;
	TOID_ASSIGN(hashmap_atomic, map.oid);

	return hm_atomic_get(pop, hashmap_atomic, key);
}

/*
 * map_hm_atomic_lookup -- wrapper for hm_atomic_lookup
 */
static int
map_hm_atomic_lookup(PMEMobjpool *pop, TOID(struct map) map, uint64_t key)
{
	TOID(struct hashmap_atomic) hashmap_atomic;
	TOID_ASSIGN(hashmap_atomic, map.oid);

	return hm_atomic_lookup(pop, hashmap_atomic, key);
}

/*
 * map_hm_atomic_foreach -- wrapper for hm_atomic_foreac
 */
static int
map_hm_atomic_foreach(PMEMobjpool *pop, TOID(struct map) map,
		int (*cb)(uint64_t key, PMEMoid value, void *arg),
		void *arg)
{
	TOID(struct hashmap_atomic) hashmap_atomic;
	TOID_ASSIGN(hashmap_atomic, map.oid);

	return hm_atomic_foreach(pop, hashmap_atomic, cb, arg);
}

/*
 * map_hm_atomic_cmd -- wrapper for hm_atomic_cmd
 */
static int
map_hm_atomic_cmd(PMEMobjpool *pop, TOID(struct map) map,
		unsigned cmd, uint64_t arg)
{
	TOID(struct hashmap_atomic) hashmap_atomic;
	TOID_ASSIGN(hashmap_atomic, map.oid);

	return hm_atomic_cmd(pop, hashmap_atomic, cmd, arg);
}

struct map_ops hashmap_atomic_ops = {
	.check		= map_hm_atomic_check,
	.new		= map_hm_atomic_new,
	.init		= map_hm_atomic_init,
	.insert		= map_hm_atomic_insert,
	.remove		= map_hm_atomic_remove,
	.get		= map_hm_atomic_get,
	.lookup		= map_hm_atomic_lookup,
	.foreach	= map_hm_atomic_foreach,
	.count		= map_hm_atomic_count,
	.cmd		= map_hm_atomic_cmd,
};
