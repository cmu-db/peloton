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
 * map_rbtree.c -- common interface for maps
 */

#include <map.h>
#include <rbtree_map.h>

/*
 * map_rbtree_check -- wrapper for rbtree_map_check
 */
static int
map_rbtree_check(PMEMobjpool *pop, TOID(struct map) map)
{
	TOID(struct rbtree_map) rbtree_map;
	TOID_ASSIGN(rbtree_map, map.oid);

	return rbtree_map_check(pop, rbtree_map);
}

/*
 * map_rbtree_new -- wrapper for rbtree_map_new
 */
static int
map_rbtree_new(PMEMobjpool *pop, TOID(struct map) *map, void *arg)
{
	TOID(struct rbtree_map) *rbtree_map =
		(TOID(struct rbtree_map) *)map;

	return rbtree_map_new(pop, rbtree_map, arg);
}

/*
 * map_rbtree_delete -- wrapper for rbtree_map_delete
 */
static int
map_rbtree_delete(PMEMobjpool *pop, TOID(struct map) *map)
{
	TOID(struct rbtree_map) *rbtree_map =
		(TOID(struct rbtree_map) *)map;

	return rbtree_map_delete(pop, rbtree_map);
}

/*
 * map_rbtree_insert -- wrapper for rbtree_map_insert
 */
static int
map_rbtree_insert(PMEMobjpool *pop, TOID(struct map) map,
		uint64_t key, PMEMoid value)
{
	TOID(struct rbtree_map) rbtree_map;
	TOID_ASSIGN(rbtree_map, map.oid);

	return rbtree_map_insert(pop, rbtree_map, key, value);
}

/*
 * map_rbtree_insert_new -- wrapper for rbtree_map_insert_new
 */
static int
map_rbtree_insert_new(PMEMobjpool *pop, TOID(struct map) map,
		uint64_t key, size_t size,
		unsigned int type_num,
		void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
		void *arg)
{
	TOID(struct rbtree_map) rbtree_map;
	TOID_ASSIGN(rbtree_map, map.oid);

	return rbtree_map_insert_new(pop, rbtree_map, key, size,
			type_num, constructor, arg);
}

/*
 * map_rbtree_remove -- wrapper for rbtree_map_remove
 */
static PMEMoid
map_rbtree_remove(PMEMobjpool *pop, TOID(struct map) map, uint64_t key)
{
	TOID(struct rbtree_map) rbtree_map;
	TOID_ASSIGN(rbtree_map, map.oid);

	return rbtree_map_remove(pop, rbtree_map, key);
}

/*
 * map_rbtree_remove_free -- wrapper for rbtree_map_remove_free
 */
static int
map_rbtree_remove_free(PMEMobjpool *pop, TOID(struct map) map, uint64_t key)
{
	TOID(struct rbtree_map) rbtree_map;
	TOID_ASSIGN(rbtree_map, map.oid);

	return rbtree_map_remove_free(pop, rbtree_map, key);
}

/*
 * map_rbtree_clear -- wrapper for rbtree_map_clear
 */
static int
map_rbtree_clear(PMEMobjpool *pop, TOID(struct map) map)
{
	TOID(struct rbtree_map) rbtree_map;
	TOID_ASSIGN(rbtree_map, map.oid);

	return rbtree_map_clear(pop, rbtree_map);
}

/*
 * map_rbtree_get -- wrapper for rbtree_map_get
 */
static PMEMoid
map_rbtree_get(PMEMobjpool *pop, TOID(struct map) map, uint64_t key)
{
	TOID(struct rbtree_map) rbtree_map;
	TOID_ASSIGN(rbtree_map, map.oid);

	return rbtree_map_get(pop, rbtree_map, key);
}

/*
 * map_rbtree_lookup -- wrapper for rbtree_map_lookup
 */
static int
map_rbtree_lookup(PMEMobjpool *pop, TOID(struct map) map, uint64_t key)
{
	TOID(struct rbtree_map) rbtree_map;
	TOID_ASSIGN(rbtree_map, map.oid);

	return rbtree_map_lookup(pop, rbtree_map, key);
}

/*
 * map_rbtree_foreach -- wrapper for rbtree_map_foreac
 */
static int
map_rbtree_foreach(PMEMobjpool *pop, TOID(struct map) map,
		int (*cb)(uint64_t key, PMEMoid value, void *arg),
		void *arg)
{
	TOID(struct rbtree_map) rbtree_map;
	TOID_ASSIGN(rbtree_map, map.oid);

	return rbtree_map_foreach(pop, rbtree_map, cb, arg);
}

/*
 * map_rbtree_is_empty -- wrapper for rbtree_map_is_empty
 */
static int
map_rbtree_is_empty(PMEMobjpool *pop, TOID(struct map) map)
{
	TOID(struct rbtree_map) rbtree_map;
	TOID_ASSIGN(rbtree_map, map.oid);

	return rbtree_map_is_empty(pop, rbtree_map);
}

struct map_ops rbtree_map_ops = {
	.check		= map_rbtree_check,
	.new		= map_rbtree_new,
	.delete		= map_rbtree_delete,
	.init		= NULL,
	.insert		= map_rbtree_insert,
	.insert_new	= map_rbtree_insert_new,
	.remove		= map_rbtree_remove,
	.remove_free	= map_rbtree_remove_free,
	.clear		= map_rbtree_clear,
	.get		= map_rbtree_get,
	.lookup		= map_rbtree_lookup,
	.is_empty	= map_rbtree_is_empty,
	.foreach	= map_rbtree_foreach,
	.count		= NULL,
	.cmd		= NULL,
};
