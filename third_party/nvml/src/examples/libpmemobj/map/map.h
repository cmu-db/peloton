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
 * map.h -- common interface for maps
 */

#ifndef MAP_H
#define	MAP_H

#include <libpmemobj.h>

#ifndef	MAP_TYPE_OFFSET
#define	MAP_TYPE_OFFSET 1000
#endif

TOID_DECLARE(struct map, MAP_TYPE_OFFSET + 0);

struct map;
struct map_ctx;

struct map_ops {
	int (*check)(PMEMobjpool *pop, TOID(struct map) map);
	int (*new)(PMEMobjpool *pop, TOID(struct map) *map, void *arg);
	int (*delete)(PMEMobjpool *pop, TOID(struct map) *map);
	int (*init)(PMEMobjpool *pop, TOID(struct map) map);
	int (*insert)(PMEMobjpool *pop, TOID(struct map) map,
			uint64_t key, PMEMoid value);
	int (*insert_new)(PMEMobjpool *pop, TOID(struct map) map,
			uint64_t key, size_t size,
		unsigned int type_num,
		void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
		void *arg);
	PMEMoid (*remove)(PMEMobjpool *pop, TOID(struct map) map,
			uint64_t key);
	int (*remove_free)(PMEMobjpool *pop, TOID(struct map) map,
			uint64_t key);
	int (*clear)(PMEMobjpool *pop, TOID(struct map) map);
	PMEMoid (*get)(PMEMobjpool *pop, TOID(struct map) map,
			uint64_t key);
	int (*lookup)(PMEMobjpool *pop, TOID(struct map) map,
			uint64_t key);
	int (*foreach)(PMEMobjpool *pop, TOID(struct map) map,
			int (*cb)(uint64_t key, PMEMoid value, void *arg),
			void *arg);
	int (*is_empty)(PMEMobjpool *pop, TOID(struct map) map);
	size_t (*count)(PMEMobjpool *pop, TOID(struct map) map);
	int (*cmd)(PMEMobjpool *pop, TOID(struct map) map,
			unsigned cmd, uint64_t arg);
};

struct map_ctx {
	PMEMobjpool *pop;
	const struct map_ops *ops;
};

struct map_ctx *map_ctx_init(const struct map_ops *ops, PMEMobjpool *pop);
void map_ctx_free(struct map_ctx *mapc);
int map_check(struct map_ctx *mapc, TOID(struct map) map);
int map_new(struct map_ctx *mapc, TOID(struct map) *map, void *arg);
int map_delete(struct map_ctx *mapc, TOID(struct map) *map);
int map_init(struct map_ctx *mapc, TOID(struct map) map);
int map_insert(struct map_ctx *mapc, TOID(struct map) map,
		uint64_t key, PMEMoid value);
int map_insert_new(struct map_ctx *mapc, TOID(struct map) map,
		uint64_t key, size_t size,
		unsigned int type_num,
		void (*constructor)(PMEMobjpool *pop, void *ptr, void *arg),
		void *arg);
PMEMoid map_remove(struct map_ctx *mapc, TOID(struct map) map, uint64_t key);
int map_remove_free(struct map_ctx *mapc, TOID(struct map) map, uint64_t key);
int map_clear(struct map_ctx *mapc, TOID(struct map) map);
PMEMoid map_get(struct map_ctx *mapc, TOID(struct map) map, uint64_t key);
int map_lookup(struct map_ctx *mapc, TOID(struct map) map, uint64_t key);
int map_foreach(struct map_ctx *mapc, TOID(struct map) map,
		int (*cb)(uint64_t key, PMEMoid value, void *arg),
		void *arg);
int map_is_empty(struct map_ctx *mapc, TOID(struct map) map);
size_t map_count(struct map_ctx *mapc, TOID(struct map) map);
int map_cmd(struct map_ctx *mapc, TOID(struct map) map,
		unsigned cmd, uint64_t arg);

#endif /* MAP_H */
