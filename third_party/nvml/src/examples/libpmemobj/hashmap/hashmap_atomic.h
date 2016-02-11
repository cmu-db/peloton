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
#ifndef	HASHMAP_ATOMIC_H
#define	HASHMAP_ATOMIC_H

#include <stddef.h>
#include <stdint.h>
#include <hashmap.h>
#include <libpmemobj.h>

#ifndef	HASHMAP_ATOMIC_TYPE_OFFSET
#define	HASHMAP_ATOMIC_TYPE_OFFSET 1000
#endif

struct hashmap_atomic;
TOID_DECLARE(struct hashmap_atomic, HASHMAP_ATOMIC_TYPE_OFFSET + 0);

int hm_atomic_check(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap);
int hm_atomic_new(PMEMobjpool *pop, TOID(struct hashmap_atomic) *map,
		void *arg);
int hm_atomic_init(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap);
int hm_atomic_insert(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		uint64_t key, PMEMoid value);
PMEMoid hm_atomic_remove(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		uint64_t key);
PMEMoid hm_atomic_get(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		uint64_t key);
int hm_atomic_lookup(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		uint64_t key);
int hm_atomic_foreach(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
	int (*cb)(uint64_t key, PMEMoid value, void *arg), void *arg);
size_t hm_atomic_count(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap);
int hm_atomic_cmd(PMEMobjpool *pop, TOID(struct hashmap_atomic) hashmap,
		unsigned cmd, uint64_t arg);

#endif /* HASHMAP_ATOMIC_H */
