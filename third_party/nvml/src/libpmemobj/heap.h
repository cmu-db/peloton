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
 * heap.h -- internal definitions for heap
 */

#define	MAX_BUCKETS 6
#define	DEFAULT_BUCKET 5
#define	RUN_UNIT_MAX 4U

enum heap_op {
	HEAP_OP_ALLOC,
	HEAP_OP_FREE,

	MAX_HEAP_OP
};

struct memory_block {
	uint32_t chunk_id;
	uint32_t zone_id;
	uint32_t size_idx;
	uint16_t block_off;
};

struct bucket_cache;

struct bucket *heap_get_best_bucket(PMEMobjpool *pop, size_t size);
struct bucket *heap_get_chunk_bucket(PMEMobjpool *pop,
		uint32_t chunk_id, uint32_t zone_id);
struct bucket *heap_get_auxiliary_bucket(PMEMobjpool *pop, size_t size);
void heap_drain_to_auxiliary(PMEMobjpool *pop, struct bucket *auxb,
	uint32_t size_idx);
void *heap_get_block_data(PMEMobjpool *pop, struct memory_block m);
void *heap_get_block_header(PMEMobjpool *pop, struct memory_block m,
	enum heap_op op, uint64_t *op_result);
struct memory_block heap_coalesce(PMEMobjpool *pop,
	struct memory_block *blocks[], int n, enum heap_op op,
	void **hdr, uint64_t *op_result);

int heap_get_adjacent_free_block(PMEMobjpool *pop, struct memory_block *m,
	struct memory_block cnt, int prev);

int heap_lock_if_run(PMEMobjpool *pop, struct memory_block m);
int heap_unlock_if_run(PMEMobjpool *pop, struct memory_block m);

int heap_get_bestfit_block(PMEMobjpool *pop, struct bucket *b,
	struct memory_block *m);
int heap_get_exact_block(PMEMobjpool *pop, struct bucket *b,
	struct memory_block *m, uint32_t new_size_idx);
int heap_degrade_run_if_empty(PMEMobjpool *pop, struct bucket *b,
	struct memory_block m);

struct memory_block heap_free_block(PMEMobjpool *pop, struct bucket *b,
	struct memory_block m, void *hdr, uint64_t *op_result);

#ifdef DEBUG
int heap_block_is_allocated(PMEMobjpool *pop, struct memory_block m);
#endif /* DEBUG */
