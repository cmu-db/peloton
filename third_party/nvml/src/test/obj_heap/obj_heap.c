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
 * obj_heap.c -- unit test for bucket
 */
#include "libpmem.h"
#include "libpmemobj.h"
#include "util.h"
#include "redo.h"
#include "heap_layout.h"
#include "heap.h"
#include "bucket.h"
#include "lane.h"
#include "list.h"
#include "obj.h"
#include "pmalloc.h"
#include "unittest.h"

#define	MOCK_POOL_SIZE PMEMOBJ_MIN_POOL

#define	MAX_BLOCKS 3

struct mock_pop {
	PMEMobjpool p;
	void *heap;
};

static void
obj_heap_persist(PMEMobjpool *pop, void *ptr, size_t sz)
{
	pmem_msync(ptr, sz);
}

static void
test_heap()
{
	struct mock_pop *mpop = Malloc(MOCK_POOL_SIZE);
	PMEMobjpool *pop = &mpop->p;
	memset(pop, 0, MOCK_POOL_SIZE);
	pop->size = MOCK_POOL_SIZE;
	pop->heap_size = MOCK_POOL_SIZE - sizeof (PMEMobjpool);
	pop->heap_offset = (uint64_t)((uint64_t)&mpop->heap - (uint64_t)mpop);
	pop->persist = obj_heap_persist;

	ASSERT(heap_check(pop) != 0);
	ASSERT(heap_init(pop) == 0);
	ASSERT(heap_boot(pop) == 0);
	ASSERT(pop->heap != NULL);

	Lane_idx = 0;

	struct bucket *b_small = heap_get_best_bucket(pop, 0);
	struct bucket *b_big = heap_get_best_bucket(pop, 1024);

	ASSERT(bucket_unit_size(b_small) < bucket_unit_size(b_big));

	struct bucket *b_def = heap_get_best_bucket(pop, CHUNKSIZE);
	ASSERT(bucket_unit_size(b_def) == CHUNKSIZE);

	/* new small buckets should be empty */
	ASSERT(bucket_is_empty(b_small));
	ASSERT(bucket_is_empty(b_big));

	struct memory_block blocks[MAX_BLOCKS] = {
		{0, 0, 1, 0},
		{0, 0, 1, 0},
		{0, 0, 1, 0}
	};

	for (int i = 0; i < MAX_BLOCKS; ++i) {
		heap_get_bestfit_block(pop, b_def, &blocks[i]);
		ASSERT(blocks[i].block_off == 0);
	}

	struct memory_block *blocksp[MAX_BLOCKS] = {NULL};

	struct memory_block prev;
	heap_get_adjacent_free_block(pop, &prev, blocks[1], 1);
	ASSERT(prev.chunk_id == blocks[0].chunk_id);
	blocksp[0] = &prev;

	struct memory_block cnt;
	heap_get_adjacent_free_block(pop, &cnt, blocks[0], 0);
	ASSERT(cnt.chunk_id == blocks[1].chunk_id);
	blocksp[1] = &cnt;

	struct memory_block next;
	heap_get_adjacent_free_block(pop, &next, blocks[1], 0);
	ASSERT(next.chunk_id == blocks[2].chunk_id);
	blocksp[2] = &next;

	void *hdr;
	uint64_t op_result;
	struct memory_block result =
		heap_coalesce(pop, blocksp, MAX_BLOCKS, HEAP_OP_FREE, &hdr,
			&op_result);
	*((uint64_t *)hdr) = op_result;
	ASSERT(result.size_idx == 3);
	ASSERT(result.chunk_id == prev.chunk_id);

	ASSERT(heap_check(pop) == 0);
	ASSERT(heap_cleanup(pop) == 0);
	ASSERT(pop->heap == NULL);

	Free(mpop);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_heap");

	test_heap();

	DONE(NULL);
}
