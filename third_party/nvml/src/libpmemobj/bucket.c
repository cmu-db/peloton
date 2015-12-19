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
 * bucket.c -- bucket implementation
 *
 * Buckets manage volatile state of the heap. They are the abstraction layer
 * between the heap-managed chunks/runs and memory allocations.
 */

#include <errno.h>

#include "libpmem.h"
#include "libpmemobj.h"
#include "util.h"
#include "out.h"
#include "redo.h"
#include "heap_layout.h"
#include "heap.h"
#include "bucket.h"
#include "ctree.h"
#include "lane.h"
#include "list.h"
#include "obj.h"
#include "valgrind_internal.h"

/*
 * The elements in the tree are sorted by the key and it's vital that the
 * order is by size, hence the order of the pack arguments.
 */
#define	CHUNK_KEY_PACK(z, c, b, s)\
((uint64_t)(s) << 48 | (uint64_t)(b) << 32 | (uint64_t)(c) << 16 | (z))

#define	CHUNK_KEY_GET_ZONE_ID(k)\
((uint16_t)((k & 0xFFFF)))

#define	CHUNK_KEY_GET_CHUNK_ID(k)\
((uint16_t)((k & 0xFFFF0000) >> 16))

#define	CHUNK_KEY_GET_BLOCK_OFF(k)\
((uint16_t)((k & 0xFFFF00000000) >> 32))

#define	CHUNK_KEY_GET_SIZE_IDX(k)\
((uint16_t)((k & 0xFFFF000000000000) >> 48))

struct bucket {
	size_t unit_size;
	unsigned unit_max;
	struct ctree *tree;
	pthread_mutex_t lock;
	uint64_t bitmap_lastval;
	unsigned bitmap_nval;
	unsigned bitmap_nallocs;
};

/*
 * bucket_new -- allocates and initializes bucket instance
 */
struct bucket *
bucket_new(size_t unit_size, unsigned unit_max)
{
	ASSERT(unit_size > 0);

	struct bucket *b = Malloc(sizeof (*b));
	if (b == NULL)
		goto error_bucket_malloc;

	b->tree = ctree_new();
	if (b->tree == NULL)
		goto error_tree_new;

	if ((errno = pthread_mutex_init(&b->lock, NULL)) != 0) {
		ERR("!pthread_mutex_init");
		goto error_mutex_init;
	}

	b->unit_size = unit_size;
	b->unit_max = unit_max;

	if (bucket_is_small(b)) {
		ASSERT(RUNSIZE / unit_size <= UINT32_MAX);
		b->bitmap_nallocs = (unsigned)(RUNSIZE / unit_size);

		ASSERT(b->bitmap_nallocs <= RUN_BITMAP_SIZE);
		unsigned unused_bits = RUN_BITMAP_SIZE - b->bitmap_nallocs;

		unsigned unused_values = unused_bits / BITS_PER_VALUE;

		ASSERT(MAX_BITMAP_VALUES >= unused_values);
		b->bitmap_nval = MAX_BITMAP_VALUES - unused_values;

		ASSERT(unused_bits >= unused_values * BITS_PER_VALUE);
		unused_bits -= unused_values * BITS_PER_VALUE;

		b->bitmap_lastval = unused_bits ?
			(((1ULL << unused_bits) - 1ULL) <<
				(BITS_PER_VALUE - unused_bits)) : 0;
	} else {
		b->bitmap_nval = 0;
		b->bitmap_lastval = 0;
		b->bitmap_nallocs = 0;
	}

	return b;

error_mutex_init:
	ctree_delete(b->tree);
error_tree_new:
	Free(b);
error_bucket_malloc:
	return NULL;
}

/*
 * bucket_delete -- cleanups and deallocates bucket instance
 */
void
bucket_delete(struct bucket *b)
{
	if ((errno = pthread_mutex_destroy(&b->lock)) != 0)
		ERR("!pthread_mutex_destroy");

	ctree_delete(b->tree);
	Free(b);
}

/*
 * bucket_bitmap_nallocs -- returns maximum number of allocations per run
 */
unsigned
bucket_bitmap_nallocs(struct bucket *b)
{
	return b->bitmap_nallocs;
}

/*
 * bucket_bitmap_nval -- returns maximum number of bitmap u64 values
 */
unsigned
bucket_bitmap_nval(struct bucket *b)
{
	return b->bitmap_nval;
}

/*
 * bucket_bitmap_lastval -- returns the last value of an empty run bitmap
 */
uint64_t
bucket_bitmap_lastval(struct bucket *b)
{
	return b->bitmap_lastval;
}

/*
 * bucket_unit_size -- returns unit size of a bucket
 */
size_t
bucket_unit_size(struct bucket *b)
{
	return b->unit_size;
}

/*
 * bucket_unit_max -- returns unit max of a bucket
 */
unsigned
bucket_unit_max(struct bucket *b)
{
	return b->unit_max;
}

/*
 * bucket_is_small -- returns whether the bucket handles small allocations
 */
int
bucket_is_small(struct bucket *b)
{
	return b->unit_size != CHUNKSIZE;
}

/*
 * bucket_calc_units -- calculates the number of units requested size requires
 */
uint32_t
bucket_calc_units(struct bucket *b, size_t size)
{
	ASSERT(size != 0);
	size = ((size - 1) / b->unit_size) + 1;
	ASSERT(size <= UINT32_MAX);
	return (uint32_t)size;
}

/*
 * bucket_insert_block -- inserts a new memory block into the container
 */
void
bucket_insert_block(PMEMobjpool *pop, struct bucket *b, struct memory_block m)
{
	ASSERT(m.chunk_id < MAX_CHUNK);
	ASSERT(m.zone_id < UINT16_MAX);
	ASSERT(m.size_idx != 0);

#ifdef USE_VG_MEMCHECK
	if (On_valgrind) {
		size_t rsize = m.size_idx * bucket_unit_size(b);
		void *block_data = heap_get_block_data(pop, m);
		VALGRIND_DO_MAKE_MEM_NOACCESS(pop, block_data, rsize);
	}
#endif

	uint64_t key = CHUNK_KEY_PACK(m.zone_id, m.chunk_id, m.block_off,
				m.size_idx);

	int ret = ctree_insert(b->tree, key, 0);
	if (ret != 0) {
		ERR("Failed to create volatile state of memory block");
		ASSERT(0);
	}
}

/*
 * bucket_get_rm_block_bestfit --
 *	removes and returns the best-fit memory block for size
 */
int
bucket_get_rm_block_bestfit(struct bucket *b, struct memory_block *m)
{
	uint64_t key = CHUNK_KEY_PACK(m->zone_id, m->chunk_id, m->block_off,
			m->size_idx);

	if ((key = ctree_remove(b->tree, key, 0)) == 0)
		return ENOMEM;

	m->chunk_id = CHUNK_KEY_GET_CHUNK_ID(key);
	m->zone_id = CHUNK_KEY_GET_ZONE_ID(key);
	m->block_off = CHUNK_KEY_GET_BLOCK_OFF(key);
	m->size_idx = CHUNK_KEY_GET_SIZE_IDX(key);


	return 0;
}

/*
 * bucket_get_rm_block_exact -- removes exact match memory block
 */
int
bucket_get_rm_block_exact(struct bucket *b, struct memory_block m)
{
	uint64_t key = CHUNK_KEY_PACK(m.zone_id, m.chunk_id, m.block_off,
			m.size_idx);

	if ((key = ctree_remove(b->tree, key, 1)) == 0)
		return ENOMEM;

	return 0;
}

/*
 * bucket_get_block_exact -- finds exact match memory block
 */
int
bucket_get_block_exact(struct bucket *b, struct memory_block m)
{
	uint64_t key = CHUNK_KEY_PACK(m.zone_id, m.chunk_id, m.block_off,
			m.size_idx);

	return ctree_find(b->tree, key) == key ? 0 : ENOMEM;
}

/*
 * bucket_is_empty -- checks whether the bucket is empty
 */
int
bucket_is_empty(struct bucket *b)
{
	return ctree_is_empty(b->tree);
}

/*
 * bucket_lock -- acquire bucket lock
 */
int
bucket_lock(struct bucket *b)
{
	return pthread_mutex_lock(&b->lock);
}

/*
 * bucket_unlock -- release bucket lock
 */
void
bucket_unlock(struct bucket *b)
{
	if (pthread_mutex_unlock(&b->lock) != 0) {
		ERR("pthread_mutex_unlock");
		ASSERT(0);
	}
}
