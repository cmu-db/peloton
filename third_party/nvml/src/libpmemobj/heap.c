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
 * heap.c -- heap implementation
 */

#include <errno.h>
#include <sys/queue.h>
#include <unistd.h>

#include "libpmem.h"
#include "libpmemobj.h"
#include "util.h"
#include "heap.h"
#include "redo.h"
#include "heap_layout.h"
#include "bucket.h"
#include "lane.h"
#include "out.h"
#include "list.h"
#include "obj.h"
#include "pmalloc.h"
#include "valgrind_internal.h"

#define	MAX_RUN_LOCKS 1024

#define	USE_PER_LANE_BUCKETS

#define	EMPTY_MEMORY_BLOCK (struct memory_block)\
{0, 0, 0, 0}

#define	NCACHES_PER_CPU	2

/*
 * Percentage of memory block units from a single run that can be migrated
 * from a cache bucket to auxiliary bucket in a single drain call.
 */
#define	MAX_UNITS_PCT_DRAINED_CACHE 0.2 /* 20% */

/*
 * Same as MAX_UNITS_PCT_DRAINED_CACHE, but for all of the cache buckets
 * combined.
 */
#define	MAX_UNITS_PCT_DRAINED_TOTAL 2 /* 200% */

static struct {
	size_t unit_size;
	unsigned unit_max;
} bucket_proto[MAX_BUCKETS];

#define	BIT_IS_CLR(a, i)	(!((a) & (1ULL << (i))))

struct active_run {
	uint32_t chunk_id;
	uint32_t zone_id;
	SLIST_ENTRY(active_run) run;
};

struct bucket_cache {
	struct bucket *buckets[MAX_BUCKETS - 1]; /* no default bucket */
};

struct pmalloc_heap {
	struct heap_layout *layout;
	struct bucket *buckets[MAX_BUCKETS];
	/* runs are lazy-loaded, removed from this list on-demand */
	SLIST_HEAD(arun, active_run) active_runs[MAX_BUCKETS - 1];
	pthread_mutex_t active_run_lock;
	uint8_t *bucket_map;
	pthread_mutex_t run_locks[MAX_RUN_LOCKS];
	unsigned max_zone;
	unsigned zones_exhausted;
	size_t last_run_max_size;

	struct bucket_cache *caches;
	unsigned ncaches;
	uint32_t last_drained[MAX_BUCKETS - 1];
};

/*
 * bucket_cache_init -- (internal) creates new bucket cache instance
 */
static int
bucket_cache_init(PMEMobjpool *pop, struct bucket_cache *cache)
{
	int i;
	for (i = 0; i < MAX_BUCKETS - 1; ++i) { /* skip the default bucket */
		ASSERTne(bucket_proto[i].unit_size, 0);
		/* protos should already be filled by heap_buckets_init */
		cache->buckets[i] = bucket_new(bucket_proto[i].unit_size,
					bucket_proto[i].unit_max);
		if (cache->buckets[i] == NULL)
			goto error_bucket_new;
	}

	return 0;

error_bucket_new:
	for (i = i - 1; i >= 0; --i)
		bucket_delete(cache->buckets[i]);

	Free(cache);

	return ENOMEM;
}

/*
 * bucket_cache_destroy -- (internal) destroys bucket cache instance
 */
static void
bucket_cache_destroy(struct bucket_cache *cache)
{
	for (int i = 0; i < MAX_BUCKETS - 1; ++i)
		bucket_delete(cache->buckets[i]);
}

/*
 * heap_get_layout -- (internal) returns pointer to the heap layout
 */
static struct heap_layout *
heap_get_layout(PMEMobjpool *pop)
{
	return (void *)((char *)pop + pop->heap_offset);
}

/*
 * heap_max_zone -- (internal) calculates how many zones can the heap fit
 */
static unsigned
heap_max_zone(size_t size)
{
	unsigned max_zone = 0;
	size -= sizeof (struct heap_header);

	while (size >= ZONE_MIN_SIZE) {
		max_zone++;
		size -= size <= ZONE_MAX_SIZE ? size : ZONE_MAX_SIZE;
	}

	return max_zone;
}

/*
 * get_zone_size_idx -- (internal) calculates zone size index
 */
static uint32_t
get_zone_size_idx(uint32_t zone_id, unsigned max_zone, size_t heap_size)
{
	ASSERT(max_zone > 0);
	if (zone_id < max_zone - 1)
		return MAX_CHUNK - 1;

	ASSERT(heap_size >= zone_id * ZONE_MAX_SIZE);
	size_t zone_raw_size = heap_size - zone_id * ZONE_MAX_SIZE;

	ASSERT(zone_raw_size >= (sizeof (struct zone_header) +
			sizeof (struct chunk_header) * MAX_CHUNK));
	zone_raw_size -= sizeof (struct zone_header) +
		sizeof (struct chunk_header) * MAX_CHUNK;

	size_t zone_size_idx = zone_raw_size / CHUNKSIZE;
	ASSERT(zone_size_idx <= UINT32_MAX);

	return (uint32_t)zone_size_idx;
}

/*
 * heap_chunk_write_footer -- (internal) writes a chunk footer
 */
static void
heap_chunk_write_footer(PMEMobjpool *pop, struct chunk_header *hdr,
		uint32_t size_idx)
{
	if (size_idx == 1) /* that would overwrite the header */
		return;

	VALGRIND_DO_MAKE_MEM_UNDEFINED(pop, hdr + size_idx - 1, sizeof (*hdr));

	struct chunk_header f = *hdr;
	f.type = CHUNK_TYPE_FOOTER;
	f.size_idx = size_idx;
	*(hdr + size_idx - 1) = f;
	/* no need to persist, footers are recreated in heap_populate_buckets */
	VALGRIND_SET_CLEAN(hdr + size_idx - 1, sizeof (f));
}

/*
 * heap_chunk_init -- (internal) writes chunk header
 */
static void
heap_chunk_init(PMEMobjpool *pop, struct chunk_header *hdr,
	uint16_t type, uint32_t size_idx)
{
	struct chunk_header nhdr = {
		.type = type,
		.flags = 0,
		.size_idx = size_idx
	};
	VALGRIND_DO_MAKE_MEM_UNDEFINED(pop, hdr, sizeof (*hdr));

	*hdr = nhdr; /* write the entire header (8 bytes) at once */
	pop->persist(pop, hdr, sizeof (*hdr));

	heap_chunk_write_footer(pop, hdr, size_idx);
}

/*
 * heap_zone_init -- (internal) writes zone's first chunk and header
 */
static void
heap_zone_init(PMEMobjpool *pop, uint32_t zone_id)
{
	struct zone *z = &pop->heap->layout->zones[zone_id];
	uint32_t size_idx = get_zone_size_idx(zone_id, pop->heap->max_zone,
			pop->heap_size);

	heap_chunk_init(pop, &z->chunk_headers[0], CHUNK_TYPE_FREE, size_idx);

	struct zone_header nhdr = {
		.size_idx = size_idx,
		.magic = ZONE_HEADER_MAGIC,
	};
	z->header = nhdr;  /* write the entire header (8 bytes) at once */
	pop->persist(pop, &z->header, sizeof (z->header));
}

/*
 * heap_init_run -- (internal) creates a run based on a chunk
 */
static void
heap_init_run(PMEMobjpool *pop, struct bucket *b, struct chunk_header *hdr,
	struct chunk_run *run)
{
	/* add/remove chunk_run and chunk_header to valgrind transaction */
	VALGRIND_ADD_TO_TX(run, sizeof (*run));
	run->block_size = bucket_unit_size(b);
	pop->persist(pop, &run->block_size, sizeof (run->block_size));

	ASSERT(hdr->type == CHUNK_TYPE_FREE);

	/* set all the bits */
	memset(run->bitmap, 0xFF, sizeof (run->bitmap));

	unsigned nval = bucket_bitmap_nval(b);
	ASSERT(nval > 0);
	/* clear only the bits available for allocations from this bucket */
	memset(run->bitmap, 0, sizeof (uint64_t) * (nval - 1));
	run->bitmap[nval - 1] = bucket_bitmap_lastval(b);
	VALGRIND_REMOVE_FROM_TX(run, sizeof (*run));

	pop->persist(pop, run->bitmap, sizeof (run->bitmap));

	VALGRIND_ADD_TO_TX(hdr, sizeof (*hdr));
	hdr->type = CHUNK_TYPE_RUN;
	VALGRIND_REMOVE_FROM_TX(hdr, sizeof (*hdr));

	pop->persist(pop, hdr, sizeof (*hdr));
}

/*
 * heap_run_insert -- (internal) inserts and splits a block of memory into a run
 */
static void
heap_run_insert(PMEMobjpool *pop, struct bucket *b, uint32_t chunk_id,
		uint32_t zone_id, uint32_t size_idx, uint16_t block_off)
{
	ASSERT(size_idx <= BITS_PER_VALUE);
	ASSERT(block_off + size_idx <= bucket_bitmap_nallocs(b));

	unsigned unit_max = bucket_unit_max(b);
	struct memory_block m = {chunk_id, zone_id,
		unit_max - (block_off % 4), block_off};

	if (m.size_idx > size_idx)
		m.size_idx = size_idx;

	do {
		bucket_insert_block(pop, b, m);
		ASSERT(m.size_idx <= UINT16_MAX);
		ASSERT(m.block_off + m.size_idx <= UINT16_MAX);
		m.block_off = (uint16_t)(m.block_off + (uint16_t)m.size_idx);
		size_idx -= m.size_idx;
		m.size_idx = size_idx > unit_max ? unit_max : size_idx;
	} while (size_idx != 0);
}

/*
 * heap_get_run_lock -- (internal) returns the lock associated with memory block
 */
static pthread_mutex_t *
heap_get_run_lock(PMEMobjpool *pop, uint32_t chunk_id)
{
	return &pop->heap->run_locks[chunk_id % MAX_RUN_LOCKS];
}

/*
 * heap_process_run_metadata -- (internal) parses the run bitmap
 */
static void
heap_process_run_metadata(PMEMobjpool *pop, struct bucket *b,
	struct chunk_run *run,
	uint32_t chunk_id, uint32_t zone_id)
{
	ASSERT(RUNSIZE / run->block_size <= UINT16_MAX);
	uint16_t run_bits = (uint16_t)(RUNSIZE / run->block_size);
	ASSERT(run_bits < (MAX_BITMAP_VALUES * BITS_PER_VALUE));
	uint16_t block_off = 0;
	uint16_t block_size_idx = 0;

	for (unsigned i = 0; i < bucket_bitmap_nval(b); ++i) {
		uint64_t v = run->bitmap[i];
		ASSERT(BITS_PER_VALUE * i <= UINT16_MAX);
		block_off = (uint16_t)(BITS_PER_VALUE * i);
		if (v == 0) {
			heap_run_insert(pop, b, chunk_id, zone_id,
				BITS_PER_VALUE, block_off);
			continue;
		} else if (v == ~(uint64_t)0) {
			continue;
		}

		for (unsigned j = 0; j < BITS_PER_VALUE; ++j) {
			if (BIT_IS_CLR(v, j)) {
				block_size_idx++;
			} else if (block_size_idx != 0) {
				ASSERT(block_off >= block_size_idx);

				heap_run_insert(pop, b, chunk_id, zone_id,
					block_size_idx,
					(uint16_t)(block_off - block_size_idx));
				block_size_idx = 0;
			}

			if ((block_off++) == run_bits) {
				i = MAX_BITMAP_VALUES;
				break;
			}
		}

		if (block_size_idx != 0) {
			ASSERT(block_off >= block_size_idx);

			heap_run_insert(pop, b, chunk_id, zone_id,
					block_size_idx,
					(uint16_t)(block_off - block_size_idx));
			block_size_idx = 0;
		}
	}
}

/*
 * heap_set_run_bucket -- (internal) sets the runtime bucket of a run
 */
static void
heap_set_run_bucket(struct chunk_run *run, struct bucket *b)
{
	VALGRIND_ADD_TO_TX(&run->bucket_vptr, sizeof (run->bucket_vptr));
	/* mark the bucket associated with this run */
	run->bucket_vptr = (uint64_t)b;
	VALGRIND_SET_CLEAN(&run->bucket_vptr, sizeof (run->bucket_vptr));
	VALGRIND_REMOVE_FROM_TX(&run->bucket_vptr, sizeof (run->bucket_vptr));
}

/*
 * heap_create_run -- (internal) initializes a new run on an existing free chunk
 */
static void
heap_create_run(PMEMobjpool *pop, struct bucket *b,
	uint32_t chunk_id, uint32_t zone_id)
{
	struct pmalloc_heap *h = pop->heap;
	struct zone *z = &h->layout->zones[zone_id];
	struct chunk_header *hdr = &z->chunk_headers[chunk_id];
	struct chunk_run *run = (struct chunk_run *)&z->chunks[chunk_id];

	ASSERT(hdr->size_idx == 1);
	ASSERT(hdr->type == CHUNK_TYPE_FREE);

	VALGRIND_DO_MAKE_MEM_UNDEFINED(pop, run, sizeof (*run));
	heap_set_run_bucket(run, b);
	heap_init_run(pop, b, hdr, run);
	heap_process_run_metadata(pop, b, run, chunk_id, zone_id);
}

/*
 * heap_populate_run_bucket -- (internal) split bitmap into memory blocks
 */
static void
heap_reuse_run(PMEMobjpool *pop, struct bucket *b,
	uint32_t chunk_id, uint32_t zone_id)
{
	pthread_mutex_lock(heap_get_run_lock(pop, chunk_id));

	struct pmalloc_heap *h = pop->heap;
	struct zone *z = &h->layout->zones[zone_id];
	struct chunk_header *hdr = &z->chunk_headers[chunk_id];
	struct chunk_run *run = (struct chunk_run *)&z->chunks[chunk_id];

	/* the run might have changed back to a chunk */
	if (hdr->type != CHUNK_TYPE_RUN)
		goto out;

	/*
	 * Between the call to this function and this moment a different
	 * thread might have already claimed this run.
	 */
	if (run->bucket_vptr != 0)
		goto out;

	heap_set_run_bucket(run, b);
	ASSERT(hdr->size_idx == 1);
	ASSERT(bucket_unit_size(b) == run->block_size);

	heap_process_run_metadata(pop, b, run, chunk_id, zone_id);

out:
	pthread_mutex_unlock(heap_get_run_lock(pop, chunk_id));
}

/*
 * heap_run_is_empty -- (internal) checks whether the run is completely dry
 */
static int
heap_run_is_empty(struct chunk_run *run)
{
	for (int i = 0; i < MAX_BITMAP_VALUES; ++i)
		if (run->bitmap[i] != ~(uint64_t)0)
			return 0;

	return 1;
}

/*
 * heap_register_active_run -- (internal) inserts a run for eventual reuse
 */
static void
heap_register_active_run(struct pmalloc_heap *h, struct chunk_run *run,
	uint32_t chunk_id, uint32_t zone_id)
{
	/* reset the volatile state of the run */
	run->bucket_vptr = 0;
	VALGRIND_SET_CLEAN(&run->bucket_vptr, sizeof (run->bucket_vptr));

	if (heap_run_is_empty(run))
		return;

	struct active_run *arun = Malloc(sizeof (*arun));
	if (arun == NULL) {
		ERR("Failed to register active run");
		ASSERT(0);
		return;
	}
	arun->chunk_id = chunk_id;
	arun->zone_id = zone_id;

	int bucket_idx = h->bucket_map[run->block_size];
	SLIST_INSERT_HEAD(&h->active_runs[bucket_idx], arun, run);
}

/*
 * heap_populate_buckets -- (internal) creates volatile state of memory blocks
 */
static int
heap_populate_buckets(PMEMobjpool *pop)
{
	struct pmalloc_heap *h = pop->heap;

	if (h->zones_exhausted == h->max_zone)
		return ENOMEM;

	uint32_t zone_id = h->zones_exhausted++;
	struct zone *z = &h->layout->zones[zone_id];

	/* ignore zone and chunk headers */
	VALGRIND_ADD_TO_GLOBAL_TX_IGNORE(z, sizeof (z->header) +
		sizeof (z->chunk_headers));

	if (z->header.magic != ZONE_HEADER_MAGIC)
		heap_zone_init(pop, zone_id);

	struct bucket *def_bucket = h->buckets[DEFAULT_BUCKET];

	struct chunk_run *run = NULL;
	struct memory_block m = {0, zone_id, 0, 0};
	for (uint32_t i = 0; i < z->header.size_idx; ) {
		struct chunk_header *hdr = &z->chunk_headers[i];
		ASSERT(hdr->size_idx != 0);
		heap_chunk_write_footer(pop, hdr, hdr->size_idx);

		switch (hdr->type) {
			case CHUNK_TYPE_RUN:
				run = (struct chunk_run *)&z->chunks[i];
				heap_register_active_run(h, run, i, zone_id);
				break;
			case CHUNK_TYPE_FREE:
				m.chunk_id = i;
				m.size_idx = hdr->size_idx;
				bucket_insert_block(pop, def_bucket, m);
				break;
			case CHUNK_TYPE_USED:
				break;
			default:
				ASSERT(0);
		}

		i += hdr->size_idx;
	}

	return 0;
}

/*
 * heap_get_active_run -- (internal) searches for an existing, unused, run
 */
static int
heap_get_active_run(struct pmalloc_heap *h, int bucket_idx,
	struct memory_block *m)
{
	if (pthread_mutex_lock(&h->active_run_lock) != 0) {
		ERR("failed to acquire active run lock");
		return 0;
	}

	int ret = 0;

	struct active_run *arun = SLIST_FIRST(&h->active_runs[bucket_idx]);
	if (arun == NULL)
		goto out;

	SLIST_REMOVE_HEAD(&h->active_runs[bucket_idx], run);

	m->chunk_id = arun->chunk_id;
	m->zone_id = arun->zone_id;

	ret = 1;

	Free(arun);

out:
	if (pthread_mutex_unlock(&h->active_run_lock) != 0)
		ERR("failed to release active run lock");

	return ret;
}

/*
 * heap_get_default_bucket --
 *	(internal) returns the bucket with CHUNKSIZE unit size
 */
static struct bucket *
heap_get_default_bucket(PMEMobjpool *pop)
{
	return pop->heap->buckets[DEFAULT_BUCKET];
}

/*
 * heap_ensure_bucket_filled -- (internal) refills the bucket if needed
 */
static int
heap_ensure_bucket_filled(PMEMobjpool *pop, struct bucket *b)
{
	if (!bucket_is_small(b)) {
		/* not much to do here apart from using the next zone */
		return heap_populate_buckets(pop);
	}

	struct pmalloc_heap *h = pop->heap;
	struct memory_block m = {0, 0, 1, 0};

	int bucket_idx = h->bucket_map[bucket_unit_size(b)];
	if (!heap_get_active_run(h, bucket_idx, &m)) {
		/* cannot reuse an existing run, create a new one */
		struct bucket *def_bucket = heap_get_default_bucket(pop);

		if (heap_get_bestfit_block(pop, def_bucket, &m) != 0)
			return ENOMEM; /* OOM */

		ASSERT(m.block_off == 0);

		heap_create_run(pop, b, m.chunk_id, m.zone_id);
	} else {
		heap_reuse_run(pop, b, m.chunk_id, m.zone_id);
	}

	return 0;
}

/*
 * heap_get_cache_bucket -- (internal) returns the bucket cache for given id
 */
static struct bucket *
heap_get_cache_bucket(struct pmalloc_heap *heap, int bucket_id)
{
	ASSERT(Lane_idx != UINT32_MAX);

	return heap->caches[Lane_idx % heap->ncaches].buckets[bucket_id];
}

/*
 * heap_get_best_bucket -- returns the bucket that best fits the requested size
 */
struct bucket *
heap_get_best_bucket(PMEMobjpool *pop, size_t size)
{
	if (size <= pop->heap->last_run_max_size) {
#ifdef USE_PER_LANE_BUCKETS
		return heap_get_cache_bucket(pop->heap,
			pop->heap->bucket_map[size]);
#else
		return pop->heap->buckets[pop->heap->bucket_map[size]];
#endif
	} else {
		return pop->heap->buckets[DEFAULT_BUCKET];
	}
}

/*
 * heap_get_run_bucket -- (internal) returns run bucket
 */
static struct bucket *
heap_get_run_bucket(struct chunk_run *run)
{
	struct bucket *b = (struct bucket *)run->bucket_vptr;
	ASSERTne(b, NULL);
	ASSERTne(bucket_unit_size(b), 0);
	ASSERTne(run->block_size, 0);
	ASSERTeq(run->block_size, bucket_unit_size(b));

	return b;
}

/*
 * heap_assign_run_bucket -- (internal) finds and sets bucket for a run
 */
static struct bucket *
heap_assign_run_bucket(PMEMobjpool *pop, struct chunk_run *run,
	uint32_t chunk_id, uint32_t zone_id)
{
	struct bucket *b = heap_get_best_bucket(pop, run->block_size);

	heap_reuse_run(pop, b, chunk_id, zone_id);

	/* different thread might have used this run, hence this get */
	return heap_get_run_bucket(run);
}

/*
 * heap_get_chunk_bucket -- returns the bucket that fits to chunk's unit size
 */
struct bucket *
heap_get_chunk_bucket(PMEMobjpool *pop, uint32_t chunk_id, uint32_t zone_id)
{
	ASSERT(zone_id < pop->heap->max_zone);
	struct zone *z = &pop->heap->layout->zones[zone_id];

	ASSERT(chunk_id < z->header.size_idx);
	struct chunk_header *hdr = &z->chunk_headers[chunk_id];

	if (hdr->type == CHUNK_TYPE_RUN) {
		struct chunk_run *run =
			(struct chunk_run *)&z->chunks[chunk_id];

		if (run->bucket_vptr != 0)
			return heap_get_run_bucket(run);
		else
			return heap_assign_run_bucket(pop, run,
				chunk_id, zone_id);
	} else {
		return pop->heap->buckets[DEFAULT_BUCKET];
	}
}

/*
 * heap_get_auxiliary_bucket -- returns bucket common for all threads
 */
struct bucket *
heap_get_auxiliary_bucket(PMEMobjpool *pop, size_t size)
{
	ASSERT(size <= pop->heap->last_run_max_size);

	return pop->heap->buckets[pop->heap->bucket_map[size]];
}

/*
 * heap_drain_to_auxiliary -- migrates memory blocks from cache buckets
 */
void
heap_drain_to_auxiliary(PMEMobjpool *pop, struct bucket *auxb,
	uint32_t size_idx)
{
	struct pmalloc_heap *h = pop->heap;

	unsigned total_drained = 0;
	unsigned drained_cache = 0;

	struct memory_block m;
	struct bucket *b;
	int b_id = h->bucket_map[bucket_unit_size(auxb)];

	/* max units drained from a single bucket cache */
	unsigned units_per_bucket = (unsigned)(bucket_bitmap_nallocs(auxb) *
				MAX_UNITS_PCT_DRAINED_CACHE);

	/* max units drained from all of the bucket caches */
	unsigned units_total = bucket_bitmap_nallocs(auxb) *
			MAX_UNITS_PCT_DRAINED_TOTAL;

	unsigned cache_id;
	int ret;

	for (unsigned i = 0;
			i < h->ncaches && total_drained < units_total; ++i) {
		cache_id = __sync_fetch_and_add(&h->last_drained[b_id], 1)
				% h->ncaches;

		b = h->caches[cache_id].buckets[b_id];

		/* don't drain from the deficient (requesting) cache */
		if (heap_get_cache_bucket(h, b_id) == b)
			continue;

		drained_cache = 0;

		if ((ret = bucket_lock(b)) != 0) {
			ERR("Failed to acquire bucket lock for migration");
			ASSERT(0);
		}

		/*
		 * XXX: Draining should make effort not to split runs
		 * between buckets because that will increase contention on
		 * the run locks and, what's worse, will make it difficult
		 * to degrade empty runs.
		 */
		while (drained_cache < units_per_bucket) {
			if (bucket_is_empty(b))
				break;

			/*
			 * Take only the memory blocks that can satisfy
			 * the memory requests.
			 */
			m = EMPTY_MEMORY_BLOCK;
			m.size_idx = size_idx;

			if ((ret = bucket_get_rm_block_bestfit(b, &m)) != 0)
				break;

			drained_cache += m.size_idx;
			bucket_insert_block(pop, auxb, m);
		}

		bucket_unlock(b);

		total_drained += drained_cache;
	}
}

/*
 * heap_buckets_init -- (internal) initializes bucket instances
 */
static int
heap_buckets_init(PMEMobjpool *pop)
{
	struct pmalloc_heap *h = pop->heap;
	size_t i;

	for (i = 0; i < MAX_BUCKETS - 1; ++i)
		SLIST_INIT(&h->active_runs[i]);

	bucket_proto[0].unit_max = RUN_UNIT_MAX;

	/*
	 * To take use of every single bit available in the run the unit size
	 * would have to be calculated using following expression:
	 * (RUNSIZE / (MAX_BITMAP_VALUES * BITS_PER_VALUE)), but to preserve
	 * cacheline alignment a little bit of memory at the end of the run
	 * is left unused.
	 */
	bucket_proto[0].unit_size = MIN_RUN_SIZE;

	for (i = 1; i < MAX_BUCKETS - 1; ++i) {
		bucket_proto[i].unit_max = RUN_UNIT_MAX;
		bucket_proto[i].unit_size =
				bucket_proto[i - 1].unit_size *
				bucket_proto[i - 1].unit_max;
	}

	bucket_proto[i].unit_max = UINT32_MAX;
	bucket_proto[i].unit_size = CHUNKSIZE;

	h->last_run_max_size = bucket_proto[i - 1].unit_size *
				bucket_proto[i - 1].unit_max;

	h->bucket_map = Malloc(sizeof (uint8_t) * (h->last_run_max_size + 1));
	if (h->bucket_map == NULL)
		goto error_bucket_map_malloc;

	for (i = 0; i < MAX_BUCKETS; ++i) {
		h->buckets[i] = bucket_new(bucket_proto[i].unit_size,
					bucket_proto[i].unit_max);
		if (h->buckets[i] == NULL)
			goto error_bucket_new;
	}

	/* XXX better way to fill the bucket map */
	for (i = 0; i <= h->last_run_max_size; ++i) {
		/*
		 * All sizes can be handled by the largest available run bucket,
		 * next pick the smallest fit.
		 */
		h->bucket_map[i] = MAX_BUCKETS - 2;
		for (unsigned char j = 0; j < MAX_BUCKETS - 2; ++j) {
			/*
			 * Skip the last unit, so that the distribution
			 * of buckets in the map is better.
			 */
			if ((bucket_proto[j].unit_size *
				((bucket_proto[j].unit_max - 1))) >= i) {
				h->bucket_map[i] = j;
				break;
			}
		}
	}

	heap_populate_buckets(pop);

	return 0;

error_bucket_new:
	Free(h->bucket_map);

	for (; i >= 1; --i)
		bucket_delete(h->buckets[i - 1]);
error_bucket_map_malloc:

	return ENOMEM;
}

/*
 * heap_resize_chunk -- (internal) splits the chunk into two smaller ones
 */
static void
heap_resize_chunk(PMEMobjpool *pop,
	uint32_t chunk_id, uint32_t zone_id, uint32_t new_size_idx)
{
	uint32_t new_chunk_id = chunk_id + new_size_idx;

	struct zone *z = &pop->heap->layout->zones[zone_id];
	struct chunk_header *old_hdr = &z->chunk_headers[chunk_id];
	struct chunk_header *new_hdr = &z->chunk_headers[new_chunk_id];

	uint32_t rem_size_idx = old_hdr->size_idx - new_size_idx;
	heap_chunk_init(pop, new_hdr, CHUNK_TYPE_FREE, rem_size_idx);
	heap_chunk_init(pop, old_hdr, CHUNK_TYPE_FREE, new_size_idx);

	struct bucket *def_bucket = pop->heap->buckets[DEFAULT_BUCKET];
	struct memory_block m = {new_chunk_id, zone_id, rem_size_idx, 0};
	bucket_insert_block(pop, def_bucket, m);
}

/*
 * heap_recycle_block -- (internal) recycles unused part of the memory block
 */
static void
heap_recycle_block(PMEMobjpool *pop, struct bucket *b, struct memory_block *m,
	uint32_t units)
{
	if (bucket_is_small(b)) {
		ASSERT(units <= UINT16_MAX);
		ASSERT(m->block_off + units <= UINT16_MAX);
		struct memory_block r = {m->chunk_id, m->zone_id,
			m->size_idx - units, (uint16_t)(m->block_off + units)};
		bucket_insert_block(pop, b, r);
	} else {
		heap_resize_chunk(pop, m->chunk_id, m->zone_id, units);
	}

	m->size_idx = units;
}

/*
 * heap_get_bestfit_block --
 *	extracts a memory block of equal size index
 */
int
heap_get_bestfit_block(PMEMobjpool *pop, struct bucket *b,
	struct memory_block *m)
{
	if (bucket_lock(b) != 0)
		return EAGAIN;

	uint32_t units = m->size_idx;

	while (bucket_get_rm_block_bestfit(b, m) != 0) {
		if (heap_ensure_bucket_filled(pop, b) == ENOMEM) {
			bucket_unlock(b);
			return ENOMEM;
		}
	}

	ASSERT(m->size_idx >= units);

	if (units != m->size_idx)
		heap_recycle_block(pop, b, m, units);

	bucket_unlock(b);

	return 0;
}

/*
 * heap_get_exact_block --
 *	extracts exactly this memory block and cuts it accordingly
 */
int
heap_get_exact_block(PMEMobjpool *pop, struct bucket *b,
	struct memory_block *m, uint32_t units)
{
	if (bucket_lock(b) != 0)
		return EAGAIN;

	if (bucket_get_rm_block_exact(b, *m) != 0) {
		bucket_unlock(b);
		return ENOMEM;
	}

	if (units != m->size_idx)
		heap_recycle_block(pop, b, m, units);

	bucket_unlock(b);

	return 0;
}

/*
 * chunk_get_chunk_hdr_value -- (internal) get value of a header for redo log
 */
static uint64_t
chunk_get_chunk_hdr_value(struct chunk_header hdr, uint16_t type,
	uint32_t size_idx)
{
	uint64_t val;
	ASSERT(sizeof (struct chunk_header) == sizeof (uint64_t));

	hdr.type = type;
	hdr.size_idx = size_idx;
	memcpy(&val, &hdr, sizeof (val));

	return val;
}

/*
 * heap_get_block_header -- returns the header of the memory block
 */
void *
heap_get_block_header(PMEMobjpool *pop, struct memory_block m,
	enum heap_op op, uint64_t *op_result)
{
	struct zone *z = &pop->heap->layout->zones[m.zone_id];
	struct chunk_header *hdr = &z->chunk_headers[m.chunk_id];

	if (hdr->type != CHUNK_TYPE_RUN) {
		*op_result = chunk_get_chunk_hdr_value(*hdr,
			op == HEAP_OP_ALLOC ? CHUNK_TYPE_USED : CHUNK_TYPE_FREE,
			m.size_idx);

		VALGRIND_DO_MAKE_MEM_NOACCESS(pop, hdr + 1,
				(hdr->size_idx - 1) *
				sizeof (struct chunk_header));

		heap_chunk_write_footer(pop, hdr, m.size_idx);

		return hdr;
	}

	struct chunk_run *r = (struct chunk_run *)&z->chunks[m.chunk_id];
	uint64_t bmask = ((1ULL << m.size_idx) - 1ULL) <<
			(m.block_off % BITS_PER_VALUE);

	int bpos = m.block_off / BITS_PER_VALUE;
	if (op == HEAP_OP_FREE)
		*op_result = r->bitmap[bpos] & ~bmask;
	else
		*op_result = r->bitmap[bpos] | bmask;

	return &r->bitmap[bpos];
}

/*
 * heap_get_block_data -- returns pointer to the data of a block
 */
void *
heap_get_block_data(PMEMobjpool *pop, struct memory_block m)
{
	struct zone *z = &pop->heap->layout->zones[m.zone_id];
	struct chunk_header *hdr = &z->chunk_headers[m.chunk_id];

	void *data = &z->chunks[m.chunk_id].data;
	if (hdr->type != CHUNK_TYPE_RUN)
		return data;

	struct chunk_run *run = data;
	ASSERT(run->block_size != 0);

	return (char *)&run->data + (run->block_size * m.block_off);
}

#ifdef DEBUG
/*
 * heap_block_is_allocated -- checks whether the memory block is allocated
 */
int
heap_block_is_allocated(PMEMobjpool *pop, struct memory_block m)
{
	struct zone *z = &pop->heap->layout->zones[m.zone_id];
	struct chunk_header *hdr = &z->chunk_headers[m.chunk_id];

	if (hdr->type == CHUNK_TYPE_USED)
		return 1;

	if (hdr->type == CHUNK_TYPE_FREE)
		return 0;

	ASSERTeq(hdr->type, CHUNK_TYPE_RUN);

	struct chunk_run *r = (struct chunk_run *)&z->chunks[m.chunk_id];

	unsigned v = m.block_off / BITS_PER_VALUE;
	uint64_t bitmap = r->bitmap[v];
	unsigned b = m.block_off % BITS_PER_VALUE;

	unsigned b_last = b + m.size_idx;
	ASSERT(b_last <= BITS_PER_VALUE);

	for (unsigned i = b; i < b_last; ++i)
		if (!BIT_IS_CLR(bitmap, i))
			return 1;

	return 0;
}
#endif /* DEBUG */

/*
 * heap_run_get_block -- (internal) returns next/prev memory block from run
 */
static int
heap_run_get_block(PMEMobjpool *pop, struct chunk_run *r,
	struct memory_block *mblock, uint32_t size_idx, uint16_t block_off,
	int prev)
{
	unsigned v = block_off / BITS_PER_VALUE;
	unsigned b = block_off % BITS_PER_VALUE;

	if (prev) {
		unsigned i;
		for (i = b;
			i % RUN_UNIT_MAX && BIT_IS_CLR(r->bitmap[v], i - 1);
			--i)
			;

		mblock->block_off = (uint16_t)(v * BITS_PER_VALUE + i);
		ASSERT(block_off >= mblock->block_off);
		mblock->size_idx = (uint16_t)(block_off - mblock->block_off);
	} else { /* next */
		unsigned i;
		for (i = b + size_idx;
			i % RUN_UNIT_MAX && BIT_IS_CLR(r->bitmap[v], i);
			++i)
			;

		ASSERT((uint64_t)block_off + size_idx <= UINT16_MAX);
		mblock->block_off = (uint16_t)(block_off + size_idx);
		mblock->size_idx = i - (b + size_idx);
	}

	if (mblock->size_idx == 0)
		return ENOENT;

	return 0;
}

/*
 * heap_get_chunk -- (internal) returns next/prev chunk from zone
 */
static int
heap_get_chunk(PMEMobjpool *pop, struct zone *z, struct chunk_header *hdr,
	struct memory_block *m, uint32_t chunk_id, int prev)
{
	if (prev) {
		if (chunk_id == 0)
			return ENOENT;

		struct chunk_header *prev_hdr = &z->chunk_headers[chunk_id - 1];
		m->chunk_id = chunk_id - prev_hdr->size_idx;

		if (z->chunk_headers[m->chunk_id].type != CHUNK_TYPE_FREE)
			return ENOENT;

		m->size_idx = z->chunk_headers[m->chunk_id].size_idx;
	} else { /* next */
		if (chunk_id + hdr->size_idx == z->header.size_idx)
			return ENOENT;

		m->chunk_id = chunk_id + hdr->size_idx;

		if (z->chunk_headers[m->chunk_id].type != CHUNK_TYPE_FREE)
			return ENOENT;

		m->size_idx = z->chunk_headers[m->chunk_id].size_idx;
	}

	return 0;
}

/*
 * heap_get_adjacent_free_block -- locates adjacent free memory block in heap
 */
int heap_get_adjacent_free_block(PMEMobjpool *pop, struct memory_block *m,
	struct memory_block cnt, int prev)
{
	struct zone *z = &pop->heap->layout->zones[cnt.zone_id];
	struct chunk_header *hdr = &z->chunk_headers[cnt.chunk_id];
	m->zone_id = cnt.zone_id;

	if (hdr->type == CHUNK_TYPE_RUN) {
		m->chunk_id = cnt.chunk_id;
		struct chunk_run *r =
				(struct chunk_run *)&z->chunks[cnt.chunk_id];
		return heap_run_get_block(pop, r, m, cnt.size_idx,
				cnt.block_off, prev);
	} else {
		return heap_get_chunk(pop, z, hdr, m, cnt.chunk_id, prev);
	}
}

/*
 * heap_lock_if_run -- acquire a run lock
 */
int
heap_lock_if_run(PMEMobjpool *pop, struct memory_block m)
{
	struct chunk_header *hdr =
		&pop->heap->layout->zones[m.zone_id].chunk_headers[m.chunk_id];

	return hdr->type == CHUNK_TYPE_RUN ?
		pthread_mutex_lock(heap_get_run_lock(pop, m.chunk_id)) : 0;
}

/*
 * heap_unlock_if_run -- release a run lock
 */
int
heap_unlock_if_run(PMEMobjpool *pop, struct memory_block m)
{
	struct chunk_header *hdr =
		&pop->heap->layout->zones[m.zone_id].chunk_headers[m.chunk_id];

	return hdr->type == CHUNK_TYPE_RUN ?
		pthread_mutex_unlock(heap_get_run_lock(pop, m.chunk_id)) : 0;
}

/*
 * heap_coalesce -- merges adjacent memory blocks
 */
struct memory_block
heap_coalesce(PMEMobjpool *pop,
	struct memory_block *blocks[], int n, enum heap_op op,
	void **hdr, uint64_t *op_result)
{
	struct memory_block ret;
	struct memory_block *b = NULL;
	ret.size_idx = 0;
	for (int i = 0; i < n; ++i) {
		if (blocks[i] == NULL)
			continue;
		b = b ? : blocks[i];
		ret.size_idx += blocks[i] ? blocks[i]->size_idx : 0;
	}

	ASSERTne(b, NULL);

	ret.chunk_id = b->chunk_id;
	ret.zone_id = b->zone_id;
	ret.block_off = b->block_off;

	*hdr = heap_get_block_header(pop, ret, op, op_result);

	return ret;
}

/*
 * heap_free_block -- creates free persistent state of a memory block
 */
struct memory_block
heap_free_block(PMEMobjpool *pop, struct bucket *b,
	struct memory_block m, void *hdr, uint64_t *op_result)
{
	struct memory_block *blocks[3] = {NULL, &m, NULL};

	struct memory_block prev = {0, 0, 0, 0};
	if (heap_get_adjacent_free_block(pop, &prev, m, 1) == 0 &&
		bucket_get_rm_block_exact(b, prev) == 0) {
		blocks[0] = &prev;
	}

	struct memory_block next = {0, 0, 0, 0};
	if (heap_get_adjacent_free_block(pop, &next, m, 0) == 0 &&
		bucket_get_rm_block_exact(b, next) == 0) {
		blocks[2] = &next;
	}

	struct memory_block res = heap_coalesce(pop, blocks, 3, HEAP_OP_FREE,
		hdr, op_result);

	return res;
}

/*
 * traverse_bucket_run -- (internal) traverses each memory block of a run
 */
static int
traverse_bucket_run(struct bucket *b, struct memory_block m,
	int (*cb)(struct bucket *b, struct memory_block m))
{
	m.block_off = 0;
	m.size_idx = RUN_UNIT_MAX;
	uint32_t size_idx_sum = 0;

	while (size_idx_sum != bucket_bitmap_nallocs(b)) {
		if (cb(b, m) != 0)
			return 1;

		size_idx_sum += m.size_idx;

		ASSERT((uint32_t)m.block_off + RUN_UNIT_MAX <= UINT16_MAX);
		m.block_off = (uint16_t)(m.block_off + RUN_UNIT_MAX);
		if (m.block_off + RUN_UNIT_MAX > bucket_bitmap_nallocs(b))
			m.size_idx = bucket_bitmap_nallocs(b) - m.block_off;
		else
			m.size_idx = RUN_UNIT_MAX;
	}

	return 0;
}

/*
 * heap_degrade_run_if_empty -- makes a chunk out of an empty run
 */
int
heap_degrade_run_if_empty(PMEMobjpool *pop, struct bucket *b,
	struct memory_block m)
{
	struct zone *z = &pop->heap->layout->zones[m.zone_id];
	struct chunk_header *hdr = &z->chunk_headers[m.chunk_id];
	ASSERT(hdr->type == CHUNK_TYPE_RUN);

	struct chunk_run *run = (struct chunk_run *)&z->chunks[m.chunk_id];

	int err = 0;
	if ((err = bucket_lock(b)) != 0)
		return err;

	if ((err = pthread_mutex_lock(heap_get_run_lock(pop, m.chunk_id))) != 0)
		goto out_bucket;

	unsigned i;
	unsigned nval = bucket_bitmap_nval(b);
	for (i = 0; nval > 0 && i < nval - 1; ++i)
		if (run->bitmap[i] != 0)
			goto out;

	if (run->bitmap[i] != bucket_bitmap_lastval(b))
		goto out;

	if (traverse_bucket_run(b, m, bucket_get_block_exact) != 0) {
		/*
		 * The memory block is in the active run list or in a
		 * different bucket, there's not much we can do here
		 * right now. It will get freed later anyway.
		 */
		goto out;
	}

	if (traverse_bucket_run(b, m, bucket_get_rm_block_exact) != 0) {
		ERR("Persistent/volatile state mismatch");
		ASSERT(0);
	}

	struct bucket *defb = heap_get_default_bucket(pop);
	if ((err = bucket_lock(defb)) != 0) {
		ERR("Failed to lock default bucket");
		ASSERT(0);
	}

	m.block_off = 0;
	m.size_idx = 1;
	heap_chunk_init(pop, hdr, CHUNK_TYPE_FREE, m.size_idx);

	uint64_t *mhdr;
	uint64_t op_result;
	struct memory_block fm =
			heap_free_block(pop, defb, m, &mhdr, &op_result);

	VALGRIND_ADD_TO_TX(mhdr, sizeof (*mhdr));
	*mhdr = op_result;
	VALGRIND_REMOVE_FROM_TX(mhdr, sizeof (*mhdr));
	pop->persist(pop, mhdr, sizeof (*mhdr));

	bucket_insert_block(pop, defb, fm);

	bucket_unlock(defb);

out:
	if (pthread_mutex_unlock(heap_get_run_lock(pop, m.chunk_id)) != 0) {
		ERR("Failed to release run lock");
		ASSERT(0);
	}

out_bucket:
	bucket_unlock(b);

	return err;
}

#ifdef USE_VG_MEMCHECK
/*
 * heap_vg_boot -- performs Valgrind-related heap initialization
 */
static void
heap_vg_boot(PMEMobjpool *pop)
{
	if (!On_valgrind)
		return;
	LOG(4, "pop %p", pop);

	/* mark unused part of the last zone as not accessible */
	struct pmalloc_heap *h = pop->heap;
	ASSERT(h->max_zone > 0);
	struct zone *last_zone = &h->layout->zones[h->max_zone - 1];
	void *unused = &last_zone->chunks[last_zone->header.size_idx];
	VALGRIND_DO_MAKE_MEM_NOACCESS(pop, unused,
			(void *)pop + pop->size - unused);
}
#endif

/*
 * heap_get_ncpus -- (internal) returns the number of available CPUs
 */
static unsigned
heap_get_ncpus(void)
{
	long cpus = sysconf(_SC_NPROCESSORS_ONLN);
	if (cpus < 1)
		cpus = 1;
	return (unsigned)cpus;
}

/*
 * heap_get_ncaches -- (internal) returns the number of bucket caches according
 * to number of cpus and number of caches per cpu.
 */
static unsigned
heap_get_ncaches(void)
{
	return NCACHES_PER_CPU * heap_get_ncpus();
}

/*
 * heap_boot -- opens the heap region of the pmemobj pool
 *
 * If successful function returns zero. Otherwise an error number is returned.
 */
int
heap_boot(PMEMobjpool *pop)
{
	struct pmalloc_heap *h = Malloc(sizeof (*h));
	int err;
	if (h == NULL) {
		err = ENOMEM;
		goto error_heap_malloc;
	}

	h->ncaches = heap_get_ncaches();
	h->caches = Malloc(sizeof (struct bucket_cache) * h->ncaches);
	if (h->caches == NULL) {
		err = ENOMEM;
		goto error_heap_cache_malloc;
	}

	h->max_zone = heap_max_zone(pop->heap_size);
	h->zones_exhausted = 0;
	h->layout = heap_get_layout(pop);

	if ((err = pthread_mutex_init(&h->active_run_lock, NULL)) != 0)
		goto error_lock_init;

	for (int i = 0; i < MAX_RUN_LOCKS; ++i)
		if ((err = pthread_mutex_init(&h->run_locks[i], NULL)) != 0)
			goto error_lock_init;

	memset(h->last_drained, 0, sizeof (h->last_drained));

	pop->heap = h;

	if ((err = heap_buckets_init(pop)) != 0)
		goto error_buckets_init;

	unsigned i;
	for (i = 0; i < h->ncaches; ++i)
		if (bucket_cache_init(pop, &h->caches[i]) != 0)
			goto error_cache_init;

#ifdef USE_VG_MEMCHECK
	heap_vg_boot(pop);
#endif

	return 0;

error_cache_init:
	for (; i >= 1; --i)
		bucket_cache_destroy(&h->caches[i - 1]);
error_buckets_init:
	/* there's really no point in destroying the locks */
error_lock_init:
	Free(h->caches);
error_heap_cache_malloc:
	Free(h);
	pop->heap = NULL;
error_heap_malloc:
	return err;
}

/*
 * heap_write_header -- (internal) creates a clean header
 */
static void
heap_write_header(struct heap_header *hdr, size_t size)
{
	struct heap_header newhdr = {
		.signature = HEAP_SIGNATURE,
		.major = HEAP_MAJOR,
		.minor = HEAP_MINOR,
		.size = size,
		.chunksize = CHUNKSIZE,
		.chunks_per_zone = MAX_CHUNK,
		.reserved = {0},
		.checksum = 0
	};

	util_checksum(&newhdr, sizeof (newhdr), &newhdr.checksum, 1);
	*hdr = newhdr;
}

#ifdef USE_VG_MEMCHECK
/*
 * heap_vg_open -- notifies Valgrind about heap layout
 */
void
heap_vg_open(PMEMobjpool *pop)
{
	VALGRIND_DO_MAKE_MEM_UNDEFINED(pop, ((void *)pop) + pop->heap_offset,
			pop->heap_size);

	struct heap_layout *layout = heap_get_layout(pop);

	VALGRIND_DO_MAKE_MEM_DEFINED(pop, &layout->header,
			sizeof (layout->header));

	unsigned zones = heap_max_zone(pop->heap_size);

	for (unsigned i = 0; i < zones; ++i) {
		struct zone *z = &layout->zones[i];
		uint32_t chunks;

		VALGRIND_DO_MAKE_MEM_DEFINED(pop, &z->header,
				sizeof (z->header));

		if (z->header.magic != ZONE_HEADER_MAGIC)
			continue;

		chunks = z->header.size_idx;

		for (uint32_t c = 0; c < chunks; ) {
			struct chunk_header *hdr = &z->chunk_headers[c];

			VALGRIND_DO_MAKE_MEM_DEFINED(pop, hdr, sizeof (*hdr));

			if (hdr->type == CHUNK_TYPE_RUN) {
				struct chunk_run *run =
					(struct chunk_run *)&z->chunks[c];

				VALGRIND_DO_MAKE_MEM_DEFINED(pop, run,
					sizeof (*run));
			}

			ASSERT(hdr->size_idx > 0);

			/* mark unused chunk headers as not accessible */
			VALGRIND_DO_MAKE_MEM_NOACCESS(pop,
					&z->chunk_headers[c + 1],
					(hdr->size_idx - 1) *
					sizeof (struct chunk_header));

			c += hdr->size_idx;
		}

		/* mark all unused chunk headers after last as not accessible */
		VALGRIND_DO_MAKE_MEM_NOACCESS(pop, &z->chunk_headers[chunks],
			(MAX_CHUNK - chunks) * sizeof (struct chunk_header));
	}
}
#endif

/*
 * heap_init -- initializes the heap
 *
 * If successful function returns zero. Otherwise an error number is returned.
 */
int
heap_init(PMEMobjpool *pop)
{
	if (pop->heap_size < HEAP_MIN_SIZE)
		return EINVAL;

	VALGRIND_DO_MAKE_MEM_UNDEFINED(pop, (char *)pop + pop->heap_offset,
			pop->heap_size);

	struct heap_layout *layout = heap_get_layout(pop);
	heap_write_header(&layout->header, pop->heap_size);
	pmem_msync(&layout->header, sizeof (struct heap_header));

	unsigned zones = heap_max_zone(pop->heap_size);
	for (unsigned i = 0; i < zones; ++i) {
		memset(&layout->zones[i].header, 0,
				sizeof (layout->zones[i].header));

		memset(&layout->zones[i].chunk_headers, 0,
				sizeof (layout->zones[i].chunk_headers));

		pmem_msync(&layout->zones[i].header,
			sizeof (layout->zones[i].header));
		pmem_msync(&layout->zones[i].chunk_headers,
			sizeof (layout->zones[i].chunk_headers));

		/* only explicitly allocated chunks should be accessible */
		VALGRIND_DO_MAKE_MEM_NOACCESS(pop,
			&layout->zones[i].chunk_headers,
			sizeof (layout->zones[i].chunk_headers));
	}

	return 0;
}

/*
 * heap_boot -- cleanups the volatile heap state
 *
 * If successful function returns zero. Otherwise an error number is returned.
 */
int
heap_cleanup(PMEMobjpool *pop)
{
	for (int i = 0; i < MAX_BUCKETS; ++i)
		bucket_delete(pop->heap->buckets[i]);

	for (int i = 0; i < MAX_RUN_LOCKS; ++i)
		pthread_mutex_destroy(&pop->heap->run_locks[i]);

	Free(pop->heap->bucket_map);

	for (unsigned i = 0; i < pop->heap->ncaches; ++i)
		bucket_cache_destroy(&pop->heap->caches[i]);

	Free(pop->heap->caches);

	pthread_mutex_destroy(&pop->heap->active_run_lock);

	struct active_run *r;
	for (int i = 0; i < MAX_BUCKETS - 1; ++i) {
		while ((r = SLIST_FIRST(&pop->heap->active_runs[i])) != NULL) {
			SLIST_REMOVE_HEAD(&pop->heap->active_runs[i], run);
			Free(r);
		}
	}

	Free(pop->heap);

	pop->heap = NULL;

	return 0;
}

/*
 * heap_verify_header -- (internal) verifies if the heap header is consistent
 */
static int
heap_verify_header(struct heap_header *hdr)
{
	if (util_checksum(hdr, sizeof (*hdr), &hdr->checksum, 0) != 1) {
		ERR("heap: invalid header's checksum");
		return -1;
	}

	if (memcmp(hdr->signature, HEAP_SIGNATURE, HEAP_SIGNATURE_LEN) != 0) {
		ERR("heap: invalid signature");
		return -1;
	}

	return 0;
}

/*
 * heap_verify_zone_header --
 *	(internal) verifies if the zone header is consistent
 */
static int
heap_verify_zone_header(struct zone_header *hdr)
{
	if (hdr->size_idx == 0) {
		ERR("heap: invalid zone size");
		return -1;
	}

	return 0;
}

/*
 * heap_verify_chunk_header --
 *	(internal) verifies if the chunk header is consistent
 */
static int
heap_verify_chunk_header(struct chunk_header *hdr)
{
	if (hdr->type == CHUNK_TYPE_UNKNOWN) {
		ERR("heap: invalid chunk type");
		return -1;
	}

	if (hdr->type >= MAX_CHUNK_TYPE) {
		ERR("heap: unknown chunk type");
		return -1;
	}

	if (hdr->flags & CHUNK_FLAG_ZEROED) {
		ERR("heap: invalid chunk flags");
		return -1;
	}

	return 0;
}

/*
 * heap_verify_zone -- (internal) verifies if the zone is consistent
 */
static int
heap_verify_zone(struct zone *zone)
{
	if (zone->header.magic == 0)
		return 0; /* not initialized, and that is OK */

	if (zone->header.magic != ZONE_HEADER_MAGIC) {
		ERR("heap: invalid zone magic");
		return -1;
	}

	if (heap_verify_zone_header(&zone->header))
		return -1;

	uint32_t i;
	for (i = 0; i < zone->header.size_idx; ) {
		if (heap_verify_chunk_header(&zone->chunk_headers[i]))
			return -1;

		i += zone->chunk_headers[i].size_idx;
	}

	if (i != zone->header.size_idx) {
		ERR("heap: chunk sizes mismatch");
		return -1;
	}

	return 0;
}

/*
 * heap_check -- verifies if the heap is consistent and can be opened properly
 *
 * If successful function returns zero. Otherwise an error number is returned.
 */
int
heap_check(PMEMobjpool *pop)
{
	if (pop->heap_size < HEAP_MIN_SIZE) {
		ERR("heap: invalid heap size");
		return -1;
	}

	struct heap_layout *layout = heap_get_layout(pop);

	if (pop->heap_size != layout->header.size) {
		ERR("heap: heap size missmatch");
		return -1;
	}

	if (heap_verify_header(&layout->header))
		return -1;

	for (unsigned i = 0; i < heap_max_zone(layout->header.size); ++i) {
		if (heap_verify_zone(&layout->zones[i]))
			return -1;
	}

	return 0;
}
