#define	JEMALLOC_CHUNK_C_
#include "jemalloc/internal/jemalloc_internal.h"

/******************************************************************************/
/* Data. */

const char	*opt_dss = DSS_DEFAULT;
size_t		opt_lg_chunk = LG_CHUNK_DEFAULT;


/* Various chunk-related settings. */
size_t		chunksize;
size_t		chunksize_mask; /* (chunksize - 1). */
size_t		chunk_npages;
size_t		map_bias;
size_t		arena_maxclass; /* Max size class for arenas. */

/******************************************************************************/
/*
 * Function prototypes for static functions that are referenced prior to
 * definition.
 */

static void chunk_dalloc_core(pool_t *pool, void *chunk, size_t size);

/******************************************************************************/

static void *
chunk_recycle(pool_t *pool, extent_tree_t *chunks_szad, extent_tree_t *chunks_ad,
    void *new_addr, size_t size, size_t alignment, bool base, bool *zero)
{
	void *ret;
	extent_node_t *node;
	extent_node_t key;
	size_t alloc_size, leadsize, trailsize;
	bool zeroed;

	if (base) {
		/*
		 * This function may need to call base_node_{,de}alloc(), but
		 * the current chunk allocation request is on behalf of the
		 * base allocator.  Avoid deadlock (and if that weren't an
		 * issue, potential for infinite recursion) by returning NULL.
		 */
		return (NULL);
	}

	alloc_size = size + alignment - chunksize;
	/* Beware size_t wrap-around. */
	if (alloc_size < size)
		return (NULL);
	key.addr = new_addr;
	key.size = alloc_size;
	malloc_mutex_lock(&pool->chunks_mtx);
	node = extent_tree_szad_nsearch(chunks_szad, &key);
	if (node == NULL || (new_addr && node->addr != new_addr)) {
		malloc_mutex_unlock(&pool->chunks_mtx);
		return (NULL);
	}
	leadsize = ALIGNMENT_CEILING((uintptr_t)node->addr, alignment) -
	    (uintptr_t)node->addr;
	assert(node->size >= leadsize + size);
	trailsize = node->size - leadsize - size;
	ret = (void *)((uintptr_t)node->addr + leadsize);
	zeroed = node->zeroed;
	if (zeroed)
	    *zero = true;
	/* Remove node from the tree. */
	extent_tree_szad_remove(chunks_szad, node);
	extent_tree_ad_remove(chunks_ad, node);
	if (leadsize != 0) {
		/* Insert the leading space as a smaller chunk. */
		node->size = leadsize;
		extent_tree_szad_insert(chunks_szad, node);
		extent_tree_ad_insert(chunks_ad, node);
		node = NULL;
	}
	if (trailsize != 0) {
		/* Insert the trailing space as a smaller chunk. */
		if (node == NULL) {
			/*
			 * An additional node is required, but
			 * base_node_alloc() can cause a new base chunk to be
			 * allocated.  Drop chunks_mtx in order to avoid
			 * deadlock, and if node allocation fails, deallocate
			 * the result before returning an error.
			 */
			malloc_mutex_unlock(&pool->chunks_mtx);
			node = base_node_alloc(pool);
			if (node == NULL) {
				chunk_dalloc_core(pool, ret, size);
				return (NULL);
			}
			malloc_mutex_lock(&pool->chunks_mtx);
		}
		node->addr = (void *)((uintptr_t)(ret) + size);
		node->size = trailsize;
		node->zeroed = zeroed;
		extent_tree_szad_insert(chunks_szad, node);
		extent_tree_ad_insert(chunks_ad, node);
		node = NULL;
	}
	malloc_mutex_unlock(&pool->chunks_mtx);

	if (node != NULL)
		base_node_dalloc(pool, node);
	if (*zero) {
		if (zeroed == false)
			memset(ret, 0, size);
		else if (config_debug) {
			size_t i;
			size_t *p = (size_t *)(uintptr_t)ret;

			JEMALLOC_VALGRIND_MAKE_MEM_DEFINED(ret, size);
			for (i = 0; i < size / sizeof(size_t); i++)
				assert(p[i] == 0);
		}
	}
	return (ret);
}

/*
 * If the caller specifies (*zero == false), it is still possible to receive
 * zeroed memory, in which case *zero is toggled to true.  arena_chunk_alloc()
 * takes advantage of this to avoid demanding zeroed chunks, but taking
 * advantage of them if they are returned.
 */
static void *
chunk_alloc_core(pool_t *pool, void *new_addr, size_t size, size_t alignment,
    bool base, bool *zero, dss_prec_t dss_prec)
{
	void *ret;

	assert(size != 0);
	assert((size & chunksize_mask) == 0);
	assert(alignment != 0);
	assert((alignment & chunksize_mask) == 0);

	/* "primary" dss. */
	if (have_dss && dss_prec == dss_prec_primary) {
		if ((ret = chunk_recycle(pool, &pool->chunks_szad_dss, &pool->chunks_ad_dss,
		    new_addr, size, alignment, base, zero)) != NULL)
			return (ret);
		/* requesting an address only implemented for recycle */
		if (new_addr == NULL
		    && (ret = chunk_alloc_dss(size, alignment, zero)) != NULL)
			return (ret);
	}
	/* mmap. */
	if ((ret = chunk_recycle(pool, &pool->chunks_szad_mmap, &pool->chunks_ad_mmap,
	    new_addr, size, alignment, base, zero)) != NULL)
		return (ret);
	/* requesting an address only implemented for recycle */
	if (new_addr == NULL &&
	    (ret = chunk_alloc_mmap(size, alignment, zero)) != NULL)
		return (ret);
	/* "secondary" dss. */
	if (have_dss && dss_prec == dss_prec_secondary) {
		if ((ret = chunk_recycle(pool, &pool->chunks_szad_dss, &pool->chunks_ad_dss,
		    new_addr, size, alignment, base, zero)) != NULL)
			return (ret);
		/* requesting an address only implemented for recycle */
		if (new_addr == NULL &&
		    (ret = chunk_alloc_dss(size, alignment, zero)) != NULL)
			return (ret);
	}

	/* All strategies for allocation failed. */
	return (NULL);
}

static bool
chunk_register(pool_t *pool, void *chunk, size_t size, bool base)
{
	assert(chunk != NULL);
	assert(CHUNK_ADDR2BASE(chunk) == chunk);

	if (config_ivsalloc && base == false) {
		if (rtree_set(pool->chunks_rtree, (uintptr_t)chunk, 1))
			return (true);
	}
	if (config_stats || config_prof) {
		bool gdump;
		malloc_mutex_lock(&pool->chunks_mtx);
		if (config_stats)
			pool->stats_chunks.nchunks += (size / chunksize);
		pool->stats_chunks.curchunks += (size / chunksize);
		if (pool->stats_chunks.curchunks > pool->stats_chunks.highchunks) {
			pool->stats_chunks.highchunks =
			    pool->stats_chunks.curchunks;
			if (config_prof)
				gdump = true;
		} else if (config_prof)
			gdump = false;
		malloc_mutex_unlock(&pool->chunks_mtx);
		if (config_prof && opt_prof && opt_prof_gdump && gdump)
			prof_gdump();
	}
	if (config_valgrind)
		JEMALLOC_VALGRIND_MAKE_MEM_UNDEFINED(chunk, size);
	return (false);
}

void *
chunk_alloc_base(pool_t *pool, size_t size)
{
	void *ret;
	bool zero;

	zero = false;

	if (pool->pool_id != 0) {
		/* Custom pools can only use existing chunks. */
		ret = chunk_recycle(pool, &pool->chunks_szad_mmap,
				    &pool->chunks_ad_mmap, NULL, size,
				    chunksize, false, &zero);
	} else {
		ret = chunk_alloc_core(pool, NULL, size, chunksize, true, &zero,
				       chunk_dss_prec_get());
	}
	if (ret == NULL)
		return (NULL);
	if (chunk_register(pool, ret, size, true)) {
		chunk_dalloc_core(pool, ret, size);
		return (NULL);
	}
	return (ret);
}

void *
chunk_alloc_arena(chunk_alloc_t *chunk_alloc, chunk_dalloc_t *chunk_dalloc,
	arena_t *arena, void *new_addr, size_t size, size_t alignment, bool *zero)
{
	void *ret;

	ret = chunk_alloc(new_addr, size, alignment, zero,
		arena->ind, arena->pool);
	if (ret != NULL && chunk_register(arena->pool, ret, size, false)) {
		chunk_dalloc(ret, size, arena->ind, arena->pool);
		ret = NULL;
	}

	return (ret);
}

/* Default arena chunk allocation routine in the absence of user override. */
void *
chunk_alloc_default(void *new_addr, size_t size, size_t alignment, bool *zero,
    unsigned arena_ind, pool_t *pool)
{
	if (pool->pool_id != 0) {
		/* Custom pools can only use existing chunks. */
		return (chunk_recycle(pool, &pool->chunks_szad_mmap,
				     &pool->chunks_ad_mmap, new_addr, size,
				     alignment, false, zero));
	} else {
		malloc_rwlock_rdlock(&pool->arenas_lock);
		dss_prec_t dss_prec = pool->arenas[arena_ind]->dss_prec;
		malloc_rwlock_unlock(&pool->arenas_lock);
		return (chunk_alloc_core(pool, new_addr, size, alignment,
					 false, zero, dss_prec));
	}
}

void
chunk_record(pool_t *pool, extent_tree_t *chunks_szad, extent_tree_t *chunks_ad, void *chunk,
    size_t size, bool zeroed)
{
	bool unzeroed, file_mapped;
	extent_node_t *xnode, *node, *prev, *xprev, key;

	file_mapped = pool_is_file_mapped(pool);
	unzeroed = pages_purge(chunk, size, file_mapped);
	JEMALLOC_VALGRIND_MAKE_MEM_NOACCESS(chunk, size);

	/*
	 * If pages_purge() returned that the pages were zeroed
	 * as a side effect of purging we can safely do this assignment.
	 */
	if (zeroed == false && unzeroed == false) {
		zeroed = true;
	}

	/*
	 * Allocate a node before acquiring chunks_mtx even though it might not
	 * be needed, because base_node_alloc() may cause a new base chunk to
	 * be allocated, which could cause deadlock if chunks_mtx were already
	 * held.
	 */
	xnode = base_node_alloc(pool);
	/* Use xprev to implement conditional deferred deallocation of prev. */
	xprev = NULL;

	malloc_mutex_lock(&pool->chunks_mtx);
	key.addr = (void *)((uintptr_t)chunk + size);
	node = extent_tree_ad_nsearch(chunks_ad, &key);
	/* Try to coalesce forward. */
	if (node != NULL && node->addr == key.addr) {
		/*
		 * Coalesce chunk with the following address range.  This does
		 * not change the position within chunks_ad, so only
		 * remove/insert from/into chunks_szad.
		 */
		extent_tree_szad_remove(chunks_szad, node);
		node->addr = chunk;
		node->size += size;
		node->zeroed = (node->zeroed && zeroed);
		extent_tree_szad_insert(chunks_szad, node);
	} else {
		/* Coalescing forward failed, so insert a new node. */
		if (xnode == NULL) {
			/*
			 * base_node_alloc() failed, which is an exceedingly
			 * unlikely failure.  Leak chunk; its pages have
			 * already been purged, so this is only a virtual
			 * memory leak.
			 */
			goto label_return;
		}
		node = xnode;
		xnode = NULL; /* Prevent deallocation below. */
		node->addr = chunk;
		node->size = size;
		node->zeroed = zeroed;
		extent_tree_ad_insert(chunks_ad, node);
		extent_tree_szad_insert(chunks_szad, node);
	}

	/* Try to coalesce backward. */
	prev = extent_tree_ad_prev(chunks_ad, node);
	if (prev != NULL && (void *)((uintptr_t)prev->addr + prev->size) ==
	    chunk) {
		/*
		 * Coalesce chunk with the previous address range.  This does
		 * not change the position within chunks_ad, so only
		 * remove/insert node from/into chunks_szad.
		 */
		extent_tree_szad_remove(chunks_szad, prev);
		extent_tree_ad_remove(chunks_ad, prev);

		extent_tree_szad_remove(chunks_szad, node);
		node->addr = prev->addr;
		node->size += prev->size;
		node->zeroed = (node->zeroed && prev->zeroed);
		extent_tree_szad_insert(chunks_szad, node);

		xprev = prev;
	}

label_return:
	malloc_mutex_unlock(&pool->chunks_mtx);
	/*
	 * Deallocate xnode and/or xprev after unlocking chunks_mtx in order to
	 * avoid potential deadlock.
	 */
	if (xnode != NULL)
		base_node_dalloc(pool, xnode);
	if (xprev != NULL)
		base_node_dalloc(pool, xprev);
}

void
chunk_unmap(pool_t *pool, void *chunk, size_t size)
{
	assert(chunk != NULL);
	assert(CHUNK_ADDR2BASE(chunk) == chunk);
	assert(size != 0);
	assert((size & chunksize_mask) == 0);

	if (have_dss && chunk_in_dss(chunk))
		chunk_record(pool, &pool->chunks_szad_dss, &pool->chunks_ad_dss, chunk, size, false);
	else if (chunk_dalloc_mmap(chunk, size))
		chunk_record(pool, &pool->chunks_szad_mmap, &pool->chunks_ad_mmap, chunk, size, false);
}

static void
chunk_dalloc_core(pool_t *pool, void *chunk, size_t size)
{

	assert(chunk != NULL);
	assert(CHUNK_ADDR2BASE(chunk) == chunk);
	assert(size != 0);
	assert((size & chunksize_mask) == 0);

	if (config_ivsalloc)
		rtree_set(pool->chunks_rtree, (uintptr_t)chunk, 0);
	if (config_stats || config_prof) {
		malloc_mutex_lock(&pool->chunks_mtx);
		assert(pool->stats_chunks.curchunks >= (size / chunksize));
		pool->stats_chunks.curchunks -= (size / chunksize);
		malloc_mutex_unlock(&pool->chunks_mtx);
	}

	chunk_unmap(pool, chunk, size);
}

/* Default arena chunk deallocation routine in the absence of user override. */
bool
chunk_dalloc_default(void *chunk, size_t size, unsigned arena_ind, pool_t *pool)
{
	chunk_dalloc_core(pool, chunk, size);
	return (false);
}

bool
chunk_global_boot() {
	if (have_dss && chunk_dss_boot())
		return (true);
	/* Set variables according to the value of opt_lg_chunk. */
	chunksize = (ZU(1) << opt_lg_chunk);
	assert(chunksize >= PAGE);
	chunksize_mask = chunksize - 1;
	chunk_npages = (chunksize >> LG_PAGE);	
	return (false);
}

bool
chunk_boot(pool_t *pool)
{
	if (config_stats || config_prof) {
		if (malloc_mutex_init(&pool->chunks_mtx))
			return (true);
		memset(&pool->stats_chunks, 0, sizeof(chunk_stats_t));
	}
	extent_tree_szad_new(&pool->chunks_szad_mmap);
	extent_tree_ad_new(&pool->chunks_ad_mmap);
	extent_tree_szad_new(&pool->chunks_szad_dss);
	extent_tree_ad_new(&pool->chunks_ad_dss);
	if (config_ivsalloc) {
		pool->chunks_rtree = rtree_new((ZU(1) << (LG_SIZEOF_PTR+3)) -
		    opt_lg_chunk, base_alloc, NULL, pool);
		if (pool->chunks_rtree == NULL)
			return (true);
	}

	return (false);
}

void
chunk_prefork(pool_t *pool)
{

	malloc_mutex_prefork(&pool->chunks_mtx);
	if (config_ivsalloc)
		rtree_prefork(pool->chunks_rtree);
}

void
chunk_postfork_parent(pool_t *pool)
{
	if (config_ivsalloc)
		rtree_postfork_parent(pool->chunks_rtree);
	malloc_mutex_postfork_parent(&pool->chunks_mtx);
}

void
chunk_postfork_child(pool_t *pool)
{
	if (config_ivsalloc)
		rtree_postfork_child(pool->chunks_rtree);
	malloc_mutex_postfork_child(&pool->chunks_mtx);
}
