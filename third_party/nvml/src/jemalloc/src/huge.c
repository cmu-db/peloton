#define	JEMALLOC_HUGE_C_
#include "jemalloc/internal/jemalloc_internal.h"

void *
huge_malloc(arena_t *arena, size_t size, bool zero)
{
	return (huge_palloc(arena, size, chunksize, zero));
}

void *
huge_palloc(arena_t *arena, size_t size, size_t alignment, bool zero)
{
	void *ret;
	size_t csize;
	extent_node_t *node;
	bool is_zeroed;
	pool_t *pool;

	/* Allocate one or more contiguous chunks for this request. */

	csize = CHUNK_CEILING(size);
	if (csize == 0) {
		/* size is large enough to cause size_t wrap-around. */
		return (NULL);
	}

	/*
	 * Copy zero into is_zeroed and pass the copy to chunk_alloc(), so that
	 * it is possible to make correct junk/zero fill decisions below.
	 */
	is_zeroed = zero;
	arena = choose_arena(arena);
	if (arena == NULL)
		return (NULL);

	pool = arena->pool;

	/* Allocate an extent node with which to track the chunk. */
	node = base_node_alloc(pool);
	if (node == NULL)
		return (NULL);

	ret = arena_chunk_alloc_huge(arena, NULL, csize, alignment, &is_zeroed);
	if (ret == NULL) {
		base_node_dalloc(pool, node);
		return (NULL);
	}

	/* Insert node into huge. */
	node->addr = ret;
	node->size = csize;
	node->arena = arena;

	malloc_mutex_lock(&pool->huge_mtx);
	extent_tree_ad_insert(&pool->huge, node);
	malloc_mutex_unlock(&pool->huge_mtx);

	if (config_fill && zero == false) {
		if (opt_junk)
			memset(ret, 0xa5, csize);
		else if (opt_zero && is_zeroed == false)
			memset(ret, 0, csize);
	}

	return (ret);
}

#ifdef JEMALLOC_JET
#undef huge_dalloc_junk
#define	huge_dalloc_junk JEMALLOC_N(huge_dalloc_junk_impl)
#endif
static void
huge_dalloc_junk(void *ptr, size_t usize)
{

	if (config_fill && have_dss && unlikely(opt_junk)) {
		/*
		 * Only bother junk filling if the chunk isn't about to be
		 * unmapped.
		 */
		if (config_munmap == false || (have_dss && chunk_in_dss(ptr)))
			memset(ptr, 0x5a, usize);
	}
}
#ifdef JEMALLOC_JET
#undef huge_dalloc_junk
#define	huge_dalloc_junk JEMALLOC_N(huge_dalloc_junk)
huge_dalloc_junk_t *huge_dalloc_junk = JEMALLOC_N(huge_dalloc_junk_impl);
#endif

static bool
huge_ralloc_no_move_expand(pool_t *pool, void *ptr, size_t oldsize, size_t size, bool zero) {
	size_t csize;
	void *expand_addr;
	size_t expand_size;
	extent_node_t *node, key;
	arena_t *arena;
	bool is_zeroed;
	void *ret;

	csize = CHUNK_CEILING(size);
	if (csize == 0) {
		/* size is large enough to cause size_t wrap-around. */
		return (true);
	}

	expand_addr = ptr + oldsize;
	expand_size = csize - oldsize;

	malloc_mutex_lock(&pool->huge_mtx);

	key.addr = ptr;
	node = extent_tree_ad_search(&pool->huge, &key);
	assert(node != NULL);
	assert(node->addr == ptr);

	/* Find the current arena. */
	arena = node->arena;

	malloc_mutex_unlock(&pool->huge_mtx);

	/*
	 * Copy zero into is_zeroed and pass the copy to chunk_alloc(), so that
	 * it is possible to make correct junk/zero fill decisions below.
	 */
	is_zeroed = zero;
	ret = arena_chunk_alloc_huge(arena, expand_addr, expand_size, chunksize,
				     &is_zeroed);
	if (ret == NULL)
		return (true);

	assert(ret == expand_addr);

	malloc_mutex_lock(&pool->huge_mtx);
	/* Update the size of the huge allocation. */
	node->size = csize;
	malloc_mutex_unlock(&pool->huge_mtx);

	if (config_fill && !zero) {
		if (unlikely(opt_junk))
			memset(expand_addr, 0xa5, expand_size);
		else if (unlikely(opt_zero) && !is_zeroed)
			memset(expand_addr, 0, expand_size);
	}
	return (false);
}


bool
huge_ralloc_no_move(pool_t *pool, void *ptr, size_t oldsize, size_t size,
    size_t extra, bool zero)
{

	/* Both allocations must be huge to avoid a move. */
	if (oldsize <= arena_maxclass)
		return (true);

	assert(CHUNK_CEILING(oldsize) == oldsize);

	/*
	 * Avoid moving the allocation if the size class can be left the same.
	 */
	if (CHUNK_CEILING(oldsize) >= CHUNK_CEILING(size)
	    && CHUNK_CEILING(oldsize) <= CHUNK_CEILING(size+extra)) {
		return (false);
	}

	/* Overflow. */
	if (CHUNK_CEILING(size) == 0)
		return (true);

	/* Shrink the allocation in-place. */
	if (CHUNK_CEILING(oldsize) > CHUNK_CEILING(size)) {
		extent_node_t *node, key;
		void *excess_addr;
		size_t excess_size;

		malloc_mutex_lock(&pool->huge_mtx);

		key.addr = ptr;
		node = extent_tree_ad_search(&pool->huge, &key);
		assert(node != NULL);
		assert(node->addr == ptr);

		/* Update the size of the huge allocation. */
		node->size = CHUNK_CEILING(size);

		malloc_mutex_unlock(&pool->huge_mtx);

		excess_addr = node->addr + CHUNK_CEILING(size);
		excess_size = CHUNK_CEILING(oldsize) - CHUNK_CEILING(size);

		/* Zap the excess chunks. */
		huge_dalloc_junk(excess_addr, excess_size);
		arena_chunk_dalloc_huge(node->arena, excess_addr, excess_size);

		return (false);
	}

	/* Attempt to expand the allocation in-place. */
	if (huge_ralloc_no_move_expand(pool, ptr, oldsize, size + extra, zero)) {
		if (extra == 0)
			return (true);

		/* Try again, this time without extra. */
		return (huge_ralloc_no_move_expand(pool, ptr, oldsize, size, zero));
	}
	return (false);
}

void *
huge_ralloc(arena_t *arena, void *ptr, size_t oldsize, size_t size,
    size_t extra, size_t alignment, bool zero, bool try_tcache_dalloc)
{
	void *ret;
	size_t copysize;

	/* Try to avoid moving the allocation. */
	if (huge_ralloc_no_move(arena->pool, ptr, oldsize, size, extra, zero) == false)
		return (ptr);

	/*
	 * size and oldsize are different enough that we need to use a
	 * different size class.  In that case, fall back to allocating new
	 * space and copying.
	 */
	if (alignment > chunksize)
		ret = huge_palloc(arena, size + extra, alignment, zero);
	else
		ret = huge_malloc(arena, size + extra, zero);

	if (ret == NULL) {
		if (extra == 0)
			return (NULL);
		/* Try again, this time without extra. */
		if (alignment > chunksize)
			ret = huge_palloc(arena, size, alignment, zero);
		else
			ret = huge_malloc(arena, size, zero);

		if (ret == NULL)
			return (NULL);
	}

	/*
	 * Copy at most size bytes (not size+extra), since the caller has no
	 * expectation that the extra bytes will be reliably preserved.
	 */
	copysize = (size < oldsize) ? size : oldsize;
	memcpy(ret, ptr, copysize);
	pool_iqalloct(arena->pool, ptr, try_tcache_dalloc);
	return (ret);
}

void
huge_dalloc(pool_t *pool, void *ptr)
{
	extent_node_t *node, key;

	malloc_mutex_lock(&pool->huge_mtx);

	/* Extract from tree of huge allocations. */
	key.addr = ptr;
	node = extent_tree_ad_search(&pool->huge, &key);
	assert(node != NULL);
	assert(node->addr == ptr);
	extent_tree_ad_remove(&pool->huge, node);

	malloc_mutex_unlock(&pool->huge_mtx);

	huge_dalloc_junk(node->addr, node->size);
	arena_chunk_dalloc_huge(node->arena, node->addr, node->size);
	base_node_dalloc(pool, node);
}

size_t
huge_salloc(const void *ptr)
{
	size_t ret = 0;
	int i;
	extent_node_t *node, key;

	malloc_mutex_lock(&pools_lock);
	for (i = 0; i < npools; ++i) {
		pool_t *pool = pools[i];
		if (pool == NULL)
			continue;
		malloc_mutex_lock(&pool->huge_mtx);

		/* Extract from tree of huge allocations. */
		key.addr = __DECONST(void *, ptr);
		node = extent_tree_ad_search(&pool->huge, &key);
		if (node != NULL)
			ret = node->size;

		malloc_mutex_unlock(&pool->huge_mtx);
		if (ret != 0)
			break;
	}

	malloc_mutex_unlock(&pools_lock);
	return (ret);
}

size_t
huge_pool_salloc(pool_t *pool, const void *ptr)
{
	size_t ret = 0;
	extent_node_t *node, key;
	malloc_mutex_lock(&pool->huge_mtx);

	/* Extract from tree of huge allocations. */
	key.addr = __DECONST(void *, ptr);
	node = extent_tree_ad_search(&pool->huge, &key);
	if (node != NULL)
		ret = node->size;

	malloc_mutex_unlock(&pool->huge_mtx);
	return (ret);
}

prof_ctx_t *
huge_prof_ctx_get(const void *ptr)
{
	prof_ctx_t *ret = NULL;
	int i;
	extent_node_t *node, key;

	malloc_mutex_lock(&pools_lock);
	for (i = 0; i < npools; ++i) {
		pool_t *pool = pools[i];
		if (pool == NULL)
			continue;
		malloc_mutex_lock(&pool->huge_mtx);

		/* Extract from tree of huge allocations. */
		key.addr = __DECONST(void *, ptr);
		node = extent_tree_ad_search(&pool->huge, &key);
		if (node != NULL)
			ret = node->prof_ctx;

		malloc_mutex_unlock(&pool->huge_mtx);
		if (ret != NULL)
			break;
	}
	malloc_mutex_unlock(&pools_lock);

	return (ret);
}

void
huge_prof_ctx_set(const void *ptr, prof_ctx_t *ctx)
{
	extent_node_t *node, key;
	int i;

	malloc_mutex_lock(&pools_lock);
	for (i = 0; i < npools; ++i) {
		pool_t *pool = pools[i];
		if (pool == NULL)
			continue;
		malloc_mutex_lock(&pool->huge_mtx);

		/* Extract from tree of huge allocations. */
		key.addr = __DECONST(void *, ptr);
		node = extent_tree_ad_search(&pool->huge, &key);
		if (node != NULL)
			node->prof_ctx = ctx;

		malloc_mutex_unlock(&pool->huge_mtx);

		if (node != NULL)
			break;
	}
	malloc_mutex_unlock(&pools_lock);
}

bool
huge_boot(pool_t *pool)
{

	/* Initialize chunks data. */
	if (malloc_mutex_init(&pool->huge_mtx))
		return (true);
	extent_tree_ad_new(&pool->huge);

	return (false);
}

void
huge_prefork(pool_t *pool)
{

	malloc_mutex_prefork(&pool->huge_mtx);
}

void
huge_postfork_parent(pool_t *pool)
{

	malloc_mutex_postfork_parent(&pool->huge_mtx);
}

void
huge_postfork_child(pool_t *pool)
{

	malloc_mutex_postfork_child(&pool->huge_mtx);
}
