#define	JEMALLOC_POOL_C_
#include "jemalloc/internal/jemalloc_internal.h"

malloc_mutex_t	pool_base_lock;
malloc_mutex_t	pools_lock;

/* Initialize pool and create its base arena. */
bool pool_new(pool_t *pool, unsigned pool_id)
{
	pool->pool_id = pool_id;

	if (malloc_mutex_init(&pool->memory_range_mtx)) {
		return (true);
	}

	if (malloc_rwlock_init(&pool->arenas_lock)) {
		return (true);
	}

	if (base_boot(pool)) {
		return (true);
	}

	if (chunk_boot(pool)) {
		return (true);
	}

	if (huge_boot(pool)) {
		return (true);
	}

	if (pools_shared_data_create()) {
		return (true);
	}

	pool->stats_cactive = 0;
	pool->ctl_stats_active = 0;
	pool->ctl_stats_allocated = 0;
	pool->ctl_stats_mapped = 0;

	pool->narenas_auto = opt_narenas;
	/*
	 * Make sure that the arenas array can be allocated.  In practice, this
	 * limit is enough to allow the allocator to function, but the ctl
	 * machinery will fail to allocate memory at far lower limits.
	 */
	if (pool->narenas_auto > chunksize / sizeof(arena_t *)) {
		pool->narenas_auto = chunksize / sizeof(arena_t *);
		malloc_printf("<jemalloc>: Reducing narenas to limit (%d)\n",
		   pool->narenas_auto);
	}
	pool->narenas_total = pool->narenas_auto;

	/* Allocate and initialize arenas. */
	pool->arenas = (arena_t **)base_calloc(pool, sizeof(arena_t *),
		pool->narenas_total);

	if (pool->arenas == NULL) {
		return (true);
	}

	arenas_extend(pool, 0);

	return false;
}

/* Release the arenas associated with a pool. */
void pool_destroy(pool_t *pool)
{
	int i;
	for (i = 0; i < pool->narenas_total; ++i) {
		if (pool->arenas[i] != NULL) {
			arena_purge_all(pool->arenas[i]);
		}
	}
	/*
	 * Set 'pool_id' to an incorrect value
	 * so that the pool cannot be used
	 * after being deleted.
	 */
	pool->pool_id = UINT_MAX;
	malloc_rwlock_destroy(&pool->arenas_lock);
}

bool pool_boot()
{
	if (malloc_mutex_init(&pools_lock)) {
		return (true);
	}

	if (malloc_mutex_init(&pool_base_lock)) {
		return (true);
	}

	return (false);
}

void pool_prefork()
{
	malloc_mutex_prefork(&pools_lock);
	malloc_mutex_prefork(&pool_base_lock);
}

void pool_postfork_parent()
{
	malloc_mutex_postfork_parent(&pools_lock);
	malloc_mutex_postfork_parent(&pool_base_lock);
}

void pool_postfork_child()
{
	malloc_mutex_postfork_child(&pools_lock);
	malloc_mutex_postfork_child(&pool_base_lock);
}
