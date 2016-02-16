#define	JEMALLOC_BASE_C_
#include "jemalloc/internal/jemalloc_internal.h"


static bool
base_pages_alloc(pool_t *pool, size_t minsize)
{
	size_t csize;
	void* base_pages;

	assert(minsize != 0);
	csize = CHUNK_CEILING(minsize);
	base_pages = chunk_alloc_base(pool, csize);
	if (base_pages == NULL)
		return (true);
	pool->base_next_addr = base_pages;
	pool->base_past_addr = (void *)((uintptr_t)base_pages + csize);

	return (false);
}

void *
base_alloc(pool_t *pool, size_t size)
{
	void *ret;
	size_t csize;

	/* Round size up to nearest multiple of the cacheline size. */
	csize = CACHELINE_CEILING(size);

	malloc_mutex_lock(&pool->base_mtx);
	/* Make sure there's enough space for the allocation. */
	if ((uintptr_t)pool->base_next_addr + csize > (uintptr_t)pool->base_past_addr) {
		if (base_pages_alloc(pool, csize)) {
			malloc_mutex_unlock(&pool->base_mtx);
			return (NULL);
		}
	}
	/* Allocate. */
	ret = pool->base_next_addr;
	pool->base_next_addr = (void *)((uintptr_t)pool->base_next_addr + csize);
	malloc_mutex_unlock(&pool->base_mtx);
	JEMALLOC_VALGRIND_MAKE_MEM_UNDEFINED(ret, csize);

	return (ret);
}

void *
base_calloc(pool_t *pool, size_t number, size_t size)
{
	void *ret = base_alloc(pool, number * size);

	if (ret != NULL)
		memset(ret, 0, number * size);

	return (ret);
}

extent_node_t *
base_node_alloc(pool_t *pool)
{
	extent_node_t *ret;

	malloc_mutex_lock(&pool->base_node_mtx);
	if (pool->base_nodes != NULL) {
		ret = pool->base_nodes;
		pool->base_nodes = *(extent_node_t **)ret;
		JEMALLOC_VALGRIND_MAKE_MEM_UNDEFINED(ret,
			sizeof(extent_node_t));
	} else {
		/* preallocated nodes for pools other than 0 */
		if (pool->pool_id == 0) {
			ret = (extent_node_t *)base_alloc(pool, sizeof(extent_node_t));
		} else {
			ret = NULL;
		}
	}
	malloc_mutex_unlock(&pool->base_node_mtx);
	return (ret);
}

void
base_node_dalloc(pool_t *pool, extent_node_t *node)
{

	JEMALLOC_VALGRIND_MAKE_MEM_UNDEFINED(node, sizeof(extent_node_t));
	malloc_mutex_lock(&pool->base_node_mtx);
	*(extent_node_t **)node = pool->base_nodes;
	pool->base_nodes = node;
	malloc_mutex_unlock(&pool->base_node_mtx);
}

size_t
base_node_prealloc(pool_t *pool, size_t number)
{
	extent_node_t *node;
	malloc_mutex_lock(&pool->base_node_mtx);
	for (; number > 0; --number) {
		node = (extent_node_t *)base_alloc(pool, sizeof(extent_node_t));
		if (node == NULL)
			break;
		JEMALLOC_VALGRIND_MAKE_MEM_UNDEFINED(node, sizeof(extent_node_t));
		*(extent_node_t **)node = pool->base_nodes;
		pool->base_nodes = node;
	}
	malloc_mutex_unlock(&pool->base_node_mtx);

	/* return number of nodes that couldn't be allocated */
	return number;
}

bool
base_boot(pool_t *pool)
{
	pool->base_nodes = NULL;
	if (malloc_mutex_init(&pool->base_mtx))
		return (true);

	if (malloc_mutex_init(&pool->base_node_mtx))
		return (true);

	return (false);
}

void
base_prefork(pool_t *pool)
{

	malloc_mutex_prefork(&pool->base_mtx);
}

void
base_postfork_parent(pool_t *pool)
{

	malloc_mutex_postfork_parent(&pool->base_mtx);
}

void
base_postfork_child(pool_t *pool)
{

	malloc_mutex_postfork_child(&pool->base_mtx);
}
