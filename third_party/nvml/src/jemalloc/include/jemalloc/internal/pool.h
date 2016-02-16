/******************************************************************************/
#ifdef JEMALLOC_H_TYPES

#define POOLS_MIN  16
#define POOLS_MAX  32768

/*
 * We want to expose pool_t to the library user
 * as a result typedef for pool_s is located in "jemalloc.h"
 */
typedef struct tsd_pool_s tsd_pool_t;

/*
 * Dummy arena is used to pass pool structure to choose_arena function
 * through various alloc/free variants
 */
#define ARENA_DUMMY_IND			(~0)
#define DUMMY_ARENA_INITIALIZE(name, p)		\
do {						\
(name).ind = ARENA_DUMMY_IND;			\
(name).pool = (p);				\
} while (0)

#define	TSD_POOL_INITIALIZER		JEMALLOC_ARG_CONCAT({.npools = 0, .arenas = NULL, .seqno = NULL })


#endif /* JEMALLOC_H_TYPES */
/******************************************************************************/
#ifdef JEMALLOC_H_STRUCTS

typedef struct pool_memory_range_node_s {
	uintptr_t addr;
	uintptr_t addr_end;
	uintptr_t usable_addr;
	uintptr_t usable_addr_end;
	struct pool_memory_range_node_s *next;
} pool_memory_range_node_t;

struct pool_s {
	/* This pool's index within the pools array. */
	unsigned pool_id;
	/*
	 * Unique pool number. A pool_id can be reused, seqno helping to check
	 * that data in Thread Storage Data are still valid.
	 */
	unsigned seqno;
	/* Protects arenas initialization (arenas, arenas_total). */
	malloc_rwlock_t	arenas_lock;
	/*
	 * Arenas that are used to service external requests.  Not all elements of the
	 * arenas array are necessarily used; arenas are created lazily as needed.
	 *
	 * arenas[0..narenas_auto) are used for automatic multiplexing of threads and
	 * arenas.  arenas[narenas_auto..narenas_total) are only used if the application
	 * takes some action to create them and allocate from them.
	 */
	arena_t **arenas;
	unsigned narenas_total;
	unsigned narenas_auto;

	/* Tree of chunks that are stand-alone huge allocations. */
	extent_tree_t	huge;
	/* Protects chunk-related data structures. */
	malloc_mutex_t	huge_mtx;

	malloc_mutex_t	chunks_mtx;
	chunk_stats_t	stats_chunks;

	/*
	 * Trees of chunks that were previously allocated (trees differ only in node
	 * ordering).  These are used when allocating chunks, in an attempt to re-use
	 * address space.  Depending on function, different tree orderings are needed,
	 * which is why there are two trees with the same contents.
	 */
	extent_tree_t	chunks_szad_mmap;
	extent_tree_t	chunks_ad_mmap;
	extent_tree_t	chunks_szad_dss;
	extent_tree_t	chunks_ad_dss;

	rtree_t		*chunks_rtree;

	/* Protects base-related data structures. */
	malloc_mutex_t	base_mtx;
	malloc_mutex_t	base_node_mtx;
	/*
	 * Current pages that are being used for internal memory allocations.  These
	 * pages are carved up in cacheline-size quanta, so that there is no chance of
	 * false cache line sharing.
	 */
	void		*base_next_addr;
	void		*base_past_addr; /* Addr immediately past base_pages. */
	extent_node_t	*base_nodes;

	/*
	 * Per pool statistics variables
	 */
	bool		ctl_initialized;
	ctl_stats_t	ctl_stats;
	size_t		ctl_stats_allocated;
	size_t		ctl_stats_active;
	size_t		ctl_stats_mapped;
	size_t		stats_cactive;

	/* Protects list of memory ranges. */
	malloc_mutex_t	memory_range_mtx;

	/* List of memory ranges inside pool, useful for pool_check(). */
	pool_memory_range_node_t *memory_range_list;
};

struct tsd_pool_s {
	size_t npools;		/* size of the arrays */
	unsigned *seqno;	/* Sequence number of pool */
	arena_t **arenas;	/* array of arenas indexed by pool id */
};

/*
 * Minimal size of pool, includes header alignment to cache line size,
 * initial space for base allocator, and size of at least one chunk
 * of memory with address alignment to multiple of chunksize.
 */
#define POOL_MINIMAL_SIZE (3*chunksize)


#endif /* JEMALLOC_H_STRUCTS */
/******************************************************************************/
#ifdef JEMALLOC_H_EXTERNS

bool pool_new(pool_t *pool, unsigned pool_id);
void pool_destroy(pool_t *pool);

extern malloc_mutex_t	pools_lock;
extern malloc_mutex_t	pool_base_lock;

bool pool_boot();
void pool_prefork();
void pool_postfork_parent();
void pool_postfork_child();

#endif /* JEMALLOC_H_EXTERNS */
/******************************************************************************/
#ifdef JEMALLOC_H_INLINES

#ifndef JEMALLOC_ENABLE_INLINE
bool pool_is_file_mapped(pool_t *pool);
#endif

#if (defined(JEMALLOC_ENABLE_INLINE) || defined (JEMALLOC_POOL_C_))
JEMALLOC_INLINE bool
pool_is_file_mapped(pool_t *pool)
{
	return pool->pool_id != 0;
}
#endif

#endif /* JEMALLOC_H_INLINES */
/******************************************************************************/
