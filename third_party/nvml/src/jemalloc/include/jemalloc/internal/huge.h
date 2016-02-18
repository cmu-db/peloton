/******************************************************************************/
#ifdef JEMALLOC_H_TYPES

#endif /* JEMALLOC_H_TYPES */
/******************************************************************************/
#ifdef JEMALLOC_H_STRUCTS

#endif /* JEMALLOC_H_STRUCTS */
/******************************************************************************/
#ifdef JEMALLOC_H_EXTERNS

void	*huge_malloc(arena_t *arena, size_t size, bool zero);
void	*huge_palloc(arena_t *arena, size_t size, size_t alignment, bool zero);
bool	huge_ralloc_no_move(pool_t *pool, void *ptr, size_t oldsize, size_t size,
    size_t extra, bool zero);
void	*huge_ralloc(arena_t *arena, void *ptr, size_t oldsize, size_t size,
    size_t extra, size_t alignment, bool zero, bool try_tcache_dalloc);
#ifdef JEMALLOC_JET
typedef void (huge_dalloc_junk_t)(void *, size_t);
extern huge_dalloc_junk_t *huge_dalloc_junk;
#endif
void	huge_dalloc(pool_t *pool, void *ptr);
size_t	huge_salloc(const void *ptr);
size_t	huge_pool_salloc(pool_t *pool, const void *ptr);
prof_ctx_t	*huge_prof_ctx_get(const void *ptr);
void	huge_prof_ctx_set(const void *ptr, prof_ctx_t *ctx);
bool	huge_boot(pool_t *pool);
void	huge_prefork(pool_t *pool);
void	huge_postfork_parent(pool_t *pool);
void	huge_postfork_child(pool_t *pool);

#endif /* JEMALLOC_H_EXTERNS */
/******************************************************************************/
#ifdef JEMALLOC_H_INLINES

#endif /* JEMALLOC_H_INLINES */
/******************************************************************************/
