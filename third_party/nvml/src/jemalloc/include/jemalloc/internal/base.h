/******************************************************************************/
#ifdef JEMALLOC_H_TYPES

#endif /* JEMALLOC_H_TYPES */
/******************************************************************************/
#ifdef JEMALLOC_H_STRUCTS

#endif /* JEMALLOC_H_STRUCTS */
/******************************************************************************/
#ifdef JEMALLOC_H_EXTERNS

void	*base_alloc(pool_t *pool, size_t size);
void	*base_calloc(pool_t *pool, size_t number, size_t size);
extent_node_t *base_node_alloc(pool_t *pool);
void	base_node_dalloc(pool_t *pool, extent_node_t *node);
size_t	base_node_prealloc(pool_t *pool, size_t number);
bool	base_boot(pool_t *pool);
void	base_prefork(pool_t *pool);
void	base_postfork_parent(pool_t *pool);
void	base_postfork_child(pool_t *pool);

#endif /* JEMALLOC_H_EXTERNS */
/******************************************************************************/
#ifdef JEMALLOC_H_INLINES

#endif /* JEMALLOC_H_INLINES */
/******************************************************************************/
