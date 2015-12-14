#define	JEMALLOC_VECTOR_C_
#include "jemalloc/internal/jemalloc_internal.h"

/* Round up the value to the closest power of two. */
static inline unsigned
ceil_p2(unsigned n)
{
	return 1 << (32 - __builtin_clz(n));
}

/* Calculate how big should be the vector list array. */
static inline unsigned
get_vec_part_len(unsigned n)
{
	return MAX(ceil_p2(n), VECTOR_MIN_PART_SIZE);
}

/*
 * Find the vector list element in which the index should be stored,
 * if no such list exist return a pointer to a place in memory where it should
 * be allocated.
 */
static vec_list_t  **
find_vec_list(vector_t *vector, int *index)
{
	vec_list_t **vec_list;

	for (vec_list = &vector->list;
		*vec_list != NULL; vec_list = &(*vec_list)->next) {
		if (*index < (*vec_list)->length)
			break;

		*index -= (*vec_list)->length;
	}

	return vec_list;
}

/* Return a value from vector at index. */
void *
vec_get(vector_t *vector, int index)
{
	vec_list_t *vec_list = *find_vec_list(vector, &index);

	return (vec_list == NULL) ? NULL : vec_list->data[index];
}

/* Set a value to vector at index. */
void
vec_set(vector_t *vector, int index, void *val)
{
	vec_list_t **vec_list = find_vec_list(vector, &index);

	/*
	 * There's no array to put the value in,
	 * which means a new one has to be allocated.
	 */
	if (*vec_list == NULL) {
		int vec_part_len = get_vec_part_len(index);

		*vec_list = je_base_malloc(sizeof(vec_list_t) +
				sizeof(void *) * vec_part_len);
		if (*vec_list == NULL)
			return;
		(*vec_list)->next = NULL;
		(*vec_list)->length = vec_part_len;
	}

	(*vec_list)->data[index] = val;
}

/* Free all the memory in the container. */
void
vec_delete(vector_t *vector)
{
	vec_list_t *vec_list_next, *vec_list = vector->list;
	while (vec_list != NULL) {
		vec_list_next = vec_list->next;
		je_base_free(vec_list);
		vec_list = vec_list_next;
	}
}