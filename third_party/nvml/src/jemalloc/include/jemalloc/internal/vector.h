/******************************************************************************/
#ifdef JEMALLOC_H_TYPES

typedef struct vector_s vector_t;
typedef struct vec_list_s vec_list_t;

#define VECTOR_MIN_PART_SIZE 8

#define VECTOR_INITIALIZER	JEMALLOC_ARG_CONCAT({.data = NULL, .size = 0})

#endif /* JEMALLOC_H_TYPES */
/******************************************************************************/
#ifdef JEMALLOC_H_STRUCTS

struct vec_list_s {
	vec_list_t *next;
	int length;
	void *data[];
};

struct vector_s {
	vec_list_t *list;
};

#endif /* JEMALLOC_H_STRUCTS */
/******************************************************************************/
#ifdef JEMALLOC_H_EXTERNS

void *vec_get(vector_t *vector, int index);
void vec_set(vector_t *vector, int index, void *val);
void vec_delete(vector_t *vector);

#endif /* JEMALLOC_H_EXTERNS */
/******************************************************************************/
#ifdef JEMALLOC_H_INLINES

#endif /* JEMALLOC_H_INLINES */
/******************************************************************************/
