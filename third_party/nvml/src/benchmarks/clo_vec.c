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
 * clo_vec.c -- command line options vector definitions
 */
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "clo_vec.h"

/*
 * clo_vec_alloc -- allocate new CLO vector
 */
struct clo_vec *
clo_vec_alloc(size_t size)
{
	struct clo_vec *clovec = malloc(sizeof (*clovec));
	assert(clovec != NULL);

	/* init list of arguments and allocations */
	TAILQ_INIT(&clovec->allocs);
	TAILQ_INIT(&clovec->args);

	clovec->nallocs = 0;

	/* size of each struct */
	clovec->size = size;

	/* add first struct to list */
	struct clo_vec_args *args = malloc(sizeof (*args));
	assert(args != NULL);

	args->args = calloc(1, size);
	assert(args->args != NULL);

	TAILQ_INSERT_TAIL(&clovec->args, args, next);

	clovec->nargs = 1;

	return clovec;
}

/*
 * clo_vec_free -- free CLO vector and all allocations
 */
void
clo_vec_free(struct clo_vec *clovec)
{
	assert(clovec != NULL);

	/* free all allocations */
	while (!TAILQ_EMPTY(&clovec->allocs)) {
		struct clo_vec_alloc *alloc = TAILQ_FIRST(&clovec->allocs);
		TAILQ_REMOVE(&clovec->allocs, alloc, next);
		free(alloc->ptr);
		free(alloc);
	}

	/* free all arguments */
	while (!TAILQ_EMPTY(&clovec->args)) {
		struct clo_vec_args *args = TAILQ_FIRST(&clovec->args);
		TAILQ_REMOVE(&clovec->args, args, next);
		free(args->args);
		free(args);
	}

	free(clovec);
}

/*
 * clo_vec_get_args -- return pointer to CLO arguments at specified index
 */
void *
clo_vec_get_args(struct clo_vec *clovec, size_t i)
{
	if (i >= clovec->nargs)
		return NULL;
	size_t c = 0;
	struct clo_vec_args *args;
	TAILQ_FOREACH(args, &clovec->args, next) {
		if (c == i)
			return args->args;
		c++;
	}

	return NULL;
}

/*
 * clo_vec_add_alloc -- add allocation to CLO vector
 */
int
clo_vec_add_alloc(struct clo_vec *clovec, void *ptr)
{
	struct clo_vec_alloc *alloc = malloc(sizeof (*alloc));
	assert(alloc != NULL);

	alloc->ptr = ptr;
	TAILQ_INSERT_TAIL(&clovec->allocs, alloc, next);
	clovec->nallocs++;
	return 0;
}

/*
 * clo_vec_grow -- (internal) grow in size the CLO vector
 */
static void
clo_vec_grow(struct clo_vec *clovec, size_t new_len)
{
	size_t nargs = new_len - clovec->nargs;
	size_t i;

	for (i = 0; i < nargs; i++) {
		struct clo_vec_args *args = calloc(1, sizeof (*args));
		assert(args != NULL);

		TAILQ_INSERT_TAIL(&clovec->args, args, next);

		args->args = malloc(clovec->size);
		assert(args->args != NULL);

		void *argscpy = clo_vec_get_args(clovec, i % clovec->nargs);
		assert(argscpy != NULL);

		memcpy(args->args, argscpy, clovec->size);
	}

	clovec->nargs = new_len;
}

/*
 * clo_vec_vlist_alloc -- allocate list of values
 */
struct clo_vec_vlist *
clo_vec_vlist_alloc(void)
{
	struct clo_vec_vlist *list = malloc(sizeof (*list));
	assert(list != NULL);

	list->nvalues = 0;
	TAILQ_INIT(&list->head);

	return list;
}

/*
 * clo_vec_vlist_free -- release list of values
 */
void
clo_vec_vlist_free(struct clo_vec_vlist *list)
{
	assert(list != NULL);

	while (!TAILQ_EMPTY(&list->head)) {
		struct clo_vec_value *val = TAILQ_FIRST(&list->head);
		TAILQ_REMOVE(&list->head, val, next);
		free(val->ptr);
		free(val);
	}

	free(list);
}

/*
 * clo_vec_vlist_add -- add value to list
 */
void
clo_vec_vlist_add(struct clo_vec_vlist *list, void *ptr, size_t size)
{
	struct clo_vec_value *val = malloc(sizeof (*val));
	assert(val != NULL);

	val->ptr = malloc(size);
	assert(val->ptr != NULL);

	memcpy(val->ptr, ptr, size);
	TAILQ_INSERT_TAIL(&list->head, val, next);
	list->nvalues++;
}

/*
 * clo_vec_memcpy -- copy value to CLO vector
 *
 * - clovec - CLO vector
 * - off    - offset to value in structure
 * - size   - size of value field
 * - ptr    - pointer to value
 */
int
clo_vec_memcpy(struct clo_vec *clovec, size_t off, size_t size, void *ptr)
{
	if (off + size > clovec->size)
		return -1;

	size_t i;
	for (i = 0; i < clovec->nargs; i++) {
		char *args = clo_vec_get_args(clovec, i);
		char *dptr = args + off;
		memcpy(dptr, ptr, size);
	}

	return 0;
}

/*
 * clo_vec_memcpy_list -- copy values from list to CLO vector
 *
 * - clovec - CLO vector
 * - off    - offset to value in structure
 * - size   - size of value field
 * - list   - list of values
 */
int
clo_vec_memcpy_list(struct clo_vec *clovec, size_t off, size_t size,
		struct clo_vec_vlist *list)
{
	if (off + size > clovec->size)
		return -1;

	size_t len = clovec->nargs;
	if (list->nvalues > 1)
		clo_vec_grow(clovec, clovec->nargs * list->nvalues);

	struct clo_vec_value *value;
	size_t value_i = 0;
	size_t i;

	TAILQ_FOREACH(value, &list->head, next) {
		for (i = value_i * len; i < (value_i + 1) * len; i++) {
			char *args = clo_vec_get_args(clovec, i);
			char *dptr = args + off;
			memcpy(dptr, value->ptr, size);
		}
		value_i++;
	}

	return 0;
}
