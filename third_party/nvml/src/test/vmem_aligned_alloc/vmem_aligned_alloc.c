/*
 * Copyright (c) 2014-2015, Intel Corporation
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
 * vmem_aligned_alloc.c -- unit test for vmem_aligned_alloc
 *
 * usage: vmem_aligned_alloc [directory]
 */

#include "unittest.h"

#define	MAX_ALLOCS (100)

static int custom_allocs;
static int custom_alloc_calls;

/*
 * malloc_custom -- custom malloc function
 *
 * This function updates statistics about custom alloc functions,
 * and returns allocated memory.
 */
static void *
malloc_custom(size_t size)
{
	++custom_alloc_calls;
	++custom_allocs;
	return malloc(size);
}

/*
 * free_custom -- custom free function
 *
 * This function updates statistics about custom alloc functions,
 * and frees allocated memory.
 */
static void
free_custom(void *ptr)
{
	++custom_alloc_calls;
	--custom_allocs;
	free(ptr);
}

/*
 * realloc_custom -- custom realloc function
 *
 * This function updates statistics about custom alloc functions,
 * and returns reallocated memory.
 */
static void *
realloc_custom(void *ptr, size_t size)
{
	++custom_alloc_calls;
	return realloc(ptr, size);
}

/*
 * strdup_custom -- custom strdup function
 *
 * This function updates statistics about custom alloc functions,
 * and returns allocated memory with a duplicated string.
 */
static char *
strdup_custom(const char *s)
{
	++custom_alloc_calls;
	++custom_allocs;
	return strdup(s);
}

int
main(int argc, char *argv[])
{
	const int test_value = 123456;
	char *dir = NULL;
	VMEM *vmp;
	size_t alignment;
	unsigned i;
	int *ptr;
	int *ptrs[MAX_ALLOCS];

	START(argc, argv, "vmem_aligned_alloc");

	if (argc == 2) {
		dir = argv[1];
	} else if (argc > 2) {
		FATAL("usage: %s [directory]", argv[0]);
	}

	/* allocate memory for function vmem_create_in_region() */
	void *mem_pool = MMAP_ANON_ALIGNED(VMEM_MIN_POOL, 4 << 20);

	/* use custom alloc functions to check for memory leaks */
	vmem_set_funcs(malloc_custom, free_custom,
		realloc_custom, strdup_custom, NULL);

	/* test with address alignment from 2B to 4MB */
	for (alignment = 2; alignment <= 4 * 1024 * 1024; alignment *= 2) {

		if (dir == NULL) {
			vmp = vmem_create_in_region(mem_pool,
				VMEM_MIN_POOL);
			if (vmp == NULL)
				FATAL("!vmem_create_in_region");
		} else {
			vmp = vmem_create(dir, VMEM_MIN_POOL);
			if (vmp == NULL)
				FATAL("!vmem_create");
		}

		memset(ptrs, 0, MAX_ALLOCS * sizeof (ptrs[0]));

		for (i = 0; i < MAX_ALLOCS; ++i) {
			ptr = vmem_aligned_alloc(vmp, alignment, sizeof (int));
			ptrs[i] = ptr;

			/* at least one allocation must succeed */
			ASSERT(i != 0 || ptr != NULL);
			if (ptr == NULL)
				break;

			/* ptr should be usable */
			*ptr = test_value;
			ASSERTeq(*ptr, test_value);

			/* check for correct address alignment */
			ASSERTeq((uintptr_t)(ptr) & (alignment - 1), 0);

			/* check that pointer came from mem_pool */
			if (dir == NULL) {
				ASSERTrange(ptr, mem_pool, VMEM_MIN_POOL);
			}
		}

		for (i = 0; i < MAX_ALLOCS; ++i) {
			if (ptrs[i] == NULL)
				break;
			vmem_free(vmp, ptrs[i]);
		}

		vmem_delete(vmp);
	}

	/* check memory leaks */
	ASSERTne(custom_alloc_calls, 0);
	ASSERTeq(custom_allocs, 0);

	DONE(NULL);
}
