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
 * vmem_valgrind.c -- unit test for vmem_valgrind
 *
 * usage: vmem_valgrind <test-number> [directory]
 *
 * test-number can be a number from 0 to 9
 */

#include "unittest.h"

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
	char *dir = NULL;
	VMEM *vmp;
	int *ptr;
	int test_case = -1;
	int expect_custom_alloc = 0;

	START(argc, argv, "vmem_valgrind");

	if (argc >= 2 && argc <= 3) {
		test_case = atoi(argv[1]);
		if (test_case > 9)
			test_case = -1;

		if (argc > 2)
			dir = argv[2];
	}

	if (test_case < 0)
		FATAL("usage: %s <test-number from 0 to 9> [directory]",
			argv[0]);

	if (test_case < 5) {
		OUT("use default allocator");
		expect_custom_alloc = 0;
	} else {
		OUT("use custom alloc functions");
		test_case -= 5;
		expect_custom_alloc = 1;
		vmem_set_funcs(malloc_custom, free_custom,
			realloc_custom, strdup_custom, NULL);
	}

	if (dir == NULL) {
		/* allocate memory for function vmem_create_in_region() */
		void *mem_pool = MMAP_ANON_ALIGNED(VMEM_MIN_POOL, 4 << 20);

		vmp = vmem_create_in_region(mem_pool, VMEM_MIN_POOL);
		if (vmp == NULL)
			FATAL("!vmem_create_in_region");
	} else {
		vmp = vmem_create(dir, VMEM_MIN_POOL);
		if (vmp == NULL)
			FATAL("!vmem_create");
	}

	switch (test_case) {
		case 0: {
			OUT("remove all allocations and delete pool");
			ptr = vmem_malloc(vmp, sizeof (int));
			if (ptr == NULL)
				FATAL("!vmem_malloc");

			vmem_free(vmp, ptr);
			vmem_delete(vmp);
			break;
		}
		case 1: {
			OUT("only remove allocations");
			ptr = vmem_malloc(vmp, sizeof (int));
			if (ptr == NULL)
				FATAL("!vmem_malloc");

			vmem_free(vmp, ptr);
			break;
		}
		case 2: {
			OUT("only delete pool");
			ptr = vmem_malloc(vmp, sizeof (int));
			if (ptr == NULL)
				FATAL("!vmem_malloc");

			vmem_delete(vmp);

			/* prevent reporting leaked memory as still reachable */
			ptr = NULL;
			break;
		}
		case 3: {
			OUT("memory leaks");
			ptr = vmem_malloc(vmp, sizeof (int));
			if (ptr == NULL)
				FATAL("!vmem_malloc");

			/* prevent reporting leaked memory as still reachable */
			ptr = NULL;
			break;
		}
		case 4: {
			OUT("heap block overrun");
			ptr = vmem_malloc(vmp, 12 * sizeof (int));
			if (ptr == NULL)
				FATAL("!vmem_malloc");

			/* heap block overrun */
			ptr[12] = 7;

			vmem_free(vmp, ptr);
			vmem_delete(vmp);
			break;
		}
		default: {
			FATAL("!unknown test-number");
		}
	}

	/* check memory leak in custom allocator */
	ASSERTeq(custom_allocs, 0);

	if (expect_custom_alloc == 0) {
		ASSERTeq(custom_alloc_calls, 0);
	} else {
		ASSERTne(custom_alloc_calls, 0);
	}

	DONE(NULL);
}
