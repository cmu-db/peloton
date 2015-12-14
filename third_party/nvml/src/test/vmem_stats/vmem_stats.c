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
 * vmem_stats.c -- unit test for vmem_stats
 *
 * usage: vmem_stats 0|1 [opts]
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
	int expect_custom_alloc = 0;
	char *opts = "";
	void *mem_pool;
	VMEM *vmp_unused;
	VMEM *vmp_used;

	START(argc, argv, "vmem_stats");

	if (argc > 3 || argc < 2) {
		FATAL("usage: %s 0|1 [opts]", argv[0]);
	} else {
		expect_custom_alloc = atoi(argv[1]);
		if (argc > 2)
			opts = argv[2];
	}

	if (expect_custom_alloc)
		vmem_set_funcs(malloc_custom, free_custom,
				realloc_custom, strdup_custom, NULL);

	mem_pool = MMAP_ANON_ALIGNED(VMEM_MIN_POOL, 4 << 20);

	vmp_unused = vmem_create_in_region(mem_pool, VMEM_MIN_POOL);
	if (vmp_unused == NULL)
		FATAL("!vmem_create_in_region");

	mem_pool = MMAP_ANON_ALIGNED(VMEM_MIN_POOL, 4 << 20);

	vmp_used = vmem_create_in_region(mem_pool, VMEM_MIN_POOL);
	if (vmp_used == NULL)
		FATAL("!vmem_create_in_region");

	int *test = vmem_malloc(vmp_used, sizeof (int)*100);
	ASSERTne(test, NULL);

	vmem_stats_print(vmp_unused, opts);
	vmem_stats_print(vmp_used, opts);

	vmem_free(vmp_used, test);

	vmem_delete(vmp_unused);
	vmem_delete(vmp_used);

	/* check memory leak in custom allocator */
	ASSERTeq(custom_allocs, 0);
	if (expect_custom_alloc == 0) {
		ASSERTeq(custom_alloc_calls, 0);
	} else {
		ASSERTne(custom_alloc_calls, 0);
	}

	DONE(NULL);
}
