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
 * vmem_malloc_usable_size.c -- unit test for vmem_malloc_usable_size
 *
 * usage: vmem_malloc_usable_size [directory]
 */

#include "unittest.h"

#define	POOL_SIZE (VMEM_MIN_POOL * 2)

static const struct {
	size_t size;
	size_t spacing;
} Check_sizes[] = {
		{.size = 10, .spacing = 8},
		{.size = 100, .spacing = 16},
		{.size = 200, .spacing = 32},
		{.size = 500, .spacing = 64},
		{.size = 1000, .spacing = 128},
		{.size = 2000, .spacing = 256},
		{.size = 3000, .spacing = 512},
		{.size = 1 * 1024 * 1024, .spacing = 4 * 1024 * 1024},
		{.size = 2 * 1024 * 1024, .spacing = 4 * 1024 * 1024},
		{.size = 3 * 1024 * 1024, .spacing = 4 * 1024 * 1024},
		{.size = 4 * 1024 * 1024, .spacing = 4 * 1024 * 1024},
		{.size = 5 * 1024 * 1024, .spacing = 4 * 1024 * 1024},
		{.size = 6 * 1024 * 1024, .spacing = 4 * 1024 * 1024},
		{.size = 7 * 1024 * 1024, .spacing = 4 * 1024 * 1024},
		{.size = 8 * 1024 * 1024, .spacing = 4 * 1024 * 1024},
		{.size = 9 * 1024 * 1024, .spacing = 4 * 1024 * 1024}
	};

int
main(int argc, char *argv[])
{
	char *dir = NULL;
	void *mem_pool = NULL;
	VMEM *vmp;
	void *alloc;
	size_t usable_size;
	size_t size;
	unsigned i;

	START(argc, argv, "vmem_malloc_usable_size");

	if (argc == 2) {
		dir = argv[1];
	} else if (argc > 2) {
		FATAL("usage: %s [directory]", argv[0]);
	}

	if (dir == NULL) {
		/* allocate memory for function vmem_create_in_region() */
		mem_pool = MMAP_ANON_ALIGNED(POOL_SIZE, 4 << 20);

		vmp = vmem_create_in_region(mem_pool, POOL_SIZE);
		if (vmp == NULL)
			FATAL("!vmem_create_in_region");
	} else {
		vmp = vmem_create(dir, POOL_SIZE);
		if (vmp == NULL)
			FATAL("!vmem_create");
	}

	ASSERTeq(vmem_malloc_usable_size(vmp, NULL), 0);

	for (i = 0; i < (sizeof (Check_sizes) / sizeof (Check_sizes[0])); ++i) {
		size = Check_sizes[i].size;
		alloc = vmem_malloc(vmp, size);
		ASSERTne(alloc, NULL);
		usable_size = vmem_malloc_usable_size(vmp, alloc);
		ASSERT(usable_size >= size);
		if (usable_size - size > Check_sizes[i].spacing) {
			FATAL("Size %zu: spacing %zu is bigger"
				"than expected: %zu", size,
				(usable_size - size), Check_sizes[i].spacing);
		}
		memset(alloc, 0xEE, usable_size);
		vmem_free(vmp, alloc);
	}

	ASSERTeq(vmem_check(vmp), 1);

	vmem_delete(vmp);

	DONE(NULL);
}
