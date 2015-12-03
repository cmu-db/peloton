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
 * vmem_mix_allocations.c -- unit test for vmem_mix_allocations
 *
 * usage: vmem_mix_allocations [directory]
 */

#include "unittest.h"

#define	COUNT 21
#define	POOL_SIZE VMEM_MIN_POOL
#define	MAX_SIZE (4 << COUNT)

int
main(int argc, char *argv[])
{
	char *dir = NULL;
	void *mem_pool = NULL;
	VMEM *vmp;
	size_t obj_size;
	int *ptr[COUNT];
	int i = 0;
	size_t sum_alloc = 0;

	START(argc, argv, "vmem_mix_allocations");

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

	obj_size = MAX_SIZE;
	/* test with multiple size of allocations from 4MB to 2B */
	for (i = 0; i < COUNT; ++i, obj_size /= 2) {
		ptr[i] = vmem_malloc(vmp, obj_size);

		if (ptr[i] == NULL)
			continue;

		sum_alloc += obj_size;

		/* check that pointer came from mem_pool */
		if (dir == NULL)
			ASSERTrange(ptr[i], mem_pool, POOL_SIZE);
	}

	/* allocate more than half of pool size */
	ASSERT(sum_alloc * 2 > POOL_SIZE);

	while (i > 0)
		vmem_free(vmp, ptr[--i]);

	vmem_delete(vmp);

	DONE(NULL);
}
