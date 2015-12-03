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
 * vmem_check_allocations -- unit test for vmem_check_allocations
 *
 * usage: vmem_check_allocations [directory]
 */

#include "unittest.h"

#define	TEST_MAX_ALLOCATION_SIZE (4L * 1024L * 1024L)
#define	TEST_ALLOCS_SIZE (VMEM_MIN_POOL / 8)

/* buffer for all allocations */
static void *allocs[TEST_ALLOCS_SIZE];

int
main(int argc, char *argv[])
{
	char *dir = NULL;
	void *mem_pool = NULL;
	VMEM *vmp;

	START(argc, argv, "vmem_check_allocations");

	if (argc == 2) {
		dir = argv[1];
	} else if (argc > 2) {
		FATAL("usage: %s [directory]", argv[0]);
	}

	size_t object_size;
	for (object_size = 8; object_size <= TEST_MAX_ALLOCATION_SIZE;
							object_size *= 2) {
		size_t i;
		size_t j;

		if (dir == NULL) {
			mem_pool = MMAP_ANON_ALIGNED(VMEM_MIN_POOL, 4 << 20);

			vmp = vmem_create_in_region(mem_pool,
				VMEM_MIN_POOL);
			if (vmp == NULL)
				FATAL("!vmem_create_in_region");
		} else {
			vmp = vmem_create(dir, VMEM_MIN_POOL);
			if (vmp == NULL)
				FATAL("!vmem_create");
		}

		memset(allocs, 0, sizeof (allocs));

		for (i = 0; i < TEST_ALLOCS_SIZE; ++i) {
			allocs[i] =  vmem_malloc(vmp, object_size);
			if (allocs[i] == NULL) {
				/* out of memory in pool */
				break;
			}

			/* check that pointer came from mem_pool */
			if (dir == NULL) {
				ASSERTrange(allocs[i],
					mem_pool, VMEM_MIN_POOL);
			}

			/* fill each allocation with a unique value */
			memset(allocs[i], (char)i, object_size);
		}

		ASSERT((i > 0) && (i + 1 < TEST_MAX_ALLOCATION_SIZE));

		/* check for unexpected modifications of the data */
		for (i = 0; i < TEST_ALLOCS_SIZE && allocs[i] != NULL; ++i) {
			char *buffer = allocs[i];
			for (j = 0; j < object_size; ++j) {
				if (buffer[j] != (char)i)
					FATAL("Content of data object was "
						"modified unexpectedly for "
						"object size: %zu, id: %zu",
						object_size, j);
			}
		}

		for (i = 0; i < TEST_ALLOCS_SIZE && allocs[i] != NULL; ++i)
			vmem_free(vmp, allocs[i]);

		vmem_delete(vmp);
	}

	DONE(NULL);
}
