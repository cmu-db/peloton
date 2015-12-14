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
 * vmmalloc_check_allocations -- unit test for
 * libvmmalloc check_allocations
 *
 * usage: vmmalloc_check_allocations
 */

#include "unittest.h"

#define	MIN_SIZE (sizeof (int))
#define	MAX_SIZE (4L * 1024L * 1024L)
#define	MAX_ALLOCS (VMEM_MIN_POOL / MIN_SIZE)

/* buffer for all allocations */
static void *allocs[MAX_ALLOCS];

int
main(int argc, char *argv[])
{
	int i, j;
	size_t size;

	START(argc, argv, "vmmalloc_check_allocations");

	for (size = MAX_SIZE; size >= MIN_SIZE; size /= 2) {
		OUT("size %zu", size);

		memset(allocs, 0, sizeof (allocs));

		for (i = 0; i < MAX_ALLOCS; ++i) {
			allocs[i] =  malloc(size);
			if (allocs[i] == NULL) {
				/* out of memory in pool */
				break;
			}

			/* fill each allocation with a unique value */
			memset(allocs[i], (char)i, size);
		}

		/* at least one allocation for each size must succeed */
		ASSERT(i > 0);

		/* check for unexpected modifications of the data */
		for (i = 0; i < MAX_ALLOCS && allocs[i] != NULL; ++i) {
			char *buffer = allocs[i];
			for (j = 0; j < size; ++j) {
				if (buffer[j] != (char)i)
					FATAL("Content of data object was "
						"modified unexpectedly for "
						"object size: %zu, id: %d",
						size, j);
			}
			free(allocs[i]);
		}
	}

	DONE(NULL);
}
