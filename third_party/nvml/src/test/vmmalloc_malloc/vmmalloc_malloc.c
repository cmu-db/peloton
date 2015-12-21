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
 * vmmalloc_malloc.c -- unit test for libvmmalloc malloc
 *
 * usage: vmmalloc_malloc
 */

#include "unittest.h"

#define	MIN_SIZE (sizeof (int))
#define	SIZE 20
#define	MAX_SIZE (MIN_SIZE << SIZE)

int
main(int argc, char *argv[])
{
	const int test_value = 12345;
	size_t size;
	int *ptr[SIZE];
	int i = 0;
	size_t sum_alloc = 0;

	START(argc, argv, "vmmalloc_malloc");

	/* test with multiple size of allocations from 4MB to sizeof (int) */
	for (size = MAX_SIZE; size > MIN_SIZE; size /= 2) {
		ptr[i] = malloc(size);

		if (ptr[i] == NULL)
			continue;

		*ptr[i] = test_value;
		ASSERTeq(*ptr[i], test_value);

		sum_alloc += size;
		i++;
	}

	/* at least one allocation for each size must succeed */
	ASSERTeq(size, MIN_SIZE);

	/* allocate more than half of pool size */
	ASSERT(sum_alloc * 2 > VMEM_MIN_POOL);

	while (i > 0)
		free(ptr[--i]);

	DONE(NULL);
}
