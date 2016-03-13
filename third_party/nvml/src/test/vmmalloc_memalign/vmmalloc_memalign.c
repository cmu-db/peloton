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
 * vmmalloc_memalign.c -- unit test for libvmmalloc memalign, posix_memalign
 * and aligned_alloc (if available)
 *
 * usage: vmmalloc_memalign [m|p|a]
 */

#include <stdlib.h>
#include <malloc.h>
#include <errno.h>
#include "unittest.h"

#define	USAGE "usage: %s [m|p|a]"

#define	MIN_ALIGN (2)
#define	MAX_ALIGN (4L * 1024L * 1024L)
#define	MAX_ALLOCS (100)

extern void *aligned_alloc(size_t alignment, size_t size);

/* buffer for all allocations */
static int *allocs[MAX_ALLOCS];

static void *(*Aalloc)(size_t alignment, size_t size);

static void *
posix_memalign_wrap(size_t alignment, size_t size)
{
	void *ptr;
	int err = posix_memalign(&ptr, alignment, size);
	/* ignore OOM */
	if (err) {
		ptr = NULL;
		if (err != ENOMEM)
			OUT("posix_memalign: %s", strerror(err));
	}
	return ptr;
}

int
main(int argc, char *argv[])
{
	const int test_value = 123456;
	size_t alignment;
	int i;

	START(argc, argv, "vmmalloc_memalign");

	if (argc != 2)
		FATAL(USAGE, argv[0]);

	switch (argv[1][0]) {
	case 'm':
		OUT("testing memalign");
		Aalloc = memalign;
		break;
	case 'p':
		OUT("testing posix_memalign");
		Aalloc = posix_memalign_wrap;
		break;
	case 'a':
		OUT("testing aligned_alloc");
		Aalloc = aligned_alloc;
		break;
	default:
		FATAL(USAGE, argv[0]);
	}

	/* test with address alignment from 2B to 4MB */
	for (alignment = MAX_ALIGN; alignment >= MIN_ALIGN; alignment /= 2) {
		OUT("alignment %zu", alignment);

		memset(allocs, 0, sizeof (allocs));

		for (i = 0; i < MAX_ALLOCS; ++i) {
			allocs[i] = Aalloc(alignment, sizeof (int));

			if (allocs[i] == NULL)
				break;

			/* ptr should be usable */
			*allocs[i] = test_value;
			ASSERTeq(*allocs[i], test_value);

			/* check for correct address alignment */
			ASSERTeq((uintptr_t)(allocs[i]) & (alignment - 1), 0);
		}

		/* at least one allocation must succeed */
		ASSERT(i > 0);

		for (i = 0; i < MAX_ALLOCS && allocs[i] != NULL; ++i)
			free(allocs[i]);
	}

	DONE(NULL);
}
