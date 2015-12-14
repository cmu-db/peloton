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
 * vmmalloc_valloc.c -- unit test for libvmmalloc valloc/pvalloc
 *
 * usage: vmmalloc_valloc [v|p]
 */

#include <sys/param.h>
#include <libvmmalloc.h>
#include "unittest.h"

static void *(*Valloc)(size_t size);

int
main(int argc, char *argv[])
{
	const int test_value = 123456;
	size_t pagesize = sysconf(_SC_PAGESIZE);
	size_t min_size = sizeof (int);
	size_t max_size = 4 * pagesize;
	size_t size;
	int *ptr;

	START(argc, argv, "vmmalloc_valloc");

	if (argc != 2)
		FATAL("usage: %s [v|p]", argv[0]);

	switch (argv[1][0]) {
	case 'v':
		OUT("testing valloc");
		Valloc = valloc;
		break;
	case 'p':
		OUT("testing pvalloc");
		Valloc = pvalloc;
		break;
	default:
		FATAL("usage: %s [v|p]", argv[0]);
	}

	for (size = min_size; size < max_size; size *= 2) {
		ptr = Valloc(size);

		/* at least one allocation must succeed */
		ASSERT(ptr != NULL);
		if (ptr == NULL)
			break;

		/* ptr should be usable */
		*ptr = test_value;
		ASSERTeq(*ptr, test_value);

		/* check for correct address alignment */
		ASSERTeq((uintptr_t)(ptr) & (pagesize - 1), 0);

		if (Valloc == pvalloc) {
			/* check for correct allocation size */
			size_t usable = malloc_usable_size(ptr);
			ASSERTeq(usable, roundup(size, pagesize));
		}

		free(ptr);
	}

	DONE(NULL);
}
