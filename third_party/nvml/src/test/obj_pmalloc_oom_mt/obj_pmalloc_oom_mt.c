/*
 * Copyright (c) 2015, Intel Corporation
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
 * obj_pmalloc_oom_mt.c -- build multithreaded out of memory test
 *
 */

#include <stddef.h>

#include "unittest.h"

#define	TEST_ALLOC_SIZE (131072 - 64) /* last unit size */
#define	LAYOUT_NAME "oom_mt"

int allocated;
PMEMobjpool *pop;

static void *
oom_worker(void *arg)
{
	allocated = 0;
	while (pmemobj_alloc(pop, NULL, TEST_ALLOC_SIZE, 0, NULL, NULL) == 0)
		allocated++;

	PMEMoid iter, iter2;
	int type;
	POBJ_FOREACH_SAFE(pop, iter, iter2, type)
		pmemobj_free(&iter);

	return NULL;
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_pmalloc_oom_mt");

	if (argc != 2)
		FATAL("usage: %s file-name", argv[0]);

	const char *path = argv[1];

	if ((pop = pmemobj_create(path, LAYOUT_NAME,
			PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create: %s", path);

	pthread_t t;
	pthread_create(&t, NULL, oom_worker, NULL);
	pthread_join(t, NULL);

	int first_thread_allocated = allocated;

	pthread_create(&t, NULL, oom_worker, NULL);
	pthread_join(t, NULL);

	ASSERTeq(first_thread_allocated, allocated);

	pmemobj_close(pop);

	DONE(NULL);
}
