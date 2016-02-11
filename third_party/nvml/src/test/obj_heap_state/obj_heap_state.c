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
 * obj_heap_state.c -- volatile heap state verification test
 */

#include <stddef.h>

#include "unittest.h"

#define	LAYOUT_NAME "heap_state"
#define	ROOT_SIZE 256
#define	ALLOCS 100
#define	ALLOC_SIZE 50

char buf[ALLOC_SIZE];

static void
test_constructor(PMEMobjpool *pop, void *addr, void *args)
{
	/* do not use pmem_memcpy_persist() here */
	pmemobj_memcpy_persist(pop, addr, buf, ALLOC_SIZE);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_heap_state");

	if (argc != 2)
		FATAL("usage: %s file-name", argv[0]);

	const char *path = argv[1];

	for (int i = 0; i < ALLOC_SIZE; i++)
		buf[i] = rand() % 256;

	PMEMobjpool *pop = NULL;

	if ((pop = pmemobj_create(path, LAYOUT_NAME,
			0, S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create: %s", path);

	pmemobj_root(pop, ROOT_SIZE); /* just to trigger allocation */

	pmemobj_close(pop);

	pop = pmemobj_open(path, LAYOUT_NAME);
	ASSERTne(pop, NULL);

	for (int i = 0; i < ALLOCS; ++i) {
		PMEMoid oid;
		pmemobj_alloc(pop, &oid, ALLOC_SIZE, 0,
				test_constructor, NULL);
		OUT("%d %lu", i, oid.off);
	}

	pmemobj_close(pop);

	DONE(NULL);
}
