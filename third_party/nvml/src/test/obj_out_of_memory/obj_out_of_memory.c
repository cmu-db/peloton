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
 * obj_out_of_memory.c -- allocate objects until OOM
 */

#include <stdlib.h>
#include "unittest.h"

#define	LAYOUT_NAME "out_of_memory"

struct cargs {
	size_t size;
};

static void
test_constructor(PMEMobjpool *pop, void *addr, void *args)
{
	struct cargs *a = args;
	pmemobj_memset_persist(pop, addr, rand() % 256, a->size / 2);
}

static void
test_alloc(PMEMobjpool *pop, size_t size)
{
	unsigned long cnt = 0;

	while (1) {
		struct cargs args = { size };
		if (pmemobj_alloc(pop, NULL, size, 0,
				test_constructor, &args) != 0)
			break;
		cnt++;
	}

	OUT("size: %zu allocs: %lu", size, cnt);
}

static void
test_free(PMEMobjpool *pop)
{
	PMEMoid oid;
	PMEMoid next;
	int type_num;

	POBJ_FOREACH_SAFE(pop, oid, next, type_num)
		pmemobj_free(&oid);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_out_of_memory");

	if (argc < 3)
		FATAL("usage: %s size filename ...", argv[0]);

	size_t size = atoll(argv[1]);

	for (int i = 2; i < argc; i++) {
		const char *path = argv[i];

		PMEMobjpool *pop = pmemobj_create(path, LAYOUT_NAME, 0,
					S_IWUSR | S_IRUSR);
		if (pop == NULL)
			FATAL("!pmemobj_create: %s", path);

		test_alloc(pop, size);

		pmemobj_close(pop);

		ASSERTeq(pmemobj_check(path, LAYOUT_NAME), 1);

		ASSERTne(pop = pmemobj_open(path, LAYOUT_NAME), NULL);

		test_free(pop);

		pmemobj_close(pop);
	}

	DONE(NULL);
}
