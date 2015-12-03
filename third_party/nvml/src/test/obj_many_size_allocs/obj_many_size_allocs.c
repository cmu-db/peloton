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
 * obj_many_size_allocs.c -- allocation of many objects with different sizes
 *
 */

#include <stddef.h>

#include "unittest.h"

#define	LAYOUT_NAME "many_size_allocs"
#define	TEST_ALLOC_SIZE 2048

#define	LAZY_LOAD_SIZE 10
#define	LAZY_LOAD_BIG_SIZE 150

struct cargs {
	size_t size;
};

static void
test_constructor(PMEMobjpool *pop, void *addr, void *args)
{
	struct cargs *a = args;
	/* do not use pmem_memset_persit() here */
	pmemobj_memset_persist(pop, addr, a->size % 256, a->size);
}

static void
test_allocs(PMEMobjpool *pop, const char *path)
{
	PMEMoid oid[TEST_ALLOC_SIZE];

	if (pmemobj_alloc(pop, &oid[0], 0, 0, NULL, NULL) == 0)
		FATAL("pmemobj_alloc(0) succeeded");

	for (int i = 1; i < TEST_ALLOC_SIZE; ++i) {
		struct cargs args = { i };
		if (pmemobj_alloc(pop, &oid[i], i, 0,
				test_constructor, &args) != 0)
			FATAL("!pmemobj_alloc");
		ASSERT(!OID_IS_NULL(oid[i]));
	}

	pmemobj_close(pop);

	ASSERT(pmemobj_check(path, LAYOUT_NAME) == 1);

	ASSERT((pop = pmemobj_open(path, LAYOUT_NAME)) != NULL);

	for (int i = 1; i < TEST_ALLOC_SIZE; ++i) {
		pmemobj_free(&oid[i]);
		ASSERT(OID_IS_NULL(oid[i]));
	}

	pmemobj_close(pop);
}

static void
test_lazy_load(PMEMobjpool *pop, const char *path)
{
	PMEMoid oid[3];

	int ret = pmemobj_alloc(pop, &oid[0], LAZY_LOAD_SIZE, 0, NULL, NULL);
	ASSERTeq(ret, 0);
	ret = pmemobj_alloc(pop, &oid[1], LAZY_LOAD_SIZE, 0, NULL, NULL);
	ASSERTeq(ret, 0);
	ret = pmemobj_alloc(pop, &oid[2], LAZY_LOAD_SIZE, 0, NULL, NULL);
	ASSERTeq(ret, 0);

	pmemobj_close(pop);
	ASSERT((pop = pmemobj_open(path, LAYOUT_NAME)) != NULL);

	pmemobj_free(&oid[1]);

	ret = pmemobj_alloc(pop, &oid[1], LAZY_LOAD_BIG_SIZE, 0, NULL, NULL);
	ASSERTeq(ret, 0);
}


int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_many_size_allocs");

	if (argc != 2)
		FATAL("usage: %s file-name", argv[0]);

	const char *path = argv[1];

	PMEMobjpool *pop = NULL;

	if ((pop = pmemobj_create(path, LAYOUT_NAME,
			0, S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create: %s", path);

	test_lazy_load(pop, path);
	test_allocs(pop, path);

	DONE(NULL);
}
