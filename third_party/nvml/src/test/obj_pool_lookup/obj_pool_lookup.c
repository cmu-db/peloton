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
 * obj_pool_lookup.c -- unit test for pmemobj_pool and pmemobj_pool_of
 */
#include "unittest.h"
#include "util.h"

#define	MAX_PATH_LEN 255
#define	LAYOUT_NAME "pool_lookup"

#define	ALLOC_SIZE 100

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_direct");

	if (argc != 3)
		FATAL("usage: %s [directory] [# of pools]", argv[0]);

	int npools = atoi(argv[2]);
	const char *dir = argv[1];
	int r;

	PMEMobjpool *pops[npools];
	void *guard_after[npools];

	char path[MAX_PATH_LEN];
	for (int i = 0; i < npools; ++i) {
		snprintf(path, MAX_PATH_LEN, "%s/testfile%d", dir, i);
		pops[i] = pmemobj_create(path, LAYOUT_NAME, PMEMOBJ_MIN_POOL,
				S_IWUSR | S_IRUSR);

		/*
		 * Reserve a page after the pool for address checks, if it
		 * doesn't map precisely at that address - it's OK.
		 */
		guard_after[i] =
			MMAP((char *)pops[i] + PMEMOBJ_MIN_POOL, Ut_pagesize,
				PROT_READ | PROT_WRITE,
				MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);

		ASSERTne(guard_after[i], NULL);

		if (pops[i] == NULL)
			FATAL("!pmemobj_create");
	}

	PMEMoid oids[npools];

	for (int i = 0; i < npools; ++i) {
		r = pmemobj_alloc(pops[i], &oids[i], ALLOC_SIZE, 1, NULL, NULL);
		ASSERTeq(r, 0);
	}

	PMEMoid invalid = {123, 321};

	ASSERTeq(pmemobj_pool_by_oid(OID_NULL), NULL);
	ASSERTeq(pmemobj_pool_by_oid(invalid), NULL);

	for (int i = 0; i < npools; ++i) {
		ASSERTeq(pmemobj_pool_by_oid(oids[i]), pops[i]);
	}

	ASSERTeq(pmemobj_pool_by_ptr(NULL), NULL);
	ASSERTeq(pmemobj_pool_by_ptr((void *)0xCBA), NULL);

	for (int i = 0; i < npools; ++i) {
		void *before_pool = (char *)pops[i] - 1;
		void *after_pool = (char *)pops[i] + PMEMOBJ_MIN_POOL + 1;
		void *edge = (char *)pops[i] + PMEMOBJ_MIN_POOL;
		void *middle = (char *)pops[i] + (PMEMOBJ_MIN_POOL / 2);
		void *in_oid = (char *)pmemobj_direct(oids[i]) +
			(ALLOC_SIZE / 2);
		ASSERTeq(pmemobj_pool_by_ptr(before_pool), NULL);
		ASSERTeq(pmemobj_pool_by_ptr(after_pool), NULL);
		ASSERTeq(pmemobj_pool_by_ptr(edge), NULL);
		ASSERTeq(pmemobj_pool_by_ptr(middle), pops[i]);
		ASSERTeq(pmemobj_pool_by_ptr(in_oid), pops[i]);
		pmemobj_close(pops[i]);
		ASSERTeq(pmemobj_pool_by_ptr(middle), NULL);
		ASSERTeq(pmemobj_pool_by_ptr(in_oid), NULL);

		MUNMAP(guard_after[i], Ut_pagesize);
	}

	DONE(NULL);
}
