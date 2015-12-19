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

#include "unittest.h"

#if defined(USE_VG_MEMCHECK) || defined(USE_VALGRIND)
#include <valgrind/valgrind.h>
#include <valgrind/memcheck.h>
#endif

/*
 * Layout definition
 */
POBJ_LAYOUT_BEGIN(mc);
POBJ_LAYOUT_ROOT(mc, struct root);
POBJ_LAYOUT_TOID(mc, struct struct1);
POBJ_LAYOUT_END(mc);

struct struct1 {
	int fld;
	int dyn[];
};

struct root {
	TOID(struct struct1) s1;
	TOID(struct struct1) s2;
};

static void
test_memcheck_bug()
{
#if defined(USE_VG_MEMCHECK) || defined(USE_VALGRIND)
	volatile char tmp[100];

	VALGRIND_CREATE_MEMPOOL(tmp, 0, 0);
	VALGRIND_MEMPOOL_ALLOC(tmp, tmp + 8, 16);
	VALGRIND_MEMPOOL_FREE(tmp, tmp + 8);
	VALGRIND_MEMPOOL_ALLOC(tmp, tmp + 8, 16);
	VALGRIND_MAKE_MEM_NOACCESS(tmp, 8);
	tmp[7] = 0x66;
#endif
}

static void
test_everything(const char *path, int overwrite_oob)
{
	PMEMobjpool *pop = NULL;

	if ((pop = pmemobj_create(path, POBJ_LAYOUT_NAME(mc),
			PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create: %s", path);

	struct root *rt = D_RW(POBJ_ROOT(pop, struct root));

	POBJ_ALLOC(pop, &rt->s1, struct struct1, sizeof (struct struct1),
			NULL, NULL);
	struct struct1 *s1 = D_RW(rt->s1);
	struct struct1 *s2;

	POBJ_ALLOC(pop, &rt->s2, struct struct1, sizeof (struct struct1),
			NULL, NULL);
	s2 = D_RW(rt->s2);
	POBJ_FREE(&rt->s2);

	/* read of uninitialized variable */
	if (s1->fld)
		printf("%d\n", 1);

	/* write to freed object */
	s2->fld = 7;

	pmemobj_persist(pop, s2, sizeof (*s2));

	POBJ_ALLOC(pop, &rt->s2, struct struct1, sizeof (struct struct1),
			NULL, NULL);
	s2 = D_RW(rt->s2);
	memset(s2, 0, pmemobj_alloc_usable_size(rt->s2.oid));
	s2->fld = 12; /* ok */

	if (overwrite_oob) {
		/* overwrite padding from oob_header */
		char *t = (void *)s2;
		t[-1] = 0x66;
	}

	/* invalid write */
	s2->dyn[100000] = 9;

	/* invalid write */
	s2->dyn[1000] = 9;

	pmemobj_persist(pop, s2, sizeof (struct struct1));

	POBJ_REALLOC(pop, &rt->s2, struct struct1,
			sizeof (struct struct1) + 100 * sizeof (int));
	s2 = D_RW(rt->s2);
	s2->dyn[0] = 9; /* ok */
	pmemobj_persist(pop, s2, sizeof (struct struct1) + 100 * sizeof (int));

	POBJ_FREE(&rt->s2);
	/* invalid write to REALLOCated and FREEd object */
	s2->dyn[0] = 9;
	pmemobj_persist(pop, s2, sizeof (struct struct1) + 100 * sizeof (int));



	POBJ_ALLOC(pop, &rt->s2, struct struct1, sizeof (struct struct1),
			NULL, NULL);
	POBJ_REALLOC(pop, &rt->s2, struct struct1,
			sizeof (struct struct1) + 30 * sizeof (int));
	s2 = D_RW(rt->s2);
	s2->dyn[0] = 0;
	s2->dyn[29] = 29;
	pmemobj_persist(pop, s2, sizeof (struct struct1) + 30 * sizeof (int));
	POBJ_FREE(&rt->s2);

	s2->dyn[0] = 9;
	pmemobj_persist(pop, s2, sizeof (struct struct1) + 30 * sizeof (int));

	pmemobj_close(pop);
}

static void usage(const char *a)
{
	FATAL("usage: %s [m|t0|t1] file-name", a);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_memcheck");

	if (argc < 2)
		usage(argv[0]);

	if (strcmp(argv[1], "m") == 0)
		test_memcheck_bug();
	else if (strcmp(argv[1], "t0") == 0) {
		if (argc < 3)
			usage(argv[0]);
		test_everything(argv[2], 0);
	} else if (strcmp(argv[1], "t1") == 0) {
		if (argc < 3)
			usage(argv[0]);
		test_everything(argv[2], 1);
	} else
		usage(argv[0]);

	DONE(NULL);
}
