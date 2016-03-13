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
 * obj_recreate.c -- recreate pool on dirty file and check consistency
 */

#include "unittest.h"

POBJ_LAYOUT_BEGIN(recreate);
POBJ_LAYOUT_ROOT(recreate, struct root);
POBJ_LAYOUT_TOID(recreate, struct foo);
POBJ_LAYOUT_END(recreate);

struct foo {
	int bar;
};

struct root {
	TOID(struct foo) foo;
};

#define	LAYOUT_NAME "obj_recreate"
#define	ZEROLEN 4096
#define	N PMEMOBJ_MIN_POOL

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_recreate");

	if (argc < 2)
		FATAL("usage: %s file-name [trunc]", argv[0]);

	const char *path = argv[1];

	PMEMobjpool *pop = NULL;

	/* create pool 2*N */
	pop = pmemobj_create(path, LAYOUT_NAME, 2 * N, S_IWUSR | S_IRUSR);
	if (pop == NULL)
		FATAL("!pmemobj_create: %s", path);

	/* allocate 1.5*N */
	TOID(struct root) root = (TOID(struct root))pmemobj_root(pop, 1.5 * N);

	/* use root object for something */
	POBJ_NEW(pop, &D_RW(root)->foo, struct foo, NULL, NULL);

	pmemobj_close(pop);

	int fd = OPEN(path, O_RDWR);

	if (argc >= 3 && strcmp(argv[2], "trunc") == 0) {
		OUT("truncating");
		/* shrink file to N */
		FTRUNCATE(fd, N);
	}

	/* zero first 4kB */
	void *p = MMAP(NULL, ZEROLEN, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	memset(p, 0, ZEROLEN);
	MUNMAP(p, ZEROLEN);
	CLOSE(fd);

	/* create pool on existing file */
	pop = pmemobj_create(path, LAYOUT_NAME, 0, S_IWUSR | S_IRUSR);
	if (pop == NULL)
		FATAL("!pmemobj_create: %s", path);

	/* try to allocate 0.7*N */
	root = (TOID(struct root))pmemobj_root(pop, 0.5 * N);

	if (TOID_IS_NULL(root))
		FATAL("couldn't allocate root object");

	/* validate root object is empty */
	if (!TOID_IS_NULL(D_RW(root)->foo))
		FATAL("root object is already filled after pmemobj_create!");

	pmemobj_close(pop);

	DONE(NULL);
}
