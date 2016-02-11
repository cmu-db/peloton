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
 * obj_debug.c -- unit test for debug features
 *
 * usage: obj_debug file operation:...
 *
 * operations are 'f' or 'l' or 'r' or 'a'
 *
 */
#include <stddef.h>
#include <sys/param.h>

#include "unittest.h"
#include "libpmemobj.h"

#define	LAYOUT_NAME "layout_obj_debug"

TOID_DECLARE_ROOT(struct root);
TOID_DECLARE(struct tobj, 0);
TOID_DECLARE(struct int3_s, 1);

struct root {
	POBJ_LIST_HEAD(listhead, struct tobj) lhead, lhead2;
	uint32_t val;
};

struct tobj {
	POBJ_LIST_ENTRY(struct tobj) next;
};

struct int3_s {
	uint32_t i1;
	uint32_t i2;
	uint32_t i3;
};

static void
test_FOREACH(const char *path)
{
	PMEMobjpool *pop = NULL;
	PMEMoid varoid, nvaroid;
	TOID(struct root) root;
	TOID(struct tobj) var, nvar;
	int type = 0;

#define	COMMANDS_FOREACH()\
	do {\
	POBJ_FOREACH(pop, varoid, type) {}\
	POBJ_FOREACH_SAFE(pop, varoid, nvaroid, type) {}\
	POBJ_FOREACH_TYPE(pop, var) {}\
	POBJ_FOREACH_SAFE_TYPE(pop, var, nvar) {}\
	POBJ_LIST_FOREACH(var, &D_RW(root)->lhead, next) {}\
	POBJ_LIST_FOREACH_REVERSE(var, &D_RW(root)->lhead, next) {}\
	} while (0)

	if ((pop = pmemobj_create(path, LAYOUT_NAME,
			PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create: %s", path);

	TOID_ASSIGN(root, pmemobj_root(pop, sizeof (struct root)));
	POBJ_LIST_INSERT_NEW_HEAD(pop, &D_RW(root)->lhead, next,
			sizeof (struct tobj), NULL, NULL);

	COMMANDS_FOREACH();
	TX_BEGIN(pop) {
		COMMANDS_FOREACH();
	} TX_ONABORT {
		ASSERT(0);
	} TX_END
	COMMANDS_FOREACH();

	pmemobj_close(pop);
}

static void
test_lists(const char *path)
{
	PMEMobjpool *pop = NULL;
	TOID(struct root) root;
	TOID(struct tobj) elm;

#define	COMMANDS_LISTS()\
	do {\
	POBJ_LIST_INSERT_NEW_HEAD(pop, &D_RW(root)->lhead, next,\
			sizeof (struct tobj), NULL, NULL);\
	POBJ_NEW(pop, &elm, struct tobj, NULL, NULL);\
	POBJ_LIST_INSERT_AFTER(pop, &D_RW(root)->lhead,\
			POBJ_LIST_FIRST(&D_RW(root)->lhead), elm, next);\
	POBJ_LIST_MOVE_ELEMENT_HEAD(pop, &D_RW(root)->lhead,\
			&D_RW(root)->lhead2, elm, next, next);\
	POBJ_LIST_REMOVE(pop, &D_RW(root)->lhead2, elm, next);\
	POBJ_FREE(&elm);\
	} while (0)

	if ((pop = pmemobj_create(path, LAYOUT_NAME,
			PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create: %s", path);

	TOID_ASSIGN(root, pmemobj_root(pop, sizeof (struct root)));

	COMMANDS_LISTS();
	TX_BEGIN(pop) {
		COMMANDS_LISTS();
	} TX_ONABORT {
		ASSERT(0);
	} TX_END
	COMMANDS_LISTS();

	pmemobj_close(pop);
}

static void
int3_constructor(PMEMobjpool *pop, void *ptr, void *arg)
{
	struct int3_s *args = arg;
	struct int3_s *val = ptr;

	val->i1 = args->i1;
	val->i2 = args->i2;
	val->i3 = args->i3;

	pmemobj_persist(pop, val, sizeof (*val));
}

static void
test_alloc_construct(const char *path)
{
	PMEMobjpool *pop = NULL;

	if ((pop = pmemobj_create(path, LAYOUT_NAME,
			PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create: %s", path);

	TX_BEGIN(pop) {
		struct int3_s args = { 1, 2, 3 };
		PMEMoid allocation;
		pmemobj_alloc(pop, &allocation, sizeof (allocation), 1,
				int3_constructor, &args);
	} TX_ONABORT {
		ASSERT(0);
	} TX_END

	pmemobj_close(pop);
}

static void
test_double_free(const char *path)
{
	PMEMobjpool *pop = NULL;

	if ((pop = pmemobj_create(path, LAYOUT_NAME,
			PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create: %s", path);

	PMEMoid oid, oid2;
	int err = pmemobj_zalloc(pop, &oid, 100, 0);
	ASSERTeq(err, 0);
	ASSERT(!OID_IS_NULL(oid));

	oid2 = oid;

	pmemobj_free(&oid);
	pmemobj_free(&oid2);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_debug");

	if (argc != 3)
		FATAL("usage: %s file-name op:f|l|r|a", argv[0]);

	const char *path = argv[1];

	if (strchr("flrap", argv[2][0]) == NULL || argv[2][1] != '\0')
		FATAL("op must be f or l or r or a or p");

	switch (argv[2][0]) {
		case 'f':
			test_FOREACH(path);
			break;
		case 'l':
			test_lists(path);
			break;
		case 'a':
			test_alloc_construct(path);
			break;
		case 'p':
			test_double_free(path);
			break;
	}

	DONE(NULL);
}
