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
 * obj_list_macro.c -- unit tests for list module
 */

#include <stddef.h>

#include "libpmemobj.h"
#include "unittest.h"


TOID_DECLARE(struct item, 0);
TOID_DECLARE(struct list, 1);

struct item {
	int id;
	POBJ_LIST_ENTRY(struct item) next;
};

struct list {
	POBJ_LIST_HEAD(listhead, struct item) head;
};

/* global lists */
TOID(struct list) List;
TOID(struct list) List_sec;
#define	LAYOUT_NAME "list_macros"

/* usage macros */
#define	FATAL_USAGE()\
	FATAL("usage: obj_list_macro <file> [PRnifr]")
#define	FATAL_USAGE_PRINT()\
	FATAL("usage: obj_list_macro <file> P:<list>")
#define	FATAL_USAGE_PRINT_REVERSE()\
	FATAL("usage: obj_list_macro <file> R:<list>")
#define	FATAL_USAGE_INSERT()\
	FATAL("usage: obj_list_macro <file> i:<where>:<num>[:<id>]")
#define	FATAL_USAGE_INSERT_NEW()\
	FATAL("usage: obj_list_macro <file> n:<where>:<num>[:<id>]")
#define	FATAL_USAGE_REMOVE_FREE()\
	FATAL("usage: obj_list_macro <file> f:<list>:<num>")
#define	FATAL_USAGE_REMOVE()\
	FATAL("usage: obj_list_macro <file> r:<list>:<num>")
#define	FATAL_USAGE_MOVE()\
	FATAL("usage: obj_list_macro <file> m:<num>:<where>:<num>")

/*
 * get_item_list -- get nth item from list
 */
static TOID(struct item)
get_item_list(TOID(struct list) list, int n)
{
	TOID(struct item) item;
	if (n >= 0) {
		POBJ_LIST_FOREACH(item, &D_RO(list)->head, next) {
			if (n == 0)
				return item;
			n--;
		}
	} else {
		POBJ_LIST_FOREACH_REVERSE(item, &D_RO(list)->head, next) {
			n++;
			if (n == 0)
				return item;
		}
	}

	return TOID_NULL(struct item);
}

/*
 * do_print -- print list elements in normal order
 */
static void
do_print(PMEMobjpool *pop, const char *arg)
{
	int L;	/* which list */
	if (sscanf(arg, "P:%d", &L) != 1)
		FATAL_USAGE_PRINT();

	TOID(struct item) item;
	if (L == 1) {
		OUT("list:");
		POBJ_LIST_FOREACH(item, &D_RW(List)->head, next) {
			OUT("id = %d", D_RO(item)->id);
		}
	} else if (L == 2) {
		OUT("list sec:");
		POBJ_LIST_FOREACH(item, &D_RW(List_sec)->head, next) {
			OUT("id = %d", D_RO(item)->id);
		}
	} else {
		FATAL_USAGE_PRINT();
	}
}

/*
 * do_print_reverse -- print list elements in reverse order
 */
static void
do_print_reverse(PMEMobjpool *pop, const char *arg)
{
	int L;	/* which list */
	if (sscanf(arg, "R:%d", &L) != 1)
		FATAL_USAGE_PRINT_REVERSE();
	TOID(struct item) item;
	if (L == 1) {
		OUT("list reverse:");
		POBJ_LIST_FOREACH_REVERSE(item, &D_RW(List)->head, next) {
			OUT("id = %d", D_RO(item)->id);
		}
	} else if (L == 2) {
		OUT("list sec reverse:");
		POBJ_LIST_FOREACH_REVERSE(item, &D_RW(List_sec)->head, next) {
			OUT("id = %d", D_RO(item)->id);
		}
	} else {
		FATAL_USAGE_PRINT_REVERSE();
	}
}

/*
 * item_constructor -- constructor which sets the item's id to
 * new value
 */
static void
item_constructor(PMEMobjpool *pop, void *ptr, void *arg)
{
	int id = *(int *)arg;
	struct item *item = (struct item *)ptr;
	item->id = id;
	OUT("constructor(id = %d)", id);
}

/*
 * do_insert_new -- insert new element to list
 */
static void
do_insert_new(PMEMobjpool *pop, const char *arg)
{
	int n;		/* which element on List */
	int before;
	int id;
	int ret = sscanf(arg, "n:%d:%d:%d", &before, &n, &id);
	if (ret != 3 && ret != 2)
		FATAL_USAGE_INSERT_NEW();
	int ptr = (ret == 3) ? id : 0;
	TOID(struct item) item;
	if (POBJ_LIST_EMPTY(&D_RW(List)->head)) {
		POBJ_LIST_INSERT_NEW_HEAD(pop, &D_RW(List)->head, next,
				sizeof (struct item), item_constructor, &ptr);
		if (POBJ_LIST_EMPTY(&D_RW(List)->head))
			FATAL("POBJ_LIST_INSERT_NEW_HEAD");
	} else {
		item = get_item_list(List, n);
		ASSERT(!TOID_IS_NULL(item));
		if (!before) {
			POBJ_LIST_INSERT_NEW_AFTER(pop, &D_RW(List)->head,
					item, next, sizeof (struct item),
					item_constructor, &ptr);
			if (TOID_IS_NULL(POBJ_LIST_NEXT(item, next)))
				FATAL("POBJ_LIST_INSERT_NEW_AFTER");
		} else {
			POBJ_LIST_INSERT_NEW_BEFORE(pop, &D_RW(List)->head,
					item, next, sizeof (struct item),
					item_constructor, &ptr);
			if (TOID_IS_NULL(POBJ_LIST_PREV(item, next)))
				FATAL("POBJ_LIST_INSERT_NEW_BEFORE");
		}
	}
}

/*
 * do_insert -- insert element to list
 */
static void
do_insert(PMEMobjpool *pop, const char *arg)
{
	int n;		/* which element on List */
	int before;
	int id;
	int ret = sscanf(arg, "i:%d:%d:%d", &before, &n, &id);
	if (ret != 3 && ret != 2)
		FATAL_USAGE_INSERT();
	int ptr = (ret == 3) ? id : 0;

	TOID(struct item) item;
	POBJ_NEW(pop, &item, struct item, item_constructor, &ptr);
	ASSERT(!TOID_IS_NULL(item));
	if (POBJ_LIST_EMPTY(&D_RW(List)->head)) {
		POBJ_LIST_INSERT_HEAD(pop, &D_RW(List)->head,
						item, next);
		if (POBJ_LIST_EMPTY(&D_RW(List)->head))
			FATAL("POBJ_LIST_INSERT_HEAD");
	} else {
		TOID(struct item) elm = get_item_list(List, n);
		ASSERT(!TOID_IS_NULL(elm));
		if (!before) {
			POBJ_LIST_INSERT_AFTER(pop, &D_RW(List)->head,
							elm, item, next);
			if (!TOID_EQUALS(item, POBJ_LIST_NEXT(elm, next)))
				FATAL("POBJ_LIST_INSERT_AFTER");
		} else {
			POBJ_LIST_INSERT_BEFORE(pop, &D_RW(List)->head,
							elm, item, next);
			if (!TOID_EQUALS(item, POBJ_LIST_PREV(elm, next)))
				FATAL("POBJ_LIST_INSERT_BEFORE");
		}
	}
}

/*
 * do_remove_free -- remove and free element from list
 */
static void
do_remove_free(PMEMobjpool *pop, const char *arg)
{
	int L;	/* which list */
	int n;	/* which element */
	if (sscanf(arg, "f:%d:%d", &L, &n) != 2)
		FATAL_USAGE_REMOVE_FREE();

	TOID(struct item) item;
	TOID(struct list) tmp_list;
	if (L == 1)
		tmp_list = List;
	else if (L == 2)
		tmp_list = List_sec;
	else
		FATAL_USAGE_REMOVE_FREE();

	if (POBJ_LIST_EMPTY(&D_RW(tmp_list)->head))
		return;
	item = get_item_list(tmp_list, n);
	ASSERT(!TOID_IS_NULL(item));
	if (POBJ_LIST_REMOVE_FREE(pop, &D_RW(tmp_list)->head,
						item, next) != 0)
		FATAL("POBJ_LIST_REMOVE_FREE");
}

/*
 * do_remove -- remove element from list
 */
static void
do_remove(PMEMobjpool *pop, const char *arg)
{
	int L;	/* which list */
	int n;	/* which element */
	if (sscanf(arg, "r:%d:%d", &L, &n) != 2)
		FATAL_USAGE_REMOVE();

	TOID(struct item) item;
	TOID(struct list) tmp_list;
	if (L == 1)
		tmp_list = List;
	else if (L == 2)
		tmp_list = List_sec;
	else
		FATAL_USAGE_REMOVE_FREE();

	if (POBJ_LIST_EMPTY(&D_RW(tmp_list)->head))
		return;
	item = get_item_list(tmp_list, n);
	ASSERT(!TOID_IS_NULL(item));
	if (POBJ_LIST_REMOVE(pop, &D_RW(tmp_list)->head, item, next) != 0)
		FATAL("POBJ_LIST_REMOVE");
	POBJ_FREE(&item);
}

/*
 * do_move -- move element from one list to another
 */
static void
do_move(PMEMobjpool *pop, const char *arg)
{
	int n;
	int d;
	int before;
	if (sscanf(arg, "m:%d:%d:%d", &n, &before, &d) != 3)
		FATAL_USAGE_MOVE();

	if (POBJ_LIST_EMPTY(&D_RW(List)->head))
		return;
	if (POBJ_LIST_EMPTY(&D_RW(List_sec)->head)) {
		if (POBJ_LIST_MOVE_ELEMENT_HEAD(pop, &D_RW(List)->head,
				&D_RW(List_sec)->head,
				get_item_list(List, n),
				next, next) != 0) {
			FATAL("POBJ_LIST_MOVE_ELEMENT_HEAD");
		}
	} else {
		if (before) {
			if (POBJ_LIST_MOVE_ELEMENT_BEFORE(pop,
					&D_RW(List)->head,
					&D_RW(List_sec)->head,
					get_item_list(List_sec, d),
					get_item_list(List, n),
					next, next) != 0) {
				FATAL("POBJ_LIST_MOVE_ELEMENT_BEFORE");
			}
		} else {
			if (POBJ_LIST_MOVE_ELEMENT_AFTER(pop, &D_RW(List)->head,
					&D_RW(List_sec)->head,
					get_item_list(List_sec, d),
					get_item_list(List, n),
					next, next) != 0) {
				FATAL("POBJ_LIST_MOVE_ELEMENT_AFTER");
			}
		}
	}
}

/*
 * do_cleanup -- de-initialization function
 */
static void
do_cleanup(PMEMobjpool *pop, TOID(struct list) list)
{
	while (!POBJ_LIST_EMPTY(&D_RW(list)->head)) {
		TOID(struct item) tmp = POBJ_LIST_FIRST(&D_RW(list)->head);
		POBJ_LIST_REMOVE_FREE(pop, &D_RW(list)->head, tmp, next);
	}
	POBJ_FREE(&list);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_list_macro");
	if (argc < 2)
		FATAL_USAGE();

	const char *path = argv[1];
	PMEMobjpool *pop;
	if ((pop = pmemobj_create(path, LAYOUT_NAME, PMEMOBJ_MIN_POOL,
						S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create");

	POBJ_ZNEW(pop, &List, struct list);
	POBJ_ZNEW(pop, &List_sec, struct list);
	int i;
	for (i = 2; i < argc; i++) {
		switch (argv[i][0]) {
		case 'P':
			do_print(pop, argv[i]);
			break;
		case 'R':
			do_print_reverse(pop, argv[i]);
			break;
		case 'n':
			do_insert_new(pop, argv[i]);
			break;
		case 'i':
			do_insert(pop, argv[i]);
			break;
		case 'f':
			do_remove_free(pop, argv[i]);
			break;
		case 'r':
			do_remove(pop, argv[i]);
			break;
		case 'm':
			do_move(pop, argv[i]);
			break;
		default:
			FATAL_USAGE();
		}
	}
	do_cleanup(pop, List);
	do_cleanup(pop, List_sec);
	pmemobj_close(pop);

	DONE(NULL);
}
