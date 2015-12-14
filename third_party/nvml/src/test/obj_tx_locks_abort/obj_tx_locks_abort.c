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
 * obj_tx_locks_nested.c -- unit test for transaction locks
 */
#include "unittest.h"
#include "libpmemobj.h"
#include "util.h"

#define	LAYOUT_NAME "locks"

TOID_DECLARE_ROOT(struct root_obj);
TOID_DECLARE(struct obj, 1);

struct root_obj {
	PMEMmutex lock;
	TOID(struct obj) head;
};

struct obj {
	int data;
	PMEMmutex lock;
	TOID(struct obj) next;
};

/*
 * do_nested_tx-- (internal) nested transaction
 */
static void
do_nested_tx(PMEMobjpool *pop, TOID(struct obj) o, int value)
{
	TX_BEGIN_LOCK(pop, TX_LOCK_MUTEX, &D_RW(o)->lock, TX_LOCK_NONE) {
		TX_ADD(o);
		D_RW(o)->data = value;
		if (!TOID_IS_NULL(D_RO(o)->next)) {
			/*
			 * Add the object to undo log, while the mutex
			 * it contains is not locked.
			 */
			TX_ADD(D_RO(o)->next);
			do_nested_tx(pop, D_RO(o)->next, value);
		}
	} TX_END;
}

/*
 * do_aborted_nested_tx -- (internal) aborted nested transaction
 */
static void
do_aborted_nested_tx(PMEMobjpool *pop, TOID(struct obj) oid, int value)
{
	TOID(struct obj) o = oid;

	TX_BEGIN_LOCK(pop, TX_LOCK_MUTEX, &D_RW(o)->lock, TX_LOCK_NONE) {
		TX_ADD(o);
		D_RW(o)->data = value;
		if (!TOID_IS_NULL(D_RO(o)->next)) {
			/*
			 * Add the object to undo log, while the mutex
			 * it contains is not locked.
			 */
			TX_ADD(D_RO(o)->next);
			do_nested_tx(pop, D_RO(o)->next, value);
		}
		pmemobj_tx_abort(EINVAL);
	} TX_FINALLY {
		o = oid;

		while (!TOID_IS_NULL(o)) {
			if (pmemobj_mutex_trylock(pop, &D_RW(o)->lock)) {
				OUT("trylock failed");
			} else {
				OUT("trylock succeeded");
				pmemobj_mutex_unlock(pop, &D_RW(o)->lock);
			}
			o = D_RO(o)->next;
		}
	} TX_END;
}

/*
 * do_check -- (internal) print 'data' value of each object on the list
 */
static void
do_check(TOID(struct obj) o)
{
	while (!TOID_IS_NULL(o)) {
		OUT("data = %d", D_RO(o)->data);
		o = D_RO(o)->next;
	}
}

int
main(int argc, char *argv[])
{
	PMEMobjpool *pop;

	START(argc, argv, "obj_tx_locks_abort");

	if (argc > 3)
		FATAL("usage: %s <file>", argv[0]);

	pop = pmemobj_create(argv[1], LAYOUT_NAME,
			PMEMOBJ_MIN_POOL * 4, S_IWUSR | S_IRUSR);
	if (pop == NULL)
		FATAL("!pmemobj_create");

	TOID(struct root_obj) root = POBJ_ROOT(pop, struct root_obj);

	TX_BEGIN_LOCK(pop, TX_LOCK_MUTEX, &D_RW(root)->lock) {
		TX_ADD(root);
		D_RW(root)->head = TX_NEW(struct obj);
		TOID(struct obj) o;
		o = D_RW(root)->head;
		D_RW(o)->data = 100;
		pmemobj_mutex_zero(pop, &D_RW(o)->lock);
		for (int i = 0; i < 3; i++) {
			D_RW(o)->next = TX_NEW(struct obj);
			o = D_RO(o)->next;
			D_RW(o)->data = 101 + i;
			pmemobj_mutex_zero(pop, &D_RW(o)->lock);
		}
		TOID_ASSIGN(D_RW(o)->next, OID_NULL);
	} TX_END;

	OUT("initial state");
	do_check(D_RO(root)->head);

	OUT("nested tx");
	do_nested_tx(pop, D_RW(root)->head, 200);
	do_check(D_RO(root)->head);

	OUT("aborted nested tx");
	do_aborted_nested_tx(pop, D_RW(root)->head, 300);
	do_check(D_RO(root)->head);

	pmemobj_close(pop);

	DONE(NULL);
}
