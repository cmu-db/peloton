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
 * obj_tx_free.c -- unit test for pmemobj_tx_free
 */
#include <sys/param.h>
#include <string.h>

#include "unittest.h"
#include "libpmemobj.h"
#include "util.h"
#include "valgrind_internal.h"

#define	LAYOUT_NAME "tx_free"

#define	OBJ_SIZE	(200 * 1024)

enum type_number {
	TYPE_FREE_NO_TX,
	TYPE_FREE_WRONG_UUID,
	TYPE_FREE_COMMIT,
	TYPE_FREE_ABORT,
	TYPE_FREE_COMMIT_NESTED1,
	TYPE_FREE_COMMIT_NESTED2,
	TYPE_FREE_ABORT_NESTED1,
	TYPE_FREE_ABORT_NESTED2,
	TYPE_FREE_ABORT_AFTER_NESTED1,
	TYPE_FREE_ABORT_AFTER_NESTED2,
	TYPE_FREE_OOM,
	TYPE_FREE_ALLOC,
};

TOID_DECLARE(struct object, 0);

struct object {
	size_t value;
	char data[OBJ_SIZE - sizeof (size_t)];
};

/*
 * do_tx_alloc -- do tx allocation with specified type number
 */
static PMEMoid
do_tx_alloc(PMEMobjpool *pop, int type_num)
{
	PMEMoid ret = OID_NULL;

	TX_BEGIN(pop) {
		ret = pmemobj_tx_alloc(sizeof (struct object), type_num);
	} TX_END

	return ret;
}

/*
 * do_tx_free_no_tx -- try to free object without transaction
 */
static void
do_tx_free_no_tx(PMEMobjpool *pop)
{
	int ret;
	PMEMoid oid = do_tx_alloc(pop, TYPE_FREE_NO_TX);

	ret = pmemobj_tx_free(oid);
	ASSERTne(ret, 0);

	TOID(struct object) obj;
	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_NO_TX));
	ASSERT(!TOID_IS_NULL(obj));
}

/*
 * do_tx_free_wrong_uuid -- try to free object with invalid uuid
 */
static void
do_tx_free_wrong_uuid(PMEMobjpool *pop)
{
	int ret;
	PMEMoid oid = do_tx_alloc(pop, TYPE_FREE_WRONG_UUID);
	oid.pool_uuid_lo = ~oid.pool_uuid_lo;

	TX_BEGIN(pop) {
		ret = pmemobj_tx_free(oid);
		ASSERTeq(ret, 0);
	} TX_ONABORT {
		ret = -1;
	} TX_END

	ASSERTeq(ret, -1);

	TOID(struct object) obj;
	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_WRONG_UUID));
	ASSERT(!TOID_IS_NULL(obj));
}

/*
 * do_tx_free_null_oid -- call pmemobj_tx_free with OID_NULL
 */
static void
do_tx_free_null_oid(PMEMobjpool *pop)
{
	int ret;

	TX_BEGIN(pop) {
		ret = pmemobj_tx_free(OID_NULL);
	} TX_ONABORT {
		ret = -1;
	} TX_END

	ASSERTeq(ret, 0);
}

/*
 * do_tx_free_commit -- do the basic transactional deallocation of object
 */
static void
do_tx_free_commit(PMEMobjpool *pop)
{
	int ret;
	PMEMoid oid = do_tx_alloc(pop, TYPE_FREE_COMMIT);

	TX_BEGIN(pop) {
		ret = pmemobj_tx_free(oid);
		ASSERTeq(ret, 0);

	} TX_ONABORT {
		ASSERT(0);
	} TX_END

	TOID(struct object) obj;
	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_COMMIT));
	ASSERT(TOID_IS_NULL(obj));
}

/*
 * do_tx_free_abort -- abort deallocation of object
 */
static void
do_tx_free_abort(PMEMobjpool *pop)
{
	int ret;
	PMEMoid oid = do_tx_alloc(pop, TYPE_FREE_ABORT);

	TX_BEGIN(pop) {
		ret = pmemobj_tx_free(oid);
		ASSERTeq(ret, 0);

		pmemobj_tx_abort(-1);
	} TX_ONCOMMIT {
		ASSERT(0);
	} TX_END

	TOID(struct object) obj;
	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_ABORT));
	ASSERT(!TOID_IS_NULL(obj));
}

/*
 * do_tx_free_commit_nested -- do allocation in nested transaction
 */
static void
do_tx_free_commit_nested(PMEMobjpool *pop)
{
	int ret;
	PMEMoid oid1 = do_tx_alloc(pop, TYPE_FREE_COMMIT_NESTED1);
	PMEMoid oid2 = do_tx_alloc(pop, TYPE_FREE_COMMIT_NESTED2);

	TX_BEGIN(pop) {
		ret = pmemobj_tx_free(oid1);
		ASSERTeq(ret, 0);

		TX_BEGIN(pop) {
			ret = pmemobj_tx_free(oid2);
			ASSERTeq(ret, 0);

		} TX_ONABORT {
			ASSERT(0);
		} TX_END
	} TX_ONABORT {
		ASSERT(0);
	} TX_END

	TOID(struct object) obj;

	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_COMMIT_NESTED1));
	ASSERT(TOID_IS_NULL(obj));

	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_COMMIT_NESTED2));
	ASSERT(TOID_IS_NULL(obj));
}

/*
 * do_tx_free_abort_nested -- abort allocation in nested transaction
 */
static void
do_tx_free_abort_nested(PMEMobjpool *pop)
{
	int ret;
	PMEMoid oid1 = do_tx_alloc(pop, TYPE_FREE_ABORT_NESTED1);
	PMEMoid oid2 = do_tx_alloc(pop, TYPE_FREE_ABORT_NESTED2);

	TX_BEGIN(pop) {
		ret = pmemobj_tx_free(oid1);
		ASSERTeq(ret, 0);

		TX_BEGIN(pop) {
			ret = pmemobj_tx_free(oid2);
			ASSERTeq(ret, 0);

			pmemobj_tx_abort(-1);
		} TX_ONCOMMIT {
			ASSERT(0);
		} TX_END
	} TX_ONCOMMIT {
		ASSERT(0);
	} TX_END

	TOID(struct object) obj;

	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_ABORT_NESTED1));
	ASSERT(!TOID_IS_NULL(obj));

	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_ABORT_NESTED2));
	ASSERT(!TOID_IS_NULL(obj));
}

/*
 * do_tx_free_abort_after_nested -- abort transaction after nested
 * pmemobj_tx_free
 */
static void
do_tx_free_abort_after_nested(PMEMobjpool *pop)
{
	int ret;
	PMEMoid oid1 = do_tx_alloc(pop, TYPE_FREE_ABORT_AFTER_NESTED1);
	PMEMoid oid2 = do_tx_alloc(pop, TYPE_FREE_ABORT_AFTER_NESTED2);

	TX_BEGIN(pop) {
		ret = pmemobj_tx_free(oid1);
		ASSERTeq(ret, 0);

		TX_BEGIN(pop) {
			ret = pmemobj_tx_free(oid2);
			ASSERTeq(ret, 0);
		} TX_END

		pmemobj_tx_abort(-1);
	} TX_ONCOMMIT {
		ASSERT(0);
	} TX_END

	TOID(struct object) obj;

	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_ABORT_AFTER_NESTED1));
	ASSERT(!TOID_IS_NULL(obj));

	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_ABORT_AFTER_NESTED2));
	ASSERT(!TOID_IS_NULL(obj));
}

/*
 * do_tx_free_oom -- allocate until OOM and free all objects
 */
static void
do_tx_free_oom(PMEMobjpool *pop)
{
	int ret;
	size_t alloc_cnt = 0;
	size_t free_cnt = 0;
	PMEMoid oid;
	while ((oid = do_tx_alloc(pop, TYPE_FREE_OOM)).off != 0)
		alloc_cnt++;

	TX_BEGIN(pop) {
		while ((oid = pmemobj_first(pop, TYPE_FREE_OOM)).off != 0) {
			ret = pmemobj_tx_free(oid);
			ASSERTeq(ret, 0);

			free_cnt++;
		}
	} TX_ONABORT {
		ASSERT(0);
	} TX_END

	ASSERTeq(alloc_cnt, free_cnt);

	TOID(struct object) obj;
	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_OOM));
	ASSERT(TOID_IS_NULL(obj));
}

/*
 * do_tx_free_alloc_abort -- free object allocated in the same transaction
 * and abort transaction
 */
static void
do_tx_free_alloc_abort(PMEMobjpool *pop)
{
	int ret;
	TOID(struct object) obj;

	TX_BEGIN(pop) {
		TOID_ASSIGN(obj, pmemobj_tx_alloc(
				sizeof (struct object), TYPE_FREE_ALLOC));
		ASSERT(!TOID_IS_NULL(obj));
		ret = pmemobj_tx_free(obj.oid);
		ASSERTeq(ret, 0);
		pmemobj_tx_abort(-1);
	} TX_ONCOMMIT {
		ASSERT(0);
	} TX_END

	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_ALLOC));
	ASSERT(TOID_IS_NULL(obj));
}

/*
 * do_tx_free_alloc_abort -- free object allocated in the same transaction
 * and commit transaction
 */
static void
do_tx_free_alloc_commit(PMEMobjpool *pop)
{
	int ret;
	TOID(struct object) obj;

	TX_BEGIN(pop) {
		TOID_ASSIGN(obj, pmemobj_tx_alloc(
				sizeof (struct object), TYPE_FREE_ALLOC));
		ASSERT(!TOID_IS_NULL(obj));
		ret = pmemobj_tx_free(obj.oid);
		ASSERTeq(ret, 0);
	} TX_ONABORT {
		ASSERT(0);
	} TX_END

	TOID_ASSIGN(obj, pmemobj_first(pop, TYPE_FREE_ALLOC));
	ASSERT(TOID_IS_NULL(obj));
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_tx_free");
	util_init();

	if (argc != 2)
		FATAL("usage: %s [file]", argv[0]);

	PMEMobjpool *pop;
	if ((pop = pmemobj_create(argv[1], LAYOUT_NAME, PMEMOBJ_MIN_POOL,
	    S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create");

	do_tx_free_no_tx(pop);
	VALGRIND_WRITE_STATS;
	do_tx_free_wrong_uuid(pop);
	VALGRIND_WRITE_STATS;
	do_tx_free_null_oid(pop);
	VALGRIND_WRITE_STATS;
	do_tx_free_commit(pop);
	VALGRIND_WRITE_STATS;
	do_tx_free_abort(pop);
	VALGRIND_WRITE_STATS;
	do_tx_free_commit_nested(pop);
	VALGRIND_WRITE_STATS;
	do_tx_free_abort_nested(pop);
	VALGRIND_WRITE_STATS;
	do_tx_free_abort_after_nested(pop);
	VALGRIND_WRITE_STATS;
	do_tx_free_alloc_commit(pop);
	VALGRIND_WRITE_STATS;
	do_tx_free_alloc_abort(pop);
	VALGRIND_WRITE_STATS;
	do_tx_free_oom(pop);
	VALGRIND_WRITE_STATS;

	pmemobj_close(pop);

	DONE(NULL);
}
