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
 * obj_tx_locks.c -- unit test for transaction locks
 */
#include "unittest.h"
#include "libpmemobj.h"
#include "util.h"

#define	LAYOUT_NAME "direct"

#define	NUM_LOCKS 2
#define	NUM_THREADS 10
#define	TEST_VALUE_A 5
#define	TEST_VALUE_B 10
#define	TEST_VALUE_C 15

#define	BEGIN_TX(pop, mutexes, rwlocks)	TX_BEGIN_LOCK((pop), TX_LOCK_MUTEX,\
		&(mutexes)[0], TX_LOCK_MUTEX, &(mutexes)[1], TX_LOCK_RWLOCK,\
		&(rwlocks)[0], TX_LOCK_RWLOCK, &(rwlocks)[1], TX_LOCK_NONE)

static struct transaction_data {
	PMEMobjpool *pop;
	PMEMmutex *mutexes;
	PMEMrwlock *rwlocks;
	int a;
	int b;
	int c;
} test_obj;

/*
 * do_tx -- (internal) thread-friendly transaction
 */
static void *
do_tx(void *arg)
{
	struct transaction_data *data = arg;

	BEGIN_TX(data->pop, data->mutexes, data->rwlocks) {
		data->a = TEST_VALUE_A;
	} TX_ONCOMMIT {
		ASSERT(data->a == TEST_VALUE_A);
		data->b = TEST_VALUE_B;
	} TX_ONABORT { /* not called */
		data->a = TEST_VALUE_B;
	} TX_FINALLY {
		ASSERT(data->b == TEST_VALUE_B);
		data->c = TEST_VALUE_C;
	} TX_END

	return NULL;
}

/*
 * do_aborted_tx -- (internal) thread-friendly aborted transaction
 */
static void *
do_aborted_tx(void *arg)
{
	struct transaction_data *data = arg;

	BEGIN_TX(data->pop, data->mutexes, data->rwlocks) {
		data->a = TEST_VALUE_A;
		pmemobj_tx_abort(EINVAL);
		data->a = TEST_VALUE_B;
	} TX_ONCOMMIT { /* not called */
		data->a = TEST_VALUE_B;
	} TX_ONABORT {
		ASSERT(data->a == TEST_VALUE_A);
		data->b = TEST_VALUE_B;
	} TX_FINALLY {
		ASSERT(data->b == TEST_VALUE_B);
		data->c = TEST_VALUE_C;
	} TX_END

	return NULL;
}

/*
 * do_nested_tx-- (internal) thread-friendly nested transaction
 */
static void *
do_nested_tx(void *arg)
{
	struct transaction_data *data = arg;

	BEGIN_TX(data->pop, data->mutexes, data->rwlocks) {
		BEGIN_TX(data->pop, data->mutexes, data->rwlocks) {
			data->a = TEST_VALUE_A;
		} TX_ONCOMMIT {
			ASSERT(data->a == TEST_VALUE_A);
			data->b = TEST_VALUE_B;
		} TX_END
	} TX_ONCOMMIT {
		data->c = TEST_VALUE_C;
	} TX_END

	return NULL;
}

/*
 * do_aborted_nested_tx -- (internal) thread-friendly aborted nested transaction
 */
static void *
do_aborted_nested_tx(void *arg)
{
	struct transaction_data *data = arg;

	BEGIN_TX(data->pop, data->mutexes, data->rwlocks) {
		data->a = TEST_VALUE_C;
		BEGIN_TX(data->pop, data->mutexes, data->rwlocks) {
			data->a = TEST_VALUE_A;
			pmemobj_tx_abort(EINVAL);
			data->a = TEST_VALUE_B;
		} TX_ONCOMMIT { /* not called */
			data->a = TEST_VALUE_C;
		} TX_ONABORT {
			ASSERT(data->a == TEST_VALUE_A);
			data->b = TEST_VALUE_B;
		} TX_FINALLY {
			ASSERT(data->b == TEST_VALUE_B);
			data->c = TEST_VALUE_C;
		} TX_END
		data->a = TEST_VALUE_B;
	} TX_ONCOMMIT { /* not called */
		ASSERT(data->a == TEST_VALUE_A);
		data->c = TEST_VALUE_C;
	} TX_ONABORT {
		ASSERT(data->a == TEST_VALUE_A);
		ASSERT(data->b == TEST_VALUE_B);
		ASSERT(data->c == TEST_VALUE_C);
		data->a = TEST_VALUE_B;
	} TX_FINALLY {
		ASSERT(data->a == TEST_VALUE_B);
		data->b = TEST_VALUE_A;
	} TX_END

	return NULL;
}

static void
run_mt_test(void *(*worker)(void *), void *arg)
{
	pthread_t thread[NUM_THREADS];
	for (int i = 0; i < NUM_THREADS; ++i) {
		PTHREAD_CREATE(&thread[i], NULL, worker, arg);
	}
	for (int i = 0; i < NUM_THREADS; ++i) {
		PTHREAD_JOIN(thread[i], NULL);
	}
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_tx_locks");

	if (argc > 3)
		FATAL("usage: %s <file> [m]", argv[0]);

	if ((test_obj.pop = pmemobj_create(argv[1], LAYOUT_NAME,
	    PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR)) == NULL)
		FATAL("!pmemobj_create");

	int multithread = 0;
	if (argc == 3) {
		multithread = (argv[2][0] == 'm');
		if (!multithread)
			FATAL("wrong test type supplied %c", argv[1][0]);
	}

	test_obj.mutexes = CALLOC(NUM_LOCKS, sizeof (PMEMmutex));
	test_obj.rwlocks = CALLOC(NUM_LOCKS, sizeof (PMEMrwlock));

	if (multithread) {
		run_mt_test(do_tx, &test_obj);
	} else {
		do_tx(&test_obj);
		do_tx(&test_obj);
	}

	ASSERT(test_obj.a == TEST_VALUE_A);
	ASSERT(test_obj.b == TEST_VALUE_B);
	ASSERT(test_obj.c == TEST_VALUE_C);

	if (multithread) {
		run_mt_test(do_aborted_tx, &test_obj);
	} else {
		do_aborted_tx(&test_obj);
		do_aborted_tx(&test_obj);
	}

	ASSERT(test_obj.a == TEST_VALUE_A);
	ASSERT(test_obj.b == TEST_VALUE_B);
	ASSERT(test_obj.c == TEST_VALUE_C);

	if (multithread) {
		run_mt_test(do_nested_tx, &test_obj);
	} else {
		do_nested_tx(&test_obj);
		do_nested_tx(&test_obj);
	}

	ASSERT(test_obj.a == TEST_VALUE_A);
	ASSERT(test_obj.b == TEST_VALUE_B);
	ASSERT(test_obj.c == TEST_VALUE_C);

	if (multithread) {
		run_mt_test(do_aborted_nested_tx, &test_obj);
	} else {
		do_aborted_nested_tx(&test_obj);
		do_aborted_nested_tx(&test_obj);
	}

	ASSERT(test_obj.a == TEST_VALUE_B);
	ASSERT(test_obj.b == TEST_VALUE_A);
	ASSERT(test_obj.c == TEST_VALUE_C);

	pmemobj_close(test_obj.pop);

	DONE(NULL);
}
