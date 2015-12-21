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
 * obj_sync.c -- unit test for PMEM-resident locks
 */
#include <pthread.h>

#include "unittest.h"
#include "libpmemobj.h"
#include "util.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "obj.h"

#define	DATA_SIZE 128

#define	FATAL_USAGE() FATAL("usage: obj_sync [mrc] <num_threads> <runs>\n")

/* posix thread worker typedef */
typedef void *(*worker)(void *);

/* the mock pmemobj pool */
static PMEMobjpool Mock_pop;

/* the tested object containing persistent synchronization primitives */
static struct mock_obj {
	PMEMmutex mutex;
	PMEMcond cond;
	PMEMrwlock rwlock;
	int check_data;
	uint8_t data[DATA_SIZE];
} *Test_obj;

FUNC_MOCK(pthread_mutex_init, int,
		pthread_mutex_t *mutex, const pthread_mutexattr_t *attr)

	FUNC_MOCK_RUN_RET_DEFAULT_REAL(pthread_mutex_init, mutex, attr)
	FUNC_MOCK_RUN(1) {
		return -1;
	}
FUNC_MOCK_END

FUNC_MOCK(pthread_rwlock_init, int,
		pthread_rwlock_t *rwlock, const pthread_rwlockattr_t *attr)
	FUNC_MOCK_RUN_RET_DEFAULT_REAL(pthread_rwlock_init, rwlock, attr)
	FUNC_MOCK_RUN(1) {
		return -1;
	}
FUNC_MOCK_END

FUNC_MOCK(pthread_cond_init, int,
		pthread_cond_t *cond, const pthread_condattr_t  *attr)
	FUNC_MOCK_RUN_RET_DEFAULT_REAL(pthread_cond_init, cond, attr)
	FUNC_MOCK_RUN(1) {
		return -1;
	}
FUNC_MOCK_END

/*
 * mock_open_pool -- (internal) simulate pool opening
 */
static void
mock_open_pool(PMEMobjpool *pop)
{
	__sync_fetch_and_add(&pop->run_id, 2);
}

/*
 * mutex_write_worker -- (internal) write data with mutex
 */
static void *
mutex_write_worker(void *arg)
{
	if (pmemobj_mutex_lock(&Mock_pop, &Test_obj->mutex)) {
		ERR("pmemobj_mutex_lock");
		return NULL;
	}
	memset(Test_obj->data, (int)(uintptr_t)arg, DATA_SIZE);
	if (pmemobj_mutex_unlock(&Mock_pop, &Test_obj->mutex))
		ERR("pmemobj_mutex_unlock");

	return NULL;
}

/*
 * mutex_check_worker -- (internal) check consistency with mutex
 */
static void *
mutex_check_worker(void *arg)
{
	if (pmemobj_mutex_lock(&Mock_pop, &Test_obj->mutex)) {
		ERR("pmemobj_mutex_lock");
		return NULL;
	}
	uint8_t val = Test_obj->data[0];
	for (int i = 1; i < DATA_SIZE; i++)
		ASSERTeq(Test_obj->data[i], val);
	if (pmemobj_mutex_unlock(&Mock_pop, &Test_obj->mutex))
		ERR("pmemobj_mutex_unlock");

	return NULL;
}

/*
 * cond_write_worker -- (internal) write data with cond variable
 */
static void *
cond_write_worker(void *arg)
{
	if (pmemobj_mutex_lock(&Mock_pop, &Test_obj->mutex))
		return NULL;

	memset(Test_obj->data, (int)(uintptr_t)arg, DATA_SIZE);
	Test_obj->check_data = 1;
	if (pmemobj_cond_signal(&Mock_pop, &Test_obj->cond))
		ERR("pmemobj_cond_signal");
	pmemobj_mutex_unlock(&Mock_pop, &Test_obj->mutex);

	return NULL;
}

/*
 * cond_check_worker -- (internal) check consistency with cond variable
 */
static void *
cond_check_worker(void *arg)
{
	if (pmemobj_mutex_lock(&Mock_pop, &Test_obj->mutex))
		return NULL;

	while (Test_obj->check_data != 1) {
		if (pmemobj_cond_wait(&Mock_pop, &Test_obj->cond,
					&Test_obj->mutex))
			ERR("pmemobj_cond_wait");
	}
	uint8_t val = Test_obj->data[0];
	for (int i = 1; i < DATA_SIZE; i++)
		ASSERTeq(Test_obj->data[i], val);
	pmemobj_mutex_unlock(&Mock_pop, &Test_obj->mutex);

	return NULL;
}

/*
 * rwlock_write_worker -- (internal) write data with rwlock
 */
static void *
rwlock_write_worker(void *arg)
{
	if (pmemobj_rwlock_wrlock(&Mock_pop, &Test_obj->rwlock)) {
		ERR("pmemobj_rwlock_wrlock");
		return NULL;
	}
	memset(Test_obj->data, (int)(uintptr_t)arg, DATA_SIZE);
	if (pmemobj_rwlock_unlock(&Mock_pop, &Test_obj->rwlock))
		ERR("pmemobj_rwlock_unlock");

	return NULL;
}

/*
 * rwlock_check_worker -- (internal) check consistency with rwlock
 */
static void *
rwlock_check_worker(void *arg)
{
	if (pmemobj_rwlock_rdlock(&Mock_pop, &Test_obj->rwlock)) {
		ERR("pmemobj_rwlock_rdlock");
		return NULL;
	}
	uint8_t val = Test_obj->data[0];
	for (int i = 1; i < DATA_SIZE; i++)
		ASSERTeq(Test_obj->data[i], val);
	if (pmemobj_rwlock_unlock(&Mock_pop, &Test_obj->rwlock))
		ERR("pmemobj_rwlock_unlock");

	return NULL;
}

/*
 * cleanup -- (internal) clean up after each run
 */
static void
cleanup(char test_type)
{
	switch (test_type) {
		case 'm':
			pthread_mutex_destroy(&Test_obj->mutex.pmemmutex.mutex);
			break;
		case 'r':
			pthread_rwlock_destroy(&Test_obj->rwlock.pmemrwlock
					.rwlock);
			break;
		case 'c':
			pthread_mutex_destroy(&Test_obj->mutex.pmemmutex.mutex);
			pthread_cond_destroy(&Test_obj->cond.pmemcond.cond);
			break;
		default:
			FATAL_USAGE();
	}

}

static void
obj_sync_persist(PMEMobjpool *pop, void *ptr, size_t sz)
{
	pmem_msync(ptr, sz);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_sync");
	util_init();

	if (argc < 4)
		FATAL_USAGE();

	worker writer;
	worker checker;

	char test_type = argv[1][0];
	switch (test_type) {
		case 'm':
			writer = mutex_write_worker;
			checker = mutex_check_worker;
			break;
		case 'r':
			writer = rwlock_write_worker;
			checker = rwlock_check_worker;
			break;
		case 'c':
			writer = cond_write_worker;
			checker = cond_check_worker;
			break;
		default:
			FATAL_USAGE();

	}

	unsigned long num_threads = strtoul(argv[2], NULL, 10);
	if (num_threads > 200)
		FATAL("Do not use more than 200 threads.\n");

	unsigned long runs = strtoul(argv[3], NULL, 10);

	pthread_t *write_threads = MALLOC(num_threads * sizeof (pthread_t));
	pthread_t *check_threads = MALLOC(num_threads * sizeof (pthread_t));

	/* first pool open */
	mock_open_pool(&Mock_pop);
	Mock_pop.persist = obj_sync_persist;
	Test_obj = MALLOC(sizeof (struct mock_obj));
	/* zero-initialize the test object */
	pmemobj_mutex_zero(&Mock_pop, &Test_obj->mutex);
	pmemobj_cond_zero(&Mock_pop, &Test_obj->cond);
	pmemobj_rwlock_zero(&Mock_pop, &Test_obj->rwlock);
	Test_obj->check_data = 0;
	memset(&Test_obj->data, 0, DATA_SIZE);

	for (int run = 0; run < runs; run++) {
		for (int i = 0; i < num_threads; i++) {
			PTHREAD_CREATE(&write_threads[i], NULL, writer,
				(void *)(uintptr_t)i);
			PTHREAD_CREATE(&check_threads[i], NULL, checker,
				NULL);
		}
		for (int i = 0; i < num_threads; i++) {
			PTHREAD_JOIN(write_threads[i], NULL);
			PTHREAD_JOIN(check_threads[i], NULL);
		}
		/* up the run_id counter and cleanup */
		mock_open_pool(&Mock_pop);
		cleanup(test_type);
	}

	FREE(check_threads);
	FREE(write_threads);
	FREE(Test_obj);
	DONE(NULL);
}
