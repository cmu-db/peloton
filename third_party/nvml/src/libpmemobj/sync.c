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
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY LOG OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * sync.c -- persistent memory resident synchronization primitives
 */

#include <errno.h>

#include "libpmem.h"
#include "libpmemobj.h"
#include "util.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "obj.h"
#include "out.h"
#include "valgrind_internal.h"

#define	GET_MUTEX(pop, mutexp)\
get_lock((pop)->run_id,\
	&(mutexp)->pmemmutex.runid,\
	&(mutexp)->pmemmutex.mutex,\
	(void *)pthread_mutex_init,\
	sizeof ((mutexp)->pmemmutex.mutex))

#define	GET_RWLOCK(pop, rwlockp)\
get_lock((pop)->run_id,\
	&(rwlockp)->pmemrwlock.runid,\
	&(rwlockp)->pmemrwlock.rwlock,\
	(void *)pthread_rwlock_init,\
	sizeof ((rwlockp)->pmemrwlock.rwlock))


#define	GET_COND(pop, condp)\
get_lock((pop)->run_id,\
	&(condp)->pmemcond.runid,\
	&(condp)->pmemcond.cond,\
	(void *)pthread_cond_init,\
	sizeof ((condp)->pmemcond.cond))

/*
 * get_lock -- (internal) atomically initialize and return a lock
 */
static void *
get_lock(uint64_t pop_runid, volatile uint64_t *runid, void *lock,
	int (*init_lock)(void *lock, void *arg), size_t size)
{
	LOG(15, "pop_runid %ju runid %ju lock %p init_lock %p", pop_runid,
		*runid, lock, init_lock);

	uint64_t tmp_runid;
	while ((tmp_runid = *runid) != pop_runid) {
		if ((tmp_runid != (pop_runid - 1))) {
			VALGRIND_REMOVE_PMEM_MAPPING(runid, sizeof (*runid));
			VALGRIND_REMOVE_PMEM_MAPPING(lock, size);

			if (__sync_bool_compare_and_swap(runid,
					tmp_runid, (pop_runid - 1))) {
				if (init_lock(lock, NULL)) {
					ERR("error initializing lock");
					__sync_fetch_and_and(runid, 0);
					return NULL;
				}

				if (__sync_bool_compare_and_swap(
						runid, (pop_runid - 1),
						pop_runid) == 0) {
					ERR("error setting lock runid");
					return NULL;
				}
			}
		}
	}
	return lock;
}

/*
 * pmemobj_mutex_zero -- zero-initialize a pmem resident mutex
 *
 * This function is not MT safe.
 */
void
pmemobj_mutex_zero(PMEMobjpool *pop, PMEMmutex *mutexp)
{
	LOG(3, "pop %p mutex %p", pop, mutexp);

	mutexp->pmemmutex.runid = 0;
	pop->persist(pop, &mutexp->pmemmutex.runid,
				sizeof (mutexp->pmemmutex.runid));
}

/*
 * pmemobj_mutex_lock -- lock a pmem resident mutex
 *
 * Atomically initializes and locks a PMEMmutex, otherwise behaves as its
 * POSIX counterpart.
 */
int
pmemobj_mutex_lock(PMEMobjpool *pop, PMEMmutex *mutexp)
{
	LOG(3, "pop %p mutex %p", pop, mutexp);

	pthread_mutex_t *mutex = GET_MUTEX(pop, mutexp);
	if (mutex == NULL)
		return EINVAL;

	return pthread_mutex_lock(mutex);
}

/*
 * pmemobj_mutex_trylock -- trylock a pmem resident mutex
 *
 * Atomically initializes and trylocks a PMEMmutex, otherwise behaves as its
 * POSIX counterpart.
 */
int
pmemobj_mutex_trylock(PMEMobjpool *pop, PMEMmutex *mutexp)
{
	LOG(3, "pop %p mutex %p", pop, mutexp);

	pthread_mutex_t *mutex = GET_MUTEX(pop, mutexp);
	if (mutex == NULL)
		return EINVAL;

	return pthread_mutex_trylock(mutex);
}

/*
 * pmemobj_mutex_unlock -- unlock a pmem resident mutex
 */
int
pmemobj_mutex_unlock(PMEMobjpool *pop, PMEMmutex *mutexp)
{
	LOG(3, "pop %p mutex %p", pop, mutexp);

	/* XXX potential performance improvement - move GET to debug version */
	pthread_mutex_t *mutex = GET_MUTEX(pop, mutexp);
	if (mutex == NULL)
		return EINVAL;

	return pthread_mutex_unlock(mutex);
}

/*
 * pmemobj_rwlock_zero -- zero-initialize a pmem resident rwlock
 *
 * This function is not MT safe.
 */
void
pmemobj_rwlock_zero(PMEMobjpool *pop, PMEMrwlock *rwlockp)
{
	LOG(3, "pop %p rwlock %p", pop, rwlockp);

	rwlockp->pmemrwlock.runid = 0;
	pop->persist(pop, &rwlockp->pmemrwlock.runid,
				sizeof (rwlockp->pmemrwlock.runid));
}

/*
 * pmemobj_rwlock_rdlock -- rdlock a pmem resident mutex
 *
 * Atomically initializes and rdlocks a PMEMrwlock, otherwise behaves as its
 * POSIX counterpart.
 */
int
pmemobj_rwlock_rdlock(PMEMobjpool *pop, PMEMrwlock *rwlockp)
{
	LOG(3, "pop %p rwlock %p", pop, rwlockp);

	pthread_rwlock_t *rwlock = GET_RWLOCK(pop, rwlockp);
	if (rwlock == NULL)
		return EINVAL;

	return pthread_rwlock_rdlock(rwlock);
}

/*
 * pmemobj_rwlock_wrlock -- wrlock a pmem resident mutex
 *
 * Atomically initializes and wrlocks a PMEMrwlock, otherwise behaves as its
 * POSIX counterpart.
 */
int
pmemobj_rwlock_wrlock(PMEMobjpool *pop, PMEMrwlock *rwlockp)
{
	LOG(3, "pop %p rwlock %p", pop, rwlockp);

	pthread_rwlock_t *rwlock = GET_RWLOCK(pop, rwlockp);
	if (rwlock == NULL)
		return EINVAL;

	return pthread_rwlock_wrlock(rwlock);
}

/*
 * pmemobj_rwlock_timedrdlock -- timedrdlock a pmem resident mutex
 *
 * Atomically initializes and timedrdlocks a PMEMrwlock, otherwise behaves as
 * its POSIX counterpart.
 */
int
pmemobj_rwlock_timedrdlock(PMEMobjpool *pop, PMEMrwlock *__restrict rwlockp,
			const struct timespec *__restrict abs_timeout)
{
	LOG(3, "pop %p rwlock %p timeout sec %ld nsec %ld", pop, rwlockp,
		abs_timeout->tv_sec, abs_timeout->tv_nsec);

	pthread_rwlock_t *rwlock = GET_RWLOCK(pop, rwlockp);
	if (rwlock == NULL)
		return EINVAL;

	return pthread_rwlock_timedrdlock(rwlock, abs_timeout);
}

/*
 * pmemobj_rwlock_timedwrlock -- timedwrlock a pmem resident mutex
 *
 * Atomically initializes and timedwrlocks a PMEMrwlock, otherwise behaves as
 * its POSIX counterpart.
 */
int
pmemobj_rwlock_timedwrlock(PMEMobjpool *pop, PMEMrwlock *__restrict rwlockp,
			const struct timespec *__restrict abs_timeout)
{
	LOG(3, "pop %p rwlock %p timeout sec %ld nsec %ld", pop, rwlockp,
		abs_timeout->tv_sec, abs_timeout->tv_nsec);

	pthread_rwlock_t *rwlock = GET_RWLOCK(pop, rwlockp);
	if (rwlock == NULL)
		return EINVAL;

	return pthread_rwlock_timedwrlock(rwlock, abs_timeout);
}

/*
 * pmemobj_rwlock_tryrdlock -- tryrdlock a pmem resident mutex
 *
 * Atomically initializes and tryrdlocks a PMEMrwlock, otherwise behaves as its
 * POSIX counterpart.
 */
int
pmemobj_rwlock_tryrdlock(PMEMobjpool *pop, PMEMrwlock *rwlockp)
{
	LOG(3, "pop %p rwlock %p", pop, rwlockp);

	pthread_rwlock_t *rwlock = GET_RWLOCK(pop, rwlockp);
	if (rwlock == NULL)
		return EINVAL;

	return pthread_rwlock_tryrdlock(rwlock);
}

/*
 * pmemobj_rwlock_trywrlock -- trywrlock a pmem resident mutex
 *
 * Atomically initializes and trywrlocks a PMEMrwlock, otherwise behaves as its
 * POSIX counterpart.
 */
int
pmemobj_rwlock_trywrlock(PMEMobjpool *pop, PMEMrwlock *rwlockp)
{
	LOG(3, "pop %p rwlock %p", pop, rwlockp);

	pthread_rwlock_t *rwlock = GET_RWLOCK(pop, rwlockp);
	if (rwlock == NULL)
		return EINVAL;

	return pthread_rwlock_trywrlock(rwlock);
}

/*
 * pmemobj_rwlock_unlock -- unlock a pmem resident rwlock
 */
int
pmemobj_rwlock_unlock(PMEMobjpool *pop, PMEMrwlock *rwlockp)
{
	LOG(3, "pop %p rwlock %p", pop, rwlockp);

	/* XXX potential performance improvement - move GET to debug version */
	pthread_rwlock_t *rwlock = GET_RWLOCK(pop, rwlockp);
	if (rwlock == NULL)
		return EINVAL;

	return pthread_rwlock_unlock(rwlock);
}

/*
 * pmemobj_cond_zero -- zero-initialize a pmem resident condition variable
 *
 * This function is not MT safe.
 */
void
pmemobj_cond_zero(PMEMobjpool *pop, PMEMcond *condp)
{
	LOG(3, "pop %p cond %p", pop, condp);

	condp->pmemcond.runid = 0;
	pop->persist(pop, &condp->pmemcond.runid,
			sizeof (condp->pmemcond.runid));
}

/*
 * pmemobj_cond_broadcast -- broadcast a pmem resident condition variable
 *
 * Atomically initializes and broadcast a PMEMcond, otherwise behaves as its
 * POSIX counterpart.
 */
int
pmemobj_cond_broadcast(PMEMobjpool *pop, PMEMcond *condp)
{
	LOG(3, "pop %p cond %p", pop, condp);

	pthread_cond_t *cond = GET_COND(pop, condp);
	if (cond == NULL)
		return EINVAL;

	return pthread_cond_broadcast(cond);
}

/*
 * pmemobj_cond_signal -- signal a pmem resident condition variable
 *
 * Atomically initializes and signal a PMEMcond, otherwise behaves as its
 * POSIX counterpart.
 */
int
pmemobj_cond_signal(PMEMobjpool *pop, PMEMcond *condp)
{
	LOG(3, "pop %p cond %p", pop, condp);

	pthread_cond_t *cond = GET_COND(pop, condp);
	if (cond == NULL)
		return EINVAL;

	return pthread_cond_signal(cond);
}

/*
 * pmemobj_cond_timedwait -- timedwait on a pmem resident condition variable
 *
 * Atomically initializes and timedwait on a PMEMcond, otherwise behaves as its
 * POSIX counterpart.
 */
int
pmemobj_cond_timedwait(PMEMobjpool *pop, PMEMcond *__restrict condp,
			PMEMmutex *__restrict mutexp,
			const struct timespec *__restrict abstime)
{
	LOG(3, "pop %p cond %p mutex %p abstime sec %ld nsec %ld", pop, condp,
		mutexp, abstime->tv_sec, abstime->tv_nsec);

	pthread_cond_t *cond = GET_COND(pop, condp);
	pthread_mutex_t *mutex = GET_MUTEX(pop, mutexp);
	if ((cond == NULL) || (mutex == NULL))
		return EINVAL;

	return pthread_cond_timedwait(cond, mutex, abstime);
}

/*
 * pmemobj_cond_wait -- wait on a pmem resident condition variable
 *
 * Atomically initializes and wait on a PMEMcond, otherwise behaves as its
 * POSIX counterpart.
 */
int
pmemobj_cond_wait(PMEMobjpool *pop, PMEMcond *condp,
			PMEMmutex *__restrict mutexp)
{
	LOG(3, "pop %p cond %p mutex %p", pop, condp, mutexp);

	pthread_cond_t *cond = GET_COND(pop, condp);
	pthread_mutex_t *mutex = GET_MUTEX(pop, mutexp);
	if ((cond == NULL) || (mutex == NULL))
		return EINVAL;

	return pthread_cond_wait(cond, mutex);
}
