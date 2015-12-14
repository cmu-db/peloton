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
 * obj_pmalloc_mt.c -- multithreaded test of allocator
 */
#include <stdint.h>

#include "libpmemobj.h"
#include "pmalloc.h"
#include "unittest.h"

#define	THREADS 32
#define	OPS_PER_THREAD 1000
#define	ALLOC_SIZE 100
#define	REALLOC_SIZE (ALLOC_SIZE * 3)
#define	FRAGMENTATION 3
#define	MIX_RERUNS 2

struct root {
	uint64_t offs[THREADS][OPS_PER_THREAD];
};

struct worker_args {
	PMEMobjpool *pop;
	struct root *r;
	int idx;
};

static void *
alloc_worker(void *arg)
{
	struct worker_args *a = arg;

	for (int i = 0; i < OPS_PER_THREAD; ++i) {
		pmalloc(a->pop, &a->r->offs[a->idx][i], ALLOC_SIZE, 0);
		ASSERTne(a->r->offs[a->idx][i], 0);
	}

	return NULL;
}

static void *
realloc_worker(void *arg)
{
	struct worker_args *a = arg;

	for (int i = 0; i < OPS_PER_THREAD; ++i) {
		prealloc(a->pop, &a->r->offs[a->idx][i], REALLOC_SIZE, 0);
		ASSERTne(a->r->offs[a->idx][i], 0);
	}

	return NULL;
}

static void *
free_worker(void *arg)
{
	struct worker_args *a = arg;

	for (int i = 0; i < OPS_PER_THREAD; ++i) {
		pfree(a->pop, &a->r->offs[a->idx][i], 0);
		ASSERTeq(a->r->offs[a->idx][i], 0);
	}

	return NULL;
}

static void *
mix_worker(void *arg)
{
	struct worker_args *a = arg;

	/*
	 * The mix scenario is ran twice to increase the chances of run
	 * contention.
	 */
	for (int i = 0; i < MIX_RERUNS; ++i) {
		for (int i = 0; i < OPS_PER_THREAD; ++i) {
			pmalloc(a->pop, &a->r->offs[a->idx][i], ALLOC_SIZE, 0);
			ASSERTne(a->r->offs[a->idx][i], 0);
		}

		for (int i = 0; i < OPS_PER_THREAD; ++i) {
			pfree(a->pop, &a->r->offs[a->idx][i], 0);
			ASSERTeq(a->r->offs[a->idx][i], 0);
		}
	}

	return NULL;
}

static void *
tx_worker(void *arg)
{
	struct worker_args *a = arg;

	/*
	 * Allocate objects until exhaustion, once that happens the transaction
	 * will automatically abort and all of the objects will be freed.
	 */
	TX_BEGIN(a->pop) {
		for (;;) /* this is NOT an infinite loop */
			pmemobj_tx_alloc(ALLOC_SIZE, a->idx);
	} TX_END

	return NULL;
}

static void *
alloc_free_worker(void *arg)
{
	struct worker_args *a = arg;

	PMEMoid oid;
	for (int i = 0; i < OPS_PER_THREAD; ++i) {
		int err = pmemobj_alloc(a->pop, &oid, ALLOC_SIZE,
				0, NULL, NULL);
		ASSERTeq(err, 0);
		pmemobj_free(&oid);
	}

	return NULL;
}

static void
run_worker(void *(worker_func)(void *arg), struct worker_args args[])
{
	pthread_t t[THREADS];

	for (int i = 0; i < THREADS; ++i)
		pthread_create(&t[i], NULL, worker_func, &args[i]);

	for (int i = 0; i < THREADS; ++i)
		pthread_join(t[i], NULL);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_pmalloc_mt");

	if (argc != 2)
		FATAL("usage: %s [file]", argv[0]);

	PMEMobjpool *pop;

	if (access(argv[1], F_OK) != 0) {
		pop = pmemobj_create(argv[1], "TEST",
		THREADS * OPS_PER_THREAD * ALLOC_SIZE * FRAGMENTATION, 0666);
	} else {
		if ((pop = pmemobj_open(argv[1], "TEST")) == NULL) {
			printf("failed to open pool\n");
			return 1;
		}
	}

	if (pop == NULL)
		FATAL("!pmemobj_create");

	PMEMoid oid = pmemobj_root(pop, sizeof (struct root));
	struct root *r = pmemobj_direct(oid);
	ASSERTne(r, NULL);

	struct worker_args args[THREADS];

	for (int i = 0; i < THREADS; ++i) {
		args[i].pop = pop;
		args[i].r = r;
		args[i].idx = i;
	}

	run_worker(alloc_worker, args);
	run_worker(realloc_worker, args);
	run_worker(free_worker, args);
	run_worker(mix_worker, args);
	run_worker(tx_worker, args);
	run_worker(alloc_free_worker, args);

	DONE(NULL);
}
