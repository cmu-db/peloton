/*
 * Copyright (c) 2014-2015, Intel Corporation
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
 * vmem_multiple_pools.c -- unit test for vmem_multiple_pools
 *
 * usage: vmem_multiple_pools directory npools [nthreads]
 */

#include "unittest.h"

#define	TEST_REPEAT_CREATE_POOLS (10)

static char **mem_pools;
static VMEM **pools;
static int npools;
static const char *dir;

static void *
thread_func(void *arg)
{
	int start_idx = *(int *)arg;

	for (int repeat = 0; repeat < TEST_REPEAT_CREATE_POOLS; ++repeat) {
		for (int idx = 0; idx < npools; ++idx) {
			int pool_id = start_idx + idx;

			/* delete old pool with the same id if exist */
			if (pools[pool_id] != NULL) {
				vmem_delete(pools[pool_id]);
				pools[pool_id] = NULL;
			}

			if (pool_id % 2 == 0) {
				/* for even pool_id, create in region */
				pools[pool_id] = vmem_create_in_region(
					mem_pools[pool_id / 2], VMEM_MIN_POOL);
				if (pools[pool_id] == NULL)
					FATAL("!vmem_create_in_region");
			} else {
				/* for odd pool_id, create in file */
				pools[pool_id] = vmem_create(dir,
					VMEM_MIN_POOL);
				if (pools[pool_id] == NULL)
					FATAL("!vmem_create");
			}

			void *test = vmem_malloc(pools[pool_id],
				sizeof (void *));

			ASSERTne(test, NULL);
			vmem_free(pools[pool_id], test);
		}
	}

	return NULL;
}

int
main(int argc, char *argv[])
{
	int nthreads = 1;

	START(argc, argv, "vmem_multiple_pools");

	if (argc < 3 || argc > 4)
		FATAL("usage: %s directory npools [nthreads]", argv[0]);

	dir = argv[1];

	npools = atoi(argv[2]);

	if (argc > 3)
		nthreads = atoi(argv[3]);

	OUT("create %d pools in %d thread(s)", npools, nthreads);

	const unsigned mem_pools_size = (npools / 2 + npools % 2) * nthreads;
	mem_pools = MALLOC(mem_pools_size * sizeof (char *));
	pools = CALLOC(npools * nthreads, sizeof (VMEM *));
	pthread_t threads[nthreads];
	int pool_idx[nthreads];

	for (int pool_id = 0; pool_id < mem_pools_size; ++pool_id) {
		/* allocate memory for function vmem_create_in_region() */
		mem_pools[pool_id] = MMAP_ANON_ALIGNED(VMEM_MIN_POOL, 4 << 20);
	}

	/* create and destroy pools multiple times */
	for (int t = 0; t < nthreads; t++) {
		pool_idx[t] = npools * t;
		PTHREAD_CREATE(&threads[t], NULL, thread_func, &pool_idx[t]);
	}

	for (int t = 0; t < nthreads; t++)
		PTHREAD_JOIN(threads[t], NULL);

	for (int pool_id = 0; pool_id < npools; ++pool_id) {
		if (pools[pool_id] != NULL) {
			vmem_delete(pools[pool_id]);
			pools[pool_id] = NULL;
		}
	}

	FREE(mem_pools);
	FREE(pools);

	DONE(NULL);
}
