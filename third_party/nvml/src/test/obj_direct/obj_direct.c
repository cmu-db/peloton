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
 * obj_direct.c -- unit test for direct
 */
#include "unittest.h"
#include "util.h"
#include "lane.h"
#include "redo.h"
#include "list.h"
#include "obj.h"

#define	MAX_PATH_LEN 255
#define	LAYOUT_NAME "direct"

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
int flag = 1;

PMEMoid thread_oid;

static void *
test_worker(void *arg)
{
	/* before pool is closed */
	ASSERTne(pmemobj_direct(thread_oid), NULL);

	flag = 0;
	pthread_mutex_lock(&lock);
	/* after pool is closed */
	ASSERTeq(pmemobj_direct(thread_oid), NULL);

	pthread_mutex_unlock(&lock);

	return NULL;
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_direct");

	if (argc != 3)
		FATAL("usage: %s [directory] [# of pools]", argv[0]);

	int npools = atoi(argv[2]);
	const char *dir = argv[1];
	int r;

	PMEMobjpool *pops[npools];

	char path[MAX_PATH_LEN];
	for (int i = 0; i < npools; ++i) {
		snprintf(path, MAX_PATH_LEN, "%s/testfile%d", dir, i);
		pops[i] = pmemobj_create(path, LAYOUT_NAME, PMEMOBJ_MIN_POOL,
				S_IWUSR | S_IRUSR);

		if (pops[i] == NULL)
			FATAL("!pmemobj_create");
	}

	PMEMoid oids[npools];
	PMEMoid tmpoids[npools];

	oids[0] = OID_NULL;
	ASSERTeq(pmemobj_direct(oids[0]), NULL);

	for (int i = 0; i < npools; ++i) {
		oids[i] = (PMEMoid) {pops[i]->uuid_lo, 0};
		ASSERTeq(pmemobj_direct(oids[i]), NULL);

		uint64_t off = pops[i]->heap_offset;
		oids[i] = (PMEMoid) {pops[i]->uuid_lo, off};
		ASSERTeq((char *)pmemobj_direct(oids[i]) - off,
			(char *)pops[i]);

		r = pmemobj_alloc(pops[i], &tmpoids[i], 100, 1, NULL, NULL);
		ASSERTeq(r, 0);
	}

	r = pmemobj_alloc(pops[0], &thread_oid, 100, 2, NULL, NULL);
	ASSERTeq(r, 0);
	ASSERTne(pmemobj_direct(thread_oid), NULL);

	pthread_mutex_lock(&lock);

	pthread_t t;
	pthread_create(&t, NULL, test_worker, NULL);

	/* wait for the thread to perform the first direct */
	while (flag)
		;

	for (int i = 0; i < npools; ++i) {
		ASSERTne(pmemobj_direct(tmpoids[i]), NULL);

		pmemobj_free(&tmpoids[i]);

		ASSERTeq(pmemobj_direct(tmpoids[i]), NULL);
		pmemobj_close(pops[i]);
		ASSERTeq(pmemobj_direct(oids[i]), NULL);
	}
	pthread_mutex_unlock(&lock);

	pthread_join(t, NULL);

	DONE(NULL);
}
