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
 * out_err_mt.c -- unit test for error messages
 */

#include <sys/types.h>
#include <stdarg.h>
#include <errno.h>
#include "unittest.h"
#include "valgrind_internal.h"
#include "util.h"

#define	NUM_THREADS 16

static void
print_errors(const char *msg)
{
	OUT("%s", msg);
	OUT("PMEM: %s", pmem_errormsg());
	OUT("PMEMOBJ: %s", pmemobj_errormsg());
	OUT("PMEMLOG: %s", pmemlog_errormsg());
	OUT("PMEMBLK: %s", pmemblk_errormsg());
	OUT("VMEM: %s", vmem_errormsg());
}

static void
check_errors(int ver)
{
	int ret;
	int err_need;
	int err_found;

	ret = sscanf(pmem_errormsg(),
		"libpmem major version mismatch (need %d, found %d)",
		&err_need, &err_found);
	ASSERTeq(ret, 2);
	ASSERTeq(err_need, ver);
	ASSERTeq(err_found, PMEM_MAJOR_VERSION);

	ret = sscanf(pmemobj_errormsg(),
		"libpmemobj major version mismatch (need %d, found %d)",
		&err_need, &err_found);
	ASSERTeq(ret, 2);
	ASSERTeq(err_need, ver);
	ASSERTeq(err_found, PMEMOBJ_MAJOR_VERSION);

	ret = sscanf(pmemlog_errormsg(),
		"libpmemlog major version mismatch (need %d, found %d)",
		&err_need, &err_found);
	ASSERTeq(ret, 2);
	ASSERTeq(err_need, ver);
	ASSERTeq(err_found, PMEMLOG_MAJOR_VERSION);

	ret = sscanf(pmemblk_errormsg(),
		"libpmemblk major version mismatch (need %d, found %d)",
		&err_need, &err_found);
	ASSERTeq(ret, 2);
	ASSERTeq(err_need, ver);
	ASSERTeq(err_found, PMEMBLK_MAJOR_VERSION);

	ret = sscanf(vmem_errormsg(),
		"libvmem major version mismatch (need %d, found %d)",
		&err_need, &err_found);
	ASSERTeq(ret, 2);
	ASSERTeq(err_need, ver);
	ASSERTeq(err_found, VMEM_MAJOR_VERSION);
}

static void *
do_test(void *arg)
{
	int ver = *(int *)arg;

	pmem_check_version(ver, 0);
	pmemobj_check_version(ver, 0);
	pmemlog_check_version(ver, 0);
	pmemblk_check_version(ver, 0);
	vmem_check_version(ver, 0);
	check_errors(ver);

	return NULL;
}

static void
run_mt_test(void *(*worker)(void *))
{
	pthread_t thread[NUM_THREADS];
	int ver[NUM_THREADS];

	for (int i = 0; i < NUM_THREADS; ++i) {
		ver[i] = 10000 + i;
		PTHREAD_CREATE(&thread[i], NULL, worker, &ver[i]);
	}
	for (int i = 0; i < NUM_THREADS; ++i) {
		PTHREAD_JOIN(thread[i], NULL);
	}
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "out_err_mt");

	if (argc != 5)
		FATAL("usage: %s filename1 filename2 filename3 dir", argv[0]);

	PMEMobjpool *pop = pmemobj_create(argv[1], "test",
		PMEMOBJ_MIN_POOL, 0666);
	PMEMlogpool *plp = pmemlog_create(argv[2],
		PMEMLOG_MIN_POOL, 0666);
	PMEMblkpool *pbp = pmemblk_create(argv[3],
		128, PMEMBLK_MIN_POOL, 0666);
	VMEM *vmp = vmem_create(argv[4], VMEM_MIN_POOL);

	util_init();

	pmem_check_version(10000, 0);
	pmemobj_check_version(10001, 0);
	pmemlog_check_version(10002, 0);
	pmemblk_check_version(10003, 0);
	vmem_check_version(10004, 0);
	print_errors("version check");

	void *ptr = NULL;
	/*
	 * We are testing library error reporting and we don't want this test
	 * to fail under memcheck.
	 */
	VALGRIND_DO_DISABLE_ERROR_REPORTING;
	pmem_msync(ptr, 1);
	VALGRIND_DO_ENABLE_ERROR_REPORTING;
	print_errors("pmem_msync");

	(void) pmemobj_first(pop, PMEMOBJ_NUM_OID_TYPES + 1);
	print_errors("pmemobj_tx_abort");

	pmemlog_append(plp, NULL, PMEMLOG_MIN_POOL);
	print_errors("pmemlog_append");

	size_t nblock = pmemblk_nblock(pbp);
	pmemblk_set_error(pbp, nblock + 1);
	print_errors("pmemblk_set_error");

	VMEM *vmp2 = vmem_create_in_region(NULL, 1);
	ASSERTeq(vmp2, NULL);
	print_errors("vmem_create_in_region");

	run_mt_test(do_test);

	pmemobj_close(pop);
	pmemlog_close(plp);
	pmemblk_close(pbp);
	vmem_delete(vmp);

	DONE(NULL);
}
