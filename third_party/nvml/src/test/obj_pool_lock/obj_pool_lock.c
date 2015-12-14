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
 * obj_pool_lock.c -- unit test which checks whether it's possible to
 *                    simultaneously open the same obj pool
 */

#include "unittest.h"

#define	LAYOUT "layout"

static void
test_reopen(const char *path)
{
	PMEMobjpool *pop1 = pmemobj_create(path, LAYOUT, PMEMOBJ_MIN_POOL,
			S_IWUSR | S_IRUSR);
	if (!pop1)
		FATAL("!create");

	PMEMobjpool *pop2 = pmemobj_open(path, LAYOUT);
	if (pop2)
		FATAL("pmemobj_open should not succeed");

	if (errno != EWOULDBLOCK)
		FATAL("!pmemobj_open failed but for unexpected reason");

	pmemobj_close(pop1);

	pop2 = pmemobj_open(path, LAYOUT);
	if (!pop2)
		FATAL("pmemobj_open should succeed after close");

	pmemobj_close(pop2);

	UNLINK(path);
}

static void
test_open_in_different_process(const char *path, int sleep)
{
	pid_t pid = fork();
	PMEMobjpool *pop;

	if (pid < 0)
		FATAL("fork failed");

	if (pid == 0) {
		/* child */
		if (sleep)
			usleep(sleep);
		while (access(path, R_OK))
			usleep(100 * 1000);

		pop = pmemobj_open(path, LAYOUT);
		if (pop)
			FATAL("pmemobj_open after fork should not succeed");

		if (errno != EWOULDBLOCK)
			FATAL("!pmemobj_open after fork failed but for "
				"unexpected reason");

		exit(0);
	}

	pop = pmemobj_create(path, LAYOUT, PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR);
	if (!pop)
		FATAL("!create");

	int status;

	if (waitpid(pid, &status, 0) < 0)
		FATAL("!waitpid failed");

	if (!WIFEXITED(status))
		FATAL("child process failed");

	pmemobj_close(pop);

	UNLINK(path);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_pool_lock");

	if (argc < 2)
		FATAL("usage: %s path", argv[0]);

	test_reopen(argv[1]);

	test_open_in_different_process(argv[1], 0);
	for (int i = 1; i < 100000; i *= 2)
		test_open_in_different_process(argv[1], i);

	DONE(NULL);
}
