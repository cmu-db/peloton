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
 * blk_pool.c -- unit test for pmemblk_create() and pmemblk_open()
 *
 * usage: blk_pool op path bsize [poolsize mode]
 *
 * op can be:
 *   c - create
 *   o - open
 *
 * "poolsize" and "mode" arguments are ignored for "open"
 */
#include "unittest.h"

#define	MB ((size_t)1 << 20)

static void
pool_create(const char *path, size_t bsize, size_t poolsize, unsigned mode)
{
	PMEMblkpool *pbp = pmemblk_create(path, bsize, poolsize, mode);

	if (pbp == NULL)
		OUT("!%s: pmemblk_create", path);
	else {
		struct stat stbuf;
		STAT(path, &stbuf);

		OUT("%s: file size %zu usable blocks %zu mode 0%o",
				path, stbuf.st_size,
				pmemblk_nblock(pbp),
				stbuf.st_mode & 0777);

		pmemblk_close(pbp);

		int result = pmemblk_check(path, bsize);

		if (result < 0)
			OUT("!%s: pmemblk_check", path);
		else if (result == 0)
			OUT("%s: pmemblk_check: not consistent", path);
		else
			ASSERTeq(pmemblk_check(path, bsize * 2), -1);
	}
}

static void
pool_open(const char *path, size_t bsize)
{
	PMEMblkpool *pbp = pmemblk_open(path, bsize);
	if (pbp == NULL)
		OUT("!%s: pmemblk_open", path);
	else {
		OUT("%s: pmemblk_open: Success", path);
		pmemblk_close(pbp);
	}
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "blk_pool");

	if (argc < 4)
		FATAL("usage: %s op path bsize [poolsize mode]", argv[0]);

	size_t bsize = strtoul(argv[3], NULL, 0);
	size_t poolsize;
	unsigned mode;

	switch (argv[1][0]) {
	case 'c':
		poolsize = strtoul(argv[4], NULL, 0) * MB; /* in megabytes */
		mode = strtoul(argv[5], NULL, 8);

		pool_create(argv[2], bsize, poolsize, mode);
		break;

	case 'o':
		pool_open(argv[2], bsize);
		break;

	default:
		FATAL("unknown operation");
	}

	DONE(NULL);
}
