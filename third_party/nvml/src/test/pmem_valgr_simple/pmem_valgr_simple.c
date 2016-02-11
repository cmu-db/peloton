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
 * pmem_valgr_simple.c -- simple unit test using pmemcheck
 *
 * usage: pmem_valgr_simple file
 */

#include "unittest.h"

int
main(int argc, char *argv[])
{
	int fd;
	struct stat stbuf;
	char *dest;

	START(argc, argv, "pmem_valgr_simple");

	if (argc != 4)
		FATAL("usage: %s file offset length", argv[0]);

	fd = OPEN(argv[1], O_RDWR);
	int dest_off = atoi(argv[2]);
	size_t bytes = strtoul(argv[3], NULL, 0);

	FSTAT(fd, &stbuf);

	dest = pmem_map(fd);
	if (dest == NULL)
		FATAL("!Could not mmap %s\n", argv[1]);

	/* these will not be made persistent */
	*(int *)dest = 4;

	/* this will be made persistent */
	uint64_t *tmp64dst = (void *)((uintptr_t)dest + 4096);
	*tmp64dst = 50;

	if (pmem_is_pmem(dest, sizeof (*tmp64dst))) {
		pmem_persist(tmp64dst, sizeof (*tmp64dst));
	} else {
		pmem_msync(tmp64dst, sizeof (*tmp64dst));
	}

	uint16_t *tmp16dst = (void *)((uintptr_t)dest + 1024);
	*tmp16dst = 21;
	/* will appear as flushed/fenced in valgrind log */
	pmem_flush(tmp16dst, sizeof (*tmp16dst));

	/* shows strange behavior of memset in some cases */
	memset(dest + dest_off, 0, bytes);

	pmem_unmap(dest, stbuf.st_size);

	CLOSE(fd);

	DONE(NULL);
}
