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
 * pmem_memcpy.c -- unit test for doing a memcpy
 *
 * usage: pmem_memcpy file destoff srcoff length
 *
 */

#include "unittest.h"

/*
 * swap_mappings - given to mmapped regions swap them.
 *
 * Try swapping src and dest by unmapping src, mapping a new dest with
 * the original src address as a hint. If successful, unmap original dest.
 * Map a new src with the original dest as a hint.
 */
static void
swap_mappings(char **dest, char **src, size_t size, int fd)
{
	char *d = *dest;
	char *s = *src;
	char *td, *ts;

	MUNMAP(*src, size);

	/* mmap destination using src addr as hint */
	td = MMAP(s, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);

	MUNMAP(*dest, size);
	*dest = td;

	/* mmap src using original destination addr as a hint */
	ts = MMAP(d, size, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_ANONYMOUS,
		-1, 0);
	*src = ts;
}

/*
 * do_memcpy: Worker function for memcpy
 *
 * Always work within the boundary of bytes. Fill in 1/2 of the src
 * memory with the pattern we want to write. This allows us to check
 * that we did not overwrite anything we were not supposed to in the
 * dest.  Use the non pmem version of the memset/memcpy commands
 * so as not to introduce any possible side affects.
 */

static void
do_memcpy(int fd, char *dest, int dest_off, char *src, int src_off,
    size_t bytes, char *file_name)
{
	void *ret;
	char buf[bytes];

	memset(buf, 0, bytes);
	memset(dest, 0, bytes);
	memset(src, 0, bytes);

	memset(src, 0x5A, bytes / 4);
	memset(src + bytes / 4, 0x46, bytes / 4);

	/* dest == src */
	ret = pmem_memcpy_persist(dest + dest_off, dest + dest_off, bytes / 2);
	ASSERTeq(ret, dest + dest_off);
	ASSERTeq(*(char *)(dest + dest_off), 0);

	/* len == 0 */
	ret = pmem_memcpy_persist(dest + dest_off, src, 0);
	ASSERTeq(ret, dest + dest_off);
	ASSERTeq(*(char *)(dest + dest_off), 0);

	ret = pmem_memcpy_persist(dest + dest_off, src + src_off, bytes / 2);
	ASSERTeq(ret, dest + dest_off);

	/* memcmp will validate that what I expect in memory. */
	if (memcmp(src + src_off, dest + dest_off, bytes / 2))
		ERR("%s: first %zu bytes do not match",
			file_name, bytes / 2);

	/* Now validate the contents of the file */
	LSEEK(fd, (off_t)dest_off, SEEK_SET);
	if (READ(fd, buf, bytes / 2) == bytes / 2) {
		if (memcmp(src + src_off, buf, bytes / 2))
			ERR("%s: first %zu bytes do not match",
				file_name, bytes / 2);
	}
}

int
main(int argc, char *argv[])
{
	int fd;
	char *dest;
	char *src;
	struct stat stbuf;

	START(argc, argv, "pmem_memcpy");

	if (argc != 5)
		FATAL("usage: %s file srcoff destoff length", argv[0]);

	fd = OPEN(argv[1], O_RDWR);
	int dest_off = atoi(argv[2]);
	int src_off = atoi(argv[3]);
	size_t bytes = strtoul(argv[4], NULL, 0);

	FSTAT(fd, &stbuf);

	/* src > dst */
	dest = pmem_map(fd);
	if (dest == NULL)
		FATAL("!could not map file: %s", argv[1]);

	src = MMAP(dest + stbuf.st_size, stbuf.st_size, PROT_READ|PROT_WRITE,
		MAP_SHARED|MAP_ANONYMOUS, -1, 0);
	/*
	 * Its very unlikely that src would not be > dest. pmem_map
	 * chooses the first unused address >= 1TB, large
	 * enough to hold the give range, and 1GB aligned. If the
	 * addresses did not get swapped to allow src > dst, log error
	 * and allow test to continue.
	 */
	if (src <= dest) {
		swap_mappings(&dest, &src, stbuf.st_size, fd);
		if (src <= dest)
			ERR("cannot map files in memory order");
	}

	memset(dest, 0, (2 * bytes));
	memset(src, 0, (2 * bytes));

	do_memcpy(fd, dest, dest_off, src, src_off, bytes, argv[1]);

	/* dest > src */
	swap_mappings(&dest, &src, stbuf.st_size, fd);

	if (dest <= src) {
		ERR("cannot map files in memory order");
	}

	do_memcpy(fd, dest, dest_off, src, src_off, bytes, argv[1]);
	MUNMAP(dest, stbuf.st_size);
	MUNMAP(src, stbuf.st_size);

	CLOSE(fd);

	DONE(NULL);
}
