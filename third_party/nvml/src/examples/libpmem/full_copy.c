/*
 * Copyright (c) 2014, Intel Corporation
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
 * full_copy.c -- show how to use pmem_memcpy_nodrain()
 *
 * usage: full_copy src-file dst-file
 *
 * Copies src-file to dst-file in 4k chunks.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <libpmem.h>

/* copying 4k at a time to pmem for this example */
#define	BUF_LEN 4096

/*
 * do_copy_to_pmem -- copy to pmem, postponing drain step until the end
 */
void
do_copy_to_pmem(char *pmemaddr, int srcfd, off_t len)
{
	char buf[BUF_LEN];
	int cc;

	/* copy the file, saving the last flush step to the end */
	while ((cc = read(srcfd, buf, BUF_LEN)) > 0) {
		pmem_memcpy_nodrain(pmemaddr, buf, cc);
		pmemaddr += cc;
	}

	if (cc < 0) {
		perror("read");
		exit(1);
	}

	/* perform final flush step */
	pmem_drain();
}

/*
 * do_copy_to_non_pmem -- copy to a non-pmem memory mapped file
 */
void
do_copy_to_non_pmem(char *addr, int srcfd, off_t len)
{
	char *startaddr = addr;
	char buf[BUF_LEN];
	int cc;

	/* copy the file, saving the last flush step to the end */
	while ((cc = read(srcfd, buf, BUF_LEN)) > 0) {
		memcpy(addr, buf, cc);
		addr += cc;
	}

	if (cc < 0) {
		perror("read");
		exit(1);
	}

	/* flush it */
	if (pmem_msync(startaddr, len) < 0) {
		perror("pmem_msync");
		exit(1);
	}
}

int
main(int argc, char *argv[])
{
	int srcfd;
	int dstfd;
	struct stat stbuf;
	char *pmemaddr;

	if (argc != 3) {
		fprintf(stderr, "usage: %s src-file dst-file\n", argv[0]);
		exit(1);
	}

	/* open src-file */
	if ((srcfd = open(argv[1], O_RDONLY)) < 0) {
		perror(argv[1]);
		exit(1);
	}

	/* create a pmem file */
	if ((dstfd = open(argv[2], O_CREAT|O_EXCL|O_RDWR, 0666)) < 0) {
		perror(argv[2]);
		exit(1);
	}

	/* find the size of the src-file */
	if (fstat(srcfd, &stbuf) < 0) {
		perror("fstat");
		exit(1);
	}

	/* allocate the pmem */
	if ((errno = posix_fallocate(dstfd, 0, stbuf.st_size)) != 0) {
		perror("posix_fallocate");
		exit(1);
	}

	/* memory map it */
	if ((pmemaddr = pmem_map(dstfd)) == NULL) {
		perror("pmem_map");
		exit(1);
	}
	close(dstfd);

	/* determine if range is true pmem, call appropriate copy routine */
	if (pmem_is_pmem(pmemaddr, stbuf.st_size))
		do_copy_to_pmem(pmemaddr, srcfd, stbuf.st_size);
	else
		do_copy_to_non_pmem(pmemaddr, srcfd, stbuf.st_size);

	close(srcfd);
	pmem_unmap(pmemaddr, stbuf.st_size);

	exit(0);
}
