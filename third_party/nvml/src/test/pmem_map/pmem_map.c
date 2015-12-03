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
 * pmem_map.c -- unit test for mapping persistent memory for raw access
 *
 * usage: pmem_map file
 */

#include "unittest.h"

#define	CHECK_BYTES 4096	/* bytes to compare before/after map call */

sigjmp_buf Jmp;

/*
 * signal_handler -- called on SIGSEGV
 */
static void
signal_handler(int sig)
{
	OUT("signal: %s", strsignal(sig));

	siglongjmp(Jmp, 1);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "pmem_map");

	if (argc != 2)
		FATAL("usage: %s file", argv[0]);

	/* arrange to catch SEGV */
	struct sigaction v;
	sigemptyset(&v.sa_mask);
	v.sa_flags = 0;
	v.sa_handler = signal_handler;
	SIGACTION(SIGSEGV, &v, NULL);

	int fd;
	void *addr;

	fd = OPEN(argv[1], O_RDWR);

	struct stat stbuf;
	FSTAT(fd, &stbuf);

	char pat[CHECK_BYTES];
	char buf[CHECK_BYTES];

	addr = pmem_map(fd);
	if (addr == NULL) {
		OUT("!pmem_map");
		goto err;
	}

	/* write some pattern to the file */
	memset(pat, 0x5A, CHECK_BYTES);
	WRITE(fd, pat, CHECK_BYTES);


	if (memcmp(pat, addr, CHECK_BYTES))
		OUT("%s: first %d bytes do not match",
			argv[1], CHECK_BYTES);

	/* fill up mapped region with new pattern */
	memset(pat, 0xA5, CHECK_BYTES);
	memcpy(addr, pat, CHECK_BYTES);

	pmem_unmap(addr, stbuf.st_size);

	if (!sigsetjmp(Jmp, 1)) {
		/* same memcpy from above should now fail */
		memcpy(addr, pat, CHECK_BYTES);
	} else {
		OUT("unmap successful");
	}

	LSEEK(fd, (off_t)0, SEEK_SET);
	if (READ(fd, buf, CHECK_BYTES) == CHECK_BYTES) {
		if (memcmp(pat, buf, CHECK_BYTES))
			OUT("%s: first %d bytes do not match",
				argv[1], CHECK_BYTES);
	}

	CLOSE(fd);

	/* re-open the file with read-only access */
	fd = OPEN(argv[1], O_RDONLY);

	addr = pmem_map(fd);
	if (addr != NULL) {
		MUNMAP(addr, stbuf.st_size);
		OUT("expected pmem_map failure");
	}

err:
	CLOSE(fd);

	DONE(NULL);
}
