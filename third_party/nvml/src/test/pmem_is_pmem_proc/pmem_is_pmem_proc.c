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
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * pmem_is_pmem_proc.c -- unit test for pmem_is_pmem() /proc parsing
 *
 * usage: pmem_is_pmem_proc file addr len [addr len]...
 */

#define	_GNU_SOURCE
#include "unittest.h"

#include <dlfcn.h>

char *Sfile;

/*
 * fopen -- interpose on libc fopen()
 *
 * This catches opens to /proc/self/smaps and sends them to the fake smaps
 * file being tested.
 */
FILE *
fopen(const char *path, const char *mode)
{
	static FILE *(*fopen_ptr)(const char *path, const char *mode);

	if (strcmp(path, "/proc/self/smaps") == 0) {
		OUT("redirecting /proc/self/smaps to %s", Sfile);
		path = Sfile;
	}

	if (fopen_ptr == NULL)
		fopen_ptr = dlsym(RTLD_NEXT, "fopen");

	return (*fopen_ptr)(path, mode);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "pmem_is_pmem_proc");

	if (argc < 4 || argc % 2)
		FATAL("usage: %s file addr len [addr len]...", argv[0]);

	Sfile = argv[1];

	for (int arg = 2; arg < argc; arg += 2) {
		void *addr;
		size_t len;

		addr = (void *)strtoull(argv[arg], NULL, 16);
		len = (size_t)strtoull(argv[arg + 1], NULL, 10);
		OUT("addr %p, len %zu: %d", addr, len, pmem_is_pmem(addr, len));
	}

	DONE(NULL);
}
