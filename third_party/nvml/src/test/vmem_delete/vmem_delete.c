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
 * vmem_delete.c -- unit test for vmem_delete
 *
 * usage: vmem_delete <operation>
 *
 * operations are: 'h', 'f', 'm', 'c', 'r', 'a', 's', 'd'
 *
 */

#include "unittest.h"

sigjmp_buf Jmp;

/*
 * signal_handler -- called on SIGSEGV
 */
static void
signal_handler(int sig)
{
	OUT("\tsignal: %s", strsignal(sig));

	siglongjmp(Jmp, 1);
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "vmem_delete");

	VMEM *vmp;
	void *ptr;

	if (argc < 2)
		FATAL("usage: %s op:h|f|m|c|r|a|s|d", argv[0]);

	/* allocate memory for function vmem_create_in_region() */
	void *mem_pool = MMAP_ANON_ALIGNED(VMEM_MIN_POOL, 4 << 20);

	vmp = vmem_create_in_region(mem_pool, VMEM_MIN_POOL);
	if (vmp == NULL)
		FATAL("!vmem_create_in_region");

	ptr = vmem_malloc(vmp, sizeof (long long int));
	if (ptr == NULL)
		ERR("!vmem_malloc");
	vmem_delete(vmp);

	/* arrange to catch SEGV */
	struct sigaction v;
	sigemptyset(&v.sa_mask);
	v.sa_flags = 0;
	v.sa_handler = signal_handler;
	SIGACTION(SIGSEGV, &v, NULL);
	SIGACTION(SIGABRT, &v, NULL);
	SIGACTION(SIGILL, &v, NULL);

	/* go through all arguments one by one */
	for (int arg = 1; arg < argc; arg++) {
		/* Scan the character of each argument. */
		if (strchr("hfmcrasd", argv[arg][0]) == NULL ||
				argv[arg][1] != '\0')
			FATAL("op must be one of: h, f, m, c, r, a, s, d");

		switch (argv[arg][0]) {
		case 'h':
			OUT("Testing vmem_check...");
			if (!sigsetjmp(Jmp, 1)) {
				OUT("\tvmem_check returned %i",
							vmem_check(vmp));
			}
			break;

		case 'f':
			OUT("Testing vmem_free...");
			if (!sigsetjmp(Jmp, 1)) {
				vmem_free(vmp, ptr);
				OUT("\tvmem_free succeeded");
			}
			break;

		case 'm':
			OUT("Testing vmem_malloc...");
			if (!sigsetjmp(Jmp, 1)) {
				ptr = vmem_malloc(vmp, sizeof (long long int));
				if (ptr != NULL)
					OUT("\tvmem_malloc succeeded");
				else
					OUT("\tvmem_malloc returned NULL");
			}
			break;

		case 'c':
			OUT("Testing vmem_calloc...");
			if (!sigsetjmp(Jmp, 1)) {
				ptr = vmem_calloc(vmp, 10, sizeof (int));
				if (ptr != NULL)
					OUT("\tvmem_calloc succeeded");
				else
					OUT("\tvmem_calloc returned NULL");
			}
			break;

		case 'r':
			OUT("Testing vmem_realloc...");
			if (!sigsetjmp(Jmp, 1)) {
				ptr = vmem_realloc(vmp, ptr, 128);
				if (ptr != NULL)
					OUT("\tvmem_realloc succeeded");
				else
					OUT("\tvmem_realloc returned NULL");
			}
			break;

		case 'a':
			OUT("Testing vmem_aligned_alloc...");
			if (!sigsetjmp(Jmp, 1)) {
				ptr = vmem_aligned_alloc(vmp, 128, 128);
				if (ptr != NULL)
					OUT("\tvmem_aligned_alloc succeeded");
				else
					OUT("\tvmem_aligned_alloc"
							" returned NULL");
			}
			break;

		case 's':
			OUT("Testing vmem_strdup...");
			if (!sigsetjmp(Jmp, 1)) {
				ptr = vmem_strdup(vmp, "Test string");
				if (ptr != NULL)
					OUT("\tvmem_strdup succeeded");
				else
					OUT("\tvmem_strdup returned NULL");
			}
			break;

		case 'd':
			OUT("Testing vmem_delete...");
			if (!sigsetjmp(Jmp, 1)) {
				vmem_delete(vmp);
				if (errno != 0)
					OUT("\tvmem_delete failed: %s",
						vmem_errormsg());
				else
					OUT("\tvmem_delete succeeded");
			}
			break;
		}
	}

	DONE(NULL);
}
