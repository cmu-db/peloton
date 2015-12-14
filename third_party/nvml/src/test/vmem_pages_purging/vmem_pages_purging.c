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
 * vmem_pages_purging.c -- unit test for vmem_pages_purging
 *
 * usage: vmem_pages_purging [-z] directory
 */

#include "unittest.h"
#include "jemalloc/internal/jemalloc_internal.h"
#include "jemalloc/internal/size_classes.h"

#define	DEFAULT_COUNT	(SMALL_MAXCLASS / 4)
#define	DEFAULT_N	100

static void
usage(char *appname)
{
	FATAL("usage: %s [-z - use calloc] directory ", appname);
}

int
main(int argc, char *argv[])
{
	const int test_value = 123456;
	char *dir = NULL;
	int count = DEFAULT_COUNT;
	int n = DEFAULT_N;
	VMEM *vmp;
	int opt;
	int i, j;
	int use_calloc = 0;

	START(argc, argv, "vmem_pages_purging");

	while ((opt = getopt(argc, argv, "z")) != -1) {
		switch (opt) {
		case 'z':
			use_calloc = 1;
			break;
		default:
			usage(argv[0]);
		}
	}

	if (optind < argc) {
		dir = argv[optind];
	} else {
		usage(argv[0]);
	}

	vmp = vmem_create(dir, VMEM_MIN_POOL);
	if (vmp == NULL)
		FATAL("!vmem_create");

	for (i = 0; i < n; i++) {
		int *test = NULL;
		if (use_calloc)
			test = vmem_calloc(vmp, 1, count * sizeof (int));
		else
			test = vmem_malloc(vmp, count * sizeof (int));
		ASSERTne(test, NULL);

		if (use_calloc) {
			/* vmem_calloc should return zeroed memory */
			for (j = 0; j < count; j++)
				ASSERTeq(test[j], 0);
		}
		for (j = 0; j < count; j++)
			test[j] = test_value;
		for (j = 0; j < count; j++)
			ASSERTeq(test[j], test_value);

		vmem_free(vmp, test);
	}

	vmem_delete(vmp);

	DONE(NULL);
}
