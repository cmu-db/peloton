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
 * vmmalloc_valgrind.c -- unit test for libvmmalloc valgrind
 *
 * usage: vmmalloc_valgrind <test-number>
 *
 * test-number can be a number from 0 to 2
 */

#include "unittest.h"

int
main(int argc, char *argv[])
{
	int *ptr;
	int test_case = -1;

	START(argc, argv, "vmmalloc_valgrind");

	if ((argc != 2) || (test_case = atoi(argv[1])) > 2)
		FATAL("usage: %s <test-number from 0 to 2>",
			argv[0]);

	switch (test_case) {
		case 0: {
			OUT("remove all allocations");
			ptr = malloc(sizeof (int));
			if (ptr == NULL)
				FATAL("!malloc");

			free(ptr);
			break;
		}
		case 1: {
			OUT("memory leaks");
			ptr = malloc(sizeof (int));
			if (ptr == NULL)
				FATAL("!malloc");

			/* prevent reporting leaked memory as still reachable */
			ptr = NULL;
			break;
		}
		case 2: {
			OUT("heap block overrun");
			ptr = malloc(12 * sizeof (int));
			if (ptr == NULL)
				FATAL("!malloc");

			/* heap block overrun */
			ptr[12] = 7;

			free(ptr);
			break;
		}
		default: {
			FATAL("!unknown test-number");
		}
	}

	DONE(NULL);
}
