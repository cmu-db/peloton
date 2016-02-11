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
 * vmmalloc_fork.c -- unit test for libvmmalloc fork() support
 *
 * usage: vmmalloc_fork [c|e] <nfork> <nthread>
 */

#include <malloc.h>
#include <sys/wait.h>
#include "unittest.h"

#define	NBUFS 16

static void *
do_test(void *arg)
{
	int **bufs = malloc(NBUFS * sizeof (void *));
	ASSERTne(bufs, NULL);

	size_t *sizes = malloc(NBUFS * sizeof (size_t));
	ASSERTne(sizes, NULL);

	for (int j = 0; j < NBUFS; j++) {
		sizes[j] = sizeof (int) + 64 * (rand() % 100);
		bufs[j] = malloc(sizes[j]);
		ASSERTne(bufs[j], NULL);
	}

	for (int j = 0; j < NBUFS; j++) {
		ASSERT(malloc_usable_size(bufs[j]) >= sizes[j]);
		free(bufs[j]);
	}

	free(sizes);
	free(bufs);

	return NULL;
}

int
main(int argc, char *argv[])
{
	if (argc == 4 && argv[3][0] == 't') {
		exit(0);
	}

	START(argc, argv, "vmmalloc_fork");

	if (argc < 4)
		FATAL("usage: %s [c|e] <nfork> <nthread>", argv[0]);

	int nfork = atoi(argv[2]);
	int nthread = atoi(argv[3]);
	ASSERT(nfork >= 0);
	ASSERT(nthread >= 0);

	pthread_t thread[nthread];
	int first_child = 0;

	int **bufs = malloc(nfork * NBUFS * sizeof (void *));
	ASSERTne(bufs, NULL);

	size_t *sizes = malloc(nfork * NBUFS * sizeof (size_t));
	ASSERTne(sizes, NULL);

	int *pids1 = malloc(nfork * sizeof (pid_t));
	ASSERTne(pids1, NULL);

	int *pids2 = malloc(nfork * sizeof (pid_t));
	ASSERTne(pids2, NULL);

	for (int i = 0; i < nfork; i++) {
		for (int j = 0; j < NBUFS; j++) {
			int idx = i * NBUFS + j;

			sizes[idx] = sizeof (int) + 64 * (rand() % 100);
			bufs[idx] = malloc(sizes[idx]);
			ASSERTne(bufs[idx], NULL);
			ASSERT(malloc_usable_size(bufs[idx]) >= sizes[idx]);
		}

		for (int t = 0; t < nthread; ++t) {
			PTHREAD_CREATE(&thread[t], NULL, do_test, NULL);
		}

		pids1[i] = fork();
		if (pids1[i] == -1)
			OUT("fork failed");
		ASSERTne(pids1[i], -1);

		if (pids1[i] == 0 && argv[1][0] == 'e' && i == nfork - 1) {
			int fd = open("/dev/null", O_RDWR, S_IWUSR);
			int res = dup2(fd, 1);
			ASSERTne(res, -1);
			close(fd);
			execl("/bin/echo", "/bin/echo", "Hello world!", NULL);
		}

		pids2[i] = getpid();

		for (int j = 0; j < NBUFS; j++) {
			*bufs[i * NBUFS + j] = ((unsigned)pids2[i] << 16) + j;
		}

		if (pids1[i]) {
			/* parent */
			for (int t = 0; t < nthread; ++t) {
				PTHREAD_JOIN(thread[t], NULL);
			}
		} else {
			/* child */
			first_child = i + 1;
		}

		for (int ii = 0; ii < i; ii++) {
			for (int j = 0; j < NBUFS; j++) {
				ASSERTeq(*bufs[ii * NBUFS + j],
					((unsigned)pids2[ii] << 16) + j);
			}
		}
	}

	for (int i = first_child; i < nfork; i++) {
		int status;
		waitpid(pids1[i], &status, 0);
		ASSERT(WIFEXITED(status));
		ASSERTeq(WEXITSTATUS(status), 0);
	}

	free(pids1);
	free(pids2);

	for (int i = 0; i < nfork; i++) {
		for (int j = 0; j < NBUFS; j++) {
			int idx = i * NBUFS + j;

			ASSERT(malloc_usable_size(bufs[idx]) >= sizes[idx]);
			free(bufs[idx]);
		}
	}

	free(sizes);
	free(bufs);

	if (first_child == 0) {
		DONE(NULL);
	}
}
