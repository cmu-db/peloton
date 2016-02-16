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
 * vmmalloc_init.c -- unit test for libvmmalloc initialization
 *
 * usage: vmmalloc_init [d|l]
 */

#include <malloc.h>
#include <dlfcn.h>
#include "unittest.h"

static void *(*Falloc)(size_t size, int val);

int
main(int argc, char *argv[])
{
	void *handle = NULL;
	void *ptr;

	START(argc, argv, "vmmalloc_init");

	/* check if malloc hooks are pointing to libvmmalloc */
	ASSERTeq((void *)__malloc_hook, (void *)malloc);
	ASSERTeq((void *)__free_hook, (void *)free);
	ASSERTeq((void *)__realloc_hook, (void *)realloc);
	ASSERTeq((void *)__memalign_hook, (void *)memalign);

	if (argc > 2)
		FATAL("usage: %s [d|l]", argv[0]);

	if (argc == 2) {
		switch (argv[1][0]) {
		case 'd':
			OUT("deep binding");
			handle = dlopen("./libtest.so",
				RTLD_NOW | RTLD_LOCAL | RTLD_DEEPBIND);
			break;
		case 'l':
			OUT("lazy binding");
			handle = dlopen("./libtest.so", RTLD_LAZY);
			break;
		default:
			FATAL("usage: %s [d|l]", argv[0]);
		}

		if (handle == NULL)
			OUT("dlopen: %s", dlerror());
		ASSERTne(handle, NULL);

		Falloc = dlsym(handle, "falloc");
		ASSERTne(Falloc, NULL);
	}

	ptr = malloc(4321);
	free(ptr);

	if (argc == 2) {
		/*
		 * NOTE: falloc calls malloc internally.
		 * If libtest is loaded with RTLD_DEEPBIND flag, then it will
		 * use its own lookup scope in preference to global symbols
		 * from already loaded (LD_PRELOAD) libvmmalloc.  So, falloc
		 * will call the stock libc's malloc.
		 * However, since we override the malloc hooks, a call to libc's
		 * malloc will be redirected to libvmmalloc anyway, and the
		 * memory can be safely reclaimed using libvmmalloc's free.
		 */
		ptr = Falloc(4321, 0xaa);
		free(ptr);
	}

	DONE(NULL);
}
