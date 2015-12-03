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
 * vmmalloc_malloc_hooks.c -- unit test for libvmmalloc malloc hooks
 *
 * usage: vmmalloc_malloc_hooks
 */

#include <malloc.h>
#include "unittest.h"

#ifdef __MALLOC_HOOK_VOLATILE
#define	MALLOC_HOOK_VOLATILE __MALLOC_HOOK_VOLATILE
#else
#define	MALLOC_HOOK_VOLATILE /* */
#endif

void *(*old_malloc_hook) (size_t, const void *);
void *(*old_realloc_hook) (void *, size_t, const void *);
void *(*old_memalign_hook) (size_t, size_t, const void *);
void (*old_free_hook) (void *, const void *);

static int malloc_cnt = 0;
static int realloc_cnt = 0;
static int memalign_cnt = 0;
static int free_cnt = 0;

static void *
hook_malloc(size_t size, const void *caller)
{
	void *p;
	malloc_cnt++;
	__malloc_hook = old_malloc_hook;
	p = malloc(size);
	old_malloc_hook = __malloc_hook; // might changed
	__malloc_hook = hook_malloc;
	return p;
}

static void *
hook_realloc(void *ptr, size_t size, const void *caller)
{
	void *p;
	realloc_cnt++;
	__realloc_hook = old_realloc_hook;
	p = realloc(ptr, size);
	old_realloc_hook = __realloc_hook; // might changed
	__realloc_hook = hook_realloc;
	return p;
}

static void *
hook_memalign(size_t alignment, size_t size, const void *caller)
{
	void *p;
	memalign_cnt++;
	__memalign_hook = old_memalign_hook;
	p = memalign(alignment, size);
	old_memalign_hook = __memalign_hook; // might changed
	__memalign_hook = hook_memalign;
	return p;
}

static void
hook_free(void *ptr, const void *caller)
{
	free_cnt++;
	__free_hook = old_free_hook;
	free(ptr);
	old_free_hook = __free_hook;
	__free_hook = hook_free;
}

static void
hook_init(void)
{
	printf("installing hooks\n");

	old_malloc_hook = __malloc_hook;
	old_realloc_hook = __realloc_hook;
	old_memalign_hook = __memalign_hook;
	old_free_hook = __free_hook;

	__malloc_hook = hook_malloc;
	__realloc_hook = hook_realloc;
	__memalign_hook = hook_memalign;
	__free_hook = hook_free;
}

void (*MALLOC_HOOK_VOLATILE __malloc_initialize_hook)(void) = hook_init;

int
main(int argc, char *argv[])
{
	void *ptr;

	START(argc, argv, "vmmalloc_malloc_hooks");

	ptr = malloc(4321);
	free(ptr);

	ptr = calloc(1, 4321);
	free(ptr);

	ptr = realloc(NULL, 4321);
	free(ptr);

	ptr = memalign(16, 4321);
	free(ptr);

	OUT("malloc %d realloc %d memalign %d free %d",
			malloc_cnt, realloc_cnt, memalign_cnt, free_cnt);

	DONE(NULL);
}
