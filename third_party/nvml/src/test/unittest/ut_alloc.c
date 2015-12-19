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
 * ut_alloc.c -- unit test memory allocation routines
 */

#include "unittest.h"

/*
 * ut_malloc -- a malloc that cannot return NULL
 */
void *
ut_malloc(const char *file, int line, const char *func, size_t size)
{
	void *retval = malloc(size);

	if (retval == NULL)
		ut_fatal(file, line, func, "cannot malloc %zu bytes", size);

	return retval;
}

/*
 * ut_calloc -- a calloc that cannot return NULL
 */
void *
ut_calloc(const char *file, int line, const char *func,
    size_t nmemb, size_t size)
{
	void *retval = calloc(nmemb, size);

	if (retval == NULL)
		ut_fatal(file, line, func, "cannot calloc %zu bytes", size);

	return retval;
}

/*
 * ut_free -- wrapper for free
 *
 * technically we don't need to wrap free since there's no return to
 * check.  using this wrapper to add memory allocation tracking later.
 */
void
ut_free(const char *file, int line, const char *func, void *ptr)
{
	free(ptr);
}

/*
 * ut_realloc -- a realloc that cannot return NULL
 */
void *
ut_realloc(const char *file, int line, const char *func,
    void *ptr, size_t size)
{
	void *retval = realloc(ptr, size);

	if (retval == NULL)
		ut_fatal(file, line, func, "cannot realloc %zu bytes", size);

	return retval;
}

/*
 * ut_strdup -- a strdup that cannot return NULL
 */
char *
ut_strdup(const char *file, int line, const char *func,
    const char *str)
{
	char *retval = strdup(str);

	if (retval == NULL)
		ut_fatal(file, line, func, "cannot strdup %zu bytes",
		    strlen(str));

	return retval;
}

/*
 * ut_pagealignmalloc -- like malloc but page-aligned memory
 */
void *
ut_pagealignmalloc(const char *file, int line, const char *func,
    size_t size)
{
	return ut_memalign(file, line, func, (size_t)Ut_pagesize, size);
}

/*
 * ut_memalign -- like malloc but page-aligned memory
 */
void *
ut_memalign(const char *file, int line, const char *func, size_t alignment,
    size_t size)
{
	void *retval;

	if ((errno = posix_memalign(&retval, alignment, size)) != 0)
		ut_fatal(file, line, func,
		    "!memalign %zu bytes (%zu alignment)", size, alignment);

	return retval;
}

/*
 * ut_mmap_anon_aligned -- mmaps anonymous memory with specified (power of two,
 *                         multiple of page size) alignment and adds guard
 *                         pages around it
 */
void *
ut_mmap_anon_aligned(const char *file, int line, const char *func,
    size_t alignment, size_t size)
{
	char *d, *d_aligned;
	uintptr_t di, di_aligned;
	size_t sz;

	if (alignment == 0)
		alignment = Ut_pagesize;

	/* alignment must be a multiple of page size */
	if (alignment & (Ut_pagesize - 1))
		return NULL;

	/* power of two */
	if (alignment & (alignment - 1))
		return NULL;

	d = ut_mmap(file, line, func, NULL, size + 2 * alignment,
			PROT_READ|PROT_WRITE, MAP_ANONYMOUS|MAP_PRIVATE, -1, 0);
	di = (uintptr_t)d;
	di_aligned = (di + alignment - 1) & ~(alignment - 1);

	if (di == di_aligned)
		di_aligned += alignment;
	d_aligned = (void *)di_aligned;

	sz = di_aligned - di;
	if (sz - Ut_pagesize)
		ut_munmap(file, line, func, d, sz - Ut_pagesize);

	/* guard page before */
	ut_mprotect(file, line, func, d_aligned - Ut_pagesize, Ut_pagesize,
			PROT_NONE);

	/* guard page after */
	ut_mprotect(file, line, func, d_aligned + size, Ut_pagesize, PROT_NONE);

	sz = di + size + 2 * alignment - (di_aligned + size) - Ut_pagesize;
	if (sz)
		ut_munmap(file, line, func, d_aligned + size + Ut_pagesize, sz);

	return d_aligned;
}

/*
 * ut_munmap_anon_aligned -- unmaps anonymous memory allocated by
 *                           ut_mmap_anon_aligned
 */
int
ut_munmap_anon_aligned(const char *file, int line, const char *func,
    void *start, size_t size)
{
	return ut_munmap(file, line, func, (char *)start - Ut_pagesize,
			size + 2 * Ut_pagesize);
}
