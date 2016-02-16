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
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY LOG OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * vmem.c -- memory pool & allocation entry points for libvmem
 */
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <errno.h>
#include <stdint.h>
#include <pthread.h>

#include "libvmem.h"

#include "jemalloc.h"
#include "util.h"
#include "out.h"
#include "vmem.h"

/*
 * private to this file...
 */
static size_t Header_size;

/*
 * print_jemalloc_messages -- custom print function, for jemalloc
 *
 * Prints traces from jemalloc. All traces from jemalloc
 * are considered as error messages.
 */
static void
print_jemalloc_messages(void *ignore, const char *s)
{
	ERR("%s", s);
}

/*
 * print_jemalloc_stats -- print function, for jemalloc statistics
 *
 * Prints statistics from jemalloc. All statistics are printed with level 0.
 */
static void
print_jemalloc_stats(void *ignore, const char *s)
{
	LOG_NONL(0, "%s", s);
}

/*
 * vmem_init -- initialization for vmem
 *
 * Called automatically by the run-time loader or on the first use of vmem.
 */
void
vmem_init(void)
{
	static bool initialized = false;
	static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
	int oerrno;

	if (initialized)
		return;

	if ((errno = pthread_mutex_lock(&lock)))
		FATAL("!pthread_mutex_lock");

	if (!initialized) {
		out_init(VMEM_LOG_PREFIX, VMEM_LOG_LEVEL_VAR,
				VMEM_LOG_FILE_VAR, VMEM_MAJOR_VERSION,
				VMEM_MINOR_VERSION);
		out_set_vsnprintf_func(je_vmem_navsnprintf);
		LOG(3, NULL);
		util_init();
		Header_size = roundup(sizeof (VMEM), Pagesize);

		/* Set up jemalloc messages to a custom print function */
		je_vmem_malloc_message = print_jemalloc_messages;

		initialized = true;
	}

	oerrno = errno;
	if ((errno = pthread_mutex_unlock(&lock)))
		ERR("!pthread_mutex_unlock");
	errno = oerrno;
}

/*
 * vmem_construct -- load-time initialization for vmem
 *
 * Called automatically by the run-time loader.
 */
__attribute__((constructor))
static void
vmem_construct(void)
{
	vmem_init();
}

/*
 * vmem_fini -- libvmem cleanup routine
 *
 * Called automatically when the process terminates.
 */
__attribute__((destructor))
static void
vmem_fini(void)
{
	LOG(3, NULL);
	out_fini();
}

/*
 * vmem_create -- create a memory pool in a temp file
 */
VMEM *
vmem_create(const char *dir, size_t size)
{
	vmem_init();
	LOG(3, "dir \"%s\" size %zu", dir, size);

	if (size < VMEM_MIN_POOL) {
		ERR("size %zu smaller than %zu", size, VMEM_MIN_POOL);
		errno = EINVAL;
		return NULL;
	}

	/* silently enforce multiple of page size */
	size = roundup(size, Pagesize);

	void *addr;
	if ((addr = util_map_tmpfile(dir, size)) == NULL)
		return NULL;

	/* store opaque info at beginning of mapped area */
	struct vmem *vmp = addr;
	memset(&vmp->hdr, '\0', sizeof (vmp->hdr));
	memcpy(vmp->hdr.signature, VMEM_HDR_SIG, POOL_HDR_SIG_LEN);
	vmp->addr = addr;
	vmp->size = size;
	vmp->caller_mapped = 0;

	/* Prepare pool for jemalloc */
	if (je_vmem_pool_create((void *)((uintptr_t)addr + Header_size),
			size - Header_size, 1) == NULL) {
		ERR("pool creation failed");
		util_unmap(vmp->addr, vmp->size);
		return NULL;
	}

	/*
	 * If possible, turn off all permissions on the pool header page.
	 *
	 * The prototype PMFS doesn't allow this when large pages are in
	 * use. It is not considered an error if this fails.
	 */
	util_range_none(addr, sizeof (struct pool_hdr));

	LOG(3, "vmp %p", vmp);
	return vmp;
}

/*
 * vmem_create_in_region -- create a memory pool in a given range
 */
VMEM *
vmem_create_in_region(void *addr, size_t size)
{
	vmem_init();
	LOG(3, "addr %p size %zu", addr, size);

	if (((uintptr_t)addr & (Pagesize - 1)) != 0) {
		ERR("addr %p not aligned to pagesize %lu", addr, Pagesize);
		errno = EINVAL;
		return NULL;
	}

	if (size < VMEM_MIN_POOL) {
		ERR("size %zu smaller than %zu", size, VMEM_MIN_POOL);
		errno = EINVAL;
		return NULL;
	}

	/* store opaque info at beginning of mapped area */
	struct vmem *vmp = addr;
	memset(&vmp->hdr, '\0', sizeof (vmp->hdr));
	memcpy(vmp->hdr.signature, VMEM_HDR_SIG, POOL_HDR_SIG_LEN);
	vmp->addr = addr;
	vmp->size = size;
	vmp->caller_mapped = 1;

	/* Prepare pool for jemalloc */
	if (je_vmem_pool_create((void *)((uintptr_t)addr + Header_size),
				size - Header_size, 0) == NULL) {
		ERR("pool creation failed");
		return NULL;
	}

	/*
	 * If possible, turn off all permissions on the pool header page.
	 *
	 * The prototype PMFS doesn't allow this when large pages are in
	 * use. It is not considered an error if this fails.
	 */
	util_range_none(addr, sizeof (struct pool_hdr));

	LOG(3, "vmp %p", vmp);
	return vmp;
}

/*
 * vmem_delete -- delete a memory pool
 */
void
vmem_delete(VMEM *vmp)
{
	LOG(3, "vmp %p", vmp);

	int ret = je_vmem_pool_delete((pool_t *)((uintptr_t)vmp + Header_size));
	if (ret != 0) {
		ERR("invalid pool handle: %p", vmp);
		errno = EINVAL;
		return;
	}

	util_range_rw(vmp->addr, sizeof (struct pool_hdr));

	if (vmp->caller_mapped == 0)
		util_unmap(vmp->addr, vmp->size);
}

/*
 * vmem_check -- memory pool consistency check
 */
int
vmem_check(VMEM *vmp)
{
	vmem_init();
	LOG(3, "vmp %p", vmp);

	return je_vmem_pool_check((pool_t *)((uintptr_t)vmp + Header_size));
}

/*
 * vmem_stats_print -- spew memory allocator stats for a pool
 */
void
vmem_stats_print(VMEM *vmp, const char *opts)
{
	LOG(3, "vmp %p opts \"%s\"", vmp, opts ? opts : "");

	je_vmem_pool_malloc_stats_print(
			(pool_t *)((uintptr_t)vmp + Header_size),
			print_jemalloc_stats, NULL, opts);
}

/*
 * vmem_malloc -- allocate memory
 */
void *
vmem_malloc(VMEM *vmp, size_t size)
{
	LOG(3, "vmp %p size %zu", vmp, size);

	return je_vmem_pool_malloc(
			(pool_t *)((uintptr_t)vmp + Header_size), size);
}

/*
 * vmem_free -- free memory
 */
void
vmem_free(VMEM *vmp, void *ptr)
{
	LOG(3, "vmp %p ptr %p", vmp, ptr);

	je_vmem_pool_free((pool_t *)((uintptr_t)vmp + Header_size), ptr);
}

/*
 * vmem_calloc -- allocate zeroed memory
 */
void *
vmem_calloc(VMEM *vmp, size_t nmemb, size_t size)
{
	LOG(3, "vmp %p nmemb %zu size %zu", vmp, nmemb, size);

	return je_vmem_pool_calloc((pool_t *)((uintptr_t)vmp + Header_size),
			nmemb, size);
}

/*
 * vmem_realloc -- resize a memory allocation
 */
void *
vmem_realloc(VMEM *vmp, void *ptr, size_t size)
{
	LOG(3, "vmp %p ptr %p size %zu", vmp, ptr, size);

	return je_vmem_pool_ralloc((pool_t *)((uintptr_t)vmp + Header_size),
			ptr, size);
}

/*
 * vmem_aligned_alloc -- allocate aligned memory
 */
void *
vmem_aligned_alloc(VMEM *vmp, size_t alignment, size_t size)
{
	LOG(3, "vmp %p alignment %zu size %zu", vmp, alignment, size);

	return je_vmem_pool_aligned_alloc(
			(pool_t *)((uintptr_t)vmp + Header_size),
			alignment, size);
}

/*
 * vmem_strdup -- allocate memory for copy of string
 */
char *
vmem_strdup(VMEM *vmp, const char *s)
{
	LOG(3, "vmp %p s %p", vmp, s);

	size_t size = strlen(s) + 1;
	void *retaddr = je_vmem_pool_malloc(
			(pool_t *)((uintptr_t)vmp + Header_size), size);
	if (retaddr == NULL)
		return NULL;

	return (char *)memcpy(retaddr, s, size);
}

/*
 * vmem_malloc_usable_size -- get usable size of allocation
 */
size_t
vmem_malloc_usable_size(VMEM *vmp, void *ptr)
{
	LOG(3, "vmp %p ptr %p", vmp, ptr);

	return je_vmem_pool_malloc_usable_size(
			(pool_t *)((uintptr_t)vmp + Header_size), ptr);
}
