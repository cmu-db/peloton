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
 * libvmmalloc.c -- entry points for libvmmalloc
 *
 * NOTES:
 * 1) Since some standard library functions (fopen, sprintf) use malloc
 *    internally, then at initialization phase, malloc(3) calls are redirected
 *    to the standard jemalloc interfaces that operate on a system heap.
 *    There is no need to track these allocations.  For small allocations,
 *    jemalloc is able to detect the corresponding pool the memory was
 *    allocated from, and Vmp argument is actually ignored.  So, it is safe
 *    to reclaim this memory using je_vmem_pool_free().
 *    The problem may occur for huge allocations only (>2MB), but it seems
 *    like such allocations do not happen at initialization phase.
 *
 * 2) Debug traces in malloc(3) functions are not available until library
 *    initialization (vmem pool creation) is completed.  This is to avoid
 *    recursive calls to malloc, leading to stack overflow.
 *
 * 3) Malloc hooks in glibc are overridden to prevent any references to glibc's
 *    malloc(3) functions in case the application uses dlopen with
 *    RTLD_DEEPBIND flag.
 *
 * 4) If the process forks, there is no separate log file open for a new
 *    process, even if the configured log file name is terminated with "-".
 */

#define	_GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/param.h>
#include <errno.h>
#include <stdint.h>
#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <malloc.h>

#include "libvmem.h"
#include "libvmmalloc.h"

#include "jemalloc.h"
#include "util.h"
#include "vmem.h"
#include "vmmalloc.h"
#include "out.h"
#include "valgrind_internal.h"

#define	HUGE (2 * 1024 * 1024)

/*
 * private to this file...
 */
static size_t Header_size;
static VMEM *Vmp;
static char *Dir;
static int Fd;
static int Fd_clone;
static int Private;
static int Forkopt = 1; /* default behavior - remap as private */


/*
 * malloc -- allocate a block of size bytes
 */
__ATTR_MALLOC__
__ATTR_ALLOC_SIZE__(1)
void *
malloc(size_t size)
{
	if (Vmp == NULL) {
		ASSERT(size <= HUGE);
		return je_vmem_malloc(size);
	}
	LOG(4, "size %zu", size);
	return je_vmem_pool_malloc(
			(pool_t *)((uintptr_t)Vmp + Header_size), size);
}

/*
 * calloc -- allocate a block of nmemb * size bytes and set its contents to zero
 */
__ATTR_MALLOC__
__ATTR_ALLOC_SIZE__(1, 2)
void *
calloc(size_t nmemb, size_t size)
{
	if (Vmp == NULL) {
		ASSERT((nmemb * size) <= HUGE);
		return je_vmem_calloc(nmemb, size);
	}
	LOG(4, "nmemb %zu, size %zu", nmemb, size);
	return je_vmem_pool_calloc((pool_t *)((uintptr_t)Vmp + Header_size),
			nmemb, size);
}

/*
 * realloc -- resize a block previously allocated by malloc
 */
__ATTR_ALLOC_SIZE__(2)
void *
realloc(void *ptr, size_t size)
{
	if (Vmp == NULL) {
		ASSERT(size <= HUGE);
		return je_vmem_realloc(ptr, size);
	}
	LOG(4, "ptr %p, size %zu", ptr, size);
	return je_vmem_pool_ralloc((pool_t *)((uintptr_t)Vmp + Header_size),
			ptr, size);
}

/*
 * free -- free a block previously allocated by malloc
 */
void
free(void *ptr)
{
	if (Vmp == NULL) {
		je_vmem_free(ptr);
		return;
	}
	LOG(4, "ptr %p", ptr);
	je_vmem_pool_free((pool_t *)((uintptr_t)Vmp + Header_size), ptr);
}

/*
 * cfree -- free a block previously allocated by calloc
 *
 * the implementation is identical to free()
 */
void
cfree(void *ptr)
{
	if (Vmp == NULL) {
		je_vmem_free(ptr);
		return;
	}
	LOG(4, "ptr %p", ptr);
	je_vmem_pool_free((pool_t *)((uintptr_t)Vmp + Header_size), ptr);
}

#ifdef	VMMALLOC_OVERRIDE_MEMALIGN
/*
 * memalign -- allocate a block of size bytes, starting on an address
 * that is a multiple of boundary
 */
__ATTR_MALLOC__
__ATTR_ALLOC_ALIGN__(1)
__ATTR_ALLOC_SIZE__(2)
void *
memalign(size_t boundary, size_t size)
{
	if (Vmp == NULL) {
		ASSERT(size <= HUGE);
		return je_vmem_memalign(boundary, size);
	}
	LOG(4, "boundary %zu  size %zu", boundary, size);
	return je_vmem_pool_aligned_alloc(
			(pool_t *)((uintptr_t)Vmp + Header_size),
			boundary, size);
}
#endif

#ifdef	VMMALLOC_OVERRIDE_ALIGNED_ALLOC
/*
 * aligned_alloc -- allocate a block of size bytes, starting on an address
 * that is a multiple of alignment
 *
 * size must be a multiple of alignment
 */
__ATTR_MALLOC__
__ATTR_ALLOC_ALIGN__(1)
__ATTR_ALLOC_SIZE__(2)
void *
aligned_alloc(size_t alignment, size_t size)
{
	/* XXX - check if size is a multiple of alignment */

	if (Vmp == NULL) {
		ASSERT(size <= HUGE);
		return je_vmem_memalign(alignment, size);
	}
	LOG(4, "alignment %zu  size %zu", alignment, size);
	return je_vmem_pool_aligned_alloc(
			(pool_t *)((uintptr_t)Vmp + Header_size),
			alignment, size);
}
#endif

/*
 * posix_memalign -- allocate a block of size bytes, starting on an address
 * that is a multiple of alignment
 */
__ATTR_NONNULL__(1)
int
posix_memalign(void **memptr, size_t alignment, size_t size)
{
	int ret = 0;
	int oerrno = errno;
	if (Vmp == NULL) {
		ASSERT(size <= HUGE);
		*memptr = je_vmem_memalign(alignment, size);
		if (*memptr == NULL)
			ret = errno;
		errno = oerrno;
		return ret;
	}
	LOG(4, "alignment %zu  size %zu", alignment, size);
	*memptr = je_vmem_pool_aligned_alloc(
			(pool_t *)((uintptr_t)Vmp + Header_size),
			alignment, size);
	if (*memptr == NULL)
		ret = errno;
	errno = oerrno;
	return ret;
}

#ifdef	VMMALLOC_OVERRIDE_VALLOC
/*
 * valloc -- allocate a block of size bytes, starting on a page boundary
 */
__ATTR_MALLOC__
__ATTR_ALLOC_SIZE__(1)
void *
valloc(size_t size)
{
	ASSERTne(Pagesize, 0);
	if (Vmp == NULL) {
		ASSERT(size <= HUGE);
		return je_vmem_valloc(size);
	}
	LOG(4, "size %zu", size);
	return je_vmem_pool_aligned_alloc(
			(pool_t *)((uintptr_t)Vmp + Header_size),
			Pagesize, size);
}

__ATTR_MALLOC__
__ATTR_ALLOC_SIZE__(1)
void *
pvalloc(size_t size)
{
	ASSERTne(Pagesize, 0);
	if (Vmp == NULL) {
		ASSERT(size <= HUGE);
		return je_vmem_valloc(roundup(size, Pagesize));
	}
	LOG(4, "size %zu", size);
	return je_vmem_pool_aligned_alloc(
			(pool_t *)((uintptr_t)Vmp + Header_size),
			Pagesize, roundup(size, Pagesize));
}
#endif

/*
 * malloc_usable_size -- get usable size of allocation
 */
size_t
malloc_usable_size(void *ptr)
{
	if (Vmp == NULL) {
		return je_vmem_malloc_usable_size(ptr);
	}
	LOG(4, "ptr %p", ptr);
	return je_vmem_pool_malloc_usable_size(
			(pool_t *)((uintptr_t)Vmp + Header_size), ptr);
}

#if (defined(__GLIBC__) && !defined(__UCLIBC__))

#ifndef __MALLOC_HOOK_VOLATILE
#define	__MALLOC_HOOK_VOLATILE
#endif

/*
 * Interpose malloc hooks in glibc.  Even if the application uses dlopen
 * with RTLD_DEEPBIND flag, all the references to libc's malloc(3) functions
 * will be redirected to libvmmalloc.
 */
void *(*__MALLOC_HOOK_VOLATILE __malloc_hook) (size_t size,
	const void *caller) = (void *)malloc;
void *(*__MALLOC_HOOK_VOLATILE __realloc_hook) (void *ptr, size_t size,
	const void *caller) = (void *)realloc;
void (*__MALLOC_HOOK_VOLATILE __free_hook) (void *ptr, const void *caller) =
	(void *)free;
void *(*__MALLOC_HOOK_VOLATILE __memalign_hook) (size_t size, size_t alignment,
	const void *caller) = (void *)memalign;
#endif

/*
 * print_jemalloc_messages -- (internal) custom print function, for jemalloc
 *
 * Prints traces from jemalloc. All traces from jemalloc
 * are considered as error messages.
 */
static void
print_jemalloc_messages(void *ignore, const char *s)
{
	LOG_NONL(1, "%s", s);
}

/*
 * print_jemalloc_stats -- (internal) print function for jemalloc statistics
 */
static void
print_jemalloc_stats(void *ignore, const char *s)
{
	LOG_NONL(0, "%s", s);
}

/*
 * libvmmalloc_create -- (internal) create a memory pool in a temp file
 */
static VMEM *
libvmmalloc_create(const char *dir, size_t size)
{
	LOG(3, "dir \"%s\" size %zu", dir, size);

	if (size < VMMALLOC_MIN_POOL) {
		LOG(1, "size %zu smaller than %zu", size, VMMALLOC_MIN_POOL);
		errno = EINVAL;
		return NULL;
	}

	/* silently enforce multiple of page size */
	size = roundup(size, Pagesize);

	Fd = util_tmpfile(dir, size);
	if (Fd == -1)
		return NULL;

	void *addr;
	if ((addr = util_map(Fd, size, 0)) == NULL)
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
		LOG(1, "vmem pool creation failed");
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
 * libvmmalloc_clone - (internal) clone the entire pool
 */
static void *
libvmmalloc_clone(void)
{
	LOG(3, NULL);

	Fd_clone = util_tmpfile(Dir, Vmp->size);
	if (Fd_clone == -1)
		return NULL;

	void *addr = mmap(NULL, Vmp->size, PROT_READ|PROT_WRITE,
			MAP_SHARED, Fd_clone, 0);
	if (addr == MAP_FAILED) {
		LOG(1, "!mmap");
		(void) close(Fd_clone);
		return NULL;
	}

	LOG(3, "copy the entire pool file: dst %p src %p size %zu",
			addr, Vmp->addr, Vmp->size);

	util_range_rw(Vmp->addr, sizeof (struct pool_hdr));

	/*
	 * Part of vmem pool was probably freed at some point, so Valgrind
	 * marked it as undefined/inaccessible. We need to duplicate the whole
	 * pool, so as a workaround temporarily disable error reporting.
	 */
	VALGRIND_DO_DISABLE_ERROR_REPORTING;
	memcpy(addr, Vmp->addr, Vmp->size);
	VALGRIND_DO_ENABLE_ERROR_REPORTING;

	util_range_none(Vmp->addr, sizeof (struct pool_hdr));

	return addr;
}

/*
 * libvmmalloc_prefork -- (internal) prepare for fork()
 *
 * Clones the entire pool or remaps it with MAP_PRIVATE flag.
 */
static void
libvmmalloc_prefork(void)
{
	LOG(3, NULL);

	/*
	 * There's no need to grab any locks here, as jemalloc pre-fork handler
	 * is executed first, and it does all the synchronization.
	 */

	ASSERTne(Vmp, NULL);
	ASSERTne(Dir, NULL);

	void *addr = Vmp->addr;
	size_t size = Vmp->size;

	if (Private) {
		LOG(3, "already mapped as private - do nothing");
		return;
	}

	switch (Forkopt) {
	case 3:
		/* clone the entire pool; if it fails - remap it as private */
		LOG(3, "clone or remap");

	case 2:
		LOG(3, "clone the entire pool file");

		if (libvmmalloc_clone() != NULL)
			break;

		if (Forkopt == 2) {
			out_log(NULL, 0, NULL, 0, "Error (libvmmalloc): "
					"pool cloning failed\n");
			abort();
		}
		/* cloning failed; fall-thru to remapping */

	case 1:
		LOG(3, "remap the pool file as private");

		Vmp = mmap(addr, size, PROT_READ|PROT_WRITE,
				MAP_PRIVATE|MAP_FIXED, Fd, 0);

		if (Vmp == MAP_FAILED) {
			out_log(NULL, 0, NULL, 0, "Error (libvmmalloc): "
					"remapping failed\n");
			abort();
		}

		if (Vmp != addr) {
			out_log(NULL, 0, NULL, 0, "Error (libvmmalloc): "
					"wrong address\n");
			abort();
		}

		Private = 1;
		break;

	case 0:
		LOG(3, "do nothing");
		break;

	default:
		FATAL("invalid fork action %d", Forkopt);
	}
}

/*
 * libvmmalloc_postfork_parent -- (internal) parent post-fork handler
 */
static void
libvmmalloc_postfork_parent(void)
{
	LOG(3, NULL);

	if (Forkopt == 0) {
		/* do nothing */
		return;
	}

	if (Private) {
		LOG(3, "pool mapped as private - do nothing");
	} else {
		LOG(3, "close the cloned pool file");
		(void) close(Fd_clone);
	}
}

/*
 * libvmmalloc_postfork_child -- (internal) child post-fork handler
 */
static void
libvmmalloc_postfork_child(void)
{
	LOG(3, NULL);

	if (Forkopt == 0) {
		/* do nothing */
		return;
	}

	if (Private) {
		LOG(3, "pool mapped as private - do nothing");
	} else {
		LOG(3, "close the original pool file");
		(void) close(Fd);
		Fd = Fd_clone;

		void *addr = Vmp->addr;
		size_t size = Vmp->size;

		LOG(3, "mapping cloned pool file at %p", addr);
		Vmp = mmap(addr, size, PROT_READ|PROT_WRITE,
				MAP_SHARED|MAP_FIXED, Fd, 0);
		if (Vmp == MAP_FAILED) {
			out_log(NULL, 0, NULL, 0, "Error (libvmmalloc): "
					"mapping failed\n");
			abort();
		}

		if (Vmp != addr) {
			out_log(NULL, 0, NULL, 0, "Error (libvmmalloc): "
					"wrong address\n");
			abort();
		}
	}

	/* XXX - open a new log file, with the new PID in the name */
}

/*
 * libvmmalloc_init -- load-time initialization for libvmmalloc
 *
 * Called automatically by the run-time loader.
 * The constructor priority guarantees this is executed before
 * libjemalloc constructor.
 */
__attribute__((constructor(101)))
static void
libvmmalloc_init(void)
{
	char *env_str;
	size_t size;

	/*
	 * Register fork handlers before jemalloc initialization.
	 * This provides the correct order of fork handlers execution.
	 * Note that the first malloc() will trigger jemalloc init, so we
	 * have to register fork handlers before the call to out_init(),
	 * as it may indirectly call malloc() when opening the log file.
	 */
	if (pthread_atfork(libvmmalloc_prefork,
			libvmmalloc_postfork_parent,
			libvmmalloc_postfork_child) != 0) {
		perror("Error (libvmmalloc): pthread_atfork");
		abort();
	}

	out_init(VMMALLOC_LOG_PREFIX, VMMALLOC_LOG_LEVEL_VAR,
			VMMALLOC_LOG_FILE_VAR, VMMALLOC_MAJOR_VERSION,
			VMMALLOC_MINOR_VERSION);
	out_set_vsnprintf_func(je_vmem_navsnprintf);
	LOG(3, NULL);
	util_init();

	/* set up jemalloc messages to a custom print function */
	je_vmem_malloc_message = print_jemalloc_messages;

	Header_size = roundup(sizeof (VMEM), Pagesize);

	if ((Dir = getenv(VMMALLOC_POOL_DIR_VAR)) == NULL) {
		out_log(NULL, 0, NULL, 0, "Error (libvmmalloc): "
				"environment variable %s not specified",
				VMMALLOC_POOL_DIR_VAR);
		abort();
	}

	if ((env_str = getenv(VMMALLOC_POOL_SIZE_VAR)) == NULL) {
		out_log(NULL, 0, NULL, 0, "Error (libvmmalloc): "
				"environment variable %s not specified",
				VMMALLOC_POOL_SIZE_VAR);
		abort();
	} else {
		long long int v = atoll(env_str);
		if (v < 0) {
			out_log(NULL, 0, NULL, 0,
				"Error (libvmmalloc): negative %s",
				VMMALLOC_POOL_SIZE_VAR);
			abort();
		}

		size = (size_t)v;
	}

	if (size < VMMALLOC_MIN_POOL) {
		out_log(NULL, 0, NULL, 0, "Error (libvmmalloc): "
				"%s value is less than minimum (%zu < %zu)",
				VMMALLOC_POOL_SIZE_VAR, size,
				VMMALLOC_MIN_POOL);
		abort();
	}

	if ((env_str = getenv(VMMALLOC_FORK_VAR)) != NULL) {
		Forkopt = atoi(env_str);
		if (Forkopt < 0 || Forkopt > 3) {
			out_log(NULL, 0, NULL, 0, "Error (libvmmalloc): "
					"incorrect %s value (%d)",
					VMMALLOC_FORK_VAR, Forkopt);
				abort();
		}
		LOG(4, "Fork action %d", Forkopt);
	}

	/*
	 * XXX - vmem_create() could be used here, but then we need to
	 * link vmem.o, including all the vmem API.
	 */
	Vmp = libvmmalloc_create(Dir, size);
	if (Vmp == NULL) {
		out_log(NULL, 0, NULL, 0, "!Error (libvmmalloc): "
				"vmem pool creation failed");
		abort();
	}

	LOG(2, "initialization completed");
}

/*
 * libvmmalloc_fini -- libvmmalloc cleanup routine
 *
 * Called automatically when the process terminates and prints
 * some basic allocator statistics.
 */
__attribute__((destructor(101)))
static void
libvmmalloc_fini(void)
{
	LOG(3, NULL);

	char *env_str = getenv(VMMALLOC_LOG_STATS_VAR);
	if ((env_str == NULL) || strcmp(env_str, "1") != 0)
		return;

	LOG_NONL(0, "\n=========   system heap  ========\n");
	je_vmem_malloc_stats_print(
		print_jemalloc_stats, NULL, "gba");

	LOG_NONL(0, "\n=========    vmem pool   ========\n");
	je_vmem_pool_malloc_stats_print(
		(pool_t *)((uintptr_t)Vmp + Header_size),
		print_jemalloc_stats, NULL, "gba");
	out_fini();
}
