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
 * libvmem.h -- definitions of libvmem entry points
 *
 * This library exposes memory-mapped files as volatile memory (a la malloc)
 *
 * See libvmem(3) for details.
 */

#ifndef	LIBVMEM_H
#define	LIBVMEM_H 1

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>

typedef struct vmem VMEM;	/* opaque type internal to libvmem */

/*
 * managing volatile memory pools...
 */

#define	VMEM_MIN_POOL ((size_t)(1024 * 1024 * 14)) /* min pool size: 14MB */

VMEM *vmem_create(const char *dir, size_t size);
VMEM *vmem_create_in_region(void *addr, size_t size);
void vmem_delete(VMEM *vmp);
int vmem_check(VMEM *vmp);
void vmem_stats_print(VMEM *vmp, const char *opts);

/*
 * support for malloc and friends...
 */
void *vmem_malloc(VMEM *vmp, size_t size);
void vmem_free(VMEM *vmp, void *ptr);
void *vmem_calloc(VMEM *vmp, size_t nmemb, size_t size);
void *vmem_realloc(VMEM *vmp, void *ptr, size_t size);
void *vmem_aligned_alloc(VMEM *vmp, size_t alignment, size_t size);
char *vmem_strdup(VMEM *vmp, const char *s);
size_t vmem_malloc_usable_size(VMEM *vmp, void *ptr);

/*
 * managing overall library behavior...
 */

/*
 * VMEM_MAJOR_VERSION and VMEM_MINOR_VERSION provide the current
 * version of the libvmem API as provided by this header file.
 * Applications can verify that the version available at run-time
 * is compatible with the version used at compile-time by passing
 * these defines to vmem_check_version().
 */
#define	VMEM_MAJOR_VERSION 1
#define	VMEM_MINOR_VERSION 0
const char *vmem_check_version(
		unsigned major_required,
		unsigned minor_required);

/*
 * Passing NULL to vmem_set_funcs() tells libvmem to continue to use
 * the default for that function.  The replacement functions must
 * not make calls back into libvmem.
 *
 * The print_func is called by libvmem based on the environment
 * variable VMEM_LOG_LEVEL:
 *	0 or unset: print_func is only called for vmem_stats_print()
 *	1:          additional details are logged when errors are returned
 *	2:          basic operations (allocations/frees) are logged
 *	3:          produce very verbose tracing of function calls in libvmem
 *	4:          also log obscure stuff used to debug the library itself
 *
 * The default print_func prints to stderr.  Applications can override this
 * by setting the environment variable VMEM_LOG_FILE, or by supplying a
 * replacement print function.
 */
void vmem_set_funcs(
		void *(*malloc_func)(size_t size),
		void (*free_func)(void *ptr),
		void *(*realloc_func)(void *ptr, size_t size),
		char *(*strdup_func)(const char *s),
		void (*print_func)(const char *s));

const char *vmem_errormsg(void);

#ifdef __cplusplus
}
#endif
#endif	/* libvmem.h */
